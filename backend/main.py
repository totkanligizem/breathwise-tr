from __future__ import annotations

import hashlib
import hmac
import json
import ntpath
import os
import traceback
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Any, Callable

import duckdb
import pandas as pd
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import Settings, get_settings
from .schemas import (
    CityCurrentSnapshot,
    CityHourlyPoint,
    DatasetMeta,
    HealthResponse,
    LocalizationMeta,
    MobileCityTimelinePoint,
    MobileProvinceMapMetric,
    ProvinceMapMetric,
    ReadinessResponse,
)


def sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def clean_records(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    clean_df = df.where(pd.notnull(df), None)
    return clean_df.to_dict(orient="records")


def required_files(settings: Settings) -> dict[str, Path]:
    return {
        "city_hourly_environment_tr": settings.marts_dir / "city_hourly_environment_tr.parquet",
        "city_forecast_vs_actual_tr": settings.marts_dir / "city_forecast_vs_actual_tr.parquet",
        "city_current_snapshot_tr": settings.marts_dir / "city_current_snapshot_tr.parquet",
        "province_map_metrics_tr": settings.marts_dir / "province_map_metrics_tr.parquet",
        "mobile_city_current_snapshot_tr_light": settings.views_dir
        / "mobile_city_current_snapshot_tr_light.parquet",
        "mobile_city_hourly_timeline_tr_light": settings.views_dir
        / "mobile_city_hourly_timeline_tr_light.parquet",
        "mobile_province_map_metrics_tr_light": settings.views_dir
        / "mobile_province_map_metrics_tr_light.parquet",
    }


def ensure_file(path: Path, name: str) -> None:
    if not path.exists():
        raise HTTPException(
            status_code=503,
            detail=(
                f"Dataset not found: {name}. Run scripts/build_analytics_marts.py first. "
                f"Expected path: {path}"
            ),
        )


def load_json_file(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _is_absolute_path_text(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if text.startswith("/"):
        return True
    if text.startswith("\\\\"):
        return True
    return ntpath.isabs(text)


def _sanitize_path_for_publish(settings: Settings, value: object) -> object:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not _is_absolute_path_text(text):
        return value

    try:
        relative = Path(text).resolve().relative_to(settings.project_root).as_posix()
    except Exception:
        return "<redacted:absolute-path>"

    return relative or "."


def _sanitize_publish_payload(
    settings: Settings,
    payload: object,
    key_name: str | None = None,
) -> object:
    path_like_key = (
        key_name == "path"
        or key_name == "project_root"
        or (isinstance(key_name, str) and (key_name.endswith("_path") or key_name.endswith("_dir")))
        or (isinstance(key_name, str) and key_name.endswith("_manifest"))
    )

    if isinstance(payload, dict):
        out: dict[str, object] = {}
        for key, value in payload.items():
            key_text = key if isinstance(key, str) else str(key)
            key_is_path_like = (
                key_text in {"path", "project_root"}
                or key_text.endswith("_path")
                or key_text.endswith("_dir")
                or key_text.endswith("_manifest")
            )
            if isinstance(value, (dict, list)):
                out[key_text] = _sanitize_publish_payload(settings, value, key_name=key_text)
                continue
            if path_like_key or key_is_path_like:
                out[key_text] = _sanitize_path_for_publish(settings, value)
            else:
                out[key_text] = value
        return out

    if isinstance(payload, list):
        return [_sanitize_publish_payload(settings, item, key_name=key_name) for item in payload]

    if path_like_key:
        return _sanitize_path_for_publish(settings, payload)
    return payload


def run_query(sql: str, params: list[object] | tuple[object, ...] | None = None) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        if params:
            return con.execute(sql, params).df()
        return con.execute(sql).df()
    finally:
        con.close()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_city_name(value: str, field: str = "city_name") -> str:
    name = value.strip()
    if not name:
        raise HTTPException(status_code=422, detail=f"{field} must not be blank")
    if len(name) > 120:
        raise HTTPException(status_code=422, detail=f"{field} is too long")
    return name


def normalize_timestamp_param(value: str, field: str) -> str:
    raw = value.strip()
    if not raw:
        raise HTTPException(status_code=422, detail=f"{field} must not be blank")

    candidate = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"{field} must be ISO-8601 compatible (example: 2024-01-01T00:00)",
        ) from exc

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)

    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def validate_time_range(start: str | None, end: str | None) -> tuple[str | None, str | None]:
    start_norm = normalize_timestamp_param(start, "start") if start is not None else None
    end_norm = normalize_timestamp_param(end, "end") if end is not None else None

    if start_norm and end_norm:
        start_dt = datetime.fromisoformat(start_norm)
        end_dt = datetime.fromisoformat(end_norm)
        if start_dt > end_dt:
            raise HTTPException(status_code=422, detail="start must be <= end")

    return start_norm, end_norm


def json_error_payload(
    request: Request,
    code: str,
    message: str,
    details: object | None = None,
) -> dict[str, object]:
    request_id = getattr(request.state, "request_id", "unknown")
    payload: dict[str, object] = {
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id,
            "path": request.url.path,
        }
    }
    if details is not None:
        payload["error"]["details"] = details
    return payload


_AQ_CATEGORY_KEY_MAP = {
    "good": "aq.category.good",
    "fair": "aq.category.fair",
    "moderate": "aq.category.moderate",
    "poor": "aq.category.poor",
    "very_poor": "aq.category.very_poor",
    "extremely_poor": "aq.category.extremely_poor",
}


def _default_i18n_contract() -> dict[str, object]:
    return {
        "supported_locales": ["tr-TR", "en-US"],
        "default_locale": "tr-TR",
        "fallback_locale": "en-US",
        "translation_keys": {
            "aq.category.good": {"tr-TR": "İyi", "en-US": "Good"},
            "aq.category.fair": {"tr-TR": "Orta", "en-US": "Fair"},
            "aq.category.moderate": {"tr-TR": "Hassas Gruplar İçin Orta", "en-US": "Moderate"},
            "aq.category.poor": {"tr-TR": "Kötü", "en-US": "Poor"},
            "aq.category.very_poor": {"tr-TR": "Çok Kötü", "en-US": "Very Poor"},
            "aq.category.extremely_poor": {"tr-TR": "Aşırı Kötü", "en-US": "Extremely Poor"},
            "alert.aq.clear": {"tr-TR": "AQ Uyarısı Yok", "en-US": "No AQ Alert"},
            "alert.aq.warning": {"tr-TR": "AQ Uyarısı", "en-US": "AQ Alert"},
            "alert.heat.clear": {"tr-TR": "Sıcaklık Uyarısı Yok", "en-US": "No Heat Alert"},
            "alert.heat.warning": {"tr-TR": "Sıcaklık Uyarısı", "en-US": "Heat Alert"},
        },
    }


def _load_i18n_contract(settings: Settings) -> dict[str, object]:
    path = settings.project_root / "data" / "contracts" / "i18n_contract.json"
    payload = load_json_file(path)
    base = _default_i18n_contract()
    if payload is None:
        payload = {}

    supported = payload.get("supported_locales")
    if not isinstance(supported, list) or not supported:
        supported = base["supported_locales"]

    default_locale = payload.get("default_locale")
    if not isinstance(default_locale, str) or default_locale.strip() == "":
        default_locale = base["default_locale"]

    fallback_locale = payload.get("fallback_locale")
    if not isinstance(fallback_locale, str) or fallback_locale.strip() == "":
        fallback_locale = base["fallback_locale"]

    translation_keys = payload.get("translation_keys")
    if not isinstance(translation_keys, dict) or not translation_keys:
        translation_keys = base["translation_keys"]

    return {
        "path": path.as_posix(),
        "generated_at_utc": payload.get("generated_at_utc"),
        "supported_locales": supported,
        "default_locale": default_locale,
        "fallback_locale": fallback_locale,
        "translation_keys": translation_keys,
    }


def _i18n_contract_version(settings: Settings) -> str:
    path = settings.project_root / "data" / "contracts" / "i18n_contract.json"
    if not path.exists():
        return "missing"
    stat = path.stat()
    return f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}"


def _locale_alias_map(contract: dict[str, object]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    supported = contract.get("supported_locales")
    if not isinstance(supported, list):
        return aliases

    for raw in supported:
        if not isinstance(raw, str):
            continue
        canonical = raw.strip()
        if not canonical:
            continue
        key = canonical.replace("_", "-").lower()
        aliases[key] = canonical
        short = key.split("-")[0]
        aliases.setdefault(short, canonical)
    return aliases


def _resolve_locale(contract: dict[str, object], requested_locale: str | None) -> str:
    aliases = _locale_alias_map(contract)
    default_locale = contract.get("default_locale")
    if not isinstance(default_locale, str) or not default_locale.strip():
        default_locale = "tr-TR"

    if requested_locale is None or requested_locale.strip() == "":
        return aliases.get(default_locale.replace("_", "-").lower(), default_locale)

    probe = requested_locale.strip().replace("_", "-").lower()
    resolved = aliases.get(probe)
    if resolved:
        return resolved

    supported = contract.get("supported_locales") if isinstance(contract.get("supported_locales"), list) else []
    raise HTTPException(
        status_code=422,
        detail={
            "message": "Unsupported locale.",
            "provided_locale": requested_locale,
            "supported_locales": supported,
        },
    )


def _translation_for_key(contract: dict[str, object], key: str | None, locale: str) -> str | None:
    if key is None:
        return None
    translation_keys = contract.get("translation_keys")
    if not isinstance(translation_keys, dict):
        return None
    entry = translation_keys.get(key)
    if not isinstance(entry, dict):
        return None

    fallback = contract.get("fallback_locale")
    default_locale = contract.get("default_locale")
    locale_order = [locale, fallback, default_locale]
    for candidate in locale_order:
        if not isinstance(candidate, str):
            continue
        value = entry.get(candidate)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _aq_category_key(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    token = value.strip().lower().replace("-", "_").replace(" ", "_")
    if not token:
        return None
    return _AQ_CATEGORY_KEY_MAP.get(token)


def _alert_level_key(value: object, domain: str) -> str:
    level = 0
    if isinstance(value, bool):
        level = int(value)
    elif isinstance(value, (int, float)):
        level = 1 if int(value) > 0 else 0
    elif isinstance(value, str):
        try:
            level = 1 if int(float(value.strip())) > 0 else 0
        except ValueError:
            level = 0
    suffix = "warning" if level > 0 else "clear"
    return f"alert.{domain}.{suffix}"


def _enrich_city_rows_localization(
    rows: list[dict[str, object]],
    contract: dict[str, object],
    locale: str,
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for row in rows:
        enriched = dict(row)
        key = _aq_category_key(enriched.get("aq_category"))
        enriched["aq_category_key"] = key
        enriched["aq_category_label"] = _translation_for_key(contract, key, locale)
        enriched["label_locale"] = locale
        out.append(enriched)
    return out


def _enrich_province_rows_localization(
    rows: list[dict[str, object]],
    contract: dict[str, object],
    locale: str,
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for row in rows:
        enriched = dict(row)
        aq_key = _alert_level_key(enriched.get("aq_alert_flag"), domain="aq")
        heat_key = _alert_level_key(enriched.get("heat_alert_flag"), domain="heat")
        enriched["aq_alert_key"] = aq_key
        enriched["aq_alert_label"] = _translation_for_key(contract, aq_key, locale)
        enriched["heat_alert_key"] = heat_key
        enriched["heat_alert_label"] = _translation_for_key(contract, heat_key, locale)
        enriched["label_locale"] = locale
        out.append(enriched)
    return out


def _localization_meta_payload(contract: dict[str, object]) -> dict[str, object]:
    supported = contract.get("supported_locales")
    if not isinstance(supported, list):
        supported = []
    translation_keys = contract.get("translation_keys")
    key_count = len(translation_keys) if isinstance(translation_keys, dict) else 0
    payload = LocalizationMeta(
        supported_locales=[item for item in supported if isinstance(item, str)],
        default_locale=str(contract.get("default_locale", "tr-TR")),
        fallback_locale=str(contract.get("fallback_locale", "en-US")),
        locale_aliases=_locale_alias_map(contract),
        translation_key_count=key_count,
        generated_at_utc=(
            _parse_iso_datetime(contract.get("generated_at_utc"))
            if isinstance(contract.get("generated_at_utc"), str)
            else None
        ),
    )
    return payload.model_dump(mode="json")


def _load_alerting_status(project_root: Path) -> dict[str, object]:
    alerts_dir = project_root / "data" / "processed" / "alerts"
    latest_alert_path = alerts_dir / "latest_alert.json"
    state_path = alerts_dir / "alert_state.json"
    latest_alert = load_json_file(latest_alert_path) or {}
    state = load_json_file(state_path) or {}
    signatures = state.get("failure_signatures") if isinstance(state.get("failure_signatures"), dict) else {}
    tracked_signature_count = len(signatures) if isinstance(signatures, dict) else 0

    latest_alert_summary = None
    if isinstance(latest_alert, dict) and latest_alert:
        latest_alert_summary = {
            "timestamp_utc": latest_alert.get("timestamp_utc"),
            "run_id": latest_alert.get("run_id"),
            "status": latest_alert.get("status"),
            "mode": latest_alert.get("mode"),
            "failed_step_name": latest_alert.get("failed_step_name"),
        }

    state_summary = {
        "updated_at_utc": state.get("updated_at_utc") if isinstance(state, dict) else None,
        "last_failure_signature": state.get("last_failure_signature") if isinstance(state, dict) else None,
        "last_failure_at_utc": state.get("last_failure_at_utc") if isinstance(state, dict) else None,
        "tracked_signature_count": tracked_signature_count,
    }

    return {
        "alerts_dir": alerts_dir.as_posix(),
        "latest_alert_path": latest_alert_path.as_posix(),
        "state_path": state_path.as_posix(),
        "latest_alert": latest_alert_summary,
        "state": state_summary,
    }


def _load_recent_failures(history_path: Path, limit: int = 5) -> list[dict[str, object]]:
    if not history_path.exists():
        return []

    lines = history_path.read_text(encoding="utf-8", errors="replace").splitlines()
    out: list[dict[str, object]] = []
    for line in reversed(lines):
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("status") != "failed":
            continue
        out.append(
            {
                "timestamp_utc": payload.get("timestamp_utc"),
                "run_id": payload.get("run_id"),
                "mode": payload.get("mode"),
                "manifest_path": payload.get("manifest_path"),
            }
        )
        if len(out) >= limit:
            break
    return list(reversed(out))


def _parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    candidate = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _scheduler_policy_from_env() -> dict[str, dict[str, object]]:
    return {
        "incremental": {
            "expected_every_hours": _env_int("BREATHWISE_SCHED_INCREMENTAL_EXPECTED_HOURS", 6),
            "max_stale_hours": _env_int("BREATHWISE_SCHED_INCREMENTAL_MAX_STALE_HOURS", 12),
            "required": _env_bool("BREATHWISE_SCHED_INCREMENTAL_REQUIRED", True),
        },
        "standard": {
            "expected_every_hours": _env_int("BREATHWISE_SCHED_STANDARD_EXPECTED_HOURS", 24),
            "max_stale_hours": _env_int("BREATHWISE_SCHED_STANDARD_MAX_STALE_HOURS", 36),
            "required": _env_bool("BREATHWISE_SCHED_STANDARD_REQUIRED", True),
        },
        "full": {
            "expected_every_hours": _env_int("BREATHWISE_SCHED_FULL_EXPECTED_HOURS", 168),
            "max_stale_hours": _env_int("BREATHWISE_SCHED_FULL_MAX_STALE_HOURS", 240),
            "required": _env_bool("BREATHWISE_SCHED_FULL_REQUIRED", False),
        },
    }


def _load_history_records(history_path: Path) -> list[dict[str, object]]:
    if not history_path.exists():
        return []
    rows: list[dict[str, object]] = []
    for line in history_path.read_text(encoding="utf-8", errors="replace").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        ts = _parse_iso_datetime(payload.get("timestamp_utc"))
        if ts is None:
            continue
        payload["_dt"] = ts
        rows.append(payload)
    rows.sort(key=lambda item: item["_dt"])  # type: ignore[index]
    return rows


def _evaluate_scheduler_health(history_records: list[dict[str, object]]) -> dict[str, object]:
    now_ref = now_utc()
    policy = _scheduler_policy_from_env()

    latest_success: dict[str, dict[str, object]] = {}
    latest_failure: dict[str, object] | None = None
    for item in history_records:
        mode = item.get("mode")
        if item.get("status") == "succeeded" and isinstance(mode, str):
            latest_success[mode] = item
        if item.get("status") == "failed":
            latest_failure = item

    modes: dict[str, object] = {}
    critical_modes: list[str] = []
    warning_modes: list[str] = []

    for mode, cfg in policy.items():
        expected_hours = int(cfg["expected_every_hours"])  # type: ignore[arg-type]
        max_stale_hours = int(cfg["max_stale_hours"])  # type: ignore[arg-type]
        required = bool(cfg["required"])
        latest = latest_success.get(mode)

        if latest is None:
            status = "missing_required" if required else "missing_optional"
            modes[mode] = {
                "status": status,
                "required": required,
                "expected_every_hours": expected_hours,
                "max_stale_hours": max_stale_hours,
                "last_success_run_id": None,
                "last_success_at_utc": None,
                "age_hours": None,
                "missed_run_estimate": None,
                "next_expected_by_utc": None,
                "stale": required,
            }
            if required:
                critical_modes.append(mode)
            continue

        latest_dt = latest.get("_dt")
        if not isinstance(latest_dt, datetime):
            continue
        age_hours = max(0.0, (now_ref - latest_dt).total_seconds() / 3600.0)
        missed_est = max(0, int(age_hours // expected_hours) - 1)
        next_expected = latest_dt + timedelta(hours=expected_hours)
        if age_hours > max_stale_hours:
            status = "stale"
            critical_modes.append(mode)
            stale = True
        elif age_hours > expected_hours:
            status = "late"
            warning_modes.append(mode)
            stale = False
        else:
            status = "ok"
            stale = False

        modes[mode] = {
            "status": status,
            "required": required,
            "expected_every_hours": expected_hours,
            "max_stale_hours": max_stale_hours,
            "last_success_run_id": latest.get("run_id"),
            "last_success_at_utc": latest_dt.isoformat(),
            "age_hours": round(age_hours, 3),
            "missed_run_estimate": int(missed_est),
            "next_expected_by_utc": next_expected.isoformat(),
            "stale": stale,
        }

    if critical_modes:
        overall = "critical"
    elif warning_modes:
        overall = "warning"
    else:
        overall = "healthy"

    latest_failure_summary = None
    if isinstance(latest_failure, dict):
        ts = latest_failure.get("_dt")
        age_hours = None
        if isinstance(ts, datetime):
            age_hours = round(max(0.0, (now_ref - ts).total_seconds() / 3600.0), 3)
        latest_failure_summary = {
            "run_id": latest_failure.get("run_id"),
            "mode": latest_failure.get("mode"),
            "timestamp_utc": ts.isoformat() if isinstance(ts, datetime) else None,
            "manifest_path": latest_failure.get("manifest_path"),
            "age_hours": age_hours,
        }

    return {
        "evaluated_at_utc": now_ref.isoformat(),
        "overall_status": overall,
        "critical_modes": sorted(critical_modes),
        "warning_modes": sorted(warning_modes),
        "policy": policy,
        "modes": modes,
        "latest_failure_run": latest_failure_summary,
    }


def _read_pointer_manifest(
    pipeline_root: Path,
    pointer_name: str,
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    pointer = load_json_file(pipeline_root / pointer_name)
    if pointer is None:
        return None, None
    manifest_rel = pointer.get("manifest_path")
    if not isinstance(manifest_rel, str):
        return pointer, None
    manifest_path = Path(manifest_rel)
    if not manifest_path.is_absolute():
        manifest_path = pipeline_root / manifest_path
    manifest = load_json_file(manifest_path)
    return pointer, manifest


def _run_summary(manifest: dict[str, object] | None) -> dict[str, object] | None:
    if manifest is None:
        return None

    failed_step_name = None
    failed_step = manifest.get("failed_step")
    if isinstance(failed_step, dict):
        failed_step_name = failed_step.get("name")
    if failed_step_name is None and isinstance(manifest.get("steps"), list):
        for item in manifest["steps"]:
            if isinstance(item, dict) and item.get("status") == "failed":
                failed_step_name = item.get("name")
                break

    return {
        "run_id": manifest.get("run_id"),
        "status": manifest.get("status"),
        "mode": manifest.get("mode"),
        "started_at_utc": manifest.get("started_at_utc"),
        "ended_at_utc": manifest.get("ended_at_utc"),
        "duration_seconds": manifest.get("duration_seconds"),
        "failed_step_name": failed_step_name,
    }


def _build_ops_summary(snapshot: dict[str, object]) -> dict[str, object]:
    alerting = snapshot.get("alerting") if isinstance(snapshot.get("alerting"), dict) else {}
    latest_alert = alerting.get("latest_alert") if isinstance(alerting.get("latest_alert"), dict) else {}
    return {
        "overall_status": (
            snapshot.get("scheduler_health", {}).get("overall_status")
            if isinstance(snapshot.get("scheduler_health"), dict)
            else None
        ),
        "latest_run_status": (
            snapshot.get("latest_run", {}).get("status")
            if isinstance(snapshot.get("latest_run"), dict)
            else None
        ),
        "latest_success_age_hours": snapshot.get("freshness", {}).get("latest_success_age_hours")
        if isinstance(snapshot.get("freshness"), dict)
        else None,
        "latest_failure_age_hours": snapshot.get("freshness", {}).get("latest_failure_age_hours")
        if isinstance(snapshot.get("freshness"), dict)
        else None,
        "validation_passed": (
            snapshot.get("validation", {}).get("passed")
            if isinstance(snapshot.get("validation"), dict)
            else None
        ),
        "recent_failure_count": len(snapshot.get("recent_failures", []))
        if isinstance(snapshot.get("recent_failures"), list)
        else None,
        "latest_alert_status": latest_alert.get("status"),
        "latest_alert_at_utc": latest_alert.get("timestamp_utc"),
    }


def _normalize_ops_status_payload(settings: Settings, payload: dict[str, object]) -> dict[str, object]:
    out = dict(payload)
    pipeline_root = settings.project_root / "data" / "processed" / "pipeline_runs"
    history_path = pipeline_root / "history.jsonl"
    history_records = _load_history_records(history_path)
    scheduler_health_eval = _evaluate_scheduler_health(history_records)
    scheduler_core = {
        key: value for key, value in scheduler_health_eval.items() if key != "latest_failure_run"
    }

    scheduler = out.get("scheduler_health")
    if not isinstance(scheduler, dict):
        out["scheduler_health"] = scheduler_core
    else:
        merged = dict(scheduler)
        for key, value in scheduler_core.items():
            merged.setdefault(key, value)
        out["scheduler_health"] = merged

    if not isinstance(out.get("recent_failures"), list):
        out["recent_failures"] = _load_recent_failures(history_path, limit=5)

    if not isinstance(out.get("latest_failure_run"), dict):
        latest_failure = scheduler_health_eval.get("latest_failure_run")
        if isinstance(latest_failure, dict):
            out["latest_failure_run"] = latest_failure

    freshness = out.get("freshness")
    if not isinstance(freshness, dict):
        freshness = {}

    if freshness.get("latest_success_age_hours") is None:
        latest_success = out.get("latest_success_run")
        if isinstance(latest_success, dict):
            ended_at = _parse_iso_datetime(latest_success.get("ended_at_utc"))
            if ended_at is not None:
                freshness["latest_success_age_hours"] = round(
                    max(0.0, (now_utc() - ended_at).total_seconds() / 3600.0),
                    3,
                )

    if freshness.get("latest_failure_age_hours") is None:
        latest_failure = out.get("latest_failure_run")
        if isinstance(latest_failure, dict):
            age = latest_failure.get("age_hours")
            if isinstance(age, (int, float)):
                freshness["latest_failure_age_hours"] = age
            else:
                failed_at = _parse_iso_datetime(latest_failure.get("timestamp_utc"))
                if failed_at is not None:
                    freshness["latest_failure_age_hours"] = round(
                        max(0.0, (now_utc() - failed_at).total_seconds() / 3600.0),
                        3,
                    )

    out["freshness"] = freshness

    if not isinstance(out.get("summary"), dict):
        out["summary"] = _build_ops_summary(out)
    else:
        merged_summary = dict(out["summary"])
        normalized = _build_ops_summary(out)
        for key, value in normalized.items():
            merged_summary.setdefault(key, value)
        out["summary"] = merged_summary

    if not isinstance(out.get("alerting"), dict):
        out["alerting"] = _load_alerting_status(settings.project_root)

    return out


def _live_ops_status_snapshot(settings: Settings) -> dict[str, object]:
    pipeline_root = settings.project_root / "data" / "processed" / "pipeline_runs"
    validation_path = settings.project_root / "data" / "processed" / "marts" / "validation_report.json"
    lock_path = pipeline_root / "pipeline.lock"
    lock_payload = load_json_file(lock_path)

    latest_ptr, latest_manifest = _read_pointer_manifest(pipeline_root, "latest_run_manifest.json")
    latest_success_ptr, latest_success_manifest = _read_pointer_manifest(
        pipeline_root,
        "latest_success_run_manifest.json",
    )

    lock_active = False
    if isinstance(lock_payload, dict):
        pid = lock_payload.get("pid")
        if isinstance(pid, int):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                lock_active = False
            except PermissionError:
                lock_active = True
            else:
                lock_active = True

    validation = load_json_file(validation_path) or {}
    history_path = pipeline_root / "history.jsonl"
    history_records = _load_history_records(history_path)
    scheduler_health = _evaluate_scheduler_health(history_records)
    latest_failure_summary = (
        scheduler_health.get("latest_failure_run")
        if isinstance(scheduler_health.get("latest_failure_run"), dict)
        else None
    )

    latest_success_summary = _run_summary(latest_success_manifest)
    latest_success_age_hours = None
    if isinstance(latest_success_summary, dict):
        ended_at = _parse_iso_datetime(latest_success_summary.get("ended_at_utc"))
        if ended_at is not None:
            latest_success_age_hours = round(max(0.0, (now_utc() - ended_at).total_seconds() / 3600.0), 3)

    snapshot = {
        "generated_at_utc": now_utc().isoformat(),
        "latest_run": _run_summary(latest_manifest),
        "latest_success_run": latest_success_summary,
        "recent_failures": _load_recent_failures(history_path, limit=5),
        "latest_failure_run": latest_failure_summary,
        "validation": {
            "passed": validation.get("passed"),
            "check_count": validation.get("check_count"),
            "failed_count": validation.get("failed_count"),
            "report_path": validation_path.as_posix(),
            "generated_at_utc": validation.get("generated_at_utc"),
        },
        "scheduler_health": {
            key: value for key, value in scheduler_health.items() if key != "latest_failure_run"
        },
        "freshness": {
            "latest_success_age_hours": latest_success_age_hours,
            "latest_failure_age_hours": latest_failure_summary.get("age_hours")
            if isinstance(latest_failure_summary, dict)
            else None,
        },
        "pipeline_lock": {
            "active": lock_active,
            "path": lock_path.as_posix(),
            "payload": lock_payload,
        },
        "pointers": {
            "latest_run": latest_ptr,
            "latest_success_run": latest_success_ptr,
        },
        "alerting": _load_alerting_status(settings.project_root),
    }
    snapshot["summary"] = _build_ops_summary(snapshot)
    return snapshot


def _readiness_snapshot(settings: Settings) -> tuple[bool, bool, list[str]]:
    files = required_files(settings)
    missing = [name for name, path in files.items() if not path.exists()]
    marts_ready = all(path.exists() for name, path in files.items() if name.endswith("_tr"))
    views_ready = all(path.exists() for name, path in files.items() if name.startswith("mobile_"))
    return marts_ready, views_ready, missing


def _dataset_version(settings: Settings) -> str:
    files = required_files(settings)
    parts: list[str] = []

    for name, path in sorted(files.items()):
        if path.exists():
            stat = path.stat()
            parts.append(f"{name}:{path.name}:{stat.st_mtime_ns}:{stat.st_size}")
        else:
            parts.append(f"{name}:missing")

    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


class TTLResponseCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, object]] = {}
        self._lock = Lock()

    def get(self, key: str, now_monotonic: float) -> object | None:
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            expires_at, value = item
            if expires_at <= now_monotonic:
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: str, value: object, ttl_seconds: int, max_entries: int, now_monotonic: float) -> None:
        with self._lock:
            self._store[key] = (now_monotonic + ttl_seconds, value)
            if len(self._store) <= max_entries:
                return

            # Evict the nearest-to-expire entries first.
            for stale_key, _ in sorted(self._store.items(), key=lambda kv: kv[1][0])[: len(self._store) - max_entries]:
                self._store.pop(stale_key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


_RESPONSE_CACHE = TTLResponseCache()
_RATE_LIMIT_LOCK = Lock()
_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = defaultdict(deque)
_ACCESS_LOG_LOCK = Lock()


def reset_runtime_state() -> None:
    _RESPONSE_CACHE.clear()
    with _RATE_LIMIT_LOCK:
        _RATE_LIMIT_BUCKETS.clear()


def append_access_log(settings: Settings, record: dict[str, object]) -> None:
    if not settings.api_access_log_enabled:
        return
    path = settings.api_access_log_path
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with _ACCESS_LOG_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)


def _cache_fetch(
    settings: Settings,
    cache_key: str,
    loader: Callable[[], object],
) -> tuple[object, str]:
    if not settings.cache_enabled:
        return loader(), "disabled"

    now_mono = monotonic()
    cached = _RESPONSE_CACHE.get(cache_key, now_monotonic=now_mono)
    if cached is not None:
        return cached, "hit"

    value = loader()
    _RESPONSE_CACHE.set(
        cache_key,
        value,
        ttl_seconds=settings.cache_ttl_seconds,
        max_entries=settings.cache_max_entries,
        now_monotonic=now_mono,
    )
    return value, "miss"


def _cache_key(settings: Settings, endpoint: str, params: dict[str, object]) -> str:
    payload = {
        "dataset_version": _dataset_version(settings),
        "endpoint": endpoint,
        "params": params,
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)


def _request_id(request: Request) -> str:
    existing = getattr(request.state, "request_id", None)
    if existing:
        return existing

    generated = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    request.state.request_id = generated
    return generated


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    value = authorization.strip()
    if not value.lower().startswith("bearer "):
        return None
    token = value[7:].strip()
    return token or None


def require_api_access(
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    settings = get_settings()
    if not settings.api_auth_enabled:
        return

    expected_key = settings.api_key
    if not expected_key:
        raise HTTPException(
            status_code=503,
            detail="API authentication is enabled but BREATHWISE_API_KEY is not configured.",
        )

    provided_key = request.headers.get(settings.api_key_header_name)
    if not provided_key:
        provided_key = _extract_bearer_token(authorization)

    if not provided_key or not hmac.compare_digest(provided_key, expected_key):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized. Provide a valid API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )


app = FastAPI(
    title="Breathwise TR Local API",
    version="0.2.0",
    description=(
        "Local-first FastAPI layer for Breathwise TR marts and mobile-ready views. "
        "Uses Parquet + DuckDB with environment-based configuration, optional auth, "
        "optional rate limiting, and endpoint caching."
    ),
)


def _private_network_origin_regex() -> str:
    return (
        r"^https?://("
        r"localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\]|"
        r"10(?:\.\d{1,3}){3}|"
        r"192\.168(?:\.\d{1,3}){2}|"
        r"172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2}"
        r")(?::\d{1,5})?$"
    )


def configure_cors(app_instance: FastAPI) -> None:
    settings = get_settings()
    if not settings.cors_enabled:
        return

    app_instance.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allowed_origins,
        allow_origin_regex=(
            _private_network_origin_regex() if settings.cors_allow_private_network_origins else None
        ),
        allow_methods=settings.cors_allowed_methods,
        allow_headers=settings.cors_allowed_headers,
        expose_headers=settings.cors_expose_headers,
        allow_credentials=settings.cors_allow_credentials,
        max_age=settings.cors_max_age_seconds,
    )


configure_cors(app)


@app.middleware("http")
async def attach_request_id(request: Request, call_next: Callable[[Request], object]) -> Response:
    settings = get_settings()
    started_mono = monotonic()
    request_id = _request_id(request)
    response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
    duration_ms = int((monotonic() - started_mono) * 1000)
    response.headers["X-Process-Time-Ms"] = str(duration_ms)

    try:
        append_access_log(
            settings,
            {
                "timestamp_utc": now_utc().isoformat(),
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "query": request.url.query,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
                "client_host": request.client.host if request.client else "unknown",
                "user_agent": request.headers.get("user-agent"),
            },
        )
    except Exception:
        # Access logging must never break request handling.
        pass
    return response


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next: Callable[[Request], object]) -> Response:
    settings = get_settings()

    if not settings.rate_limit_enabled:
        return await call_next(request)

    if request.method == "OPTIONS":
        return await call_next(request)

    if request.url.path in {"/health", "/ready", "/openapi.json"} or request.url.path.startswith("/docs"):
        return await call_next(request)

    if not request.url.path.startswith("/v1/"):
        return await call_next(request)

    client_host = request.client.host if request.client else "unknown"
    bucket_key = f"{client_host}:{request.url.path}"
    now_mono = monotonic()
    window = settings.rate_limit_window_seconds
    limit = settings.rate_limit_requests

    with _RATE_LIMIT_LOCK:
        bucket = _RATE_LIMIT_BUCKETS[bucket_key]
        while bucket and now_mono - bucket[0] > window:
            bucket.popleft()

        if len(bucket) >= limit:
            retry_after = max(1, int(window - (now_mono - bucket[0]))) if bucket else window
            return JSONResponse(
                status_code=429,
                content=json_error_payload(
                    request,
                    code="rate_limit_exceeded",
                    message="Too many requests. Slow down and retry.",
                ),
                headers={
                    "Retry-After": str(retry_after),
                    "X-Request-Id": _request_id(request),
                    "X-RateLimit-Limit": str(limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Window-Seconds": str(window),
                },
            )

        bucket.append(now_mono)
        remaining = max(0, limit - len(bucket))

    response = await call_next(request)
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Window-Seconds"] = str(window)
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    details = exc.detail if not isinstance(exc.detail, str) else None
    message = exc.detail if isinstance(exc.detail, str) else "Request failed"

    headers = dict(exc.headers) if exc.headers else {}
    headers["X-Request-Id"] = _request_id(request)

    return JSONResponse(
        status_code=exc.status_code,
        content=json_error_payload(request, code="http_error", message=message, details=details),
        headers=headers,
    )


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=json_error_payload(
            request,
            code="request_validation_error",
            message="Request validation failed.",
            details=exc.errors(),
        ),
        headers={"X-Request-Id": _request_id(request)},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content=json_error_payload(
            request,
            code="internal_server_error",
            message="Unexpected server error.",
        ),
        headers={"X-Request-Id": _request_id(request)},
    )


@app.get("/health", response_model=HealthResponse)
def health(
    publish_safe: bool = Query(default=False, description="When true, redact absolute local paths."),
) -> HealthResponse:
    settings = get_settings()
    marts_ready, views_ready, _ = _readiness_snapshot(settings)
    project_root = settings.project_root.as_posix()
    if publish_safe:
        sanitized = _sanitize_path_for_publish(settings, project_root)
        project_root = sanitized if isinstance(sanitized, str) else project_root

    return HealthResponse(
        status="ok",
        project_root=project_root,
        marts_ready=marts_ready,
        views_ready=views_ready,
        timestamp_utc=now_utc(),
    )


@app.get("/ready", response_model=ReadinessResponse)
def readiness() -> ReadinessResponse | JSONResponse:
    settings = get_settings()
    marts_ready, views_ready, missing = _readiness_snapshot(settings)
    ready = marts_ready and views_ready

    payload = ReadinessResponse(
        status="ready" if ready else "not_ready",
        ready=ready,
        missing_datasets=sorted(missing),
        marts_ready=marts_ready,
        views_ready=views_ready,
        timestamp_utc=now_utc(),
    )

    if ready:
        return payload

    return JSONResponse(status_code=503, content=payload.model_dump(mode="json"))


@app.get("/v1/meta/ops-status", dependencies=[Depends(require_api_access)])
def ops_status(
    response: Response,
    compact: bool = Query(default=False),
    max_recent_failures: int = Query(default=5, ge=0, le=50),
    publish_safe: bool = Query(default=False, description="When true, redact absolute local paths."),
) -> dict[str, object]:
    settings = get_settings()

    def load() -> dict[str, object]:
        status_path = settings.project_root / "data" / "processed" / "ops" / "ops_status_latest.json"
        file_payload = load_json_file(status_path)
        if file_payload is not None:
            payload = dict(file_payload)
            payload["source"] = "ops_status_file"
            payload["status_path"] = status_path.as_posix()
            return payload

        payload = _live_ops_status_snapshot(settings)
        payload["source"] = "live_snapshot"
        payload["status_path"] = status_path.as_posix()
        return payload

    key = _cache_key(settings, "ops_status", params={})
    payload, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    if not isinstance(payload, dict):
        return {"status": "unavailable"}

    out = dict(payload)
    out = _normalize_ops_status_payload(settings, out)
    if isinstance(out.get("recent_failures"), list):
        out["recent_failures"] = out["recent_failures"][-max_recent_failures:]
    if publish_safe:
        sanitized = _sanitize_publish_payload(settings, out)
        if isinstance(sanitized, dict):
            out = sanitized

    if not compact:
        return out

    scheduler = out.get("scheduler_health") if isinstance(out.get("scheduler_health"), dict) else {}
    return {
        "generated_at_utc": out.get("generated_at_utc"),
        "summary": out.get("summary"),
        "latest_run": out.get("latest_run"),
        "latest_success_run": out.get("latest_success_run"),
        "latest_failure_run": out.get("latest_failure_run"),
        "alerting": out.get("alerting"),
        "scheduler_health": {
            "overall_status": scheduler.get("overall_status"),
            "critical_modes": scheduler.get("critical_modes"),
            "warning_modes": scheduler.get("warning_modes"),
        },
        "validation": out.get("validation"),
    }


@app.get("/v1/meta/datasets", response_model=list[DatasetMeta], dependencies=[Depends(require_api_access)])
def dataset_metadata(
    response: Response,
    publish_safe: bool = Query(default=False, description="When true, redact absolute local paths."),
) -> list[DatasetMeta]:
    settings = get_settings()

    def load() -> list[dict[str, object]]:
        datasets: list[dict[str, object]] = []

        for name, path in required_files(settings).items():
            ensure_file(path, name)
            src = f"read_parquet('{sql_path(path)}')"
            row_count = int(run_query(f"select count(*) as c from {src}").iloc[0]["c"])

            entity_column = "province_name" if "province" in name else "city_name"
            entity_count: int | None = None
            cols = run_query(f"describe select * from {src}")
            if entity_column in cols["column_name"].tolist():
                entity_count = int(
                    run_query(f"select count(distinct {entity_column}) as c from {src}").iloc[0]["c"]
                )

            time_min = None
            time_max = None
            if "time_ts" in cols["column_name"].tolist():
                time_bounds = run_query(f"select min(time_ts) as min_t, max(time_ts) as max_t from {src}")
                time_min = time_bounds.iloc[0]["min_t"]
                time_max = time_bounds.iloc[0]["max_t"]

            datasets.append(
                {
                    "dataset_name": name,
                    "path": path.as_posix(),
                    "row_count": row_count,
                    "entity_count": entity_count,
                    "time_min": time_min,
                    "time_max": time_max,
                }
            )

        return datasets

    key = _cache_key(settings, "dataset_metadata", params={})
    rows, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    rows_out = rows
    if publish_safe:
        sanitized = _sanitize_publish_payload(settings, rows)
        if isinstance(sanitized, list):
            rows_out = sanitized
    return [DatasetMeta(**row) for row in rows_out]


@app.get("/v1/meta/localization", dependencies=[Depends(require_api_access)])
def localization_metadata(
    response: Response,
    include_translations: bool = Query(default=False),
    max_keys: int = Query(default=200, ge=1, le=5000),
    publish_safe: bool = Query(default=False, description="When true, redact absolute local paths."),
) -> dict[str, object]:
    settings = get_settings()
    contract = _load_i18n_contract(settings)
    default_locale = _resolve_locale(contract, None)
    i18n_version = _i18n_contract_version(settings)

    def load() -> dict[str, object]:
        payload = _localization_meta_payload(contract)
        payload["contract_path"] = contract.get("path")
        if include_translations:
            translation_keys = (
                contract.get("translation_keys", {})
                if isinstance(contract.get("translation_keys"), dict)
                else {}
            )
            selected: dict[str, object] = {}
            for key_name in sorted(translation_keys.keys())[:max_keys]:
                value = translation_keys.get(key_name)
                if isinstance(value, dict):
                    selected[key_name] = value
            payload["translation_keys_total"] = len(translation_keys)
            payload["translation_keys_returned"] = len(selected)
            payload["translation_keys"] = selected
        return payload

    key = _cache_key(
        settings,
        "localization_metadata",
        params={"include_translations": include_translations, "max_keys": max_keys, "i18n_version": i18n_version},
    )
    payload, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    response.headers["Content-Language"] = default_locale
    if isinstance(payload, dict):
        if publish_safe:
            sanitized = _sanitize_publish_payload(settings, payload)
            if isinstance(sanitized, dict):
                return sanitized
        return payload
    return {"status": "unavailable"}


@app.get("/v1/cities/current", response_model=list[CityCurrentSnapshot], dependencies=[Depends(require_api_access)])
def get_city_current_snapshot(
    response: Response,
    city_name: str | None = Query(default=None),
    locale: str | None = Query(default=None, description="Localization locale (tr-TR or en-US)."),
    limit: int = Query(default=200, ge=1, le=1000),
) -> list[CityCurrentSnapshot]:
    settings = get_settings()
    path = required_files(settings)["city_current_snapshot_tr"]
    ensure_file(path, "city_current_snapshot_tr")

    contract = _load_i18n_contract(settings)
    locale_resolved = _resolve_locale(contract, locale)
    i18n_version = _i18n_contract_version(settings)
    city_norm = normalize_city_name(city_name, field="city_name") if city_name is not None else None

    def load() -> list[dict[str, object]]:
        src = f"read_parquet('{sql_path(path)}')"
        sql = (
            "select "
            "city_name, province_name, snapshot_time, city_latitude, city_longitude, "
            "forecast_temperature_2m, forecast_apparent_temperature, forecast_weather_code, "
            "forecast_wind_speed_10m, aq_european_aqi, aq_category, aq_pm2_5, aq_pm10 "
            f"from {src}"
        )
        params: list[object] = []

        if city_norm:
            sql += " where lower(city_name) = lower(?)"
            params.append(city_norm)

        sql += " order by city_name limit ?"
        params.append(limit)

        rows = clean_records(run_query(sql, params))
        return _enrich_city_rows_localization(rows, contract=contract, locale=locale_resolved)

    key = _cache_key(
        settings,
        "city_current_snapshot",
        params={"city_name": city_norm, "locale": locale_resolved, "limit": limit, "i18n_version": i18n_version},
    )
    rows, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    response.headers["Content-Language"] = locale_resolved
    return [CityCurrentSnapshot(**row) for row in rows]


@app.get("/v1/cities/{city_name}/hourly", response_model=list[CityHourlyPoint], dependencies=[Depends(require_api_access)])
def get_city_hourly_environment(
    city_name: str,
    response: Response,
    start: str | None = Query(default=None, description="ISO timestamp lower bound"),
    end: str | None = Query(default=None, description="ISO timestamp upper bound"),
    limit: int = Query(default=1000, ge=1, le=10000),
) -> list[CityHourlyPoint]:
    settings = get_settings()
    path = required_files(settings)["city_hourly_environment_tr"]
    ensure_file(path, "city_hourly_environment_tr")

    city_norm = normalize_city_name(city_name, field="city_name")
    start_norm, end_norm = validate_time_range(start, end)

    def load() -> list[dict[str, object]]:
        src = f"read_parquet('{sql_path(path)}')"
        clauses = ["lower(city_name) = lower(?)"]
        params: list[object] = [city_norm]

        if start_norm:
            clauses.append("time_ts >= cast(? as timestamp)")
            params.append(start_norm)
        if end_norm:
            clauses.append("time_ts <= cast(? as timestamp)")
            params.append(end_norm)

        sql = (
            "select "
            "city_name, province_name, time, hw_temperature_2m, hf_temperature_2m, fw_temperature_2m, "
            "aq_european_aqi, aq_pm2_5, cams_pm2p5, available_source_count "
            f"from {src} "
            f"where {' and '.join(clauses)} "
            "order by time_ts "
            "limit ?"
        )
        params.append(limit)

        return clean_records(run_query(sql, params))

    key = _cache_key(
        settings,
        "city_hourly_environment",
        params={"city_name": city_norm, "start": start_norm, "end": end_norm, "limit": limit},
    )
    rows, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    return [CityHourlyPoint(**row) for row in rows]


@app.get("/v1/provinces/map-metrics", response_model=list[ProvinceMapMetric], dependencies=[Depends(require_api_access)])
def get_province_map_metrics(
    response: Response,
    locale: str | None = Query(default=None, description="Localization locale (tr-TR or en-US)."),
    limit: int = Query(default=200, ge=1, le=500),
) -> list[ProvinceMapMetric]:
    settings = get_settings()
    path = required_files(settings)["province_map_metrics_tr"]
    ensure_file(path, "province_map_metrics_tr")
    contract = _load_i18n_contract(settings)
    locale_resolved = _resolve_locale(contract, locale)
    i18n_version = _i18n_contract_version(settings)

    def load() -> list[dict[str, object]]:
        src = f"read_parquet('{sql_path(path)}')"
        sql = (
            "select "
            "province_name, shape_iso, snapshot_time, city_count, "
            "avg_forecast_temperature_2m, avg_aq_european_aqi, max_aq_european_aqi, "
            "avg_aq_pm2_5, hw_2024_avg_temperature_2m, "
            "cams_avg_pm2p5, cams_avg_no2, cams_2024_01_avg_pm2p5, "
            "aq_alert_flag, heat_alert_flag, map_priority_score "
            f"from {src} "
            "order by province_name "
            "limit ?"
        )
        rows = clean_records(run_query(sql, [limit]))
        return _enrich_province_rows_localization(rows, contract=contract, locale=locale_resolved)

    key = _cache_key(
        settings,
        "province_map_metrics",
        params={"locale": locale_resolved, "limit": limit, "i18n_version": i18n_version},
    )
    rows, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    response.headers["Content-Language"] = locale_resolved
    return [ProvinceMapMetric(**row) for row in rows]


@app.get("/v1/mobile/cities/current", response_model=list[CityCurrentSnapshot], dependencies=[Depends(require_api_access)])
def get_mobile_city_current(
    response: Response,
    city_name: str | None = Query(default=None),
    locale: str | None = Query(default=None, description="Localization locale (tr-TR or en-US)."),
    limit: int = Query(default=200, ge=1, le=1000),
) -> list[CityCurrentSnapshot]:
    settings = get_settings()
    path = required_files(settings)["mobile_city_current_snapshot_tr_light"]
    ensure_file(path, "mobile_city_current_snapshot_tr_light")

    contract = _load_i18n_contract(settings)
    locale_resolved = _resolve_locale(contract, locale)
    i18n_version = _i18n_contract_version(settings)
    city_norm = normalize_city_name(city_name, field="city_name") if city_name is not None else None

    def load() -> list[dict[str, object]]:
        src = f"read_parquet('{sql_path(path)}')"
        sql = f"select * from {src}"
        params: list[object] = []

        if city_norm:
            sql += " where lower(city_name) = lower(?)"
            params.append(city_norm)

        sql += " order by city_name limit ?"
        params.append(limit)

        rows = clean_records(run_query(sql, params))
        return _enrich_city_rows_localization(rows, contract=contract, locale=locale_resolved)

    key = _cache_key(
        settings,
        "mobile_city_current",
        params={"city_name": city_norm, "locale": locale_resolved, "limit": limit, "i18n_version": i18n_version},
    )
    rows, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    response.headers["Content-Language"] = locale_resolved
    return [CityCurrentSnapshot(**row) for row in rows]


@app.get("/v1/mobile/cities/{city_name}/timeline", response_model=list[MobileCityTimelinePoint], dependencies=[Depends(require_api_access)])
def get_mobile_city_timeline(
    city_name: str,
    response: Response,
    start: str | None = Query(default=None, description="ISO timestamp lower bound"),
    end: str | None = Query(default=None, description="ISO timestamp upper bound"),
    limit: int = Query(default=240, ge=1, le=2000),
) -> list[MobileCityTimelinePoint]:
    settings = get_settings()
    path = required_files(settings)["mobile_city_hourly_timeline_tr_light"]
    ensure_file(path, "mobile_city_hourly_timeline_tr_light")

    city_norm = normalize_city_name(city_name, field="city_name")
    start_norm, end_norm = validate_time_range(start, end)

    def load() -> list[dict[str, object]]:
        src = f"read_parquet('{sql_path(path)}')"
        clauses = ["lower(city_name) = lower(?)"]
        params: list[object] = [city_norm]

        if start_norm:
            clauses.append("cast(time as timestamp) >= cast(? as timestamp)")
            params.append(start_norm)
        if end_norm:
            clauses.append("cast(time as timestamp) <= cast(? as timestamp)")
            params.append(end_norm)

        sql = (
            f"select * from {src} "
            f"where {' and '.join(clauses)} "
            "order by cast(time as timestamp) "
            "limit ?"
        )
        params.append(limit)

        return clean_records(run_query(sql, params))

    key = _cache_key(
        settings,
        "mobile_city_timeline",
        params={"city_name": city_norm, "start": start_norm, "end": end_norm, "limit": limit},
    )
    rows, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    return [MobileCityTimelinePoint(**row) for row in rows]


@app.get(
    "/v1/mobile/provinces/map-metrics",
    response_model=list[MobileProvinceMapMetric],
    dependencies=[Depends(require_api_access)],
)
def get_mobile_province_map_metrics(
    response: Response,
    locale: str | None = Query(default=None, description="Localization locale (tr-TR or en-US)."),
    limit: int = Query(default=200, ge=1, le=500),
) -> list[MobileProvinceMapMetric]:
    settings = get_settings()
    path = required_files(settings)["mobile_province_map_metrics_tr_light"]
    ensure_file(path, "mobile_province_map_metrics_tr_light")
    contract = _load_i18n_contract(settings)
    locale_resolved = _resolve_locale(contract, locale)
    i18n_version = _i18n_contract_version(settings)

    def load() -> list[dict[str, object]]:
        src = f"read_parquet('{sql_path(path)}')"
        sql = f"select * from {src} order by province_name limit ?"
        rows = clean_records(run_query(sql, [limit]))
        return _enrich_province_rows_localization(rows, contract=contract, locale=locale_resolved)

    key = _cache_key(
        settings,
        "mobile_province_map_metrics",
        params={"locale": locale_resolved, "limit": limit, "i18n_version": i18n_version},
    )
    rows, cache_status = _cache_fetch(settings, key, load)
    response.headers["X-Breathwise-Cache"] = cache_status
    response.headers["Content-Language"] = locale_resolved
    return [MobileProvinceMapMetric(**row) for row in rows]
