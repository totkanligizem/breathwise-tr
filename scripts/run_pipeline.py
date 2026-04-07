from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from _shared import discover_project_root


@dataclass(frozen=True)
class StepCommand:
    name: str
    command: list[str]
    description: str


@dataclass(frozen=True)
class AlertConfig:
    enabled: bool
    on_success: bool
    on_failure: bool
    alerts_dir: Path
    webhook_url: str | None
    webhook_timeout_seconds: int
    webhook_retries: int
    webhook_backoff_seconds: int
    dedup_window_minutes: int
    repeat_every_failures: int
    state_path: Path
    mac_notify: bool


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return now_utc().isoformat()


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}, got {value}")
    return value


def resolve_path(project_root: Path, path_like: str) -> Path:
    candidate = Path(path_like).expanduser()
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return candidate.resolve()


def rel_path(project_root: Path, path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return resolved.as_posix()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def command_display(command: list[str]) -> str:
    return " ".join(command)


def tail_text(path: Path, max_lines: int = 80) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:])


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def acquire_run_lock(lock_path: Path, run_id: str, allow_concurrent: bool) -> dict[str, Any] | None:
    if allow_concurrent:
        return None

    lock_path.parent.mkdir(parents=True, exist_ok=True)

    if lock_path.exists():
        existing = load_json(lock_path) or {}
        existing_pid_raw = existing.get("pid")
        existing_pid = existing_pid_raw if isinstance(existing_pid_raw, int) else None
        if existing_pid is not None and is_pid_alive(existing_pid):
            existing_run_id = existing.get("run_id")
            raise RuntimeError(
                "Another pipeline run appears active "
                f"(pid={existing_pid}, run_id={existing_run_id}). "
                "Use --allow-concurrent to bypass this guard."
            )
        # Stale lock from a crashed run. Remove and continue.
        lock_path.unlink(missing_ok=True)

    payload = {
        "pid": os.getpid(),
        "run_id": run_id,
        "acquired_at_utc": iso_now(),
        "lock_version": 1,
    }
    write_json(lock_path, payload)
    return payload


def release_run_lock(lock_path: Path, allow_concurrent: bool) -> None:
    if allow_concurrent:
        return
    if not lock_path.exists():
        return

    existing = load_json(lock_path) or {}
    owner_pid = existing.get("pid")
    if isinstance(owner_pid, int) and owner_pid != os.getpid():
        return
    lock_path.unlink(missing_ok=True)


def parse_run_dir_timestamp(run_dir_name: str) -> datetime | None:
    try:
        return datetime.strptime(run_dir_name, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_iso_datetime(value: object) -> datetime | None:
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


def prune_old_runs(pipeline_root: Path, retention_days: int, keep_run_ids: set[str]) -> dict[str, Any]:
    cutoff = now_utc() - timedelta(days=retention_days)
    scanned = 0
    pruned = 0
    skipped = 0
    removed_dirs: list[str] = []

    for candidate in sorted(pipeline_root.iterdir()):
        if not candidate.is_dir():
            continue
        scanned += 1
        run_ts = parse_run_dir_timestamp(candidate.name)
        if run_ts is None:
            skipped += 1
            continue
        if candidate.name in keep_run_ids:
            skipped += 1
            continue
        if run_ts >= cutoff:
            skipped += 1
            continue

        shutil.rmtree(candidate, ignore_errors=False)
        removed_dirs.append(candidate.name)
        pruned += 1

    return {
        "retention_days": retention_days,
        "cutoff_utc": cutoff.isoformat(),
        "scanned_run_dirs": scanned,
        "pruned_run_dirs": pruned,
        "skipped_run_dirs": skipped,
        "removed_run_ids": removed_dirs,
    }


def prune_history_ledger(
    history_path: Path,
    retention_days: int,
    keep_run_ids: set[str],
) -> dict[str, Any]:
    if not history_path.exists():
        return {
            "exists": False,
            "retention_days": retention_days,
            "pruned_rows": 0,
            "kept_rows": 0,
        }

    cutoff = now_utc() - timedelta(days=retention_days)
    lines = history_path.read_text(encoding="utf-8", errors="replace").splitlines()
    kept_lines: list[str] = []
    pruned_rows = 0

    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            # Keep malformed rows to avoid destructive loss.
            kept_lines.append(text)
            continue
        if not isinstance(payload, dict):
            kept_lines.append(text)
            continue

        run_id = payload.get("run_id")
        if isinstance(run_id, str) and run_id in keep_run_ids:
            kept_lines.append(text)
            continue

        ts = parse_iso_datetime(payload.get("timestamp_utc"))
        if ts is not None and ts < cutoff:
            pruned_rows += 1
            continue

        kept_lines.append(text)

    body = "\n".join(kept_lines)
    if body:
        body += "\n"
    history_path.write_text(body, encoding="utf-8")

    return {
        "exists": True,
        "retention_days": retention_days,
        "cutoff_utc": cutoff.isoformat(),
        "pruned_rows": pruned_rows,
        "kept_rows": len(kept_lines),
    }


def resolve_manifest_from_pointer(run_root: Path, pointer_path: Path) -> dict[str, Any] | None:
    pointer = load_json(pointer_path)
    if not pointer:
        return None
    manifest_rel = pointer.get("manifest_path")
    if not isinstance(manifest_rel, str):
        return None
    manifest_path = run_root / manifest_rel if not Path(manifest_rel).is_absolute() else Path(manifest_rel)
    return load_json(manifest_path)


def flatten_artifact_metrics(payload: dict[str, Any], prefix: str = "") -> dict[str, float]:
    out: dict[str, float] = {}
    for key, value in payload.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, bool):
            out[path] = float(int(value))
        elif isinstance(value, (int, float)):
            out[path] = float(value)
        elif isinstance(value, dict):
            out.update(flatten_artifact_metrics(value, prefix=path))
    return out


def build_artifacts_delta(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    if previous is None:
        return {
            "has_previous": False,
            "changed_metric_count": 0,
            "metrics": {},
        }

    current_flat = flatten_artifact_metrics(current)
    previous_flat = flatten_artifact_metrics(previous)
    keys = sorted(set(current_flat) | set(previous_flat))

    metrics: dict[str, Any] = {}
    changed_count = 0
    for key in keys:
        cur = current_flat.get(key)
        prev = previous_flat.get(key)
        delta = None
        changed = cur != prev
        if cur is not None and prev is not None:
            delta = cur - prev
        if changed:
            changed_count += 1
        metrics[key] = {
            "current": cur,
            "previous": prev,
            "delta": delta,
            "changed": changed,
        }

    return {
        "has_previous": True,
        "changed_metric_count": changed_count,
        "metrics": metrics,
    }


def manifest_path_from_pointer(run_root: Path, pointer: dict[str, Any]) -> Path | None:
    manifest_rel = pointer.get("manifest_path")
    if not isinstance(manifest_rel, str):
        return None
    manifest_path = run_root / manifest_rel if not Path(manifest_rel).is_absolute() else Path(manifest_rel)
    return manifest_path


def summarize_run_manifest(
    project_root: Path,
    manifest: dict[str, Any],
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    failed_step_name = None
    failed_step = manifest.get("failed_step")
    if isinstance(failed_step, dict):
        failed_step_name = failed_step.get("name")

    if failed_step_name is None and isinstance(manifest.get("steps"), list):
        for step in manifest["steps"]:
            if isinstance(step, dict) and step.get("status") == "failed":
                failed_step_name = step.get("name")
                break

    summary: dict[str, Any] = {
        "run_id": manifest.get("run_id"),
        "status": manifest.get("status"),
        "mode": manifest.get("mode"),
        "started_at_utc": manifest.get("started_at_utc"),
        "ended_at_utc": manifest.get("ended_at_utc"),
        "duration_seconds": manifest.get("duration_seconds"),
        "failed_step_name": failed_step_name,
    }
    if manifest_path is not None:
        summary["manifest_path"] = rel_path(project_root, manifest_path)
    return summary


def load_recent_failures(history_path: Path, limit: int = 5) -> list[dict[str, Any]]:
    if not history_path.exists():
        return []

    lines = history_path.read_text(encoding="utf-8", errors="replace").splitlines()
    candidates = []
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
        candidates.append(
            {
                "timestamp_utc": payload.get("timestamp_utc"),
                "run_id": payload.get("run_id"),
                "mode": payload.get("mode"),
                "manifest_path": payload.get("manifest_path"),
            }
        )
        if len(candidates) >= limit:
            break

    return list(reversed(candidates))


def load_history_records(history_path: Path) -> list[dict[str, Any]]:
    if not history_path.exists():
        return []

    records: list[dict[str, Any]] = []
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
        ts = parse_iso_datetime(payload.get("timestamp_utc"))
        if ts is None:
            continue
        payload["_timestamp_utc"] = ts.isoformat()
        payload["_dt"] = ts
        records.append(payload)

    records.sort(key=lambda item: item["_dt"])
    return records


def scheduler_policy_from_env() -> dict[str, dict[str, Any]]:
    return {
        "incremental": {
            "expected_every_hours": env_int("BREATHWISE_SCHED_INCREMENTAL_EXPECTED_HOURS", default=6, minimum=1),
            "max_stale_hours": env_int("BREATHWISE_SCHED_INCREMENTAL_MAX_STALE_HOURS", default=12, minimum=1),
            "required": env_bool("BREATHWISE_SCHED_INCREMENTAL_REQUIRED", default=True),
        },
        "standard": {
            "expected_every_hours": env_int("BREATHWISE_SCHED_STANDARD_EXPECTED_HOURS", default=24, minimum=1),
            "max_stale_hours": env_int("BREATHWISE_SCHED_STANDARD_MAX_STALE_HOURS", default=36, minimum=1),
            "required": env_bool("BREATHWISE_SCHED_STANDARD_REQUIRED", default=True),
        },
        "full": {
            "expected_every_hours": env_int("BREATHWISE_SCHED_FULL_EXPECTED_HOURS", default=168, minimum=1),
            "max_stale_hours": env_int("BREATHWISE_SCHED_FULL_MAX_STALE_HOURS", default=240, minimum=1),
            "required": env_bool("BREATHWISE_SCHED_FULL_REQUIRED", default=False),
        },
    }


def latest_success_by_mode(history_records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for item in history_records:
        if item.get("status") != "succeeded":
            continue
        mode = item.get("mode")
        if not isinstance(mode, str):
            continue
        latest[mode] = item
    return latest


def latest_failure_record(history_records: list[dict[str, Any]]) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for item in history_records:
        if item.get("status") == "failed":
            latest = item
    return latest


def evaluate_scheduler_health(
    history_records: list[dict[str, Any]],
    policy: dict[str, dict[str, Any]],
    now_dt: datetime | None = None,
) -> dict[str, Any]:
    now_ref = now_dt or now_utc()
    latest_success = latest_success_by_mode(history_records)

    modes: dict[str, Any] = {}
    critical_modes: list[str] = []
    warning_modes: list[str] = []

    for mode, config in policy.items():
        expected_hours = int(config["expected_every_hours"])
        max_stale_hours = int(config["max_stale_hours"])
        required = bool(config["required"])
        latest = latest_success.get(mode)

        if latest is None:
            status = "missing_required" if required else "missing_optional"
            entry = {
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
            modes[mode] = entry
            if required:
                critical_modes.append(mode)
            continue

        latest_dt = latest.get("_dt")
        if not isinstance(latest_dt, datetime):
            continue

        age_hours = max(0.0, (now_ref - latest_dt).total_seconds() / 3600.0)
        next_expected = latest_dt + timedelta(hours=expected_hours)
        missed_run_estimate = max(0, int(age_hours // expected_hours) - 1)

        if age_hours > max_stale_hours:
            status = "stale"
            stale = True
            critical_modes.append(mode)
        elif age_hours > expected_hours:
            status = "late"
            stale = False
            warning_modes.append(mode)
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
            "missed_run_estimate": int(missed_run_estimate),
            "next_expected_by_utc": next_expected.isoformat(),
            "stale": stale,
        }

    if critical_modes:
        overall_status = "critical"
    elif warning_modes:
        overall_status = "warning"
    else:
        overall_status = "healthy"

    return {
        "evaluated_at_utc": now_ref.isoformat(),
        "overall_status": overall_status,
        "critical_modes": sorted(critical_modes),
        "warning_modes": sorted(warning_modes),
        "policy": policy,
        "modes": modes,
    }


def build_ops_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    latest_run = snapshot.get("latest_run") if isinstance(snapshot.get("latest_run"), dict) else {}
    latest_failure = (
        snapshot.get("latest_failure_run") if isinstance(snapshot.get("latest_failure_run"), dict) else {}
    )
    alerting = snapshot.get("alerting") if isinstance(snapshot.get("alerting"), dict) else {}
    latest_alert = alerting.get("latest_alert") if isinstance(alerting.get("latest_alert"), dict) else {}
    validation = snapshot.get("validation") if isinstance(snapshot.get("validation"), dict) else {}
    scheduler = snapshot.get("scheduler_health") if isinstance(snapshot.get("scheduler_health"), dict) else {}
    freshness = snapshot.get("freshness") if isinstance(snapshot.get("freshness"), dict) else {}

    return {
        "overall_status": scheduler.get("overall_status"),
        "latest_run_status": latest_run.get("status"),
        "latest_success_age_hours": freshness.get("latest_success_age_hours"),
        "latest_failure_age_hours": freshness.get("latest_failure_age_hours", latest_failure.get("age_hours")),
        "validation_passed": validation.get("passed"),
        "recent_failure_count": len(snapshot.get("recent_failures", []))
        if isinstance(snapshot.get("recent_failures"), list)
        else 0,
        "latest_alert_status": latest_alert.get("status"),
        "latest_alert_at_utc": latest_alert.get("timestamp_utc"),
    }


def load_alerting_snapshot(project_root: Path) -> dict[str, Any]:
    alerts_dir = project_root / "data" / "processed" / "alerts"
    latest_alert_path = alerts_dir / "latest_alert.json"
    state_path = alerts_dir / "alert_state.json"
    latest_alert = load_json(latest_alert_path) or {}
    state = load_json(state_path) or {}

    signatures = state.get("failure_signatures") if isinstance(state.get("failure_signatures"), dict) else {}
    signature_count = len(signatures) if isinstance(signatures, dict) else 0

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
        "tracked_signature_count": signature_count,
    }

    return {
        "alerts_dir": rel_path(project_root, alerts_dir),
        "latest_alert_path": rel_path(project_root, latest_alert_path),
        "state_path": rel_path(project_root, state_path),
        "latest_alert": latest_alert_summary,
        "state": state_summary,
    }


def build_ops_status_snapshot(
    project_root: Path,
    pipeline_root: Path,
    current_manifest_path: Path | None = None,
    current_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    latest_ptr_path = pipeline_root / "latest_run_manifest.json"
    latest_success_ptr_path = pipeline_root / "latest_success_run_manifest.json"

    latest_ptr = load_json(latest_ptr_path)
    latest_success_ptr = load_json(latest_success_ptr_path)

    latest_run_summary = None
    if current_manifest is not None:
        latest_run_summary = summarize_run_manifest(
            project_root=project_root,
            manifest=current_manifest,
            manifest_path=current_manifest_path,
        )
    elif isinstance(latest_ptr, dict):
        latest_manifest_path = manifest_path_from_pointer(pipeline_root, latest_ptr)
        if latest_manifest_path is not None:
            latest_manifest = load_json(latest_manifest_path)
            if latest_manifest:
                latest_run_summary = summarize_run_manifest(
                    project_root=project_root,
                    manifest=latest_manifest,
                    manifest_path=latest_manifest_path,
                )

    latest_success_summary = None
    if isinstance(latest_success_ptr, dict):
        latest_success_manifest_path = manifest_path_from_pointer(pipeline_root, latest_success_ptr)
        if latest_success_manifest_path is not None:
            latest_success_manifest = load_json(latest_success_manifest_path)
            if latest_success_manifest:
                latest_success_summary = summarize_run_manifest(
                    project_root=project_root,
                    manifest=latest_success_manifest,
                    manifest_path=latest_success_manifest_path,
                )

    validation_path = project_root / "data" / "processed" / "marts" / "validation_report.json"
    validation_payload = load_json(validation_path) or {}
    validation_summary = {
        "report_path": rel_path(project_root, validation_path),
        "passed": validation_payload.get("passed"),
        "check_count": validation_payload.get("check_count"),
        "failed_count": validation_payload.get("failed_count"),
        "generated_at_utc": validation_payload.get("generated_at_utc"),
    }

    lock_path = pipeline_root / "pipeline.lock"
    lock_payload = load_json(lock_path) if lock_path.exists() else None
    lock_active = False
    if isinstance(lock_payload, dict):
        pid = lock_payload.get("pid")
        if isinstance(pid, int):
            lock_active = is_pid_alive(pid)

    history_path = pipeline_root / "history.jsonl"
    history_records = load_history_records(history_path)
    scheduler_policy = scheduler_policy_from_env()
    scheduler_health = evaluate_scheduler_health(
        history_records=history_records,
        policy=scheduler_policy,
        now_dt=now_utc(),
    )
    latest_failure = latest_failure_record(history_records)

    latest_success_age_hours = None
    if isinstance(latest_success_summary, dict):
        ended_at = parse_iso_datetime(latest_success_summary.get("ended_at_utc"))
        if ended_at is not None:
            latest_success_age_hours = round(max(0.0, (now_utc() - ended_at).total_seconds() / 3600.0), 3)

    latest_failure_summary = None
    if isinstance(latest_failure, dict):
        failure_ts = latest_failure.get("_dt")
        age_hours = None
        if isinstance(failure_ts, datetime):
            age_hours = round(max(0.0, (now_utc() - failure_ts).total_seconds() / 3600.0), 3)
        latest_failure_summary = {
            "run_id": latest_failure.get("run_id"),
            "mode": latest_failure.get("mode"),
            "timestamp_utc": latest_failure.get("_timestamp_utc"),
            "manifest_path": latest_failure.get("manifest_path"),
            "age_hours": age_hours,
        }

    snapshot = {
        "generated_at_utc": iso_now(),
        "latest_run": latest_run_summary,
        "latest_success_run": latest_success_summary,
        "recent_failures": load_recent_failures(history_path, limit=5),
        "latest_failure_run": latest_failure_summary,
        "validation": validation_summary,
        "scheduler_health": scheduler_health,
        "freshness": {
            "latest_success_age_hours": latest_success_age_hours,
            "latest_failure_age_hours": latest_failure_summary.get("age_hours")
            if isinstance(latest_failure_summary, dict)
            else None,
        },
        "pipeline_lock": {
            "path": rel_path(project_root, lock_path),
            "active": lock_active,
            "payload": lock_payload,
        },
        "alerting": load_alerting_snapshot(project_root),
        "pointers": {
            "latest_run_manifest": rel_path(project_root, latest_ptr_path),
            "latest_success_run_manifest": rel_path(project_root, latest_success_ptr_path),
        },
    }
    snapshot["summary"] = build_ops_summary(snapshot)
    return snapshot


def write_ops_status_snapshot(project_root: Path, snapshot: dict[str, Any]) -> Path:
    path = project_root / "data" / "processed" / "ops" / "ops_status_latest.json"
    write_json(path, snapshot)
    return path


def load_alert_config(project_root: Path) -> AlertConfig:
    alerts_dir = resolve_path(
        project_root,
        os.getenv("BREATHWISE_ALERTS_DIR", "data/processed/alerts"),
    )
    webhook_url = os.getenv("BREATHWISE_ALERT_WEBHOOK_URL")
    webhook_url = webhook_url.strip() if isinstance(webhook_url, str) else None
    if webhook_url == "":
        webhook_url = None

    return AlertConfig(
        enabled=env_bool("BREATHWISE_ALERTS_ENABLED", default=False),
        on_success=env_bool("BREATHWISE_ALERT_ON_SUCCESS", default=False),
        on_failure=env_bool("BREATHWISE_ALERT_ON_FAILURE", default=True),
        alerts_dir=alerts_dir,
        webhook_url=webhook_url,
        webhook_timeout_seconds=env_int("BREATHWISE_ALERT_WEBHOOK_TIMEOUT_SECONDS", default=5, minimum=1),
        webhook_retries=env_int("BREATHWISE_ALERT_WEBHOOK_RETRIES", default=2, minimum=0),
        webhook_backoff_seconds=env_int("BREATHWISE_ALERT_WEBHOOK_BACKOFF_SECONDS", default=2, minimum=1),
        dedup_window_minutes=env_int("BREATHWISE_ALERT_DEDUP_WINDOW_MINUTES", default=60, minimum=1),
        repeat_every_failures=env_int("BREATHWISE_ALERT_REPEAT_EVERY_FAILURES", default=5, minimum=1),
        state_path=resolve_path(
            project_root,
            os.getenv("BREATHWISE_ALERT_STATE_PATH", "data/processed/alerts/alert_state.json"),
        ),
        mac_notify=env_bool("BREATHWISE_ALERT_MAC_NOTIFY", default=False),
    )


def should_emit_alert(config: AlertConfig, run_status: str) -> bool:
    if run_status == "failed":
        return config.on_failure
    if run_status == "succeeded":
        return config.on_success
    return False


def _send_webhook_once(url: str, payload: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status_code = getattr(response, "status", None)
            return {
                "sink": "webhook",
                "status": "sent",
                "http_status": status_code,
            }
    except urllib.error.HTTPError as exc:
        return {
            "sink": "webhook",
            "status": "failed",
            "error": f"HTTPError {exc.code}",
        }
    except urllib.error.URLError as exc:
        return {
            "sink": "webhook",
            "status": "failed",
            "error": f"URLError {exc.reason}",
        }
    except Exception as exc:
        return {
            "sink": "webhook",
            "status": "failed",
            "error": str(exc),
        }


def send_webhook_alert(
    url: str,
    payload: dict[str, Any],
    timeout_seconds: int,
    retries: int,
    backoff_seconds: int,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    max_attempts = retries + 1
    for attempt_index in range(max_attempts):
        attempt_number = attempt_index + 1
        result = _send_webhook_once(url=url, payload=payload, timeout_seconds=timeout_seconds)
        result["attempt"] = attempt_number
        attempts.append(result)
        if result.get("status") == "sent":
            return {
                "sink": "webhook",
                "status": "sent",
                "attempts": attempts,
                "attempt_count": attempt_number,
            }

        if attempt_number < max_attempts:
            sleep_seconds = backoff_seconds * (2**attempt_index)
            time.sleep(sleep_seconds)

    return {
        "sink": "webhook",
        "status": "failed",
        "attempts": attempts,
        "attempt_count": max_attempts,
    }


def load_alert_state(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    if payload is None:
        return {"failure_signatures": {}, "updated_at_utc": iso_now()}
    if not isinstance(payload.get("failure_signatures"), dict):
        payload["failure_signatures"] = {}
    return payload


def write_alert_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at_utc"] = iso_now()
    write_json(path, state)


def alert_failure_signature(record: dict[str, Any]) -> str:
    mode = record.get("mode") or "unknown"
    failed_step = record.get("failed_step_name") or "unknown"
    validation = record.get("validation_passed")
    return f"{mode}|{failed_step}|validation={validation}"


def evaluate_alert_escalation(
    record: dict[str, Any],
    config: AlertConfig,
    state: dict[str, Any],
) -> dict[str, Any]:
    status = record.get("status")
    if status != "failed":
        ts = parse_iso_datetime(record.get("timestamp_utc")) or now_utc()
        state["last_non_failure_alert_at_utc"] = ts.isoformat()
        return {
            "should_send": True,
            "reason": "non_failure_event",
            "signature": None,
        }

    signatures = state.setdefault("failure_signatures", {})
    signature = alert_failure_signature(record)
    sig_state = signatures.get(signature)
    if not isinstance(sig_state, dict):
        sig_state = {
            "last_sent_at_utc": None,
            "failure_count_since_last_send": 0,
        }

    sig_state["failure_count_since_last_send"] = int(sig_state.get("failure_count_since_last_send", 0)) + 1
    count_since_send = sig_state["failure_count_since_last_send"]

    now_dt = parse_iso_datetime(record.get("timestamp_utc")) or now_utc()
    last_sent_dt = parse_iso_datetime(sig_state.get("last_sent_at_utc"))
    within_window = False
    if last_sent_dt is not None:
        within_window = (now_dt - last_sent_dt) < timedelta(minutes=config.dedup_window_minutes)

    should_send = True
    reason = "first_failure_signature"
    repeat_threshold = max(0, int(config.repeat_every_failures) - 1)
    if within_window and count_since_send < repeat_threshold:
        should_send = False
        reason = "deduplicated_within_window"
    elif within_window and count_since_send >= repeat_threshold:
        should_send = True
        reason = "repeat_failure_threshold_reached"
    elif last_sent_dt is not None and not within_window:
        should_send = True
        reason = "dedup_window_elapsed"

    if should_send:
        sig_state["last_sent_at_utc"] = now_dt.isoformat()
        sig_state["failure_count_since_last_send"] = 0

    signatures[signature] = sig_state
    state["last_failure_signature"] = signature
    state["last_failure_at_utc"] = now_dt.isoformat()

    return {
        "should_send": should_send,
        "reason": reason,
        "signature": signature,
        "failure_count_since_last_send": count_since_send,
    }


def send_mac_notification(record: dict[str, Any]) -> dict[str, Any]:
    if shutil.which("osascript") is None:
        return {
            "sink": "mac_notification",
            "status": "skipped",
            "reason": "osascript_not_found",
        }

    run_id = str(record.get("run_id", "unknown"))
    status = str(record.get("status", "unknown")).upper()
    failed_step = record.get("failed_step_name")
    detail = f"run_id={run_id}"
    if failed_step:
        detail += f", failed_step={failed_step}"

    title = f"Breathwise Pipeline {status}".replace('"', "'")
    message = detail.replace('"', "'")
    script = f'display notification "{message}" with title "{title}"'

    try:
        completed = subprocess.run(
            ["osascript", "-e", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if completed.returncode == 0:
            return {"sink": "mac_notification", "status": "sent"}
        return {
            "sink": "mac_notification",
            "status": "failed",
            "error": completed.stderr.strip() or f"exit_code={completed.returncode}",
        }
    except Exception as exc:
        return {"sink": "mac_notification", "status": "failed", "error": str(exc)}


def emit_run_alert(
    project_root: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    config = load_alert_config(project_root)
    status = str(manifest.get("status", "unknown"))
    failed_step = manifest.get("failed_step")
    failed_step_name = failed_step.get("name") if isinstance(failed_step, dict) else None
    should_send = config.enabled and should_emit_alert(config, status)

    record = {
        "timestamp_utc": iso_now(),
        "run_id": manifest.get("run_id"),
        "status": status,
        "mode": manifest.get("mode"),
        "duration_seconds": manifest.get("duration_seconds"),
        "failed_step_name": failed_step_name,
        "manifest_path": rel_path(project_root, manifest_path),
        "validation_passed": (
            manifest.get("artifacts", {}).get("validation", {}).get("passed")
            if isinstance(manifest.get("artifacts"), dict)
            else None
        ),
    }

    result: dict[str, Any] = {
        "enabled": config.enabled,
        "should_send": should_send,
        "escalation": None,
        "channels": [],
        "record_path": None,
    }

    if not should_send:
        return result

    config.alerts_dir.mkdir(parents=True, exist_ok=True)
    config.state_path.parent.mkdir(parents=True, exist_ok=True)
    history_path = config.alerts_dir / "alerts_history.jsonl"
    latest_alert_path = config.alerts_dir / "latest_alert.json"
    state = load_alert_state(config.state_path)
    escalation = evaluate_alert_escalation(record=record, config=config, state=state)
    result["escalation"] = escalation
    write_alert_state(config.state_path, state)

    if not escalation.get("should_send", True):
        result["should_send"] = False
        result["channels"].append({"sink": "alert_policy", "status": "suppressed", "reason": escalation.get("reason")})
        return result

    append_jsonl(history_path, record)
    write_json(latest_alert_path, record)
    result["record_path"] = rel_path(project_root, history_path)
    result["channels"].append({"sink": "local_file", "status": "sent"})

    if config.webhook_url:
        result["channels"].append(
            send_webhook_alert(
                url=config.webhook_url,
                payload=record,
                timeout_seconds=config.webhook_timeout_seconds,
                retries=config.webhook_retries,
                backoff_seconds=config.webhook_backoff_seconds,
            )
        )

    if config.mac_notify:
        result["channels"].append(send_mac_notification(record))

    return result


def build_step_catalog(args: argparse.Namespace, python_bin: str) -> dict[str, StepCommand]:
    cams_cmd = [python_bin, "scripts/extract_cams_city_hourly_tr.py"]
    if args.cams_months:
        cams_cmd.extend(["--months", args.cams_months])
    if args.skip_cams_combined:
        cams_cmd.append("--skip-combined")

    refresh_mode = args.marts_refresh_mode
    if refresh_mode is None:
        refresh_mode = {
            "full": "full",
            "standard": "auto",
            "incremental": "incremental",
        }[args.mode]

    return {
        "historical_forecast_extended": StepCommand(
            name="historical_forecast_extended",
            command=[
                python_bin,
                "scripts/build_historical_forecast_extended.py",
                "--year",
                str(args.hf_year),
                "--min-city-count-per-month",
                str(args.hf_min_city_count_per_month),
            ],
            description="Build canonical extended historical forecast outputs.",
        ),
        "cams_extract": StepCommand(
            name="cams_extract",
            command=cams_cmd,
            description="Extract CAMS city-hourly monthly and combined outputs.",
        ),
        "marts_build": StepCommand(
            name="marts_build",
            command=[
                python_bin,
                "scripts/build_analytics_marts.py",
                "--refresh-mode",
                refresh_mode,
                "--incremental-lookback-hours",
                str(args.incremental_lookback_hours),
            ],
            description="Build/update analytics marts and mobile views.",
        ),
        "validate_outputs": StepCommand(
            name="validate_outputs",
            command=[python_bin, "scripts/validate_analytics_outputs.py"],
            description="Run strict analytics and manifest validation checks.",
        ),
        "export_contracts": StepCommand(
            name="export_contracts",
            command=[python_bin, "scripts/export_data_contracts.py"],
            description="Export API and parquet data contracts.",
        ),
        "api_smoke": StepCommand(
            name="api_smoke",
            command=[python_bin, "scripts/smoke_test_api.py"],
            description="Run local API smoke checks via TestClient.",
        ),
        "pytest": StepCommand(
            name="pytest",
            command=[python_bin, "-m", "pytest", "-q"],
            description="Run regression tests.",
        ),
    }


def mode_steps(mode: str) -> list[str]:
    if mode == "full":
        return [
            "historical_forecast_extended",
            "cams_extract",
            "marts_build",
            "validate_outputs",
            "export_contracts",
            "api_smoke",
            "pytest",
        ]

    if mode == "standard":
        return [
            "historical_forecast_extended",
            "cams_extract",
            "marts_build",
            "validate_outputs",
            "export_contracts",
            "api_smoke",
        ]

    if mode == "incremental":
        return [
            "marts_build",
            "validate_outputs",
            "export_contracts",
            "api_smoke",
        ]

    raise ValueError(f"Unsupported mode: {mode}")


def resolve_steps(args: argparse.Namespace, catalog: dict[str, StepCommand], run_root: Path) -> list[str]:
    if args.steps:
        selected = [item.strip() for item in args.steps.split(",") if item.strip()]
    else:
        selected = mode_steps(args.mode)

    if args.resume_from_latest_failed:
        latest_ptr = run_root / "latest_run_manifest.json"
        latest = load_json(latest_ptr)
        if not latest:
            raise RuntimeError("No latest run pointer found to resume from.")

        latest_status = latest.get("status")
        if latest_status != "failed":
            raise RuntimeError(
                "Latest run is not failed; cannot resume. "
                f"Current latest status: {latest_status!r}"
            )

        manifest_rel = latest.get("manifest_path")
        if not isinstance(manifest_rel, str):
            raise RuntimeError("latest_run_manifest.json is missing manifest_path.")

        manifest_path = run_root / manifest_rel if not Path(manifest_rel).is_absolute() else Path(manifest_rel)
        manifest = load_json(manifest_path)
        if not manifest:
            raise RuntimeError(f"Could not read latest run manifest: {manifest_path}")

        steps = manifest.get("steps", [])
        failed_step_name = None
        if isinstance(steps, list):
            for item in steps:
                if not isinstance(item, dict):
                    continue
                if item.get("status") == "failed":
                    failed_step_name = item.get("name")
                    break

        if not failed_step_name:
            # Backward-compatible fallback for older manifests if failed step details are absent.
            if isinstance(steps, list):
                for item in steps:
                    if not isinstance(item, dict):
                        continue
                    if item.get("status") == "pending":
                        failed_step_name = item.get("name")
                        break

        if not failed_step_name:
            raise RuntimeError("Latest failed run has no resumable failed/pending step.")

        if failed_step_name in selected:
            selected = selected[selected.index(failed_step_name) :]

    invalid = [name for name in selected if name not in catalog]
    if invalid:
        raise ValueError("Unknown step(s): " + ", ".join(invalid))

    if args.from_step:
        if args.from_step not in selected:
            raise ValueError(f"--from-step {args.from_step!r} not in selected steps")
        selected = selected[selected.index(args.from_step) :]

    if args.to_step:
        if args.to_step not in selected:
            raise ValueError(f"--to-step {args.to_step!r} not in selected steps")
        selected = selected[: selected.index(args.to_step) + 1]

    if args.skip_api_smoke:
        selected = [name for name in selected if name != "api_smoke"]

    if args.skip_tests:
        selected = [name for name in selected if name != "pytest"]

    if args.include_tests and "pytest" not in selected:
        selected.append("pytest")

    return selected


def summarize_artifacts(project_root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}

    hf_manifest_path = (
        project_root
        / "data"
        / "raw"
        / "open_meteo"
        / "historical_forecast"
        / "manifests"
        / "historical_forecast_extended_manifest.json"
    )
    hf_manifest = load_json(hf_manifest_path)
    if hf_manifest:
        stats = hf_manifest.get("stats", {}) if isinstance(hf_manifest.get("stats"), dict) else {}
        out["historical_forecast_extended"] = {
            "manifest_path": rel_path(project_root, hf_manifest_path),
            "validated_min_city_count": stats.get("validated_min_city_count"),
            "rows_after_dedup": stats.get("rows_after_dedup"),
            "city_count": stats.get("city_count"),
            "time_min": stats.get("time_min"),
            "time_max": stats.get("time_max"),
        }

    cams_manifest_path = project_root / "data" / "processed" / "cams" / "cams_city_hourly_manifest.json"
    cams_manifest = load_json(cams_manifest_path)
    if cams_manifest:
        available_months = cams_manifest.get("available_months")
        out["cams"] = {
            "manifest_path": rel_path(project_root, cams_manifest_path),
            "available_month_count": len(available_months) if isinstance(available_months, list) else None,
            "available_months": available_months,
            "combined_rows": cams_manifest.get("combined_rows"),
        }

    marts_manifest_path = project_root / "data" / "processed" / "marts" / "marts_build_manifest.json"
    marts_manifest = load_json(marts_manifest_path)
    if marts_manifest:
        marts = marts_manifest.get("marts", {}) if isinstance(marts_manifest.get("marts"), dict) else {}
        out["marts"] = {
            "manifest_path": rel_path(project_root, marts_manifest_path),
            "build_mode": marts_manifest.get("build_mode"),
            "refresh_start": marts_manifest.get("refresh_start"),
            "city_hourly_environment_rows": marts.get("city_hourly_environment_tr", {}).get("row_count")
            if isinstance(marts.get("city_hourly_environment_tr"), dict)
            else None,
            "city_forecast_vs_actual_rows": marts.get("city_forecast_vs_actual_tr", {}).get("row_count")
            if isinstance(marts.get("city_forecast_vs_actual_tr"), dict)
            else None,
        }

    validation_path = project_root / "data" / "processed" / "marts" / "validation_report.json"
    validation = load_json(validation_path)
    if validation:
        out["validation"] = {
            "report_path": rel_path(project_root, validation_path),
            "passed": validation.get("passed"),
            "check_count": validation.get("check_count"),
            "failed_count": validation.get("failed_count"),
        }

    contracts_summary: dict[str, Any] = {}

    parquet_contracts_path = project_root / "data" / "contracts" / "parquet_contracts.json"
    parquet_contracts = load_json(parquet_contracts_path)
    if parquet_contracts:
        datasets = (
            parquet_contracts.get("datasets", {})
            if isinstance(parquet_contracts.get("datasets"), dict)
            else {}
        )
        contracts_summary["parquet_manifest_path"] = rel_path(project_root, parquet_contracts_path)
        contracts_summary["dataset_contract_count"] = len(datasets)

    i18n_contract_path = project_root / "data" / "contracts" / "i18n_contract.json"
    i18n_contract = load_json(i18n_contract_path)
    if i18n_contract:
        supported_locales = i18n_contract.get("supported_locales")
        translation_keys = (
            i18n_contract.get("translation_keys", {})
            if isinstance(i18n_contract.get("translation_keys"), dict)
            else {}
        )
        contracts_summary["i18n_contract_path"] = rel_path(project_root, i18n_contract_path)
        contracts_summary["supported_locales"] = (
            supported_locales if isinstance(supported_locales, list) else None
        )
        contracts_summary["translation_key_count"] = len(translation_keys)

    product_shell_contract_path = project_root / "data" / "contracts" / "product_shell_view_models.json"
    product_shell_contract = load_json(product_shell_contract_path)
    if product_shell_contract:
        screens = product_shell_contract.get("screens")
        contracts_summary["product_shell_contract_path"] = rel_path(project_root, product_shell_contract_path)
        contracts_summary["product_shell_screen_count"] = len(screens) if isinstance(screens, list) else None

    if contracts_summary:
        out["contracts"] = contracts_summary

    return out


def run_step(
    project_root: Path,
    run_dir: Path,
    step_index: int,
    total_steps: int,
    step: StepCommand,
    quiet: bool,
    events_path: Path,
) -> dict[str, Any]:
    started = now_utc()
    log_path = run_dir / f"{step_index:02d}_{step.name}.log"

    append_jsonl(
        events_path,
        {
            "timestamp_utc": started.isoformat(),
            "event": "step_started",
            "step": step.name,
            "command": step.command,
            "log_path": log_path.name,
        },
    )

    if not quiet:
        print(f"[{step_index}/{total_steps}] {step.name}: {step.description}")
        print(f"  $ {command_display(step.command)}")

    with log_path.open("w", encoding="utf-8") as log_handle:
        log_handle.write(f"$ {command_display(step.command)}\n")

        process = subprocess.Popen(
            step.command,
            cwd=project_root.as_posix(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert process.stdout is not None
        for line in process.stdout:
            log_handle.write(line)
            if not quiet:
                print(f"    {line}", end="")

        process.wait()
        exit_code = int(process.returncode)

    ended = now_utc()
    duration_seconds = (ended - started).total_seconds()

    status = "succeeded" if exit_code == 0 else "failed"
    error_tail = tail_text(log_path, max_lines=60) if exit_code != 0 else None

    append_jsonl(
        events_path,
        {
            "timestamp_utc": ended.isoformat(),
            "event": "step_finished",
            "step": step.name,
            "status": status,
            "exit_code": exit_code,
            "duration_seconds": duration_seconds,
        },
    )

    return {
        "name": step.name,
        "description": step.description,
        "status": status,
        "command": step.command,
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "duration_seconds": duration_seconds,
        "exit_code": exit_code,
        "log_path": log_path.name,
        "error_tail": error_tail,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Breathwise TR local-first data pipeline in orchestrated modes with "
            "structured run manifests and step logs."
        )
    )
    parser.add_argument("--mode", choices=["full", "standard", "incremental"], default="standard")
    parser.add_argument(
        "--steps",
        default=None,
        help=(
            "Optional comma-separated explicit step list. Overrides mode defaults. "
            "Valid steps: historical_forecast_extended,cams_extract,marts_build,"
            "validate_outputs,export_contracts,api_smoke,pytest"
        ),
    )
    parser.add_argument("--from-step", default=None)
    parser.add_argument("--to-step", default=None)
    parser.add_argument("--resume-from-latest-failed", action="store_true")

    parser.add_argument("--hf-year", type=int, default=2024)
    parser.add_argument("--hf-min-city-count-per-month", type=int, default=81)
    parser.add_argument("--cams-months", default=None, help="Optional CAMS month filter: 2024_01,2024_02")
    parser.add_argument("--skip-cams-combined", action="store_true")

    parser.add_argument(
        "--marts-refresh-mode",
        choices=["auto", "full", "incremental"],
        default=None,
        help="Override refresh mode passed to build_analytics_marts.py",
    )
    parser.add_argument("--incremental-lookback-hours", type=int, default=96)

    parser.add_argument("--skip-api-smoke", action="store_true")
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--include-tests", action="store_true", help="Include pytest even if mode omits it.")
    parser.add_argument(
        "--allow-concurrent",
        action="store_true",
        help="Bypass lock guard and allow concurrent pipeline runs (not recommended).",
    )
    parser.add_argument(
        "--prune-old-runs",
        action="store_true",
        help="After run finalization, prune old pipeline run directories under data/processed/pipeline_runs.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Retention horizon used with --prune-old-runs.",
    )

    parser.add_argument("--dry-run", action="store_true", help="Print planned steps but do not execute.")
    parser.add_argument("--quiet", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.incremental_lookback_hours <= 0:
        raise ValueError("--incremental-lookback-hours must be positive")
    if args.retention_days <= 0:
        raise ValueError("--retention-days must be positive")

    project_root = discover_project_root(Path(__file__))
    python_bin = sys.executable

    pipeline_root = project_root / "data" / "processed" / "pipeline_runs"
    pipeline_root.mkdir(parents=True, exist_ok=True)

    catalog = build_step_catalog(args, python_bin=python_bin)
    selected_names = resolve_steps(args, catalog=catalog, run_root=pipeline_root)

    if not selected_names:
        raise RuntimeError("No steps selected after applying filters.")

    run_started = now_utc()
    run_id = run_started.strftime("%Y%m%dT%H%M%SZ")
    lock_path = pipeline_root / "pipeline.lock"
    lock_payload = acquire_run_lock(lock_path, run_id=run_id, allow_concurrent=args.allow_concurrent)
    previous_success = resolve_manifest_from_pointer(
        run_root=pipeline_root,
        pointer_path=pipeline_root / "latest_success_run_manifest.json",
    )
    previous_success_artifacts = (
        previous_success.get("artifacts")
        if isinstance(previous_success, dict) and isinstance(previous_success.get("artifacts"), dict)
        else None
    )
    previous_success_run_id = (
        previous_success.get("run_id") if isinstance(previous_success, dict) and isinstance(previous_success.get("run_id"), str) else None
    )

    run_dir = pipeline_root / run_id
    try:
        run_dir.mkdir(parents=True, exist_ok=True)

        events_path = run_dir / "events.jsonl"
        manifest_path = run_dir / "run_manifest.json"

        manifest: dict[str, Any] = {
            "run_id": run_id,
            "mode": args.mode,
            "status": "running",
            "started_at_utc": run_started.isoformat(),
            "ended_at_utc": None,
            "duration_seconds": None,
            "project_root": project_root.as_posix(),
            "python": python_bin,
            "args": vars(args),
            "selected_steps": selected_names,
            "steps": [
                {
                    "name": name,
                    "description": catalog[name].description,
                    "status": "pending",
                    "command": catalog[name].command,
                }
                for name in selected_names
            ],
            "artifacts": {},
            "run_management": {
                "lock_enabled": not args.allow_concurrent,
                "lock_path": rel_path(project_root, lock_path),
                "lock_pid": lock_payload.get("pid") if isinstance(lock_payload, dict) else None,
                "previous_success_run_id": previous_success_run_id,
            },
            "run_dir": rel_path(project_root, run_dir),
            "events_path": rel_path(project_root, events_path),
        }

        write_json(manifest_path, manifest)
        append_jsonl(
            events_path,
            {
                "timestamp_utc": run_started.isoformat(),
                "event": "run_started",
                "run_id": run_id,
                "mode": args.mode,
                "selected_steps": selected_names,
            },
        )
        append_jsonl(
            events_path,
            {
                "timestamp_utc": iso_now(),
                "event": "lock_acquired",
                "lock_path": lock_path.name,
                "lock_pid": lock_payload.get("pid") if isinstance(lock_payload, dict) else None,
                "allow_concurrent": args.allow_concurrent,
            },
        )

        if args.dry_run:
            print("Planned pipeline steps:")
            for idx, name in enumerate(selected_names, start=1):
                step = catalog[name]
                print(f"{idx}. {step.name}: {step.description}")
                print(f"   $ {command_display(step.command)}")

            manifest["status"] = "dry_run"
            manifest["ended_at_utc"] = iso_now()
            manifest["duration_seconds"] = (now_utc() - run_started).total_seconds()
            write_json(manifest_path, manifest)
            latest_ptr = {
                "run_id": run_id,
                "manifest_path": rel_path(pipeline_root, manifest_path),
                "status": manifest["status"],
                "updated_at_utc": iso_now(),
            }
            write_json(pipeline_root / "latest_run_manifest.json", latest_ptr)

            ops_snapshot = build_ops_status_snapshot(
                project_root=project_root,
                pipeline_root=pipeline_root,
                current_manifest_path=manifest_path,
                current_manifest=manifest,
            )
            ops_status_path = write_ops_status_snapshot(project_root=project_root, snapshot=ops_snapshot)
            manifest["run_management"]["ops_status_path"] = rel_path(project_root, ops_status_path)
            write_json(manifest_path, manifest)
            return

        completed_steps: list[dict[str, Any]] = []
        failed_step: dict[str, Any] | None = None

        try:
            for idx, step_name in enumerate(selected_names, start=1):
                step_result = run_step(
                    project_root=project_root,
                    run_dir=run_dir,
                    step_index=idx,
                    total_steps=len(selected_names),
                    step=catalog[step_name],
                    quiet=args.quiet,
                    events_path=events_path,
                )
                completed_steps.append(step_result)

                manifest["steps"] = completed_steps + [
                    {
                        "name": name,
                        "description": catalog[name].description,
                        "status": "pending",
                        "command": catalog[name].command,
                    }
                    for name in selected_names[idx:]
                ]
                write_json(manifest_path, manifest)

                if step_result["status"] != "succeeded":
                    failed_step = step_result
                    break

        except Exception as exc:
            failed_step = {
                "name": "pipeline_internal_error",
                "status": "failed",
                "description": "Unexpected orchestration failure",
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }

        run_ended = now_utc()
        duration_seconds = (run_ended - run_started).total_seconds()

        manifest["ended_at_utc"] = run_ended.isoformat()
        manifest["duration_seconds"] = duration_seconds
        manifest["artifacts"] = summarize_artifacts(project_root)
        manifest["artifacts_delta"] = build_artifacts_delta(
            current=manifest["artifacts"],
            previous=previous_success_artifacts,
        )

        if failed_step is None:
            manifest["status"] = "succeeded"
            append_jsonl(
                events_path,
                {
                    "timestamp_utc": run_ended.isoformat(),
                    "event": "run_succeeded",
                    "run_id": run_id,
                    "duration_seconds": duration_seconds,
                },
            )
            exit_code = 0
        else:
            manifest["status"] = "failed"
            manifest["failed_step"] = failed_step
            append_jsonl(
                events_path,
                {
                    "timestamp_utc": run_ended.isoformat(),
                    "event": "run_failed",
                    "run_id": run_id,
                    "duration_seconds": duration_seconds,
                    "failed_step": failed_step,
                },
            )
            exit_code = 1

        write_json(manifest_path, manifest)

        latest_ptr = {
            "run_id": run_id,
            "manifest_path": rel_path(pipeline_root, manifest_path),
            "status": manifest["status"],
            "updated_at_utc": iso_now(),
        }
        write_json(pipeline_root / "latest_run_manifest.json", latest_ptr)

        if manifest["status"] == "succeeded":
            latest_success_ptr = {
                "run_id": run_id,
                "manifest_path": rel_path(pipeline_root, manifest_path),
                "status": manifest["status"],
                "updated_at_utc": iso_now(),
            }
            write_json(pipeline_root / "latest_success_run_manifest.json", latest_success_ptr)

        if args.prune_old_runs:
            try:
                keep_ids = {run_id}
                if previous_success_run_id:
                    keep_ids.add(previous_success_run_id)
                prune_result = prune_old_runs(
                    pipeline_root=pipeline_root,
                    retention_days=args.retention_days,
                    keep_run_ids=keep_ids,
                )
                manifest["run_management"]["prune"] = prune_result
                history_prune_result = prune_history_ledger(
                    history_path=pipeline_root / "history.jsonl",
                    retention_days=args.retention_days,
                    keep_run_ids=keep_ids,
                )
                manifest["run_management"]["history_prune"] = history_prune_result
                append_jsonl(
                    events_path,
                    {
                        "timestamp_utc": iso_now(),
                        "event": "run_pruned",
                        "prune_result": prune_result,
                        "history_prune_result": history_prune_result,
                    },
                )
            except Exception as exc:
                prune_error = {
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
                manifest["run_management"]["prune_error"] = prune_error
                append_jsonl(
                    events_path,
                    {
                        "timestamp_utc": iso_now(),
                        "event": "run_prune_failed",
                        "error": str(exc),
                    },
                )
            write_json(manifest_path, manifest)

        append_jsonl(
            pipeline_root / "history.jsonl",
            {
                "timestamp_utc": iso_now(),
                "run_id": run_id,
                "mode": args.mode,
                "status": manifest["status"],
                "manifest_path": rel_path(project_root, manifest_path),
                "duration_seconds": duration_seconds,
                "selected_steps": selected_names,
            },
        )

        ops_snapshot = build_ops_status_snapshot(
            project_root=project_root,
            pipeline_root=pipeline_root,
            current_manifest_path=manifest_path,
            current_manifest=manifest,
        )
        ops_status_path = write_ops_status_snapshot(project_root=project_root, snapshot=ops_snapshot)
        manifest["run_management"]["ops_status_path"] = rel_path(project_root, ops_status_path)
        append_jsonl(
            events_path,
            {
                "timestamp_utc": iso_now(),
                "event": "ops_status_updated",
                "ops_status_path": manifest["run_management"]["ops_status_path"],
            },
        )

        try:
            alert_summary = emit_run_alert(project_root=project_root, manifest_path=manifest_path, manifest=manifest)
            manifest["alerting"] = alert_summary
            append_jsonl(
                events_path,
                {
                    "timestamp_utc": iso_now(),
                    "event": "alert_processed",
                    "alerting": alert_summary,
                },
            )
        except Exception as exc:
            manifest["alerting"] = {
                "enabled": True,
                "should_send": False,
                "channels": [],
                "error": str(exc),
            }
            append_jsonl(
                events_path,
                {
                    "timestamp_utc": iso_now(),
                    "event": "alert_processing_failed",
                    "error": str(exc),
                },
            )

        write_json(manifest_path, manifest)

        print(f"Run manifest: {manifest_path}")
        print(f"Status: {manifest['status']}")
        if failed_step is not None:
            print(f"Failed step: {failed_step.get('name')}")

        raise SystemExit(exit_code)
    finally:
        release_run_lock(lock_path, allow_concurrent=args.allow_concurrent)


if __name__ == "__main__":
    main()
