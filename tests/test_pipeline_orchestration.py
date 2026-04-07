from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if SCRIPTS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR.as_posix())

import run_pipeline as rp  # noqa: E402


def build_args(**overrides: object) -> argparse.Namespace:
    base = {
        "mode": "standard",
        "steps": None,
        "from_step": None,
        "to_step": None,
        "resume_from_latest_failed": False,
        "hf_year": 2024,
        "hf_min_city_count_per_month": 81,
        "cams_months": None,
        "skip_cams_combined": False,
        "marts_refresh_mode": None,
        "incremental_lookback_hours": 96,
        "skip_api_smoke": False,
        "skip_tests": False,
        "include_tests": False,
        "allow_concurrent": False,
        "prune_old_runs": False,
        "retention_days": 30,
        "dry_run": False,
        "quiet": True,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_mode_step_resolution_standard() -> None:
    args = build_args(mode="standard")
    catalog = rp.build_step_catalog(args, python_bin="python3")

    selected = rp.resolve_steps(args, catalog=catalog, run_root=PROJECT_ROOT / "data" / "processed" / "pipeline_runs")

    assert selected == [
        "historical_forecast_extended",
        "cams_extract",
        "marts_build",
        "validate_outputs",
        "export_contracts",
        "api_smoke",
    ]


def test_mode_step_resolution_with_from_to() -> None:
    args = build_args(mode="full", from_step="marts_build", to_step="export_contracts")
    catalog = rp.build_step_catalog(args, python_bin="python3")

    selected = rp.resolve_steps(args, catalog=catalog, run_root=PROJECT_ROOT / "data" / "processed" / "pipeline_runs")

    assert selected == ["marts_build", "validate_outputs", "export_contracts"]


def test_resume_rejects_when_latest_not_failed(tmp_path: Path) -> None:
    run_root = tmp_path / "pipeline_runs"
    run_root.mkdir(parents=True, exist_ok=True)

    latest_manifest = run_root / "20260406T000000Z" / "run_manifest.json"
    latest_manifest.parent.mkdir(parents=True, exist_ok=True)
    latest_manifest.write_text(
        json.dumps(
            {
                "run_id": "20260406T000000Z",
                "status": "succeeded",
                "steps": [{"name": "marts_build", "status": "succeeded"}],
            }
        ),
        encoding="utf-8",
    )

    (run_root / "latest_run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "20260406T000000Z",
                "manifest_path": "20260406T000000Z/run_manifest.json",
                "status": "succeeded",
            }
        ),
        encoding="utf-8",
    )

    args = build_args(mode="standard", resume_from_latest_failed=True)
    catalog = rp.build_step_catalog(args, python_bin="python3")

    with pytest.raises(RuntimeError):
        rp.resolve_steps(args, catalog=catalog, run_root=run_root)


def test_build_artifacts_delta_changes_detected() -> None:
    current = {
        "validation": {"passed": True, "check_count": 41, "failed_count": 0},
        "marts": {"city_hourly_environment_rows": 756216},
    }
    previous = {
        "validation": {"passed": True, "check_count": 40, "failed_count": 1},
        "marts": {"city_hourly_environment_rows": 755000},
    }

    delta = rp.build_artifacts_delta(current=current, previous=previous)

    assert delta["has_previous"] is True
    assert delta["changed_metric_count"] >= 1
    assert delta["metrics"]["validation.check_count"]["delta"] == 1.0
    assert delta["metrics"]["marts.city_hourly_environment_rows"]["delta"] == 1216.0


def test_run_lock_blocks_active_process(tmp_path: Path) -> None:
    lock_path = tmp_path / "pipeline.lock"
    lock_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "run_id": "existing_run",
                "acquired_at_utc": "2026-04-06T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError):
        rp.acquire_run_lock(lock_path=lock_path, run_id="new_run", allow_concurrent=False)


def test_run_lock_replaces_stale_lock(tmp_path: Path) -> None:
    lock_path = tmp_path / "pipeline.lock"
    lock_path.write_text(
        json.dumps(
            {
                "pid": 999999,
                "run_id": "stale_run",
                "acquired_at_utc": "2026-04-06T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    payload = rp.acquire_run_lock(lock_path=lock_path, run_id="fresh_run", allow_concurrent=False)
    assert isinstance(payload, dict)
    assert payload.get("run_id") == "fresh_run"

    written = json.loads(lock_path.read_text(encoding="utf-8"))
    assert written.get("run_id") == "fresh_run"

    rp.release_run_lock(lock_path=lock_path, allow_concurrent=False)
    assert not lock_path.exists()


def test_should_emit_alert_logic(tmp_path: Path) -> None:
    config = rp.AlertConfig(
        enabled=True,
        on_success=False,
        on_failure=True,
        alerts_dir=tmp_path,
        webhook_url=None,
        webhook_timeout_seconds=5,
        webhook_retries=0,
        webhook_backoff_seconds=1,
        dedup_window_minutes=60,
        repeat_every_failures=3,
        state_path=tmp_path / "alert_state.json",
        mac_notify=False,
    )

    assert rp.should_emit_alert(config, "failed") is True
    assert rp.should_emit_alert(config, "succeeded") is False
    assert rp.should_emit_alert(config, "dry_run") is False


def test_build_ops_status_snapshot_reads_recent_failure(tmp_path: Path) -> None:
    project_root = tmp_path
    pipeline_root = project_root / "data" / "processed" / "pipeline_runs"
    pipeline_root.mkdir(parents=True, exist_ok=True)
    marts_dir = project_root / "data" / "processed" / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)

    run_dir = pipeline_root / "20260406T000000Z"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_manifest = run_dir / "run_manifest.json"
    run_manifest.write_text(
        json.dumps(
            {
                "run_id": "20260406T000000Z",
                "status": "failed",
                "mode": "standard",
                "started_at_utc": "2026-04-06T00:00:00+00:00",
                "ended_at_utc": "2026-04-06T00:10:00+00:00",
                "duration_seconds": 600,
                "failed_step": {"name": "validate_outputs"},
            }
        ),
        encoding="utf-8",
    )
    (pipeline_root / "latest_run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "20260406T000000Z",
                "manifest_path": "20260406T000000Z/run_manifest.json",
                "status": "failed",
            }
        ),
        encoding="utf-8",
    )
    (pipeline_root / "history.jsonl").write_text(
        json.dumps(
            {
                "timestamp_utc": "2026-04-06T00:10:00+00:00",
                "run_id": "20260406T000000Z",
                "status": "failed",
                "mode": "standard",
                "manifest_path": "data/processed/pipeline_runs/20260406T000000Z/run_manifest.json",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (marts_dir / "validation_report.json").write_text(
        json.dumps({"passed": False, "check_count": 40, "failed_count": 1}),
        encoding="utf-8",
    )
    alerts_dir = project_root / "data" / "processed" / "alerts"
    alerts_dir.mkdir(parents=True, exist_ok=True)
    (alerts_dir / "latest_alert.json").write_text(
        json.dumps(
            {
                "timestamp_utc": "2026-04-06T00:11:00+00:00",
                "run_id": "20260406T000000Z",
                "status": "failed",
                "mode": "standard",
                "failed_step_name": "validate_outputs",
            }
        ),
        encoding="utf-8",
    )
    (alerts_dir / "alert_state.json").write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-06T00:12:00+00:00",
                "last_failure_signature": "standard|validate_outputs|validation=False",
                "last_failure_at_utc": "2026-04-06T00:11:00+00:00",
                "failure_signatures": {
                    "standard|validate_outputs|validation=False": {
                        "last_sent_at_utc": "2026-04-06T00:11:00+00:00",
                        "failure_count_since_last_send": 0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    snapshot = rp.build_ops_status_snapshot(project_root=project_root, pipeline_root=pipeline_root)

    assert snapshot["latest_run"]["status"] == "failed"
    assert len(snapshot["recent_failures"]) == 1
    assert snapshot["validation"]["passed"] is False
    assert snapshot["alerting"]["latest_alert"]["status"] == "failed"
    assert snapshot["summary"]["latest_alert_status"] == "failed"


def test_prune_history_ledger_removes_old_rows(tmp_path: Path) -> None:
    history_path = tmp_path / "history.jsonl"
    old_ts = (rp.now_utc() - timedelta(days=90)).isoformat()
    recent_ts = rp.now_utc().isoformat()
    history_path.write_text(
        "\n".join(
            [
                json.dumps({"timestamp_utc": old_ts, "run_id": "old_run", "status": "succeeded"}),
                json.dumps({"timestamp_utc": recent_ts, "run_id": "recent_run", "status": "failed"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = rp.prune_history_ledger(history_path=history_path, retention_days=30, keep_run_ids={"recent_run"})
    assert result["pruned_rows"] == 1

    lines = [line for line in history_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["run_id"] == "recent_run"


def test_evaluate_scheduler_health_detects_stale_mode() -> None:
    now_ref = rp.now_utc()
    history = [
        {
            "run_id": "r1",
            "mode": "incremental",
            "status": "succeeded",
            "_dt": now_ref - timedelta(hours=20),
        },
        {
            "run_id": "r2",
            "mode": "standard",
            "status": "succeeded",
            "_dt": now_ref - timedelta(hours=10),
        },
    ]
    policy = {
        "incremental": {"expected_every_hours": 6, "max_stale_hours": 12, "required": True},
        "standard": {"expected_every_hours": 24, "max_stale_hours": 36, "required": True},
        "full": {"expected_every_hours": 168, "max_stale_hours": 240, "required": False},
    }

    health = rp.evaluate_scheduler_health(history_records=history, policy=policy, now_dt=now_ref)
    assert health["overall_status"] == "critical"
    assert "incremental" in health["critical_modes"]
    assert health["modes"]["incremental"]["status"] == "stale"


def test_build_ops_summary_uses_freshness_fields() -> None:
    summary = rp.build_ops_summary(
        {
            "latest_run": {"status": "succeeded"},
            "latest_failure_run": {"age_hours": 5.0},
            "alerting": {"latest_alert": {"status": "succeeded", "timestamp_utc": "2026-04-06T00:00:00+00:00"}},
            "validation": {"passed": True},
            "scheduler_health": {"overall_status": "healthy"},
            "freshness": {"latest_success_age_hours": 1.25, "latest_failure_age_hours": 5.0},
            "recent_failures": [],
        }
    )

    assert summary["latest_success_age_hours"] == 1.25
    assert summary["latest_failure_age_hours"] == 5.0
    assert summary["overall_status"] == "healthy"
    assert summary["latest_alert_status"] == "succeeded"


def test_alert_escalation_deduplicates_failures(tmp_path: Path) -> None:
    config = rp.AlertConfig(
        enabled=True,
        on_success=False,
        on_failure=True,
        alerts_dir=tmp_path,
        webhook_url=None,
        webhook_timeout_seconds=5,
        webhook_retries=0,
        webhook_backoff_seconds=1,
        dedup_window_minutes=120,
        repeat_every_failures=3,
        state_path=tmp_path / "alert_state.json",
        mac_notify=False,
    )
    state = {"failure_signatures": {}}
    record = {
        "timestamp_utc": rp.iso_now(),
        "run_id": "run1",
        "status": "failed",
        "mode": "incremental",
        "failed_step_name": "validate_outputs",
        "validation_passed": False,
    }

    first = rp.evaluate_alert_escalation(record=record, config=config, state=state)
    second = rp.evaluate_alert_escalation(record=record, config=config, state=state)
    third = rp.evaluate_alert_escalation(record=record, config=config, state=state)

    assert first["should_send"] is True
    assert second["should_send"] is False
    assert third["should_send"] is True
    assert third["reason"] == "repeat_failure_threshold_reached"


def test_send_webhook_alert_retries(monkeypatch) -> None:
    call_count = {"n": 0}

    def fake_send_once(url: str, payload: dict[str, object], timeout_seconds: int) -> dict[str, object]:
        _ = url, payload, timeout_seconds
        call_count["n"] += 1
        if call_count["n"] < 3:
            return {"sink": "webhook", "status": "failed", "error": "boom"}
        return {"sink": "webhook", "status": "sent", "http_status": 200}

    monkeypatch.setattr(rp, "_send_webhook_once", fake_send_once)
    monkeypatch.setattr(rp.time, "sleep", lambda *_args, **_kwargs: None)

    result = rp.send_webhook_alert(
        url="https://example.com/hook",
        payload={"ok": True},
        timeout_seconds=1,
        retries=3,
        backoff_seconds=1,
    )
    assert result["status"] == "sent"
    assert result["attempt_count"] == 3
