from __future__ import annotations

import argparse
import json
from pathlib import Path

from _shared import discover_project_root
from run_pipeline import build_ops_status_snapshot, write_ops_status_snapshot


def render_text(snapshot: dict[str, object]) -> str:
    latest_run = snapshot.get("latest_run") if isinstance(snapshot.get("latest_run"), dict) else {}
    latest_success = (
        snapshot.get("latest_success_run") if isinstance(snapshot.get("latest_success_run"), dict) else {}
    )
    validation = snapshot.get("validation") if isinstance(snapshot.get("validation"), dict) else {}
    recent_failures = (
        snapshot.get("recent_failures")
        if isinstance(snapshot.get("recent_failures"), list)
        else []
    )
    pipeline_lock = snapshot.get("pipeline_lock") if isinstance(snapshot.get("pipeline_lock"), dict) else {}
    scheduler = snapshot.get("scheduler_health") if isinstance(snapshot.get("scheduler_health"), dict) else {}
    modes = scheduler.get("modes") if isinstance(scheduler.get("modes"), dict) else {}
    summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
    alerting = snapshot.get("alerting") if isinstance(snapshot.get("alerting"), dict) else {}
    latest_alert = alerting.get("latest_alert") if isinstance(alerting.get("latest_alert"), dict) else {}
    alert_state = alerting.get("state") if isinstance(alerting.get("state"), dict) else {}

    lines = [
        "Breathwise TR Ops Status",
        f"- generated_at_utc: {snapshot.get('generated_at_utc')}",
        f"- overall_status: {summary.get('overall_status')}",
        f"- latest_run: id={latest_run.get('run_id')} status={latest_run.get('status')} mode={latest_run.get('mode')}",
        (
            "- latest_success_run: "
            f"id={latest_success.get('run_id')} status={latest_success.get('status')} mode={latest_success.get('mode')}"
        ),
        (
            "- validation: "
            f"passed={validation.get('passed')} failed_count={validation.get('failed_count')} "
            f"check_count={validation.get('check_count')}"
        ),
        f"- pipeline_lock_active: {pipeline_lock.get('active')}",
        f"- recent_failures_count: {len(recent_failures)}",
        (
            "- latest_alert: "
            f"status={latest_alert.get('status')} "
            f"run_id={latest_alert.get('run_id')} "
            f"at={latest_alert.get('timestamp_utc')}"
        ),
        (
            "- alert_state: "
            f"tracked_signatures={alert_state.get('tracked_signature_count')} "
            f"last_failure_at={alert_state.get('last_failure_at_utc')}"
        ),
        "- scheduler_health:",
    ]

    header = "  mode         status            age_h    expected_h  max_stale_h  missed_est"
    lines.append(header)
    for mode in ["incremental", "standard", "full"]:
        entry = modes.get(mode) if isinstance(modes.get(mode), dict) else {}
        lines.append(
            "  "
            f"{mode:<12} "
            f"{str(entry.get('status')):<16} "
            f"{str(entry.get('age_hours')):<8} "
            f"{str(entry.get('expected_every_hours')):<11} "
            f"{str(entry.get('max_stale_hours')):<12} "
            f"{str(entry.get('missed_run_estimate'))}"
        )

    if recent_failures:
        lines.append("- recent_failures:")
        for item in recent_failures[-3:]:
            if not isinstance(item, dict):
                continue
            lines.append(
                "  "
                f"{item.get('timestamp_utc')} "
                f"run_id={item.get('run_id')} "
                f"mode={item.get('mode')}"
            )

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print and optionally persist lightweight Breathwise operational status summary."
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format for stdout.",
    )
    parser.add_argument(
        "--write-file",
        action="store_true",
        help="Write snapshot to data/processed/ops/ops_status_latest.json.",
    )
    parser.add_argument(
        "--fail-on-unhealthy",
        action="store_true",
        help="Exit non-zero when scheduler health overall_status is not healthy.",
    )
    parser.add_argument(
        "--max-recent-failures",
        type=int,
        default=5,
        help="Limit recent failures displayed/exported in snapshot output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = discover_project_root(Path(__file__))
    pipeline_root = project_root / "data" / "processed" / "pipeline_runs"
    snapshot = build_ops_status_snapshot(project_root=project_root, pipeline_root=pipeline_root)
    if isinstance(snapshot.get("recent_failures"), list) and args.max_recent_failures >= 0:
        snapshot["recent_failures"] = snapshot["recent_failures"][-args.max_recent_failures :]

    if args.write_file:
        path = write_ops_status_snapshot(project_root=project_root, snapshot=snapshot)
        snapshot["written_path"] = path.as_posix()

    if args.format == "json":
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    else:
        print(render_text(snapshot))

    if args.fail_on_unhealthy:
        scheduler = snapshot.get("scheduler_health") if isinstance(snapshot.get("scheduler_health"), dict) else {}
        overall = scheduler.get("overall_status")
        if overall != "healthy":
            raise SystemExit(1)

    raise SystemExit(0)


if __name__ == "__main__":
    main()
