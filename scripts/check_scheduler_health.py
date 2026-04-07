from __future__ import annotations

import argparse
import json
from pathlib import Path

from _shared import discover_project_root
from run_pipeline import build_ops_status_snapshot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate scheduler cadence health from pipeline history and exit non-zero when unhealthy."
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
    )
    parser.add_argument(
        "--fail-level",
        choices=["critical", "warning"],
        default="critical",
        help="Exit non-zero at this severity or above.",
    )
    return parser.parse_args()


def should_fail(overall_status: str | None, fail_level: str) -> bool:
    if overall_status is None:
        return fail_level in {"critical", "warning"}
    if overall_status == "critical":
        return True
    if overall_status == "warning" and fail_level == "warning":
        return True
    return False


def main() -> None:
    args = parse_args()
    project_root = discover_project_root(Path(__file__))
    pipeline_root = project_root / "data" / "processed" / "pipeline_runs"
    snapshot = build_ops_status_snapshot(project_root=project_root, pipeline_root=pipeline_root)
    scheduler = snapshot.get("scheduler_health") if isinstance(snapshot.get("scheduler_health"), dict) else {}
    overall = scheduler.get("overall_status")

    if args.format == "json":
        print(json.dumps({"scheduler_health": scheduler}, ensure_ascii=False, indent=2))
    else:
        print("Breathwise Scheduler Health")
        print(f"- evaluated_at_utc: {scheduler.get('evaluated_at_utc')}")
        print(f"- overall_status: {overall}")
        modes = scheduler.get("modes") if isinstance(scheduler.get("modes"), dict) else {}
        for mode in ["incremental", "standard", "full"]:
            entry = modes.get(mode) if isinstance(modes.get(mode), dict) else {}
            print(
                f"- {mode}: status={entry.get('status')} age_hours={entry.get('age_hours')} "
                f"missed_est={entry.get('missed_run_estimate')}"
            )

    if should_fail(overall_status=overall if isinstance(overall, str) else None, fail_level=args.fail_level):
        raise SystemExit(1)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
