from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if SCRIPTS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR.as_posix())

import build_analytics_marts as bam  # noqa: E402
import build_historical_forecast_extended as bhf  # noqa: E402


def test_incremental_refresh_start_clamps_future_max_ts() -> None:
    max_ts = datetime(2026, 4, 10, 12, 0, 0)
    now_ref = datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)

    refresh_start = bam.compute_incremental_refresh_start(
        max_ts=max_ts,
        lookback_hours=96,
        now_utc=now_ref,
    )

    assert refresh_start == datetime(2026, 4, 2, 12, 0, 0)


def test_incremental_refresh_start_uses_historical_anchor_when_past() -> None:
    max_ts = datetime(2026, 4, 4, 6, 0, 0)
    now_ref = datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)

    refresh_start = bam.compute_incremental_refresh_start(
        max_ts=max_ts,
        lookback_hours=48,
        now_utc=now_ref,
    )

    assert refresh_start == datetime(2026, 4, 2, 6, 0, 0)


def test_incremental_refresh_start_rejects_non_positive_lookback() -> None:
    with pytest.raises(ValueError):
        bam.compute_incremental_refresh_start(
            max_ts=datetime(2026, 4, 1, 0, 0, 0),
            lookback_hours=0,
            now_utc=datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc),
        )


def test_historical_forecast_prefers_latest_timestamped_file(tmp_path: Path) -> None:
    files = [
        "sirnak_2024_01_historical_forecast.json",
        "sirnak_2024_01_historical_forecast_20260101_010101.json",
        "sirnak_2024_01_historical_forecast_20260201_010101.json",
        "ankara_2024_02_historical_forecast.json",
    ]

    paths = []
    for name in files:
        path = tmp_path / name
        path.write_text("{}", encoding="utf-8")
        paths.append(path)

    selected = bhf.select_preferred_monthly_files(paths, year=2024)
    selected_map = {(item.city_slug, item.month): item.path.name for item in selected}

    assert selected_map[("sirnak", 1)] == "sirnak_2024_01_historical_forecast_20260201_010101.json"
    assert selected_map[("ankara", 2)] == "ankara_2024_02_historical_forecast.json"
    assert len(selected) == 2
