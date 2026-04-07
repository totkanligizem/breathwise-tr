from __future__ import annotations

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MARTS_DIR = PROJECT_ROOT / "data" / "processed" / "marts"
VIEWS_DIR = PROJECT_ROOT / "data" / "processed" / "views"
MANIFEST_PATH = MARTS_DIR / "marts_build_manifest.json"
HF_EXT_MANIFEST_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "open_meteo"
    / "historical_forecast"
    / "manifests"
    / "historical_forecast_extended_manifest.json"
)
CAMS_MANIFEST_PATH = PROJECT_ROOT / "data" / "processed" / "cams" / "cams_city_hourly_manifest.json"


def parquet_src(path: Path) -> str:
    return f"read_parquet('{path.as_posix()}')"


def extended_historical_forecast_expected() -> bool:
    if not MANIFEST_PATH.exists():
        return False

    payload = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return False

    inputs = payload.get("inputs", {})
    if not isinstance(inputs, dict):
        return False

    hist_fc_input = inputs.get("historical_forecast_hourly") or inputs.get("historical_forecast_full")
    return isinstance(hist_fc_input, str) and "_extended_" in hist_fc_input


def test_required_outputs_exist() -> None:
    required_paths = [
        MARTS_DIR / "city_hourly_environment_tr.parquet",
        MARTS_DIR / "city_forecast_vs_actual_tr.parquet",
        MARTS_DIR / "city_current_snapshot_tr.parquet",
        MARTS_DIR / "province_map_metrics_tr.parquet",
        VIEWS_DIR / "mobile_city_current_snapshot_tr_light.parquet",
        VIEWS_DIR / "mobile_city_hourly_timeline_tr_light.parquet",
        VIEWS_DIR / "mobile_province_map_metrics_tr_light.parquet",
    ]
    missing = [path.as_posix() for path in required_paths if not path.exists()]
    assert not missing, f"Missing required output files: {missing}"


def test_city_hourly_environment_integrity() -> None:
    path = MARTS_DIR / "city_hourly_environment_tr.parquet"
    expect_extended = extended_historical_forecast_expected()
    con = duckdb.connect()
    try:
        src = parquet_src(path)
        row_count = con.execute(f"select count(*) from {src}").fetchone()[0]
        city_count = con.execute(f"select count(distinct city_name) from {src}").fetchone()[0]
        province_count = con.execute(f"select count(distinct province_name) from {src}").fetchone()[0]
        dupes = con.execute(
            f"select count(*) - count(distinct city_name || '|' || time) from {src}"
        ).fetchone()[0]
        hf_rows = con.execute(
            f"select count(*) from {src} where has_historical_forecast"
        ).fetchone()[0]
        cams_rows = con.execute(
            f"select count(*) from {src} where has_cams_reanalysis"
        ).fetchone()[0]

        assert row_count > 0
        assert city_count == 81
        assert province_count == 81
        assert dupes == 0
        assert hf_rows > 0
        assert cams_rows > 0
        if expect_extended:
            assert hf_rows > 50000

    finally:
        con.close()


def test_city_forecast_vs_actual_integrity() -> None:
    path = MARTS_DIR / "city_forecast_vs_actual_tr.parquet"
    expect_extended = extended_historical_forecast_expected()
    con = duckdb.connect()
    try:
        src = parquet_src(path)
        row_count = con.execute(f"select count(*) from {src}").fetchone()[0]
        city_count = con.execute(f"select count(distinct city_name) from {src}").fetchone()[0]
        province_count = con.execute(f"select count(distinct province_name) from {src}").fetchone()[0]
        dupes = con.execute(
            f"select count(*) - count(distinct city_name || '|' || time) from {src}"
        ).fetchone()[0]
        null_temperature_rows = con.execute(
            f"select count(*) from {src} where hf_temperature_2m is null or hw_temperature_2m is null"
        ).fetchone()[0]
        time_span_hours = con.execute(
            f"select datediff('hour', min(time_ts), max(time_ts)) from {src}"
        ).fetchone()[0]
        missing_window = con.execute(
            "select count(*) "
            f"from {src} "
            "where forecast_validation_window is null or trim(forecast_validation_window) = ''"
        ).fetchone()[0]

        assert row_count > 0
        assert city_count == 81
        assert province_count == 81
        assert dupes == 0
        assert null_temperature_rows == 0
        assert time_span_hours >= 160
        assert missing_window == 0
        if expect_extended:
            assert row_count > 50000

    finally:
        con.close()


def test_city_current_snapshot_integrity() -> None:
    path = MARTS_DIR / "city_current_snapshot_tr.parquet"
    con = duckdb.connect()
    try:
        src = parquet_src(path)
        row_count = con.execute(f"select count(*) from {src}").fetchone()[0]
        city_count = con.execute(f"select count(distinct city_name) from {src}").fetchone()[0]
        province_count = con.execute(f"select count(distinct province_name) from {src}").fetchone()[0]
        null_snapshot_time = con.execute(
            f"select count(*) from {src} where snapshot_time is null"
        ).fetchone()[0]
        null_shape_iso = con.execute(
            f"select count(*) from {src} where shape_iso is null"
        ).fetchone()[0]

        assert row_count == 81
        assert city_count == 81
        assert province_count == 81
        assert null_snapshot_time == 0
        assert null_shape_iso == 0

    finally:
        con.close()


def test_province_map_metrics_integrity() -> None:
    path = MARTS_DIR / "province_map_metrics_tr.parquet"
    con = duckdb.connect()
    try:
        src = parquet_src(path)
        row_count = con.execute(f"select count(*) from {src}").fetchone()[0]
        province_count = con.execute(f"select count(distinct province_name) from {src}").fetchone()[0]
        null_shape = con.execute(f"select count(*) from {src} where shape_iso is null").fetchone()[0]
        columns = {row[0] for row in con.execute(f"describe select * from {src}").fetchall()}

        assert row_count == 81
        assert province_count == 81
        assert null_shape == 0

        if "cams_avg_pm2p5" in columns:
            cams_nulls = con.execute(
                f"select count(*) from {src} where cams_avg_pm2p5 is null"
            ).fetchone()[0]
            assert cams_nulls < 81

        if "cams_avg_pm2p5" in columns and "cams_2024_01_avg_pm2p5" in columns:
            mismatch = con.execute(
                "select count(*) "
                f"from {src} "
                "where coalesce(cams_avg_pm2p5, -999999.0) <> coalesce(cams_2024_01_avg_pm2p5, -999999.0)"
            ).fetchone()[0]
            assert mismatch == 0

    finally:
        con.close()


def test_mobile_views_have_rows() -> None:
    view_paths = [
        VIEWS_DIR / "mobile_city_current_snapshot_tr_light.parquet",
        VIEWS_DIR / "mobile_city_hourly_timeline_tr_light.parquet",
        VIEWS_DIR / "mobile_province_map_metrics_tr_light.parquet",
    ]

    con = duckdb.connect()
    try:
        for path in view_paths:
            src = parquet_src(path)
            row_count = con.execute(f"select count(*) from {src}").fetchone()[0]
            assert row_count > 0, f"Expected > 0 rows for {path.name}"
    finally:
        con.close()


def test_pipeline_manifests_integrity() -> None:
    assert MANIFEST_PATH.exists(), f"Missing marts manifest: {MANIFEST_PATH}"
    marts_manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    assert marts_manifest.get("build_mode") in {"auto", "full", "incremental"}
    input_fingerprints = marts_manifest.get("input_fingerprints", {})
    assert isinstance(input_fingerprints, dict) and input_fingerprints

    for name, fingerprint in input_fingerprints.items():
        assert isinstance(fingerprint, dict), f"Invalid fingerprint entry for {name}"
        path_value = fingerprint.get("path")
        assert isinstance(path_value, str) and path_value
        assert not path_value.startswith("/"), f"Expected relative fingerprint path for {name}"
        assert isinstance(fingerprint.get("sha256"), str) and len(fingerprint["sha256"]) == 64

    if extended_historical_forecast_expected():
        assert HF_EXT_MANIFEST_PATH.exists(), f"Missing HF extended manifest: {HF_EXT_MANIFEST_PATH}"
        hf_manifest = json.loads(HF_EXT_MANIFEST_PATH.read_text(encoding="utf-8"))
        hf_stats = hf_manifest.get("stats", {})
        hf_outputs = hf_manifest.get("outputs", {})

        assert hf_stats.get("files_failed") == 0
        assert hf_stats.get("files_unmapped") == 0
        assert hf_stats.get("city_count") == 81
        assert isinstance(hf_stats.get("rows_after_dedup"), int) and hf_stats["rows_after_dedup"] > 50000
        assert hf_stats.get("validated_min_city_count") == 81

        for value in hf_outputs.values():
            assert isinstance(value, str)
            assert not value.startswith("/"), "HF manifest outputs should be relative paths"

    assert CAMS_MANIFEST_PATH.exists(), f"Missing CAMS manifest: {CAMS_MANIFEST_PATH}"
    cams_manifest = json.loads(CAMS_MANIFEST_PATH.read_text(encoding="utf-8"))
    assert cams_manifest.get("city_count") == 81
    assert isinstance(cams_manifest.get("combined_rows"), int) and cams_manifest["combined_rows"] > 0
    monthly = cams_manifest.get("monthly_row_counts", {})
    assert isinstance(monthly, dict) and monthly
    assert all(isinstance(v, int) and v > 0 for v in monthly.values())
