from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from _shared import discover_project_root


@dataclass
class CheckResult:
    name: str
    passed: bool
    actual: object
    expected: str

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "passed": self.passed,
            "actual": self.actual,
            "expected": self.expected,
        }


def fetch_scalar(con: duckdb.DuckDBPyConnection, sql: str) -> object:
    return con.execute(sql).fetchone()[0]


def validate_city_hourly_environment(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    expect_extended_hf: bool = False,
) -> list[CheckResult]:
    src = f"read_parquet('{path.as_posix()}')"
    checks: list[CheckResult] = []

    row_count = fetch_scalar(con, f"select count(*) from {src}")
    checks.append(CheckResult("city_hourly_environment.row_count_positive", row_count > 0, row_count, "> 0"))

    city_count = fetch_scalar(con, f"select count(distinct city_name) from {src}")
    checks.append(CheckResult("city_hourly_environment.city_count", city_count == 81, city_count, "== 81"))

    province_count = fetch_scalar(con, f"select count(distinct province_name) from {src}")
    checks.append(
        CheckResult(
            "city_hourly_environment.province_count",
            province_count == 81,
            province_count,
            "== 81",
        )
    )

    dupes = fetch_scalar(
        con,
        (
            f"select count(*) - count(distinct city_name || '|' || time) "
            f"from {src}"
        ),
    )
    checks.append(CheckResult("city_hourly_environment.duplicates_city_time", dupes == 0, dupes, "== 0"))

    min_src_count = fetch_scalar(con, f"select min(available_source_count) from {src}")
    max_src_count = fetch_scalar(con, f"select max(available_source_count) from {src}")
    checks.append(
        CheckResult(
            "city_hourly_environment.available_source_count_range",
            min_src_count >= 1 and max_src_count <= 5,
            {"min": min_src_count, "max": max_src_count},
            "min >= 1 and max <= 5",
        )
    )

    hf_rows = fetch_scalar(con, f"select count(*) from {src} where has_historical_forecast")
    checks.append(
        CheckResult(
            "city_hourly_environment.hf_rows_positive",
            hf_rows > 0,
            hf_rows,
            "> 0",
        )
    )
    if expect_extended_hf:
        checks.append(
            CheckResult(
                "city_hourly_environment.hf_rows_extended_threshold",
                hf_rows > 50000,
                hf_rows,
                "> 50000",
            )
        )

    cams_rows = fetch_scalar(con, f"select count(*) from {src} where has_cams_reanalysis")
    checks.append(
        CheckResult(
            "city_hourly_environment.cams_rows_positive",
            cams_rows > 0,
            cams_rows,
            "> 0",
        )
    )

    return checks


def validate_city_forecast_vs_actual(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    expect_extended_hf: bool = False,
) -> list[CheckResult]:
    src = f"read_parquet('{path.as_posix()}')"
    checks: list[CheckResult] = []

    row_count = fetch_scalar(con, f"select count(*) from {src}")
    checks.append(CheckResult("city_forecast_vs_actual.row_count_positive", row_count > 0, row_count, "> 0"))

    city_count = fetch_scalar(con, f"select count(distinct city_name) from {src}")
    checks.append(CheckResult("city_forecast_vs_actual.city_count", city_count == 81, city_count, "== 81"))

    province_count = fetch_scalar(con, f"select count(distinct province_name) from {src}")
    checks.append(
        CheckResult(
            "city_forecast_vs_actual.province_count",
            province_count == 81,
            province_count,
            "== 81",
        )
    )

    dupes = fetch_scalar(
        con,
        (
            f"select count(*) - count(distinct city_name || '|' || time) "
            f"from {src}"
        ),
    )
    checks.append(CheckResult("city_forecast_vs_actual.duplicates_city_time", dupes == 0, dupes, "== 0"))

    null_rate = fetch_scalar(
        con,
        (
            "select avg(case when hf_temperature_2m is null or hw_temperature_2m is null then 1 else 0 end) "
            f"from {src}"
        ),
    )
    checks.append(
        CheckResult(
            "city_forecast_vs_actual.temperature_null_rate",
            null_rate == 0,
            null_rate,
            "== 0.0",
        )
    )

    time_span_hours = fetch_scalar(
        con,
        f"select datediff('hour', min(time_ts), max(time_ts)) from {src}",
    )
    checks.append(
        CheckResult(
            "city_forecast_vs_actual.time_span_hours",
            time_span_hours >= 160,
            time_span_hours,
            ">= 160",
        )
    )
    if expect_extended_hf:
        checks.append(
            CheckResult(
                "city_forecast_vs_actual.extended_row_threshold",
                row_count > 50000,
                row_count,
                "> 50000",
            )
        )

    missing_window = fetch_scalar(
        con,
        (
            "select count(*) "
            f"from {src} "
            "where forecast_validation_window is null or trim(forecast_validation_window) = ''"
        ),
    )
    checks.append(
        CheckResult(
            "city_forecast_vs_actual.forecast_validation_window_missing",
            missing_window == 0,
            missing_window,
            "== 0",
        )
    )

    return checks


def validate_city_current_snapshot(con: duckdb.DuckDBPyConnection, path: Path) -> list[CheckResult]:
    src = f"read_parquet('{path.as_posix()}')"
    checks: list[CheckResult] = []

    row_count = fetch_scalar(con, f"select count(*) from {src}")
    checks.append(CheckResult("city_current_snapshot.row_count", row_count == 81, row_count, "== 81"))

    province_count = fetch_scalar(con, f"select count(distinct province_name) from {src}")
    checks.append(
        CheckResult(
            "city_current_snapshot.province_count",
            province_count == 81,
            province_count,
            "== 81",
        )
    )

    dupes = fetch_scalar(
        con,
        (
            f"select count(*) - count(distinct city_name) "
            f"from {src}"
        ),
    )
    checks.append(CheckResult("city_current_snapshot.duplicates_city", dupes == 0, dupes, "== 0"))

    missing_snapshot = fetch_scalar(
        con,
        f"select count(*) from {src} where snapshot_time is null",
    )
    checks.append(
        CheckResult(
            "city_current_snapshot.snapshot_time_missing",
            missing_snapshot == 0,
            missing_snapshot,
            "== 0",
        )
    )

    null_shape = fetch_scalar(con, f"select count(*) from {src} where shape_iso is null")
    checks.append(CheckResult("city_current_snapshot.shape_iso_nulls", null_shape == 0, null_shape, "== 0"))

    return checks


def validate_province_map_metrics(con: duckdb.DuckDBPyConnection, path: Path) -> list[CheckResult]:
    src = f"read_parquet('{path.as_posix()}')"
    checks: list[CheckResult] = []

    row_count = fetch_scalar(con, f"select count(*) from {src}")
    checks.append(CheckResult("province_map_metrics.row_count", row_count == 81, row_count, "== 81"))

    dupes = fetch_scalar(
        con,
        (
            f"select count(*) - count(distinct province_name) "
            f"from {src}"
        ),
    )
    checks.append(CheckResult("province_map_metrics.duplicates_province", dupes == 0, dupes, "== 0"))

    null_shape = fetch_scalar(con, f"select count(*) from {src} where shape_iso is null")
    checks.append(CheckResult("province_map_metrics.shape_iso_nulls", null_shape == 0, null_shape, "== 0"))

    columns = {
        row[0]
        for row in con.execute(f"describe select * from {src}").fetchall()
    }
    if "cams_avg_pm2p5" in columns:
        cams_nulls = fetch_scalar(
            con,
            f"select count(*) from {src} where cams_avg_pm2p5 is null",
        )
        checks.append(
            CheckResult(
                "province_map_metrics.cams_avg_pm2p5_nulls",
                cams_nulls < 81,
                cams_nulls,
                "< 81",
            )
        )

    if "cams_avg_pm2p5" in columns and "cams_2024_01_avg_pm2p5" in columns:
        mismatch = fetch_scalar(
            con,
            (
                "select count(*) "
                f"from {src} "
                "where coalesce(cams_avg_pm2p5, -999999.0) <> coalesce(cams_2024_01_avg_pm2p5, -999999.0)"
            ),
        )
        checks.append(
            CheckResult(
                "province_map_metrics.cams_legacy_alias_match",
                mismatch == 0,
                mismatch,
                "== 0",
            )
        )

    return checks


def validate_mobile_views(con: duckdb.DuckDBPyConnection, paths: dict[str, Path]) -> list[CheckResult]:
    checks: list[CheckResult] = []
    for name, path in paths.items():
        src = f"read_parquet('{path.as_posix()}')"
        row_count = fetch_scalar(con, f"select count(*) from {src}")
        checks.append(CheckResult(f"{name}.row_count_positive", row_count > 0, row_count, "> 0"))
    return checks


def validate_historical_forecast_extended_manifest(manifest_path: Path) -> list[CheckResult]:
    checks: list[CheckResult] = []
    if not manifest_path.exists():
        checks.append(
            CheckResult(
                "historical_forecast_extended_manifest.exists",
                False,
                False,
                "== True",
            )
        )
        return checks

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    stats = payload.get("stats", {}) if isinstance(payload, dict) else {}
    outputs = payload.get("outputs", {}) if isinstance(payload, dict) else {}

    files_failed = stats.get("files_failed")
    files_unmapped = stats.get("files_unmapped")
    city_count = stats.get("city_count")
    rows_after_dedup = stats.get("rows_after_dedup")
    validated_min_city_count = stats.get("validated_min_city_count")
    time_min = stats.get("time_min")
    time_max = stats.get("time_max")

    checks.append(
        CheckResult(
            "historical_forecast_extended_manifest.files_failed",
            files_failed == 0,
            files_failed,
            "== 0",
        )
    )
    checks.append(
        CheckResult(
            "historical_forecast_extended_manifest.files_unmapped",
            files_unmapped == 0,
            files_unmapped,
            "== 0",
        )
    )
    checks.append(
        CheckResult(
            "historical_forecast_extended_manifest.city_count",
            city_count == 81,
            city_count,
            "== 81",
        )
    )
    checks.append(
        CheckResult(
            "historical_forecast_extended_manifest.rows_after_dedup",
            isinstance(rows_after_dedup, int) and rows_after_dedup > 50000,
            rows_after_dedup,
            "> 50000",
        )
    )
    checks.append(
        CheckResult(
            "historical_forecast_extended_manifest.validated_min_city_count",
            isinstance(validated_min_city_count, int) and validated_min_city_count == 81,
            validated_min_city_count,
            "== 81",
        )
    )
    checks.append(
        CheckResult(
            "historical_forecast_extended_manifest.time_bounds_present",
            bool(time_min) and bool(time_max),
            {"time_min": time_min, "time_max": time_max},
            "time_min and time_max are non-empty",
        )
    )

    has_absolute_output_path = False
    if isinstance(outputs, dict):
        for value in outputs.values():
            if isinstance(value, str) and (value.startswith("/") or value.startswith("C:\\")):
                has_absolute_output_path = True
                break

    checks.append(
        CheckResult(
            "historical_forecast_extended_manifest.relative_output_paths",
            not has_absolute_output_path,
            not has_absolute_output_path,
            "== True",
        )
    )
    return checks


def validate_cams_manifest(manifest_path: Path) -> list[CheckResult]:
    checks: list[CheckResult] = []
    if not manifest_path.exists():
        checks.append(CheckResult("cams_manifest.exists", False, False, "== True"))
        return checks

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    available_months = payload.get("available_months", []) if isinstance(payload, dict) else []
    monthly_row_counts = payload.get("monthly_row_counts", {}) if isinstance(payload, dict) else {}
    combined_rows = payload.get("combined_rows") if isinstance(payload, dict) else None
    city_count = payload.get("city_count") if isinstance(payload, dict) else None

    checks.append(
        CheckResult(
            "cams_manifest.available_months_nonempty",
            isinstance(available_months, list) and len(available_months) > 0,
            available_months,
            "len > 0",
        )
    )
    checks.append(
        CheckResult(
            "cams_manifest.combined_rows_positive",
            isinstance(combined_rows, int) and combined_rows > 0,
            combined_rows,
            "> 0",
        )
    )
    checks.append(
        CheckResult(
            "cams_manifest.city_count",
            city_count == 81,
            city_count,
            "== 81",
        )
    )

    all_month_rows_positive = (
        isinstance(monthly_row_counts, dict)
        and len(monthly_row_counts) > 0
        and all(isinstance(v, int) and v > 0 for v in monthly_row_counts.values())
    )
    checks.append(
        CheckResult(
            "cams_manifest.monthly_rows_positive",
            all_month_rows_positive,
            monthly_row_counts,
            "all month row counts > 0",
        )
    )

    if isinstance(monthly_row_counts, dict) and "2024_01" in monthly_row_counts:
        checks.append(
            CheckResult(
                "cams_manifest.2024_01_expected_rows",
                monthly_row_counts.get("2024_01") == 60264,
                monthly_row_counts.get("2024_01"),
                "== 60264",
            )
        )

    return checks


def main() -> None:
    project_root = discover_project_root(Path(__file__))

    marts_dir = project_root / "data" / "processed" / "marts"
    views_dir = project_root / "data" / "processed" / "views"

    mart_paths = {
        "city_hourly_environment": marts_dir / "city_hourly_environment_tr.parquet",
        "city_forecast_vs_actual": marts_dir / "city_forecast_vs_actual_tr.parquet",
        "city_current_snapshot": marts_dir / "city_current_snapshot_tr.parquet",
        "province_map_metrics": marts_dir / "province_map_metrics_tr.parquet",
    }
    view_paths = {
        "mobile_city_current_snapshot": views_dir / "mobile_city_current_snapshot_tr_light.parquet",
        "mobile_city_hourly_timeline": views_dir / "mobile_city_hourly_timeline_tr_light.parquet",
        "mobile_province_map_metrics": views_dir / "mobile_province_map_metrics_tr_light.parquet",
    }

    missing = [name for name, path in {**mart_paths, **view_paths}.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing outputs. Build marts first. Missing: " + ", ".join(sorted(missing))
        )

    build_manifest_path = marts_dir / "marts_build_manifest.json"
    expect_extended_hf = False
    uses_combined_cams = False
    if build_manifest_path.exists():
        payload = json.loads(build_manifest_path.read_text(encoding="utf-8"))
        inputs = payload.get("inputs", {}) if isinstance(payload, dict) else {}
        if isinstance(inputs, dict):
            historical_forecast_input = inputs.get("historical_forecast_hourly")
            if historical_forecast_input is None:
                historical_forecast_input = inputs.get("historical_forecast_full")
            if isinstance(historical_forecast_input, str) and "_extended_" in historical_forecast_input:
                expect_extended_hf = True

            cams_input = inputs.get("cams_city_hourly")
            if isinstance(cams_input, str) and "cams_city_hourly_tr_all_available.csv" in cams_input:
                uses_combined_cams = True

    con = duckdb.connect()
    try:
        checks: list[CheckResult] = []
        checks.extend(
            validate_city_hourly_environment(
                con,
                mart_paths["city_hourly_environment"],
                expect_extended_hf=expect_extended_hf,
            )
        )
        checks.extend(
            validate_city_forecast_vs_actual(
                con,
                mart_paths["city_forecast_vs_actual"],
                expect_extended_hf=expect_extended_hf,
            )
        )
        checks.extend(validate_city_current_snapshot(con, mart_paths["city_current_snapshot"]))
        checks.extend(validate_province_map_metrics(con, mart_paths["province_map_metrics"]))
        checks.extend(validate_mobile_views(con, view_paths))

        if expect_extended_hf:
            hf_manifest_path = (
                project_root
                / "data"
                / "raw"
                / "open_meteo"
                / "historical_forecast"
                / "manifests"
                / "historical_forecast_extended_manifest.json"
            )
            checks.extend(validate_historical_forecast_extended_manifest(hf_manifest_path))

        if uses_combined_cams:
            cams_manifest_path = (
                project_root
                / "data"
                / "processed"
                / "cams"
                / "cams_city_hourly_manifest.json"
            )
            checks.extend(validate_cams_manifest(cams_manifest_path))

        passed = all(check.passed for check in checks)
        summary = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "passed": passed,
            "check_count": len(checks),
            "failed_count": sum(0 if c.passed else 1 for c in checks),
            "checks": [c.to_dict() for c in checks],
        }

        report_path = marts_dir / "validation_report.json"
        report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"Validation report: {report_path}")
        print(f"Passed: {passed}")
        if not passed:
            for check in checks:
                if not check.passed:
                    print(
                        f"FAILED: {check.name} actual={check.actual!r} expected={check.expected}"
                    )
            raise SystemExit(1)

    finally:
        con.close()


if __name__ == "__main__":
    main()
