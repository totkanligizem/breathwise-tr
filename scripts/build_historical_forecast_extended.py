from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from _shared import discover_project_root, slugify_ascii


MONTHLY_RAW_SUFFIX_PATTERN = re.compile(
    r"(?P<city>.+)_(?P<year>\d{4})_(?P<month>\d{2})_historical_forecast_(?P<batch_ts>\d{8}_\d{6})\.json$"
)
MONTHLY_RAW_PLAIN_PATTERN = re.compile(
    r"(?P<city>.+)_(?P<year>\d{4})_(?P<month>\d{2})_historical_forecast\.json$"
)

FULL_OUTPUT_COLUMNS = [
    "city_name",
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation_probability",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "weather_code",
    "pressure_msl",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "visibility",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "uv_index",
    "is_day",
    "freezing_level_height",
    "latitude_requested",
    "longitude_requested",
    "latitude_used",
    "longitude_used",
    "elevation_used",
    "timezone",
    "timezone_abbreviation",
    "utc_offset_seconds",
    "generationtime_ms",
    "source_tier",
    "source_batch",
    "raw_file_name",
]

LIGHT_OUTPUT_COLUMNS = [
    "city_name",
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation_probability",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "pressure_msl",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "uv_index",
    "is_day",
    "freezing_level_height",
    "generationtime_ms",
    "source_tier",
    "source_batch",
]


@dataclass(frozen=True)
class Paths:
    dim_city: Path
    raw_validated_monthly: Path
    canonical_short_range_full: Path
    tidy_validated_dir: Path
    tidy_canonical_dir: Path
    manifests_dir: Path


@dataclass(frozen=True)
class BuildStats:
    files_total: int
    files_selected: int
    files_parsed: int
    files_unmapped: int
    files_failed: int
    validated_rows: int
    canonical_rows: int
    rows_before_dedup: int
    rows_after_dedup: int
    duplicates_removed: int
    city_count: int
    validated_min_city_count: int | None
    time_min: str | None
    time_max: str | None


@dataclass(frozen=True)
class MonthlyRawFile:
    path: Path
    city_token: str
    city_slug: str
    year: int
    month: int
    sort_batch: str
    is_timestamped: bool


def resolve_paths(project_root: Path) -> Paths:
    open_meteo_root = project_root / "data" / "raw" / "open_meteo"
    hist_fc_root = open_meteo_root / "historical_forecast"

    return Paths(
        dim_city=sorted((open_meteo_root / "geocoding").glob("dim_city_*.csv"))[-1],
        raw_validated_monthly=hist_fc_root / "raw_json" / "validated" / "monthly_chunks",
        canonical_short_range_full=(
            hist_fc_root
            / "tidy"
            / "canonical"
            / "historical_forecast_hourly_tr_2024_01_15_2024_01_21_full.csv"
        ),
        tidy_validated_dir=hist_fc_root / "tidy" / "validated",
        tidy_canonical_dir=hist_fc_root / "tidy" / "canonical",
        manifests_dir=hist_fc_root / "manifests",
    )


def build_slug_map(dim_city_path: Path) -> dict[str, str]:
    dim_city = pd.read_csv(dim_city_path)
    cities = sorted(dim_city["city_name"].dropna().astype(str).unique())
    return {slugify_ascii(city): city for city in cities}


def parse_monthly_raw_filename(json_path: Path) -> MonthlyRawFile | None:
    suffix_match = MONTHLY_RAW_SUFFIX_PATTERN.match(json_path.name)
    if suffix_match:
        city_token = suffix_match.group("city")
        year = int(suffix_match.group("year"))
        month = int(suffix_match.group("month"))
        batch_ts = suffix_match.group("batch_ts")
        return MonthlyRawFile(
            path=json_path,
            city_token=city_token,
            city_slug=slugify_ascii(city_token),
            year=year,
            month=month,
            sort_batch=batch_ts,
            is_timestamped=True,
        )

    plain_match = MONTHLY_RAW_PLAIN_PATTERN.match(json_path.name)
    if plain_match:
        city_token = plain_match.group("city")
        year = int(plain_match.group("year"))
        month = int(plain_match.group("month"))
        return MonthlyRawFile(
            path=json_path,
            city_token=city_token,
            city_slug=slugify_ascii(city_token),
            year=year,
            month=month,
            sort_batch="00000000_000000",
            is_timestamped=False,
        )

    return None


def select_preferred_monthly_files(paths: list[Path], year: int) -> list[MonthlyRawFile]:
    preferred: dict[tuple[str, int, int], MonthlyRawFile] = {}

    for json_path in sorted(paths):
        parsed = parse_monthly_raw_filename(json_path)
        if parsed is None or parsed.year != year:
            continue

        key = (parsed.city_slug, parsed.year, parsed.month)
        current = preferred.get(key)
        if current is None:
            preferred[key] = parsed
            continue

        # Timestamped files are preferred over legacy plain names. If both are timestamped,
        # the most recent batch stamp is preferred.
        candidate_rank = (1 if parsed.is_timestamped else 0, parsed.sort_batch)
        current_rank = (1 if current.is_timestamped else 0, current.sort_batch)
        if candidate_rank > current_rank:
            preferred[key] = parsed

    return sorted(
        preferred.values(),
        key=lambda item: (item.city_slug, item.year, item.month, item.sort_batch),
    )


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = None
    return out[columns]


def parse_validated_monthly_payload(
    json_path: Path,
    city_name: str,
    source_batch: str,
) -> pd.DataFrame:
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    hourly = payload.get("hourly")
    if not hourly:
        return pd.DataFrame(columns=FULL_OUTPUT_COLUMNS)

    df = pd.DataFrame(hourly)
    if df.empty:
        return pd.DataFrame(columns=FULL_OUTPUT_COLUMNS)

    df.insert(0, "city_name", city_name)
    df["latitude_requested"] = payload.get("latitude")
    df["longitude_requested"] = payload.get("longitude")
    df["latitude_used"] = payload.get("latitude")
    df["longitude_used"] = payload.get("longitude")
    df["elevation_used"] = payload.get("elevation")
    df["timezone"] = payload.get("timezone")
    df["timezone_abbreviation"] = payload.get("timezone_abbreviation")
    df["utc_offset_seconds"] = payload.get("utc_offset_seconds")
    df["generationtime_ms"] = payload.get("generationtime_ms")
    df["source_tier"] = "validated_monthly"
    df["source_batch"] = source_batch
    df["raw_file_name"] = json_path.name

    return ensure_columns(df, FULL_OUTPUT_COLUMNS)


def load_canonical_short_range(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=FULL_OUTPUT_COLUMNS)

    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=FULL_OUTPUT_COLUMNS)

    df["source_tier"] = "canonical_short_range"
    df["source_batch"] = "2024_01_15_2024_01_21"
    df["raw_file_name"] = "canonical_short_range_reference"
    return ensure_columns(df, FULL_OUTPUT_COLUMNS)


def deduplicate_priority(df: pd.DataFrame) -> pd.DataFrame:
    priority_map = {
        "canonical_short_range": 2,
        "validated_monthly": 1,
    }

    out = df.copy()
    out["_priority"] = out["source_tier"].map(priority_map).fillna(0)

    out = out.sort_values(
        ["city_name", "time", "_priority", "source_batch", "raw_file_name"],
        ascending=[True, True, False, False, False],
        kind="mergesort",
    )
    out = out.drop_duplicates(subset=["city_name", "time"], keep="first")
    out = out.drop(columns=["_priority"])

    out = out.sort_values(["city_name", "time"]).reset_index(drop=True)
    return out


def build_monthly_coverage(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["source_batch", "source_tier", "row_count", "city_count", "time_min", "time_max"]
        )

    coverage = (
        df.groupby(["source_batch", "source_tier"], as_index=False)
        .agg(
            row_count=("time", "size"),
            city_count=("city_name", "nunique"),
            time_min=("time", "min"),
            time_max=("time", "max"),
        )
        .sort_values(["source_tier", "source_batch"])
        .reset_index(drop=True)
    )
    return coverage


def write_outputs(
    project_root: Path,
    paths: Paths,
    year: int,
    deduped: pd.DataFrame,
    validated_only: pd.DataFrame,
    monthly_coverage: pd.DataFrame,
    stats: BuildStats,
) -> None:
    paths.tidy_validated_dir.mkdir(parents=True, exist_ok=True)
    paths.tidy_canonical_dir.mkdir(parents=True, exist_ok=True)
    paths.manifests_dir.mkdir(parents=True, exist_ok=True)

    validated_full_path = (
        paths.tidy_validated_dir / f"historical_forecast_hourly_tr_{year}_validated_monthly_full.csv"
    )
    validated_light_path = (
        paths.tidy_validated_dir / f"historical_forecast_hourly_tr_{year}_validated_monthly_light.csv"
    )
    canonical_extended_full_path = (
        paths.tidy_canonical_dir / f"historical_forecast_hourly_tr_{year}_extended_full.csv"
    )
    canonical_extended_light_path = (
        paths.tidy_canonical_dir / f"historical_forecast_hourly_tr_{year}_extended_light.csv"
    )

    validated_only = ensure_columns(validated_only, FULL_OUTPUT_COLUMNS)
    deduped = ensure_columns(deduped, FULL_OUTPUT_COLUMNS)

    validated_only.to_csv(validated_full_path, index=False, encoding="utf-8-sig")
    ensure_columns(validated_only, LIGHT_OUTPUT_COLUMNS).to_csv(
        validated_light_path, index=False, encoding="utf-8-sig"
    )

    deduped.to_csv(canonical_extended_full_path, index=False, encoding="utf-8-sig")
    ensure_columns(deduped, LIGHT_OUTPUT_COLUMNS).to_csv(
        canonical_extended_light_path, index=False, encoding="utf-8-sig"
    )

    coverage_path = paths.manifests_dir / "historical_forecast_extended_monthly_coverage.csv"
    monthly_coverage.to_csv(coverage_path, index=False, encoding="utf-8")

    def rel(path: Path) -> str:
        resolved = path.resolve()
        try:
            return resolved.relative_to(project_root).as_posix()
        except ValueError:
            return resolved.as_posix()

    manifest_payload = {
        "year": year,
        "generated_at_utc": pd.Timestamp.now("UTC").isoformat(),
        "outputs": {
            "validated_full": rel(validated_full_path),
            "validated_light": rel(validated_light_path),
            "canonical_extended_full": rel(canonical_extended_full_path),
            "canonical_extended_light": rel(canonical_extended_light_path),
            "monthly_coverage": rel(coverage_path),
        },
        "stats": {
            "files_total": stats.files_total,
            "files_selected": stats.files_selected,
            "files_parsed": stats.files_parsed,
            "files_unmapped": stats.files_unmapped,
            "files_failed": stats.files_failed,
            "validated_rows": stats.validated_rows,
            "canonical_rows": stats.canonical_rows,
            "rows_before_dedup": stats.rows_before_dedup,
            "rows_after_dedup": stats.rows_after_dedup,
            "duplicates_removed": stats.duplicates_removed,
            "city_count": stats.city_count,
            "validated_min_city_count": stats.validated_min_city_count,
            "time_min": stats.time_min,
            "time_max": stats.time_max,
        },
    }

    manifest_path = paths.manifests_dir / "historical_forecast_extended_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("Extended historical forecast build complete.")
    print(f"- validated_full: {validated_full_path}")
    print(f"- validated_light: {validated_light_path}")
    print(f"- canonical_extended_full: {canonical_extended_full_path}")
    print(f"- canonical_extended_light: {canonical_extended_light_path}")
    print(f"- monthly_coverage: {coverage_path}")
    print(f"- manifest: {manifest_path}")
    print(f"- files_selected: {stats.files_selected}")
    print(f"- rows_after_dedup: {stats.rows_after_dedup}")
    print(f"- city_count: {stats.city_count}")
    print(f"- time_range: {stats.time_min} -> {stats.time_max}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build extended historical forecast tidy outputs by consolidating validated monthly raw JSONs "
            "and canonical short-range reference rows."
        )
    )
    parser.add_argument("--year", type=int, default=2024, help="Target year to filter monthly raw files.")
    parser.add_argument(
        "--min-city-count-per-month",
        type=int,
        default=81,
        help=(
            "Minimum expected city count per validated monthly batch. "
            "Build fails if any validated month is below this threshold."
        ),
    )
    args = parser.parse_args()

    project_root = discover_project_root(Path(__file__))
    paths = resolve_paths(project_root)
    slug_map = build_slug_map(paths.dim_city)

    monthly_files = sorted(paths.raw_validated_monthly.glob("*.json"))
    selected_files = select_preferred_monthly_files(monthly_files, year=args.year)

    validated_frames: list[pd.DataFrame] = []
    files_parsed = 0
    files_unmapped = 0
    files_failed = 0

    for selected in selected_files:
        json_path = selected.path
        city_slug = selected.city_slug
        city_name = slug_map.get(city_slug)

        if city_name is None:
            files_unmapped += 1
            continue

        source_batch = f"{selected.year:04d}_{selected.month:02d}"

        try:
            frame = parse_validated_monthly_payload(json_path, city_name=city_name, source_batch=source_batch)
            if not frame.empty:
                validated_frames.append(frame)
            files_parsed += 1
        except Exception:
            files_failed += 1

    validated_df = (
        pd.concat(validated_frames, ignore_index=True)
        if validated_frames
        else pd.DataFrame(columns=FULL_OUTPUT_COLUMNS)
    )

    canonical_df = load_canonical_short_range(paths.canonical_short_range_full)

    all_rows = pd.concat([validated_df, canonical_df], ignore_index=True)
    rows_before_dedup = int(len(all_rows))
    deduped = deduplicate_priority(all_rows)
    rows_after_dedup = int(len(deduped))

    monthly_coverage = build_monthly_coverage(deduped)
    validated_monthly_coverage = monthly_coverage[
        monthly_coverage["source_tier"] == "validated_monthly"
    ].copy()
    validated_min_city_count: int | None = None
    if not validated_monthly_coverage.empty:
        validated_min_city_count = int(validated_monthly_coverage["city_count"].min())
        if validated_min_city_count < args.min_city_count_per_month:
            raise ValueError(
                "Validated monthly historical forecast city coverage below threshold: "
                f"min={validated_min_city_count}, threshold={args.min_city_count_per_month}"
            )

    stats = BuildStats(
        files_total=len(monthly_files),
        files_selected=len(selected_files),
        files_parsed=files_parsed,
        files_unmapped=files_unmapped,
        files_failed=files_failed,
        validated_rows=int(len(validated_df)),
        canonical_rows=int(len(canonical_df)),
        rows_before_dedup=rows_before_dedup,
        rows_after_dedup=rows_after_dedup,
        duplicates_removed=rows_before_dedup - rows_after_dedup,
        city_count=int(deduped["city_name"].nunique()) if not deduped.empty else 0,
        validated_min_city_count=validated_min_city_count,
        time_min=str(deduped["time"].min()) if not deduped.empty else None,
        time_max=str(deduped["time"].max()) if not deduped.empty else None,
    )

    write_outputs(
        project_root=project_root,
        paths=paths,
        year=args.year,
        deduped=deduped,
        validated_only=validated_df,
        monthly_coverage=monthly_coverage,
        stats=stats,
    )


if __name__ == "__main__":
    main()
