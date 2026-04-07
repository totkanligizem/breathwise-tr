from __future__ import annotations

import argparse
import json
import re
import shutil
import time
from pathlib import Path

import pandas as pd
import requests

from _shared import discover_project_root, slugify_ascii


PROJECT_ROOT = discover_project_root(Path(__file__))

OUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "open_meteo"
    / "historical_forecast"
    / "raw_json"
    / "validated"
    / "monthly_chunks"
)

CITY_DIM_DIR = PROJECT_ROOT / "data" / "raw" / "open_meteo" / "geocoding"
DEFAULT_MISSING_CSV = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "open_meteo"
    / "historical_forecast"
    / "manifests"
    / "missing_validated_monthly_city_coverage_2024_suffix_only.csv"
)

HOURLY_VARS = [
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
]

URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fill missing timestamped validated monthly historical forecast JSON files "
            "from a missing-coverage manifest."
        )
    )
    parser.add_argument(
        "--missing-csv",
        type=Path,
        default=DEFAULT_MISSING_CSV,
        help="CSV with month, city_name, city_slug, expected_file_name columns.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Sleep interval between successful requests.",
    )
    parser.add_argument(
        "--retry-sleep-seconds",
        type=float,
        default=2.0,
        help="Sleep interval after failed requests.",
    )
    return parser.parse_args()


def get_latest_dim_city_file() -> Path:
    files = sorted(CITY_DIM_DIR.glob("dim_city_*.csv"))
    if not files:
        raise FileNotFoundError(f"No dim_city file found in {CITY_DIM_DIR}")
    return files[-1]


def load_city_dimension() -> pd.DataFrame:
    path = get_latest_dim_city_file()
    df = pd.read_csv(path)
    required = {"city_name", "latitude", "longitude"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"dim_city missing required cols: {missing}")

    out = df[["city_name", "latitude", "longitude"]].copy()
    out["city_slug"] = out["city_name"].map(slugify_ascii)
    return out


def derive_plain_name(expected_name: str) -> str:
    match = re.match(r"^(?P<base>.+_historical_forecast)(?:_\d{8}_\d{6})?\.json$", expected_name)
    if not match:
        raise ValueError(f"Could not derive plain name from: {expected_name}")
    return f"{match.group('base')}.json"


def month_bounds(ym: str) -> tuple[str, str]:
    period = pd.Period(ym.replace("_", "-"), freq="M")
    return str(period.start_time.date()), str(period.end_time.date())


def fetch_payload(latitude: float, longitude: float, start_date: str, end_date: str) -> dict:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "models": "best_match",
        "timezone": "auto",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "timeformat": "iso8601",
    }

    response = requests.get(URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


def rel_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def main() -> None:
    args = parse_args()
    missing_csv = args.missing_csv if args.missing_csv.is_absolute() else (PROJECT_ROOT / args.missing_csv)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not missing_csv.exists():
        raise FileNotFoundError(f"Missing CSV not found: {missing_csv}")

    df = pd.read_csv(missing_csv)
    if df.empty:
        print(f"No missing rows found in: {missing_csv}")
        return

    required_cols = {"month", "city_name", "city_slug", "expected_file_name"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in missing CSV: {sorted(missing_cols)}")

    city_dim = load_city_dimension()

    copied = 0
    fetched = 0
    skipped = 0
    failures: list[dict[str, str]] = []

    print(f"Using missing list: {missing_csv}")
    print(f"Rows to process: {len(df)}")

    for _, row in df.iterrows():
        month = str(row["month"]).strip()
        city_name = str(row["city_name"]).strip()
        city_slug = str(row["city_slug"]).strip()
        expected_name = str(row["expected_file_name"]).strip()

        expected_path = OUT_DIR / expected_name

        if expected_path.exists():
            skipped += 1
            continue

        try:
            plain_name = derive_plain_name(expected_name)
            plain_path = OUT_DIR / plain_name

            if plain_path.exists():
                shutil.copy2(plain_path, expected_path)
                copied += 1
                print(f"COPY  {plain_name} -> {expected_name}")
                continue

            city_match = city_dim[city_dim["city_slug"] == city_slug]
            if city_match.empty:
                raise ValueError(f"No city match in dim_city for slug={city_slug}")

            city_row = city_match.iloc[0]
            start_date, end_date = month_bounds(month)

            payload = fetch_payload(
                latitude=float(city_row["latitude"]),
                longitude=float(city_row["longitude"]),
                start_date=start_date,
                end_date=end_date,
            )

            payload["_request_meta"] = {
                "city_name": city_name,
                "city_slug": city_slug,
                "latitude_requested": float(city_row["latitude"]),
                "longitude_requested": float(city_row["longitude"]),
                "start_date": start_date,
                "end_date": end_date,
                "source_missing_csv": rel_path(missing_csv),
                "source_expected_filename": expected_name,
            }

            expected_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            fetched += 1
            print(f"FETCH {expected_name}")
            time.sleep(args.sleep_seconds)

        except Exception as exc:
            failures.append(
                {
                    "month": month,
                    "city_name": city_name,
                    "city_slug": city_slug,
                    "expected_name": expected_name,
                    "error": str(exc),
                }
            )
            print(f"FAIL  {expected_name} | {exc}")
            time.sleep(args.retry_sleep_seconds)

    print("\nSUMMARY")
    print(f"skipped_existing: {skipped}")
    print(f"copied_from_plain: {copied}")
    print(f"fetched_from_api: {fetched}")
    print(f"failures: {len(failures)}")

    if failures:
        fail_path = OUT_DIR / "fill_missing_validated_suffix_only_failures.json"
        fail_path.write_text(
            json.dumps(failures, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Failure log written to: {fail_path}")
    else:
        print("No failures.")


if __name__ == "__main__":
    main()
