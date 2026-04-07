from __future__ import annotations

import argparse
import calendar
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from _shared import discover_project_root, slugify_ascii


PROJECT_ROOT = discover_project_root(Path(__file__))

CITY_DIM_DIR = PROJECT_ROOT / "data" / "raw" / "open_meteo" / "geocoding"
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

OUT_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

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


def build_month_chunks(year: int, start_month: int, end_month: int) -> list[tuple[str, str, str]]:
    if start_month < 1 or start_month > 12:
        raise ValueError("--start-month must be between 1 and 12")
    if end_month < 1 or end_month > 12:
        raise ValueError("--end-month must be between 1 and 12")
    if end_month < start_month:
        raise ValueError("--end-month must be >= --start-month")

    chunks: list[tuple[str, str, str]] = []
    for month in range(start_month, end_month + 1):
        last_day = calendar.monthrange(year, month)[1]
        label = f"{year:04d}_{month:02d}"
        chunks.append(
            (
                f"{year:04d}-{month:02d}-01",
                f"{year:04d}-{month:02d}-{last_day:02d}",
                label,
            )
        )
    return chunks


def existing_month_payload(city_slug: str, chunk_label: str) -> bool:
    # Prefer timestamped validated naming, but treat legacy plain files as present.
    pattern = f"{city_slug}_{chunk_label}_historical_forecast_*.json"
    if any(OUT_DIR.glob(pattern)):
        return True

    legacy_plain = OUT_DIR / f"{city_slug}_{chunk_label}_historical_forecast.json"
    return legacy_plain.exists()


def get_latest_dim_city_file() -> Path:
    files = sorted(CITY_DIM_DIR.glob("dim_city_*.csv"))
    if not files:
        raise FileNotFoundError(f"No dim_city file found under: {CITY_DIM_DIR}")
    return files[-1]


def load_city_dimension() -> pd.DataFrame:
    path = get_latest_dim_city_file()
    df = pd.read_csv(path)
    required = {"city_name", "latitude", "longitude"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"dim_city file is missing required columns: {missing}")
    return df[["city_name", "latitude", "longitude"]].copy()


def fetch_city_month(city_name: str, latitude: float, longitude: float, start_date: str, end_date: str) -> dict:
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
    payload = response.json()

    payload["_request_meta"] = {
        "city_name": city_name,
        "latitude_requested": latitude,
        "longitude_requested": longitude,
        "start_date": start_date,
        "end_date": end_date,
    }
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill Open-Meteo historical forecast monthly validated raw JSON files "
            "using timestamped canonical naming."
        )
    )
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=3)
    parser.add_argument(
        "--run-stamp",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        help="File suffix stamp. Example: 20260406_120000",
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
    parser.add_argument(
        "--write-plain-compat",
        action="store_true",
        help="Also write legacy plain filename without timestamp suffix for compatibility.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    month_chunks = build_month_chunks(
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
    )

    cities = load_city_dimension()
    failures = []

    print(f"Loaded cities: {len(cities)}")
    print(f"Month chunks: {', '.join(chunk_label for _, _, chunk_label in month_chunks)}")
    print(f"Run stamp: {args.run_stamp}")

    for _, row in cities.iterrows():
        city_name = row["city_name"]
        latitude = float(row["latitude"])
        longitude = float(row["longitude"])
        city_slug = slugify_ascii(city_name)

        for start_date, end_date, chunk_label in month_chunks:
            out_path = OUT_DIR / f"{city_slug}_{chunk_label}_historical_forecast_{args.run_stamp}.json"

            if existing_month_payload(city_slug, chunk_label):
                print(f"SKIP {city_name} [{chunk_label}] -> payload already exists")
                continue

            try:
                print(f"FETCH {city_name} [{chunk_label}] ...")
                payload = fetch_city_month(
                    city_name=city_name,
                    latitude=latitude,
                    longitude=longitude,
                    start_date=start_date,
                    end_date=end_date,
                )
                out_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                if args.write_plain_compat:
                    plain_path = OUT_DIR / f"{city_slug}_{chunk_label}_historical_forecast.json"
                    if not plain_path.exists():
                        plain_path.write_text(
                            json.dumps(payload, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )

                print(f"  -> saved: {out_path.name}")
                time.sleep(args.sleep_seconds)

            except Exception as exc:
                failures.append(
                    {
                        "city_name": city_name,
                        "chunk_label": chunk_label,
                        "start_date": start_date,
                        "end_date": end_date,
                        "error": str(exc),
                    }
                )
                print(f"  -> FAILED: {city_name} [{chunk_label}] | {exc}")
                time.sleep(args.retry_sleep_seconds)

    if failures:
        fail_path = OUT_DIR / (
            f"historical_forecast_backfill_failures_{args.year}_{args.start_month:02d}_{args.end_month:02d}.json"
        )
        fail_path.write_text(
            json.dumps(failures, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\nCompleted with failures: {len(failures)}")
        print(f"Failure log written to: {fail_path}")
    else:
        print("\nCompleted with no failures.")


if __name__ == "__main__":
    main()
