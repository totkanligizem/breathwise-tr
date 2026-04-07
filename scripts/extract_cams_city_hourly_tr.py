from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd
import xarray as xr

from _shared import discover_project_root


PROJECT_ROOT = discover_project_root(Path(__file__))
CAMS_ROOT = PROJECT_ROOT / "data" / "raw" / "cams"
GEO_DIR = PROJECT_ROOT / "data" / "raw" / "open_meteo" / "geocoding"
OUT_DIR = PROJECT_ROOT / "data" / "processed" / "cams"

OUT_DIR.mkdir(parents=True, exist_ok=True)

POLLUTANTS = ["co", "no2", "o3", "pm10", "pm2p5", "so2"]
MONTH_FOLDER_PATTERN = re.compile(r"^(?P<month_key>\d{4}_\d{2})_interim_surface_ensemble$")


def load_city_dim() -> pd.DataFrame:
    csv_candidates = sorted(GEO_DIR.glob("dim_city_*.csv"))
    if not csv_candidates:
        raise FileNotFoundError(f"No dim_city CSV found in: {GEO_DIR}")

    latest = csv_candidates[-1]
    df = pd.read_csv(latest)

    required_cols = {"city_name", "latitude", "longitude"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {latest.name}: {missing}")

    df = (
        df[["city_name", "latitude", "longitude", "timezone", "admin1", "country_code"]]
        .drop_duplicates(subset=["city_name"])
        .sort_values("city_name")
        .reset_index(drop=True)
    )
    return df


def extract_variable_for_cities(
    ds: xr.Dataset,
    var_name: str,
    cities: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []

    for _, city in cities.iterrows():
        city_name = city["city_name"]
        lat = float(city["latitude"])
        lon = float(city["longitude"])

        point = ds[var_name].sel(lat=lat, lon=lon, method="nearest")
        point_df = point.to_dataframe(name=var_name).reset_index()

        selected_lat = float(point_df["lat"].iloc[0])
        selected_lon = float(point_df["lon"].iloc[0])

        point_df["city_name"] = city_name
        point_df["latitude_requested"] = lat
        point_df["longitude_requested"] = lon
        point_df["latitude_used"] = selected_lat
        point_df["longitude_used"] = selected_lon

        rows.append(
            point_df[
                [
                    "city_name",
                    "time",
                    "latitude_requested",
                    "longitude_requested",
                    "latitude_used",
                    "longitude_used",
                    var_name,
                ]
            ]
        )

    return pd.concat(rows, ignore_index=True)


def discover_available_months() -> list[str]:
    months: list[str] = []
    for path in sorted(CAMS_ROOT.iterdir()):
        if not path.is_dir():
            continue
        match = MONTH_FOLDER_PATTERN.match(path.name)
        if match:
            months.append(match.group("month_key"))
    return months


def month_dir(month_key: str) -> Path:
    return CAMS_ROOT / f"{month_key}_interim_surface_ensemble"


def nc_path(month_key: str, pollutant: str) -> Path:
    return month_dir(month_key) / f"cams_eu_aq_interim_{month_key}_surface_ensemble_{pollutant}.nc"


def process_month(month_key: str, cities: pd.DataFrame) -> pd.DataFrame:
    merged: pd.DataFrame | None = None

    for pollutant in POLLUTANTS:
        path = nc_path(month_key, pollutant)
        if not path.exists():
            raise FileNotFoundError(f"Missing NetCDF file for month={month_key} pollutant={pollutant}: {path}")

        print(f"Processing {month_key} - {pollutant}: {path.name}")
        ds = xr.open_dataset(path)
        extracted = extract_variable_for_cities(ds, pollutant, cities)
        ds.close()

        if merged is None:
            merged = extracted
        else:
            merged = merged.merge(
                extracted[["city_name", "time", pollutant]],
                on=["city_name", "time"],
                how="inner",
            )

    if merged is None:
        raise RuntimeError(f"No CAMS data extracted for month={month_key}")

    city_meta = cities.rename(columns={"latitude": "city_latitude", "longitude": "city_longitude"})
    merged = merged.merge(city_meta, on="city_name", how="left")
    merged["source_month"] = month_key

    merged = merged.sort_values(["city_name", "time"]).reset_index(drop=True)

    month_out = OUT_DIR / f"cams_city_hourly_tr_{month_key}.csv"
    merged.to_csv(month_out, index=False, encoding="utf-8-sig")

    print(f"- month rows: {len(merged)}")
    print(f"- written: {month_out}")

    return merged


def build_combined(month_frames: list[pd.DataFrame]) -> tuple[pd.DataFrame, int]:
    if not month_frames:
        return pd.DataFrame(), 0

    combined = pd.concat(month_frames, ignore_index=True)
    before = len(combined)

    combined = combined.sort_values(["city_name", "time", "source_month"])
    combined = combined.drop_duplicates(subset=["city_name", "time"], keep="last")
    dedup_removed = before - len(combined)

    combined = combined.sort_values(["city_name", "time"]).reset_index(drop=True)
    return combined, dedup_removed


def write_manifest(
    months: list[str],
    month_row_counts: dict[str, int],
    combined_rows: int,
    dedup_removed: int,
) -> Path:
    payload = {
        "generated_at_utc": pd.Timestamp.now("UTC").isoformat(),
        "available_months": months,
        "monthly_row_counts": month_row_counts,
        "combined_rows": combined_rows,
        "combined_duplicates_removed": dedup_removed,
        "city_count": 81,
    }

    manifest_path = OUT_DIR / "cams_city_hourly_manifest.json"
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def parse_months_arg(months_arg: str | None, available_months: list[str]) -> list[str]:
    if not months_arg:
        return available_months

    requested = [x.strip() for x in months_arg.split(",") if x.strip()]
    invalid = [x for x in requested if x not in available_months]
    if invalid:
        raise ValueError(
            "Requested month(s) are not available in raw CAMS folders: " + ", ".join(invalid)
        )
    return requested


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Extract CAMS interim surface ensemble NetCDF files to city-hourly CSV outputs. "
            "Processes all available month folders by default."
        )
    )
    parser.add_argument(
        "--months",
        default=None,
        help="Optional comma-separated list like 2024_01,2024_02. Defaults to all available months.",
    )
    parser.add_argument(
        "--skip-combined",
        action="store_true",
        help="If set, only monthly outputs are written.",
    )
    args = parser.parse_args()

    cities = load_city_dim()
    print(f"Loaded cities: {len(cities)}")

    available_months = discover_available_months()
    if not available_months:
        raise FileNotFoundError(
            f"No CAMS raw month folders found under {CAMS_ROOT}. Expected *_interim_surface_ensemble directories."
        )

    months = parse_months_arg(args.months, available_months)
    print(f"Months to process: {', '.join(months)}")

    month_frames: list[pd.DataFrame] = []
    month_row_counts: dict[str, int] = {}

    for month_key in months:
        frame = process_month(month_key, cities)
        month_frames.append(frame)
        month_row_counts[month_key] = int(len(frame))

    combined_rows = 0
    dedup_removed = 0

    if not args.skip_combined:
        combined, dedup_removed = build_combined(month_frames)
        if not combined.empty:
            combined_path = OUT_DIR / "cams_city_hourly_tr_all_available.csv"
            combined.to_csv(combined_path, index=False, encoding="utf-8-sig")
            combined_rows = int(len(combined))
            print(f"Combined written: {combined_path}")
            print(f"Combined rows: {combined_rows}")
            print(f"Combined duplicates removed: {dedup_removed}")

    manifest_path = write_manifest(months, month_row_counts, combined_rows, dedup_removed)
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
