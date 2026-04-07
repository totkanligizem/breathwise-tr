from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from _shared import discover_project_root, normalize_turkish_city_name, slugify_ascii


PROJECT_ROOT = discover_project_root(Path(__file__))
DATA_ROOT = PROJECT_ROOT / "data" / "raw" / "open_meteo"

HIST_FC_BASE_DIR = DATA_ROOT / "historical_forecast"
HIST_FC_RAW_DIR = HIST_FC_BASE_DIR / "raw_json"
HIST_FC_TIDY_DIR = HIST_FC_BASE_DIR / "tidy"
HIST_FC_RAW_EXPERIMENTAL_DAILY_DIR = HIST_FC_RAW_DIR / "experimental" / "daily_chunks"
HIST_FC_RAW_VALIDATED_MONTHLY_DIR = HIST_FC_RAW_DIR / "validated" / "monthly_chunks"
HIST_FC_RAW_CANONICAL_SHORT_RANGE_DIR = HIST_FC_RAW_DIR / "canonical" / "short_range"
HIST_FC_TIDY_EXPERIMENTAL_DIR = HIST_FC_TIDY_DIR / "experimental"
HIST_FC_TIDY_VALIDATED_DIR = HIST_FC_TIDY_DIR / "validated"
HIST_FC_TIDY_CANONICAL_DIR = HIST_FC_TIDY_DIR / "canonical"
GEO_DIR = DATA_ROOT / "geocoding"

for folder in [
    HIST_FC_RAW_EXPERIMENTAL_DAILY_DIR,
    HIST_FC_RAW_VALIDATED_MONTHLY_DIR,
    HIST_FC_RAW_CANONICAL_SHORT_RANGE_DIR,
    HIST_FC_TIDY_EXPERIMENTAL_DIR,
    HIST_FC_TIDY_VALIDATED_DIR,
    HIST_FC_TIDY_CANONICAL_DIR,
    GEO_DIR,
]:
    folder.mkdir(parents=True, exist_ok=True)


GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
HIST_FC_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

RUN_YEAR = 2024
RUN_START = f"{RUN_YEAR}-01-01"
RUN_END = f"{RUN_YEAR}-12-31"

PROVINCES_TR = [
    "Adana", "Adıyaman", "Afyonkarahisar", "Ağrı", "Aksaray", "Amasya", "Ankara",
    "Antalya", "Ardahan", "Artvin", "Aydın", "Balıkesir", "Bartın", "Batman",
    "Bayburt", "Bilecik", "Bingöl", "Bitlis", "Bolu", "Burdur", "Bursa",
    "Çanakkale", "Çankırı", "Çorum", "Denizli", "Diyarbakır", "Düzce",
    "Edirne", "Elazığ", "Erzincan", "Erzurum", "Eskişehir", "Gaziantep",
    "Giresun", "Gümüşhane", "Hakkâri", "Hatay", "Iğdır", "Isparta", "İstanbul",
    "İzmir", "Kahramanmaraş", "Karabük", "Karaman", "Kars", "Kastamonu",
    "Kayseri", "Kilis", "Kırıkkale", "Kırklareli", "Kırşehir", "Kocaeli",
    "Konya", "Kütahya", "Malatya", "Manisa", "Mardin", "Mersin", "Muğla",
    "Muş", "Nevşehir", "Niğde", "Ordu", "Osmaniye", "Rize", "Sakarya",
    "Samsun", "Siirt", "Sinop", "Sivas", "Şanlıurfa", "Şırnak", "Tekirdağ",
    "Tokat", "Trabzon", "Tunceli", "Uşak", "Van", "Yalova", "Yozgat", "Zonguldak",
]

# İstersen sadece sorunlu şehirleri çalıştır:
# TARGET_CITIES = ["Adana", "Kilis", "Kırıkkale"]
TARGET_CITIES: list[str] | None = None

CITY_OVERRIDES = {
    "Ağrı": {
        "latitude": 39.7191,
        "longitude": 43.0503,
        "elevation": None,
        "timezone": "Europe/Istanbul",
        "country_code": "TR",
        "admin1": "Ağrı",
        "population": None,
    },
    "Bartın": {
        "latitude": 41.6344,
        "longitude": 32.3375,
        "elevation": None,
        "timezone": "Europe/Istanbul",
        "country_code": "TR",
        "admin1": "Bartın",
        "population": None,
    },
    "Çankırı": {
        "latitude": 40.6013,
        "longitude": 33.6134,
        "elevation": None,
        "timezone": "Europe/Istanbul",
        "country_code": "TR",
        "admin1": "Çankırı",
        "population": None,
    },
    "Iğdır": {
        "latitude": 39.9237,
        "longitude": 44.0450,
        "elevation": None,
        "timezone": "Europe/Istanbul",
        "country_code": "TR",
        "admin1": "Iğdır",
        "population": None,
    },
    "Kırıkkale": {
        "latitude": 39.8468,
        "longitude": 33.5153,
        "elevation": None,
        "timezone": "Europe/Istanbul",
        "country_code": "TR",
        "admin1": "Kırıkkale",
        "population": None,
    },
    "Kırklareli": {
        "latitude": 41.7351,
        "longitude": 27.2252,
        "elevation": None,
        "timezone": "Europe/Istanbul",
        "country_code": "TR",
        "admin1": "Kırklareli",
        "population": None,
    },
    "Kırşehir": {
        "latitude": 39.1458,
        "longitude": 34.1605,
        "elevation": None,
        "timezone": "Europe/Istanbul",
        "country_code": "TR",
        "admin1": "Kırşehir",
        "population": None,
    },
    "Şanlıurfa": {
        "latitude": 37.1674,
        "longitude": 38.7955,
        "elevation": None,
        "timezone": "Europe/Istanbul",
        "country_code": "TR",
        "admin1": "Şanlıurfa",
        "population": None,
    },
}

HOURLY_HIST_FC_VARS = [
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


@dataclass
class CityMeta:
    city_name: str
    latitude: float
    longitude: float
    elevation: float | None
    timezone: str | None
    country_code: str | None
    admin1: str | None
    population: int | None


def build_daily_chunks(start_date: str, end_date: str) -> list[tuple[str, str, str]]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    chunks: list[tuple[str, str, str]] = []
    cur = start
    i = 1
    while cur <= end:
        label = f"D{i:03d}"
        chunks.append((cur.isoformat(), cur.isoformat(), label))
        cur += timedelta(days=1)
        i += 1
    return chunks


DAILY_CHUNKS = build_daily_chunks(RUN_START, RUN_END)


def geocoding_cache_path(city_name: str) -> Path:
    return GEO_DIR / f"{slugify_ascii(city_name)}_geocoding.json"


def load_cached_city(city_name: str) -> CityMeta | None:
    path = geocoding_cache_path(city_name)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return CityMeta(
        city_name=data["city_name"],
        latitude=float(data["latitude"]),
        longitude=float(data["longitude"]),
        elevation=data.get("elevation"),
        timezone=data.get("timezone"),
        country_code=data.get("country_code"),
        admin1=data.get("admin1"),
        population=data.get("population"),
    )


def save_cached_city(city: CityMeta) -> None:
    geocoding_cache_path(city.city_name).write_text(
        json.dumps(asdict(city), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def safe_request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout: int = 120,
    max_retries: int = 6,
    sleep_seconds: float = 4.0,
) -> dict[str, Any]:
    last_error: Exception | None = None
    last_status: int | None = None
    last_text: str | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)
            last_status = response.status_code
            last_text = response.text[:500]
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(sleep_seconds * attempt)
            else:
                extra = f" status={last_status}" if last_status is not None else ""
                if last_text:
                    extra += f" body={last_text!r}"
                raise RuntimeError(
                    f"Request failed after {max_retries} attempts. URL={url} params={params}{extra}"
                ) from last_error

    raise RuntimeError("Unexpected request failure branch reached.")


def geocode_city(session: requests.Session, city_name: str) -> CityMeta:
    if city_name in CITY_OVERRIDES:
        row = CITY_OVERRIDES[city_name]
        city = CityMeta(
            city_name=city_name,
            latitude=float(row["latitude"]),
            longitude=float(row["longitude"]),
            elevation=row.get("elevation"),
            timezone=row.get("timezone"),
            country_code=row.get("country_code"),
            admin1=row.get("admin1"),
            population=row.get("population"),
        )
        save_cached_city(city)
        return city

    cached = load_cached_city(city_name)
    if cached is not None:
        return cached

    q = normalize_turkish_city_name(city_name)
    params = {
        "name": q,
        "count": 10,
        "language": "tr",
        "format": "json",
        "countryCode": "TR",
    }
    payload = safe_request_json(
        session=session,
        url=GEOCODING_URL,
        params=params,
        timeout=60,
        max_retries=4,
        sleep_seconds=1.5,
    )

    results = payload.get("results", [])
    if not results:
        raise ValueError(f"Geocoding returned no results for city: {city_name}")

    best = next((row for row in results if row.get("country_code") == "TR"), results[0])

    city = CityMeta(
        city_name=city_name,
        latitude=float(best["latitude"]),
        longitude=float(best["longitude"]),
        elevation=best.get("elevation"),
        timezone=best.get("timezone"),
        country_code=best.get("country_code"),
        admin1=best.get("admin1"),
        population=best.get("population"),
    )
    save_cached_city(city)
    return city


def get_historical_forecast_for_city_chunk(
    session: requests.Session,
    city: CityMeta,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_HIST_FC_VARS),
        # Best match varsayılan; explicit models kaldırıldı
        "timezone": "auto",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "timeformat": "iso8601",
    }
    return safe_request_json(session, HIST_FC_URL, params)


def parse_hourly(city: CityMeta, payload: dict[str, Any], chunk_label: str) -> pd.DataFrame:
    hourly = payload.get("hourly")
    if not hourly:
        return pd.DataFrame()

    df = pd.DataFrame(hourly)
    if df.empty:
        return df

    df.insert(0, "city_name", city.city_name)
    df["chunk_label"] = chunk_label
    df["latitude_requested"] = city.latitude
    df["longitude_requested"] = city.longitude
    df["latitude_used"] = payload.get("latitude")
    df["longitude_used"] = payload.get("longitude")
    df["elevation_used"] = payload.get("elevation")
    df["timezone"] = payload.get("timezone")
    df["timezone_abbreviation"] = payload.get("timezone_abbreviation")
    df["utc_offset_seconds"] = payload.get("utc_offset_seconds")
    df["generationtime_ms"] = payload.get("generationtime_ms")
    return df


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def raw_json_path(city_name: str, chunk_label: str) -> Path:
    return HIST_FC_RAW_EXPERIMENTAL_DAILY_DIR / f"{slugify_ascii(city_name)}_{chunk_label}.json"


def build_final_hourly_from_raw(run_date: str) -> None:
    frames: list[pd.DataFrame] = []

    candidate_dirs = [HIST_FC_RAW_EXPERIMENTAL_DAILY_DIR, HIST_FC_RAW_DIR]
    raw_files: list[Path] = []
    for raw_dir in candidate_dirs:
        if raw_dir.exists():
            raw_files.extend(sorted(raw_dir.glob("*.json")))

    for json_path in raw_files:
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            stem = json_path.stem  # adana_D001
            match = re.match(r"(.+)_(D\d{3})$", stem)
            if not match:
                continue
            city_part, chunk_label = match.group(1), match.group(2)

            city_name = next((c for c in PROVINCES_TR if slugify_ascii(c) == city_part), None)
            if city_name is None:
                continue

            city = CityMeta(
                city_name=city_name,
                latitude=float(payload.get("latitude", 0.0)),
                longitude=float(payload.get("longitude", 0.0)),
                elevation=payload.get("elevation"),
                timezone=payload.get("timezone"),
                country_code="TR",
                admin1=None,
                population=None,
            )

            df = parse_hourly(city, payload, chunk_label)
            if not df.empty:
                frames.append(df)

        except Exception as exc:
            print(f"  -> SKIP broken raw file {json_path.name}: {exc}")

    if not frames:
        print("No raw JSON files found to build final hourly dataset.")
        return

    all_hourly = pd.concat(frames, ignore_index=True)
    all_hourly = all_hourly.drop_duplicates(subset=["city_name", "chunk_label", "time"]).reset_index(drop=True)

    out_path = HIST_FC_TIDY_EXPERIMENTAL_DIR / f"historical_forecast_hourly_tr_{RUN_YEAR}_{run_date}.csv"
    all_hourly.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Final hourly file written: {out_path}")


def main() -> None:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "breathwise-tr/0.4",
            "Accept": "application/json",
        }
    )

    cities = TARGET_CITIES if TARGET_CITIES is not None else PROVINCES_TR

    geocoding_records: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []

    run_date = pd.Timestamp.now(tz="Europe/Istanbul").strftime("%Y-%m-%d")

    for idx, city_name in enumerate(cities, start=1):
        print(f"[{idx:02d}/{len(cities)}] Processing {city_name} ...")

        try:
            city = geocode_city(session, city_name)
            geocoding_records.append(
                {
                    "city_name": city.city_name,
                    "latitude": city.latitude,
                    "longitude": city.longitude,
                    "elevation": city.elevation,
                    "timezone": city.timezone,
                    "country_code": city.country_code,
                    "admin1": city.admin1,
                    "population": city.population,
                }
            )

            city_ok = True

            for start_date, end_date, chunk_label in DAILY_CHUNKS:
                path = raw_json_path(city_name, chunk_label)

                # daha önce inmişse geç
                if path.exists():
                    continue

                try:
                    payload = get_historical_forecast_for_city_chunk(
                        session=session,
                        city=city,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    save_json(path, payload)
                    time.sleep(1.5)

                except Exception as exc:
                    city_ok = False
                    failures.append(
                        {
                            "city_name": city_name,
                            "chunk_label": chunk_label,
                            "start_date": start_date,
                            "end_date": end_date,
                            "error": str(exc),
                        }
                    )
                    print(f"  -> FAILED for {city_name} [{chunk_label}]: {exc}")

            if city_ok:
                print(f"  -> OK for {city_name}")

        except Exception as exc:
            failures.append(
                {
                    "city_name": city_name,
                    "chunk_label": "ALL",
                    "start_date": RUN_START,
                    "end_date": RUN_END,
                    "error": str(exc),
                }
            )
            print(f"  -> FAILED for {city_name} [ALL]: {exc}")

    if geocoding_records:
        pd.DataFrame(geocoding_records).to_csv(
            GEO_DIR / f"dim_city_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    build_final_hourly_from_raw(run_date)

    if failures:
        pd.DataFrame(failures).to_csv(
            HIST_FC_TIDY_EXPERIMENTAL_DIR / f"historical_forecast_failures_tr_{RUN_YEAR}_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )
        print(f"\nCompleted with {len(failures)} failed chunk(s).")
    else:
        print("\nCompleted with no failures.")

    print("\nFiles written to:")
    print(f"- {GEO_DIR}")
    print(f"- {HIST_FC_RAW_EXPERIMENTAL_DAILY_DIR}")
    print(f"- {HIST_FC_TIDY_EXPERIMENTAL_DIR}")


if __name__ == "__main__":
    main()
    
