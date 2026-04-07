from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from _shared import discover_project_root, normalize_turkish_city_name, slugify_ascii


PROJECT_ROOT = discover_project_root(Path(__file__))
DATA_ROOT = PROJECT_ROOT / "data" / "raw" / "open_meteo"

HIST_RAW_DIR = DATA_ROOT / "historical_weather" / "raw_json"
HIST_TIDY_DIR = DATA_ROOT / "historical_weather" / "tidy"
GEO_DIR = DATA_ROOT / "geocoding"

for folder in [HIST_RAW_DIR, HIST_TIDY_DIR, GEO_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
HIST_URL = "https://archive-api.open-meteo.com/v1/archive"


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


# Sabit il merkezi koordinatları - sorunlu geocoding kayıtlarını override eder
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


HOURLY_HIST_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "pressure_msl",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "is_day",
    "sunshine_duration",
]

# 2024'ü 4 parçaya bölerek çekiyoruz
DATE_CHUNKS_2024 = [
    ("2024-01-01", "2024-03-31", "Q1"),
    ("2024-04-01", "2024-06-30", "Q2"),
    ("2024-07-01", "2024-09-30", "Q3"),
    ("2024-10-01", "2024-12-31", "Q4"),
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
    path = geocoding_cache_path(city.city_name)
    path.write_text(
        json.dumps(asdict(city), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def safe_request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout: int = 120,
    max_retries: int = 6,
    sleep_seconds: float = 3.0,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(sleep_seconds * attempt)
            else:
                raise RuntimeError(
                    f"Request failed after {max_retries} attempts. "
                    f"URL={url} params={params}"
                ) from last_error

    raise RuntimeError("Unexpected request failure branch reached.")


def geocode_city(session: requests.Session, city_name: str) -> CityMeta:
    # 1) Override varsa her zaman override kazanır
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

    # 2) Sonra cache'e bak
    cached = load_cached_city(city_name)
    if cached is not None:
        return cached

    # 3) Sonra geocoding isteği at
    q = normalize_turkish_city_name(city_name)
    params = {
        "name": q,
        "count": 10,
        "language": "tr",
        "format": "json",
        "countryCode": "TR",
    }
    payload = safe_request_json(session, GEOCODING_URL, params, timeout=60, max_retries=4, sleep_seconds=1.5)

    results = payload.get("results", [])
    if not results:
        raise ValueError(f"Geocoding returned no results for city: {city_name}")

    best = None
    for row in results:
        if row.get("country_code") == "TR":
            best = row
            break

    if best is None:
        best = results[0]

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


def get_historical_weather_for_city_chunk(
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
        "hourly": ",".join(HOURLY_HIST_VARS),
        "models": "era5_land",
        "timezone": "auto",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "timeformat": "iso8601",
    }
    return safe_request_json(session, HIST_URL, params)


def parse_hourly(city: CityMeta, payload: dict[str, Any], chunk_label: str) -> pd.DataFrame:
    hourly = payload.get("hourly")
    if not hourly:
        return pd.DataFrame()

    df = pd.DataFrame(hourly)
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
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": "breathwise-tr/0.2"})

    geocoding_records: list[dict[str, Any]] = []
    hourly_frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []

    run_date = pd.Timestamp.now(tz="Europe/Istanbul").strftime("%Y-%m-%d")
    run_stamp = pd.Timestamp.now(tz="Europe/Istanbul").strftime("%Y%m%d_%H%M%S")

    for idx, city_name in enumerate(PROVINCES_TR, start=1):
        print(f"[{idx:02d}/{len(PROVINCES_TR)}] Processing {city_name} ...")

        try:
            city = geocode_city(session, city_name)
            geocoding_records.append({
                "city_name": city.city_name,
                "latitude": city.latitude,
                "longitude": city.longitude,
                "elevation": city.elevation,
                "timezone": city.timezone,
                "country_code": city.country_code,
                "admin1": city.admin1,
                "population": city.population,
            })

            city_ok = True

            for start_date, end_date, chunk_label in DATE_CHUNKS_2024:
                try:
                    payload = get_historical_weather_for_city_chunk(
                        session=session,
                        city=city,
                        start_date=start_date,
                        end_date=end_date,
                    )

                    raw_json_path = HIST_RAW_DIR / (
                        f"{slugify_ascii(city_name)}_{chunk_label}_historical_weather_{run_stamp}.json"
                    )
                    save_json(raw_json_path, payload)

                    hourly_df = parse_hourly(city, payload, chunk_label)
                    if not hourly_df.empty:
                        hourly_frames.append(hourly_df)

                    time.sleep(0.8)

                except Exception as exc:
                    city_ok = False
                    failures.append({
                        "city_name": city_name,
                        "chunk_label": chunk_label,
                        "start_date": start_date,
                        "end_date": end_date,
                        "error": str(exc),
                    })
                    print(f"  -> FAILED for {city_name} [{chunk_label}]: {exc}")

            if city_ok:
                print(f"  -> OK for {city_name}")

        except Exception as exc:
            failures.append({
                "city_name": city_name,
                "chunk_label": "ALL",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "error": str(exc),
            })
            print(f"  -> FAILED for {city_name} [ALL]: {exc}")

    geocoding_df = pd.DataFrame(geocoding_records)
    if not geocoding_df.empty:
        geocoding_df.to_csv(
            GEO_DIR / f"dim_city_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    if hourly_frames:
        all_hourly = pd.concat(hourly_frames, ignore_index=True)

        # Aynı şehir/chunk/time duplicate oluşursa temizle
        subset_cols = ["city_name", "chunk_label", "time"]
        all_hourly = all_hourly.drop_duplicates(subset=subset_cols).reset_index(drop=True)

        all_hourly.to_csv(
            HIST_TIDY_DIR / f"historical_weather_hourly_tr_2024_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    if failures:
        pd.DataFrame(failures).to_csv(
            HIST_TIDY_DIR / f"historical_weather_failures_tr_2024_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )
        print(f"\nCompleted with {len(failures)} failed chunk(s).")
    else:
        print("\nCompleted with no failures.")

    print("\nFiles written to:")
    print(f"- {GEO_DIR}")
    print(f"- {HIST_RAW_DIR}")
    print(f"- {HIST_TIDY_DIR}")


if __name__ == "__main__":
    main()
    
