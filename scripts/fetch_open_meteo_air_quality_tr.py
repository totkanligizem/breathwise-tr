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

AQ_RAW_DIR = DATA_ROOT / "air_quality" / "raw_json"
AQ_TIDY_DIR = DATA_ROOT / "air_quality" / "tidy"
GEO_DIR = DATA_ROOT / "geocoding"

for folder in [AQ_RAW_DIR, AQ_TIDY_DIR, GEO_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"


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


HOURLY_AQ_VARS = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "uv_index",
    "european_aqi",
    "european_aqi_pm2_5",
    "european_aqi_pm10",
    "european_aqi_ozone",
    "european_aqi_nitrogen_dioxide",
    "european_aqi_sulphur_dioxide",
]

CURRENT_AQ_VARS = [
    "european_aqi",
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "uv_index",
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
    timeout: int = 60,
    max_retries: int = 4,
    sleep_seconds: float = 1.5,
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
    cached = load_cached_city(city_name)
    if cached is not None:
        return cached

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

    q = normalize_turkish_city_name(city_name)
    params = {
        "name": q,
        "count": 10,
        "language": "tr",
        "format": "json",
        "countryCode": "TR",
    }
    payload = safe_request_json(session, GEOCODING_URL, params)

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


def get_air_quality_for_city(session: requests.Session, city: CityMeta) -> dict[str, Any]:
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": ",".join(HOURLY_AQ_VARS),
        "current": ",".join(CURRENT_AQ_VARS),
        "timezone": "auto",
        "past_days": 7,
        "forecast_days": 5,
        "domains": "cams_europe",
        "timeformat": "iso8601",
    }
    return safe_request_json(session, AQ_URL, params)


def parse_current(city: CityMeta, payload: dict[str, Any]) -> pd.DataFrame:
    current = payload.get("current")
    if not current:
        return pd.DataFrame()

    base = {
        "city_name": city.city_name,
        "latitude_requested": city.latitude,
        "longitude_requested": city.longitude,
        "latitude_used": payload.get("latitude"),
        "longitude_used": payload.get("longitude"),
        "elevation_used": payload.get("elevation"),
        "timezone": payload.get("timezone"),
        "timezone_abbreviation": payload.get("timezone_abbreviation"),
        "utc_offset_seconds": payload.get("utc_offset_seconds"),
        "generationtime_ms": payload.get("generationtime_ms"),
    }

    row = {**base, **current}
    return pd.DataFrame([row])


def parse_hourly(city: CityMeta, payload: dict[str, Any]) -> pd.DataFrame:
    hourly = payload.get("hourly")
    if not hourly:
        return pd.DataFrame()

    df = pd.DataFrame(hourly)
    df.insert(0, "city_name", city.city_name)
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
    session.headers.update({"User-Agent": "breathwise-tr/0.1"})

    geocoding_records: list[dict[str, Any]] = []
    current_frames: list[pd.DataFrame] = []
    hourly_frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []

    run_date = pd.Timestamp.now(tz="Europe/Istanbul").strftime("%Y-%m-%d")
    run_stamp = pd.Timestamp.now(tz="Europe/Istanbul").strftime("%Y%m%d_%H%M%S")

    for idx, city_name in enumerate(PROVINCES_TR, start=1):
        try:
            print(f"[{idx:02d}/{len(PROVINCES_TR)}] Processing {city_name} ...")

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

            payload = get_air_quality_for_city(session, city)

            raw_json_path = AQ_RAW_DIR / f"{slugify_ascii(city_name)}_air_quality_{run_stamp}.json"
            save_json(raw_json_path, payload)

            current_df = parse_current(city, payload)
            hourly_df = parse_hourly(city, payload)

            if not current_df.empty:
                current_frames.append(current_df)
            if not hourly_df.empty:
                hourly_frames.append(hourly_df)

            time.sleep(0.5)

        except Exception as exc:
            failures.append({"city_name": city_name, "error": str(exc)})
            print(f"  -> FAILED for {city_name}: {exc}")

    geocoding_df = pd.DataFrame(geocoding_records)
    if not geocoding_df.empty:
        geocoding_df.to_csv(
            GEO_DIR / f"dim_city_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    if current_frames:
        all_current = pd.concat(current_frames, ignore_index=True)
        all_current.to_csv(
            AQ_TIDY_DIR / f"air_quality_current_tr_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    if hourly_frames:
        all_hourly = pd.concat(hourly_frames, ignore_index=True)
        all_hourly.to_csv(
            AQ_TIDY_DIR / f"air_quality_hourly_tr_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    if failures:
        pd.DataFrame(failures).to_csv(
            AQ_TIDY_DIR / f"air_quality_failures_tr_{run_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )
        print(f"\nCompleted with {len(failures)} failures.")
    else:
        print("\nCompleted with no failures.")

    print("\nFiles written to:")
    print(f"- {GEO_DIR}")
    print(f"- {AQ_RAW_DIR}")
    print(f"- {AQ_TIDY_DIR}")


if __name__ == "__main__":
    main()
    
