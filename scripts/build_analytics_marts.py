from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from _shared import discover_project_root


FULL_REBUILD_SENSITIVE_INPUTS = {
    "dim_city",
    "dim_province",
    "historical_weather_hourly",
    "historical_forecast_hourly",
    "cams_city_hourly",
}


@dataclass(frozen=True)
class InputPaths:
    dim_city: Path
    dim_province: Path
    forecast_current: Path
    forecast_hourly: Path
    air_quality_current: Path
    air_quality_hourly: Path
    historical_weather_hourly: Path
    historical_forecast_hourly: Path
    cams_city_hourly: Path


@dataclass(frozen=True)
class RefreshDecision:
    mode: str
    reasons: list[str]


def latest_file(folder: Path, pattern: str) -> Path:
    candidates = sorted(folder.glob(pattern))
    if not candidates:
        raise FileNotFoundError(f"No files matched pattern={pattern!r} in {folder}")
    return candidates[-1]


def resolve_historical_forecast_input(open_meteo_root: Path) -> Path:
    tidy_root = open_meteo_root / "historical_forecast" / "tidy"
    canonical_dir = tidy_root / "canonical"
    validated_dir = tidy_root / "validated"

    # Preferred: extended canonical coverage produced from validated monthly raws.
    extended_canonical = sorted(canonical_dir.glob("historical_forecast_hourly_tr_*_extended_full.csv"))
    if extended_canonical:
        return extended_canonical[-1]

    # Fallback: validated tidy full exports.
    validated_full = sorted(validated_dir.glob("historical_forecast_hourly_tr_*_validated*_full.csv"))
    if validated_full:
        return validated_full[-1]

    # Legacy canonical short-range set.
    legacy_full = canonical_dir / "historical_forecast_hourly_tr_2024_01_15_2024_01_21_full.csv"
    legacy_light = canonical_dir / "historical_forecast_hourly_tr_2024_01_15_2024_01_21_light.csv"
    if legacy_full.exists():
        return legacy_full
    if legacy_light.exists():
        return legacy_light

    raise FileNotFoundError(
        "Expected at least one historical forecast tidy file in canonical/validated folders."
    )


def resolve_cams_input(project_root: Path) -> Path:
    cams_dir = project_root / "data" / "processed" / "cams"

    preferred = cams_dir / "cams_city_hourly_tr_all_available.csv"
    if preferred.exists():
        return preferred

    month_candidates = sorted(cams_dir.glob("cams_city_hourly_tr_????_??.csv"))
    if month_candidates:
        return month_candidates[-1]

    legacy = cams_dir / "cams_city_hourly_tr_2024_01.csv"
    if legacy.exists():
        return legacy

    raise FileNotFoundError(
        "No processed CAMS city-hourly file found. Expected cams_city_hourly_tr_all_available.csv "
        "or cams_city_hourly_tr_YYYY_MM.csv"
    )


def infer_forecast_validation_window(source_path: Path) -> str:
    stem = source_path.stem
    prefix = "historical_forecast_hourly_tr_"
    if stem.startswith(prefix):
        stem = stem[len(prefix) :]
    if stem.endswith("_full"):
        stem = stem[: -len("_full")]
    return stem or "historical_forecast_unknown"


def resolve_inputs(project_root: Path) -> InputPaths:
    open_meteo_root = project_root / "data" / "raw" / "open_meteo"

    return InputPaths(
        dim_city=latest_file(open_meteo_root / "geocoding", "dim_city_*.csv"),
        dim_province=project_root / "data" / "processed" / "geography" / "dim_province_tr.csv",
        forecast_current=latest_file(
            open_meteo_root / "forecast" / "tidy", "forecast_current_tr_*.csv"
        ),
        forecast_hourly=latest_file(
            open_meteo_root / "forecast" / "tidy", "forecast_hourly_tr_*.csv"
        ),
        air_quality_current=latest_file(
            open_meteo_root / "air_quality" / "tidy", "air_quality_current_tr_*.csv"
        ),
        air_quality_hourly=latest_file(
            open_meteo_root / "air_quality" / "tidy", "air_quality_hourly_tr_*.csv"
        ),
        historical_weather_hourly=latest_file(
            open_meteo_root / "historical_weather" / "tidy", "historical_weather_hourly_tr_*.csv"
        ),
        historical_forecast_hourly=resolve_historical_forecast_input(open_meteo_root),
        cams_city_hourly=resolve_cams_input(project_root),
    )


def sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def sql_text(value: str) -> str:
    return value.replace("'", "''")


def to_rel_path(project_root: Path, path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return resolved.as_posix()


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def timestamp_literal(value: datetime) -> str:
    ts = value.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    return f"timestamp '{ts}'"


def build_stage_views(
    con: duckdb.DuckDBPyConnection,
    inputs: InputPaths,
    refresh_start: datetime | None = None,
) -> None:
    # Keep stage views full-range to avoid CSV type-inference edge cases when filtered views are empty.
    _ = refresh_start
    time_filter = ""

    con.execute(
        f"""
        create or replace temp view dim_city as
        select
            city_name,
            latitude as city_latitude,
            longitude as city_longitude,
            timezone as city_timezone,
            admin1,
            country_code
        from read_csv_auto('{sql_path(inputs.dim_city)}', sample_size=-1)
        qualify row_number() over (partition by city_name order by city_name) = 1
        """
    )

    con.execute(
        f"""
        create or replace temp view dim_province as
        select
            province_name,
            shape_iso,
            shape_id,
            shape_group,
            shape_type,
            level
        from read_csv_auto('{sql_path(inputs.dim_province)}', sample_size=-1)
        """
    )

    con.execute(
        f"""
        create or replace temp view fw as
        select
            city_name,
            cast(time as timestamp) as time_ts,
            try_cast(temperature_2m as double) as fw_temperature_2m,
            try_cast(relative_humidity_2m as double) as fw_relative_humidity_2m,
            try_cast(apparent_temperature as double) as fw_apparent_temperature,
            try_cast(precipitation_probability as double) as fw_precipitation_probability,
            try_cast(precipitation as double) as fw_precipitation,
            try_cast(rain as double) as fw_rain,
            try_cast(snowfall as double) as fw_snowfall,
            try_cast(weather_code as integer) as fw_weather_code,
            try_cast(pressure_msl as double) as fw_pressure_msl,
            try_cast(cloud_cover as double) as fw_cloud_cover,
            try_cast(wind_speed_10m as double) as fw_wind_speed_10m,
            try_cast(wind_direction_10m as double) as fw_wind_direction_10m,
            try_cast(wind_gusts_10m as double) as fw_wind_gusts_10m,
            try_cast(uv_index as double) as fw_uv_index,
            try_cast(is_day as integer) as fw_is_day,
            try_cast(generationtime_ms as double) as fw_generationtime_ms
        from read_csv_auto('{sql_path(inputs.forecast_hourly)}', sample_size=-1)
        {time_filter}
        qualify row_number() over (
            partition by city_name, cast(time as timestamp)
            order by coalesce(generationtime_ms, 0) desc
        ) = 1
        """
    )

    con.execute(
        f"""
        create or replace temp view aq as
        select
            city_name,
            cast(time as timestamp) as time_ts,
            try_cast(pm10 as double) as aq_pm10,
            try_cast(pm2_5 as double) as aq_pm2_5,
            try_cast(carbon_monoxide as double) as aq_carbon_monoxide,
            try_cast(nitrogen_dioxide as double) as aq_nitrogen_dioxide,
            try_cast(sulphur_dioxide as double) as aq_sulphur_dioxide,
            try_cast(ozone as double) as aq_ozone,
            try_cast(uv_index as double) as aq_uv_index,
            try_cast(european_aqi as double) as aq_european_aqi,
            try_cast(european_aqi_pm2_5 as double) as aq_european_aqi_pm2_5,
            try_cast(european_aqi_pm10 as double) as aq_european_aqi_pm10,
            try_cast(european_aqi_ozone as double) as aq_european_aqi_ozone,
            try_cast(european_aqi_nitrogen_dioxide as double) as aq_european_aqi_nitrogen_dioxide,
            try_cast(european_aqi_sulphur_dioxide as double) as aq_european_aqi_sulphur_dioxide,
            try_cast(generationtime_ms as double) as aq_generationtime_ms
        from read_csv_auto('{sql_path(inputs.air_quality_hourly)}', sample_size=-1)
        {time_filter}
        qualify row_number() over (
            partition by city_name, cast(time as timestamp)
            order by coalesce(generationtime_ms, 0) desc
        ) = 1
        """
    )

    con.execute(
        f"""
        create or replace temp view hw as
        select
            city_name,
            cast(time as timestamp) as time_ts,
            try_cast(temperature_2m as double) as hw_temperature_2m,
            try_cast(relative_humidity_2m as double) as hw_relative_humidity_2m,
            try_cast(apparent_temperature as double) as hw_apparent_temperature,
            try_cast(precipitation as double) as hw_precipitation,
            try_cast(rain as double) as hw_rain,
            try_cast(snowfall as double) as hw_snowfall,
            try_cast(weather_code as integer) as hw_weather_code,
            try_cast(pressure_msl as double) as hw_pressure_msl,
            try_cast(cloud_cover as double) as hw_cloud_cover,
            try_cast(wind_speed_10m as double) as hw_wind_speed_10m,
            try_cast(wind_direction_10m as double) as hw_wind_direction_10m,
            try_cast(wind_gusts_10m as double) as hw_wind_gusts_10m,
            try_cast(is_day as integer) as hw_is_day,
            try_cast(sunshine_duration as double) as hw_sunshine_duration,
            try_cast(generationtime_ms as double) as hw_generationtime_ms
        from read_csv_auto('{sql_path(inputs.historical_weather_hourly)}', sample_size=-1)
        {time_filter}
        qualify row_number() over (
            partition by city_name, cast(time as timestamp)
            order by coalesce(generationtime_ms, 0) desc
        ) = 1
        """
    )

    con.execute(
        f"""
        create or replace temp view hf as
        select
            city_name,
            cast(time as timestamp) as time_ts,
            try_cast(temperature_2m as double) as hf_temperature_2m,
            try_cast(relative_humidity_2m as double) as hf_relative_humidity_2m,
            try_cast(apparent_temperature as double) as hf_apparent_temperature,
            try_cast(precipitation_probability as double) as hf_precipitation_probability,
            try_cast(precipitation as double) as hf_precipitation,
            try_cast(rain as double) as hf_rain,
            try_cast(snowfall as double) as hf_snowfall,
            try_cast(weather_code as integer) as hf_weather_code,
            try_cast(pressure_msl as double) as hf_pressure_msl,
            try_cast(cloud_cover as double) as hf_cloud_cover,
            try_cast(wind_speed_10m as double) as hf_wind_speed_10m,
            try_cast(wind_direction_10m as double) as hf_wind_direction_10m,
            try_cast(wind_gusts_10m as double) as hf_wind_gusts_10m,
            try_cast(uv_index as double) as hf_uv_index,
            try_cast(is_day as integer) as hf_is_day,
            try_cast(freezing_level_height as double) as hf_freezing_level_height,
            try_cast(generationtime_ms as double) as hf_generationtime_ms
        from read_csv_auto('{sql_path(inputs.historical_forecast_hourly)}', sample_size=-1)
        {time_filter}
        qualify row_number() over (
            partition by city_name, cast(time as timestamp)
            order by coalesce(generationtime_ms, 0) desc
        ) = 1
        """
    )

    con.execute(
        f"""
        create or replace temp view cams as
        select
            city_name,
            cast(time as timestamp) as time_ts,
            try_cast(co as double) as cams_co,
            try_cast(no2 as double) as cams_no2,
            try_cast(o3 as double) as cams_o3,
            try_cast(pm10 as double) as cams_pm10,
            try_cast(pm2p5 as double) as cams_pm2p5,
            try_cast(so2 as double) as cams_so2
        from read_csv_auto('{sql_path(inputs.cams_city_hourly)}', sample_size=-1)
        {time_filter}
        qualify row_number() over (
            partition by city_name, cast(time as timestamp)
            order by city_name
        ) = 1
        """
    )

    con.execute(
        f"""
        create or replace temp view forecast_current as
        select
            city_name,
            cast(time as timestamp) as fc_time_ts,
            try_cast(temperature_2m as double) as forecast_temperature_2m,
            try_cast(relative_humidity_2m as double) as forecast_relative_humidity_2m,
            try_cast(apparent_temperature as double) as forecast_apparent_temperature,
            try_cast(precipitation as double) as forecast_precipitation,
            try_cast(weather_code as integer) as forecast_weather_code,
            try_cast(cloud_cover as double) as forecast_cloud_cover,
            try_cast(wind_speed_10m as double) as forecast_wind_speed_10m,
            try_cast(wind_direction_10m as double) as forecast_wind_direction_10m,
            try_cast(wind_gusts_10m as double) as forecast_wind_gusts_10m,
            try_cast(is_day as integer) as forecast_is_day,
            try_cast(generationtime_ms as double) as forecast_generationtime_ms
        from read_csv_auto('{sql_path(inputs.forecast_current)}', sample_size=-1)
        qualify row_number() over (
            partition by city_name
            order by cast(time as timestamp) desc, coalesce(generationtime_ms, 0) desc
        ) = 1
        """
    )

    con.execute(
        f"""
        create or replace temp view air_quality_current as
        select
            city_name,
            cast(time as timestamp) as aq_current_time_ts,
            try_cast(pm10 as double) as aq_current_pm10,
            try_cast(pm2_5 as double) as aq_current_pm2_5,
            try_cast(carbon_monoxide as double) as aq_current_carbon_monoxide,
            try_cast(nitrogen_dioxide as double) as aq_current_nitrogen_dioxide,
            try_cast(sulphur_dioxide as double) as aq_current_sulphur_dioxide,
            try_cast(ozone as double) as aq_current_ozone,
            try_cast(european_aqi as double) as aq_current_european_aqi,
            try_cast(generationtime_ms as double) as aq_current_generationtime_ms
        from read_csv_auto('{sql_path(inputs.air_quality_current)}', sample_size=-1)
        qualify row_number() over (
            partition by city_name
            order by cast(time as timestamp) desc, coalesce(generationtime_ms, 0) desc
        ) = 1
        """
    )


def build_city_hourly_environment(
    con: duckdb.DuckDBPyConnection,
    target_table: str = "city_hourly_environment_tr",
    refresh_start: datetime | None = None,
) -> None:
    key_filter = ""
    if refresh_start is not None:
        key_filter = f"where time_ts >= {timestamp_literal(refresh_start)}"

    con.execute(
        f"""
        create or replace temp view city_time_keys as
        select city_name, time_ts
        from (
            select city_name, time_ts from hw
            union
            select city_name, time_ts from hf
            union
            select city_name, time_ts from fw
            union
            select city_name, time_ts from aq
            union
            select city_name, time_ts from cams
        ) keys
        {key_filter}
        """
    )

    con.execute(
        f"""
        create or replace table {target_table} as
        select
            k.city_name,
            strftime(k.time_ts, '%Y-%m-%dT%H:%M') as time,
            k.time_ts,
            coalesce(dp.province_name, k.city_name) as province_name,
            dp.shape_iso,
            dc.city_latitude,
            dc.city_longitude,
            dc.city_timezone,
            dc.country_code,
            cast(hw.city_name is not null as boolean) as has_historical_weather,
            cast(hf.city_name is not null as boolean) as has_historical_forecast,
            cast(fw.city_name is not null as boolean) as has_forecast_hourly,
            cast(aq.city_name is not null as boolean) as has_air_quality_hourly,
            cast(cams.city_name is not null as boolean) as has_cams_reanalysis,
            cast(hw.city_name is not null as integer)
                + cast(hf.city_name is not null as integer)
                + cast(fw.city_name is not null as integer)
                + cast(aq.city_name is not null as integer)
                + cast(cams.city_name is not null as integer) as available_source_count,
            hw.hw_temperature_2m,
            hw.hw_relative_humidity_2m,
            hw.hw_apparent_temperature,
            hw.hw_precipitation,
            hw.hw_rain,
            hw.hw_snowfall,
            hw.hw_weather_code,
            hw.hw_pressure_msl,
            hw.hw_cloud_cover,
            hw.hw_wind_speed_10m,
            hw.hw_wind_direction_10m,
            hw.hw_wind_gusts_10m,
            hw.hw_is_day,
            hw.hw_sunshine_duration,
            hf.hf_temperature_2m,
            hf.hf_relative_humidity_2m,
            hf.hf_apparent_temperature,
            hf.hf_precipitation_probability,
            hf.hf_precipitation,
            hf.hf_rain,
            hf.hf_snowfall,
            hf.hf_weather_code,
            hf.hf_pressure_msl,
            hf.hf_cloud_cover,
            hf.hf_wind_speed_10m,
            hf.hf_wind_direction_10m,
            hf.hf_wind_gusts_10m,
            hf.hf_uv_index,
            hf.hf_is_day,
            hf.hf_freezing_level_height,
            fw.fw_temperature_2m,
            fw.fw_relative_humidity_2m,
            fw.fw_apparent_temperature,
            fw.fw_precipitation_probability,
            fw.fw_precipitation,
            fw.fw_rain,
            fw.fw_snowfall,
            fw.fw_weather_code,
            fw.fw_pressure_msl,
            fw.fw_cloud_cover,
            fw.fw_wind_speed_10m,
            fw.fw_wind_direction_10m,
            fw.fw_wind_gusts_10m,
            fw.fw_uv_index,
            fw.fw_is_day,
            aq.aq_pm10,
            aq.aq_pm2_5,
            aq.aq_carbon_monoxide,
            aq.aq_nitrogen_dioxide,
            aq.aq_sulphur_dioxide,
            aq.aq_ozone,
            aq.aq_uv_index,
            aq.aq_european_aqi,
            aq.aq_european_aqi_pm2_5,
            aq.aq_european_aqi_pm10,
            aq.aq_european_aqi_ozone,
            aq.aq_european_aqi_nitrogen_dioxide,
            aq.aq_european_aqi_sulphur_dioxide,
            cams.cams_co,
            cams.cams_no2,
            cams.cams_o3,
            cams.cams_pm10,
            cams.cams_pm2p5,
            cams.cams_so2
        from city_time_keys k
        left join hw using (city_name, time_ts)
        left join hf using (city_name, time_ts)
        left join fw using (city_name, time_ts)
        left join aq using (city_name, time_ts)
        left join cams using (city_name, time_ts)
        left join dim_city dc using (city_name)
        left join dim_province dp on k.city_name = dp.province_name
        order by k.city_name, k.time_ts
        """
    )


def build_city_forecast_vs_actual(
    con: duckdb.DuckDBPyConnection,
    forecast_validation_window: str,
    target_table: str = "city_forecast_vs_actual_tr",
    refresh_start: datetime | None = None,
) -> None:
    window_sql = sql_text(forecast_validation_window)
    row_filter = ""
    if refresh_start is not None:
        row_filter = f"where hf.time_ts >= {timestamp_literal(refresh_start)}"

    con.execute(
        f"""
        create or replace table {target_table} as
        select
            hf.city_name,
            strftime(hf.time_ts, '%Y-%m-%dT%H:%M') as time,
            hf.time_ts,
            coalesce(dp.province_name, hf.city_name) as province_name,
            dp.shape_iso,
            hf.hf_temperature_2m,
            hw.hw_temperature_2m,
            hf.hf_temperature_2m - hw.hw_temperature_2m as err_temperature_2m,
            abs(hf.hf_temperature_2m - hw.hw_temperature_2m) as abs_err_temperature_2m,
            hf.hf_apparent_temperature,
            hw.hw_apparent_temperature,
            hf.hf_apparent_temperature - hw.hw_apparent_temperature as err_apparent_temperature,
            abs(hf.hf_apparent_temperature - hw.hw_apparent_temperature) as abs_err_apparent_temperature,
            hf.hf_precipitation,
            hw.hw_precipitation,
            hf.hf_precipitation - hw.hw_precipitation as err_precipitation,
            abs(hf.hf_precipitation - hw.hw_precipitation) as abs_err_precipitation,
            hf.hf_wind_speed_10m,
            hw.hw_wind_speed_10m,
            hf.hf_wind_speed_10m - hw.hw_wind_speed_10m as err_wind_speed_10m,
            abs(hf.hf_wind_speed_10m - hw.hw_wind_speed_10m) as abs_err_wind_speed_10m,
            hf.hf_weather_code,
            hw.hw_weather_code,
            cast(hf.hf_weather_code = hw.hw_weather_code as boolean) as weather_code_match,
            '{window_sql}' as forecast_validation_window
        from hf
        inner join hw using (city_name, time_ts)
        left join dim_city dc using (city_name)
        left join dim_province dp on hf.city_name = dp.province_name
        {row_filter}
        order by hf.city_name, hf.time_ts
        """
    )


def build_city_current_snapshot(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace table city_current_snapshot_tr as
        select
            coalesce(fc.city_name, aq.city_name) as city_name,
            coalesce(dp.province_name, coalesce(fc.city_name, aq.city_name)) as province_name,
            dp.shape_iso,
            dc.city_latitude,
            dc.city_longitude,
            dc.city_timezone,
            greatest(
                coalesce(fc.fc_time_ts, timestamp '1900-01-01'),
                coalesce(aq.aq_current_time_ts, timestamp '1900-01-01')
            ) as snapshot_time_ts,
            strftime(
                greatest(
                    coalesce(fc.fc_time_ts, timestamp '1900-01-01'),
                    coalesce(aq.aq_current_time_ts, timestamp '1900-01-01')
                ),
                '%Y-%m-%dT%H:%M'
            ) as snapshot_time,
            cast(fc.city_name is not null as boolean) as has_forecast_current,
            cast(aq.city_name is not null as boolean) as has_air_quality_current,
            fc.forecast_temperature_2m,
            fc.forecast_relative_humidity_2m,
            fc.forecast_apparent_temperature,
            fc.forecast_precipitation,
            fc.forecast_weather_code,
            fc.forecast_cloud_cover,
            fc.forecast_wind_speed_10m,
            fc.forecast_wind_direction_10m,
            fc.forecast_wind_gusts_10m,
            fc.forecast_is_day,
            aq.aq_current_european_aqi as aq_european_aqi,
            aq.aq_current_pm2_5 as aq_pm2_5,
            aq.aq_current_pm10 as aq_pm10,
            aq.aq_current_carbon_monoxide as aq_carbon_monoxide,
            aq.aq_current_nitrogen_dioxide as aq_nitrogen_dioxide,
            aq.aq_current_sulphur_dioxide as aq_sulphur_dioxide,
            aq.aq_current_ozone as aq_ozone,
            case
                when aq.aq_current_european_aqi is null then null
                when aq.aq_current_european_aqi <= 20 then 'good'
                when aq.aq_current_european_aqi <= 40 then 'fair'
                when aq.aq_current_european_aqi <= 60 then 'moderate'
                when aq.aq_current_european_aqi <= 80 then 'poor'
                when aq.aq_current_european_aqi <= 100 then 'very_poor'
                else 'extremely_poor'
            end as aq_category
        from forecast_current fc
        full outer join air_quality_current aq using (city_name)
        left join dim_city dc on dc.city_name = coalesce(fc.city_name, aq.city_name)
        left join dim_province dp on coalesce(fc.city_name, aq.city_name) = dp.province_name
        order by city_name
        """
    )


def build_province_map_metrics(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace temp view hw_2024_city_reference as
        select
            city_name,
            avg(hw_temperature_2m) as hw_2024_avg_temperature_2m,
            avg(hw_precipitation) as hw_2024_avg_precipitation
        from city_hourly_environment_tr
        where hw_temperature_2m is not null or hw_precipitation is not null
        group by city_name
        """
    )

    con.execute(
        """
        create or replace temp view cams_city_reference as
        select
            city_name,
            avg(cams_pm2p5) as cams_avg_pm2p5,
            avg(cams_no2) as cams_avg_no2
        from city_hourly_environment_tr
        where cams_pm2p5 is not null or cams_no2 is not null
        group by city_name
        """
    )

    con.execute(
        """
        create or replace table province_map_metrics_tr as
        select
            coalesce(dp.province_name, cs.city_name) as province_name,
            any_value(dp.shape_iso) as shape_iso,
            max(cs.snapshot_time_ts) as snapshot_time_ts,
            strftime(max(cs.snapshot_time_ts), '%Y-%m-%dT%H:%M') as snapshot_time,
            count(*) as city_count,
            avg(cs.forecast_temperature_2m) as avg_forecast_temperature_2m,
            avg(cs.forecast_apparent_temperature) as avg_forecast_apparent_temperature,
            avg(cs.aq_european_aqi) as avg_aq_european_aqi,
            max(cs.aq_european_aqi) as max_aq_european_aqi,
            avg(cs.aq_pm2_5) as avg_aq_pm2_5,
            avg(cs.aq_pm10) as avg_aq_pm10,
            avg(hw_ref.hw_2024_avg_temperature_2m) as hw_2024_avg_temperature_2m,
            avg(hw_ref.hw_2024_avg_precipitation) as hw_2024_avg_precipitation,
            avg(cams_ref.cams_avg_pm2p5) as cams_avg_pm2p5,
            avg(cams_ref.cams_avg_no2) as cams_avg_no2,
            avg(cams_ref.cams_avg_pm2p5) as cams_2024_01_avg_pm2p5,
            avg(cams_ref.cams_avg_no2) as cams_2024_01_avg_no2,
            max(case when cs.aq_european_aqi >= 80 then 1 else 0 end) as aq_alert_flag,
            max(case when cs.forecast_apparent_temperature >= 32 then 1 else 0 end) as heat_alert_flag,
            avg(coalesce(cs.aq_european_aqi, 0)) * 0.7
                + avg(greatest(coalesce(cs.forecast_apparent_temperature, 0) - 20, 0)) * 1.3
                as map_priority_score
        from city_current_snapshot_tr cs
        left join dim_province dp on cs.city_name = dp.province_name
        left join hw_2024_city_reference hw_ref on cs.city_name = hw_ref.city_name
        left join cams_city_reference cams_ref on cs.city_name = cams_ref.city_name
        group by 1
        order by 1
        """
    )


def build_mobile_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace table mobile_city_current_snapshot_tr_light as
        select
            city_name,
            province_name,
            snapshot_time,
            city_latitude,
            city_longitude,
            forecast_temperature_2m,
            forecast_apparent_temperature,
            forecast_weather_code,
            forecast_wind_speed_10m,
            aq_european_aqi,
            aq_category,
            aq_pm2_5,
            aq_pm10
        from city_current_snapshot_tr
        order by city_name
        """
    )

    con.execute(
        """
        create or replace table mobile_city_hourly_timeline_tr_light as
        with anchor as (
            select max(snapshot_time_ts) as anchor_ts
            from city_current_snapshot_tr
        )
        select
            e.city_name,
            e.province_name,
            strftime(e.time_ts, '%Y-%m-%dT%H:%M') as time,
            e.fw_temperature_2m as forecast_temperature_2m,
            e.fw_weather_code as forecast_weather_code,
            e.fw_precipitation_probability as forecast_precipitation_probability,
            e.fw_wind_speed_10m as forecast_wind_speed_10m,
            e.aq_european_aqi,
            e.aq_pm2_5,
            e.aq_pm10
        from city_hourly_environment_tr e
        cross join anchor a
        where e.time_ts between a.anchor_ts - interval '24 hours' and a.anchor_ts + interval '120 hours'
          and (e.has_forecast_hourly or e.has_air_quality_hourly)
        order by e.city_name, e.time_ts
        """
    )

    con.execute(
        """
        create or replace table mobile_province_map_metrics_tr_light as
        select
            province_name,
            shape_iso,
            snapshot_time,
            avg_aq_european_aqi,
            max_aq_european_aqi,
            avg_forecast_temperature_2m,
            map_priority_score,
            aq_alert_flag,
            heat_alert_flag
        from province_map_metrics_tr
        order by province_name
        """
    )


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    exists = con.execute(
        """
        select count(*)
        from information_schema.tables
        where lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()[0]
    return bool(exists)


def table_has_column(con: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    count = con.execute(
        """
        select count(*)
        from information_schema.columns
        where lower(table_name) = lower(?)
          and lower(column_name) = lower(?)
        """,
        [table_name, column_name],
    ).fetchone()[0]
    return bool(count)


def table_max_timestamp(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str = "time_ts",
) -> datetime | None:
    if not table_exists(con, table_name):
        return None
    if not table_has_column(con, table_name, column_name):
        return None
    max_ts = con.execute(f"select max({column_name}) from {table_name}").fetchone()[0]
    return max_ts


def merge_time_window_table(
    con: duckdb.DuckDBPyConnection,
    target_table: str,
    delta_table: str,
    refresh_start: datetime,
) -> None:
    if not table_exists(con, target_table):
        con.execute(f"create table {target_table} as select * from {delta_table}")
        return

    delta_count = con.execute(f"select count(*) from {delta_table}").fetchone()[0]
    if delta_count == 0:
        print(f"[incremental] no rows in {delta_table}; {target_table} unchanged")
        return

    con.execute(
        f"delete from {target_table} where time_ts >= cast(? as timestamp)",
        [refresh_start],
    )
    con.execute(f"insert into {target_table} select * from {delta_table}")


def copy_table_to_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        copy (
            select * from {table_name}
        ) to '{sql_path(output_path)}' (format parquet, compression zstd)
        """
    )


def compute_table_summary(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    entity_column: str,
) -> dict[str, object]:
    result: dict[str, object] = {}
    result["row_count"] = con.execute(f"select count(*) from {table_name}").fetchone()[0]

    has_entity_col = con.execute(
        f"""
        select count(*)
        from information_schema.columns
        where lower(table_name) = lower('{table_name}')
          and lower(column_name) = lower('{entity_column}')
        """
    ).fetchone()[0]
    if has_entity_col:
        result["entity_count"] = con.execute(
            f"select count(distinct {entity_column}) from {table_name}"
        ).fetchone()[0]

    has_time_col = con.execute(
        f"""
        select count(*)
        from information_schema.columns
        where lower(table_name) = lower('{table_name}')
          and lower(column_name) = 'time_ts'
        """
    ).fetchone()[0]
    if has_time_col:
        min_time, max_time = con.execute(
            f"select min(time_ts), max(time_ts) from {table_name}"
        ).fetchone()
        result["time_min"] = min_time.isoformat() if min_time is not None else None
        result["time_max"] = max_time.isoformat() if max_time is not None else None

    return result


def input_fingerprint(path: Path, project_root: Path) -> dict[str, object]:
    stats = path.stat()
    return {
        "path": to_rel_path(project_root, path),
        "size_bytes": stats.st_size,
        "mtime_ns": stats.st_mtime_ns,
        "sha256": file_sha256(path),
    }


def load_previous_manifest(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def normalize_previous_input_path(project_root: Path, value: str | None) -> str | None:
    if not value:
        return None
    p = Path(value)
    if not p.is_absolute():
        p = project_root / p
    return p.resolve().as_posix()


def inputs_require_full_rebuild(
    project_root: Path,
    previous_manifest: dict[str, object] | None,
    inputs: InputPaths,
) -> list[str]:
    reasons_set: set[str] = set()

    if previous_manifest is None:
        reasons_set.add("no_previous_manifest")
        return sorted(reasons_set)

    previous_fingerprints = previous_manifest.get("input_fingerprints")
    previous_inputs = previous_manifest.get("inputs", {})

    if not isinstance(previous_inputs, dict):
        previous_inputs = {}
    if not isinstance(previous_fingerprints, dict):
        previous_fingerprints = {}

    for key in FULL_REBUILD_SENSITIVE_INPUTS:
        current_path = getattr(inputs, key)
        current_fp = input_fingerprint(current_path, project_root)
        current_abs_path = current_path.resolve().as_posix()

        prior_fp = previous_fingerprints.get(key)
        if isinstance(prior_fp, dict):
            prior_path = prior_fp.get("path")
            if isinstance(prior_path, str):
                normalized_prior = normalize_previous_input_path(project_root, prior_path)
                if normalized_prior != current_abs_path:
                    reasons_set.add(f"input_changed:{key}")

            prior_sha = prior_fp.get("sha256")
            if isinstance(prior_sha, str):
                if prior_sha != current_fp["sha256"]:
                    reasons_set.add(f"input_changed:{key}")
            else:
                if (
                    prior_fp.get("size_bytes") != current_fp["size_bytes"]
                    or prior_fp.get("mtime_ns") != current_fp["mtime_ns"]
                ):
                    reasons_set.add(f"input_changed:{key}")
            continue

        # Backward compatibility for old manifests without fingerprints.
        prior_path = previous_inputs.get(key)
        if key == "historical_forecast_hourly" and not prior_path:
            prior_path = previous_inputs.get("historical_forecast_full")

        normalized_prior = normalize_previous_input_path(project_root, prior_path)
        if normalized_prior != current_abs_path:
            reasons_set.add(f"input_changed:{key}")

    return sorted(reasons_set)


def decide_refresh_mode(
    requested_mode: str,
    project_root: Path,
    previous_manifest: dict[str, object] | None,
    inputs: InputPaths,
    con: duckdb.DuckDBPyConnection,
) -> RefreshDecision:
    reasons = inputs_require_full_rebuild(project_root, previous_manifest, inputs)

    for table_name in ["city_hourly_environment_tr", "city_forecast_vs_actual_tr"]:
        if not table_exists(con, table_name):
            reasons.append(f"missing_table:{table_name}")

    if requested_mode == "full":
        return RefreshDecision(mode="full", reasons=["requested_full"])

    if requested_mode == "incremental":
        if reasons:
            return RefreshDecision(mode="full", reasons=reasons)
        return RefreshDecision(mode="incremental", reasons=[])

    # auto
    if reasons:
        return RefreshDecision(mode="full", reasons=reasons)
    return RefreshDecision(mode="incremental", reasons=[])


def build_manifest(
    con: duckdb.DuckDBPyConnection,
    project_root: Path,
    inputs: InputPaths,
    mart_paths: dict[str, Path],
    view_paths: dict[str, Path],
    manifest_path: Path,
    build_mode: str,
    refresh_start: datetime | None,
) -> None:
    def rel(path: Path) -> str:
        resolved = path.resolve()
        try:
            return resolved.relative_to(project_root).as_posix()
        except ValueError:
            return resolved.as_posix()

    manifest = {
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "build_mode": build_mode,
        "refresh_start": refresh_start.isoformat() if refresh_start is not None else None,
        "inputs": {name: rel(path) for name, path in inputs.__dict__.items()},
        "input_fingerprints": {
            name: input_fingerprint(path, project_root) for name, path in inputs.__dict__.items()
        },
        "marts": {},
        "views": {},
    }

    for table_name, output_path in mart_paths.items():
        entity_col = "province_name" if "province" in table_name else "city_name"
        manifest["marts"][table_name] = {
            "path": rel(output_path),
            **compute_table_summary(con, table_name, entity_col),
        }

    for table_name, output_path in view_paths.items():
        entity_col = "province_name" if "province" in table_name else "city_name"
        manifest["views"][table_name] = {
            "path": rel(output_path),
            **compute_table_summary(con, table_name, entity_col),
        }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def run_full_build(
    con: duckdb.DuckDBPyConnection,
    inputs: InputPaths,
    forecast_validation_window: str,
) -> None:
    build_stage_views(con, inputs, refresh_start=None)
    build_city_hourly_environment(con, target_table="city_hourly_environment_tr", refresh_start=None)
    build_city_forecast_vs_actual(
        con,
        forecast_validation_window=forecast_validation_window,
        target_table="city_forecast_vs_actual_tr",
        refresh_start=None,
    )
    build_city_current_snapshot(con)
    build_province_map_metrics(con)
    build_mobile_views(con)


def run_incremental_build(
    con: duckdb.DuckDBPyConnection,
    inputs: InputPaths,
    forecast_validation_window: str,
    lookback_hours: int,
) -> datetime:
    max_ts = table_max_timestamp(con, "city_hourly_environment_tr", "time_ts")
    if max_ts is None:
        raise RuntimeError("city_hourly_environment_tr has no max(time_ts); cannot run incremental mode")

    refresh_start = compute_incremental_refresh_start(max_ts, lookback_hours)

    build_stage_views(con, inputs, refresh_start=None)

    build_city_hourly_environment(
        con,
        target_table="city_hourly_environment_delta_tr",
        refresh_start=refresh_start,
    )
    merge_time_window_table(
        con,
        target_table="city_hourly_environment_tr",
        delta_table="city_hourly_environment_delta_tr",
        refresh_start=refresh_start,
    )

    build_city_forecast_vs_actual(
        con,
        forecast_validation_window=forecast_validation_window,
        target_table="city_forecast_vs_actual_delta_tr",
        refresh_start=refresh_start,
    )
    merge_time_window_table(
        con,
        target_table="city_forecast_vs_actual_tr",
        delta_table="city_forecast_vs_actual_delta_tr",
        refresh_start=refresh_start,
    )

    build_city_current_snapshot(con)
    build_province_map_metrics(con)
    build_mobile_views(con)

    con.execute("drop table if exists city_hourly_environment_delta_tr")
    con.execute("drop table if exists city_forecast_vs_actual_delta_tr")

    return refresh_start


def compute_incremental_refresh_start(
    max_ts: datetime,
    lookback_hours: int,
    now_utc: datetime | None = None,
) -> datetime:
    if lookback_hours <= 0:
        raise ValueError("lookback_hours must be positive")

    now_ref = now_utc or datetime.now(timezone.utc)

    # city_hourly_environment contains future forecast rows. If we anchor incremental
    # windows on a future max timestamp, refresh_start can drift into the future and
    # skip newly arrived near-now rows. Clamp the anchor to current UTC time.
    now_anchor = now_ref.astimezone(max_ts.tzinfo) if max_ts.tzinfo else now_ref.replace(tzinfo=None)
    anchor_ts = min(max_ts, now_anchor)
    return anchor_ts - timedelta(hours=lookback_hours)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build analytics marts and mobile-ready views with DuckDB and Parquet outputs."
    )
    parser.add_argument(
        "--db-path",
        default="data/db/breathwise_tr.duckdb",
        help="DuckDB file path relative to project root.",
    )
    parser.add_argument(
        "--refresh-mode",
        choices=["auto", "full", "incremental"],
        default="auto",
        help="Build mode. auto selects incremental when safe, otherwise full rebuild.",
    )
    parser.add_argument(
        "--incremental-lookback-hours",
        type=int,
        default=96,
        help="When incremental mode is used, rebuild this trailing window from max(time_ts).",
    )
    args = parser.parse_args()

    if args.incremental_lookback_hours <= 0:
        raise ValueError("--incremental-lookback-hours must be a positive integer")

    project_root = discover_project_root(Path(__file__))
    db_path = (project_root / args.db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    marts_dir = project_root / "data" / "processed" / "marts"
    views_dir = project_root / "data" / "processed" / "views"
    marts_dir.mkdir(parents=True, exist_ok=True)
    views_dir.mkdir(parents=True, exist_ok=True)

    inputs = resolve_inputs(project_root)
    forecast_validation_window = infer_forecast_validation_window(inputs.historical_forecast_hourly)

    mart_paths = {
        "city_hourly_environment_tr": marts_dir / "city_hourly_environment_tr.parquet",
        "city_forecast_vs_actual_tr": marts_dir / "city_forecast_vs_actual_tr.parquet",
        "city_current_snapshot_tr": marts_dir / "city_current_snapshot_tr.parquet",
        "province_map_metrics_tr": marts_dir / "province_map_metrics_tr.parquet",
    }
    view_paths = {
        "mobile_city_current_snapshot_tr_light": views_dir
        / "mobile_city_current_snapshot_tr_light.parquet",
        "mobile_city_hourly_timeline_tr_light": views_dir
        / "mobile_city_hourly_timeline_tr_light.parquet",
        "mobile_province_map_metrics_tr_light": views_dir
        / "mobile_province_map_metrics_tr_light.parquet",
    }

    manifest_path = marts_dir / "marts_build_manifest.json"

    con = duckdb.connect(db_path.as_posix())
    try:
        previous_manifest = load_previous_manifest(manifest_path)
        decision = decide_refresh_mode(
            requested_mode=args.refresh_mode,
            project_root=project_root,
            previous_manifest=previous_manifest,
            inputs=inputs,
            con=con,
        )

        refresh_start: datetime | None = None
        if decision.mode == "incremental":
            refresh_start = run_incremental_build(
                con,
                inputs=inputs,
                forecast_validation_window=forecast_validation_window,
                lookback_hours=args.incremental_lookback_hours,
            )
        else:
            run_full_build(
                con,
                inputs=inputs,
                forecast_validation_window=forecast_validation_window,
            )

        for table_name, output_path in mart_paths.items():
            copy_table_to_parquet(con, table_name, output_path)

        for table_name, output_path in view_paths.items():
            copy_table_to_parquet(con, table_name, output_path)

        build_manifest(
            con,
            project_root,
            inputs,
            mart_paths,
            view_paths,
            manifest_path,
            build_mode=decision.mode,
            refresh_start=refresh_start,
        )

        print("Build complete.")
        print(f"DuckDB: {db_path}")
        print(f"Mode: {decision.mode}")
        if decision.reasons:
            print(f"Mode reasons: {', '.join(decision.reasons)}")
        print(f"Forecast validation window label: {forecast_validation_window}")
        if refresh_start is not None:
            print(f"Incremental refresh_start: {refresh_start.isoformat()}")
        for table_name, output_path in mart_paths.items():
            print(f"- {table_name}: {output_path}")
        for table_name, output_path in view_paths.items():
            print(f"- {table_name}: {output_path}")
        print(f"- manifest: {manifest_path}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
