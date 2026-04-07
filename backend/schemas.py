from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(description="Health status value.")
    project_root: str = Field(description="Resolved local project root.")
    marts_ready: bool = Field(description="Whether required mart files are present.")
    views_ready: bool = Field(description="Whether required mobile view files are present.")
    timestamp_utc: datetime = Field(description="Response timestamp in UTC.")


class ReadinessResponse(BaseModel):
    status: str = Field(description="Readiness status value.")
    ready: bool = Field(description="Whether the API is ready to serve analytical data.")
    missing_datasets: list[str] = Field(default_factory=list)
    marts_ready: bool = Field(description="Whether required mart files are present.")
    views_ready: bool = Field(description="Whether required mobile view files are present.")
    timestamp_utc: datetime = Field(description="Response timestamp in UTC.")


class DatasetMeta(BaseModel):
    dataset_name: str
    path: str
    row_count: int
    entity_count: int | None = None
    time_min: datetime | None = None
    time_max: datetime | None = None


class LocalizationMeta(BaseModel):
    supported_locales: list[str]
    default_locale: str
    fallback_locale: str
    locale_aliases: dict[str, str] = Field(default_factory=dict)
    translation_key_count: int
    generated_at_utc: datetime | None = None


class CityCurrentSnapshot(BaseModel):
    city_name: str
    province_name: str | None = None
    snapshot_time: str
    city_latitude: float | None = None
    city_longitude: float | None = None
    forecast_temperature_2m: float | None = None
    forecast_apparent_temperature: float | None = None
    forecast_weather_code: int | None = None
    forecast_wind_speed_10m: float | None = None
    aq_european_aqi: float | None = None
    aq_category: str | None = None
    aq_category_key: str | None = None
    aq_category_label: str | None = None
    label_locale: str | None = None
    aq_pm2_5: float | None = None
    aq_pm10: float | None = None


class CityHourlyPoint(BaseModel):
    city_name: str
    province_name: str | None = None
    time: str
    hw_temperature_2m: float | None = None
    hf_temperature_2m: float | None = None
    fw_temperature_2m: float | None = None
    aq_european_aqi: float | None = None
    aq_pm2_5: float | None = None
    cams_pm2p5: float | None = None
    available_source_count: int


class ProvinceMapMetric(BaseModel):
    province_name: str
    shape_iso: str | None = None
    snapshot_time: str
    city_count: int
    avg_forecast_temperature_2m: float | None = None
    avg_aq_european_aqi: float | None = None
    max_aq_european_aqi: float | None = None
    avg_aq_pm2_5: float | None = None
    hw_2024_avg_temperature_2m: float | None = None
    cams_avg_pm2p5: float | None = None
    cams_avg_no2: float | None = None
    cams_2024_01_avg_pm2p5: float | None = None
    aq_alert_flag: int
    aq_alert_key: str | None = None
    aq_alert_label: str | None = None
    heat_alert_flag: int
    heat_alert_key: str | None = None
    heat_alert_label: str | None = None
    label_locale: str | None = None
    map_priority_score: float | None = None


class MobileCityTimelinePoint(BaseModel):
    city_name: str
    province_name: str | None = None
    time: str
    forecast_temperature_2m: float | None = None
    forecast_weather_code: int | None = None
    forecast_precipitation_probability: float | None = None
    forecast_wind_speed_10m: float | None = None
    aq_european_aqi: float | None = None
    aq_pm2_5: float | None = None
    aq_pm10: float | None = None


class MobileProvinceMapMetric(BaseModel):
    province_name: str
    shape_iso: str | None = None
    snapshot_time: str
    avg_aq_european_aqi: float | None = None
    max_aq_european_aqi: float | None = None
    avg_forecast_temperature_2m: float | None = None
    map_priority_score: float | None = None
    aq_alert_flag: int
    aq_alert_key: str | None = None
    aq_alert_label: str | None = None
    heat_alert_flag: int
    heat_alert_key: str | None = None
    heat_alert_label: str | None = None
    label_locale: str | None = None
