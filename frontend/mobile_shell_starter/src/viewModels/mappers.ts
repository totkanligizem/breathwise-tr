import type {
  CityCurrentSnapshot,
  LocalizationMeta,
  MobileCityTimelinePoint,
  MobileProvinceMapMetric,
} from "../types/backend";
import type { CityCurrentOverviewCard, CityTimelinePointVM, ProvinceMapMetricVM } from "../types/viewModels";
import { localizeFromContract } from "../i18n/localization";

export function mapCityCurrentToCard(
  row: CityCurrentSnapshot,
  locale: string,
  localizationMeta: LocalizationMeta
): CityCurrentOverviewCard {
  return {
    cityName: row.city_name,
    provinceName: row.province_name,
    updatedAt: row.snapshot_time,
    temperatureC: row.forecast_temperature_2m ?? null,
    apparentTemperatureC: row.forecast_apparent_temperature ?? null,
    weatherCode: row.forecast_weather_code ?? null,
    windSpeedMps: row.forecast_wind_speed_10m ?? null,
    aqi: row.aq_european_aqi ?? null,
    aqiCategoryKey: row.aq_category_key,
    aqiCategoryLabel: localizeFromContract(row.aq_category_key, locale, localizationMeta, row.aq_category_label),
    labelLocale: row.label_locale,
    pm25: row.aq_pm2_5 ?? null,
    pm10: row.aq_pm10 ?? null,
  };
}

export function mapTimelinePoint(row: MobileCityTimelinePoint): CityTimelinePointVM {
  return {
    time: row.time,
    temperatureC: row.forecast_temperature_2m ?? null,
    weatherCode: row.forecast_weather_code ?? null,
    precipitationProbability: row.forecast_precipitation_probability ?? null,
    windSpeed10m: row.forecast_wind_speed_10m ?? null,
    aqi: row.aq_european_aqi ?? null,
    pm25: row.aq_pm2_5 ?? null,
    pm10: row.aq_pm10 ?? null,
  };
}

export function mapProvinceMetric(
  row: MobileProvinceMapMetric,
  locale: string,
  localizationMeta: LocalizationMeta
): ProvinceMapMetricVM {
  return {
    provinceName: row.province_name,
    shapeIso: row.shape_iso,
    updatedAt: row.snapshot_time,
    avgAqi: row.avg_aq_european_aqi,
    maxAqi: row.max_aq_european_aqi,
    avgTempC: row.avg_forecast_temperature_2m,
    priorityScore: row.map_priority_score,
    aqAlertKey: row.aq_alert_key,
    aqAlertLabel: localizeFromContract(row.aq_alert_key, locale, localizationMeta, row.aq_alert_label),
    heatAlertKey: row.heat_alert_key,
    heatAlertLabel: localizeFromContract(row.heat_alert_key, locale, localizationMeta, row.heat_alert_label),
    labelLocale: row.label_locale,
  };
}
