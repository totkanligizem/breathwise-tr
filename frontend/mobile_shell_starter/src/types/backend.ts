export type SupportedLocale = "tr-TR" | "en-US";

export type CityCurrentSnapshot = {
  city_name: string;
  province_name: string | null;
  snapshot_time: string;
  city_latitude: number | null;
  city_longitude: number | null;
  forecast_temperature_2m: number | null;
  forecast_apparent_temperature: number | null;
  forecast_weather_code: number | null;
  forecast_wind_speed_10m: number | null;
  aq_european_aqi: number | null;
  aq_category: string | null;
  aq_category_key: string | null;
  aq_category_label: string | null;
  label_locale: string | null;
  aq_pm2_5: number | null;
  aq_pm10: number | null;
};

export type MobileCityTimelinePoint = {
  city_name: string;
  province_name: string | null;
  time: string;
  forecast_temperature_2m: number | null;
  forecast_weather_code: number | null;
  forecast_precipitation_probability: number | null;
  forecast_wind_speed_10m: number | null;
  aq_european_aqi: number | null;
  aq_pm2_5: number | null;
  aq_pm10: number | null;
};

export type MobileProvinceMapMetric = {
  province_name: string;
  shape_iso: string | null;
  snapshot_time: string;
  avg_aq_european_aqi: number | null;
  max_aq_european_aqi: number | null;
  avg_forecast_temperature_2m: number | null;
  map_priority_score: number | null;
  aq_alert_flag: number;
  aq_alert_key: string | null;
  aq_alert_label: string | null;
  heat_alert_flag: number;
  heat_alert_key: string | null;
  heat_alert_label: string | null;
  label_locale: string | null;
};

export type LocalizationMeta = {
  supported_locales: string[];
  default_locale: string;
  fallback_locale: string;
  locale_aliases: Record<string, string>;
  translation_key_count: number;
  contract_path: string;
  generated_at_utc: string | null;
  translation_keys?: Record<string, Record<string, string>>;
};
