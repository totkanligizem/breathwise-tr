export type CityCurrentOverviewCard = {
  cityName: string;
  provinceName: string | null;
  updatedAt: string;
  temperatureC: number | null;
  apparentTemperatureC: number | null;
  weatherCode: number | null;
  windSpeedMps: number | null;
  aqi: number | null;
  aqiCategoryKey: string | null;
  aqiCategoryLabel: string | null;
  labelLocale: string | null;
  pm25: number | null;
  pm10: number | null;
};

export type CityTimelinePointVM = {
  time: string;
  temperatureC: number | null;
  weatherCode: number | null;
  precipitationProbability: number | null;
  windSpeed10m: number | null;
  aqi: number | null;
  pm25: number | null;
  pm10: number | null;
};

export type ProvinceMapMetricVM = {
  provinceName: string;
  shapeIso: string | null;
  updatedAt: string;
  avgAqi: number | null;
  maxAqi: number | null;
  avgTempC: number | null;
  priorityScore: number | null;
  aqAlertKey: string | null;
  aqAlertLabel: string | null;
  heatAlertKey: string | null;
  heatAlertLabel: string | null;
  labelLocale: string | null;
};

export type AppLocaleState = {
  selectedLocale: string;
  fallbackLocale: string;
  supportedLocales: string[];
};
