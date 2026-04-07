import type {
  CityCurrentSnapshot,
  LocalizationMeta,
  MobileCityTimelinePoint,
  MobileProvinceMapMetric,
} from "../types/backend";
import { BreathwiseApiClient } from "./client";

export async function fetchLocalizationMeta(client: BreathwiseApiClient): Promise<LocalizationMeta> {
  return client.get<LocalizationMeta>("/v1/meta/localization", {
    include_translations: true,
    max_keys: 5000,
  });
}

export async function fetchCityCurrent(
  client: BreathwiseApiClient,
  locale: string,
  cityName?: string
): Promise<CityCurrentSnapshot[]> {
  return client.get<CityCurrentSnapshot[]>("/v1/mobile/cities/current", {
    city_name: cityName,
    limit: 81,
    locale,
  });
}

export async function fetchCityTimeline(
  client: BreathwiseApiClient,
  cityName: string,
  limit = 72
): Promise<MobileCityTimelinePoint[]> {
  return client.get<MobileCityTimelinePoint[]>(`/v1/mobile/cities/${encodeURIComponent(cityName)}/timeline`, {
    limit,
  });
}

export async function fetchProvinceMapMetrics(
  client: BreathwiseApiClient,
  locale: string
): Promise<MobileProvinceMapMetric[]> {
  return client.get<MobileProvinceMapMetric[]>("/v1/mobile/provinces/map-metrics", {
    limit: 81,
    locale,
  });
}
