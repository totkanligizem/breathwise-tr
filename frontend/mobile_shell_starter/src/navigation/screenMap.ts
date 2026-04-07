export type ScreenId =
  | "city_current_overview"
  | "city_hourly_timeline"
  | "province_map_metrics"
  | "settings_locale";

export type ScreenConfig = {
  id: ScreenId;
  titleKey: string;
  subtitleKey: string;
  path: string;
};

export const SCREEN_MAP: ScreenConfig[] = [
  {
    id: "city_current_overview",
    titleKey: "ui.nav.city_overview",
    subtitleKey: "ui.nav.city_overview.subtitle",
    path: "/city/current",
  },
  {
    id: "city_hourly_timeline",
    titleKey: "ui.nav.city_timeline",
    subtitleKey: "ui.nav.city_timeline.subtitle",
    path: "/city/timeline",
  },
  {
    id: "province_map_metrics",
    titleKey: "ui.nav.province_map",
    subtitleKey: "ui.nav.province_map.subtitle",
    path: "/province/map",
  },
  {
    id: "settings_locale",
    titleKey: "ui.nav.settings",
    subtitleKey: "ui.nav.settings.subtitle",
    path: "/settings",
  },
];
