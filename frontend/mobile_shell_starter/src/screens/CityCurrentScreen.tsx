import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Pressable, ScrollView, StyleSheet, Text, TextInput, View, useWindowDimensions } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { fetchCityCurrent, fetchCityTimeline } from "../api/endpoints";
import { EmptyBlock } from "../components/EmptyBlock";
import { ErrorBlock } from "../components/ErrorBlock";
import { LoadingBlock } from "../components/LoadingBlock";
import { WeatherMoodBackdrop } from "../components/WeatherMoodBackdrop";
import {
  HourlyIconStrip,
  MiniLineChart,
  StatPill,
  TrendPill,
  ValueStrip,
  getAqiColor,
  weatherIconForConditions,
  weatherMoodForConditions,
} from "../components/visuals";
import { colors, radius, shadow, spacing, typography } from "../theme/tokens";
import { formatCityName, normalizeCityToken, prioritizeCityNames } from "../utils/cities";
import { mapCityCurrentToCard, mapTimelinePoint } from "../viewModels/mappers";
import type { BaseScreenProps } from "./types";

type OverviewCard = ReturnType<typeof mapCityCurrentToCard>;
type TimelinePoint = ReturnType<typeof mapTimelinePoint>;

export function CityCurrentScreen({ locale, t, client, localizationMeta }: BaseScreenProps) {
  const { width } = useWindowDimensions();
  const isWide = width >= 960;

  const [cityQuery, setCityQuery] = useState<string>("");
  const [quickCities, setQuickCities] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<OverviewCard[]>([]);
  const [heroTimeline, setHeroTimeline] = useState<TimelinePoint[]>([]);
  const [heroTimelineLoading, setHeroTimelineLoading] = useState<boolean>(false);

  const load = useCallback(
    async (query: string) => {
      if (!client) {
        setError(t("ui.common.backend_unreachable", "Backend unreachable"));
        setLoading(false);
        return;
      }

      const effectiveQuery = query.trim();
      setLoading(true);
      setError(null);
      try {
        const payload = await fetchCityCurrent(client, locale, effectiveQuery || undefined);
        const mapped = payload
          .map((row) => mapCityCurrentToCard(row, locale, localizationMeta))
          .sort((a, b) => {
            const aqiA = a.aqi ?? -1;
            const aqiB = b.aqi ?? -1;
            if (aqiA !== aqiB) return aqiB - aqiA;
            return a.cityName.localeCompare(b.cityName);
          });
        const uniqueRows = dedupeByCityToken(mapped);
        setRows(uniqueRows);

        if (!effectiveQuery) {
          const names = prioritizeCityNames(uniqueRows.map((row) => row.cityName).filter(Boolean));
          setQuickCities(names.slice(0, 8));
        }
      } catch (fetchError) {
        setError(fetchError instanceof Error ? fetchError.message : "Request failed.");
        setRows([]);
      } finally {
        setLoading(false);
      }
    },
    [client, locale, localizationMeta, t]
  );

  useEffect(() => {
    load("");
  }, [load]);

  const featured = useMemo(() => {
    if (rows.length === 0) return null;
    const query = cityQuery.trim();
    if (query) return rows[0];
    return rows.slice().sort((a, b) => (b.aqi ?? -1) - (a.aqi ?? -1))[0] ?? rows[0];
  }, [cityQuery, rows]);

  useEffect(() => {
    if (!featured || !client) {
      setHeroTimeline([]);
      return;
    }
    let cancelled = false;
    setHeroTimelineLoading(true);

    fetchCityTimeline(client, featured.cityName, 24)
      .then((timeline) => {
        if (!cancelled) {
          setHeroTimeline(timeline.map(mapTimelinePoint));
        }
      })
      .catch(() => {
        if (!cancelled) {
          setHeroTimeline([]);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setHeroTimelineLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [client, featured]);

  const topPolluted = useMemo(() => rows.slice(0, 4), [rows]);
  const comparisonRows = useMemo(() => rows.slice(0, 6), [rows]);
  const featuredToken = cityToken(featured?.cityName ?? "");

  const timelineTemps = useMemo(() => heroTimeline.map((row) => row.temperatureC), [heroTimeline]);
  const timelineAqi = useMemo(() => heroTimeline.map((row) => row.aqi), [heroTimeline]);
  const heroPrecipNow = useMemo(() => {
    const current = heroTimeline[0];
    const value = current?.precipitationProbability ?? null;
    return value !== null && !Number.isNaN(value) ? value : null;
  }, [heroTimeline]);
  const featuredWeather = useMemo(
    () =>
      weatherIconForConditions(
        featured?.weatherCode ?? null,
        featured?.temperatureC ?? null,
        featured?.aqi ?? null,
        heroPrecipNow
      ),
    [featured?.aqi, featured?.temperatureC, featured?.weatherCode, heroPrecipNow]
  );
  const heroInsight = useMemo(() => buildCurrentInsight(featured, t), [featured, t]);
  const featuredProvinceLabel = useMemo(() => {
    if (!featured?.provinceName) return null;
    const normalizedProvince = normalizeCityToken(featured.provinceName);
    const normalizedCity = normalizeCityToken(featured.cityName);
    if (!normalizedProvince || normalizedProvince === normalizedCity) return null;
    return formatCityName(featured.provinceName, locale);
  }, [featured, locale]);

  return (
    <ScrollView contentContainerStyle={styles.content} showsVerticalScrollIndicator={false}>
      <View style={styles.titleWrap}>
        <Text style={styles.title}>{t("ui.city.current.title", "Current Conditions")}</Text>
        <Text style={styles.subtitle}>{t("ui.city.current.subtitle", "Real-time city weather and air quality")}</Text>
      </View>

      <View style={styles.searchCard}>
        <View style={styles.searchHeader}>
          <Text style={styles.sectionEyebrow}>{t("ui.city.current.search_label", "Search city")}</Text>
        </View>
        <View style={styles.searchWrap}>
          <TextInput
            value={cityQuery}
            onChangeText={setCityQuery}
            placeholder={t("ui.city.current.search_placeholder", "Ankara")}
            placeholderTextColor={colors.textTertiary}
            style={styles.searchInput}
            autoCapitalize="words"
            autoCorrect={false}
            returnKeyType="search"
            onSubmitEditing={() => load(cityQuery)}
          />
          <Pressable onPress={() => load(cityQuery)} style={styles.searchButton}>
            <MaterialCommunityIcons name="magnify" size={16} color={colors.onAccent} />
            <Text style={styles.searchButtonText}>{t("ui.common.refresh", "Refresh")}</Text>
          </Pressable>
        </View>

        {quickCities.length > 0 ? (
          <View style={styles.quickWrap}>
            <Text style={styles.quickTitle}>{t("ui.city.current.quick_select", "Quick select")}</Text>
            <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.quickList}>
              {quickCities.map((name) => {
                const active = cityToken(name) === cityToken(cityQuery.trim());
                return (
                  <Pressable
                    key={name}
                    onPress={() => {
                      setCityQuery(name);
                      load(name);
                    }}
                    style={[styles.quickChip, active ? styles.quickChipActive : null]}
                  >
                    <Text style={[styles.quickChipText, active ? styles.quickChipTextActive : null]}>
                      {formatCityName(name, locale)}
                    </Text>
                  </Pressable>
                );
              })}
            </ScrollView>
          </View>
        ) : null}
      </View>

      {loading ? (
        <LoadingBlock
          label={t("ui.common.loading", "Loading")}
          detail={t("ui.common.loading_detail", "Connecting to data sources")}
        />
      ) : null}

      {!loading && error ? (
        <ErrorBlock
          title={t("ui.common.backend_unreachable", "Backend unreachable")}
          detail={error}
          retryLabel={t("ui.common.retry", "Retry")}
          badgeLabel={t("ui.common.connection_badge", "Connection")}
          onRetry={() => load(cityQuery)}
        />
      ) : null}

      {!loading && !error && rows.length === 0 ? (
        <EmptyBlock
          title={t("ui.common.empty", "No data")}
          detail={t("ui.common.empty_detail", "Adjust your filter and try again.")}
          badgeLabel={t("ui.common.no_data_badge", "No Data")}
        />
      ) : null}

      {!loading && !error && featured ? (
        <View style={styles.heroCard}>
          <WeatherMoodBackdrop
            mood={weatherMoodForConditions(
              featured.weatherCode,
              featured.temperatureC,
              heroPrecipNow
            )}
          />
          <View style={styles.heroContent}>
            <View style={styles.heroTop}>
              <View>
                <Text style={styles.heroEyebrow}>{t("ui.city.current.featured", "Featured City")}</Text>
                <Text style={styles.heroCity}>{formatCityName(featured.cityName, locale)}</Text>
                {featuredProvinceLabel ? <Text style={styles.heroMeta}>{featuredProvinceLabel}</Text> : null}
              </View>
              <View style={styles.heroWeatherWrap}>
                <MaterialCommunityIcons
                  name={featuredWeather.name}
                  size={42}
                  color={featuredWeather.color}
                />
                <Text style={styles.heroTemp}>{formatTemperature(featured.temperatureC)}</Text>
                <Text style={styles.heroFeelsLike}>
                  {`${t("ui.city.current.feels_like", "Feels")}: ${formatTemperature(featured.apparentTemperatureC)}`}
                </Text>
              </View>
            </View>

            <View style={styles.heroBadgeRow}>
              <View style={[styles.aqiBadge, { borderColor: getAqiColor(featured.aqi) }]}>
                <View style={[styles.aqiDot, { backgroundColor: getAqiColor(featured.aqi) }]} />
                <Text style={styles.aqiBadgeText}>{featured.aqiCategoryLabel ?? "AQI"}</Text>
              </View>
              <Text style={styles.heroUpdated}>{formatDateTime(featured.updatedAt, locale)}</Text>
            </View>
            <Text style={styles.heroInsight}>{heroInsight}</Text>

            <View style={styles.heroPills}>
              <StatPill
                icon="chart-bell-curve-cumulative"
                label={t("ui.city.current.kpi_aqi", "AQI")}
                value={formatAqi(featured.aqi)}
                tone={featured.aqi !== null && featured.aqi >= 120 ? "danger" : featured.aqi !== null && featured.aqi >= 80 ? "warning" : "accent"}
              />
              <StatPill
                icon="thermometer"
                label={t("ui.city.current.kpi_temperature", "Temperature")}
                value={formatTemperature(featured.temperatureC)}
                tone="accent"
              />
              <StatPill
                icon="weather-windy"
                label={t("ui.city.current.kpi_wind", "Wind")}
                value={formatWind(featured.windSpeedMps)}
                tone="default"
              />
              <StatPill
                icon="blur"
                label={t("ui.city.timeline.metric_pm25", "PM2.5")}
                value={formatPm(featured.pm25)}
                tone="warning"
              />
            </View>

            {heroTimeline.length > 0 ? (
              <View style={styles.heroChartsWrap}>
                <View style={styles.chartCard}>
                  <View style={styles.chartHeader}>
                    <Text style={styles.chartTitle}>{t("ui.city.current.temp_trend", "Temperature Trend")}</Text>
                    <TrendPill values={timelineTemps} label={t("ui.common.range", "Range")} />
                  </View>
                  <MiniLineChart values={timelineTemps} lineColor={colors.heatWarm} fillColor="rgba(243,178,79,0.20)" />
                </View>

                <View style={styles.chartCard}>
                  <View style={styles.chartHeader}>
                    <Text style={styles.chartTitle}>{t("ui.city.current.aqi_trend", "AQI Strip")}</Text>
                    <TrendPill values={timelineAqi} label={t("ui.city.current.live", "Live")} />
                  </View>
                  <ValueStrip values={timelineAqi} colorScale="aqi" />
                </View>
              </View>
            ) : null}

            {heroTimelineLoading ? (
              <Text style={styles.timelineLoadingText}>{t("ui.city.current.timeline_loading", "Loading hourly insight")}</Text>
            ) : null}
          </View>
        </View>
      ) : null}

      {!loading && !error && heroTimeline.length > 0 ? (
        <View style={styles.glanceCard}>
          <HourlyIconStrip
            entries={heroTimeline.map((row) => ({
              time: row.time,
              temperatureC: row.temperatureC,
              aqi: row.aqi,
              weatherCode: row.weatherCode,
              precipitationProbability: row.precipitationProbability,
            }))}
            subtitle={t("ui.city.current.hourly_glance", "Hourly glance")}
          />
        </View>
      ) : null}

      {!loading && !error ? (
        <View style={[styles.panelRow, isWide ? styles.panelRowWide : null]}>
          <View style={styles.panelCard}>
            <Text style={styles.panelTitle}>{t("ui.city.current.top_polluted", "High AQI Cities")}</Text>
            <View style={styles.panelBody}>
              {topPolluted.map((row, idx) => (
                <Pressable
                  key={`${row.cityName}-${idx}`}
                  style={[styles.rankRow, cityToken(row.cityName) === featuredToken ? styles.rankRowActive : null]}
                  onPress={() => {
                    setCityQuery(row.cityName);
                    load(row.cityName);
                  }}
                >
                  <View style={styles.rankLeft}>
                    <View style={styles.rankBadge}>
                      <Text style={styles.rankBadgeText}>{idx + 1}</Text>
                    </View>
                    <View>
                      <Text style={styles.rankCity}>{formatCityName(row.cityName, locale)}</Text>
                      <Text style={styles.rankMeta}>{row.aqiCategoryLabel ?? "—"}</Text>
                    </View>
                  </View>
                  <View style={styles.rankRight}>
                    <Text style={styles.rankValue}>{formatAqi(row.aqi)}</Text>
                    <View style={styles.rankBarTrack}>
                      <View
                        style={[
                          styles.rankBarFill,
                          {
                            width: `${Math.min(100, Math.max(7, (row.aqi ?? 0) / 2))}%`,
                            backgroundColor: getAqiColor(row.aqi),
                          },
                        ]}
                      />
                    </View>
                  </View>
                </Pressable>
              ))}
            </View>
          </View>

          <View style={styles.panelCard}>
            <Text style={styles.panelTitle}>{t("ui.city.current.more_cities", "City Comparison")}</Text>
            <View style={styles.cityGrid}>
              {comparisonRows.map((row, idx) => (
                <Pressable
                  key={`${row.cityName}-${idx}`}
                  style={[
                    styles.cityTile,
                    cityToken(row.cityName) === featuredToken ? styles.cityTileActive : null,
                  ]}
                  onPress={() => {
                    setCityQuery(row.cityName);
                    load(row.cityName);
                  }}
                >
                  <View style={styles.cityTileTop}>
                    <Text numberOfLines={1} style={styles.cityTileName}>
                      {formatCityName(row.cityName, locale)}
                    </Text>
                    <View style={[styles.cityTileDot, { backgroundColor: getAqiColor(row.aqi) }]} />
                  </View>
                  <Text style={styles.cityTileAqi}>{`AQI ${formatAqi(row.aqi)}`}</Text>
                  <Text style={styles.cityTileTemp}>{formatTemperature(row.temperatureC)}</Text>
                </Pressable>
              ))}
            </View>
          </View>
        </View>
      ) : null}

    </ScrollView>
  );
}

function formatDateTime(value: string, locale?: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, { hour: "2-digit", minute: "2-digit" })}`;
}

function formatTemperature(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value.toFixed(1)}°C`;
}

function formatAqi(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return value.toFixed(0);
}

function formatWind(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value.toFixed(1)} m/s`;
}

function formatPm(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return value.toFixed(1);
}

function cityToken(value: string): string {
  return normalizeCityToken(value);
}

function dedupeByCityToken(rows: OverviewCard[]): OverviewCard[] {
  const seen = new Set<string>();
  const output: OverviewCard[] = [];
  rows.forEach((row) => {
    const token = cityToken(row.cityName);
    if (seen.has(token)) return;
    seen.add(token);
    output.push(row);
  });
  return output;
}

function buildCurrentInsight(featured: OverviewCard | null, t: (key: string, fallback?: string) => string): string {
  if (!featured) return t("ui.city.current.now_unknown", "Current insight is unavailable.");
  const aqi = featured.aqi ?? null;
  const temp = featured.temperatureC ?? null;
  if (aqi !== null && aqi >= 110) {
    return t("ui.city.current.now_high_risk", "Air quality is elevated right now. Consider reducing outdoor exposure.");
  }
  if (temp !== null && temp >= 30) {
    return t("ui.city.current.now_hot", "Conditions are warm now. Prefer shade and hydration outdoors.");
  }
  if (aqi !== null && aqi <= 60) {
    return t("ui.city.current.now_good", "Air quality is currently in a comfortable range for most users.");
  }
  return t("ui.city.current.now_moderate", "Conditions are moderate now. Check the timeline before longer activities.");
}

const styles = StyleSheet.create({
  content: {
    paddingHorizontal: spacing.lg,
    paddingBottom: spacing.xxl,
    gap: spacing.sm,
  },
  titleWrap: {
    gap: spacing.xxs,
  },
  title: {
    color: colors.textPrimary,
    fontSize: typography.size.title,
    fontWeight: "800",
    fontFamily: typography.display,
  },
  subtitle: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  searchCard: {
    borderRadius: radius.lg,
    borderColor: "#416C93",
    borderWidth: 1,
    backgroundColor: "#102D45",
    padding: spacing.xs,
    gap: spacing.xs,
    ...shadow.card,
  },
  searchHeader: {
    flexDirection: "row",
    alignItems: "flex-end",
    paddingHorizontal: spacing.xs,
    paddingTop: 2,
  },
  sectionEyebrow: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    letterSpacing: 0.6,
    fontWeight: "700",
    fontFamily: typography.body,
    textTransform: "uppercase",
  },
  searchWrap: {
    flexDirection: "row",
    gap: spacing.sm,
  },
  searchInput: {
    flex: 1,
    backgroundColor: colors.canvasElevated,
    borderColor: colors.borderStrong,
    borderWidth: 1,
    borderRadius: radius.sm,
    color: colors.textPrimary,
    paddingHorizontal: spacing.md,
    paddingVertical: spacing.sm,
    fontSize: typography.size.body,
    fontFamily: typography.body,
  },
  searchButton: {
    borderRadius: radius.sm,
    backgroundColor: colors.accentStrong,
    borderColor: "#69B9FB",
    borderWidth: 1,
    paddingHorizontal: spacing.md,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 6,
  },
  searchButtonText: {
    color: colors.onAccent,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  quickWrap: {
    gap: spacing.xs,
  },
  quickTitle: {
    color: "#B8D2E7",
    fontSize: typography.size.caption,
    textTransform: "uppercase",
    letterSpacing: 0.45,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  quickList: {
    gap: spacing.xs,
  },
  quickChip: {
    borderRadius: radius.pill,
    borderColor: "#476A8B",
    borderWidth: 1,
    backgroundColor: "rgba(26, 56, 84, 0.9)",
    paddingHorizontal: spacing.sm,
    paddingVertical: 6,
  },
  quickChipActive: {
    borderColor: "#68B9FB",
    backgroundColor: colors.accentStrong,
  },
  quickChipText: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  quickChipTextActive: {
    color: colors.onAccent,
  },
  heroCard: {
    borderRadius: radius.lg,
    borderColor: "#46729A",
    borderWidth: 1,
    backgroundColor: colors.darkPanel,
    padding: spacing.sm,
    gap: spacing.xs,
    position: "relative",
    overflow: "hidden",
    ...shadow.floating,
  },
  heroContent: {
    gap: spacing.sm,
    zIndex: 1,
  },
  heroTop: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    gap: spacing.md,
  },
  heroEyebrow: {
    color: "#A6C8E9",
    fontSize: typography.size.caption,
    letterSpacing: 0.5,
    fontWeight: "700",
    fontFamily: typography.body,
    textTransform: "uppercase",
  },
  heroCity: {
    color: colors.darkTextOnPanel,
    fontSize: 28,
    lineHeight: 31,
    fontWeight: "800",
    fontFamily: typography.display,
  },
  heroMeta: {
    color: "#AFC9E2",
    fontSize: typography.size.body,
    fontFamily: typography.body,
  },
  heroWeatherWrap: {
    alignItems: "flex-end",
    gap: 2,
  },
  heroTemp: {
    color: colors.darkTextOnPanel,
    fontSize: 28,
    fontWeight: "800",
    fontFamily: typography.display,
  },
  heroFeelsLike: {
    color: "#CFE4F7",
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  heroBadgeRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.md,
  },
  aqiBadge: {
    alignSelf: "flex-start",
    borderRadius: radius.pill,
    borderWidth: 1,
    backgroundColor: "#16344F",
    paddingHorizontal: spacing.sm,
    paddingVertical: 6,
    flexDirection: "row",
    alignItems: "center",
    gap: 7,
  },
  aqiDot: {
    width: 8,
    height: 8,
    borderRadius: radius.pill,
  },
  aqiBadgeText: {
    color: colors.darkTextOnPanel,
    fontSize: typography.size.caption,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  heroUpdated: {
    color: "#A3C2DD",
    fontSize: typography.size.caption,
    fontFamily: typography.mono,
  },
  heroInsight: {
    color: "#D7E8F7",
    fontSize: typography.size.bodySm,
    lineHeight: 18,
    fontFamily: typography.body,
  },
  heroPills: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.xs,
  },
  heroChartsWrap: {
    gap: spacing.sm,
  },
  chartCard: {
    borderRadius: radius.md,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.surface,
    padding: spacing.sm,
    gap: spacing.xs,
  },
  chartHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.md,
  },
  chartTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  timelineLoadingText: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  panelRow: {
    gap: spacing.md,
  },
  panelRowWide: {
    flexDirection: "row",
    alignItems: "flex-start",
  },
  panelCard: {
    flex: 1,
    borderRadius: radius.md,
    borderColor: "#365A7D",
    borderWidth: 1,
    backgroundColor: "#112E45",
    padding: spacing.sm,
    gap: spacing.sm,
    ...shadow.card,
  },
  panelTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  panelBody: {
    gap: spacing.xs,
  },
  rankRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.md,
    paddingVertical: spacing.xs,
    borderBottomColor: colors.border,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  rankRowActive: {
    backgroundColor: "#1A3854",
    borderRadius: radius.sm,
    paddingHorizontal: spacing.xs,
  },
  rankLeft: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.sm,
    flex: 1,
  },
  rankBadge: {
    width: 24,
    height: 24,
    borderRadius: radius.pill,
    borderWidth: 1,
    borderColor: colors.borderStrong,
    backgroundColor: colors.canvasElevated,
    alignItems: "center",
    justifyContent: "center",
  },
  rankBadgeText: {
    color: colors.textPrimary,
    fontSize: typography.size.caption,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  rankCity: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  rankMeta: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  rankRight: {
    width: 88,
    alignItems: "flex-end",
    gap: 4,
  },
  rankValue: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  rankBarTrack: {
    width: "100%",
    height: 5,
    borderRadius: radius.pill,
    backgroundColor: "#274663",
    overflow: "hidden",
  },
  rankBarFill: {
    height: "100%",
    borderRadius: radius.pill,
  },
  cityGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.xs,
  },
  cityTile: {
    width: "48%",
    minWidth: 132,
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: "#355B7D",
    backgroundColor: "#1A3956",
    padding: spacing.sm,
    gap: 4,
    ...shadow.card,
  },
  cityTileActive: {
    borderColor: "#7BC7FF",
    backgroundColor: "#245074",
  },
  cityTileTop: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.xs,
  },
  cityTileName: {
    flex: 1,
    color: colors.textPrimary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  cityTileDot: {
    width: 8,
    height: 8,
    borderRadius: radius.pill,
  },
  cityTileAqi: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  cityTileTemp: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "800",
    fontFamily: typography.display,
  },
  glanceCard: {
    borderRadius: radius.md,
    borderColor: "#365B7D",
    borderWidth: 1,
    backgroundColor: "#122F46",
    padding: spacing.sm,
    ...shadow.card,
  },
});
