import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Pressable, ScrollView, StyleSheet, Text, TextInput, View, useWindowDimensions } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { fetchCityCurrent, fetchCityTimeline } from "../api/endpoints";
import { EmptyBlock } from "../components/EmptyBlock";
import { ErrorBlock } from "../components/ErrorBlock";
import { LoadingBlock } from "../components/LoadingBlock";
import { MetricCard } from "../components/MetricCard";
import { WeatherMoodBackdrop } from "../components/WeatherMoodBackdrop";
import {
  HourlyIconStrip,
  MiniLineChart,
  TrendPill,
  ValueStrip,
  getAqiColor,
  weatherIconForConditions,
  weatherMoodForConditions,
} from "../components/visuals";
import { colors, radius, shadow, spacing, typography } from "../theme/tokens";
import { formatCityName, normalizeCityToken, prioritizeCityNames } from "../utils/cities";
import { mapTimelinePoint } from "../viewModels/mappers";
import type { BaseScreenProps } from "./types";

type TimelinePoint = ReturnType<typeof mapTimelinePoint>;

type DayGroup = {
  day: string;
  dayLabel: string;
  rows: TimelinePoint[];
};

type TimelineMode = "hourly" | "daily" | "weekly";

export function CityTimelineScreen({ locale, t, client }: BaseScreenProps) {
  const { width } = useWindowDimensions();
  const isWide = width >= 940;

  const [selectedCity, setSelectedCity] = useState<string | null>(null);
  const [citySearch, setCitySearch] = useState<string>("");
  const [timelineMode, setTimelineMode] = useState<TimelineMode>("hourly");
  const [cities, setCities] = useState<string[]>([]);
  const [rows, setRows] = useState<TimelinePoint[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const loadCities = useCallback(async () => {
    if (!client) {
      setError(t("ui.common.backend_unreachable", "Backend unreachable"));
      return;
    }
    const current = await fetchCityCurrent(client, locale);
    const names = current.map((row) => row.city_name).filter(Boolean);
    const uniqueNames = Array.from(new Set(names)).sort((a, b) => a.localeCompare(b, "tr-TR", { sensitivity: "base" }));
    const prioritizedNames = prioritizeCityNames(uniqueNames);
    setCities(prioritizedNames);
    setSelectedCity((prev) => prev ?? prioritizedNames[0] ?? null);
  }, [client, locale, t]);

  const loadTimeline = useCallback(
    async (city: string, mode: TimelineMode) => {
      if (!client) return;
      const limit = mode === "hourly" ? 120 : mode === "daily" ? 21 * 24 : 56 * 24;
      const timeline = await fetchCityTimeline(client, city, limit);
      setRows(timeline.map(mapTimelinePoint));
    },
    [client]
  );

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      await loadCities();
    } catch (rootError) {
      setError(rootError instanceof Error ? rootError.message : "Load failed.");
    } finally {
      setLoading(false);
    }
  }, [loadCities]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    if (!selectedCity || !client) return;
    setLoading(true);
    setError(null);
    loadTimeline(selectedCity, timelineMode)
      .catch((timelineError) => {
        setError(timelineError instanceof Error ? timelineError.message : "Timeline request failed.");
      })
      .finally(() => setLoading(false));
  }, [client, loadTimeline, selectedCity, timelineMode]);

  const displayRows = useMemo(() => {
    if (timelineMode === "daily") return aggregateDaily(rows);
    if (timelineMode === "weekly") return aggregateWeekly(rows);
    return rows;
  }, [rows, timelineMode]);

  const stats = useMemo(() => {
    if (displayRows.length === 0) {
      return {
        avgTemp: null,
        avgAqi: null,
        maxPm25: null,
        rangeLabel: "—",
      };
    }

    const tempValues = displayRows.map((row) => row.temperatureC).filter((v): v is number => v !== null);
    const aqiValues = displayRows.map((row) => row.aqi).filter((v): v is number => v !== null);
    const pm25Values = displayRows.map((row) => row.pm25).filter((v): v is number => v !== null);

    const avgTemp = tempValues.length > 0 ? tempValues.reduce((acc, cur) => acc + cur, 0) / tempValues.length : null;
    const avgAqi = aqiValues.length > 0 ? aqiValues.reduce((acc, cur) => acc + cur, 0) / aqiValues.length : null;
    const maxPm25 = pm25Values.length > 0 ? Math.max(...pm25Values) : null;

    return {
      avgTemp,
      avgAqi,
      maxPm25,
      rangeLabel: `${formatDate(displayRows[0]?.time, locale)} → ${formatDate(displayRows[displayRows.length - 1]?.time, locale)}`,
    };
  }, [displayRows, locale]);

  const grouped = useMemo(
    () => (timelineMode === "hourly" ? groupRowsByDay(displayRows, locale) : groupRowsByPeriod(displayRows, locale, timelineMode)),
    [displayRows, locale, timelineMode]
  );

  const tempSeries = useMemo(() => displayRows.map((row) => row.temperatureC), [displayRows]);
  const aqiSeries = useMemo(() => displayRows.map((row) => row.aqi), [displayRows]);
  const pmSeries = useMemo(() => displayRows.map((row) => row.pm25), [displayRows]);
  const timelineMood = useMemo(() => {
    const current = rows[0] ?? displayRows[0] ?? null;
    const precip = current?.precipitationProbability ?? null;
    return weatherMoodForConditions(current?.weatherCode ?? null, current?.temperatureC ?? null, precip);
  }, [displayRows, rows]);
  const modeLabel = useMemo(() => timelineModeLabel(timelineMode, t), [timelineMode, t]);
  const timelineInsight = useMemo(() => buildTimelineInsight(displayRows, timelineMode, t), [displayRows, timelineMode, t]);
  const chartHeight = timelineMode === "hourly" ? 82 : 68;
  const pmStripHeight = timelineMode === "hourly" ? 44 : 34;
  const compactTrendView = timelineMode !== "hourly" && displayRows.length <= 5;
  const tempMin = useMemo(() => minNumber(tempSeries), [tempSeries]);
  const tempMax = useMemo(() => maxNumber(tempSeries), [tempSeries]);
  const aqiMin = useMemo(() => minNumber(aqiSeries), [aqiSeries]);
  const aqiMax = useMemo(() => maxNumber(aqiSeries), [aqiSeries]);
  const visibleCityChips = useMemo(() => {
    const query = normalizeCityToken(citySearch);
    if (query.length > 0) {
      const filtered = cities.filter((name) => normalizeCityToken(name).includes(query));
      if (filtered.length > 0) return filtered.slice(0, 5);
      return selectedCity ? [selectedCity] : [];
    }
    const prioritized = selectedCity ? [selectedCity, ...cities.filter((name) => name !== selectedCity)] : cities;
    return prioritized.slice(0, 5);
  }, [cities, citySearch, selectedCity]);
  const weeklyHighlights = useMemo(
    () => (timelineMode === "weekly" ? buildWeeklyHighlights(displayRows, locale, t) : null),
    [displayRows, locale, t, timelineMode]
  );

  return (
    <ScrollView contentContainerStyle={styles.content} showsVerticalScrollIndicator={false}>
      <View style={styles.titleWrap}>
        <Text style={styles.title}>{t("ui.city.timeline.title", "Hourly Timeline")}</Text>
        <Text style={styles.subtitle}>
          {`${t("ui.city.timeline.subtitle", "Hourly environmental flow for selected city")} · ${modeLabel}`}
        </Text>
      </View>

      <View style={styles.citySelectorCard}>
        <View style={styles.citySelectorHead}>
          <View>
            <Text style={styles.citySelectorLabel}>{t("ui.city.timeline.city_selector", "City")}</Text>
            <Text style={styles.citySelectorValue}>
              {selectedCity ? formatCityName(selectedCity, locale) : t("ui.common.no_city_selected", "No city selected")}
            </Text>
          </View>
        </View>
        <TextInput
          value={citySearch}
          onChangeText={setCitySearch}
          placeholder={t("ui.city.timeline.search_city_placeholder", "Filter city")}
          placeholderTextColor={colors.textTertiary}
          style={styles.citySearchInput}
          autoCapitalize="words"
          autoCorrect={false}
        />
        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.cityChips}>
          {visibleCityChips.map((name) => {
            const active = name === selectedCity;
            return (
              <Pressable key={name} onPress={() => setSelectedCity(name)} style={[styles.chip, active ? styles.chipActive : null]}>
                <Text style={[styles.chipText, active ? styles.chipTextActive : null]}>{formatCityName(name, locale)}</Text>
              </Pressable>
            );
          })}
        </ScrollView>
        <View style={styles.modeWrap}>
          <ModeChip
            label={t("ui.city.timeline.mode_hourly", "Hourly")}
            active={timelineMode === "hourly"}
            onPress={() => setTimelineMode("hourly")}
          />
          <ModeChip
            label={t("ui.city.timeline.mode_daily", "Daily")}
            active={timelineMode === "daily"}
            onPress={() => setTimelineMode("daily")}
          />
          <ModeChip
            label={t("ui.city.timeline.mode_weekly", "Weekly")}
            active={timelineMode === "weekly"}
            onPress={() => setTimelineMode("weekly")}
          />
        </View>
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
          onRetry={refresh}
        />
      ) : null}

      {!loading && !error && displayRows.length === 0 ? (
        <EmptyBlock
          title={t("ui.common.empty", "No data")}
          detail={t("ui.common.empty_detail", "Adjust your filter and try again.")}
          badgeLabel={t("ui.common.no_data_badge", "No Data")}
        />
      ) : null}

      {!loading && !error && displayRows.length > 0 ? (
        <>
          <View style={styles.heroCard}>
            <WeatherMoodBackdrop mood={timelineMood} />
            <View style={styles.heroHead}>
              <View style={styles.heroLeft}>
                <MaterialCommunityIcons name="clock-time-eight-outline" size={18} color={colors.info} />
                <Text style={styles.heroTitle}>{modeLabel}</Text>
              </View>
              <Text style={styles.heroRange}>{stats.rangeLabel}</Text>
            </View>
            <Text style={styles.heroInsight}>{timelineInsight}</Text>

            <HourlyIconStrip
              entries={displayRows.map((row) => ({
                time: row.time,
                temperatureC: row.temperatureC,
                aqi: row.aqi,
                weatherCode: row.weatherCode,
                precipitationProbability: row.precipitationProbability,
              }))}
              subtitle={t("ui.city.timeline.icon_strip", "Weather glance strip")}
            />

            <View style={[styles.heroChartRow, isWide ? styles.heroChartRowWide : null]}>
              <View style={styles.chartCard}>
                <View style={styles.chartTitleRow}>
                  <Text style={styles.chartTitle}>{t("ui.city.timeline.temp_trend", "Temperature Curve")}</Text>
                  <TrendPill values={tempSeries} label={t("ui.common.range", "Range")} />
                </View>
                {compactTrendView ? (
                  <Text style={styles.chartCompactNote}>{`${formatTemp(tempMin)} → ${formatTemp(tempMax)}`}</Text>
                ) : (
                  <MiniLineChart values={tempSeries} height={chartHeight} lineColor={colors.heatWarm} fillColor="rgba(243,178,79,0.20)" />
                )}
              </View>
              <View style={styles.chartCard}>
                <View style={styles.chartTitleRow}>
                  <Text style={styles.chartTitle}>{t("ui.city.timeline.aqi_trend", "AQI Curve")}</Text>
                  <TrendPill values={aqiSeries} label="AQI" />
                </View>
                {compactTrendView ? (
                  <Text style={styles.chartCompactNote}>{`AQI ${formatNumber(aqiMin)} → ${formatNumber(aqiMax)}`}</Text>
                ) : (
                  <MiniLineChart values={aqiSeries} height={chartHeight} lineColor={colors.aqiHazard} fillColor="rgba(239,94,104,0.18)" />
                )}
              </View>
            </View>

            <View style={styles.chartCard}>
              <View style={styles.chartTitleRow}>
                <Text style={styles.chartTitle}>{t("ui.city.timeline.pm_strip", "PM2.5 Strip")}</Text>
                <TrendPill values={pmSeries} label="PM2.5" />
              </View>
              {compactTrendView ? (
                <Text style={styles.chartCompactNote}>{`${t("ui.city.timeline.max_pm25", "Max PM2.5")}: ${formatPm(stats.maxPm25)}`}</Text>
              ) : (
                <ValueStrip values={pmSeries} colorScale="aqi" height={pmStripHeight} />
              )}
            </View>
          </View>

          <View style={styles.statsGrid}>
            <MetricCard
              title={t("ui.city.timeline.avg_temp", "Avg Temperature")}
              value={formatTemp(stats.avgTemp)}
              subtitle={`${t("ui.common.range", "Range")}: ${stats.rangeLabel}`}
              icon="thermometer"
              compact
            />
            <MetricCard
              title={t("ui.city.timeline.avg_aqi", "Avg AQI")}
              value={formatNumber(stats.avgAqi)}
              subtitle={selectedCity ? formatCityName(selectedCity, locale) : "—"}
              tone="accent"
              icon="chart-bell-curve-cumulative"
              compact
            />
            <MetricCard
              title={t("ui.city.timeline.max_pm25", "Max PM2.5")}
              value={formatPm(stats.maxPm25)}
              subtitle={t("ui.city.timeline.summary_window", "Observation Window")}
              tone="warning"
              icon="blur"
              compact
            />
          </View>

          {timelineMode === "weekly" && weeklyHighlights ? (
            <View style={styles.weeklySummaryCard}>
              <Text style={styles.weeklySummaryTitle}>{t("ui.city.timeline.weekly_summary", "Weekly Highlights")}</Text>
              <Text style={styles.weeklySummaryInsight}>{weeklyHighlights.insight}</Text>
              <View style={styles.weeklySummaryGrid}>
                <View style={styles.weeklySummaryTile}>
                  <Text style={styles.weeklySummaryLabel}>{t("ui.city.timeline.best_window", "Best Air Window")}</Text>
                  <Text style={styles.weeklySummaryValue}>{weeklyHighlights.bestAqiLabel}</Text>
                </View>
                <View style={styles.weeklySummaryTile}>
                  <Text style={styles.weeklySummaryLabel}>{t("ui.city.timeline.risk_window", "Highest Pressure")}</Text>
                  <Text style={styles.weeklySummaryValue}>{weeklyHighlights.worstAqiLabel}</Text>
                </View>
                <View style={styles.weeklySummaryTile}>
                  <Text style={styles.weeklySummaryLabel}>{t("ui.city.timeline.temp_span", "Temperature Span")}</Text>
                  <Text style={styles.weeklySummaryValue}>{weeklyHighlights.tempSpanLabel}</Text>
                </View>
              </View>
            </View>
          ) : null}

          <View style={styles.timelineCard}>
            <Text style={styles.timelineTitle}>
              {timelineMode === "weekly"
                ? t("ui.city.timeline.week_blocks", "Week Blocks")
                : t("ui.city.timeline.day_blocks", "Day Blocks")}
            </Text>
            <Text style={styles.timelineSubtitle}>{stats.rangeLabel}</Text>

            <View style={styles.timelineBody}>
              {grouped.map((group) => (
                <View key={group.day} style={styles.dayGroup}>
                  <View style={styles.dayHeader}>
                    <Text style={styles.dayLabel}>{group.dayLabel}</Text>
                    <View style={styles.dayMetaWrap}>
                      <Text style={styles.dayMetaText}>
                        {timelineMode === "hourly"
                          ? `${group.rows.length} h`
                          : t("ui.city.timeline.summary", "summary")}
                      </Text>
                      <Text style={styles.dayMetaText}>{`AQI ${formatNumber(dayAvgAqi(group.rows))}`}</Text>
                    </View>
                  </View>

                  {group.rows.map((row) => {
                    const rowWeatherIcon = weatherIconForConditions(
                      row.weatherCode,
                      row.temperatureC,
                      row.aqi,
                      row.precipitationProbability
                    );

                    if (timelineMode !== "hourly") {
                      return (
                        <View key={row.time} style={styles.periodRow}>
                          <View style={styles.periodRowTop}>
                            <View style={styles.periodTitleWrap}>
                              <MaterialCommunityIcons name={rowWeatherIcon.name} size={15} color={rowWeatherIcon.color} />
                              <Text style={styles.periodTitle}>{formatTimelinePointLabel(row.time, timelineMode, locale)}</Text>
                            </View>
                            <Text style={styles.periodTemp}>{formatTemp(row.temperatureC)}</Text>
                          </View>
                          <View style={styles.periodRowBottom}>
                            <View style={[styles.metricPill, styles.metricPillAqi]}>
                              <MaterialCommunityIcons name="waves-arrow-up" size={14} color={getAqiColor(row.aqi)} />
                              <Text style={styles.metricPillValue}>{`AQI ${formatNumber(row.aqi)}`}</Text>
                            </View>
                            <Text style={styles.periodMetaText}>{`PM2.5 ${formatPm(row.pm25)} · ${t("ui.city.timeline.metric_wind", "Wind")} ${formatWind(row.windSpeed10m)}`}</Text>
                          </View>
                        </View>
                      );
                    }

                    return (
                      <View key={row.time} style={styles.timelineRow}>
                        <View style={styles.timelineRowLeft}>
                          <Text style={styles.timelineTime}>{formatTimelinePointLabel(row.time, timelineMode, locale)}</Text>
                          <View style={[styles.timelineAqiDot, { backgroundColor: getAqiColor(row.aqi) }]} />
                        </View>
                        <View style={styles.timelineMetrics}>
                          <View style={[styles.metricPill, styles.metricPillTemp]}>
                            <MaterialCommunityIcons name={rowWeatherIcon.name} size={14} color={rowWeatherIcon.color} />
                            <Text style={styles.metricPillValue}>{formatTemp(row.temperatureC)}</Text>
                          </View>
                          <View style={[styles.metricPill, styles.metricPillAqi]}>
                            <MaterialCommunityIcons name="waves-arrow-up" size={14} color={getAqiColor(row.aqi)} />
                            <Text style={styles.metricPillValue}>{`AQI ${formatNumber(row.aqi)}`}</Text>
                          </View>
                          <View style={styles.metricStack}>
                            <Text style={styles.metricStackText}>{`PM2.5 ${formatPm(row.pm25)}`}</Text>
                            <Text style={styles.metricStackText}>{`PM10 ${formatPm(row.pm10)}`}</Text>
                            <Text style={styles.metricStackText}>{`${t("ui.city.timeline.metric_wind", "Wind")} ${formatWind(row.windSpeed10m)}`}</Text>
                          </View>
                        </View>
                      </View>
                    );
                  })}
                </View>
              ))}
            </View>
          </View>
        </>
      ) : null}
    </ScrollView>
  );
}

function ModeChip({
  label,
  active,
  onPress,
}: {
  label: string;
  active: boolean;
  onPress: () => void;
}) {
  return (
    <Pressable onPress={onPress} style={[styles.modeChip, active ? styles.modeChipActive : null]}>
      <Text style={[styles.modeChipText, active ? styles.modeChipTextActive : null]}>{label}</Text>
    </Pressable>
  );
}

function timelineModeLabel(mode: TimelineMode, t: (key: string, fallback?: string) => string): string {
  if (mode === "daily") return t("ui.city.timeline.mode_daily", "Daily");
  if (mode === "weekly") return t("ui.city.timeline.mode_weekly", "Weekly");
  return t("ui.city.timeline.mode_hourly", "Hourly");
}

function buildTimelineInsight(
  rows: TimelinePoint[],
  mode: TimelineMode,
  t: (key: string, fallback?: string) => string
): string {
  const tempValues = rows.map((row) => row.temperatureC).filter((value): value is number => value !== null && !Number.isNaN(value));
  const aqiValues = rows.map((row) => row.aqi).filter((value): value is number => value !== null && !Number.isNaN(value));
  if (tempValues.length < 2 || aqiValues.length < 2) {
    return t("ui.city.timeline.insight_missing", "Not enough points yet for a stable trend insight.");
  }

  const tempDelta = tempValues[tempValues.length - 1] - tempValues[0];
  const aqiDelta = aqiValues[aqiValues.length - 1] - aqiValues[0];
  const tempPhrase =
    tempDelta > 1.5
      ? t("ui.city.timeline.insight_temp_up", "temperature is trending upward")
      : tempDelta < -1.5
        ? t("ui.city.timeline.insight_temp_down", "temperature is trending downward")
        : t("ui.city.timeline.insight_temp_flat", "temperature is staying relatively stable");
  const aqiPhrase =
    aqiDelta > 4
      ? t("ui.city.timeline.insight_aqi_up", "air quality pressure is increasing")
      : aqiDelta < -4
        ? t("ui.city.timeline.insight_aqi_down", "air quality is improving")
        : t("ui.city.timeline.insight_aqi_flat", "air quality is mostly stable");

  const modeLabel = mode === "hourly" ? t("ui.city.timeline.mode_hourly", "Hourly") : mode === "daily" ? t("ui.city.timeline.mode_daily", "Daily") : t("ui.city.timeline.mode_weekly", "Weekly");
  return `${modeLabel}: ${tempPhrase}; ${aqiPhrase}.`;
}

function aggregateDaily(rows: TimelinePoint[]): TimelinePoint[] {
  const grouped = new Map<string, TimelinePoint[]>();
  rows.forEach((row) => {
    const key = timeKeyByDay(row.time);
    const bucket = grouped.get(key) ?? [];
    bucket.push(row);
    grouped.set(key, bucket);
  });
  return Array.from(grouped.entries()).map(([dayKey, bucket]) => reduceTimelineBucket(bucket, dayKey));
}

function aggregateWeekly(rows: TimelinePoint[]): TimelinePoint[] {
  const grouped = new Map<string, TimelinePoint[]>();
  rows.forEach((row) => {
    const key = timeKeyByWeek(row.time);
    const bucket = grouped.get(key) ?? [];
    bucket.push(row);
    grouped.set(key, bucket);
  });
  return Array.from(grouped.entries()).map(([weekKey, bucket]) => reduceTimelineBucket(bucket, weekKey));
}

function reduceTimelineBucket(bucket: TimelinePoint[], key: string): TimelinePoint {
  return {
    time: key,
    temperatureC: meanNumber(bucket.map((row) => row.temperatureC)),
    weatherCode: dominantNumber(bucket.map((row) => row.weatherCode)),
    precipitationProbability: maxNumber(bucket.map((row) => row.precipitationProbability)),
    windSpeed10m: meanNumber(bucket.map((row) => row.windSpeed10m)),
    aqi: meanNumber(bucket.map((row) => row.aqi)),
    pm25: meanNumber(bucket.map((row) => row.pm25)),
    pm10: meanNumber(bucket.map((row) => row.pm10)),
  };
}

function groupRowsByDay(rows: TimelinePoint[], locale?: string): DayGroup[] {
  const grouped = new Map<string, TimelinePoint[]>();
  rows.forEach((row) => {
    const d = new Date(row.time);
    const dayKey = Number.isNaN(d.getTime()) ? row.time.slice(0, 10) : d.toISOString().slice(0, 10);
    const bucket = grouped.get(dayKey) ?? [];
    bucket.push(row);
    grouped.set(dayKey, bucket);
  });

  return Array.from(grouped.entries()).map(([day, dayRows]) => ({
    day,
    dayLabel: formatDay(day, locale),
    rows: dayRows,
  }));
}

function groupRowsByPeriod(rows: TimelinePoint[], locale: string, mode: TimelineMode): DayGroup[] {
  return rows.map((row) => ({
    day: row.time,
    dayLabel:
      mode === "daily"
        ? formatDay(row.time, locale)
        : `${formatDate(row.time, locale)} · ${formatWeekLabel(row.time, locale)}`,
    rows: [row],
  }));
}

function dayAvgAqi(rows: TimelinePoint[]): number | null {
  const values = rows.map((row) => row.aqi).filter((value): value is number => value !== null && !Number.isNaN(value));
  if (values.length === 0) return null;
  return values.reduce((acc, cur) => acc + cur, 0) / values.length;
}

function buildWeeklyHighlights(
  rows: TimelinePoint[],
  locale: string,
  t: (key: string, fallback?: string) => string
): {
  insight: string;
  bestAqiLabel: string;
  worstAqiLabel: string;
  tempSpanLabel: string;
} {
  if (rows.length === 0) {
    return {
      insight: t("ui.city.timeline.insight_missing", "Not enough points yet for a stable trend insight."),
      bestAqiLabel: "—",
      worstAqiLabel: "—",
      tempSpanLabel: "—",
    };
  }

  const withAqi = rows.filter((row) => row.aqi !== null && !Number.isNaN(row.aqi));
  const withTemp = rows.filter((row) => row.temperatureC !== null && !Number.isNaN(row.temperatureC));
  const bestAqi = withAqi.length > 0 ? withAqi.reduce((best, row) => ((row.aqi ?? Infinity) < (best.aqi ?? Infinity) ? row : best), withAqi[0]) : null;
  const worstAqi = withAqi.length > 0 ? withAqi.reduce((worst, row) => ((row.aqi ?? -Infinity) > (worst.aqi ?? -Infinity) ? row : worst), withAqi[0]) : null;
  const tempValues = withTemp.map((row) => row.temperatureC as number);
  const minTemp = tempValues.length > 0 ? Math.min(...tempValues) : null;
  const maxTemp = tempValues.length > 0 ? Math.max(...tempValues) : null;

  const bestAqiLabel =
    bestAqi !== null ? `${formatWeekLabel(bestAqi.time, locale)} · AQI ${formatNumber(bestAqi.aqi)}` : "—";
  const worstAqiLabel =
    worstAqi !== null ? `${formatWeekLabel(worstAqi.time, locale)} · AQI ${formatNumber(worstAqi.aqi)}` : "—";
  const tempSpanLabel = `${formatTemp(minTemp)} → ${formatTemp(maxTemp)}`;

  const insight = `${t("ui.city.timeline.mode_weekly", "Weekly")}: ${t(
    "ui.city.timeline.weekly_insight_prefix",
    "Air quality pressure varies across weeks; use highlighted windows for planning."
  )}`;
  return { insight, bestAqiLabel, worstAqiLabel, tempSpanLabel };
}

function formatDay(day: string, locale?: string): string {
  const d = new Date(day);
  if (Number.isNaN(d.getTime())) return day;
  return d.toLocaleDateString(locale, { weekday: "short", month: "short", day: "numeric" });
}

function formatDate(value?: string, locale?: string): string {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString(locale);
}

function formatHour(value: string): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return `${String(d.getHours()).padStart(2, "0")}:00`;
}

function formatTimelinePointLabel(value: string, mode: TimelineMode, locale?: string): string {
  if (mode === "hourly") return formatHour(value);
  if (mode === "daily") return formatDay(value, locale);
  return formatWeekLabel(value, locale);
}

function formatTemp(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value.toFixed(1)}°`;
}

function formatPm(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return value.toFixed(1);
}

function formatNumber(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return value.toFixed(0);
}

function formatWind(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value.toFixed(1)} m/s`;
}

function formatWeekLabel(value: string, locale?: string): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString(locale, { month: "short", day: "numeric" });
}

function timeKeyByDay(value: string): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value.slice(0, 10);
  return d.toISOString().slice(0, 10);
}

function timeKeyByWeek(value: string): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value.slice(0, 10);
  const copy = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = copy.getUTCDay() || 7;
  copy.setUTCDate(copy.getUTCDate() - (day - 1));
  return copy.toISOString().slice(0, 10);
}

function meanNumber(values: Array<number | null>): number | null {
  const numeric = values.filter((value): value is number => value !== null && !Number.isNaN(value));
  if (numeric.length === 0) return null;
  return numeric.reduce((acc, cur) => acc + cur, 0) / numeric.length;
}

function maxNumber(values: Array<number | null>): number | null {
  const numeric = values.filter((value): value is number => value !== null && !Number.isNaN(value));
  if (numeric.length === 0) return null;
  return Math.max(...numeric);
}

function minNumber(values: Array<number | null>): number | null {
  const numeric = values.filter((value): value is number => value !== null && !Number.isNaN(value));
  if (numeric.length === 0) return null;
  return Math.min(...numeric);
}

function dominantNumber(values: Array<number | null>): number | null {
  const counts = new Map<number, number>();
  values.forEach((value) => {
    if (value === null || Number.isNaN(value)) return;
    counts.set(value, (counts.get(value) ?? 0) + 1);
  });
  let selected: number | null = null;
  let maxCount = -1;
  counts.forEach((count, key) => {
    if (count > maxCount) {
      maxCount = count;
      selected = key;
    }
  });
  return selected;
}

const styles = StyleSheet.create({
  content: {
    paddingHorizontal: spacing.lg,
    paddingBottom: spacing.xxl,
    gap: spacing.md,
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
  citySelectorCard: {
    borderRadius: radius.md,
    borderColor: "#395E83",
    borderWidth: 1,
    backgroundColor: "#102B41",
    padding: spacing.xs,
    gap: spacing.xs,
    ...shadow.card,
  },
  citySelectorHead: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.md,
  },
  citySelectorLabel: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    textTransform: "uppercase",
    letterSpacing: 0.5,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  citySelectorValue: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  citySearchInput: {
    backgroundColor: "#0D2337",
    borderColor: "#466B90",
    borderWidth: 1,
    borderRadius: radius.sm,
    color: colors.textPrimary,
    paddingHorizontal: spacing.sm,
    paddingVertical: 7,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  cityChips: {
    gap: spacing.xs,
  },
  modeWrap: {
    alignSelf: "stretch",
    flexDirection: "row",
    gap: 4,
    borderRadius: radius.pill,
    borderWidth: 1,
    borderColor: "#4F77A0",
    backgroundColor: "#17344D",
    padding: 2,
  },
  modeChip: {
    flex: 1,
    borderRadius: radius.pill,
    borderColor: "transparent",
    borderWidth: 1,
    backgroundColor: "transparent",
    paddingHorizontal: spacing.sm,
    paddingVertical: 6,
    alignItems: "center",
  },
  modeChipActive: {
    borderColor: "#6CBCFF",
    backgroundColor: colors.accentStrong,
    ...shadow.glow,
  },
  modeChipText: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  modeChipTextActive: {
    color: colors.onAccent,
  },
  chip: {
    borderRadius: radius.pill,
    borderColor: "#4A6D90",
    borderWidth: 1,
    backgroundColor: "rgba(23, 56, 83, 0.88)",
    paddingHorizontal: spacing.sm,
    paddingVertical: 5,
  },
  chipActive: {
    borderColor: "#67BAFE",
    backgroundColor: colors.accentStrong,
  },
  chipText: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  chipTextActive: {
    color: colors.onAccent,
  },
  heroCard: {
    borderRadius: radius.lg,
    borderColor: "#3A6488",
    borderWidth: 1,
    backgroundColor: colors.darkPanel,
    padding: spacing.sm,
    gap: spacing.sm,
    position: "relative",
    overflow: "hidden",
    ...shadow.floating,
  },
  heroHead: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.md,
    zIndex: 1,
  },
  heroLeft: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.xs,
  },
  heroTitle: {
    color: colors.darkTextOnPanel,
    fontSize: typography.size.bodyLg,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  heroRange: {
    color: "#A9C7E1",
    fontSize: typography.size.caption,
    fontFamily: typography.mono,
  },
  heroInsight: {
    color: "#D5E8F9",
    fontSize: typography.size.bodySm,
    lineHeight: 18,
    fontFamily: typography.body,
    zIndex: 1,
  },
  heroChartRow: {
    gap: spacing.sm,
    zIndex: 1,
  },
  heroChartRowWide: {
    flexDirection: "row",
  },
  chartCard: {
    flex: 1,
    borderRadius: radius.md,
    borderColor: "#365A7D",
    borderWidth: 1,
    backgroundColor: "#112B42",
    padding: spacing.sm,
    gap: spacing.xs,
    zIndex: 1,
  },
  chartTitleRow: {
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
  chartCompactNote: {
    color: "#BDD7EC",
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
    paddingVertical: spacing.xs,
  },
  weeklySummaryCard: {
    borderRadius: radius.md,
    borderWidth: 1,
    borderColor: "#3D6388",
    backgroundColor: "#112E45",
    padding: spacing.sm,
    gap: spacing.xs,
    ...shadow.card,
  },
  weeklySummaryTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.bodyLg,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  weeklySummaryInsight: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    lineHeight: 18,
    fontFamily: typography.body,
  },
  weeklySummaryGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.xs,
  },
  weeklySummaryTile: {
    flex: 1,
    minWidth: 150,
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: "#33597C",
    backgroundColor: "#17344D",
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.xs,
    gap: 3,
  },
  weeklySummaryLabel: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  weeklySummaryValue: {
    color: colors.textPrimary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  statsGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.sm,
  },
  timelineCard: {
    borderRadius: radius.md,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.surface,
    padding: spacing.md,
    gap: spacing.sm,
    ...shadow.card,
  },
  timelineTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.bodyLg,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  timelineSubtitle: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  timelineBody: {
    gap: spacing.sm,
  },
  dayGroup: {
    borderRadius: radius.sm,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.surfaceMuted,
    padding: spacing.xs,
    gap: spacing.xs,
  },
  dayHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.md,
  },
  dayLabel: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  dayMetaWrap: {
    flexDirection: "row",
    gap: spacing.xs,
    alignItems: "center",
  },
  dayMetaText: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  timelineRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.sm,
    borderBottomColor: colors.border,
    borderBottomWidth: StyleSheet.hairlineWidth,
    paddingVertical: spacing.xs,
  },
  timelineRowLeft: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.xs,
    minWidth: 70,
  },
  timelineTime: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.mono,
  },
  timelineAqiDot: {
    width: 7,
    height: 7,
    borderRadius: radius.pill,
  },
  timelineMetrics: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.xs,
  },
  metricPill: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    borderRadius: radius.pill,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.canvasElevated,
    paddingVertical: 5,
    paddingHorizontal: spacing.xs,
  },
  metricPillTemp: {
    borderColor: "#7A6642",
  },
  metricPillAqi: {
    borderColor: "#6A5260",
  },
  metricPillValue: {
    color: colors.textPrimary,
    fontSize: typography.size.caption,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  metricStack: {
    alignItems: "flex-end",
    gap: 1,
  },
  metricStackText: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  periodRow: {
    borderRadius: radius.sm,
    borderColor: "#33597C",
    borderWidth: 1,
    backgroundColor: "#193850",
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.xs,
    gap: spacing.xs,
  },
  periodRowTop: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.sm,
  },
  periodTitleWrap: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  periodTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  periodTemp: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "800",
    fontFamily: typography.display,
  },
  periodRowBottom: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.sm,
  },
  periodMetaText: {
    flex: 1,
    textAlign: "right",
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
});
