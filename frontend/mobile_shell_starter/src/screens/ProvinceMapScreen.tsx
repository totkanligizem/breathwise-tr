import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Pressable, ScrollView, StyleSheet, Text, View, useWindowDimensions } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { fetchProvinceMapMetrics } from "../api/endpoints";
import { EmptyBlock } from "../components/EmptyBlock";
import { ErrorBlock } from "../components/ErrorBlock";
import { LoadingBlock } from "../components/LoadingBlock";
import { MetricCard } from "../components/MetricCard";
import { ProvinceChoroplethMap } from "../components/ProvinceChoroplethMap";
import { TrendPill, ValueStrip, getAqiColor } from "../components/visuals";
import { colors, radius, shadow, spacing, typography } from "../theme/tokens";
import { mapProvinceMetric } from "../viewModels/mappers";
import type { BaseScreenProps } from "./types";

type ProvinceMetric = ReturnType<typeof mapProvinceMetric>;
type SortMode = "priority" | "aqi" | "temp";

export function ProvinceMapScreen({ locale, t, client, localizationMeta }: BaseScreenProps) {
  const { width } = useWindowDimensions();
  const isWide = width >= 980;

  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<ProvinceMetric[]>([]);
  const [sortMode, setSortMode] = useState<SortMode>("priority");
  const [selectedProvince, setSelectedProvince] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!client) {
      setError(t("ui.common.backend_unreachable", "Backend unreachable"));
      setLoading(false);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const payload = await fetchProvinceMapMetrics(client, locale);
      const mapped = payload.map((row) => mapProvinceMetric(row, locale, localizationMeta));
      setRows(mapped);
      setSelectedProvince((prev) => prev ?? mapped[0]?.provinceName ?? null);
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : "Request failed.");
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, [client, locale, localizationMeta, t]);

  useEffect(() => {
    load();
  }, [load]);

  const sortedRows = useMemo(() => {
    const copy = [...rows];
    copy.sort((a, b) => {
      if (sortMode === "priority") return (b.priorityScore ?? -1) - (a.priorityScore ?? -1);
      if (sortMode === "aqi") return (b.avgAqi ?? -1) - (a.avgAqi ?? -1);
      return (b.avgTempC ?? -999) - (a.avgTempC ?? -999);
    });
    return copy;
  }, [rows, sortMode]);

  const topRisk = useMemo(() => sortedRows.slice(0, 5), [sortedRows]);
  const aqAlertCount = useMemo(() => rows.filter((row) => row.aqAlertKey?.endsWith("warning")).length, [rows]);
  const heatAlertCount = useMemo(() => rows.filter((row) => row.heatAlertKey?.endsWith("warning")).length, [rows]);
  const maxPriority = useMemo(() => Math.max(...sortedRows.map((row) => row.priorityScore ?? 0), 1), [sortedRows]);
  const medianPriority = useMemo(() => {
    const values = sortedRows
      .map((row) => row.priorityScore)
      .filter((value): value is number => value !== null && !Number.isNaN(value))
      .sort((a, b) => a - b);
    if (values.length === 0) return null;
    const middle = Math.floor(values.length / 2);
    return values.length % 2 === 0 ? (values[middle - 1] + values[middle]) / 2 : values[middle];
  }, [sortedRows]);

  const selected = useMemo(
    () => sortedRows.find((row) => row.provinceName === selectedProvince) ?? sortedRows[0] ?? null,
    [selectedProvince, sortedRows]
  );

  const prioritySeries = useMemo(() => sortedRows.slice(0, 24).map((row) => row.priorityScore), [sortedRows]);
  const mapInsight = useMemo(() => {
    if (sortedRows.length === 0) return t("ui.province.map.insight_missing", "Map insight is not available yet.");
    const top = sortedRows[0];
    return `${t("ui.province.map.insight_top_prefix", "Highest current priority:")} ${top.provinceName} · ${formatInt(top.avgAqi)} AQI`;
  }, [sortedRows, t]);
  const topRiskProvince = sortedRows[0]?.provinceName ?? null;
  const selectedReason = useMemo(() => {
    if (!selected || medianPriority === null || selected.priorityScore === null || Number.isNaN(selected.priorityScore)) {
      return t("ui.province.map.insight_selected_neutral", "Selected province is currently close to the national baseline.");
    }
    const delta = selected.priorityScore - medianPriority;
    if (delta > 0.8) {
      return t("ui.province.map.insight_selected_above", "Selected province is above baseline and needs closer monitoring.");
    }
    if (delta < -0.8) {
      return t("ui.province.map.insight_selected_below", "Selected province is below baseline and currently lower risk.");
    }
    return t("ui.province.map.insight_selected_neutral", "Selected province is currently close to the national baseline.");
  }, [medianPriority, selected, t]);

  return (
    <ScrollView contentContainerStyle={styles.content} showsVerticalScrollIndicator={false}>
      <View style={styles.titleWrap}>
        <Text style={styles.title}>{t("ui.province.map.title", "Province Map Metrics")}</Text>
        <Text style={styles.subtitle}>
          {t("ui.province.map.subtitle", "Province comparison by risk priority and alert level")}
        </Text>
      </View>

      <View style={styles.heroCard}>
        <View style={styles.heroTop}>
          <View style={styles.heroTitleRow}>
            <MaterialCommunityIcons name="map-search" size={18} color={colors.info} />
            <Text style={styles.heroEyebrow}>{t("ui.province.map.alert_summary", "Alert Summary")}</Text>
          </View>
          {topRiskProvince ? (
            <View style={styles.heroTopBadge}>
              <MaterialCommunityIcons name="alert-circle-outline" size={14} color={colors.warning} />
              <Text numberOfLines={1} style={styles.heroTopBadgeText}>
                {topRiskProvince}
              </Text>
            </View>
          ) : null}
        </View>
        <Text style={styles.heroInsight}>{mapInsight}</Text>
        <View style={styles.heroPillRow}>
          <View style={[styles.heroPill, styles.heroPillAq]}>
            <Text style={styles.heroPillText}>{`${t("ui.province.map.aq_alert_count", "AQ Alerts")}: ${aqAlertCount}`}</Text>
          </View>
          <View style={[styles.heroPill, styles.heroPillHeat]}>
            <Text style={styles.heroPillText}>{`${t("ui.province.map.heat_alert_count", "Heat Alerts")}: ${heatAlertCount}`}</Text>
          </View>
        </View>

        <View style={styles.sortOptions}>
          <SortChip label={t("ui.province.map.sort_priority", "Priority")} active={sortMode === "priority"} onPress={() => setSortMode("priority")} />
          <SortChip label={t("ui.province.map.sort_aqi", "AQI")} active={sortMode === "aqi"} onPress={() => setSortMode("aqi")} />
          <SortChip label={t("ui.province.map.sort_temp", "Temperature")} active={sortMode === "temp"} onPress={() => setSortMode("temp")} />
        </View>
      </View>

      {loading ? (
        <LoadingBlock label={t("ui.common.loading", "Loading")} detail={t("ui.common.loading_detail", "Connecting to data sources")} />
      ) : null}
      {!loading && error ? (
        <ErrorBlock
          title={t("ui.common.backend_unreachable", "Backend unreachable")}
          detail={error}
          retryLabel={t("ui.common.retry", "Retry")}
          badgeLabel={t("ui.common.connection_badge", "Connection")}
          onRetry={load}
        />
      ) : null}
      {!loading && !error && rows.length === 0 ? (
        <EmptyBlock
          title={t("ui.common.empty", "No data")}
          detail={t("ui.common.empty_detail", "Adjust your filter and try again.")}
          badgeLabel={t("ui.common.no_data_badge", "No Data")}
        />
      ) : null}

      {!loading && !error && sortedRows.length > 0 ? (
        <View style={[styles.mapSection, isWide ? styles.mapSectionWide : null]}>
          <View style={styles.mapPanel}>
            <View style={styles.mapHeader}>
              <Text style={styles.sectionTitle}>{t("ui.province.map.choropleth", "Province Choropleth")}</Text>
              <TrendPill values={prioritySeries} label={t("ui.province.map.priority_score", "Priority Score")} />
            </View>
            <ProvinceChoroplethMap
              rows={sortedRows}
              sortMode={sortMode}
              locale={locale}
              selectedProvince={selected?.provinceName ?? null}
              onSelectProvince={setSelectedProvince}
              t={t}
            />
            <View style={styles.selectionMetaRow}>
              <Text style={styles.selectionMetaLabel}>{t("ui.province.map.selected", "Selected Province")}</Text>
              <Text style={styles.selectionMetaValue}>{selected?.provinceName ?? "—"}</Text>
            </View>
          </View>

          {selected ? (
            <View style={styles.detailPanel}>
              <View style={styles.detailHead}>
                <Text style={styles.detailTitle}>{selected.provinceName}</Text>
                <View style={[styles.detailAqiBadge, { borderColor: getAqiColor(selected.avgAqi) }]}> 
                  <View style={[styles.detailAqiDot, { backgroundColor: getAqiColor(selected.avgAqi) }]} />
                  <Text style={styles.detailAqiText}>{`AQI ${formatInt(selected.avgAqi)}`}</Text>
                </View>
              </View>

              <View style={styles.detailMetaRow}>
                <Text style={styles.detailMeta}>{selected.aqAlertLabel ?? t("ui.province.map.aq_label", "AQ Alert")}</Text>
                <Text style={styles.detailMeta}>{selected.heatAlertLabel ?? t("ui.province.map.heat_label", "Heat Alert")}</Text>
              </View>
              <Text style={styles.detailInsight}>{selectedReason}</Text>

              <View style={styles.detailStatsRow}>
                <MetricCard
                  title={t("ui.province.map.sort_priority", "Priority")}
                  value={selected.priorityScore !== null && !Number.isNaN(selected.priorityScore) ? selected.priorityScore.toFixed(1) : "—"}
                  subtitle={t("ui.province.map.priority_score", "Priority Score")}
                  icon="target-variant"
                  compact
                />
                <MetricCard
                  title={t("ui.province.map.sort_temp", "Temperature")}
                  value={formatTemp(selected.avgTempC)}
                  subtitle={t("ui.province.map.sort_temp", "Temperature")}
                  icon="thermometer"
                  tone="accent"
                  compact
                />
              </View>

              <Text style={styles.detailStripLabel}>{t("ui.province.map.risk_strip", "Risk Strip")}</Text>
              <ValueStrip values={sortedRows.slice(0, 32).map((row) => row.avgAqi)} colorScale="aqi" />
            </View>
          ) : null}
        </View>
      ) : null}

      {!loading && !error && topRisk.length > 0 ? (
        <View style={styles.sectionCard}>
          <Text style={styles.sectionTitle}>{t("ui.province.map.ranking", "Province Ranking")}</Text>
          <View style={styles.sectionBody}>
            {topRisk.map((row, idx) => {
              const score = row.priorityScore ?? 0;
              const widthPercent = Math.min(100, Math.max(7, (score / maxPriority) * 100));
              return (
                <View key={row.provinceName} style={styles.rankCard}>
                  <View style={styles.rankHead}>
                    <View style={styles.rankNameWrap}>
                      <MaterialCommunityIcons
                        name={rankIconName(idx)}
                        size={14}
                        color={idx === 0 ? colors.warning : idx < 3 ? colors.info : colors.textTertiary}
                      />
                      <Text numberOfLines={1} style={styles.rankName}>
                        {t("ui.common.rank", "Rank")} {idx + 1} · {row.provinceName}
                      </Text>
                    </View>
                    <Text style={styles.rankScore}>{score.toFixed(1)}</Text>
                  </View>
                  <View style={styles.rankBarTrack}>
                    <View style={[styles.rankBarFill, { width: `${widthPercent}%`, backgroundColor: getAqiColor(row.avgAqi) }]} />
                  </View>
                  <View style={styles.rankMetrics}>
                    <View style={[styles.rankMetricPill, { borderColor: getAqiColor(row.avgAqi) }]}>
                      <Text style={styles.rankMetricText}>{`AQI ${formatInt(row.avgAqi)}`}</Text>
                    </View>
                    <View style={styles.rankMetricPill}>
                      <Text style={styles.rankMetricText}>{`PMAX ${formatInt(row.maxAqi)}`}</Text>
                    </View>
                    <View style={styles.rankMetricPill}>
                      <Text style={styles.rankMetricText}>{formatTemp(row.avgTempC)}</Text>
                    </View>
                  </View>
                </View>
              );
            })}
          </View>
        </View>
      ) : null}
    </ScrollView>
  );
}

function SortChip({ label, active, onPress }: { label: string; active: boolean; onPress: () => void }) {
  return (
    <Pressable style={[styles.sortChip, active ? styles.sortChipActive : null]} onPress={onPress}>
      <Text style={[styles.sortChipText, active ? styles.sortChipTextActive : null]}>{label}</Text>
    </Pressable>
  );
}

function formatInt(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return value.toFixed(0);
}

function formatTemp(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value.toFixed(1)}°C`;
}

function rankIconName(idx: number): React.ComponentProps<typeof MaterialCommunityIcons>["name"] {
  if (idx === 0) return "crown-outline";
  if (idx === 1) return "medal-outline";
  if (idx === 2) return "star-outline";
  return "circle-small";
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
  heroCard: {
    borderRadius: radius.lg,
    borderColor: "#3E668E",
    borderWidth: 1,
    backgroundColor: colors.darkPanel,
    padding: spacing.xs,
    gap: spacing.xs,
    ...shadow.floating,
  },
  heroTop: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.md,
  },
  heroTitleRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.xs,
  },
  heroEyebrow: {
    color: "#B3CDE8",
    fontSize: typography.size.caption,
    textTransform: "uppercase",
    letterSpacing: 0.5,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  heroTopBadge: {
    maxWidth: "52%",
    borderRadius: radius.pill,
    borderWidth: 1,
    borderColor: "#6C5A3A",
    backgroundColor: "#3C2F1B",
    paddingHorizontal: spacing.sm,
    paddingVertical: 5,
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  heroTopBadgeText: {
    color: "#FFE3BC",
    fontSize: typography.size.caption,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  heroInsight: {
    color: "#DAEAF9",
    fontSize: typography.size.bodySm,
    lineHeight: 17,
    fontFamily: typography.body,
  },
  heroPillRow: {
    flexDirection: "row",
    alignItems: "center",
    flexWrap: "wrap",
    gap: 6,
  },
  heroPill: {
    borderRadius: radius.pill,
    borderWidth: 1,
    paddingHorizontal: spacing.sm,
    paddingVertical: 5,
  },
  heroPillAq: {
    borderColor: "#5A6D43",
    backgroundColor: "#223821",
  },
  heroPillHeat: {
    borderColor: "#7D6040",
    backgroundColor: "#3A2E1F",
  },
  heroPillText: {
    color: "#DFECF8",
    fontSize: typography.size.caption,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  sortOptions: {
    flexDirection: "row",
    gap: 4,
    borderRadius: radius.pill,
    borderColor: "#4B7296",
    borderWidth: 1,
    backgroundColor: "#17344D",
    padding: 2,
  },
  sortChip: {
    borderRadius: radius.pill,
    flex: 1,
    borderColor: "transparent",
    borderWidth: 1,
    backgroundColor: "transparent",
    paddingHorizontal: spacing.sm,
    paddingVertical: 6,
    alignItems: "center",
  },
  sortChipActive: {
    borderColor: "#6CBCFF",
    backgroundColor: colors.accentStrong,
  },
  sortChipText: {
    color: "#BCD5EB",
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  sortChipTextActive: {
    color: colors.onAccent,
  },
  mapSection: {
    gap: spacing.md,
  },
  mapSectionWide: {
    flexDirection: "row",
    alignItems: "flex-start",
  },
  mapPanel: {
    flex: 1.45,
    borderRadius: radius.md,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.surface,
    padding: spacing.md,
    gap: spacing.sm,
    ...shadow.card,
  },
  mapHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.sm,
  },
  selectionMetaRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.sm,
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surfaceMuted,
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.xs,
  },
  selectionMetaLabel: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  selectionMetaValue: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  sectionTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.bodyLg,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  tileGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.xs,
  },
  tile: {
    width: "23.2%",
    minWidth: 100,
    borderRadius: radius.sm,
    borderWidth: 1,
    paddingVertical: spacing.sm,
    paddingHorizontal: spacing.xs,
    gap: 4,
  },
  tileName: {
    color: colors.textPrimary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  tileMeta: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  detailPanel: {
    flex: 0.95,
    borderRadius: radius.md,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.surface,
    padding: spacing.md,
    gap: spacing.sm,
    ...shadow.card,
  },
  detailHead: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.sm,
  },
  detailTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.bodyLg,
    fontWeight: "800",
    fontFamily: typography.display,
  },
  detailAqiBadge: {
    borderRadius: radius.pill,
    borderWidth: 1,
    backgroundColor: colors.canvasElevated,
    paddingHorizontal: spacing.sm,
    paddingVertical: 6,
    flexDirection: "row",
    alignItems: "center",
    gap: 7,
  },
  detailAqiDot: {
    width: 8,
    height: 8,
    borderRadius: radius.pill,
  },
  detailAqiText: {
    color: colors.textPrimary,
    fontSize: typography.size.caption,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  detailMetaRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.sm,
  },
  detailMeta: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  detailInsight: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    lineHeight: 18,
    fontFamily: typography.body,
  },
  detailStatsRow: {
    flexDirection: "row",
    gap: spacing.xs,
    flexWrap: "wrap",
  },
  detailStripLabel: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  sectionCard: {
    borderRadius: radius.md,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.surface,
    padding: spacing.md,
    gap: spacing.sm,
    ...shadow.card,
  },
  sectionBody: {
    gap: spacing.xs,
  },
  rankCard: {
    borderRadius: radius.sm,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.surfaceMuted,
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.xs,
    gap: spacing.xs,
  },
  rankHead: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.sm,
  },
  rankNameWrap: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  rankName: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  rankScore: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "800",
    fontFamily: typography.display,
  },
  rankBarTrack: {
    width: "100%",
    height: 5,
    borderRadius: radius.pill,
    backgroundColor: "#244564",
    overflow: "hidden",
  },
  rankBarFill: {
    height: "100%",
    borderRadius: radius.pill,
  },
  rankMetrics: {
    flexDirection: "row",
    gap: spacing.xs,
    flexWrap: "wrap",
  },
  rankMetricPill: {
    borderRadius: radius.pill,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.canvasElevated,
    paddingHorizontal: spacing.sm,
    paddingVertical: 5,
  },
  rankMetricText: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontWeight: "700",
    fontFamily: typography.body,
  },
});
