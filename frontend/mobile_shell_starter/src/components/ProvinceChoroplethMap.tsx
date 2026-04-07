import React, { useMemo, useState } from "react";
import { LayoutChangeEvent, Platform, StyleSheet, Text, View } from "react-native";
import Svg, { Path } from "react-native-svg";

import { colors, radius, shadow, spacing, typography } from "../theme/tokens";

type SortMode = "priority" | "aqi" | "temp";

type ProvinceDatum = {
  provinceName: string;
  shapeIso: string | null;
  updatedAt: string;
  avgAqi: number | null;
  avgTempC: number | null;
  priorityScore: number | null;
};

type MapFeature = {
  province_name: string;
  shape_iso: string;
  path: string;
  centroid: [number, number];
};

type MapAsset = {
  view_box: {
    width: number;
    height: number;
  };
  features: MapFeature[];
};

const MAP_ASSET = require("../assets/tr_adm1_map_lite.json") as MapAsset;
const DEFAULT_WIDTH = 860;
const DEFAULT_HEIGHT = 500;

export function ProvinceChoroplethMap({
  rows,
  sortMode,
  locale,
  selectedProvince,
  onSelectProvince,
  t,
}: {
  rows: ProvinceDatum[];
  sortMode: SortMode;
  locale: string;
  selectedProvince: string | null;
  onSelectProvince: (provinceName: string) => void;
  t: (key: string, fallback?: string) => string;
}) {
  const [layoutWidth, setLayoutWidth] = useState<number>(0);
  const [hoverProvince, setHoverProvince] = useState<string | null>(null);
  const viewWidth = MAP_ASSET.view_box?.width ?? DEFAULT_WIDTH;
  const viewHeight = MAP_ASSET.view_box?.height ?? DEFAULT_HEIGHT;
  const canvasWidth = Math.max(260, layoutWidth || 0);
  const canvasHeight = canvasWidth > 0 ? (canvasWidth / viewWidth) * viewHeight : viewHeight;

  const valueByProvince = useMemo(() => {
    const map = new Map<string, number | null>();
    rows.forEach((row) => {
      map.set(normalizeToken(row.provinceName), metricValue(row, sortMode));
      if (row.shapeIso) {
        map.set(normalizeToken(row.shapeIso), metricValue(row, sortMode));
      }
    });
    return map;
  }, [rows, sortMode]);

  const valueStats = useMemo(() => {
    const values = Array.from(valueByProvince.values()).filter((v): v is number => v !== null && !Number.isNaN(v));
    if (values.length === 0) return { min: 0, max: 1 };
    return { min: Math.min(...values), max: Math.max(...values) };
  }, [valueByProvince]);

  const selectedToken = normalizeToken(selectedProvince ?? "");
  const hoverToken = normalizeToken(hoverProvince ?? "");
  const mapCoverage = useMemo(() => {
    let matched = 0;
    let withValue = 0;
    for (const feature of MAP_ASSET.features) {
      const provinceToken = normalizeToken(feature.province_name);
      const isoToken = normalizeToken(feature.shape_iso);
      const value = valueByProvince.get(provinceToken) ?? valueByProvince.get(isoToken) ?? null;
      const hasKey = valueByProvince.has(provinceToken) || valueByProvince.has(isoToken);
      if (hasKey) matched += 1;
      if (value !== null && !Number.isNaN(value)) withValue += 1;
    }
    return {
      matched,
      withValue,
      total: MAP_ASSET.features.length,
    };
  }, [valueByProvince]);

  const legendLabel =
    sortMode === "aqi"
      ? t("ui.province.map.sort_aqi", "AQI")
      : sortMode === "temp"
      ? t("ui.province.map.sort_temp", "Temperature")
      : t("ui.province.map.sort_priority", "Priority");
  const focusedToken = hoverToken || selectedToken;
  const focusedRow = useMemo(() => {
    if (!focusedToken) return null;
    return (
      rows.find((row) => normalizeToken(row.provinceName) === focusedToken || normalizeToken(row.shapeIso) === focusedToken) ??
      null
    );
  }, [focusedToken, rows]);
  const hoverRow = useMemo(() => {
    if (!hoverToken) return null;
    return (
      rows.find((row) => normalizeToken(row.provinceName) === hoverToken || normalizeToken(row.shapeIso) === hoverToken) ??
      null
    );
  }, [hoverToken, rows]);
  const metricSummary = useMemo(() => {
    const numeric = rows
      .map((row) => metricValue(row, sortMode))
      .filter((value): value is number => value !== null && !Number.isNaN(value))
      .sort((a, b) => a - b);
    if (numeric.length === 0) {
      return { median: null, min: null, max: null };
    }
    const middle = Math.floor(numeric.length / 2);
    const median =
      numeric.length % 2 === 0 ? (numeric[middle - 1] + numeric[middle]) / 2 : numeric[middle];
    return {
      median,
      min: numeric[0],
      max: numeric[numeric.length - 1],
    };
  }, [rows, sortMode]);
  const hoverTrend = hoverRow ? resolveTrend(metricValue(hoverRow, sortMode), metricSummary, sortMode) : "stable";

  return (
    <View style={styles.wrap}>
      <View style={styles.headerRow}>
        <Text style={styles.title}>{t("ui.province.map.choropleth_title", "Turkey Province Map")}</Text>
        <Text style={styles.subtitle}>{`${legendLabel} · ${mapCoverage.withValue}/${mapCoverage.total}`}</Text>
      </View>

      <View style={styles.mapFrame} onLayout={(event) => handleLayout(event, setLayoutWidth)}>
        {canvasWidth > 0 ? (
          <Svg width={canvasWidth} height={canvasHeight} viewBox={`0 0 ${viewWidth} ${viewHeight}`}>
            {MAP_ASSET.features.map((feature) => {
              const provinceToken = normalizeToken(feature.province_name);
              const isoToken = normalizeToken(feature.shape_iso);
              const value = valueByProvince.get(provinceToken) ?? valueByProvince.get(isoToken) ?? null;
              const isFocused = focusedToken !== "" && focusedToken === provinceToken;
              const isSelected = selectedToken !== "" && selectedToken === provinceToken;
              const fill = colorForValue(value, sortMode, valueStats.min, valueStats.max);
              const webHoverHandlers =
                Platform.OS === "web"
                  ? ({
                      onMouseEnter: () => setHoverProvince(feature.province_name),
                      onMouseLeave: () => setHoverProvince(null),
                    } as const)
                  : {};
              return (
                <Path
                  key={`${feature.shape_iso}-${feature.province_name}`}
                  d={feature.path}
                  fill={fill}
                  stroke={isFocused ? "#D7EDFF" : "#2A4561"}
                  strokeWidth={isSelected ? 2.4 : isFocused ? 1.8 : 1}
                  onPress={() => {
                    onSelectProvince(feature.province_name);
                    setHoverProvince(feature.province_name);
                  }}
                  onPressIn={() => setHoverProvince(feature.province_name)}
                  onPressOut={() => {
                    if (Platform.OS !== "web") {
                      setHoverProvince(null);
                    }
                  }}
                  {...(webHoverHandlers as Record<string, unknown>)}
                />
              );
            })}
          </Svg>
        ) : null}

        {Platform.OS === "web" && hoverRow ? (
          <View pointerEvents="none" style={styles.webTooltip}>
            <Text numberOfLines={1} style={styles.webTooltipTitle}>
              {hoverRow.provinceName}
            </Text>
            <Text style={styles.webTooltipValue}>
              {`${legendLabel}: ${formatMetricValue(metricValue(hoverRow, sortMode), sortMode, locale)}`}
            </Text>
            <Text style={styles.webTooltipMedian}>
              {`${t("ui.province.map.median_ref", "Median")}: ${formatMetricValue(metricSummary.median, sortMode, locale)}`}
            </Text>
            <Text style={styles.webTooltipFreshness}>
              {`${t("ui.common.updated_at", "Updated At")}: ${formatDateTime(hoverRow.updatedAt, locale)}`}
            </Text>
            <View style={styles.webTooltipTrendRow}>
              <Text
                style={[
                  styles.webTooltipTrendArrow,
                  { color: trendToneColor(hoverTrend) },
                ]}
              >
                {trendGlyph(hoverTrend)}
              </Text>
              <Text style={styles.webTooltipTrendText}>
                {trendLabel(hoverTrend, t)}
              </Text>
            </View>
          </View>
        ) : null}
      </View>

      <View style={styles.focusCard}>
        <View style={styles.focusHead}>
          <Text style={styles.focusLabel}>{t("ui.province.map.focus", "Map Focus")}</Text>
          <Text style={styles.focusMetricLabel}>{legendLabel}</Text>
        </View>
        <View style={styles.focusBody}>
          <Text numberOfLines={1} style={styles.focusProvince}>
            {focusedRow?.provinceName ?? t("ui.common.empty", "No data")}
          </Text>
          <Text style={styles.focusValue}>
            {formatMetricValue(
              focusedRow ? metricValue(focusedRow, sortMode) : null,
              sortMode,
              locale
            )}
          </Text>
        </View>
      </View>

      <View style={styles.legendCard}>
        <View style={styles.legendHeader}>
          <Text style={styles.legendTitle}>{t("ui.province.map.legend", "Legend")}</Text>
          <Text style={styles.legendMeta}>{legendLabel}</Text>
        </View>
        <View style={styles.legendScale}>
          {Array.from({ length: 7 }).map((_, idx) => {
            const ratio = idx / 6;
            return (
              <View
                key={`legend-${idx}`}
                style={[
                  styles.legendScaleBar,
                  { backgroundColor: colorByRatio(sortMode, ratio) },
                ]}
              />
            );
          })}
        </View>
        <View style={styles.legendLabels}>
          <Text style={styles.legendLabel}>{t("ui.province.map.legend_low", "Low")}</Text>
          <Text style={styles.legendLabel}>{t("ui.province.map.legend_high", "High")}</Text>
        </View>
        <Text style={styles.legendCoverage}>
          {`${t("ui.province.map.coverage", "Coverage")}: ${mapCoverage.matched}/${mapCoverage.total}`}
        </Text>
        <Text style={styles.legendHint}>
          {t("ui.province.map.choropleth_hint", "Tap a province to inspect details and compare risk.")}
        </Text>
      </View>
    </View>
  );
}

function metricValue(row: ProvinceDatum, sortMode: SortMode): number | null {
  if (sortMode === "aqi") return row.avgAqi;
  if (sortMode === "temp") return row.avgTempC;
  return row.priorityScore;
}

function formatMetricValue(value: number | null, sortMode: SortMode, locale: string): string {
  if (value === null || Number.isNaN(value)) return "—";
  if (sortMode === "temp") return `${value.toLocaleString(locale, { maximumFractionDigits: 1 })}°C`;
  if (sortMode === "aqi") return `${Math.round(value).toLocaleString(locale)}`;
  return value.toLocaleString(locale, { maximumFractionDigits: 1 });
}

function formatDateTime(value: string, locale: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value || "—";
  return `${parsed.toLocaleDateString(locale)} ${parsed.toLocaleTimeString(locale, { hour: "2-digit", minute: "2-digit" })}`;
}

function resolveTrend(
  value: number | null,
  summary: { median: number | null; min: number | null; max: number | null },
  sortMode: SortMode
): "up" | "down" | "stable" {
  if (value === null || Number.isNaN(value) || summary.median === null) return "stable";
  const minValue = summary.min ?? value;
  const maxValue = summary.max ?? value;
  const span = Math.max(1, maxValue - minValue);
  const baseThreshold = span * 0.08;
  const modeFloor = sortMode === "aqi" ? 5 : sortMode === "temp" ? 1 : 0.8;
  const threshold = Math.max(baseThreshold, modeFloor);
  if (value > summary.median + threshold) return "up";
  if (value < summary.median - threshold) return "down";
  return "stable";
}

function trendGlyph(trend: "up" | "down" | "stable"): string {
  if (trend === "up") return "▲";
  if (trend === "down") return "▼";
  return "●";
}

function trendToneColor(trend: "up" | "down" | "stable"): string {
  if (trend === "up") return colors.danger;
  if (trend === "down") return colors.aqiGood;
  return colors.info;
}

function trendLabel(trend: "up" | "down" | "stable", t: (key: string, fallback?: string) => string): string {
  if (trend === "up") return t("ui.province.map.trend_up", "Above baseline");
  if (trend === "down") return t("ui.province.map.trend_down", "Below baseline");
  return t("ui.province.map.trend_stable", "Near baseline");
}

function normalizeToken(value: string | null | undefined): string {
  return (value ?? "").trim().toLowerCase();
}

function colorForValue(value: number | null, sortMode: SortMode, minValue: number, maxValue: number): string {
  if (value === null || Number.isNaN(value)) return "#314D67";
  const span = maxValue - minValue || 1;
  const ratio = Math.min(1, Math.max(0, (value - minValue) / span));
  return colorByRatio(sortMode, ratio);
}

function colorByRatio(sortMode: SortMode, ratio: number): string {
  const clamped = Math.min(1, Math.max(0, ratio));
  if (sortMode === "temp") return interpolateRgb("#365675", "#E58D3D", clamped);
  if (sortMode === "aqi") return interpolateRgb("#2B6C8B", "#D5535E", clamped);
  return interpolateRgb("#2F5C80", "#8A65E8", clamped);
}

function interpolateRgb(startHex: string, endHex: string, ratio: number): string {
  const s = hexToRgb(startHex);
  const e = hexToRgb(endHex);
  const r = Math.round(s.r + (e.r - s.r) * ratio);
  const g = Math.round(s.g + (e.g - s.g) * ratio);
  const b = Math.round(s.b + (e.b - s.b) * ratio);
  return `rgb(${r}, ${g}, ${b})`;
}

function hexToRgb(hex: string): { r: number; g: number; b: number } {
  const cleaned = hex.replace("#", "");
  const normalized = cleaned.length === 3 ? cleaned.split("").map((c) => c + c).join("") : cleaned;
  const value = Number.parseInt(normalized, 16);
  return {
    r: (value >> 16) & 255,
    g: (value >> 8) & 255,
    b: value & 255,
  };
}

function handleLayout(
  event: LayoutChangeEvent,
  setLayoutWidth: React.Dispatch<React.SetStateAction<number>>
): void {
  const next = Math.max(260, Math.floor(event.nativeEvent.layout.width));
  setLayoutWidth((prev) => (prev === next ? prev : next));
}

const styles = StyleSheet.create({
  wrap: {
    gap: spacing.sm,
  },
  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: spacing.md,
  },
  title: {
    color: colors.textPrimary,
    fontSize: typography.size.bodyLg,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  subtitle: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  mapFrame: {
    position: "relative",
    borderRadius: radius.md,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: "#13293C",
    overflow: "hidden",
    minHeight: 180,
    ...shadow.card,
  },
  webTooltip: {
    position: "absolute",
    top: spacing.sm,
    right: spacing.sm,
    maxWidth: 196,
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: "#4E7598",
    backgroundColor: "rgba(17, 36, 53, 0.74)",
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.xs,
    gap: 1,
  },
  webTooltipTitle: {
    color: colors.darkTextOnPanel,
    fontSize: typography.size.bodySm,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  webTooltipValue: {
    color: "#D6E7F7",
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  webTooltipMedian: {
    color: "#B7CFE4",
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  webTooltipFreshness: {
    color: "#A8C4DB",
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  webTooltipTrendRow: {
    marginTop: 2,
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  webTooltipTrendArrow: {
    fontSize: typography.size.caption,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  webTooltipTrendText: {
    color: "#C4DBEE",
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  focusCard: {
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.xs,
    gap: 4,
  },
  focusHead: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.sm,
  },
  focusLabel: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
    fontWeight: "700",
  },
  focusMetricLabel: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  focusBody: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.sm,
  },
  focusProvince: {
    flex: 1,
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontFamily: typography.heading,
    fontWeight: "700",
  },
  focusValue: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontFamily: typography.heading,
    fontWeight: "800",
  },
  legendCard: {
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surfaceMuted,
    padding: spacing.sm,
    gap: spacing.xs,
  },
  legendHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  legendTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  legendMeta: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  legendScale: {
    flexDirection: "row",
    gap: 2,
    alignItems: "stretch",
    height: 10,
  },
  legendScaleBar: {
    flex: 1,
    borderRadius: 2,
  },
  legendLabels: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  legendLabel: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  legendHint: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
    lineHeight: 16,
  },
  legendCoverage: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
    fontWeight: "700",
  },
});
