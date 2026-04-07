import React, { useMemo, useState } from "react";
import { LayoutChangeEvent, ScrollView, StyleSheet, Text, View } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { colors, radius, shadow, spacing, typography } from "../theme/tokens";

type MiniLineChartProps = {
  values: Array<number | null>;
  height?: number;
  lineColor?: string;
  fillColor?: string;
};

type HourlyStripEntry = {
  time: string;
  temperatureC: number | null;
  aqi: number | null;
  weatherCode?: number | null;
  precipitationProbability?: number | null;
};

export function MiniLineChart({
  values,
  height = 86,
  lineColor = colors.accent,
  fillColor = "rgba(59, 169, 255, 0.18)",
}: MiniLineChartProps) {
  const [layoutWidth, setLayoutWidth] = useState<number>(0);
  const width = layoutWidth > 0 ? layoutWidth : 280;
  const normalizedValues = values.slice(0, 36);

  const { points, min, max } = useMemo(() => {
    const numeric = normalizedValues.filter((v): v is number => typeof v === "number" && !Number.isNaN(v));
    const minValue = numeric.length > 0 ? Math.min(...numeric) : 0;
    const maxValue = numeric.length > 0 ? Math.max(...numeric) : 1;
    const range = maxValue - minValue || 1;
    const dotTopPadding = 6;
    const plotHeight = Math.max(12, height - 14);
    const items = normalizedValues.map((value, index) => {
      if (value === null || Number.isNaN(value)) return null;
      const x = normalizedValues.length <= 1 ? width / 2 : (index / (normalizedValues.length - 1)) * width;
      const y = dotTopPadding + ((maxValue - value) / range) * plotHeight;
      return { x, y, value };
    });
    return { points: items, min: minValue, max: maxValue };
  }, [height, normalizedValues, width]);

  const segments = useMemo(() => {
    const rows: Array<{ x: number; y: number; length: number; angle: number }> = [];
    for (let i = 1; i < points.length; i += 1) {
      const prev = points[i - 1];
      const next = points[i];
      if (!prev || !next) continue;
      const dx = next.x - prev.x;
      const dy = next.y - prev.y;
      rows.push({
        x: prev.x,
        y: prev.y,
        length: Math.max(2, Math.sqrt(dx * dx + dy * dy)),
        angle: Math.atan2(dy, dx),
      });
    }
    return rows;
  }, [points]);

  const bars = useMemo(() => {
    const range = max - min || 1;
    const dotTopPadding = 6;
    const plotHeight = Math.max(12, height - 14);
    return points
      .map((point, index) => {
        if (!point) return null;
        const normalized = (point.value - min) / range;
        const barHeight = Math.max(6, normalized * plotHeight);
        return {
          key: index,
          left: point.x - 3,
          height: barHeight,
          top: dotTopPadding + plotHeight - barHeight + 1,
        };
      })
      .filter(Boolean) as Array<{ key: number; left: number; height: number; top: number }>;
  }, [height, max, min, points]);

  return (
    <View style={[styles.lineWrap, { height }]} onLayout={(event) => onChartLayout(event, setLayoutWidth)}>
      <View style={styles.gridLine} />
      <View style={[styles.gridLine, styles.gridLineMid]} />
      <View style={[styles.gridLine, styles.gridLineBottom]} />
      {bars.map((bar) => (
        <View
          key={`bar-${bar.key}`}
          style={[styles.fillBar, { left: bar.left, top: bar.top, height: bar.height, backgroundColor: fillColor }]}
        />
      ))}
      {segments.map((segment, index) => (
        <View
          key={`seg-${index}`}
          style={[
            styles.segment,
            {
              left: segment.x,
              top: segment.y,
              width: segment.length,
              backgroundColor: lineColor,
              transform: [{ rotate: `${segment.angle}rad` }],
            },
          ]}
        />
      ))}
      {points.map((point, index) =>
        point ? (
          <View
            key={`dot-${index}`}
            style={[
              styles.dot,
              {
                left: point.x - 2.8,
                top: point.y - 2.8,
                borderColor: lineColor,
              },
            ]}
          />
        ) : null
      )}
    </View>
  );
}

export function ValueStrip({
  values,
  maxValue,
  colorScale = "aqi",
  height = 56,
}: {
  values: Array<number | null>;
  maxValue?: number;
  colorScale?: "aqi" | "heat";
  height?: number;
}) {
  const numeric = values.filter((value): value is number => value !== null && !Number.isNaN(value));
  const localMax = maxValue ?? (numeric.length > 0 ? Math.max(...numeric) : 1);

  return (
    <View style={[styles.stripWrap, { height }]}>
      {values.slice(0, 48).map((value, index) => {
        const ratio = value === null || Number.isNaN(value) ? 0.1 : Math.min(1, Math.max(0.12, value / (localMax || 1)));
        return (
          <View
            key={`strip-${index}`}
            style={[
              styles.stripBar,
              {
                height: `${Math.round(ratio * 100)}%`,
                backgroundColor: colorScale === "aqi" ? getAqiColor(value) : getHeatColor(value),
              },
            ]}
          />
        );
      })}
    </View>
  );
}

export function HourlyIconStrip({
  entries,
  subtitle,
}: {
  entries: HourlyStripEntry[];
  subtitle?: string;
}) {
  return (
    <View style={styles.hourlyWrap}>
      {subtitle ? <Text style={styles.hourlySubtitle}>{subtitle}</Text> : null}
      <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.hourlyList}>
        {entries.slice(0, 24).map((entry, index) => {
          const icon = weatherIconForConditions(
            entry.weatherCode ?? null,
            entry.temperatureC,
            entry.aqi,
            entry.precipitationProbability ?? null
          );
          return (
            <View key={`${entry.time}-${index}`} style={styles.hourlyItem}>
              <Text style={styles.hourlyTime}>{formatHour(entry.time)}</Text>
              <MaterialCommunityIcons name={icon.name} size={18} color={icon.color} />
              <Text style={styles.hourlyTemp}>{formatTemperature(entry.temperatureC)}</Text>
            </View>
          );
        })}
      </ScrollView>
    </View>
  );
}

export function StatPill({
  icon,
  label,
  value,
  tone = "default",
}: {
  icon: React.ComponentProps<typeof MaterialCommunityIcons>["name"];
  label: string;
  value: string;
  tone?: "default" | "accent" | "warning" | "danger";
}) {
  return (
    <View
      style={[
        styles.statPill,
        tone === "accent" ? styles.statPillAccent : null,
        tone === "warning" ? styles.statPillWarning : null,
        tone === "danger" ? styles.statPillDanger : null,
      ]}
    >
      <MaterialCommunityIcons
        name={icon}
        size={16}
        color={tone === "danger" ? colors.danger : tone === "warning" ? colors.warning : colors.accent}
      />
      <View style={styles.statPillTextWrap}>
        <Text style={styles.statPillLabel}>{label}</Text>
        <Text style={styles.statPillValue}>{value}</Text>
      </View>
    </View>
  );
}

export function TrendPill({ values, label }: { values: Array<number | null>; label: string }) {
  const direction = useMemo(() => {
    const numeric = values.filter((value): value is number => value !== null && !Number.isNaN(value));
    if (numeric.length < 2) return "stable" as const;
    const first = numeric[0];
    const last = numeric[numeric.length - 1];
    if (last - first > 2) return "up" as const;
    if (first - last > 2) return "down" as const;
    return "stable" as const;
  }, [values]);

  const icon = direction === "up" ? "trending-up" : direction === "down" ? "trending-down" : "trending-neutral";
  const color = direction === "up" ? colors.danger : direction === "down" ? colors.aqiGood : colors.info;

  return (
    <View style={styles.trendPill}>
      <MaterialCommunityIcons name={icon} size={15} color={color} />
      <Text style={styles.trendPillText}>{label}</Text>
    </View>
  );
}

export function getAqiColor(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "#456380";
  if (value >= 140) return colors.aqiHazard;
  if (value >= 95) return colors.aqiRisky;
  if (value >= 60) return colors.aqiModerate;
  return colors.aqiGood;
}

export type WeatherMood = "rain" | "sunny" | "cloudy" | "storm" | "snow" | "neutral";

export function weatherMoodForConditions(
  weatherCode: number | null,
  temperatureC: number | null,
  precipitationProbability: number | null
): WeatherMood {
  if (weatherCode !== null && !Number.isNaN(weatherCode)) {
    if (weatherCode >= 95 && weatherCode <= 99) return "storm";
    if ((weatherCode >= 71 && weatherCode <= 77) || weatherCode === 85 || weatherCode === 86) return "snow";
    if (
      (weatherCode >= 51 && weatherCode <= 67) ||
      (weatherCode >= 80 && weatherCode <= 82)
    )
      return "rain";
    if ((weatherCode >= 1 && weatherCode <= 3) || weatherCode === 45 || weatherCode === 48) return "cloudy";
    if (weatherCode === 0) return "sunny";
  }

  if (precipitationProbability !== null && !Number.isNaN(precipitationProbability)) {
    if (precipitationProbability >= 80) return "rain";
    if (precipitationProbability >= 45) return "cloudy";
    if (precipitationProbability <= 20 && temperatureC !== null && !Number.isNaN(temperatureC) && temperatureC >= 27) {
      return "sunny";
    }
  }

  return "neutral";
}

export function weatherIconForConditions(
  weatherCode: number | null,
  temperatureC: number | null,
  aqi: number | null,
  precipitationProbability: number | null = null
): { name: React.ComponentProps<typeof MaterialCommunityIcons>["name"]; color: string } {
  const mood = weatherMoodForConditions(weatherCode, temperatureC, precipitationProbability);
  if (aqi !== null && aqi >= 130) return { name: "weather-windy-variant", color: colors.aqiHazard };
  if (mood === "storm") return { name: "weather-lightning-rainy", color: colors.warning };
  if (mood === "rain") return { name: "weather-pouring", color: colors.info };
  if (mood === "snow") return { name: "snowflake", color: colors.heatCool };
  if (mood === "sunny") return { name: "white-balance-sunny", color: colors.heatWarm };
  if (mood === "neutral") return { name: "weather-cloudy-clock", color: colors.textSecondary };
  if (temperatureC === null || Number.isNaN(temperatureC)) return { name: "weather-cloudy-clock", color: colors.textSecondary };
  return { name: "weather-partly-cloudy", color: colors.info };
}

function getHeatColor(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "#365675";
  if (value >= 32) return colors.heatHot;
  if (value >= 23) return colors.heatWarm;
  return colors.heatCool;
}

function formatHour(time: string): string {
  const d = new Date(time);
  if (Number.isNaN(d.getTime())) return time.slice(11, 16);
  return `${String(d.getHours()).padStart(2, "0")}:00`;
}

function formatTemperature(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value.toFixed(0)}°`;
}

function onChartLayout(event: LayoutChangeEvent, setWidth: (value: number) => void): void {
  const nextWidth = Math.max(120, event.nativeEvent.layout.width - 2);
  setWidth(nextWidth);
}

const styles = StyleSheet.create({
  lineWrap: {
    borderRadius: radius.md,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surfaceMuted,
    overflow: "hidden",
    position: "relative",
    ...shadow.card,
  },
  gridLine: {
    position: "absolute",
    left: 0,
    right: 0,
    top: 8,
    height: 1,
    backgroundColor: "#35597E",
  },
  gridLineMid: {
    top: "50%",
  },
  gridLineBottom: {
    top: undefined,
    bottom: 8,
  },
  fillBar: {
    position: "absolute",
    width: 6,
    borderTopLeftRadius: 4,
    borderTopRightRadius: 4,
    opacity: 0.85,
  },
  segment: {
    position: "absolute",
    height: 2.2,
    borderRadius: radius.pill,
  },
  dot: {
    position: "absolute",
    width: 5.6,
    height: 5.6,
    borderRadius: radius.pill,
    backgroundColor: colors.canvas,
    borderWidth: 1.5,
  },
  stripWrap: {
    flexDirection: "row",
    alignItems: "flex-end",
    gap: 3,
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: colors.border,
    paddingHorizontal: spacing.xs,
    paddingTop: spacing.xs,
    backgroundColor: colors.surfaceMuted,
    ...shadow.card,
  },
  stripBar: {
    flex: 1,
    borderTopLeftRadius: 3,
    borderTopRightRadius: 3,
    opacity: 0.88,
    minHeight: 7,
  },
  hourlyWrap: {
    gap: spacing.xs,
  },
  hourlySubtitle: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  hourlyList: {
    gap: spacing.xs,
  },
  hourlyItem: {
    width: 60,
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surfaceMuted,
    alignItems: "center",
    paddingVertical: spacing.xs,
    gap: 2,
  },
  hourlyTime: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    fontFamily: typography.mono,
  },
  hourlyTemp: {
    color: colors.textPrimary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  statPill: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.xs,
    borderRadius: radius.sm,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surfaceMuted,
    paddingVertical: spacing.xs,
    paddingHorizontal: spacing.sm,
    minWidth: 130,
  },
  statPillAccent: {
    backgroundColor: "#184264",
    borderColor: "#2F6C98",
  },
  statPillWarning: {
    backgroundColor: "#43331E",
    borderColor: "#8A6A3A",
  },
  statPillDanger: {
    backgroundColor: "#45272B",
    borderColor: "#8F4B52",
  },
  statPillTextWrap: {
    gap: 1,
  },
  statPillLabel: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    fontFamily: typography.body,
  },
  statPillValue: {
    color: colors.textPrimary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  trendPill: {
    alignSelf: "flex-start",
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    borderRadius: radius.pill,
    borderColor: colors.borderStrong,
    borderWidth: 1,
    backgroundColor: colors.surfaceMuted,
    paddingVertical: 6,
    paddingHorizontal: spacing.sm,
  },
  trendPillText: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontWeight: "700",
    fontFamily: typography.body,
  },
});
