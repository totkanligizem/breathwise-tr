import React from "react";
import { StyleSheet, Text, View } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { colors, radius, shadow, spacing, typography } from "../theme/tokens";

export function MetricCard({
  title,
  value,
  subtitle,
  tone = "default",
  compact = false,
  icon,
}: {
  title: string;
  value: string;
  subtitle?: string | null;
  tone?: "default" | "accent" | "warning" | "info";
  compact?: boolean;
  icon?: React.ComponentProps<typeof MaterialCommunityIcons>["name"];
}) {
  const toneStyle = tone === "accent" ? styles.accent : tone === "warning" ? styles.warning : tone === "info" ? styles.info : null;

  return (
    <View style={[styles.wrap, toneStyle, compact ? styles.compactWrap : null]}>
      <View style={[styles.topLine, tone === "warning" ? styles.topLineWarning : tone === "info" ? styles.topLineInfo : styles.topLineAccent]} />
      <View style={styles.titleRow}>
        {icon ? (
          <MaterialCommunityIcons
            name={icon}
            size={15}
            color={tone === "warning" ? colors.warning : tone === "info" ? colors.info : colors.accent}
          />
        ) : null}
        <Text style={styles.title}>{title}</Text>
      </View>
      <Text style={[styles.value, compact ? styles.compactValue : null]}>{value}</Text>
      {subtitle ? <Text style={styles.subtitle}>{subtitle}</Text> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    flex: 1,
    minWidth: 140,
    borderRadius: radius.md,
    backgroundColor: colors.surface,
    borderColor: colors.border,
    borderWidth: 1,
    padding: spacing.md,
    gap: spacing.xs,
    ...shadow.card,
  },
  compactWrap: {
    minWidth: 112,
    paddingVertical: spacing.sm,
  },
  accent: {
    backgroundColor: "#164160",
    borderColor: "#357AAC",
  },
  warning: {
    backgroundColor: "#463520",
    borderColor: "#8F6E3C",
  },
  info: {
    backgroundColor: "#173A56",
    borderColor: "#3C77A3",
  },
  topLine: {
    height: 3.5,
    borderRadius: radius.pill,
    marginBottom: 2,
  },
  topLineAccent: {
    backgroundColor: colors.accent,
  },
  topLineWarning: {
    backgroundColor: colors.warning,
  },
  topLineInfo: {
    backgroundColor: colors.info,
  },
  titleRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.xs,
  },
  title: {
    color: colors.textTertiary,
    fontSize: typography.size.caption,
    letterSpacing: 0.5,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  value: {
    color: colors.textPrimary,
    fontSize: 24,
    fontWeight: "800",
    fontFamily: typography.display,
    lineHeight: 30,
  },
  compactValue: {
    fontSize: 21,
    lineHeight: 25,
  },
  subtitle: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontWeight: "500",
    fontFamily: typography.body,
  },
});
