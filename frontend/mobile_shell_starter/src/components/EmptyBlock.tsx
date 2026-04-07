import React from "react";
import { StyleSheet, Text, View } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { colors, radius, shadow, spacing, typography } from "../theme/tokens";

export function EmptyBlock({
  title,
  detail,
  badgeLabel = "No Data",
}: {
  title: string;
  detail?: string;
  badgeLabel?: string;
}) {
  return (
    <View style={styles.wrap}>
      <View style={styles.badge}>
        <Text style={styles.badgeText}>{badgeLabel}</Text>
      </View>
      <MaterialCommunityIcons name="weather-cloudy-alert" size={20} color={colors.textSecondary} />
      <Text style={styles.title}>{title}</Text>
      {detail ? <Text style={styles.detail}>{detail}</Text> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    backgroundColor: colors.surface,
    borderRadius: radius.md,
    borderColor: colors.border,
    borderWidth: 1,
    padding: spacing.xl,
    alignItems: "center",
    gap: spacing.xs,
    ...shadow.card,
  },
  badge: {
    backgroundColor: colors.surfaceMuted,
    borderColor: colors.border,
    borderWidth: 1,
    borderRadius: radius.pill,
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.xxs,
  },
  badgeText: {
    color: colors.textSecondary,
    fontSize: typography.size.caption,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
    fontFamily: typography.body,
  },
  title: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    textAlign: "center",
    fontFamily: typography.heading,
  },
  detail: {
    color: colors.textTertiary,
    fontSize: typography.size.bodySm,
    textAlign: "center",
    fontFamily: typography.body,
  },
});
