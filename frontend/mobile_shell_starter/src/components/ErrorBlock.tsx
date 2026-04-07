import React from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { colors, radius, shadow, spacing, typography } from "../theme/tokens";

export function ErrorBlock({
  title,
  detail,
  retryLabel,
  onRetry,
  badgeLabel = "Connection",
}: {
  title: string;
  detail?: string | null;
  retryLabel: string;
  onRetry?: () => void;
  badgeLabel?: string;
}) {
  return (
    <View style={styles.wrap}>
      <View style={styles.badge}>
        <Text style={styles.badgeText}>{badgeLabel}</Text>
      </View>
      <MaterialCommunityIcons name="alert-octagon-outline" size={18} color={colors.danger} />
      <Text style={styles.title}>{title}</Text>
      {detail ? <Text style={styles.detail}>{detail}</Text> : null}
      {onRetry ? (
        <Pressable onPress={onRetry} style={styles.retryButton}>
          <Text style={styles.retryText}>{retryLabel}</Text>
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    backgroundColor: colors.dangerSoft,
    borderColor: "#95525C",
    borderWidth: 1,
    borderRadius: radius.md,
    padding: spacing.xl,
    gap: spacing.sm,
    ...shadow.card,
  },
  badge: {
    alignSelf: "flex-start",
    borderRadius: radius.pill,
    borderColor: "#A25D66",
    borderWidth: 1,
    paddingVertical: spacing.xxs,
    paddingHorizontal: spacing.sm,
    backgroundColor: "#5A2B32",
  },
  badgeText: {
    color: "#F0CDD3",
    fontSize: typography.size.caption,
    fontWeight: "700",
    letterSpacing: 0.35,
    textTransform: "uppercase",
    fontFamily: typography.body,
  },
  title: {
    color: colors.danger,
    fontSize: typography.size.bodyLg,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  detail: {
    color: "#E8BEC3",
    fontSize: typography.size.bodySm,
    lineHeight: 18,
    fontFamily: typography.body,
  },
  retryButton: {
    alignSelf: "flex-start",
    borderRadius: radius.pill,
    borderWidth: 1,
    borderColor: "#E98D97",
    backgroundColor: "#B33E4B",
    paddingHorizontal: spacing.lg,
    paddingVertical: spacing.xs,
  },
  retryText: {
    color: "#FFFFFF",
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
});
