import React from "react";
import { ActivityIndicator, StyleSheet, Text, View } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { colors, radius, shadow, spacing, typography } from "../theme/tokens";

export function LoadingBlock({ label, detail }: { label: string; detail?: string }) {
  return (
    <View style={styles.wrap}>
      <View style={styles.indicatorWrap}>
        <ActivityIndicator size="small" color={colors.accent} />
      </View>
      <MaterialCommunityIcons name="cloud-sync-outline" size={18} color={colors.info} />
      <Text style={styles.label}>{label}</Text>
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
    gap: spacing.sm,
    ...shadow.card,
  },
  indicatorWrap: {
    width: 34,
    height: 34,
    borderRadius: radius.pill,
    backgroundColor: "#214B6D",
    alignItems: "center",
    justifyContent: "center",
  },
  label: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  detail: {
    color: colors.textTertiary,
    fontSize: typography.size.bodySm,
    textAlign: "center",
    lineHeight: 18,
    fontFamily: typography.body,
  },
});
