import React from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import type { ScreenId } from "../navigation/screenMap";
import { SCREEN_MAP } from "../navigation/screenMap";
import { colors, radius, shadow, spacing, typography } from "../theme/tokens";

export function TabBar({
  activeScreen,
  onChange,
  t,
}: {
  activeScreen: ScreenId;
  onChange: (next: ScreenId) => void;
  t: (key: string, fallback?: string) => string;
}) {
  return (
    <View style={styles.wrap}>
      {SCREEN_MAP.map((screen) => {
        const active = screen.id === activeScreen;
        return (
          <Pressable
            key={screen.id}
            style={[styles.tab, active ? styles.tabActive : null]}
            onPress={() => onChange(screen.id)}
          >
            <MaterialCommunityIcons
              name={iconName(screen.id)}
              size={17}
              color={active ? colors.onAccent : "#95B2D1"}
              style={styles.icon}
            />
            <Text numberOfLines={1} style={[styles.text, active ? styles.textActive : null]}>
              {t(screen.titleKey, screen.id)}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}

function iconName(screenId: ScreenId): React.ComponentProps<typeof MaterialCommunityIcons>["name"] {
  if (screenId === "city_current_overview") return "weather-partly-cloudy";
  if (screenId === "city_hourly_timeline") return "chart-line-variant";
  if (screenId === "province_map_metrics") return "map-marker-radius-outline";
  return "cog-outline";
}

const styles = StyleSheet.create({
  wrap: {
    flexDirection: "row",
    gap: spacing.xs,
    marginHorizontal: spacing.sm,
    marginBottom: spacing.md,
    marginTop: spacing.xs,
    padding: spacing.xs,
    borderColor: "#31587E",
    borderWidth: 1,
    borderRadius: radius.lg,
    backgroundColor: "#0F253A",
    ...shadow.floating,
  },
  tab: {
    flex: 1,
    borderRadius: radius.sm,
    paddingVertical: spacing.sm,
    paddingHorizontal: spacing.xs,
    alignItems: "center",
    justifyContent: "center",
    gap: 2,
  },
  tabActive: {
    borderColor: "#66BAFF",
    borderWidth: 1,
    backgroundColor: colors.accentStrong,
    ...shadow.glow,
  },
  icon: {
    marginBottom: 2,
  },
  text: {
    color: "#9BB9D6",
    fontSize: 11.5,
    fontWeight: "600",
    fontFamily: typography.body,
  },
  textActive: {
    color: colors.onAccent,
  },
});
