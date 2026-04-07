import React, { useMemo, useState } from "react";
import { Platform, SafeAreaView, StatusBar, StyleSheet, Text, View, useWindowDimensions } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { TabBar } from "./src/components/TabBar";
import { SCREEN_MAP, ScreenId } from "./src/navigation/screenMap";
import { CityCurrentScreen } from "./src/screens/CityCurrentScreen";
import { CityTimelineScreen } from "./src/screens/CityTimelineScreen";
import { ProvinceMapScreen } from "./src/screens/ProvinceMapScreen";
import { SettingsScreen } from "./src/screens/SettingsScreen";
import { LocaleProvider, useLocaleContext } from "./src/state/LocaleContext";
import { colors, layout, radius, shadow, spacing, typography } from "./src/theme/tokens";

export default function App() {
  return (
    <LocaleProvider>
      <RootShell />
    </LocaleProvider>
  );
}

function RootShell() {
  const { locale, localizationMeta, t, apiClient } = useLocaleContext();
  const [screenId, setScreenId] = useState<ScreenId>("city_current_overview");
  const { width } = useWindowDimensions();
  const targetMax = width >= 1080 ? 840 : layout.maxContentWidth;
  const contentWidth = Math.min(targetMax, Math.max(340, width - spacing.md * 2));
  const showSignal = screenId !== "settings_locale";

  const { screenTitle, screenSubtitle } = useMemo(() => {
    const found = SCREEN_MAP.find((item) => item.id === screenId);
    return {
      screenTitle: t(found?.titleKey ?? "ui.nav.city_overview", "Breathwise"),
      screenSubtitle: t(found?.subtitleKey ?? "ui.nav.city_overview.subtitle", "Environmental intelligence"),
    };
  }, [screenId, t]);

  return (
    <SafeAreaView style={styles.safe}>
      <StatusBar barStyle="light-content" />
      <View style={styles.decorA} />
      <View style={styles.decorB} />
      <View style={[styles.shell, Platform.OS === "web" ? styles.shellWeb : null, { width: contentWidth }]}>
        <View style={styles.header}>
          <View style={styles.headerTopRow}>
            <View style={styles.brandWrap}>
              <MaterialCommunityIcons name="weather-partly-lightning" size={18} color="#8BCBFF" />
              <Text style={styles.brand}>Breathwise TR</Text>
            </View>
          </View>
          <Text style={styles.screenTitle}>{screenTitle}</Text>
          <Text style={styles.screenSubtitle}>{screenSubtitle}</Text>
          {showSignal ? (
            <View style={styles.headerSignalRow}>
              <View style={styles.signalDot} />
              <Text style={styles.headerSignalText}>{t("ui.app.signal", "Live environmental intelligence")}</Text>
            </View>
          ) : null}
        </View>

        <View style={styles.body}>
          {screenId === "city_current_overview" ? (
            <CityCurrentScreen locale={locale} t={t} client={apiClient} localizationMeta={localizationMeta} />
          ) : null}
          {screenId === "city_hourly_timeline" ? (
            <CityTimelineScreen locale={locale} t={t} client={apiClient} localizationMeta={localizationMeta} />
          ) : null}
          {screenId === "province_map_metrics" ? (
            <ProvinceMapScreen locale={locale} t={t} client={apiClient} localizationMeta={localizationMeta} />
          ) : null}
          {screenId === "settings_locale" ? (
            <SettingsScreen locale={locale} t={t} client={apiClient} localizationMeta={localizationMeta} />
          ) : null}
        </View>

        <TabBar activeScreen={screenId} onChange={setScreenId} t={t} />
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: {
    flex: 1,
    backgroundColor: colors.canvas,
    position: "relative",
  },
  decorA: {
    position: "absolute",
    width: 300,
    height: 300,
    borderRadius: radius.pill,
    backgroundColor: "#12395A",
    top: -180,
    right: -90,
    opacity: 0.55,
  },
  decorB: {
    position: "absolute",
    width: 240,
    height: 240,
    borderRadius: radius.pill,
    backgroundColor: "#234D73",
    top: 78,
    left: -140,
    opacity: 0.48,
  },
  shell: {
    flex: 1,
    alignSelf: "center",
    gap: spacing.sm,
  },
  shellWeb: {
    paddingTop: spacing.sm,
  },
  header: {
    marginHorizontal: spacing.sm,
    marginTop: spacing.xs,
    paddingHorizontal: spacing.lg,
    paddingTop: spacing.xs,
    paddingBottom: spacing.xs,
    gap: 6,
    borderRadius: radius.lg,
    borderColor: "#3E668E",
    borderWidth: 1,
    backgroundColor: colors.darkPanelSoft,
    ...shadow.floating,
  },
  headerTopRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  brandWrap: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.xs,
  },
  brand: {
    color: "#B9D8F4",
    fontSize: typography.size.caption,
    fontWeight: "800",
    letterSpacing: 1.1,
    textTransform: "uppercase",
    fontFamily: typography.heading,
  },
  screenTitle: {
    color: colors.darkTextOnPanel,
    fontSize: 21,
    fontWeight: "800",
    fontFamily: typography.display,
  },
  screenSubtitle: {
    color: "#BDD5EC",
    fontSize: typography.size.bodySm,
    fontWeight: "500",
    fontFamily: typography.body,
  },
  headerSignalRow: {
    alignSelf: "flex-start",
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    marginTop: 1,
  },
  signalDot: {
    width: 6,
    height: 6,
    borderRadius: radius.pill,
    backgroundColor: colors.aqiGood,
  },
  headerSignalText: {
    color: "#B7D3EA",
    fontSize: 11,
    fontWeight: "600",
    fontFamily: typography.body,
  },
  body: {
    flex: 1,
  },
});
