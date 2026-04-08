import { Platform } from "react-native";

export const colors = {
  canvas: "#071522",
  canvasElevated: "#10243A",
  surface: "#132B44",
  surfaceMuted: "#1B3A5B",
  surfaceStrong: "#234569",
  textPrimary: "#ECF5FF",
  textSecondary: "#BCD0E6",
  textTertiary: "#86A1BE",
  accent: "#4CB9FF",
  accentStrong: "#2C97E6",
  accentSoft: "#163955",
  warning: "#F4A62D",
  warningSoft: "#4A371B",
  danger: "#F36E6E",
  dangerSoft: "#4A2427",
  info: "#79D0FF",
  infoSoft: "#1A3854",
  border: "#315A80",
  borderStrong: "#4A739A",
  shadow: "#000000",
  onAccent: "#F6FBFF",
  darkPanel: "#0C1E31",
  darkPanelSoft: "#1A3B5C",
  darkTextOnPanel: "#EAF4FF",
  aqiGood: "#4AD3A8",
  aqiModerate: "#F2C64B",
  aqiRisky: "#F08C45",
  aqiHazard: "#EF5E68",
  heatCool: "#75B8FF",
  heatWarm: "#F3B24F",
  heatHot: "#F27763",
};

export const spacing = {
  xxs: 4,
  xs: 8,
  sm: 12,
  md: 16,
  lg: 20,
  xl: 26,
  xxl: 36,
};

export const radius = {
  xs: 10,
  sm: 15,
  md: 20,
  lg: 28,
  pill: 999,
};

export const typography = {
  display: Platform.select({ ios: "AvenirNext-Bold", android: "sans-serif-black", default: "system-ui" }),
  heading: Platform.select({ ios: "AvenirNext-DemiBold", android: "sans-serif-medium", default: "system-ui" }),
  body: Platform.select({ ios: "AvenirNext-Regular", android: "sans-serif", default: "system-ui" }),
  mono: Platform.select({ ios: "Menlo", android: "monospace", default: "monospace" }),
  size: {
    caption: 11.5,
    bodySm: 13,
    body: 14.5,
    bodyLg: 16.5,
    title: 23,
    hero: 34,
  },
};

export const shadow = {
  card: {
    shadowColor: colors.shadow,
    shadowOpacity: 0.2,
    shadowOffset: { width: 0, height: 10 },
    shadowRadius: 24,
    elevation: 6,
  },
  floating: {
    shadowColor: colors.shadow,
    shadowOpacity: 0.26,
    shadowOffset: { width: 0, height: 14 },
    shadowRadius: 28,
    elevation: 9,
  },
  glow: {
    shadowColor: "#2496EB",
    shadowOpacity: 0.36,
    shadowOffset: { width: 0, height: 0 },
    shadowRadius: 18,
    elevation: 7,
  },
};

export const layout = {
  maxContentWidth: 1080,
  compactContentWidth: 760,
};
