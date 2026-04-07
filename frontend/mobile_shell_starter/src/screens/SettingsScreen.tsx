import React from "react";
import { Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { MaterialCommunityIcons } from "@expo/vector-icons";

import { useLocaleContext } from "../state/LocaleContext";
import { colors, radius, shadow, spacing, typography } from "../theme/tokens";
import type { BaseScreenProps } from "./types";

export function SettingsScreen({ t }: BaseScreenProps) {
  const { locale, setLocale, localizationMeta } = useLocaleContext();
  const locales = localizationMeta.supported_locales.length > 0 ? localizationMeta.supported_locales : ["tr-TR", "en-US"];

  return (
    <ScrollView contentContainerStyle={styles.content} showsVerticalScrollIndicator={false}>
      <View style={styles.titleWrap}>
        <Text style={styles.title}>{t("ui.settings.locale.title", "Language & Experience")}</Text>
        <Text style={styles.subtitle}>{t("ui.settings.subtitle", "Personalize how Breathwise feels and reads")}</Text>
      </View>

      <View style={styles.heroCard}>
        <View style={styles.heroHead}>
          <View style={styles.heroLeft}>
            <MaterialCommunityIcons name="palette-outline" size={18} color={colors.info} />
            <Text style={styles.heroEyebrow}>{t("ui.settings.personalization_title", "Personalization")}</Text>
          </View>
        </View>

        <Text style={styles.heroMain}>{t("ui.settings.personalization_subtitle", "Calm visuals, clear language, and trustworthy weather insights for every session")}</Text>
      </View>

      <View style={styles.sectionCard}>
        <View style={styles.sectionHead}>
          <MaterialCommunityIcons name="translate" size={17} color={colors.accent} />
          <Text style={styles.sectionTitle}>{t("ui.common.locale", "Language")}</Text>
        </View>

        <View style={styles.localeOptions}>
          {locales.map((item) => {
            const active = item === locale;
            return (
              <Pressable key={item} style={[styles.localeChip, active ? styles.localeChipActive : null]} onPress={() => setLocale(item)}>
                <Text numberOfLines={1} style={[styles.localeText, active ? styles.localeTextActive : null]}>
                  {item === "tr-TR" ? t("ui.settings.locale.tr", "Turkish") : item === "en-US" ? t("ui.settings.locale.en", "English") : item}
                </Text>
                {active ? <MaterialCommunityIcons name="check-circle" size={14} color={colors.onAccent} /> : null}
              </Pressable>
            );
          })}
        </View>

        <View style={styles.metaRow}>
          <Text style={styles.metaLabel}>{t("ui.settings.locale.active", "Current Language")}</Text>
          <Text style={styles.metaValue}>{localeDisplayName(locale, t)}</Text>
        </View>
      </View>

      <View style={styles.sectionCard}>
        <View style={styles.sectionHead}>
          <MaterialCommunityIcons name="tune-variant" size={17} color={colors.info} />
          <Text style={styles.sectionTitle}>{t("ui.settings.experience", "Experience")}</Text>
        </View>

        <ExperienceInfoRow
          title={t("ui.settings.pref_ambient", "Ambient Weather Effects")}
          subtitle={t("ui.settings.pref_ambient_detail", "Rain / sun visual mood layers on major cards")}
          icon="weather-rainy"
        />
        <ExperienceInfoRow
          title={t("ui.settings.pref_compact", "Compact Timeline Rows")}
          subtitle={t("ui.settings.pref_compact_detail", "Tighter timeline cards for denser data scanning")}
          icon="view-compact-outline"
        />
      </View>

      <View style={styles.sectionCard}>
        <View style={styles.sectionHead}>
          <MaterialCommunityIcons name="information-outline" size={17} color={colors.accent} />
          <Text style={styles.sectionTitle}>{t("ui.settings.about_title", "About")}</Text>
        </View>
        <Text style={styles.aboutText}>{t("ui.settings.about_text", "Breathwise combines weather and air quality intelligence for all 81 provinces of Turkey.")}</Text>
        <Text style={styles.aboutText}>{t("ui.settings.about_update_note", "Data refreshes continuously so city and province insights stay timely and dependable.")}</Text>
      </View>
    </ScrollView>
  );
}

function ExperienceInfoRow({
  title,
  subtitle,
  icon,
}: {
  title: string;
  subtitle: string;
  icon: React.ComponentProps<typeof MaterialCommunityIcons>["name"];
}) {
  return (
    <View style={styles.prefRow}>
      <View style={styles.prefTextWrap}>
        <Text style={styles.prefTitle}>{title}</Text>
        <Text style={styles.prefSubtitle}>{subtitle}</Text>
      </View>
      <View style={styles.prefIconWrap}>
        <MaterialCommunityIcons name={icon} size={15} color={colors.info} />
      </View>
    </View>
  );
}

function localeDisplayName(locale: string, t: (key: string, fallback?: string) => string): string {
  if (locale === "tr-TR") return `Türkçe / ${t("ui.settings.locale.tr", "Turkish")}`;
  if (locale === "en-US") return `English / ${t("ui.settings.locale.en", "English")}`;
  return locale;
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
    borderColor: "#416B91",
    borderWidth: 1,
    backgroundColor: colors.darkPanel,
    padding: spacing.md,
    gap: spacing.xs,
    ...shadow.floating,
  },
  heroHead: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  heroLeft: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.xs,
  },
  heroEyebrow: {
    color: "#AFC9E4",
    fontSize: typography.size.caption,
    textTransform: "uppercase",
    letterSpacing: 0.5,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  heroMain: {
    color: colors.darkTextOnPanel,
    fontSize: typography.size.body,
    lineHeight: 21,
    fontFamily: typography.body,
  },
  sectionCard: {
    borderRadius: radius.md,
    borderColor: "#375C80",
    borderWidth: 1,
    backgroundColor: "#112E45",
    padding: spacing.md,
    gap: spacing.sm,
    ...shadow.card,
  },
  sectionHead: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.xs,
  },
  sectionTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.bodyLg,
    fontWeight: "800",
    fontFamily: typography.heading,
  },
  localeOptions: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.xs,
  },
  localeChip: {
    borderRadius: radius.pill,
    borderColor: colors.border,
    borderWidth: 1,
    backgroundColor: colors.surfaceMuted,
    paddingVertical: spacing.xs,
    paddingHorizontal: spacing.md,
    minWidth: 132,
    alignItems: "center",
    justifyContent: "center",
    flexDirection: "row",
    gap: spacing.xs,
  },
  localeChipActive: {
    backgroundColor: colors.accentStrong,
    borderColor: "#73C2FF",
    ...shadow.glow,
  },
  localeText: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  localeTextActive: {
    color: colors.onAccent,
  },
  metaRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.md,
  },
  metaLabel: {
    flex: 1,
    color: colors.textTertiary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    fontFamily: typography.body,
  },
  metaValue: {
    flex: 1.2,
    color: colors.textPrimary,
    fontSize: typography.size.bodySm,
    fontWeight: "700",
    textAlign: "right",
    fontFamily: typography.heading,
  },
  prefRow: {
    borderRadius: radius.sm,
    borderColor: "#3B6186",
    borderWidth: 1,
    backgroundColor: "#183652",
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.sm,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.sm,
  },
  prefTextWrap: {
    flex: 1,
    gap: 2,
  },
  prefTitle: {
    color: colors.textPrimary,
    fontSize: typography.size.body,
    fontWeight: "700",
    fontFamily: typography.heading,
  },
  prefSubtitle: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    fontFamily: typography.body,
  },
  prefIconWrap: {
    borderRadius: radius.pill,
    borderColor: "#5E86AB",
    borderWidth: 1,
    backgroundColor: "#214969",
    width: 28,
    height: 28,
    alignItems: "center",
    justifyContent: "center",
  },
  aboutText: {
    color: colors.textSecondary,
    fontSize: typography.size.bodySm,
    lineHeight: 19,
    fontFamily: typography.body,
  },
});
