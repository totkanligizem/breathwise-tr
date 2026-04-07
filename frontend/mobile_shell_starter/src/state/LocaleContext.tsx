import React, { createContext, useContext, useEffect, useMemo, useState } from "react";

import { buildClientFromEnv, BreathwiseApiClient } from "../api/client";
import { fetchLocalizationMeta } from "../api/endpoints";
import { FALLBACK_TRANSLATIONS } from "../i18n/fallbackCatalog";
import { DEFAULT_LOCALE, FALLBACK_LOCALE, localizeFromContract, resolveLocale } from "../i18n/localization";
import { getInitialLocalePreference, loadLocalePreference, saveLocalePreference } from "./localePreference";
import type { LocalizationMeta } from "../types/backend";

type LocaleContextValue = {
  locale: string;
  setLocale: (locale: string) => void;
  localizationMeta: LocalizationMeta;
  localizationLoading: boolean;
  localizationError: string | null;
  t: (key: string, serverLabel?: string | null) => string;
  apiClient: BreathwiseApiClient | null;
  apiConfigError: string | null;
};

const FALLBACK_META: LocalizationMeta = {
  supported_locales: [DEFAULT_LOCALE, FALLBACK_LOCALE],
  default_locale: DEFAULT_LOCALE,
  fallback_locale: FALLBACK_LOCALE,
  locale_aliases: {
    tr: DEFAULT_LOCALE,
    en: FALLBACK_LOCALE,
    "tr-tr": DEFAULT_LOCALE,
    "en-us": FALLBACK_LOCALE,
  },
  translation_key_count: Object.keys(FALLBACK_TRANSLATIONS).length,
  contract_path: "fallback_catalog",
  generated_at_utc: null,
  translation_keys: FALLBACK_TRANSLATIONS,
};

const LocaleContext = createContext<LocaleContextValue | undefined>(undefined);

export function LocaleProvider({ children }: { children: React.ReactNode }) {
  const [locale, setLocaleState] = useState<string>(() =>
    resolveLocale(FALLBACK_META, getInitialLocalePreference() ?? DEFAULT_LOCALE)
  );
  const [localizationMeta, setLocalizationMeta] = useState<LocalizationMeta>(FALLBACK_META);
  const [localizationLoading, setLocalizationLoading] = useState<boolean>(true);
  const [localizationError, setLocalizationError] = useState<string | null>(null);
  const [apiClient, setApiClient] = useState<BreathwiseApiClient | null>(null);
  const [apiConfigError, setApiConfigError] = useState<string | null>(null);

  useEffect(() => {
    try {
      setApiClient(buildClientFromEnv());
      setApiConfigError(null);
    } catch (error) {
      setApiClient(null);
      setApiConfigError(error instanceof Error ? error.message : "API configuration is missing.");
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    loadLocalePreference().then((storedLocale) => {
      if (cancelled || !storedLocale) return;
      setLocaleState((prev) => {
        if (prev === storedLocale) return prev;
        return resolveLocale(localizationMeta, storedLocale);
      });
    });

    return () => {
      cancelled = true;
    };
  }, [localizationMeta]);

  useEffect(() => {
    if (!apiClient) {
      setLocalizationLoading(false);
      return;
    }
    let cancelled = false;
    setLocalizationLoading(true);

    fetchLocalizationMeta(apiClient)
      .then((meta) => {
        if (cancelled) return;
        setLocalizationMeta(meta);
        setLocaleState((prev) => {
          const resolved = resolveLocale(meta, prev);
          void saveLocalePreference(resolved);
          return resolved;
        });
        setLocalizationError(null);
      })
      .catch((error) => {
        if (cancelled) return;
        setLocalizationMeta(FALLBACK_META);
        setLocalizationError(error instanceof Error ? error.message : "Localization metadata could not be loaded.");
      })
      .finally(() => {
        if (!cancelled) {
          setLocalizationLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [apiClient]);

  const value = useMemo<LocaleContextValue>(
    () => ({
      locale,
      setLocale: (next) => {
        const resolved = resolveLocale(localizationMeta, next);
        setLocaleState(resolved);
        void saveLocalePreference(resolved);
      },
      localizationMeta,
      localizationLoading,
      localizationError,
      t: (key: string, serverLabel?: string | null) =>
        localizeFromContract(key, locale, localizationMeta, serverLabel) ?? "—",
      apiClient,
      apiConfigError,
    }),
    [apiClient, apiConfigError, locale, localizationError, localizationLoading, localizationMeta]
  );

  return <LocaleContext.Provider value={value}>{children}</LocaleContext.Provider>;
}

export function useLocaleContext(): LocaleContextValue {
  const ctx = useContext(LocaleContext);
  if (!ctx) {
    throw new Error("useLocaleContext must be used inside LocaleProvider");
  }
  return ctx;
}
