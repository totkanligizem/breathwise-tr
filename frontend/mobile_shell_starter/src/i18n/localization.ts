import type { LocalizationMeta } from "../types/backend";
import { FALLBACK_TRANSLATIONS } from "./fallbackCatalog";

export const DEFAULT_LOCALE = "tr-TR";
export const FALLBACK_LOCALE = "en-US";

export function normalizeLocale(locale: string | null | undefined): string {
  if (!locale) return DEFAULT_LOCALE;
  return locale.trim().replace("_", "-");
}

export function resolveLocale(meta: LocalizationMeta, requestedLocale?: string): string {
  const aliases = meta.locale_aliases || {};
  const normalized = normalizeLocale(requestedLocale).toLowerCase();
  const resolved = aliases[normalized];
  if (resolved) return resolved;

  const supported = meta.supported_locales || [];
  const exact = supported.find((item) => item.toLowerCase() === normalized);
  if (exact) return exact;

  return meta.default_locale || DEFAULT_LOCALE;
}

export function localizeFromContract(
  key: string | null | undefined,
  locale: string,
  meta: LocalizationMeta,
  serverLabel?: string | null
): string | null {
  if (!key) return serverLabel ?? "—";
  const entries = meta.translation_keys || {};
  const trans = entries[key] || FALLBACK_TRANSLATIONS[key];
  if (!trans) return serverLabel ?? "—";

  const preferred = trans[locale];
  if (preferred) return preferred;
  if (serverLabel && serverLabel.trim()) return serverLabel;
  return "—";
}
