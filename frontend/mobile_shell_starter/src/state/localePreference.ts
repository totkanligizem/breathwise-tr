import AsyncStorage from "@react-native-async-storage/async-storage";
import { Platform } from "react-native";

const LOCALE_STORAGE_KEY = "breathwise.locale.v1";

function readWebLocaleSync(): string | null {
  if (Platform.OS !== "web") return null;
  if (typeof window === "undefined" || !window.localStorage) return null;
  try {
    const raw = window.localStorage.getItem(LOCALE_STORAGE_KEY);
    return raw && raw.trim() ? raw : null;
  } catch {
    return null;
  }
}

export function getInitialLocalePreference(): string | null {
  return readWebLocaleSync();
}

export async function loadLocalePreference(): Promise<string | null> {
  const webSync = readWebLocaleSync();
  if (webSync) return webSync;
  try {
    const raw = await AsyncStorage.getItem(LOCALE_STORAGE_KEY);
    return raw && raw.trim() ? raw : null;
  } catch {
    return null;
  }
}

export async function saveLocalePreference(locale: string): Promise<void> {
  try {
    await AsyncStorage.setItem(LOCALE_STORAGE_KEY, locale);
  } catch {
    if (Platform.OS !== "web") return;
    if (typeof window === "undefined" || !window.localStorage) return;
    try {
      window.localStorage.setItem(LOCALE_STORAGE_KEY, locale);
    } catch {
      // Locale persistence failures are non-fatal.
    }
  }
}
