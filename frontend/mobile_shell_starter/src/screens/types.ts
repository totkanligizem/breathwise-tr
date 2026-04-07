import { BreathwiseApiClient } from "../api/client";
import type { LocalizationMeta } from "../types/backend";

export type BaseScreenProps = {
  locale: string;
  t: (key: string, fallback?: string) => string;
  client: BreathwiseApiClient | null;
  localizationMeta: LocalizationMeta;
};
