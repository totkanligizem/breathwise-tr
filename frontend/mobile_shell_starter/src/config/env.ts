export type MobileEnvConfig = {
  apiBaseUrl: string | null;
  apiKeyConfigured: boolean;
};

export function readEnvConfig(): MobileEnvConfig {
  const apiBaseUrl = process.env.EXPO_PUBLIC_BREATHWISE_API_BASE_URL?.trim() || null;
  const apiKeyConfigured = Boolean(process.env.EXPO_PUBLIC_BREATHWISE_API_KEY?.trim());
  return {
    apiBaseUrl,
    apiKeyConfigured,
  };
}
