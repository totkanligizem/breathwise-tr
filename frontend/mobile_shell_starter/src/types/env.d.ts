declare namespace NodeJS {
  interface ProcessEnv {
    EXPO_PUBLIC_BREATHWISE_API_BASE_URL?: string;
    EXPO_PUBLIC_BREATHWISE_API_KEY?: string;
  }
}

declare const process: {
  env: NodeJS.ProcessEnv;
};
