import { Platform } from "react-native";

type QueryValue = string | number | boolean | null | undefined;

export type ApiClientConfig = {
  baseUrl: string;
  apiKey?: string;
  apiKeyHeaderName?: string;
  timeoutMs?: number;
};

export class ApiRequestError extends Error {
  public readonly kind: "http" | "network" | "timeout";
  public readonly status: number;
  public readonly payload: unknown;
  public readonly url: string;

  constructor(
    message: string,
    opts: {
      kind: "http" | "network" | "timeout";
      status: number;
      payload?: unknown;
      url: string;
    }
  ) {
    super(message);
    this.name = "ApiRequestError";
    this.kind = opts.kind;
    this.status = opts.status;
    this.payload = opts.payload ?? null;
    this.url = opts.url;
  }
}

export class BreathwiseApiClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly apiKeyHeaderName: string;
  private readonly timeoutMs: number;

  constructor(config: ApiClientConfig) {
    this.baseUrl = normalizeBaseUrl(config.baseUrl);
    this.apiKey = config.apiKey;
    this.apiKeyHeaderName = config.apiKeyHeaderName || "X-API-Key";
    this.timeoutMs = config.timeoutMs ?? 12_000;
  }

  async get<T>(path: string, query?: Record<string, QueryValue>): Promise<T> {
    const url = buildRequestUrl(this.baseUrl, path, query);
    const requestUrl = url.toString();
    const requestOrigin = safeOrigin(url);

    if (Platform.OS === "web") {
      const warning = computeWebOriginWarning(requestOrigin);
      if (warning) {
        // Keep as console warning to avoid failing native builds and to aid web debugging.
        console.warn(warning);
      }
    }

    const headers: Record<string, string> = {};
    if (this.apiKey) headers[this.apiKeyHeaderName] = this.apiKey;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    let res: Response;
    try {
      res = await fetch(requestUrl, { method: "GET", headers, signal: controller.signal });
    } catch (error) {
      const aborted = isAbortError(error);
      const browserOrigin = getWebOrigin();
      const hint =
        Platform.OS === "web"
          ? "Possible CORS/preflight rejection or unreachable backend target."
          : "Check backend host reachability from device/emulator network.";
      const detailParts = [hint];
      if (browserOrigin && requestOrigin) {
        detailParts.push(`origin=${browserOrigin} target=${requestOrigin}`);
      }
      throw new ApiRequestError(
        aborted
          ? `Request timeout after ${this.timeoutMs}ms for ${requestUrl}.`
          : `Network request failed for ${requestUrl}. ${detailParts.join(" ")}`,
        {
          kind: aborted ? "timeout" : "network",
          status: 0,
          payload: error instanceof Error ? error.message : String(error),
          url: requestUrl,
        }
      );
    } finally {
      clearTimeout(timeout);
    }

    if (!res.ok) {
      let payload: unknown = null;
      try {
        payload = await res.json();
      } catch {
        payload = null;
      }
      const messageFromPayload = extractMessageFromPayload(payload);
      const message = messageFromPayload
        ? `API request failed (${res.status}): ${messageFromPayload}`
        : `API request failed: ${res.status} ${res.statusText}`;
      throw new ApiRequestError(message, {
        kind: "http",
        status: res.status,
        payload,
        url: requestUrl,
      });
    }
    return (await res.json()) as T;
  }
}

export function buildClientFromEnv(): BreathwiseApiClient {
  const baseUrl = process.env.EXPO_PUBLIC_BREATHWISE_API_BASE_URL?.trim();
  if (!baseUrl) {
    throw new Error("Missing EXPO_PUBLIC_BREATHWISE_API_BASE_URL in app environment");
  }

  return new BreathwiseApiClient({
    baseUrl,
    apiKey: process.env.EXPO_PUBLIC_BREATHWISE_API_KEY,
    apiKeyHeaderName: "X-API-Key",
    timeoutMs: 12_000,
  });
}

function normalizeBaseUrl(value: string): string {
  const trimmed = value.trim();
  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch (error) {
    throw new Error(
      `Invalid EXPO_PUBLIC_BREATHWISE_API_BASE_URL: ${trimmed}. Use a full URL like http://127.0.0.1:8000`
    );
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error(`Unsupported API base URL protocol: ${parsed.protocol}. Use http or https.`);
  }

  if (Platform.OS === "web" && parsed.hostname === "0.0.0.0") {
    const browserHost = getWebHost();
    if (browserHost) parsed.hostname = browserHost;
  }

  return parsed.toString().replace(/\/+$/, "");
}

function buildRequestUrl(baseUrl: string, path: string, query?: Record<string, QueryValue>): URL {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const url = new URL(`${baseUrl}${normalizedPath}`);
  if (!query) return url;

  for (const [key, value] of Object.entries(query)) {
    if (value === null || value === undefined) continue;
    url.searchParams.set(key, String(value));
  }
  return url;
}

function extractMessageFromPayload(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") return null;

  const root = payload as Record<string, unknown>;
  const directMessage = root.message;
  if (typeof directMessage === "string" && directMessage.trim()) return directMessage;

  const errorNode = root.error;
  if (!errorNode || typeof errorNode !== "object") return null;
  const errorMessage = (errorNode as Record<string, unknown>).message;
  if (typeof errorMessage === "string" && errorMessage.trim()) return errorMessage;

  return null;
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function safeOrigin(url: URL): string | null {
  try {
    return url.origin;
  } catch {
    return null;
  }
}

function getWebHost(): string | null {
  if (typeof window === "undefined" || !window.location) return null;
  return window.location.hostname || null;
}

function getWebOrigin(): string | null {
  if (typeof window === "undefined" || !window.location) return null;
  return window.location.origin || null;
}

function computeWebOriginWarning(targetOrigin: string | null): string | null {
  const origin = getWebOrigin();
  if (!origin || !targetOrigin) return null;
  if (origin === targetOrigin) return null;
  return `[Breathwise API] Cross-origin request from ${origin} to ${targetOrigin}. Ensure backend CORS allows this origin.`;
}
