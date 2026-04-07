const PRIORITY_CITY_TOKENS = [
  "istanbul",
  "ankara",
  "izmir",
  "bursa",
  "antalya",
  "mugla",
  "balikesir",
  "edirne",
  "adana",
  "gaziantep",
];

export function normalizeCityToken(value: string): string {
  return value
    .trim()
    .toLocaleLowerCase("tr-TR")
    .replace(/ı/g, "i")
    .replace(/ş/g, "s")
    .replace(/ğ/g, "g")
    .replace(/ç/g, "c")
    .replace(/ö/g, "o")
    .replace(/ü/g, "u")
    .replace(/\s+/g, " ");
}

export function formatCityName(value: string, locale: string): string {
  const trimmed = value.trim();
  if (!trimmed) return value;
  return trimmed
    .split(/\s+/)
    .map((part) => `${part.charAt(0).toLocaleUpperCase(locale)}${part.slice(1).toLocaleLowerCase(locale)}`)
    .join(" ");
}

export function prioritizeCityNames(names: string[]): string[] {
  const uniqueByToken = new Map<string, string>();
  names.forEach((name) => {
    const token = normalizeCityToken(name);
    if (!token || uniqueByToken.has(token)) return;
    uniqueByToken.set(token, name);
  });

  const preferred = PRIORITY_CITY_TOKENS.map((token) => uniqueByToken.get(token)).filter(Boolean) as string[];
  const preferredTokens = new Set(preferred.map((name) => normalizeCityToken(name)));
  const remaining = Array.from(uniqueByToken.values())
    .filter((name) => !preferredTokens.has(normalizeCityToken(name)))
    .sort((a, b) => a.localeCompare(b, "tr-TR", { sensitivity: "base" }));

  return [...preferred, ...remaining];
}
