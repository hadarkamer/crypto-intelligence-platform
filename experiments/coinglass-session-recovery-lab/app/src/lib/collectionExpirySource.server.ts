import { EXPIRY_EXCHANGES } from "./collectionStructured";

type CoinGlassExchange = "Deribit" | "Binance" | "Bybit";
export type VerifiedExpirySnapshot = {
  rows: Array<{
    date: string;
    exchanges: Array<{ exchange: string; price: number | null; amount_usd: number | null }>;
  }>;
  capturedAt: string;
  sourceUrl: string;
};

const providers: ReadonlyArray<{ api: CoinGlassExchange; label: string }> = [
  { api: "Deribit", label: EXPIRY_EXCHANGES[0] },
  { api: "Binance", label: EXPIRY_EXCHANGES[1] },
  { api: "Bybit", label: EXPIRY_EXCHANGES[2] },
];
const maximumResponseBytes = 1024 * 1024;

/** The configured public page stays the provenance source; requests only use
 * CoinGlass's static API host, never a URL supplied by a page or API response. */
export function isCoinGlassExpirySource(sourceUrl: string): boolean {
  const match = /^https:\/\/([^/?#]+)(\/pro\/options\/max-pain\/?)$/i.exec(sourceUrl);
  return (
    !!match &&
    match[0] === sourceUrl &&
    ["coinglass.com", "www.coinglass.com"].includes(match[1].toLowerCase()) &&
    ["/pro/options/max-pain", "/pro/options/max-pain/"].includes(match[2])
  );
}

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function nullableDecimal(value: unknown, positive: boolean): number | null {
  if (value === undefined || value === null) return null;
  if (
    typeof value !== "number" &&
    (typeof value !== "string" ||
      value.trim() !== value ||
      !/^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$/.test(value))
  )
    throw new Error("invalid_numeric_value");
  const number = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(number) || (positive ? number <= 0 : number < 0))
    throw new Error("invalid_numeric_value");
  return number;
}

function expiryDate(value: unknown): string {
  if (typeof value !== "string" || value.length !== 6 || !/^\d{6}$/.test(value))
    throw new Error("invalid_expiry_date");
  const date = `20${value.slice(0, 2)}-${value.slice(2, 4)}-${value.slice(4, 6)}`;
  const time = Date.parse(`${date}T00:00:00.000Z`);
  if (!Number.isFinite(time) || new Date(time).toISOString().slice(0, 10) !== date)
    throw new Error("invalid_expiry_date");
  return date;
}

function assertIdentity(row: Record<string, unknown>, exchange: CoinGlassExchange): void {
  if (("symbol" in row && row.symbol !== "BTC") || ("exchange" in row && row.exchange !== exchange))
    throw new Error("source_identity_mismatch");
}

function jerusalemDate(now: Date): string {
  if (!Number.isFinite(now.getTime())) throw new Error("invalid_capture_time");
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Jerusalem",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((entry) => entry.type === type)!.value;
  return `${part("year")}-${part("month")}-${part("day")}`;
}

async function boundedJson(response: Response): Promise<unknown> {
  const reader = response.body?.getReader();
  if (!reader) throw new Error("empty_response");
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let size = 0,
    text = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maximumResponseBytes) {
        await reader.cancel();
        throw new Error("response_too_large");
      }
      text += decoder.decode(value, { stream: true });
    }
    text += decoder.decode();
    return JSON.parse(text);
  } finally {
    reader.releaseLock();
  }
}

function poolRows(
  envelope: unknown,
  exchange: CoinGlassExchange,
  today: string,
): Map<string, { price: number | null; amount_usd: number | null }> {
  if (
    !record(envelope) ||
    envelope.code !== "0" ||
    !Array.isArray(envelope.data) ||
    envelope.data.length > 366
  )
    throw new Error("invalid_response");
  assertIdentity(envelope, exchange);
  const rows = new Map<string, { price: number | null; amount_usd: number | null }>();
  const seen = new Set<string>();
  for (const row of envelope.data) {
    if (!record(row)) throw new Error("invalid_expiry_row");
    assertIdentity(row, exchange);
    const date = expiryDate(row.date);
    if (seen.has(date)) throw new Error("duplicate_expiry_date");
    seen.add(date);
    // These documented fields are USD notional, not contract counts or market
    // value. An incomplete pair remains missing; never total a partial side.
    const price = nullableDecimal(row.max_pain_price, true);
    const calls = nullableDecimal(row.call_open_interest_notional, false);
    const puts = nullableDecimal(row.put_open_interest_notional, false);
    const amount_usd = calls === null || puts === null ? null : calls + puts;
    if (amount_usd !== null && !Number.isFinite(amount_usd))
      throw new Error("invalid_numeric_value");
    if (date >= today && (price !== null || amount_usd !== null))
      rows.set(date, { price, amount_usd });
  }
  return rows;
}

/** Three sequential bounded reads; a failure in any requested exchange fails
 * the whole snapshot. No retries, alternate exchanges, scraping or model calls. */
export async function fetchCoinGlassExpirySnapshot(
  sourceUrl: string,
  now = new Date(),
): Promise<VerifiedExpirySnapshot> {
  if (!isCoinGlassExpirySource(sourceUrl))
    throw new Error("מקור פקיעות CoinGlass אינו נתמך; התיבה לא שונתה.");
  const key = process.env.COINGLASS_API_KEY;
  if (!key?.trim())
    throw new Error("קריאת פקיעות CoinGlass דורשת COINGLASS_API_KEY בשרת; התיבה לא שונתה.");
  const today = jerusalemDate(now);
  const pools: Array<Map<string, { price: number | null; amount_usd: number | null }>> = [];
  for (const provider of providers) {
    try {
      const response = await fetch(
        `https://open-api-v4.coinglass.com/api/option/max-pain?symbol=BTC&exchange=${provider.api}`,
        {
          method: "GET",
          redirect: "manual",
          signal: AbortSignal.timeout(10_000),
          headers: { "CG-API-KEY": key, Accept: "application/json" },
        },
      );
      if (!response.ok) throw new Error("provider_unavailable");
      pools.push(poolRows(await boundedJson(response), provider.api, today));
    } catch {
      throw new Error(`לא אומתו נתוני פקיעות CoinGlass עבור ${provider.api}; התיבה לא שונתה.`);
    }
  }
  const dates = [...new Set(pools.flatMap((pool) => [...pool.keys()]))].sort();
  return {
    sourceUrl,
    capturedAt: now.toISOString(),
    rows: dates.map((date) => ({
      date,
      exchanges: providers.map((provider, index) => ({
        exchange: provider.label,
        price: pools[index].get(date)?.price ?? null,
        amount_usd: pools[index].get(date)?.amount_usd ?? null,
      })),
    })),
  };
}
