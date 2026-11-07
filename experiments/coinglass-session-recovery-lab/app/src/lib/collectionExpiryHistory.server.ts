import type { SupabaseClient } from "@supabase/supabase-js";

export type VerifiedExpiryMarketDay = {
  day: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volumeBtc: number;
  openedAt: string;
  closedAt: string;
  capturedAt: string;
  sourceUrl: string;
};

const DAY_MS = 86_400_000;
const MARKET = "https://www.bitstamp.net/api/v2/ohlc/btcusd/";

function dayStart(day: string): number {
  const start = Date.parse(`${day}T00:00:00.000Z`);
  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(day) ||
    !Number.isFinite(start) ||
    new Date(start).toISOString().slice(0, 10) !== day
  )
    throw new Error("invalid_market_day");
  return start;
}

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function decimal(value: unknown, positive = true): number {
  if (
    typeof value !== "number" &&
    (typeof value !== "string" || !/^(?:\d+(?:\.\d+)?|\.\d+)$/.test(value))
  )
    throw new Error("invalid_market_number");
  const result = Number(value);
  if (!Number.isFinite(result) || (positive ? result <= 0 : result < 0))
    throw new Error("invalid_market_number");
  return result;
}

export function verifyExpiryMarketDay(
  payload: unknown,
  day: string,
  now: number,
  sourceUrl: string,
): VerifiedExpiryMarketDay {
  const start = dayStart(day),
    end = start + DAY_MS;
  // Allow the provider two minutes to finalize the completed daily candle.
  if (!Number.isSafeInteger(now) || now < end + 120_000) throw new Error("market_day_not_closed");
  if (!record(payload) || !record(payload.data)) throw new Error("invalid_market_response");
  const data = payload.data;
  if (
    (data.pair !== "BTC/USD" && data.market !== "BTC/USD") ||
    ("pair" in data && data.pair !== "BTC/USD") ||
    ("market" in data && data.market !== "BTC/USD") ||
    !Array.isArray(data.ohlc) ||
    data.ohlc.length !== 1 ||
    !record(data.ohlc[0])
  )
    throw new Error("invalid_market_identity");
  const row = data.ohlc[0];
  const timestamp = decimal(row.timestamp, false);
  if (!Number.isSafeInteger(timestamp) || timestamp * 1000 !== start)
    throw new Error("wrong_market_day");
  const open = decimal(row.open),
    high = decimal(row.high),
    low = decimal(row.low),
    close = decimal(row.close),
    volumeBtc = decimal(row.volume, false);
  if (high < Math.max(open, low, close) || low > Math.min(open, high, close))
    throw new Error("inconsistent_market_prices");
  const endpoint = new URL(sourceUrl);
  if (
    endpoint.origin + endpoint.pathname !== MARKET ||
    endpoint.username ||
    endpoint.password ||
    endpoint.searchParams.get("step") !== "86400" ||
    endpoint.searchParams.get("limit") !== "1" ||
    endpoint.searchParams.get("end") !== String(end / 1000 - 1) ||
    endpoint.searchParams.get("exclude_current_candle") !== "true"
  )
    throw new Error("invalid_market_source");
  return {
    day,
    open,
    high,
    low,
    close,
    volumeBtc,
    openedAt: new Date(start).toISOString(),
    closedAt: new Date(end).toISOString(),
    capturedAt: new Date(now).toISOString(),
    sourceUrl,
  };
}

async function boundedMarketJson(response: Response): Promise<unknown> {
  const reader = response.body?.getReader();
  if (!reader) throw new Error("empty_market_response");
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let bytes = 0,
    text = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > 128 * 1024) {
        await reader.cancel();
        throw new Error("market_response_too_large");
      }
      text += decoder.decode(value, { stream: true });
    }
    return JSON.parse(text + decoder.decode());
  } finally {
    reader.releaseLock();
  }
}

/** Public, fixed BTC/USD endpoint, one UTC daily candle. No redirects, retries,
 * model interpretation, account keys or destinations from stored data. */
export async function readExpiryMarketDay(
  day: string,
  options: { now?: () => number; fetch?: typeof fetch } = {},
): Promise<VerifiedExpiryMarketDay> {
  const now = options.now ?? Date.now;
  const startedAt = now(),
    start = dayStart(day),
    end = start + DAY_MS;
  if (startedAt < end + 120_000) throw new Error("market_day_not_closed");
  const endpoint = new URL(MARKET);
  endpoint.search = new URLSearchParams({
    step: "86400",
    limit: "1",
    end: String(end / 1000 - 1),
    exclude_current_candle: "true",
  }).toString();
  const response = await (options.fetch ?? fetch)(endpoint.href, {
    method: "GET",
    redirect: "manual",
    signal: AbortSignal.timeout(8000),
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error("market_source_unavailable");
  }
  const payload = await boundedMarketJson(response);
  const capturedAt = now();
  if (capturedAt < startedAt || capturedAt - startedAt > 10_000)
    throw new Error("market_capture_timing");
  return verifyExpiryMarketDay(payload, day, capturedAt, endpoint.href);
}

export type ExpiryMaintenanceResult = { archived: number; completed: number; pending: number };

/** Called on the authenticated worker's due tick, even if no fields are due.
 * SQL owns atomic archival, row locks, leases and backoff; outbound work is at
 * most two eight-second reads and missing data stays pending for a later tick. */
export async function maintainCollectionExpiries(
  db: SupabaseClient,
  readDay: typeof readExpiryMarketDay = readExpiryMarketDay,
): Promise<ExpiryMaintenanceResult> {
  const archive = await db.rpc("v2_collection_expiry_maintain", { p_limit: 100 });
  if (archive.error) throw new Error("לא ניתן לארכב את נתוני הפקיעות.");
  const claims = await db.rpc("v2_collection_expiry_market_claim", { p_limit: 2 });
  if (claims.error || !Array.isArray(claims.data) || claims.data.length > 2)
    throw new Error("לא ניתן לקרוא את תור סטטיסטיקת הפקיעות.");
  let completed = 0,
    pending = 0;
  for (const claim of claims.data) {
    if (!record(claim) || typeof claim.day !== "string" || typeof claim.lease_id !== "string")
      throw new Error("תור סטטיסטיקת הפקיעות אינו תקין.");
    dayStart(claim.day);
    let candle: VerifiedExpiryMarketDay | null = null;
    try {
      candle = await readDay(claim.day);
    } catch {
      /* Pending is an explicit state. */
    }
    const result = await db.rpc("v2_collection_expiry_market_finish", {
      p_day: claim.day,
      p_lease_id: claim.lease_id,
      p_candle: candle,
    });
    if (result.error) throw new Error("לא ניתן לשמור את סטטיסטיקת הפקיעות.");
    if (result.data === true && candle) completed++;
    else pending++;
  }
  return { archived: typeof archive.data === "number" ? archive.data : 0, completed, pending };
}
