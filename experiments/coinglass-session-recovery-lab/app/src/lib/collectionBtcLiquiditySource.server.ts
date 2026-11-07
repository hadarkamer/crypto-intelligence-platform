import { SourceHttpError, SourceTransportError, sourceRequest } from "./collectionSources.server";

export const BTC_LIQUIDITY_CHART_URL = "https://www.binance.com/en/trade/BTC_USDT?type=spot";
const API = "https://data-api.binance.vision/api/v3/";
const KRAKEN_API = "https://api.kraken.com/0/public/";
const KRAKEN_CHART_URL = "https://pro.kraken.com/app/trade/btc-usd";
const OKX_API = "https://openapi.okx.com/api/v5/market/";
const OKX_CHART_URL = "https://www.okx.com/trade-spot/btc-usdt";
const BITSTAMP_API = "https://www.bitstamp.net/api/v2/";
const BITSTAMP_CHART_URL = "https://www.bitstamp.net/markets/btc/usd/";
const OKX_BARS = {
  "15m": "15m",
  "1h": "1H",
  "4h": "4H",
  "1d": "1Dutc",
  "1w": "1Wutc",
  "1M": "1Mutc",
} as const;
const LIMIT = 120;
export const BTC_LIQUIDITY_TIMEFRAMES = ["15m", "1h", "4h", "1d", "1w", "1M"] as const;
export type BtcLiquidityTimeframe = (typeof BTC_LIQUIDITY_TIMEFRAMES)[number];
type Timeframe = BtcLiquidityTimeframe;
// Keep established request order for short frames; renderers use the public order.
const REQUEST_TIMEFRAMES: Timeframe[] = ["1h", "4h", "1d", "15m", "1w", "1M"];
const TIMEFRAMES = {
  "15m": 900_000,
  "1h": 3_600_000,
  "4h": 14_400_000,
  "1d": 86_400_000,
  "1w": 604_800_000,
  "1M": 2_678_400_000,
} as const;
function periodStart(
  at: number,
  interval: Timeframe,
  provider: MarketProvider = "Binance",
): number {
  if (interval === "1M") {
    const date = new Date(at);
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
  }
  const offset = interval === "1w" && provider !== "Kraken" ? 345_600_000 : 0;
  return Math.floor((at - offset) / TIMEFRAMES[interval]) * TIMEFRAMES[interval] + offset;
}
function nextPeriod(at: number, interval: Timeframe): number {
  if (interval === "1M") {
    const date = new Date(at);
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1);
  }
  return at + TIMEFRAMES[interval];
}
function providerIntervals(provider: MarketProvider): Timeframe[] {
  return REQUEST_TIMEFRAMES.filter((interval) =>
    provider === "Kraken"
      ? interval !== "1M"
      : provider === "Bitstamp"
        ? interval !== "1w" && interval !== "1M"
        : true,
  );
}
type Candle = {
  openedAt: number;
  closedAt: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volumeBtc: number;
};
export type BtcLiquidityCandidate = {
  id: string;
  timeframe: Timeframe;
  side: "above" | "below";
  kind:
    | "swing_high"
    | "swing_low"
    | "equal_highs"
    | "equal_lows"
    | "recent_range_high"
    | "recent_range_low";
  low: number;
  high: number;
  distancePercent: number;
  touches: number;
  candleTimesUtc: string[];
  evidence: string;
  interpretation: "inferred_stop_area_not_measured_liquidations";
};
export type BtcTechnicalFrame = {
  interval: Timeframe;
  closedCandleCount: number;
  latestClosedAt: string;
  lastClose: number;
  ema20: number;
  ema50: number;
  ema20ChangeOver5Bars: number;
  atr14Sma: number;
  recent24BarRange: { low: number; high: number };
  confirmedSwingStructure:
    "higher_highs_higher_lows" | "lower_highs_lower_lows" | "mixed_or_insufficient";
  recentClosedCandles: Candle[];
};
export type VerifiedBtcLiquiditySnapshot = {
  sourceUrl: string;
  chartUrl: string;
  capturedAt: string;
  tickerCloseTimeUtc: string;
  currentPrice: number;
  symbol: "BTCUSDT" | "BTCUSD";
  quoteAsset: "USDT" | "USD";
  marketExchange: "Binance" | "Kraken" | "OKX" | "Bitstamp";
  sourceEndpoints: string[];
  frames: BtcTechnicalFrame[];
  unavailableFrames?: Array<{ interval: Timeframe; reason: "provider_interval_unsupported" }>;
  candidates: BtcLiquidityCandidate[];
  pageText: string;
};

/** Dedicated BTC chart marker. It never supplies a request destination. */
export function isBtcLiquiditySource(raw: string): boolean {
  try {
    const url = new URL(raw);
    if (url.protocol !== "https:" || url.username || url.password || url.port) return false;
    return (
      (url.hostname === "www.binance.com" &&
        /^\/(?:[a-z]{2}\/)?trade\/BTC_USDT\/?$/.test(url.pathname)) ||
      (url.hostname === "data-api.binance.vision" &&
        url.pathname === "/api/v3/ticker/24hr" &&
        url.searchParams.get("symbol") === "BTCUSDT")
    );
  } catch {
    return false;
  }
}

const INVALID_SOURCE_MESSAGE = "נתוני גרף הביטקוין חסרים, ישנים או אינם עקביים; התיבה לא שונתה.";
type MarketProvider = "Binance" | "Kraken" | "OKX" | "Bitstamp";
type ValidationStage =
  | "provider_response"
  | "capture_timing"
  | "quote"
  | "quote_freshness"
  | `${Timeframe}_candles`
  | "price_consistency";
type ValidationReason =
  | "stale_quote"
  | "future_quote"
  | "provider_envelope"
  | "malformed_result"
  | "provider_error"
  | "provider_rate_limited"
  | "provider_invalid_arguments"
  | "provider_unavailable"
  | "provider_busy"
  | "provider_permission_denied"
  | "provider_internal_error"
  | "provider_unknown_pair"
  | "provider_temporary_lockout"
  | "provider_bad_request"
  | "provider_unknown_method"
  | "provider_feature_disabled"
  | "provider_deadline_elapsed"
  | "pair_shape"
  | "row_count"
  | "cursor"
  | "row_shape"
  | "row_values"
  | "grid"
  | "duration"
  | "future_bar"
  | "ohlc_values"
  | "contiguity"
  | "closed_count"
  | "closed_freshness"
  | "current_bar"
  | "confirmation";
const VALIDATION_FACTS = [
  "row_count",
  "min_count",
  "max_count",
  "row_index",
  "row_width",
  "row_column",
  "error_count",
  "result_key_count",
  "interval_ms",
  "opened_at_ms",
  "closed_at_ms",
  "expected_open_ms",
  "previous_open_ms",
  "last_closed_at_ms",
  "now_ms",
  "age_ms",
  "max_age_ms",
  "cursor_ms",
  "closed_count",
] as const;
const ERROR_SEVERITIES = ["error", "warning", "mixed", "unknown"] as const;
const ERROR_CATEGORIES = [
  "general",
  "api",
  "query",
  "order",
  "trade",
  "funding",
  "service",
  "auth",
  "account",
  "session",
  "bm",
  "mixed",
  "unknown",
] as const;
type ValidationFacts = Partial<Record<(typeof VALIDATION_FACTS)[number], number>> & {
  error_severity?: (typeof ERROR_SEVERITIES)[number];
  error_category?: (typeof ERROR_CATEGORIES)[number];
};
type InvalidSourceError = Error & {
  validationProvider?: MarketProvider;
  verifiedKrakenRateRejection?: boolean;
  validationReason?: ValidationReason;
  validationFacts?: ValidationFacts;
};
const invalid = (reason?: ValidationReason, facts?: ValidationFacts): InvalidSourceError => {
  const error: InvalidSourceError = new Error(INVALID_SOURCE_MESSAGE);
  error.validationReason = reason;
  error.validationFacts = facts;
  return error;
};
const invalidAtStage = (
  provider: MarketProvider,
  stage: ValidationStage,
  reason?: ValidationReason,
  facts?: ValidationFacts,
  verifiedKrakenRateRejection = false,
) => {
  const safeFacts = Object.entries(facts ?? {})
    .filter(
      ([key, value]) =>
        (VALIDATION_FACTS.includes(key as (typeof VALIDATION_FACTS)[number]) &&
          Number.isSafeInteger(value)) ||
        (key === "error_severity" &&
          ERROR_SEVERITIES.includes(value as (typeof ERROR_SEVERITIES)[number])) ||
        (key === "error_category" &&
          ERROR_CATEGORIES.includes(value as (typeof ERROR_CATEGORIES)[number])),
    )
    .map(([key, value]) => `${key}=${value}`)
    .join(" ");
  const error: InvalidSourceError = new Error(
    `${INVALID_SOURCE_MESSAGE} [${provider}/${stage}${reason ? `/${reason}` : ""}]${safeFacts ? ` ${safeFacts}` : ""}`,
  );
  error.validationProvider = provider;
  error.validationReason = reason;
  error.verifiedKrakenRateRejection =
    provider === "Kraken" && reason === "provider_rate_limited" && verifiedKrakenRateRejection;
  return error;
};
function validatedStage<T>(provider: MarketProvider, stage: ValidationStage, read: () => T): T {
  try {
    return read();
  } catch (error) {
    if (error instanceof Error && error.message === INVALID_SOURCE_MESSAGE)
      throw invalidAtStage(
        provider,
        stage,
        (error as InvalidSourceError).validationReason,
        (error as InvalidSourceError).validationFacts,
        (error as InvalidSourceError).verifiedKrakenRateRejection === true,
      );
    throw error;
  }
}
const timestamp = (value: unknown): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) throw invalid();
  return value;
};
const decimal = (value: unknown, allowZero = false): number => {
  if (typeof value !== "string" || !/^\d+(?:\.\d+)?$/.test(value)) throw invalid();
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || (allowZero ? parsed < 0 : parsed <= 0)) throw invalid();
  return parsed;
};
function validatedCandleRow<T>(rowIndex: number, read: () => T): T {
  try {
    return read();
  } catch (error) {
    if (error instanceof Error && error.message === INVALID_SOURCE_MESSAGE) {
      const failure = error as InvalidSourceError;
      throw invalid(failure.validationReason ?? "row_values", {
        ...failure.validationFacts,
        row_index: rowIndex,
      });
    }
    throw error;
  }
}
function validatedCandleValue<T>(column: number, read: () => T): T {
  try {
    return read();
  } catch (error) {
    if (error instanceof Error && error.message === INVALID_SOURCE_MESSAGE)
      throw invalid("row_values", { row_column: column });
    throw error;
  }
}
function candles(payload: unknown, interval: Timeframe, now: number): Candle[] {
  if (!Array.isArray(payload) || payload.length < 60 || payload.length > LIMIT)
    throw invalid("row_count", {
      row_count: Array.isArray(payload) ? payload.length : -1,
      min_count: 60,
      max_count: LIMIT,
    });
  const rows: Candle[] = [];
  for (const [rowIndex, row] of payload.entries()) {
    if (!Array.isArray(row) || row.length !== 12)
      throw invalid("row_shape", {
        row_index: rowIndex,
        row_width: Array.isArray(row) ? row.length : -1,
      });
    rows.push(
      validatedCandleRow(rowIndex, () => {
        const openedAt = validatedCandleValue(0, () => timestamp(row[0]));
        const closedAt = validatedCandleValue(6, () => timestamp(row[6]));
        const open = validatedCandleValue(1, () => decimal(row[1]));
        const high = validatedCandleValue(2, () => decimal(row[2]));
        const low = validatedCandleValue(3, () => decimal(row[3]));
        const close = validatedCandleValue(4, () => decimal(row[4]));
        const volumeBtc = validatedCandleValue(5, () => decimal(row[5], true));
        const quoteVolume = validatedCandleValue(7, () => decimal(row[7], true));
        const takerVolume = validatedCandleValue(9, () => decimal(row[9], true));
        const takerQuoteVolume = validatedCandleValue(10, () => decimal(row[10], true));
        if (
          !Number.isSafeInteger(row[8]) ||
          row[8] < 0 ||
          takerVolume > volumeBtc + 1e-8 ||
          takerQuoteVolume > quoteVolume + 1e-8
        )
          throw invalid("row_values");
        return { openedAt, closedAt, open, high, low, close, volumeBtc };
      }),
    );
  }
  return checkedCandleSeries(rows, interval, now);
}
function checkedCandleSeries(
  rows: Candle[],
  interval: Timeframe,
  now: number,
  provider: MarketProvider = "Binance",
): Candle[] {
  const step = TIMEFRAMES[interval];
  for (const [index, row] of rows.entries()) {
    const facts = {
      row_index: index,
      interval_ms: step,
      opened_at_ms: row.openedAt,
      closed_at_ms: row.closedAt,
      now_ms: now,
    };
    if (row.openedAt !== periodStart(row.openedAt, interval, provider))
      throw invalid("grid", facts);
    if (row.closedAt !== nextPeriod(row.openedAt, interval) - 1) throw invalid("duration", facts);
    if (row.openedAt > now) throw invalid("future_bar", facts);
    if (
      row.high < Math.max(row.open, row.close) ||
      row.low > Math.min(row.open, row.close) ||
      row.low > row.high
    )
      throw invalid("ohlc_values", { row_index: index });
    if (index && row.openedAt !== nextPeriod(rows[index - 1].openedAt, interval))
      throw invalid("contiguity", {
        ...facts,
        previous_open_ms: rows[index - 1].openedAt,
        expected_open_ms: nextPeriod(rows[index - 1].openedAt, interval),
      });
  }
  const closed = rows.filter((row) => row.closedAt < now);
  if (closed.length < 59)
    throw invalid("closed_count", {
      closed_count: closed.length,
      row_count: rows.length,
      min_count: 59,
    });
  const lastClosedAt = closed[closed.length - 1].closedAt;
  const currentPeriodEnd = nextPeriod(periodStart(now, interval, provider), interval);
  const maxAge = currentPeriodEnd - periodStart(now, interval, provider) + 30_000;
  if (
    lastClosedAt < periodStart(now, interval, provider) - 1 - 30_000 ||
    now - lastClosedAt > maxAge
  )
    throw invalid("closed_freshness", {
      last_closed_at_ms: lastClosedAt,
      now_ms: now,
      age_ms: now - lastClosedAt,
      max_age_ms: maxAge,
    });
  return rows;
}
// Fixed classifications from Kraken's documented error codes. Suffix details
// are discarded; unknown responses expose only bounded severity/category enums.
const KRAKEN_ERROR_REASONS: ReadonlyArray<readonly [string, ValidationReason]> = [
  ["EAPI:Rate limit exceeded", "provider_rate_limited"],
  ["EService:Throttled", "provider_rate_limited"],
  ["EOrder:Rate limit exceeded", "provider_rate_limited"],
  ["EOrder:Domain rate limit exceeded", "provider_rate_limited"],
  ["EAuth:Rate limit exceeded", "provider_rate_limited"],
  ["EAuth:Too many requests", "provider_rate_limited"],
  ["EGeneral:Too many requests", "provider_rate_limited"],
  ["EGeneral:Invalid arguments", "provider_invalid_arguments"],
  ["EService:Unavailable", "provider_unavailable"],
  ["EService:Busy", "provider_busy"],
  ["EGeneral:Permission denied", "provider_permission_denied"],
  ["EGeneral:Internal error", "provider_internal_error"],
  ["EQuery:Unknown asset pair", "provider_unknown_pair"],
  ["EGeneral:Temporary lockout", "provider_temporary_lockout"],
  ["EAPI:Bad request", "provider_bad_request"],
  ["EGeneral:Unknown Method", "provider_unknown_method"],
  ["EAPI:Feature disabled", "provider_feature_disabled"],
  ["EService:Deadline elapsed", "provider_deadline_elapsed"],
];
function krakenErrorFacts(errors: unknown[]): ValidationFacts {
  const prefixes = errors.map((value) =>
    typeof value === "string" ? /^([EW])([A-Za-z]+):/.exec(value) : null,
  );
  const severities = new Set<(typeof ERROR_SEVERITIES)[number]>(
    prefixes.map((prefix) =>
      prefix?.[1] === "E" ? "error" : prefix?.[1] === "W" ? "warning" : "unknown",
    ),
  );
  const categories = new Set<(typeof ERROR_CATEGORIES)[number]>(
    prefixes.map((prefix) => {
      const category = prefix?.[2].toLowerCase();
      return ERROR_CATEGORIES.includes(category as (typeof ERROR_CATEGORIES)[number]) &&
        category !== "mixed"
        ? (category as (typeof ERROR_CATEGORIES)[number])
        : "unknown";
    }),
  );
  return {
    error_count: errors.length,
    error_severity: severities.size === 1 ? [...severities][0] : "mixed",
    error_category: categories.size === 1 ? [...categories][0] : "mixed",
  };
}
function krakenResult(payload: unknown): Record<string, unknown> {
  if (!payload || typeof payload !== "object" || Array.isArray(payload))
    throw invalid("provider_envelope");
  const envelope = payload as Record<string, unknown>;
  if (!Array.isArray(envelope.error)) throw invalid("provider_envelope");
  if (envelope.error.length) {
    const errors = envelope.error.map((value) =>
      typeof value === "string" ? value.trim().replace(/\s*:\s*/g, ":") : value,
    );
    const known = KRAKEN_ERROR_REASONS.find(([code]) =>
      errors.some(
        (value) => typeof value === "string" && (value === code || value.startsWith(`${code}:`)),
      ),
    );
    const failure = invalid(
      known?.[1] ?? "provider_error",
      known ? { error_count: errors.length } : krakenErrorFacts(errors),
    );
    failure.verifiedKrakenRateRejection = errors.every((value) =>
      KRAKEN_ERROR_REASONS.some(
        ([code, reason]) =>
          reason === "provider_rate_limited" &&
          typeof value === "string" &&
          (value === code || value.startsWith(`${code}:`)),
      ),
    );
    throw failure;
  }
  if (!envelope.result || typeof envelope.result !== "object" || Array.isArray(envelope.result))
    throw invalid("malformed_result");
  const result = envelope.result as Record<string, unknown>;
  if (Object.keys(result).length !== 2 || !("XXBTZUSD" in result) || !("last" in result))
    throw invalid("pair_shape", { result_key_count: Object.keys(result).length });
  return result;
}
function krakenCandles(payload: unknown, interval: Timeframe, now: number): Candle[] {
  const result = krakenResult(payload);
  const values = result.XXBTZUSD;
  if (!Array.isArray(values) || values.length < 60 || values.length > LIMIT + 1)
    throw invalid("row_count", {
      row_count: Array.isArray(values) ? values.length : -1,
      min_count: 60,
      max_count: LIMIT + 1,
    });
  let cursor: number;
  try {
    cursor = timestamp(result.last) * 1000;
  } catch {
    throw invalid("cursor");
  }
  if (cursor > now + 30_000) throw invalid("cursor", { cursor_ms: cursor, now_ms: now });
  const step = TIMEFRAMES[interval];
  const rows = values.map((row, index): Candle =>
    validatedCandleRow(index, () => {
      if (!Array.isArray(row) || row.length !== 8)
        throw invalid("row_shape", { row_width: Array.isArray(row) ? row.length : -1 });
      if (!Number.isSafeInteger(row[7]) || row[7] < 0)
        throw invalid("row_values", { row_column: 7 });
      const openedAt = validatedCandleValue(0, () => {
        const value = timestamp(row[0]) * 1000;
        if (!Number.isSafeInteger(value)) throw invalid();
        return value;
      });
      validatedCandleValue(5, () => decimal(row[5], true)); // VWAP is validated, but is not a liquidation amount.
      return {
        openedAt,
        closedAt: nextPeriod(openedAt, interval) - 1,
        open: validatedCandleValue(1, () => decimal(row[1])),
        high: validatedCandleValue(2, () => decimal(row[2])),
        low: validatedCandleValue(3, () => decimal(row[3])),
        close: validatedCandleValue(4, () => decimal(row[4])),
        volumeBtc: validatedCandleValue(6, () => decimal(row[6], true)),
      };
    }),
  );
  const checked = checkedCandleSeries(rows, interval, now, "Kraken");
  // Kraken always includes the current uncommitted bar. Without it we cannot
  // exclude targets already swept in this interval, even with fresh closed bars.
  if (checked[checked.length - 1].openedAt !== periodStart(now, interval, "Kraken"))
    throw invalid("current_bar", {
      opened_at_ms: checked[checked.length - 1].openedAt,
      expected_open_ms: periodStart(now, interval, "Kraken"),
      now_ms: now,
      interval_ms: step,
    });
  return checked.slice(-LIMIT);
}
function krakenQuote(payload: unknown): { price: number; time: number } {
  const result = krakenResult(payload);
  const trades = result.XXBTZUSD;
  if (
    !Array.isArray(trades) ||
    trades.length !== 1 ||
    typeof result.last !== "string" ||
    !/^\d{10,30}$/.test(result.last)
  )
    throw invalid();
  const trade = trades[0];
  if (
    !Array.isArray(trade) ||
    trade.length !== 7 ||
    typeof trade[2] !== "number" ||
    !Number.isFinite(trade[2]) ||
    trade[2] <= 0 ||
    !["b", "s"].includes(trade[3]) ||
    !["m", "l"].includes(trade[4]) ||
    typeof trade[5] !== "string" ||
    !Number.isSafeInteger(trade[6]) ||
    trade[6] < 0
  )
    throw invalid();
  decimal(trade[1], true);
  return { price: decimal(trade[0]), time: timestamp(Math.floor(trade[2] * 1000)) };
}
function okxData(payload: unknown): unknown[] {
  if (!payload || typeof payload !== "object" || Array.isArray(payload))
    throw invalid("provider_envelope");
  const envelope = payload as Record<string, unknown>;
  if (typeof envelope.code !== "string" || typeof envelope.msg !== "string")
    throw invalid("provider_envelope");
  if (envelope.code !== "0")
    throw invalid(envelope.code === "50011" ? "provider_rate_limited" : "provider_error", {
      error_count: 1,
    });
  if (!Array.isArray(envelope.data)) throw invalid("malformed_result");
  return envelope.data;
}
function stringTimestamp(value: unknown): number {
  if (typeof value !== "string" || !/^\d{1,16}$/.test(value)) throw invalid();
  return timestamp(Number(value));
}
function okxQuote(payload: unknown): { price: number; time: number } {
  const trades = okxData(payload);
  if (
    trades.length !== 1 ||
    !trades[0] ||
    typeof trades[0] !== "object" ||
    Array.isArray(trades[0])
  )
    throw invalid();
  const trade = trades[0] as Record<string, unknown>;
  if (
    trade.instId !== "BTC-USDT" ||
    typeof trade.side !== "string" ||
    !["buy", "sell"].includes(trade.side) ||
    typeof trade.tradeId !== "string" ||
    !/^\d{1,40}$/.test(trade.tradeId)
  )
    throw invalid();
  decimal(trade.sz);
  return { price: decimal(trade.px), time: stringTimestamp(trade.ts) };
}
function okxCandles(payload: unknown, interval: Timeframe, now: number): Candle[] {
  const values = okxData(payload);
  if (values.length < 60 || values.length > LIMIT)
    throw invalid("row_count", { row_count: values.length, min_count: 60, max_count: LIMIT });
  const step = TIMEFRAMES[interval];
  const rows = values
    .map((row, index): Candle =>
      validatedCandleRow(index, () => {
        if (!Array.isArray(row) || row.length !== 9)
          throw invalid("row_shape", { row_width: Array.isArray(row) ? row.length : -1 });
        const openedAt = validatedCandleValue(0, () => stringTimestamp(row[0]));
        const volumeBtc = validatedCandleValue(5, () => decimal(row[5], true));
        validatedCandleValue(6, () => decimal(row[6], true));
        validatedCandleValue(7, () => decimal(row[7], true));
        return {
          openedAt,
          closedAt: nextPeriod(openedAt, interval) - 1,
          open: validatedCandleValue(1, () => decimal(row[1])),
          high: validatedCandleValue(2, () => decimal(row[2])),
          low: validatedCandleValue(3, () => decimal(row[3])),
          close: validatedCandleValue(4, () => decimal(row[4])),
          volumeBtc,
        };
      }),
    )
    .reverse(); // OKX returns newest first; do not sort away gaps or wrong ordering.
  const checked = checkedCandleSeries(rows, interval, now, "OKX");
  const confirmations = values.map((row) => (row as unknown[])[8]).reverse();
  for (const [index, row] of checked.entries()) {
    if (confirmations[index] !== (row.closedAt < now ? "1" : "0"))
      throw invalid("confirmation", {
        row_index: index,
        row_column: 8,
        opened_at_ms: row.openedAt,
        now_ms: now,
      });
  }
  if (checked[checked.length - 1].openedAt !== periodStart(now, interval, "OKX"))
    throw invalid("current_bar", {
      opened_at_ms: checked[checked.length - 1].openedAt,
      expected_open_ms: periodStart(now, interval, "OKX"),
      now_ms: now,
      interval_ms: step,
    });
  return checked;
}
function bitstampInteger(value: unknown): number {
  if (typeof value !== "string" || !/^[1-9]\d*$/.test(value)) throw invalid("row_values");
  return timestamp(Number(value));
}
function bitstampQuote(payload: unknown): { price: number; time: number } {
  if (!Array.isArray(payload) || !payload.length) throw invalid("provider_envelope");
  const trades = new Map<number, { price: number; time: number; amount: number; type: string }>();
  let latest: { price: number; time: number; tid: number } | undefined;
  for (const [rowIndex, raw] of payload.entries()) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw))
      throw invalid("row_shape", { row_index: rowIndex });
    validatedCandleRow(rowIndex, () => {
      const row = raw as Record<string, unknown>;
      const time = timestamp(bitstampInteger(row.date) * 1000);
      const tid = bitstampInteger(row.tid);
      const price = decimal(row.price);
      const amount = decimal(row.amount);
      if (row.type !== "0" && row.type !== "1") throw invalid("row_values");
      const prior = trades.get(tid);
      if (
        prior &&
        (prior.price !== price ||
          prior.time !== time ||
          prior.amount !== amount ||
          prior.type !== row.type)
      )
        throw invalid("row_values");
      trades.set(tid, { price, time, amount, type: row.type });
      if (!latest || time > latest.time || (time === latest.time && tid > latest.tid))
        latest = { price, time, tid };
    });
  }
  if (!latest) throw invalid("provider_envelope");
  return { price: latest.price, time: latest.time };
}
function bitstampCandles(payload: unknown, interval: Timeframe, now: number): Candle[] {
  if (!payload || typeof payload !== "object" || Array.isArray(payload))
    throw invalid("provider_envelope");
  const data = (payload as Record<string, unknown>).data;
  if (!data || typeof data !== "object" || Array.isArray(data)) throw invalid("malformed_result");
  const result = data as Record<string, unknown>;
  if (result.pair !== "BTC/USD") throw invalid("pair_shape");
  if (!Array.isArray(result.ohlc) || result.ohlc.length < 60 || result.ohlc.length > LIMIT)
    throw invalid("row_count", {
      row_count: Array.isArray(result.ohlc) ? result.ohlc.length : -1,
      min_count: 60,
      max_count: LIMIT,
    });
  const rows = result.ohlc.map((raw, index) => {
    if (!raw || typeof raw !== "object" || Array.isArray(raw))
      throw invalid("row_shape", { row_index: index });
    return validatedCandleRow(index, () => {
      const row = raw as Record<string, unknown>;
      const openedAt = timestamp(bitstampInteger(row.timestamp) * 1000);
      return {
        openedAt,
        closedAt: timestamp(nextPeriod(openedAt, interval) - 1),
        open: decimal(row.open),
        high: decimal(row.high),
        low: decimal(row.low),
        close: decimal(row.close),
        volumeBtc: decimal(row.volume, true),
      };
    });
  });
  const checked = checkedCandleSeries(rows, interval, now, "Bitstamp");
  const current = checked[checked.length - 1];
  if (current.openedAt !== periodStart(now, interval, "Bitstamp"))
    throw invalid("current_bar", {
      opened_at_ms: current.openedAt,
      expected_open_ms: periodStart(now, interval, "Bitstamp"),
      now_ms: now,
    });
  return checked;
}
const verifiedOkxExhaustions = new WeakSet<Error>();
const verifiedBinanceTlsFailures = new WeakSet<Error>();
type CaptureRetry = {
  used: boolean;
  lastAttemptAt?: number;
  startedAt: number;
  now: () => number;
  wait: (milliseconds: number) => Promise<void>;
};
function httpFailure(
  status: number,
  url: string,
  provider: MarketProvider,
  stage: ValidationStage,
  retryStop?: "invalid_retry_after" | "budget_exceeded" | "exhausted",
  delayMs?: number,
) {
  return new Error(
    `המקור אינו זמין (HTTP ${status}). [${provider}/${stage}/http_${status}] source_url=${url}${retryStop ? ` retry=${retryStop}` : ""}${Number.isSafeInteger(delayMs) ? ` retry_delay_ms=${delayMs}` : ""}`,
  );
}
async function marketRequest(
  url: string,
  request: typeof sourceRequest,
  provider: MarketProvider,
  stage: ValidationStage,
  retry?: CaptureRetry,
) {
  try {
    return await request(url, 128 * 1024, { Accept: "application/json" });
  } catch (error) {
    if (
      provider !== "OKX" ||
      !retry ||
      !(error instanceof SourceHttpError) ||
      error.status !== 429 ||
      error.url !== url
    )
      throw error;
    if (error.retryAfterMs === null)
      throw httpFailure(429, url, provider, stage, "invalid_retry_after");
    const observedAt = retry.now();
    if (!Number.isSafeInteger(observedAt) || observedAt < (retry.lastAttemptAt ?? retry.startedAt))
      throw invalidAtStage(provider, "capture_timing");
    if (retry.used) {
      if (observedAt - retry.startedAt >= 60_000)
        throw httpFailure(429, url, provider, stage, "budget_exceeded");
      const failure: InvalidSourceError = httpFailure(429, url, provider, stage, "exhausted");
      verifiedOkxExhaustions.add(failure);
      throw failure;
    }
    const delay = Math.max(1_100, error.retryAfterMs ?? 5_000);
    if (delay >= 60_000 - (observedAt - retry.startedAt))
      throw httpFailure(429, url, provider, stage, "budget_exceeded", delay);
    retry.used = true;
    await retry.wait(delay);
    const retriedAt = retry.now();
    if (!Number.isSafeInteger(retriedAt) || retriedAt < observedAt)
      throw invalidAtStage(provider, "capture_timing");
    if (retriedAt - retry.startedAt >= 60_000)
      throw httpFailure(429, url, provider, stage, "budget_exceeded", delay);
    retry.lastAttemptAt = retriedAt;
    return marketRequest(url, request, provider, stage, retry);
  }
}
async function publicJson(
  urls: string[],
  request: typeof sourceRequest,
  provider: MarketProvider,
  stages: ValidationStage[],
  retry?: CaptureRetry,
): Promise<unknown[]> {
  const results = await Promise.allSettled(
    urls.map((url, index) => marketRequest(url, request, provider, stages[index], retry)),
  );
  return results.map((result, index) => {
    if (result.status === "rejected") {
      const error = result.reason;
      if (error instanceof SourceTransportError) {
        const failure = new Error(
          `לא ניתן לאמת את החיבור המאובטח למקור. [${provider}/${stages[index]}/tls_hostname_mismatch]`,
        );
        if (provider === "Binance" && error.url === urls[index])
          verifiedBinanceTlsFailures.add(failure);
        throw failure;
      }
      const match =
        error && typeof error === "object" && typeof error.message === "string"
          ? /^המקור אינו זמין \(HTTP (\d{3})\)\.$/.exec(error.message)
          : null;
      // Keep the exact Binance403 gate; other HTTP failures identify only our
      // internally fixed public endpoint, provider, stage and numeric status.
      if (match && !(provider === "Binance" && match[1] === "403"))
        throw httpFailure(Number(match[1]), urls[index], provider, stages[index]);
      throw error;
    }
    if (
      result.value.url !== urls[index] ||
      !/^application\/json(?:\s*;|$)/i.test(result.value.contentType)
    )
      throw invalidAtStage(provider, "provider_response");
    try {
      return JSON.parse(
        new TextDecoder("utf-8", { fatal: true }).decode(result.value.bytes),
      ) as unknown;
    } catch {
      throw invalidAtStage(provider, "provider_response");
    }
  });
}
async function sequentialMarketJson(
  urls: string[],
  stages: ValidationStage[],
  request: typeof sourceRequest,
  wait: (milliseconds: number) => Promise<void>,
  now: () => number,
  startedAt: number,
  provider: "Kraken" | "OKX",
  retry?: CaptureRetry,
): Promise<unknown[]> {
  const checkCaptureBudget = () => {
    const observedAt = now();
    if (
      !Number.isSafeInteger(observedAt) ||
      observedAt < startedAt ||
      observedAt - startedAt > 60_000
    )
      throw invalidAtStage(provider, "capture_timing");
  };
  const payloads: unknown[] = [];
  for (let index = 0; index < urls.length; index++) {
    // Space bounded public requests after completion,
    // and stop on a provider rejection instead of sending the remaining calls.
    if (index) await wait(1_100);
    checkCaptureBudget();
    const [payload] = await publicJson([urls[index]], request, provider, [stages[index]], retry);
    checkCaptureBudget();
    validatedStage(provider, stages[index], () =>
      provider === "Kraken" ? krakenResult(payload) : okxData(payload),
    );
    payloads.push(payload);
  }
  return payloads;
}
function sourceHttp403(error: unknown): boolean {
  return Boolean(
    error &&
    typeof error === "object" &&
    "message" in error &&
    typeof error.message === "string" &&
    /^המקור אינו זמין \(HTTP 403\)\.$/.test(error.message),
  );
}
const round = (value: number) => Number(value.toFixed(8));
function ema(rows: Candle[], period: number): number[] {
  let value = rows[0].close;
  const alpha = 2 / (period + 1);
  return rows.map((row) => (value += alpha * (row.close - value)));
}
type Pivot = { index: number; price: number; openedAt: number };
function pivots(rows: Candle[], side: "high" | "low"): Pivot[] {
  const result: Pivot[] = [];
  for (let index = Math.max(2, rows.length - 60); index < rows.length - 2; index++) {
    const price = rows[index][side];
    const neighbours = [rows[index - 2], rows[index - 1], rows[index + 1], rows[index + 2]];
    if (
      neighbours.every((row) => (side === "high" ? price >= row[side] : price <= row[side])) &&
      neighbours.some((row) => (side === "high" ? price > row[side] : price < row[side]))
    )
      result.push({ index, price, openedAt: rows[index].openedAt });
  }
  return result;
}
function describeFrame(rows: Candle[], interval: Timeframe): BtcTechnicalFrame {
  const closed = rows;
  const latest = closed[closed.length - 1];
  const fast = ema(closed, 20);
  const slow = ema(closed, 50);
  const ranges = closed.slice(-14).map((row, index) => {
    const previous = closed[closed.length - 15 + index];
    return Math.max(
      row.high - row.low,
      Math.abs(row.high - previous.close),
      Math.abs(row.low - previous.close),
    );
  });
  const highs = pivots(closed, "high").slice(-2);
  const lows = pivots(closed, "low").slice(-2);
  let structure: BtcTechnicalFrame["confirmedSwingStructure"] = "mixed_or_insufficient";
  if (highs.length === 2 && lows.length === 2) {
    if (highs[1].price > highs[0].price && lows[1].price > lows[0].price)
      structure = "higher_highs_higher_lows";
    if (highs[1].price < highs[0].price && lows[1].price < lows[0].price)
      structure = "lower_highs_lower_lows";
  }
  const recent = closed.slice(-24);
  return {
    interval,
    closedCandleCount: closed.length,
    latestClosedAt: new Date(latest.closedAt).toISOString(),
    lastClose: latest.close,
    ema20: round(fast[fast.length - 1]),
    ema50: round(slow[slow.length - 1]),
    ema20ChangeOver5Bars: round(fast[fast.length - 1] - fast[fast.length - 6]),
    atr14Sma: round(ranges.reduce((sum, range) => sum + range, 0) / ranges.length),
    recent24BarRange: {
      low: Math.min(...recent.map((row) => row.low)),
      high: Math.max(...recent.map((row) => row.high)),
    },
    confirmedSwingStructure: structure,
    recentClosedCandles: closed.slice(-48),
  };
}
function targetCandidates(
  rows: Candle[],
  frame: BtcTechnicalFrame,
  current: number,
  observedRows: Candle[],
  quoteAsset: "USDT" | "USD",
): BtcLiquidityCandidate[] {
  const result: Array<Omit<BtcLiquidityCandidate, "id" | "evidence">> = [];
  // This is a grouping tolerance, not a claimed liquidation price or amount.
  const tolerance = Math.max(current * 0.0005, frame.atr14Sma * 0.1);
  for (const edge of ["high", "low"] as const) {
    const points = pivots(rows, edge).sort((a, b) => a.price - b.price);
    const groups: Pivot[][] = [];
    for (const point of points) {
      const previous = groups[groups.length - 1];
      if (previous && point.price - previous[0].price <= tolerance) previous.push(point);
      else groups.push([point]);
    }
    for (const group of groups) {
      const low = group[0].price;
      const high = group[group.length - 1].price;
      const latestIndex = Math.max(...group.map((point) => point.index));
      // Exclude clearly exceeded structures; matching touches form equal levels.
      if (
        rows
          .slice(latestIndex + 1)
          .some((row) =>
            edge === "high" ? row.high > high + tolerance : row.low < low - tolerance,
          )
      )
        continue;
      if (edge === "high" ? low <= current : high >= current) continue;
      result.push({
        timeframe: frame.interval,
        side: edge === "high" ? "above" : "below",
        kind:
          group.length > 1
            ? edge === "high"
              ? "equal_highs"
              : "equal_lows"
            : edge === "high"
              ? "swing_high"
              : "swing_low",
        low,
        high,
        distancePercent: round(
          ((edge === "high" ? low - current : current - high) / current) * 100,
        ),
        touches: group.length,
        candleTimesUtc: group.map((point) => new Date(point.openedAt).toISOString()),
        interpretation: "inferred_stop_area_not_measured_liquidations",
      });
    }
    const level = edge === "high" ? frame.recent24BarRange.high : frame.recent24BarRange.low;
    if (
      (edge === "high" ? level > current : level < current) &&
      !result.some((candidate) => candidate.low <= level && candidate.high >= level)
    ) {
      const matches = rows.slice(-24).filter((row) => row[edge] === level);
      result.push({
        timeframe: frame.interval,
        side: edge === "high" ? "above" : "below",
        kind: edge === "high" ? "recent_range_high" : "recent_range_low",
        low: level,
        high: level,
        distancePercent: round((Math.abs(level - current) / current) * 100),
        touches: matches.length,
        candleTimesUtc: matches.map((row) => new Date(row.openedAt).toISOString()),
        interpretation: "inferred_stop_area_not_measured_liquidations",
      });
    }
  }
  // Candidate formation and indicators use closed bars only. The current open
  // bar can already have swept an otherwise valid level, so use its observed
  // extremes to remove consumed candidates before selecting the nearest three.
  const unswept = result.filter((candidate) => {
    const formedAt = Math.max(...candidate.candleTimesUtc.map((time) => Date.parse(time)));
    return !observedRows.some(
      (row) =>
        row.openedAt > formedAt &&
        (candidate.side === "above"
          ? row.high > candidate.high + tolerance
          : row.low < candidate.low - tolerance),
    );
  });
  return ["above", "below"]
    .flatMap((side) =>
      unswept
        .filter((candidate) => candidate.side === side)
        .sort((a, b) => a.distancePercent - b.distancePercent)
        .slice(0, 3),
    )
    .map((candidate) => ({
      ...candidate,
      id: `${frame.interval}:${candidate.kind}:${candidate.low}:${candidate.high}`,
      evidence: `${frame.interval} ${candidate.kind}; observed OHLC price bounds ${candidate.low} to ${candidate.high} ${quoteAsset}; ${candidate.touches} confirmed candle observations at ${candidate.candleTimesUtc.join(", ")}. Possible stop area is an inference, no liquidation notional measured.`,
    }));
}

/** Fixed GET-only unauthenticated market-data endpoints. Binance HTTP 403 or a
 * verified TLS hostname failure at the exact fixed Binance endpoint permits
 * a complete Kraken BTC/USD snapshot, then a complete OKX BTC/USDT
 * snapshot only on a verified Kraken rate rejection. Bitstamp BTC/USD requires
 * a verified OKX HTTP 429 after the single retry is exhausted. Providers and quote
 * assets are never mixed. Closed bars form signals, current bars remove swept targets. */
export async function readBtcLiquiditySnapshot(
  raw: string,
  options: {
    request?: typeof sourceRequest;
    now?: () => number;
    wait?: (milliseconds: number) => Promise<void>;
  } = {},
): Promise<VerifiedBtcLiquiditySnapshot> {
  if (!isBtcLiquiditySource(raw)) throw new Error("מקור גרף הביטקוין אינו נתמך.");
  const request = options.request ?? sourceRequest;
  const now = options.now ?? Date.now;
  const wait =
    options.wait ??
    ((milliseconds) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds)));
  const startedAt = now();
  if (!Number.isSafeInteger(startedAt) || startedAt <= 0)
    throw invalidAtStage("Binance", "capture_timing");
  const retry: CaptureRetry = { used: false, startedAt, now, wait };
  let intervals = providerIntervals("Binance");
  const stageList = (): ValidationStage[] => [
    "quote",
    ...intervals.map((interval): ValidationStage => `${interval}_candles`),
  ];
  let stages = stageList();
  let urls = [
    `${API}ticker/24hr?symbol=BTCUSDT&type=FULL`,
    ...intervals.map(
      (interval) => `${API}klines?symbol=BTCUSDT&interval=${interval}&limit=${LIMIT}`,
    ),
  ];
  let payloads: unknown[];
  let kraken = false;
  let okx = false;
  let bitstamp = false;
  let minimumObservedAt = startedAt;
  try {
    payloads = await publicJson(urls, request, "Binance", stages);
  } catch (error) {
    if (
      !sourceHttp403(error) &&
      (!(error instanceof Error) || !verifiedBinanceTlsFailures.has(error))
    )
      throw error;
    kraken = true;
    const queriedAt = now();
    if (!Number.isSafeInteger(queriedAt) || queriedAt < startedAt)
      throw invalidAtStage("Kraken", "capture_timing");
    intervals = providerIntervals("Kraken");
    stages = stageList();
    urls = [
      `${KRAKEN_API}Trades?pair=XBTUSD&count=1`,
      ...intervals.map((interval) => {
        const step = TIMEFRAMES[interval];
        const since = (periodStart(queriedAt, interval, "Kraken") - (LIMIT - 1) * step) / 1000;
        return `${KRAKEN_API}OHLC?pair=XBTUSD&interval=${step / 60_000}&since=${since}`;
      }),
    ];
    try {
      payloads = await sequentialMarketJson(urls, stages, request, wait, now, startedAt, "Kraken");
    } catch (error) {
      if (
        !(error instanceof Error) ||
        (error as InvalidSourceError).validationProvider !== "Kraken" ||
        (error as InvalidSourceError).validationReason !== "provider_rate_limited" ||
        (error as InvalidSourceError).verifiedKrakenRateRejection !== true
      )
        throw error;
      kraken = false;
      okx = true;
      intervals = providerIntervals("OKX");
      stages = stageList();
      urls = [
        `${OKX_API}trades?instId=BTC-USDT&limit=1`,
        ...intervals.map(
          (interval) =>
            `${OKX_API}candles?instId=BTC-USDT&bar=${OKX_BARS[interval]}&limit=${LIMIT}`,
        ),
      ];
      try {
        payloads = await sequentialMarketJson(
          urls,
          stages,
          request,
          wait,
          now,
          startedAt,
          "OKX",
          retry,
        );
      } catch (error) {
        if (!(error instanceof Error) || !verifiedOkxExhaustions.has(error)) throw error;
        const fallbackAt = now();
        if (
          !Number.isSafeInteger(fallbackAt) ||
          fallbackAt < (retry.lastAttemptAt ?? startedAt) ||
          fallbackAt - startedAt >= 60_000
        )
          throw invalidAtStage("Bitstamp", "capture_timing");
        okx = false;
        bitstamp = true;
        minimumObservedAt = fallbackAt;
        intervals = providerIntervals("Bitstamp");
        stages = stageList();
        urls = [
          `${BITSTAMP_API}transactions/btcusd/?time=minute`,
          ...intervals.map(
            (interval) =>
              `${BITSTAMP_API}ohlc/btcusd/?step=${TIMEFRAMES[interval] / 1000}&limit=${LIMIT}&exclude_current_candle=false`,
          ),
        ];
        payloads = await publicJson(urls, request, "Bitstamp", stages);
      }
    }
  }
  const marketExchange: MarketProvider = bitstamp
    ? "Bitstamp"
    : okx
      ? "OKX"
      : kraken
        ? "Kraken"
        : "Binance";
  const observedAt = now();
  if (
    !Number.isSafeInteger(observedAt) ||
    observedAt < minimumObservedAt ||
    observedAt - startedAt > 60_000
  )
    throw invalidAtStage(marketExchange, "capture_timing");
  const quote = validatedStage(marketExchange, "quote", () => {
    if (bitstamp) return bitstampQuote(payloads[0]);
    if (okx) return okxQuote(payloads[0]);
    if (kraken) return krakenQuote(payloads[0]);
    const ticker = payloads[0];
    if (!ticker || typeof ticker !== "object" || Array.isArray(ticker)) throw invalid();
    const record = ticker as Record<string, unknown>;
    if (record.symbol !== "BTCUSDT") throw invalid();
    return { price: decimal(record.lastPrice), time: timestamp(record.closeTime) };
  });
  const currentPrice = quote.price;
  const tickerTime = quote.time;
  if (tickerTime > observedAt + 30_000)
    throw invalidAtStage(marketExchange, "quote_freshness", "future_quote");
  if (observedAt - tickerTime > 120_000)
    throw invalidAtStage(marketExchange, "quote_freshness", "stale_quote");
  const parsed = intervals.map((interval, index) =>
    validatedStage(marketExchange, `${interval}_candles`, () =>
      bitstamp
        ? bitstampCandles(payloads[index + 1], interval, observedAt)
        : okx
          ? okxCandles(payloads[index + 1], interval, observedAt)
          : kraken
            ? krakenCandles(payloads[index + 1], interval, observedAt)
            : candles(payloads[index + 1], interval, observedAt),
    ),
  );
  const closed = parsed.map((rows) => rows.filter((row) => row.closedAt < observedAt));
  const frames = closed.map((rows, index) => describeFrame(rows, intervals[index]));
  const latestHour = parsed[0][parsed[0].length - 1];
  const priceTolerance = Math.max(currentPrice * 0.0025, frames[0].atr14Sma * 0.2);
  if (
    currentPrice < latestHour.low - priceTolerance ||
    currentPrice > latestHour.high + priceTolerance
  )
    throw invalidAtStage(marketExchange, "price_consistency");
  const quoteAsset = kraken || bitstamp ? ("USD" as const) : ("USDT" as const);
  const symbol = kraken || bitstamp ? ("BTCUSD" as const) : ("BTCUSDT" as const);
  const snapshot = {
    sourceUrl: kraken || okx || bitstamp ? urls[0] : new URL(raw).href,
    chartUrl: bitstamp
      ? BITSTAMP_CHART_URL
      : okx
        ? OKX_CHART_URL
        : kraken
          ? KRAKEN_CHART_URL
          : BTC_LIQUIDITY_CHART_URL,
    capturedAt: new Date(observedAt).toISOString(),
    tickerCloseTimeUtc: new Date(tickerTime).toISOString(),
    currentPrice,
    symbol,
    quoteAsset,
    marketExchange,
    sourceEndpoints: urls,
    frames,
    unavailableFrames: BTC_LIQUIDITY_TIMEFRAMES.filter(
      (interval) => !intervals.includes(interval),
    ).map((interval) => ({ interval, reason: "provider_interval_unsupported" as const })),
    candidates: frames.flatMap((frame, index) =>
      targetCandidates(closed[index], frame, currentPrice, parsed[index], quoteAsset),
    ),
  };
  return {
    ...snapshot,
    pageText: JSON.stringify({
      ...snapshot,
      methodology: `${marketExchange} ${symbol} spot OHLC: at most 120 retained candles per available 15m/1h/4h/1d/1w/1M frame; calendar monthly bars are native UTC calendar months, Binance/OKX weekly bars open Monday UTC and Kraken weekly bars open Thursday UTC; unsupported provider intervals are explicitly unavailable and never substituted; ${bitstamp ? "Bitstamp public BTC/USD transactions and native UTC OHLC are a complete single-exchange fallback after the single allowed retry for a verified OKX HTTP 429 is exhausted, in USD; " : okx ? "OKX public recent-trade price/timestamp and native 15m/1H/4H/1Dutc/1Wutc/1Mutc candles are a complete single-exchange fallback after confirmed Binance unavailability (HTTP 403 or a verified TLS hostname failure) and a verified Kraken rate rejection, in USDT; " : kraken ? "Kraken public recent-trade price/timestamp and OHLC are a complete single-exchange fallback after confirmed Binance unavailability (HTTP 403 or a verified TLS hostname failure), in USD, not USDT; " : ""}open candles excluded from indicator calculation and candidate formation, but their observed extremes remove already swept candidates; EMA20/EMA50 seeded from first available close; ATR14 is simple mean true range; confirmed swings use two closed bars on each side; equal-level grouping and sweep tolerance=max(0.05% spot,10% timeframe ATR); closest three remaining candle-based candidates per side/timeframe. No measured stop orders, liquidation sizes, order-book depth, market-news context or calibrated probabilities are supplied.`,
      sources: urls,
    }),
  };
}

export const BTC_LIQUIDITY_ANALYSIS_INSTRUCTIONS =
  "This field requests YOUR concise technical scenario about where BTC price may reach. Analyze only the verified spot chart across 15m/1h/4h/1d/1w/1M and its actual marketExchange, symbol and quoteAsset; never mix failed providers or pairs. Produce one overall synthesis considering agreement and conflicts across all available frames, plus a separate frame-specific judgment for every requested timeframe. Only use a candidate from that same timeframe in its individual judgment; if source marks the frame unavailable or no unswept candidate exists, emit a missing result, never borrow another timeframe or invent a target. Choose one best-supported supplied overall destination candidate, favoring nearby unswept structures over distant extremes unless observed structure justifies otherwise. Explain in short, plain causal Hebrew why the observed trend, confirmed swings, momentum or distance favors a rise or fall toward that target; merely listing upper/lower levels or technical labels is not a scenario. Add an opposite-side alternative only if a concrete chart-observable condition would make it relevant; otherwise omit it. Preserve an opposite-side candle-based invalidation in structured metadata. The visible field is a short estimated scenario, its source-owned high/low target or range, optional 15m/1H/4H/1D/1W/1M label and a relevant brief reason. Do not include snapshot time, current price, source/exchange boilerplate, evidence, kind, JSON/code fields or data dumps in the explanation. Where evidence is balanced, set a weak conditional preference. Candle highs/lows imply possible stop areas, not measured liquidations; do not claim liquidation amounts, visible orders, unprovided news/derivatives data, probabilities or guaranteed movement. Target and invalidation prices must come from supplied candidate IDs. No entry, position-size or leverage advice.";
