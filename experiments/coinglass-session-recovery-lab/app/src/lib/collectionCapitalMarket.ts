export type CapitalMarketRowId = "nasdaq" | "sp500" | "oil";
export type CapitalMarketProxy = "QQQ" | "SPY" | "WTI";
export type CapitalMarketUnit = "points" | "usd_per_barrel";

export const CAPITAL_MARKET_ASSETS: ReadonlyArray<{
  id: CapitalMarketRowId;
  label: string;
  symbol: string;
  proxy: CapitalMarketProxy;
  unit: CapitalMarketUnit;
  quote_url: string;
  correlation_url: string;
}> = [
  {
    id: "nasdaq",
    label: "נאסד״ק 100",
    symbol: "^NDX",
    proxy: "QQQ",
    unit: "points",
    quote_url: "https://finance.yahoo.com/quote/%5ENDX/",
    correlation_url: "https://newhedge.io/bitcoin/bitcoin-correlations",
  },
  {
    id: "sp500",
    label: "S&P 500",
    symbol: "^GSPC",
    proxy: "SPY",
    unit: "points",
    quote_url: "https://finance.yahoo.com/quote/%5EGSPC/",
    correlation_url: "https://newhedge.io/bitcoin/bitcoin-correlations",
  },
  {
    id: "oil",
    label: "נפט WTI — חוזה קרוב",
    symbol: "CL=F",
    proxy: "WTI",
    unit: "usd_per_barrel",
    quote_url: "https://finance.yahoo.com/quote/CL%3DF/",
    correlation_url: "https://newhedge.io/bitcoin/crude-oil-correlation",
  },
];

export type CapitalMarketQuote = {
  price: number;
  previous_close: number;
  /** Relative to the provider's previous trading-session close, not a rolling 24h window. */
  change_percent: number;
  currency: "USD";
  symbol: string;
  unit: CapitalMarketUnit;
  contract_label: string | null;
  source_name: "Yahoo Finance";
  source_url: string;
  as_of: string;
  fetched_at: string;
  exchange_timezone: string;
  market_status: "open" | "closed" | "unknown";
  delay: "may_be_delayed";
};

export type CapitalMarketCorrelation = {
  coefficient: number;
  proxy: CapitalMarketProxy;
  window: "30d";
  source_name: "Newhedge";
  source_url: string;
  /** The public latest-snapshot cards do not publish an observation timestamp. */
  as_of: null;
  fetched_at: string;
};

export type CapitalMarketRow = {
  id: CapitalMarketRowId;
  quote: CapitalMarketQuote | null;
  correlation: CapitalMarketCorrelation | null;
  quote_error?: string;
  correlation_error?: string;
};

export type CapitalMarketSnapshot = {
  version: 1;
  rows: CapitalMarketRow[];
  scanned_at: string | null;
};

export type CapitalMarketNote = {
  text: string;
  updated_at: string | null;
  updated_by: string | null;
};
export type CapitalMarketNotes = Partial<Record<CapitalMarketRowId, CapitalMarketNote>>;
export type CapitalMarketValue = {
  capital_market?: CapitalMarketSnapshot;
  capital_market_notes?: CapitalMarketNotes;
};
export const CAPITAL_MARKET_NOTE_MAX_LENGTH = 2000;

type CellUpdate<T> = { value: T; error?: never } | { error: string; value?: never };
export type CapitalMarketCellUpdates = Partial<
  Record<
    CapitalMarketRowId,
    { quote?: CellUpdate<CapitalMarketQuote>; correlation?: CellUpdate<CapitalMarketCorrelation> }
  >
>;

function record(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
function finite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}
function timestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}T/.test(value) &&
    Number.isFinite(Date.parse(value))
  );
}
function errorText(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.slice(0, 500) : undefined;
}

function quoteValue(
  raw: unknown,
  asset: (typeof CAPITAL_MARKET_ASSETS)[number],
): CapitalMarketQuote | null {
  if (
    !record(raw) ||
    !finite(raw.price) ||
    raw.price <= 0 ||
    !finite(raw.previous_close) ||
    raw.previous_close <= 0 ||
    !finite(raw.change_percent) ||
    Math.abs(raw.change_percent - ((raw.price - raw.previous_close) / raw.previous_close) * 100) >
      0.000001 ||
    raw.symbol !== asset.symbol ||
    raw.unit !== asset.unit ||
    raw.currency !== "USD" ||
    raw.source_name !== "Yahoo Finance" ||
    raw.source_url !== asset.quote_url ||
    !timestamp(raw.as_of) ||
    !timestamp(raw.fetched_at) ||
    typeof raw.exchange_timezone !== "string" ||
    raw.exchange_timezone.length > 100 ||
    !["open", "closed", "unknown"].includes(String(raw.market_status)) ||
    raw.delay !== "may_be_delayed" ||
    (raw.contract_label !== null &&
      (typeof raw.contract_label !== "string" || raw.contract_label.length > 120))
  )
    return null;
  return {
    price: raw.price,
    previous_close: raw.previous_close,
    change_percent: raw.change_percent,
    currency: "USD",
    symbol: asset.symbol,
    unit: asset.unit,
    contract_label: raw.contract_label as string | null,
    source_name: "Yahoo Finance",
    source_url: asset.quote_url,
    as_of: raw.as_of,
    fetched_at: raw.fetched_at,
    exchange_timezone: raw.exchange_timezone,
    market_status: raw.market_status as CapitalMarketQuote["market_status"],
    delay: "may_be_delayed",
  };
}

function correlationValue(
  raw: unknown,
  asset: (typeof CAPITAL_MARKET_ASSETS)[number],
): CapitalMarketCorrelation | null {
  if (
    !record(raw) ||
    !finite(raw.coefficient) ||
    Math.abs(raw.coefficient) > 1 ||
    raw.proxy !== asset.proxy ||
    raw.window !== "30d" ||
    raw.source_name !== "Newhedge" ||
    raw.source_url !== asset.correlation_url ||
    raw.as_of !== null ||
    !timestamp(raw.fetched_at)
  )
    return null;
  return {
    coefficient: raw.coefficient,
    proxy: asset.proxy,
    window: "30d",
    source_name: "Newhedge",
    source_url: asset.correlation_url,
    as_of: null,
    fetched_at: raw.fetched_at,
  };
}

/** Canonical fixed rows; unknown, duplicate or invalid source data stays empty. */
export function readCapitalMarketSnapshot(raw: unknown): CapitalMarketSnapshot {
  if (record(raw) && "capital_market" in raw) raw = raw.capital_market;
  const snapshot = record(raw) && raw.version === 1 ? raw : {};
  const rows = Array.isArray(snapshot.rows) ? snapshot.rows : [];
  return {
    version: 1,
    scanned_at: timestamp(snapshot.scanned_at) ? snapshot.scanned_at : null,
    rows: CAPITAL_MARKET_ASSETS.map((asset) => {
      const matches = rows.filter((row) => record(row) && row.id === asset.id);
      const row = matches.length === 1 ? (matches[0] as Record<string, unknown>) : {};
      return {
        id: asset.id,
        quote: quoteValue(row.quote, asset),
        correlation: correlationValue(row.correlation, asset),
        ...(errorText(row.quote_error) ? { quote_error: errorText(row.quote_error) } : {}),
        ...(errorText(row.correlation_error)
          ? { correlation_error: errorText(row.correlation_error) }
          : {}),
      };
    }),
  };
}

export function capitalMarketSnapshotFromValue(value: unknown): CapitalMarketSnapshot {
  return readCapitalMarketSnapshot(record(value) ? value.capital_market : null);
}

export function readCapitalMarketNotes(raw: unknown): CapitalMarketNotes {
  if (!record(raw)) return {};
  const notes: CapitalMarketNotes = {};
  for (const { id } of CAPITAL_MARKET_ASSETS) {
    const note = raw[id];
    if (!record(note) || typeof note.text !== "string") continue;
    notes[id] = {
      text: note.text.slice(0, CAPITAL_MARKET_NOTE_MAX_LENGTH),
      updated_at: timestamp(note.updated_at) ? note.updated_at : null,
      updated_by: typeof note.updated_by === "string" ? note.updated_by.slice(0, 128) : null,
    };
  }
  return notes;
}

/** A failed cell keeps its own last verified value and its original timestamps.
 * Manual minute observations live outside this snapshot and are never emitted. */
export function mergeCapitalMarketScan(
  previous: unknown,
  updates: CapitalMarketCellUpdates,
  scannedAt: string,
): CapitalMarketSnapshot {
  if (!timestamp(scannedAt)) throw new Error("invalid_capital_market_scan_time");
  const snapshot = readCapitalMarketSnapshot(previous);
  return {
    version: 1,
    scanned_at: scannedAt,
    rows: snapshot.rows.map((row) => {
      const asset = CAPITAL_MARKET_ASSETS.find((asset) => asset.id === row.id)!;
      const update = updates[row.id];
      const result = { ...row };
      if (update?.quote) {
        if (update.quote.value) {
          const value = quoteValue(update.quote.value, asset);
          if (!value) throw new Error("invalid_capital_market_quote_update");
          result.quote = value;
          delete result.quote_error;
        } else result.quote_error = errorText(update.quote.error) ?? "המחיר לא עודכן.";
      }
      if (update?.correlation) {
        if (update.correlation.value) {
          const value = correlationValue(update.correlation.value, asset);
          if (!value) throw new Error("invalid_capital_market_correlation_update");
          result.correlation = value;
          delete result.correlation_error;
        } else
          result.correlation_error = errorText(update.correlation.error) ?? "הקורלציה לא עודכנה.";
      }
      return result;
    }),
  };
}
