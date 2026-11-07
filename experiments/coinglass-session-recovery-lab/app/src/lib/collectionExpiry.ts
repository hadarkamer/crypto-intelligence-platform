/** Selection describes relative expiry size; it is not a price prediction. */
export const EXPIRY_SELECTION_POLICY = "time_local_size_v2";
export const EXPIRY_SELECTION_RULES = Object.freeze({
  primaryExchange: "דריביט",
  horizonDays: 30,
  localWindowDays: 7,
  nearDays: 7,
  localMedianMultiple: 2,
  localPeakShare: 0.25,
  nearMinimumUsd: 300_000_000,
  laterMinimumUsd: 1_000_000_000,
  isolatedNearMinimumUsd: 1_000_000_000,
  isolatedLaterMinimumUsd: 5_000_000_000,
});

export type ExpiryNow = Date | string | number;
export type ExpiryInputCell = {
  exchange: string;
  price?: unknown;
  amount?: unknown;
  amount_usd?: unknown;
};
export type ExpiryInputRow = { date: string; exchanges: readonly ExpiryInputCell[] };
export type ExpiryUrgency =
  "invalid" | "expired" | "today" | "urgent" | "near" | "later" | "distant";

/** Missing stays null, and an explicitly entered or verified zero stays zero. */
export function parseExpiryMoney(value: unknown): number | null {
  if (typeof value === "number") return Number.isFinite(value) && value >= 0 ? value : null;
  if (typeof value !== "string") return null;
  const raw = value.trim();
  const match =
    /^(?:\$\s*|USD\s*)?((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*([KMB])?\s*(?:\$|USD)?$/i.exec(raw);
  if (!match) return null;
  // A currency marker may be leading or trailing, but not both.
  if (/^(?:\$|USD)/i.test(raw) && /(?:\$|USD)$/i.test(raw)) return null;
  const suffix = match[2]?.toUpperCase();
  const multiplier =
    suffix === "K" ? 1_000 : suffix === "M" ? 1_000_000 : suffix === "B" ? 1_000_000_000 : 1;
  const number = Number(match[1].replace(/,/g, "")) * multiplier;
  return Number.isFinite(number) && number >= 0 ? number : null;
}

export function parseExpiryPrice(value: unknown): number | null {
  const number = parseExpiryMoney(value);
  return number !== null && number > 0 ? number : null;
}

/** Display rounding never changes the raw amount or its saved provenance. */
export function formatExpiryUsd(value: unknown): string {
  const number = parseExpiryMoney(value);
  if (number === null) return "";
  let scale =
    number >= 1_000_000_000
      ? 1_000_000_000
      : number >= 1_000_000
        ? 1_000_000
        : number >= 1_000
          ? 1_000
          : 1;
  if (scale < 1_000_000_000 && Math.round((number / scale) * 100) / 100 >= 1_000) scale *= 1_000;
  const suffix =
    scale === 1_000_000_000 ? "B" : scale === 1_000_000 ? "M" : scale === 1_000 ? "K" : "";
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: scale === 1 ? 0 : 2 }).format(number / scale)}${suffix}$`;
}

export function formatExpiryPrice(value: unknown): string {
  const number = parseExpiryPrice(value);
  return number === null
    ? ""
    : new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(number);
}

export function validExpiryDate(date: unknown): date is string {
  if (typeof date !== "string" || date.length !== 10 || !/^\d{4}-\d{2}-\d{2}$/.test(date))
    return false;
  const time = Date.parse(`${date}T00:00:00.000Z`);
  return Number.isFinite(time) && new Date(time).toISOString().slice(0, 10) === date;
}

export function expiryDateInJerusalem(now: ExpiryNow = new Date()): string | null {
  const date = new Date(now instanceof Date ? now.getTime() : now);
  if (!Number.isFinite(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Jerusalem",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((entry) => entry.type === type)?.value;
  return `${part("year")}-${part("month")}-${part("day")}`;
}

/** Calendar days rather than 24-hour intervals, including Jerusalem DST days. */
export function expiryDaysLeft(date: string, now: ExpiryNow = new Date()): number | null {
  const today = expiryDateInJerusalem(now);
  if (!validExpiryDate(date) || !today || !validExpiryDate(today)) return null;
  return Math.round(
    (Date.parse(`${date}T00:00:00Z`) - Date.parse(`${today}T00:00:00Z`)) / 86_400_000,
  );
}

export function expiryUrgency(date: string, now: ExpiryNow = new Date()): ExpiryUrgency {
  const days = expiryDaysLeft(date, now);
  return days === null
    ? "invalid"
    : days < 0
      ? "expired"
      : days === 0
        ? "today"
        : days <= 3
          ? "urgent"
          : days <= 7
            ? "near"
            : days <= 30
              ? "later"
              : "distant";
}

export function isUrgentExpiry(
  date: string,
  significant: boolean,
  now: ExpiryNow = new Date(),
): boolean {
  return significant && expiryUrgency(date, now) === "urgent";
}

function cellAmount(cell: ExpiryInputCell): number | null {
  return parseExpiryMoney(
    Object.prototype.hasOwnProperty.call(cell, "amount_usd") ? cell.amount_usd : cell.amount,
  );
}

export function maximumExpiryAmount(rows: readonly ExpiryInputRow[]): number | null {
  const amounts = rows
    .flatMap((row) => row.exchanges.map(cellAmount))
    .filter((value): value is number => value !== null);
  return amounts.length ? Math.max(...amounts) : null;
}

export function expiryAmountRatio(amount: unknown, maximum: unknown): number | null {
  const value = parseExpiryMoney(amount),
    max = parseExpiryMoney(maximum);
  if (value === null || max === null) return null;
  return max === 0 ? 0 : Math.min(1, value / max);
}

export function expiryDateTotal(row: ExpiryInputRow): {
  amountUsd: number | null;
  complete: boolean;
  populatedExchanges: number;
  expectedExchanges: number;
} {
  const amounts = row.exchanges.map(cellAmount).filter((value): value is number => value !== null);
  const sum = amounts.reduce((total, value) => total + value, 0);
  const expectedExchanges = Math.max(3, row.exchanges.length);
  const amountUsd = amounts.length && Number.isFinite(sum) ? sum : null;
  return {
    amountUsd,
    complete: amounts.length === expectedExchanges && amountUsd !== null,
    populatedExchanges: amounts.length,
    expectedExchanges,
  };
}

export function expiryTotalsAtSamePrice(row: ExpiryInputRow): Array<{
  price: number;
  amountUsd: number;
  exchanges: string[];
  complete: boolean;
}> {
  const allPricesKnown = row.exchanges.every((cell) => parseExpiryPrice(cell.price) !== null);
  const groups = new Map<
    number,
    { price: number; amountUsd: number; exchanges: string[]; complete: boolean; populated: boolean }
  >();
  for (const cell of row.exchanges) {
    const price = parseExpiryPrice(cell.price);
    if (price === null) continue;
    const group = groups.get(price) ?? {
      price,
      amountUsd: 0,
      exchanges: [],
      complete: allPricesKnown,
      populated: false,
    };
    const amount = cellAmount(cell);
    // One exchange contributes once to a shared price, even in malformed
    // legacy rows containing duplicate exchange cells.
    const exchange = cell.exchange.trim();
    if (!exchange || group.exchanges.some((name) => name.toLowerCase() === exchange.toLowerCase()))
      continue;
    group.exchanges.push(exchange);
    if (amount === null) group.complete = false;
    else {
      group.amountUsd += amount;
      group.populated = true;
    }
    groups.set(price, group);
  }
  return [...groups.values()]
    .filter(
      (group) => group.exchanges.length >= 2 && group.populated && Number.isFinite(group.amountUsd),
    )
    .map(({ populated: _populated, ...group }) => group)
    .sort((a, b) => a.price - b.price);
}

/** Deribit leads selection, with a complete three-exchange total as an exception.
 * Nearby peers avoid comparing a daily expiry with a distant monthly giant.
 * A local peak gate suppresses minor dates next to a dominant same-week expiry.
 * Sparse cohorts require more size; incomplete totals never masquerade as zero. */
export function selectSignificantExpiryRows<T extends ExpiryInputRow>(
  rows: readonly T[],
  now: ExpiryNow = new Date(),
): T[] {
  const rules = EXPIRY_SELECTION_RULES;
  const active = rows
    .map((row) => ({ row, days: expiryDaysLeft(row.date, now) }))
    .filter(
      (entry): entry is { row: T; days: number } =>
        entry.days !== null && entry.days >= 0 && entry.days <= rules.horizonDays,
    );
  const amountFor = (row: T, exchange: string | null): number | null => {
    if (exchange !== null) {
      const cell = row.exchanges.find((entry) => entry.exchange === exchange);
      return cell ? cellAmount(cell) : null;
    }
    const total = expiryDateTotal(row);
    return total.complete ? total.amountUsd : null;
  };
  return active
    .filter(({ row, days }) => {
      const nearby = active.filter(
        (entry) =>
          entry.row.date !== row.date && Math.abs(entry.days - days) <= rules.localWindowDays,
      );
      return [rules.primaryExchange, null].some((exchange) => {
        const amount = amountFor(row, exchange);
        const near = days <= rules.nearDays;
        if (amount === null || amount < (near ? rules.nearMinimumUsd : rules.laterMinimumUsd))
          return false;
        const peers = nearby
          .map((entry) => amountFor(entry.row, exchange))
          .filter((value): value is number => value !== null)
          .sort((a, b) => a - b);
        if (!peers.length)
          return amount >= (near ? rules.isolatedNearMinimumUsd : rules.isolatedLaterMinimumUsd);
        const middle = Math.floor(peers.length / 2);
        const median = peers.length % 2 ? peers[middle] : peers[middle - 1] / 2 + peers[middle] / 2;
        return (
          amount / rules.localMedianMultiple >= median &&
          amount >= Math.max(...peers, amount) * rules.localPeakShare
        );
      });
    })
    .map(({ row }) => row)
    .sort((a, b) => a.date.localeCompare(b.date));
}

export function isSignificantExpiry(
  row: ExpiryInputRow,
  rows: readonly ExpiryInputRow[],
  now: ExpiryNow = new Date(),
): boolean {
  if (
    !row.exchanges.some((cell) => {
      const amount = cellAmount(cell);
      return amount !== null && amount > 0;
    })
  )
    return false;
  // Current editable cells supersede that date in the last verified cohort.
  const context = [...rows.filter((entry) => entry.date !== row.date), row];
  return selectSignificantExpiryRows(context, now).some((selected) => selected.date === row.date);
}

/** Accept only the numeric, three-exchange cohort produced by verified expiry
 * captures. Legacy selected provenance is usable when a full cohort is absent. */
export function expirySignificanceRows(
  value: Record<string, unknown>,
  fallbackRows: readonly ExpiryInputRow[],
): readonly ExpiryInputRow[] {
  const metadata = value._autofill;
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) return fallbackRows;
  const info = metadata as Record<string, unknown>;
  if (info.kind !== "expiry") return fallbackRows;
  const candidate = Object.prototype.hasOwnProperty.call(info, "expiry_source_rows")
    ? info.expiry_source_rows
    : info.expiry_rows;
  if (!Array.isArray(candidate) || candidate.length > 1098) return fallbackRows;
  const labels = ["דריביט", "בייננס", "בייביט"];
  const dates = new Set<string>();
  for (const row of candidate) {
    if (
      !row ||
      typeof row !== "object" ||
      Array.isArray(row) ||
      !validExpiryDate(row.date) ||
      dates.has(row.date) ||
      !Array.isArray(row.exchanges) ||
      row.exchanges.length !== labels.length
    )
      return fallbackRows;
    dates.add(row.date);
    for (const [index, cell] of row.exchanges.entries()) {
      if (
        !cell ||
        typeof cell !== "object" ||
        Array.isArray(cell) ||
        cell.exchange !== labels[index] ||
        !(
          cell.price === null ||
          (typeof cell.price === "number" && Number.isFinite(cell.price) && cell.price > 0)
        ) ||
        !(
          cell.amount_usd === null ||
          (typeof cell.amount_usd === "number" &&
            Number.isFinite(cell.amount_usd) &&
            cell.amount_usd >= 0)
        )
      )
        return fallbackRows;
    }
    if (
      !row.exchanges.some(
        (cell: { price: number | null; amount_usd: number | null }) =>
          cell.price !== null || cell.amount_usd !== null,
      )
    )
      return fallbackRows;
  }
  const currentRows = Array.isArray(fallbackRows)
    ? fallbackRows.filter(
        (row) =>
          row &&
          typeof row === "object" &&
          !Array.isArray(row) &&
          validExpiryDate(row.date) &&
          Array.isArray(row.exchanges) &&
          row.exchanges.length === labels.length &&
          row.exchanges.every(
            (cell: ExpiryInputCell, index: number) =>
              cell &&
              typeof cell === "object" &&
              !Array.isArray(cell) &&
              cell.exchange === labels[index],
          ),
      )
    : [];
  const occurrences = new Map<string, number>();
  for (const row of currentRows) occurrences.set(row.date, (occurrences.get(row.date) ?? 0) + 1);
  const overrides = currentRows.filter((row) => occurrences.get(row.date) === 1);
  if (!overrides.length) return candidate as ExpiryInputRow[];
  const byDate = new Map(overrides.map((row) => [row.date, row]));
  // Every current date supersedes its source values, including cleared amounts.
  // Unselected verified peers remain available for nearby-size comparisons.
  return [
    ...(candidate as ExpiryInputRow[]).map((row) => byDate.get(row.date) ?? row),
    ...overrides.filter((row) => !dates.has(row.date)),
  ];
}
