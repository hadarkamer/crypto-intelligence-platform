import {
  capitalMarketSnapshotFromValue,
  readCapitalMarketNotes,
} from "./collectionCapitalMarket.ts";

/** Completion requires the verified quote/change and correlation for every fixed asset. */
export function capitalMarketValueComplete(value: unknown): boolean {
  return capitalMarketSnapshotFromValue(value).rows.every(
    (row) => row.quote !== null && row.correlation !== null,
  );
}

/** Scan metadata and source errors alone must not hide an older filled worksheet. */
export function capitalMarketValuePresent(value: unknown): boolean {
  if (capitalMarketSnapshotFromValue(value).rows.some((row) => row.quote || row.correlation))
    return true;
  const raw =
    value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>).capital_market_notes
      : undefined;
  return Object.values(readCapitalMarketNotes(raw)).some(
    (note) => !!note?.text.replace(/[\s\u200B-\u200D\uFEFF]/g, ""),
  );
}
