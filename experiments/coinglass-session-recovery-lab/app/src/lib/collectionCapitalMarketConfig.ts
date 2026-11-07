import { CAPITAL_MARKET_ASSETS } from "./collectionCapitalMarket.ts";

/** The table has fixed instruments and sources; instructions cannot change its feeds. */
export const CAPITAL_MARKET_SOURCE_URLS = [
  ...new Set(CAPITAL_MARKET_ASSETS.flatMap((asset) => [asset.quote_url, asset.correlation_url])),
];

export function capitalMarketTemplateSettings(settings: Record<string, unknown>) {
  const raw = settings.autofill;
  const autofill =
    raw && typeof raw === "object" && !Array.isArray(raw) ? (raw as Record<string, unknown>) : {};
  return {
    ...settings,
    autofill: { enabled: true, interval_hours: 24, ...autofill },
    autofill_source_urls: CAPITAL_MARKET_SOURCE_URLS,
  };
}
