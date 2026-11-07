/** Only the public, current Newhedge altcoin snapshot has this adapter. */
export function isNewhedgeAltcoinSource(raw: string): boolean {
  try {
    const url = new URL(raw);
    return (
      url.protocol === "https:" &&
      url.hostname === "newhedge.io" &&
      !url.username &&
      !url.password &&
      !url.port &&
      /^\/bitcoin\/altcoins-correlation\/?$/.test(url.pathname)
    );
  } catch {
    return false;
  }
}

/** Read the labelled latest snapshot, never a navigation tile or historical chart.
 * Ambiguous layouts and percentage values remain unreadable; no guessing/conversion. */
export function readNewhedgeAltcoinSnapshot(raw: string): { alt: number; evidence: string } | null {
  const text = raw.replace(/\s+/g, " ").trim();
  const snapshots = [...text.matchAll(/Latest Altcoins Correlation snapshot/g)];
  if (snapshots.length !== 1) return null;
  const section = text.slice(snapshots[0].index! + snapshots[0][0].length);
  const boundary = section.indexOf("Altcoins Marketcap");
  if (boundary < 0) return null;
  const snapshot = section
    .slice(0, boundary)
    .replace(/#{1,6}\s*$/, "")
    .trim();
  if ([...snapshot.matchAll(/Altcoins Correlation/g)].length !== 1) return null;
  const match = snapshot.match(/Altcoins Correlation\s*([+-]?(?:\d+(?:\.\d+)?|\.\d+))\s*$/);
  if (!match) return null;
  const alt = Number(match[1]);
  if (!Number.isFinite(alt) || Math.abs(alt) > 1) return null;
  return { alt, evidence: match[0].trim() };
}

export function isTradingViewChart(raw: string): boolean {
  try {
    const url = new URL(raw);
    return (
      /^(?:(?:www|[a-z]{2})\.)?tradingview\.com$/.test(url.hostname) &&
      /^\/chart(?:\/|$)/.test(url.pathname)
    );
  } catch {
    return false;
  }
}

/** A public daily chart of the configured symbol, independent of a private layout. */
export function tradingViewDominanceChartUrl(raw: string): string | null {
  try {
    const url = new URL(raw);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      !/^(?:(?:www|[a-z]{2})\.)?tradingview\.com$/.test(url.hostname) ||
      !/^\/chart(?:\/|$)/.test(url.pathname)
    )
      return null;
    const symbol = url.searchParams.get("symbol");
    if (symbol !== "CRYPTOCAP:BTC.D" && symbol !== "CRYPTOCAP:USDT.D") return null;
    return `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(symbol)}&interval=D`;
  } catch {
    return null;
  }
}

/** Keep the configured stablecoin expression, including each listed component. */
export function tradingViewStablecoinCompositeChartUrl(raw: string): string | null {
  try {
    const url = new URL(raw);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      !/^(?:(?:www|[a-z]{2})\.)?tradingview\.com$/.test(url.hostname) ||
      !isTradingViewChart(raw)
    )
      return null;
    // Existing links contain literal '+' separators, decoded as spaces by URL.
    const parts = (url.searchParams.get("symbol") ?? "").trim().split(/\s*\+\s*|\s+/);
    if (
      parts.length < 2 ||
      parts.length > 3 ||
      new Set(parts).size !== parts.length ||
      parts.some((part) => !/^CRYPTOCAP:(?:USDT|USDC|DAI)\.D$/.test(part))
    )
      return null;
    return `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(parts.join("+"))}&interval=D`;
  } catch {
    return null;
  }
}

/** Public overview of the same BTC.D symbol, with current period changes. */
export function tradingViewDominanceOverviewUrl(raw: string): string | null {
  try {
    const url = new URL(raw);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      !/^(?:(?:www|[a-z]{2})\.)?tradingview\.com$/.test(url.hostname)
    )
      return null;
    if (
      !(isTradingViewChart(raw) && url.searchParams.get("symbol") === "CRYPTOCAP:BTC.D") &&
      !/^\/symbols\/(?:CRYPTOCAP-)?BTC\.D\/?$/.test(url.pathname)
    )
      return null;
    return `https://${url.hostname}/symbols/BTC.D/`;
  } catch {
    return null;
  }
}

export type DominanceSnapshot = {
  dominancePercent: number;
  changes: { "1D": number | null; "1W": number | null; "1M": number | null };
  evidence: string;
};

/** The header is current; ideas and historical quotes below it are excluded. */
export function readTradingViewDominanceSnapshot(raw: string): DominanceSnapshot | null {
  const text = raw
    .replace(/[\u200e\u200f\u202a-\u202e\u2066-\u2069\ufeff]/g, "")
    .replace(/\u2212/g, "-")
    .replace(/[*_]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  const head = text.split(/(?:נקודות נתונים מרכזיות|Key data points|רעיונות|Ideas)/i)[0];
  if (!/Market Cap BTC Dominance,?\s*%|BTC\.D/.test(head)) return null;
  const current =
    head.match(/BTC\.D\s+(\d+(?:\.\d+)?)\s*%/) ??
    head.match(
      /Crypto Market Capitalization(?:\s+(?:Market open|שוק פתוח))?\s+(\d+(?:\.\d+)?)\s*%/,
    );
  if (!current || Number(current[1]) < 0 || Number(current[1]) > 100) return null;
  const patterns = {
    "1D": /(?:יום\s*1|1\s*day)\s*([+-]?\d+(?:\.\d+)?)\s*%/i,
    "1W": /(?:שבוע\s*1|1\s*week)\s*([+-]?\d+(?:\.\d+)?)\s*%/i,
    "1M": /(?:חודש\s*1|1\s*month)\s*([+-]?\d+(?:\.\d+)?)\s*%/i,
  };
  const changes = Object.fromEntries(
    Object.entries(patterns).map(([period, pattern]) => {
      const matches = [...head.matchAll(new RegExp(pattern.source, "gi"))];
      const values = matches.map((match) => Number(match[1]));
      return [
        period,
        values.length && values.every((value) => Number.isFinite(value) && value === values[0])
          ? values[0]
          : null,
      ];
    }),
  ) as DominanceSnapshot["changes"];
  return { dominancePercent: Number(current[1]), changes, evidence: current[0] };
}

export const DOMINANCE_SNAPSHOT_SCRIPT = `(() => {
 try {
  if(!/^(?:(?:www|[a-z]{2})\\.)?tradingview\\.com$/.test(location.hostname)||!/^\\/symbols\\/BTC\\.D\\/?$/.test(location.pathname)) return JSON.stringify({stage:'dominance',ok:false});
  const read=(${readTradingViewDominanceSnapshot.toString()});
  const snapshot=read(document.querySelector('main')?.innerText||'');
  return JSON.stringify({stage:'dominance',ok:!!snapshot,snapshot});
 } catch {return JSON.stringify({stage:'dominance',ok:false});}
})()`;

export function verifiedDominanceSnapshot(returns: unknown[]): DominanceSnapshot | null {
  const entries = returns
    .map((raw) => {
      const value = raw && typeof raw === "object" && "value" in raw ? raw.value : raw;
      try {
        return typeof value === "string" ? JSON.parse(value) : value;
      } catch {
        return null;
      }
    })
    .filter((raw) => raw?.stage === "dominance");
  if (entries.length !== 1 || entries[0]?.ok !== true) return null;
  const snapshot = entries[0].snapshot;
  if (
    !snapshot ||
    typeof snapshot.dominancePercent !== "number" ||
    !Number.isFinite(snapshot.dominancePercent) ||
    snapshot.dominancePercent < 0 ||
    snapshot.dominancePercent > 100
  )
    return null;
  const changes = Object.fromEntries(
    ["1D", "1W", "1M"].map((period) => [
      period,
      typeof snapshot.changes?.[period] === "number" && Number.isFinite(snapshot.changes[period])
        ? snapshot.changes[period]
        : null,
    ]),
  ) as DominanceSnapshot["changes"];
  return {
    dominancePercent: snapshot.dominancePercent,
    changes,
    evidence: `BTC.D ${snapshot.dominancePercent}%`,
  };
}

/** A verified US Bitcoin ETF chart is not interchangeable with ETH or other fund flows. */
export function isSosovalueBitcoinEtfSource(raw: string): boolean {
  try {
    const url = new URL(raw);
    return (
      url.protocol === "https:" &&
      url.hostname === "sosovalue.com" &&
      !url.username &&
      !url.password &&
      !url.port &&
      url.pathname === "/assets/etf/Total_Crypto_Spot_ETF_Fund_Flow" &&
      url.searchParams.size === 1 &&
      url.searchParams.get("page") === "usBTC"
    );
  } catch {
    return false;
  }
}
