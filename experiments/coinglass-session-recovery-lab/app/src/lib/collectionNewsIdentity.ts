/** Stable source identity, independent of summaries, scan runs and X handles. */
export function collectionNewsSourceIdentity(raw: string): string | null {
  if (/^x:status:[1-9][0-9]*$/.test(raw)) return raw;
  try {
    const url = new URL(raw);
    if (url.protocol !== "https:" || url.username || url.password || url.port) return null;
    const host = url.hostname.toLowerCase().replace(/^www\./, "");
    if (["x.com", "twitter.com", "mobile.twitter.com", "mobile.x.com"].includes(host)) {
      const post =
        /^\/(?:[a-z0-9_]{1,15}|i(?:\/web)?)\/status\/([1-9][0-9]*)(?:\/(?:photo|video)\/\d+)?\/?$/i.exec(
          url.pathname,
        );
      return post ? `x:status:${post[1]}` : null;
    }
    url.hostname = host;
    url.hash = "";
    url.pathname = url.pathname.replace(/\/+$/, "") || "/";
    // Preserve functional query parameters. Ignore only recognised tracking;
    // deterministic raw ordering also matches the database's identity helper.
    const query = url.search
      .slice(1)
      .split("&")
      .filter(Boolean)
      .filter((part) => !/^(?:utm_[^=]*|fbclid|gclid)(?:=|$)/i.test(part))
      .sort();
    url.search = query.join("&");
    return url.href;
  } catch {
    return null;
  }
}

/** Source adapters supply the URL. Model summaries and browser HTML never
 * decide identity; overlapping feeds/reposts pay for only one summary. */
export function uniqueCollectionNewsSources<T extends { url: string }>(items: T[]): T[] {
  const unique = new Map<string, T>();
  for (const item of items) {
    const identity = collectionNewsSourceIdentity(item.url);
    if (!identity) throw new Error("לידיעה אין קישור מקור תקין; נקודת הסריקה לא קודמה.");
    if (!unique.has(identity)) unique.set(identity, item);
  }
  return [...unique.values()];
}

const SOCIAL_ONLY =
  /\b(?:gm|gn|good\s+morning|good\s+night|morning\s+(?:everyone|all|fam)|happy\s+(?:weekend|birthday)|have\s+a\s+(?:great|good|nice)\s+(?:day|weekend)|my\s+(?:birthday|wedding|vacation)|coffee\s+time|going\s+to\s+the\s+gym)\b|בוקר\s+טוב|לילה\s+טוב|ברכת\s+בוקר|ברכות\s+(?:בוקר|יומיות)|יום\s+הולדת|סוף\s+שבוע\s+נעים|יום\s+(?:טוב|נעים)/i;
const MARKET_INFORMATION =
  /\b(?:support|resistance|liquidations?|funding|inflows?|outflows?|etfs?|inflation|cpi|pce|gdp|nfp|fomc|interest\s+rates?|rate\s+(?:cuts?|hikes?)|earnings|supply|demand|breakout|consolidat\w*|(?:fed|sec)\s+(?:decision|approv\w*|reject\w*|announc\w*)|(?:btc|bitcoin|eth|ethereum|stocks?|market)\s+(?:is\s+)?(?:up|down|rising|falling|holds?|broke|breaks?|gains?|drops?|rall\w*|crash\w*))\b|תמיכה|התנגדות|הנזל|נזילות|פקיע|זרימ(?:ת|ות)\s+הון|אינפלצי|ריבית|תמ.?ג|תעסוק|מניות|פריצ|דומיננט|(?:ביטקוין|ביטקואין|אתריום|השוק)\s+(?:עול|יורד|עלה|ירד|מחזיק|שבר|פורץ|מדשדש)/i;
// Match complete social utterances, never a short length or market keyword alone.
const X_SOCIAL_REPLY =
  /^(?:(?:same (?:here|way)|yes|yeah|yep|yup|nope|exactly|agreed|indeed|absolutely|true|sure|thanks|thank you|nice|well said|lol|lmao)|(?:the )?(?:volatility|market) waiting room is (?:full|packed)(?: today)?|(?:the )?press conference countdown is (?:basically )?(?:the )?market['’]s whole personality(?: today)?)$/i;

/** Exploration covers marginal MARKET information. Pure greetings/personal
 * updates never enter that policy, even before any feedback has been learned. */
export function obviouslyNonMarketNews(
  raw: string,
  source?: { url: string; images: string[] },
): boolean {
  const text = raw
    .replace(/https?:\/\/\S+|@[\p{L}_][\p{L}\p{N}_]*/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
  // Source context keeps this conservative X-only removal away from transcripts,
  // images, linked evidence and substantive replies/self-thread continuations.
  if (
    source &&
    /^https:\/\/(?:www\.|mobile\.)?(?:x\.com|twitter\.com)\//i.test(source.url) &&
    source.images.length === 0 &&
    !/https?:\/\//i.test(raw) &&
    X_SOCIAL_REPLY.test(text.replace(/[\p{P}\p{S}\s]+$/gu, ""))
  )
    return true;
  if (!SOCIAL_ONLY.test(text)) return false;
  if (MARKET_INFORMATION.test(text)) return false;
  // A greeting may introduce real price/market data without using those terms.
  if (
    /(?:\b(?:btc|bitcoin|eth|ethereum|usd|usdt|gold|oil|nasdaq|price)\b|ביטקוין|אתריום|מחיר)[^.!?\n]{0,80}\d/i.test(
      text,
    )
  )
    return false;
  return true;
}
