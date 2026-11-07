export {
  collectionNewsSourceIdentity,
  uniqueCollectionNewsSources,
  obviouslyNonMarketNews,
} from "./collectionNewsIdentity.ts";

export const COLLECTION_NEWS_RELEVANCE_LEVELS = [
  "most_important",
  "very_important",
  "important",
  "useful",
  "borderline",
  "irrelevant",
  "completely_irrelevant",
] as const;
export type CollectionNewsRelevanceLevel = (typeof COLLECTION_NEWS_RELEVANCE_LEVELS)[number];
export type CollectionNewsRelevance = {
  v: 1;
  score: number;
  level: CollectionNewsRelevanceLevel;
  reason: string;
};
export const COLLECTION_NEWS_RELEVANCE_LABELS: Record<CollectionNewsRelevanceLevel, string> = {
  most_important: "חשוב ביותר",
  very_important: "חשוב מאוד",
  important: "חשוב",
  useful: "מידע שימושי",
  borderline: "גבולי / לא מועיל",
  irrelevant: "לא רלוונטי",
  completely_irrelevant: "בכלל לא רלוונטי",
};
export const NEWS_RELEVANCE_ATTRIBUTE = "data-news-relevance";

/** Versioned plain text metadata: never allow hidden HTML or unchecked provider output. */
export function validCollectionNewsRelevance(value: unknown): value is CollectionNewsRelevance {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const item = value as Record<string, unknown>;
  const bands: Record<string, [number, number]> = {
    most_important: [90, 100],
    very_important: [75, 89],
    important: [60, 74],
    useful: [40, 59],
    borderline: [20, 39],
    irrelevant: [5, 19],
    completely_irrelevant: [0, 4],
  };
  const band = typeof item.level === "string" ? bands[item.level] : undefined;
  return (
    Object.keys(item).sort().join(",") === "level,reason,score,v" &&
    item.v === 1 &&
    typeof item.score === "number" &&
    Number.isInteger(item.score) &&
    item.score >= 0 &&
    item.score <= 100 &&
    COLLECTION_NEWS_RELEVANCE_LEVELS.includes(item.level as CollectionNewsRelevanceLevel) &&
    !!band &&
    item.score >= band[0] &&
    item.score <= band[1] &&
    typeof item.reason === "string" &&
    item.reason.trim().length > 0 &&
    item.reason.length <= 160 &&
    !/[<>]/.test(item.reason) &&
    !Array.from(item.reason).some(
      (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
    )
  );
}

export function validCollectionNewsRelevanceAttribute(raw: string): boolean {
  if (typeof raw !== "string" || raw.length > 3000) return false;
  try {
    return validCollectionNewsRelevance(JSON.parse(decodeURIComponent(raw)));
  } catch {
    return false;
  }
}

export function readCollectionNewsRelevance(
  html: string | null | undefined,
): CollectionNewsRelevance | null {
  if (typeof html !== "string") return null;
  const outer = /<div\b[^>]*\bdata-autofill-id\s*=\s*(?:"[^"]*"|'[^']*')[^>]*>/i.exec(html);
  const encoded = outer && /\bdata-news-relevance\s*=\s*(?:"([^"]*)"|'([^']*)')/i.exec(outer[0]);
  const raw = encoded?.[1] ?? encoded?.[2];
  if (!raw || !validCollectionNewsRelevanceAttribute(raw)) return null;
  return JSON.parse(decodeURIComponent(raw)) as CollectionNewsRelevance;
}

export function attachCollectionNewsRelevance(
  html: string,
  assessment: CollectionNewsRelevance,
): string {
  if (!validCollectionNewsRelevance(assessment)) throw new Error("הערכת הרלוונטיות אינה תקינה.");
  const marker = /<div\b[^>]*\bdata-autofill-id\s*=\s*(?:"[^"]*"|'[^']*')[^>]*>/i.exec(html);
  if (!marker) throw new Error("לא נמצא אייטם אוטומטי לשמירת הערכת הרלוונטיות.");
  const tag = marker[0].replace(/\sdata-news-relevance\s*=\s*(?:"[^"]*"|'[^']*')/gi, "");
  const encoded = encodeURIComponent(JSON.stringify(assessment));
  return (
    html.slice(0, marker.index) +
    tag.slice(0, -1) +
    ` ${NEWS_RELEVANCE_ATTRIBUTE}="${encoded}">` +
    html.slice(marker.index + marker[0].length)
  );
}

/** Distinct reviewed articles, not repeated clicks, must justify hiding borderline content. */
export function collectionBorderlineFilterReady(predictions: unknown): boolean {
  if (!predictions || typeof predictions !== "object") return false;
  const data = predictions as {
    predictedBorderlineUniqueItems?: number;
    predictedBorderlineDiscardedItems?: number;
  };
  const total = data.predictedBorderlineUniqueItems;
  const rejected = data.predictedBorderlineDiscardedItems;
  if (
    !Number.isInteger(total) ||
    !Number.isInteger(rejected) ||
    total! < 20 ||
    rejected! < 0 ||
    rejected! > total!
  )
    return false;
  return rejected! / total! >= 0.9;
}

/** Group post URLs by account and article URLs by publication, never by a unique permalink. */
export function collectionNewsLearningSource(raw: string): string {
  try {
    const url = new URL(raw);
    if (!["https:", "http:"].includes(url.protocol) || url.username || url.password) return "";
    const host = url.hostname.toLowerCase().replace(/^www\./, "");
    if (["x.com", "twitter.com"].includes(host)) {
      const account = url.pathname.split("/").filter(Boolean)[0];
      return account && /^[a-z0-9_]{1,20}$/i.test(account) && account.toLowerCase() !== "i"
        ? `https://x.com/${account.toLowerCase()}`
        : "https://x.com";
    }
    return `https://${host}`;
  } catch {
    return "";
  }
}
