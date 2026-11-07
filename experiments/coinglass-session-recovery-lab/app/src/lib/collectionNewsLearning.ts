export const NEWS_FEEDBACK_LEVELS = [
  "most_important",
  "very_important",
  "important",
  "useful",
  "borderline",
  "irrelevant",
  "completely_irrelevant",
] as const;
export type NewsFeedbackLevel = (typeof NEWS_FEEDBACK_LEVELS)[number];
export type NewsFeedbackCounts = Record<NewsFeedbackLevel, number>;
export type NewsFeedbackSourceStats = {
  sourceUrl: string;
  total: number;
  uniqueItems: number;
  counts: NewsFeedbackCounts;
  averageScore: number | null;
  predictedBorderlineUniqueItems: number;
  predictedBorderlineDiscardedItems: number;
  predictedBorderlineUsefulItems: number;
  borderlineDiscardRate: number | null;
};
export type CollectionNewsLearningContext = {
  version: 1;
  total: number;
  uniqueItems: number;
  counts: NewsFeedbackCounts;
  averageScore: number | null;
  bySource: NewsFeedbackSourceStats[];
  terms: Array<{ term: string; count: number; averageScore: number }>;
  examples: Array<{
    sourceUrl: string;
    summary: string;
    feedbackLevel: NewsFeedbackLevel;
    score: number;
    note: string | null;
    model: Record<string, unknown> | null;
    updatedAt: string;
  }>;
};

/** A source/account identity is stable across individual posts and articles. */
export { collectionNewsLearningSource as canonicalNewsLearningSource } from "./collectionNewsRelevance.ts";

export function newsFeedbackScore(sentiment: string): number | null {
  return (
    (
      {
        very_bullish: 3,
        bullish: 2,
        positive: 1,
        neutral: 0,
        negative: 1,
        bearish: 2,
        very_bearish: 3,
        borderline: -1,
        irrelevant: -2,
        completely_irrelevant: -3,
      } as Record<string, number>
    )[sentiment] ?? null
  );
}

/** Labels/direction remain distinct; importance is symmetric around useful. */
export function newsFeedbackLevel(score: number): NewsFeedbackLevel {
  return NEWS_FEEDBACK_LEVELS[Math.max(0, Math.min(6, 3 - score))];
}

/** Include borderline candidates until enough independently reviewed items
 * from this exact source agree they should be discarded. A useful dissent
 * prevents that item counting as a correct discard in the SQL aggregate. */
export function canSkipBorderlineNews(stats: NewsFeedbackSourceStats | undefined): boolean {
  return (
    !!stats &&
    stats.predictedBorderlineUniqueItems >= 20 &&
    stats.predictedBorderlineDiscardedItems / stats.predictedBorderlineUniqueItems >= 0.9
  );
}

/** Stored HTML and deletion notes are untrusted examples, never instructions.
 * Avoid leaking encoded bilingual duplicates, image URLs or editor markup. */
export function plainNewsLearningText(raw: string, limit = 600): string {
  return raw
    .replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, " ")
    .replace(/<[^>]*>/g, " ")
    .replace(/&(?:nbsp|#160);/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, limit);
}
