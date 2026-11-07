import type { V2Sentiment } from "./workspaceV1";

/** Background strength follows the three levels on each side of neutral. */
export const newsSentimentStyle: Record<V2Sentiment, string> = {
  very_bullish: "!bg-green-500/25 !border-green-500/60",
  bullish: "!bg-green-500/15 !border-green-500/40",
  positive: "!bg-green-500/5 !border-green-500/25",
  neutral: "",
  negative: "!bg-red-500/5 !border-red-500/25",
  bearish: "!bg-red-500/15 !border-red-500/40",
  very_bearish: "!bg-red-500/25 !border-red-500/60",
};

/** Make the current choice legible in both themes, including before its save completes. */
export const newsSentimentSelectStyle: Record<V2Sentiment, string> = {
  very_bullish: "!bg-green-500/30 !border-green-500/80 !text-green-800 dark:!text-green-200",
  bullish: "!bg-green-500/20 !border-green-500/60 !text-green-800 dark:!text-green-200",
  positive: "!bg-green-500/10 !border-green-500/40 !text-green-800 dark:!text-green-200",
  neutral:
    "!border-yellow-500 !bg-yellow-100 !text-yellow-900 dark:!bg-yellow-950 dark:!text-yellow-200",
  negative: "!bg-red-500/10 !border-red-500/40 !text-red-800 dark:!text-red-200",
  bearish: "!bg-red-500/20 !border-red-500/60 !text-red-800 dark:!text-red-200",
  very_bearish: "!bg-red-500/30 !border-red-500/80 !text-red-800 dark:!text-red-200",
};
