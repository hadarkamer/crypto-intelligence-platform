/**
 * מתאם התור לאיסוף סדרתי (טהור, ניתן לבדיקה אופליין).
 * אותה לוגיקה משרתת גם את "כל הטווחים" בשדה יחיד וגם את "איסוף כל מפות
 * CoinGlass" בגיליון — אין כאן שתי מימושים מקבילים.
 *
 * עיקרון: פריט אחד בכל רגע. לעולם לא נפתחות שתי סריקות במקביל.
 */
import {
  COLLECTOR_TIMEFRAMES,
  type CollectorTimeframe,
  type HeatmapModel,
} from "@/lib/coinglassCollector";
import type { CollectResponse } from "@/lib/coinglassCollectCore";

export type BatchTarget = { fieldId: string; model: HeatmapModel; label?: string };

export type BatchStatus = "pending" | "running" | "done" | "failed" | "stopped";

export type BatchEntry = {
  fieldId: string;
  model: HeatmapModel;
  timeframe: CollectorTimeframe;
  label?: string;
  status: BatchStatus;
  note?: string;
};

/** סדר קבוע: שדה אחרי שדה, ובכל שדה 12H → 24H → 48H. */
export function buildBatch(
  targets: BatchTarget[],
  timeframes: readonly CollectorTimeframe[] = COLLECTOR_TIMEFRAMES,
): BatchEntry[] {
  const entries: BatchEntry[] = [];
  for (const target of targets)
    for (const timeframe of timeframes)
      entries.push({
        fieldId: target.fieldId,
        model: target.model,
        timeframe,
        ...(target.label === undefined ? {} : { label: target.label }),
        status: "pending",
      });
  return entries;
}

/** הפריט הבא שעדיין לא הושלם. הפעלה מחדש ממשיכה רק את מה שלא הצליח. */
export function nextPendingIndex(entries: readonly BatchEntry[]): number | null {
  const index = entries.findIndex((entry) => entry.status === "pending");
  return index === -1 ? null : index;
}

export const batchProgress = (entries: readonly BatchEntry[]) => ({
  done: entries.filter((entry) => entry.status === "done").length,
  total: entries.length,
});

/** מצבים שבהם התור נעצר מיד — מכסה, התנגשות, ניתוק או תצורה חסרה. */
export function shouldStopQueue(response: CollectResponse): boolean {
  return (
    response.status === "rate_limited" ||
    response.status === "conflict" ||
    response.status === "stale" ||
    response.status === "not_connected" ||
    (response.status === "failed" && response.retryable === true)
  );
}

/** כשל מקור בפריט אחד מסומן ככישלון וממשיכים לפריט הבא, בלי ניסיון חוזר. */
export function outcomeStatus(response: CollectResponse): BatchStatus {
  if (response.status === "saved" || response.status === "duplicate") return "done";
  if (shouldStopQueue(response)) return "stopped";
  return "failed";
}

export function applyOutcome(
  entries: readonly BatchEntry[],
  index: number,
  response: CollectResponse,
): BatchEntry[] {
  const status = outcomeStatus(response);
  return entries.map((entry, position) =>
    position === index ? { ...entry, status, note: response.note } : entry,
  );
}

export function markStatus(
  entries: readonly BatchEntry[],
  index: number,
  status: BatchStatus,
  note?: string,
): BatchEntry[] {
  return entries.map((entry, position) =>
    position === index ? { ...entry, status, ...(note === undefined ? {} : { note }) } : entry,
  );
}

/** אזהרה לפני הפעלה: מספר הניתוחים החדשים האפשריים (תלוי מטמון בשירות). */
export const batchWarning = (count: number) =>
  `הפעולה עשויה להריץ עד ${count} ניתוחים חדשים, בהתאם למה שכבר שמור בשירות. האיסופים רצים בזה אחר זה.`;
