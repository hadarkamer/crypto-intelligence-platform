import type {
  BtcLiquidityCandidate,
  VerifiedBtcLiquiditySnapshot,
} from "./collectionBtcLiquiditySource.server";
import type { BtcLiquidityJudgment } from "./collectionAutofillModel.server";

const INVALID = "ניתוח הביטקוין אינו תואם את רמות הגרף; התיבה לא שונתה.";
const frames = {
  "15m": "15m",
  "1h": "1H",
  "4h": "4H",
  "1d": "1D",
  "1w": "1W",
  "1M": "1M",
} as const;
const frameNames = {
  "15m": "רבע שעה",
  "1h": "שעה",
  "4h": "ארבע שעות",
  "1d": "יום",
  "1w": "שבוע",
  "1M": "חודש",
} as const;

function conciseReason(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const reason = value.trim();
  return (
    reason.length > 0 &&
    reason.length <= 160 &&
    reason.split(/\s+/).length <= 22 &&
    /[א-ת]/.test(reason) &&
    !/[\r\n<>`{}]|https?:\/\/|www\.|\b(?:snapshot|kind|evidence|source_url|captured_at|current_price|JSON)\b|תמונת מצב|מקור\s*:/i.test(
      reason,
    ) &&
    !/\d/.test(reason.replace(/\b(?:15m|1H|4H|1D|1W|1M)\b/g, ""))
  );
}

/** Plain text only. The caller escapes it and retains provenance in metadata. */
export function renderBtcLiquiditySummary(
  snapshot: VerifiedBtcLiquiditySnapshot,
  judgment: BtcLiquidityJudgment,
  options: { showTimeframes?: boolean } = {},
): string {
  const candidates = snapshot.candidates;
  const current = snapshot.currentPrice;
  if (
    !Number.isFinite(current) ||
    current <= 0 ||
    !Array.isArray(candidates) ||
    !candidates.length ||
    new Set(candidates.map((candidate) => candidate.id)).size !== candidates.length ||
    candidates.some(
      (candidate) =>
        !candidate.id ||
        !Object.hasOwn(frames, candidate.timeframe) ||
        !Number.isFinite(candidate.low) ||
        !Number.isFinite(candidate.high) ||
        candidate.low <= 0 ||
        candidate.high < candidate.low ||
        (candidate.side === "above"
          ? candidate.low <= current
          : candidate.side === "below"
            ? candidate.high >= current
            : true),
    )
  )
    throw new Error(INVALID);
  const primary = candidates.find((candidate) => candidate.id === judgment.primary_id);
  const alternate = candidates.find((candidate) => candidate.id === judgment.alternate_id);
  const invalidation = candidates.find((candidate) => candidate.id === judgment.invalidation_id);
  const reason = typeof judgment.rationale === "string" ? judgment.rationale.trim() : "";
  const condition = judgment.alternate_condition;
  if (
    !primary ||
    (judgment.alternate_id !== null && (!alternate || alternate.side === primary.side)) ||
    !invalidation ||
    invalidation.side === primary.side ||
    !["conditional", "weak"].includes(judgment.preference) ||
    !conciseReason(reason) ||
    (condition !== undefined &&
      (judgment.alternate_id === null
        ? condition !== null
        : !conciseReason(condition) ||
          !/^אם\s+\S/.test(condition.trim()) ||
          /^אם\s+(?:המצב ישתנה|הכיוון ישתנה|התרחיש יתבטל|תהיה ירידה|תהיה עלייה|זה יקרה|יתאפשר)[.!?\s]*$/.test(
            condition.trim(),
          )))
  )
    throw new Error(INVALID);
  const price = (number: number) =>
    new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(number);
  const target = (
    candidate: BtcLiquidityCandidate,
    includeTimeframe = options.showTimeframes !== false,
  ) => {
    const range =
      candidate.low === candidate.high
        ? price(candidate.low)
        : `${price(candidate.high)}–${price(candidate.low)}`;
    const timeframe = includeTimeframe ? ` (${frames[candidate.timeframe]})` : "";
    return `${candidate.side === "above" ? "עלייה" : "ירידה"} ל־${range}${timeframe}`;
  };
  const detailed = judgment.timeframe_judgments;
  const lines = [
    `${detailed === undefined ? "" : "יעד כולל: "}${judgment.preference === "weak" ? "אפשרי" : "סביר"}: ${target(primary)} — ${reason.replace(/\s+/g, " ")}`,
    ...(alternate && typeof condition === "string"
      ? [`חלופה: ${target(alternate)} — ${condition.trim().replace(/\s+/g, " ")}`]
      : []),
  ];
  // Historical saved decisions retain their original compact presentation.
  if (detailed === undefined) return lines.join("\n");
  if (
    !Array.isArray(detailed) ||
    detailed.length !== Object.keys(frames).length ||
    detailed.some((frame) => !frame || typeof frame !== "object" || Array.isArray(frame)) ||
    new Set(detailed.map((frame) => frame.timeframe)).size !== Object.keys(frames).length ||
    detailed.some((frame) => !Object.hasOwn(frames, frame.timeframe)) ||
    !Array.isArray(snapshot.frames)
  )
    throw new Error(INVALID);
  lines.push("");
  for (const timeframe of Object.keys(frames) as Array<keyof typeof frames>) {
    const frame = detailed.find((entry) => entry.timeframe === timeframe)!;
    const prefix = `${frameNames[timeframe]} (${frames[timeframe]})`;
    const available = snapshot.frames.some((source) => source.interval === timeframe);
    const levels = candidates.filter((candidate) => candidate.timeframe === timeframe);
    if (!available || !levels.length) {
      if (frame.target_id !== null || frame.preference !== null || frame.rationale !== null)
        throw new Error(INVALID);
      lines.push(
        `${prefix}: ${available ? "לא נמצא יעד נזילות ברור" : "אין נתונים זמינים לגרף זה"}.`,
      );
      continue;
    }
    const candidate = levels.find((level) => level.id === frame.target_id);
    if (
      !candidate ||
      !["conditional", "weak"].includes(frame.preference ?? "") ||
      !conciseReason(frame.rationale)
    )
      throw new Error(INVALID);
    lines.push(
      `${prefix}: ${frame.preference === "weak" ? "אפשרי" : "סביר"}: ${target(candidate, false)} — ${frame.rationale.trim().replace(/\s+/g, " ")}`,
    );
  }
  return lines.join("\n");
}
