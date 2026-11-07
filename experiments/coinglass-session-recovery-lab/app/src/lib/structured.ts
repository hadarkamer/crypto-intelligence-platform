import { supabase } from "@/integrations/supabase/client";
import type { TableLevel } from "./coinglassCollector";
import { savedHover, hoverLevelData, type HoverSnapshot } from "./coinglassHover";
import {
  coinglassLevelQuantity,
  coinglassSideTotal,
  type CoinglassQuantitySnapshot,
} from "./coinglassQuantities";

/**
 * מודל נתונים מנורמל לשני סוגי השאלה המובנים (טבלת נזילות וטבלת חדשות).
 * כל שורה ורמה נשמרות בנפרד, כדי ששני משתמשים יוכלו לערוך חלקים שונים
 * באותה טבלה בלי לדרוס זה את זה (אין שמירה של הטבלה כולה כ-JSON יחיד).
 */

export const LIQUIDITY_TYPE = "liquidity_table";
export const NEWS_TYPE = "news_table";

export const isStructuredType = (t: string) => t === LIQUIDITY_TYPE || t === NEWS_TYPE;

/* --------------------------------- נזילות --------------------------------- */

/** סדר קנוני קבוע — הטבלה תמיד ממוינת מהטווח הקצר לארוך. */
export const TIMEFRAMES = [
  "12H",
  "24H",
  "48H",
  "3D",
  "1W",
  "2W",
  "1M",
  "3M",
  "6M",
  "1Y",
  "2Y",
] as const;
export type Timeframe = (typeof TIMEFRAMES)[number];

/** טווחי ברירת המחדל המוצגים תמיד, גם לפני שנשמר בהם דבר. */
export const DEFAULT_TIMEFRAMES: Timeframe[] = ["12H", "24H", "48H"];

export const timeframeOrder = (tf: string) => {
  const i = TIMEFRAMES.indexOf(tf as Timeframe);
  return i < 0 ? TIMEFRAMES.length : i;
};

export type LiquidityAmount = "many" | "normal" | "few";

export const AMOUNT_LABELS: Record<LiquidityAmount, string> = {
  many: "הרבה",
  normal: "בינוני",
  few: "מעט",
};

export const AMOUNT_TEXT_CLASS: Record<LiquidityAmount, string> = {
  many: "text-liq-many",
  normal: "text-liq-normal",
  few: "text-liq-few",
};

export type LiquiditySide = "above" | "below";

export type LiquidityLevel = {
  id: string;
  row_id: string;
  side: LiquiditySide;
  position: number;
  price_text: string;
  amount: LiquidityAmount;
  taken: boolean;
  updated_at: string | null;
  updated_by: string | null;
} & Partial<
  Pick<
    TableLevel,
    | "shape"
    | "price_precision"
    | "prices"
    | "evidence_basis"
    | "price_evidence"
    | "uncertainty_usd"
    | "strength_score"
    | "price_low"
    | "price_high"
    | "source_observation"
    | "amount_usd"
    | "quantity_bucket_ids"
    | "source_row_ids"
    | "source_value"
  >
>;

export type LiquidityRow = {
  id: string;
  question_id: string;
  timeframe: string;
  position: number;
  updated_at: string | null;
  updated_by: string | null;
  levels: LiquidityLevel[];
  quantity_snapshot?: CoinglassQuantitySnapshot | null;
  hover_snapshot?: HoverSnapshot | null;
};

/* --------------------------------- חדשות --------------------------------- */

export type NewsImportance =
  "very_bullish" | "bullish" | "positive" | "neutral" | "negative" | "bearish" | "very_bearish";

export const IMPORTANCE_OPTIONS: { value: NewsImportance; label: string }[] = [
  { value: "very_bullish", label: "שורי מאוד" },
  { value: "bullish", label: "שורי" },
  { value: "positive", label: "חיובי" },
  { value: "neutral", label: "ניטרלי" },
  { value: "negative", label: "שלילי" },
  { value: "bearish", label: "דובי" },
  { value: "very_bearish", label: "דובי מאוד" },
];

export const importanceLabel = (v: string) =>
  IMPORTANCE_OPTIONS.find((o) => o.value === v)?.label ?? v;

export const IMPORTANCE_TEXT_CLASS: Record<NewsImportance, string> = {
  very_bullish: "text-news-very-bullish font-bold",
  bullish: "text-news-bullish font-semibold",
  positive: "text-news-positive",
  neutral: "text-foreground",
  negative: "text-news-negative",
  bearish: "text-news-bearish font-semibold",
  very_bearish: "text-news-very-bearish font-bold",
};

export type NewsRow = {
  id: string;
  question_id: string;
  position: number;
  body: string;
  importance: NewsImportance;
  updated_at: string | null;
  updated_by: string | null;
};

/* ------------------------------- מבנה כללי ------------------------------- */

export type StructuredData = {
  liquidity: Record<string, LiquidityRow[]>;
  news: Record<string, NewsRow[]>;
};

export const EMPTY_STRUCTURED: StructuredData = { liquidity: {}, news: {} };

/** צילום מצב קפוא — נשמר בנעיצה ובהיסטוריה, ואינו הפניה חיה לנתונים. */
export type StructuredSnapshot =
  | { type: typeof LIQUIDITY_TYPE; rows: LiquidityRow[] }
  | { type: typeof NEWS_TYPE; rows: NewsRow[] };

/** האם יש בטבלה תוכן אמיתי — שורות ורמות ריקות אינן נחשבות תשובה. */
export function liquidityFilled(rows: LiquidityRow[] | undefined): boolean {
  return (rows ?? []).some((r) => r.levels.some((l) => l.price_text.trim().length > 0));
}

export function newsFilled(rows: NewsRow[] | undefined): boolean {
  return (rows ?? []).some((r) => (r.body ?? "").trim().length > 0);
}

export function snapshotFilled(snap: StructuredSnapshot | null | undefined): boolean {
  if (!snap) return false;
  return snap.type === LIQUIDITY_TYPE
    ? liquidityFilled(snap.rows as LiquidityRow[])
    : newsFilled(snap.rows as NewsRow[]);
}

/** חותמת העדכון האחרונה בטבלה כולה. */
export function snapshotUpdatedAt(snap: StructuredSnapshot): string | null {
  const stamps: string[] = [];
  for (const r of snap.rows as { updated_at: string | null; levels?: LiquidityLevel[] }[]) {
    if (r.updated_at) stamps.push(r.updated_at);
    for (const l of r.levels ?? []) if (l.updated_at) stamps.push(l.updated_at);
  }
  if (!stamps.length) return null;
  return stamps.reduce((a, b) => (b > a ? b : a));
}

/** מזהה המשתמש שעדכן אחרון בטבלה. */
export function snapshotAuthor(snap: StructuredSnapshot): string | null {
  let best: { at: string; by: string | null } | null = null;
  const consider = (at: string | null, by: string | null) => {
    if (!at) return;
    if (!best || at > best.at) best = { at, by };
  };
  for (const r of snap.rows as {
    updated_at: string | null;
    updated_by: string | null;
    levels?: LiquidityLevel[];
  }[]) {
    consider(r.updated_at, r.updated_by);
    for (const l of r.levels ?? []) consider(l.updated_at, l.updated_by);
  }
  return best ? (best as { by: string | null }).by : null;
}

/** בונה snapshot קפוא מהנתונים הנוכחיים של שאלה. */
export function snapshotOf(
  questionType: string,
  data: StructuredData,
  questionId: string,
): StructuredSnapshot | null {
  if (questionType === LIQUIDITY_TYPE) {
    return {
      type: LIQUIDITY_TYPE,
      rows: JSON.parse(JSON.stringify(data.liquidity[questionId] ?? [])) as LiquidityRow[],
    };
  }
  if (questionType === NEWS_TYPE) {
    return {
      type: NEWS_TYPE,
      rows: JSON.parse(JSON.stringify(data.news[questionId] ?? [])) as NewsRow[],
    };
  }
  return null;
}

/** קריאה בטוחה של snapshot ששמור ב-JSON (נעיצה או היסטוריה). */
export function parseSnapshot(raw: unknown): StructuredSnapshot | null {
  const o = (raw ?? {}) as Record<string, unknown>;
  const inner = (o.structured ?? o) as Record<string, unknown>;
  const type = inner?.type;
  const rows = inner?.rows;
  if ((type !== LIQUIDITY_TYPE && type !== NEWS_TYPE) || !Array.isArray(rows)) return null;
  return { type, rows } as StructuredSnapshot;
}

/* --------------------------------- שליפה --------------------------------- */

function toRow(r: Record<string, unknown>): LiquidityRow {
  return {
    id: String(r.id),
    question_id: String(r.question_id),
    timeframe: String(r.timeframe),
    position: Number(r.position ?? 0),
    updated_at: (r.updated_at as string | null) ?? null,
    updated_by: (r.updated_by as string | null) ?? null,
    levels: [],
  };
}

function toLevel(l: Record<string, unknown>): LiquidityLevel {
  return {
    id: String(l.id),
    row_id: String(l.row_id),
    side: (l.side as LiquiditySide) ?? "above",
    position: Number(l.position ?? 0),
    price_text: String(l.price_text ?? ""),
    amount: (l.amount as LiquidityAmount) ?? "many",
    taken: !!l.taken,
    updated_at: (l.updated_at as string | null) ?? null,
    updated_by: (l.updated_by as string | null) ?? null,
  };
}

function toNews(n: Record<string, unknown>): NewsRow {
  return {
    id: String(n.id),
    question_id: String(n.question_id),
    position: Number(n.position ?? 0),
    body: String(n.body ?? ""),
    importance: (n.importance as NewsImportance) ?? "neutral",
    updated_at: (n.updated_at as string | null) ?? null,
    updated_by: (n.updated_by as string | null) ?? null,
  };
}

function assemble(
  rows: Record<string, unknown>[],
  levels: Record<string, unknown>[],
  news: Record<string, unknown>[],
): StructuredData {
  const byId = new Map<string, LiquidityRow>();
  const liquidity: Record<string, LiquidityRow[]> = {};
  for (const raw of rows) {
    const row = toRow(raw);
    byId.set(row.id, row);
    (liquidity[row.question_id] ??= []).push(row);
  }
  for (const raw of levels) {
    const level = toLevel(raw);
    byId.get(level.row_id)?.levels.push(level);
  }
  for (const list of Object.values(liquidity)) {
    list.sort((a, b) => timeframeOrder(a.timeframe) - timeframeOrder(b.timeframe));
    for (const r of list) r.levels.sort((a, b) => a.position - b.position);
  }
  const newsMap: Record<string, NewsRow[]> = {};
  for (const raw of news) {
    const n = toNews(raw);
    (newsMap[n.question_id] ??= []).push(n);
  }
  for (const list of Object.values(newsMap)) list.sort((a, b) => a.position - b.position);
  return { liquidity, news: newsMap };
}

/** כל הנתונים המובנים של הפרויקט — שלוש שאילתות בלבד, ללא N+1. */
export async function fetchStructuredData(projectId: string): Promise<StructuredData> {
  const [rowsRes, levelsRes, newsRes] = await Promise.all([
    supabase.from("project_liquidity_rows").select("*").eq("project_id", projectId),
    supabase.from("project_liquidity_levels").select("*").eq("project_id", projectId),
    supabase.from("project_news_rows").select("*").eq("project_id", projectId),
  ]);
  if (rowsRes.error) throw rowsRes.error;
  if (levelsRes.error) throw levelsRes.error;
  if (newsRes.error) throw newsRes.error;
  return assemble(
    (rowsRes.data ?? []) as Record<string, unknown>[],
    (levelsRes.data ?? []) as Record<string, unknown>[],
    (newsRes.data ?? []) as Record<string, unknown>[],
  );
}

export type StructuredHistoryRow = {
  project_id: string;
  question_id: string;
  snapshot: StructuredSnapshot;
};

/**
 * נתונים מובנים של אותן שאלות בפרויקטים אחרים — לצורך „תשובה קודמת”.
 * שלוש שאילתות בלבד לכל קבוצת שאלות (ללא N+1).
 */
export async function fetchStructuredHistory(
  questionIds: string[],
  excludeProjectId: string,
): Promise<StructuredHistoryRow[]> {
  const ids = [...new Set(questionIds.filter(Boolean))];
  if (!ids.length) return [];
  const [rowsRes, newsRes] = await Promise.all([
    supabase
      .from("project_liquidity_rows")
      .select("*")
      .in("question_id", ids)
      .neq("project_id", excludeProjectId),
    supabase
      .from("project_news_rows")
      .select("*")
      .in("question_id", ids)
      .neq("project_id", excludeProjectId),
  ]);
  const rows = (rowsRes.data ?? []) as Record<string, unknown>[];
  const rowIds = rows.map((r) => String(r.id));
  let levels: Record<string, unknown>[] = [];
  if (rowIds.length) {
    const { data } = await supabase
      .from("project_liquidity_levels")
      .select("*")
      .in("row_id", rowIds);
    levels = (data ?? []) as Record<string, unknown>[];
  }

  const out: StructuredHistoryRow[] = [];
  const liquidityByPair = new Map<string, LiquidityRow[]>();
  const levelsByRow = new Map<string, LiquidityLevel[]>();
  for (const raw of levels) {
    const l = toLevel(raw);
    (levelsByRow.get(l.row_id) ?? levelsByRow.set(l.row_id, []).get(l.row_id)!).push(l);
  }
  for (const raw of rows) {
    const row = toRow(raw);
    row.levels = (levelsByRow.get(row.id) ?? []).sort((a, b) => a.position - b.position);
    const key = `${String(raw.project_id)}|${row.question_id}`;
    (liquidityByPair.get(key) ?? liquidityByPair.set(key, []).get(key)!).push(row);
  }
  for (const [key, list] of liquidityByPair) {
    const [project_id, question_id] = key.split("|");
    list.sort((a, b) => timeframeOrder(a.timeframe) - timeframeOrder(b.timeframe));
    out.push({ project_id, question_id, snapshot: { type: LIQUIDITY_TYPE, rows: list } });
  }

  const newsByPair = new Map<string, NewsRow[]>();
  for (const raw of (newsRes.data ?? []) as Record<string, unknown>[]) {
    const n = toNews(raw);
    const key = `${String(raw.project_id)}|${n.question_id}`;
    (newsByPair.get(key) ?? newsByPair.set(key, []).get(key)!).push(n);
  }
  for (const [key, list] of newsByPair) {
    const [project_id, question_id] = key.split("|");
    list.sort((a, b) => a.position - b.position);
    out.push({ project_id, question_id, snapshot: { type: NEWS_TYPE, rows: list } });
  }

  return out.filter((r) => snapshotFilled(r.snapshot));
}

/* --------------------------------- כתיבה --------------------------------- */

export async function addLiquidityRow(
  projectId: string,
  questionId: string,
  timeframe: string,
): Promise<string> {
  const { data, error } = await supabase
    .from("project_liquidity_rows")
    .insert({
      project_id: projectId,
      question_id: questionId,
      timeframe,
      position: timeframeOrder(timeframe),
    })
    .select("id")
    .single();
  if (error) throw error;
  return String(data.id);
}

export async function deleteLiquidityRow(id: string) {
  const { error } = await supabase.from("project_liquidity_rows").delete().eq("id", id);
  if (error) throw error;
}

export async function addLiquidityLevel(
  projectId: string,
  rowId: string,
  side: LiquiditySide,
  position: number,
  patch: Partial<Pick<LiquidityLevel, "price_text" | "amount" | "taken">> = {},
) {
  const { error } = await supabase.from("project_liquidity_levels").insert({
    project_id: projectId,
    row_id: rowId,
    side,
    position,
    price_text: patch.price_text ?? "",
    amount: patch.amount ?? "many",
    taken: patch.taken ?? false,
  });
  if (error) throw error;
}

export async function updateLiquidityLevel(
  id: string,
  patch: Partial<Pick<LiquidityLevel, "price_text" | "amount" | "taken">>,
) {
  const { error } = await supabase
    .from("project_liquidity_levels")
    .update(patch as never)
    .eq("id", id);
  if (error) throw error;
}

export async function deleteLiquidityLevel(id: string) {
  const { error } = await supabase.from("project_liquidity_levels").delete().eq("id", id);
  if (error) throw error;
}

export async function addNewsRow(
  projectId: string,
  questionId: string,
  position: number,
  patch: Partial<Pick<NewsRow, "body" | "importance">> = {},
) {
  const { error } = await supabase.from("project_news_rows").insert({
    project_id: projectId,
    question_id: questionId,
    position,
    body: patch.body ?? "",
    importance: patch.importance ?? "neutral",
  });
  if (error) throw error;
}

export async function updateNewsRow(
  id: string,
  patch: Partial<Pick<NewsRow, "body" | "importance">>,
) {
  const { error } = await supabase
    .from("project_news_rows")
    .update(patch as never)
    .eq("id", id);
  if (error) throw error;
}

export async function deleteNewsRow(id: string) {
  const { error } = await supabase.from("project_news_rows").delete().eq("id", id);
  if (error) throw error;
}

/* --------------------------------- ייצוא --------------------------------- */

/** ייצוג קריא לייצוא JSON. */
export function exportStructured(data: StructuredData) {
  return {
    liquidity: Object.entries(data.liquidity).map(([question_id, rows]) => ({
      question_id,
      rows: rows.map((r) => ({
        timeframe: r.timeframe,
        updated_at: r.updated_at,
        updated_by: r.updated_by,
        above_total_usd: coinglassSideTotal(r, "above"),
        below_total_usd: coinglassSideTotal(r, "below"),
        ...(savedHover(r) ? { hover_snapshot: savedHover(r) } : {}),
        above: r.levels
          .filter((l) => l.side === "above")
          .map((l) => ({
            price: l.price_text,
            amount: AMOUNT_LABELS[l.amount],
            ...(hoverLevelData(r, l, "above")
              ? { source_measurement: hoverLevelData(r, l, "above") }
              : {}),
            ...(coinglassLevelQuantity(r, l, "above")
              ? {
                  amount_usd: coinglassLevelQuantity(r, l, "above")!.amountUsd,
                  side_percent: coinglassLevelQuantity(r, l, "above")!.percent,
                }
              : {}),
            taken: l.taken,
            updated_at: l.updated_at,
            updated_by: l.updated_by,
          })),
        below: r.levels
          .filter((l) => l.side === "below")
          .map((l) => ({
            price: l.price_text,
            amount: AMOUNT_LABELS[l.amount],
            ...(hoverLevelData(r, l, "below")
              ? { source_measurement: hoverLevelData(r, l, "below") }
              : {}),
            ...(coinglassLevelQuantity(r, l, "below")
              ? {
                  amount_usd: coinglassLevelQuantity(r, l, "below")!.amountUsd,
                  side_percent: coinglassLevelQuantity(r, l, "below")!.percent,
                }
              : {}),
            taken: l.taken,
            updated_at: l.updated_at,
            updated_by: l.updated_by,
          })),
      })),
    })),
    news: Object.entries(data.news).map(([question_id, rows]) => ({
      question_id,
      rows: rows.map((r) => ({
        body: r.body,
        importance: importanceLabel(r.importance),
        updated_at: r.updated_at,
        updated_by: r.updated_by,
      })),
    })),
  };
}
