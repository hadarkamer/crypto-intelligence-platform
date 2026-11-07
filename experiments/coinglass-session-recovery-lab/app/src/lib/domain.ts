import { hasMeaningfulContent } from "./content";
import { capitalMarketValueComplete } from "./capitalMarketPresence.ts";

export type QuestionType =
  | "info"
  | "checkbox"
  | "yes_no_na"
  | "single_choice"
  | "multi_choice"
  | "short_text"
  | "long_text"
  | "currency_select"
  | "reasons_with_evidence"
  | "notes"
  | "conclusions"
  | "pnl"
  | "number"
  | "datetime"
  | "image"
  | "link"
  | "stage_conclusion"
  | "liquidity_table"
  | "news_table"
  | "economic_calendar"
  | "button";

export const QUESTION_TYPE_LABELS: Record<QuestionType, string> = {
  info: "מידע קבוע",
  checkbox: "תיבת סימון",
  yes_no_na: "כן / לא / לא רלוונטי",
  single_choice: "בחירה יחידה",
  multi_choice: "בחירה מרובה",
  short_text: "טקסט קצר",
  long_text: "טקסט ארוך",
  currency_select: "בחירת מטבע",
  reasons_with_evidence: "סיבות עם סימוכין",
  notes: "הערות",
  conclusions: "מסקנות",
  pnl: "PnL",
  number: "מספר",
  datetime: "תאריך ושעה",
  image: "תמונה",
  link: "קישור",
  stage_conclusion: "מסקנת שלב",
  liquidity_table: "טקסט נזילות",
  news_table: "חדשות",
  economic_calendar: "יומן כלכלי",
  button: "כפתור",
};

export const QUESTION_TYPES = Object.keys(QUESTION_TYPE_LABELS) as QuestionType[];

export const CHOICE_TYPES: QuestionType[] = ["single_choice", "multi_choice", "stage_conclusion"];

/** סוגי שאלה שבהם יש טקסט חופשי, ולכן ניתן לקבוע להם יישור ברירת מחדל. */
export const TEXT_ALIGN_TYPES: QuestionType[] = [
  "short_text",
  "long_text",
  "stage_conclusion",
  "reasons_with_evidence",
  "notes",
  "conclusions",
];

/** סוגי שאלה שהתוכן שלהם נשמר בטבלאות מנורמלות ולא ב-project_answers. */
export const STRUCTURED_TYPES: QuestionType[] = ["liquidity_table", "news_table"];

export const PROJECT_STATUSES = [
  { value: "draft", label: "טיוטה" },
  { value: "reviewing", label: "בבדיקה" },
  { value: "waiting", label: "ממתין" },
  { value: "approved", label: "אושר לכניסה" },
  { value: "rejected", label: "נפסל" },
  { value: "active_trade", label: "עסקה פעילה" },
  { value: "completed", label: "הושלם" },
  { value: "archived", label: "בארכיון" },
] as const;

export const statusLabel = (value: string) =>
  PROJECT_STATUSES.find((s) => s.value === value)?.label ?? value;

export const DIRECTIONS = [
  { value: "undecided", label: "טרם נקבע" },
  { value: "long", label: "Long" },
  { value: "short", label: "Short" },
] as const;

export const directionLabel = (value?: string | null) =>
  DIRECTIONS.find((d) => d.value === value)?.label ?? "טרם נקבע";

export const OPPORTUNITY_SOURCES = [
  { value: "bot", label: "בוט" },
  { value: "manual_scan", label: "סריקה ידנית" },
  { value: "alert", label: "התראה" },
  { value: "other", label: "אחר" },
] as const;

export const TRADE_RESULTS = [
  { value: "profit", label: "רווח" },
  { value: "loss", label: "הפסד" },
  { value: "breakeven", label: "איזון" },
  { value: "no_entry", label: "לא נכנסנו" },
  { value: "cancelled", label: "בוטלה" },
] as const;

export const TEMPLATE_VERSION_STATUS: Record<string, string> = {
  draft: "טיוטה",
  active: "פעילה",
  archived: "ארכיון",
};

export type QuestionLink = { id: string; label: string; url: string };

export type Question = {
  id: string;
  group_id: string;
  title: string;
  description: string | null;
  question_type: string;
  position: number;
  required: boolean;
  active: boolean;
  options_json: unknown;
  visibility_rule_json: unknown;
  help_text: string | null;
  source_url: string | null;
  links_json?: unknown;
  /** מזהה סמנטי יציב, למשל "trade_pnl" לשאלת הרווח וההפסד ביומן. */
  field_key?: string | null;
  /** מזהה קבוע הנשמר בין גרסאות תבנית, לצורך היסטוריית תשובות. */
  question_key?: string | null;
  /** יישור ברירת מחדל של תיבת הכתיבה: right / left / center. */
  default_text_align?: string | null;
};

/**
 * סוגי שאלות שהם סימון בלבד — אין בהם טקסט משמעותי להשוואה,
 * ולכן לא מוצגת עבורם "תשובה קודמת".
 */
const MARK_ONLY_TYPES = new Set([
  "checkbox",
  "done_not_done",
  "yes_no_na",
  "single_choice",
  "multi_choice",
  "button",
]);
export const showsHistory = (questionType: string) => !MARK_ONLY_TYPES.has(questionType);

/**
 * זיהוי יציב של "אותה שאלה" בין פרויקטים וגרסאות תבנית:
 * question_key → field_key → כמוצא אחרון צירוף גליון/סקשן/כותרת.
 */
export function questionKeyOf(q: Question, stageTitle: string, groupTitle: string) {
  const stable = (q.question_key ?? "").trim() || (q.field_key ?? "").trim();
  if (stable) return stable;
  const norm = (s: string) => s.trim().toLowerCase();
  return `${norm(stageTitle)}/${norm(groupTitle)}/${norm(q.title)}`;
}

/** רשימת הקישורים של שאלה, כולל תאימות לאחור לקישור הבודד הישן. */
export function linksOf(q: { links_json?: unknown; source_url?: string | null }): QuestionLink[] {
  const raw = Array.isArray(q.links_json) ? (q.links_json as unknown[]) : [];
  const list = raw
    .map((item, i) => {
      const o = (item ?? {}) as Record<string, unknown>;
      const url = typeof o.url === "string" ? o.url : "";
      if (!url) return null;
      return {
        id: typeof o.id === "string" && o.id ? o.id : `link-${i}`,
        label: typeof o.label === "string" && o.label.trim() ? o.label : "מקור",
        url,
      };
    })
    .filter(Boolean) as QuestionLink[];
  if (list.length === 0 && q.source_url) {
    return [{ id: "legacy", label: "מקור", url: q.source_url }];
  }
  return list;
}

export type Group = {
  id: string;
  stage_id: string;
  title: string;
  description: string | null;
  position: number;
  collapsible: boolean;
  active: boolean;
  /** התנהגות מיוחדת של הסקשן, למשל 'mental' לסימון אישי לכל משתמש. */
  behavior?: string | null;
  questions: Question[];
};

/** סקשן "נפשי" — סימוני ה-checkbox בו הם אישיים לכל משתמש. */
export const isMentalGroup = (g: { behavior?: string | null }) => (g.behavior ?? "") === "mental";

export type Stage = {
  id: string;
  template_version_id: string;
  title: string;
  description: string | null;
  position: number;
  active: boolean;
  groups: Group[];
};

export type Answer = {
  /** Collection timeframe rows remain structured in the shared reader. */
  timeframe_scenarios?: unknown;
  /** Preserve each saved automatic cell and the separate manual observations. */
  capital_market?: unknown;
  capital_market_notes?: unknown;
  id: string;
  project_id: string;
  question_id: string;
  /** Trade 1/2/... identity. Shared early-stage answers use Trade 1. */
  trade_cycle_id?: string | null;
  text_value: string | null;
  number_value: number | null;
  boolean_value: boolean | null;
  choice_value_json: unknown;
  date_value: string | null;
  /** "אין חדש / לא רלוונטי" — נחשב מטופל בהתקדמות ואינו מוחק תוכן קיים. */
  not_relevant?: boolean | null;
  /** סימון break even מפורש על שדה ה-PnL */
  break_even?: boolean | null;
  created_at?: string | null;
  updated_at?: string | null;
};

/** מועד עדכון אחרון של סעיף, בפורמט עברי מקומי ללא שניות. */
export function formatUpdatedAt(value?: string | null) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(d.getDate())}.${pad(d.getMonth() + 1)}.${d.getFullYear()}, ${pad(
    d.getHours(),
  )}:${pad(d.getMinutes())}`;
}

export function optionsOf(q: Question): string[] {
  const raw = q.options_json;
  if (Array.isArray(raw)) return raw.map(String);
  return [];
}

export function isCountable(q: Question) {
  return q.active && !["info", "button"].includes(q.question_type);
}

/** שאלת "הסבר" מותנית מוצגת רק כאשר השאלה שלפניה נענתה בחיוב. */
export function visibleQuestions(
  questions: Question[],
  answersById: Record<string, Answer>,
): Question[] {
  const active = questions.filter((q) => q.active);
  const out: Question[] = [];
  for (let i = 0; i < active.length; i++) {
    const q = active[i];
    const rule = q.visibility_rule_json as { show_when?: string } | null;
    if (rule?.show_when === "prev_yes") {
      const prev = active[i - 1];
      if (!prev) continue;
      const a = answersById[prev.id];
      const yes = ["checkbox", "done_not_done"].includes(prev.question_type)
        ? a?.boolean_value === true
        : a?.text_value === "yes";
      if (!yes) continue;
    }
    out.push(q);
  }
  return out;
}

export function isAnswered(
  q: Question,
  a: Answer | undefined,
  imageCount = 0,
  /** האם בטבלה המובנית של השאלה יש תוכן אמיתי (שורות ריקות אינן נחשבות). */
  structuredFilled = false,
): boolean {
  if (q.question_type === "image") return imageCount > 0;
  // התוכן של הסוגים המובנים נשמר בטבלאות נפרדות, לא בשורת התשובה
  if (q.question_type === "liquidity_table" || q.question_type === "news_table") {
    return structuredFilled;
  }
  if (!a) return false;
  switch (q.question_type) {
    case "capital_market":
      return capitalMarketValueComplete(a);
    case "done_not_done":
      return typeof a.boolean_value === "boolean";
    case "checkbox":
      return a.boolean_value === true;
    case "number":
    case "pnl":
      return a.number_value !== null && a.number_value !== undefined;
    case "datetime":
      return !!a.date_value;
    case "multi_choice":
      return Array.isArray(a.choice_value_json) && a.choice_value_json.length > 0;
    case "stage_conclusion":
      return !!(a.choice_value_json as { choice?: string } | null)?.choice;
    case "reasons_with_evidence": {
      const evidence =
        a.choice_value_json &&
        typeof a.choice_value_json === "object" &&
        !Array.isArray(a.choice_value_json)
          ? (a.choice_value_json as Record<string, unknown>).evidence
          : undefined;
      return (
        hasMeaningfulContent(a.text_value) ||
        (Array.isArray(evidence) &&
          evidence.some(
            (item) =>
              item && typeof item === "object" && typeof item.id === "string" && item.snapshot,
          ))
      );
    }
    case "long_text":
    case "notes":
    case "conclusions":
      return hasMeaningfulContent(a.text_value);
    default:
      return hasMeaningfulContent(a.text_value);
  }
}

/** הקשר לחישוב התקדמות אחיד בכל המסכים. */
export type ProgressContext = {
  answersById: Record<string, Answer>;
  imageCountOf: (questionId: string) => number;
  /** מזהי שאלות שהמשתמש הנוכחי סימן בסקשן mental. */
  myChecks: Set<string>;
  /** האם יש תוכן אמיתי בטבלה המובנית של השאלה (נזילות / חדשות). */
  structuredFilled?: (questionId: string) => boolean;
};

/** האם הסעיף נחשב "מטופל" — כולל סימון "אין חדש / לא רלוונטי". */
export function isHandled(
  q: Question,
  group: { behavior?: string | null },
  ctx: ProgressContext,
): boolean {
  const a = ctx.answersById[q.id];
  if (a?.not_relevant === true) return true;
  if (isMentalGroup(group) && q.question_type === "checkbox") {
    // בסקשן נפשי הסימון אישי לכל משתמש ואינו נשען על boolean_value המשותף
    return ctx.myChecks.has(q.id);
  }
  return isAnswered(q, a, ctx.imageCountOf(q.id), ctx.structuredFilled?.(q.id) ?? false);
}

/** התקדמות של סקשן יחיד: רק שאלות גלויות ונספרות. */
export function groupProgress(
  group: { behavior?: string | null; questions: Question[] },
  ctx: ProgressContext,
) {
  let total = 0;
  let done = 0;
  for (const q of visibleQuestions(group.questions, ctx.answersById)) {
    if (!isCountable(q)) continue;
    total++;
    if (isHandled(q, group, ctx)) done++;
  }
  return { total, done, pct: total ? Math.round((done / total) * 100) : 0 };
}

/** התקדמות של גליון שלם — סכום כל הסקשנים הפעילים. */
export function stageProgress(
  stage: { groups: { active: boolean; behavior?: string | null; questions: Question[] }[] },
  ctx: ProgressContext,
) {
  let total = 0;
  let done = 0;
  for (const g of stage.groups.filter((g) => g.active)) {
    const s = groupProgress(g, ctx);
    total += s.total;
    done += s.done;
  }
  return { total, done, pct: total ? Math.round((done / total) * 100) : 0 };
}

/** פורמט אחיד: `66% - 21/32` */
export const progressLabel = (p: { pct: number; done: number; total: number }) =>
  `${p.pct}% - ${p.done}/${p.total}`;

export function formatDate(value?: string | null) {
  if (!value) return "—";
  return new Date(value).toLocaleDateString("he-IL", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  });
}

export function formatDateTime(value?: string | null) {
  if (!value) return "—";
  return new Date(value).toLocaleString("he-IL", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

/** ממיר ערך PnL גולמי למספר תקין, או null אם אין ערך שמיש. */
export function normalizePnl(raw: unknown): number | null {
  if (raw === null || raw === undefined) return null;
  if (typeof raw === "number") return Number.isFinite(raw) ? raw : null;
  if (typeof raw !== "string") return null;
  const cleaned = raw.replace(/[,\s\u200f\u200e]/g, "").replace(/[^0-9.+-]/g, "");
  if (!cleaned || !/\d/.test(cleaned)) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

/** תצוגת PnL בפורמט אחיד עם סימן. */
export function formatPnl(n: number) {
  return `${n > 0 ? "+" : n < 0 ? "-" : ""}${Math.abs(n).toLocaleString("he-IL", {
    maximumFractionDigits: 2,
  })}`;
}

export const pnlToneClass = (n: number) =>
  n > 0 ? "text-success" : n < 0 ? "text-destructive" : "text-muted-foreground";
