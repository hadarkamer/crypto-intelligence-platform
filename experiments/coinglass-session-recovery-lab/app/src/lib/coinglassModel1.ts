/**
 * זיהוי שדות מפות החיסולים של CoinGlass (Model 1/2/3) ואימות תוצאות ניתוח חזותי.
 * המודול טהור (ללא Supabase / רשת) כדי שיהיה ניתן לבדיקה ולשימוש בשרת ובלקוח.
 */

/** שלוש מפות החום הנתמכות. כל מודל הוא מסלול מקור נפרד לחלוטין. */
export const HEATMAP_MODELS = [1, 2, 3] as const;
export type HeatmapModel = (typeof HEATMAP_MODELS)[number];

export const isHeatmapModel = (value: unknown): value is HeatmapModel =>
  value === 1 || value === 2 || value === 3;

/** הכתובות הקנוניות שאומתו מול האתר החי. */
export const HEATMAP_MODEL_URL: Record<HeatmapModel, string> = {
  1: "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol",
  2: "https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin=BTC&type=symbol",
  3: "https://www.coinglass.com/pro/futures/LiquidationHeatMapModel3?coin=BTC&type=symbol",
};

/** The live Model 3 route and the older saved-result route identify the same
 * model. Keep the stored canonical identity compatible with prior captures. */
export const HEATMAP_MODEL_SOURCE_URLS: Record<HeatmapModel, readonly string[]> = {
  1: [HEATMAP_MODEL_URL[1]],
  2: [HEATMAP_MODEL_URL[2]],
  3: [
    HEATMAP_MODEL_URL[3],
    "https://www.coinglass.com/liquidation-heatmap-model3?coin=BTC&type=symbol",
  ],
};

export const canonicalHeatmapSource = (value: unknown, model: HeatmapModel): string | null =>
  typeof value === "string" && HEATMAP_MODEL_SOURCE_URLS[model].includes(value)
    ? HEATMAP_MODEL_URL[model]
    : null;

export const HEATMAP_MODEL_LABEL: Record<HeatmapModel, string> = {
  1: "Model 1",
  2: "Model 2",
  3: "Model 3",
};

export const COINGLASS_MODEL1_URL = HEATMAP_MODEL_URL[1];

/** מסלול מדויק לכל מודל. כל מסלול אחר אינו מפת חום נתמכת. */
const MODEL_PATHNAME: Record<HeatmapModel, string> = {
  1: "/pro/futures/liquidationheatmap",
  2: "/pro/futures/liquidationheatmapnew",
  3: "/pro/futures/liquidationheatmapmodel3",
};

/**
 * מסלולי CoinGlass מוכרים שאינם מפת חום — אינם נסרקים ואינם פוסלים את השדה.
 * למשל הקישור הנוסף בשדה Model 3 (LiquidationMap) שנשאר ידני לחלוטין.
 */
const IGNORED_PATHNAMES = new Set(["/pro/futures/liquidationmap"]);

const COINGLASS_HOSTS = new Set(["coinglass.com", "www.coinglass.com"]);

export const CAPTURE_TIMEFRAMES = ["12H", "24H", "48H"] as const;
export type CaptureTimeframe = (typeof CAPTURE_TIMEFRAMES)[number];

export const isCaptureTimeframe = (value: unknown): value is CaptureTimeframe =>
  typeof value === "string" && (CAPTURE_TIMEFRAMES as readonly string[]).includes(value);

/** התווית שמופיעה בבורר הטווח באתר המקור (אומת מול ה-DOM החי). */
export const TIMEFRAME_LABEL: Record<CaptureTimeframe, string> = {
  "12H": "12 hour",
  "24H": "24 hour",
  "48H": "48 hour",
};

export const TIMEFRAME_HOURS: Record<CaptureTimeframe, number> = {
  "12H": 12,
  "24H": 24,
  "48H": 48,
};

type FieldLike = {
  field_type?: string | null;
  title?: string | null;
  settings?: unknown;
};

const normalize = (value: unknown) =>
  typeof value === "string" ? value.trim().toLocaleLowerCase() : "";

function settingsLinks(settings: unknown): string[] {
  if (!settings || typeof settings !== "object") return [];
  const raw = settings as { links?: unknown; links_json?: unknown; autofill_source_urls?: unknown };
  if (Object.prototype.hasOwnProperty.call(raw, "autofill_source_urls")) {
    const selected = raw.autofill_source_urls;
    return Array.isArray(selected) && selected.every((url) => typeof url === "string")
      ? selected.map((url: string) => url.trim()).filter(Boolean)
      : [];
  }
  const lists = [raw.links, raw.links_json].filter(Array.isArray) as unknown[][];
  const urls: string[] = [];
  for (const list of lists)
    for (const link of list) {
      const url = link && typeof link === "object" ? (link as { url?: unknown }).url : undefined;
      if (typeof url === "string" && url.trim()) urls.push(url.trim());
    }
  return urls;
}

/** מודל מזוהה, מסלול CoinGlass מוכר שאינו נסרק, מסלול CoinGlass זר, או כתובת שאינה CoinGlass. */
export type LinkVerdict = HeatmapModel | "ignored" | "other-coinglass" | "unrelated";

/** התאמה לפי URL מלא: סכימה, מארח מדויק, מסלול מדויק ופרמטרים — לא substring. */
export function classifyCoinglassLink(rawUrl: string): LinkVerdict {
  let url: URL;
  try {
    url = new URL(rawUrl);
  } catch {
    return "unrelated";
  }
  if (url.protocol !== "https:") return "unrelated";
  const host = url.hostname.toLocaleLowerCase();
  if (!COINGLASS_HOSTS.has(host)) return "unrelated";
  if (url.username || url.password || url.port) return "other-coinglass";
  const path = url.pathname.replace(/\/+$/, "").toLocaleLowerCase();
  if (IGNORED_PATHNAMES.has(path)) return "ignored";
  const model = HEATMAP_MODELS.find(
    (candidate) =>
      MODEL_PATHNAME[candidate] === path ||
      (candidate === 3 && path === "/liquidation-heatmap-model3"),
  );
  if (!model) return "other-coinglass";
  if (url.searchParams.getAll("coin").length !== 1 || url.searchParams.getAll("type").length !== 1)
    return "other-coinglass";
  const coin = (url.searchParams.get("coin") ?? "").trim().toLocaleLowerCase();
  const type = (url.searchParams.get("type") ?? "").trim().toLocaleLowerCase();
  if (coin !== "btc" || type !== "symbol") return "other-coinglass";
  return model;
}

/**
 * זהות לוגית יציבה: סוג השדה חייב להיות טבלת נזילות, והמקור חייב להיות
 * מפת חיסולים של BTC באחד משלושת המודלים, לפי ה-URL המדויק שהוגדר בשדה.
 * שני מודלים סותרים באותו שדה, או קישור CoinGlass לא מוכר, פוסלים את השדה.
 */
export function detectCoinglassHeatmapModel(field: FieldLike): HeatmapModel | null {
  if (normalize(field.field_type) !== "liquidity_table") return null;
  const verdicts = settingsLinks(field.settings).map(classifyCoinglassLink);
  if (
    !verdicts.length &&
    field.settings &&
    Object.prototype.hasOwnProperty.call(field.settings, "autofill_source_urls")
  )
    return null;
  if (verdicts.includes("other-coinglass")) return null;
  const models = new Set<HeatmapModel>();
  for (const verdict of verdicts) if (typeof verdict === "number") models.add(verdict);
  if (models.size > 1) return null;
  const title = normalize(field.title);
  const titleMatches = title.match(/model\s*(\d{1,2})/g) ?? [];
  const titleModel =
    titleMatches.length === 1 ? Number((titleMatches[0].match(/\d{1,2}/) ?? ["0"])[0]) : null;
  const [only] = [...models];
  if (only) {
    // כותרת שמכריזה על מודל אחר מהקישור — סתירה, לא מזהים שדה.
    if (titleModel !== null && titleModel !== only) return null;
    return only;
  }
  if (
    field.settings &&
    Object.prototype.hasOwnProperty.call(field.settings, "autofill_source_urls")
  )
    return null;
  if (verdicts.some((verdict) => verdict === "ignored")) return null;
  // אין קישור מפת חום כלל — נפילה אחורה לכותרת בלבד.
  if (!title.includes("coinglass") && !title.includes("קוינגאלס")) return null;
  return titleModel !== null && isHeatmapModel(titleModel) ? titleModel : null;
}

export const isCoinglassHeatmapField = (field: FieldLike): boolean =>
  detectCoinglassHeatmapModel(field) !== null;

export function isCoinglassModel1Field(field: FieldLike): boolean {
  return detectCoinglassHeatmapModel(field) === 1;
}

/* ------------------------------ אימות ניתוח ------------------------------ */

export type ZoneIntensity = "many" | "normal" | "few";
export type ZoneSide = "above" | "below";

export type AnalysedZone = {
  side: ZoneSide;
  price_text: string;
  amount: ZoneIntensity;
};

export type AnalysisResult = {
  observed_price: number;
  above: AnalysedZone[];
  below: AnalysedZone[];
};

export class AnalysisError extends Error {
  code: string;
  constructor(message: string, code: string) {
    super(message);
    this.code = code;
    this.name = "AnalysisError";
  }
}

export const MAX_ZONES_PER_SIDE = 5;
const MAX_RAW_ZONES = 12;

const usd = (value: number) => Math.round(value).toLocaleString("en-US");

export const formatPriceRange = (low: number, high: number) =>
  low === high ? `$${usd(low)}` : `$${usd(low)}–$${usd(high)}`;

/** מספר JSON אמיתי בלבד — מחרוזות, בוליאנים ו-null נפסלים. */
function jsonNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0)
    throw new AnalysisError(`ערך מספרי לא תקין עבור ${label}`, "invalid_number");
  return value;
}

const modelPattern = (model: HeatmapModel) => new RegExp(`^(?:model\\s*)?${model}$`);
const SYMBOL_PATTERN = /^btc(?:[/\- ]?(?:usd|usdt|perp))?$/;
const TIMEFRAME_PATTERN = /^(\d{1,3})\s*(?:h|hr|hrs|hour|hours)$/;

/**
 * אימות קשיח של פלט המודל. כישלון סוגר — עדיף לא לכתוב כלום מאשר לנחש.
 * הזהות (readable / model / symbol / timeframe) חייבת להופיע במפורש בתשובה;
 * הטווח שביקשנו אינו ראיה לטווח שנצפה בפועל.
 */
export function validateAnalysis(
  raw: unknown,
  timeframe: CaptureTimeframe,
  expectedModel: HeatmapModel = 1,
): AnalysisResult {
  if (!raw || typeof raw !== "object" || Array.isArray(raw))
    throw new AnalysisError("תשובת הניתוח ריקה", "empty");
  const data = raw as Record<string, unknown>;

  const readable = data.readable ?? data.chart_readable;
  if (readable !== true) throw new AnalysisError("הגרף לא נקרא בוודאות", "unreadable");

  // בלי אזורים אין מה לאמת — נכשל סגור לפני שאר הבדיקות.
  if (!Array.isArray(data.zones) || data.zones.length === 0)
    throw new AnalysisError("לא זוהו אזורי נזילות", "no_zones");

  const verifySource = () => {
    const model = normalize(data.model);
    if (!model) throw new AnalysisError("הניתוח לא זיהה את המודל", "missing_model");
    if (!modelPattern(expectedModel).test(model))
      throw new AnalysisError(
        `הצילום אינו של ${HEATMAP_MODEL_LABEL[expectedModel]}`,
        "wrong_model",
      );
    const symbol = normalize(data.symbol);
    if (!symbol) throw new AnalysisError("הניתוח לא זיהה את הנכס", "missing_symbol");
    if (!SYMBOL_PATTERN.test(symbol)) throw new AnalysisError("הצילום אינו של BTC", "wrong_symbol");
    const seenTimeframe = normalize(data.timeframe);
    if (!seenTimeframe) throw new AnalysisError("הניתוח לא זיהה טווח זמן", "missing_timeframe");
    const match = TIMEFRAME_PATTERN.exec(seenTimeframe);
    if (!match || Number(match[1]) !== TIMEFRAME_HOURS[timeframe])
      throw new AnalysisError("טווח הזמן בצילום אינו תואם", "wrong_timeframe");
  };

  const observed = jsonNumber(data.observed_price, "מחיר נוכחי");
  if (!Array.isArray(data.zones)) throw new AnalysisError("לא זוהו אזורי נזילות", "no_zones");
  if (data.zones.length > MAX_RAW_ZONES)
    throw new AnalysisError("יותר מדי אזורים בתשובה", "too_many_zones");

  const above: AnalysedZone[] = [];
  const below: AnalysedZone[] = [];
  for (const rawZone of data.zones) {
    if (!rawZone || typeof rawZone !== "object" || Array.isArray(rawZone))
      throw new AnalysisError("אזור נזילות לא תקין", "invalid_zone");
    const zone = rawZone as Record<string, unknown>;
    const low = jsonNumber(zone.price_low, "גבול תחתון");
    const high = jsonNumber(zone.price_high ?? zone.price_low, "גבול עליון");
    if (high < low) throw new AnalysisError("טווח מחירים הפוך", "invalid_range");
    const intensity = zone.intensity;
    if (intensity !== "many" && intensity !== "normal" && intensity !== "few")
      throw new AnalysisError("עוצמה לא תקינה", "invalid_intensity");
    const side = zone.side;
    if (side !== "above" && side !== "below") throw new AnalysisError("צד לא תקין", "invalid_side");
    // אזור שחוצה את מחיר השוק אינו ניתן לשיוך חד-משמעי — נפסל.
    if (low <= observed && high >= observed)
      throw new AnalysisError("אזור חוצה את המחיר הנוכחי", "crosses_price");
    if (side === "above" && low <= observed)
      throw new AnalysisError("אזור מעל שאינו מעל המחיר", "side_mismatch");
    if (side === "below" && high >= observed)
      throw new AnalysisError("אזור מתחת שאינו מתחת למחיר", "side_mismatch");
    (side === "above" ? above : below).push({
      side,
      price_text: formatPriceRange(low, high),
      amount: intensity,
    });
  }
  if (!above.length && !below.length) throw new AnalysisError("לא זוהו אזורי נזילות", "no_zones");
  // אימות מקור: מודל, נכס וטווח זמן חייבים להתאים לבקשה.
  verifySource();
  return {
    observed_price: observed,
    above: above.slice(0, MAX_ZONES_PER_SIDE),
    below: below.slice(0, MAX_ZONES_PER_SIDE),
  };
}

/** ניקוי וקיצוץ של רשימת הטווחים שהתבקשה: ייחודיים, חוקיים, עד שלושה. */
export function normalizeTimeframes(value: unknown): CaptureTimeframe[] {
  if (!Array.isArray(value)) return [];
  const unique: CaptureTimeframe[] = [];
  for (const item of value)
    if (isCaptureTimeframe(item) && !unique.includes(item)) unique.push(item);
  return unique.slice(0, CAPTURE_TIMEFRAMES.length);
}

/* --------------------------- שלבי תפיסה בדף המקור --------------------------- */

export type CaptureStages = {
  /** איך טופל דיאלוג אופציונלי: closed = נלחץ כפתור סגירה גלוי, escape = נשלח Escape, none = לא נמצא דיאלוג. */
  dismissed: "closed" | "escape" | "none";
  /** דחיית דיאלוג אחרי החלפת הטווח, מיד לפני השער הסופי והצילום. */
  finalDismissed: "closed" | "escape" | "none";
  openedLabel: string;
  verifiedLabel: string;
  canvasCount: number;
};

/**
 * ניתוח תשובות שלבי ה-executeJavascript של התפיסה. כל שלב מחזיר מחרוזת עם
 * קידומת שם (dismiss: / gate: / open: / select: / verify: / finaldismiss: / finalgate:)
 * כדי שהוספת שלב חדש לא תזיז אינדקסים ולא תשבור את אימות הטווח.
 * השער הסופי (finalgate) — אחרי הבחירה והטעינה, כולל אימות ה-URL המדויק —
 * הוא זה שנבדק כתנאי לצילום. כישלון סוגר: חסימה, טשטוש, מקור שגוי, שלב חסר
 * או כפול, מטא-אימות לא תקין או טווח לא תואם — לא שולחים לניתוח.
 */
export function parseCaptureStages(values: unknown[], expectedLabel: string): CaptureStages {
  const texts = values.map((value) => String(value ?? ""));
  const stage = (prefix: string): string => {
    const hits = texts.filter((text) => text.startsWith(`${prefix}:`));
    if (!hits.length)
      throw new AnalysisError(`שלב ${prefix} חסר בתשובת ספק הצילום`, "missing_stage");
    if (hits.length > 1)
      throw new AnalysisError(`שלב ${prefix} הופיע יותר מפעם אחת`, "duplicate_stage");
    return hits[0].slice(prefix.length + 1);
  };

  const asDismissal = (raw: string): CaptureStages["dismissed"] =>
    raw === "closed" || raw === "escape" ? raw : "none";

  const dismissed = asDismissal(stage("dismiss"));

  const gate = stage("gate");
  if (gate.startsWith("blocked"))
    throw new AnalysisError(
      "המקור דורש התחברות או שהגרף מטושטש — לא בוצע ניתוח ולא נשמר דבר",
      "gate_blocked",
    );
  if (gate !== "clear") throw new AnalysisError("בדיקת הנגישות של הגרף נכשלה", "gate_unknown");

  const open = stage("open");
  if (!open || open === "no-trigger")
    throw new AnalysisError("בורר הטווח לא נפתח בדף המקור", "open_failed");

  if (stage("select") !== "selected")
    throw new AnalysisError(`לא נמצאה האפשרות "${expectedLabel}" בבורר הטווח`, "select_failed");

  const finalDismissed = asDismissal(stage("finaldismiss"));

  // השער הסופי — אחרי הבחירה, כולל אימות ה-URL המדויק של Model 1 — הוא הנבדק.
  const finalGate = stage("finalgate");
  if (finalGate === "blocked:wrong-source")
    throw new AnalysisError("הדף הסופי אינו עמוד Model 1 של BTC ב-CoinGlass", "wrong_source");
  if (finalGate.startsWith("blocked"))
    throw new AnalysisError(
      "המקור דורש התחברות או שהגרף מטושטש — לא בוצע ניתוח ולא נשמר דבר",
      "gate_blocked",
    );
  if (finalGate !== "clear") throw new AnalysisError("בדיקת הנגישות של הגרף נכשלה", "gate_unknown");

  // שליפת השלב מחוץ ל-try כדי ששגיאות שלב (חסר/כפול) לא ייבלעו כ-verify_failed.
  const verifyRaw = stage("verify");
  let info: unknown;
  try {
    info = JSON.parse(verifyRaw);
  } catch {
    throw new AnalysisError("אימות הטווח בדף המקור נכשל", "verify_failed");
  }
  if (!info || typeof info !== "object" || Array.isArray(info))
    throw new AnalysisError("אימות הטווח בדף המקור נכשל", "verify_failed");
  const payload = info as { label?: unknown; canvas?: unknown };
  const verifiedLabel = typeof payload.label === "string" ? payload.label : "";
  if (verifiedLabel.trim().toLocaleLowerCase() !== expectedLabel.trim().toLocaleLowerCase())
    throw new AnalysisError(
      `הטווח בדף נשאר "${verifiedLabel}" במקום "${expectedLabel}"`,
      "timeframe_mismatch",
    );
  const canvasCount = payload.canvas;
  if (
    typeof canvasCount !== "number" ||
    !Number.isFinite(canvasCount) ||
    !Number.isInteger(canvasCount) ||
    canvasCount <= 0
  )
    throw new AnalysisError("הגרף לא נטען בדף המקור", "no_canvas");

  return { dismissed, finalDismissed, openedLabel: open, verifiedLabel, canvasCount };
}

/**
 * בחירת הצילום הנכון מתשובת הספק: צילום הפעולה האחרון (הדף המלא אחרי השער
 * הסופי) עדיף על תמונת ברירת המחדל של הספק, שיכולה להיקלט לפני הפעולות.
 */
export function pickCaptureScreenshot(data: unknown): string {
  if (!data || typeof data !== "object") return "";
  const record = data as { screenshot?: unknown; actions?: unknown };
  const shots =
    record.actions &&
    typeof record.actions === "object" &&
    Array.isArray((record.actions as { screenshots?: unknown }).screenshots)
      ? ((record.actions as { screenshots: unknown[] }).screenshots as unknown[])
      : [];
  const last = shots.at(-1);
  if (typeof last === "string" && last) return last;
  return typeof record.screenshot === "string" ? record.screenshot : "";
}

/** תיאור עברי קבוע לסיווג החזותי היחסי, לשמירה במטא-דאטה. */
export const RELATIVE_SCALE_NOTE =
  "העוצמה (הרבה/רגיל/מעט) היא סיווג חזותי יחסי לאותו גרף בלבד, ואינה גודל כספי מאומת או יעד מחיר.";
