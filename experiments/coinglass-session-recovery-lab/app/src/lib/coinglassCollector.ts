/**
 * חוזה v1 מול שירות האיסוף החיצוני של מפות החיסולים ב-CoinGlass (Python + OpenAI).
 * מודול טהור: אימות תצורה, אימות מעטפת/תוצאה, המרה דטרמיניסטית לשורות הטבלה
 * ובדיקת טריות. אין כאן רשת, אין Supabase ואין ניחוש — כישלון סוגר.
 *
 * שלושת המודלים חולקים בדיוק את אותו מבנה תוצאה; מה שמשתנה הוא גרסת הסכימה,
 * מקטע ה-API, וכתובת המקור הקנונית. Model 1 נשאר תואם-בייט לחוזה הקודם.
 */
import {
  HEATMAP_MODELS,
  HEATMAP_MODEL_LABEL,
  HEATMAP_MODEL_URL,
  isHeatmapModel,
  canonicalHeatmapSource,
  type HeatmapModel,
} from "./coinglassModel1.ts";
import {
  validateCoinglassQuantities,
  quantityForZone,
  type CoinglassQuantitySnapshot,
} from "./coinglassQuantities.ts";
import {
  validateHoverSnapshot,
  hoverQuantityForZone,
  hoverLevelData,
  type HoverSnapshot,
} from "./coinglassHover.ts";

export {
  HEATMAP_MODELS,
  HEATMAP_MODEL_LABEL,
  HEATMAP_MODEL_URL,
  isHeatmapModel,
  type HeatmapModel,
};

export const collectorSchemaVersion = (model: HeatmapModel) => `coinglass-model${model}.v1`;
export const COLLECTOR_SCHEMA_VERSION = collectorSchemaVersion(1);

export const collectorSourceUrl = (model: HeatmapModel) => HEATMAP_MODEL_URL[model];

/** השירות תומך ב-12H/24H/48H; כל בקשה אוספת ושומרת רק את הטווח שנבחר. */
export const COLLECTOR_TIMEFRAMES = ["12H", "24H", "48H"] as const;
export type CollectorTimeframe = (typeof COLLECTOR_TIMEFRAMES)[number];

export const COLLECTOR_SUPPORT_NOTE =
  "האיסוף האוטומטי תומך ב-12H, 24H ו-48H. כל הפעלה מנתחת ושומרת את הטווח שנבחר; שאר הנתונים בגיליון אינם משתנים.";

export const COLLECTOR_SOURCE_URL = HEATMAP_MODEL_URL[1];

/** תוצאה ישנה מדי לא נשמרת אוטומטית — מוצגת כמיושנת במקום להיחתם כ"עכשיו". */
export const MAX_AUTO_SAVE_AGE_MS = 15 * 60 * 1000;
const MAX_FUTURE_SKEW_MS = 2 * 60 * 1000;

export const isCollectorTimeframe = (value: unknown): value is CollectorTimeframe =>
  typeof value === "string" && (COLLECTOR_TIMEFRAMES as readonly string[]).includes(value);

export class CollectorContractError extends Error {
  code: string;
  constructor(message: string, code: string) {
    super(message);
    this.code = code;
    this.name = "CollectorContractError";
  }
}

function fail(message: string, code: string): never {
  throw new CollectorContractError(message, code);
}

export const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isUuid = (value: unknown): value is string =>
  typeof value === "string" && UUID_PATTERN.test(value);

const SHA256_PATTERN = /^[0-9a-f]{64}$/i;

/* ------------------------------- תצורת השירות ------------------------------- */

/** אורך מינימלי לאישור הגשר — תואם למינימום שהשירות עצמו אוכף. */
export const MIN_COLLECTOR_TOKEN_LENGTH = 32;

const IPV4_LITERAL = /^\d{1,3}(?:\.\d{1,3}){3}$/;
const DNS_LABEL = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;
const LOCAL_SUFFIX = [".local", ".internal", ".localhost", ".home", ".lan", ".test", ".invalid"];

/**
 * בסיס חוקי: HTTPS בפורט ברירת המחדל, שם מארח ציבורי אמיתי (לא כתובת IP ולא שם
 * מקומי), בלי שם משתמש/נתיב/שאילתה/פרגמנט. הכתובת נקבעת בשרת בלבד.
 */
export function parseCollectorBase(raw: unknown): string {
  if (typeof raw !== "string" || !raw.trim()) fail("כתובת שירות האיסוף חסרה", "missing_base");
  let url: URL;
  try {
    url = new URL((raw as string).trim());
  } catch {
    return fail("כתובת שירות האיסוף אינה תקינה", "invalid_base");
  }
  if (url.protocol !== "https:") fail("כתובת שירות האיסוף חייבת להיות HTTPS", "insecure_base");
  if (url.username || url.password) fail("כתובת שירות האיסוף אינה תקינה", "invalid_base");
  if (url.search || url.hash) fail("כתובת שירות האיסוף אינה תקינה", "invalid_base");
  if (url.pathname !== "/" && url.pathname !== "")
    fail("כתובת שירות האיסוף חייבת להיות מקור בלבד", "invalid_base");
  // url.port ריק כשהפורט הוא 443; כל פורט אחר נדחה.
  if (url.port && url.port !== "443") fail("פורט לא מקובל בכתובת השירות", "invalid_port");
  const host = url.hostname.toLocaleLowerCase();
  if (host.startsWith("[")) fail("כתובת IP אינה מקובלת כשירות איסוף", "ip_literal");
  if (IPV4_LITERAL.test(host)) fail("כתובת IP אינה מקובלת כשירות איסוף", "ip_literal");
  const labels = host.split(".");
  if (labels.length < 2 || labels.some((label) => !DNS_LABEL.test(label)))
    fail("שם המארח של שירות האיסוף אינו תקין", "invalid_host");
  const tld = labels[labels.length - 1] ?? "";
  if (tld.length < 2 || /^\d+$/.test(tld))
    fail("שם המארח של שירות האיסוף אינו תקין", "invalid_host");
  if (host === "localhost" || LOCAL_SUFFIX.some((suffix) => host.endsWith(suffix)))
    fail("כתובת שירות האיסוף חייבת להיות כתובת ציבורית", "private_base");
  return url.origin;
}

/** אישור גשר חוקי: מחרוזת ארוכה מספיק. הערך עצמו לעולם אינו מודפס. */
export function assertCollectorToken(token: unknown): string {
  if (typeof token !== "string" || token.trim().length < MIN_COLLECTOR_TOKEN_LENGTH)
    fail(`אישור הגשר חייב להיות באורך ${MIN_COLLECTOR_TOKEN_LENGTH} תווים לפחות`, "invalid_token");
  return (token as string).trim();
}

const assertModel = (model: unknown): HeatmapModel => {
  if (!isHeatmapModel(model)) fail("מודל מפת חום לא נתמך", "invalid_model");
  return model;
};

/** מקטע API נפרד לכל מודל; המשימות והמטמון מופרדים לחלוטין לפי מודל וטווח. */
export const jobsUrl = (base: string, model: HeatmapModel = 1) =>
  `${parseCollectorBase(base)}/api/collection/model${assertModel(model)}/jobs`;
export const jobUrl = (base: string, jobId: string, model: HeatmapModel = 1) => {
  if (!isUuid(jobId)) fail("מזהה משימה לא תקין", "invalid_job_id");
  return `${jobsUrl(base, model)}/${jobId}`;
};

/** קריאה בלבד של התצפית האחרונה שכבר נאספה — לעולם אינה פותחת סריקה. */
export const latestJobUrl = (
  base: string,
  timeframe: CollectorTimeframe,
  model: HeatmapModel = 1,
) => {
  if (!isCollectorTimeframe(timeframe)) fail("טווח זמן לא נתמך", "invalid_timeframe");
  return `${jobsUrl(base, model)}/latest?timeframe=${encodeURIComponent(timeframe)}`;
};

/** בדיקת בריאות/יכולות: /health מפרסם את המודלים והטווחים שהשירות תומך בהם. */
export const healthUrl = (base: string) => `${parseCollectorBase(base)}/health`;

export type CollectorCapabilities = {
  models: HeatmapModel[];
  timeframes: CollectorTimeframe[];
  selectionPolicy?: "prominent_right_edge.v1";
};

/**
 * פענוח שמרני של /health. מה שלא פורסם במפורש אינו נתמך — כדי שלא נפנה
 * למודל 2/3 מול גרסת שרת ישנה שאינה מכירה אותם.
 */
export function parseCollectorCapabilities(raw: unknown): CollectorCapabilities {
  const data =
    raw && typeof raw === "object" && !Array.isArray(raw) ? (raw as Record<string, unknown>) : {};
  const rawModels = Array.isArray(data.supported_models) ? data.supported_models : [];
  const rawTimeframes = Array.isArray(data.supported_timeframes) ? data.supported_timeframes : [];
  // Missing, empty or partly invalid declarations do not authorize any jobs.
  if (
    data.ok !== true ||
    !rawModels.length ||
    !rawTimeframes.length ||
    rawModels.some((model) => !isHeatmapModel(model)) ||
    rawTimeframes.some((timeframe) => !isCollectorTimeframe(timeframe))
  )
    return { models: [], timeframes: [] };
  const models = HEATMAP_MODELS.filter((model) => rawModels.includes(model));
  const timeframes = COLLECTOR_TIMEFRAMES.filter((timeframe) => rawTimeframes.includes(timeframe));
  return {
    models,
    timeframes,
    ...(data.selection_policy === "prominent_right_edge.v1"
      ? { selectionPolicy: "prominent_right_edge.v1" as const }
      : {}),
  };
}

/* --------------------------------- מעטפת --------------------------------- */

export type CollectorZone = {
  side: "above" | "below";
  price_low: number;
  price_high: number;
  intensity: "many" | "normal" | "few";
  /** Monetary values are derived only from verified source-data buckets. */
  amount_usd?: number;
  quantity_bucket_ids?: string[];
  source_row_ids?: string[];
  source_value?: number;
  shape?: "point" | "adjacent_prices" | "range";
  row_colour?: "yellow";
  continuous?: true;
  weak_gap_present?: false;
  price_precision?: "verified_label" | "axis_estimate";
  prices?: number[];
  evidence_basis?: "numeric_row_label" | "tooltip" | "axis_interpolation";
  price_evidence?: string;
  uncertainty_usd?: number;
  strength_score?: number;
};

/**
 * הפניית מחיר: תוצאה ישנה נושאת נקודת מחיר מדויקת; תוצאה חדשה נושאת טווח
 * ויזואלי צר בלבד. אין אמצע טווח ואין דיוק מדומה.
 */
export type CollectorPriceReferenceType = "exact_point" | "visual_range";

export type CollectorPriceRange = {
  low: number;
  high: number;
  confidence: "high" | "medium";
  basis: "last_candle_axis_bracket";
  axis_low: number;
  axis_high: number;
};

/** מגבלת רוחב הנדסית ראשונית (1%) — אינה סף מסחר ואינה הוכחת דיוק. */
export const MAX_PRICE_RANGE_WIDTH_RATIO = 0.01;
const RANGE_WIDTH_TOLERANCE = 1e-12;

export type CollectorResult = {
  schema_version: string;
  level_contract_version?: "prominent-rows.v1";
  selection_policy?: "prominent_right_edge.v1";
  run_id: string;
  source_url: string;
  symbol: "BTC";
  heatmap_model: HeatmapModel;
  timeframe: CollectorTimeframe;
  captured_at: string;
  source_updated_at: string | null;
  provider: string;
  model: string;
  usage: unknown;
  observed_price: number | null;
  price_reference_type: CollectorPriceReferenceType;
  observed_price_range: CollectorPriceRange | null;
  price_axis_range?: { low: number; high: number; price_evidence: string };
  omitted_ambiguous_zones: number;
  zones: CollectorZone[];
  quantities?: CoinglassQuantitySnapshot;
  hover_snapshot?: HoverSnapshot;
  quantity_status?: "unavailable";
  quantity_reason?: "source_usd_unverified";
  evidence: { sha256: string; artifact_id: string; content_type: "image/png" };
  summary: string;
  quality: "visual_estimate";
};

export type CollectorStatus = "queued" | "running" | "ready" | "failed";

export type CollectorEnvelope = {
  schema_version: string;
  job_id: string;
  timeframe: CollectorTimeframe;
  status: CollectorStatus;
  result: CollectorResult | null;
  error: { code: string; message: string } | null;
};

function positive(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0)
    fail(`ערך מספרי לא תקין (${label})`, "invalid_number");
  return value as number;
}

/** Precision comes from the source observation, never from range width. */
function validateZoneMetadata(
  zone: Record<string, unknown>,
  low: number,
  high: number,
  required: boolean,
): Partial<CollectorZone> {
  const keys = [
    "shape",
    "price_precision",
    "prices",
    "evidence_basis",
    "price_evidence",
    "uncertainty_usd",
    "strength_score",
  ];
  if (!keys.some((key) => zone[key] !== undefined)) {
    if (required) fail("חסרים דיוק וראיות לרמת הנזילות", "missing_zone_precision");
    return {};
  }
  const shape = zone.shape;
  const precision = zone.price_precision;
  const basis = zone.evidence_basis;
  if (shape !== "point" && shape !== "adjacent_prices" && shape !== "range")
    fail("צורת רמת הנזילות אינה תקינה", "invalid_zone_shape");
  if (precision !== "verified_label" && precision !== "axis_estimate")
    fail("דיוק רמת הנזילות אינו תקין", "invalid_zone_precision");
  const prices = zone.prices;
  if (
    prices !== undefined &&
    (!Array.isArray(prices) || prices.some((price) => !finitePositive(price)))
  )
    fail("מחירים נפרדים אינם תקינים", "invalid_zone_prices");
  const points = prices as number[] | undefined;
  if (
    (shape === "point" && points?.length !== 1) ||
    (shape === "adjacent_prices" && points?.length !== 2) ||
    (shape === "range" && points !== undefined && points.length !== 0 && points.length !== 2) ||
    (points && points.some((price) => price < low || price > high)) ||
    (points && points.length === 2 && points[0] >= points[1]) ||
    (shape === "range" && low >= high)
  )
    fail("מחירי הרמה אינם תואמים את צורתה", "invalid_zone_prices");
  if (
    zone.price_evidence !== undefined &&
    (typeof zone.price_evidence !== "string" || zone.price_evidence.length > 1000)
  )
    fail("ראיית המחיר אינה תקינה", "invalid_zone_evidence");
  const evidence = typeof zone.price_evidence === "string" ? zone.price_evidence.trim() : "";
  if (precision === "verified_label") {
    if ((basis !== "numeric_row_label" && basis !== "tooltip") || !evidence || !points)
      fail("מחיר מדויק דורש תווית מספרית מאומתת", "missing_zone_evidence");
    const observedLabels = (
      evidence.match(/(?<![\d.])(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?!\d|\.\d)/g) ?? []
    ).map((value) => Number(value.replaceAll(",", "")));
    const exactPrices = shape === "range" ? [low, high] : points;
    if (
      exactPrices.some((price) => !observedLabels.includes(price)) ||
      exactPrices[0] !== low ||
      exactPrices[exactPrices.length - 1] !== high
    )
      fail("ראיית המחיר אינה תואמת למחירים", "zone_evidence_mismatch");
    if (zone.uncertainty_usd !== undefined && zone.uncertainty_usd !== 0)
      fail("מחיר מאומת אינו אומדן ציר", "conflicting_zone_precision");
  } else if (basis !== "axis_interpolation" || !finitePositive(zone.uncertainty_usd)) {
    fail("אומדן ציר דורש טווח אי ודאות", "missing_zone_uncertainty");
  }
  if (
    precision === "axis_estimate" &&
    shape !== "range" &&
    points &&
    points.some((price) => (zone.uncertainty_usd as number) < Math.max(price - low, high - price))
  )
    fail("אי הוודאות אינה מכסה את גבולות אומדן המחיר", "zone_uncertainty_mismatch");
  if (
    zone.strength_score !== undefined &&
    (typeof zone.strength_score !== "number" ||
      !Number.isFinite(zone.strength_score) ||
      zone.strength_score < 0 ||
      zone.strength_score > 1)
  )
    fail("דירוג העוצמה אינו תקין", "invalid_strength_score");
  if (
    required &&
    ((zone.intensity === "many" &&
      shape === "range" &&
      (zone.row_colour !== "yellow" ||
        zone.continuous !== true ||
        zone.weak_gap_present !== false ||
        !Array.isArray(prices) ||
        prices.length !== 0)) ||
      (zone.intensity === "normal" && shape !== "range"))
  )
    fail("הצורה אינה תואמת פס צהוב או רצף ירוק", "zone_shape_intensity_mismatch");
  return {
    shape,
    price_precision: precision,
    ...(required && zone.intensity === "many" && shape === "range"
      ? {
          row_colour: "yellow" as const,
          continuous: true as const,
          weak_gap_present: false as const,
        }
      : {}),
    ...(points ? { prices: [...points] } : {}),
    evidence_basis: basis as CollectorZone["evidence_basis"],
    ...(evidence ? { price_evidence: evidence } : {}),
    ...(zone.uncertainty_usd !== undefined
      ? { uncertainty_usd: zone.uncertainty_usd as number }
      : {}),
    ...(zone.strength_score !== undefined ? { strength_score: zone.strength_score as number } : {}),
  };
}

/** הספק היחיד המותר בחוזה v1. */
export const COLLECTOR_PROVIDER = "OpenAI";

/** ISO-8601 עם אזור זמן מפורש בלבד (Z או היסט). זמן בלי אזור זמן נדחה. */
const ISO_WITH_ZONE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;

/** שומר את הרגע המקורי בדיוק כפי שהתקבל — בלי חתימה מחדש כ"עכשיו". */
function isoTimestamp(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) fail(`חסר זמן (${label})`, "invalid_timestamp");
  const text = (value as string).trim();
  if (!ISO_WITH_ZONE.test(text)) fail(`זמן ללא אזור זמן מפורש (${label})`, "invalid_timestamp");
  if (!Number.isFinite(Date.parse(text))) fail(`זמן לא תקין (${label})`, "invalid_timestamp");
  return text;
}

/* ------------------------- הפניית מחיר: נקודה או טווח ------------------------- */

export type PriceReference =
  | { type: "exact_point"; point: number; range: null; omitted: number }
  | { type: "visual_range"; point: null; range: CollectorPriceRange; omitted: number };

const finitePositive = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value) && value > 0;

/** מספר שלם אי-שלילי בלבד; חסר => 0. bool/NaN/שבר נדחים. */
function omittedCount(value: unknown): number {
  if (value === undefined || value === null) return 0;
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0)
    fail("מספר האזורים שהושמטו אינו תקין", "invalid_omitted_count");
  return value;
}

/** טווח ויזואלי תקין בלבד; אין אמצע טווח ואין השלמת ערכים חסרים. */
export function validatePriceRange(raw: unknown): CollectorPriceRange {
  if (!raw || typeof raw !== "object" || Array.isArray(raw))
    fail("טווח המחיר חסר", "missing_price_range");
  const range = raw as Record<string, unknown>;
  if (range.basis !== "last_candle_axis_bracket")
    fail("בסיס הטווח אינו נתמך", "invalid_price_range");
  if (range.confidence !== "high" && range.confidence !== "medium")
    fail("רמת הוודאות של הטווח אינה מספיקה", "low_price_confidence");
  if (
    !finitePositive(range.low) ||
    !finitePositive(range.high) ||
    !finitePositive(range.axis_low) ||
    !finitePositive(range.axis_high)
  )
    fail("ערך לא תקין בטווח המחיר", "invalid_price_range");
  const {
    low,
    high,
    axis_low: axisLow,
    axis_high: axisHigh,
  } = range as unknown as {
    low: number;
    high: number;
    axis_low: number;
    axis_high: number;
  };
  if (!(low < high)) fail("טווח המחיר אינו עולה", "invalid_price_range");
  if (!(axisLow < axisHigh)) fail("סרגל הציר אינו תקין", "invalid_price_range");
  if (low < axisLow || high > axisHigh)
    fail("הטווח אינו מוכל בסוגריים של הציר", "price_range_bracket_mismatch");
  const width = (high - low) / ((high + low) / 2);
  if (width > MAX_PRICE_RANGE_WIDTH_RATIO + RANGE_WIDTH_TOLERANCE)
    fail("טווח המחיר רחב מדי", "price_range_too_wide");
  return {
    low,
    high,
    confidence: range.confidence,
    basis: "last_candle_axis_bracket",
    axis_low: axisLow,
    axis_high: axisHigh,
  };
}

/**
 * תוצאה ישנה = נקודת מחיר חיובית בלי סימון טווח. תוצאה חדשה = טווח ויזואלי
 * תקין עם observed_price ריק. נוכחות טווח לעולם אינה מחליפה נקודה חסרה אלא אם
 * price_reference_type הוא בדיוק visual_range.
 */
export function validatePriceReference(data: Record<string, unknown>): PriceReference {
  const marker = data.price_reference_type;
  const omitted = omittedCount(data.omitted_ambiguous_zones);
  if (marker === "visual_range") {
    if (data.observed_price !== null && data.observed_price !== undefined)
      fail("תוצאת טווח אינה יכולה לשאת מחיר מדויק", "conflicting_price_reference");
    return {
      type: "visual_range",
      point: null,
      range: validatePriceRange(data.observed_price_range),
      omitted,
    };
  }
  if (marker !== undefined && marker !== null && marker !== "exact_point")
    fail("סוג הפניית המחיר אינו נתמך", "invalid_price_reference");
  if (data.observed_price_range !== null && data.observed_price_range !== undefined)
    fail("טווח מחיר הוחזר בלי סימון טווח", "conflicting_price_reference");
  return {
    type: "exact_point",
    point: positive(data.observed_price, "observed_price"),
    range: null,
    omitted,
  };
}

export function validateCollectorResult(
  raw: unknown,
  timeframe: CollectorTimeframe,
  expected?: { jobId?: string; model?: HeatmapModel },
): CollectorResult {
  if (!raw || typeof raw !== "object" || Array.isArray(raw))
    fail("תוצאת האיסוף ריקה", "empty_result");
  const model = assertModel(expected?.model ?? 1);
  const data = raw as Record<string, unknown>;
  if (
    data.level_contract_version !== undefined &&
    data.level_contract_version !== "prominent-rows.v1"
  )
    fail("חוזה רמות נזילות לא נתמך", "level_contract_mismatch");
  if (data.selection_policy !== undefined && data.selection_policy !== "prominent_right_edge.v1")
    fail("מדיניות בחירת רמות לא נתמכת", "selection_policy_mismatch");
  if (
    data.selection_policy === "prominent_right_edge.v1" &&
    data.level_contract_version !== "prominent-rows.v1"
  )
    fail("בחירת הרמות החדשה דורשת דיוק וראיות לכל רמה", "missing_level_contract");
  if (data.schema_version !== collectorSchemaVersion(model))
    fail("גרסת חוזה לא נתמכת", "schema_mismatch");
  if (!isUuid(data.run_id)) fail("מזהה הרצה לא תקין", "invalid_run_id");
  const runId = (data.run_id as string).toLowerCase();
  if (expected?.jobId && runId !== expected.jobId.toLowerCase())
    fail("מזהה ההרצה אינו תואם למזהה המשימה", "run_job_mismatch");
  if (!canonicalHeatmapSource(data.source_url, model))
    fail(`מקור הצילום אינו ${HEATMAP_MODEL_LABEL[model]}`, "wrong_source");
  if (data.symbol !== "BTC") fail("הצילום אינו של BTC", "wrong_symbol");
  if (data.heatmap_model !== model)
    fail(`הצילום אינו של ${HEATMAP_MODEL_LABEL[model]}`, "wrong_model");
  if (data.timeframe !== timeframe) fail("טווח הזמן בתוצאה אינו תואם", "wrong_timeframe");
  const capturedAt = isoTimestamp(data.captured_at, "captured_at");
  // בחוזה v1 זמן העדכון במקור אינו ידוע ולכן חייב להישאר ריק.
  if (data.source_updated_at !== null && data.source_updated_at !== undefined)
    fail("זמן עדכון במקור אינו נתמך בחוזה הזה", "unexpected_source_updated_at");
  const sourceUpdatedAt = null;
  if (data.provider !== COLLECTOR_PROVIDER) fail("ספק הניתוח אינו תואם", "wrong_provider");
  if (typeof data.model !== "string" || !data.model.trim()) fail("שם המודל חסר", "missing_model");
  if (
    data.usage !== null &&
    data.usage !== undefined &&
    (typeof data.usage !== "object" || Array.isArray(data.usage))
  )
    fail("נתוני שימוש לא תקינים", "invalid_usage");
  const reference = validatePriceReference(data);
  let priceAxis: CollectorResult["price_axis_range"];
  if (data.price_axis_range !== undefined) {
    if (
      !data.price_axis_range ||
      typeof data.price_axis_range !== "object" ||
      Array.isArray(data.price_axis_range)
    )
      fail("ראיית ציר המחיר אינה תקינה", "invalid_price_axis");
    const axis = data.price_axis_range as Record<string, unknown>;
    const low = positive(axis.low, "price_axis_low");
    const high = positive(axis.high, "price_axis_high");
    if (
      low >= high ||
      typeof axis.price_evidence !== "string" ||
      !axis.price_evidence.trim() ||
      axis.price_evidence.length > 1000
    )
      fail("ראיית ציר המחיר אינה תקינה", "invalid_price_axis");
    const labels = (
      axis.price_evidence.match(/(?<![\d.])(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?!\d|\.\d)/g) ?? []
    ).map((value) => Number(value.replaceAll(",", "")));
    if (!labels.includes(low) || !labels.includes(high))
      fail("ציר המחיר אינו תואם את ראיותיו", "price_axis_evidence_mismatch");
    priceAxis = { low, high, price_evidence: axis.price_evidence.trim() };
  } else if (data.level_contract_version === "prominent-rows.v1") {
    fail("חסרה ראיית ציר המחיר", "missing_price_axis");
  }
  // refLow/refHigh הם גבולות ההפניה: בנקודה מדויקת שניהם זהים, בטווח ויזואלי
  // הם קצות הטווח — אזור שנוגע או חופף לטווח נפסל ולא נכנס לטבלה.
  const refLow = reference.range ? reference.range.low : (reference.point as number);
  const refHigh = reference.range ? reference.range.high : (reference.point as number);
  if (priceAxis && (refLow < priceAxis.low || refHigh > priceAxis.high))
    fail("המחיר הנצפה אינו מוכל בציר המקור", "price_axis_mismatch");
  if (!Array.isArray(data.zones) || data.zones.length === 0)
    fail("לא זוהו אזורי נזילות", "no_zones");
  const rawZones = data.zones as unknown[];
  if (rawZones.length > 40) fail("יותר מדי אזורים", "too_many_zones");
  const zones: CollectorZone[] = rawZones.map((rawZone): CollectorZone => {
    if (!rawZone || typeof rawZone !== "object" || Array.isArray(rawZone))
      fail("אזור נזילות לא תקין", "invalid_zone");
    const zone = rawZone as Record<string, unknown>;
    const low = positive(zone.price_low, "price_low");
    const high = positive(zone.price_high, "price_high");
    if (priceAxis && (low < priceAxis.low || high > priceAxis.high))
      fail("רמת נזילות מחוץ לציר המקור", "zone_axis_mismatch");
    if (high < low) fail("טווח מחירים הפוך", "invalid_range");
    if (zone.side !== "above" && zone.side !== "below") fail("צד לא תקין", "invalid_side");
    if (zone.intensity !== "many" && zone.intensity !== "normal" && zone.intensity !== "few")
      fail("עוצמה לא תקינה", "invalid_intensity");
    if (low <= refHigh && high >= refLow) fail("אזור חוצה את המחיר הנוכחי", "crosses_price");
    if (zone.side === "above" && low <= refHigh) fail("אזור מעל שאינו מעל המחיר", "side_mismatch");
    if (zone.side === "below" && high >= refLow)
      fail("אזור מתחת שאינו מתחת למחיר", "side_mismatch");
    return {
      side: zone.side,
      price_low: low,
      price_high: high,
      intensity: zone.intensity,
      ...validateZoneMetadata(zone, low, high, data.level_contract_version === "prominent-rows.v1"),
    };
  });
  if (
    data.level_contract_version === "prominent-rows.v1" &&
    zones.some((zone) => zone.intensity === "few") &&
    zones.some((zone) => zone.intensity !== "few")
  )
    fail("רמות חלשות אינן מצטרפות לרמות בולטות", "mixed_faint_levels");

  if (!data.evidence || typeof data.evidence !== "object" || Array.isArray(data.evidence))
    fail("ראיה חסרה", "missing_evidence");
  const evidence = data.evidence as Record<string, unknown>;
  if (typeof evidence.sha256 !== "string" || !SHA256_PATTERN.test(evidence.sha256))
    fail("חתימת הראיה אינה תקינה", "invalid_evidence");
  if (!isUuid(evidence.artifact_id)) fail("מזהה ראיה לא תקין", "invalid_evidence");
  // בחוזה v1 הראיה היא בדיוק אותה הרצה: artifact_id == run_id == job_id.
  if ((evidence.artifact_id as string).toLowerCase() !== runId)
    fail("מזהה הראיה אינו תואם למזהה ההרצה", "evidence_mismatch");
  if (evidence.content_type !== "image/png") fail("סוג ראיה לא נתמך", "invalid_evidence");
  if (data.quality !== "visual_estimate") fail("סוג איכות לא נתמך", "invalid_quality");
  let quantities: CoinglassQuantitySnapshot | null;
  try {
    quantities = validateCoinglassQuantities(data.quantities, {
      model,
      timeframe,
      capturedAt,
      sourceUrl: data.source_url as string,
      artifactId: evidence.artifact_id as string,
      sha256: evidence.sha256 as string,
      observedPrice: reference.point,
      observedRange: reference.range,
    });
    if (
      (data.quantity_status !== undefined || data.quantity_reason !== undefined) &&
      (data.quantity_status !== "unavailable" ||
        data.quantity_reason !== "source_usd_unverified" ||
        quantities)
    )
      throw new Error("invalid_quantity_status");
    const usedBuckets = new Set<string>();
    for (let index = 0; index < zones.length; index++) {
      const rawZone = rawZones[index] as Record<string, unknown>;
      const quantity = quantityForZone(quantities, {
        ...zones[index],
        quantity_bucket_ids: rawZone.quantity_bucket_ids,
      });
      if (rawZone.amount_usd !== undefined && rawZone.amount_usd !== quantity?.amount_usd)
        throw new Error("unverified_amount");
      if (quantity) {
        if (quantity.quantity_bucket_ids.some((id) => usedBuckets.has(id)))
          throw new Error("reused_quantity_bucket");
        quantity.quantity_bucket_ids.forEach((id) => usedBuckets.add(id));
        zones[index] = { ...zones[index], ...quantity };
      }
    }
  } catch {
    return fail("סכומי הדולר לא אומתו מול נתוני המקור", "invalid_quantities");
  }
  let hover: HoverSnapshot | null;
  try {
    hover = validateHoverSnapshot(data.hover_snapshot, {
      model,
      timeframe,
      capturedAt,
      sourceUrl: data.source_url as string,
      artifactId: evidence.artifact_id as string,
      sha256: evidence.sha256 as string,
      observedRange: reference.range,
      observedPrice: reference.point,
    });
    const used = new Set<string>();
    for (let index = 0; index < zones.length; index++) {
      const rawZone = rawZones[index] as Record<string, unknown>;
      const quantity = hoverQuantityForZone(hover, {
        ...zones[index],
        source_row_ids: rawZone.source_row_ids,
        source_value: rawZone.source_value,
      });
      if (hover && !quantity) throw new Error("missing_hover_pool");
      if (!quantity && rawZone.source_value !== undefined)
        throw new Error("unverified_hover_value");
      if (quantity) {
        if (quantity.source_row_ids.some((id) => used.has(id))) throw new Error("reused_hover_row");
        quantity.source_row_ids.forEach((id) => used.add(id));
        zones[index] = {
          ...zones[index],
          source_row_ids: quantity.source_row_ids,
          source_value: quantity.source_value,
        };
      }
    }
  } catch {
    return fail(
      "נתוני הריחוף או שיוכם לבריכות אינם תקינים; דבר לא נשמר.",
      "invalid_hover_quantities",
    );
  }
  return {
    schema_version: collectorSchemaVersion(model),
    ...(data.level_contract_version === "prominent-rows.v1"
      ? { level_contract_version: "prominent-rows.v1" as const }
      : {}),
    ...(data.selection_policy === "prominent_right_edge.v1"
      ? { selection_policy: "prominent_right_edge.v1" as const }
      : {}),
    run_id: data.run_id as string,
    source_url: data.source_url as string,
    symbol: "BTC",
    heatmap_model: model,
    timeframe,
    captured_at: capturedAt,
    source_updated_at: sourceUpdatedAt,
    provider: data.provider as string,
    model: data.model as string,
    usage: data.usage ?? null,
    observed_price: reference.point,
    price_reference_type: reference.type,
    observed_price_range: reference.range,
    ...(priceAxis ? { price_axis_range: priceAxis } : {}),
    omitted_ambiguous_zones: reference.omitted,
    zones,
    ...(quantities ? { quantities } : {}),
    ...(hover ? { hover_snapshot: hover } : {}),
    ...(data.quantity_status === "unavailable"
      ? {
          quantity_status: "unavailable" as const,
          quantity_reason: "source_usd_unverified" as const,
        }
      : {}),

    evidence: {
      sha256: (evidence.sha256 as string).toLowerCase(),
      artifact_id: evidence.artifact_id as string,
      content_type: "image/png",
    },
    summary: typeof data.summary === "string" ? data.summary : "",
    quality: "visual_estimate",
  };
}

export function validateCollectorEnvelope(
  raw: unknown,
  expected: { timeframe: CollectorTimeframe; jobId?: string; model?: HeatmapModel },
): CollectorEnvelope {
  if (!raw || typeof raw !== "object" || Array.isArray(raw))
    fail("תשובת שירות האיסוף ריקה", "empty_envelope");
  const model = assertModel(expected.model ?? 1);
  const data = raw as Record<string, unknown>;
  if (data.schema_version !== collectorSchemaVersion(model))
    fail("גרסת חוזה לא נתמכת", "schema_mismatch");
  if (!isUuid(data.job_id)) fail("מזהה משימה לא תקין", "invalid_job_id");
  if (expected.jobId && (data.job_id as string).toLowerCase() !== expected.jobId.toLowerCase())
    fail("מזהה המשימה אינו תואם", "job_mismatch");
  if (!isCollectorTimeframe(data.timeframe)) fail("טווח זמן לא נתמך", "invalid_timeframe");
  if (data.timeframe !== expected.timeframe) fail("טווח הזמן אינו תואם", "wrong_timeframe");
  const status = data.status as CollectorStatus;
  if (status !== "queued" && status !== "running" && status !== "ready" && status !== "failed")
    fail("סטטוס לא תקין", "invalid_status");
  let error: CollectorEnvelope["error"] = null;
  if (data.error !== null && data.error !== undefined) {
    const raw2 = data.error as Record<string, unknown>;
    if (typeof raw2 !== "object" || Array.isArray(raw2)) fail("שגיאה לא תקינה", "invalid_error");
    error = {
      code: typeof raw2.code === "string" ? raw2.code.slice(0, 64) : "unknown",
      message: typeof raw2.message === "string" ? raw2.message.slice(0, 300) : "",
    };
  }
  const result =
    status === "ready"
      ? validateCollectorResult(data.result, expected.timeframe, {
          jobId: data.job_id as string,
          model,
        })
      : ((): null => {
          if (data.result !== null && data.result !== undefined)
            fail("תוצאה הוחזרה בלי סטטוס מוכן", "unexpected_result");
          return null;
        })();
  if (status === "failed" && !error) error = { code: "failed", message: "האיסוף נכשל במקור" };
  return {
    schema_version: collectorSchemaVersion(model),
    job_id: data.job_id as string,
    timeframe: expected.timeframe,
    status,
    result,
    error,
  };
}

/* ------------------------- המרה דטרמיניסטית לטבלה ------------------------- */

const usd = (value: number) => value.toLocaleString("en-US", { maximumFractionDigits: 8 });
export const formatCollectorRange = (low: number, high: number) => {
  if (low === high) return `$${usd(low)}`;
  // Distinct bounds must stay distinct even when narrower than display rounding.
  const lowText = usd(low);
  const highText = usd(high);
  return lowText === highText ? `$${low}–$${high}` : `$${lowText}–$${highText}`;
};

/** תווית הטווח — טקסט עברי בלבד, בלי מספרים. */
export const PRICE_RANGE_LABEL = "טווח מחיר משוער במפה:";

/** הערך המספרי של הטווח — נמוך ואחריו גבוה, תמיד באותו סדר עולה. */
export const priceRangeValueText = (range: { low: number; high: number }) =>
  `$${usd(range.low)}–$${usd(range.high)}`;

/** תצוגת הטווח הוויזואלי — תמיד שני קצוות, לעולם לא אמצע טווח. */
export const priceRangeText = (range: { low: number; high: number }) =>
  `${PRICE_RANGE_LABEL} ${priceRangeValueText(range)}`;

export const omittedZonesText = (count: number) =>
  count > 0 ? `${count} אזורים שחפפו לטווח לא סווגו` : "";

/**
 * נרמול טווח שנקרא ממקור לא מהימן (יומן ההרצות, תשובת שרת) דרך אותו מאמת —
 * בלי המרות טיפוס עיוורות. ערך לא תקין מוחזר כ-null במקום להיזרק.
 */
export function normalizePriceRange(raw: unknown): CollectorPriceRange | null {
  try {
    return validatePriceRange(raw);
  } catch {
    return null;
  }
}

export function normalizeOmittedCount(raw: unknown): number {
  return typeof raw === "number" && Number.isInteger(raw) && raw >= 0 ? raw : 0;
}

export type TableLevel = Omit<CollectorZone, "intensity"> & {
  price_text: string;
  amount: CollectorZone["intensity"];
  source_observation: {
    source_url: string;
    captured_at: string;
    artifact_id: string;
    sha256: string;
    heatmap_model: HeatmapModel;
    timeframe: CollectorTimeframe;
    quality: "visual_estimate";
    level_contract_version?: "prominent-rows.v1";
    selection_policy?: "prominent_right_edge.v1";
    price_axis_range?: CollectorResult["price_axis_range"];
  };
};

export function levelPrecisionText(
  level: Pick<CollectorZone, "price_precision" | "uncertainty_usd">,
): string {
  if (level.price_precision === "verified_label") return "מחיר מאומת מתווית המקור";
  return finitePositive(level.uncertainty_usd)
    ? `מחיר משוער; אי ודאות ±$${usd(level.uncertainty_usd)}`
    : "מחיר משוער מהציר";
}

/** A changed manual price cannot retain verification of a different captured price. */
export function manualLiquidityPricePatch(
  priceText: string,
  hasObservation: boolean,
): { price_text: string } & Record<string, unknown> {
  if (!hasObservation) return { price_text: priceText };
  return {
    price_text: priceText,
    shape: null,
    row_colour: null,
    continuous: null,
    weak_gap_present: null,
    price_precision: null,
    prices: null,
    evidence_basis: null,
    price_evidence: null,
    uncertainty_usd: null,
    strength_score: null,
    price_low: null,
    price_high: null,
    source_observation: null,
    amount_usd: null,
    quantity_bucket_ids: null,
    source_row_ids: null,
    source_value: null,
  };
}

const hasTwoVerifiedYellowPrices = (zone: Partial<CollectorZone>) =>
  zone.price_precision === "verified_label" &&
  zone.source_row_ids?.length === 2 &&
  (zone.shape === "adjacent_prices" ||
    (zone.shape === "range" &&
      zone.row_colour === "yellow" &&
      zone.continuous === true &&
      zone.weak_gap_present === false));

export function collectorZonePriceText(
  zone: Pick<
    CollectorZone,
    | "price_low"
    | "price_high"
    | "shape"
    | "prices"
    | "price_precision"
    | "source_row_ids"
    | "row_colour"
    | "continuous"
    | "weak_gap_present"
  >,
): string {
  if (zone.price_precision === "verified_label" && zone.source_row_ids?.length) {
    const exact = (p: number) =>
      `$${new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(p)}`;
    if (hasTwoVerifiedYellowPrices(zone))
      return `${exact(zone.price_low)} + ${exact(zone.price_high)}`;
    return zone.price_low === zone.price_high
      ? exact(zone.price_low)
      : `${exact(zone.price_low)}–${exact(zone.price_high)}`;
  }
  if (zone.shape === "point" && zone.prices?.length === 1) return `$${usd(zone.prices[0])}`;
  if (zone.shape === "adjacent_prices" && zone.prices?.length === 2)
    return zone.prices.map((price) => `$${usd(price)}`).join(" + ");
  return formatCollectorRange(zone.price_low, zone.price_high);
}

/** Render already-saved yellow pairs from their verified evidence without rewriting history. */
export function savedLiquidityPriceText(
  row: unknown,
  level: Partial<TableLevel> & Pick<TableLevel, "price_text" | "side">,
): string {
  if (
    !hasTwoVerifiedYellowPrices(level) ||
    typeof level.price_low !== "number" ||
    typeof level.price_high !== "number" ||
    !hoverLevelData(row, level, level.side)
  )
    return level.price_text;
  return collectorZonePriceText({
    ...level,
    price_low: level.price_low,
    price_high: level.price_high,
  });
}

/** העוצמה החזקה תחילה בכל צד, ואחריה המרחק מהמחיר. אין AI בשלב הזה. */
export function collectorZonesToLevels(result: CollectorResult): TableLevel[] {
  const rank = { many: 3, normal: 2, few: 1 };
  const byStrength = (a: CollectorZone, b: CollectorZone) =>
    rank[b.intensity] - rank[a.intensity] || (b.strength_score ?? 0) - (a.strength_score ?? 0);
  const above = result.zones
    .filter((zone) => zone.side === "above")
    .sort((a, b) => byStrength(a, b) || a.price_low - b.price_low);
  const below = result.zones
    .filter((zone) => zone.side === "below")
    .sort((a, b) => byStrength(a, b) || b.price_high - a.price_high);
  return [...above, ...below].map((zone) => ({
    ...zone,
    price_precision: zone.price_precision ?? "axis_estimate",
    evidence_basis: zone.evidence_basis ?? "axis_interpolation",
    shape: zone.shape ?? (zone.price_low === zone.price_high ? "point" : "range"),
    price_text: collectorZonePriceText(zone),
    amount: zone.intensity,
    source_observation: {
      source_url: result.source_url,
      captured_at: result.captured_at,
      artifact_id: result.evidence.artifact_id,
      sha256: result.evidence.sha256,
      heatmap_model: result.heatmap_model,
      timeframe: result.timeframe,
      quality: result.quality,
      ...(result.level_contract_version
        ? { level_contract_version: result.level_contract_version }
        : {}),
      ...(result.selection_policy ? { selection_policy: result.selection_policy } : {}),
      ...(result.price_axis_range ? { price_axis_range: result.price_axis_range } : {}),
    },
  }));
}

export type Freshness = { ageMs: number; stale: boolean; invalid: boolean };

/** זמן התפיסה המקורי נשמר כפי שהוא; עתידי או ישן מאוד נפסל לשמירה אוטומטית. */
export function resultFreshness(capturedAt: string, now: number): Freshness {
  const parsed = Date.parse(capturedAt);
  if (!Number.isFinite(parsed)) return { ageMs: Number.NaN, stale: true, invalid: true };
  const ageMs = now - parsed;
  if (ageMs < -MAX_FUTURE_SKEW_MS) return { ageMs, stale: true, invalid: true };
  return { ageMs, stale: ageMs > MAX_AUTO_SAVE_AGE_MS, invalid: false };
}

/* ------------------- תצפית שמורה: גיל, תצוגה ומכסת איסוף ------------------- */

/** תצפית ישנה מ-6 שעות אינה מוצעת כלל לצפייה או להעתקה. */
export const MAX_REVIEW_AGE_MS = 6 * 60 * 60 * 1000;

export type ReviewFreshness = { ageMs: number; stale: boolean; invalid: boolean; tooOld: boolean };

/** גיל התצפית השמורה: עתידית => פסולה, מעל 6 שעות => לא מוצעת, מעל 15 דק' => מיושנת. */
export function reviewFreshness(capturedAt: string, now: number): ReviewFreshness {
  const parsed = Date.parse(capturedAt);
  if (!Number.isFinite(parsed))
    return { ageMs: Number.NaN, stale: true, invalid: true, tooOld: true };
  const ageMs = now - parsed;
  if (ageMs < -MAX_FUTURE_SKEW_MS) return { ageMs, stale: true, invalid: true, tooOld: true };
  return {
    ageMs,
    stale: ageMs > MAX_AUTO_SAVE_AGE_MS,
    invalid: false,
    tooOld: ageMs > MAX_REVIEW_AGE_MS,
  };
}

/** שעון ישראל לתצוגה בלבד; הערך הנשמר תמיד נשאר ה-ISO המקורי. */
export function israelTime(iso: string | null | undefined): string {
  if (typeof iso !== "string" || !Number.isFinite(Date.parse(iso))) return "";
  return new Intl.DateTimeFormat("he-IL", {
    timeZone: "Asia/Jerusalem",
    dateStyle: "short",
    timeStyle: "short",
  }).format(new Date(iso));
}

export function ageText(ageMs: number): string {
  if (!Number.isFinite(ageMs) || ageMs < 0) return "";
  const minutes = Math.floor(ageMs / 60_000);
  if (minutes < 1) return "פחות מדקה";
  if (minutes < 60) return `${minutes} דקות`;
  const hours = Math.floor(minutes / 60);
  return `${hours} שעות ו-${minutes % 60} דקות`;
}

export const SAVED_OBSERVATION_LABEL = "מידע שמור — לא איסוף חדש";
export const SAVED_OBSERVATION_STALE_NOTE =
  "התצפית השמורה ישנה מ-15 דקות — היא אינה משקפת בהכרח את המצב כרגע.";
export const savedObservationOriginText = (capturedAt: string) =>
  `איסוף שמור מ־${israelTime(capturedAt)}`;

/* -------------------------- מכסת איסופים חדשים -------------------------- */

export type RateLimitInfo = { retryAt: string | null; retryAfterSeconds: number | null };

const MAX_RETRY_AFTER_SECONDS = 24 * 60 * 60;

/**
 * פענוח בטוח של זמן ההמתנה שהשירות מסר (גוף התשובה או כותרת Retry-After).
 * ערך חסר או פסול מוחזר כ-null — לעולם לא ממציאים זמן.
 */
export function parseRateLimit(raw: {
  retryAt?: unknown;
  retryAfterSeconds?: unknown;
  header?: unknown;
  now?: number;
}): RateLimitInfo {
  const now = typeof raw.now === "number" && Number.isFinite(raw.now) ? raw.now : Date.now();
  let seconds: number | null = null;
  const fromBody = raw.retryAfterSeconds;
  if (typeof fromBody === "number" && Number.isInteger(fromBody) && fromBody >= 0)
    seconds = Math.min(fromBody, MAX_RETRY_AFTER_SECONDS);
  if (seconds === null && typeof raw.header === "string" && /^\d{1,7}$/.test(raw.header.trim()))
    seconds = Math.min(Number(raw.header.trim()), MAX_RETRY_AFTER_SECONDS);

  let retryAt: string | null = null;
  if (typeof raw.retryAt === "string" && ISO_WITH_ZONE.test(raw.retryAt.trim())) {
    const parsed = Date.parse(raw.retryAt.trim());
    if (Number.isFinite(parsed) && parsed - now <= MAX_RETRY_AFTER_SECONDS * 1000)
      retryAt = new Date(parsed).toISOString();
  }
  if (!retryAt && seconds !== null) retryAt = new Date(now + seconds * 1000).toISOString();
  if (retryAt && seconds === null)
    seconds = Math.max(0, Math.round((Date.parse(retryAt) - now) / 1000));
  return { retryAt, retryAfterSeconds: seconds };
}

/** ההודעה המדויקת שמוצגת כשמכסת האיסופים החדשים לשעה נוצלה. */
export function rateLimitNote(retryAt: string | null | undefined): string {
  const when = israelTime(retryAt);
  return when
    ? `מכסת האיסופים החדשים לשעה נוצלה. ניתן לנסות שוב ב־${when}. אפשר לצפות באיסוף שכבר נשמר ללא צילום נוסף.`
    : "מכסת האיסופים החדשים לשעה נוצלה. זמן הניסיון הבא אינו ידוע — אפשר לנסות שוב ידנית מאוחר יותר, או לצפות באיסוף שכבר נשמר ללא צילום נוסף.";
}

/* ---------------------- הודעות שגיאה בטוחות לפי קוד ---------------------- */

/**
 * קודי שגיאה מוסכמים מהאספן => הודעה עברית מובנת. הודעת המקור עצמה לעולם
 * אינה מוצגת, כדי שלא ידלוף טקסט פנימי או פרטי ספק.
 */
const COLLECTOR_ERROR_NOTES: Record<string, string> = {
  rate_limited: "מכסת האיסופים החדשים לשעה נוצלה.",
  no_cached_result: "אין איסוף שמור זמין לטווח הזה.",
  source_capture_failed: "קריאת נתוני המקור נכשלה. אפשר לנסות איסוף חדש מאוחר יותר.",
  source_timeout: "המקור לא סיים להיטען בזמן. אפשר לנסות איסוף חדש מאוחר יותר.",
  scan_timeout: "סריקת המפה לא הסתיימה בזמן; הנתונים הקודמים נשמרו. אפשר לנסות שוב מאוחר יותר.",
  store_unavailable:
    "שירות האיסוף לא הצליח לשמור את התוצאה; הנתונים הקודמים נשמרו. אפשר לנסות שוב מאוחר יותר.",
  source_login_required:
    "קוינגלאס דורש התחברות כדי לפתוח את נתוני המפה בטווח שנבחר; התיבה לא שונתה.",
  source_not_readable: "הגרף במקור לא היה קריא (טעינה או טשטוש) ולכן לא נשמר דבר.",
  screenshot_identity_mismatch: "הצילום לא התאים ל-BTC, למודל או לטווח שנבחר — לא נשמר דבר.",
  image_evidence_invalid: "ראיית הצילום אינה תקינה — לא נשמר דבר.",
  analysis_timeout: "ניתוח התמונה לא הסתיים בזמן.",
  analysis_incomplete: "הניתוח הוחזר חלקי ולכן נפסל.",
  analysis_response_invalid: "תשובת הניתוח אינה בפורמט תקין ולכן נפסלה.",
  analysis_unauthorized: "שירות הניתוח דחה את ההרשאה. יש לבדוק את הגדרת המפתח באספן.",
  analysis_rate_limited: "שירות הניתוח עמוס כרגע. נסו שוב מאוחר יותר.",
  analysis_quota_exceeded: "מכסת הניתוח נוצלה. יש להשלים מכסה אצל ספק הניתוח.",
  analysis_http_error: "שירות הניתוח החזיר שגיאה. נסו שוב מאוחר יותר.",
  price_uncertain: "המחיר הנוכחי לא זוהה בוודאות בצילום ולכן לא נשמר דבר.",
  price_range_uncertain: "טווח המחיר במפה לא זוהה בוודאות מספקת ולכן לא נשמר דבר.",
  price_range_too_wide: "טווח המחיר שזוהה רחב מדי ולכן נפסל — לא נשמר דבר.",
  no_unambiguous_zones: "לא נמצא אף אזור נזילות חד-משמעי מעל או מתחת לטווח — לא נשמר דבר.",
  zones_invalid: "אזורי הנזילות שזוהו לא עברו אימות ולכן לא נשמר דבר.",

  worker_crashed: "תהליך האיסוף נעצר באמצע. אפשר לנסות איסוף חדש.",
  source_or_analysis_failed: "האיסוף או הניתוח נכשלו במקור — לא נשמר דבר.",
};

const SAFE_ERROR_CODE = /^[a-z0-9_]{1,64}$/;

/**
 * הודעה ידידותית לקוד שגיאה. חיפוש במאפיין עצמי בלבד (לא דרך הפרוטוטייפ),
 * וקוד לא מוכר מוצג רק אם הוא מזהה שמרני; אחרת הודעה כללית.
 */
export function collectorErrorNote(code: string | null | undefined): string {
  const key = typeof code === "string" ? code.trim() : "";
  if (Object.prototype.hasOwnProperty.call(COLLECTOR_ERROR_NOTES, key)) {
    const known = COLLECTOR_ERROR_NOTES[key];
    if (typeof known === "string") return known;
  }
  return SAFE_ERROR_CODE.test(key)
    ? `האיסוף נכשל במקור (${key})`
    : "האיסוף נכשל במקור — לא נשמר דבר.";
}
