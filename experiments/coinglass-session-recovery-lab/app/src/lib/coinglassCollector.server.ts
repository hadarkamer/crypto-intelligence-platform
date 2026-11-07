/**
 * לקוח שרת-לשרת לשירות האיסוף החיצוני (Model 1). אין CORS ואין קריאה מהדפדפן.
 * POST פותח משימה (אידמפוטנטי לפי request_id), GET רק קורא — לעולם לא סורק.
 */
import {
  assertCollectorToken,
  healthUrl,
  jobUrl,
  jobsUrl,
  latestJobUrl,
  parseCollectorBase,
  parseCollectorCapabilities,
  parseRateLimit,
  validateCollectorEnvelope,
  type CollectorCapabilities,
  type CollectorEnvelope,
  type CollectorTimeframe,
  type HeatmapModel,
  type RateLimitInfo,
} from "@/lib/coinglassCollector";

const HTTP_TIMEOUT_MS = 15_000;
const MAX_BODY_BYTES = 512 * 1024;

/** קריאה חסומה בבייטים: הזרם נקטע ומבוטל ברגע שחורג, בלי לצרוך גוף לא מוגבל. */
async function readBoundedText(response: Response): Promise<string> {
  const body = response.body;
  if (!body) return "";
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_BODY_BYTES) {
        await reader.cancel().catch(() => undefined);
        throw new CollectorRequestError("תשובת שירות האיסוף גדולה מדי");
      }
      text += decoder.decode(value, { stream: true });
    }
  } finally {
    reader.releaseLock?.();
  }
  return text + decoder.decode();
}

export class CollectorNotConfiguredError extends Error {
  readonly code = "NOT_CONNECTED";
  constructor(readonly prerequisite: string) {
    super(prerequisite);
    this.name = "CollectorNotConfiguredError";
  }
}

export class CollectorRequestError extends Error {
  readonly code = "COLLECTOR_FAILED";
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = "CollectorRequestError";
  }
}

/** 429 מהאספן: מכסת איסופים חדשים, לא כישלון איסוף. אין ניסיון חוזר אוטומטי. */
export class CollectorRateLimitedError extends Error {
  readonly code = "RATE_LIMITED";
  constructor(readonly info: RateLimitInfo) {
    super("מכסת האיסופים החדשים לשעה נוצלה");
    this.name = "CollectorRateLimitedError";
  }
}

/** אין תצפית שמורה זמינה לטווח המבוקש (404 no_cached_result). */
export class CollectorNoCachedResultError extends Error {
  readonly code = "NO_CACHED_RESULT";
  constructor() {
    super("אין איסוף שמור זמין לטווח הזה");
    this.name = "CollectorNoCachedResultError";
  }
}

export type CollectorConfig = { base: string; token: string };

/** תצורה חסרה או פסולה => NOT_CONNECTED, בלי שום קריאה לספק. */
export function readCollectorConfig(): CollectorConfig {
  const rawBase = process.env["COINGLASS_COLLECTOR_URL"];
  const rawToken = process.env["COINGLASS_COLLECTOR_TOKEN"];
  if (!rawBase || !rawToken)
    throw new CollectorNotConfiguredError(
      "שירות האיסוף אינו מוגדר (COINGLASS_COLLECTOR_URL / COINGLASS_COLLECTOR_TOKEN)",
    );
  try {
    // הערכים עצמם לעולם אינם מודפסים — רק תקינותם נבדקת.
    return { base: parseCollectorBase(rawBase), token: assertCollectorToken(rawToken) };
  } catch (error) {
    throw new CollectorNotConfiguredError(
      error instanceof Error ? error.message : "תצורת שירות האיסוף אינה תקינה",
    );
  }
}

async function call(
  config: CollectorConfig,
  url: string,
  init: { method: "GET" | "POST"; body?: string; timeoutMs?: number },
): Promise<unknown> {
  let response: Response;
  try {
    response = await fetch(url, {
      method: init.method,
      redirect: "manual",
      signal: AbortSignal.timeout(init.timeoutMs ?? HTTP_TIMEOUT_MS),
      headers: {
        Authorization: `Bearer ${config.token}`,
        Accept: "application/json",
        ...(init.body ? { "Content-Type": "application/json" } : {}),
      },
      ...(init.body ? { body: init.body } : {}),
    });
  } catch (error) {
    // הודעה מנוקה: לא חושפים כתובת, סוד או פרטי רשת — אבל כן מבחינים בין
    // חריגת זמן (TimeoutError/AbortError) לבין כשל חיבור אמיתי.
    const name = error instanceof Error ? error.name : "";
    throw new CollectorRequestError(
      name === "TimeoutError" || name === "AbortError"
        ? "שירות האיסוף לא נענה בזמן"
        : "לא ניתן להתחבר לשירות האיסוף",
    );
  }

  if (response.status >= 300 && response.status < 400)
    throw new CollectorRequestError(
      `שירות האיסוף החזיר שגיאה (${response.status})`,
      response.status,
    );

  const text = await readBoundedText(response);
  let parsed: unknown = null;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = null;
  }
  if (!response.ok) {
    const errorBody =
      parsed && typeof parsed === "object"
        ? ((parsed as { error?: Record<string, unknown> }).error ?? null)
        : null;
    const code = errorBody?.code ?? null;
    // 429 אינו כישלון איסוף אלא מכסה: זמן ההמתנה נקרא מהגוף או מהכותרת בלבד.
    if (response.status === 429)
      throw new CollectorRateLimitedError(
        parseRateLimit({
          retryAt: errorBody?.retry_at,
          retryAfterSeconds: errorBody?.retry_after_seconds,
          header: response.headers.get("Retry-After"),
        }),
      );
    if (response.status === 404 && code === "no_cached_result")
      throw new CollectorNoCachedResultError();
    throw new CollectorRequestError(
      `שירות האיסוף החזיר שגיאה (${response.status}${typeof code === "string" ? `/${code.slice(0, 40)}` : ""})`,
      response.status,
    );
  }
  return parsed;
}

/** פתיחת משימה. חזרה עם אותו request_id לא פותחת סריקה נוספת (אידמפוטנטיות בשרת). */
export async function startCollectorJob(
  requestId: string,
  timeframe: CollectorTimeframe,
  config = readCollectorConfig(),
  model: HeatmapModel = 1,
): Promise<CollectorEnvelope> {
  const raw = await call(config, jobsUrl(config.base, model), {
    method: "POST",
    body: JSON.stringify({ request_id: requestId, timeframe }),
  });
  return validateCollectorEnvelope(raw, { timeframe, model });
}

/** קריאה בלבד — אף פעם לא מפעילה סריקה חדשה. */
export async function readCollectorJob(
  jobId: string,
  timeframe: CollectorTimeframe,
  config = readCollectorConfig(),
  model: HeatmapModel = 1,
): Promise<CollectorEnvelope> {
  const raw = await call(config, jobUrl(config.base, jobId, model), { method: "GET" });
  return validateCollectorEnvelope(raw, { timeframe, jobId, model });
}

/**
 * התצפית האחרונה שכבר נאספה לטווח הזה — GET בלבד, ללא POST וללא נגיעה במקור.
 * מזהה המשימה מגיע מהשירות עצמו; האפליקציה אינה מכילה מזהי בדיקה קשיחים.
 */
export async function readLatestCollectorJob(
  timeframe: CollectorTimeframe,
  config = readCollectorConfig(),
  model: HeatmapModel = 1,
): Promise<CollectorEnvelope> {
  const raw = await call(config, latestJobUrl(config.base, timeframe, model), { method: "GET" });
  return validateCollectorEnvelope(raw, { timeframe, model });
}

/**
 * יכולות השירות מתוך /health — GET בלבד. מודל 2/3 אינו מוצע כלל מול גרסת
 * שרת שאינה מפרסמת אותו. התשובה נשמרת לזמן קצר בזיכרון התהליך.
 */
const CAPABILITIES_TTL_MS = 5 * 60 * 1000;
let capabilitiesCache: { at: number; value: CollectorCapabilities } | null = null;

export async function readCollectorCapabilities(
  config = readCollectorConfig(),
  now: number = Date.now(),
): Promise<CollectorCapabilities> {
  if (capabilitiesCache && now - capabilitiesCache.at < CAPABILITIES_TTL_MS)
    return capabilitiesCache.value;
  // Free Render instances can wake slowly. Warm/read health once with a bounded
  // deadline before a provider job; POST and polling retain their short deadlines.
  const raw = await call(config, healthUrl(config.base), { method: "GET", timeoutMs: 90_000 });
  const value = parseCollectorCapabilities(raw);
  capabilitiesCache = { at: now, value };
  return value;
}
