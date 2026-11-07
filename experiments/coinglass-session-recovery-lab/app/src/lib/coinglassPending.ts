/**
 * מכונת המצבים הטהורה של האיסוף הממתין (Model 1), מחוץ לרכיב כדי שתהיה ניתנת
 * לבדיקה אופליין. העיקרון: מזהה הבקשה, גרסת הבסיס והטווח נשמרים *לפני* הקריאה
 * לשירות. אם התשובה אבדה (רשת/רענון), ההמשך משתמש באותו מזהה ואותה גרסת בסיס,
 * כך שלעולם לא נפתחת סריקה חדשה בתשלום מאליה.
 */
import {
  isCollectorTimeframe,
  isHeatmapModel,
  type CollectorTimeframe,
  type HeatmapModel,
} from "@/lib/coinglassCollector";

// A worker can spend six minutes on Model 3. Allow seven minutes of polling;
// reaching this bound pauses observation, never fails or restarts the job.
export const POLL_INTERVAL_MS = 5_000;
export const MAX_POLLS = 84;

export type PendingCollection = {
  requestId: string;
  timeframe: CollectorTimeframe;
  baselineUpdatedAt: string | null;
  userId: string;
  sessionId: string;
  fieldId: string;
  /** מודל מפת החום. רשומה ישנה בלי השדה = Model 1 בלבד. */
  model?: HeatmapModel;
  /** מתקבל רק אחרי תשובת התחלה מוצלחת; בלעדיו עדיין ממתינים לתשובה. */
  ticket?: string | null;
  /** מכסת האיסופים החדשים נוצלה; אינו משפיע על סקר GET או על צפייה בשמור. */
  rateLimited?: boolean;
  /** מועד ISO מאומת שעד אליו אין לשלוח בקשה חדשה. null = לא ידוע. */
  retryAt?: string | null;
};

/** רק מועד ISO שניתן לפענוח נשמר — אף פעם לא מחרוזת שרירותית. */
export const validRetryAt = (raw: unknown): string | null => {
  if (typeof raw !== "string" || !raw) return null;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
};

export type PendingStorage = {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
  removeItem: (key: string) => void;
};

/** תחילית המפתח לפי מודל. Model 1 שומר על המפתחות ההיסטוריים בדיוק. */
const modelPrefix = (model: HeatmapModel = 1) => `coinglass:model${model}`;

/** מפתח נפרד לכל טווח: 12H/24H/48H אינם דורסים זה את מצב הבקשה של זה. */
export const pendingKey = (
  userId: string,
  sessionId: string,
  fieldId: string,
  timeframe: CollectorTimeframe,
  model: HeatmapModel = 1,
) => `${modelPrefix(model)}:${userId}:${sessionId}:${fieldId}:${timeframe}`;

const isTimeframe = isCollectorTimeframe;

/** זהות הבקשה כוללת טווח ומודל — לכל שילוב מצב ממתין נפרד משלו. */
export type PendingIdentity = {
  userId: string;
  sessionId: string;
  fieldId: string;
  timeframe: CollectorTimeframe;
  model?: HeatmapModel;
};

const modelOf = (identity: { model?: HeatmapModel }): HeatmapModel =>
  isHeatmapModel(identity.model) ? identity.model : 1;

/** המפתח מהגרסה הקודמת — בלי טווח. נקרא לצורך תאימות לאחור בלבד (Model 1). */
export const legacyPendingKey = (
  userId: string,
  sessionId: string,
  fieldId: string,
  model: HeatmapModel = 1,
) => `${modelPrefix(model)}:${userId}:${sessionId}:${fieldId}`;

/** מפתח הטווח האחרון שנבחר, לפי משתמש+גיליון+שדה. */
export const choiceKey = (
  userId: string,
  sessionId: string,
  fieldId: string,
  model: HeatmapModel = 1,
) => `${modelPrefix(model)}:choice:${userId}:${sessionId}:${fieldId}`;

/** פענוח רשומה גולמית לפי זהות מלאה; כל אי-התאמה מחזירה null. */
function parsePending(raw: string | null, identity: PendingIdentity): PendingCollection | null {
  let parsed: unknown;
  try {
    if (!raw) return null;
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;
  const value = parsed as Partial<PendingCollection>;
  if (typeof value.requestId !== "string" || !value.requestId) return null;
  if (!isTimeframe(value.timeframe) || value.timeframe !== identity.timeframe) return null;
  if (
    value.userId !== identity.userId ||
    value.sessionId !== identity.sessionId ||
    value.fieldId !== identity.fieldId
  )
    return null;
  // רשומה ישנה בלי מודל היא Model 1 בלבד; לעולם לא משמשת ל-2/3.
  const model: HeatmapModel = isHeatmapModel(value.model) ? value.model : 1;
  if (model !== modelOf(identity)) return null;
  return {
    requestId: value.requestId,
    timeframe: value.timeframe,
    baselineUpdatedAt: typeof value.baselineUpdatedAt === "string" ? value.baselineUpdatedAt : null,
    userId: identity.userId,
    sessionId: identity.sessionId,
    fieldId: identity.fieldId,
    model,
    ticket: typeof value.ticket === "string" && value.ticket ? value.ticket : null,
    // רשומה ישנה בלי השדות האלה נשארת תקפה לחלוטין.
    rateLimited: value.rateLimited === true,
    retryAt: validRetryAt(value.retryAt),
  };
}

/**
 * קריאה מגוננת: רשומה של משתמש/גיליון/שדה/טווח אחרים או פגומה נדחית.
 * אם אין רשומה במפתח החדש, נקראת הרשומה מהמפתח הישן — אך ורק אם הטווח שבה
 * תואם לטווח המבוקש. מזהה הבקשה, הכרטיס וגרסת הבסיס נשמרים כפי שהם; הרשומה
 * הישנה נמחקת רק אחרי העתקה מוצלחת למפתח החדש.
 */
export function readPending(
  storage: PendingStorage,
  identity: PendingIdentity,
): PendingCollection | null {
  const model = modelOf(identity);
  let current: string | null = null;
  try {
    current = storage.getItem(
      pendingKey(identity.userId, identity.sessionId, identity.fieldId, identity.timeframe, model),
    );
  } catch {
    current = null;
  }
  const parsed = parsePending(current, identity);
  if (parsed) return parsed;
  if (current) return null;

  let legacyRaw: string | null = null;
  try {
    legacyRaw = storage.getItem(
      legacyPendingKey(identity.userId, identity.sessionId, identity.fieldId, model),
    );
  } catch {
    return null;
  }
  const legacy = parsePending(legacyRaw, identity);
  if (!legacy) return null;
  // העברה שמרנית: מחיקה רק אם ההעתקה הצליחה. אחרת נשארת רשומה קריאה.
  if (tryWritePending(storage, legacy)) {
    try {
      storage.removeItem(
        legacyPendingKey(identity.userId, identity.sessionId, identity.fieldId, model),
      );
    } catch {
      // מחיקה שנכשלה אינה משנה דבר — הקריאה החדשה מקבלת קדימות.
    }
  }
  return legacy;
}

export function writePending(storage: PendingStorage, pending: PendingCollection): void {
  storage.setItem(
    pendingKey(
      pending.userId,
      pending.sessionId,
      pending.fieldId,
      pending.timeframe,
      modelOf(pending),
    ),
    JSON.stringify(pending),
  );
}

/**
 * כתיבה מגוננת: אחסון מלא/מושבת עלול לזרוק. מחזיר false במקום לזרוק, כדי
 * שהמתקשר יוכל לעצור לפני כל קריאת רשת ולשחזר את הפקדים.
 */
export function tryWritePending(storage: PendingStorage, pending: PendingCollection): boolean {
  try {
    writePending(storage, pending);
    return true;
  } catch {
    return false;
  }
}

/** מנקה רק את הטווח, המודל והזהות התואמים — כולל רשומה ישנה של אותו טווח. */
export function clearPending(storage: PendingStorage, identity: PendingIdentity): void {
  const model = modelOf(identity);
  try {
    storage.removeItem(
      pendingKey(identity.userId, identity.sessionId, identity.fieldId, identity.timeframe, model),
    );
  } catch {
    // אין מה לעשות; המצב בזיכרון כבר אופס.
  }
  try {
    const legacyRaw = storage.getItem(
      legacyPendingKey(identity.userId, identity.sessionId, identity.fieldId, model),
    );
    if (parsePending(legacyRaw, identity))
      storage.removeItem(
        legacyPendingKey(identity.userId, identity.sessionId, identity.fieldId, model),
      );
  } catch {
    // רשומה ישנה של טווח אחר נשארת נגישה.
  }
}

/** הטווח האחרון שנבחר — רק ערך מאומת 12H/24H/48H נשמר או נקרא. */
export function readChoice(
  storage: PendingStorage,
  identity: { userId: string; sessionId: string; fieldId: string; model?: HeatmapModel },
): CollectorTimeframe | null {
  try {
    const raw = storage.getItem(
      choiceKey(identity.userId, identity.sessionId, identity.fieldId, modelOf(identity)),
    );
    return isTimeframe(raw) ? raw : null;
  } catch {
    return null;
  }
}

export function writeChoice(
  storage: PendingStorage,
  identity: { userId: string; sessionId: string; fieldId: string; model?: HeatmapModel },
  timeframe: CollectorTimeframe,
): void {
  if (!isTimeframe(timeframe)) return;
  try {
    storage.setItem(
      choiceKey(identity.userId, identity.sessionId, identity.fieldId, modelOf(identity)),
      timeframe,
    );
  } catch {
    // בחירה שלא נשמרה אינה משפיעה על המשימה עצמה.
  }
}

/**
 * מה לעשות כשחוזרים אל איסוף ממתין:
 * - יש כרטיס => סקר (GET בלבד) על אותה משימה.
 * - אין כרטיס => התשובה להתחלה אבדה; חוזרים עם *אותו* request_id, וזה אידמפוטנטי
 *   בשירות ולכן לא מתחיל סריקה נוספת.
 */
export function resumeAction(pending: PendingCollection | null): "none" | "poll" | "resend-start" {
  if (!pending) return "none";
  return pending.ticket ? "poll" : "resend-start";
}

/**
 * האם אסור לשלוח בקשת איסוף חדשה כרגע (כולל חידוש אותה בקשה).
 * מועד לא ידוע => לא חוסמים; ניסיון ידני עדיין מותר.
 */
export function isQuotaBlocked(
  pending: { rateLimited?: boolean; retryAt?: string | null } | null | undefined,
  now: number,
): boolean {
  if (!pending?.rateLimited) return false;
  const at = validRetryAt(pending.retryAt);
  return at !== null && Date.parse(at) > now;
}
