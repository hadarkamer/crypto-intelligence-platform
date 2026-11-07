/**
 * כרטיס חתום קצר-מועד שמחבר בין הפעלת האיסוף לבין הסקרים שאחריה.
 * החתימה נעשית בשרת בלבד (HMAC-SHA256 עם אישור הגשר הייעודי); הכרטיס עצמו
 * אינו מכיל את הסוד ואינו נסמך על מה שהלקוח מצהיר עליו.
 */
import {
  isCollectorTimeframe,
  isHeatmapModel,
  isUuid,
  type CollectorTimeframe,
  type HeatmapModel,
} from "@/lib/coinglassCollector";

export const TICKET_TTL_MS = 20 * 60 * 1000;

/** כרטיס העתקה של תצפית שמורה — חלון קצר, פעולה מודעת וחד-פעמית. */
export const REVIEW_TICKET_TTL_MS = 10 * 60 * 1000;

/** ייעוד הכרטיס. כרטיס ישן בלי ייעוד נחשב "collect" כמו קודם. */
export type TicketPurpose = "collect" | "review_saved_observation";

const SHA256 = /^[0-9a-f]{64}$/i;

export type TicketPayload = {
  v: 1;
  userId: string;
  workspaceId: string;
  sessionId: string;
  fieldId: string;
  timeframe: CollectorTimeframe;
  /** מודל מפת החום. כרטיס ישן בלי השדה = Model 1 בלבד, ואינו ניתן לשימוש ב-2/3. */
  model?: HeatmapModel;
  jobId: string;
  requestId: string;
  /** גרסת הבסיס המקורית שלפיה נפתחה ההרצה — אף פעם לא נלקחת מהלקוח בסקר. */
  baselineUpdatedAt: string | null;
  exp: number;
  purpose?: TicketPurpose;
  /** זמן התפיסה המקורי של התצפית השמורה (רק בכרטיס העתקה). */
  capturedAt?: string | null;
  /** חתימת הראיה של אותה תצפית (רק בכרטיס העתקה). */
  evidenceSha256?: string | null;
};

export class TicketError extends Error {
  constructor(
    message: string,
    readonly code: string,
  ) {
    super(message);
    this.name = "TicketError";
  }
}

const encoder = new TextEncoder();

const toBase64Url = (bytes: Uint8Array) => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
};

const fromBase64Url = (value: string) => {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(padded + "=".repeat((4 - (padded.length % 4)) % 4));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
};

const importKey = (secret: string) =>
  crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, [
    "sign",
    "verify",
  ]);

export async function signTicket(payload: TicketPayload, secret: string): Promise<string> {
  if (!secret) throw new TicketError("אישור הגשר חסר", "no_secret");
  const body = toBase64Url(encoder.encode(JSON.stringify(payload)));
  const key = await importKey(secret);
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(body)));
  return `${body}.${toBase64Url(signature)}`;
}

/** אימות בזמן קבוע דרך crypto.subtle.verify, ואז בדיקת תוקף וזהות המשתמש. */
export async function verifyTicket(
  token: unknown,
  secret: string,
  options: { userId: string; now: number; purpose?: TicketPurpose; model?: HeatmapModel },
): Promise<TicketPayload> {
  if (!secret) throw new TicketError("אישור הגשר חסר", "no_secret");
  if (typeof token !== "string" || token.length > 4096 || !token.includes("."))
    throw new TicketError("כרטיס לא תקין", "malformed");
  const [body, signature, ...rest] = token.split(".");
  if (!body || !signature || rest.length) throw new TicketError("כרטיס לא תקין", "malformed");
  let valid: boolean;
  try {
    const key = await importKey(secret);
    valid = await crypto.subtle.verify(
      "HMAC",
      key,
      fromBase64Url(signature).slice().buffer as ArrayBuffer,
      encoder.encode(body),
    );
  } catch {
    throw new TicketError("כרטיס לא תקין", "malformed");
  }
  if (!valid) throw new TicketError("חתימת הכרטיס אינה תקפה", "bad_signature");
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(fromBase64Url(body)));
  } catch {
    throw new TicketError("כרטיס לא תקין", "malformed");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
    throw new TicketError("כרטיס לא תקין", "malformed");
  const data = parsed as Record<string, unknown>;
  if (data.v !== 1) throw new TicketError("גרסת כרטיס לא נתמכת", "bad_version");
  for (const field of ["userId", "workspaceId", "sessionId", "fieldId", "jobId", "requestId"])
    if (!isUuid(data[field])) throw new TicketError("כרטיס לא תקין", "malformed");
  if (!isCollectorTimeframe(data.timeframe)) throw new TicketError("כרטיס לא תקין", "malformed");
  // מודל חסר => 1 (תאימות לאחור). כרטיס של מודל אחד לעולם אינו תקף לאחר.
  if (data.model !== undefined && data.model !== null && !isHeatmapModel(data.model))
    throw new TicketError("כרטיס לא תקין", "malformed");
  const model: HeatmapModel = isHeatmapModel(data.model) ? data.model : 1;
  if (options.model !== undefined && model !== options.model)
    throw new TicketError("הכרטיס אינו שייך למודל הזה", "wrong_model");
  if (typeof data.exp !== "number" || !Number.isFinite(data.exp))
    throw new TicketError("כרטיס לא תקין", "malformed");
  if (data.baselineUpdatedAt !== null && typeof data.baselineUpdatedAt !== "string")
    throw new TicketError("כרטיס לא תקין", "malformed");
  if (data.exp <= options.now) throw new TicketError("פג תוקף הכרטיס", "expired");
  if ((data.userId as string) !== options.userId)
    throw new TicketError("הכרטיס שייך למשתמש אחר", "wrong_user");
  // ייעוד: כרטיס ישן בלי שדה purpose נשאר "collect" בדיוק כמו קודם.
  const purpose: TicketPurpose =
    data.purpose === undefined || data.purpose === null
      ? "collect"
      : data.purpose === "collect" || data.purpose === "review_saved_observation"
        ? data.purpose
        : (() => {
            throw new TicketError("ייעוד הכרטיס אינו נתמך", "bad_purpose");
          })();
  if (options.purpose && purpose !== options.purpose)
    throw new TicketError("הכרטיס אינו מיועד לפעולה הזו", "wrong_purpose");
  let capturedAt: string | null = null;
  let evidenceSha256: string | null = null;
  if (purpose === "review_saved_observation") {
    // כרטיס העתקה קשור לתצפית מסוימת: זמן תפיסה מקורי וחתימת ראיה חובה.
    if (typeof data.capturedAt !== "string" || !Number.isFinite(Date.parse(data.capturedAt)))
      throw new TicketError("כרטיס לא תקין", "malformed");
    if (typeof data.evidenceSha256 !== "string" || !SHA256.test(data.evidenceSha256))
      throw new TicketError("כרטיס לא תקין", "malformed");
    capturedAt = data.capturedAt;
    evidenceSha256 = data.evidenceSha256.toLowerCase();
  }
  return {
    v: 1,
    userId: data.userId as string,
    workspaceId: data.workspaceId as string,
    sessionId: data.sessionId as string,
    fieldId: data.fieldId as string,
    timeframe: data.timeframe,
    model,
    jobId: data.jobId as string,
    requestId: data.requestId as string,
    baselineUpdatedAt: (data.baselineUpdatedAt as string | null) ?? null,
    exp: data.exp,
    purpose,
    capturedAt,
    evidenceSha256,
  };
}
