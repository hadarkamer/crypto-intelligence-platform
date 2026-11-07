/**
 * הליבה של מסלול האיסוף היחיד ל-"Coinglass - Model 1" — בלי תלות ב-TanStack
 * ובלי מודול שרת, כדי שכל ההתנהגות תיבדק אופליין עם תלויות מוזרקות.
 *
 * התחלה = POST אחד (אידמפוטנטי לפי request_id). סקר = GET בלבד, ולעולם לא
 * פותח סריקה חדשה. השמירה נעשית דרך v2_commit_liquidity_ai_run הקיים.
 */
import {
  collectorErrorNote,
  collectorZonesToLevels,
  CollectorContractError,
  isCollectorTimeframe,
  isUuid,
  rateLimitNote,
  resultFreshness,
  reviewFreshness,
  type CollectorEnvelope,
  type CollectorPriceRange,
  type CollectorPriceReferenceType,
  type CollectorResult,
  type CollectorTimeframe,
  type CollectorZone,
  type TableLevel,
  type CollectorCapabilities,
  type HeatmapModel,
} from "@/lib/coinglassCollector";

import {
  detectCoinglassHeatmapModel,
  HEATMAP_MODEL_LABEL,
  isHeatmapModel,
} from "@/lib/coinglassModel1";
import {
  signTicket,
  verifyTicket,
  REVIEW_TICKET_TTL_MS,
  TICKET_TTL_MS,
} from "@/lib/coinglassTicket";

export type CollectStatus =
  | "queued"
  | "running"
  | "saved"
  | "duplicate"
  | "stale"
  | "conflict"
  | "failed"
  | "not_connected"
  | "busy"
  | "rate_limited";

export type CollectResponse = {
  status: CollectStatus;
  note: string;
  prerequisite?: string;
  ticket?: string;
  jobId?: string;
  timeframe?: CollectorTimeframe;
  capturedAt?: string;
  provider?: string;
  model?: string;
  /** null בתוצאת טווח — לעולם לא אמצע טווח. */
  observedPrice?: number | null;
  priceReferenceType?: CollectorPriceReferenceType;
  observedPriceRange?: CollectorPriceRange | null;
  omittedAmbiguousZones?: number;

  levels?: number;
  runId?: string;
  /** ניתן לנסות שוב לשמור את אותה תוצאה שכבר נאספה (GET בלבד, בלי סריקה). */
  retryable?: boolean;
  /** מתי מותר לנסות איסוף חדש (מכסה). null = לא ידוע, בלי המצאת זמן. */
  retryAt?: string | null;
  retryAfterSeconds?: number | null;
  /** שמירה שהיא העתקה מודעת של תצפית שמורה, לא איסוף חדש. */
  importedFromStoredObservation?: boolean;
};

/** תצפית שכבר נאספה, כפי שהיא מוצגת לצפייה בלבד — אין כאן שום כתיבה. */
export type SavedObservation = {
  jobId: string;
  timeframe: CollectorTimeframe;
  capturedAt: string;
  ageMs: number;
  stale: boolean;
  provider: string;
  model: string;
  observedPrice: number | null;
  priceReferenceType: CollectorPriceReferenceType;
  observedPriceRange: CollectorPriceRange | null;
  omittedAmbiguousZones: number;
  zones: CollectorZone[];
  levels: TableLevel[];
  evidenceSha256: string;
  /** כרטיס קצר-מועד שמאפשר אישור מפורש להעתקה; בלעדיו אין שמירה. */
  reviewTicket: string;
};

export type ReviewStatus = "available" | "none" | "failed" | "not_connected";

export type ReviewResponse = {
  status: ReviewStatus;
  note: string;
  prerequisite?: string;
  observation?: SavedObservation;
};

export type CollectorConfigLike = { base: string; token: string };

/* eslint-disable @typescript-eslint/no-explicit-any */
export type SupabaseLike = {
  from: (table: string) => any;
  rpc: (name: string, args?: unknown) => any;
};
/* eslint-enable @typescript-eslint/no-explicit-any */

export type CollectDeps = {
  supabase: SupabaseLike;
  userId: string;
  /** זורק כשאין תצורה — חייב להיבדק לפני כל קריאה יוצאת. */
  config: () => CollectorConfigLike;
  startJob: (
    requestId: string,
    timeframe: CollectorTimeframe,
    config: CollectorConfigLike,
    model: HeatmapModel,
  ) => Promise<CollectorEnvelope>;
  readJob: (
    jobId: string,
    timeframe: CollectorTimeframe,
    config: CollectorConfigLike,
    model: HeatmapModel,
  ) => Promise<CollectorEnvelope>;
  /** קריאת התצפית האחרונה שכבר נאספה — GET בלבד, לעולם לא פותח סריקה. */
  readLatestJob?: (
    timeframe: CollectorTimeframe,
    config: CollectorConfigLike,
    model: HeatmapModel,
  ) => Promise<CollectorEnvelope>;
  /** יכולות השירות (/health). בלעדיה מותר רק Model 1 — החוזה הקיים. */
  capabilities?: (config: CollectorConfigLike) => Promise<CollectorCapabilities>;
  now?: () => number;
};

/** זיהוי שגיאות הגשר בלי לייבא את מודול השרת (הליבה נשארת טהורה). */
const errorCode = (error: unknown): string =>
  error && typeof error === "object" && typeof (error as { code?: unknown }).code === "string"
    ? (error as { code: string }).code
    : "";

const rateLimitOf = (
  error: unknown,
): { retryAt: string | null; retryAfterSeconds: number | null } =>
  (error as { info?: { retryAt?: unknown; retryAfterSeconds?: unknown } })?.info &&
  typeof (error as { info: object }).info === "object"
    ? {
        retryAt:
          typeof (error as { info: { retryAt?: unknown } }).info.retryAt === "string"
            ? (error as { info: { retryAt: string } }).info.retryAt
            : null,
        retryAfterSeconds:
          typeof (error as { info: { retryAfterSeconds?: unknown } }).info.retryAfterSeconds ===
          "number"
            ? (error as { info: { retryAfterSeconds: number } }).info.retryAfterSeconds
            : null,
      }
    : { retryAt: null, retryAfterSeconds: null };

/**
 * הנעילה מגינה רק על ההתחלה הקצרה (POST/bootstrap) ומשוחררת ב-finally מיד
 * כשהבקשה הזו נגמרת. אידמפוטנטיות הסריקה מובטחת בשירות החיצוני לפי request_id,
 * והכתיבה מוגנת ב-RPC האטומי הקיים — ולכן אין מנעול לאורך כל מחזור הסקר.
 */
const LEASE_TTL_SECONDS = 60;
const leaseKeyFor = (sessionId: string, fieldId: string, model: HeatmapModel = 1) =>
  model === 1
    ? `coinglass:${sessionId}:${fieldId}`
    : `coinglass:model${model}:${sessionId}:${fieldId}`;

export const CONFLICT_NOTE = "השדה עודכן ידנית בזמן האיסוף — לא נשמר דבר. רעננו ונסו שוב.";
const READ_ERROR_NOTE = "בדיקת ההרשאות או הנתונים נכשלה — לא בוצעה שום פעולה. נסו שוב.";

type Guarded = {
  ok: true;
  workspaceId: string;
  storedUpdatedAt: string | null;
  /** המודל נקבע בשרת מתוך הגדרת השדה עצמה — לא לפי מה שהלקוח מצהיר. */
  model: HeatmapModel;
};
type GuardFailure = { ok: false; response: CollectResponse; readError?: boolean };

const denied = (note: string): GuardFailure => ({
  ok: false,
  response: { status: "failed", note },
});

/** שגיאת קריאה מהמסד אינה "אין הרשאה" — נכשלים סגור, בלי קריאה בתשלום ובלי כתיבה. */
const readFailed = (): GuardFailure => ({
  ok: false,
  readError: true,
  response: { status: "failed", note: READ_ERROR_NOTE, retryable: true },
});

/** גיליון טיוטה, שדה מפת חום פעיל בתבנית של הגיליון, והרשאת עריכה בפועל. */
export async function guardTarget(
  supabase: SupabaseLike,
  sessionId: string,
  fieldId: string,
  expectedModel?: HeatmapModel,
): Promise<Guarded | GuardFailure> {
  const { data: session, error } = await supabase
    .from("v2_collection_sessions")
    .select("id, workspace_id, template_id, status, deleted_at")
    .eq("id", sessionId)
    .maybeSingle();
  if (error) return readFailed();
  if (!session) return denied("הגיליון לא נמצא או אינו נגיש");
  if (session.deleted_at) return denied("גיליון בארכיון הוא לקריאה בלבד");
  if (session.status !== "draft") return denied("הגיליון סגור לעריכה");

  const { data: canEdit, error: permissionError } = await supabase.rpc("v2_can_edit_workspace", {
    p_workspace_id: session.workspace_id,
  });
  if (permissionError) return readFailed();
  if (!canEdit) return denied("אין לכם הרשאת עריכה בסביבת העבודה");

  const { data: field, error: fieldError } = await supabase
    .from("v2_template_fields")
    .select("id, title, field_type, settings, active, section:v2_template_sections(template_id)")
    .eq("id", fieldId)
    .maybeSingle();
  if (fieldError) return readFailed();
  const sectionTemplate = (field?.section as { template_id?: string } | null)?.template_id;
  if (!field || !field.active || sectionTemplate !== session.template_id)
    return denied("השדה אינו שייך לתבנית של הגיליון");
  const model = detectCoinglassHeatmapModel(field);
  if (!model)
    return denied("האיסוף האוטומטי זמין רק בשדות מפת החיסולים של CoinGlass (Model 1/2/3)");
  if (expectedModel !== undefined && expectedModel !== model)
    return denied("המודל המבוקש אינו תואם להגדרת השדה");

  const { data: existing, error: valueError } = await supabase
    .from("v2_collection_values")
    .select("updated_at")
    .eq("session_id", sessionId)
    .eq("field_id", fieldId)
    .maybeSingle();
  if (valueError) return readFailed();

  return {
    ok: true,
    workspaceId: session.workspace_id as string,
    storedUpdatedAt: (existing?.updated_at as string | null) ?? null,
    model,
  };
}

/**
 * מתחילים איסוף רק כשהשירות מפרסם במפורש את המודל ואת הטווח המבוקשים.
 */
async function ensureModelSupported(
  deps: CollectDeps,
  config: CollectorConfigLike,
  model: HeatmapModel,
  timeframe: CollectorTimeframe,
): Promise<CollectResponse | null> {
  if (!deps.capabilities)
    return {
      status: "not_connected",
      note: "האיסוף האוטומטי אינו מחובר",
      prerequisite: `שירות האיסוף אינו מפרסם תמיכה ב-${HEATMAP_MODEL_LABEL[model]}`,
    };
  try {
    const capabilities = await deps.capabilities(config);
    if (
      !capabilities.models.includes(model) ||
      !capabilities.timeframes.includes(timeframe) ||
      capabilities.selectionPolicy !== "prominent_right_edge.v1"
    )
      return {
        status: "not_connected",
        note: "האיסוף האוטומטי אינו מחובר",
        prerequisite: `שירות האיסוף אינו מפרסם תמיכה ב-${HEATMAP_MODEL_LABEL[model]} / ${timeframe}`,
      };
    return null;
  } catch {
    return {
      status: "failed",
      note: "לא ניתן לאמת את יכולות שירות האיסוף כרגע",
      retryable: true,
    };
  }
}

export type StartInput = {
  sessionId: string;
  fieldId: string;
  timeframe: CollectorTimeframe;
  requestId: string;
  baselineUpdatedAt: string | null;
  /** מה שהלקוח מצפה לו; המודל האמיתי נקבע מהשדה והאי-התאמה נדחית. */
  expectedModel?: HeatmapModel;
};

export function validateStartInput(input: StartInput): StartInput {
  if (!input || typeof input !== "object") throw new Error("קלט לא תקין");
  if (!isUuid(input.sessionId) || !isUuid(input.fieldId) || !isUuid(input.requestId))
    throw new Error("מזהה לא תקין");
  if (!isCollectorTimeframe(input.timeframe))
    throw new Error("האיסוף האוטומטי תומך ב-12H, 24H ו-48H בלבד");
  if (input.expectedModel !== undefined && !isHeatmapModel(input.expectedModel))
    throw new Error("מודל מפת חום לא נתמך");

  return {
    sessionId: input.sessionId,
    fieldId: input.fieldId,
    timeframe: input.timeframe,
    requestId: input.requestId,
    baselineUpdatedAt: typeof input.baselineUpdatedAt === "string" ? input.baselineUpdatedAt : null,
    ...(input.expectedModel === undefined ? {} : { expectedModel: input.expectedModel }),
  };
}

const notConnected = (error: unknown): CollectResponse => ({
  status: "not_connected",
  note: "האיסוף האוטומטי אינו מחובר",
  prerequisite: error instanceof Error ? error.message : "תצורה חסרה",
});

export async function startCollection(
  deps: CollectDeps,
  input: StartInput,
): Promise<CollectResponse> {
  const data = validateStartInput(input);
  const guarded = await guardTarget(
    deps.supabase,
    data.sessionId,
    data.fieldId,
    data.expectedModel,
  );
  if (!guarded.ok) return guarded.response;
  if (guarded.storedUpdatedAt !== data.baselineUpdatedAt)
    return { status: "conflict", note: CONFLICT_NOTE };
  const model = guarded.model;

  let config: CollectorConfigLike;
  try {
    config = deps.config();
  } catch (error) {
    return notConnected(error);
  }

  const unsupported = await ensureModelSupported(deps, config, model, data.timeframe);
  if (unsupported) return unsupported;

  const leaseKey = leaseKeyFor(data.sessionId, data.fieldId, model);
  const { data: lease, error: leaseError } = await deps.supabase.rpc("v2_acquire_ai_lease", {
    p_scope_key: leaseKey,
    p_workspace_id: guarded.workspaceId,
    p_ttl_seconds: LEASE_TTL_SECONDS,
  });
  if (leaseError) return { status: "failed", note: "לא ניתן להתחיל איסוף כרגע" };
  const leaseInfo = (lease ?? {}) as { acquired?: boolean; reason?: string };
  if (!leaseInfo.acquired)
    return leaseInfo.reason === "rate_limited"
      ? {
          status: "rate_limited",
          note: rateLimitNote(null),
          retryAt: null,
          retryAfterSeconds: null,
        }
      : { status: "busy", note: "איסוף אחר כבר רץ על השדה הזה" };

  let envelope: CollectorEnvelope;
  try {
    envelope = await deps.startJob(data.requestId, data.timeframe, config, model);
  } catch (error) {
    // 429 מהאספן אינו כישלון: הבקשה הממתינה נשמרת, אין ניסיון חוזר אוטומטי,
    // והמשתמש מקבל זמן המתנה אמיתי בלבד (או הודעה כנה שאינו ידוע).
    if (errorCode(error) === "RATE_LIMITED") {
      const info = rateLimitOf(error);
      return {
        status: "rate_limited",
        note: rateLimitNote(info.retryAt),
        timeframe: data.timeframe,
        retryAt: info.retryAt,
        retryAfterSeconds: info.retryAfterSeconds,
      };
    }
    return {
      status: "failed",
      note: error instanceof Error ? error.message : "פתיחת האיסוף נכשלה",
      // אותו request_id נשמר בצד הלקוח; ניסיון חוזר איתו אינו פותח סריקה נוספת.
      retryable: !(error instanceof CollectorContractError),
    };
  } finally {
    // הנעילה משוחררת תמיד בסיום ההתחלה — הסקר עצמו רץ בלי מנעול.
    // rpc() מחזיר builder שהוא PromiseLike בלבד (ייתכן בלי .catch), ולכן await
    // רגיל בתוך try/catch — אחרת השחרור עלול שלא להתבצע כלל.
    try {
      await deps.supabase.rpc("v2_release_ai_lease", { p_scope_key: leaseKey });
    } catch {
      // שגיאת שחרור אינה חושפת פרטים; הנעילה תפוג מעצמה (TTL קצר).
    }
  }

  const ticket = await signTicket(
    {
      v: 1,
      userId: deps.userId,
      workspaceId: guarded.workspaceId,
      sessionId: data.sessionId,
      fieldId: data.fieldId,
      timeframe: data.timeframe,
      model,
      jobId: envelope.job_id,
      requestId: data.requestId,
      baselineUpdatedAt: data.baselineUpdatedAt,
      exp: (deps.now?.() ?? Date.now()) + TICKET_TTL_MS,
    },
    config.token,
  );

  if (envelope.status === "failed")
    return {
      status: "failed",
      note: collectorErrorNote(envelope.error?.code),
      jobId: envelope.job_id,
    };

  return {
    status: envelope.status === "ready" ? "running" : envelope.status,
    note: envelope.status === "ready" ? "יש תוצאה מוכנה — שומר…" : "האיסוף התחיל",
    ticket,
    jobId: envelope.job_id,
    timeframe: data.timeframe,
  };
}

const runFacts = (result: CollectorResult) => ({
  provider: result.provider,
  model: result.model,
  observedPrice: result.observed_price,
  priceReferenceType: result.price_reference_type,
  observedPriceRange: result.observed_price_range,
  omittedAmbiguousZones: result.omitted_ambiguous_zones,
  levels: result.zones.length,
  runId: result.run_id,
  timeframe: result.timeframe,
  capturedAt: result.captured_at,
});

const applied = (
  result: CollectorResult,
  note: string,
  status: CollectStatus,
): CollectResponse => ({
  status,
  note,
  ...runFacts(result),
});

export async function pollCollection(
  deps: CollectDeps,
  input: { ticket: string },
): Promise<CollectResponse> {
  let config: CollectorConfigLike;
  try {
    config = deps.config();
  } catch (error) {
    return notConnected(error);
  }
  const now = deps.now?.() ?? Date.now();

  let ticket;
  try {
    // מסלול השמירה האוטומטי מקבל אך ורק כרטיס איסוף; כרטיס צפייה נדחה כאן.
    ticket = await verifyTicket(input?.ticket, config.token, {
      userId: deps.userId,
      now,
      purpose: "collect",
    });
  } catch {
    return { status: "failed", note: "הכרטיס אינו תקף — התחילו איסוף חדש" };
  }

  // המודל שבכרטיס חייב להתאים להגדרת השדה — כרטיס של מודל אחר אינו ניתן לשימוש.
  const ticketModel: HeatmapModel = ticket.model ?? 1;
  const guarded = await guardTarget(deps.supabase, ticket.sessionId, ticket.fieldId, ticketModel);
  if (!guarded.ok) return guarded.response;
  if (guarded.workspaceId !== ticket.workspaceId)
    return { status: "failed", note: "הכרטיס אינו שייך לסביבת העבודה הזו" };

  // הסקר לעולם אינו משחרר נעילה — היא שייכת להתחלה, ואולי להתחלה חדשה יותר.
  let envelope: CollectorEnvelope;
  try {
    envelope = await deps.readJob(ticket.jobId, ticket.timeframe, config, ticketModel);
  } catch (error) {
    // תשובה שאינה עומדת בחוזה היא כשל סופי — אין טעם להמשיך לסקור אותה.
    if (error instanceof CollectorContractError)
      return {
        status: "failed",
        note: `תשובת שירות האיסוף אינה תקינה (${error.code}) — לא נשמר דבר`,
        jobId: ticket.jobId,
      };
    // תקלת תקשורת זמנית — ממשיכים עם GET בלבד על אותה משימה.
    return {
      status: "running",
      note: error instanceof Error ? error.message : "שירות האיסוף לא נענה",
      ticket: input.ticket,
      jobId: ticket.jobId,
      retryable: true,
    };
  }

  if (envelope.status === "queued" || envelope.status === "running")
    return {
      status: envelope.status,
      note: "ממתין לאיסוף שכבר רץ במקור…",
      ticket: input.ticket,
      jobId: ticket.jobId,
      timeframe: ticket.timeframe,
    };

  if (envelope.status === "failed" || !envelope.result)
    return {
      status: "failed",
      note: collectorErrorNote(envelope.error?.code),
      jobId: ticket.jobId,
    };

  const result = envelope.result;

  if (result.selection_policy !== "prominent_right_edge.v1")
    return {
      status: "failed",
      note: "תוצאת האיסוף משתמשת בפענוח קודם — לא נשמר דבר",
      jobId: ticket.jobId,
    };

  // הרצה שכבר הוחלה — סקר חוזר או ניסיון שמירה נוסף לא כותב שוב.
  const alreadyApplied = await findAppliedRun(
    deps.supabase,
    ticket.sessionId,
    ticket.fieldId,
    result.run_id,
  );
  if (alreadyApplied === "error")
    return {
      status: "failed",
      note: READ_ERROR_NOTE,
      ticket: input.ticket,
      jobId: ticket.jobId,
      retryable: true,
    };
  if (alreadyApplied === "yes") return applied(result, "התוצאה הזו כבר נשמרה בגיליון", "duplicate");

  const freshness = resultFreshness(result.captured_at, now);
  if (freshness.invalid)
    return { status: "failed", note: "זמן התפיסה של התוצאה אינו תקין — לא נשמר דבר" };
  if (freshness.stale)
    // תוצאה ישנה לעולם לא תתרענן — אין הצעת "נסה לשמור שוב", רק איסוף חדש.
    return {
      status: "stale",
      note: "התוצאה שנאספה ישנה מ-15 דקות ולא נשמרה. יש לאסוף מחדש.",
      jobId: ticket.jobId,
      ...runFacts(result),
    };

  // גרסת הבסיס מגיעה מהכרטיס החתום, לא מהלקוח.
  if (guarded.storedUpdatedAt !== ticket.baselineUpdatedAt)
    return { status: "conflict", note: CONFLICT_NOTE, jobId: ticket.jobId };

  const commit = await commitResult(deps.supabase, ticket, result);
  if (commit === "conflict") {
    const raced = await findAppliedRun(
      deps.supabase,
      ticket.sessionId,
      ticket.fieldId,
      result.run_id,
    );
    return raced === "yes"
      ? applied(result, "התוצאה הזו כבר נשמרה בגיליון", "duplicate")
      : { status: "conflict", note: CONFLICT_NOTE, jobId: ticket.jobId };
  }
  if (commit !== "ok")
    return {
      status: "failed",
      note: "לא התקבל אישור שמירה. אפשר לבדוק ולנסות לשמור שוב את אותה תוצאה.",
      ticket: input.ticket,
      jobId: ticket.jobId,
      retryable: true,
      ...runFacts(result),
    };

  return applied(result, "הנתונים נשמרו בגיליון", "saved");
}

/** שגיאת קריאה אינה "לא הוחל" — מחזירים error ונכשלים סגור לפני כל כתיבה. */
async function findAppliedRun(
  supabase: SupabaseLike,
  sessionId: string,
  fieldId: string,
  runId: string,
): Promise<"yes" | "no" | "error"> {
  const { data, error } = await supabase
    .from("v2_liquidity_ai_runs")
    .select("id")
    .eq("session_id", sessionId)
    .eq("field_id", fieldId)
    .eq("analysis->>collector_run_id", runId)
    .limit(1)
    .maybeSingle();
  if (error) return "error";
  return data ? "yes" : "no";
}

async function commitResult(
  supabase: SupabaseLike,
  ticket: { sessionId: string; fieldId: string; baselineUpdatedAt: string | null },
  result: CollectorResult,
  imported?: { importedAt: string },
): Promise<"ok" | "conflict" | "error"> {
  const levels = collectorZonesToLevels(result);
  const { data: commit, error } = await supabase.rpc("v2_commit_liquidity_ai_run", {
    p_session_id: ticket.sessionId,
    p_field_id: ticket.fieldId,
    p_baseline_updated_at: ticket.baselineUpdatedAt,
    p_results: [
      {
        timeframe: result.timeframe,
        levels,
        ...(result.hover_snapshot ? { hover_snapshot: result.hover_snapshot } : {}),
      },
    ],
    p_audit: [
      {
        timeframe: result.timeframe,
        source_url: result.source_url,
        provider: result.provider,
        model: result.model,
        captured_at: result.captured_at,
        // בתוצאת טווח העמודה נשמרת ריקה — אין הכנסת אמצע טווח כ"מחיר שנצפה".
        observed_price: result.observed_price,
        screenshot: null,
        usage: result.usage ?? null,
        analysis: {
          collector_run_id: result.run_id,
          schema_version: result.schema_version,
          level_contract_version: result.level_contract_version ?? null,
          selection_policy: result.selection_policy ?? null,
          evidence_sha256: result.evidence.sha256,
          evidence_artifact_id: result.evidence.artifact_id,
          source_updated_at: result.source_updated_at,
          quality: result.quality,
          summary: result.summary,
          observed_price: result.observed_price,
          price_reference_type: result.price_reference_type,
          observed_price_range: result.observed_price_range,
          price_axis_range: result.price_axis_range ?? null,
          omitted_ambiguous_zones: result.omitted_ambiguous_zones,
          zones: result.zones,
          ...(result.hover_snapshot ? { hover_snapshot: result.hover_snapshot } : {}),
          // העתקה מודעת של תצפית שמורה מסומנת במפורש; זמן התפיסה נשאר המקורי.
          ...(imported
            ? {
                imported_from_stored_observation: true,
                source_capture_timestamp: result.captured_at,
                imported_at: imported.importedAt,
              }
            : {}),
        },
      },
    ],
  });
  if (error) {
    console.error("[coinglass-collector] commit failed");
    return "error";
  }
  // רק "ok" מפורש נחשב הצלחה; null או סטטוס לא מוכר => כשל, לא שמירה.
  const status = (commit as { status?: unknown } | null)?.status;
  if (status === "ok") return "ok";
  if (status === "conflict") return "conflict";
  return "error";
}

/* ------------------- צפייה בתצפית שמורה והעתקה מודעת ------------------- */

const REVIEW_NONE_NOTE = "אין איסוף שמור זמין לטווח הזה (עד 6 שעות אחורה).";

/**
 * צפייה בלבד בתצפית האחרונה שכבר נאספה: GET אחד לשירות, בלי POST, בלי נגיעה
 * במקור ובלי שום כתיבה. הזמן המוצג הוא זמן התפיסה המקורי, לא "עכשיו".
 */
export async function reviewSavedObservation(
  deps: CollectDeps,
  input: { sessionId: string; fieldId: string; timeframe: CollectorTimeframe },
): Promise<ReviewResponse> {
  if (!isUuid(input?.sessionId) || !isUuid(input?.fieldId))
    return { status: "failed", note: "מזהה לא תקין" };
  if (!isCollectorTimeframe(input.timeframe)) return { status: "failed", note: "טווח זמן לא נתמך" };

  let config: CollectorConfigLike;
  try {
    config = deps.config();
  } catch (error) {
    const response = notConnected(error);
    return { status: "not_connected", note: response.note, prerequisite: response.prerequisite };
  }
  if (!deps.readLatestJob) return { status: "not_connected", note: "קריאת איסוף שמור אינה זמינה" };

  const guarded = await guardTarget(deps.supabase, input.sessionId, input.fieldId);
  if (!guarded.ok) return { status: "failed", note: guarded.response.note };

  let envelope: CollectorEnvelope;
  try {
    envelope = await deps.readLatestJob(input.timeframe, config, guarded.model);
  } catch (error) {
    if (errorCode(error) === "NO_CACHED_RESULT") return { status: "none", note: REVIEW_NONE_NOTE };
    if (error instanceof CollectorContractError)
      return { status: "failed", note: `האיסוף השמור אינו תקין (${error.code}) — לא הוצג` };
    return {
      status: "failed",
      note: error instanceof Error ? error.message : "קריאת האיסוף השמור נכשלה",
    };
  }
  if (envelope.status !== "ready" || !envelope.result)
    return { status: "none", note: REVIEW_NONE_NOTE };

  const result = envelope.result;
  const now = deps.now?.() ?? Date.now();
  const freshness = reviewFreshness(result.captured_at, now);
  // עתידי או ישן מ-6 שעות לא מוצע כלל — לא לצפייה ולא להעתקה.
  if (freshness.invalid || freshness.tooOld) return { status: "none", note: REVIEW_NONE_NOTE };

  const reviewTicket = await signTicket(
    {
      v: 1,
      userId: deps.userId,
      workspaceId: guarded.workspaceId,
      sessionId: input.sessionId,
      fieldId: input.fieldId,
      timeframe: input.timeframe,
      model: guarded.model,
      jobId: envelope.job_id,
      requestId: crypto.randomUUID(),
      baselineUpdatedAt: guarded.storedUpdatedAt,
      exp: now + REVIEW_TICKET_TTL_MS,
      purpose: "review_saved_observation",
      capturedAt: result.captured_at,
      evidenceSha256: result.evidence.sha256,
    },
    config.token,
  );

  return {
    status: "available",
    note: "הוצג איסוף שמור — לא בוצע צילום חדש.",
    observation: {
      jobId: envelope.job_id,
      timeframe: result.timeframe,
      capturedAt: result.captured_at,
      ageMs: freshness.ageMs,
      stale: freshness.stale,
      provider: result.provider,
      model: result.model,
      observedPrice: result.observed_price,
      priceReferenceType: result.price_reference_type,
      observedPriceRange: result.observed_price_range,
      omittedAmbiguousZones: result.omitted_ambiguous_zones,
      zones: result.zones,
      levels: collectorZonesToLevels(result),
      evidenceSha256: result.evidence.sha256,
      reviewTicket,
    },
  };
}

/**
 * העתקה מודעת של אותה תצפית שמורה לתיבה, עם זמן האיסוף המקורי. זו פעולה
 * היסטורית מפורשת: אישור חובה, כרטיס חתום לפעולה הזו בלבד, קריאת GET חוזרת
 * לאותה משימה, התאמת זמן תפיסה וחתימת ראיה, ואותה כתיבה אטומית קיימת.
 */
export async function importSavedObservation(
  deps: CollectDeps,
  input: { reviewTicket: string; confirm: boolean },
): Promise<CollectResponse> {
  if (input?.confirm !== true)
    return { status: "failed", note: "נדרש אישור מפורש לשמירת האיסוף השמור" };

  let config: CollectorConfigLike;
  try {
    config = deps.config();
  } catch (error) {
    return notConnected(error);
  }
  const now = deps.now?.() ?? Date.now();

  let ticket;
  try {
    ticket = await verifyTicket(input.reviewTicket, config.token, {
      userId: deps.userId,
      now,
      purpose: "review_saved_observation",
    });
  } catch {
    return { status: "failed", note: "אישור הצפייה אינו תקף — פתחו שוב את האיסוף השמור" };
  }

  const reviewModel: HeatmapModel = ticket.model ?? 1;
  const guarded = await guardTarget(deps.supabase, ticket.sessionId, ticket.fieldId, reviewModel);
  if (!guarded.ok) return guarded.response;
  if (guarded.workspaceId !== ticket.workspaceId)
    return { status: "failed", note: "הכרטיס אינו שייך לסביבת העבודה הזו" };
  let envelope: CollectorEnvelope;
  try {
    // קריאה בלבד של אותה משימה — לעולם לא סריקה חדשה.
    envelope = await deps.readJob(ticket.jobId, ticket.timeframe, config, reviewModel);
  } catch (error) {
    if (error instanceof CollectorContractError)
      return { status: "failed", note: `האיסוף השמור אינו תקין (${error.code}) — לא נשמר דבר` };
    return {
      status: "failed",
      note: error instanceof Error ? error.message : "קריאת האיסוף השמור נכשלה",
      retryable: true,
    };
  }
  if (envelope.status !== "ready" || !envelope.result)
    return { status: "failed", note: "האיסוף השמור אינו זמין יותר — לא נשמר דבר" };

  const result = envelope.result;
  if (result.captured_at !== ticket.capturedAt || result.evidence.sha256 !== ticket.evidenceSha256)
    return { status: "failed", note: "האיסוף השמור השתנה מאז הצפייה — לא נשמר דבר" };

  const freshness = reviewFreshness(result.captured_at, now);
  if (freshness.invalid || freshness.tooOld)
    return { status: "failed", note: "האיסוף השמור ישן מ-6 שעות ולכן אינו ניתן לשמירה" };

  const alreadyApplied = await findAppliedRun(
    deps.supabase,
    ticket.sessionId,
    ticket.fieldId,
    result.run_id,
  );
  if (alreadyApplied === "error")
    return { status: "failed", note: READ_ERROR_NOTE, retryable: true };
  // התאוששות מאישור שמירה שאבד: הרצה שכבר הוחלה מוכרזת ככפולה לפני בדיקת
  // גרסת הבסיס, ואילו שינוי ידני אמיתי שלא הוחל עדיין מחזיר התנגשות.
  if (alreadyApplied === "yes") return applied(result, "התוצאה הזו כבר נשמרה בגיליון", "duplicate");

  if (guarded.storedUpdatedAt !== ticket.baselineUpdatedAt)
    return { status: "conflict", note: CONFLICT_NOTE };

  const commit = await commitResult(deps.supabase, ticket, result, {
    importedAt: new Date(now).toISOString(),
  });
  if (commit === "conflict") {
    const raced = await findAppliedRun(
      deps.supabase,
      ticket.sessionId,
      ticket.fieldId,
      result.run_id,
    );
    return raced === "yes"
      ? applied(result, "התוצאה הזו כבר נשמרה בגיליון", "duplicate")
      : { status: "conflict", note: CONFLICT_NOTE };
  }
  if (commit !== "ok")
    return {
      status: "failed",
      note: "לא התקבל אישור שמירה. ניתן לבדוק ולנסות לשמור את אותה תוצאה.",
      retryable: true,
    };

  return {
    ...applied(result, "האיסוף השמור הועתק לתיבה עם זמן האיסוף המקורי", "saved"),
    importedFromStoredObservation: true,
  };
}
