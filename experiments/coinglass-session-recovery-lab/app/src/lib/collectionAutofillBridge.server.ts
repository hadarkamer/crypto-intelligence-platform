import { timingSafeEqual } from "node:crypto";
import type { SupabaseClient } from "@supabase/supabase-js";
import type { AutofillResult } from "./collectionAutofill";
import { sourceUrls } from "./collectionAutofill";
import { economicCalendarScanIdentity } from "./collectionEconomicCalendar";
import { validatedAutofillSerialGroups } from "./collectionAutofillQueue";
import type { ExpiryMaintenanceResult } from "./collectionExpiryHistory.server";

const MAX_BODY_BYTES = 2048;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const STATUSES: ReadonlySet<AutofillResult["status"]> = new Set([
  "completed",
  "unchanged",
  "running",
  "failed",
  "conflict",
]);

type Fill = (
  db: SupabaseClient,
  fieldId: string,
  sessionId: string,
  manual: boolean,
  resumeRunId?: string,
) => Promise<AutofillResult>;
type Services = {
  db: SupabaseClient;
  fill: Fill;
  maintainExpiries?: (db: SupabaseClient) => Promise<ExpiryMaintenanceResult>;
};
export type AutofillBridgeDependencies = {
  environment?: Record<string, string | undefined>;
  loadServices?: () => Promise<Services>;
};
type BridgeInput =
  { action: "due" } | { action: "fill"; field_id: string; session_id: string; run_id?: string };

function response(body: Record<string, unknown>, status = 200): Response {
  return Response.json(body, {
    status,
    headers: {
      "Cache-Control": "no-store",
      ...(status === 405 ? { Allow: "POST" } : {}),
    },
  });
}

function authenticated(request: Request, token: string | undefined): boolean {
  if (!token || !/^[a-f0-9]{64}$/.test(token)) return false;
  const authorization = request.headers.get("authorization");
  if (!authorization || !/^Bearer [a-f0-9]{64}$/.test(authorization)) return false;
  return timingSafeEqual(Buffer.from(authorization.slice(7), "hex"), Buffer.from(token, "hex"));
}

class InvalidBody extends Error {
  constructor(readonly status: 400 | 413) {
    super(status === 413 ? "body_too_large" : "invalid_request");
  }
}

async function readInput(request: Request): Promise<BridgeInput> {
  if (
    request.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase() !==
    "application/json"
  )
    throw new InvalidBody(400);
  const declaredLength = request.headers.get("content-length");
  if (declaredLength !== null) {
    if (!/^\d+$/.test(declaredLength)) throw new InvalidBody(400);
    if (Number(declaredLength) > MAX_BODY_BYTES) throw new InvalidBody(413);
  }
  if (!request.body) throw new InvalidBody(400);
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > MAX_BODY_BYTES) {
        await reader.cancel();
        throw new InvalidBody(413);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  const body: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  if (!body || typeof body !== "object" || Array.isArray(body)) throw new InvalidBody(400);
  const input = body as Record<string, unknown>;
  const keys = Object.keys(input);
  if (input.action === "due" && keys.length === 1) return { action: "due" };
  if (
    input.action === "fill" &&
    (keys.length === 3 ||
      (keys.length === 4 && typeof input.run_id === "string" && UUID.test(input.run_id))) &&
    typeof input.field_id === "string" &&
    typeof input.session_id === "string" &&
    UUID.test(input.field_id) &&
    UUID.test(input.session_id)
  )
    return {
      action: "fill",
      field_id: input.field_id,
      session_id: input.session_id,
      ...(typeof input.run_id === "string" ? { run_id: input.run_id } : {}),
    };
  throw new InvalidBody(400);
}

async function loadServices(): Promise<Services> {
  const [{ supabaseAdmin }, { fillCollectionField }, { maintainCollectionExpiries }] =
    await Promise.all([
      import("@/integrations/supabase/client.server"),
      import("./collectionAutofill.server"),
      import("./collectionExpiryHistory.server"),
    ]);
  return {
    db: supabaseAdmin as unknown as SupabaseClient,
    fill: fillCollectionField,
    maintainExpiries: maintainCollectionExpiries,
  };
}

/** The scheduler holds only a narrow worker token. Database and provider keys
 * stay in the application backend and are never returned to GitHub Actions. */
export async function handleCollectionAutofillBridge(
  request: Request,
  dependencies: AutofillBridgeDependencies = {},
): Promise<Response> {
  if (request.method !== "POST") return response({ error: "method_not_allowed" }, 405);
  const environment = dependencies.environment ?? process.env;
  if (!authenticated(request, environment.COLLECTION_AUTOFILL_WORKER_TOKEN))
    return response({ error: "unauthorized" }, 401);
  if (
    !["OPENAI_API_KEY", "SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"].every((name) =>
      environment[name]?.trim(),
    )
  )
    return response({ error: "autofill_not_configured" }, 503);

  let input: BridgeInput;
  try {
    input = await readInput(request);
  } catch (error) {
    return response(
      {
        error:
          error instanceof InvalidBody && error.status === 413
            ? "body_too_large"
            : "invalid_request",
      },
      error instanceof InvalidBody ? error.status : 400,
    );
  }
  let stage = "load_services";
  try {
    // No privileged client, scanner or model code is imported until the worker
    // token, server configuration and bounded request have all been validated.
    const { db, fill, maintainExpiries } = await (dependencies.loadServices ?? loadServices)();
    if (input.action === "due") {
      // Expired rows and completed-day statistics are maintained even when the
      // ordinary autofill schedule has no due fields and no browser is open.
      stage = "expiry_maintenance";
      await maintainExpiries?.(db);
      stage = "rollover";
      const rollover = await db.rpc("v2_collection_autofill_rollover");
      if (rollover.error) throw rollover.error;
      stage = "due_fields";
      const due = await db.rpc("v2_collection_autofill_due", { p_limit: 100 });
      if (due.error) throw due.error;
      if (!Array.isArray(due.data) || due.data.length > 100) throw new Error("autofill_failed");
      const fields = due.data.map((field: unknown) => {
        if (!field || typeof field !== "object") throw new Error("autofill_failed");
        const row = field as Record<string, unknown>;
        if (
          typeof row.field_id !== "string" ||
          typeof row.session_id !== "string" ||
          !UUID.test(row.field_id) ||
          !UUID.test(row.session_id)
        )
          throw new Error("autofill_failed");
        return { field_id: row.field_id, session_id: row.session_id };
      });
      // Return only a public scheduling hint, never field settings or sources.
      // The relay serializes maps before claiming them, so their leases start
      // when collection can begin rather than while another map is running.
      if (!fields.length) return response({ fields });
      stage = "field_metadata";
      const metadata = await db
        .from("v2_template_fields")
        .select("id,title,field_type,settings")
        .in(
          "id",
          fields.map((field) => field.field_id),
        );
      if (metadata.error) throw metadata.error;
      const serialFields = validatedAutofillSerialGroups(
        metadata.data,
        fields.map((field) => field.field_id),
      );
      // Metadata has passed the complete field-ID/type/settings validation
      // above. Duplicate calendar boxes share one workspace calendar, but
      // distinct sessions (including other workspaces) retain their own work.
      const calendars = new Map<string, string>();
      for (const field of metadata.data as Array<{
        id: string;
        field_type: string;
        settings: Record<string, unknown>;
      }>) {
        const identity = economicCalendarScanIdentity(field, sourceUrls(field));
        if (identity) calendars.set(field.id, identity);
      }
      const seenCalendars = new Set<string>();
      const scheduled = fields.filter((field) => {
        const identity = calendars.get(field.field_id);
        if (!identity) return true;
        const key = `${field.session_id}:${identity}`;
        if (seenCalendars.has(key)) return false;
        seenCalendars.add(key);
        return true;
      });
      return response({
        fields: scheduled.map((field) => ({
          ...field,
          ...(serialFields.has(field.field_id)
            ? { serial_group: serialFields.get(field.field_id)! }
            : {}),
        })),
      });
    }
    stage = "fill";
    const result = input.run_id
      ? await fill(db, input.field_id, input.session_id, false, input.run_id)
      : await fill(db, input.field_id, input.session_id, false);
    if (!STATUSES.has(result.status)) throw new Error("autofill_failed");
    if (result.continue && (result.status !== "running" || !UUID.test(result.run_id)))
      throw new Error("autofill_failed");
    if (input.run_id && result.run_id !== input.run_id) throw new Error("autofill_failed");
    return response({
      status: result.status,
      ...(result.continue ? { continue: true } : {}),
      ...(result.continue || input.run_id ? { run_id: result.run_id } : {}),
    });
  } catch (error) {
    const code = error && typeof error === "object" && "code" in error ? String(error.code) : "";
    console.error("collection_autofill_failed", {
      action: input.action,
      stage,
      ...(input.action === "fill"
        ? { field_id: input.field_id, session_id: input.session_id }
        : {}),
      ...(/^[0-9A-Z]{5}$/.test(code) ? { database_code: code } : {}),
    });
    return response({ error: "autofill_failed" }, 500);
  }
}
