import type { SupabaseClient } from "@supabase/supabase-js";

export type CollectionAutofillScan = {
  id: string;
  field_id: string;
  logical_key: string;
  sheet_key: string;
  session_id: string | null;
  status: "completed" | "unchanged" | "failed" | "conflict" | "running";
  finished_at: string | null;
  created_at?: string;
  message?: string | null;
  /** Display-only evidence of a later dedicated save; the run status is unchanged. */
  superseded_at?: string;
};
export type CollectionScanScope = {
  sheetKey: string;
  logicalKey: string;
  sessionId: string;
};
export const COLLECTION_AUTOFILL_METADATA_COLOR = "text-purple-700 dark:text-purple-300";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const PAGE_SIZE = 100;
const RUNNING_TIMEOUT_MS = 20 * 60_000;

/** Durable data is shared by logical field; news keeps its original session. */
export function collectionScanKey(scope: CollectionScanScope): string {
  return JSON.stringify([
    scope.sheetKey,
    scope.sheetKey === "data" ? null : scope.sessionId,
    scope.logicalKey,
  ]);
}

const scanTimestamp = (row: CollectionAutofillScan) => row.created_at ?? row.finished_at ?? "";

export function latestCollectionScans(
  rows: CollectionAutofillScan[],
  scopes: CollectionScanScope[],
): Map<string, CollectionAutofillScan> {
  const expected = new Set(scopes.map(collectionScanKey));
  const latest = new Map<string, CollectionAutofillScan>();
  for (const row of rows) {
    if (
      !row ||
      !["completed", "unchanged", "failed", "conflict", "running"].includes(row.status) ||
      (row.status === "running" && !row.created_at) ||
      !Number.isFinite(Date.parse(scanTimestamp(row)))
    )
      continue;
    const key = collectionScanKey({
      sheetKey: row.sheet_key,
      logicalKey: row.logical_key,
      sessionId: row.session_id ?? "",
    });
    if (!expected.has(key)) continue;
    const previous = latest.get(key);
    if (
      !previous ||
      Date.parse(scanTimestamp(row)) > Date.parse(scanTimestamp(previous)) ||
      (Date.parse(scanTimestamp(row)) === Date.parse(scanTimestamp(previous)) &&
        row.id > previous.id)
    )
      latest.set(key, row);
  }
  return latest;
}

/** Read only the public run status/timestamp, with existing workspace-member RLS.
 * Pages stop once the newest started scan for every visible identity is found. */
export async function fetchLatestCollectionScans(
  db: SupabaseClient,
  workspaceId: string,
  scopes: CollectionScanScope[],
): Promise<Map<string, CollectionAutofillScan>> {
  const expected = new Set(scopes.map(collectionScanKey));
  if (!expected.size) return new Map();
  const sessions = [
    ...new Set(scopes.filter((scope) => scope.sheetKey !== "data").map((scope) => scope.sessionId)),
  ];
  if (sessions.some((id) => !UUID.test(id))) throw new Error("Invalid collection scan session.");
  const logicalKeys = [...new Set(scopes.map((scope) => scope.logicalKey))];
  const rows: CollectionAutofillScan[] = [];
  for (let from = 0; ;) {
    let query = db
      .from("v2_collection_autofill_runs")
      .select("id,field_id,logical_key,sheet_key,session_id,status,finished_at,created_at,message")
      .eq("workspace_id", workspaceId)
      .in("logical_key", logicalKeys)
      .in("status", ["completed", "unchanged", "failed", "conflict", "running"]);
    query = sessions.length
      ? query.or(`sheet_key.eq.data,session_id.in.(${sessions.join(",")})`)
      : query.eq("sheet_key", "data");
    const result = await query
      .order("created_at", { ascending: false })
      .order("id", { ascending: false })
      .range(from, from + PAGE_SIZE - 1);
    if (result.error) throw result.error;
    if (!Array.isArray(result.data) || result.data.length > PAGE_SIZE)
      throw new Error("Invalid collection scan response.");
    rows.push(...(result.data as CollectionAutofillScan[]));
    const latest = latestCollectionScans(rows, scopes);
    if (latest.size === expected.size || result.data.length === 0)
      return withoutSupersededLiquidityErrors(db, workspaceId, latest);
    // A server-side response cap may be lower than our requested page size.
    from += result.data.length;
  }
}

/** Dedicated CoinGlass saves do not write a general autofill run. Reconcile
 * only the display against committed success evidence; retain the run history. */
async function withoutSupersededLiquidityErrors(
  db: SupabaseClient,
  workspaceId: string,
  latest: Map<string, CollectionAutofillScan>,
): Promise<Map<string, CollectionAutofillScan>> {
  const failures = [...latest.entries()].flatMap(([key, scan]) => {
    if (scan.sheet_key !== "data" || !collectionScanProblem(scan)) return [];
    const endedAt =
      scan.status === "running"
        ? Date.parse(scan.created_at ?? "") + RUNNING_TIMEOUT_MS
        : Date.parse(scan.finished_at ?? "");
    return Number.isFinite(endedAt) ? [{ key, fieldId: scan.field_id, endedAt }] : [];
  });
  if (!failures.length) return latest;
  const since = new Date(Math.min(...failures.map((item) => item.endedAt))).toISOString();
  const fieldIds = [...new Set(failures.map((item) => item.fieldId))];
  for (let from = 0; ;) {
    const { data, error } = await db
      .from("v2_liquidity_ai_runs")
      .select("field_id,created_at,captured_at")
      .eq("workspace_id", workspaceId)
      .in("field_id", fieldIds)
      .gt("created_at", since)
      .gt("captured_at", since)
      .order("created_at", { ascending: false })
      .order("id", { ascending: false })
      .range(from, from + PAGE_SIZE - 1);
    // If success cannot be verified, preserve the existing warning.
    if (error || !Array.isArray(data) || !data.length) return latest;
    for (const failure of failures) {
      const saved = data.find(
        (saved) =>
          saved.field_id === failure.fieldId &&
          Date.parse(saved.created_at) > failure.endedAt &&
          Date.parse(saved.captured_at) > failure.endedAt,
      );
      const scan = latest.get(failure.key)!;
      if (saved && !scan.superseded_at)
        latest.set(failure.key, { ...scan, superseded_at: saved.created_at });
    }
    if (failures.every((failure) => latest.get(failure.key)?.superseded_at)) return latest;
    from += data.length;
  }
}

/** Scan time is explicit and stable across browser time zones and page reloads. */
export function formatCollectionScanTime(timestamp: string): string | null {
  const date = new Date(timestamp);
  if (!Number.isFinite(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat("he-IL", {
    timeZone: "Asia/Jerusalem",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((item) => item.type === type)?.value;
  return `${part("day")}.${part("month")}.${part("year")} ${part("hour")}:${part("minute")}`;
}

export function collectionNoNewScanLabel(scan?: CollectionAutofillScan): string | null {
  if (scan?.superseded_at) return null;
  if (scan?.status === "running" && !collectionScanProblem(scan)) return "הסריקה מתבצעת כעת…";
  if (scan && ["completed", "unchanged"].includes(scan.status) && scan.message?.trim())
    return scan.message.trim();
  if (scan?.status !== "unchanged" || !scan.finished_at) return null;
  const time = formatCollectionScanTime(scan.finished_at);
  return time ? `נסרק ב־${time}, אין חדש` : null;
}

export function collectionScanProblem(scan?: CollectionAutofillScan): string | null {
  if (scan?.superseded_at) return null;
  if (
    scan?.status === "running" &&
    scan.created_at &&
    Date.now() - Date.parse(scan.created_at) >= RUNNING_TIMEOUT_MS
  )
    return "חלף זמן ההרצה המותר ללא אישור סיום. אין להסתמך על תוצאת הסריקה הקודמת כעדכון חדש.";
  if (!scan || !["failed", "conflict"].includes(scan.status)) return null;
  return scan.message?.trim() || "הסריקה נכשלה; התיבה לא שונתה. אפשר לנסות שוב.";
}
