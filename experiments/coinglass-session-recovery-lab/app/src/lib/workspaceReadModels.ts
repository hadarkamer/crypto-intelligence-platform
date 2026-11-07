import type { SupabaseClient } from "@supabase/supabase-js";
import type {
  V2NewsItem,
  V2NewsSource,
  V2Sentiment,
  V2TemplateField,
  V2TemplateSection,
} from "./workspaceV1";

export type CollectionSnapshotMode = "latest_filled" | "last_session" | "session";
export type CollectionSnapshotRow = {
  snapshot_key: string;
  sheet_key: string;
  logical_key: string;
  field_id: string;
  field: V2TemplateField;
  section: Omit<V2TemplateSection, "fields">;
  session_id: string;
  session_created_at: string;
  value: Record<string, unknown>;
  news_items: V2NewsItem[];
  updated_at: string;
  updated_by: string | null;
};

export type WorkspaceNewsFeedRow = Omit<V2NewsItem, "created_by" | "source"> & {
  created_by: string | null;
  source: V2NewsSource | null;
  source_name: string;
  origin:
    | "collection_news"
    | "legacy_project"
    | "collection_value"
    | "trade_value"
    | "scenario_value"
    | "spot_value";
  read_only: boolean;
  project_id: string | null;
  trade_id: string | null;
  entity_id: string | null;
  raw_content: Record<string, unknown> | null;
  question_id: string | null;
  worksheet_created_at: string;
};

/** A short page can be the server's configured cap, not the end of the report.
 * Use stable IDs so an earlier insert/delete cannot shift the next page. */
async function readStablePages<T>(
  page: (
    after: string | null,
  ) => PromiseLike<{ data: T[] | null; error: unknown; count?: number | null }>,
  keyOf: (row: T) => string,
): Promise<T[]> {
  const result: T[] = [];
  let after: string | null = null;
  for (;;) {
    const { data, error, count } = await page(after);
    if (error) throw error;
    const rows = data ?? [];
    if (!rows.length) return result;
    const next = keyOf(rows[rows.length - 1]);
    if (!next || next === after) throw new Error("Workspace report pagination did not advance");
    result.push(...rows);
    if (count != null && count >= 0 && rows.length >= count) return result;
    after = next;
  }
}

/** Full values and source timestamps; no text truncation or display conversion.
 * The RPC resolves logical field identities across immutable template versions. */
export async function readCollectionSnapshot(
  db: SupabaseClient,
  workspaceId: string,
  mode: CollectionSnapshotMode = "latest_filled",
  sessionId?: string,
): Promise<CollectionSnapshotRow[]> {
  if (mode === "session" && !sessionId) throw new Error("A collection session is required");
  return readStablePages<CollectionSnapshotRow>(
    (after) => {
      const query = db
        .rpc(
          "v2_collection_snapshot",
          {
            p_workspace_id: workspaceId,
            p_mode: mode,
            p_session_id: sessionId ?? null,
          },
          { count: "exact" },
        )
        .order("snapshot_key")
        .limit(500);
      return after === null ? query : query.gt("snapshot_key", after);
    },
    (row) => row.snapshot_key,
  );
}

/** This read API includes caller-accessible legacy news without copying it into
 * a shared workspace. Native news CRUD continues to use v2_news_items. */
export async function readWorkspaceNewsFeed(
  db: SupabaseClient,
  workspaceId: string,
  filters: {
    from?: string;
    importance?: V2Sentiment[];
    sourceId?: string;
  } = {},
): Promise<WorkspaceNewsFeedRow[]> {
  const rows = await readStablePages<WorkspaceNewsFeedRow>(
    (after) => {
      let query = db
        .rpc("v2_news_feed", { p_workspace_id: workspaceId }, { count: "exact" })
        .order("id")
        .limit(500);
      if (after !== null) query = query.gt("id", after);
      // Time ranges follow the news timestamp shown to the reader, even in an older worksheet.
      if (filters.from) query = query.gte("occurred_at", filters.from);
      if (filters.importance?.length) query = query.in("importance", filters.importance);
      if (filters.sourceId) query = query.eq("source_id", filters.sourceId);
      return query;
    },
    (row) => row.id,
  );
  return rows.sort(
    (a, b) => Date.parse(b.occurred_at) - Date.parse(a.occurred_at) || a.id.localeCompare(b.id),
  );
}
