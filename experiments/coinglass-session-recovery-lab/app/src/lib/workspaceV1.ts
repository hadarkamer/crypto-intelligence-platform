import { readAllPages } from "./readAllPages";
import type { SupabaseClient } from "@supabase/supabase-js";
import { supabase } from "@/integrations/supabase/client";
import { effectiveTemplateRequirements } from "./v2TradeFieldPolicy";
import { captureAuthWriteScope } from "./authWriteScope.ts";
import { worksheetSaveReceipt } from "./worksheetDraftState";
import type { Scenario } from "./v2ScenarioTrade";
import {
  readCollectionSnapshot,
  readWorkspaceNewsFeed,
  type CollectionSnapshotMode,
} from "./workspaceReadModels";

const db = supabase as unknown as SupabaseClient;

/** Backend read contracts for the independent collection and news sections. */
export function fetchV2CollectionSnapshot(
  workspaceId: string,
  mode: CollectionSnapshotMode = "latest_filled",
  sessionId?: string,
) {
  return readCollectionSnapshot(db, workspaceId, mode, sessionId);
}

export function listWorkspaceNewsFeed(
  workspaceId: string,
  filters: { from?: string; importance?: V2Sentiment[]; sourceId?: string } = {},
) {
  return readWorkspaceNewsFeed(db, workspaceId, filters);
}

export const V2_SENTIMENTS = [
  "very_bullish",
  "bullish",
  "positive",
  "neutral",
  "negative",
  "bearish",
  "very_bearish",
] as const;
export type V2Sentiment = (typeof V2_SENTIMENTS)[number];

export const V2_SENTIMENT_LABELS: Record<V2Sentiment, string> = {
  very_bullish: "שורי מאוד",
  bullish: "שורי",
  positive: "חיובי",
  neutral: "ניטרלי",
  negative: "שלילי",
  bearish: "דובי",
  very_bearish: "דובי מאוד",
};

export const V2_SENTIMENT_WEIGHT: Record<V2Sentiment, number> = {
  very_bullish: 3,
  bullish: 2,
  positive: 1,
  neutral: 0,
  negative: -1,
  bearish: -2,
  very_bearish: -3,
};

export type V2Workspace = {
  id: string;
  owner_id: string;
  name: string;
  created_at: string;
  updated_at: string;
};

export type V2Template = {
  id: string;
  workspace_id: string;
  kind: "collection" | "scenario" | "trade" | "spot";
  name: string;
  active: boolean;
  is_default: boolean;
  asset_symbol: string | null;
  settings: Record<string, unknown>;
  created_by: string | null;
  created_at: string;
  updated_at: string;
};

export type V2TemplateField = {
  id: string;
  section_id: string;
  field_key: string;
  title: string;
  field_type: string;
  position: number;
  required: boolean;
  default_align: "right" | "left";
  pinned_scope: "none" | "section" | "template";
  settings: Record<string, unknown>;
  active: boolean;
};

export type V2TemplateSection = {
  id: string;
  template_id: string;
  title: string;
  position: number;
  active: boolean;
  settings: Record<string, unknown>;
  fields: V2TemplateField[];
};

export type V2CollectionSession = {
  id: string;
  workspace_id: string;
  template_id: string | null;
  title: string;
  status: "draft" | "completed" | "archived";
  created_by: string;
  last_edited_by: string | null;
  deleted_at?: string | null;
  creation_origin?: "user" | "chatgpt";
  created_at: string;
  updated_at: string;
};

export type V2CollectionValue = {
  id: string;
  session_id: string;
  field_id: string;
  value: Record<string, unknown>;
  updated_by: string | null;
  created_at: string;
  updated_at: string;
  updated_origin?: "user" | "chatgpt";
};

export type V2NewsSource = {
  id: string;
  workspace_id: string;
  name: string;
  current_sentiment: V2Sentiment;
  current_scenario: string | null;
  status_defined?: boolean;
  updated_by: string | null;
  created_at: string;
  updated_at: string;
};

export type V2NewsItem = {
  id: string;
  workspace_id: string;
  collection_session_id: string | null;
  field_id?: string | null;
  source_id: string | null;
  body: string;
  importance: V2Sentiment;
  occurred_at: string;
  created_by: string;
  updated_by: string | null;
  created_at: string;
  updated_at: string;
  source?: V2NewsSource | null;
  updated_origin?: "user" | "chatgpt";
  source_url?: string | null;
  source_name?: string | null;
  category?: "bullish" | "bearish" | null;
  rating_pending?: boolean;
};

export type V2Scenario = {
  id: string;
  template_id?: string | null;
  author_name?: string | null;
  alternatives?: Array<{
    id: string;
    scenario_html: string;
    reasons_html: string;
    invalidation_html: string;
  }>;
  workspace_id: string;
  asset_symbol: string;
  title: string;
  scenario_html: string | null;
  reasons_html: string | null;
  invalidation_html: string | null;
  status: "active" | "not_relevant" | "archived";
  pinned: boolean;
  position: number;
  created_by: string;
  updated_by: string | null;
  created_at: string;
  updated_at: string;
};

export type V2AutomatedTradeCard = {
  workspace_id: string;
  card_id: string;
  revision: number;
  payload: Record<string, unknown>;
  observed_at: string;
  created_at: string;
  updated_at: string;
};

export type V2Trade = {
  id: string;
  archived_at?: string | null;
  root_trade_id?: string | null;
  cycle_number?: number;
  workspace_id: string;
  template_id: string | null;
  scenario_id: string | null;
  source_kind?: "trade" | "bot_alert";
  bot_alert_html?: string;
  allow_cross_asset_scenario?: boolean;
  asset_symbol: string;
  title: string;
  status: "evaluating" | "limit" | "active" | "closed";
  activated_at: string | null;
  closed_at: string | null;
  pnl: number | null;
  /** מועד רישום ה-PnL הראשון, נשמר בשרת ואינו זז */
  first_pnl_at?: string | null;
  /** סימון break even מפורש של המשתמש */
  break_even?: boolean;
  formula_name?: string | null;
  created_by: string;
  updated_by: string | null;
  created_at: string;
  updated_at: string;
};

export type V2SpotAsset = {
  id: string;
  workspace_id: string;
  symbol: string;
  category: "btc" | "alt" | "stable";
  quantity: number;
  location_type: "exchange" | "wallet" | "other" | null;
  location_name: string | null;
  wallet_address: string | null;
  device_name: string | null;
  security_notes: string | null;
  created_by: string;
  updated_by: string | null;
  created_at: string;
  updated_at: string;
};

export type V2HubSummary = {
  legacyProjects: number;
  collections: number;
  news: number;
  scenarios: number;
  activeTrades: number;
  closedTrades: number;
  spotAssets: number;
};

function throwOn(error: unknown): asserts error is null | undefined {
  if (error) throw error;
}

export async function ensureV2Workspace(): Promise<V2Workspace> {
  const { data: workspaceId, error: rpcError } = await db.rpc("v2_ensure_my_workspace");
  throwOn(rpcError);
  if (!workspaceId) throw new Error("לא ניתן היה ליצור סביבת עבודה.");
  const { data, error } = await db.from("v2_workspaces").select("*").eq("id", workspaceId).single();
  throwOn(error);
  return data as V2Workspace;
}

export async function listV2Templates(
  workspaceId: string,
  kind?: V2Template["kind"],
): Promise<V2Template[]> {
  let query = db
    .from("v2_templates")
    .select("*")
    .eq("workspace_id", workspaceId)
    .eq("active", true)
    .order("is_default", { ascending: false })
    .order("updated_at", { ascending: false });
  if (kind) query = query.eq("kind", kind);
  const { data, error } = await query;
  throwOn(error);
  return (data ?? []) as V2Template[];
}

export async function fetchV2TemplateStructure(templateId: string): Promise<V2TemplateSection[]> {
  const [templateResult, sectionsResult] = await Promise.all([
    db.from("v2_templates").select("kind").eq("id", templateId).single(),
    db
      .from("v2_template_sections")
      .select("*")
      .eq("template_id", templateId)
      .eq("active", true)
      .order("position"),
  ]);
  throwOn(templateResult.error);
  throwOn(sectionsResult.error);
  const sections = (sectionsResult.data ?? []) as Omit<V2TemplateSection, "fields">[];
  if (!sections.length) return [];
  const fields: V2TemplateField[] = [];
  for (let offset = 0; ;) {
    const result = await db
      .from("v2_template_fields")
      .select("*")
      .in(
        "section_id",
        sections.map((section) => section.id),
      )
      .eq("active", true)
      .order("position")
      .order("id")
      .range(offset, offset + 999);
    throwOn(result.error);
    fields.push(...((result.data ?? []) as V2TemplateField[]));
    if (!result.data?.length) break;
    offset += result.data.length;
  }
  return effectiveTemplateRequirements(
    String(templateResult.data.kind),
    sections.map((section) => ({
      ...section,
      fields: fields.filter((field) => field.section_id === section.id),
    })),
  );
}

export async function listV2CollectionSessions(
  workspaceId: string,
): Promise<V2CollectionSession[]> {
  const sessions: V2CollectionSession[] = [];
  let after: string | null = null;
  for (;;) {
    let query = db
      .from("v2_collection_sessions")
      .select("*")
      .eq("workspace_id", workspaceId)
      .is("deleted_at", null)
      .neq("status", "archived")
      .order("id")
      .limit(500);
    if (after !== null) query = query.gt("id", after);
    const { data, error } = await query;
    throwOn(error);
    const rows = (data ?? []) as V2CollectionSession[];
    if (!rows.length) break;
    const next = rows.at(-1)!.id;
    if (next === after) throw new Error("Collection history pagination did not advance");
    sessions.push(...rows);
    after = next;
  }
  return sessions.sort(
    (a, b) => b.created_at.localeCompare(a.created_at) || a.id.localeCompare(b.id),
  );
}

export async function createV2CollectionSession(
  workspaceId: string,
  templateId: string | null,
  title?: string,
): Promise<V2CollectionSession> {
  const { data: authData, error: authError } = await supabase.auth.getUser();
  throwOn(authError);
  const userId = authData.user?.id;
  if (!userId) throw new Error("נדרשת התחברות.");
  const defaultTitle = `איסוף ${new Intl.DateTimeFormat("he-IL", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(new Date())}`;
  const { data, error } = await db
    .from("v2_collection_sessions")
    .insert({
      workspace_id: workspaceId,
      template_id: templateId,
      title: title?.trim() || defaultTitle,
      created_by: userId,
      last_edited_by: userId,
    })
    .select("*")
    .single();
  throwOn(error);
  return data as V2CollectionSession;
}

export async function fetchV2CollectionValues(sessionId: string): Promise<V2CollectionValue[]> {
  const rows: V2CollectionValue[] = [];
  for (let offset = 0; ;) {
    const { data, error } = await db
      .rpc("v2_collection_current_values", { p_session_id: sessionId })
      .order("field_id")
      .range(offset, offset + 499);
    throwOn(error);
    rows.push(...((data ?? []) as V2CollectionValue[]));
    if (!data?.length) return rows;
    offset += data.length;
  }
}

export async function saveV2CollectionValue(
  sessionId: string,
  fieldId: string,
  value: Record<string, unknown>,
  baseline?: Record<string, unknown>,
): Promise<V2CollectionValue> {
  const scope = captureAuthWriteScope();
  const { data: authData, error: authError } = await supabase.auth.getUser();
  scope.assertCurrent();
  throwOn(authError);
  const userId = authData.user?.id;
  if (!userId) throw new Error("נדרשת התחברות.");
  const { data, error } = await db
    .rpc("v2_save_collection_value_receipt", {
      p_session_id: sessionId,
      p_field_id: fieldId,
      p_value: value,
      p_baseline: baseline ?? null,
    })
    .single();
  scope.assertCurrent();
  throwOn(error);
  const receipt = data as Partial<V2CollectionValue> | null;
  if (
    !receipt ||
    receipt.session_id !== sessionId ||
    receipt.field_id !== fieldId ||
    !worksheetSaveReceipt({ revision: receipt.updated_at, value: receipt.value })
  )
    throw new Error("לא התקבל אישור שמירה תקין. הטקסט נשאר בתיבה וניתן לנסות שוב.");
  return receipt as V2CollectionValue;
}

export async function listV2NewsSources(workspaceId: string): Promise<V2NewsSource[]> {
  const { data, error } = await db
    .from("v2_news_sources")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("name");
  throwOn(error);
  return (data ?? []) as V2NewsSource[];
}

export async function upsertV2NewsSource(input: {
  workspaceId: string;
  id?: string;
  name: string;
  currentSentiment?: V2Sentiment;
  currentScenario?: string | null;
  statusDefined?: boolean;
}): Promise<V2NewsSource> {
  const { data: authData, error: authError } = await supabase.auth.getUser();
  throwOn(authError);
  const userId = authData.user?.id;
  if (!userId) throw new Error("נדרשת התחברות.");
  const payload = {
    workspace_id: input.workspaceId,
    name: input.name.trim(),
    current_sentiment: input.currentSentiment ?? "neutral",
    current_scenario: input.currentScenario?.trim() || null,
    status_defined: input.statusDefined ?? true,
    updated_by: userId,
  };
  const query = input.id
    ? db.from("v2_news_sources").update(payload).eq("id", input.id)
    : db.from("v2_news_sources").insert(payload);
  const { data, error } = await query.select("*").single();
  throwOn(error);
  return data as V2NewsSource;
}

export async function createV2NewsItem(input: {
  workspaceId: string;
  sessionId?: string | null;
  fieldId?: string | null;
  sourceId?: string | null;
  body: string;
  importance: V2Sentiment;
  occurredAt?: string;
}): Promise<V2NewsItem> {
  const { data: authData, error: authError } = await supabase.auth.getUser();
  throwOn(authError);
  const userId = authData.user?.id;
  if (!userId) throw new Error("נדרשת התחברות.");
  const { data, error } = await db
    .from("v2_news_items")
    .insert({
      workspace_id: input.workspaceId,
      collection_session_id: input.sessionId ?? null,
      field_id: input.fieldId ?? null,
      source_id: input.sourceId ?? null,
      body: input.body.trim(),
      importance: input.importance,
      occurred_at: input.occurredAt ?? new Date().toISOString(),
      created_by: userId,
      updated_by: userId,
    })
    .select("*")
    .single();
  throwOn(error);
  return data as V2NewsItem;
}

export async function deleteV2NewsItem(id: string): Promise<void> {
  const { error } = await db.from("v2_news_items").delete().eq("id", id);
  throwOn(error);
}

export async function listV2Scenarios(
  workspaceId: string,
  assetSymbol?: string,
  includeArchived = false,
): Promise<V2Scenario[]> {
  let query = db
    .from("v2_scenarios")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("pinned", { ascending: false })
    .order("position")
    .order("created_at", { ascending: false })
    .order("id");
  if (assetSymbol) query = query.eq("asset_symbol", assetSymbol.toUpperCase());
  if (!includeArchived) query = query.neq("status", "archived");
  const { data, error } = await query;
  throwOn(error);
  return (data ?? []) as V2Scenario[];
}

export async function listV2AutomatedTradeCards(
  workspaceId: string,
): Promise<V2AutomatedTradeCard[]> {
  const cards: V2AutomatedTradeCard[] = [];
  for (let offset = 0; ;) {
    const { data, error } = await db
      .from("v2_automated_trade_cards")
      .select("workspace_id,card_id,revision,payload,observed_at,created_at,updated_at")
      .eq("workspace_id", workspaceId)
      .order("observed_at", { ascending: false })
      .order("card_id")
      .range(offset, offset + 499);
    throwOn(error);
    const page = (data ?? []) as V2AutomatedTradeCard[];
    cards.push(...page);
    if (!page.length) break;
    offset += page.length;
  }
  return cards;
}

export async function listV2Trades(
  workspaceId: string,
  includeArchived = false,
): Promise<V2Trade[]> {
  const trades: V2Trade[] = [];
  let after: string | null = null;
  for (;;) {
    let query = db
      .from("v2_trades")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("id")
      .limit(500);
    if (!includeArchived) query = query.is("archived_at", null);
    if (after) query = query.gt("id", after);
    const { data, error } = await query;
    throwOn(error);
    if (!data?.length) break;
    trades.push(...(data as V2Trade[]));
    after = String(data[data.length - 1].id);
  }
  return trades.sort(
    (a, b) => Date.parse(b.created_at) - Date.parse(a.created_at) || a.id.localeCompare(b.id),
  );
}

export type CreateV2TradeInput = {
  /** Keep this ID for every retry of one create form. */
  id?: string;
  workspaceId: string;
  templateId?: string | null;
  scenarioId?: string | null;
  assetSymbol: string;
  title?: string;
  allowCrossAssetScenario?: boolean;
  sourceKind?: "trade" | "bot_alert";
  botAlertHtml?: string;
  scenarioDraft?: {
    scenario: Partial<Scenario> & {
      id: string;
      workspace_id: string;
      title: string;
      asset_symbol: string;
    };
    values: Record<string, Record<string, unknown>>;
  };
};

export async function createV2Trade(input: CreateV2TradeInput): Promise<V2Trade> {
  const symbol = input.assetSymbol.trim().toUpperCase() || "BTC";
  const { data, error } = await db.rpc("v2_create_trade", {
    p_trade: {
      id: input.id ?? crypto.randomUUID(),
      workspace_id: input.workspaceId,
      template_id: input.templateId ?? null,
      scenario_id: input.scenarioId ?? null,
      asset_symbol: symbol,
      title: input.title?.trim() || `${symbol} · ${new Date().toLocaleDateString("he-IL")}`,
      source_kind: input.sourceKind ?? "trade",
      bot_alert_html: input.botAlertHtml ?? "",
      allow_cross_asset_scenario: input.allowCrossAssetScenario ?? false,
    },
    p_scenario: input.scenarioDraft?.scenario ?? null,
    p_scenario_values: Object.entries(input.scenarioDraft?.values ?? {}).map(
      ([field_id, value]) => ({ field_id, value }),
    ),
  });
  throwOn(error);
  return data as V2Trade;
}

export async function updateV2Trade(
  id: string,
  patch: Partial<
    Pick<
      V2Trade,
      | "title"
      | "status"
      | "pnl"
      | "activated_at"
      | "closed_at"
      | "bot_alert_html"
      | "break_even"
      | "formula_name"
    >
  >,
): Promise<V2Trade> {
  const { data: authData, error: authError } = await supabase.auth.getUser();
  throwOn(authError);
  const userId = authData.user?.id;
  if (!userId) throw new Error("נדרשת התחברות.");
  const { data, error } = await db
    .from("v2_trades")
    .update({ ...patch, updated_by: userId })
    .eq("id", id)
    .select("*")
    .single();
  throwOn(error);
  return data as V2Trade;
}

export async function exportV2Workspace(workspaceId: string): Promise<Record<string, unknown>> {
  type Row = Record<string, unknown>;
  const idsOf = (rows: Row[], key = "id") => [
    ...new Set(
      rows.flatMap((row) => (typeof row[key] === "string" && row[key] ? [row[key] as string] : [])),
    ),
  ];
  const read = async (
    table: string,
    column?: string,
    ids?: string[],
    order: string | string[] = "id",
  ) => {
    const rows: Row[] = [];
    if (ids?.length === 0) return rows;
    const chunks = ids
      ? Array.from({ length: Math.ceil(ids.length / 100) }, (_, i) =>
          ids.slice(i * 100, (i + 1) * 100),
        )
      : [undefined];
    for (const chunk of chunks) {
      rows.push(
        ...(await readAllPages<Row>((from, to) => {
          let query = db.from(table).select("*").range(from, to);
          for (const field of Array.isArray(order) ? order : [order]) query = query.order(field);
          if (column && chunk) query = query.in(column, chunk);
          return query;
        })),
      );
    }
    return rows;
  };
  const result: Record<string, unknown> = {
    format: "decision-hub-workspace-v2",
    exported_at: new Date().toISOString(),
    workspace_id: workspaceId,
    export_scope:
      "Selected workspace, accessible legacy projects, shared calendar and current-user personal state; RLS permissions are applied.",
    media: "Image metadata and storage paths are included; image binaries are not included.",
  };
  const data: Record<string, Row[]> = {};
  for (const [table, order = "id"] of [
    ["v2_workspaces"],
    ["v2_workspace_members", "user_id"],
    ["v2_templates"],
    ["v2_collection_sessions"],
    ["v2_news_sources"],
    ["v2_news_items"],
    ["v2_scenarios"],
    ["v2_trades"],
    ["v2_spot_assets"],
    ["v2_spot_transactions"],
    ["v2_spot_orders"],
    ["v2_spot_targets"],
    ["v2_asset_settings"],
    ["v2_economic_schedules"],
    ["v2_manual_entries"],
    ["v2_manual_batches", "batch_id"],
    ["v2_report_settings", "user_id"],
    ["v2_report_archives"],
    ["v2_collection_live_values"],
    ["v2_collection_scenario_links"],
    ["v2_news_pins", "item_key"],
    ["v2_workspace_preferences", "user_id"],
    ["v2_news_summary_preferences", "user_id"],
    ["v2_collection_clear_history"],
    ["v2_collection_field_activity", "version"],
    ["v2_collection_news_feedback", ["sheet_key", "logical_key", "item_key", "actor_id"]],
    ["v2_collection_news_feedback_events"],
    ["v2_collection_live_seen", ["logical_key", "user_id"]],
  ] as [string, (string | string[])?][]) {
    data[table] = await read(
      table,
      table === "v2_workspaces" ? "id" : "workspace_id",
      [workspaceId],
      order,
    );
  }
  for (const [table, parent, column] of [
    ["v2_template_sections", "v2_templates", "template_id"],
    ["v2_template_fields", "v2_template_sections", "section_id"],
    ["v2_collection_values", "v2_collection_sessions", "session_id"],
    ["v2_scenario_alternatives", "v2_scenarios", "scenario_id"],
    ["v2_scenario_values", "v2_scenarios", "scenario_id"],
    ["v2_trade_values", "v2_trades", "trade_id"],
    ["v2_spot_values", "v2_spot_assets", "asset_id"],
  ])
    data[table] = await read(table, column, idsOf(data[parent]));
  for (const [table, order] of [
    ["v2_collection_user_state", ["session_id", "user_id"]],
    ["v2_collection_field_seen", ["session_id", "field_id", "user_id"]],
    ["v2_collection_news_seen", ["session_id", "field_id", "row_id", "user_id"]],
  ] as const)
    data[table] = await read(table, "session_id", idsOf(data.v2_collection_sessions), [...order]);
  data.economic_events = (await read("economic_events")).filter(
    (row) => row.workspace_id == null || row.workspace_id === workspaceId,
  );
  data.economic_event_reminders = await read(
    "economic_event_reminders",
    "event_id",
    idsOf(data.economic_events),
  );
  data.economic_reminder_acknowledgements = await read(
    "economic_reminder_acknowledgements",
    "reminder_id",
    idsOf(data.economic_event_reminders),
    ["reminder_id", "user_id"],
  );
  data.v2_economic_reminder_preferences = [
    ...(await read("v2_economic_reminder_preferences", "event_id", idsOf(data.economic_events))),
    ...(await read(
      "v2_economic_reminder_preferences",
      "schedule_id",
      idsOf(data.v2_economic_schedules),
    )),
  ];
  data.v2_spot_target_acknowledgements = await read(
    "v2_spot_target_acknowledgements",
    "target_id",
    idsOf(data.v2_spot_targets),
    ["target_id", "user_id"],
  );
  Object.assign(result, data);
  const legacy: Record<string, Row[]> = {};
  legacy.projects = await read("projects");
  const projectIds = idsOf(legacy.projects);
  for (const table of [
    "project_answers",
    "project_trade_cycles",
    "project_images",
    "project_news_rows",
    "project_liquidity_rows",
    "trades",
    "project_members",
    "question_user_checks",
    "project_activity",
  ]) {
    legacy[table] = await read(table, "project_id", projectIds);
  }
  legacy.project_liquidity_levels = await read(
    "project_liquidity_levels",
    "row_id",
    idsOf(legacy.project_liquidity_rows),
  );
  legacy.project_user_state = await read("project_user_state", "project_id", projectIds, [
    "project_id",
    "user_id",
  ]);
  legacy.project_question_seen = await read("project_question_seen", "project_id", projectIds, [
    "project_id",
    "question_id",
    "trade_cycle_id",
    "user_id",
  ]);
  legacy.pinned_answers = await read("pinned_answers");
  legacy.user_preferences = await read("user_preferences", undefined, undefined, "user_id");
  legacy.app_settings = await read("app_settings");
  // Include editable templates too, even if no project has used them yet.
  legacy.templates = await read("templates");
  legacy.template_versions = await read(
    "template_versions",
    "template_id",
    idsOf(legacy.templates),
  );
  legacy.template_stages = await read(
    "template_stages",
    "template_version_id",
    idsOf(legacy.template_versions),
  );
  legacy.template_groups = await read("template_groups", "stage_id", idsOf(legacy.template_stages));
  legacy.template_questions = await read(
    "template_questions",
    "group_id",
    idsOf(legacy.template_groups),
  );
  const actorIds = [
    ...new Set(
      [...Object.values(data), ...Object.values(legacy)].flatMap((rows) =>
        [
          "user_id",
          "owner_id",
          "created_by",
          "updated_by",
          "last_edited_by",
          "pinned_by",
          "actor_id",
        ].flatMap((key) => idsOf(rows, key)),
      ),
    ),
  ];
  result.profiles = await read("profiles", "id", actorIds);
  result.legacy = legacy;
  result.record_counts = Object.fromEntries([
    ...Object.entries(data).map(([table, rows]) => [table, rows.length]),
    ...Object.entries(legacy).map(([table, rows]) => [`legacy.${table}`, rows.length]),
    ["profiles", (result.profiles as Row[]).length],
  ]);
  return result;
}

export function downloadJson(filename: string, payload: unknown) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}
