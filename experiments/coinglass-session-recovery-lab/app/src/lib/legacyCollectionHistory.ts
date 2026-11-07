import type { SupabaseClient } from "@supabase/supabase-js";
import type { Answer, Group, Question, Stage } from "./domain";
import type { ReadOnlyImage } from "../components/ReadOnlyStage";
import type { PreviousHistoryItem } from "../components/PreviousAnswer";
import type { LiquidityRow, NewsRow, StructuredData, StructuredSnapshot } from "./structured";
import type { CollectionSnapshotRow } from "./workspaceReadModels";
import type { V2TemplateField, V2TemplateSection } from "./workspaceV1";
import { hasUnbiasWeeklySnapshot } from "./collectionUnbiasWeekly.ts";
import { capitalMarketValuePresent } from "./capitalMarketPresence.ts";

type Row = { id: string; [key: string]: unknown };
type Project = Row & {
  name: string;
  template_version_id: string;
  created_at: string;
  updated_at: string;
};
type ImageRow = ReadOnlyImage & Row & { project_id: string; trade_cycle_id?: string | null };
type AnswerRow = Answer & Row & { last_edited_by?: string | null };

export type LegacyCollectionSession = {
  id: string;
  project_id: string;
  title: string;
  created_at: string;
  updated_at: string;
  read_only: true;
  created_by?: string | null;
  origin_href: string;
  stage: Stage;
  answersById: Record<string, Answer>;
  images: ReadOnlyImage[];
  structured: StructuredData;
};

export type LegacyCollectionSnapshotRow = CollectionSnapshotRow & {
  read_only: true;
  origin_href: string;
  project_id: string;
  project_name: string;
};

export type LegacyCollectionHistory = {
  sessions: LegacyCollectionSession[];
  rows: LegacyCollectionSnapshotRow[];
};

/** Query only the caller's RLS-visible rows. Never use an admin RPC to export
 * private project content into the shared workspace. Stable paging handles
 * projects with more than the server's default 1,000 rows. */
async function readRows<T extends { id: string }>(
  db: SupabaseClient,
  table: string,
  filter?: { column: string; ids: string[] },
): Promise<T[]> {
  if (filter && !filter.ids.length) return [];
  const batches = filter
    ? Array.from({ length: Math.ceil(filter.ids.length / 100) }, (_, index) =>
        filter.ids.slice(index * 100, index * 100 + 100),
      )
    : [null];
  const result: T[] = [];
  for (const batch of batches) {
    let after: string | null = null;
    for (;;) {
      let query = db.from(table).select("*").order("id").limit(500);
      if (batch && filter) query = query.in(filter.column, batch);
      if (after !== null) query = query.gt("id", after);
      const { data, error } = await query;
      if (error) throw error;
      const rows = (data ?? []) as T[];
      if (!rows.length) break;
      const next: string = rows[rows.length - 1].id;
      if (!next || next === after)
        throw new Error("Legacy collection history pagination did not advance");
      result.push(...rows);
      after = next;
    }
  }
  return result;
}

const groupSheet = (title: string): "news_macro" | "data" | null => {
  const normalized = title.trim();
  if (/^(חדשות(?:\s+מאקרו)?|מאקרו)$/.test(normalized)) return "news_macro";
  if (
    normalized === "נזילות" ||
    /^(טכני( ונתונים| \/ נתונים)?|נתונים וטכני|טכני, נתונים)$/.test(normalized)
  )
    return "data";
  return null;
};

/** Same identity order as v2_collection_field_logical_key, including original
 * question_key across template versions. No title-based accidental merging. */
export function collectionLogicalKey(field: V2TemplateField): string {
  const settings = field.settings ?? {};
  const original = (settings.legacy_question ?? {}) as Record<string, unknown>;
  for (const candidate of [
    settings.question_key,
    settings.legacy_question_key,
    original.question_key,
    original.field_key,
    settings.legacy_field_key,
  ]) {
    if (typeof candidate === "string" && candidate.trim()) return candidate;
  }
  return field.field_key?.trim() || field.id;
}

/** Old copies retain their original question identity even after a source is
 * renamed or its generated field key changes. Titles are never identifiers. */
export function collectionHistoryFieldKeys(field: V2TemplateField): Set<string> {
  const settings = field.settings ?? {};
  const original = (settings.legacy_question ?? {}) as Record<string, unknown>;
  return new Set(
    [
      collectionLogicalKey(field),
      settings.question_key,
      settings.legacy_question_key,
      original.question_key,
      original.field_key,
      settings.legacy_field_key,
      field.field_key,
      typeof settings.legacy_question_id === "string"
        ? `question:${settings.legacy_question_id}`
        : null,
      typeof original.id === "string" ? `question:${original.id}` : null,
    ]
      .filter(
        (candidate): candidate is string => typeof candidate === "string" && !!candidate.trim(),
      )
      .map((candidate) => candidate.trim()),
  );
}

export function collectionHistoryFieldsMatch(a: V2TemplateField, b: V2TemplateField): boolean {
  if (a.id === b.id) return true;
  const other = collectionHistoryFieldKeys(b);
  return [...collectionHistoryFieldKeys(a)].some((key) => other.has(key));
}

const latestStamp = (rows: Array<Record<string, unknown>>, fallback: string) => {
  let latest = { at: fallback, by: null as string | null };
  for (const row of rows) {
    const at = String(row.updated_at ?? row.created_at ?? fallback);
    if (Date.parse(at) >= Date.parse(latest.at)) {
      latest = {
        at,
        by:
          typeof row.updated_by === "string"
            ? row.updated_by
            : typeof row.last_edited_by === "string"
              ? row.last_edited_by
              : null,
      };
    }
  }
  return latest;
};

/** Adds canonical V2 keys for the common renderer while retaining every legacy
 * typed column, attachment caption/path, normalized row and provenance value. */
function snapshotValue(
  answer: Answer | undefined,
  images: ImageRow[],
  structured: StructuredSnapshot | null,
) {
  const value: Record<string, unknown> = { ...(answer ?? {}) };
  if (answer?.text_value !== null && answer?.text_value !== undefined) {
    value.text = answer.text_value;
    value.html = answer.text_value;
  }
  if (typeof answer?.number_value === "number") value.number = answer.number_value;
  if (typeof answer?.boolean_value === "boolean") value.checked = answer.boolean_value;
  if (answer?.date_value) value.datetime = answer.date_value;
  if (Array.isArray(answer?.choice_value_json)) value.choices = answer.choice_value_json;
  if (typeof answer?.choice_value_json === "string") value.choice = answer.choice_value_json;
  if (
    answer?.choice_value_json &&
    typeof answer.choice_value_json === "object" &&
    !Array.isArray(answer.choice_value_json)
  ) {
    Object.assign(value, answer.choice_value_json);
  }
  if (images.length) value.images = images;
  if (structured) {
    const text = answer?.text_value;
    const hasSavedText =
      typeof text === "string" &&
      !!(
        /<img\b/i.test(text) ||
        text
          .replace(/<[^>]*>/g, " ")
          .replace(/&nbsp;/g, " ")
          .trim()
      );
    const snapshot =
      structured.type === "news_table" &&
      hasSavedText &&
      !structured.rows.some((row) => row.body === text)
        ? {
            ...structured,
            rows: [
              {
                id: `history-text:${answer!.id}`,
                question_id: answer!.question_id,
                body: text,
                importance: "neutral" as const,
                position: -1,
                updated_at: answer!.updated_at ?? null,
                updated_by: null,
              },
              ...structured.rows,
            ],
          }
        : structured;
    value.structured = snapshot;
    value.rows = snapshot.rows;
  }
  return value;
}

export async function readLegacyCollectionHistory(
  db: SupabaseClient,
  options: { excludeArchived?: boolean; includeInactiveNews?: boolean; projectIds?: string[] } = {},
): Promise<LegacyCollectionHistory> {
  const projects = (
    await readRows<Project>(
      db,
      "projects",
      options.projectIds ? { column: "id", ids: options.projectIds } : undefined,
    )
  ).filter(
    (project) =>
      !!project.template_version_id &&
      (!options.excludeArchived || (project.status !== "archived" && !project.archived_at)),
  );
  if (!projects.length) return { sessions: [], rows: [] };
  const versionIds = [...new Set(projects.map((project) => project.template_version_id))];
  const stages = await readRows<Stage>(db, "template_stages", {
    column: "template_version_id",
    ids: versionIds,
  });
  const stageByVersion = new Map<string, Stage>();
  for (const stage of stages
    .filter((stage) => stage.active || options.includeInactiveNews)
    .sort((a, b) => a.position - b.position || a.id.localeCompare(b.id))) {
    if (!stageByVersion.has(stage.template_version_id))
      stageByVersion.set(stage.template_version_id, stage);
  }
  const groups = (
    await readRows<Group>(db, "template_groups", {
      column: "stage_id",
      ids: [...stageByVersion.values()].map((stage) => stage.id),
    })
  ).filter(
    (group) =>
      !!groupSheet(group.title) &&
      (group.active || (options.includeInactiveNews && groupSheet(group.title) === "news_macro")),
  );
  // These projects were already omitted from the result below. Exclude them
  // before loading their answers, images and structured rows as well.
  const collectionStageIds = new Set(groups.map((group) => group.stage_id));
  const projectIds = projects
    .filter((project) =>
      collectionStageIds.has(stageByVersion.get(project.template_version_id)?.id ?? ""),
    )
    .map((project) => project.id);
  if (!projectIds.length) return { sessions: [], rows: [] };
  const [questions, answers, images, cycles, news, liquidity, levels] = await Promise.all([
    readRows<Question>(db, "template_questions", {
      column: "group_id",
      ids: groups.map((group) => group.id),
    }),
    readRows<AnswerRow>(db, "project_answers", { column: "project_id", ids: projectIds }),
    readRows<ImageRow>(db, "project_images", { column: "project_id", ids: projectIds }),
    readRows<Row & { project_id: string; cycle_number: number }>(db, "project_trade_cycles", {
      column: "project_id",
      ids: projectIds,
    }),
    readRows<NewsRow & Row & { project_id: string }>(db, "project_news_rows", {
      column: "project_id",
      ids: projectIds,
    }),
    readRows<LiquidityRow & Row & { project_id: string }>(db, "project_liquidity_rows", {
      column: "project_id",
      ids: projectIds,
    }),
    readRows<Row & { project_id: string; row_id: string; position: number }>(
      db,
      "project_liquidity_levels",
      { column: "project_id", ids: projectIds },
    ),
  ]);
  const sessions: LegacyCollectionSession[] = [];
  const rows: LegacyCollectionSnapshotRow[] = [];
  for (const project of projects) {
    const sourceStage = stageByVersion.get(project.template_version_id);
    if (!sourceStage) continue;
    const stage: Stage = {
      ...sourceStage,
      groups: groups
        .filter((group) => group.stage_id === sourceStage.id)
        .sort((a, b) => a.position - b.position || a.id.localeCompare(b.id))
        .map((group) => ({
          ...group,
          questions: questions
            .filter(
              (question) =>
                question.group_id === group.id &&
                (question.active ||
                  (options.includeInactiveNews && groupSheet(group.title) === "news_macro")),
            )
            .sort((a, b) => a.position - b.position || a.id.localeCompare(b.id)),
        })),
    };
    if (!stage.groups.length) continue;
    const primaryCycle = cycles
      .filter((cycle) => cycle.project_id === project.id)
      .sort((a, b) => a.cycle_number - b.cycle_number)[0]?.id;
    const relevantIds = new Set(
      stage.groups.flatMap((group) => group.questions.map((question) => question.id)),
    );
    const projectAnswers = answers.filter(
      (answer) =>
        answer.project_id === project.id &&
        relevantIds.has(answer.question_id) &&
        (!primaryCycle || !answer.trade_cycle_id || answer.trade_cycle_id === primaryCycle),
    );
    const answersById = Object.fromEntries(
      projectAnswers.map((answer) => [answer.question_id, answer]),
    );
    const projectImages = images.filter(
      (image) =>
        image.project_id === project.id &&
        image.question_id &&
        relevantIds.has(image.question_id) &&
        (!primaryCycle || !image.trade_cycle_id || image.trade_cycle_id === primaryCycle),
    );
    const structured: StructuredData = { news: {}, liquidity: {} };
    for (const item of news.filter(
      (row) => row.project_id === project.id && relevantIds.has(row.question_id),
    )) {
      (structured.news[item.question_id] ??= []).push(item);
    }
    for (const item of liquidity.filter(
      (row) => row.project_id === project.id && relevantIds.has(row.question_id),
    )) {
      (structured.liquidity[item.question_id] ??= []).push({
        ...item,
        levels: levels
          .filter((level) => level.row_id === item.id)
          .sort((a, b) => a.position - b.position) as unknown as LiquidityRow["levels"],
      });
    }
    for (const list of [...Object.values(structured.news), ...Object.values(structured.liquidity)])
      list.sort((a, b) => a.position - b.position);
    const session: LegacyCollectionSession = {
      id: `legacy:${project.id}`,
      project_id: project.id,
      title: project.name,
      created_at: project.created_at,
      updated_at: project.updated_at,
      ...(typeof project.user_id === "string" ? { created_by: project.user_id } : {}),
      read_only: true,
      origin_href: `/projects/${project.id}?stage=${stage.id}`,
      stage,
      answersById,
      images: projectImages,
      structured,
    };
    sessions.push(session);
    for (const group of stage.groups) {
      const sheetKey = groupSheet(group.title)!;
      const section: Omit<V2TemplateSection, "fields"> = {
        id: `legacy-group:${group.id}`,
        template_id: `legacy:${project.template_version_id}`,
        title: group.title,
        position: group.position,
        active: true,
        settings: {
          sheet_key: sheetKey,
          legacy_stage_id: stage.id,
          legacy_group_id: group.id,
          legacy_group: { ...group, questions: undefined },
          legacy_stage: { ...stage, groups: undefined },
          description: group.description,
          collapsible: group.collapsible,
        },
      };
      for (const question of group.questions) {
        const field: V2TemplateField = {
          id: `legacy-question:${question.id}`,
          section_id: section.id,
          field_key: question.question_key || question.field_key || `legacy_${question.id}`,
          title: question.title,
          field_type: question.question_type,
          position: question.position,
          required: question.required,
          default_align: question.default_text_align === "left" ? "left" : "right",
          pinned_scope: "none",
          active: question.active,
          settings: {
            legacy_question_id: question.id,
            legacy_question_key: question.question_key,
            question_key: question.question_key,
            legacy_field_key: question.field_key,
            legacy_question: question,
            description: question.description,
            help_text: question.help_text,
            source_url: question.source_url,
            links_json: question.links_json,
            options: question.options_json,
            visibility_rule_json: question.visibility_rule_json,
          },
        };
        const fieldImages = projectImages.filter((image) => image.question_id === question.id);
        const fieldNews = structured.news[question.id] ?? [];
        const fieldLiquidity = structured.liquidity[question.id] ?? [];
        const fieldStructured: StructuredSnapshot | null =
          question.question_type === "news_table" || fieldNews.length > 0
            ? { type: "news_table", rows: fieldNews }
            : question.question_type === "liquidity_table"
              ? { type: "liquidity_table", rows: fieldLiquidity }
              : null;
        const answer = answersById[question.id];
        const updated = latestStamp(
          [
            ...(answer ? [answer] : []),
            ...fieldImages,
            ...fieldNews,
            ...fieldLiquidity,
            ...fieldLiquidity.flatMap((item) => item.levels),
          ] as Record<string, unknown>[],
          project.created_at,
        );
        const logicalKey = collectionLogicalKey(field);
        rows.push({
          snapshot_key: `${sheetKey}:${logicalKey}`,
          sheet_key: sheetKey,
          logical_key: logicalKey,
          field_id: field.id,
          field,
          section,
          session_id: session.id,
          session_created_at: project.created_at,
          value: snapshotValue(answer, fieldImages, fieldStructured),
          news_items: [],
          updated_at: updated.at,
          updated_by: updated.by,
          read_only: true,
          project_id: project.id,
          project_name: project.name,
          origin_href: `/projects/${project.id}?stage=${stage.id}&group=${group.id}&question=${question.id}`,
        });
      }
    }
  }
  sessions.sort(
    (a, b) => Date.parse(b.created_at) - Date.parse(a.created_at) || a.id.localeCompare(b.id),
  );
  return { sessions, rows };
}

export function collectionSnapshotPresent(row: CollectionSnapshotRow): boolean {
  if (row.field.field_type === "capital_market") return capitalMarketValuePresent(row.value);
  if (row.news_items?.length) return true;
  const value = row.value;
  if (hasUnbiasWeeklySnapshot(value)) return true;
  const evidence =
    value.evidence ?? (value.choice_value_json as { evidence?: unknown } | null)?.evidence;
  if (
    row.field.field_type === "reasons_with_evidence" &&
    Array.isArray(evidence) &&
    evidence.some(
      (item) =>
        item &&
        typeof item === "object" &&
        typeof item.id === "string" &&
        item.id &&
        item.snapshot &&
        typeof item.snapshot === "object" &&
        !Array.isArray(item.snapshot),
    )
  )
    return true;
  if (
    value.not_relevant === true ||
    value.completion === "na" ||
    typeof value.checked === "boolean" ||
    typeof value.number === "number"
  )
    return true;
  if (Array.isArray(value.images) && value.images.length) return true;
  if (Array.isArray(value.choices) && value.choices.length) return true;
  if (
    [value.text, value.html, value.choice, value.datetime, value.date].some(
      (item) =>
        typeof item === "string" &&
        (/<img\b/i.test(item) ||
          item
            .replace(/<[^>]*>/g, " ")
            .replace(/&nbsp;/g, " ")
            .trim()),
    )
  )
    return true;
  if (Array.isArray(value.rows))
    return value.rows.some((item: Record<string, unknown>) => {
      if (String(item.body ?? item.text ?? "").trim()) return true;
      if (String(item.price ?? "").trim()) return true;
      return [item.levels, item.above, item.below].some(
        (levels) =>
          Array.isArray(levels) &&
          levels.some((level: Record<string, unknown>) =>
            String(level.price_text ?? level.price ?? "").trim(),
          ),
      );
    });
  return false;
}

/** Keeps complete values, including explicit false/zero, and uses each source
 * answer's update time instead of the enclosing project's latest edit. */
export function mergeCollectionSnapshots(
  native: CollectionSnapshotRow[],
  legacy: LegacyCollectionSnapshotRow[],
): CollectionSnapshotRow[] {
  const latest = new Map<string, CollectionSnapshotRow>();
  for (const row of [...native, ...legacy]
    .filter(collectionSnapshotPresent)
    .sort(
      (a, b) =>
        Date.parse(b.updated_at) - Date.parse(a.updated_at) ||
        Date.parse(b.session_created_at) - Date.parse(a.session_created_at),
    )) {
    const key = `${row.sheet_key}:${row.logical_key}`;
    if (!latest.has(key)) latest.set(key, row);
  }
  return [...latest.values()].sort(
    (a, b) =>
      a.sheet_key.localeCompare(b.sheet_key) ||
      a.section.position - b.section.position ||
      a.field.position - b.field.position,
  );
}

export function legacyHistoryByField(
  sections: V2TemplateSection[],
  rows: LegacyCollectionSnapshotRow[],
): Record<string, PreviousHistoryItem[]> {
  const result: Record<string, PreviousHistoryItem[]> = {};
  for (const section of sections) {
    const sheetKey = String(section.settings.sheet_key ?? "data");
    for (const field of section.fields) {
      const original = field.settings.legacy_question_id;
      const key =
        typeof original === "string"
          ? (rows.find((row) => row.field.settings.legacy_question_id === original)?.logical_key ??
            collectionLogicalKey(field))
          : collectionLogicalKey(field);
      result[field.id] = rows
        .filter(
          (row) =>
            row.sheet_key === sheetKey &&
            (row.logical_key === key || collectionHistoryFieldsMatch(field, row.field)) &&
            collectionSnapshotPresent(row),
        )
        .sort((a, b) => Date.parse(b.updated_at) - Date.parse(a.updated_at))
        .map((row) => ({
          project_id: row.project_id,
          project_name: row.project_name,
          question_id: String(row.field.settings.legacy_question_id),
          stage_id: String(row.section.settings.legacy_stage_id),
          group_id: String(row.section.settings.legacy_group_id),
          trade_cycle_id:
            typeof row.value.trade_cycle_id === "string" ? row.value.trade_cycle_id : null,
          author_id: row.updated_by,
          updated_at: row.updated_at,
          source_created_at: row.session_created_at,
          answer: row.value as unknown as Answer,
          images: (row.value.images ?? []) as PreviousHistoryItem["images"],
          structured: (row.value.structured ?? null) as StructuredSnapshot | null,
          question_type: row.field.field_type,
          origin_href: row.origin_href,
          source_snapshot: row,
        }));
    }
  }
  return result;
}
