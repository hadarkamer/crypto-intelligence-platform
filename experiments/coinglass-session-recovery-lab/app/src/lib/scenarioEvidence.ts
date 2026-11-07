import type { CollectionSnapshotRow } from "./workspaceReadModels";
import type { V2NewsSource, V2TemplateField } from "./workspaceV1";
import { collectionSnapshotPresent, mergeCollectionSnapshots } from "./legacyCollectionHistory.ts";
import { hasUnbiasWeeklySnapshot } from "./collectionUnbiasWeekly.ts";

export type ScenarioEvidenceSentiment = "all" | "positive" | "negative";
export type ScenarioEvidenceFilters = {
  from?: number;
  sentiment: ScenarioEvidenceSentiment;
};

/** The date belongs to the originating worksheet, not a later edit to its text. */
export function scenarioEvidenceDateRange(
  range: string,
  customDays: string,
  date: string,
  now = Date.now(),
): { from?: number; error?: string } {
  if (range === "all") return {};
  if (range === "date") {
    const from = Date.parse(`${date}T00:00:00`);
    return /^\d{4}-\d{2}-\d{2}$/.test(date) && Number.isFinite(from)
      ? { from }
      : { error: "בחרו תאריך שממנו יוצג המידע." };
  }
  const raw = range === "custom" ? customDays : range;
  const days = Number(raw);
  return /^\d+$/.test(raw) && Number.isSafeInteger(days) && days > 0
    ? { from: now - days * 24 * 60 * 60 * 1000 }
    : { error: "הזינו מספר ימים שלם, החל מ־1." };
}

const positiveEvidence = new Set(["positive", "bullish", "very_bullish"]);
const negativeEvidence = new Set(["negative", "bearish", "very_bearish"]);

/** Only an item's own rating affects filtering, never its source's outlook. */
export function scenarioEvidenceMatchesSentiment(
  row: CollectionSnapshotRow,
  sentiment: ScenarioEvidenceSentiment,
): boolean {
  if (sentiment === "all") return true;
  const accepted = sentiment === "positive" ? positiveEvidence : negativeEvidence;
  const matches = (node: unknown): boolean => {
    if (Array.isArray(node)) return node.some(matches);
    if (!node || typeof node !== "object") return false;
    return Object.entries(node).some(([key, value]) => {
      // A source's current outlook must not reclassify an old news item.
      if (key === "source") return false;
      return ["importance", "sentiment", "current_sentiment"].includes(key)
        ? accepted.has(String(value))
        : matches(value);
    });
  };
  return matches(row.value) || matches(row.news_items);
}

/** Filter the full source history before collapsing logical fields. Otherwise
 * a recent opposite rating would hide an older matching box from that source. */
export function selectScenarioEvidenceRows(
  rows: CollectionSnapshotRow[],
  filters: ScenarioEvidenceFilters,
  latestOnly = false,
  sources: V2NewsSource[] = [],
): CollectionSnapshotRow[] {
  const matching = rows.filter(
    (row) =>
      collectionSnapshotPresent(row) &&
      (filters.from === undefined || Date.parse(row.session_created_at) >= filters.from) &&
      scenarioEvidenceMatchesSentiment(row, filters.sentiment),
  );
  const selected = latestOnly
    ? mergeCollectionSnapshots(matching, [])
    : matching.sort(
        (a, b) =>
          Date.parse(b.session_created_at) - Date.parse(a.session_created_at) ||
          Date.parse(b.updated_at) - Date.parse(a.updated_at) ||
          a.snapshot_key.localeCompare(b.snapshot_key),
      );
  return splitScenarioEvidenceRows(selected, sources).filter((row) =>
    scenarioEvidenceMatchesSentiment(row, filters.sentiment),
  );
}

type EvidenceSnapshot = CollectionSnapshotRow & {
  evidence_parent_key?: string;
  evidence_news_id?: string;
  evidence_source_name?: string;
  evidence_source_status?: V2NewsSource;
};
const record = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

/** Individual news keep their original row IDs and timestamps. Parent worksheet
 * metadata stays intact, while sibling news and their images are not imported. */
export function splitScenarioEvidenceRows(
  rows: CollectionSnapshotRow[],
  sources: V2NewsSource[] = [],
): CollectionSnapshotRow[] {
  return rows.flatMap((row) => {
    if ((row as EvidenceSnapshot).evidence_parent_key) return [row];
    const embedded = record(row.value.structured);
    const newsField = [
      "news_table",
      "news_source_table",
      "news_group",
      "unbiased_news",
      "unbiased",
    ].includes(row.field.field_type);
    const raw = newsField
      ? ((Array.isArray(embedded.rows) ? embedded.rows : row.value.rows) ?? [])
      : [];
    const items = new Map<string, Record<string, unknown>>();
    if (Array.isArray(raw))
      raw.forEach((entry, index) => {
        const item = record(entry);
        items.set(String(item.id ?? `legacy-position:${index}`), item);
      });
    for (const item of row.news_items ?? []) items.set(item.id, item);
    if (!items.size) return [row];
    const weekly =
      newsField && hasUnbiasWeeklySnapshot(row.value) ? row.value._autofill_consensus : null;
    const weeklyProvenance = record(row.value._autofill).kind === "weekly_consensus";
    const articles = [...items].flatMap(([id, item]) => {
      const body = String(item.body ?? item.text ?? "");
      if (
        !body
          .replace(/<[^>]*>/g, " ")
          .replace(/&nbsp;/g, " ")
          .trim() &&
        !/<img\b/i.test(body)
      )
        return [];
      const source = record(item.source);
      const name = String(
        source.name ??
          (typeof item.source === "string" ? item.source : item.source_name) ??
          row.field.title,
      );
      const status =
        sources.find(
          (candidate) =>
            candidate.name.trim().toLocaleLowerCase() === name.trim().toLocaleLowerCase(),
        ) ?? (source.name ? (source as V2NewsSource) : undefined);
      const value = { ...row.value };
      for (const key of ["text", "html", "text_value", "images", "_autofill_consensus"])
        delete value[key];
      if (weeklyProvenance) delete value._autofill;
      const individual = { ...item, id, body };
      return [
        {
          ...row,
          snapshot_key: `${row.snapshot_key}:news:${id}`,
          evidence_parent_key: evidenceSourceKey(row),
          evidence_news_id: id,
          evidence_source_name: name,
          ...(status ? { evidence_source_status: status } : {}),
          value: {
            ...value,
            rows: [individual],
            structured: { ...embedded, type: "news_table", rows: [individual] },
          },
          news_items: [],
          updated_at: String(
            item.updated_at ?? item.occurred_at ?? item.created_at ?? row.updated_at,
          ),
          updated_by: typeof item.updated_by === "string" ? item.updated_by : row.updated_by,
        } as EvidenceSnapshot,
      ];
    });
    if (!weekly) return articles;
    // The source-wide chart is a separate reference; an individual article
    // must never import it or its sibling articles as incidental evidence.
    return [
      {
        ...row,
        snapshot_key: `${row.snapshot_key}:weekly-consensus`,
        evidence_parent_key: evidenceSourceKey(row),
        evidence_source_name: `${row.field.title} · Weekly Consensus`,
        value: {
          _autofill_consensus: weekly,
          ...(weeklyProvenance ? { _autofill: row.value._autofill } : {}),
        },
        news_items: [],
        updated_at: weekly.capturedAt,
      } as EvidenceSnapshot,
      ...articles,
    ];
  });
}

export function scenarioEvidenceSource(row: CollectionSnapshotRow) {
  const snapshot = row as EvidenceSnapshot;
  const items = row.news_items?.length ? row.news_items : row.value.rows;
  const first = record(Array.isArray(items) ? items[0] : undefined);
  const source = record(first.source);
  return {
    name:
      snapshot.evidence_source_name ??
      String(
        source.name ??
          (typeof first.source === "string" ? first.source : first.source_name) ??
          row.field.title,
      ),
    status: snapshot.evidence_source_status ?? (source.name ? (source as V2NewsSource) : undefined),
  };
}

/** Old whole-source snapshots already contain each child and remain compatible. */
export function scenarioEvidenceAlreadyAdded(
  row: CollectionSnapshotRow,
  keys: ReadonlySet<string>,
) {
  return (
    keys.has(evidenceSourceKey(row)) ||
    keys.has((row as EvidenceSnapshot).evidence_parent_key ?? "")
  );
}

/** Strip template decoration only for rendering; stored snapshots stay untouched. */
export function scenarioEvidenceContentField(field: V2TemplateField): V2TemplateField {
  return {
    ...field,
    settings: {
      ...field.settings,
      source_url: null,
      links_json: [],
      links: [],
      description: null,
      help_text: null,
    },
  };
}

export type ScenarioExtraBox = { id: string; title: string; text: string };
export type ScenarioEvidence = {
  id: string;
  captured_at: string;
  source_key: string;
  snapshot: CollectionSnapshotRow;
  original_image_paths?: string[];
};
export type ScenarioAttachments = {
  extra_boxes?: ScenarioExtraBox[];
  supporting_evidence?: ScenarioEvidence[];
  invalidating_evidence?: ScenarioEvidence[];
  general_evidence?: ScenarioEvidence[];
};

export type ScenarioFieldRole = "body" | "reasons" | "invalidation";
export function scenarioRoleFields(fields: V2TemplateField[]) {
  const roles: Partial<Record<ScenarioFieldRole, V2TemplateField>> = {};
  for (const field of fields) {
    if (!field.active) continue;
    const role =
      field.settings?.scenario_role ??
      (
        {
          scenario_body: "body",
          scenario_reasons: "reasons",
          scenario_invalidation: "invalidation",
        } as Record<string, string>
      )[field.field_key];
    if (
      ["body", "reasons", "invalidation"].includes(String(role)) &&
      !roles[role as ScenarioFieldRole]
    )
      roles[role as ScenarioFieldRole] = field;
  }
  return roles;
}

export function scenarioBaseValue(value: Record<string, unknown> | undefined, text: string) {
  return {
    ...value,
    text,
    ...("text_value" in (value ?? {}) ? { text_value: text } : {}),
    ...("html" in (value ?? {}) ? { html: text } : {}),
  };
}

export function evidenceSourceKey(row: CollectionSnapshotRow) {
  return `${row.session_id}:${row.snapshot_key || `${row.sheet_key}:${row.field_id}`}`;
}

/** A deliberate copy is independent of later edits to the collection source. */
export function captureScenarioEvidence(
  rows: CollectionSnapshotRow[],
  capturedAt = new Date().toISOString(),
  id = () => crypto.randomUUID(),
): ScenarioEvidence[] {
  const seen = new Set<string>();
  return rows.flatMap((row) => {
    const source_key = evidenceSourceKey(row);
    if (seen.has(source_key)) return [];
    seen.add(source_key);
    return [{ id: id(), source_key, captured_at: capturedAt, snapshot: structuredClone(row) }];
  });
}

export function appendScenarioEvidence(current: ScenarioEvidence[], incoming: ScenarioEvidence[]) {
  const seen = new Set(current.map((item) => item.source_key));
  return [
    ...current,
    ...incoming.filter((item) => {
      if (seen.has(item.source_key)) return false;
      seen.add(item.source_key);
      return true;
    }),
  ];
}

export function toggleEvidenceSelection(current: ReadonlySet<string>, key: string): Set<string> {
  const next = new Set(current);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

export function scenarioAttachments(value: ScenarioAttachments): Required<ScenarioAttachments> {
  return {
    extra_boxes: structuredClone(value.extra_boxes ?? []),
    supporting_evidence: structuredClone(value.supporting_evidence ?? []),
    invalidating_evidence: structuredClone(value.invalidating_evidence ?? []),
    general_evidence: structuredClone(value.general_evidence ?? []),
  };
}

export function evidenceImagePaths(value: unknown): string[] {
  const found = new Set<string>();
  const visit = (node: unknown, key = "") => {
    if (typeof node === "string") {
      if (["storage_path", "original_storage_path", "image_path"].includes(key) && node)
        found.add(node);
      for (const match of node.matchAll(/data-path=["']([^"']+)["']/g)) found.add(match[1]);
    } else if (Array.isArray(node)) node.forEach((item) => visit(item));
    else if (node && typeof node === "object")
      Object.entries(node).forEach(([name, item]) => visit(item, name));
  };
  visit(value);
  return [...found];
}

export function replaceEvidenceImagePaths<T>(value: T, paths: Map<string, string>): T {
  const visit = (node: unknown, key = ""): unknown => {
    if (typeof node === "string") {
      if (["storage_path", "original_storage_path", "image_path"].includes(key))
        return paths.get(node) ?? node;
      return node.replace(
        /(data-path=["'])([^"']+)(["'])/g,
        (_, before, path, after) => `${before}${paths.get(path) ?? path}${after}`,
      );
    }
    if (Array.isArray(node)) return node.map((item) => visit(item));
    if (node && typeof node === "object")
      return Object.fromEntries(
        Object.entries(node).map(([name, item]) => [name, visit(item, name)]),
      );
    return node;
  };
  return visit(value) as T;
}
