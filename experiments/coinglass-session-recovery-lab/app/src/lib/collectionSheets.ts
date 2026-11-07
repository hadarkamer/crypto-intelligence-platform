import type { CollectionSnapshotRow } from "./workspaceReadModels";
import type { V2TemplateSection } from "./workspaceV1";

export const DEFAULT_COLLECTION_SHEETS = [
  { key: "news_macro", title: "חדשות" },
  { key: "data", title: "נזילות, טכני ונתונים" },
] as const;

/** Matches the snapshot RPC, including old four-group templates and copied sheets. */
export function collectionSheetKey(
  section: Pick<V2TemplateSection, "id" | "title" | "settings">,
): string {
  const explicit = section.settings.sheet_key;
  if (typeof explicit === "string" && explicit.trim()) return explicit;
  if (Array.isArray(section.settings.groups) || Array.isArray(section.settings.legacy_groups))
    return `sheet:${section.id}`;
  return /חדשות|מאקרו/.test(section.title) ? "news_macro" : "data";
}

export function collectionSheetTitle(
  row: Pick<CollectionSnapshotRow, "sheet_key" | "section">,
): string {
  return (
    DEFAULT_COLLECTION_SHEETS.find((sheet) => sheet.key === row.sheet_key)?.title ??
    row.section.title
  );
}

/** Keep the requested default pair and expose additional copied or custom sheets. */
export function collectionSheetOptions(
  rows: Array<Pick<CollectionSnapshotRow, "sheet_key" | "section">>,
  sections: V2TemplateSection[] = [],
): Array<{ key: string; title: string }> {
  const options = new Map<string, { key: string; title: string }>(
    DEFAULT_COLLECTION_SHEETS.map((sheet) => [sheet.key, { ...sheet }]),
  );
  for (const section of [...sections]
    .filter((item) => item.active)
    .sort((a, b) => a.position - b.position)) {
    const key = collectionSheetKey(section);
    if (!options.has(key)) options.set(key, { key, title: section.title });
  }
  for (const row of rows) {
    if (!options.has(row.sheet_key))
      options.set(row.sheet_key, { key: row.sheet_key, title: collectionSheetTitle(row) });
  }
  return [...options.values()];
}

export function collectionSnapshotGroupTitle(
  row: Pick<CollectionSnapshotRow, "field" | "section">,
): string {
  const groupId = row.field.settings.group_id ?? row.field.settings.legacy_group_id;
  const groups = row.section.settings.groups ?? row.section.settings.legacy_groups;
  const group = Array.isArray(groups)
    ? groups.find((item) => item && typeof item === "object" && item.id === groupId)
    : undefined;
  return String(group?.title ?? row.field.settings.legacy_group_title ?? row.section.title);
}
