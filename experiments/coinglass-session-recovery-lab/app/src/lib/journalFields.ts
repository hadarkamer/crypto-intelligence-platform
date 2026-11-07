import { hasMeaningfulContent } from "./content.ts";

export type JournalFieldKind = "notes" | "conclusions" | "pnl";
type JournalField = {
  field_type?: string;
  question_type?: string;
  field_key?: string | null;
  settings?: Record<string, unknown>;
};
const object = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const normalize = (value: unknown) =>
  String(value ?? "")
    .trim()
    .toLowerCase();

/** Explicit types win; exact historical semantic keys keep old templates readable.
 * The migration marker permits an intentional later change back to ordinary text. */
export function journalFieldKind(field: JournalField): JournalFieldKind | null {
  const kind = field.field_type ?? field.question_type ?? "";
  if (["notes", "conclusions", "pnl"].includes(kind)) return kind as JournalFieldKind;
  const settings = field.settings ?? {};
  if (settings.journal_type_migrated === true) return null;
  if (!["short_text", "long_text", "rich_text", "dynamic_text", "journal", "number"].includes(kind))
    return null;
  const keys = [
    field.field_key,
    settings.legacy_field_key,
    object(settings.legacy_question).field_key,
  ].map(normalize);
  if (keys.includes("trade_pnl")) return "pnl";
  if (keys.some((key) => ["notes", "journal_notes"].includes(key))) return "notes";
  if (keys.some((key) => ["conclusions", "journal_conclusions"].includes(key)))
    return "conclusions";
  return null;
}

/** Retain every filled journal field in template order instead of overwriting
 * previously found notes/conclusions with the last matching field. */
export function collectJournalText(
  fields: Array<JournalField & { id: string; position?: number }>,
  values: Array<{ field_id: string; value: unknown }>,
): { notes: string; conclusions: string } {
  const byField = new Map(values.map((row) => [row.field_id, object(row.value)]));
  const result: Record<"notes" | "conclusions", string[]> = { notes: [], conclusions: [] };
  for (const field of [...fields].sort(
    (a, b) => (a.position ?? 0) - (b.position ?? 0) || a.id.localeCompare(b.id),
  )) {
    const kind = journalFieldKind(field);
    if (kind !== "notes" && kind !== "conclusions") continue;
    const value = byField.get(field.id) ?? {};
    const text = String(value.text_value ?? value.html ?? value.text ?? "");
    if (hasMeaningfulContent(text)) result[kind].push(text);
  }
  return {
    notes: result.notes.join("<br><br>"),
    conclusions: result.conclusions.join("<br><br>"),
  };
}

/** רשומה נכנסת ליומן רק כשיש תוכן אמיתי בהערות או במסקנות. */
export function hasJournalContent(row: { notes?: string | null; conclusions?: string | null }) {
  return hasMeaningfulContent(row.notes) || hasMeaningfulContent(row.conclusions);
}

export const tradeSourceLabel = (sourceKind?: string | null) =>
  sourceKind === "bot_alert" ? "התראת בוט" : "טרייד";
