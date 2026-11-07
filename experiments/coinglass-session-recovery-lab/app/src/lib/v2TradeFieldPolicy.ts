import type { V2TemplateField, V2TemplateSection } from "./workspaceV1";

const POST_TRADE_TITLES = new Set([
  "יומן",
  "יומן טרייד",
  "תחקור",
  "תחקור ותוצאה",
  "תחקור וסיכום",
  "journal",
  "debrief",
]);
const canonical = (value: unknown) =>
  String(value ?? "")
    .trim()
    .toLowerCase();

/** Keep aligned with SQL v2_trade_field_is_post_trade. This is an effective
 * requirement policy, not an edit to an already-used template version. */
export function isPostTradeField(
  field: Pick<V2TemplateField, "field_type"> &
    Partial<Pick<V2TemplateField, "field_key" | "settings">>,
  section?: Pick<V2TemplateSection, "title" | "settings">,
): boolean {
  const settings = field.settings ?? {};
  const legacyQuestion = settings.legacy_question as Record<string, unknown> | undefined;
  return (
    ["pnl", "journal"].includes(field.field_type) ||
    canonical(field.field_key) === "trade_pnl" ||
    canonical(settings.legacy_field_key) === "trade_pnl" ||
    canonical(legacyQuestion?.field_key) === "trade_pnl" ||
    ["journal", "debrief", "post_trade"].includes(String(section?.settings.workflow_role ?? "")) ||
    POST_TRADE_TITLES.has(canonical(section?.title)) ||
    POST_TRADE_TITLES.has(canonical(section?.settings.legacy_stage_title))
  );
}

/** Journal/debrief and PnL are optional for the current approved phase. Other
 * template kinds and persisted required flags remain unchanged. */
export function effectiveTemplateRequirements(
  kind: string,
  sections: V2TemplateSection[],
): V2TemplateSection[] {
  if (kind !== "trade") return sections;
  return sections.map((section) => ({
    ...section,
    fields: section.fields.map((field) =>
      field.required &&
      (["info", "button"].includes(field.field_type) || isPostTradeField(field, section))
        ? { ...field, required: false }
        : field,
    ),
  }));
}
