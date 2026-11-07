export type CollectionManagedSnapshot = {
  version: 1;
  kind: "btc_liquidity_destination" | "hyperliquid_liquidation_spikes";
  generated_html: string;
  manual_html: string;
  image_path?: string;
  chart?: unknown;
  [key: string]: unknown;
};

const record = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);

/** Human note times never fall back to the row/scan timestamp. */
export function managedSnapshotUpdate(value: unknown, segment: "manual" | "generated") {
  const snapshot = readManagedSnapshot(value);
  const source = record(value) && record(value._autofill) ? value._autofill : {};
  const raw =
    snapshot?.[`${segment}_updated_at`] ??
    (segment === "generated" ? (source.capturedAt ?? source.captured_at) : undefined);
  return {
    updatedAt: typeof raw === "string" && Number.isFinite(Date.parse(raw)) ? raw : null,
    updatedBy:
      segment === "manual" && typeof snapshot?.manual_updated_by === "string"
        ? snapshot.manual_updated_by
        : null,
  };
}

/** Only explicit managed metadata separates content. Legacy HTML is never guessed. */
export function readManagedSnapshot(value: unknown): CollectionManagedSnapshot | null {
  if (!record(value)) return null;
  const snapshot = value._autofill_snapshot;
  if (
    !record(snapshot) ||
    snapshot.version !== 1 ||
    typeof snapshot.kind !== "string" ||
    !["btc_liquidity_destination", "hyperliquid_liquidation_spikes"].includes(snapshot.kind) ||
    typeof snapshot.generated_html !== "string" ||
    typeof snapshot.manual_html !== "string" ||
    (snapshot.image_path !== undefined && typeof snapshot.image_path !== "string")
  )
    return null;
  return snapshot as CollectionManagedSnapshot;
}

/** Exact content survives, including manual colours, images and whitespace. */
export function composeManagedSnapshotHtml(snapshot: CollectionManagedSnapshot): string {
  return [snapshot.generated_html, snapshot.manual_html]
    .filter((html) => html.length > 0)
    .join("\n");
}

function scopedImagePath(path: unknown, workspaceId: string, scenarioId?: string): string | null {
  if (typeof path !== "string" || !workspaceId || /[\s/\\:%?#]/.test(workspaceId)) return null;
  const collectionPrefix = `workspace-images:${workspaceId}/collection-autofill/`;
  const scenarioPrefix =
    scenarioId && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(scenarioId)
      ? `workspace-images:${workspaceId}/scenario/${scenarioId}/`
      : null;
  const prefix = path.startsWith(collectionPrefix)
    ? collectionPrefix
    : scenarioPrefix && path.startsWith(scenarioPrefix)
      ? scenarioPrefix
      : null;
  if (!prefix) return null;
  const segments = path.slice(prefix.length).split("/");
  return segments.length >= (prefix === collectionPrefix ? 2 : 1) &&
    segments.every((part) => /^[a-zA-Z0-9._-]+$/.test(part) && part !== "." && part !== "..")
    ? path
    : null;
}

export function managedSnapshotDisplay(value: unknown, workspaceId: string, scenarioId?: string) {
  const snapshot = readManagedSnapshot(value);
  if (!snapshot) return null;
  return {
    kind: snapshot.kind,
    generatedHtml: snapshot.generated_html,
    manualHtml: snapshot.manual_html,
    imagePath:
      snapshot.kind === "hyperliquid_liquidation_spikes"
        ? scopedImagePath(snapshot.image_path, workspaceId, scenarioId)
        : null,
    chart: snapshot.chart,
  };
}

/** The caller supplies its latest value; the server also merges against the latest row. */
export function patchManagedSnapshotManual(
  value: Record<string, unknown>,
  manualHtml: string | null,
): Record<string, unknown> {
  const snapshot = readManagedSnapshot(value);
  if (!snapshot || (manualHtml !== null && typeof manualHtml !== "string"))
    throw new Error("לא ניתן לשמור את ההערות ללא עדכון אוטומטי תקין.");
  const next = { ...snapshot, manual_html: manualHtml ?? "" };
  const combined = composeManagedSnapshotHtml(next);
  return {
    ...value,
    _autofill_snapshot: next,
    text: combined,
    ...(Object.hasOwn(value, "html") ? { html: combined } : {}),
    ...(Object.hasOwn(value, "text_value") ? { text_value: combined } : {}),
  };
}
