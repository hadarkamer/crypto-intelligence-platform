import { detectCoinglassHeatmapModel } from "./coinglassModel1";
import { sourceUrls } from "./collectionAutofill";
import { isYouTubeSourceUrl } from "./collectionYouTube";

export type AutofillSerialGroup = "coinglass" | "youtube";
export type AutofillQueueField = { serial_group?: AutofillSerialGroup };

/** An incomplete lookup must never silently send a map through the parallel
 * lane. Require one well-formed metadata row for every requested field. */
export function validatedAutofillSerialGroups(
  metadata: unknown,
  fieldIds: readonly string[],
): Map<string, AutofillSerialGroup> {
  const expected = new Set(fieldIds);
  if (!Array.isArray(metadata) || metadata.length !== expected.size)
    throw new Error("Invalid autofill scheduling metadata.");
  const seen = new Set<string>();
  const serial = new Map<string, AutofillSerialGroup>();
  for (const entry of metadata) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry))
      throw new Error("Invalid autofill scheduling metadata.");
    const field = entry as Record<string, unknown>;
    if (
      typeof field.id !== "string" ||
      !expected.has(field.id) ||
      seen.has(field.id) ||
      typeof field.title !== "string" ||
      typeof field.field_type !== "string" ||
      !field.field_type.trim() ||
      !field.settings ||
      typeof field.settings !== "object" ||
      Array.isArray(field.settings)
    )
      throw new Error("Invalid autofill scheduling metadata.");
    seen.add(field.id);
    if (detectCoinglassHeatmapModel(field)) serial.set(field.id, "coinglass");
    else if (
      sourceUrls({ settings: field.settings as Record<string, unknown> }).some(isYouTubeSourceUrl)
    )
      serial.set(field.id, "youtube");
  }
  if (seen.size !== expected.size) throw new Error("Invalid autofill scheduling metadata.");
  return serial;
}

/** Serialize each constrained provider within this scheduler run. The existing
 * workflow concurrency prevents overlapping scheduled runs; independent
 * providers retain the three-request pool. */
export async function runAutofillFieldPool<T extends AutofillQueueField, R>(
  fields: readonly T[],
  fill: (field: T) => Promise<R>,
  deadline: number,
  now: () => number = Date.now,
): Promise<Array<PromiseSettledResult<R>>> {
  const pending = [...fields];
  const activeGroups = new Set<AutofillSerialGroup>();
  const results: Array<PromiseSettledResult<R>> = [];
  async function worker() {
    while (now() < deadline) {
      const index = pending.findIndex(
        (field) => !field.serial_group || !activeGroups.has(field.serial_group),
      );
      if (index < 0) return;
      const [field] = pending.splice(index, 1);
      if (field.serial_group) activeGroups.add(field.serial_group);
      try {
        results.push({ status: "fulfilled", value: await fill(field) });
      } catch (reason) {
        results.push({ status: "rejected", reason });
      } finally {
        if (field.serial_group) activeGroups.delete(field.serial_group);
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(3, fields.length) }, worker));
  return results;
}
