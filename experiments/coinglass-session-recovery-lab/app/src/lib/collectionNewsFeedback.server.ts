import type { SupabaseClient } from "@supabase/supabase-js";
import {
  plainNewsLearningText,
  type CollectionNewsLearningContext,
} from "./collectionNewsLearning";

/** Aggregates every current explicit vote in the shared workspace; the SQL
 * computes term/source statistics before bounding only the prompt examples. */
export async function readCollectionNewsLearningContext(
  db: SupabaseClient,
  workspaceId: string,
  fieldId: string,
  sourceUrl?: string,
  signal?: AbortSignal,
): Promise<CollectionNewsLearningContext> {
  signal?.throwIfAborted();
  const query = db.rpc("v2_read_collection_news_learning", {
    p_workspace_id: workspaceId,
    p_field_id: fieldId,
    p_source_url: sourceUrl ?? null,
  });
  const { data, error } = await (signal ? query.abortSignal(signal) : query);
  if (error) throw new Error("לא ניתן לטעון את המשוב השמור לבחירת החדשות.");
  if (!data || data.version !== 1 || !Array.isArray(data.examples) || !Array.isArray(data.bySource))
    throw new Error("נתוני המשוב השמור אינם תקינים.");
  const context = data as CollectionNewsLearningContext;
  return {
    ...context,
    examples: context.examples.slice(0, 24).map((example) => ({
      ...example,
      summary: plainNewsLearningText(example.summary),
      note: example.note ? plainNewsLearningText(example.note, 200) : null,
    })),
  };
}

export const getCollectionNewsLearningContext = readCollectionNewsLearningContext;
