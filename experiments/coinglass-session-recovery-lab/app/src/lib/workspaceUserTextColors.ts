import type { SupabaseClient } from "@supabase/supabase-js";
import { supabase } from "@/integrations/supabase/client";
import { isColorClass } from "@/lib/richtext";
import { COLLECTION_AUTOFILL_METADATA_COLOR } from "@/lib/collectionAutofillDisplay";

export type WorkspaceUserTextColors = Record<string, string>;

/** Only the public display color is shared, never the user's other preferences. */
export async function fetchWorkspaceUserTextColors(
  workspaceId: string,
): Promise<WorkspaceUserTextColors> {
  const { data, error } = await (supabase as unknown as SupabaseClient).rpc(
    "v2_workspace_user_text_colors",
    { p_workspace_id: workspaceId },
  );
  if (error) throw error;
  return Object.fromEntries(
    (Array.isArray(data) ? data : []).flatMap(
      (row: { user_id?: unknown; default_text_class?: unknown }) =>
        typeof row.user_id === "string" &&
        typeof row.default_text_class === "string" &&
        isColorClass(row.default_text_class)
          ? [[row.user_id, row.default_text_class]]
          : [],
    ),
  );
}

export function workspaceUpdatedByClass(
  userId: string | null | undefined,
  automated: boolean,
  colors: WorkspaceUserTextColors = {},
): string {
  if (automated) return COLLECTION_AUTOFILL_METADATA_COLOR;
  const color = userId ? colors[userId] : undefined;
  return color && isColorClass(color) && color !== "rt-color-default"
    ? color
    : "text-muted-foreground";
}
