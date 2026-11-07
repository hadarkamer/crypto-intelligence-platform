import type { SupabaseClient } from "@supabase/supabase-js";
import { supabase } from "@/integrations/supabase/client";
import { captureAuthWriteScope } from "./authWriteScope";
import { defaultNewsLanguage, newsLanguage, type NewsLanguage } from "./newsLanguage";

const db = supabase as unknown as SupabaseClient;

export const newsLanguagePreferenceKey = (userId: string | null) =>
  ["news-language", userId] as const;

export async function readNewsLanguagePreference(userId: string): Promise<NewsLanguage> {
  const scope = captureAuthWriteScope();
  const { data, error } = await db
    .from("user_preferences")
    .select("collection_news_language")
    .eq("user_id", userId)
    .maybeSingle();
  scope.assertCurrent();
  if (error) throw error;
  return newsLanguage(data?.collection_news_language, defaultNewsLanguage(userId));
}

export async function saveNewsLanguagePreference(userId: string, language: NewsLanguage) {
  const scope = captureAuthWriteScope();
  const { data, error: authError } = await db.auth.getUser();
  scope.assertCurrent();
  if (authError) throw authError;
  if (data.user?.id !== userId) throw new Error("החשבון השתנה לפני שמירת השפה");
  const { error } = await db
    .from("user_preferences")
    .upsert(
      { user_id: userId, collection_news_language: newsLanguage(language) },
      { onConflict: "user_id" },
    );
  scope.assertCurrent();
  if (error) throw error;
}
