import { createContext, createElement, useContext, useRef, useState, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { captureAuthWriteScope } from "@/lib/authWriteScope";
import { defaultNewsLanguage, type NewsLanguage } from "@/lib/newsLanguage";
import {
  newsLanguagePreferenceKey,
  readNewsLanguagePreference,
  saveNewsLanguagePreference,
} from "@/lib/newsLanguagePreferences";

type Preference = {
  userId: string | null;
  language: NewsLanguage;
  setLanguage: (value: NewsLanguage) => Promise<void>;
  saving: boolean;
  failed: boolean;
};
const Context = createContext<Preference>({
  userId: null,
  language: "he",
  setLanguage: async () => {},
  saving: false,
  failed: false,
});

export function NewsLanguageProvider({
  userId,
  children,
}: {
  userId: string | null;
  children: ReactNode;
}) {
  const client = useQueryClient();
  const key = newsLanguagePreferenceKey(userId);
  const state = useRef({ userId, scope: captureAuthWriteScope(), saving: false, failed: false });
  if (state.current.userId !== userId)
    state.current = { userId, scope: captureAuthWriteScope(), saving: false, failed: false };
  const active = state.current;
  const [, render] = useState(0);
  const query = useQuery({
    queryKey: key,
    queryFn: () => readNewsLanguagePreference(userId!),
    enabled: !!userId,
    staleTime: 60_000,
  });
  const language = query.data ?? defaultNewsLanguage(userId);
  const setLanguage = async (next: NewsLanguage) => {
    if (!userId || active.saving || !active.scope.isCurrent()) return;
    active.saving = true;
    active.failed = false;
    render((value) => value + 1);
    await client.cancelQueries({ queryKey: key });
    if (!active.scope.isCurrent() || state.current !== active) return;
    client.setQueryData(key, next);
    try {
      await saveNewsLanguagePreference(userId, next);
    } catch {
      if (active.scope.isCurrent() && state.current === active) {
        client.setQueryData(key, language);
        active.failed = true;
      }
    } finally {
      if (active.scope.isCurrent() && state.current === active) {
        active.saving = false;
        render((value) => value + 1);
      }
    }
  };
  return createElement(
    Context.Provider,
    {
      value: {
        userId,
        language,
        setLanguage,
        saving: active.saving,
        failed: active.failed || query.isError,
      },
    },
    children,
  );
}

export const useNewsLanguage = () => useContext(Context);
