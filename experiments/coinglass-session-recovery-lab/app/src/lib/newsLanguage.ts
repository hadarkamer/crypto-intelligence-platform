export type NewsLanguage = "he" | "en";

// These are presentation defaults for the two existing accounts, not permissions.
export const COLLECTION_ACCOUNTS = [
  { id: "2fbb3051-058a-4912-a70c-5eca850d7606", name: "pacificasnake", language: "en" },
  { id: "de1cc656-32ec-4efe-b254-3d7ab4f36871", name: "yoniori93", language: "he" },
] as const;

export function defaultNewsLanguage(
  userId: string | null,
  displayName?: string | null,
): NewsLanguage {
  const account = COLLECTION_ACCOUNTS.find((item) => item.id === userId);
  return account?.language ?? (displayName?.toLowerCase() === "pacificasnake" ? "en" : "he");
}

export function newsLanguage(value: unknown, fallback: NewsLanguage = "he"): NewsLanguage {
  return value === "en" || value === "he" ? value : fallback;
}
