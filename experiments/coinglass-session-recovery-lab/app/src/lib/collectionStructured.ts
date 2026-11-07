export const EXPIRY_EXCHANGES = ["דריביט", "בייננס", "בייביט"] as const;
export type ExpiryDate = {
  id: string;
  date: string;
  exchanges: Array<{ exchange: string; price: string; amount: string }>;
};
export function expiryDates(value: Record<string, unknown>): ExpiryDate[] {
  if (!Array.isArray(value.dates)) return [];
  return value.dates
    .filter((row) => row && typeof row === "object")
    .map((row, i) => ({
      id: String(row.id ?? `expiry-${i}`),
      date: String(row.date ?? ""),
      exchanges: EXPIRY_EXCHANGES.map((exchange, index) => {
        const found = Array.isArray(row.exchanges)
          ? (row.exchanges.find((entry: Record<string, unknown>) => entry.exchange === exchange) ??
            row.exchanges[index])
          : undefined;
        return { exchange, price: String(found?.price ?? ""), amount: String(found?.amount ?? "") };
      }),
    }));
}
export function structuredCollectionComplete(
  type: string,
  value: Record<string, unknown>,
): boolean | undefined {
  if (type === "correlation")
    return ["qqq", "spy"].every(
      (key) => String(value[key] ?? "").trim() !== "" && Number.isFinite(Number(value[key])),
    );
  if (type === "options_expiry")
    return expiryDates(value).some(
      (row) =>
        row.date &&
        row.exchanges.some((exchange) => exchange.price.trim() || exchange.amount.trim()),
    );
  return undefined;
}
