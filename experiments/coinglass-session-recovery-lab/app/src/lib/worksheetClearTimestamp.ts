/** Postgres preserves microseconds; Date.parse alone would discard them. */
export function compareWorksheetTimestamps(
  first: string | null | undefined,
  second: string | null | undefined,
): number | null {
  if (!first || !second) return null;
  const firstMs = Date.parse(first);
  const secondMs = Date.parse(second);
  if (!Number.isFinite(firstMs) || !Number.isFinite(secondMs)) return null;
  if (firstMs !== secondMs) return firstMs > secondMs ? 1 : -1;
  const submillisecond = (value: string) => {
    const fraction = /\.(\d+)(?:Z|[+-]\d{2}:?\d{2})$/i.exec(value)?.[1] ?? "";
    return Number(fraction.padEnd(9, "0").slice(3, 9));
  };
  const firstFraction = submillisecond(first);
  const secondFraction = submillisecond(second);
  return firstFraction === secondFraction ? 0 : firstFraction > secondFraction ? 1 : -1;
}
