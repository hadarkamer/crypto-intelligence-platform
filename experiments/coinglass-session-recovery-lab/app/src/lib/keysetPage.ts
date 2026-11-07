/**
 * שליפה מלאה בעמודים לפי מפתח (keyset) על עמודת id.
 * שירות הנתונים מחזיר מספר שורות מוגבל לבקשה, ולכן דוחות שקוראים טבלאות
 * שלמות חייבים להמשיך עד עמוד ריק — גם אחרי עמוד קצר.
 */

export type PageResult<T> = { data: T[] | null; error: unknown };

export const DEFAULT_PAGE_SIZE = 500;

export async function fetchAllPages<T>(
  page: (afterId: string | null, limit: number) => PromiseLike<PageResult<T>>,
  options: { pageSize?: number; idOf?: (row: T) => string; maxPages?: number } = {},
): Promise<T[]> {
  const limit = options.pageSize ?? DEFAULT_PAGE_SIZE;
  const idOf = options.idOf ?? ((row: T) => String((row as { id?: unknown }).id ?? ""));
  const maxPages = options.maxPages ?? 200;
  const rows: T[] = [];
  let afterId: string | null = null;
  for (let index = 0; index < maxPages; index += 1) {
    const { data, error } = await page(afterId, limit);
    if (error) throw error;
    const batch = data ?? [];
    // עמוד ריק בלבד מסיים; עמוד קצר עדיין ממשיך
    if (!batch.length) return rows;
    rows.push(...batch);
    const last = idOf(batch[batch.length - 1]);
    if (!last || last === afterId) return rows;
    afterId = last;
  }
  throw new Error("טעינת הנתונים חרגה ממספר העמודים המותר");
}
