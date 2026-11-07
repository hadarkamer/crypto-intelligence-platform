/** Read beyond PostgREST's default page cap without silently truncating workspace history. */
export async function readAllPages<T>(
  page: (
    from: number,
    to: number,
  ) => PromiseLike<{ data: T[] | null; error: unknown; count?: number | null }>,
): Promise<T[]> {
  const result: T[] = [];
  for (let offset = 0; ;) {
    const { data, error, count } = await page(offset, offset + 499);
    if (error) throw error;
    const rows = data ?? [];
    if (!rows.length) return result;
    result.push(...rows);
    // Only an exact server count proves completion under a lower server cap.
    if (count != null && count >= 0 && result.length >= count) return result;
    // A server may cap responses below our requested size; a short page is not the end.
    offset += rows.length;
  }
}
