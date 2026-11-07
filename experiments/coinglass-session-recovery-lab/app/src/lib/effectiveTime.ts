/**
 * כלל אחד לזמן האירוע ("effective time") שמשותף ליומן המרכזי ולדוח ה-PnL:
 * המוקדם מבין מועד הסגירה הראשון (או מועד הסגירה הידוע, כשאין חותמת ראשונה)
 * לבין מועד ה-PnL התקין הראשון. חותמות אלה נרשמות פעם אחת ואינן זזות בעריכות.
 * כאשר אין אף אירוע, נופלים לתאריכים אחרים שידועים בוודאות — לא ממציאים מועד.
 */

export type EffectiveTimeInput = {
  firstCloseAt?: string | null;
  firstPnlAt?: string | null;
  closedAt?: string | null;
  fallback?: (string | null | undefined)[];
};

const valid = (value: string | null | undefined): number | null => {
  if (!value) return null;
  const time = new Date(value).getTime();
  return Number.isNaN(time) ? null : time;
};

export function effectiveTime(input: EffectiveTimeInput): string {
  // מועד סגירה ידוע נחשב אירוע גם כשאין חותמת סגירה ראשונה שמורה
  const candidates = [input.firstCloseAt ?? input.closedAt, input.firstPnlAt];
  let earliest: { value: string; time: number } | null = null;
  for (const candidate of candidates) {
    const time = valid(candidate);
    if (time === null || !candidate) continue;
    if (!earliest || time < earliest.time) earliest = { value: candidate, time };
  }
  if (earliest) return earliest.value;
  const rest = [input.closedAt, ...(input.fallback ?? [])].filter(
    (value): value is string => valid(value) !== null,
  );
  return rest[0] ?? "";
}
