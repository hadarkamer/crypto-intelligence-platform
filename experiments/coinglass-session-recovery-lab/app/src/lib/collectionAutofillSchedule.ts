export const AUTOFILL_TIME_ZONE = "Asia/Jerusalem";
export const AUTOFILL_FIRST_HOURS = Array.from({ length: 24 }, (_, index) => (index + 6) % 24);
export type AutofillSchedule = {
  first_hour: number;
  active_hours: { enabled: boolean; start: number; end: number };
  adapt_to_publications: boolean;
};
export type PublicationWindow = { day: number; start_minute: number; end_minute: number };
export type PublicationScheduleModel = {
  version: "publication_schedule.v1";
  eligible: boolean;
  confidence: "insufficient" | "provisional" | "learned";
  first_hour: number;
  interval_hours: number;
  weekly_windows: PublicationWindow[];
  quiet_windows: PublicationWindow[];
  sample_count: number;
  distinct_days: number;
  coverage_weeks: number;
  history_start: string | null;
  history_end: string | null;
  reason: string;
};
export type PublicationObservation = {
  source_url: string;
  item_key: string;
  item_url?: string;
  published_at: string;
  basis?: "verified_scan" | "accepted_history";
};
export type PublicationCoverage = { source_url: string; from_at: string; to_at: string };
const object = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const hour = (value: unknown, fallback: number) =>
  typeof value === "number" && Number.isInteger(value) && value >= 0 && value < 24
    ? value
    : fallback;

export function autofillScheduleSettings(raw: unknown): AutofillSchedule {
  const value = object(raw),
    active = object(value.active_hours);
  return {
    first_hour: hour(value.first_hour, 6),
    active_hours: {
      enabled: active.enabled === true,
      start: hour(active.start, 8),
      end: hour(active.end, 0),
    },
    adapt_to_publications: value.adapt_to_publications === true,
  };
}

export function autofillHourLabel(value: number) {
  return `${value % 12 || 12}:00${value < 12 ? "AM" : "PM"}`;
}
const formatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: AUTOFILL_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});
function localParts(instant: number) {
  const parts = Object.fromEntries(
    formatter.formatToParts(instant).map(({ type, value }) => [type, value]),
  );
  const year = Number(parts.year),
    month = Number(parts.month),
    day = Number(parts.day);
  return {
    year,
    month,
    day,
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    second: Number(parts.second),
    dayKey: `${parts.year}-${parts.month}-${parts.day}`,
    weekday: new Date(Date.UTC(year, month - 1, day)).getUTCDay(),
  };
}
function localWallHour(wall: number): number | null {
  const offsets = new Set<number>();
  for (const delta of [-36, 0, 36]) {
    const at = wall + delta * 3600_000,
      parts = localParts(at);
    offsets.add(
      Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second) - at,
    );
  }
  const target = new Date(wall);
  const candidates = [...offsets]
    .map((offset) => wall - offset)
    .filter((candidate) => {
      const parts = localParts(candidate);
      return (
        parts.year === target.getUTCFullYear() &&
        parts.month === target.getUTCMonth() + 1 &&
        parts.day === target.getUTCDate() &&
        parts.hour === target.getUTCHours() &&
        parts.minute === 0
      );
    });
  // Skip a nonexistent spring hour. During the autumn fold, run that wall-hour
  // once at its later, standard-time occurrence, matching the database worker.
  return candidates.length ? Math.max(...candidates) : null;
}
export function autofillHourActive(schedule: AutofillSchedule, value: number) {
  const active = schedule.active_hours;
  if (!active.enabled) return true;
  if (active.start === active.end) return true;
  return active.start < active.end
    ? value >= active.start && value < active.end
    : value >= active.start || value < active.end;
}
export function publicationModelUsable(model?: PublicationScheduleModel | null) {
  return (
    !!model &&
    model.version === "publication_schedule.v1" &&
    model.eligible === true &&
    ["provisional", "learned"].includes(model.confidence) &&
    Number.isInteger(model.first_hour) &&
    model.first_hour >= 0 &&
    model.first_hour < 24 &&
    Number.isInteger(model.interval_hours) &&
    model.interval_hours >= 1 &&
    model.interval_hours <= 24 &&
    Array.isArray(model.weekly_windows) &&
    model.weekly_windows.length > 0 &&
    model.weekly_windows.length <= 168 &&
    model.weekly_windows.every(
      (window) =>
        Number.isInteger(window.day) &&
        window.day >= 0 &&
        window.day <= 6 &&
        Number.isInteger(window.start_minute) &&
        window.start_minute >= 0 &&
        Number.isInteger(window.end_minute) &&
        window.end_minute <= 1440 &&
        window.end_minute > window.start_minute,
    )
  );
}
/** Every local calendar day starts another anchor cycle. The last slot can be
 * after midnight, but the following day always restarts at the chosen hour. */
export function nextAutofillScheduleTimes(
  schedule: AutofillSchedule,
  intervalHours: number,
  after: Date | string | number,
  count = 8,
  model?: PublicationScheduleModel | null,
): string[] {
  const time = new Date(after).getTime();
  if (
    !Number.isFinite(time) ||
    !Number.isInteger(intervalHours) ||
    intervalHours < 1 ||
    intervalHours > 24
  )
    return [];
  const learned = schedule.adapt_to_publications && publicationModelUsable(model) ? model! : null;
  const first = learned?.first_hour ?? schedule.first_hour,
    interval = learned?.interval_hours ?? intervalHours;
  const today = localParts(time),
    start = Date.UTC(today.year, today.month - 1, today.day);
  const candidates = new Set<number>();
  for (let day = -1; day <= 56; day++) {
    for (let offset = 0; offset < 24; offset += interval) {
      const instant = localWallHour(start + (day * 24 + first + offset) * 3600_000);
      if (instant === null || instant <= time) continue;
      const local = localParts(instant);
      if (!autofillHourActive(schedule, local.hour)) continue;
      if (
        learned &&
        !learned.weekly_windows.some(
          (window) =>
            window.day === local.weekday &&
            local.hour * 60 >= window.start_minute &&
            local.hour * 60 < window.end_minute,
        )
      )
        continue;
      candidates.add(instant);
    }
    if (candidates.size >= count + 24) break;
  }
  return [...candidates]
    .sort((a, b) => a - b)
    .slice(0, Math.min(Math.max(count, 0), 100))
    .map((value) => new Date(value).toISOString());
}

function weekKey(parts: ReturnType<typeof localParts>) {
  return new Date(Date.UTC(parts.year, parts.month - 1, parts.day - parts.weekday))
    .toISOString()
    .slice(0, 10);
}
const fullWeek = (): PublicationWindow[] =>
  Array.from({ length: 7 }, (_, day) => ({ day, start_minute: 0, end_minute: 1440 }));
function windowsOf(cells: ReadonlySet<number>): PublicationWindow[] {
  const windows: PublicationWindow[] = [];
  for (let day = 0; day < 7; day++) {
    let start: number | null = null;
    for (let h = 0; h <= 24; h++) {
      if (h < 24 && cells.has(day * 24 + h)) {
        start ??= h;
        continue;
      }
      if (start !== null) {
        windows.push({ day, start_minute: start * 60, end_minute: h * 60 });
        start = null;
      }
    }
  }
  return windows;
}

/** Learn only from real publication dates. Positive history can suggest an
 * anchor/frequency immediately; silence suppresses hours only when all sources
 * have complete, repeated coverage of that local weekly hour. */
export function learnPublicationSchedule({
  publications,
  coverage,
  sourceUrls,
  now = new Date(),
}: {
  publications: PublicationObservation[];
  coverage: PublicationCoverage[];
  sourceUrls: string[];
  now?: Date;
}): PublicationScheduleModel {
  const end = now.getTime(),
    start = end - 56 * 86400_000,
    sources = new Set(sourceUrls);
  const seen = new Set<string>();
  const samples = publications
    .filter((item) => {
      const time = Date.parse(item.published_at),
        key = `${item.source_url}\0${item.item_url || item.item_key}`;
      if (
        !sources.has(item.source_url) ||
        !item.item_key ||
        !Number.isFinite(time) ||
        time < start ||
        time > end ||
        seen.has(key)
      )
        return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => Date.parse(a.published_at) - Date.parse(b.published_at));
  const dates = new Set(samples.map((item) => localParts(Date.parse(item.published_at)).dayKey));
  const base: PublicationScheduleModel = {
    version: "publication_schedule.v1",
    eligible: false,
    confidence: "insufficient",
    first_hour: 6,
    interval_hours: 24,
    weekly_windows: fullWeek(),
    quiet_windows: [],
    sample_count: samples.length,
    distinct_days: dates.size,
    coverage_weeks: 0,
    history_start: samples[0]?.published_at ?? null,
    history_end: samples.at(-1)?.published_at ?? null,
    reason: "אין עדיין מספיק פרסומים מאומתים. נשמר התזמון הידני.",
  };
  if (!sources.size || samples.length < 4 || dates.size < 2) return base;
  const hours = Array.from({ length: 24 }, () => 0),
    gaps: number[] = [];
  for (const url of sources) {
    const own = samples.filter((item) => item.source_url === url);
    own.forEach((item, index) => {
      const parts = localParts(Date.parse(item.published_at));
      hours[(parts.hour + (parts.minute > 0 ? 1 : 0)) % 24]++;
      if (index > 0) {
        const gap =
          (Date.parse(item.published_at) - Date.parse(own[index - 1].published_at)) / 3600_000;
        if (gap > 0) gaps.push(gap);
      }
    });
  }
  const first = AUTOFILL_FIRST_HOURS.reduce(
    (best, candidate) => (hours[candidate] > hours[best] ? candidate : best),
    6,
  );
  gaps.sort((a, b) => a - b);
  const median = gaps.length ? gaps[Math.floor(gaps.length / 2)] : 24;
  let interval = Math.max(1, Math.min(24, Math.ceil(median)));
  // One global anchor must still reach differently timed publication windows
  // on different weekdays. Reduce the cadence only as far as needed to cover
  // observed publication hours (with at most one extra hour of delay).
  for (; interval > 1; interval--) {
    const scanHours = new Set<number>();
    for (let offset = 0; offset < 24; offset += interval) scanHours.add((first + offset) % 24);
    if (hours.every((count, h) => count === 0 || scanHours.has(h) || scanHours.has((h + 1) % 24)))
      break;
  }
  const covered = new Map<string, Map<number, Set<string>>>();
  const rangesBySource = new Map<string, Array<[number, number]>>();
  for (const range of coverage) {
    if (!sources.has(range.source_url)) continue;
    const from = Math.max(start, Date.parse(range.from_at)),
      to = Math.min(end, Date.parse(range.to_at));
    if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) continue;
    const ranges = rangesBySource.get(range.source_url) ?? [];
    ranges.push([from, to]);
    rangesBySource.set(range.source_url, ranges);
  }
  for (const [url, ranges] of rangesBySource) {
    ranges.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
    const union: Array<[number, number]> = [];
    for (const range of ranges) {
      const previous = union.at(-1);
      if (previous && range[0] <= previous[1]) previous[1] = Math.max(previous[1], range[1]);
      else union.push([...range]);
    }
    // Repeated complete feed scans overlap heavily. Convert each fully covered
    // hour once per source, bounded by the 56-day history rather than scan count.
    const own = new Map<number, Set<string>>();
    for (const [from, to] of union) {
      for (let at = Math.ceil(from / 3600_000) * 3600_000; at + 3600_000 <= to; at += 3600_000) {
        const parts = localParts(at),
          cell = parts.weekday * 24 + parts.hour;
        const weeks = own.get(cell) ?? new Set<string>();
        weeks.add(weekKey(parts));
        own.set(cell, weeks);
      }
    }
    covered.set(url, own);
  }
  const verified = samples.filter((item) => item.basis !== "accepted_history");
  const sourceWeeks = new Map(
    [...sources].map((url) => [
      url,
      new Set(
        verified
          .filter((item) => item.source_url === url)
          .map((item) => weekKey(localParts(Date.parse(item.published_at)))),
      ),
    ]),
  );
  const occupied = new Set<number>();
  // An accepted historical post still proves activity at that time, even
  // though filtered history can never prove silence at another time.
  samples.forEach((item) => {
    const parts = localParts(Date.parse(item.published_at)),
      cell = parts.weekday * 24 + parts.hour;
    for (const neighbor of [-1, 0, 1, 2]) occupied.add((cell + neighbor + 168) % 168);
  });
  const quiet = new Set<number>();
  for (let cell = 0; cell < 168; cell++) {
    if (
      !occupied.has(cell) &&
      [...sources].every(
        (url) =>
          (sourceWeeks.get(url)?.size ?? 0) >= 3 && (covered.get(url)?.get(cell)?.size ?? 0) >= 3,
      )
    )
      quiet.add(cell);
  }
  const allowed = new Set(
    Array.from({ length: 168 }, (_, cell) => cell).filter((cell) => !quiet.has(cell)),
  );
  const learned = quiet.size > 0;
  return {
    ...base,
    eligible: true,
    confidence: learned ? "learned" : "provisional",
    first_hour: first,
    interval_hours: interval,
    weekly_windows: windowsOf(allowed),
    quiet_windows: windowsOf(quiet),
    coverage_weeks: Math.min(
      ...[...sources].map(
        (url) =>
          new Set([...(covered.get(url)?.values() ?? [])].flatMap((weeks) => [...weeks])).size,
      ),
    ),
    reason: learned
      ? "השעות והתדירות נלמדו מפרסומים אמיתיים; חלונות שקטים חוזרים אומתו לאורך שלושה שבועות לפחות."
      : "תזמון ראשוני לפי שעות ותדירות הפרסומים. עדיין אין דילוג על ימים או שעות ללא כיסוי היסטורי מספיק.",
  };
}
