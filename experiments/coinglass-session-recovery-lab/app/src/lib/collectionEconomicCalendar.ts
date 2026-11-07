export const ECONOMIC_CALENDAR_SOURCE = "https://www.investing.com/economic-calendar";
export const ECONOMIC_CALENDAR_TIMEZONE = "Asia/Jerusalem";
export const ECONOMIC_CALENDAR_MAX_DAYS = 7;

export type CollectedEconomicEvent = {
  sourceId: string;
  name: string;
  eventAt: string;
  importance: 3;
  description: string;
  sourceUrl: string;
};
export type EconomicCalendarSnapshot = {
  sourceUrl: typeof ECONOMIC_CALENDAR_SOURCE;
  capturedAt: string;
  rangeStart: string;
  rangeEnd: string;
  partial: boolean;
  events: CollectedEconomicEvent[];
};

export function isEconomicCalendarSource(raw: string): boolean {
  try {
    const url = new URL(raw);
    return (
      url.protocol === "https:" &&
      !/^https:\/\/[^/?#]*:\d+(?:[/?#]|$)/i.test(raw) &&
      !url.username &&
      !url.password &&
      !url.port &&
      ["www.investing.com", "investing.com"].includes(url.hostname.toLowerCase()) &&
      /^\/economic-calendar\/?$/.test(url.pathname) &&
      !url.search &&
      !url.hash
    );
  } catch {
    return false;
  }
}

function zonedDate(instant: number): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: ECONOMIC_CALENDAR_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(instant);
  const get = (type: string) => parts.find((part) => part.type === type)?.value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

/** Resolve local midnight rather than adding 24h across Israel's DST changes. */
function midnight(date: string): number {
  const target = Date.parse(`${date}T00:00:00Z`);
  let instant = target;
  const format = new Intl.DateTimeFormat("en-CA", {
    timeZone: ECONOMIC_CALENDAR_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
  for (let i = 0; i < 3; i++) {
    const parts = format.formatToParts(instant);
    const get = (type: string) => parts.find((part) => part.type === type)?.value;
    const displayed = Date.parse(
      `${get("year")}-${get("month")}-${get("day")}T${get("hour")}:${get("minute")}:${get("second")}Z`,
    );
    instant += target - displayed;
  }
  return instant;
}

export function economicCalendarWindow(now: string | Date) {
  const instant = now instanceof Date ? now.getTime() : Date.parse(now);
  if (!Number.isFinite(instant)) throw new Error("זמן הסריקה אינו תקין.");
  const startDate = zonedDate(instant);
  const endDate = new Date(Date.parse(`${startDate}T12:00:00Z`) + 7 * 86_400_000)
    .toISOString()
    .slice(0, 10);
  return {
    startDate,
    endDate,
    rangeStart: new Date(midnight(startDate)).toISOString(),
    rangeEnd: new Date(midnight(endDate)).toISOString(),
  };
}

/** An explicit offset is mandatory; never infer a web page's local timezone. */
export function economicEventInstant(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const match =
    /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d{1,3})?)?(Z|([+-])(\d{2}):(\d{2}))$/.exec(
      raw,
    );
  if (!match) return null;
  const day = new Date(`${match[1]}T00:00:00Z`);
  if (
    !Number.isFinite(day.getTime()) ||
    day.toISOString().slice(0, 10) !== match[1] ||
    Number(match[2]) > 23 ||
    Number(match[3]) > 59 ||
    Number(match[4] ?? 0) > 59 ||
    Number(match[7] ?? 0) > 14 ||
    Number(match[8] ?? 0) > 59 ||
    (Number(match[7]) === 14 && Number(match[8]) !== 0)
  )
    return null;
  const instant = Date.parse(raw);
  return Number.isFinite(instant) ? new Date(instant).toISOString() : null;
}

export function economicEventSourceUrl(raw: unknown): string | null {
  try {
    if (typeof raw !== "string") return null;
    const url = new URL(raw, ECONOMIC_CALENDAR_SOURCE);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      !["www.investing.com", "investing.com"].includes(url.hostname.toLowerCase()) ||
      !/^\/economic-calendar\/[a-z0-9-]+-\d+\/?$/i.test(url.pathname) ||
      url.search ||
      url.hash
    )
      return null;
    return `https://www.investing.com${url.pathname.replace(/\/$/, "")}`;
  } catch {
    return null;
  }
}

export function validateEconomicCalendarSnapshot(
  snapshot: EconomicCalendarSnapshot,
  startedAt: string,
): EconomicCalendarSnapshot {
  const bounds = economicCalendarWindow(startedAt);
  if (!snapshot || typeof snapshot !== "object")
    throw new Error("לא אומתו נתוני היומן; היומן הקיים נשמר.");
  const capturedAt = economicEventInstant(snapshot.capturedAt);
  const rangeStart = economicEventInstant(snapshot.rangeStart);
  const rangeEnd = economicEventInstant(snapshot.rangeEnd);
  if (
    snapshot.sourceUrl !== ECONOMIC_CALENDAR_SOURCE ||
    typeof snapshot.partial !== "boolean" ||
    !capturedAt ||
    Math.abs(Date.parse(capturedAt) - Date.parse(startedAt)) > 10 * 60_000 ||
    rangeStart !== bounds.rangeStart ||
    !rangeEnd ||
    Date.parse(rangeEnd) > Date.parse(bounds.rangeEnd) ||
    Date.parse(rangeEnd) <= Date.parse(bounds.rangeStart) ||
    snapshot.partial !== Date.parse(rangeEnd) < Date.parse(bounds.rangeEnd) ||
    !Array.isArray(snapshot.events) ||
    snapshot.events.length > 300
  )
    throw new Error("לא אומתו טווח התאריכים ושעות היומן; היומן הקיים נשמר.");
  const seen = new Map<string, CollectedEconomicEvent>();
  for (const event of snapshot.events) {
    if (!event || typeof event !== "object")
      throw new Error("לא אומתו פרטי האירוע; היומן הקיים נשמר.");
    const eventAt = economicEventInstant(event.eventAt);
    if (
      typeof event.sourceId !== "string" ||
      !/^[1-9]\d{0,14}$/.test(event.sourceId) ||
      event.importance !== 3 ||
      !eventAt ||
      Date.parse(eventAt) < Date.parse(bounds.rangeStart) ||
      Date.parse(eventAt) >= Date.parse(snapshot.rangeEnd) ||
      typeof event.name !== "string" ||
      typeof event.description !== "string" ||
      !event.name.trim() ||
      event.name.length > 300 ||
      event.description.length > 4000 ||
      economicEventSourceUrl(event.sourceUrl) !== event.sourceUrl
    )
      throw new Error("לא אומתו חשיבות האירוע, תאריך או שעת הפרסום; היומן הקיים נשמר.");
    const normalized = { ...event, name: event.name.trim(), eventAt };
    const previous = seen.get(event.sourceId);
    if (previous && JSON.stringify(previous) !== JSON.stringify(normalized))
      throw new Error("המקור החזיר פרטים סותרים לאותו אירוע; היומן הקיים נשמר.");
    seen.set(event.sourceId, normalized);
  }
  return {
    ...snapshot,
    capturedAt,
    rangeStart,
    rangeEnd,
    events: [...seen.values()].sort((a, b) => a.eventAt.localeCompare(b.eventAt)),
  };
}

type CalendarRecord = Record<string, unknown>;
const record = (value: unknown): CalendarRecord | null =>
  value && typeof value === "object" && !Array.isArray(value) ? (value as CalendarRecord) : null;
const positiveId = (value: unknown): string | null =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? String(value) : null;

function sourceDescription(value: unknown, currency: unknown): string {
  const html = typeof value === "string" ? value : "";
  const plain = html
    .replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, "")
    .replace(/<[^>]*>/g, " ")
    .replace(
      /&(?:amp|lt|gt|quot|apos|nbsp);/g,
      (entity) =>
        ({
          "&amp;": "&",
          "&lt;": "<",
          "&gt;": ">",
          "&quot;": '"',
          "&apos;": "'",
          "&nbsp;": " ",
        })[entity] ?? entity,
    )
    .replace(/&#(x[0-9a-f]+|\d+);/gi, (_entity, number: string) => {
      const code = number.startsWith("x") ? parseInt(number.slice(1), 16) : Number(number);
      return code > 0 && code <= 0x10ffff ? String.fromCodePoint(code) : "";
    })
    .replace(/\s+/g, " ")
    .trim();
  const text = plain.length > 450 ? `${plain.slice(0, 447).trimEnd()}…` : plain;
  const unit = typeof currency === "string" && /^[A-Z]{3,4}$/.test(currency) ? currency : "";
  return [unit, text].filter(Boolean).join(" · ");
}

/** Parse native public UI responses, not its lossy markdown/star graphics. */
export function parseEconomicCalendarCapture(
  raw: unknown,
  startedAt: string,
): EconomicCalendarSnapshot {
  const capture = record(raw),
    bounds = economicCalendarWindow(startedAt);
  if (
    !capture ||
    capture.version !== 1 ||
    !isEconomicCalendarSource(String(capture.sourceUrl)) ||
    !Array.isArray(capture.responses) ||
    capture.responses.length > 16
  )
    throw new Error("לא נקראו נתוני היומן הכלכלי המקוריים; היומן הקיים נשמר.");
  const capturedAt = economicEventInstant(capture.capturedAt);
  if (!capturedAt) throw new Error("זמן קריאת היומן לא אומת; היומן הקיים נשמר.");
  const ranges: Array<{ start: number; end: number }> = [];
  const cursors = new Map<string, number>();
  const consumed = new Set<string>();
  const events = new Map<string, CollectedEconomicEvent>();
  for (const value of capture.responses) {
    const response = record(value);
    if (!response || response.status !== 200) continue;
    const query = record(response.query),
      body = record(response.body);
    if (
      response.invalid === true ||
      !query ||
      !body ||
      response.host !== "endpoints.investing.com" ||
      response.path !== "/pd-instruments/v1/calendars/economic/events/occurrences" ||
      query.domain_id !== "1" ||
      !Array.isArray(body.events) ||
      !Array.isArray(body.occurrences) ||
      body.events.length > 1000 ||
      body.occurrences.length > 1000 ||
      !("next_page_cursor" in body)
    )
      throw new Error("מבנה אירועי היומן השתנה; היומן הקיים נשמר.");
    const start = economicEventInstant(query.start_date),
      end = economicEventInstant(query.end_date);
    const limit =
      typeof query.limit === "string" && /^\d{1,4}$/.test(query.limit) ? Number(query.limit) : NaN;
    if (
      !start ||
      !end ||
      Date.parse(end) <= Date.parse(start) ||
      Date.parse(end) - Date.parse(start) > 9 * 86_400_000 ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 1000 ||
      body.occurrences.length > limit
    )
      throw new Error("טווח קריאת היומן לא אומת; היומן הקיים נשמר.");
    const rangeKey = JSON.stringify([start, end, limit, query.domain_id]);
    let coveredStart = Date.parse(start);
    if (query.cursor != null) {
      const key = `${rangeKey}:${query.cursor}`;
      const previous = cursors.get(key);
      if (typeof query.cursor !== "string" || previous === undefined || consumed.has(key))
        throw new Error("רצף מנות היומן לא אומת; היומן הקיים נשמר.");
      consumed.add(key);
      coveredStart = previous;
    }
    let coveredEnd = Date.parse(end) + 1;
    if (body.next_page_cursor !== null) {
      if (
        typeof body.next_page_cursor !== "string" ||
        body.next_page_cursor.length > 2000 ||
        !/^[A-Za-z0-9_+/=-]+$/.test(body.next_page_cursor)
      )
        throw new Error("סמן ההמשך של היומן לא אומת; היומן הקיים נשמר.");
      let cursor: CalendarRecord | null;
      try {
        cursor = record(
          JSON.parse(atob(body.next_page_cursor.replace(/-/g, "+").replace(/_/g, "/"))),
        );
      } catch {
        throw new Error("סמן ההמשך של היומן לא נקרא; היומן הקיים נשמר.");
      }
      const cursorTime = economicEventInstant(cursor?.time);
      const last = record(body.occurrences.at(-1));
      if (
        !cursorTime ||
        !last ||
        body.next_page_cursor === query.cursor ||
        positiveId(cursor?.id) !== positiveId(last.occurrence_id) ||
        cursorTime !== economicEventInstant(last.occurrence_time)
      )
        throw new Error("גבול הסריקה החלקית לא אומת; היומן הקיים נשמר.");
      // The cursor may split simultaneous releases. Exclude its entire instant.
      coveredEnd = Date.parse(cursorTime);
      cursors.set(`${rangeKey}:${body.next_page_cursor}`, coveredEnd);
    }
    if (coveredEnd < coveredStart || coveredEnd > Date.parse(end) + 1)
      throw new Error("גבול טווח היומן לא אומת; היומן הקיים נשמר.");
    ranges.push({ start: coveredStart, end: coveredEnd });
    const definitions = new Map<string, CalendarRecord>();
    for (const definition of body.events) {
      const item = record(definition),
        id = positiveId(item?.event_id);
      if (!item || !id || !["low", "medium", "high"].includes(String(item.importance)))
        throw new Error("דירוג חשיבות האירועים לא אומת; היומן הקיים נשמר.");
      if (definitions.has(id) && JSON.stringify(definitions.get(id)) !== JSON.stringify(item))
        throw new Error("המקור החזיר הגדרות אירוע סותרות; היומן הקיים נשמר.");
      definitions.set(id, item);
    }
    let previousTime = -Infinity;
    for (const occurrence of body.occurrences) {
      const row = record(occurrence),
        id = positiveId(row?.occurrence_id),
        eventId = positiveId(row?.event_id);
      const eventAt = economicEventInstant(row?.occurrence_time),
        definition = eventId ? definitions.get(eventId) : undefined;
      if (
        !row ||
        !id ||
        !eventId ||
        !eventAt ||
        !definition ||
        Date.parse(eventAt) < previousTime ||
        Date.parse(eventAt) < coveredStart ||
        Date.parse(eventAt) > Date.parse(end)
      )
        throw new Error("לא הותאמו כל האירועים לשעות ולהגדרות המקור; היומן הקיים נשמר.");
      previousTime = Date.parse(eventAt);
      if (
        definition.importance !== "high" ||
        Date.parse(eventAt) < Date.parse(bounds.rangeStart) ||
        Date.parse(eventAt) >= Date.parse(bounds.rangeEnd)
      )
        continue;
      const name =
        typeof definition.long_name === "string" && definition.long_name.trim()
          ? definition.long_name
          : definition.short_name;
      const sourceUrl = economicEventSourceUrl(definition.page_link);
      if (typeof name !== "string" || !sourceUrl || !sourceUrl.endsWith(`-${eventId}`))
        throw new Error("שם האירוע או קישור המקור לא אומתו; היומן הקיים נשמר.");
      const item: CollectedEconomicEvent = {
        sourceId: id,
        name,
        eventAt,
        importance: 3,
        description: sourceDescription(definition.description, definition.currency),
        sourceUrl,
      };
      const known = events.get(id);
      if (known && JSON.stringify(known) !== JSON.stringify(item))
        throw new Error("המקור החזיר שעות או תוכן סותרים לאותו אירוע; היומן הקיים נשמר.");
      events.set(id, item);
    }
  }
  let coveredThrough = Date.parse(bounds.rangeStart);
  for (const range of ranges.sort((a, b) => a.start - b.start)) {
    if (range.start > coveredThrough) break;
    coveredThrough = Math.max(coveredThrough, range.end);
  }
  coveredThrough = Math.min(coveredThrough, Date.parse(bounds.rangeEnd));
  if (coveredThrough <= Date.parse(bounds.rangeStart)) {
    const stage = ["not-found", "opened", "loaded", "no-response"].includes(
      String(capture.rangeSelection),
    )
      ? String(capture.rangeSelection)
      : "unknown";
    const first = ranges.length ? new Date(ranges[0].start).toISOString() : "none";
    throw new Error(
      `לא אומתה קריאה רצופה מתחילת היום בישראל; היומן הקיים נשמר. (range=${stage}, pages=${ranges.length}, first=${first})`,
    );
  }
  return validateEconomicCalendarSnapshot(
    {
      sourceUrl: ECONOMIC_CALENDAR_SOURCE,
      capturedAt,
      rangeStart: bounds.rangeStart,
      rangeEnd: new Date(coveredThrough).toISOString(),
      partial: coveredThrough < Date.parse(bounds.rangeEnd),
      events: [...events.values()].filter((event) => Date.parse(event.eventAt) < coveredThrough),
    },
    startedAt,
  );
}

/** Both copies of this shared calendar need just one paid request per batch. */
export function economicCalendarScanIdentity(
  field: { field_type: string },
  urls: string[],
): string | null {
  return field.field_type === "economic_calendar" &&
    urls.length > 0 &&
    urls.every(isEconomicCalendarSource)
    ? ECONOMIC_CALENDAR_SOURCE
    : null;
}
