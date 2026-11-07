export const INSTAGRAM_STORY_LIMIT = 64;
export const INSTAGRAM_IMAGE_LIMIT = 2 * 1024 * 1024;
export const INSTAGRAM_CAPTURE_LIMIT = 24 * 1024 * 1024;

export function instagramStoryAccount(raw: string): string | null {
  try {
    const url = new URL(raw);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      !["instagram.com", "www.instagram.com"].includes(url.hostname) ||
      url.search ||
      url.hash
    )
      return null;
    const match = url.pathname.match(/^\/([a-zA-Z0-9._]{1,30})\/?$/);
    const account = match?.[1].toLowerCase();
    return account && !["accounts", "stories", "reels", "explore", "p", "direct"].includes(account)
      ? account
      : null;
  } catch {
    return null;
  }
}

export type InstagramStorySlide = {
  id: string;
  publishedAt: string;
  url: string;
  image: string;
  isVideo: boolean;
};
export type InstagramStoryCapture = {
  version: "instagram-stories.v1";
  account: string;
  capturedAt: string;
  expectedIds: string[];
  slides: InstagramStorySlide[];
};

const record = (raw: unknown): Record<string, unknown> | null =>
  raw && typeof raw === "object" && !Array.isArray(raw) ? (raw as Record<string, unknown>) : null;
export type InstagramStoryStage =
  "session_create" | "session_close" | "login_check" | "story_capture";
export function instagramStoryFailure(
  reason: string,
  stage?: InstagramStoryStage,
  httpStatus?: number,
): Error & { code: string; reason: string; stage?: InstagramStoryStage; httpStatus?: number } {
  const messages: Record<string, string> = {
    login_required:
      "ההתחברות ל-Instagram לא זוהתה בחלון הסריקה בענן. פתחו את חלון ההתחברות מכאן, התחברו בתוכו ואשרו שוב.",
    no_stories: "לא ניתן לאמת את רשימת הסטוריז הפעילים; התוכן הקיים נשמר.",
    unsupported_video: "לא ניתן לאמת את התוכן הקולי של אחד הסטוריז; הסריקה לא נשמרה.",
    provider_unavailable: "חלון אינסטגרם אינו זמין כרגע. נסו שוב מאוחר יותר.",
    profile_busy: "חלון התחברות לאינסטגרם כבר פתוח. השלימו או סגרו אותו תחילה.",
    expired: "חלון ההתחברות פג. פתחו חלון חדש.",
    incomplete: "לא ניתן לאמת קריאה מלאה של כל הסטוריז; התוכן הקיים נשמר.",
    provider_configuration_missing:
      "חיבור שירות דפדפן הסריקה אינו מוגדר. נדרש לבדוק את הגדרות Firecrawl במערכת.",
    provider_auth_failed: "שירות דפדפן הסריקה דחה את הרשאת המערכת. נדרש לחדש את חיבור Firecrawl.",
    provider_payment_required: "שירות דפדפן הסריקה דורש יתרה או הרשאה מתאימה ב-Firecrawl.",
    provider_protocol_rejected:
      "שירות דפדפן הסריקה דחה את הבקשה. נדרש לבדוק את חיבור Firecrawl במערכת.",
    provider_rate_limited: "שירות דפדפן הסריקה הגביל את קצב הבקשות. נסו שוב מאוחר יותר.",
    provider_endpoint_unavailable:
      "פתיחת דפדפן הסריקה אינה זמינה דרך החיבור הנוכחי. נדרש לבדוק את חיבור Firecrawl.",
    provider_response_invalid: "שירות דפדפן הסריקה החזיר תשובה שלא ניתן לאמת. החלון לא נפתח.",
    provider_timeout: "שירות דפדפן הסריקה לא ענה בזמן. נסו שוב.",
    session_identity_invalid: "שירות דפדפן הסריקה לא החזיר מזהה חלון תקין. החלון לא נפתח.",
    session_expiry_invalid: "שירות דפדפן הסריקה החזיר זמן תפוגה לא תקין. החלון לא נפתח.",
    session_live_url_invalid:
      "שירות דפדפן הסריקה לא החזיר קישור התחברות בטוח ותקין. החלון לא נפתח.",
    execution_invalid: "דפדפן הסריקה החזיר תוצאה חסרה או סותרת; התוכן הקיים נשמר.",
    execution_failed: "דפדפן הסריקה לא השלים את הקריאה; התוכן הקיים נשמר.",
    execution_timeout: "הקריאה בדפדפן הסריקה לא הושלמה בזמן; התוכן הקיים נשמר.",
    inventory_unverified: "לא התקבלה רשימה מאומתת של הסטוריז הפעילים; התוכן הקיים נשמר.",
    inventory_changed: "רשימת הסטוריז השתנתה במהלך הקריאה. נסו לסרוק שוב; התוכן הקיים נשמר.",
    slide_identity: "לא ניתן לוודא שהצילום שייך לסטורי המבוקש; התוכן הקיים נשמר.",
    slide_unreadable: "לא ניתן לקרוא בבטחה את תמונת אחד הסטוריז; התוכן הקיים נשמר.",
  };
  const safeReason = Object.hasOwn(messages, reason) ? reason : "incomplete";
  const stages: Record<InstagramStoryStage, string> = {
    session_create: "פתיחת חלון Instagram",
    session_close: "סגירת חלון Instagram",
    login_check: "בדיקת ההתחברות ל-Instagram",
    story_capture: "קריאת הסטוריז",
  };
  const status =
    Number.isInteger(httpStatus) && httpStatus! >= 300 && httpStatus! <= 599
      ? httpStatus
      : undefined;
  return Object.assign(
    new Error(
      `${stage ? stages[stage] + ": " : ""}${messages[safeReason]}${status ? ` (HTTP ${status})` : ""}`,
    ),
    {
      code: `INSTAGRAM_${safeReason.toUpperCase()}`,
      reason: safeReason,
      ...(stage ? { stage } : {}),
      ...(status ? { httpStatus: status } : {}),
    },
  );
}

/** The source provides IDs/times. A model cannot create or replace them. */
export function validateInstagramStoryCapture(
  raw: unknown,
  account: string,
  startedAt: number,
): InstagramStoryCapture {
  const body = record(raw);
  const capturedAt = typeof body?.capturedAt === "string" ? Date.parse(body.capturedAt) : NaN;
  if (
    !body ||
    body.version !== "instagram-stories.v1" ||
    body.account !== account ||
    !Number.isFinite(startedAt) ||
    !Number.isFinite(capturedAt) ||
    capturedAt < startedAt ||
    capturedAt - startedAt > 180_000 ||
    !Array.isArray(body.expectedIds) ||
    body.expectedIds.length > INSTAGRAM_STORY_LIMIT ||
    body.expectedIds.some((id) => typeof id !== "string" || !/^[1-9]\d{5,24}$/.test(id)) ||
    new Set(body.expectedIds).size !== body.expectedIds.length ||
    !Array.isArray(body.slides) ||
    body.slides.length !== body.expectedIds.length
  )
    throw instagramStoryFailure("incomplete");
  const expected = new Set(body.expectedIds as string[]);
  let bytes = 0;
  const seen = new Set<string>();
  const slides = body.slides.map((rawSlide): InstagramStorySlide => {
    const slide = record(rawSlide);
    const id = slide?.id;
    const at = typeof slide?.publishedAt === "string" ? Date.parse(slide.publishedAt) : NaN;
    if (
      typeof id !== "string" ||
      !expected.has(id) ||
      seen.has(id) ||
      typeof slide?.publishedAt !== "string" ||
      !Number.isFinite(at) ||
      new Date(at).toISOString() !== slide.publishedAt ||
      at > capturedAt + 30_000 ||
      at < capturedAt - 24 * 60 * 60_000 ||
      slide.url !== `https://www.instagram.com/stories/${account}/${id}/` ||
      typeof slide.isVideo !== "boolean" ||
      typeof slide.image !== "string"
    )
      throw instagramStoryFailure("incomplete");
    // Crops originate in the browser; only actual PNG bytes may reach vision/storage.
    const match = slide.image.match(/^data:image\/png;base64,([A-Za-z0-9+/]+={0,2})$/);
    if (!match || match[1].length % 4 !== 0 || !match[1].startsWith("iVBORw0KGgo"))
      throw instagramStoryFailure("incomplete");
    const size =
      (match[1].length * 3) / 4 - (match[1].endsWith("==") ? 2 : match[1].endsWith("=") ? 1 : 0);
    bytes += size;
    if (size < 24 || size > INSTAGRAM_IMAGE_LIMIT || bytes > INSTAGRAM_CAPTURE_LIMIT)
      throw instagramStoryFailure("incomplete");
    if (slide.isVideo) throw instagramStoryFailure("unsupported_video");
    const png = Uint8Array.from(atob(match[1]), (char) => char.charCodeAt(0));
    if (png.length < 33 || String.fromCharCode(...png.slice(12, 16)) !== "IHDR")
      throw instagramStoryFailure("incomplete");
    const view = new DataView(png.buffer);
    const width = view.getUint32(16),
      height = view.getUint32(20);
    if (
      width < 240 ||
      height < 320 ||
      width > 4096 ||
      height > 4096 ||
      height / width < 1.2 ||
      height / width > 2.5
    )
      throw instagramStoryFailure("incomplete");
    seen.add(id);
    return {
      id,
      publishedAt: slide.publishedAt,
      url: slide.url as string,
      image: slide.image,
      isVideo: slide.isVideo,
    };
  });
  return {
    version: "instagram-stories.v1",
    account,
    capturedAt: new Date(capturedAt).toISOString(),
    expectedIds: [...expected],
    slides,
  };
}

export type InstagramStoryReading = {
  id: string;
  text: string;
  informative: boolean;
  audioRequired: boolean;
};

/** The exact native standalone Relay envelope, never a tray or arbitrary nested items. */
export function instagramBootstrapReelPayloads(raw: unknown): unknown[] | null {
  const obj = (value: unknown): Record<string, unknown> | null =>
    value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  const root = obj(raw);
  if (!Array.isArray(root?.require)) return [];
  if (root.require.length > 128) return null;
  const matches: unknown[] = [];
  const prefix = "adp_PolarisStoriesV3ReelPageStandaloneQueryRelayPreloader_";
  for (const outer of root.require) {
    if (!Array.isArray(outer) || outer[0] !== "ScheduledServerJS" || outer[1] !== "handle")
      continue;
    if (outer[2] !== null || !Array.isArray(outer[3]) || outer[3].length > 32) return null;
    for (const argument of outer[3]) {
      const nested = obj(obj(argument)?.__bbox)?.require;
      if (nested === undefined) continue;
      if (!Array.isArray(nested) || nested.length > 256) return null;
      for (const inner of nested) {
        if (
          !Array.isArray(inner) ||
          inner[0] !== "RelayPrefetchedStreamCache" ||
          inner[1] !== "next"
        )
          continue;
        const pair = inner[3];
        if (!Array.isArray(pair) || typeof pair[0] !== "string" || !pair[0].startsWith(prefix))
          continue;
        if (
          !pair[0].slice(prefix.length) ||
          pair.length !== 2 ||
          !Array.isArray(inner[2]) ||
          inner[2].length !== 0
        )
          return null;
        const bbox = obj(obj(pair[1])?.__bbox);
        const result = obj(bbox?.result);
        const connection = obj(obj(result?.data)?.xdt_api__v1__feed__reels_media);
        if (
          bbox?.complete !== true ||
          obj(result?.extensions)?.is_final !== true ||
          !Array.isArray(connection?.reels_media) ||
          (Array.isArray(result?.errors) ? result.errors.length !== 0 : result?.errors != null) ||
          result?.error != null
        )
          return null;
        matches.push(result);
        if (matches.length > 1) return null;
      }
    }
  }
  return matches;
}

/** Signing query parameters may change; the exact trusted CDN resource must not. */
export function instagramMediaResourceKey(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  try {
    const url = new URL(raw);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      !/(^|\.)(cdninstagram\.com|fbcdn\.net)$/.test(url.hostname) ||
      url.pathname === "/"
    )
      return null;
    return url.origin + url.pathname;
  } catch {
    return null;
  }
}

/** Only a full owner/items legacy reel or the observed native standalone result. */
export function instagramReelInventory(
  raw: unknown,
  account: string,
  now: number,
): Array<{
  id: string;
  takenAt: number;
  isVideo: boolean;
  mediaUrls: string[];
}> | null {
  const obj = (value: unknown): Record<string, unknown> | null =>
    value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  const root = obj(raw);
  if (!root) return null;
  const data = obj(root.data);
  const native = !!data && Object.hasOwn(data, "xdt_api__v1__feed__reels_media");
  const body = native ? obj(data?.xdt_api__v1__feed__reels_media) : root;
  if (!body) return null;
  const incomplete = (level: Record<string, unknown> | null) =>
    !!level &&
    (level.more_available === true ||
      level.has_more === true ||
      !!level.next_max_id ||
      level.hasNext === true ||
      obj(level.page_info)?.has_next_page === true ||
      (Array.isArray(level.errors) ? level.errors.length !== 0 : level.errors != null) ||
      level.error != null ||
      level.status === "fail");
  if (
    incomplete(root) ||
    incomplete(data) ||
    incomplete(body) ||
    (native && obj(root.extensions)?.is_final !== true) ||
    (native &&
      (!Array.isArray(body.unviewable_authors_infos) || body.unviewable_authors_infos.length !== 0))
  )
    return null;
  const reels = Array.isArray(body.reels_media)
    ? body.reels_media
    : body.reels && typeof body.reels === "object" && !Array.isArray(body.reels)
      ? Object.values(body.reels)
      : null;
  if (!reels) return null;
  const matching = reels.map(obj).filter((entry) => {
    const username = obj(entry?.user)?.username;
    return typeof username === "string" && username.toLowerCase() === account;
  });
  if (matching.length !== 1) return null;
  const reel = matching[0]!;
  const user = obj(reel.user)!;
  const ownerIdentity = (value: unknown): string | null =>
    typeof value === "string" && /^[1-9]\d{0,24}$/.test(value)
      ? value
      : typeof value === "number" && Number.isSafeInteger(value) && value > 0
        ? String(value)
        : null;
  const rawOwnerIds = [reel.id, user.pk, user.id].filter((id) => id !== undefined);
  const ownerIds = rawOwnerIds.map(ownerIdentity);
  if (
    ownerIds.some((id) => id === null) ||
    new Set(ownerIds).size > 1 ||
    (native && (ownerIds.length !== 3 || rawOwnerIds.some((id) => typeof id !== "string"))) ||
    (reel.reel_type !== undefined && reel.reel_type !== "user_reel") ||
    incomplete(reel)
  )
    return null;
  const ownerId = typeof ownerIds[0] === "string" ? ownerIds[0] : null;
  if (
    !Array.isArray(reel.items) ||
    reel.items.length > 64 ||
    (reel.media_count !== undefined && reel.media_count !== reel.items.length)
  )
    return null;
  const result: Array<{
    id: string;
    takenAt: number;
    isVideo: boolean;
    mediaUrls: string[];
  }> = [];
  const seen = new Set<string>();
  for (const rawItem of reel.items) {
    const item = obj(rawItem);
    const itemUser = obj(item?.user);
    const username = itemUser?.username;
    const identity =
      typeof item?.id === "string" ? item.id.match(/^([1-9]\d{5,24})(?:_([1-9]\d{0,24}))?$/) : null;
    const id =
      identity?.[1] ?? (item?.id === undefined && typeof item?.pk === "string" ? item.pk : null);
    const rawItemOwnerIds = [itemUser?.pk, itemUser?.id].filter((id) => id !== undefined);
    const itemOwnerIds = rawItemOwnerIds.map(ownerIdentity);
    if (
      !id ||
      !/^[1-9]\d{5,24}$/.test(id) ||
      (item?.pk !== undefined && item.pk !== id) ||
      (ownerId && identity?.[2] !== undefined && identity[2] !== ownerId) ||
      itemOwnerIds.some((value) => value === null || (!!ownerId && value !== ownerId)) ||
      (native &&
        (!identity?.[2] ||
          item?.pk !== id ||
          itemOwnerIds.length !== 2 ||
          rawItemOwnerIds.some((id) => typeof id !== "string"))) ||
      seen.has(id) ||
      typeof item?.taken_at !== "number" ||
      !Number.isSafeInteger(item.taken_at) ||
      item.taken_at * 1000 > now + 30_000 ||
      item.taken_at * 1000 < now - 24 * 60 * 60_000 ||
      ![1, 2].includes(item.media_type as number) ||
      incomplete(item) ||
      (item.expiring_at !== undefined &&
        (typeof item.expiring_at !== "number" ||
          !Number.isSafeInteger(item.expiring_at) ||
          item.expiring_at <= item.taken_at ||
          item.expiring_at * 1000 <= now)) ||
      (item.product_type !== undefined && item.product_type !== "story") ||
      (item.carousel_media != null &&
        (!Array.isArray(item.carousel_media) || item.carousel_media.length !== 0)) ||
      (username !== undefined &&
        (typeof username !== "string" || username.toLowerCase() !== account))
    )
      return null;
    const candidates = obj(item.image_versions2)?.candidates;
    const mediaUrls: string[] = [];
    if (item.media_type === 1) {
      if (!Array.isArray(candidates) || !candidates.length || candidates.length > 32) return null;
      for (const candidate of candidates) {
        const url = obj(candidate)?.url;
        if (typeof url !== "string") return null;
        try {
          const parsed = new URL(url);
          if (
            parsed.protocol !== "https:" ||
            parsed.username ||
            parsed.password ||
            parsed.port ||
            !/(^|\.)(cdninstagram\.com|fbcdn\.net)$/.test(parsed.hostname)
          )
            return null;
        } catch {
          return null;
        }
        mediaUrls.push(url);
      }
    }
    seen.add(id);
    result.push({
      id,
      takenAt: item.taken_at,
      isVideo:
        item.media_type === 2 ||
        (Array.isArray(item.video_versions) && item.video_versions.length > 0) ||
        (typeof item.video_dash_manifest === "string" && !!item.video_dash_manifest),
      mediaUrls,
    });
  }
  if (
    result.length &&
    reel.latest_reel_media !== undefined &&
    reel.latest_reel_media !== Math.max(...result.map((item) => item.takenAt))
  )
    return null;
  if (
    native &&
    !result.length &&
    reel.latest_reel_media !== undefined &&
    reel.latest_reel_media !== 0
  )
    return null;
  return result;
}
export function validateInstagramStoryReadings(
  raw: unknown,
  slides: InstagramStorySlide[],
): InstagramStoryReading[] {
  const body = record(raw);
  if (!body || !Array.isArray(body.readings) || body.readings.length !== slides.length)
    throw instagramStoryFailure("incomplete");
  const ids = new Set(slides.map((slide) => slide.id));
  const seen = new Set<string>();
  return body.readings.map((rawReading) => {
    const reading = record(rawReading);
    if (
      !reading ||
      typeof reading.id !== "string" ||
      !ids.has(reading.id) ||
      seen.has(reading.id) ||
      typeof reading.text !== "string" ||
      reading.text.length > 8_000 ||
      typeof reading.informative !== "boolean" ||
      typeof reading.audioRequired !== "boolean" ||
      (reading.informative && !reading.text.trim())
    )
      throw instagramStoryFailure("incomplete");
    if (reading.audioRequired) throw instagramStoryFailure("unsupported_video");
    seen.add(reading.id);
    return {
      id: reading.id,
      text: reading.text.trim(),
      informative: reading.informative,
      audioRequired: false,
    };
  });
}
