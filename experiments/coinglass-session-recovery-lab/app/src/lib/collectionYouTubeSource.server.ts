import { parseHTML } from "linkedom";
import {
  edgeSourceAddresses,
  edgeSourceTransport,
  SourceHttpError,
  SourceTransportError,
  sourceRequest,
  type SourceCursor,
  type SourceItem,
  type SourceScan,
  type SourceScanOptions,
} from "./collectionSources.server";
import {
  parseYouTubeSource,
  youtubeVideoUrl,
  YOUTUBE_CHANNEL_ID,
  YOUTUBE_VIDEO_ID,
} from "./collectionYouTube";

const CHANNEL_PAGE_MAX_BYTES = 8 * 1024 * 1024;
const CHANNEL_FEED_MAX_BYTES = 512 * 1024;
const SOURCE_BUDGET_MS = 8 * 60_000;
const FEED_SIZE = 15;
type YouTubeVideo = {
  videoId: string;
  channelId: string;
  publishedAt: string;
  sourceName: string;
};
type Player = {
  videoId: string;
  channelId: string;
  publishedAt: string;
  author: string | null;
};
const object = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;

class YouTubeSourceError extends Error {
  constructor(
    readonly reason: string,
    message: string,
  ) {
    super(message);
  }
}
function failure(reason: string, videoId?: string): YouTubeSourceError {
  console.warn("[YouTubeSource]", { reason, ...(videoId ? { videoId } : {}) });
  const messages: Record<string, string> = {
    video_access_required:
      "שירות החילוץ החזיר HTTP 403 עבור הסרטון. הסרטון לא סוכם; יתר הסרטונים נבדקים בנפרד.",
    access_required: "YouTube דורש אישור גישה או מציג חסימה. הסרטונים לא נקראו; התוכן הקיים נשמר.",
    live_incomplete:
      "אחד הסרטונים עדיין בשידור חי או ממתין לפרסום; אין תמלול מלא. התוכן הקיים נשמר.",
    transcript_too_large: "תמלול הסרטון גדול מדי לקריאה מלאה בסריקה אחת. הסרטון לא קוצר ולא סוכם.",
    transcript_unavailable:
      "התמליל המקורי של YouTube לא נטען בזמן ההמתנה. הסרטון נשאר לניסיון הבא; התוכן הקיים נשמר.",
    description_unavailable: "תיאור הסרטון לא נטען בזמן ההמתנה; הסרטון נשאר לניסיון הבא.",
    transcript_button_unavailable:
      "כפתור הצגת התמליל לא הופיע בתיאור הסרטון; הסרטון נשאר לניסיון הבא.",
    discovery_incomplete:
      "לא ניתן לוודא שרשימת הסרטונים כוללת את כל העדכונים מאז הסריקה הקודמת. התוכן הקיים נשמר.",
    source_unavailable: "לא ניתן לקרוא כעת את ערוץ YouTube. נסו שוב מאוחר יותר; התוכן הקיים נשמר.",
    source_timeout:
      "החיבור לשירות איסוף התמליל הסתיים לפני שהתקבלה תשובה; הסרטון נשאר לניסיון הבא.",
    scan_deadline: "לא נותר זמן להשלמת הסרטון בהרצה זו; הוא נשאר לסריקה הבאה.",
    channel_unavailable: "לא ניתן לקרוא את עמוד ערוץ YouTube כדי לזהות אותו. התוכן הקיים נשמר.",
    feed_unavailable: "לא ניתן לקרוא את רשימת הסרטונים של ערוץ YouTube. התוכן הקיים נשמר.",
    video_unverified: "לא אומתו זהות הסרטון ונתוני הנגן שלו; הסרטון לא סוכם.",
    publication_unverified: "מועד הפרסום בנגן אינו תואם למועד הפרסום ברשימת הערוץ; הסרטון לא סוכם.",
    player_date_unverified: "הנגן לא החזיר מועד פרסום מלא שניתן לאמת; הסרטון לא סוכם.",
  };
  return new YouTubeSourceError(
    reason,
    messages[reason] ?? "לא ניתן לאמת את הסרטון, מועד הפרסום או התמלול שלו. התוכן הקיים נשמר.",
  );
}
function sourceDate(raw: unknown): string | null {
  if (
    typeof raw !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(raw)
  )
    return null;
  const parts = raw.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/)!;
  const [year, month, day, hour, minute, second] = parts.slice(1).map(Number);
  const calendar = new Date(Date.UTC(year, month - 1, day));
  if (
    calendar.getUTCFullYear() !== year ||
    calendar.getUTCMonth() !== month - 1 ||
    calendar.getUTCDate() !== day ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  )
    return null;
  const at = Date.parse(raw);
  return Number.isFinite(at) ? new Date(at).toISOString() : null;
}
function identity(raw: unknown, videoId: string): boolean {
  if (typeof raw !== "string") return false;
  const source = parseYouTubeSource(raw);
  return source?.kind === "video" && source.videoId === videoId;
}
function text(node: Element | null): string {
  return node?.textContent?.trim() ?? "";
}
async function publicPage(url: string, maxBytes: number, stage: "channel" | "feed") {
  const accepts = (candidate: string) => {
    if (stage === "channel") {
      const expected = parseYouTubeSource(url),
        actual = parseYouTubeSource(candidate);
      return actual?.kind === "channel" && actual.url === expected?.url;
    }
    try {
      const expected = new URL(url),
        actual = new URL(candidate);
      return (
        actual.protocol === "https:" &&
        ["youtube.com", "www.youtube.com"].includes(actual.hostname) &&
        !actual.username &&
        !actual.password &&
        !actual.port &&
        !actual.hash &&
        ["/feeds/videos.xml", "/xml/feeds/videos.xml"].includes(actual.pathname) &&
        [...actual.searchParams.keys()].length === 1 &&
        actual.searchParams.getAll("channel_id").length === 1 &&
        actual.searchParams.get("channel_id") === expected.searchParams.get("channel_id")
      );
    } catch {
      return false;
    }
  };
  try {
    try {
      return await sourceRequest(url, maxBytes);
    } catch (error) {
      // A pinned Node TLS connection can fail in the hosted runtime even when
      // its public fetch boundary works. Only this typed failure on the same
      // verified YouTube identity may use that boundary; do not relax TLS or
      // retry HTTP, DNS, size, timeout, login or unrelated redirect failures.
      if (
        !(error instanceof SourceTransportError) ||
        error.kind !== "tls_hostname_mismatch" ||
        !accepts(error.url)
      )
        throw error;
      return await edgeSourceTransport(edgeSourceAddresses, accepts)(url, maxBytes);
    }
  } catch (error) {
    const unavailable = failure(stage === "channel" ? "channel_unavailable" : "feed_unavailable");
    // Keep an actual HTTP failure distinct from an unclassified transport
    // failure. Never include the terminal URL, headers or native error text.
    if (
      error instanceof SourceHttpError &&
      Number.isInteger(error.status) &&
      error.status >= 400 &&
      error.status <= 599
    ) {
      console.warn("[YouTubeSource]", { stage, status: error.status });
      throw new YouTubeSourceError(
        unavailable.reason,
        `${unavailable.message} המקור החזיר HTTP ${error.status}.`,
      );
    }
    throw unavailable;
  }
}

export function resolveYouTubeChannelId(html: string, expectedUrl: string): string {
  const { document } = parseHTML(html);
  const ids = new Set<string>();
  for (const meta of document.querySelectorAll('meta[itemprop="channelId"]')) {
    const id = meta.getAttribute("content");
    if (id && YOUTUBE_CHANNEL_ID.test(id)) ids.add(id);
  }
  for (const link of document.querySelectorAll('link[type="application/rss+xml"]')) {
    const href = link.getAttribute("href");
    if (!href) continue;
    try {
      const feed = new URL(href, expectedUrl);
      if (
        feed.protocol !== "https:" ||
        feed.hostname !== "www.youtube.com" ||
        feed.username ||
        feed.password ||
        feed.port ||
        !["/feeds/videos.xml", "/xml/feeds/videos.xml"].includes(feed.pathname) ||
        feed.searchParams.getAll("channel_id").length !== 1 ||
        [...feed.searchParams.keys()].some((key) => key !== "channel_id")
      )
        continue;
      const id = feed.searchParams.get("channel_id")!;
      if (YOUTUBE_CHANNEL_ID.test(id)) ids.add(id);
    } catch {
      /* No arbitrary feed URLs are followed. */
    }
  }
  if (ids.size !== 1) throw failure("channel_unverified");
  return [...ids][0];
}

/** Atom publication dates are independent of titles and transcript availability. */
export function parseYouTubeFeed(xml: string, channelId: string): YouTubeVideo[] {
  if (
    !YOUTUBE_CHANNEL_ID.test(channelId) ||
    /<!DOCTYPE|<!ENTITY/i.test(xml) ||
    !xml.trim().endsWith("</feed>")
  )
    throw failure("feed_unverified");
  const { document } = parseHTML(xml);
  const feed = document.querySelector("feed");
  if (
    !feed ||
    feed.getAttribute("xmlns") !== "http://www.w3.org/2005/Atom" ||
    feed.getAttribute("xmlns:yt") !== "http://www.youtube.com/xml/schemas/2015"
  )
    throw failure("feed_unverified");
  const ownChannel = Array.from(feed.children).find(
    (node) => node.tagName.toLowerCase() === "yt:channelid",
  );
  const feedAlternate = Array.from(feed.children).find(
    (node) => node.tagName.toLowerCase() === "link" && node.getAttribute("rel") === "alternate",
  );
  const alternateSource = parseYouTubeSource(feedAlternate?.getAttribute("href") ?? "");
  const authorSource = parseYouTubeSource(text(feed.querySelector(":scope > author > uri")));
  const self = Array.from(feed.children).find(
    (node) => node.tagName.toLowerCase() === "link" && node.getAttribute("rel") === "self",
  );
  let selfId: string | null = null;
  try {
    const url = new URL(self?.getAttribute("href") ?? "");
    if (
      ["www.youtube.com", "youtube.com"].includes(url.hostname) &&
      ["/feeds/videos.xml", "/xml/feeds/videos.xml"].includes(url.pathname) &&
      !url.username &&
      !url.password &&
      !url.port &&
      url.searchParams.getAll("channel_id").length === 1
    )
      selfId = url.searchParams.get("channel_id");
  } catch {
    /* Reject below. */
  }
  const ownId = text(ownChannel ?? null);
  // The observed public Atom feed omits UC only at its top level. Entry IDs
  // remain full UC IDs; require three independent own-channel links for this variant.
  const ownMatches =
    ownId === channelId ||
    (ownId === channelId.slice(2) &&
      selfId === channelId &&
      alternateSource?.kind === "channel" &&
      alternateSource.channelId === channelId &&
      authorSource?.kind === "channel" &&
      authorSource.channelId === channelId);
  if (!ownMatches && selfId !== channelId) throw failure("feed_unverified");
  if ((ownChannel && !ownMatches) || (selfId !== null && selfId !== channelId))
    throw failure("feed_unverified");
  const entries = Array.from(feed.children).filter(
    (node) => node.tagName.toLowerCase() === "entry",
  );
  if (entries.length > FEED_SIZE) throw failure("discovery_incomplete");
  const seen = new Set<string>();
  const result = entries.map((entry) => {
    const direct = (tag: string) =>
      Array.from(entry.children).find((node) => node.tagName.toLowerCase() === tag) ?? null;
    const videoId = text(direct("yt:videoid"));
    const owner = text(direct("yt:channelid"));
    const id = text(direct("id"));
    const publishedAt = sourceDate(text(direct("published")));
    const sourceName = text(entry.querySelector("author name"));
    const link = Array.from(entry.children).find(
      (node) => node.tagName.toLowerCase() === "link" && node.getAttribute("rel") === "alternate",
    );
    const href = link
      ?.getAttribute("href")
      ?.replace(/^http:\/\/((?:www\.)?youtube\.com)\//, "https://$1/");
    if (
      !YOUTUBE_VIDEO_ID.test(videoId) ||
      owner !== channelId ||
      id !== `yt:video:${videoId}` ||
      !publishedAt ||
      !sourceName ||
      sourceName.length > 500 ||
      !identity(href, videoId) ||
      seen.has(videoId)
    )
      throw failure("feed_unverified", videoId);
    seen.add(videoId);
    return { videoId, channelId, publishedAt, sourceName };
  });
  return result.sort(
    (a, b) => a.publishedAt.localeCompare(b.publishedAt) || a.videoId.localeCompare(b.videoId),
  );
}

function assignedJson(script: string): unknown | null {
  const match = /\bytInitialPlayerResponse\s*=\s*\{/.exec(script);
  if (!match) return null;
  const start = script.indexOf("{", match.index);
  let depth = 0,
    string = false,
    escaped = false;
  for (let i = start; i < script.length; i++) {
    const char = script[i];
    if (string) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') string = false;
      continue;
    }
    if (char === '"') {
      string = true;
      continue;
    }
    if (char === "{") depth++;
    else if (char === "}" && --depth === 0) {
      try {
        return JSON.parse(script.slice(start, i + 1));
      } catch {
        return null;
      }
    }
  }
  return null;
}
export function parseYouTubePlayer(html: string, videoId: string): Player {
  const { document } = parseHTML(html);
  const candidates = Array.from(document.querySelectorAll("script"))
    .map((node) => object(assignedJson(node.textContent ?? "")))
    .filter((value): value is Record<string, unknown> => !!value);
  const matches = candidates.filter((player) => object(player.videoDetails)?.videoId === videoId);
  if (matches.length !== 1) throw failure("video_unverified", videoId);
  const player = matches[0],
    details = object(player.videoDetails)!;
  const playback = object(player.playabilityStatus);
  const micro = object(object(player.microformat)?.playerMicroformatRenderer);
  if (
    details.isUpcoming === true ||
    details.isLive === true ||
    object(micro?.liveBroadcastDetails)?.isLiveNow === true
  )
    throw failure("live_incomplete", videoId);
  if (playback?.status !== "OK")
    throw failure(
      playback?.status === "LOGIN_REQUIRED" ? "access_required" : "video_unverified",
      videoId,
    );
  const channelId = details.channelId,
    publishedAt = sourceDate(micro?.publishDate);
  if (!publishedAt) throw failure("player_date_unverified", videoId);
  if (typeof channelId !== "string" || !YOUTUBE_CHANNEL_ID.test(channelId))
    throw failure("video_unverified", videoId);
  return {
    videoId,
    channelId,
    publishedAt,
    author:
      typeof details.author === "string" && details.author.trim() ? details.author.trim() : null,
  };
}

/** Read the text returned from Show transcript in the same page request. */
function readTranscript(data: Record<string, unknown>, video: YouTubeVideo): string {
  const meta = object(data.metadata);
  if (
    !meta ||
    !identity(meta.sourceURL, video.videoId) ||
    (meta.url !== undefined && !identity(meta.url, video.videoId)) ||
    (meta.identifier !== undefined && meta.identifier !== video.videoId) ||
    meta.statusCode !== 200 ||
    meta.error
  )
    throw failure("video_unverified", video.videoId);
  for (const key of ["ogUrl", "og:url"]) {
    if (meta[key] !== undefined && !identity(meta[key], video.videoId))
      throw failure("video_unverified", video.videoId);
  }
  const returns = object(data.actions)?.javascriptReturns;
  const transcript = Array.isArray(returns) ? object(returns[0])?.value : null;
  if (meta.isLive === true || transcript === "YOUTUBE_WAIT:live_incomplete")
    throw failure("live_incomplete", video.videoId);
  if (meta.datePublished !== undefined && sourceDate(meta.datePublished) !== video.publishedAt)
    throw failure("publication_unverified", video.videoId);
  for (const stage of [
    "description_unavailable",
    "transcript_button_unavailable",
    "transcript_unavailable",
  ])
    if (transcript === `YOUTUBE_ERROR:${stage}`) throw failure(stage, video.videoId);
  if (typeof transcript !== "string" || !transcript.trim())
    throw failure("transcript_unavailable", video.videoId);
  if (transcript.length > 200_000) throw failure("transcript_too_large", video.videoId);
  return transcript.trim();
}

async function scrapeVideo(
  videoId: string,
  remaining: number,
  transcript = true,
): Promise<Record<string, unknown>> {
  const key = process.env.FIRECRAWL_DIRECT_API_KEY || process.env.FIRECRAWL_API_KEY,
    direct = key?.startsWith("fc-"),
    gateway = process.env.LOVABLE_API_KEY;
  if (!key || (!direct && !gateway)) throw failure("source_unavailable", videoId);
  const deadline = Date.now() + Math.min(120_000, remaining);
  let response: Response;
  for (let attempt = 0; ; attempt++) {
    // Leave room below the observed gateway cutoff, and share one budget across both attempts.
    const timeoutMs = Math.min(direct ? 120_000 : 55_000, deadline - Date.now());
    if (timeoutMs <= 20_000) throw failure("scan_deadline", videoId);
    try {
      response = await fetch(
        direct
          ? "https://api.firecrawl.dev/v2/scrape"
          : "https://connector-gateway.lovable.dev/firecrawl/v2/scrape",
        {
          method: "POST",
          redirect: "manual",
          signal: AbortSignal.timeout(timeoutMs),
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${direct ? key : gateway}`,
            ...(!direct ? { "X-Connection-Api-Key": key } : {}),
          },
          body: JSON.stringify({
            url: youtubeVideoUrl(videoId),
            formats: ["rawHtml"],
            maxAge: 0,
            onlyMainContent: false,
            skipTlsVerification: false,
            timeout: Math.min(110_000, timeoutMs - 5_000),
            location: { languages: ["he"] },
            ...(transcript
              ? {
                  actions: [
                    {
                      type: "executeJavascript",
                      // Wait inside the opened page, not before making the request.
                      // Both stages share the caller's remaining collection budget.
                      script: `(async () => {
                      const deadline = Date.now() + ${Math.min(90_000, timeoutMs - 15_000)};
                      const visible = element => element && element.getClientRects().length > 0 &&
                        getComputedStyle(element).visibility !== 'hidden' && !element.disabled &&
                        element.getAttribute('aria-disabled') !== 'true';
                      const pending = () => {
                        const player = window.ytInitialPlayerResponse;
                        if (player?.videoDetails?.videoId !== '${videoId}') return false;
                        return player.videoDetails.isUpcoming === true || player.videoDetails.isLive === true ||
                          player.microformat?.playerMicroformatRenderer?.liveBroadcastDetails?.isLiveNow === true;
                      };
                      const transcriptButton = () => Array.from(document.querySelectorAll(
                        'ytd-video-description-transcript-section-renderer button'
                      )).find(visible);
                      const opener = () => Array.from(document.querySelectorAll(
                        '#description-inline-expander #expand, button[role="tab"]'
                      )).find(element => visible(element) && (element.id === 'expand' ||
                        /description|תיאור/i.test((element.getAttribute('aria-label') || '') + ' ' + element.textContent)
                      ));
                      const descriptionOpen = () => opener()?.getAttribute('aria-selected') === 'true' ||
                        !!opener()?.closest('#description-inline-expander[is-expanded]');
                      const transcriptPanel = () => Array.from(document.querySelectorAll(
                        'ytd-engagement-panel-section-list-renderer'
                      )).find(element => visible(element) && element.getAttribute('visibility') === 'ENGAGEMENT_PANEL_VISIBILITY_EXPANDED' &&
                        (['engagement-panel-searchable-transcript', 'PAmodern_transcript_view'].includes(element.getAttribute('target-id')) ||
                          element.querySelector('yt-section-list-renderer[data-target-id="PAmodern_transcript_view"][panel-content-visible]')));
                      const waitFor = async (read, until, retry) => {
                        const retryAt = Date.now() + 2000;
                        let retried = false;
                        while (Date.now() < until) {
                          const value = read();
                          if (value) return value;
                          if (retry && !retried && Date.now() >= retryAt) {
                            retry();
                            retried = true;
                          }
                          await new Promise(resolve => setTimeout(resolve, 250));
                        }
                        return null;
                      };
                      const pageDeadline = Math.min(deadline, Date.now() + 30000);
                      const expand = await waitFor(() => pending() || transcriptButton() || opener(), pageDeadline);
                      if (pending()) return 'YOUTUBE_WAIT:live_incomplete';
                      if (!expand) return 'YOUTUBE_ERROR:description_unavailable';
                      if (!transcriptButton() && !descriptionOpen()) {
                        expand.scrollIntoView({ block: 'center' });
                        expand.click();
                      }
                      const button = await waitFor(() => pending() || transcriptButton(), Math.min(deadline, Date.now() + 15000), () => {
                        if (!descriptionOpen()) opener()?.click();
                      });
                      if (pending()) return 'YOUTUBE_WAIT:live_incomplete';
                      if (!button) return 'YOUTUBE_ERROR:transcript_button_unavailable';
                      button.scrollIntoView({ block: 'center' });
                      if (!transcriptPanel()) button.click();
                      let previous = '', stableSince = Date.now();
                      const transcript = await waitFor(() => {
                        const panel = transcriptPanel();
                        if (!panel || panel.querySelector('tp-yt-paper-spinner[active]')) {
                          previous = '';
                          return null;
                        }
                        const rows = Array.from(panel.querySelectorAll(
                          'ytd-transcript-segment-renderer, transcript-segment-view-model'
                        ));
                        rows.at(-1)?.scrollIntoView({ block: 'end' });
                        const lines = rows.map(segment => {
                          const time = segment.querySelector('.segment-timestamp, .ytwTranscriptSegmentViewModelTimestamp')?.textContent?.trim() || '';
                          const text = segment.querySelector('.segment-text, span[role="text"]')?.textContent?.trim() || '';
                          return text ? '[' + time + '] ' + text : '';
                        });
                        const text = lines.length && lines.every(Boolean) ? lines.join('\\n') : '';
                        if (text !== previous) { previous = text; stableSince = Date.now(); }
                        return text && Date.now() - stableSince >= 1500 ? text : null;
                      }, Math.min(deadline, Date.now() + 60000), () => {
                        if (!transcriptPanel()) transcriptButton()?.click();
                      });
                      return transcript || 'YOUTUBE_ERROR:transcript_unavailable';
                    })()`,
                    },
                  ],
                }
              : {}),
          }),
        },
      );
    } catch (error) {
      const timedOut = ["TimeoutError", "AbortError"].includes(String(object(error)?.name));
      if (transcript && attempt === 0 && timedOut && deadline - Date.now() > 20_000) continue;
      throw failure(timedOut ? "source_timeout" : "source_unavailable", videoId);
    }
    if (
      transcript &&
      attempt === 0 &&
      [502, 503, 504].includes(response.status) &&
      deadline - Date.now() > 20_000
    ) {
      console.warn(
        "[YouTubeSourceHTTP]",
        JSON.stringify({
          videoId,
          transport: direct ? "firecrawl" : "lovable_gateway",
          status: response.status,
          retry: 1,
        }),
      );
      await response.body?.cancel().catch(() => undefined);
      continue;
    }
    break;
  }
  const reader = response.body?.getReader();
  try {
    if (!response.ok || !reader) {
      let detail: Record<string, unknown> | null = null;
      if (reader) {
        try {
          const parts: Uint8Array[] = [];
          let bytes = 0;
          while (true) {
            const part = await reader.read();
            if (part.done) break;
            bytes += part.value.byteLength;
            if (bytes > 8192) break;
            parts.push(part.value);
          }
          if (bytes <= 8192) detail = object(JSON.parse(Buffer.concat(parts).toString("utf8")));
        } catch {
          // Preserve the HTTP failure even when its body is unavailable.
        }
      }
      const safe = (value: unknown) => {
        if (typeof value !== "string") return undefined;
        let text = value;
        for (const secret of [key, gateway])
          if (secret) text = text.split(secret).join("[redacted]");
        return text
          .replace(/https?:\/\/\S+|Bearer\s+\S+|\b(?:fc-|sk-)[\w-]+/gi, "[redacted]")
          .replace(/\s+/g, " ")
          .slice(0, 1000);
      };
      console.warn(
        "[YouTubeSourceHTTP]",
        JSON.stringify({
          videoId,
          transport: direct ? "firecrawl" : "lovable_gateway",
          status: response.status,
          code: safe(detail?.code) ?? safe(object(detail?.error)?.code),
          error:
            safe(detail?.error) ?? safe(object(detail?.error)?.message) ?? safe(detail?.message),
        }),
      );
      throw failure(
        response.status === 401
          ? "access_required"
          : response.status === 403
            ? "video_access_required"
            : "source_unavailable",
        videoId,
      );
    }
    const decoder = new TextDecoder();
    let raw = "",
      size = 0;
    while (true) {
      let part;
      try {
        part = await reader.read();
      } catch {
        throw failure("source_unavailable", videoId);
      }
      if (part.done) break;
      size += part.value.byteLength;
      if (size > 8 * 1024 * 1024) throw failure("transcript_too_large", videoId);
      raw += decoder.decode(part.value, { stream: true });
    }
    raw += decoder.decode();
    let body;
    try {
      body = object(JSON.parse(raw));
    } catch {
      throw failure("source_unavailable", videoId);
    }
    const data = object(body?.data);
    if (body?.success !== true || !data || typeof data.rawHtml !== "string" || !data.rawHtml)
      throw failure(transcript ? "transcript_unavailable" : "video_unverified", videoId);
    return data;
  } finally {
    await reader?.cancel().catch(() => undefined);
    reader?.releaseLock();
  }
}

/** Timing discovery reads the verified channel feed without requesting any
 * transcripts or summarising/re-emitting videos. */
export async function readYouTubePublicationHistory(
  raw: string,
  at: string,
): Promise<NonNullable<SourceScan["publicationHistory"]>> {
  const source = parseYouTubeSource(raw),
    now = sourceDate(at);
  if (!source || !now) throw failure("source_unverified");
  if (source.kind !== "channel") {
    const data = await scrapeVideo(source.videoId, SOURCE_BUDGET_MS, false);
    const player = parseYouTubePlayer(data.rawHtml as string, source.videoId);
    return {
      items:
        Date.parse(player.publishedAt) <= Date.parse(now)
          ? [
              {
                key: `youtube:video:${player.videoId}`,
                url: youtubeVideoUrl(player.videoId),
                publishedAt: player.publishedAt,
              },
            ]
          : [],
    };
  }
  let channelId = source.channelId;
  if (!channelId) {
    const page = await publicPage(source.url, CHANNEL_PAGE_MAX_BYTES, "channel");
    const final = parseYouTubeSource(page.url);
    if (final?.kind !== "channel" || final.url !== source.url) throw failure("access_required");
    channelId = resolveYouTubeChannelId(page.bytes.toString("utf8"), source.url);
  }
  const feedUrl = `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
  const feed = await publicPage(feedUrl, CHANNEL_FEED_MAX_BYTES, "feed");
  const final = new URL(feed.url);
  if (
    final.protocol !== "https:" ||
    final.hostname !== "www.youtube.com" ||
    !["/feeds/videos.xml", "/xml/feeds/videos.xml"].includes(final.pathname) ||
    final.searchParams.get("channel_id") !== channelId
  )
    throw failure("feed_unverified");
  const known = parseYouTubeFeed(feed.bytes.toString("utf8"), channelId).filter(
    (video) => Date.parse(video.publishedAt) <= Date.parse(now),
  );
  return {
    items: known.map((video) => ({
      key: `youtube:video:${video.videoId}`,
      url: youtubeVideoUrl(video.videoId),
      publishedAt: video.publishedAt,
    })),
    ...(known.length ? { completeSince: known[0].publishedAt } : {}),
  };
}

export async function scanYouTubeSource(
  raw: string,
  cursor: SourceCursor,
  scanStartedAt: string,
  options?: SourceScanOptions,
): Promise<SourceScan> {
  const source = parseYouTubeSource(raw),
    at = sourceDate(scanStartedAt),
    prior = cursor.scanned_at === undefined ? null : sourceDate(cursor.scanned_at);
  if (
    !source ||
    !at ||
    (cursor.scanned_at !== undefined && !prior) ||
    (prior && Date.parse(prior) > Date.parse(at))
  )
    throw failure("source_unverified");
  const cutoff = prior ? Date.parse(prior) : Date.parse(at) - 48 * 60 * 60_000;
  const startedAt = Date.now();
  const deadline = Math.min(startedAt + SOURCE_BUDGET_MS, options?.deadline ?? Infinity);
  const remaining = () => {
    const elapsed = Date.now() - startedAt;
    if (elapsed < 0) throw failure("source_unavailable");
    if (Date.now() >= deadline) throw failure("scan_deadline");
    return deadline - Date.now();
  };
  remaining();
  let videos: YouTubeVideo[] = [],
    directData: Record<string, unknown> | null = null;
  let publicationHistory: SourceScan["publicationHistory"];
  let verifiedChannelId: string | undefined;
  if (source.kind === "channel") {
    let channelId = source.channelId ?? cursor.youtube_channel_id ?? null;
    if (channelId && !YOUTUBE_CHANNEL_ID.test(channelId)) throw failure("source_unverified");
    if (!channelId) {
      const page = await publicPage(source.url, CHANNEL_PAGE_MAX_BYTES, "channel");
      const final = parseYouTubeSource(page.url);
      if (final?.kind !== "channel" || final.url !== source.url) throw failure("access_required");
      channelId = resolveYouTubeChannelId(page.bytes.toString("utf8"), source.url);
    }
    remaining();
    const feedUrl = `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
    const feed = await publicPage(feedUrl, CHANNEL_FEED_MAX_BYTES, "feed");
    const final = new URL(feed.url);
    if (
      final.protocol !== "https:" ||
      final.hostname !== "www.youtube.com" ||
      !["/feeds/videos.xml", "/xml/feeds/videos.xml"].includes(final.pathname) ||
      final.searchParams.get("channel_id") !== channelId
    )
      throw failure("feed_unverified");
    videos = parseYouTubeFeed(feed.bytes.toString("utf8"), channelId);
    verifiedChannelId = channelId;
    const known = videos.filter((video) => Date.parse(video.publishedAt) <= Date.parse(at));
    publicationHistory = {
      items: known.map((video) => ({
        key: `youtube:video:${video.videoId}`,
        url: youtubeVideoUrl(video.videoId),
        publishedAt: video.publishedAt,
      })),
      ...(known.length ? { completeSince: known[0].publishedAt } : {}),
    };
    if (videos.length === FEED_SIZE && Date.parse(videos[0].publishedAt) >= cutoff)
      throw failure("discovery_incomplete");
    videos = videos.filter(
      (video) =>
        Date.parse(video.publishedAt) >= cutoff && Date.parse(video.publishedAt) <= Date.parse(at),
    );
  } else {
    if (
      options?.knownItems &&
      (await options.knownItems([`youtube:video:${source.videoId}`])).includes(
        `youtube:video:${source.videoId}`,
      )
    )
      return { items: [], cursor: { scanned_at: scanStartedAt } };
    directData = await scrapeVideo(source.videoId, remaining());
    remaining();
    let player: Player;
    try {
      player = parseYouTubePlayer(directData.rawHtml as string, source.videoId);
    } catch (error) {
      if (!(error instanceof YouTubeSourceError) || error.reason !== "live_incomplete") throw error;
      return {
        items: [],
        cursor: { ...cursor },
        pending: [`${source.videoId}: ממתין לסיום השידור ולתמליל מלא.`],
      };
    }
    if (!player.author) throw failure("video_unverified", source.videoId);
    videos = [
      {
        videoId: player.videoId,
        channelId: player.channelId,
        publishedAt: player.publishedAt,
        sourceName: player.author,
      },
    ].filter(
      (video) =>
        Date.parse(video.publishedAt) >= cutoff && Date.parse(video.publishedAt) <= Date.parse(at),
    );
  }
  const known = new Set(
    options?.knownItems && videos.length && !directData
      ? await options.knownItems(videos.map((video) => `youtube:video:${video.videoId}`))
      : [],
  );
  const items: SourceItem[] = [];
  const failures: string[] = [];
  const pending: string[] = [];
  let scannedAt = scanStartedAt;
  for (const video of videos) {
    if (known.has(`youtube:video:${video.videoId}`)) continue;
    // Use the existing collection budget, retaining successes and the next
    // inclusive boundary when another extraction cannot safely start.
    if (!directData && deadline - Date.now() <= 20_000) {
      if (Date.parse(video.publishedAt) < Date.parse(scannedAt)) scannedAt = video.publishedAt;
      pending.push(`${video.videoId}: לא נותר זמן לסרטון נוסף; ההתקדמות נשמרה להמשך.`);
      break;
    }
    try {
      const data = directData ?? (await scrapeVideo(video.videoId, remaining()));
      remaining();
      const transcript = readTranscript(data, video);
      const item: SourceItem = {
        key: `youtube:video:${video.videoId}`,
        url: youtubeVideoUrl(video.videoId),
        sourceName: video.sourceName,
        publishedAt: video.publishedAt,
        text: transcript,
        images: [],
      };
      // Finish analysis while its reserved budget is still available. A failed
      // analysis retains this video's inclusive cursor for the next scan.
      const analysisError = await options?.onItem?.(item);
      if (analysisError) throw new YouTubeSourceError("analysis_failed", analysisError);
      items.push(item);
    } catch (error) {
      if (!(error instanceof YouTubeSourceError)) throw error;
      if (error.reason === "live_incomplete")
        pending.push(`${video.videoId}: ממתין לסיום השידור ולתמליל מלא.`);
      else failures.push(`${video.videoId}: ${error.message}`);
      // Keep the inclusive watermark at the first unprocessed video. Later
      // successful videos can be saved and skipped by ID on the next scan.
      if (Date.parse(video.publishedAt) < Date.parse(scannedAt)) scannedAt = video.publishedAt;
      if (
        Date.now() >= deadline ||
        ["source_unavailable", "source_timeout", "scan_deadline", "access_required"].includes(
          error.reason,
        )
      )
        break;
    }
  }
  // The last analysis may legitimately use the time reserved after collection.
  // Do not discard completed items merely because that collection cutoff passed.
  if (!failures.length && !pending.length && !options?.onItem) remaining();
  return {
    items,
    cursor: {
      scanned_at: scannedAt,
      ...(verifiedChannelId ? { youtube_channel_id: verifiedChannelId } : {}),
    },
    ...(failures.length ? { failures } : {}),
    ...(pending.length ? { pending } : {}),
    ...(publicationHistory ? { publicationHistory } : {}),
  };
}
