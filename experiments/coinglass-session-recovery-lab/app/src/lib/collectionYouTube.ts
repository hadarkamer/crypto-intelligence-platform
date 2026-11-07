export type YouTubeSource =
  | { kind: "video"; videoId: string; url: string }
  | { kind: "channel"; channelId: string | null; url: string };
export const YOUTUBE_VIDEO_ID = /^[A-Za-z0-9_-]{11}$/;
export const YOUTUBE_CHANNEL_ID = /^UC[A-Za-z0-9_-]{22}$/;
export function youtubeVideoUrl(videoId: string): string {
  if (!YOUTUBE_VIDEO_ID.test(videoId)) throw new Error("מזהה סרטון YouTube אינו תקין.");
  return `https://www.youtube.com/watch?v=${videoId}`;
}

/** Only supported public identities are enabled; no arbitrary YouTube endpoints. */
export function parseYouTubeSource(raw: string): YouTubeSource | null {
  try {
    const url = new URL(raw);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      url.hash ||
      raw.length > 1000
    )
      return null;
    const host = url.hostname.toLowerCase();
    if (
      !["youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be", "www.youtu.be"].includes(
        host,
      )
    )
      return null;
    const keys = [...url.searchParams.keys()];
    if (
      keys.some((key) => !["v", "t", "start", "si", "feature"].includes(key)) ||
      new Set(keys).size !== keys.length
    )
      return null;
    let videoId: string | null = null;
    if (host.endsWith("youtu.be")) {
      const match = url.pathname.match(/^\/([A-Za-z0-9_-]{11})\/?$/);
      if (!match || url.searchParams.has("v")) return null;
      videoId = match[1];
    } else if (url.pathname === "/watch") {
      videoId = url.searchParams.get("v");
      if (!videoId || !YOUTUBE_VIDEO_ID.test(videoId)) return null;
    } else {
      const direct = url.pathname.match(/^\/(?:live|shorts|embed)\/([A-Za-z0-9_-]{11})\/?$/);
      if (direct) {
        if (url.searchParams.has("v")) return null;
        videoId = direct[1];
      }
    }
    if (videoId) return { kind: "video", videoId, url: youtubeVideoUrl(videoId) };
    if (url.search || host.endsWith("youtu.be")) return null;
    const channel = url.pathname.match(/^\/channel\/(UC[A-Za-z0-9_-]{22})(?:\/videos)?\/?$/);
    if (channel)
      return {
        kind: "channel",
        channelId: channel[1],
        url: `https://www.youtube.com/channel/${channel[1]}/videos`,
      };
    const handle = url.pathname.match(/^\/@([^/]{1,200})(?:\/videos)?\/?$/);
    if (handle) {
      const decoded = decodeURIComponent(handle[1]);
      if (!/^[\p{L}\p{N}._-]{1,100}$/u.test(decoded)) return null;
      return {
        kind: "channel",
        channelId: null,
        url: `https://www.youtube.com/@${encodeURIComponent(decoded)}/videos`,
      };
    }
    return null;
  } catch {
    return null;
  }
}
export function isYouTubeSourceUrl(raw: string): boolean {
  return parseYouTubeSource(raw) !== null;
}
