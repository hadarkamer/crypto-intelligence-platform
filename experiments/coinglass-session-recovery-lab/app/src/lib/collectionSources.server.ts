import { lookup } from "node:dns/promises";
import { request } from "node:https";
import { isIP } from "node:net";
import { DOMParser, parseHTML } from "linkedom";
import { canonicalSourceUrl, supportedSourceUrl } from "./collectionAutofill";
import type { NewsDecision } from "./collectionAutofillModel.server";

export type SourceItem = {
  key: string;
  url: string;
  text: string;
  sourceName: string;
  publishedAt: string;
  images: string[];
  media?: SourceMedia[];
  mediaOrigin?: "feed" | "article" | "post";
};
export type SourceMedia = {
  url: string;
  provenance: "article_body" | "feed_body" | "post_attachment";
  context: string;
};
export type SourceCursor = {
  scanned_at?: string;
  since_id?: string;
  youtube_channel_id?: string;
  x_user_id?: string;
  x_user_checked_at?: string;
  x_window_started_at?: string;
  x_daily?: { day: string; used: number };
  x_pending?: {
    items: SourceItem[];
    cursor: Omit<SourceCursor, "x_pending">;
    analysis?: Record<
      string,
      {
        v: 1;
        input_hash: string;
        decision: NewsDecision;
        writeMode: "default" | "append" | "replace";
      }
    >;
    pagination?: {
      since_id?: string;
      start_time?: string;
      next_token?: string;
      until_id?: string;
      coverage_proven: boolean;
    };
    publicationHistory?: SourcePublicationHistory;
    // Reserve before a paid read; retain its response until parsing/checkpointing succeeds.
    request?: { day: string; reserved: number; response?: XResponse };
  };
};
export type SourcePublicationHistory = {
  items: Array<{ key: string; url: string; publishedAt: string }>;
  completeSince?: string;
};
export type SourceScan = {
  items: SourceItem[];
  cursor: SourceCursor;
  pageText?: string;
  publicationHistory?: SourcePublicationHistory;
  failures?: string[];
  pending?: string[];
};
export type SourceScanOptions = {
  workspaceId?: string;
  knownItems?: (keys: string[]) => Promise<string[]>;
  deadline?: number;
  onItem?: (item: SourceItem) => Promise<string | null>;
  onXCheckpoint?: (pending: NonNullable<SourceCursor["x_pending"]>) => Promise<void>;
};
const privateNetworks = [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const;
const ipv4Number = (address: string) =>
  address.split(".").reduce((value, octet) => value * 256 + Number(octet), 0);
export function publicAddress(address: string): boolean {
  if (isIP(address) === 4)
    return !privateNetworks.some(
      ([base, prefix]) =>
        Math.floor(ipv4Number(address) / 2 ** (32 - prefix)) ===
        Math.floor(ipv4Number(base) / 2 ** (32 - prefix)),
    );
  // Require global unicast IPv6; reject documentation, special-use and 6to4
  // ranges, which can encode a private IPv4 destination.
  return (
    isIP(address) === 6 &&
    /^[23][0-9a-f]{3}:/i.test(address) &&
    !/^2001:(?:db8|0*[0-9a-f]{1,2}|0*1[0-9a-f]{2}):/i.test(address) &&
    !/^2002:|^3fff:0*[0-9a-f]{1,3}:/i.test(address)
  );
}

const sourceHeaders = (headers: Record<string, string>) => ({
  "User-Agent": "TradePlanPad/1.0 (+collection-autofill)",
  Accept: "text/html,application/xml,application/rss+xml,application/json;q=0.9,*/*;q=0.5",
  "Accept-Encoding": "identity",
  ...headers,
});

/** A discovered YouTube Atom feed is an internal read endpoint, not a source
 * that can enable autofill in the template. Keep this exception exact and
 * apply the same DNS, redirect, TLS and response-size boundary as other reads. */
export function supportedSourceRequestUrl(raw: string): boolean {
  if (supportedSourceUrl(raw)) return true;
  try {
    const url = new URL(raw);
    return (
      raw.length <= 1000 &&
      url.protocol === "https:" &&
      ["youtube.com", "www.youtube.com"].includes(url.hostname) &&
      !url.username &&
      !url.password &&
      !url.port &&
      !url.hash &&
      ["/feeds/videos.xml", "/xml/feeds/videos.xml"].includes(url.pathname) &&
      [...url.searchParams.keys()].length === 1 &&
      url.searchParams.getAll("channel_id").length === 1 &&
      /^UC[A-Za-z0-9_-]{22}$/.test(url.searchParams.get("channel_id") ?? "")
    );
  } catch {
    return false;
  }
}

function retryAfterMilliseconds(raw: unknown, receivedAt: number): number | null | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw !== "string" || raw.length > 128) return null;
  const value = raw.trim();
  if (/^\d+$/.test(value)) {
    const milliseconds = Number(value) * 1000;
    return Number.isSafeInteger(milliseconds) ? milliseconds : null;
  }
  if (
    !/^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(
      value,
    )
  )
    return null;
  const deadline = Date.parse(value);
  if (
    !Number.isSafeInteger(deadline) ||
    !Number.isSafeInteger(receivedAt) ||
    new Date(deadline).toUTCString() !== value
  )
    return null;
  return Math.max(0, deadline - receivedAt);
}

/** Status metadata comes only from the checked transport response. Keep URLs
 * and parsed retry hints out of generic error serialization and messages. */
export class SourceHttpError extends Error {
  declare readonly status: number;
  declare readonly url: string;
  declare readonly retryAfterMs: number | null | undefined;

  constructor(
    status: number,
    url: string,
    retryAfter?: unknown,
    receivedAt = Date.now(),
    body?: string,
  ) {
    let message = `המקור אינו זמין (HTTP ${status}).`;
    if (status === 402 && new URL(url).hostname === "api.x.com") {
      let depleted = false;
      try {
        const problem = JSON.parse(body ?? "{}");
        depleted =
          problem?.title === "CreditsDepleted" ||
          /^https:\/\/api\.(?:x|twitter)\.com\/2\/problems\/credits-depleted$/.test(
            problem?.type ?? "",
          );
      } catch {
        /* Never include the provider's free text or account details. */
      }
      const stage = new URL(url).pathname.startsWith("/2/users/by/username/")
        ? "איתור החשבון"
        : "קריאת הפוסטים";
      message =
        `X API דחה את ${stage} (HTTP 402${depleted ? "; credits-depleted" : ""}). ` +
        (depleted
          ? "X מדווח שאין קרדיטים זמינים לבקשה; יש לבדוק יתרה ושיוך חיוב בחשבון המפתחים."
          : "יש לבדוק את החיוב ומכסת החשבון ב־X; ניסיון חוזר אינו פותר דרישת תשלום.");
    }
    super(message);
    Object.defineProperties(this, {
      status: { value: status },
      url: { value: url },
      retryAfterMs: { value: retryAfterMilliseconds(retryAfter, receivedAt) },
    });
  }
}

/** Only a structured TLS failure from the HTTPS request receives this type.
 * Preserve the checked terminal URL privately; never retain native error text,
 * pinned addresses, certificate details or the original error as a cause. */
export class SourceTransportError extends Error {
  declare readonly kind: "tls_hostname_mismatch";
  declare readonly url: string;

  constructor(url: string) {
    super("לא ניתן לאמת את החיבור המאובטח למקור.");
    Object.defineProperties(this, {
      kind: { value: "tls_hostname_mismatch" },
      url: { value: url },
    });
  }
}

/** Workers cannot pin node:https lookups. Use their public-internet fetch
 * boundary, never a VPC binding, and still validate DNS and every redirect.
 * The resolver argument lets the same boundary run in isolated contract tests. */
export function edgeSourceTransport(
  resolveAddresses: (host: string) => Promise<string[]>,
  acceptUrl?: (raw: string) => boolean,
) {
  const get = async (
    raw: string,
    maxBytes = 2 * 1024 * 1024,
    headers: Record<string, string> = {},
    redirects = 0,
    signal?: AbortSignal,
  ): Promise<{ bytes: Buffer; contentType: string; url: string }> => {
    signal?.throwIfAborted();
    if (!supportedSourceRequestUrl(raw) || (acceptUrl && !acceptUrl(raw)))
      throw new Error("קישור המקור אינו מקור ציבורי נתמך.");
    const url = new URL(raw);
    const addresses = await resolveAddresses(url.hostname);
    signal?.throwIfAborted();
    if (!addresses.length || addresses.some((address) => !publicAddress(address)))
      throw new Error("כתובת המקור אינה ציבורית.");
    const response = await fetch(url.href, {
      headers: sourceHeaders(headers),
      redirect: "manual",
      signal: signal
        ? AbortSignal.any([signal, AbortSignal.timeout(20_000)])
        : AbortSignal.timeout(20_000),
    });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      await response.body?.cancel();
      const location = response.headers.get("location");
      if (redirects >= 4 || !location) throw new Error("המקור הפנה פעמים רבות מדי.");
      const next = new URL(location, url);
      return get(
        next.href,
        maxBytes,
        next.origin === url.origin ? headers : {},
        redirects + 1,
        signal,
      );
    }
    const xPayment = response.status === 402 && url.hostname === "api.x.com";
    if (!response.ok && !xPayment) {
      await response.body?.cancel();
      throw new SourceHttpError(
        response.status,
        url.href,
        response.headers.get("retry-after") ?? undefined,
      );
    }
    const reader = response.body?.getReader();
    if (!reader && xPayment) throw new SourceHttpError(402, url.href);
    if (!reader)
      return {
        bytes: Buffer.alloc(0),
        contentType: response.headers.get("content-type") ?? "",
        url: url.href,
      };
    const chunks: Buffer[] = [];
    let size = 0;
    try {
      for (;;) {
        const result = await reader.read();
        if (result.done) break;
        size += result.value.length;
        if (size > (xPayment ? Math.min(maxBytes, 8192) : maxBytes))
          throw xPayment ? new SourceHttpError(402, url.href) : new Error("תשובת המקור גדולה מדי.");
        chunks.push(Buffer.from(result.value));
      }
    } catch (error) {
      await reader.cancel().catch(() => undefined);
      throw error;
    } finally {
      reader.releaseLock();
    }
    if (xPayment)
      throw new SourceHttpError(
        402,
        url.href,
        undefined,
        Date.now(),
        Buffer.concat(chunks).toString("utf8"),
      );
    return {
      bytes: Buffer.concat(chunks),
      contentType: response.headers.get("content-type") ?? "",
      url: url.href,
    };
  };
  return get;
}
const DNS_RESPONSE_BYTES = 32 * 1024;
const DNS_FAILURE = "לא ניתן לאמת את כתובת המקור דרך DNS.";

async function edgeDnsRecords(host: string, type: 1 | 28): Promise<string[]> {
  const endpoint = new URL("https://cloudflare-dns.com/dns-query");
  endpoint.searchParams.set("name", host);
  endpoint.searchParams.set("type", String(type));
  const signal = AbortSignal.timeout(5000);
  const response = await fetch(endpoint.href, {
    headers: { Accept: "application/dns-json" },
    redirect: "manual",
    signal,
  });
  if (!response.ok || !response.body) {
    await response.body?.cancel();
    throw new Error(DNS_FAILURE);
  }
  const reader = response.body.getReader();
  const cancelOnAbort = () => void reader.cancel().catch(() => undefined);
  signal.addEventListener("abort", cancelOnAbort, { once: true });
  const chunks: Buffer[] = [];
  let size = 0;
  let raw: string;
  try {
    for (;;) {
      signal.throwIfAborted();
      const { done, value } = await reader.read();
      signal.throwIfAborted();
      if (done) break;
      size += value.length;
      if (size > DNS_RESPONSE_BYTES) throw new Error(DNS_FAILURE);
      chunks.push(Buffer.from(value));
    }
    raw = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks));
  } catch (error) {
    await reader.cancel().catch(() => undefined);
    throw error;
  } finally {
    signal.removeEventListener("abort", cancelOnAbort);
    reader.releaseLock();
  }
  const decoded: unknown = JSON.parse(raw);
  if (!decoded || typeof decoded !== "object" || Array.isArray(decoded))
    throw new Error(DNS_FAILURE);
  const payload = decoded as Record<string, unknown>;
  const question = Array.isArray(payload.Question) ? payload.Question[0] : null;
  if (
    payload.Status !== 0 ||
    payload.TC !== false ||
    !Array.isArray(payload.Question) ||
    payload.Question.length !== 1 ||
    !question ||
    typeof question !== "object" ||
    question.type !== type ||
    typeof question.name !== "string" ||
    question.name.toLowerCase().replace(/\.$/, "") !== host.toLowerCase().replace(/\.$/, "") ||
    (payload.Answer !== undefined && !Array.isArray(payload.Answer))
  )
    throw new Error(DNS_FAILURE);
  const addresses: string[] = [];
  for (const answer of (payload.Answer ?? []) as unknown[]) {
    if (!answer || typeof answer !== "object" || Array.isArray(answer))
      throw new Error(DNS_FAILURE);
    const record = answer as Record<string, unknown>;
    if (
      !Number.isInteger(record.type) ||
      typeof record.name !== "string" ||
      typeof record.data !== "string"
    )
      throw new Error(DNS_FAILURE);
    // CNAME and DNSSEC records are names/metadata, never destination addresses.
    if (record.type !== 1 && record.type !== 28) continue;
    if (record.type !== type || isIP(record.data) !== (type === 1 ? 4 : 6))
      throw new Error(DNS_FAILURE);
    addresses.push(record.data);
  }
  return addresses;
}

/** Fixed unauthenticated DoH requests avoid runtime-dependent Node DNS shims.
 * Both families must resolve successfully; an absent family is valid NOERROR.
 * edgeSourceTransport still rejects every non-public address before fetching. */
export async function edgeSourceAddresses(host: string): Promise<string[]> {
  if (isIP(host)) return [host];
  try {
    const results = await Promise.allSettled([edgeDnsRecords(host, 1), edgeDnsRecords(host, 28)]);
    const addresses: string[] = [];
    for (const result of results) {
      if (result.status === "rejected") throw new Error(DNS_FAILURE);
      addresses.push(...result.value);
    }
    return addresses;
  } catch {
    throw new Error(DNS_FAILURE);
  }
}
const edgeRequest = edgeSourceTransport(edgeSourceAddresses);

/** Resolve and pin the destination on every redirect; DNS rebinding cannot turn
 * a public source URL into a request to a private host or cloud metadata on
 * Node/Bun. Cloudflare uses its public-internet fetch transport instead. */
export async function sourceRequest(
  raw: string,
  maxBytes = 2 * 1024 * 1024,
  headers: Record<string, string> = {},
  redirects = 0,
  signal?: AbortSignal,
): Promise<{ bytes: Buffer; contentType: string; url: string }> {
  signal?.throwIfAborted();
  if (typeof navigator !== "undefined" && navigator.userAgent === "Cloudflare-Workers")
    return edgeRequest(raw, maxBytes, headers, redirects, signal);
  if (!supportedSourceRequestUrl(raw)) throw new Error("קישור המקור אינו מקור ציבורי נתמך.");
  const url = new URL(raw);
  const addresses = await lookup(url.hostname, { all: true, verbatim: true });
  signal?.throwIfAborted();
  if (!addresses.length || addresses.some((entry) => !publicAddress(entry.address)))
    throw new Error("כתובת המקור אינה ציבורית.");
  const pinned = addresses[0];
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => req.destroy(new Error("סריקת המקור חרגה מזמן ההמתנה.")), 20_000);
    const req = request(
      url,
      {
        headers: sourceHeaders(headers),
        ...(signal ? { signal } : {}),
        // DNS pinning changes the connection address, not the TLS identity.
        // Keep native certificate verification against the original DNS name.
        ...(isIP(url.hostname.replace(/^\[|\]$/g, "")) === 0 ? { servername: url.hostname } : {}),
        lookup: (_host, options, callback) => {
          if (options.all) callback(null, [pinned]);
          else callback(null, pinned.address, pinned.family);
        },
      },
      (response) => {
        if ([301, 302, 303, 307, 308].includes(response.statusCode ?? 0)) {
          response.resume();
          clearTimeout(timer);
          if (redirects >= 4 || !response.headers.location) {
            reject(new Error("המקור הפנה פעמים רבות מדי."));
            return;
          }
          // Never forward an API credential to a redirected host.
          const next = new URL(response.headers.location, url);
          const forwarded = next.origin === url.origin ? headers : {};
          void sourceRequest(next.href, maxBytes, forwarded, redirects + 1, signal).then(
            resolve,
            reject,
          );
          return;
        }
        const xPayment = response.statusCode === 402 && url.hostname === "api.x.com";
        if (
          !xPayment &&
          ((response.statusCode ?? 500) < 200 || (response.statusCode ?? 500) >= 300)
        ) {
          response.resume();
          clearTimeout(timer);
          reject(
            new SourceHttpError(
              response.statusCode ?? 500,
              url.href,
              response.headers["retry-after"],
            ),
          );
          return;
        }
        if (
          response.headers["content-encoding"] &&
          response.headers["content-encoding"] !== "identity"
        ) {
          response.resume();
          clearTimeout(timer);
          reject(
            xPayment
              ? new SourceHttpError(402, url.href)
              : new Error("המקור החזיר קידוד שאינו נתמך."),
          );
          return;
        }
        const chunks: Buffer[] = [];
        let total = 0;
        response.on("data", (chunk: Buffer) => {
          total += chunk.length;
          if (total > (xPayment ? Math.min(maxBytes, 8192) : maxBytes)) {
            req.destroy(
              xPayment ? new SourceHttpError(402, url.href) : new Error("תשובת המקור גדולה מדי."),
            );
          } else chunks.push(chunk);
        });
        response.on("end", () => {
          clearTimeout(timer);
          if (xPayment) {
            reject(
              new SourceHttpError(
                402,
                url.href,
                undefined,
                Date.now(),
                Buffer.concat(chunks).toString("utf8"),
              ),
            );
            return;
          }
          resolve({
            bytes: Buffer.concat(chunks),
            contentType: String(response.headers["content-type"] ?? ""),
            url: url.href,
          });
        });
        response.on("error", (error) => {
          clearTimeout(timer);
          reject(error);
        });
      },
    );
    req.on("error", (error) => {
      clearTimeout(timer);
      reject(
        error &&
          typeof error === "object" &&
          "code" in error &&
          error.code === "ERR_TLS_CERT_ALTNAME_INVALID"
          ? new SourceTransportError(url.href)
          : error,
      );
    });
    req.end();
  });
}

const plain = (html: string) => {
  const { document } = parseHTML(`<html><body>${html}</body></html>`);
  document
    .querySelectorAll("script,style,nav,footer,header,noscript")
    .forEach((node) => node.remove());
  return (document.body.textContent ?? "").replace(/\s+/g, " ").trim();
};
const date = (raw: unknown): string | null => {
  const stamp = Date.parse(String(raw ?? ""));
  return Number.isFinite(stamp) && stamp <= Date.now() + 120_000
    ? new Date(stamp).toISOString()
    : null;
};
const absolute = (raw: string | null | undefined, base: string): string | null => {
  try {
    const url = new URL(raw ?? "", base);
    return raw && supportedSourceUrl(url.href) ? canonicalSourceUrl(url.href) : null;
  } catch {
    return null;
  }
};

export function isCryptoJungleArticle(raw: string): boolean {
  try {
    return /(^|\.)cryptojungle\.co\.il$/i.test(new URL(raw).hostname);
  } catch {
    return false;
  }
}

/** Location supplies provenance, never an assertion that an image is useful.
 * Covers, social cards and feed enclosures are excluded before visual review. */
function bodyMedia(
  body: Element | null,
  base: string,
  provenance: SourceMedia["provenance"],
  excluded: Set<string> = new Set(),
): SourceMedia[] {
  const media: SourceMedia[] = [];
  for (const image of body?.querySelectorAll("img") ?? []) {
    if (
      image.closest("header,nav,footer,aside") ||
      /(?:wp-post-image|post-thumbnail|featured|hero|avatar|logo)/i.test(image.className ?? "")
    )
      continue;
    const url = absolute(
      image.getAttribute("data-lazy-src") ??
        image.getAttribute("data-src") ??
        image.getAttribute("src"),
      base,
    );
    if (!url || excluded.has(url) || media.some((entry) => entry.url === url)) continue;
    const context = [
      image.getAttribute("alt"),
      image.closest("figure")?.querySelector("figcaption")?.textContent,
      image.parentElement?.textContent,
    ]
      .filter(Boolean)
      .join(" ")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 600);
    media.push({ url, provenance, context });
    if (media.length === 4) break;
  }
  return media;
}

export function feedItems(xml: string, base: string): SourceItem[] {
  const document = new DOMParser().parseFromString(xml, "text/xml");
  const items: SourceItem[] = [];
  for (const node of document.querySelectorAll("item,entry")) {
    const linkNode = node.querySelector('link[rel="alternate"],link:not([rel])');
    const url = absolute(linkNode?.getAttribute("href") ?? linkNode?.textContent, base);
    const publishedAt = date(
      node.querySelector("pubDate,published,updated,dc\\:date")?.textContent,
    );
    if (!url || !publishedAt) continue;
    const coverImages = [...node.querySelectorAll("enclosure,media\\:content,media\\:thumbnail")]
      .map((image) => absolute(image.getAttribute("url"), base))
      .filter((image): image is string => !!image);
    const rawContent =
      node.querySelector("content\\:encoded,content,description,summary")?.textContent ?? "";
    const contentDoc = parseHTML(`<html><body>${rawContent}</body></html>`).document;
    const media = bodyMedia(contentDoc.body, url, "feed_body", new Set(coverImages));
    items.push({
      key: url,
      url,
      publishedAt,
      sourceName:
        document.querySelector("channel>title,feed>title")?.textContent ?? new URL(base).hostname,
      text: plain(`${node.querySelector("title")?.textContent ?? ""}\n${rawContent}`).slice(
        0,
        18_000,
      ),
      images: media.map((image) => image.url),
      media,
      mediaOrigin: "feed",
    });
  }
  return items;
}

export function articleItem(html: string, url: string): SourceItem | null {
  const { document } = parseHTML(html);
  let publishedAt: string | null = null;
  let sourceName = new URL(url).hostname;
  for (const script of document.querySelectorAll('script[type="application/ld+json"]')) {
    try {
      const root = JSON.parse(script.textContent ?? "null");
      const nodes = Array.isArray(root)
        ? root
        : [root, ...(Array.isArray(root?.["@graph"]) ? root["@graph"] : [])];
      const article = nodes.find((node) =>
        /Article|NewsPosting|BlogPosting/.test(String(node?.["@type"])),
      );
      if (article) {
        publishedAt = date(article.datePublished);
        sourceName = String(article.publisher?.name ?? sourceName);
        break;
      }
    } catch {
      /* A malformed metadata block is not evidence of a publication date. */
    }
  }
  publishedAt ??= date(
    document
      .querySelector('meta[property="article:published_time"],meta[name="date"],time[datetime]')
      ?.getAttribute("content") ??
      document.querySelector("time[datetime]")?.getAttribute("datetime"),
  );
  if (!publishedAt) return null;
  const canonical =
    absolute(document.querySelector('link[rel="canonical"]')?.getAttribute("href"), url) ?? url;
  const cover = absolute(
    document.querySelector('meta[property="og:image"]')?.getAttribute("content"),
    url,
  );
  const article = document.querySelector("article,main") ?? document.body;
  const explicitBody = document.querySelector(
    '.entry-content,.post-content,[itemprop="articleBody"],.article-content,.article-body',
  );
  // CryptoJungle feed/OG covers are particularly noisy. Without a recognizable
  // article body there is no reliable provenance, so retain text without media.
  const body = explicitBody ?? (isCryptoJungleArticle(url) ? null : article);
  const media = bodyMedia(body, url, "article_body", new Set(cover ? [cover] : []));
  return {
    key: canonical,
    url: canonical,
    publishedAt,
    sourceName,
    text: plain(article?.innerHTML ?? html).slice(0, 18_000),
    images: media.map((image) => image.url),
    media,
    mediaOrigin: "article",
  };
}

/** Only the accepted-item media stage should perform this optional lookup.
 * RSS metadata cannot prove that an image came from the actual article body. */
export async function hydrateNewsArticleMedia(item: SourceItem): Promise<SourceItem> {
  if (!isCryptoJungleArticle(item.url) || item.mediaOrigin !== "feed") return item;
  const withoutMedia = { ...item, images: [], media: [] };
  try {
    const response = await sourceRequest(item.url);
    const article = articleItem(response.bytes.toString("utf8"), response.url);
    return article && article.key === item.key
      ? { ...item, images: article.images, media: article.media, mediaOrigin: "article" }
      : withoutMedia;
  } catch {
    // An unavailable body must not discard an otherwise valid dated article.
    return withoutMedia;
  }
}

type XPost = {
  id: string;
  author_id?: string;
  created_at?: string;
  text?: string;
  note_tweet?: { text?: string };
  attachments?: { media_keys?: string[] };
  referenced_tweets?: { type: string; id: string }[];
};
type XResponse = {
  data?: XPost | XPost[];
  errors?: unknown[];
  includes?: {
    tweets?: XPost[];
    users?: { id: string; username: string }[];
    media?: { media_key: string; type: string; url: string; alt_text?: string }[];
  };
  meta?: { result_count?: number; next_token?: string };
};

/** X can return a complete timeline with errors for deleted/private expansions.
 * Ignore only a permanent failure tied to an optional resource on this page;
 * an unknown, primary-resource or transient error still blocks cursor progress. */
function xProblemResourceId(problem: Record<string, unknown>): string | null {
  const ids = [problem.resource_id, problem.value].filter(
    (id): id is string => typeof id === "string" && id.length > 0,
  );
  return ids.length && ids.every((id) => id === ids[0]) ? ids[0] : null;
}
function unavailableXExpansion(error: unknown, response: XResponse, posts: XPost[]): boolean {
  if (!error || typeof error !== "object") return false;
  const problem = error as Record<string, unknown>;
  if (
    typeof problem.type !== "string" ||
    !/^https:\/\/api\.(?:x|twitter)\.com\/2\/problems\/(?:resource-not-found|not-authorized-for-resource)$/.test(
      problem.type,
    )
  )
    return false;
  const id = xProblemResourceId(problem);
  if (!id || (problem.section !== undefined && problem.section !== "includes")) return false;
  // X's documented partial-error shape can omit parameter/value and identify
  // the unavailable resource with resource_id alone. Its explicit relation to
  // this page remains mandatory; absence of a parameter is never enough.
  const optionalParameter = (allowed: string[]) =>
    problem.parameter === undefined ||
    (typeof problem.parameter === "string" && allowed.includes(problem.parameter));
  const expandedPosts = [...posts, ...(response.includes?.tweets ?? [])];
  if (problem.resource_type === "tweet" && optionalParameter(["referenced_tweets.id", "ids"]))
    return (
      !posts.some((post) => post.id === id) &&
      expandedPosts.some((post) => post.referenced_tweets?.some((reference) => reference.id === id))
    );
  if (problem.resource_type === "media" && optionalParameter(["attachments.media_keys"]))
    return expandedPosts.some((post) => post.attachments?.media_keys?.includes(id));
  if (
    problem.resource_type === "user" &&
    optionalParameter(["referenced_tweets.id.author_id", "author_id"])
  )
    return (
      !posts.some((post) => post.author_id === id) &&
      (response.includes?.tweets ?? []).some((post) => post.author_id === id)
    );
  return false;
}

/** Keep the provider's stable problem code for diagnosis, never its free-text
 * detail, credentials, URLs or resource values in the saved scan message. */
function xPartialResponseError(error?: unknown, fallback?: string): Error {
  const problem = error && typeof error === "object" ? (error as Record<string, unknown>) : {};
  const code =
    typeof problem.type === "string"
      ? /^https:\/\/api\.(?:x|twitter)\.com\/2\/problems\/([a-z-]{1,48})$/.exec(problem.type)?.[1]
      : null;
  const parameter = [
    "referenced_tweets.id",
    "referenced_tweets.id.author_id",
    "attachments.media_keys",
    "ids",
    "author_id",
    "since_id",
    "start_time",
    "pagination_token",
    "max_results",
    "tweet.fields",
    "media.fields",
    "user.fields",
    "expansions",
  ].includes(String(problem.parameter))
    ? String(problem.parameter)
    : null;
  const reason = code ? [code, parameter].filter(Boolean).join("; ") : fallback;
  return new Error(`X החזיר סריקה חלקית${reason ? ` (${reason})` : ""}; לא קודמה נקודת הסריקה.`);
}

// X Snowflake IDs encode milliseconds since 2010-11-04 in their upper bits.
// since_id is exclusive; subtract one to include every post at the cutoff.
function xTimeBoundary(at: number): bigint {
  return ((BigInt(at) - 1288834974657n) << 22n) - 1n;
}

/** Only a new account starts at yesterday's midnight in Israel. Saved bounds
 * remain fixed across quota days and interruptions, including DST changes. */
function xInitialStart(scanStartedAt: string): string {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Jerusalem",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
  const wallTime = (at: number) => {
    const p = Object.fromEntries(
      formatter.formatToParts(at).map(({ type, value }) => [type, value]),
    );
    return Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second);
  };
  const today = new Date(wallTime(Date.parse(scanStartedAt)));
  const midnight = Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() - 1);
  let instant = midnight;
  for (let attempt = 0; attempt < 3; attempt++) {
    const delta = midnight - wallTime(instant);
    if (!delta) return new Date(instant).toISOString();
    instant += delta;
  }
  throw new Error("לא ניתן לאמת את תחילת חלון X; לא תישלח בקשת איסוף.");
}

// Approved account-specific ceilings. Unknown accounts require an explicit policy.
const X_READ_LIMITS: Record<string, { scan: number; day: number }> = {
  tedpillows: { scan: 25, day: 200 },
  joeconsorti: { scan: 10, day: 20 },
  dor_ophir: { scan: 5, day: 20 },
  lynaldencontact: { scan: 30, day: 30 },
  wublockchain: { scan: 30, day: 30 },
  lookonchain: { scan: 10, day: 10 },
  coinglass_com: { scan: 10, day: 10 },
  glassnode: { scan: 10, day: 10 },
  wclemente: { scan: 5, day: 5 },
  ja_maartun: { scan: 5, day: 5 },
  velo_xyz: { scan: 5, day: 5 },
  cryptoquant_com: { scan: 5, day: 5 },
  vetlelunde: { scan: 5, day: 5 },
};
const xReadDay = () =>
  new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Jerusalem",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(Date.now()));
function xIncomplete(
  cursor: SourceCursor,
  saved: NonNullable<SourceCursor["x_pending"]>,
  message: string,
): SourceScan {
  return {
    items: saved.items,
    cursor: { ...cursor, x_pending: saved },
    // Incomplete pagination is not evidence that the time window was covered.
    publicationHistory: { items: saved.publicationHistory?.items ?? [] },
    pending: [message],
  };
}

async function xScan(
  url: URL,
  cursor: SourceCursor,
  scanStartedAt: string,
  options: SourceScanOptions,
): Promise<SourceScan> {
  const { deadline, onXCheckpoint } = options;
  const pending = cursor.x_pending;
  if (pending) {
    const continuation = pending.pagination;
    if (
      !Array.isArray(pending.items) ||
      !pending.cursor ||
      "x_pending" in pending.cursor ||
      !date(pending.cursor.scanned_at) ||
      (pending.publicationHistory && !Array.isArray(pending.publicationHistory.items)) ||
      (continuation &&
        (typeof continuation.coverage_proven !== "boolean" ||
          Boolean(continuation.since_id) === Boolean(continuation.start_time) ||
          (continuation.since_id !== undefined && !/^\d+$/.test(continuation.since_id)) ||
          (continuation.start_time !== undefined && !date(continuation.start_time)) ||
          (continuation.until_id !== undefined && !/^\d+$/.test(continuation.until_id)) ||
          (continuation.next_token !== undefined &&
            (typeof continuation.next_token !== "string" || !continuation.next_token))))
    )
      throw new Error("מצב איסוף X השמור אינו תקין; לא תיפתח סריקה חוזרת.");
    // Downloaded input is durable work, not a new time window. Age cannot
    // discard cached summaries after an interruption. Unread pages are bounded below.
    if (!continuation) return pending;
    if (pending.items.length && !pending.request)
      return xIncomplete(
        cursor,
        pending,
        "הפוסטים שנאספו ממתינים לסיכום; יתרת האיסוף תמשיך בסריקה הבאה.",
      );
  }
  const progress = pending?.cursor ?? cursor;
  if (
    (progress.since_id !== undefined && !/^\d+$/.test(progress.since_id)) ||
    (progress.scanned_at !== undefined && !date(progress.scanned_at)) ||
    (progress.x_window_started_at !== undefined && !date(progress.x_window_started_at)) ||
    (!pending && progress.x_window_started_at && !progress.since_id && !progress.scanned_at)
  )
    throw new Error("מצב איסוף X השמור אינו תקין; לא תיפתח סריקה חוזרת.");
  const scanEnd = pending?.cursor.scanned_at ?? scanStartedAt;
  const priorScan = date(cursor.scanned_at);
  const previousBoundary = pending?.pagination?.since_id ?? cursor.since_id;
  const legacyStart = pending?.pagination?.start_time;
  const savedStart =
    progress.x_window_started_at ??
    legacyStart ??
    (previousBoundary
      ? new Date(Number(BigInt(previousBoundary) >> 22n) + 1288834974657).toISOString()
      : (priorScan ?? xInitialStart(scanStartedAt)));
  const replaying = Boolean(pending?.request?.response);
  const initialStart = savedStart;
  const timeStart = legacyStart ?? (!previousBoundary ? (priorScan ?? initialStart) : null);
  const savedBoundary = previousBoundary ?? xTimeBoundary(Date.parse(timeStart!)).toString();
  const sinceId = savedBoundary;
  // Tokens belong to their original query. Only legacy start_time checkpoints
  // need conversion; preserve their lower bound and resume below saved posts.
  let pagination = pending?.pagination?.next_token;
  let untilId = pending?.pagination?.until_id;
  if (pagination && !replaying && Date.parse(scanEnd) > Date.parse(initialStart) && legacyStart) {
    const ids = (pending?.publicationHistory?.items ?? [])
      .map((item) => /^x:timeline:(\d+)$/.exec(item.key)?.[1])
      .filter((id): id is string => Boolean(id));
    if (!ids.length)
      throw new Error(
        "לא ניתן לאמת את גבול העמוד שנשמר ב־X; לא תישלח בקשה שעלולה למשוך פוסטים שוב.",
      );
    untilId = ids.reduce((oldest, id) => (BigInt(id) < BigInt(oldest) ? id : oldest));
    pagination = undefined;
  }
  const recent = (item: { publishedAt: string }) => {
    const at = Date.parse(item.publishedAt);
    return at >= Date.parse(initialStart) && at <= Date.parse(scanEnd);
  };
  const status = url.pathname.match(/\/status\/(\d+)/)?.[1];
  if (
    status &&
    (BigInt(status) <= xTimeBoundary(Date.parse(initialStart)) ||
      (cursor.since_id && BigInt(status) <= BigInt(cursor.since_id)))
  ) {
    const saved = {
      items: [],
      cursor: pending?.cursor ?? { ...cursor, scanned_at: scanStartedAt },
    };
    await onXCheckpoint?.(saved);
    return saved;
  }
  const token = process.env.X_BEARER_TOKEN;
  const username = url.pathname.split("/").filter(Boolean)[0];
  if (!username || !/^[a-zA-Z0-9_]{1,15}$/.test(username))
    throw new Error("נדרש קישור לחשבון X או לציוץ.");
  const limits = Object.hasOwn(X_READ_LIMITS, username.toLowerCase())
    ? X_READ_LIMITS[username.toLowerCase()]
    : undefined;
  if (!limits) throw new Error("לחשבון X זה לא הוגדרה מכסת איסוף; לא נשלחה בקשה בתשלום.");
  let usage = progress.x_daily ?? { day: xReadDay(), used: 0 };
  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(usage.day) ||
    !Number.isFinite(Date.parse(`${usage.day}T00:00:00Z`)) ||
    new Date(`${usage.day}T00:00:00Z`).toISOString().slice(0, 10) !== usage.day ||
    !Number.isSafeInteger(usage.used) ||
    usage.used < 0 ||
    usage.day > xReadDay()
  )
    throw new Error("מכסת X השמורה אינה תקינה; האיסוף נעצר ללא איפוס.");
  let paidRequest = pending?.request;
  if (
    paidRequest &&
    (!progress.x_daily ||
      !paidRequest.response ||
      !Number.isSafeInteger(paidRequest.reserved) ||
      paidRequest.reserved < 1 ||
      paidRequest.reserved > 100)
  )
    throw new Error(
      "תוצאת בקשת X הקודמת אינה שמורה בוודאות; נדרש בירור לפני איסוף נוסף כדי למנוע חיוב כפול.",
    );
  if (usage.day !== xReadDay()) usage = { day: xReadDay(), used: 0 };
  const minimum = status ? 1 : 5;
  if (!paidRequest && limits.day - usage.used < minimum) {
    const message = `מכסת האיסוף היומית של @${username} (${limits.day} פוסטים) נוצלה; איסוף נוסף ימתין ליום הבא.`;
    return pending
      ? xIncomplete(cursor, pending, message)
      : { items: [], cursor: { ...cursor, x_daily: usage }, pending: [message] };
  }
  if (!token && !paidRequest) throw new Error("סריקת X דורשת חיבור X API בשרת (X_BEARER_TOKEN).");
  if (!onXCheckpoint) throw new Error("סריקת X מחייבת שמירת התקדמות לפני בקשה בתשלום.");
  const headers = { Authorization: `Bearer ${token}` };
  const signal = AbortSignal.timeout(
    Math.max(1, Math.min(135_000, (deadline ?? Date.now() + 135_000) - Date.now())),
  );
  const read = async (endpoint: string, maxBytes: number) => {
    if (deadline !== undefined && Date.now() >= deadline)
      throw new Error("סריקת X חרגה מהזמן שהוקצה; נקודת הסריקה לא קודמה.");
    return JSON.parse(
      (await sourceRequest(endpoint, maxBytes, headers, 0, signal)).bytes.toString(),
    );
  };
  const savedCoverageStart = pending
    ? pending.publicationHistory?.completeSince
    : (priorScan ?? (cursor.since_id ? undefined : initialStart));
  const coverageStart = savedCoverageStart
    ? new Date(Math.max(Date.parse(savedCoverageStart), Date.parse(initialStart))).toISOString()
    : undefined;
  const items: SourceItem[] = [...(pending?.items ?? [])];
  const publications = new Map(
    (pending?.publicationHistory?.items ?? []).map((item) => [item.key, item]),
  );
  let newest = pending?.cursor.since_id ?? cursor.since_id;
  let coverageProven = pending?.pagination?.coverage_proven ?? !status;
  const savedUser = pending?.cursor ?? cursor;
  const checkedAt = date(savedUser.x_user_checked_at);
  const cacheAge = checkedAt ? Date.parse(scanStartedAt) - Date.parse(checkedAt) : Infinity;
  let userId = savedUser.x_user_id;
  let userCheckedAt = checkedAt;
  let readThisScan = 0;
  const checkpoint = async (complete = false) => {
    const snapshot: NonNullable<SourceCursor["x_pending"]> = {
      items: [...items],
      cursor: {
        since_id: newest,
        scanned_at: scanEnd,
        x_user_id: userId,
        x_user_checked_at: userCheckedAt ?? undefined,
        x_window_started_at: initialStart,
        x_daily: { ...usage },
      },
      ...(paidRequest ? { request: paidRequest } : {}),
      ...(!complete
        ? {
            pagination: {
              ...(replaying && legacyStart ? { start_time: legacyStart } : { since_id: sinceId }),
              ...(untilId ? { until_id: untilId } : {}),
              ...(pagination ? { next_token: pagination } : {}),
              coverage_proven: coverageProven,
            },
          }
        : {}),
      publicationHistory: {
        items: [...publications.values()],
        ...(coverageProven && coverageStart && Date.parse(coverageStart) <= Date.parse(scanEnd)
          ? { completeSince: coverageStart }
          : {}),
      },
    };
    await onXCheckpoint(snapshot);
    return snapshot;
  };
  // Persist the original bounds before provider work, including delayed retries.
  if (
    !paidRequest &&
    (Date.parse(scanEnd) <= Date.parse(initialStart) ||
      (untilId && BigInt(untilId) <= BigInt(sinceId)))
  ) {
    const saved = await checkpoint(true);
    return {
      items: saved.items,
      cursor: saved.cursor,
      publicationHistory: saved.publicationHistory,
    };
  }
  await checkpoint();
  // Revalidate daily so renamed/reassigned handles cannot stay cached indefinitely.
  // Only a downloaded page pins the account ID to its continuation token.
  if (
    !paidRequest &&
    (!userId ||
      !/^[1-9][0-9]{0,19}$/.test(userId) ||
      (!pending?.pagination?.next_token && (cacheAge < 0 || cacheAge >= 24 * 3600_000)))
  ) {
    const user = await read(`https://api.x.com/2/users/by/username/${username}`, 256 * 1024);
    if (typeof user.data?.id !== "string" || !/^[1-9][0-9]{0,19}$/.test(user.data.id))
      throw new Error("חשבון X אינו נגיש.");
    if (userId && userId !== user.data.id)
      throw new Error("זהות חשבון X השתנתה; ההתקדמות והמכסה לא יאופסו אוטומטית.");
    userId = user.data.id;
    userCheckedAt = scanStartedAt;
  }
  for (let page = 0; page < 10; page++) {
    const priorPublicationCount = publications.size;
    if (usage.day !== xReadDay()) usage = { day: xReadDay(), used: 0 };
    const allowance = Math.min(
      status ? 1 : 100,
      limits.scan - readThisScan,
      limits.day - usage.used,
    );
    if (!paidRequest && allowance < minimum) {
      const saved = await checkpoint();
      return xIncomplete(
        cursor,
        saved,
        limits.day - usage.used < minimum
          ? `מכסת האיסוף היומית של @${username} (${limits.day} פוסטים) נוצלה; ההמשך נשמר ליום הבא.`
          : `הושגה מכסת הסריקה של @${username} (${limits.scan} פוסטים); ההמשך נשמר לסריקה הבאה.`,
      );
    }
    const endpoint = new URL(
      status
        ? `https://api.x.com/2/tweets/${status}`
        : `https://api.x.com/2/users/${userId}/tweets`,
    );
    endpoint.searchParams.set(
      "tweet.fields",
      "created_at,attachments,note_tweet,referenced_tweets,author_id",
    );
    endpoint.searchParams.set(
      "expansions",
      "attachments.media_keys,author_id,referenced_tweets.id,referenced_tweets.id.author_id",
    );
    endpoint.searchParams.set("media.fields", "type,url,preview_image_url,alt_text");
    endpoint.searchParams.set("user.fields", "username");
    if (!status) {
      endpoint.searchParams.set("max_results", String(allowance));
      endpoint.searchParams.set("since_id", sinceId);
      if (untilId) endpoint.searchParams.set("until_id", untilId);
      endpoint.searchParams.set("end_time", scanEnd);
      if (pagination) endpoint.searchParams.set("pagination_token", pagination);
    }
    if (!paidRequest) {
      if (!token) throw new Error("סריקת X דורשת חיבור X API בשרת (X_BEARER_TOKEN).");
      // If the request or its save has an unknown outcome, the persisted reservation
      // blocks automatic re-purchase. Never reset it merely because a new day began.
      paidRequest = { day: usage.day, reserved: allowance };
      usage = { ...usage, used: usage.used + allowance };
      await checkpoint();
      let response: XResponse;
      try {
        response = await read(endpoint.href, 2 * 1024 * 1024);
      } catch (error) {
        // An explicit rejected request returned no posts. Unknown delivery or
        // an interrupted successful response remains reserved for diagnosis.
        if (
          error instanceof SourceHttpError &&
          [400, 401, 402, 403, 404, 422, 429].includes(error.status)
        ) {
          usage = { ...usage, used: usage.used - allowance };
          paidRequest = undefined;
          await checkpoint();
        }
        throw error;
      }
      const returned = Array.isArray(response.data) ? response.data.length : response.data ? 1 : 0;
      usage = { ...usage, used: usage.used - allowance + returned };
      paidRequest = { ...paidRequest, response };
      await checkpoint();
      readThisScan += returned;
    }
    const response = paidRequest.response!;
    const posts = Array.isArray(response.data)
      ? response.data
      : response.data
        ? [response.data]
        : [];
    if (!status && !posts.length && response.meta?.result_count !== 0)
      throw xPartialResponseError(undefined, "תגובה ריקה ללא אימות מספר התוצאות");
    if (posts.length > paidRequest.reserved)
      throw xPartialResponseError(undefined, "מספר הפוסטים חורג ממכסת הבקשה");
    if (response.errors?.some((error) => !unavailableXExpansion(error, response, posts)))
      throw xPartialResponseError(
        response.errors.find((error) => !unavailableXExpansion(error, response, posts)),
      );
    if (response.meta?.result_count !== undefined && response.meta.result_count !== posts.length)
      throw xPartialResponseError(undefined, "מספר התוצאות אינו תואם");
    if (response.meta?.result_count !== posts.length) coverageProven = false;
    if (
      response.meta?.next_token !== undefined &&
      (typeof response.meta.next_token !== "string" || !response.meta.next_token)
    )
      throw xPartialResponseError(undefined, "סמן הדף הבא אינו תקין");
    for (const tweet of posts) {
      if (!tweet || typeof tweet.id !== "string" || !/^\d+$/.test(tweet.id))
        throw new Error("X החזיר ציוץ ללא מזהה תקין; לא קודמה נקודת הסריקה.");
      if (tweet.author_id !== userId)
        throw xPartialResponseError(undefined, "מחבר הפוסט אינו החשבון שהתבקש");
      const primaryPublishedAt = date(tweet.created_at);
      if (!primaryPublishedAt) throw new Error("מועד פרסום הציוץ חסר; לא קודמה נקודת הסריקה.");
      if (!recent({ publishedAt: primaryPublishedAt })) continue;
      if (cursor.since_id && BigInt(tweet.id) <= BigInt(cursor.since_id)) continue;
      if (publications.has(`x:timeline:${tweet.id}`)) continue;
      if (!newest || BigInt(tweet.id) > BigInt(newest)) newest = tweet.id;
      // Scheduling learns the account's verified activity before relevance and
      // original-post deduplication. A repost happened now even if its original
      // post was written months ago or is permanently unavailable.
      publications.set(`x:timeline:${tweet.id}`, {
        key: `x:timeline:${tweet.id}`,
        url: `https://x.com/${username}/status/${tweet.id}`,
        publishedAt: primaryPublishedAt,
      });
      const reference = tweet.referenced_tweets?.find((reference: { type: string }) =>
        ["retweeted", "quoted"].includes(reference.type),
      );
      const expandedOriginal = reference
        ? (response.includes?.tweets ?? []).find(
            (entry: { id: string }) => entry.id === reference.id,
          )
        : null;
      const originalAuthor = expandedOriginal
        ? (response.includes?.users ?? []).find((entry) => entry.id === expandedOriginal.author_id)
        : null;
      const original =
        expandedOriginal && (originalAuthor || expandedOriginal.author_id === userId)
          ? expandedOriginal
          : null;
      if (reference && !original) {
        // A quoted post retains the account's own dated commentary. A retweet
        // contains a truncated copy, so skip only that permanently unavailable
        // reference instead of fabricating its full text, author or media.
        const permanentlyUnavailable = response.errors?.some((error) => {
          if (!unavailableXExpansion(error, response, posts)) return false;
          const problem = error as Record<string, unknown>;
          const id = xProblemResourceId(problem);
          return (
            (problem.resource_type === "tweet" && id === reference.id) ||
            (problem.resource_type === "user" &&
              expandedOriginal &&
              id === expandedOriginal.author_id)
          );
        });
        if (!permanentlyUnavailable)
          throw new Error("תוכן הציוץ המקורי חסר; לא קודמה נקודת הסריקה.");
        if (reference.type === "retweeted") continue;
      }
      // Reposts share the original identity; a quote is a new authored post.
      const content = reference?.type === "retweeted" && original ? original : tweet;
      const author = (response.includes?.users ?? []).find(
        (entry: { id: string }) => entry.id === content.author_id,
      );
      const attributedUsername = author?.username ?? username;
      const publishedAt = date(content.created_at);
      if (!publishedAt) throw new Error("מועד פרסום הציוץ חסר.");
      if (!recent({ publishedAt }) || items.some((item) => item.key === `x:status:${content.id}`))
        continue;
      const tweetUrl = `https://x.com/${attributedUsername}/status/${content.id}`;
      const media = (response.includes?.media ?? [])
        .filter(
          (media: { media_key: string; type: string }) =>
            media.type === "photo" &&
            (content.attachments?.media_keys?.includes(media.media_key) ||
              (reference?.type === "quoted" &&
                original?.attachments?.media_keys?.includes(media.media_key))),
        )
        .filter((media: { url: string }) => supportedSourceUrl(media.url))
        .slice(0, 4)
        .map((media: { url: string; alt_text?: string }) => ({
          url: media.url,
          provenance: "post_attachment" as const,
          context: String(media.alt_text ?? "").slice(0, 600),
        }));
      items.push({
        key: `x:status:${content.id}`,
        url: tweetUrl,
        text:
          String(content.note_tweet?.text ?? content.text ?? "") +
          (reference?.type === "quoted" && original
            ? `\nQuoted post by @${originalAuthor?.username ?? username}: ${original.note_tweet?.text ?? original.text ?? ""}`
            : ""),
        sourceName: `@${attributedUsername}`,
        publishedAt,
        images: media.map((image: SourceMedia) => image.url),
        media,
        mediaOrigin: "post",
      });
    }
    const next = response.meta?.next_token;
    if (next && next === pagination)
      throw xPartialResponseError(undefined, "X החזיר שוב את אותו עמוד; לא תישלח בקשה חוזרת");
    if (next && publications.size === priorPublicationCount)
      throw xPartialResponseError(undefined, "לא נמצאה התקדמות בעמוד; לא תישלח בקשה חוזרת");
    pagination = next;
    paidRequest = undefined;
    const complete = Boolean(status) || !pagination;
    // The next page is never purchased before this page is durably saved.
    const saved = await checkpoint(complete);
    if (complete)
      return {
        items: saved.items,
        cursor: saved.cursor,
        publicationHistory: saved.publicationHistory,
      };
    if (replaying)
      return xIncomplete(
        cursor,
        saved,
        "התגובה השמורה שוחזרה ללא איסוף נוסף; הפוסטים ממתינים לסיכום.",
      );
  }
  return xIncomplete(cursor, await checkpoint(), "ההתקדמות נשמרה; יתרת האיסוף תמשיך בסריקה הבאה.");
}

/** Fetch all currently exposed feed entries. Website publication dates must
 * come from the source, never from the language model or the fetch timestamp. */
export async function scanSource(
  raw: string,
  cursor: SourceCursor,
  scanStartedAt: string,
  news = true,
  options: SourceScanOptions = {},
): Promise<SourceScan> {
  const url = new URL(raw);
  if (/(^|\.)(youtube\.com|youtu\.be)$/i.test(url.hostname)) {
    if (!news) throw new Error("סריקת YouTube זמינה בתיבות חדשות בלבד.");
    const { scanYouTubeSource } = await import("./collectionYouTubeSource.server");
    return scanYouTubeSource(raw, cursor, scanStartedAt, options);
  }
  if (/(^|\.)instagram\.com$/i.test(url.hostname)) {
    if (!news) throw new Error("סריקת סטוריז זמינה בתיבות חדשות בלבד.");
    if (!options.workspaceId) throw new Error("נדרשת סביבת עבודה מחוברת לסריקת סטוריז באינסטגרם.");
    // A profile page is not a feed. Only the dedicated authenticated scanner
    // can prove that every active story slide has been read before advancing.
    const { readInstagramStories } = await import("./collectionInstagramBrowser.server");
    return readInstagramStories(raw, cursor, scanStartedAt, { workspaceId: options.workspaceId });
  }
  if (/(^|\.)(x\.com|twitter\.com)$/.test(url.hostname))
    return xScan(url, cursor, scanStartedAt, options);
  const response = await sourceRequest(raw);
  const body = response.bytes.toString("utf8");
  if (!news)
    return {
      items: [],
      cursor: { scanned_at: scanStartedAt },
      pageText: plain(body).slice(0, 50_000),
    };
  let items: SourceItem[] = [];
  let completeFeed = false;
  if (/xml|rss|atom/i.test(response.contentType) || /^\s*<\?xml|<rss\b|<feed\b/i.test(body)) {
    items = feedItems(body, response.url);
    completeFeed =
      items.length ===
      new DOMParser().parseFromString(body, "text/xml").querySelectorAll("item,entry").length;
  } else {
    const { document } = parseHTML(body);
    const feed = absolute(
      document
        .querySelector('link[type="application/rss+xml"],link[type="application/atom+xml"]')
        ?.getAttribute("href"),
      response.url,
    );
    if (feed) {
      const xml = await sourceRequest(feed);
      items = feedItems(xml.bytes.toString(), feed);
      completeFeed =
        items.length ===
        new DOMParser()
          .parseFromString(xml.bytes.toString(), "text/xml")
          .querySelectorAll("item,entry").length;
    } else {
      const own = articleItem(body, response.url);
      if (own) items.push(own);
      const links = [...document.querySelectorAll("article a[href],main a[href]")]
        .map((node) => absolute(node.getAttribute("href"), response.url))
        .filter(
          (link): link is string =>
            !!link && new URL(link).hostname === url.hostname && link !== response.url,
        );
      const unique = [...new Set(links)];
      if (unique.length > 80)
        throw new Error("עמוד המקור גדול מדי; הוסיפו קישור RSS ממוקד כדי לסרוק את כל העדכונים.");
      for (const link of unique) {
        const article = await sourceRequest(link);
        const item = articleItem(article.bytes.toString(), article.url);
        if (item) items.push(item);
      }
    }
  }
  if (!items.length)
    throw new Error("לא נמצאו במקור אייטמים עם מועד פרסום מאומת. נדרש RSS או מקור נתונים נגיש.");
  const publications = [...new Map(items.map((item) => [item.key, item])).values()]
    .filter((item) => Date.parse(item.publishedAt) <= Date.parse(scanStartedAt))
    .map(({ key, url, publishedAt }) => ({ key, url, publishedAt }));
  const completeSince =
    completeFeed && publications.length
      ? new Date(
          Math.min(...publications.map((item) => Date.parse(item.publishedAt))),
        ).toISOString()
      : undefined;
  const cutoff = Date.parse(
    cursor.scanned_at ?? new Date(Date.parse(scanStartedAt) - 48 * 3600_000).toISOString(),
  );
  // Inclusive boundary plus durable dedup protects articles at an exact watermark.
  items = [...new Map(items.map((item) => [item.key, item])).values()].filter(
    (item) =>
      Date.parse(item.publishedAt) >= cutoff &&
      Date.parse(item.publishedAt) <= Date.parse(scanStartedAt),
  );
  return {
    items: items.sort((a, b) => a.publishedAt.localeCompare(b.publishedAt)),
    cursor: { scanned_at: scanStartedAt },
    publicationHistory: { items: publications, ...(completeSince ? { completeSince } : {}) },
  };
}
