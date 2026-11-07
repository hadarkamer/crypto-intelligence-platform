import { safeRichTextImageSrc } from "./richtext";

// Both storage buckets issue one-hour URLs. Refresh before their expiry rather
// than keeping a signed URL for the lifetime of an open collection sheet.
export const RICH_TEXT_IMAGE_REFRESH_MS = 50 * 60 * 1000;

/** Imported news media is 30% smaller; manual uploads keep their chosen size. */
export function automaticNewsImageWidth(image: HTMLElement): string | undefined {
  const path = image.getAttribute("data-path") ?? "";
  if (!/^workspace-images:[^/]+\/collection-autofill\//.test(path)) return undefined;
  const width = image.style.width || "70%";
  // The marker persists through editor saves, so resize and URL renewal never
  // apply the reduction a second time. A later manual resize remains exact.
  if (image.getAttribute("data-autofill-image-size") === "compact") return width;
  const numeric = width.match(/^(\d+(?:\.\d+)?)(%|px|rem|em|vw)$/);
  return numeric ? `${Math.round(Number(numeric[1]) * 0.7 * 100) / 100}${numeric[2]}` : "49%";
}

export function createRichTextImageUrls(
  resolveUrl: (path: string) => Promise<string>,
  now: () => number = Date.now,
) {
  const urls = new Map<string, { src: string; refreshAt: number }>();
  const pending = new Map<string, Promise<string>>();
  return {
    async get(path: string, refresh = false): Promise<string> {
      const cached = urls.get(path);
      if (!refresh && cached && cached.refreshAt > now()) return cached.src;
      const existing = pending.get(path);
      if (existing) return existing;
      const task = Promise.resolve()
        .then(() => resolveUrl(path))
        .then((raw) => {
          const src = safeRichTextImageSrc(raw);
          if (!src || !/^https?:\/\//i.test(src)) throw new Error("Image URL unavailable");
          urls.set(path, { src, refreshAt: now() + RICH_TEXT_IMAGE_REFRESH_MS });
          return src;
        })
        .catch(() => {
          urls.delete(path);
          throw new Error("Image URL unavailable");
        })
        .finally(() => pending.delete(path));
      pending.set(path, task);
      return task;
    },
  };
}

/** The DOM parser decodes attribute entities, unlike matching stored HTML. */
export function richTextImagePaths(html: string): string[] {
  if (typeof DOMParser === "undefined") return [];
  const document = new DOMParser().parseFromString(html, "text/html");
  return [
    ...new Set(
      Array.from(document.querySelectorAll("img[data-path]"))
        .map((image) => image.getAttribute("data-path"))
        .filter((path): path is string => !!path),
    ),
  ];
}
