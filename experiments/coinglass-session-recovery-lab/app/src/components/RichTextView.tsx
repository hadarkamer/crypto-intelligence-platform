import { useEffect, useMemo, useRef, useState } from "react";
import type { ElementType, ReactNode } from "react";
import { signedImageUrl } from "@/lib/api";
import { useNewsLanguage } from "@/hooks/useNewsLanguage";
import { projectCollectionNewsBody } from "@/lib/collectionNewsLanguage";
import {
  ALLOWED_RICH_TEXT_TAGS,
  normalizeRichTextHtml,
  richTextToPlain,
  safeRichTextImageSrc,
} from "@/lib/richtext";
import { sanitizeRtClass, RT_ALIGN_CLASS, type RtAlign } from "@/lib/richtext";
import {
  automaticNewsImageWidth,
  createRichTextImageUrls,
  richTextImagePaths,
  RICH_TEXT_IMAGE_REFRESH_MS,
} from "@/lib/richtextImages";

/**
 * מציג תוכן Rich Text (HTML) בצורה בטוחה:
 * - התוכן עובר סינון (allow-list) ומומר לאלמנטים של React, ללא dangerouslySetInnerHTML.
 * - תמונות נשמרות כ־data-path בלבד, ולכן הן מומרות כאן ל־URL חתום ומוצגות כתמונה.
 */

function safeHref(raw: string | null): string | null {
  if (!raw) return null;
  const v = raw.trim();
  return /^(https?:|mailto:)/i.test(v) ? v : null;
}

export function RichTextView({
  value,
  imageMode = "inline",
  className,
  align,
}: {
  value: string | null | undefined;
  imageMode?: "inline" | "thumb";
  className?: string;
  /** יישור ברירת מחדל; בלוקים עם יישור מפורש גוברים עליו. */
  align?: RtAlign;
}) {
  const { language } = useNewsLanguage();
  const html = useMemo(
    () => normalizeRichTextHtml(projectCollectionNewsBody((value ?? "").trim(), language)),
    [value, language],
  );
  const paths = useMemo(() => richTextImagePaths(html), [html]);
  const imageUrls = useMemo(() => createRichTextImageUrls(signedImageUrl), []);
  const [urls, setUrls] = useState<Record<string, string>>({});
  const [imageVersions, setImageVersions] = useState<Record<string, number>>({});
  const [failed, setFailed] = useState<Set<string>>(new Set());
  const retried = useRef(new Set<string>());
  const loadImage = useRef<((path: string, refresh: boolean) => Promise<void>) | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async (path: string, refresh: boolean) => {
      try {
        const src = await imageUrls.get(path, refresh);
        if (cancelled) return;
        setUrls((prev) => ({ ...prev, [path]: src }));
        // Signing again in the same second can return the identical URL. A new
        // image node still needs to retry the failed browser request once.
        if (refresh) setImageVersions((prev) => ({ ...prev, [path]: (prev[path] ?? 0) + 1 }));
        setFailed((prev) => {
          const next = new Set(prev);
          next.delete(path);
          return next;
        });
      } catch {
        if (!cancelled) setFailed((prev) => new Set(prev).add(path));
      }
    };
    loadImage.current = load;
    const refresh = () => {
      for (const path of paths) void load(path, false);
    };
    refresh();
    const timer = window.setInterval(refresh, RICH_TEXT_IMAGE_REFRESH_MS);
    window.addEventListener("focus", refresh);
    document.addEventListener("visibilitychange", refresh);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
      window.removeEventListener("focus", refresh);
      document.removeEventListener("visibilitychange", refresh);
      if (loadImage.current === load) loadImage.current = null;
    };
  }, [paths, imageUrls]);

  const retry = (path: string) => {
    retried.current.delete(path);
    void loadImage.current?.(path, true);
  };
  const imageError = (path: string) => {
    if (retried.current.has(path)) setFailed((prev) => new Set(prev).add(path));
    else {
      retried.current.add(path);
      void loadImage.current?.(path, true);
    }
  };
  const nodes = useRenderedNodes(
    html,
    urls,
    imageVersions,
    imageMode,
    failed,
    imageError,
    retry,
    (path) => retried.current.delete(path),
  );
  if (!html) return null;
  return (
    <div
      className={`rt-richtext min-w-0 space-y-1 break-words leading-relaxed [overflow-wrap:anywhere] ${
        align ? RT_ALIGN_CLASS[align] : ""
      } ${className ?? ""}`}
    >
      {nodes}
    </div>
  );
}

function useRenderedNodes(
  html: string,
  urls: Record<string, string>,
  imageVersions: Record<string, number>,
  imageMode: "inline" | "thumb",
  failed: Set<string>,
  imageError: (path: string) => void,
  retry: (path: string) => void,
  imageLoaded: (path: string) => void,
) {
  const keyRef = useRef(0);
  return useMemo(() => {
    if (!html) return null;
    if (typeof window === "undefined" || typeof DOMParser === "undefined") {
      // בצד השרת מוצג טקסט בלבד, כדי לא לרנדר HTML ללא סינון
      return richTextToPlain(html);
    }
    keyRef.current = 0;
    const doc = new DOMParser().parseFromString(`<div>${html}</div>`, "text/html");
    const root = doc.body.firstElementChild;
    if (!root) return null;

    const render = (node: Node): ReactNode => {
      // תווי אפס-רוחב משאריות עריכה ישנות אינם חלק מהתוכן
      if (node.nodeType === Node.TEXT_NODE)
        return (node.textContent ?? "").replace(/\u200B|\u200C|\u200D|\uFEFF/g, "");
      if (node.nodeType !== Node.ELEMENT_NODE) return null;
      const el = node as HTMLElement;
      const tag = el.tagName.toUpperCase();
      if (!ALLOWED_RICH_TEXT_TAGS.has(tag)) return null;
      const key = `n${keyRef.current++}`;

      if (tag === "BR") return <br key={key} />;

      if (tag === "IMG") {
        const path = el.getAttribute("data-path");
        const src = path ? urls[path] : safeRichTextImageSrc(el.getAttribute("src"));
        if (path && failed.has(path))
          return (
            <span key={key} className="my-2 block text-xs text-muted-foreground" role="status">
              התמונה לא נטענה.{" "}
              <button type="button" className="underline" onClick={() => retry(path)}>
                נסה שוב
              </button>
            </span>
          );
        if (!src) {
          return path ? (
            <span key={key} className="text-xs text-muted-foreground">
              טוען תמונה…
            </span>
          ) : null;
        }
        const alt = el.getAttribute("alt") || "תמונה מהיומן";
        const automaticWidth = automaticNewsImageWidth(el);
        const width =
          imageMode === "inline" ? (automaticWidth ?? (el.style.width || "70%")) : undefined;
        return (
          <a key={key} href={src} target="_blank" rel="noreferrer" className="block">
            <img
              key={path ? `${path}:${imageVersions[path] ?? 0}` : undefined}
              src={src}
              alt={alt}
              loading="lazy"
              referrerPolicy="no-referrer"
              onError={path ? () => imageError(path) : undefined}
              onLoad={path ? () => imageLoaded(path) : undefined}
              style={imageMode === "inline" ? { width } : undefined}
              className={
                imageMode === "inline"
                  ? "my-2 max-w-full rounded-md border border-border"
                  : automaticWidth
                    ? "my-2 h-[4.2rem] max-w-[6.3rem] rounded-md border border-border object-cover"
                    : "my-2 h-24 max-w-[9rem] rounded-md border border-border object-cover"
              }
            />
          </a>
        );
      }

      const children = Array.from(el.childNodes).map(render);

      if (tag === "A") {
        const href = safeHref(el.getAttribute("href"));
        if (!href) return <span key={key}>{children}</span>;
        return (
          <a
            key={key}
            href={href}
            target="_blank"
            rel="noreferrer"
            className={`underline ${sanitizeRtClass(el.getAttribute("class"))}`.trim()}
          >
            {children}
          </a>
        );
      }

      const Tag = tag.toLowerCase() as ElementType;
      const structural =
        tag === "UL"
          ? "list-disc pr-5"
          : tag === "OL"
            ? "list-decimal pr-5"
            : tag === "BLOCKQUOTE"
              ? "border-r-2 border-border pr-3 text-muted-foreground"
              : "";
      // רק classes מהפלטה המאושרת עוברים — כל CSS אחר נזרק
      const allowed = sanitizeRtClass(el.getAttribute("class"));
      const className = [structural, allowed].filter(Boolean).join(" ") || undefined;
      return (
        <Tag
          key={key}
          className={className}
          dir={
            el.getAttribute("dir") === "ltr"
              ? "ltr"
              : el.getAttribute("dir") === "rtl"
                ? "rtl"
                : undefined
          }
        >
          {children.length ? children : null}
        </Tag>
      );
    };

    return Array.from(root.childNodes).map(render);
  }, [html, urls, imageVersions, imageMode, failed, imageError, retry, imageLoaded]);
}

/** מעטפת „הצג עוד / הצג פחות” שאינה חותכת HTML באמצע תגית. */
export function ClampedRichText({
  value,
  imageMode = "thumb",
}: {
  value: string | null | undefined;
  imageMode?: "inline" | "thumb";
}) {
  const [open, setOpen] = useState(false);
  const [clampable, setClampable] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const text = (value ?? "").trim();

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    setClampable(el.scrollHeight > 120);
  }, [text, open]);

  if (!text) return <span className="text-muted-foreground">לא הוזן</span>;

  return (
    <div>
      <div
        ref={ref}
        className={open ? undefined : "max-h-[7.5rem] overflow-hidden"}
        style={open ? undefined : { maskImage: "linear-gradient(180deg,#000 70%,transparent)" }}
      >
        <RichTextView value={text} imageMode={imageMode} />
      </div>
      {(clampable || open) && (
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          className="mt-1 text-xs font-medium text-primary underline-offset-2 hover:underline"
        >
          {open ? "הצג פחות" : "הצג עוד"}
        </button>
      )}
    </div>
  );
}
