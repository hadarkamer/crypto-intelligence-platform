/**
 * פלטת עיצוב סמנטית ל-Rich Text.
 * נשמר class בלבד (לא צבע hex), כדי שהצבע יתאים אוטומטית ל-Light/Dark Mode
 * וכדי שלא ניתן יהיה להזריק CSS חופשי לתוכן.
 */

export type RtColor = { id: string; className: string; label: string };

export const RT_COLORS: RtColor[] = [
  { id: "default", className: "rt-color-default", label: "רגיל" },
  { id: "red", className: "rt-color-red", label: "אדום" },
  { id: "orange", className: "rt-color-orange", label: "כתום" },
  { id: "gold", className: "rt-color-gold", label: "זהב" },
  { id: "green", className: "rt-color-green", label: "ירוק" },
  { id: "blue", className: "rt-color-blue", label: "כחול" },
  { id: "purple", className: "rt-color-purple", label: "סגול" },
  { id: "gray", className: "rt-color-gray", label: "אפור" },
];

export type RtSize = { className: string; label: string };

export const RT_SIZES: RtSize[] = [
  { className: "rt-size-xs", label: "11" },
  { className: "rt-size-sm", label: "13" },
  { className: "rt-size-base", label: "15" },
  { className: "rt-size-lg", label: "18" },
  { className: "rt-size-xl", label: "21" },
  { className: "rt-size-2xl", label: "24" },
  { className: "rt-size-3xl", label: "28" },
  { className: "rt-size-4xl", label: "32" },
  { className: "rt-size-h", label: "כותרת 21" },
];

export const DEFAULT_TEXT_CLASS = "rt-color-default";
export const DEFAULT_SIZE_CLASS = "rt-size-lg";

/** יישור פסקה — נשמר כ-class מאושר בלבד, כדי שהסניטייזר יכבד אותו. */
export type RtAlign = "right" | "left" | "center";
export const RT_ALIGN_CLASS: Record<RtAlign, string> = {
  right: "rt-align-right",
  left: "rt-align-left",
  center: "rt-align-center",
};
export const DEFAULT_ALIGN: RtAlign = "right";
const ALIGN_CLASSES = Object.values(RT_ALIGN_CLASS);
export const isAlignClass = (c: string) => ALIGN_CLASSES.includes(c);
export function alignOfClass(c: string | null | undefined): RtAlign | null {
  const entry = (Object.entries(RT_ALIGN_CLASS) as [RtAlign, string][]).find(
    ([, cls]) => cls === c,
  );
  return entry ? entry[0] : null;
}
export const normalizeAlign = (v: string | null | undefined): RtAlign =>
  v === "left" ? "left" : v === "center" ? "center" : "right";

const COLOR_CLASSES = RT_COLORS.map((c) => c.className);
const SIZE_CLASSES = RT_SIZES.map((s) => s.className);

/** כל ה-classes המותרים בתוכן שמור. כל class אחר נזרק ברינדור. */
export const ALLOWED_RT_CLASSES = new Set<string>([
  ...COLOR_CLASSES,
  ...SIZE_CLASSES,
  ...ALIGN_CLASSES,
  "rt-news-summary",
  "rt-news-date",
]);

export const isColorClass = (c: string) => COLOR_CLASSES.includes(c);
export const isSizeClass = (c: string) => SIZE_CLASSES.includes(c);

export function colorLabel(className: string | null | undefined) {
  return RT_COLORS.find((c) => c.className === className)?.label ?? "רגיל";
}

export function sizeLabel(className: string | null | undefined) {
  return RT_SIZES.find((s) => s.className === className)?.label ?? "רגיל";
}

/** מסנן רשימת classes ומשאיר רק את המאושרים. */
export function sanitizeRtClass(raw: string | null | undefined): string {
  if (!raw) return "";
  return raw
    .split(/\s+/)
    .filter((c) => ALLOWED_RT_CLASSES.has(c))
    .join(" ");
}

function escapeHtml(s: string) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

/** טקסט ישן שאינו HTML מוצג כטקסט רגיל עם שורות חדשות. */
/**
 * תוכן ישן עשוי להכיל גם ישויות HTML (&nbsp; &#160; &#xa0;) בלי תגיות כלל.
 * אם נבצע להן escape הן יוצגו כטקסט מילולי, ולכן הן נחשבות תוכן HTML.
 * הסניטייזר ב-RichTextView נשאר הסמכות היחידה לגבי מה מותר להציג.
 */
const HTML_ENTITY = /&(?:[a-z][a-z0-9]{1,31}|#\d{1,7}|#x[0-9a-f]{1,6});/i;

export function toHtml(value: string) {
  if (!value) return "";
  const looksLikeHtml = /<(img|br|div|p|span|b|i|u|a)\b/i.test(value) || HTML_ENTITY.test(value);
  if (looksLikeHtml) return value;
  return escapeHtml(value).replace(/\n/g, "<br>");
}

/** Supported display elements; attributes are still filtered by RichTextView. */
export const ALLOWED_RICH_TEXT_TAGS = new Set([
  "DIV",
  "P",
  "BR",
  "B",
  "STRONG",
  "I",
  "EM",
  "U",
  "S",
  "UL",
  "OL",
  "LI",
  "SPAN",
  "IMG",
  "A",
  "H1",
  "H2",
  "H3",
  "BLOCKQUOTE",
  "CODE",
  "PRE",
  "FONT",
]);

/** Image sources are deliberately narrower than link URLs: no SVG, HTML or script data. */
export function safeRichTextImageSrc(raw: string | null): string | null {
  if (typeof raw !== "string" || !raw) return null;
  const value = raw.trim();
  if (/^https?:\/\//i.test(value)) {
    for (const character of value) {
      const code = character.charCodeAt(0);
      if (code <= 0x20 || code === 0x7f) return null;
    }
    try {
      const url = new URL(value);
      return url.hostname && !url.username && !url.password ? url.href : null;
    } catch {
      return null;
    }
  }
  // Older worksheets embedded raster images before storage paths were introduced.
  // Permit only base64 raster payloads, never arbitrary data: URLs.
  const match = /^data:image\/(png|jpe?g|gif|webp|avif|bmp);base64,([a-z0-9+/=\r\n\t ]+)$/i.exec(
    value,
  );
  if (!match) return null;
  const payload = match[2].replace(/\s/g, "");
  if (!/^[a-z0-9+/]+={0,2}$/i.test(payload)) return null;
  return `data:image/${match[1].toLowerCase()};base64,${payload}`;
}

// Only recover recognizable editor markup, never arbitrary angle brackets in prose.
const EDITOR_MARKUP =
  /<(?:span\s+[^>]*\bclass\s*=\s*["'][^"']*\brt-(?:color|size|align)-|img\s+[^>]*(?:src|data-path)\s*=)/i;
const ESCAPED_EDITOR_MARKUP = /&(?:amp;)*(?:lt;|#0*60;|#x0*3c;)(?:span\s+[^>]*\bclass\s*=|img\s+)/i;

function repairEditorFragments(value: string): string {
  // A few legacy strings lost the opening '<' of generated color spans. The
  // strict class signature and matching end tag keep ordinary quotations intact.
  if (!/<\/\s*span\s*>/i.test(value)) return value;
  return value
    .replace(
      /(^|[>\s])(?<!<)(span\s+class\s*=\s*(["'])rt-(?:color|size|align)-[a-z-]+\3\s*>)/gi,
      "$1<$2",
    )
    .replace(/<\/\s+span\s*>/gi, "</span>");
}

/**
 * Recover escaped legacy editor markup for display only. This is not a sanitizer;
 * RichTextView still enforces the element/attribute/URL allowlists afterwards.
 * Do not use it to inject HTML into the editable field.
 */
export function normalizeRichTextHtml(value: string): string {
  const repaired = repairEditorFragments(value);
  const html = /<(strong|em|s|ul|ol|li|a|h[123]|blockquote|code|pre|font)\b/i.test(repaired)
    ? repaired
    : toHtml(repaired);
  if (typeof DOMParser === "undefined") return html;
  const doc = new DOMParser().parseFromString(html, "text/html");
  const recover = (parent: Node, depth: number) => {
    if (depth >= 4) return;
    for (const node of Array.from(parent.childNodes)) {
      if (node.nodeType === 1) {
        const element = node as Element;
        if (element.tagName !== "CODE" && element.tagName !== "PRE") recover(element, depth);
        continue;
      }
      if (node.nodeType !== 3) continue;
      const text = node.textContent ?? "";
      const repaired = repairEditorFragments(text);
      if (!EDITOR_MARKUP.test(repaired) && !ESCAPED_EDITOR_MARKUP.test(repaired)) continue;
      const fragment = new DOMParser().parseFromString(repaired, "text/html").body;
      recover(fragment, depth + 1);
      node.replaceWith(...Array.from(fragment.childNodes));
    }
  };
  recover(doc.body, 0);
  return doc.body.innerHTML;
}

/** Human-readable chart labels, without image data or HTML attributes. */
export function richTextToPlain(value: string): string {
  const html = normalizeRichTextHtml(value);
  if (typeof DOMParser === "undefined") {
    // SSR has no DOM parser. Do not leak image payloads into labels while waiting
    // for the browser render; quoted attributes may themselves contain '>'.
    const decoded = html.replace(
      /&(?:amp;)*(lt|gt|quot|apos|nbsp);/gi,
      (_, name: string) =>
        ({ lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " " })[name.toLowerCase()] ?? "",
    );
    const hasImage = /<img\b/i.test(decoded);
    const text = repairEditorFragments(decoded)
      .replace(/<(script|style|iframe)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, "")
      .replace(/<(?:[^"'<>]|"[^"]*"|'[^']*')*>/g, " ")
      .replace(/&amp;/gi, "&")
      .replace(/[\u200B-\u200D\uFEFF]/g, "")
      .replace(/\s+/g, " ")
      .trim();
    return text || (hasImage ? "תמונה" : "");
  }
  const doc = new DOMParser().parseFromString(html, "text/html");
  let hasImage = false;
  const read = (node: Node): string => {
    if (node.nodeType === 3) return node.textContent ?? "";
    if (node.nodeType !== 1) return "";
    const el = node as Element;
    if (!ALLOWED_RICH_TEXT_TAGS.has(el.tagName)) return "";
    if (el.tagName === "IMG") {
      hasImage ||= !!el.getAttribute("data-path") || !!safeRichTextImageSrc(el.getAttribute("src"));
      return "";
    }
    const content = Array.from(el.childNodes).map(read).join("");
    return /^(DIV|P|BR|LI|H[123]|BLOCKQUOTE|PRE)$/.test(el.tagName) ? ` ${content} ` : content;
  };
  const text = Array.from(doc.body.childNodes)
    .map(read)
    .join("")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  return text || (hasImage ? "תמונה" : "");
}
