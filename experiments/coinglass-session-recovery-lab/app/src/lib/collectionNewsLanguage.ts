export type CollectionNewsLanguage = "he" | "en";
export const NEWS_I18N_ATTRIBUTE = "data-news-i18n";
export const NEWS_SUMMARY_ATTRIBUTE = "data-news-summary";
const MAX_HTML = 24_000;
const MAX_ATTRIBUTE = 120_000;

type Metadata = { version: 1; source_language: "en"; he_html: string; en_html: string };
type Element = {
  tag: string;
  start: number;
  openEnd: number;
  closeStart: number;
  end: number;
  attrs: Map<string, string>;
  parent: Element | null;
};
type Segment = {
  element: Element;
  summary: Element;
  metadata: Metadata;
  encoded: string;
  id: string;
};
const VOID = new Set(["br", "img", "hr"]);
const TAGS = new Set([
  "div",
  "p",
  "br",
  "b",
  "strong",
  "i",
  "em",
  "u",
  "s",
  "ul",
  "ol",
  "li",
  "span",
  "img",
  "a",
  "h1",
  "h2",
  "h3",
  "blockquote",
  "code",
  "pre",
  "font",
]);
const ATTRS = new Set([
  "class",
  "style",
  "dir",
  "align",
  "title",
  "href",
  "target",
  "rel",
  "src",
  "alt",
  "width",
  "height",
  "data-path",
  "data-autofill-image-size",
  "color",
  "size",
  "face",
]);
const decode = (value: string) =>
  value.replace(/&(?:amp|quot|apos|lt|gt|#39|#\d+|#x[\da-f]+);/gi, (entity) => {
    const named: Record<string, string> = {
      "&amp;": "&",
      "&quot;": '"',
      "&apos;": "'",
      "&lt;": "<",
      "&gt;": ">",
      "&#39;": "'",
    };
    if (named[entity.toLowerCase()]) return named[entity.toLowerCase()];
    const number = entity.toLowerCase().startsWith("&#x")
      ? parseInt(entity.slice(3, -1), 16)
      : Number(entity.slice(2, -1));
    return Number.isInteger(number) && number > 0 && number <= 0x10ffff
      ? String.fromCodePoint(number)
      : entity;
  });
const escapeAttribute = (value: string) =>
  value.replace(
    /[&<>"']/g,
    (character) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character]!,
  );

/** A lossless tokenizer: rendering remains the rich-text renderer's responsibility. */
function elements(html: string): Element[] | null {
  const result: Element[] = [];
  const stack: Element[] = [];
  const tokens = /<!--[\s\S]*?-->|<\/?([a-z][\w:-]*)\b(?:"[^"]*"|'[^']*'|[^'">])*>/gi;
  let lastEnd = 0;
  for (const token of html.matchAll(tokens)) {
    if (html.slice(lastEnd, token.index).includes("<")) return null;
    lastEnd = token.index! + token[0].length;
    if (token[0].startsWith("<!--")) continue;
    const tag = token[1].toLowerCase();
    if (token[0].startsWith("</")) {
      const element = stack.pop();
      if (!element || element.tag !== tag) return null;
      element.closeStart = token.index!;
      element.end = token.index! + token[0].length;
      continue;
    }
    const attrs = new Map<string, string>();
    const raw = token[0].slice(1 + token[1].length, token[0].endsWith("/>") ? -2 : -1);
    let cursor = 0;
    const attribute = /\s+([\w:-]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s'"=<>`]+)))?/gy;
    while (cursor < raw.length) {
      if (!raw.slice(cursor).trim()) break;
      attribute.lastIndex = cursor;
      const match = attribute.exec(raw);
      if (!match || attrs.has(match[1].toLowerCase())) return null;
      attrs.set(match[1].toLowerCase(), decode(match[2] ?? match[3] ?? match[4] ?? ""));
      cursor = attribute.lastIndex;
    }
    const element: Element = {
      tag,
      start: token.index!,
      openEnd: token.index! + token[0].length,
      closeStart: token.index! + token[0].length,
      end: token.index! + token[0].length,
      attrs,
      parent: stack.at(-1) ?? null,
    };
    result.push(element);
    if (!VOID.has(tag) && !token[0].endsWith("/>")) stack.push(element);
  }
  return stack.length || html.slice(lastEnd).includes("<") ? null : result;
}

function safeVariantHtml(html: unknown): html is string {
  if (typeof html !== "string" || html.length > MAX_HTML) return false;
  const nodes = elements(html);
  if (!nodes) return false;
  return nodes.every(
    (node) =>
      TAGS.has(node.tag) &&
      [...node.attrs].every(([name, value]) => {
        if (
          !ATTRS.has(name) ||
          value.length > MAX_HTML ||
          [...value].some((character) => {
            const code = character.charCodeAt(0);
            return code < 32 && ![9, 10, 13].includes(code);
          })
        )
          return false;
        if (name === "href" || name === "src") {
          if (
            name === "src" &&
            /^data:image\/(?:png|jpe?g|gif|webp);base64,[a-z\d+/=\s]+$/i.test(value)
          )
            return true;
          try {
            return ["https:", "http:", ...(name === "href" ? ["mailto:"] : [])].includes(
              new URL(value).protocol,
            );
          } catch {
            return false;
          }
        }
        if (name === "style")
          return value
            .split(";")
            .filter((rule) => rule.trim())
            .every(
              (rule) =>
                /^(?:width|height|text-align|font-weight|font-style|text-decoration|color|font-size)\s*:\s*[\w\s.,#%()-]+$/i.test(
                  rule.trim(),
                ) && !/url|expression|var\(/i.test(rule),
            );
        return true;
      }),
  );
}

function metadata(raw: string | undefined): Metadata | null {
  if (!raw || raw.length > MAX_ATTRIBUTE) return null;
  try {
    const value = JSON.parse(decodeURIComponent(raw));
    if (
      !value ||
      value.version !== 1 ||
      value.source_language !== "en" ||
      Object.keys(value).sort().join(",") !== "en_html,he_html,source_language,version" ||
      !safeVariantHtml(value.he_html) ||
      !safeVariantHtml(value.en_html)
    )
      return null;
    return value as Metadata;
  } catch {
    return null;
  }
}

/** Sanitizers may preserve only these bounded, versioned presentation markers. */
export function validCollectionNewsI18nAttribute(raw: string): boolean {
  return metadata(raw) !== null;
}

/** Hebrew source text always remains Hebrew, including mixed Hebrew/market abbreviations. */
export function collectionNewsSourceLanguage(text: string): CollectionNewsLanguage {
  return /[א-ת]/.test(text) || !/[a-z]/i.test(text) ? "he" : "en";
}

function segments(html: string): Segment[] | null {
  const nodes = elements(html);
  if (!nodes) return null;
  const output: Segment[] = [];
  for (const element of nodes.filter((node) => node.attrs.has(NEWS_I18N_ATTRIBUTE))) {
    const encoded = element.attrs.get(NEWS_I18N_ATTRIBUTE)!;
    const value = metadata(encoded);
    const id = element.attrs.get("data-autofill-id");
    const summaryNodes = nodes.filter(
      (node) => node.attrs.has(NEWS_SUMMARY_ATTRIBUTE) && node.parent === element,
    );
    if (
      element.tag !== "div" ||
      !value ||
      !id ||
      id.length > 256 ||
      summaryNodes.length !== 1 ||
      summaryNodes[0].tag !== "div" ||
      summaryNodes[0].attrs.get(NEWS_SUMMARY_ATTRIBUTE) !== "1" ||
      output.some((segment) => segment.id === id)
    )
      return null;
    const summary = summaryNodes[0];
    const inner = html.slice(summary.openEnd, summary.closeStart);
    if (inner !== value.he_html && inner !== value.en_html) return null;
    output.push({ element, summary, metadata: value, encoded, id });
  }
  return output;
}

function replaceParts(
  html: string,
  parts: Array<{ start: number; end: number; value: string }>,
): string {
  let result = html;
  for (const part of parts.sort((a, b) => b.start - a.start))
    result = result.slice(0, part.start) + part.value + result.slice(part.end);
  return result;
}

/** Adds metadata only to verified English-source automatic output, never manual or historic text. */
export function bilingualCollectionNewsBody(hebrewBody: string, englishBody: string): string {
  const he = elements(hebrewBody);
  const en = elements(englishBody);
  const outer = he?.find(
    (node) => node.tag === "div" && node.attrs.has("data-autofill-id") && node.parent === null,
  );
  const other = en?.find(
    (node) =>
      node.tag === "div" &&
      node.attrs.get("data-autofill-id") === outer?.attrs.get("data-autofill-id") &&
      node.parent === null,
  );
  if (!outer || !other) throw new Error("סיכומי השפות אינם תואמים לאייטם המקור.");
  const summaryEnd = (nodes: Element[], owner: Element) =>
    nodes.find(
      (node) =>
        node.parent === owner &&
        node.tag === "p" &&
        nodes.some((child) => child.parent === node && child.tag === "a"),
    )?.start;
  const heEnd = summaryEnd(he!, outer);
  const enEnd = summaryEnd(en!, other);
  if (
    heEnd === undefined ||
    enEnd === undefined ||
    hebrewBody.slice(heEnd, outer.closeStart) !== englishBody.slice(enEnd, other.closeStart)
  )
    throw new Error("תאריך או תמונות המקור שונים בין סיכומי השפות.");
  const value: Metadata = {
    version: 1,
    source_language: "en",
    he_html: hebrewBody.slice(outer.openEnd, heEnd),
    en_html: englishBody.slice(other.openEnd, enEnd),
  };
  const encoded = encodeURIComponent(JSON.stringify(value));
  if (!validCollectionNewsI18nAttribute(encoded)) throw new Error("סיכומי השפות אינם תקינים.");
  return replaceParts(hebrewBody, [
    {
      start: outer.openEnd - 1,
      end: outer.openEnd - 1,
      value: ` ${NEWS_I18N_ATTRIBUTE}="${escapeAttribute(encoded)}"`,
    },
    {
      start: outer.openEnd,
      end: heEnd,
      value: `<div ${NEWS_SUMMARY_ATTRIBUTE}="1">${value.he_html}</div>`,
    },
  ]);
}

/** Pure presentation: calling this never writes a row, a preference or a receipt. */
export function projectCollectionNewsBody(
  canonicalHtml: string,
  language: CollectionNewsLanguage,
): string {
  const found = segments(canonicalHtml);
  if (!found) return canonicalHtml;
  return replaceParts(
    canonicalHtml,
    found.map((segment) => ({
      start: segment.summary.openEnd,
      end: segment.summary.closeStart,
      value: segment.metadata[language === "en" ? "en_html" : "he_html"],
    })),
  );
}

function skeleton(html: string, found: Segment[]): string {
  return replaceParts(
    html,
    found.flatMap((segment) => [
      {
        start: segment.summary.openEnd,
        end: segment.summary.closeStart,
        value: `[[news-summary:${segment.id}]]`,
      },
      {
        start: segment.element.start,
        end: segment.element.openEnd,
        value: html
          .slice(segment.element.start, segment.element.openEnd)
          .replace(/\sdata-news-i18n\s*=\s*(?:"[^"]*"|'[^']*')/i, ""),
      },
    ]),
  );
}

/** Merge the edited language into canonical Hebrew storage without losing the other language.
 * expectedProjectionHtml must be captured when the editor's draft starts. A stale shared
 * body or active variant is rejected; a concurrent edit of the OTHER variant can merge. */
export function mergeCollectionNewsBodyEdit(
  canonicalHtml: string,
  editedHtml: string,
  language: CollectionNewsLanguage,
  expectedProjectionHtml = projectCollectionNewsBody(canonicalHtml, language),
): string {
  const current = segments(canonicalHtml);
  const expected = segments(expectedProjectionHtml);
  // Edited summary text is expected to differ from its stored variant. Extract its
  // nodes separately, then restore old contents only for marker/metadata validation.
  const editedNodes = elements(editedHtml);
  const candidateSummaries =
    editedNodes?.filter((node) => node.attrs.get(NEWS_SUMMARY_ATTRIBUTE) === "1") ?? [];
  const restored = replaceParts(
    editedHtml,
    candidateSummaries.map((node) => {
      const id = node.parent?.attrs.get("data-autofill-id");
      const old = id ? metadata(node.parent?.attrs.get(NEWS_I18N_ATTRIBUTE)) : null;
      return {
        start: node.openEnd,
        end: node.closeStart,
        value:
          old?.[language === "en" ? "en_html" : "he_html"] ??
          editedHtml.slice(node.openEnd, node.closeStart),
      };
    }),
  );
  const edited = segments(restored);
  const fail = () => {
    throw new Error("הידיעה השתנתה או שסימוני השפות נמחקו. טענו מחדש לפני שמירה.");
  };
  if (!current || !expected || !edited || !editedNodes) return fail();
  if (!current.length && !expected.length && !edited.length) return editedHtml;
  if (
    current.length !== expected.length ||
    edited.length !== expected.length ||
    skeleton(canonicalHtml, current) !== skeleton(expectedProjectionHtml, expected)
  )
    return fail();
  const parts: Array<{ start: number; end: number; value: string }> = [];
  for (const old of expected) {
    const latest = current.find((segment) => segment.id === old.id);
    const changed = edited.find((segment) => segment.id === old.id);
    const summary = candidateSummaries.find(
      (node) => node.parent?.attrs.get("data-autofill-id") === old.id,
    );
    const key = language === "en" ? "en_html" : "he_html";
    // Dirty editors retain their original encoded metadata across local saves.
    // Never write that metadata back: only summary HTML is editable here.
    if (!latest || !changed || !summary || latest.metadata[key] !== old.metadata[key])
      return fail();
    const html = editedHtml.slice(summary.openEnd, summary.closeStart);
    if (!safeVariantHtml(html)) return fail();
    const value = { ...latest.metadata, [key]: html };
    const encoded = encodeURIComponent(JSON.stringify(value));
    if (!validCollectionNewsI18nAttribute(encoded)) return fail();
    const opening = editedHtml
      .slice(summary.parent!.start, summary.parent!.openEnd)
      .replace(/(\sdata-news-i18n\s*=\s*)(?:"[^"]*"|'[^']*')/i, `$1"${escapeAttribute(encoded)}"`);
    parts.push(
      { start: summary.parent!.start, end: summary.parent!.openEnd, value: opening },
      { start: summary.openEnd, end: summary.closeStart, value: value.he_html },
    );
  }
  return replaceParts(editedHtml, parts);
}
