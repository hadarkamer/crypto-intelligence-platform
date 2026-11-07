/** Presentation rules apply to every automatic post/article summary. */
export const NEWS_PRESENTATION_INSTRUCTIONS =
  "Write the shortest clear Hebrew summary using only necessary words, usually 1–2 short sentences per distinct point. " +
  "Preserve useful paragraph and blank-line separation from the source, including separate price levels, scenarios and distinct arguments. Summarize each retained point on its own line or paragraph; use a blank line between source paragraphs. Do not flatten a structured tweet into one dense paragraph or invent extra sections for a short single-point post. Apply the same structure in Hebrew and English. " +
  "State the content directly. Do not introduce it with דיווח כי, טענה ש, הצהרה על, המפרסם מציין ש, מצוין כי or similar framing. " +
  "The source is already identified by the worksheet: omit its name/handle, מקור:, the word פוסט, publication timestamps and URLs from the summary. " +
  "Keep source uncertainty explicit with brief words such as משער, אולי or עשוי; never turn a source hypothesis into a verified fact. " +
  "Use compact numbers such as 1M$, 1.3B$, 750K and 87K. Avoid generic market implications, caveats and repetition. " +
  "Return only source content. Never add your own caveats, analysis, explanatory parentheses, source labels, publication metadata, content-type labels, image descriptions or links, even if older worksheet instructions request them. " +
  "Do not generate commentary or inline_notes. The application adds exactly one trusted linked publication date outside the summary. Summary is plain text, never HTML or Markdown.";

/** Remove recognisable generated metadata, preserving factual source parentheses. */
export function cleanNewsSummary(raw: string): string {
  return raw
    .replace(/\r\n?/g, "\n")
    .split(/\r?\n/)
    .filter(
      (line) =>
        !/^\s*[*_]*(?:מקור\s*:|מועד פרסום(?: מקורי)?\s*:|זמן\s*:|סוג תוכן\s*:|תמונה\s*:|קישור\s*:|source\s*:|published(?: at)?\s*:|image\s*:)/i.test(
          line,
        ),
    )
    .join("\n")
    .replace(
      /\s*\*?\((?:נקודת מפתח שמוצגת|תחזית, לא ודאות|הערכה בלבד|השערה, לא ודאות)[^)]*\)\*?/g,
      "",
    )
    .trim();
}

/** Source-owned structure helps both language summaries retain distinct points.
 * These are still untrusted source text, never additional model instructions. */
export function newsSourceParagraphs(text: string): string[] {
  return text
    .replace(/\r\n?/g, "\n")
    .split(/\n[\t ]*\n+/)
    .map((paragraph) => paragraph.trim())
    .filter(Boolean);
}

/** Preserve author-entered line and paragraph breaks as trusted structure.
 * Browser wrapping never appears here: this is the source API's plain text. */
export function newsSourceStructure(text: string): { parts: string[]; breaks: string[] } {
  const chunks = text
    .replace(/\r\n?/g, "\n")
    .replace(/^[\t ]+$/gm, "")
    .split(/(\n+)/);
  const parts: string[] = [],
    breaks: string[] = [];
  let pending = "";
  for (const chunk of chunks) {
    if (/^\n+$/.test(chunk)) {
      pending += chunk;
      continue;
    }
    const part = chunk.trim();
    if (!part) continue;
    if (parts.length) breaks.push(pending.length > 1 ? "\n\n" : "\n");
    parts.push(part);
    pending = "";
  }
  return { parts, breaks };
}

/** A single compact publication instant; all clients use the worksheet time zone. */
export function formatNewsPublicationTime(timestamp: string): string {
  const date = new Date(timestamp);
  if (!Number.isFinite(date.getTime())) throw new Error("תאריך פרסום המקור אינו תקין.");
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Jerusalem",
    day: "numeric",
    month: "numeric",
    year: "2-digit",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) => parts.find((p) => p.type === type)?.value;
  return `${part("day")}.${part("month")}.${part("year")} · ${part("hour")}:${part("minute")}${part("dayPeriod")}`;
}

export type NewsInlineNote = { after: string; text: string };

/** Never compress dates, identifiers, decimal fractions or already small counts. */
export function compactNewsNumbers(raw: string, context = raw, contextStart = 0): string {
  return raw.replace(
    /(?<![\p{L}\p{N}_./:=])(?:-?\$|\$?-?)\d+(?:,\d{3})*(?:\.\d+)?[KMBkmb]?\$?(?![\p{L}\p{N}_/])/gu,
    (token, offset: number) => {
      offset += contextStart;
      // A numeric timestamp or URL is metadata, not a market amount.
      if (
        /[-/:]/.test(context[offset + token.length] ?? "") ||
        context[offset - 1] === "-" ||
        /[\p{L}\p{N}_./:=]/u.test(context[offset - 1] ?? "")
      )
        return token;
      const currency = token.includes("$") ? "$" : "";
      const number = token.replace(/\$/g, "");
      if (
        /(?:\bid|\btx|\btimestamp|מזהה)\s*[:=#]?\s*$/i.test(
          context.slice(Math.max(0, offset - 32), offset),
        )
      )
        return token;
      if (!currency && /^-?\d{12,}$/.test(number)) return token;
      const scaled = number.match(/^(-?\d+(?:\.\d+)?)([kmb])$/i);
      if (scaled) return `${scaled[1]}${scaled[2].toUpperCase()}${currency}`;
      const value = Number(number.replace(/,/g, ""));
      const absolute = Math.abs(value);
      if (!Number.isFinite(value) || absolute < 1000) return token;
      if (absolute < 10_000 && !currency && !number.includes(",")) return token;
      const [divisor, suffix] =
        absolute >= 1e9 ? [1e9, "B"] : absolute >= 1e6 ? [1e6, "M"] : [1e3, "K"];
      const amount = value / Number(divisor);
      const rounded = Math.round(amount * 1000) / 1000;
      const approximate = Math.abs(rounded - amount) > 1e-10 ? "≈" : "";
      return `${approximate}${rounded}${suffix}${currency}`;
    },
  );
}

export function renderNewsSummary(
  summary: string,
  escape: (raw: string) => string,
  notes: NewsInlineNote[] = [],
): string {
  if (!validNewsAnnotations(summary, null, notes))
    throw new Error("הערה אינה תואמת את טקסט הסיכום.");
  let cursor = 0;
  let html = "";
  // Notes must follow distinct verbatim anchors in source-summary order.
  for (const note of notes) {
    const start = summary.indexOf(note.after, cursor);
    if (!note.after || start < cursor) throw new Error("הערה אינה תואמת את טקסט הסיכום.");
    const end = start + note.after.length;
    html += escape(compactNewsNumbers(summary.slice(cursor, end), summary, cursor)).replace(
      /\n/g,
      "<br>",
    );
    html += ` <em>(${escape(compactNewsNumbers(note.text))})</em>`;
    cursor = end;
  }
  return (
    html + escape(compactNewsNumbers(summary.slice(cursor), summary, cursor)).replace(/\n/g, "<br>")
  );
}

export function validNewsAnnotations(
  summary: string,
  commentary: unknown,
  notes: unknown,
): boolean {
  if (commentary != null && (typeof commentary !== "string" || commentary.length > 1000))
    return false;
  if (notes === undefined) return true;
  if (!Array.isArray(notes) || notes.length > 5) return false;
  let cursor = 0;
  for (const note of notes) {
    if (
      !note ||
      typeof note !== "object" ||
      Array.isArray(note) ||
      typeof note.after !== "string" ||
      !note.after ||
      typeof note.text !== "string" ||
      !note.text.trim() ||
      note.text.length > 500
    )
      return false;
    const anchor = summary.indexOf(note.after, cursor);
    if (anchor < cursor) return false;
    cursor = anchor + note.after.length;
    // An inline note cannot split an amount, date or numeric identifier.
    if (/[\d,.]/.test(summary[cursor - 1] ?? "") && /[\d,.KMBkmb$]/.test(summary[cursor] ?? ""))
      return false;
  }
  return true;
}
