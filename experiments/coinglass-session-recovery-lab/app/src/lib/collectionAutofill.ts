import { linksOf } from "./domain";
import { isYouTubeSourceUrl } from "./collectionYouTube";
import { unbiasWeeklySourceUrl } from "./collectionUnbiasWeekly";
import { CAPITAL_MARKET_SOURCE_URLS } from "./collectionCapitalMarketConfig";
import { autofillScheduleSettings, type AutofillSchedule } from "./collectionAutofillSchedule";
import type { V2TemplateField } from "./workspaceV1";
import {
  cleanNewsSummary,
  formatNewsPublicationTime,
  renderNewsSummary,
  type NewsInlineNote,
} from "./collectionNewsPresentation";

export const AUTOFILL_HOURS = Array.from({ length: 24 }, (_, i) => i + 1);
export const AUTOFILL_COLOR = "rt-color-purple";
export const COLLECTION_FIELD_TYPES = {
  capital_market: "שוק ההון",
  news_group: "קבוצת חדשות",
  unbiased_news: "חדשות unbiased",
  options_expiry: "פקיעות",
  correlation: "קורלציה",
} as const;
export type AutofillKind =
  | "capital_market"
  | "weekly_consensus"
  | "news"
  | "economic_calendar"
  | "liquidity"
  | "expiry"
  | "correlation"
  | "alt_correlation"
  | "range"
  | "dominance"
  | "etf"
  | "snapshot";
export type AutofillSettings = {
  enabled: boolean;
  interval_hours: number;
  schedule: AutofillSchedule;
  instructions: string;
  rate_importance: boolean;
  write_mode: "default" | "append" | "replace";
  color: string | null;
};
export type RolloverSettings = { mode: "days" | "light" | "heavy" | "never"; days: number };
export type AutofillResult = {
  run_id: string;
  status: "completed" | "unchanged" | "running" | "failed" | "conflict";
  session_id?: string;
  count?: number;
  message?: string;
  /** Continue the existing leased run; never claim another run. */
  continue?: boolean;
};

const object = (raw: unknown): Record<string, unknown> =>
  raw && typeof raw === "object" && !Array.isArray(raw) ? (raw as Record<string, unknown>) : {};

export function sourceUrls(
  field: Pick<V2TemplateField, "settings"> & { field_type?: string },
): string[] {
  if (field.field_type === "capital_market") return [...CAPITAL_MARKET_SOURCE_URLS];
  const settings = field.settings;
  if (Object.prototype.hasOwnProperty.call(settings, "autofill_source_urls")) {
    const raw = settings.autofill_source_urls;
    if (!Array.isArray(raw) || raw.some((value) => typeof value !== "string")) return [];
    const urls = raw.map((value: string) => value.trim());
    return urls.every(supportedSourceUrl) ? [...new Set(urls)] : [];
  }
  const legacy = object(settings.legacy_question);
  const explicit = (key: string) => Object.prototype.hasOwnProperty.call(settings, key);
  const links = explicit("links_json")
    ? settings.links_json
    : explicit("links")
      ? settings.links
      : legacy.links_json;
  const source = explicit("source_url") ? settings.source_url : legacy.source_url;
  return [
    ...new Set(
      linksOf({
        links_json: links,
        source_url: String(source ?? "") || null,
      })
        .map((link) => link.url.trim())
        .filter(Boolean),
    ),
  ];
}

/** Reject credentials, local destinations and deferred sources before enabling automation. */
export function supportedSourceUrl(raw: string): boolean {
  try {
    const url = new URL(raw);
    const host = url.hostname.toLowerCase();
    return (
      url.protocol === "https:" &&
      !url.username &&
      !url.password &&
      !url.port &&
      host.includes(".") &&
      !/^(localhost|127\.|0\.|10\.|192\.168\.|169\.254\.|172\.(1[6-9]|2\d|3[01])\.)/.test(host) &&
      !host.endsWith(".local") &&
      !host.endsWith(".internal") &&
      !/(^|\.)(whatsapp\.com|wa\.me)$/.test(host) &&
      (!/(^|\.)(youtube\.com|youtu\.be)$/.test(host) || isYouTubeSourceUrl(raw))
    );
  } catch {
    return false;
  }
}

export function manualOnlyField(field: Pick<V2TemplateField, "title" | "field_type">): boolean {
  return (
    ["info", "button", "checkbox", "done_not_done", "yes_no_na"].includes(field.field_type) ||
    /חפק\s*חמל|חפ[״"]?ק\s*חמ[״"]?ל|לאן לדעתך ילך הגרף|מה הוייב|אינד[יק]*טור רעי/.test(field.title)
  );
}

export function autofillKind(
  field: Pick<V2TemplateField, "title" | "field_type" | "settings">,
  sheetKey = "data",
): AutofillKind {
  if (field.field_type === "capital_market") return "capital_market";
  if (field.field_type === "economic_calendar") return "economic_calendar";
  if (sourceUrls(field).some((url) => unbiasWeeklySourceUrl(url))) return "weekly_consensus";
  if (field.field_type === "options_expiry" || /אופציות.*יפקעו|פקיע/.test(field.title))
    return "expiry";
  if (field.field_type === "correlation") return "correlation";
  if (/קורלצי.*אלט/.test(field.title)) return "alt_correlation";
  if (/קורלצי/.test(field.title)) return "correlation";
  if (field.field_type === "liquidity_table") return "liquidity";
  if (/\bRange\b|טווח המחיר/i.test(field.title)) return "range";
  if (/דומיננס|דומיננט|(?:BTC|USDT)\.D/i.test(field.title)) return "dominance";
  if (/\bETF\b/i.test(field.title)) return "etf";
  if (
    sheetKey === "news_macro" ||
    ["news_table", "news_source_table", "news_group", "unbiased_news"].includes(field.field_type)
  )
    return "news";
  return "snapshot";
}

export function defaultAutofillInstructions(
  field: Pick<V2TemplateField, "title" | "field_type" | "settings">,
  sheetKey?: string,
): string {
  const kind = autofillKind(field, sheetKey);
  if (kind === "capital_market")
    return "מלא מחיר ושינוי יומי לעומת הסגירה הקודמת עבור נאסד״ק 100, S&P 500 ונפט WTI. קרא מ־Newhedge את QQQ Correlation, SPY Correlation ו־Crude Oil Correlation המדויקים, בחלון מתגלגל של 30 יום. שמור מקור וזמן לכל נתון. קורלציה דקתית מיועדת למשתמש בלבד; אין לשנות את ההערות או להמציא נתון חסר.";
  if (kind === "weekly_consensus")
    return "בחר Weekly Consensus של ביטקוין. צלם את יחס השורי־דובי ואת כל רשימות הסימוכין שמתחתיו מהמקור עצמו. אמת שהחלון הוא 7 ימים; אין לשמור צילום חודשי, סימוכין חלקיים או נתונים שלא זוהו.";
  if (kind === "economic_calendar")
    return "סרוק ביומן Investing את היום והימים הקרובים, עד שבעה ימים לכל היותר. הוסף ליומן המשותף רק אירועי 3 כוכבים עם תיאור ושעה מאומתת. שמור התראות ברירת מחדל, עריכות ידניות ואירועים קיימים; אין להוסיף כפילויות.";
  if (kind === "news")
    return "סרוק רק אייטמים חדשים מאז הסריקה המוצלחת האחרונה. סכם במינימום מילים חדשות וניתוח רלוונטיים לביטקוין, קריפטו, שוק ההון ומאקרו, ללא הקדמות או שם המקור. מתחת לסיכום הוסף תאריך ושעת פרסום כקישור למקור ותמונה אם קיימת. אם התבקשת להוסיף הערות משלך, כתוב אותן בכתב נטוי; מספרים בפורמט K/M/B. דלג על פרסום ותוכן שאינו מסביר את מצב השוק. ביוטיוב סכם רק מתוך תמליל הסרטון המקורי, בדגש על ביטקוין; אין לסכם על סמך כותרת או תיאור בלבד. קבוצות וואטסאפ אינן נסרקות בשלב זה.";
  if (kind === "liquidity")
    return "קרא את מודל קוינגלאס המצורף. הצג רק רמות נזילות פעילות בקצה הימני, עם מחירים מלאים בדולר, צד ביחס למחיר הביטקוין וכמות יחסית: הרבה, בינוני או מעט. אמת את מספר המודל וטווח הזמן. אין להמציא רמות שאינן קריאות.";
  if (kind === "expiry")
    return "בכל סריקה בחר מחדש פקיעות משמעותיות ב־30 הימים הקרובים לפי הזמן שנותר והגודל ביחס לתאריכים סמוכים. דריביט היא הבורסה העיקרית לבחירה; אפשר לבחור גם תאריך שבו הסכום המאומת המצטבר בשלוש הבורסות משמעותי. פקיעה עצומה באותו שבוע מצמצמת את הבחירה לתאריכים הבולטים; פקיעה רחוקה אינה מסתירה חריגה קרובה. דווח עבור דריביט, בייננס ובייביט סכום נומינלי בדולר ומחיר מקס פיין מהממשק הרשמי של קוינגלאס. שמור מספרים מדויקים ונתון חסר ריק; אין להחליף מקס פיין במחיר הביטקוין.";
  if (kind === "correlation")
    return "קרא בתחתית העמוד את QQQ Correlation ואת SPY Correlation. העתק את שני המספרים, ללא השערה או חישוב מקורב.";
  if (kind === "alt_correlation")
    return "קרא בתחתית העמוד את Altcoins Correlation והעתק את המספר המדויק.";
  if (kind === "range")
    return "זהה בגרף את טווח המחיר האחרון מאז המהלך ששינה אותו. כתוב את הקצה הנמוך והגבוה במחירי דולר מלאים. אם הגבולות אינם קריאים או חד משמעיים, אל תמלא.";
  if (kind === "dominance")
    return "קרא את גרף הדומיננס היומי מהמקור שהוגדר. הצג את האחוז הנוכחי, שינויים מאומתים של 1D, 1W, 1M וצילום הגרף היומי עצמו. אם הוגדר גם צירוף מטבעות יציבים, קרא גם אותו והצג את אותם אחוזי שינוי. הוסף לכל גרף תחזית קצרה ופשוטה בעברית המבוססת על המגמה היומית שנמדדה, ללא מחירי יעד מומצאים או מצייני מקום לתמונה.";
  if (kind === "etf")
    return "כתוב את סכום הדולר של נר Daily Total Net Inflow האחרון בפורמט K/M/B, חיובי בירוק ושלילי באדום. הוסף צילום עם זום לנרות האחרונים. אם הצילום אינו זמין, הצג את הנרות האחרונים המאומתים כשורות תאריך וסכום באדום וירוק. אין להמציא נתון חסר.";
  return "קרא את הנתונים העדכניים מהקישורים המצורפים וסכם רק את הנדרש בשאלה. אל תמציא נתון חסר או לא קריא.";
}

export function autofillSettings(
  field: Pick<V2TemplateField, "settings" | "title" | "field_type">,
): AutofillSettings {
  const raw = object(field.settings.autofill);
  const hours = Number(raw.interval_hours ?? 24);
  return {
    enabled:
      (raw.enabled === true ||
        (field.field_type === "capital_market" && raw.enabled === undefined)) &&
      !manualOnlyField(field) &&
      sourceUrls(field).some(supportedSourceUrl) &&
      (field.field_type !== "economic_calendar" ||
        sourceUrls(field).every((url) =>
          /^https:\/\/(?:www\.)?investing\.com\/economic-calendar\/?$/.test(url),
        )),
    interval_hours: Number.isInteger(hours) && hours >= 1 && hours <= 24 ? hours : 24,
    schedule: autofillScheduleSettings(raw.schedule),
    instructions:
      typeof raw.instructions === "string" ? raw.instructions : defaultAutofillInstructions(field),
    rate_importance: raw.rate_importance !== false,
    write_mode:
      raw.write_mode === "append" || raw.write_mode === "replace" ? raw.write_mode : "default",
    color:
      typeof raw.color === "string" &&
      /^rt-color-(default|red|orange|gold|green|blue|purple|gray)$/.test(raw.color)
        ? raw.color
        : null,
  };
}

export function rolloverSettings(settings: Record<string, unknown>): RolloverSettings {
  const raw = object(settings.autofill_rollover);
  const days = Number(raw.days ?? 1);
  return {
    mode: ["days", "light", "heavy", "never"].includes(String(raw.mode))
      ? (raw.mode as RolloverSettings["mode"])
      : "days",
    days: Number.isInteger(days) && days >= 1 && days <= 7 ? days : 1,
  };
}

export function effectiveAutofillRating(
  settings: AutofillSettings,
  section: Record<string, unknown>,
): boolean {
  return section.autofill_importance_override === "on"
    ? true
    : section.autofill_importance_override === "off"
      ? false
      : settings.rate_importance;
}

export function canonicalSourceUrl(raw: string): string {
  const url = new URL(raw);
  url.hash = "";
  for (const key of [...url.searchParams.keys()])
    if (/^utm_|^(fbclid|gclid)$/.test(key)) url.searchParams.delete(key);
  return url.href;
}

export function escapeAutofillHtml(raw: string): string {
  return raw.replace(
    /[&<>"']/g,
    (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]!,
  );
}

export function renderAutofillItem(
  item: {
    id: string;
    summary: string;
    url: string;
    sourceName: string;
    publishedAt: string;
    imagePaths?: string[];
    commentary?: string | null;
    inlineNotes?: NewsInlineNote[];
  },
  color = AUTOFILL_COLOR,
): string {
  const escaped = escapeAutofillHtml;
  const summary = cleanNewsSummary(item.summary);
  if (!summary) throw new Error("סיכום המקור ריק; דבר לא נשמר.");
  return (
    `<div data-autofill-id="${escaped(item.id)}" class="${color}"><p class="rt-news-summary">${renderNewsSummary(summary, escaped)}</p>` +
    `<p class="rt-news-date"><a href="${escaped(item.url)}" target="_blank" rel="noopener noreferrer">${escaped(formatNewsPublicationTime(item.publishedAt))}</a></p>` +
    (item.imagePaths ?? [])
      .map(
        (path) =>
          `<img data-path="${escaped(path)}" data-autofill-image-size="compact" alt="תמונה מהמקור" style="width:49%">`,
      )
      .join("") +
    `</div>`
  );
}
