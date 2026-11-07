/**
 * מקור אמת יחיד לשאלה "האם יש כאן תוכן משמעותי?".
 * משמש ב-isAnswered, בהתקדמות, במצב קריאה ובבדיקות היסטוריה,
 * כדי שלא ייספרו כתשובה ערכים ריקים כמו <br>, &nbsp; או תווי אפס-רוחב.
 */

// eslint-disable-next-line no-misleading-character-class -- הסרה תו-אחר-תו של תווי אפס רוחב, לא של גרפמות
const ZERO_WIDTH = /[\u200B\u200C\u200D\u200E\u200F\uFEFF]/g;

/** מפענח ישויות HTML נפוצות, כולל מקרה של קידוד כפול (&amp;nbsp;). */
function decodeEntities(input: string): string {
  return input
    .replace(/&amp;nbsp;/gi, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#160;|&#xa0;/gi, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

/** האם ה-HTML מכיל תמונה שעדיין קיימת בתוכן. */
export function htmlHasImage(raw: string | null | undefined): boolean {
  if (!raw) return false;
  return /<img\b[^>]*>/i.test(raw);
}

/** טקסט נקי מתוך HTML — לתצוגה מקדימה ולהשוואות. */
export function plainTextOf(raw: string | null | undefined): string {
  if (!raw) return "";
  return decodeEntities(raw.replace(/<[^>]*>/g, " "))
    .replace(ZERO_WIDTH, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * האם התוכן נחשב "מלא".
 * ריק: HTML ריק, רווחים בלבד, <br>, <div><br></div>, <p><br></p>,
 * &nbsp; / &amp;nbsp;, תווי אפס-רוחב, או תוכן שממנו הוסרה התמונה.
 */
export function hasMeaningfulContent(raw: string | null | undefined): boolean {
  if (raw === null || raw === undefined) return false;
  if (typeof raw !== "string") return false;
  if (htmlHasImage(raw)) return true;
  return plainTextOf(raw).length > 0;
}

/**
 * ערך מנורמל לשמירה: תוכן שאינו משמעותי נשמר כמחרוזת ריקה,
 * ותווי אפס-רוחב (סמני עריכה) מוסרים כדי שמעבר focus/blur בלבד
 * לא ייצור תשובה חדשה ולא יעדכן חותמת זמן.
 */
export function normalizeRichTextValue(raw: string | null | undefined): string {
  if (!hasMeaningfulContent(raw)) return "";
  return (raw as string).replace(ZERO_WIDTH, "");
}

/**
 * השוואה בטוחה בין ערך קיים לערך חדש.
 * שני ערכים ריקים (רווחים, <br>, &nbsp;, תווי אפס-רוחב) נחשבים זהים,
 * אבל כל שינוי אמיתי ב-HTML — כולל עיצוב בלבד (בולד, צבע, גודל) —
 * נחשב לשינוי ונשמר.
 */
export function sameRichTextValue(
  a: string | null | undefined,
  b: string | null | undefined,
): boolean {
  const na = normalizeRichTextValue(a);
  const nb = normalizeRichTextValue(b);
  if (na === nb) return true;
  return !hasMeaningfulContent(na) && !hasMeaningfulContent(nb);
}

/** נתיבי האחסון של תמונות שהודבקו בתוך HTML (data-path). */
export function inlineImagePathsIn(raw: string | null | undefined): string[] {
  if (!raw) return [];
  const out: string[] = [];
  const re = /data-path=["']([^"']+)["']/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(raw))) out.push(m[1]);
  return out;
}

/**
 * תמונה שהודבקה בתוך טקסט עשיר — נשמרת בנתיב עם סיומת inline.
 * היא אינה "קובץ מצורף" של הסעיף, ולכן אינה נספרת כתוכן בפני עצמה
 * (התוכן נקבע לפי ה-HTML עצמו, כך שתמונה שהוסרה מהטקסט אינה משאירה ראיה שקרית).
 */
export const isInlineImagePath = (path: string | null | undefined) =>
  !!path && /-inline-/.test(path);
