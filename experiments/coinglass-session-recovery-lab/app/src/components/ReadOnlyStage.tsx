import { useState } from "react";
import { ChevronDown } from "lucide-react";
import { AnswerValue } from "@/components/AnswerValue";
import { PinnedAnswers } from "@/components/PinnedAnswers";
import { formatUpdatedAt, isAnswered, visibleQuestions } from "@/lib/domain";
import type { Answer, Question, Stage } from "@/lib/domain";
import type { PinnedAnswer } from "@/lib/api";
import { snapshotUpdatedAt, type StructuredSnapshot } from "@/lib/structured";
import { isInlineImagePath } from "@/lib/content";
import { QuestionSeenObserver } from "@/components/QuestionSeenObserver";

export type ReadOnlyImage = {
  question_id: string | null;
  storage_path: string;
  file_name: string | null;
  updated_at?: string | null;
  created_at?: string | null;
};

/**
 * מצב קריאה — תצוגה קומפקטית של מה שכבר מולא בגליון.
 * שאלה ללא תשובה אינה מוצגת, וסקשן ללא שאלות מוצגות מוסתר לגמרי.
 * זהו מצב תצוגה בלבד: אין כאן שום שדה עריכה ואין שינוי נתונים.
 */
export function ReadOnlyStage({
  stage,
  answersById,
  images,
  pinsFor,
  showPinnedOnly,
  unreadByQuestion = {},
  onSeen,
  structuredFor,
  structuredFilled,
}: {
  stage: Stage;
  answersById: Record<string, Answer>;
  images: ReadOnlyImage[];
  pinsFor: (q: Question) => PinnedAnswer[];
  showPinnedOnly: boolean;
  /** question_id -> latest_at שטרם נצפה */
  unreadByQuestion?: Record<string, string>;
  onSeen?: (questionId: string) => void;
  structuredFor?: (q: Question) => StructuredSnapshot | null;
  structuredFilled?: (questionId: string) => boolean;
}) {
  // תמונות שהודבקו בתוך טקסט אינן קבצים מצורפים ואינן ראיה לתוכן בפני עצמן
  const imagesOf = (q: Question) =>
    images.filter((i) => i.question_id === q.id && !isInlineImagePath(i.storage_path));

  const groups = stage.groups
    .filter((g) => g.active)
    .map((group) => {
      const items = visibleQuestions(group.questions, answersById)
        .filter((q) => q.question_type !== "info")
        .map((q) => {
          const qImages = imagesOf(q);
          const structured = structuredFor?.(q) ?? null;
          const answered = isAnswered(
            q,
            answersById[q.id],
            qImages.length,
            structuredFilled?.(q.id) ?? false,
          );
          const pins = pinsFor(q);
          return { q, qImages, structured, answered, pins };
        })
        .filter((it) => it.answered || (showPinnedOnly && it.pins.length > 0));
      return { group, items };
    })
    .filter((g) => g.items.length > 0);

  if (!groups.length) {
    return (
      <div className="rounded-xl border border-dashed border-border bg-card p-6 text-center text-sm text-muted-foreground">
        אין מידע שמולא בגליון זה.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {groups.map(({ group, items }) => (
        <ReadOnlyGroup key={group.id} title={group.title} count={items.length}>
          {items.map(({ q, qImages, structured, answered, pins }) => {
            const stamps = [
              answersById[q.id]?.updated_at ?? null,
              structured ? snapshotUpdatedAt(structured) : null,
              ...qImages.map((i) => i.updated_at ?? i.created_at ?? null),
            ].filter(Boolean) as string[];
            const updatedAt = stamps.length
              ? stamps.reduce((a, b) => (+new Date(b) > +new Date(a) ? b : a))
              : null;
            return (
              <div
                key={q.id}
                id={`q-${q.id}`}
                className={`border-b border-border/60 pb-3 last:border-0 last:pb-0 ${
                  unreadByQuestion[q.id] ? "rounded-lg bg-emerald-500/5 px-2" : ""
                }`}
              >
                <div className="flex flex-wrap items-baseline gap-2">
                  <span className="text-sm font-semibold">{q.title}</span>
                  {updatedAt && (
                    <span className="text-[11px] text-muted-foreground">
                      עודכן {formatUpdatedAt(updatedAt)}
                    </span>
                  )}
                </div>
                {/* מצב קריאה מסמן "נצפה" באותה לוגיקה בדיוק: תוכן גלוי + שהייה */}
                <QuestionSeenObserver
                  active={!!unreadByQuestion[q.id] && !!onSeen}
                  version={unreadByQuestion[q.id] ?? null}
                  onSeen={() => onSeen?.(q.id)}
                  className="mt-1 text-sm leading-relaxed"
                >
                  {answered ? (
                    <AnswerValue
                      questionType={q.question_type}
                      answer={answersById[q.id] ?? {}}
                      images={qImages}
                      imageMode="inline"
                      align={q.default_text_align}
                      structured={structured}
                    />
                  ) : (
                    <span className="text-muted-foreground">אין תשובה בפרויקט זה</span>
                  )}
                </QuestionSeenObserver>
                {showPinnedOnly && pins.length > 0 && (
                  <div className="mt-2">
                    <PinnedAnswers pins={pins} />
                  </div>
                )}
              </div>
            );
          })}
        </ReadOnlyGroup>
      ))}
    </div>
  );
}

/** קיפול עצמאי לכל סקשן במצב קריאה — ברירת המחדל פתוח. */
function ReadOnlyGroup({
  title,
  count,
  children,
}: {
  title: string;
  count: number;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(true);
  return (
    <section className="rounded-xl border border-border bg-card p-4 shadow-card">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        className="mb-3 flex w-full items-center gap-2 text-right"
      >
        <ChevronDown
          className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${
            open ? "" : "-rotate-90"
          }`}
        />
        <h3 className="text-sm font-bold">{title}</h3>
        <span className="text-[11px] text-muted-foreground">({count})</span>
      </button>
      {open && <div className="space-y-3">{children}</div>}
    </section>
  );
}
