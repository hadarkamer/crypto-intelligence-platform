import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { useVerticalResize } from "@/hooks/useVerticalResize";
import { PersistentScrollbar } from "@/components/PersistentScrollbar";
import type { HistoryItem } from "@/lib/api";
import type { Question } from "@/lib/domain";
import { formatUpdatedAt } from "@/lib/domain";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronRight, GripHorizontal, Pin } from "lucide-react";
import { PreviousAnswerBody } from "@/components/PreviousAnswerBody";
import { previousAnswerNewsRows } from "@/lib/collectionPreviousAnswerPresentation";
import type { NewsRow, StructuredSnapshot } from "@/lib/structured";
import type { CollectionSnapshotRow } from "@/lib/workspaceReadModels";
import type { HistoricalAnswerSelector } from "@/lib/collectionPreviousAnswerSelection";

export type PreviousHistoryItem = HistoryItem & {
  structured?: StructuredSnapshot | null;
  /** Retain the saved type when the current template has since changed. */
  question_type?: string;
  origin_href?: string;
  source_created_at?: string;
  source_snapshot?: CollectionSnapshotRow;
  source_selector?: HistoricalAnswerSelector;
  source_pin_id?: string;
};

/**
 * תצוגה לקריאה בלבד של התשובות שניתנו לאותה שאלה בפרויקטים קודמים,
 * עם דפדוף אחורה בהיסטוריה. אין כאן שום אפשרות עריכה.
 */
export function PreviousAnswer({
  question,
  history,
  canPin = false,
  pinnedIdOf,
  onPin,
  onUnpin,
  label = "תשובה קודמת",
  historyContextKey,
  renderActions,
  renderNewsRowActions,
}: {
  question: Question;
  history: PreviousHistoryItem[];
  /** נעיצת תשובה מההיסטוריה — אותו pipeline של נעיצה רגילה. */
  canPin?: boolean;
  pinnedIdOf?: (item: PreviousHistoryItem) => string | null;
  onPin?: (item: PreviousHistoryItem) => void;
  onUnpin?: (pinId: string) => void;
  label?: string;
  /** Parent worksheet identity prevents paging state leaking across sheets. */
  historyContextKey?: string;
  /** Actions must use the historical entry, including when all entries are visible. */
  renderActions?: (item: PreviousHistoryItem) => ReactNode;
  renderNewsRowActions?: (item: PreviousHistoryItem, row: NewsRow) => ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const [index, setIndex] = useState(0);
  const [showAll, setShowAll] = useState(false);
  const paneId = useId();
  const pane = useRef<HTMLDivElement>(null);
  const [height, setHeight] = useState<number | null>(null);
  const [maxHeight, setMaxHeight] = useState(768);
  useEffect(() => {
    const resize = () => {
      const maximum = Math.max(64, Math.floor(window.innerHeight * 0.75));
      setMaxHeight(maximum);
      setHeight((current) => (current === null ? null : Math.min(current, maximum)));
    };
    resize();
    window.addEventListener("resize", resize);
    return () => {
      window.removeEventListener("resize", resize);
    };
  }, []);
  useEffect(() => {
    setOpen(false);
    setIndex(0);
    setShowAll(false);
    setHeight(null);
  }, [question.id, historyContextKey]);

  useEffect(() => {
    if (pane.current) pane.current.scrollTop = 0;
  }, [index, showAll, question.id, historyContextKey]);

  const safeIndex = Math.min(index, history.length - 1);
  const item = history[safeIndex];
  const currentHeight = () => pane.current?.getBoundingClientRect().height || height || 320;
  const resizeTo = (value: number) => setHeight(Math.max(64, Math.min(maxHeight, value)));
  const resizing = useVerticalResize({
    viewport: pane,
    enabled: open && !!history.length,
    onResize: resizeTo,
    onToggle: () => resizeTo(height !== null && height <= 64 ? maxHeight : 64),
  });
  if (!history.length) return null;
  const controls = (placement: "top" | "bottom") =>
    (history.length > 1 || showAll) && (
      <nav
        aria-label={`ניווט בתשובות קודמות — ${placement === "top" ? "עליון" : "תחתון"}`}
        className="flex flex-wrap items-center gap-2"
        data-history-controls={placement}
      >
        <Button size="sm" variant="outline" onClick={() => setShowAll((value) => !value)}>
          {showAll ? "תשובה אחת בכל פעם" : "הצג את כל התשובות"}
        </Button>
        {!showAll && (
          <div dir="ltr" className="flex items-center gap-2" data-history-pager>
            <Button
              dir="ltr"
              aria-label="הקודם"
              size="sm"
              variant="outline"
              disabled={safeIndex >= history.length - 1}
              onClick={() => setIndex(Math.min(safeIndex + 1, history.length - 1))}
            >
              <span aria-hidden="true">‹</span> <span dir="rtl">הקודם</span>
            </Button>
            <Button
              dir="ltr"
              aria-label="הבא"
              size="sm"
              variant="outline"
              disabled={safeIndex === 0}
              onClick={() => setIndex(Math.max(safeIndex - 1, 0))}
            >
              <span dir="rtl">הבא</span> <span aria-hidden="true">›</span>
            </Button>
          </div>
        )}
      </nav>
    );

  return (
    <div className="rounded-lg border border-dashed border-border bg-muted/30">
      <div className="flex items-center">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          className="flex min-w-0 flex-1 items-center gap-2 p-2 text-right text-xs font-medium text-muted-foreground"
          aria-expanded={open}
        >
          {open ? (
            <ChevronDown className="h-4 w-4 shrink-0" />
          ) : (
            <ChevronRight className="h-4 w-4 shrink-0 rotate-180" />
          )}
          <span>{label}</span>
          <span className="text-[11px] opacity-70">({history.length} רשומות)</span>
        </button>
      </div>

      {open && (
        <div className="space-y-2 border-t border-border/60 p-3">
          <div className="flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
            <span>פרויקט: {item.project_name}</span>
            <span>·</span>
            <span>עודכן: {formatUpdatedAt(item.updated_at) ?? "—"}</span>
            <span>·</span>
            <span className="tabular-nums">
              {safeIndex + 1} מתוך {history.length}
            </span>
            {item.origin_href && !showAll && (
              <a href={item.origin_href} className="text-primary underline">
                לגליון
              </a>
            )}
          </div>

          {controls("top")}
          <div className="relative min-w-0">
            <div
              id={paneId}
              ref={pane}
              dir="ltr"
              className="persistent-scroll-viewport min-w-0 resize-none overflow-auto rounded-md border border-border bg-card p-2 text-sm"
              style={{
                height: height === null ? "auto" : `${height}px`,
                minHeight: "4rem",
                maxHeight: height === null ? "min(20rem, 75dvh)" : "75dvh",
                paddingRight: 22,
                scrollbarGutter: "stable",
              }}
              role="region"
              aria-label={showAll ? "כל התשובות הקודמות" : "תוכן התשובה הקודמת"}
              tabIndex={0}
            >
              <div dir="rtl" className="min-w-0 text-right">
                {(showAll ? history : [item]).map((entry, entryIndex) => {
                  const pinId = pinnedIdOf?.(entry) ?? null;
                  const rowActions =
                    renderNewsRowActions && !!previousAnswerNewsRows(entry)?.length;
                  const entryActions = !rowActions && ((canPin && onPin) || renderActions);
                  return (
                    <div
                      key={`${entry.project_id}:${entry.question_id}:${entry.trade_cycle_id ?? ""}:${entry.updated_at}:${entryIndex}`}
                      className={`relative min-w-0 ${entryActions ? "pl-16" : ""} ${showAll ? "space-y-2 border-b border-border/60 py-3 last:border-0" : ""}`}
                    >
                      {entryActions ? (
                        <div
                          className="absolute left-0 top-0 z-10 flex items-center gap-1"
                          data-history-actions
                        >
                          {canPin && onPin && (
                            <button
                              type="button"
                              onClick={() => (pinId ? onUnpin?.(pinId) : onPin(entry))}
                              title={pinId ? "הסר נעיצה" : "נעץ תשובה זו"}
                              aria-label={pinId ? "הסר נעיצה" : "נעץ תשובה זו"}
                              aria-pressed={!!pinId}
                              className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-full border transition-colors ${
                                pinId
                                  ? "border-primary bg-primary/15 text-primary"
                                  : "border-border text-muted-foreground hover:bg-muted"
                              }`}
                            >
                              <Pin
                                className={`h-3.5 w-3.5 ${pinId ? "fill-current" : "-rotate-45"}`}
                              />
                            </button>
                          )}
                          {renderActions?.(entry)}
                        </div>
                      ) : null}
                      {showAll && (
                        <div className="flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
                          <span>{entry.project_name}</span>
                          <span>{formatUpdatedAt(entry.updated_at) ?? "—"}</span>
                          {entry.origin_href && (
                            <a href={entry.origin_href} className="text-primary underline">
                              לגליון
                            </a>
                          )}
                        </div>
                      )}
                      <PreviousAnswerBody
                        item={entry}
                        question={question}
                        renderNewsRowActions={renderNewsRowActions}
                      />
                    </div>
                  );
                })}
              </div>
            </div>
            <PersistentScrollbar viewport={pane} />
          </div>
          <button
            type="button"
            role="separator"
            aria-label="שינוי גובה התשובות הקודמות"
            aria-orientation="horizontal"
            aria-controls={paneId}
            aria-valuemin={64}
            aria-valuemax={maxHeight}
            aria-valuenow={Math.min(height ?? 320, maxHeight)}
            title="גררו לשינוי הגובה; לחיצה כפולה לכיווץ או לפתיחה מלאה"
            className="resize-handle flex h-6 w-full select-none cursor-ns-resize touch-none items-center justify-center rounded-md border border-border/60 bg-muted/50 text-muted-foreground hover:bg-muted focus-visible:ring-2 focus-visible:ring-ring"
            data-history-resize
            {...resizing}
            onKeyDown={(event) => {
              const amount =
                event.key === "ArrowDown"
                  ? 16
                  : event.key === "ArrowUp"
                    ? -16
                    : event.key === "PageDown"
                      ? 64
                      : event.key === "PageUp"
                        ? -64
                        : 0;
              if (amount || event.key === "Home" || event.key === "End") {
                event.preventDefault();
                resizeTo(
                  event.key === "Home"
                    ? 64
                    : event.key === "End"
                      ? maxHeight
                      : currentHeight() + amount,
                );
              }
            }}
          >
            <GripHorizontal className="h-4 w-6" aria-hidden="true" />
          </button>
          {controls("bottom")}
        </div>
      )}
    </div>
  );
}
