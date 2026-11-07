import { LtrNum } from "@/components/LtrNumeric";
import {
  workspaceUpdatedByClass,
  type WorkspaceUserTextColors,
} from "@/lib/workspaceUserTextColors";
import { useEffect, useMemo, useRef, useState } from "react";
import { Info, Plus, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  AMOUNT_LABELS,
  AMOUNT_TEXT_CLASS,
  DEFAULT_TIMEFRAMES,
  IMPORTANCE_OPTIONS,
  IMPORTANCE_TEXT_CLASS,
  LIQUIDITY_TYPE,
  NEWS_TYPE,
  TIMEFRAMES,
  importanceLabel,
  timeframeOrder,
  type LiquidityAmount,
  type LiquidityLevel,
  type LiquidityRow,
  type LiquiditySide,
  type NewsImportance,
  type NewsRow,
  type StructuredSnapshot,
} from "@/lib/structured";
import { formatUpdatedAt } from "@/lib/domain";
import { cn } from "@/lib/utils";
import { RichTextView } from "@/components/RichTextView";
import { newsSentimentStyle } from "@/lib/newsSentimentStyle";
import {
  levelPrecisionText,
  manualLiquidityPricePatch,
  savedLiquidityPriceText,
} from "@/lib/coinglassCollector";
import { coinglassQuantityText, coinglassSideTotalText } from "@/lib/coinglassQuantities";

function QuantityTotal({ row, side }: { row: unknown; side: LiquiditySide }) {
  return <bdi dir="auto">{coinglassSideTotalText(row, side)}</bdi>;
}

/** פעולות הכתיבה שהמסך הראשי מספק לטבלאות המובנות. */
export type StructuredActions = {
  addLiquidityRow: (questionId: string, timeframe: string) => void;
  createLiquidityLevel: (
    questionId: string,
    timeframe: string,
    side: LiquiditySide,
    priceText: string,
    amount: LiquidityAmount,
  ) => Promise<boolean>;
  deleteLiquidityRow: (rowId: string) => void;
  updateLiquidityLevel: (
    levelId: string,
    patch: Partial<Pick<LiquidityLevel, "price_text" | "amount" | "taken">>,
  ) => void;
  deleteLiquidityLevel: (levelId: string) => void;
  createNewsRow: (questionId: string, body: string, importance: NewsImportance) => Promise<boolean>;
  addNewsRow: (questionId: string) => Promise<boolean>;
  updateNewsRow: (rowId: string, patch: Partial<Pick<NewsRow, "body" | "importance">>) => void;
  deleteNewsRow: (rowId: string) => void;
};

type Labels = Record<string, string>;

function UpdatedMeta({
  updatedAt,
  updatedBy,
  labels,
  userTextColors,
}: {
  updatedAt?: string | null;
  updatedBy?: string | null;
  labels?: Labels;
  userTextColors?: WorkspaceUserTextColors;
}) {
  const [open, setOpen] = useState(false);
  const updateColor = workspaceUpdatedByClass(updatedBy, false, userTextColors);
  if (!updatedAt && !updatedBy) return null;
  const text = [
    updatedAt ? `עודכן ${formatUpdatedAt(updatedAt) ?? "—"}` : null,
    updatedBy ? `ע״י ${labels?.[updatedBy] ?? updatedBy.split("@")[0]}` : null,
  ]
    .filter(Boolean)
    .join(" · ");
  return (
    <>
      <span className={`hidden text-[10px] sm:inline ${updateColor}`}>{text}</span>
      <span className="relative sm:hidden">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-label="מידע על העדכון"
          className="flex h-6 w-6 items-center justify-center rounded-full border border-border text-muted-foreground"
        >
          <Info className="h-3.5 w-3.5" />
        </button>
        {open && (
          <span
            className={`absolute left-0 top-7 z-20 whitespace-nowrap rounded-md border border-border bg-popover px-2 py-1 text-[10px] shadow-md ${updateColor}`}
          >
            {text}
          </span>
        )}
      </span>
    </>
  );
}

/** שדה טקסט עם שמירה מושהית — שומר רק אם הערך השתנה. */
function DebouncedInput({
  value,
  onCommit,
  className,
  placeholder,
  dir,
}: {
  value: string;
  onCommit: (v: string) => void;
  className?: string;
  placeholder?: string;
  dir?: "ltr" | "rtl";
}) {
  const [local, setLocal] = useState(value);
  const focused = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => {
    if (!focused.current) setLocal(value);
  }, [value]);
  useEffect(() => () => clearTimeout(timer.current), []);

  return (
    <Input
      value={local}
      placeholder={placeholder}
      dir={dir}
      className={className}
      onFocus={() => (focused.current = true)}
      onChange={(e) => {
        const v = e.target.value;
        setLocal(v);
        clearTimeout(timer.current);
        timer.current = setTimeout(() => {
          if (v !== value) onCommit(v);
        }, 700);
      }}
      onBlur={() => {
        focused.current = false;
        clearTimeout(timer.current);
        if (local !== value) onCommit(local);
      }}
    />
  );
}

function DebouncedTextarea({
  value,
  onCommit,
  className,
  placeholder,
  onAddRow,
}: {
  value: string;
  onCommit: (v: string) => void;
  className?: string;
  placeholder?: string;
  onAddRow?: () => void;
}) {
  const [local, setLocal] = useState(value);
  const focused = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const ref = useRef<HTMLTextAreaElement | null>(null);

  const resize = () => {
    const el = ref.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.max(36, el.scrollHeight)}px`;
  };

  useEffect(() => {
    if (!focused.current) setLocal(value);
  }, [value]);
  useEffect(() => resize(), [local]);
  useEffect(() => () => clearTimeout(timer.current), []);

  return (
    <Textarea
      ref={ref}
      rows={1}
      value={local}
      placeholder={placeholder}
      className={cn("min-h-9 resize-none overflow-hidden", className)}
      onFocus={() => (focused.current = true)}
      onChange={(e) => {
        const v = e.target.value;
        setLocal(v);
        clearTimeout(timer.current);
        timer.current = setTimeout(() => {
          if (v !== value) onCommit(v);
        }, 700);
      }}
      onBlur={() => {
        focused.current = false;
        clearTimeout(timer.current);
        if (local !== value) onCommit(local);
      }}
      onKeyDown={(event) => {
        if (event.key !== "Enter" || !event.ctrlKey || event.repeat || !onAddRow) return;
        event.preventDefault();
        clearTimeout(timer.current);
        if (local !== value) onCommit(local);
        onAddRow();
      }}
    />
  );
}

function latestLiquidityMeta(row: LiquidityRow) {
  let at = row.updated_at ?? null;
  let by = row.updated_by ?? null;
  for (const level of row.levels) {
    if (level.updated_at && (!at || level.updated_at > at)) {
      at = level.updated_at;
      by = level.updated_by;
    }
  }
  return { at, by };
}

/* ============================ טבלת נזילות — עריכה ============================ */

function LevelEditor({
  level,
  row,
  actions,
}: {
  level: LiquidityLevel;
  row?: LiquidityRow;
  actions: StructuredActions;
}) {
  const quantity = coinglassQuantityText(row, level, level.side);
  return (
    <div className="min-w-0 space-y-0.5">
      <div className={cn("flex min-w-0 items-center gap-1.5", level.taken && "opacity-65")}>
        <DebouncedInput
          value={savedLiquidityPriceText(row, level)}
          placeholder="מחיר"
          dir="ltr"
          className={cn(
            "h-8 min-w-[5.5rem] flex-1 text-right font-bold tabular-nums",
            AMOUNT_TEXT_CLASS[level.amount],
            level.taken && "line-through text-muted-foreground",
          )}
          onCommit={(v) =>
            actions.updateLiquidityLevel(
              level.id,
              manualLiquidityPricePatch(
                v,
                Boolean(level.price_precision || level.source_observation),
              ),
            )
          }
        />
        <Select
          value={level.amount}
          onValueChange={(v) =>
            actions.updateLiquidityLevel(level.id, { amount: v as LiquidityAmount })
          }
        >
          <SelectTrigger
            className={cn("h-8 w-[82px] shrink-0 text-xs", AMOUNT_TEXT_CLASS[level.amount])}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {(Object.keys(AMOUNT_LABELS) as LiquidityAmount[]).map((k) => (
              <SelectItem key={k} value={k} className={AMOUNT_TEXT_CLASS[k]}>
                {AMOUNT_LABELS[k]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <label className="flex shrink-0 items-center gap-1 text-[10px] text-muted-foreground">
          <Checkbox
            checked={level.taken}
            onCheckedChange={(v) => actions.updateLiquidityLevel(level.id, { taken: v === true })}
          />
          נלקח
        </label>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-7 w-7 shrink-0 text-muted-foreground"
          aria-label="מחיקת רמה"
          onClick={() => actions.deleteLiquidityLevel(level.id)}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
      {quantity ? (
        <p className="text-xs font-semibold tabular-nums">
          <LtrNum>{quantity}</LtrNum>
        </p>
      ) : null}
      {level.price_precision ? (
        <p className="text-[10px] text-muted-foreground">{levelPrecisionText(level)}</p>
      ) : null}
    </div>
  );
}

function VirtualLevelEditor({
  questionId,
  timeframe,
  side,
  actions,
  onSaved,
}: {
  questionId: string;
  timeframe: string;
  side: LiquiditySide;
  actions: StructuredActions;
  onSaved?: () => void;
}) {
  const [amount, setAmount] = useState<LiquidityAmount | "">("");
  const [priceText, setPriceText] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const submittingRef = useRef(false);
  const inputRef = useRef<HTMLInputElement | null>(null);

  /**
   * רמת נזילות חדשה נוצרת רק בפעולה מפורשת: לחיצה על + או Enter.
   * בעבר שדה מושהה שמר אחרי 700ms וגם שוב ב-blur; בזמן הרענון
   * ה-editor הווירטואלי הוחלף בשורה שמורה, מה שיצר כפילויות וקפיצת focus.
   */
  const submit = async () => {
    const clean = priceText.trim();
    if (!clean || !amount || submittingRef.current) return;
    submittingRef.current = true;
    setSubmitting(true);
    const ok = await actions.createLiquidityLevel(questionId, timeframe, side, clean, amount);
    if (ok) {
      setPriceText("");
      setAmount("");
      onSaved?.();
    }
    submittingRef.current = false;
    setSubmitting(false);
    if (!ok) {
      inputRef.current?.focus();
    }
  };

  return (
    <div className="flex min-w-0 items-center gap-1.5">
      <Input
        ref={inputRef}
        value={priceText}
        placeholder="מחיר"
        disabled={submitting}
        dir="ltr"
        className={cn(
          "h-8 min-w-[5.5rem] flex-1 text-right font-bold tabular-nums",
          amount && AMOUNT_TEXT_CLASS[amount],
        )}
        onChange={(e) => setPriceText(e.target.value)}
        onKeyDown={(e) => {
          if (e.key !== "Enter" || e.repeat) return;
          e.preventDefault();
          void submit();
        }}
      />
      <Select
        value={amount}
        disabled={submitting}
        onValueChange={(v) => setAmount(v as LiquidityAmount)}
      >
        <SelectTrigger
          className={cn("h-8 w-[100px] shrink-0 text-xs", amount && AMOUNT_TEXT_CLASS[amount])}
        >
          <SelectValue placeholder="בחר כמות" />
        </SelectTrigger>
        <SelectContent>
          {(Object.keys(AMOUNT_LABELS) as LiquidityAmount[]).map((k) => (
            <SelectItem key={k} value={k} className={AMOUNT_TEXT_CLASS[k]}>
              {AMOUNT_LABELS[k]}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Button
        type="button"
        variant="outline"
        size="icon"
        className="h-8 w-8 shrink-0"
        disabled={!priceText.trim() || !amount || submitting}
        aria-label="הוסף מחיר"
        title="הוסף מחיר"
        onClick={() => void submit()}
      >
        <Plus className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

function SideColumn({
  title,
  side,
  row,
  questionId,
  timeframe,
  actions,
}: {
  title: string;
  side: LiquiditySide;
  row?: LiquidityRow;
  questionId: string;
  timeframe: string;
  actions: StructuredActions;
}) {
  const levels = row?.levels.filter((l) => l.side === side) ?? [];
  const [adding, setAdding] = useState(false);
  return (
    <div dir="rtl" className="space-y-1.5">
      <p className="text-[11px] font-semibold text-muted-foreground">{title}</p>
      <p className="text-[11px] text-muted-foreground">
        <QuantityTotal row={row} side={side} />
      </p>
      {levels.map((l) => (
        <LevelEditor key={l.id} level={l} row={row} actions={actions} />
      ))}
      {(levels.length === 0 || adding) && (
        <VirtualLevelEditor
          questionId={questionId}
          timeframe={timeframe}
          side={side}
          actions={actions}
          onSaved={() => setAdding(false)}
        />
      )}
      {levels.length > 0 && !adding && (
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-7 w-full text-xs"
          onClick={() => setAdding(true)}
        >
          <Plus className="ms-1 h-3 w-3" /> הוסף מחיר
        </Button>
      )}
    </div>
  );
}

function LiquidityRowCard({
  questionId,
  timeframe,
  row,
  actions,
  labels,
  userTextColors,
}: {
  questionId: string;
  timeframe: string;
  row?: LiquidityRow;
  actions: StructuredActions;
  labels?: Labels;
  userTextColors?: WorkspaceUserTextColors;
}) {
  const meta = row ? latestLiquidityMeta(row) : { at: null, by: null };
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 flex items-center justify-between gap-2">
        <span className="rounded-md bg-muted px-2 py-0.5 text-xs font-bold tabular-nums">
          {timeframe}
        </span>
        <div className="flex items-center gap-1.5">
          <UpdatedMeta
            updatedAt={meta.at}
            updatedBy={meta.by}
            labels={labels}
            userTextColors={userTextColors}
          />
          {row &&
            !DEFAULT_TIMEFRAMES.includes(timeframe as (typeof DEFAULT_TIMEFRAMES)[number]) && (
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="h-7 w-7 text-muted-foreground"
                aria-label="מחיקת טווח זמן"
                onClick={() => actions.deleteLiquidityRow(row.id)}
              >
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            )}
        </div>
      </div>
      <div className="grid gap-3 md:grid-cols-2">
        <SideColumn
          title="מחירים מעל"
          side="above"
          row={row}
          questionId={questionId}
          timeframe={timeframe}
          actions={actions}
        />
        <SideColumn
          title="מחירים מתחת"
          side="below"
          row={row}
          questionId={questionId}
          timeframe={timeframe}
          actions={actions}
        />
      </div>
    </div>
  );
}

export function LiquidityTableField({
  questionId,
  rows,
  actions,
  userLabels,
  userTextColors,
}: {
  questionId: string;
  rows: LiquidityRow[];
  actions: StructuredActions;
  userLabels?: Labels;
  userTextColors?: WorkspaceUserTextColors;
}) {
  const rowByTimeframe = useMemo(() => new Map(rows.map((r) => [r.timeframe, r])), [rows]);
  const displayTimeframes = useMemo(
    () =>
      [...new Set([...DEFAULT_TIMEFRAMES, ...rows.map((r) => r.timeframe)])].sort(
        (a, b) => timeframeOrder(a) - timeframeOrder(b),
      ),
    [rows],
  );
  const optional = TIMEFRAMES.filter(
    (t) =>
      !DEFAULT_TIMEFRAMES.includes(t as (typeof DEFAULT_TIMEFRAMES)[number]) &&
      !rowByTimeframe.has(t),
  );

  return (
    <div className="space-y-2">
      {/* בדסקטופ העמודות הפיזיות נשארות משמאל לימין: טווח | מעל | מתחת. בנייד כרטיסים. */}
      <div
        className="hidden grid-cols-[80px_minmax(0,1fr)_minmax(0,1fr)] gap-3 rounded-lg border border-border bg-muted/30 px-3 py-2 text-xs font-semibold md:grid"
        dir="ltr"
      >
        <span>טווח זמן</span>
        <span dir="rtl">מחירים מעל</span>
        <span dir="rtl">מחירים מתחת</span>
      </div>

      {displayTimeframes.map((timeframe) => (
        <LiquidityRowCard
          key={timeframe}
          questionId={questionId}
          timeframe={timeframe}
          row={rowByTimeframe.get(timeframe)}
          actions={actions}
          labels={userLabels}
          userTextColors={userTextColors}
        />
      ))}

      {optional.length > 0 && (
        <Select
          value=""
          onValueChange={(t) => {
            if (t) actions.addLiquidityRow(questionId, t);
          }}
        >
          <SelectTrigger className="h-8 w-full sm:w-48">
            <SelectValue placeholder="+ הוסף טווח זמן" />
          </SelectTrigger>
          <SelectContent>
            {optional.map((t) => (
              <SelectItem key={t} value={t}>
                {t}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      )}
    </div>
  );
}

/* ============================ טבלת חדשות — עריכה ============================ */

function NewsRowEditor({
  row,
  actions,
  labels,
  userTextColors,
  onAddRow,
}: {
  row: NewsRow;
  actions: StructuredActions;
  labels?: Labels;
  userTextColors?: WorkspaceUserTextColors;
  onAddRow: () => void;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-start">
        <DebouncedTextarea
          value={row.body}
          placeholder="טקסט החדשה"
          className={cn("flex-1 text-sm", IMPORTANCE_TEXT_CLASS[row.importance])}
          onCommit={(v) => actions.updateNewsRow(row.id, { body: v })}
          onAddRow={onAddRow}
        />
        <div className="flex items-center gap-1.5">
          <Select
            value={row.importance}
            onValueChange={(v) =>
              actions.updateNewsRow(row.id, { importance: v as NewsImportance })
            }
          >
            <SelectTrigger
              className={cn("h-9 w-[135px] text-xs", IMPORTANCE_TEXT_CLASS[row.importance])}
            >
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {IMPORTANCE_OPTIONS.map((o) => (
                <SelectItem
                  key={o.value}
                  value={o.value}
                  className={IMPORTANCE_TEXT_CLASS[o.value]}
                >
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <UpdatedMeta
            updatedAt={row.updated_at}
            updatedBy={row.updated_by}
            labels={labels}
            userTextColors={userTextColors}
          />
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="h-8 w-8 text-muted-foreground"
            aria-label="מחיקת שורת חדשות"
            onClick={() => {
              if (window.confirm("למחוק את שורת החדשות?")) actions.deleteNewsRow(row.id);
            }}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>
    </div>
  );
}

function VirtualNewsRow({
  questionId,
  actions,
}: {
  questionId: string;
  actions: StructuredActions;
}) {
  const [importance, setImportance] = useState<NewsImportance>("neutral");
  const [body, setBody] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const submittingRef = useRef(false);
  const inputRef = useRef<HTMLTextAreaElement | null>(null);
  useEffect(() => {
    if (!inputRef.current) return;
    inputRef.current.style.height = "auto";
    inputRef.current.style.height = `${Math.max(36, inputRef.current.scrollHeight)}px`;
  }, [body]);
  const submit = async () => {
    const clean = body.trim();
    if (!clean || submittingRef.current) return;
    submittingRef.current = true;
    setSubmitting(true);
    const saved = await actions.createNewsRow(questionId, clean, importance);
    if (saved) setBody("");
    submittingRef.current = false;
    setSubmitting(false);
  };
  return (
    <div className="rounded-lg border border-dashed border-border bg-card/60 p-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-start">
        <Textarea
          ref={inputRef}
          rows={1}
          value={body}
          disabled={submitting}
          placeholder="טקסט החדשה"
          className={cn(
            "min-h-9 flex-1 resize-none overflow-hidden text-sm",
            IMPORTANCE_TEXT_CLASS[importance],
          )}
          onChange={(event) => setBody(event.target.value)}
          onKeyDown={(event) => {
            if (event.key !== "Enter" || !event.ctrlKey || event.repeat) return;
            event.preventDefault();
            void submit();
          }}
        />
        <Select
          value={importance}
          disabled={submitting}
          onValueChange={(v) => setImportance(v as NewsImportance)}
        >
          <SelectTrigger className={cn("h-9 w-[135px] text-xs", IMPORTANCE_TEXT_CLASS[importance])}>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {IMPORTANCE_OPTIONS.map((o) => (
              <SelectItem key={o.value} value={o.value} className={IMPORTANCE_TEXT_CLASS[o.value]}>
                {o.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button
          type="button"
          size="sm"
          disabled={!body.trim() || submitting}
          onClick={() => void submit()}
        >
          <Plus className="ms-1 h-3 w-3" /> שמור חדשה
        </Button>
      </div>
    </div>
  );
}

export function NewsTableField({
  questionId,
  rows,
  actions,
  userLabels,
  userTextColors,
}: {
  questionId: string;
  rows: NewsRow[];
  actions: StructuredActions;
  userLabels?: Labels;
  userTextColors?: WorkspaceUserTextColors;
}) {
  const sorted = [...rows].sort((a, b) => a.position - b.position);
  const addingRef = useRef(false);
  const [adding, setAdding] = useState(false);
  const addRow = async () => {
    if (addingRef.current) return;
    addingRef.current = true;
    setAdding(true);
    try {
      await actions.addNewsRow(questionId);
    } finally {
      addingRef.current = false;
      setAdding(false);
    }
  };
  return (
    <div className="space-y-2">
      {sorted.map((row) => (
        <NewsRowEditor
          key={row.id}
          row={row}
          actions={actions}
          labels={userLabels}
          userTextColors={userTextColors}
          onAddRow={() => void addRow()}
        />
      ))}
      {sorted.length === 0 && (
        /* שורת ברירת מחדל אחת בלבד. אחרי השמירה הראשונה היא נעלמת;
           שורות נוספות נוצרות רק בלחיצה מפורשת על הכפתור. */
        <VirtualNewsRow questionId={questionId} actions={actions} />
      )}
      {sorted.length > 0 && (
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-8 text-xs"
          disabled={adding}
          onClick={() => void addRow()}
        >
          <Plus className="ms-1 h-3 w-3" /> הוסף שורה נוספת
        </Button>
      )}
    </div>
  );
}

/* ============================== תצוגה בלבד ============================== */

export function StructuredView({
  snapshot,
  richTextNews = false,
}: {
  snapshot: StructuredSnapshot | null;
  richTextNews?: boolean;
}) {
  if (!snapshot) return <span>—</span>;

  if (snapshot.type === LIQUIDITY_TYPE) {
    const rows = (snapshot.rows as LiquidityRow[]).filter((r) =>
      r.levels.some((l) => l.price_text.trim()),
    );
    if (!rows.length) return <span>—</span>;
    return (
      <div className="space-y-2">
        {rows
          .sort((a, b) => timeframeOrder(a.timeframe) - timeframeOrder(b.timeframe))
          .map((row) => (
            <div key={row.id} className="rounded-md border border-border/70 p-2">
              <span className="rounded bg-muted px-1.5 py-0.5 text-[11px] font-bold tabular-nums">
                {row.timeframe}
              </span>
              <div className="mt-1.5 grid gap-2 sm:grid-cols-2">
                {(["above", "below"] as LiquiditySide[]).map((side) => (
                  <div key={side}>
                    <p className="text-[11px] font-semibold text-muted-foreground">
                      {side === "above" ? "מעל" : "מתחת"}
                    </p>
                    <p className="text-[11px] text-muted-foreground">
                      <QuantityTotal row={row} side={side} />
                    </p>
                    <ul className="space-y-0.5">
                      {row.levels
                        .filter((l) => l.side === side && l.price_text.trim())
                        .map((l) => (
                          <li
                            key={l.id}
                            className={cn(
                              "text-sm font-bold tabular-nums",
                              AMOUNT_TEXT_CLASS[l.amount],
                              l.taken && "line-through text-muted-foreground opacity-60",
                            )}
                          >
                            <LtrNum>{savedLiquidityPriceText(row, l)}</LtrNum>
                            <span className="ms-1 text-[11px] font-normal opacity-80">
                              ({AMOUNT_LABELS[l.amount]})
                            </span>
                            {coinglassQuantityText(row, l, side) ? (
                              <span className="ms-1 text-xs font-normal">
                                <LtrNum>{coinglassQuantityText(row, l, side)}</LtrNum>
                              </span>
                            ) : null}
                            {l.price_precision ? (
                              <span className="block text-[10px] font-normal text-muted-foreground">
                                {levelPrecisionText(l)}
                              </span>
                            ) : null}
                          </li>
                        ))}
                    </ul>
                  </div>
                ))}
              </div>
            </div>
          ))}
      </div>
    );
  }

  const rows = (snapshot.rows as NewsRow[]).filter((r) => r.body.trim());
  if (!rows.length) return <span>—</span>;
  return (
    <ul className="space-y-1.5">
      {rows.map((r) => (
        <li
          key={r.id}
          className={cn(
            "rounded-md border border-border/60 px-2 py-1.5 text-sm",
            IMPORTANCE_TEXT_CLASS[r.importance],
            richTextNews && newsSentimentStyle[r.importance],
          )}
        >
          {richTextNews ? (
            <RichTextView value={r.body} imageMode="inline" />
          ) : (
            <span className="whitespace-pre-wrap">{r.body}</span>
          )}
          <span
            className={cn(
              "ms-1 text-[11px] opacity-80",
              (r as NewsRow & { rating_pending?: boolean }).rating_pending &&
                "text-yellow-700 dark:text-yellow-300",
            )}
          >
            ({importanceLabel(r.importance)})
          </span>
        </li>
      ))}
    </ul>
  );
}

export { LIQUIDITY_TYPE, NEWS_TYPE };
