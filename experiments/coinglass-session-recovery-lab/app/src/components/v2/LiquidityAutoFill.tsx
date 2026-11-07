/**
 * כפתור "הזן אוטומטית" עבור שדה Coinglass - Model 1.
 * ההפעלה פותחת משימה אחת בשירות האיסוף החיצוני, והלקוח רק מסקר (GET) עד
 * שהתוצאה נשמרת. מזהה הבקשה נשמר מקומית לפי משתמש+גיליון+שדה כדי שרענון
 * ימשיך את אותה משימה — אף פעם לא נפתחת סריקה חדשה בתשלום מאליה.
 */
import { LtrNum } from "@/components/LtrNumeric";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { Bot, Loader2 } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { lastCoinglassRun } from "@/lib/coinglassHeatmap.functions";
import {
  startModel1Collection,
  pollModel1Collection,
  reviewSavedModel1Observation,
  saveSavedModel1Observation,
} from "@/lib/coinglassCollect.functions";
import type { CollectResponse, SavedObservation } from "@/lib/coinglassCollectCore";
import {
  COLLECTOR_TIMEFRAMES,
  COLLECTOR_SUPPORT_NOTE,
  HEATMAP_MODEL_LABEL,
  SAVED_OBSERVATION_LABEL,
  SAVED_OBSERVATION_STALE_NOTE,
  ageText,
  omittedZonesText,
  PRICE_RANGE_LABEL,
  priceRangeValueText,
  levelPrecisionText,
  savedObservationOriginText,
  type CollectorTimeframe,
  type HeatmapModel,
} from "@/lib/coinglassCollector";
import { batchWarning } from "@/lib/coinglassBatch";
import { useCoinglassBatch } from "@/components/v2/useCoinglassBatch";

import {
  clearPending,
  readChoice,
  readPending,
  writeChoice,
  tryWritePending,
  isQuotaBlocked,
  validRetryAt,
  POLL_INTERVAL_MS,
  MAX_POLLS,
  type PendingCollection,
} from "@/lib/coinglassPending";
import { AMOUNT_LABELS, type LiquidityAmount } from "@/lib/structured";
import { supabase } from "@/integrations/supabase/client";

const formatTime = (iso: string | null | undefined) =>
  iso
    ? new Intl.DateTimeFormat("he-IL", {
        timeZone: "Asia/Jerusalem",
        dateStyle: "short",
        timeStyle: "short",
      }).format(new Date(iso))
    : "";

type Props = {
  sessionId: string;
  fieldId: string;
  model: HeatmapModel;
  baselineUpdatedAt: string | null;
  onDone: () => void;
};

export function LiquidityAutoFill({ sessionId, fieldId, model, baselineUpdatedAt, onDone }: Props) {
  const [choice, setChoice] = useState<CollectorTimeframe>("12H");
  const [result, setResult] = useState<CollectResponse | null>(null);
  const [pending, setPending] = useState<PendingCollection | null>(null);
  const [polls, setPolls] = useState(0);
  const [paused, setPaused] = useState(false);
  const [starting, setStarting] = useState(false);
  /** מכסת איסופים חדשים: עד המועד הזה לא נשלחת בקשה נוספת. null = לא ידוע. */
  const [retryAt, setRetryAt] = useState<string | null>(null);
  const [rateLimited, setRateLimited] = useState(false);
  const [observation, setObservation] = useState<SavedObservation | null>(null);
  const [reviewNote, setReviewNote] = useState<string | null>(null);
  const [reviewing, setReviewing] = useState(false);
  const [importing, setImporting] = useState(false);
  /** פעימה לצורך רענון התצוגה בלבד כשחלון ההמתנה נגמר — לעולם לא שולחת בקשה. */
  const [tick, setTick] = useState(0);
  const busyRef = useRef(false);
  const queryClient = useQueryClient();
  const start = useServerFn(startModel1Collection);
  const poll = useServerFn(pollModel1Collection);
  const review = useServerFn(reviewSavedModel1Observation);
  const saveObservation = useServerFn(saveSavedModel1Observation);
  const readLastRun = useServerFn(lastCoinglassRun);

  // מזהה המשתמש נשמר מעודכן: החלפת משתמש או התנתקות מאפסות מיד כל מצב מקומי.
  const [userId, setUserId] = useState<string | null>(null);
  useEffect(() => {
    let active = true;
    const apply = (next: string | null) => {
      if (!active) return;
      setUserId((current) => {
        if (current !== next) {
          setPending(null);
          setResult(null);
          setPolls(0);
          setPaused(false);
          setObservation(null);
          setReviewNote(null);
          setRateLimited(false);
          setRetryAt(null);
        }
        return next;
      });
    };
    void supabase.auth.getUser().then(({ data }) => apply(data.user?.id ?? null));
    const { data: sub } = supabase.auth.onAuthStateChange((_event, session) =>
      apply(session?.user?.id ?? null),
    );
    return () => {
      active = false;
      sub.subscription.unsubscribe();
    };
  }, []);

  const identity = useMemo(
    () => (userId ? { userId, sessionId, fieldId, model } : null),
    [userId, sessionId, fieldId, model],
  );
  /** הזהות המלאה כוללת את הטווח — מצב ממתין נפרד לכל 12H/24H/48H. */
  const pendingIdentity = useMemo(
    () => (identity ? { ...identity, timeframe: choice } : null),
    [identity, choice],
  );
  /** תשובה שמגיעה אחרי החלפת משתמש/גיליון/שדה/טווח נזרקת ולא מוצגת. */
  const identityRef = useRef(pendingIdentity);
  identityRef.current = pendingIdentity;
  const sameIdentity = useCallback(
    (snapshot: typeof pendingIdentity) =>
      snapshot !== null &&
      identityRef.current !== null &&
      snapshot.userId === identityRef.current.userId &&
      snapshot.sessionId === identityRef.current.sessionId &&
      snapshot.fieldId === identityRef.current.fieldId &&
      snapshot.timeframe === identityRef.current.timeframe,
    [],
  );

  // שחזור הטווח שנבחר אחרי רענון — קריאה בלבד, בלי שום בקשה לשרת.
  useEffect(() => {
    if (!identity) return;
    const stored = readChoice(window.localStorage, identity);
    if (stored) setChoice(stored);
  }, [identity]);

  const chooseTimeframe = useCallback(
    (value: CollectorTimeframe) => {
      setChoice(value);
      if (identity) writeChoice(window.localStorage, identity, value);
    },
    [identity],
  );

  // חידוש אחרי רענון — רק עבור אותו משתמש, גיליון, שדה וטווח. סקר בלבד.
  useEffect(() => {
    if (!pendingIdentity) {
      setRateLimited(false);
      setRetryAt(null);
      return;
    }
    const stored = readPending(window.localStorage, pendingIdentity);
    setPending(stored);
    setResult(null);
    setPolls(0);
    setPaused(false);
    setObservation(null);
    setReviewNote(null);
    // מצב המכסה משוחזר מהרשומה עצמה — בלי לשנות מזהה בקשה, כרטיס או בסיס.
    setRateLimited(stored?.rateLimited === true);
    setRetryAt(validRetryAt(stored?.retryAt));
  }, [pendingIdentity]);

  const forget = useCallback(() => {
    if (pendingIdentity) clearPending(window.localStorage, pendingIdentity);
    setPending(null);
    setPaused(false);
    setPolls(0);
    setRateLimited(false);
    setRetryAt(null);
  }, [pendingIdentity]);

  const lastRun = useQuery({
    queryKey: ["coinglass-last-run", userId, sessionId, fieldId],
    queryFn: () => readLastRun({ data: { sessionId, fieldId } }),
    enabled: Boolean(userId),
  });

  const refreshSheet = useCallback(async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["v2-collection-values", sessionId] }),
      queryClient.invalidateQueries({ queryKey: ["v2-collection-snapshot", sessionId] }),
      queryClient.invalidateQueries({ queryKey: ["v2-collection-history", sessionId] }),
      queryClient.invalidateQueries({ queryKey: ["collection-autofill-scans"] }),
      queryClient.invalidateQueries({
        queryKey: ["coinglass-last-run", userId, sessionId, fieldId],
      }),
    ]);
    onDone();
  }, [queryClient, userId, sessionId, fieldId, onDone]);

  const handle = useCallback(
    async (response: CollectResponse) => {
      setResult(response);
      if (response.status === "saved" || response.status === "duplicate") {
        forget();
        toast.success(response.status === "saved" ? "הנתונים נשמרו" : "התוצאה כבר נשמרה");
        await refreshSheet();
      } else if (response.status === "rate_limited") {
        // מכסה — לא כישלון: הבקשה הממתינה נשמרת, ואין ניסיון חוזר אוטומטי.
        const until = validRetryAt(response.retryAt);
        setRateLimited(true);
        setRetryAt(until);
        // נשמר ברשומה הקיימת כדי שרענון ימשיך לחסום — אותו מזהה, אותו כרטיס.
        setPending((current) => {
          if (!current) return current;
          const next: PendingCollection = { ...current, rateLimited: true, retryAt: until };
          tryWritePending(window.localStorage, next);
          return next;
        });
      } else if (response.status === "not_connected") {
        forget();
        toast.error("האיסוף האוטומטי אינו מחובר", { description: response.prerequisite });
      } else if (response.status === "conflict" || response.status === "stale") {
        forget();
        toast.error(response.note);
      } else if (response.status === "failed" && !response.retryable) {
        forget();
        toast.error(response.note);
      }
    },
    [forget, refreshSheet],
  );

  /**
   * התחלה או חידוש התחלה. מזהה הבקשה, גרסת הבסיס והטווח נשמרים לפני הקריאה,
   * כך שתשובה שאבדה או רענון ממשיכים עם אותה משימה בדיוק.
   */
  const beginOrResend = useCallback(async () => {
    if (busyRef.current || !pendingIdentity) return;
    // חסימת מכסה חלה גם על התחלה חדשה וגם על "המשך את אותה בקשה".
    if (isQuotaBlocked({ rateLimited, retryAt }, Date.now())) return;
    const existing = readPending(window.localStorage, pendingIdentity);
    if (existing?.ticket) return;
    busyRef.current = true;
    setStarting(true);
    setResult(null);
    const snapshot = pendingIdentity;
    const record: PendingCollection = existing ?? {
      requestId: crypto.randomUUID(),
      baselineUpdatedAt,
      ...pendingIdentity,
      ticket: null,
    };
    try {
      // השמירה המקומית חייבת לקרות *לפני* כל קריאת רשת. אם היא נכשלת
      // (אחסון מלא/מושבת) לא שולחים שום בקשה והפקדים משוחזרים ב-finally.
      if (!tryWritePending(window.localStorage, record)) {
        toast.error("לא ניתן לשמור את מצב האיסוף בדפדפן — האיסוף לא התחיל");
        return;
      }
      setPending(record);

      const response = await start({
        data: {
          sessionId,
          fieldId,
          timeframe: record.timeframe,
          requestId: record.requestId,
          baselineUpdatedAt: record.baselineUpdatedAt,
          expectedModel: model,
        },
      });
      if (!sameIdentity(snapshot)) return;
      if (response.ticket) {
        const next: PendingCollection = { ...record, ticket: response.ticket };
        // אם אחסון הדפדפן נכשל כאן, הכרטיס עדיין נשמר בזיכרון והסקר ממשיך.
        tryWritePending(window.localStorage, next);
        setPending(next);
        setPolls(0);
        setPaused(false);
      }
      await handle(response);
    } catch (error) {
      if (!sameIdentity(snapshot)) return;
      // כשל רשת מעורפל: מזהה הבקשה כבר שמור, וניסיון חוזר לא יפתח סריקה חדשה.
      setResult({
        status: "failed",
        note: error instanceof Error ? error.message : "ההתחלה נכשלה — אפשר לנסות עם אותה בקשה",
        retryable: true,
      });
      setPaused(true);
    } finally {
      setStarting(false);
      busyRef.current = false;
    }
  }, [
    pendingIdentity,
    baselineUpdatedAt,
    start,
    sessionId,
    fieldId,
    model,
    handle,
    sameIdentity,
    rateLimited,
    retryAt,
  ]);

  const runPoll = useCallback(async () => {
    if (busyRef.current || !pending?.ticket) return;
    busyRef.current = true;
    const snapshot = pendingIdentity;

    const ticket = pending.ticket;
    try {
      const response = await poll({ data: { ticket } });
      if (!sameIdentity(snapshot)) return;
      setPolls((count) => count + 1);
      await handle(response);
    } catch (error) {
      if (!sameIdentity(snapshot)) return;
      setResult({
        status: "running",
        note: error instanceof Error ? error.message : "הסקר נכשל — אפשר להמשיך",
        retryable: true,
      });
      setPaused(true);
    } finally {
      busyRef.current = false;
    }
  }, [pending, pendingIdentity, poll, handle, sameIdentity]);

  const waiting =
    Boolean(pending?.ticket) &&
    (!result || result.status === "queued" || result.status === "running");

  useEffect(() => {
    if (!waiting || paused) return;
    if (polls >= MAX_POLLS) {
      setPaused(true);
      return;
    }
    const timer = window.setTimeout(() => void runPoll(), polls === 0 ? 1_500 : POLL_INTERVAL_MS);
    return () => window.clearTimeout(timer);
  }, [waiting, paused, polls, runPoll]);

  /**
   * שעון תצוגה בלבד: כשחלון ההמתנה נגמר הפקדים נפתחים וההודעה מתעדכנת.
   * הוא לעולם אינו שולח בקשה ואינו מתחיל איסוף.
   */
  useEffect(() => {
    if (!rateLimited || !retryAt) return;
    const remaining = Date.parse(retryAt) - Date.now();
    if (!Number.isFinite(remaining) || remaining <= 0) return;
    const timer = window.setTimeout(
      () => setTick((value) => value + 1),
      Math.min(remaining + 250, 30_000),
    );
    return () => window.clearTimeout(timer);
  }, [rateLimited, retryAt, tick]);

  /** צפייה באיסוף שמור — קריאה בלבד, זמינה גם כשמכסת האיסופים נוצלה. */
  const openSaved = useCallback(async () => {
    if (reviewing) return;
    setReviewing(true);
    setObservation(null);
    setReviewNote(null);
    const snapshot = pendingIdentity;
    try {
      const response = await review({ data: { sessionId, fieldId, timeframe: choice } });
      if (!sameIdentity(snapshot)) return;
      setReviewNote(response.note + (response.prerequisite ? ` (${response.prerequisite})` : ""));
      setObservation(response.observation ?? null);
    } catch (error) {
      if (!sameIdentity(snapshot)) return;
      setReviewNote(error instanceof Error ? error.message : "קריאת האיסוף השמור נכשלה");
    } finally {
      setReviewing(false);
    }
  }, [review, reviewing, sessionId, fieldId, choice, pendingIdentity, sameIdentity]);

  /** אישור מפורש: העתקת אותה תצפית לתיבה עם זמן האיסוף המקורי. */
  const confirmSaved = useCallback(async () => {
    if (importing || !observation) return;
    setImporting(true);
    const snapshot = pendingIdentity;
    try {
      const response = await saveObservation({
        data: { reviewTicket: observation.reviewTicket, confirm: true },
      });
      if (!sameIdentity(snapshot)) return;
      setReviewNote(response.note);
      if (response.status === "saved" || response.status === "duplicate") {
        setObservation(null);
        toast.success(response.note);
        await refreshSheet();
      } else {
        toast.error(response.note);
      }
    } catch (error) {
      if (!sameIdentity(snapshot)) return;
      setReviewNote(error instanceof Error ? error.message : "השמירה נכשלה");
    } finally {
      setImporting(false);
    }
  }, [importing, observation, saveObservation, pendingIdentity, sameIdentity, refreshSheet]);

  const busy = starting || waiting;
  const last = lastRun.data;
  // המשימה נפתחה אך התשובה אבדה — חידוש עם אותו מזהה בקשה, לא סריקה חדשה.
  const needsResend = Boolean(pending) && !pending?.ticket && !starting;
  const canRetrySave = Boolean(pending?.ticket) && Boolean(result?.retryable) && !waiting;
  // חסימת התחלה חדשה עד המועד שהשירות מסר; זמן לא ידוע => ניסיון ידני מותר.
  void tick; // הפעימה רק מרעננת את החישוב הבא כשחלון ההמתנה נגמר.
  const quotaBlocked = isQuotaBlocked({ rateLimited, retryAt }, Date.now());

  /** "כל הטווחים" — אותו מתאם תור, פריט אחד בכל רגע. */
  const batch = useCoinglassBatch(sessionId, userId, onDone);
  const runAllTimeframes = useCallback(() => {
    if (batch.running) return;
    if (!window.confirm(batchWarning(COLLECTOR_TIMEFRAMES.length))) return;
    batch.startBatch([{ fieldId, model, label: HEATMAP_MODEL_LABEL[model] }]);
  }, [batch, fieldId, model]);

  return (
    <div className="flex flex-col gap-2" dir="rtl">
      <div className="flex flex-wrap items-center gap-2">
        <Select
          value={choice}
          onValueChange={(value) => chooseTimeframe(value as CollectorTimeframe)}
          disabled={busy || Boolean(pending)}
        >
          <SelectTrigger className="h-9 w-32" aria-label="טווח זמן לאיסוף">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {COLLECTOR_TIMEFRAMES.map((timeframe) => (
              <SelectItem key={timeframe} value={timeframe}>
                {timeframe}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button
          type="button"
          size="sm"
          variant="secondary"
          onClick={() => void beginOrResend()}
          disabled={busy || Boolean(pending) || quotaBlocked}
          aria-label="הזן אוטומטית"
          title="הזן אוטומטית"
          className="gap-2"
        >
          {busy ? (
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
          ) : (
            <Bot className="h-4 w-4" aria-hidden />
          )}
          <span className="hidden sm:inline">{busy ? "אוסף…" : "הזן אוטומטית"}</span>
        </Button>
        {busy ? (
          <span className="text-xs text-muted-foreground" role="status">
            {starting ? "מתחיל איסוף…" : "ממתין לאיסוף שכבר רץ…"}
          </span>
        ) : null}
        {needsResend ? (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            disabled={quotaBlocked}
            onClick={() => void beginOrResend()}
          >
            המשך את אותה בקשה
          </Button>
        ) : null}
        {pending?.ticket && paused ? (
          <Button type="button" size="sm" variant="ghost" onClick={() => void runPoll()}>
            המשך לבדוק את אותו איסוף
          </Button>
        ) : null}
        {canRetrySave ? (
          <Button type="button" size="sm" variant="ghost" onClick={() => void runPoll()}>
            נסה לשמור שוב
          </Button>
        ) : null}
        {pending && !waiting ? (
          <Button type="button" size="sm" variant="ghost" onClick={forget}>
            הפסק לעקוב אחרי האיסוף הזה
          </Button>
        ) : null}
        {/* פעולה נפרדת: קריאה בלבד של מה שכבר נאסף — לא מפעילה צילום חדש. */}
        <Button
          type="button"
          size="sm"
          variant="outline"
          onClick={() => void openSaved()}
          disabled={reviewing || importing}
        >
          {reviewing ? "טוען איסוף שמור…" : "צפייה באיסוף שמור"}
        </Button>
        {/* כל הטווחים: 12H ואז 24H ואז 48H, אחד אחרי השני. */}
        <Button
          type="button"
          size="sm"
          variant="outline"
          onClick={runAllTimeframes}
          disabled={busy || batch.running || quotaBlocked}
        >
          {batch.running ? `אוסף ${batch.progress.done}/${batch.progress.total}…` : "כל הטווחים"}
        </Button>
        {batch.running ? (
          <Button type="button" size="sm" variant="ghost" onClick={batch.stop}>
            {batch.stopRequested ? "עוצר אחרי הפריט הנוכחי…" : "עצור אחרי הפריט הנוכחי"}
          </Button>
        ) : null}
      </div>

      {batch.entries.length > 0 ? (
        <p className="text-xs text-muted-foreground" role="status">
          {`${HEATMAP_MODEL_LABEL[model]}: ${batch.progress.done}/${batch.progress.total} טווחים נשמרו`}
          {batch.entries.find((entry) => entry.status === "stopped" || entry.status === "failed")
            ?.note
            ? ` · ${batch.entries.find((entry) => entry.status === "stopped" || entry.status === "failed")?.note}`
            : ""}
        </p>
      ) : null}

      <p className="text-xs text-muted-foreground">{COLLECTOR_SUPPORT_NOTE}</p>

      {last ? (
        <div className="space-y-1 text-xs text-muted-foreground break-words">
          <p>
            איסוף AI אחרון: {last.timeframe} · {formatTime(last.capturedAt)}
            {last.provider ? ` · ${last.provider}` : ""}
          </p>
          {last.observedPriceRange ? (
            <p>
              {PRICE_RANGE_LABEL} <LtrNum>{priceRangeValueText(last.observedPriceRange)}</LtrNum>
            </p>
          ) : last.observedPrice !== null ? (
            <p>
              מחיר מוערך במפה:{" "}
              <LtrNum>${Math.round(last.observedPrice).toLocaleString("en-US")}</LtrNum>
            </p>
          ) : null}
          {last.omittedAmbiguousZones > 0 ? (
            <p>{omittedZonesText(last.omittedAmbiguousZones)}</p>
          ) : null}
          {/* נשמר אחרי רענון: זמן התפיסה המקורי, לא "עודכן עכשיו". */}
          {last.importedFromStoredObservation && last.capturedAt ? (
            <p>{savedObservationOriginText(last.capturedAt)}</p>
          ) : null}
        </div>
      ) : null}

      {reviewNote ? (
        <p className="text-xs text-muted-foreground break-words" role="status">
          {reviewNote}
        </p>
      ) : null}

      {observation ? (
        <div className="space-y-1 rounded-md border p-2 text-xs break-words">
          <p className="font-medium">{SAVED_OBSERVATION_LABEL}</p>
          <p className="text-muted-foreground">
            {observation.timeframe} · זמן האיסוף המקורי: {formatTime(observation.capturedAt)} · גיל:{" "}
            {ageText(observation.ageMs)}
          </p>
          {observation.stale ? (
            <p className="text-destructive">{SAVED_OBSERVATION_STALE_NOTE}</p>
          ) : null}
          {observation.observedPriceRange ? (
            <p className="text-muted-foreground">
              {PRICE_RANGE_LABEL}{" "}
              <LtrNum>{priceRangeValueText(observation.observedPriceRange)}</LtrNum>
            </p>
          ) : observation.observedPrice !== null ? (
            <p className="text-muted-foreground">
              מחיר מוערך במפה:{" "}
              <LtrNum>${Math.round(observation.observedPrice).toLocaleString("en-US")}</LtrNum>
            </p>
          ) : null}
          {observation.omittedAmbiguousZones > 0 ? (
            <p className="text-muted-foreground">
              {omittedZonesText(observation.omittedAmbiguousZones)}
            </p>
          ) : null}
          <table className="w-full text-right">
            <thead className="text-muted-foreground">
              <tr>
                <th scope="col">צד</th>
                <th scope="col">מחיר</th>
                <th scope="col">עוצמה</th>
              </tr>
            </thead>
            <tbody>
              {observation.levels.map((level, index) => (
                <tr key={`${level.price_text}-${index}`}>
                  <td>{level.side === "above" ? "מעל" : "מתחת"}</td>
                  <td>
                    <LtrNum>{level.price_text}</LtrNum>
                    <span className="block text-xs text-muted-foreground">
                      {levelPrecisionText(level)}
                    </span>
                  </td>
                  <td>{AMOUNT_LABELS[level.amount as LiquidityAmount] ?? level.amount}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <Button
            type="button"
            size="sm"
            variant="secondary"
            onClick={() => void confirmSaved()}
            disabled={importing}
          >
            {importing ? "שומר…" : "שמור לתיבה עם זמן האיסוף המקורי"}
          </Button>
        </div>
      ) : null}

      {result ? (
        <div className="space-y-1 text-xs break-words">
          <p
            className={
              result.status === "saved" || result.status === "duplicate"
                ? "text-muted-foreground"
                : result.status === "queued" ||
                    result.status === "running" ||
                    result.status === "stale"
                  ? "text-muted-foreground"
                  : "text-destructive"
            }
          >
            {result.note}
            {result.prerequisite ? ` (${result.prerequisite})` : ""}
          </p>
          {result.observedPriceRange ? (
            <p className="text-muted-foreground">
              {PRICE_RANGE_LABEL} <LtrNum>{priceRangeValueText(result.observedPriceRange)}</LtrNum>
            </p>
          ) : typeof result.observedPrice === "number" ? (
            <p className="text-muted-foreground">
              מחיר מוערך במפה:{" "}
              <LtrNum>${Math.round(result.observedPrice).toLocaleString("en-US")}</LtrNum>
            </p>
          ) : null}
          {result.omittedAmbiguousZones ? (
            <p className="text-muted-foreground">
              {omittedZonesText(result.omittedAmbiguousZones)}
            </p>
          ) : null}
          {result.capturedAt ? (
            <p className="text-muted-foreground">
              זמן תפיסה במקור: {formatTime(result.capturedAt)}
              {result.provider ? ` · ${result.provider}` : ""}
              {result.levels ? ` · ${result.levels} אזורים` : ""}
            </p>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

export default LiquidityAutoFill;
