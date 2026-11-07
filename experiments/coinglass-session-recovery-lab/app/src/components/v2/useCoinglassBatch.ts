/**
 * מריץ תור איסוף סדרתי אחד. משמש גם ל"כל הטווחים" בשדה יחיד וגם ל"איסוף כל
 * מפות CoinGlass" בגיליון — מימוש אחד בלבד.
 *
 * כללים: פריט אחד בכל רגע (אף פעם לא שתי סריקות במקביל); מזהה הבקשה נשמר
 * מקומית לפני כל קריאה; גרסת הבסיס נקראת מחדש מהמסד לפני כל פריט; עצירה
 * מיידית במכסה/התנגשות/ניתוק; כשל מקור בפריט אחד ממשיך לפריט הבא בלי ניסיון
 * חוזר אוטומטי.
 */
import { useCallback, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { supabase } from "@/integrations/supabase/client";
import { pollModel1Collection, startModel1Collection } from "@/lib/coinglassCollect.functions";
import type { CollectResponse } from "@/lib/coinglassCollectCore";
import {
  applyOutcome,
  batchProgress,
  buildBatch,
  markStatus,
  nextPendingIndex,
  shouldStopQueue,
  type BatchEntry,
  type BatchTarget,
} from "@/lib/coinglassBatch";
import {
  clearPending,
  isQuotaBlocked,
  readPending,
  tryWritePending,
  validRetryAt,
  POLL_INTERVAL_MS,
  MAX_POLLS,
  type PendingCollection,
} from "@/lib/coinglassPending";

const wait = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

const terminal = (response: CollectResponse) =>
  response.status !== "queued" && response.status !== "running";

export function useCoinglassBatch(sessionId: string, userId: string | null, onDone: () => void) {
  const [entries, setEntries] = useState<BatchEntry[]>([]);
  const [running, setRunning] = useState(false);
  const [stopRequested, setStopRequested] = useState(false);
  const stopRef = useRef(false);
  const runningRef = useRef(false);
  const queryClient = useQueryClient();
  const start = useServerFn(startModel1Collection);
  const poll = useServerFn(pollModel1Collection);

  const refresh = useCallback(async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["v2-collection-values", sessionId] }),
      queryClient.invalidateQueries({ queryKey: ["v2-collection-snapshot", sessionId] }),
      queryClient.invalidateQueries({ queryKey: ["v2-collection-history", sessionId] }),
    ]);
    onDone();
  }, [queryClient, sessionId, onDone]);

  /** גרסת הבסיס נקראת מחדש לפני כל פריט — שמירת טווח קודם מקדמת אותה. */
  const freshBaseline = useCallback(
    async (fieldId: string) => {
      const { data, error } = await supabase
        .from("v2_collection_values")
        .select("updated_at")
        .eq("session_id", sessionId)
        .eq("field_id", fieldId)
        .maybeSingle();
      if (error) throw new Error("קריאת מצב השדה נכשלה — התור נעצר");
      return (data?.updated_at as string | null) ?? null;
    },
    [sessionId],
  );

  const runQueue = useCallback(
    async (initial: BatchEntry[]) => {
      if (runningRef.current || !userId) return;
      runningRef.current = true;
      stopRef.current = false;
      setStopRequested(false);
      setRunning(true);
      let current = initial;
      setEntries(current);

      try {
        for (;;) {
          if (stopRef.current) break;
          const index = nextPendingIndex(current);
          if (index === null) break;
          const item = current[index]!;
          current = markStatus(current, index, "running");
          setEntries(current);

          const identity = {
            userId,
            sessionId,
            fieldId: item.fieldId,
            timeframe: item.timeframe,
            model: item.model,
          };

          let response: CollectResponse;
          try {
            const existing = readPending(window.localStorage, identity);
            if (isQuotaBlocked(existing, Date.now())) {
              current = markStatus(current, index, "stopped", "מכסת האיסופים נוצלה — התור נעצר");
              setEntries(current);
              break;
            }
            const baseline = existing
              ? existing.baselineUpdatedAt
              : await freshBaseline(item.fieldId);
            const record: PendingCollection = existing ?? {
              requestId: crypto.randomUUID(),
              baselineUpdatedAt: baseline,
              ...identity,
              ticket: null,
            };
            // שמירה מקומית לפני כל קריאה — רענון ימשיך את אותה משימה.
            if (!tryWritePending(window.localStorage, record)) {
              current = markStatus(
                current,
                index,
                "stopped",
                "לא ניתן לשמור את מצב האיסוף בדפדפן — התור נעצר",
              );
              setEntries(current);
              break;
            }

            let ticket = record.ticket ?? null;
            if (ticket) {
              response = { status: "running", note: "ממשיך איסוף שכבר רץ" };
            } else {
              response = await start({
                data: {
                  sessionId,
                  fieldId: item.fieldId,
                  timeframe: item.timeframe,
                  requestId: record.requestId,
                  baselineUpdatedAt: record.baselineUpdatedAt,
                  expectedModel: item.model,
                },
              });
              if (response.ticket) {
                ticket = response.ticket;
                tryWritePending(window.localStorage, { ...record, ticket });
              }
            }

            let polls = 0;
            while (ticket && !terminal(response) && polls < MAX_POLLS) {
              if (stopRef.current) break;
              await wait(polls === 0 ? 1_500 : POLL_INTERVAL_MS);
              if (stopRef.current) break;
              response = await poll({ data: { ticket } });
              polls += 1;
            }
            if (!terminal(response)) {
              current = markStatus(
                current,
                index,
                "stopped",
                "האיסוף עדיין פעיל בשירות — המשך התור יבדוק את אותה משימה",
              );
              setEntries(current);
              break;
            }

            if (response.status === "saved" || response.status === "duplicate") {
              clearPending(window.localStorage, identity);
              await refresh();
            } else if (response.status === "rate_limited") {
              tryWritePending(window.localStorage, {
                ...record,
                ticket,
                rateLimited: true,
                retryAt: validRetryAt(response.retryAt),
              });
            } else if (
              response.status === "not_connected" ||
              response.status === "conflict" ||
              response.status === "stale" ||
              (response.status === "failed" && !response.retryable)
            ) {
              clearPending(window.localStorage, identity);
            }
          } catch (error) {
            // כשל רשת/הרשאה — עצירה בטוחה, בלי ניסיון חוזר אוטומטי.
            current = markStatus(
              current,
              index,
              "stopped",
              error instanceof Error ? error.message : "התור נעצר",
            );
            setEntries(current);
            break;
          }

          current = applyOutcome(current, index, response);
          setEntries(current);
          if (shouldStopQueue(response)) break;
        }
      } finally {
        runningRef.current = false;
        setRunning(false);
      }
    },
    [userId, sessionId, freshBaseline, start, poll, refresh],
  );

  /** הפעלה חדשה או המשך: פריטים שכבר הצליחו אינם רצים שוב. */
  const startBatch = useCallback(
    (targets: BatchTarget[]) => {
      const base =
        entries.length > 0 &&
        entries.every(
          (entry, index) =>
            targets.some((target) => target.fieldId === entry.fieldId) && index < entries.length,
        ) &&
        entries.some((entry) => entry.status !== "pending")
          ? entries.map((entry) =>
              entry.status === "done" ? entry : { ...entry, status: "pending" as const },
            )
          : buildBatch(targets);
      void runQueue(base);
    },
    [entries, runQueue],
  );

  const stop = useCallback(() => {
    // עצירה אחרי הפריט הנוכחי; משימה שכבר רצה בשרת אינה מבוטלת.
    stopRef.current = true;
    setStopRequested(true);
  }, []);

  return { entries, running, stopRequested, startBatch, stop, progress: batchProgress(entries) };
}
