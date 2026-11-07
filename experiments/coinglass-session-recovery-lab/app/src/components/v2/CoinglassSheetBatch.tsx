/**
 * פעולה אחת לכל גיליון האיסוף: "איסוף כל מפות CoinGlass".
 * מריצה את שלושת שדות מפת החיסולים × שלושת הטווחים, אחד אחרי השני, דרך אותו
 * מתאם תור של "כל הטווחים" — בלי סריקות מקבילות ובלי תזמון רקע.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { supabase } from "@/integrations/supabase/client";
import { batchWarning } from "@/lib/coinglassBatch";
import {
  COLLECTOR_TIMEFRAMES,
  HEATMAP_MODEL_LABEL,
  type HeatmapModel,
} from "@/lib/coinglassCollector";
import { useCoinglassBatch } from "@/components/v2/useCoinglassBatch";

type Props = {
  sessionId: string;
  targets: { fieldId: string; model: HeatmapModel }[];
  onDone: () => void;
  disabled?: boolean;
  onBusyChange?: (busy: boolean) => void;
};

export function CoinglassSheetBatch({
  sessionId,
  targets,
  onDone,
  disabled = false,
  onBusyChange,
}: Props) {
  const [userId, setUserId] = useState<string | null>(null);
  useEffect(() => {
    let active = true;
    void supabase.auth.getUser().then(({ data }) => {
      if (active) setUserId(data.user?.id ?? null);
    });
    const { data: sub } = supabase.auth.onAuthStateChange((_event, session) =>
      setUserId(session?.user?.id ?? null),
    );
    return () => {
      active = false;
      sub.subscription.unsubscribe();
    };
  }, []);

  const batch = useCoinglassBatch(sessionId, userId, onDone);
  const observer = useRef(onBusyChange);
  observer.current = onBusyChange;
  useEffect(() => {
    observer.current?.(batch.running);
  }, [batch.running]);
  const total = targets.length * COLLECTOR_TIMEFRAMES.length;

  const run = useCallback(() => {
    if (disabled || batch.running || targets.length === 0) return;
    if (!window.confirm(batchWarning(total))) return;
    batch.startBatch(
      targets.map((target) => ({ ...target, label: HEATMAP_MODEL_LABEL[target.model] })),
    );
  }, [batch, targets, total, disabled]);

  if (targets.length === 0) return null;
  const problem = batch.entries.find(
    (entry) => entry.status === "stopped" || entry.status === "failed",
  );

  return (
    <div className="flex flex-col gap-1 rounded-md border p-2" dir="rtl">
      <div className="flex flex-wrap items-center gap-2">
        <Button
          type="button"
          size="sm"
          variant="secondary"
          onClick={run}
          disabled={disabled || batch.running || !userId}
        >
          {batch.running
            ? `אוסף ${batch.progress.done}/${batch.progress.total}…`
            : "איסוף כל מפות CoinGlass"}
        </Button>
        {batch.running ? (
          <Button type="button" size="sm" variant="ghost" onClick={batch.stop}>
            {batch.stopRequested ? "עוצר אחרי הפריט הנוכחי…" : "עצור אחרי הפריט הנוכחי"}
          </Button>
        ) : null}
        <span className="text-xs text-muted-foreground">
          {`עד ${total} איסופים (3 מפות × 12H/24H/48H), אחד אחרי השני`}
        </span>
      </div>
      {batch.entries.length > 0 ? (
        <p className="text-xs text-muted-foreground break-words" role="status">
          {`התקדמות: ${batch.progress.done}/${batch.progress.total}`}
          {problem?.note ? ` · ${problem.note}` : ""}
        </p>
      ) : null}
    </div>
  );
}

export default CoinglassSheetBatch;
