/**
 * המסלול הפעיל היחיד לאיסוף "Coinglass - Model 1": גשר שרת-לשרת אל שירות
 * האיסוף החיצוני (Python + OpenAI). אין כאן צילום, אין Firecrawl ואין Gemini.
 *
 * הקובץ הזה רק עוטף את הליבה (coinglassCollectCore) בפונקציות שרת מאומתות;
 * מודול ה-HTTP נטען דינמית בתוך ה-handler כדי שלא ידלוף לצד הלקוח.
 */
import { createServerFn } from "@tanstack/react-start";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import {
  importSavedObservation,
  pollCollection,
  reviewSavedObservation,
  startCollection,
  validateStartInput,
  type CollectDeps,
  type CollectResponse,
  type ReviewResponse,
  type StartInput,
} from "@/lib/coinglassCollectCore";
import { isCollectorTimeframe, type CollectorTimeframe } from "@/lib/coinglassCollector";

export type { CollectResponse, CollectStatus } from "@/lib/coinglassCollectCore";
export type { ReviewResponse, SavedObservation } from "@/lib/coinglassCollectCore";

async function makeDeps(supabase: CollectDeps["supabase"], userId: string): Promise<CollectDeps> {
  const {
    readCollectorConfig,
    startCollectorJob,
    readCollectorJob,
    readLatestCollectorJob,
    readCollectorCapabilities,
  } = await import("@/lib/coinglassCollector.server");
  return {
    supabase,
    userId,
    config: readCollectorConfig,
    startJob: startCollectorJob,
    readJob: readCollectorJob,
    readLatestJob: readLatestCollectorJob,
    capabilities: readCollectorCapabilities,
  };
}

export const startModel1Collection = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((input: StartInput) => validateStartInput(input))
  .handler(async ({ data, context }): Promise<CollectResponse> => {
    const deps = await makeDeps(
      context.supabase as unknown as CollectDeps["supabase"],
      context.userId,
    );
    return startCollection(deps, data);
  });

export const pollModel1Collection = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((input: { ticket: string }) => {
    if (!input || typeof input.ticket !== "string" || !input.ticket) throw new Error("כרטיס חסר");
    return { ticket: input.ticket };
  })
  .handler(async ({ data, context }): Promise<CollectResponse> => {
    const deps = await makeDeps(
      context.supabase as unknown as CollectDeps["supabase"],
      context.userId,
    );
    return pollCollection(deps, data);
  });

/** צפייה בלבד באיסוף שמור — GET אחד לשירות, בלי סריקה חדשה ובלי כתיבה. */
export const reviewSavedModel1Observation = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator(
    (input: { sessionId: string; fieldId: string; timeframe: CollectorTimeframe }) => {
      if (!input || typeof input.sessionId !== "string" || typeof input.fieldId !== "string")
        throw new Error("מזהה חסר");
      if (!isCollectorTimeframe(input.timeframe)) throw new Error("טווח זמן לא נתמך");
      return { sessionId: input.sessionId, fieldId: input.fieldId, timeframe: input.timeframe };
    },
  )
  .handler(async ({ data, context }): Promise<ReviewResponse> => {
    const deps = await makeDeps(
      context.supabase as unknown as CollectDeps["supabase"],
      context.userId,
    );
    return reviewSavedObservation(deps, data);
  });

/** שמירה מפורשת של אותו איסוף שמור, עם זמן האיסוף המקורי. */
export const saveSavedModel1Observation = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((input: { reviewTicket: string; confirm: boolean }) => {
    if (!input || typeof input.reviewTicket !== "string" || !input.reviewTicket)
      throw new Error("אישור צפייה חסר");
    if (input.confirm !== true) throw new Error("נדרש אישור מפורש");
    return { reviewTicket: input.reviewTicket, confirm: true };
  })
  .handler(async ({ data, context }): Promise<CollectResponse> => {
    const deps = await makeDeps(
      context.supabase as unknown as CollectDeps["supabase"],
      context.userId,
    );
    return importSavedObservation(deps, data);
  });
