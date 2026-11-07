import type { SupabaseClient } from "@supabase/supabase-js";
import { createHash } from "node:crypto";
import { bilingualCollectionNewsBody } from "./collectionNewsLanguage";
import {
  attachCollectionNewsRelevance,
  collectionBorderlineFilterReady,
  collectionNewsLearningSource,
  uniqueCollectionNewsSources,
  obviouslyNonMarketNews,
} from "./collectionNewsRelevance";
import { getCollectionNewsLearningContext } from "./collectionNewsFeedback.server";
import { observeCollectionPublicationScan } from "./collectionAutofillPublications.server";
import {
  autofillKind,
  autofillSettings,
  defaultAutofillInstructions,
  effectiveAutofillRating,
  escapeAutofillHtml,
  renderAutofillItem,
  sourceUrls,
  supportedSourceUrl,
  type AutofillResult,
} from "./collectionAutofill";
import { collectionSheetKey } from "./collectionSheets";
import { isYouTubeSourceUrl } from "./collectionYouTube";
import {
  hydrateNewsArticleMedia,
  scanSource,
  type SourceCursor,
  type SourceItem,
} from "./collectionSources.server";
import { captureCollectionSource, verifiedImage } from "./collectionCapture.server";
import {
  fetchCoinGlassExpirySnapshot,
  isCoinGlassExpirySource,
} from "./collectionExpirySource.server";
import {
  assessNewsMedia,
  judgeBtcLiquidity,
  marketDataValue,
  readMarketData,
  summarizeNews,
  type NewsDecision,
} from "./collectionAutofillModel.server";
import {
  eligibleNewsMedia,
  NEWS_MEDIA_MAX_BYTES,
  NEWS_MEDIA_REVIEW_TIMEOUT_MS,
  NEWS_MEDIA_SCAN_BUDGET_MS,
  type VerifiedNewsMedia,
} from "./collectionNewsMedia";
import {
  isBtcLiquiditySource,
  readBtcLiquiditySnapshot,
} from "./collectionBtcLiquiditySource.server";
import { renderBtcLiquiditySummary } from "./collectionBtcLiquidityPresentation";
import {
  composeManagedSnapshotHtml,
  type CollectionManagedSnapshot,
} from "./collectionManagedSnapshot";
import type { DominanceChartSymbol, VerifiedDominanceChart } from "./collectionDominanceChart";
import { unbiasWeeklySourceUrl } from "./collectionUnbiasWeekly";
import { openInterestSourceUrl, openInterestChartValue } from "./collectionOpenInterestChart";
import {
  isHyperliquidLiquidationSource,
  readHyperliquidSnapshot,
} from "./collectionHyperliquidSource.server";
import {
  formatHyperliquidLevelsText,
  hyperliquidCaptureFailureCode,
} from "./collectionHyperliquidChart";
import type { VerifiedEtfSnapshot } from "./collectionEtfTooltip";
import {
  isNewhedgeAltcoinSource,
  isTradingViewChart,
  readNewhedgeAltcoinSnapshot,
  tradingViewDominanceOverviewUrl,
  tradingViewDominanceChartUrl,
  tradingViewStablecoinCompositeChartUrl,
  type DominanceSnapshot,
} from "./collectionMarketSource";
import {
  collectorErrorNote,
  collectorZonesToLevels,
  resultFreshness,
  isUuid,
  type CollectorTimeframe,
  type HeatmapModel,
} from "./coinglassCollector";
import { detectCoinglassHeatmapModel } from "./coinglassModel1";
import {
  readCollectorCapabilities,
  readCollectorConfig,
  readCollectorJob,
  startCollectorJob,
} from "./coinglassCollector.server";
import type { V2CollectionSession, V2TemplateField, V2TemplateSection } from "./workspaceV1";
import { runAutofillFieldPool, validatedAutofillSerialGroups } from "./collectionAutofillQueue";

const LIQUIDITY_FIELD_TIMEOUT_MS = 18 * 60_000;
const COLLECTOR_QUEUE_TIMEOUT_MS = 11 * 60_000;
const COLLECTOR_RUNNING_TIMEOUT_MS = 330_000;
const COLLECTOR_POLL_MS = 4000;
// The published request closes at five minutes. Leave 45 seconds for the
// existing apply/fail transactions and response; analysis retains two minutes.
const SOCIAL_NEWS_FIELD_TIMEOUT_MS = 255_000;
const X_SUMMARY_RETRY_KEY = "__x_summary_retry";

type Claim = {
  run_id: string;
  status: "claimed" | "running";
  field: V2TemplateField;
  section: V2TemplateSection;
  session: V2CollectionSession;
  baseline: Record<string, unknown>;
  source_cursors: Record<string, SourceCursor>;
  last_scanned_at: string | null;
  created_at?: string;
  configuration_current?: boolean;
};

type LiquidityProgress = {
  run_id: string;
  deadline: number;
  model?: HeatmapModel;
  field_id?: string;
  session_id?: string;
  timeframes?: CollectorTimeframe[];
  configuration_key?: string;
  stopped?: string;
  jobs: Array<{
    timeframe: CollectorTimeframe;
    job_id: string;
    queue_deadline: number;
    running_deadline: number | null;
    ready: boolean;
    captured_at?: string;
    failure?: string;
  }>;
  reusable?: LiquidityProgress["jobs"];
};

type LiquidityValue = {
  value: Record<string, unknown>;
  progress: LiquidityProgress;
  failureMessage?: string;
};
const LIQUIDITY_PROGRESS_KEY = "__coinglass_pending";
function hyperliquidLevelsHtml(text: string): string {
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) =>
      line === "---"
        ? '<hr class="rt-color-purple">'
        : `<p class="rt-color-purple">\u2066${escapeAutofillHtml(line)}\u2069</p>`,
    )
    .join("");
}

async function rpc<T>(
  db: SupabaseClient,
  name: string,
  args: Record<string, unknown> = {},
  signal?: AbortSignal,
): Promise<T> {
  signal?.throwIfAborted();
  const query = db.rpc(name, args);
  const { data, error } = await (signal ? query.abortSignal(signal) : query);
  if (error) throw new Error(error.message);
  return data as T;
}

async function storeSourceImage(
  db: SupabaseClient,
  workspaceId: string,
  runId: string,
  image: string,
): Promise<string> {
  const verified = await verifiedImage(image);
  const digest = createHash("sha256").update(verified.bytes).digest("hex").slice(0, 24);
  const ext = verified.mime.split("/")[1];
  const path = `${workspaceId}/collection-autofill/${runId}/${digest}.${ext}`;
  const { error } = await db.storage
    .from("workspace-images")
    .upload(path, verified.bytes, { contentType: verified.mime, upsert: true });
  if (error) throw new Error("שמירת תמונת המקור נכשלה.");
  return `workspace-images:${path}`;
}

/** Optional media shares one bounded budget across the field. All network
 * images still pass the existing public-address and raster validation. */
async function withinNewsMediaBudget<T>(work: () => Promise<T>, deadline: number): Promise<T> {
  const remaining = deadline - Date.now();
  if (remaining <= 0) throw new Error("Optional news media budget expired.");
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work(),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error("Optional news media budget expired.")),
          remaining,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

export async function storeInformativeNewsImages(
  db: SupabaseClient,
  workspaceId: string,
  runId: string,
  source: SourceItem,
  summary: string,
  deadline: number,
): Promise<string[]> {
  if (Date.now() >= deadline) return [];
  try {
    source = await withinNewsMediaBudget(() => hydrateNewsArticleMedia(source), deadline);
  } catch {
    console.warn("collection_news_media_body_lookup_failed");
    return [];
  }
  const candidates: VerifiedNewsMedia[] = [];
  let bytes = 0;
  for (const media of eligibleNewsMedia(source)) {
    if (Date.now() >= deadline) break;
    try {
      const verified = await withinNewsMediaBudget(() => verifiedImage(media.url), deadline);
      if (Date.now() >= deadline || bytes + verified.bytes.length > NEWS_MEDIA_MAX_BYTES) continue;
      bytes += verified.bytes.length;
      candidates.push({
        ...media,
        id: `image_${candidates.length}`,
        dataUrl: verified.dataUrl,
      });
    } catch {
      console.warn("collection_news_media_download_failed");
    }
  }
  const remaining = deadline - Date.now();
  if (!candidates.length || remaining <= 0) return [];
  const accepted = await assessNewsMedia(
    source,
    summary,
    candidates,
    Math.min(remaining, NEWS_MEDIA_REVIEW_TIMEOUT_MS),
  );
  if (Date.now() >= deadline) return [];
  const paths: string[] = [];
  for (const media of accepted) {
    if (Date.now() >= deadline) break;
    try {
      // Upload the exact bytes assessed, not a second download from the URL.
      paths.push(
        await withinNewsMediaBudget(
          () => storeSourceImage(db, workspaceId, runId, media.dataUrl),
          deadline,
        ),
      );
    } catch {
      console.warn("collection_news_media_storage_failed");
    }
  }
  return paths;
}

async function liquidityValue(db: SupabaseClient, claim: Claim): Promise<LiquidityValue | null> {
  // Keep the original jobs in durable leased progress. A failed horizon must
  // not prevent the other validated horizons from reaching the final write.
  const stored = (claim.source_cursors as Record<string, unknown>)[LIQUIDITY_PROGRESS_KEY] as
    LiquidityProgress | undefined;
  const model = detectCoinglassHeatmapModel(claim.field);
  if (!model) throw new Error("לא זוהה קישור קוינגלאס למודל נתמך.");
  const runningBudget = model === 3 ? 390_000 : COLLECTOR_RUNNING_TIMEOUT_MS;
  const config = readCollectorConfig();
  const capabilities = await readCollectorCapabilities(config);
  if (capabilities.selectionPolicy !== "prominent_right_edge.v1")
    throw new Error("שירות הנזילות טרם פרסם את פענוח הרמות החדש; דבר לא נשמר.");
  if (!capabilities.models.includes(model))
    throw new Error(`שירות האיסוף אינו תומך במודל ${model}.`);
  const configured = claim.field.settings.autofill_timeframes;
  if (
    configured !== undefined &&
    (!Array.isArray(configured) ||
      configured.some((timeframe) => !["12H", "24H", "48H"].includes(timeframe)) ||
      new Set(configured).size !== configured.length)
  )
    throw new Error("הגדרת טווחי הנזילות אינה תקינה.");
  const timeframes: CollectorTimeframe[] = Array.isArray(configured)
    ? (configured as CollectorTimeframe[])
    : ["12H", "24H", "48H"];
  if (
    !timeframes.length ||
    timeframes.some((timeframe) => !capabilities.timeframes.includes(timeframe))
  )
    throw new Error("טווחי הנזילות המבוקשים אינם נתמכים.");
  const validJob = (job: LiquidityProgress["jobs"][number]) =>
    job &&
    timeframes.includes(job.timeframe) &&
    isUuid(job.job_id) &&
    Number.isFinite(job.queue_deadline) &&
    typeof job.ready === "boolean" &&
    (job.running_deadline === null || Number.isFinite(job.running_deadline)) &&
    (job.captured_at === undefined ||
      (typeof job.captured_at === "string" && Number.isFinite(Date.parse(job.captured_at)))) &&
    (job.failure === undefined ||
      (!job.ready &&
        typeof job.failure === "string" &&
        job.failure.length > 0 &&
        job.failure.length <= 500));
  const freshJob = (job: LiquidityProgress["jobs"][number]) => {
    if (!validJob(job) || !job.ready || !job.captured_at) return false;
    const freshness = resultFreshness(job.captured_at, Date.now());
    return !freshness.invalid && !freshness.stale;
  };
  const configurationKey = createHash("sha256")
    .update(JSON.stringify(claim.field.settings))
    .digest("hex");
  const sameScope =
    stored?.model === model &&
    stored.field_id === claim.field.id &&
    stored.session_id === claim.session.id &&
    stored.configuration_key === configurationKey &&
    JSON.stringify(stored.timeframes) === JSON.stringify(timeframes);
  const progress: LiquidityProgress =
    stored?.run_id === claim.run_id
      ? structuredClone(stored)
      : {
          run_id: claim.run_id,
          deadline:
            (claim.created_at ? Date.parse(claim.created_at) : Date.now()) +
            LIQUIDITY_FIELD_TIMEOUT_MS,
          jobs: [],
          // Only an unfinished/partial run retains this state. A matching
          // explicit retry can GET its still-fresh successes instead of POST.
          reusable:
            sameScope &&
            Array.isArray(stored.jobs) &&
            stored.jobs.length <= timeframes.length &&
            (stored.reusable === undefined ||
              (Array.isArray(stored.reusable) && stored.reusable.length <= timeframes.length))
              ? [...stored.jobs, ...(Array.isArray(stored.reusable) ? stored.reusable : [])]
                  .filter(freshJob)
                  .filter(
                    (job, index, jobs) =>
                      jobs.findIndex((other) => other.timeframe === job.timeframe) === index,
                  )
              : [],
        };
  const fieldDeadline = progress.deadline;
  if (
    !Number.isFinite(fieldDeadline) ||
    (progress.model !== undefined && progress.model !== model) ||
    (progress.field_id !== undefined && progress.field_id !== claim.field.id) ||
    (progress.session_id !== undefined && progress.session_id !== claim.session.id) ||
    (progress.timeframes !== undefined &&
      JSON.stringify(progress.timeframes) !== JSON.stringify(timeframes)) ||
    (progress.configuration_key !== undefined && progress.configuration_key !== configurationKey) ||
    (progress.stopped !== undefined &&
      (typeof progress.stopped !== "string" || progress.stopped.length > 1000)) ||
    !Array.isArray(progress.jobs) ||
    progress.jobs.length > timeframes.length ||
    progress.jobs.some((job, index) => !validJob(job) || job.timeframe !== timeframes[index]) ||
    new Set(progress.jobs.map((job) => job.job_id)).size !== progress.jobs.length ||
    (progress.reusable !== undefined &&
      (!Array.isArray(progress.reusable) ||
        progress.reusable.length > timeframes.length ||
        progress.reusable.some((job) => !validJob(job) || !job.ready)))
  )
    throw new Error("מצב עבודת הנזילות אינו תקין; לא תיפתח סריקה נוספת.");
  Object.assign(progress, {
    model,
    field_id: claim.field.id,
    session_id: claim.session.id,
    timeframes,
    configuration_key: configurationKey,
  });
  if (Date.now() >= fieldDeadline)
    progress.stopped = "איסוף הנזילות לא הושלם בזמן; לא נפתח טווח נוסף.";
  const checkpoint = async () => {
    if (JSON.stringify(stored) === JSON.stringify(progress)) return true;
    const { data, error } = await db
      .from("v2_collection_autofill_state")
      .update({
        source_cursors: {
          ...claim.source_cursors,
          [LIQUIDITY_PROGRESS_KEY]: progress,
        },
      })
      .eq("lease_id", claim.run_id)
      .eq("workspace_id", claim.session.workspace_id)
      .gt("lease_until", new Date().toISOString())
      .eq("source_cursors", JSON.stringify(claim.source_cursors))
      .select("lease_id");
    if (error) throw new Error("שמירת מצב האיסוף נכשלה; לא תיפתח סריקה נוספת.");
    // A concurrent reader may already have advanced the same run. Its state
    // wins; the next request reloads it instead of overwriting newer progress.
    return Array.isArray(data) && data.length === 1;
  };
  const note = (timeframe: CollectorTimeframe, message: string) =>
    `Model ${model}, ${timeframe}: ${message}`;
  if (!progress.stopped) {
    for (const [index, timeframe] of timeframes.entries()) {
      let pending = progress.jobs[index];
      if (pending?.ready || pending?.failure) continue;
      const reused = !pending && progress.reusable?.find((job) => job.timeframe === timeframe);
      if (reused && freshJob(reused)) {
        progress.jobs.push(structuredClone(reused));
        progress.reusable = progress.reusable?.filter((job) => job.timeframe !== timeframe);
        await checkpoint();
        return null;
      }
      const deadline = Math.min(
        fieldDeadline,
        pending?.running_deadline ?? pending?.queue_deadline ?? fieldDeadline,
      );
      if (Date.now() >= deadline) {
        progress.stopped = note(timeframe, "האיסוף לא הושלם בזמן; לא נפתח טווח נוסף.");
        break;
      }
      // Keep the same deterministic request ID when a start/checkpoint response
      // is lost. All envelopes still pass the existing model/job/timeframe checks.
      const requestId = createHash("sha256").update(`${claim.run_id}:${timeframe}`).digest("hex");
      const uuid = `${requestId.slice(0, 8)}-${requestId.slice(8, 12)}-4${requestId.slice(13, 16)}-a${requestId.slice(17, 20)}-${requestId.slice(20, 32)}`;
      try {
        const job = pending
          ? await readCollectorJob(pending.job_id, timeframe, config, model)
          : await startCollectorJob(uuid, timeframe, config, model);
        if (Date.now() > deadline) throw new Error("האיסוף לא הושלם בזמן.");
        if (!pending) {
          pending = {
            timeframe,
            job_id: job.job_id,
            queue_deadline: Date.now() + COLLECTOR_QUEUE_TIMEOUT_MS,
            running_deadline: null,
            ready: false,
          };
          progress.jobs.push(pending);
        }
        if (job.status === "running" && pending.running_deadline === null)
          pending.running_deadline = Date.now() + runningBudget;
        if (job.status === "queued" || job.status === "running") {
          await checkpoint();
          return null;
        }
        if (job.status === "failed") {
          pending.failure = collectorErrorNote(job.error?.code).slice(0, 500);
          // Quota, authorization and login gates stop new captures. Previously
          // validated horizons can still be read and saved independently.
          if (
            [
              "source_login_required",
              "analysis_unauthorized",
              "analysis_rate_limited",
              "analysis_quota_exceeded",
            ].includes(job.error?.code ?? "")
          ) {
            progress.stopped = note(timeframe, pending.failure);
            break;
          }
          await checkpoint();
          return null;
        }
        const result = job.result;
        if (job.status !== "ready" || !result) throw new Error("האיסוף לא החזיר תוצאה תקינה.");
        if (result.selection_policy !== "prominent_right_edge.v1")
          throw new Error("תוצאת הנזילות אינה כוללת את פענוח הרמות החדש.");
        const freshness = resultFreshness(result.captured_at, Date.now());
        if (freshness.invalid || freshness.stale) {
          pending.failure = "תצפית הנזילות אינה עדכנית.";
        } else {
          pending.ready = true;
          pending.captured_at = result.captured_at;
        }
        await checkpoint();
        return null;
      } catch (error) {
        // Transport, configuration and contract failures stop the queue; they
        // never turn an unverified result into a successful timeframe.
        progress.stopped = note(
          timeframe,
          error instanceof Error ? error.message.slice(0, 500) : "האיסוף נכשל.",
        );
        break;
      }
    }
  }
  const previous = claim.baseline ?? {};
  const structured = previous.structured as Record<string, unknown> | undefined;
  const previousAutofill = previous._autofill as Record<string, unknown> | undefined;
  const previousRows = previous.rows ?? structured?.rows;
  const hasOtherModel = (row: Record<string, unknown>) => {
    const explicitModels = [
      row.heatmap_model,
      (row.source_observation as Record<string, unknown> | undefined)?.heatmap_model,
      (row.hover_snapshot as Record<string, unknown> | undefined)?.heatmap_model,
      (row.quantity_snapshot as Record<string, unknown> | undefined)?.heatmap_model,
      ...(Array.isArray(row.levels)
        ? row.levels.map(
            (level: Record<string, unknown>) =>
              (level?.source_observation as Record<string, unknown> | undefined)?.heatmap_model,
          )
        : []),
    ].filter((value) => typeof value === "number" && [1, 2, 3].includes(value));
    const models = explicitModels.length ? explicitModels : [previousAutofill?.model];
    return models.some(
      (previousModel) =>
        typeof previousModel === "number" &&
        [1, 2, 3].includes(previousModel) &&
        previousModel !== model,
    );
  };
  const rows: Array<Record<string, unknown>> = [];
  const observations: Array<
    Record<string, unknown> & { timeframe: CollectorTimeframe; captured_at: string }
  > = [];
  // Finalize once. Re-read only completed jobs and validate each independently;
  // a later failure or stale result cannot discard another fresh observation.
  for (const pending of progress.jobs.filter((job) => job.ready)) {
    const timeframe = pending.timeframe;
    let result;
    try {
      const job = await readCollectorJob(pending.job_id, timeframe, config, model);
      result = job.result;
      if (job.status !== "ready" || !result) throw new Error("התוצאה שהושלמה אינה זמינה עוד.");
      if (result.selection_policy !== "prominent_right_edge.v1")
        throw new Error("תוצאת הנזילות אינה כוללת את פענוח הרמות החדש.");
      const freshness = resultFreshness(result.captured_at, Date.now());
      if (freshness.invalid || freshness.stale) throw new Error("תצפית הנזילות אינה עדכנית.");
    } catch (error) {
      pending.ready = false;
      pending.failure = error instanceof Error ? error.message.slice(0, 500) : "אימות התוצאה נכשל.";
      continue;
    }
    const alreadySaved =
      Array.isArray(previousAutofill?.observations) &&
      previousAutofill.observations.find(
        (observation) =>
          observation?.timeframe === timeframe &&
          (observation.heatmap_model ?? previousAutofill.model) === model &&
          (observation.collector_run_id ?? observation.evidence?.artifact_id) === result.run_id &&
          observation.evidence?.sha256 === result.evidence.sha256 &&
          observation.captured_at === result.captured_at,
      );
    const savedRow =
      Array.isArray(previousRows) && previousRows.find((row) => row?.timeframe === timeframe);
    if (alreadySaved && savedRow && !hasOtherModel(savedRow)) {
      // A retry of another horizon must not reset IDs, taken flags or user edits
      // in a result that this worksheet has already committed.
      rows.push(structuredClone(savedRow));
      observations.push(structuredClone(alreadySaved));
      continue;
    }
    rows.push({
      id: crypto.randomUUID(),
      timeframe,
      quantity_snapshot: result.quantities ?? null,
      hover_snapshot: result.hover_snapshot ?? null,
      levels: collectorZonesToLevels(result).map((level, index) => ({
        ...level,
        id: crypto.randomUUID(),
        position: index,
        taken: false,
      })),
    });
    observations.push({
      timeframe,
      heatmap_model: model,
      collector_run_id: result.run_id,
      captured_at: result.captured_at,
      observed_price: result.observed_price,
      source_url: result.source_url,
      observed_price_range: result.observed_price_range,
      evidence: result.evidence,
      quality: result.quality,
      selection_policy: result.selection_policy,
      level_contract_version: result.level_contract_version,
      quantities: result.quantities ?? null,
      hover_snapshot: result.hover_snapshot ?? null,
      ...(result.quantity_status === "unavailable"
        ? {
            quantity_status: result.quantity_status,
            quantity_reason: result.quantity_reason,
          }
        : {}),
    });
  }
  // Do not label an early result fresh if the final GETs consumed its remaining
  // freshness window. Preserve the old row for that timeframe instead.
  const saveAt = Date.now();
  for (let index = observations.length - 1; index >= 0; index--) {
    const observation = observations[index]!;
    const freshness = resultFreshness(observation.captured_at, saveAt);
    if (!freshness.invalid && !freshness.stale) continue;
    const pending = progress.jobs.find((job) => job.timeframe === observation.timeframe)!;
    pending.ready = false;
    pending.failure = "תצפית הנזילות אינה עדכנית.";
    observations.splice(index, 1);
    rows.splice(index, 1);
  }
  if (!(await checkpoint())) return null;
  const failures = progress.jobs
    .filter((job) => job.failure)
    .map((job) => note(job.timeframe, job.failure!));
  if (progress.stopped && !failures.includes(progress.stopped)) failures.push(progress.stopped);
  if (!rows.length) throw new Error((failures.join("; ") || "לא נשמר טווח חדש.").slice(0, 1000));
  const failureMessage = failures.length
    ? `סריקה חלקית: נשמרו הטווחים ${observations.map((observation) => observation.timeframe).join(", ")}; ${failures.join("; ")}`.slice(
        0,
        1000,
      )
    : undefined;
  const replacedTimeframes = new Set(observations.map((observation) => observation.timeframe));
  const retainedRows = (Array.isArray(previousRows) ? previousRows : []).filter(
    (row) => row && typeof row === "object" && !replacedTimeframes.has(row.timeframe),
  );
  const retainedTimeframes = new Set(retainedRows.map((row) => row.timeframe));
  const previousObservations = Array.isArray(previousAutofill?.observations)
    ? previousAutofill.observations.filter(
        (observation) =>
          observation &&
          typeof observation === "object" &&
          (replacedTimeframes.has(observation.timeframe) ||
            retainedTimeframes.has(observation.timeframe)),
      )
    : [];
  if (
    [
      ...retainedRows,
      ...previousObservations.filter(
        (observation) => !replacedTimeframes.has(observation.timeframe),
      ),
    ].some(hasOtherModel)
  )
    throw new Error("לא ניתן לשלב תוצאה חלקית עם טווחים שמורים ממודל אחר. התוכן הקיים לא שונה.");
  const merge = (
    existing: unknown,
    replacements: Array<Record<string, unknown>>,
    preserveRowIdentity = false,
  ) => {
    const remaining = new Map(replacements.map((row) => [row.timeframe, row]));
    const replaced = new Set<unknown>();
    const merged = (Array.isArray(existing) ? existing : []).flatMap((row) => {
      if (row && typeof row === "object" && replaced.has(row.timeframe)) return [];
      const replacement = row && typeof row === "object" && remaining.get(row.timeframe);
      if (!replacement) return [row];
      remaining.delete(row.timeframe);
      replaced.add(row.timeframe);
      // A new capture replaces all old source fields, including optional status
      // or provenance. An unchanged retry already supplies the full saved row.
      return [
        {
          ...replacement,
          ...(preserveRowIdentity && typeof row.id === "string" ? { id: row.id } : {}),
          ...(preserveRowIdentity && Number.isFinite(row.position)
            ? { position: row.position }
            : {}),
        },
      ];
    });
    return [...merged, ...remaining.values()];
  };
  const mergedRows = merge(previousRows, rows, true);
  return {
    value: {
      rows: mergedRows,
      ...(Array.isArray(structured?.rows)
        ? { structured: { ...structured, rows: mergedRows } }
        : {}),
      _autofill: {
        kind: "liquidity",
        model,
        observations: merge(previousObservations, observations),
      },
      _autofill_color: autofillSettings(claim.field).color ?? "rt-color-purple",
    },
    progress,
    ...(failureMessage ? { failureMessage } : {}),
  };
}

export async function fillCollectionField(
  db: SupabaseClient,
  fieldId: string,
  sessionId: string,
  manual = false,
  resumeRunId?: string,
): Promise<AutofillResult> {
  const startedAt = Date.now();
  let boundedNewsRun = false;
  let claim = await rpc<Claim | AutofillResult | null>(db, "v2_collection_autofill_claim", {
    p_field_id: fieldId,
    p_session_id: sessionId,
    p_manual: manual,
    ...(resumeRunId ? { p_run_id: resumeRunId } : {}),
  });
  if (!claim)
    return {
      run_id: "",
      status: "unchanged",
      message: "מועד הסריקה הבא טרם הגיע.",
    };
  if (claim.status !== "claimed" && claim.status !== "running")
    return { ...claim, status: claim.status };
  if (claim.status === "running" && !("field" in claim) && !resumeRunId) {
    // Leases are shared across sheets; only the original target can resume one.
    const { data: owner, error } = await db
      .from("v2_collection_autofill_runs")
      .select("field_id,session_id")
      .eq("id", claim.run_id)
      .single();
    if (error || !owner)
      return {
        run_id: claim.run_id,
        status: "conflict",
        message: "לא ניתן לאמת את יעד הסריקה הפעילה; לא נפתחה סריקה נוספת.",
      };
    if (owner.field_id !== fieldId || owner.session_id !== sessionId)
      return {
        run_id: claim.run_id,
        status: "conflict",
        message: "סריקה של התיבה כבר מתבצעת בגיליון אחר. המתינו לסיומה; לא נפתחה סריקה נוספת.",
      };
    // A lost start response can be recovered from the existing lease. Reading
    // this context cannot claim or start a replacement run.
    claim = await rpc<Claim | AutofillResult>(db, "v2_collection_autofill_claim", {
      p_field_id: fieldId,
      p_session_id: sessionId,
      p_manual: manual,
      p_run_id: claim.run_id,
    });
    if (claim.status !== "claimed" && claim.status !== "running")
      return { ...claim, status: claim.status };
  }
  if (
    !("field" in claim) ||
    (claim.status === "running" &&
      !(claim.source_cursors as Record<string, unknown>)[X_SUMMARY_RETRY_KEY] &&
      !detectCoinglassHeatmapModel(claim.field) &&
      !(claim.source_cursors as Record<string, unknown>)[LIQUIDITY_PROGRESS_KEY])
  )
    return {
      run_id: claim.run_id,
      status: "running",
      ...("continue" in claim && claim.continue === true ? { continue: true } : {}),
      message: "סריקה של התיבה כבר מתבצעת.",
    };
  const retry = (claim.source_cursors as Record<string, unknown>)[X_SUMMARY_RETRY_KEY] as
    { run_id?: string; ready?: boolean; scan_started_at?: string } | undefined;
  const resumingX = retry?.run_id === claim.run_id && retry.ready === false;
  let xCheckpointUncertain = false;
  const scanStartedAt =
    (resumingX ? retry.scan_started_at : undefined) ?? claim.created_at ?? new Date().toISOString();
  try {
    if (manual && (claim.session.id !== sessionId || claim.field.id !== fieldId))
      throw new Error("יעד הסריקה השתנה; הפעל שוב מתוך הגיליון הנבחר.");
    if (claim.configuration_current === false)
      return await rpc<AutofillResult>(db, "v2_collection_autofill_apply", {
        p_run_id: claim.run_id,
        p_payload: {},
        p_cursors: claim.source_cursors,
        p_scan_started_at: scanStartedAt,
      });
    const settings = autofillSettings(claim.field);
    if (!settings.enabled) throw new Error("לא הוגדר מקור ציבורי נתמך למילוי התיבה.");
    const sheet = collectionSheetKey(claim.section);
    const kind = autofillKind(claim.field, sheet);
    if (resumeRunId && (claim.run_id !== resumeRunId || (kind !== "liquidity" && !resumingX)))
      throw new Error("לא ניתן להמשיך עבודה אחרת.");
    if (retry?.run_id === claim.run_id && (!resumingX || kind !== "news"))
      throw new Error("מצב המשך הסיכום אינו תקין; לא נפתחה סריקה נוספת.");
    const instructions =
      typeof (claim.field.settings.autofill as Record<string, unknown>)?.instructions === "string"
        ? settings.instructions
        : defaultAutofillInstructions(claim.field, sheet);
    const urls = sourceUrls(claim.field).filter(supportedSourceUrl);
    boundedNewsRun =
      kind === "news" &&
      urls.some(
        (url) =>
          isYouTubeSourceUrl(url) || /(^|\.)(x\.com|twitter\.com)$/.test(new URL(url).hostname),
      );
    if (!urls.length || urls.length > 8)
      throw new Error("נדרשים בין קישור מקור אחד לשמונה קישורי מקור נתמכים.");
    const cursors = { ...claim.source_cursors };
    if (!resumingX) delete (cursors as Record<string, unknown>)[X_SUMMARY_RETRY_KEY];
    const sourceFailures: string[] = [];
    const pendingSources: string[] = [];
    const payload: Record<string, unknown> = {
      write_mode:
        settings.write_mode === "default"
          ? sheet === "news_macro"
            ? "append"
            : "replace"
          : settings.write_mode,
    };
    if (kind === "economic_calendar") {
      const { isEconomicCalendarSource } = await import("./collectionEconomicCalendar");
      const { readEconomicCalendarSnapshot } =
        await import("./collectionEconomicCalendarSource.server");
      const source = urls.find(isEconomicCalendarSource);
      if (!source || !urls.every(isEconomicCalendarSource))
        throw new Error("נדרש קישור ליומן הכלכלי של Investing.");
      const snapshot = await readEconomicCalendarSnapshot(source, scanStartedAt);
      cursors[snapshot.sourceUrl] = { scanned_at: scanStartedAt };
      return await rpc<AutofillResult>(db, "v2_collection_autofill_apply_calendar", {
        p_run_id: claim.run_id,
        p_snapshot: snapshot,
        p_cursors: cursors,
        p_scan_started_at: scanStartedAt,
      });
    }
    if (kind === "capital_market") {
      const { fetchCapitalMarketSnapshot } = await import("./collectionCapitalMarket.server");
      const snapshot = await fetchCapitalMarketSnapshot(claim.baseline, {
        captureText: async (url) => {
          const captured = await captureCollectionSource(url, false, { textOnly: true });
          return { text: captured.text, capturedAt: captured.capturedAt };
        },
      });
      const verified = snapshot.rows.some(
        (row) => (row.quote && !row.quote_error) || (row.correlation && !row.correlation_error),
      );
      if (!verified)
        throw new Error("לא ניתן היה לאמת את נתוני שוק ההון. הנתונים וההערות הקיימים נשמרו.");
      for (const row of snapshot.rows) {
        if (row.quote && !row.quote_error)
          cursors[row.quote.source_url] = { scanned_at: row.quote.fetched_at };
        if (row.correlation && !row.correlation_error)
          cursors[row.correlation.source_url] = { scanned_at: row.correlation.fetched_at };
      }
      return await rpc<AutofillResult>(db, "v2_collection_capital_market_apply", {
        p_run_id: claim.run_id,
        p_snapshot: snapshot,
        p_cursors: cursors,
        p_scan_started_at: scanStartedAt,
      });
    }
    if (kind === "liquidity") {
      const value = await liquidityValue(db, claim);
      if (!value)
        return {
          run_id: claim.run_id,
          session_id: claim.session.id,
          status: "running",
          continue: true,
        };
      // The value already merges by timeframe. Appending that merged array
      // would duplicate old rows and bypass the existing baseline conflict guard.
      payload.write_mode = "replace";
      payload.value = value.value;
      if (value.failureMessage) {
        payload.source_failure_message = value.failureMessage;
        (cursors as Record<string, unknown>)[LIQUIDITY_PROGRESS_KEY] = value.progress;
      } else delete cursors[LIQUIDITY_PROGRESS_KEY];
    } else if (kind === "snapshot" && urls.length === 1 && isBtcLiquiditySource(urls[0])) {
      const snapshot = await readBtcLiquiditySnapshot(urls[0]);
      const judgment = await judgeBtcLiquidity(snapshot, instructions);
      const primary = snapshot.candidates.find(
        (candidate) => candidate.id === judgment.primary_id,
      )!;
      const alternate = snapshot.candidates.find(
        (candidate) => candidate.id === judgment.alternate_id,
      );
      const invalidation = snapshot.candidates.find(
        (candidate) => candidate.id === judgment.invalidation_id,
      )!;
      const summary = renderBtcLiquiditySummary(snapshot, judgment);
      const generatedHtml = summary
        .split("\n")
        .map((line) => {
          const split = line.indexOf(" — ");
          const target = split < 0 ? line : line.slice(0, split);
          // Isolate the numeric range/timeframe so RTL text retains high–low order.
          const readableTarget = target.replace(
            /(\d[\d,]*(?:–\d[\d,]*)?(?: \((?:15m|1H|4H|1D|1W|1M)\))?)$/,
            "\u2066$1\u2069",
          );
          return `<p class="rt-color-purple"><strong>${escapeAutofillHtml(readableTarget)}</strong>${split < 0 ? "" : escapeAutofillHtml(line.slice(split))}</p>`;
        })
        .join("");
      const managed: CollectionManagedSnapshot = {
        version: 1,
        kind: "btc_liquidity_destination",
        generated_html: generatedHtml,
        manual_html: "",
      };
      payload.value = {
        text: composeManagedSnapshotHtml(managed),
        _autofill_snapshot: managed,
        _autofill: {
          kind: "btc_liquidity_destination",
          captured_at: snapshot.capturedAt,
          source_url: snapshot.sourceUrl,
          current_price: snapshot.currentPrice,
          quote_asset: snapshot.quoteAsset,
          market_exchange: snapshot.marketExchange,
          symbol: snapshot.symbol,
          source_endpoints: snapshot.sourceEndpoints,
          primary,
          alternate: alternate ?? null,
          invalidation,
          judgment,
          frames: snapshot.frames.map(({ recentClosedCandles: _candles, ...frame }) => frame),
          unavailable_frames: snapshot.unavailableFrames ?? [],
          timeframe_targets:
            judgment.timeframe_judgments?.map((frame) => ({
              ...frame,
              target:
                snapshot.candidates.find((candidate) => candidate.id === frame.target_id) ?? null,
            })) ?? [],
          source_sha256: createHash("sha256").update(snapshot.pageText).digest("hex"),
          methodology: "verified_closed_OHLC_with_conditional_directional_inference",
        },
      };
      cursors[urls[0]] = { scanned_at: scanStartedAt };
    } else if (
      kind === "snapshot" &&
      urls.length === 1 &&
      isHyperliquidLiquidationSource(urls[0])
    ) {
      let capture: Awaited<ReturnType<typeof captureCollectionSource>> | undefined;
      let captureFailure: string | undefined;
      let captureFailureCode: string | undefined;
      try {
        capture = await captureCollectionSource(urls[0], false, {
          hyperliquid: true,
        });
        if (!capture.image || !capture.hyperliquidChart)
          throw new Error("לא אומת צילום של גרף היפרליקוויד.");
      } catch (error) {
        captureFailure = error instanceof Error ? error.message : "צילום הגרף אינו זמין.";
        captureFailureCode = hyperliquidCaptureFailureCode(error);
        capture = undefined;
      }
      if (capture?.image && capture.hyperliquidChart) {
        const imagePath = await storeSourceImage(
          db,
          claim.session.workspace_id,
          claim.run_id,
          capture.image,
        );
        const managed: CollectionManagedSnapshot = {
          version: 1,
          kind: "hyperliquid_liquidation_spikes",
          generated_html:
            `<p class="rt-color-purple"><img data-path="${escapeAutofillHtml(imagePath)}" alt="מפת הנזלות היפרליקוויד"></p>` +
            hyperliquidLevelsHtml(
              formatHyperliquidLevelsText(
                capture.hyperliquidChart.spikes,
                capture.hyperliquidChart.observedPrice,
              ),
            ),
          manual_html: "",
          image_path: imagePath,
          chart: capture.hyperliquidChart,
        };
        payload.value = {
          text: composeManagedSnapshotHtml(managed),
          _autofill_snapshot: managed,
          _autofill: {
            kind: "hyperliquid_liquidation_spikes",
            capturedAt: capture.capturedAt,
            sourceUrl: urls[0],
            presentation: "verified_source_chart",
            image_sha256: createHash("sha256").update(capture.image).digest("hex"),
          },
        };
      } else {
        const snapshot = await readHyperliquidSnapshot(urls[0]);
        const summary = formatHyperliquidLevelsText(snapshot.spikes, snapshot.observedPrice);
        const managed: CollectionManagedSnapshot = {
          version: 1,
          kind: "hyperliquid_liquidation_spikes",
          generated_html:
            `${hyperliquidLevelsHtml(summary)}<p class="rt-color-purple"><em>רמות מפוזיציות לווייתנים מעל מיליון דולר; צילום מפת קוינגלאס לא זמין.</em></p>` +
            (captureFailureCode === "source_chart_unmounted"
              ? '<p class="rt-color-purple">גרף קוינגלאס לא הוצג בדף הסריקה.</p>'
              : ""),
          manual_html: "",
        };
        payload.value = {
          text: composeManagedSnapshotHtml(managed),
          _autofill_snapshot: managed,
          _autofill: {
            kind: "hyperliquid_liquidation_spikes",
            ...snapshot,
            presentation: "verified_positions_fallback",
            capture_failure: captureFailure,
            capture_failure_stage: captureFailureCode ? "hyperliquid_chart" : "capture",
            capture_failure_code: captureFailureCode,
          },
        };
      }
      cursors[urls[0]] = { scanned_at: scanStartedAt };
    } else if (kind === "expiry") {
      if (urls.length !== 1 || !isCoinGlassExpirySource(urls[0]))
        throw new Error("מילוי פקיעות דורש את מקור Max Pain של קוינגלאס; התיבה לא שונתה.");
      const snapshot = await fetchCoinGlassExpirySnapshot(urls[0]);
      const data = await readMarketData(
        kind,
        instructions,
        [{ url: urls[0], text: JSON.stringify(snapshot.rows) }],
        [],
        undefined,
        snapshot,
      );
      const value = marketDataValue(kind, data, {
        color: settings.color,
        capturedAt: snapshot.capturedAt,
        sourceUrl: snapshot.sourceUrl,
        verifiedExpiry: snapshot,
      });
      if (value) payload.value = value;
      cursors[urls[0]] = { scanned_at: scanStartedAt };
    } else if (kind === "weekly_consensus") {
      const sourceUrl = urls.length === 1 ? unbiasWeeklySourceUrl(urls[0]) : null;
      if (!sourceUrl)
        throw new Error("צילום Weekly Consensus דורש קישור יחיד למקור ביטקוין; התיבה לא שונתה.");
      const capture = await captureCollectionSource(sourceUrl, false, {
        unbiasWeekly: true,
      });
      if (!capture.image || !capture.unbiasWeekly)
        throw new Error("לא אומת צילום Weekly Consensus מלא; התיבה לא שונתה.");
      const imagePath = await storeSourceImage(
        db,
        claim.session.workspace_id,
        claim.run_id,
        capture.image,
      );
      // This isolated metadata snapshot does not replace any manual/native news.
      payload.write_mode = "append";
      payload.value = {
        _autofill_consensus: { ...capture.unbiasWeekly, image_path: imagePath },
        _autofill: {
          kind: "weekly_consensus",
          capturedAt: capture.capturedAt,
          sourceUrl,
          presentation: "verified_source_screenshot",
          image_sha256: createHash("sha256").update(capture.image).digest("hex"),
        },
      };
      cursors[urls[0]] = { scanned_at: scanStartedAt };
    } else if (kind === "news") {
      const bounded = boundedNewsRun;
      const newsDeadline = bounded
        ? startedAt + SOCIAL_NEWS_FIELD_TIMEOUT_MS
        : Date.now() + 10 * 60_000;
      const workSignal = bounded
        ? AbortSignal.timeout(Math.max(1, newsDeadline - Date.now()))
        : undefined;
      // Durable human feedback is read once on each scan, before any source cursor advances.
      const learning = await getCollectionNewsLearningContext(
        db,
        claim.session.workspace_id,
        claim.field.id,
        undefined,
        workSignal,
      );
      const scanned: SourceItem[] = [];
      const itemSources = new Map<string, string>();
      const downloadedX = new Map<string, NonNullable<SourceCursor["x_pending"]>>();
      let savedCursors = claim.source_cursors;
      const checkpointX = async (
        updates: Record<string, NonNullable<SourceCursor["x_pending"]>>,
      ) => {
        const checkpoint = {
          ...savedCursors,
          ...Object.fromEntries(
            Object.entries(updates).map(([url, pending]) => [
              url,
              { ...claim.source_cursors[url], x_pending: pending },
            ]),
          ),
        };
        // News runs have one lease owner. Keep post content in the update body,
        // not a growing URL filter, and save each page before requesting another.
        xCheckpointUncertain = true;
        const { data, error } = await db
          .from("v2_collection_autofill_state")
          .update({ source_cursors: checkpoint })
          .eq("lease_id", claim.run_id)
          .eq("workspace_id", claim.session.workspace_id)
          .gt("lease_until", new Date().toISOString())
          .select("lease_id")
          .abortSignal(workSignal ?? AbortSignal.timeout(15_000));
        if (error || !Array.isArray(data) || data.length !== 1) {
          // The database may have committed even when its acknowledgement was
          // lost. Read the durable state before another source can overwrite it.
          xCheckpointUncertain = true;
          const { data: stored, error: readError } = await db
            .from("v2_collection_autofill_state")
            .select("source_cursors")
            .eq("lease_id", claim.run_id)
            .eq("workspace_id", claim.session.workspace_id)
            .gt("lease_until", new Date().toISOString())
            .abortSignal(AbortSignal.timeout(15_000));
          if (
            !readError &&
            Array.isArray(stored) &&
            stored.length === 1 &&
            stored[0].source_cursors &&
            typeof stored[0].source_cursors === "object" &&
            !Array.isArray(stored[0].source_cursors)
          ) {
            savedCursors = stored[0].source_cursors;
            for (const url of Object.keys(updates))
              cursors[url] = savedCursors[url] ?? claim.source_cursors[url];
            claim.source_cursors = savedCursors;
            xCheckpointUncertain = false;
          }
          throw new Error(
            "שמירת ההתקדמות באיסוף X לא אושרה; התוכן השמור נשמר להמשך ללא רכישה חוזרת.",
          );
        }
        xCheckpointUncertain = false;
        savedCursors = checkpoint;
        claim.source_cursors = checkpoint;
        // A different successful source may still apply after this one fails.
        // Its final write must retain the pages already saved for this source.
        for (const url of Object.keys(updates)) cursors[url] = checkpoint[url];
      };
      const collectionDeadline = newsDeadline - 120_000;
      let timedOutXFailures = 0;
      const videoDecisions: NewsDecision[] = [];
      const analyzedVideos = new Set<string>();
      let videoWriteMode: "default" | "append" | "replace" = "default";
      // Independent news sources keep independent cursors. Failed sources
      // stay at their prior cursor while verified sources can still be saved.
      let successfulSources = 0;
      for (const url of urls) {
        const xAccount = (raw: string) =>
          /^https:\/\/(?:www\.|mobile\.)?(?:x\.com|twitter\.com)\/([a-z0-9_]{1,15})(?:[/?#]|$)/i
            .exec(raw)?.[1]
            .toLowerCase();
        const account = xAccount(url);
        if (
          account &&
          Object.keys(cursors).some(
            (savedUrl) => savedUrl !== url && xAccount(savedUrl) === account,
          )
        ) {
          sourceFailures.push(
            `@${account}: קיימת התקדמות תחת קישור אחר לאותו חשבון; לא תיפתח סריקה חדשה שתעקוף את המכסה.`,
          );
          continue;
        }
        // Resume only already downloaded X content; do not re-read completed
        // sources (including YouTube) or request another page from X.
        if (resumingX && (!cursors[url]?.x_pending || cursors[url].x_pending.pagination)) continue;
        if (bounded && Date.now() >= collectionDeadline) {
          (isYouTubeSourceUrl(url) ? pendingSources : sourceFailures).push(
            `${new URL(url).hostname}: לא נותר זמן לאיסוף ולניתוח מקור נוסף.`,
          );
          continue;
        }
        try {
          const scan = await scanSource(url, cursors[url] ?? {}, scanStartedAt, true, {
            workspaceId: claim.session.workspace_id,
            ...(bounded ? { deadline: collectionDeadline } : {}),
            ...(/(^|\.)(x\.com|twitter\.com)$/.test(new URL(url).hostname)
              ? {
                  onXCheckpoint: (pending: NonNullable<SourceCursor["x_pending"]>) =>
                    checkpointX({ [url]: pending }),
                }
              : {}),
            ...(isYouTubeSourceUrl(url)
              ? {
                  knownItems: async (keys: string[]) => [
                    ...keys.filter((key) => analyzedVideos.has(key)),
                    ...(await rpc<string[]>(
                      db,
                      "v2_collection_autofill_known_items",
                      {
                        p_run_id: claim.run_id,
                        p_keys: keys,
                      },
                      workSignal,
                    )),
                  ],
                  onItem: async (item: SourceItem) => {
                    if (obviouslyNonMarketNews(item.text)) return null;
                    if (analyzedVideos.has(item.key)) return null;
                    let failure: string | null = null;
                    const summary = await summarizeNews(
                      [item],
                      instructions,
                      claim.field.field_type === "unbiased_news",
                      learning,
                      {
                        deadline: newsDeadline,
                        onFailure: (_batch, message) => {
                          failure = message;
                        },
                      },
                    );
                    if (failure) return failure;
                    if (summary.writeMode !== "default") {
                      if (videoWriteMode !== "default" && videoWriteMode !== summary.writeMode)
                        return "הנחיות העריכה אינן עקביות בין אייטמי המקור.";
                      videoWriteMode = summary.writeMode;
                    }
                    videoDecisions.push(...summary.decisions);
                    analyzedVideos.add(item.key);
                    // Retain finished work even if a later video/source fails.
                    scanned.push(item);
                    itemSources.set(item.key, url);
                    return null;
                  },
                }
              : {}),
          });
          if (/(^|\.)(x\.com|twitter\.com)$/.test(new URL(url).hostname)) {
            const saved = savedCursors[url]?.x_pending;
            const pending: NonNullable<SourceCursor["x_pending"]> = saved ?? {
              items: scan.items,
              cursor: scan.cursor,
              ...(scan.publicationHistory ? { publicationHistory: scan.publicationHistory } : {}),
            };
            if (pending.pagination && !scan.pending?.length)
              throw new Error("איסוף X טרם הושלם; ההתקדמות נשמרה להמשך הסריקה.");
            if (!saved) await checkpointX({ [url]: pending });
            downloadedX.set(url, pending);
          }
          // Learn from every verified publication, including items later
          // rejected as irrelevant. Provider failures never count as silence.
          if (!resumingX)
            try {
              await observeCollectionPublicationScan(db, {
                fieldId: claim.field.id,
                runId: claim.run_id,
                sourceUrl: url,
                scan,
                scannedAt: downloadedX.has(url)
                  ? (downloadedX.get(url)!.cursor.scanned_at ?? scanStartedAt)
                  : scanStartedAt,
                signal: workSignal,
              });
            } catch {
              console.warn("collection_publication_timing_not_recorded", {
                fieldId: claim.field.id,
              });
            }
          scanned.push(...scan.items);
          for (const item of scan.items) itemSources.set(item.key, url);
          cursors[url] = scan.cursor;
          for (const message of scan.failures ?? [])
            sourceFailures.push(`${new URL(url).hostname}: ${message}`);
          for (const message of scan.pending ?? [])
            pendingSources.push(`${new URL(url).hostname}: ${message}`);
          successfulSources++;
        } catch (error) {
          if (xCheckpointUncertain) throw error;
          sourceFailures.push(
            `${new URL(url).hostname}: ${error instanceof Error ? error.message : "קריאת המקור נכשלה"}`,
          );
        }
      }
      if (!successfulSources && !analyzedVideos.size) throw new Error(sourceFailures.join("; "));
      const unique = uniqueCollectionNewsSources(scanned).filter(
        (item) => !obviouslyNonMarketNews(item.text, item),
      );
      const unchecked = unique.filter((item) => !isYouTubeSourceUrl(item.url));
      const known = unchecked.length
        ? new Set(
            await rpc<string[]>(
              db,
              "v2_collection_autofill_known_items",
              {
                p_run_id: claim.run_id,
                p_keys: unchecked.map((item) => item.key),
              },
              workSignal,
            ),
          )
        : new Set<string>();
      // Revisited boundary timestamps and overlapping sources need neither
      // another paid summary nor another image download for an existing item.
      const items = unique.filter((item) => !known.has(item.key));
      // Canonical keys survive PostgreSQL jsonb key ordering. v1 binds this
      // analysis contract; incompatible prompt changes must change the version.
      const xAnalysisHash = (url: string, item: SourceItem) =>
        createHash("sha256")
          .update(
            JSON.stringify(
              [
                1,
                claim.session.workspace_id,
                claim.field.id,
                url,
                savedCursors[url]?.x_pending?.cursor.x_user_id,
                item,
                instructions,
                claim.field.field_type === "unbiased_news",
                learning,
                process.env.COLLECTION_AUTOFILL_MODEL || "gpt-5-mini",
              ],
              (_key, part) =>
                part && typeof part === "object" && !Array.isArray(part)
                  ? Object.fromEntries(
                      Object.keys(part)
                        .sort()
                        .map((key) => [key, part[key]]),
                    )
                  : part,
            ),
          )
          .digest("hex");
      const savedAnalyses = new Map<
        string,
        {
          decision: NewsDecision;
          writeMode: "default" | "append" | "replace";
        }
      >();
      for (const item of items) {
        const url = itemSources.get(item.key)!;
        const pending = downloadedX.get(url);
        if (!pending || pending.analysis === undefined) continue;
        if (
          !pending.analysis ||
          typeof pending.analysis !== "object" ||
          Array.isArray(pending.analysis)
        )
          throw new Error("מצב סיכומי X השמור אינו תקין; לא יופעל ניתוח חוזר.");
        const saved = pending.analysis[item.key];
        if (saved === undefined) continue;
        if (
          !saved ||
          saved.v !== 1 ||
          saved.input_hash !== xAnalysisHash(url, item) ||
          saved.decision?.key !== item.key
        )
          throw new Error("סיכום X שמור אינו תואם למקור או להוראות; נשמר לבירור ללא ניתוח חוזר.");
        savedAnalyses.set(item.key, saved);
      }
      const summary = await summarizeNews(
        items.filter((item) => !analyzedVideos.has(item.key)),
        instructions,
        claim.field.field_type === "unbiased_news",
        learning,
        bounded || downloadedX.size
          ? {
              deadline: newsDeadline,
              saved: savedAnalyses,
              onBatch: async (batch, response) => {
                const updates: Record<string, NonNullable<SourceCursor["x_pending"]>> = {};
                for (const item of batch) {
                  const url = itemSources.get(item.key)!;
                  if (!downloadedX.has(url)) continue;
                  const pending = updates[url] ?? savedCursors[url]?.x_pending;
                  if (!pending || !pending.items.some((entry) => entry.key === item.key))
                    throw new Error("קלט X לא נמצא בשמירה המאושרת; לא ניתן לשמור סיכום.");
                  updates[url] = {
                    ...pending,
                    analysis: {
                      ...pending.analysis,
                      [item.key]: {
                        v: 1,
                        input_hash: xAnalysisHash(url, item),
                        decision: response.decisions.find((decision) => decision.key === item.key)!,
                        writeMode: response.write_mode,
                      },
                    },
                  };
                }
                if (!Object.keys(updates).length) return;
                await checkpointX(updates);
                for (const [url, pending] of Object.entries(updates)) downloadedX.set(url, pending);
              },
              onFailure: (batch, message, timedOut) => {
                if (
                  !urls.some(isYouTubeSourceUrl) &&
                  batch.some((item) => !downloadedX.has(itemSources.get(item.key)!))
                )
                  throw new Error(message);
                const failedSources = new Set<string>();
                for (const item of batch) {
                  const url = itemSources.get(item.key)!;
                  failedSources.add(url);
                  if (isYouTubeSourceUrl(url)) {
                    const cursor = cursors[url];
                    if (
                      !cursor.scanned_at ||
                      Date.parse(item.publishedAt) < Date.parse(cursor.scanned_at)
                    )
                      cursors[url] = {
                        ...cursor,
                        scanned_at: item.publishedAt,
                      };
                  } else if (!downloadedX.has(url)) {
                    cursors[url] = claim.source_cursors[url] ?? {};
                  }
                }
                for (const url of failedSources)
                  sourceFailures.push(`${new URL(url).hostname}: ${message}`);
                if (timedOut && [...failedSources].every((url) => downloadedX.has(url)))
                  timedOutXFailures += failedSources.size;
              },
            }
          : undefined,
      );
      if (videoWriteMode !== "default") {
        if (summary.writeMode !== "default" && summary.writeMode !== videoWriteMode)
          throw new Error("הנחיות העריכה אינן עקביות בין אייטמי המקור.");
        summary.writeMode = videoWriteMode;
      }
      const decisions = [...videoDecisions, ...summary.decisions];
      const completed = new Set([...known, ...decisions.map((item) => item.key)]);
      for (const [url, pending] of downloadedX) {
        const remaining = pending.items.filter(
          (item) => !completed.has(item.key) && !obviouslyNonMarketNews(item.text, item),
        );
        // Results and the reduced pending set are committed together by apply.
        // Until all downloaded posts are handled, only the saved content resumes.
        cursors[url] =
          remaining.length || pending.pagination
            ? {
                ...claim.source_cursors[url],
                x_pending: {
                  ...pending,
                  items: remaining,
                  ...(pending.analysis
                    ? {
                        analysis: Object.fromEntries(
                          Object.entries(pending.analysis).filter(([key]) =>
                            remaining.some((item) => item.key === key),
                          ),
                        ),
                      }
                    : {}),
                },
              }
            : pending.cursor;
      }
      const unfinishedX = [...downloadedX.keys()].some(
        (url) => cursors[url]?.x_pending?.items.length && !cursors[url]?.x_pending?.pagination,
      );
      // One continuation, only for timeouts after actual progress. The existing
      // apply transaction saves successes and the remainder before exposing it.
      if (
        sheet === "news_macro" &&
        !resumingX &&
        unfinishedX &&
        decisions.length > 0 &&
        timedOutXFailures > 0 &&
        sourceFailures.length === timedOutXFailures &&
        pendingSources.length === 0
      ) {
        (cursors as Record<string, unknown>)[X_SUMMARY_RETRY_KEY] = {
          run_id: claim.run_id,
          ready: true,
          scan_started_at: scanStartedAt,
        };
        payload.x_summary_continue = true;
      }
      if (settings.write_mode === "default" && summary.writeMode !== "default")
        payload.write_mode = summary.writeMode;
      if (resumingX || payload.x_summary_continue) payload.write_mode = "append";
      const rating = effectiveAutofillRating(settings, claim.section.settings);
      const accepted = [];
      const mediaDeadline = Math.min(
        Date.now() + NEWS_MEDIA_SCAN_BUDGET_MS,
        bounded ? newsDeadline : Infinity,
      );
      for (const decision of decisions.filter((decision) => decision.relevant)) {
        const source = items.find((item) => item.key === decision.key);
        if (
          !source ||
          obviouslyNonMarketNews(source.text, source) ||
          obviouslyNonMarketNews(decision.summary)
        )
          continue;
        const learningSource = collectionNewsLearningSource(source.url);
        const history = learning.bySource.find((item) => item.sourceUrl === learningSource);
        // Explore until distinct reviews for this source justify hiding marginal articles.
        if (decision.relevance.level === "borderline" && collectionBorderlineFilterReady(history))
          continue;
        const imagePaths = await storeInformativeNewsImages(
          db,
          claim.session.workspace_id,
          claim.run_id,
          source,
          decision.summary,
          mediaDeadline,
        );
        const category = decision.category;
        const renderItem = (variant: {
          summary: string;
          commentary?: string | null;
          inline_notes?: Array<{ after: string; text: string }>;
        }) =>
          renderAutofillItem(
            {
              id: createHash("sha256").update(source.key).digest("hex"),
              summary: variant.summary,
              commentary: variant.commentary,
              inlineNotes: variant.inline_notes,
              url: source.url,
              sourceName: source.sourceName,
              publishedAt: source.publishedAt,
              imagePaths,
            },
            settings.color ?? "rt-color-purple",
          );
        const hebrewHtml = renderItem(decision);
        accepted.push({
          key: source.key,
          url: source.url,
          source_name: source.sourceName,
          published_at: source.publishedAt,
          category,
          importance: !rating
            ? "neutral"
            : claim.field.field_type === "unbiased_news"
              ? category
              : decision.importance,
          rating_pending: !rating,
          bot_relevance: decision.relevance,
          learning_source_url: learningSource,
          html: attachCollectionNewsRelevance(
            decision.english
              ? bilingualCollectionNewsBody(hebrewHtml, renderItem(decision.english))
              : hebrewHtml,
            decision.relevance,
          ),
        });
      }
      payload.items = accepted;
    } else if (kind === "snapshot" && urls.length === 1 && openInterestSourceUrl(urls[0])) {
      const capture = await captureCollectionSource(openInterestSourceUrl(urls[0])!, false, {
        openInterest: true,
      });
      if (!capture.image || !capture.openInterestChart)
        throw new Error("לא אומתו גרף ונתוני Open Interest עדכניים; התיבה לא שונתה.");
      const imagePath = await storeSourceImage(
        db,
        claim.session.workspace_id,
        claim.run_id,
        capture.image,
      );
      payload.value = openInterestChartValue(capture.openInterestChart, imagePath, settings.color);
      cursors[urls[0]] = { scanned_at: scanStartedAt };
    } else if (
      kind === "dominance" &&
      urls.length > 0 &&
      urls.every(
        (url) => tradingViewDominanceChartUrl(url) || tradingViewStablecoinCompositeChartUrl(url),
      )
    ) {
      const { dominanceChartsValue } = await import("./collectionDominancePresentation");
      const readings: Parameters<typeof dominanceChartsValue>[0] = [];
      const chartImages = new Map<string, string>();
      const seen = new Set<string>();
      for (const sourceUrl of urls) {
        const url =
          tradingViewDominanceChartUrl(sourceUrl) ??
          tradingViewStablecoinCompositeChartUrl(sourceUrl)!;
        if (seen.has(url)) continue;
        seen.add(url);
        const symbol = new URL(url).searchParams
          .get("symbol")!
          .split("+")
          .map((part) => part.replace(/^CRYPTOCAP:/, ""))
          .join("+") as DominanceChartSymbol;
        const capture = await captureCollectionSource(url, false, {
          dominanceChart: symbol,
        });
        if (!capture.image || !capture.dominanceChart)
          throw new Error("לא אומתו צילום ואחוזי הגרף היומי; התיבה לא שונתה.");
        // Both configured charts retain their own real source-owned image.
        chartImages.set(url, capture.image);
        readings.push({ chart: capture.dominanceChart, sourceUrl: url });
        cursors[sourceUrl] = { scanned_at: scanStartedAt };
      }
      for (const reading of readings) {
        const image = chartImages.get(reading.sourceUrl);
        if (image)
          reading.imagePath = await storeSourceImage(
            db,
            claim.session.workspace_id,
            claim.run_id,
            image,
          );
      }
      payload.value = dominanceChartsValue(readings, claim.baseline, settings.color ?? undefined);
    } else {
      const pages: Array<{ url: string; text: string }> = [];
      const images: string[] = [];
      let screenshot: string | undefined;
      let etfCandles: 21 | undefined;
      let etfSnapshot: VerifiedEtfSnapshot | undefined;
      let dominanceSnapshot: DominanceSnapshot | undefined;
      let dominanceChart: VerifiedDominanceChart | undefined;
      const unavailableDominanceSources: string[] = [];
      let etfSourceUrl: string | undefined;
      let capturedAt = scanStartedAt;
      for (const url of urls) {
        const directAltSnapshot = kind === "alt_correlation" && isNewhedgeAltcoinSource(url);
        const dominanceOverview =
          kind === "dominance" ? tradingViewDominanceOverviewUrl(url) : null;
        const dailyDominanceUrl = tradingViewDominanceChartUrl(url);
        const stablecoinCompositeUrl = tradingViewStablecoinCompositeChartUrl(url);
        if (dominanceOverview) {
          const capture = await captureCollectionSource(dominanceOverview, false, {
            textOnly: true,
            dominance: true,
          });
          if (!capture.dominanceSnapshot)
            throw new Error("לא אומתו נתוני BTC.D במקור; התיבה לא שונתה.");
          if (
            dominanceSnapshot &&
            JSON.stringify(dominanceSnapshot) !== JSON.stringify(capture.dominanceSnapshot)
          )
            throw new Error("מקורות הדומיננס אינם מסכימים; התיבה לא שונתה.");
          dominanceSnapshot = capture.dominanceSnapshot;
          capturedAt = capture.capturedAt;
          pages.push({ url: dominanceOverview, text: capture.text });
          if (dailyDominanceUrl) {
            try {
              const chart = await captureCollectionSource(dailyDominanceUrl, false, {
                dominanceChart: "BTC.D",
              });
              if (
                chart.image &&
                chart.dominanceChart &&
                Math.abs(
                  chart.dominanceChart.dominancePercent -
                    capture.dominanceSnapshot.dominancePercent,
                ) <= 0.1
              ) {
                screenshot = chart.image;
                dominanceChart = chart.dominanceChart;
                capturedAt = chart.capturedAt;
              }
            } catch {
              // The independently verified current level/changes remain usable.
              console.warn("collection_dominance_daily_chart_unavailable", {
                symbol: "BTC.D",
              });
            }
          }
        } else if (
          dailyDominanceUrl &&
          new URL(dailyDominanceUrl).searchParams.get("symbol") === "CRYPTOCAP:USDT.D"
        ) {
          // USDT.D's custom instructions also request a second composite source.
          // Keep those instructions/data; attach only the verified primary chart.
          try {
            const chart = await captureCollectionSource(dailyDominanceUrl, false, {
              dominanceChart: "USDT.D",
            });
            if (!chart.image || !chart.dominanceChart) throw new Error("Daily chart unavailable.");
            pages.push({ url: dailyDominanceUrl, text: chart.text });
            images.push(chart.image);
            screenshot = chart.image;
            dominanceChart = chart.dominanceChart;
            capturedAt = chart.capturedAt;
          } catch {
            console.warn("collection_dominance_daily_chart_unavailable", {
              symbol: "USDT.D",
            });
            const capture = await captureCollectionSource(url);
            pages.push({ url, text: capture.text });
            if (capture.image) images.push(capture.image);
            capturedAt = capture.capturedAt;
          }
        } else if (stablecoinCompositeUrl) {
          // The worksheet explicitly requests this second expression without an
          // image. Its public chart replaces only the inaccessible private layout.
          try {
            const capture = await captureCollectionSource(stablecoinCompositeUrl, false, true);
            pages.push({ url: stablecoinCompositeUrl, text: capture.text });
          } catch {
            if (dominanceChart?.symbol !== "USDT.D")
              throw new Error("מקור הדומיננס המצטבר אינו נגיש; התיבה לא שונתה.");
            unavailableDominanceSources.push(stablecoinCompositeUrl);
          }
        } else if (directAltSnapshot) {
          const scan = await scanSource(url, cursors[url] ?? {}, scanStartedAt, false);
          let text = scan.pageText ?? "";
          // Public HTML may be a loading shell. Render this same configured
          // source once and still require the exact labelled text coefficient.
          // Newhedge-only fields use deterministic extraction after either route.
          if (!readNewhedgeAltcoinSnapshot(text)) {
            const capture = await captureCollectionSource(url, false, true);
            text = capture.text;
            capturedAt = capture.capturedAt;
            if (!readNewhedgeAltcoinSnapshot(text))
              throw new Error(
                "לא נמצא נתון Altcoins Correlation קריא בתמונת המצב העדכנית; התיבה לא שונתה.",
              );
          }
          pages.push({ url, text });
        } else if (
          process.env.FIRECRAWL_API_KEY ||
          ["range", "etf"].includes(kind) ||
          (kind === "dominance" && isTradingViewChart(url))
        ) {
          const capture = await captureCollectionSource(url, kind === "etf");
          pages.push({ url, text: capture.text });
          capturedAt = capture.capturedAt;
          if (capture.image) images.push(capture.image);
          if (kind === "etf") {
            if (!capture.etfSnapshot)
              throw new Error("לא אומת הנתון האחרון ויחידות הדולר בגרף ETF; התיבה לא שונתה.");
            if (
              etfSnapshot &&
              (etfSnapshot.date !== capture.etfSnapshot.date ||
                etfSnapshot.inflowMusd !== capture.etfSnapshot.inflowMusd)
            )
              throw new Error("מקורות ה־ETF אינם מסכימים על הנתון האחרון; התיבה לא שונתה.");
            etfSnapshot = capture.etfSnapshot;
            etfSourceUrl = url;
            screenshot = capture.image;
            etfCandles = capture.visibleEtfCandles;
          }
        } else {
          const scan = await scanSource(url, cursors[url] ?? {}, scanStartedAt, false);
          pages.push({ url, text: scan.pageText ?? "" });
        }
        cursors[url] = { scanned_at: scanStartedAt };
      }
      const data =
        unavailableDominanceSources.length && dominanceChart?.symbol === "USDT.D"
          ? {
              write_mode: "default" as const,
              readable: true,
              text: `USDT.D ${dominanceChart.dominancePercent}%`,
              evidence: `USDT.D ${dominanceChart.dominancePercent}%`,
              qqq: null,
              spy: null,
              alt: null,
              dominance_percent: null,
              range_low: null,
              range_high: null,
              etf_inflow_musd: null,
              expiry_dates: [],
            }
          : await readMarketData(
              kind,
              instructions,
              pages,
              images,
              etfSnapshot,
              undefined,
              dominanceSnapshot,
            );
      if (
        settings.write_mode === "default" &&
        ["append", "replace"].includes(data.write_mode ?? "")
      )
        payload.write_mode = data.write_mode;
      if (data.readable && !images.length) {
        const normalized = (text: string) => text.replace(/\s+/g, " ").trim();
        if (
          !data.evidence.trim() ||
          !pages.some((page) => normalized(page.text).includes(normalized(data.evidence)))
        )
          throw new Error("לא אומתה הראיה לנתונים במקור; התיבה לא שונתה.");
      }
      const screenshotPath = screenshot
        ? await storeSourceImage(db, claim.session.workspace_id, claim.run_id, screenshot)
        : undefined;
      const value = marketDataValue(kind, data, {
        color: settings.color,
        capturedAt,
        sourceUrl: etfSourceUrl ?? urls[0],
        screenshotPath,
        verifiedEtfCandles: etfCandles,
        verifiedEtf: etfSnapshot,
        verifiedDominanceChart: dominanceChart,
        previous: claim.baseline,
      });
      if (value && unavailableDominanceSources.length) {
        value.text += '<p class="rt-color-orange">מקור הדומיננס המצטבר לא אומת בסריקה זו.</p>';
        Object.assign(value._autofill as Record<string, unknown>, {
          partial: true,
          unavailable_sources: unavailableDominanceSources,
          warning: "מקור הדומיננס המצטבר לא אומת בסריקה זו.",
        });
      }
      if (value) payload.value = value;
    }
    if (kind === "news" && (sourceFailures.length || pendingSources.length))
      payload.write_mode = "append";
    if (pendingSources.length)
      payload.source_pending_message =
        `${Array.isArray(payload.items) && payload.items.length ? `${payload.items.length} סיכומים נשמרו. ` : ""}ממתין להשלמת ${pendingSources.length} מקורות או סרטונים: ${pendingSources.join("; ")}`.slice(
          0,
          1000,
        );
    if (sourceFailures.length)
      payload.source_failure_message =
        `${Array.isArray(payload.items) && payload.items.length ? `סריקה חלקית: הפריטים התקינים עובדו (${payload.items.length} סיכומים)` : "הסריקה לא הוסיפה פריטים"}; ${sourceFailures.length} כשלים: ${sourceFailures.join("; ")}${pendingSources.length ? `; ${payload.source_pending_message}` : ""}`.slice(
          0,
          1000,
        );
    return await rpc<AutofillResult>(
      db,
      "v2_collection_autofill_apply",
      {
        p_run_id: claim.run_id,
        p_payload: payload,
        p_cursors: cursors,
        p_scan_started_at: scanStartedAt,
      },
      boundedNewsRun ? AbortSignal.timeout(15_000) : undefined,
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "הסריקה נכשלה; התוכן הקיים לא השתנה.";
    // A resumed request owns only the consumed remainder. Finalize through the
    // same guarded apply; an already committed result is returned unchanged.
    if (resumingX && !xCheckpointUncertain)
      return await rpc<AutofillResult>(
        db,
        "v2_collection_autofill_apply",
        {
          p_run_id: claim.run_id,
          p_payload: { write_mode: "append", source_failure_message: message },
          p_cursors: claim.source_cursors,
          p_scan_started_at: scanStartedAt,
        },
        AbortSignal.timeout(15_000),
      );
    await rpc(
      db,
      "v2_collection_autofill_fail",
      { p_run_id: claim.run_id, p_message: message },
      boundedNewsRun ? AbortSignal.timeout(15_000) : undefined,
    );
    if (boundedNewsRun) {
      // An apply response can be lost after commit. The guarded fail cannot
      // cancel a handed-off continuation; report the actual committed state.
      const { data: run, error: readError } = await db
        .from("v2_collection_autofill_runs")
        .select("id,status,session_id,count,message")
        .eq("id", claim.run_id)
        .abortSignal(AbortSignal.timeout(15_000))
        .single();
      if (readError || !run) throw error;
      return {
        ...run,
        run_id: run.id,
        ...(run.status === "running" ? { continue: true } : {}),
      };
    }
    return { run_id: claim.run_id, status: "failed", message };
  }
}

/** Runs independently of an open browser. Cadence is persisted by logical field
 * identity, so a newly generated news session does not reset all due times. */
export async function runCollectionAutofillSchedule(
  db: SupabaseClient,
): Promise<{ processed: number; failed: number }> {
  const { maintainCollectionExpiries } = await import("./collectionExpiryHistory.server");
  await maintainCollectionExpiries(db);
  await rpc(db, "v2_collection_autofill_rollover");
  let processed = 0;
  let failed = 0;
  const attempted = new Set<string>();
  const attemptedCalendars = new Set<string>();
  const deadline = Date.now() + 40 * 60_000;
  for (let batch = 0; batch < 20 && Date.now() < deadline; batch++) {
    const due = await rpc<Array<{ field_id: string; session_id: string }>>(
      db,
      "v2_collection_autofill_due",
      { p_limit: 100 },
    );
    const pending = due.filter((field) => !attempted.has(field.field_id));
    if (!pending.length) break;
    const metadata = await db
      .from("v2_template_fields")
      .select("id,title,field_type,settings")
      .in(
        "id",
        pending.map((field) => field.field_id),
      );
    if (metadata.error) throw new Error("לא ניתן לקרוא את הגדרות תור האיסוף.");
    const serialFields = validatedAutofillSerialGroups(
      metadata.data,
      pending.map((field) => field.field_id),
    );
    const definitions = new Map(
      (metadata.data ?? []).map((field) => [field.id, field as V2TemplateField]),
    );
    const distinct = pending.filter((field) => {
      const definition = definitions.get(field.field_id);
      if (definition?.field_type !== "economic_calendar") return true;
      // Session identity keeps distinct workspaces separate. Keep this set
      // across batches because an unclaimed alias would otherwise remain due.
      const identity = `${field.session_id}:investing-economic-calendar`;
      if (attemptedCalendars.has(identity)) {
        attempted.add(field.field_id);
        return false;
      }
      attemptedCalendars.add(identity);
      return true;
    });
    const fields = distinct.map((field) => ({
      ...field,
      ...(serialFields.has(field.field_id)
        ? { serial_group: serialFields.get(field.field_id)! }
        : {}),
    }));
    // The same lane policy applies to the legacy direct scheduler. Leases are
    // claimed inside the pool only once the field is ready to start.
    const results = await runAutofillFieldPool(
      fields,
      async (field) => {
        attempted.add(field.field_id);
        let result = await fillCollectionField(db, field.field_id, field.session_id);
        while (result.status === "running" && result.continue) {
          if (Date.now() >= deadline) throw new Error("Autofill window expired.");
          await new Promise((resolve) => setTimeout(resolve, COLLECTOR_POLL_MS));
          result = await fillCollectionField(
            db,
            field.field_id,
            field.session_id,
            false,
            result.run_id,
          );
        }
        return result;
      },
      deadline,
    );
    for (const result of results) {
      processed++;
      if (result.status === "rejected" || result.value.status === "failed") failed++;
    }
  }
  return { processed, failed };
}
