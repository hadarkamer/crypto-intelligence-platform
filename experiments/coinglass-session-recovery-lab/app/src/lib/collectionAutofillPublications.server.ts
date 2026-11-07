import type { SupabaseClient } from "@supabase/supabase-js";
import { autofillKind, sourceUrls, supportedSourceUrl } from "./collectionAutofill";
import { collectionNewsLearningSource } from "./collectionNewsRelevance";
import {
  learnPublicationSchedule,
  type PublicationCoverage,
  type PublicationObservation,
  type PublicationScheduleModel,
} from "./collectionAutofillSchedule";
import type { V2TemplateField } from "./workspaceV1";
import { scanSource, type SourceScan } from "./collectionSources.server";

export type PublicationProfileInput = {
  field: V2TemplateField;
  workspace_id: string;
  sheet_key: string;
  logical_key: string;
  source_fingerprint: string;
  source_urls: string[];
  profile: {
    evidence_revision: string;
    model_revision: string | null;
    updated_at: string;
    model: PublicationScheduleModel;
  } | null;
  publications: PublicationObservation[];
  coverage: PublicationCoverage[];
  accepted_history: Array<{ source_url: string; item_key: string; published_at: string }>;
  truncated: boolean;
};
export type PublicationProfileResult = {
  model: PublicationScheduleModel;
  checked_at: string;
  source_count: number;
  warnings: string[];
};
async function profileInput(
  db: SupabaseClient,
  fieldId: string,
  signal?: AbortSignal,
): Promise<PublicationProfileInput> {
  signal?.throwIfAborted();
  const query = db.rpc("v2_collection_autofill_publication_profile", {
    p_field_id: fieldId,
  });
  const { data, error } = await (signal ? query.abortSignal(signal) : query);
  if (error) throw new Error("לא ניתן לקרוא את היסטוריית הפרסומים.");
  if (!data?.field || !Array.isArray(data.publications) || !Array.isArray(data.coverage))
    throw new Error("היסטוריית הפרסומים אינה זמינה.");
  return data as PublicationProfileInput;
}

export function publicationModelFromInput(input: PublicationProfileInput, now = new Date()) {
  const urls = sourceUrls(input.field).filter(supportedSourceUrl);
  const accepted = input.accepted_history.flatMap((item): PublicationObservation[] => {
    // A video permalink does not prove its channel, and an Instagram host does
    // not identify an account. Never assign another author's history to either.
    let host: string;
    try {
      host = new URL(item.source_url).hostname;
    } catch {
      return [];
    }
    if (/(^|\.)(youtube\.com|youtu\.be|instagram\.com)$/i.test(host)) return [];
    const identity = collectionNewsLearningSource(item.source_url);
    const matching = urls.filter((url) => collectionNewsLearningSource(url) === identity);
    return matching.length === 1
      ? [
          {
            source_url: matching[0],
            item_key: item.item_key,
            item_url: item.source_url,
            published_at: item.published_at,
            basis: "accepted_history",
          },
        ]
      : [];
  });
  return learnPublicationSchedule({
    publications: [...input.publications, ...accepted],
    coverage: input.truncated ? [] : input.coverage,
    sourceUrls: urls,
    now,
  });
}

async function cacheModel(
  db: SupabaseClient,
  fieldId: string,
  input: PublicationProfileInput,
  now = new Date(),
  signal?: AbortSignal,
) {
  let latest = input;
  for (let attempt = 0; attempt < 2; attempt++) {
    signal?.throwIfAborted();
    const model = publicationModelFromInput(latest, now);
    if (!latest.profile) throw new Error("תזמון הפרסומים עדיין לא נשמר. בינתיים חל התזמון הידני.");
    const query = db.rpc("v2_collection_autofill_save_publication_model", {
      p_field_id: fieldId,
      p_evidence_revision: latest.profile.evidence_revision,
      p_model: model,
    });
    const saved = await (signal ? query.abortSignal(signal) : query);
    if (saved.error) throw new Error("לא ניתן לעדכן את תזמון הפרסומים שנלמד.");
    if (saved.data === true) return model;
    latest = await profileInput(db, fieldId, signal);
  }
  throw new Error(
    "היסטוריית הפרסומים התעדכנה במהלך הבדיקה. יש לנסות שוב; בינתיים חל התזמון הידני.",
  );
}

/** Record metadata before filtering/summarising; no source cursor or worksheet
 * answer is changed by this separate, service-only evidence transaction. */
export async function observeCollectionPublicationScan(
  db: SupabaseClient,
  {
    fieldId,
    runId,
    sourceUrl,
    scan,
    scannedAt,
    signal,
  }: {
    fieldId: string;
    runId: string | null;
    sourceUrl: string;
    scan: SourceScan;
    scannedAt: string;
    signal?: AbortSignal;
  },
) {
  const end = Date.parse(scannedAt),
    start = end - 56 * 86400_000;
  const raw = scan.publicationHistory?.items ?? scan.items;
  const seen = new Set<string>();
  const valid = raw.filter((item) => {
    const at = Date.parse(item.publishedAt);
    if (
      !item.key ||
      !supportedSourceUrl(item.url) ||
      !Number.isFinite(at) ||
      at < start ||
      at > end ||
      seen.has(item.key)
    )
      return false;
    seen.add(item.key);
    return true;
  });
  const items = valid.slice(-5000).map(({ key, url, publishedAt }) => ({ key, url, publishedAt }));
  const coverageStart = scan.publicationHistory?.completeSince;
  const from =
    valid.length <= 5000 &&
    coverageStart &&
    Number.isFinite(Date.parse(coverageStart)) &&
    Date.parse(coverageStart) < end
      ? new Date(Math.max(start, Date.parse(coverageStart))).toISOString()
      : null;
  signal?.throwIfAborted();
  const query = db.rpc("v2_collection_autofill_record_publications", {
    p_field_id: fieldId,
    p_run_id: runId,
    p_source_url: sourceUrl,
    p_items: items,
    p_coverage_from: from,
    p_scanned_at: scannedAt,
  });
  const { data, error } = await (signal ? query.abortSignal(signal) : query);
  if (error) throw new Error("שמירת זמני הפרסום לא הושלמה.");
  if (!data?.field) throw new Error("לא התקבל אישור להיסטוריית הפרסומים.");
  return cacheModel(db, fieldId, data as PublicationProfileInput, new Date(scannedAt), signal);
}

async function readPublicationHistory(
  url: string,
  workspaceId: string,
  at: string,
): Promise<SourceScan> {
  if (/(^|\.)(youtube\.com|youtu\.be)$/i.test(new URL(url).hostname)) {
    const { readYouTubePublicationHistory } = await import("./collectionYouTubeSource.server");
    return {
      items: [],
      cursor: {},
      publicationHistory: await readYouTubePublicationHistory(url, at),
    };
  }
  if (/(^|\.)instagram\.com$/i.test(new URL(url).hostname)) {
    const { readInstagramStories } = await import("./collectionInstagramBrowser.server");
    return readInstagramStories(url, {}, at, { workspaceId, publicationOnly: true });
  }
  return scanSource(url, {}, at, true, { workspaceId });
}

export async function readCollectionPublicationProfile(
  db: SupabaseClient,
  fieldId: string,
  bootstrap = false,
): Promise<PublicationProfileResult> {
  let input = await profileInput(db, fieldId);
  const at = new Date().toISOString(),
    warnings: string[] = [];
  const urls = sourceUrls(input.field).filter(supportedSourceUrl);
  if (bootstrap && autofillKind(input.field, input.sheet_key) === "news") {
    if (urls.length > 8)
      warnings.push(
        "בבדיקה זו נבדקת היסטוריית שמונת המקורות הראשונים; יתר המקורות נלמדים בסריקות הרגילות. היסטוריה חסרה אינה ראיה לשעות ללא פרסומים.",
      );
    for (const url of urls.slice(0, 8)) {
      // X timing is learned by normal incremental scans. A separate read with
      // an empty cursor purchases the same timeline again without saving progress.
      if (/(^|\.)(x\.com|twitter\.com)$/i.test(new URL(url).hostname)) continue;
      try {
        const scan = await readPublicationHistory(url, input.workspace_id, at);
        await observeCollectionPublicationScan(db, {
          fieldId,
          runId: null,
          sourceUrl: url,
          scan,
          scannedAt: at,
        });
      } catch {
        warnings.push(
          `לא נוספה היסטוריה מאומתת מ־${new URL(url).hostname}; כשל בסריקה אינו נחשב לזמן שקט.`,
        );
      }
    }
    input = await profileInput(db, fieldId);
  } else if (bootstrap) {
    warnings.push("למקור נתונים זה אין היסטוריית פרסומים מאומתת; התזמון הידני נשמר.");
  }
  if (input.truncated)
    warnings.push("ההיסטוריה גדולה מדי לבדיקת חלונות שקטים; לא ידולגו שעות על סמך המדגם החלקי.");
  return {
    model: await cacheModel(db, fieldId, input, new Date(at)),
    checked_at: at,
    source_count: urls.length,
    warnings,
  };
}
