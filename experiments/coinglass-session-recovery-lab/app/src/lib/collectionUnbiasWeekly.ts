export const UNBIAS_WEEKLY_URL = "https://consensus.cryptoquant.com/consensus/btc?period=7d";
export const UNBIAS_VIEWPORT = { width: 1400, height: 900 } as const;

/** The old public BTC route redirects to CryptoQuant's same consensus product. */
export function unbiasWeeklySourceUrl(raw: string): string | null {
  try {
    const url = new URL(raw);
    if (url.protocol !== "https:" || url.username || url.password || url.port) return null;
    const host = url.hostname.toLowerCase();
    if (
      !["consensus.cryptoquant.com", "unbias.fyi", "www.unbias.fyi"].includes(host) ||
      !/^\/consensus\/btc\/?$/.test(url.pathname)
    )
      return null;
    return UNBIAS_WEEKLY_URL;
  } catch {
    return null;
  }
}

export type UnbiasWeeklyProof = {
  version: "unbias_weekly.v1";
  period: "7d";
  sourceUrl: typeof UNBIAS_WEEKLY_URL;
  capturedAt: string;
  asOf: string;
  bullishPercent: number;
  bearishReasons: string[];
  bullishReasons: string[];
  sourceWidth: number;
  sourceHeight: number;
  cropX: number;
  cropY: number;
  width: number;
  height: number;
};
export type UnbiasWeeklySnapshot = UnbiasWeeklyProof & { image_path: string };
const record = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
export function freshUnbiasAsOf(asOf: string, capturedAt: string): boolean {
  const parts = /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})$/.exec(asOf);
  const captured = Date.parse(capturedAt);
  if (!parts || !Number.isFinite(captured)) return false;
  const month = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ].indexOf(parts[1]);
  let year = new Date(captured).getUTCFullYear();
  let day = Date.UTC(year, month, Number(parts[2]));
  if (day > captured + 86400000) day = Date.UTC(--year, month, Number(parts[2]));
  const date = new Date(day);
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month &&
    date.getUTCDate() === Number(parts[2]) &&
    day <= captured + 86400000 &&
    captured - day <= 4 * 86400000
  );
}
const reasons = (value: unknown): value is string[] =>
  Array.isArray(value) &&
  value.length <= 30 &&
  value.every(
    (item) => typeof item === "string" && item.trim().length > 0 && item.length <= 1000,
  ) &&
  new Set(value).size === value.length;
function validProof(value: unknown): value is UnbiasWeeklyProof {
  return (
    record(value) &&
    value.version === "unbias_weekly.v1" &&
    value.period === "7d" &&
    value.sourceUrl === UNBIAS_WEEKLY_URL &&
    typeof value.capturedAt === "string" &&
    Number.isFinite(Date.parse(value.capturedAt)) &&
    typeof value.asOf === "string" &&
    value.asOf.length > 0 &&
    value.asOf.length <= 80 &&
    freshUnbiasAsOf(value.asOf, value.capturedAt) &&
    typeof value.bullishPercent === "number" &&
    value.bullishPercent >= 0 &&
    value.bullishPercent <= 100 &&
    reasons(value.bearishReasons) &&
    reasons(value.bullishReasons) &&
    value.bearishReasons.length + value.bullishReasons.length > 0 &&
    [
      value.sourceWidth,
      value.sourceHeight,
      value.cropX,
      value.cropY,
      value.width,
      value.height,
    ].every(Number.isInteger) &&
    Number(value.sourceWidth) >= 1000 &&
    Number(value.sourceWidth) <= 4096 &&
    Number(value.sourceHeight) >= 500 &&
    Number(value.sourceHeight) <= 4000 &&
    Number(value.cropX) >= 0 &&
    Number(value.cropY) >= 0 &&
    Number(value.width) >= 1000 &&
    Number(value.height) >= 200 &&
    Number(value.cropX) + Number(value.width) <= Number(value.sourceWidth) &&
    Number(value.cropY) + Number(value.height) <= Number(value.sourceHeight)
  );
}
const failure =
  "לא אומת צילום Weekly Consensus הכולל יחס שורי־דובי ואת כל הסימוכין; התיבה לא שונתה.";
export function verifiedUnbiasWeeklyCapture(
  returns: unknown[],
  screenshots: unknown,
  now = Date.now(),
) {
  const entries = returns
    .map((raw) => {
      const value = record(raw) && "value" in raw ? raw.value : raw;
      try {
        return typeof value === "string" ? JSON.parse(value) : value;
      } catch {
        return null;
      }
    })
    .filter(
      (value) =>
        record(value) &&
        ["unbias_weekly_prepare", "unbias_weekly_verify"].includes(String(value.stage)),
    );
  const prepare = entries.filter((value) => value.stage === "unbias_weekly_prepare");
  const verify = entries.filter((value) => value.stage === "unbias_weekly_verify");
  const proof = verify[0]?.proof;
  if (
    prepare.length !== 1 ||
    verify.length !== 1 ||
    prepare[0].ok !== true ||
    verify[0].ok !== true ||
    !validProof(proof) ||
    JSON.stringify(prepare[0].proof) !== JSON.stringify(proof) ||
    Date.parse(proof.capturedAt) > now + 60_000 ||
    now - Date.parse(proof.capturedAt) > 10 * 60_000 ||
    !Array.isArray(screenshots) ||
    screenshots.length !== 2 ||
    typeof screenshots[1] !== "string" ||
    !screenshots[1]
  )
    throw new Error(failure);
  return { image: screenshots[1] as string, proof };
}

/** Stored image access is scoped to the worksheet's own workspace. */
export function readUnbiasWeeklySnapshot(
  value: unknown,
  workspaceId: string,
  scenarioId?: string,
): UnbiasWeeklySnapshot | null {
  if (!record(value) || !validProof(value._autofill_consensus)) return null;
  const snapshot = value._autofill_consensus as UnbiasWeeklySnapshot;
  const prefix = `workspace-images:${workspaceId}/collection-autofill/`;
  const scenarioPrefix =
    scenarioId && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(scenarioId)
      ? `workspace-images:${workspaceId}/scenario/${scenarioId}/`
      : null;
  if (
    !workspaceId ||
    /[\s/\\:%?#]/.test(workspaceId) ||
    typeof snapshot.image_path !== "string" ||
    !(
      (snapshot.image_path.startsWith(prefix) &&
        /^[a-zA-Z0-9-]+\/[a-f0-9]{24}\.png$/.test(snapshot.image_path.slice(prefix.length))) ||
      (scenarioPrefix &&
        snapshot.image_path.startsWith(scenarioPrefix) &&
        /^[a-zA-Z0-9._-]+$/.test(snapshot.image_path.slice(scenarioPrefix.length)) &&
        ![".", ".."].includes(snapshot.image_path.slice(scenarioPrefix.length)))
    )
  )
    return null;
  return snapshot;
}
export function unbiasWeeklySnapshotHtml(snapshot: UnbiasWeeklySnapshot): string {
  return `<p><a href="${UNBIAS_WEEKLY_URL}" target="_blank" rel="noopener noreferrer"><img data-path="${snapshot.image_path}" alt="Weekly Consensus — יחס שורי ודובי וסימוכין" style="width:100%"></a></p>`;
}

export function hasUnbiasWeeklySnapshot(
  value: unknown,
): value is { _autofill_consensus: UnbiasWeeklySnapshot } {
  if (!record(value) || !validProof(value._autofill_consensus)) return false;
  const path = (value._autofill_consensus as UnbiasWeeklySnapshot).image_path;
  return (
    typeof path === "string" &&
    /^workspace-images:[a-zA-Z0-9-]+\/(?:collection-autofill\/[a-zA-Z0-9-]+\/[a-f0-9]{24}\.png|scenario\/[a-zA-Z0-9-]+\/[a-zA-Z0-9._-]+)$/.test(
      path,
    )
  );
}
