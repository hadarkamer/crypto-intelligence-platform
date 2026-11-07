/** The scheduler holds only a narrowly scoped relay token. Database and provider
 * credentials stay in the hosted application's server environment. */
import { runAutofillFieldPool, type AutofillSerialGroup } from "../src/lib/collectionAutofillQueue";

const DEFAULT_APP_URL = "https://trade-plan-pad.lovable.app";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const STATUSES = new Set(["completed", "unchanged", "running", "failed", "conflict"]);
const DUE_TIMEOUT_MS = 30_000;
const FILL_TIMEOUT_MS = 12 * 60_000;
// Preserve the existing field budget below the twenty-minute claim lease.
// CoinGlass waiting happens between requests; other sources retain 12min.
const COINGLASS_RUN_TIMEOUT_MS = 18.5 * 60_000;
const COINGLASS_FILL_TIMEOUT_MS = 180_000;
const RUN_WINDOW_MS = 40 * 60_000;

type DueField = { field_id: string; session_id: string; serial_group?: AutofillSerialGroup };
type RelayFetch = (url: string, init: RequestInit) => Promise<Response>;
type RelayOptions = {
  appUrl?: string;
  workerToken?: string;
  fetchImpl?: RelayFetch;
  now?: () => number;
  wait?: (ms: number) => Promise<void>;
};
export type RelayResult = { processed: number; failed: number; ready?: false };

export class CollectionAutofillRelayError extends Error {
  constructor(
    readonly code: "configuration" | "unauthorized" | "not_configured" | "network" | "protocol",
    message: string,
  ) {
    super(message);
    this.name = "CollectionAutofillRelayError";
  }
}

const protocolError = () =>
  new CollectionAutofillRelayError("protocol", "The app returned an invalid autofill response.");

function endpointFor(raw: string): string {
  try {
    if (
      [...raw].some(
        (character) => character.charCodeAt(0) <= 32 || character.charCodeAt(0) === 127,
      ) ||
      !/^https:\/\/[^/?#]+\/?$/i.test(raw)
    )
      throw new Error();
    const url = new URL(raw);
    const labels = url.hostname.split(".");
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      url.search ||
      url.hash ||
      url.pathname !== "/" ||
      labels.length < 2 ||
      labels.some((label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(label)) ||
      /^\d+$/.test(labels.at(-1)!) ||
      /\.(?:local|internal)$/i.test(url.hostname)
    )
      throw new Error();
    return `${url.origin}/api/collection-autofill`;
  } catch {
    throw new CollectionAutofillRelayError(
      "configuration",
      "COLLECTION_AUTOFILL_APP_URL must be a valid HTTPS app origin using port 443.",
    );
  }
}

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

async function limitedJson(response: Response, maximumBytes: number): Promise<unknown> {
  if (!/^application\/json(?:\s*;|$)/i.test(response.headers.get("content-type") ?? ""))
    throw protocolError();
  const reader = response.body?.getReader();
  if (!reader) throw protocolError();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { value, done } = await reader.read().catch(() => {
        throw new CollectionAutofillRelayError("network", "The autofill response was interrupted.");
      });
      if (done) break;
      total += value.byteLength;
      if (total > maximumBytes) {
        await reader.cancel().catch(() => undefined);
        throw protocolError();
      }
      chunks.push(value);
    }
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown;
  } catch (error) {
    if (error instanceof CollectionAutofillRelayError && error.code === "network") throw error;
    throw protocolError();
  } finally {
    reader.releaseLock();
  }
}

/** Each logical field is attempted once per invocation, including failed or
 * busy fields and fields moved to a new session by news-sheet rollover. */
export async function runCollectionAutofillRelay(options: RelayOptions): Promise<RelayResult> {
  const endpoint = endpointFor(
    options.appUrl === undefined || options.appUrl === "" ? DEFAULT_APP_URL : options.appUrl,
  );
  const token = options.workerToken;
  if (!token || !/^[0-9a-f]{64}$/.test(token))
    throw new CollectionAutofillRelayError(
      "configuration",
      "Configure COLLECTION_AUTOFILL_WORKER_TOKEN as exactly 64 lowercase hexadecimal characters.",
    );
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? Date.now;
  const wait =
    options.wait ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const deadline = now() + RUN_WINDOW_MS;
  const attempted = new Set<string>();
  let processed = 0;
  let failed = 0;

  async function post(
    body:
      | { action: "due" }
      | {
          action: "fill";
          field_id: string;
          session_id: string;
          run_id?: string;
        },
    serialGroup?: AutofillSerialGroup,
    requestDeadline = deadline,
  ) {
    let response: Response;
    try {
      response = await fetchImpl(endpoint, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(body),
        redirect: "error",
        credentials: "omit",
        cache: "no-store",
        referrerPolicy: "no-referrer",
        signal: AbortSignal.timeout(
          Math.max(
            1,
            Math.min(
              requestDeadline - now(),
              body.action === "due"
                ? DUE_TIMEOUT_MS
                : serialGroup === "coinglass"
                  ? COINGLASS_FILL_TIMEOUT_MS
                  : FILL_TIMEOUT_MS,
            ),
          ),
        ),
      });
    } catch {
      throw new CollectionAutofillRelayError(
        "network",
        "The autofill app request failed or timed out.",
      );
    }
    if (response.status === 401 || response.status === 403)
      throw new CollectionAutofillRelayError(
        "unauthorized",
        "The app rejected COLLECTION_AUTOFILL_WORKER_TOKEN. Set the same token in Actions and the app.",
      );
    const data = await limitedJson(response, body.action === "due" ? 25_000 : 2_000);
    if (response.status === 503 && record(data) && data.error === "autofill_not_configured")
      throw new CollectionAutofillRelayError(
        "not_configured",
        "Collection autofill is waiting for the hosted app's AI configuration.",
      );
    if (!response.ok)
      throw new CollectionAutofillRelayError(
        "protocol",
        `The app returned an invalid autofill response (HTTP ${response.status}).`,
      );
    return data;
  }

  for (let cycle = 0; cycle < 20 && now() < deadline; cycle++) {
    let data: unknown;
    try {
      data = await post({ action: "due" });
    } catch (error) {
      if (
        cycle === 0 &&
        error instanceof CollectionAutofillRelayError &&
        error.code === "not_configured"
      )
        return { processed: 0, failed: 0, ready: false };
      throw error;
    }
    if (!record(data) || !Array.isArray(data.fields) || data.fields.length > 100)
      throw protocolError();
    const pending = new Map<string, DueField>();
    for (const field of data.fields) {
      if (
        !record(field) ||
        typeof field.field_id !== "string" ||
        !UUID.test(field.field_id) ||
        typeof field.session_id !== "string" ||
        !UUID.test(field.session_id) ||
        (field.serial_group !== undefined &&
          field.serial_group !== "coinglass" &&
          field.serial_group !== "youtube")
      )
        throw protocolError();
      const fieldId = field.field_id.toLowerCase();
      if (!attempted.has(fieldId) && !pending.has(fieldId))
        pending.set(fieldId, {
          field_id: fieldId,
          session_id: field.session_id.toLowerCase(),
          ...(field.serial_group
            ? { serial_group: field.serial_group as AutofillSerialGroup }
            : {}),
        });
    }
    if (!pending.size) break;
    const results = await runAutofillFieldPool(
      [...pending.values()],
      async (field) => {
        attempted.add(field.field_id);
        let result: unknown;
        try {
          const end = Math.min(
            deadline,
            now() +
              (field.serial_group === "coinglass" ? COINGLASS_RUN_TIMEOUT_MS : FILL_TIMEOUT_MS),
          );
          let runId: string | undefined;
          let failures = 0;
          for (;;) {
            if (runId && now() >= end)
              throw new CollectionAutofillRelayError("network", "Autofill run deadline exceeded.");
            try {
              result = await post(
                {
                  action: "fill",
                  field_id: field.field_id,
                  session_id: field.session_id,
                  ...(runId ? { run_id: runId } : {}),
                },
                field.serial_group,
                end,
              );
              failures = 0;
            } catch (error) {
              if (
                (field.serial_group !== "coinglass" && !runId) ||
                !(error instanceof CollectionAutofillRelayError) ||
                error.code !== "network" ||
                ++failures > 2 ||
                now() >= end
              )
                throw error;
              await wait(4000);
              continue;
            }
            if (
              !record(result) ||
              typeof result.status !== "string" ||
              !STATUSES.has(result.status) ||
              (runId !== undefined && result.run_id !== runId)
            )
              throw protocolError();
            if (result.continue !== true) break;
            if (
              result.status !== "running" ||
              typeof result.run_id !== "string" ||
              !UUID.test(result.run_id)
            )
              throw protocolError();
            runId = result.run_id;
            await wait(4000);
          }
        } catch (error) {
          console.error(
            JSON.stringify({
              event: "autofill_field_failed",
              field_id: field.field_id,
              session_id: field.session_id,
              code: error instanceof CollectionAutofillRelayError ? error.code : "unknown",
              message:
                error instanceof CollectionAutofillRelayError
                  ? error.message
                  : "The field request failed.",
            }),
          );
          throw error;
        }
        if (!record(result) || typeof result.status !== "string" || !STATUSES.has(result.status))
          throw protocolError();
        if (result.status === "failed" || result.status === "conflict")
          console.error(
            JSON.stringify({
              event: "autofill_field_result",
              field_id: field.field_id,
              session_id: field.session_id,
              status: result.status,
            }),
          );
        return result.status;
      },
      deadline,
      now,
    );
    for (const result of results) {
      processed++;
      if (result.status === "rejected" || result.value === "failed") failed++;
    }
  }
  return { processed, failed };
}

if (import.meta.main) {
  try {
    const result = await runCollectionAutofillRelay({
      appUrl: process.env.COLLECTION_AUTOFILL_APP_URL,
      workerToken: process.env.COLLECTION_AUTOFILL_WORKER_TOKEN,
    });
    console.log(JSON.stringify({ processed: result.processed, failed: result.failed }));
    if (result.ready === false) {
      console.error("Collection autofill is waiting for the hosted app's AI configuration.");
      if (process.env.COLLECTION_AUTOFILL_ALLOW_UNCONFIGURED !== "true") process.exitCode = 1;
    } else if (result.failed) process.exitCode = 1;
  } catch (error) {
    console.error(
      error instanceof CollectionAutofillRelayError ? error.message : "The autofill relay failed.",
    );
    console.log(JSON.stringify({ processed: 0, failed: 1 }));
    process.exitCode = 1;
  }
}
