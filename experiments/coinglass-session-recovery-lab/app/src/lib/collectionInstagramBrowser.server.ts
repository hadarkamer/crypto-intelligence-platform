import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import {
  instagramStoryAccount,
  instagramStoryFailure,
  validateInstagramStoryCapture,
  type InstagramStoryStage,
} from "./collectionInstagramStories";
import {
  INSTAGRAM_CAPTURE_FILE,
  instagramCaptureScript,
  instagramLoginScript,
} from "./collectionInstagramBrowserScripts";
import { readInstagramStoryImages } from "./collectionInstagramVision.server";
import type { SourceCursor, SourceScan } from "./collectionSources.server";

type Binding = {
  userId: string;
  workspaceId: string;
  fieldId: string;
  account: string;
};
type Handoff = Binding & { version: 1; sessionId: string; expiresAt: number };
const sessionIdPattern = /^[a-zA-Z0-9_-]{8,128}$/;
function secret(): string {
  const value = process.env.COLLECTION_AUTOFILL_WORKER_TOKEN || process.env.FIRECRAWL_API_KEY;
  if (!value || value.length < 32) throw instagramStoryFailure("provider_configuration_missing");
  return value;
}
function signed(payload: unknown): string {
  const encoded = Buffer.from(JSON.stringify(payload)).toString("base64url");
  return `${encoded}.${createHmac("sha256", secret()).update(encoded).digest("base64url")}`;
}
function unpack(token: string): Record<string, unknown> {
  if (typeof token !== "string" || token.length > 4_000) throw instagramStoryFailure("expired");
  const parts = token.split(".");
  if (parts.length !== 2 || !parts.every((part) => /^[A-Za-z0-9_-]+$/.test(part)))
    throw instagramStoryFailure("expired");
  const expected = createHmac("sha256", secret()).update(parts[0]).digest();
  const actual = Buffer.from(parts[1], "base64url");
  if (actual.length !== expected.length || !timingSafeEqual(expected, actual))
    throw instagramStoryFailure("expired");
  try {
    const body = JSON.parse(Buffer.from(parts[0], "base64url").toString());
    if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error();
    return body;
  } catch {
    throw instagramStoryFailure("expired");
  }
}
function profileName(workspaceId: string, account: string): string {
  return `collection-ig-${createHmac("sha256", secret())
    .update(JSON.stringify([workspaceId, account]))
    .digest("hex")}`;
}
function activation(workspaceId: string, account: string): string {
  return signed({
    purpose: "instagram-stories-profile.v1",
    workspaceId,
    account,
  });
}
function checkActivation(token: unknown, workspaceId: string, account: string): void {
  if (typeof token !== "string") throw instagramStoryFailure("login_required");
  let body: Record<string, unknown>;
  try {
    body = unpack(token);
  } catch {
    throw instagramStoryFailure("login_required");
  }
  if (
    body.purpose !== "instagram-stories-profile.v1" ||
    body.workspaceId !== workspaceId ||
    body.account !== account
  )
    throw instagramStoryFailure("login_required");
}
function checkHandoff(token: string, binding: Binding, allowExpired = false): Handoff {
  const body = unpack(token);
  if (
    body.version !== 1 ||
    Object.entries(binding).some(([key, value]) => body[key] !== value) ||
    typeof body.sessionId !== "string" ||
    !sessionIdPattern.test(body.sessionId) ||
    typeof body.expiresAt !== "number" ||
    !Number.isFinite(body.expiresAt) ||
    (!allowExpired && body.expiresAt <= Date.now()) ||
    body.expiresAt > Date.now() + 600_000
  )
    throw instagramStoryFailure("expired");
  return body as Handoff;
}

async function provider(
  method: "POST" | "DELETE",
  path: string,
  body?: unknown,
  timeoutMs = 45_000,
  allowClosed = false,
  stage: InstagramStoryStage = path ? "session_close" : "session_create",
): Promise<Record<string, unknown>> {
  const key = process.env.FIRECRAWL_API_KEY;
  const direct = key?.startsWith("fc-");
  const gateway = process.env.LOVABLE_API_KEY;
  if (!key || (!direct && !gateway))
    throw instagramStoryFailure("provider_configuration_missing", stage);
  const base = direct
    ? "https://api.firecrawl.dev/v2/interact"
    : "https://connector-gateway.lovable.dev/firecrawl/v2/interact";
  let response: Response;
  try {
    response = await fetch(base + path, {
      method,
      redirect: "manual",
      signal: AbortSignal.timeout(timeoutMs),
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${direct ? key : gateway}`,
        ...(!direct ? { "X-Connection-Api-Key": key } : {}),
      },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
  } catch (error) {
    throw instagramStoryFailure(
      error instanceof Error && ["TimeoutError", "AbortError"].includes(error.name)
        ? "provider_timeout"
        : "provider_unavailable",
      stage,
    );
  }
  const limit = 40 * 1024 * 1024;
  const reader = response.body?.getReader();
  try {
    if (allowClosed && method === "DELETE" && [404, 410].includes(response.status))
      return { success: true };
    if (!response.ok) {
      const reason =
        response.status === 409
          ? "profile_busy"
          : [401, 403].includes(response.status)
            ? "provider_auth_failed"
            : response.status === 402
              ? "provider_payment_required"
              : response.status === 429
                ? "provider_rate_limited"
                : [400, 422].includes(response.status)
                  ? "provider_protocol_rejected"
                  : [404, 410].includes(response.status)
                    ? path
                      ? "expired"
                      : "provider_endpoint_unavailable"
                    : "provider_unavailable";
      throw instagramStoryFailure(reason, stage, response.status);
    }
    if (!reader) throw instagramStoryFailure("provider_response_invalid", stage);
    const decoder = new TextDecoder();
    let text = "",
      bytes = 0;
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > limit) throw instagramStoryFailure("provider_response_invalid", stage);
      text += decoder.decode(part.value, { stream: true });
    }
    text += decoder.decode();
    let result;
    try {
      result = JSON.parse(text);
    } catch {
      throw instagramStoryFailure("provider_response_invalid", stage);
    }
    if (
      !result ||
      typeof result !== "object" ||
      Array.isArray(result) ||
      (result.success !== true && !(path.endsWith("/execute") && result.success === false))
    )
      throw instagramStoryFailure("provider_response_invalid", stage);
    return result;
  } finally {
    await reader?.cancel().catch(() => undefined);
    reader?.releaseLock();
  }
}
function interactiveLoginUrl(value: unknown, sessionId: string): string {
  const invalid = () => instagramStoryFailure("session_live_url_invalid", "session_create");
  if (
    typeof value !== "string" ||
    value.length > 4_096 ||
    Array.from(value).some(
      (character) => character.charCodeAt(0) <= 32 || character.charCodeAt(0) === 127,
    )
  )
    throw invalid();
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw invalid();
  }
  if (url.protocol !== "https:" || url.port || url.username || url.password) throw invalid();
  if (url.hostname === "hangar.firecrawl.dev") {
    // Current Firecrawl returns Hangar's control_url. Its /live client reads an
    // opaque capability from the fragment; the public session ID is not in it.
    // Keep the interactive field verbatim: reconstructing it loses that token.
    const capability = url.hash.slice(1);
    const separator = capability.lastIndexOf(".");
    if (
      url.pathname !== "/live" ||
      url.search ||
      !/^[A-Za-z0-9_.-]+$/.test(capability) ||
      separator < 8 ||
      capability.length - separator - 1 < 8
    )
      throw invalid();
  } else if (url.hostname === "liveview.firecrawl.dev") {
    // Preserve the documented legacy provider contract without accepting a
    // different session or an arbitrary Firecrawl subdomain.
    if (url.pathname !== `/${sessionId}` || url.searchParams.get("interactive") !== "true")
      throw invalid();
  } else throw invalid();
  return value;
}
async function createSession(profile: string, login: boolean) {
  const body = await provider("POST", "", {
    ttl: login ? 600 : 240,
    activityTtl: login ? 600 : 240,
    streamWebView: login,
    blockAds: true,
    profile: { name: profile, saveChanges: login },
  });
  const receivedAt = Date.now();
  if (typeof body.id !== "string" || !sessionIdPattern.test(body.id))
    throw instagramStoryFailure("session_identity_invalid", "session_create");
  const id = body.id;
  try {
    const expiresAt = typeof body.expiresAt === "string" ? Date.parse(body.expiresAt) : NaN;
    if (
      !Number.isFinite(expiresAt) ||
      expiresAt <= Date.now() ||
      expiresAt > receivedAt + (login ? 600_000 : 240_000) + 5_000
    )
      throw instagramStoryFailure("session_expiry_invalid", "session_create");
    const loginUrl = login ? interactiveLoginUrl(body.interactiveLiveViewUrl, id) : null;
    return {
      id,
      expiresAt,
      loginUrl,
    };
  } catch (error) {
    await provider("DELETE", `/${id}`).catch(() => undefined);
    throw error;
  }
}
async function execute(
  id: string,
  code: string,
  seconds: number,
  stage: InstagramStoryStage = "login_check",
): Promise<Record<string, unknown>> {
  if (!sessionIdPattern.test(id)) throw instagramStoryFailure("expired");
  const result = await provider(
    "POST",
    `/${id}/execute`,
    { code, language: "node", timeout: seconds },
    (seconds + 15) * 1_000,
    false,
    stage,
  );
  if (
    result.success !== true ||
    result.killed === true ||
    (typeof result.exitCode === "number" && result.exitCode !== 0) ||
    result.error ||
    result.stderr
  ) {
    // Only fixed reasons from our fixed script may enter a user-facing error.
    const match = [result.error, result.stderr]
      .filter((value): value is string => typeof value === "string")
      .map((value) =>
        value.match(
          /IG_STORIES:(login_required|no_stories|unsupported_video|inventory_unverified|inventory_changed|slide_identity|slide_unreadable|incomplete)\b/,
        ),
      )
      .find(Boolean);
    throw instagramStoryFailure(
      match?.[1] ?? (result.killed === true ? "execution_timeout" : "execution_failed"),
      stage,
    );
  }
  const outputs = [result.stdout, result.result];
  if (outputs.some((value) => value != null && typeof value !== "string"))
    throw instagramStoryFailure("execution_invalid", stage);
  const nonempty = outputs.filter(
    (value): value is string => typeof value === "string" && !!value.trim(),
  );
  try {
    const parsed = nonempty.map((output) => JSON.parse(output));
    if (
      !parsed.length ||
      parsed.some((value) => !value || typeof value !== "object" || Array.isArray(value)) ||
      parsed.some((value) => JSON.stringify(value) !== JSON.stringify(parsed[0]))
    )
      throw new Error();
    return parsed[0];
  } catch {
    throw instagramStoryFailure("execution_invalid", stage);
  }
}

export async function beginInstagramConnection(binding: Binding) {
  const session = await createSession(profileName(binding.workspaceId, binding.account), true);
  try {
    await execute(session.id, instagramLoginScript(binding.account), 40);
    const expiresAt = Math.min(session.expiresAt, Date.now() + 600_000);
    return {
      loginUrl: session.loginUrl!,
      expiresAt: new Date(expiresAt).toISOString(),
      token: signed({
        ...binding,
        version: 1,
        sessionId: session.id,
        expiresAt,
      }),
    };
  } catch (error) {
    await provider("DELETE", `/${session.id}`).catch(() => undefined);
    throw error;
  }
}
export async function completeInstagramConnection(token: string, binding: Binding) {
  const handoff = checkHandoff(token, binding);
  const result = await execute(
    handoff.sessionId,
    instagramLoginScript(binding.account, activation(binding.workspaceId, binding.account)),
    40,
  );
  if (result.account !== binding.account || result.authenticated !== true)
    throw instagramStoryFailure("login_required");
  await provider("DELETE", `/${handoff.sessionId}`);
  return { connected: true as const };
}
export async function cancelInstagramConnection(token: string, binding: Binding) {
  const handoff = checkHandoff(token, binding, true);
  if (handoff.expiresAt <= Date.now()) return { closed: true as const };
  await provider("DELETE", `/${handoff.sessionId}`, undefined, 45_000, true);
  return { closed: true as const };
}

/** Firecrawl truncates each stdout/result at 1 MiB; transfer the unchanged capture in chunks. */
async function captureResult(id: string, manifest: Record<string, unknown>) {
  const invalid = () => instagramStoryFailure("execution_invalid", "story_capture");
  if (
    typeof manifest.bytes !== "number" ||
    !Number.isSafeInteger(manifest.bytes) ||
    manifest.bytes < 1 ||
    manifest.bytes > 40 * 1024 * 1024 ||
    typeof manifest.sha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(manifest.sha256)
  )
    throw invalid();
  const chunks: Buffer[] = [];
  for (let offset = 0; offset < manifest.bytes; offset += 512 * 1024) {
    const count = Math.min(512 * 1024, manifest.bytes - offset);
    const part = await execute(
      id,
      `const handle = await require('node:fs/promises').open(${JSON.stringify(INSTAGRAM_CAPTURE_FILE)},'r');
try { const data = Buffer.alloc(${count}); const read = await handle.read(data,0,${count},${offset});
 console.log(JSON.stringify({offset:${offset},bytes:read.bytesRead,data:data.toString('base64')})); }
finally { await handle.close(); }`,
      10,
      "story_capture",
    );
    if (
      part.offset !== offset ||
      part.bytes !== count ||
      typeof part.data !== "string" ||
      part.data.length !== Math.ceil(count / 3) * 4 ||
      !/^[A-Za-z0-9+/]*={0,2}$/.test(part.data)
    )
      throw invalid();
    const bytes = Buffer.from(part.data, "base64");
    if (bytes.length !== count) throw invalid();
    chunks.push(bytes);
  }
  const payload = Buffer.concat(chunks);
  if (
    payload.length !== manifest.bytes ||
    createHash("sha256").update(payload).digest("hex") !== manifest.sha256
  )
    throw invalid();
  try {
    const result = JSON.parse(payload.toString());
    if (!result || typeof result !== "object" || Array.isArray(result)) throw invalid();
    return result as Record<string, unknown>;
  } catch {
    throw invalid();
  }
}

/** Every active photo is captured before extracting any facts. Failure returns no items/cursor. */
export async function readInstagramStories(
  sourceUrl: string,
  cursor: SourceCursor,
  scanStartedAt: string,
  options: { workspaceId: string; publicationOnly?: boolean },
): Promise<SourceScan> {
  const account = instagramStoryAccount(sourceUrl);
  const scanAt = Date.parse(scanStartedAt);
  const cutoff =
    cursor.scanned_at === undefined ? scanAt - 48 * 60 * 60_000 : Date.parse(cursor.scanned_at);
  if (
    !account ||
    !/^[0-9a-f-]{36}$/i.test(options.workspaceId) ||
    !Number.isFinite(scanAt) ||
    !Number.isFinite(cutoff) ||
    cutoff > scanAt
  )
    throw instagramStoryFailure("incomplete");
  const startedAt = Date.now();
  const session = await createSession(profileName(options.workspaceId, account), false);
  let raw: Record<string, unknown>;
  let failed = false;
  try {
    const status = await execute(session.id, instagramLoginScript(account), 40);
    if (status.account !== account || status.authenticated !== true)
      throw instagramStoryFailure("login_required");
    checkActivation(status.activation, options.workspaceId, account);
    const manifest = await execute(
      session.id,
      instagramCaptureScript(account, status.activation as string),
      180,
      "story_capture",
    );
    raw = await captureResult(session.id, manifest);
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    const close = provider("DELETE", `/${session.id}`);
    if (failed) await close.catch(() => undefined);
    else await close;
  }
  checkActivation(raw.activation, options.workspaceId, account);
  const capture = validateInstagramStoryCapture(raw, account, startedAt);
  if (options.publicationOnly)
    return {
      items: [],
      cursor: {},
      publicationHistory: {
        items: capture.slides.map((slide) => ({
          key: slide.url,
          url: slide.url,
          publishedAt: slide.publishedAt,
        })),
        completeSince: new Date(Date.parse(capture.capturedAt) - 24 * 3600_000).toISOString(),
      },
    };
  // Traverse every active slide, but do not pay for/re-emit older successful scans.
  // Keep the inclusive boundary; durable canonical-item keys remove exact repeats.
  const newSlides = capture.slides.filter(
    (slide) =>
      Date.parse(slide.publishedAt) >= cutoff &&
      Date.parse(slide.publishedAt) <= Date.parse(capture.capturedAt),
  );
  const readings = await readInstagramStoryImages(newSlides);
  const byId = new Map(readings.map((reading) => [reading.id, reading]));
  const items = newSlides.flatMap((slide) => {
    const reading = byId.get(slide.id)!;
    // An empty decorative slide is scanned but does not become invented source text.
    if (!reading.text) return [];
    return [
      {
        key: slide.url,
        url: slide.url,
        text: reading.text,
        sourceName: account,
        publishedAt: slide.publishedAt,
        images: reading.informative ? [slide.image] : [],
        mediaOrigin: "post" as const,
        media: reading.informative
          ? [
              {
                url: slide.image,
                provenance: "post_attachment" as const,
                context: reading.text,
              },
            ]
          : [],
      },
    ];
  });
  return {
    items,
    cursor: { scanned_at: scanStartedAt },
    publicationHistory: {
      items: capture.slides.map((slide) => ({
        key: slide.url,
        url: slide.url,
        publishedAt: slide.publishedAt,
      })),
      completeSince: new Date(Date.parse(capture.capturedAt) - 24 * 3600_000).toISOString(),
    },
  };
}
