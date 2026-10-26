// anthropic.js -- shared Anthropic Messages API plumbing. ADDED 2026-10-26.
//
// One place for model ids and the API version, one way to read text out of
// a response, and one per-day usage log. Call sites keep their own request
// bodies, retries and !res.ok handling -- each of those encodes a specific
// past incident and differs on purpose.
//
// Usage log: every response read through readMessage() appends one entry to
// PODCAST_KV["usage:<YYYY-MM-DD>"] (UTC day), a JSON array of
// { at, tag, model, input, output, cacheRead, cacheWrite, searches }.
// The log is read-modify-write and best-effort: it never throws, and two
// calls finishing in the same instant can drop one entry. Every caller in
// this Worker makes its calls one at a time, so that is not expected.

export const MODELS = {
  SONNET: "claude-sonnet-5-5",
  HAIKU: "claude-haiku-5-5",
};
export const ANTHROPIC_VERSION = "2023-06-01";
const MESSAGES_URL = "https://api.anthropic.com/v1/messages";

// POST a Messages request. Returns the raw Response so callers keep their
// own status handling.
export function postMessages(env, body, fetchImpl = fetch) {
  return fetchImpl(MESSAGES_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": ANTHROPIC_VERSION,
    },
    body: JSON.stringify(body),
  });
}

// Joins every content block of type "text". Throws if there is none: a
// response with no text block (refusal, tool-only, empty content) used to
// read as "" and fail later in a less obvious place.
export function textOf(data, joiner = "") {
  const blocks = (data?.content || []).filter((b) => b.type === "text");
  if (blocks.length === 0) {
    throw new Error(`Anthropic response had no text block (stop_reason: ${data?.stop_reason ?? "unknown"})`);
  }
  return blocks.map((b) => b.text).join(joiner);
}

// Parses an ok Response, logs its usage, and returns { data, text }. Usage
// is logged before the text check so a response with no text still counts.
export async function readMessage(env, res, { tag, joiner = "" } = {}) {
  const data = await res.json();
  await recordUsage(env, tag, data);
  return { data, text: textOf(data, joiner) };
}

export async function recordUsage(env, tag, data) {
  const kv = env?.PODCAST_KV;
  if (!kv) return;
  const u = data?.usage || {};
  const entry = {
    at: new Date().toISOString(),
    tag: tag || null,
    model: data?.model || null,
    input: u.input_tokens ?? null,
    output: u.output_tokens ?? null,
    cacheRead: u.cache_read_input_tokens ?? 0,
    cacheWrite: u.cache_creation_input_tokens ?? 0,
    searches: u.server_tool_use?.web_search_requests ?? 0,
  };
  const key = `usage:${entry.at.slice(0, 10)}`;
  try {
    let log = [];
    try { log = JSON.parse((await kv.get(key)) || "[]"); } catch { log = []; }
    if (!Array.isArray(log)) log = [];
    log.push(entry);
    await kv.put(key, JSON.stringify(log));
  } catch (e) {
    console.error("[anthropic] usage log write failed:", e);
  }
}
