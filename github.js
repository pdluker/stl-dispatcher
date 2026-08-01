/**
 * github.js — minimal GitHub Contents API client for reading/writing data.json
 * in the stl-status repo, from within a Cloudflare Worker (no dependencies).
 *
 * Requires a fine-grained PAT scoped to ONLY the stl-status repo, Contents:
 * Read and write. Do not use a broad/classic token here — this secret lives
 * in a Worker that also runs unrelated tasks (keepalive pings), so keep its
 * blast radius to exactly one repo.
 */

const GITHUB_API = 'https://api.github.com';
const REPO = 'pdluker/stl-status'; // update if the repo ever moves
const FILE_PATH = 'data.json';
const BRANCH = 'main';

/**
 * Fetch the current data.json from GitHub, along with its blob SHA
 * (required by the Contents API to commit an update without clobbering
 * someone else's concurrent edit).
 */
export async function fetchDataJson(env) {
  // CRITICAL: Cloudflare Workers cache fetch() subrequests by default. Without
  // explicitly disabling this, GitHub's Contents API response gets cached at
  // the CF edge and every subsequent call (including cron fires and manual
  // /trigger tests) can silently receive the SAME stale bytes indefinitely —
  // this is exactly what happened on 2026-07-06: 11 consecutive commits all
  // re-wrote the same pre-session Jun 29 snapshot because every fetch was
  // served from a poisoned cache entry, not GitHub's real HEAD.
  const cacheBust = Date.now();
  const res = await fetch(
    `${GITHUB_API}/repos/${REPO}/contents/${FILE_PATH}?ref=${BRANCH}&_cb=${cacheBust}`,
    {
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: 'application/vnd.github+json',
        'User-Agent': 'stl-dispatcher',
        'Cache-Control': 'no-cache',
      },
      cf: { cacheTtl: 0, cacheEverything: false },
    }
  );
  if (!res.ok) {
    throw new Error(`GitHub fetch failed: ${res.status} ${await res.text()}`);
  }
  const body = await res.json();
  const content = decodeBase64Utf8(body.content);
  return { data: JSON.parse(content), sha: body.sha };
}

/**
 * Commit an updated data.json back to GitHub. Uses the SHA from
 * fetchDataJson() to avoid overwriting concurrent manual edits — if someone
 * pushed a change (like the Jul 2 T-13 fixes) between our read and write,
 * GitHub rejects with a 409 rather than silently clobbering it.
 */
export async function commitDataJson(env, data, sha, message) {
  const content = encodeBase64Utf8(JSON.stringify(data, null, 2));
  const res = await fetch(
    `${GITHUB_API}/repos/${REPO}/contents/${FILE_PATH}`,
    {
      method: 'PUT',
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: 'application/vnd.github+json',
        'User-Agent': 'stl-dispatcher',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        message: message || `chore: auto-reconcile status data [${new Date().toISOString().slice(0,10)}]`,
        content,
        sha,
        branch: BRANCH,
      }),
    }
  );
  if (!res.ok) {
    const body = await res.text();
    if (res.status === 409) {
      throw new Error(
        `GitHub commit conflict (409) — data.json was edited concurrently. ` +
        `Not overwriting; next run will pick up the current state. Detail: ${body}`
      );
    }
    throw new Error(`GitHub commit failed: ${res.status} ${body}`);
  }
  return res.json();
}

// ── Base64 helpers (Workers runtime has atob/btoa but they're Latin-1 only —
//    need explicit UTF-8 handling since data.json has em dashes, arrows etc.
//    This is exactly the class of bug that produced the \u2014 vs — diffs
//    seen in this repo's history — handled correctly here from the start.) ──
function encodeBase64Utf8(str) {
  const bytes = new TextEncoder().encode(str);
  let binary = '';
  bytes.forEach((b) => (binary += String.fromCharCode(b)));
  return btoa(binary);
}

function decodeBase64Utf8(b64) {
  const binary = atob(b64.replace(/\n/g, ''));
  const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
  return new TextDecoder('utf-8').decode(bytes);
}
