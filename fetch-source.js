// fetch-source.js — one shared ingestion fetch layer for every stluker Worker
//
// WHY THIS EXISTS
// ---------------
// Six separate source failures were each diagnosed and fixed independently:
//   ESPN      403  (stl-sports, Aug 6)  — egress IP / default UA
//   LL2       429  (space, Jul 24)      — header-less request, no retry
//   MDA       524  (intel, Jul 24)      — upstream timeout, no bound
//   war.gov   403  (intel, Jul 24)      — WAF block, permanent
//   NGA       n/a  (intel, Jul 23)      — client-side rendered
//   Parkway   n/a  (schools)            — parsed, but publishedAt null
//
// Every one of them needed the same three things: a browser-shaped User-Agent,
// a bounded retry with backoff, and a hard timeout. This centralizes those so
// the seventh source failure is handled before it happens, not after.
//
// It also adds the thing none of the ad-hoc fixes had: a persisted
// zeroRunStreak per source. A source that returns HTTP 200 and zero items is
// currently indistinguishable from a quiet news day — that ambiguity is
// exactly what let stl-sports' 7 school iCal feeds sit at count:0 with
// error:null for an unknown period. After N consecutive empty runs, this
// emits a finding instead of staying silent.

export const INTERNAL_UA =
  'stl-dispatcher/1.0 (+https://stluker.com; internal service call)';

// Browser-shaped UA for third parties that block obvious bots (ESPN, LL2, MDA,
// Space Force). Used for external sources only — never for calling your own
// Workers, which expect INTERNAL_UA to pass zone-level bot protection.
export const BROWSER_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36';

const DEFAULT_TIMEOUT_MS = 12000;
const DEFAULT_RETRIES = 1;
const ZERO_STREAK_THRESHOLD = 3;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Fetch one ingestion source with consistent headers, timeout, and retry.
 *
 * @returns {Promise<{ok, status, body, error, attempts, ms}>}
 *   Never throws. Callers branch on .ok, exactly like the existing
 *   Promise.allSettled patterns in space-ingest.js / earthIngest.js.
 */
export async function fetchSource(url, opts = {}) {
  const {
    accept = 'application/rss+xml, application/xml, text/xml, application/json;q=0.9, */*;q=0.8',
    ua = BROWSER_UA,
    timeoutMs = DEFAULT_TIMEOUT_MS,
    retries = DEFAULT_RETRIES,
    headers = {},
    method = 'GET',
    parse = 'text', // 'text' | 'json'
  } = opts;

  const started = Date.now();
  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    if (attempt > 0) {
      // Backoff. 429 with a Retry-After is honored below; this is the floor.
      await sleep(Math.min(2000 * attempt, 5000));
    }

    const controller = new AbortController();
    let timer;
    // The AbortController alone is not sufficient: it only aborts if the
    // underlying fetch implementation actually honors `signal`. Workers' fetch
    // does, but a mock, a polyfill, or a future runtime change would silently
    // turn this timeout into no timeout at all -- and an ingest task that hangs
    // instead of failing is strictly worse than one that errors, because it
    // burns the invocation and records no diagnostic. Racing against an
    // explicit timer makes the bound hold either way.
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new Error(`__timeout__${timeoutMs}`));
      }, timeoutMs);
    });

    try {
      const res = await Promise.race([
        fetch(url, {
          method,
          signal: controller.signal,
          headers: { 'User-Agent': ua, Accept: accept, ...headers },
        }),
        timeout,
      ]);
      clearTimeout(timer);

      // Retry once on the two shapes that are known to be transient here.
      if ((res.status === 429 || res.status >= 500) && attempt < retries) {
        const ra = parseInt(res.headers.get('retry-after') || '0', 10);
        if (ra > 0 && ra <= 10) await sleep(ra * 1000);
        lastError = `HTTP ${res.status}`;
        continue;
      }

      if (!res.ok) {
        return {
          ok: false,
          status: res.status,
          body: null,
          // 403 is called out explicitly: in this ecosystem it has always meant
          // IP/WAF-level blocking that no header change fixes (war.gov, ESPN).
          error:
            res.status === 403
              ? `HTTP 403 — likely IP/WAF block, not fixable by headers. Consider Browser Rendering.`
              : `HTTP ${res.status}`,
          attempts: attempt + 1,
          ms: Date.now() - started,
        };
      }

      const body = parse === 'json' ? await res.json() : await res.text();
      return {
        ok: true,
        status: res.status,
        body,
        error: null,
        attempts: attempt + 1,
        ms: Date.now() - started,
      };
    } catch (e) {
      clearTimeout(timer);
      const raced = String(e.message || '').startsWith('__timeout__');
      lastError =
        e.name === 'AbortError' || raced
          ? `timeout after ${timeoutMs}ms`
          : String(e);
      if (attempt >= retries) break;
    }
  }

  return {
    ok: false,
    status: 0,
    body: null,
    error: lastError || 'unknown fetch failure',
    attempts: retries + 1,
    ms: Date.now() - started,
  };
}

/**
 * Record how a source performed and detect the silent-zero case.
 *
 * Call once per source per run, after parsing. Returns a diagnostics entry in
 * the same shape the existing dashboards already render, plus a findings array
 * suitable for merging into reconcile()'s output.
 *
 * @param kv        namespace to persist streak state (any of the *_KV bindings)
 * @param sourceId  stable id, e.g. 'espn:mlb' or 'rss:esa'
 * @param count     items kept after parsing/filtering
 */
export async function recordSourceResult(kv, sourceId, { count, error, status, ms }) {
  const key = `sourcestreak:${sourceId}`;
  let streak = 0;
  try {
    const raw = await kv.get(key);
    streak = raw ? JSON.parse(raw).zeroRunStreak || 0 : 0;
  } catch {
    streak = 0;
  }

  const emptyOrFailed = !!error || !count;
  streak = emptyOrFailed ? streak + 1 : 0;

  try {
    await kv.put(
      key,
      JSON.stringify({ zeroRunStreak: streak, lastNonZero: emptyOrFailed ? undefined : new Date().toISOString() }),
      { expirationTtl: 60 * 60 * 24 * 60 }
    );
  } catch {
    /* streak tracking must never break ingestion */
  }

  const findings = [];
  if (streak >= ZERO_STREAK_THRESHOLD) {
    findings.push({
      type: 'source_silent',
      severity: error ? 'critical' : 'warning',
      source: sourceId,
      zeroRunStreak: streak,
      message: `Source "${sourceId}" has returned no usable items for ${streak} consecutive runs${
        error ? ` (last error: ${error})` : ' with no error — check whether the feed shape changed'
      }.`,
    });
  }

  return {
    diagnostics: { source: sourceId, count: count || 0, error: error || null, status, ms, zeroRunStreak: streak },
    findings,
  };
}
