/**
 * stl-dispatcher — periodic-task Worker
 *
 * HISTORY
 *   Originally built as a single "consolidated dispatcher" because Cloudflare's
 *   free tier capped cron triggers at 5/account. As of 2026-07-02 the account
 *   was at 5/5, leaving no slot for stl-bucket's Friday newsletter ingestion
 *   after a deploy silently dropped its trigger — so every subsequent task
 *   (space, earth, intel, podcast, schools, music/sports) got bundled into
 *   this one Worker's single daily invocation, gated by in-code day-of-week
 *   checks instead of real Cron Triggers.
 *
 *   That bundling had a real cost: the free tier also caps subrequests at
 *   50/invocation. Every fetch() across every task in one invocation counts
 *   against the same 50 — whichever tasks ran LAST inherited whatever budget
 *   was left. This caused two separate incidents:
 *     - Aug 1-2 / Aug 6, 2026: podcastIngest (last in the original task
 *       order) started failing "Too many subrequests" once spaceIngest +
 *       earthIngest's combined ~26 subrequests plus everything ahead of them
 *       ate most of the 50-subrequest budget. Fixed by giving podcast its
 *       own cron (30 11 * * *), confirmed via runPodcastOnly() below.
 *     - Aug 10-21, 2026: intelRefresh and schoolsRefresh (also late in the
 *       task order, gated Mon/Wed/Thu and Mon/Wed/Fri respectively) went
 *       silently stale for 11+ days via the identical mechanism — caught via
 *       manual /health + forced /trigger diagnosis on 2026-08-21.
 *
 * UPDATED 2026-08-21: account upgraded to Workers Paid ($5/mo — 250 cron
 *   triggers/account, 10,000 subrequests/invocation by default). The
 *   original constraint that forced everything into one Worker no longer
 *   applies. Every previously-bundled task now has its own dedicated Cron
 *   Trigger (see wrangler.jsonc) and its own top-level scheduled entry point
 *   below — no more in-code utcDay gating; the cron expression's own
 *   day-of-week field does that now, so the schedule can't silently drift
 *   from the code the way Task 10 (music/sports) once did when it vanished
 *   from this file entirely during an unrelated edit.
 *
 *   This file is kept as ONE Worker (not split into 8 separate Workers)
 *   deliberately — it already owns all the relevant KV bindings, secrets,
 *   and heartbeat/health plumbing, and Cloudflare bills/executes each cron
 *   firing as its own independent invocation regardless of how many crons
 *   point at the same Worker. Splitting into separate Workers would only
 *   add secret/binding duplication with no isolation benefit beyond what
 *   separate crons on this one Worker already provide.
 *
 * SCHEDULE (see wrangler.jsonc "triggers" for the authoritative list)
 *   NOTE: weekday fields use Cloudflare's Quartz-style names (MON/TUE/etc),
 *   not numbers — Cloudflare's cron numbering (1=Sunday...7=Saturday) does
 *   NOT match JS's Date.getUTCDay() (0=Sunday...6=Saturday), and the first
 *   deploy of this schedule used numeric days and silently landed on the
 *   wrong weekdays as a result (caught 2026-08-22, before it could cause a
 *   repeat of the Aug 10-21 stale-data incident). Always use names here.
 *   0 11 * * *            keepalive (gated internally to >=5 days) + statusSync
 *   5 11 * * FRI          stlBucket newsletter ingestion — Friday only
 *   10 11 * * *           spaceIngest — daily
 *   15 11 * * *           earthIngest — daily (must finish before :30 podcast run)
 *   20 11 * * MON,WED,THU intelRefresh — Mon/Wed/Thu
 *   25 11 * * MON,WED,FRI schoolsRefresh — Mon/Wed/Fri
 *   30 11 * * *           podcastIngest — daily, reads SPACE_KV/EARTH_KV
 *                         written by the :10/:15 runs, needs its own fresh
 *                         subrequest budget (ElevenLabs TTS + Workers AI)
 *   35 11 * * SUN         music + sports pulse refresh — Sunday only
 *
 * BINDINGS (wrangler.jsonc)
 *   STATUS_KV            KV namespace (heartbeat storage)
 *   SPACE_KV              KV namespace (space-news dashboard data — written
 *                         here, read back out by the separate `space` Worker)
 *   EARTH_KV              KV namespace (earth-sciences dashboard data —
 *                         written here, read back out by the separate
 *                         `earth` Worker, key name "earth-data")
 *   PODCAST_KV            KV namespace — episode records, RSS manifest,
 *                         monthly ElevenLabs credit ledger. Also bound on
 *                         the separate `pod` Worker (pod.stluker.com).
 *   POD_BUCKET            R2 bucket (pod-audio) — generated MP3s, also
 *                         bound on the `pod` Worker for Range-request serving.
 *   SUPABASE_URL          secret
 *   SUPABASE_ANON_KEY      secret
 *   DISPATCH_SECRET        secret (gates /trigger and authenticated /health)
 *   CF_API_TOKEN            secret — scoped to Workers Scripts:Read, Workers Cron:Read
 *   CF_ACCOUNT_ID           secret
 *   GITHUB_TOKEN            secret — fine-grained PAT, Contents:Read/Write on
 *                           pdluker/stl-status ONLY
 *   STL_BUCKET_SECRET       secret — Bearer token for stl-bucket's /refresh.
 *                           Must match stl-bucket's own REFRESH_SECRET.
 *   ANTHROPIC_API_KEY       secret — used by spaceIngest/earthIngest/podcast
 *   INTEL_SECRET            secret — Bearer token for intel's /refresh.
 *                           Must match intel's own INTEL_SECRET.
 *   SCHOOLS_SECRET          secret — Bearer token for schools' /refresh.
 *                           Must match schools' own SCHOOLS_SECRET.
 *   ELEVENLABS_API_KEY       secret
 *   ELEVENLABS_VOICE_ID      secret
 *   MUSIC_SECRET             secret — Bearer token for stl-music's /api/pulse
 *   SPORTS_SECRET            secret — Bearer token for stl-sports' /api/pulse
 *
 * HTTP ROUTES
 *   GET  /health   — unauthenticated callers get {ok:true}; authenticated
 *                    (Bearer DISPATCH_SECRET) callers get full per-task
 *                    heartbeat detail.
 *   POST /trigger   — Authorization: Bearer <DISPATCH_SECRET> required.
 *                     Manually fires whichever tasks are opted in via
 *                     includeX=true query params, regardless of day —
 *                     the day-of-week gate now lives only in the cron
 *                     expressions, not in this handler, so manual testing
 *                     is no longer blocked by "wrong day of the week."
 */

import { checkAuth } from './auth.js';
import { recordHeartbeat, readHeartbeat, evaluateHeartbeat } from './heartbeat.js';
import { reconcile } from './reconcile.js';
import { fetchDataJson, commitDataJson } from './github.js';
import { runSpaceIngest } from './space-ingest.js';
import { runEarthIngest } from './earthIngest.js';
import { runPodcastIngest } from './podcast-ingest.js';

const KEEPALIVE_MAX_AGE_HOURS = 24 * 5; // 5 days

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/health' && request.method === 'GET') {
      const unauthorized = await checkAuth(request, env.DISPATCH_SECRET);
      if (unauthorized) {
        return jsonResponse({ ok: true });
      }

      const [keepaliveHb, syncHb, stlBucketHb, spaceHb, earthHb, intelHb, podcastHb, schoolsHb, musicHb, sportsHb] = await Promise.all([
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:keepalive'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:status-sync'),
        readHeartbeat(env.STATUS_KV, 'stl-bucket:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:space-ingest'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:earth-ingest'),
        readHeartbeat(env.STATUS_KV, 'intel:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:podcast-ingest'),
        readHeartbeat(env.STATUS_KV, 'schools:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-music:pulse'),
        readHeartbeat(env.STATUS_KV, 'stl-sports:pulse'),
      ]);

      let podcastLastError = null;
      try {
        const raw = await env.STATUS_KV.get('podcast:last-error');
        if (raw) podcastLastError = JSON.parse(raw);
      } catch { /* absent is the normal, healthy case */ }

      return jsonResponse({
        ok: true,
        keepalive: evaluateHeartbeat(keepaliveHb, KEEPALIVE_MAX_AGE_HOURS),
        statusSync: evaluateHeartbeat(syncHb, 24),
        stlBucket: evaluateHeartbeat(stlBucketHb, 24 * 8),
        spaceIngest: evaluateHeartbeat(spaceHb, 24),
        earthIngest: evaluateHeartbeat(earthHb, 24),
        intelRefresh: evaluateHeartbeat(intelHb, 24 * 4),
        podcastIngest: evaluateHeartbeat(podcastHb, 30),
        podcastLastError,
        schoolsRefresh: evaluateHeartbeat(schoolsHb, 24 * 4),
        musicRefresh: evaluateHeartbeat(musicHb, 24 * 8),
        sportsRefresh: evaluateHeartbeat(sportsHb, 24 * 8),
      });
    }

    if (url.pathname === '/trigger' && request.method === 'POST') {
      const unauthorized = await checkAuth(request, env.DISPATCH_SECRET);
      if (unauthorized) return unauthorized;

      const includeBucket = url.searchParams.get('includeBucket') === 'true';
      const includeSpace = url.searchParams.get('includeSpace') === 'true';
      const includeEarth = url.searchParams.get('includeEarth') === 'true';
      const includeIntel = url.searchParams.get('includeIntel') === 'true';
      const includePodcast = url.searchParams.get('includePodcast') === 'true';
      const includeSchools = url.searchParams.get('includeSchools') === 'true';
      const includeMusicSports = url.searchParams.get('includeMusicSports') === 'true';
      const forcePodcastEpisode = url.searchParams.get('forcePodcast') === 'true';

      // Each task is independent now — no shared runAll(), no day gate.
      // Only the tasks explicitly opted in via includeX=true actually run.
      const results = {};
      results.keepalive_statusSync = await runKeepaliveAndSync(env);
      results.stlBucket = includeBucket
        ? await runBucketOnly(env)
        : { ran: false, reason: 'not requested (includeBucket=false)' };
      results.spaceIngest = includeSpace
        ? await runSpaceOnly(env)
        : { ran: false, reason: 'not requested (includeSpace=false)' };
      results.earthIngest = includeEarth
        ? await runEarthOnly(env)
        : { ran: false, reason: 'not requested (includeEarth=false)' };
      results.intelRefresh = includeIntel
        ? await runIntelOnly(env)
        : { ran: false, reason: 'not requested (includeIntel=false)' };
      results.schoolsRefresh = includeSchools
        ? await runSchoolsOnly(env)
        : { ran: false, reason: 'not requested (includeSchools=false)' };
      results.podcastIngest = includePodcast
        ? await runPodcastOnly(env, { force: forcePodcastEpisode })
        : { ran: false, reason: 'not requested (includePodcast=false)' };
      const musicSports = includeMusicSports
        ? await runMusicSportsOnly(env)
        : { musicRefresh: { ran: false, reason: 'not requested (includeMusicSports=false)' },
            sportsRefresh: { ran: false, reason: 'not requested (includeMusicSports=false)' } };
      results.musicRefresh = musicSports.musicRefresh;
      results.sportsRefresh = musicSports.sportsRefresh;

      return jsonResponse({
        ok: true,
        forced: true,
        includeBucket, includeSpace, includeEarth, includeIntel, includePodcast, includeSchools, includeMusicSports,
        results,
      });
    }

    return new Response('Not found', { status: 404 });
  },

  // Each cron below owns exactly one task and gets its own fresh invocation
  // (fresh subrequest budget, fresh CPU time) — see wrangler.jsonc for the
  // full list and the day-of-week reasoning per task.
  async scheduled(event, env, ctx) {
    // CORRECTED 2026-08-22: these strings must match wrangler.jsonc's
    // "triggers.crons" EXACTLY, character for character -- event.cron is
    // whatever Cloudflare echoes back for the trigger that fired, which is
    // the literal string configured, not a parsed/normalized form. The
    // first version of this file used numeric weekdays (e.g. "1,3,4" meaning
    // Mon/Wed/Thu under JS's Date.getUTCDay() numbering) -- Cloudflare Cron
    // Triggers use different numbering (1=Sunday...7=Saturday, not
    // 0=Sunday...6=Saturday), so those numbers silently meant the wrong days
    // once deployed. Switched to 3-letter weekday names to remove the
    // ambiguity entirely, per Cloudflare's own documentation recommendation.
    switch (event.cron) {
      case '0 11 * * *':
        ctx.waitUntil(runKeepaliveAndSync(env));
        return;
      case '5 11 * * FRI':
        ctx.waitUntil(runBucketOnly(env));
        return;
      case '10 11 * * *':
        ctx.waitUntil(runSpaceOnly(env));
        return;
      case '15 11 * * *':
        ctx.waitUntil(runEarthOnly(env));
        return;
      case '20 11 * * MON,WED,THU':
        ctx.waitUntil(runIntelOnly(env));
        return;
      case '25 11 * * MON,WED,FRI':
        ctx.waitUntil(runSchoolsOnly(env));
        return;
      case '30 11 * * *':
        ctx.waitUntil(runPodcastOnly(env, { force: false }));
        return;
      case '35 11 * * SUN':
        ctx.waitUntil(runMusicSportsOnly(env));
        return;
      default:
        console.error(`[stl-dispatcher] scheduled() fired with unrecognized cron expression: ${event.cron}`);
    }
  },
};

// ── keepalive (gated to >=5 days) + status sync — daily, 11:00 UTC ────────
async function runKeepaliveAndSync(env) {
  const hb = await readHeartbeat(env.STATUS_KV, 'stl-dispatcher:keepalive');
  const { stale } = evaluateHeartbeat(hb, KEEPALIVE_MAX_AGE_HOURS);
  const results = {};

  if (stale) {
    try {
      const pingResults = await runKeepalive(env);
      const allOk = pingResults.every((r) => r.ok);
      if (!allOk) throw new Error(`One or more keepalive pings failed: ${JSON.stringify(pingResults)}`);
      results.keepalive = { ran: true, ok: true, pingResults };
      await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:keepalive', { targets: pingResults.length });
    } catch (e) {
      results.keepalive = { ran: true, ok: false, error: String(e) };
      console.error('[stl-dispatcher] keepalive failed:', e);
    }
  } else {
    results.keepalive = { ran: false, reason: 'not due yet (< 5 days since last success)' };
  }

  try {
    const syncResult = await runStatusSync(env);
    results.statusSync = { ran: true, ok: true, ...syncResult };
    await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:status-sync', {});
  } catch (e) {
    results.statusSync = { ran: true, ok: false, error: String(e) };
    console.error('[stl-dispatcher] status sync failed:', e);
  }

  return results;
}

// ── stl-bucket newsletter ingestion — Friday only, own cron now ───────────
async function runBucketOnly(env) {
  try {
    if (!env.STL_BUCKET_SECRET) throw new Error('STL_BUCKET_SECRET not bound');
    const res = await fetch('https://stl.stluker.com/refresh', {
      method: 'GET',
      headers: { Authorization: `Bearer ${env.STL_BUCKET_SECRET}` },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(`stl-bucket returned ${res.status}: ${JSON.stringify(body)}`);
    const result = { ran: true, ok: true, ...body };
    await recordHeartbeat(env.STATUS_KV, 'stl-bucket:refresh', { triggeredBy: 'dispatcher' });
    return result;
  } catch (e) {
    console.error('[stl-dispatcher] stl-bucket refresh failed:', e);
    return { ran: true, ok: false, error: String(e) };
  }
}

// ── spaceIngest — daily, own cron now ──────────────────────────────────────
async function runSpaceOnly(env) {
  try {
    if (!env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not bound');
    const blob = await runSpaceIngest(env);
    const result = {
      ran: true,
      ok: true,
      sourcesPolled: blob.meta.sourcesPolled,
      sourcesErrored: blob.meta.sourcesErrored,
    };
    await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:space-ingest', {
      breakingCount: blob.breaking.last24h.length,
      launchCount: blob.launches.next7Days.length,
    });
    return result;
  } catch (e) {
    console.error('[stl-dispatcher] space ingest failed:', e);
    return { ran: true, ok: false, error: String(e) };
  }
}

// ── earthIngest — daily, own cron now (must complete before :30 podcast) ──
async function runEarthOnly(env) {
  try {
    if (!env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not bound');
    if (!env.EARTH_KV) throw new Error('EARTH_KV not bound');
    const blob = await runEarthIngest(env);
    const result = {
      ran: true,
      ok: true,
      warnings: blob.meta.warnings,
      significantQuakes: blob.quakes.count,
      nmszStatus: blob.nmsz.status,
    };
    await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:earth-ingest', {
      significantQuakes: blob.quakes.count,
      nmszEventCount: blob.nmsz.eventCount,
      warningCount: blob.meta.warnings.length,
    });
    return result;
  } catch (e) {
    console.error('[stl-dispatcher] earth ingest failed:', e);
    return { ran: true, ok: false, error: String(e) };
  }
}

// ── intel refresh — Mon/Wed/Thu, own cron now (day gate is the cron itself)
async function runIntelOnly(env) {
  try {
    if (!env.INTEL_SECRET) throw new Error('INTEL_SECRET not bound');
    const res = await fetch('https://intel.stluker.com/refresh', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.INTEL_SECRET}`,
        'User-Agent': 'stl-dispatcher/1.0 (+https://stluker.com; internal service call)',
      },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || !body.ok) throw new Error(`intel returned ${res.status}: ${JSON.stringify(body)}`);
    const result = { ran: true, ok: true, storyCount: body.storyCount, diagnostics: body.diagnostics };
    await recordHeartbeat(env.STATUS_KV, 'intel:refresh', { storyCount: body.storyCount, triggeredBy: 'dispatcher' });
    return result;
  } catch (e) {
    console.error('[stl-dispatcher] intel refresh failed:', e);
    return { ran: true, ok: false, error: String(e) };
  }
}

// ── schools refresh — Mon/Wed/Fri, own cron now (day gate is the cron itself)
async function runSchoolsOnly(env) {
  try {
    if (!env.SCHOOLS_SECRET) throw new Error('SCHOOLS_SECRET not bound');
    const res = await fetch('https://schools.stluker.com/refresh', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.SCHOOLS_SECRET}`,
        'User-Agent': 'stl-dispatcher/1.0 (+https://stluker.com; internal service call)',
      },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || !body.ok) throw new Error(`schools returned ${res.status}: ${JSON.stringify(body)}`);
    const result = {
      ran: true,
      ok: true,
      storyCount: body.storyCount,
      counts: body.counts,
      diagnostics: body.diagnostics,
      sourcesErrored: body.sourcesErrored,
    };
    await recordHeartbeat(env.STATUS_KV, 'schools:refresh', { storyCount: body.storyCount, triggeredBy: 'dispatcher' });
    return result;
  } catch (e) {
    console.error('[stl-dispatcher] schools refresh failed:', e);
    return { ran: true, ok: false, error: String(e) };
  }
}

// ── podcastIngest — daily, own cron (unchanged from the Aug 6 fix) ────────
// Deliberately its own invocation with nothing run ahead of it — the entire
// point of the original split this file's history documents above.
async function runPodcastOnly(env, { force = false } = {}) {
  try {
    for (const k of ['ANTHROPIC_API_KEY', 'ELEVENLABS_API_KEY', 'ELEVENLABS_VOICE_ID']) {
      if (!env[k]) throw new Error(`missing secret: ${k}`);
    }
    if (!env.PODCAST_KV) throw new Error('PODCAST_KV not bound');
    if (!env.POD_BUCKET) throw new Error('POD_BUCKET not bound');

    const result = await runPodcastIngest(env, { force });
    if (result.ok) {
      await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:podcast-ingest', {
        day: result.day,
        skipped: result.skipped || null,
        episodeId: result.episode?.id || null,
        credits: result.episode?.credits || null,
      });
      try { await env.STATUS_KV.delete('podcast:last-error'); } catch { /* non-fatal */ }
    } else {
      console.error('[stl-dispatcher] podcast run returned ok:false:', result.error);
      try {
        await env.STATUS_KV.put('podcast:last-error', JSON.stringify({
          error: result.error || 'unknown (ok:false with no error field)',
          diagnostics: result.diagnostics || null,
          day: result.day || new Date().toISOString().slice(0, 10),
          timestamp: new Date().toISOString(),
          wasForced: force,
          source: 'podcast-cron-ok-false',
        }));
      } catch { /* logging the error must never itself throw */ }
    }
    return result;
  } catch (e) {
    console.error('[stl-dispatcher] podcast run failed:', e);
    try {
      await env.STATUS_KV.put('podcast:last-error', JSON.stringify({
        error: String(e),
        stack: e?.stack || null,
        day: new Date().toISOString().slice(0, 10),
        timestamp: new Date().toISOString(),
        wasForced: force,
        source: 'podcast-cron-exception',
      }));
    } catch { /* logging the error must never itself throw */ }
    return { ran: true, ok: false, error: String(e) };
  }
}

// ── music + sports pulse — Sunday only, own cron now ──────────────────────
async function runMusicSportsOnly(env) {
  const results = {};
  const uaHeaders = { 'User-Agent': 'stl-dispatcher/1.0 (+https://stluker.com; internal service call)' };

  try {
    if (!env.MUSIC_SECRET) throw new Error('MUSIC_SECRET not bound');
    const musicRes = await fetch('https://music.stluker.com/api/pulse', {
      headers: { Authorization: `Bearer ${env.MUSIC_SECRET}`, ...uaHeaders },
    });
    const musicBody = await musicRes.json().catch(() => ({}));
    if (!musicRes.ok) throw new Error(`stl-music returned ${musicRes.status}: ${JSON.stringify(musicBody)}`);
    results.musicRefresh = { ran: true, ok: true, ...musicBody };
    await recordHeartbeat(env.STATUS_KV, 'stl-music:pulse', { triggeredBy: 'dispatcher' });
  } catch (e) {
    results.musicRefresh = { ran: true, ok: false, error: String(e) };
    console.error('[stl-dispatcher] music pulse failed:', e);
  }

  try {
    if (!env.SPORTS_SECRET) throw new Error('SPORTS_SECRET not bound');
    const sportsRes = await fetch('https://sports.stluker.com/api/pulse', {
      headers: { Authorization: `Bearer ${env.SPORTS_SECRET}`, ...uaHeaders },
    });
    const sportsBody = await sportsRes.json().catch(() => ({}));
    if (!sportsRes.ok || sportsBody.ok === false) {
      throw new Error(`stl-sports returned ${sportsRes.status}: ${JSON.stringify(sportsBody)}`);
    }
    results.sportsRefresh = { ran: true, ok: true, ...sportsBody };
    await recordHeartbeat(env.STATUS_KV, 'stl-sports:pulse', { triggeredBy: 'dispatcher' });
  } catch (e) {
    results.sportsRefresh = { ran: true, ok: false, error: String(e) };
    console.error('[stl-dispatcher] sports pulse failed:', e);
  }

  return results;
}

// ── Task 1 implementation — pings Supabase + fire-api ─────────────────────
async function runKeepalive(env) {
  const targets = [
    { name: 'supabase', url: `${env.SUPABASE_URL}/rest/v1/stl_issues?select=id&limit=1`, key: env.SUPABASE_ANON_KEY },
    { name: 'fire-api', url: 'https://fire-api.stluker.com/api/health' },
  ];

  const out = [];
  for (const t of targets) {
    try {
      const res = await fetch(t.url, {
        headers: t.key ? { apikey: t.key, Authorization: `Bearer ${t.key}` } : {},
      });
      out.push({ table: t.name, status: res.status, ok: res.ok });
    } catch (e) {
      out.push({ table: t.name, status: 0, ok: false, error: String(e) });
    }
  }
  return out;
}

// ── Task 2 implementation — reconciliation against live Cloudflare state ──
async function runStatusSync(env) {
  const { sha } = await fetchDataJson(env);
  const stored = await env.STATUS_KV.get('status-data');
  const data = stored ? JSON.parse(stored) : (await fetchDataJson(env)).data;

  const before = JSON.stringify(data.verification?.findings ?? []);
  const reconciled = await reconcile(env, data);
  const after = JSON.stringify(reconciled.verification.findings);

  reconciled.meta = {
    lastUpdated: new Date().toISOString().slice(0, 10),
    version: bumpPatchVersion(reconciled.meta?.version),
  };

  await env.STATUS_KV.put('status-data', JSON.stringify(reconciled));

  const findingsChanged = before !== after;

  if (findingsChanged) {
    await commitDataJson(
      env,
      reconciled,
      sha,
      `chore: auto-sync status data — ${reconciled.verification.findingsCount} finding(s) [${reconciled.meta.lastUpdated}]`
    );
  }

  return {
    findingsCount: reconciled.verification.findingsCount,
    findingsChanged,
    committed: findingsChanged,
  };
}

function bumpPatchVersion(current) {
  const m = /^(\d+)\.(\d+)$/.exec(current || '');
  if (!m) return current || '1.0';
  return `${m[1]}.${parseInt(m[2], 10) + 1}`;
}

// ── Helpers ────────────────────────────────────────────────────────────────
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
  });
}
