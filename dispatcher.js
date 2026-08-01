/**
 * stl-dispatcher — consolidated periodic-task Worker
 *
 * WHY THIS EXISTS
 *   Cloudflare's free tier caps cron triggers at 5 per account. As of 2026-07-02
 *   the account was at 5/5 (stluker, stl-sports, stl-music, supabase-keepalive,
 *   stl-status-sync), which left no slot for stl-bucket's Friday newsletter
 *   ingestion after a deploy silently dropped its trigger.
 *
 *   supabase-keepalive and stl-status-sync are both simple periodic jobs with
 *   no interdependency — ideal candidates to merge under one Worker with one
 *   cron trigger. This frees a permanent slot rather than working around the
 *   ceiling with an external scheduler.
 *
 * SCHEDULE
 *   Single trigger: 0 11 * * *  (daily, 6AM CT / 11:00 UTC — matches the
 *   original stl-status-sync cadence). The keepalive task does NOT need to run
 *   daily (its real requirement is "at least once every 5 days"), so it's
 *   gated by a heartbeat check inside the daily run rather than its own
 *   trigger — same effective behavior, one less cron slot consumed.
 *
 *   REVERTED 2026-07-29 (later same day): briefly ran two schedules on this
 *   Worker (0 11,15 * * *) to stagger Task 9 (Daily Creature Clash) from the
 *   rest — see git history / prior session notes if that combined-expression
 *   approach and its "5 cron trigger limit hit" incident (error 10072) are
 *   ever relevant again. Reverted because Task 9 itself was removed — see
 *   below — Creature Clash now has its own dedicated Cron Trigger directly on
 *   the `podcast` Worker (0 9 * * * = 4am CDT/UTC-5, confirm vs true CST/
 *   UTC-6 if that distinction matters) once a slot was freed up elsewhere.
 *   This dispatcher no longer calls that Worker at all.
 *
 * TASKS
 *   1. runKeepalive(env)   — pings Supabase + fire-api, but only actually
 *                            fires if the last successful ping was >= 5 days
 *                            ago (checked via STATUS_KV heartbeat).
 *   2. runStatusSync(env)  — reconciles data.json against live Cloudflare
 *                            state (cron drift, phantom/untracked Workers,
 *                            heartbeat staleness — see reconcile.js) and
 *                            commits any changes back to the stl-status repo
 *                            via the GitHub Contents API. This is the module
 *                            that closes the "dashboard says fine but isn't"
 *                            failure category hit three times this project
 *                            (keepalive auth, cron drift, stale schema).
 *   3. runAll's stlBucket block — triggers stl-bucket's /refresh endpoint on
 *                            Fridays (UTC), replacing the old cron-job.org job.
 *   4. runSpaceIngest(env) — NEW (2026-07-21). Pulls the top space-news RSS
 *                            feeds + Launch Library 2 launch data, dedups
 *                            cross-source duplicates, Haiku-summarizes, and
 *                            writes the combined blob to SPACE_KV for
 *                            space.stluker.com. Runs daily (every scheduled
 *                            fire); manual /trigger needs includeSpace=true.
 *                            See space-ingest.js for implementation.
 *   5. runEarthIngest(env) — NEW (2026-07-22). Pulls USGS significant quakes
 *                            + a fixed New Madrid Seismic Zone bbox query +
 *                            NASA EONET (wildfires/storms/ice) + the
 *                            Smithsonian/USGS GVP weekly volcano report
 *                            (only re-fetched if the cached copy is >6 days
 *                            old — it's a weekly report, not daily), Haiku-
 *                            explains only the significant/notable items
 *                            (skipped entirely if there's nothing to
 *                            explain), and writes the combined blob to
 *                            EARTH_KV for earth.stluker.com. Runs daily
 *                            (every scheduled fire); manual /trigger needs
 *                            includeEarth=true. Each of its four sources
 *                            degrades independently on failure (see
 *                            earthIngest.js) rather than failing the whole
 *                            task. See earthIngest.js for implementation.
 *   6. runIntelRefresh(env) — NEW (2026-07-24). Calls the separate `intel`
 *                            Worker's own /refresh endpoint (Bearer-auth'd)
 *                            rather than running ingestion logic inline —
 *                            unlike space/earth, intel's fetch+scrape logic
 *                            lives in its own Worker (stl-intel/src/ingest.js),
 *                            not in this repo. Gated Mon/Wed/Thu (UTC), per
 *                            stl-intel/README.md, mirroring the stlBucket
 *                            Friday-gate pattern below — the day-of-week
 *                            logic lives here, not in intel's own index.js,
 *                            which explicitly documents that it doesn't know
 *                            or care what day it is. Runs on the gated days
 *                            for every scheduled fire; manual /trigger needs
 *                            includeIntel=true, same opt-in shape as
 *                            includeBucket/includeSpace/includeEarth.
 *   7. runPodcastIngest(env) — NEW (2026-07-27). MUST run after Tasks 4/5 —
 *                            reads the SPACE_KV/EARTH_KV blobs they just wrote
 *                            in this same invocation to write one daily
 *                            "Orbit and Ground" audio briefing (space + earth
 *                            news, a themed inspirational quote, a fact-
 *                            audited script). One Haiku call for the script,
 *                            one ElevenLabs TTS call, one R2 write, plus a
 *                            same-day idempotency check and a monthly
 *                            ElevenLabs credit budget gate — both enforced
 *                            inside podcast-ingest.js regardless of how this
 *                            task is invoked. Runs daily (every scheduled
 *                            fire); manual /trigger needs includePodcast=true,
 *                            same opt-in shape as includeSpace/includeEarth
 *                            (real ElevenLabs spend, not just Anthropic
 *                            tokens). See podcast-ingest.js for implementation.
 *   8. runSchoolsRefresh(env) — NEW (2026-07-27). Calls the separate
 *                            `schools` Worker's own /refresh endpoint
 *                            (Bearer-auth'd), same shape as Task 6's intel
 *                            call — schools' own fetch+scrape logic lives
 *                            entirely in schools/src/ingest.js, not in this
 *                            repo. Gated Mon/Wed/Fri (UTC) — board/district
 *                            news moves slower than space news and clusters
 *                            early-to-mid week; change the gate days below
 *                            if that proves wrong after a few weeks of real
 *                            diagnostics. Runs on the gated days for every
 *                            scheduled fire; manual /trigger needs
 *                            includeSchools=true, same opt-in shape as
 *                            includeIntel/includeBucket. Costs zero
 *                            Anthropic/ElevenLabs spend in its current MVP
 *                            form (no Haiku pass yet), so this task carries
 *                   *                            less urgency around the opt-in gate than Tasks
 *                            3/4/5/7 — kept it anyway for consistency, and
 *                            because a future Haiku "why it matters" pass
 *                            would change that calculus.
 *
 *   REMOVED 2026-07-29 (later same day): Task 9, runCreatureClashRefresh —
 *   this dispatcher briefly called the separate `podcast` Worker's /refresh
 *   endpoint (Daily Creature Clash), same external-call shape as Task 6/8.
 *   Removed once that Worker got its own dedicated Cron Trigger directly
 *   (0 9 * * * on the `podcast` Worker itself, once a slot was freed up) —
 *   scheduling for that show now lives entirely in that Worker's own
 *   wrangler.jsonc/scheduled() handler, not here. CREATURE_CLASH_SECRET is no
 *   longer bound on this Worker and can be removed if nothing else needs it.
 *
 * BINDINGS (wrangler.jsonc)
 *   STATUS_KV            KV namespace (heartbeat storage)
 *   SPACE_KV              KV namespace (space-news dashboard data — written
 *                         here, read back out by the separate `space` Worker)
 *   EARTH_KV              KV namespace (earth-sciences dashboard data —
 *                         written here, read back out by the separate
 *                         `earth` Worker, key name "earth-data" — NOT the
 *                         namespace ID, see the STATUS_KV mismatch incident
 *                         this project already hit once)
 *   SUPABASE_URL          secret (carried over from supabase-keepalive)
 *   SUPABASE_ANON_KEY      secret
 *   DISPATCH_SECRET        secret (gates the manual /trigger endpoint)
 *   CF_API_TOKEN            secret — scoped to Workers Scripts:Read, Workers Cron:Read
 *   CF_ACCOUNT_ID           secret
 *   GITHUB_TOKEN            secret — fine-grained PAT, Contents:Read/Write on
 *                           pdluker/stl-status ONLY (not a broad token — this
 *                           Worker also handles unrelated keepalive pings, so
 *                           keep the GitHub write scope as narrow as possible)
 *   STL_BUCKET_SECRET       secret — Authorization: Bearer token for stl-bucket
 *                           /refresh endpoint. Same value as stl-bucket's
 *                           REFRESH_SECRET. Replaces cron-job.org stl-bucket job.
 *   ANTHROPIC_API_KEY       secret — NEW (2026-07-21). Not shared with
 *                           stl-bucket's copy — set independently for this
 *                           Worker. Used by space-ingest.js's Haiku pass.
 *   INTEL_SECRET            secret — NEW (2026-07-24). Authorization: Bearer
 *                           token for intel's /refresh endpoint. Must match
 *                           the value set on the `intel` Worker itself — this
 *                           dispatcher does NOT inherit intel's secret, it
 *                           needs its own copy (`wrangler secret put
 *                           INTEL_SECRET` from stl-dispatcher/, same value
 *                           as intel's own INTEL_SECRET).
 *   PODCAST_KV              KV namespace — NEW (2026-07-27). Podcast episode
 *                           records, the RSS manifest, and the monthly
 *                           ElevenLabs credit ledger. Also bound on the
 *                           separate `pod` Worker (pod.stluker.com), which
 *                           reads it at request time to serve /feed.xml —
 *                           same KV-at-request-time pattern as status/space/
 *                           earth.
 *   POD_BUCKET              R2 bucket (pod-audio) — NEW (2026-07-27). Stores
 *                           the generated MP3s. Also bound on the `pod`
 *                           Worker, which streams episodes from it with
 *                           Range support.
 *   ELEVENLABS_API_KEY       secret — NEW (2026-07-27). Not shared with any
 *                           other Worker in this account.
 *   ELEVENLABS_VOICE_ID      secret — NEW (2026-07-27). The narrator voice
 *                           for every episode. Changing this mid-run means
 *                           future episodes sound different from the
 *                           archive — treat as a deliberate, rare change.
 *   SCHOOLS_SECRET          secret — NEW (2026-07-27). Authorization: Bearer
 *                           token for schools' /refresh endpoint. Must match
 *                           the value set on the `schools` Worker itself —
 *                           this dispatcher does NOT inherit schools' secret,
 *                           it needs its own copy (`wrangler secret put
 *                           SCHOOLS_SECRET` from stl-dispatcher/, same value
 *                           as schools' own SCHOOLS_SECRET).
 *   CREATURE_CLASH_SECRET   REMOVED 2026-07-29 — no longer bound or read
 *                           anywhere in this file (Task 9 removed, see TASKS
 *                           above). Safe to `wrangler secret delete
 *                           CREATURE_CLASH_SECRET` from stl-dispatcher/ if
 *                           nothing else references it.
 *
 * HTTP ROUTES
 *   GET  /health   — unauthenticated. Returns last-run status for both tasks.
 *   POST /trigger   — Authorization: Bearer <DISPATCH_SECRET> required.
 *                     Manually fires both tasks immediately (useful for
 *                     testing without waiting for the next cron fire, and for
 *                     verifying this Worker before decommissioning the two it
 *                     replaces).
 */

import { checkAuth } from './auth.js';
import { recordHeartbeat, readHeartbeat, evaluateHeartbeat } from './heartbeat.js';
import { reconcile } from './reconcile.js';
import { fetchDataJson, commitDataJson } from './github.js';
import { runSpaceIngest } from './space-ingest.js';
import { runEarthIngest } from './earthIngest.js';
// No local import for intel — unlike space/earth, intel's ingestion logic
// lives entirely in the separate `intel` Worker (stl-intel/src/ingest.js).
// runIntelRefresh() below just calls its /refresh endpoint over HTTP.
import { runPodcastIngest } from './podcast-ingest.js';
// podcastIngest (Task 7) reads SPACE_KV and EARTH_KV, which Tasks 4/5 must
// have already written in this same run — it is deliberately called last.

const KEEPALIVE_MAX_AGE_HOURS = 24 * 5; // 5 days — matches the original supabase-keepalive cron

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/health' && request.method === 'GET') {
      const [keepaliveHb, syncHb, stlBucketHb, spaceHb, earthHb, intelHb, podcastHb, schoolsHb] = await Promise.all([
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:keepalive'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:status-sync'),
        readHeartbeat(env.STATUS_KV, 'stl-bucket:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:space-ingest'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:earth-ingest'),
        readHeartbeat(env.STATUS_KV, 'intel:refresh'),
        readHeartbeat(env.STATUS_KV, 'stl-dispatcher:podcast-ingest'),
        readHeartbeat(env.STATUS_KV, 'schools:refresh'),
      ]);

      // Surfaces the actual thrown error from the last podcastIngest failure,
      // if any -- written by Task 7's catch block, cleared on next success.
      // Exists so a silent scheduled-run failure is diagnosable from /health
      // alone the next morning, without needing a wrangler tail session
      // running at the exact minute it happened (confirmed necessary after
      // three such failures in a row, Jul 30 - Aug 1).
      let podcastLastError = null;
      try {
        const raw = await env.STATUS_KV.get('podcast:last-error');
        if (raw) podcastLastError = JSON.parse(raw);
      } catch { /* absent is the normal, healthy case */ }

      return jsonResponse({
        ok: true,
        keepalive: evaluateHeartbeat(keepaliveHb, KEEPALIVE_MAX_AGE_HOURS),
        statusSync: evaluateHeartbeat(syncHb, 24),       // expect daily
        stlBucket: evaluateHeartbeat(stlBucketHb, 24 * 8), // expect weekly (Friday) + grace
        spaceIngest: evaluateHeartbeat(spaceHb, 24),     // expect daily
        earthIngest: evaluateHeartbeat(earthHb, 24),     // expect daily
        intelRefresh: evaluateHeartbeat(intelHb, 24 * 4), // expect Mon/Wed/Thu + grace over the Fri-Sun gap
        podcastIngest: evaluateHeartbeat(podcastHb, 30), // expect daily; small grace for TTS/upload time
        podcastLastError,
        schoolsRefresh: evaluateHeartbeat(schoolsHb, 24 * 4), // expect Mon/Wed/Fri + grace over the weekend gap
      });
    }

    if (url.pathname === '/trigger' && request.method === 'POST') {
      const unauthorized = await checkAuth(request, env.DISPATCH_SECRET);
      if (unauthorized) return unauthorized;

      // includeBucket is opt-in and separate from `force`: forcing keepalive/
      // statusSync is free (Cloudflare + GitHub API calls only), but stlBucket
      // calls stl-bucket's /refresh, which spends real Anthropic API tokens on
      // Haiku extraction. A manual /trigger for testing shouldn't silently
      // trigger a paid run — you have to ask for it explicitly.
      const includeBucket = url.searchParams.get('includeBucket') === 'true';
      const includeSpace = url.searchParams.get('includeSpace') === 'true';
      const includeEarth = url.searchParams.get('includeEarth') === 'true';
      const includeIntel = url.searchParams.get('includeIntel') === 'true';
      const includePodcast = url.searchParams.get('includePodcast') === 'true';
      const includeSchools = url.searchParams.get('includeSchools') === 'true';
      // forcePodcast bypasses same-day idempotency ONLY — it never bypasses the
      // monthly ElevenLabs credit budget, which podcastIngest checks internally.
      const forcePodcastEpisode = url.searchParams.get('forcePodcast') === 'true';
      const results = await runAll(env, { force: true, includeBucket, includeSpace, includeEarth, includeIntel, includePodcast, forcePodcastEpisode, includeSchools });
      return jsonResponse({ ok: true, forced: true, includeBucket, includeSpace, includeEarth, includeIntel, includePodcast, includeSchools, results });
    }

    return new Response('Not found', { status: 404 });
  },

  // Single cron trigger — fires all tasks. Keepalive self-gates via heartbeat.
  // REVERTED 2026-07-29 (later same day): this briefly routed on event
  // hour to stagger Task 9 (Daily Creature Clash) onto a second schedule —
  // removed along with Task 9 itself, now that Worker has its own dedicated
  // Cron Trigger instead of being called from here.
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runAll(env, { force: false }));
  },
};

async function runAll(env, { force, includeBucket = false, includeSpace = false, includeEarth = false, includeIntel = false, includePodcast = false, forcePodcastEpisode = false, includeSchools = false }) {
  const results = {};

  // ── Task 1: keepalive (gated) ────────────────────────────────────────
  const hb = await readHeartbeat(env.STATUS_KV, 'stl-dispatcher:keepalive');
  const { stale } = evaluateHeartbeat(hb, KEEPALIVE_MAX_AGE_HOURS);

  if (force || stale) {
    try {
      const pingResults = await runKeepalive(env);
      const allOk = pingResults.every((r) => r.ok);
      if (!allOk) {
        throw new Error(
          `One or more keepalive pings failed: ${JSON.stringify(pingResults)}`
        );
      }
      results.keepalive = { ran: true, ok: true, pingResults };
      await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:keepalive', {
        targets: pingResults.length,
      });
    } catch (e) {
      results.keepalive = { ran: true, ok: false, error: String(e) };
      console.error('[stl-dispatcher] keepalive failed:', e);
    }
  } else {
    results.keepalive = { ran: false, reason: 'not due yet (< 5 days since last success)' };
  }

  // ── Task 2: status sync (runs every invocation — daily) ──────────────
  try {
    const syncResult = await runStatusSync(env);
    results.statusSync = { ran: true, ok: true, ...syncResult };
    await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:status-sync', {});
  } catch (e) {
    results.statusSync = { ran: true, ok: false, error: String(e) };
    console.error('[stl-dispatcher] status sync failed:', e);
  }

  // ── Task 3: stl-bucket newsletter ingestion (Fridays only) ──────────────
  // Replaces the cron-job.org "stl-bucket refresh" job. Gates on UTC day = 5
  // (Friday) so the daily dispatcher cron only fires the ingestion on the one
  // day the newsletter actually publishes. force=true (manual /trigger) bypasses
  // the day-of-week gate for testing.
  const utcDay = new Date().getUTCDay(); // 0=Sun … 5=Fri … 6=Sat
  const shouldRunBucket = utcDay === 5 || (force && includeBucket);
  if (shouldRunBucket) {
    try {
      if (!env.STL_BUCKET_SECRET) throw new Error('STL_BUCKET_SECRET not bound');
      const res = await fetch('https://stl.stluker.com/refresh', {
        method: 'GET',
        headers: { Authorization: `Bearer ${env.STL_BUCKET_SECRET}` },
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(`stl-bucket returned ${res.status}: ${JSON.stringify(body)}`);
      results.stlBucket = { ran: true, ok: true, ...body };
      await recordHeartbeat(env.STATUS_KV, 'stl-bucket:refresh', { triggeredBy: 'dispatcher' });
    } catch (e) {
      results.stlBucket = { ran: true, ok: false, error: String(e) };
      console.error('[stl-dispatcher] stl-bucket refresh failed:', e);
    }
  } else {
    results.stlBucket = {
      ran: false,
      reason: force
        ? 'force set but includeBucket not requested — skipped to avoid an unscheduled Anthropic API spend'
        : `not Friday (UTC day ${utcDay})`,
    };
  }

  // ── Task 4: space-ingest (daily — runs on every scheduled fire; manual
  // /trigger calls need includeSpace=true, same opt-in shape as includeBucket,
  // since a bare /trigger should stay fast and not silently fan out ~16
  // outbound fetches + a Haiku call) ───────────────────────────────────────
  const shouldRunSpace = !force || includeSpace;
  if (shouldRunSpace) {
    try {
      if (!env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not bound');
      const blob = await runSpaceIngest(env);
      results.spaceIngest = {
        ran: true,
        ok: true,
        sourcesPolled: blob.meta.sourcesPolled,
        sourcesErrored: blob.meta.sourcesErrored,
      };
      await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:space-ingest', {
        breakingCount: blob.breaking.last24h.length,
        launchCount: blob.launches.next7Days.length,
      });
    } catch (e) {
      results.spaceIngest = { ran: true, ok: false, error: String(e) };
      console.error('[stl-dispatcher] space ingest failed:', e);
    }
  } else {
    results.spaceIngest = { ran: false, reason: 'forced run without includeSpace=true' };
  }

  // ── Task 5: earth-ingest (daily — runs on every scheduled fire; manual
  // /trigger calls need includeEarth=true, same opt-in shape as includeSpace,
  // since a bare /trigger should stay fast and not silently fan out several
  // outbound fetches + a Haiku call) ───────────────────────────────────────
  const shouldRunEarth = !force || includeEarth;
  if (shouldRunEarth) {
    try {
      if (!env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not bound');
      if (!env.EARTH_KV) throw new Error('EARTH_KV not bound');
      const blob = await runEarthIngest(env);
      results.earthIngest = {
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
    } catch (e) {
      results.earthIngest = { ran: true, ok: false, error: String(e) };
      console.error('[stl-dispatcher] earth ingest failed:', e);
    }
  } else {
    results.earthIngest = { ran: false, reason: 'forced run without includeEarth=true' };
  }

  // ── Task 6: intel refresh (Mon/Wed/Thu only, per stl-intel/README.md) ───
  // Unlike space/earth, intel's own ingestion logic lives in the separate
  // `intel` Worker — this just calls its /refresh endpoint over HTTP, same
  // pattern as Task 3's stl-bucket call below. force=true (manual /trigger)
  // bypasses the day-of-week gate only when includeIntel=true is also set,
  // matching includeBucket's opt-in shape — a bare /trigger shouldn't
  // silently fire a scrape run outside its scheduled days.
  const shouldRunIntel = [1, 3, 4].includes(utcDay) || (force && includeIntel); // Mon=1, Wed=3, Thu=4
  if (shouldRunIntel) {
    try {
      if (!env.INTEL_SECRET) throw new Error('INTEL_SECRET not bound');
      // A real User-Agent stops AI Labyrinth (link_maze) from treating this
      // legitimate internal call as a bot — same fix already proven for LL2
      // in space-ingest.js. Confirmed via firewall event export (2026-07-29)
      // that this endpoint was getting link_maze_injected with an empty UA.
      const res = await fetch('https://intel.stluker.com/refresh', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${env.INTEL_SECRET}`,
          'User-Agent': 'stl-dispatcher/1.0 (+https://stluker.com; internal service call)',
        },
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok || !body.ok) throw new Error(`intel returned ${res.status}: ${JSON.stringify(body)}`);
      results.intelRefresh = { ran: true, ok: true, storyCount: body.storyCount, diagnostics: body.diagnostics };
      await recordHeartbeat(env.STATUS_KV, 'intel:refresh', {
        storyCount: body.storyCount,
        triggeredBy: 'dispatcher',
      });
    } catch (e) {
      results.intelRefresh = { ran: true, ok: false, error: String(e) };
      console.error('[stl-dispatcher] intel refresh failed:', e);
    }
  } else {
    results.intelRefresh = {
      ran: false,
      reason: force
        ? 'force set but includeIntel not requested — skipped to stay consistent with includeBucket\'s opt-in pattern'
        : `not a scheduled intel day (UTC day ${utcDay}, expects Mon/Wed/Thu)`,
    };
  }

  // ── Task 7: podcastIngest (daily — MUST run after Tasks 4/5, reads what
  // spaceIngest/earthIngest just wrote to SPACE_KV/EARTH_KV in this same run).
  // Same opt-in shape as includeSpace/includeEarth for forced runs, since this
  // is the one task that spends real ElevenLabs credits, not just Anthropic
  // tokens. runPodcastIngest has its own internal same-day idempotency check
  // and monthly credit budget gate — both apply regardless of how it's
  // invoked, so this call site stays a thin pass-through.
  const shouldRunPodcast = !force || includePodcast;
  if (shouldRunPodcast) {
    try {
      if (!env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not bound');
      if (!env.ELEVENLABS_API_KEY) throw new Error('ELEVENLABS_API_KEY not bound');
      if (!env.ELEVENLABS_VOICE_ID) throw new Error('ELEVENLABS_VOICE_ID not bound');
      if (!env.PODCAST_KV) throw new Error('PODCAST_KV not bound');
      if (!env.POD_BUCKET) throw new Error('POD_BUCKET not bound');
      const podcastResult = await runPodcastIngest(env, { force: forcePodcastEpisode });
      results.podcastIngest = podcastResult;
      if (podcastResult.ok) {
        await recordHeartbeat(env.STATUS_KV, 'stl-dispatcher:podcast-ingest', {
          day: podcastResult.day,
          skipped: podcastResult.skipped || null,
          episodeId: podcastResult.episode?.id || null,
          credits: podcastResult.episode?.credits || null,
        });
        // Clear any stale failure record now that a run has actually
        // succeeded -- otherwise a since-fixed problem's error message
        // lingers in /health forever and confuses future debugging.
        try { await env.STATUS_KV.delete('podcast:last-error'); } catch { /* non-fatal */ }
      }
    } catch (e) {
      results.podcastIngest = { ran: true, ok: false, error: String(e) };
      console.error('[stl-dispatcher] podcast ingest failed:', e);
      // Persisted so /health can show WHY the next morning, without needing
      // a live wrangler tail session running at the exact moment of failure.
      // Confirmed necessary Jul 30/31/Aug 1 -- three silent scheduled
      // failures in a row with the actual error never visible after the
      // fact, only recoverable by re-running (successfully) hours later.
      try {
        await env.STATUS_KV.put('podcast:last-error', JSON.stringify({
          error: String(e),
          stack: e?.stack || null,
          day: new Date().toISOString().slice(0, 10),
          timestamp: new Date().toISOString(),
          wasForced: !!forcePodcastEpisode,
        }));
      } catch { /* logging the error must never itself throw */ }
    }
  } else {
    results.podcastIngest = { ran: false, reason: 'forced run without includePodcast=true' };
  }

  // ── Task 8: schools refresh (Mon/Wed/Fri only) ──────────────────────────
  // Unlike space/earth, schools' own ingestion logic lives in the separate
  // `schools` Worker — this just calls its /refresh endpoint over HTTP, same
  // pattern as Task 6's intel call above. force=true (manual /trigger)
  // bypasses the day-of-week gate only when includeSchools=true is also set,
  // matching includeIntel's opt-in shape. Reuses `utcDay`, already computed
  // above for Task 3/6 — do not recompute it, same rule those tasks follow.
  const shouldRunSchools = [1, 3, 5].includes(utcDay) || (force && includeSchools); // Mon=1, Wed=3, Fri=5
  if (shouldRunSchools) {
    try {
      if (!env.SCHOOLS_SECRET) throw new Error('SCHOOLS_SECRET not bound');
      // Same AI Labyrinth / empty-UA fix as intel's Task 6 above — this
      // endpoint showed the identical link_maze_injected pattern in the
      // 2026-07-29 firewall event export.
      const res = await fetch('https://schools.stluker.com/refresh', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${env.SCHOOLS_SECRET}`,
          'User-Agent': 'stl-dispatcher/1.0 (+https://stluker.com; internal service call)',
        },
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok || !body.ok) throw new Error(`schools returned ${res.status}: ${JSON.stringify(body)}`);
      results.schoolsRefresh = {
        ran: true,
        ok: true,
        storyCount: body.storyCount,
        counts: body.counts,
        diagnostics: body.diagnostics,
        sourcesErrored: body.sourcesErrored,
      };
      await recordHeartbeat(env.STATUS_KV, 'schools:refresh', {
        storyCount: body.storyCount,
        triggeredBy: 'dispatcher',
      });
    } catch (e) {
      results.schoolsRefresh = { ran: true, ok: false, error: String(e) };
      console.error('[stl-dispatcher] schools refresh failed:', e);
    }
  } else {
    results.schoolsRefresh = {
      ran: false,
      reason: force
        ? "force set but includeSchools not requested — skipped to stay consistent with includeIntel's opt-in pattern"
        : `not a scheduled schools day (UTC day ${utcDay}, expects Mon/Wed/Fri)`,
    };
  }

  // Task 9 (Daily Creature Clash) removed 2026-07-29 — that Worker now has
  // its own dedicated Cron Trigger and is no longer called from here. See
  // the SCHEDULE and TASKS comments at the top of this file for context.

  return results;
}

// ── Task 1 implementation — ported from supabase-keepalive ──────────────
async function runKeepalive(env) {
  const targets = [
    // NOTE: bare /rest/v1/ (schema root) requires service_role, not anon —
    // confirmed via direct Supabase error on 2026-07-06: "Only the
    // 'service_role' API key can be used for this endpoint." A keepalive
    // ping doesn't need schema access anyway — querying a real table with a
    // tight limit is both anon-safe and a truer test that the DB is alive.
    { name: 'supabase', url: `${env.SUPABASE_URL}/rest/v1/stl_issues?select=id&limit=1`, key: env.SUPABASE_ANON_KEY },
    // fire-api / crash_readings confirmed reachable in the 2026-06-30 manual
    // test — re-enabled 2026-07-14 so keepalive actually covers both targets.
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

// ── Task 2 implementation — reconciliation against live Cloudflare state ─
async function runStatusSync(env) {
  // CHANGED (2026-07-23): base object now comes from STATUS_KV, not GitHub.
  // Root cause of the data.json regression: fetchDataJson() pulls from
  // pdluker/stl-status, which only gets a commit when findings actually
  // change (by design, per the Jul 14 architecture change) — so it had been
  // silently stale since before Jul 14. Every nightly run was rebuilding
  // from that stale GitHub snapshot, re-stamping it with today's date and a
  // bumped version, and overwriting STATUS_KV with it — clobbering every
  // manual KV fix and every new site (space/earth/intel) that was never
  // separately committed to GitHub. KV is what the live site actually reads
  // at request time; it must also be the reconciliation source of truth.
  const { sha } = await fetchDataJson(env); // still needed: GitHub commit below requires the current file SHA
  const stored = await env.STATUS_KV.get('status-data');
  const data = stored ? JSON.parse(stored) : (await fetchDataJson(env)).data;

  const before = JSON.stringify(data.verification?.findings ?? []);
  const reconciled = await reconcile(env, data);
  const after = JSON.stringify(reconciled.verification.findings);

  // Bump meta so the "synced" badge on the dashboard reflects this run,
  // regardless of whether any findings changed.
  reconciled.meta = {
    lastUpdated: new Date().toISOString().slice(0, 10),
    version: bumpPatchVersion(reconciled.meta?.version),
  };

  // CHANGED (2026-07-14): the live site now reads from STATUS_KV at request
  // time (see status Worker's fetch handler), not from the static data.json
  // baked into the deploy. This KV write is what actually reaches site
  // visitors — always do it, independent of whether GitHub gets a commit.
  await env.STATUS_KV.put('status-data', JSON.stringify(reconciled));

  // The GitHub commit below is now an audit trail only — pdluker/stl-status
  // is no longer the deploy source and shouldn't be expected to redeploy the
  // site on every data change. (If Build watch paths for the `status`
  // Worker's Git connection are still set to `*`, narrow them to exclude
  // data.json — e.g. `src/**` — in Cloudflare's dashboard, or every one of
  // these audit commits will keep triggering a needless rebuild.)
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

// ── Helpers ────────────────────────────────────────────────────────────
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
  });
}
