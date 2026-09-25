// reconcile.js — verification layer, now running inside stl-dispatcher
// Turns the status board from "reports what a human typed" into "reports what
// Cloudflare actually shows." Three checks, each producing structured findings
// that get merged into data.json so the board self-corrects.
//
// Needs (bound to stl-dispatcher):
//   CF_API_TOKEN   — scoped token: Account > Workers Scripts:Read, Workers Cron:Read
//   CF_ACCOUNT_ID
//   GITHUB_TOKEN   — fine-grained PAT, Contents:Read/Write on pdluker/stl-status only
//   STATUS_KV      — same namespace heartbeats already use
//
// CF API refs:
//   list workers:  GET /accounts/{acct}/workers/scripts
//   cron triggers: GET /accounts/{acct}/workers/scripts/{name}/schedules

const CF_API = "https://api.cloudflare.com/client/v4";

async function cf(env, path) {
  const res = await fetch(`${CF_API}${path}`, {
    headers: { Authorization: `Bearer ${env.CF_API_TOKEN}` },
  });
  if (!res.ok) throw new Error(`CF API ${res.status} on ${path}`);
  const body = await res.json();
  if (!body.success) throw new Error(`CF API error on ${path}: ${JSON.stringify(body.errors)}`);
  return body.result;
}

/**
 * CHECK 1 — Cron drift.
 * Diff the cron strings declared in data.json against what's actually deployed.
 * Catches the wrangler.jsonc-vs-dashboard drift automatically.
 *
 * @param {object} env
 * @param {Array<{worker:string, expected:string}>} declared
 *        e.g. [{worker:"supabase-keepalive", expected:"0 0 (every-5-days) * *"}]
 * @returns {Promise<Array<object>>} findings
 */
export async function checkCronDrift(env, declared) {
  const findings = [];
  for (const { worker, expected } of declared) {
    try {
      const schedules = await cf(
        env,
        `/accounts/${env.CF_ACCOUNT_ID}/workers/scripts/${worker}/schedules`
      );
      const live = (schedules?.schedules || []).map((s) => s.cron);
      const normalizedExpected = expected.trim();
      const hit = live.some((c) => c.trim() === normalizedExpected);
      if (!hit) {
        findings.push({
          type: "cron_drift",
          severity: "warning",
          worker,
          expected: normalizedExpected,
          live,
          message: `Deployed cron for ${worker} (${live.join(", ") || "none"}) does not match declared "${normalizedExpected}".`,
        });
      }
    } catch (e) {
      findings.push({
        type: "cron_check_failed",
        severity: "info",
        worker,
        message: `Could not verify cron for ${worker}: ${String(e)}`,
      });
    }
  }
  return findings;
}

/**
 * CHECK 2 — Orphaned recommendations / phantom workers.
 * Any Worker referenced in data.json that no longer exists in the account is a
 * stale reference (this is exactly how surfflow-scoring lingered). Conversely,
 * a deployed Worker absent from data.json is untracked drift.
 *
 * @param {object} env
 * @param {string[]} trackedWorkers  worker names data.json believes exist
 * @returns {Promise<Array<object>>}
 */
export async function checkWorkerInventory(env, trackedWorkers) {
  const scripts = await cf(env, `/accounts/${env.CF_ACCOUNT_ID}/workers/scripts`);
  const liveNames = new Set((scripts || []).map((s) => s.id));
  const findings = [];

  for (const w of trackedWorkers) {
    if (!liveNames.has(w)) {
      findings.push({
        type: "phantom_worker",
        severity: "warning",
        worker: w,
        message: `data.json references Worker "${w}" but it no longer exists in Cloudflare. Remove stale references/recommendations.`,
      });
    }
  }
  for (const name of liveNames) {
    if (!trackedWorkers.includes(name)) {
      findings.push({
        type: "untracked_worker",
        severity: "info",
        worker: name,
        message: `Deployed Worker "${name}" is not tracked in data.json.`,
      });
    }
  }
  return findings;
}

/**
 * CHECK 3 — Heartbeat staleness.
 * Read heartbeat:<job> keys and flag any that haven't succeeded within their
 * expected window. This is what would have caught the keepalive failure on day
 * one instead of day four.
 *
 * @param {object} env
 * @param {Array<{job:string, maxAgeHours:number}>} jobs
 * @returns {Promise<Array<object>>}
 */
export async function checkHeartbeats(env, jobs) {
  const findings = [];
  for (const { job, maxAgeHours } of jobs) {
    const raw = await env.STATUS_KV.get(`heartbeat:${job}`);
    if (!raw) {
      findings.push({
        type: "heartbeat_missing",
        severity: "critical",
        job,
        message: `No successful run ever recorded for ${job} (or last success aged out of KV). Job may be dead.`,
      });
      continue;
    }
    const hb = JSON.parse(raw);
    const ageHours = (Date.now() - new Date(hb.lastSuccess).getTime()) / 3.6e6;
    if (ageHours > maxAgeHours) {
      findings.push({
        type: "heartbeat_stale",
        severity: "critical",
        job,
        lastSuccess: hb.lastSuccess,
        ageHours: Math.round(ageHours),
        message: `${job} last succeeded ${Math.round(ageHours)}h ago (expected within ${maxAgeHours}h).`,
      });
    }
  }
  return findings;
}

/**
 * Orchestrator — run all checks, fold findings into the data.json object under
 * a `verification` block plus synthesized smart recommendations. Returns the
 * mutated object for stl-status-sync to persist.
 */
export async function reconcile(env, data) {
  const declaredCrons = [
    { worker: "stluker", expected: "0 9 " + "*/3 * *" },
    // "stl-music" native cron entry REMOVED 2026-09-08: it was retired
    // 2026-07-30 when music/sports pulse folded into stl-dispatcher's
    // Sunday task ("35 11 * * SUN" below). This entry had no matching live
    // cron since that date, so checkCronDrift has been emitting a phantom
    // cron_drift warning on every single run for over a month. A findings
    // list with a permanent false positive in it is a findings list that
    // gets ignored -- removing the check, not adding a fix, since there is
    // nothing left to check.
    // REPLACED 2026-08-23: the Aug 22 schools-outage investigation resulted
    // in splitting EVERY day-gated task in dispatcher.js onto its own cron +
    // its own dedicated runXOnly() function, the same pattern the podcast
    // fix used back on Aug 6 (own invocation = own fresh subrequest budget,
    // and a task that fails is independently visible instead of hiding
    // inside one shared runAll()). stl-dispatcher now has EIGHT crons, not
    // two -- this is the exact list from wrangler.jsonc as of that change.
    // Two entries below (bucket, schools) were previously modeled as
    // separate Workers' own triggers or as gates inside runAll() -- they are
    // now stl-dispatcher crons instead, so the old standalone "stl-bucket"
    // entry that used to sit below this block has been folded in here.
    { worker: "stl-dispatcher", expected: "0 11 * * *" },            // keepalive + statusSync
    { worker: "stl-dispatcher", expected: "5 11 * * FRI" },          // stlBucket
    { worker: "stl-dispatcher", expected: "10 11 * * *" },           // spaceIngest
    { worker: "stl-dispatcher", expected: "15 11 * * *" },           // earthIngest
    { worker: "stl-dispatcher", expected: "20 11 * * MON,WED,THU" }, // intelRefresh
    { worker: "stl-dispatcher", expected: "25 11 * * MON,WED,FRI" }, // schoolsRefresh
    { worker: "stl-dispatcher", expected: "30 11 * * *" },           // podcastIngest
    { worker: "stl-dispatcher", expected: "35 11 * * SUN" },         // music + sports pulse
    // ADDED 2026-09-08: stl-weekly was the one dispatcher task with no
    // drift check at all -- confirmed via a live pull of the deployed
    // dispatcher.js that the cron and runWeeklyOnly() wiring are real and
    // correct, but nothing here would have caught it if that cron were
    // ever accidentally changed or dropped. Closing that gap now that the
    // automated path itself has been confirmed working end-to-end via a
    // real /trigger?includeWeekly=true call (episodeId 2026-09-08).
    { worker: "stl-dispatcher", expected: "40 11 * * MON" },         // stl-weekly refresh
    // stl-sports intentionally omitted until its expression is confirmed (T-10)
    //
    // ADDED 2026-09-01: rails-beneath-us runs its OWN native cron, confirmed
    // directly from its wrangler.jsonc — not folded into stl-dispatcher
    // (unlike everything else in this list). If it's ever consolidated,
    // remove this entry and add its task to the stl-dispatcher block above.
    { worker: "rails-beneath-us", expected: "0 11 * * 1,3,5" }, // Mon/Wed/Fri 06:00 CT
    { worker: "innovation-daily", expected: "0 11 * * 2,4" },   // Tue/Thu 06:00 CT
    { worker: "civicsignal", expected: "0 11 * * MON-FRI" },    // weekday mornings, ~06:00 CT
    { worker: "bigbuilds", expected: "0 13 */2 * *" },          // every other day, ~08:00 CT
  ];
  const trackedWorkers = [
    "stluker", "family", "stl-sports", "stl-music", "stl-bucket",
    "fire-api", "reunion", "pokelab", "ironrails",
    // "memory-lattice" REMOVED 2026-09-08: a live checkWorkerInventory run
    // flagged it as phantom_worker (the Worker no longer exists in
    // Cloudflare, only its R2 bucket memory-lattice-photos remains). Removing
    // the reference here is the fix that finding calls for; the orphaned R2
    // bucket is a separate cleanup, not something this check can act on.
    "stl-dispatcher", "space", "earth", "intel",
    // supabase-keepalive and stl-status-sync fully decommissioned 2026-07-xx —
    // removed from this list, so checkWorkerInventory will now correctly flag
    // them under untracked_worker if either ever reappears.
    // "space" added 2026-07-21 — the space.stluker.com Worker.
    // "earth"/"intel" added 2026-07-23 — both went live that day but this
    // list lagged them, meaning checkWorkerInventory was silently flagging
    // both as untracked_worker instead of recognizing them as expected.
    //
    // ADDED 2026-09-01: this list had silently lagged live deploys for
    // six-plus weeks (root cause of the Sep 1 status.stluker.com staleness
    // incident) — every name below came straight off a live untracked_worker
    // finding, not a guess. "status" itself is included since the status
    // Worker was never in this list despite existing since Jun 18.
    "status",           // status.stluker.com — the Status Board Worker itself
    "retire",           // retire.stluker.com — confirmed a real Worker Jul 16
    "pod",              // pod.stluker.com — daily podcast pipeline, stl-dispatcher Task 7
    "podcast",          // distinct name from "pod" per a live untracked_worker finding — confirm at next code touch whether this is a leftover/duplicate deploy or a genuinely separate Worker before removing either
    "schools",          // schools.stluker.com — stl-dispatcher Task 8
    "stl-weekly",       // weekly.stluker.com — stl-dispatcher's Monday task.
                        // WEEKLY_SECRET confirmed set on both stl-dispatcher
                        // and stl-weekly as of 2026-09-08, and the full
                        // automated path (dispatcher /trigger -> stl-weekly
                        // /refresh) was confirmed working end-to-end the
                        // same day (episodeId 2026-09-08). Open task closed.
    "rails-beneath-us", // distinct from "ironrails" (trains.stluker.com) — confirm at next code touch whether this is a duplicate/stale deploy or a real separate project
    "civicsignal",      // untracked, purpose/status unconfirmed — verify before treating as permanently expected
    "innovation-daily", // untracked, purpose/status unconfirmed — verify before treating as permanently expected
    "mech-match",       // untracked, purpose/status unconfirmed — verify before treating as permanently expected
    "bigbuilds",        // untracked, purpose/status unconfirmed — verify before treating as permanently expected
  ];
  const heartbeatJobs = [
    { job: "stl-dispatcher:keepalive", maxAgeHours: 24 * 6 }, // every 5 days + grace
    { job: "stl-dispatcher:status-sync", maxAgeHours: 30 },   // daily + grace
    { job: "stl-bucket:refresh", maxAgeHours: 24 * 8 },       // weekly (Friday) + 1-day grace
    // ADDED 2026-08-12: the three daily ingests were never monitored here.
    // /health already evaluates all three with these same windows, but /health
    // is a pull -- someone has to look. These entries make a stale ingest
    // surface as a CRITICAL autoRecommendation on the status board instead.
    // podcast-ingest matters most: it's the only user-facing daily deliverable,
    // it now runs on its own separate cron, and both times it broke (Aug 1-2,
    // Aug 6) it was noticed by a human missing the episode, not by any check.
    { job: "stl-dispatcher:podcast-ingest", maxAgeHours: 30 }, // daily; grace for TTS/upload
    { job: "stl-dispatcher:space-ingest", maxAgeHours: 30 },   // daily + grace
    { job: "stl-dispatcher:earth-ingest", maxAgeHours: 30 },   // daily + grace
    // ADDED 2026-08-23: four more confirmed via a literal grep of every
    // recordHeartbeat() call in dispatcher.js (not assumed by naming
    // convention -- two of these don't follow the "stl-dispatcher:" prefix
    // the others use, which is exactly why this was checked directly rather
    // than guessed). schools:refresh is the specific job that was missing
    // when schools.stluker.com went 10 days without updating and nothing
    // paged anyone -- that gap is what started the Aug 22 investigation that
    // led to every task getting its own cron in the first place. Closing it
    // here is the actual fix for that root cause, not just documentation of
    // it.
    { job: "intel:refresh", maxAgeHours: 24 * 5 },   // Mon/Wed/Thu; longest real gap is Thu->Mon (96h) + grace
    { job: "schools:refresh", maxAgeHours: 24 * 4 }, // Mon/Wed/Fri; longest real gap is Fri->Mon (72h) + grace
    { job: "stl-music:pulse", maxAgeHours: 24 * 8 },  // weekly (Sunday) + 1-day grace, same pattern as stl-bucket
    { job: "stl-sports:pulse", maxAgeHours: 24 * 8 }, // weekly (Sunday) + 1-day grace, same pattern as stl-bucket
    // ADDED 2026-09-08: runWeeklyOnly() in dispatcher.js has recorded a
    // stl-weekly:refresh heartbeat since stl-weekly's cron was added, but
    // this entry was never added here -- the identical omission shape that
    // let schools:refresh go unmonitored for 10 days in August. Confirmed
    // via a real /trigger?includeWeekly=true call the same day (episodeId
    // 2026-09-08) that the job name and recordHeartbeat wiring are exactly
    // this string. Monday-only cron, so the longest legitimate gap between
    // successes is 168h; 8 days matches the grace window already used for
    // every other weekly job in this list (stl-bucket, stl-music, stl-sports).
    { job: "stl-weekly:refresh", maxAgeHours: 24 * 8 },
  ];

  const findings = [
    ...(await safe(() => checkCronDrift(env, declaredCrons))),
    ...(await safe(() => checkWorkerInventory(env, trackedWorkers))),
    ...(await safe(() => checkHeartbeats(env, heartbeatJobs))),
  ];

  data.verification = {
    ranAt: new Date().toISOString(),
    findingsCount: findings.length,
    findings,
  };

  // Surface criticals/warnings as smart recommendations so they show on the board.
  const auto = findings
    .filter((f) => f.severity === "critical" || f.severity === "warning")
    .map((f) => ({
      icon: f.severity === "critical" ? "🔴" : "🟠",
      title: f.message,
      detail: `Auto-detected by stl-status-sync reconciliation (${f.type}).`,
      cat: f.severity === "critical" ? "CRITICAL" : "DRIFT",
    }));
  data.autoRecommendations = auto;

  return data;
}

async function safe(fn) {
  try {
    return await fn();
  } catch (e) {
    return [{ type: "check_error", severity: "info", message: String(e) }];
  }
}
