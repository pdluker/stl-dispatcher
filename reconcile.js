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
    { worker: "stl-music", expected: "0 10 * * 1" },
    { worker: "stl-dispatcher", expected: "0 11 * * *" },
    { worker: "stl-bucket", expected: "0 14 * * 5" },
    // stl-sports intentionally omitted until its expression is confirmed (T-10)
  ];
  const trackedWorkers = [
    "stluker", "family", "stl-sports", "stl-music", "stl-bucket",
    "fire-api", "memory-lattice", "reunion", "pokelab", "ironrails",
    "stl-dispatcher", "space", "earth", "intel",
    // supabase-keepalive and stl-status-sync fully decommissioned 2026-07-xx —
    // removed from this list, so checkWorkerInventory will now correctly flag
    // them under untracked_worker if either ever reappears.
    // "space" added 2026-07-21 — the space.stluker.com Worker.
    // "earth"/"intel" added 2026-07-23 — both went live that day but this
    // list lagged them, meaning checkWorkerInventory was silently flagging
    // both as untracked_worker instead of recognizing them as expected.
  ];
  const heartbeatJobs = [
    { job: "stl-dispatcher:keepalive", maxAgeHours: 24 * 6 }, // every 5 days + grace
    { job: "stl-dispatcher:status-sync", maxAgeHours: 30 },   // daily + grace
    { job: "stl-bucket:refresh", maxAgeHours: 24 * 8 },       // weekly (Friday) + 1-day grace
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
