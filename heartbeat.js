// heartbeat.js — outbound liveness tracking for scheduled Workers
// Closes the gap that let keepalive fail silently for 26 runs: nothing on YOUR
// side noticed missing pings. Each successful run stamps a timestamp into KV;
// stl-status-sync later reads these and surfaces any that are stale.
//
// Requires a KV namespace binding named STATUS_KV (add in wrangler.jsonc).
// Keys are namespaced: heartbeat:<job-name>
//
// In the scheduled Worker (e.g. supabase-keepalive), after work succeeds:
//   import { recordHeartbeat } from "./heartbeat.js";
//   await recordHeartbeat(env.STATUS_KV, "supabase-keepalive", { ok: true, note });

/**
 * Record a successful run. Store last-success time + optional metadata.
 * @param {KVNamespace} kv        STATUS_KV binding
 * @param {string} job            stable job name, e.g. "supabase-keepalive"
 * @param {object} [meta]         arbitrary small JSON (status, rows touched, etc.)
 */
export async function recordHeartbeat(kv, job, meta = {}) {
  const payload = {
    job,
    lastSuccess: new Date().toISOString(),
    meta,
  };
  // 30-day TTL so dead jobs eventually disappear from KV rather than lingering.
  await kv.put(`heartbeat:${job}`, JSON.stringify(payload), {
    expirationTtl: 60 * 60 * 24 * 30,
  });
}

/**
 * Read one heartbeat. Returns null if never recorded (or expired = long dead).
 * @param {KVNamespace} kv
 * @param {string} job
 * @returns {Promise<{job:string,lastSuccess:string,meta:object}|null>}
 */
export async function readHeartbeat(kv, job) {
  const raw = await kv.get(`heartbeat:${job}`);
  return raw ? JSON.parse(raw) : null;
}

/**
 * Evaluate staleness against an expected max interval.
 * @param {object|null} hb         result of readHeartbeat
 * @param {number} maxAgeHours     how long since last success is "too long"
 * @returns {{stale:boolean, ageHours:number|null, lastSuccess:string|null}}
 */
export function evaluateHeartbeat(hb, maxAgeHours) {
  if (!hb) return { stale: true, ageHours: null, lastSuccess: null };
  const ageMs = Date.now() - new Date(hb.lastSuccess).getTime();
  const ageHours = ageMs / (1000 * 60 * 60);
  return { stale: ageHours > maxAgeHours, ageHours, lastSuccess: hb.lastSuccess };
}
