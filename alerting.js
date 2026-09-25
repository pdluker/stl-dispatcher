// alerting.js — push notification layer for stl-dispatcher reconciliation findings
//
// WHY THIS EXISTS
// ---------------
// reconcile() already produces good findings (cron_drift, phantom_worker,
// untracked_worker, heartbeat_missing, heartbeat_stale) and writes them to
// data.autoRecommendations on status.stluker.com. That is a PULL surface —
// someone has to go look at it. Every multi-day outage in this project's
// history (stl-music 9d, schools 10d, Task 10 gone 7d, stl-sports serving
// seed data, pod audio failures Aug 1-2 and Aug 6) was ultimately detected by
// a human noticing a page looked wrong, not by any check firing.
//
// This module closes that loop: critical findings become an email, once,
// with a cooldown so a persistent failure doesn't mail you every single day.
//
// DEPENDENCIES
//   env.RESEND_API_KEY   — new secret, create at resend.com (domain already verified)
//   env.ALERT_TO         — new secret or plain var, e.g. "pdluker@gmail.com"
//   env.STATUS_KV        — already bound; used for cooldown state only
//
// WIRING (see dispatcher-patches doc for exact placement)
//   import { maybeAlert } from './alerting.js';
//   ... inside runKeepaliveAndSync(), after runStatusSync():
//   await maybeAlert(env, findings);

const ALERT_FROM = 'stl-dispatcher <alerts@stluker.com>';

// Fallback recipient. env.ALERT_TO still wins when set — this exists so that a
// missing or fat-fingered binding degrades to a working address rather than to
// silence. A monitoring system that stops mailing because a var went missing
// reproduces the exact failure mode it was built to eliminate.
const DEFAULT_ALERT_TO = 'pdluker@gmail.com';

// How long a given finding stays "already reported" before it can mail again.
// 20h (not 24h) so a daily 11:00 UTC run is never skipped by clock jitter.
const COOLDOWN_HOURS = 20;

// Findings at or above this severity page you. 'info' (untracked_worker) does
// not — that's inventory drift, it belongs on the dashboard, not in your inbox.
const ALERT_SEVERITIES = new Set(['critical', 'warning']);

/**
 * Stable identity for a finding, so the same ongoing failure is recognized
 * across runs. Deliberately excludes ageHours/lastSuccess — those change every
 * run and would defeat the cooldown entirely.
 */
function fingerprint(f) {
  return [f.type, f.job || f.worker || '-', f.expected || ''].join('|');
}

async function getCooldownState(env) {
  const raw = await env.STATUS_KV.get('alert:cooldown');
  return raw ? JSON.parse(raw) : {};
}

async function putCooldownState(env, state) {
  await env.STATUS_KV.put('alert:cooldown', JSON.stringify(state), {
    expirationTtl: 60 * 60 * 24 * 30,
  });
}

function renderEmail(newFindings, allFindings) {
  const crit = newFindings.filter((f) => f.severity === 'critical');
  const warn = newFindings.filter((f) => f.severity === 'warning');

  const line = (f) => `  - [${f.severity.toUpperCase()}] ${f.message}`;
  const body = [
    `stl-dispatcher reconciliation found ${newFindings.length} new issue(s).`,
    '',
    crit.length ? `CRITICAL (${crit.length}) — a pipeline is dead or stale:` : null,
    crit.length ? crit.map(line).join('\n') : null,
    crit.length ? '' : null,
    warn.length ? `WARNING (${warn.length}) — drift between declared and live:` : null,
    warn.length ? warn.map(line).join('\n') : null,
    warn.length ? '' : null,
    `Total open findings this run (including previously-reported): ${allFindings.length}`,
    '',
    'Dashboard: https://status.stluker.com',
    `Reported at ${new Date().toISOString()}. Each issue is reported at most`,
    `once per ${COOLDOWN_HOURS}h; it will re-alert if it is still failing after that.`,
  ]
    .filter((l) => l !== null)
    .join('\n');

  const subject = crit.length
    ? `[stluker] ${crit.length} CRITICAL: ${crit[0].job || crit[0].worker || 'pipeline failure'}`
    : `[stluker] ${warn.length} warning(s) from reconciliation`;

  return { subject, body };
}

/**
 * Send an alert email for any newly-appeared critical/warning finding.
 * Returns a small result object; never throws — alerting must not be able to
 * break the reconciliation run that produced the findings.
 */
export async function maybeAlert(env, findings) {
  try {
    const to = env.ALERT_TO || DEFAULT_ALERT_TO;
    if (!env.RESEND_API_KEY) {
      // Deliberately loud: this is the one condition under which the entire
      // alerting layer is a no-op, and it must not be discoverable only by
      // noticing that no mail ever arrives.
      console.error('[alerting] RESEND_API_KEY not set — alerting is DISABLED');
      return { sent: false, reason: 'RESEND_API_KEY not set' };
    }

    const alertable = (findings || []).filter((f) => ALERT_SEVERITIES.has(f.severity));
    if (alertable.length === 0) {
      // Nothing wrong: clear cooldown so a recurrence tomorrow alerts immediately
      // rather than being suppressed by a stale entry.
      await putCooldownState(env, {});
      return { sent: false, reason: 'no alertable findings' };
    }

    const state = await getCooldownState(env);
    const now = Date.now();
    const cutoff = now - COOLDOWN_HOURS * 3600 * 1000;

    const fresh = alertable.filter((f) => {
      const seen = state[fingerprint(f)];
      return !seen || seen < cutoff;
    });

    if (fresh.length === 0) {
      return { sent: false, reason: 'all findings within cooldown', suppressed: alertable.length };
    }

    const { subject, body } = renderEmail(fresh, alertable);

    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.RESEND_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        from: ALERT_FROM,
        to: [to],
        subject,
        text: body,
      }),
    });

    if (!res.ok) {
      const detail = await res.text().catch(() => '');
      console.error(`[alerting] Resend ${res.status}: ${detail.slice(0, 300)}`);
      // Deliberately do NOT record cooldown on failure — retry next run.
      return { sent: false, reason: `resend ${res.status}` };
    }

    // Only record cooldown for what actually went out.
    const nextState = { ...state };
    for (const f of fresh) nextState[fingerprint(f)] = now;
    // Drop entries older than the cooldown so this key can't grow forever.
    for (const [k, v] of Object.entries(nextState)) {
      if (v < cutoff - 7 * 24 * 3600 * 1000) delete nextState[k];
    }
    await putCooldownState(env, nextState);

    return { sent: true, count: fresh.length, suppressed: alertable.length - fresh.length };
  } catch (e) {
    console.error(`[alerting] failed: ${String(e)}`);
    return { sent: false, reason: String(e) };
  }
}
