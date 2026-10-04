// trackers-checks.js — monitoring + signal alerts for trackers.stluker.com and insights.stluker.com
// ADDED 2026-10-04.
//
// Both Workers run their OWN crons and write heartbeats to TRACKERS_KV (not STATUS_KV), so
// checkHeartbeats() can't see them. Like checkStickersHealth(), these checks read each Worker's
// public JSON and decide freshness here. No secrets needed: every endpoint used is public.
//
//   checkTrackersHealth  -> critical when a tracker's pipeline is stale or unreachable,
//                           critical/warning when the insights jobs stop running.
//   checkInsightsSignals -> warning-level SIGNAL findings (signal: true) that alerting.js mails
//                           under their own heading: river freight risk Elevated/High, and
//                           TVA/MISO at >= 95% of (or above) the monthly demand record.
//
// Fingerprints (alerting.js: type|job|expected) include the band / month, so an escalation
// (elevated -> high, 95% -> new record, a new month) mails again even inside the 20h cooldown.

const UA = "stl-dispatcher/1.0 (+https://stluker.com; internal service call)";
const TRACKERS = ["river", "grid", "fires"];
const TRACKERS_BASE = "https://trackers.stluker.com";
const INSIGHTS_BASE = "https://insights.stluker.com";

// insights runs every 3 h (40 */3 * * *); the full daily refresh runs at 12:40 UTC.
const INSIGHTS_RUN_MAX_H = 7;     // two missed 3-hourly runs + grace
const INSIGHTS_DAILY_MAX_H = 30;  // one missed daily run + grace
const GRID_NEAR_RECORD_PCT = 95;

async function getJson(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "application/json", "Cache-Control": "no-cache" },
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function checkTrackersHealth() {
  const findings = [];
  for (const t of TRACKERS) {
    const job = `trackers:${t}`;
    let snap;
    try {
      snap = await getJson(`${TRACKERS_BASE}/api/${t}/latest.json`);
    } catch (e) {
      findings.push({ type: "tracker_unreachable", severity: "critical", job, message: `trackers.stluker.com/api/${t}/latest.json failed: ${String(e.message || e)}.` });
      continue;
    }
    if (snap.pipelineStale) {
      findings.push({ type: "heartbeat_stale", severity: "critical", job, lastSuccess: snap.verifiedAt || null, message: `The ${t} tracker's pipeline has stopped (last verified ${snap.verifiedAt || "unknown"}); the page is showing old data.` });
    } else if (snap.stale) {
      findings.push({ type: "tracker_source_stale", severity: "warning", job, message: `The ${t} tracker is running but its upstream data is stale (as of ${snap.asOf || "unknown"}).` });
    }
  }

  const job = "insights:run";
  let h;
  try {
    h = await getJson(`${INSIGHTS_BASE}/api/insights/health`);
  } catch (e) {
    findings.push({ type: "tracker_unreachable", severity: "critical", job, message: `insights.stluker.com/api/insights/health failed: ${String(e.message || e)}.` });
    return findings;
  }
  if (!h.lastRun || h.lastRun.ageHours > INSIGHTS_RUN_MAX_H) {
    findings.push({ type: "heartbeat_stale", severity: "critical", job, lastSuccess: h.lastRun?.at || null, message: `insights has not run for ${h.lastRun ? Math.round(h.lastRun.ageHours) + "h" : "ever"} (expected every 3h).` });
  }
  if (!h.lastDaily || h.lastDaily.ageHours > INSIGHTS_DAILY_MAX_H) {
    findings.push({ type: "heartbeat_stale", severity: "critical", job: "insights:daily", lastSuccess: h.lastDaily?.at || null, message: `insights' daily refresh (12:40 UTC) last ran ${h.lastDaily ? Math.round(h.lastDaily.ageHours) + "h ago" : "never"}.` });
  } else if (h.lastDaily.failed?.length) {
    findings.push({ type: "heartbeat_degraded", severity: "warning", job: "insights:daily", expected: h.lastDaily.failed.join(","), message: `insights' daily refresh ran but these jobs failed: ${h.lastDaily.failed.join(", ")}.` });
  }
  return findings;
}

export async function checkInsightsSignals() {
  let s;
  try {
    s = await getJson(`${INSIGHTS_BASE}/api/insights/summary`);
  } catch (e) {
    return [{ type: "check_error", severity: "info", message: `insights summary unavailable for signal checks: ${String(e.message || e)}` }];
  }
  const findings = [];
  const risk = s.river?.risk;
  if (risk && (risk.band === "elevated" || risk.band === "high")) {
    findings.push({
      type: "signal_river_risk", severity: "warning", signal: true, job: "insights:river-risk", expected: risk.band,
      message: `River freight risk is ${risk.band.toUpperCase()} (${risk.index}/100). St. Louis ${s.river.stageFt != null ? s.river.stageFt.toFixed(1) + " ft" : "stage n/a"}; barges $${s.river.bargeUsd ?? "n/a"}/ton (${s.river.bargeVsNormal ?? "n/a"}x normal). ${INSIGHTS_BASE}/#river`,
    });
  }
  const ym = new Date().toISOString().slice(0, 7);
  for (const m of s.grid?.month || []) {
    if (m.pctOfRecord == null || m.pctOfRecord < GRID_NEAR_RECORD_PCT) continue;
    const record = m.pctOfRecord >= 100;
    findings.push({
      type: record ? "signal_grid_record" : "signal_grid_near_record", severity: "warning", signal: true,
      job: `insights:grid-${m.region}`, expected: `${ym}:${record ? "record" : "95"}`,
      message: record
        ? `${m.region} set a new monthly demand record: ${(m.peakMw / 1000).toFixed(1)} GW (${m.at || "time n/a"} UTC). ${INSIGHTS_BASE}/#grid`
        : `${m.region} demand reached ${m.pctOfRecord}% of its monthly record (${(m.peakMw / 1000).toFixed(1)} GW at ${m.at || "n/a"} UTC). ${INSIGHTS_BASE}/#grid`,
    });
  }
  return findings;
}
