/**
 * stl-now-calendar.js  --  lives in C:\Users\pdluk\stl-dispatcher\
 *
 * Separate from stl-now-ingest.js on purpose: this needs to run EARLIER in the
 * evening (events are forward-scheduled, so there's no urgency, but the main
 * ingest task at the end of the run needs this already sitting in KV, not
 * fetched live at 0500-adjacent time). Two small tasks with a clean handoff
 * through STLNOW_KV are easier to reason about than one task doing both --
 * same principle as space/earth writing their own KV blobs independently
 * before podcastIngest reads them.
 *
 * STUB: real event-source fetches (Explore STL, venue calendars, Eventbrite)
 * are not implemented here yet. Each one, when added, must be confirmed
 * against the organizer's OWN page before being written -- that confirmation
 * step is what makes this segment cheap and reliable; skipping it just to
 * save a fetch defeats the reason this segment exists.
 */

export async function runStlNowCalendarRefresh(env) {
  const diagnostics = [];
  try {
    if (!env.STLNOW_KV) throw new Error('missing binding: STLNOW_KV');

    // TODO: replace with real fetch + per-organizer confirmation.
    const events = await fetchAndVerifyEvents(env, diagnostics);

    await env.STLNOW_KV.put('calendar:pending', JSON.stringify(events));
    diagnostics.push({ step: 'calendar', ok: true, count: events.length });
    return { ok: true, count: events.length, diagnostics };
  } catch (err) {
    diagnostics.push({ step: 'error', ok: false, error: err.message });
    return { ok: false, error: err.message, diagnostics };
  }
}

async function fetchAndVerifyEvents(env, diagnostics) {
  // Stub -- intentionally returns nothing until real sources are wired in.
  // A short, high-confidence list (a handful of picks) beats an exhaustive,
  // unverified dump -- keep it that way even once this is real.
  diagnostics.push({ step: 'events', ok: true, note: 'stub -- no live sources wired yet' });
  return [];
}
