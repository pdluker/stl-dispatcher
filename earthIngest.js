// earthIngest.js — daily ingestion task for earth.stluker.com
//
// Designed to be called from stl-dispatcher's existing daily cron
// (0 11 * * *), alongside the other tasks (keepalive, statusSync,
// stlBucket). Does NOT need its own cron slot.
//
// Subrequest budget for this task's own fetches (CF free-tier cap is 50
// per invocation, shared across the whole dispatcher run):
//   1. USGS significant_day.geojson        — required, never skipped
//   2. USGS FDSN NMSZ bbox query (24h)      — required, never skipped
//   3. USGS FDSN Yellowstone bbox query (24h) — required, never skipped
//      (2026-07-23: added as a second fixed-region watch strip, same
//      pattern as NMSZ, independent failure)
//   4. NASA EONET events (status=open only) — best-effort
//      (2026-08-26: dropped the days=2 recency filter that used to be on
//      this query. That parameter was silently excluding wildfires whose
//      most recent geometry update was more than 2 days old, even when
//      EONET still marked them "open" (closed: null) - i.e. still an
//      active, ongoing incident. Concrete case: the Hawk Fire near Reno,
//      NV (EONET_23218) sat unlisted on the dashboard for days during an
//      active evacuation, purely because its last geometry ping was
//      >2 days old by the time anyone re-checked, not because it had
//      closed or because EONET didn't have it. Category matching itself
//      (parseEonetEvents below, comparing against string ids like
//      "wildfires") was already correct for the v3 API and was never the
//      problem - the recency window was. status=open plus the
//      EONET_FETCH_LIMIT/EONET_DISPLAY_LIMIT/EONET_WHY_PER_CATEGORY caps
//      below now bound the result set the way days=2 used to, without
//      silently dropping events that are still genuinely open.)
//   5. GVP weekly RSS                       — best-effort, and only
//      fetched at all if the cached copy is >6 days old (it's a
//      weekly report; re-fetching daily wastes a subrequest for
//      content that hasn't changed)
//   6-10. NEWS_FEEDS (ocean/atmosphere/geography RSS, 2026-07-30, URLs
//      verified live) — best-effort, same fetchFeed()/degrade-to-empty
//      pattern as space-ingest.js's RSS layer, ported here rather than
//      reinvented. 5 feeds, fetched concurrently via Promise.all (not
//      sequentially) so they add latency, not much subrequest serialization
//      time, to the run. One failed feed does not affect the others.
// Worst case: 5 + 5 = 10 subrequests, well under the 50/invocation cap
// shared across the whole dispatcher run. The three USGS "required"
// sources are fetched first and are the ones this module treats as
// must-succeed; EONET, GVP, and every NEWS_FEEDS entry degrade to "carry
// forward previous data" / "empty" on failure rather than fail the whole
// run.
//
// One additional call (NOT a CF subrequest, but a real Anthropic API
// call): a single batched Haiku call that returns BOTH a daily plain-
// language briefing AND per-item "why it matters" text, system prompt
// cache_control: ephemeral, capped output tokens. Unlike the old
// design, the briefing runs every day regardless of whether anything
// significant happened (2026-07-23) — "today was quiet" is itself
// useful information, matching space-ingest's accepted one-call/day
// cost model. As of 2026-08-04, whyItMatters items also include up to
// 3 top news stories per category (ocean/atmosphere/geography, ~9 max)
// alongside quakes/wildfires, so news stories on the frontend get a
// genuine "so what" sentence instead of only a raw RSS excerpt.
//
// Also computes the Earth Pulse Score (2026-07-23) — a same-day
// activity-level snapshot built ONLY from categories this pipeline
// actually ingests. Deliberately does NOT include solar activity,
// meteor impacts, or population/economic impact modeling (all
// suggested in an external product review) since we have no real data
// source for any of those — a score partly built on fabricated inputs
// would be the same "analysis theater" the space project's roadmap
// explicitly rejected for its 8-axis Story Importance Engine.
//
// STANDING RULE (see stluker-infrastructure.md, Jul 23 2026 entry):
// anything written to EARTH_KV via the Windows/PowerShell 5.1 deploy
// path has bitten us on non-ASCII punctuation before. This module
// scrubs em dashes/curly quotes out of any LLM-generated text before
// it's ever written to KV, so that risk can't resurface through
// ingest output even though this task itself runs server-side, not
// through PowerShell.

const EARTH_DATA_KEY = "earth-data";
const NMSZ_BBOX = { minlatitude: 35.0, maxlatitude: 38.5, minlongitude: -91.5, maxlongitude: -88.5 };
const YELLOWSTONE_BBOX = { minlatitude: 44.0, maxlatitude: 45.2, minlongitude: -111.3, maxlongitude: -109.7 };
const USER_AGENT = "stluker.com earth-ingest/1.0 (contact: pdluker@gmail.com)";
const GVP_MAX_AGE_MS = 6 * 24 * 3600 * 1000; // re-fetch GVP at most ~weekly
// Bump this whenever parseGvpRss's output shape changes (new fields, fixed
// bugs, etc). A cached volcanoes block with a missing/older version is
// treated as stale regardless of fetchedAt age - otherwise a parser fix can
// silently sit unused for up to 6 days behind a "not stale yet" cache hit,
// which is exactly what happened on 2026-07-23: the coordinate/encoding
// fixes were deployed but the cached pre-fix data kept getting served
// because only time-based staleness was checked, not a schema change.
const GVP_SCHEMA_VERSION = 2;

// EONET result-set caps (2026-08-26, added alongside the days=2 removal
// above). Previously the days=2 query parameter did double duty as both
// "is this event still relevant" (wrong - status=open already answers
// that) and "keep the result set/payload/Haiku input a sane size" (right
// goal, wrong mechanism). These three constants now do that second job
// explicitly, the same way BBOX_EVENT_LIMIT already guards the USGS bbox
// queries against a swarm-day blowing up the payload, and the same way
// NEWS_WHY_PER_CATEGORY already caps news items going into the Haiku call.
const EONET_FETCH_LIMIT = 150;    // server-side cap via EONET's own `limit` param
const EONET_DISPLAY_LIMIT = 30;   // per-category cap on what's stored/shown, newest-first
const EONET_WHY_PER_CATEGORY = 5; // per-category cap on what reaches the Haiku call

// ---------- small utilities ----------

function sanitizeAscii(str) {
  if (typeof str !== "string") return str;
  return str
    .replace(/[\u2014\u2013]/g, "-")   // em/en dash -> hyphen
    .replace(/[\u2018\u2019]/g, "'")   // curly single quotes -> straight
    .replace(/[\u201C\u201D]/g, '"');  // curly double quotes -> straight
}

async function fetchWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs || 8000);
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": USER_AGENT, Accept: "application/json, application/rss+xml, */*" },
      signal: controller.signal,
    });
    return res;
  } finally {
    clearTimeout(t);
  }
}

// GVP's RSS feed is served as Windows-1252, not UTF-8 (confirmed via a live
// test run on 2026-07-23 — decoding it as UTF-8 corrupted every accented
// character and smart quote into U+FFFD / mojibake). Decode explicitly
// rather than trusting res.text()'s default.
async function fetchGvpRssText(url, timeoutMs) {
  const res = await fetchWithTimeout(url, timeoutMs);
  if (!res.ok) return { ok: false, status: res.status };
  const buf = await res.arrayBuffer();
  const decoder = new TextDecoder("windows-1252");
  return { ok: true, text: decoder.decode(buf) };
}

const HTML_ENTITIES = {
  "&lt;": "<", "&gt;": ">", "&amp;": "&", "&quot;": '"', "&#39;": "'", "&apos;": "'",
  "&rsquo;": "'", "&lsquo;": "'", "&rdquo;": '"', "&ldquo;": '"',
  "&ndash;": "-", "&mdash;": "-", "&nbsp;": " ",
};

function decodeHtmlEntities(str) {
  if (typeof str !== "string") return str;
  return str
    .replace(/&lt;|&gt;|&amp;|&quot;|&#39;|&apos;|&rsquo;|&lsquo;|&rdquo;|&ldquo;|&ndash;|&mdash;|&nbsp;/g, (m) => HTML_ENTITIES[m] || m)
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(parseInt(code, 10)));
}

function isoHoursAgo(hours) {
  return new Date(Date.now() - hours * 3600 * 1000).toISOString();
}

// ---------- parsers (pure functions, independently testable) ----------

function parseUSGSFeatureCollection(geojson) {
  if (!geojson || !Array.isArray(geojson.features)) return [];
  return geojson.features.map((f) => {
    const coords = (f.geometry && f.geometry.coordinates) || [];
    return {
      id: f.id,
      place: sanitizeAscii(f.properties.place || "Unknown location"),
      mag: typeof f.properties.mag === "number" ? f.properties.mag : null,
      time: f.properties.time,
      url: f.properties.url,
      lon: typeof coords[0] === "number" ? coords[0] : null,
      lat: typeof coords[1] === "number" ? coords[1] : null,
    };
  }).filter((q) => q.mag !== null);
}

function parseEonetEvents(json, wantedCategoryId) {
  if (!json || !Array.isArray(json.events)) return [];
  return json.events
    .filter((ev) => (ev.categories || []).some((c) => c.id === wantedCategoryId))
    .map((ev) => {
      const geom = (ev.geometry && ev.geometry[ev.geometry.length - 1]) || {};
      const coords = geom.coordinates || [];
      return {
        id: ev.id,
        title: sanitizeAscii(ev.title || "Untitled event"),
        date: geom.date ? new Date(geom.date).getTime() : Date.now(),
        url: ev.link || (ev.sources && ev.sources[0] && ev.sources[0].url) || null,
        lon: typeof coords[0] === "number" ? coords[0] : null,
        lat: typeof coords[1] === "number" ? coords[1] : null,
        // ADDED 2026-09-24: EONET storm geometries carry max sustained wind
        // (usually kts) on the latest fix. Without it, the script could only
        // say where a storm was, never how strong -- the Sep 24 episode
        // described two hurricanes by position alone.
        magnitudeValue: typeof geom.magnitudeValue === "number" ? geom.magnitudeValue : null,
        magnitudeUnit: geom.magnitudeUnit || null,
      };
    });
}

// GVP RSS titles follow the structure "Name (Country) - Report for DATE - Activity
// Type" (confirmed against a real fetched sample on 2026-07-23 — this replaced an
// earlier guess that assumed "Name (Country)" was the whole title). Split on " - "
// (space-hyphen-space) rather than a bare "-", since the date range itself contains
// an un-spaced hyphen (e.g. "2 July-8 July 2026") that would otherwise be split on.
function parseGvpRss(xmlText) {
  if (!xmlText || typeof xmlText !== "string") return [];
  const items = [];
  const itemRe = /<item>([\s\S]*?)<\/item>/g;
  let m;
  while ((m = itemRe.exec(xmlText)) !== null) {
    const block = m[1];
    const rawTitle = (block.match(/<title>([\s\S]*?)<\/title>/) || [, ""])[1].trim();
    const rawDescription = (block.match(/<description>([\s\S]*?)<\/description>/) || [, ""])[1].trim();
    const link = (block.match(/<link>([\s\S]*?)<\/link>/) || [, ""])[1].trim();
    const geoMatch = block.match(/<georss:point>\s*([\-\d.]+)\s+([\-\d.]+)\s*<\/georss:point>/);
    const lat = geoMatch ? parseFloat(geoMatch[1]) : null;
    const lon = geoMatch ? parseFloat(geoMatch[2]) : null;
    if (!rawTitle) continue;

    const title = decodeHtmlEntities(rawTitle);
    const parts = title.split(" - ");
    const volcanoPart = parts[0] || title;
    const reportPart = parts[1] || "";
    const activityPart = parts[2] || "";

    const nameMatch = volcanoPart.match(/^(.*?)\s*\(([^)]*)\)\s*$/);
    const reportMatch = reportPart.match(/Report for\s+(.+)/i);

    const description = sanitizeAscii(
      decodeHtmlEntities(rawDescription)
        .replace(/<[^>]+>/g, "")
        .trim()
        .slice(0, 220)
    );

    items.push({
      id: "gvp-" + title.replace(/[^a-z0-9]+/gi, "-").toLowerCase(),
      name: sanitizeAscii(nameMatch ? nameMatch[1].trim() : volcanoPart),
      country: sanitizeAscii(nameMatch ? nameMatch[2].trim() : ""),
      status: sanitizeAscii(activityPart) || description,
      description,
      reportWeekOf: reportMatch ? sanitizeAscii(reportMatch[1].trim()) : null,
      url: link || null,
      lat,
      lon,
    });
  }
  return items;
}

// ---------- news feeds: ocean / atmosphere / geography (2026-07-30) ----------
//
// Mirrors space-ingest.js's RSS layer: each feed is fetched independently,
// parsed into a common {id, title, url, publishedAt, summary, source,
// category} shape, and one bad feed never takes down the others. All
// sources below are free/no-key RSS, same bar as everything else this
// pipeline already ingests.
// Every URL below was verified live via a direct fetch on 2026-07-30 before
// being committed here (see chat notes) - each returned a real
// application/rss+xml document, not a redirect to an HTML page. Two of the
// four originally-guessed URLs were wrong and are recorded here so the same
// mistake isn't repeated:
//   - NOAA Ocean Service's real feed is /rss/nosnews.xml, not /rss/news.xml
//     (the guessed path doesn't exist; the feed library lists several other
//     named feeds under oceanservice.noaa.gov/rss/).
//   - NASA Earth Observatory's old earthobservatory.nasa.gov/feeds/*.rss
//     paths now redirect into the migrated science.nasa.gov site and no
//     longer serve RSS at all; the live replacement is
//     science.nasa.gov/feed/earth-observatory/natural-events.
//   - climate.gov itself is an ARCHIVED site as of 2026-06-25 per its own
//     banner ("Content is not being updated or maintained") - its RSS feeds
//     are technically live but structurally stale, so it's dropped in favor
//     of NOAA's actively-updated top-level feed (noaa.gov/rss.xml), which
//     still surfaces atmosphere/ocean/climate stories from across NOAA.
//   - ECMWF has no real news RSS feed (only a quarterly PDF newsletter,
//     wrong cadence for a 24h window regardless) - dropped rather than
//     guessed at, replaced with a second geography source (Eos.org, AGU)
//     to keep 2 sources per category.
const NEWS_MAX_AGE_HOURS = 24;
const NEWS_FEEDS = [
  { name: "NOAA Ocean Service", category: "ocean", url: "https://oceanservice.noaa.gov/rss/nosnews.xml" },
  { name: "NASA Earth Observatory", category: "ocean", url: "https://science.nasa.gov/feed/earth-observatory/natural-events" },
  { name: "NOAA News", category: "atmosphere", url: "https://www.noaa.gov/rss.xml" },
  { name: "AGU Eos.org", category: "geography", url: "https://eos.org/feed" },
  { name: "USGS National News", category: "geography", url: "https://www.usgs.gov/news/national-news-release/feed" },
];

// Generic RSS <item> parser — unlike parseGvpRss above, this makes no
// assumption about title structure (GVP's "Name (Country) - Report for
// DATE - Type" format is specific to that one feed). Every other feed here
// is a normal news RSS: title, link, pubDate, description.
//
// The ESA CDATA-link bug found in space-ingest.js (2026-07-24) — a <link>
// value wrapped in <![CDATA[...]]> was never stripped, so sourceUrl landed
// in KV as literal CDATA text — is guarded against here from the start
// rather than waiting to hit it live a second time.
function stripCdata(str) {
  if (typeof str !== "string") return str;
  const m = str.match(/^<!\[CDATA\[([\s\S]*?)\]\]>$/);
  return (m ? m[1] : str).trim();
}

function parseRssItems(xmlText, sourceName, category) {
  if (!xmlText || typeof xmlText !== "string") return [];
  const items = [];
  const itemRe = /<item>([\s\S]*?)<\/item>/g;
  let m;
  while ((m = itemRe.exec(xmlText)) !== null) {
    const block = m[1];
    const rawTitle = (block.match(/<title>([\s\S]*?)<\/title>/) || [, ""])[1];
    const rawLink = (block.match(/<link>([\s\S]*?)<\/link>/) || [, ""])[1];
    const rawDescription = (block.match(/<description>([\s\S]*?)<\/description>/) || [, ""])[1];
    const rawPubDate = (block.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [, ""])[1].trim();

    const title = sanitizeAscii(decodeHtmlEntities(stripCdata(rawTitle)).trim());
    if (!title) continue;
    const url = stripCdata(rawLink) || null;
    const summary = sanitizeAscii(
      decodeHtmlEntities(stripCdata(rawDescription))
        .replace(/<[^>]+>/g, "")
        .trim()
        .slice(0, 220)
    );
    const publishedAt = rawPubDate ? new Date(rawPubDate).getTime() : Date.now();

    items.push({
      id: "news-" + sourceName.replace(/[^a-z0-9]+/gi, "-").toLowerCase() + "-" +
        title.replace(/[^a-z0-9]+/gi, "-").toLowerCase().slice(0, 60),
      title,
      url,
      summary,
      source: sourceName,
      category,
      publishedAt,
    });
  }
  return items;
}

// Fetches every NEWS_FEEDS entry concurrently. Each feed fails
// independently (matches the EONET/GVP degrade-to-empty pattern already
// used elsewhere in this module) and results are filtered to the last
// NEWS_MAX_AGE_HOURS by publishedAt before being returned, so a feed that
// doesn't support recency filtering server-side still gets one applied
// here.
async function fetchNewsFeeds() {
  const cutoff = Date.now() - NEWS_MAX_AGE_HOURS * 3600 * 1000;
  const results = await Promise.all(
    NEWS_FEEDS.map(async (feed) => {
      try {
        const res = await fetchWithTimeout(feed.url, 8000);
        if (!res.ok) throw new Error(feed.name + " returned " + res.status);
        const text = await res.text();
        const items = parseRssItems(text, feed.name, feed.category).filter(
          (it) => it.publishedAt >= cutoff
        );
        return { feed: feed.name, category: feed.category, items, error: null };
      } catch (err) {
        return { feed: feed.name, category: feed.category, items: [], error: err.message };
      }
    })
  );
  return results;
}

// BBOX_EVENT_LIMIT guards against an unbounded response during a real
// multi-day swarm (Yellowstone in particular has historically produced
// hundreds of micro-quakes in a 24h window) - without a cap, a swarm day
// could return a payload large enough to strain the 8s fetch timeout with
// no fallback. orderby=magnitude ensures that even if the API-side limit
// truncates results, what we DO get back is the biggest events in the
// window, not an arbitrary/time-ordered subset - this is also what makes
// events[0] a valid "largest" claim (see computeRegionStatus below).
const BBOX_EVENT_LIMIT = 500;

async function fetchBboxQuakes(bbox) {
  const start = isoHoursAgo(24);
  const url =
    "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson" +
    "&starttime=" + encodeURIComponent(start) +
    "&minlatitude=" + bbox.minlatitude +
    "&maxlatitude=" + bbox.maxlatitude +
    "&minlongitude=" + bbox.minlongitude +
    "&maxlongitude=" + bbox.maxlongitude +
    "&orderby=magnitude" +
    "&limit=" + BBOX_EVENT_LIMIT;
  const res = await fetchWithTimeout(url);
  if (!res.ok) throw new Error("USGS FDSN bbox query returned " + res.status);
  return parseUSGSFeatureCollection(await res.json());
}

function computeRegionStatus(events) {
  // Sort by magnitude descending before slicing, so events[0] is genuinely
  // the largest event in the window - not just whichever one the API
  // happened to return first. This was previously assumed (the frontend
  // labels events[0] as "largest M...") without ever actually being sorted.
  // Kept as a defensive sort here even though fetchBboxQuakes now requests
  // orderby=magnitude at the API level, so this function's own contract
  // ("events[0] is the largest") holds regardless of fetch-side ordering.
  const sorted = events.slice().sort((a, b) => (b.mag || 0) - (a.mag || 0));
  return {
    windowHours: 24,
    eventCount: events.length,
    events: sorted.slice(0, 10),
    status: events.length > 0 ? "active" : "quiet",
  };
}

function computeNmszStatus(events) {
  return computeRegionStatus(events);
}

// Earth Pulse Score — deliberately built ONLY from categories this pipeline
// actually ingests (quakes, NMSZ/Yellowstone, EONET wildfires/storms/ice,
// GVP volcano activity). NOT solar activity, meteor impacts, or anything else
// that would require fabricating a number from data we don't have. This is a
// same-day activity-level snapshot, not a trend against history yet (that
// needs a stored baseline — Tier B, not built here).
const PULSE_WEIGHTS = {
  perSignificantQuake: 8,
  regionActive: 15, // NMSZ or Yellowstone showing any activity
  perWildfire: 3,
  perStorm: 5,
  perIceEvent: 2,
  perNewVolcanicActivity: 4,
  perContinuingVolcanicActivity: 1,
};

function computePulseScore(counts) {
  const raw =
    counts.significantQuakes * PULSE_WEIGHTS.perSignificantQuake +
    (counts.nmszActive ? PULSE_WEIGHTS.regionActive : 0) +
    (counts.yellowstoneActive ? PULSE_WEIGHTS.regionActive : 0) +
    counts.wildfires * PULSE_WEIGHTS.perWildfire +
    counts.storms * PULSE_WEIGHTS.perStorm +
    counts.ice * PULSE_WEIGHTS.perIceEvent +
    counts.volcanoNew * PULSE_WEIGHTS.perNewVolcanicActivity +
    counts.volcanoContinuing * PULSE_WEIGHTS.perContinuingVolcanicActivity;
  const score = Math.min(100, Math.max(0, Math.round(raw)));
  const label = score <= 25 ? "Quiet" : score <= 50 ? "Normal" : score <= 75 ? "Active" : "High";
  return { score, label };
}

function countVolcanicActivity(entries) {
  let volcanoNew = 0, volcanoContinuing = 0;
  (entries || []).forEach((v) => {
    const status = (v.status || "").toLowerCase();
    if (status.includes("new")) volcanoNew++;
    else if (status.includes("continuing")) volcanoContinuing++;
  });
  return { volcanoNew, volcanoContinuing };
}

const INTELLIGENCE_SYSTEM = sanitizeAscii(
  "You write a short daily situational-awareness briefing for a personal " +
  "earth-hazards and earth-science-news dashboard, plus one plain-language " +
  "sentence of context for specific notable events, given only verified " +
  "facts (counts, magnitudes, locations, categories, news headlines from " +
  "ocean/atmosphere/geography sources). Never invent facts not present in " +
  "the input - if activity is low, say so plainly rather than manufacturing " +
  "drama. When real ocean, atmosphere, or geography news items are present " +
  "in referencePool, prefer weaving in at least one alongside the hazard " +
  "counts, rather than only ever discussing quakes/fires/volcanoes. Use plain " +
  "ASCII punctuation only (hyphens, straight quotes) - no em dashes or curly " +
  "quotes. When the briefing mentions a SPECIFIC named item from the provided " +
  "referencePool (a named volcano, a specific earthquake location, a named " +
  "storm, a specific wildfire), immediately follow that mention with a " +
  "citation marker in the exact form {{ref:ID}} using the literal id from " +
  "referencePool - for example 'ongoing eruptive activity at Kilauea{{ref:" +
  "gvp-kilauea-...}}'. Only use ids that literally appear in referencePool - " +
  "never invent or guess an id, and do not add a marker for a general " +
  "statement that isn't citing one specific item. For each item in the " +
  "provided items list, including any item whose kind starts with 'news-', " +
  "write a genuine 'why this matters' or 'so what' sentence in whyItMatters - " +
  "explain the real-world significance or implication of the story, do not " +
  "just restate or rephrase its headline; if you cannot identify a real " +
  "implication beyond the headline, omit that item's key from whyItMatters " +
  "entirely rather than filling it with a restatement. Respond with ONLY a " +
  "JSON object of this exact shape: " +
  '{"briefing": "2-3 sentence plain-language summary of today\'s global ' +
  'activity, mentioning specific real numbers/locations from the input where ' +
  'relevant, with {{ref:ID}} markers after specific named mentions", ' +
  '"whyItMatters": {"<id>": "one sentence per significant item"}}. ' +
  "Nothing else, no markdown fences."
);

function buildIntelligenceUserPrompt(counts, items, referencePool) {
  return JSON.stringify({
    counts: counts,
    items: items.map((i) => ({ id: i.id, kind: i.kind, summary: i.summary })),
    referencePool: referencePool.map((r) => ({ id: r.id, kind: r.kind, label: r.label })),
  });
}

async function callIntelligence(env, counts, items, referencePool) {
  if (!env.ANTHROPIC_API_KEY) {
    return { briefing: null, whyItMatters: {}, error: "ANTHROPIC_API_KEY not bound on stl-dispatcher" };
  }
  const body = {
    model: "claude-haiku-4-5-20251001",
    max_tokens: 1300,
    system: [{ type: "text", text: INTELLIGENCE_SYSTEM, cache_control: { type: "ephemeral" } }],
    messages: [{ role: "user", content: buildIntelligenceUserPrompt(counts, items, referencePool || []) }],
  };
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const bodyText = await res.text();
    return { briefing: null, whyItMatters: {}, error: "Anthropic API returned " + res.status + ": " + bodyText.slice(0, 300) };
  }
  const data = await res.json();
  const text = (data.content || []).filter((b) => b.type === "text").map((b) => b.text).join("");
  try {
    const cleaned = text.replace(/```json|```/g, "").trim();
    const parsed = JSON.parse(cleaned);
    const whyItMatters = {};
    Object.keys(parsed.whyItMatters || {}).forEach((k) => {
      whyItMatters[k] = sanitizeAscii(parsed.whyItMatters[k]);
    });
    return {
      briefing: typeof parsed.briefing === "string" ? sanitizeAscii(parsed.briefing) : null,
      whyItMatters,
    };
  } catch (err) {
    return { briefing: null, whyItMatters: {}, error: "Could not parse Haiku response as JSON: " + err.message + " - raw text: " + text.slice(0, 300) };
  }
}

// Every real item the briefing is allowed to cite. Kept server-side only
// (not shipped in the payload) since the frontend already has id/url on
// each item in quakes/surfaceEvents/volcanoes - it just needs to walk those
// and match by id when it encounters a {{ref:ID}} marker in the briefing.
function buildReferencePool({ significantQuakes, wildfires, storms, ice, volcanoes, news }) {
  const pool = [];
  (significantQuakes || []).forEach((q) => pool.push({ id: q.id, kind: "earthquake", label: q.place }));
  (wildfires || []).forEach((w) => pool.push({ id: w.id, kind: "wildfire", label: w.title }));
  (storms || []).forEach((s) => pool.push({ id: s.id, kind: "storm", label: s.title }));
  (ice || []).forEach((i) => pool.push({ id: i.id, kind: "ice", label: i.title }));
  ((volcanoes && volcanoes.entries) || []).forEach((v) =>
    pool.push({ id: v.id, kind: "volcano", label: v.name + (v.country ? ", " + v.country : "") })
  );
  if (news) {
    Object.keys(news).forEach((category) => {
      (news[category] || []).forEach((n) =>
        pool.push({ id: n.id, kind: "news-" + category, label: n.title })
      );
    });
  }
  return pool;
}

// Defensive check against a hallucinated/invented id in a {{ref:ID}} marker -
// an LLM occasionally invents a plausible-looking id despite instructions not
// to. Any marker whose id isn't in the real reference pool is stripped
// entirely (not left as a dead link) rather than trusting model output.
function stripInvalidRefs(briefing, referencePool) {
  if (typeof briefing !== "string") return briefing;
  const validIds = new Set((referencePool || []).map((r) => r.id));
  return briefing.replace(/\{\{ref:([^}]+)\}\}/g, (match, id) => (validIds.has(id) ? match : ""));
}

// ---------- main orchestration ----------

async function runEarthIngest(env) {
  const warnings = [];

  // Single KV read, reused by every carry-forward path below (quakes,
  // NMSZ, Yellowstone, GVP) - previously this was fetched twice
  // separately just for the GVP block; consolidating it also makes it
  // trivial to extend carry-forward to more sources later.
  let existing = null;
  try {
    const existingRaw = await env.EARTH_KV.get(EARTH_DATA_KEY);
    existing = existingRaw ? JSON.parse(existingRaw) : null;
  } catch (_) {
    // No previous data to carry forward from (first-ever run, or KV
    // itself is having a bad day) - each carry-forward path below already
    // falls back to an honest empty/quiet default in that case.
  }

  // ---- required source 1: global significant quakes ----
  // On failure, carry forward the previous run's list rather than
  // reporting zero - a fetch failure and "confirmed no significant
  // quakes today" are different facts, and this pipeline should not
  // collapse them into the same "quiet" output.
  let significantQuakes = [];
  try {
    const res = await fetchWithTimeout(
      "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson"
    );
    if (!res.ok) throw new Error("USGS significant feed returned " + res.status);
    significantQuakes = parseUSGSFeatureCollection(await res.json());
  } catch (err) {
    warnings.push("USGS significant quakes fetch failed (carrying forward previous data): " + err.message);
    if (existing && existing.quakes && Array.isArray(existing.quakes.significant)) {
      significantQuakes = existing.quakes.significant;
    }
  }

  // ---- required source 2: NMSZ bbox, any magnitude, always-on ----
  // Carries forward the previous run's already-computed status object
  // (not just a raw event list) since that's what "quiet due to no data"
  // vs "quiet due to a real quiet 24h" actually needs to distinguish.
  let nmszStatus;
  try {
    const nmszEvents = await fetchBboxQuakes(NMSZ_BBOX);
    nmszStatus = computeRegionStatus(nmszEvents);
  } catch (err) {
    warnings.push("NMSZ bbox fetch failed (carrying forward previous data): " + err.message);
    nmszStatus = (existing && existing.nmsz) || computeRegionStatus([]);
  }

  // ---- required source 3: Yellowstone bbox, any magnitude, always-on ----
  // Same pattern as NMSZ — a second fixed-region watch strip, independent
  // failure (one region failing doesn't affect the other).
  let yellowstoneStatus;
  try {
    const yellowstoneEvents = await fetchBboxQuakes(YELLOWSTONE_BBOX);
    yellowstoneStatus = computeRegionStatus(yellowstoneEvents);
  } catch (err) {
    warnings.push("Yellowstone bbox fetch failed (carrying forward previous data): " + err.message);
    yellowstoneStatus = (existing && existing.yellowstone) || computeRegionStatus([]);
  }

  // ---- best-effort source: EONET ----
  // 2026-08-26: query is now status=open only (no days=N recency filter -
  // see header comment for the incident that caused this and why
  // category-id matching below was never actually the problem). Results
  // are sorted newest-first and capped per category via
  // EONET_DISPLAY_LIMIT so removing the days window doesn't let an
  // unusually active wildfire/storm day blow up payload size the way
  // BBOX_EVENT_LIMIT already guards against for USGS swarms.
  let wildfires = [], storms = [], ice = [];
  try {
    const res = await fetchWithTimeout(
      "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=" + EONET_FETCH_LIMIT
    );
    if (!res.ok) throw new Error("EONET returned " + res.status);
    const json = await res.json();
    const newestFirst = (a, b) => b.date - a.date;
    wildfires = parseEonetEvents(json, "wildfires").sort(newestFirst).slice(0, EONET_DISPLAY_LIMIT);
    storms = parseEonetEvents(json, "severeStorms").sort(newestFirst).slice(0, EONET_DISPLAY_LIMIT);
    ice = parseEonetEvents(json, "seaLakeIce").sort(newestFirst).slice(0, EONET_DISPLAY_LIMIT);
  } catch (err) {
    warnings.push("EONET fetch failed (degrading to empty): " + err.message);
  }

  // ---- best-effort source: GVP weekly, only refetched if stale ----
  let volcanoes = { reportWeekOf: null, entries: [] };
  try {
    const lastFetchedAt = existing && existing.volcanoes && existing.volcanoes.fetchedAt
      ? new Date(existing.volcanoes.fetchedAt).getTime()
      : 0;
    const cachedSchemaVersion = existing && existing.volcanoes && existing.volcanoes.schemaVersion;
    const isStale = Date.now() - lastFetchedAt > GVP_MAX_AGE_MS || cachedSchemaVersion !== GVP_SCHEMA_VERSION;

    if (!isStale && existing && existing.volcanoes) {
      volcanoes = existing.volcanoes; // carry forward, no network call spent
    } else {
      const result = await fetchGvpRssText("https://volcano.si.edu/news/WeeklyVolcanoRSS.xml", 10000);
      if (!result.ok) throw new Error("GVP RSS returned " + result.status);
      const entries = parseGvpRss(result.text);
      const reportWeekOf = entries.length && entries[0].reportWeekOf ? entries[0].reportWeekOf : null;
      volcanoes = {
        reportWeekOf,
        entries: entries.map(({ reportWeekOf, ...rest }) => rest), // don't repeat the same week string on every entry
        fetchedAt: new Date().toISOString(),
        schemaVersion: GVP_SCHEMA_VERSION,
      };
    }
  } catch (err) {
    warnings.push("GVP RSS fetch failed (carrying forward previous data): " + err.message);
    if (existing && existing.volcanoes) volcanoes = existing.volcanoes;
  }

  // ---- best-effort source: ocean/atmosphere/geography news (2026-07-30) ----
  // Same degrade-to-empty contract as EONET above: a failed feed just
  // means fewer news items today, not a failed run. Not carried forward
  // on failure (unlike quakes/GVP) since these are time-sensitive news
  // items, not a status snapshot — stale news would be actively misleading
  // where stale "no significant quakes" carry-forward is not.
  let news = { ocean: [], atmosphere: [], geography: [] };
  try {
    const feedResults = await fetchNewsFeeds();
    feedResults.forEach((r) => {
      if (r.error) warnings.push("News feed '" + r.feed + "' fetch failed (skipped): " + r.error);
      if (news[r.category]) news[r.category] = news[r.category].concat(r.items);
    });
    // Newest first within each category, capped to keep the payload sane
    // on a busy news day across 6+ feeds.
    Object.keys(news).forEach((cat) => {
      news[cat] = news[cat].sort((a, b) => b.publishedAt - a.publishedAt).slice(0, 12);
    });
  } catch (err) {
    warnings.push("News feed layer failed entirely (degrading to empty): " + err.message);
  }

  // ---- counts + Pulse Score (built only from what we actually ingested) ----
  const { volcanoNew, volcanoContinuing } = countVolcanicActivity(volcanoes.entries);
  const counts = {
    significantQuakes: significantQuakes.length,
    nmszActive: nmszStatus.status === "active",
    yellowstoneActive: yellowstoneStatus.status === "active",
    wildfires: wildfires.length,
    storms: storms.length,
    ice: ice.length,
    volcanoNew,
    volcanoContinuing,
  };
  const pulse = computePulseScore(counts);

  // ---- intelligence: daily briefing (always) + why-it-matters (for notable items) ----
  // Unlike the old why-it-matters-only call, the briefing runs every day even
  // when everything is quiet ("today has been quiet globally" is itself useful
  // information) - same accepted cost model as space-ingest's one-Haiku-call/day.
  //
  // referencePool covers every real item the briefing is allowed to cite via
  // a {{ref:ID}} marker (see INTELLIGENCE_SYSTEM). Kept separate from
  // whyItemsInput (which is only the "significant enough for its own
  // one-liner" subset) since the briefing should be able to reference
  // anything real - including volcanoes/storms that don't get their own
  // why-it-matters entry.
  const whyItemsInput = [];
  significantQuakes.forEach((q) =>
    whyItemsInput.push({ id: q.id, kind: "earthquake", summary: q.place + ", M" + q.mag })
  );
  // Capped via EONET_WHY_PER_CATEGORY (2026-08-26): wildfires is no longer
  // implicitly bounded by the old days=2 recency window, so on a genuinely
  // busy wildfire day (dozens of events nationwide, same order of magnitude
  // as the Reno-area Hawk Fire incident that prompted this fix) this list
  // needs its own explicit cap - otherwise every open wildfire in the
  // country goes into the Haiku call, the same class of uncapped-input cost
  // risk already documented and fixed for fire-api's topic loop. wildfires
  // is already sorted newest-first above, so slice(0, N) keeps the most
  // recently updated fires, not an arbitrary subset.
  wildfires.slice(0, EONET_WHY_PER_CATEGORY).forEach((w) =>
    whyItemsInput.push({ id: w.id, kind: "wildfire", summary: w.title })
  );

  // News items (2026-08-04): previously never sent to Haiku at all, so
  // whyItMatters had no entry for any news story and the frontend could
  // only show the raw RSS <description> excerpt - not real "why it
  // matters" analysis. Capped per category (top 3, already newest-first
  // from fetchNewsFeeds' sort) to keep this call's token cost bounded -
  // same "accepted one-call/day" cost model as the rest of this pipeline,
  // not an open-ended per-headline expense.
  const NEWS_WHY_PER_CATEGORY = 3;
  Object.keys(news).forEach((category) => {
    (news[category] || []).slice(0, NEWS_WHY_PER_CATEGORY).forEach((n) =>
      whyItemsInput.push({ id: n.id, kind: "news-" + category, summary: n.title })
    );
  });

  const referencePool = buildReferencePool({ significantQuakes, wildfires, storms, ice, volcanoes, news });

  let briefing = null;
  let whyItMatters = {};
  try {
    const result = await callIntelligence(env, counts, whyItemsInput, referencePool);
    briefing = result.briefing ? stripInvalidRefs(result.briefing, referencePool) : null;
    whyItMatters = result.whyItMatters;
    if (result.error) warnings.push("Haiku intelligence call did not produce a briefing: " + result.error);
  } catch (err) {
    warnings.push("Haiku intelligence call failed: " + err.message);
  }

  const payload = {
    meta: { generatedAt: new Date().toISOString(), warnings },
    pulse,
    briefing,
    nmsz: nmszStatus,
    yellowstone: yellowstoneStatus,
    quakes: { significant: significantQuakes, count: significantQuakes.length },
    surfaceEvents: { wildfires, storms, ice },
    volcanoes,
    news,
    whyItMatters,
  };

  await env.EARTH_KV.put(EARTH_DATA_KEY, JSON.stringify(payload));
  return payload;
}

export {
  runEarthIngest,
  // exported for testing
  sanitizeAscii,
  decodeHtmlEntities,
  parseUSGSFeatureCollection,
  parseEonetEvents,
  parseGvpRss,
  parseRssItems,
  stripCdata,
  fetchNewsFeeds,
  computeNmszStatus,
  computeRegionStatus,
  computePulseScore,
  countVolcanicActivity,
  buildReferencePool,
  stripInvalidRefs,
};
