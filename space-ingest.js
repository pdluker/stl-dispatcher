/**
 * space-ingest.js
 *
 * New stl-dispatcher task: pulls the top-25 space news RSS feeds + Launch Library 2
 * launch data, dedups near-identical stories across sources, summarizes via Haiku
 * 4.5 (never storing scraped text verbatim), and writes one combined blob to
 * SPACE_KV["space-data"] matching the contract in space-worker/SCHEMA.md.
 *
 * Designed to be imported and called as a new task from stl-dispatcher's existing
 * cron handler, e.g.:
 *
 *   import { runSpaceIngest } from "./space-ingest.js";
 *   // inside the daily cron handler, alongside keepalive/statusSync/stl-bucket:
 *   await runSpaceIngest(env);
 *
 * Env bindings required: SPACE_KV, ANTHROPIC_API_KEY (secret)
 * Env bindings optional: LL2_API_KEY (secret) — NEW 2026-07-24 (later same
 *   day). Register a free account at thespacedevs.com to get one. Without
 *   it, LL2 calls fall back to the old anonymous/shared-IP behavior, which a
 *   live test confirmed gets 429'd consistently on Cloudflare Workers' IP
 *   range — this key is the actual fix, not the earlier header/retry patch
 *   alone. meta.diagnostics' "Launch Library 2" entry reports `authenticated:
 *   true/false` so a future run can confirm at a glance whether the key is
 *   actually being sent, without re-deriving this from logs.
 *
 * CHANGED 2026-07-24: fetchUpcomingLaunches() was sending zero headers to LL2,
 * unlike every RSS feed fetch in this file — root cause of launches.next7Days
 * showing empty on the live dashboard while RSS-sourced sections populated
 * normally. Added a User-Agent/Accept header (matching the RSS fetch pattern)
 * plus a single retry-after-429. Also fixed mapLl2Response() silently dropping
 * launches with a missing/malformed `net` field (NaN date comparison). See
 * stluker-infrastructure.md for the full incident writeup.
 *
 * CHANGED 2026-07-24 (later same day): added meta.diagnostics — per-source
 * {name, count, error} for all 14 RSS feeds + LL2, ported from the same
 * pattern already shipped in stl-sports/intel. Also added a debug.ll2 field
 * (lastLL2Fetch timestamp + a truncated raw response snippet, capped at
 * LL2_RAW_SNIPPET_LEN chars) — not surfaced in the UI, queryable only, so a
 * future LL2-specific issue can be read straight out of KV.
 *
 * CHANGED 2026-07-24 (later still): added LL2_API_KEY support with graceful
 * anonymous fallback (see above) after a live test confirmed the earlier
 * header/retry fix alone wasn't enough — LL2 still 429s Workers' shared IP
 * pool without a real key. Decided not to buy a Patreon-gated key for now
 * (cost-efficiency call, Jul 24, 2026) — logged as a deferred enhancement in
 * stluker-infrastructure.md rather than left unexplained.
 *
 * CHANGED 2026-07-24 (later still): added two free, no-key sources found
 * while researching LL2 alternatives — fetchSpaceflightNewsArticles() (SNAPI,
 * same org as LL2, ~43 outlets, confirmed unauthenticated) feeding the same
 * RSS clustering pipeline, and fetchIssPasses() (iss-api.polluxlabs.io, the
 * maintained successor to open-notify.org's now-shut-down pass predictions)
 * completing the "tonight" card's roadmap-named ISS-passes feature, which
 * had never actually been built despite moon phase shipping. Both fail soft
 * into meta.diagnostics like every other source.
 */

// ---------------------------------------------------------------------------
// 1. Source list (subset of the top-25 audit — RSS-friendly feeds only;
//    a few outlets in the original 25, e.g. NASA Watch, don't publish clean RSS
//    and are omitted here rather than scraped)
// ---------------------------------------------------------------------------

export const FEEDS = [
  { name: "Space.com", url: "https://www.space.com/feeds/all", category: "General" },
  { name: "SpaceNews", url: "https://spacenews.com/feed/", category: "Industry" },
  { name: "Spaceflight Now", url: "https://spaceflightnow.com/feed/", category: "Launch" },
  { name: "NASA", url: "https://www.nasa.gov/news-release/feed/", category: "Mission" },
  { name: "Astronomy Magazine", url: "https://www.astronomy.com/feed", category: "Astronomy" },
  { name: "Astronomy Now", url: "https://astronomynow.com/feed/", category: "Astronomy" },
  { name: "Universe Today", url: "https://www.universetoday.com/feed/", category: "Astronomy" },
  { name: "SpaceQ", url: "https://spaceq.ca/feed/", category: "Industry" },
  { name: "SpaceDaily", url: "https://www.spacedaily.com/spacedaily.xml", category: "General" },
  { name: "Sky & Telescope", url: "https://skyandtelescope.org/feed/", category: "Astronomy" },
  { name: "SpaceRef", url: "https://spaceref.com/feed/", category: "General" },
  { name: "Phys.org Space", url: "https://phys.org/rss-feed/space-news/", category: "Astronomy" },
  { name: "ESA", url: "https://www.esa.int/rssfeed/Our_Activities", category: "Mission" },
  // REMOVED 2026-07-21 after first live run confirmed both 404 (feed URL drift, not a
  // transient error — confirmed via two consecutive fetches in the same run):
  //   "The Space Review"  — https://www.thespacereview.com/rss/rss.xml
  //   "Mars Daily"         — https://www.marsdaily.com/marsdaily.xml
  // Re-add once correct current feed URLs are confirmed by hand (don't guess a
  // replacement URL blindly — same principle as not deriving a literal value
  // from a weak signal).
  // Add remaining sources as their feed URLs are confirmed working — see
  // VALIDATION.md for the ones deferred pending a live-network check.
];

const LL2_UPCOMING_URL =
  "https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=25&mode=normal";

const KV_KEY = "space-data";
const CLUSTER_WINDOW_HOURS = 48;
const TITLE_SIMILARITY_THRESHOLD = 0.4;

// Only summarize items published within this window before clustering/Haiku.
// This is the main lever against the Workers free-tier 50-subrequest-per-invocation
// cap: 15 feeds can easily return 150+ raw items across a week of history, which
// used to mean 15+ Haiku batch calls. The dashboard only ever surfaces "last 24h" /
// "next 7 days" anyway, so anything older than this is dead weight before it's even
// summarized. Confirmed necessary after the 2026-07-21 live run tripped the
// subrequest limit on Launch Library 2 (the last call in the chain).
const INGEST_RECENCY_HOURS = 72;

// Items per Haiku batch call. Bumped from 10 -> 25 on 2026-07-21 for the same
// reason — fewer, larger batches means fewer subrequests for the same content.
const SUMMARIZE_BATCH_SIZE = 25;

// ---------------------------------------------------------------------------
// 2. RSS fetch + lightweight parse (no DOM parser available in Workers —
//    regex-based extraction, tolerant of both RSS 2.0 and Atom-ish feeds)
// ---------------------------------------------------------------------------

export async function fetchFeed(feed, fetchImpl = fetch) {
  try {
    const res = await fetchImpl(feed.url, {
      headers: { "User-Agent": "stluker-space-dashboard/1.0 (+https://space.stluker.com)" },
      cf: { cacheTtl: 0 },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const xml = await res.text();
    return { feed, items: parseRssItems(xml), error: null };
  } catch (err) {
    return { feed, items: [], error: String(err.message || err) };
  }
}

// ADDED 2026-07-24: The Spaceflight News API (SNAPI, run by The Space Devs —
// same org as LL2) — free, no API key, no Patreon gate, confirmed working via
// a live fetch this session. Aggregates ~43 outlets (NASA, SpaceX, Reuters,
// NASASpaceflight, Spaceflight Now, and others) into one clean JSON endpoint —
// no XML/regex parsing risk the way every RSS source above has. Returns the
// exact same {feed, items, error} shape as fetchFeed so it slots directly into
// the existing Promise.all/clustering/Haiku pipeline with zero special-casing
// downstream. v4 is confirmed unauthenticated ("No authentication required" —
// unlike the deprecated v3, which needed a token); if that ever changes,
// this fails soft into meta.diagnostics like every other source, not a hard
// crash.
const SNAPI_ARTICLES_URL =
  "https://api.spaceflightnewsapi.net/v4/articles/?limit=30&ordering=-published_at";

const SNAPI_FEED_DESCRIPTOR = { name: "Spaceflight News API", category: "General" };

export async function fetchSpaceflightNewsArticles(fetchImpl = fetch) {
  try {
    const res = await fetchImpl(SNAPI_ARTICLES_URL, {
      headers: {
        "User-Agent": "stluker-space-dashboard/1.0 (+https://space.stluker.com)",
        "Accept": "application/json",
      },
      cf: { cacheTtl: 0 },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    const items = (data.results || [])
      .filter((a) => a.title && a.url)
      .map((a) => ({
        title: a.title,
        link: a.url,
        pubDate: a.published_at ? new Date(a.published_at).toISOString() : null,
        // Truncated to match the RSS description field's own 500-char cap
        // above — keeps Haiku prompt size consistent regardless of which
        // source a story came from.
        description: a.summary ? String(a.summary).slice(0, 500) : "",
      }));

    return { feed: SNAPI_FEED_DESCRIPTOR, items, error: null };
  } catch (err) {
    return { feed: SNAPI_FEED_DESCRIPTOR, items: [], error: String(err.message || err) };
  }
}

export function parseRssItems(xml) {
  const items = [];
  // Match both <item>...</item> (RSS) and <entry>...</entry> (Atom)
  const blocks = xml.match(/<(item|entry)>[\s\S]*?<\/(item|entry)>/g) || [];

  for (const block of blocks) {
    const title = extractTag(block, "title");
    const link = extractLink(block);
    const pubDate = extractTag(block, "pubDate") || extractTag(block, "published") || extractTag(block, "updated");
    const description = extractTag(block, "description") || extractTag(block, "summary");

    if (!title || !link) continue;

    items.push({
      title: decodeEntities(stripCdata(title)).trim(),
      link: link.trim(),
      pubDate: pubDate ? new Date(pubDate).toISOString() : null,
      description: description ? decodeEntities(stripCdata(description)).replace(/<[^>]+>/g, "").trim().slice(0, 500) : "",
    });
  }
  return items;
}

function extractTag(block, tag) {
  const m = block.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i"));
  return m ? m[1] : null;
}

function extractLink(block) {
  // Atom: <link href="..."/>  RSS: <link>url</link>
  const atom = block.match(/<link[^>]*href="([^"]+)"/i);
  if (atom) return atom[1];
  const rss = block.match(/<link>([\s\S]*?)<\/link>/i);
  // FIXED 2026-07-24: ESA's feed wraps its <link> content in CDATA
  // (<link><![CDATA[https://...]]></link>), unlike every other feed here —
  // title/description already get stripCdata() but this branch never did,
  // so ESA's sourceUrl was silently landing in KV as a literal
  // "<![CDATA[...]]>" string. Invisible until now because no card in the UI
  // has ever actually linked out to sourceUrl — the new briefing citations
  // are the first consumer that would have broken on it.
  return rss ? stripCdata(rss[1]).trim() : null;
}

function stripCdata(s) {
  const m = s.match(/^<!\[CDATA\[([\s\S]*)\]\]>$/);
  return m ? m[1] : s;
}

function decodeEntities(s) {
  return s
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#0?39;/g, "'");
}

// ---------------------------------------------------------------------------
// 3. Cross-source dedup / clustering
// ---------------------------------------------------------------------------

function normalizeTitle(title) {
  return title
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, "")
    .split(/\s+/)
    .filter((w) => w.length > 2 && !STOPWORDS.has(w));
}

const STOPWORDS = new Set([
  "the", "and", "for", "with", "from", "into", "over", "after", "before",
  "new", "its", "his", "her", "their", "will", "has", "have", "are", "was",
]);

function jaccard(aWords, bWords) {
  const a = new Set(aWords);
  const b = new Set(bWords);
  const intersection = [...a].filter((w) => b.has(w)).length;
  const union = new Set([...a, ...b]).size;
  return union === 0 ? 0 : intersection / union;
}

/** Groups items from any feed into clusters of the same underlying story. */
export function clusterItems(allItems) {
  const clusters = [];

  for (const item of allItems) {
    const words = normalizeTitle(item.title);
    const itemTime = item.pubDate ? new Date(item.pubDate).getTime() : Date.now();

    let placed = false;
    for (const cluster of clusters) {
      const withinWindow =
        Math.abs(itemTime - cluster.anchorTime) <= CLUSTER_WINDOW_HOURS * 3600 * 1000;
      if (!withinWindow) continue;

      const sim = jaccard(words, cluster.anchorWords);
      if (sim >= TITLE_SIMILARITY_THRESHOLD) {
        cluster.items.push(item);
        placed = true;
        break;
      }
    }

    if (!placed) {
      clusters.push({
        anchorWords: words,
        anchorTime: itemTime,
        items: [item],
      });
    }
  }

  return clusters.map((c) => ({
    sources: c.items.map((i) => i.sourceName),
    representative: c.items[0],
    clusterOf: c.items.length,
    allItems: c.items,
    latestPubDate: c.items.reduce(
      (max, i) => (i.pubDate && i.pubDate > max ? i.pubDate : max),
      c.items[0].pubDate || ""
    ),
  }));
}

// ---------------------------------------------------------------------------
// 4. Haiku summarization (rewrite only — never verbatim, per project copyright practice)
// ---------------------------------------------------------------------------

export async function summarizeClusters(clusters, env, fetchImpl = fetch) {
  // Returns { summarized, batchesTotal, batchesFailed, lastRawSnippet } instead
  // of a bare array — see the 2026-07-25 comment below for why.
  if (clusters.length === 0) {
    return { summarized: [], batchesTotal: 0, batchesFailed: 0, lastRawSnippet: null };
  }

  // Batch into groups to keep both prompt size and subrequest count manageable
  const batches = [];
  for (let i = 0; i < clusters.length; i += SUMMARIZE_BATCH_SIZE) batches.push(clusters.slice(i, i + SUMMARIZE_BATCH_SIZE));

  const results = [];
  let batchesFailed = 0;
  let lastRawSnippet = null;
  let skippedOffTopic = 0;
  let unmatchedEntries = 0;

  for (const batch of batches) {
    const prompt = buildSummarizationPrompt(batch);
    const res = await fetchImpl("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        // CHANGED 2026-07-25: raised from 2000 — a full batch of
        // SUMMARIZE_BATCH_SIZE (25) items each needing a headline, summary,
        // whyItMatters, and category can plausibly exceed 2000 tokens on a
        // heavy-news day, truncating the JSON array mid-response. A
        // truncated response fails JSON.parse, and safeParseJsonArray()
        // fails soft (returns []) by design — meaning a token-limit
        // truncation on EVERY batch in a run would silently produce zero
        // stories with no error anywhere. This is the leading suspect for
        // the Jul 25, 2026 zero-stories incident (real fetch counts across
        // 12 sources, zero summarized output, no thrown error). Root cause
        // not independently confirmed via a captured raw response — nothing
        // was stored at the time — so this is a preventive fix plus the
        // batchesFailed/lastRawSnippet tracking below, so a repeat is
        // diagnosable instead of another guess.
        max_tokens: 4096,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    if (!res.ok) {
      throw new Error(`Anthropic API error ${res.status}: ${await res.text()}`);
    }

    const data = await res.json();
    const text = data.content.map((b) => b.text || "").join("");
    const parsed = safeParseJsonArray(text);

    if (parsed.length === 0 && batch.length > 0) {
      // A batch that produced no parseable entries despite having real input
      // clusters is exactly the failure mode this session couldn't diagnose
      // after the fact — capture a snippet now so next time doesn't require
      // another guess.
      batchesFailed++;
      lastRawSnippet = text.slice(0, 500);
    }

    // FIXED 2026-07-25: this used to match parsed[idx] to batch[idx] by pure
    // array position. Adding the Skip category exposed a real bug in that
    // approach — Haiku sometimes omitted an entry entirely for a Skip'd item
    // instead of including it with category:"Skip" as instructed, which
    // shifted every subsequent entry in the batch by one position and
    // silently paired real headlines with the wrong article's sourceUrl
    // (confirmed live: a "lunar samples" headline pointing at a SpaceX
    // Starship URL). Matching by the explicit "index" field the prompt now
    // requires every entry to echo back is robust to omissions/reordering —
    // a positional match is not.
    const entryByIndex = new Map();
    parsed.forEach((entry) => {
      if (typeof entry.index === "number") entryByIndex.set(entry.index, entry);
    });

    let unmatchedInBatch = 0;
    batch.forEach((cluster, i) => {
      const entry = entryByIndex.get(i);
      if (!entry) {
        // No entry at all for this cluster (omitted, or missing/invalid
        // index) — safer to drop it than to guess, since guessing is exactly
        // what caused the mismatch bug above.
        unmatchedInBatch++;
        return;
      }

      // ADDED 2026-07-25: SpaceDaily (and potentially other aggregator feeds)
      // mixes genuine space-off-topic filler into its RSS feed — Haiku was
      // already correctly noticing this in whyItMatters ("This is not
      // space-related content") but nothing acted on that signal, so it
      // still landed on the dashboard under a real category. The prompt now
      // asks for an explicit "Skip" category for exactly this case, and this
      // is where that signal actually gets enforced.
      if (entry.category === "Skip") {
        skippedOffTopic++;
        return;
      }

      results.push({
        id: hashId(cluster.representative.link),
        headline: entry.headline,
        summary: entry.summary,
        category: entry.category,
        sourceName: cluster.representative.sourceName,
        sourceUrl: cluster.representative.link,
        publishedAt: cluster.latestPubDate,
        clusterOf: cluster.clusterOf,
        sources: [...new Set(cluster.sources)],
        whyItMatters: entry.whyItMatters || null,
      });
    });

    if (unmatchedInBatch > 0) unmatchedEntries += unmatchedInBatch;
  }
  return { summarized: results, batchesTotal: batches.length, batchesFailed, skippedOffTopic, unmatchedEntries, lastRawSnippet };
}

// ---------------------------------------------------------------------------
// 4b. Daily briefing — one cross-story synthesis paragraph, citation-linked
// back to real stories via {{ref:ID}} markers. Ported from the pattern
// earth.stluker.com already ships (renderBriefingWithRefs on the frontend
// resolves these into numbered <a> links against a real reference map built
// from that day's actual events) — same idea here, just referencing space
// story ids instead of quake/wildfire/volcano ids. One extra Haiku call per
// run (small, capped at 15 input items, 300 max_tokens output).
// ---------------------------------------------------------------------------

export async function generateBriefing(summarized, env, fetchImpl = fetch) {
  if (!summarized || summarized.length === 0) return { briefing: null, error: null };

  try {
    // Newest-first, capped at 15 — same recency-over-volume principle as the
    // rest of this file. The model picks which 2-4 of these actually matter;
    // it doesn't need the full day's list to do that well.
    const top = summarized
      .slice()
      .sort((a, b) => new Date(b.publishedAt || 0) - new Date(a.publishedAt || 0))
      .slice(0, 15)
      .map((s) => ({ id: s.id, headline: s.headline, category: s.category }));

    const prompt = [
      'You are writing the opening "Today\'s Briefing" paragraph for a space-news',
      "dashboard. Given the list of today's story headlines below, write ONE 2-4",
      "sentence synthesis of what's actually happening in space today, in your own",
      "words — prioritize the most significant 2-4 items rather than listing",
      "everything. Reference specific stories inline using {{ref:ID}} immediately",
      "after the relevant clause, using only the exact id values given below —",
      "never invent an id. Return ONLY the paragraph text: no markdown, no preamble,",
      "no quotation marks.",
      "",
      "Stories:",
      JSON.stringify(top, null, 2),
    ].join("\n");

    const res = await fetchImpl("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 300,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    if (!res.ok) throw new Error(`Anthropic API error ${res.status}: ${await res.text()}`);
    const data = await res.json();
    const text = data.content.map((b) => b.text || "").join("").trim();
    return { briefing: text || null, error: text ? null : "Empty response from Haiku" };
  } catch (err) {
    return { briefing: null, error: String(err.message || err) };
  }
}

function buildSummarizationPrompt(batch) {
  const items = batch.map((c, i) => ({
    index: i,
    title: c.representative.title,
    description: c.representative.description,
    sourceCount: c.clusterOf,
    otherSources: c.sources.slice(1),
  }));

  return [
    "You are writing entries for a space-news dashboard. For each story below,",
    "write an ORIGINAL headline and a 1-2 sentence ORIGINAL summary in your own words —",
    "never copy phrasing from the title/description verbatim. Also write one honest,",
    "plain-language sentence for \"whyItMatters\" — not hype, just what actually",
    "changed or what a reader should understand because of this. Also assign one",
    "category: Launch, Astronomy, Mission, Policy, Industry, or Skip.",
    "",
    "Use Skip if the story is NOT meaningfully about space exploration, astronomy,",
    "spaceflight, or the space industry — some source feeds mix in unrelated general-",
    "science trivia, entertainment reviews, or human-interest filler (e.g. animal",
    "biology, history unrelated to spaceflight, TV show recaps, geography trivia).",
    "When in doubt, Skip rather than force an unrelated story into a space category.",
    "For a Skip item, headline/summary/whyItMatters can be empty strings — they won't",
    "be used.",
    "",
    "CRITICAL: you MUST return exactly one object per input item below, including",
    "Skip items — never omit an entry, even one you're marking Skip. Each object",
    "MUST include the item's original \"index\" value unchanged, so entries can be",
    "matched back to their source item regardless of output order.",
    "",
    "Return ONLY a JSON array, each object with exactly the keys",
    '"index", "headline", "summary", "whyItMatters", "category". No markdown, no preamble.',
    "",
    "Stories:",
    JSON.stringify(items, null, 2),
  ].join("\n");
}

function safeParseJsonArray(text) {
  const cleaned = text.replace(/```json|```/g, "").trim();
  try {
    const parsed = JSON.parse(cleaned);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function hashId(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  }
  return "story-" + Math.abs(h).toString(36);
}

// ---------------------------------------------------------------------------
// 5. Launch Library 2 — upcoming launches
// ---------------------------------------------------------------------------

const LL2_BASE_HEADERS = {
  "User-Agent": "stluker-space-dashboard/1.0 (+https://space.stluker.com)",
  "Accept": "application/json",
};

const LL2_RAW_SNIPPET_LEN = 800;

// CHANGED 2026-07-24 (later same day): the header-less-request fix earlier
// today was necessary but not sufficient — a live /trigger?includeSpace=true
// run confirmed LL2 still returns 429 even with a proper User-Agent and a
// single retry, on both attempts. LL2's free/anonymous tier shares one rate
// limit across every request coming from Cloudflare Workers' IP range
// (everyone's Worker looks like the same caller to LL2), so no amount of
// politeness from one Worker fixes it — a registered API key gives this
// project its own independent limit instead of fighting over the shared
// anonymous one. Falls back to the old anonymous behavior if LL2_API_KEY
// isn't bound, so this doesn't hard-fail a deploy that hasn't set it yet.
function buildLl2Headers(env) {
  const headers = { ...LL2_BASE_HEADERS };
  if (env?.LL2_API_KEY) {
    headers["Authorization"] = `Token ${env.LL2_API_KEY}`;
  }
  return headers;
}

export async function fetchUpcomingLaunches(env, fetchImpl = fetch) {
  const lastLL2Fetch = new Date().toISOString();
  const authenticated = Boolean(env?.LL2_API_KEY);
  const headers = buildLl2Headers(env);
  try {
    const res = await fetchImpl(LL2_UPCOMING_URL, {
      headers,
      cf: { cacheTtl: 0 },
    });

    if (res.status === 429) {
      // One short retry costs a single extra subrequest but avoids treating a
      // transient rate-limit the same as a real outage — LL2 429s on Workers
      // are expected occasionally, not exceptional. Once LL2_API_KEY is
      // bound, this branch should become rare — its own log entries are the
      // signal to check whether the key is actually being sent/valid.
      await new Promise((r) => setTimeout(r, 1500));
      const retry = await fetchImpl(LL2_UPCOMING_URL, {
        headers,
        cf: { cacheTtl: 0 },
      });
      if (!retry.ok) throw new Error(`HTTP ${retry.status} (after 429 retry, authenticated=${authenticated})`);
      const rawText = await retry.text();
      const data = JSON.parse(rawText);
      const { launches } = mapLl2Response(data);
      return {
        launches,
        error: null,
        debug: { lastLL2Fetch, status: retry.status, retried: true, authenticated, raw: rawText.slice(0, LL2_RAW_SNIPPET_LEN) },
      };
    }

    if (!res.ok) throw new Error(`HTTP ${res.status} (authenticated=${authenticated})`);
    const rawText = await res.text();
    const data = JSON.parse(rawText);
    const { launches } = mapLl2Response(data);
    return {
      launches,
      error: null,
      debug: { lastLL2Fetch, status: res.status, retried: false, authenticated, raw: rawText.slice(0, LL2_RAW_SNIPPET_LEN) },
    };
  } catch (err) {
    return {
      launches: [],
      error: String(err.message || err),
      debug: { lastLL2Fetch, status: null, retried: false, authenticated, raw: null },
    };
  }
}

export function mapLl2Response(data) {
  const now = Date.now();
  const sevenDays = now + 7 * 24 * 3600 * 1000;

  const launches = (data.results || [])
    .filter((l) => {
      const t = new Date(l.net).getTime();
      // FIXED 2026-07-24: a missing/malformed `net` (common for early-TBD
      // launches — the same kind Spaceflight Now lists under "TBD") used to
      // produce NaN here, and `NaN >= now` is silently false — dropping a
      // real upcoming launch with no error surfaced anywhere. Include it
      // instead and let the UI's own "TBD" status badge communicate the
      // uncertainty.
      if (Number.isNaN(t)) return true;
      return t >= now && t <= sevenDays;
    })
    .map((l) => ({
      id: "ll2-" + l.id,
      vehicle: l.rocket?.configuration?.name || "Unknown vehicle",
      mission: l.mission?.name || l.name || "Unnamed mission",
      provider: l.launch_service_provider?.name || "Unknown",
      pad: l.pad?.name ? `${l.pad.name}, ${l.pad.location?.name || ""}`.trim() : "TBD",
      windowStart: l.net,
      status: l.status?.abbrev || "TBD",
      source: "Launch Library 2",
    }));

  return { launches, error: null };
}

// ---------------------------------------------------------------------------
// 5b. "Tonight's Sky" — the one feature a generic aggregator structurally can't
// do, since it's grounded in one specific location instead of serving everyone.
// Moon phase is computed locally (no network call, so it always works even if
// every external API is down) using standard synodic-month math. Astronomical
// twilight window comes from sunrise-sunset.org, a free no-key API — if it
// fails, we still return the moon phase alone rather than losing the whole card.
// ---------------------------------------------------------------------------

const STL_LAT = 38.6270;
const STL_LNG = -90.1994;
const SYNODIC_MONTH_DAYS = 29.530588861;
// Reference new moon: 2000-01-06 18:14 UTC (well-known epoch for this calculation)
const KNOWN_NEW_MOON = Date.UTC(2000, 0, 6, 18, 14, 0);

const MOON_PHASE_NAMES = [
  { max: 1.84566, name: "New Moon", emoji: "🌑" },
  { max: 5.53699, name: "Waxing Crescent", emoji: "🌒" },
  { max: 9.22831, name: "First Quarter", emoji: "🌓" },
  { max: 12.91963, name: "Waxing Gibbous", emoji: "🌔" },
  { max: 16.61096, name: "Full Moon", emoji: "🌕" },
  { max: 20.30228, name: "Waning Gibbous", emoji: "🌖" },
  { max: 23.99361, name: "Last Quarter", emoji: "🌗" },
  { max: 27.68493, name: "Waning Crescent", emoji: "🌘" },
  { max: SYNODIC_MONTH_DAYS, name: "New Moon", emoji: "🌑" },
];

export function computeMoonPhase(date = new Date()) {
  const daysSinceNew = ((date.getTime() - KNOWN_NEW_MOON) / 86400000) % SYNODIC_MONTH_DAYS;
  const age = daysSinceNew < 0 ? daysSinceNew + SYNODIC_MONTH_DAYS : daysSinceNew;
  const illumination = Math.round(50 * (1 - Math.cos((2 * Math.PI * age) / SYNODIC_MONTH_DAYS)));
  const phase = MOON_PHASE_NAMES.find((p) => age <= p.max) || MOON_PHASE_NAMES[MOON_PHASE_NAMES.length - 1];
  return { name: phase.name, emoji: phase.emoji, illumination, ageDays: Math.round(age * 10) / 10 };
}

export async function fetchTwilightWindow(fetchImpl = fetch) {
  try {
    const url = `https://api.sunrise-sunset.org/json?lat=${STL_LAT}&lng=${STL_LNG}&formatted=0`;
    const res = await fetchImpl(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (data.status !== "OK") throw new Error(`API status: ${data.status}`);
    return {
      astronomicalTwilightBegin: data.results.astronomical_twilight_begin,
      astronomicalTwilightEnd: data.results.astronomical_twilight_end,
      sunset: data.results.sunset,
      error: null,
    };
  } catch (err) {
    return { astronomicalTwilightBegin: null, astronomicalTwilightEnd: null, sunset: null, error: String(err.message || err) };
  }
}

// ADDED 2026-07-24: open-notify.org's ISS pass predictions (the obvious
// no-key choice) were confirmed shut down — only its current-location
// endpoint still runs. iss-api.polluxlabs.io is the maintained free
// successor: no key, no Patreon, and it goes further than the old API by
// also flagging which passes are actually naked-eye visible (above horizon
// AND outside daylight AND outside Earth's shadow), not just "overhead."
// This closes a real gap: the roadmap named "ISS passes" as part of the
// Phase 1 differentiation feature, but only moon phase/twilight ever
// actually got built. Same fail-soft shape as fetchTwilightWindow — if this
// API is ever down, "tonight" still returns with moon phase intact.
export async function fetchIssPasses(fetchImpl = fetch) {
  try {
    const url = `https://iss-api.polluxlabs.io/iss-pass?lat=${STL_LAT}&lon=${STL_LNG}`;
    const res = await fetchImpl(url, {
      headers: { "User-Agent": "stluker-space-dashboard/1.0 (+https://space.stluker.com)" },
      cf: { cacheTtl: 0 },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    // Cap at 5 — the dashboard only ever needs "next" and "next visible,"
    // not a full week of passes; keeps the KV payload small regardless of
    // how many the API happens to return.
    const passes = (data.passes || []).slice(0, 5).map((p) => ({
      riseTime: p.rise?.time || null,
      riseCompass: p.rise?.compass || null,
      culminationTime: p.culmination?.time || null,
      culminationElevationDeg: p.culmination?.elevation_deg ?? null,
      setTime: p.set?.time || null,
      setCompass: p.set?.compass || null,
      durationSec: p.duration_sec ?? null,
      visible: Boolean(p.visible),
    }));

    return { passes, error: null };
  } catch (err) {
    return { passes: [], error: String(err.message || err) };
  }
}

export async function buildTonightSky(fetchImpl = fetch) {
  const moon = computeMoonPhase();
  // CHANGED 2026-07-24: fetched alongside twilight rather than after it —
  // same "don't let one slow/failed call starve the next" lesson already
  // applied to LL2 vs. RSS feeds earlier today.
  const [twilight, iss] = await Promise.all([
    fetchTwilightWindow(fetchImpl),
    fetchIssPasses(fetchImpl),
  ]);

  let note;
  if (moon.illumination >= 85) {
    note = `${moon.name} tonight (${moon.illumination}% lit) — bright skies will wash out all but the brightest objects.`;
  } else if (moon.illumination <= 15) {
    note = `${moon.name} tonight (${moon.illumination}% lit) — dark skies, good night for fainter objects if it's clear.`;
  } else {
    note = `${moon.name} tonight, ${moon.illumination}% illuminated.`;
  }

  const nextVisiblePass = iss.passes.find((p) => p.visible) || null;

  return {
    location: "St. Louis, MO",
    moonPhase: moon.name,
    moonEmoji: moon.emoji,
    moonIllumination: moon.illumination,
    astronomicalTwilightBegin: twilight.astronomicalTwilightBegin,
    astronomicalTwilightEnd: twilight.astronomicalTwilightEnd,
    note,
    twilightError: twilight.error,
    // ADDED 2026-07-24: raw ISO timestamps, same convention as the twilight
    // fields above — formatting into local time/compass display stays a
    // frontend concern, not baked in here.
    issPasses: iss.passes,
    issNextVisiblePass: nextVisiblePass,
    issError: iss.error,
  };
}

// ---------------------------------------------------------------------------
// 6. Orchestration — the actual task stl-dispatcher calls
// ---------------------------------------------------------------------------

export async function runSpaceIngest(env, fetchImpl = fetch) {
  const startedAt = new Date().toISOString();

  // --- RSS + Launch Library 2, fetched concurrently ---
  // CHANGED 2026-07-21: LL2 used to run last, after all RSS fetches and every
  // Haiku summarization batch — so on the free tier's 50-subrequest cap, it was
  // the one call starved when everything upstream had already used the budget.
  // Fetching it alongside the RSS feeds means launch data is never the casualty
  // of a heavy news day.
  // CHANGED 2026-07-24: added SNAPI alongside the RSS feeds — same recency/
  // subrequest-budget treatment as every other source, just one more entry
  // in the same Promise.all rather than a parallel pipeline to maintain.
  const [feedResults, snapiResult, ll2Result, tonight] = await Promise.all([
    Promise.all(FEEDS.map((f) => fetchFeed(f, fetchImpl))),
    fetchSpaceflightNewsArticles(fetchImpl),
    fetchUpcomingLaunches(env, fetchImpl),
    buildTonightSky(fetchImpl),
  ]);

  // Treat SNAPI as just one more "feed result" from here on — same shape,
  // same downstream handling, no special-casing needed.
  feedResults.push(snapiResult);

  const sourcesErrored = feedResults.filter((r) => r.error).map((r) => ({ name: r.feed.name, error: r.error }));
  let { launches, error: ll2Error, debug: ll2Debug } = ll2Result;
  let launchesStale = false;
  let launchesAsOf = startedAt;

  // ADDED 2026-07-24: per-source diagnostics ({name, count, error}) for all 14
  // feeds + LL2, ported from the same pattern already shipped in stl-sports
  // and intel. sourcesErrored above is kept as-is (existing consumers may read
  // it), diagnostics is the new, complete picture — including sources that
  // succeeded with a zero count, which sourcesErrored alone can't show.
  const diagnostics = feedResults.map((r) => ({
    name: r.feed.name,
    count: r.items.length,
    error: r.error,
  }));

  if (ll2Error) {
    sourcesErrored.push({ name: "Launch Library 2", error: ll2Error });

    // CHANGED 2026-07-23: an LL2 429/network failure used to silently wipe the
    // dashboard's launch list to empty, even though yesterday's data was still
    // mostly valid. LL2's free tier rate-limits shared-IP platforms like
    // Cloudflare Workers more aggressively than typical hosts, so a 429 here is
    // expected occasionally, not exceptional — carry forward the last KV write's
    // still-future launches instead of erasing real data over a transient failure.
    try {
      const prevRaw = await env.SPACE_KV.get(KV_KEY);
      if (prevRaw) {
        const prev = JSON.parse(prevRaw);
        const now = Date.now();
        const stillFuture = (prev.launches?.next7Days || []).filter(
          (l) => l.windowStart && new Date(l.windowStart).getTime() >= now
        );
        if (stillFuture.length > 0) {
          launches = stillFuture;
          launchesStale = true;
          launchesAsOf = prev.meta?.lastUpdated || null;
        }
      }
    } catch (e) {
      // No previous KV entry, or it was malformed — fall through with the
      // empty launches list from this run rather than throwing.
    }
  }

  diagnostics.push({
    name: "Launch Library 2",
    count: launches.length,
    authenticated: ll2Debug?.authenticated ?? false,
    error: launchesStale ? `${ll2Error} (serving ${launches.length} carried-forward stale launches)` : ll2Error,
  });

  diagnostics.push({
    name: "ISS Pass API",
    count: tonight.issPasses?.length ?? 0,
    error: tonight.issError,
  });

  const allItemsRaw = feedResults.flatMap((r) =>
    r.items.map((item) => ({ ...item, sourceName: r.feed.name, defaultCategory: r.feed.category }))
  );

  // Drop anything older than the recency window BEFORE clustering/summarizing —
  // the dashboard only ever shows "last 24h" / "next 7 days" anyway, so this is
  // the main lever against tripping the subrequest cap on a heavy news day.
  // Items with no parseable pubDate are kept (missing a date isn't evidence an
  // item is old — safer to include and let the "last 24h" filter downstream
  // exclude it from that specific bucket than to silently drop it here).
  const recencyCutoff = Date.now() - INGEST_RECENCY_HOURS * 3600 * 1000;
  const allItems = allItemsRaw.filter((item) => !item.pubDate || new Date(item.pubDate).getTime() >= recencyCutoff);

  const clusters = clusterItems(allItems);
  const {
    summarized,
    batchesTotal: summaryBatchesTotal,
    batchesFailed: summaryBatchesFailed,
    skippedOffTopic,
    unmatchedEntries,
    lastRawSnippet: summaryLastRawSnippet,
  } = await summarizeClusters(clusters, env, fetchImpl);

  // ADDED 2026-07-25: visibility into the clustering→summarization step —
  // the exact gap that made the Jul 25 zero-stories incident undiagnosable
  // after the fact (real fetch counts, zero summarized output, no error
  // surfaced anywhere). clustersFound vs. count tells you immediately
  // whether items existed but failed to summarize (this bug) vs. genuinely
  // nothing was fetched (a source-level problem, already visible above).
  // skippedOffTopic surfaces the new Skip-category filter's effect — e.g.
  // SpaceDaily mixing non-space filler into its feed — so a source that's
  // becoming mostly off-topic noise is visible here rather than a mystery.
  // unmatchedEntries surfaces the follow-on bug the Skip filter exposed:
  // Haiku omitting an entry rather than returning it with category:"Skip"
  // used to silently misalign every later item in that batch (a real
  // headline paired with the wrong article's sourceUrl, confirmed live
  // Jul 25) — matching by explicit index (see summarizeClusters) fixes the
  // misalignment, and this count means a dropped-but-not-misattributed item
  // is visible here instead of invisible.
  diagnostics.push({
    name: "Summarization",
    count: summarized.length,
    clustersFound: clusters.length,
    batchesTotal: summaryBatchesTotal,
    batchesFailed: summaryBatchesFailed,
    skippedOffTopic,
    unmatchedEntries,
    error: summaryBatchesFailed > 0
      ? `${summaryBatchesFailed}/${summaryBatchesTotal} batch(es) produced no parseable output — possible truncation. Raw snippet: ${summaryLastRawSnippet}`
      : null,
  });

  // Split summarized items into dashboard buckets by category
  const breaking = [];
  const astronomy = [];
  const missions = [];
  const policy = [];

  for (const item of summarized) {
    switch (item.category) {
      case "Astronomy":
        astronomy.push(item);
        break;
      case "Mission":
        missions.push(item);
        break;
      case "Policy":
      case "Industry":
        policy.push(item);
        break;
      default:
        breaking.push(item);
    }
  }

  // Everything also rolls into "breaking" (last 24h), sorted newest first
  const cutoff = Date.now() - 24 * 3600 * 1000;
  const last24h = summarized
    .filter((i) => i.publishedAt && new Date(i.publishedAt).getTime() >= cutoff)
    .sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));

  // ADDED 2026-07-24: one cross-story synthesis paragraph, same "Today's
  // Briefing" pattern earth.stluker.com already ships. Sourced from the full
  // summarized set (not just last24h) so a quiet-news day still gets a real
  // briefing rather than an empty one just because nothing happened to land
  // in the last 24h specifically.
  const { briefing, error: briefingError } = await generateBriefing(summarized, env, fetchImpl);
  diagnostics.push({ name: "Daily Briefing", count: briefing ? 1 : 0, error: briefingError });

  const blob = {
    meta: {
      version: startedAt,
      lastUpdated: startedAt,
      // CHANGED 2026-07-23: this used to be a hardcoded FEEDS.length + 1 regardless
      // of outcome — meaning it read "14 sources checked" even on a run where LL2
      // 429'd and a feed 525'd. Now it's the actual successful-fetch count, with
      // the failures still visible separately in sourcesErrored.
      // CHANGED 2026-07-24: +2, not +1 — SNAPI joined feedResults as an extra
      // "feed" (already counted via feedResults.length below), plus LL2
      // counted separately as before. Using feedResults.length instead of the
      // old FEEDS.length keeps this correct automatically if more non-RSS
      // sources get pushed into feedResults later, rather than needing a
      // manual +N bump each time.
      sourcesPolled: feedResults.length + 1 - sourcesErrored.length,
      sourcesAttempted: feedResults.length + 1,
      sourcesErrored,
      // ADDED 2026-07-24: complete per-source {name, count, error} for all 14
      // feeds + LL2 — ported from the stl-sports/intel pattern. Answers "why
      // is X empty" from a single KV read instead of a log-tailing session.
      diagnostics,
    },
    // ADDED 2026-07-24: not surfaced in the UI, queryable only — cheap
    // insurance for diagnosing future LL2-specific failures (shape changes,
    // unexpected fields, a 200 with an empty/malformed result set) without
    // needing a fresh live test call.
    debug: {
      ll2: ll2Debug || null,
    },
    launches: { next7Days: launches, stale: launchesStale, asOf: launchesAsOf },
    breaking: { last24h },
    astronomy,
    missions,
    policy,
    tonight,
    briefing,
  };

  await env.SPACE_KV.put(KV_KEY, JSON.stringify(blob));

  return blob;
}
