/**
 * podcast-ingest.js  --  lives in C:\Users\pdluk\stl-dispatcher\
 *
 * Daily "Orbit + Ground" audio briefing: reads the SPACE_KV and EARTH_KV blobs
 * that spaceIngest/earthIngest already wrote earlier in the same dispatcher run,
 * writes one script via a single Haiku call, renders it with ElevenLabs, stores
 * the MP3 in R2, and appends an episode entry to PODCAST_KV.
 *
 * Deliberate design constraints (each one traces to a past incident in
 * stluker-infrastructure.md -- do not remove without reading the note):
 *
 *  - ZERO new external news fetches. This is a consumer of existing KV data,
 *    not a 3rd ingestion pipeline. If a source blob is missing or stale it
 *    degrades and says so in diagnostics rather than going and re-fetching.
 *  - ONE Anthropic call, max_tokens capped, NO web_search tool.
 *    (fire-api, Jul 22: 5 uncapped web-search call sites = $14.90/mo surprise.)
 *  - Hard monthly ElevenLabs credit budget enforced BEFORE the TTS call, from a
 *    counter in KV. This is the same shape as /trigger's includeBucket gate.
 *  - Idempotent per UTC day. A second run on the same day is a no-op.
 *    (stl-bucket, Jul 22: duplicate Friday cron fired fetchAndParse twice.)
 *  - ASCII-only sanitizer on everything before it leaves this file.
 *    (earth, Jul 23: PowerShell 5.1 mangled an em dash into a mojibake string.
 *    Separately, TTS engines mispronounce smart quotes and unicode dashes.)
 */

const SCRIPT_MODEL = 'claude-haiku-4-5-20251001';
const ANTHROPIC_VERSION = '2023-06-01';

// ---- Cost controls. These are the whole cost story; tune here, nowhere else.
const TARGET_WORDS = 900;            // ~6 min at a 150 wpm read
const MAX_SCRIPT_TOKENS = 1400;      // hard ceiling on the Haiku response
const MAX_SCRIPT_CHARS = 7000;       // hard ceiling on what we hand to TTS
const MONTHLY_CREDIT_BUDGET = 92000; // of 100k on Creator; 8k reserved headroom
const TTS_MODEL = 'eleven_flash_v2_5'; // 0.5 credits/char vs 1.0 on multilingual
const CREDITS_PER_CHAR = 0.5;          // must match TTS_MODEL above
const VOICE_SETTINGS = { stability: 0.45, similarity_boost: 0.75, speed: 1.0 };

const STORIES_FROM_SPACE = 4;
const STORIES_FROM_EARTH = 3;

const SYSTEM_PROMPT = `You write a short daily audio briefing on space and earth science, listened to by one person on their morning drive. Warm, curious, personable. Someone who finds this stuff genuinely interesting talking to someone who also does.

STRUCTURE
1. The episode opens with a fixed welcome line stating today's date, supplied to you already prepended -- do not write your own greeting or restate the date. Your opening picks up right after it: two or three sentences naming what today is actually about, the one thing that makes today different from yesterday. Warm but not chirpy. Do not repeat "welcome" or "today" as your first word -- the line before yours already did that work.
2. The stories. See SELECTION AND FLOW below - this is the part that most determines whether the episode sounds alive or like a list being read.
3. The reflection. See its own section below.

SELECTION AND FLOW - read this section twice, it's the one that matters most
You will be handed 5-7 candidate stories, mixed together, not grouped by topic. You do not have to use all of them.

STEP ONE - find the organizing idea BEFORE you choose anything. Scan all the candidates for one thing that connects three or more of them: a mechanism, a place, a tension, an escalation, a contradiction, a before-and-after. This is not decoration you add at the end - it is the reason you're choosing these stories over the others. If nothing genuinely connects three or more stories, pick the two that connect most strongly and let the rest be a single plain aside, not a forced member of the theme.

STEP TWO - choose 3 to 5 stories using that idea as the filter, not "which are individually most important." Cut whichever ones don't serve it - a story that's individually interesting but doesn't fit is exactly what makes a briefing feel like a list. Fewer stories covered well, all pulling the same direction, beats more stories covered thinly.

STEP THREE - let the organizing idea show up early, not just in the reflection. The opening line or the first segue should already be operating on it, even if you don't name it outright yet. If the listener only learns why these stories belong together in the closing line, the middle of the episode already read as disconnected while they were listening to it - the reflection is a place to land the idea, not the first place you reveal it.

Order the stories by what they share, not by which pipeline they came from. A segue is not a sentence that mentions both stories. It's a sentence where the second story is already implied by how you ended the first one. If you have to write "meanwhile," "also," "in addition," "elsewhere," or "speaking of," stop - that word is doing the work your sentence structure should be doing. Delete the connector and rewrite the boundary between the two stories so it doesn't need one. This is checked after generation - a script full of these connector words reads as a failure of this instruction, not a stylistic choice.

Vary the shape of each story's treatment. Not every item gets the same two-sentence setup-then-fact rhythm - that's what makes something read as a list even without list punctuation. Let one story get four sentences because it's genuinely interesting and another get one because there's nothing more to say. A story can open mid-thought. A story can be one plain sentence with no build-up at all if that's honestly all it needs.

Worked example, illustrative only, not to be reused verbatim:
LIST-LIKE (avoid this): "A magnitude six point four earthquake struck offshore Indonesia. Meanwhile, wildfires continued to burn in Alberta. In other news, Kilauea remains active."
FLOWING (aim for this): "Offshore Indonesia took the bigger shock overnight, magnitude six point four, deep enough and far out enough that nobody's counting damage yet. Alberta's fire is a slower version of the same problem - nothing sudden, just four days of not stopping. Kilauea doesn't even have that arc. It's not building toward anything. It's just still going, the way it has been for years."
Notice the second version has zero transition words and the connection is doing the work: sudden versus slow versus indefinite, as a real observation, not three unrelated facts filed one after another. Also notice the organizing idea here - different timescales of the same restlessness - is legible from the first sentence, not just at a summary at the end.

VOICE
Plain and specific. Short sentences next to long ones. Comfortable being interested in something without justifying why. You can be dry. You can let a thing be strange. You are not performing enthusiasm and you are not narrating a nature documentary.

THE INFERENCE RULE - the one that matters
Two standards apply to the stories section.

LOAD-BEARING FACTS may only appear if supplied, exactly as supplied. Never estimate, round into a new number, convert, or infer:
  magnitudes, depths, distances, counts, dates, clock times, durations,
  casualty or damage figures, causes, place names, agency names, alert levels,
  what happens next.
If a load-bearing fact was not supplied, the sentence needing it does not get written. Say less instead.

TEXTURE is yours: rhythm, ordering, emphasis, phrasing, segues, and observations about how supplied facts sit next to each other. You may note that six satellite passes in four days means a thing is moving. You may not decide where it is moving.

The test: could a listener act on this, or be wrong about the world because of it? Then it is load-bearing.

THE REFLECTION - different rules, read carefully
Sixty to a hundred words at the end. A thought about the earth, or our place in things, that grows out of TODAY'S material specifically. Not a general meditation with today's news pasted on top.

It must contain ZERO load-bearing facts. No numbers, no dates, no place names, no agency names, no measurements. Nothing that could be checked and found wrong. It reflects on what you already said; it does not add anything new about the world.

What makes it work: it is grounded in one concrete thing from the briefing and stays with that one thing. A specific noticing, not a summary of all of it.

This is a continuation of the organizing idea from STEP ONE above, not the first place you introduce it. If the listener has been following the throughline since the opening, the reflection should feel like the last turn of a thought already in motion. Avoid naming it flatly ("the theme running through this is X") - that tells the listener what to have noticed instead of trusting that they did. Extend the idea one step further instead of summarizing it.

Banned outright, these are the corny failure modes:
- rhetorical questions of any kind
- telling the listener what to do or feel: "remember that", "take a moment", "next time you look up"
- "in the grand scheme", "puts things in perspective", "makes you realize how small", "we are all stardust", "pale blue dot", "the universe is vast"
- ending on a swell or a payoff line. It should land flat and stop.
- three-part rising lists
- the word "humbling"
- naming the throughline explicitly ("the theme running through this is...", "what connects all of this is...", "the through-line here is...")

It is allowed to be uncertain, to sit with something unresolved, or to be about something small rather than something cosmic. A reflection about how long a volcano has been doing the same thing is better than a reflection about infinity.

THE QUOTE
You will be handed one quotation and the name of the person who said it. Work it into the reflection.

- Reproduce the quotation EXACTLY as given. Not one word changed, added, dropped, or reordered. Do not modernize it, trim it, or fold it into your own sentence.
- Attribute it to the name given, and only that name. Never guess at a date, a book, a mission, or a context for it.
- If the quotation does not fit the day honestly, use it anyway but keep the connection light. A strained link is better than a rewritten quote.
- It does not have to end the reflection. Landing on it is the obvious move and usually the weakest one; letting it sit in the middle and then saying one more plain thing after it is better.
- Do not say "as somebody once said", "in the words of", or "this reminds me of a quote". Just say the name and the line.

DISASTERS
Plainly reported. No spin, no silver lining, no "fortunately", no admiring the response. Never make a death into a lesson, and never build the reflection out of one.

READ-ALOUD FORMAT
Plain prose. No markdown, headers, bullets, labels, emoji, citations, URLs. Numbers spoken naturally - "magnitude six point four", "about seventy miles offshore". NASA and ISS as-is; USGS as "the U.S. Geological Survey", EONET as "the NASA Earth Observatory tracker". ASCII punctuation only: straight quotes, hyphens, no em dashes.

LENGTH
About ${TARGET_WORDS} words total including the reflection. Under 1000.

COVER IMAGE PROMPT
Write one image-generation prompt, 30-60 words, for a piece of cover art for today's episode. It should be inspired by whichever single story or image in today's material is most visually striking, not an attempt to illustrate everything you covered.

Hard rules for this prompt specifically:
- NEVER describe a real, named, identifiable person - no astronaut by name, no agency official, no scientist. Describe the phenomenon, the place, the instrument, or the scene instead. "A volcano's summit glowing faintly against a night sky" not "Dr. Smith observing the eruption."
- NEVER request a real logo, insignia, flag, or trademarked visual identity (no NASA meatball, no mission patches, no agency seals).
- NEVER request text, words, numbers, or labels rendered in the image.
- Describe it as a scene or a piece of art, not a diagram or infographic - painterly, photographic, or illustrative language, not chart/graph language.
- Ground it in something real from today's material (a place, a phenomenon, a described scale or color) rather than inventing generic "space background" filler.

DISASTER IMAGERY: if the most striking story is a disaster that harmed people, do not illustrate the harm. Depict the underlying natural phenomenon (the storm system, the fault line, the ash plume) without human suffering, wreckage, or victims in frame.

OUTPUT
Return ONLY a JSON object, no fences:
{"script":"<opening and stories, no reflection>","reflection":"<the reflection, containing the supplied quotation verbatim>","imagePrompt":"<the cover image prompt>","claims":[{"text":"<load-bearing fact as stated>","sourceId":"<exact id it came from>"}]}
Every load-bearing fact in the script gets a claims entry. If you cannot name the id a fact came from, the fact does not belong in the script.`;

/* ------------------------------------------------------------------ helpers */

/** Strip everything that TTS mispronounces or that PowerShell 5.1 can corrupt. */
function toSpeakableAscii(text) {
  if (!text) return '';
  return String(text)
    // Fold accents to their base letter FIRST -- otherwise the non-ASCII strip
    // below silently eats them ("Curiosite" is fine, "Curiosit" is not, and a
    // researcher's surname losing a letter is worse than either).
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\u00DF/g, 'ss')
    .replace(/\u00C6/g, 'AE').replace(/\u00E6/g, 'ae')
    .replace(/\u0152/g, 'OE').replace(/\u0153/g, 'oe')
    .replace(/\u00D8/g, 'O').replace(/\u00F8/g, 'o')
    .replace(/\u0110/g, 'D').replace(/\u0111/g, 'd')
    .replace(/[\u2018\u2019\u201A\u201B]/g, "'")
    .replace(/[\u201C\u201D\u201E\u201F]/g, '"')
    .replace(/[\u2013\u2014\u2015]/g, '-')
    .replace(/\u2026/g, '...')
    .replace(/\u00A0/g, ' ')
    .replace(/[\u2022\u00B7]/g, '-')
    .replace(/[^\x20-\x7E\n]/g, '')      // drop any remaining non-ASCII
    .replace(/\[\d+\]/g, '')             // stray citation markers
    .replace(/\{\{ref:[^}]*\}\}/g, '')   // earth/space briefing ref markers
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function utcDayKey(now = new Date()) {
  return now.toISOString().slice(0, 10);           // 2026-07-24
}
function utcMonthKey(now = new Date()) {
  return now.toISOString().slice(0, 7);            // 2026-07
}

/**
 * Read SPACE_KV. Confirmed live shape (Jul 27, 2026, via direct curl against
 * space.stluker.com/data.json -- NOT a top-level stories[]/items[] array,
 * which is what this function originally assumed and which silently
 * returned count:0/ok:true every time, exactly the "looks fine, wrong
 * result" failure this project keeps hitting):
 *
 *   { meta: { version, lastUpdated, sourcesPolled, sourcesErrored[] },
 *     launches: { next7Days[], stale, asOf },
 *     breaking: { last24h[] },            // the actual top headlines
 *     astronomy: [...], missions: [...], policy: [...],  // same stories,
 *                                          // reorganized by category --
 *                                          // heavily overlapping ids with
 *                                          // breaking.last24h, not additive
 *     tonight: { location, moonPhase, issNextVisiblePass, ... },
 *     briefing: string }                  // already has {{ref:ID}} markers
 *
 * Stories are deduped by id across breaking/astronomy/missions/policy before
 * being handed to buildSourceDigest, since the same story legitimately
 * appears in both breaking.last24h and its category array.
 */
async function readSourceBlob(ns, key, label) {
  if (!ns) return { label, ok: false, error: 'KV binding not present', stories: [] };
  try {
    const raw = await ns.get(key);
    if (!raw) return { label, ok: false, error: 'key empty', stories: [] };
    const data = JSON.parse(raw);

    const byId = new Map();
    for (const arr of [data.breaking?.last24h, data.astronomy, data.missions, data.policy]) {
      if (!Array.isArray(arr)) continue;
      for (const s of arr) {
        if (s?.id && !byId.has(s.id)) {
          byId.set(s.id, {
            id: s.id,
            title: s.headline || '',
            summary: s.whyItMatters || s.summary || '',
            kind: (s.category || '').toLowerCase(),
          });
        }
      }
    }
    const stories = [...byId.values()];

    return {
      label,
      ok: true,
      lastUpdated: data.meta?.lastUpdated || data.meta?.version || null,
      count: stories.length,
      stories,
      raw: data,
    };
  } catch (err) {
    return { label, ok: false, error: `parse failed: ${err.message}`, stories: [] };
  }
}

/**
 * Read EARTH_KV. Confirmed live shape (Jul 27, 2026, via direct curl against
 * earth.stluker.com/data.json -- NOT the generic stories[] shape this file
 * originally assumed):
 *
 *   { meta: { generatedAt, warnings[] },
 *     pulse: { score, label },
 *     briefing: string|null,               // already has {{ref:ID}} markers
 *     nmsz: { windowHours, eventCount, events[], status },
 *     yellowstone: { windowHours, eventCount, events[], status },
 *     quakes: { significant[], count },
 *     surfaceEvents: { wildfires[], storms[], ice[] },
 *     volcanoes: { reportWeekOf, entries[], fetchedAt, schemaVersion },
 *     whyItMatters: { [id]: string } }     // top-level sibling map, not per-story
 *
 * This reader normalizes all of that into the same
 * { ok, count, lastUpdated, events[], raw } shape readSourceBlob returns for
 * space, so buildSourceDigest can treat both sources uniformly. `events[]`
 * items carry a synthetic `id` when the source data doesn't provide one
 * (nmsz/yellowstone/quakes entries), so whyItMatters lookups and the podcast's
 * own claims-audit validIds set both resolve correctly.
 */
async function readEarthBlob(ns, key) {
  const label = 'earth';
  if (!ns) return { label, ok: false, error: 'KV binding not present', events: [] };
  try {
    const raw = await ns.get(key);
    if (!raw) return { label, ok: false, error: 'key empty', events: [] };
    const data = JSON.parse(raw);
    const why = data.whyItMatters || {};
    const events = [];

    // Regional watch panels -- each event needs a synthetic id since the
    // source objects here don't carry one of their own. Field names for
    // these event objects have never been confirmed against real non-quiet
    // data (every live payload seen so far has status:"quiet", events:[]),
    // so this tries several plausible field names and falls back to nothing
    // rather than assuming a shape -- condense() below drops the story
    // entirely if this ends up empty, rather than handing the model a
    // title with no fact behind it (confirmed Jul 29: the model correctly
    // refused to write about exactly this shape of placeholder).
    for (const [region, block] of [['nmsz', data.nmsz], ['yellowstone', data.yellowstone]]) {
      if (!block || !Array.isArray(block.events)) continue;
      block.events.forEach((ev, i) => {
        const id = ev.id || `${region}-${i}`;
        const facts = [];
        if (ev.magnitude || ev.mag) facts.push(`magnitude ${ev.magnitude || ev.mag}`);
        if (ev.depth || ev.depthKm) facts.push(`depth ${ev.depth || ev.depthKm} km`);
        if (ev.place || ev.location || ev.region) facts.push(String(ev.place || ev.location || ev.region));
        if (ev.time || ev.date) {
          const d = new Date(ev.time || ev.date);
          if (!Number.isNaN(d.getTime())) facts.push(d.toISOString().slice(0, 10));
        }
        events.push({
          id, kind: region, title: ev.title || ev.summary || `${region} event`,
          summary: ev.description || ev.summary || facts.join(', '),
          whyItMatters: why[id] || '',
        });
      });
    }

    // Significant quakes.
    for (const q of (data.quakes?.significant || [])) {
      const id = q.id || `quake-${q.title || q.place || Math.random().toString(36).slice(2, 8)}`;
      events.push({
        id, kind: 'earthquake', title: q.title || q.place || 'Significant earthquake',
        summary: q.description || '', whyItMatters: why[id] || '',
      });
    }

    // Wildfires / storms / ice -- these DO carry real EONET ids already, but
    // confirmed live data shows NO description field on these objects (only
    // id/title/date/url/lat/lon). Build a minimal factual summary from the
    // fields that actually exist -- date and location -- rather than leaving
    // summary empty, which produced a title-only entry with nothing for the
    // model to safely report (the exact failure from Jul 29).
    const se = data.surfaceEvents || {};
    for (const kind of ['wildfires', 'storms', 'ice']) {
      for (const ev of (se[kind] || [])) {
        const facts = [];
        if (ev.description) {
          facts.push(ev.description);
        } else {
          if (typeof ev.lat === 'number' && typeof ev.lon === 'number') {
            facts.push(`last tracked near ${Math.abs(ev.lat).toFixed(1)} deg ${ev.lat >= 0 ? 'N' : 'S'}, ${Math.abs(ev.lon).toFixed(1)} deg ${ev.lon >= 0 ? 'E' : 'W'}`);
          }
          if (ev.date) {
            const d = new Date(ev.date);
            if (!Number.isNaN(d.getTime())) facts.push(`as of ${d.toISOString().slice(0, 10)}`);
          }
        }
        events.push({
          id: ev.id, kind, title: ev.title || '',
          summary: facts.join(', '), whyItMatters: why[ev.id] || '',
        });
      }
    }

    // Volcanoes -- weekly GVP report, ids already real (gvp-etna-...).
    for (const v of (data.volcanoes?.entries || [])) {
      events.push({
        id: v.id, kind: 'volcano', title: `${v.name}, ${v.country} -- ${v.status}`,
        summary: (v.description || '').trim(), whyItMatters: why[v.id] || '',
      });
    }

    return {
      label, ok: true,
      lastUpdated: data.meta?.generatedAt || null,
      count: events.length,
      events,
      pulse: data.pulse || null,
      briefingRaw: data.briefing || '',   // already has {{ref:ID}} markers -- reusable as-is
      raw: data,
    };
  } catch (err) {
    return { label, ok: false, error: `parse failed: ${err.message}`, events: [] };
  }
}

/** Freshness check -- a stale blob is worse than a missing one, silently. */
function ageHours(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  return Math.round((Date.now() - t) / 3600000);
}

/**
 * Trim a story down to just what the model needs. Keeps the prompt small.
 *
 * Returns null -- and the story is dropped from the digest entirely -- if
 * there is no usable fact beyond a bare title. A title-only line ("nmsz
 * event", "Hurricane Fausto" with nothing after it) gives the model no
 * material to safely report and no way to comply with the inference rule
 * except by inventing something or refusing outright. Confirmed Jul 29: the
 * model did the right thing and refused, but the pipeline should never have
 * handed it an empty story in the first place. This is the backstop -- even
 * if a future source schema change reintroduces empty summaries, the story
 * silently drops here rather than reaching the prompt as a placeholder.
 */
function condense(story, max) {
  const title = toSpeakableAscii(story.title || story.headline || '');
  const summary = toSpeakableAscii(story.summary || story.whyItMatters || story.description || '');
  if (!title) return null;
  if (!summary.trim()) return null;   // no fact to report -- drop, don't guess
  return `- id=${story.id} ${title}: ${summary.slice(0, max)}`;
}

/**
 * Whether a story/event carries any fact at all beyond a bare title. Mirrors
 * condense()'s own emptiness check exactly -- kept as one function so the
 * two can never drift out of sync with each other.
 */
function hasReportableFact(story) {
  const summary = (story.summary || story.whyItMatters || story.description || '').toString().trim();
  return summary.length > 0;
}

/**
 * Earth events are picked in a fixed priority order rather than "first N in
 * array order" -- nmsz/yellowstone are regionally relevant (St. Louis) and
 * should surface even when a volcano list of 24 entries would otherwise
 * crowd them out. Quakes and active storms outrank routine GVP "continuing
 * activity" status entries; "new eruptive activity" outranks "continuing".
 *
 * Factless events are dropped BEFORE ranking, not after. Confirmed Jul 29:
 * ranking-then-slicing-then-dropping let nmsz/yellowstone placeholders with
 * no actual data (weight 100, the highest) fill the slice quota and then
 * get discarded by condense() downstream, silently squeezing out a real,
 * factual volcano story that should have taken that slot instead.
 */
function pickTopEarthEvents(earth, max) {
  const weight = (e) => {
    if (e.kind === 'nmsz' || e.kind === 'yellowstone') return 100;   // regional, always surface
    if (e.kind === 'earthquake') return 90;
    if (e.kind === 'storms') return 70;
    if (e.kind === 'wildfires') return 60;
    if (e.kind === 'volcano') {
      return /new eruptive/i.test(e.title) ? 55 : 20;                // new > continuing/unrest
    }
    if (e.kind === 'ice') return 30;
    return 10;
  };
  return earth.events
    .filter(hasReportableFact)
    .sort((a, b) => weight(b) - weight(a))
    .slice(0, max);
}

function buildSourceDigest(space, earth) {
  const lines = [];
  const spaceItems = space.stories.filter(hasReportableFact).slice(0, STORIES_FROM_SPACE)
    .map((s) => condense(s, 400)).filter(Boolean);
  const earthItems = pickTopEarthEvents(earth, STORIES_FROM_EARTH)
    .map((s) => condense(s, 400)).filter(Boolean);

  // Deliberately NOT split into "SPACE STORIES" / "EARTH EVENTS" blocks.
  // That split used to be structural in the input itself, meaning every
  // episode had a hard seam baked in before the model wrote a word: all
  // space stories, then all earth stories, regardless of what would actually
  // flow best together. One pool lets the model order and select across both
  // freely. The SOURCE tag is for the model's own reference (which pipeline a
  // fact came from), not a presentation grouping -- it's told not to sort by it.
  lines.push("TODAY'S SOURCE MATERIAL (order and select freely -- do not group by SOURCE tag; it's provenance, not a section header):");
  const allItems = [...spaceItems, ...earthItems];
  if (allItems.length) {
    lines.push(...spaceItems.map((l) => l.replace('- id=', '- SOURCE=space id=')));
    lines.push(...earthItems.map((l) => l.replace('- id=', '- SOURCE=earth id=')));
  } else {
    lines.push('- (no stories available today)');
  }

  // nmsz/yellowstone "quiet" status is itself worth knowing even with zero
  // events -- it's the difference between "nothing happened" and "no data."
  const quietNotes = [];
  if (earth.raw?.nmsz?.status === 'quiet' && !earthItems.some((l) => l.includes('id=nmsz'))) {
    quietNotes.push('New Madrid seismic zone: quiet, no events in the last 24 hours.');
  }
  if (earth.raw?.yellowstone?.status === 'quiet' && !earthItems.some((l) => l.includes('id=yellowstone'))) {
    quietNotes.push('Yellowstone: quiet, no events in the last 24 hours.');
  }
  if (quietNotes.length) {
    lines.push('');
    lines.push(`REGIONAL STATUS (mention only if it fits naturally, do not force it): ${quietNotes.join(' ')}`);
  }

  // Optional extras the space pipeline already computes -- free to include.
  const sky = space.raw?.tonight;
  if (sky) {
    const bits = [];
    if (sky.moonPhase) bits.push(`moon phase ${toSpeakableAscii(sky.moonPhase)}`);
    if (sky.issNextVisiblePass?.visible) {
      // riseTime is a UTC ISO string, not a pre-formatted local time -- render
      // it as a clock time so the model doesn't have to parse an ISO string,
      // and so nothing downstream mistakes "2026-07-28T02:39:49Z" for a fact
      // it invented (the unsourced-time audit check matches literal clock
      // strings, not ISO timestamps).
      const t = sky.issNextVisiblePass.riseTime;
      const local = t ? new Date(t).toLocaleTimeString('en-US', {
        timeZone: 'America/Chicago', hour: 'numeric', minute: '2-digit',
      }) : '';
      bits.push(`an ISS pass visible tonight from St. Louis around ${toSpeakableAscii(local)}`);
    }
    if (bits.length) {
      lines.push('');
      lines.push(`TONIGHT'S SKY (optional, use only if it fits the close): ${bits.join(', ')}.`);
    }
  }
  return lines.join('\n');
}

/* ----------------------------------------------------------- claims audit */

/**
 * Structural check on the model's own declared claims. This is the enforcement
 * half of the inference rule -- the prompt states the policy, this measures
 * compliance. It does not block publishing: a flagged episode with a logged
 * warning is more useful than a silent skip, and the flags surface on the
 * transcript route for spot-checking.
 *
 * Catches two things reliably:
 *  1. Claims attributed to an id that was never supplied (invented events).
 *  2. Clock times in the script with no counterpart in the source data --
 *     the specific failure mode that motivated the bounded rule. Times are
 *     load-bearing, easy to hallucinate, and cheap to regex.
 *
 * Does NOT catch: a wrong detail invented inside an otherwise real event.
 * Nothing short of a second model call would, and that is not worth the bill.
 */
function auditClaims(script, claims, validIds, sourceText, reflection = '', quote = null) {
  const flags = [];

  const cited = Array.isArray(claims) ? claims : [];
  for (const c of cited) {
    if (!c || !c.sourceId) {
      flags.push({ type: 'uncited-claim', detail: String(c?.text || '').slice(0, 80) });
    } else if (!validIds.has(c.sourceId)) {
      flags.push({ type: 'invented-source-id', id: c.sourceId, detail: String(c.text || '').slice(0, 80) });
    }
  }
  if (!cited.length) flags.push({ type: 'no-claims-returned' });

  // Clock times: "9:41", "at three in the morning", "3 a.m."
  const timeRe = /\b(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?)?|\b(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+(?:in the (?:morning|afternoon|evening)|o'clock)|\d{1,2}\s*(?:a\.?m\.?|p\.?m\.?))/gi;
  const haystack = sourceText.toLowerCase();
  for (const m of script.matchAll(timeRe)) {
    const t = m[0].trim().toLowerCase();
    const digits = t.match(/\d{1,2}:\d{2}/)?.[0];
    const present = digits ? haystack.includes(digits) : haystack.includes(t);
    if (!present) flags.push({ type: 'unsourced-time', detail: m[0].trim() });
  }

  // --- Flow quality checks. These are craft failures, not factual ones --
  // the prompt asks the model not to lean on these, but asking nicely once
  // clearly isn't sufficient (confirmed Jul 28: a real script used "Meanwhile,"
  // exactly where the prompt told it not to). Flag rather than block --
  // an episode with a connector crutch is still a valid episode, just a
  // weaker one, and these flags are the signal for whether the STEP ONE/TWO/
  // THREE prompt rewrite is actually landing over the next several episodes.
  const CONNECTOR_CRUTCHES = [
    'meanwhile,', 'also,', 'in addition,', 'elsewhere,', 'speaking of',
    'in other news', 'turning now to', 'next up', 'moving on to',
  ];
  const scriptLower = script.toLowerCase();
  for (const phrase of CONNECTOR_CRUTCHES) {
    if (scriptLower.includes(phrase)) flags.push({ type: 'connector-crutch-word', detail: phrase });
  }

  // Bald theme-naming in the reflection -- banned in the prompt directly
  // above THE REFLECTION section; checked here because "tell instead of
  // show" is exactly the failure that made the Jul 28 episode's throughline
  // feel tacked-on rather than earned.
  const THEME_NAMING = [
    'the theme running through', 'the through-line here', 'the throughline here',
    'what connects all of this', 'what ties these together', 'the common thread here',
  ];
  const reflectionLower = (reflection || '').toLowerCase();
  for (const phrase of THEME_NAMING) {
    if (reflectionLower.includes(phrase)) flags.push({ type: 'bald-theme-naming', detail: phrase });
  }

  // --- Reflection: held to a stricter standard than the rest of the script.
  // It has no source id by definition, so the only safe rule is that it may
  // contain no checkable assertion at all. Anything numeric, dated, or named
  // is a fact that escaped its citation.
  if (reflection) {
    const r = reflection.toLowerCase();

    // "one" and "two" are dropped from this list: they show up constantly as
    // ordinary words ("each one," "one more," "on one side") rather than as
    // fabricated counts, and flagged too often against real, clean
    // reflections (confirmed Jul 27 -- "each following the same physics"
    // tripped on the word "one" with no actual invented quantity present).
    // Three and up are rare enough in ordinary phrasing to keep as signal.
    const NUM_WORDS = /\b(three|four|five|six|seven|eight|nine|ten|eleven|twelve|twenty|thirty|forty|fifty|hundred|thousand|million|billion)\b/g;
    const stripped = quote ? reflection.split(quote.text).join(' ') : reflection;
    if (/\d/.test(stripped)) {
      flags.push({ type: 'reflection-contains-digits', detail: (stripped.match(/\d[\d.,:]*/g) || []).join(', ').slice(0, 60) });
    }
    const numWords = stripped.match(NUM_WORDS);
    if (numWords) flags.push({ type: 'reflection-contains-quantity', detail: [...new Set(numWords)].join(', ') });

    // The quotation and its attribution are the ONE exception to the
    // no-named-entities rule -- strip them before checking the rest.
    let checkable = reflection;
    if (quote) {
      checkable = checkable.split(quote.text).join(' ').split(quote.who).join(' ');
    }

    const NAMED = /\b(NASA|ISS|USGS|EONET|Geological Survey|Smithsonian|Earth Observatory|January|February|March|April|May|June|July|August|September|October|November|December|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b/g;
    const named = checkable.match(NAMED);
    if (named) flags.push({ type: 'reflection-names-source-or-date', detail: [...new Set(named)].join(', ') });

    // The quote must appear letter-for-letter. A paraphrased quote is a
    // fabricated one, and no amount of source data can catch it downstream.
    if (quote) {
      const norm = (t) => t.replace(/["\u201C\u201D]/g, '').replace(/\s+/g, ' ').trim().toLowerCase();
      if (!norm(reflection).includes(norm(quote.text))) {
        flags.push({ type: 'quote-altered-or-missing', id: quote.id, detail: quote.text.slice(0, 60) });
      }
      if (!reflection.includes(quote.who)) {
        flags.push({ type: 'quote-attribution-missing', detail: quote.who });
      }
    }

    // The corny failure modes the prompt bans. Cheap to detect, and the whole
    // reason this segment risks sounding like a fortune cookie.
    const CORNY = [
      'in the grand scheme', 'puts things in perspective', 'puts it in perspective',
      'how small we', 'makes you realize', 'we are all stardust', 'we are stardust',
      'pale blue dot', 'the universe is vast', 'humbling', 'take a moment',
      'next time you look', 'remember that we', 'speck', 'cosmic dance',
    ];
    for (const phrase of CORNY) {
      if (r.includes(phrase)) flags.push({ type: 'reflection-cliche', detail: phrase });
    }
    if (/\?/.test(reflection)) flags.push({ type: 'reflection-rhetorical-question' });

    const words = reflection.trim().split(/\s+/).length;
    if (words > 130) flags.push({ type: 'reflection-too-long', detail: `${words} words` });
  }

  return { flags, claimCount: cited.length, clean: flags.length === 0 };
}

/* ------------------------------------------------------------ quote library */

/**
 * Quotes are selected HERE, in code, never written by the model.
 *
 * Reason: a misattributed or misworded quote is the single most common LLM
 * failure mode, and it is the one claim type that cannot be audited against
 * source data -- there is no field in SPACE_KV that says what Sagan said. So
 * the model receives a fixed string it is forbidden to alter, and the audit
 * checks the string survived verbatim.
 *
 * On provenance: these are individual short attributed quotations, not a copy
 * of any one site's compilation. Several widely-circulated "space quotes" are
 * misattributed on aggregator sites -- the Einstein antimatter line and the
 * "two things are infinite" line both fail basic scrutiny and are excluded
 * deliberately. If you add entries, verify against a primary source (the
 * speaker's own book, transcript, or mission audio), not another quote site.
 *
 * Themes map to the day's dominant story so the quote lands as a comment on
 * what was actually discussed rather than as generic filler.
 */
const QUOTES = [
  { id: 'sagan-incredible', text: 'Somewhere, something incredible is waiting to be known.',
    who: 'Carl Sagan', themes: ['discovery', 'space', 'default'] },
  { id: 'sagan-suns', text: 'Across the sea of space, the stars are other suns.',
    who: 'Carl Sagan', themes: ['space', 'sky'] },
  { id: 'cernan-curiosity', text: 'Curiosity is the essence of our existence.',
    who: 'Gene Cernan', themes: ['discovery', 'space', 'default'] },
  { id: 'ride-brighter', text: "The stars don't look bigger, but they do look brighter.",
    who: 'Sally Ride', themes: ['sky', 'space'] },
  { id: 'hadfield-dawn', text: 'To some this may look like a sunset. But it is a new dawn.',
    who: 'Chris Hadfield', themes: ['sky', 'earth', 'default'] },
  { id: 'garan-smalltown', text: 'Earth is a small town with many neighborhoods in a very big universe.',
    who: 'Ron Garan', themes: ['earth', 'perspective'] },
  { id: 'tsiolkovsky-cradle', text: 'The Earth is the cradle of humanity, but mankind cannot stay in the cradle forever.',
    who: 'Konstantin Tsiolkovsky', themes: ['space', 'exploration'] },
  { id: 'jemison-imagination', text: 'Never limit yourself because of others\u0027 limited imagination.',
    who: 'Mae Jemison', themes: ['exploration', 'discovery'] },
  { id: 'hawking-terrestrial', text: 'To confine our attention to terrestrial matters would be to limit the human spirit.',
    who: 'Stephen Hawking', themes: ['space', 'perspective'] },
  { id: 'hawking-lookup', text: 'Remember to look up at the stars and not down at your feet.',
    who: 'Stephen Hawking', themes: ['sky', 'perspective', 'default'] },
  { id: 'gagarin-looked', text: "I looked and looked but I didn't see God.",
    who: 'Yuri Gagarin', themes: ['space', 'perspective'] },
  { id: 'liwei-greatwall', text: 'The scenery was very beautiful. But I did not see the Great Wall.',
    who: 'Yang Liwei', themes: ['perspective', 'earth'] },
  { id: 'glenn-sunsets', text: "I don't know what you could say about a day in which you have seen four beautiful sunsets.",
    who: 'John Glenn', themes: ['sky', 'perspective'] },
  { id: 'earle-lifesupport', text: 'Do everything you can to learn about your life support system, then everything you can to take care of it.',
    who: 'Sylvia Earle', themes: ['earth', 'fragility'] },
  { id: 'lessing-dialect', text: 'Space or science fiction has become a dialect for our time.',
    who: 'Doris Lessing', themes: ['space', 'discovery'] },
];

/** Which theme does today lean on? Drives quote choice. */
function dayTheme(space, earth) {
  const e = earth.events.length;
  const s = space.stories.length;
  const kinds = earth.events.map((x) => x.kind || '');
  if (kinds.some((k) => k === 'earthquake' || k === 'nmsz' || k === 'yellowstone'
                      || k === 'wildfires' || k === 'storms')) {
    return e >= s ? 'fragility' : 'earth';
  }
  if (kinds.some((k) => k === 'volcano')) return 'earth';
  if (space.raw?.tonight?.issNextVisiblePass?.visible) return 'sky';
  if (s > e) return 'space';
  return 'default';
}

/**
 * Pick a themed quote, avoiding the last N used so the show doesn't loop.
 * @param {string[]} recentIds most-recent-first
 */
function pickQuote(theme, recentIds = []) {
  const recent = new Set(recentIds.slice(0, 8));
  const themed = QUOTES.filter((q) => q.themes.includes(theme) && !recent.has(q.id));
  const anyFresh = QUOTES.filter((q) => !recent.has(q.id));
  const pool = themed.length ? themed : (anyFresh.length ? anyFresh : QUOTES);
  // Deterministic per day, so a re-run produces the same episode.
  const seed = utcDayKey().split('-').join('');
  return pool[Number(seed) % pool.length];
}

/* ------------------------------------------------------------ closeout line */

/**
 * Fixed sign-off, rotated daily. Code-generated, not model-generated -- same
 * reasoning as the welcome line: a repeated device only works if it's
 * genuinely identical each time it recurs, and a model asked to write "a
 * closing line" fresh every day will drift toward exactly the swelling
 * payoff-line cheese the reflection prompt already works hard to avoid.
 * Fixing it in code also means the reflection's "land flat and stop" rule
 * can't be undone by a warm sign-off tacked on after it.
 *
 * Two of these are date-templated ({date}); three are plain. All five were
 * chosen to do as little as possible -- state a fact, or nothing more than
 * a continuity note -- rather than trying to land a meaningful line every
 * single day, which is the failure mode that makes a sign-off feel like a
 * tic after 200 episodes instead of a voice.
 */
const CLOSEOUTS = [
  { id: 'thats-show-date', render: (date) => `That's Earth and Orbit for ${date}.` },
  { id: 'thats-show-date-tomorrow', render: (date) => `That's Earth and Orbit for ${date}. Same time tomorrow.` },
  { id: 'same-planet-tomorrow', render: () => 'Same planet, tomorrow.' },
  { id: 'more-tomorrow', render: () => 'More tomorrow.' },
  { id: 'thats-what-happened', render: () => "That's what happened today." },
];

/**
 * Same rotation-avoiding-recent shape as pickQuote, deliberately kept as a
 * separate function rather than generalized into one shared helper -- the
 * two pools are unrelated in size and selection criteria (quote picks by
 * theme, closeout doesn't), and keeping them separate means a future change
 * to one can't accidentally affect the other.
 * @param {string[]} recentIds most-recent-first
 */
function pickCloseout(recentIds = []) {
  const recent = new Set(recentIds.slice(0, 3));   // pool of 5 -- avoid last 3, not 8
  const fresh = CLOSEOUTS.filter((c) => !recent.has(c.id));
  const pool = fresh.length ? fresh : CLOSEOUTS;
  // Different seed offset than pickQuote's, so the two selections don't
  // correlate for the same day (a day landing on quote index 2 shouldn't
  // always also land on closeout index 2).
  const seed = utcDayKey().split('-').join('') + '7';
  return pool[Number(seed) % pool.length];
}

/* ------------------------------------------------------- monthly cost gate */

/**
 * Lifetime episode number, e.g. "Episode 13". Stored in its own KV key,
 * deliberately NOT part of the monthly credit ledger above -- that ledger
 * resets every billing month by design (readCreditLedger, line ~750), which
 * would make the show's own episode count reset to 1 every August 1st if
 * this reused it. This counter only ever increments, forever.
 */
async function nextEpisodeNumber(kv) {
  let n = 0;
  try {
    const raw = await kv.get('podcast:episode-number');
    n = raw ? JSON.parse(raw).n : 0;
  } catch { n = 0; }
  const next = n + 1;
  await kv.put('podcast:episode-number', JSON.stringify({ n: next }));
  return next;
}

async function readCreditLedger(kv) {
  const month = utcMonthKey();
  try {
    const raw = await kv.get('podcast:credits');
    const led = raw ? JSON.parse(raw) : null;
    if (led && led.month === month) return led;
    return { month, creditsUsed: 0, episodes: 0 };   // new billing month
  } catch {
    return { month, creditsUsed: 0, episodes: 0 };
  }
}

async function writeCreditLedger(kv, ledger) {
  await kv.put('podcast:credits', JSON.stringify(ledger));
}

/* ----------------------------------------------------------- external calls */

async function generateScript(env, digest, quote, diagnostics) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': env.ANTHROPIC_API_KEY,
      'anthropic-version': ANTHROPIC_VERSION,
    },
    body: JSON.stringify({
      model: SCRIPT_MODEL,
      max_tokens: MAX_SCRIPT_TOKENS,
      system: [{ type: 'text', text: SYSTEM_PROMPT, cache_control: { type: 'ephemeral' } }],
      messages: [{
        role: 'user',
        content: `Here is today's source material. Write the briefing.\n\n${digest}\n\n`
          + `QUOTATION TO USE IN THE REFLECTION, verbatim, attributed to ${quote.who}:\n"${quote.text}"`,
      }],
      // NOTE: no tools block. No web_search. This call must never search.
    }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Anthropic HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json();
  diagnostics.push({
    step: 'script',
    ok: true,
    inputTokens: data.usage?.input_tokens ?? null,
    outputTokens: data.usage?.output_tokens ?? null,
    cacheRead: data.usage?.cache_read_input_tokens ?? 0,
    stopReason: data.stop_reason,
  });

  let text = (data.content || [])
    .filter((b) => b.type === 'text')
    .map((b) => b.text)
    .join('\n')
    .replace(/^```(?:json)?\s*/i, '')
    .replace(/\s*```$/, '')
    .trim();

  let script = '';
  let reflection = '';
  let imagePrompt = '';
  let claims = [];
  try {
    const parsed = JSON.parse(text);
    script = parsed.script || '';
    reflection = parsed.reflection || '';
    imagePrompt = parsed.imagePrompt || '';
    claims = parsed.claims || [];
    if (!reflection) diagnostics.push({ step: 'script', ok: true, note: 'no reflection returned' });
    if (!imagePrompt) diagnostics.push({ step: 'script', ok: true, note: 'no imagePrompt returned -- cover art will fall back to static cover.jpg' });
  } catch {
    // Model ignored the JSON contract. Salvage the prose rather than burning
    // the run, but flag it -- an unaudited script is a known-weaker artifact.
    diagnostics.push({ step: 'script', ok: true, note: 'response was not valid JSON; using raw text, claims unaudited, no image prompt' });
    script = text;
  }

  return {
    script: toSpeakableAscii(script),
    reflection: toSpeakableAscii(reflection),
    imagePrompt: toSpeakableAscii(imagePrompt),
    claims,
  };
}

/**
 * Generate one piece of cover art via Cloudflare Workers AI (env.AI binding).
 * Deliberately NOT calling an external image API (DALL-E, Midjourney, etc.) --
 * Workers AI is already available on this account (see the `pokepod` sibling
 * project's AI binding), keeps this to zero new vendor keys, and its image
 * models are free-tier-friendly for one image a day. Verify current neuron
 * pricing/allowance in the Cloudflare dashboard before assuming this stays
 * free forever -- that's an account-level fact this file can't see.
 *
 * This is treated as BEST-EFFORT, not required for a valid episode. Image
 * generation failing does not fail the episode -- the audio is the actual
 * product; the image is a nice-to-have that falls back to the static
 * cover.jpg (the channel-level image already in feed.xml) if it's missing.
 */
const IMAGE_MODEL = '@cf/black-forest-labs/flux-1-schnell';

async function generateCoverImage(env, imagePrompt, diagnostics) {
  if (!imagePrompt) {
    diagnostics.push({ step: 'image', ok: false, error: 'no imagePrompt from script generation; skipping' });
    return null;
  }
  if (!env.AI) {
    diagnostics.push({ step: 'image', ok: false, error: 'AI binding not present; skipping (episode still publishes with static cover.jpg)' });
    return null;
  }
  try {
    const result = await env.AI.run(IMAGE_MODEL, {
      prompt: imagePrompt,
      // flux-1-schnell is optimized for very few steps; this is near its
      // default. Raising steps raises neuron cost for marginal quality gain
      // on a thumbnail-sized daily image -- not worth it here.
      num_steps: 4,
    });

    // Workers AI image models return either a raw binary stream or a
    // { image: base64string } object depending on model/runtime version --
    // handle both rather than assuming one shape untested.
    let bytes;
    if (result instanceof ReadableStream) {
      const chunks = [];
      const reader = result.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
      }
      bytes = new Uint8Array(chunks.reduce((a, c) => a + c.length, 0));
      let offset = 0;
      for (const c of chunks) { bytes.set(c, offset); offset += c.length; }
    } else if (result?.image) {
      const bin = atob(result.image);
      bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    } else if (result instanceof ArrayBuffer) {
      bytes = new Uint8Array(result);
    } else {
      throw new Error(`unrecognized Workers AI response shape: ${typeof result}`);
    }

    if (bytes.byteLength < 500) {
      // A near-empty response "succeeded" with nothing usable -- same shape
      // as the ElevenLabs near-empty-MP3 check below. Treat as failure.
      throw new Error(`image response only ${bytes.byteLength} bytes -- treating as failure`);
    }

    diagnostics.push({ step: 'image', ok: true, bytes: bytes.byteLength, model: IMAGE_MODEL });
    return bytes;
  } catch (err) {
    diagnostics.push({ step: 'image', ok: false, error: err.message });
    return null;   // never throws -- episode publishes without custom art
  }
}

async function synthesizeSpeech(env, script, diagnostics) {
  const voiceId = env.ELEVENLABS_VOICE_ID;
  const url = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}?output_format=mp3_44100_128`;

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'xi-api-key': env.ELEVENLABS_API_KEY,
      'content-type': 'application/json',
      accept: 'audio/mpeg',
    },
    body: JSON.stringify({
      text: script,
      model_id: TTS_MODEL,
      voice_settings: VOICE_SETTINGS,
    }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`ElevenLabs HTTP ${res.status}: ${body.slice(0, 200)}`);
  }

  const buf = await res.arrayBuffer();
  diagnostics.push({ step: 'tts', ok: true, bytes: buf.byteLength, model: TTS_MODEL });
  if (buf.byteLength < 20000) {
    // A near-empty MP3 means the call "succeeded" but produced nothing usable.
    // Same failure shape as the STATUS_KV wrong-key write: exit code 0, wrong target.
    throw new Error(`ElevenLabs returned only ${buf.byteLength} bytes -- treating as failure`);
  }
  return buf;
}

/* --------------------------------------------------------------- main entry */

/**
 * @param {object} env  dispatcher env: SPACE_KV, EARTH_KV, PODCAST_KV,
 *                      POD_BUCKET (R2), ANTHROPIC_API_KEY,
 *                      ELEVENLABS_API_KEY, ELEVENLABS_VOICE_ID
 * @param {object} opts { force?: boolean } -- force bypasses same-day idempotency
 *                      but NEVER bypasses the monthly credit budget.
 */
export async function runPodcastIngest(env, opts = {}) {
  const diagnostics = [];
  const day = utcDayKey();
  const result = { ran: false, ok: false, day, diagnostics };

  try {
    // --- 0. Preflight. Fail loud and early, before spending anything.
    for (const k of ['ANTHROPIC_API_KEY', 'ELEVENLABS_API_KEY', 'ELEVENLABS_VOICE_ID']) {
      if (!env[k]) throw new Error(`missing secret: ${k}`);
    }
    if (!env.PODCAST_KV) throw new Error('missing binding: PODCAST_KV');
    if (!env.POD_BUCKET) throw new Error('missing binding: POD_BUCKET (R2)');

    // --- 1. Idempotency: already made today's episode?
    const existing = await env.PODCAST_KV.get(`podcast:episode:${day}`);
    if (existing && !opts.force) {
      diagnostics.push({ step: 'idempotency', ok: true, note: 'episode already exists for this UTC day, skipping' });
      result.ran = false;
      result.ok = true;
      result.skipped = 'already-generated';
      return result;
    }
    // If this IS a forced regen of an already-made day, reuse its episode
    // number rather than minting a new one -- confirmed necessary Jul 30,
    // when the same day was force-regenerated several times while testing.
    // "Episode 6" should stay "Episode 6" no matter how many times its
    // content gets redone before the day is over.
    let reusedEpisodeNumber = null;
    if (existing) {
      try { reusedEpisodeNumber = JSON.parse(existing).episodeNumber || null; } catch { /* fall through */ }
    }

    // --- 2. Read what the other two pipelines already wrote today.
    const space = await readSourceBlob(env.SPACE_KV, 'space-data', 'space');
    const earth = await readEarthBlob(env.EARTH_KV, 'earth-data');
    diagnostics.push({
      step: 'sources',
      space: { ok: space.ok, count: space.count ?? 0, ageHours: ageHours(space.lastUpdated), error: space.error || null },
      earth: { ok: earth.ok, count: earth.count ?? 0, ageHours: ageHours(earth.lastUpdated), error: earth.error || null },
    });

    const totalStories = (space.stories.length || 0) + (earth.events.length || 0);
    if (totalStories < 3) {
      // Better to skip a day than to spend credits narrating nothing.
      diagnostics.push({ step: 'gate', ok: false, note: `only ${totalStories} source items (space stories + earth events), need 3+; skipping today` });
      result.ok = true;
      result.skipped = 'insufficient-source-material';
      return result;
    }

    // --- 3. Budget gate. Estimate BEFORE calling ElevenLabs.
    const ledger = await readCreditLedger(env.PODCAST_KV);
    const estChars = Math.min(TARGET_WORDS * 6.2, MAX_SCRIPT_CHARS);
    const estCredits = Math.ceil(estChars * CREDITS_PER_CHAR);
    if (ledger.creditsUsed + estCredits > MONTHLY_CREDIT_BUDGET) {
      diagnostics.push({
        step: 'budget',
        ok: false,
        note: `monthly ElevenLabs budget would be exceeded (${ledger.creditsUsed} used + ~${estCredits} est > ${MONTHLY_CREDIT_BUDGET}); skipping`,
      });
      result.ok = true;
      result.skipped = 'monthly-credit-budget';
      result.ledger = ledger;
      return result;
    }

    // --- 4. Script.
    result.ran = true;
    const digest = buildSourceDigest(space, earth);

    // Quote chosen in code, themed to the day, rotated against recent use.
    const theme = dayTheme(space, earth);
    const quote = pickQuote(theme, ledger.recentQuotes || []);
    diagnostics.push({ step: 'quote', ok: true, theme, id: quote.id, who: quote.who });

    // Closeout chosen the same way -- fixed pool, rotated, never model-written.
    const closeout = pickCloseout(ledger.recentCloseouts || []);
    diagnostics.push({ step: 'closeout', ok: true, id: closeout.id });

    const gen = await generateScript(env, digest, quote, diagnostics);

    // Welcome line is code-generated, not model-generated -- a spoken date is
    // exactly the class of fact this pipeline treats as load-bearing, and the
    // model never gets a chance to phrase or mis-state it. Built from `day`
    // (the episode's own YYYY-MM-DD), not wall-clock time, so it can never
    // drift from the actual episode date even if generation straddles
    // midnight UTC.
    const niceDate = new Date(`${day}T12:00:00Z`).toLocaleDateString('en-US', {
      month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC',
    });
    const welcomeLine = `Welcome to today's briefing for ${niceDate}.`;
    const closeoutLine = closeout.render(niceDate);

    // Reflection is audited separately, then joined for the read.
    let script = gen.reflection ? `${gen.script}\n\n${gen.reflection}` : gen.script;

    // Audit before spending a single TTS credit on it.
    const validIds = new Set(
      [...space.stories, ...earth.events].map((s) => s.id).filter(Boolean)
    );
    const audit = auditClaims(gen.script, gen.claims, validIds, digest, gen.reflection, quote);
    diagnostics.push({ step: 'audit', ok: audit.clean, claims: audit.claimCount, flags: audit.flags });

    if (!script || script.length < 800) {
      throw new Error(`script too short (${script.length} chars) -- refusing to spend TTS credits`);
    }
    if (script.length > MAX_SCRIPT_CHARS) {
      // Cut at the last sentence boundary rather than mid-word.
      const cut = script.slice(0, MAX_SCRIPT_CHARS);
      script = cut.slice(0, cut.lastIndexOf('.') + 1) || cut;
      diagnostics.push({ step: 'script', ok: true, note: 'truncated to char ceiling' });
    }

    // Prepend/append now, after the length gate/truncation above -- those
    // checks are about the model's own content, not the fixed welcome/
    // closeout lines, so this stays out of their way. Everything downstream
    // (char count, credits, word count, TTS, transcript) includes both,
    // since they're genuinely part of what gets read aloud.
    script = `${welcomeLine} ${script}\n\n${closeoutLine}`;

    const charCount = script.length;
    const credits = Math.ceil(charCount * CREDITS_PER_CHAR);
    const words = script.split(/\s+/).length;

    // Re-check the real number now that we know it (estimate could be low).
    if (ledger.creditsUsed + credits > MONTHLY_CREDIT_BUDGET) {
      diagnostics.push({ step: 'budget', ok: false, note: 'actual script exceeded remaining budget; skipping TTS' });
      result.ok = true;
      result.skipped = 'monthly-credit-budget-actual';
      return result;
    }

    // --- 5. Audio + cover image, concurrently -- independent of each other,
    // and image generation is best-effort so it must never block or fail
    // the audio path. Promise.allSettled, not Promise.all, on purpose.
    const [audioResult, imageResult] = await Promise.allSettled([
      synthesizeSpeech(env, script, diagnostics),
      generateCoverImage(env, gen.imagePrompt, diagnostics),
    ]);

    if (audioResult.status === 'rejected') {
      throw audioResult.reason;   // audio is required; propagate as a real failure
    }
    const audio = audioResult.value;
    const imageBytes = imageResult.status === 'fulfilled' ? imageResult.value : null;
    if (imageResult.status === 'rejected') {
      // generateCoverImage is written to never throw, but Promise.allSettled
      // is defensive here in case that contract is ever violated by a future
      // edit -- log and continue rather than let an image bug take down audio.
      diagnostics.push({ step: 'image', ok: false, error: `unexpected throw: ${imageResult.reason}` });
    }

    const objectKey = `episodes/${day}.mp3`;
    await env.POD_BUCKET.put(objectKey, audio, {
      httpMetadata: { contentType: 'audio/mpeg', cacheControl: 'public, max-age=31536000, immutable' },
    });
    diagnostics.push({ step: 'r2', ok: true, key: objectKey, bytes: audio.byteLength });

    let imageKey = null;
    if (imageBytes) {
      imageKey = `episodes/${day}.jpg`;
      await env.POD_BUCKET.put(imageKey, imageBytes, {
        httpMetadata: { contentType: 'image/jpeg', cacheControl: 'public, max-age=31536000, immutable' },
      });
      diagnostics.push({ step: 'r2-image', ok: true, key: imageKey, bytes: imageBytes.byteLength });
    }

    // Minted only now, after audio has actually written to R2 successfully --
    // not earlier in the function, so a run that throws or skips before this
    // point never burns a lifetime number on an episode that didn't publish.
    const episodeNumber = reusedEpisodeNumber || await nextEpisodeNumber(env.PODCAST_KV);
    diagnostics.push({ step: 'episode-number', ok: true, number: episodeNumber, reused: !!reusedEpisodeNumber });

    // --- 6. Episode record + manifest.
    const estSeconds = Math.round(words / 2.5);   // ~150 wpm
    const episode = {
      id: day,
      episodeNumber,
      title: `Earth and Orbit -- ${new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC' })}`,
      pubDate: new Date().toUTCString(),
      audioKey: objectKey,
      imageKey,                    // null if generation failed -- feed falls back to cover.jpg
      imagePrompt: gen.imagePrompt || null,
      bytes: audio.byteLength,
      durationSeconds: estSeconds,
      words,
      chars: charCount,
      credits,
      script,
      reflection: gen.reflection,
      quote: { id: quote.id, text: quote.text, who: quote.who, theme },
      closeoutId: closeout.id,
      audit,
      sourceCounts: { space: space.stories.length, earth: earth.events.length },
    };
    episode.title = toSpeakableAscii(episode.title);

    await env.PODCAST_KV.put(`podcast:episode:${day}`, JSON.stringify(episode));

    // Manifest: newest-first, capped. The feed Worker reads only this one key.
    let manifest = [];
    try {
      const rawManifest = await env.PODCAST_KV.get('podcast:manifest');
      if (rawManifest) manifest = JSON.parse(rawManifest);
    } catch { manifest = []; }

    manifest = manifest.filter((e) => e.id !== day);
    manifest.unshift({
      id: episode.id,
      episodeNumber: episode.episodeNumber,
      title: episode.title,
      pubDate: episode.pubDate,
      audioKey: episode.audioKey,
      imageKey: episode.imageKey,   // null if generation failed -- feed Worker falls back to cover.jpg
      bytes: episode.bytes,
      durationSeconds: episode.durationSeconds,
      // First two sentences, as the show-notes blurb.
      blurb: script.split(/(?<=\.)\s+/).slice(0, 2).join(' ').slice(0, 400),
    });
    manifest = manifest.slice(0, 60);   // ~2 months of episodes in the feed
    await env.PODCAST_KV.put('podcast:manifest', JSON.stringify(manifest));

    // --- 7. Ledger.
    ledger.creditsUsed += credits;
    ledger.episodes += 1;
    ledger.recentQuotes = [quote.id, ...(ledger.recentQuotes || [])].slice(0, 12);
    ledger.recentCloseouts = [closeout.id, ...(ledger.recentCloseouts || [])].slice(0, 5);
    ledger.lastEpisode = day;
    await writeCreditLedger(env.PODCAST_KV, ledger);

    result.ok = true;
    result.episode = {
      id: episode.id, words, chars: charCount, credits,
      bytes: audio.byteLength, durationSeconds: estSeconds,
      auditClean: audit.clean, auditFlags: audit.flags.length,
    };
    result.ledger = { creditsUsed: ledger.creditsUsed, budget: MONTHLY_CREDIT_BUDGET, episodes: ledger.episodes };
    return result;

  } catch (err) {
    diagnostics.push({ step: 'error', ok: false, error: err.message });
    result.ok = false;
    result.error = err.message;
    return result;
  }
}
