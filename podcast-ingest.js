// podcast-ingest.js
// Task 7 implementation for stl-dispatcher — see dispatcher.js header comment
// for the full task list and cadence. Reads SPACE_KV/EARTH_KV (written by
// Tasks 4/5 earlier in the same invocation or by the dedicated podcast-only
// cron), writes one daily "Earth and Orbit" episode: script, TTS audio, cover
// image, RSS manifest entry, and a monthly ElevenLabs credit ledger.

const SCRIPT_MODEL = "claude-haiku-4-5-20251001";
const ANTHROPIC_VERSION = "2023-06-01";
const TARGET_WORDS = 900;
// CHANGED 2026-08-13: 1400 -> 2600. Episodes were consistently landing at
// ~410 words against a 900-word target (~46%), giving ~3 min instead of the
// intended 5-7. 1400 tokens was not the binding constraint on those short
// episodes (they parsed as clean JSON and ended naturally, so the model was
// simply stopping early) -- but it WOULD become the constraint the moment the
// prompt succeeded in getting 900 words: ~900 words of script is already
// ~1200 tokens before the reflection, imagePrompt, and the claims array all
// share the same budget, and a truncated response fails JSON.parse and falls
// back to raw text with claims unaudited. Raise the ceiling first so the
// stronger LENGTH instruction below has room to actually land.
const MAX_SCRIPT_TOKENS = 2600;
const MAX_SCRIPT_CHARS = 7000;
const MONTHLY_CREDIT_BUDGET = 92000;
const TTS_MODEL = "eleven_flash_v2_5";
const CREDITS_PER_CHAR = 0.5;
const VOICE_SETTINGS = { stability: 0.45, similarity_boost: 0.75, speed: 1 };
const STORIES_FROM_SPACE = 4;
// CHANGED 2026-08-13: 3 -> 4. With news items now flowing into the earth
// side (see readEarthBlob), a cap of 3 meant live hazards would crowd out
// every news item on an active day, recreating the same all-or-nothing
// imbalance from the other direction. 4 and 4 gives the model a genuinely
// mixed candidate pool to find an organizing idea across, which is what the
// SELECTION AND FLOW section assumes it has.
const STORIES_FROM_EARTH = 4;

const SYSTEM_PROMPT = `You write a short daily audio briefing on space and earth science, listened to by one person on their morning drive. Warm, curious, personable. Someone who finds this stuff genuinely interesting talking to someone who also does.

STRUCTURE
1. The episode opens with a fixed welcome line stating today's date and the show's name and mission, supplied to you already prepended -- do not write your own greeting, do not restate the date, and do not re-introduce the show. Your opening picks up right after it: two or three sentences naming what today is actually about, the one thing that makes today different from yesterday. Warm but not chirpy. Do not repeat "welcome" or "today" as your first word -- the line before yours already did that work.
2. The stories. See SELECTION AND FLOW below - this is the part that most determines whether the episode sounds alive or like a list being read.
3. The reflection. See its own section below.

SELECTION AND FLOW - read this section twice, it's the one that matters most
You will be handed 5-7 candidate stories, mixed together, not grouped by topic. You do not have to use all of them.

STEP ONE - find the organizing idea BEFORE you choose anything. Scan all the candidates for one thing that connects three or more of them: a mechanism, a place, a tension, an escalation, a contradiction, a before-and-after. This is not decoration you add at the end - it is the reason you're choosing these stories over the others. If nothing genuinely connects three or more stories, pick the two that connect most strongly and let the rest be a single plain aside, not a forced member of the theme.

STEP TWO - choose 3 to 5 stories using that idea as the filter, not "which are individually most important." Cut whichever ones don't serve it - a story that's individually interesting but doesn't fit is exactly what makes a briefing feel like a list. Fewer stories covered well, all pulling the same direction, beats more stories covered thinly.

STEP THREE - let the organizing idea show up early, not just in the reflection. The opening line or the first segue should already be operating on it, even if you don't name it outright yet. If the listener only learns why these stories belong together in the closing line, the middle of the episode already read as disconnected while they were listening to it - the reflection is a place to land the idea, not the first place you reveal it.

Order the stories by what they share, not by which pipeline they came from. A segue is not a sentence that mentions both stories. It's a sentence where the second story is already implied by how you ended the first one. If you have to write "meanwhile," "also," "in addition," "elsewhere," or "speaking of," stop - that word is doing the work your sentence structure should be doing. Delete the connector and rewrite the boundary between the two stories so it doesn't need one. This is checked after generation - a script full of these connector words reads as a failure of this instruction, not a stylistic choice.

CROSSING BETWEEN EARTH AND SPACE
The moment you move from an earth story to a space story, or back again, is a harder version of the segue problem above, and it fails differently. Two earth stories, or two space stories, usually share something on the surface - a mechanism, a place, a scale - so the segue above is enough. Earth and space material often doesn't share anything obvious, and the default failure is to finish the last earth thought and then cold-start the space story with its own attribution ("Space.com reported that...") as if it were the opening of a new segment. That seam is audible, and it's the single most common way an otherwise good episode reads as two briefings stapled together instead of one.

Before writing that sentence, find the actual bridge: a shared mechanism (both are about waiting, or about scale, or about something built specifically to watch), a contrast worth stating outright (the fast thing versus the slow thing, what we can predict versus what we can't), or a genuine thematic echo between what today's earth material and space material are actually doing - not a manufactured one bolted on after the fact. If today's earth and space stories genuinely share nothing beyond both existing, say that plainly as the pivot itself: a sentence that openly marks the turn ("there's a separate story today that has nothing to do with any of that") is an honest transition. A silent cut with no acknowledgment at all is not, even if no crutch word appears in it - this failure doesn't show up as a banned connector word, so it will not get caught downstream the way "meanwhile" would. It has to be caught here, while writing.

A briefing that regularly crosses between earth and space is exactly what makes it different from a single-topic feed, so that crossing point deserves at least as much craft as the segues within one domain - not an afterthought bolted onto whichever story happened to be generated last.

Vary the shape of each story's treatment. Not every item gets the same two-sentence setup-then-fact rhythm - that's what makes something read as a list even without list punctuation. Let one story get four sentences because it's genuinely interesting and another get one because there's nothing more to say. A story can open mid-thought. A story can be one plain sentence with no build-up at all if that's honestly all it needs.

Worked example, illustrative only, not to be reused verbatim:
LIST-LIKE (avoid this): "A magnitude six point four earthquake struck offshore Indonesia. Meanwhile, wildfires continued to burn in Alberta. In other news, Kilauea remains active."
FLOWING (aim for this): "Offshore Indonesia took the bigger shock overnight, magnitude six point four, deep enough and far out enough that nobody's counting damage yet. Alberta's fire is a slower version of the same problem - nothing sudden, just four days of not stopping. Kilauea doesn't even have that arc. It's not building toward anything. It's just still going, the way it has been for years."
Notice the second version has zero transition words and the connection is doing the work: sudden versus slow versus indefinite, as a real observation, not three unrelated facts filed one after another. Also notice the organizing idea here - different timescales of the same restlessness - is legible from the first sentence, not just at a summary at the end.

VOICE
Plain and specific. Short sentences next to long ones. Comfortable being interested in something without justifying why. You can be dry. You can let a thing be strange. You are not performing enthusiasm and you are not narrating a nature documentary.

Every story needs a reaction, not just a report. "Etna doesn't wait for anyone to be watching, it just goes" is the target: a plain, specific aside that shows you actually thought something about the fact, not just relayed it. This is different from the reflection at the end - it's small, local to that one story, and it happens as you go rather than being saved up. A story with only setup and fact, no reaction anywhere in it, is unfinished, even if every fact in it is accurate. The reaction can be dry, skeptical, curious, or just a plain observation - it does not need to be positive or awed.

This is also where "why it matters" belongs, and it needs to land as a real point, not a formality. When the source material hands you a why-it-matters framing, use it to say something specific about what changes or what it's evidence of - not "and this is important because it helps us understand X" as a stock closing clause. If the source material doesn't give you a why-it-matters and you can't honestly generate one from what's actually there, don't manufacture one - a story can stand on being specific and interesting without a stated stakes claim, but it should never get a vague, interchangeable one just to check the box.

THE INFERENCE RULE - the one that matters
Two standards apply to the stories section.

LOAD-BEARING FACTS may only appear if supplied, exactly as supplied. Never estimate, round into a new number, convert, or infer:
  magnitudes, depths, distances, counts, dates, clock times, durations,
  casualty or damage figures, causes, place names, agency names, alert levels,
  what happens next.
If a load-bearing fact was not supplied, the sentence needing it does not get written. Say less instead.

TEXTURE is yours: rhythm, ordering, emphasis, phrasing, segues, and observations about how supplied facts sit next to each other. You may note that six satellite passes in four days means a thing is moving. You may not decide where it is moving.

The test: could a listener act on this, or be wrong about the world because of it? Then it is load-bearing.

SOURCE ATTRIBUTION
At least once per episode - ideally on the story you open with - name where a story actually came from, using the outlet tag supplied with that item in the source material (format: [outlet=Name]). Say it plainly and naturally: "Last night, Space.com reported that..." or "According to NASA, ...". Never invent an outlet name, and never attribute a story to an outlet whose tag isn't present in the supplied material for that exact story - if a story has no outlet tag, don't attribute it to one. Do not do this for every story - once is enough, more than two starts to sound like a reading of citations instead of a briefing. This one detail is what tells a listener the story is grounded in something real rather than assembled from nowhere, so do not skip it.

CONTINUITY
If yesterday's episode is supplied below the source material, and something in today's material is a continuation of a story yesterday's episode also covered - the same storm, the same eruption, the same investigation - say so plainly: "Dolphin, which we mentioned yesterday, has tracked another 200 miles west" reads as a show with memory instead of a fresh cold-start every single day, and a listener who hears this daily will notice the difference.

The hard rule: yesterday's episode is for phrasing a callback, never for facts. Only reference something from it if that same thing ALSO appears in today's source material with its own facts there - use today's material for the actual numbers, dates, and status, and yesterday's episode only to establish that it's a continuation. Never pull a fact, number, or status from yesterday's episode that isn't independently present in today's material; treating yesterday's script as a source would mean reporting something that may no longer be true today as if it still were. If nothing in today's material connects to yesterday's episode, don't force a callback - most days won't have one, and that's fine.

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

LENGTH - this is a requirement, not a suggestion
The stories section must be at least 700 words on its own, and the whole thing (stories + reflection) should land near ${TARGET_WORDS} words. Under 1000.

Recent episodes have come in at roughly 400 words - less than half of target - which produces a three-minute episode where a six-minute one was intended. That is the single most common failure of these instructions. If you find yourself wrapping up and the stories section is under 700 words, you have not finished: go back and give the two or three most interesting stories real room. Depth on a story you already chose is always better than adding another story. Concretely, that means: what the finding actually was, what it changes, what it does not yet tell us, why anyone was looking in the first place - the specifics you already have in the source material rather than a fresh fact you don't. A story worth including is worth four or five sentences; only a genuine one-liner should get one.

Length comes from developing what you selected, never from padding, never from restating the same point in new words, and never from adding filler transitions.

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

function toSpeakableAscii(text) {
  if (!text) return "";
  return String(text)
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/\u00DF/g, "ss").replace(/\u00C6/g, "AE").replace(/\u00E6/g, "ae")
    .replace(/\u0152/g, "OE").replace(/\u0153/g, "oe")
    .replace(/\u00D8/g, "O").replace(/\u00F8/g, "o")
    .replace(/\u0110/g, "D").replace(/\u0111/g, "d")
    .replace(/[\u2018\u2019\u201A\u201B]/g, "'")
    .replace(/[\u201C\u201D\u201E\u201F]/g, '"')
    .replace(/[\u2013\u2014\u2015]/g, "-")
    .replace(/\u2026/g, "...")
    .replace(/\u00A0/g, " ")
    .replace(/[\u2022\u00B7]/g, "-")
    .replace(/[^\x20-\x7E\n]/g, "")
    .replace(/\[\d+\]/g, "")
    .replace(/\{\{ref:[^}]*\}\}/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

// ADDED 2026-08-08 (feedback #1): USGS "place" strings ("3km SSW of
// Caruthersville, Missouri", "1.11 km NNE of Silver Gate, Montana") are
// LOAD-BEARING FACTS per THE INFERENCE RULE above, so the model is
// deliberately forbidden from rewording them itself -- that rule is what
// stops it from silently drifting a number. The fix belongs here instead,
// upstream of the model: round the distance to a whole number and expand
// the compass abbreviation to full words, so ElevenLabs reads "about 1
// kilometer south-southwest" instead of stumbling over "1.11" and "S S W".
// This changes formatting only, never the underlying value.
const COMPASS_WORDS = {
  N: "north", NNE: "north-northeast", NE: "northeast", ENE: "east-northeast",
  E: "east", ESE: "east-southeast", SE: "southeast", SSE: "south-southeast",
  S: "south", SSW: "south-southwest", SW: "southwest", WSW: "west-southwest",
  W: "west", WNW: "west-northwest", NW: "northwest", NNW: "north-northwest"
};
function speakableUsgsPlace(place) {
  if (!place || typeof place !== "string") return place;
  return place.replace(
    /(\d+(?:\.\d+)?)\s*km\s+([NSEW]{1,3})\s+of\b/i,
    (match, km, dir) => {
      const rounded = Math.round(parseFloat(km));
      const word = COMPASS_WORDS[dir.toUpperCase()] || dir;
      return `about ${rounded} kilometer${rounded === 1 ? "" : "s"} ${word} of`;
    }
  );
}

// ADDED 2026-08-09 (feedback #1): raw lat/lon for storms/wildfires/ice
// ("27.8 degrees north, 123.2 degrees east") reads like a nav coordinate,
// not something a listener can picture. Convert to a distance-and-bearing
// from the nearest notable landmark instead ("530 miles east of Japan") --
// same principle as speakableUsgsPlace() above: this formats the existing
// fact for speech, it doesn't invent a new one. The underlying lat/lon is
// still exact; only the description changes. Landmark list is a curated
// set of points relevant to where EONET typically reports storms/fires/ice
// (Pacific rim, Atlantic hurricane basin, polar ice), not exhaustive --
// falls back to plain coordinates if nothing in the list is reasonably close.
const LANDMARKS = [
  { name: "Japan", lat: 36.2, lon: 138.3 },
  { name: "Okinawa", lat: 26.5, lon: 127.9 },
  { name: "Taiwan", lat: 23.7, lon: 121.0 },
  { name: "the Philippines", lat: 12.9, lon: 121.8 },
  { name: "Guam", lat: 13.4, lon: 144.8 },
  { name: "South Korea", lat: 35.9, lon: 127.8 },
  { name: "eastern China", lat: 31.2, lon: 121.5 },
  { name: "Vietnam", lat: 14.1, lon: 108.3 },
  { name: "Hawaii", lat: 20.8, lon: -156.3 },
  { name: "the Aleutian Islands", lat: 52.0, lon: -176.0 },
  { name: "Alaska", lat: 61.2, lon: -149.9 },
  { name: "the Pacific Northwest", lat: 45.5, lon: -122.7 },
  { name: "California", lat: 36.8, lon: -119.7 },
  { name: "Mexico", lat: 23.6, lon: -102.5 },
  { name: "the Gulf of Mexico", lat: 25.0, lon: -90.0 },
  { name: "Florida", lat: 27.8, lon: -81.7 },
  { name: "the Carolinas", lat: 34.0, lon: -80.9 },
  { name: "the Bahamas", lat: 24.3, lon: -76.6 },
  { name: "Cuba", lat: 21.5, lon: -79.5 },
  { name: "the Yucatan Peninsula", lat: 19.6, lon: -88.2 },
  { name: "Brazil", lat: -10.3, lon: -53.2 },
  { name: "Portugal", lat: 39.4, lon: -8.2 },
  { name: "the Canary Islands", lat: 28.3, lon: -16.5 },
  { name: "Iceland", lat: 64.9, lon: -19.0 },
  { name: "Greenland", lat: 71.7, lon: -42.6 },
  { name: "Antarctica", lat: -75.0, lon: 0.0 },
  { name: "India", lat: 20.6, lon: 78.9 },
  { name: "Sri Lanka", lat: 7.9, lon: 80.8 },
  { name: "the Bay of Bengal", lat: 15.0, lon: 88.0 },
  { name: "Indonesia", lat: -0.8, lon: 113.9 },
  { name: "Papua New Guinea", lat: -6.3, lon: 143.9 },
  { name: "Fiji", lat: -17.7, lon: 178.1 },
  { name: "the Marshall Islands", lat: 7.1, lon: 171.2 },
  { name: "Australia", lat: -25.3, lon: 133.8 },
  { name: "New Zealand", lat: -41.0, lon: 174.9 },
  { name: "Madagascar", lat: -18.8, lon: 46.9 },
  { name: "the Horn of Africa", lat: 8.0, lon: 47.0 }
];
function toRad(d) { return (d * Math.PI) / 180; }
function toDeg(r) { return (r * 180) / Math.PI; }
function haversineMiles(lat1, lon1, lat2, lon2) {
  const R = 3958.8;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}
function bearingFrom(lat1, lon1, lat2, lon2) {
  const phi1 = toRad(lat1), phi2 = toRad(lat2);
  const dLon = toRad(lon2 - lon1);
  const y = Math.sin(dLon) * Math.cos(phi2);
  const x = Math.cos(phi1) * Math.sin(phi2) - Math.sin(phi1) * Math.cos(phi2) * Math.cos(dLon);
  return (toDeg(Math.atan2(y, x)) + 360) % 360;
}
const EIGHT_POINT_WORDS = ["north", "northeast", "east", "southeast", "south", "southwest", "west", "northwest"];
function describeLocationByLandmark(lat, lon) {
  let nearest = null, nearestDist = Infinity;
  for (const lm of LANDMARKS) {
    const d = haversineMiles(lat, lon, lm.lat, lm.lon);
    if (d < nearestDist) { nearestDist = d; nearest = lm; }
  }
  if (!nearest) {
    return `near ${Math.abs(lat).toFixed(1)} degrees ${lat >= 0 ? "north" : "south"}, ${Math.abs(lon).toFixed(1)} degrees ${lon >= 0 ? "east" : "west"}`;
  }
  // CHANGED 2026-08-14: was always Math.round(dist/10)*10, which produced
  // "1510 miles southwest of California" -- false precision (the landmark is
  // a single representative point for a whole region, so the tens digit is
  // meaningless) and awkward aloud, since TTS reads 1510 as "fifteen ten".
  // Scale the rounding to the distance so it stays honest about how precise
  // this estimate actually is.
  let miles;
  if (nearestDist >= 1000) miles = Math.round(nearestDist / 100) * 100;
  else if (nearestDist >= 300) miles = Math.round(nearestDist / 50) * 50;
  else miles = Math.round(nearestDist / 10) * 10;
  if (miles === 0) return `right near ${nearest.name}`;
  const brg = bearingFrom(nearest.lat, nearest.lon, lat, lon);
  const word = EIGHT_POINT_WORDS[Math.round(brg / 45) % 8];
  return `${miles} miles ${word} of ${nearest.name}`;
}

function utcDayKey(now = new Date()) {
  return now.toISOString().slice(0, 10);
}
function utcMonthKey(now = new Date()) {
  return now.toISOString().slice(0, 7);
}

async function readSourceBlob(ns, key, label) {
  if (!ns) return { label, ok: false, error: "KV binding not present", stories: [] };
  try {
    const raw = await ns.get(key);
    if (!raw) return { label, ok: false, error: "key empty", stories: [] };
    const data = JSON.parse(raw);
    const byId = new Map();
    for (const arr of [data.breaking?.last24h, data.astronomy, data.missions, data.policy]) {
      if (!Array.isArray(arr)) continue;
      for (const s of arr) {
        if (s?.id && !byId.has(s.id)) {
          byId.set(s.id, {
            id: s.id,
            title: s.headline || "",
            summary: s.whyItMatters || s.summary || "",
            kind: (s.category || "").toLowerCase(),
            // ADDED 2026-08-08 (feedback #2): carry the real outlet name
            // through so condense()/the model can cite it. Previously
            // dropped here, meaning attribution was structurally impossible
            // no matter what the prompt said.
            sourceName: s.sourceName || null
          });
        }
      }
    }
    const stories = [...byId.values()];
    return {
      label, ok: true,
      lastUpdated: data.meta?.lastUpdated || data.meta?.version || null,
      count: stories.length, stories, raw: data
    };
  } catch (err) {
    return { label, ok: false, error: `parse failed: ${err.message}`, stories: [] };
  }
}

async function readEarthBlob(ns, key) {
  const label = "earth";
  if (!ns) return { label, ok: false, error: "KV binding not present", events: [] };
  try {
    const raw = await ns.get(key);
    if (!raw) return { label, ok: false, error: "key empty", events: [] };
    const data = JSON.parse(raw);
    const why = data.whyItMatters || {};
    const events = [];
    for (const [region, block] of [["nmsz", data.nmsz], ["yellowstone", data.yellowstone]]) {
      if (!block || !Array.isArray(block.events)) continue;
      block.events.forEach((ev, i) => {
        const id = ev.id || `${region}-${i}`;
        const facts = [];
        if (ev.magnitude || ev.mag) facts.push(`magnitude ${ev.magnitude || ev.mag}`);
        if (ev.depth || ev.depthKm) facts.push(`depth ${ev.depth || ev.depthKm} km`);
        if (ev.place || ev.location || ev.region) {
          facts.push(speakableUsgsPlace(String(ev.place || ev.location || ev.region)));
        }
        if (ev.time || ev.date) {
          const d = new Date(ev.time || ev.date);
          if (!Number.isNaN(d.getTime())) facts.push(d.toISOString().slice(0, 10));
        }
        // CHANGED 2026-08-13: was `ev.title || ev.summary || "<region> event"`.
        // USGS objects (from parseUSGSFeatureCollection) have NO title and NO
        // summary field -- only id/place/mag/time/url/lon/lat -- so every NMSZ
        // and Yellowstone event reached the model titled literally "nmsz
        // event" / "yellowstone event". Build a real title from place, which
        // does exist.
        const regionLabel = region === "nmsz" ? "New Madrid seismic zone" : "Yellowstone";
        const placeStr = ev.place || ev.location || ev.region;
        events.push({
          id, kind: region,
          title: ev.title || (placeStr ? speakableUsgsPlace(String(placeStr)) : `${regionLabel} event`),
          summary: ev.description || ev.summary || facts.join(", "),
          whyItMatters: why[id] || ""
        });
      });
    }
    for (const q of data.quakes?.significant || []) {
      const id = q.id || `quake-${q.title || q.place || Math.random().toString(36).slice(2, 8)}`;
      // CHANGED 2026-08-13: summary was `q.description || ""`, but USGS
      // significant-quake objects have no `description` field -- so summary
      // was ALWAYS "", which meant hasReportableFact() filtered every
      // significant earthquake out of the digest unless the Haiku
      // intelligence pass happened to write a whyItMatters for that exact id.
      // Worse, magnitude never reached the script at all even when one did
      // survive. Build the same facts list the NMSZ path uses above, so the
      // single most important fact about an earthquake actually gets through.
      const qFacts = [];
      if (q.magnitude || q.mag) qFacts.push(`magnitude ${q.magnitude || q.mag}`);
      if (q.depth || q.depthKm) qFacts.push(`depth ${q.depth || q.depthKm} km`);
      if (q.place) qFacts.push(speakableUsgsPlace(String(q.place)));
      if (q.time) {
        const d = new Date(q.time);
        if (!Number.isNaN(d.getTime())) qFacts.push(d.toISOString().slice(0, 10));
      }
      events.push({
        id, kind: "earthquake",
        // CHANGED 2026-08-08 (feedback #1): q.title/q.place is the raw USGS
        // place string ("3km SSW of X") -- run it through
        // speakableUsgsPlace() so the spoken title is clean.
        title: speakableUsgsPlace(q.title || q.place || "Significant earthquake"),
        summary: q.description || qFacts.join(", "),
        whyItMatters: why[id] || ""
      });
    }
    const se = data.surfaceEvents || {};
    for (const kind of ["wildfires", "storms", "ice"]) {
      for (const ev of se[kind] || []) {
        const facts = [];
        if (ev.description) {
          facts.push(ev.description);
        } else {
          if (typeof ev.lat === "number" && typeof ev.lon === "number") {
            // CHANGED 2026-08-09 (feedback #1): was raw "X.X deg N, Y.Y deg E"
            // -- now a landmark-relative distance/bearing, see
            // describeLocationByLandmark() above.
            facts.push(`last tracked ${describeLocationByLandmark(ev.lat, ev.lon)}`);
          }
          if (ev.date) {
            const d = new Date(ev.date);
            if (!Number.isNaN(d.getTime())) facts.push(`as of ${d.toISOString().slice(0, 10)}`);
          }
        }
        events.push({ id: ev.id, kind, title: ev.title || "", summary: facts.join(", "), whyItMatters: why[ev.id] || "" });
      }
    }
    for (const v of data.volcanoes?.entries || []) {
      events.push({
        id: v.id, kind: "volcano",
        title: `${v.name}, ${v.country} -- ${v.status}`,
        summary: (v.description || "").trim(),
        whyItMatters: why[v.id] || ""
      });
    }
    // ADDED 2026-08-13: data.news was never read here. earthIngest fetches 5
    // earth-science news feeds every run (NOAA Ocean Service, NASA Earth
    // Observatory, NOAA News, AGU Eos, USGS National News) and keeps up to 12
    // items per category -- none of it has ever reached the podcast. This is
    // the main reason episodes drift to all-space on days with no quakes or
    // storms: hazard events are the ONLY earth content the script could see,
    // so a quiet hazard day meant no earth content at all, on a show called
    // Earth and Orbit. These items carry a real outlet name in `source`, so
    // they also feed the [outlet=...] attribution path like space stories do.
    const newsBlock = data.news || {};
    for (const category of ["ocean", "atmosphere", "geography"]) {
      for (const n of newsBlock[category] || []) {
        if (!n?.id || !n?.title) continue;
        events.push({
          id: n.id,
          kind: `news-${category}`,
          title: n.title,
          summary: (n.summary || "").trim(),
          sourceName: n.source || null,
          whyItMatters: why[n.id] || ""
        });
      }
    }
    return {
      label, ok: true,
      lastUpdated: data.meta?.generatedAt || null,
      count: events.length, events,
      pulse: data.pulse || null,
      briefingRaw: data.briefing || "",
      raw: data
    };
  } catch (err) {
    return { label, ok: false, error: `parse failed: ${err.message}`, events: [] };
  }
}

function ageHours(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  return Math.round((Date.now() - t) / 3600000);
}

function condense(story, max) {
  const title = toSpeakableAscii(story.title || story.headline || "");
  // CHANGED 2026-08-23 (closing gap #3): was `story.summary || story.whyItMatters
  // || story.description`, which meant whyItMatters was ONLY used when summary
  // was empty -- any story that had BOTH a summary and a real whyItMatters
  // silently lost the whyItMatters entirely. That's most stories, since
  // summary is usually populated. This is why "why this matters" framing was
  // inconsistent in transcripts despite the field existing and being
  // populated upstream: the digest simply wasn't passing it through. Now both
  // reach the model, clearly separated, so the model can draw on the
  // significance framing instead of only the raw fact.
  const rawSummary = toSpeakableAscii(story.summary || story.description || "");
  const rawWhy = toSpeakableAscii(story.whyItMatters || "");
  let summary;
  if (rawSummary && rawWhy) {
    summary = `${rawSummary} | why it matters: ${rawWhy}`;
  } else {
    summary = rawSummary || rawWhy;
  }
  if (!title) return null;
  if (!summary.trim()) return null;
  // ADDED 2026-08-08 (feedback #2): surface the outlet name as a tag the
  // model can use for attribution. Only added when a real sourceName is
  // present -- never fabricated downstream in the prompt.
  const outletTag = story.sourceName ? ` [outlet=${toSpeakableAscii(story.sourceName)}]` : "";
  return `- id=${story.id}${outletTag} ${title}: ${summary.slice(0, max)}`;
}

function hasReportableFact(story) {
  const summary = (story.summary || story.whyItMatters || story.description || "").toString().trim();
  return summary.length > 0;
}

function pickTopEarthEvents(earth, max) {
  const weight = (e) => {
    if (e.kind === "nmsz" || e.kind === "yellowstone") return 100;
    if (e.kind === "earthquake") return 90;
    if (e.kind === "storms") return 70;
    if (e.kind === "wildfires") return 60;
    if (e.kind === "volcano") return /new eruptive/i.test(e.title) ? 55 : 20;
    // ADDED 2026-08-13: news items rank below live hazards (a real quake or
    // storm should always outrank a news story) but above routine
    // "continuing activity" volcano entries and ice events -- so on a quiet
    // hazard day there's still genuine earth content to carry the episode,
    // instead of the digest falling through to space-only.
    if (e.kind === "news-ocean" || e.kind === "news-atmosphere" || e.kind === "news-geography") return 40;
    if (e.kind === "ice") return 30;
    return 10;
  };
  return earth.events.filter(hasReportableFact).sort((a, b) => weight(b) - weight(a)).slice(0, max);
}

// ADDED 2026-08-23 (closing gap #1, continuity): read yesterday's stored
// episode so today's script can acknowledge an ongoing story ("Dolphin,
// which we mentioned yesterday...") instead of restarting from zero every
// single day. Deliberately returns the prior script as reference material
// only, never as something merged into today's fact-checkable digest --
// see the CONTINUITY section in SYSTEM_PROMPT and the explicit labeling
// in buildSourceDigest below for the guardrail that keeps this from
// becoming a second, unaudited source of claims.
async function readYesterdayEpisode(env, day) {
  try {
    const d = new Date(`${day}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() - 1);
    const yKey = d.toISOString().slice(0, 10);
    const raw = await env.PODCAST_KV.get(`podcast:episode:${yKey}`);
    if (!raw) return null;
    const ep = JSON.parse(raw);
    if (!ep.script) return null;
    // ADDED 2026-08-23: also carry forward yesterday's storm fingerprint (see
    // buildStormFingerprint below) so today's run can detect a storm whose
    // reported position hasn't moved since yesterday -- a real accuracy
    // problem (the Aug 22 episode reported Lala and Saudel at the exact same
    // distances as Aug 21, which storms simply don't do), not just a style
    // one. Separate from the CONTINUITY feature above, which is about
    // phrasing a callback, not detecting whether the underlying data is
    // actually stale.
    return { day: yKey, script: ep.script, stormFingerprint: ep.stormFingerprint || null };
  } catch {
    return null;
  }
}

// ADDED 2026-08-23: fingerprint storm-kind earth events by their condensed
// fact line. Scoped to storms specifically (not quakes, not quiet-region
// status) because those are legitimately allowed to repeat unchanged day to
// day -- a quake is a fixed past event, "NMSZ: quiet" can be true for a week
// straight, but a tracked storm reporting the identical distance/bearing two
// days running means the source data didn't refresh, not that nothing
// changed.
function buildStormFingerprint(earth) {
  const fp = {};
  for (const e of earth.events || []) {
    if (e.kind !== "storms") continue;
    const line = condense(e, 400);
    if (line) fp[e.id] = line;
  }
  return fp;
}

function detectStaleStorms(todayFingerprint, yesterdayFingerprint) {
  if (!yesterdayFingerprint) return [];
  const stale = [];
  for (const [id, line] of Object.entries(todayFingerprint)) {
    if (yesterdayFingerprint[id] && yesterdayFingerprint[id] === line) {
      stale.push({ id, line });
    }
  }
  return stale;
}

function buildSourceDigest(space, earth, yesterday = null) {
  const lines = [];
  const spaceItems = space.stories.filter(hasReportableFact).slice(0, STORIES_FROM_SPACE).map((s) => condense(s, 400)).filter(Boolean);
  const earthItems = pickTopEarthEvents(earth, STORIES_FROM_EARTH).map((s) => condense(s, 400)).filter(Boolean);
  lines.push("TODAY'S SOURCE MATERIAL (order and select freely -- do not group by SOURCE tag; it's provenance, not a section header. The [outlet=...] tag on some items is the real outlet name -- see SOURCE ATTRIBUTION in your instructions for how to use it):");
  const allItems = [...spaceItems, ...earthItems];
  if (allItems.length) {
    lines.push(...spaceItems.map((l) => l.replace("- id=", "- SOURCE=space id=")));
    lines.push(...earthItems.map((l) => l.replace("- id=", "- SOURCE=earth id=")));
  } else {
    lines.push("- (no stories available today)");
  }
  const quietNotes = [];
  if (earth.raw?.nmsz?.status === "quiet" && !earthItems.some((l) => l.includes("id=nmsz"))) {
    quietNotes.push("New Madrid seismic zone: quiet, no events in the last 24 hours.");
  }
  if (earth.raw?.yellowstone?.status === "quiet" && !earthItems.some((l) => l.includes("id=yellowstone"))) {
    quietNotes.push("Yellowstone: quiet, no events in the last 24 hours.");
  }
  if (quietNotes.length) {
    lines.push("");
    lines.push(`REGIONAL STATUS (mention only if it fits naturally, do not force it): ${quietNotes.join(" ")}`);
  }
  const sky = space.raw?.tonight;
  if (sky) {
    const bits = [];
    if (sky.moonPhase) bits.push(`moon phase ${toSpeakableAscii(sky.moonPhase)}`);
    if (sky.issNextVisiblePass?.visible) {
      const t = sky.issNextVisiblePass.riseTime;
      const local = t ? new Date(t).toLocaleTimeString("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit" }) : "";
      bits.push(`an ISS pass visible tonight from St. Louis around ${toSpeakableAscii(local)}`);
    }
    if (bits.length) {
      lines.push("");
      lines.push(`TONIGHT'S SKY (optional, use only if it fits the close): ${bits.join(", ")}.`);
    }
  }
  // ADDED 2026-08-23 (closing gap #1, continuity): yesterday's script,
  // clearly separated and explicitly scoped so it can't be mistaken for
  // fact-checkable source material. See the CONTINUITY section in
  // SYSTEM_PROMPT for the actual usage rule.
  if (yesterday?.script) {
    lines.push("");
    lines.push(`YESTERDAY'S EPISODE (${yesterday.day}) -- for continuity callbacks ONLY, per the CONTINUITY rule in your instructions. This is NOT source material: never cite a fact from it that isn't also present in today's material above.`);
    lines.push(yesterday.script.slice(0, 4000));
  }
  return lines.join("\n");
}

function auditClaims(script, claims, validIds, sourceText, reflection = "", quote = null) {
  const flags = [];
  const cited = Array.isArray(claims) ? claims : [];
  for (const c of cited) {
    if (!c || !c.sourceId) {
      flags.push({ type: "uncited-claim", detail: String(c?.text || "").slice(0, 80) });
    } else if (!validIds.has(c.sourceId)) {
      flags.push({ type: "invented-source-id", id: c.sourceId, detail: String(c.text || "").slice(0, 80) });
    }
  }
  if (!cited.length) flags.push({ type: "no-claims-returned" });
  // ADDED 2026-08-13: nothing in the audit ever checked length, so a run of
  // ~410-word episodes (46% of target) shipped for a week with no signal
  // anywhere -- it was only caught by reading a transcript by hand. This
  // flags it into diagnostics and /health's podcastLastError path like any
  // other quality issue. Deliberately a flag, not a throw: a short episode
  // is still a publishable episode, and failing the run would trade a
  // three-minute show for no show at all.
  const scriptWords = script.trim().split(/\s+/).filter(Boolean).length;
  if (scriptWords < 700) {
    flags.push({ type: "script-too-short", detail: `${scriptWords} words (stories section; target 700+)` });
  }
  const timeRe = /\b(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?)?|\b(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+(?:in the (?:morning|afternoon|evening)|o'clock)|\d{1,2}\s*(?:a\.?m\.?|p\.?m\.?))/gi;
  const haystack = sourceText.toLowerCase();
  for (const m of script.matchAll(timeRe)) {
    const t = m[0].trim().toLowerCase();
    const digits = t.match(/\d{1,2}:\d{2}/)?.[0];
    const present = digits ? haystack.includes(digits) : haystack.includes(t);
    if (!present) flags.push({ type: "unsourced-time", detail: m[0].trim() });
  }
  const CONNECTOR_CRUTCHES = ["meanwhile,", "also,", "in addition,", "elsewhere,", "speaking of", "in other news", "turning now to", "next up", "moving on to"];
  const scriptLower = script.toLowerCase();
  for (const phrase of CONNECTOR_CRUTCHES) {
    if (scriptLower.includes(phrase)) flags.push({ type: "connector-crutch-word", detail: phrase });
  }
  const THEME_NAMING = ["the theme running through", "the through-line here", "the throughline here", "what connects all of this", "what ties these together", "the common thread here"];
  const reflectionLower = (reflection || "").toLowerCase();
  for (const phrase of THEME_NAMING) {
    if (reflectionLower.includes(phrase)) flags.push({ type: "bald-theme-naming", detail: phrase });
  }
  if (reflection) {
    const r = reflection.toLowerCase();
    const NUM_WORDS = /\b(three|four|five|six|seven|eight|nine|ten|eleven|twelve|twenty|thirty|forty|fifty|hundred|thousand|million|billion)\b/g;
    const stripped = quote ? reflection.split(quote.text).join(" ") : reflection;
    if (/\d/.test(stripped)) {
      flags.push({ type: "reflection-contains-digits", detail: (stripped.match(/\d[\d.,:]*/g) || []).join(", ").slice(0, 60) });
    }
    const numWords = stripped.match(NUM_WORDS);
    if (numWords) flags.push({ type: "reflection-contains-quantity", detail: [...new Set(numWords)].join(", ") });
    let checkable = reflection;
    if (quote) checkable = checkable.split(quote.text).join(" ").split(quote.who).join(" ");
    const NAMED = /\b(NASA|ISS|USGS|EONET|Geological Survey|Smithsonian|Earth Observatory|January|February|March|April|May|June|July|August|September|October|November|December|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b/g;
    const named = checkable.match(NAMED);
    if (named) flags.push({ type: "reflection-names-source-or-date", detail: [...new Set(named)].join(", ") });
    if (quote) {
      const norm = (t) => t.replace(/["\u201C\u201D]/g, "").replace(/\s+/g, " ").trim().toLowerCase();
      if (!norm(reflection).includes(norm(quote.text))) flags.push({ type: "quote-altered-or-missing", id: quote.id, detail: quote.text.slice(0, 60) });
      if (!reflection.includes(quote.who)) flags.push({ type: "quote-attribution-missing", detail: quote.who });
    }
    const CORNY = ["in the grand scheme", "puts things in perspective", "puts it in perspective", "how small we", "makes you realize", "we are all stardust", "we are stardust", "pale blue dot", "the universe is vast", "humbling", "take a moment", "next time you look", "remember that we", "speck", "cosmic dance"];
    for (const phrase of CORNY) {
      if (r.includes(phrase)) flags.push({ type: "reflection-cliche", detail: phrase });
    }
    if (/\?/.test(reflection)) flags.push({ type: "reflection-rhetorical-question" });
    const words = reflection.trim().split(/\s+/).length;
    if (words > 130) flags.push({ type: "reflection-too-long", detail: `${words} words` });
  }
  return { flags, claimCount: cited.length, clean: flags.length === 0 };
}

const QUOTES = [
  { id: "sagan-incredible", text: "Somewhere, something incredible is waiting to be known.", who: "Carl Sagan", themes: ["discovery", "space", "default"] },
  { id: "sagan-suns", text: "Across the sea of space, the stars are other suns.", who: "Carl Sagan", themes: ["space", "sky"] },
  { id: "cernan-curiosity", text: "Curiosity is the essence of our existence.", who: "Gene Cernan", themes: ["discovery", "space", "default"] },
  { id: "ride-brighter", text: "The stars don't look bigger, but they do look brighter.", who: "Sally Ride", themes: ["sky", "space"] },
  { id: "hadfield-dawn", text: "To some this may look like a sunset. But it is a new dawn.", who: "Chris Hadfield", themes: ["sky", "earth", "default"] },
  { id: "garan-smalltown", text: "Earth is a small town with many neighborhoods in a very big universe.", who: "Ron Garan", themes: ["earth", "perspective"] },
  { id: "tsiolkovsky-cradle", text: "The Earth is the cradle of humanity, but mankind cannot stay in the cradle forever.", who: "Konstantin Tsiolkovsky", themes: ["space", "exploration"] },
  { id: "jemison-imagination", text: "Never limit yourself because of others' limited imagination.", who: "Mae Jemison", themes: ["exploration", "discovery"] },
  { id: "hawking-terrestrial", text: "To confine our attention to terrestrial matters would be to limit the human spirit.", who: "Stephen Hawking", themes: ["space", "perspective"] },
  { id: "hawking-lookup", text: "Remember to look up at the stars and not down at your feet.", who: "Stephen Hawking", themes: ["sky", "perspective", "default"] },
  { id: "gagarin-looked", text: "I looked and looked but I didn't see God.", who: "Yuri Gagarin", themes: ["space", "perspective"] },
  { id: "liwei-greatwall", text: "The scenery was very beautiful. But I did not see the Great Wall.", who: "Yang Liwei", themes: ["perspective", "earth"] },
  { id: "glenn-sunsets", text: "I don't know what you could say about a day in which you have seen four beautiful sunsets.", who: "John Glenn", themes: ["sky", "perspective"] },
  { id: "earle-lifesupport", text: "Do everything you can to learn about your life support system, then everything you can to take care of it.", who: "Sylvia Earle", themes: ["earth", "fragility"] },
  { id: "lessing-dialect", text: "Space or science fiction has become a dialect for our time.", who: "Doris Lessing", themes: ["space", "discovery"] }
];

function dayTheme(space, earth) {
  const e = earth.events.length;
  const s = space.stories.length;
  const kinds = earth.events.map((x) => x.kind || "");
  if (kinds.some((k) => k === "earthquake" || k === "nmsz" || k === "yellowstone" || k === "wildfires" || k === "storms")) {
    return e >= s ? "fragility" : "earth";
  }
  if (kinds.some((k) => k === "volcano")) return "earth";
  if (space.raw?.tonight?.issNextVisiblePass?.visible) return "sky";
  if (s > e) return "space";
  return "default";
}

function pickQuote(theme, recentIds = []) {
  const recent = new Set(recentIds.slice(0, 8));
  const themed = QUOTES.filter((q) => q.themes.includes(theme) && !recent.has(q.id));
  const anyFresh = QUOTES.filter((q) => !recent.has(q.id));
  const pool = themed.length ? themed : anyFresh.length ? anyFresh : QUOTES;
  const seed = utcDayKey().split("-").join("");
  return pool[Number(seed) % pool.length];
}

const CLOSEOUTS = [
  { id: "thats-show-date", render: (date) => `That's Earth and Orbit for ${date}.` },
  { id: "thats-show-date-tomorrow", render: (date) => `That's Earth and Orbit for ${date}. Same time tomorrow.` },
  { id: "same-planet-tomorrow", render: () => "Same planet, tomorrow." },
  { id: "more-tomorrow", render: () => "More tomorrow." },
  { id: "thats-what-happened", render: () => "That's what happened today." }
];
function pickCloseout(recentIds = []) {
  const recent = new Set(recentIds.slice(0, 3));
  const fresh = CLOSEOUTS.filter((c) => !recent.has(c.id));
  const pool = fresh.length ? fresh : CLOSEOUTS;
  const seed = utcDayKey().split("-").join("") + "7";
  return pool[Number(seed) % pool.length];
}

async function nextEpisodeNumber(kv) {
  let n = 0;
  try {
    const raw = await kv.get("podcast:episode-number");
    n = raw ? JSON.parse(raw).n : 0;
  } catch { n = 0; }
  const next = n + 1;
  await kv.put("podcast:episode-number", JSON.stringify({ n: next }));
  return next;
}

async function readCreditLedger(kv) {
  const month = utcMonthKey();
  try {
    const raw = await kv.get("podcast:credits");
    const led = raw ? JSON.parse(raw) : null;
    if (led && led.month === month) return led;
    return { month, creditsUsed: 0, episodes: 0 };
  } catch { return { month, creditsUsed: 0, episodes: 0 }; }
}
async function writeCreditLedger(kv, ledger) {
  await kv.put("podcast:credits", JSON.stringify(ledger));
}

async function repairConnectorCrutches(env, script, flags, diagnostics) {
  const crutchFlags = flags.filter((f) => f.type === "connector-crutch-word");
  if (crutchFlags.length === 0) return script;
  let repaired = script;
  let repairedCount = 0;
  for (const flag of crutchFlags) {
    const sentences = repaired.match(/[^.!?]+[.!?]+/g) || [];
    const target = sentences.find((s) => s.toLowerCase().includes(flag.detail));
    if (!target) continue;
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": env.ANTHROPIC_API_KEY, "anthropic-version": ANTHROPIC_VERSION },
        body: JSON.stringify({
          model: SCRIPT_MODEL,
          max_tokens: 150,
          messages: [{
            role: "user",
            content: `Rewrite this single sentence from a spoken audio script so it does NOT use the word "${flag.detail.replace(",", "")}" or any similar connector word (also, meanwhile, elsewhere, in addition, speaking of). Keep every fact exactly as stated -- change ONLY the sentence structure/connector, nothing else. Return ONLY the rewritten sentence, no preamble, no quotes.\n\nSentence: ${target.trim()}`
          }]
        })
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const rewritten = (data.content || []).filter((b) => b.type === "text").map((b) => b.text).join("").trim();
      if (rewritten && rewritten.length > 10) {
        repaired = repaired.replace(target, ` ${rewritten} `);
        repairedCount++;
      }
    } catch (err) {
      diagnostics.push({ step: "repair", ok: false, phrase: flag.detail, error: String(err.message || err) });
    }
  }
  diagnostics.push({ step: "repair", ok: true, attempted: crutchFlags.length, repaired: repairedCount });
  return repaired;
}

// ADDED 2026-08-14: length-expansion retry. Prompt-side enforcement has now
// been tried twice (the LENGTH section was rewritten on 2026-08-13 with an
// explicit 700-word floor) and moved the number from ~412 to ~449 words of
// stories -- real improvement, but still ~64% of target, producing 3-4 minute
// episodes where 5-7 was intended. Haiku simply wraps up early on this task.
// So: measure, and if short, ask for a second pass. Same shape as
// repairConnectorCrutches above -- one extra Anthropic call, only when the
// flag actually fires, on the podcast-only cron where subrequest budget is
// not contended.
//
// The hard constraint is that expansion must not manufacture facts. The
// prompt below forbids new load-bearing content explicitly, and the caller
// re-runs auditClaims() afterward so anything invented still gets flagged.
// If the expansion fails or comes back shorter, the original is kept -- a
// short episode is strictly better than a broken or hallucinated one.
async function expandShortScript(env, script, digest, diagnostics) {
  const before = script.trim().split(/\s+/).filter(Boolean).length;
  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": env.ANTHROPIC_API_KEY, "anthropic-version": ANTHROPIC_VERSION },
      body: JSON.stringify({
        model: SCRIPT_MODEL,
        max_tokens: 2000,
        messages: [{
          role: "user",
          content: `Below is the stories section of a spoken daily briefing, and the source material it was written from. It is ${before} words. It needs to be at least 700 words.

Expand it by developing the stories that are ALREADY THERE. Do not add a new story. Do not add any new fact, number, date, place, name, or agency that is not already present in the source material below -- if a detail isn't in the source, it does not go in. Draw the additional length from the source material's own specifics: what a finding actually was, what it changes, what it doesn't yet tell us, why anyone was looking.

Keep the existing voice exactly: plain, specific, varied sentence length, dry where it's dry. Do not add transition words (meanwhile, also, in addition, elsewhere, speaking of). Do not add a conclusion or a reflection -- this is the stories section only, and something else follows it. Do not restate points in new words to pad the length.

Return ONLY the expanded stories section as plain prose. No preamble, no quotes, no markdown.

SOURCE MATERIAL:
${digest}

CURRENT STORIES SECTION:
${script}`
        }]
      })
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const expanded = (data.content || []).filter((b) => b.type === "text").map((b) => b.text).join("").trim();
    const after = expanded ? expanded.trim().split(/\s+/).filter(Boolean).length : 0;
    if (!expanded || after <= before) {
      diagnostics.push({ step: "expand", ok: false, before, after, note: "expansion returned nothing longer; keeping original" });
      return script;
    }
    diagnostics.push({ step: "expand", ok: true, before, after, outputTokens: data.usage?.output_tokens ?? null });
    return toSpeakableAscii(expanded);
  } catch (err) {
    diagnostics.push({ step: "expand", ok: false, before, error: String(err.message || err) });
    return script;
  }
}

async function generateScript(env, digest, quote, diagnostics) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": env.ANTHROPIC_API_KEY, "anthropic-version": ANTHROPIC_VERSION },
    body: JSON.stringify({
      model: SCRIPT_MODEL,
      max_tokens: MAX_SCRIPT_TOKENS,
      system: [{ type: "text", text: SYSTEM_PROMPT, cache_control: { type: "ephemeral" } }],
      messages: [{
        role: "user",
        content: `Here is today's source material. Write the briefing.\n\n${digest}\n\nQUOTATION TO USE IN THE REFLECTION, verbatim, attributed to ${quote.who}:\n"${quote.text}"`
      }]
      // NOTE: no tools block. No web_search. This call must never search.
    })
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Anthropic HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json();
  diagnostics.push({
    step: "script", ok: true,
    inputTokens: data.usage?.input_tokens ?? null,
    outputTokens: data.usage?.output_tokens ?? null,
    cacheRead: data.usage?.cache_read_input_tokens ?? 0,
    stopReason: data.stop_reason
  });
  let text = (data.content || []).filter((b) => b.type === "text").map((b) => b.text).join("\n").replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "").trim();
  let script = "", reflection = "", imagePrompt = "", claims = [];
  try {
    const parsed = JSON.parse(text);
    script = parsed.script || "";
    reflection = parsed.reflection || "";
    imagePrompt = parsed.imagePrompt || "";
    claims = parsed.claims || [];
    if (!reflection) diagnostics.push({ step: "script", ok: true, note: "no reflection returned" });
    if (!imagePrompt) diagnostics.push({ step: "script", ok: true, note: "no imagePrompt returned -- cover art will fall back to static cover.jpg" });
  } catch {
    diagnostics.push({ step: "script", ok: true, note: "response was not valid JSON; using raw text, claims unaudited, no image prompt" });
    script = text;
  }
  return {
    script: toSpeakableAscii(script),
    reflection: toSpeakableAscii(reflection),
    imagePrompt: toSpeakableAscii(imagePrompt),
    claims
  };
}

const IMAGE_MODEL = "@cf/black-forest-labs/flux-1-schnell";
async function generateCoverImage(env, imagePrompt, diagnostics) {
  if (!imagePrompt) {
    diagnostics.push({ step: "image", ok: false, error: "no imagePrompt from script generation; skipping" });
    return null;
  }
  if (!env.AI) {
    diagnostics.push({ step: "image", ok: false, error: "AI binding not present; skipping (episode still publishes with static cover.jpg)" });
    return null;
  }
  try {
    // CHANGED 2026-08-22: was `num_steps: 4`. Confirmed via Cloudflare's
    // current flux-1-schnell docs that the accepted parameter is `steps`
    // (integer, default 4, max 8) -- `num_steps` was never valid against
    // this model's schema and produced a hard validation rejection:
    //   5006: Error: Additional or unevaluated properties '/num_steps' at
    //   '/' not allowed
    // This is why every episode from roughly Aug 15 onward shipped with no
    // cover image: not a subrequest-budget issue, not a Workers AI quota
    // issue (both of which were reasoned about and deliberately NOT acted on
    // without confirmation) -- a flat, deterministic API rejection that
    // fires identically regardless of content or invocation size, which
    // matches the observed 100% failure rate exactly.
    const result = await env.AI.run(IMAGE_MODEL, { prompt: imagePrompt, steps: 4 });
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
    if (bytes.byteLength < 500) throw new Error(`image response only ${bytes.byteLength} bytes -- treating as failure`);
    diagnostics.push({ step: "image", ok: true, bytes: bytes.byteLength, model: IMAGE_MODEL });
    return bytes;
  } catch (err) {
    // ADDED 2026-08-15: image failures previously only existed in the
    // `diagnostics` array, which is returned to the CALLER of /trigger but is
    // never persisted anywhere for a scheduled (cron) run -- meaning every
    // "no poster today" day up to now was structurally undiagnosable after
    // the fact. This log line is at minimum visible live via `wrangler tail`;
    // the episode.imageError field added below is what makes it visible
    // after the fact too.
    console.error(`[stl-dispatcher] cover image generation failed: ${err.message}`);
    diagnostics.push({ step: "image", ok: false, error: err.message });
    return null;
  }
}

// ADDED 2026-08-14: TTS-only prosody pass. toSpeakableAscii() converts em
// dashes to plain ASCII hyphens (correct -- ElevenLabs mispronounces the
// unicode ones), but that leaves the script full of " - " constructions.
// ElevenLabs treats a spaced hyphen as a hard break, so an episode reading
// smoothly on the page came out audibly choppy: the Aug 14 script had 7 of
// them in the body alone. Convert them into real punctuation the voice model
// already handles naturally.
//
// Deliberately applied ONLY to the string handed to synthesizeSpeech, never
// to the episode.script stored in KV -- the written transcript on
// pod.stluker.com reads better with the dashes intact, and rewriting stored
// text would also desync it from what the audit already validated.
function normalizeForTts(text) {
  return String(text)
    // " - " or " -- " mid-sentence: comma carries the same pause without the
    // hard stop. The multi-hyphen case matters because the fixed welcome line
    // uses " -- ", so it was hitting every single episode.
    .replace(/ +-{1,3} +/g, ", ")
    // Hyphen jammed between two words with no spaces ("one-the crowd") is an
    // em dash that lost its spacing; TTS runs the words together.
    .replace(/([a-z,;:])-([A-Za-z])/g, "$1, $2")
    // Collapse any doubled punctuation the substitutions above may create.
    .replace(/,\s*,/g, ",")
    .replace(/,\s*([.!?])/g, "$1")
    .replace(/ {2,}/g, " ")
    .trim();
}

async function synthesizeSpeech(env, script, diagnostics) {
  const voiceId = env.ELEVENLABS_VOICE_ID;
  const url = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}?output_format=mp3_44100_128`;
  // Normalize here rather than at the call site so the caller's `script`
  // variable (which is what gets stored as episode.script) is never mutated.
  const spoken = normalizeForTts(script);
  const dashesFixed = (script.match(/ +- +/g) || []).length;
  diagnostics.push({ step: "tts-normalize", ok: true, spacedDashesConverted: dashesFixed, charsBefore: script.length, charsAfter: spoken.length });
  const res = await fetch(url, {
    method: "POST",
    headers: { "xi-api-key": env.ELEVENLABS_API_KEY, "content-type": "application/json", accept: "audio/mpeg" },
    body: JSON.stringify({ text: spoken, model_id: TTS_MODEL, voice_settings: VOICE_SETTINGS })
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`ElevenLabs HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const buf = await res.arrayBuffer();
  diagnostics.push({ step: "tts", ok: true, bytes: buf.byteLength, model: TTS_MODEL });
  if (buf.byteLength < 20000) throw new Error(`ElevenLabs returned only ${buf.byteLength} bytes -- treating as failure`);
  return buf;
}

export async function runPodcastIngest(env, opts = {}) {
  const diagnostics = [];
  const day = utcDayKey();
  const result = { ran: false, ok: false, day, diagnostics };
  try {
    for (const k of ["ANTHROPIC_API_KEY", "ELEVENLABS_API_KEY", "ELEVENLABS_VOICE_ID"]) {
      if (!env[k]) throw new Error(`missing secret: ${k}`);
    }
    if (!env.PODCAST_KV) throw new Error("missing binding: PODCAST_KV");
    if (!env.POD_BUCKET) throw new Error("missing binding: POD_BUCKET (R2)");

    const existing = await env.PODCAST_KV.get(`podcast:episode:${day}`);
    if (existing && !opts.force) {
      diagnostics.push({ step: "idempotency", ok: true, note: "episode already exists for this UTC day, skipping" });
      result.ran = false; result.ok = true; result.skipped = "already-generated";
      return result;
    }
    let reusedEpisodeNumber = null;
    if (existing) {
      try { reusedEpisodeNumber = JSON.parse(existing).episodeNumber || null; } catch {}
    }

    const space = await readSourceBlob(env.SPACE_KV, "space-data", "space");
    const earth = await readEarthBlob(env.EARTH_KV, "earth-data");
    diagnostics.push({
      step: "sources",
      space: { ok: space.ok, count: space.count ?? 0, ageHours: ageHours(space.lastUpdated), error: space.error || null },
      earth: { ok: earth.ok, count: earth.count ?? 0, ageHours: ageHours(earth.lastUpdated), error: earth.error || null }
    });

    const totalStories = (space.stories.length || 0) + (earth.events.length || 0);
    if (totalStories < 3) {
      diagnostics.push({ step: "gate", ok: false, note: `only ${totalStories} source items (space stories + earth events), need 3+; skipping today` });
      result.ok = true; result.skipped = "insufficient-source-material";
      return result;
    }

    const ledger = await readCreditLedger(env.PODCAST_KV);
    const estChars = Math.min(TARGET_WORDS * 6.2, MAX_SCRIPT_CHARS);
    const estCredits = Math.ceil(estChars * CREDITS_PER_CHAR);
    if (ledger.creditsUsed + estCredits > MONTHLY_CREDIT_BUDGET) {
      diagnostics.push({ step: "budget", ok: false, note: `monthly ElevenLabs budget would be exceeded (${ledger.creditsUsed} used + ~${estCredits} est > ${MONTHLY_CREDIT_BUDGET}); skipping` });
      result.ok = true; result.skipped = "monthly-credit-budget"; result.ledger = ledger;
      return result;
    }

    result.ran = true;
    // ADDED 2026-08-23 (closing gap #1, continuity): failure here should
    // never block the episode -- readYesterdayEpisode already catches its
    // own errors and returns null, so a missing or malformed prior episode
    // just means no callback today, not a failed run.
    const yesterday = await readYesterdayEpisode(env, day);
    diagnostics.push({ step: "continuity", ok: true, hasYesterday: !!yesterday, yesterdayDay: yesterday?.day || null });

    // ADDED 2026-08-23: staleness check, separate from continuity above.
    // Prompted by the Aug 22 episode reporting Lala at 1100 miles west of
    // Hawaii and Saudel at 300 miles east of Guam -- the EXACT distances
    // reported on Aug 21, for storms that should have moved on the order of
    // hundreds of miles in 24 hours. This does not throw or block the
    // episode -- a stale-but-real fact is still better than no episode --
    // but it surfaces the problem loudly, in diagnostics and persisted on
    // the episode record, the same way imageError made the image failures
    // checkable after the fact instead of invisible.
    const todayStormFingerprint = buildStormFingerprint(earth);
    const staleStorms = detectStaleStorms(todayStormFingerprint, yesterday?.stormFingerprint);
    if (staleStorms.length) {
      diagnostics.push({
        step: "staleness", ok: false,
        note: "one or more tracked storms report the exact same position as yesterday's episode -- likely a source-data refresh failure, not a real observation",
        staleStorms
      });
    } else {
      diagnostics.push({ step: "staleness", ok: true, checked: Object.keys(todayStormFingerprint).length });
    }

    const digest = buildSourceDigest(space, earth, yesterday);
    const theme = dayTheme(space, earth);
    const quote = pickQuote(theme, ledger.recentQuotes || []);
    diagnostics.push({ step: "quote", ok: true, theme, id: quote.id, who: quote.who });
    const closeout = pickCloseout(ledger.recentCloseouts || []);
    diagnostics.push({ step: "closeout", ok: true, id: closeout.id });

    const gen = await generateScript(env, digest, quote, diagnostics);

    const niceDate = new Date(`${day}T12:00:00Z`).toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric", timeZone: "UTC" });

    // CHANGED 2026-08-08 (feedback #3): the welcome line now names the show
    // and its mission (was just the date before) -- this is still a fixed
    // template, not model-generated, same reasoning as the closeout rotation
    // below: consistency matters more than daily novelty for a branding line.
    const welcomeLine = `Welcome to today's Earth and Orbit for ${niceDate} -- where we cover some of the stories happening in space and here on Earth.`;
    // ADDED 2026-08-08 (feedback #3): fixed call-to-action pointing listeners
    // to the two source sites, inserted right before the rotated closeout
    // line. Domain spelled with " dot " rather than "." to keep TTS
    // pronunciation reliable regardless of how the model would have read it.
    // CHANGED 2026-08-09 (feedback #2): "stluker" was coming out garbled --
    // spelling the first two letters as individual letters ("S T Luker")
    // gives ElevenLabs an unambiguous pronunciation instead of guessing at
    // an unfamiliar word.
    const ctaLine = "For all of today's stories, head to space dot S T Luker dot com and earth dot S T Luker dot com.";
    const closeoutLine = closeout.render(niceDate);

    let script = gen.reflection ? `${gen.script}\n\n${gen.reflection}` : gen.script;
    const validIds = new Set([...space.stories, ...earth.events].map((s) => s.id).filter(Boolean));
    const audit = auditClaims(gen.script, gen.claims, validIds, digest, gen.reflection, quote);
    diagnostics.push({ step: "audit", ok: audit.clean, claims: audit.claimCount, flags: audit.flags });

    // CHANGED 2026-08-17: was a single expandShortScript() call. Real data
    // from Aug 13-16 shows one pass reliably narrows the gap (412->449,
    // 545->581 words) but doesn't reliably clear 700 -- Haiku seems to have a
    // similar undershoot tendency on the expansion prompt as it does on the
    // original one. Loop it, same guardrails each time: never accept a result
    // that isn't longer than what came before, never exceed MAX_ATTEMPTS, and
    // re-audit after every attempt so claims/crutch flags stay accurate. This
    // still can't manufacture facts -- expandShortScript's own prompt forbids
    // new load-bearing content on every call, looped or not -- so a script
    // that's exhausted everything the source material actually supports will
    // just stop improving and ship a bit short, which is the correct failure
    // mode; length is not more important than accuracy.
    const MAX_EXPANSION_ATTEMPTS = 3;
    let expansionAttempts = 0;
    while (
      audit.flags.some((f) => f.type === "script-too-short") &&
      expansionAttempts < MAX_EXPANSION_ATTEMPTS
    ) {
      expansionAttempts++;
      const expanded = await expandShortScript(env, gen.script, digest, diagnostics);
      if (expanded === gen.script) {
        diagnostics.push({ step: "expand-loop", ok: false, attempt: expansionAttempts, note: "no improvement; stopping loop" });
        break;
      }
      gen.script = expanded;
      script = gen.reflection ? `${gen.script}\n\n${gen.reflection}` : gen.script;
      const reAudit = auditClaims(gen.script, gen.claims, validIds, digest, gen.reflection, quote);
      diagnostics.push({ step: "audit", ok: reAudit.clean, claims: reAudit.claimCount, flags: reAudit.flags, note: `post-expansion re-audit (attempt ${expansionAttempts})` });
      audit.flags = reAudit.flags;
      audit.clean = reAudit.clean;
    }
    if (expansionAttempts > 0) {
      diagnostics.push({
        step: "expand-loop", ok: true, attempts: expansionAttempts,
        stillShort: audit.flags.some((f) => f.type === "script-too-short")
      });
    }

    if (audit.flags.some((f) => f.type === "connector-crutch-word")) {
      const repairedScriptOnly = await repairConnectorCrutches(env, gen.script, audit.flags, diagnostics);
      if (repairedScriptOnly !== gen.script) {
        gen.script = repairedScriptOnly;
        script = gen.reflection ? `${gen.script}\n\n${gen.reflection}` : gen.script;
        const reAudit = auditClaims(gen.script, gen.claims, validIds, digest, gen.reflection, quote);
        diagnostics.push({ step: "audit", ok: reAudit.clean, claims: reAudit.claimCount, flags: reAudit.flags, note: "post-repair re-audit" });
        audit.flags = reAudit.flags;
        audit.clean = reAudit.clean;
      }
    }

    if (!script || script.length < 800) {
      throw new Error(`script too short (${script.length} chars) -- refusing to spend TTS credits`);
    }
    if (script.length > MAX_SCRIPT_CHARS) {
      const cut = script.slice(0, MAX_SCRIPT_CHARS);
      script = cut.slice(0, cut.lastIndexOf(".") + 1) || cut;
      diagnostics.push({ step: "script", ok: true, note: "truncated to char ceiling" });
    }

    // CHANGED 2026-08-08 (feedback #3): ctaLine inserted between the model's
    // content and the rotated closeout.
    script = `${welcomeLine} ${script}\n\n${ctaLine} ${closeoutLine}`;

    const charCount = script.length;
    const credits = Math.ceil(charCount * CREDITS_PER_CHAR);
    const words = script.split(/\s+/).length;
    if (ledger.creditsUsed + credits > MONTHLY_CREDIT_BUDGET) {
      diagnostics.push({ step: "budget", ok: false, note: "actual script exceeded remaining budget; skipping TTS" });
      result.ok = true; result.skipped = "monthly-credit-budget-actual";
      return result;
    }

    const [audioResult, imageResult] = await Promise.allSettled([
      synthesizeSpeech(env, script, diagnostics),
      generateCoverImage(env, gen.imagePrompt, diagnostics)
    ]);
    if (audioResult.status === "rejected") throw audioResult.reason;
    const audio = audioResult.value;
    const imageBytes = imageResult.status === "fulfilled" ? imageResult.value : null;
    // ADDED 2026-08-15: capture the reason as a plain string on the episode
    // record itself (see episode.imageError below), not just in the
    // ephemeral diagnostics array. This is what makes a "no poster today"
    // day diagnosable via a plain `wrangler kv key get` after the fact,
    // instead of only being visible if someone happened to be running
    // `wrangler tail` at the exact moment the scheduled run fired.
    let imageErrorReason = null;
    if (imageResult.status === "rejected") {
      imageErrorReason = `unexpected throw: ${imageResult.reason}`;
      diagnostics.push({ step: "image", ok: false, error: imageErrorReason });
    } else if (!imageBytes) {
      const imgStep = diagnostics.find((d) => d.step === "image" && d.ok === false);
      imageErrorReason = imgStep?.error || "cover image generation failed for an unrecorded reason";
    }

    const objectKey = `episodes/${day}.mp3`;
    await env.POD_BUCKET.put(objectKey, audio, { httpMetadata: { contentType: "audio/mpeg", cacheControl: "public, max-age=31536000, immutable" } });
    diagnostics.push({ step: "r2", ok: true, key: objectKey, bytes: audio.byteLength });

    let imageKey = null;
    if (imageBytes) {
      imageKey = `episodes/${day}.jpg`;
      await env.POD_BUCKET.put(imageKey, imageBytes, { httpMetadata: { contentType: "image/jpeg", cacheControl: "public, max-age=31536000, immutable" } });
      diagnostics.push({ step: "r2-image", ok: true, key: imageKey, bytes: imageBytes.byteLength });
    }

    const episodeNumber = reusedEpisodeNumber || await nextEpisodeNumber(env.PODCAST_KV);
    diagnostics.push({ step: "episode-number", ok: true, number: episodeNumber, reused: !!reusedEpisodeNumber });

    const estSeconds = Math.round(words / 2.5);
    const episode = {
      id: day,
      episodeNumber,
      title: `Earth and Orbit -- ${new Date().toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric", timeZone: "UTC" })}`,
      pubDate: new Date().toUTCString(),
      audioKey: objectKey,
      imageKey,
      imageError: imageErrorReason,
      imagePrompt: gen.imagePrompt || null,
      bytes: audio.byteLength,
      durationSeconds: estSeconds,
      words, chars: charCount, credits,
      script,
      reflection: gen.reflection,
      quote: { id: quote.id, text: quote.text, who: quote.who, theme },
      closeoutId: closeout.id,
      audit,
      sourceCounts: { space: space.stories.length, earth: earth.events.length },
      // ADDED 2026-08-23: stormFingerprint carries forward for TOMORROW's
      // comparison (see readYesterdayEpisode/detectStaleStorms above).
      // staleStorms is TODAY's own finding, persisted so it's checkable via
      // a plain KV get after the fact, same reasoning as episode.imageError.
      stormFingerprint: todayStormFingerprint,
      staleStorms: staleStorms.length ? staleStorms : null
    };
    episode.title = toSpeakableAscii(episode.title);
    await env.PODCAST_KV.put(`podcast:episode:${day}`, JSON.stringify(episode));

    let manifest = [];
    try {
      const rawManifest = await env.PODCAST_KV.get("podcast:manifest");
      if (rawManifest) manifest = JSON.parse(rawManifest);
    } catch { manifest = []; }
    manifest = manifest.filter((e) => e.id !== day);
    manifest.unshift({
      id: episode.id, episodeNumber: episode.episodeNumber, title: episode.title, pubDate: episode.pubDate,
      audioKey: episode.audioKey, imageKey: episode.imageKey, bytes: episode.bytes, durationSeconds: episode.durationSeconds,
      blurb: script.split(/(?<=\.)\s+/).slice(0, 2).join(" ").slice(0, 400)
    });
    manifest = manifest.slice(0, 60);
    await env.PODCAST_KV.put("podcast:manifest", JSON.stringify(manifest));

    ledger.creditsUsed += credits;
    ledger.episodes += 1;
    ledger.recentQuotes = [quote.id, ...(ledger.recentQuotes || [])].slice(0, 12);
    ledger.recentCloseouts = [closeout.id, ...(ledger.recentCloseouts || [])].slice(0, 5);
    ledger.lastEpisode = day;
    await writeCreditLedger(env.PODCAST_KV, ledger);

    result.ok = true;
    result.episode = {
      id: episode.id, words, chars: charCount, credits, bytes: audio.byteLength,
      durationSeconds: estSeconds, auditClean: audit.clean, auditFlags: audit.flags.length
    };
    result.ledger = { creditsUsed: ledger.creditsUsed, budget: MONTHLY_CREDIT_BUDGET, episodes: ledger.episodes };
    return result;
  } catch (err) {
    diagnostics.push({ step: "error", ok: false, error: err.message });
    result.ok = false;
    result.error = err.message;
    return result;
  }
}
