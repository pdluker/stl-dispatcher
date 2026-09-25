// podcast-ingest.js
// Task 7 implementation for stl-dispatcher — see dispatcher.js header comment
// for the full task list and cadence. Reads SPACE_KV/EARTH_KV (written by
// Tasks 4/5 earlier in the same invocation or by the dedicated podcast-only
// cron), writes one daily "Earth and Orbit" episode: script, TTS audio, cover
// image, RSS manifest entry, and a monthly ElevenLabs credit ledger.

// CHANGED 2026-09-24: Haiku 4.5 -> Sonnet 5. Haiku repeatedly ignored the
// longer rules in SYSTEM_PROMPT (e.g. the Sep 24 reflection used the retired
// space-patience/Earth-urgency closing move). Sonnet 5 rejects temperature/
// top_p and assistant prefill (none used here) and runs adaptive thinking by
// default, which counts against max_tokens -- so the main script call gets a
// larger cap (MAX_SCRIPT_TOKENS) and the small helper calls disable thinking
// via HELPER_THINKING to keep their tight caps meaningful.
const SCRIPT_MODEL = "claude-sonnet-5";
const HELPER_THINKING = { type: "disabled" };
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
// CHANGED 2026-09-24: 2600 -> 16000 with the move to Sonnet 5, whose
// adaptive thinking shares this budget with the JSON output. Still
// non-streaming; 16k is the documented safe ceiling for that.
const MAX_SCRIPT_TOKENS = 16000;
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

AVOIDING A REPEATED FRAME - part of STEP ONE, not a separate step
Below the source material you'll find a short list of the organizing ideas used in recent episodes. Check today's instinct against that list before committing to it. If your first idea is the same kind of frame as a recent one - even reworded, even with entirely different facts plugged in - that is not a fresh angle, it's the same essay again with new names in it.

This happens most easily when today's stories are shaped like a recent day's stories. Multiple active storms at once is close to the default state during parts of the year, not an occasional coincidence, which means "several things happening simultaneously reveals something about finite capacity" is available as an angle on most days - it will keep presenting itself as the easiest connective thread precisely because it always fits. Easiest is exactly the problem: if it's on the recent list, it is no longer available to you today, no matter how well it fits.

When the obvious angle is already used, look past it deliberately. What's specifically different about WHICH things are happening today, not just that several things are happening. A different kind of connection than capacity or scale: shared history or naming, a contrast in timescale or in what can versus can't be predicted, a question one story raises that another answers, an irony, a reversal, a mechanism one story shares with another that has nothing to do with how many things are active. A genuinely different angle is worth the extra effort even when the familiar one would have been easier to write.

If, having actually looked, nothing about today's stories connects in a way that isn't already on the recent list, say so plainly in the organizingIdea field below rather than forcing a stretch. A more list-like episode that day is a smaller failure than quietly reusing last week's idea with today's facts swapped in.

STEP TWO - choose 3 to 5 stories using that idea as the filter, not "which are individually most important." Cut whichever ones don't serve it - a story that's individually interesting but doesn't fit is exactly what makes a briefing feel like a list. Fewer stories covered well, all pulling the same direction, beats more stories covered thinly.

STEP THREE - let the organizing idea show up early, not just in the reflection. The opening line or the first segue should already be operating on it, even if you don't name it outright yet. If the listener only learns why these stories belong together in the closing line, the middle of the episode already read as disconnected while they were listening to it - the reflection is a place to land the idea, not the first place you reveal it.

Order the stories by what they share, not by which pipeline they came from. A segue is not a sentence that mentions both stories. It's a sentence where the second story is already implied by how you ended the first one. If you have to write "meanwhile," "also," "in addition," "elsewhere," or "speaking of," stop - that word is doing the work your sentence structure should be doing. Delete the connector and rewrite the boundary between the two stories so it doesn't need one. This is checked after generation - a script full of these connector words reads as a failure of this instruction, not a stylistic choice.

CROSSING BETWEEN EARTH AND SPACE
The moment you move from an earth story to a space story, or back again, is a harder version of the segue problem above, and it fails differently. Two earth stories, or two space stories, usually share something on the surface - a mechanism, a place, a scale - so the segue above is enough. Earth and space material often doesn't share anything obvious, and the default failure is to finish the last earth thought and then cold-start the space story with its own attribution ("Space.com reported that...") as if it were the opening of a new segment. That seam is audible, and it's the single most common way an otherwise good episode reads as two briefings stapled together instead of one.

Before writing that sentence, find the actual bridge: a shared mechanism (both are about waiting, or about scale, or about something built specifically to watch), a contrast worth stating outright (the fast thing versus the slow thing, what we can predict versus what we can't), or a genuine thematic echo between what today's earth material and space material are actually doing - not a manufactured one bolted on after the fact. If today's earth and space stories genuinely share nothing beyond both existing, say that plainly as the pivot itself rather than cutting silently with no acknowledgment at all - that silent cut doesn't show up as a banned connector word, so it won't get caught downstream the way "meanwhile" would. It has to be caught here, while writing.

There is no single correct way to mark an honest pivot, and reusing the same one becomes exactly the kind of crutch this section exists to prevent - vary it: sometimes it's a plain admission ("this next one doesn't connect to any of that"), sometimes it's simply starting the new story on its own terms without any pivot sentence at all, sometimes it's a question the first half leaves open that the second half happens to answer. Whichever you reach for, don't reach for the same phrasing twice in a row across episodes - this is checked after generation the same way "meanwhile" is.

A briefing that regularly crosses between earth and space is exactly what makes it different from a single-topic feed, so that crossing point deserves at least as much craft as the segues within one domain - not an afterthought bolted onto whichever story happened to be generated last.

Vary the shape of each story's treatment. Not every item gets the same two-sentence setup-then-fact rhythm - that's what makes something read as a list even without list punctuation. Let one story get four sentences because it's genuinely interesting and another get one because there's nothing more to say. A story can open mid-thought. A story can be one plain sentence with no build-up at all if that's honestly all it needs.

Worked example, illustrative only, not to be reused verbatim:
LIST-LIKE (avoid this): "A magnitude six point four earthquake struck offshore Indonesia. Meanwhile, wildfires continued to burn in Alberta. In other news, Kilauea remains active."
FLOWING (aim for this): "Offshore Indonesia took the bigger shock overnight, magnitude six point four, deep enough and far out enough that nobody's counting damage yet. Alberta's fire is a slower version of the same problem - nothing sudden, just four days of not stopping. Kilauea doesn't even have that arc. It's not building toward anything. It's just still going, the way it has been for years."
Notice the second version has zero transition words and the connection is doing the work: sudden versus slow versus indefinite, as a real observation, not three unrelated facts filed one after another. Also notice the organizing idea here - different timescales of the same restlessness - is legible from the first sentence, not just at a summary at the end.

VOICE
You are an informed enthusiast who reads space and earth-science coverage obsessively, every day, so the listener doesn't have to - not a journalist performing neutrality, not a brand, not an institution reading a press release aloud. A specific person who finds this stuff genuinely interesting and has already done the reading before this recording started.

NOT breathless-AI-profound - no strained cosmic-significance turns, no forced awe, no reflection that could have been generated about any topic on any day regardless of what actually happened (this is the same failure the banned-phrase list and the retired structural move above exist to catch, stated here as the thing to avoid rather than a list of symptoms). NOT a dry aggregator either - reciting facts in the order they arrived, with no throughline and no one visibly interested in any of it, is a press release with extra steps, not a briefing from a person. The target sits between those two poles, and it's a narrower target than either extreme: interested without performing interest, informed without lecturing.

Landing description: plain, curious, unhurried. If a sentence wouldn't sound natural coming from someone who actually finds this interesting and is telling a friend about it over coffee, it's off-voice - too stiff if it reads like a wire report, too reverent if it reads like a eulogy for the universe.

Plain and specific. Short sentences next to long ones. Comfortable being interested in something without justifying why. You can be dry. You can let a thing be strange. You are not performing enthusiasm and you are not narrating a nature documentary.

Every story needs a reaction, not just a report. "Etna doesn't wait for anyone to be watching, it just goes" is the target: a plain, specific aside that shows you actually thought something about the fact, not just relayed it. This is different from the reflection at the end - it's small, local to that one story, and it happens as you go rather than being saved up. A story with only setup and fact, no reaction anywhere in it, is unfinished, even if every fact in it is accurate. The reaction can be dry, skeptical, curious, or just a plain observation - it does not need to be positive or awed.

This is also where "why it matters" belongs, and it needs to land as a real point, not a formality. When the source material hands you a why-it-matters framing, use it to say something specific about what changes or what it's evidence of - not "and this is important because it helps us understand X" as a stock closing clause. If the source material doesn't give you a why-it-matters and you can't honestly generate one from what's actually there, don't manufacture one - a story can stand on being specific and interesting without a stated stakes claim, but it should never get a vague, interchangeable one just to check the box.

The "significance note" attached to some items is a note TO YOU, not script copy. Never read it aloud as written, and never paste it in after you've already made the same point in your own words - that produces the same idea twice in a row, the second time in a flatter voice, and listeners hear the seam. Say the point once, in your own words, or skip it. Copying runs of source wording is checked after generation.

NEGATION CRUTCH. "It's not X. It's not Y. It's Z." and "That's not a finding. That's infrastructure." are one move, and it goes stale fast. Use it at most once per episode. State what a thing IS directly instead. This is checked after generation.

NO FILLER FOR THE THEME. Every sentence has to say something about the story it's in. A sentence that only exists to make a story fit the organizing idea ("There's no way to make that faster. There's no intervention possible.") gets cut. If a story needs that much help to fit, it doesn't fit.

KEEP THE OPENING'S PROMISE. Whatever frame your opening names, the stories that follow have to actually use it. Don't open by promising a three-way split or a claim about "where human spaceflight is" and then drop it after the first story.

LIVE HAZARDS. Never call an active storm, fire, or eruption boring, routine, or "not doing anything interesting" - people are in its path. If strength facts (winds, category) are supplied, use them; a storm described only by position leaves out the thing a listener most needs.

THE INFERENCE RULE - the one that matters
Two standards apply to the stories section.

LOAD-BEARING FACTS may only appear if supplied, exactly as supplied. Never estimate, round into a new number, convert, or infer:
  magnitudes, depths, distances, counts, dates, clock times, durations,
  casualty or damage figures, causes, place names, agency names, alert levels,
  what happens next,
  and how far along a technology or finding is. "Researchers developed a
  training method" does not become "it's being tested on crewed vehicles";
  a simulation result is not a flight result. Don't upgrade the stage.
  Don't invent operational details either (e.g. communication delays,
  visibility to the naked eye) that weren't supplied.
If a load-bearing fact was not supplied, the sentence needing it does not get written. Say less instead.

TEXTURE is yours: rhythm, ordering, emphasis, phrasing, segues, and observations about how supplied facts sit next to each other. You may note that six satellite passes in four days means a thing is moving. You may not decide where it is moving.

The test: could a listener act on this, or be wrong about the world because of it? Then it is load-bearing.

SOURCE ATTRIBUTION
At least once per episode - ideally on the story you open with - name where a story actually came from, using the outlet tag supplied with that item in the source material (format: [outlet=Name]). Say it plainly and naturally: "Last night, Space.com reported that..." or "According to NASA, ...". Never invent an outlet name, and never attribute a story to an outlet whose tag isn't present in the supplied material for that exact story - if a story has no outlet tag, don't attribute it to one.

HARD CAP, not a guideline: at most ONE named outlet per episode. A second is the absolute ceiling and should be rare - reach for it only when a second story's grounding genuinely needs it, not by default. Never a third, under any circumstance, and never the same outlet named twice in one episode even if it's the true source for two different stories - if that happens, name it once and let the second story stand without repeating the attribution. This is checked after generation the same way connector-crutch words are: an episode with three or more named outlets, or the same outlet named twice, reads as a citation list, not a briefing, and will get flagged regardless of how naturally each individual mention was written.

CONTINUITY
If yesterday's episode is supplied below the source material, and something in today's material is a continuation of a story yesterday's episode also covered - the same storm, the same eruption, the same investigation - say so plainly: "Dolphin, which we mentioned yesterday, has tracked another 200 miles west" reads as a show with memory instead of a fresh cold-start every single day, and a listener who hears this daily will notice the difference.

The hard rule: yesterday's episode is for phrasing a callback, never for facts. Only reference something from it if that same thing ALSO appears in today's source material with its own facts there - use today's material for the actual numbers, dates, and status, and yesterday's episode only to establish that it's a continuation. Never pull a fact, number, or status from yesterday's episode that isn't independently present in today's material; treating yesterday's script as a source would mean reporting something that may no longer be true today as if it still were. If nothing in today's material connects to yesterday's episode, don't force a callback - most days won't have one, and that's fine.

A second, equally hard rule: never reuse yesterday's sentences, even when today's version of a story is genuinely similar to yesterday's. A storm that's barely moved, or a funding story that's still developing, is real continuity - reporting it again in your own words, freshly, is exactly right. Copying yesterday's sentence structure and swapping in a new number is not continuity, it's the same paragraph twice with a different name in it, and it is checked for after generation the same way connector-crutch words are. Read yesterday's episode below for what happened and how you phrased the callback, then write today's version as if you were describing it for the first time - never as an edit of what's already there.

THE REFLECTION - different rules, read carefully
Sixty to a hundred words at the end. A thought about the earth, or our place in things, that grows out of TODAY'S material specifically. Not a general meditation with today's news pasted on top.

RETIRED STRUCTURAL MOVE - read this before anything else in this section
One specific closing move has become the default fallback across recent episodes regardless of what words dress it up: space framed as patient and timescale-agnostic, watching slow questions with no urgency, set against Earth framed as urgent and real-time, demanding immediate response - landing on the mismatch between what's watchable and what needs action. It has shown up as "longevity," as "capacity," as "observable timescales," as "watching versus responding" - different vocabulary every time, identical structure every time. This move is retired. Do not close on a contrast between space's patience and Earth's urgency, however it's phrased, for the foreseeable stretch of episodes.

If today's material doesn't hand you a genuinely different closing idea, don't fall back to this one anyway - stay with ONE specific story instead of reaching for a cross-domain contrast at all. A reflection that stays close to one concrete thing, with no space/Earth pairing in it, is better than this duality wearing new words.

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
{"script":"<opening and stories, no reflection>","reflection":"<the reflection, containing the supplied quotation verbatim>","imagePrompt":"<the cover image prompt>","organizingIdea":"<5-15 word description of today's actual connecting idea from STEP ONE, or 'no strong connection found' if that's honestly true>","claims":[{"text":"<load-bearing fact as stated>","sourceId":"<exact id it came from>"}]}
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
// CHANGED 2026-09-24: most mainland entries were geographic CENTROIDS
// (California = the Central Valley, Mexico = inland Zacatecas, India = the
// Deccan) and a few were bodies of water. That produced "1500 miles south of
// California", "550 miles south of Mexico", and "350 miles northwest of the
// Bay of Bengal" (which is on land) in the Sep 24 episode. Storms are
// described relative to the coast they threaten, so every mainland entry is
// now a named coastal city/point. Small islands keep their own coordinates.
const LANDMARKS = [
  { name: "Tokyo", lat: 35.7, lon: 139.7 },
  { name: "southern Japan", lat: 31.6, lon: 130.6 },
  { name: "Okinawa", lat: 26.5, lon: 127.9 },
  { name: "Taiwan", lat: 23.7, lon: 121.0 },
  { name: "Manila", lat: 14.6, lon: 121.0 },
  { name: "Guam", lat: 13.4, lon: 144.8 },
  { name: "Busan", lat: 35.2, lon: 129.1 },
  { name: "Shanghai", lat: 31.2, lon: 121.5 },
  { name: "Hong Kong", lat: 22.3, lon: 114.2 },
  { name: "Da Nang, Vietnam", lat: 16.1, lon: 108.2 },
  { name: "Hawaii", lat: 20.8, lon: -156.3 },
  { name: "the Aleutian Islands", lat: 52.0, lon: -176.0 },
  { name: "Anchorage", lat: 61.2, lon: -149.9 },
  { name: "Portland, Oregon", lat: 45.5, lon: -122.7 },
  { name: "San Francisco", lat: 37.8, lon: -122.4 },
  { name: "Los Angeles", lat: 34.1, lon: -118.2 },
  { name: "Cabo San Lucas", lat: 22.9, lon: -109.9 },
  { name: "Manzanillo, Mexico", lat: 19.1, lon: -104.3 },
  { name: "Acapulco", lat: 16.9, lon: -99.9 },
  { name: "Guatemala's Pacific coast", lat: 13.9, lon: -90.8 },
  { name: "Houston", lat: 29.8, lon: -95.4 },
  { name: "New Orleans", lat: 30.0, lon: -90.1 },
  { name: "Tampa", lat: 27.9, lon: -82.5 },
  { name: "Miami", lat: 25.8, lon: -80.2 },
  { name: "Cape Hatteras", lat: 35.3, lon: -75.5 },
  { name: "Bermuda", lat: 32.3, lon: -64.8 },
  { name: "the Bahamas", lat: 24.3, lon: -76.6 },
  { name: "Cuba", lat: 21.5, lon: -79.5 },
  { name: "Puerto Rico", lat: 18.2, lon: -66.6 },
  { name: "Barbados", lat: 13.2, lon: -59.5 },
  { name: "Cancun", lat: 21.2, lon: -86.8 },
  { name: "Cape Verde", lat: 16.0, lon: -24.0 },
  { name: "Lisbon", lat: 38.7, lon: -9.1 },
  { name: "the Canary Islands", lat: 28.3, lon: -16.5 },
  { name: "Iceland", lat: 64.9, lon: -19.0 },
  { name: "Greenland", lat: 71.7, lon: -42.6 },
  { name: "Antarctica", lat: -75.0, lon: 0.0 },
  { name: "Mumbai", lat: 19.1, lon: 72.9 },
  { name: "Karachi", lat: 24.9, lon: 67.0 },
  { name: "Muscat, Oman", lat: 23.6, lon: 58.4 },
  { name: "Chennai", lat: 13.1, lon: 80.3 },
  { name: "Visakhapatnam, India", lat: 17.7, lon: 83.2 },
  { name: "Kolkata", lat: 22.6, lon: 88.4 },
  { name: "Chittagong, Bangladesh", lat: 22.4, lon: 91.8 },
  { name: "Myanmar's coast", lat: 16.8, lon: 94.7 },
  { name: "Sri Lanka", lat: 7.9, lon: 80.8 },
  { name: "Indonesia", lat: -0.8, lon: 113.9 },
  { name: "Papua New Guinea", lat: -6.3, lon: 143.9 },
  { name: "Fiji", lat: -17.7, lon: 178.1 },
  { name: "the Marshall Islands", lat: 7.1, lon: 171.2 },
  { name: "Darwin, Australia", lat: -12.5, lon: 130.8 },
  { name: "Brisbane", lat: -27.5, lon: 153.0 },
  { name: "Perth", lat: -32.0, lon: 115.9 },
  { name: "New Zealand", lat: -41.0, lon: 174.9 },
  { name: "Madagascar", lat: -18.8, lon: 46.9 },
  { name: "Mozambique's coast", lat: -19.8, lon: 34.8 },
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
            // CHANGED 2026-09-24: was `s.whyItMatters || s.summary`, which
            // discarded the actual summary whenever a why-it-matters existed
            // and handed the model a single polished sentence as the story's
            // only content -- which it then read aloud verbatim (Sep 24:
            // docking, Neptune, ESA). Keep both; condense() labels them.
            summary: s.summary || "",
            whyItMatters: s.whyItMatters || "",
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
          mag: (ev.magnitude ?? ev.mag) == null ? NaN : Number(ev.magnitude ?? ev.mag),
          title: ev.title || (placeStr ? speakableUsgsPlace(String(placeStr)) : `${regionLabel} event`),
          // CHANGED 2026-09-24: region label now leads the fact line. Before,
          // the model saw only "about 9 kilometers south-southeast of Marston,
          // Missouri" and had no way to know it was in the New Madrid zone --
          // the one piece of context that makes a small tremor worth a line.
          summary: ev.description || ev.summary || [regionLabel, ...facts].join(", "),
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
          // ADDED 2026-09-24: storm strength, converted here (not by the
          // model) so the INFERENCE RULE still holds -- same principle as
          // speakableUsgsPlace(). Rounded to 5 mph; the source is itself an
          // estimate reported in 5-knot steps.
          if (typeof ev.magnitudeValue === "number" && /^kts?$/i.test(ev.magnitudeUnit || "")) {
            const mph = Math.round((ev.magnitudeValue * 1.15078) / 5) * 5;
            facts.push(`maximum sustained winds about ${mph} miles per hour`);
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
    // CHANGED 2026-09-24: label now says outright this is a note, not copy.
    summary = `${rawSummary} | significance note (paraphrase, never read verbatim): ${rawWhy}`;
  } else if (rawSummary) {
    summary = rawSummary;
  } else {
    summary = rawWhy ? `significance note (paraphrase, never read verbatim): ${rawWhy}` : "";
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

function pickTopEarthEvents(earth, max, staleIds = null) {
  const weight = (e) => {
    // CHANGED 2026-09-24: was a flat 100, so a magnitude 1.33 microquake
    // outranked every storm and news item (Sep 24 episode). Below M2.5 an
    // event is instrument-only; it drops beneath news so it's used only on a
    // genuinely quiet day. Unknown magnitude keeps the old priority.
    if (e.kind === "nmsz" || e.kind === "yellowstone") {
      return Number.isFinite(e.mag) && e.mag < 2.5 ? 25 : 100;
    }
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
  // ADDED 2026-08-28: deprioritize (never exclude) an event confirmed
  // byte-identical to yesterday's episode -- see buildStormFingerprint/
  // detectStaleStorms below. Prompted by Julio holding the exact same
  // reported position for three straight days (Aug 26-28): confirmed via
  // /health that this wasn't a pipeline failure, the underlying source data
  // genuinely hadn't changed, so the fix belongs here in selection, not in
  // fixing a bug that doesn't exist. -55 drops a stale storm (base 70) below
  // every other category including ice (30), so any genuinely fresh
  // alternative wins the slot -- but it stays in the pool and can still be
  // picked if it's honestly the only thing available that day.
  const STALE_PENALTY = 55;
  const weightWithStaleness = (e) => {
    const base = weight(e);
    return staleIds && staleIds.has(e.id) ? base - STALE_PENALTY : base;
  };
  return earth.events.filter(hasReportableFact).sort((a, b) => weightWithStaleness(b) - weightWithStaleness(a)).slice(0, max);
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
    // ADDED 2026-08-28: spaceFingerprint alongside it, same reasoning applied
    // to space stories -- confirmed via /health on Aug 28 that the Aug 27-28
    // repeat of the Mars-refueling/Enceladus-hopper stories was NOT a
    // pipeline failure (spaceIngest ran on schedule both days); a story can
    // legitimately stay eligible for up to 72h (INGEST_RECENCY_HOURS) with no
    // fresher coverage. Same fix as storms: deprioritize a confirmed-repeat,
    // don't treat it as a bug.
    return { day: yKey, script: ep.script, stormFingerprint: ep.stormFingerprint || null, spaceFingerprint: ep.spaceFingerprint || null };
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

// ADDED 2026-08-28: same fingerprint approach as storms above, applied to
// space stories. Unlike earth events, space.stories has no "kind" that
// specifically means "should always change day to day" -- a story's own
// text never updates once published, so the interesting signal isn't
// whether the FACTS changed, it's whether the SAME story (by id) is still
// the one being selected with nothing fresher to replace it. Fingerprints
// every eligible story, not a subcategory, since that distinction doesn't
// apply on the space side the way storms-vs-quakes does on the earth side.
// Reuses detectStaleStorms() as-is below -- it only ever compared two plain
// id->line dictionaries, nothing storm-specific in its logic.
function buildSpaceFingerprint(space) {
  const fp = {};
  for (const s of space.stories || []) {
    const line = condense(s, 400);
    if (line) fp[s.id] = line;
  }
  return fp;
}

// ADDED 2026-08-28: space-side counterpart to pickTopEarthEvents' staleness
// penalty. Space selection has no existing weight function to subtract a
// penalty from (it's currently a plain ordered slice), so this deprioritizes
// by partitioning instead: every non-stale story keeps its original relative
// order and fills the budget first; stale stories are appended after, in
// their own original order, and only get a slot if room is left. Same
// contract as the earth-side fix -- deprioritize, never exclude.
function selectSpaceStories(stories, max, staleIds) {
  const eligible = stories.filter(hasReportableFact);
  if (!staleIds || staleIds.size === 0) return eligible.slice(0, max);
  const fresh = eligible.filter((s) => !staleIds.has(s.id));
  const stale = eligible.filter((s) => staleIds.has(s.id));
  return [...fresh, ...stale].slice(0, max);
}

function buildSourceDigest(space, earth, yesterday = null, staleEarthIds = null, staleSpaceIds = null, recentIdeas = []) {
  const lines = [];
  // CHANGED 2026-09-16: prompted by Cyclone Lowell reporting the exact same
  // position, citing the exact same Sep 11 source date, on four straight
  // transcripts (Sep 13-16). The deprioritization penalty was working
  // correctly -- Lowell just had no fresher alternative to lose to on 3 of
  // those 4 days, so the designed fallback (still include it rather than
  // drop real content) kept firing silently. That's the right selection
  // outcome; the problem was the model had no way to know it was reporting
  // stale data, so it wrote it as if freshly observed each time. Annotating
  // the digest line itself closes that gap without changing selection logic
  // at all -- the model can now choose to say "still parked at the same
  // spot" instead of repeating the identical sentence.
  const STALE_NOTE = " [NOTE: this exact fact was already reported in a recent episode with nothing new since -- if you use it, say so honestly (e.g. \"still holding the same position\") rather than presenting it as freshly observed]";
  const spaceSelected = selectSpaceStories(space.stories, STORIES_FROM_SPACE, staleSpaceIds);
  const spaceItems = spaceSelected.map((s) => {
    const line = condense(s, 400);
    if (!line) return null;
    return staleSpaceIds?.has(s.id) ? line + STALE_NOTE : line;
  }).filter(Boolean);
  const earthSelected = pickTopEarthEvents(earth, STORIES_FROM_EARTH, staleEarthIds);
  const earthItems = earthSelected.map((e) => {
    const line = condense(e, 400);
    if (!line) return null;
    return staleEarthIds?.has(e.id) ? line + STALE_NOTE : line;
  }).filter(Boolean);
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
  // ADDED 2026-09-01: recent organizing ideas, for the AVOIDING A REPEATED
  // FRAME rule in SYSTEM_PROMPT. This is what actually closes the loop --
  // without seeing what's already been used, the model has no way to know
  // "concurrent systems strain finite capacity" was also yesterday's idea
  // and the day before's, just reworded each time. Pulled from the credit
  // ledger (see ledger.recentOrganizingIdeas), not from a single day's
  // episode, since this needs a rolling multi-day window the same way
  // recentQuotes/recentCloseouts already work -- one day back isn't enough
  // to catch a 3-day drift.
  if (recentIdeas.length) {
    lines.push("");
    lines.push("RECENT ORGANIZING IDEAS (most recent first) -- do not reuse any of these as today's connecting idea, per the AVOIDING A REPEATED FRAME rule in your instructions:");
    lines.push(...recentIdeas.map((r) => `- ${r.day}: ${r.idea}`));
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
  // ADDED 2026-09-16: attribution had drifted from "once, maybe twice" soft
  // guidance to real episodes citing the same outlet twice (Sep 14:
  // SpaceDaily x2) and up to 4 total citations in one episode (Sep 15:
  // SpaceQ x2, Universe Today x2). Regex-based, same heuristic-pattern
  // approach as the connector-crutch and reflection-cliche checks below --
  // calibrated against both transcripts before inclusion: correctly counts
  // 4 hits with 2 repeats on each real over-attribution case, 1 hit on a
  // clean single-outlet episode.
  const ATTRIBUTION_PATTERNS = [
    /\baccording to (?:the )?([A-Z][\w.&']{1,35}(?:\s+[A-Z][\w.&']{1,35}){0,3})/gi,
    /\b([A-Z][\w.&']{1,35}(?:\s+[A-Z][\w.&']{1,35}){0,3})\s+(?:reported|reports|noted)\b/g,
    /reported by ([A-Z][\w.&']{1,35}(?:\s+[A-Z][\w.&']{1,35}){0,3})/gi,
  ];
  const outletHits = [];
  for (const re of ATTRIBUTION_PATTERNS) {
    let m;
    while ((m = re.exec(script))) outletHits.push(m[1].trim().replace(/\s+(that|,)$/, ""));
  }
  const outletCounts = {};
  for (const name of outletHits) outletCounts[name] = (outletCounts[name] || 0) + 1;
  const repeatedOutlets = Object.entries(outletCounts).filter(([, n]) => n > 1).map(([name]) => name);
  if (repeatedOutlets.length) {
    flags.push({ type: "outlet-repeated", detail: repeatedOutlets.join(", ") });
  }
  if (outletHits.length > 2) {
    flags.push({ type: "too-many-outlets", detail: `${outletHits.length} citations: ${outletHits.join(", ")}` });
  }
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
  // "nothing to do with" catches the "there's a separate story today that
  // has nothing to do with any of that" crutch confirmed recurring verbatim
  // across real episodes (Aug 25, Sep 13) -- the prompt suggested it as ONE
  // example of an honest pivot and the model turned it into a formula. Same
  // failure class as "meanwhile," just longer.
  const CONNECTOR_CRUTCHES = ["meanwhile,", "also,", "in addition,", "elsewhere,", "speaking of", "in other news", "turning now to", "next up", "moving on to", "nothing to do with"];
  const scriptLower = script.toLowerCase();
  for (const phrase of CONNECTOR_CRUTCHES) {
    if (scriptLower.includes(phrase)) flags.push({ type: "connector-crutch-word", detail: phrase });
  }
  // ADDED 2026-09-24: the Sep 24 episode pasted four source sentences in
  // verbatim right after paraphrasing them. Any 10-word run shared with the
  // digest is a copy -- long enough that shared place names/titles don't trip it.
  const words = (t) => t.toLowerCase().replace(/[^a-z0-9' ]+/g, " ").split(/\s+/).filter(Boolean);
  const SHINGLE = 10;
  const srcWords = words(sourceText);
  const srcShingles = new Set();
  for (let i = 0; i + SHINGLE <= srcWords.length; i++) srcShingles.add(srcWords.slice(i, i + SHINGLE).join(" "));
  const scriptWordList = words(script);
  const copied = [];
  for (let i = 0; i + SHINGLE <= scriptWordList.length; i++) {
    const s = scriptWordList.slice(i, i + SHINGLE).join(" ");
    if (srcShingles.has(s)) { copied.push(s); i += SHINGLE - 1; }
  }
  if (copied.length) flags.push({ type: "verbatim-source-copy", detail: `${copied.length} run(s): ${copied[0].slice(0, 70)}` });
  // ADDED 2026-09-24: "It's not X. It's not Y." / "That's not a finding.
  // That's infrastructure." -- ~8 in the Sep 24 episode. Prompt allows one.
  const negations = script.match(/\b(?:it's|it is|that's|that is|this is|they're|they are|none of (?:them|it) (?:is|are))\s+not\b|\b(?:it|that|this) isn't\b/gi) || [];
  if (negations.length > 2) flags.push({ type: "negation-crutch", detail: `${negations.length} uses` });
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
  // ADDED 2026-09-01: closing the fragility pool gap that forced the exact
  // same Sylvia Earle quote to repeat verbatim on Aug 31 and Sep 1 -- a pool
  // of one has no possible variation once dayTheme() lands on "fragility"
  // twice in the same 8-day recency window, which it will keep doing given
  // this account's input (multiple concurrent storms most days) reliably
  // trips the fragility branch. Both quotes below were verified via web
  // search against NASA.gov and cross-checked against multiple independent
  // secondary sources before inclusion -- verbatim accuracy matters more
  // here than usual, since THE QUOTE rule requires reproducing these exactly
  // as given with no rewording.
  { id: "collins-fragility", text: "It had an air of fragility, like something that is easily broken.", who: "Michael Collins", themes: ["earth", "fragility"] },
  { id: "merbold-terrified", text: "I was terrified by its fragile appearance.", who: "Ulf Merbold", themes: ["earth", "fragility"] },
  // ADDED 2026-09-01: not fragility-specific (it's oasis/isolation imagery,
  // not vulnerability), but the "earth" pool was also thin (4, now 5) --
  // included here to help that pool rather than left out for not fitting
  // fragility narrowly. Verified via web search; NASA's own two official
  // pages (a Kennedy Space Center retrospective and a science.nasa.gov
  // Earthrise anniversary post) both consistently render it "in the big
  // vastness" -- used that wording over a secondary site's "to the big
  // vastness" variant.
  { id: "lovell-oasis", text: "The Earth from here is a grand oasis in the big vastness of space.", who: "James Lovell", themes: ["earth", "perspective"] },
  // ADDED 2026-09-01, second verification pass: all four confirmed via web
  // search against multiple independent sources before inclusion (see
  // Schweickart's Wikiquote entry citing the original 1974 Lindisfarne
  // Conference talk; Mitchell's People Magazine, 8 April 1974 attribution;
  // Leonov's Euronews obituary and The Planetary Society's own tribute;
  // Acton's sourcing to the published compilation "The Home Planet").
  //
  // The Mitchell quote is truncated before its original closing line, which
  // ends in profanity that doesn't belong in this show -- the cut lands on
  // a genuine sentence boundary ("...look so petty."), and nothing kept was
  // altered.
  { id: "schweickart-borders", text: "You look down there and you can't imagine how many borders and boundaries you cross, again and again and again, and you don't even see them.", who: "Rusty Schweickart", themes: ["earth", "perspective"] },
  { id: "mitchell-consciousness", text: "You develop an instant global consciousness, a people orientation, an intense dissatisfaction with the state of the world, and a compulsion to do something about it. From out there on the moon, international politics look so petty.", who: "Edgar Mitchell", themes: ["earth", "fragility", "perspective"] },
  { id: "leonov-relic", text: "The Earth was small, light blue, and so touchingly alone, our home that must be defended like a holy relic.", who: "Alexei Leonov", themes: ["earth", "fragility"] },
  { id: "acton-welcoming", text: "Looking outward to the blackness of space, sprinkled with the glory of a universe of lights, I saw majesty - but no welcome. Below was a welcoming planet.", who: "Loren Acton", themes: ["earth", "fragility", "default"] },
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
          thinking: HELPER_THINKING,
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
        thinking: HELPER_THINKING,
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

// ADDED 2026-09-02: n-gram overlap detector, prompted by the Sep 1 -> Sep 2
// episode reusing 94% of a paragraph's words verbatim (storm names/numbers
// swapped, everything else identical) -- the CONTINUITY block feeds
// yesterday's full script in for callback phrasing, and nothing previously
// stopped the model from copying its sentences outright instead of writing
// fresh ones. Calibrated against real data before picking a threshold: the
// actual Sep1/Sep2 copy scored 77.8% on this metric; two genuinely unrelated
// paragraphs on different topics scored 0%. THRESHOLD is set well below the
// observed failure and well above plausible incidental overlap (shared
// proper nouns, "according to", etc.).
function computeTextOverlap(todayText, yesterdayText, n = 8) {
  const normalize = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter(Boolean);
  const todayWords = normalize(todayText);
  const yesterdayWords = normalize(yesterdayText);
  if (todayWords.length < n || yesterdayWords.length < n) return 0;
  const yesterdayGrams = new Set();
  for (let i = 0; i <= yesterdayWords.length - n; i++) yesterdayGrams.add(yesterdayWords.slice(i, i + n).join(" "));
  let matches = 0, total = 0;
  for (let i = 0; i <= todayWords.length - n; i++) {
    total++;
    if (yesterdayGrams.has(todayWords.slice(i, i + n).join(" "))) matches++;
  }
  return total > 0 ? matches / total : 0;
}
const TEXT_OVERLAP_THRESHOLD = 0.15;

// ADDED 2026-09-02: rewrite retry, same shape and same guardrails as
// expandShortScript above -- never adds a fact not already in the digest,
// never accepted if it doesn't actually improve (here: doesn't actually
// lower overlap), original kept on any failure. One attempt only; this is a
// full-script rewrite, not a single-sentence fix, so it's the most expensive
// retry in the pipeline and deliberately not looped the way expansion is.
async function rewriteForFreshness(env, script, digest, yesterdayScript, diagnostics) {
  const before = computeTextOverlap(script, yesterdayScript);
  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": env.ANTHROPIC_API_KEY, "anthropic-version": ANTHROPIC_VERSION },
      body: JSON.stringify({
        model: SCRIPT_MODEL,
        max_tokens: 2000,
        thinking: HELPER_THINKING,
        messages: [{
          role: "user",
          content: `Below is the stories section of a spoken daily briefing. It reuses too much of yesterday's episode's own sentences -- ${(before * 100).toFixed(0)}% of it overlaps with yesterday's wording, even after accounting for storm names and numbers changing.

Rewrite it to report the same facts, from the same source material, in genuinely different sentences and structure than yesterday's episode used. Do not add any new fact, number, date, place, name, or agency that isn't already in the source material below. Do not change what happened -- only how it's written. Keep the same voice: plain, specific, varied sentence length.

Return ONLY the rewritten stories section as plain prose. No preamble, no quotes, no markdown.

SOURCE MATERIAL:
${digest}

YESTERDAY'S EPISODE (do not reuse this wording):
${yesterdayScript.slice(0, 4000)}

TODAY'S CURRENT DRAFT (too similar to yesterday's, needs fresh wording):
${script}`
        }]
      })
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const rewritten = (data.content || []).filter((b) => b.type === "text").map((b) => b.text).join("").trim();
    if (!rewritten) {
      diagnostics.push({ step: "rewrite-freshness", ok: false, before, note: "rewrite returned nothing; keeping original" });
      return script;
    }
    const after = computeTextOverlap(rewritten, yesterdayScript);
    if (after >= before) {
      diagnostics.push({ step: "rewrite-freshness", ok: false, before, after, note: "rewrite did not reduce overlap; keeping original" });
      return script;
    }
    diagnostics.push({ step: "rewrite-freshness", ok: true, before, after, outputTokens: data.usage?.output_tokens ?? null });
    return toSpeakableAscii(rewritten);
  } catch (err) {
    diagnostics.push({ step: "rewrite-freshness", ok: false, before, error: String(err.message || err) });
    return script;
  }
}

// ADDED 2026-09-16, per the Informed Local Voice Guide's principle 4:
// "length is a ceiling, not a suggestion... never trust the model's
// self-reported length... if meaningfully over, do exactly ONE tightening
// pass, then ship regardless of the result. Never loop." Deliberately the
// opposite discipline from expandShortScript's up-to-3-attempt loop above --
// that asymmetry is intentional per the guide: a too-short episode is
// missing content and worth retrying to actually fix, but a too-long
// episode already has everything it needs, so one honest attempt at
// trimming is enough and looping just risks quietly eroding the content
// each pass. This closes a real, previously-unfixed gap: reflection length
// has been audited (reflection-too-long, cap 130) since the audit was
// built, but nothing ever acted on the flag -- which is exactly how a
// reflection went 109 -> 159 -> 259 words across three real episodes with
// the problem visible in diagnostics the entire time and nothing shipping
// a fix.
async function tightenText(env, text, targetWords, label, diagnostics) {
  const before = text.trim().split(/\s+/).filter(Boolean).length;
  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": env.ANTHROPIC_API_KEY, "anthropic-version": ANTHROPIC_VERSION },
      body: JSON.stringify({
        model: SCRIPT_MODEL,
        max_tokens: 1000,
        thinking: HELPER_THINKING,
        messages: [{
          role: "user",
          content: `The following ${label} for a spoken audio script is ${before} words, over its ${targetWords}-word ceiling. Cut it to fit at or under ${targetWords} words. Preserve every distinct fact and idea and the same voice -- tighten sentences, cut redundancy and elaboration that isn't load-bearing, do not remove entire ideas if avoidable, and never add anything new. Return ONLY the tightened text, no preamble, no quotes, no word count.\n\nTEXT:\n${text}`
        }]
      })
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const tightened = (data.content || []).filter((b) => b.type === "text").map((b) => b.text).join("").trim();
    const after = tightened ? tightened.trim().split(/\s+/).filter(Boolean).length : before;
    if (!tightened || after >= before) {
      diagnostics.push({ step: "tighten", ok: false, label, before, after, note: "tightening returned nothing shorter; keeping original per one-shot rule" });
      return text;
    }
    diagnostics.push({ step: "tighten", ok: true, label, before, after, outputTokens: data.usage?.output_tokens ?? null });
    return toSpeakableAscii(tightened);
  } catch (err) {
    diagnostics.push({ step: "tighten", ok: false, label, before, error: String(err.message || err), note: "keeping original per one-shot rule -- a slightly-long episode beats a failed publish" });
    return text;
  }
}

// ADDED 2026-09-25: mirrors the OUTPUT section of SYSTEM_PROMPT. Enforced
// server-side via output_config.format so the response always parses.
const SCRIPT_SCHEMA = {
  type: "object",
  properties: {
    script: { type: "string" },
    reflection: { type: "string" },
    imagePrompt: { type: "string" },
    organizingIdea: { type: "string" },
    claims: {
      type: "array",
      items: {
        type: "object",
        properties: { text: { type: "string" }, sourceId: { type: "string" } },
        required: ["text", "sourceId"],
        additionalProperties: false
      }
    }
  },
  required: ["script", "reflection", "imagePrompt", "organizingIdea", "claims"],
  additionalProperties: false
};

// ADDED 2026-09-25: belt-and-braces for the structured-output guarantee.
// The one failure actually observed (Sep 25) was raw newlines/tabs inside
// string values; escape control characters that sit inside a string and
// retry once before giving up.
function parseScriptJson(text) {
  try {
    return JSON.parse(text);
  } catch (first) {
    let out = "", inStr = false, esc = false;
    for (const ch of text) {
      if (inStr) {
        if (esc) { esc = false; out += ch; continue; }
        if (ch === "\\") { esc = true; out += ch; continue; }
        if (ch === '"') { inStr = false; out += ch; continue; }
        if (ch === "\n") { out += "\\n"; continue; }
        if (ch === "\r") { out += "\\r"; continue; }
        if (ch === "\t") { out += "\\t"; continue; }
        out += ch;
      } else {
        if (ch === '"') inStr = true;
        out += ch;
      }
    }
    try { return JSON.parse(out); } catch { throw first; }
  }
}

async function generateScript(env, digest, quote, diagnostics) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": env.ANTHROPIC_API_KEY, "anthropic-version": ANTHROPIC_VERSION },
    body: JSON.stringify({
      model: SCRIPT_MODEL,
      max_tokens: MAX_SCRIPT_TOKENS,
      thinking: { type: "adaptive" },
      // CHANGED 2026-09-25: added format. Sonnet 5 writes the script with
      // real paragraph breaks, and a raw newline inside a JSON string is
      // invalid JSON -- the Sep 25 episode failed JSON.parse, fell through to
      // the raw-text fallback, and read the entire JSON object (claims array
      // and all) aloud. Structured outputs guarantee a parseable response.
      output_config: { effort: "medium", format: { type: "json_schema", schema: SCRIPT_SCHEMA } },
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
  let script = "", reflection = "", imagePrompt = "", organizingIdea = "", claims = [];
  try {
    const parsed = parseScriptJson(text);
    script = parsed.script || "";
    reflection = parsed.reflection || "";
    imagePrompt = parsed.imagePrompt || "";
    organizingIdea = parsed.organizingIdea || "";
    claims = parsed.claims || [];
    if (!reflection) diagnostics.push({ step: "script", ok: true, note: "no reflection returned" });
    if (!imagePrompt) diagnostics.push({ step: "script", ok: true, note: "no imagePrompt returned -- cover art will fall back to static cover.jpg" });
    // ADDED 2026-09-01: non-fatal by design, same reasoning as the two notes
    // above -- a missing organizingIdea shouldn't block an otherwise-good
    // episode, it just means tomorrow's anti-repetition check has one less
    // data point to work with.
    if (!organizingIdea) diagnostics.push({ step: "script", ok: true, note: "no organizingIdea returned -- recent-frame tracking will have a gap for today" });
  } catch (err) {
    // CHANGED 2026-09-25: never hand JSON-shaped text to TTS. The old
    // fallback (script = text) is what put `{"script":"...` and the whole
    // claims array on air on Sep 25. A failed run surfaces via
    // podcast:last-error and alerting; a JSON dump published as an episode
    // surfaces only when someone listens to it.
    if (/^\s*\{/.test(text) || /"claims"\s*:/.test(text)) {
      throw new Error(`script response was unparseable JSON (stop_reason=${data.stop_reason}): ${String(err.message || err).slice(0, 120)}`);
    }
    diagnostics.push({ step: "script", ok: true, note: "response was not valid JSON; using raw text, claims unaudited, no image prompt" });
    script = text;
  }
  return {
    script: toSpeakableAscii(script),
    reflection: toSpeakableAscii(reflection),
    imagePrompt: toSpeakableAscii(imagePrompt),
    organizingIdea: toSpeakableAscii(organizingIdea),
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
    // CHANGED 2026-08-28: renamed from staleIds now that there are two sets
    // (earth and space) feeding buildSourceDigest's deprioritization below.
    const staleEarthIds = staleStorms.length ? new Set(staleStorms.map((s) => s.id)) : null;
    if (staleStorms.length) {
      diagnostics.push({
        step: "staleness", ok: false,
        // CHANGED 2026-08-28: the original note here assumed this meant a
        // source-data refresh failure. Confirmed via an authenticated
        // /health check on Aug 28 that this was wrong -- spaceIngest and
        // earthIngest were both current, no error anywhere. A storm holding
        // the same reported position for multiple days is usually a real,
        // correctly-reported observation (a stalling or dissipating system,
        // or nothing newer available within the 72h recency window), not a
        // broken pipeline. Don't re-diagnose this as an outage without
        // checking /health first.
        note: "one or more tracked storms report the exact same position as yesterday's episode -- usually a real unchanged observation, not a pipeline failure; now deprioritized (not excluded) in favor of fresher earth content when both exist",
        staleStorms
      });
    } else {
      diagnostics.push({ step: "staleness", ok: true, checked: Object.keys(todayStormFingerprint).length });
    }

    // ADDED 2026-08-28: space-side counterpart. Same detectStaleStorms()
    // function reused as-is -- see buildSpaceFingerprint's comment for why
    // that's safe (it never contained storm-specific logic).
    const todaySpaceFingerprint = buildSpaceFingerprint(space);
    const staleSpaceStories = detectStaleStorms(todaySpaceFingerprint, yesterday?.spaceFingerprint);
    const staleSpaceIds = staleSpaceStories.length ? new Set(staleSpaceStories.map((s) => s.id)) : null;
    if (staleSpaceStories.length) {
      diagnostics.push({
        step: "staleness-space", ok: false,
        note: "one or more space stories are byte-identical to yesterday's episode -- expected when a story stays within the 72h recency window with nothing fresher to replace it, not a pipeline failure; now deprioritized (not excluded) in favor of fresher space content when both exist",
        staleSpaceStories
      });
    } else {
      diagnostics.push({ step: "staleness-space", ok: true, checked: Object.keys(todaySpaceFingerprint).length });
    }

    const digest = buildSourceDigest(space, earth, yesterday, staleEarthIds, staleSpaceIds, ledger.recentOrganizingIdeas || []);
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

    // ADDED 2026-09-02: prompted by Sep1->Sep2 reusing 94% of a paragraph's
    // words verbatim. Runs BEFORE length expansion, deliberately -- fixing
    // originality first means expansion (if still needed after) works from
    // genuinely fresh text rather than padding out a copy.
    let textOverlap = yesterday?.script ? computeTextOverlap(gen.script, yesterday.script) : 0;
    if (yesterday?.script && textOverlap >= TEXT_OVERLAP_THRESHOLD) {
      diagnostics.push({ step: "freshness", ok: false, overlap: textOverlap, threshold: TEXT_OVERLAP_THRESHOLD });
      const rewritten = await rewriteForFreshness(env, gen.script, digest, yesterday.script, diagnostics);
      if (rewritten !== gen.script) {
        gen.script = rewritten;
        script = gen.reflection ? `${gen.script}\n\n${gen.reflection}` : gen.script;
        textOverlap = computeTextOverlap(gen.script, yesterday.script);
        const reAudit = auditClaims(gen.script, gen.claims, validIds, digest, gen.reflection, quote);
        diagnostics.push({ step: "audit", ok: reAudit.clean, claims: reAudit.claimCount, flags: reAudit.flags, note: "post-freshness-rewrite re-audit" });
        audit.flags = reAudit.flags;
        audit.clean = reAudit.clean;
      }
    } else if (yesterday?.script) {
      diagnostics.push({ step: "freshness", ok: true, overlap: textOverlap });
    }

    // ADDED 2026-09-16: one-shot tightening for the reflection specifically,
    // per the voice guide's length-ceiling principle. audit.flags already
    // contains reflection-too-long if the cap (130 words) was exceeded --
    // this is the first thing that ever actually acts on that flag instead
    // of just recording it.
    const reflectionTooLong = audit.flags.find((f) => f.type === "reflection-too-long");
    if (reflectionTooLong && gen.reflection) {
      const tightened = await tightenText(env, gen.reflection, 100, "reflection", diagnostics);
      if (tightened !== gen.reflection) {
        gen.reflection = tightened;
        script = `${gen.script}\n\n${gen.reflection}`;
        const reAudit = auditClaims(gen.script, gen.claims, validIds, digest, gen.reflection, quote);
        diagnostics.push({ step: "audit", ok: reAudit.clean, claims: reAudit.claimCount, flags: reAudit.flags, note: "post-tighten re-audit" });
        audit.flags = reAudit.flags;
        audit.clean = reAudit.clean;
      }
    }

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
      staleStorms: staleStorms.length ? staleStorms : null,
      // ADDED 2026-08-28: space-side counterpart to the two fields above.
      spaceFingerprint: todaySpaceFingerprint,
      staleSpaceStories: staleSpaceStories.length ? staleSpaceStories : null,
      // ADDED 2026-09-01: today's organizing idea, persisted for the same
      // retroactive-checkability reason as imageError/staleStorms -- lets
      // you confirm via a plain KV get whether the model actually returned
      // one and what it thought today's connecting idea was, without
      // needing a manual /trigger call.
      organizingIdea: gen.organizingIdea || null,
      // ADDED 2026-09-02: retroactively checkable, same pattern as
      // imageError/staleStorms. null when there was no yesterday to compare
      // against (day one, or a gap in episodes).
      textOverlapWithYesterday: yesterday?.script ? textOverlap : null
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
    // ADDED 2026-09-01: only recorded when the model actually returned one --
    // a missing organizingIdea (see generateScript's diagnostics note) just
    // means today leaves no trace in the anti-repetition check rather than
    // polluting it with an empty entry.
    if (gen.organizingIdea) {
      ledger.recentOrganizingIdeas = [{ day, idea: gen.organizingIdea }, ...(ledger.recentOrganizingIdeas || [])].slice(0, 8);
    }
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
