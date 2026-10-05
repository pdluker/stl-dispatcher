// ai-podcast.js
// Task 12 for stl-dispatcher -- "AI Daily Recap", the Mon/Thu ~5 minute
// episode in Paul's own (cloned) voice. ADDED 2026-10-04.
//
//   cron 50 11 * * MON,THU  (5 min after the :45 aiIngest, before :55 statusSync)
//     |- reads ai:day:* editions not yet covered by a previous episode
//     |    Mon episode -> Fri/Sat/Sun/Mon editions (news Thu..Sun)
//     |    Thu episode -> Tue/Wed/Thu editions     (news Mon..Wed)
//     |- 1x Claude call -> JSON beats (coldOpen, body, watch, title, blurb)
//     |- lint (numbers vs source, banned phrases, outlet cap, length)
//     |    -> at most ONE revise pass, never a loop (FIRE-INTEL rule)
//     |- code assembles: coldOpen + fixed ident + body + watch + closeout
//     |- budget gate against the LIVE ElevenLabs balance (see below)
//     |- toSpokenText() spells numbers out -> ElevenLabs TTS (ELEVENLABS_AI_VOICE_ID)
//     |- R2 put  pod-audio/ai/episodes/YYYY-MM-DD.mp3
//     '- KV put  ai:pod:episode:*, ai:pod:manifest, ai:pod:credits
//
// ── Style rewrite 2026-10-05 ───────────────────────────────────────────────
// Rebuilt from the two shows that already run in production:
//   Earth and Orbit (podcast-ingest.js): organizing idea before selection,
//     segues without connector crutches, a reaction on every story, hedges
//     are facts, "why it matters" notes are for the writer and are not read
//     aloud, outlet attribution capped, continuity with the last episode
//     used for callbacks but never as a source of facts, code-owned
//     ident and rotating closeouts.
//   FIRE-INTEL (fire-intel/workers/api): written for Paul's cloned voice;
//     have a take, but "opinionated, not snide"; reason out loud; banned
//     podcast cliches; airtime follows what CHANGED; every number must be
//     verbatim from the source (the three-different-Hormuz-numbers incident);
//     one tightening pass max, never loop.
// The difference from both: this one is PUBLIC. FIRE-INTEL is Paul talking to
// himself and E&O is pitched at one listener; this is Paul talking to people
// who chose to subscribe. So: a hook in the first 15 seconds, a consistent
// format a listener can learn, and a reason to come back Thursday.
//
// Numbers: eleven_flash_v2_5 does NOT normalize numbers on non-Enterprise
// plans (ElevenLabs docs, checked 2026-10-05), so "$40B" or "GPT-5.5" would be
// read badly. The script is written WITH digits (so the transcript reads well
// and every number can be audited exactly against the source text), and
// toSpokenText() converts them to words only for the TTS request.
//
// ── ElevenLabs budget: why this gate is different from podcast-ingest.js ──
// Earth and Orbit (Task 7) runs every day on the same ElevenLabs account and
// keeps its own ledger (podcast:credits, 92k budget). That ledger cannot see
// this task's spend. Daily E&O on Flash is ~2,700 credits/day (~84k/month);
// a ~720-word recap is ~4,300 chars = ~2,150 credits, x ~9/month = ~19k.
// 84k + 19k > the Creator plan's 100k. So this task gates on the account's
// REAL remaining balance (GET /v1/user/subscription) and always leaves enough
// for the daily show to finish the billing cycle. When credits are tight,
// the recap is what gets skipped, never the daily.
//
// Voice: ELEVENLABS_AI_VOICE_ID must be Paul's own voice clone. There is
// deliberately NO fallback to ELEVENLABS_VOICE_ID -- an episode billed as
// "in my voice" going out in the stock narrator's voice is worse than none.

import { callClaude, utcDayKey } from './ai-ingest.js';

const TTS_MODEL = 'eleven_flash_v2_5';
const CREDITS_PER_CHAR = 0.5;
const VOICE_SETTINGS = { stability: 0.5, similarity_boost: 0.8, speed: 1 };
const TARGET_WORDS = 720; // ~5:00 at a natural ~145 wpm, ident + closeout included
const MIN_WORDS = 600;
const MAX_WORDS = 840;
const MAX_EDITIONS = 5;
const MANIFEST_CAP = 300;
const MAX_NAMED_OUTLETS = 2;

// Reserve for Earth and Orbit until the next ElevenLabs reset.
const DAILY_POD_CREDITS_PER_DAY = 2800;
const SAFETY_CREDITS = 3000;
// Used only if the subscription endpoint can't be read (e.g. the API key
// lacks the user_read permission). Sized to ~9 episodes.
const FALLBACK_MONTHLY_BUDGET = 19000;

export const SHOW = {
  title: 'AI Daily Recap',
  host: 'Paul Luker',
};

// Code-owned so the show sounds the same every time (E&O's fixed welcome
// line; FIRE-INTEL's "consistent shape every day"). Rotated so the same
// closeout never runs twice in a row.
const CLOSEOUTS = [
  { id: 'recap-links', render: (next) => `That's the recap. Every story I mentioned, with links to the sources, is in the show notes. Talk to you ${next}.` },
  { id: 'show-back', render: (next) => `That's ${SHOW.title}. Sources for all of it are in the show notes. I'll be back ${next}.` },
  { id: 'thats-it', render: (next) => `That's it for this one. The links are in the show notes, and I'll see you ${next}.` },
];

// ── the prompt ─────────────────────────────────────────────────────────────
const SYSTEM_PROMPT = `You write "${SHOW.title}", a five-minute AI news recap that comes out every Monday and Thursday. It is read aloud in ${SHOW.host}'s own cloned voice. Paul works in IT at a large government contractor, uses these tools, and reads AI news obsessively so his listeners don't have to. Listeners are smart and busy: they use AI at work or follow it closely, and they subscribed because they want the few things that actually mattered since the last episode, from a person with a point of view, without the hype.

You are given several days of the AI Daily edition from ai.stluker.com (each with a date, lede, and stories with category, importance, summary, a "why it matters" note, and the outlets that covered it), plus the script of the previous episode.

WHAT YOU WRITE: five pieces, assembled by code in this order:
  coldOpen  ->  [fixed ident, supplied by code]  ->  body  ->  watch  ->  [fixed closeout, supplied by code]
The ident ("I'm Paul Luker, and this is ${SHOW.title} for <date>.") and the closeout are added for you. Do NOT write a greeting, the show name, the date, "welcome", a sign-off, or a request to subscribe anywhere.

1. coldOpen (25-50 words). The hook. Start inside the most interesting thing that happened, with a concrete detail, as if continuing a conversation. Not a summary of the episode, not "this week in AI", not a question. It must make someone keep listening past the first 15 seconds.

2. body (about 520-600 words). The heart of it.
   STEP ONE, before writing: find the organizing idea. Across these days, what is the actual story of the stretch? A shift, a tension, a pattern, a contradiction ("everyone shipped agents; nobody shipped a way to check their work"). Choose stories by that idea, not by importance score alone. If nothing honestly connects, don't force it: say so in organizingIdea and let the episode be a cleaner list.
   THE LEAD (about 40% of the body): the one story that matters most, done properly. What happened. What the easy read is. What it actually changes, and what it does not tell us yet. What Paul thinks. Reason out loud.
   TWO OR THREE MORE STORIES: real treatment, but uneven. One gets four sentences, another gets two. Vary the shape so it never sounds like a list being read.
   QUICK HITS: three to five smaller items, one or two sentences each, faster rhythm. Introduce the run with a plain half-sentence once ("A few quicker ones."), then go.
   Order by what connects, not by date or category. Mention the day when it helps the listener place it ("On Tuesday, ...").

3. watch (30-60 words). One specific thing to watch before the next episode, grounded in something in the material (a launch said to be coming, a vote, a rollout, an open question a story left hanging). Say why it matters in one line. No predictions beyond what the material says.

VOICE
Blend three sensibilities, braided, not imitated:
- The sharp explainer who understands the tech better than the people hyping it and will say plainly when a headline is bigger than the thing.
- The analyst who models thinking rather than concluding: here's what happened, here's why the obvious read is tempting, here's what I actually think. Comfortable saying "I'm not sure yet" when that's honest.
- The narrator who lands on the concrete, specific detail and never moralizes. Says the vivid true thing instead of the smooth generic one. Trusts facts to carry their own weight.
First person, contractions always, short sentences next to longer ones. Talking to listeners, not at them: "you" is fine sparingly; "folks", "guys", and "here's what you need to know" are not.

HAVE A TAKE. Every major story gets a reaction, not just a report: skeptical, impressed, worried, amused, unsure. Product launches and model releases get "sure, noted" energy unless something in them actually changes what people can do. Disagree with the headline's framing when the material supports it.
OPINIONATED, NOT SNIDE. Land a take once, in one line, then move on. Dunking for a second sentence is performing contempt, not having a take. A day with nothing overhyped in it should sound calm and direct, not manufactured.
NOT breathless, NOT a press release. No "the future is here", no "this changes everything", no awe. Also no flat recital of facts with no one visibly interested in them.

THE NOTES ARE FOR YOU. The "why it matters" line on each story is a note to the writer, not script copy. Never read it out as written or restate it after you've made the same point. Say the point once, in Paul's words, or skip it.

SEGUES. A segue is a sentence where the next story is already implied by how you ended the last one. If you need "meanwhile", "also", "in other news", "elsewhere", "speaking of", or "moving on", rewrite the boundary instead. If two stories share nothing, it is fine to just start the next one on its own terms, or to say plainly that it's unrelated.

CONTINUITY. If the previous episode's script is supplied and a story continues something it covered (or it said to watch something that now happened), call back to it in one plain line: "Last episode I said to watch for X. It shipped." Use the previous script ONLY to know what was said. Never take a fact from it, never re-explain background listeners already heard, never reuse its sentences. Airtime follows what changed: a continuing story with nothing new gets one clause or nothing.

THE FACT RULES (these are checked by code after you write)
- Use ONLY facts in the material. Never add a number, name, date, benchmark, price, quote, product, or claim that is not there, even if you are confident it is true. If the material is thin on a story, say less.
- Every number must appear in the material exactly as given. Write numbers as DIGITS exactly as the material has them ("$40 billion", "GPT-5.5", "3x", "70%"). Code spells them out for the voice later. Never round, convert, or compute a new number. If you want scale and lack the figure, say it in words ("a lot more", "most of them").
- Hedges are facts: "about", "nearly", "up to", "may", "plans to", "reportedly" stay in. "May" never becomes "will"; a test is not a launch; a preview is not general availability. Don't upgrade the stage.
- No invented quotes. Never present anyone's words in quotation marks unless the material quotes them.
- Attribution: name an outlet at most ${MAX_NAMED_OUTLETS} times in the whole episode, ideally once on the lead ("TechCrunch reported..."). Only outlets listed for that story. Never name the same outlet twice.
- Disasters, layoffs, harm to people: reported plainly. No silver linings, no jokes.

WRITE FOR THE EAR
- Plain ASCII only. Straight quotes. No em or en dashes: use commas or periods. Spaced hyphens read as hard stops, so avoid them.
- No parentheses, bullets, markdown, URLs, emoji, or stage directions.
- A blank line between beats (lead, each story, quick hits). The voice reads a blank line as a real pause.
- Don't stack numbers back to back; give each one a half-beat of context.
- Acronyms people say as words stay as-is (NASA, GPU, API). Spell nothing out letter by letter with periods.

BANNED (checked by code): "meanwhile", "in other news", "speaking of", "let's dive in", "dive into", "buckle up", "game-changer", "game changing", "revolutionary", "groundbreaking", "it remains to be seen", "time will tell", "at the end of the day", "here's the deal", "the AI landscape", "rapidly evolving", "ever-evolving", "folks", "stay tuned", "without further ado". No rhetorical question as the last line of any beat. The "It's not X. It's Y." construction at most once.

LENGTH: the whole episode (your pieces plus about 45 words of ident and closeout) lands near ${TARGET_WORDS} words, never under ${MIN_WORDS} or over ${MAX_WORDS}. Length comes from depth on the lead, never from padding or restating.

TITLE: max 70 characters, front-load the concrete subject people would search for, specific not clickbait ("OpenAI puts ads in ChatGPT, Google's bug bounty buckles"). No "Episode", no date, no show name.
BLURB: 1-2 plain sentences for show notes naming the main stories.

Return ONLY a JSON object:
{"organizingIdea":"<5-15 words, or 'no strong connection found'>","title":"...","blurb":"...","coldOpen":"...","body":"...","watch":"..."}`;

// ── helpers ────────────────────────────────────────────────────────────────
function wordCount(s) { return String(s || '').trim().split(/\s+/).filter(Boolean).length; }

function parseJsonLoose(text) {
  const s = String(text || '');
  const a = s.indexOf('{');
  const b = s.lastIndexOf('}');
  if (a < 0 || b <= a) throw new Error('no JSON object in model output');
  return JSON.parse(s.slice(a, b + 1));
}

// Display-safe ASCII (the stored transcript). Same reasoning as
// podcast-ingest.js toSpeakableAscii()/normalizeForTts(): unicode dashes and
// quotes get mispronounced and spaced hyphens read as hard stops.
export function toSpeakable(text) {
  return String(text)
    .replace(/[‘’]/g, "'")
    .replace(/[“”]/g, '"')
    .replace(/[–—]/g, ', ')
    .replace(/…/g, '...')
    .replace(/ +-{1,3} +/g, ', ')
    .replace(/[^\x20-\x7E\n]/g, '')
    .replace(/,\s*,/g, ',')
    .replace(/,\s*([.!?])/g, '$1')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

// ── numbers -> words, for the TTS request only ─────────────────────────────
const ONES = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
  'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen', 'sixteen', 'seventeen', 'eighteen', 'nineteen'];
const TENS = ['', '', 'twenty', 'thirty', 'forty', 'fifty', 'sixty', 'seventy', 'eighty', 'ninety'];
const SCALES = [[1e12, 'trillion'], [1e9, 'billion'], [1e6, 'million'], [1e3, 'thousand']];
const ORD = { one: 'first', two: 'second', three: 'third', five: 'fifth', eight: 'eighth', nine: 'ninth', twelve: 'twelfth' };
const MONTHS = 'January|February|March|April|May|June|July|August|September|October|November|December';
const SUFFIX = { k: 'thousand', K: 'thousand', m: 'million', M: 'million', b: 'billion', B: 'billion', bn: 'billion', T: 'trillion' };

function intWords(n) {
  if (n < 20) return ONES[n];
  if (n < 100) return TENS[Math.floor(n / 10)] + (n % 10 ? `-${ONES[n % 10]}` : '');
  if (n < 1000) return `${ONES[Math.floor(n / 100)]} hundred` + (n % 100 ? ` ${intWords(n % 100)}` : '');
  for (const [v, name] of SCALES) {
    if (n >= v) return `${intWords(Math.floor(n / v))} ${name}` + (n % v ? ` ${intWords(n % v)}` : '');
  }
  return String(n);
}

function ordinalWords(n) {
  const w = intWords(n);
  const parts = w.split(/([ -])/);
  const last = parts.pop();
  const ord = ORD[last] || (last.endsWith('y') ? `${last.slice(0, -1)}ieth` : `${last}th`);
  return parts.join('') + ord;
}

function numWords(str) {
  const [i, d] = String(str).replace(/,/g, '').split('.');
  const n = Number(i);
  if (!Number.isFinite(n) || n > 9e15) return str;
  let out = intWords(n);
  if (d) out += ` point ${d.split('').map((c) => ONES[Number(c)]).join(' ')}`;
  return out;
}

function yearWords(y) {
  if (y >= 2000 && y < 2010) return intWords(y);
  return `${intWords(Math.floor(y / 100))} ${y % 100 === 0 ? 'hundred' : y % 100 < 10 ? `oh ${ONES[y % 100]}` : intWords(y % 100)}`;
}

export function toSpokenText(text) {
  const NUM = '(\\d{1,3}(?:,\\d{3})+|\\d+)(?:\\.(\\d+))?';
  const num = (i, d) => numWords(d ? `${i}.${d}` : i);
  return String(text)
    // Month + day -> ordinal ("October 5" -> "October fifth")
    .replace(new RegExp(`\\b(${MONTHS}) (\\d{1,2})(?:st|nd|rd|th)?\\b`, 'g'), (_, m, d) => `${m} ${ordinalWords(Number(d))}`)
    // Currency with optional scale: $40 billion, $1.5B, $200M
    .replace(new RegExp(`([$£€])${NUM}\\s?(trillion|billion|million|thousand|bn|[kKmMbBT])?\\b`, 'g'), (_, cur, i, d, sc) => {
      const unit = cur === '$' ? 'dollars' : cur === '£' ? 'pounds' : 'euros';
      const scale = sc ? ` ${SUFFIX[sc] || sc}` : '';
      return `${num(i, d)}${scale} ${unit}`;
    })
    // Percent
    .replace(new RegExp(`${NUM}\\s?%`, 'g'), (_, i, d) => `${num(i, d)} percent`)
    // Multipliers: 3x, 10x
    .replace(new RegExp(`\\b${NUM}x\\b`, 'g'), (_, i, d) => `${num(i, d)} times`)
    // Ordinals: 1st, 22nd, 5th
    .replace(/\b(\d+)(?:st|nd|rd|th)\b/g, (_, n) => ordinalWords(Number(n)))
    // Years
    .replace(/\b(19\d{2}|20\d{2})s\b/g, (_, y) => `${yearWords(Number(y))}s`.replace(/ys$/, 'ies'))
    .replace(/\b(19\d{2}|20\d{2})\b/g, (_, y) => yearWords(Number(y)))
    // Bare suffixed scale: 70B params, 405B
    .replace(new RegExp(`\\b${NUM}(bn|[kKMBT])\\b`, 'g'), (_, i, d, sc) => `${num(i, d)} ${SUFFIX[sc]}`)
    // Version-style numbers glued to names: GPT-5.5, Llama-4 -> "GPT five point five"
    .replace(/([A-Za-z])-(\d)/g, '$1 $2')
    // Digits glued to letters: GPT-4o -> "four o", not "fouro"
    .replace(new RegExp(`${NUM}(?=[A-Za-z])`, 'g'), (_, i, d) => `${num(i, d)} `)
    // Everything else
    .replace(new RegExp(NUM, 'g'), (_, i, d) => num(i, d))
    .replace(/[ \t]{2,}/g, ' ');
}

function spokenDate(day) {
  return new Date(`${day}T12:00:00Z`).toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', timeZone: 'UTC' });
}

function nextEpisodeDay(day) {
  const wd = new Date(`${day}T12:00:00Z`).getUTCDay();
  return wd >= 1 && wd <= 3 ? 'Thursday' : 'Monday';
}

async function readJson(kv, key, fallback) {
  try { const raw = await kv.get(key); return raw ? JSON.parse(raw) : fallback; } catch { return fallback; }
}

// ── which editions does this episode cover? ────────────────────────────────
async function editionsToCover(env, today, diagnostics) {
  const index = await readJson(env.PODCAST_KV, 'ai:index', []);
  const manifest = await readJson(env.PODCAST_KV, 'ai:pod:manifest', []);
  // Exclude an episode already published today, so a forced re-run covers
  // the same window instead of an empty one.
  const prev = manifest.find((e) => e.id < today) || null;
  const after = prev?.coversThrough || null;
  let days = index.map((e) => e.day).filter((d) => d <= today && (!after || d > after));
  if (!after) days = days.slice(0, 4); // first episode ever: last ~4 editions
  days = days.sort().slice(-MAX_EDITIONS);
  diagnostics.push({ step: 'window', previousEpisode: prev?.id || null, coversAfter: after, days });
  const editions = [];
  for (const d of days) {
    const ed = await readJson(env.PODCAST_KV, `ai:day:${d}`, null);
    if (ed) editions.push(ed);
  }
  const prevEpisode = prev ? await readJson(env.PODCAST_KV, `ai:pod:episode:${prev.id}`, null) : null;
  return { editions, prevEpisode, prevCloseoutId: prev?.closeoutId || null };
}

function buildDigest(editions) {
  return editions.map((ed) => {
    const stories = ed.stories.map((s, i) => {
      const outlets = [...new Set((s.links || []).map((l) => l.source).filter(Boolean))].join(', ');
      return `  ${i + 1}. [${s.category}, importance ${s.importance}${outlets ? `, outlets: ${outlets}` : ''}] ${s.headline}\n     ${s.summary}${s.why ? `\n     (note to writer, why it matters: ${s.why})` : ''}`;
    }).join('\n');
    return `=== Edition ${ed.day} (${spokenDate(ed.day)}; covers the news of the day before) ===\nLede: ${ed.lede}\n${stories}`;
  }).join('\n\n');
}

// ── lint: what code can check after generation ─────────────────────────────
const BANNED = [
  'meanwhile', 'in other news', 'speaking of', "let's dive in", 'dive into', 'buckle up', 'game-changer',
  'game changer', 'game changing', 'game-changing', 'revolutionary', 'groundbreaking', 'it remains to be seen',
  'time will tell', 'at the end of the day', "here's the deal", 'the ai landscape', 'rapidly evolving',
  'ever-evolving', 'ever evolving', 'folks', 'stay tuned', 'without further ado', 'subscribe', 'welcome to',
];

function numberValues(text) {
  return (String(text).match(/\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?/g) || []).map((t) => Number(t.replace(/,/g, '')));
}

export function lintScript(parts, sourceText, outlets) {
  const issues = [];
  const writer = [parts.coldOpen, parts.body, parts.watch].join('\n\n');
  const lower = writer.toLowerCase();

  // 1. Every number the writer used must exist in the source material.
  const allowed = new Set(numberValues(sourceText));
  const bad = [...new Set(numberValues(writer).filter((n) => !allowed.has(n)))];
  if (bad.length) issues.push(`Numbers not found anywhere in the material: ${bad.join(', ')}. Remove them or use the exact figure the material gives.`);

  // 2. Banned phrases.
  const hits = BANNED.filter((p) => new RegExp(`(^|[^a-z])${p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}([^a-z]|$)`).test(lower));
  if (hits.length) issues.push(`Banned phrases used: ${hits.map((h) => `"${h}"`).join(', ')}. Rewrite those sentences without them.`);

  // 3. Outlet attribution cap and no repeats.
  const named = outlets.map((o) => ({ o, n: (writer.match(new RegExp(`\\b${o.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'g')) || []).length })).filter((x) => x.n);
  const total = named.reduce((a, x) => a + x.n, 0);
  if (total > MAX_NAMED_OUTLETS || named.some((x) => x.n > 1)) {
    issues.push(`Outlets named ${total} times (${named.map((x) => `${x.o} x${x.n}`).join(', ')}). Name at most ${MAX_NAMED_OUTLETS}, each once.`);
  }

  // 4. Beats must not end on a rhetorical question.
  for (const [k, v] of Object.entries({ coldOpen: parts.coldOpen, body: parts.body, watch: parts.watch })) {
    if (/\?\s*$/.test(String(v || '').trim())) issues.push(`The ${k} ends on a question. End it on a statement.`);
  }

  // 5. Negation crutch, at most once.
  const negations = (writer.match(/\b(it's|that's|this is) not [^.]{1,60}\.\s+(it's|that's|this is)\b/gi) || []).length;
  if (negations > 1) issues.push(`The "It's not X. It's Y." move is used ${negations} times. Keep at most one; state what things are directly.`);

  // 6. Length (whole episode is writer pieces + ~45 words of ident/closeout).
  const words = wordCount(writer) + 45;
  if (words < MIN_WORDS) issues.push(`Too short: about ${words} words with ident and closeout. Target ${TARGET_WORDS}. Add depth to the lead story from the material, not new facts.`);
  if (words > MAX_WORDS) issues.push(`Too long: about ${words} words with ident and closeout. Target ${TARGET_WORDS}. Cut repetition and the weakest quick hit, not facts in the lead.`);

  // 7. Shape.
  for (const k of ['coldOpen', 'body', 'watch', 'title']) if (!String(parts[k] || '').trim()) issues.push(`Missing "${k}".`);

  return { issues, words };
}

// ── budget ─────────────────────────────────────────────────────────────────
async function checkBudget(env, estCredits, diagnostics) {
  try {
    const res = await fetch('https://api.elevenlabs.io/v1/user/subscription', { headers: { 'xi-api-key': env.ELEVENLABS_API_KEY } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const sub = await res.json();
    const remaining = (sub.character_limit || 0) - (sub.character_count || 0);
    const resetMs = (sub.next_character_count_reset_unix || 0) * 1000;
    const daysToReset = Math.max(0, Math.ceil((resetMs - Date.now()) / 86400000));
    const reserve = daysToReset * DAILY_POD_CREDITS_PER_DAY + SAFETY_CREDITS;
    const ok = remaining - estCredits >= reserve;
    diagnostics.push({ step: 'budget', mode: 'live', ok, remaining, estCredits, reserveForDaily: reserve, daysToReset, tier: sub.tier || null });
    return ok;
  } catch (e) {
    const month = new Date().toISOString().slice(0, 7);
    const ledger = await readJson(env.PODCAST_KV, 'ai:pod:credits', null);
    const used = ledger && ledger.month === month ? ledger.creditsUsed : 0;
    const ok = used + estCredits <= FALLBACK_MONTHLY_BUDGET;
    diagnostics.push({ step: 'budget', mode: 'fallback-ledger', ok, error: String(e), used, estCredits, budget: FALLBACK_MONTHLY_BUDGET });
    return ok;
  }
}

async function addToLedger(env, credits) {
  const month = new Date().toISOString().slice(0, 7);
  const ledger = await readJson(env.PODCAST_KV, 'ai:pod:credits', null);
  const next = ledger && ledger.month === month
    ? { ...ledger, creditsUsed: ledger.creditsUsed + credits, episodes: ledger.episodes + 1 }
    : { month, creditsUsed: credits, episodes: 1 };
  await env.PODCAST_KV.put('ai:pod:credits', JSON.stringify(next));
  return next;
}

// ── script ─────────────────────────────────────────────────────────────────
function pickCloseout(today, prevId) {
  const pool = CLOSEOUTS.filter((c) => c.id !== prevId);
  const seed = Number(today.replace(/-/g, ''));
  return pool[seed % pool.length];
}

async function writeScript(env, today, editions, prevEpisode, prevCloseoutId, diagnostics) {
  const digest = buildDigest(editions);
  const outlets = [...new Set(editions.flatMap((e) => e.stories.flatMap((s) => (s.links || []).map((l) => l.source)).filter(Boolean)))];
  const prevBlock = prevEpisode?.script
    ? `PREVIOUS EPISODE (${spokenDate(prevEpisode.id)}), for callbacks only, never a source of facts:\n${prevEpisode.script}`
    : 'PREVIOUS EPISODE: none. This is the first episode, so no callbacks.';
  const user = `Episode date: ${spokenDate(today)} (${today}). Next episode: ${nextEpisodeDay(today)}.\n\nMATERIAL SINCE THE LAST EPISODE:\n\n${digest}\n\n${prevBlock}`;
  // Numbers are audited against the material plus the episode date line.
  const sourceText = `${digest}\n${spokenDate(today)} ${today}`;

  const r = await callClaude(env, { system: SYSTEM_PROMPT, user });
  let parts = parseJsonLoose(r.text);
  let lint = lintScript(parts, sourceText, outlets);
  diagnostics.push({ step: 'script', ok: true, words: lint.words, organizingIdea: parts.organizingIdea || null, issues: lint.issues, usage: r.usage, stopReason: r.stopReason });

  // One revise pass, never a loop. Keep whichever version has fewer issues.
  if (lint.issues.length) {
    try {
      const fix = await callClaude(env, {
        system: SYSTEM_PROMPT,
        user: `${user}\n\nYOUR DRAFT:\n${JSON.stringify(parts)}\n\nAn automated check found these problems:\n- ${lint.issues.join('\n- ')}\n\nFix exactly these problems and nothing else. Keep everything that works: the structure, the organizing idea, the takes, the voice. Return the same JSON shape.`,
      });
      const revised = parseJsonLoose(fix.text);
      const relint = lintScript(revised, sourceText, outlets);
      const better = relint.issues.length < lint.issues.length;
      diagnostics.push({ step: 'script-revise', ok: true, kept: better ? 'revised' : 'original', issuesAfter: relint.issues, words: relint.words, usage: fix.usage });
      if (better) { parts = revised; lint = relint; }
    } catch (e) {
      diagnostics.push({ step: 'script-revise', ok: false, error: String(e) });
    }
  }
  if (wordCount(parts.body) < 200) throw new Error(`script unusable (body ${wordCount(parts.body)} words)`);

  const closeout = pickCloseout(today, prevCloseoutId);
  const ident = `I'm ${SHOW.host}, and this is ${SHOW.title} for ${spokenDate(today)}.`;
  const script = toSpeakable([parts.coldOpen, ident, parts.body, parts.watch, closeout.render(nextEpisodeDay(today))].map((s) => String(s).trim()).join('\n\n'));
  return {
    title: String(parts.title || `${SHOW.title}: ${spokenDate(today)}`).slice(0, 100),
    blurb: String(parts.blurb || ''),
    organizingIdea: parts.organizingIdea || null,
    script,
    spoken: toSpokenText(script),
    words: wordCount(script),
    closeoutId: closeout.id,
    unresolvedIssues: lint.issues,
  };
}

async function synthesize(env, text, diagnostics) {
  const url = `https://api.elevenlabs.io/v1/text-to-speech/${env.ELEVENLABS_AI_VOICE_ID}?output_format=mp3_44100_128`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'xi-api-key': env.ELEVENLABS_API_KEY, 'content-type': 'application/json', accept: 'audio/mpeg' },
    body: JSON.stringify({ text, model_id: TTS_MODEL, voice_settings: VOICE_SETTINGS }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`ElevenLabs HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const buf = await res.arrayBuffer();
  diagnostics.push({ step: 'tts', ok: true, bytes: buf.byteLength, model: TTS_MODEL });
  if (buf.byteLength < 20000) throw new Error(`ElevenLabs returned only ${buf.byteLength} bytes -- treating as failure`);
  return buf;
}

// ── entry point ────────────────────────────────────────────────────────────
export async function runAiPodcast(env, { force = false, dryRun = false, now = new Date() } = {}) {
  const diagnostics = [];
  const today = utcDayKey(now);

  if (!force && (await env.PODCAST_KV.get(`ai:pod:episode:${today}`))) {
    return { ok: true, day: today, skipped: 'episode already exists for today', diagnostics };
  }

  const { editions, prevEpisode, prevCloseoutId } = await editionsToCover(env, today, diagnostics);
  if (editions.length < (force ? 1 : 2)) {
    return { ok: true, day: today, skipped: `only ${editions.length} new edition(s) since last episode`, diagnostics };
  }

  const ep = await writeScript(env, today, editions, prevEpisode, prevCloseoutId, diagnostics);
  const estCredits = Math.ceil(ep.spoken.length * CREDITS_PER_CHAR);
  if (dryRun) return { ok: true, day: today, dryRun: true, estCredits, episode: ep, diagnostics };

  if (!(await checkBudget(env, estCredits, diagnostics))) {
    return { ok: true, day: today, skipped: 'ElevenLabs budget: would cut into the daily show reserve', diagnostics };
  }

  const audio = await synthesize(env, ep.spoken, diagnostics);
  const audioKey = `ai/episodes/${today}.mp3`;
  await env.POD_BUCKET.put(audioKey, audio, { httpMetadata: { contentType: 'audio/mpeg' } });
  const ledger = await addToLedger(env, estCredits);

  const manifest = await readJson(env.PODCAST_KV, 'ai:pod:manifest', []);
  const prior = manifest.filter((e) => e.id !== today);
  const episode = {
    id: today,
    episodeNumber: (prior[0]?.episodeNumber || prior.length) + 1,
    title: ep.title,
    blurb: ep.blurb,
    script: ep.script,
    words: ep.words,
    organizingIdea: ep.organizingIdea,
    closeoutId: ep.closeoutId,
    unresolvedIssues: ep.unresolvedIssues,
    covers: editions.map((e) => e.day),
    coversThrough: editions[editions.length - 1].day,
    pubDate: now.toUTCString(),
    audioKey,
    bytes: audio.byteLength,
    durationSeconds: Math.round((audio.byteLength * 8) / 128000),
    credits: estCredits,
  };
  await env.PODCAST_KV.put(`ai:pod:episode:${today}`, JSON.stringify(episode));
  const { script: _omit, unresolvedIssues: _u, ...manifestEntry } = episode;
  await env.PODCAST_KV.put('ai:pod:manifest', JSON.stringify([manifestEntry, ...prior].slice(0, MANIFEST_CAP)));

  return { ok: true, day: today, episode: manifestEntry, ledger, diagnostics };
}
