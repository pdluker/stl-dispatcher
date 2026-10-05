// ai-podcast.js
// Task 12 for stl-dispatcher -- "AI Daily Recap", the Mon/Thu ~5 minute
// episode in Paul's own (cloned) voice. ADDED 2026-10-04.
//
//   cron 50 11 * * MON,THU  (5 min after the :45 aiIngest, before :55 statusSync)
//     |- reads ai:day:* editions not yet covered by a previous episode
//     |    Mon episode -> Fri/Sat/Sun/Mon editions (news Thu..Sun)
//     |    Thu episode -> Tue/Wed/Thu editions     (news Mon..Wed)
//     |- 1x Claude call -> ~720 word first-person script (+1 length fix if needed)
//     |- budget gate against the LIVE ElevenLabs balance (see below)
//     |- 1x ElevenLabs TTS with ELEVENLABS_AI_VOICE_ID -> mp3
//     |- R2 put  pod-audio/ai/episodes/YYYY-MM-DD.mp3
//     '- KV put  ai:pod:episode:*, ai:pod:manifest, ai:pod:credits
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
const TARGET_WORDS = 720; // ~5:00 at a natural ~145 wpm
const MIN_WORDS = 600;
const MAX_WORDS = 840;
const MAX_EDITIONS = 5;
const MANIFEST_CAP = 300;

// Reserve for Earth and Orbit until the next ElevenLabs reset.
const DAILY_POD_CREDITS_PER_DAY = 2800;
const SAFETY_CREDITS = 3000;
// Used only if the subscription endpoint can't be read (e.g. the API key
// lacks the user_read permission). Sized to ~9 episodes.
const FALLBACK_MONTHLY_BUDGET = 19000;

export const SHOW = {
  title: 'AI Daily Recap',
  host: 'Paul',
};

const SYSTEM_PROMPT = `You write the script for "${SHOW.title}", a twice-weekly, five-minute spoken recap of AI news. It is read aloud, in the first person, by the host, ${SHOW.host}, in his own voice. You receive several days of a daily AI news page (lede + stories, each with a date) and you turn them into one tight episode that outlines what happened since the last episode.

Voice: a knowledgeable friend catching you up on the drive in. Conversational, direct, a little dry. First person singular ("I", "what caught my eye"). Never "we at", never a sponsor read, never "smash that subscribe button".

Structure:
1. Open: "Hey, it's ${SHOW.host}, and this is ${SHOW.title} for <spoken weekday, month day>." Then one or two sentences on the big picture of these days.
2. The three or four stories that matter most, each in depth: what happened, then why it matters. Say which day it happened when it helps ("On Tuesday, ...").
3. A quick-hits run of three to five smaller items, one or two sentences each.
4. Close in one or two sentences with something specific to watch for next, drawn from the material. Then: "That's the recap. Talk to you next time."

Hard rules:
- Use ONLY facts in the provided material. Never add a number, name, date, benchmark, price, quote, or claim that is not there. If the material is thin on a story, say less about it.
- Do not present a story's "why it matters" line as a quote from anyone.
- Length: about ${TARGET_WORDS} words of script, never under ${MIN_WORDS} or over ${MAX_WORDS}. Count carefully.
- Write for the ear: short sentences, no parentheses, no bullet points, no markdown, no URLs, no emoji. Spell out symbols ("percent", "dollars"). Model names as they are said aloud.
- Plain ASCII punctuation only.

Return ONLY a JSON object:
{
  "title": "Episode title, max 70 chars, naming the top story",
  "blurb": "One or two sentence show-notes summary",
  "script": "The full spoken script"
}`;

// ── helpers ────────────────────────────────────────────────────────────────
function wordCount(s) { return String(s || '').trim().split(/\s+/).filter(Boolean).length; }

function parseJsonLoose(text) {
  const s = String(text || '');
  const a = s.indexOf('{');
  const b = s.lastIndexOf('}');
  if (a < 0 || b <= a) throw new Error('no JSON object in model output');
  return JSON.parse(s.slice(a, b + 1));
}

// Same reasoning as podcast-ingest.js toSpeakableAscii()/normalizeForTts():
// unicode dashes/quotes get mispronounced and spaced hyphens read as hard stops.
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
    .replace(/ {2,}/g, ' ')
    .trim();
}

function spokenDate(day) {
  return new Date(`${day}T12:00:00Z`).toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', timeZone: 'UTC' });
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
  const prev = manifest.find((e) => e.id < today);
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
  return editions;
}

function buildDigest(editions) {
  return editions.map((ed) => {
    const stories = ed.stories.map((s, i) =>
      `  ${i + 1}. [${s.category}, importance ${s.importance}] ${s.headline}\n     ${s.summary}${s.why ? `\n     Why it matters: ${s.why}` : ''}`).join('\n');
    return `=== Edition ${ed.day} (${spokenDate(ed.day)}; covers the news of the day before) ===\nLede: ${ed.lede}\n${stories}`;
  }).join('\n\n');
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
async function writeScript(env, today, digest, diagnostics) {
  const user = `Episode date: ${spokenDate(today)} (${today}).\n\nMaterial since the last episode:\n\n${digest}`;
  const r = await callClaude(env, { system: SYSTEM_PROMPT, user });
  let ep = parseJsonLoose(r.text);
  let words = wordCount(ep.script);
  diagnostics.push({ step: 'script', ok: true, words, usage: r.usage, stopReason: r.stopReason });

  if (words < MIN_WORDS || words > MAX_WORDS) {
    const dir = words < MIN_WORDS ? 'expand' : 'tighten';
    const fix = await callClaude(env, {
      system: SYSTEM_PROMPT,
      user: `${user}\n\nHere is a draft script that is ${words} words. ${dir === 'expand'
        ? `Expand it to about ${TARGET_WORDS} words by developing the stories already in it with detail FROM THE MATERIAL ONLY. No new facts.`
        : `Tighten it to about ${TARGET_WORDS} words. Keep the structure, opening, and closing lines.`} Return the same JSON shape.\n\nDraft:\n${ep.script}`,
    });
    try {
      const fixed = parseJsonLoose(fix.text);
      const fw = wordCount(fixed.script);
      diagnostics.push({ step: `script-${dir}`, ok: true, wordsBefore: words, wordsAfter: fw, usage: fix.usage });
      if (Math.abs(fw - TARGET_WORDS) < Math.abs(words - TARGET_WORDS)) { ep = { ...ep, ...fixed }; words = fw; }
    } catch (e) {
      diagnostics.push({ step: `script-${dir}`, ok: false, error: String(e) });
    }
  }
  if (!ep.script || words < 300) throw new Error(`script unusable (${words} words)`);
  return {
    title: String(ep.title || `${SHOW.title}: ${spokenDate(today)}`).slice(0, 100),
    blurb: String(ep.blurb || ''),
    script: toSpeakable(ep.script),
    words,
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

  const editions = await editionsToCover(env, today, diagnostics);
  if (editions.length < (force ? 1 : 2)) {
    return { ok: true, day: today, skipped: `only ${editions.length} new edition(s) since last episode`, diagnostics };
  }

  const script = await writeScript(env, today, buildDigest(editions), diagnostics);
  const estCredits = Math.ceil(script.script.length * CREDITS_PER_CHAR);
  if (dryRun) return { ok: true, day: today, dryRun: true, estCredits, episode: script, diagnostics };

  if (!(await checkBudget(env, estCredits, diagnostics))) {
    return { ok: true, day: today, skipped: 'ElevenLabs budget: would cut into the daily show reserve', diagnostics };
  }

  const audio = await synthesize(env, script.script, diagnostics);
  const audioKey = `ai/episodes/${today}.mp3`;
  await env.POD_BUCKET.put(audioKey, audio, { httpMetadata: { contentType: 'audio/mpeg' } });
  const ledger = await addToLedger(env, estCredits);

  const manifest = await readJson(env.PODCAST_KV, 'ai:pod:manifest', []);
  const prior = manifest.filter((e) => e.id !== today);
  const episode = {
    id: today,
    episodeNumber: (prior[0]?.episodeNumber || prior.length) + 1,
    title: script.title,
    blurb: script.blurb,
    script: script.script,
    words: script.words,
    covers: editions.map((e) => e.day),
    coversThrough: editions[editions.length - 1].day,
    pubDate: now.toUTCString(),
    audioKey,
    bytes: audio.byteLength,
    durationSeconds: Math.round((audio.byteLength * 8) / 128000),
    credits: estCredits,
  };
  await env.PODCAST_KV.put(`ai:pod:episode:${today}`, JSON.stringify(episode));
  const { script: _omit, ...manifestEntry } = episode;
  await env.PODCAST_KV.put('ai:pod:manifest', JSON.stringify([manifestEntry, ...prior].slice(0, MANIFEST_CAP)));

  return { ok: true, day: today, episode: manifestEntry, ledger, diagnostics };
}
