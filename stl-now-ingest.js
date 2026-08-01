/**
 * stl-now-ingest.js  --  lives in C:\Users\pdluk\stl-dispatcher\
 *
 * Daily "St. Louis Now" 0500 CST show: reads a pre-batched history entry from
 * STLNOW_KV (no API call needed for that part), reads the calendar segment an
 * EARLIER dispatcher task already staged today (see stl-now-calendar.js),
 * optionally reads a food segment on scheduled food days, writes one script
 * via a single Haiku call, renders it with ElevenLabs, stores the MP3 in R2,
 * and appends an episode entry to STLNOW_KV -- same shape as podcast-ingest.js.
 *
 * Corrected from the first draft of this file after reading the REAL
 * podcast-ingest.js / pod-worker.js / DISPATCHER-SNIPPET.md. The first draft
 * put ingest logic inside the content Worker behind a /refresh POST route.
 * That is not the actual pattern -- the Worker only ever reads KV and serves
 * static output. All the actual work (script gen, TTS, R2 write) happens here,
 * inside stl-dispatcher, invoked as a plain function call, not an HTTP round
 * trip. Fixing that now rather than carrying the wrong shape forward.
 *
 * Design constraints carried over from podcast-ingest.js on purpose (each
 * traces to a real past incident -- do not remove without reading the note
 * in podcast-ingest.js or stluker-infrastructure.md):
 *
 *  - ZERO new external news fetches from THIS file. History is pre-batched
 *    KV data; calendar/food are read from what an earlier dispatcher task
 *    already staged today. This file is a consumer, not a 4th ingestion path.
 *  - ONE Anthropic call, max_tokens capped, NO web_search tool.
 *  - Hard monthly ElevenLabs credit budget enforced BEFORE the TTS call.
 *  - Idempotent per UTC day.
 *  - ASCII-only sanitizer on everything before it leaves this file.
 *
 * One rule that is NEW to this file, not inherited from podcast-ingest.js:
 *  - THE FACT/DRAMATIZATION SEAM IS STRUCTURAL, NOT STYLISTIC. The history
 *    segment must keep "the record shows" and "dramatized, not verbatim
 *    record" in clearly separated blocks. This is the content-integrity
 *    rule from the strategy doc, and it is audited below the same way
 *    podcast-ingest.js audits the reflection's no-new-facts rule -- because
 *    a prompt instruction alone is a request, not a guarantee.
 */

const SCRIPT_MODEL = 'claude-haiku-4-5-20251001';
const ANTHROPIC_VERSION = '2023-06-01';

// ---- Cost controls. Same shape as podcast-ingest.js; tune here, nowhere else.
const TARGET_WORDS = 700;              // ~4-5 min at a 150 wpm read -- shorter than
                                        // Orbit and Ground; three light segments, not
                                        // deep science stories.
const MAX_SCRIPT_TOKENS = 1200;
const MAX_SCRIPT_CHARS = 5500;
const MONTHLY_CREDIT_BUDGET = 92000;   // separate ledger key from podcast:credits --
                                        // confirm actual remaining headroom on the
                                        // SAME ElevenLabs account before assuming this
                                        // number is available on top of Orbit+Ground's
                                        // existing budget, not in addition to some
                                        // already-spoken-for total.
const TTS_MODEL = 'eleven_flash_v2_5';
const CREDITS_PER_CHAR = 0.5;
const VOICE_SETTINGS = { stability: 0.45, similarity_boost: 0.75, speed: 1.0 };

const SYSTEM_PROMPT = `You write a short daily morning show for St. Louis, airing at 0500 CST, listened to on someone's morning commute. Warm, curious, plainspoken. Three segments, always in this order and ALWAYS clearly separated -- never blend them into one continuous narrative.

SEGMENT 1 -- TODAY IN STL HISTORY
You will be given either a history entry or nothing.
If given an entry, output exactly two labeled parts, in this order, and do not merge them:
  "The record shows:" followed by the verified fact, restated plainly in your own words -- do not embellish, add unstated numbers, or invent details not present in the supplied fact.
  "Dramatized -- not verbatim record:" followed by a short scene-setting telling, clearly imaginative in tone, built from the supplied dramatization text or your own short elaboration of it. This part may NOT introduce new load-bearing facts (dates, names, numbers) beyond what "The record shows" already stated.
If given nothing, say plainly that there's no confirmed history entry for today and move on. Do not invent one.

SEGMENT 2 -- AROUND TOWN TODAY
A short rundown of the supplied events, in plain language. If the event list is empty, say there's nothing confirmed today rather than filling space or inventing an event.

SEGMENT 3 -- FOOD DESK
Only include this segment if food content is supplied. State its verification status explicitly using the exact word given (Confirmed, Reported, or Developing). If no food content is supplied, omit this segment entirely -- do not pad, do not say "nothing new today," just skip straight from segment 2 to the sign-off.

VOICE
Plain and specific. Comfortable, unhurried morning-radio warmth without being chirpy. Never "good morning" as an opener, never "here's what we've got" -- just start.

READ-ALOUD FORMAT
Plain prose. No markdown, headers, bullets, emoji, citations, or URLs. ASCII punctuation only: straight quotes, hyphens, no em dashes.

LENGTH
About ${TARGET_WORDS} words total. Under 850.

OUTPUT
Return ONLY a JSON object, no fences:
{"script":"<the full three-segment script>","historyFactUsed":"<the exact fact text you were given, or null if none>","claims":[{"text":"<any load-bearing fact stated in segments 2 or 3>","sourceId":"<exact id it came from>"}]}
Every load-bearing fact in segments 2/3 gets a claims entry. If you cannot name the id, the fact does not belong in the script.`;

/* ------------------------------------------------------------------ helpers */

/** Identical to podcast-ingest.js's sanitizer -- same TTS/PowerShell hazards apply. */
function toSpeakableAscii(text) {
  if (!text) return '';
  return String(text)
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
    .replace(/[^\x20-\x7E\n]/g, '')
    .replace(/\[\d+\]/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function utcDayKey(now = new Date()) {
  return now.toISOString().slice(0, 10);
}
function utcMonthKey(now = new Date()) {
  return now.toISOString().slice(0, 7);
}
function mmddKey(now = new Date()) {
  return now.toISOString().slice(5, 10); // "MM-DD" -- matches history-backlog keys
}

/** Read today's pre-batched history entry. Never throws; missing is a valid state. */
async function readHistoryEntry(kv, dateKey) {
  if (!kv) return { ok: false, entry: null, error: 'KV binding not present' };
  try {
    const raw = await kv.get(`history:${dateKey}`);
    if (!raw) return { ok: true, entry: null }; // legitimately no entry yet -- not an error
    return { ok: true, entry: JSON.parse(raw) };
  } catch (err) {
    return { ok: false, entry: null, error: `parse failed: ${err.message}` };
  }
}

/** Read what the earlier evening calendar task staged. Degrades to empty, never throws. */
async function readCalendarSegment(kv) {
  if (!kv) return { ok: false, events: [], error: 'KV binding not present' };
  try {
    const raw = await kv.get('calendar:pending');
    if (!raw) return { ok: true, events: [] };
    const data = JSON.parse(raw);
    return { ok: true, events: Array.isArray(data) ? data : (data.events || []) };
  } catch (err) {
    return { ok: false, events: [], error: `parse failed: ${err.message}` };
  }
}

/** Food is scheduled 1-2x/week per the strategy doc, not daily. Placeholder cadence. */
function isFoodDay(date) {
  const day = date.getUTCDay();
  return day === 2 || day === 5; // Tue / Fri -- adjust once the real schedule is set
}

async function readFoodSegment(kv) {
  if (!kv) return { ok: false, item: null, error: 'KV binding not present' };
  try {
    const raw = await kv.get('food:pending');
    if (!raw) return { ok: true, item: null };
    return { ok: true, item: JSON.parse(raw) };
  } catch (err) {
    return { ok: false, item: null, error: `parse failed: ${err.message}` };
  }
}

function buildDigest(historyEntry, events, foodItem) {
  const lines = [];
  lines.push('HISTORY ENTRY FOR TODAY:');
  if (historyEntry) {
    lines.push(`fact: ${toSpeakableAscii(historyEntry.fact)}`);
    lines.push(`dramatization seed: ${toSpeakableAscii(historyEntry.dramatization || '')}`);
    lines.push(`verified: ${historyEntry.verified === true}`); // pass through honestly --
    // do NOT let the model or this pipeline round an unverified entry up to verified.
  } else {
    lines.push('(none available for this date)');
  }
  lines.push('');
  lines.push('EVENTS TODAY (each pre-confirmed against its own organizer page by the earlier calendar task):');
  if (events.length) {
    events.forEach((e, i) => lines.push(`- id=event-${i} ${toSpeakableAscii(e.name)}: ${toSpeakableAscii(e.detail || '')}`));
  } else {
    lines.push('(none confirmed today)');
  }
  lines.push('');
  lines.push('FOOD ITEM:');
  if (foodItem) {
    lines.push(`- id=food-1 ${toSpeakableAscii(foodItem.headline)}: ${toSpeakableAscii(foodItem.detail || '')} [status: ${foodItem.status || 'Reported'}]`);
  } else {
    lines.push('(none scheduled today)');
  }
  return lines.join('\n');
}

/* ----------------------------------------------------------- claims + seam audit */

/**
 * Structural check, same spirit as podcast-ingest.js's auditClaims: the
 * prompt states the policy, this measures compliance rather than trusting it.
 * Flags do not block publishing -- a flagged episode with a logged warning
 * is more useful than a silent skip, and surfaces on /transcript for spot-checking.
 */
function auditScript(script, claims, validIds, historyEntry) {
  const flags = [];
  const cited = Array.isArray(claims) ? claims : [];
  for (const c of cited) {
    if (!c || !c.sourceId) flags.push({ type: 'uncited-claim', detail: String(c?.text || '').slice(0, 80) });
    else if (!validIds.has(c.sourceId)) flags.push({ type: 'invented-source-id', id: c.sourceId });
  }

  // The seam check: if a history entry was supplied, both required labels
  // must appear, in order, and not collapsed into each other.
  if (historyEntry) {
    const factIdx = script.indexOf('The record shows');
    const dramIdx = script.indexOf('Dramatized');
    if (factIdx === -1) flags.push({ type: 'missing-fact-label' });
    if (dramIdx === -1) flags.push({ type: 'missing-dramatization-label' });
    if (factIdx !== -1 && dramIdx !== -1 && dramIdx < factIdx) {
      flags.push({ type: 'seam-order-reversed' });
    }
  }

  // If NO history entry was supplied, the script must not fabricate one --
  // a crude but cheap check: it shouldn't contain "The record shows" at all.
  if (!historyEntry && script.includes('The record shows')) {
    flags.push({ type: 'history-fabricated-with-no-source-entry' });
  }

  return { flags, claimCount: cited.length, clean: flags.length === 0 };
}

/* ------------------------------------------------------- monthly cost gate */
// Identical shape to podcast-ingest.js's ledger, separate KV key so the two
// shows' budgets are tracked (and can be reasoned about) independently.

async function readCreditLedger(kv) {
  const month = utcMonthKey();
  try {
    const raw = await kv.get('stlnow:credits');
    const led = raw ? JSON.parse(raw) : null;
    if (led && led.month === month) return led;
    return { month, creditsUsed: 0, episodes: 0 };
  } catch {
    return { month, creditsUsed: 0, episodes: 0 };
  }
}
async function writeCreditLedger(kv, ledger) {
  await kv.put('stlnow:credits', JSON.stringify(ledger));
}

/* ----------------------------------------------------------- external calls */

async function generateScript(env, digest, diagnostics) {
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
      messages: [{ role: 'user', content: `Here is today's material. Write the show.\n\n${digest}` }],
      // No tools block. No web_search. Same rule as podcast-ingest.js.
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Anthropic HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json();
  diagnostics.push({
    step: 'script', ok: true,
    inputTokens: data.usage?.input_tokens ?? null,
    outputTokens: data.usage?.output_tokens ?? null,
    stopReason: data.stop_reason,
  });

  let text = (data.content || []).filter((b) => b.type === 'text').map((b) => b.text).join('\n')
    .replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();

  let script = '', claims = [];
  try {
    const parsed = JSON.parse(text);
    script = parsed.script || '';
    claims = parsed.claims || [];
  } catch {
    diagnostics.push({ step: 'script', ok: true, note: 'response was not valid JSON; using raw text, claims unaudited' });
    script = text;
  }
  return { script: toSpeakableAscii(script), claims };
}

async function synthesizeSpeech(env, script, diagnostics) {
  const voiceId = env.STLNOW_VOICE_ID || env.ELEVENLABS_VOICE_ID; // separate voice optional;
                                                                    // falls back to the shared one
  const url = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}?output_format=mp3_44100_128`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'xi-api-key': env.ELEVENLABS_API_KEY, 'content-type': 'application/json', accept: 'audio/mpeg' },
    body: JSON.stringify({ text: script, model_id: TTS_MODEL, voice_settings: VOICE_SETTINGS }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`ElevenLabs HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const buf = await res.arrayBuffer();
  diagnostics.push({ step: 'tts', ok: true, bytes: buf.byteLength, model: TTS_MODEL });
  if (buf.byteLength < 15000) {
    throw new Error(`ElevenLabs returned only ${buf.byteLength} bytes -- treating as failure`);
  }
  return buf;
}

/* --------------------------------------------------------------- main entry */

/**
 * @param {object} env  dispatcher env: STLNOW_KV, STLNOW_BUCKET (R2),
 *                      ANTHROPIC_API_KEY, ELEVENLABS_API_KEY,
 *                      ELEVENLABS_VOICE_ID (or STLNOW_VOICE_ID)
 * @param {object} opts { force?: boolean }
 */
export async function runStlNowIngest(env, opts = {}) {
  const diagnostics = [];
  const day = utcDayKey();
  const dateKey = mmddKey();
  const result = { ran: false, ok: false, day, diagnostics };

  try {
    for (const k of ['ANTHROPIC_API_KEY', 'ELEVENLABS_API_KEY']) {
      if (!env[k]) throw new Error(`missing secret: ${k}`);
    }
    if (!env.STLNOW_KV) throw new Error('missing binding: STLNOW_KV');
    if (!env.STLNOW_BUCKET) throw new Error('missing binding: STLNOW_BUCKET (R2)');

    // --- 1. Idempotency.
    const existing = await env.STLNOW_KV.get(`stlnow:episode:${day}`);
    if (existing && !opts.force) {
      diagnostics.push({ step: 'idempotency', ok: true, note: 'episode already exists for this UTC day, skipping' });
      result.ran = false; result.ok = true; result.skipped = 'already-generated';
      return result;
    }

    // --- 2. Read today's material. Zero external fetches here.
    const hist = await readHistoryEntry(env.STLNOW_KV, dateKey);
    const cal = await readCalendarSegment(env.STLNOW_KV);
    const food = isFoodDay(new Date()) ? await readFoodSegment(env.STLNOW_KV) : { ok: true, item: null };
    diagnostics.push({
      step: 'sources',
      history: { ok: hist.ok, found: !!hist.entry, verified: hist.entry?.verified ?? null, error: hist.error || null },
      calendar: { ok: cal.ok, count: cal.events.length, error: cal.error || null },
      food: { ok: food.ok, present: !!food.item, error: food.error || null },
    });

    // --- 3. Log a backlog gap rather than silently shipping an empty segment.
    if (!hist.entry) {
      const gaps = (await env.STLNOW_KV.get('stlnow:history-gaps', { type: 'json' })) || [];
      if (!gaps.includes(dateKey)) gaps.push(dateKey);
      await env.STLNOW_KV.put('stlnow:history-gaps', JSON.stringify(gaps));
      diagnostics.push({ step: 'gap', ok: true, note: `no history entry for ${dateKey}, logged to stlnow:history-gaps` });
    }

    // --- 4. Budget gate, BEFORE the TTS call.
    const ledger = await readCreditLedger(env.STLNOW_KV);
    const estChars = Math.min(TARGET_WORDS * 6.2, MAX_SCRIPT_CHARS);
    const estCredits = Math.ceil(estChars * CREDITS_PER_CHAR);
    if (ledger.creditsUsed + estCredits > MONTHLY_CREDIT_BUDGET) {
      diagnostics.push({ step: 'budget', ok: false, note: `monthly budget would be exceeded; skipping` });
      result.ok = true; result.skipped = 'monthly-credit-budget'; result.ledger = ledger;
      return result;
    }

    // --- 5. Script.
    result.ran = true;
    const digest = buildDigest(hist.entry, cal.events, food.item);
    const gen = await generateScript(env, digest, diagnostics);

    const validIds = new Set([
      ...cal.events.map((_, i) => `event-${i}`),
      ...(food.item ? ['food-1'] : []),
    ]);
    const audit = auditScript(gen.script, gen.claims, validIds, hist.entry);
    diagnostics.push({ step: 'audit', ok: audit.clean, claims: audit.claimCount, flags: audit.flags });

    let script = gen.script;
    if (!script || script.length < 400) {
      throw new Error(`script too short (${script?.length || 0} chars) -- refusing to spend TTS credits`);
    }
    if (script.length > MAX_SCRIPT_CHARS) {
      const cut = script.slice(0, MAX_SCRIPT_CHARS);
      script = cut.slice(0, cut.lastIndexOf('.') + 1) || cut;
      diagnostics.push({ step: 'script', ok: true, note: 'truncated to char ceiling' });
    }

    const charCount = script.length;
    const credits = Math.ceil(charCount * CREDITS_PER_CHAR);
    const words = script.split(/\s+/).length;

    if (ledger.creditsUsed + credits > MONTHLY_CREDIT_BUDGET) {
      diagnostics.push({ step: 'budget', ok: false, note: 'actual script exceeded remaining budget; skipping TTS' });
      result.ok = true; result.skipped = 'monthly-credit-budget-actual';
      return result;
    }

    // --- 6. Audio.
    const audio = await synthesizeSpeech(env, script, diagnostics);
    const objectKey = `episodes/${day}.mp3`;
    await env.STLNOW_BUCKET.put(objectKey, audio, {
      httpMetadata: { contentType: 'audio/mpeg', cacheControl: 'public, max-age=31536000, immutable' },
    });
    diagnostics.push({ step: 'r2', ok: true, key: objectKey, bytes: audio.byteLength });

    // --- 7. Episode record + manifest -- same shape as podcast:manifest.
    const estSeconds = Math.round(words / 2.5);
    const episode = {
      id: day,
      title: `St. Louis Now -- ${new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC' })}`,
      pubDate: new Date().toUTCString(),
      audioKey: objectKey,
      bytes: audio.byteLength,
      durationSeconds: estSeconds,
      words, chars: charCount, credits,
      script,
      historyEntryUsed: !!hist.entry,
      historyEntryVerified: hist.entry?.verified ?? null,
      calendarCount: cal.events.length,
      foodIncluded: !!food.item,
      audit,
    };
    episode.title = toSpeakableAscii(episode.title);
    await env.STLNOW_KV.put(`stlnow:episode:${day}`, JSON.stringify(episode));

    let manifest = [];
    try {
      const raw = await env.STLNOW_KV.get('stlnow:manifest');
      if (raw) manifest = JSON.parse(raw);
    } catch { manifest = []; }
    manifest = manifest.filter((e) => e.id !== day);
    manifest.unshift({
      id: episode.id, title: episode.title, pubDate: episode.pubDate,
      audioKey: episode.audioKey, bytes: episode.bytes, durationSeconds: episode.durationSeconds,
      blurb: script.split(/(?<=\.)\s+/).slice(0, 2).join(' ').slice(0, 400),
    });
    manifest = manifest.slice(0, 60);
    await env.STLNOW_KV.put('stlnow:manifest', JSON.stringify(manifest));

    // --- 8. Ledger.
    ledger.creditsUsed += credits;
    ledger.episodes += 1;
    ledger.lastEpisode = day;
    await writeCreditLedger(env.STLNOW_KV, ledger);

    result.ok = true;
    result.episode = {
      id: episode.id, words, chars: charCount, credits, bytes: audio.byteLength,
      durationSeconds: estSeconds, auditClean: audit.clean, auditFlags: audit.flags.length,
    };
    result.ledger = { creditsUsed: ledger.creditsUsed, budget: MONTHLY_CREDIT_BUDGET, episodes: ledger.episodes };
    return result;

  } catch (err) {
    diagnostics.push({ step: 'error', ok: false, error: err.message });
    result.ok = false; result.error = err.message;
    return result;
  }
}
