// ai-ingest.js
// Task 11 for stl-dispatcher -- the daily AI edition behind ai.stluker.com.
// ADDED 2026-10-04.
//
//   cron 45 11 * * *  (after podcastIngest at :30, before statusSync at :55)
//     |- fetch ~11 AI news feeds + Hacker News (fetchSource, parallel)
//     |- parse, keep last ~30h, dedupe by URL/title  -> candidate list
//     |- 1x Claude call -> lede + 6-10 picked stories (JSON)
//     |- audit: every picked story must cite a real candidate id, and its
//     |         link is taken FROM the candidate, never from the model
//     '- KV put  ai:day:YYYY-MM-DD, ai:index
//
// The ai-daily Worker (ai.stluker.com) reads those keys at request time,
// same KV-at-request-time pattern as space/earth/pod. Storage reuses
// PODCAST_KV with an "ai:" prefix rather than a new namespace, so there is
// no REPLACE_AFTER_CREATE id to forget (Jul 23 intel placeholder failure).
//
// Edition dating: the edition run on UTC day D covers news from roughly the
// preceding 30 hours, and is labeled D. The Mon/Thu recap episode
// (ai-podcast.js) reads editions by these labels.

import { fetchSource, recordSourceResult } from './fetch-source.js';

const MODEL = 'claude-sonnet-5';
const ANTHROPIC_VERSION = '2023-06-01';
// Adaptive thinking shares this budget with the JSON output (see the
// 2026-09-24 note in podcast-ingest.js) -- keep it generous.
const MAX_TOKENS = 16000;
const WINDOW_HOURS = 30;
const MAX_CANDIDATES = 70;
const MIN_STORIES = 4;
const MAX_STORIES = 10;
const INDEX_CAP = 400;

// Verified reachable 2026-10-04 (all HTTP 200 with items). Dropped at build
// time: venturebeat (429 to plain requests), anthropic.com (no RSS feed).
export const AI_FEEDS = [
  { id: 'openai', name: 'OpenAI', url: 'https://openai.com/news/rss.xml', firstParty: true },
  { id: 'deepmind', name: 'Google DeepMind', url: 'https://deepmind.google/blog/rss.xml', firstParty: true },
  { id: 'google-ai', name: 'Google AI', url: 'https://blog.google/technology/ai/rss/', firstParty: true },
  { id: 'huggingface', name: 'Hugging Face', url: 'https://huggingface.co/blog/feed.xml', firstParty: true },
  { id: 'verge-ai', name: 'The Verge', url: 'https://www.theverge.com/rss/ai-artificial-intelligence/index.xml' },
  { id: 'techcrunch-ai', name: 'TechCrunch', url: 'https://techcrunch.com/category/artificial-intelligence/feed/' },
  { id: 'ars-ai', name: 'Ars Technica', url: 'https://arstechnica.com/ai/feed/' },
  { id: 'mittr-ai', name: 'MIT Technology Review', url: 'https://www.technologyreview.com/topic/artificial-intelligence/feed' },
  { id: 'wired-ai', name: 'Wired', url: 'https://www.wired.com/feed/tag/ai/latest/rss' },
  { id: 'willison', name: 'Simon Willison', url: 'https://simonwillison.net/atom/everything/' },
];

// HN has no AI-only feed; take well-upvoted stories and keyword-filter.
const HN_MIN_POINTS = 120;
const AI_KEYWORDS = /\b(ai|a\.i\.|llms?|gpt[-\w.]*|claude|gemini|llama|mistral|openai|anthropic|deepmind|chatgpt|copilot|agents?|agentic|transformer|diffusion|neural|machine learning|ml|inference|fine-?tun\w*|rlhf|embeddings?|nvidia|gpus?|datacenter|data center|model weights|open-?weights?|reasoning model|frontier model)\b/i;

export const CATEGORIES = ['Models', 'Products', 'Research', 'Business', 'Policy', 'Open Source', 'Hardware', 'Safety'];

// ── date helpers ───────────────────────────────────────────────────────────
export function utcDayKey(d = new Date()) {
  return d.toISOString().slice(0, 10);
}

// ── feed parsing (regex, like the rest of this Worker -- no DOM in Workers) ─
function decodeEntities(s) {
  return String(s || '')
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCodePoint(parseInt(n, 16)))
    .replace(/&quot;/g, '"')
    .replace(/&apos;|&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&');
}

function stripTags(s) {
  // Decode first: many feeds entity-encode their HTML descriptions.
  return decodeEntities(decodeEntities(s))
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tag(block, name) {
  const m = block.match(new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)</${name}>`, 'i'));
  return m ? m[1] : '';
}

function atomLink(block) {
  // Prefer rel="alternate" (or no rel); Atom feeds often list several links.
  const links = [...block.matchAll(/<link\b([^>]*)\/?>/gi)].map((m) => m[1]);
  for (const attrs of links) {
    const rel = (attrs.match(/rel=["']([^"']+)["']/i) || [])[1];
    const href = (attrs.match(/href=["']([^"']+)["']/i) || [])[1];
    if (href && (!rel || rel === 'alternate')) return href;
  }
  return '';
}

export function parseFeed(xml) {
  const items = [];
  const isAtom = /<feed\b/i.test(xml) && !/<rss\b/i.test(xml);
  const blocks = isAtom
    ? xml.match(/<entry\b[\s\S]*?<\/entry>/gi) || []
    : xml.match(/<item\b[\s\S]*?<\/item>/gi) || [];
  for (const b of blocks) {
    const title = stripTags(tag(b, 'title'));
    let link = isAtom ? atomLink(b) : decodeEntities(tag(b, 'link')).trim();
    if (!link && !isAtom) link = atomLink(b); // some RSS feeds use <link href>
    if (!link) {
      const guid = decodeEntities(tag(b, 'guid')).trim();
      if (/^https?:\/\//.test(guid)) link = guid;
    }
    const dateRaw = tag(b, 'pubDate') || tag(b, 'published') || tag(b, 'updated') || tag(b, 'dc:date');
    const ts = Date.parse(decodeEntities(dateRaw).trim());
    const summary = stripTags(tag(b, 'description') || tag(b, 'summary') || tag(b, 'content') || tag(b, 'content:encoded'));
    if (!title || !link) continue;
    items.push({ title, link: link.trim(), publishedAt: Number.isFinite(ts) ? new Date(ts).toISOString() : null, summary });
  }
  return items;
}

function normUrl(u) {
  try {
    const x = new URL(u);
    x.hash = '';
    for (const k of [...x.searchParams.keys()]) if (/^(utm_|ref$|source$)/i.test(k)) x.searchParams.delete(k);
    return (x.host.replace(/^www\./, '') + x.pathname.replace(/\/$/, '') + x.search).toLowerCase();
  } catch { return String(u).toLowerCase(); }
}

function normTitle(t) {
  return String(t).toLowerCase().replace(/[^a-z0-9 ]/g, '').replace(/\s+/g, ' ').trim();
}

// ── gather ─────────────────────────────────────────────────────────────────
async function gatherCandidates(env, now, diagnostics, findings) {
  const since = now.getTime() - WINDOW_HOURS * 3600 * 1000;
  const sinceSec = Math.floor(since / 1000);

  const feedResults = await Promise.all(AI_FEEDS.map(async (f) => {
    const r = await fetchSource(f.url);
    let parsed = [];
    if (r.ok) {
      try { parsed = parseFeed(r.body); } catch (e) { r.error = `parse: ${e}`; }
    }
    // Streak counts PARSED items (feed health), not in-window items: the
    // first-party blogs legitimately go days without a post, and that must
    // not read as a silent source.
    const rec = await recordSourceResult(env.PODCAST_KV, `ai:${f.id}`, { count: parsed.length, error: r.error, status: r.status, ms: r.ms });
    findings.push(...rec.findings);
    const fresh = parsed.filter((it) => it.publishedAt && Date.parse(it.publishedAt) >= since);
    diagnostics.push({ ...rec.diagnostics, fresh: fresh.length });
    return fresh.map((it) => ({ ...it, source: f.name, sourceId: f.id, firstParty: !!f.firstParty }));
  }));

  // Hacker News -- community signal on what actually landed.
  let hn = [];
  const hnUrl = `https://hn.algolia.com/api/v1/search?tags=story&hitsPerPage=100&numericFilters=created_at_i>${sinceSec},points>${HN_MIN_POINTS}`;
  const hr = await fetchSource(hnUrl, { parse: 'json', accept: 'application/json' });
  if (hr.ok) {
    hn = (hr.body?.hits || [])
      .filter((h) => h.title && AI_KEYWORDS.test(h.title))
      .map((h) => ({
        title: h.title,
        link: h.url || `https://news.ycombinator.com/item?id=${h.objectID}`,
        publishedAt: h.created_at,
        summary: `Hacker News: ${h.points} points, ${h.num_comments || 0} comments.`,
        source: 'Hacker News',
        sourceId: 'hn',
        hnPoints: h.points,
        discussion: `https://news.ycombinator.com/item?id=${h.objectID}`,
      }));
  }
  const hrec = await recordSourceResult(env.PODCAST_KV, 'ai:hn', { count: (hr.body?.hits || []).length, error: hr.error, status: hr.status, ms: hr.ms });
  findings.push(...hrec.findings);
  diagnostics.push({ ...hrec.diagnostics, fresh: hn.length });

  // Dedupe: same URL or same title. First-party posts win over coverage of
  // them, and an HN item that duplicates a feed item just adds its points.
  const all = [...feedResults.flat(), ...hn];
  all.sort((a, b) => (b.firstParty ? 1 : 0) - (a.firstParty ? 1 : 0));
  const byKey = new Map();
  const out = [];
  for (const it of all) {
    const ku = normUrl(it.link);
    const kt = normTitle(it.title);
    const dup = byKey.get(ku) || byKey.get(kt);
    if (dup) {
      if (it.hnPoints && !dup.hnPoints) { dup.hnPoints = it.hnPoints; dup.discussion = it.discussion; }
      continue;
    }
    byKey.set(ku, it); byKey.set(kt, it);
    out.push(it);
  }

  // Cap what we send the model: first-party + HN-ranked first, then newest.
  out.sort((a, b) =>
    (b.firstParty ? 1 : 0) - (a.firstParty ? 1 : 0) ||
    (b.hnPoints || 0) - (a.hnPoints || 0) ||
    Date.parse(b.publishedAt || 0) - Date.parse(a.publishedAt || 0));
  return out.slice(0, MAX_CANDIDATES).map((it, i) => ({ id: `c${i + 1}`, ...it, summary: (it.summary || '').slice(0, 500) }));
}

// ── editorial call ─────────────────────────────────────────────────────────
const SYSTEM_PROMPT = `You are the editor of a daily AI news page for a smart, busy general reader who follows AI closely but does not have time to read everything. You receive a numbered list of candidate items gathered from AI news feeds and Hacker News over the last day.

Pick the ${MIN_STORIES}-${MAX_STORIES} items that genuinely matter: new models and capabilities, notable product launches, significant research, major business moves (funding, acquisitions, partnerships), regulation and policy, safety findings, hardware and compute. Merge duplicates covering the same event into ONE story and list every candidate id that covers it.

Skip: listicles, how-to tutorials, opinion pieces with no news, minor feature tweaks, sponsored posts, and anything only loosely about AI.

Hard rules:
- Use ONLY facts present in the candidate titles and summaries. Never add a number, name, date, benchmark, price, or claim that is not in them. If a summary is thin, write a shorter, vaguer summary rather than filling gaps from memory.
- Every story must cite at least one candidate id from the list in "sources". Do not invent ids.
- The headline must not claim more than the summary supports. Keep the stage exact: "will test" is not "rolls out", "plans to" is not "launches", a preview is not general availability.
- Hacker News points and comment counts are a ranking signal for you, not news. Never put them in a headline, summary, or why-it-matters line. Report what the linked story says.
- Plain, specific language. No hype words (revolutionary, game-changing, groundbreaking, insane). No "In a move that...".
- "category" must be one of: ${CATEGORIES.join(', ')}.

Return ONLY a JSON object, no prose around it:
{
  "lede": "2-3 sentences: the shape of the day in AI, naming the top one or two stories.",
  "stories": [
    {
      "headline": "Short factual headline, max 90 chars",
      "summary": "2-3 sentences of what happened.",
      "why": "One sentence on why it matters, grounded in the source text.",
      "category": "Models",
      "importance": 1-5 (5 = the story of the day),
      "sources": ["c3", "c17"]
    }
  ]
}
Order stories from most to least important.`;

function parseJsonLoose(text) {
  const s = String(text || '');
  const start = s.indexOf('{');
  const end = s.lastIndexOf('}');
  if (start < 0 || end <= start) throw new Error('no JSON object in model output');
  return JSON.parse(s.slice(start, end + 1));
}

export async function callClaude(env, { system, user, maxTokens = MAX_TOKENS }) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': ANTHROPIC_VERSION },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: maxTokens,
      system: [{ type: 'text', text: system, cache_control: { type: 'ephemeral' } }],
      messages: [{ role: 'user', content: user }],
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Anthropic HTTP ${res.status}: ${body.slice(0, 300)}`);
  }
  const data = await res.json();
  const text = (data.content || []).filter((b) => b.type === 'text').map((b) => b.text).join('');
  return { text, usage: data.usage || null, stopReason: data.stop_reason };
}

function auditEdition(raw, candidates) {
  const byId = new Map(candidates.map((c) => [c.id, c]));
  const dropped = [];
  const stories = [];
  for (const s of Array.isArray(raw.stories) ? raw.stories : []) {
    const ids = [...new Set((s.sources || []).filter((id) => byId.has(id)))];
    if (!ids.length || !s.headline || !s.summary) {
      dropped.push({ headline: s.headline || null, reason: !ids.length ? 'no valid source ids' : 'missing fields' });
      continue;
    }
    // Links come from the candidates we fetched, never from model output.
    const links = ids.map((id) => {
      const c = byId.get(id);
      return { title: c.title, url: c.link, source: c.source, discussion: c.discussion || null };
    });
    stories.push({
      headline: String(s.headline).slice(0, 140),
      summary: String(s.summary),
      why: s.why ? String(s.why) : '',
      category: CATEGORIES.includes(s.category) ? s.category : 'Products',
      importance: Math.min(5, Math.max(1, Number(s.importance) || 3)),
      links,
    });
    if (stories.length >= MAX_STORIES) break;
  }
  return { lede: String(raw.lede || '').trim(), stories, dropped };
}

// ── entry point ────────────────────────────────────────────────────────────
export async function runAiIngest(env, { force = false, now = new Date() } = {}) {
  const diagnostics = [];
  const findings = [];
  const day = utcDayKey(now);

  if (!force) {
    const existing = await env.PODCAST_KV.get(`ai:day:${day}`);
    if (existing) return { ok: true, day, skipped: 'edition already exists for today', diagnostics };
  }

  const candidates = await gatherCandidates(env, now, diagnostics, findings);
  diagnostics.push({ step: 'candidates', count: candidates.length });
  if (candidates.length < MIN_STORIES) {
    return { ok: false, day, error: `only ${candidates.length} candidates in the last ${WINDOW_HOURS}h -- feeds likely broken`, diagnostics, findings };
  }

  const userMsg = `Today is ${day}. Candidates:\n\n` + candidates.map((c) =>
    `[${c.id}] (${c.source}${c.hnPoints ? `, HN ${c.hnPoints} pts` : ''}) ${c.title}\n${c.summary}`).join('\n\n');

  let edition;
  let usage = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const r = await callClaude(env, { system: SYSTEM_PROMPT, user: userMsg });
      usage = r.usage;
      edition = auditEdition(parseJsonLoose(r.text), candidates);
      diagnostics.push({ step: 'editor', attempt, ok: true, stories: edition.stories.length, dropped: edition.dropped, usage, stopReason: r.stopReason });
      if (edition.stories.length >= MIN_STORIES) break;
    } catch (e) {
      diagnostics.push({ step: 'editor', attempt, ok: false, error: String(e) });
    }
  }
  if (!edition || edition.stories.length < MIN_STORIES) {
    return { ok: false, day, error: 'editor call did not produce enough valid stories', diagnostics, findings };
  }

  const record = {
    day,
    generatedAt: new Date().toISOString(),
    lede: edition.lede,
    stories: edition.stories,
    candidateCount: candidates.length,
    sources: [...new Set(candidates.map((c) => c.source))],
  };
  await env.PODCAST_KV.put(`ai:day:${day}`, JSON.stringify(record));

  let index = [];
  try { index = JSON.parse((await env.PODCAST_KV.get('ai:index')) || '[]'); } catch { index = []; }
  const entry = { day, headline: record.stories[0].headline, count: record.stories.length };
  index = [entry, ...index.filter((e) => e.day !== day)].sort((a, b) => (a.day < b.day ? 1 : -1)).slice(0, INDEX_CAP);
  await env.PODCAST_KV.put('ai:index', JSON.stringify(index));

  return { ok: true, day, stories: record.stories.length, candidates: candidates.length, usage, diagnostics, findings };
}
