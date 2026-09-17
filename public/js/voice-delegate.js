// ══════════════════════════════════════════════════════
// VOICE DELEGATE — speak a task instead of typing the form
//
// The browser's own speech recognition transcribes what you say, then
// the parser below pulls the doer, client, due date and priority out of
// that text and drops them into the Delegate Task modal. Nothing leaves
// the browser: no transcript is posted anywhere and there is no API cost
// per task.
//
// Recognition runs in English (en-IN) only — see VD_LANG for why a Hindi
// locale was dropped. The parser still accepts the Hindi words that turn up
// inside spoken English, because the team mixes them in mid-sentence: "due
// tomorrow" and "kal tak" land on the same date. Those aliases are all
// Latin script, which is what en-IN returns.
//
// Anything it cannot place confidently is left blank instead of guessed.
// A misheard name silently assigning a task to the wrong person is a far
// worse outcome than an empty dropdown, which the assigner can see.
// ══════════════════════════════════════════════════════

// Tokens dropped before a client name is matched. They carry no
// identifying weight — "Sharma Pvt Ltd" and "Sharma Limited" should both
// match on "Sharma" — and leaving them in lets any two companies score
// against each other on their suffixes alone.
const VD_CLIENT_NOISE = new Set(['pvt', 'private', 'ltd', 'limited', 'llp', 'inc', 'the', 'and', 'india']);

// Connectors that mean nothing on their own. Stripped from the ends of
// the description once the matched phrases are carved out, so a sentence
// does not end on a dangling "ko" or "tak".
const VD_EDGE_FILLER = new Set(['ko', 'ki', 'ka', 'ke', 'se', 'tak', 'aur', 'and', 'to', 'for', 'of', 'liye', 'par', 'pe', 'na', 'ye', 'yeh', 'wo', 'woh', 'is', 'hi', 'bhi', 'by', 'the', 'plz', 'please']);

// A comma-separated run made of nothing but these is dropped whole. Carving
// a matched phrase out of dictated speech routinely strands the label that
// introduced it — "client Vaidehi Enterprises, due tomorrow" leaves behind
// "client," and "due," once both values are claimed. Judged per segment
// rather than per word, so a task that is genuinely about client onboarding
// keeps the word "client" in the middle of its own sentence.
const VD_SEGMENT_FILLER = new Set([...VD_EDGE_FILLER,
  'hai', 'he', 'hain', 'karna', 'kare', 'karo', 'client', 'due', 'date', 'deadline', 'priority', 'doer', 'approval', 'task', 'by']);

// Lead-ins people say before the task itself. Removed only from the
// front — "bolo ki" mid-sentence is usually part of the task.
const VD_LEAD_INS = [
  'assign kar do', 'assign karna hai', 'assign karo', 'assign kardo',
  'delegate kar do', 'delegate karna hai', 'delegate karo', 'delegate kardo',
  'task assign karo', 'ek task hai', 'ye task hai', 'yeh task hai', 'task hai',
  'bol do ki', 'bolo ki', 'keh do ki', 'kehna hai ki', 'bata do ki',
  'please', 'plz', 'task'
];

const VD_WEEKDAYS = {
  sunday: 0, ravivar: 0, itwar: 0, itvar: 0,
  monday: 1, somvar: 1, somwar: 1,
  tuesday: 2, mangalvar: 2, mangalwar: 2,
  wednesday: 3, budhvar: 3, budhwar: 3,
  thursday: 4, guruvar: 4, guruwar: 4, brihaspativar: 4,
  friday: 5, shukravar: 5, shukrawar: 5,
  saturday: 6, shanivar: 6, shaniwar: 6
};

const VD_MONTHS = {
  jan: 0, january: 0, feb: 1, february: 1, mar: 2, march: 2, apr: 3, april: 3,
  may: 4, jun: 5, june: 5, jul: 6, july: 6, aug: 7, august: 7,
  sep: 8, sept: 8, september: 8, oct: 9, october: 9, nov: 10, november: 10,
  dec: 11, december: 11
};

const VD_NUMBER_WORDS = {
  one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10,
  ek: 1, do: 2, teen: 3, char: 4, chaar: 4, paanch: 5, panch: 5, chhe: 6, che: 6,
  saat: 7, aath: 8, nau: 9, das: 10
};

const VD_PRIORITY_RULES = [
  ['urgent', '(?:urgent(?:\\s+priority)?|emergency|asap|turant|foran|fauran|sabse\\s+pehle|top\\s+priority)'],
  ['high', '(?:high\\s+priority|high|zaroori|zaruri|jaruri|jaroori|important)'],
  ['medium', '(?:medium\\s+priority|medium|normal(?:\\s+priority)?)'],
  ['low', '(?:low\\s+priority|low|koi\\s+jaldi\\s+nahi|no\\s+rush)']
];

function vdISO(d) {
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}

function vdAddDays(base, n) {
  const d = new Date(base.getFullYear(), base.getMonth(), base.getDate());
  d.setDate(d.getDate() + n);
  return d;
}

// Next occurrence of a weekday, never today. Someone saying "Friday" on a
// Friday means the coming one, not the deadline they are already inside.
function vdNextWeekday(base, target) {
  let delta = (target - base.getDay() + 7) % 7;
  if (delta === 0) delta = 7;
  return vdAddDays(base, delta);
}

function vdNumFrom(word) {
  if (!word) return null;
  const n = parseInt(word, 10);
  if (!isNaN(n)) return n;
  return VD_NUMBER_WORDS[String(word).toLowerCase()] || null;
}

function vdEscapeRe(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Whole-word match. Written with lookaround rather than \b so a name ending
// in punctuation still matches while "abc" stays out of "abcd".
function vdWordRe(word) {
  return '(?<![a-z0-9])' + vdEscapeRe(word) + '(?![a-z0-9])';
}

// ──────────────────────────────────────────────
// Due date
//
// The first rule that fires wins, most specific first: an explicit date
// beats "in 3 days", which beats "day after tomorrow", which beats the
// bare "tomorrow" hiding inside it.
//
// "kal" means both yesterday and tomorrow in Hindi. On a due date it is
// read as tomorrow — a due date in the past is never what anyone meant.
// ──────────────────────────────────────────────
function vdParseDate(lower, today) {
  const base = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const monthNames = Object.keys(VD_MONTHS).sort((a, b) => b.length - a.length).join('|');
  const numWords = Object.keys(VD_NUMBER_WORDS).join('|');
  const dayNames = Object.keys(VD_WEEKDAYS).join('|');

  const rules = [
    // 20 September / 20th Sept / September 20
    ['(\\d{1,2})\\s*(?:st|nd|rd|th)?\\s+(?:of\\s+)?(' + monthNames + ')\\b',
      m => vdFromDayMonth(base, parseInt(m[1], 10), VD_MONTHS[m[2]])],
    ['\\b(' + monthNames + ')\\s+(\\d{1,2})\\s*(?:st|nd|rd|th)?\\b',
      m => vdFromDayMonth(base, parseInt(m[2], 10), VD_MONTHS[m[1]])],
    // 20/09/2026, 20-9-26
    ['\\b(\\d{1,2})[\\/\\-](\\d{1,2})[\\/\\-](\\d{2,4})\\b', m => {
      let yr = parseInt(m[3], 10);
      if (yr < 100) yr += 2000;
      return new Date(yr, parseInt(m[2], 10) - 1, parseInt(m[1], 10));
    }],
    // "20 tarikh" — a day with no month behind it
    ['\\b(\\d{1,2})\\s*(?:tarikh|tareekh|tareek|taarikh)\\b',
      m => vdFromDayMonth(base, parseInt(m[1], 10), null)],
    // in 3 days / 3 din baad / teen din me / 3 days later
    ['\\b(?:in|after|within)\\s+(\\d{1,2}|' + numWords + ')\\s+(?:days?|din)\\b',
      m => (vdNumFrom(m[1]) ? vdAddDays(base, vdNumFrom(m[1])) : null)],
    ['\\b(\\d{1,2}|' + numWords + ')\\s+din\\s*(?:baad|bad|me|mein|ke\\s+andar)\\b',
      m => (vdNumFrom(m[1]) ? vdAddDays(base, vdNumFrom(m[1])) : null)],
    ['\\b(\\d{1,2}|' + numWords + ')\\s+days?\\s+(?:later|from\\s+now)\\b',
      m => (vdNumFrom(m[1]) ? vdAddDays(base, vdNumFrom(m[1])) : null)],
    // Checked before "tomorrow" and "kal", both of which they contain
    ['\\bday\\s+after\\s+tomorrow\\b', () => vdAddDays(base, 2)],
    ['\\b(?:parso|parson|parsu|parsoon)\\b', () => vdAddDays(base, 2)],
    ['\\b(?:end\\s+of\\s+(?:the\\s+)?month|month\\s*end|mahine\\s+ke\\s+(?:end|aakhir|akhir))\\b',
      () => new Date(base.getFullYear(), base.getMonth() + 1, 0)],
    ['\\b(?:next\\s+month|agle\\s+(?:mahine|month))\\b',
      () => new Date(base.getFullYear(), base.getMonth() + 1, base.getDate())],
    ['\\b(?:next\\s+week|agle\\s+(?:hafte|hafta|week))\\b', () => vdAddDays(base, 7)],
    ['\\b(?:next\\s+|agle\\s+)?(' + dayNames + ')\\b',
      m => vdNextWeekday(base, VD_WEEKDAYS[m[1]])],
    ['\\btomorrow\\b', () => vdAddDays(base, 1)],
    ['\\b(?:kal|kl)\\b', () => vdAddDays(base, 1)],
    ['\\btoday\\b', () => base],
    ['\\b(?:aaj|aj)\\b', () => base]
  ];

  for (const [pattern, build] of rules) {
    const m = new RegExp(pattern, 'i').exec(lower);
    if (!m) continue;
    const d = build(m);
    if (!d || isNaN(d.getTime())) continue;
    let end = m.index + m[0].length;
    // "kal tak", "20 tarikh tak" — swallow the trailing "tak"/"by" so it
    // does not survive into the description on its own.
    const tail = /^\s*(?:tak|se\s+pehle|before\s+that)\b/i.exec(lower.slice(end));
    if (tail) end += tail[0].length;
    return { date: vdISO(d), phrase: lower.slice(m.index, end).trim(), span: [m.index, end] };
  }
  return null;
}

// A day with no month attached belongs to this month, unless that day has
// already gone by — "20 tarikh" said on the 25th means next month.
function vdFromDayMonth(base, day, month) {
  if (day < 1 || day > 31) return null;
  if (month === null) {
    const d = new Date(base.getFullYear(), base.getMonth(), day);
    return d < base ? new Date(base.getFullYear(), base.getMonth() + 1, day) : d;
  }
  const d = new Date(base.getFullYear(), month, day);
  return d < base ? new Date(base.getFullYear() + 1, month, day) : d;
}

// ──────────────────────────────────────────────
// People
//
// Scored rather than first-match: "Naman" should lose to "Naman Sharma"
// when both are in the list and the full name was spoken. A tie between
// two different people is reported as ambiguous and fills nothing, since
// picking either one at random is how the wrong person gets the task.
// ──────────────────────────────────────────────
// Edit distance, abandoned as soon as it passes `cap`. Only ever used to
// reject, so the exact number above the cap never matters.
function vdEditDistance(a, b, cap) {
  if (Math.abs(a.length - b.length) > cap) return cap + 1;
  let prev = Array.from({ length: b.length + 1 }, (_, i) => i);
  for (let i = 1; i <= a.length; i++) {
    const cur = [i];
    let rowMin = i;
    for (let j = 1; j <= b.length; j++) {
      cur[j] = Math.min(
        prev[j] + 1,
        cur[j - 1] + 1,
        prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1)
      );
      if (cur[j] < rowMin) rowMin = cur[j];
    }
    if (rowMin > cap) return cap + 1;
    prev = cur;
  }
  return prev[b.length];
}

// Last-resort name matching, tried only once every exact candidate has
// failed. Speech mangles Indian names in two ways that an exact match can
// never survive: it mis-spells them ("Namon", "Ashis"), and it splits them
// across a word break ("no man" for "Naman"). So each word is compared to
// the name, and so is each adjacent pair joined together.
//
// The tolerance is deliberately tight — one edit for a short name, two for
// a long one — because this runs against every person in the directory at
// once. Loosening it does not produce a better guess, it produces a
// confident wrong one, and the ambiguity guard below is the only thing
// standing between that and somebody else's task list.
// Speech stretches vowels far more often than it gets a consonant wrong —
// "Rahool", "Shailoo", "Nammann". Collapsing every doubled letter on both
// sides absorbs that whole class before the edit budget is touched, which
// is why the budget can stay at one for short names. Spending a second edit
// on it instead would let "Admin" match "adding", and Admin is a real row
// in this directory.
function vdSquash(s) {
  return String(s).toLowerCase().replace(/(.)\1+/g, '$1');
}

function vdFuzzyHit(lower, token) {
  if (token.length < 4) return null;
  const cap = token.length >= 6 ? 2 : 1;
  const tokenSq = vdSquash(token);
  const re = /(?<![a-z0-9])([a-z]+)(?:(\s+)([a-z]+))?/gi;
  let m;
  let best = null;
  while ((m = re.exec(lower)) !== null) {
    const d1 = vdEditDistance(vdSquash(m[1]), tokenSq, cap);
    if (d1 <= cap && (!best || d1 < best.dist)) {
      best = { dist: d1, index: m.index, length: m[1].length };
    }
    if (m[3]) {
      const d2 = vdEditDistance(vdSquash(m[1] + m[3]), tokenSq, cap);
      if (d2 <= cap && (!best || d2 < best.dist)) {
        best = { dist: d2, index: m.index, length: m[0].length };
      }
    }
    re.lastIndex = m.index + m[1].length;   // let pairs overlap
  }
  return best;
}

function vdMatchPerson(lower, people, excludeId) {
  let best = null;
  let runnerUp = null;

  for (const p of people) {
    if (excludeId && String(p.id) === String(excludeId)) continue;
    const name = String(p.name || '').trim();
    if (!name) continue;
    const parts = name.toLowerCase().split(/\s+/).filter(Boolean);
    const candidates = [[name.toLowerCase(), 10]];
    if (parts.length > 1) {
      candidates.push([parts[0], 6]);
      // Short surnames collide with ordinary words too often to be trusted
      // on their own ("Rai", "Jha", "Roy" all turn up inside other speech).
      const last = parts[parts.length - 1];
      if (last.length >= 4) candidates.push([last, 4]);
    }

    let hit = null;
    for (const [token, weight] of candidates) {
      const m = new RegExp(vdWordRe(token), 'i').exec(lower);
      if (!m) continue;
      let start = m.index;
      let end = m.index + m[0].length;
      let score = weight;
      // "Naman ko", "Naman ji", "Naman ke liye" — a postposition right
      // after a name is the clearest signal in Hinglish that this is the
      // person being addressed, not a name mentioned inside the task.
      const after = /^\s*(?:ko|ji|ke\s+liye|sir|madam|ma'am)\b/i.exec(lower.slice(end));
      if (after) { score += 3; end += after[0].length; }
      // "assign to Naman", "give it to Naman"
      const beforeRe = /(?:assign(?:\s+it)?\s+to|delegate(?:\s+it)?\s+to|give(?:\s+it)?\s+to|dedo|de\s+do)\s*$/i;
      const beforeM = beforeRe.exec(lower.slice(0, start));
      if (beforeM) { score += 3; start -= beforeM[0].length; }
      if (!hit || score > hit.score) hit = { score: score, span: [start, end] };
    }

    // Nothing spelled right. Try the near-misses, scored below every exact
    // tier so a correctly heard name always beats a repaired one.
    if (!hit) {
      const fuzzTargets = parts.length > 1
        ? [[name.toLowerCase(), 3], [parts[0], 2]]
        : [[parts[0], 2]];
      for (const [token, weight] of fuzzTargets) {
        const f = vdFuzzyHit(lower, token);
        if (!f) continue;
        // A closer repair outranks a looser one at the same tier.
        const score = weight + (f.dist === 0 ? 1 : 0);
        if (!hit || score > hit.score) hit = { score: score, span: [f.index, f.index + f.length], fuzzy: true };
      }
    }
    if (!hit) continue;

    const cand = { id: p.id, name: name, score: hit.score, span: hit.span };
    if (!best || cand.score > best.score) { runnerUp = best; best = cand; }
    else if (!runnerUp || cand.score > runnerUp.score) runnerUp = cand;
  }

  if (!best) return null;
  if (runnerUp && runnerUp.score === best.score) {
    return { ambiguous: [best.name, runnerUp.name] };
  }
  return best;
}

// ──────────────────────────────────────────────
// Client
//
// Company names come back from speech in shortened form far more often
// than people's names do — "ABC Technologies Pvt Ltd" is spoken as "ABC".
// So the whole name is tried first, then the significant words are scored
// individually and the best-covered client wins.
// ──────────────────────────────────────────────
function vdMatchClient(lower, clients, relaxed) {
  let best = null;
  let runnerUp = null;

  for (const c of clients) {
    const name = String(c.name || '').trim();
    if (!name) continue;
    const full = new RegExp(vdWordRe(name.toLowerCase()), 'i').exec(lower);
    if (full) {
      const cand = { id: c.id, name: name, score: 100, span: [full.index, full.index + full[0].length] };
      if (!best || cand.score > best.score) { runnerUp = best; best = cand; }
      continue;
    }

    const tokens = name.toLowerCase().split(/[\s.,]+/)
      .filter(t => t.length > 1 && !VD_CLIENT_NOISE.has(t));
    if (!tokens.length) continue;

    let matched = 0;
    let lo = Infinity;
    let hi = -1;
    for (const t of tokens) {
      const m = new RegExp(vdWordRe(t), 'i').exec(lower);
      const hit = m ? { index: m.index, length: m[0].length }
        : (relaxed ? vdPrefixHit(lower, t) : null);
      if (!hit) continue;
      matched++;
      lo = Math.min(lo, hit.index);
      hi = Math.max(hi, hit.index + hit.length);
    }
    if (!matched) continue;
    // A single-word client has to match that one word outright. A longer
    // name needs at least half its words, so "ABC Technologies" is not
    // claimed by every sentence that happens to contain "technologies".
    if (tokens.length > 1 && matched / tokens.length < 0.5) continue;

    const cand = { id: c.id, name: name, score: matched * 10 + tokens.length, span: [lo, hi] };
    if (!best || cand.score > best.score) { runnerUp = best; best = cand; }
    else if (!runnerUp || cand.score > runnerUp.score) runnerUp = cand;
  }

  if (!best) return null;
  if (runnerUp && runnerUp.score === best.score) {
    return { ambiguous: [best.name, runnerUp.name] };
  }
  // "ABC ka report", "ABC ke liye" — pull the possessive in with the name
  // so it does not head the description.
  const tail = /^\s*(?:ka|ke|ki|ka\s+kaam|ke\s+liye)\b/i.exec(lower.slice(best.span[1]));
  if (tail) best.span[1] += tail[0].length;
  return best;
}

// ──────────────────────────────────────────────
// Template mode
//
// Free-form parsing has to work out which words are the task and which are
// scaffolding, and it gets that wrong on a sentence that names its own
// fields: "I want to delegate a task to Naman, the due date will be 17th
// September, priority will be medium" left the entire frame sitting in the
// description, because none of those words belonged to any other field.
//
// So a sentence that labels its fields is read by those labels instead.
// Each marker ends one slot and begins the next, and a slot is matched only
// against its own text — a client name can no longer be read out of the
// description, or a doer out of the client.
//
// The spelling alternatives are not tidiness, they are what recognition
// actually returns. "a task to" comes back as "ask to" and "client name" as
// "clan name" often enough that leaving them out would make the template
// fail on the very sentence it was written for.
// ──────────────────────────────────────────────
const VD_SLOT_MARKERS = [
  ['doer', '(?:delegate|assign|give)\\s+(?:a\\s+)?(?:task|ask|desk)?\\s*to'],
  ['desc', '(?:the\\s+)?task\\s+(?:will\\s+be|would\\s+be|is|hai|hoga|hogi)'],
  ['date', '(?:the\\s+)?due\\s*date\\s+(?:will\\s+be|would\\s+be|is|hogi|hoga|hai)'],
  ['priority', '(?:the\\s+)?priority\\s+(?:will\\s+be|would\\s+be|is|hogi|hoga|hai)'],
  ['client', '(?:the\\s+)?(?:client|clan|plant|climate|glint)\\s*(?:name)?\\s*(?:will\\s+be|would\\s+be|is|hai)'],
  ['approval', '(?:the\\s+)?approv(?:al|er)\\s*(?:name)?\\s*(?:will\\s+be|would\\s+be|is|from|by|hai)']
];

// Closers and joins that belong to the frame, not to the value inside a slot.
function vdTrimSlot(s) {
  let out = String(s || '').trim();
  out = out.replace(/\b(?:that'?s\s+it|thats\s+it|bas\s+itna|bas|done|over)\s*$/i, '');
  out = out.replace(/^[\s,.;:\-–—]+|[\s,.;:\-–—]+$/g, '');
  out = out.replace(/^(?:and|aur|the)\s+/i, '');
  out = out.replace(/\s+(?:and|aur)$/i, '');
  return out.replace(/^[\s,.;:\-–—]+|[\s,.;:\-–—]+$/g, '');
}

function vdParseSlots(lower, original) {
  const found = [];
  for (const [key, pattern] of VD_SLOT_MARKERS) {
    const m = new RegExp('\\b' + pattern + '\\b', 'i').exec(lower);
    if (m) found.push({ key: key, start: m.index, end: m.index + m[0].length });
  }
  // One marker is as likely to be a turn of phrase as a template. Two is a
  // frame, and everything before the first one is preamble ("I want to…").
  if (found.length < 2) return null;
  found.sort((a, b) => a.start - b.start);

  // Every slot is kept twice. `lat` is the lower-cased text the matchers
  // read; `raw` keeps the assigner's own capitalisation for the description
  // box and for reporting back what was heard on a failed match.
  const raw = {};
  const lat = {};
  for (let i = 0; i < found.length; i++) {
    const next = found[i + 1];
    const ls = found[i].end;
    const le = next ? next.start : lower.length;
    const rv = vdTrimSlot(original.slice(ls, le));
    const lv = vdTrimSlot(lower.slice(ls, le));
    if (!rv && !lv) continue;
    raw[found[i].key] = rv || lv;
    lat[found[i].key] = lv;
  }
  return Object.keys(lat).length ? { raw: raw, lat: lat } : null;
}

// In template mode the slot is already known to be a client name, so a token
// may match on a shared prefix — "technology" for "technologies", which is
// the difference between recognising ABC Technologies and not. Five
// characters is long enough that it does not fire across two unrelated
// companies, and this relaxation is never applied to free-form text.
function vdPrefixHit(lower, token) {
  if (token.length < 5) return null;
  const re = /(?<![a-z0-9])([a-z]{5,})/gi;
  let m;
  while ((m = re.exec(lower)) !== null) {
    const w = m[1].toLowerCase();
    let i = 0;
    while (i < w.length && i < token.length && w[i] === token[i]) i++;
    if (i >= 5) return { index: m.index, length: m[1].length };
  }
  return null;
}

function vdFirstMatch(lower, pattern) {
  const m = new RegExp(pattern, 'i').exec(lower);
  return m ? { value: m, span: [m.index, m.index + m[0].length] } : null;
}

// ──────────────────────────────────────────────
// Domain vocabulary
//
// Recognition has no idea what this company talks about, so it renders
// familiar terms as whatever ordinary English sounds closest — "score
// sheet" comes back as "scot sheet". No parsing change fixes that; the
// words are simply wrong before the parser ever sees them.
//
// ⚠️ This repair is applied to the DESCRIPTION ONLY, and that limit is the
// safety argument, not an oversight. The description lands in a textarea
// the assigner is already reading and can edit, so a wrong correction is
// visible. The same trick on a doer would be invisible and would quietly
// hand the task to somebody else.
//
// Matching is per phrase rather than per word, because the surrounding
// words are what make it safe: "scot" alone is within one edit of plenty of
// real words, while "scot sheet" is close to almost nothing except the term
// intended. ADD TERMS HERE as they turn up — this list is meant to grow,
// and it is the cheapest lever on transcription quality in the whole file.
const VD_VOCAB = [
  'score sheet', 'scorecard', 'balance sheet', 'attendance sheet',
  'salary sheet', 'stock report', 'sales report', 'monthly report',
  'GST return', 'purchase order', 'payment request', 'follow up',
  'reconciliation', 'invoice', 'client feedback', 'week plan'
];

function vdFixVocab(text) {
  if (!text) return text;
  // Longest phrases first, so "attendance sheet" is tried before "sheet"
  // could ever claim half of it.
  const terms = VD_VOCAB.filter(t => t.length >= 6)
    .sort((a, b) => b.split(/\s+/).length - a.split(/\s+/).length);
  const tokens = text.split(/\s+/).filter(Boolean);
  const out = [];
  let i = 0;

  while (i < tokens.length) {
    let hit = null;
    for (const term of terms) {
      const n = term.split(/\s+/).length;
      if (i + n > tokens.length) continue;
      const bare = tokens.slice(i, i + n).join(' ').replace(/[^\w\s]/g, '');
      if (!bare) continue;
      // Already correct, or an inflection of it. Without the prefix test
      // "pending invoices" is one edit from "invoice" and gets "repaired"
      // into the singular — the pass rewriting correct English is worse
      // than it missing a mistake.
      const bl = bare.toLowerCase();
      const tl = term.toLowerCase();
      if (bl === tl || bl.startsWith(tl)) break;
      // A longer term carries more context, so it can afford a second edit.
      const cap = term.length >= 8 ? 2 : 1;
      if (vdEditDistance(vdSquash(bare), vdSquash(term), cap) <= cap) { hit = { term, n }; break; }
      // Recognition also breaks a single word in two — "scorecard" arrives
      // as "scot card". Retry the next token joined on, for one-word terms
      // only, where there is no space in the term to account for.
      if (n === 1 && i + 1 < tokens.length) {
        const glued = tokens.slice(i, i + 2).join('').replace(/[^\w]/g, '');
        if (glued && vdEditDistance(vdSquash(glued), vdSquash(term), cap) <= cap) { hit = { term, n: 2 }; break; }
      }
    }
    if (!hit) { out.push(tokens[i]); i++; continue; }
    // Whatever punctuation closed the original phrase closes the new one.
    const tail = (tokens[i + hit.n - 1].match(/[^\w]+$/) || [''])[0];
    out.push(hit.term + tail);
    i += hit.n;
  }
  return out.join(' ');
}

// ──────────────────────────────────────────────
// Description
//
// Everything the matchers did not consume. Built by blanking their spans
// out of the original transcript rather than by re-joining tokens, so the
// assigner's own wording and capitalisation survive intact.
// ──────────────────────────────────────────────
function vdBuildDesc(transcript, spans) {
  const keep = transcript.split('');
  for (const [start, end] of spans) {
    for (let i = Math.max(0, start); i < Math.min(keep.length, end); i++) keep[i] = ' ';
  }
  let out = keep.join('')
    .split(',')
    .filter(seg => seg.split(/\s+/).filter(Boolean)
      .some(w => !VD_SEGMENT_FILLER.has(w.toLowerCase().replace(/[.\-–—]/g, ''))))
    .join(', ')
    .replace(/\s+/g, ' ')
    .trim();

  // Lead-ins come off the front only, longest first so "assign karo" is
  // not left as "karo" by an earlier match on "assign".
  const leadIns = VD_LEAD_INS.slice().sort((a, b) => b.length - a.length);
  let changed = true;
  while (changed) {
    changed = false;
    out = out.replace(/^[\s,.\-–—]+/, '');
    for (const lead of leadIns) {
      if (out.toLowerCase().startsWith(lead + ' ') || out.toLowerCase() === lead) {
        out = out.slice(lead.length).trim();
        changed = true;
        break;
      }
    }
  }

  // Dangling connectors at either end, repeatedly — "ka GST return ko"
  // sheds both sides.
  changed = true;
  while (changed) {
    changed = false;
    out = out.replace(/^[\s,.\-–—]+|[\s,.\-–—]+$/g, '');
    const words = out.split(/\s+/).filter(Boolean);
    if (words.length > 1 && VD_EDGE_FILLER.has(words[0].toLowerCase())) {
      words.shift(); out = words.join(' '); changed = true; continue;
    }
    if (words.length > 1 && VD_EDGE_FILLER.has(words[words.length - 1].toLowerCase())) {
      words.pop(); out = words.join(' '); changed = true;
    }
  }

  out = out.replace(/\s+([,.])/g, '$1').trim();
  out = vdFixVocab(out);
  return out ? out.charAt(0).toUpperCase() + out.slice(1) : '';
}

// ──────────────────────────────────────────────
// Entry point
//
// opts: { doers: [{id,name}], clients: [{id,name}], today: 'yyyy-mm-dd' }
// `today` is injectable so the date rules can be tested against a fixed
// day instead of whenever the suite happens to run.
// ──────────────────────────────────────────────
function parseDelegationSpeech(transcript, opts) {
  const o = opts || {};
  const doers = o.doers || [];
  const clients = o.clients || [];
  const today = o.today ? new Date(o.today + 'T00:00:00') : new Date();
  const text = String(transcript || '').trim();

  const result = {
    transcript: text,
    doer: null, doerAmbiguous: null,
    client: null, clientAmbiguous: null,
    approver: null,
    date: null, datePhrase: null,
    priority: null, approval: null,
    doerHeard: null, clientHeard: null, dateHeard: null,
    mode: 'freeform',
    desc: '', filled: [], missing: []
  };
  if (!text) { result.missing = ['doer', 'client', 'date', 'description']; return result; }

  const lower = text.toLowerCase();

  // A sentence that labels its own fields is read by those labels.
  const slots = vdParseSlots(lower, text);
  if (slots) return vdFromSlots(slots, result, { doers, clients, today, lower });

  const spans = [];

  const date = vdParseDate(lower, today);
  if (date) { result.date = date.date; result.datePhrase = date.phrase; spans.push(date.span); }

  for (const [level, pattern] of VD_PRIORITY_RULES) {
    const hit = vdFirstMatch(lower, '\\b' + pattern + '(?:\\s+priority)?\\b');
    if (!hit) continue;
    result.priority = level;
    // "urgent hai", "zaroori hai" — the copula belongs to the priority, and
    // stranding it turns the description into "... hai isliye jaldi karo".
    const copula = /^\s*(?:hai|hain|he)\b/i.exec(lower.slice(hit.span[1]));
    spans.push([hit.span[0], hit.span[1] + (copula ? copula[0].length : 0)]);
    break;
  }

  // An explicit "no approval" has to be read before the generic approval
  // patterns, which it contains.
  const noApproval = vdFirstMatch(lower, '\\b(?:no\\s+approval|approval\\s+(?:nahi|ki\\s+zarurat\\s+nahi)|bina\\s+approval|without\\s+approval)\\b');
  // Naming an approver is itself a request for approval — "approval from
  // Ashish" carries no other keyword, so without these the approver would
  // be found and then dropped for want of an approval flag to hang it on.
  const yesApproval = noApproval ? null : vdFirstMatch(lower, '\\b(?:approval\\s+(?:chahiye|required|lena|le\\s+lena|zaroori|from|by|se)|needs?\\s+approval|with\\s+approval|approval\\s+ke\\s+saath|approve\\s+karwana|se\\s+approval)\\b');
  // "Ashish se approval chahiye" matches on "se approval" first, because
  // alternation takes the leftmost position rather than the longest phrase.
  // Swallowing the trailing verb keeps "chahiye" out of the description.
  const approvalHit = noApproval || yesApproval;
  if (approvalHit) {
    result.approval = noApproval ? 'no' : 'yes';
    const tail = /^\s*(?:chahiye|chahiya|lena\s+hai|le\s+lena|required|zaroori|hai)\b/i.exec(lower.slice(approvalHit.span[1]));
    spans.push([approvalHit.span[0], approvalHit.span[1] + (tail ? tail[0].length : 0)]);
  }

  // An approver is only read out of an explicit construction — "Ashish se
  // approval", "approval from Ashish". Merely standing near the word is not
  // enough: "approval chahiye, Shailu ko do" puts the doer within three
  // words of it, and treating proximity as evidence claimed Shailu as the
  // approver and then left the doer empty.
  if (result.approval === 'yes') {
    const named = /(?:approval|approve[ds]?)\s*(?:from|by|se)\s+([^,.;]{2,40})/i.exec(lower)
      || /([^,.;]{2,40}?)\s+se\s+approval/i.exec(lower);
    if (named) {
      const approver = vdMatchPerson(named[1], doers);
      if (approver && !approver.ambiguous) {
        result.approver = { id: approver.id, name: approver.name };
        // Spans from the fragment are relative to it, not to the transcript.
        const at = lower.indexOf(named[1]);
        if (at !== -1) spans.push([at + approver.span[0], at + approver.span[1]]);
      }
    }
  }

  const doer = vdMatchPerson(lower, doers, result.approver ? result.approver.id : null);
  if (doer && doer.ambiguous) result.doerAmbiguous = doer.ambiguous;
  else if (doer) { result.doer = { id: doer.id, name: doer.name }; spans.push(doer.span); }

  const client = vdMatchClient(lower, clients);
  if (client && client.ambiguous) result.clientAmbiguous = client.ambiguous;
  else if (client) { result.client = { id: client.id, name: client.name }; spans.push(client.span); }

  result.desc = vdBuildDesc(text, spans);

  return vdTally(result);
}

function vdTally(result) {
  if (result.doer) result.filled.push('doer'); else result.missing.push('doer');
  if (result.client) result.filled.push('client'); else result.missing.push('client');
  if (result.date) result.filled.push('date'); else result.missing.push('date');
  if (result.priority) result.filled.push('priority');
  if (result.desc) result.filled.push('description'); else result.missing.push('description');
  return result;
}

// Slot-scoped reading. Each field is matched against its own slot, and a
// field whose slot the speaker skipped falls back to the whole sentence, so
// a template naming only the doer and date still picks up a priority
// mentioned in passing. The description is the deliberate exception: with no
// task slot it stays empty rather than swallowing the frame, which is the
// entire reason this mode exists.
function vdFromSlots(slots, result, ctx) {
  result.mode = 'template';
  const lat = slots.lat;
  const raw = slots.raw;
  const scope = key => (lat[key] ? lat[key] : ctx.lower);

  const doer = vdMatchPerson(scope('doer'), ctx.doers);
  if (doer && doer.ambiguous) result.doerAmbiguous = doer.ambiguous;
  else if (doer) result.doer = { id: doer.id, name: doer.name };
  if (raw.doer) result.doerHeard = raw.doer;

  // Relaxed only when the speaker labelled the slot as a client.
  const client = vdMatchClient(scope('client'), ctx.clients, !!lat.client);
  if (client && client.ambiguous) result.clientAmbiguous = client.ambiguous;
  else if (client) result.client = { id: client.id, name: client.name };
  if (raw.client) result.clientHeard = raw.client;

  const date = vdParseDate(scope('date'), ctx.today);
  if (date) { result.date = date.date; result.datePhrase = date.phrase; }
  if (raw.date) result.dateHeard = raw.date;

  for (const [level, pattern] of VD_PRIORITY_RULES) {
    if (!new RegExp('\\b' + pattern + '\\b', 'i').test(scope('priority'))) continue;
    result.priority = level;
    break;
  }

  if (lat.approval) {
    result.approval = /\b(?:no|nahi|nahin|bina|without|not)\b/i.test(lat.approval) ? 'no' : 'yes';
    if (result.approval === 'yes') {
      const appr = vdMatchPerson(lat.approval, ctx.doers, result.doer ? result.doer.id : null);
      if (appr && !appr.ambiguous) result.approver = { id: appr.id, name: appr.name };
    }
  }

  result.desc = raw.desc ? vdBuildDesc(raw.desc, []) : '';
  return vdTally(result);
}

// ══════════════════════════════════════════════════════
// RECOGNITION — browser speech in, parsed fields out
// ══════════════════════════════════════════════════════

// Chrome closes a recognition session on its own after a pause. Restarting
// it keeps a long dictation going, but an unbounded restart loop against a
// failing microphone would hold the device open indefinitely, so both the
// restarts and the whole session are capped.
const VD_MAX_RESTARTS = 20;
const VD_MAX_MS = 90000;

let _vdRec = null;
let _vdActive = false;
let _vdFinal = '';
// Indian-accented English. Fixed rather than selectable: doer and client
// names are stored in Latin script, and a Hindi locale returns Devanagari,
// which fills a description fine but leaves both dropdowns unmatchable.
const VD_LANG = 'en-IN';
// Edge's Chromium build routes recognition to Azure rather than Google and
// rejects locales Chrome accepts, en-IN among them. Falling back to US
// English transcribes an Indian accent less well, but a worse transcript
// beats a dead button — and a wrong name still has to survive the doer
// dropdown before it becomes anybody's task.
const VD_LANG_FALLBACK = 'en-US';
let _vdLangUsed = VD_LANG;
let _vdRestarts = 0;
let _vdGuard = null;
let _vdCap = null;

// Works in Chrome, in Safari 14.1+ / iOS 14.5+, and — measured on this
// team's own Windows 11 machine, against caniuse and an open MDN compat
// issue that both say otherwise — in Edge, which transcribed cleanly end to
// end on en-US. Trust the measurement over the tables here; Edge routes to
// Azure rather than Google, so third-party data about Chromium does not
// describe it.
//
// Firefox ships it disabled behind dom.webspeech.recognition.enable. Brave
// declines to ship it at all, because Chromium's version streams audio to
// Google. That is the real constraint and it is not ours to lift: the API
// belongs to the browser, so reaching those two means recording locally and
// transcribing server-side — a different feature, not a fix.
//
// ⚠️ A non-null return still only proves the constructor exists. The locale
// is the part that varies by browser, which is what VD_LANG_FALLBACK and the
// language-not-supported branch in vdOnError exist for.
function vdSpeechCtor() {
  return window.SpeechRecognition || window.webkitSpeechRecognition || null;
}

function vdEl(id) { return document.getElementById(id); }

function vdEsc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Read the lists straight off the dropdowns openDelegate has already filled,
// rather than fetching them again. The parser then cannot match a doer or a
// client that this form has no option to select.
function vdOptionsOf(id) {
  const sel = vdEl(id);
  if (!sel) return [];
  return Array.from(sel.options).filter(o => o.value)
    .map(o => ({ id: o.value, name: o.textContent.trim() }));
}

function vdPaintButton(on) {
  const btn = vdEl('vdMicBtn');
  const label = vdEl('vdMicLabel');
  if (btn) btn.classList.toggle('recording', on);
  if (label) label.textContent = on ? 'Stop & Fill' : 'Speak Task';
}

function vdShowError(msg) {
  const box = vdEl('vdError');
  if (!box) return;
  box.textContent = msg;
  box.style.display = msg ? 'block' : 'none';
}

function vdRenderTranscript(interim) {
  const t = vdEl('vdTranscript');
  if (!t) return;
  t.classList.toggle('listening', _vdActive);
  t.innerHTML = vdEsc(_vdFinal) +
    (interim ? ' <span class="vd-interim">' + vdEsc(interim) + '</span>' : '');
}

// Called by openDelegate so a previous dictation never bleeds into the next
// task, and so an unsupported browser disables the button up front instead
// of failing on the click.
function vdResetVoice() {
  vdStopRecording(true);
  _vdFinal = '';
  const panel = vdEl('vdPanel'); if (panel) panel.classList.remove('open');
  const t = vdEl('vdTranscript'); if (t) { t.innerHTML = ''; t.classList.remove('listening'); }
  const s = vdEl('vdSummary'); if (s) s.innerHTML = '';
  vdShowError('');
  ['dDoer', 'dClient', 'dDate', 'dPriority', 'dDesc', 'dApproval', 'dApprover']
    .forEach(id => { const el = vdEl(id); if (el) el.classList.remove('vd-filled'); });
  const btn = vdEl('vdMicBtn');
  if (btn && !vdSpeechCtor()) {
    btn.disabled = true;
    const hint = vdEl('vdHint');
    if (hint) hint.textContent = 'This browser has no voice input. Use Chrome, Edge or Safari, or fill the form below.';
  }
}

function vdToggleRecording() {
  if (_vdActive) vdStopRecording(); else vdStartRecording();
}

function vdStartRecording() {
  const Ctor = vdSpeechCtor();
  if (!Ctor) { vdShowError('This browser has no voice input. Use Chrome, Edge or Safari instead.'); return; }

  const panel = vdEl('vdPanel'); if (panel) panel.classList.add('open');
  const summary = vdEl('vdSummary'); if (summary) summary.innerHTML = '';
  vdShowError('');
  _vdFinal = '';
  _vdRestarts = 0;
  vdRenderTranscript('');

  const rec = new Ctor();
  rec.lang = _vdLangUsed;
  rec.continuous = true;
  rec.interimResults = true;
  rec.onresult = vdOnResult;
  rec.onerror = vdOnError;
  rec.onend = vdOnEnd;
  _vdRec = rec;
  _vdActive = true;
  vdPaintButton(true);
  vdRenderTranscript('');

  try {
    rec.start();
  } catch (e) {
    _vdActive = false;
    _vdRec = null;
    vdPaintButton(false);
    vdShowError('Could not start the microphone. Close any other tab using it and try again.');
    return;
  }

  // Closing the modal mid-sentence must not leave the microphone live. Every
  // way out of this form — the Close button, the overlay, a successful
  // Assign — just drops the .open class, so watching for that covers them
  // all without threading a stop call through each one.
  _vdGuard = setInterval(() => {
    const m = vdEl('delegateModal');
    if (!m || !m.classList.contains('open')) vdStopRecording(true);
  }, 500);
  _vdCap = setTimeout(() => { if (_vdActive) vdStopRecording(); }, VD_MAX_MS);
}

function vdStopRecording(skipFill) {
  if (_vdGuard) { clearInterval(_vdGuard); _vdGuard = null; }
  if (_vdCap) { clearTimeout(_vdCap); _vdCap = null; }
  if (!_vdActive) return;
  _vdActive = false;
  vdPaintButton(false);

  const rec = _vdRec;
  _vdRec = null;
  if (rec) { try { rec.stop(); } catch (e) { /* already closed */ } }

  const t = vdEl('vdTranscript'); if (t) t.classList.remove('listening');
  if (skipFill) return;
  const said = _vdFinal.trim();
  if (said) vdApplyTranscript(said);
  else vdShowError('Nothing was picked up. Check the microphone and try again.');
}

function vdOnResult(ev) {
  let interim = '';
  for (let i = ev.resultIndex; i < ev.results.length; i++) {
    const chunk = ev.results[i][0].transcript;
    if (ev.results[i].isFinal) _vdFinal += (_vdFinal ? ' ' : '') + chunk.trim();
    else interim += chunk;
  }
  vdRenderTranscript(interim);
}

function vdOnEnd() {
  if (!_vdActive) return;
  if (_vdRestarts >= VD_MAX_RESTARTS) { vdStopRecording(); return; }
  _vdRestarts++;
  try { _vdRec.start(); } catch (e) { vdStopRecording(); }
}

function vdOnError(ev) {
  // A pause in speech is reported as an error but leaves the session usable,
  // and an abort is what stopping deliberately looks like. Neither should
  // tear the recording down — onEnd restarts it.
  if (ev.error === 'no-speech' || ev.error === 'aborted') return;

  // Edge rejects en-IN outright. Retrying once on US English turns a dead
  // button into a working one; the switch is announced rather than silent,
  // because the transcript really is worse afterwards. Guarded by the
  // locale check so this can fire at most once per page.
  if (ev.error === 'language-not-supported' && _vdLangUsed !== VD_LANG_FALLBACK) {
    _vdLangUsed = VD_LANG_FALLBACK;
    vdStopRecording(true);
    vdShowError('Indian English is not available in this browser — switched to US English. Press Speak Task again.');
    return;
  }

  const msgs = {
    'not-allowed': 'Microphone access is blocked. Allow it for this site in your browser settings, then try again.',
    'service-not-allowed': 'Microphone access is blocked by your browser or system settings.',
    'audio-capture': 'No microphone found. Check that one is connected and selected as the input device.',
    'network': 'Speech recognition could not reach the network. Check your connection and try again.'
  };
  vdShowError(msgs[ev.error] || ('Voice input failed (' + ev.error + '). Please type the task instead.'));
  vdStopRecording(true);
}

// ══════════════════════════════════════════════════════
// FORM FILL
//
// Fills only what the parser was sure of and reports the rest as chips.
// Nothing is submitted: the assigner reads the form back and presses
// Assign, which is the check that keeps a misheard word from quietly
// becoming somebody's task.
// ══════════════════════════════════════════════════════
function vdMarkFilled(id, value) {
  const el = vdEl(id);
  if (!el) return;
  el.value = value;
  // Every <select> here is wrapped by the searchable-select enhancer, and the
  // visible control is its .ss-input, not the native box. Assigning .value
  // fires no change event, so without this the dropdown still reads "Select
  // Doer" while carrying the right id underneath — the form would submit the
  // correct task off a screen that looks like voice did nothing. See Section
  // 9 of brain.md.
  if (typeof el._ssSync === 'function') el._ssSync();
  el.classList.remove('vd-filled');
  void el.offsetWidth;   // reflow, so a second dictation flashes again
  el.classList.add('vd-filled');
}

function vdApplyTranscript(text) {
  const parsed = parseDelegationSpeech(text, {
    doers: vdOptionsOf('dDoer'),
    clients: vdOptionsOf('dClient'),
    today: new Date().toISOString().split('T')[0]
  });

  const chips = [];

  if (parsed.doer) {
    vdMarkFilled('dDoer', String(parsed.doer.id));
    chips.push(['ok', 'Doer: ' + parsed.doer.name]);
  } else if (parsed.doerAmbiguous) {
    chips.push(['warn', 'More than one person matched — pick the doer yourself']);
  } else {
    // Naming what was heard turns "it did not work" into something the
    // assigner can act on — usually a mis-heard name they can just reread.
    chips.push(['miss', parsed.doerHeard ? 'No doer named ' + parsed.doerHeard : 'Doer not caught']);
  }

  if (parsed.client) {
    vdMarkFilled('dClient', String(parsed.client.id));
    chips.push(['ok', 'Client: ' + parsed.client.name]);
  } else if (parsed.clientAmbiguous) {
    chips.push(['warn', 'More than one client matched — pick one yourself']);
  } else {
    chips.push(['miss', parsed.clientHeard ? 'No client named ' + parsed.clientHeard : 'Client not caught']);
  }

  if (parsed.date) {
    const dateEl = vdEl('dDate');
    const doerSets = vdEl('dDoerSetsDate');
    // A spoken date and "doer-defined due date" contradict each other, and
    // the tickbox disables the field the date would go into.
    if (doerSets && doerSets.checked) {
      doerSets.checked = false;
      if (typeof onDoerSetsDateChange === 'function') onDoerSetsDateChange();
    }
    if (dateEl && dateEl.min && parsed.date < dateEl.min) {
      chips.push(['warn', 'That date has already passed — set the due date yourself']);
    } else {
      vdMarkFilled('dDate', parsed.date);
      chips.push(['ok', 'Due: ' + (typeof fmtDate === 'function' ? fmtDate(parsed.date) : parsed.date)]);
    }
  } else {
    chips.push(['miss', 'Due date not caught']);
  }

  if (parsed.priority) {
    vdMarkFilled('dPriority', parsed.priority);
    chips.push(['ok', 'Priority: ' + parsed.priority.charAt(0).toUpperCase() + parsed.priority.slice(1)]);
  }

  if (parsed.approval) {
    vdMarkFilled('dApproval', parsed.approval);
    if (typeof onDelegateApprovalChange === 'function') onDelegateApprovalChange();
    if (parsed.approval === 'yes' && parsed.approver) {
      vdMarkFilled('dApprover', String(parsed.approver.id));
      if (typeof onDelegateApproverChange === 'function') onDelegateApproverChange();
      chips.push(['ok', 'Approval: ' + parsed.approver.name]);
    } else if (parsed.approval === 'yes') {
      chips.push(['miss', 'Approval needed — pick the approver']);
    }
  }

  if (parsed.desc) {
    vdMarkFilled('dDesc', parsed.desc);
    chips.push(['ok', 'Description filled']);
  } else {
    chips.push(['miss', 'Description not caught']);
  }

  const summary = vdEl('vdSummary');
  if (summary) {
    summary.innerHTML = chips.map(c =>
      '<span class="vd-chip vd-chip-' + c[0] + '">' + vdEsc(c[1]) + '</span>').join('');
  }
  vdShowError(parsed.missing.length
    ? 'Check the highlighted fields before assigning.'
    : '');
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { parseDelegationSpeech, vdParseDate, vdMatchPerson, vdMatchClient, vdBuildDesc };
}
