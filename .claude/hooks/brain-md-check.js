// Stop hook: keeps brain.md honest against the real project files, using
// content hashes (not mtimes — mtimes are too easy to bump without any
// real content change, e.g. a touch, a git checkout, an editor autosave).
//
// State lives in .brain-md-sync-state.json (gitignored): the hash of
// brain.md itself plus the hashes of every tracked file, as of the last
// time brain.md's content actually changed. Two cases:
//   1. brain.md's own content differs from the stored baseline -> someone
//      (Claude or the user) just edited brain.md. Trust that edit,
//      re-snapshot all tracked-file hashes against it, done quietly.
//   2. brain.md is unchanged, but a tracked file's hash differs from the
//      stored baseline -> real source drift brain.md hasn't accounted for
//      yet. Block the stop once and ask the agent to reconcile it.
// This is source-agnostic: it doesn't care whether the tracked-file edit
// came from Claude Code, a manual edit in another editor, or git.
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const ROOT = path.join(__dirname, '..', '..');
const BRAIN_MD = path.join(ROOT, 'brain.md');
const STATE_FILE = path.join(__dirname, '..', '.brain-md-sync-state.json');

const TRACKED_FILES = [
  'server.js',
  'public/app.html',
  'public/client.html',
  'public/index.html',
  'vercel.json',
  'package.json',
  'migrate-birthdays.js',
  '.env.example',
];

const TRACKED_DIRS = ['.github/workflows'];

function hashOf(p) {
  try {
    const buf = fs.readFileSync(p);
    return crypto.createHash('sha256').update(buf).digest('hex');
  } catch (e) {
    return null;
  }
}

function collectDirFiles(dir) {
  let out = [];
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (e) {
    return out;
  }
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out = out.concat(collectDirFiles(full));
    else out.push(full);
  }
  return out;
}

function loadState() {
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch (e) {
    return null;
  }
}

function saveState(state) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  } catch (e) {
    // Non-fatal — worst case we re-check next time.
  }
}

const candidatePaths = TRACKED_FILES.map((f) => path.join(ROOT, f));
for (const d of TRACKED_DIRS) {
  candidatePaths.push(...collectDirFiles(path.join(ROOT, d)));
}

const currentTrackedHashes = {};
for (const full of candidatePaths) {
  const h = hashOf(full);
  if (h !== null) {
    currentTrackedHashes[path.relative(ROOT, full).replace(/\\/g, '/')] = h;
  }
}

const currentBrainMdHash = hashOf(BRAIN_MD);
const stored = loadState();

const brainMdChanged = !stored || stored.brainMdHash !== currentBrainMdHash;

if (brainMdChanged) {
  // brain.md itself moved (or this is the first run) — trust it, re-baseline.
  saveState({ brainMdHash: currentBrainMdHash, trackedFileHashes: currentTrackedHashes });
  process.exit(0);
}

const storedHashes = stored.trackedFileHashes || {};
const changed = [];
for (const [rel, h] of Object.entries(currentTrackedHashes)) {
  if (storedHashes[rel] !== h) changed.push(rel);
}
// Also flag tracked files that existed in the baseline but vanished.
for (const rel of Object.keys(storedHashes)) {
  if (!(rel in currentTrackedHashes)) changed.push(rel + ' (removed)');
}

if (changed.length === 0) {
  process.exit(0);
}

const missing = currentBrainMdHash === null;

process.stdout.write(JSON.stringify({
  decision: 'block',
  reason:
    (missing
      ? 'brain.md does not exist yet. '
      : 'These project files changed since brain.md was last updated: ' + changed.join(', ') + '. ') +
    'Per brain.md\'s own Section 17 (AI Agent Instructions), review whether brain.md ' +
    'at the repo root needs updating to reflect the current state of these files ' +
    '(schema, API routes, features, business rules, env vars, etc.), then update the ' +
    'relevant section(s) and save brain.md — even just bumping its "Last generated" line ' +
    'after confirming no real update is needed is enough, since this check is hash-based ' +
    'and only re-baselines on an actual byte change to brain.md.',
  continue: true,
}));
