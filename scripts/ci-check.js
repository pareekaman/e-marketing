// Checks run on every push and pull request (.github/workflows/ci.yml) and
// locally with `node scripts/ci-check.js`. Nothing here starts the server or
// touches a database; it only reads the files in the repository.
//
//   1. every tracked .js file parses (node --check)
//   2. vercel.json and package.json are valid JSON
//   3. every cron path in vercel.json has a route in the code
//   4. every /js/... script that public/app.html loads exists
//
// Exits 1 and lists the failures when anything is wrong.
const { execFileSync, spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
const failures = [];
const fail = (what, detail) => failures.push(`${what}\n    ${String(detail).trim().split('\n').slice(0, 6).join('\n    ')}`);

// 1. Syntax of every tracked JavaScript file
const jsFiles = execFileSync('git', ['ls-files', '*.js'], { cwd: ROOT, encoding: 'utf8' })
  .split('\n').map(s => s.trim()).filter(Boolean);
for (const f of jsFiles) {
  const r = spawnSync(process.execPath, ['--check', f], { cwd: ROOT, encoding: 'utf8' });
  if (r.status !== 0) fail(`Syntax error in ${f}`, r.stderr || r.stdout);
}
console.log(`syntax: ${jsFiles.length} files checked`);

// 2. JSON config files
const json = {};
for (const f of ['vercel.json', 'package.json']) {
  try { json[f] = JSON.parse(read(f)); } catch (e) { fail(`${f} is not valid JSON`, e.message); }
}

// 3. Each Vercel cron must point at a route that exists, or it fails silently every day
if (json['vercel.json']) {
  const code = ['server.js', ...jsFiles.filter(f => f.startsWith('backend/'))].map(read).join('\n');
  const crons = json['vercel.json'].crons || [];
  for (const c of crons) {
    const p = String(c.path || '').split('?')[0];
    if (!code.includes(`'${p}'`) && !code.includes(`"${p}"`) && !code.includes(`\`${p}\``)) {
      fail(`vercel.json cron has no route: ${p}`, `schedule ${c.schedule}`);
    }
  }
  console.log(`crons: ${crons.length} checked`);
}

// 4. Scripts the app page loads
const html = read('public/app.html');
const srcs = [...html.matchAll(/<script[^>]+src="(\/js\/[^"?#]+)/g)].map(m => m[1]);
for (const s of srcs) {
  if (!fs.existsSync(path.join(ROOT, 'public', s))) fail(`public/app.html loads a missing file: ${s}`, 'no such file in public/');
}
console.log(`app.html scripts: ${srcs.length} checked`);

if (failures.length) {
  console.error(`\n${failures.length} problem(s):\n\n- ` + failures.join('\n\n- '));
  process.exit(1);
}
console.log('\nAll checks passed.');
