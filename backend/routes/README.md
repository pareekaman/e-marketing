# backend/routes — how a route module works here

Every file in this folder exports one function that registers a group of routes.
`server.js` calls it and hands over the things it needs. Nothing in here reaches
back into `server.js` on its own.

```js
require('./backend/routes/inventory')(app, { db, requireAuth, /* … */ });
```

That call sits in `server.js` at exactly the point the routes used to occupy, so
every binding passed in is in scope at the same moment it always was.

---

## The template

```js
// ══════════════════════════════════════════════════════
// <FEATURE> — one line on what this group is for
// ══════════════════════════════════════════════════════
// Dependencies are passed in rather than re-required: these must be the SAME
// instances server.js uses, not fresh copies.
module.exports = function registerThingRoutes(app, deps) {
  const {
    db,
    requireAuth,
    // …whatever else this group actually uses
  } = deps;

  app.get('/api/things', requireAuth, async (req, res) => {
    try {
      const [rows] = await db.query('SELECT * FROM things WHERE owner_id = ?', [req.session.userId]);
      res.json(rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

};
```

Then, in `server.js`, next to the other `require('./backend/routes/…')` lines:

```js
require('./backend/routes/things')(app, { db, requireAuth });
```

---

## Why dependencies are injected instead of required

Not style — these have to be the *same objects*:

- **`db`** is not the raw mysql2 pool. It is a wrapper that retries on
  `max_user_connections`, which shared hosting throws regularly. A fresh
  `require('mysql2')` gives you a second pool with no retry and one more
  connection against a cap of about 5–10.
- **Helpers with state or side effects** — `sendMeetingNotification` is also
  driven by the pre-meeting reminder cron; `canSeeTask` is called by the
  task-comments routes that stayed in `server.js`. Two copies means two
  behaviours.

If your module needs something that lives in `server.js`, add it to the `deps`
object at the call site and destructure it at the top. Do not `require` it.

---

## Two traps that bite when code moves into this folder

Both are real; both shipped and were caught by running the app, not by reading it.

### 1. `__dirname` is now `backend/routes`, not the repo root

```js
// WRONG in this folder — resolves to backend/routes/public/signature.png
path.join(__dirname, 'public', 'signature.png')

// Right — server.js passes its own __dirname in as appRoot
path.join(appRoot, 'public', 'signature.png')
```

This one is nasty because the offer-letter signature read sits inside a
`try/catch` that falls back to `''`. Nothing throws, nothing logs — offer
letters simply go out unsigned. See `hrm.js`.

### 2. Relative `require()` needs one more `../`

```js
require('./credentials.json')      // was fine in server.js
require('../../credentials.json')  // correct from backend/routes/
```

A dependency scanner will **not** catch this one. Scanners read identifiers;
`'./credentials.json'` is a string. Check relative requires by hand, or just
boot the app — it throws `Cannot find module` immediately.

---

## Before you move an existing group out of server.js

1. **Count the routes with every method.** `grep` for
   `app.get|post|put|delete` silently misses `app.patch`, which is how the
   payment-requests group was counted as 8 when it was 9.
2. **Check the group is contiguous.** Payment-requests had `/api/mdo-tasks`
   sitting in the middle of it, and the tasks group had two helpers in the
   middle that had to stay behind. Either lift the slices separately or leave
   the stragglers where they are.
3. **Check what the block defines that is used elsewhere.** `canSeeTask` is
   defined among the task routes but called by the task-comments routes far
   below; it stayed in `server.js` and is injected back in.
4. **Move the code by script, not by retyping.** Then prove it: pull the old
   block out of `HEAD`, normalise line endings, and compare. A pure move should
   be byte-identical.
5. **Route order only matters between routes that can match the same request.**
   Moving `/api/payment-requests/:id/wa-debug` ahead of `/api/mdo-tasks` changed
   nothing, because neither path can ever match the other.

---

## Testing

There is no test runner in this repo. What exists is a local setup that makes
real verification possible — use it, because several of today's bugs were
invisible to reading.

⚠️ **`.env` points at the production database.** Never boot `server.js` on it.

Run against a local MySQL instead:

```bash
DB_HOST=127.0.0.1 DB_NAME=emk_test DB_USER=… DB_PASSWORD=… \
WA_DISABLED=true PORT=3100 node server.js
```

The app builds its whole schema on an empty database, and seeds a default admin
(`aman@test.com` / `password`), so an empty database is enough to get a working
app. `WA_DISABLED=true` stops WhatsApp sends; with no mail credentials set,
email is disabled too.

Then drive the routes over HTTP and assert on real responses — status codes,
returned rows, and the guards. For frontend work, a headless-Chrome pass that
logs in and opens every page catches what a syntax check cannot.

If a migration ever needs to re-run against your test database:

```sql
DELETE FROM app_settings WHERE key_name='schema_deploy_marker';
```
