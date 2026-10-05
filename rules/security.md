# Security Controls — E-Marketing Task Manager

An audit of the security controls the code implements, and of the gaps it
shows. It covers the same snapshot as [business.md](business.md): `TM_branch`
at `e9073b8` plus the working-tree changes present on 2026-10-05
(see business.md for the exact snapshot). The tags
(**Implemented / Missing / Inferred / Unknown**) mean the same as there. Who
may do what is covered in [permissions.md](permissions.md); this file covers
*how* access is protected.

> **No secret values appear in this document.** Where the code or a file holds
> a credential, only its location is given.

---

## 1. Authentication

| # | Control | Status | Evidence |
|---|---|---|---|
| AU-1 | Login checks a bcrypt hash (`bcrypt.compareSync`). Passwords are stored as bcrypt with cost 10. | Implemented | `server.js:1776-1777`, `3375` |
| AU-2 | The session is a **stateless JWT** `{userId, role, name}` signed with `SESSION_SECRET` and valid for **7 days**. | Implemented | `server.js:1783-1787` |
| AU-3 | It is sent in the cookie `token` with `httpOnly`, `sameSite: 'lax'` and a 7-day `maxAge`. `secure` is set **only when `NODE_ENV === 'production'`**. | Implemented | `server.js:1788-1794`, `2006-2013` |
| AU-4 | `/api/login` **also returns the JWT in the JSON body**. The current login page ignores it and deletes any old `localStorage` copy. | Implemented | `server.js:1795`; `public/index.html:1000-1004`, `public/js/core.js:285-292` |
| AU-5 | `requireAuth` also accepts `Authorization: Bearer <token>`. | Implemented | `server.js:1047` |
| AU-6 | The **role is re-read from the database** on each request (30 s cache, cleared when the role changes). A deleted user's token is rejected. | Implemented | `server.js:1018-1060`, `3414`, `3501`, `3677` |
| AU-7 | The `name` in the session still comes from the token, so it is stale until the next login. | Implemented | `server.js:1061-1066` |
| AU-8 | If the database role lookup errors, the request uses the role **inside the token** (fail-open by design). | Implemented | `server.js:1030-1037` |
| AU-9 | **Logout only clears the cookie.** Nothing revokes a token on the server, so a copied token works until it expires (7 days; 1 day when impersonating). | Implemented (gap) | `server.js:1799-1802` |
| AU-10 | **Login throttle per email:** 10 failures within 15 min lock that email for 15 min, even for the correct password. Emails that do not exist are counted too. State lives in the `login_attempts` table. Any database error **fails open**. | Implemented | `server.js:1700-1761`, `1767-1773` |
| AU-11 | **Per-IP limits** on the unauthenticated auth routes: login 30 failed (401) attempts per 15 min, forgot-password 10 per 15 min, reset 20 failed attempts per 15 min. They are kept in memory per server instance and keyed on the first `X-Forwarded-For` value. | Implemented | `server.js:60-90` |
| AU-12 | Whether the `X-Forwarded-For` value can be spoofed past Vercel's proxy cannot be told from the repository. | Unknown | `server.js:71` |
| AU-13 | The login query uses the email exactly as typed (`WHERE email = ?`), while the throttle lowercases it. Matching then depends on the database collation. | Inferred | `server.js:1774`, `1715` |
| AU-14 | **Forgot / reset password** (staff only, never client logins): a 6-digit code from `crypto.randomInt`, stored as a bcrypt hash, valid 10 min, single use, at most 5 wrong tries. The forgot response is the same whether or not the email exists. | Implemented | `server.js:1803-1897` |
| AU-15 | Changing your login email needs the current password, because the reset code is sent to that email. | Implemented | `server.js:3700-3708` |
| AU-16 | Minimum password length (6) is enforced **only** on reset and the client-portal password change. Admin-created passwords, bulk-imported passwords and profile password changes have **no length rule**. | Implemented / Missing (inconsistent) | `server.js:1872`, `7296` versus `3362-3375`, `3507-3525`, `3709-3712` |
| AU-17 | An admin can set any user's password without knowing the old one (`PUT /api/users/:id`). | Implemented | `server.js:3410-3411` |
| AU-18 | **A new user's password is emailed to them in plain text.** | Implemented | `server.js:3376-3379` |
| AU-19 | If the `users` table is empty at startup, a **default admin with a fixed password** is created (the email and password are literals in `server.js`). `/api/setup` repeats this. `README.md` documents an older default login. | Implemented | `server.js:879-892`, `1660-1675`; `README.md:11`, `205-212` |
| AU-20 | Multi-factor authentication. | Missing (none in the code) | grep: no MFA/TOTP code |

### 1.1 Impersonation

- Only an admin can start it. The impersonation token carries
  `impersonatedBy`, lasts 1 day, and cannot target a client login.
  **Implemented**: `server.js:2015-2043`.
- No audit trail of impersonation was found. **Missing**: other privileged
  changes are logged (`task_activity`, `deleted_records`), but
  `server.js:2015-2062` records nothing.

---

## 2. Unauthenticated surface

Every route not listed here requires `requireAuth`. This was checked by
scanning all 282 route definitions in `server.js` and `backend/routes/*.js`.

| Route | Protection | Status | Evidence |
|---|---|---|---|
| `POST /api/login`, `/api/forgot-password`, `/api/reset-password` | IP limits + per-email throttle (AU-10, AU-11) | Implemented | `server.js:85-90`, `1763-1897` |
| `POST /api/logout` | none needed | Implemented | `server.js:1799` |
| `GET /api/setup` | `?secret=` must equal `SETUP_SECRET`; closed if unset. The secret travels in the **query string**, so it can end up in logs. | Implemented / Inferred (log exposure) | `server.js:1505-1507` |
| `GET /api/cron/*` (14 routes) | `Authorization: Bearer <CRON_SECRET>`; **closed if `CRON_SECRET` is unset**. Plain string comparison. | Implemented | `server.js:5740`, `5767`, `6024`, `6042`, `6126`, `6231`, `6277`, `6627`, `6721`, `7016`, `7031`, `7046`, `7077`, `10234` |
| `POST /api/wa-bot/task` | `X-Bot-Key` or body `bot_key` checked against `BOT_API_KEY` with `timingSafeEqual`; closed if unset. A code comment says it is unset in production. | Implemented | `server.js:5136-5151` |
| `GET /api/wa-delegation/approve/:token`, `/deny/:token` | A 32-byte random token. **These GET requests change data** (approve or deny a task). They can only be decided once. | Implemented / Inferred (link prefetchers could trigger them) | `server.js:5195-5250`, `5157` |
| `GET /api/theme/public` | Returns only the theme name and Navratri day. | Implemented | `server.js:9653-9662` |
| `GET /offer/:token`, `/offer-pdf/:token`, `/offer-pdf-prelim/:token` | 24-byte random capability tokens. **No expiry was found.** | Implemented / Missing (expiry) | `backend/routes/hrm.js:255`, `713-790` |
| `POST /api/hrm/joining-form` | Shared secret (`x-hrm-form-secret` or body) **plus** a per-candidate token. The secret falls back to a **literal in source** when `HRM_JOINING_FORM_SECRET` is unset, and there is **no closed-if-unset guard**, unlike the cron, bot and setup routes. Plain string comparison. | Implemented / Missing (fail-closed) | `backend/routes/hrm.js:60`, `797-801`, `815-818` |
| `GET /`, `/app`, `/client`, static `public/` | Static pages; every data call they make is authenticated. | Implemented | `server.js:91`, `10252-10259` |

---

## 3. Authorization enforcement (summary)

The full matrix is in [permissions.md](permissions.md). The security-relevant
points:

| # | Finding | Status | Evidence |
|---|---|---|---|
| AZ-1 | Client logins are confined to an allow-list of API paths by regex, including routes added later. | Implemented | `server.js:1069-1077` |
| AZ-2 | Object-level checks exist on task side-data (comments, activity, sub-tasks). They were added to close earlier id-guessing gaps. | Implemented | `server.js:2195-2220`, `3734-3757` |
| AZ-3 | Department scoping takes the department from the caller's own `users` row, never from request input (dashboard, MIS, transfers, weekly plan). | Implemented | `server.js:2087-2091`, `2382-2393`, `3957-3964`, `4063-4069` |
| AZ-4 | **Objects readable by id without an ownership check:** `GET /api/meetings/:id` (includes attendee emails), the DMS folder file list for any client. | Implemented (gap) | `backend/routes/meetings.js:92-122`; `backend/routes/clients.js:895-899` |
| AZ-5 | `GET /api/users` returns every user's email, phone, birthday, joining date **and `user_permissions`** to any staff login. | Implemented | `server.js:3308-3360` |
| AZ-6 | Task creators can set `assigned_by` to another user (as the approver) by email or id. | Implemented | `backend/routes/tasks.js:253-265` |
| AZ-7 | Approval decisions, transfers, leave, payment requests and inventory claims use atomic `UPDATE … WHERE status='pending'` (or equivalent), so double decisions cannot happen. | Implemented | `server.js:2292-2296`, `3980-3983`, `9386-9392`; `payment-requests.js:327-330`, `351-355`; `inventory.js:203-205` |
| AZ-8 | Self-approval is blocked for leave (approve) and payment requests. It is **not** blocked for a task revise on a self-assigned task. | Implemented / Inferred | `server.js:9378-9380`; `payment-requests.js:345-347`; `tasks.js:676-680` |
| AZ-9 | Many routes check roles inline in the handler rather than through a central middleware, so each route has to be read on its own. | Implemented | e.g. `server.js:5258-5262`, `7954-7956`, `7090-7091` |

---

## 4. Input validation and injection

| # | Control | Status | Evidence |
|---|---|---|---|
| IV-1 | SQL uses `mysql2` placeholders (`?`, `??`). A pattern search found no request value placed directly into SQL text. Values that are interpolated (task type, status, table name) are first checked against fixed lists. | Implemented | `tasks.js:56-60`, `579-592`; `server.js:2125-2128`, `1139-1141` |
| IV-2 | `getTable()` returns `checklist_tasks` for any value other than `delegation`. Routes that do not validate `type` first (edit, delete, detail) would quietly act on the checklist table. | Inferred | `server.js:1139-1141`; `tasks.js:759-763`, `794-806`, `844-850` |
| IV-3 | Dates are checked against `^\d{4}-\d{2}-\d{2}$` in most routes; times against `HH:MM`. | Implemented | `tasks.js:779-784`, `server.js:2117`, `9162` |
| IV-4 | Fields written into Google Sheets are limited to the columns configured for the FMS step, and only to rows below the header. | Implemented | `fms.js:486-500` |
| IV-5 | Profile photos must match a strict `data:image/...;base64` pattern. | Implemented | `server.js:3690-3693` |
| IV-6 | Request bodies up to **10 MB** are accepted (JSON and urlencoded). | Implemented | `server.js:57-58` |
| IV-7 | Bulk inputs are capped (bulk checklist 800 dates; payment sentinel ids follow a strict format). | Implemented | `tasks.js:546`; `payment-requests.js:162-172` |

---

## 5. Output encoding and browser security

| # | Control | Status | Evidence |
|---|---|---|---|
| OE-1 | Escape helpers exist (`dtEscape`, `escapeHtml`, `esc`, plus module-specific `ntEsc` and `vdEsc`). Every frontend file that writes `innerHTML` also calls one of them. `chatbot.js` puts server text in with `textContent` and uses `innerHTML` only for fixed SVG. | Implemented | `public/js/daily.js:520`, `public/js/fms.js:242`, `public/js/access.js:6`, `public/client.html:232`, `public/app.html:4042`, `public/js/notify.js:58-73`, `public/js/chatbot.js:7-20` |
| OE-2 | Each individual `innerHTML` sink was **not** checked to confirm that every user-controlled value passes through an escape helper. | Unknown | per-file counts by grep over `public/` |
| OE-3 | Task `url` values are HTML-escaped but their **scheme is not checked**, so a `javascript:` URL would render as a clickable link. Client-portal and meeting links do force `http(s)`. | Inferred | `public/js/tasks.js:647`; `public/client.html:785`, `489`; `public/js/meetings.js:220` |
| OE-4 | **Offer letters put the candidate name, position and similar fields into HTML without escaping.** That HTML is served unauthenticated at `/offer/:token` on the app's own origin. | Inferred (stored-XSS risk) | `backend/routes/hrm.js:175-200`, `713-727` |
| OE-5 | Server-generated HTML pages (WhatsApp approve/deny) escape task text with `waHtmlText`. The daily-report confirmation email escapes its rows. | Implemented | `server.js:5080-5087`, `5218`, `5248`, `8335-8340` |
| OE-6 | `helmet` is on with **Content-Security-Policy disabled** and COEP disabled. The code says enabling CSP would first need the inline handlers rewritten. It keeps X-Frame-Options `SAMEORIGIN`, nosniff, HSTS, Referrer-Policy `no-referrer`, and hides `X-Powered-By`. | Implemented (CSP Missing by decision) | `server.js:20-54` |
| OE-7 | CDN scripts (Chart.js, jsPDF from cdnjs) load **without Subresource Integrity**. | Missing | `public/app.html:19-20`, `public/client.html:9` |
| OE-8 | All `/api` responses carry `Cache-Control: no-store`. | Implemented | `server.js:101-104` |
| OE-9 | **CSRF:** there is no CSRF token. The protection relies on `SameSite=Lax` cookies plus JSON request bodies. `express.urlencoded` is also enabled. | Inferred (mitigated by SameSite) | `server.js:57-58`, `1792` |
| OE-10 | **CORS:** the `cors` package is not used. The browser's same-origin defaults apply. | Implemented | `package.json`; grep finds no `cors` in `server.js` |

---

## 6. Secrets and configuration

| # | Finding | Status | Evidence |
|---|---|---|---|
| SC-1 | `.env`, `.env.local`, `credentials.json`, `brain.md` and `local/` are gitignored and not tracked. | Implemented | `.gitignore:6-12`, `36`, `57`; `git ls-files` |
| SC-2 | **`README.md` (tracked) contains the database host, user and password in plain text.** Whether they are still valid is unknown. | Implemented (exposure) / Unknown (validity) | `README.md:62-64` |
| SC-3 | `JWT_SECRET` is `SESSION_SECRET` or, if that is unset, a literal string. If the variable is missing, tokens are signed with a value published in the source, and the app still starts. | Implemented | `server.js:17` |
| SC-4 | `HRM_JOINING_FORM_SECRET` has a literal fallback and no unset guard (see §2). | Implemented | `backend/routes/hrm.js:60` |
| SC-5 | The **untracked** `employee-form/Code.gs` contains a hardcoded portal secret for the joining-form webhook. It is not in git yet, but it is not ignored either. | Implemented (exposure risk if committed) | `employee-form/Code.gs:14-15`; `git status` |
| SC-6 | The cron routes' literal fallback for `CRON_SECRET` is never used, because each route first refuses when the variable is unset. `BOT_API_KEY` and `WAUMFY_API_KEY` have no literal fallback. | Implemented | `server.js:5742-5745`, `5146-5150`, `4978-4979` |
| SC-7 | Hardcoded identifiers in source: WhatsApp group ids, a phone number, Google Sheet, Drive and Apps Script ids and URLs, and production user ids. | Implemented | `server.js:3388-3390`, `5344-5347`, `6752-6753`, `7832-7836`, `8929`, `8936`, `9792`; `backend/routes/hrm.js:39-47`; `backend/routes/leads.js:40-50`; `backend/routes/clients.js:50-51`; `public/js/mdo.js:1215-1217` |
| SC-8 | Staff names and birthdays are hardcoded in a one-time admin route and in a script. | Implemented | `server.js:3436-3483`; `scripts/migrate-birthdays.js` |
| SC-9 | Partly masked card numbers are seeded in source. | Implemented | `backend/routes/credit-cards.js:396-402` |
| SC-10 | `.env.example` contains only placeholders. A code comment notes it is out of date with what the app reads. | Implemented / Inferred | `.env.example`; key list checked with values redacted |
| SC-11 | Whether production sets `SESSION_SECRET`, `HRM_JOINING_FORM_SECRET`, `CRON_SECRET` and `NODE_ENV=production`. | Unknown | not answerable from the repository |

---

## 7. Data protection

| # | Finding | Status | Evidence |
|---|---|---|---|
| DP-1 | User passwords and reset codes are bcrypt hashes. | Implemented | `server.js:3375`, `1847` |
| DP-2 | **The client credentials vault stores passwords in plain text** ("stored as typed so they can be read back"). All four routes are admin-only, and the list returns the passwords. | Implemented | `server.js:7598-7720` |
| DP-3 | Deleted rows are kept as full JSON in `deleted_records` for 60 days. That includes **user password hashes and reset-code fields**, and **plaintext vault passwords** when a credential is deleted. Admins can read them through the Logs page. | Inferred | `server.js:1197-1232`, `3496-3497`, `7711-7712`, `10149-10160`, `10213` |
| DP-4 | **Credit card statements (images or text) are sent to the OpenAI API** for parsing. Only masked card numbers are stored. The original statement PDF is stored in the database. | Implemented | `backend/routes/credit-cards.js:118-125`, `747-822`, `782-790` |
| DP-5 | Candidate joining details (guardians, address, Aadhaar and PAN files through Google Drive) are collected by the Apps Script form and posted to the app. | Implemented | `employee-form/Code.gs:1-25`, `105-120`; `backend/routes/hrm.js:797-907` |
| DP-6 | Billing names are removed from API responses on the server for anyone without the grant. | Implemented | `backend/routes/clients.js:242-245` |
| DP-7 | For non-admins, 5xx error bodies are replaced with a generic message and the real error is logged. **Admins still receive raw database errors.** | Implemented | `server.js:106-125` |
| DP-8 | Routes outside `/api` do not get that masking. For example `/offer/:token` returns `err.message` in HTML. | Implemented | `backend/routes/hrm.js:723` |
| DP-9 | The database TLS option, when enabled, sets `rejectUnauthorized: false`, so the server certificate is not verified. | Implemented | `server.js:172` |

---

## 8. File uploads

| Upload | Limits | Type check | Evidence |
|---|---|---|---|
| Client invoices | 4 MB | `%PDF` magic bytes | `server.js:7192`, `7231-7235` |
| Credit card Excel / PDF | 10 MB / 25 MB | parsed by the XLSX / PDF libraries | `server.js:7727-7728` |
| DMS file | 50 MB (in memory) | none found | `server.js:7729` |
| DMS chunked upload | 6 MB per chunk (`express.raw`) | none found | `backend/routes/clients.js:996` |
| DMS external link | — | must be `http:` or `https:` | `server.js:1326-1328`, `clients.js:963` |
| Client logo | 1.5 MB data URL (per comment) | Unknown | `backend/routes/clients.js:271-277` |
| Profile photo | 10 MB body limit | data-URL pattern | `server.js:3690-3693` |
| Payment bill files | Uploaded **straight from the browser to a Google Apps Script URL**, bypassing the server | none on the server | `public/js/mdo.js:1215-1217` |

---

## 9. Audit trail and recovery

| # | Control | Status | Evidence |
|---|---|---|---|
| AT-1 | `task_activity` records task status, due-date, assignee and reason changes with the actor and source. | Implemented | `server.js:1170-1190` |
| AT-2 | Every hard delete in the app is archived to `deleted_records` first, and the delete is aborted if archiving fails. The one exception is the purge. | Implemented | `server.js:1186-1232` |
| AT-3 | Admins can restore archived rows from an allow-list of tables, with id-clash and already-restored checks. | Implemented | `server.js:10110-10208` |
| AT-4 | Archives are purged after 60 days (configurable) by cron or admin action. | Implemented | `server.js:10213-10250` |
| AT-5 | DMS actions are logged per user (`dms_file_activity`). | Implemented | `server.js:1333-1340` |
| AT-6 | No audit log was found for role or permission changes, user edits, impersonation, payment approvals (beyond the row status) or vault reads. | Missing (relative to AT-1 and AT-2) | `server.js:3402-3434`, `3642-3685`, `2015-2062` |

---

## 10. Availability and infrastructure

| # | Control | Status | Evidence |
|---|---|---|---|
| AV-1 | Each server instance uses one database connection by default (`DB_POOL_SIZE`). Connection-limit errors are retried 6 times (about 11 s), because preview and production deployments share the database. | Implemented | `server.js:150-200` |
| AV-2 | Every `/api` request waits for the startup migrations; a deploy marker skips them on warm deploys. | Implemented | `server.js:127-133`, `276-345`, `4925-4939` |
| AV-3 | Any `/api` hit can trigger the meeting-reminder send (at most once every 5 min). | Implemented | `server.js:137-145` |
| AV-4 | Vercel function `maxDuration` is 60 s. | Implemented | `vercel.json` |
| AV-5 | No automated tests exist (no test script or test directory). | Implemented | `package.json` |

---

## 11. Security contradictions and gaps (summary)

| # | Finding | Status | Evidence |
|---|---|---|---|
| SX-1 | The cron, bot and setup secrets **fail closed** when unset; the HRM joining-form secret **falls back to a literal in source**. | Implemented (inconsistent) | `server.js:5743`, `5147`, `1507` versus `backend/routes/hrm.js:60`, `801` |
| SX-2 | The bot key uses a constant-time comparison; the cron, setup and joining-form secrets use plain `!==`. | Implemented (inconsistent) | `server.js:5148-5149` versus `5743`, `1507`, `hrm.js:801` |
| SX-3 | The login page deliberately keeps no token in JavaScript, yet `/api/login` still returns it in the body. | Implemented (inconsistent) | `server.js:1795`; `public/index.html:1000-1004` |
| SX-4 | Password length is checked on two flows only (AU-16). | Missing (inconsistent) | see AU-16 |
| SX-5 | A comment in `GET /api/users` says permissions are hidden from non-admins, but `user_permissions` is still returned. | Implemented (contradiction) | `server.js:3318-3328`, `3337-3348` |
| SX-6 | Tracked documentation holds live-looking database credentials (SC-2), and an untracked script holds a production webhook secret (SC-5). | Implemented | `README.md:62-64`; `employee-form/Code.gs:15` |
| SX-7 | Plaintext vault passwords (DP-2), and they are also copied into the delete archive (DP-3). | Implemented / Inferred | `server.js:7598-7720`, `7711` |
| SX-8 | Offer-letter HTML is unescaped and public (OE-4); task URLs have no scheme check (OE-3). | Inferred | see OE-3, OE-4 |
| SX-9 | No token revocation, no MFA, no CSP, no SRI (AU-9, AU-20, OE-6, OE-7). | Missing | see the rows cited |
| SX-10 | Whether production environment variables are set safely (SC-11), and whether the README credentials are still valid (SC-2). | Unknown | — |
