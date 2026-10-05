# Business Rules — E-Marketing Task Manager

This file is an audit of the business rules the code actually enforces. It does
not describe intended behaviour that the code lacks, except where a finding is
marked **Missing**, and then it explains which part of the project implies the
rule should exist.

- **Snapshot analysed:** branch `TM_branch` at commit `e9073b8` (2026-10-04),
  plus the uncommitted working-tree changes present on 2026-10-05
  (`public/css/features.css`, `public/js/tasks.js` modified; `employee-form/`
  and `scripts/export-departments.js` untracked). While this audit was being
  written, commit `02e56b6` changed only `public/js/reports.js`. No line cited
  in these files is in `reports.js`, so the citations also hold at `02e56b6`.
- **Sources read:** `server.js`, every file in `backend/routes/`,
  `backend/offer-letter-pdf.js` (skimmed), `public/*.html`, `public/js/*.js`
  (role and permission logic, rendering helpers), `vercel.json`,
  `.github/workflows/*.yml`, `package.json`, `.env.example` (keys only),
  `.gitignore`, `README.md`, `scripts/`, and `employee-form/`.
- **Not used as evidence:** `brain.md` and `local/`. Both are gitignored
  personal notes, and parts of `brain.md` no longer match the code. For
  example, it says there is no forgot-password flow, but `server.js:1834`
  has one.

Companion files: [security.md](security.md) and [permissions.md](permissions.md).

## Status legend

| Tag | Meaning |
|---|---|
| **Implemented** | The code enforces it. The evidence column points at the enforcing line. |
| **Missing** | Absent, although the project's own code, comments or data model imply it should be there. The reason is given. |
| **Inferred** | Follows from how the code behaves, or from its comments, but no line states it as a rule. |
| **Unknown** | The repository alone cannot answer it (production data, environment values, external systems). |

---

## 1. System overview

A single Express app (`server.js`, ~10,300 lines) with route modules in
`backend/routes/`, a MySQL database, and a vanilla-JS frontend
(`public/app.html` for staff, `public/client.html` for client logins,
`public/index.html` for login). It is deployed on Vercel (`vercel.json`).

| Module | Main code |
|---|---|
| Delegation and checklist tasks, sub-tasks, activity | `backend/routes/tasks.js`, `server.js:2165-2230` |
| Approvals (revise / completion) | `server.js:2227-2380` |
| Task transfers | `server.js:3786-4045` |
| Weekly plan, Monday check-in, MIS, Race | `server.js:2380-3250`, `4045-4600` |
| Leave Tracker and Extra Working | `server.js:8878-9450` |
| Daily Task reports, Compliance, Employee 360 | `server.js:8231-8880` |
| Clients, CRM, portal, invoices, feedback, DMS, credentials vault | `backend/routes/clients.js`, `server.js:7100-8200` |
| Payment requests | `backend/routes/payment-requests.js`, `server.js:7783-7945` |
| Credit card statements | `backend/routes/credit-cards.js`, `server.js:7730-7782` |
| Inventory | `backend/routes/inventory.js` |
| Meetings / Scheduler, Day Plan | `backend/routes/meetings.js`, `server.js:9784-10035` |
| HR Portal (HRM) and offer letters | `backend/routes/hrm.js`, `backend/offer-letter-pdf.js` |
| FMS (Google-Sheet flow management) | `backend/routes/fms.js`, `server.js:2460-2570` |
| Leads Enquiry (Google Sheets only) | `backend/routes/leads.js` |
| Task Assistant chat (rule-based) | `backend/routes/chatbot.js` |
| WhatsApp / email notifications and cron jobs | `server.js:4975-7100`, `vercel.json`, `.github/workflows/` |
| Delete archive, restore, purge | `server.js:1197-1232`, `10105-10250` |
| Festival theme | `server.js:9590-9680` |

---

## 2. Calendar and time rules

| # | Rule | Status | Evidence |
|---|---|---|---|
| T-1 | "Today" means the IST calendar day. The code computes it as `Date.now() + 5.5h`, not from the server clock. | Implemented | `server.js:8295-8297`, `backend/routes/tasks.js:193-194`, `907`, `server.js:4155-4161` |
| T-2 | Company off-days are **every Sunday, the last Saturday of each month, and dates in the `holidays` table**. The same rule applies to everyone. | Implemented | `server.js:9466-9477`, `9493-9498` |
| T-3 | Per-user `week_off` / `extra_off` are **not** considered when deciding off-days, although both columns exist and can be set on each user. | Implemented (as stated in the code comment) | `server.js:9469` |
| T-4 | "Next working day" skips the off-days in T-2 and looks ahead at most 60 days; if it finds none, it returns the original date. | Implemented | `server.js:9578-9589` |
| T-5 | When pushing tasks off an employee's leave, the next working day also skips that employee's own approved full-day leave dates. | Implemented | `server.js:9565-9575`, `9539` |
| T-6 | Reminder and summary crons send nothing on Sundays, last Saturdays and holidays. | Implemented | `server.js:9500-9510` (`getTodayOffIST`) |

---

## 3. Delegation and checklist tasks

### 3.1 Two task kinds

- **Delegation** (`delegation_tasks`): one-off work with optional approval,
  sub-tasks, client asks, a due time, and statuses
  `pending | completed | revised`. **Implemented**:
  `backend/routes/tasks.js:597-601`.
- **Checklist** (`checklist_tasks`): one row per due date, used for recurring
  work. Statuses are only `pending | completed`. There is no approval, no
  revise, and no `completed_at`. **Implemented**:
  `backend/routes/tasks.js:597-601`, `713-724`.

### 3.2 Creating a task — `POST /api/tasks` (`backend/routes/tasks.js:142-342`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| TK-1 | Staff need the `create_task` permission (delegation) or `create_checklist` (checklist). Client logins are exempt from this check. | Implemented | `tasks.js:165-173` |
| TK-2 | A description is required. A date is required unless the assigner chose "doer sets due date" (delegation only). | Implemented | `tasks.js:203-204`, `148-149` |
| TK-3 | Admin, HOD and user roles may assign to any user. **A PC's task is always assigned to the PC themselves**, whatever `assignedTo` says. | Implemented | `tasks.js:199-202` |
| TK-4 | A **client login** can only create delegation tasks, and only for **one of its own handlers** (`client_handlers` or `clients.handler_id`). Any other target falls back to the primary handler. The task is force-tagged with the client's own `client_id`. | Implemented | `tasks.js:177-190` |
| TK-5 | A client-created task must be due **at least 2 days after today (IST)**, unless the doer sets the date. | Implemented | `tasks.js:190-198` |
| TK-6 | Staff may delegate **to a client's portal login** only if that login belongs to the tagged client, the creator is admin, HOD or a handler of that client, and a due date is set. | Implemented | `tasks.js:206-224` |
| TK-7 | Optional due time must be `HH:MM` (24-hour). | Implemented | `tasks.js:226-227`, `779-784` |
| TK-8 | If the date falls on an off-day (T-2), a **delegation** moves to the next working day. A **checklist** row is **not created** at all; the response says it was skipped. Tasks for client doers are not shifted. | Implemented | `tasks.js:229-252` |
| TK-9 | **Duplicate guard:** the same description, doer, assigner, client and due date created within **2 minutes** returns the existing id instead of inserting a second row. NULL-safe comparison. Applies to both kinds. | Implemented | `server.js:1157-1166`, `tasks.js:266-270`, `330-335` |
| TK-10 | The creator may name another user as the **approver**, which is stored as `assigned_by`: by `approverEmail`, or by `approver` id when `approval='yes'`. No permission is checked on who may be named. | Implemented (behaviour) / Inferred (no restriction) | `tasks.js:253-265` |
| TK-11 | A task you assign to yourself is stamped as already seen, so it does not trigger the new-task popup. | Implemented | `tasks.js:271-280` |
| TK-12 | The doer is notified by **email**. If the doer is a client login, the client's WhatsApp group is notified when one is set. Personal WhatsApp DMs are retired. | Implemented | `tasks.js:283-328` |
| TK-13 | **Bulk checklist** (`POST /api/tasks/bulk-checklist`) needs `create_checklist`. A PC's series is always their own. At most **800 dates** per call. Off-days are filtered out. **There is no duplicate check**, so the same series can be inserted twice. | Implemented / Missing (dedupe) | `tasks.js:537-568`. The project guards single creates against duplicates (TK-9) but not this path. |

### 3.3 Due date set by the doer — `PUT /api/tasks/:id/due-date`

| # | Rule | Status | Evidence |
|---|---|---|---|
| TK-14 | Only works while the task is still `awaiting_due_date`. Otherwise it answers 409. | Implemented | `tasks.js:365` |
| TK-15 | Allowed for the doer, the assigner, admin, PC, or anyone holding `admin_tasks`. | Implemented | `tasks.js:361-369` |
| TK-16 | The caller must give a date **or** a reason (≤500 chars). A reason alone keeps the task date-less but records why. | Implemented | `tasks.js:354-383` |
| TK-17 | A date given here is also pushed past off-days. | Implemented | `tasks.js:384-388` |

### 3.4 Status changes — `PUT /api/tasks/:id/status` (`tasks.js:570-737`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| TK-18 | `type` must be exactly `delegation` or `checklist`. The status must belong to that table's ENUM. | Implemented | `tasks.js:579-592` |
| TK-19 | A **revise needs a reason**. | Implemented | `tasks.js:594-596` |
| TK-20 | Privileged actors are **admin, PC, or `admin_tasks`**. Everyone else may only change tasks **assigned to them**. **HOD is not privileged here.** | Implemented | `tasks.js:601-609` |
| TK-21 | While an approval is pending (`waiting_approval=1`), a non-privileged doer can neither revise nor complete. | Implemented | `tasks.js:625-627` |
| TK-22 | **No completion without a due date**, for every role. | Implemented | `tasks.js:644-649` |
| TK-23 | Reopening a completed task (back to `pending`) needs the `reopen_task` permission. Completing your own task needs no permission. | Implemented | `tasks.js:657-660` |
| TK-24 | **Every revise of a delegation goes to the assigner for approval**, including admin and self-assigned tasks. The new date waits in `task_approvals` until approved. **Exception:** if the assigner is a client login, the revise applies immediately. | Implemented | `tasks.js:667-684` |
| TK-25 | A delegation **cannot be completed while it has open sub-tasks**, for every role. | Implemented | `tasks.js:690-697`, re-checked at approval `server.js:2280-2290` |
| TK-26 | **Completion needs approval** only when the task has `approval='yes'`, the actor is not privileged, and the actor is not the assigner. | Implemented | `tasks.js:701-708` |
| TK-27 | A privileged direct change cancels (deletes) any pending approval request on that task. | Implemented | `tasks.js:713-715` |
| TK-28 | Completing stamps `completed_at`; reopening clears it. | Implemented | `tasks.js:718-725` |
| TK-29 | A revise on a self-assigned task routes the approval to the same person, so they can approve their own date push. | Inferred | `tasks.js:676-680` (`requested_to = task.assigned_by`) with `server.js:2275` |

### 3.5 Edit and delete

| # | Rule | Status | Evidence |
|---|---|---|---|
| TK-30 | Edit or delete is allowed for **admin, HOD, `admin_tasks`, or the task's assigner** (`canModifyTask`). HOD is not limited to their own department here. PC is not in this list. | Implemented | `tasks.js:786-792` |
| TK-31 | Edit also needs `edit_task`; delete also needs `delete_task`. | Implemented | `tasks.js:802`, `848` |
| TK-32 | Writing a real date in Edit clears `awaiting_due_date`. Date changes are logged to `task_activity`. | Implemented | `tasks.js:808-838` |
| TK-33 | Deleting a **checklist series** removes rows with the same description, doer and assigner, from today onward (or all dates with `includePast=1`). Every row is archived first. | Implemented | `tasks.js:860-886` |
| TK-34 | Admin-only bulk tools: delete every task of a user, move all of a user's pending tasks to today (IST), delete every checklist on a date, and count or delete a year of checklist rows. | Implemented | `tasks.js:888-967`, `1142-1170` |

### 3.6 Sub-tasks and client asks

| # | Rule | Status | Evidence |
|---|---|---|---|
| TK-35 | **Only a client login can add sub-tasks** (the client's follow-up channel). | Implemented | `tasks.js:420-422` |
| TK-36 | Staff (doer, assigner, admin, HOD, PC) mark sub-tasks done or pending and add remarks. Clients cannot. | Implemented | `tasks.js:448-474`, `server.js:2195-2206` |
| TK-37 | A sub-task can be deleted by its creator, admin, PC or `admin_tasks`. It is archived first. | Implemented | `tasks.js:520-535` |
| TK-38 | **Client ask** ("what we need from the client by when"): staff only. The task must already have a due date, and the ask cannot be due later than the task. Blank text clears it. | Implemented | `tasks.js:476-518` |

### 3.7 Lists and buckets

| # | Rule | Status | Evidence |
|---|---|---|---|
| TK-39 | All Tasks visibility: admin, PC and `admin_tasks` see everything. **HOD sees tasks of users in the same department.** Others see their own. `mine=1` shows tasks I gave to others. | Implemented | `tasks.js:60-95` |
| TK-40 | Tasks whose **doer is a client login** appear only in the "Client Tasks" tab. Non-managers see only the ones they delegated. | Implemented | `tasks.js:71-79`, `97-106` |
| TK-41 | The checklist list is capped at today + 30 days unless a range is given. | Implemented | `tasks.js:112-119` |
| TK-42 | Dashboard **Pending** = `status IN (pending, revised)` and due today or earlier (or no date). **Upcoming** = due later **in the current calendar month** only. Checklist stats are capped at month end. | Implemented | `server.js:2117-2152` |
| TK-43 | The dashboard HOD view always uses the department on the HOD's own user row. A `hodDept` query value is ignored. | Implemented | `server.js:2087-2095` |
| TK-44 | The new-task popup covers delegation tasks from the last **7 days** not yet seen, and lists **5**. | Implemented | `tasks.js:964-1015` |

### 3.8 Activity log

- Status changes, due-date changes, transfers, holiday shifts, approvals and
  reasons are appended to `task_activity`. A failed log write never blocks the
  user. **Implemented**: `server.js:1170-1190`.
- Reopening a task clears `completed_at`, which leaves `task_activity` as the
  only record that the task was ever completed. **Implemented**:
  `tasks.js:718-733`.

---

## 4. Approvals (`server.js:2227-2380`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| AP-1 | Each request is routed to the task's assigner (`requested_to = assigned_by`). | Implemented | `tasks.js:676-680`, `702-704` |
| AP-2 | Admin, PC and `admin_approvals` see and decide **all** pending requests. Others see only requests routed to them. | Implemented | `server.js:2227-2240`, `2273-2275` |
| AP-3 | The action must be `approved` or `rejected`. A decision is one-time (atomic `WHERE status='pending'`). If the task write fails, the claim is rolled back. | Implemented | `server.js:2270`, `2292-2325` |
| AP-4 | An approved revise sets the task back to `pending` with the new date. An approved completion completes it and stamps `completed_at`. | Implemented | `server.js:2296-2311` |
| AP-5 | **Rejecting changes nothing on the task** except clearing `waiting_approval`. | Implemented | `server.js:2312-2314` |
| AP-6 | **Approve all revises** (admin, PC, `admin_approvals`) never brings back a task that has since been completed. The stale request is closed with a note. | Implemented | `server.js:2333-2378` |

---

## 5. Task transfers (`server.js:3786-4045`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| TR-1 | Requesting needs `transfer_task`. **User:** own tasks only. **HOD:** only tasks whose doer is in the HOD's department. Admin and PC: any task. | Implemented | `server.js:3798-3820` |
| TR-2 | A task can have only one pending transfer at a time. | Implemented | `server.js:3824-3831` |
| TR-3 | Admin, HOD and PC can see or decide transfers, as can holders of `admin_approvals`. An HOD (without `admin_approvals`) sees and acts only where the from-user or to-user is in their department. A blank department never matches. | Implemented | `server.js:3860-3863`, `3874-3883`, `3955-3964` |
| TR-4 | Decisions are one-time and atomic. **Approval moves the task only if it is still with the original doer.** Otherwise the request returns to pending with an "out of date" error. | Implemented | `server.js:3966-3995` |
| TR-5 | An approved transfer is logged in `task_activity`. | Implemented | `server.js:3996-4004` |

---

## 6. Weekly plan, check-in and scoring

| # | Rule | Status | Evidence |
|---|---|---|---|
| WP-1 | **Score formula:** `max(-100, -(pending/total×100 + overdue/total×50 + revised/total×25))`, rounded to 0.1. Range −100 to 0; `null` when there are no tasks. | Implemented | `server.js:4169-4174` |
| WP-2 | Setting a weekly plan needs `set_plan`. A non-admin can plan only for others in their own department, never themselves. `hod_id` is always the caller. | Implemented | `server.js:4053-4075` |
| WP-3 | The Monday check-in prompt appears only **Monday to Wednesday (IST)**, until the user commits a score or snoozes. | Implemented | `server.js:4311-4346` |
| WP-4 | A committed score must be between −100 and 0. A snooze lasts until tomorrow. | Implemented | `server.js:4362-4406` |
| WP-5 | The committed-score API accepts any `startDate`, not only the current week's Monday. | Inferred | `server.js:4366-4369` |
| WP-6 | MIS and Employee 360 read the same planned-vs-actual helper so their numbers agree. | Implemented | `server.js:4176-4184` |

---

## 7. Leave Tracker and Extra Working (`server.js:8878-9450`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| LV-1 | Types: `full_day`, `half_day`, `work_from_home`, `extra_working`. A reason is required for everything except Extra Working. | Implemented | `server.js:9153-9159` |
| LV-2 | **Approver routing uses the org role `user_role`** (falling back to `role`), not the app role. **user** → first HOD of the same department (by id), otherwise first admin. **hod / pc** → first admin by id. **admin** → another HOD of the same department, otherwise another admin, otherwise themselves. | Implemented | `server.js:8882-8924` |
| LV-3 | Each Extra Working row needs a client and a description (≤5000 chars), plus either minutes (1–1440) or hours (1–24). A day's total may not exceed 24 h. `hours` is always stored. | Implemented | `server.js:9149`, `9171-9227` |
| LV-4 | **One Extra Working request per user per date** while it is pending or approved. A rejected one frees the date. | Implemented | `server.js:9238-9256` |
| LV-5 | A **hardcoded overseer (user id 6)** sees every pending request in Approvals. | Implemented | `server.js:8927-8929`, `9009-9012` |
| LV-6 | **One named user (id 41)** has a Profile switch that sends their Extra Working **only** to the overseer as `sole_approver`. Only that approver may then decide it. | Implemented | `server.js:8931-8988`, `9262-9267`, `9361-9363` |
| LV-7 | A decision is approve or reject, only while pending, and atomic. Allowed for the assigned approver, an admin or `admin_leaves`, or an HOD (by `user_role`) in the same department as the assigned approver. | Implemented | `server.js:9351-9393` |
| LV-8 | **Nobody may approve their own request.** Rejecting your own is not blocked. | Implemented (approve) / Inferred (reject) | `server.js:9376-9380` |
| LV-9 | The person who decides overwrites `approver_id`. | Implemented | `server.js:9388-9392` |
| LV-10 | Delete is allowed for your own pending request, or for any request with `admin_leaves` (admin passes). It is archived first. | Implemented | `server.js:9432-9447` |
| LV-11 | Team view: admin, `leaves_all` (extra_access) and `admin_leaves` see all. HOD sees their department. **PC sees the whole company.** A user sees their own. | Implemented | `server.js:9030-9047` |
| LV-12 | For daily-report reminders, only full-day and half-day leave (pending or approved) count as "on leave". WFH and Extra Working do not. | Implemented | `server.js:9512-9531` |
| LV-13 | The reminder check uses the `from_date`–`to_date` range, but a request can hold non-consecutive dates (`dates_json`). Days between picked dates would also count as leave. | Inferred | `server.js:9518-9525` versus `9172-9245` |

---

## 8. Daily Task reports and Compliance

| # | Rule | Status | Evidence |
|---|---|---|---|
| DR-1 | Reports can only be filed for **today or yesterday (IST)**. | Implemented | `server.js:8295-8301` |
| DR-2 | Every row needs a client, department, description and duration over 0. | Implemented | `server.js:8304-8314` |
| DR-3 | **One submission per user per date; no editing afterwards.** This is a read-then-insert check; no unique key was found that enforces it. | Implemented / Inferred (race window) | `server.js:8316-8324` |
| DR-4 | The department list comes from users' departments plus `YouTube`, `LinkedIn` and `MDO`, with `mis executive` hidden. | Implemented | `server.js:8231-8247` |
| DR-5 | Department `CXO` and users flagged `exclude_from_reminder` are left out of daily-report reminders. | Implemented | `server.js:5347`, `5385-5390`, `5796-5797` |
| DR-6 | Compliance scope: admin sees all; HOD and PC see their own department; others see only themselves. | Implemented | `server.js:8364-8374`, `8478-8488` |
| DR-7 | Daily Reports (the all-staff report) is admin-only. | Implemented | `server.js:8750`, `8809` |

---

## 9. Clients, CRM, client portal

### 9.1 Client Master (`backend/routes/clients.js`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| CL-1 | Without `?scope=master`, any staff login gets the **full client list**, because the pickers across the app need every client. | Implemented | `clients.js:203-209` |
| CL-2 | Client Master scope: admin, PC, HOD and `admin_clients` see all. **CRM access** (`crm_clients` plus the clients page in the saved permissions) sees only clients that person added. Everyone else sees clients they handle. | Implemented | `clients.js:143-170` |
| CL-3 | **Billing name** is removed from responses unless the user is in `billing_name_viewer_ids` or holds `billing_name` in saved permissions. Being admin is **not** enough. Writing it needs the same right. | Implemented | `server.js:7741-7768`, `clients.js:242-245`, `445-449` |
| CL-4 | Full edits need `edit_clients`. A **handler** may change only the active flag, system links and WhatsApp group id. | Implemented | `clients.js:418-486` |
| CL-5 | On full edit, name, brand name and billing name cannot be blanked. A kickstart date must be `YYYY-MM-DD`. At least one department is required. | Implemented | `clients.js:438-457` |
| CL-6 | A WhatsApp group id must match `…@g.us`; blank clears it. | Implemented | `clients.js:474-482` |
| CL-7 | Deleting a client needs `admin_clients` (admin passes) and archives the row. No cascade of its tasks, handlers or portal login was found. | Implemented / Inferred (no cascade) | `clients.js:534-543` |
| CL-8 | **One portal login per client.** It is created or reset (email and password) by anyone with `edit_clients`. | Implemented | `clients.js:695-726` |
| CL-9 | A CRM transfer moves `added_by` to another user who holds CRM access. It needs `admin_clients`. | Implemented | `clients.js:187-201` |
| CL-10 | Saving handlers replaces the whole list. The primary `handler_id` becomes the first one selected. | Implemented | `clients.js:513-531` |

### 9.2 Client portal (`server.js:7100-7600`, `8137-8200`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| CP-1 | A client login only ever reads its own linked client; `?clientId` is ignored. Staff preview it with `?clientId` (managers, `admin_clients`, handlers, or whoever added the client). | Implemented | `server.js:7131-7168` |
| CP-2 | Writes from the portal (password, feedback) are **client-role only**. Staff preview cannot submit. | Implemented | `server.js:7292-7303`, `7580`, `8156`, `8177` |
| CP-3 | Feedback: rating 1–5. The employee rated must be one of the client's handlers. Recipients are filtered to the HODs of the handlers' departments plus `feedback_fixed_ids`. | Implemented | `server.js:7558-7597` |
| CP-4 | A client may edit or delete only its own feedback. Deletes are archived. | Implemented | `server.js:8154-8195` |
| CP-5 | Two named people are hidden from the portal's Top Performers. | Implemented | `server.js:7319` |

### 9.3 Invoices

| # | Rule | Status | Evidence |
|---|---|---|---|
| IN-1 | Upload and delete are for admins and `invoice_manager_ids` only. The client can view and download, never delete. | Implemented | `server.js:7181-7206`, `7210-7260` |
| IN-2 | Must be a real PDF (`%PDF` header), at most 4 MB, with an invoice number, a date and an amount over 0. | Implemented | `server.js:7192`, `7224-7234` |

### 9.4 DMS (Google Drive)

- Writes (folders, uploads, links, rename, delete) need `requireClientEditor`:
  admin, HOD, PC, or a handler of that client. **Implemented**: `server.js:1131-1138`,
  `clients.js:743-1110`.
- Listing a folder's files needs only that the folder belongs to the client in
  the URL. There is no handler or role check. **Implemented**: `clients.js:895-899`.

### 9.5 Client credentials vault

- Stores logins for systems built for clients. **Passwords are stored as typed
  so they can be read back.** Every route is admin-only. **Implemented**:
  `server.js:7598-7720`.

---

## 10. Payment requests (`backend/routes/payment-requests.js`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| PR-1 | Any staff login can submit. At least one department is required, the amount must be over 0, and it must match the `[₹X]` prefix carried in the reason. | Implemented | `payment-requests.js:117-150` |
| PR-2 | Approvers are admins plus `payment_approver_ids`. **Nobody may approve their own request.** Decisions are one-time and atomic. | Implemented | `server.js:7848-7851`, `payment-requests.js:335-356` |
| PR-3 | Editing is allowed for the owner or `admin_paymentreq`, **only while pending**, and atomic. | Implemented | `payment-requests.js:278-331` |
| PR-4 | Paid, cancelled and bill markers are stored as **sentinel rows** (`bank_name='__system__'`, reason `__paid__:<id>` and similar), not as columns. They are allowed only on an **approved** request, only once, by the owner or an approver. | Implemented | `payment-requests.js:152-188` |
| PR-5 | In the live flow, the requester marks their own request paid. The old "settler" role was removed. | Implemented (per code comment) | `server.js:7797-7803` |
| PR-6 | Adding or deleting cards, and deleting requests, needs `admin_paymentreq`. | Implemented | `payment-requests.js:52-113` |

## 11. Credit card statements (`backend/routes/credit-cards.js`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| CC-1 | Viewing is for admins plus `cc_viewer_ids` (read-only). **Every write is admin-only.** | Implemented | `server.js:7735-7741` |
| CC-2 | PDF statements are parsed by the **OpenAI API** (`OPENAI_MODEL`, default `gpt-4.1-mini`). An optional PDF password comes with the upload. | Implemented | `credit-cards.js:118-125`, `747-822` |
| CC-3 | Only **masked** card numbers are kept. An unmasked number from the AI is replaced by the masked number printed on page 1. | Implemented | `credit-cards.js:782-790` |
| CC-4 | Upload limits: Excel 10 MB, PDF 25 MB. | Implemented | `server.js:7727-7728` |
| CC-5 | Deleting a statement cascades to its transactions (foreign key). | Implemented | `credit-cards.js:309`, `330` |

## 12. Inventory (`backend/routes/inventory.js`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| IV-1 | Reading needs the `inventory` page. Admin, HOD and `edit_inventory` see all items; others see what is assigned to them. | Implemented | `inventory.js:43-82` |
| IV-2 | Create, edit, assign, list assignments and confirm return need `edit_inventory`. Brand and model are required; type comes from a fixed list. | Implemented | `inventory.js:86-107`, `134-159`, `187-238`, `279-302` |
| IV-3 | **Any logged-in user can self-add** an item that is immediately assigned to them. | Implemented | `inventory.js:112-131` |
| IV-4 | An item can be assigned only from `available` (atomic). Its status cannot change while someone holds it. | Implemented | `inventory.js:144-151`, `199-212` |
| IV-5 | Delete needs admin or `admin_inventory`, and **not while assigned**. | Implemented | `inventory.js:170-183` |
| IV-6 | Starting a handover: the holder (holder reasons only), or admin, HOD or `admin_inventory`. Only admins can retire. A final return sets the item to available, damaged or retired. | Implemented | `inventory.js:245-273`, `284-300` |

## 13. Meetings and Day Plan

| # | Rule | Status | Evidence |
|---|---|---|---|
| MT-1 | Any staff login can create a meeting and becomes its organiser. Recurring meetings need an end date and at least one weekday. | Implemented | `meetings.js:158-216` |
| MT-2 | Only the organiser or `admin_meetings` may edit, change status or cancel. | Implemented | `meetings.js:233-237`, `272-274`, `292-294` |
| MT-3 | **"Delete" is a soft cancel** (`status='cancelled'`). It can cover the following occurrences in a series and sends one notification. | Implemented | `meetings.js:284-310` |
| MT-4 | Slots run 10:00–19:00 in 30-minute steps; Sundays and holidays are excluded. | Implemented | `server.js:9793-9794`, `9803` |
| MT-5 | Client meeting notifications go to one fixed WhatsApp group. | Implemented | `server.js:9790-9792` |
| MT-6 | Day-plan items belong to their owner. Delete is for the owner or an admin, and is archived. | Implemented | `server.js:9997-10035` |

## 14. HR Portal (`backend/routes/hrm.js`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| HR-1 | Candidate statuses: `Scheduled`, `Rescheduled`, `Selected`, `Rejected`, `Offer Sent`, `Offer Letter Sent`. | Implemented | `hrm.js:1086-1087` |
| HR-2 | **No offer letter, preliminary or final, until the joining-details form is received** for departments that need it. `HRM_JOINING_FORM_DEPTS` defaults to `*`, meaning all. | Implemented | `hrm.js:55-92`, `1094-1097` |
| HR-3 | Salary is entered **monthly**; annual CTC is calculated as ×12, rounded to paise. | Implemented | `hrm.js:336-342` |
| HR-4 | The joining form (Google Apps Script, `employee-form/`) posts to `/api/hrm/joining-form` with a shared secret and a per-candidate token. | Implemented | `hrm.js:793-907`, `employee-form/Code.gs:105-120` |
| HR-5 | Offer letters are served at public token URLs (`/offer/:token`, `/offer-pdf/:token`, `/offer-pdf-prelim/:token`). | Implemented | `hrm.js:713-790` |
| HR-6 | Reading needs the `hrm` page (in no role default, so admin unless granted). Scheduling needs `hrm_schedule`, status and offer actions need `hrm_update_status`, delete needs `admin_hrm`. | Implemented | `hrm.js:909-1640` |

## 15. FMS (`backend/routes/fms.js`)

| # | Rule | Status | Evidence |
|---|---|---|---|
| FM-1 | FMS configuration (sheets, steps, doers) is admin-only. | Implemented | `fms.js:27-255` |
| FM-2 | `admin_fms_tasks` sees every FMS. Others see only FMSes where they are a step doer. | Implemented | `fms.js:257-273` |
| FM-3 | Only an assigned doer can mark a step done. **A step with no doers is open to everyone.** | Implemented | `fms.js:317-330`, `480-485` |
| FM-4 | The row must be below the header row. Extra inputs may only target columns configured for that step. The timestamp is written in IST. | Implemented | `fms.js:486-500` |

## 16. Leads Enquiry (`backend/routes/leads.js`)

- Four Google Sheet sources: website, Meta, Google Ads, manual. Nothing is
  stored in the database. Manual entries are append-only. **Implemented**:
  `leads.js:1-50`, `292-343`.
- Reading needs the `leads` page and writing needs `edit_leads`. Neither is in
  any role default. **Implemented**: `leads.js:84-93`.

## 17. Task Assistant (`backend/routes/chatbot.js`)

- Rule-based with **no AI**, **read-only**, and limited to admin, HOD and PC.
  It only answers about people the asker could already see: HOD gets their own
  department. MIS answers need (admin or HOD) plus `mis`; compliance answers
  need compliance access. **Implemented**: `chatbot.js:1-33`, `1041`,
  `1118-1140`, `1280-1320`.

## 18. Users and onboarding

| # | Rule | Status | Evidence |
|---|---|---|---|
| US-1 | Only admins create, edit or delete users. Roles allowed in the form: `admin`, `hod`, `pc`, `user`. | Implemented | `server.js:3362-3434`, `3493-3505` |
| US-2 | A new user is **emailed their login and plaintext password**, a welcome is posted to a fixed WhatsApp group, and a row is appended to a fixed Google Sheet. | Implemented | `server.js:3376-3399` |
| US-3 | An admin cannot delete themselves. The role-only endpoint stops an admin removing their own admin role. | Implemented | `server.js:3495`, `3668-3669` |
| US-4 | Users may edit their own profile. **Changing the login email needs the current password.** The name can be changed freely. | Implemented | `server.js:3695-3721` |
| US-5 | A profile photo must be a base64 image data URL. | Implemented | `server.js:3690-3693` |

## 19. Deletion, archive, restore, purge

| # | Rule | Status | Evidence |
|---|---|---|---|
| DL-1 | Before a hard delete, rows are copied into `deleted_records`. **If the archive write fails, the delete does not happen.** | Implemented | `server.js:1186-1232` |
| DL-2 | Admins may restore archived rows from an allow-list of tables. Columns that no longer exist are dropped. A restore that would reuse an existing id is refused. A row is restored at most once. | Implemented | `server.js:10110-10208` |
| DL-3 | Archive rows are **purged after 60 days** (`DELETED_RECORDS_RETENTION_DAYS`). | Implemented | `server.js:10213-10232` |
| DL-4 | Marking a holiday deletes pending checklist rows on that date (archived first) and moves pending delegation tasks to the next working day. | Implemented | `server.js:9750-9782` |
| DL-5 | Large blobs (invoice PDFs) are left out of the archive. | Implemented | `server.js:7250-7256` |

## 20. Scheduled jobs (times converted to IST)

| Job | Trigger | Schedule (IST) | Evidence |
|---|---|---|---|
| daily-reminder | Vercel Cron | 19:30 daily | `vercel.json` |
| pending-reminder | Vercel Cron | 12:00 daily | `vercel.json` |
| pending-summary | Vercel Cron | 10:00 and 16:00 daily | `vercel.json` |
| celebrations | Vercel Cron | 10:00 daily | `vercel.json` |
| holiday-notice | Vercel Cron | 12:00 daily | `vercel.json` |
| leave-tracker-reminder | Vercel Cron | 10:00 on the 3rd of each month | `vercel.json` |
| purge-deleted-records | Vercel Cron | 01:30 daily | `vercel.json` |
| client-pending-digest | Vercel Cron | 09:30 Mon–Fri | `vercel.json` |
| department-pending-digest | Vercel Cron | 09:30 daily | `vercel.json` |
| client-completed-digest | Vercel Cron | 17:30 daily | `vercel.json` |
| handler-leave-notice | Vercel Cron | 09:30 Mon–Sat | `vercel.json` |
| due-date-reminder | GitHub Actions | every 4 hours | `.github/workflows/due-date-reminder.yml:12` |
| meeting-reminder | GitHub Actions, plus a lazy check on any API call (at most every 5 min) | every 5 min | `.github/workflows/meeting-reminder.yml:13`, `server.js:137-145` |
| mdo-new-task-notify | No scheduler in the repo | — | `server.js:5740` — **Unknown** who calls it |

The other GitHub workflows are manual (`workflow_dispatch`) only.

---

## 21. Business-rule contradictions and gaps

| # | Finding | Status | Evidence |
|---|---|---|---|
| BC-1 | Users carry `week_off` / `extra_off`, and task messages talk about the "doer's week-off", yet off-day logic ignores both columns. | Implemented (contradiction) | `server.js:9469`; `tasks.js:245-249` message text |
| BC-2 | PC holds `create_task`, but every task a PC creates is assigned to the PC. PC also holds `edit_task` and `delete_task`, yet `canModifyTask` lets PC change only tasks the PC assigned. Meanwhile PC is fully privileged for status changes and approvals. | Implemented (inconsistent) | `tasks.js:201`, `786-792`, `601`; `server.js:3577-3580` |
| BC-3 | HOD can edit or delete **any** task company-wide (`canModifyTask`), but can only see their own department's tasks and **cannot** change the status of others' tasks. | Implemented (inconsistent) | `tasks.js:786-788`, `81-91`, `601-609` |
| BC-4 | The single-task duplicate guard exists, but bulk checklist creation has none. | Missing | `tasks.js:537-568` versus `server.js:1157` |
| BC-5 | The leave reminder treats every day between the first and last picked date as leave, even when the picked dates are not consecutive. | Inferred | `server.js:9518-9525` |
| BC-6 | A comment says users on **approved** leave are excluded from the report-not-filed list, but the set also includes **pending** leave. | Implemented (comment mismatch) | `server.js:5382` versus `9512-9525` |
| BC-7 | The `extra_access` option is labelled "Receive Pending Task Summary **on WhatsApp**", but those recipients are sent **email**. | Implemented (label mismatch) | `server.js:3304` versus `5631-5645` |
| BC-8 | Meeting lists are limited to the organiser and attendees, but a single meeting (with attendee emails) can be fetched by any staff login. | Implemented (inconsistent) | `meetings.js:33-35` versus `92-122` |
| BC-9 | The DMS file list is readable by any staff login for any client folder, while every DMS write is limited to managers and handlers. | Implemented (inconsistent) | `clients.js:895-899` versus `server.js:1131` |
| BC-10 | Self-assigned revises are approved by the same person (TK-29). | Inferred | `tasks.js:678-680` |
| BC-11 | Whether `mdo-new-task-notify` runs on any schedule. | Unknown | `server.js:5740`; absent from `vercel.json` and the workflows |
| BC-12 | `README.md` says 11 tables and a different default admin; the code creates 43 tables and seeds a different account. | Implemented (stale doc) | `README.md:11`, `205-230`, `server.js:879-892` |
