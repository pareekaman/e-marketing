# Roles and Permissions — E-Marketing Task Manager

An audit of how the code decides who can see and do what. It covers the same
snapshot as [business.md](business.md): `TM_branch` at `e9073b8` plus the
working-tree changes present on 2026-10-05. The status tags (**Implemented /
Missing / Inferred / Unknown**) mean the same as in business.md. Evidence is
given as `file:line`.

---

## 1. Roles

| Role | Meaning in code | Evidence |
|---|---|---|
| `admin` | Full access. `getEffectivePerms` returns `'all'`. | `server.js:3594-3596` |
| `hod` | Head of department. Many reads are scoped to the HOD's own `users.department`. | `server.js:3564-3576` |
| `pc` | A staff role with company-wide views in several places (dashboard, approvals, transfers, leave team view, client list). | `server.js:3577-3580`, `2070`, `2227-2229` |
| `user` | A regular employee. Sees their own data. | `server.js:3581-3591` |
| `client` | An external client portal login, linked to one `clients` row through `users.client_id`. | `server.js:1072-1077`, `backend/routes/clients.js:695-726` |

- **Two role columns.** `users.role` is the app permission role.
  `users.user_role` is the org-hierarchy role, used for leave approval
  routing, the feedback page and "My Team". Code reads it as
  `COALESCE(user_role, role)`. **Implemented**: `server.js:8882-8890`,
  `3036-3041`, `8064-8067`.
- The ENUM was widened to include `client` by a migration. **Implemented**:
  `server.js:4670-4671`. The base `CREATE TABLE` still lists only four roles
  (`server.js:356`).
- The Users form and the role endpoint accept only `admin | hod | pc | user`.
  Client logins are created only through Client Master. **Implemented**:
  `server.js:3370`, `3667`, `backend/routes/clients.js:695-726`.

---

## 2. How a request is authorised

### 2.1 Server side

1. **`requireAuth`** (`server.js:1046-1074`) verifies the JWT, then reads the
   **live role from the database**. It caches that role for 30 s; any write to
   `users.role` clears the cache entry (`server.js:1018-1044`). A deleted user
   gets a 401.
2. A **client** login is limited to `/api/me`, `/api/logout`, `/api/tasks…`
   and `/api/client-portal…` (`CLIENT_ALLOWED_API`, `server.js:1069-1077`).
3. Routes then apply one or more of: role middleware (§6), `userCanSee(page)`,
   `userCanDo(action)`, ownership checks, department scoping, or membership in
   an `app_settings` id list (§5).

**Permission resolution** (`getEffectivePerms`, `server.js:3594-3608`):

| Step | Rule | Status |
|---|---|---|
| 1 | `role === 'admin'` → everything (`'all'`). | Implemented |
| 2 | If the user has a saved `users.user_permissions` row (`{pages, actions}`), it **replaces** the role defaults completely. Nothing is merged. | Implemented |
| 3 | Otherwise: role defaults (`SERVER_ROLE_DEFAULTS`) for pages and actions, **plus** `users.extra_access` pages. | Implemented |
| 4 | `extra_access` is **not consulted at all** once a `user_permissions` row exists. | Implemented (follows from step 2) |
| 5 | A role with no defaults (`client`) resolves to empty pages and actions unless a row is saved. | Implemented (`server.js:3603`) |

One-time backfill: on 2026-10-03 `create_task`, `create_checklist` and
`transfer_task` were added to every saved `user_permissions` row for
hod/pc/user. A marker in `app_settings` (`perm_task_buttons_v1`) stops it from
running twice. **Implemented**: `server.js:4941-4972`.

### 2.2 Frontend

- `/api/me` returns the resolved answer as `can: {all, pages, actions}`. The
  browser does not recompute it. On error it returns `can: null`, so everything
  is hidden. **Implemented**: `server.js:1940-1953`.
- `canSee(page)` / `canDo(action)` read `ME.can` only and return false when it
  is missing. **Implemented**: `public/js/core.js:105-122`.
- `/api/me` also returns per-feature flags: `canViewAllLeaves`,
  `canApprovePayments`, `canReviewMdoTasks`, `canViewCreditCards`,
  `canViewBillingName`, `canManageInvoices`. **Implemented**:
  `server.js:1966-1989`.
- Frontend files still contain **68 lines with direct `ME.role ===` / `!==` checks** (for example
  `public/js/dashboard.js` 18, `core.js` 11, `tasks.js` 8). They hide or show UI
  only. **Implemented**: count by grep across `public/js/*.js`.
- One button (the external "Manpur" task manager link) is shown by **user
  name**. The code says this is presentation only and not a permission
  boundary. **Implemented**: `public/js/core.js:124-142`.

### 2.3 Access Control panel (`public/js/access.js`)

- `PERM_TREE` (`access.js:42-190`) lists 22 pages with levels **No Access /
  View / Editor / Admin**. Editor maps to the page's `edit_*` (and similar)
  action keys; Admin maps to its `admin_*` key.
- Flags: `enforced` (the server actually checks the level), `readOnly` (no
  Editor level), `grantable:false` / `locked` (can only revoke, or cannot be
  changed).
- Saving writes `PUT /api/user-permissions/:id` (admin only). Unknown keys are
  dropped (`server.js:3642-3655`). The role is changed through
  `PATCH /api/users/:id/role` (`server.js:3659-3685`).
- Billing Name and CRM Access are per-person checkboxes on the Client Master
  row. They are stored as the actions `billing_name` and `crm_clients`.
  **Implemented**: `access.js:466-500`.
- `extra_access` is edited on the **Users** tab, not in Access Control
  (`server.js:3278-3306`).

---

## 3. Role defaults (`SERVER_ROLE_DEFAULTS`, `server.js:3558-3592`)

These apply only to users **without** a saved `user_permissions` row.

| Page key | hod | pc | user |
|---|:-:|:-:|:-:|
| dashboard | ✓ | ✓ | ✓ |
| alltasks | ✓ | ✓ | ✓ |
| approvals | ✓ | ✓ | ✓ |
| mis | ✓ | | |
| clients | ✓ | ✓ | ✓ |
| leaves | ✓ | ✓ | ✓ |
| meetings | ✓ | ✓ | ✓ |
| daily | ✓ | ✓ | ✓ |
| fms-tasks | ✓ | ✓ | |
| inventory | ✓ | ✓ | ✓ |
| dms | | ✓ | |
| compliance | ✓ | ✓ | ✓ |
| paymentreq | ✓ | ✓ | ✓ |
| feedback | ✓ | ✓ | ✓ |
| creditcards | ✓ | ✓ | ✓ |
| race, fms, users, hrm, dailyreports, logs, leads | | | |

| Action key | hod | pc | user |
|---|:-:|:-:|:-:|
| create_task | ✓ | ✓ | ✓ |
| create_checklist | ✓ | ✓ | ✓ |
| transfer_task | ✓ | ✓ | ✓ |
| edit_task | ✓ | ✓ | ✓ |
| delete_task | ✓ | ✓ | ✓ |
| reopen_task | ✓ | ✓ | |
| approve_revision | ✓ | ✓ | |
| bulk_approve | | ✓ | |
| set_plan | ✓ | | |
| delete_leave | ✓ | | |
| edit_inventory | ✓ | | |
| edit_clients | ✓ | | |

The HR Portal is admin-only by an explicit decision dated 2026-08-11
(`server.js:3559-3563`).

---

## 4. Permission keys and where the server enforces them

### 4.1 Page keys

The server checks `userCanSee` for only **five** page keys. For every other
page, "No Access" in Access Control hides the tab but leaves the API reachable;
those APIs are then limited by role, action key or ownership (§7).

| Page key | Server-side page check | Evidence |
|---|---|---|
| `mis` | Yes. (admin or hod) **and** `canSee('mis')`. | `server.js:1108-1111` |
| `compliance` | Yes, `requireComplianceViewer`. | `server.js:1113-1116`, `8375`, `8495`, `8718` |
| `hrm` | Yes, every HRM read. | `backend/routes/hrm.js:909-1615` |
| `inventory` | Yes, the item list. | `backend/routes/inventory.js:49` |
| `leads` | Yes, every leads read. | `backend/routes/leads.js:84-88` |
| `dashboard`, `alltasks`, `approvals`, `race`, `fms`, `fms-tasks`, `daily`, `clients`, `dailyreports`, `leaves`, `meetings`, `dms`, `paymentreq`, `feedback`, `users`, `creditcards`, `logs` | No page-level check. | grep: no `userCanSee(…,'<key>')` |

### 4.2 Action keys (`VALID_UP_ACTIONS`, `server.js:3533-3549`)

| Action key | Enforced on the server? | Evidence |
|---|---|---|
| `create_task`, `create_checklist` | **Yes**, `POST /api/tasks` (key picked by task type) and bulk checklist. | `tasks.js:165-173`, `539` |
| `edit_task` / `delete_task` | **Yes**, on top of `canModifyTask`. | `tasks.js:802`, `848`, `864` |
| `reopen_task` | **Yes**, completed → pending. | `tasks.js:657-660` |
| `transfer_task` | **Yes**. | `server.js:3798` |
| `set_plan` | **Yes**. | `server.js:4054` |
| `edit_clients` | **Yes**, Client Master writes and portal-login creation. | `server.js:1120-1123`, `clients.js:277-725` |
| `edit_inventory` | **Yes**. | `inventory.js:58-285`, `chatbot.js:1280` |
| `edit_leads` | **Yes**. | `leads.js:89-93` |
| `hrm_schedule`, `hrm_update_status` | **Yes**. | `hrm.js:993`, `1045`, `960`, `1083`, `1190`, `1301`, `1487`, `1618`, `1628` |
| `admin_tasks` | **Yes**. | `tasks.js:52`, `361`, `487`, `525`, `601`, `767`, `787`, `1090`; `server.js:3763` |
| `admin_approvals` | **Yes**. | `server.js:2228`, `3861` |
| `admin_fms_tasks` | **Yes**. | `fms.js:263-482` |
| `admin_clients` | **Yes**. | `server.js:7157`; `clients.js:128`, `152`, `183`, `189`, `535` |
| `admin_leaves` | **Yes**. | `server.js:9035`, `9366`, `9439` |
| `admin_meetings` | **Yes**. | `meetings.js:235`, `272`, `292` |
| `admin_inventory` | **Yes**. | `inventory.js:171`, `257` |
| `admin_paymentreq` | **Yes**. | `payment-requests.js:54`, `65`, `100`, `293` |
| `admin_hrm` | **Yes**. | `hrm.js:1070` |
| `billing_name` | **Yes**, read directly from the saved row. Admin role does **not** imply it. | `server.js:7760-7768` |
| `crm_clients` | **Yes**, read directly from the saved row (needs the `clients` page too). | `clients.js:115-124`, `170-180` |
| `approve_revision`, `bulk_approve` | **No**. Approvals use role or `admin_approvals` instead. | `server.js:2227-2229`, `2334` |
| `delete_leave` | **No**. Leave delete uses owner-pending or `admin_leaves`. | `server.js:9432-9440` |
| `edit_dashboard`, `edit_mis`, `edit_race`, `edit_fms`, `edit_fms_tasks`, `edit_compliance`, `edit_dailyreports`, `edit_meetings`, `edit_dms`, `edit_paymentreq`, `edit_feedback`, `edit_users`, `edit_creditcards`, `edit_logs` | **No**. A code comment says these are stored so the choice survives until a page starts honouring them. | `server.js:3529-3532`; grep finds no `userCanDo` for these keys |

### 4.3 `extra_access` keys (`server.js:3278`)

`race`, `mis`, `fms`, `users`, `clients`, `compliance`, `leaves_all`,
`pending_summary_recipient`. They are added to pages only while the user has
no saved `user_permissions` row (`server.js:3605-3607`).

- `leaves_all` → full-team leave view (`server.js:1900-1907`, `9035`).
- `pending_summary_recipient` → receives the pending-task summary by email
  (`server.js:5631-5645`).
- `GET /api/users` removes `extra_access` from the response for non-admins
  (`server.js:3318-3328`).

---

## 5. Access given to named people (not to roles)

These rights belong to specific user ids stored in `app_settings`, or written
straight into code. Being an admin does **not** always include them.

| Setting / constant | Grants | Admin included? | Seeded or defined | Evidence |
|---|---|---|---|---|
| `payment_approver_ids` | Approve or reject payment requests; list all | Yes, for API calls (`isPaymentApprover`). The tab shows only to list members. | Seeded once from names | `server.js:7804`, `7848-7851`, `1966-1971` |
| `cc_viewer_ids` | Read credit card statements | Yes | Seeded once from a name | `server.js:7805`, `7735-7738` |
| `wa_task_approver_ids` | Gets the email to review WhatsApp-bot tasks | n/a (the review routes are admin-only) | Seeded once from a name | `server.js:7806`, `5168` |
| `mdo_reviewer_ids` | Shows the MDO tab (`canReviewMdoTasks`) | n/a (routes are admin-only) | Seeded once from a name | `server.js:7807`, `1979`, `7954-7965` |
| `onboarding_owner_ids` | HRM onboarding notifications | n/a | Seeded once from a name | `server.js:7808`, `hrm.js:1159` |
| `feedback_fixed_ids` | Always gets client escalations; can open Feedback | n/a | Seeded once from names; falls back to a name match | `server.js:7809-7812`, `7859-7865` |
| `billing_name_viewer_ids` | Read and write client billing name | **No** | Written by code on every boot (code wins) | `server.js:7832`, `7760` |
| `invoice_manager_ids` | Upload or delete client invoices | Yes | Written by code on every boot | `server.js:7834`, `7202-7205` |
| `theme_admin_ids` | Change the company festival theme | **No** | Written by code on every boot | `server.js:7836`, `9596-9668` |
| `LEAVE_OVERSEER_ID = 6` | Sees every pending leave in Approvals | n/a | Hardcoded | `server.js:8927-8929` |
| `EW_SIMRAN_ONLY_USER_ID = 41` | Profile switch to send Extra Working only to the overseer | n/a | Hardcoded | `server.js:8931-8937`, `8969-8987` |

The name seeds run only while the setting is absent. The id lists are
reconciled to the code on every boot, so editing the database row by hand does
not last (`server.js:7816-7830`, `7909-7942`).

---

## 6. Middleware catalogue

| Middleware | Who passes | Evidence | Note |
|---|---|---|---|
| `requireAuth` | Any valid, existing user; clients only on allowed paths | `server.js:1046-1074` | |
| `requireAdmin` | `admin` | `server.js:1078-1081` | |
| `requireAdminOrHod` | `admin`, `hod` **and `pc`** | `server.js:1082-1085` | The name does not mention PC |
| `requireAdminOrHodOnly` | `admin`, `hod` | `server.js:1086-1089` | Used by `/api/dashboard/activity` |
| `requireMisViewer` | (`admin` or `hod`) **and** `canSee('mis')` | `server.js:1108-1111` | A grant can narrow MIS, never widen it |
| `requireComplianceViewer` | `canSee('compliance')` | `server.js:1113-1116` | |
| `requireClientsEditor` | `canDo('edit_clients')` | `server.js:1120-1123` | |
| `requireAdminOrPC` | `admin`, `pc` | `server.js:1124-1127` | **Defined but never used** (grep) |
| `requireClientEditor` | `admin`, `hod`, `pc`, or a handler of client `:id` | `server.js:1131-1138` | Used by DMS writes |

---

## 7. Who can do what — by module

"Any staff" means any authenticated non-client login. "Own" means
assigned-to-me, created-by-me or similar, as stated.

### 7.1 Tasks

| Action | Allowed | Evidence |
|---|---|---|
| List tasks | admin, pc, `admin_tasks`: all · hod: own department · others: own | `tasks.js:60-95` |
| Create delegation / checklist | `create_task` / `create_checklist`; client logins only to their own handlers; **PC only to self** | `tasks.js:165-202` |
| Set the due date on an awaiting task | doer, assigner, admin, pc, `admin_tasks` | `tasks.js:361-369` |
| Change status | admin, pc, `admin_tasks` on any task; otherwise doer only. Reopen needs `reopen_task`. | `tasks.js:601-609`, `657-660` |
| Edit / delete | (admin, hod, `admin_tasks`, or assigner) **and** `edit_task` / `delete_task` | `tasks.js:786-802`, `844-848` |
| Read detail | admin, hod, pc, `admin_tasks`, doer, assigner | `tasks.js:759-773` |
| Comments, activity, sub-task view | admin, hod, pc, doer, assigner, or the task's client | `server.js:2195-2220`, `3734-3757`, `tasks.js:741-757` |
| Add sub-task | **client only** | `tasks.js:420-422` |
| Bulk delete or move a user's tasks, delete by date, checklist-year tools | admin | `tasks.js:888-967`, `1142-1170` |
| Delete a comment | its author or `admin_tasks` | `server.js:3759-3768` |

### 7.2 Approvals and transfers

| Action | Allowed | Evidence |
|---|---|---|
| See or decide approvals | the routed assigner; admin, pc, `admin_approvals` see and decide all | `server.js:2227-2275` |
| Approve all revises | admin, pc, `admin_approvals` | `server.js:2333-2334` |
| Request a transfer | `transfer_task`; user: own tasks; hod: own department's tasks | `server.js:3798-3820` |
| See or decide transfers | admin, hod, pc, `admin_approvals`; hod limited to own department | `server.js:3860-3963` |

### 7.3 Reporting

| Action | Allowed | Evidence |
|---|---|---|
| Dashboard | Any staff. admin and pc see all (and can filter by employee). hod sees own department. Others see their own. | `server.js:2066-2110` |
| Dashboard activity | admin, hod (hod scoped to department) | `server.js:3023` |
| Leaderboard | Any staff; `scope=team` limits to the HOD's department | `server.js:3104-3112` |
| MIS (all routes) | `requireMisViewer`; hod limited to own department, including drill-downs | `server.js:2382-2395`, `2748`, `2968`, `3136`, `4575` |
| FMS dashboard | admin and pc all; hod own department; others own | `server.js:2570-2600` |
| Compliance / Employee 360 | `canSee('compliance')`; admin all; hod and pc own department; others self | `server.js:8364-8488` |
| Daily Reports (all staff) | admin | `server.js:8750`, `8809` |
| Weekly plan: read / write | read: `requireAdminOrHod` (admin, hod, pc); write: `set_plan`, non-admin only for own department and never self | `server.js:4053-4135` |

### 7.4 Leave

| Action | Allowed | Evidence |
|---|---|---|
| File a request | Any staff | `server.js:9151` |
| Team view | admin, `leaves_all`, `admin_leaves`: all · hod: own department · **pc: all** · user: own | `server.js:9030-9047` |
| Decide | the assigned approver, admin or `admin_leaves`, or an HOD (by `user_role`) of the approver's department; `sole_approver` requests only by their approver; **never approve your own** | `server.js:9351-9380` |
| Delete | own pending request, or `admin_leaves` | `server.js:9432-9440` |

### 7.5 Clients, portal, DMS, vault

| Action | Allowed | Evidence |
|---|---|---|
| Full client list (pickers) | Any staff | `clients.js:203-209` |
| Client Master list | admin, pc, hod, `admin_clients`: all · CRM access: clients they added · others: clients they handle | `clients.js:143-170` |
| Create, bulk import, logo, handlers, portal login | `edit_clients` (hod by default) | `clients.js:277-725` |
| Edit a client | `edit_clients` fully; a handler only active flag, links and WhatsApp group | `clients.js:418-486` |
| Delete a client, CRM transfer | `admin_clients` (admin passes) | `clients.js:181-201`, `534-543` |
| Billing name | `billing_name_viewer_ids` or a saved `billing_name` grant; admin not enough | `server.js:7760-7768` |
| Portal reads | the client itself; staff preview: admin, hod, pc, `admin_clients`, handler, or whoever added the client | `server.js:7131-7168` |
| Portal writes (password, feedback) | client role only | `server.js:7292-7303`, `7578-7597`, `8154-8195` |
| Invoices | admin or `invoice_manager_ids`; the client reads its own | `server.js:7202-7290` |
| DMS writes | `requireClientEditor` | `clients.js:743-1110` |
| DMS file list | **Any staff**, if the folder belongs to the client in the URL | `clients.js:895-899` |
| Credentials vault | admin | `server.js:7644-7720` |
| Feedback page | admin or hod (by role or `user_role`), or `feedback_fixed_ids`; the list shows only items where the viewer is a recipient | `server.js:8061-8135` |

### 7.6 Money

| Action | Allowed | Evidence |
|---|---|---|
| Submit a payment request | Any staff | `payment-requests.js:117` |
| List all / approve / reject | `isPaymentApprover` (admin or list); never your own | `payment-requests.js:220-227`, `335-356` |
| Edit a request | owner or `admin_paymentreq`, while pending | `payment-requests.js:278-331` |
| Mark paid / cancelled / bill | owner or approver, only on approved requests | `payment-requests.js:152-188` |
| Cards, delete a request | `admin_paymentreq` | `payment-requests.js:52-113` |
| Credit card statements: read | admin or `cc_viewer_ids` | `server.js:7735-7738` |
| Credit card statements: write | **admin role only** (there is no permission key for it) | `server.js:7739-7741` |

### 7.7 Other modules

| Module | Read | Write | Evidence |
|---|---|---|---|
| Inventory | `inventory` page; all items for admin, hod, `edit_inventory` | `edit_inventory`; delete by admin or `admin_inventory`; any staff may self-add | `inventory.js` |
| Meetings | own meetings in the list; **any staff for one meeting by id** | create: any staff; edit/cancel: organiser or `admin_meetings` | `meetings.js:29-310` |
| HR Portal | `hrm` page | `hrm_schedule`, `hrm_update_status`; delete `admin_hrm` | `hrm.js:909-1640` |
| FMS admin | admin | admin | `fms.js:27-255` |
| FMS tasks | `admin_fms_tasks`: all · others: FMSes where they are a doer | step done: doer of the step (steps with no doers are open) | `fms.js:257-500` |
| Leads | `leads` page | `edit_leads` | `leads.js:84-343` |
| Task Assistant | admin, hod, pc (hod scoped) | — (read-only) | `chatbot.js:1041` |
| Users | directory: any staff | admin | `server.js:3308-3530` |
| Logs (deleted records), restore, purge | admin | admin | `server.js:10122-10250` |
| Holidays | any staff | admin | `server.js:9678-9748` |
| Festival theme | any staff; the login page through a public route | `theme_admin_ids` only | `server.js:9596-9676` |
| WhatsApp task queue (`/api/wa-delegation`, `/api/mdo-tasks`) | admin | admin | `server.js:5258-5340`, `7954-8058` |

---

## 8. Impersonation ("View as Employee")

| Rule | Status | Evidence |
|---|---|---|
| Only a real admin can start it (or an admin already impersonating, to switch to another user). | Implemented | `server.js:2015-2023` |
| A client login cannot be impersonated. | Implemented | `server.js:2032` |
| The token carries the target's id. Every check, including the live role lookup, uses the **target's** role. The token lasts 1 day. | Implemented | `server.js:1056`, `2033-2038` |
| Stopping needs only the `impersonatedBy` marker, and returns a 7-day token for the original admin with that admin's **current** role. | Implemented | `server.js:2046-2062` |
| No audit record of impersonation start or stop was found. | Missing (inferred need: every other privileged change is logged in `task_activity` or `deleted_records`) | grep: no insert in `server.js:2015-2062` |

---

## 9. Contradictions and inconsistencies

| # | Finding | Status | Evidence |
|---|---|---|---|
| PC-1 | `requireAdminOrHod` admits PC, but its error says "Admin or HOD only". The PC role is easy to miss when reading routes. | Implemented | `server.js:1082-1085` |
| PC-2 | 17 action keys are offered in Access Control, but no route checks them (§4.2: `approve_revision`, `bulk_approve`, `delete_leave` and 14 `edit_<page>` keys). Granting or revoking them changes UI at most. | Implemented (comment says this is intended) | `server.js:3529-3532` |
| PC-3 | Saving anything in Access Control creates a `user_permissions` row, and from then on `extra_access` pages are ignored for that user. Access Control does not display `extra_access`. | Implemented | `server.js:3594-3608`, `3278-3306` |
| PC-4 | Changing `SERVER_ROLE_DEFAULTS` only reaches users who have no saved row. Others need a one-off backfill (as was done in `perm_task_buttons_v1`). | Implemented | `server.js:3601-3607`, `4941-4972` |
| PC-5 | `GET /api/users` hides `extra_access` from non-admins, but **still returns every user's `user_permissions`** to any staff login. The comment says the directory is sent "without the permissions". | Implemented (contradiction) | `server.js:3318-3328` versus `3337-3348` |
| PC-6 | `canApprovePayments` (tab visibility) uses list membership only, while the API also lets every admin through. An admin outside the list cannot see the tab but can call the API. | Implemented (documented in code) | `server.js:1966-1971`, `7848-7851` |
| PC-7 | `canReviewMdoTasks` shows the MDO tab to `mdo_reviewer_ids`, but `/api/mdo-tasks` is admin-only, so a non-admin reviewer's tab gets 403s. | Implemented (documented in code) | `server.js:1972-1979`, `7954-7965` |
| PC-8 | The PERM_TREE note for Credit Cards says access comes from "the CC_VIEWERS list in code". The server now uses `cc_viewer_ids` in `app_settings`. | Implemented (stale note) | `public/js/access.js:179-181` versus `server.js:7735-7738` |
| PC-9 | Every role default holds the `creditcards` page, but the server allows reads only to admin and `cc_viewer_ids`. | Implemented | `server.js:3564-3591` versus `7735-7738` |
| PC-10 | PC holds `create_task`, but its tasks are always self-assigned. It holds `edit_task` and `delete_task`, but `canModifyTask` does not include PC. Yet PC is fully privileged for status changes and approvals. | Implemented (inconsistent) | `tasks.js:201`, `786-792`, `601`; `server.js:2227-2229` |
| PC-11 | HOD can edit or delete any task company-wide, but sees only its own department and cannot change others' task status. | Implemented (inconsistent) | `tasks.js:786-788`, `81-91`, `601-609` |
| PC-12 | The PC team views (leave, transfers, approvals) are company-wide, while PC compliance and Employee 360 are department-scoped. | Implemented (inconsistent) | `server.js:9043-9045`, `8364-8374` |
| PC-13 | HOD's `dms` page is off by default, yet `requireClientEditor` lets HOD write to any client's DMS. | Implemented | `server.js:3564`, `1131-1134` |
| PC-14 | `edit_clients` (on by default for HOD) lets an HOD create or reset any client's portal login and password. | Implemented | `clients.js:695-726`, `server.js:3576` |
| PC-15 | The PATCH role endpoint stops an admin removing their own admin role. The full user edit (`PUT /api/users/:id`) does not. | Implemented (inconsistent) | `server.js:3668-3669` versus `3402-3413` |
| PC-16 | Meeting lists are scoped to organiser and attendees, but `GET /api/meetings/:id` is not. | Implemented (inconsistent) | `meetings.js:33-35` versus `92-122` |
| PC-17 | The DMS file list is open to any staff login, unlike every other DMS route. | Implemented (inconsistent) | `clients.js:895-899` |
| PC-18 | Inventory handover lets `hod` act on others' equipment, although the comment says "Admin by role". | Implemented (comment mismatch) | `inventory.js:253-258` |
| PC-19 | Several rights are tied to **production user ids** (6, 21, 31, 41, …). On any other database they point at different people. The code comment says so. | Implemented | `server.js:7816-7830`, `8927-8937` |
| PC-20 | Users can rename themselves (`PUT /api/profile`). Some remaining logic still matches on names: the feedback fallback, the `added_by` backfill, Top Performers exclusion, the Google Sheet row update and the Manpur button. | Implemented | `server.js:3695-3721`, `7859-7865`, `7319`, `3420-3428`; `clients.js:102`; `public/js/core.js:137-141` |
