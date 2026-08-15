// ══════════════════════════════════════════════════════
// E-Marketing Task Manager — Server
// Vercel-ready (serverless + local dev support)
// ══════════════════════════════════════════════════════
require('dotenv').config();
const express = require('express');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const mysql = require('mysql2/promise');
const path = require('path');
const nodemailer = require('nodemailer');
const multer = require('multer');
const XLSX = require('xlsx');

const app = express();
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.SESSION_SECRET || 'taskmanager_secret_2026';

const cookieParser = require('cookie-parser');
const helmet = require('helmet');
// ══════════════════════════════════════════════════════
// SECURITY HEADERS
// ══════════════════════════════════════════════════════
// Placed before express.static so the static files carry the headers too.
//
// Two of helmet's defaults are switched OFF deliberately, because leaving them
// on would break this app outright rather than protect it:
//
//   contentSecurityPolicy — app.html carries ~2,250 inline style="" attributes,
//     inline onclick handlers throughout, one inline <script>, plus three
//     <style> blocks, and it loads Chart.js and jsPDF from cdnjs and Inter from
//     Google Fonts. A default CSP blocks every one of those, so the page would
//     render unstyled and no button would work. A real CSP here is not a config
//     change, it is a refactor: the inline handlers have to become addEventListener
//     calls first. Worth doing, but not as a one-line "hardening" step.
//
//   crossOriginEmbedderPolicy — helmet's default (require-corp) blocks the cdnjs
//     and Google Fonts loads, which are exactly what this page needs.
//
// What stays on is the part that pays here: X-Frame-Options (helmet's default
// SAMEORIGIN), which stops the clickjacking case — another site framing this app
// and tricking a logged-in admin into clicking through it — plus nosniff, HSTS,
// Referrer-Policy: no-referrer, and removal of the X-Powered-By: Express banner.
//
// SAMEORIGIN rather than DENY on purpose. DENY was checked against the three
// offer-letter iframes first; all three are filled with srcdoc, never a
// same-origin src, so DENY would not break them today. It is left at SAMEORIGIN
// anyway because the threat DENY additionally covers requires an attacker to
// already be serving content from this origin, and SAMEORIGIN cannot break a
// legitimate same-origin iframe that someone adds later.
app.use(helmet({
  contentSecurityPolicy: false,
  crossOriginEmbedderPolicy: false,
}));

app.use(cookieParser());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ══════════════════════════════════════════════════════
// MIGRATION GATE — every /api request awaits in-flight schema migrations.
// On Vercel cold starts, fire-and-forget migration IIFEs may not complete
// before requests arrive. Without this, queries that reference newly-added
// columns (e.g. client_id) fail with "Unknown column" errors.
// On warm starts the promises are already resolved → near-zero overhead.
// ══════════════════════════════════════════════════════
// API responses must never be cached. Express sends an ETag by default, so the
// browser (and Vercel's edge) were serving GETs like /api/clients/:id/stats
// from cache via 304 — a value saved a moment earlier (e.g. whatsapp_group_id)
// then appeared to vanish on refresh because the stale cached body was shown.
// no-store forces every read to come fresh from the server.
app.use('/api', (req, res, next) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  next();
});

app.use('/api', async (req, res, next) => {
  try {
    await _startupMigrationsPromise;
    await _clientsTableMigrationsPromise;
  } catch (e) { /* migration failures are logged elsewhere — keep serving */ }
  next();
});

// Lazy meeting-reminder check — fires at most once every 5 min on any API hit.
// Works as long as someone is using the app during business hours.
let _lastReminderCheck = 0;
app.use('/api', (req, res, next) => {
  const now = Date.now();
  if (now - _lastReminderCheck > 5 * 60 * 1000) {
    _lastReminderCheck = now;
    sendMeetingReminders().catch(e => console.error('lazy reminder err:', e.message));
  }
  next();
});

// ══════════════════════════════════════════════════════
// MYSQL CONNECTION
// ══════════════════════════════════════════════════════
const dbConfig = {
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'emarketing_task_manager',
  port: Number(process.env.DB_PORT) || 3306,
  waitForConnections: true,
  // ⚠️ Shared hosting's max_user_connections is usually 5-10.
  // Vercel serverless spins up multiple function instances that all connect at
  // once, and every deployment (production + each preview) adds its own set. To
  // stay under the shared-host cap, each instance is capped at a single
  // connection; concurrent queries within one request just queue briefly.
  connectionLimit: Number(process.env.DB_POOL_SIZE) || 1,
  queueLimit: 0,
  connectTimeout: 30000,
  // Release idle connections quickly (mysql default is 8hrs, 30s is ideal here)
  idleTimeout: 30000,
  enableKeepAlive: false,
  // SSL support for cloud MySQL providers (Aiven, PlanetScale, Railway, etc.)
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
};

const _rawPool = mysql.createPool(dbConfig);

// Wrap pool with retry logic for "max_user_connections" errors
// Shared hosting keeps throwing this error when Vercel fires concurrent requests.
// Auto-retry helps recover gracefully without showing errors to users.
const db = {
  async query(sql, params) {
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await _rawPool.query(sql, params);
      } catch (err) {
        const isConnLimit = err.message && (
          err.message.includes('max_user_connections') ||
          err.message.includes('Too many connections') ||
          err.code === 'ER_USER_LIMIT_REACHED' ||
          err.code === 'ER_CON_COUNT_ERROR'
        );
        if (isConnLimit && attempt < maxRetries) {
          // Wait progressively longer before retry: 200ms, 500ms, 1000ms
          const wait = attempt * 250 + Math.random() * 250;
          console.warn(`  ⚠️ DB conn limit hit, retry ${attempt}/${maxRetries} after ${Math.round(wait)}ms`);
          await new Promise(r => setTimeout(r, wait));
          continue;
        }
        throw err;
      }
    }
  },
  // Pass-through for other methods (getConnection used by transactions)
  getConnection: (...args) => _rawPool.getConnection(...args),
  end: (...args) => _rawPool.end(...args),
};

// ══════════════════════════════════════════════════════
// COLD-START MIGRATION GUARD
// ══════════════════════════════════════════════════════
// The two migration blocks below issue ~155 statements one at a time, and the
// /api middleware awaits both before serving ANY request. On Vercel that toll
// was paid in full on every cold start — which is why a plain refresh stayed
// slow long after the deploy had finished, not just right after it.
//
// The marker is a HASH OF THIS FILE, not a hand-bumped version number and not
// a deployment id. Every migration statement lives in this file, so the hash
// changes precisely when the schema can have changed: edit server.js and the
// migrations replay once, then every later cold start skips them. There is
// nothing to remember when adding a table or a column — which is the whole
// point of not using a version constant. A Vercel deployment id was the other
// candidate and was rejected: it needs "expose System Environment Variables"
// switched on to exist at all, and any env var that turned out to be stable
// across deploys would skip a real migration. The file hash cannot be stale.
//
// It fails OPEN wherever it is unsure — file unreadable, no marker row, a
// marker that does not match, or any error whatsoever — and then behaves
// exactly as it did before. The worst case is the old speed, never a migration
// that quietly did not run.
//
// To force a full replay (e.g. after changing the schema by hand):
//   DELETE FROM app_settings WHERE key_name='schema_deploy_marker';
const _DEPLOY_ID = (() => {
  try {
    return require('crypto').createHash('sha1')
      .update(require('fs').readFileSync(__filename)).digest('hex');
  } catch (e) {
    return ''; // cannot read own source → never skip, migrate as before
  }
})();

// Byte-identical to the app_settings table created in the clients migration
// block, so this is a no-op once that has ever run. It has to be repeated here
// because the marker must be readable BEFORE either block may skip itself.
const _APP_SETTINGS_DDL = `CREATE TABLE IF NOT EXISTS app_settings (
    key_name  VARCHAR(100) PRIMARY KEY,
    value     TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`;

let _migrationsSkipped = false;
let _migrationGuardPromise = null;

// Memoised: both migration blocks ask, but this costs 2 queries per cold start,
// not 2 per caller.
function _migrationsAlreadyApplied() {
  if (!_migrationGuardPromise) _migrationGuardPromise = (async () => {
    if (!_DEPLOY_ID) return false;
    try {
      await db.query(_APP_SETTINGS_DDL);
      const [rows] = await db.query(
        `SELECT value FROM app_settings WHERE key_name='schema_deploy_marker'`);
      const applied = rows.length > 0 && rows[0].value === _DEPLOY_ID;
      if (applied) {
        _migrationsSkipped = true;
        console.log('  ⏩ Schema already migrated for this deploy — skipping ~155 statements');
      }
      return applied;
    } catch (e) {
      return false; // any doubt at all → migrate, exactly as before
    }
  })();
  return _migrationGuardPromise;
}

(async () => {
  try {
    await db.query('SELECT 1');
    console.log('  ✅ MySQL Connected Successfully!');
  } catch (err) {
    console.error('  ❌ MySQL Connection Failed:', err.message);
  }
})();

// ══════════════════════════════════════════════════════
// AUTO DB MIGRATIONS — runs on every server start
// Creates all tables + columns. Safe to re-run (uses IF NOT EXISTS / silent ALTER).
// On a fresh empty database, this gives you a fully working schema.
// ══════════════════════════════════════════════════════
// We capture the migration IIFE's promise so the request middleware can
// AWAIT it before serving any /api request. Vercel serverless cold starts
// otherwise begin handling requests while ALTERs are still in flight — which
// causes "Unknown column" errors for newly-added columns.
const _startupMigrationsPromise = (async () => {
  if (await _migrationsAlreadyApplied()) return;
  const sa = async (sql) => { try { await db.query(sql); } catch(e) { /* silent — column/table may already exist */ } };

  // ── Base tables (CREATE IF NOT EXISTS) ────────────────
  await sa(`CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    role ENUM('admin','hod','pc','user') DEFAULT 'user',
    phone VARCHAR(50) DEFAULT NULL,
    profile_image LONGTEXT DEFAULT NULL,
    exclude_from_reminder TINYINT(1) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // Failed-login counter, keyed by the email that was TRIED — including
  // addresses that belong to nobody. Counting those too is deliberate: locking
  // only real accounts would tell an attacker which emails exist.
  //
  // It lives in the database rather than in memory because this app runs
  // serverless. Each request can land on a different Vercel instance and
  // instances are torn down freely, so an in-process counter would reset
  // constantly and an attacker could simply keep trying until they hit a cold
  // one. The password-reset OTP already counts attempts this way.
  await sa(`CREATE TABLE IF NOT EXISTS login_attempts (
    email VARCHAR(255) NOT NULL PRIMARY KEY,
    attempts INT NOT NULL DEFAULT 0,
    locked_until DATETIME NULL DEFAULT NULL,
    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS delegation_tasks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    description TEXT NOT NULL,
    assigned_to INT NOT NULL,
    assigned_by INT NOT NULL,
    due_date DATE,
    status ENUM('pending','completed','revised') DEFAULT 'pending',
    priority ENUM('low','medium','high') DEFAULT 'low',
    approval ENUM('yes','no') DEFAULT 'no',
    waiting_approval TINYINT(1) DEFAULT 0,
    remarks TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_assigned_to (assigned_to),
    INDEX idx_status (status),
    INDEX idx_due_date (due_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS checklist_tasks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    description TEXT NOT NULL,
    assigned_to INT NOT NULL,
    assigned_by INT NOT NULL,
    due_date DATE,
    status ENUM('pending','completed') DEFAULT 'pending',
    priority ENUM('low','medium','high') DEFAULT 'low',
    remarks TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_assigned_to (assigned_to),
    INDEX idx_status (status),
    INDEX idx_due_date (due_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS task_approvals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id INT NOT NULL,
    task_type VARCHAR(20) NOT NULL,
    requested_by INT NOT NULL,
    requested_to INT NOT NULL,
    action_type VARCHAR(50) DEFAULT NULL,
    new_date DATE DEFAULT NULL,
    status ENUM('pending','approved','rejected') DEFAULT 'pending',
    note TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_task (task_id, task_type),
    INDEX idx_requested_to (requested_to)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS task_comments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id INT NOT NULL,
    task_type VARCHAR(20) NOT NULL,
    user_id INT NOT NULL,
    comment TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_task (task_id, task_type)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // Every status change on a task, with who did it and from where. Until this
  // existed, a task that went back to pending left no trace at all — reopening
  // clears completed_at — so "I marked it done and it came back" could not be
  // answered from the data. Append-only; nothing reads it during normal use.
  await sa(`CREATE TABLE IF NOT EXISTS task_activity (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id INT NOT NULL,
    task_type VARCHAR(20) NOT NULL,
    old_status VARCHAR(20) DEFAULT NULL,
    new_status VARCHAR(20) DEFAULT NULL,
    -- field/old_value/new_value carry the non-status changes: who the task is
    -- assigned to, and when it is due. The default is 'status' so the rows
    -- written before these columns existed stay correctly labelled — every one
    -- of them was a status change.
    field VARCHAR(20) NOT NULL DEFAULT 'status',
    old_value VARCHAR(120) DEFAULT NULL,
    new_value VARCHAR(120) DEFAULT NULL,
    changed_by INT DEFAULT NULL,
    source VARCHAR(40) DEFAULT NULL,
    note TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_task (task_id, task_type),
    INDEX idx_changed_by (changed_by),
    INDEX idx_created (created_at)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // The table already exists in production from d6f1a98, so CREATE IF NOT
  // EXISTS above will not add these three to it.
  await sa(`ALTER TABLE task_activity ADD COLUMN field VARCHAR(20) NOT NULL DEFAULT 'status' AFTER new_status`);
  await sa(`ALTER TABLE task_activity ADD COLUMN old_value VARCHAR(120) DEFAULT NULL AFTER field`);
  await sa(`ALTER TABLE task_activity ADD COLUMN new_value VARCHAR(120) DEFAULT NULL AFTER old_value`);

  await sa(`CREATE TABLE IF NOT EXISTS task_transfers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id INT NOT NULL,
    task_type VARCHAR(20) NOT NULL,
    from_user INT NOT NULL,
    to_user INT NOT NULL,
    requested_by INT NOT NULL,
    status ENUM('pending','approved','rejected') DEFAULT 'pending',
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS fms_sheets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sheet_name VARCHAR(255) NOT NULL,
    sheet_id VARCHAR(255) NOT NULL,
    header_row INT DEFAULT 1,
    total_steps INT DEFAULT 0,
    created_by INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS fms_steps (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fms_id INT NOT NULL,
    step_order INT NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    plan_col VARCHAR(10) DEFAULT '',
    actual_col VARCHAR(10) DEFAULT '',
    extra_input VARCHAR(10) DEFAULT 'no',
    extra_col VARCHAR(10) DEFAULT '',
    INDEX idx_fms (fms_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS fms_step_doers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    step_id INT NOT NULL,
    user_id INT NOT NULL,
    INDEX idx_step (step_id),
    INDEX idx_user (user_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS fms_extra_rows (
    id INT AUTO_INCREMENT PRIMARY KEY,
    step_id INT NOT NULL,
    row_label VARCHAR(255) DEFAULT '',
    INDEX idx_step (step_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS week_plans (
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT NOT NULL,
    hod_id INT,
    start_date DATE NOT NULL,
    target_count INT DEFAULT 0,
    improvement_pct DECIMAL(5,2) DEFAULT 0,
    user_committed_score DECIMAL(5,1) DEFAULT NULL,
    user_committed_at TIMESTAMP NULL DEFAULT NULL,
    checkin_skipped_until DATE DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_emp_week (employee_id, start_date),
    INDEX idx_employee (employee_id),
    INDEX idx_start (start_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // Migrate older deploys that lack these columns
  try { await sa(`ALTER TABLE week_plans ADD COLUMN user_committed_score DECIMAL(5,1) DEFAULT NULL`); } catch {}
  try { await sa(`ALTER TABLE week_plans ADD COLUMN user_committed_at TIMESTAMP NULL DEFAULT NULL`); } catch {}
  try { await sa(`ALTER TABLE week_plans ADD COLUMN checkin_skipped_until DATE DEFAULT NULL`); } catch {}

  await sa(`CREATE TABLE IF NOT EXISTS holidays (
    id INT AUTO_INCREMENT PRIMARY KEY,
    holiday_date DATE NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    created_by INT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date (holiday_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS leave_requests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    leave_type ENUM('full_day','half_day','work_from_home','extra_working') NOT NULL,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    dates_json TEXT DEFAULT NULL,
    reason TEXT NOT NULL,
    status ENUM('pending','approved','rejected') DEFAULT 'pending',
    approver_id INT DEFAULT NULL,
    approver_note TEXT DEFAULT NULL,
    decided_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_status (status),
    INDEX idx_approver (approver_id),
    INDEX idx_from (from_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  await sa(`ALTER TABLE leave_requests ADD COLUMN dates_json TEXT DEFAULT NULL AFTER to_date`);

  // ── Column additions (safe ALTERs from previous versions) ─────
  await sa(`ALTER TABLE fms_sheets ADD COLUMN fms_name VARCHAR(255) DEFAULT '' AFTER id`);
  await sa(`ALTER TABLE fms_steps ADD COLUMN show_cols TEXT AFTER extra_col`);
  await sa(`ALTER TABLE fms_steps ADD COLUMN delay_reason_col VARCHAR(10) DEFAULT '' AFTER show_cols`);
  await sa(`ALTER TABLE fms_steps ADD COLUMN doer_name_col VARCHAR(10) DEFAULT '' AFTER delay_reason_col`);
  await sa(`ALTER TABLE users ADD COLUMN department VARCHAR(255) DEFAULT '' AFTER phone`);
  await sa(`ALTER TABLE users ADD COLUMN week_off VARCHAR(50) DEFAULT '' AFTER department`);
  await sa(`ALTER TABLE users ADD COLUMN extra_off TEXT AFTER week_off`);
  await sa(`ALTER TABLE users ADD COLUMN notification_email VARCHAR(255) DEFAULT '' AFTER email`);
  await sa(`ALTER TABLE users ADD COLUMN exclude_from_reminder TINYINT(1) DEFAULT 0 AFTER extra_off`);
  await sa(`ALTER TABLE users ADD COLUMN extra_access TEXT DEFAULT NULL AFTER exclude_from_reminder`);
  // user_role — separate from app `role`. Decides leave-approval hierarchy
  // (e.g. an IT person may have app role 'admin' but user role 'user',
  // so their leave still goes to their HOD).
  await sa(`ALTER TABLE users ADD COLUMN user_role ENUM('admin','hod','pc','user') DEFAULT NULL AFTER role`);
  await sa(`UPDATE users SET user_role=role WHERE user_role IS NULL`);
  // Optional client tagging on tasks (uses clients table created later in this file)
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN client_id INT DEFAULT NULL AFTER remarks`);
  await sa(`ALTER TABLE delegation_tasks ADD INDEX idx_client (client_id)`);
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN url VARCHAR(2048) DEFAULT NULL AFTER client_id`);
  // Delegation where the doer sets their own due date (assigner doesn't know occupancy).
  // due_date stays NULL until the doer (or assigner) picks one; then this flips to 0.
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN awaiting_due_date TINYINT(1) DEFAULT 0 AFTER waiting_approval`);
  // Why this task still has no due date. The doer either picks a date or says
  // why they cannot yet — the Set Due Date modal asks for one or the other, and
  // the answer rides on the task row so it is visible to anyone who can see it.
  // A reason never substitutes for a date: completion still requires one.
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN no_due_date_reason TEXT DEFAULT NULL AFTER awaiting_due_date`);
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN no_due_date_reason_at TIMESTAMP NULL DEFAULT NULL AFTER no_due_date_reason`);
  // Optional clock time on the deadline. Only the handler→client flow sets it
  // (they commit the client to a date AND time); every internal task leaves it
  // NULL and keeps behaving as a date-only deadline.
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN due_time TIME DEFAULT NULL AFTER due_date`);
  // When the task was marked done. Needed to answer "what was closed today";
  // status alone cannot say when. Set on completion and cleared whenever the
  // task leaves 'completed', so a reopened task stops counting. Rows completed
  // before this column existed stay NULL and never appear in a daily digest.
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN completed_at DATETIME DEFAULT NULL AFTER status`);
  // What the handler still needs FROM the client to finish this task, and by
  // when. Kept apart from `remarks` on purpose: on a client-delegated task that
  // column already holds the CLIENT's own note from the portal form, and
  // overwriting it would destroy what they asked for.
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN client_ask TEXT DEFAULT NULL AFTER remarks`);
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN client_ask_by DATETIME DEFAULT NULL AFTER client_ask`);
  // Client portal delegation form offers an 'urgent' priority tier above 'high'.
  await sa(`ALTER TABLE delegation_tasks MODIFY COLUMN priority ENUM('low','medium','high','urgent') DEFAULT 'low'`);
  // Sub-tasks — follow-up asks nested under a delegation task (e.g. client says
  // "make a dashboard" then later "change its color") instead of a brand-new task.
  await sa(`CREATE TABLE IF NOT EXISTS task_subtasks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id INT NOT NULL,
    description TEXT NOT NULL,
    status ENUM('pending','completed') DEFAULT 'pending',
    priority ENUM('low','medium','high','urgent') DEFAULT 'low',
    created_by INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    INDEX idx_task (task_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // Handler's note on why a sub-task is stuck — almost always something the
  // client still owes. Surfaced in the client's pending digest so the delay is
  // recorded where the client can see it, instead of the handler explaining it.
  await sa(`ALTER TABLE task_subtasks ADD COLUMN remarks TEXT DEFAULT NULL AFTER description`);
  await sa(`ALTER TABLE task_subtasks ADD COLUMN priority ENUM('low','medium','high','urgent') DEFAULT 'low' AFTER description`);
  // Revise approval holds the requested new due-date here until the assigner approves.
  await sa(`ALTER TABLE task_approvals ADD COLUMN new_date DATE DEFAULT NULL AFTER action_type`);
  await sa(`ALTER TABLE checklist_tasks ADD COLUMN client_id INT DEFAULT NULL AFTER remarks`);
  await sa(`ALTER TABLE checklist_tasks ADD INDEX idx_client (client_id)`);
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN col_letter VARCHAR(10) DEFAULT '' AFTER row_label`);
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN field_type VARCHAR(20) DEFAULT 'text' AFTER col_letter`);
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN dropdown_options TEXT AFTER field_type`);
  // Required flag — default 1 so existing rows continue to be mandatory (backward compat)
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN required TINYINT(1) DEFAULT 1 AFTER dropdown_options`);
  // Add new handlers to Pre-Order FMS "Handle by Doer Name" dropdown
  await sa(`UPDATE fms_extra_rows SET dropdown_options = CONCAT(dropdown_options, ',Taran Jain,Rahul,Ashish Jha') WHERE row_label = 'Handle by Doer Name' AND dropdown_options NOT LIKE '%Taran Jain%'`);

  // ── Inventory tables ──────────────────────────────────
  await sa(`CREATE TABLE IF NOT EXISTS inventory_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type ENUM('laptop','keyboard','mouse','mobile','sim','charger','other') NOT NULL,
    brand VARCHAR(255) DEFAULT '',
    model VARCHAR(255) DEFAULT '',
    serial_number VARCHAR(255) DEFAULT '',
    photo LONGTEXT DEFAULT NULL,
    item_condition ENUM('new','good','fair','poor') DEFAULT 'good',
    status ENUM('available','assigned','damaged','retired') DEFAULT 'available',
    notes TEXT,
    created_by INT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_type (type)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS inventory_assignments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    item_id INT NOT NULL,
    user_id INT NOT NULL,
    assigned_by INT NOT NULL,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    returned_at TIMESTAMP NULL DEFAULT NULL,
    handover_status ENUM('active','pending_handover','returned') DEFAULT 'active',
    handover_notes TEXT,
    INDEX idx_item (item_id),
    INDEX idx_user (user_id),
    INDEX idx_handover (handover_status)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // Why an assignment ended. Drives the item's post-return status, so it is an
  // ENUM rather than free text: 'damaged'/'retired' take the item out of
  // circulation, 'offboarding' sends it back to available stock.
  await sa(`ALTER TABLE inventory_assignments
    ADD COLUMN return_reason ENUM('damaged','retired','offboarding') DEFAULT NULL AFTER handover_notes`);

  // ── Deleted-records archive ───────────────────────────
  // Every user-facing delete snapshots the row here before the hard DELETE,
  // so nothing ever leaves the database unrecoverably. Generic by design:
  // record_data holds the whole row as JSON, since each source table has its
  // own columns.
  await sa(`CREATE TABLE IF NOT EXISTS deleted_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_table VARCHAR(64) NOT NULL,
    record_id INT DEFAULT NULL,
    record_data LONGTEXT NOT NULL,
    summary VARCHAR(255) DEFAULT '',
    deleted_by INT DEFAULT NULL,
    deleted_by_name VARCHAR(255) DEFAULT '',
    deleted_by_role VARCHAR(20) DEFAULT '',
    deleted_via VARCHAR(120) DEFAULT '',
    delete_reason TEXT,
    restored_at TIMESTAMP NULL DEFAULT NULL,
    restored_by INT DEFAULT NULL,
    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_source (source_table, record_id),
    INDEX idx_deleted_by (deleted_by),
    INDEX idx_deleted_at (deleted_at)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // ── HRM tables ────────────────────────────────────────
  await sa(`CREATE TABLE IF NOT EXISTS hrm_candidates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    phone VARCHAR(50) NOT NULL,
    profile_position VARCHAR(255) DEFAULT '',
    interview_date DATE DEFAULT NULL,
    interview_time VARCHAR(20) DEFAULT '',
    status ENUM('Scheduled','Rescheduled','Selected','Rejected','Offer Sent') DEFAULT 'Scheduled',
    reschedule_date DATE DEFAULT NULL,
    reschedule_time VARCHAR(20) DEFAULT '',
    reschedule_reason TEXT,
    joining_date DATE DEFAULT NULL,
    offer_sent TINYINT(1) DEFAULT 0,
    salary VARCHAR(100) DEFAULT '',
    notes TEXT,
    meeting_link VARCHAR(1024) DEFAULT '',
    interviewer_phone VARCHAR(50) DEFAULT '',
    created_by INT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_interview_date (interview_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS hrm_message_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    candidate_id INT DEFAULT NULL,
    candidate_name VARCHAR(255) DEFAULT '',
    phone VARCHAR(50) DEFAULT '',
    action VARCHAR(255) DEFAULT '',
    type ENUM('text','image','file') DEFAULT 'text',
    status ENUM('Sent','Failed') DEFAULT 'Failed',
    error_detail TEXT,
    payload_json LONGTEXT,
    retry_count INT DEFAULT 0,
    last_retry_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_candidate (candidate_id),
    INDEX idx_status (status)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // Add columns to existing installs (no "IF NOT EXISTS" — invalid syntax on
  // MySQL 5.7, which silently no-ops the whole ALTER via sa()'s catch-all;
  // sa() already makes these idempotent, so plain ADD COLUMN is correct here)
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN reschedule_reason TEXT`);
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN offer_drive_id VARCHAR(500) DEFAULT NULL`);
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN offer_token VARCHAR(64) DEFAULT NULL`);
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN offer_html MEDIUMTEXT DEFAULT NULL`);
  await sa(`ALTER TABLE hrm_message_log ADD COLUMN deleted_at TIMESTAMP NULL DEFAULT NULL`);
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN department VARCHAR(255) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_candidates MODIFY COLUMN status ENUM('Scheduled','Rescheduled','Selected','Rejected','Offer Sent','Offer Letter Sent') DEFAULT 'Scheduled'`);
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN final_offer_drive_id VARCHAR(500) DEFAULT NULL`);
  // Live-preview final offer: token addresses the public PDF endpoint; data is a
  // JSON snapshot of the HR-approved letter fields so the PDF renders statelessly.
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN final_offer_token VARCHAR(64) DEFAULT NULL`);
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN final_offer_data MEDIUMTEXT DEFAULT NULL`);
  // Preliminary offer: its own token/snapshot (separate from final_offer_* so a
  // later final send can't overwrite the still-live preliminary PDF link).
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN prelim_offer_token VARCHAR(64) DEFAULT NULL`);
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN prelim_offer_data MEDIUMTEXT DEFAULT NULL`);
  // Interviewer's email — the department HOD chosen on the Schedule Interview
  // form. The interview-scheduled notification email goes here.
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN interviewer_email VARCHAR(255) DEFAULT ''`);
  // Candidate email — captured on the Schedule Interview form, used to email
  // the offer letter PDF (optional; WhatsApp remains the default channel).
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN email VARCHAR(255) DEFAULT ''`);
  // Joining-details form: the token is what the candidate's form link carries,
  // and is how the Apps Script submission is mapped back to the candidate.
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN joining_form_token VARCHAR(64) DEFAULT NULL`);
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN joining_form_sent_at TIMESTAMP NULL DEFAULT NULL`);

  // One row per candidate — the basic details the candidate submits before any
  // offer letter goes out. Filled by the public Apps Script webhook, never by
  // hand. UNIQUE(candidate_id) makes the webhook an idempotent upsert, so a
  // re-submission corrects the row instead of duplicating it.
  await sa(`CREATE TABLE IF NOT EXISTS hrm_joining_details (
    id INT AUTO_INCREMENT PRIMARY KEY,
    candidate_id INT NOT NULL,
    full_name VARCHAR(255) DEFAULT '',
    emp_mobile VARCHAR(20) DEFAULT '',
    email VARCHAR(255) DEFAULT '',
    guardian1_name VARCHAR(255) DEFAULT '',
    guardian1_relation VARCHAR(100) DEFAULT '',
    guardian1_mobile VARCHAR(20) DEFAULT '',
    guardian2_name VARCHAR(255) DEFAULT '',
    guardian2_relation VARCHAR(100) DEFAULT '',
    guardian2_mobile VARCHAR(20) DEFAULT '',
    dob DATE DEFAULT NULL,
    street VARCHAR(500) DEFAULT '',
    city VARCHAR(255) DEFAULT '',
    state VARCHAR(255) DEFAULT '',
    pincode VARCHAR(20) DEFAULT '',
    aadhaar_no VARCHAR(20) DEFAULT '',
    pan_no VARCHAR(20) DEFAULT '',
    resume_file_url VARCHAR(1024) DEFAULT '',
    -- One PDF, or two images (front + back) — hence a second URL per document.
    aadhaar_file_url VARCHAR(1024) DEFAULT '',
    aadhaar_file_url_2 VARCHAR(1024) DEFAULT '',
    pan_file_url VARCHAR(1024) DEFAULT '',
    pan_file_url_2 VARCHAR(1024) DEFAULT '',
    raw_payload LONGTEXT DEFAULT NULL,
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_candidate (candidate_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // The table above shipped before these columns existed, and CREATE TABLE IF
  // NOT EXISTS no-ops once the table is there — so every column added after the
  // first deploy needs its own ALTER, or the webhook 500s with "Unknown column".
  // sa() swallows the duplicate-column error, so re-running is harmless.
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN emp_mobile VARCHAR(20) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN email VARCHAR(255) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN guardian1_name VARCHAR(255) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN guardian1_relation VARCHAR(100) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN guardian1_mobile VARCHAR(20) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN guardian2_name VARCHAR(255) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN guardian2_relation VARCHAR(100) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN guardian2_mobile VARCHAR(20) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN street VARCHAR(500) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN city VARCHAR(255) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN state VARCHAR(255) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN pincode VARCHAR(20) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN resume_file_url VARCHAR(1024) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN aadhaar_file_url_2 VARCHAR(1024) DEFAULT ''`);
  await sa(`ALTER TABLE hrm_joining_details ADD COLUMN pan_file_url_2 VARCHAR(1024) DEFAULT ''`);

  // Per-user permissions column (replaces role_permissions)
  await sa(`ALTER TABLE users ADD COLUMN user_permissions TEXT DEFAULT NULL AFTER extra_access`);
  await sa(`ALTER TABLE users ADD COLUMN birthday DATE DEFAULT NULL`);
  await sa(`ALTER TABLE users ADD COLUMN joining_date DATE DEFAULT NULL`);

  // WhatsApp bot delegation — approval queue before tasks reach the main table
  // CREATE TABLE safely; if user already created it with different columns, ALTER statements below will fill the gaps
  await sa(`CREATE TABLE IF NOT EXISTS tasks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    description TEXT NOT NULL,
    status ENUM('pending','approved','denied') DEFAULT 'pending',
    approval_token VARCHAR(64) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // Ensure all required columns exist — no IF NOT EXISTS (MySQL 5.7 compat)
  // sa() silently swallows "Duplicate column name" if column already exists
  await sa(`ALTER TABLE tasks ADD COLUMN description TEXT DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD COLUMN assigned_to INT DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD COLUMN assigned_by INT DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD COLUMN sender_phone VARCHAR(20) DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD COLUMN sender_name VARCHAR(255) DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD COLUMN due_date DATE DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD COLUMN priority ENUM('low','medium','high') DEFAULT 'low'`);
  await sa(`ALTER TABLE tasks ADD COLUMN remarks TEXT`);
  await sa(`ALTER TABLE tasks ADD COLUMN client_id INT DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD COLUMN url VARCHAR(2048) DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD COLUMN approved_task_id INT DEFAULT NULL`);
  await sa(`ALTER TABLE tasks ADD INDEX idx_status (status)`);
  await sa(`ALTER TABLE tasks ADD UNIQUE INDEX idx_token (approval_token)`);

  console.log('  ✅ DB migrations checked');

  // ── Auto-seed default admin if no users exist ─────────
  try {
    const [[{ cnt }]] = await db.query('SELECT COUNT(*) AS cnt FROM users');
    if (cnt === 0) {
      const hash = bcrypt.hashSync('password', 10);
      await db.query(
        'INSERT INTO users (name, email, password, role, department) VALUES (?,?,?,?,?)',
        ['Simran Admin', 'aman@test.com', hash, 'admin', 'Management']
      );
      console.log('  🌱 Default admin seeded → aman@test.com / password');
    }
  } catch (e) {
    console.error('  ⚠️ Admin seed skipped:', e.message);
  }
})();

// ══════════════════════════════════════════════════════
// EMAIL CONFIGURATION (Gmail SMTP via Nodemailer)
// ══════════════════════════════════════════════════════
const mailTransporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS
  }
});

(async () => {
  try {
    if (process.env.SMTP_USER && process.env.SMTP_PASS) {
      await mailTransporter.verify();
      console.log('  ✅ Gmail SMTP Ready');
    } else {
      console.log('  ⚠️  SMTP credentials missing — emails disabled');
    }
  } catch (err) {
    console.error('  ❌ SMTP verification failed:', err.message);
  }
})();

// Reusable email sender — never throws (failures are logged only)
// opts.attachments (optional) is passed straight to nodemailer, e.g.
// [{ filename: 'x.pdf', content: <Buffer>, contentType: 'application/pdf' }].
// Returns true on success so callers that care (e.g. the offer-email endpoint)
// can report/throw; existing fire-and-forget callers can ignore it.
async function sendMail(to, subject, html, opts = {}) {
  if (!to || !process.env.SMTP_USER) return false;
  try {
    await mailTransporter.sendMail({
      from: `"${process.env.SMTP_FROM_NAME || 'E-Marketing Task Manager'}" <${process.env.SMTP_USER}>`,
      to, subject, html,
      ...(opts.cc ? { cc: opts.cc } : {}),
      ...(opts.attachments ? { attachments: opts.attachments } : {})
    });
    console.log(`  📧 Email sent to ${to} — ${subject}`);
    return true;
  } catch (err) {
    console.error(`  ❌ Email failed (${to}):`, err.message);
    return false;
  }
}

// Helper: get user's notification email + name. Falls back to the login email
// when no separate notification_email is set — otherwise users without one
// silently got no delegation/leave emails at all.
async function getNotifyTarget(userId) {
  try {
    const [rows] = await db.query(
      'SELECT name, email, notification_email FROM users WHERE id=? LIMIT 1',
      [userId]
    );
    const u = rows[0];
    const email = u && (u.notification_email || u.email);
    if (!email) return null;
    return { name: u.name, email };
  } catch { return null; }
}

// ── WhatsApp → Email migration helpers ───────────────────────────────
// The team asked for every personal-number notification to arrive by email
// instead of WhatsApp (only the HR onboarding email-ID notice and the client/
// team GROUP messages stay on WhatsApp). Rather than hand-write an HTML
// template per message, these turn the existing WhatsApp text into a simple
// email so the wording stays identical.
function waTextToEmailHtml(text) {
  const esc = s => String(s == null ? '' : s).replace(/[&<>]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;' }[c]));
  const body = esc(text)
    .replace(/\*([^*\n]+)\*/g, '<strong>$1</strong>')  // *bold* → <strong>
    .replace(/\n/g, '<br>');
  return `<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111;line-height:1.6">${body}</div>`;
}
// Email a WhatsApp-style message to a user resolved by id. Best-effort; returns
// true if it went out. Safe to call fire-and-forget.
async function emailUserWaText(userId, subject, text) {
  const t = await getNotifyTarget(userId);
  if (!t?.email) return false;
  return sendMail(t.email, subject, waTextToEmailHtml(text));
}
// Resolve a notification email from a phone number (bot senders are known only
// by phone). Tries the number as stored and without a leading 91. Null if no
// matching user — the caller then falls back to WhatsApp.
async function emailForPhone(phone) {
  if (!phone) return null;
  const p = String(phone).trim();
  const alt = p.replace(/^\+?91/, '');
  try {
    const [rows] = await db.query(
      'SELECT email, notification_email FROM users WHERE phone=? OR phone=? OR phone=? LIMIT 1',
      [p, alt, '91' + alt]);
    const u = rows[0];
    return u ? (u.notification_email || u.email) : null;
  } catch { return null; }
}
// Notify a WhatsApp-bot sender on BOTH channels: always WhatsApp, plus email if
// their phone matches a registered user (a truly external sender has no email).
async function notifyBotSender(phone, subject, text) {
  if (!phone) return;
  await sendWhatsApp(phone, text).catch(() => {});
  const email = await emailForPhone(phone);
  if (email) await sendMail(email, subject, waTextToEmailHtml(text)).catch(() => {});
}

// (delegationEmailHtml removed — the delegation email now uses the plain
// WhatsApp-style text via waTextToEmailHtml, with no "Open Task Manager" button.)

// ══════════════════════════════════════════════════════
// MIDDLEWARE
// ══════════════════════════════════════════════════════
function requireAuth(req, res, next) {
  const token = req.cookies?.token || req.headers['authorization']?.replace('Bearer ','');
  if (!token) return res.status(401).json({ error: 'Not authenticated' });
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.session = {
      userId: decoded.userId, role: decoded.role, name: decoded.name,
      // Set only while an admin is viewing the app AS another user.
      impersonatedBy: decoded.impersonatedBy || null,
      impersonatorName: decoded.impersonatorName || null
    };
    next();
  } catch(e) { res.status(401).json({ error: 'Invalid token' }); }
}
function requireAdmin(req, res, next) {
  if (req.session.role === 'admin') return next();
  res.status(403).json({ error: 'Admin only' });
}
function requireAdminOrHod(req, res, next) {
  if (req.session.role === 'admin' || req.session.role === 'hod' || req.session.role === 'pc') return next();
  res.status(403).json({ error: 'Admin or HOD only' });
}
function requireAdminOrHodOnly(req, res, next) {
  if (req.session.role === 'admin' || req.session.role === 'hod') return next();
  res.status(403).json({ error: 'Admin or HOD (App Role) only' });
}
// Named so Compliance's three read routes carry their gate in the signature
// rather than three copies of the same check. There is no matching editor
// guard because Compliance has no write routes at all — see the readOnly flag
// on its PERM_TREE entry.
// MIS is narrow-only ON PURPOSE: it keeps the admin/hod floor and ANDs the page
// key onto it, so an admin's revoke finally reaches the API while a grant can
// never widen the audience. Three hazards make the bare canSee() form wrong
// here, and all three are closed by keeping the floor:
//   - a stale extra_access 'mis' tick would go live the moment the key alone
//     decided access;
//   - department scoping keys on ROLE, not permission — misHodMaySee returns
//     true for any non-hod and the list routes filter only when role==='hod' —
//     so the first pc or user ever granted 'mis' would see EVERY department,
//     strictly wider than any hod;
//   - /api/dashboard/activity feeds the same section and stays admin/hod, so a
//     matching audience keeps the dashboard coherent.
// Whether Race Tracker / MIS should become grantable at all is a product
// decision that has not been taken; this shape leaves it open either way.
async function requireMisViewer(req, res, next) {
  const role = req.session.role;
  if ((role === 'admin' || role === 'hod') && await userCanSee(req.session, 'mis')) return next();
  res.status(403).json({ error: 'No access to MIS Report' });
}
async function requireComplianceViewer(req, res, next) {
  if (await userCanSee(req.session, 'compliance')) return next();
  res.status(403).json({ error: 'No access to Compliance' });
}
// Middleware form of userCanDo('edit_clients'), so the Client Master write
// routes read the same as the ones that still use requireAdmin — a name in the
// signature rather than a check buried three lines into the body.
async function requireClientsEditor(req, res, next) {
  if (await userCanDo(req.session, 'edit_clients')) return next();
  res.status(403).json({ error: 'You do not have edit access to Client Master' });
}
function requireAdminOrPC(req, res, next) {
  if (req.session.role === 'admin' || req.session.role === 'pc') return next();
  res.status(403).json({ error: 'Admin or PC only' });
}
// Allows a manager (admin/hod/pc) OR a handler of the client named in :id to
// act on that client's operational data (portal links, WhatsApp group, DMS).
// Structural client edits (name/handler) stay manager-only — see PUT.
async function requireClientEditor(req, res, next) {
  try {
    if (['admin', 'hod', 'pc'].includes(req.session.role)) return next();
    const [[c]] = await db.query('SELECT id, handler_id FROM clients WHERE id=?', [req.params.id]);
    if (c && await isHandlerOf(req.session.userId, c)) return next();
    return res.status(403).json({ error: 'Forbidden' });
  } catch (err) { return res.status(500).json({ error: err.message }); }
}
function getTable(type) {
  return type === 'delegation' ? 'delegation_tasks' : 'checklist_tasks';
}

// The same task, for the same doer, from the same assigner, for the same client
// and the same date, created seconds ago is never a second delegation — it is a
// double tap or a retried request. Production held groups of four identical
// tasks written within one second, and the doer had to close every copy before
// the task left their list. Returns the existing row's id so the caller can
// answer with it instead of inserting another.
//
// Client and date are part of the match on purpose: the same wording for two
// different clients, or two different dates, is real work. `<=>` rather than `=`
// so a NULL client or a task still awaiting its due date matches as well.
//
// Lives here, outside POST /api/tasks, because that route branches on task type
// and the guard was written inside the delegation arm — leaving checklist
// creation unprotected against exactly the same double tap.
async function findRecentDuplicateTask(table, { desc, assignedTo, assignedBy, clientId, dueDate }) {
  const [[row]] = await db.query(
    `SELECT id FROM ${table}
      WHERE description=? AND assigned_to=? AND assigned_by=?
        AND client_id <=> ? AND due_date <=> ?
        AND created_at >= DATE_SUB(NOW(), INTERVAL 2 MINUTE)
      ORDER BY id DESC LIMIT 1`,
    [desc, assignedTo, assignedBy, clientId, dueDate]);
  return row ? row.id : null;
}

// Append a row to task_activity. Never throws and never blocks the caller —
// an audit write failing must not stop a user from marking their task done.
function logTaskActivity({ taskId, taskType, field, oldStatus, newStatus, oldValue, newValue, changedBy, source, note }) {
  const f = field || 'status';
  // A status row fills old_value/new_value too, so one query can read the whole
  // history of a task without caring which kind of change each row is.
  const ov = oldValue !== undefined ? oldValue : (f === 'status' ? oldStatus : null);
  const nv = newValue !== undefined ? newValue : (f === 'status' ? newStatus : null);
  db.query(
    `INSERT INTO task_activity (task_id, task_type, old_status, new_status, field, old_value, new_value, changed_by, source, note)
     VALUES (?,?,?,?,?,?,?,?,?,?)`,
    [taskId, taskType || 'delegation', oldStatus || null, newStatus || null,
     f, ov == null ? null : String(ov), nv == null ? null : String(nv),
     changedBy || null, source || null, note || null]
  ).catch(e => console.error('task_activity log err:', e.message));
}

// ══════════════════════════════════════════════════════
// DELETE ARCHIVE
// ══════════════════════════════════════════════════════
// Snapshot rows into deleted_records before a hard DELETE removes them.
//
// Call this BEFORE the DELETE and let it throw: if the archive write fails we
// must NOT go on to delete the rows, or the data is gone with no copy. A
// silently-failing safety net is worse than none, so callers should leave this
// un-caught and let their route's own catch turn it into a 500.
//
// `rows` = the full row object(s) about to be deleted (SELECT them first).
// `opts.summary` = string, or fn(row) -> string, for a readable one-liner.
async function archiveDeleted(sourceTable, rows, req, opts = {}) {
  const list = (Array.isArray(rows) ? rows : [rows]).filter(Boolean);
  if (!list.length) return;

  const via = (opts.via || `${req?.method || ''} ${req?.originalUrl || ''}`).trim().slice(0, 120);
  const actorId   = req?.session?.userId ?? null;
  const actorName = String(req?.session?.name || '').slice(0, 255);
  const actorRole = String(req?.session?.role || '').slice(0, 20);

  const values = list.map(row => {
    let summary = '';
    try {
      summary = typeof opts.summary === 'function' ? opts.summary(row) : (opts.summary || '');
    } catch (e) { summary = ''; }
    return [
      sourceTable,
      row?.id ?? null,
      JSON.stringify(row),
      String(summary || '').slice(0, 255),
      actorId, actorName, actorRole, via,
      opts.reason || null,
    ];
  });

  await db.query(
    `INSERT INTO deleted_records
       (source_table, record_id, record_data, summary,
        deleted_by, deleted_by_name, deleted_by_role, deleted_via, delete_reason)
     VALUES ?`,
    [values]);
}

// ══════════════════════════════════════════════════════
// GOOGLE SHEETS HELPERS
// ══════════════════════════════════════════════════════
let _sheetsReadClient = null;
let _sheetsWriteClient = null;

async function getSheetsClient(scopes) {
  const { google } = require('googleapis');
  let creds;
  if (process.env.GOOGLE_CREDENTIALS) {
    creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  } else {
    // Local-dev fallback — credentials.json file (gitignored, never committed)
    try {
      creds = require('./credentials.json');
    } catch (e) {
      throw new Error('Google credentials missing — set GOOGLE_CREDENTIALS env var (or place credentials.json locally for dev)');
    }
  }
  const isWrite = scopes.some(s => !s.includes('readonly'));
  if (isWrite) {
    if (_sheetsWriteClient) return _sheetsWriteClient;
    const auth = new google.auth.GoogleAuth({ credentials: creds, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    _sheetsWriteClient = google.sheets({ version: 'v4', auth: await auth.getClient() });
    return _sheetsWriteClient;
  } else {
    if (_sheetsReadClient) return _sheetsReadClient;
    const auth = new google.auth.GoogleAuth({ credentials: creds, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    _sheetsReadClient = google.sheets({ version: 'v4', auth: await auth.getClient() });
    return _sheetsReadClient;
  }
}

// Pre-warm Google auth on startup (reduces cold start time)
(async () => {
  try {
    await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    console.log('  ✅ Google Auth pre-warmed');
  } catch(e) { console.log('  ⚠️ Google Auth pre-warm failed:', e.message); }
})();

// ══════════════════════════════════════════════════════
// GOOGLE DRIVE HELPERS (DMS) — same service account as Sheets/Calendar
// (GOOGLE_CREDENTIALS). The root DMS folder must be shared with that
// service account's client_email (Editor) so it can create/read files
// inside it — no separate OAuth consent flow needed.
// ══════════════════════════════════════════════════════
let _driveClient = null;

function _dmsCreds() {
  if (process.env.GOOGLE_CREDENTIALS) return JSON.parse(process.env.GOOGLE_CREDENTIALS);
  // Local-dev fallback — credentials.json file (gitignored, never committed)
  try { return require('./credentials.json'); }
  catch (e) { throw new Error('Google credentials missing — set GOOGLE_CREDENTIALS env var (or place credentials.json locally for dev)'); }
}

async function getDriveClient() {
  if (_driveClient) return _driveClient;
  const { google } = require('googleapis');
  const auth = new google.auth.GoogleAuth({ credentials: _dmsCreds(), scopes: ['https://www.googleapis.com/auth/drive'] });
  _driveClient = google.drive({ version: 'v3', auth: await auth.getClient() });
  return _driveClient;
}

// Vercel serverless functions cap request bodies at ~4.5MB, well under what
// people actually want to upload here. A resumable-upload session lets the
// BROWSER send the file bytes straight to Google — our server only ever
// handles the small JSON init/complete calls, never the file itself.
async function dmsInitiateResumableUpload(name, mimeType, size, parentId) {
  const { google } = require('googleapis');
  const auth = new google.auth.GoogleAuth({ credentials: _dmsCreds(), scopes: ['https://www.googleapis.com/auth/drive'] });
  const client = await auth.getClient();
  const { token } = await client.getAccessToken();
  const fetchFn = global.fetch || (await import('node-fetch')).default;
  const initUrl = 'https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true&fields=id,name,webViewLink,mimeType,modifiedTime,size';
  const r = await fetchFn(initUrl, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json; charset=UTF-8',
      'X-Upload-Content-Type': mimeType || 'application/octet-stream',
      'X-Upload-Content-Length': String(size),
    },
    body: JSON.stringify({ name, parents: [parentId] }),
  });
  if (!r.ok) throw new Error(`Drive resumable-init failed: ${r.status} ${await r.text()}`);
  const uploadUrl = r.headers.get('location');
  if (!uploadUrl) throw new Error('Drive did not return an upload URL');
  return uploadUrl;
}

function _dmsServiceAccountEmail() {
  return _dmsCreds().client_email;
}

// External-link pseudo-files get opened via window.open() and rendered as a
// clickable row, so only allow http/https to block javascript:/data: URI XSS.
function _dmsIsSafeUrl(u) {
  try { const p = new URL(u); return p.protocol === 'http:' || p.protocol === 'https:'; } catch { return false; }
}

// Records who did what through the app (Drive itself only ever sees our
// single shared service account, so this is the only per-user attribution
// available for app-driven creates/renames/deletes).
async function _dmsLogActivity(fileId, action, fileName, req, clientId) {
  try {
    await db.query(
      'INSERT INTO dms_file_activity (file_id, client_id, action, file_name, user_id, user_name) VALUES (?,?,?,?,?,?)',
      [fileId, clientId || null, action, fileName || null, req.session.userId, req.session.name || '']
    );
  } catch (e) { console.error('DMS activity log failed:', e.message); }
}

async function dmsCreateFolder(name, parentId) {
  const drive = await getDriveClient();
  const res = await drive.files.create({
    requestBody: {
      name,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId],
    },
    fields: 'id,webViewLink',
    supportsAllDrives: true,
  });
  return res.data; // { id, webViewLink }
}

async function dmsShareFolder(folderId, email, role = 'writer') {
  const drive = await getDriveClient();
  await drive.permissions.create({
    fileId: folderId,
    requestBody: { type: 'user', role, emailAddress: email },
    sendNotificationEmail: false,
    supportsAllDrives: true,
  });
}

async function dmsListFiles(folderId) {
  const drive = await getDriveClient();
  // Drive caps a single list() response at 1000 items and won't paginate on
  // its own — loop through nextPageToken so folders with 100+ children (like
  // the DMS root, one per client) don't silently drop the tail alphabetically.
  const files = [];
  let pageToken;
  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = false`,
      fields: 'nextPageToken,files(id,name,mimeType,webViewLink,modifiedTime,thumbnailLink,iconLink,size,lastModifyingUser(displayName,emailAddress,permissionId))',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      orderBy: 'folder,name',
      pageSize: 1000,
      pageToken,
    });
    files.push(...(res.data.files || []));
    pageToken = res.data.nextPageToken;
  } while (pageToken);
  if (!files.length) return files;

  // Drive withholds emailAddress on lastModifyingUser for accounts outside
  // our service account's own domain (privacy visibility rules) — only
  // displayName ("mis2") comes through. The folder's own permissions list
  // isn't subject to that restriction, so look up the full email there by
  // matching permissionId, for any modifier missing one.
  const needsEmailLookup = files.some(f => f.lastModifyingUser && !f.lastModifyingUser.emailAddress && f.lastModifyingUser.permissionId);
  if (needsEmailLookup) {
    try {
      const perms = await drive.permissions.list({
        fileId: folderId, supportsAllDrives: true, fields: 'permissions(id,emailAddress)',
      });
      const emailByPermId = Object.fromEntries(
        (perms.data.permissions || []).filter(p => p.emailAddress).map(p => [p.id, p.emailAddress])
      );
      for (const f of files) {
        if (f.lastModifyingUser && !f.lastModifyingUser.emailAddress) {
          const email = emailByPermId[f.lastModifyingUser.permissionId];
          if (email) f.lastModifyingUser.emailAddress = email;
        }
      }
    } catch (e) { console.error('DMS permissions lookup failed:', e.message); }
  }

  // For files whose last change came through the app (Drive reports our
  // shared service account as the editor), resolve the real app user from
  // our own activity log instead of showing the generic service-account name.
  const svcEmail = _dmsServiceAccountEmail();
  const appEditedIds = files
    .filter(f => f.lastModifyingUser?.emailAddress === svcEmail)
    .map(f => f.id);
  let latestByFile = {};
  if (appEditedIds.length) {
    const [rows] = await db.query(
      `SELECT file_id, action, user_name, created_at FROM dms_file_activity
       WHERE file_id IN (${appEditedIds.map(()=>'?').join(',')})
       ORDER BY created_at DESC`,
      appEditedIds
    ).catch(() => [[]]);
    for (const r of rows) { if (!latestByFile[r.file_id]) latestByFile[r.file_id] = r; }
  }
  for (const f of files) {
    const log = latestByFile[f.id];
    if (log) {
      f.modified_by = log.user_name;
      f.modified_via = 'app';
    } else if (f.lastModifyingUser) {
      const email = f.lastModifyingUser.emailAddress || f.lastModifyingUser.displayName;
      // Our own service account with no matching activity-log entry (e.g. a
      // folder created before per-user attribution existed) — we genuinely
      // don't know which staff member did this, so leave it blank instead
      // of showing the internal bot's technical email.
      f.modified_by = email === svcEmail ? null : email; // Full email (not the short Drive display name) for direct-Drive edits
      f.modified_via = f.lastModifyingUser.emailAddress === svcEmail ? 'app' : 'drive';
    }
    delete f.lastModifyingUser;
  }
  return files;
}

const DMS_MIME_TYPES = {
  doc: 'application/vnd.google-apps.document',
  sheet: 'application/vnd.google-apps.spreadsheet',
  slide: 'application/vnd.google-apps.presentation',
};

async function dmsCreateFile(name, kind, parentId) {
  const mimeType = DMS_MIME_TYPES[kind];
  if (!mimeType) throw new Error('Invalid kind — use doc, sheet, or slide');
  const drive = await getDriveClient();
  const res = await drive.files.create({
    requestBody: { name, mimeType, parents: [parentId] },
    fields: 'id,webViewLink',
    supportsAllDrives: true,
  });
  return res.data; // { id, webViewLink }
}

async function dmsUploadFile(name, mimeType, buffer, parentId) {
  const { Readable } = require('stream');
  const drive = await getDriveClient();
  const res = await drive.files.create({
    requestBody: { name, parents: [parentId] },
    media: { mimeType: mimeType || 'application/octet-stream', body: Readable.from(buffer) },
    fields: 'id,webViewLink',
    supportsAllDrives: true,
  });
  return res.data;
}

function extractSpreadsheetId(raw) {
  const s = (raw || '').trim();
  const m = s.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
  return m ? m[1] : s;
}

function colToIdx(col) {
  if (!col) return -1;
  col = col.toUpperCase().trim();
  let idx = 0;
  for (let i = 0; i < col.length; i++) idx = idx * 26 + (col.charCodeAt(i) - 64);
  return idx - 1;
}

function idxToCol(idx) {
  let s = '', n = idx + 1;
  while (n > 0) { const r = (n-1) % 26; s = String.fromCharCode(65+r) + s; n = Math.floor((n-1)/26); }
  return s;
}

// ══════════════════════════════════════════════════════
// AUTH
// ══════════════════════════════════════════════════════
// ══════════════════════════════════════════════════════
// 🛠️ SETUP ENDPOINT — Forces migrations + admin seed on demand
// Visit: /api/setup in browser to manually trigger
// Useful when auto-migrations on startup fail silently
// ══════════════════════════════════════════════════════
app.get('/api/setup', async (req, res) => {
  const secret = process.env.SETUP_SECRET;
  if (!secret || req.query.secret !== secret) return res.status(403).json({ error: 'Forbidden — set SETUP_SECRET env and pass ?secret=...' });
  const log = [];
  const sa = async (sql, label) => {
    try { await db.query(sql); log.push(`✅ ${label}`); }
    catch(e) { log.push(`⚠️ ${label} — ${e.code || e.message}`); }
  };

  try {
    // Test connection first
    await db.query('SELECT 1');
    log.push('✅ DB connection OK');

    // ── Create base tables ────────────────────────────
    await sa(`CREATE TABLE IF NOT EXISTS users (
      id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL,
      email VARCHAR(255) NOT NULL UNIQUE, notification_email VARCHAR(255) DEFAULT '',
      password VARCHAR(255) NOT NULL, role ENUM('admin','hod','pc','user') DEFAULT 'user',
      user_role ENUM('admin','hod','pc','user') DEFAULT NULL,
      phone VARCHAR(50) DEFAULT NULL, department VARCHAR(255) DEFAULT '',
      week_off VARCHAR(50) DEFAULT '', extra_off TEXT,
      exclude_from_reminder TINYINT(1) DEFAULT 0,
      profile_image LONGTEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'users table');
    await sa(`ALTER TABLE users ADD COLUMN user_role ENUM('admin','hod','pc','user') DEFAULT NULL AFTER role`, 'users.user_role');
    await sa(`ALTER TABLE users ADD COLUMN extra_off TEXT AFTER week_off`, 'users.extra_off');
    await sa(`ALTER TABLE users ADD COLUMN exclude_from_reminder TINYINT(1) DEFAULT 0 AFTER extra_off`, 'users.exclude_from_reminder');
    await sa(`ALTER TABLE users ADD COLUMN extra_access TEXT DEFAULT NULL AFTER exclude_from_reminder`, 'users.extra_access');
    // Forgot-password OTP (team members only). Hash of the 6-digit code, its
    // expiry, and a failed-attempt counter so a code can't be brute-forced.
    await sa(`ALTER TABLE users ADD COLUMN reset_otp_hash VARCHAR(255) DEFAULT NULL`, 'users.reset_otp_hash');
    await sa(`ALTER TABLE users ADD COLUMN reset_otp_expires DATETIME DEFAULT NULL`, 'users.reset_otp_expires');
    await sa(`ALTER TABLE users ADD COLUMN reset_otp_attempts INT DEFAULT 0`, 'users.reset_otp_attempts');
    await sa(`UPDATE users SET user_role=role WHERE user_role IS NULL`, 'backfill user_role from role');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN client_id INT DEFAULT NULL AFTER remarks`, 'delegation_tasks.client_id');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN url VARCHAR(2048) DEFAULT NULL AFTER client_id`, 'delegation_tasks.url');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN awaiting_due_date TINYINT(1) DEFAULT 0 AFTER waiting_approval`, 'delegation_tasks.awaiting_due_date');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN no_due_date_reason TEXT DEFAULT NULL AFTER awaiting_due_date`, 'delegation_tasks.no_due_date_reason');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN no_due_date_reason_at TIMESTAMP NULL DEFAULT NULL AFTER no_due_date_reason`, 'delegation_tasks.no_due_date_reason_at');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN due_time TIME DEFAULT NULL AFTER due_date`, 'delegation_tasks.due_time');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN completed_at DATETIME DEFAULT NULL AFTER status`, 'delegation_tasks.completed_at');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN client_ask TEXT DEFAULT NULL AFTER remarks`, 'delegation_tasks.client_ask');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN client_ask_by DATETIME DEFAULT NULL AFTER client_ask`, 'delegation_tasks.client_ask_by');
    await sa(`ALTER TABLE checklist_tasks ADD COLUMN client_id INT DEFAULT NULL AFTER remarks`, 'checklist_tasks.client_id');

    await sa(`CREATE TABLE IF NOT EXISTS delegation_tasks (
      id INT AUTO_INCREMENT PRIMARY KEY, description TEXT NOT NULL,
      assigned_to INT NOT NULL, assigned_by INT NOT NULL, due_date DATE,
      status ENUM('pending','completed','revised') DEFAULT 'pending',
      priority ENUM('low','medium','high') DEFAULT 'low',
      approval ENUM('yes','no') DEFAULT 'no', waiting_approval TINYINT(1) DEFAULT 0,
      remarks TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_assigned_to (assigned_to), INDEX idx_status (status), INDEX idx_due_date (due_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'delegation_tasks table');

    await sa(`CREATE TABLE IF NOT EXISTS checklist_tasks (
      id INT AUTO_INCREMENT PRIMARY KEY, description TEXT NOT NULL,
      assigned_to INT NOT NULL, assigned_by INT NOT NULL, due_date DATE,
      status ENUM('pending','completed') DEFAULT 'pending',
      priority ENUM('low','medium','high') DEFAULT 'low', remarks TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_assigned_to (assigned_to), INDEX idx_status (status), INDEX idx_due_date (due_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'checklist_tasks table');

    await sa(`CREATE TABLE IF NOT EXISTS task_approvals (
      id INT AUTO_INCREMENT PRIMARY KEY, task_id INT NOT NULL, task_type VARCHAR(20) NOT NULL,
      requested_by INT NOT NULL, requested_to INT NOT NULL, action_type VARCHAR(50),
      new_date DATE DEFAULT NULL,
      status ENUM('pending','approved','rejected') DEFAULT 'pending', note TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_task (task_id, task_type), INDEX idx_requested_to (requested_to)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'task_approvals table');

    await sa(`CREATE TABLE IF NOT EXISTS task_comments (
      id INT AUTO_INCREMENT PRIMARY KEY, task_id INT NOT NULL, task_type VARCHAR(20) NOT NULL,
      user_id INT NOT NULL, comment TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_task (task_id, task_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'task_comments table');

    await sa(`CREATE TABLE IF NOT EXISTS task_transfers (
      id INT AUTO_INCREMENT PRIMARY KEY, task_id INT NOT NULL, task_type VARCHAR(20) NOT NULL,
      from_user INT NOT NULL, to_user INT NOT NULL, requested_by INT NOT NULL,
      status ENUM('pending','approved','rejected') DEFAULT 'pending', note TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'task_transfers table');

    await sa(`CREATE TABLE IF NOT EXISTS fms_sheets (
      id INT AUTO_INCREMENT PRIMARY KEY, fms_name VARCHAR(255) DEFAULT '',
      sheet_name VARCHAR(255) NOT NULL, sheet_id VARCHAR(255) NOT NULL,
      header_row INT DEFAULT 1, total_steps INT DEFAULT 0, created_by INT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'fms_sheets table');

    await sa(`CREATE TABLE IF NOT EXISTS fms_steps (
      id INT AUTO_INCREMENT PRIMARY KEY, fms_id INT NOT NULL, step_order INT NOT NULL,
      step_name VARCHAR(255) NOT NULL, plan_col VARCHAR(10) DEFAULT '',
      actual_col VARCHAR(10) DEFAULT '', extra_input VARCHAR(10) DEFAULT 'no',
      extra_col VARCHAR(10) DEFAULT '', show_cols TEXT,
      delay_reason_col VARCHAR(10) DEFAULT '', doer_name_col VARCHAR(10) DEFAULT '',
      INDEX idx_fms (fms_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'fms_steps table');

    await sa(`CREATE TABLE IF NOT EXISTS fms_step_doers (
      id INT AUTO_INCREMENT PRIMARY KEY, step_id INT NOT NULL, user_id INT NOT NULL,
      INDEX idx_step (step_id), INDEX idx_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'fms_step_doers table');

    await sa(`CREATE TABLE IF NOT EXISTS fms_extra_rows (
      id INT AUTO_INCREMENT PRIMARY KEY, step_id INT NOT NULL,
      row_label VARCHAR(255) DEFAULT '', col_letter VARCHAR(10) DEFAULT '',
      field_type VARCHAR(20) DEFAULT 'text', dropdown_options TEXT,
      required TINYINT(1) DEFAULT 1,
      INDEX idx_step (step_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'fms_extra_rows table');
    await sa(`ALTER TABLE fms_extra_rows ADD COLUMN required TINYINT(1) DEFAULT 1 AFTER dropdown_options`, 'fms_extra_rows.required');

    await sa(`CREATE TABLE IF NOT EXISTS week_plans (
      id INT AUTO_INCREMENT PRIMARY KEY, employee_id INT NOT NULL, hod_id INT,
      start_date DATE NOT NULL, target_count INT DEFAULT 0,
      improvement_pct DECIMAL(5,2) DEFAULT 0,
      user_committed_score DECIMAL(5,1) DEFAULT NULL,
      user_committed_at TIMESTAMP NULL DEFAULT NULL,
      checkin_skipped_until DATE DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_emp_week (employee_id, start_date),
      INDEX idx_employee (employee_id), INDEX idx_start (start_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'week_plans table');
    await sa(`ALTER TABLE week_plans ADD COLUMN user_committed_score DECIMAL(5,1) DEFAULT NULL`, 'week_plans.user_committed_score');
    await sa(`ALTER TABLE week_plans ADD COLUMN user_committed_at TIMESTAMP NULL DEFAULT NULL`, 'week_plans.user_committed_at');
    await sa(`ALTER TABLE week_plans ADD COLUMN checkin_skipped_until DATE DEFAULT NULL`, 'week_plans.checkin_skipped_until');

    await sa(`CREATE TABLE IF NOT EXISTS holidays (
      id INT AUTO_INCREMENT PRIMARY KEY, holiday_date DATE NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL, created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_date (holiday_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'holidays table');

    await sa(`CREATE TABLE IF NOT EXISTS leave_requests (
      id INT AUTO_INCREMENT PRIMARY KEY, user_id INT NOT NULL,
      leave_type ENUM('full_day','half_day','work_from_home','extra_working') NOT NULL,
      from_date DATE NOT NULL, to_date DATE NOT NULL, dates_json TEXT DEFAULT NULL,
      reason TEXT NOT NULL,
      status ENUM('pending','approved','rejected') DEFAULT 'pending',
      approver_id INT DEFAULT NULL, approver_note TEXT DEFAULT NULL,
      decided_at TIMESTAMP NULL DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_user (user_id), INDEX idx_status (status),
      INDEX idx_approver (approver_id), INDEX idx_from (from_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'leave_requests table');
    await sa(`ALTER TABLE leave_requests ADD COLUMN dates_json TEXT DEFAULT NULL AFTER to_date`, 'leave_requests.dates_json');

    // ── Seed admin user ────────────────────────────────
    try {
      const [[{ cnt }]] = await db.query('SELECT COUNT(*) AS cnt FROM users WHERE email=?', ['aman@test.com']);
      if (cnt === 0) {
        const hash = bcrypt.hashSync('password', 10);
        await db.query(
          'INSERT INTO users (name, email, password, role, department) VALUES (?,?,?,?,?)',
          ['Simran Admin', 'aman@test.com', hash, 'admin', 'Management']
        );
        log.push('🌱 Admin user seeded: aman@test.com / password');
      } else {
        log.push('ℹ️ Admin user already exists');
      }
    } catch(e) {
      log.push(`❌ Admin seed failed: ${e.message}`);
    }

    res.send(`
      <html><head><title>Setup Complete</title>
      <style>body{font-family:monospace;background:#1a1a1a;color:#0f0;padding:30px;line-height:1.6;}
      h2{color:#F39C12;}a{color:#F39C12;}</style></head>
      <body>
      <h2>🎯 E-Marketing Task Manager — Setup</h2>
      <pre>${log.join('\n')}</pre>
      <hr>
      <p>✅ Setup done! Now <a href="/">click here to login</a></p>
      <p style="color:#aaa;font-size:12px;">Login: aman@test.com / password</p>
      </body></html>
    `);
  } catch (err) {
    res.status(500).send(`
      <html><body style="font-family:monospace;background:#1a1a1a;color:#f55;padding:30px;">
      <h2 style="color:#f55;">❌ Setup Failed</h2>
      <pre>${err.message}\n\nLogs so far:\n${log.join('\n')}</pre>
      </body></html>
    `);
  }
});

// ── Failed-login throttle ────────────────────────────────────────────────
// Ten wrong passwords for one email buys a fifteen-minute pause. Deliberately
// lenient: someone fat-fingering their own password a few times, or a shared
// machine where two people try the wrong account, must never be locked out —
// this is here to stop a script trying thousands, not to punish typing.
//
// Everything below FAILS OPEN. If any of these queries throws, login proceeds
// as if the throttle did not exist. A database hiccup must not lock the whole
// company out of their own task manager; the worst case of failing open is
// that a brute-force window stays open a little longer.
const LOGIN_MAX_ATTEMPTS = 10;
const LOGIN_LOCK_MINUTES = 15;
// Attempts older than the lock window are stale and start again from zero, so
// one wrong password a week never accumulates into a lockout.
const LOGIN_ATTEMPT_WINDOW_MINUTES = 15;

const loginKey = email => String(email || '').trim().toLowerCase();

// Returns minutes remaining if this email is currently locked, else 0.
async function loginLockRemaining(email) {
  const key = loginKey(email);
  if (!key) return 0;
  try {
    const [[row]] = await db.query(
      `SELECT locked_until, TIMESTAMPDIFF(MINUTE, NOW(), locked_until) AS mins
         FROM login_attempts WHERE email=?`, [key]);
    if (!row || !row.locked_until) return 0;
    return row.mins > 0 ? row.mins : 0;
  } catch (e) { return 0; }
}

async function noteLoginFailure(email) {
  const key = loginKey(email);
  if (!key) return;
  try {
    // One statement, so two simultaneous attempts cannot both read "9" and both
    // write "10". The first CASE restarts the count when the previous attempt
    // has aged out of the window.
    //
    // ⚠️ The second assignment reads `attempts`, NOT `attempts + 1`. MySQL
    // evaluates ON DUPLICATE KEY UPDATE assignments left to right, so by the
    // time locked_until is computed, attempts already holds the value just
    // written. Writing `attempts + 1` here counts the same failure twice and
    // trips the lock one attempt early — which is exactly what the test caught.
    await db.query(
      `INSERT INTO login_attempts (email, attempts, last_attempt)
       VALUES (?, 1, NOW())
       ON DUPLICATE KEY UPDATE
         attempts = CASE WHEN last_attempt < NOW() - INTERVAL ? MINUTE
                         THEN 1 ELSE attempts + 1 END,
         locked_until = CASE WHEN attempts >= ?
                             THEN NOW() + INTERVAL ? MINUTE ELSE locked_until END,
         last_attempt = NOW()`,
      [key, LOGIN_ATTEMPT_WINDOW_MINUTES, LOGIN_MAX_ATTEMPTS, LOGIN_LOCK_MINUTES]);
  } catch (e) { /* fail open — see the note above */ }
}

async function clearLoginFailures(email) {
  const key = loginKey(email);
  if (!key) return;
  try { await db.query('DELETE FROM login_attempts WHERE email=?', [key]); }
  catch (e) { /* fail open */ }
}

app.post('/api/login', async (req, res) => {
  try {
    const { email, password } = req.body;

    const lockedFor = await loginLockRemaining(email);
    if (lockedFor > 0) {
      return res.status(429).json({
        error: `Too many failed attempts. Try again in ${lockedFor} minute${lockedFor === 1 ? '' : 's'}.`,
      });
    }

    const [rows] = await db.query('SELECT * FROM users WHERE email = ?', [email]);
    const user = rows[0];
    if (!user || !bcrypt.compareSync(password, user.password)) {
      await noteLoginFailure(email);
      return res.status(401).json({ error: 'Invalid email or password' });
    }
    await clearLoginFailures(email);

    // Issue JWT token
    const token = jwt.sign(
      { userId: user.id, role: user.role, name: user.name },
      JWT_SECRET,
      { expiresIn: '7d' }
    );
    const isProduction = process.env.NODE_ENV === 'production';
    res.cookie('token', token, {
      httpOnly: true,
      secure: isProduction,
      sameSite: 'lax',
      maxAge: 7 * 24 * 60 * 60 * 1000
    });
    res.json({ id: user.id, name: user.name, email: user.email, role: user.role, token });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/logout', (req, res) => {
  res.clearCookie('token');
  res.json({ success: true });
});

// ── Forgot password (team members only) ──────────────────────────────
// A user who forgets their password gets a 6-digit code emailed to their login
// address, then sets a new password with it. Client logins (role='client') are
// deliberately excluded — this is for internal team accounts only.
//
// Security posture: the response is always the same generic success whether or
// not the email exists, so the endpoint can't be used to discover which emails
// are registered. The code is stored only as a bcrypt hash, expires in 10
// minutes, is single-use, and is locked after 5 wrong tries.
const RESET_OTP_TTL_MIN = 10;
const RESET_OTP_MAX_ATTEMPTS = 5;

// The reset columns are also added by the cold-start migration, but a request
// can hit a fresh serverless instance (or a DB the migration never reached)
// before that runs — which surfaced as "Unknown column 'reset_otp_hash'".
// Ensure them lazily on first use: one ALTER per process, then a no-op. Same
// self-heal pattern the client-credentials table uses.
let _resetColsReady = null;
function ensureResetColumns() {
  if (!_resetColsReady) {
    _resetColsReady = (async () => {
      const add = async sql => { try { await db.query(sql); } catch (e) { /* duplicate column — already there */ } };
      await add('ALTER TABLE users ADD COLUMN reset_otp_hash VARCHAR(255) DEFAULT NULL');
      await add('ALTER TABLE users ADD COLUMN reset_otp_expires DATETIME DEFAULT NULL');
      await add('ALTER TABLE users ADD COLUMN reset_otp_attempts INT DEFAULT 0');
    })().catch(e => { _resetColsReady = null; throw e; });
  }
  return _resetColsReady;
}

app.post('/api/forgot-password', async (req, res) => {
  try {
    await ensureResetColumns();
    const email = String(req.body.email || '').trim().toLowerCase();
    const generic = { ok: true, message: 'If that email belongs to a team account, a code has been sent to it.' };
    if (!email) return res.status(400).json({ error: 'Email is required' });
    // Team members only — never client logins.
    const [rows] = await db.query(
      `SELECT id, name, email FROM users WHERE LOWER(email)=? AND role<>'client' LIMIT 1`, [email]);
    const user = rows[0];
    if (!user) return res.json(generic);  // same response — don't reveal existence

    const otp = String(require('crypto').randomInt(0, 1000000)).padStart(6, '0');
    const hash = bcrypt.hashSync(otp, 10);
    const expires = new Date(Date.now() + RESET_OTP_TTL_MIN * 60 * 1000);
    await db.query(
      'UPDATE users SET reset_otp_hash=?, reset_otp_expires=?, reset_otp_attempts=0 WHERE id=?',
      [hash, expires, user.id]);

    const html = `<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111;line-height:1.6">
      <p>Hi ${user.name || 'there'},</p>
      <p>Use this code to reset your E-Marketing Task Manager password:</p>
      <p style="font-size:26px;font-weight:800;letter-spacing:4px;color:#4f46e5;margin:16px 0">${otp}</p>
      <p>This code expires in ${RESET_OTP_TTL_MIN} minutes and can be used once. If you didn't request this, you can ignore this email — your password stays unchanged.</p>
      <p style="color:#777;font-size:12px;margin-top:20px">E-Marketing Task Manager</p>
    </div>`;
    await sendMail(user.email, 'Your password reset code', html);
    res.json(generic);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/reset-password', async (req, res) => {
  try {
    await ensureResetColumns();
    const email = String(req.body.email || '').trim().toLowerCase();
    const otp = String(req.body.otp || '').trim();
    const newPassword = String(req.body.newPassword || '');
    if (!email || !otp || !newPassword) return res.status(400).json({ error: 'Email, code and new password are all required' });
    if (newPassword.length < 6) return res.status(400).json({ error: 'New password must be at least 6 characters' });

    const [rows] = await db.query(
      `SELECT id, reset_otp_hash, reset_otp_expires, reset_otp_attempts FROM users WHERE LOWER(email)=? AND role<>'client' LIMIT 1`, [email]);
    const user = rows[0];
    if (!user || !user.reset_otp_hash) return res.status(400).json({ error: 'No reset in progress — request a new code' });
    if (new Date(user.reset_otp_expires).getTime() < Date.now()) {
      await db.query('UPDATE users SET reset_otp_hash=NULL, reset_otp_expires=NULL, reset_otp_attempts=0 WHERE id=?', [user.id]);
      return res.status(400).json({ error: 'That code has expired — request a new one' });
    }
    if ((user.reset_otp_attempts || 0) >= RESET_OTP_MAX_ATTEMPTS) {
      await db.query('UPDATE users SET reset_otp_hash=NULL, reset_otp_expires=NULL, reset_otp_attempts=0 WHERE id=?', [user.id]);
      return res.status(400).json({ error: 'Too many wrong attempts — request a new code' });
    }
    if (!bcrypt.compareSync(otp, user.reset_otp_hash)) {
      await db.query('UPDATE users SET reset_otp_attempts=reset_otp_attempts+1 WHERE id=?', [user.id]);
      return res.status(400).json({ error: 'Incorrect code' });
    }
    // Success — set the new password and clear the OTP so it can't be reused.
    await db.query(
      'UPDATE users SET password=?, reset_otp_hash=NULL, reset_otp_expires=NULL, reset_otp_attempts=0 WHERE id=?',
      [bcrypt.hashSync(newPassword, 10), user.id]);
    res.json({ ok: true, message: 'Password updated — you can now sign in with your new password.' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Leave-report viewing is now driven by per-user extra_access (granted via the
// admin Users UI), so anyone with 'leaves_all' ticked gets the full team view.
function isLeaveReportViewer(user) {
  if (!user) return false;
  if (user.role === 'admin') return true;
  const access = Array.isArray(user.extra_access)
    ? user.extra_access
    : parseExtraAccess(user.extra_access);
  return access.includes('leaves_all');
}

app.get('/api/me', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT id,name,email,notification_email,role,
              COALESCE(user_role, role) AS user_role,
              phone,profile_image,department,week_off,extra_access
       FROM users WHERE id=?`, [req.session.userId]);
    if (!rows[0]) return res.status(404).json({ error: 'User not found' });
    // extra_off + user_permissions fetched separately — safe if columns not yet added
    try {
      const [ex] = await db.query('SELECT extra_off FROM users WHERE id=?', [req.session.userId]);
      rows[0].extra_off = ex[0]?.extra_off || '';
    } catch(e) { rows[0].extra_off = ''; }
    try {
      const [up] = await db.query('SELECT user_permissions FROM users WHERE id=?', [req.session.userId]);
      const raw = up[0]?.user_permissions;
      rows[0].user_permissions = raw ? (() => { try { return JSON.parse(raw); } catch { return null; } })() : null;
    } catch(e) { rows[0].user_permissions = null; }
    // The resolved answer to "what can this person reach", worked out once, here.
    // canSee() / canDo() in app.html used to re-run the whole cascade in the
    // browser off a second copy of the role defaults — two implementations of
    // one rule, in two files, kept in step by hand. Every drift between them
    // surfaced as the UI offering something the API then refused, which is the
    // shape of most of the bugs found on 2026-08-11. The client no longer
    // decides; it asks.
    //
    // `all` is admin, kept as a flag rather than an expanded list so it keeps
    // meaning "everything, including pages nobody has enumerated yet" — exactly
    // what the old `role === 'admin'` short-circuit meant. Clients never reach
    // getEffectivePerms (no role defaults exist for them), so their one page is
    // named here; without it the client portal would resolve to an empty set.
    try {
      if (rows[0].role === 'client') {
        rows[0].can = { all: false, pages: ['client-portal'], actions: [] };
      } else {
        const eff = await getEffectivePerms(req.session);
        rows[0].can = eff === 'all'
          ? { all: true,  pages: [], actions: [] }
          : { all: false, pages: eff.pages || [], actions: eff.actions || [] };
      }
    } catch (e) {
      // Fail closed. A missing `can` leaves canSee()/canDo() returning false
      // rather than falling back to a guess the server never made.
      rows[0].can = null;
    }
    try {
      const [bd] = await db.query('SELECT birthday, joining_date FROM users WHERE id=?', [req.session.userId]);
      rows[0].birthday = bd[0]?.birthday || null;
      rows[0].joining_date = bd[0]?.joining_date || null;
    } catch(e) { rows[0].birthday = null; rows[0].joining_date = null; }
    rows[0].extra_access = parseExtraAccess(rows[0].extra_access);
    rows[0].canViewAllLeaves = isLeaveReportViewer(rows[0]);
    // The UI used to decide these by comparing ME.name against a copy of the
    // approver list. It now asks the server, so the names live in one place.
    try {
      // List membership only — NOT isPaymentApprover(), which lets any admin
      // through. That admin bypass is right for the routes, because the old
      // name check sat behind `role !== 'admin' &&` and admins always passed.
      // It is wrong here: the tab was gated on the name list alone, so an admin
      // outside that list never saw it. Sending the bypassed value put the
      // Payment Approvals tab in front of every admin in the company.
      rows[0].canApprovePayments = (await readIdSetting(PR_APPROVER_KEY)).includes(Number(req.session.userId));
      // canSettlePayments used to be sent from here. Nothing in the client ever
      // read it, and the route it described was never called — see the note on
      // the removed settler machinery near PEOPLE_SETTINGS.
      // Same reasoning as above: this replaces a UI check that read
      // ME.name === 'Purvi Saini', so admins did not see the MDO tab either.
      // Note the /api/mdo-tasks routes are admin-only, so a reviewer who is not
      // an admin gets a tab whose every request 403s — true before this change
      // too, and not something to fix by widening access on a guess.
      rows[0].canReviewMdoTasks  = (await readIdSetting('mdo_reviewer_ids')).includes(Number(req.session.userId));
      rows[0].canViewCreditCards = await canViewCreditCards(req.session);
    } catch (e) {
      rows[0].canApprovePayments = false;
      rows[0].canReviewMdoTasks  = false; rows[0].canViewCreditCards = false;
    }
    // When an admin is "viewing as" this user, expose who's really behind the wheel
    // so the UI can show an exit-impersonation banner.
    rows[0].impersonatedBy = req.session.impersonatedBy || null;
    rows[0].impersonatorName = req.session.impersonatorName || null;
    res.json(rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── View as Employee (impersonation) ──────────────────────────────
// Admin picks a user and gets a fresh token scoped to THAT user, so the whole
// app renders exactly what the employee sees. The token carries impersonatedBy
// so we can revert and so the UI can show a banner.
function setAuthCookie(res, token) {
  res.cookie('token', token, {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: 7 * 24 * 60 * 60 * 1000
  });
}

app.post('/api/admin/impersonate', requireAuth, async (req, res) => {
  try {
    // Allowed for a real admin, or for an admin who is already impersonating
    // (so they can hop straight from one user's dashboard to another).
    const realRole = req.session.impersonatedBy ? 'admin' : req.session.role;
    if (realRole !== 'admin') {
      return res.status(403).json({ error: 'Admin only' });
    }
    // The true admin behind the wheel — stays constant across hops so "exit"
    // always returns to the real admin, never a previously-viewed user.
    const adminId   = req.session.impersonatedBy   || req.session.userId;
    const adminName = req.session.impersonatorName || req.session.name;
    const targetId = parseInt(req.body.userId, 10);
    if (!targetId) return res.status(400).json({ error: 'userId required' });
    const [rows] = await db.query('SELECT id, name, role FROM users WHERE id=?', [targetId]);
    const target = rows[0];
    if (!target) return res.status(404).json({ error: 'User not found' });
    if (target.role === 'client') return res.status(400).json({ error: 'Cannot view as a client login' });
    const token = jwt.sign(
      { userId: target.id, role: target.role, name: target.name,
        impersonatedBy: adminId, impersonatorName: adminName },
      JWT_SECRET,
      { expiresIn: '1d' }
    );
    setAuthCookie(res, token);
    res.json({ ok: true, name: target.name, role: target.role });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Stop impersonation — only auth required (the effective role is the employee's,
// so requireAdmin would block it). Guarded by the impersonatedBy marker.
app.post('/api/admin/stop-impersonate', requireAuth, async (req, res) => {
  try {
    const adminId = req.session.impersonatedBy;
    if (!adminId) return res.status(400).json({ error: 'Not impersonating' });
    const [rows] = await db.query('SELECT id, name, role FROM users WHERE id=?', [adminId]);
    const admin = rows[0];
    if (!admin) return res.status(404).json({ error: 'Original user not found' });
    const token = jwt.sign(
      { userId: admin.id, role: admin.role, name: admin.name },
      JWT_SECRET,
      { expiresIn: '7d' }
    );
    setAuthCookie(res, token);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// DASHBOARD
// ══════════════════════════════════════════════════════
app.get('/api/dashboard', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const role = req.session.role;
    const isAdmin = role === 'admin' || role === 'pc';
    const isHod = role === 'hod';
    const isPC = role === 'pc';
    const filterEmployee = req.query.employee;
    const hodDept = req.query.hodDept || '';
    // PC date range filter — default to today if not provided
    const dateFrom = req.query.dateFrom || '';
    const dateTo   = req.query.dateTo   || '';

    let userFilter, params;

    if (isAdmin && filterEmployee && filterEmployee !== 'all') {
      userFilter = 'AND t.assigned_to = ?'; params = [filterEmployee];
    } else if (isAdmin) {
      userFilter = ''; params = [];
    } else if (isHod) {
      // HOD cannot drill into a single employee — they always see their whole
      // department's aggregate. Only admin (and PC) may switch the view, so the
      // `employee` query param is intentionally ignored here.
      // Fetch the HOD's department from the DB — don't rely on the query param
      let resolvedDept = hodDept;
      if (!resolvedDept) {
        const [meRow] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
        resolvedDept = meRow[0]?.department || '';
      }
      if (!resolvedDept) {
        // No department set — fall back to just their own tasks
        userFilter = 'AND t.assigned_to = ?'; params = [uid];
      } else {
        const [deptUsers] = await db.query('SELECT id FROM users WHERE department=? AND role NOT IN (?,?)', [resolvedDept, 'admin','hod']);
        if (!deptUsers.length) {
          // No users in this department — fall back to just their own tasks
          userFilter = 'AND t.assigned_to = ?'; params = [uid];
        } else {
          const ids = deptUsers.map(u=>u.id);
          // Include the HOD themselves too
          if (!ids.includes(uid)) ids.push(uid);
          userFilter = `AND t.assigned_to IN (${ids.map(()=>'?').join(',')})`;
          params = ids;
        }
      }
    } else {
      userFilter = 'AND t.assigned_to = ?'; params = [uid];
    }

    // Stats + Table:
    //   Delegation: ALL dates (matches FMS — no date cap, show all pending)
    //   Checklist : today + next 10 days (upcoming visibility for recurring tasks)
    // PC: if a date range was provided, use it (overrides both)
    const dateRe = /^\d{4}-\d{2}-\d{2}$/;
    const validDates = dateFrom && dateTo && dateRe.test(dateFrom) && dateRe.test(dateTo);
    const usingPCRange = isPC && validDates;
    const delDateClause = usingPCRange ? 'AND t.due_date BETWEEN ? AND ?' : '';
    const chlDateClause = usingPCRange ? 'AND t.due_date BETWEEN ? AND ?' : 'AND t.due_date <= LAST_DAY(CURDATE())';
    const delParams = usingPCRange ? [...params, dateFrom, dateTo] : params;
    const chlParams = usingPCRange ? [...params, dateFrom, dateTo] : params;

    const taskType = req.query.taskType || 'both';
    // status filter for the returned task rows. Default 'pending' for backward compat.
    // Counts (pending/revised/completed) are always computed so the stat cards stay in sync.
    const reqStatus = (req.query.status || 'pending').toLowerCase();
    const rowStatus = ['pending','completed','revised','all'].includes(reqStatus) ? reqStatus : 'pending';
    const rowStatusClause = rowStatus === 'all' ? '' : `AND t.status='${rowStatus}'`;
    const skipStats = req.query.skipStats === '1';
    let pending = 0, revised = 0, completed = 0;

    let upcoming = 0;
    if (!skipStats && (taskType === 'delegation' || taskType === 'both')) {
      const [d] = await db.query(`SELECT SUM(CASE WHEN status='pending' AND (due_date IS NULL OR due_date <= CURDATE()) THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN status='revised' AND (due_date IS NULL OR due_date <= CURDATE()) THEN 1 ELSE 0 END) AS revised,SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed FROM delegation_tasks t WHERE 1=1 ${userFilter} ${delDateClause}`, delParams);
      pending += parseInt(d[0].pending)||0; revised += parseInt(d[0].revised)||0; completed += parseInt(d[0].completed)||0;
    }
    if (!skipStats && (taskType === 'checklist' || taskType === 'both')) {
      const [d] = await db.query(`SELECT SUM(CASE WHEN status='pending' AND (due_date IS NULL OR due_date <= CURDATE()) THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN status='revised' AND (due_date IS NULL OR due_date <= CURDATE()) THEN 1 ELSE 0 END) AS revised,SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed FROM checklist_tasks t WHERE 1=1 ${userFilter} ${chlDateClause}`, chlParams);
      pending += parseInt(d[0].pending)||0; revised += parseInt(d[0].revised)||0; completed += parseInt(d[0].completed)||0;
    }
    if (!skipStats && (taskType === 'delegation' || taskType === 'both')) {
      const [[u1]] = await db.query(`SELECT COUNT(*) AS cnt FROM delegation_tasks t WHERE status IN ('pending','revised') AND due_date > CURDATE() AND YEAR(due_date)=YEAR(CURDATE()) AND MONTH(due_date)=MONTH(CURDATE()) ${userFilter}`, params);
      upcoming += parseInt(u1.cnt)||0;
    }
    if (!skipStats && (taskType === 'checklist' || taskType === 'both')) {
      const [[u2]] = await db.query(`SELECT COUNT(*) AS cnt FROM checklist_tasks t WHERE status IN ('pending','revised') AND due_date > CURDATE() AND YEAR(due_date)=YEAR(CURDATE()) AND MONTH(due_date)=MONTH(CURDATE()) ${userFilter}`, params);
      upcoming += parseInt(u2.cnt)||0;
    }

    let delegationRows = [], checklistRows = [];
    if (taskType === 'delegation' || taskType === 'both') {
      const [rows] = await db.query(`SELECT t.id,'delegation' AS type,t.description,t.status,t.assigned_to,COALESCE(t.priority,'low') AS priority,COALESCE(t.approval,'no') AS approval,COALESCE(t.waiting_approval,0) AS waiting_approval,COALESCE(t.awaiting_due_date,0) AS awaiting_due_date,t.no_due_date_reason,t.remarks,t.url,t.client_id,c.name AS client_name,DATE_FORMAT(t.created_at,'%Y-%m-%d') AS delegated_on,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,DATE_FORMAT(t.completed_at,'%Y-%m-%d') AS completed_on,COALESCE(u1.name,'— deleted user —') AS assignedToName,COALESCE(u2.name,'—') AS assignedByName FROM delegation_tasks t LEFT JOIN users u1 ON t.assigned_to=u1.id LEFT JOIN users u2 ON t.assigned_by=u2.id LEFT JOIN clients c ON t.client_id=c.id WHERE 1=1 ${rowStatusClause} ${delDateClause} ${userFilter} ORDER BY t.due_date ASC LIMIT 500`, delParams);
      delegationRows = rows;
    }
    if (taskType === 'checklist' || taskType === 'both') {
      const [rows] = await db.query(`SELECT t.id,'checklist' AS type,t.description,t.status,t.assigned_to,COALESCE(t.priority,'low') AS priority,'no' AS approval,0 AS waiting_approval,0 AS awaiting_due_date,t.remarks,t.client_id,c.name AS client_name,DATE_FORMAT(t.created_at,'%Y-%m-%d') AS delegated_on,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,NULL AS completed_on,COALESCE(u1.name,'— deleted user —') AS assignedToName,COALESCE(u2.name,'—') AS assignedByName FROM checklist_tasks t LEFT JOIN users u1 ON t.assigned_to=u1.id LEFT JOIN users u2 ON t.assigned_by=u2.id LEFT JOIN clients c ON t.client_id=c.id WHERE 1=1 ${rowStatusClause} ${chlDateClause} ${userFilter} ORDER BY t.due_date ASC LIMIT 500`, chlParams);
      checklistRows = rows;
    }
    // `todayPending` kept for backwards compatibility (regular pending load still uses it).
    // `tasks` is the generic field for any status filter.
    res.json({ pending, revised, completed, upcoming, todayPending: [...delegationRows, ...checklistRows], tasks: [...delegationRows, ...checklistRows], status: rowStatus });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Task routes now live in routes/tasks.js. canSeeTask and canTouchSubtasks
// stay here on purpose — the task-comments routes below call canSeeTask —
// and are injected back into the module.
require('./backend/routes/tasks')(app, {
  db,
  requireAuth,
  requireAdmin,
  archiveDeleted,
  userCanDo,
  isHandlerOf,
  getTable,
  logTaskActivity,
  findRecentDuplicateTask,
  getNotifyTarget,
  isUserOffOn,
  loadHolidaysSet,
  nextWorkingDay,
  fmtClock,
  sendMail,
  sendWhatsAppRaw,
  emailUserWaText,
  waTextToEmailHtml,
  canTouchSubtasks,
  canSeeTask,
});

// ── Sub-tasks: follow-up asks nested under a delegation task (e.g. client says
// "make a dashboard" then later "change its color") instead of a brand-new task.
async function canTouchSubtasks(req, task) {
  if (!task) return false;
  const role = req.session.role;
  if (role === 'client') {
    const [[me]] = await db.query('SELECT client_id FROM users WHERE id=? LIMIT 1', [req.session.userId]);
    return !!me?.client_id && Number(me.client_id) === Number(task.client_id);
  }
  return role === 'admin' || role === 'hod' || role === 'pc'
    || Number(task.assigned_to) === Number(req.session.userId)
    || Number(task.assigned_by) === Number(req.session.userId);
}

// May this session read one task's side data — its comments, its activity log?
// Same rule sub-tasks already use: managers, the doer, the assigner, or the
// client the task belongs to. Both routes below took an id off the URL and
// answered, so changing the number in it read somebody else's task.
async function canSeeTask(req, taskId, taskType) {
  const tt = taskType === 'checklist' ? 'checklist' : 'delegation';
  const id = parseInt(taskId, 10);
  if (!Number.isFinite(id)) return false;
  const [[task]] = await db.query(
    `SELECT id, assigned_to, assigned_by, client_id FROM ${getTable(tt)} WHERE id=?`, [id]);
  return canTouchSubtasks(req, task);
}


// ══════════════════════════════════════════════════════
// APPROVALS
// ══════════════════════════════════════════════════════
app.get('/api/approvals', requireAuth, async (req, res) => {
  try {
    const role = req.session.role;
    const isAdminOrPC = role === 'admin' || role === 'pc';
    // Everyone sees the approvals routed to THEM (the task's assigner). Admin/PC
    // see ALL pending approvals so nothing is ever stuck — e.g. requests routed to
    // a client (no approvals screen) or to a missing/invalid assigner.
    let whereClause, params;
    if (isAdminOrPC) { whereClause = `WHERE ta.status='pending'`; params = []; }
    else { whereClause = `WHERE ta.requested_to=? AND ta.status='pending'`; params = [req.session.userId]; }
    const [rows] = await db.query(`SELECT ta.*,DATE_FORMAT(ta.new_date,'%Y-%m-%d') AS reviseToDate,COALESCE(u1.name,'(deleted)') AS requestedByName,COALESCE(u2.name,'(deleted)') AS requestedToName,COALESCE(dt.description,ct.description) AS description,dt.approval AS taskApproval,DATE_FORMAT(COALESCE(dt.due_date,ct.due_date),'%Y-%m-%d') AS currentDue FROM task_approvals ta LEFT JOIN users u1 ON ta.requested_by=u1.id LEFT JOIN users u2 ON ta.requested_to=u2.id LEFT JOIN delegation_tasks dt ON ta.task_id=dt.id AND ta.task_type='delegation' LEFT JOIN checklist_tasks ct ON ta.task_id=ct.id AND ta.task_type='checklist' ${whereClause} ORDER BY ta.created_at DESC`, params);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/approvals/count', requireAuth, async (req, res) => {
  try {
    // Count approvals waiting on THIS user; admin/PC count ALL pending so orphaned
    // ones (client-routed or missing assigner) surface instead of getting stuck.
    const isAdminOrPC = req.session.role === 'admin' || req.session.role === 'pc';
    const sql = isAdminOrPC
      ? `SELECT COUNT(*) AS count FROM task_approvals WHERE status='pending'`
      : `SELECT COUNT(*) AS count FROM task_approvals WHERE requested_to=? AND status='pending'`;
    const [rows] = await db.query(sql, isAdminOrPC ? [] : [req.session.userId]);
    res.json({ count: rows[0].count });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/approvals/:id', requireAuth, async (req, res) => {
  try {
    const { action, note } = req.body;
    const role = req.session.role;
    const [rows] = await db.query(`SELECT *, DATE_FORMAT(new_date,'%Y-%m-%d') AS new_date_fmt FROM task_approvals WHERE id=?`, [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Approval not found' });
    const appr = rows[0];
    // PC and admin can approve any; others only their own
    const canApprove = role === 'admin' || role === 'pc' || appr.requested_to === req.session.userId;
    if (!canApprove) return res.status(403).json({ error: 'Not allowed' });
    // Same sub-task rule as PUT /api/tasks/:id/status — re-checked here because
    // the client can add a sub-task after completion was requested, and
    // approving would otherwise close the task over it.
    if (action === 'approved' && appr.action_type === 'completed' && appr.task_type === 'delegation') {
      const [[open]] = await db.query(
        `SELECT COUNT(*) AS n FROM task_subtasks WHERE task_id=? AND status<>'completed'`, [appr.task_id]);
      if (open?.n > 0) {
        return res.status(409).json({
          error: `${open.n} sub-task${open.n === 1 ? '' : 's'} still pending on this task — it cannot be approved as done yet.`
        });
      }
    }
    await db.query('UPDATE task_approvals SET status=?,note=? WHERE id=?', [action, note||'', req.params.id]);
    const table = getTable(appr.task_type);
    if (action === 'approved') {
      // Revise approved → push the held new date now. Other actions just set status.
      if (appr.action_type === 'revised' && appr.new_date_fmt) {
        await db.query(`UPDATE ${table} SET status='pending',waiting_approval=0,due_date=? WHERE id=?`, [appr.new_date_fmt, appr.task_id]);
      } else if (appr.action_type === 'revised') {
        await db.query(`UPDATE ${table} SET status='pending',waiting_approval=0 WHERE id=?`, [appr.task_id]);
      } else if (appr.task_type === 'delegation') {
        // Stamp completed_at here too — approving a completion is the moment
        // the task actually closes.
        await db.query(`UPDATE ${table} SET status=?,waiting_approval=0,completed_at=IF(?='completed',NOW(),NULL) WHERE id=?`,
          [appr.action_type, appr.action_type, appr.task_id]);
      } else {
        await db.query(`UPDATE ${table} SET status=?,waiting_approval=0 WHERE id=?`, [appr.action_type, appr.task_id]);
      }
    } else {
      // Rejected → drop the waiting flag; due_date and status stay unchanged.
      await db.query(`UPDATE ${table} SET waiting_approval=0 WHERE id=?`, [appr.task_id]);
    }
    logTaskActivity({
      taskId: appr.task_id, taskType: appr.task_type,
      newStatus: action === 'approved' ? (appr.action_type === 'revised' ? 'pending' : appr.action_type) : null,
      changedBy: req.session.userId, source: `approval-${action}`,
      note: `${appr.action_type} request${appr.new_date_fmt ? ` (due -> ${appr.new_date_fmt})` : ''}`
    });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk-approve ALL pending revise requests (admin/PC). Applies each held new date
// to its task and clears the waiting flag — clears orphaned/stuck revises in one go.
app.post('/api/approvals/approve-all-revises', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const [pending] = await db.query(
      `SELECT id, task_id, task_type, DATE_FORMAT(new_date,'%Y-%m-%d') AS nd
         FROM task_approvals WHERE status='pending' AND action_type='revised'`);
    let approved = 0;
    for (const a of pending) {
      const table = getTable(a.task_type);
      // A task that was completed after its revise was filed must not be dragged
      // back to pending by this bulk sweep — that would look exactly like "my
      // finished task came back". Clear the stale request and leave the task alone.
      const [[cur]] = await db.query(`SELECT status FROM ${table} WHERE id=?`, [a.task_id]);
      if (!cur) { await db.query(`UPDATE task_approvals SET status='approved' WHERE id=?`, [a.id]); continue; }
      if (cur.status === 'completed') {
        await db.query(`UPDATE task_approvals SET status='approved', note=CONCAT(COALESCE(note,''),' [skipped — task already completed]') WHERE id=?`, [a.id]);
        logTaskActivity({ taskId: a.task_id, taskType: a.task_type, oldStatus: 'completed', newStatus: 'completed',
          changedBy: req.session.userId, source: 'bulk-revise-skipped', note: 'stale revise cleared, task left completed' });
        continue;
      }
      if (a.nd) await db.query(`UPDATE ${table} SET status='pending', waiting_approval=0, due_date=? WHERE id=?`, [a.nd, a.task_id]);
      else      await db.query(`UPDATE ${table} SET status='pending', waiting_approval=0 WHERE id=?`, [a.task_id]);
      await db.query(`UPDATE task_approvals SET status='approved' WHERE id=?`, [a.id]);
      logTaskActivity({ taskId: a.task_id, taskType: a.task_type, oldStatus: cur.status, newStatus: 'pending',
        changedBy: req.session.userId, source: 'bulk-revise-approve', note: a.nd ? `due -> ${a.nd}` : null });
      approved++;
    }
    res.json({ ok: true, approved });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// MIS
// ══════════════════════════════════════════════════════

// May this session pull MIS figures for one named employee? A hod's MIS is
// scoped to their own department; admin sees everyone. The list routes below
// enforce that with an `AND u.department=?` on their aggregate query, but the
// two drill-down routes take a userId off the query string and had no check of
// their own, so this exists to give them the same answer. Deliberately fails
// closed: a hod with no department on their own row matches nobody.
async function misHodMaySee(session, targetUserId) {
  if (session.role !== 'hod') return true;
  const id = parseInt(targetUserId, 10);
  if (!Number.isFinite(id)) return false;
  const [[me]]     = await db.query('SELECT department FROM users WHERE id=?', [session.userId]);
  const [[target]] = await db.query('SELECT department FROM users WHERE id=?', [id]);
  if (!me || !target) return false;
  const dept = (me.department || '').trim();
  if (!dept) return false;
  return (target.department || '').trim() === dept;
}

app.get('/api/mis', requireAuth, requireMisViewer, async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'Dates required' });
    const isHod = req.session.role === 'hod';
    // Department filter for HOD
    let deptFilter = '';
    let deptParams = [start, end];
    if (isHod) {
      const [me] = await db.query('SELECT department FROM users WHERE id=?', [req.session.userId]);
      const dept = me[0]?.department || '';
      deptFilter = 'AND u.department=?';
      deptParams = [start, end, dept];
    }

    const calc = rows => rows.map(r => {
      const total=parseInt(r.total)||0, pending=parseInt(r.pending)||0, overdue=parseInt(r.overdue)||0, revised=parseInt(r.revised)||0;
      let score = total > 0 ? Math.max(-100, Math.round((0-(pending/total)*100-(overdue/total)*50-(revised/total)*25)*10)/10) : 0;
      return { ...r, delayed: overdue, score };
    });
    const [delRows] = await db.query(`SELECT u.id AS userId,u.name,COUNT(*) AS total,SUM(CASE WHEN t.status='pending' THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN t.status='completed' THEN 1 ELSE 0 END) AS completed,SUM(CASE WHEN t.status='revised' THEN 1 ELSE 0 END) AS revised,SUM(CASE WHEN t.status='pending' AND t.due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue FROM delegation_tasks t JOIN users u ON t.assigned_to=u.id WHERE t.due_date BETWEEN ? AND ? AND u.role <> 'client' AND u.client_id IS NULL ${deptFilter} GROUP BY u.id,u.name ORDER BY u.name`, deptParams);
    const [chlRows] = await db.query(`SELECT u.id AS userId,u.name,COUNT(*) AS total,SUM(CASE WHEN t.status='pending' THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN t.status='completed' THEN 1 ELSE 0 END) AS completed,0 AS revised,SUM(CASE WHEN t.status='pending' AND t.due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue FROM checklist_tasks t JOIN users u ON t.assigned_to=u.id WHERE t.due_date BETWEEN ? AND ? AND u.role <> 'client' AND u.client_id IS NULL ${deptFilter} GROUP BY u.id,u.name ORDER BY u.name`, deptParams);
    res.json({ delegation: calc(delRows), checklist: calc(chlRows) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// FMS BATCH LOADERS
// ══════════════════════════════════════════════════════
// The three FMS reports below (/api/fms-dashboard, /api/mis/all, /api/mis/fms)
// each used to walk sheet-by-sheet and then step-by-step, issuing one query per
// sheet for its steps and one per step for its doers. With connectionLimit: 1
// those run strictly one after another, so a report over S sheets averaging M
// steps cost 1 + S + S×M serialized round trips — 141 of them at S=20, M=6.
// These two helpers fetch the same rows in one query each; the callers group in
// memory and map the result into whatever shape they used before.
//
// Both take an id list and return a Map, and both short-circuit on an empty
// list: `IN ()` is a MySQL syntax error, not an empty result set.

// fms_id -> steps[]. ORDER BY mirrors the per-sheet query's `step_order ASC`
// exactly; ties stay as arbitrary as they already were.
async function fmsStepsBySheet(sheetIds) {
  const out = new Map();
  if (!sheetIds.length) return out;
  const [rows] = await db.query(
    `SELECT * FROM fms_steps WHERE fms_id IN (${sheetIds.map(() => '?').join(',')})
     ORDER BY fms_id ASC, step_order ASC`, sheetIds);
  for (const r of rows) {
    if (!out.has(r.fms_id)) out.set(r.fms_id, []);
    out.get(r.fms_id).push(r);
  }
  return out;
}

// Same, for the doer-filtered variant /api/fms-dashboard uses for non-admins.
async function fmsStepsBySheetForUsers(sheetIds, userIds) {
  const out = new Map();
  if (!sheetIds.length || !userIds.length) return out;
  const [rows] = await db.query(
    `SELECT DISTINCT fst.* FROM fms_steps fst
     JOIN fms_step_doers fsd ON fsd.step_id=fst.id
     WHERE fst.fms_id IN (${sheetIds.map(() => '?').join(',')})
       AND fsd.user_id IN (${userIds.map(() => '?').join(',')})
     ORDER BY fst.fms_id ASC, fst.step_order ASC`, [...sheetIds, ...userIds]);
  for (const r of rows) {
    if (!out.has(r.fms_id)) out.set(r.fms_id, []);
    out.get(r.fms_id).push(r);
  }
  return out;
}

// step_id -> doer rows, with `cols` naming the same user columns the caller
// asked for before. The per-step queries carried no ORDER BY and read through
// idx_step, so rows arrived ordered by (step_id, fsd.id); that is stated
// explicitly here so the batched read cannot reorder anyone — /api/fms-dashboard
// joins these names into a display string.
//
// step_id is selected only to group by, then deleted: the callers' own queries
// never returned that column, and one of them hands these rows straight to the
// client.
async function fmsDoersByStep(stepIds, cols) {
  const out = new Map();
  if (!stepIds.length) return out;
  const [rows] = await db.query(
    `SELECT fsd.step_id, ${cols} FROM fms_step_doers fsd
     JOIN users u ON fsd.user_id=u.id
     WHERE fsd.step_id IN (${stepIds.map(() => '?').join(',')})
     ORDER BY fsd.step_id ASC, fsd.id ASC`, stepIds);
  for (const r of rows) {
    const k = r.step_id;
    delete r.step_id;
    if (!out.has(k)) out.set(k, []);
    out.get(k).push(r);
  }
  return out;
}

// Fetch one Google Sheets range per FMS sheet, a few at a time.
//
// These used to be awaited one after another inside the per-sheet loop, so a
// report over four sheets paid four sequential round trips to Google before it
// could render — which is what made the Dashboard's Performance & Activity
// panel arrive late. Only the network part is parallel; the callers still walk
// the results in their original order, so the numbers they accumulate cannot
// shift.
//
// The cap exists so this stays true if the sheet count grows. Four concurrent
// reads is nothing to Google's quota, forty would not be — and nobody adding an
// FMS sheet is going to think about read limits.
//
// A sheet that fails resolves to null rather than rejecting, which preserves
// the old per-sheet `catch { /* skip this sheet */ }`: one unreadable or
// unshared spreadsheet must not blank the whole report.
const FMS_SHEET_FETCH_CONCURRENCY = 5;

async function fmsFetchRanges(sheetsApi, requests) {
  const out = new Array(requests.length).fill(null);
  for (let i = 0; i < requests.length; i += FMS_SHEET_FETCH_CONCURRENCY) {
    const batch = requests.slice(i, i + FMS_SHEET_FETCH_CONCURRENCY);
    await Promise.all(batch.map(async (r, j) => {
      if (!r) return;
      try {
        const resp = await sheetsApi.spreadsheets.values.get({
          spreadsheetId: r.spreadsheetId, range: r.range });
        out[i + j] = resp.data.values || [];
      } catch (e) { out[i + j] = null; }
    }));
  }
  return out;
}

// step_id -> user ids. Deliberately does NOT join users, because the caller it
// replaces (/api/mis/all) did not either: a doer row whose user has since been
// deleted still counts there, and a join would silently drop it.
async function fmsDoerIdsByStep(stepIds) {
  const out = new Map();
  if (!stepIds.length) return out;
  const [rows] = await db.query(
    `SELECT step_id, user_id FROM fms_step_doers
     WHERE step_id IN (${stepIds.map(() => '?').join(',')})
     ORDER BY step_id ASC, id ASC`, stepIds);
  for (const r of rows) {
    if (!out.has(r.step_id)) out.set(r.step_id, []);
    out.get(r.step_id).push(r.user_id);
  }
  return out;
}

// ── FMS Dashboard — row-level pending tasks (like delegation/checklist) ──
app.get('/api/fms-dashboard', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const role = req.session.role;
    const isAdmin = role === 'admin' || role === 'pc';
    const isHod = role === 'hod';
    const filterEmployee = req.query.employee;

    const today = new Date().toISOString().split('T')[0];

    // Determine which user IDs to show
    let targetUserIds = null; // null = all (admin)
    if (isAdmin && filterEmployee && filterEmployee !== 'all') {
      targetUserIds = [parseInt(filterEmployee)];
    } else if (isHod) {
      const [me] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
      const dept = me[0]?.department || '';
      if (filterEmployee && filterEmployee !== 'all') {
        targetUserIds = [parseInt(filterEmployee)];
      } else {
        const [deptUsers] = await db.query('SELECT id FROM users WHERE department=? AND role NOT IN (?,?)', [dept, 'admin', 'hod']);
        targetUserIds = deptUsers.map(u => u.id);
        if (!targetUserIds.length) return res.json({ rows: [], pendingCount: 0 });
      }
    } else {
      // Regular employee — only their own steps
      targetUserIds = [uid];
    }

    // Get FMS sheets
    let fmsList;
    if (isAdmin && !filterEmployee || (isAdmin && filterEmployee === 'all')) {
      [fmsList] = await db.query('SELECT * FROM fms_sheets ORDER BY fms_name ASC');
    } else {
      // Get FMS where targetUserIds are doers
      [fmsList] = await db.query(
        `SELECT DISTINCT fs.* FROM fms_sheets fs
         JOIN fms_steps fst ON fst.fms_id=fs.id
         JOIN fms_step_doers fsd ON fsd.step_id=fst.id
         WHERE fsd.user_id IN (${targetUserIds.map(()=>'?').join(',')})
         ORDER BY fs.fms_name ASC`, targetUserIds);
    }

    if (!fmsList.length) return res.json({ rows: [], pendingCount: 0 });

    const allRows = [];

    // Steps for every sheet, then doers for every step — two queries, hoisted
    // out of the loop below. The admin/non-admin split is the same one the
    // per-sheet query made; it just gets decided once now.
    const stepsBySheet = (isAdmin && (!filterEmployee || filterEmployee === 'all'))
      ? await fmsStepsBySheet(fmsList.map(s => s.id))
      : await fmsStepsBySheetForUsers(fmsList.map(s => s.id), targetUserIds);
    const doersByStep = await fmsDoersByStep(
      [...stepsBySheet.values()].flat().map(s => s.id), 'u.id, u.name');

    for (const sheet of fmsList) {
      const fmsName = sheet.fms_name || sheet.sheet_name;

      const steps = stepsBySheet.get(sheet.id) || [];
      if (!steps.length) continue;

      for (const step of steps) {
        const doers = doersByStep.get(step.id) || [];
        step.doerNames = doers.map(d => d.name).join(', ');
        step.doerIds = doers.map(d => d.id);
      }

      try {
        const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
        const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
        const tabName = sheet.sheet_name || 'Sheet1';
        const headerRowIdx = (sheet.header_row || 1) - 1;

        const filteredSteps = steps; // fix: was undefined, use steps array
        // Include show_cols indices so we can return their values for the All Tasks "Details" column.
        const showColsByStep = filteredSteps.map(s => {
          try { return JSON.parse(s.show_cols || '[]').filter(n => Number.isInteger(n) && n >= 0); }
          catch { return []; }
        });
        const allCols = filteredSteps.flatMap(s => [colToIdx(s.plan_col), colToIdx(s.actual_col)])
          .concat(showColsByStep.flat())
          .filter(x => x >= 0);
        if (!allCols.length) continue;
        const maxCol = Math.max(...allCols);
        const lastCol = idxToCol(maxCol);
        const range = `${tabName}!A:${lastCol}`;

        const response = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range });
        const sheetData = response.data.values || [];
        const headers = sheetData[headerRowIdx] || [];
        const dataRows = sheetData.slice(headerRowIdx + 1);

        for (let si = 0; si < steps.length; si++) {
          const step = steps[si];
          const showCols = showColsByStep[si];
          const planIdx = colToIdx(step.plan_col);
          const actualIdx = colToIdx(step.actual_col);
          if (planIdx < 0 || actualIdx < 0) continue;

          // Strip every flavour of whitespace (regular, NBSP, zero-width, BOM) so cells
          // that only contain invisible chars don't slip past as "non-blank".
          const blankClean = v => (v || '').toString().replace(/[\s ​-‍﻿]+/g, '');
          dataRows.forEach((row, i) => {
            const planVal = (row[planIdx] || '').trim();
            const actualVal = (row[actualIdx] || '').trim();
            if (!blankClean(planVal) || blankClean(actualVal)) return; // skip if no plan or already done

            // Parse plan date — try to extract date from value
            // planVal might be a date string like "2026-04-07" or "07/04/2026" or just text,
            // optionally followed by a time like " 14:30" or " 14:30:00".
            let planDate = '';
            let planTime = '';
            const dateMatch = planVal.match(/(\d{4}-\d{2}-\d{2})|(\d{2}[\/\-]\d{2}[\/\-]\d{4})/);
            if (dateMatch) {
              const raw = dateMatch[0];
              if (raw.includes('-') && raw.length === 10 && raw[4] === '-') {
                planDate = raw; // already YYYY-MM-DD
              } else {
                // DD/MM/YYYY → YYYY-MM-DD
                const parts = raw.split(/[\/\-]/);
                if (parts.length === 3) planDate = `${parts[2]}-${parts[1]}-${parts[0]}`;
              }
              // Time tail (HH:MM or HH:MM:SS) anywhere after the date.
              const after = planVal.slice(dateMatch.index + raw.length);
              const timeMatch = after.match(/\b(\d{1,2}):(\d{2})(?::(\d{2}))?\b/);
              if (timeMatch) {
                const hh = timeMatch[1].padStart(2,'0');
                const mm = timeMatch[2];
                const ss = timeMatch[3];
                planTime = ss ? `${hh}:${mm}:${ss}` : `${hh}:${mm}`;
              }
            }

            // isLate: plan date is in the past and still pending
            const isLate = planDate && planDate < today;

            // Build "details" — first 5 configured show_cols with their headers + values.
            const details = [];
            const colsToShow = (showCols && showCols.length ? showCols : []).slice(0, 5);
            for (const ci of colsToShow) {
              const header = headers[ci] || `Col ${idxToCol(ci)}`;
              const value = (row[ci] || '').toString().trim();
              details.push({ header, value });
            }

            allRows.push({
              fmsName,
              fmsId: sheet.id,
              stepName: step.step_name,
              stepId: step.id,
              doer: step.doerNames || '—',
              planValue: planVal,
              planDate: planDate || '',
              planTime: planTime || '',
              isLate,
              rowNumber: headerRowIdx + 1 + i + 1,
              details
            });
          });
        }
      } catch(e) {
        // Skip sheet on error, don't fail whole request
      }
    }

    res.json({ rows: allRows, pendingCount: allRows.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/mis/detail', requireAuth, requireMisViewer, async (req, res) => {
  try {
    const { userId, type, start, end } = req.query;
    if (!userId || !start || !end) return res.status(400).json({ error: 'Missing params' });
    // A hod's MIS is scoped to their own department — but that scoping lived in
    // /api/mis and /api/mis/all, the two routes that build the LIST. This is the
    // drill-down, and it took userId straight off the query string, so editing
    // that number handed back another department's task detail. The list never
    // offers those rows, which is why nothing surfaced; the endpoint had no lock
    // of its own. Admins are unaffected and still see everyone.
    if (!(await misHodMaySee(req.session, userId))) {
      return res.status(403).json({ error: 'That employee is not in your department' });
    }
    const table = type === 'delegation' ? 'delegation_tasks' : 'checklist_tasks';
    const [tasks] = await db.query(`SELECT t.id,t.description,t.status,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u2.name AS assigned_by_name FROM ${table} t JOIN users u2 ON t.assigned_by=u2.id WHERE t.assigned_to=? AND t.due_date BETWEEN ? AND ? ORDER BY t.due_date ASC`, [userId, start, end]);
    res.json({ tasks });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── All MIS — per employee combined score ──
app.get('/api/mis/all', requireAuth, requireMisViewer, async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'Dates required' });
    const isHod = req.session.role === 'hod';
    const uid = req.session.userId;

    // Same deptFilter logic as /api/mis — tasks JOIN users se filter
    let deptFilter = '';
    let deptParams = [start, end];
    if (isHod) {
      const [me] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
      const dept = me[0]?.department || '';
      deptFilter = 'AND u.department=?';
      deptParams = [start, end, dept];
    }

    const calc = (total, pending, overdue, revised) => {
      total = parseInt(total)||0; pending = parseInt(pending)||0;
      overdue = parseInt(overdue)||0; revised = parseInt(revised)||0;
      const score = total > 0 ? Math.max(-100, Math.round((0-(pending/total)*100-(overdue/total)*50-(revised/total)*25)*10)/10) : 0;
      return { total, pending, overdue, revised, score };
    };

    // Fetch delegation + checklist stats per user (same style as /api/mis)
    const [delRows] = await db.query(
      `SELECT u.id AS userId, u.name, u.department,
        COUNT(*) AS total,
        SUM(CASE WHEN t.status='pending' THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN t.status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN t.status='revised' THEN 1 ELSE 0 END) AS revised,
        SUM(CASE WHEN t.status='pending' AND t.due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM delegation_tasks t JOIN users u ON t.assigned_to=u.id
       WHERE t.due_date BETWEEN ? AND ? AND u.role <> 'client' AND u.client_id IS NULL ${deptFilter}
       GROUP BY u.id, u.name, u.department ORDER BY u.name`, deptParams);

    const [chlRows] = await db.query(
      `SELECT u.id AS userId, u.name, u.department,
        COUNT(*) AS total,
        SUM(CASE WHEN t.status='pending' THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN t.status='completed' THEN 1 ELSE 0 END) AS completed,
        0 AS revised,
        SUM(CASE WHEN t.status='pending' AND t.due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM checklist_tasks t JOIN users u ON t.assigned_to=u.id
       WHERE t.due_date BETWEEN ? AND ? AND u.role <> 'client' AND u.client_id IS NULL ${deptFilter}
       GROUP BY u.id, u.name, u.department ORDER BY u.name`, deptParams);

    // Merge by userId
    const userMap = {};
    for (const r of delRows) {
      userMap[r.userId] = { userId: r.userId, name: r.name, department: r.department||'',
        delegation: calc(r.total, r.pending, r.overdue, r.revised),
        delegationCompleted: parseInt(r.completed)||0,
        checklist: calc(0,0,0,0), checklistCompleted: 0 };
      userMap[r.userId].delegation.completed = parseInt(r.completed)||0;
    }
    for (const r of chlRows) {
      if (!userMap[r.userId]) {
        userMap[r.userId] = { userId: r.userId, name: r.name, department: r.department||'',
          delegation: calc(0,0,0,0), delegationCompleted: 0,
          checklist: calc(0,0,0,0), checklistCompleted: 0 };
        userMap[r.userId].delegation.completed = 0;
      }
      userMap[r.userId].checklist = calc(r.total, r.pending, r.overdue, 0);
      userMap[r.userId].checklist.completed = parseInt(r.completed)||0;
      userMap[r.userId].checklistCompleted = parseInt(r.completed)||0;
    }

    // Fetch week plan for each user — DATE_FORMAT so the frontend gets a clean YYYY-MM-DD (not an ISO timestamp)
    let planMap = {};
    try {
      const [plans] = await db.query(
        `SELECT employee_id, target_count, DATE_FORMAT(start_date,'%Y-%m-%d') AS start_date, improvement_pct FROM week_plans WHERE start_date BETWEEN ? AND ? ORDER BY start_date DESC`, [start, end]);
      for (const p of plans) {
        if (!planMap[p.employee_id]) planMap[p.employee_id] = p;
      }
    } catch(e) { /* week_plans table may not exist yet */ }

    // ── FMS contribution per user ─────────────────────────────────────
    // For each user, work out the pending/done count on the steps where they're a doer.
    // FMS is applicable to admin only (HOD will also get counts for their dept's users).
    const fmsUserMap = {};   // userId -> { total, pending, done }
    try {
      const [allSheets] = await db.query('SELECT * FROM fms_sheets');
      if (allSheets.length) {
        const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']).catch(()=>null);
        if (sheetsApi) {
          const stepsBySheet = await fmsStepsBySheet(allSheets.map(s => s.id));
          const doerIdsByStep = await fmsDoerIdsByStep(
            [...stepsBySheet.values()].flat().map(s => s.id));

          // Work out every sheet's range first, then fetch them together. The
          // accumulation below still runs one sheet at a time, in the original
          // order — only the waiting is shared.
          const plans = allSheets.map(sheet => {
            const steps = stepsBySheet.get(sheet.id) || [];
            if (!steps.length) return null;
            for (const step of steps) step.doerIds = doerIdsByStep.get(step.id) || [];
            const allCols = steps.flatMap(s => [colToIdx(s.plan_col), colToIdx(s.actual_col)]).filter(x => x >= 0);
            if (!allCols.length) return null;
            return {
              sheet, steps,
              headerRowIdx: (sheet.header_row || 1) - 1,
              spreadsheetId: extractSpreadsheetId(sheet.sheet_id),
              range: `${sheet.sheet_name || 'Sheet1'}!A:${idxToCol(Math.max(...allCols))}`,
            };
          });
          const fetched = await fmsFetchRanges(sheetsApi, plans);

          for (let si = 0; si < plans.length; si++) {
            const plan = plans[si];
            if (!plan || !fetched[si]) continue;   // no steps, no columns, or the fetch failed
            const { steps, headerRowIdx } = plan;
            try {
              const dataRows = fetched[si].slice(headerRowIdx + 1);
              for (const step of steps) {
                if (!step.doerIds.length) continue;
                const planIdx = colToIdx(step.plan_col);
                const actualIdx = colToIdx(step.actual_col);
                if (planIdx < 0 || actualIdx < 0) continue;
                let stepPending = 0, stepDone = 0;
                dataRows.forEach(row => {
                  const planVal = (row[planIdx]||'').toString().trim();
                  const actualVal = (row[actualIdx]||'').toString().trim();
                  if (!planVal) return;
                  // Date-range filter — only count FMS rows whose plan-date falls in [start, end].
                  // Mirrors what /api/mis/fms-detail returns so counts and detail stay in sync.
                  const planDate = parseFmsPlanDate(planVal);
                  if (!planDate || planDate < start || planDate > end) return;
                  if (!actualVal) stepPending++;
                  else stepDone++;
                });
                // Distribute counts to each doer (each doer gets the full count attributed — shared work)
                step.doerIds.forEach(uid => {
                  if (!fmsUserMap[uid]) fmsUserMap[uid] = { total: 0, pending: 0, done: 0 };
                  fmsUserMap[uid].pending += stepPending;
                  fmsUserMap[uid].done    += stepDone;
                  fmsUserMap[uid].total   += stepPending + stepDone;
                });
              }
            } catch(e) { /* skip this sheet on error */ }
          }
        }
      }
    } catch(e) { /* ignore — FMS optional */ }

    // If a user only works in FMS (0 tasks in del/chl), add them to userMap too,
    // so their FMS contribution shows up in the All MIS view.
    if (Object.keys(fmsUserMap).length) {
      const fmsUserIds = Object.keys(fmsUserMap).map(x => parseInt(x)).filter(x => !userMap[x]);
      if (fmsUserIds.length) {
        let userQ = `SELECT id, name, department FROM users WHERE id IN (${fmsUserIds.map(()=>'?').join(',')})`;
        const userQParams = [...fmsUserIds];
        if (isHod) {
          const [me] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
          const dept = me[0]?.department || '';
          userQ += ' AND department=?';
          userQParams.push(dept);
        }
        const [extraUsers] = await db.query(userQ, userQParams);
        for (const u of extraUsers) {
          userMap[u.id] = { userId: u.id, name: u.name, department: u.department||'',
            delegation: calc(0,0,0,0), delegationCompleted: 0,
            checklist: calc(0,0,0,0), checklistCompleted: 0 };
          userMap[u.id].delegation.completed = 0;
        }
      }
    }

    const result = Object.values(userMap).map(u => {
      const d = u.delegation, c = u.checklist;
      const fms = fmsUserMap[u.userId] || { total: 0, pending: 0, done: 0 };
      const totalAll = d.total + c.total + fms.total;
      const pendingAll = d.pending + c.pending + fms.pending;
      const overdueAll = d.overdue + c.overdue;
      const revisedAll = d.revised;
      const completedAll = (d.completed||0) + (c.completed||0) + fms.done;
      const overallScore = totalAll > 0
        ? Math.max(-100, Math.round((0-(pendingAll/totalAll)*100-(overdueAll/totalAll)*50-(revisedAll/totalAll)*25)*10)/10)
        : null;
      const plan = planMap[u.userId] || null;
      // FMS score: jitne pending utna negative, jitne done utna acha
      const fmsScore = fms.total > 0
        ? Math.round((fms.done/fms.total)*100*10)/10  // 0-100% completion
        : null;
      return { ...u, fms: { ...fms, score: fmsScore }, totalAll, pendingAll, overdueAll, revisedAll, completedAll, overallScore, plan };
    }).filter(u => u.totalAll > 0).sort((a,b) => a.name.localeCompare(b.name));

    // Attach profile photos (used as the race-tracker runner avatars).
    const ids = result.map(u => u.userId);
    if (ids.length) {
      const [imgs] = await db.query(`SELECT id, profile_image FROM users WHERE id IN (${ids.map(()=>'?').join(',')})`, ids);
      const imgBy = {};
      for (const r of imgs) imgBy[r.id] = r.profile_image || null;
      for (const u of result) u.profileImage = imgBy[u.userId] || null;
    }

    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Composite "Most Active" ranking for the Dashboard. Aggregates per-user signals
// of engagement in a date range: tasks they own, tasks they delegated to OTHERS,
// revises they triggered on others' work, and leaves they filed.
app.get('/api/dashboard/activity', requireAuth, requireAdminOrHodOnly, async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'Dates required' });
    const isHod = req.session.role === 'hod';
    const uid = req.session.userId;
    let deptFilter = '';
    let deptParams = [];
    if (isHod) {
      const [me] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
      const dept = me[0]?.department || '';
      deptFilter = ' AND u.department=?';
      deptParams = [dept];
    }
    const [rows] = await db.query(
      `SELECT u.id AS userId, u.name, u.department, u.profile_image AS profileImage,
         COALESCE((SELECT COUNT(*) FROM delegation_tasks dt
                   WHERE dt.assigned_to=u.id AND dt.due_date BETWEEN ? AND ?), 0)
         + COALESCE((SELECT COUNT(*) FROM checklist_tasks ct
                     WHERE ct.assigned_to=u.id AND ct.due_date BETWEEN ? AND ?), 0) AS active_tasks,
         COALESCE((SELECT COUNT(*) FROM delegation_tasks dt
                   WHERE dt.assigned_by=u.id AND dt.assigned_to<>u.id AND dt.due_date BETWEEN ? AND ?), 0) AS delegated_to_others,
         COALESCE((SELECT COUNT(*) FROM delegation_tasks dt
                   WHERE dt.assigned_by=u.id AND dt.status='revised' AND dt.due_date BETWEEN ? AND ?), 0) AS revises_triggered,
         COALESCE((SELECT COUNT(*) FROM leave_requests lr
                   WHERE lr.user_id=u.id AND DATE(lr.created_at) BETWEEN ? AND ?), 0) AS leaves_submitted
       FROM users u
       WHERE u.role <> 'client' AND u.client_id IS NULL${deptFilter}`,
      [start, end, start, end, start, end, start, end, start, end, ...deptParams]
    );
    const scored = rows
      .map(r => ({
        userId: r.userId,
        name: r.name,
        department: r.department || '',
        profileImage: r.profileImage || null,
        active_tasks: Number(r.active_tasks) || 0,
        delegated_to_others: Number(r.delegated_to_others) || 0,
        revises_triggered: Number(r.revises_triggered) || 0,
        leaves_submitted: Number(r.leaves_submitted) || 0,
        activityScore:
          (Number(r.active_tasks) || 0) +
          (Number(r.delegated_to_others) || 0) +
          (Number(r.revises_triggered) || 0) +
          (Number(r.leaves_submitted) || 0)
      }))
      .filter(r => r.activityScore > 0)
      .sort((a, b) => b.activityScore - a.activityScore);
    res.json(scored);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── FMS MIS ──
app.get('/api/mis/fms', requireAuth, requireMisViewer, async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'Dates required' });
    const isHod = req.session.role === 'hod';
    const uid = req.session.userId;

    // Get FMS sheets
    const [sheets] = await db.query('SELECT * FROM fms_sheets ORDER BY fms_name ASC');
    if (!sheets.length) return res.json([]);

    // Fetch the HOD's department first (once)
    let hodDept = '';
    if (isHod) {
      const [meRow] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
      hodDept = meRow[0]?.department || '';
    }

    const results = [];

    const stepsBySheet = await fmsStepsBySheet(sheets.map(s => s.id));
    const doersByStep = await fmsDoersByStep(
      [...stepsBySheet.values()].flat().map(s => s.id),
      'fsd.user_id, u.name, u.department');

    for (const sheet of sheets) {
      // Get all steps with doers
      const steps = stepsBySheet.get(sheet.id) || [];
      for (const step of steps) {
        step.doers = doersByStep.get(step.id) || [];
      }

      // HOD: only the steps that have doers from their department
      const filteredSteps = isHod
        ? steps.filter(s => s.doers.some(d => d.department === hodDept))
        : steps;
      if (isHod && filteredSteps.length === 0) continue;

      // Build per-user per-step stats from Google Sheet
      try {
        const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
        const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
        const tabName = sheet.sheet_name || 'Sheet1';
        const headerRowIdx = (sheet.header_row || 1) - 1;

        const allCols = filteredSteps.flatMap(s => [colToIdx(s.plan_col), colToIdx(s.actual_col)]).filter(x => x >= 0);
        if (!allCols.length) continue;
        const maxCol = Math.max(...allCols);
        const lastCol = idxToCol(maxCol);
        const range = `${tabName}!A:${lastCol}`;

        const response = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range });
        const allRowsData = response.data.values || [];
        const dataRows = allRowsData.slice(headerRowIdx + 1);

        // Per-FMS aggregate stats
        let fmsPending = 0, fmsDone = 0, fmsTotal = 0;
        const perStepStats = [];

        for (const step of filteredSteps) {
          const planIdx = colToIdx(step.plan_col);
          const actualIdx = colToIdx(step.actual_col);
          if (planIdx < 0 || actualIdx < 0) continue;

          let stepPending = 0, stepDone = 0;
          dataRows.forEach(row => {
            const planVal = (row[planIdx]||'').trim();
            const actualVal = (row[actualIdx]||'').trim();
            if (planVal && !actualVal) stepPending++;
            if (planVal && actualVal) stepDone++;
          });

          fmsPending += stepPending;
          fmsDone += stepDone;
          fmsTotal += stepPending + stepDone;

          const stepDoerNames = step.doers.map(d=>d.name).join(', ') || '—';

          perStepStats.push({
            stepName: step.step_name,
            stepOrder: step.step_order,
            doers: stepDoerNames,
            pending: stepPending,
            done: stepDone,
            total: stepPending + stepDone
          });
        }

        if (perStepStats.length > 0 || !isHod) {
          results.push({
            fmsId: sheet.id,
            fmsName: sheet.fms_name || sheet.sheet_name,
            pending: fmsPending,
            done: fmsDone,
            total: fmsTotal,
            steps: perStepStats
          });
        }
      } catch(e) {
        results.push({
          fmsId: sheet.id,
          fmsName: sheet.fms_name || sheet.sheet_name,
          pending: 0, done: 0, total: 0,
          steps: [], error: e.message
        });
      }
    }

    res.json(results);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── PC: Users with pending tasks (for smart dropdown) ──
app.get('/api/users/with-pending-tasks', requireAuth, async (req, res) => {
  try {
    const { dateFrom, dateTo } = req.query;
    const dateRe2 = /^\d{4}-\d{2}-\d{2}$/;
    const hasRange = dateFrom && dateTo && dateRe2.test(dateFrom) && dateRe2.test(dateTo);
    const dateFilter = hasRange ? 'AND t.due_date BETWEEN ? AND ?' : 'AND t.due_date <= CURDATE()';
    const dateParams = hasRange ? [dateFrom, dateTo, dateFrom, dateTo] : [];
    const [rows] = await db.query(`
      SELECT DISTINCT u.id, u.name FROM users u
      WHERE u.id IN (
        SELECT DISTINCT assigned_to FROM delegation_tasks t WHERE status='pending' ${dateFilter}
        UNION
        SELECT DISTINCT assigned_to FROM checklist_tasks t WHERE status='pending' ${dateFilter}
      ) AND u.role NOT IN ('admin','pc')
      ORDER BY u.name ASC`, dateParams);
    res.json(rows);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// USERS
// ══════════════════════════════════════════════════════
// Page keys an admin can grant to non-admin users via the user-edit checkboxes.
// Single source of truth for both the dropdown and server-side validation.
// 'dailyreports' is deliberately absent. Every Daily Reports endpoint is
// requireAdmin — the report itself, both reminder previews, both sends — so
// granting the page here only ever produced a page that answered "Admin only"
// to each of its own calls, while the Access Control panel (which never reads
// extra_access) went on showing No Access for the same user.
const EXTRA_ACCESS_KEYS = ['race','mis','fms','users','clients','compliance','leaves_all','pending_summary_recipient'];
function sanitizeExtraAccess(input) {
  let arr = input;
  if (typeof input === 'string') {
    try { arr = JSON.parse(input); } catch { arr = input.split(',').map(s => s.trim()); }
  }
  if (!Array.isArray(arr)) return [];
  return [...new Set(arr.filter(k => EXTRA_ACCESS_KEYS.includes(k)))];
}
function parseExtraAccess(raw) {
  if (!raw) return [];
  try {
    const a = JSON.parse(raw);
    return Array.isArray(a) ? a : [];
  } catch { return []; }
}

app.get('/api/access/pages', requireAuth, requireAdmin, (_req, res) => {
  res.json([
    { key: 'race',                      label: 'Race Tracker' },
    { key: 'mis',                       label: 'MIS Report' },
    { key: 'fms',                       label: 'FMS Admin' },
    { key: 'users',                     label: 'Users' },
    { key: 'clients',                   label: 'Clients' },
    { key: 'compliance',                label: 'Compliance Tracker' },
    { key: 'leaves_all',                label: 'Leaves — Full Team Report' },
    { key: 'pending_summary_recipient', label: 'Receive Pending Task Summary on WhatsApp' }
  ]);
});

app.get('/api/users', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT id,name,email,notification_email,role,
              COALESCE(user_role, role) AS user_role,
              phone,department,week_off,extra_off,
              COALESCE(exclude_from_reminder,0) AS exclude_from_reminder,
              extra_access
       FROM users WHERE role <> 'client' AND client_id IS NULL ORDER BY name ASC`
    );
    // extra_access is permission data, and this list is the company directory —
    // every dropdown in the app fills from it, so it cannot be locked down. It
    // can stop carrying who has been granted what: only the Users tab reads this
    // field, and that page is admin-only. Everyone else gets the directory
    // without the permissions.
    const seesPerms = req.session.role === 'admin';
    for (const r of rows) {
      if (seesPerms) r.extra_access = parseExtraAccess(r.extra_access);
      else delete r.extra_access;
    }
    // birthday/joining_date fetched separately — safe before migration runs
    try {
      const ids = rows.map(r=>r.id);
      if (ids.length) {
        const [bd] = await db.query(`SELECT id,birthday,joining_date FROM users WHERE id IN (${ids.map(()=>'?').join(',')})`, ids);
        const bdMap = Object.fromEntries(bd.map(u=>[u.id, u]));
        for (const r of rows) { r.birthday = bdMap[r.id]?.birthday || null; r.joining_date = bdMap[r.id]?.joining_date || null; }
      }
    } catch(e) { for (const r of rows) { r.birthday = null; r.joining_date = null; } }
    // user_permissions fetched separately — safe before server restart runs migration
    try {
      const ids = rows.map(r=>r.id);
      if (ids.length) {
        const [ups] = await db.query(`SELECT id,user_permissions FROM users WHERE id IN (${ids.map(()=>'?').join(',')})`, ids);
        const upMap = Object.fromEntries(ups.map(u=>[u.id, u.user_permissions]));
        for (const r of rows) {
          const raw = upMap[r.id];
          r.user_permissions = raw ? (() => { try { return JSON.parse(raw); } catch { return null; } })() : null;
        }
      }
    } catch(e) { for (const r of rows) r.user_permissions = null; }
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/users', requireAuth, requireAdmin, async (req, res) => {
  try {
    // `position` is not a users column — it is sent by the Add User form for the
    // team welcome announcement below and nothing else.
    const { name, email, notification_email, password, role, user_role, phone, department, position, week_off, extra_off, exclude_from_reminder, extra_access, birthday, joining_date } = req.body;
    if (!name || !email || !password) return res.status(400).json({ error: 'All fields required' });
    const [ex] = await db.query('SELECT id FROM users WHERE email=?', [email]);
    if (ex[0]) return res.status(400).json({ error: 'Email already exists' });
    const validRoles = ['admin','hod','pc','user'];
    const appRole = validRoles.includes(role) ? role : 'user';
    const userRole = validRoles.includes(user_role) ? user_role : appRole;
    const accessJson = JSON.stringify(sanitizeExtraAccess(extra_access));
    await db.query('INSERT INTO users (name,email,notification_email,password,role,user_role,phone,department,week_off,extra_off,exclude_from_reminder,extra_access,birthday,joining_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [name, email, notification_email||'', bcrypt.hashSync(password,10), appRole, userRole, phone||null, department||'', week_off||'', extra_off||'', exclude_from_reminder?1:0, accessJson, birthday||null, joining_date||null]);
    // Credentials now go by EMAIL to the new user (was WhatsApp).
    const credMsg = `Hi ${name},\nWelcome to e-marketing. We are granting you access to the our task manager.🌸\n\nhttps://taskmanager.e-marketing.io/app\nid : ${email}\npass : ${password}`;
    const credTo = notification_email || email;
    if (credTo) sendMail(credTo, 'Welcome to E-Marketing — Your Task Manager Login', waTextToEmailHtml(credMsg)).catch(e => console.error('new user email err:', e.message));
    // Team welcome announcement — the position they are appointed to, falling
    // back to the department and then to a plain "team member" when blank.
    const joinedAs = String(position || '').trim() || department || 'team member';
    // "a"/"an" by pronunciation, the same rule the offer letter uses: acronyms
    // go by the first letter's NAME (SEO = "es" -> "an SEO Executive").
    const _jaFirst = joinedAs.split(/\s+/)[0] || '';
    const joinedArticle = (/^[A-Z]{2,}$/.test(_jaFirst) ? /^[AEFHILMNORSX]/.test(_jaFirst) : /^[aeiouAEIOU]/.test(_jaFirst)) ? 'an' : 'a';
    const welcomeMsg = `Hello Team,\nPlease join me in welcoming ${name} our new team member who has joined us as ${joinedArticle} ${joinedAs}.\nWe are excited to have them on board and look forward to working together.\nWelcome to the team, ${name}! 🌸`;
    sendWhatsAppRaw('919602694444-1618492040@g.us', welcomeMsg).catch(e => console.error('WA team welcome err:', e.message));
    // Append new user to Google Sheet
    const SHEET_ID = '1k8GTp731LMNE6E1_FwNO8yvGJu7ogo-4PX6c7JP4emM';
    const fmtDate = d => { if (!d) return ''; const [y,m,dd] = d.split('-'); return `${dd}/${m}/${y}`; };
    getSheetsClient(['https://www.googleapis.com/auth/spreadsheets']).then(sheets => sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: 'Sheet1!A:D',
      valueInputOption: 'USER_ENTERED',
      resource: { values: [[name, fmtDate(birthday), fmtDate(joining_date), 'Active']] }
    })).catch(e => console.error('Sheets append err:', e.message));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/users/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { name, email, notification_email, role, user_role, password, phone, department, week_off, extra_off, exclude_from_reminder, extra_access, birthday, joining_date } = req.body;
    const exclVal = exclude_from_reminder ? 1 : 0;
    const validRoles = ['admin','hod','pc','user'];
    const appRole = validRoles.includes(role) ? role : 'user';
    const userRole = validRoles.includes(user_role) ? user_role : appRole;
    const accessJson = JSON.stringify(sanitizeExtraAccess(extra_access));
    if (password) await db.query('UPDATE users SET name=?,email=?,notification_email=?,role=?,user_role=?,password=?,phone=?,department=?,week_off=?,extra_off=?,exclude_from_reminder=?,extra_access=?,birthday=?,joining_date=? WHERE id=?',
      [name,email,notification_email||'',appRole,userRole,bcrypt.hashSync(password,10),phone||null,department||'',week_off||'',extra_off||'',exclVal,accessJson,birthday||null,joining_date||null,req.params.id]);
    else await db.query('UPDATE users SET name=?,email=?,notification_email=?,role=?,user_role=?,phone=?,department=?,week_off=?,extra_off=?,exclude_from_reminder=?,extra_access=?,birthday=?,joining_date=? WHERE id=?',
      [name,email,notification_email||'',appRole,userRole,phone||null,department||'',week_off||'',extra_off||'',exclVal,accessJson,birthday||null,joining_date||null,req.params.id]);
    // Update Google Sheet row matching this user's name
    const SHEET_ID = '1k8GTp731LMNE6E1_FwNO8yvGJu7ogo-4PX6c7JP4emM';
    const fmtDate = d => { if (!d) return ''; const [y,m,dd] = d.split('-'); return `${dd}/${m}/${y}`; };
    getSheetsClient(['https://www.googleapis.com/auth/spreadsheets']).then(async sheets => {
      const get = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'Sheet1!A:A' });
      const rows = get.data.values || [];
      const rowIdx = rows.findIndex(r => r[0] === name);
      if (rowIdx >= 1) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID,
          range: `Sheet1!B${rowIdx+1}:C${rowIdx+1}`,
          valueInputOption: 'USER_ENTERED',
          resource: { values: [[fmtDate(birthday), fmtDate(joining_date)]] }
        });
      }
    }).catch(e => console.error('Sheets update err:', e.message));
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// One-time migration: populate birthday & joining_date from sheet data
app.post('/api/admin/migrate-birthdays', requireAuth, requireAdmin, async (req, res) => {
  const DATA = [
    { name: 'Akhilesh Vyas',      birthday: '2001-04-28', joining_date: '2025-06-02' },
    { name: 'Taaran Jain',        birthday: '2003-04-25', joining_date: '2025-06-16' },
    { name: 'Priya Saini',        birthday: '1997-10-07', joining_date: '2025-05-12' },
    { name: 'Garvit Kedia',       birthday: '2002-04-08', joining_date: '2024-04-14' },
    { name: 'Purvi Saini',        birthday: '2003-11-21', joining_date: '2024-12-04' },
    { name: 'Nisha Madaan',       birthday: '1989-11-14', joining_date: '2024-11-10' },
    { name: 'Nupur Kothari',      birthday: '1999-05-17', joining_date: '2024-09-23' },
    { name: 'Aman Bejal',         birthday: '2001-05-03', joining_date: '2024-07-16' },
    { name: 'Akshita Jain',       birthday: '2004-12-13', joining_date: '2024-03-01' },
    { name: 'Divya Srivastava',   birthday: '2001-07-12', joining_date: '2023-12-11' },
    { name: 'Tushar Chauhan',     birthday: '1998-08-01', joining_date: '2023-07-20' },
    { name: 'Ritu Tilokani',      birthday: '2002-01-07', joining_date: '2023-06-12' },
    { name: 'Sakshi Saini',       birthday: '2001-10-12', joining_date: '2023-04-03' },
    { name: 'Pradhuman Kumar',    birthday: '1987-12-09', joining_date: '2023-04-01' },
    { name: 'Saurav Pareek',      birthday: '1999-01-14', joining_date: '2023-02-13' },
    { name: 'Satish Khichi',      birthday: '1989-12-27', joining_date: '2022-04-06' },
    { name: 'Kritika Saini',      birthday: '1998-11-08', joining_date: '2022-04-04' },
    { name: 'Rotan Singh',        birthday: '1984-02-29', joining_date: '2021-11-11' },
    { name: 'Swati Joshi',        birthday: '1992-10-20', joining_date: '2021-06-16' },
    { name: 'Divyy Jain',         birthday: '2003-03-31', joining_date: '2025-09-29' },
    { name: 'Kushagra Dubey',     birthday: '2004-06-08', joining_date: '2025-10-10' },
    { name: 'Nikita khandelwal',  birthday: '2002-07-27', joining_date: '2025-11-03' },
    { name: 'Bhanu sharma',       birthday: '2005-12-04', joining_date: '2025-12-03' },
    { name: 'Abhishek Samriya',   birthday: '2004-10-29', joining_date: '2025-12-15' },
    { name: 'Harsh Daharwal',     birthday: '2003-02-20', joining_date: '2026-01-05' },
    { name: 'Simran Gurnani',     birthday: '1999-03-05', joining_date: '2022-01-21' },
    { name: 'Aman Pareek',        birthday: '2006-10-11', joining_date: '2026-02-25' },
    { name: 'Gaurav Gupta',       birthday: '2002-11-12', joining_date: '2026-03-30' },
    { name: 'Vishal Jaga',        birthday: '2001-06-12', joining_date: '2026-04-06' },
    { name: 'Ashish Jha',         birthday: '1999-10-20', joining_date: '2026-04-13' },
    { name: 'Chirag',             birthday: '2001-09-03', joining_date: '2026-05-01' },
    { name: 'Naman Gupta',        birthday: '2004-08-24', joining_date: '2026-05-25' },
  ];
  try {
    // Step 1: ensure columns exist
    try { await db.query(`ALTER TABLE users ADD COLUMN birthday DATE DEFAULT NULL`); } catch(e) {}
    try { await db.query(`ALTER TABLE users ADD COLUMN joining_date DATE DEFAULT NULL`); } catch(e) {}
    // Step 2: populate data
    const results = [];
    for (const row of DATA) {
      const [r] = await db.query('UPDATE users SET birthday=?, joining_date=? WHERE name=?', [row.birthday, row.joining_date, row.name]);
      results.push({ name: row.name, updated: r.affectedRows > 0 });
    }
    res.json({ ok: true, results });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/send-birthday-reminder', requireAuth, requireAdmin, async (req, res) => {
  const msg = `Hello Everyone! 🌸\n\nKindly request the following team members to update their Birthday and Joining Date on the task manager profile page:\n\n• Chetna Agrawal\n• Chirag Thakral\n• Divvy Jain\n• Diya Khandelwal\n• Nikhil Jain\n• Rahul Meharchandani\n\nThank you!`;
  try {
    await sendWhatsAppRaw('919602694444-1618492040@g.us', msg);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/users/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    if (parseInt(req.params.id) === req.session.userId) return res.status(400).json({ error: 'Cannot delete yourself' });
    const [doomed] = await db.query('SELECT * FROM users WHERE id=?', [req.params.id]);
    await archiveDeleted('users', doomed, req, { summary: r => `User: ${r.name || ''} <${r.email || ''}>` });
    await db.query('DELETE FROM users WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk add users via CSV
app.post('/api/users/bulk', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { users } = req.body;
    if (!users || !users.length) return res.status(400).json({ error: 'No users provided' });
    const validRoles = ['admin','hod','pc','user'];
    let added = 0, skipped = 0, errors = [];
    for (const u of users) {
      if (!u.name || !u.email || !u.password) { errors.push(`${u.email||'?'}: missing fields`); continue; }
      const [ex] = await db.query('SELECT id FROM users WHERE email=?', [u.email]);
      if (ex[0]) { skipped++; continue; }
      const appRole = validRoles.includes(u.role) ? u.role : 'user';
      const userRole = validRoles.includes(u.user_role) ? u.user_role : appRole;
      await db.query('INSERT INTO users (name,email,password,role,user_role,phone,department,week_off,extra_off) VALUES (?,?,?,?,?,?,?,?,?)',
        [u.name, u.email, bcrypt.hashSync(u.password,10), appRole, userRole, u.phone||null, u.department||'', u.week_off||'', u.extra_off||'']);
      added++;
    }
    res.json({ success: true, added, skipped, errors });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Per-user permission overrides
const VALID_UP_PAGES   = new Set(['dashboard','alltasks','approvals','mis','race','fms','fms-tasks','daily','clients','compliance','dailyreports','leaves','meetings','inventory','hrm','users','dms','paymentreq','feedback','creditcards','logs']);
// The edit_<page> keys carry the Access Control panel's "Editor" level for
// features that have no individually gated buttons. They are stored now so the
// choice survives; a page starts honouring it as soon as its controls are
// wired to canDo('edit_<page>'). Keep this in sync with PERM_TREE in app.html.
const VALID_UP_ACTIONS = new Set(['edit_task','delete_task','create_task','create_checklist','approve_revision','bulk_approve','transfer_task','reopen_task','delete_leave','set_plan','hrm_schedule','hrm_update_status',
  'edit_dashboard','edit_mis','edit_race','edit_fms','edit_fms_tasks','edit_clients','edit_compliance','edit_dailyreports','edit_meetings','edit_inventory','edit_dms','edit_paymentreq','edit_feedback','edit_users','edit_creditcards','edit_logs']);

// ── Server-side mirror of the frontend's canSee() / canDo() ──────────────
// Until this existed, `user_permissions` was write-only as far as the API was
// concerned: the Access Control panel could grant a page, the nav would show
// it, and then every endpoint behind it still refused on its own hardcoded
// role check. Routes that want to honour an admin's grant call these.
//
// SERVER_ROLE_DEFAULTS must stay identical to ROLE_DEFAULTS in public/app.html
// — the two are the same fallback, and drift between them shows up as the UI
// offering a page the API then rejects.
const SERVER_ROLE_DEFAULTS = {
  // HR Portal is admin-only by decision (2026-08-11). 'hrm' leaves the page list
  // AND hrm_schedule / hrm_update_status leave the action list, because the HRM
  // write routes gate on the action alone — see POST /api/hrm/candidates. Taking
  // only the page away would have hidden the tab while leaving a hod able to
  // create candidates, change status and send offer letters through the API.
  hod:  { pages: ['dashboard','alltasks','approvals','mis','clients','leaves','meetings','daily','fms-tasks','inventory','compliance','paymentreq','feedback','creditcards'],
          // edit_inventory and edit_clients are here so those pages' routes could
          // start asking for a permission instead of a hardcoded role list
          // without taking anything away from a hod. Seed the default to match
          // what the role can already do, THEN gate on it — otherwise the first
          // person to deploy the gate discovers it by losing access.
          //
          // ⚠ edit_clients is a small, deliberate widening, named rather than
          // buried: a hod could already create clients, reassign handlers, upload
          // logos, bulk-import and mint portal logins, but NOT rename one,
          // because PUT /api/clients/:id asked for edit_clients while every
          // neighbouring route asked for admin-or-hod. That split was arbitrary.
          actions: ['edit_task','delete_task','create_task','create_checklist','transfer_task','reopen_task','approve_revision','set_plan','delete_leave','edit_inventory','edit_clients'] },
  pc:   { pages: ['dashboard','alltasks','approvals','clients','leaves','meetings','daily','fms-tasks','inventory','dms','compliance','paymentreq','feedback','creditcards'],
          actions: ['approve_revision','bulk_approve','create_task','reopen_task','edit_task','delete_task'] },
  user: { pages: ['dashboard','alltasks','approvals','leaves','meetings','daily','inventory','compliance','clients','paymentreq','feedback','creditcards'],
          actions: ['create_task','edit_task','delete_task'] }
};

// Mirrors canSee()'s cascade exactly: an explicit user_permissions row wins
// outright, then extra_access, then the role defaults.
async function getEffectivePerms(session) {
  if (session.role === 'admin') return 'all';
  let stored = null, extra = [];
  try {
    const [[row]] = await db.query('SELECT user_permissions, extra_access FROM users WHERE id=?', [session.userId]);
    if (row?.user_permissions) { try { stored = JSON.parse(row.user_permissions); } catch {} }
    extra = parseExtraAccess(row?.extra_access);
  } catch {}
  const def = SERVER_ROLE_DEFAULTS[session.role] || { pages: [], actions: [] };
  return {
    pages:   (stored && Array.isArray(stored.pages))   ? stored.pages   : [...def.pages, ...extra],
    actions: (stored && Array.isArray(stored.actions)) ? stored.actions : [...def.actions]
  };
}
// The role defaults, served so app.html can stop carrying its own copy. Only
// the Access Control panel needs these — it renders what OTHER roles get, which
// /api/me cannot answer since that only describes the caller. Admin-only for the
// same reason: nobody else opens that panel.
//
// This is what finally makes SERVER_ROLE_DEFAULTS the single definition. The
// warning above it — "must stay identical to ROLE_DEFAULTS in public/app.html"
// — describes a hazard that no longer exists, because the second copy is gone.
app.get('/api/access/role-defaults', requireAuth, requireAdmin, (_req, res) => {
  res.json(SERVER_ROLE_DEFAULTS);
});

async function userCanSee(session, page) {
  const p = await getEffectivePerms(session);
  return p === 'all' || p.pages.includes(page);
}
async function userCanDo(session, action) {
  const p = await getEffectivePerms(session);
  return p === 'all' || p.actions.includes(action);
}

app.put('/api/user-permissions/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const userId = parseInt(req.params.id);
    if (!userId) return res.status(400).json({ error: 'Invalid user ID' });
    const { pages, actions } = req.body;
    if (!Array.isArray(pages) || !Array.isArray(actions)) return res.status(400).json({ error: 'Invalid data' });
    const cleanPages   = pages.filter(p => VALID_UP_PAGES.has(p));
    const cleanActions = actions.filter(a => VALID_UP_ACTIONS.has(a));
    await db.query('UPDATE users SET user_permissions=? WHERE id=?',
      [JSON.stringify({ pages: cleanPages, actions: cleanActions }), userId]);
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// PATCH /api/users/:id/role — role-only update from the Access Control panel.
// PUT /api/users/:id rewrites every profile column, so it cannot be used to
// change just the role without the caller re-sending the whole user.
app.patch('/api/users/:id/role', requireAuth, requireAdmin, async (req, res) => {
  try {
    const userId = parseInt(req.params.id);
    if (!userId) return res.status(400).json({ error: 'Invalid user ID' });
    const role = req.body.role;
    // 'client' is deliberately excluded: a client login is tied to a clients
    // row via users.client_id, so switching roles into/out of it here would
    // leave that link inconsistent. Use the Users form for those.
    if (!['admin','hod','pc','user'].includes(role)) return res.status(400).json({ error: 'Invalid role' });
    if (userId === req.session.userId && role !== 'admin')
      return res.status(400).json({ error: 'You cannot remove your own admin role' });
    const [[target]] = await db.query('SELECT role FROM users WHERE id=?', [userId]);
    if (!target) return res.status(404).json({ error: 'User not found' });
    if (target.role === 'client') return res.status(400).json({ error: 'Client logins cannot be changed here' });
    await db.query('UPDATE users SET role=? WHERE id=?', [role, userId]);
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// PROFILE
// ══════════════════════════════════════════════════════
app.put('/api/profile', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const { name, email, notification_email, phone, birthday, joining_date, currentPassword, newPassword, profileImage } = req.body;
    if (currentPassword) {
      const [rows] = await db.query('SELECT password FROM users WHERE id=?', [uid]);
      if (!bcrypt.compareSync(currentPassword, rows[0].password)) return res.status(400).json({ error: 'Current password is incorrect' });
      if (newPassword) await db.query('UPDATE users SET name=?,email=?,notification_email=?,phone=?,birthday=?,joining_date=?,password=? WHERE id=?', [name,email,notification_email||'',phone||null,birthday||null,joining_date||null,bcrypt.hashSync(newPassword,10),uid]);
      else await db.query('UPDATE users SET name=?,email=?,notification_email=?,phone=?,birthday=?,joining_date=? WHERE id=?', [name,email,notification_email||'',phone||null,birthday||null,joining_date||null,uid]);
    } else {
      await db.query('UPDATE users SET name=?,email=?,notification_email=?,phone=?,birthday=?,joining_date=? WHERE id=?', [name,email,notification_email||'',phone||null,birthday||null,joining_date||null,uid]);
    }
    if (profileImage !== undefined) await db.query('UPDATE users SET profile_image=? WHERE id=?', [profileImage||null, uid]);
    req.session.name = name;
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/profile/image', requireAuth, async (req, res) => {
  try {
    await db.query('UPDATE users SET profile_image=? WHERE id=?', [req.body.image||null, req.session.userId]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// COMMENTS
// ══════════════════════════════════════════════════════
app.get('/api/comments/:type/:taskId', requireAuth, async (req, res) => {
  try {
    // Comments are a conversation about one task, so seeing them follows seeing
    // the task. Both ids arrive in the URL; without this, walking the number
    // read every discussion in the company.
    if (!(await canSeeTask(req, req.params.taskId, req.params.type))) {
      return res.status(403).json({ error: 'Not allowed' });
    }
    const [rows] = await db.query(`SELECT tc.id,tc.comment,tc.created_at,u.name AS userName FROM task_comments tc JOIN users u ON tc.user_id=u.id WHERE tc.task_id=? AND tc.task_type=? ORDER BY tc.created_at ASC`, [req.params.taskId, req.params.type]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/comments', requireAuth, async (req, res) => {
  try {
    const { taskId, taskType, comment } = req.body;
    if (!comment || !taskId || !taskType) return res.status(400).json({ error: 'All fields required' });
    // Writing followed the same open path as reading — the comment was stored
    // under the caller's own name, but against whatever task id they sent.
    if (!(await canSeeTask(req, taskId, taskType))) return res.status(403).json({ error: 'Not allowed' });
    await db.query('INSERT INTO task_comments (task_id,task_type,user_id,comment) VALUES (?,?,?,?)', [taskId, taskType, req.session.userId, comment]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/comments/:id', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query('SELECT * FROM task_comments WHERE id=?', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    if (Number(rows[0].user_id) !== Number(req.session.userId) && req.session.role !== 'admin') return res.status(403).json({ error: 'Not allowed' });
    await archiveDeleted('task_comments', rows[0], req, { summary: r => `Comment: ${r.comment || ''}` });
    await db.query('DELETE FROM task_comments WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// FMS routes now live in routes/fms.js. The call sits exactly
// where the routes did, so every binding passed in is in scope at the same
// point it always was.
require('./backend/routes/fms')(app, {
  db,
  requireAuth,
  requireAdmin,
  archiveDeleted,
  colToIdx,
  idxToCol,
  extractSpreadsheetId,
  getSheetsClient,
});

// ══════════════════════════════════════════════════════
// TASK TRANSFERS
// ══════════════════════════════════════════════════════

// POST — Create transfer request (user/hod/admin)
app.post('/api/transfers', requireAuth, async (req, res) => {
  try {
    const { tasks, toUserId } = req.body;
    // tasks = [{taskId, taskType}]
    if (!tasks || !tasks.length || !toUserId)
      return res.status(400).json({ error: 'Tasks and target user required' });
    // The "🔀 Transfer" button already hides behind this key; the route did not
    // ask for it, so the ownership rules below were the only limit.
    if (!(await userCanDo(req.session, 'transfer_task'))) {
      return res.status(403).json({ error: 'You do not have access to transfer tasks' });
    }

    const uid = req.session.userId;
    const role = req.session.role;

    // Validate each task — user can only transfer their own, HOD dept, admin any
    for (const t of tasks) {
      const table = getTable(t.taskType);
      const [rows] = await db.query(`SELECT * FROM ${table} WHERE id=?`, [t.taskId]);
      if (!rows[0]) return res.status(404).json({ error: `Task ${t.taskId} not found` });
      const task = rows[0];

      if (role === 'user' && Number(task.assigned_to) !== Number(uid))
        return res.status(403).json({ error: 'You can only transfer your own tasks' });

      if (role === 'hod') {
        const [taskUser] = await db.query('SELECT department FROM users WHERE id=?', [task.assigned_to]);
        const [hodUser] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
        if (taskUser[0]?.department !== hodUser[0]?.department)
          return res.status(403).json({ error: 'HOD can only transfer tasks of their department' });
      }
    }

    // Insert transfer requests — skip if already pending
    let inserted = 0, skipped = 0;
    for (const t of tasks) {
      const table = getTable(t.taskType);
      const [rows] = await db.query(`SELECT assigned_to FROM ${table} WHERE id=?`, [t.taskId]);
      const fromUser = rows[0].assigned_to;
      const [existing] = await db.query(
        `SELECT id FROM task_transfers WHERE task_id=? AND task_type=? AND status='pending'`,
        [t.taskId, t.taskType]
      );
      if (existing[0]) { skipped++; continue; }
      await db.query(
        `INSERT INTO task_transfers (task_id, task_type, from_user, to_user, requested_by, status) VALUES (?,?,?,?,?,'pending')`,
        [t.taskId, t.taskType, fromUser, toUserId, uid]
      );
      inserted++;
    }

    res.json({ success: true, count: inserted, skipped });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET — Task IDs that already have a pending transfer (for current user's tasks)
app.get('/api/transfers/pending-tasks', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT task_id, task_type FROM task_transfers WHERE status='pending' AND requested_by=?`,
      [req.session.userId]
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET — Pending transfers for approval (admin sees all, HOD sees dept)
app.get('/api/transfers', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const uid = req.session.userId;
    const role = req.session.role;
    let deptFilter = '';
    let params = [];

    if (role === 'hod') {
      const [me] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
      const dept = me[0]?.department || '';
      // HOD sees transfers of users in their department
      const [deptUsers] = await db.query('SELECT id FROM users WHERE department=?', [dept]);
      if (!deptUsers.length) return res.json([]);
      const ids = deptUsers.map(u=>u.id);
      deptFilter = `AND (tt.from_user IN (${ids.map(()=>'?').join(',')}) OR tt.to_user IN (${ids.map(()=>'?').join(',')}))`;
      params = [...ids, ...ids];
    }

    const [rows] = await db.query(`
      SELECT tt.*,
        uf.name AS fromUserName, ut.name AS toUserName,
        ur.name AS requestedByName,
        u_from.department AS fromDept
      FROM task_transfers tt
      JOIN users uf ON tt.from_user = uf.id
      JOIN users ut ON tt.to_user = ut.id
      JOIN users ur ON tt.requested_by = ur.id
      JOIN users u_from ON tt.from_user = u_from.id
      WHERE tt.status = 'pending' ${deptFilter}
      ORDER BY tt.created_at DESC`, params);

    // Attach task description
    for (const r of rows) {
      const table = getTable(r.task_type);
      const [t] = await db.query(`SELECT description, DATE_FORMAT(due_date,'%Y-%m-%d') AS due_date FROM ${table} WHERE id=?`, [r.task_id]);
      r.description = t[0]?.description || '—';
      r.due_date = t[0]?.due_date || '—';
    }

    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET — Transfer count for badge
app.get('/api/transfers/count', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const uid = req.session.userId;
    const role = req.session.role;
    let count = 0;
    if (role === 'admin') {
      const [r] = await db.query(`SELECT COUNT(*) AS c FROM task_transfers WHERE status='pending'`);
      count = r[0].c;
    } else {
      const [me] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
      const dept = me[0]?.department || '';
      const [deptUsers] = await db.query('SELECT id FROM users WHERE department=?', [dept]);
      if (deptUsers.length) {
        const ids = deptUsers.map(u=>u.id);
        const [r] = await db.query(`SELECT COUNT(*) AS c FROM task_transfers WHERE status='pending' AND (from_user IN (${ids.map(()=>'?').join(',')}) OR to_user IN (${ids.map(()=>'?').join(',')}))`, [...ids,...ids]);
        count = r[0].c;
      }
    }
    res.json({ count });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// PUT — Approve or reject transfer
app.put('/api/transfers/:id', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const { action, note } = req.body;
    // The comment here used to say 'approved' | 'rejected' and nothing enforced
    // it. status is an ENUM, and non-strict MySQL turns an unexpected value into
    // the empty string rather than erroring — leaving a transfer that is neither
    // pending nor decided: gone from the pending list, task never moved, and the
    // request still answered success.
    if (!['approved', 'rejected'].includes(action)) {
      return res.status(400).json({ error: 'action must be approved or rejected' });
    }
    const [rows] = await db.query('SELECT * FROM task_transfers WHERE id=?', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Transfer not found' });
    const tr = rows[0];

    // An HOD may only act on transfers touching their own department. Creating
    // one already checks this and so does the pending count, but approving did
    // not — requireAdminOrHod asks "an HOD?", never "which HOD?" — so any HOD
    // could reassign a task between two people in another department. Not
    // reachable by accident, since the list and count are both scoped, but
    // nothing stopped it either, and transfers leave no audit row.
    //
    // Ordered before the pending check on purpose: an HOD with no business here
    // should be turned away without being told whether the transfer is still
    // open or how it was decided.
    if (req.session.role === 'hod') {
      const [[me]] = await db.query('SELECT department FROM users WHERE id=?', [req.session.userId]);
      const dept = me?.department || '';
      const [parties] = await db.query(
        'SELECT department FROM users WHERE id IN (?,?)', [tr.from_user, tr.to_user]);
      // A blank department must not match another blank one into access.
      if (!dept || !parties.some(p => p.department === dept)) {
        return res.status(403).json({ error: 'You can only act on transfers in your own department' });
      }
    }

    // Deciding a transfer is a one-time act. Without this a rejected transfer
    // could be approved later and would move the task, so two people looking at
    // the same stale list could overwrite each other's decision silently.
    if (tr.status !== 'pending') {
      return res.status(409).json({ error: `This transfer was already ${tr.status}` });
    }

    await db.query('UPDATE task_transfers SET status=?, note=? WHERE id=?', [action, note||'', req.params.id]);

    if (action === 'approved') {
      const table = getTable(tr.task_type);
      await db.query(`UPDATE ${table} SET assigned_to=? WHERE id=?`, [tr.to_user, tr.task_id]);
      // The one change that moves a task to a different person, and until now
      // it left nothing behind: "my task went to someone else" was unanswerable.
      const [names] = await db.query('SELECT id, name FROM users WHERE id IN (?,?)', [tr.from_user, tr.to_user]);
      const nameOf = id => names.find(n => Number(n.id) === Number(id))?.name || `#${id}`;
      logTaskActivity({
        taskId: tr.task_id, taskType: tr.task_type, field: 'assigned_to',
        oldValue: tr.from_user, newValue: tr.to_user,
        changedBy: req.session.userId, source: 'transfer-approved',
        note: `${nameOf(tr.from_user)} -> ${nameOf(tr.to_user)}`
      });
    }
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET — My sent transfer requests (for users to track)
app.get('/api/transfers/my', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(`
      SELECT tt.*, uf.name AS fromUserName, ut.name AS toUserName
      FROM task_transfers tt
      JOIN users uf ON tt.from_user = uf.id
      JOIN users ut ON tt.to_user = ut.id
      WHERE tt.requested_by=?
      ORDER BY tt.created_at DESC LIMIT 20`, [req.session.userId]);
    for (const r of rows) {
      const table = getTable(r.task_type);
      const [t] = await db.query(`SELECT description FROM ${table} WHERE id=?`, [r.task_id]);
      r.description = t[0]?.description || '—';
    }
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// WEEK PLAN
// ══════════════════════════════════════════════════════
// The "📅 Set Plan" button hides behind canDo('set_plan'); this route asked for
// admin-or-hod instead. Same people either way — hod carries set_plan in its
// default and admin passes everything — but now the panel decides rather than a
// hardcoded pair of roles. The GET stays on requireAdminOrHod: reading the plan
// is not the act the key describes.
app.post('/api/week-plan', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'set_plan'))) {
    return res.status(403).json({ error: 'You do not have access to set the weekly plan' });
  }
  try {
    const { employeeId, startDate, targetCount, hodId, improvementPct } = req.body;
    if (!employeeId || !startDate) {
      return res.status(400).json({ error: 'employeeId and startDate required' });
    }
    const impPct = (improvementPct !== undefined && improvementPct !== null && improvementPct !== '') ? parseInt(improvementPct) : null;
    // Upsert: insert or update if same employee+startDate exists
    await db.query(
      `INSERT INTO week_plans (employee_id, hod_id, start_date, target_count, improvement_pct, created_at)
       VALUES (?, ?, ?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE target_count = VALUES(target_count), hod_id = VALUES(hod_id), improvement_pct = VALUES(improvement_pct), created_at = NOW()`,
      [employeeId, hodId || req.session.userId, startDate, targetCount, impPct]
    );
    res.json({ success: true });
  } catch (e) {
    // If table doesn't exist, create it first then retry
    if (e.code === 'ER_NO_SUCH_TABLE') {
      await db.query(`
        CREATE TABLE IF NOT EXISTS week_plans (
          id INT AUTO_INCREMENT PRIMARY KEY,
          employee_id INT NOT NULL,
          hod_id INT NOT NULL,
          start_date DATE NOT NULL,
          target_count INT NOT NULL,
          improvement_pct INT DEFAULT NULL,
          created_at DATETIME,
          UNIQUE KEY uq_emp_week (employee_id, start_date)
        )
      `);
      const { employeeId, startDate, targetCount, hodId, improvementPct } = req.body;
      const impPct = (improvementPct !== undefined && improvementPct !== null && improvementPct !== '') ? parseInt(improvementPct) : null;
      await db.query(
        `INSERT INTO week_plans (employee_id, hod_id, start_date, target_count, improvement_pct, created_at)
         VALUES (?, ?, ?, ?, ?, NOW())
         ON DUPLICATE KEY UPDATE target_count = VALUES(target_count), hod_id = VALUES(hod_id), improvement_pct = VALUES(improvement_pct), created_at = NOW()`,
        [employeeId, hodId || req.session.userId, startDate, targetCount, impPct]
      );
      return res.json({ success: true });
    }
    // If improvement_pct column missing (old table), add it then retry
    if (e.code === 'ER_BAD_FIELD_ERROR') {
      try {
        await db.query(`ALTER TABLE week_plans ADD COLUMN improvement_pct INT DEFAULT NULL`);
      } catch(ae) { /* already exists */ }
      const { employeeId, startDate, targetCount, hodId, improvementPct } = req.body;
      const impPct = (improvementPct !== undefined && improvementPct !== null && improvementPct !== '') ? parseInt(improvementPct) : null;
      await db.query(
        `INSERT INTO week_plans (employee_id, hod_id, start_date, target_count, improvement_pct, created_at)
         VALUES (?, ?, ?, ?, ?, NOW())
         ON DUPLICATE KEY UPDATE target_count = VALUES(target_count), hod_id = VALUES(hod_id), improvement_pct = VALUES(improvement_pct), created_at = NOW()`,
        [employeeId, hodId || req.session.userId, startDate, targetCount, impPct]
      );
      return res.json({ success: true });
    }
    console.error(e);
    res.json({ error: 'Failed to save plan' });
  }
});

app.get('/api/week-plan', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT wp.*, u.name as employee_name FROM week_plans wp
       JOIN users u ON u.id = wp.employee_id
       ORDER BY wp.start_date DESC LIMIT 50`
    );
    res.json(rows);
  } catch (e) {
    res.json([]);
  }
});

// ══════════════════════════════════════════════════════
// 📆 MONDAY WEEKLY CHECK-IN — per-user self-commitment + last week recap
// ══════════════════════════════════════════════════════
// Helper — returns YYYY-MM-DD of the Monday of the IST week containing `date`.
function istMondayOf(date) {
  const ist = new Date(date.getTime() + (5.5 * 60 * 60 * 1000));
  const dayUTC = ist.getUTCDay(); // 0=Sun, 1=Mon..6=Sat
  const diff = (dayUTC === 0 ? -6 : 1 - dayUTC); // shift back to Monday
  const mon = new Date(Date.UTC(ist.getUTCFullYear(), ist.getUTCMonth(), ist.getUTCDate() + diff));
  return mon.toISOString().split('T')[0];
}
function addDays(yyyyMmDd, n) {
  const d = new Date(yyyyMmDd + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().split('T')[0];
}

// Score formula — matches existing MIS calc. Returns score in [-100, 0].
function scoreFor(total, pending, overdue, revised) {
  total = parseInt(total)||0; pending = parseInt(pending)||0;
  overdue = parseInt(overdue)||0; revised = parseInt(revised)||0;
  if (total <= 0) return null;
  return Math.max(-100, Math.round((0 - (pending/total)*100 - (overdue/total)*50 - (revised/total)*25)*10)/10);
}

// Aggregates last/this-week numbers for ONE user (the caller).
async function getMyWeekBundle(userId) {
  const now = new Date();
  const istToday = new Date(now.getTime() + (5.5 * 60 * 60 * 1000));
  const todayStr = istToday.toISOString().split('T')[0];
  const istDayOfWeek = istToday.getUTCDay(); // 0=Sun..6=Sat

  const thisMon = istMondayOf(now);
  const thisSun = addDays(thisMon, 6);
  const lastMon = addDays(thisMon, -7);
  const lastSun = addDays(thisMon, -1);

  // Compute stats for a given window (delegation + checklist)
  async function statsFor(start, end) {
    const [del] = await db.query(
      `SELECT COUNT(*) AS total,
              SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
              SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
              SUM(CASE WHEN status='revised'   THEN 1 ELSE 0 END) AS revised,
              SUM(CASE WHEN status='pending' AND due_date < ? THEN 1 ELSE 0 END) AS overdue
         FROM delegation_tasks WHERE assigned_to=? AND due_date BETWEEN ? AND ?`,
      [todayStr, userId, start, end]);
    const [chl] = await db.query(
      `SELECT COUNT(*) AS total,
              SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
              SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
              SUM(CASE WHEN status='pending' AND due_date < ? THEN 1 ELSE 0 END) AS overdue
         FROM checklist_tasks WHERE assigned_to=? AND due_date BETWEEN ? AND ?`,
      [todayStr, userId, start, end]);
    const d = del[0] || {}, c = chl[0] || {};
    const dPack = {
      total: +d.total||0, pending: +d.pending||0, completed: +d.completed||0,
      overdue: +d.overdue||0, revised: +d.revised||0,
      score: scoreFor(d.total, d.pending, d.overdue, d.revised)
    };
    const cPack = {
      total: +c.total||0, pending: +c.pending||0, completed: +c.completed||0,
      overdue: +c.overdue||0, revised: 0,
      score: scoreFor(c.total, c.pending, c.overdue, 0)
    };
    const totalAll = dPack.total + cPack.total;
    const pendAll  = dPack.pending + cPack.pending;
    const overAll  = dPack.overdue + cPack.overdue;
    const revAll   = dPack.revised;
    return {
      delegation: dPack, checklist: cPack,
      overall: {
        total: totalAll, pending: pendAll, overdue: overAll, revised: revAll,
        completed: dPack.completed + cPack.completed,
        score: scoreFor(totalAll, pendAll, overAll, revAll)
      }
    };
  }

  const [lastStats, thisStats] = await Promise.all([
    statsFor(lastMon, lastSun),
    statsFor(thisMon, thisSun)
  ]);

  // Pull this & last week plan rows
  const [planRows] = await db.query(
    `SELECT DATE_FORMAT(start_date,'%Y-%m-%d') AS start_date,
            user_committed_score, target_count, improvement_pct,
            DATE_FORMAT(checkin_skipped_until,'%Y-%m-%d') AS checkin_skipped_until
       FROM week_plans WHERE employee_id=? AND start_date IN (?, ?)`,
    [userId, thisMon, lastMon]);
  const planByMon = {};
  for (const p of planRows) planByMon[p.start_date] = p;

  return {
    todayStr, istDayOfWeek,
    thisWeek: { start: thisMon, end: thisSun, plan: planByMon[thisMon] || null, stats: thisStats },
    lastWeek: { start: lastMon, end: lastSun, plan: planByMon[lastMon] || null, stats: lastStats }
  };
}

// Lightweight status — used by app bootstrap to decide whether to pop the modal.
// Fires on EVERY page load, so it must be cheap: one row lookup, no stats.
app.get('/api/my-week-status', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const now = new Date();
    const istToday = new Date(now.getTime() + (5.5 * 60 * 60 * 1000));
    const todayStr = istToday.toISOString().split('T')[0];
    const istDayOfWeek = istToday.getUTCDay(); // 0=Sun..6=Sat

    // Early exit if today isn't Mon/Tue/Wed — no DB hit at all on Thu–Sun.
    const dayOK = istDayOfWeek >= 1 && istDayOfWeek <= 3;
    const thisMon = istMondayOf(now);
    if (!dayOK) {
      return res.json({ needsCheckin: false, todayStr, istDayOfWeek, thisWeekStart: thisMon, lastWeekStart: addDays(thisMon, -7) });
    }

    // Single-row plan lookup — cheap, replaces full getMyWeekBundle.
    const [planRows] = await db.query(
      `SELECT user_committed_score,
              DATE_FORMAT(checkin_skipped_until,'%Y-%m-%d') AS checkin_skipped_until
         FROM week_plans WHERE employee_id=? AND start_date=? LIMIT 1`,
      [uid, thisMon]);
    const thisPlan = planRows[0] || null;
    const committed = thisPlan && thisPlan.user_committed_score !== null && thisPlan.user_committed_score !== undefined;
    const skipUntil = thisPlan && thisPlan.checkin_skipped_until;
    const snoozed = skipUntil && todayStr <= skipUntil;
    res.json({
      needsCheckin: !committed && !snoozed,
      todayStr,
      istDayOfWeek,
      thisWeekStart: thisMon,
      lastWeekStart: addDays(thisMon, -7)
    });
  } catch (err) {
    console.error('my-week-status error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Full bundle — used when the modal opens.
app.get('/api/my-week-data', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const bundle = await getMyWeekBundle(uid);
    res.json(bundle);
  } catch (err) {
    console.error('my-week-data error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Save the user's committed score for a given Monday.
app.post('/api/my-week-plan', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const { startDate, committedScore } = req.body || {};
    if (!startDate || !/^\d{4}-\d{2}-\d{2}$/.test(startDate)) {
      return res.status(400).json({ error: 'startDate (YYYY-MM-DD) required' });
    }
    const score = parseFloat(committedScore);
    if (isNaN(score) || score < -100 || score > 0) {
      return res.status(400).json({ error: 'committedScore must be between -100 and 0' });
    }
    await db.query(
      `INSERT INTO week_plans (employee_id, start_date, user_committed_score, user_committed_at, created_at)
       VALUES (?, ?, ?, NOW(), NOW())
       ON DUPLICATE KEY UPDATE user_committed_score=VALUES(user_committed_score), user_committed_at=NOW()`,
      [uid, startDate, score]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error('my-week-plan save error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Snooze the Monday check-in until tomorrow (or to end of this week).
app.post('/api/my-week-plan/snooze', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const bundle = await getMyWeekBundle(uid);
    const thisMon = bundle.thisWeek.start;
    // Snooze until tomorrow (IST)
    const tomorrow = addDays(bundle.todayStr, 1);
    await db.query(
      `INSERT INTO week_plans (employee_id, start_date, checkin_skipped_until, created_at)
       VALUES (?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE checkin_skipped_until=VALUES(checkin_skipped_until)`,
      [uid, thisMon, tomorrow]
    );
    res.json({ ok: true, snoozedUntil: tomorrow });
  } catch (err) {
    console.error('my-week-plan snooze error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Detail drill-down — list of tasks for the user in a week window.
app.get('/api/my-week-tasks', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const { type, start, end } = req.query;
    if (!start || !end || !/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)) {
      return res.status(400).json({ error: 'start, end (YYYY-MM-DD) required' });
    }
    const table = type === 'delegation' ? 'delegation_tasks' : 'checklist_tasks';
    if (!['delegation','checklist'].includes(type)) {
      return res.status(400).json({ error: 'type must be delegation or checklist' });
    }
    const [tasks] = await db.query(
      `SELECT t.id, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u.name AS assigned_by_name
         FROM ${table} t LEFT JOIN users u ON u.id=t.assigned_by
         WHERE t.assigned_to=? AND t.due_date BETWEEN ? AND ?
         ORDER BY t.due_date ASC, t.id ASC`,
      [uid, start, end]);
    res.json({ tasks });
  } catch (err) {
    console.error('my-week-tasks error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Parse FMS plan-column values that look like dates (DD-MM-YYYY, DD/MM/YYYY,
// YYYY-MM-DD, optional trailing time). Returns YYYY-MM-DD or null.
function parseFmsPlanDate(val) {
  if (!val) return null;
  const v = String(val).trim();
  let m = v.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
  if (m) {
    const [, d, mo, y] = m;
    const dt = new Date(`${y}-${mo.padStart(2,'0')}-${d.padStart(2,'0')}T00:00:00Z`);
    return isNaN(dt.getTime()) ? null : dt.toISOString().split('T')[0];
  }
  m = v.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/);
  if (m) {
    const [, y, mo, d] = m;
    const dt = new Date(`${y}-${mo.padStart(2,'0')}-${d.padStart(2,'0')}T00:00:00Z`);
    return isNaN(dt.getTime()) ? null : dt.toISOString().split('T')[0];
  }
  return null;
}

// FMS rows for the caller within a date window (planned date in [start, end]).
// Reads each fms_sheet the user is a doer in; safe for non-FMS users (returns []).
// Core: FMS rows for ONE user. By default returns every row in their assigned
// sheets where the plan column is filled (matches /api/mis/all aggregate counts).
// Pass { applyDateFilter: true } to restrict to rows whose plan-date falls in
// [start, end] — used by the Monday check-in last-week view.
async function fmsTasksForUserInRange(uid, start, end, opts = {}) {
  const applyDateFilter = opts.applyDateFilter === true;
  const [doerSteps] = await db.query(
    `SELECT fs.id AS step_id, fs.step_name, fs.fms_id, fs.plan_col, fs.actual_col,
            fsh.fms_name, fsh.sheet_name, fsh.sheet_id, fsh.header_row
       FROM fms_step_doers fsd
       JOIN fms_steps  fs  ON fs.id = fsd.step_id
       JOIN fms_sheets fsh ON fsh.id = fs.fms_id
      WHERE fsd.user_id = ?`,
    [uid]);
  if (!doerSteps.length) return [];

  const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']).catch(() => null);
  if (!sheetsApi) return [];

  // Group by sheet so we fetch each spreadsheet once even if user has multiple steps in it
  const bySheet = {};
  for (const s of doerSteps) {
    if (!bySheet[s.fms_id]) bySheet[s.fms_id] = { sheet: s, steps: [] };
    bySheet[s.fms_id].steps.push(s);
  }

  const tasks = [];
  for (const fmsId of Object.keys(bySheet)) {
    const { sheet, steps } = bySheet[fmsId];
    try {
      const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
      const tabName = sheet.sheet_name || 'Sheet1';
      const headerRowIdx = (sheet.header_row || 1) - 1;
      const allCols = steps.flatMap(s => [colToIdx(s.plan_col), colToIdx(s.actual_col)]).filter(x => x >= 0);
      if (!allCols.length) continue;
      const lastCol = idxToCol(Math.max(...allCols));
      const response = await sheetsApi.spreadsheets.values.get({
        spreadsheetId, range: `${tabName}!A:${lastCol}`
      });
      const rows = (response.data.values || []).slice(headerRowIdx + 1);

      for (const step of steps) {
        const planIdx = colToIdx(step.plan_col);
        const actualIdx = colToIdx(step.actual_col);
        if (planIdx < 0) continue;
        rows.forEach((row, i) => {
          const planVal = (row[planIdx] || '').toString().trim();
          if (!planVal) return;
          const planDate = parseFmsPlanDate(planVal);
          if (applyDateFilter && (!planDate || planDate < start || planDate > end)) return;
          const actualVal = actualIdx >= 0 ? (row[actualIdx] || '').toString().trim() : '';
          tasks.push({
            fmsName: sheet.fms_name || sheet.sheet_name,
            stepName: step.step_name,
            planValue: planVal,
            actualValue: actualVal,
            planDate: planDate || '',
            status: actualVal ? 'completed' : 'pending',
            rowNumber: headerRowIdx + 1 + i + 1
          });
        });
      }
    } catch (e) { /* skip this sheet on error */ }
  }
  tasks.sort((a, b) => (a.planDate || '').localeCompare(b.planDate || ''));
  return tasks;
}

app.get('/api/my-week-fms-tasks', requireAuth, async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end || !/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)) {
      return res.status(400).json({ error: 'start, end (YYYY-MM-DD) required' });
    }
    // Monday check-in cares about last-week-only rows — keep the date filter on.
    const tasks = await fmsTasksForUserInRange(req.session.userId, start, end, { applyDateFilter: true });
    res.json({ tasks });
  } catch (err) {
    console.error('my-week-fms-tasks error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Calendar feed — the caller's own tasks (delegation + checklist + FMS) whose
// due/plan date falls in [from, to]. Returned as a flat list with a `date` field
// so the Meetings calendar can show tasks alongside meetings on each day.
app.get('/api/calendar/tasks', requireAuth, async (req, res) => {
  try {
    const { from, to } = req.query;
    const isDate = v => /^\d{4}-\d{2}-\d{2}$/.test(v || '');
    if (!isDate(from) || !isDate(to)) return res.status(400).json({ error: 'from, to (YYYY-MM-DD) required' });
    const uid = req.session.userId;

    const [del] = await db.query(
      `SELECT t.id, t.description, t.status, COALESCE(t.priority,'low') AS priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS date, c.name AS client_name
         FROM delegation_tasks t LEFT JOIN clients c ON t.client_id=c.id
        WHERE t.assigned_to=? AND t.due_date BETWEEN ? AND ?`, [uid, from, to]);
    const [chl] = await db.query(
      `SELECT t.id, t.description, t.status, COALESCE(t.priority,'low') AS priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS date, c.name AS client_name
         FROM checklist_tasks t LEFT JOIN clients c ON t.client_id=c.id
        WHERE t.assigned_to=? AND t.due_date BETWEEN ? AND ?`, [uid, from, to]);

    let fms = [];
    try { fms = await fmsTasksForUserInRange(uid, from, to, { applyDateFilter: true }); } catch (e) { /* FMS optional */ }

    const items = [
      ...del.map(t => ({ type: 'delegation', id: t.id, date: t.date, title: t.description, status: t.status, priority: t.priority, client_name: t.client_name })),
      ...chl.map(t => ({ type: 'checklist', id: t.id, date: t.date, title: t.description, status: t.status, priority: t.priority, client_name: t.client_name })),
      ...fms.map(t => ({ type: 'fms', date: t.planDate, title: `${t.fmsName} · ${t.stepName}`, status: t.status }))
    ].filter(x => x.date);

    res.json({ items });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Admin / HOD endpoint — FMS rows for ANY user in a date range (used by Race
// Tracker / MIS detail drill-down).
app.get('/api/mis/fms-detail', requireAuth, requireMisViewer, async (req, res) => {
  try {
    const { userId, start, end } = req.query;
    if (!userId || !start || !end || !/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)) {
      return res.status(400).json({ error: 'userId, start, end (YYYY-MM-DD) required' });
    }
    // Same gap as /api/mis/detail — the FMS drill-down trusted the query string
    // while /api/mis/fms scoped the list it came from. See the note there.
    if (!(await misHodMaySee(req.session, userId))) {
      return res.status(403).json({ error: 'That employee is not in your department' });
    }
    // Date filter ON so the drill-down rows match the aggregate counts shown
    // on the Race Tracker / MIS card (both filter by plan-date in [start, end]).
    const tasks = await fmsTasksForUserInRange(parseInt(userId), start, end, { applyDateFilter: true });
    res.json({ tasks });
  } catch (err) {
    console.error('mis/fms-detail error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════
// DEBUG ENDPOINT (remove after fixing)
// ══════════════════════════════════════════════════════
app.get('/api/debug', requireAuth, requireAdmin, async (req, res) => {
  const result = { time: new Date().toISOString(), env: {}, db: {}, tables: {} };
  result.env = {
    NODE_ENV: process.env.NODE_ENV || '(not set)',
    DB_HOST: process.env.DB_HOST || 'localhost (default)',
    DB_USER: process.env.DB_USER || 'root (default)',
    DB_NAME: process.env.DB_NAME || 'emarketing_task_manager (default)',
    PORT: process.env.PORT || '3000 (default)',
  };
  try {
    await db.query('SELECT 1');
    result.db.connected = true;
    const counts = ['users','delegation_tasks','checklist_tasks','fms_sheets'];
    for (const t of counts) {
      try {
        const [[row]] = await db.query(`SELECT COUNT(*) AS c FROM ${t}`);
        result.tables[t] = row.c;
      } catch(e) { result.tables[t] = 'ERROR: ' + e.message; }
    }
    // Show users with their roles and departments
    try {
      const [users] = await db.query('SELECT id, name, role, department FROM users ORDER BY role, name');
      result.users = users;
    } catch(e) { result.users = 'ERROR: ' + e.message; }
  } catch(e) {
    result.db.connected = false;
    result.db.error = e.message;
  }
  res.json(result);
});

// ══════════════════════════════════════════════════════
// PAGES
// ══════════════════════════════════════════════════════
// ══════════════════════════════════════════════════════
// 📅 DAILY TASK FORM + CLIENTS + COMPLIANCE + WHATSAPP
// ══════════════════════════════════════════════════════

// Auto-create new tables on startup (safe, runs once per cold start)
const _clientsTableMigrationsPromise = (async () => {
  if (await _migrationsAlreadyApplied()) return;
  const sa = async (sql) => { try { await db.query(sql); } catch(e){} };
  await sa(`CREATE TABLE IF NOT EXISTS clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // Handler = the user (account manager) responsible for this client. Drives the
  // default doer in the "Delegate Task" shortcut on the Client Master row.
  await sa(`ALTER TABLE clients ADD COLUMN handler_id INT DEFAULT NULL AFTER name`);
  await sa(`ALTER TABLE clients ADD INDEX idx_handler (handler_id)`);
  // Logo — base64 data URL (client-side resized to 256x256 JPEG so payloads
  // stay small enough for the DB without external object storage).
  await sa(`ALTER TABLE clients ADD COLUMN logo_url LONGTEXT DEFAULT NULL AFTER handler_id`);
  // System Links — per-client quick links shown on the client portal. Stored as
  // a JSON array of { label, url }. Managed by admin/PC on the Client detail page.
  await sa(`ALTER TABLE clients ADD COLUMN system_links LONGTEXT DEFAULT NULL AFTER logo_url`);
  // Active flag — admin/PC marks a client active or inactive (e.g. churned). Drives
  // the active/inactive split in the Compliance → Employee 360 view used at increment time.
  await sa(`ALTER TABLE clients ADD COLUMN is_active TINYINT(1) NOT NULL DEFAULT 1 AFTER system_links`);
  // Multiple handlers per client (many-to-many). handler_id stays as primary handler for backward compat.
  await sa(`CREATE TABLE IF NOT EXISTS client_handlers (
    client_id INT NOT NULL,
    user_id   INT NOT NULL,
    PRIMARY KEY (client_id, user_id),
    KEY idx_ch_user (user_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // Seed from existing handler_id values so old data is preserved.
  await sa(`INSERT IGNORE INTO client_handlers (client_id, user_id) SELECT id, handler_id FROM clients WHERE handler_id IS NOT NULL`);
  // Client feedback submitted via the client portal.
  await sa(`CREATE TABLE IF NOT EXISTS client_feedback (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL,
    employee_id INT NOT NULL,
    rating TINYINT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_cfb_client (client_id),
    KEY idx_cfb_employee (employee_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  await sa(`ALTER TABLE client_feedback ADD COLUMN recipients TEXT`);
  // Allow "client" as a login role + back-link users to clients so the client
  // portal can resolve "my client" from the session.
  await sa(`ALTER TABLE users MODIFY COLUMN role ENUM('admin','hod','pc','user','client') DEFAULT 'user'`);
  await sa(`ALTER TABLE users MODIFY COLUMN user_role ENUM('admin','hod','pc','user','client') DEFAULT NULL`);
  await sa(`ALTER TABLE users ADD COLUMN client_id INT DEFAULT NULL AFTER extra_access`);
  await sa(`ALTER TABLE users ADD INDEX idx_client (client_id)`);

  await sa(`CREATE TABLE IF NOT EXISTS daily_tasks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    entry_date DATE NOT NULL,
    client_name VARCHAR(255) NOT NULL,
    department VARCHAR(255) DEFAULT '',
    description TEXT NOT NULL,
    duration_min INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_date (user_id, entry_date),
    INDEX idx_entry_date (entry_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // ── Meetings ────────────────────────────────────────────
  await sa(`CREATE TABLE IF NOT EXISTS meetings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    agenda TEXT DEFAULT NULL,
    client_id INT DEFAULT NULL,
    organizer_id INT NOT NULL,
    meeting_date DATE NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    meet_link VARCHAR(2048) DEFAULT NULL,
    status ENUM('scheduled','cancelled','done') DEFAULT 'scheduled',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_date (meeting_date),
    INDEX idx_organizer (organizer_id),
    INDEX idx_client (client_id),
    INDEX idx_status (status)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  await sa(`CREATE TABLE IF NOT EXISTS meeting_attendees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    meeting_id INT NOT NULL,
    user_id INT NOT NULL,
    UNIQUE KEY uq_meeting_user (meeting_id, user_id),
    INDEX idx_meeting (meeting_id),
    INDEX idx_user (user_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // 10-minute pre-meeting reminder flag — set when the cron fires the reminder
  // so we never double-send. Cleared if the meeting is rescheduled.
  await sa(`ALTER TABLE meetings ADD COLUMN reminder_sent TINYINT(1) DEFAULT 0 AFTER status`);
  await sa(`ALTER TABLE meetings ADD INDEX idx_reminder (meeting_date, start_time, reminder_sent, status)`);
  // Groups the individual meeting rows generated by a recurring schedule (e.g.
  // "daily with a client") so they share an id — lets us tell they came from
  // one form submission even though each occurrence is its own row.
  await sa(`ALTER TABLE meetings ADD COLUMN recurrence_group_id VARCHAR(40) DEFAULT NULL AFTER status`);
  await sa(`ALTER TABLE meetings ADD INDEX idx_recurrence (recurrence_group_id)`);

  // Day-view quick-add — lightweight personal plan entries ("9am to 10am meeting"),
  // separate from formal client meetings.
  await sa(`CREATE TABLE IF NOT EXISTS day_plan_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    item_date DATE NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    title VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_date (user_id, item_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // Generic key-value store for runtime settings (e.g. OAuth refresh tokens)
  await sa(`CREATE TABLE IF NOT EXISTS app_settings (
    key_name  VARCHAR(100) PRIMARY KEY,
    value     TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // DMS — Google Drive folder IDs per client and per department
  await sa(`ALTER TABLE clients ADD COLUMN drive_folder_id VARCHAR(255) DEFAULT NULL AFTER is_active`);
  // This client's own WhatsApp group (e.g. "1203634...@g.us"), used for the
  // pending-task digest. Left NULL the digest simply skips that client — the
  // shared client group must never receive one client's task list, since every
  // other client can read it there.
  await sa(`ALTER TABLE clients ADD COLUMN whatsapp_group_id VARCHAR(255) DEFAULT NULL AFTER drive_folder_id`);
  await sa(`CREATE TABLE IF NOT EXISTS client_department_folders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL,
    department_name VARCHAR(255) NOT NULL,
    drive_folder_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_client_dept (client_id, department_name),
    INDEX idx_cdf_client (client_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // Who-did-what log for DMS files created/renamed/deleted through the app
  // (Drive itself only ever sees our single shared service account, so this
  // is the only way to attribute an app-driven change to a real staff member).
  await sa(`CREATE TABLE IF NOT EXISTS dms_file_activity (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_id VARCHAR(255) NOT NULL,
    action VARCHAR(20) NOT NULL,
    file_name VARCHAR(500) DEFAULT NULL,
    user_id INT NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_dfa_file (file_id, created_at)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // client_id lets the DMS "Clients" root table show "last activity anywhere
  // in this client's folder" — Drive's own folder modifiedTime does NOT bump
  // when a child file is added/changed, so that alone can't drive this.
  await sa(`ALTER TABLE dms_file_activity ADD COLUMN client_id INT DEFAULT NULL AFTER file_id`);
  await sa(`ALTER TABLE dms_file_activity ADD INDEX idx_dfa_client (client_id, created_at)`);
  // Name+link entries pasted into a client's DMS folder — NOT real Drive
  // objects, just our own DB rows merged into the file listing. Replaces the
  // old Drive-shortcut approach, which required the target to be shared with
  // our service account first (impractical when we don't own the sharing).
  await sa(`CREATE TABLE IF NOT EXISTS dms_external_links (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL,
    folder_id VARCHAR(255) NOT NULL,
    name VARCHAR(500) NOT NULL,
    url TEXT NOT NULL,
    created_by INT NOT NULL,
    created_by_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_del_folder (folder_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // Admin-only credential vault: the logins for whatever the team builds for a
  // client (a bespoke task manager, a dashboard, etc.). These are the client's
  // OWN external systems, not our portal login — passwords are stored as typed
  // because the whole point is to read them back, so the routes are admin-only.
  await sa(`CREATE TABLE IF NOT EXISTS client_credentials (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL,
    system_name VARCHAR(255) NOT NULL,
    role_label VARCHAR(100) DEFAULT NULL,
    url VARCHAR(1000) DEFAULT NULL,
    username VARCHAR(500) DEFAULT NULL,
    password VARCHAR(500) DEFAULT NULL,
    notes TEXT DEFAULT NULL,
    created_by INT DEFAULT NULL,
    created_by_name VARCHAR(255) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_cc_client (client_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // ── One-time data migration: Access Control page keys ─────────────────
  // These pages were added to the Access Control feature list after some users
  // already had a saved user_permissions row. canSee() trusts
  // user_permissions.pages exclusively when it exists, so without this those
  // users would silently LOSE pages they can currently reach: Payment Request
  // used to be shown to everyone, Feedback is granted by /api/feedback/access,
  // and Credit Cards by the CC_VIEWERS list — the last two are now ANDed with
  // canSee(), so the key has to be present or the AND fails. Append once, to
  // existing rows only; after that an admin's explicit revoke must stick,
  // which is why this is marker-guarded and never re-run.
  //
  // 'logs' is deliberately NOT in this list. It is admin-only by design and
  // admins bypass canSee() entirely, so the absent key keeps every non-admin
  // fail-closed even if the role check above it is ever loosened.
  //
  // Bumping the marker version re-runs the append with the new key set; the
  // appends are idempotent, so an already-patched row is simply left alone.
  try {
    const [[marker]] = await db.query(`SELECT value FROM app_settings WHERE key_name='perm_pages_backfill_v2'`);
    if (!marker) {
      const [rows] = await db.query(`SELECT id, user_permissions FROM users WHERE user_permissions IS NOT NULL AND user_permissions <> ''`);
      let patched = 0;
      for (const r of rows) {
        let up;
        try { up = JSON.parse(r.user_permissions); } catch { continue; }
        if (!up || !Array.isArray(up.pages)) continue;
        const before = up.pages.length;
        for (const key of ['dms','paymentreq','feedback','creditcards']) {
          if (!up.pages.includes(key)) up.pages.push(key);
        }
        if (up.pages.length === before) continue;
        await db.query(`UPDATE users SET user_permissions=? WHERE id=?`,
          [JSON.stringify({ pages: up.pages, actions: Array.isArray(up.actions) ? up.actions : [] }), r.id]);
        patched++;
      }
      await db.query(`INSERT INTO app_settings (key_name, value) VALUES ('perm_pages_backfill_v2', ?)`,
        [`patched ${patched} of ${rows.length} rows`]);
      console.log(`  ✅ Access Control page backfill — ${patched} user_permissions rows updated`);
    }
  } catch(e) { console.log('  ⚠️ Access Control page backfill skipped —', e.code || e.message); }

  // ── One-time: make the role the single source of truth for access ──
  //
  // Eighteen users carried a saved user_permissions row, and a stored row wins
  // OUTRIGHT over the role default in both canSee() and getEffectivePerms() —
  // so those eighteen had quietly drifted away from their own role. A live audit
  // on 2026-08-11 found six hods missing MIS Report while a seventh kept it,
  // Taaran Jain holding HR Portal, Vishal Jaga holding DMS, and Pranaya Pareek
  // holding Daily Reports. Same role, different sidebars, and nothing in the
  // panel showed why. Clearing the rows drops everyone back onto their role,
  // which is what the user asked for: role decides, and a new employee is
  // correct the moment they are created.
  //
  // What this costs the nine users who also held approve_revision /
  // bulk_approve / delete_leave / edit_creditcards: nothing. None of those four
  // is read by canDo() in app.html or by userCanDo() here — the panel's own
  // "Editor not enforced yet" label has been telling the truth. The actions that
  // ARE checked (create_task, create_checklist, transfer_task, set_plan,
  // reopen_task, edit_task, delete_task, hrm_schedule, hrm_update_status) all
  // sit in the role defaults these rows are being replaced by.
  //
  // Why this is safe against the backfill above: that block exists because
  // feedback and creditcards are ANDed with canSee(), so a missing key would
  // fail the AND. Every role default carries both keys, so falling back to a
  // default cannot lose them — Rotan Singh already runs on defaults today and
  // keeps his Credit Card Statement view.
  //
  // The rows are copied verbatim into app_settings first, so this is reversible
  // without a database backup — which matters here because the user has no
  // phpMyAdmin access and could not take one. To undo:
  //   SELECT value FROM app_settings WHERE key_name='user_permissions_backup_20260811';
  // then write each id's user_permissions back.
  try {
    const [[resetMarker]] = await db.query(`SELECT value FROM app_settings WHERE key_name='user_permissions_reset_v1'`);
    if (!resetMarker) {
      const [rows] = await db.query(
        `SELECT id, name, role, user_permissions FROM users
         WHERE user_permissions IS NOT NULL AND user_permissions <> ''`);
      if (!rows.length) {
        console.log('  ✅ Access Control reset — no saved rows, everyone already on role defaults');
      } else {
        // Written before the wipe, and only if absent, so a retry after a failed
        // UPDATE cannot overwrite a good backup with half-cleared state.
        const [[haveBackup]] = await db.query(
          `SELECT key_name FROM app_settings WHERE key_name='user_permissions_backup_20260811'`);
        if (!haveBackup) {
          await db.query(`INSERT INTO app_settings (key_name, value) VALUES ('user_permissions_backup_20260811', ?)`,
            [JSON.stringify(rows)]);
        }
        await db.query(`UPDATE users SET user_permissions=NULL WHERE user_permissions IS NOT NULL AND user_permissions <> ''`);
        console.log(`  ✅ Access Control reset — ${rows.length} rows cleared to role defaults: `
          + rows.map(r => `${r.name} (${r.role})`).join(', '));
      }
      await db.query(`INSERT INTO app_settings (key_name, value) VALUES ('user_permissions_reset_v1', ?)`,
        [`cleared ${rows.length} rows on first boot after the 2026-08-11 Access Control rebuild`]);
    }
  } catch(e) { console.log('  ⚠️ Access Control reset skipped —', e.code || e.message); }

  await seedPaymentRoleIds();

  console.log('  ✅ Daily Task + Meetings tables ready');
})();

// Stamp the marker only once BOTH migration blocks have finished, so a cold
// start that dies halfway leaves no marker and the next one replays in full.
// Two instances racing here is harmless — the DDL is idempotent and the write
// is an upsert. This sits below both promises on purpose: they are `const`, so
// naming _clientsTableMigrationsPromise any earlier would hit the temporal
// dead zone. See the cold-start migration guard near the top of this file.
const _migrationMarkerPromise = (async () => {
  if (!_DEPLOY_ID) return;
  try {
    await _startupMigrationsPromise;
    await _clientsTableMigrationsPromise;
    if (_migrationsSkipped) return; // marker already matches — nothing to write
    await db.query(
      `INSERT INTO app_settings (key_name, value) VALUES ('schema_deploy_marker', ?)
       ON DUPLICATE KEY UPDATE value = VALUES(value)`, [_DEPLOY_ID]);
    console.log('  ✅ Schema marker stamped — later cold starts will skip migrations');
  } catch (e) {
    // No marker written → the next cold start simply migrates again, as before.
    console.warn('  ⚠️ Schema marker not stamped:', e.message);
  }
})();

// ── WhatsApp helper (Aumpfy API) ──────────────────────
async function sendWhatsApp(phone, text) {
  if (process.env.WA_DISABLED === 'true') {
    console.log(`  🔇 WhatsApp send SKIPPED (WA_DISABLED) → ${phone}: ${text.slice(0, 60)}...`);
    return { ok: true, skipped: true };
  }
  const AUMPFY_URL = process.env.AUMPFY_URL || 'https://api.aumpfy.com/api/apis/trigger/emk-dbde65';
  const AUMPFY_API_KEY = process.env.AUMPFY_API_KEY || 'sl_f7f604b7eeb89f938399b888621a341f2183bceea4bcb9650f3b8a529d396bfe';

  if (!phone) return { ok: false, reason: 'no phone' };
  // Strip non-digits, ensure 91 prefix (India)
  let to = String(phone).replace(/\D/g, '');
  if (to.length === 10) to = '91' + to;          // 10-digit → add 91
  else if (to.length === 12 && to.startsWith('91')) {} // already correct
  else if (to.length === 11 && to.startsWith('0')) to = '91' + to.slice(1);
  else return { ok: false, reason: 'invalid phone format' };

  // Cap the provider call at 30s. Without it a stalled Aumpfy fetch hangs the
  // whole awaiting request — which is how a cron that sends WhatsApp (e.g. the
  // due-date nudge) would run past Vercel's 60s limit and never respond.
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 30000);
  try {
    const fetch = global.fetch || (await import('node-fetch')).default;
    const r = await fetch(AUMPFY_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': AUMPFY_API_KEY },
      body: JSON.stringify({ to, text }),
      signal: ctrl.signal
    });
    const data = await r.text();
    if (r.ok) {
      console.log(`  📱 WhatsApp sent → ${to}`);
      return { ok: true, response: data };
    } else {
      console.error(`  ❌ WhatsApp failed (${r.status}): ${data}`);
      return { ok: false, status: r.status, error: data };
    }
  } catch (err) {
    const msg = err.name === 'AbortError' ? 'provider timeout after 30s' : err.message;
    console.error('  ❌ WhatsApp error:', msg);
    return { ok: false, error: msg };
  } finally { clearTimeout(timer); }
}

// Raw send — used for WhatsApp group IDs (e.g. "120363400573269993@g.us")
// No phone formatting, no 91-prefix logic — sends "to" as-is.
async function sendWhatsAppRaw(to, text) {
  if (process.env.WA_DISABLED === 'true') {
    console.log(`  🔇 WhatsApp (raw) send SKIPPED (WA_DISABLED) → ${to}: ${text.slice(0, 60)}...`);
    return { ok: true, skipped: true };
  }
  const AUMPFY_URL = process.env.AUMPFY_URL || 'https://api.aumpfy.com/api/apis/trigger/emk-dbde65';
  const AUMPFY_API_KEY = process.env.AUMPFY_API_KEY || 'sl_f7f604b7eeb89f938399b888621a341f2183bceea4bcb9650f3b8a529d396bfe';

  if (!to) return { ok: false, reason: 'no destination' };

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 30000);
  try {
    const fetch = global.fetch || (await import('node-fetch')).default;
    const r = await fetch(AUMPFY_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': AUMPFY_API_KEY },
      body: JSON.stringify({ to: String(to), text }),
      signal: ctrl.signal
    });
    const data = await r.text();
    if (r.ok) {
      console.log(`  📱 WhatsApp (raw) sent → ${to}`);
      return { ok: true, response: data };
    } else {
      console.error(`  ❌ WhatsApp (raw) failed (${r.status}): ${data}`);
      return { ok: false, status: r.status, error: data };
    }
  } catch (err) {
    const msg = err.name === 'AbortError' ? 'provider timeout after 30s' : err.message;
    console.error('  ❌ WhatsApp (raw) error:', msg);
    return { ok: false, error: msg };
  } finally { clearTimeout(timer); }
}

// Test endpoint — visit /api/test-whatsapp?phone=98XXXXXXXX&text=hi to test
app.get('/api/test-whatsapp', requireAuth, requireAdmin, async (req, res) => {
  const result = await sendWhatsApp(req.query.phone, req.query.text || 'Test from E-Marketing Task Manager');
  res.json(result);
});

// ══════════════════════════════════════════════════════
// WhatsApp Bot — Task Delegation Approval Flow
// Bot sends task → pending in tasks → Naman approves/denies via link
// → approved tasks move to delegation_tasks → sender gets WhatsApp notification
// ══════════════════════════════════════════════════════

function waDelegationPage(title, message, isSuccess) {
  const color = isSuccess ? '#27ae60' : '#e74c3c';
  const icon  = isSuccess ? '✅' : '❌';
  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title}</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:Arial,sans-serif;background:#f5f5f5;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}
  .card{background:#fff;border-radius:16px;padding:48px 40px;text-align:center;box-shadow:0 4px 24px rgba(0,0,0,.1);max-width:400px;width:100%}
  .icon{font-size:56px;margin-bottom:16px}
  h1{color:${color};font-size:22px;margin-bottom:12px}
  p{color:#666;font-size:15px;line-height:1.6}
  .brand{color:#bbb;font-size:12px;margin-top:24px}
</style></head>
<body><div class="card">
  <div class="icon">${icon}</div>
  <h1>${title}</h1>
  <p>${message}</p>
  <p class="brand">E-Marketing Task Manager</p>
</div></body></html>`;
}

// POST /api/wa-bot/task
// Called by the WhatsApp bot when a user delegates a task via voice/text.
// Auth: X-Bot-Key header (set BOT_API_KEY in .env; default: emk_bot_2026)
app.post('/api/wa-bot/task', async (req, res) => {
  try {
    const botKey = req.headers['x-bot-key'] || req.body.bot_key;
    if (!botKey || botKey !== (process.env.BOT_API_KEY || 'emk_bot_2026')) {
      return res.status(401).json({ error: 'Invalid bot key' });
    }

    const { description, assigned_to, assigned_by, sender_phone, sender_name, due_date, priority, remarks, client_id, url } = req.body;
    if (!description) return res.status(400).json({ error: 'description is required' });

    const { randomBytes } = require('crypto');
    const token = randomBytes(32).toString('hex');

    const [result] = await db.query(
      `INSERT INTO tasks
         (description,assigned_to,assigned_by,sender_phone,sender_name,due_date,priority,remarks,client_id,url,approval_token)
       VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
      [description, assigned_to||null, assigned_by||null, sender_phone||null, sender_name||null,
       due_date||null, priority||'low', remarks||'', client_id||null, url||null, token]
    );

    // Look up Naman Gupta's phone from the users table
    const [naman] = await usersForSetting('wa_task_approver_ids', 'id, name, email, notification_email');

    const baseUrl = (process.env.APP_URL || 'http://localhost:3000').replace(/\/$/, '');

    const waMsg =
      `📋 *New WhatsApp Task — Approval Required*\n\n` +
      `*Task:* ${description}\n` +
      (due_date    ? `*Due Date:* ${due_date}\n`  : '') +
      (priority    ? `*Priority:* ${priority}\n`  : '') +
      (sender_name ? `*From:* ${sender_name}\n`   : '') +
      (remarks     ? `*Remarks:* ${remarks}\n`    : '') +
      `\n📲 *E-Marketing App* → Approvals → WhatsApp Tasks\n${baseUrl}/app`;

    // Approver notified by email now (was WhatsApp).
    const namanEmail = naman && (naman.notification_email || naman.email);
    if (namanEmail) {
      sendMail(namanEmail, 'New WhatsApp Task — Approval Required', waTextToEmailHtml(waMsg)).catch(e => console.error('approval notify email err:', e.message));
    }

    res.json({ ok: true, id: result.insertId, pending: true });
  } catch (err) {
    console.error('wa-bot/task err:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/wa-delegation/approve/:token — Naman clicks to approve the task
app.get('/api/wa-delegation/approve/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const [[row]] = await db.query(
      `SELECT * FROM tasks WHERE approval_token=? LIMIT 1`, [token]
    );

    if (!row) return res.status(404).send(waDelegationPage('Link Invalid', 'This approval link is invalid or has already expired.', false));
    if (row.status === 'approved') return res.send(waDelegationPage('Already Approved ✅', 'This task was already approved and added to the system.', true));
    if (row.status === 'denied')   return res.send(waDelegationPage('Already Denied', 'This task was already denied.', false));

    // Move task into delegation_tasks
    const [ins] = await db.query(
      `INSERT INTO delegation_tasks
         (description,assigned_to,assigned_by,due_date,status,priority,approval,remarks,client_id,url,awaiting_due_date)
       VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
      [row.description, row.assigned_to, row.assigned_by, row.due_date,
       'pending', row.priority, 'no', row.remarks, row.client_id, row.url,
       row.due_date ? 0 : 1]
    );

    await db.query(
      `UPDATE tasks SET status='approved', approved_task_id=? WHERE id=?`,
      [ins.insertId, row.id]
    );

    // Notify the sender via WhatsApp
    if (row.sender_phone) {
      const msg = `✅ *Task Approved!*\n\nYour task has been approved and added to the system.\n\n📋 *Task:* ${row.description}` +
        (row.due_date ? `\n📅 *Due Date:* ${row.due_date}` : '');
      notifyBotSender(row.sender_phone, 'Task Approved', msg);
    }

    return res.send(waDelegationPage('Task Approved ✅',
      `Task has been approved and added to the delegation system.<br><br><em>"${row.description}"</em>`, true));
  } catch (err) {
    console.error('wa-delegation approve err:', err.message);
    return res.status(500).send(waDelegationPage('Error', 'Something went wrong. Please try again.', false));
  }
});

// GET /api/wa-delegation/deny/:token — Naman clicks to deny the task
app.get('/api/wa-delegation/deny/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const [[row]] = await db.query(
      `SELECT * FROM tasks WHERE approval_token=? LIMIT 1`, [token]
    );

    if (!row) return res.status(404).send(waDelegationPage('Link Invalid', 'This link is invalid or has already expired.', false));
    if (row.status === 'approved') return res.send(waDelegationPage('Already Approved ✅', 'This task was already approved and added to the system.', true));
    if (row.status === 'denied')   return res.send(waDelegationPage('Already Denied', 'This task was already denied.', false));

    await db.query(`UPDATE tasks SET status='denied' WHERE id=?`, [row.id]);

    // Notify the sender via WhatsApp
    if (row.sender_phone) {
      const msg = `❌ *Task Not Approved*\n\nYour task was reviewed and was not approved.\n\n📋 *Task:* ${row.description}`;
      notifyBotSender(row.sender_phone, 'Task Not Approved', msg);
    }

    return res.send(waDelegationPage('Task Denied ❌',
      `Task has been denied.<br><br><em>"${row.description}"</em>`, false));
  } catch (err) {
    console.error('wa-delegation deny err:', err.message);
    return res.status(500).send(waDelegationPage('Error', 'Something went wrong. Please try again.', false));
  }
});

// ── Web app endpoints for WhatsApp delegation ─────────

// GET /api/wa-delegation — pending tasks for Naman to review in the app
app.get('/api/wa-delegation', requireAuth, async (req, res) => {
  try {
    const me = req.session;
    if (me.role !== 'admin') {
      return res.status(403).json({ error: 'Access denied' });
    }
    const [rows] = await db.query(
      `SELECT wd.id, wd.description, wd.status, wd.sender_phone, wd.sender_name,
              wd.due_date, wd.priority, wd.remarks, wd.created_at, wd.approval_token,
              wd.assigned_to, wd.assigned_by, wd.client_id, wd.url, wd.approved_task_id,
              u1.name AS assignedToName,
              u2.name AS assignedByName,
              c.name  AS clientName
       FROM tasks wd
       LEFT JOIN users u1 ON wd.assigned_to = u1.id
       LEFT JOIN users u2 ON wd.assigned_by = u2.id
       LEFT JOIN clients c ON wd.client_id  = c.id
       WHERE wd.status = 'pending'
       ORDER BY wd.created_at DESC`
    );
    console.log(`[wa-delegation] found ${rows.length} pending tasks`);
    res.json(rows);
  } catch (err) {
    console.error('[wa-delegation] list error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/wa-delegation/count — badge count (pending only)
app.get('/api/wa-delegation/count', requireAuth, async (req, res) => {
  try {
    const me = req.session;
    if (me.role !== 'admin') {
      return res.json({ count: 0 });
    }
    const [[{ cnt }]] = await db.query(
      `SELECT COUNT(*) AS cnt FROM tasks WHERE status='pending'`
    );
    res.json({ count: cnt || 0 });
  } catch (err) { res.json({ count: 0 }); }
});

// PUT /api/wa-delegation/:id — approve or deny from the web app
app.put('/api/wa-delegation/:id', requireAuth, async (req, res) => {
  try {
    const me = req.session;
    if (me.role !== 'admin') {
      return res.status(403).json({ error: 'Access denied' });
    }
    const { action } = req.body; // 'approved' | 'denied'
    if (!['approved', 'denied'].includes(action)) {
      return res.status(400).json({ error: 'action must be approved or denied' });
    }

    const [[row]] = await db.query(
      `SELECT * FROM tasks WHERE id=? LIMIT 1`, [req.params.id]
    );
    if (!row) return res.status(404).json({ error: 'Not found' });
    if (row.status !== 'pending') return res.status(400).json({ error: `Already ${row.status}` });

    if (action === 'approved') {
      const [ins] = await db.query(
        `INSERT INTO delegation_tasks
           (description,assigned_to,assigned_by,due_date,status,priority,approval,remarks,client_id,url,awaiting_due_date)
         VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
        [row.description, row.assigned_to, row.assigned_by, row.due_date,
         'pending', row.priority, 'no', row.remarks, row.client_id, row.url,
         row.due_date ? 0 : 1]
      );
      await db.query(
        `UPDATE tasks SET status='approved', approved_task_id=? WHERE id=?`,
        [ins.insertId, row.id]
      );
      // Notify the sender
      if (row.sender_phone) {
        const msg = `✅ *Task Approved!*\n\nYour task has been approved and added to the system.\n\n📋 *Task:* ${row.description}` +
          (row.due_date ? `\n📅 *Due Date:* ${row.due_date}` : '');
        notifyBotSender(row.sender_phone, 'Task Approved', msg);
      }
    } else {
      await db.query(`UPDATE tasks SET status='denied' WHERE id=?`, [row.id]);
      if (row.sender_phone) {
        const msg = `❌ *Task Not Approved*\n\nYour task was reviewed and was not approved.\n\n📋 *Task:* ${row.description}`;
        notifyBotSender(row.sender_phone, 'Task Not Approved', msg);
      }
    }

    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// 📢 DAILY REMINDER — sends list of users who didn't fill today's task
// to a WhatsApp group. Excludes CXO department.
// ══════════════════════════════════════════════════════
// Daily "report not filled" message goes to the management WhatsApp GROUP.
const REMINDER_GROUP_ID = process.env.REMINDER_GROUP_ID || '919602694444-1618492040@g.us';
// Pending Task Summary (delegation / checklist / FMS) goes to this personal WhatsApp NUMBER, not the group.
const PENDING_SUMMARY_PHONE = process.env.PENDING_SUMMARY_PHONE || '9301878061';
const EXCLUDED_DEPARTMENTS = ['CXO']; // case-insensitive match

// Reminder destination can be either a WhatsApp group ID (xxx@g.us) or a phone
// number (with/without + and 91). Routes to the right sender automatically.
async function sendToReminderDestination(text) {
  const dest = String(REMINDER_GROUP_ID || '').trim();
  if (!dest) return { ok: false, reason: 'no destination configured' };
  if (dest.includes('@g.us')) return sendWhatsAppRaw(dest, text);
  return sendWhatsApp(dest, text);
}

async function buildAndSendReminder() {
  // Sunday + holidays: don't send anything at all (not even a "holiday" group message).
  const off = await getTodayOffIST();
  if (off.off) return { ok: true, skipped: true, date: off.today, reason: off.reason };
  const today = off.today;
  const holidaysSet = off.holidaysSet;

  // Get all users — excluding CXO department + flagged users (case-insensitive).
  // role='client' users are external client logins, not team members, so they
  // never appear in the daily-report-not-filled list.
  const [users] = await db.query(
    `SELECT id, name, COALESCE(department,'') AS department,
            COALESCE(week_off,'') AS week_off, COALESCE(extra_off,'') AS extra_off,
            COALESCE(exclude_from_reminder,0) AS exclude_from_reminder
     FROM users WHERE role <> 'client' ORDER BY name ASC`
  );

  // Users on approved leave today are excluded from the "report not filed" name list.
  const onLeave = await usersOnLeaveSet(today);

  // Filter out CXO + manually-excluded users + users whose today is week-off/holiday + on-leave
  const eligible = users.filter(u =>
    !EXCLUDED_DEPARTMENTS.some(d => (u.department || '').toLowerCase() === d.toLowerCase()) &&
    !u.exclude_from_reminder &&
    !isUserOffOn(u, today, holidaysSet) &&
    !onLeave.has(u.id)
  );

  if (!eligible.length) {
    return { ok: false, reason: 'No eligible users (everyone is CXO / on leave / off)' };
  }

  // Get IDs of users who already submitted today
  const [filled] = await db.query(
    `SELECT DISTINCT user_id FROM daily_tasks WHERE entry_date = ?`,
    [today]
  );
  const filledSet = new Set(filled.map(r => r.user_id));

  // Names of users who haven't filled yet
  const missingNames = eligible
    .filter(u => !filledSet.has(u.id))
    .map(u => u.name);

  if (!missingNames.length) {
    // Everyone (eligible) has filled — send a "all done" or skip
    const allDoneMsg = `Hello,\n\nGreat news! ✅ Everyone has filled today's Daily Task report.\n\nThanks team!`;
    const sendRes = await sendToReminderDestination(allDoneMsg);
    return { ok: true, allDone: true, missingCount: 0, send: sendRes, date: today };
  }

  // Build the reminder message
  let message = "Hello,\n\n";
  message += "Today's Daily task report is not filled by :-\n\n";
  message += missingNames.join("\n");
  message += "\n\nPlease update today's report.";

  const sendRes = await sendToReminderDestination(message);
  return {
    ok: sendRes.ok,
    date: today,
    missingCount: missingNames.length,
    missingNames,
    eligibleCount: eligible.length,
    send: sendRes
  };
}

// ── Manual trigger (admin button) ────────────────────────
app.post('/api/daily-reminder/send', requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await buildAndSendReminder();
    res.json(result);
  } catch (err) {
    console.error('Manual reminder error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ══════════════════════════════════════════════════════
// 📊 PENDING TASK SUMMARY — sends THREE separate WhatsApp messages
// to the management group (Delegation / Checklist / FMS), each grouped by user.
// ══════════════════════════════════════════════════════
async function buildPendingSummaryMessages() {
  const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
  const today = istNow.toISOString().split('T')[0];
  const fmtIN = d => (d || '').split('-').reverse().join('/');

  // ── DELEGATION ─────────────────────────────────────────
  const [delRows] = await db.query(`
    SELECT t.id, t.description, t.priority,
           DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
           u.name AS doer_name, c.name AS client_name
    FROM delegation_tasks t
    JOIN users u ON t.assigned_to=u.id
    LEFT JOIN clients c ON t.client_id=c.id
    WHERE t.status='pending'
    ORDER BY u.name, t.due_date ASC`);

  // ── CHECKLIST ──────────────────────────────────────────
  const [chlRows] = await db.query(`
    SELECT t.id, t.description, t.priority,
           DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
           u.name AS doer_name, c.name AS client_name
    FROM checklist_tasks t
    JOIN users u ON t.assigned_to=u.id
    LEFT JOIN clients c ON t.client_id=c.id
    WHERE t.status='pending' AND t.due_date <= ?
    ORDER BY u.name, t.due_date ASC`, [today]);

  // ── FMS ────────────────────────────────────────────────
  // Reuse the same shape the dashboard endpoint already returns — pending rows
  // come pre-filtered by the planVal/actualVal check there.
  // We'll just call into the existing handler via a direct query against fms_sheets.
  let fmsRows = [];
  try {
    const [allSheets] = await db.query('SELECT * FROM fms_sheets');
    if (allSheets.length) {
      const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']).catch(() => null);
      if (sheetsApi) {
        for (const sheet of allSheets) {
          const [steps] = await db.query('SELECT * FROM fms_steps WHERE fms_id=? ORDER BY step_order ASC', [sheet.id]);
          if (!steps.length) continue;
          for (const step of steps) {
            const [doers] = await db.query(`SELECT u.id, u.name FROM fms_step_doers fsd JOIN users u ON fsd.user_id=u.id WHERE fsd.step_id=?`, [step.id]);
            step.doerNames = doers.map(d => d.name).join(', ');
          }
          try {
            const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
            const tabName = sheet.sheet_name || 'Sheet1';
            const headerRowIdx = (sheet.header_row || 1) - 1;
            const showColsByStep = steps.map(s => {
              try { return JSON.parse(s.show_cols || '[]').filter(n => Number.isInteger(n) && n >= 0); }
              catch { return []; }
            });
            const allCols = steps.flatMap(s => [colToIdx(s.plan_col), colToIdx(s.actual_col)])
              .concat(showColsByStep.flat()).filter(x => x >= 0);
            if (!allCols.length) continue;
            const maxCol = Math.max(...allCols);
            const range = `${tabName}!A:${idxToCol(maxCol)}`;
            const resp = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range });
            const data = resp.data.values || [];
            const headers = data[headerRowIdx] || [];
            const dataRows = data.slice(headerRowIdx + 1);
            const blankClean = v => (v || '').toString().replace(/[\s ​‌‍﻿]+/g, '');
            for (let si = 0; si < steps.length; si++) {
              const step = steps[si];
              const showCols = showColsByStep[si];
              const planIdx = colToIdx(step.plan_col);
              const actualIdx = colToIdx(step.actual_col);
              if (planIdx < 0 || actualIdx < 0) continue;
              dataRows.forEach(row => {
                const planVal = (row[planIdx] || '').toString().trim();
                const actualVal = (row[actualIdx] || '').toString().trim();
                if (!blankClean(planVal) || blankClean(actualVal)) return;
                // Pick the "Client Name" header among configured show_cols (case-insensitive).
                let clientName = '';
                for (const ci of showCols) {
                  if (/client/i.test(headers[ci] || '')) { clientName = (row[ci] || '').toString().trim(); break; }
                }
                fmsRows.push({
                  fmsName: sheet.fms_name || sheet.sheet_name,
                  stepName: step.step_name,
                  doer: step.doerNames || '—',
                  planValue: planVal,
                  clientName
                });
              });
            }
          } catch(e) { /* skip sheet on error */ }
        }
      }
    }
  } catch(e) { console.error('FMS summary build err:', e.message); }

  // Render per-type message, grouped by user.
  const groupBy = (rows, key) => rows.reduce((acc, r) => {
    const k = r[key] || '—';
    (acc[k] = acc[k] || []).push(r);
    return acc;
  }, {});

  function delegationMsg() {
    if (!delRows.length) return null;
    const grouped = groupBy(delRows, 'doer_name');
    let out = 'Hello,\n\n*All Delegation Pending Task Summary*\n';
    for (const name of Object.keys(grouped).sort()) {
      out += `\n*${name} - Delegation Pending Task Summary*\n`;
      for (const t of grouped[name]) {
        out += `\nTask ID - ${t.id}`;
        out += `\nTask - ${t.description || '—'}`;
        out += `\nTarget Date - ${t.due_date ? fmtIN(t.due_date) : 'To be set by doer'}`;
        out += `\nPriority - ${(t.priority || 'low').replace(/^./, c => c.toUpperCase())}`;
        out += `\nClient Name - ${t.client_name || '-'}\n`;
      }
    }
    return out.trim();
  }
  function checklistMsg() {
    if (!chlRows.length) return null;
    const grouped = groupBy(chlRows, 'doer_name');
    let out = 'Hello,\n\n*All Checklist Pending Task Summary*\n';
    for (const name of Object.keys(grouped).sort()) {
      out += `\n*${name} - Checklist Pending Task Summary*\n`;
      for (const t of grouped[name]) {
        out += `\nTask ID - ${t.id}`;
        out += `\nTask - ${t.description || '—'}`;
        out += `\nTarget Date - ${fmtIN(t.due_date)}`;
        out += `\nClient Name - ${t.client_name || '-'}\n`;
      }
    }
    return out.trim();
  }
  function fmsMsg() {
    if (!fmsRows.length) return null;
    // Each FMS row's "doer" can be a comma-list — split so each name gets credited.
    const expanded = [];
    for (const r of fmsRows) {
      const names = (r.doer || '').split(',').map(s => s.trim()).filter(Boolean);
      if (!names.length) names.push('—');
      for (const n of names) expanded.push({ ...r, doer: n });
    }
    const grouped = groupBy(expanded, 'doer');
    const label = 'FMS Pending Task Summary';
    let out = `Hello,\n\n*All ${label}*\n`;
    for (const name of Object.keys(grouped).sort()) {
      out += `\n*${name} - ${label}*\n`;
      for (const t of grouped[name]) {
        out += `\nClient Name - ${t.clientName || '-'}`;
        out += `\nAt Step - ${t.stepName || '—'}`;
        out += `\nPlanned - ${t.planValue || '—'}\n`;
      }
    }
    return out.trim();
  }

  return {
    delegation: delegationMsg(),
    checklist:  checklistMsg(),
    fms:        fmsMsg(),
    counts: { delegation: delRows.length, checklist: chlRows.length, fms: fmsRows.length }
  };
}

async function sendPendingSummaryMessages() {
  // Sunday + holidays: no summary messages at all.
  const offCheck = await getTodayOffIST();
  if (offCheck.off) return { ok: true, skipped: true, date: offCheck.today, reason: offCheck.reason };

  const msgs = await buildPendingSummaryMessages();
  // Pending summary goes to the configured PERSONAL WhatsApp number, NOT the
  // daily-reminder group. The group only receives the "report not filled" message.
  const groupResults = {};
  // The fixed recipient now gets the summary on BOTH WhatsApp and email. Its
  // email is PENDING_SUMMARY_EMAIL if set, else resolved from the phone number.
  const summaryEmail = process.env.PENDING_SUMMARY_EMAIL || await emailForPhone(PENDING_SUMMARY_PHONE);
  const summaryTypeLabel = { delegation: 'Delegation', checklist: 'Checklist', fms: 'FMS' };
  for (const type of ['delegation','checklist','fms']) {
    if (!msgs[type]) { groupResults[type] = { skipped: 'no pending tasks' }; continue; }
    const r = await sendWhatsApp(PENDING_SUMMARY_PHONE, msgs[type]);
    groupResults[type] = r;
    if (summaryEmail) await sendMail(summaryEmail, `${summaryTypeLabel[type]} Pending Task Summary`, waTextToEmailHtml(msgs[type])).catch(e => console.error('pending summary email err:', e.message));
    await new Promise(r => setTimeout(r, 1500)); // small spacing so messages stay readable
  }
  // Also EMAIL each user who has the "pending_summary_recipient" access ticked
  // (was a personal WhatsApp DM). The fixed PENDING_SUMMARY_PHONE above stays on
  // WhatsApp; only these opted-in recipients moved to email.
  const [recipients] = await db.query(
    `SELECT id, name, email, notification_email, extra_access FROM users WHERE extra_access IS NOT NULL`
  );
  const targets = recipients.filter(u => parseExtraAccess(u.extra_access).includes('pending_summary_recipient'));
  const typeLabel = { delegation: 'Delegation', checklist: 'Checklist', fms: 'FMS' };
  const dmResults = await Promise.all(targets.map(async u => {
    const email = u.notification_email || u.email;
    const perType = {};
    for (const type of ['delegation','checklist','fms']) {
      if (!msgs[type]) { perType[type] = { skipped: 'no pending tasks' }; continue; }
      if (!email) { perType[type] = { skipped: 'no email' }; continue; }
      perType[type] = await sendMail(email, `${typeLabel[type]} Pending Task Summary`, waTextToEmailHtml(msgs[type]));
    }
    return { userId: u.id, name: u.name, email, perType };
  }));
  return { ok: true, counts: msgs.counts, group: groupResults, dms: dmResults };
}

app.get('/api/pending-summary/preview', requireAuth, requireAdmin, async (_req, res) => {
  try {
    const msgs = await buildPendingSummaryMessages();
    res.json(msgs);
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

app.post('/api/pending-summary/send', requireAuth, requireAdmin, async (_req, res) => {
  try {
    const out = await sendPendingSummaryMessages();
    res.json(out);
  } catch (err) {
    console.error('Pending summary send error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Checks the WhatsApp-bot task intake queue (`tasks`) for rows not yet
// flagged purvi_notified, messages Purvi Saini for each, then marks them
// notified. Marks BEFORE sending so a crash mid-loop can't double-send
// on the next run.
async function notifyPurviOfNewMdoTasks() {
  const [purvi] = await usersForSetting('mdo_reviewer_ids', 'id, name, email, notification_email');
  const purviEmail = purvi && (purvi.notification_email || purvi.email);
  if (!purviEmail) return { ok: false, reason: 'no MDO reviewer with an email on file — check app_settings.mdo_reviewer_ids' };

  const [rows] = await db.query(`SELECT * FROM tasks WHERE purvi_notified = 0 OR purvi_notified IS NULL`);
  let sent = 0;
  for (const task of rows) {
    await db.query('UPDATE tasks SET purvi_notified = 1 WHERE id = ?', [task.id]);
    const dueDate = task.target_date || task.due_date;
    const waMsg =
      `🔔 *New Task Delegated via WhatsApp — Needs Your Approval*\n\n` +
      `📋 *Task:* ${task.task_description || task.description || '—'}\n` +
      `🆔 *Task ID:* ${task.task_id || '—'}\n` +
      `🙋 *Assigned By:* ${task.assigned_by || task.assigned_name || '—'}\n` +
      `👤 *Assigned To:* ${task.assigned_to || '—'}\n` +
      `⚡ *Priority:* ${task.priority || '—'}\n` +
      `📅 *Due Date:* ${dueDate ? new Date(dueDate).toLocaleDateString('en-IN') : '—'}\n` +
      `🏢 *Client:* ${task.client_name || '—'}\n\n` +
      `Please review and approve/reject this task in the MDO Approvals dashboard.`;
    await sendMail(purviEmail, 'New WhatsApp Task — Needs Your Approval', waTextToEmailHtml(waMsg)).catch(e => console.error('MDO new-task email err:', e.message));
    sent++;
  }
  return { ok: true, sent };
}

// Cron endpoint — checks for newly delegated WhatsApp-bot tasks and notifies
// Purvi Saini. Wire this up to an external pinger (Vercel Cron is daily-only
// on the Hobby plan; use a frequent external pinger like GitHub Actions for
// near-real-time checks, same as /api/cron/meeting-reminder).
// Protected by CRON_SECRET (Authorization: Bearer ...).
app.get('/api/cron/mdo-new-task-notify', async (req, res) => {
  const authHeader = req.headers.authorization || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    console.log('  ⏰ Cron triggered: mdo-new-task-notify');
    const out = await notifyPurviOfNewMdoTasks();
    res.json(out);
  } catch (err) {
    console.error('Cron mdo-new-task-notify error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Local dev only — Vercel serverless functions don't stay warm between
// requests, so setInterval has no effect there; production relies on the
// cron endpoint above being pinged externally instead.
if (!process.env.VERCEL && !process.env.NOW_REGION) {
  setInterval(() => {
    notifyPurviOfNewMdoTasks().catch(e => console.error('mdo-new-task-notify poll err:', e.message));
  }, 30000);
}

// Cron endpoint — called by Vercel Cron at 10 AM IST (04:30 UTC) and 4 PM IST (10:30 UTC).
// Protected by CRON_SECRET (Authorization: Bearer ...).
app.get('/api/cron/pending-summary', async (req, res) => {
  const authHeader = req.headers.authorization || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    console.log('  ⏰ Cron triggered: pending-summary');
    const out = await sendPendingSummaryMessages();
    res.json(out);
  } catch (err) {
    console.error('Cron pending-summary error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── Preview (admin) — see who would get reminded without actually sending ──
app.get('/api/daily-reminder/preview', requireAuth, requireAdmin, async (req, res) => {
  try {
    const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
    const today = istNow.toISOString().split('T')[0];

    // Client logins (role='client') are external accounts — they never fill
    // daily team reports and shouldn't show up in this preview.
    const [users] = await db.query(
      `SELECT id, name, email, COALESCE(department,'') AS department,
              COALESCE(exclude_from_reminder,0) AS exclude_from_reminder
       FROM users WHERE role <> 'client' ORDER BY name ASC`
    );
    const isCxo = u => EXCLUDED_DEPARTMENTS.some(d => (u.department || '').toLowerCase() === d.toLowerCase());
    const eligible = users.filter(u => !isCxo(u) && !u.exclude_from_reminder);
    const [filled] = await db.query(
      `SELECT DISTINCT user_id FROM daily_tasks WHERE entry_date = ?`, [today]
    );
    const filledSet = new Set(filled.map(r => r.user_id));
    const missing = eligible.filter(u => !filledSet.has(u.id));
    const filledList = eligible.filter(u => filledSet.has(u.id));
    // Excluded list — combine CXO + flagged users (deduplicated by id)
    const excludedList = users.filter(u => isCxo(u) || u.exclude_from_reminder)
      .map(u => ({
        ...u,
        reason: isCxo(u) && u.exclude_from_reminder ? 'CXO + Flagged'
              : isCxo(u) ? 'CXO Department'
              : 'Manually Excluded'
      }));

    res.json({
      date: today,
      group_id: REMINDER_GROUP_ID,
      missing_count: missing.length,
      missing,
      filled_count: filledList.length,
      filled: filledList,
      excluded_count: excludedList.length,
      excluded: excludedList
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════
// 📋 PENDING TASKS REMINDER — 12 PM daily (per-user consolidated WhatsApp)
// Sends each user a single WhatsApp listing ALL their pending tasks
// (delegation + checklist + FMS) due today or earlier.
// ══════════════════════════════════════════════════════

// FMS pending rows (plan filled, actual blank) grouped by doer user-id.
// Same row logic as the management Pending Task Summary, but attributed to each
// step doer's id so individual users (e.g. FMS-only doers) get their own DM.
// Each sheet is fetched once. Rows with a plan-date after `today` are skipped to
// match the "due today or earlier" intent; rows with an unparseable plan-date are
// kept (we can't tell, so we remind).
async function buildFmsPendingByUser(today) {
  const byUser = {};
  try {
    const [allSheets] = await db.query('SELECT * FROM fms_sheets');
    if (!allSheets.length) return byUser;
    const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']).catch(() => null);
    if (!sheetsApi) return byUser;
    for (const sheet of allSheets) {
      const [steps] = await db.query('SELECT * FROM fms_steps WHERE fms_id=? ORDER BY step_order ASC', [sheet.id]);
      if (!steps.length) continue;
      for (const step of steps) {
        const [doers] = await db.query(`SELECT u.id, u.name FROM fms_step_doers fsd JOIN users u ON fsd.user_id=u.id WHERE fsd.step_id=?`, [step.id]);
        step.doers = doers;
      }
      try {
        const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
        const tabName = sheet.sheet_name || 'Sheet1';
        const headerRowIdx = (sheet.header_row || 1) - 1;
        const showColsByStep = steps.map(s => {
          try { return JSON.parse(s.show_cols || '[]').filter(n => Number.isInteger(n) && n >= 0); }
          catch { return []; }
        });
        const allCols = steps.flatMap(s => [colToIdx(s.plan_col), colToIdx(s.actual_col)])
          .concat(showColsByStep.flat()).filter(x => x >= 0);
        if (!allCols.length) continue;
        const maxCol = Math.max(...allCols);
        const resp = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range: `${tabName}!A:${idxToCol(maxCol)}` });
        const data = resp.data.values || [];
        const headers = data[headerRowIdx] || [];
        const dataRows = data.slice(headerRowIdx + 1);
        const blankClean = v => (v || '').toString().replace(/[\s ​‌‍﻿]+/g, '');
        for (let si = 0; si < steps.length; si++) {
          const step = steps[si];
          if (!step.doers || !step.doers.length) continue;
          const showCols = showColsByStep[si];
          const planIdx = colToIdx(step.plan_col);
          const actualIdx = colToIdx(step.actual_col);
          if (planIdx < 0 || actualIdx < 0) continue;
          dataRows.forEach(row => {
            const planVal = (row[planIdx] || '').toString().trim();
            const actualVal = (row[actualIdx] || '').toString().trim();
            if (!blankClean(planVal) || blankClean(actualVal)) return;
            const planDate = parseFmsPlanDate(planVal);
            if (planDate && planDate > today) return; // future-dated → not due yet
            let clientName = '';
            for (const ci of showCols) {
              if (/client/i.test(headers[ci] || '')) { clientName = (row[ci] || '').toString().trim(); break; }
            }
            const entry = {
              type: 'FMS',
              fmsName: sheet.fms_name || sheet.sheet_name,
              stepName: step.step_name,
              clientName,
              planValue: planVal,
              planDate: planDate || '',
              due_date: planDate || ''
            };
            for (const d of step.doers) (byUser[d.id] = byUser[d.id] || []).push({ ...entry });
          });
        }
      } catch (e) { /* skip this sheet on error */ }
    }
  } catch (e) { console.error('FMS per-user pending build err:', e.message); }
  return byUser;
}

async function buildAndSendPendingTasksReminder() {
  // Sunday + holiday guard — no DMs to anyone on those days.
  const offCheck = await getTodayOffIST();
  if (offCheck.off) return { ok: true, sent: 0, skipped: 0, total: 0, reason: offCheck.reason };
  const today = offCheck.today;

  // Fetch pending tasks (due today or earlier) — delegation + checklist
  const [delRows] = await db.query(`
    SELECT t.id, t.description, t.assigned_to, t.priority,
           DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
           u2.name AS assigned_by_name
    FROM delegation_tasks t
    JOIN users u2 ON t.assigned_by = u2.id
    WHERE t.status='pending' AND t.due_date <= ?`, [today]);

  const [chlRows] = await db.query(`
    SELECT t.id, t.description, t.assigned_to, t.priority,
           DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
           u2.name AS assigned_by_name
    FROM checklist_tasks t
    JOIN users u2 ON t.assigned_by = u2.id
    WHERE t.status='pending' AND t.due_date <= ?`, [today]);

  // Group by assigned_to
  const byUser = {};
  for (const r of delRows) {
    if (!byUser[r.assigned_to]) byUser[r.assigned_to] = [];
    byUser[r.assigned_to].push({ ...r, type: 'Delegation' });
  }
  for (const r of chlRows) {
    if (!byUser[r.assigned_to]) byUser[r.assigned_to] = [];
    byUser[r.assigned_to].push({ ...r, type: 'Checklist' });
  }

  // FMS pending rows (plan filled, actual blank) — same per-user DM as del/chl.
  // Pulls in FMS-only doers (e.g. Purvi) who'd otherwise never get a reminder.
  try {
    const fmsByUser = await buildFmsPendingByUser(today);
    for (const uid of Object.keys(fmsByUser)) {
      if (!byUser[uid]) byUser[uid] = [];
      byUser[uid].push(...fmsByUser[uid]);
    }
  } catch (e) { console.error('FMS reminder merge err:', e.message); }

  const userIds = Object.keys(byUser).map(Number);
  if (!userIds.length) {
    return { ok: true, sent: 0, skipped: 0, total: 0, reason: 'No pending tasks for anyone' };
  }

  // Fetch user details (name + phone + off info)
  const [users] = await db.query(
    `SELECT id, name, phone, email, notification_email, COALESCE(department,'') AS department,
            COALESCE(week_off,'') AS week_off, COALESCE(extra_off,'') AS extra_off,
            COALESCE(exclude_from_reminder,0) AS exclude_from_reminder
       FROM users WHERE id IN (${userIds.map(()=>'?').join(',')})`, userIds);
  const userMap = {};
  users.forEach(u => userMap[u.id] = u);

  // Load holidays for off-day check
  const holidaysSet = offCheck.holidaysSet || await loadHolidaysSet();
  // Holiday/Sunday already guarded above. Per-user leave exclusion below.
  const onLeave = await usersOnLeaveSet(today);

  let sent = 0, skipped = 0;
  const skippedDetails = [];

  for (const uid of userIds) {
    const u = userMap[uid];
    if (!u) { skipped++; skippedDetails.push({ id: uid, reason: 'user not found' }); continue; }
    if (u.exclude_from_reminder) { skipped++; skippedDetails.push({ name: u.name, reason: 'manually excluded' }); continue; }
    const uEmail = u.notification_email || u.email;
    if (!uEmail) { skipped++; skippedDetails.push({ name: u.name, reason: 'no email' }); continue; }
    if (onLeave.has(uid)) { skipped++; skippedDetails.push({ name: u.name, reason: 'on approved leave' }); continue; }
    // Skip CXO department
    if (EXCLUDED_DEPARTMENTS.some(d => (u.department || '').toLowerCase() === d.toLowerCase())) {
      skipped++; skippedDetails.push({ name: u.name, reason: 'CXO' }); continue;
    }
    // Skip if today is user's week-off
    if (isUserOffOn(u, today, holidaysSet)) {
      skipped++; skippedDetails.push({ name: u.name, reason: 'week-off' }); continue;
    }

    // Sort tasks: oldest due date first
    const tasks = byUser[uid].sort((a, b) => (a.due_date || '').localeCompare(b.due_date || ''));
    const lines = tasks.map((t, i) => {
      if (t.type === 'FMS') {
        const when = t.planDate ? t.planDate.split('-').reverse().join('-') : (t.planValue || '');
        const overdue = t.planDate && t.planDate < today ? ' ⚠️ overdue' : '';
        const client = t.clientName ? ` (${t.clientName})` : '';
        return `${i+1}. ${t.fmsName} — ${t.stepName}${client}\n   📅 ${when}${overdue} · FMS`;
      }
      const dueFmt = (t.due_date || '').split('-').reverse().join('-');
      const overdue = t.due_date && t.due_date < today ? ' ⚠️ overdue' : '';
      return `${i+1}. ${t.description}\n   📅 ${dueFmt}${overdue} · ${t.type}${t.priority && t.priority !== 'low' ? ' · ' + t.priority.toUpperCase() : ''}`;
    }).join('\n\n');

    const taskWord = tasks.length === 1 ? 'task' : 'tasks';
    const msg = `Hello ${u.name || ''},\n\n📋 *You have ${tasks.length} pending ${taskWord}*\n\n${lines}\n\nPlease update status by EOD.\n\n— E-Marketing Task Manager`;

    const ok = await sendMail(uEmail, `You have ${tasks.length} pending ${taskWord}`, waTextToEmailHtml(msg));
    if (ok) sent++; else { skipped++; skippedDetails.push({ name: u.name, reason: 'email send failed' }); }
  }

  return { ok: true, date: today, total: userIds.length, sent, skipped, skippedDetails };
}

// Manual trigger (admin)
app.post('/api/pending-reminder/send', requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await buildAndSendPendingTasksReminder();
    res.json(result);
  } catch (err) {
    console.error('Pending reminder error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Cron endpoint — called at 12 PM IST = 6:30 AM UTC
app.get('/api/cron/pending-reminder', async (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    console.log('  ⏰ Cron triggered: pending-reminder (12 PM IST)');
    const result = await buildAndSendPendingTasksReminder();
    res.json(result);
  } catch (err) {
    console.error('Cron pending-reminder error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Cron endpoint (called by Vercel Cron at 7:30 PM IST = 2:00 PM UTC) ──
// Protected by CRON_SECRET so random visitors can't trigger it.
app.get('/api/cron/daily-reminder', async (req, res) => {
  // Vercel Cron sends header: authorization: Bearer <CRON_SECRET>
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    console.log('  ⏰ Cron triggered: daily-reminder');
    const result = await buildAndSendReminder();
    res.json(result);
  } catch (err) {
    console.error('Cron error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── 10-min pre-meeting reminder. Hit by an external cron (GitHub Actions, 5-min
// schedule) since the Vercel Hobby plan only allows daily crons. ──
async function sendMeetingReminders() {
  // External cron fires every 5 min, so the window is widened (6-14 min) to
  // guarantee each meeting is caught at least once. reminder_sent flag stops
  // duplicate sends within the window.
  const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
  const today = istNow.toISOString().split('T')[0];
  // Sunday + last-Saturday-of-month are off days — no meeting reminders.
  if (istNow.getUTCDay() === 0 || isLastSaturdayOfMonth(today)) return { ok: true, skipped: 'off day' };
  const totalNow = istNow.getUTCHours() * 60 + istNow.getUTCMinutes();
  const minLow  = totalNow + 6, minHigh = totalNow + 14;
  if (minHigh > 24 * 60) return { ok: true, skipped: 'late-night window' };
  const lowH = String(Math.floor(minLow / 60)).padStart(2,'0') + ':' + String(minLow % 60).padStart(2,'0') + ':00';
  const highH = String(Math.floor(minHigh / 60)).padStart(2,'0') + ':' + String(minHigh % 60).padStart(2,'0') + ':00';
  const [due] = await db.query(
    `SELECT id FROM meetings
     WHERE status='scheduled' AND reminder_sent=0
       AND meeting_date=? AND start_time BETWEEN ? AND ?`,
    [today, lowH, highH]);
  if (!due.length) return { ok: true, fired: 0 };
  let fired = 0;
  for (const m of due) {
    // Mark first so concurrent crons don't double-send, then notify.
    await db.query('UPDATE meetings SET reminder_sent=1 WHERE id=? AND reminder_sent=0', [m.id]);
    await sendMeetingNotification(m.id, 'reminder').catch(e => console.error('meet reminder err:', e.message));
    fired++;
  }
  return { ok: true, fired, window: `${lowH}-${highH} IST` };
}

app.get('/api/cron/meeting-reminder', async (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    const r = await sendMeetingReminders();
    res.json(r);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Due-date nudge ───────────────────────────────────────────────────
// A client-delegated task lands on the handler with no due date (the client
// never sets one), so until the handler fills it in the client sees no
// deadline at all. This nudges every handler who still owes one.
//
// Batched to ONE message per handler, however many tasks they owe — a handler
// with five of them should get one nudge, not five. The 4-hourly cadence comes
// from whatever pings the endpoint below; this function only answers "who is
// owed a nudge right now", so it is safe to call more often (it will just send
// again, which is the intended behaviour until the date is set).
//
// Deliberately runs around the clock, on the user's instruction: the nudge is
// meant to keep arriving until the handler fills the date in, night included.
async function remindHandlersOfMissingDueDates({ force = false } = {}) {
  // Office hours only — 9:30 AM to 6:00 PM IST. The 4-hourly cron still fires
  // round the clock, but the nudge is held outside these hours so handlers are
  // not chased at night. With the current schedule that lands the reminder at
  // ~9:30, 13:30 and 17:30 IST. ?force=1 bypasses this for manual testing.
  const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
  const mins = istNow.getUTCHours() * 60 + istNow.getUTCMinutes();
  if (!force && (mins < 9 * 60 + 30 || mins >= 18 * 60)) {
    return { ok: true, skipped: 'outside office hours (09:30-18:00 IST)', sent: 0 };
  }
  // Only tasks a CLIENT delegated. A client login is role='client'; the
  // client_id back-link is checked too, matching how assignerIsClient is
  // decided in PUT /api/tasks/:id/status.
  const [rows] = await db.query(
    `SELECT t.id, t.description, t.assigned_to,
            TIMESTAMPDIFF(HOUR, t.created_at, NOW()) AS waiting_hrs,
            c.name AS client_name,
            d.name AS handler_name, d.phone AS handler_phone
       FROM delegation_tasks t
       JOIN users a ON t.assigned_by = a.id
       JOIN users d ON t.assigned_to = d.id
       LEFT JOIN clients c ON t.client_id = c.id
      WHERE t.awaiting_due_date = 1
        -- The flag alone is not proof the date is missing. Edit used to write a
        -- date without clearing it, so rows exist right now that carry a real
        -- deadline and a stale 1, and they have been nudged every four hours
        -- ever since. Asking the question the message actually asks — is there
        -- a date? — stops those immediately, with no data migration, and keeps
        -- the cron honest if anything else ever forgets the flag again.
        AND t.due_date IS NULL
        AND t.status <> 'completed'
        AND (a.role = 'client' OR a.client_id IS NOT NULL)
      ORDER BY t.assigned_to ASC, t.created_at ASC`);

  const byHandler = new Map();
  for (const r of rows) {
    if (!byHandler.has(r.assigned_to)) byHandler.set(r.assigned_to, []);
    byHandler.get(r.assigned_to).push(r);
  }

  let sent = 0, noEmail = 0;
  for (const tasks of byHandler.values()) {
    const h = tasks[0];
    // Now emails the handler (was WhatsApp) — resolved by their user id.
    const ok = await emailUserWaText(h.assigned_to, 'Due date pending on client tasks', dueDateNudgeMessage(h.handler_name, tasks))
      .catch(e => { console.error('due-date nudge email err:', e.message); return false; });
    if (ok) sent++; else noEmail++;
  }
  return { ok: true, sent, handlers: byHandler.size, tasks: rows.length, noEmail };
}

// Kept separate so the wording can be reviewed without reading the query.
function dueDateNudgeMessage(handlerName, tasks) {
  const n = tasks.length;
  const list = tasks.map((t, i) => {
    const hrs = Number(t.waiting_hrs) || 0;
    const waited = hrs < 1 ? 'waiting under an hour' : `waiting ${hrs} hr${hrs === 1 ? '' : 's'}`;
    return `${i + 1}. *${t.client_name || 'Client'}* — ${t.description}\n   _${waited}_`;
  }).join('\n');
  return `Hello ${handlerName || ''},\n\n` +
    `🗓 *Due Date Pending*\n\n` +
    `*${n} client task${n === 1 ? '' : 's'}* ${n === 1 ? 'is' : 'are'} waiting for you to set a due date:\n\n` +
    `${list}\n\n` +
    `The client cannot see any deadline until you set one.\n\n` +
    `— E-Marketing Task Manager`;
}

// ── Client pending-task digest ───────────────────────────────────────
// Sends each client's own WhatsApp group a list of the tasks they delegated
// that are still open — old ones included, oldest first, so a task sitting
// since the 13th is visible next to today's.
//
// One message PER HANDLER, not per client: a client with three handlers gets
// three messages, each addressed to that handler and listing only their tasks.
//
// A client with no whatsapp_group_id is skipped entirely. There is a shared
// "all clients" group in this app (MEETING_CLIENT_GROUP_ID) and it must never
// be used as a fallback here — every other client can read it.
//
// Monday-Friday only, and nothing at all goes out when nothing is pending —
// both on the user's instruction. The day check lives here rather than only in
// the schedule so any trigger respects it; pass ?force=1 to test on a weekend.
async function sendClientPendingDigests({ force = false } = {}) {
  const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
  const day = istNow.getUTCDay();  // 0 Sun … 6 Sat
  if (!force && (day === 0 || day === 6)) {
    return { ok: true, skipped: 'weekend', sent: 0 };
  }
  const [rows] = await db.query(
    `SELECT t.id, t.description, t.assigned_to,
            DATE_FORMAT(t.created_at,'%d-%m-%Y') AS given_on,
            DATEDIFF(CURDATE(), DATE(t.created_at)) AS days_pending,
            c.id AS client_id, c.name AS client_name, c.whatsapp_group_id AS group_id,
            d.name AS handler_name
       FROM delegation_tasks t
       JOIN users a   ON t.assigned_by = a.id
       JOIN users d   ON t.assigned_to = d.id
       JOIN clients c ON t.client_id   = c.id
      WHERE t.status IN ('pending','revised')
        AND (a.role = 'client' OR a.client_id IS NOT NULL)
        AND c.whatsapp_group_id IS NOT NULL AND c.whatsapp_group_id <> ''
      ORDER BY c.name ASC, d.name ASC, t.created_at ASC`);

  // The other direction: what the CLIENT still owes us. Only rows whose
  // deadline has actually passed — a task due tomorrow is not "not done".
  // due_time is honoured when set, so a 2:30 PM deadline counts from 2:30 PM.
  const [owed] = await db.query(
    `SELECT t.id, t.description, t.remarks, t.assigned_by AS handler_id,
            DATE_FORMAT(t.due_date,'%d-%m-%Y') AS due_on,
            TIME_FORMAT(t.due_time,'%H:%i') AS due_time,
            DATEDIFF(CURDATE(), t.due_date) AS days_over,
            c.id AS client_id, c.name AS client_name, c.whatsapp_group_id AS group_id,
            h.name AS handler_name
       FROM delegation_tasks t
       JOIN users d   ON t.assigned_to = d.id
       JOIN users h   ON t.assigned_by = h.id
       JOIN clients c ON t.client_id   = c.id
      WHERE t.status IN ('pending','revised')
        AND d.role = 'client'
        AND t.due_date IS NOT NULL
        AND (t.due_date < CURDATE()
             OR (t.due_date = CURDATE() AND t.due_time IS NOT NULL AND t.due_time < CURTIME()))
        AND c.whatsapp_group_id IS NOT NULL AND c.whatsapp_group_id <> ''
      ORDER BY c.name ASC, h.name ASC, t.due_date ASC`);

  // What handlers have recorded as still needed FROM the client on their own
  // open tasks. Listed as soon as it is written, not only once late — it is a
  // request, not a complaint; the overdue marker gets added when the moment passes.
  const [asks] = await db.query(
    `SELECT t.id, t.client_ask, t.description AS parent_desc, t.assigned_to AS handler_id,
            DATE_FORMAT(t.client_ask_by,'%d-%m-%Y') AS by_date,
            TIME_FORMAT(t.client_ask_by,'%H:%i') AS by_time,
            (t.client_ask_by < NOW()) AS is_late,
            c.id AS client_id, c.name AS client_name, c.whatsapp_group_id AS group_id,
            d.name AS handler_name
       FROM delegation_tasks t
       JOIN users d   ON t.assigned_to = d.id
       JOIN clients c ON t.client_id   = c.id
      WHERE t.status <> 'completed'
        AND t.client_ask IS NOT NULL AND t.client_ask <> ''
        AND c.whatsapp_group_id IS NOT NULL AND c.whatsapp_group_id <> ''
      ORDER BY c.name ASC, d.name ASC, t.client_ask_by ASC`);

  // Sub-tasks the handler has flagged. These carry no date of their own — the
  // remark IS the signal, so any sub-task with one and still open is listed.
  const [stuckSubs] = await db.query(
    `SELECT s.id, s.description, s.remarks, t.description AS parent_desc,
            t.assigned_to AS handler_id,
            c.id AS client_id, c.name AS client_name, c.whatsapp_group_id AS group_id,
            d.name AS handler_name
       FROM task_subtasks s
       JOIN delegation_tasks t ON s.task_id     = t.id
       JOIN users d            ON t.assigned_to = d.id
       JOIN clients c          ON t.client_id   = c.id
      WHERE s.status <> 'completed'
        AND s.remarks IS NOT NULL AND s.remarks <> ''
        AND c.whatsapp_group_id IS NOT NULL AND c.whatsapp_group_id <> ''
      ORDER BY c.name ASC, d.name ASC, s.created_at ASC`);

  // Bucket by client+handler — that pair is one message.
  const buckets = new Map();
  const bucketOf = (clientId, handlerId, seed) => {
    const key = `${clientId}|${handlerId}`;
    if (!buckets.has(key)) buckets.set(key, { head: seed, tasks: [], owed: [], asks: [], subs: [] });
    return buckets.get(key);
  };
  for (const r of rows)       bucketOf(r.client_id, r.assigned_to, r).tasks.push(r);
  // Owed tasks hang off the handler who asked for them, not the client doing them.
  for (const r of owed)       bucketOf(r.client_id, r.handler_id, r).owed.push(r);
  for (const r of asks)       bucketOf(r.client_id, r.handler_id, r).asks.push(r);
  for (const r of stuckSubs)  bucketOf(r.client_id, r.handler_id, r).subs.push(r);

  // Nothing pending anywhere → stay silent, rather than sending "0 tasks".
  if (!buckets.size) return { ok: true, sent: 0, tasks: 0, quiet: 'nothing pending' };

  let sent = 0;
  for (const b of buckets.values()) {
    await sendWhatsAppRaw(b.head.group_id, clientPendingDigestMessage(b))
      .catch(e => console.error('WA client digest err:', e.message));
    sent++;
  }
  return { ok: true, sent, pairs: buckets.size, tasks: rows.length, owed: owed.length,
           asks: asks.length, flaggedSubtasks: stuckSubs.length };
}

// Kept separate so the wording can be reviewed without reading the query.
function clientPendingDigestMessage({ head, tasks, owed, asks, subs }) {
  // Second half of the message: what the CLIENT owes us. Overdue tasks carry
  // the handler's remark when one was written; flagged sub-tasks are listed by
  // their remark, since they have no deadline of their own.
  let tail = '';
  if (owed.length || asks.length || subs.length) {
    const lines = [];
    owed.forEach((t, i) => {
      const d = Number(t.days_over) || 0;
      const late = d <= 0 ? 'due today' : `${d} day${d === 1 ? '' : 's'} overdue`;
      const due = `due ${t.due_on}${t.due_time ? ` · ${fmtClock(t.due_time)}` : ''} IST · ${late}`;
      lines.push(`${i + 1}. ${t.description}\n   _${due}_` + (t.remarks ? `\n   _"${t.remarks}"_` : ''));
    });
    asks.forEach(a => {
      lines.push(`• "${a.client_ask}"\n   _for "${a.parent_desc}"_\n   ` +
        `_needed by ${a.by_date} · ${fmtClock(a.by_time)} IST${Number(a.is_late) ? ' · overdue' : ''}_`);
    });
    subs.forEach(s => {
      lines.push(`• ${s.description}\n   _under "${s.parent_desc}"_\n   _"${s.remarks}"_`);
    });
    tail = `\n⚠️ *Task not done by client*\n\n${lines.join('\n')}\n`;
  }

  const n = tasks.length;
  // A client can owe us things while owing no open asks of their own, in which
  // case the message is only the warning half.
  if (!n) return `Hello ${head.handler_name || ''},\n${tail}\n— E-Marketing Task Manager`;

  const list = tasks.map((t, i) => {
    const d = Number(t.days_pending) || 0;
    const age = d === 0 ? 'today' : `${d} day${d === 1 ? '' : 's'} pending`;
    // No handler name per line — the whole message is addressed to one handler,
    // so repeating it on every task just adds noise.
    return `${i + 1}. ${t.description}\n   _given ${t.given_on} · ${age}_`;
  }).join('\n');
  // No client name in the heading: this lands in that client's own group, so
  // telling them who they are is noise. It would only earn its place if the
  // message went to the handler personally, where the client is the context.
  return `Hello ${head.handler_name || ''},\n\n` +
    `📋 *Pending Tasks*\n\n` +
    `*${n} task${n === 1 ? '' : 's'}* ${n === 1 ? 'is' : 'are'} still open with us:\n\n` +
    `${list}\n` +
    tail +
    `\n— E-Marketing Task Manager`;
}

// "14:30" → "02:30 PM". Input is always the server's TIME_FORMAT '%H:%i'.
function fmtClock(hhmm) {
  const [h, m] = String(hhmm).split(':').map(Number);
  if (!Number.isFinite(h) || !Number.isFinite(m)) return hhmm;
  const period = h < 12 ? 'AM' : 'PM';
  const h12 = h % 12 === 0 ? 12 : h % 12;
  return `${String(h12).padStart(2, '0')}:${String(m).padStart(2, '0')} ${period}`;
}

// ── Client "completed today" digest ──────────────────────────────────
// The other side of the pending digest: at the end of the day, tell each
// client's group what actually got finished for them, credited to the handler
// who did it. Silent on days nothing was finished, so it never becomes noise —
// which is also why it runs every day, weekends included.
//
// Sub-tasks are reported ONLY when their parent task is still open. Completing
// a task already requires every sub-task to be done (see the guard in
// PUT /api/tasks/:id/status), so a closed task's sub-tasks are implied — listing
// both would report the same work twice. A sub-task under an open task is the
// case worth telling the client about: the job is not finished, but this piece is.
async function sendClientCompletedDigests() {
  const scope = `AND (a.role = 'client' OR a.client_id IS NOT NULL)
                 AND c.whatsapp_group_id IS NOT NULL AND c.whatsapp_group_id <> ''`;

  const [tasks] = await db.query(
    `SELECT t.id, t.description, t.assigned_to,
            DATE_FORMAT(t.created_at,'%d-%m-%Y') AS given_on,
            DATEDIFF(DATE(t.completed_at), DATE(t.created_at)) AS took_days,
            c.id AS client_id, c.name AS client_name, c.whatsapp_group_id AS group_id,
            d.name AS handler_name
       FROM delegation_tasks t
       JOIN users a   ON t.assigned_by = a.id
       JOIN users d   ON t.assigned_to = d.id
       JOIN clients c ON t.client_id   = c.id
      WHERE t.status = 'completed' AND DATE(t.completed_at) = CURDATE() ${scope}
      ORDER BY c.name ASC, d.name ASC, t.completed_at ASC`);

  const [subs] = await db.query(
    `SELECT s.id, s.description, t.description AS parent_desc, t.assigned_to,
            c.id AS client_id, c.name AS client_name, c.whatsapp_group_id AS group_id,
            d.name AS handler_name
       FROM task_subtasks s
       JOIN delegation_tasks t ON s.task_id     = t.id
       JOIN users a            ON t.assigned_by = a.id
       JOIN users d            ON t.assigned_to = d.id
       JOIN clients c          ON t.client_id   = c.id
      WHERE s.status = 'completed' AND DATE(s.completed_at) = CURDATE()
        AND t.status <> 'completed' ${scope}
      ORDER BY c.name ASC, d.name ASC, s.completed_at ASC`);

  // Bucket both lists by client+handler — that pair is one message.
  const buckets = new Map();
  const bucket = (r) => {
    const key = `${r.client_id}|${r.assigned_to}`;
    if (!buckets.has(key)) buckets.set(key, { head: r, tasks: [], subs: [] });
    return buckets.get(key);
  };
  for (const t of tasks) bucket(t).tasks.push(t);
  for (const s of subs)  bucket(s).subs.push(s);

  if (!buckets.size) return { ok: true, sent: 0, tasks: 0, subtasks: 0, quiet: 'nothing completed today' };

  let sent = 0;
  for (const b of buckets.values()) {
    await sendWhatsAppRaw(b.head.group_id, clientCompletedDigestMessage(b))
      .catch(e => console.error('WA completed digest err:', e.message));
    sent++;
  }
  return { ok: true, sent, pairs: buckets.size, tasks: tasks.length, subtasks: subs.length };
}

// Kept separate so the wording can be reviewed without reading the queries.
function clientCompletedDigestMessage({ head, tasks, subs }) {
  // No client name anywhere: this lands in that client's own group.
  let msg = `✅ *Completed Today*\n\n`;
  if (tasks.length) {
    const list = tasks.map((t, i) => {
      const d = Number(t.took_days) || 0;
      const took = d === 0 ? 'same day' : `took ${d} day${d === 1 ? '' : 's'}`;
      return `${i + 1}. ${t.description}\n   _given ${t.given_on} · ${took}_`;
    }).join('\n');
    msg += `*${tasks.length} task${tasks.length === 1 ? '' : 's'}* done by ${head.handler_name}:\n\n${list}\n\n`;
  }
  if (subs.length) {
    const list = subs.map(s => `• ${s.description}\n   _under "${s.parent_desc}"_`).join('\n');
    // When nothing else was closed, this section carries the handler's name.
    msg += tasks.length
      ? `🧩 *Also progressed:*\n\n${list}\n\n`
      : `🧩 *Progressed by ${head.handler_name}:*\n\n${list}\n\n`;
  }
  return msg + `— E-Marketing Task Manager`;
}

app.get('/api/cron/client-completed-digest', async (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    console.log('  ⏰ Cron triggered: client-completed-digest');
    res.json(await sendClientCompletedDigests());
  } catch (err) {
    console.error('Cron client-completed-digest error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── Handler-on-leave notice ──────────────────────────────────────────
// On each day a handler is on approved full-day leave, their clients' groups
// are told, and that day's tasks are pushed to the handler's next working day.
// Scattered leave (18th, 20th, 22nd…) is handled a date at a time: each is its
// own notice, and the push skips the handler's other leave days.
//
// Only the due DATE moves — the task stays with the same handler, and its
// status is left alone so this never shows up as a "revised" against them.
async function sendHandlerLeaveNotices({ force = false } = {}) {
  const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
  const today = istNow.toISOString().split('T')[0];
  if (!force && isLastSaturdayOfMonth(today)) return { ok: true, skipped: 'off day', sent: 0 };

  // Candidates: anyone whose approved full-day leave window covers today. The
  // window can be wider than the actual days when dates_json is used, so each
  // one is expanded and re-checked below.
  const [cands] = await db.query(
    `SELECT DISTINCT lr.user_id, u.name, u.week_off, u.extra_off
       FROM leave_requests lr JOIN users u ON lr.user_id = u.id
      WHERE lr.status='approved' AND lr.leave_type='full_day'
        AND lr.from_date <= ? AND lr.to_date >= ?`, [today, today]);
  if (!cands.length) return { ok: true, sent: 0, quiet: 'nobody on approved leave today' };

  const holidaysSet = await loadHolidaysSet();
  let sent = 0, moved = 0, handlers = 0;

  for (const h of cands) {
    // A week back as well as ahead, so today can be placed within its block.
    const leaveDates = await approvedLeaveDates(h.user_id, _addDays(today, -7), _addDays(today, 60));
    if (!leaveDates.has(today)) continue;   // window covered today, the actual days did not
    handlers++;
    const backOn = nextWorkingDayOffLeave(h, today, holidaysSet, leaveDates);
    // "Back on" earns its place only for a run of consecutive days, where the
    // client genuinely does not know when the handler returns. On an isolated
    // day it says nothing they cannot work out, so it is left off.
    const inBlock = leaveDates.has(_addDays(today, 1)) || leaveDates.has(_addDays(today, -1));

    // Push today's tasks, remembering which client each belonged to so the
    // message only claims a move for the client it actually affected.
    const [due] = await db.query(
      `SELECT id, client_id FROM delegation_tasks
        WHERE assigned_to=? AND status IN ('pending','revised') AND due_date=?`, [h.user_id, today]);
    const movedPerClient = new Map();
    for (const t of due) {
      await db.query('UPDATE delegation_tasks SET due_date=? WHERE id=?', [backOn, t.id]);
      logTaskActivity({ taskId: t.id, field: 'due_date', oldValue: today, newValue: backOn,
        source: 'handler-leave', note: `${h.name || 'handler'} on leave` });
      moved++;
      if (t.client_id) movedPerClient.set(t.client_id, (movedPerClient.get(t.client_id) || 0) + 1);
    }

    // Every client this person handles, primary or secondary, that has a group.
    const [clients] = await db.query(
      `SELECT DISTINCT c.id, c.whatsapp_group_id AS g
         FROM clients c
         LEFT JOIN client_handlers ch ON ch.client_id = c.id
        WHERE (c.handler_id=? OR ch.user_id=?)
          AND c.whatsapp_group_id IS NOT NULL AND c.whatsapp_group_id <> ''`,
      [h.user_id, h.user_id]);

    for (const c of clients) {
      await sendWhatsAppRaw(c.g, handlerLeaveMessage(h.name, today, backOn, movedPerClient.get(c.id) || 0, inBlock))
        .catch(e => console.error('WA leave notice err:', e.message));
      sent++;
    }
  }
  return { ok: true, sent, handlers, tasksMoved: moved };
}

function handlerLeaveMessage(name, todayYmd, backOnYmd, movedCount, inBlock) {
  const d = s => s.split('-').reverse().join('-');
  return `📢 *Handler on Leave*\n\n` +
    `*${name}* is on leave today (${d(todayYmd)}).\n\n` +
    (inBlock ? `*Back on:* ${d(backOnYmd)}\n` : '') +
    // Only claimed when something of theirs actually moved.
    (movedCount ? `*Your pending tasks:* moved to ${d(backOnYmd)}\n` : '') +
    `\n— E-Marketing Task Manager`;
}

app.get('/api/cron/handler-leave-notice', async (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    console.log('  ⏰ Cron triggered: handler-leave-notice');
    res.json(await sendHandlerLeaveNotices({ force: req.query.force === '1' }));
  } catch (err) {
    console.error('Cron handler-leave-notice error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/cron/client-pending-digest', async (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    console.log('  ⏰ Cron triggered: client-pending-digest');
    res.json(await sendClientPendingDigests({ force: req.query.force === '1' }));
  } catch (err) {
    console.error('Cron client-pending-digest error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/cron/due-date-reminder', async (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    console.log('  ⏰ Cron triggered: due-date-reminder');
    res.json(await remindHandlersOfMissingDueDates({ force: req.query.force === '1' }));
  } catch (err) {
    console.error('Cron due-date-reminder error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── Leave Tracker Reminder helper ──
async function sendLeaveTrackerReminder() {
  const now = new Date();
  const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  const lastMonthIndex = now.getMonth() === 0 ? 11 : now.getMonth() - 1;
  const lastMonthName = monthNames[lastMonthIndex];
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const yyyy = now.getFullYear();
  const msg = `Hello Everyone 👋,\nPlease update the leave tracker for the month of ${lastMonthName} in the Task Manager app by 05/${mm}/${yyyy}.\nThank You.`;
  await sendWhatsAppRaw('919602694444-1618492040@g.us', msg);
  console.log('Leave tracker reminder sent:', msg);
  return msg;
}

// Cron trigger (GitHub Actions)
app.get('/api/cron/leave-tracker-reminder', async (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    const msg = await sendLeaveTrackerReminder();
    res.json({ success: true, message: msg });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Admin manual trigger (for testing)
app.post('/api/admin/send-leave-reminder', requireAuth, async (req, res) => {
  if (req.session.role !== 'admin') return res.status(403).json({ error: 'Access denied' });
  try {
    const msg = await sendLeaveTrackerReminder();
    res.json({ success: true, message: msg });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ══════════════════════════════════════════════════════
// CLIENTS — admin manages, everyone reads
// ══════════════════════════════════════════════════════
// Client portal — only callable by users with role='client'. Returns the
// linked client info + handler + all tasks (delegation + checklist) tagged
// to that client.
// Parse the JSON system_links column into a clean [{label,url}] array (safe on null/garbage).
function parseSystemLinks(raw) {
  if (!raw) return [];
  try {
    const a = typeof raw === 'string' ? JSON.parse(raw) : raw;
    if (!Array.isArray(a)) return [];
    return a
      .map(l => ({ label: String(l.label || '').trim(), url: String(l.url || '').trim(), liveDate: String(l.liveDate || '').trim() }))
      .filter(l => l.label && l.url);
  } catch { return []; }
}
// Sanitize incoming system_links (from admin) → JSON string ready for the DB.
function sanitizeSystemLinks(input) {
  let arr = input;
  if (typeof arr === 'string') { try { arr = JSON.parse(arr); } catch { arr = []; } }
  if (!Array.isArray(arr)) arr = [];
  const clean = arr
    .map(l => ({
      label: String(l && l.label || '').trim().slice(0, 60),
      url: String(l && l.url || '').trim().slice(0, 500),
      liveDate: String(l && l.liveDate || '').trim().slice(0, 10)
    }))
    .filter(l => l.label && l.url)
    .slice(0, 20);
  return JSON.stringify(clean);
}

// Decide whose portal data a request is allowed to READ.
//  - role='client'          → always their own linked client; ?clientId= is ignored,
//                             so a client can never read another client's portal.
//  - admin / hod / pc       → may pass ?clientId=N to read any client's portal.
//                             This backs the Client Master "Client Dashboard"
//                             button, which opens /client?clientId=N.
//  - any other staff member → same, but only for clients they actually handle,
//                             so a handler can open their own clients' portals
//                             (and delegate to them) without seeing the rest.
//  - anyone else            → 403.
// Only the GET endpoints use this. Every write endpoint below (password, feedback
// POST/PUT/DELETE) keeps its own hard role==='client' check on purpose: staff
// preview must never be able to submit escalations or change a client's password.
async function resolvePortalClientId(req) {
  if (req.session.role === 'client') {
    const [[u]] = await db.query('SELECT client_id FROM users WHERE id=?', [req.session.userId]);
    if (!u?.client_id) return { error: 'No linked client', status: 404 };
    return { id: u.client_id, preview: false };
  }
  const wanted = parseInt(req.query.clientId);
  if (!wanted) return { error: 'Client portal only', status: 403 };
  const [[c]] = await db.query('SELECT id, handler_id FROM clients WHERE id=?', [wanted]);
  if (!c) return { error: 'Client not found', status: 404 };
  if (['admin', 'hod', 'pc'].includes(req.session.role)) return { id: c.id, preview: true };
  // Not a manager — allow only if this user handles this client. Check both the
  // primary handler_id and the many-to-many client_handlers table.
  if (await isHandlerOf(req.session.userId, c)) return { id: c.id, preview: true };
  return { error: 'Client portal only', status: 403 };
}

// True when `userId` is a handler of the given client row (primary or secondary).
async function isHandlerOf(userId, client) {
  if (!client) return false;
  if (Number(client.handler_id) === Number(userId)) return true;
  const [[row]] = await db.query(
    'SELECT 1 AS ok FROM client_handlers WHERE client_id=? AND user_id=? LIMIT 1', [client.id, userId]);
  return !!row;
}

// Client changes their own portal login password.
app.put('/api/client-portal/password', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'client') return res.status(403).json({ error: 'Client portal only' });
    const { currentPassword, newPassword } = req.body || {};
    if (!newPassword || String(newPassword).length < 6) return res.status(400).json({ error: 'New password must be at least 6 characters' });
    const [[u]] = await db.query('SELECT password FROM users WHERE id=?', [req.session.userId]);
    if (!u) return res.status(404).json({ error: 'User not found' });
    if (!currentPassword || !bcrypt.compareSync(currentPassword, u.password)) return res.status(400).json({ error: 'Current password is incorrect' });
    await db.query('UPDATE users SET password=? WHERE id=?', [bcrypt.hashSync(String(newPassword), 10), req.session.userId]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/client-portal/me', requireAuth, async (req, res) => {
  try {
    const resolved = await resolvePortalClientId(req);
    if (resolved.error) return res.status(resolved.status).json({ error: resolved.error });
    const [[c]] = await db.query(
      `SELECT c.id, c.name, c.handler_id, u.name AS handler_name, u.email AS handler_email
       FROM clients c LEFT JOIN users u ON c.handler_id = u.id WHERE c.id=?`, [resolved.id]);
    res.json(c || null);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Names excluded from client-portal Top Performers panel. These are internal
// roles (admin / owner) whose completion counts shouldn't show on a client-
// facing leaderboard. Match is case-insensitive on the trimmed name.
const TOP_PERFORMER_EXCLUDE_NAMES = ['Abhishek Jain', 'Simran Gurnani'];

// Client-portal stats — same shape as /api/clients/:id/stats but auto-resolves
// the client_id from the logged-in client session. Defaults to current month.
app.get('/api/client-portal/stats', requireAuth, async (req, res) => {
  try {
    const resolved = await resolvePortalClientId(req);
    if (resolved.error) return res.status(resolved.status).json({ error: resolved.error });
    const id = resolved.id;
    const [[client]] = await db.query(
      `SELECT c.id, c.name, c.handler_id, c.logo_url, c.system_links, u.name AS handler_name, u.email AS handler_email
       FROM clients c LEFT JOIN users u ON c.handler_id = u.id WHERE c.id=?`, [id]);
    if (client) client.system_links = parseSystemLinks(client.system_links);
    // The client's own portal login, if one exists. A handler needs it to
    // delegate TO the client; when it is null the UI says so instead of
    // offering an option that cannot work.
    if (client) {
      const [[pu]] = await db.query(
        `SELECT id, name FROM users WHERE role='client' AND client_id=? ORDER BY id LIMIT 1`, [id]);
      client.portal_user_id   = pu?.id   || null;
      client.portal_user_name = pu?.name || null;
      // A client can have several handlers (client_handlers); c.handler_id is
      // only the primary. The portal needs them all so a multi-handler client
      // can pick which handler a task goes to, and see every name in the header.
      const [handlerRows] = await db.query(
        `SELECT u.id, u.name, u.email, u.department
           FROM client_handlers ch JOIN users u ON ch.user_id = u.id
          WHERE ch.client_id=? AND u.role!='client'
          ORDER BY (u.id=?) DESC, u.name`, [id, client.handler_id || 0]);
      let handlers = handlerRows;
      if (!handlers.length && client.handler_id) {
        handlers = [{ id: client.handler_id, name: client.handler_name, email: client.handler_email, department: null }];
      }
      client.handlers = handlers;
      // Keep the legacy single fields pointed at the primary for older UI paths.
      if (!client.handler_id && handlers.length) {
        client.handler_id    = handlers[0].id;
        client.handler_name  = handlers[0].name;
        client.handler_email = handlers[0].email;
      }
    }
    // Who is looking: the client themselves, or a staff member previewing.
    const viewerId = resolved.preview ? null : req.session.userId;
    const ist = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
    const y = ist.getUTCFullYear(), m = ist.getUTCMonth();
    const defaultFrom = `${y}-${String(m+1).padStart(2,'0')}-01`;
    const lastDay = new Date(Date.UTC(y, m+1, 0)).getUTCDate();
    const defaultTo = `${y}-${String(m+1).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;
    const isDate = v => /^\d{4}-\d{2}-\d{2}$/.test(v || '');
    const from = isDate(req.query.from) ? req.query.from : defaultFrom;
    const to   = isDate(req.query.to)   ? req.query.to   : defaultTo;
    const [[del]] = await db.query(
      `SELECT COUNT(*) AS total,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='revised'   THEN 1 ELSE 0 END) AS revised,
        SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM delegation_tasks WHERE client_id=? AND due_date BETWEEN ? AND ?`, [id, from, to]);
    const [[chl]] = await db.query(
      `SELECT COUNT(*) AS total,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM checklist_tasks WHERE client_id=? AND due_date BETWEEN ? AND ?`, [id, from, to]);
    const [[meet]] = await db.query(
      `SELECT COUNT(*) AS total,
        SUM(CASE WHEN status='scheduled' THEN 1 ELSE 0 END) AS scheduled,
        SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
        SUM(CASE WHEN status='done'      THEN 1 ELSE 0 END) AS done
       FROM meetings WHERE client_id=? AND meeting_date BETWEEN ? AND ?`, [id, from, to]);
    const [meetRecent] = await db.query(
      `SELECT m.id, m.title, m.status, m.meet_link,
              DATE_FORMAT(m.meeting_date,'%Y-%m-%d') AS meeting_date,
              TIME_FORMAT(m.start_time,'%H:%i') AS start_time,
              TIME_FORMAT(m.end_time,'%H:%i')   AS end_time,
              u.name AS organizer_name
       FROM meetings m LEFT JOIN users u ON m.organizer_id = u.id
       WHERE m.client_id=? AND m.meeting_date BETWEEN ? AND ?
       ORDER BY m.meeting_date DESC, m.start_time DESC LIMIT 15`, [id, from, to]);
    // created_date / created_time record when the task was RAISED, as opposed
    // to due_date. Both are formatted in SQL and never sent as a raw DATETIME:
    // created_at is already the DB's local IST, and handing a DATETIME to the
    // browser lets it be read as UTC and shifted a second time.
    const [recentDel] = await db.query(
      `SELECT t.id, 'delegation' AS type, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              TIME_FORMAT(t.due_time,'%H:%i') AS due_time, t.assigned_to,
              COALESCE(u1.name,'— deleted user —') AS doer, DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created,
              DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created_date,
              TIME_FORMAT(t.created_at,'%H:%i') AS created_time,
              DATE_FORMAT(t.completed_at,'%Y-%m-%d') AS done_date,
              TIME_FORMAT(t.completed_at,'%H:%i') AS done_time
       FROM delegation_tasks t LEFT JOIN users u1 ON t.assigned_to=u1.id
       WHERE t.client_id=? AND DATE(t.created_at) BETWEEN ? AND ?
       ORDER BY t.created_at DESC LIMIT 25`, [id, from, to]);
    const [recentChl] = await db.query(
      `SELECT t.id, 'checklist' AS type, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              COALESCE(u1.name,'— deleted user —') AS doer, DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created,
              DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created_date,
              TIME_FORMAT(t.created_at,'%H:%i') AS created_time,
              NULL AS done_date, NULL AS done_time
       FROM checklist_tasks t LEFT JOIN users u1 ON t.assigned_to=u1.id
       WHERE t.client_id=? AND DATE(t.created_at) BETWEEN ? AND ?
       ORDER BY t.created_at DESC LIMIT 25`, [id, from, to]);
    const recent = [...recentDel, ...recentChl]
      .sort((a,b) => (b.created||'').localeCompare(a.created||''))
      .slice(0, 20);
    // Daily activity buckets — tasks created per day in the window. Used by
    // the bar chart in the client portal.
    const [dailyDel] = await db.query(
      `SELECT DATE_FORMAT(created_at,'%Y-%m-%d') AS d, COUNT(*) AS c
       FROM delegation_tasks WHERE client_id=? AND DATE(created_at) BETWEEN ? AND ?
       GROUP BY d`, [id, from, to]);
    const [dailyChl] = await db.query(
      `SELECT DATE_FORMAT(created_at,'%Y-%m-%d') AS d, COUNT(*) AS c
       FROM checklist_tasks WHERE client_id=? AND DATE(created_at) BETWEEN ? AND ?
       GROUP BY d`, [id, from, to]);
    const [dailyDone] = await db.query(
      `SELECT DATE_FORMAT(due_date,'%Y-%m-%d') AS d, COUNT(*) AS c
       FROM delegation_tasks WHERE client_id=? AND status='completed' AND due_date BETWEEN ? AND ?
       GROUP BY d`, [id, from, to]);
    const [dailyDoneChl] = await db.query(
      `SELECT DATE_FORMAT(due_date,'%Y-%m-%d') AS d, COUNT(*) AS c
       FROM checklist_tasks WHERE client_id=? AND status='completed' AND due_date BETWEEN ? AND ?
       GROUP BY d`, [id, from, to]);
    const createdByDay = {}, doneByDay = {};
    for (const r of [...dailyDel, ...dailyChl]) createdByDay[r.d] = (createdByDay[r.d]||0) + (parseInt(r.c)||0);
    for (const r of [...dailyDone, ...dailyDoneChl]) doneByDay[r.d] = (doneByDay[r.d]||0) + (parseInt(r.c)||0);
    // Upcoming Deadlines — pending tasks (delegation + checklist) sorted by
    // soonest due date. Overdue rows surface first.
    const [upDel] = await db.query(
      `SELECT t.id, 'delegation' AS type, t.description, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              TIME_FORMAT(t.due_time,'%H:%i') AS due_time, t.assigned_to,
              u.name AS doer
       FROM delegation_tasks t JOIN users u ON t.assigned_to=u.id
       WHERE t.client_id=? AND t.status IN ('pending','revised')
       ORDER BY t.due_date ASC LIMIT 15`, [id]);
    const [upChl] = await db.query(
      `SELECT t.id, 'checklist' AS type, t.description, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u.name AS doer
       FROM checklist_tasks t JOIN users u ON t.assigned_to=u.id
       WHERE t.client_id=? AND t.status='pending'
         AND t.due_date <= DATE_ADD(CURDATE(), INTERVAL 30 DAY)
       ORDER BY t.due_date ASC LIMIT 15`, [id]);
    const upcoming = [...upDel, ...upChl]
      .sort((a,b) => (a.due_date||'').localeCompare(b.due_date||''))
      .slice(0, 10);
    // Tasks the client themselves owe us — the handler→client direction. Kept
    // separate from `recent`/`upcoming` (which cover everything tagged to this
    // client, whoever the doer is) because this is the only list the client can
    // act on.
    const [clientTasks] = client?.portal_user_id ? await db.query(
      `SELECT t.id, 'delegation' AS type, t.description, t.status, t.priority,
              COALESCE(t.waiting_approval,0) AS waiting_approval, t.remarks,
              DATE_FORMAT(t.created_at,'%Y-%m-%d') AS delegated_on,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              TIME_FORMAT(t.due_time,'%H:%i') AS due_time,
              COALESCE(u.name,'—') AS assigned_by_name
       FROM delegation_tasks t LEFT JOIN users u ON t.assigned_by=u.id
       WHERE t.assigned_to=? ORDER BY t.status='completed', t.due_date ASC, t.due_time ASC
       LIMIT 100`, [client.portal_user_id]) : [[]];
    res.json({
      client, range: { from, to },
      delegation: del, checklist: chl, meetings: { ...meet, recent: meetRecent },
      recent,
      upcoming,
      clientTasks,
      viewerId
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/client-portal/tasks', requireAuth, async (req, res) => {
  try {
    const resolved = await resolvePortalClientId(req);
    if (resolved.error) return res.status(resolved.status).json({ error: resolved.error });
    const cid = resolved.id;
    const [delegation] = await db.query(
      `SELECT t.id, 'delegation' AS type, t.description, t.status, t.priority,
              COALESCE(t.waiting_approval,0) AS waiting_approval, t.remarks, t.url,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u1.name AS assignedToName, COALESCE(u2.name,'—') AS assignedByName
       FROM delegation_tasks t
       JOIN users u1 ON t.assigned_to=u1.id
       LEFT JOIN users u2 ON t.assigned_by=u2.id
       WHERE t.client_id=? ORDER BY t.due_date DESC LIMIT 500`, [cid]);
    const [checklist] = await db.query(
      `SELECT t.id, 'checklist' AS type, t.description, t.status, t.priority,
              0 AS waiting_approval, t.remarks, NULL AS url,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u1.name AS assignedToName, COALESCE(u2.name,'—') AS assignedByName
       FROM checklist_tasks t
       JOIN users u1 ON t.assigned_to=u1.id
       LEFT JOIN users u2 ON t.assigned_by=u2.id
       WHERE t.client_id=? ORDER BY t.due_date DESC LIMIT 500`, [cid]);
    res.json([...delegation, ...checklist]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Client feedback endpoints ─────────────────────────────────────────────

// Returns handlers assigned to this client (for the feedback form).
app.get('/api/client-portal/handlers', requireAuth, async (req, res) => {
  try {
    const resolved = await resolvePortalClientId(req);
    if (resolved.error) return res.status(resolved.status).json({ error: resolved.error });
    const [handlers] = await db.query(
      `SELECT ch.user_id AS id, u.name, u.department,
              (u.user_role='hod' OR u.role='hod') AS is_hod
       FROM client_handlers ch JOIN users u ON ch.user_id = u.id
       WHERE ch.client_id = ? AND u.role != 'client'`, [resolved.id]);
    // Find HOD for each unique department (with id)
    const depts = [...new Set(handlers.map(h => h.department).filter(Boolean))];
    const hodMap = {};
    for (const dept of depts) {
      const [hods] = await db.query(`SELECT id, name FROM users WHERE department=? AND (user_role='hod' OR role='hod')`, [dept]);
      if (hods.length) hodMap[dept] = hods;
    }
    // Fixed recipients: Abhishek Jain and Simran Gurnani
    const [fixedRows] = await db.query(
      `SELECT id, name FROM users WHERE name IN ('Abhishek Jain','Simran Gurnani') AND role != 'client'`);
    res.json({ handlers, hodMap, fixedRecipients: fixedRows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Submit feedback from client portal.
app.post('/api/client-portal/feedback', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'client') return res.status(403).json({ error: 'Client portal only' });
    const [[u]] = await db.query('SELECT client_id FROM users WHERE id=?', [req.session.userId]);
    if (!u?.client_id) return res.status(404).json({ error: 'No linked client' });
    const { employee_id, rating, description, recipients } = req.body;
    if (!employee_id || !rating) return res.status(400).json({ error: 'Employee and rating are required' });
    const r = parseInt(rating);
    if (r < 1 || r > 5) return res.status(400).json({ error: 'Rating must be between 1 and 5' });
    const recipientsStr = Array.isArray(recipients) ? recipients.join(',') : (recipients || '');
    await db.query(
      'INSERT INTO client_feedback (client_id, employee_id, rating, description, recipients) VALUES (?, ?, ?, ?, ?)',
      [u.client_id, parseInt(employee_id), r, (description || '').trim(), recipientsStr]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// CLIENT CREDENTIALS VAULT — admin-only
// Stores the logins for whatever the team builds for a client (a bespoke task
// manager, a dashboard, etc.). Passwords are stored as typed so they can be
// read back, so every route here is requireAdmin — never widen it.
// ══════════════════════════════════════════════════════
// The cold-start IIFE also creates this table, but a request can land on a
// fresh serverless instance before that finishes (or on a DB the deploy's init
// never reached), which shows up as a 500 on the very first save. Ensure the
// table lazily on each call — memoized, so it's one CREATE per process, then a
// no-op. Same self-heal pattern the Credit Cards routes use.
// Surface the REAL cause instead of a bare "HTTP 500". The frontend only ever
// showed "HTTP 500" because the old catch returned {error: err.message} and some
// driver errors carry the useful text on .code/.sqlMessage, not .message. Log
// the full error server-side (Vercel function logs) and return a readable line.
function ccVaultErr(res, err, op){
  console.error(`[client-credentials:${op}]`, err && (err.stack || err));
  const detail = (err && (err.sqlMessage || err.message)) || 'Unknown error';
  const code = err && err.code ? `${err.code}: ` : '';
  res.status(500).json({ error: `Vault ${op} failed — ${code}${detail}` });
}

let _ccVaultReady = null;
function ensureClientCredentialsTable(){
  if (!_ccVaultReady) {
    _ccVaultReady = db.query(`CREATE TABLE IF NOT EXISTS client_credentials (
      id INT AUTO_INCREMENT PRIMARY KEY,
      client_id INT NOT NULL,
      system_name VARCHAR(255) NOT NULL,
      role_label VARCHAR(100) DEFAULT NULL,
      url VARCHAR(1000) DEFAULT NULL,
      username VARCHAR(500) DEFAULT NULL,
      password VARCHAR(500) DEFAULT NULL,
      notes TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_by_name VARCHAR(255) DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_cc_client (client_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`).catch(e => { _ccVaultReady = null; throw e; });
  }
  return _ccVaultReady;
}

// List every stored credential, newest system first, with the client name
// joined in so the vault can group by client. Optional ?client_id= narrows it.
app.get('/api/client-credentials', requireAuth, requireAdmin, async (req, res) => {
  try {
    await ensureClientCredentialsTable();
    const clientId = parseInt(req.query.client_id);
    const where = clientId ? 'WHERE cc.client_id=?' : '';
    const params = clientId ? [clientId] : [];
    const [rows] = await db.query(
      `SELECT cc.id, cc.client_id, c.name AS client_name, cc.system_name, cc.role_label,
              cc.url, cc.username, cc.password, cc.notes, cc.created_by_name,
              DATE_FORMAT(cc.updated_at,'%Y-%m-%d %H:%i') AS updated_at
         FROM client_credentials cc
         LEFT JOIN clients c ON cc.client_id = c.id
         ${where}
         ORDER BY c.name ASC, cc.system_name ASC, cc.role_label ASC, cc.id ASC`, params);
    res.json(rows);
  } catch (err) { ccVaultErr(res, err, 'list'); }
});

// Add a credential entry. client_id + system_name are the only required fields;
// a system can have several rows (e.g. an Admin login and a User login).
app.post('/api/client-credentials', requireAuth, requireAdmin, async (req, res) => {
  try {
    await ensureClientCredentialsTable();
    const clientId = parseInt(req.body.client_id);
    const system = String(req.body.system_name || '').trim();
    if (!clientId || !system) return res.status(400).json({ error: 'Client and system name are required' });
    const [[c]] = await db.query('SELECT id FROM clients WHERE id=?', [clientId]);
    if (!c) return res.status(404).json({ error: 'Client not found' });
    const clean = (v, n) => { const s = (v == null ? '' : String(v)).trim(); return s ? s.slice(0, n) : null; };
    const [r] = await db.query(
      `INSERT INTO client_credentials
         (client_id, system_name, role_label, url, username, password, notes, created_by, created_by_name)
       VALUES (?,?,?,?,?,?,?,?,?)`,
      [clientId, system.slice(0, 255), clean(req.body.role_label, 100), clean(req.body.url, 1000),
       clean(req.body.username, 500), clean(req.body.password, 500), clean(req.body.notes, 5000),
       req.session.userId, req.session.name || null]);
    res.json({ success: true, id: r.insertId });
  } catch (err) { ccVaultErr(res, err, 'add'); }
});

// Edit a credential entry. Same field rules as add; client_id is not moved.
app.put('/api/client-credentials/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    await ensureClientCredentialsTable();
    const id = parseInt(req.params.id);
    const system = String(req.body.system_name || '').trim();
    if (!system) return res.status(400).json({ error: 'System name is required' });
    const clean = (v, n) => { const s = (v == null ? '' : String(v)).trim(); return s ? s.slice(0, n) : null; };
    const [r] = await db.query(
      `UPDATE client_credentials
          SET system_name=?, role_label=?, url=?, username=?, password=?, notes=?
        WHERE id=?`,
      [system.slice(0, 255), clean(req.body.role_label, 100), clean(req.body.url, 1000),
       clean(req.body.username, 500), clean(req.body.password, 500), clean(req.body.notes, 5000), id]);
    if (!r.affectedRows) return res.status(404).json({ error: 'Credential not found' });
    res.json({ success: true });
  } catch (err) { ccVaultErr(res, err, 'edit'); }
});

// Delete a credential entry. Archived to deleted_records first (same as every
// other user-facing delete) so nothing is ever truly lost.
app.delete('/api/client-credentials/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    await ensureClientCredentialsTable();
    const id = parseInt(req.params.id);
    const [[row]] = await db.query('SELECT * FROM client_credentials WHERE id=?', [id]);
    if (!row) return res.status(404).json({ error: 'Credential not found' });
    await archiveDeleted('client_credentials', row, req, {
      summary: r => `Credential: ${r.system_name || ''}${r.role_label ? ' · ' + r.role_label : ''}` });
    await db.query('DELETE FROM client_credentials WHERE id=?', [id]);
    res.json({ success: true });
  } catch (err) { ccVaultErr(res, err, 'delete'); }
});

// ══════════════════════════════════════════════════════
// UPLOAD LIMITS + CREDIT CARD ACCESS
// ══════════════════════════════════════════════════════
// The Credit Cards routes themselves live in routes/credit-cards.js now. What
// stays here is what something else also needs: dmsUpload belongs to the DMS
// client-folder upload route further down, and canViewCreditCards is read by
// /api/me to tell the frontend whether to show the nav item. The two cc*
// multer instances stay beside dmsUpload so all three size limits are in one
// place, and are passed into the module.
const ccUpload    = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
const ccPdfUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });
const dmsUpload   = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

// Credit Card Statement access. Admins get full read/write. The users behind
// cc_viewer_ids are read-only: they can open the page and see the existing
// data, but cannot upload, edit, or delete anything. Ids, not names — a rename
// used to move this access silently. Async now, since it reads app_settings.
async function canViewCreditCards(session) {
  if (session.role === 'admin') return true;
  return (await readIdSetting('cc_viewer_ids')).includes(Number(session.userId));
}
function canEditCreditCards(session) {
  return session.role === 'admin';
}

// CREDIT CARDS routes now live in routes/credit-cards.js. The call sits exactly
// where the routes did, so every binding passed in is in scope at the same
// point it always was.
require('./backend/routes/credit-cards')(app, {
  db,
  requireAuth,
  archiveDeleted,
  canViewCreditCards,
  canEditCreditCards,
  ccUpload,
  ccPdfUpload,
  XLSX,
});

// Who may approve a payment request, and who may mark one paid. Held as user
// ids in app_settings, not as names in this file: a name is display data, and
// keying rights to it meant a rename in the admin screen silently moved the
// authority — lost by that person, or gained by whoever now held the string,
// with no error and nothing logged. Two people sharing a name both passed.
//
// The names below are used exactly once, by the seed in seedPaymentRoleIds(),
// to fill the settings on first boot. After that they are historical.
// Everywhere a person is singled out by role. Each key holds a JSON array of
// user ids in app_settings; the names on the right are seed values, read once
// on first boot and never again. Adding a key here is all it takes — the seeder
// below walks this object.
const PEOPLE_SETTINGS = {
  // payment_settler_ids is deliberately absent. It named one person (Vishal
  // Jaga) as the only one who could mark a payment paid, but the single route
  // that consulted it had no caller anywhere in the repo, its frontend twin
  // prMarkPaymentDone() was never wired to a button, and the canSettlePayments
  // flag it fed was read by nothing. In the live flow the requester marks their
  // own request paid — a deliberate design, see the note above payStatusCell in
  // app.html. The app_settings row may still exist; nothing reads it.
  payment_approver_ids: ['Naman Gupta', 'Abhishek Jain', 'Simran Gurnani'],
  cc_viewer_ids:        ['Rotan Singh'],
  wa_task_approver_ids: ['Naman Gupta'],
  mdo_reviewer_ids:     ['Purvi Saini'],
  onboarding_owner_ids: ['Simran Gurnani'],
};
const PR_APPROVER_KEY = 'payment_approver_ids';

async function readIdSetting(key) {
  try {
    const [[row]] = await db.query('SELECT value FROM app_settings WHERE key_name=?', [key]);
    const ids = row?.value ? JSON.parse(row.value) : [];
    return Array.isArray(ids) ? ids.map(Number).filter(Number.isFinite) : [];
  } catch { return []; }
}

// Admins keep blanket access, exactly as before.
async function isPaymentApprover(session) {
  if (session.role === 'admin') return true;
  return (await readIdSetting(PR_APPROVER_KEY)).includes(Number(session.userId));
}
// The users behind a key, for notification lookups. Returns [] when the setting
// is empty or every id has since been deleted — callers already treat a missing
// phone as "skip the send", which is the same outcome the name lookup gave when
// it matched nothing, except this way the reason is visible in the settings row.
async function usersForSetting(key, cols = 'id, name, phone') {
  const ids = await readIdSetting(key);
  if (!ids.length) return [];
  try {
    const [rows] = await db.query(
      `SELECT ${cols} FROM users WHERE id IN (${ids.map(() => '?').join(',')})`, ids);
    return rows;
  } catch { return []; }
}

// One-time: turn the seed names into ids. Unresolved names are logged rather
// than dropped, so a typo or a departed employee is visible at boot instead of
// quietly costing someone their access.
async function seedPaymentRoleIds() {
  // Called from the startup block, which is defined earlier in the file than
  // the consts above. It only reaches this point after several awaits, by which
  // time module evaluation has finished — but a seed failing must never take
  // the boot down with it, so the whole body is guarded.
  try {
  for (const [key, names] of Object.entries(PEOPLE_SETTINGS)) {
    try {
      const [[existing]] = await db.query('SELECT value FROM app_settings WHERE key_name=?', [key]);
      if (existing) continue;
      const [rows] = await db.query(
        `SELECT id, name FROM users WHERE name IN (${names.map(() => '?').join(',')})`, names);
      const ids = rows.map(r => r.id);
      const missing = names.filter(n => !rows.some(r => r.name === n));
      await db.query('INSERT INTO app_settings (key_name, value) VALUES (?,?)', [key, JSON.stringify(ids)]);
      console.log(`  ✅ ${key} seeded with ${ids.length} id(s)`
        + (missing.length ? ` — NO USER MATCHED: ${missing.join(', ')}` : ''));
    } catch (e) { console.log(`  ⚠️ ${key} seed skipped —`, e.code || e.message); }
  }
  } catch (e) { console.log('  ⚠️ payment role seed skipped —', e.code || e.message); }
}

// Payment Requests routes now live in routes/payment-requests.js. The call
// sits where the main block did, so every binding passed in is in scope at
// the same point it always was.
require('./backend/routes/payment-requests')(app, {
  db,
  requireAuth,
  requireAdmin,
  archiveDeleted,
  emailUserWaText,
  isPaymentApprover,
  sendWhatsApp,
});
// GET /api/mdo-tasks — WhatsApp-bot task intake queue (admin/MDO only)
app.get('/api/mdo-tasks', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const [rows] = await db.query('SELECT * FROM tasks ORDER BY timestamp DESC');
    res.json(rows);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// PATCH /api/mdo-tasks/:id — approve or reject (admin/MDO only)
app.patch('/api/mdo-tasks/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const { status } = req.body;
    if (!['Approved','Rejected'].includes(status)) return res.status(400).json({ error:'Invalid status' });
    const [[task]] = await db.query('SELECT * FROM tasks WHERE id=?', [req.params.id]);
    if (!task) return res.status(404).json({ error:'Task not found' });

    const assignedByName = task.assigned_by || task.assigned_name;
    const [[assignedToUser]] = await db.query('SELECT id, phone FROM users WHERE name=?', [task.assigned_to]);
    const [[assignedByUser]] = await db.query('SELECT id, phone FROM users WHERE name=?', [assignedByName]);

    let delegationTaskId = null;
    if (status === 'Approved') {
      if (!assignedToUser || !assignedByUser) {
        const missing = !assignedToUser ? `Assigned To ("${task.assigned_to}")` : `Assigned By ("${assignedByName}")`;
        return res.status(400).json({ error: `Cannot approve — no matching user found for ${missing}` });
      }
      const dueDate = task.target_date || task.due_date;
      const validPriorities = ['low','medium','high','urgent'];
      const priority = validPriorities.includes(String(task.priority || '').toLowerCase()) ? String(task.priority).toLowerCase() : 'low';
      const [ins] = await db.query(
        `INSERT INTO delegation_tasks
           (description,assigned_to,assigned_by,due_date,status,priority,approval,remarks,client_id,url,awaiting_due_date)
         VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
        [task.task_description || task.description || '', assignedToUser.id, assignedByUser.id, dueDate || null,
         'pending', priority, 'no', task.remarks || null, task.client_id || null, task.url || null, dueDate ? 0 : 1]
      );
      delegationTaskId = ins.insertId;
    }

    await db.query(
      `UPDATE tasks SET status=?, updated_timestamp=NOW()${delegationTaskId ? ', approved_task_id=?' : ''} WHERE id=?`,
      delegationTaskId ? [status, delegationTaskId, req.params.id] : [status, req.params.id]
    );
    const approverName = String(req.session.name || '').toUpperCase();
    const dueDate = task.target_date || task.due_date;
    const dueDateStr = dueDate ? new Date(dueDate).toLocaleDateString('en-IN') : '—';
    const taskDesc = task.task_description || task.description || '—';

    // Admin oversight — always email Naman Gupta (was WhatsApp)
    const [naman] = await usersForSetting('wa_task_approver_ids', 'id, name, email, notification_email');
    const namanEmail = naman && (naman.notification_email || naman.email);
    if (namanEmail) {
      const emoji = status === 'Approved' ? '✅' : '❌';
      const waMsg =
        `${emoji} *Task ${status} by ${approverName}*\n\n` +
        `📋 *Task:* ${taskDesc}\n` +
        `🆔 *Task ID:* ${task.task_id || '—'}\n` +
        `👤 *Assigned To:* ${task.assigned_to || '—'}\n` +
        `🙋 *Assigned By:* ${assignedByName || '—'}\n` +
        `📅 *Due Date:* ${dueDateStr}\n` +
        `🏢 *Client:* ${task.client_name || '—'}\n\n` +
        `Status updated to *${status}*.`;
      sendMail(namanEmail, `Task ${status} by ${approverName}`, waTextToEmailHtml(waMsg)).catch(e => console.error('mdo-task email err:', e.message));
    }

    // Notify Assigned To by email (was WhatsApp)
    if (assignedToUser?.id) {
      const waMsg = status === 'Approved'
        ? `✅ *Task Approved & Assigned to ${task.assigned_to || '—'}*\n\n📋 *Task:* ${taskDesc}\n🆔 *Task ID:* ${task.task_id || '—'}\n🙋 *Assigned By:* ${assignedByName || '—'}\n📅 *Due Date:* ${dueDateStr}\n🏢 *Client:* ${task.client_name || '—'}\n\nThis task has been approved by *${approverName}* and assigned to you.`
        : `❌ *Task Rejected*\n\n📋 *Task:* ${taskDesc}\n🆔 *Task ID:* ${task.task_id || '—'}\n🙋 *Assigned By:* ${assignedByName || '—'}\n\nThis task was reviewed and rejected by *${approverName}*. No action needed from you.`;
      emailUserWaText(assignedToUser.id, `Task ${status}`, waMsg).catch(e => console.error('mdo-task assignedTo email err:', e.message));
    }

    // Notify Assigned By by email (was WhatsApp)
    if (assignedByUser?.id) {
      const waMsg = status === 'Approved'
        ? `✅ *Task Delegated Successfully*\n\n📋 *Task:* ${taskDesc}\n🆔 *Task ID:* ${task.task_id || '—'}\n👤 *Assigned To:* ${task.assigned_to || '—'}\n📅 *Due Date:* ${dueDateStr}\n\nYour task has been approved by *${approverName}* and delegated to *${task.assigned_to}*.`
        : `❌ *Task Request Rejected*\n\n📋 *Task:* ${taskDesc}\n🆔 *Task ID:* ${task.task_id || '—'}\n👤 *Assigned To:* ${task.assigned_to || '—'}\n\nYour task request was reviewed and rejected by *${approverName}*.`;
      emailUserWaText(assignedByUser.id, `Task ${status}`, waMsg).catch(e => console.error('mdo-task assignedBy email err:', e.message));
    }

    res.json({ success: true, delegationTaskId });
  } catch(err) { res.status(500).json({ error: err.message }); }
});


// POST /api/payment-requests/:id/payment-done used to live here — the only
// route gated on payment_settler_ids. It had no caller in this repo, and its
// UPDATE was the sole writer of payment_requests.payment_done, so that column
// has never held anything but 0. "Paid" is carried entirely by the __paid__
// sentinel rows read in app.html. It also shipped a catch() that fell back to
// `SET status="approved"`, which would have approved a pending or rejected
// request on any transient error. Removed with the rest of the settler
// machinery. The payment_done / payment_done_at columns are left in place —
// they are the right home if the sentinel scheme is ever replaced.

// Check if logged-in user can access the feedback page.
app.get('/api/feedback/access', requireAuth, async (req, res) => {
  try {
    const [[me]] = await db.query(
      `SELECT user_role, role, name FROM users WHERE id=?`, [req.session.userId]);
    if (!me) return res.json({ canAccess: false });
    const isAdmin = me.role === 'admin' || me.user_role === 'admin';
    const isHod = me.user_role === 'hod' || me.role === 'hod';
    const [[fixed]] = await db.query(
      `SELECT id FROM users WHERE id=? AND name IN ('Abhishek Jain','Simran Gurnani')`,
      [req.session.userId]);
    res.json({ canAccess: isAdmin || isHod || !!fixed });
  } catch (err) { res.status(500).json({ canAccess: false }); }
});

// Feedback view — only show entries where this user is in the recipients list.
app.get('/api/feedback', requireAuth, async (req, res) => {
  try {
    const userId = req.session.userId;
    const [rows] = await db.query(
      `SELECT f.id, f.rating, f.description, f.created_at,
              c.name AS client_name,
              e.name AS employee_name, e.department,
              hod.name AS hod_name
       FROM client_feedback f
       JOIN clients c ON f.client_id = c.id
       JOIN users e ON f.employee_id = e.id
       LEFT JOIN users hod ON (hod.user_role = 'hod' OR hod.role = 'hod') AND hod.department = e.department
       WHERE FIND_IN_SET(?, f.recipients)
       ORDER BY f.created_at DESC`, [userId]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete a feedback entry (admin/pc only).
app.delete('/api/feedback/:id', requireAuth, async (req, res) => {
  try {
    if (!['admin','pc'].includes(req.session.role)) return res.status(403).json({ error: 'Access denied' });
    const [doomed] = await db.query('SELECT * FROM client_feedback WHERE id=?', [parseInt(req.params.id)]);
    await archiveDeleted('client_feedback', doomed, req, {
      summary: r => `Feedback (${r.rating ?? '?'}★): ${r.description || ''}`,
    });
    await db.query('DELETE FROM client_feedback WHERE id=?', [parseInt(req.params.id)]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Client: get own feedback history.
app.get('/api/client-portal/feedback', requireAuth, async (req, res) => {
  try {
    const resolved = await resolvePortalClientId(req);
    if (resolved.error) return res.status(resolved.status).json({ error: resolved.error });
    const [rows] = await db.query(
      `SELECT f.id, f.employee_id, f.rating, f.description, f.recipients,
              DATE_FORMAT(f.created_at,'%Y-%m-%dT%H:%i:%sZ') AS created_at,
              e.name AS employee_name, e.department
       FROM client_feedback f
       JOIN users e ON f.employee_id = e.id
       WHERE f.client_id = ?
       ORDER BY f.created_at DESC`, [resolved.id]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Client: edit own feedback.
app.put('/api/client-portal/feedback/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'client') return res.status(403).json({ error: 'Client portal only' });
    const [[u]] = await db.query('SELECT client_id FROM users WHERE id=?', [req.session.userId]);
    if (!u?.client_id) return res.status(404).json({ error: 'No linked client' });
    const { rating, description, recipients } = req.body;
    const r = parseInt(rating);
    if (!r || r < 1 || r > 5) return res.status(400).json({ error: 'Rating must be 1–5' });
    const recipientsStr = Array.isArray(recipients) ? recipients.join(',') : (recipients || '');
    const [result] = await db.query(
      'UPDATE client_feedback SET rating=?, description=?, recipients=? WHERE id=? AND client_id=?',
      [r, (description || '').trim(), recipientsStr, parseInt(req.params.id), u.client_id]);
    if (result.affectedRows === 0) return res.status(404).json({ error: 'Feedback not found' });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Client: delete own feedback.
app.delete('/api/client-portal/feedback/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'client') return res.status(403).json({ error: 'Client portal only' });
    const [[u]] = await db.query('SELECT client_id FROM users WHERE id=?', [req.session.userId]);
    if (!u?.client_id) return res.status(404).json({ error: 'No linked client' });
    const [doomed] = await db.query(
      'SELECT * FROM client_feedback WHERE id=? AND client_id=?',
      [parseInt(req.params.id), u.client_id]);
    await archiveDeleted('client_feedback', doomed, req, {
      summary: r => `Feedback (${r.rating ?? '?'}★): ${r.description || ''}`,
      reason: 'Deleted by the client from the client portal',
    });
    const [result] = await db.query(
      'DELETE FROM client_feedback WHERE id=? AND client_id=?',
      [parseInt(req.params.id), u.client_id]);
    if (result.affectedRows === 0) return res.status(404).json({ error: 'Feedback not found' });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// CLIENT MASTER routes now live in routes/clients.js. The call sits exactly
// where the routes did, so every binding passed in is in scope at the same
// point it always was.
require('./backend/routes/clients')(app, {
  db,
  express,
  bcrypt,
  requireAuth,
  requireAdmin,
  requireAdminOrHod,
  requireClientsEditor,
  requireClientEditor,
  archiveDeleted,
  userCanSee,
  userCanDo,
  isHandlerOf,
  parseSystemLinks,
  sanitizeSystemLinks,
  getDriveClient,
  dmsUpload,
  dmsCreateFolder,
  dmsCreateFile,
  dmsListFiles,
  dmsShareFolder,
  dmsUploadFile,
  dmsInitiateResumableUpload,
  _dmsLogActivity,
  _dmsIsSafeUrl,
  DMS_MIME_TYPES,
});

// ══════════════════════════════════════════════════════
// DEPARTMENTS — unique list from users.department
// ══════════════════════════════════════════════════════
app.get('/api/departments', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT DISTINCT department FROM users
       WHERE department IS NOT NULL AND department != ''
       ORDER BY department ASC`
    );
    const fromUsers = rows.map(r => r.department);
    const extras = ['YouTube', 'LinkedIn', 'MDO'];
    // Departments to hide from the daily-form dropdown (kept on user records).
    const hidden = new Set(['mis executive']);
    const merged = [...new Set([...fromUsers, ...extras])]
      .filter(d => !hidden.has(String(d).trim().toLowerCase()))
      .sort((a,b) => a.localeCompare(b));
    res.json(merged);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// DAILY TASK — submit, list, and check today's status
// ══════════════════════════════════════════════════════

// Check if current user already submitted for given date (default today)
app.get('/api/daily-tasks/status', requireAuth, async (req, res) => {
  try {
    const date = req.query.date || new Date().toISOString().split('T')[0];
    const [[{ cnt }]] = await db.query(
      'SELECT COUNT(*) AS cnt FROM daily_tasks WHERE user_id=? AND entry_date=?',
      [req.session.userId, date]
    );
    res.json({ submitted: cnt > 0, date });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Get current user's own past entries (read-only)
app.get('/api/daily-tasks/mine', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT id, DATE_FORMAT(entry_date,'%Y-%m-%d') AS entry_date,
              client_name, department, description, duration_min, created_at
       FROM daily_tasks WHERE user_id=?
       ORDER BY entry_date DESC, id DESC LIMIT 200`,
      [req.session.userId]
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Submit daily task — multiple rows in single call
app.post('/api/daily-tasks', requireAuth, async (req, res) => {
  try {
    const { entry_date, rows } = req.body;
    if (!entry_date || !Array.isArray(rows) || !rows.length) {
      return res.status(400).json({ error: 'Date and at least 1 row required' });
    }

    // Date restriction: only today or yesterday
    const today = new Date(); today.setHours(0,0,0,0);
    const yesterday = new Date(today); yesterday.setDate(today.getDate() - 1);
    const todayStr = today.toISOString().split('T')[0];
    const yesterdayStr = yesterday.toISOString().split('T')[0];
    if (entry_date !== todayStr && entry_date !== yesterdayStr) {
      return res.status(400).json({ error: 'Only today or yesterday entries are allowed' });
    }

    // Validate each row
    const cleanRows = [];
    for (const r of rows) {
      const client = (r.client_name || '').trim();
      const dept = (r.department || '').trim();
      const desc = (r.description || '').trim();
      const dur = parseInt(r.duration_min) || 0;
      if (!client || !desc || dur <= 0) {
        return res.status(400).json({ error: 'Each row needs client, description, and duration > 0' });
      }
      cleanRows.push([req.session.userId, entry_date, client, dept, desc, dur]);
    }

    // Lock check — already submitted for this date?
    const [[{ cnt }]] = await db.query(
      'SELECT COUNT(*) AS cnt FROM daily_tasks WHERE user_id=? AND entry_date=?',
      [req.session.userId, entry_date]
    );
    if (cnt > 0) {
      return res.status(400).json({ error: 'You have already submitted for this date. Editing is not allowed.' });
    }

    // Bulk insert
    await db.query(
      `INSERT INTO daily_tasks (user_id, entry_date, client_name, department, description, duration_min) VALUES ?`,
      [cleanRows]
    );

    // Confirmation now goes by EMAIL only — the WhatsApp DM has been retired.
    // Email confirmation (to the submitter's notification/login email).
    const target = await getNotifyTarget(req.session.userId);
    if (target) {
      const esc = s => String(s||'').replace(/[&<>]/g, ch => ({ '&':'&amp;','<':'&lt;','>':'&gt;' }[ch]));
      const rowsHtml = cleanRows.map(r =>
        `<tr><td style="padding:5px 9px;border:1px solid #e2e8f0">${esc(r[2])}</td><td style="padding:5px 9px;border:1px solid #e2e8f0">${esc(r[4])}</td><td style="padding:5px 9px;border:1px solid #e2e8f0;text-align:right;white-space:nowrap">${r[5]} min</td></tr>`
      ).join('');
      const html = `<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111;line-height:1.6">
        <p>✨ Hello ${esc(target.name || '')},<br>
        Thank you for submitting your daily task ✔️<br>
        Your response for the date <strong>${esc(entry_date)}</strong> has been successfully recorded 📄✨ — ${cleanRows.length} ${cleanRows.length === 1 ? 'entry' : 'entries'}.</p>
        <table style="border-collapse:collapse;font-size:13px;margin-top:4px">
          <thead><tr>
            <th style="padding:5px 9px;border:1px solid #e2e8f0;text-align:left;background:#f8fafc">Client</th>
            <th style="padding:5px 9px;border:1px solid #e2e8f0;text-align:left;background:#f8fafc">Task</th>
            <th style="padding:5px 9px;border:1px solid #e2e8f0;background:#f8fafc">Duration</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
        <p style="margin-top:14px">— E-Marketing Task Manager</p>
      </div>`;
      sendMail(target.email, `Daily Report Submitted — ${entry_date}`, html).catch(e => console.error('daily report email err:', e.message));
    }

    res.json({ success: true, count: cleanRows.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// COMPLIANCE — Last 7 days grid
// Scope: admin sees everyone; hod/pc see their own department; plain user sees only self.
// ══════════════════════════════════════════════════════
async function getComplianceScope(req) {
  const role = req.session.role;
  const uid = req.session.userId;
  if (role === 'admin') return { clause: '', params: [] };
  if (role === 'hod' || role === 'pc') {
    const [[me]] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
    return { clause: 'AND department=?', params: [me?.department || '\0'] };
  }
  return { clause: 'AND id=?', params: [uid] };
}

app.get('/api/compliance/last7', requireAuth, requireComplianceViewer, async (req, res) => {
  try {
    // Last 7 days inclusive of today
    const dates = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      dates.push(d.toISOString().split('T')[0]);
    }

    const scope = await getComplianceScope(req);
    // All users with week_off / extra_off so we can mark off-days
    const [users] = await db.query(
      `SELECT id, name, email, role, department,
              COALESCE(week_off,'') AS week_off,
              COALESCE(extra_off,'') AS extra_off,
              DATE_FORMAT(joining_date,'%Y-%m-%d') AS joining_date
       FROM users
       WHERE role IN ('admin','hod','pc','user') ${scope.clause}
       ORDER BY name ASC`,
      scope.params
    );

    // All filled (user_id, date) pairs in this range
    const [filled] = await db.query(
      `SELECT user_id, DATE_FORMAT(entry_date,'%Y-%m-%d') AS d
       FROM daily_tasks
       WHERE entry_date BETWEEN ? AND ?
       GROUP BY user_id, entry_date`,
      [dates[0], dates[dates.length - 1]]
    );

    // Build lookup: { userId: Set(dates) }
    const filledMap = {};
    for (const f of filled) {
      if (!filledMap[f.user_id]) filledMap[f.user_id] = new Set();
      filledMap[f.user_id].add(f.d);
    }

    // Full-day approved/pending leave requests overlapping this range — shown as
    // "A" (Absent) instead of a missed ✗, since the user wasn't expected to fill in.
    const [leaveRows] = await db.query(
      `SELECT user_id, dates_json, DATE_FORMAT(from_date,'%Y-%m-%d') AS from_date, DATE_FORMAT(to_date,'%Y-%m-%d') AS to_date
       FROM leave_requests
       WHERE status <> 'rejected' AND leave_type='full_day' AND from_date <= ? AND to_date >= ?`,
      [dates[dates.length - 1], dates[0]]
    );
    const leaveMap = {};
    for (const lr of leaveRows) {
      if (!leaveMap[lr.user_id]) leaveMap[lr.user_id] = new Set();
      let leaveDates = null;
      if (lr.dates_json) { try { leaveDates = JSON.parse(lr.dates_json).map(x => x.date); } catch { leaveDates = null; } }
      if (leaveDates) {
        leaveDates.forEach(d => leaveMap[lr.user_id].add(d));
      } else {
        for (const d of dates) { if (d >= lr.from_date && d <= lr.to_date) leaveMap[lr.user_id].add(d); }
      }
    }

    const holidaysSet = await loadHolidaysSet();

    // Build grid — mark off-days so UI doesn't count them as missed
    const grid = users.map(u => ({
      id: u.id,
      name: u.name,
      email: u.email,
      role: u.role,
      department: u.department || '—',
      status: dates.map(d => ({
        date: d,
        filled: filledMap[u.id]?.has(d) || false,
        off: isUserOffOn(u, d, holidaysSet),
        preJoin: !!(u.joining_date && d < u.joining_date),
        isHoliday: holidaysSet.has(d),
        onLeave: leaveMap[u.id]?.has(d) || false
      }))
    }));

    res.json({ dates, users: grid, holidays: [...holidaysSet] });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Returns true if req's user is allowed to view targetId's compliance/Employee-360 data:
// admin → anyone; hod/pc → same department; plain user → only self.
async function canViewComplianceEmployee(req, targetId) {
  const role = req.session.role;
  const uid = req.session.userId;
  if (role === 'admin') return true;
  if (Number(targetId) === Number(uid)) return true;
  if (role === 'hod' || role === 'pc') {
    const [[me]] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
    const [[target]] = await db.query('SELECT department FROM users WHERE id=?', [targetId]);
    return !!me?.department && me.department === target?.department;
  }
  return false;
}

// Employee 360 — everything about one employee in one place for increment review:
// delegation + checklist task stats, daily-report compliance, handled clients
// (active/inactive + activity in window), and meetings. Window defaults to the
// current month (IST); ?from=YYYY-MM-DD&to=YYYY-MM-DD widens it.
app.get('/api/compliance/employee/:id', requireAuth, requireComplianceViewer, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid employee id' });
    if (!await canViewComplianceEmployee(req, id)) return res.status(403).json({ error: 'Not allowed' });

    // Default window — current month (IST)
    const ist = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
    const yy = ist.getUTCFullYear(), mm = ist.getUTCMonth();
    const defaultFrom = `${yy}-${String(mm+1).padStart(2,'0')}-01`;
    const lastDay = new Date(Date.UTC(yy, mm+1, 0)).getUTCDate();
    const defaultTo = `${yy}-${String(mm+1).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;
    const isDate = v => /^\d{4}-\d{2}-\d{2}$/.test(v || '');
    const from = isDate(req.query.from) ? req.query.from : defaultFrom;
    const to   = isDate(req.query.to)   ? req.query.to   : defaultTo;

    const [[user]] = await db.query(
      `SELECT id, name, email, role, COALESCE(department,'—') AS department,
              COALESCE(week_off,'') AS week_off, COALESCE(extra_off,'') AS extra_off
       FROM users WHERE id=?`, [id]);
    if (!user) return res.status(404).json({ error: 'Employee not found' });

    const N = v => Number(v) || 0;

    // ── Delegation + checklist task stats (by due_date in window) ──
    const [[del]] = await db.query(
      `SELECT COUNT(*) AS total,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='revised'   THEN 1 ELSE 0 END) AS revised,
        SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM delegation_tasks WHERE assigned_to=? AND due_date BETWEEN ? AND ?`, [id, from, to]);
    const [[chl]] = await db.query(
      `SELECT COUNT(*) AS total,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM checklist_tasks WHERE assigned_to=? AND due_date BETWEEN ? AND ?`, [id, from, to]);
    const delegation = { total: N(del.total), pending: N(del.pending), completed: N(del.completed), revised: N(del.revised), overdue: N(del.overdue) };
    const checklist  = { total: N(chl.total), pending: N(chl.pending), completed: N(chl.completed), revised: 0, overdue: N(chl.overdue) };

    // ── Daily-report compliance ──
    const [[dr]] = await db.query(
      `SELECT COUNT(*) AS entries, COALESCE(SUM(duration_min),0) AS minutes,
              COUNT(DISTINCT entry_date) AS days_filled
       FROM daily_tasks WHERE user_id=? AND entry_date BETWEEN ? AND ?`, [id, from, to]);
    const holidaysSet = await loadHolidaysSet();
    let workingDays = 0;
    {
      let cur = new Date(from + 'T00:00:00Z');
      const endU = new Date(to + 'T00:00:00Z');
      let guard = 0;
      while (cur <= endU && guard++ < 1000) {
        const ds = cur.toISOString().split('T')[0];
        const off = cur.getUTCDay() === 0           // Sunday
                 || isLastSaturdayOfMonth(ds)        // company off Saturday
                 || isUserOffOn(user, ds, holidaysSet); // holidays
        if (!off) workingDays++;
        cur.setUTCDate(cur.getUTCDate() + 1);
      }
    }
    const daysFilled = N(dr.days_filled);
    const dailyReport = {
      entries: N(dr.entries), minutes: N(dr.minutes), hours: Math.round(N(dr.minutes) / 6) / 10,
      daysFilled, workingDays,
      fillPct: workingDays > 0 ? Math.round((daysFilled / workingDays) * 100) : 0
    };
    const [recentEntries] = await db.query(
      `SELECT id, DATE_FORMAT(entry_date,'%Y-%m-%d') AS entry_date,
              client_name, COALESCE(department,'') AS department, description, duration_min
       FROM daily_tasks WHERE user_id=? AND entry_date BETWEEN ? AND ?
       -- No LIMIT. It used to be 20, which quietly cut a five-week range down
       -- to about eight days with nothing on screen saying so — the range
       -- filter looked broken when it was the cap doing it. The date range is
       -- what bounds this query, the same as every other one on this page, and
       -- the UI now groups by day so the length is manageable.
       ORDER BY entry_date DESC, id DESC`, [id, from, to]);

    // ── Clients handled by this employee (handler) + activity in window ──
    const [clientRows] = await db.query(
      `SELECT id, name, COALESCE(is_active,1) AS is_active, logo_url
       FROM clients WHERE handler_id=? ORDER BY COALESCE(is_active,1) DESC, name ASC`, [id]);
    if (clientRows.length) {
      const ids = clientRows.map(c => c.id);
      const ph = ids.map(() => '?').join(',');
      const [dc] = await db.query(
        `SELECT client_id, COUNT(*) AS total, SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending
         FROM delegation_tasks WHERE client_id IN (${ph}) AND due_date BETWEEN ? AND ? GROUP BY client_id`, [...ids, from, to]);
      const [cc] = await db.query(
        `SELECT client_id, COUNT(*) AS total, SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending
         FROM checklist_tasks WHERE client_id IN (${ph}) AND due_date BETWEEN ? AND ? GROUP BY client_id`, [...ids, from, to]);
      const [mc] = await db.query(
        `SELECT client_id, COUNT(*) AS total FROM meetings
         WHERE client_id IN (${ph}) AND meeting_date BETWEEN ? AND ? GROUP BY client_id`, [...ids, from, to]);
      const dMap = Object.fromEntries(dc.map(r => [r.client_id, r]));
      const cMap = Object.fromEntries(cc.map(r => [r.client_id, r]));
      const mMap = Object.fromEntries(mc.map(r => [r.client_id, r]));
      for (const c of clientRows) {
        c.is_active = N(c.is_active);
        const d = dMap[c.id] || {}, k = cMap[c.id] || {}, m = mMap[c.id] || {};
        c.tasks = N(d.total) + N(k.total);
        c.pending = N(d.pending) + N(k.pending);
        c.meetings = N(m.total);
        c.activity = c.tasks + c.meetings; // any touch in window
      }
    }
    const clients = {
      total: clientRows.length,
      active: clientRows.filter(c => c.is_active).length,
      inactive: clientRows.filter(c => !c.is_active).length,
      list: clientRows
    };

    // ── Meetings (organized + attended) ──
    const [[mo]] = await db.query(
      `SELECT COUNT(*) AS total,
        SUM(CASE WHEN status='scheduled' THEN 1 ELSE 0 END) AS scheduled,
        SUM(CASE WHEN status='done'      THEN 1 ELSE 0 END) AS done,
        SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled
       FROM meetings WHERE organizer_id=? AND meeting_date BETWEEN ? AND ?`, [id, from, to]);
    const [[ma]] = await db.query(
      `SELECT COUNT(DISTINCT m.id) AS total
       FROM meetings m JOIN meeting_attendees mt ON mt.meeting_id=m.id
       WHERE mt.user_id=? AND m.meeting_date BETWEEN ? AND ?`, [id, from, to]);
    const [mtgRecent] = await db.query(
      `SELECT m.id, m.title, m.status,
              DATE_FORMAT(m.meeting_date,'%Y-%m-%d') AS meeting_date,
              TIME_FORMAT(m.start_time,'%H:%i') AS start_time,
              c.name AS client_name,
              CASE WHEN m.organizer_id=? THEN 'Organizer' ELSE 'Attendee' END AS my_role
       FROM meetings m LEFT JOIN clients c ON m.client_id=c.id
       WHERE (m.organizer_id=? OR EXISTS (SELECT 1 FROM meeting_attendees mt WHERE mt.meeting_id=m.id AND mt.user_id=?))
         AND m.meeting_date BETWEEN ? AND ?
       ORDER BY m.meeting_date DESC, m.start_time DESC LIMIT 20`, [id, id, id, from, to]);
    const meetings = {
      organized: { total: N(mo.total), scheduled: N(mo.scheduled), done: N(mo.done), cancelled: N(mo.cancelled) },
      attended: N(ma.total),
      recent: mtgRecent
    };

    // ── Scorecard — each section scored 0-100 (higher = better), null if N/A.
    // Average = equal-weight mean of available sections. Final = weighted, where
    // delegation/checklist/daily-report carry the most weight (re-normalised over
    // whatever sections actually apply to this employee).
    const clamp = n => Math.max(0, Math.min(100, n));
    const r1 = n => Math.round(n * 10) / 10;
    const cat = {
      delegation: delegation.total > 0
        ? r1(clamp((delegation.completed / delegation.total) * 100 - (delegation.overdue / delegation.total) * 30 - (delegation.revised / delegation.total) * 15))
        : null,
      checklist: checklist.total > 0
        ? r1(clamp((checklist.completed / checklist.total) * 100 - (checklist.overdue / checklist.total) * 30))
        : null,
      dailyReport: dailyReport.workingDays > 0 ? r1(clamp(dailyReport.fillPct)) : null,
      meetings: meetings.organized.total > 0 ? r1(clamp((meetings.organized.done / meetings.organized.total) * 100)) : null,
      clients: clients.total > 0 ? r1(clamp((clients.active / clients.total) * 100)) : null
    };
    const weights = { delegation: 30, checklist: 25, dailyReport: 20, meetings: 15, clients: 10 };
    const present = Object.keys(weights).filter(k => cat[k] !== null);
    const average = present.length ? r1(present.reduce((a, k) => a + cat[k], 0) / present.length) : null;
    let wSum = 0, wTot = 0;
    for (const k of present) { wSum += cat[k] * weights[k]; wTot += weights[k]; }
    const final = wTot ? r1(wSum / wTot) : null;
    const grade = final == null ? 'N/A'
      : final >= 85 ? 'Excellent' : final >= 70 ? 'Good' : final >= 50 ? 'Average' : 'Needs Improvement';
    const scores = { categories: cat, weights, average, final, grade };

    // ── Weekly Planned (committed) vs Actual (achieved) scoring ──
    // Planned = score the employee committed in their Monday "My Week" check-in.
    // Actual  = score auto-computed from that week's task performance (scoreFor).
    // regression = committed score worse than the PREVIOUS week's achieved score.
    const weekly = [];
    {
      const firstMon = istMondayOf(new Date(from + 'T00:00:00Z'));
      const mondays = [];
      for (let m = firstMon; m <= to; m = addDays(m, 7)) mondays.push(m);
      // Include one week before the first as the regression baseline.
      const baselineMon = addDays(firstMon, -7);
      const allMons = [baselineMon, ...mondays];
      const rangeStart = baselineMon;
      const rangeEnd = mondays.length ? addDays(mondays[mondays.length - 1], 6) : addDays(baselineMon, 6);
      const [planRows] = await db.query(
        `SELECT DATE_FORMAT(start_date,'%Y-%m-%d') AS mon, user_committed_score
           FROM week_plans WHERE employee_id=? AND start_date IN (${allMons.map(()=>'?').join(',')})`,
        [id, ...allMons]);
      const committedBy = {};
      for (const r of planRows) committedBy[r.mon] = r.user_committed_score == null ? null : Number(r.user_committed_score);
      // Achieved score per week — bucket tasks by their Monday (WEEKDAY: Mon=0), 2 grouped queries.
      const wkExpr = `DATE_FORMAT(DATE_SUB(due_date, INTERVAL WEEKDAY(due_date) DAY),'%Y-%m-%d')`;
      const [delWk] = await db.query(
        `SELECT ${wkExpr} AS wk, COUNT(*) AS total,
          SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
          SUM(CASE WHEN status='revised' THEN 1 ELSE 0 END) AS revised,
          SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
         FROM delegation_tasks WHERE assigned_to=? AND due_date BETWEEN ? AND ? GROUP BY wk`, [id, rangeStart, rangeEnd]);
      const [chlWk] = await db.query(
        `SELECT ${wkExpr} AS wk, COUNT(*) AS total,
          SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
          SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
         FROM checklist_tasks WHERE assigned_to=? AND due_date BETWEEN ? AND ? GROUP BY wk`, [id, rangeStart, rangeEnd]);
      const agg = {};
      const bump = (wk, t, p, o, r) => { const a = agg[wk] || (agg[wk] = { total:0, pending:0, overdue:0, revised:0 }); a.total+=t; a.pending+=p; a.overdue+=o; a.revised+=r; };
      for (const r of delWk) bump(r.wk, N(r.total), N(r.pending), N(r.overdue), N(r.revised));
      for (const r of chlWk) bump(r.wk, N(r.total), N(r.pending), N(r.overdue), 0);
      const achievedBy = {};
      for (const wkMon of allMons) {
        const a = agg[wkMon];
        achievedBy[wkMon] = a ? scoreFor(a.total, a.pending, a.overdue, a.revised) : null;
      }
      for (const wkMon of mondays) {
        const committed = wkMon in committedBy ? committedBy[wkMon] : null;
        const achieved = achievedBy[wkMon];
        const prevAchieved = achievedBy[addDays(wkMon, -7)];
        const wAgg = agg[wkMon] || { total: 0, pending: 0, revised: 0 };
        weekly.push({
          weekStart: wkMon, weekEnd: addDays(wkMon, 6),
          committed, achieved,
          prevAchieved: prevAchieved == null ? null : prevAchieved,
          gap: (committed !== null && achieved !== null) ? Math.round((achieved - committed) * 10) / 10 : null,
          regression: committed !== null && prevAchieved != null && committed < prevAchieved,
          taskTotal: wAgg.total,
          taskPending: wAgg.pending,
          taskCompleted: Math.max(0, wAgg.total - wAgg.pending - (wAgg.revised || 0))
        });
      }
    }

    res.json({
      range: { from, to },
      user: { id: user.id, name: user.name, email: user.email, role: user.role, department: user.department },
      delegation, checklist, dailyReport, recentEntries, clients, meetings, scores, weekly
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Week-level task detail for Employee 360 weekly table drill-down
app.get('/api/compliance/employee/:id/week-tasks', requireAuth, requireComplianceViewer, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid employee id' });
    if (!await canViewComplianceEmployee(req, id)) return res.status(403).json({ error: 'Not allowed' });
    const isDate = v => /^\d{4}-\d{2}-\d{2}$/.test(v || '');
    const from = isDate(req.query.from) ? req.query.from : null;
    const to   = isDate(req.query.to)   ? req.query.to   : null;
    if (!from || !to) return res.status(400).json({ error: 'from and to required' });
    const [delTasks] = await db.query(
      `SELECT dt.id, dt.description AS title, dt.status, DATE_FORMAT(dt.due_date,'%Y-%m-%d') AS due_date,
              COALESCE(c.name,'—') AS client_name, 'delegation' AS task_type,
              COALESCE(u2.name,'—') AS assigned_by
       FROM delegation_tasks dt
       LEFT JOIN clients c ON c.id = dt.client_id
       LEFT JOIN users u2 ON u2.id = dt.assigned_by
       WHERE dt.assigned_to=? AND dt.due_date BETWEEN ? AND ?
       ORDER BY dt.due_date, dt.id`, [id, from, to]);
    const [chlTasks] = await db.query(
      `SELECT ct.id, ct.description AS title, ct.status, DATE_FORMAT(ct.due_date,'%Y-%m-%d') AS due_date,
              COALESCE(c.name,'—') AS client_name, 'checklist' AS task_type,
              COALESCE(u2.name,'—') AS assigned_by
       FROM checklist_tasks ct
       LEFT JOIN clients c ON c.id = ct.client_id
       LEFT JOIN users u2 ON u2.id = ct.assigned_by
       WHERE ct.assigned_to=? AND ct.due_date BETWEEN ? AND ?
       ORDER BY ct.due_date, ct.id`, [id, from, to]);
    res.json([...delTasks, ...chlTasks].sort((a,b) => a.due_date < b.due_date ? -1 : 1));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Admin view — all daily task entries with filters
app.get('/api/daily-tasks/all', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { from, to, userId } = req.query;
    let where = '1=1';
    const params = [];
    if (from) { where += ' AND dt.entry_date >= ?'; params.push(from); }
    if (to)   { where += ' AND dt.entry_date <= ?'; params.push(to); }
    if (userId) { where += ' AND dt.user_id = ?'; params.push(userId); }

    const [rows] = await db.query(
      `SELECT dt.id, DATE_FORMAT(dt.entry_date,'%Y-%m-%d') AS entry_date,
              dt.client_name, dt.department, dt.description, dt.duration_min,
              u.name AS doer_name, u.email AS doer_email
       FROM daily_tasks dt
       JOIN users u ON dt.user_id = u.id
       WHERE ${where}
       ORDER BY dt.entry_date DESC, dt.id DESC
       LIMIT 1000`,
      params
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Monthly report — summary + day-wise entries (admin only)
app.get('/api/daily-tasks/report', requireAuth, requireAdmin, async (req, res) => {
  try {
    // Explicit from/to win over month; month is the fallback.
    const isDate = v => /^\d{4}-\d{2}-\d{2}$/.test(v);
    let fromDate, toDate;
    if (req.query.from && req.query.to && isDate(req.query.from) && isDate(req.query.to)) {
      fromDate = req.query.from;
      toDate   = req.query.to;
    } else {
      const now = new Date();
      const month = req.query.month || `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
      if (!/^\d{4}-\d{2}$/.test(month)) {
        return res.status(400).json({ error: 'Invalid month format. Use YYYY-MM' });
      }
      const [year, mm] = month.split('-').map(Number);
      fromDate = `${year}-${String(mm).padStart(2,'0')}-01`;
      const lastDay = new Date(year, mm, 0).getDate();
      toDate = `${year}-${String(mm).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;
    }

    const filterParts = ['dt.entry_date BETWEEN ? AND ?'];
    const params = [fromDate, toDate];
    if (req.query.user_id) { filterParts.push('dt.user_id = ?'); params.push(req.query.user_id); }
    if (req.query.client)  { filterParts.push('dt.client_name = ?'); params.push(req.query.client); }

    const [rows] = await db.query(
      `SELECT dt.id, DATE_FORMAT(dt.entry_date,'%Y-%m-%d') AS entry_date,
              dt.client_name, dt.department, dt.description, dt.duration_min,
              dt.user_id, u.name AS doer_name, u.email AS doer_email,
              COALESCE(u.department, '') AS doer_department
       FROM daily_tasks dt
       JOIN users u ON dt.user_id = u.id
       WHERE ${filterParts.join(' AND ')}
       ORDER BY dt.entry_date ASC, u.name ASC, dt.id ASC`,
      params
    );

    // Per-user totals
    const userTotals = {};
    for (const r of rows) {
      if (!userTotals[r.user_id]) {
        userTotals[r.user_id] = {
          user_id: r.user_id, name: r.doer_name, email: r.doer_email,
          department: r.doer_department, total_minutes: 0, total_tasks: 0,
          days_filled: new Set()
        };
      }
      userTotals[r.user_id].total_minutes += r.duration_min;
      userTotals[r.user_id].total_tasks += 1;
      userTotals[r.user_id].days_filled.add(r.entry_date);
    }
    // Convert Set to count
    const summary = Object.values(userTotals)
      .map(u => ({ ...u, days_filled: u.days_filled.size }))
      .sort((a, b) => b.total_minutes - a.total_minutes);

    res.json({
      month: req.query.month || fromDate.slice(0, 7),
      from: fromDate, to: toDate,
      total_entries: rows.length,
      total_minutes: rows.reduce((a, b) => a + b.duration_min, 0),
      summary,
      entries: rows
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// LEAVE TRACKER
// Flow: user → HOD of same dept; hod/pc → admin; admin → self (auto-approved)
// ══════════════════════════════════════════════════════
async function resolveLeaveApprover(userId) {
  // Uses `user_role` (org hierarchy), NOT `role` (app permissions). An IT employee
  // may have role='admin' for full app access but user_role='user' so leaves still
  // route to their HOD.
  const [rows] = await db.query(
    `SELECT id, COALESCE(user_role, role) AS user_role, department
     FROM users WHERE id=?`, [userId]);
  const me = rows[0];
  if (!me) return null;
  if (me.user_role === 'admin') {
    // Admin's leave → department HOD if one exists (covers employees whose
    // user_role got set to 'admin' for app-permission reasons but who still
    // report to a real HOD), else another admin, else self.
    if (me.department) {
      const [hods] = await db.query(
        `SELECT id FROM users
         WHERE COALESCE(user_role, role)='hod' AND department=? AND id<>? ORDER BY id ASC LIMIT 1`,
        [me.department, me.id]);
      if (hods[0]) return hods[0].id;
    }
    const [adm] = await db.query(
      `SELECT id FROM users
       WHERE COALESCE(user_role, role)='admin' AND id<>? ORDER BY id ASC LIMIT 1`,
      [me.id]);
    if (adm[0]) return adm[0].id;
    return me.id;
  }
  if (me.user_role === 'hod' || me.user_role === 'pc') {
    const [adm] = await db.query(
      `SELECT id FROM users WHERE COALESCE(user_role, role)='admin' ORDER BY id ASC LIMIT 1`);
    return adm[0]?.id || null;
  }
  // user → HOD of same department; fallback to admin
  if (me.department) {
    const [hods] = await db.query(
      `SELECT id FROM users
       WHERE COALESCE(user_role, role)='hod' AND department=? ORDER BY id ASC LIMIT 1`,
      [me.department]);
    if (hods[0]) return hods[0].id;
  }
  const [adm] = await db.query(
    `SELECT id FROM users WHERE COALESCE(user_role, role)='admin' ORDER BY id ASC LIMIT 1`);
  return adm[0]?.id || null;
}

// Simran Gurnani — oversees leave approvals org-wide, so she sees every
// pending request in her Task Manager in addition to the assigned HOD.
const LEAVE_OVERSEER_ID = 6;

// List leaves — scope based on role + ?scope= filter
//   scope=mine       → only my requests (default for users)
//   scope=approvals  → requests awaiting my approval (hod/admin/pc)
//   scope=team       → all in my visibility (hod = dept, admin = all)
app.get('/api/leaves', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const role = req.session.role;
    // Anything that is not one of the three known scopes falls back to 'mine'.
    // The branches below are an if / else-if / else-if chain with NO final else,
    // so an unrecognised value — ?scope=all, a typo, a repeated query param that
    // arrives as an array — left `where` at '1=1' and the SELECT returned every
    // leave request in the company: name, email, department and the free-text
    // reason, to any authenticated session, client logins included. Every real
    // caller sends one of these three literals (app.html 13643 / 15895 / 16487),
    // so narrowing costs nothing.
    const SCOPES = ['mine', 'approvals', 'team'];
    const scope = SCOPES.includes(req.query.scope) ? req.query.scope : 'mine';
    const status = req.query.status || '';

    let where = '1=1', params = [];
    if (scope === 'mine') {
      where += ' AND lr.user_id=?'; params.push(uid);
    } else if (scope === 'approvals') {
      if (uid === LEAVE_OVERSEER_ID) {
        where += ' AND lr.user_id<>?'; params.push(uid);
      } else {
        // If this user is an HOD, show leaves for ALL HODs in same department
        const [[meInfo]] = await db.query(
          'SELECT department, COALESCE(user_role, role) AS user_role FROM users WHERE id=?', [uid]);
        if (meInfo?.user_role === 'hod' && meInfo?.department) {
          const [deptHods] = await db.query(
            `SELECT id FROM users WHERE COALESCE(user_role, role)='hod' AND department=?`,
            [meInfo.department]);
          const hodIds = deptHods.map(h => h.id);
          where += ` AND lr.approver_id IN (${hodIds.map(()=>'?').join(',')}) AND lr.user_id<>?`;
          params.push(...hodIds, uid);
        } else {
          where += ' AND lr.approver_id=? AND lr.user_id<>?'; params.push(uid, uid);
        }
      }
    } else if (scope === 'team') {
      // Pull current user once so we can apply leave-viewer override and HOD dept-scoping.
      const [[me]] = await db.query('SELECT name, role, department, extra_access FROM users WHERE id=?', [uid]);
      if (role === 'admin' || isLeaveReportViewer(me)) {
        // no filter — all
      } else if (role === 'hod') {
        if (me?.department) {
          where += ' AND u.department=?'; params.push(me.department);
        } else {
          where += ' AND lr.user_id=?'; params.push(uid);
        }
      } else if (role === 'pc') {
        // PC keeps its existing org-wide team view.
      } else {
        where += ' AND lr.user_id=?'; params.push(uid);
      }
    }
    if (status) { where += ' AND lr.status=?'; params.push(status); }
    if (req.query.user_id) { where += ' AND lr.user_id=?'; params.push(req.query.user_id); }
    if (req.query.from)    { where += ' AND lr.to_date >= ?'; params.push(req.query.from); }
    if (req.query.to)      { where += ' AND lr.from_date <= ?'; params.push(req.query.to); }

    const [rows] = await db.query(`
      SELECT lr.id, lr.user_id, lr.leave_type, lr.status, lr.reason,
        lr.approver_id, lr.approver_note, lr.dates_json,
        DATE_FORMAT(lr.from_date,'%Y-%m-%d') AS from_date,
        DATE_FORMAT(lr.to_date,'%Y-%m-%d')   AS to_date,
        DATE_FORMAT(lr.created_at,'%Y-%m-%d %H:%i:%s') AS created_at,
        DATE_FORMAT(lr.decided_at,'%Y-%m-%d %H:%i:%s') AS decided_at,
        u.name AS user_name, u.email AS user_email, u.department AS user_department,
        ap.name AS approver_name,
        (SELECT GROUP_CONCAT(hod.name ORDER BY hod.name SEPARATOR ', ')
         FROM users hod
         WHERE COALESCE(hod.user_role, hod.role)='hod'
           AND hod.department=u.department
           AND u.department IS NOT NULL AND u.department<>'') AS dept_hod_names
      FROM leave_requests lr
      JOIN users u ON lr.user_id=u.id
      LEFT JOIN users ap ON lr.approver_id=ap.id
      WHERE ${where}
      ORDER BY lr.created_at DESC
      LIMIT 500
    `, params);
    // Parse dates_json into structured array for client
    for (const r of rows) {
      if (r.dates_json) {
        try { r.dates = JSON.parse(r.dates_json); }
        catch { r.dates = null; }
      } else {
        // Legacy rows (pre dates_json): fall back to from/to range
        r.dates = [{ date: r.from_date }];
      }
      delete r.dates_json;
    }
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Pending approvals count — for badge
// Returns names of all HODs who will approve current user's leave
app.get('/api/leaves/my-approvers', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const [[me]] = await db.query(
      'SELECT department, COALESCE(user_role, role) AS user_role FROM users WHERE id=?', [uid]);
    if (!me) return res.json({ names: '' });
    if (me.user_role === 'admin') return res.json({ names: 'Another Admin' });
    if (me.user_role === 'hod' || me.user_role === 'pc') return res.json({ names: 'Admin' });
    if (me.department) {
      const [hods] = await db.query(
        `SELECT name FROM users WHERE COALESCE(user_role, role)='hod' AND department=? ORDER BY name`,
        [me.department]);
      return res.json({ names: hods.map(h => h.name).join(', ') || 'HOD' });
    }
    res.json({ names: 'HOD' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leaves/pending-count', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    let cnt = 0;
    if (uid === LEAVE_OVERSEER_ID) {
      const [[r]] = await db.query(
        "SELECT COUNT(*) AS cnt FROM leave_requests WHERE status='pending' AND user_id<>?",
        [uid]);
      return res.json({ count: r.cnt || 0 });
    }
    // Count pending leaves for all HODs in same department
    const [[meInfo]] = await db.query(
      'SELECT department, COALESCE(user_role, role) AS user_role FROM users WHERE id=?', [uid]);
    if (meInfo?.user_role === 'hod' && meInfo?.department) {
      const [deptHods] = await db.query(
        `SELECT id FROM users WHERE COALESCE(user_role, role)='hod' AND department=?`,
        [meInfo.department]);
      const hodIds = deptHods.map(h => h.id);
      const [[r]] = await db.query(
        `SELECT COUNT(*) AS cnt FROM leave_requests WHERE approver_id IN (${hodIds.map(()=>'?').join(',')}) AND status='pending' AND user_id<>?`,
        [...hodIds, uid]);
      cnt = r.cnt || 0;
    } else {
      const [[r]] = await db.query(
        "SELECT COUNT(*) AS cnt FROM leave_requests WHERE approver_id=? AND status='pending' AND user_id<>?",
        [uid, uid]);
      cnt = r.cnt || 0;
    }
    res.json({ count: cnt });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Apply for leave
// Upper bound on one Extra Working task description. Generous on purpose:
// the reason column is TEXT (65,535), and the point of the limit is to stop a
// runaway paste, not to shape what someone writes about their own work.
const EXTRA_WORK_DESC_MAX = 5000;

app.post('/api/leaves', requireAuth, async (req, res) => {
  try {
    const { leave_type, dates, reason } = req.body;
    const allowedTypes = ['full_day','half_day','work_from_home','extra_working'];
    if (!allowedTypes.includes(leave_type)) return res.status(400).json({ error: 'Invalid leave type' });
    if (!Array.isArray(dates) || !dates.length) return res.status(400).json({ error: 'Select at least one date' });
    // Extra working carries per-row task descriptions instead of a single reason
    if (leave_type !== 'extra_working' && (!reason || !reason.trim()))
      return res.status(400).json({ error: 'Reason required' });

    // Normalize + validate dates
    const dateRe = /^\d{4}-\d{2}-\d{2}$/;
    const seen = new Set();
    const cleanDates = [];
    for (const d of dates) {
      const date = (d && d.date) || d;
      if (!dateRe.test(date)) return res.status(400).json({ error: 'Invalid date format' });
      if (seen.has(date)) continue;
      seen.add(date);
      const item = { date };
      if (leave_type === 'extra_working') {
        if (Array.isArray(d && d.entries)) {
          // Client-wise work rows: [{client, department, description, hours, minutes}]
          // Two forms feed this: the Leave Tracker calendar sends `hours`, the Daily
          // Task page sends `minutes` (and a department). Minutes win when present,
          // but `hours` is ALWAYS stored so every existing reader — approval emails,
          // WhatsApp, Leave Tracker cards — keeps working untouched.
          const entries = [];
          let totalMin = 0;
          let anyMin = false;
          for (const e of d.entries) {
            const client = String((e && e.client) || '').trim().slice(0, 120);
            const department = String((e && e.department) || '').trim().slice(0, 120);
            // Stored in full. This used to be .slice(0, 500), which cut long
            // entries mid-word on the way in — the text never reached the
            // database and nothing said so, so the person who wrote it only
            // found out when they read their own approval row back. The list
            // view now shows the first 50 words with a "Read more", which is
            // where shortening belongs: in the display, not in the data.
            //
            // The remaining cap is a guard against a runaway paste, and it
            // REFUSES rather than trims — silent truncation is the bug being
            // fixed here, so it must not come back in a larger size.
            const description = String((e && e.description) || '').trim();
            if (description.length > EXTRA_WORK_DESC_MAX) {
              return res.status(400).json({
                error: `Description for ${e && e.client ? String(e.client).trim() : 'a client'} on ${date} is ${description.length} characters — the limit is ${EXTRA_WORK_DESC_MAX}. Please shorten it.`,
              });
            }
            const rawMin = e && e.minutes;
            const hasMin = rawMin !== undefined && rawMin !== null && rawMin !== '';
            const min = hasMin ? parseInt(rawMin, 10) : null;
            if (!client) return res.status(400).json({ error: `Client required for ${date}` });
            if (!description) return res.status(400).json({ error: `Task description required for ${client} on ${date}` });
            if (hasMin) {
              if (!Number.isFinite(min) || min <= 0 || min > 1440)
                return res.status(400).json({ error: `Time in minutes (1-1440) required for ${client} on ${date}` });
            }
            const h = hasMin ? Math.round((min / 60) * 100) / 100 : Number(e && e.hours);
            if (!hasMin && (!h || h <= 0 || h > 24))
              return res.status(400).json({ error: `Hours required (1-24) for ${client} on ${date}` });
            const entry = { client, description, hours: h };
            if (department) entry.department = department;
            if (hasMin) { entry.minutes = min; anyMin = true; }
            entries.push(entry);
            totalMin += hasMin ? min : Math.round(h * 60);
          }
          if (!entries.length) return res.status(400).json({ error: `Add at least one client row for ${date}` });
          if (totalMin > 1440) return res.status(400).json({ error: `Total hours exceed 24 for ${date}` });
          item.entries = entries;
          item.hours = Math.round((totalMin / 60) * 100) / 100;
          if (anyMin) item.minutes = totalMin;
        } else {
          // Legacy payload from a cached page: plain hours per date
          const h = Number(d && d.hours);
          if (!h || h <= 0 || h > 24) return res.status(400).json({ error: `Hours required (1-24) for ${date}` });
          item.hours = h;
        }
      }
      cleanDates.push(item);
    }
    cleanDates.sort((a,b) => a.date.localeCompare(b.date));
    const from_date = cleanDates[0].date;
    const to_date = cleanDates[cleanDates.length - 1].date;

    const uid = req.session.userId;

    // One Extra Working request per date. A rejected request frees the date up
    // again so the user can correct it and re-submit.
    if (leave_type === 'extra_working') {
      const [existing] = await db.query(
        `SELECT dates_json FROM leave_requests
          WHERE user_id=? AND leave_type='extra_working' AND status IN ('pending','approved')
            AND to_date >= ? AND from_date <= ?`,
        [uid, from_date, to_date]
      );
      const taken = new Set();
      for (const row of existing) {
        try {
          for (const d of JSON.parse(row.dates_json || '[]')) taken.add(String(d.date).slice(0, 10));
        } catch {}
      }
      const clash = cleanDates.find(d => taken.has(d.date));
      if (clash) return res.status(400).json({
        error: `Extra Working is already submitted for ${clash.date.split('-').reverse().join('-')}`
      });
    }

    const approverId = await resolveLeaveApprover(uid);

    const [r] = await db.query(
      `INSERT INTO leave_requests
       (user_id, leave_type, from_date, to_date, dates_json, reason, status, approver_id)
       VALUES (?,?,?,?,?,?,'pending',?)`,
      [uid, leave_type, from_date, to_date, JSON.stringify(cleanDates), (reason || '').trim(), approverId]
    );

    // Notify approver — email + WhatsApp (best-effort)
    if (approverId && approverId !== uid) {
      const typeLabel = ({full_day:'Full Day Leave',half_day:'Half Day Leave',work_from_home:'Work From Home',extra_working:'Extra Working'})[leave_type];
      const datesLine = cleanDates.map(d => leave_type === 'extra_working' ? `${d.date} (${d.hours}h)` : d.date).join(', ');
      const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [uid]);
      const esc = (s) => String(s || '').replace(/</g, '&lt;');
      const hasEntries = leave_type === 'extra_working' && cleanDates.some(d => Array.isArray(d.entries) && d.entries.length);
      const totalHours = leave_type === 'extra_working'
        ? Math.round(cleanDates.reduce((s, d) => s + (d.hours || 0), 0) * 100) / 100 : 0;
      // Rows logged from the Daily Task page carry minutes; show them in the unit
      // the submitter actually typed and fall back to hours for calendar rows.
      const fmtDur = (o) => (o && o.minutes) ? `${o.minutes} min` : `${(o && o.hours) || 0}h`;

      // Notify the assigned approver AND every other same-dept HOD — all the
      // same plain WhatsApp-style email (no HTML card). Personal WhatsApp retired.
      try {
        const daysWord = cleanDates.length === 1 ? '1 day' : `${cleanDates.length} days`;
        const datesPretty = cleanDates.map(d => {
          const dd = d.date.split('-').reverse().join('-');
          return leave_type === 'extra_working' ? `${dd} (${d.hours}h)` : dd;
        }).join(', ');
        const [[submitter]] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
        let recipients = [];
        if (submitter?.department) {
          const [allHods] = await db.query(
            `SELECT id, name, email, notification_email FROM users WHERE COALESCE(user_role, role)='hod' AND department=?`,
            [submitter.department]);
          recipients = allHods;
        }
        // Always include the assigned approver, even if not a dept HOD.
        if (approverId && !recipients.some(r => r.id === approverId)) {
          const [[apRow]] = await db.query('SELECT id, name, email, notification_email FROM users WHERE id=?', [approverId]);
          if (apRow) recipients.push(apRow);
        }
        const waHeading = ({
          extra_working: 'New Extra Working Request',
          work_from_home: 'New Work From Home Request',
          half_day: 'New Half Day Leave Request'
        })[leave_type] || 'New Leave Request';
        // Client-wise breakdown replaces the Dates/Reason lines for extra_working
        const waDetail = hasEntries
          ? `*Total:* ${totalHours}h\n\n` +
            cleanDates.map(d => {
              const dd = d.date.split('-').reverse().join('-');
              const lines = (d.entries || []).map(e =>
                `  • ${e.client}${e.department ? ` [${e.department}]` : ''} — ${e.description} (${fmtDur(e)})`).join('\n');
              return `*${dd} (${fmtDur(d)}):*${lines ? '\n' + lines : ''}`;
            }).join('\n')
          : `*Dates:* ${datesPretty}\n` +
            `*Reason:* ${reason}`;
        for (const hod of recipients) {
          const hodEmail = hod.notification_email || hod.email;
          if (!hodEmail) continue;
          const msg = `Hello ${hod.name || ''},\n\n🗓 *${waHeading}*\n\n` +
            `*Employee:* ${me?.name || ''}\n` +
            `*Type:* ${typeLabel}\n` +
            `*Duration:* ${daysWord}\n` +
            waDetail + `\n\n` +
            `Please approve / reject from the Approvals tab.\n\n— E-Marketing Task Manager`;
          sendMail(hodEmail, `${waHeading} — ${me?.name || ''}`, waTextToEmailHtml(msg)).catch(e => console.error('leave req email err:', e.message));
        }
      } catch (e) { console.error('leave req email lookup err:', e.message); }
    }

    res.json({ id: r.insertId, status: 'pending', approver_id: approverId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Approve / Reject leave — only the assigned approver (or admin) can act
app.put('/api/leaves/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const { action, note } = req.body; // action = 'approve' | 'reject'
    if (!['approve','reject'].includes(action)) return res.status(400).json({ error: 'Invalid action' });

    const [rows] = await db.query('SELECT * FROM leave_requests WHERE id=?', [id]);
    const lr = rows[0];
    if (!lr) return res.status(404).json({ error: 'Leave not found' });
    if (lr.status !== 'pending') return res.status(400).json({ error: 'Already decided' });

    const uid = req.session.userId;
    const role = req.session.role;
    // Allow: admin always, assigned approver, OR any HOD in same department as the assigned approver
    if (lr.approver_id !== uid && role !== 'admin') {
      const [[myInfo]] = await db.query(
        'SELECT department, COALESCE(user_role, role) AS user_role FROM users WHERE id=?', [uid]);
      const [[apInfo]] = await db.query(
        'SELECT department FROM users WHERE id=?', [lr.approver_id]);
      const samedept = myInfo?.user_role === 'hod' && myInfo?.department &&
                       apInfo?.department === myInfo.department;
      if (!samedept) return res.status(403).json({ error: 'Not authorized to act on this request' });
    }

    const newStatus = action === 'approve' ? 'approved' : 'rejected';
    await db.query(
      `UPDATE leave_requests
         SET status=?, approver_id=?, approver_note=?, decided_at=NOW()
       WHERE id=?`,
      [newStatus, uid, (note || '').trim() || null, id]
    );

    // Notify requester — email + WhatsApp
    const typeLabel = ({full_day:'Full Day Leave',half_day:'Half Day Leave',work_from_home:'Work From Home',extra_working:'Extra Working'})[lr.leave_type];
    let datesLine = '';
    try {
      const arr = lr.dates_json ? JSON.parse(lr.dates_json) : null;
      if (arr && arr.length) {
        datesLine = arr.map(d => lr.leave_type === 'extra_working' ? `${d.date.split('-').reverse().join('-')} (${d.hours}h)` : d.date.split('-').reverse().join('-')).join(', ');
      }
    } catch {}
    if (!datesLine) {
      const fmt = (v) => v instanceof Date ? v.toISOString().slice(0,10).split('-').reverse().join('-') : String(v).slice(0,10).split('-').reverse().join('-');
      datesLine = `${fmt(lr.from_date)} → ${fmt(lr.to_date)}`;
    }

    const target = await getNotifyTarget(lr.user_id);
    if (target) {
      // Same plain WhatsApp-style wording as every other notification email.
      const statusIcon = newStatus === 'approved' ? '✅' : '❌';
      const statusWord = newStatus === 'approved' ? 'APPROVED' : 'REJECTED';
      const subjectWord = lr.leave_type === 'extra_working' ? 'Extra Working' : 'Leave';
      const [[apRow]] = await db.query('SELECT name FROM users WHERE id=? LIMIT 1', [uid]);
      const msg = `Hello ${target.name || ''},\n\n${statusIcon} *${subjectWord} ${statusWord}*\n\n` +
        `*Type:* ${typeLabel}\n` +
        `*Dates:* ${datesLine}\n` +
        `*Decided by:* ${apRow?.name || 'Approver'}\n` +
        (note ? `*Note:* ${note}\n` : '') +
        `\n— E-Marketing Task Manager`;
      sendMail(target.email, `${subjectWord} ${newStatus} — ${typeLabel}`, waTextToEmailHtml(msg)).catch(()=>{});
    }
    // Requester is notified by EMAIL only now (sent just above via
    // getNotifyTarget) — the personal WhatsApp DM has been retired.

    res.json({ success: true, status: newStatus });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete own pending leave (or admin force-delete)
app.delete('/api/leaves/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const uid = req.session.userId;
    const role = req.session.role;
    const [rows] = await db.query('SELECT * FROM leave_requests WHERE id=?', [id]);
    const lr = rows[0];
    if (!lr) return res.status(404).json({ error: 'Not found' });
    if (role !== 'admin' && (lr.user_id !== uid || lr.status !== 'pending')) {
      return res.status(403).json({ error: 'Cannot delete this request' });
    }
    await archiveDeleted('leave_requests', lr, req, {
      summary: r => `Leave (${r.leave_type || ''}, ${r.status || ''}) for user ${r.user_id}`,
    });
    await db.query('DELETE FROM leave_requests WHERE id=?', [id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// HOLIDAYS — global; everyone reads, admin writes
// Plus helpers used everywhere to decide if a user is "off" on a date.
// ══════════════════════════════════════════════════════
function _toDateStr(v) {
  if (v instanceof Date) return v.toISOString().slice(0,10);
  return String(v).slice(0,10);
}

// 'YYYY-MM-DD' + n days, same format back.
function _addDays(dateStr, n) {
  const d = new Date(_toDateStr(dateStr) + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

// dateStr = 'YYYY-MM-DD'; holidaysSet = Set of YYYY-MM-DD strings
// Single source of truth for off-days: Sunday, the last Saturday of the month,
// and the Holiday tab (holidays table) — same rule applies to everyone.
// Per-user week_off / extra_off are NOT considered.
function isUserOffOn(_user, dateStr, holidaysSet) {
  const ds = _toDateStr(dateStr);
  const d = new Date(ds + 'T00:00:00Z');
  if (d.getUTCDay() === 0) return true; // Sunday
  if (isLastSaturdayOfMonth(ds)) return true;
  if (holidaysSet && holidaysSet.has(ds)) return true;
  return false;
}

async function loadHolidaysSet() {
  try {
    const [rows] = await db.query('SELECT DATE_FORMAT(holiday_date,"%Y-%m-%d") AS d FROM holidays');
    return new Set(rows.map(r => r.d));
  } catch (e) {
    console.error('loadHolidaysSet error:', e.message);
    return new Set();
  }
}

// Universal "no-message day" guard for all reminder/summary crons.
// Returns { off: true, reason } if today is Sunday IST OR in the holidays table.
// True if the given YYYY-MM-DD is the LAST Saturday of its month — a company
// off day. (Saturday AND the next Saturday falls in a different month.)
function isLastSaturdayOfMonth(dateStr) {
  const d = new Date(dateStr + 'T00:00:00Z');
  if (d.getUTCDay() !== 6) return false; // 6 = Saturday
  const next = new Date(d.getTime() + 7 * 24 * 60 * 60 * 1000);
  return next.getUTCMonth() !== d.getUTCMonth();
}

async function getTodayOffIST() {
  const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
  const today = istNow.toISOString().split('T')[0];
  const istDay = istNow.getUTCDay(); // 0 = Sunday IST
  if (istDay === 0) return { off: true, reason: 'Sunday — reminders skipped', today };
  // Last Saturday of every month is a company off day.
  if (isLastSaturdayOfMonth(today)) return { off: true, reason: 'Last Saturday of month — reminders skipped', today };
  const holidaysSet = await loadHolidaysSet();
  if (holidaysSet.has(today)) return { off: true, reason: 'Holiday — reminders skipped', today, holidaysSet };
  return { off: false, today, holidaysSet };
}

// User IDs who have filed a leave covering `today` — pending or approved both
// count (only rejected leaves leave the user on the missing-names list).
// extra_working is the OPPOSITE of leave so it's deliberately excluded.
// work_from_home is ALSO excluded — WFH people are still working, so they must
// fill the daily report (and still get task reminders) and should appear in the
// "report not filled" list if they don't.
async function usersOnLeaveSet(today) {
  try {
    const [rows] = await db.query(
      `SELECT DISTINCT user_id FROM leave_requests
        WHERE status <> 'rejected'
          AND leave_type IN ('full_day','half_day')
          AND from_date <= ? AND to_date >= ?`,
      [today, today]);
    return new Set(rows.map(r => r.user_id));
  } catch (e) {
    console.error('usersOnLeaveSet error:', e.message);
    return new Set();
  }
}

// Find next working day on/after fromDate for a given user (max 60 day lookahead)
// Every date a user is on APPROVED full-day leave, within a window.
// Deliberately stricter than usersOnLeaveSet(), which counts anything not
// rejected — announcing to a client that someone is away on the strength of an
// unapproved request would be wrong. Leave is stored either as a from/to range
// or, when the days are scattered, as a dates_json list; the list wins.
async function approvedLeaveDates(userId, fromYmd, toYmd) {
  const out = new Set();
  try {
    const [rows] = await db.query(
      `SELECT dates_json,
              DATE_FORMAT(from_date,'%Y-%m-%d') AS from_date,
              DATE_FORMAT(to_date,'%Y-%m-%d')   AS to_date
         FROM leave_requests
        WHERE user_id=? AND status='approved' AND leave_type='full_day'
          AND from_date <= ? AND to_date >= ?`, [userId, toYmd, fromYmd]);
    for (const r of rows) {
      let listed = null;
      if (r.dates_json) {
        try { listed = JSON.parse(r.dates_json).map(x => _toDateStr(x.date || x)); } catch { listed = null; }
      }
      if (listed) { listed.forEach(d => out.add(d)); continue; }
      const d = new Date(r.from_date + 'T00:00:00Z');
      const end = new Date(r.to_date + 'T00:00:00Z');
      for (let i = 0; i < 400 && d <= end; i++) {
        out.add(d.toISOString().split('T')[0]);
        d.setUTCDate(d.getUTCDate() + 1);
      }
    }
  } catch (e) { console.error('approvedLeaveDates err:', e.message); }
  return out;
}

// Next day this user actually works: skips Sundays, last-Saturdays, holidays —
// and their own further leave days, so a task due on the 18th of a 18/20 leave
// lands on the 19th, not back on the 20th.
function nextWorkingDayOffLeave(user, fromDateStr, holidaysSet, leaveDates) {
  let ds = _toDateStr(fromDateStr);
  for (let i = 0; i < 60; i++) {
    ds = nextWorkingDay(user, ds, holidaysSet);
    if (!leaveDates || !leaveDates.has(ds)) return ds;
  }
  return ds;
}

function nextWorkingDay(user, fromDateStr, holidaysSet) {
  const d = new Date(_toDateStr(fromDateStr) + 'T00:00:00');
  for (let i = 0; i < 60; i++) {
    d.setDate(d.getDate() + 1);
    const yy = d.getFullYear();
    const mm = String(d.getMonth()+1).padStart(2,'0');
    const dd = String(d.getDate()).padStart(2,'0');
    const ds = `${yy}-${mm}-${dd}`;
    if (!isUserOffOn(user, ds, holidaysSet)) return ds;
  }
  return _toDateStr(fromDateStr); // fallback
}

app.get('/api/holidays', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(`
      SELECT id, name, DATE_FORMAT(holiday_date,'%Y-%m-%d') AS holiday_date
      FROM holidays ORDER BY holiday_date ASC`);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/holidays', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { date, name } = req.body;
    if (!date || !name) return res.status(400).json({ error: 'date and name required' });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return res.status(400).json({ error: 'Invalid date format' });

    await db.query(
      'INSERT INTO holidays (holiday_date, name, created_by) VALUES (?,?,?) ' +
      'ON DUPLICATE KEY UPDATE name=VALUES(name)',
      [date, name.trim(), req.session.userId]
    );

    // Cascade: delete checklist tasks on this date + push delegation tasks forward
    const cascade = await cascadeHolidayDate(date, req);
    res.json({ success: true, ...cascade });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk holidays — accepts array of {date, name}
app.post('/api/holidays/bulk', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { holidays } = req.body;
    if (!Array.isArray(holidays) || !holidays.length) return res.status(400).json({ error: 'No holidays provided' });

    let added = 0, skipped = 0, errors = [];
    let cascadeDeleted = 0, cascadePushed = 0;

    for (const h of holidays) {
      const date = (h.date || '').trim();
      const name = (h.name || '').trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !name) {
        skipped++; errors.push({ row: h, reason: 'invalid date or empty name' });
        continue;
      }
      try {
        await db.query(
          'INSERT INTO holidays (holiday_date, name, created_by) VALUES (?,?,?) ' +
          'ON DUPLICATE KEY UPDATE name=VALUES(name)',
          [date, name, req.session.userId]
        );
        const c = await cascadeHolidayDate(date, req);
        cascadeDeleted += c.deletedChecklist || 0;
        cascadePushed += c.pushedDelegation || 0;
        added++;
      } catch (e) {
        skipped++; errors.push({ row: h, reason: e.message });
      }
    }

    res.json({ success: true, added, skipped, cascadeDeleted, cascadePushed, errors });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/holidays/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [doomed] = await db.query('SELECT * FROM holidays WHERE id=?', [parseInt(req.params.id, 10)]);
    await archiveDeleted('holidays', doomed, req, { summary: r => `Holiday: ${r.name || ''} (${r.holiday_date || ''})` });
    await db.query('DELETE FROM holidays WHERE id=?', [parseInt(req.params.id, 10)]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// On holiday add: delete checklist tasks on that date + push delegation tasks
async function cascadeHolidayDate(dateStr, req) {
  let deletedChecklist = 0, pushedDelegation = 0;
  try {
    // These rows vanish as a side effect of marking a holiday — the user never
    // explicitly deleted them — so archiving them matters more here, not less.
    const [doomed] = await db.query(
      "SELECT * FROM checklist_tasks WHERE due_date=? AND status='pending'", [dateStr]);
    await archiveDeleted('checklist_tasks', doomed, req, {
      summary: r => `Checklist: ${r.description || ''}`,
      via: 'cascadeHolidayDate',
      reason: `Auto-deleted: ${dateStr} was marked a holiday`,
    });
    const [del] = await db.query("DELETE FROM checklist_tasks WHERE due_date=? AND status='pending'", [dateStr]);
    deletedChecklist = del.affectedRows || 0;
  } catch (e) { console.error('cascade checklist:', e.message); }

  try {
    const holidaysSet = await loadHolidaysSet();
    const [delegationsOnDate] = await db.query(
      "SELECT t.id, t.assigned_to, u.week_off, u.extra_off " +
      "FROM delegation_tasks t JOIN users u ON t.assigned_to=u.id " +
      "WHERE t.due_date=? AND t.status='pending'",
      [dateStr]
    );
    for (const t of delegationsOnDate) {
      const newDate = nextWorkingDay(t, dateStr, holidaysSet);
      await db.query('UPDATE delegation_tasks SET due_date=? WHERE id=?', [newDate, t.id]);
      logTaskActivity({ taskId: t.id, field: 'due_date', oldValue: dateStr, newValue: newDate,
        changedBy: req?.session?.userId, source: 'holiday-cascade',
        note: `${dateStr} marked a holiday` });
      pushedDelegation++;
    }
  } catch (e) { console.error('cascade delegation:', e.message); }

  return { deletedChecklist, pushedDelegation };
}

// ══════════════════════════════════════════════════════
// MEETINGS — scheduler with WhatsApp + Google Meet hooks
// ══════════════════════════════════════════════════════
// All client meeting notifications fan out to this single WhatsApp group.
// User confirmed (2026-05-21) this group is used for every client.
const MEETING_CLIENT_GROUP_ID = process.env.MEETING_CLIENT_GROUP_ID || '120363400573269993@g.us';
// Business hours for slot generation. Sundays + holidays excluded automatically.
const MEETING_BIZ_HOURS = { startHour: 10, endHour: 19, slotMin: 30 };
// Google Workspace user to impersonate for Meet link creation via service-account DWD.
// Leave empty to disable auto-Meet-link (link will be optional manual paste instead).
const MEETING_GMEET_IMPERSONATE = process.env.GOOGLE_MEET_IMPERSONATE_EMAIL || '';

// Build every slot for a given date, mark booked vs free per attendee set.
// Returns: [{ start: 'HH:MM', end: 'HH:MM', booked: bool, busyUserIds: [int] }]
// viewerId — the logged-in user; only meetings involving this user count as
// "booked" so each user's calendar shows their own conflicts only.
async function buildMeetingSlots(dateStr, userIds = [], viewerId = null) {
  const slots = [];
  const { startHour, endHour, slotMin } = MEETING_BIZ_HOURS;
  for (let h = startHour; h < endHour; h++) {
    for (let m = 0; m < 60; m += slotMin) {
      const sh = String(h).padStart(2, '0'), sm = String(m).padStart(2, '0');
      const endMin = m + slotMin;
      const eh = String(endMin >= 60 ? h + 1 : h).padStart(2, '0');
      const em = String(endMin % 60).padStart(2, '0');
      slots.push({ start: `${sh}:${sm}`, end: `${eh}:${em}`, booked: false, busyUserIds: [] });
    }
  }
  const [meetings] = await db.query(
    `SELECT m.id, m.title, TIME_FORMAT(m.start_time,'%H:%i') AS start_time,
            TIME_FORMAT(m.end_time,'%H:%i') AS end_time, m.organizer_id
     FROM meetings m
     WHERE m.meeting_date = ? AND m.status = 'scheduled'`,
    [dateStr]
  );
  // For each requested user, the exact meeting time ranges that make them busy
  // that day — lets the caller say e.g. "Naman busy 9:00–10:00 AM" instead of
  // just flagging a conflict.
  const busyRanges = {};
  if (meetings.length) {
    const mIds = meetings.map(m => m.id);
    const [attendees] = await db.query(
      `SELECT meeting_id, user_id FROM meeting_attendees WHERE meeting_id IN (${mIds.map(()=>'?').join(',')})`,
      mIds
    );
    const attByMtg = {};
    for (const a of attendees) (attByMtg[a.meeting_id] = attByMtg[a.meeting_id] || []).push(a.user_id);
    for (const m of meetings) {
      const involved = new Set([m.organizer_id, ...(attByMtg[m.id] || [])]);
      // A slot is "booked" for the viewer only if the viewer themselves is in
      // this meeting. Others' meetings don't block the viewer's calendar.
      const viewerInvolved = viewerId != null && involved.has(viewerId);
      for (const slot of slots) {
        if (slot.start < m.end_time && slot.end > m.start_time) {
          if (viewerInvolved) slot.booked = true;
          // busyUserIds still records everyone (used for team availability hints
          // in the schedule modal when picking attendees).
          for (const uid of involved) if (!slot.busyUserIds.includes(uid)) slot.busyUserIds.push(uid);
        }
      }
      for (const uid of involved) {
        if (userIds.length && !userIds.includes(uid)) continue;
        (busyRanges[uid] = busyRanges[uid] || []).push({ start: m.start_time, end: m.end_time, title: m.title });
      }
    }
  }
  if (userIds.length) {
    for (const slot of slots) {
      const conflict = slot.busyUserIds.some(uid => userIds.includes(uid));
      slot.conflictForSelection = conflict;
    }
  }
  return { slots, busyRanges };
}

// Try to auto-create a Google Meet link via Calendar API + service-account DWD.
// Returns null on any failure so the caller can fall back to a manual link.
async function createGoogleMeetLink({ title, dateStr, startTime, endTime, attendeeEmails = [] }) {
  if (!MEETING_GMEET_IMPERSONATE || !process.env.GOOGLE_CREDENTIALS) return null;
  try {
    const { google } = require('googleapis');
    const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
    const auth = new google.auth.JWT({
      email: creds.client_email,
      key: creds.private_key,
      scopes: ['https://www.googleapis.com/auth/calendar.events'],
      subject: MEETING_GMEET_IMPERSONATE
    });
    await auth.authorize();
    const calendar = google.calendar({ version: 'v3', auth });
    const startIso = `${dateStr}T${startTime}:00+05:30`;
    const endIso   = `${dateStr}T${endTime}:00+05:30`;
    const requestId = `meet-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
    const event = await calendar.events.insert({
      calendarId: 'primary',
      conferenceDataVersion: 1,
      requestBody: {
        summary: title,
        start: { dateTime: startIso, timeZone: 'Asia/Kolkata' },
        end:   { dateTime: endIso,   timeZone: 'Asia/Kolkata' },
        attendees: attendeeEmails.filter(Boolean).map(e => ({ email: e })),
        conferenceData: { createRequest: { requestId, conferenceSolutionKey: { type: 'hangoutsMeet' } } }
      }
    });
    const link = event.data.hangoutLink || event.data.conferenceData?.entryPoints?.[0]?.uri || null;
    return link;
  } catch (err) {
    console.error('  ⚠️ Google Meet auto-create failed:', err.message);
    return null;
  }
}

function _meetingMsgBody(action, meeting, clientName, organizerName, attendeeNames, forClientGroup = false) {
  const fmtDate = d => (d || '').split('-').reverse().join('/');
  const fmtTime12 = t => {
    if (!t) return '';
    const [h, m] = t.split(':').map(Number);
    const period = h < 12 ? 'AM' : 'PM';
    const h12 = h % 12 || 12;
    return m === 0 ? `${h12} ${period}` : `${h12}:${String(m).padStart(2,'0')} ${period}`;
  };
  const headline = action === 'created' ? '📅 *New Meeting Scheduled*'
                 : action === 'rescheduled' ? '🔄 *Meeting Rescheduled*'
                 : action === 'reminder' ? `⏰ *${fmtTime12(meeting.start_time)} Meeting starts soon!*`
                 : '❌ *Meeting Cancelled*';
  // Client group sees only the essentials — no organizer / team / agenda / client name.
  // Internal DMs (organizer + attendees) get the full context.
  if (forClientGroup) {
    const lines = [
      headline,
      '',
      `*Title:* ${meeting.title}`,
      `*Date:* ${fmtDate(meeting.meeting_date)}`,
      `*Time:* ${fmtTime12(meeting.start_time)} – ${fmtTime12(meeting.end_time)}`
    ];
    if (action !== 'cancelled' && meeting.meet_link) lines.push('', `*Join:* ${meeting.meet_link}`);
    return lines.join('\n');
  }
  const lines = [
    headline,
    '',
    `*Title:* ${meeting.title}`,
    `*Client:* ${clientName || '—'}`,
    `*Date:* ${fmtDate(meeting.meeting_date)}`,
    `*Time:* ${fmtTime12(meeting.start_time)} – ${fmtTime12(meeting.end_time)}`,
    `*Organizer:* ${organizerName || '—'}`
  ];
  if (attendeeNames && attendeeNames.length) lines.push(`*Team:* ${attendeeNames.join(', ')}`);
  if (action !== 'reminder' && meeting.agenda) lines.push('', `*Agenda:* ${meeting.agenda}`);
  if (action !== 'cancelled' && meeting.meet_link) lines.push('', `*Join:* ${meeting.meet_link}`);
  return lines.join('\n');
}

async function sendMeetingNotification(meetingId, action) {
  try {
    const [[m]] = await db.query(
      `SELECT m.id, m.title, m.agenda, m.client_id, m.organizer_id,
              DATE_FORMAT(m.meeting_date,'%Y-%m-%d') AS meeting_date,
              TIME_FORMAT(m.start_time,'%H:%i') AS start_time,
              TIME_FORMAT(m.end_time,'%H:%i')   AS end_time,
              m.meet_link, m.status,
              c.name AS client_name, u.name AS organizer_name
       FROM meetings m
       LEFT JOIN clients c ON m.client_id = c.id
       LEFT JOIN users   u ON m.organizer_id = u.id
       WHERE m.id = ?`, [meetingId]);
    if (!m) return { ok: false, reason: 'meeting not found' };
    const [atts] = await db.query(
      `SELECT u.id, u.name, u.phone FROM meeting_attendees ma
       JOIN users u ON ma.user_id = u.id WHERE ma.meeting_id = ?`, [meetingId]);
    const internalBody = _meetingMsgBody(action, m, m.client_name, m.organizer_name, atts.map(a => a.name), false);
    // Notify only the host (organizer) + attendees by EMAIL now (was WhatsApp) —
    // no client-group fanout. Resolved by user id.
    const targetIds = new Set(atts.map(a => a.id));
    if (m.organizer_id) targetIds.add(m.organizer_id);
    const subjectMap = { created: 'New Meeting Scheduled', rescheduled: 'Meeting Rescheduled', cancelled: 'Meeting Cancelled', reminder: 'Meeting Reminder' };
    const subject = `${subjectMap[action] || 'Meeting Update'} — ${m.title || ''}`;
    const dmResults = [];
    for (const uid of targetIds) {
      dmResults.push(await emailUserWaText(uid, subject, internalBody));
    }
    return { ok: true, dms: dmResults.filter(Boolean).length };
  } catch (err) {
    console.error('  ⚠️ Meeting notification failed:', err.message);
    return { ok: false, error: err.message };
  }
}

// Meetings / Scheduler routes now live in routes/meetings.js. They are handed
// the same instances this file uses rather than re-requiring anything: db has
// the connection-limit retry wrapper on it, and sendMeetingNotification is also
// driven by the pre-meeting reminder cron above. The call sits exactly where
// the routes did, so every binding passed in is in scope at the same point it
// always was.
require('./backend/routes/meetings')(app, {
  db,
  requireAuth,
  buildMeetingSlots,
  createGoogleMeetLink,
  sendMeetingNotification,
  loadHolidaysSet,
  isLastSaturdayOfMonth,
});

// ══════════════════════════════════════════════════════
// DAY PLAN ITEMS — Day-view quick-add ("9am to 10am meeting")
// ══════════════════════════════════════════════════════

app.get('/api/day-plan-items', requireAuth, async (req, res) => {
  try {
    const { from, to } = req.query;
    const fromDate = from || to;
    const toDate = to || from;
    if (!fromDate || !toDate) return res.status(400).json({ error: 'from/to required' });
    const [rows] = await db.query(
      `SELECT * FROM day_plan_items WHERE user_id=? AND item_date BETWEEN ? AND ? ORDER BY start_time`,
      [req.session.userId, fromDate, toDate]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/day-plan-items', requireAuth, async (req, res) => {
  try {
    const { title, item_date, start_time, end_time } = req.body;
    if (!title || !item_date || !start_time || !end_time) {
      return res.status(400).json({ error: 'title, item_date, start_time, end_time required' });
    }
    const [result] = await db.query(
      `INSERT INTO day_plan_items (user_id, item_date, start_time, end_time, title) VALUES (?,?,?,?,?)`,
      [req.session.userId, item_date, start_time, end_time, title]);
    res.json({ ok: true, id: result.insertId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/day-plan-items/:id', requireAuth, async (req, res) => {
  try {
    const [[existing]] = await db.query('SELECT * FROM day_plan_items WHERE id=?', [req.params.id]);
    if (!existing) return res.status(404).json({ error: 'not found' });
    if (existing.user_id !== req.session.userId && req.session.role !== 'admin') {
      return res.status(403).json({ error: 'only owner or admin can delete' });
    }
    await archiveDeleted('day_plan_items', existing, req, {
      summary: r => `Day plan: ${r.title || ''} (${r.item_date || ''})`,
    });
    await db.query('DELETE FROM day_plan_items WHERE id=?', [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// INVENTORY MANAGEMENT routes now live in routes/inventory.js. The call sits exactly
// where the routes did, so every binding passed in is in scope at the same
// point it always was.
require('./backend/routes/inventory')(app, {
  db,
  requireAuth,
  userCanDo,
  userCanSee,
  archiveDeleted,
});

// HRM routes now live in routes/hrm.js. The call sits exactly
// where the routes did, so every binding passed in is in scope at the same
// point it always was.
require('./backend/routes/hrm')(app, {
  db,
  requireAuth,
  requireAdmin,
  archiveDeleted,
  userCanSee,
  userCanDo,
  sendMail,
  sendWhatsApp,
  waTextToEmailHtml,
  usersForSetting,
  getDriveClient,
  // The module lives in routes/, so its own __dirname points one level too
  // deep. The offer-letter signature is read from <root>/public/signature.png,
  // so the root is passed in explicitly rather than guessed with '..'.
  appRoot: __dirname,
});

// ══════════════════════════════════════════════════════
// LOGS — deleted-records archive viewer + restore (admin only)
// ══════════════════════════════════════════════════════

// Only rows from these tables can be restored. source_table is written by our
// own code, but it is interpolated straight into SQL below, so it is validated
// against this list rather than trusted. Tables whose rows are meaningless on
// their own (join/child rows re-created by their parent's own flow) are left
// out deliberately.
const RESTORABLE_TABLES = new Set([
  'delegation_tasks', 'checklist_tasks', 'task_subtasks', 'task_comments',
  'users', 'clients', 'client_feedback', 'client_department_folders',
  'dms_external_links', 'leave_requests', 'holidays', 'day_plan_items',
  'inventory_items', 'fms_sheets', 'cc_cards', 'cc_statements',
  'cc_transactions', 'cc_departments', 'pr_cards', 'payment_requests',
  'hrm_candidates', 'client_credentials',
]);

// GET /api/deleted-records — 150 most recent deletes.
// record_data is deliberately omitted: it can carry base64 photos and would
// bloat the list response. Fetch a single row's full JSON via /:id below.
app.get('/api/deleted-records', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [rows] = await db.query(`
      SELECT dr.id, dr.source_table, dr.record_id, dr.summary,
             dr.deleted_by, dr.deleted_by_name, dr.deleted_by_role,
             dr.deleted_via, dr.delete_reason,
             -- Format in SQL, not JS: the pool sets no timezone, so mysql2
             -- reads these back in the Node process's tz (UTC on Vercel) while
             -- MySQL wrote them in its own (IST) — the browser then shifts
             -- again, landing +5:30 out. Handing the frontend a ready string
             -- keeps the value exactly as the DB clock recorded it.
             DATE_FORMAT(dr.deleted_at, '%d %b %Y, %h:%i %p') AS deleted_at_fmt,
             DATE_FORMAT(dr.restored_at, '%d %b %Y, %h:%i %p') AS restored_at_fmt,
             dr.restored_at, dr.restored_by,
             ru.name AS restored_by_name,
             CHAR_LENGTH(dr.record_data) AS record_size
        FROM deleted_records dr
        LEFT JOIN users ru ON ru.id = dr.restored_by
       ORDER BY dr.id DESC
       LIMIT 150`);
    const restorable = {};
    for (const r of rows) restorable[r.id] = RESTORABLE_TABLES.has(r.source_table);
    res.json({ rows, restorable });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/deleted-records/:id — one row incl. its full JSON snapshot
app.get('/api/deleted-records/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [[rec]] = await db.query(
      `SELECT *,
              DATE_FORMAT(deleted_at, '%d %b %Y, %h:%i %p') AS deleted_at_fmt,
              DATE_FORMAT(restored_at, '%d %b %Y, %h:%i %p') AS restored_at_fmt
         FROM deleted_records WHERE id=?`, [req.params.id]);
    if (!rec) return res.status(404).json({ error: 'Not found' });
    let data = null;
    try { data = JSON.parse(rec.record_data); } catch (e) { /* keep raw below */ }
    res.json({ ...rec, record_data: data, record_data_raw: data ? undefined : rec.record_data });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /api/deleted-records/:id/restore — put an archived row back.
// Re-inserts with the ORIGINAL id so existing references to it line up again;
// if that id has since been taken, we refuse rather than silently re-home the
// row under a new id and leave every reference pointing at the wrong record.
app.post('/api/deleted-records/:id/restore', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [[rec]] = await db.query('SELECT * FROM deleted_records WHERE id=?', [req.params.id]);
    if (!rec) return res.status(404).json({ error: 'Not found' });
    if (rec.restored_at) return res.status(400).json({ error: 'This record was already restored' });
    if (!RESTORABLE_TABLES.has(rec.source_table)) {
      return res.status(400).json({ error: `${rec.source_table} rows cannot be restored from here` });
    }

    let data;
    try { data = JSON.parse(rec.record_data); }
    catch (e) { return res.status(400).json({ error: 'Archived snapshot is not valid JSON — cannot restore' }); }
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      return res.status(400).json({ error: 'Archived snapshot is not a row object — cannot restore' });
    }

    // Drop any key that is no longer a real column (the schema may have moved
    // on since the delete), so restore degrades instead of hard-failing.
    const [cols] = await db.query('SHOW COLUMNS FROM ??', [rec.source_table]);
    const live = new Set(cols.map(c => c.Field));
    const keys = Object.keys(data).filter(k => live.has(k));
    if (!keys.length) return res.status(400).json({ error: 'No columns from the snapshot still exist — cannot restore' });
    const dropped = Object.keys(data).filter(k => !live.has(k));

    if (data.id != null && live.has('id')) {
      const [[clash]] = await db.query('SELECT id FROM ?? WHERE id=? LIMIT 1', [rec.source_table, data.id]);
      if (clash) {
        return res.status(409).json({
          error: `Cannot restore: ${rec.source_table} #${data.id} already exists (that id was reused). Restore it manually if needed.`,
        });
      }
    }

    await db.query(
      `INSERT INTO ?? (${keys.map(() => '??').join(',')}) VALUES (${keys.map(() => '?').join(',')})`,
      [rec.source_table, ...keys, ...keys.map(k => data[k])]);

    await db.query(
      'UPDATE deleted_records SET restored_at=NOW(), restored_by=? WHERE id=?',
      [req.session.userId, rec.id]);

    res.json({ ok: true, restored_id: data.id ?? null, droppedColumns: dropped });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// How long an archived delete is kept before the purge cron drops it.
const DELETED_RECORDS_RETENTION_DAYS = Number(process.env.DELETED_RECORDS_RETENTION_DAYS) || 60;

// Purge archive rows older than the retention window.
//
// This is the one hard delete in the app with no archive behind it — by
// definition, since this IS the archive. Past the window, a deleted row and
// the record of who deleted it are both gone for good. Owner-approved
// (2026-07-16) as an explicit trade for keeping the table from growing without
// bound (record_data can carry base64 photos).
async function purgeDeletedRecords() {
  const days = DELETED_RECORDS_RETENTION_DAYS;
  const [[{ due }]] = await db.query(
    'SELECT COUNT(*) AS due FROM deleted_records WHERE deleted_at < NOW() - INTERVAL ? DAY', [days]);
  if (!due) return { purged: 0, retentionDays: days };
  const [r] = await db.query(
    'DELETE FROM deleted_records WHERE deleted_at < NOW() - INTERVAL ? DAY', [days]);
  const purged = r.affectedRows || 0;
  console.log(`purgeDeletedRecords: removed ${purged} archive row(s) older than ${days} days`);
  return { purged, retentionDays: days };
}

app.get('/api/cron/purge-deleted-records', async (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (!process.env.CRON_SECRET || authHeader !== expected) {
    return res.status(401).json({ error: 'Unauthorized cron request' });
  }
  try {
    res.json({ success: true, ...(await purgeDeletedRecords()) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Admin manual trigger (for testing / one-off cleanup)
app.post('/api/admin/purge-deleted-records', requireAuth, requireAdmin, async (req, res) => {
  try {
    res.json({ success: true, ...(await purgeDeletedRecords()) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
// Auth check is handled client-side via /api/me in init() — removing server-side
// requireAuth here prevents app.html from loading if cookie has any timing/domain issue
app.get('/app', (req, res) => res.sendFile(path.join(__dirname, 'public', 'app.html')));
// Standalone client portal — separate page so clients don't pull the full
// team-app bundle. Role gate happens client-side in client.html via /api/me.
app.get('/client', (req, res) => res.sendFile(path.join(__dirname, 'public', 'client.html')));

// ══════════════════════════════════════════════════════
// EXPORT FOR VERCEL (serverless) + LISTEN FOR LOCAL DEV
// ══════════════════════════════════════════════════════
// On Vercel, the platform handles HTTP — we just export the app.
// Locally (and on traditional hosts), we call app.listen().
if (process.env.VERCEL || process.env.NOW_REGION) {
  module.exports = app;
} else {
  app.listen(PORT, () => {
    console.log(`\n  ✦ E-Marketing Task Manager: http://localhost:${PORT}`);
    console.log(`  Login: aman@test.com / password\n`);
  });
  module.exports = app;
}