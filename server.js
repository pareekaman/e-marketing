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

  await sa(`CREATE TABLE IF NOT EXISTS task_transfers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id INT NOT NULL,
    task_type VARCHAR(20) NOT NULL,
    from_user INT NOT NULL,
    to_user INT NOT NULL,
    requested_by INT NOT NULL,
    status ENUM('pending','approved','rejected') DEFAULT 'pending',
    note TEXT DEFAULT '',
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
  await sa(`ALTER TABLE fms_steps ADD COLUMN show_cols TEXT DEFAULT '' AFTER extra_col`);
  await sa(`ALTER TABLE fms_steps ADD COLUMN delay_reason_col VARCHAR(10) DEFAULT '' AFTER show_cols`);
  await sa(`ALTER TABLE fms_steps ADD COLUMN doer_name_col VARCHAR(10) DEFAULT '' AFTER delay_reason_col`);
  await sa(`ALTER TABLE users ADD COLUMN department VARCHAR(255) DEFAULT '' AFTER phone`);
  await sa(`ALTER TABLE users ADD COLUMN week_off VARCHAR(50) DEFAULT '' AFTER department`);
  await sa(`ALTER TABLE users ADD COLUMN extra_off TEXT DEFAULT '' AFTER week_off`);
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
  // Optional clock time on the deadline. Only the handler→client flow sets it
  // (they commit the client to a date AND time); every internal task leaves it
  // NULL and keeps behaving as a date-only deadline.
  await sa(`ALTER TABLE delegation_tasks ADD COLUMN due_time TIME DEFAULT NULL AFTER due_date`);
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
  await sa(`ALTER TABLE task_subtasks ADD COLUMN priority ENUM('low','medium','high','urgent') DEFAULT 'low' AFTER description`);
  // Revise approval holds the requested new due-date here until the assigner approves.
  await sa(`ALTER TABLE task_approvals ADD COLUMN new_date DATE DEFAULT NULL AFTER action_type`);
  await sa(`ALTER TABLE checklist_tasks ADD COLUMN client_id INT DEFAULT NULL AFTER remarks`);
  await sa(`ALTER TABLE checklist_tasks ADD INDEX idx_client (client_id)`);
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN col_letter VARCHAR(10) DEFAULT '' AFTER row_label`);
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN field_type VARCHAR(20) DEFAULT 'text' AFTER col_letter`);
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN dropdown_options TEXT DEFAULT '' AFTER field_type`);
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
    notes TEXT DEFAULT '',
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
    handover_notes TEXT DEFAULT '',
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
    reschedule_reason TEXT DEFAULT '',
    joining_date DATE DEFAULT NULL,
    offer_sent TINYINT(1) DEFAULT 0,
    salary VARCHAR(100) DEFAULT '',
    notes TEXT DEFAULT '',
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
    error_detail TEXT DEFAULT '',
    payload_json LONGTEXT DEFAULT '',
    retry_count INT DEFAULT 0,
    last_retry_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_candidate (candidate_id),
    INDEX idx_status (status)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

  // Add columns to existing installs (no "IF NOT EXISTS" — invalid syntax on
  // MySQL 5.7, which silently no-ops the whole ALTER via sa()'s catch-all;
  // sa() already makes these idempotent, so plain ADD COLUMN is correct here)
  await sa(`ALTER TABLE hrm_candidates ADD COLUMN reschedule_reason TEXT DEFAULT ''`);
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
  await sa(`ALTER TABLE tasks ADD COLUMN remarks TEXT DEFAULT ''`);
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
async function sendMail(to, subject, html) {
  if (!to || !process.env.SMTP_USER) return;
  try {
    await mailTransporter.sendMail({
      from: `"${process.env.SMTP_FROM_NAME || 'E-Marketing Task Manager'}" <${process.env.SMTP_USER}>`,
      to, subject, html
    });
    console.log(`  📧 Email sent to ${to} — ${subject}`);
  } catch (err) {
    console.error(`  ❌ Email failed (${to}):`, err.message);
  }
}

// Helper: get user's notification email + name
async function getNotifyTarget(userId) {
  try {
    const [rows] = await db.query(
      'SELECT name, notification_email FROM users WHERE id=? LIMIT 1',
      [userId]
    );
    if (!rows[0] || !rows[0].notification_email) return null;
    return { name: rows[0].name, email: rows[0].notification_email };
  } catch { return null; }
}

// Email template for delegation task
function delegationEmailHtml({ assigneeName, assignerName, desc, dueDate, priority, approval, remarks }) {
  const appUrl = process.env.APP_URL || '#';
  return `
  <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#f6f9fc;padding:20px;">
    <div style="background:#fff;border-radius:8px;padding:30px;box-shadow:0 2px 8px rgba(0,0,0,0.05);">
      <h2 style="color:#F39C12;margin-top:0;">📋 New Task Assigned to You</h2>
      <p>Hi <b>${assigneeName || 'there'}</b>,</p>
      <p><b>${assignerName || 'Someone'}</b> has assigned you a new delegation task:</p>
      <table style="width:100%;border-collapse:collapse;margin:20px 0;">
        <tr><td style="padding:8px;background:#f0f4f8;width:140px;"><b>Task</b></td><td style="padding:8px;">${desc}</td></tr>
        <tr><td style="padding:8px;background:#f0f4f8;"><b>Due Date</b></td><td style="padding:8px;">${dueDate}</td></tr>
        <tr><td style="padding:8px;background:#f0f4f8;"><b>Priority</b></td><td style="padding:8px;text-transform:capitalize;">${priority}</td></tr>
        <tr><td style="padding:8px;background:#f0f4f8;"><b>Approval Required</b></td><td style="padding:8px;text-transform:capitalize;">${approval}</td></tr>
        ${remarks ? `<tr><td style="padding:8px;background:#f0f4f8;"><b>Remarks</b></td><td style="padding:8px;">${remarks}</td></tr>` : ''}
      </table>
      <a href="${appUrl}" style="display:inline-block;background:#F39C12;color:#fff;text-decoration:none;padding:12px 24px;border-radius:6px;font-weight:600;">Open E-Marketing Task Manager</a>
      <p style="color:#777;font-size:12px;margin-top:30px;">This is an automated email from E-Marketing Task Manager.</p>
    </div>
  </div>`;
}

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
function requireAdminOrPC(req, res, next) {
  if (req.session.role === 'admin' || req.session.role === 'pc') return next();
  res.status(403).json({ error: 'Admin or PC only' });
}
function getTable(type) {
  return type === 'delegation' ? 'delegation_tasks' : 'checklist_tasks';
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
    await sa(`ALTER TABLE users ADD COLUMN extra_off TEXT DEFAULT '' AFTER week_off`, 'users.extra_off');
    await sa(`ALTER TABLE users ADD COLUMN exclude_from_reminder TINYINT(1) DEFAULT 0 AFTER extra_off`, 'users.exclude_from_reminder');
    await sa(`ALTER TABLE users ADD COLUMN extra_access TEXT DEFAULT NULL AFTER exclude_from_reminder`, 'users.extra_access');
    await sa(`UPDATE users SET user_role=role WHERE user_role IS NULL`, 'backfill user_role from role');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN client_id INT DEFAULT NULL AFTER remarks`, 'delegation_tasks.client_id');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN url VARCHAR(2048) DEFAULT NULL AFTER client_id`, 'delegation_tasks.url');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN awaiting_due_date TINYINT(1) DEFAULT 0 AFTER waiting_approval`, 'delegation_tasks.awaiting_due_date');
    await sa(`ALTER TABLE delegation_tasks ADD COLUMN due_time TIME DEFAULT NULL AFTER due_date`, 'delegation_tasks.due_time');
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

app.post('/api/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const [rows] = await db.query('SELECT * FROM users WHERE email = ?', [email]);
    const user = rows[0];
    if (!user || !bcrypt.compareSync(password, user.password))
      return res.status(401).json({ error: 'Invalid email or password' });

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
    try {
      const [bd] = await db.query('SELECT birthday, joining_date FROM users WHERE id=?', [req.session.userId]);
      rows[0].birthday = bd[0]?.birthday || null;
      rows[0].joining_date = bd[0]?.joining_date || null;
    } catch(e) { rows[0].birthday = null; rows[0].joining_date = null; }
    rows[0].extra_access = parseExtraAccess(rows[0].extra_access);
    rows[0].canViewAllLeaves = isLeaveReportViewer(rows[0]);
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
      const [d] = await db.query(`SELECT SUM(CASE WHEN status='pending' AND due_date <= CURDATE() THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN status='revised' AND due_date <= CURDATE() THEN 1 ELSE 0 END) AS revised,SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed FROM delegation_tasks t WHERE 1=1 ${userFilter} ${delDateClause}`, delParams);
      pending += parseInt(d[0].pending)||0; revised += parseInt(d[0].revised)||0; completed += parseInt(d[0].completed)||0;
    }
    if (!skipStats && (taskType === 'checklist' || taskType === 'both')) {
      const [d] = await db.query(`SELECT SUM(CASE WHEN status='pending' AND due_date <= CURDATE() THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN status='revised' AND due_date <= CURDATE() THEN 1 ELSE 0 END) AS revised,SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed FROM checklist_tasks t WHERE 1=1 ${userFilter} ${chlDateClause}`, chlParams);
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
      const [rows] = await db.query(`SELECT t.id,'delegation' AS type,t.description,t.status,t.assigned_to,COALESCE(t.priority,'low') AS priority,COALESCE(t.approval,'no') AS approval,COALESCE(t.waiting_approval,0) AS waiting_approval,COALESCE(t.awaiting_due_date,0) AS awaiting_due_date,t.remarks,t.url,t.client_id,c.name AS client_name,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u1.name AS assignedToName,COALESCE(u2.name,'—') AS assignedByName FROM delegation_tasks t JOIN users u1 ON t.assigned_to=u1.id LEFT JOIN users u2 ON t.assigned_by=u2.id LEFT JOIN clients c ON t.client_id=c.id WHERE 1=1 ${rowStatusClause} ${delDateClause} ${userFilter} ORDER BY t.due_date ASC LIMIT 500`, delParams);
      delegationRows = rows;
    }
    if (taskType === 'checklist' || taskType === 'both') {
      const [rows] = await db.query(`SELECT t.id,'checklist' AS type,t.description,t.status,t.assigned_to,COALESCE(t.priority,'low') AS priority,'no' AS approval,0 AS waiting_approval,0 AS awaiting_due_date,t.remarks,t.client_id,c.name AS client_name,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u1.name AS assignedToName,COALESCE(u2.name,'—') AS assignedByName FROM checklist_tasks t JOIN users u1 ON t.assigned_to=u1.id LEFT JOIN users u2 ON t.assigned_by=u2.id LEFT JOIN clients c ON t.client_id=c.id WHERE 1=1 ${rowStatusClause} ${chlDateClause} ${userFilter} ORDER BY t.due_date ASC LIMIT 500`, chlParams);
      checklistRows = rows;
    }
    // `todayPending` kept for backwards compatibility (regular pending load still uses it).
    // `tasks` is the generic field for any status filter.
    res.json({ pending, revised, completed, upcoming, todayPending: [...delegationRows, ...checklistRows], tasks: [...delegationRows, ...checklistRows], status: rowStatus });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// TASKS
// ══════════════════════════════════════════════════════
app.get('/api/tasks', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const role = req.session.role;
    const isAdmin = role === 'admin';
    const isHod = role === 'hod';
    const { type, mine } = req.query;
    const isMine = (mine === '1' || mine === 'true');
    const table = getTable(type || 'delegation');
    const isDeleg = (type || 'delegation') === 'delegation';
    let where = 'WHERE 1=1';
    const params = [];

    if (isMine) {
      // "Delegate by Me" mode — only tasks I've assigned to someone ELSE.
      // Self-delegated tasks (assigned_to === me) already show in the regular Delegation tab — don't duplicate here.
      where += ' AND t.assigned_by = ? AND t.assigned_to <> t.assigned_by';
      params.push(uid);
    } else if (isAdmin || role === 'pc') {
      // Admin/PC — see everything
    } else if (isHod) {
      // HOD — tasks belonging to users in their department
      const [me] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
      const dept = me[0]?.department || '';
      const [deptUsers] = await db.query('SELECT id FROM users WHERE department=?', [dept]);
      if (!deptUsers.length) {
        return res.json({ grouped: [] });
      }
      const ids = deptUsers.map(u=>u.id);
      where += ` AND t.assigned_to IN (${ids.map(()=>'?').join(',')})`;
      params.push(...ids);
    } else {
      // Regular user — only their own tasks
      where += ' AND t.assigned_to = ?';
      params.push(uid);
    }

    // Explicit from/to range (sent by admin filter) overrides defaults for BOTH types.
    // Otherwise: delegation shows all future (for transfers), checklist caps at
    // today + 30 days so recurring checklists don't flood the table.
    const isDate = v => /^\d{4}-\d{2}-\d{2}$/.test(v);
    if (req.query.from && isDate(req.query.from)) { where += ' AND t.due_date >= ?'; params.push(req.query.from); }
    if (req.query.to   && isDate(req.query.to))   { where += ' AND t.due_date <= ?'; params.push(req.query.to);   }
    if (!isDeleg && !(req.query.from || req.query.to)) {
      where += ' AND t.due_date <= DATE_ADD(CURDATE(), INTERVAL 30 DAY)';
    }

    const subtaskCols = isDeleg
      ? "COALESCE((SELECT COUNT(*) FROM task_subtasks s WHERE s.task_id=t.id AND s.status='completed'),0) AS subtasks_done,COALESCE((SELECT COUNT(*) FROM task_subtasks s WHERE s.task_id=t.id AND s.status='pending'),0) AS subtasks_pending,"
      : "0 AS subtasks_done,0 AS subtasks_pending,";
    const [tasks] = await db.query(`SELECT t.id,'${type||'delegation'}' AS type,t.description,t.status,t.assigned_to,t.assigned_by,COALESCE(t.priority,'low') AS priority,${isDeleg?"COALESCE(t.approval,'no') AS approval,COALESCE(t.waiting_approval,0) AS waiting_approval,COALESCE(t.awaiting_due_date,0) AS awaiting_due_date,t.remarks,t.url,":"'no' AS approval,0 AS waiting_approval,0 AS awaiting_due_date,t.remarks,NULL AS url,"}${subtaskCols}t.client_id,c.name AS client_name,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u1.name AS assignedToName,u2.name AS assignedByName FROM ${table} t JOIN users u1 ON t.assigned_to=u1.id JOIN users u2 ON t.assigned_by=u2.id LEFT JOIN clients c ON t.client_id=c.id ${where} ORDER BY t.due_date ASC`, params);

    // mine=1 mode always returns flat tasks (never grouped)
    if (isMine) {
      return res.json({ tasks });
    }
    if (isAdmin || isHod || role === 'pc') {
      const grouped = {};
      tasks.forEach(t => {
        if (!grouped[t.assigned_to]) grouped[t.assigned_to] = { userId: t.assigned_to, name: t.assignedToName, tasks: [] };
        grouped[t.assigned_to].tasks.push(t);
      });
      return res.json({ grouped: Object.values(grouped) });
    }
    res.json({ tasks });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/tasks', requireAuth, async (req, res) => {
  try {
    const { type, desc, assignedTo, approverEmail, approver, date, priority, approval, remarks, client_id, clientId, url } = req.body;
    // Delegation only: assigner leaves the due date to the doer (their occupancy).
    const doerWillSet = (req.body.doerSetsDueDate === true || req.body.doerSetsDueDate === 'true')
      && (type || 'checklist') === 'delegation';
    // Accept either client_id or clientId from request body
    const clientIdInt = (() => {
      const raw = client_id != null ? client_id : clientId;
      const n = parseInt(raw, 10);
      return Number.isFinite(n) && n > 0 ? n : null;
    })();
    const isAdmin = req.session.role === 'admin';
    const isHod   = req.session.role === 'hod';
    const isUser  = req.session.role === 'user';
    const isClient = req.session.role === 'client';
    // Clients can only assign to their handler. Resolve from clients table.
    let targetUser;
    let enforcedClientId = clientIdInt;
    if (isClient) {
      const [[me]] = await db.query('SELECT client_id FROM users WHERE id=? LIMIT 1', [req.session.userId]);
      if (!me?.client_id) return res.status(403).json({ error: 'Client portal: no linked client' });
      const [[c]] = await db.query('SELECT handler_id FROM clients WHERE id=? LIMIT 1', [me.client_id]);
      if (!c?.handler_id) return res.status(400).json({ error: 'Your client does not have a handler assigned yet — contact admin' });
      targetUser = c.handler_id;
      enforcedClientId = me.client_id; // force tag the task to client's own id
      // Clients don't set a due date — the handler fills it in after assignment.
      if (!doerWillSet) {
        const ist = new Date(Date.now() + (5.5*60*60*1000));
        const minDate = new Date(ist.getTime() + 2*24*60*60*1000).toISOString().split('T')[0];
        if (!date || date < minDate) {
          return res.status(400).json({ error: `Due date must be at least 2 days from now (${minDate})` });
        }
      }
    } else {
      // Admin, HOD and regular users can all assign to others; fallback to self if not specified
      targetUser = (isAdmin || isHod || isUser) && assignedTo ? parseInt(assignedTo) : req.session.userId;
    }
    if (!desc) return res.status(400).json({ error: 'Description required' });
    if (!doerWillSet && !date) return res.status(400).json({ error: 'Description and date required' });

    // Staff delegating TO a client (the handler→client direction). The doer is
    // the client's own portal login, so it is only allowed when that login
    // really belongs to the client the task is tagged with — otherwise one
    // client's login could be handed another client's work.
    let doerIsClient = false;
    if (!isClient && targetUser !== req.session.userId) {
      const [[doer]] = await db.query('SELECT id, role, client_id FROM users WHERE id=? LIMIT 1', [targetUser]);
      if (!doer) return res.status(400).json({ error: 'Doer not found' });
      if (doer.role === 'client') {
        if (!enforcedClientId || Number(doer.client_id) !== Number(enforcedClientId)) {
          return res.status(403).json({ error: 'That portal login does not belong to this client' });
        }
        const [[cli]] = await db.query('SELECT id, handler_id FROM clients WHERE id=? LIMIT 1', [enforcedClientId]);
        const mayDelegate = isAdmin || isHod || await isHandlerOf(req.session.userId, cli);
        if (!mayDelegate) return res.status(403).json({ error: 'Only this client\'s handler can delegate to them' });
        if (doerWillSet) return res.status(400).json({ error: 'A client task needs a due date set by you' });
        doerIsClient = true;
      }
    }
    // Optional clock time on the deadline — only meaningful alongside a date.
    let dueTime = null;
    if (!doerWillSet && req.body.dueTime) {
      const m = /^([01]\d|2[0-3]):([0-5]\d)$/.exec(String(req.body.dueTime).trim());
      if (!m) return res.status(400).json({ error: 'Due time must be HH:MM (24-hour)' });
      dueTime = `${m[1]}:${m[2]}:00`;
    }

    // Holiday / week-off check — auto-adjust due_date if needed.
    // Skipped when the doer will set their own date (none yet to adjust), and
    // when the doer is a client: our holiday calendar and week-offs describe
    // staff, so silently moving a date the handler agreed with the client
    // would be wrong.
    let effectiveDate = doerWillSet ? null : date;
    let adjusted = false, adjustedReason = '';
    if (!doerWillSet && !doerIsClient) try {
      const holidaysSet = await loadHolidaysSet();
      const [[doerUser]] = await db.query('SELECT week_off, extra_off FROM users WHERE id=? LIMIT 1', [targetUser]);
      if (doerUser && isUserOffOn(doerUser, date, holidaysSet)) {
        const tt = (type||'checklist') === 'delegation' ? 'delegation' : 'checklist';
        if (tt === 'delegation') {
          // Push to next working day
          effectiveDate = nextWorkingDay(doerUser, date, holidaysSet);
          adjusted = true;
          adjustedReason = `Original date was a holiday/week-off — moved to ${effectiveDate}`;
        } else {
          // Checklist: skip creation on off day
          return res.json({ success: true, skipped: true, reason: 'Skipped — selected date is a holiday or doer\'s week-off' });
        }
      }
    } catch (e) { console.error('holiday check error:', e.message); }

    if ((type||'checklist') === 'delegation') {
      // Approver: prefer approverEmail; otherwise approver ID from form; otherwise logged-in user
      let assignedBy = req.session.userId;
      if (approverEmail) {
        const [aprRows] = await db.query('SELECT id FROM users WHERE email=? LIMIT 1', [approverEmail]);
        if (aprRows.length) assignedBy = aprRows[0].id;
      } else if (approver && approval === 'yes') {
        const apId = parseInt(approver);
        if (apId) {
          const [aprRows] = await db.query('SELECT id FROM users WHERE id=? LIMIT 1', [apId]);
          if (aprRows.length) assignedBy = aprRows[0].id;
        }
      }
      await db.query(`INSERT INTO delegation_tasks (description,assigned_to,assigned_by,due_date,due_time,status,priority,approval,remarks,client_id,url,awaiting_due_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, [desc, targetUser, assignedBy, effectiveDate, dueTime, 'pending', priority||'low', approval||'no', remarks||'', enforcedClientId, url||null, doerWillSet ? 1 : 0]);
      // 📧 Send delegation email + 📱 WhatsApp (non-blocking — fire and forget)
      (async () => {
        const target = await getNotifyTarget(targetUser);
        const [aprRows] = await db.query('SELECT name FROM users WHERE id=? LIMIT 1', [assignedBy]);
        const assignerName = aprRows[0]?.name || 'Admin';
        if (target) {
          await sendMail(
            target.email,
            `📋 New Task Assigned: ${(desc||'').slice(0,60)}`,
            delegationEmailHtml({
              assigneeName: target.name,
              assignerName,
              desc, dueDate: doerWillSet ? 'Aap set karein' : effectiveDate,
              priority: priority||'low',
              approval: approval||'no',
              remarks: remarks||''
            })
          );
        }
        // WhatsApp to doer
        try {
          const [[doerRow]] = await db.query('SELECT name, phone FROM users WHERE id=? LIMIT 1', [targetUser]);
          if (doerRow && doerRow.phone) {
            const dueFmt = doerWillSet
              ? 'Aap set karein 👉 task me due date daalein'
              : (effectiveDate||'').split('-').reverse().join('-') + (dueTime ? ` at ${dueTime.slice(0,5)}` : '');
            const msg = `Hello ${doerRow.name || ''},\n\n📋 *New Task Delegated*\n\n` +
              `*By:* ${assignerName}\n` +
              `*Due:* ${dueFmt}\n` +
              `*Priority:* ${(priority||'low').toUpperCase()}\n` +
              (approval==='yes' ? `*Approval Required:* Yes\n` : '') +
              `\n*Task:* ${desc}` +
              (remarks ? `\n\n*Remarks:* ${remarks}` : '') +
              `\n\n— E-Marketing Task Manager`;
            sendWhatsApp(doerRow.phone, msg).catch(e => console.error('WA delegation err:', e.message));
          }
        } catch (e) { console.error('WA delegation lookup err:', e.message); }
      })();
    } else {
      await db.query(`INSERT INTO checklist_tasks (description,assigned_to,assigned_by,due_date,status,priority,remarks,client_id) VALUES (?,?,?,?,?,?,?,?)`, [desc, targetUser, req.session.userId, effectiveDate, 'pending', priority||'low', remarks||'', enforcedClientId]);
    }
    res.json({ success: true, adjusted, effectiveDate, adjustedReason });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Doer sets the due date on a "doer-defines-date" delegation (applies directly,
// no approval). Assigner/admin can also set it as a fallback if the doer delays.
app.put('/api/tasks/:id/due-date', requireAuth, async (req, res) => {
  try {
    const { date } = req.body;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '')) return res.status(400).json({ error: 'Valid date (YYYY-MM-DD) required' });
    const uid = req.session.userId;
    const isPrivileged = req.session.role === 'admin' || req.session.role === 'pc';
    const [rows] = await db.query('SELECT * FROM delegation_tasks WHERE id=?', [parseInt(req.params.id, 10)]);
    const task = rows[0];
    if (!task) return res.status(404).json({ error: 'Task not found' });
    if (!task.awaiting_due_date) return res.status(409).json({ error: 'Due date is already set on this task' });
    // Doer, assigner, or admin/PC only.
    if (!isPrivileged && task.assigned_to !== uid && task.assigned_by !== uid) {
      return res.status(403).json({ error: 'Not allowed' });
    }
    // Nudge past a holiday / week-off, same as a normal delegation date.
    let effectiveDate = date;
    try {
      const holidaysSet = await loadHolidaysSet();
      const [[doerUser]] = await db.query('SELECT week_off, extra_off FROM users WHERE id=? LIMIT 1', [task.assigned_to]);
      if (doerUser && isUserOffOn(doerUser, date, holidaysSet)) effectiveDate = nextWorkingDay(doerUser, date, holidaysSet);
    } catch (e) { console.error('due-date holiday check err:', e.message); }
    await db.query('UPDATE delegation_tasks SET due_date=?, awaiting_due_date=0 WHERE id=?', [effectiveDate, task.id]);
    res.json({ success: true, effectiveDate });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Sub-tasks: follow-up asks nested under a delegation task (e.g. client says
// "make a dashboard" then later "change its color") instead of a brand-new task.
async function canTouchSubtasks(req, task) {
  if (!task) return false;
  const role = req.session.role;
  if (role === 'client') {
    const [[me]] = await db.query('SELECT client_id FROM users WHERE id=? LIMIT 1', [req.session.userId]);
    return !!me?.client_id && me.client_id === task.client_id;
  }
  return role === 'admin' || role === 'hod' || role === 'pc'
    || task.assigned_to === req.session.userId || task.assigned_by === req.session.userId;
}

app.get('/api/tasks/:id/subtasks', requireAuth, async (req, res) => {
  try {
    const taskId = parseInt(req.params.id, 10);
    const [[task]] = await db.query('SELECT id, assigned_to, assigned_by, client_id FROM delegation_tasks WHERE id=?', [taskId]);
    if (!task) return res.status(404).json({ error: 'Task not found' });
    if (!(await canTouchSubtasks(req, task))) return res.status(403).json({ error: 'Not allowed' });
    const [subtasks] = await db.query(
      `SELECT s.id, s.description, s.status, COALESCE(s.priority,'low') AS priority,
              DATE_FORMAT(s.created_at,'%Y-%m-%d') AS created_at,
              DATE_FORMAT(s.completed_at,'%Y-%m-%d') AS completed_at, u.name AS createdByName
       FROM task_subtasks s JOIN users u ON s.created_by = u.id
       WHERE s.task_id=? ORDER BY s.created_at ASC`, [taskId]);
    res.json({ subtasks });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/tasks/:id/subtasks', requireAuth, async (req, res) => {
  try {
    // Only the client can add sub-tasks — staff (handler/admin/hod/pc) can view,
    // complete, and delete them, but adding is the client's follow-up channel only.
    if (req.session.role !== 'client') return res.status(403).json({ error: 'Not allowed' });
    const taskId = parseInt(req.params.id, 10);
    const desc = (req.body.description || '').trim();
    if (!desc) return res.status(400).json({ error: 'Description required' });
    const priority = ['low','medium','high','urgent'].includes(req.body.priority) ? req.body.priority : 'low';
    const [[task]] = await db.query('SELECT id, assigned_to, assigned_by, client_id, description FROM delegation_tasks WHERE id=?', [taskId]);
    if (!task) return res.status(404).json({ error: 'Task not found' });
    if (!(await canTouchSubtasks(req, task))) return res.status(403).json({ error: 'Not allowed' });
    await db.query('INSERT INTO task_subtasks (task_id, description, priority, created_by) VALUES (?,?,?,?)', [taskId, desc, priority, req.session.userId]);

    // 📱 WhatsApp to the handler — non-blocking (fire and forget).
    (async () => {
      const [[client]] = await db.query('SELECT name FROM users WHERE id=? LIMIT 1', [req.session.userId]);
      const [[handler]] = await db.query('SELECT name, phone FROM users WHERE id=? LIMIT 1', [task.assigned_to]);
      if (handler?.phone) {
        const msg = `Hello ${handler.name || ''},\n\n🧩 *New Sub-task Added*\n\n` +
          `*By:* ${client?.name || 'Client'} (Client)\n` +
          `*Under Task:* ${task.description}\n\n` +
          `*Sub-task:* ${desc}\n\n` +
          `— E-Marketing Task Manager`;
        sendWhatsApp(handler.phone, msg).catch(e => console.error('WA subtask err:', e.message));
      }
    })().catch(e => console.error('WA subtask lookup err:', e.message));

    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/subtasks/:id', requireAuth, async (req, res) => {
  try {
    // Client can add sub-tasks but only the doer/assigner/admin/hod/pc mark them done.
    if (req.session.role === 'client') return res.status(403).json({ error: 'Not allowed' });
    const id = parseInt(req.params.id, 10);
    const status = req.body.status === 'completed' ? 'completed' : 'pending';
    const [[sub]] = await db.query('SELECT task_id FROM task_subtasks WHERE id=?', [id]);
    if (!sub) return res.status(404).json({ error: 'Sub-task not found' });
    const [[task]] = await db.query('SELECT id, assigned_to, assigned_by, client_id FROM delegation_tasks WHERE id=?', [sub.task_id]);
    if (!(await canTouchSubtasks(req, task))) return res.status(403).json({ error: 'Not allowed' });
    await db.query(`UPDATE task_subtasks SET status=?, completed_at=IF(?='completed', NOW(), NULL) WHERE id=?`, [status, status, id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/subtasks/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const [[sub]] = await db.query('SELECT * FROM task_subtasks WHERE id=?', [id]);
    if (!sub) return res.status(404).json({ error: 'Sub-task not found' });
    const isPrivileged = req.session.role === 'admin' || req.session.role === 'pc';
    if (sub.created_by !== req.session.userId && !isPrivileged) return res.status(403).json({ error: 'Not allowed' });
    await archiveDeleted('task_subtasks', sub, req, { summary: r => `Sub-task: ${r.description || ''}` });
    await db.query('DELETE FROM task_subtasks WHERE id=?', [id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/tasks/bulk-checklist', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { desc, assignedTo, priority, remarks, dates, client_id, clientId } = req.body;
    if (!desc || !assignedTo || !dates || !dates.length) return res.status(400).json({ error: 'Missing fields' });
    const cidRaw = client_id != null ? client_id : clientId;
    const cid = (() => { const n = parseInt(cidRaw, 10); return Number.isFinite(n) && n > 0 ? n : null; })();

    // Filter out holiday + week-off dates for this user
    let skippedCount = 0;
    try {
      const holidaysSet = await loadHolidaysSet();
      const [[doerUser]] = await db.query('SELECT week_off, extra_off FROM users WHERE id=? LIMIT 1', [parseInt(assignedTo)]);
      if (doerUser) {
        const filtered = dates.filter(d => !isUserOffOn(doerUser, d, holidaysSet));
        skippedCount = dates.length - filtered.length;
        if (!filtered.length) return res.json({ success: true, count: 0, skipped: skippedCount, message: 'All dates were holidays / week-offs — nothing inserted' });
        dates.length = 0;
        dates.push(...filtered);
      }
    } catch (e) { console.error('bulk-checklist holiday filter err:', e.message); }

    const values = dates.map(date => [desc, parseInt(assignedTo), req.session.userId, date, 'pending', priority||'low', remarks||'', cid]);
    await db.query(`INSERT INTO checklist_tasks (description,assigned_to,assigned_by,due_date,status,priority,remarks,client_id) VALUES ?`, [values]);
    res.json({ success: true, count: dates.length, skipped: skippedCount });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/tasks/:id/status', requireAuth, async (req, res) => {
  try {
    const { status, type, newDate, reason } = req.body;
    const table = getTable(type||'delegation');
    const isAdmin = req.session.role === 'admin';
    const isPC = req.session.role === 'pc';
    const uid = req.session.userId;
    const isPrivileged = isAdmin || isPC;
    const tt = type || 'delegation';
    const [rows] = await db.query(`SELECT * FROM ${table} WHERE id=?`, [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Task not found' });
    const task = rows[0];
    if (!isPrivileged && task.assigned_to !== uid) return res.status(403).json({ error: 'Not allowed' });

    // Approval workflow only exists for delegation tasks (checklist has no approval column).
    const supportsApproval = tt === 'delegation';
    // The reviser is also the one who assigned the task → no separate approval needed.
    const reviserIsAssigner = Number(task.assigned_by) === Number(uid);
    // Client-delegated task: the assigner is a client login (no approvals screen),
    // so a doer's revise applies directly — no approval step.
    let assignerIsClient = false;
    try {
      const [[asg]] = await db.query('SELECT role, client_id FROM users WHERE id=? LIMIT 1', [task.assigned_by]);
      assignerIsClient = !!asg && (asg.role === 'client' || asg.client_id != null);
    } catch (e) {}

    // While a revise/approval is pending, the doer can neither revise again nor mark done.
    // Privileged users (admin/PC) act directly; the assigner decides via the Approvals screen.
    if (supportsApproval && task.waiting_approval && !isPrivileged) {
      return res.status(409).json({ error: 'Approval is pending — you cannot revise or mark done until it is approved.' });
    }

    // REVISE (date push) ALWAYS needs the assigner's approval — for every role,
    // including admin and self-assigned tasks (the request just routes back to the
    // assigner, who approves it on the Approvals screen). The requested new date is
    // held in the approval row and applied to the task only once approved. Anyone
    // who can directly change a date should use Edit, not Revise.
    if (status === 'revised' && supportsApproval) {
      // Client-delegated → apply the revise directly (push the new date), no approval.
      if (assignerIsClient) {
        if (newDate) await db.query(`UPDATE ${table} SET status='revised', waiting_approval=0, due_date=? WHERE id=?`, [newDate, req.params.id]);
        else         await db.query(`UPDATE ${table} SET status='revised', waiting_approval=0 WHERE id=?`, [req.params.id]);
        return res.json({ success: true, applied: true });
      }
      await db.query(
        `INSERT INTO task_approvals (task_id,task_type,requested_by,requested_to,action_type,new_date,status,note)
         VALUES (?,?,?,?,?,?,'pending',?)`,
        [req.params.id, tt, uid, task.assigned_by, 'revised', newDate || null, reason || '']
      );
      await db.query(`UPDATE ${table} SET waiting_approval=1 WHERE id=?`, [req.params.id]);
      return res.json({ success: true, needsApproval: true });
    }

    // COMPLETION approval — only when the task was created with approval='yes'.
    if (status === 'completed' && supportsApproval && task.approval === 'yes' && !isPrivileged && !reviserIsAssigner) {
      await db.query(
        `INSERT INTO task_approvals (task_id,task_type,requested_by,requested_to,action_type,status,note)
         VALUES (?,?,?,?,?,'pending',?)`,
        [req.params.id, tt, uid, task.assigned_by, 'completed', reason || '']
      );
      await db.query(`UPDATE ${table} SET waiting_approval=1 WHERE id=?`, [req.params.id]);
      return res.json({ success: true, needsApproval: true });
    }

    // Direct apply: privileged user, self-assigner, plain completion, or checklist.
    // If a privileged user overrides while a request was pending, cancel the stale one.
    if (supportsApproval && task.waiting_approval && isPrivileged) {
      await db.query(`DELETE FROM task_approvals WHERE task_id=? AND task_type=? AND status='pending'`, [req.params.id, tt]);
    }
    if (newDate && status === 'revised') {
      if (tt === 'checklist') await db.query(`UPDATE ${table} SET status=?,due_date=? WHERE id=?`, [status, newDate, req.params.id]);
      else await db.query(`UPDATE ${table} SET status=?,waiting_approval=0,due_date=? WHERE id=?`, [status, newDate, req.params.id]);
    } else {
      // checklist_tasks has no waiting_approval column
      if (tt === 'checklist') await db.query(`UPDATE ${table} SET status=? WHERE id=?`, [status, req.params.id]);
      else await db.query(`UPDATE ${table} SET status=?,waiting_approval=0 WHERE id=?`, [status, req.params.id]);
    }
    res.json({ success: true, needsApproval: false });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/tasks/:id/detail', requireAuth, async (req, res) => {
  try {
    const { type } = req.query;
    const table = getTable(type||'delegation');
    const [rows] = await db.query(`SELECT t.*,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date FROM ${table} t WHERE t.id=?`, [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Task not found' });
    // Admin/HOD see anything; otherwise only the assigner can pull a task's detail (needed for self-edit).
    const role = req.session.role;
    if (role !== 'admin' && role !== 'hod' && role !== 'pc') {
      if (Number(rows[0].assigned_by) !== Number(req.session.userId) && Number(rows[0].assigned_to) !== Number(req.session.userId)) {
        return res.status(403).json({ error: 'Not allowed' });
      }
    }
    res.json({ task: rows[0] });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Allow edit/delete if user is admin/hod OR if they are the task's assigner.
// This covers the "user delegated a task (incl. self-delegation) → can edit/delete" case.
async function canModifyTask(req, taskId, type) {
  if (req.session.role === 'admin' || req.session.role === 'hod') return true;
  const [rows] = await db.query(
    `SELECT assigned_by FROM ${getTable(type||'delegation')} WHERE id=?`, [taskId]
  );
  return !!rows[0] && Number(rows[0].assigned_by) === Number(req.session.userId);
}

app.put('/api/tasks/:id/edit', requireAuth, async (req, res) => {
  try {
    const { type, desc, date, priority, approval, remarks, url, client_id, clientId } = req.body;
    if (!await canModifyTask(req, req.params.id, type)) return res.status(403).json({ error: 'Not allowed to edit this task' });
    const table = getTable(type||'delegation');
    const cidRaw = client_id != null ? client_id : clientId;
    const cid = (() => { const n = parseInt(cidRaw, 10); return Number.isFinite(n) && n > 0 ? n : null; })();
    if (type === 'delegation') await db.query(`UPDATE ${table} SET description=?,due_date=?,priority=?,approval=?,remarks=?,url=?,client_id=? WHERE id=?`, [desc, date, priority||'low', approval||'no', remarks||'', url||null, cid, req.params.id]);
    else await db.query(`UPDATE ${table} SET description=?,due_date=?,remarks=?,client_id=? WHERE id=?`, [desc, date, remarks||'', cid, req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/tasks/:id', requireAuth, async (req, res) => {
  try {
    const { type } = req.query;
    if (!await canModifyTask(req, req.params.id, type)) return res.status(403).json({ error: 'Not allowed to delete this task' });
    const tbl = getTable(type||'delegation');
    const [rows] = await db.query(`SELECT * FROM ${tbl} WHERE id=?`, [req.params.id]);
    await archiveDeleted(tbl, rows, req, { summary: r => `Task: ${r.description || ''}` });
    await db.query(`DELETE FROM ${tbl} WHERE id=?`, [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete every checklist row that shares description + assigned_to with the given
// task and has due_date >= today. Use this to nuke a recurring "series" so it
// doesn't keep reappearing day after day.
app.delete('/api/tasks/:id/checklist-series', requireAuth, async (req, res) => {
  try {
    if (!await canModifyTask(req, req.params.id, 'checklist')) return res.status(403).json({ error: 'Not allowed' });
    const [[task]] = await db.query('SELECT description, assigned_to FROM checklist_tasks WHERE id=?', [req.params.id]);
    if (!task) return res.status(404).json({ error: 'Task not found' });
    const includePast = req.query.includePast === '1';
    const dateClause = includePast ? '' : ' AND due_date >= CURDATE()';
    const [doomed] = await db.query(
      `SELECT * FROM checklist_tasks WHERE description=? AND assigned_to=?${dateClause}`,
      [task.description, task.assigned_to]);
    await archiveDeleted('checklist_tasks', doomed, req, {
      summary: r => `Checklist series: ${r.description || ''}`,
      reason: 'Recurring checklist series deleted',
    });
    const [result] = await db.query(
      `DELETE FROM checklist_tasks WHERE description=? AND assigned_to=?${dateClause}`,
      [task.description, task.assigned_to]
    );
    res.json({ success: true, deleted: result.affectedRows || 0 });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk delete by user
app.delete('/api/tasks/user/:userId', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { type } = req.query;
    const table = getTable(type || 'delegation');
    const [doomed] = await db.query(`SELECT * FROM ${table} WHERE assigned_to = ?`, [req.params.userId]);
    await archiveDeleted(table, doomed, req, {
      summary: r => `Task: ${r.description || ''}`,
      reason: `Bulk delete of all ${table} rows for user ${req.params.userId}`,
    });
    await db.query(`DELETE FROM ${table} WHERE assigned_to = ?`, [req.params.userId]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Transfer pending tasks to today
app.put('/api/tasks/user/:userId/transfer-today', requireAuth, requireAdmin, async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const { type } = req.query;
    const table = getTable(type || 'delegation');
    await db.query(`UPDATE ${table} SET due_date=? WHERE assigned_to=? AND status='pending'`,
      [today, req.params.userId]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/tasks/delete-by-date', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { date } = req.body;
    if (!date) return res.status(400).json({ error: 'Date required' });
    const [doomed] = await db.query('SELECT * FROM checklist_tasks WHERE due_date=?', [date]);
    await archiveDeleted('checklist_tasks', doomed, req, {
      summary: r => `Checklist: ${r.description || ''}`,
      reason: `Bulk delete of all checklist tasks due ${date}`,
    });
    const [result] = await db.query('DELETE FROM checklist_tasks WHERE due_date=?', [date]);
    res.json({ success: true, deleted: result.affectedRows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Count checklist tasks for a user (all time or by year)
app.get('/api/tasks/checklist-year-count', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { userId, year } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    let rows;
    if (!year || year === 'all') {
      [rows] = await db.query(`SELECT COUNT(*) AS count FROM checklist_tasks WHERE assigned_to=?`, [userId]);
    } else {
      [rows] = await db.query(`SELECT COUNT(*) AS count FROM checklist_tasks WHERE assigned_to=? AND YEAR(due_date)=?`, [userId, year]);
    }
    res.json({ count: rows[0].count });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete all checklist tasks for a user (POST used to avoid body parse issues with DELETE)
app.post('/api/tasks/checklist-year-delete', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const [doomed] = await db.query(`SELECT * FROM checklist_tasks WHERE assigned_to=?`, [userId]);
    await archiveDeleted('checklist_tasks', doomed, req, {
      summary: r => `Checklist: ${r.description || ''}`,
      reason: `Bulk delete of all checklist tasks for user ${userId}`,
    });
    const [result] = await db.query(`DELETE FROM checklist_tasks WHERE assigned_to=?`, [userId]);
    res.json({ success: true, deleted: result.affectedRows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

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
    await db.query('UPDATE task_approvals SET status=?,note=? WHERE id=?', [action, note||'', req.params.id]);
    const table = getTable(appr.task_type);
    if (action === 'approved') {
      // Revise approved → push the held new date now. Other actions just set status.
      if (appr.action_type === 'revised' && appr.new_date_fmt) {
        await db.query(`UPDATE ${table} SET status='pending',waiting_approval=0,due_date=? WHERE id=?`, [appr.new_date_fmt, appr.task_id]);
      } else if (appr.action_type === 'revised') {
        await db.query(`UPDATE ${table} SET status='pending',waiting_approval=0 WHERE id=?`, [appr.task_id]);
      } else {
        await db.query(`UPDATE ${table} SET status=?,waiting_approval=0 WHERE id=?`, [appr.action_type, appr.task_id]);
      }
    } else {
      // Rejected → drop the waiting flag; due_date and status stay unchanged.
      await db.query(`UPDATE ${table} SET waiting_approval=0 WHERE id=?`, [appr.task_id]);
    }
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
      if (a.nd) await db.query(`UPDATE ${table} SET status='pending', waiting_approval=0, due_date=? WHERE id=?`, [a.nd, a.task_id]);
      else      await db.query(`UPDATE ${table} SET status='pending', waiting_approval=0 WHERE id=?`, [a.task_id]);
      await db.query(`UPDATE task_approvals SET status='approved' WHERE id=?`, [a.id]);
      approved++;
    }
    res.json({ ok: true, approved });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// MIS
// ══════════════════════════════════════════════════════
app.get('/api/mis', requireAuth, requireAdminOrHodOnly, async (req, res) => {
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

    for (const sheet of fmsList) {
      const fmsName = sheet.fms_name || sheet.sheet_name;

      // Get steps for this FMS that are assigned to targetUserIds
      let steps;
      if (isAdmin && (!filterEmployee || filterEmployee === 'all')) {
        [steps] = await db.query('SELECT * FROM fms_steps WHERE fms_id=? ORDER BY step_order ASC', [sheet.id]);
      } else {
        [steps] = await db.query(
          `SELECT DISTINCT fst.* FROM fms_steps fst
           JOIN fms_step_doers fsd ON fsd.step_id=fst.id
           WHERE fst.fms_id=? AND fsd.user_id IN (${targetUserIds.map(()=>'?').join(',')})
           ORDER BY fst.step_order ASC`, [sheet.id, ...targetUserIds]);
      }
      if (!steps.length) continue;

      // Get doer names for each step
      for (const step of steps) {
        const [doers] = await db.query(
          `SELECT u.id, u.name FROM fms_step_doers fsd JOIN users u ON fsd.user_id=u.id WHERE fsd.step_id=?`, [step.id]);
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

app.get('/api/mis/detail', requireAuth, requireAdminOrHodOnly, async (req, res) => {
  try {
    const { userId, type, start, end } = req.query;
    if (!userId || !start || !end) return res.status(400).json({ error: 'Missing params' });
    const table = type === 'delegation' ? 'delegation_tasks' : 'checklist_tasks';
    const [tasks] = await db.query(`SELECT t.id,t.description,t.status,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u2.name AS assigned_by_name FROM ${table} t JOIN users u2 ON t.assigned_by=u2.id WHERE t.assigned_to=? AND t.due_date BETWEEN ? AND ? ORDER BY t.due_date ASC`, [userId, start, end]);
    res.json({ tasks });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── All MIS — per employee combined score ──
app.get('/api/mis/all', requireAuth, requireAdminOrHodOnly, async (req, res) => {
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
          for (const sheet of allSheets) {
            const [steps] = await db.query('SELECT * FROM fms_steps WHERE fms_id=? ORDER BY step_order ASC', [sheet.id]);
            if (!steps.length) continue;
            // Doers per step
            for (const step of steps) {
              const [doers] = await db.query(
                `SELECT fsd.user_id FROM fms_step_doers fsd WHERE fsd.step_id=?`, [step.id]);
              step.doerIds = doers.map(d => d.user_id);
            }
            try {
              const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
              const tabName = sheet.sheet_name || 'Sheet1';
              const headerRowIdx = (sheet.header_row || 1) - 1;
              const allCols = steps.flatMap(s => [colToIdx(s.plan_col), colToIdx(s.actual_col)]).filter(x => x >= 0);
              if (!allCols.length) continue;
              const maxCol = Math.max(...allCols);
              const lastCol = idxToCol(maxCol);
              const range = `${tabName}!A:${lastCol}`;
              const response = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range });
              const allRowsData = response.data.values || [];
              const dataRows = allRowsData.slice(headerRowIdx + 1);
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
app.get('/api/mis/fms', requireAuth, requireAdminOrHodOnly, async (req, res) => {
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

    for (const sheet of sheets) {
      // Get all steps with doers
      const [steps] = await db.query('SELECT * FROM fms_steps WHERE fms_id=? ORDER BY step_order ASC', [sheet.id]);
      for (const step of steps) {
        const [doers] = await db.query(
          `SELECT fsd.user_id, u.name, u.department FROM fms_step_doers fsd
           JOIN users u ON fsd.user_id=u.id WHERE fsd.step_id=?`, [step.id]);
        step.doers = doers;
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
const EXTRA_ACCESS_KEYS = ['race','mis','fms','users','clients','compliance','dailyreports','leaves_all','pending_summary_recipient'];
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
    { key: 'dailyreports',              label: 'Daily Reports' },
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
    for (const r of rows) r.extra_access = parseExtraAccess(r.extra_access);
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
    const { name, email, notification_email, password, role, user_role, phone, department, week_off, extra_off, exclude_from_reminder, extra_access, birthday, joining_date } = req.body;
    if (!name || !email || !password) return res.status(400).json({ error: 'All fields required' });
    const [ex] = await db.query('SELECT id FROM users WHERE email=?', [email]);
    if (ex[0]) return res.status(400).json({ error: 'Email already exists' });
    const validRoles = ['admin','hod','pc','user'];
    const appRole = validRoles.includes(role) ? role : 'user';
    const userRole = validRoles.includes(user_role) ? user_role : appRole;
    const accessJson = JSON.stringify(sanitizeExtraAccess(extra_access));
    await db.query('INSERT INTO users (name,email,notification_email,password,role,user_role,phone,department,week_off,extra_off,exclude_from_reminder,extra_access,birthday,joining_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [name, email, notification_email||'', bcrypt.hashSync(password,10), appRole, userRole, phone||null, department||'', week_off||'', extra_off||'', exclude_from_reminder?1:0, accessJson, birthday||null, joining_date||null]);
    const waMsg = `Hi ${name},\nWelcome to e-marketing. We are granting you access to the our task manager.🌸\n\nhttps://taskmanager.e-marketing.io/app\nid : ${email}\npass : ${password}`;
    if (phone) sendWhatsApp(phone, waMsg).catch(e => console.error('WA new user err:', e.message));
    // Team welcome announcement
    const welcomeMsg = `Hello Team,\nPlease join me in welcoming ${name} our new team member who has joined us as a ${department || 'team member'}.\nWe are excited to have them on board and look forward to working together.\nWelcome to the team, ${name}! 🌸`;
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
const VALID_UP_PAGES   = new Set(['dashboard','alltasks','approvals','mis','race','fms','fms-tasks','daily','clients','compliance','dailyreports','leaves','meetings','inventory','hrm','users']);
const VALID_UP_ACTIONS = new Set(['edit_task','delete_task','create_task','create_checklist','approve_revision','bulk_approve','transfer_task','reopen_task','delete_leave','set_plan','hrm_schedule','hrm_update_status']);

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
    const [rows] = await db.query(`SELECT tc.id,tc.comment,tc.created_at,u.name AS userName FROM task_comments tc JOIN users u ON tc.user_id=u.id WHERE tc.task_id=? AND tc.task_type=? ORDER BY tc.created_at ASC`, [req.params.taskId, req.params.type]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/comments', requireAuth, async (req, res) => {
  try {
    const { taskId, taskType, comment } = req.body;
    if (!comment || !taskId || !taskType) return res.status(400).json({ error: 'All fields required' });
    await db.query('INSERT INTO task_comments (task_id,task_type,user_id,comment) VALUES (?,?,?,?)', [taskId, taskType, req.session.userId, comment]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/comments/:id', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query('SELECT * FROM task_comments WHERE id=?', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    if (rows[0].user_id !== req.session.userId && req.session.role !== 'admin') return res.status(403).json({ error: 'Not allowed' });
    await archiveDeleted('task_comments', rows[0], req, { summary: r => `Comment: ${r.comment || ''}` });
    await db.query('DELETE FROM task_comments WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// FMS ADMIN APIs
// ══════════════════════════════════════════════════════

app.get('/api/fms', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [sheets] = await db.query(`SELECT f.*,u.name AS createdByName FROM fms_sheets f JOIN users u ON f.created_by=u.id ORDER BY f.created_at DESC`);
    res.json(sheets);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// IMPORTANT: This route must be defined BEFORE /api/fms/:id
// to avoid being captured by the :id parameter wildcard.
// Get unique values from a specific column of a Google Sheet
// Used by FMS admin to auto-populate Step Doers from Doer Name column
// Query: ?sheetId=...&tabName=...&col=E&headerRow=1
app.get('/api/fms/sheet-column-values', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { sheetId, tabName, col, headerRow } = req.query;
    if (!sheetId || !col) return res.status(400).json({ error: 'sheetId and col required' });

    const colIdx = colToIdx(col);
    if (colIdx < 0) return res.status(400).json({ error: 'Invalid column letter' });

    const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const spreadsheetId = extractSpreadsheetId(sheetId);
    const tab = tabName || 'Sheet1';
    const headerIdx = (parseInt(headerRow) || 1) - 1;

    const range = `${tab}!${col}:${col}`;
    const response = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range });
    const values = response.data.values || [];

    // Skip header row(s), collect unique non-empty values
    const dataValues = values.slice(headerIdx + 1).map(r => (r[0] || '').trim()).filter(v => v);
    const uniqueNames = [...new Set(dataValues)];

    // Match each name with DB users (case-insensitive exact match)
    const [allUsers] = await db.query('SELECT id, name, email, role FROM users');
    const matched = [];
    const unmatched = [];
    for (const sheetName of uniqueNames) {
      const user = allUsers.find(u => u.name.trim().toLowerCase() === sheetName.toLowerCase());
      if (user) {
        matched.push({ sheet_name: sheetName, user_id: user.id, user_name: user.name, email: user.email });
      } else {
        unmatched.push(sheetName);
      }
    }

    res.json({
      total_unique: uniqueNames.length,
      matched_count: matched.length,
      unmatched_count: unmatched.length,
      matched,
      unmatched,
      all_unique: uniqueNames
    });
  } catch (err) {
    if (err.code === 403) return res.status(400).json({ error: 'Sheet access denied. Share with service account.' });
    if (err.code === 404) return res.status(400).json({ error: 'Sheet not found.' });
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/fms/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [sheets] = await db.query('SELECT * FROM fms_sheets WHERE id=?', [req.params.id]);
    if (!sheets[0]) return res.status(404).json({ error: 'FMS not found' });
    const [steps] = await db.query('SELECT * FROM fms_steps WHERE fms_id=? ORDER BY step_order ASC', [req.params.id]);
    for (const step of steps) {
      const [doers] = await db.query(`SELECT fsd.user_id,u.name FROM fms_step_doers fsd JOIN users u ON fsd.user_id=u.id WHERE fsd.step_id=?`, [step.id]);
      step.doers = doers;
      const [extraRows] = await db.query('SELECT * FROM fms_extra_rows WHERE step_id=? ORDER BY id ASC', [step.id]);
      step.extraRows = extraRows;
      try { step.show_cols_parsed = JSON.parse(step.show_cols || '[]'); } catch(e) { step.show_cols_parsed = []; }
    }
    res.json({ sheet: sheets[0], steps });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/fms', requireAuth, requireAdmin, async (req, res) => {
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();
    const { fmsName, sheetName, sheetId, headerRow, totalSteps, steps } = req.body;
    const [result] = await conn.query(
      `INSERT INTO fms_sheets (fms_name,sheet_name,sheet_id,header_row,total_steps,created_by) VALUES (?,?,?,?,?,?)`,
      [fmsName||sheetName, sheetName, sheetId, headerRow||1, totalSteps||1, req.session.userId]
    );
    const fmsId = result.insertId;
    for (let i = 0; i < steps.length; i++) {
      const s = steps[i];
      const [sr] = await conn.query(
        `INSERT INTO fms_steps (fms_id,step_order,step_name,plan_col,actual_col,extra_input,extra_col,show_cols,delay_reason_col,doer_name_col) VALUES (?,?,?,?,?,?,?,?,?,?)`,
        [fmsId,i+1,s.stepName,s.planCol||'',s.actualCol||'',s.extraInput||'no',s.extraCol||'',JSON.stringify(s.showCols||[]),s.delayReasonCol||'',s.doerNameCol||'']
      );
      const stepId = sr.insertId;
      if (s.doers?.length) for (const uid of s.doers) await conn.query('INSERT INTO fms_step_doers (step_id,user_id) VALUES (?,?)', [stepId, uid]);
      if (s.extraInput==='yes' && s.extraRows?.length) for (const row of s.extraRows) await conn.query('INSERT INTO fms_extra_rows (step_id,row_label,col_letter,field_type,dropdown_options,required) VALUES (?,?,?,?,?,?)', [stepId, row.label||row.col_letter||'', row.col_letter||'', row.field_type||'text', row.dropdown_options||'', row.required===false||row.required===0?0:1]);
    }
    await conn.commit();
    res.json({ success: true, id: fmsId });
  } catch (err) { await conn.rollback(); res.status(500).json({ error: err.message }); } finally { conn.release(); }
});

app.put('/api/fms/:id', requireAuth, requireAdmin, async (req, res) => {
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();
    const { fmsName, sheetName, sheetId, headerRow, steps } = req.body;
    await conn.query(`UPDATE fms_sheets SET fms_name=?,sheet_name=?,sheet_id=?,header_row=?,total_steps=? WHERE id=?`, [fmsName||sheetName, sheetName, sheetId, headerRow||1, steps.length, req.params.id]);
    const [oldSteps] = await conn.query('SELECT id FROM fms_steps WHERE fms_id=?', [req.params.id]);
    for (const os of oldSteps) {
      await conn.query('DELETE FROM fms_step_doers WHERE step_id=?', [os.id]);
      await conn.query('DELETE FROM fms_extra_rows WHERE step_id=?', [os.id]);
    }
    await conn.query('DELETE FROM fms_steps WHERE fms_id=?', [req.params.id]);
    for (let i=0; i<steps.length; i++) {
      const s = steps[i];
      const [sr] = await conn.query(
        `INSERT INTO fms_steps (fms_id,step_order,step_name,plan_col,actual_col,extra_input,extra_col,show_cols,delay_reason_col,doer_name_col) VALUES (?,?,?,?,?,?,?,?,?,?)`,
        [req.params.id,i+1,s.stepName,s.planCol||'',s.actualCol||'',s.extraInput||'no',s.extraCol||'',JSON.stringify(s.showCols||[]),s.delayReasonCol||'',s.doerNameCol||'']
      );
      const stepId = sr.insertId;
      if (s.doers?.length) for (const uid of s.doers) await conn.query('INSERT INTO fms_step_doers (step_id,user_id) VALUES (?,?)', [stepId, uid]);
      if (s.extraInput==='yes' && s.extraRows?.length) for (const row of s.extraRows) await conn.query('INSERT INTO fms_extra_rows (step_id,row_label,col_letter,field_type,dropdown_options,required) VALUES (?,?,?,?,?,?)', [stepId, row.label||row.col_letter||'', row.col_letter||'', row.field_type||'text', row.dropdown_options||'', row.required===false||row.required===0?0:1]);
    }
    await conn.commit();
    res.json({ success: true });
  } catch (err) { await conn.rollback(); res.status(500).json({ error: err.message }); } finally { conn.release(); }
});

app.delete('/api/fms/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [doomed] = await db.query('SELECT * FROM fms_sheets WHERE id=?', [req.params.id]);
    await archiveDeleted('fms_sheets', doomed, req, { summary: r => `FMS sheet: ${r.sheet_name || ''}` });
    await db.query('DELETE FROM fms_sheets WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Fetch headers ONLY (fast — just one row from sheet) ──
app.post('/api/fms/fetch-headers', requireAuth, async (req, res) => {
  try {
    const { sheetId, sheetName, headerRow } = req.body;
    if (!sheetId) return res.status(400).json({ error: 'sheetId required' });
    const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const spreadsheetId = extractSpreadsheetId(sheetId);
    const hRow = parseInt(headerRow) || 1;
    // Fetch ONLY the header row — very fast even for 10000-row sheets
    const range = sheetName ? `${sheetName}!${hRow}:${hRow}` : `${hRow}:${hRow}`;
    const response = await sheetsApi.spreadsheets.values.get({
      spreadsheetId, range,
      majorDimension: 'ROWS',
      valueRenderOption: 'UNFORMATTED_VALUE'
    });
    const rawHeaders = (response.data.values || [[]])[0] || [];
    const headers = rawHeaders
      .map((h, i) => ({
        name: String(h ?? '').trim() || `COL_${idxToCol(i)}`,
        col: idxToCol(i),
        index: i
      }))
      .filter(h => String(h.name).trim().length > 0);
    res.json({ headers });
  } catch (err) {
    if (err.code === 403) return res.status(400).json({ error: 'Access denied. Share sheet with service account.' });
    if (err.code === 404) return res.status(400).json({ error: 'Sheet not found. Check Sheet ID.' });
    res.status(500).json({ error: err.message });
  }
});

// ── Sync data (full) — FIX: now uses sheet.sheet_name as tab name ──
app.get('/api/fms/:id/sync', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [sheets] = await db.query('SELECT * FROM fms_sheets WHERE id=?', [req.params.id]);
    if (!sheets[0]) return res.status(404).json({ error: 'FMS not found' });
    const sheet = sheets[0];
    const headerRowIdx = (sheet.header_row || 1) - 1;
    const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
    // ✅ FIXED: use sheet.sheet_name (actual tab name) instead of hardcoded 'Sheet1'
    const tabName = sheet.sheet_name || 'Sheet1';
    const response = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range: tabName });
    const allRows = response.data.values || [];
    if (allRows.length <= headerRowIdx) {
      return res.status(400).json({ error: `Sheet has only ${allRows.length} rows but header row is set to ${sheet.header_row}` });
    }
    const headers = allRows[headerRowIdx].filter(h => h && h.trim());
    const dataRows = allRows.slice(headerRowIdx + 1);
    // Return ALL data rows
    res.json({ success: true, headers, totalRows: dataRows.length, headerRow: sheet.header_row, sample: dataRows });
  } catch (err) {
    if (err.message?.includes('ENOENT') || err.message?.includes('credentials')) return res.status(500).json({ error: 'credentials.json not found.' });
    if (err.code === 403) return res.status(400).json({ error: 'Access denied. Share sheet with service account.' });
    if (err.code === 404) return res.status(400).json({ error: 'Sheet not found. Check Sheet ID.' });
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════
// FMS TASKS APIs (all users)
// ══════════════════════════════════════════════════════

// List FMS visible to user
app.get('/api/fms-tasks', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const isAdmin = req.session.role === 'admin';
    let list;
    if (isAdmin) {
      [list] = await db.query('SELECT * FROM fms_sheets ORDER BY created_at DESC');
    } else {
      [list] = await db.query(`SELECT DISTINCT fs.* FROM fms_sheets fs JOIN fms_steps fst ON fst.fms_id=fs.id JOIN fms_step_doers fsd ON fsd.step_id=fst.id WHERE fsd.user_id=? ORDER BY fs.created_at DESC`, [uid]);
    }
    res.json(list);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Get FMS steps for tasks view
app.get('/api/fms-tasks/:id', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const isAdmin = req.session.role === 'admin';
    const [sheets] = await db.query('SELECT * FROM fms_sheets WHERE id=?', [req.params.id]);
    if (!sheets[0]) return res.status(404).json({ error: 'FMS not found' });
    const [steps] = await db.query('SELECT * FROM fms_steps WHERE fms_id=? ORDER BY step_order ASC', [req.params.id]);
    for (const step of steps) {
      const [doers] = await db.query(`SELECT fsd.user_id,u.name FROM fms_step_doers fsd JOIN users u ON fsd.user_id=u.id WHERE fsd.step_id=?`, [step.id]);
      step.doers = doers;
      step.isMyStep = isAdmin || doers.some(d => d.user_id === uid);
      try { step.show_cols_parsed = JSON.parse(step.show_cols||'[]'); } catch(e) { step.show_cols_parsed = []; }
      const [extraRows] = await db.query('SELECT * FROM fms_extra_rows WHERE step_id=? ORDER BY id ASC', [step.id]);
      step.extraRows = extraRows;
    }
    res.json({ sheet: sheets[0], steps });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Get pending rows for a step (plan filled, actual empty)
app.get('/api/fms-tasks/:fmsId/steps/:stepId/rows', requireAuth, async (req, res) => {
  try {
    const isAdmin = req.session.role === 'admin';
    const [sheets] = await db.query('SELECT * FROM fms_sheets WHERE id=?', [req.params.fmsId]);
    if (!sheets[0]) return res.status(404).json({ error: 'FMS not found' });
    const sheet = sheets[0];
    const [steps] = await db.query('SELECT * FROM fms_steps WHERE id=? AND fms_id=?', [req.params.stepId, req.params.fmsId]);
    if (!steps[0]) return res.status(404).json({ error: 'Step not found' });
    const step = steps[0];

    // Get current user's name for doer filtering
    const [[currentUser]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
    const myName = (currentUser?.name || '').trim().toLowerCase();

    const planIdx = colToIdx(step.plan_col);
    const actualIdx = colToIdx(step.actual_col);
    const doerNameIdx = step.doer_name_col ? colToIdx(step.doer_name_col) : -1;
    let showCols = [];
    try { showCols = JSON.parse(step.show_cols||'[]'); } catch(e) {}

    const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
    const tabName = sheet.sheet_name || 'Sheet1';

    // Optimized: fetch only up to the furthest needed column (include doerNameIdx if set)
    const maxIdx = Math.max(planIdx, actualIdx, doerNameIdx, ...(showCols.length ? showCols : [0]));
    const lastCol = maxIdx >= 0 ? idxToCol(maxIdx) : 'Z';
    const range = `${tabName}!A:${lastCol}`;

    const response = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range });
    const allRows = response.data.values || [];
    const headerRowIdx = (sheet.header_row || 1) - 1;
    const headers = allRows[headerRowIdx] || [];
    const dataRows = allRows.slice(headerRowIdx + 1);

    // Doer filtering: non-admins see only their rows; admins see all
    const applyDoerFilter = !isAdmin && doerNameIdx >= 0 && myName;

    const matchedRows = [];
    let totalPending = 0;       // total pending in this step (for admin info)
    let assignedToMe = 0;       // assigned to current user
    const blankClean = v => (v || '').toString().replace(/[\s ​-‍﻿]+/g, '');
    dataRows.forEach((row, i) => {
      const planVal = planIdx >= 0 ? (row[planIdx]||'').trim() : '';
      const actualVal = actualIdx >= 0 ? (row[actualIdx]||'').trim() : '';
      if (!blankClean(planVal) || blankClean(actualVal)) return; // skip non-pending rows
      totalPending++;

      // Check doer name match (case-insensitive exact)
      const rowDoer = doerNameIdx >= 0 ? (row[doerNameIdx]||'').trim() : '';
      const rowDoerLower = rowDoer.toLowerCase();
      const isMine = rowDoerLower === myName;
      if (isMine) assignedToMe++;

      // For non-admin, skip rows that don't belong to them
      if (applyDoerFilter && !isMine) return;

      const rowData = {};
      let colsToShow = showCols.length ? showCols : headers.map((_,hi) => hi);
      // Plan column is always shown — mandatory
      if (planIdx >= 0 && !colsToShow.includes(planIdx)) colsToShow = [planIdx, ...colsToShow];
      colsToShow.forEach(ci => {
        const h = headers[ci] || `COL ${idxToCol(ci)}`;
        rowData[h] = row[ci] || '';
      });
      matchedRows.push({
        sheetRowNumber: headerRowIdx + 1 + i + 1,
        planValue: planVal,
        actualValue: actualVal,
        rowDoerName: rowDoer,
        isMine,
        data: rowData
      });
    });

    res.json({
      rows: matchedRows,
      headers,
      total: matchedRows.length,
      totalPending,
      assignedToMe,
      filtered: applyDoerFilter,
      doerColumn: step.doer_name_col || null,
      isAdmin
    });
  } catch (err) {
    if (err.code === 403) return res.status(400).json({ error: 'Access denied.' });
    if (err.code === 404) return res.status(400).json({ error: 'Sheet not found.' });
    res.status(500).json({ error: err.message });
  }
});

// Mark row as done — writes actual (full timestamp) + delay reason to sheet
app.post('/api/fms-tasks/:fmsId/steps/:stepId/done', requireAuth, async (req, res) => {
  try {
    const { rowNumber, actualValue, delayReason, extraInputs } = req.body;
    if (!rowNumber || !actualValue) return res.status(400).json({ error: 'rowNumber and actualValue required' });

    // Build full timestamp in IST: DD/MM/YYYY HH:mm:ss
    const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
    const pad = n => String(n).padStart(2, '0');
    const fullTimestamp = `${pad(istNow.getUTCDate())}/${pad(istNow.getUTCMonth()+1)}/${istNow.getUTCFullYear()} ${pad(istNow.getUTCHours())}:${pad(istNow.getUTCMinutes())}:${pad(istNow.getUTCSeconds())}`;

    const [sheets] = await db.query('SELECT * FROM fms_sheets WHERE id=?', [req.params.fmsId]);
    if (!sheets[0]) return res.status(404).json({ error: 'FMS not found' });
    const sheet = sheets[0];
    const [steps] = await db.query('SELECT * FROM fms_steps WHERE id=? AND fms_id=?', [req.params.stepId, req.params.fmsId]);
    if (!steps[0]) return res.status(404).json({ error: 'Step not found' });
    const step = steps[0];

    const actualCol = (step.actual_col||'').toUpperCase();
    if (!actualCol) return res.status(400).json({ error: 'Actual column not configured for this step' });

    const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets']);
    const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
    const tabName = sheet.sheet_name || 'Sheet1';

    await sheetsApi.spreadsheets.values.update({
      spreadsheetId,
      range: `${tabName}!${actualCol}${rowNumber}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[fullTimestamp]] }
    });

    if (delayReason && step.delay_reason_col) {
      const drCol = step.delay_reason_col.toUpperCase();
      await sheetsApi.spreadsheets.values.update({
        spreadsheetId,
        range: `${tabName}!${drCol}${rowNumber}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[delayReason]] }
      });
    }

    // Write extra input values to their respective columns
    if (extraInputs && extraInputs.length) {
      for (const ei of extraInputs) {
        if (ei.colLetter && ei.value !== undefined && ei.value !== '') {
          await sheetsApi.spreadsheets.values.update({
            spreadsheetId,
            range: `${tabName}!${ei.colLetter.toUpperCase()}${rowNumber}`,
            valueInputOption: 'USER_ENTERED',
            requestBody: { values: [[ei.value]] }
          });
        }
      }
    }

    // Write the doer's name into the sheet (if doer_name_col is configured)
    if (step.doer_name_col) {
      const [userRows] = await db.query('SELECT name FROM users WHERE id=? LIMIT 1', [req.session.userId]);
      const doerName = userRows[0]?.name || '';
      if (doerName) {
        await sheetsApi.spreadsheets.values.update({
          spreadsheetId,
          range: `${tabName}!${step.doer_name_col.toUpperCase()}${rowNumber}`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[doerName]] }
        });
      }
    }

    res.json({ success: true });
  } catch (err) {
    if (err.code === 403) return res.status(400).json({ error: 'Access denied. Sheet write permission needed.' });
    res.status(500).json({ error: err.message });
  }
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

    const uid = req.session.userId;
    const role = req.session.role;

    // Validate each task — user can only transfer their own, HOD dept, admin any
    for (const t of tasks) {
      const table = getTable(t.taskType);
      const [rows] = await db.query(`SELECT * FROM ${table} WHERE id=?`, [t.taskId]);
      if (!rows[0]) return res.status(404).json({ error: `Task ${t.taskId} not found` });
      const task = rows[0];

      if (role === 'user' && task.assigned_to !== uid)
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
    const { action, note } = req.body; // action: 'approved' | 'rejected'
    const [rows] = await db.query('SELECT * FROM task_transfers WHERE id=?', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Transfer not found' });
    const tr = rows[0];

    await db.query('UPDATE task_transfers SET status=?, note=? WHERE id=?', [action, note||'', req.params.id]);

    if (action === 'approved') {
      const table = getTable(tr.task_type);
      await db.query(`UPDATE ${table} SET assigned_to=? WHERE id=?`, [tr.to_user, tr.task_id]);
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
app.post('/api/week-plan', requireAuth, requireAdminOrHod, async (req, res) => {
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
app.get('/api/mis/fms-detail', requireAuth, requireAdminOrHodOnly, async (req, res) => {
  try {
    const { userId, start, end } = req.query;
    if (!userId || !start || !end || !/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)) {
      return res.status(400).json({ error: 'userId, start, end (YYYY-MM-DD) required' });
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
    description TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_cfb_client (client_id),
    KEY idx_cfb_employee (employee_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  await sa(`ALTER TABLE client_feedback ADD COLUMN recipients TEXT DEFAULT ''`);
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

  console.log('  ✅ Daily Task + Meetings tables ready');
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

  try {
    const fetch = global.fetch || (await import('node-fetch')).default;
    const r = await fetch(AUMPFY_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': AUMPFY_API_KEY },
      body: JSON.stringify({ to, text })
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
    console.error('  ❌ WhatsApp error:', err.message);
    return { ok: false, error: err.message };
  }
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

  try {
    const fetch = global.fetch || (await import('node-fetch')).default;
    const r = await fetch(AUMPFY_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': AUMPFY_API_KEY },
      body: JSON.stringify({ to: String(to), text })
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
    console.error('  ❌ WhatsApp (raw) error:', err.message);
    return { ok: false, error: err.message };
  }
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
    const [[naman]] = await db.query(`SELECT phone FROM users WHERE name='Naman Gupta' LIMIT 1`);

    const baseUrl = (process.env.APP_URL || 'http://localhost:3000').replace(/\/$/, '');

    const waMsg =
      `📋 *New WhatsApp Task — Approval Required*\n\n` +
      `*Task:* ${description}\n` +
      (due_date    ? `*Due Date:* ${due_date}\n`  : '') +
      (priority    ? `*Priority:* ${priority}\n`  : '') +
      (sender_name ? `*From:* ${sender_name}\n`   : '') +
      (remarks     ? `*Remarks:* ${remarks}\n`    : '') +
      `\n📲 *E-Marketing App* → Approvals → WhatsApp Tasks\n${baseUrl}/app`;

    if (naman?.phone) {
      sendWhatsApp(naman.phone, waMsg).catch(e => console.error('WA approval notify err:', e.message));
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
      sendWhatsApp(row.sender_phone, msg).catch(() => {});
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
      sendWhatsApp(row.sender_phone, msg).catch(() => {});
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
        sendWhatsApp(row.sender_phone, msg).catch(() => {});
      }
    } else {
      await db.query(`UPDATE tasks SET status='denied' WHERE id=?`, [row.id]);
      if (row.sender_phone) {
        const msg = `❌ *Task Not Approved*\n\nYour task was reviewed and was not approved.\n\n📋 *Task:* ${row.description}`;
        sendWhatsApp(row.sender_phone, msg).catch(() => {});
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
  for (const type of ['delegation','checklist','fms']) {
    if (!msgs[type]) { groupResults[type] = { skipped: 'no pending tasks' }; continue; }
    const r = await sendWhatsApp(PENDING_SUMMARY_PHONE, msgs[type]);
    groupResults[type] = r;
    await new Promise(r => setTimeout(r, 1500)); // small spacing so messages stay readable
  }
  // Also DM each user who has the "pending_summary_recipient" access ticked.
  // Recipients are messaged in parallel — each recipient's own 3 messages stay
  // sequential/spaced (for readability), but different recipients no longer
  // wait on each other. With sends done one recipient at a time, total time
  // grew with recipient count and started exceeding Vercel's 60s function
  // limit / GitHub Actions' 2-min job timeout once a few recipients were
  // configured, causing the whole cron run to get cancelled mid-request.
  const [recipients] = await db.query(
    `SELECT id, name, phone, extra_access FROM users WHERE phone IS NOT NULL AND phone <> '' AND extra_access IS NOT NULL`
  );
  const targets = recipients.filter(u => parseExtraAccess(u.extra_access).includes('pending_summary_recipient'));
  const dmResults = await Promise.all(targets.map(async u => {
    const perType = {};
    for (const type of ['delegation','checklist','fms']) {
      if (!msgs[type]) { perType[type] = { skipped: 'no pending tasks' }; continue; }
      perType[type] = await sendWhatsApp(u.phone, msgs[type]);
      await new Promise(r => setTimeout(r, 1200));
    }
    return { userId: u.id, name: u.name, phone: u.phone, perType };
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
  const [[purvi]] = await db.query(`SELECT phone FROM users WHERE name='Purvi Saini' LIMIT 1`);
  if (!purvi?.phone) return { ok: false, reason: 'Purvi Saini has no phone on file' };

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
    await sendWhatsApp(purvi.phone, waMsg).catch(e => console.error('WA new-mdo-task notify err:', e.message));
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
    `SELECT id, name, phone, COALESCE(department,'') AS department,
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
    if (!u.phone) { skipped++; skippedDetails.push({ name: u.name, reason: 'no phone' }); continue; }
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

    const r = await sendWhatsApp(u.phone, msg);
    if (r.ok) sent++; else { skipped++; skippedDetails.push({ name: u.name, reason: r.error || r.reason || 'send failed' }); }
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
    const [recentDel] = await db.query(
      `SELECT t.id, 'delegation' AS type, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              TIME_FORMAT(t.due_time,'%H:%i') AS due_time, t.assigned_to,
              u1.name AS doer, DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created
       FROM delegation_tasks t JOIN users u1 ON t.assigned_to=u1.id
       WHERE t.client_id=? AND DATE(t.created_at) BETWEEN ? AND ?
       ORDER BY t.created_at DESC LIMIT 25`, [id, from, to]);
    const [recentChl] = await db.query(
      `SELECT t.id, 'checklist' AS type, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u1.name AS doer, DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created
       FROM checklist_tasks t JOIN users u1 ON t.assigned_to=u1.id
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
// CREDIT CARDS — Excel Upload + Parse
// ══════════════════════════════════════════════════════
const ccUpload    = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
const ccPdfUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });
const dmsUpload   = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

const CC_BANK_KEYWORDS = {
  'RBL Bank': ['rbl'],
  'ICICI':    ['icici'],
  'HDFC':     ['hdfc'],
  'AXIS':     ['axis'],
  'AMEX':     ['amex','american express'],
  'SBI':      ['sbi','state bank'],
  'SCB':      ['scb','standard chartered'],
};

function detectBankName(text) {
  const lower = (text || '').toLowerCase();
  for (const [bank, keywords] of Object.entries(CC_BANK_KEYWORDS)) {
    if (keywords.some(k => lower.includes(k))) return bank;
  }
  return null;
}

// Parse date value from Excel cell (handles serial numbers + strings)
function parseExcelDate(val) {
  if (!val) return '';
  if (typeof val === 'number') {
    const d = XLSX.SSF.parse_date_code(val);
    if (d) return `${String(d.y)}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
  }
  return String(val).trim();
}

app.post('/api/credit-cards/upload-excel', requireAuth, ccUpload.single('file'), async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error: 'Access denied' });
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const wb = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: false });

    // ── Sheet 1: Card meta (Bank Name, Card Number, Statement Date, Payment Due Date, Payable Amount, Min Amount Due)
    const metaSheet = wb.Sheets[wb.SheetNames[0]];
    const metaRows  = XLSX.utils.sheet_to_json(metaSheet, { header: 1, defval: '' });

    let bankName = '', cardNumber = '', statementDate = '', paymentDueDate = '', payableAmount = 0, minAmountDue = 0;

    if (metaRows.length >= 2) {
      const hdrs = metaRows[0].map(h => String(h).toLowerCase().trim());
      const data  = metaRows[1];
      const col   = key => hdrs.findIndex(h => h.includes(key));

      bankName       = String(data[col('bank')]      || '').trim();
      cardNumber     = String(data[col('card')]      || '').trim();
      statementDate  = parseExcelDate(data[col('statement')]);
      paymentDueDate = parseExcelDate(data[Math.max(col('payment due'), col('due date'), col('due'))]);
      payableAmount  = parseFloat(String(data[col('payable')] || '0').replace(/[^0-9.]/g,'')) || 0;
      minAmountDue   = parseFloat(String(data[col('minimum')] || '0').replace(/[^0-9.]/g,'')) || 0;
    }

    // Detect canonical bank name
    const canonicalBank = detectBankName(bankName) || detectBankName(wb.SheetNames[0]) || detectBankName(metaRows.flat().join(' '));
    if (!canonicalBank) return res.status(422).json({ error: 'Bank name not detected. Ensure Sheet 1 contains Bank Name column with: RBL Bank, ICICI, HDFC, AXIS, AMEX, SBI, or SCB' });

    // ── Sheet 2: Transactions (Transaction Date, Description, Amount, Expenses, Department, Ownership)
    const transactions = [];
    if (wb.SheetNames.length >= 2) {
      const txSheet = wb.Sheets[wb.SheetNames[1]];
      const txRows  = XLSX.utils.sheet_to_json(txSheet, { header: 1, defval: '' });

      if (txRows.length >= 2) {
        const hdrs   = txRows[0].map(h => String(h).toLowerCase().trim());
        const col    = key => hdrs.findIndex(h => h.includes(key));
        const dateC  = col('date');
        const descC  = col('desc');
        const amtC   = col('amount');
        const expC   = col('expense');
        const deptC  = col('dept') >= 0 ? col('dept') : col('department');
        const ownC   = col('owner');

        for (let i = 1; i < txRows.length; i++) {
          const row = txRows[i];
          if (!row || row.every(c => c === '' || c === null || c === undefined)) continue;
          const dateVal = parseExcelDate(row[dateC >= 0 ? dateC : 0]);
          const desc    = String(row[descC >= 0 ? descC : 1] || '').trim();
          const amt     = parseFloat(String(row[amtC >= 0 ? amtC : 2] || '0').replace(/[^0-9.]/g,'')) || 0;
          const exp     = expC  >= 0 ? String(row[expC]  || '').trim() : '';
          const dept    = deptC >= 0 ? String(row[deptC] || '').trim() : '';
          const own     = ownC  >= 0 ? String(row[ownC]  || '').trim() : '';
          if (!dateVal && !desc && !amt) continue;
          transactions.push({ date: dateVal, description: desc, amount: amt, expenses: exp, department: dept, ownership: own });
        }
      }
    }

    res.json({ bankName: canonicalBank, cardNumber, statementDate, paymentDueDate, payableAmount, minAmountDue, transactions, rowsParsed: transactions.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// CREDIT CARDS — DB tables + PDF upload
// ══════════════════════════════════════════════════════
const { OpenAI } = require('openai');
const pdfjsLib    = require('pdfjs-dist/legacy/build/pdf.js');
const { createCanvas } = require('canvas');
// Explicit require ensures pdf.worker.js is bundled by Vercel's nft
require('pdfjs-dist/legacy/build/pdf.worker.js');
pdfjsLib.GlobalWorkerOptions.workerSrc = require.resolve('pdfjs-dist/legacy/build/pdf.worker.js');
const CC_OPENAI_KEY   = process.env.OPENAI_API_KEY || '';
const CC_OPENAI_MODEL = process.env.OPENAI_MODEL   || 'gpt-4.1-mini';

async function pdfToBase64Images(pdfBuffer, password = '') {
  const data       = new Uint8Array(pdfBuffer);
  const loadParams = { data };
  if (password) loadParams.password = password;
  const doc      = await pdfjsLib.getDocument(loadParams).promise;
  const numPages = Math.min(doc.numPages, 8); // CC statements never need more than 8 pages
  const pageNums = Array.from({ length: numPages }, (_, i) => i + 1);
  const imgs = await Promise.all(pageNums.map(async p => {
    const page     = await doc.getPage(p);
    const viewport = page.getViewport({ scale: 1.5 }); // 1.5x is sufficient for OCR, 44% less pixels than 2x
    const canvas   = createCanvas(viewport.width, viewport.height);
    await page.render({ canvasContext: canvas.getContext('2d'), viewport }).promise;
    return canvas.toBuffer('image/jpeg', { quality: 0.85 }).toString('base64'); // JPEG ~5-10x smaller than PNG
  }));
  return imgs;
}

const CC_EXTRACT_PROMPT = `You are a careful OCR and data-extraction engine reading a credit card statement PDF.
Return ONLY valid JSON — no markdown fences, no extra text.

Output structure:
{
  "fields": {
    "Bank Name": "...",
    "Credit Card No.": "...",
    "Statement Date": "DD/MM/YYYY",
    "Billing Period": "...",
    "Total Amount Due": "12345.67",
    "Minimum Due": "1234.56",
    "Due Date": "DD/MM/YYYY"
  },
  "transactions": [
    {"date":"DD/MM/YYYY","description":"...","amount":"1234.56","type":"Dr or Cr"}
  ]
}

Rules for ALL banks:
- "type" must be exactly "Dr" for debits, "Cr" for credits/payments
- "amount" must be numeric string only, no currency symbols
- "transactions" must always be present ([] if none found)
- All dates in DD/MM/YYYY format

════ HDFC BANK field names in the PDF: ════
  Credit Card No.  ← "Credit Card Number" or "Card Number"
  Statement Date   ← "Statement Generation Date"
  Billing Period   ← "Statement Period"
  Total Amount Due ← "Total Payment Due"
  Minimum Due      ← "Minimum Amount Due"
  Due Date         ← "Payment Due Date"
  Transactions: date includes time if printed (DD/MM/YYYY HH:MM:SS), description from "Transaction Description"

════ AXIS BANK field names in the PDF: ════
  Credit Card No.  ← "Card Number"
  Statement Date   ← "Statement Generation Date"
  Billing Period   ← "Statement Period"
  Total Amount Due ← "Total Payment Due"
  Minimum Due      ← "Minimum Amount Due"
  Due Date         ← "Payment Due Date"
  Transactions: description from "Transaction Details", amount from "Amount (Rs.)"

════ RBL BANK field names in the PDF: ════
  Credit Card No.  ← "Card Number"
  Statement Date   ← "Statement Date"
  Billing Period   ← "Statement Period"
  Total Amount Due ← "Total Amount Due"
  Minimum Due      ← "Minimum Amount Due"
  Due Date         ← "Payment Due Date"
  Transactions: description from "Description", amount from "Amount /₹"

════ AMEX (American Express Banking Corp.) field names in the PDF: ════
  Credit Card No.  ← "Membership Number" (e.g. XXXX-XXXXXX-21000)
  Statement Date   ← "Date" label at top-right of page 1 (format DD/MM/YYYY, e.g. 11/06/2026)
  Billing Period   ← "Statement Period" label followed by "From <date> to <date>" on the same line
  Total Amount Due ← "Closing Balance Rs" box in the summary row (numeric, e.g. 41451.54)
  Minimum Due      ← "Minimum Payment Rs" box in the summary row — extract ONLY the NUMERIC AMOUNT (e.g. 2073.00), NOT a date
  Due Date         ← CRITICAL: "Minimum Payment Due" label — the DATE that appears on the line BELOW this label (e.g. "June 29, 2026").
                     This is NOT the same as "Minimum Payment Rs" (which is an amount).
                     Also check "Payment Advice" section for "Due by June 29, 2026" or "by DD/MM/YYYY".
                     Output as DD/MM/YYYY.
  Transactions: each row in the "Details" column contains date + description together; split them — date is first (DD Mon or DD/MM/YYYY), rest is description; amount from "Amount Rs" column

════ ICICI BANK field names in the PDF: ════
  Credit Card No.  ← "Card Number" (printed below barcode, typically 16 digits)
  Statement Date   ← "STATEMENT DATE"
  Billing Period   ← Not explicitly labeled; derive from statement footer or "Statement for the period" if present; otherwise leave blank
  Total Amount Due ← "Total Amount due" (case-insensitive)
  Minimum Due      ← "Minimum Amount due" (case-insensitive)
  Due Date         ← "PAYMENT DUE DATE"
  Transactions: date from "Date" column (DD/MM/YYYY), description from "Transaction Details" or "Particulars" column, amount from "Amount (in Rs.)" or "Amount" column; CR/DR indicator in separate column

════ SCB (Standard Chartered Bank) field names in the PDF: ════
  Credit Card No.  ← "Card No." or the card number printed on the statement (16 digits); card type "DigiSmart" is NOT the card number
  Statement Date   ← "Statement Date"
  Billing Period   ← "Statement Period"
  Total Amount Due ← "Total Payment Due (INR)"
  Minimum Due      ← "Minimum Payment Due (INR)"
  Due Date         ← "Payment Due Date"
  Transactions: date from "Date" column (DD/MM/YYYY), description from "Description" or "Transaction Details" column, amount from "Amount" column; CR/DR indicator in type column`;

function safeParseCC(text) {
  text = String(text||'').trim().replace(/^```(?:json)?/m,'').replace(/```$/m,'').trim();
  try { return JSON.parse(text); } catch(e) {}
  const m = text.match(/\{[\s\S]*\}/);
  if (m) { try { return JSON.parse(m[0]); } catch(e) {} }
  return {};
}

// Create tables once on startup
;(async () => {
  try {
    await db.query(`CREATE TABLE IF NOT EXISTS cc_cards (
      id INT AUTO_INCREMENT PRIMARY KEY,
      bank_name  VARCHAR(50) NOT NULL,
      card_number VARCHAR(50) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_card (bank_name, card_number)
    )`);
    await db.query(`CREATE TABLE IF NOT EXISTS cc_statements (
      id INT AUTO_INCREMENT PRIMARY KEY,
      card_id          INT NOT NULL,
      statement_date   DATE,
      payment_due_date DATE,
      payable_amount   DECIMAL(12,2) DEFAULT 0,
      min_amount_due   DECIMAL(12,2) DEFAULT 0,
      statement_period VARCHAR(150),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (card_id) REFERENCES cc_cards(id) ON DELETE CASCADE,
      UNIQUE KEY uq_stmt (card_id, statement_date)
    )`);
    // Add pdf_data column if not present (try-catch for MySQL 5.7 compatibility)
    try { await db.query(`ALTER TABLE cc_statements ADD COLUMN pdf_data LONGBLOB DEFAULT NULL`); } catch(e) { /* already exists */ }
    // Add drive_file_id column for Google Drive storage
    try { await db.query(`ALTER TABLE cc_statements ADD COLUMN drive_file_id VARCHAR(200) DEFAULT NULL`); } catch(e) { /* already exists */ }
    // Add bill_drive_id column on cc_transactions for per-transaction bill PDF
    try { await db.query(`ALTER TABLE cc_transactions ADD COLUMN bill_drive_id VARCHAR(200) DEFAULT NULL`); } catch(e) { /* already exists */ }
    await db.query(`CREATE TABLE IF NOT EXISTS cc_transactions (
      id           INT AUTO_INCREMENT PRIMARY KEY,
      statement_id INT NOT NULL,
      txn_date     DATE,
      description  VARCHAR(500),
      amount       DECIMAL(12,2) DEFAULT 0,
      txn_type     ENUM('debit','credit') DEFAULT 'debit',
      expenses     VARCHAR(200),
      department   VARCHAR(100),
      created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (statement_id) REFERENCES cc_statements(id) ON DELETE CASCADE
    )`);
    await db.query(`CREATE TABLE IF NOT EXISTS cc_departments (
      id         INT AUTO_INCREMENT PRIMARY KEY,
      name       VARCHAR(100) NOT NULL UNIQUE,
      sort_order INT DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    // Seed from users + fixed extras if table is empty
    const [[{cnt}]] = await db.query('SELECT COUNT(*) as cnt FROM cc_departments');
    if (!cnt) {
      const [uRows] = await db.query("SELECT DISTINCT department FROM users WHERE department IS NOT NULL AND department != '' ORDER BY department");
      const fromUsers = uRows.map(r => r.department);
      const extras = ['Common', 'Advance Laminate'];
      const all = [...new Set([...fromUsers, ...extras])].sort((a,b) => a.localeCompare(b));
      if (all.length) {
        await db.query(
          'INSERT IGNORE INTO cc_departments (name, sort_order) VALUES ' + all.map((_,i) => '(?,?)').join(','),
          all.flatMap((n,i) => [n, i+1])
        );
      }
    }
  } catch(e) { console.error('CC tables init:', e.message); }
})();

// Payment requests table
;(async () => {
  try {
    await db.query(`CREATE TABLE IF NOT EXISTS payment_requests (
      id          INT AUTO_INCREMENT PRIMARY KEY,
      submitted_by INT NOT NULL,
      name        VARCHAR(100) NOT NULL,
      bank_name   VARCHAR(50)  NOT NULL,
      card_number VARCHAR(50)  NOT NULL,
      amount      DECIMAL(12,2) DEFAULT 0,
      reason      TEXT         NOT NULL,
      status      ENUM('pending','approved','rejected') DEFAULT 'pending',
      payment_done TINYINT(1)  DEFAULT 0,
      payment_done_at TIMESTAMP NULL,
      reviewed_at TIMESTAMP NULL,
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    // Add columns if they don't exist yet (individual try/catch for MySQL 5.7 compatibility)
    try { await db.query(`ALTER TABLE payment_requests ADD COLUMN amount DECIMAL(12,2) DEFAULT 0 AFTER card_number`); } catch(e) {}
    try { await db.query(`ALTER TABLE payment_requests ADD COLUMN payment_done TINYINT(1) DEFAULT 0 AFTER status`); } catch(e) {}
    try { await db.query(`ALTER TABLE payment_requests ADD COLUMN payment_done_at TIMESTAMP NULL AFTER payment_done`); } catch(e) {}
    // Manual card list for Payment Request dropdown (independent of PDF uploads)
    await db.query(`CREATE TABLE IF NOT EXISTS pr_cards (
      id         INT AUTO_INCREMENT PRIMARY KEY,
      bank_name  VARCHAR(50) NOT NULL,
      card_number VARCHAR(50) NOT NULL,
      UNIQUE KEY uq_pr_card (bank_name, card_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    // Seed known cards (INSERT IGNORE avoids duplicates)
    const seedCards = [
      ['AMEX',     'XXXX-XXXXXX-21000'],
      ['AXIS',     '539494******7928'],
      ['HDFC',     '545964XXXXXX8650'],
      ['HDFC',     '558983XXXXXX6349'],
      ['ICICI',    '5241XXXXXXXX7007'],
      ['RBL Bank', 'XXXXXXXXXXXXXX73'],
    ];
    for (const [b, c] of seedCards)
      await db.query('INSERT IGNORE INTO pr_cards (bank_name, card_number) VALUES (?,?)', [b, c]);
  } catch(e) { console.error('payment_requests init:', e.message); }
})();

// ── Parsing helpers ─────────────────────────────────────
function parseCCDateDMY(str) {
  // DD/MM/YYYY or DD-MM-YYYY
  const m = String(str||'').match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  return m ? `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}` : null;
}
function parseCCDateLong(str) {
  const MO = {january:'01',february:'02',march:'03',april:'04',may:'05',june:'06',july:'07',august:'08',september:'09',october:'10',november:'11',december:'12'};
  const m = String(str||'').match(/(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
  return (m && MO[m[1].toLowerCase()]) ? `${m[3]}-${MO[m[1].toLowerCase()]}-${m[2].padStart(2,'0')}` : null;
}
function parseCCDateAny(str) {
  return parseCCDateLong(str) || parseCCDateDMY(str) || null;
}
function parseCCAmount(str) {
  return parseFloat(String(str||'').replace(/[^0-9.]/g,'')) || 0;
}

// Deduplicate transactions: same date+amount → keep the one with the longest description
function dedupeTxns(txns) {
  const seen = new Map();
  for (const t of txns) {
    const key = `${t.txn_date}|${t.amount}|${t.txn_type}`;
    const existing = seen.get(key);
    if (!existing || String(t.description||'').length > String(existing.description||'').length)
      seen.set(key, t);
  }
  return Array.from(seen.values());
}

function parseAmexCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Membership Number']
                  || Object.entries(f).find(([k]) => k.startsWith('Membership Number'))?.[1]
                  || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date'])
                  || parseCCDateDMY(f['Date']) || parseCCDateLong(f['Date']);
  // "Minimum Payment Due" field contains the DATE (not the amount "Minimum Payment Rs")
  const _mpd = f['Minimum Payment Due'] || f['Due Date'] || f['Payment Due Date'] || f['Pay By'] || f['Due by'] || '';
  // also scan raw string for "Due by June 29, 2026" or "by 29/06/2026" patterns
  const _mpd2 = String(f['Payment Advice'] || f['due_by'] || '');
  const _dueFallback = (() => {
    const m = _mpd2.match(/(?:due\s+by|by)\s+([\w\s,\/]+?\d{4})/i);
    return m ? parseCCDateAny(m[1].trim()) : null;
  })();
  const dueDate = parseCCDateAny(_mpd) || _dueFallback
                || parseCCDateAny(f['Pay By']) || parseCCDateAny(f['Due by']);
  const payable    = parseCCAmount(f['Closing Balance Rs'] || f['Total Amount Due'] || f['New Balance']);
  const minDue     = parseCCAmount(f['Minimum Payment Rs'] || f['Minimum Due'] || f['Minimum Amount Due'] || f['Minimum Payment']);
  const period     = f['Statement Period'] || f['Billing Period'] || f['For the period'] || '';
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]) || parseCCDateLong(t.date);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
  return { bankName:'AMEX', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseHdfcCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Credit Card Number'] || f['Card Number'] || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  const dueDate    = parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date'])
                  || parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date']);
  const payable    = parseCCAmount(f['Total Amount Due']);
  const minDue     = parseCCAmount(f['Minimum Due'] || f['Minimum Amount Due']);
  const period     = f['Billing Period'] || '';
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
  return { bankName:'HDFC', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseAxisCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Card Number'] || f['Credit Card Number'] || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date'])
                  || parseCCDateDMY(f['Statement Generation Date']) || parseCCDateLong(f['Statement Generation Date']);
  const dueDate    = parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date'])
                  || parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date']);
  const payable    = parseCCAmount(f['Total Amount Due'] || f['Total Payment Due'] || f['Payable Amount']);
  const minDue     = parseCCAmount(f['Minimum Due'] || f['Minimum Amount Due'] || f['Minimum Payment Due']);
  const period     = f['Billing Period'] || f['Statement Period'] || '';
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
  return { bankName:'AXIS', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseRblCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Card Number'] || f['Credit Card Number'] || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  const dueDate    = parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date'])
                  || parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date']);
  const payable    = parseCCAmount(f['Total Amount Due'] || f['Payable Amount']);
  const minDue     = parseCCAmount(f['Minimum Due'] || f['Minimum Amount Due'] || f['Minimum Payment Due']);
  const period     = f['Billing Period'] || f['Statement Period'] || '';
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
  return { bankName:'RBL Bank', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseIciciCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Card Number'] || f['Credit Card Number'] || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  const dueDate    = parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date'])
                  || parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date']);
  const payable    = parseCCAmount(f['Total Amount Due'] || f['Total Amount due'] || f['Payable Amount']);
  const minDue     = parseCCAmount(f['Minimum Due'] || f['Minimum Amount Due'] || f['Minimum Amount due']);
  const period     = f['Billing Period'] || f['Statement Period'] || '';
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
  return { bankName:'ICICI', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseSbiCC(j, txns) {
  const f          = j.fields || j;
  // SBI PDF header: "Credit Card Number"
  const cardNumber = f['Credit Card Number'] || f['Credit Card No.'] || f['Card Number'] || 'Unknown Card';
  // SBI PDF header: "Statement Date"
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  // SBI PDF header: "Payment Due Date"
  const dueDate    = parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date'])
                  || parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date']);
  // SBI PDF header: "*Total Amount Due"
  const payable    = parseCCAmount(f['*Total Amount Due'] || f['Total Amount Due'] || f['Total Amount due']);
  // SBI PDF header: "**Minimum Amount Due"
  const minDue     = parseCCAmount(f['**Minimum Amount Due'] || f['Minimum Amount Due'] || f['Minimum Due']);
  // SBI PDF header: "for Statement Period"
  const period     = f['for Statement Period'] || f['Statement Period'] || f['Billing Period'] || '';
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
  return { bankName:'SBI', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseScbCC(j, txns) {
  const f          = j.fields || j;
  // SCB PDF: Card No. shown as card type (DigiSmart etc.) — card number may be separate
  const cardNumber = f['Credit Card No.'] || f['Card Number'] || f['Card No.'] || f['DigiSmart'] || 'Unknown Card';
  // SCB PDF: "Statement Date"
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  // SCB PDF: "Payment Due Date"
  const dueDate    = parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date'])
                  || parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date']);
  // SCB PDF: "Total Payment Due (INR)"
  const payable    = parseCCAmount(f['Total Payment Due (INR)'] || f['Total Payment Due'] || f['Total Amount Due'] || f['Payable Amount']);
  // SCB PDF: "Minimum Payment Due (INR)"
  const minDue     = parseCCAmount(f['Minimum Payment Due (INR)'] || f['Minimum Payment Due'] || f['Minimum Due'] || f['Minimum Amount Due']);
  // SCB PDF: "Statement Period"
  const period     = f['Statement Period'] || f['Billing Period'] || '';
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]) || parseCCDateLong(t.date);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
  return { bankName:'SCB', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseCCJson(extracted, filename) {
  const text  = JSON.stringify(extracted).toLowerCase();
  const fname = (filename||'').toLowerCase();
  // HDFC
  if (text.includes('hdfc') || fname.includes('hdfc'))
    return parseHdfcCC(extracted, extracted.transactions);
  // AXIS
  if (text.includes('axis') || fname.includes('axis'))
    return parseAxisCC(extracted, extracted.transactions);
  // RBL
  if (text.includes('rbl') || fname.includes('rbl'))
    return parseRblCC(extracted, extracted.transactions);
  // AMEX
  if (text.includes('american express') || text.includes('membership number') || fname.includes('amex'))
    return parseAmexCC(extracted, extracted.transactions);
  // ICICI
  if (text.includes('icici') || fname.includes('icici'))
    return parseIciciCC(extracted, extracted.transactions);
  // SBI — "sbi card" is the bank name in the PDF
  if (text.includes('sbi card') || text.includes('sbi') || fname.includes('sbi'))
    return parseSbiCC(extracted, extracted.transactions);
  // SCB — Standard Chartered Bank
  if (text.includes('standard chartered') || text.includes('scb') || fname.includes('scb'))
    return parseScbCC(extracted, extracted.transactions);
  const bank = detectBankName(text) || detectBankName(fname) || 'Unknown';
  return { bankName:bank, cardNumber:'Unknown Card', statementDate:null, paymentDueDate:null, payableAmount:0, minAmountDue:0, statementPeriod:'', transactions:[] };
}

async function saveCCToDb(parsed) {
  const { bankName, cardNumber, statementDate, paymentDueDate, payableAmount, minAmountDue, statementPeriod, transactions } = parsed;
  await db.query('INSERT IGNORE INTO cc_cards (bank_name,card_number) VALUES (?,?)', [bankName, cardNumber]);
  const [[card]] = await db.query('SELECT id FROM cc_cards WHERE bank_name=? AND card_number=?', [bankName, cardNumber]);
  await db.query(`INSERT IGNORE INTO cc_statements (card_id,statement_date,payment_due_date,payable_amount,min_amount_due,statement_period) VALUES (?,?,?,?,?,?)`,
    [card.id, statementDate, paymentDueDate, payableAmount, minAmountDue, statementPeriod]);
  const [[stmt]] = await db.query('SELECT id FROM cc_statements WHERE card_id=? AND statement_date<=>?', [card.id, statementDate]);
  let added = 0;
  for (const t of transactions) {
    const [[ex]] = await db.query('SELECT id FROM cc_transactions WHERE statement_id=? AND txn_date<=>? AND description=? AND amount=?',
      [stmt.id, t.txn_date, t.description, t.amount]);
    if (!ex) {
      await db.query('INSERT INTO cc_transactions (statement_id,txn_date,description,amount,txn_type) VALUES (?,?,?,?,?)',
        [stmt.id, t.txn_date, t.description, t.amount, t.txn_type||'debit']);
      added++;
    }
  }
  return { statementId:stmt.id, addedTransactions:added };
}

// POST /api/credit-cards/upload-pdf
app.post('/api/credit-cards/upload-pdf', requireAuth, ccPdfUpload.single('pdf'), async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    if (!req.file) return res.status(400).json({ error:'No file uploaded' });
    if (!CC_OPENAI_KEY) return res.status(500).json({ error:'OPENAI_API_KEY not set in .env' });

    const openai = new OpenAI({ apiKey: CC_OPENAI_KEY });

    // Convert each PDF page to PNG, then send all pages as images to OpenAI
    const pdfPassword = req.body.password || '';
    const pageImages = await pdfToBase64Images(req.file.buffer, pdfPassword);
    const content = [{ type: 'input_text', text: CC_EXTRACT_PROMPT }];
    for (const b64 of pageImages) {
      content.push({ type: 'input_image', image_url: `data:image/jpeg;base64,${b64}` });
    }

    const aiResp = await openai.responses.create({
      model: CC_OPENAI_MODEL,
      input: [{ role: 'user', content }]
    });

    const raw    = safeParseCC(aiResp.output_text);
    const parsed = parseCCJson(raw, req.file.originalname);
    if (parsed.bankName === 'Unknown') return res.status(422).json({ error:'Bank not detected. Supported: AMEX, HDFC, RBL Bank, ICICI, AXIS, SBI, SCB' });

    const saved = await saveCCToDb(parsed);
    // Upload original PDF to Drive (best-effort — statement data already saved)
    let driveFileId = null;
    try {
      const safe = s => String(s||'').replace(/[^a-zA-Z0-9_-]/g,'_').substring(0,20);
      const filename = 'CC_' + safe(parsed.bankName) + '_' + safe(parsed.statementDate) + '.pdf';
      const pdfB64 = req.file.buffer.toString('base64');
      const driveResp = await fetch(CC_DRIVE_SCRIPT, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
        body: JSON.stringify({ pdf: pdfB64, filename, folderId: '13Bn8WPbD1bEoQdM_GfirEE-9W7Gxot4k' }),
        redirect: 'follow'
      });
      const driveResult = await driveResp.json();
      if (driveResult.fileId) {
        driveFileId = driveResult.fileId;
        await db.query('UPDATE cc_statements SET drive_file_id=? WHERE id=?', [driveFileId, saved.statementId]);
      }
    } catch(e) { console.error('Drive upload failed:', e.message); }
    res.json({ success:true, bankName:parsed.bankName, cardNumber:parsed.cardNumber, statementDate:parsed.statementDate, transactionsAdded:saved.addedTransactions, totalTransactions:parsed.transactions.length, statementId:saved.statementId, driveFileId });
  } catch(err) {
    if (err.name === 'PasswordException') {
      const wrongPwd = err.code === 2;
      return res.status(400).json({ error: wrongPwd ? 'PDF_WRONG_PASSWORD' : 'PDF_PASSWORD_REQUIRED' });
    }
    res.status(500).json({ error: err.message });
  }
});

// GET /api/credit-cards/data
app.get('/api/credit-cards/data', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const [cards] = await db.query('SELECT * FROM cc_cards ORDER BY bank_name,card_number');
    const [stmts] = await db.query('SELECT * FROM cc_statements ORDER BY statement_date DESC');
    const [txns]  = await db.query('SELECT * FROM cc_transactions ORDER BY txn_date');
    const result = {};
    for (const card of cards) {
      if (!result[card.bank_name]) result[card.bank_name] = {};
      const cardStmts = stmts.filter(s => s.card_id === card.id);
      if (!cardStmts.length) continue; // skip cards with no statements
      result[card.bank_name][card.card_number] = cardStmts.map(s => ({
        id: s.id,
        statement_date:   s.statement_date   ? s.statement_date.toISOString().substring(0,10)   : '',
        payment_due_date: s.payment_due_date ? s.payment_due_date.toISOString().substring(0,10) : '',
        payable_amount:   parseFloat(s.payable_amount)||0,
        min_amount_due:   parseFloat(s.min_amount_due)||0,
        statement_period: s.statement_period||'',
        pdf_url: s.drive_file_id ? `https://drive.google.com/file/d/${s.drive_file_id}/view` : null,
        transactions: (() => {
          const raw = txns.filter(t => t.statement_id === s.id).map(t => ({
            id:          t.id,
            date:        t.txn_date ? t.txn_date.toISOString().substring(0,10) : '',
            description: t.description||'',
            amount:      parseFloat(t.amount)||0,
            txn_type:    t.txn_type||'debit',
            expenses:     t.expenses||'',
            department:   t.department||'',
            bill_drive_id: t.bill_drive_id||null
          }));
          // Dedup by date+amount+type — keep row with longest description (or any saved expenses/dept)
          const seen = new Map();
          for (const t of raw) {
            const key = `${t.date}|${t.amount}|${t.txn_type}`;
            const ex = seen.get(key);
            const prefer = !ex
              || (t.expenses || t.department)                             // prefer saved metadata
              || t.description.length > ex.description.length;           // else prefer longer desc
            if (prefer) seen.set(key, t);
          }
          return Array.from(seen.values());
        })()
      }));
    }
    res.json(result);
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// GET /api/credit-cards/statement-pdf/:stmtId — redirect to Drive URL
app.get('/api/credit-cards/statement-pdf/:stmtId', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const [[stmt]] = await db.query('SELECT drive_file_id FROM cc_statements WHERE id=?', [req.params.stmtId]);
    if (!stmt?.drive_file_id) return res.status(404).json({ error:'PDF not uploaded to Drive yet' });
    res.redirect(`https://drive.google.com/file/d/${stmt.drive_file_id}/view`);
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// POST /api/credit-cards/transaction/:id/bill — save Drive fileId (upload done client-side)
app.post('/api/credit-cards/transaction/:id/bill', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const { fileId } = req.body;
    if (!fileId) return res.status(400).json({ error:'No fileId provided' });
    await db.query('UPDATE cc_transactions SET bill_drive_id=? WHERE id=?', [fileId, req.params.id]);
    res.json({ success:true, fileId });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// PATCH /api/credit-cards/statement/:id  (update statement fields like period/due date)
app.patch('/api/credit-cards/statement/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const { statement_period, payment_due_date } = req.body;
    await db.query('UPDATE cc_statements SET statement_period=?, payment_due_date=? WHERE id=?',
      [statement_period||null, payment_due_date||null, req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// DELETE /api/credit-cards/statement/:id
app.delete('/api/credit-cards/statement/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    // get card_id before deleting
    const [[stmt]] = await db.query('SELECT card_id FROM cc_statements WHERE id=?', [req.params.id]);
    // Archive the statement and everything the FK cascade will take with it.
    // pdf_data (LONGBLOB) is deliberately excluded — it would bloat the archive
    // by megabytes per row; drive_file_id is the recovery path for the PDF.
    const [stmtRows] = await db.query(
      `SELECT id, card_id, statement_date, payment_due_date, payable_amount, min_amount_due,
              statement_period, drive_file_id, created_at
         FROM cc_statements WHERE id=?`, [req.params.id]);
    await archiveDeleted('cc_statements', stmtRows, req, {
      summary: r => `CC statement: ${r.statement_period || ''} (payable ${r.payable_amount ?? '?'})`,
      reason: 'pdf_data (LONGBLOB) not archived — recover via drive_file_id',
    });
    const [txnRows] = await db.query('SELECT * FROM cc_transactions WHERE statement_id=?', [req.params.id]);
    await archiveDeleted('cc_transactions', txnRows, req, {
      summary: r => `CC txn: ${r.description || ''} ${r.amount ?? ''}`,
      reason: `Cascade-deleted with cc_statements #${req.params.id}`,
    });
    await db.query('DELETE FROM cc_statements WHERE id=?', [req.params.id]);
    // if no more statements remain for this card, delete the orphan card too
    if (stmt) {
      const [[{ cnt }]] = await db.query('SELECT COUNT(*) AS cnt FROM cc_statements WHERE card_id=?', [stmt.card_id]);
      if (cnt === 0) {
        const [cardRows] = await db.query('SELECT * FROM cc_cards WHERE id=?', [stmt.card_id]);
        await archiveDeleted('cc_cards', cardRows, req, {
          summary: r => `CC card: ${r.bank_name || ''} ${r.card_number || ''}`,
          reason: 'Orphaned — last statement for this card was deleted',
        });
        await db.query('DELETE FROM cc_cards WHERE id=?', [stmt.card_id]);
      }
    }
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// DELETE /api/credit-cards/transaction/:id
app.delete('/api/credit-cards/transaction/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const [doomed] = await db.query('SELECT * FROM cc_transactions WHERE id=?', [req.params.id]);
    await archiveDeleted('cc_transactions', doomed, req, {
      summary: r => `CC txn: ${r.description || ''} ${r.amount ?? ''}`,
    });
    await db.query('DELETE FROM cc_transactions WHERE id=?', [req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// PATCH /api/credit-cards/transaction/:id  (update expenses / department)
app.patch('/api/credit-cards/transaction/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const { expenses, department } = req.body;
    await db.query('UPDATE cc_transactions SET expenses=?,department=? WHERE id=?', [expenses??null, department??null, req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// GET /api/credit-cards/departments — CC-only department master
app.get('/api/credit-cards/departments', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query('SELECT name FROM cc_departments ORDER BY sort_order, name');
    res.json(rows.map(r => r.name));
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /api/credit-cards/departments — add a new CC department (Naman only)
app.post('/api/credit-cards/departments', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const name = (req.body.name||'').trim();
    if (!name) return res.status(400).json({ error:'Name required' });
    const [[{maxOrd}]] = await db.query('SELECT COALESCE(MAX(sort_order),0) AS maxOrd FROM cc_departments');
    await db.query('INSERT INTO cc_departments (name, sort_order) VALUES (?,?)', [name, maxOrd+1]);
    res.json({ success:true });
  } catch(err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error:'Department already exists' });
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/credit-cards/departments/:name — remove a CC department (Naman only)
app.delete('/api/credit-cards/departments/:name', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const [doomed] = await db.query('SELECT * FROM cc_departments WHERE name=?', [req.params.name]);
    await archiveDeleted('cc_departments', doomed, req, { summary: r => `CC department: ${r.name || ''}` });
    await db.query('DELETE FROM cc_departments WHERE name=?', [req.params.name]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /api/credit-cards/drive-upload — save row to Sheet (GET) + upload PDF to Drive (POST)
const CC_DRIVE_SCRIPT = 'https://script.google.com/macros/s/AKfycbxh0cevqSgujIctWiQ17Py5n0OvxPp7Ji6JnI151FdIi-Uyv2rM-a4XUk5D7J3iqgE3/exec';
app.post('/api/credit-cards/drive-upload', requireAuth, async (req, res) => {
  try {
    const { pdf, filename, ...rowData } = req.body;
    // 1. Append row to Sheet via GET
    const params = new URLSearchParams({
      date: rowData.date||'', description: rowData.description||'',
      amount: rowData.amount||'', type: rowData.type||'',
      bank: rowData.bank||'', card: rowData.card||'',
      owner: rowData.owner||'', department: rowData.department||''
    });
    await fetch(`${CC_DRIVE_SCRIPT}?${params.toString()}`, { redirect: 'follow' });
    // 2. Upload PDF to Drive via POST
    if (pdf) {
      await fetch(CC_DRIVE_SCRIPT, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
        body: JSON.stringify({ pdf, filename: filename||'transaction.pdf' }),
        redirect: 'follow'
      });
    }
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

const PR_APPROVERS = ['Naman Gupta', 'Abhishek Jain', 'Simran Gurnani'];

// GET /api/payment-requests/cards — card list for dropdown (all logged-in users)
app.get('/api/payment-requests/cards', requireAuth, async (req, res) => {
  try {
    // Merge manually-managed pr_cards + any cc_cards from PDF uploads
    const [rows] = await db.query(`
      SELECT bank_name, card_number, id, 'manual' AS src FROM pr_cards
      UNION
      SELECT bank_name, card_number, id, 'cc' AS src FROM cc_cards
      ORDER BY bank_name, card_number`);
    // Deduplicate by bank+card (prefer manual entry so id is available for delete)
    const seen = new Map();
    for (const r of rows) {
      const key = `${r.bank_name}|${r.card_number}`;
      if (!seen.has(key) || r.src === 'manual') seen.set(key, r);
    }
    res.json(Array.from(seen.values()));
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /api/payment-requests/cards — add card (Naman only)
app.post('/api/payment-requests/cards', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const { bank_name, card_number } = req.body;
    if (!bank_name || !card_number) return res.status(400).json({ error:'bank_name and card_number required' });
    await db.query('INSERT IGNORE INTO pr_cards (bank_name, card_number) VALUES (?,?)', [bank_name.trim(), card_number.trim()]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// DELETE /api/payment-requests/cards/:id — remove card (Naman only, pr_cards only)
app.delete('/api/payment-requests/cards/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const [doomed] = await db.query('SELECT * FROM pr_cards WHERE id=?', [req.params.id]);
    await archiveDeleted('pr_cards', doomed, req, {
      summary: r => `PR card: ${r.bank_name || ''} ${r.card_number || ''}`,
    });
    await db.query('DELETE FROM pr_cards WHERE id=?', [req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// DELETE /api/payment-requests/:id — hard delete row + its sentinels (admin only, temporary cleanup)
// Readable one-liner for an archived payment_requests row.
// The reason column is not plain text: the frontend packs the amount into it
// as "[<currency><amount>] <real reason>" (see prParseReason in app.html), and
// bill/paid sentinels are stored as fake rows under bank_name '__system__'.
// Mirror that here so the Logs page doesn't show the amount twice or print a
// raw sentinel string.
function prSummary(r) {
  const raw = String(r.reason || '');
  if (r.bank_name === '__system__') return `Payment request sentinel: ${raw}`;
  let amount = r.amount ?? '';
  let reason = raw;
  if (raw.charAt(0) === '[') {
    const close = raw.indexOf('] ');
    if (close > 1) {
      const inner = raw.slice(1, close);
      const num = parseFloat(inner.slice(1).replace(/,/g, ''));
      if (!isNaN(num) && num >= 0) { amount = inner; reason = raw.slice(close + 2); }
    }
  }
  return `Payment request: ${amount}${reason ? ' — ' + reason : ''}`;
}

app.delete('/api/payment-requests/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const id = req.params.id;
    const [doomed] = await db.query(
      'SELECT * FROM payment_requests WHERE id=? OR (bank_name=\'__system__\' AND reason LIKE ?)', [id, `%:${id}%`]);
    await archiveDeleted('payment_requests', doomed, req, { summary: prSummary });
    await db.query('DELETE FROM payment_requests WHERE id=? OR (bank_name=\'__system__\' AND reason LIKE ?)', [id, `%:${id}%`]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /api/payment-requests — submit new request (all logged-in users)
app.post('/api/payment-requests', requireAuth, async (req, res) => {
  try {
    const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
    if (!me) return res.status(403).json({ error:'Access denied' });
    const { bank_name, card_number, amount, reason } = req.body;
    if (!bank_name || !card_number || !reason) return res.status(400).json({ error:'All fields required' });
    try {
      await db.query(
        'INSERT INTO payment_requests (submitted_by, name, bank_name, card_number, amount, reason) VALUES (?,?,?,?,?,?)',
        [req.session.userId, me.name, bank_name, card_number, parseFloat(amount)||0, reason]
      );
    } catch(insertErr) {
      // Fallback if amount column not yet migrated (server not restarted)
      await db.query(
        'INSERT INTO payment_requests (submitted_by, name, bank_name, card_number, reason) VALUES (?,?,?,?,?)',
        [req.session.userId, me.name, bank_name, card_number, reason]
      );
    }
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Helper: pass the row to the client untouched.
// This used to un-pack the "[<currency><amount>]" prefix out of the reason and
// move the number into `amount`. Doing so threw away the currency symbol, so
// every request came back with a bare number and the client always rendered ₹,
// even for rows submitted in $ (e.g. "[$100.00] Claude Subscription"). The
// client parses the encoded reason itself (prParseReason in app.html) to get
// amount + currency, so it needs the reason raw. Keep this as a pass-through.
function parsePrRow(row) {
  return row;
}

// GET /api/payment-requests — all requests (admin + payment approvers)
app.get('/api/payment-requests', requireAuth, async (req, res) => {
  try {
    const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
    if (req.session.role !== 'admin' && (!me || !PR_APPROVERS.includes(me.name))) return res.status(403).json({ error:'Access denied' });
    const [rows] = await db.query(
      'SELECT * FROM payment_requests ORDER BY created_at DESC'
    );
    res.json(rows.map(parsePrRow));
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /api/payment-requests/my — own requests (any logged-in user)
app.get('/api/payment-requests/my', requireAuth, async (req, res) => {
  try {
    const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
    if (!me) return res.status(403).json({ error:'Access denied' });
    const [rows] = await db.query(
      'SELECT * FROM payment_requests WHERE submitted_by=? ORDER BY created_at DESC',
      [req.session.userId]
    );
    // Payment-done/cancelled/bill markers are stored as separate "__system__" sentinel
    // rows submitted by whoever actioned them (usually an admin, not this employee), so
    // the submitted_by filter above misses them — without this the employee's payment
    // status stays stuck on "Pending" forever even after an admin marks it paid. Pull in
    // only the sentinels that reference one of this employee's own request ids.
    const myIds = new Set(rows.map(r => String(r.id)));
    if (myIds.size) {
      const [sentinelRows] = await db.query(
        `SELECT * FROM payment_requests WHERE bank_name='__system__'`
      );
      const mySentinels = sentinelRows.filter(s => {
        const match = /^__(?:paid|cancelled|bill)__:(\d+)/.exec(s.reason || '');
        return match && myIds.has(match[1]);
      });
      rows.push(...mySentinels);
    }
    res.json(rows.map(parsePrRow));
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// PATCH /api/payment-requests/:id — approve or reject (admin + payment approvers)
app.patch('/api/payment-requests/:id', requireAuth, async (req, res) => {
  try {
    const [[me2]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
    if (req.session.role !== 'admin' && (!me2 || !PR_APPROVERS.includes(me2.name))) return res.status(403).json({ error:'Access denied' });
    const { status } = req.body;
    if (!['approved','rejected'].includes(status)) return res.status(400).json({ error:'Invalid status' });
    await db.query(
      'UPDATE payment_requests SET status=?, reviewed_at=NOW() WHERE id=?',
      [status, req.params.id]
    );
    res.json({ success: true });
    // WhatsApp notification is fire-and-forget, matching the pattern used for other
    // approval flows (mdo-tasks, leave requests, meetings) — the approve/reject
    // response no longer waits on the WhatsApp round trip.
    (async () => {
      try {
        const [[pr]] = await db.query('SELECT submitted_by, reason FROM payment_requests WHERE id=?', [req.params.id]);
        if (pr && pr.submitted_by) {
          const [[submitter]] = await db.query('SELECT name, phone FROM users WHERE id=?', [pr.submitted_by]);
          if (submitter && submitter.phone) {
            const emoji = status === 'approved' ? '✅' : '❌';
            const statusText = status === 'approved' ? 'Approved' : 'Rejected';
            let amtStr = '', cleanReason = pr.reason || '';
            if (pr.reason) {
              const s = String(pr.reason);
              if (s.charAt(0) === '[') {
                const close = s.indexOf('] ');
                if (close > 1) {
                  const inner = s.slice(1, close);
                  const num = parseFloat(inner.slice(1).replace(/,/g, ''));
                  if (!isNaN(num)) {
                    amtStr = `\n*Amount:* ${inner.charAt(0)}${num.toFixed(2)}`;
                    cleanReason = s.slice(close + 2);
                  }
                }
              }
            }
            const msg = `${emoji} *Payment Request ${statusText}*\n\nHi ${submitter.name},\n\nYour payment request has been *${statusText.toLowerCase()}*.${amtStr}\n*Reason:* ${cleanReason}\n\n— E-Marketing`;
            await sendWhatsApp(submitter.phone, msg);
          }
        }
      } catch(waErr) { console.error('WA payment notify err:', waErr.message); }
    })();
  } catch(err) { res.status(500).json({ error: err.message }); }
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

    // Admin oversight — always notify Naman Gupta
    const [[naman]] = await db.query(`SELECT phone FROM users WHERE name='Naman Gupta' LIMIT 1`);
    if (naman?.phone) {
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
      sendWhatsApp(naman.phone, waMsg).catch(e => console.error('WA mdo-task notify err:', e.message));
    }

    // Notify Assigned To
    if (assignedToUser?.phone) {
      const waMsg = status === 'Approved'
        ? `✅ *Task Approved & Assigned to ${task.assigned_to || '—'}*\n\n📋 *Task:* ${taskDesc}\n🆔 *Task ID:* ${task.task_id || '—'}\n🙋 *Assigned By:* ${assignedByName || '—'}\n📅 *Due Date:* ${dueDateStr}\n🏢 *Client:* ${task.client_name || '—'}\n\nThis task has been approved by *${approverName}* and assigned to you.`
        : `❌ *Task Rejected*\n\n📋 *Task:* ${taskDesc}\n🆔 *Task ID:* ${task.task_id || '—'}\n🙋 *Assigned By:* ${assignedByName || '—'}\n\nThis task was reviewed and rejected by *${approverName}*. No action needed from you.`;
      sendWhatsApp(assignedToUser.phone, waMsg).catch(e => console.error('WA mdo-task assignedTo notify err:', e.message));
    }

    // Notify Assigned By
    if (assignedByUser?.phone) {
      const waMsg = status === 'Approved'
        ? `✅ *Task Delegated Successfully*\n\n📋 *Task:* ${taskDesc}\n🆔 *Task ID:* ${task.task_id || '—'}\n👤 *Assigned To:* ${task.assigned_to || '—'}\n📅 *Due Date:* ${dueDateStr}\n\nYour task has been approved by *${approverName}* and delegated to *${task.assigned_to}*.`
        : `❌ *Task Request Rejected*\n\n📋 *Task:* ${taskDesc}\n🆔 *Task ID:* ${task.task_id || '—'}\n👤 *Assigned To:* ${task.assigned_to || '—'}\n\nYour task request was reviewed and rejected by *${approverName}*.`;
      sendWhatsApp(assignedByUser.phone, waMsg).catch(e => console.error('WA mdo-task assignedBy notify err:', e.message));
    }

    res.json({ success: true, delegationTaskId });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /api/payment-requests/:id/wa-debug
app.get('/api/payment-requests/:id/wa-debug', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [[pr]] = await db.query('SELECT id, submitted_by, reason, status FROM payment_requests WHERE id=?', [req.params.id]);
    if (!pr) return res.json({ error: 'request not found' });
    let submitter = null;
    if (pr.submitted_by) {
      [[submitter]] = await db.query('SELECT id, name, phone FROM users WHERE id=?', [pr.submitted_by]);
    }
    let waResult = null;
    if (submitter && submitter.phone) {
      waResult = await sendWhatsApp(submitter.phone, `✅ Test — Payment Request #${pr.id} WA debug`);
    }
    res.json({ pr, submitter, waResult });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/payment-requests/:id/payment-done — mark payment as done (Vishal only)
app.post('/api/payment-requests/:id/payment-done', requireAuth, async (req, res) => {
  try {
    const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
    if (!me || me.name !== 'Vishal Jaga') return res.status(403).json({ error:'Access denied' });
    try {
      await db.query(
        'UPDATE payment_requests SET payment_done=1, payment_done_at=NOW() WHERE id=? AND status="approved"',
        [req.params.id]
      );
    } catch(e) {
      // Fallback if payment_done column not yet migrated
      await db.query(
        'UPDATE payment_requests SET status="approved" WHERE id=?',
        [req.params.id]
      );
    }
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

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

app.get('/api/clients', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT c.id, c.name, c.handler_id, c.logo_url, COALESCE(c.is_active,1) AS is_active,
              u.name AS handler_name,
              (SELECT GROUP_CONCAT(u2.name ORDER BY u2.name SEPARATOR '||')
               FROM client_handlers ch JOIN users u2 ON ch.user_id = u2.id
               WHERE ch.client_id = c.id) AS all_handler_names
       FROM clients c LEFT JOIN users u ON c.handler_id = u.id
       ORDER BY c.name ASC`);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Update / clear client logo. Body: { logo: <data-URL string> | null }.
// 1.5 MB cap on payload — base64 expansion + headroom for a 256x256 JPEG.
app.put('/api/clients/:id/logo', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const id = req.params.id;
    let { logo } = req.body;
    if (logo === undefined) return res.status(400).json({ error: 'logo field required (string or null)' });
    if (logo !== null && typeof logo === 'string') {
      if (!/^data:image\/(png|jpe?g|webp|gif);base64,/.test(logo)) {
        return res.status(400).json({ error: 'logo must be a data:image/* base64 URL' });
      }
      if (logo.length > 1_500_000) return res.status(413).json({ error: 'Logo too large — keep under 1 MB after resize' });
    } else if (logo !== null) {
      return res.status(400).json({ error: 'logo must be a string or null' });
    }
    const [[exists]] = await db.query('SELECT id FROM clients WHERE id=?', [id]);
    if (!exists) return res.status(404).json({ error: 'Client not found' });
    await db.query('UPDATE clients SET logo_url=? WHERE id=?', [logo, id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/clients', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const name = (req.body.name || '').trim();
    const handlerRaw = req.body.handler_id;
    const handlerId = handlerRaw == null || handlerRaw === '' ? null : parseInt(handlerRaw, 10);
    const loginEmail = (req.body.login_email || '').trim().toLowerCase();
    const loginPassword = req.body.login_password || '';
    if (!name) return res.status(400).json({ error: 'Client name required' });
    // Provisioning a login is optional. If asked, both fields must be present.
    if ((loginEmail && !loginPassword) || (!loginEmail && loginPassword)) {
      return res.status(400).json({ error: 'Both login email and password required to provision client login' });
    }
    const [r] = await db.query('INSERT INTO clients (name, handler_id) VALUES (?, ?)',
      [name, Number.isFinite(handlerId) ? handlerId : null]);
    const newClientId = r.insertId;
    if (loginEmail && loginPassword) {
      try {
        const hash = bcrypt.hashSync(loginPassword, 10);
        await db.query(
          `INSERT INTO users (name, email, password, role, user_role, client_id)
           VALUES (?, ?, ?, 'client', 'client', ?)`,
          [name, loginEmail, hash, newClientId]);
      } catch (e) {
        // Client row was created — surface auth provisioning error separately so
        // admin knows the client exists but login was not set up.
        return res.status(201).json({
          success: true, client_id: newClientId,
          warning: e.code === 'ER_DUP_ENTRY' ? 'Client added but login email already in use' : 'Client added but login provisioning failed: ' + e.message
        });
      }
    }
    // Auto-create Drive folder if root folder is configured (fire-and-forget, never blocks the response)
    const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    if (rootFolderId) {
      dmsCreateFolder(name, rootFolderId)
        .then(folder => db.query('UPDATE clients SET drive_folder_id=? WHERE id=?', [folder.id, newClientId]))
        .catch(e => console.error('DMS auto-folder creation failed for client', newClientId, e.message));
    }
    res.json({ success: true, client_id: newClientId });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'Client already exists' });
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/clients/:id', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const id = req.params.id;
    const name = req.body.name == null ? null : String(req.body.name).trim();
    const handlerRaw = req.body.handler_id;
    const handlerId = handlerRaw === undefined ? undefined
                    : (handlerRaw == null || handlerRaw === '') ? null
                    : parseInt(handlerRaw, 10);
    if (name === '') return res.status(400).json({ error: 'Client name cannot be empty' });
    // Only update fields that were sent.
    const sets = [], params = [];
    if (name !== null) { sets.push('name=?'); params.push(name); }
    if (handlerId !== undefined) { sets.push('handler_id=?'); params.push(handlerId); }
    if (req.body.system_links !== undefined) { sets.push('system_links=?'); params.push(sanitizeSystemLinks(req.body.system_links)); }
    if (req.body.is_active !== undefined) { sets.push('is_active=?'); params.push(req.body.is_active ? 1 : 0); }
    if (!sets.length) return res.json({ success: true, noop: true });
    params.push(id);
    await db.query(`UPDATE clients SET ${sets.join(', ')} WHERE id=?`, params);
    res.json({ success: true });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'Client name already exists' });
    res.status(500).json({ error: err.message });
  }
});

// Multi-handler support — get all handlers for a client
app.get('/api/clients/:id/handlers', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT ch.user_id AS id, u.name, COALESCE(u.department,'') AS department
       FROM client_handlers ch JOIN users u ON u.id=ch.user_id
       WHERE ch.client_id=? ORDER BY u.name`, [req.params.id]);
    res.json(rows);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Multi-handler support — replace all handlers for a client
app.put('/api/clients/:id/handlers', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const userIds = Array.isArray(req.body.user_ids)
      ? req.body.user_ids.map(Number).filter(n => Number.isFinite(n) && n > 0)
      : [];
    await db.query('DELETE FROM client_handlers WHERE client_id=?', [id]);
    if (userIds.length) {
      await db.query(
        `INSERT INTO client_handlers (client_id, user_id) VALUES ${userIds.map(() => '(?,?)').join(',')}`,
        userIds.flatMap(uid => [id, uid]));
    }
    // Keep primary handler_id in sync with first selected (for backward compat)
    await db.query('UPDATE clients SET handler_id=? WHERE id=?', [userIds[0] || null, id]);
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/clients/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [doomed] = await db.query('SELECT * FROM clients WHERE id=?', [req.params.id]);
    await archiveDeleted('clients', doomed, req, { summary: r => `Client: ${r.name || ''}` });
    await db.query('DELETE FROM clients WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk add clients via CSV
app.post('/api/clients/bulk', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const { names } = req.body;
    if (!Array.isArray(names) || !names.length) {
      return res.status(400).json({ error: 'No clients to add' });
    }
    // Clean + dedupe within request
    const cleanNames = [...new Set(
      names.map(n => String(n||'').trim()).filter(n => n)
    )];
    if (!cleanNames.length) return res.status(400).json({ error: 'No valid client names' });

    let added = 0, skipped = 0;
    const skippedNames = [];
    for (const name of cleanNames) {
      try {
        await db.query('INSERT INTO clients (name) VALUES (?)', [name]);
        added++;
      } catch (e) {
        if (e.code === 'ER_DUP_ENTRY') { skipped++; skippedNames.push(name); }
        else throw e;
      }
    }
    res.json({ success: true, added, skipped, skippedNames });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Client stats — full client snapshot for the detail page. Defaults to the
// current month (IST). Accepts ?from=YYYY-MM-DD&to=YYYY-MM-DD to widen the
// window. Includes delegation/checklist task counts + meetings + recent rows.
app.get('/api/clients/:id/stats', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const id = req.params.id;
    const [[client]] = await db.query(
      `SELECT c.id, c.name, c.handler_id, c.logo_url, c.system_links, u.name AS handler_name, u.email AS handler_email
       FROM clients c LEFT JOIN users u ON c.handler_id = u.id WHERE c.id=?`, [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    client.system_links = parseSystemLinks(client.system_links);

    // Login user (if provisioned) — there's at most one client login per client.
    const [[loginUser]] = await db.query(
      "SELECT id, email FROM users WHERE role='client' AND client_id=? LIMIT 1", [id]);

    // Default window — current month (IST)
    const ist = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
    const y = ist.getUTCFullYear(), m = ist.getUTCMonth();
    const defaultFrom = `${y}-${String(m+1).padStart(2,'0')}-01`;
    const lastDay = new Date(Date.UTC(y, m+1, 0)).getUTCDate();
    const defaultTo = `${y}-${String(m+1).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;
    const isDate = v => /^\d{4}-\d{2}-\d{2}$/.test(v || '');
    const from = isDate(req.query.from) ? req.query.from : defaultFrom;
    const to   = isDate(req.query.to)   ? req.query.to   : defaultTo;

    const [[del]] = await db.query(
      `SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='revised'   THEN 1 ELSE 0 END) AS revised,
        SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM delegation_tasks WHERE client_id=? AND due_date BETWEEN ? AND ?`, [id, from, to]);
    const [[chl]] = await db.query(
      `SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM checklist_tasks WHERE client_id=? AND due_date BETWEEN ? AND ?`, [id, from, to]);

    // Meetings tied to this client (by client_id)
    const [[meet]] = await db.query(
      `SELECT
        COUNT(*) AS total,
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

    // Recent activity — tasks (delegation + checklist) created in the window
    const [recentDel] = await db.query(
      `SELECT t.id, 'delegation' AS type, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u1.name AS doer, COALESCE(u2.name,'—') AS assigner,
              DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created
       FROM delegation_tasks t
       JOIN users u1 ON t.assigned_to=u1.id
       LEFT JOIN users u2 ON t.assigned_by=u2.id
       WHERE t.client_id=? AND DATE(t.created_at) BETWEEN ? AND ?
       ORDER BY t.created_at DESC LIMIT 25`, [id, from, to]);
    const [recentChl] = await db.query(
      `SELECT t.id, 'checklist' AS type, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u1.name AS doer, COALESCE(u2.name,'—') AS assigner,
              DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created
       FROM checklist_tasks t
       JOIN users u1 ON t.assigned_to=u1.id
       LEFT JOIN users u2 ON t.assigned_by=u2.id
       WHERE t.client_id=? AND DATE(t.created_at) BETWEEN ? AND ?
       ORDER BY t.created_at DESC LIMIT 25`, [id, from, to]);
    const recent = [...recentDel, ...recentChl]
      .sort((a,b) => (b.created||'').localeCompare(a.created||''))
      .slice(0, 20);

    res.json({
      client: {
        id: client.id, name: client.name, logo_url: client.logo_url,
        handler_id: client.handler_id, handler_name: client.handler_name, handler_email: client.handler_email,
        system_links: client.system_links
      },
      login: loginUser ? { provisioned: true, email: loginUser.email } : { provisioned: false },
      range: { from, to },
      delegation: del, checklist: chl, meetings: { ...meet, recent: meetRecent },
      recent
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Provision or update client login. Creates the users row if missing, or
// updates email/password on the existing one. Admin/HOD/PC only.
app.post('/api/clients/:id/login', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const id = req.params.id;
    const email = (req.body.email || '').trim().toLowerCase();
    const password = req.body.password || '';
    if (!email || !password) return res.status(400).json({ error: 'email and password required' });
    const [[client]] = await db.query('SELECT id, name FROM clients WHERE id=?', [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    const [[existing]] = await db.query(
      "SELECT id FROM users WHERE role='client' AND client_id=? LIMIT 1", [id]);
    const hash = bcrypt.hashSync(password, 10);
    try {
      if (existing) {
        await db.query('UPDATE users SET email=?, password=? WHERE id=?', [email, hash, existing.id]);
      } else {
        await db.query(
          `INSERT INTO users (name, email, password, role, user_role, client_id)
           VALUES (?, ?, ?, 'client', 'client', ?)`,
          [client.name, email, hash, id]);
      }
    } catch (e) {
      if (e.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'That email is already in use' });
      throw e;
    }
    res.json({ success: true, email });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// GOOGLE DRIVE STATUS — service account (GOOGLE_CREDENTIALS) must have
// Editor access on GOOGLE_DRIVE_ROOT_FOLDER_ID (share the folder with its
// client_email in the Drive UI). No OAuth consent flow needed.
// ══════════════════════════════════════════════════════
app.get('/api/google/drive-status', requireAuth, requireAdmin, async (req, res) => {
  const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
  if (!rootFolderId) return res.json({ connected: false, reason: 'GOOGLE_DRIVE_ROOT_FOLDER_ID not set' });
  try {
    const drive = await getDriveClient();
    await drive.files.get({ fileId: rootFolderId, fields: 'id', supportsAllDrives: true });
    res.json({ connected: true });
  } catch (e) { res.json({ connected: false, reason: e.message }); }
});

// ══════════════════════════════════════════════════════
// DMS — Document Management System (Google Drive)
// ══════════════════════════════════════════════════════

// Get DMS status for a client
app.get('/api/clients/:id/dms', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const id = req.params.id;
    const [[client]] = await db.query(
      'SELECT id, name, drive_folder_id FROM clients WHERE id=?', [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    const [depts] = await db.query(
      'SELECT department_name, drive_folder_id FROM client_department_folders WHERE client_id=? ORDER BY department_name',
      [id]);
    const drive_configured = !!process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    res.json({
      client_id: client.id,
      client_name: client.name,
      drive_folder_id: client.drive_folder_id || null,
      drive_configured,
      departments: depts,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Create Drive folders for every existing client that doesn't have one yet
// (one-time catch-up after GOOGLE_DRIVE_ROOT_FOLDER_ID is first configured).
app.post('/api/admin/dms/bulk-setup', requireAuth, requireAdmin, async (req, res) => {
  try {
    const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    if (!rootFolderId) return res.status(400).json({ error: 'GOOGLE_DRIVE_ROOT_FOLDER_ID env var not set' });
    const [clients] = await db.query(
      "SELECT id, name FROM clients WHERE COALESCE(is_active,1) != 0 AND (drive_folder_id IS NULL OR drive_folder_id = '')");
    let created = 0;
    const errors = [];
    for (const c of clients) {
      try {
        const folder = await dmsCreateFolder(c.name, rootFolderId);
        await db.query('UPDATE clients SET drive_folder_id=? WHERE id=?', [folder.id, c.id]);
        await _dmsLogActivity(folder.id, 'created', c.name, req, c.id);
        created++;
      } catch (e) { errors.push(`${c.name}: ${e.message}`); }
    }
    res.json({ success: true, total: clients.length, created, failed: errors.length, errors });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Live listing of the DMS root Drive folder — every client folder with its
// real modifiedTime/size/last-editor, matched back to our client_id so the
// DMS "Clients" table can look and behave exactly like Drive's own list view.
app.get('/api/admin/dms/root-files', requireAuth, requireAdmin, async (req, res) => {
  try {
    const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    if (!rootFolderId) return res.status(400).json({ error: 'GOOGLE_DRIVE_ROOT_FOLDER_ID env var not set' });
    const files = await dmsListFiles(rootFolderId);
    const [clients] = await db.query("SELECT id, drive_folder_id FROM clients WHERE drive_folder_id IS NOT NULL AND drive_folder_id != ''");
    const byFolderId = Object.fromEntries(clients.map(c => [c.drive_folder_id, c.id]));
    for (const f of files) { const cid = byFolderId[f.id]; if (cid) f.client_id = cid; }

    // Prefer "last activity anywhere in this client's folder" from our own
    // log over the Drive folder's own modifiedTime — Drive never bumps a
    // folder's timestamp when a file inside it is added/changed.
    const clientIds = Object.values(byFolderId);
    if (clientIds.length) {
      // created_at is stored in the DB server's own local time (IST here, per
      // @@session.time_zone), not UTC. Convert to an explicit UTC ISO string
      // in SQL so mysql2/the browser can't double-apply the offset — sending
      // the raw DATETIME let the frontend re-interpret an already-local value
      // as UTC and shift it by the server's offset again (3:12 PM -> 8:42 PM).
      const [rows] = await db.query(
        `SELECT client_id, user_name,
                DATE_FORMAT(CONVERT_TZ(created_at, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%SZ') AS created_at
         FROM dms_file_activity
         WHERE client_id IN (${clientIds.map(()=>'?').join(',')})
         ORDER BY created_at DESC`,
        clientIds
      ).catch(() => [[]]);
      const latestByClient = {};
      for (const r of rows) { if (!latestByClient[r.client_id]) latestByClient[r.client_id] = r; }
      for (const f of files) {
        const log = f.client_id && latestByClient[f.client_id];
        if (log) { f.modified_by = log.user_name; f.modifiedTime = log.created_at; }
      }
    }
    res.json(files);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Create the client's root Drive folder (one-time setup)
app.post('/api/clients/:id/dms/setup', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    if (!rootFolderId) return res.status(400).json({ error: 'GOOGLE_DRIVE_ROOT_FOLDER_ID env var not set' });
    const id = req.params.id;
    const [[client]] = await db.query('SELECT id, name, drive_folder_id FROM clients WHERE id=?', [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    if (client.drive_folder_id) {
      return res.json({ success: true, drive_folder_id: client.drive_folder_id, already_exists: true });
    }
    const folder = await dmsCreateFolder(client.name, rootFolderId);
    await db.query('UPDATE clients SET drive_folder_id=? WHERE id=?', [folder.id, id]);
    await _dmsLogActivity(folder.id, 'created', client.name, req, id);
    res.json({ success: true, drive_folder_id: folder.id, web_view_link: folder.webViewLink });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add a department subfolder under the client's Drive folder
app.post('/api/clients/:id/dms/departments', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const id = req.params.id;
    const dept = (req.body.department_name || '').trim();
    if (!dept) return res.status(400).json({ error: 'department_name required' });
    const [[client]] = await db.query('SELECT id, name, drive_folder_id FROM clients WHERE id=?', [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    if (!client.drive_folder_id) return res.status(400).json({ error: 'Set up the client Drive folder first' });
    const [[existing]] = await db.query(
      'SELECT drive_folder_id FROM client_department_folders WHERE client_id=? AND department_name=?',
      [id, dept]);
    if (existing) return res.json({ success: true, drive_folder_id: existing.drive_folder_id, already_exists: true });
    const folder = await dmsCreateFolder(dept, client.drive_folder_id);
    await db.query(
      'INSERT INTO client_department_folders (client_id, department_name, drive_folder_id) VALUES (?,?,?)',
      [id, dept, folder.id]);
    // Share with all users in this department + all admins (fire-and-forget)
    db.query(
      `SELECT DISTINCT email FROM users
       WHERE email IS NOT NULL AND email != ''
         AND (department=? OR role='admin')
         AND role != 'client'`,
      [dept]
    ).then(([members]) => {
      return Promise.all(members.map(m => dmsShareFolder(folder.id, m.email).catch(() => {})));
    }).catch(() => {});
    res.json({ success: true, drive_folder_id: folder.id, web_view_link: folder.webViewLink });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Remove a department mapping (does NOT delete the Drive folder)
app.delete('/api/clients/:id/dms/departments/:dept', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const id = req.params.id;
    const dept = decodeURIComponent(req.params.dept);
    const [doomed] = await db.query(
      'SELECT * FROM client_department_folders WHERE client_id=? AND department_name=?', [id, dept]);
    await archiveDeleted('client_department_folders', doomed, req, {
      summary: r => `DMS dept folder mapping: ${r.department_name || ''} (client ${r.client_id})`,
      reason: 'Mapping removed — the Drive folder itself is left untouched',
    });
    await db.query(
      'DELETE FROM client_department_folders WHERE client_id=? AND department_name=?', [id, dept]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// List files in a Drive folder (must belong to this client) — merged with any
// name+link entries pasted into this folder via the external-link feature,
// since those aren't real Drive objects and dmsListFiles() can't see them.
app.get('/api/clients/:id/dms/folders/:folderId/files', requireAuth, async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const files = await dmsListFiles(folderId);
    const [linkRows] = await db.query(
      `SELECT id, name, url, created_by_name,
              DATE_FORMAT(CONVERT_TZ(created_at, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%SZ') AS created_at
       FROM dms_external_links WHERE folder_id=? ORDER BY created_at DESC`,
      [folderId]
    );
    const linkFiles = linkRows.map(r => ({
      id: 'ext-' + r.id,
      name: r.name,
      mimeType: 'application/x-emk-external-link',
      webViewLink: r.url,
      modifiedTime: r.created_at,
      modified_by: r.created_by_name,
      size: null,
    }));
    const merged = [...files, ...linkFiles].sort((a, b) => {
      const aFolder = a.mimeType === 'application/vnd.google-apps.folder' ? 0 : 1;
      const bFolder = b.mimeType === 'application/vnd.google-apps.folder' ? 0 : 1;
      if (aFolder !== bFolder) return aFolder - bFolder;
      return a.name.toLowerCase().localeCompare(b.name.toLowerCase());
    });
    res.json(merged);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Create a Google Doc / Sheet / Slide in a folder
app.post('/api/clients/:id/dms/folders/:folderId/files', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const name = (req.body.name || '').trim();
    const kind = (req.body.kind || '').toLowerCase();
    if (!name) return res.status(400).json({ error: 'name required' });
    if (kind !== 'folder' && !DMS_MIME_TYPES[kind]) return res.status(400).json({ error: 'kind must be doc, sheet, slide, or folder' });
    const file = kind === 'folder' ? await dmsCreateFolder(name, folderId) : await dmsCreateFile(name, kind, folderId);
    await _dmsLogActivity(file.id, 'created', name, req, id);
    res.json({ success: true, id: file.id, web_view_link: file.webViewLink });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Upload an actual file (PDF, image, doc, etc.) into a Drive folder
app.post('/api/clients/:id/dms/folders/:folderId/upload', requireAuth, requireAdminOrPC, dmsUpload.single('file'), async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!req.file) return res.status(400).json({ error: 'file required' });
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const file = await dmsUploadFile(req.file.originalname, req.file.mimetype, req.file.buffer, folderId);
    await _dmsLogActivity(file.id, 'uploaded', req.file.originalname, req, id);
    res.json({ success: true, id: file.id, web_view_link: file.webViewLink });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add a simple name+link entry to a client's folder — NOT a real Drive object,
// just a DB row we merge into the file listing. No Drive API / sharing needed,
// unlike the (removed) Drive-shortcut approach — the user often doesn't control
// sharing permissions on files owned by other people.
app.post('/api/clients/:id/dms/folders/:folderId/external-link', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const name = (req.body.name || '').trim();
    const url = (req.body.url || '').trim();
    if (!name) return res.status(400).json({ error: 'Name is required' });
    if (!url || !_dmsIsSafeUrl(url)) return res.status(400).json({ error: 'A valid http(s) link is required' });
    const [result] = await db.query(
      'INSERT INTO dms_external_links (client_id, folder_id, name, url, created_by, created_by_name) VALUES (?,?,?,?,?,?)',
      [id, folderId, name, url, req.session.userId, req.session.name || '']
    );
    const linkFileId = 'ext-' + result.insertId;
    await _dmsLogActivity(linkFileId, 'created', name, req, id);
    res.json({ success: true, id: linkFileId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Step 1 of a large-file upload: get a Drive resumable-session URL.
// A direct browser PUT to this URL bypasses Vercel's ~4.5MB request cap, but
// Drive's resumable-upload response is missing CORS headers on completion,
// so the browser can't read the result even though the file was created.
// The browser instead sends chunks through /upload-chunk below, which
// proxies each one to this URL server-side (no CORS involved there).
app.post('/api/clients/:id/dms/folders/:folderId/upload-session', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const { name, mimeType, size } = req.body;
    if (!name || !size) return res.status(400).json({ error: 'name and size required' });
    const uploadUrl = await dmsInitiateResumableUpload(name, mimeType, Number(size), folderId);
    res.json({ success: true, uploadUrl });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Step 2: proxy one chunk of the file to the resumable session URL from
// step 1. Each chunk stays comfortably under Vercel's request-body cap even
// though the overall file can be far larger. Responds 308 (with the byte
// range Drive has received so far) while more chunks are expected, or the
// created file's metadata once Drive reports the upload complete.
app.post('/api/clients/:id/dms/folders/:folderId/upload-chunk', requireAuth, requireAdminOrPC, express.raw({ type: () => true, limit: '6mb' }), async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const uploadUrl = req.query.uploadUrl;
    const contentRange = req.headers['content-range'];
    if (!uploadUrl || !contentRange) return res.status(400).json({ error: 'uploadUrl and Content-Range required' });
    const chunk = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body || '');
    const fetchFn = global.fetch || (await import('node-fetch')).default;
    const driveRes = await fetchFn(uploadUrl, {
      method: 'PUT',
      headers: { 'Content-Range': contentRange, 'Content-Length': String(chunk.length) },
      body: chunk,
      redirect: 'manual',
    });
    if (driveRes.status === 308) {
      return res.status(308).json({ incomplete: true, range: driveRes.headers.get('range') || null });
    }
    const text = await driveRes.text();
    if (!driveRes.ok) return res.status(driveRes.status).json({ error: text || `Drive chunk upload failed (${driveRes.status})` });
    let file; try { file = JSON.parse(text); } catch { file = {}; }
    if (file.id) await _dmsLogActivity(file.id, 'uploaded', file.name, req, id);
    res.json({ success: true, ...file });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

async function _dmsValidFolderIds(clientId) {
  const [[clientRow]] = await db.query('SELECT drive_folder_id FROM clients WHERE id=?', [clientId]);
  const [deptRows] = await db.query(
    'SELECT drive_folder_id FROM client_department_folders WHERE client_id=?', [clientId]);
  return new Set([clientRow?.drive_folder_id, ...deptRows.map(r => r.drive_folder_id)].filter(Boolean));
}

// A folder qualifies if it's the client's own root, a registered department
// folder, OR any nested subfolder under either (e.g. one made via the
// "New folder" right-click action, which isn't tracked in our DB at all) —
// walk up Drive's own parent chain to confirm ancestry, bounded so a bad
// folderId can't trigger an unbounded walk.
async function _dmsCanAccessFolder(clientId, folderId) {
  const validIds = await _dmsValidFolderIds(clientId);
  if (validIds.has(folderId)) return true;
  try {
    const drive = await getDriveClient();
    let current = folderId;
    for (let i = 0; i < 10; i++) {
      const res = await drive.files.get({ fileId: current, fields: 'parents', supportsAllDrives: true });
      const parents = res.data.parents || [];
      if (parents.some(p => validIds.has(p))) return true;
      if (!parents.length) return false;
      current = parents[0];
    }
  } catch (e) { console.error('DMS ancestor check failed:', e.message); }
  return false;
}

// Rename a file/folder in a client's Drive folder
app.patch('/api/clients/:id/dms/folders/:folderId/files/:fileId', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const { id, folderId, fileId } = req.params;
    const name = (req.body.name || '').trim();
    if (!name) return res.status(400).json({ error: 'name required' });
    if (fileId.startsWith('ext-')) {
      const linkId = fileId.slice(4);
      if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
      const [r] = await db.query('UPDATE dms_external_links SET name=? WHERE id=? AND folder_id=?', [name, linkId, folderId]);
      if (!r.affectedRows) return res.status(404).json({ error: 'Link not found' });
      await _dmsLogActivity(fileId, 'renamed', name, req, id);
      return res.json({ success: true });
    }
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const drive = await getDriveClient();
    await drive.files.update({ fileId, requestBody: { name }, supportsAllDrives: true });
    await _dmsLogActivity(fileId, 'renamed', name, req, id);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete (trash) a file/folder in a client's Drive folder — moves to Drive's
// Trash rather than a permanent delete, so it stays recoverable.
app.delete('/api/clients/:id/dms/folders/:folderId/files/:fileId', requireAuth, requireAdminOrPC, async (req, res) => {
  try {
    const { id, folderId, fileId } = req.params;
    if (fileId.startsWith('ext-')) {
      const linkId = fileId.slice(4);
      if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
      const [[linkRow]] = await db.query('SELECT * FROM dms_external_links WHERE id=? AND folder_id=?', [linkId, folderId]);
      await archiveDeleted('dms_external_links', linkRow, req, {
        summary: r => `DMS link: ${r.name || ''}`,
      });
      const [r] = await db.query('DELETE FROM dms_external_links WHERE id=? AND folder_id=?', [linkId, folderId]);
      if (!r.affectedRows) return res.status(404).json({ error: 'Link not found' });
      await _dmsLogActivity(fileId, 'deleted', linkRow?.name || null, req, id);
      return res.json({ success: true });
    }
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const drive = await getDriveClient();
    let name = null;
    try { const meta = await drive.files.get({ fileId, fields: 'name', supportsAllDrives: true }); name = meta.data.name; } catch {}
    await drive.files.update({ fileId, requestBody: { trashed: true }, supportsAllDrives: true });
    await _dmsLogActivity(fileId, 'deleted', name, req, id);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
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

    // Get user's name + phone for WhatsApp
    const [[user]] = await db.query('SELECT name, phone FROM users WHERE id=?', [req.session.userId]);

    // Fire WhatsApp (don't await — don't block response)
    if (user && user.phone) {
      const msg = `✨ Hello ${user.name},\nThank you for submitting your daily task ✔️\nYour response for the date ${entry_date} has been successfully recorded in the database 📄✨`;
      sendWhatsApp(user.phone, msg).catch(e => console.error('WA send err:', e.message));
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

app.get('/api/compliance/last7', requireAuth, async (req, res) => {
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
app.get('/api/compliance/employee/:id', requireAuth, async (req, res) => {
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
       ORDER BY entry_date DESC, id DESC LIMIT 20`, [id, from, to]);

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
app.get('/api/compliance/employee/:id/week-tasks', requireAuth, async (req, res) => {
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
    const scope = req.query.scope || 'mine';
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
app.post('/api/leaves', requireAuth, async (req, res) => {
  try {
    const { leave_type, dates, reason } = req.body;
    const allowedTypes = ['full_day','half_day','work_from_home','extra_working'];
    if (!allowedTypes.includes(leave_type)) return res.status(400).json({ error: 'Invalid leave type' });
    if (!Array.isArray(dates) || !dates.length) return res.status(400).json({ error: 'Select at least one date' });
    if (!reason || !reason.trim()) return res.status(400).json({ error: 'Reason required' });

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
        const h = Number(d && d.hours);
        if (!h || h <= 0 || h > 24) return res.status(400).json({ error: `Hours required (1-24) for ${date}` });
        item.hours = h;
      }
      cleanDates.push(item);
    }
    cleanDates.sort((a,b) => a.date.localeCompare(b.date));
    const from_date = cleanDates[0].date;
    const to_date = cleanDates[cleanDates.length - 1].date;

    const uid = req.session.userId;
    const approverId = await resolveLeaveApprover(uid);

    const [r] = await db.query(
      `INSERT INTO leave_requests
       (user_id, leave_type, from_date, to_date, dates_json, reason, status, approver_id)
       VALUES (?,?,?,?,?,?,'pending',?)`,
      [uid, leave_type, from_date, to_date, JSON.stringify(cleanDates), reason.trim(), approverId]
    );

    // Notify approver — email + WhatsApp (best-effort)
    if (approverId && approverId !== uid) {
      const typeLabel = ({full_day:'Full Day Leave',half_day:'Half Day Leave',work_from_home:'Work From Home',extra_working:'Extra Working'})[leave_type];
      const datesLine = cleanDates.map(d => leave_type === 'extra_working' ? `${d.date} (${d.hours}h)` : d.date).join(', ');
      const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [uid]);

      const target = await getNotifyTarget(approverId);
      if (target) {
        const html = `
          <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#f6f9fc;padding:20px;">
            <div style="background:#fff;border-radius:8px;padding:30px;">
              <h2 style="color:#F39C12;margin-top:0;">🗓 New Leave Request</h2>
              <p>Hi <b>${target.name||'there'}</b>,</p>
              <p><b>${me?.name || 'An employee'}</b> has submitted a leave request for your approval.</p>
              <table style="width:100%;border-collapse:collapse;margin:14px 0;">
                <tr><td style="padding:8px;background:#f0f4f8;width:140px"><b>Type</b></td><td style="padding:8px;">${typeLabel}</td></tr>
                <tr><td style="padding:8px;background:#f0f4f8;"><b>Dates</b></td><td style="padding:8px;">${datesLine}</td></tr>
                <tr><td style="padding:8px;background:#f0f4f8;"><b>Reason</b></td><td style="padding:8px;">${(reason||'').replace(/</g,'&lt;')}</td></tr>
              </table>
              <p style="color:#777;font-size:12px;margin-top:20px;">E-Marketing Task Manager · Leave Tracker</p>
            </div>
          </div>`;
        sendMail(target.email, `Leave Request — ${me?.name || ''}`, html).catch(()=>{});
      }
      // WhatsApp to ALL HODs in same department (so both HODs get notified)
      try {
        const daysWord = cleanDates.length === 1 ? '1 day' : `${cleanDates.length} days`;
        const datesPretty = cleanDates.map(d => {
          const dd = d.date.split('-').reverse().join('-');
          return leave_type === 'extra_working' ? `${dd} (${d.hours}h)` : dd;
        }).join(', ');
        const [[submitter]] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
        let waRecipients = [];
        if (submitter?.department) {
          const [allHods] = await db.query(
            `SELECT name, phone FROM users WHERE COALESCE(user_role, role)='hod' AND department=? AND phone IS NOT NULL AND phone<>''`,
            [submitter.department]);
          waRecipients = allHods;
        }
        if (!waRecipients.length) {
          // fallback to single assigned approver
          const [[apRow]] = await db.query('SELECT name, phone FROM users WHERE id=?', [approverId]);
          if (apRow?.phone) waRecipients = [apRow];
        }
        const waHeading = ({
          extra_working: 'New Extra Working Request',
          work_from_home: 'New Work From Home Request',
          half_day: 'New Half Day Leave Request'
        })[leave_type] || 'New Leave Request';
        for (const hod of waRecipients) {
          const msg = `Hello ${hod.name || ''},\n\n🗓 *${waHeading}*\n\n` +
            `*Employee:* ${me?.name || ''}\n` +
            `*Type:* ${typeLabel}\n` +
            `*Duration:* ${daysWord}\n` +
            `*Dates:* ${datesPretty}\n` +
            `*Reason:* ${reason}\n\n` +
            `Please approve / reject from the Approvals tab.\n\n— E-Marketing Task Manager`;
          sendWhatsApp(hod.phone, msg).catch(e => console.error('WA leave req err:', e.message));
        }
      } catch (e) { console.error('WA leave req lookup err:', e.message); }
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
      const color = newStatus === 'approved' ? '#16a34a' : '#dc2626';
      const html = `
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#f6f9fc;padding:20px;">
          <div style="background:#fff;border-radius:8px;padding:30px;">
            <h2 style="color:${color};margin-top:0;">Leave ${newStatus === 'approved' ? 'Approved ✅' : 'Rejected ❌'}</h2>
            <p>Hi <b>${target.name || 'there'}</b>,</p>
            <p>Your leave request <b>${typeLabel}</b> (${datesLine}) has been <b style="color:${color}">${newStatus}</b>.</p>
            ${note ? `<p><b>Note:</b> ${(note||'').replace(/</g,'&lt;')}</p>` : ''}
            <p style="color:#777;font-size:12px;margin-top:20px;">E-Marketing Task Manager · Leave Tracker</p>
          </div>
        </div>`;
      sendMail(target.email, `Leave ${newStatus} — ${typeLabel}`, html).catch(()=>{});
    }
    // WhatsApp to requester
    try {
      const [[reqRow]] = await db.query('SELECT name, phone FROM users WHERE id=? LIMIT 1', [lr.user_id]);
      if (reqRow && reqRow.phone) {
        const statusIcon = newStatus === 'approved' ? '✅' : '❌';
        const statusWord = newStatus === 'approved' ? 'APPROVED' : 'REJECTED';
        const subjectWord = lr.leave_type === 'extra_working' ? 'Extra Working' : 'Leave';
        const [[apRow]] = await db.query('SELECT name FROM users WHERE id=? LIMIT 1', [uid]);
        const msg = `Hello ${reqRow.name || ''},\n\n${statusIcon} *${subjectWord} ${statusWord}*\n\n` +
          `*Type:* ${typeLabel}\n` +
          `*Dates:* ${datesLine}\n` +
          `*Decided by:* ${apRow?.name || 'Approver'}\n` +
          (note ? `*Note:* ${note}\n` : '') +
          `\n— E-Marketing Task Manager`;
        sendWhatsApp(reqRow.phone, msg).catch(e => console.error('WA leave decide err:', e.message));
      }
    } catch (e) { console.error('WA leave decide lookup err:', e.message); }

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
    // Notify only the host (organizer) + attendees — no client-group fanout.
    const dmTargets = new Set();
    const [[org]] = await db.query('SELECT phone FROM users WHERE id=?', [m.organizer_id]);
    if (org?.phone) dmTargets.add(org.phone);
    for (const a of atts) if (a.phone) dmTargets.add(a.phone);
    const dmResults = [];
    for (const phone of dmTargets) {
      dmResults.push(await sendWhatsApp(phone, internalBody));
      await new Promise(r => setTimeout(r, 600));
    }
    return { ok: true, dms: dmResults.length };
  } catch (err) {
    console.error('  ⚠️ Meeting notification failed:', err.message);
    return { ok: false, error: err.message };
  }
}

// List meetings (filter by date range / status / organizer).
// Every user is scoped to meetings they organize or are invited to — nobody
// sees someone else's private meetings here.
app.get('/api/meetings', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const { from, to, status, organizer } = req.query;
    let where = `(m.organizer_id = ? OR EXISTS
      (SELECT 1 FROM meeting_attendees ma WHERE ma.meeting_id = m.id AND ma.user_id = ?))`;
    const params = [uid, uid];
    if (from && /^\d{4}-\d{2}-\d{2}$/.test(from)) { where += ' AND m.meeting_date >= ?'; params.push(from); }
    if (to   && /^\d{4}-\d{2}-\d{2}$/.test(to))   { where += ' AND m.meeting_date <= ?'; params.push(to); }
    if (status) { where += ' AND m.status = ?'; params.push(status); }
    if (organizer && organizer !== 'all') { where += ' AND m.organizer_id = ?'; params.push(organizer); }
    const [rows] = await db.query(
      `SELECT m.id, m.title, m.agenda, m.client_id, m.organizer_id,
              DATE_FORMAT(m.meeting_date,'%Y-%m-%d') AS meeting_date,
              TIME_FORMAT(m.start_time,'%H:%i') AS start_time,
              TIME_FORMAT(m.end_time,'%H:%i')   AS end_time,
              m.meet_link, m.status, m.created_at,
              c.name AS client_name, u.name AS organizer_name
       FROM meetings m
       LEFT JOIN clients c ON m.client_id = c.id
       LEFT JOIN users   u ON m.organizer_id = u.id
       WHERE ${where}
       ORDER BY m.meeting_date ASC, m.start_time ASC
       LIMIT 500`, params);
    if (rows.length) {
      const ids = rows.map(r => r.id);
      const [atts] = await db.query(
        `SELECT ma.meeting_id, ma.user_id, u.name
         FROM meeting_attendees ma JOIN users u ON ma.user_id = u.id
         WHERE ma.meeting_id IN (${ids.map(()=>'?').join(',')})`, ids);
      const byMtg = {};
      for (const a of atts) (byMtg[a.meeting_id] = byMtg[a.meeting_id] || []).push({ id: a.user_id, name: a.name });
      for (const r of rows) r.attendees = byMtg[r.id] || [];
    }
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Slot map for a given date — used by the scheduler UI.
app.get('/api/meetings/slots', requireAuth, async (req, res) => {
  try {
    const date = req.query.date;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '')) return res.status(400).json({ error: 'date=YYYY-MM-DD required' });
    const userIds = String(req.query.userIds || '')
      .split(',').map(s => parseInt(s, 10)).filter(n => Number.isFinite(n) && n > 0);
    // Off-day check (Sunday + holidays) — return empty slots with a reason.
    const off = await (async () => {
      try {
        const holidays = await loadHolidaysSet();
        const d = new Date(date + 'T00:00:00');
        if (d.getDay() === 0) return { off: true, reason: 'Sunday' };
        if (isLastSaturdayOfMonth(date)) return { off: true, reason: 'Last Saturday (off)' };
        if (holidays.has(date)) return { off: true, reason: 'Holiday' };
        return { off: false };
      } catch { return { off: false }; }
    })();
    if (off.off) return res.json({ date, off: true, reason: off.reason, slots: [], busyRanges: {} });
    const { slots, busyRanges } = await buildMeetingSlots(date, userIds, req.session.userId);
    res.json({ date, off: false, slots, busyRanges });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Single meeting detail.
app.get('/api/meetings/:id', requireAuth, async (req, res) => {
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
       WHERE m.id = ?`, [req.params.id]);
    if (!m) return res.status(404).json({ error: 'not found' });
    const [atts] = await db.query(
      `SELECT u.id, u.name, u.email FROM meeting_attendees ma
       JOIN users u ON ma.user_id = u.id WHERE ma.meeting_id = ?`, [req.params.id]);
    m.attendees = atts;
    res.json(m);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Create meeting + notify.
// Expands a recurrence rule into the list of meeting_date strings it covers.
// Capped so a mistyped "repeat until 2099" can't spawn thousands of rows.
const RECURRENCE_MAX_OCCURRENCES = 120;
function generateRecurrenceDates(startDateStr, frequency, untilStr, customDays) {
  const start = new Date(startDateStr + 'T00:00:00Z');
  const until = new Date(untilStr + 'T00:00:00Z');
  const dates = [];
  if (until < start) return dates;
  if (frequency === 'monthly') {
    const cur = new Date(start);
    while (cur <= until && dates.length < RECURRENCE_MAX_OCCURRENCES) {
      dates.push(cur.toISOString().split('T')[0]);
      cur.setUTCMonth(cur.getUTCMonth() + 1);
    }
    return dates;
  }
  const customSet = new Set((customDays || []).map(d => parseInt(d, 10)));
  const startDow = start.getUTCDay();
  const cur = new Date(start);
  while (cur <= until && dates.length < RECURRENCE_MAX_OCCURRENCES) {
    const dow = cur.getUTCDay(); // 0=Sun..6=Sat
    let include = false;
    if (frequency === 'daily') include = true;
    else if (frequency === 'weekday') include = dow !== 0; // Mon–Sat
    else if (frequency === 'weekly') include = dow === startDow;
    else if (frequency === 'custom') include = customSet.has(dow);
    if (include) dates.push(cur.toISOString().split('T')[0]);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return dates;
}

app.post('/api/meetings', requireAuth, async (req, res) => {
  try {
    const { title, agenda, client_id, meeting_date, start_time, end_time, meet_link, attendee_ids,
            frequency, repeat_until, repeat_days } = req.body;
    if (!title || !meeting_date || !start_time || !end_time) {
      return res.status(400).json({ error: 'title, meeting_date, start_time, end_time required' });
    }
    const organizerId = req.session.userId;

    const freq = ['daily','weekday','weekly','monthly','custom'].includes(frequency) ? frequency : null;
    let occurrenceDates = [meeting_date];
    if (freq) {
      if (!repeat_until) return res.status(400).json({ error: 'Pick a "repeat until" date for a recurring meeting' });
      if (freq === 'custom' && !(Array.isArray(repeat_days) && repeat_days.length)) {
        return res.status(400).json({ error: 'Select at least one day to repeat on' });
      }
      occurrenceDates = generateRecurrenceDates(meeting_date, freq, repeat_until, repeat_days);
      if (!occurrenceDates.length) return res.status(400).json({ error: 'No occurrences fall in the selected range' });
    }

    // Auto-create Google Meet link if env enabled and caller didn't paste one — reused across every occurrence.
    let finalLink = meet_link || null;
    if (!finalLink) {
      const attEmails = [];
      const attIdArr = Array.isArray(attendee_ids) ? attendee_ids : [];
      if (attIdArr.length) {
        const [emails] = await db.query(
          `SELECT email FROM users WHERE id IN (${attIdArr.map(()=>'?').join(',')})`, attIdArr);
        for (const r of emails) if (r.email) attEmails.push(r.email);
      }
      finalLink = await createGoogleMeetLink({
        title, dateStr: meeting_date, startTime: start_time, endTime: end_time,
        attendeeEmails: attEmails
      });
    }

    const recurrenceGroupId = freq ? `rg_${Date.now()}_${organizerId}` : null;
    const newIds = [];
    for (const d of occurrenceDates) {
      const [result] = await db.query(
        `INSERT INTO meetings (title, agenda, client_id, organizer_id, meeting_date, start_time, end_time, meet_link, recurrence_group_id)
         VALUES (?,?,?,?,?,?,?,?,?)`,
        [title, agenda || null, client_id || null, organizerId, d, start_time, end_time, finalLink, recurrenceGroupId]);
      const newId = result.insertId;
      newIds.push(newId);
      if (Array.isArray(attendee_ids) && attendee_ids.length) {
        const values = attendee_ids.filter(n => Number.isFinite(parseInt(n))).map(uid => [newId, parseInt(uid)]);
        if (values.length) {
          await db.query(
            `INSERT IGNORE INTO meeting_attendees (meeting_id, user_id) VALUES ${values.map(()=>'(?,?)').join(',')}`,
            values.flat());
        }
      }
    }
    // One WhatsApp ping for the series (not one per occurrence) — the existing
    // 10-min pre-meeting reminder cron still fires individually for each date.
    sendMeetingNotification(newIds[0], 'created').catch(e => console.error('notify err:', e.message));
    res.json({ ok: true, id: newIds[0], meet_link: finalLink, count: newIds.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Reschedule / edit meeting + notify.
app.put('/api/meetings/:id', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const { title, agenda, client_id, meeting_date, start_time, end_time, meet_link, attendee_ids } = req.body;
    const [[existing]] = await db.query('SELECT organizer_id, meeting_date, start_time, end_time FROM meetings WHERE id=?', [id]);
    if (!existing) return res.status(404).json({ error: 'not found' });
    // Only organizer or admin can edit.
    if (existing.organizer_id !== req.session.userId && req.session.role !== 'admin') {
      return res.status(403).json({ error: 'only organizer or admin can edit' });
    }
    const rescheduled = (meeting_date && meeting_date !== String(existing.meeting_date).slice(0,10))
                    || (start_time && start_time !== String(existing.start_time).slice(0,5))
                    || (end_time   && end_time   !== String(existing.end_time).slice(0,5));
    await db.query(
      `UPDATE meetings SET
         title=COALESCE(?,title), agenda=?, client_id=?,
         meeting_date=COALESCE(?,meeting_date),
         start_time=COALESCE(?,start_time), end_time=COALESCE(?,end_time),
         meet_link=COALESCE(?,meet_link)${rescheduled ? ', reminder_sent=0' : ''}
       WHERE id=?`,
      [title, agenda || null, client_id || null, meeting_date || null, start_time || null, end_time || null, meet_link || null, id]);
    if (Array.isArray(attendee_ids)) {
      await db.query('DELETE FROM meeting_attendees WHERE meeting_id=?', [id]);
      const values = attendee_ids.filter(n => Number.isFinite(parseInt(n))).map(uid => [id, parseInt(uid)]);
      if (values.length) {
        await db.query(
          `INSERT IGNORE INTO meeting_attendees (meeting_id, user_id) VALUES ${values.map(()=>'(?,?)').join(',')}`,
          values.flat());
      }
    }
    sendMeetingNotification(id, rescheduled ? 'rescheduled' : 'created').catch(e => console.error('notify err:', e.message));
    res.json({ ok: true, rescheduled });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Change meeting status — mark done / reopen. Organizer or admin only.
app.put('/api/meetings/:id/status', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const status = String(req.body.status || '');
    if (!['scheduled', 'done', 'cancelled'].includes(status)) return res.status(400).json({ error: 'Invalid status' });
    const [[existing]] = await db.query('SELECT organizer_id FROM meetings WHERE id=?', [id]);
    if (!existing) return res.status(404).json({ error: 'not found' });
    if (existing.organizer_id !== req.session.userId && req.session.role !== 'admin') {
      return res.status(403).json({ error: 'only organizer or admin can change status' });
    }
    await db.query('UPDATE meetings SET status=? WHERE id=?', [status, id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Cancel (soft) — status flips to cancelled, notification fires.
app.delete('/api/meetings/:id', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const [[existing]] = await db.query('SELECT organizer_id FROM meetings WHERE id=?', [id]);
    if (!existing) return res.status(404).json({ error: 'not found' });
    if (existing.organizer_id !== req.session.userId && req.session.role !== 'admin') {
      return res.status(403).json({ error: 'only organizer or admin can cancel' });
    }
    await db.query("UPDATE meetings SET status='cancelled' WHERE id=?", [id]);
    sendMeetingNotification(id, 'cancelled').catch(e => console.error('notify err:', e.message));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
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

// ══════════════════════════════════════════════════════
// INVENTORY MANAGEMENT
// ══════════════════════════════════════════════════════

// Why an assignment ended, and where that leaves the item. Single source of
// truth for both the valid reasons and the status each one implies — keep in
// step with the return_reason ENUM and inventory_items.status.
const INV_RETURN_REASONS = Object.freeze({
  damaged:     { label: 'Damaged',     itemStatus: 'damaged'   },
  retired:     { label: 'Retired',     itemStatus: 'retired'   },
  offboarding: { label: 'Offboarding', itemStatus: 'available' },
});
// Own-property check, not a bare lookup: reason is client-supplied, and
// `INV_RETURN_REASONS['constructor']` would otherwise pass validation and then
// blow up with an undefined itemStatus.
const invReturnReason = r =>
  (typeof r === 'string' && Object.hasOwn(INV_RETURN_REASONS, r)) ? INV_RETURN_REASONS[r] : null;
// Retiring an item is a judgement about the asset's life, so it stays with the
// custodian — the person handing kit back can only say why they're handing it
// back, not that it's finished.
const INV_HOLDER_REASONS = new Set(['offboarding', 'damaged']);

// Get all items (admin/hod see all; others see only assigned to them)
app.get('/api/inventory/items', requireAuth, async (req, res) => {
  try {
    const isAdmin = ['admin','hod'].includes(req.session.role);
    let rows;
    if (isAdmin) {
      [rows] = await db.query(`
        SELECT i.*, u.name AS assigned_to_name, u.id AS assigned_to_id,
               a.id AS assignment_id, a.assigned_at, a.handover_status, a.return_reason,
               cu.name AS created_by_name
        FROM inventory_items i
        LEFT JOIN inventory_assignments a ON a.item_id = i.id AND a.handover_status IN ('active','pending_handover')
        LEFT JOIN users u ON u.id = a.user_id
        LEFT JOIN users cu ON cu.id = i.created_by
        ORDER BY i.created_at DESC`);
    } else {
      [rows] = await db.query(`
        SELECT i.*, u.name AS assigned_to_name, u.id AS assigned_to_id,
               a.id AS assignment_id, a.assigned_at, a.handover_status, a.return_reason,
               cu.name AS created_by_name
        FROM inventory_items i
        JOIN inventory_assignments a ON a.item_id = i.id AND a.user_id = ? AND a.handover_status IN ('active','pending_handover')
        JOIN users u ON u.id = a.user_id
        LEFT JOIN users cu ON cu.id = i.created_by
        ORDER BY i.created_at DESC`, [req.session.userId]);
    }
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add new item (admin only)
app.post('/api/inventory/items', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Admin only' });
  try {
    const { name, type, brand, model, serial_number, photo, item_condition, notes } = req.body;
    if (!name || !type) return res.status(400).json({ error: 'name and type required' });
    // Brand/model are what tell two items of the same type apart, since the
    // form has no free-text name field.
    if (!brand || !String(brand).trim()) return res.status(400).json({ error: 'Brand is required' });
    if (!model || !String(model).trim()) return res.status(400).json({ error: 'Model is required' });
    const validTypes = ['laptop','keyboard','mouse','mobile','sim','charger','other'];
    if (!validTypes.includes(type)) return res.status(400).json({ error: 'Invalid type' });
    const [r] = await db.query(
      `INSERT INTO inventory_items (name,type,brand,model,serial_number,photo,item_condition,notes,created_by)
       VALUES (?,?,?,?,?,?,?,?,?)`,
      [name, type, brand||'', model||'', serial_number||'', photo||null, item_condition||'good', notes||'', req.session.userId]);
    res.json({ ok: true, id: r.insertId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Self-report equipment an employee already has — creates the item and
// immediately assigns it to the reporting user, no admin approval step.
app.post('/api/inventory/self-add', requireAuth, async (req, res) => {
  try {
    const { name, type, brand, model, serial_number, photo, item_condition, notes } = req.body;
    if (!name || !type) return res.status(400).json({ error: 'name and type required' });
    // Brand/model are what tell two items of the same type apart, since the
    // form has no free-text name field.
    if (!brand || !String(brand).trim()) return res.status(400).json({ error: 'Brand is required' });
    if (!model || !String(model).trim()) return res.status(400).json({ error: 'Model is required' });
    const validTypes = ['laptop','keyboard','mouse','mobile','sim','charger','other'];
    if (!validTypes.includes(type)) return res.status(400).json({ error: 'Invalid type' });
    const [r] = await db.query(
      `INSERT INTO inventory_items (name,type,brand,model,serial_number,photo,item_condition,notes,status,created_by)
       VALUES (?,?,?,?,?,?,?,?,'assigned',?)`,
      [name, type, brand||'', model||'', serial_number||'', photo||null, item_condition||'good', notes||'', req.session.userId]);
    await db.query(
      `INSERT INTO inventory_assignments (item_id, user_id, assigned_by) VALUES (?,?,?)`,
      [r.insertId, req.session.userId, req.session.userId]);
    res.json({ ok: true, id: r.insertId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Update item (admin only)
app.put('/api/inventory/items/:id', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Admin only' });
  try {
    const { name, brand, model, serial_number, photo, item_condition, status, notes } = req.body;
    await db.query(
      `UPDATE inventory_items SET name=COALESCE(?,name), brand=COALESCE(?,brand), model=COALESCE(?,model),
       serial_number=COALESCE(?,serial_number), photo=COALESCE(?,photo), item_condition=COALESCE(?,item_condition),
       status=COALESCE(?,status), notes=COALESCE(?,notes) WHERE id=?`,
      [name||null, brand||null, model||null, serial_number||null, photo||null,
       item_condition||null, status||null, notes||null, req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete item (admin only, only if not currently assigned)
app.delete('/api/inventory/items/:id', requireAuth, async (req, res) => {
  if (req.session.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
  try {
    const [[item]] = await db.query('SELECT * FROM inventory_items WHERE id=?', [req.params.id]);
    if (!item) return res.status(404).json({ error: 'Not found' });
    if (item.status === 'assigned') return res.status(400).json({ error: 'Cannot delete an assigned item. Return it first.' });
    await archiveDeleted('inventory_items', item, req, {
      summary: r => `Equipment: ${r.name || ''} (${r.type || ''})${r.serial_number ? ' SN:' + r.serial_number : ''}`,
    });
    await db.query('DELETE FROM inventory_items WHERE id=?', [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Assign item to user (admin only)
app.post('/api/inventory/assign', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Admin only' });
  try {
    const { item_id, user_id } = req.body;
    if (!item_id || !user_id) return res.status(400).json({ error: 'item_id and user_id required' });
    const [[item]] = await db.query('SELECT status FROM inventory_items WHERE id=?', [item_id]);
    if (!item) return res.status(404).json({ error: 'Item not found' });
    if (item.status === 'assigned') return res.status(400).json({ error: 'Item already assigned' });
    await db.query(
      `INSERT INTO inventory_assignments (item_id, user_id, assigned_by) VALUES (?,?,?)`,
      [item_id, user_id, req.session.userId]);
    await db.query(`UPDATE inventory_items SET status='assigned' WHERE id=?`, [item_id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Get all assignments (admin/hod)
app.get('/api/inventory/assignments', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Admin only' });
  try {
    const [rows] = await db.query(`
      SELECT a.*, i.name AS item_name, i.type AS item_type, i.brand, i.model, i.serial_number, i.photo,
             u.name AS user_name, u.department,
             ab.name AS assigned_by_name
      FROM inventory_assignments a
      JOIN inventory_items i ON i.id = a.item_id
      JOIN users u ON u.id = a.user_id
      LEFT JOIN users ab ON ab.id = a.assigned_by
      ORDER BY a.assigned_at DESC`);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Initiate handover — flags the item as pending return.
// Two ways in: an admin/HOD starting the handover when someone leaves, or the
// holder themselves saying "I'm giving this back" from My Equipment. Either
// way it only raises the intent; an admin still has to confirm physical
// receipt via /return, so this is deliberately NOT admin-only.
app.post('/api/inventory/handover/:assignment_id', requireAuth, async (req, res) => {
  try {
    const { notes, reason } = req.body;
    if (!invReturnReason(reason)) {
      return res.status(400).json({ error: 'A valid return reason is required' });
    }
    const [[a]] = await db.query('SELECT * FROM inventory_assignments WHERE id=?', [req.params.assignment_id]);
    if (!a) return res.status(404).json({ error: 'Assignment not found' });
    const isAdmin = ['admin','hod'].includes(req.session.role);
    if (!isAdmin && a.user_id !== req.session.userId) {
      return res.status(403).json({ error: 'You can only return equipment assigned to you' });
    }
    if (!isAdmin && !INV_HOLDER_REASONS.has(reason)) {
      return res.status(403).json({ error: 'Only an admin can retire an item' });
    }
    if (a.handover_status !== 'active') {
      return res.status(400).json({ error: 'This assignment is not active' });
    }
    await db.query(
      `UPDATE inventory_assignments SET handover_status='pending_handover', handover_notes=?, return_reason=? WHERE id=?`,
      [notes||'', reason, req.params.assignment_id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Complete handover — admin confirms physical receipt.
// The reason is the admin's final call (it may correct whatever the holder
// claimed when they raised the return) and decides where the item lands:
// damaged/retired take it out of circulation so it can't be assigned again,
// offboarding puts it back in available stock.
app.post('/api/inventory/return/:assignment_id', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Admin only' });
  try {
    const { reason } = req.body;
    const mapped = invReturnReason(reason);
    if (!mapped) return res.status(400).json({ error: 'A valid return reason is required' });
    const [[a]] = await db.query('SELECT * FROM inventory_assignments WHERE id=?', [req.params.assignment_id]);
    if (!a) return res.status(404).json({ error: 'Assignment not found' });
    await db.query(
      `UPDATE inventory_assignments SET handover_status='returned', returned_at=NOW(), return_reason=? WHERE id=?`,
      [reason, req.params.assignment_id]);
    await db.query(`UPDATE inventory_items SET status=? WHERE id=?`, [mapped.itemStatus, a.item_id]);
    res.json({ ok: true, itemStatus: mapped.itemStatus });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// HRM — INTERVIEW MANAGEMENT
// ══════════════════════════════════════════════════════
const HRM_AMUFIY_API_KEY  = process.env.HRM_AMUFIY_API_KEY  || 'sl_f7f604b7eeb89f938399b888621a341f2183bceea4bcb9650f3b8a529d396bfe';
const HRM_TEXT_ENDPOINT   = 'https://api.aumpfy.com/api/apis/trigger/emk-dbde65';
const HRM_FILE_ENDPOINT   = 'https://api.aumpfy.com/api/apis/trigger/hrm-file-6b7116';
const HRM_COMPANY         = process.env.HRM_COMPANY || 'E-Marketing';
const HRM_OFFER_FOLDER_ID   = process.env.HRM_OFFER_FOLDER_ID   || '1DWfwjSdkVP_sDEe62mM50Mc1mV52f6rA';
const HRM_OFFER_TEMPLATE_ID = process.env.HRM_OFFER_TEMPLATE_ID || '11f3STYRR4Lyk2HaoBfo7Kiiw5DsEoyr0P3lZnpZR_G4';
const HRM_OFFER_SCRIPT      = process.env.HRM_OFFER_SCRIPT      || 'https://script.google.com/macros/s/AKfycbyDG7Wqih7LW3p7ttqONoqzwy5t5Gq7B3RgTxEJcD3QL6qzALTMaC3cUvnxW2CGT3VQ/exec';

// Logo: pre-sized 185x110 PNG hardcoded as base64.
// Google Docs renders base64 at natural pixel size (ignores HTML w/h attrs),
// so the image must already be 185x110 before encoding — which this file is.
const _HRM_LOGO_SRC = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAALkAAABuCAYAAAB7lrLLAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAACXuSURBVHhe7V0HtBRF1p7HQyUtuAqCgq67uvqLGMBd3V0VVMAE6C4suLqAJDEjScSIrnFFMaFIeIAkyUEBBQUMwGMBFRBFEFRyFCS8N3nuf75bXdXVPT05vHnPued8Z2aqq6uqu7+6feveqhpXKBS6ORQKDckjj4oKVygUGkF5yUsFFpB8CL6EQqE8KgqCDmnxINnzchgRSR6MdbHBYHhaphCrLSmAHNJyGhm8FxUN8tlGJHlZwCRcFjuQE5LswBm7h0m2J1vIdUWRUyTPIxxpJ1AudJistUFwOu0kj/ehxJuvvKCiXU+uIOiQlgiY6ekmeUrIYA+3k9D+OxoMlRCWrpDBdiukUEfUtseAOjeF+ssayZE8mxcco65UHmC5hdM9SeT5JYCEeBEFaX1ODtcfrfzkSJ5HxhHtocWEAwlyFlloa57kv3Kk1JnKCX4dJM+CtkgJud6+co6cJ/mvQdPkkVnkPMlzDnmtW+5QYUhekTV+Rb62bKD8kTzHNGm6CRjM0PWlu50KOcIbx+sz5vmUG5I7XkQiwPUZ4GvVvsdz7SnXX56RoY6XLWSF5GkjSBw3G8w1PkwCSwT9FPIcomDJbgqV7KJQyW4Keg5biJ/J+5BHtiH4Ask4ybMGyWpcS8BNwf0rKfDtm+T7rAd55zUjz/T/o9KJ9ah03InkHn8ieSecRKUTTiP3jCYUWNqNAj/OomDAY7yCK8D9yDbiUEJlgfhInmuN1+dU68T2HqbAT7PI92kX8kz5A3mKXOQZ5iL3MBd5RrjIDYx0kWeUi0pHuqjE+M7HhrmoBHmnNqTg9nmivHQS3XYPo97vdCHJ55apcUFakUAb4yN5FOimSNrMknigkTu4bxX5lt1Hnkn1yf22QWoQuMhF7qIC8owGKim4te/yd2kRvheQd7iLPG+7yLdmsEnEdC1USODBOCLV89OIrD7rFJEyyTOFyDfRJHdgx2LyLmjN2tj9lkHs0YBBXhC8qBJ5JaHHGNC+g+CC9GZHgLYvfctFwS1TDKLn1r3JOeRQ51PQ2pSzJHeClOD+L8i38GZyDxdkdMPkYI0tSOpVZNY1eIGV5Ez0QkuaSfhK5B3pIu/EuhTy/JxhN1n2CVIuzJE0IrdIHuHmSwl5DpJ/RV9yF1UWJglsbtbWVoJ7xxhafYSRZ0yhkV6oyGzR7DbtLlDAbwf/16+Juh3alQtQb7wI9+7XDHlv4iN5Gm9gZDMkAgyCw/PhnvR78sAsUSaJaW+DwL4xBWxPl0KzTz+H/LMvIv/YyoLsrLUFySVMsheY2h8mjAGU5Z3X3CB5lPuTIcS6V7GOZxxp5EUmER/J0404bw63KVBK3qX3MuHYC2JoXEFKkNMgKMyL4S7yL+lI/r0rhE8cnWP/avLNuoTND9NUsQ5EFektmlyU6Z78Owr5SwyTxaHdcV5LLJQ5YSswyobkMSG0d/DwJvLMvpTcbxp2t2ZvC/PDsLNHuMg3uioFvp8gTsS16CjZSb4JJwnTRSM5l8UDVJvHRQ5IUee4kyhUui/DdnnyneVX3zniuG9xkzx7N1OQNLDzI3JPOIVdghbNK7Ws1OjQ4KNrUHDnYkFuu1lhkN6zuD2XJYntCJBdgslvkLxkT3Ikj+MB5BEfUuFf3CSPibT4kg2Cb5lEnqLKYvCoEdokuGGiQNMOr0ShHR9GJSHEt/oxDgyZJouAdC/C3SigaXO4JrPiYckjU5CkSg/JU4ZB8I0jOTLJBFZkNM0LCf79losC617i88I0uA4i8n/1NHtL7GaJMk1sXhppk3tnnEuhoDdPcqCcvpkcSQ5L1p4x02CCbxojPCEOBNc1OZMcpsf85qbd7VCmgOg83uK+7FO3lGl4ZeTbwa7JPcNd5FtwQ+xOVE4fftIoZ9frSHJGWsyP+MAE/2mWME8sBDcIqWtwoKiAvKNPoOChDTEIDohO4FvUVnhojDKll0YFhNgWN1ySqGtsJTZvfF8+ZZDcXm6OIRbxYh2voJCDMmeSZwkQzBb0jKnmbIMb9rf0prBHBeRb8UB85EOeoI+8M88VLkHDm2JGQAvJPbpQaXDVkXB8pIvnxYTVk0XCpDLgKreIdn+1Y/Hem6ySPKxRqLN0L3km/84kuEFyRTZNg0O7eotc5B1/IoVKd8ehxUUdwYPryDO6shFEErY3myajjQCREREVZowwZdgTM/tSYbrFqicYDL+2CoTyfm1ZJbkF4i1Cng+uEyF6EBgmgo3cFi0+toC8b7vIv6q/oV1jtVnU4V/zrGGqGATW3ISesYWi86AObQCKQa1/0zijHnu5FRDRtGc5R+okT/LmCPI9L2YPGlFLRTYbBOFhh6MzVKHgkR9ia1eATRU/h/jFDEXxllDeFLwxxhZyB9PfIOybn34+kRE1DSs3WSR5r4B4tWmqG2RWRKRO8iQACRz4ktwjjzMmUIUTWye40uzwdiz5l6FdY7WXs5F/8yTxppDRUeVdMb02oh7jN7T4cBcFty9ImuDZvJd5xFYe0Uke4+SkwNo1QL73/yKCM7r3RJksctagbroYA8HdS+IjEfIEvOSdLgacemcRZLZ5cKQWH+oiX3Ev544k70cm7ku2UI7GD8m1M/zZRCe5A5KrWDsf7rz1Q6n0TYTjpUbVPChssmiDQWmqjHSRb3YjCoUCcWlY1uJfDhJaXKvH1Oa6Rje0OOzwuc24c8RTR84gi+5eCSn29FxEwiRPCfB0uPeTd1Jdsd5SD7zIqbCS2IZ9rn6/7aLAmqedyWd5yOLmB7Z/SO4RdiLbTBSD7PwdY4PpF1LIfSDs4cngmC5hbbC1xS5h+dIAdbEZrMMJdrEfzzVAskRycUN8K/rxYFMnmYLFPJEQA073qEoUPLA2xk0VdQT2FJNn7G8tg03dLNGJjvLRHt+sJhQqNSZiRdCMXq+X5s2bR99u2BC1HbIhxcXFtGjRoqh5k4Ws44svvuA2+f1+YV5FaHsykGJP27p1G910003UpEkTGj58eFiesoLdyjDsTW5fdkgOLX50K3nH1jCjmjwYFMSzr9ax2OSYSju7iSonrGwjHRLYNo+879QS81+4I5naW5SrmSwY9L7pIt/8ljwJK9o9gDz44IPkcrmoZs2a9NOPP1nyyyVlUkA85AXGvZN+VyRk2bLlVKlSJa7joYceSmsddtHTH354oLq2qlWr0uHDhy15ygp2kqv0bJBcMtC3oj/PDVcLHXTNyjMCtXWY0p1ohNf9Xzxq3EhbO42HAPGveU5tOWFfBaS/JRiIfiJyuuz+6HZ+0KyhabOm6uF2795DpVuuFe3w++miCy9UeXs/IKKzqCSs/CQg5e2331Z1XH311Srdnj9RQL7/fjNdeeWV9Oc//5mWL18uC+aPSZMmqXpbNG9OPp8v5XojETQdgISRPNUGhwFa3HOAPJNqO5gQDtC0OHeIUS4K7vnUbJfgi5LgnmXkndtU+Lc1l6RJas3exzYVWIQxri4FtrwryoxxvVKat2ihHu5xxx1HGxzMFsi4ceNUPquWFXnttrQUe7163fbfkKKiIlXHjTfeGDFvpDJ1sR9/6aWXVNmdOnYy8xmdfs2aNfTJJ59QybESdb7+UJzqsbcjUnvsafb8iQISRvJ0A+L/dqjpMjQ0uL463gkgKm8P8e4Z7HbkNsorD3gosG0+eT5uZ8wrx7wUUa4wdbSV+Aj4GOs/vTB9lnSh4LHtxg2Mfd1SWmgkB2699TZ1TObzeDz0xz/+0ZJv4EMDjUwqO8uxo0dp586d9PMBYSpJkS5KiNvtptLSUsvxgwcP8ufo0aMdSS6lpEQQEGK/lmPHjnHdetn68VdffVWV3aVLF5XuKNp5h38RpouUo0eP0o4dO8jjdqs0p3v7yy+/cD793Yz2BwLGc3d4LvECEhfJk3md8DlGuZ45lzARlYkCz4qcv620to3omF8C23ry7yn4y0byH1hH/o3vkPezHuSZerbYkoI7jjm7ULe5RXli7SdjbnNzBVGM67VchyEtWlpJXlipEq1ZYw6GIcOGmSaExIAHH1RlwH59+eUh1Lx5c6pfvz7V/M1vqHbtk+nyyy+n8eOF7S5Js7y4mP7whz/QWWedRRs2fEeTp0yhiy66iOrUqUOvvDKEpkyebJL8BivJ58yZQ2eccQabHLt371HpW7ZsYdI2qF+fqlWrRr8/80zq3fsBOnToEB+fPXsOXXfddXT2WWepsuufdhq1bNGCWrduTeu/Xk8//3yQrrj8cqpXrx6NGjVKlY3rQlq7tm1p9+7d1KtXL77G6tWq8TW8/PLLnE+/rz/88APdeuutVLduXapRozpf33vvvUdLly7j9l/QqBF99913lvMSBSQukicD2SkD+1YKW1mtsTSILU0TLQBkkt2cSOXjcP4JVIpOgdX4w1xUygNLadtLUguC4+0AE4fNEmju+S0puG2u0hGi/4W3NxKktGzZUpFbEuDmm29Wx4+VlNDvzjhD6wQFguQDBqg8bdq0CesEOsaPG6/y9uvXV6VfqNn4wLXXXkuTNZK3atVKnTd37jyqXLmyOgbCQ9av/4ZOOeWUsDqBP/3pTzyWaNW6lUorcMg3ZMgQ+vjjj9XvSy/FJDYhIDLSCgsK1Hc7YGJJ2bZ9O51++ulheYDTT2+gvj/33HOc3/5c4gUkYyQHIL6VDwk/tKG5rXO5DfBEKS3SqTSzyI+9C1ljw+XnRGqcB2JDs6MTjD2ZPIu7UWD3cpPcSV6jFGmunHbqqXT99dfz94KCAiouXsHHBw8WdizIDe10/HGCaP37iwllEJgVSPvLX/5CTz/9NJscHTt2VA+04XkN1UCuf/9+og7t4UPjd7n9dlq7dp2F5DfcIBZ3LFq8mKpUqaLSr2rWjM0bvPah1WV6//4P0qeffkqPPvooVTI6Iwi8bt06uu++++iKK65Q9Z577rl09913U+/evWnPnj00ffp0VQ7KhMC71PD8hiodwBvjhRdeoIYNz1Npeqe47bbbVDreKlAGL774Ip17zjmWcp555hnOb38u8QLiTPJ0hK65zAB5ZjZi00Ju2+ZIdLabNa1ss9ullkcACcd82EAIpIadPUyYI+7xp5Dv4/YU2DSegqX7xI1JQnPbIUWSHObCZ599TieeeCL//vvf/872I8iP3zBFFiz4UD2kAQ8KTY6yYHt+/fXXqkwp5xgPtnr16mwrQzBglWXUrl2bli1bZjkHpoI83rFjJ1q9ejXVqFFDpcHjApsYAg+JTL+pTRtLOW3atOb0xo0bq7Q333xT5e95xx2W/BMmTFDHJMlxbfrbpp/WsdGZoAyQ3qBBA35jwDz6Tc2aKv+kiZNU/v3791veiM8++6yqIxlAnElu/+2AmHY6z+X+mjyYtw2vBi8x02xuns8tia6ZL8Z3zosJUyAzzBMQ2iA1fOfeiSeTf15T8v9vAAW2fUAht7Hg2LhZKZFbC6pIkSSvWrUaHTlyhJ544gn+XaNaNdak8qF8/vnn/GDl74GGdwXaDoJB36xZs/jh9e7Th3p0784kRt7q1arS5i1bOF+/fkKTA/fcfbdqhxR94IlOArsW36GB27VtZxlUDh06VOW94IILqGf37tSlc2fWtmeffTan1z75ZOXzhv0s83fu3Fmrlejdd99VxyTJIY0uaKTSP/vsM5UOu/uEE07g9Hp16/KbCh1d5j21Xr2wwXW3bt3U8Zw2VyD+DcMMr4phZ1vscazKMQeeJoSPm8mO8P/42uSddQGvt/QX96LAhuEU3L2UQu79itQKqRA7AqQ0b9Gcb3r1atVp3779rJVr1aqlHgbQ/JprOO+cObNVGoJIUsaMGUMnnXSS5RygoEB8Vq1ShTZt2sR5ZfAJUHa91oulJrfbztCa8KHrgle+zCsDSBLVqlblANdVV11FHreH8+skh3kEkffCieRQaRdoJP/kE+HyheB6qkiS16vHJEekVuY983dnkNfjtdTRq9f96vjzzz+vjsUD+3RjSGZIblyg95N/GfsWIjyvk1yzx3Vy62SHtl7Rj4LuAxYic1szSGqGvszKEEXy6tXphx9+5DSpzSWkBps5a6ZKk35ymCFVqwp7GZ0Dmvj7779nO/e884TdCo238buNnH/gQDOy2LdvP9UO2aZRRaa5As8HbGm9LW+8/oY6Z+TIkSodYwC8Tfbt20f79u9n0wFTFnR5cfBglb9b166WY04khzRqdL5Khw9dCpO8iiB5nTqncN179u7ltxbSCgsLaZkMOBlycePGqqyENbltagPEJHka5z2g6GDAR55p54gNgKT2tpgqGGiKRQtqwCmJz4PMKhQ6uj11QscxvohmekkxzZWqiuTQ5qeeWo/TMZ9DCswR+ZCkuQIzRqY1bnyxyrtt2zaqZdinILnU5AMGmJpcDl71NhUVmeYKxgWQTp07qTQAg0nIunVrVRrs4u3bRZwAAt/+xIkT6ZMlJjFfeeUVlf9q4+0kZfz48epY06ZNOQ1tatTI1ORLlogp0ZCNGzfS8ccfz+n16tWlXwyTCINimR+D2/fff5/NvPb//CenyTdU5gaeKQISPLyFV/JgXaYkt+kZMbwpamWObp8X8qDSO+UMCvmOGgQPryNbkIKBHG46SP7jj4LkENiX77zzDhNeysyZpibv26cPp+3du5fNAqQVFlainj17MgkbNKiv8lY94XgmBaRv3z4qvU/v3pymt0kP619/3fWqbt1bA4DAkH8a5AHgm4cHqH2HDkx6pNWpXZsJD1m2bKnKC7I1a9qUbfNvv/3W4l3529/+JioNheg8zYvy8ccfqfYgMnz8ccdx+il16tDPP4vgFzqC3dSSqFG9uvouNXk0RRQNkLSTXAYz/FvnGtu8aZv6sF9ckls3WaymCgaa3jl/UjfKXocj4tDYyUCKdBvCpt2xQ3hA9OP6b32C1mOPPaaO6wNACQRMzj9fvOphTyPyB3n88cdVHnyX5csB7NSpU9Vx3V8P6djx3+qYtOd/+eVQWNRWx1133WWZaGY3fwAQbsWKFeo3PElSMKCV6f/73/9UOt5UkszoSIcPH1HH3pszh7W4PA/eIdjget1Z0eTJ9CCIb/2rvDjC1N7GxCtJcs0WZ82umTRwOXoXiCieaEJ4HdmCHAisXLWSOnXqRCNGjBD92CEv5+eQdCk9/PDDdMcddyiXoDy28KOFdHuXLnTdtdfS/fffT9u2bacvv/ySNSvKlrJr1y4+v2fPOziCKMtQdRwr4RmBnW+/nb7+er2lDnw++ugj1K1bdy5HCtKnTJlC3Xv04IBSm9atecygDxRlGZDZc2ZzGxDE6t69O3tK4AJ85JFHuL1fffWVygs7vEOHDjR48GD2y+v3b+SokfzWmD5tmsqvC8qBqbJrl7jOf//b7KSvvvIqp9nvc7yAOJI80gOMD6LhvuJ7jZU5uufEOsAUNrltwGksJvYu6WhcXPIXmDIcXIn2ewWIEb2hBa1ZLfnjEVFEeN5o7eHj8pg2c1Idi5BuF/2a4hbj+dhF1elw7yCYqgAXJgbFGIxKWbBggcXfL+MDetsSAcSR5CnBaKxnYWvDXDFNFTvJnYAwP5P8c+EbDis/04hi9iTW+SO3XYo9PZtIpA32fMbFhedzSLNDCt5iksgnnliLmjRpbBm8Ate2bGl5KyQDSEySx9NwC0TnJu97l4plbjZ7nDW3HuHEpwzr8yC0QJB82f3GzTXb5tSWaG1PGOn0MMWLsqizjAGB3X7xxRdbSK0DUyAQ/Uz1+cZF8rghHxbK4pXy5/DELBXwMexxQXJDo0sb/R0BSX5e0PD5XWEkzyPLiPJWSxWQQCBIS5cupSFDXqYHHniAvUjwOGFwK8V+XqJIM8mNG4KyvIfJO7mBWsSga2vlVdF/g+DvVFY7WrEmX9QhdZKHaUlbQCmBsu1iPx6OcILo58ZTjv7msos9r+W8OPLkAmKJPX8ygKSP5BK8EuggeSefKnzkYQQXWh1Ed+vmC9KMDsBbUMy9zCSkUbaTuRI3jLKCP80kz5I7zW3g4rh2KR988AENHfqGWhUUTdR52vkrV65kfzkEoW3pedFFr09PQ4QSbkP4veHlUMfUgNI8DwGltWvFXHcp9nLtv6XYrz2nEOvN4nAckiGSHyLv5NPUXwxa7HD572rKs6J9yshnkYtKJ/yW56ek2jbuGMYD9H31nBgMw7WJ9aarBkUu37a9BKaBwrWF5W1wnyEkj4lFclEzJvdv3bqVv2MCFwQreyCY7gofNGbw3XPPPRxKv+GGG+nAgQO0bu1aNVsQASXkw2+ce+jgIfrpJ1H+2LFj2UU3e/ZstlfheuSJTSGxGEO2AZ0IOwVgHjnKAOGlGxHzRuCuk9eF4I50TyLfbuVutF57WUvYs3FAJAUIyQjJQ/5S8k77vbmoOELQx/EYD1TFhkJysUNYHXFCEhzwr+jPG/GXGDtqYbxQUlSZ/zgrWh0QLFHD6hjM8cBkIpAIq2zgP35o4EBeLoaZc4gKvvHGG/Tkk08yaTDpCSTteUdP9i9DYHsiKINVOFhLiXnaXbt25e0r5DwXzFvB3BbMv8YnBBocfvqJEydxXVhBI6OBDw4YQMOHj+A52pgmi7yjRo7iqCoCKwj7wxf/6KOPMTADElFaREcROUXHRQdGR5YdTpLG7/NRwO9nLwdsaPGZQfit3+3PIxFI7ZYZkodC5JvdWPxzhKOfXCe7neCGNoddvjjevQ+dYDwrz0HyLfy7+DsVnvJbiUqwqy2iqhNOctxQSAcEk6huuF4sTMDc7Hbt2tFTTz7JgRKQQi4/gybt0L4DL0YAbryxFU+ZRYBECpa1geToBP/4xz84DcTE2+Ghh0R0EsTHOegQUjBnBEScM3sORydxXJK8R48eNGPGDOpxRw+eQDVs2DB6+j9Pc2fANWBW4e23304tmreg0aOLuJxRo4rovvvu58UXH330Ed155508s1EuTsZbANd52WWX0ZVXXMkLKRiXXy4gf1+hfzfzYIGHym+kI43TjbQrjXT5KfPI46h75oyZ3B77c4kF2Ukh6Se5QS7vwlYcuQz3kdtIrpkqEuxKxMzFMVUoeNCM5sUN0QQK7FpCnqn/xx1GXzSNpXSlQ13k/2aoUXbk8mVh99x7LxMWE62wiHnG9OlMHGj3rl27scYd9OSTTD4EOBDmhsavWbMWrV8vrgECjQqSwTTBpC7Y+dDkb731FrVv357mzp3Lb4DBg1+0LJ1DmXhTwPPQunUbevG/L1KHDu1p5sxZ1KxZM/pg/gccMUUkEz5odEKU6/X66D//+Q9HKVE+tDa0N4j91lvD+FykjRg5khc+yEUd0KSYRwOTBmMQAN8lOE07ZgLHzd/6+Ru+NcuwHJf55TEj3zfffMOmnf2ZJAJIBkguCsY+4uof1xTB7UQP39xTJzpW/HjmXEak/pwqSjsVHfDfnbvJt/wB4cKUHU0HNvaM8JaIZNvBnoWGfOzxx9VUUhASAn/uU089RUNeeYXNEszywzHY19Cs0lRB2fv27qX58+bxTxAK03UxaxEya/ZsLgfnYOBYvLxY1b9582buQP/9739p0SKxIHv4iOH0/Asv8FwZdKRnnnmWZ/PxucXFPAkKJkbxihU8qQwDX5gqixcv5vIwL2TevPm0+fvN9Oyzz9G0adOM3bjM55gLYn8WiQCSfpIbo/3AjzOEyWEnua7Z5Z6Htg34meTyO7Zsnne1qdH1qzeEOwCvRPpGrCmdeIoxj90h4opds96/ire1EB3H4RocYBdlDjkccxJ7Ocmel6g4TRGIJfZrL7cwJrMlRPJ48zHh3D/zErWwDX+0NZsmTJPFckwukcMazjFVyLvoVgpsHEPBvcUUOvQtBfauIP+PM8m7+nHyzmtKnjHHmeQ26pL1cbkwUea3oJDvSEIETxlh/vr4EenNUhEhZ0GmEwmTPBFAfJ934XWZOoHtf5uik9wyBcAgqVjMXCj+PNZYjS8iqYW8iv+YsU2FMEs0cmsdhv9r6E0XBT5uz56fmASPcLOdNJ2e5nQ8WrodTvn0NJ3wlnTtu9NvO2Id1/PEyqfyO6TlAiAZIzkT6dB68hQJgpqb7JtQ02uV5ta0tyS/PY0XRLuoFKSHxtb+ZFZOIbDY+ViZNNxFgVUPJ/TQdOgPUO5MJX/DVw3gu8/v5+N8jlYXbHTpy7bXL39D4DfHrDt9nSJsZMzSE+0wz4dP3uvx8PcjR4+Qx+MWmpDEDryyTXp90sbCGALt4XQtHqC32+8PGFNrxRYZejkqfxn852uigGSW5Dzl9n7yQ5sjXC9JqAWGFMl1skuCq8GptOcNbc8r+U1zRP7ZlXUPcvF3iN7xdSjww1TjoUS5zgjaWwICr8SZZ55pWROJgePo0WP4OwabcMXZBQNGuB7hxQCRIbJMXeBteeyxxy3HFi9ZzPO/7dK3Xz9ekACBS/Gqq8xNP7FRp1xo4SR9+vSxzF3XBeTHJkDoXPDX650zktjvVS4BopE8A41FuQgMzbxQ7AOOiVi6y5AJqf22kdxcYCGP23arVX90Zf5mLY6ADyZ5LWxLoaNym+Xkrk9qP3xHMAauQQRipGA7NLloeNrUqey2Q/j97rvvobZt27LXA96T119/nddywr0H9xwEfulbbrmF3YdYY1n75Nr00ktiOzUpH374IXXq1Jk2btrEHQjn482AiUxyrSbSKhdW5t/fbfyOZ/FhhQ28K/DlY5MfdC7shwI3KHzyWMYGtyc6JqKo8KkjqoqOXKtmLQ4WwTe/YMFC6tXrAY62Fo0axeTv368/zwXv27eviqba71suQD67zGlyCXhaDm8m39Q/Cpego9Y2tK+cqSjNDUlw+S/K0myxaG+tDERYob2nnE2Bze+qt0lYm2JobR3SRFi1ehUvMwMROncyd3pFwEZuAQH33b333suRRPi44dvGipq77ryThr4xlFq1upE+WriQywUBEezArlUoF4EdhOvtc04WLlxAXbt0YT8yIqvYrg5uRvi8sfUbBNtXINCDvRhfe+11uuaaaziaClchOgPmacP9iaVmeKPg7YO6e/a8k1atXEUNGzbktmIZ3qBBg7g9m7dsprbt2vFvrMrH1FjUjTdE1y5d2d+Pbd5kDMB+3xJBJu15SOZJblQULNlB7vea8pZx2N9Qt8eF/WydUy5JrzqFrr3lX4RLT40k94R65P/qeW0BdHquC/LEoCd4Q0poQez1t3+/MDugGUE6CIiC6COIju0pNm7ayBoS81WmTJ7CEUQ5V0RqcQiIgxX5CBLJzTcRdII5srx4Ob9BXn/9DV5Sh++PPPwwB4bkZpjQ8Fg/eu1119Ett/yL3xqImqKjvfbaa7wWEz5xrISHIBrboH4D7qDobCB/0ejR9MSgQRwBxTVCoL1BcgCCzt26TWt+80BAetkp7fcsVwCxkDxjPQqDG64jSIF1L5Bv/G95/ScPCpnQBsHZnBH2tNWVaECZJeLf4JjY8KNPO5f/FzTk1reHc2hHEoBgMIkws9RaIJh80JjwdMkllzCxm1xyCc+PvvOuuziiuHbdWiY9SAiTAK/4e++5hweImEiFxdEwd1q2vJa3ebv5ppuUzY6AEOaugEjY+B6mAcwSaFnYyp06dqTvjNmQCOdjI85bb7uVtfr8+fO5XOTFnot//etfObQP0kJA7okTJnIHReeCiYX5NkhHlBE2vXwLgeByTg06KcrGJzoRxidyRqb9vuUKIM6aHK/zTPwdnkHAwJGt5FkxkHzTzia/1MIgPFx9AG/HLPzd2CaObWwsboarUO5W+24D8n/SmQJb36NQQGylkE5y68A8DrlVBATbKiCCiGMQ2KUgMSZtQaCBMacF80BgJ+/YvoOOHjnK5yFaGgyIIAVmK773/vu0a6ewa380FgrLchGplBsWoQ2YAoCOhrcB6pc7XmGFP8rHIPHY0WNcLt40e/fu42go2o7OIweqO3fu4GkFiOLibXD06DEeZ2BmJQTEXb1qNc/ZQWfElGC0Cd4WREqxAwEmosHk2b5d7C5gv2e5AogzyTOIoKHVuc6Am4I7FpJv5UDyL7iBfLPOI//kuuSbUJN846uSZ2w18o7/Lfkmn07uGY3J91E7Cqx9gQK7PqWQV0xnlR3HXk86oYv+O9bxiCIbbUkzEhzKjCaJ5IXEzB/hsDwP7ke8STDDEWMQPpbh+58KIGVAcjno0wgqb2QwwBt3Bo9up+DhzfwX48GS3aaNrec1eGEvPy1IYGAKpP2Nl26kEHG1wy7244kg1QhnPPcdknWSR4RG4jAyZ5rUGUQ8D6K8QXerZgOpdIbcInm2kMINixvZqCOPuBBO8ngfTrz58sg5pKIV04oE25HsGzGc5FlGWdUbF6I9hGjH8nBEsiR1RAL3v8xJngwi3axE05NFusvLI7MoO5KzH94hPY/yjwS0bDZQdiTPBMrrNeQYKSoaKhbJs4UyIKUKEDkcyxSyWVcmkV6Sp838cG5Lyje9DMiZR9lCBlvSR/JUUM4IaOlw5a3t2X7WZXx/cofkBlLW1nnkoUFGZhMmeU4TMQNaI2euN45ry5lAj44yblNSJM9VMBmTuaHJnGNDqh0h1fPtf9Cah4mESZ7qw2CkgVQZQRLtKguvB5Dt+sozEiZ5eYH6qz6HY/Eg2fPKHEl01FxAJu93XCTPZANSRcbalqA7NGPtKKdg8ylHOlxcJM8jNsqS5GVZdzyQ3CqrQbEgeTBP8ljIdSLlERkVXpP/WsjpdJ1OaU6IN195RYUneVpRRq/brCLBsUg8KOtOlBjJfw0POceRdsJk4Zmmvc0JIjrJs3AD8sgjk0AHi07yco5MaJBMlFn2qHjPXiKnSF4xyRMDuOf5t2VGERfJc4l8SbXl10KiRK8z0fxRkNRzSQMicdYOR5Kn1Og03rw88oiIBHjmSPJsIqUOVcYoz20HEm5/AsSKBwnXryOBtkQleUqNSAZoeAKNTxrZqCMJJHO/sxEqd+JGeUJUkqcV6XwYyZSVzDkVAdm47kh16H+aFSlPFpA9khtIRlvlkUcqECQPhpz/Biwveakg8v8YdrbwszgjyQAAAABJRU5ErkJggg==';
// Signature image for the final offer letter (Abhishek Jain), printed between
// the "e-Marketing" line and "Abhishek Jain". Loaded once at startup from the
// pre-trimmed public/signature.png (bundled on Vercel via includeFiles); falls
// back to '' (blank space) if the file is missing.
const _HRM_SIGN_SRC = (() => {
  try {
    return 'data:image/png;base64,' + require('fs').readFileSync(require('path').join(__dirname, 'public', 'signature.png')).toString('base64');
  } catch { return ''; }
})();
function _getHrmLogoSrc() { return _HRM_LOGO_SRC; }

async function _hrmDriveClient() {
  const { google } = require('googleapis');
  let creds;
  if (process.env.GOOGLE_CREDENTIALS) {
    creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
    // Vercel sometimes double-escapes \n in private key — fix it
    if (creds.private_key) creds.private_key = creds.private_key.replace(/\\n/g, '\n');
  } else {
    creds = require('./credentials.json');
  }
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/drive'],
  });
  const client = await auth.getClient();
  return google.drive({ version: 'v3', auth: client });
}

function hrmBuildOfferHtml(candidateName, candidatePosition, joiningFmt, today) {
  const logoSrc  = _getHrmLogoSrc();
  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><style>
    body{margin:0;padding:0;font-family:'Times New Roman',Times,serif;font-size:16px;color:#000;line-height:1.1}
    table.hdr{width:100%;border:none;border-collapse:collapse;margin-bottom:8px}
    table.hdr td{border:none;vertical-align:top;padding:0}
    h2{text-align:center;font-size:16px;font-weight:bold;letter-spacing:.3px;margin:8px 0 6px}
    .pc{text-align:right;margin-bottom:8px;font-size:16px}
    p{margin:0 0 7px;text-align:justify}ol{margin:2px 0 8px 18px}ol li{margin-bottom:2px}
    .footer{margin-top:14px}a{color:#00f}
  </style></head><body><div>
  <table class="hdr"><tr>
    <td width="197" valign="top" style="padding-right:12px"><img src="${logoSrc}" alt="e-Marketing" width="185" height="110" style="display:block"></td>
    <td valign="top" style="font-size:13px;line-height:1.4;text-align:right">
      <p style="margin:0;text-align:right"><strong>e-Marketing.io (A Unit of Jai Marketing)</strong><br>
      Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,<br>
      Malviya Nagar, Jaipur, Rajasthan – 307017 (India)<br>
      <br>
      Phone: +91-9602694444<br>
      Email: <a href="mailto:abhishek@e-marketing.io">abhishek@e-marketing.io</a><br>
      Website: www.e-marketing.io</p>
    </td>
  </tr></table>
  <h2>PRELIMINARY OFFER LETTER</h2>
  <div class="pc" style="text-align:right">Private &amp; Confidential<br>Date :-${today}</div>
  <p><strong>Dear ${candidateName},</strong></p>
  <p>With reference to your application and the subsequent interview you had with us, we are pleased to offer you an appointment as <strong>${candidatePosition}</strong> with <strong>e-Marketing (a unit of Jai Marketing)</strong>, Jaipur.</p>
  <p>You are required to join us on <strong>${joiningFmt}</strong>. Your place of work will be <strong>Jaipur</strong> (8/10 shaheed amit bhardwaj marg, malviya nagar Jaipur 302017)</p>
  <p>The detailed terms and conditions of your appointment and the salary details, as discussed, shall be issued to you at the time of joining. We expect you to maintain the confidentiality of the salary offer to you.</p>
  <p>Please submit the following documents on your Joining Day:</p>
  <ol>
    <li>Educational/Professional/Technical Qualification certificates</li>
    <li>Copy of Resignation Acceptance letter or relieving letter from last employer, if applicable.</li>
    <li>Salary Certificate from last employer, if applicable.</li>
    <li>One (1) passport size color photograph</li>
    <li>Copy of Present and Permanent Address Proof.</li>
    <li>ID Proof (Aadhar Card, PAN Card).</li>
  </ol>
  <p>If you fail to join on the aforesaid date and in the absence of any written communication to this effect from you, the said Preliminary Offer Letter shall automatically be treated as withdrawn.</p>
  <p>Please send a <strong>token of your acceptance</strong> of this Preliminary Offer Letter.</p>
  <p>Again, we are excited about the growth trajectory that e-Marketing Consulting is on, and we look forward to having you on board as a team member.</p>
  <div class="footer"><p>For</p><p>e-Marketing (a unit of Jai Marketing)</p></div>
  </div></body></html>`;
}

async function hrmGenerateOfferDoc(candidate, joining_date, salary, overrideName, overridePosition) {
  const crypto = require('crypto');
  const token = crypto.randomBytes(24).toString('hex');

  const candidateName     = overrideName     || candidate.name             || '';
  const candidatePosition = overridePosition || candidate.profile_position || '';

  const joiningFmt = joining_date
    ? new Date(joining_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' })
    : '';
  const today = new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });

  const html = hrmBuildOfferHtml(candidateName, candidatePosition, joiningFmt, today);
  await db.query('UPDATE hrm_candidates SET offer_token=?, offer_html=? WHERE id=?', [token, html, candidate.id]).catch(() => {});

  const fetchFn = global.fetch || (await import('node-fetch')).default;
  const scriptRes = await fetchFn(HRM_OFFER_SCRIPT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      // Template approach (new Apps Script uses this)
      templateId: HRM_OFFER_TEMPLATE_ID,
      replacements: {
        '{{CANDIDATE_NAME}}': candidateName,
        '{{POSITION}}':       candidatePosition,
        '{{JOINING_DATE}}':   joiningFmt,
        '{{Today_Date}}':     today,
      },
      // HTML fallback (old Apps Script uses this)
      html,
      filename: `PRELIMINARY OFFER LETTER - ${candidateName}`,
      folderId: HRM_OFFER_FOLDER_ID,
    }),
  });
  const scriptData = await scriptRes.json();
  if (!scriptData.ok) throw new Error(scriptData.error || 'Apps Script upload failed');

  const fileId = scriptData.fileId;
  const pdfUrl = scriptData.pdfUrl;

  await db.query('UPDATE hrm_candidates SET offer_drive_id=? WHERE id=?', [fileId, candidate.id])
    .catch(() => {});

  return { fileId, pdfUrl };
}

// Verbatim transcription of the user-supplied final offer letter/employment
// contract format (screenshots, 2026-07-18) — every word, section number and
// clause is intentionally kept identical to the source, including its own
// inconsistencies (the stray "14.7" clause after "15.6", "theaccounts" typo
// in 13.1, the fixed "9th day of July, 2026" / "10th day of July 2026"
// acceptance-block dates which were static in the source, not merge fields).
// The clause TEXT must stay verbatim — do not "fix" wording without the user
// re-confirming against their real document. User-approved corrections
// (2026-07-19 consistency audit): six cross-reference/numbering fixes (13.3
// now cites Clause 13; 14.1/14.5/14.6 cite Clause 14; the stray "14.7" after
// 15.6 is now 15.7; the article before the position is computed a/an) and a
// blank hand-filled acceptance date. Clause 15.6's jurisdiction was changed
// from "Bangalore" to "Rajasthan" on 2026-07-22 at the user's explicit
// request (it had been flagged as inconsistent with the Jaipur letterhead in
// the source document, and the user then approved the correction). The page STRUCTURE, however, is
// now built for browser/Chromium rendering (user-approved): the logo/address
// header is a running header applied on every page by the PDF renderer (see
// hrmFinalOfferHeaderTemplate), so it is no longer repeated inline in the body;
// section boundaries use ".pb" page-break divs (Chromium honours these, unlike
// the old Google-Doc pipeline). Pass opts.inlineHeader=true to also show the
// header once at the top for the on-screen live preview.
function hrmBuildFinalOfferHtml(candidateName, candidatePosition, joiningFmt, salary, todayFmt, opts = {}) {
  const logoSrc = _getHrmLogoSrc();
  // Signature block sits between "e-Marketing" and "Abhishek Jain". Renders the
  // real signature image once _HRM_SIGN_SRC is set; otherwise leaves blank space.
  const signBlock = _HRM_SIGN_SRC
    ? `<img src="${_HRM_SIGN_SRC}" alt="signature" style="width:170px;height:auto;display:block;margin:4px 0">`
    : `<br><br>`;
  // Acceptance-block dates (dynamic, per the source Doc's placeholders
  // "{one day before Date of Joining}, {present year}"): rendered literally as
  // "29 July, 2026" from opts.joiningDate (raw). Year comes from the joining
  // date. Falls back to the joiningFmt string if no raw date given.
  // Probation period (clause 4) — HR-editable, defaults to the source Doc's 2.
  const _probN = parseInt(opts.probationMonths, 10);
  const probationTxt = (Number.isFinite(_probN) && _probN >= 0)
    ? `${_probN} month${_probN === 1 ? '' : 's'}`
    : '2 months';
  // HR enters the MONTHLY salary (cover page: "Rs.<salary>/- per month");
  // clause 3 states the ANNUAL CTC, so multiply by 12 when the value is
  // numeric. Non-numeric input (e.g. "6 LPA") is used as-is in both places.
  const _salNum = parseFloat(String(salary || '').replace(/,/g, ''));
  const annualCtc = (Number.isFinite(_salNum) && _salNum > 0) ? String(_salNum * 12) : (salary || '');
  // "a"/"an" before the position, by pronunciation: vowel-letter words get
  // "an"; all-caps acronyms go by the first letter's NAME (M = "em" -> "an
  // MIS Analyst", C = "see" -> "a CA").
  const _posFirst = String(candidatePosition || '').trim().split(/\s+/)[0] || '';
  const _acronym = /^[A-Z]{2,}$/.test(_posFirst);
  const article = (_acronym ? /^[AEFHILMNORSX]/.test(_posFirst) : /^[aeiouAEIOU]/.test(_posFirst)) ? 'an' : 'a';
  // Acceptance-block dates (per the source Doc's placeholders): the acceptance
  // line pre-fills joining−1 ("one day before Date of Joining" — user chose to
  // keep this over a blank hand-filled date), the join line the joining date.
  // The candidate still hand-fills the "Date:" line under their signature.
  const _fmtDate = (d) => `${d.getDate()} ${d.toLocaleDateString('en-IN', { month: 'long' })}, ${d.getFullYear()}`;
  let acceptDateStr = joiningFmt || '', joinDateStr = joiningFmt || '';
  if (opts.joiningDate) {
    const jd = new Date(opts.joiningDate);
    if (!isNaN(jd.getTime())) {
      joinDateStr = _fmtDate(jd);
      const prev = new Date(jd); prev.setDate(prev.getDate() - 1);
      acceptDateStr = _fmtDate(prev);
    }
  }
  // Header used only for the on-screen preview (opts.inlineHeader). The printed
  // PDF gets the same logo/address as a running header on every page instead.
  const header = `<table class="hdr"><tr>
    <td width="197" valign="top" style="padding-right:12px"><img src="${logoSrc}" alt="e-Marketing" width="185" height="110" style="display:block"></td>
    <td valign="top" style="font-size:13px;line-height:1.4;text-align:right">
      <p style="margin:0;text-align:right"><strong>e-Marketing.io (A Unit of Jai Marketing)</strong><br>
      Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,<br>
      Malviya Nagar, Jaipur, Rajasthan – 307017 (India)<br>
      <br>
      Phone: +91-9602694444<br>
      Email: <a href="mailto:abhishek@e-marketing.io">abhishek@e-marketing.io</a><br>
      Website: www.e-marketing.io</p>
    </td>
  </tr></table>`;

  // Compact running header (logo + address) for browser print: repeats on every
  // page because it is position:fixed inside the @page top margin.
  const runHeader = `<table style="width:100%;border-collapse:collapse"><tr>
    <td style="vertical-align:top;padding:0"><img src="${logoSrc}" alt="e-Marketing" style="height:48px;width:auto;display:block"></td>
    <td style="vertical-align:top;padding:0;text-align:right;font-size:10px;line-height:1.4">
      <strong>e-Marketing.io (A Unit of Jai Marketing)</strong><br>
      Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,<br>
      Malviya Nagar, Jaipur, Rajasthan – 307017 (India)<br>
      Phone: +91-9602694444 &nbsp;|&nbsp; <a href="mailto:abhishek@e-marketing.io">abhishek@e-marketing.io</a> &nbsp;|&nbsp; www.e-marketing.io
    </td>
  </tr></table>`;
  // Print styling: the browser (HR's or the candidate's Chrome/Edge) renders the
  // PDF via its own print engine — no server Chromium. A .dlbar "Save as PDF"
  // button (screen only) triggers window.print().
  const printCss = opts.forPrint ? `
    @page { size: A4; margin: 34mm 16mm 18mm; }
    @media print {
      .dlbar { display: none !important; }
      .sheet { max-width: none !important; margin: 0 !important; padding: 0 !important; box-shadow: none !important; }
      .run-header { position: fixed; top: 8mm; left: 16mm; right: 16mm; margin: 0 !important; }
    }
    @media screen {
      body { background: #eef1f5; }
      .sheet { max-width: 820px; margin: 16px auto; padding: 28px 34px; background: #fff; box-shadow: 0 1px 8px rgba(0,0,0,.15); }
      .run-header { margin-bottom: 14px; }
    }
    .dlbar { position: sticky; top: 0; z-index: 9; background: #4f46e5; color: #fff; padding: 11px 18px; display: flex; justify-content: space-between; align-items: center; gap: 12px; font-family: system-ui, -apple-system, sans-serif; font-size: 14px; }
    .dlbar button { background: #fff; color: #4f46e5; border: none; border-radius: 7px; padding: 8px 18px; font-weight: 700; font-size: 14px; cursor: pointer; white-space: nowrap; }
    .run-header table { width: 100%; border-collapse: collapse; }
  ` : '';
  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Offer Letter${candidateName ? ' - ' + candidateName : ''}</title><style>
    body{margin:0;padding:0;font-family:'Times New Roman',Times,serif;font-size:14px;color:#000;line-height:1.35}
    table.hdr{width:100%;border:none;border-collapse:collapse;margin-bottom:10px}
    table.hdr td{border:none;vertical-align:top;padding:0}
    p{margin:0 0 10px;text-align:justify}
    ul{margin:2px 0 8px 18px}ul li{margin-bottom:4px}
    .center{text-align:center}
    .pb{page-break-before:always}
    .rule{border:none;border-top:1px solid #999;margin:12px 0}
    a{color:#00f}${printCss}
  </style></head><body>
${opts.forPrint ? `  <div class="dlbar"><span>📄 Offer Letter${candidateName ? ' — ' + candidateName : ''}</span><button onclick="window.print()">⬇ Save as PDF</button></div>
  <div class="sheet">
  <div class="run-header">${runHeader}</div>` : ''}
  <div class="page">
    ${opts.inlineHeader ? header : ''}
    <p>${todayFmt}</p>
    <p>Dear <strong>${candidateName}</strong> ,</p>
    <p>We are pleased to offer you an appointment as ${article} <strong>${candidatePosition}</strong> with e-Marketing (a unit of Jai Marketing)</p>
    <p>We expect your appointment to be effective on or before <strong>${joiningFmt}</strong>.</p>
    <p>Your gross remuneration package will be <strong>Rs.${salary || ''}/- per month</strong>.</p>
    <p>Please sign the duplicate copy of this letter to acknowledge your acceptance of the above and return it to us at the address below.</p>
    <p><strong>Sincerely Yours,</strong></p>
    <p><strong>e-Marketing</strong></p>
    ${signBlock}
    <p>Abhishek Jain</p>
    <p>Partner: eMarketing</p>
    <hr class="rule">
    <br><br><br>
    <p class="center">Agreed and accepted this ${acceptDateStr}.</p>
    <p class="center">I will join eMarketing on the ${joinDateStr}.</p>
    <br><br>
    <p class="center">____________________________</p>
    <p class="center"><strong>${candidateName}</strong></p>
  </div>

  <div class="page">
    <div class="pb"></div>
    <p><strong><u>CHECKLIST OF DOCUMENTS REQUIRED AT THE TIME OF JOINING:</u></strong></p>
    <ul>
      <li>Copy of the offer letter accepted and signed by you.</li>
      <li>Resignation Acceptance/Relieving Certificate from last employer.</li>
      <li>Form 16 (pertaining to tax deducted at source) from the previous employer or salary certificate.</li>
      <li>Xerox of Educational Certificates (Copy of 10th, 12th, and graduation/post-graduation certificates).</li>
      <li>Four recent passport-size photographs.</li>
      <li>Xerox of Proof of Birth Date (Copy of Birth Certificate/School Leaving Certificate).</li>
      <li>Proof of identity (original and Xerox copy of passport/driving license/voter's ID card).</li>
      <li>Residential Proof (Ration Card Copy/Voter's ID Card/Passport).</li>
      <li>PAN Card original and three Xerox.</li>
      <li>Bank Account Details for Salary Transfer.</li>
    </ul>
  </div>

  <div class="page">
    <div class="pb"></div>
    <p class="center"><strong>OFFER OF EMPLOYMENT (Private &amp; Confidential)</strong></p>
    <p>We are pleased to offer you employment with eMarketing under the following terms and conditions set out in this Contract of Employment (&ldquo;Agreement&rdquo;), subject to satisfactory reference and background screening and upon approval of any applicable work pass application.</p>

    <p><strong>1. DESIGNATION</strong></p>
    <p>You are employed as ${article} <strong><u>${candidatePosition}</u></strong>.</p>

    <p><strong>2. COMMENCEMENT</strong></p>
    <p>You will commence employment on <strong>${joiningFmt}</strong>. Your employment with the company will commence on your actual and effective date of joining the company, subject to the completion of all joining formalities. Till such time, no relationship (employment, contractual, or otherwise) will exist between the parties. The company reserves the right to withdraw this offer at its sole discretion at any time before the date of joining, with due communication to you.</p>

    <p><strong>3. REMUNERATION</strong></p>
    <p>Your fixed annual CTC will be Rs <strong>${annualCtc}</strong>/- subject to the appropriate withholding tax in accordance with India's laws and regulations. The prerequisites and benefits applicable within the CTC will be discussed with you further.</p>

    <p><strong>4. PROBATION</strong></p>
    <p>You shall serve a probationary period of up to <strong>${probationTxt}</strong>. The company reserves the right to extend the probationary period, if necessary.</p>

    <p><strong>5. ANNUAL LEAVE</strong></p>
    <p>All employees shall be entitled to annual leave of <strong>twelve (12) working days</strong> per year.</p>

    <p><strong>6. NORMAL DAYS/HOURS OF WORK</strong></p>
    <p>All employees would observe a <strong>Six (6) day work week</strong>, Monday through Saturday, with working hours from 9:30 am to 6:00 p.m. and a half-hour lunch break between 1:30 pm and 2:00 pm.</p>

    <p><strong>7. TIMELY ARRIVAL INCENTIVE</strong></p>
    <p>In recognition of your commitment to punctuality, we offer an additional day off on the last Saturday of every month, contingent on timely arrival to the office each day from the last Saturday of the previous month, with no exceptions, and the day will be forfeited in case of tardiness.</p>

    <p><strong>8. PUBLIC HOLIDAYS</strong></p>
    <p>All employees shall be entitled to all gazette public holidays with full pay.</p>

    <p><strong>9. OUTSIDE INTEREST</strong></p>
    <p>You will not be permitted, while in the employment of the company, to carry on any business other than the business of the company and/or divulge to any person any information concerning the methods, arrangements, practices, or transactions that may injure or prejudice the interest or reputation of the company in any manner or form.</p>
  </div>

  <div class="page">
    <p><strong>10. CONFLICT OF INTEREST</strong></p>
    <p>All employees shall be required to report to the company if any member of his family, or close relatives, is engaged in any trade or business involving supplies of goods and/or services to the company or has any other type of business relationship with the company.</p>

    <p><strong>11. AMENDMENT</strong></p>
    <p>This agreement may be amended by the company from time to time as and when the company considers it proper in the best interests of the company. The amendment shall be in the form of a notification in writing addressed to you at your last known address, and then such amendment shall be incorporated into this Agreement and shall form part of this Agreement.</p>

    <p><strong>12. PERSONAL INFORMATION</strong></p>
    <p>12.1 For any applicable data protection legislation, you consent to the collecting, holding, processing, accessing, use, and disclosing of any personal data relating to you or provided by you to the Company for all purposes relating to compliance with any applicable laws and/or the Company's exercise of any of its rights or performance or discharge of any of its obligations under this Agreement or where such disclosure is for any purpose that is related to your employment with the Company, including but not limited to:</p>
    <p>A. Administering and maintaining personal records;<br>
    B. Paying and reviewing salary and other remuneration and benefits;<br>
    C. Providing and administering benefits (including, if relevant, pension, life assurance, permanent health insurance, and medical insurance) or compliance with a legal requirement;<br>
    D. Undertaking performance appraisals and development reviews;<br>
    E. Maintaining sickness, holiday, and other absence records;<br>
    F. Making decisions about your fitness for work or the need for adjustments in the workplace;<br>
    G. Providing references and information to future employers;<br>
    H. Providing information to governmental and quasi-governmental bodies where required or requested by such bodies, including without limitation the revenue and tax authorities, customs, and immigration authorities, and taking decisions regarding any such information;<br>
    I. Investigating and recording the commission or alleged commission of any offense in order to comply with legal requirements and obligations to third parties;<br>
    J. Providing information to future purchasers of the Company or any of its associated companies; and<br>
    K. Transferring information concerning you to a country or territory outside India (all HR information is maintained in the shared services in India).</p>

    <p>12.2 You also consent to the company monitoring and recording your actions and activities, such as those conducted on your laptop or desktop computer that is issued to you by the company, telecommunications, and security systems, and any use you make of your telecommunication or computer systems. You agree to comply with the company's policy concerning the use of such systems.</p>

    <p>12.3 You agree to comply with the company's data policies and will take all steps to ensure that any associated company companies' information or personal data that you have, hold, or process will be kept securely by you, particularly if such information is accessed by or accessible to you via a mobile device, such as a laptop, desktop, personal digital assistant (PDA) or mobile telephone.</p>

    <p>12.4 Concerning the Personal Information shared under this Agreement, you agree that for Section 43A of the Information Technology Act 2000, the aforesaid personal data policies of the Company or such other policy of the Company dealing with data protection and security shall constitute reasonable security practices and procedures and accordingly, the Information Technology (Reasonable Security Practices and Procedures and Sensitive Personal Data or Information) Rules 2011 are hereby excluded.</p>
  </div>

  <div class="page">
    <p><strong>13. CONFIDENTIALITY</strong></p>
    <p>13.1 You shall not during your employment or after the termination thereof (howsoever arising) make use of for your own purposes or those of any other person, firm or company or disclose to any person (except the proper officers of the Company or under the authority of the Board or required by law) any trade secrets or confidential information relating to the business, accounts, affairs or finances of the Company or its associated companies or their customers or suppliers, whether recorded or not (and if recorded, whether on paper, tape, hard drive or computer disk) and includes without limitation all and any information about business plans, new business opportunities, research and development projects, product formulae, processes, inventions, designs, discoveries or know-how, sales statistics (including targets and statistics, market share and pricing statistics, forecasts and reports) maturing business opportunities, processes, designs, marketing surveys and plans, costs, profit or loss or financial information relating to theaccounts, prices and discount structures of the Company its associated companies or their customers or suppliers, the names, addresses, telephone numbers, fax numbers, e-mail or contact details, activities or personal affairs of the Company's or its associated companies' customers, agents, consultants, distributors and suppliers, any Company or its associated companies' database, mailing list, software application, component list, any information relating the terms of business between the customers, suppliers or agents and the Company or its associated companies' (the &ldquo;Confidential Information&rdquo;).</p>

    <p>13.2 You acknowledge that you will have access during your employment to Confidential Information belonging to the Company or its associated companies or their customers or suppliers and that the Company (for itself or on behalf of its associated companies or their customers or suppliers) has a legitimate commercial interest in preventing the unauthorized disclosure of such Confidential Information.</p>

    <p>13.3 The obligations contained in this Clause 13 shall continue to apply without limitation in time following the termination of your employment, however arising, but they shall cease to apply to any information or knowledge that may subsequently come into the public domain other than by way of unauthorized disclosure.</p>

    <p>13.4 All confidential information, plans, statistics, records, and other documentation (including any copies thereof, whether in paper or electronic form) of whatsoever nature relating to the business of the company or its associated companies or their customers or suppliers, shall be immediately returned by you to the company or, at the option of the company, destroyed or deleted (in the case of information that is stored electronically) in the event of the termination of your employment, however arising (or at any earlier time on demand).</p>

    <p>13.5 You acknowledge that the remedy of damages may be inadequate to protect the interests of the Company and that the Company is entitled to seek and obtain an injunction or any other legal or equitable relief against you for any threatened or actual breach of any provisions of this Agreement by you or any other relevant person, and no proof of special damages shall be necessary for the enforcement by the Company of its rights under this Agreement.</p>
  </div>

  <div class="page">
    <p><strong>14. INTELLECTUAL PROPERTY</strong></p>
    <p>14.1 For this Clause 14, &ldquo;Intellectual Property&rdquo; means patents, utility models, registered designs, registered trade and service marks, copyright (whether registered or not), improvements and modifications to any of the foregoing, and the right to apply for protection for such registered rights anywhere in the world, inventions, discoveries, copyright design rights, unregistered trade and service marks, brand names, secret or confidential information, know-how, or any other intellectual property and any similar or equivalent rights, whether registrable or not arising or granted under the law of any country or state.</p>

    <p>14.2 Any Intellectual Property made created or discovered by you (either alone or with any other persons) during your employment (whether capable of being patented or registered or not and whether or not created or discovered in the course of your employment and whether or not it was created or discovered with the use of the Company's machinery or equipment of the Company or any of its associated companies) in conjunction with or in any way affecting or relating to the business or other Intellectual Property rights for the time being and from time to time of the Company or any of its associated companies or in the opinion of the management of the Company is capable of being used or adapted for such use shall forthwith be disclosed to the Company and shall (subject to all relevant legislation), on a worldwide and perpetual basis, belong to and be the absolute property of the Company or its associated companies, as the case may be.</p>

    <p>14.3 If and whenever required to do so by the company, you will, at the expense of the company, apply or join with the Company or any of its associated companies in applying for letters patent or other protection or registration in India and/or any other part of the world for any such Intellectual Property which belongs to the Company or its associated companies. You will, at the company's expense, execute and do or procure to be executed and done all instruments and things necessary for vesting the said letters patent or other protection or registration when obtained, and all rights, title, and interest to and in the intellectual property in the company absolutely or in such other persons or companies as the company may specify. Any assignment/transfer of such rights, titles, and interests shall not lapse if the company has not exercised its rights under the assignment for any period.</p>

    <p>14.4 You waive all your moral rights under applicable law and any foreign corresponding rights in respect of any work of which you are the author or co-author.</p>
  </div>

  <div class="page">
    <p>14.5 Rights and obligations under Clause 14 shall continue in force after the termination of your employment concerning intellectual property created or discovered during the period of your employment and shall be binding upon your representatives.</p>

    <p>14.6 You agree that, as and when requested by the Company, you shall appoint the Company as your attorney in your name to execute and do all documents and things, that are required to give effect to the provisions of this Clause 14.</p>

    <p><strong>15. MISCELLANEOUS</strong></p>
    <p>15.1 This Agreement together with any documents referred to in it constitutes the entire agreement and understanding between you and the Company and supersedes any previous agreement relating to your employment with the Company.</p>

    <p>15.2 In the event of any conflict between the terms of this Agreement and any other document purporting to relate to your employment, the terms of this Agreement shall prevail.</p>

    <p>15.3 This Agreement is personal and may not be assigned to any third party by any party.</p>

    <p>15.4 If either party agrees to waive its rights under a provision of this Agreement, that waiver will only be effective if it is in writing and it is signed by that party. A party's agreement to waive any breach of any term or condition of this Agreement will not be regarded as a waiver of any subsequent breach of the same term or condition or a different term or condition.</p>

    <p>15.5 Any notice or other document to be given under this Agreement shall be in writing and may be given personally to you or may be sent by first-class post or other fast postal service to, in the case of the Company, its registered office for the time being and your case, at your last known place of residence. Any such notice shall be deemed served upon the earlier of (i) delivery, if served personally; or (ii) upon receipt, if sent by mail.</p>

    <p>15.6 This Agreement shall be governed by Indian law, and the Company and you submit to the exclusive jurisdiction of the Indian courts in Rajasthan.</p>

    <p>15.7 Notwithstanding the above terms and conditions, the Company reserves the right to amend, delete, and/or implement new terms and conditions which the Company deems necessary from time to time, and such amendment/deletion/implementation of new terms and conditions shall be notified to you in writing by prior notice.</p>

    <p><strong>16. TERMINATION</strong></p>
    <p>Employment may be terminated at any time by either party giving notice or pay in lieu of notice, or part thereof, for any reason other than redundancy. Periods of notice shall be two (2) weeks during the probationary period and one (1) month after confirmation and shall be in writing, except in the case of serious misconduct in which case you may be terminated at any time without notice. Absenteeism beyond 10 days is liable for termination unless and otherwise such absence is supported by valid reason in writing and with valid documents.</p>

    <p><strong>17. AGE OF SUPERANNUATION</strong></p>
    <p>Completion of sixty years as per date of birth and as declared by you at the time of appointment.</p>
  </div>

  <div>
    <p>If the above terms and conditions are acceptable to you, please signify by signing the duplicate of this letter and returning the same to us within three (3) working days.</p>
  </div>
${opts.forPrint ? '  </div>' : ''}
  </body></html>`;
}

// Decoded image buffers for the pdfkit renderer (offer-letter-pdf.js).
function _hrmLogoBuffer() { try { return Buffer.from(_HRM_LOGO_SRC.split(',')[1], 'base64'); } catch { return null; } }
function _hrmSignBuffer() { try { return _HRM_SIGN_SRC ? Buffer.from(_HRM_SIGN_SRC.split(',')[1], 'base64') : null; } catch { return null; } }

// Build the final-offer HTML and render it to a PDF Buffer via pdfkit
// (offer-letter-pdf.js): letterhead on every page, signature below the
// sign-off, real page breaks. No browser involved.
async function hrmRenderFinalOfferPdfBuffer({ name, position, joiningFmt, salary, today, joiningDate, probationMonths }) {
  const { renderOfferPdfFromHtml } = require('./offer-letter-pdf');
  const html = hrmBuildFinalOfferHtml(name || '', position || '', joiningFmt || '', salary || '', today || '', { inlineHeader: false, joiningDate, probationMonths });
  return renderOfferPdfFromHtml(html, { logoBuffer: _hrmLogoBuffer(), signBuffer: _hrmSignBuffer() });
}

// Final "Offer Letter Sent" stage — sends the exact contract transcribed in
// hrmBuildFinalOfferHtml above through the same HRM_OFFER_SCRIPT Apps Script
// the preliminary letter uses (html-only, no templateId — the script proved
// to ignore templateId and only ever render html, and a direct Drive/Docs-API
// read of the user's own template hit a separate, unrelated wall: service
// accounts have no personal Drive storage quota, so file creation failed
// outright even once sharing/mimetype were fixed). This keeps final-offer
// generation on the one path that's actually proven to work end-to-end.
async function hrmGenerateFinalOfferDoc(candidate, joining_date, salary, overrideName, overridePosition) {
  const candidateName     = overrideName     || candidate.name             || '';
  const candidatePosition = overridePosition || candidate.profile_position || '';

  const joiningFmt = joining_date
    ? new Date(joining_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' })
    : '';
  const today = new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
  // inlineHeader:true -> letterhead shown once at the top, like the preliminary
  // letter, so the Apps Script HTML->Google-Doc->PDF conversion renders cleanly.
  const html = hrmBuildFinalOfferHtml(candidateName, candidatePosition, joiningFmt, salary, today, { inlineHeader: true, joiningDate: joining_date });

  const fetchFn = global.fetch || (await import('node-fetch')).default;
  const scriptRes = await fetchFn(HRM_OFFER_SCRIPT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      html,
      filename: `OFFER LETTER - ${candidateName}`,
      folderId: HRM_OFFER_FOLDER_ID,
    }),
  });
  const scriptData = await scriptRes.json();
  if (!scriptData.ok) throw new Error(scriptData.error || 'Apps Script upload failed');

  const fileId = scriptData.fileId;
  const pdfUrl = scriptData.pdfUrl;

  await db.query('UPDATE hrm_candidates SET final_offer_drive_id=? WHERE id=?', [fileId, candidate.id])
    .catch(() => {});

  return { fileId, pdfUrl };
}

async function hrmSendWhatsApp(endpoint, payload, type, candidateId, candidateName, action) {
  let status = 'Failed', errorDetail = '';
  try {
    const fetchFn = global.fetch || (await import('node-fetch')).default;
    const resp = await fetchFn(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': HRM_AMUFIY_API_KEY },
      body: JSON.stringify(payload)
    });
    if (resp.ok) { status = 'Sent'; } else {
      const txt = await resp.text();
      errorDetail = `HTTP ${resp.status}: ${txt.slice(0,200)}`;
    }
  } catch (e) { errorDetail = e.message; }

  const payloadJson = JSON.stringify({ endpoint, body: payload });
  await db.query(
    `INSERT INTO hrm_message_log (candidate_id,candidate_name,phone,action,type,status,error_detail,payload_json)
     VALUES (?,?,?,?,?,?,?,?)`,
    [candidateId||null, candidateName||'', payload.to||'', action||type, type, status, errorDetail, payloadJson]
  ).catch(e => console.error('hrm_message_log insert failed:', e.message));
  return status === 'Sent';
}

function hrmFormatPhone(phone) {
  const clean = String(phone||'').replace(/[\s\-\+\(\)]/g,'');
  if (clean.length >= 12 && clean.startsWith('91')) return clean;
  if (clean.startsWith('0')) return '91' + clean.slice(1);
  return '91' + clean;
}

// Public offer letter view — no login needed, token is the secret
app.get('/offer/:token', async (req, res) => {
  try {
    const [[c]] = await db.query(
      'SELECT offer_html FROM hrm_candidates WHERE offer_token=?',
      [req.params.token]
    );
    if (!c || !c.offer_html) return res.status(404).send('<h3 style="font-family:sans-serif;padding:40px">Offer letter not found or link has expired.</h3>');
    const printCss = `<style>@media print{@page{margin:0;size:A4 portrait}body{margin:18mm 15mm!important}}</style>`;
    res.send(c.offer_html.replace('</head>', printCss + '</head>'));
  } catch (err) {
    res.status(500).send('<h3>Error: ' + err.message + '</h3>');
  }
});

// Public PDF of the final offer letter — the URL the WhatsApp provider fetches
// to attach the document. Renders the stored HR-approved snapshot on demand via
// pdfkit (fast, no browser). No auth: the random token is the capability, same
// pattern as /offer/:token.
app.get('/offer-pdf/:token', async (req, res) => {
  try {
    const [[c]] = await db.query(
      'SELECT final_offer_data FROM hrm_candidates WHERE final_offer_token=? LIMIT 1',
      [req.params.token]
    );
    if (!c || !c.final_offer_data) return res.status(404).send('Offer letter not found or link has expired.');
    const d = JSON.parse(c.final_offer_data);
    const pdf = await hrmRenderFinalOfferPdfBuffer({
      name: d.name, position: d.position, joiningFmt: d.joiningFmt,
      salary: d.salary, today: d.today, joiningDate: d.joining_date,
      probationMonths: d.probation_months,
    });
    const safeName = String(d.name || 'candidate').replace(/[^a-zA-Z0-9 _-]/g, '').trim() || 'candidate';
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="OFFER LETTER - ${safeName}.pdf"`);
    res.send(pdf);
  } catch (err) {
    console.error('offer-pdf error:', err.message);
    res.status(500).send('Failed to load offer letter.');
  }
});

// Dashboard stats
app.get('/api/hrm/stats', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[totals]] = await db.query(`
      SELECT
        COUNT(*) AS total,
        SUM(status='Scheduled')  AS scheduled,
        SUM(status='Rescheduled') AS rescheduled,
        SUM(status='Selected')   AS selected,
        SUM(status='Rejected')   AS rejected,
        SUM(status='Offer Sent') AS offer_sent,
        SUM((DATE(interview_date)=CURDATE() AND status='Scheduled') OR (DATE(reschedule_date)=CURDATE() AND status='Rescheduled')) AS today_interviews
      FROM hrm_candidates`);
    res.json(totals);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Get all candidates
app.get('/api/hrm/candidates', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [rows] = await db.query('SELECT * FROM hrm_candidates ORDER BY created_at DESC');
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add candidate + schedule interview
app.post('/api/hrm/candidates', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const { name, phone, profile_position, department, interview_date, interview_time, notes, meeting_link, interviewer_phone } = req.body;
    if (!name || !phone) return res.status(400).json({ error: 'name and phone required' });
    const [r] = await db.query(
      `INSERT INTO hrm_candidates (name,phone,profile_position,department,interview_date,interview_time,notes,meeting_link,interviewer_phone,created_by)
       VALUES (?,?,?,?,?,?,?,?,?,?)`,
      [name, phone, profile_position||'', department||'', interview_date||null, interview_time||'', notes||'', meeting_link||'', interviewer_phone||'', req.session.userId]);
    const cid = r.insertId;

    const meetLine = meeting_link ? `\n🔗 Meeting Link: ${meeting_link}` : '';
    hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(phone), text:
`Hello ${name}! 👋\n\nYour interview has been scheduled.\n\n🏢 Company: ${HRM_COMPANY}\n💼 Position: ${profile_position||''}\n📅 Date: ${interview_date||''}\n⏰ Time: ${interview_time||''}${meetLine}\n\nPlease be available on time.\n\n— ${HRM_COMPANY} HR Team`
    }, 'text', cid, name, 'Interview Scheduled - Candidate').catch(e => console.error('HRM WA candidate err:', e.message));

    if (interviewer_phone) {
      hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(interviewer_phone), text:
`📋 *New Interview Scheduled*\n\n👤 Candidate: ${name}\n📱 Phone: ${phone}\n💼 Position: ${profile_position||''}\n📅 Date: ${interview_date||''}\n⏰ Time: ${interview_time||''}${meetLine}\n\n— ${HRM_COMPANY} HR Portal`
      }, 'text', cid, name, 'Interview Scheduled - Interviewer').catch(e => console.error('HRM WA interviewer err:', e.message));
    }

    res.json({ ok: true, id: cid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Update candidate status
app.put('/api/hrm/candidates/:id/status', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const { status, reschedule_date, reschedule_time, reschedule_reason, joining_date, salary, department } = req.body;
    const validStatuses = ['Scheduled','Rescheduled','Selected','Rejected','Offer Sent','Offer Letter Sent'];
    if (!validStatuses.includes(status)) return res.status(400).json({ error: 'Invalid status' });
    const [[c]] = await db.query('SELECT * FROM hrm_candidates WHERE id=?', [req.params.id]);
    if (!c) return res.status(404).json({ error: 'Not found' });

    const HRM_ALLOWED_COLS = new Set(['status','reschedule_date','reschedule_time','reschedule_reason','joining_date','salary','offer_sent','department']);
    const updates = { status };
    if (status === 'Rescheduled') { updates.reschedule_date = reschedule_date||null; updates.reschedule_time = reschedule_time||''; updates.reschedule_reason = reschedule_reason||''; }
    if (status === 'Offer Sent')  { updates.joining_date = joining_date||null; updates.salary = salary||''; updates.offer_sent = 1; updates.department = department||''; }
    if (status === 'Offer Letter Sent') { updates.joining_date = joining_date||c.joining_date||null; updates.salary = salary||c.salary||''; updates.department = department||c.department||''; }

    const invalidCol = Object.keys(updates).find(k => !HRM_ALLOWED_COLS.has(k));
    if (invalidCol) return res.status(400).json({ error: `Invalid field: ${invalidCol}` });
    const fields = Object.keys(updates).map(k => `${k}=?`).join(',');
    await db.query(`UPDATE hrm_candidates SET ${fields} WHERE id=?`, [...Object.values(updates), req.params.id]);

    const meetLine = c.meeting_link ? `\n🔗 Meeting Link: ${c.meeting_link}` : '';

    if (status === 'Rescheduled') {
      hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`Hello ${c.name}! 🔄\n\nYour interview has been rescheduled.\n\n💼 Position: ${c.profile_position}\n📅 New Date: ${reschedule_date||''}\n⏰ New Time: ${reschedule_time||''}${meetLine}${reschedule_reason ? '\n\n📝 Reason: '+reschedule_reason : ''}\n\nSorry for the inconvenience.\n\n— ${HRM_COMPANY} HR Team`
      }, 'text', c.id, c.name, 'Rescheduled - Candidate').catch(e => console.error('HRM WA resched candidate err:', e.message));
      if (c.interviewer_phone) {
        hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.interviewer_phone), text:
`🔄 *Interview Rescheduled*\n\n👤 Candidate: ${c.name}\n💼 Position: ${c.profile_position}\n📅 New Date: ${reschedule_date||''}\n⏰ New Time: ${reschedule_time||''}${meetLine}\n\n— ${HRM_COMPANY} HR Portal`
        }, 'text', c.id, c.name, 'Rescheduled - Interviewer').catch(e => console.error('HRM WA resched interviewer err:', e.message));
      }
    }
    if (status === 'Selected') {
      hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`Congratulations ${c.name}! 🎉\n\nYou have been selected for ${c.profile_position}.\n\nWelcome to ${HRM_COMPANY}. Our HR team will share offer details soon.\n\nPlease keep documents ready:\n- Educational certificates\n- Experience letters\n- ID proof\n- 2 passport-size photos\n\n— ${HRM_COMPANY} HR Team`
      }, 'text', c.id, c.name, 'Selected').catch(e => console.error('HRM WA selected err:', e.message));
    }
    if (status === 'Rejected') {
      hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`Hello ${c.name},\n\nThank you for applying for ${c.profile_position}.\n\nAfter careful review, we are unable to move forward at this time. We may consider you for future openings.\n\nBest wishes.\n\n— ${HRM_COMPANY} HR Team`
      }, 'text', c.id, c.name, 'Rejected').catch(e => console.error('HRM WA rejected err:', e.message));
    }
    let pdfGenerated = true, pdfError = null;
    if (status === 'Offer Sent') {
      const { offer_name, offer_position } = req.body;
      const displayName = offer_name || c.name;
      const displayPos  = offer_position || c.profile_position;
      const displayDept = department || displayPos;
      const joiningFmt  = joining_date ? new Date(joining_date).toLocaleDateString('en-IN',{day:'2-digit',month:'long',year:'numeric'}) : '';

      // Awaited (not fire-and-forget) — on Vercel serverless, work started after
      // the response is sent can get frozen mid-flight, which was silently
      // dropping the PDF generation / WhatsApp send for some candidates.
      try {
        const { fileId, pdfUrl } = await hrmGenerateOfferDoc(c, joining_date, salary, offer_name, offer_position);
        await db.query('UPDATE hrm_candidates SET offer_drive_id=? WHERE id=?', [fileId, c.id]);
        const caption = `Hello ${displayName}! 🎉\n\n*OFFER LETTER - ${HRM_COMPANY}*\n\nCongratulations! You have been offered the position of *${displayPos}*.\n\n📅 Joining Date: ${joiningFmt}\n💰 CTC: ${salary||'To be discussed'}\n\nPlease confirm acceptance within 3 working days.\n\nWelcome to the team!\n\n— ${HRM_COMPANY} HR Team`;
        // Field names (mediaUrl/mediaType/fileName) match the old Apps Script's
        // working sendWhatsAppFile() call — a previous attempt here used
        // document/filename instead and was blamed for "not delivering the PDF",
        // but that failure was actually the missing offer_drive_id column
        // throwing before this call was ever reached (now fixed).
        const fileSent = await hrmSendWhatsApp(HRM_FILE_ENDPOINT, {
          to: hrmFormatPhone(c.phone),
          mediaUrl: pdfUrl,
          mediaType: 'document',
          fileName: `PRELIMINARY OFFER LETTER - ${displayName}.pdf`,
          caption
        }, 'file', c.id, c.name, 'Offer Sent');
        if (!fileSent) {
          // Fallback so the candidate isn't left with nothing if the file API rejects this call.
          const driveLink = `https://drive.google.com/file/d/${fileId}/view`;
          await hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`${caption}\n\n📄 *Offer Letter PDF:*\n${driveLink}`
          }, 'text', c.id, c.name, 'Offer Sent - Link Fallback');
        }
      } catch (e) {
        pdfGenerated = false;
        pdfError = e.message;
        console.error('HRM offer doc generation failed:', e.message);
        await hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`Hello ${displayName}! 🎉\n\n*OFFER LETTER - ${HRM_COMPANY}*\n\nCongratulations! You have been offered the position of ${displayPos}.\n\n📅 Joining Date: ${joiningFmt}\n💰 CTC: ${salary||'To be discussed'}\n\nPlease confirm acceptance within 3 working days.\n\nWelcome to the team!\n\n— ${HRM_COMPANY} HR Team`
        }, 'text', c.id, c.name, 'Offer Sent').catch(() => {});
        await db.query(
          `INSERT INTO hrm_message_log (candidate_id,candidate_name,phone,action,type,status,error_detail,payload_json) VALUES (?,?,?,?,?,?,?,?)`,
          [c.id, c.name, hrmFormatPhone(c.phone), 'Offer Letter PDF', 'file', 'Failed', `Drive error: ${e.message}`, '{}']
        ).catch(() => {});
      }

      // Notify Simran so she can create the official email ID before joining date
      const [[simran]] = await db.query(`SELECT id, name, phone FROM users WHERE name='Simran Gurnani' LIMIT 1`);
      if (simran?.phone) {
        hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(simran.phone), text:
`🆕 *New Employee Onboarding*\n\n👤 Name: ${displayName}\n🏢 Department: ${displayDept}\n💼 Position: ${displayPos}\n📅 Joining Date: ${joiningFmt}\n\n⚠️ Please create the official email ID before the joining date.\n\n— HR Portal`
        }, 'text', c.id, c.name, 'Offer Sent - Simran Notify').catch(e => console.error('HRM WA simran notify err:', e.message));
      }

      // Auto-delegate a task to Simran — due exactly on the joining date (no
      // holiday/week-off shifting — the employee joins that day regardless).
      if (simran?.id) {
        const taskDesc = `Create official email ID for ${displayName} — Department: ${displayDept}, Position: ${displayPos}, Joining Date: ${joiningFmt}`;
        db.query(
          `INSERT INTO delegation_tasks (description,assigned_to,assigned_by,due_date,status,priority,approval,remarks,client_id,url,awaiting_due_date) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
          [taskDesc, simran.id, req.session.userId, joining_date||null, 'pending', 'low', 'no', '', null, null, 0]
        ).catch(e => console.error('HRM auto-delegate task err:', e.message));

        if (simran.phone) {
          const dueFmt = (joining_date||'').split('-').reverse().join('-');
          const assignerName = req.session.name || 'HR';
          const taskMsg = `Hello ${simran.name || ''},\n\n📋 *New Task Delegated*\n\n*By:* ${assignerName}\n*Due:* ${dueFmt}\n*Priority:* LOW\n\n*Task:* ${taskDesc}\n\n— E-Marketing Task Manager`;
          sendWhatsApp(simran.phone, taskMsg).catch(e => console.error('HRM task delegation WA err:', e.message));
        }
      }
    }

    // Final "Offer Letter Sent" stage — reuses whatever position/department/
    // joining-date/salary is already stored on the candidate from the
    // preliminary stage; no new form fields, same as Selected/Rejected.
    if (status === 'Offer Letter Sent') {
      const { offer_name, offer_position } = req.body;
      const displayName = offer_name || c.name;
      const displayPos  = offer_position || c.profile_position;
      const finalJoiningDate = joining_date || c.joining_date;
      const finalSalary = salary || c.salary;
      const finalJoiningFmt = finalJoiningDate ? new Date(finalJoiningDate).toLocaleDateString('en-IN',{day:'2-digit',month:'long',year:'numeric'}) : '';

      try {
        const { fileId, pdfUrl } = await hrmGenerateFinalOfferDoc(c, finalJoiningDate, finalSalary, offer_name, offer_position);
        await db.query('UPDATE hrm_candidates SET final_offer_drive_id=? WHERE id=?', [fileId, c.id]);
        const caption = `Hello ${displayName}! 🎉\n\n*OFFER LETTER - ${HRM_COMPANY}*\n\nCongratulations! Please find attached your official Offer Letter for the position of *${displayPos}*.\n\n📅 Joining Date: ${finalJoiningFmt}\n💰 CTC: ${finalSalary||'To be discussed'}\n\nWelcome to the team!\n\n— ${HRM_COMPANY} HR Team`;
        const fileSent = await hrmSendWhatsApp(HRM_FILE_ENDPOINT, {
          to: hrmFormatPhone(c.phone),
          mediaUrl: pdfUrl,
          mediaType: 'document',
          fileName: `OFFER LETTER - ${displayName}.pdf`,
          caption
        }, 'file', c.id, c.name, 'Offer Letter Sent');
        if (!fileSent) {
          const driveLink = `https://drive.google.com/file/d/${fileId}/view`;
          await hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`${caption}\n\n📄 *Offer Letter PDF:*\n${driveLink}`
          }, 'text', c.id, c.name, 'Offer Letter Sent - Link Fallback');
        }
      } catch (e) {
        pdfGenerated = false;
        pdfError = e.message;
        console.error('HRM final offer doc generation failed:', e.message);
        await hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`Hello ${displayName}! 🎉\n\n*OFFER LETTER - ${HRM_COMPANY}*\n\nCongratulations! You have been issued your official Offer Letter for the position of ${displayPos}.\n\n📅 Joining Date: ${finalJoiningFmt}\n💰 CTC: ${finalSalary||'To be discussed'}\n\nWelcome to the team!\n\n— ${HRM_COMPANY} HR Team`
        }, 'text', c.id, c.name, 'Offer Letter Sent').catch(() => {});
        await db.query(
          `INSERT INTO hrm_message_log (candidate_id,candidate_name,phone,action,type,status,error_detail,payload_json) VALUES (?,?,?,?,?,?,?,?)`,
          [c.id, c.name, hrmFormatPhone(c.phone), 'Offer Letter PDF (Final)', 'file', 'Failed', `Drive error: ${e.message}`, '{}']
        ).catch(() => {});
      }
    }

    res.json({ ok: true, pdfGenerated, pdfError });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Generate / regenerate offer letter doc for an existing candidate
app.post('/api/hrm/candidates/:id/generate-offer', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[c]] = await db.query('SELECT * FROM hrm_candidates WHERE id=?', [req.params.id]);
    if (!c) return res.status(404).json({ error: 'Not found' });
    if (c.status !== 'Offer Sent') return res.status(400).json({ error: 'Candidate status is not Offer Sent' });
    const { fileId, pdfUrl } = await hrmGenerateOfferDoc(c, c.joining_date, c.salary);
    await db.query('UPDATE hrm_candidates SET offer_drive_id=? WHERE id=?', [fileId, c.id]);
    const joiningFmt = c.joining_date ? new Date(c.joining_date).toLocaleDateString('en-IN',{day:'2-digit',month:'long',year:'numeric'}) : '';
    const caption = `Hello ${c.name}! 🎉\n\n*OFFER LETTER - ${HRM_COMPANY}*\n\nCongratulations! You have been offered the position of *${c.profile_position||''}*.\n\n📅 Joining Date: ${joiningFmt}\n💰 CTC: ${c.salary||'To be discussed'}\n\nPlease confirm acceptance within 3 working days.\n\nWelcome to the team!\n\n— ${HRM_COMPANY} HR Team`;
    let waSent = await hrmSendWhatsApp(HRM_FILE_ENDPOINT, {
      to: hrmFormatPhone(c.phone),
      mediaUrl: pdfUrl,
      mediaType: 'document',
      fileName: `PRELIMINARY OFFER LETTER - ${c.name}.pdf`,
      caption
    }, 'file', c.id, c.name, 'Offer Letter PDF');
    if (!waSent) {
      const driveLink = `https://drive.google.com/file/d/${fileId}/view`;
      waSent = await hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`${caption}\n\n📄 *Offer Letter PDF:*\n${driveLink}`
      }, 'text', c.id, c.name, 'Offer Letter PDF - Link Fallback');
    }
    res.json({ ok: true, fileId, url: `https://drive.google.com/file/d/${fileId}/view`, pdfUrl, waSent });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Export offer letter template as HTML for live preview in portal
app.get('/api/hrm/offer-template-html', requireAuth, (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  res.json({ html: hrmBuildOfferHtml('{{CANDIDATE_NAME}}', '{{POSITION}}', '{{JOINING_DATE}}', '{{Today_Date}}') });
});

// Live HTML preview of the FINAL offer letter for the in-app editor. Returns the
// letter with the letterhead shown once at the top (inlineHeader) so the on-screen
// preview reads like a page; the sent PDF repeats it on every page instead.
app.get('/api/hrm/final-offer-preview-html', requireAuth, (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  const name = String(req.query.name || '');
  const position = String(req.query.position || '');
  const salary = String(req.query.salary || '');
  const joiningFmt = req.query.joining_date
    ? new Date(req.query.joining_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' })
    : '';
  const today = _hrmLetterDateFmt(req.query.letter_date);
  res.json({ html: hrmBuildFinalOfferHtml(name, position, joiningFmt, salary, today, { inlineHeader: true, joiningDate: req.query.joining_date, probationMonths: req.query.probation_months }) });
});

// Letter-date line: HR-editable (letter_date input), defaults to today.
function _hrmLetterDateFmt(letterDate) {
  const d = letterDate ? new Date(letterDate) : new Date();
  return (isNaN(d.getTime()) ? new Date() : d).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
}

// Exact-PDF preview for HR: streams the same pdfkit PDF the candidate will get.
app.post('/api/hrm/final-offer-render', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const { name = '', position = '', joining_date = '', salary = '', probation_months = '', letter_date = '' } = req.body;
    const joiningFmt = joining_date
      ? new Date(joining_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' })
      : '';
    const today = _hrmLetterDateFmt(letter_date);
    const pdf = await hrmRenderFinalOfferPdfBuffer({
      name: String(name), position: String(position), joiningFmt,
      salary: String(salary), today, joiningDate: joining_date,
      probationMonths: probation_months,
    });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'inline; filename="offer-letter-preview.pdf"');
    res.send(pdf);
  } catch (err) {
    console.error('final-offer-render error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Send the final offer letter: persist the HR-approved fields as a snapshot +
// token, then WhatsApp the candidate the public /offer-pdf/:token URL as an
// attached document — that endpoint serves the pdfkit-rendered PDF (letterhead
// on every page, signature, real page breaks), which neither the Apps Script
// Google-Doc pipeline nor Vercel-hosted Chromium could produce.
app.post('/api/hrm/candidates/:id/send-final-offer', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[c]] = await db.query('SELECT * FROM hrm_candidates WHERE id=?', [req.params.id]);
    if (!c) return res.status(404).json({ error: 'Not found' });

    const { offer_name, offer_position, joining_date, salary, department, probation_months, letter_date } = req.body;
    const name = (offer_name || c.name || '').trim();
    const position = (offer_position || c.profile_position || '').trim();
    const finalJoining = joining_date || c.joining_date;
    const finalSalary = (salary != null && salary !== '') ? salary : c.salary;
    if (!name) return res.status(400).json({ error: 'Candidate name required' });
    if (!finalJoining) return res.status(400).json({ error: 'Joining date required' });

    const joiningFmt = new Date(finalJoining).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
    const today = _hrmLetterDateFmt(letter_date);
    // Raw YYYY-MM-DD joining date for the dynamic acceptance-block dates.
    const rawJoin = (typeof finalJoining === 'string')
      ? finalJoining.slice(0, 10)
      : new Date(new Date(finalJoining).getTime() - new Date(finalJoining).getTimezoneOffset() * 60000).toISOString().slice(0, 10);

    const token = require('crypto').randomBytes(24).toString('hex');
    const snapshot = { name, position, joiningFmt, joining_date: rawJoin, salary: finalSalary || '', today, probation_months: probation_months || '' };

    await db.query(
      `UPDATE hrm_candidates SET status='Offer Letter Sent', joining_date=?, salary=?, department=COALESCE(?, department), final_offer_token=?, final_offer_data=? WHERE id=?`,
      [finalJoining, finalSalary || null, department || null, token, JSON.stringify(snapshot), c.id]
    );

    // Pick the host for the public PDF URL carefully — the WhatsApp provider
    // must be able to fetch it anonymously:
    // - *.vercel.app preview/deployment URLs (hash or branch subdomains) sit
    //   behind Vercel Authentication and 302 to a login page for anonymous
    //   fetchers (observed: provider got the redirect, attach failed, bare
    //   link fallback went out) -> use APP_URL (stable production) instead.
    // - A custom domain (e.g. taskmanager.e-marketing.io) is public: use it.
    // Either way production must run current code, else /offer-pdf 500s there.
    const reqHost = req.headers['x-forwarded-host'] || req.get('host') || '';
    const isVercelPreview = /\.vercel\.app$/i.test(reqHost) ;
    const base = ((isVercelPreview || !reqHost)
      ? (process.env.APP_URL || `https://${reqHost}`)
      : `${req.headers['x-forwarded-proto'] || req.protocol}://${reqHost}`).replace(/\/$/, '');
    const pdfUrl = `${base}/offer-pdf/${token}`;

    const caption = `Hello ${name}! 🎉\n\n*OFFER LETTER - ${HRM_COMPANY}*\n\nCongratulations! Please find attached your official Offer Letter for the position of *${position}*.\n\n📅 Joining Date: ${joiningFmt}\n💰 CTC: ${finalSalary || 'To be discussed'}\n\nWelcome to the team!\n\n— ${HRM_COMPANY} HR Team`;

    let waSent = false;
    if (c.phone) {
      waSent = await hrmSendWhatsApp(HRM_FILE_ENDPOINT, {
        to: hrmFormatPhone(c.phone),
        mediaUrl: pdfUrl,
        mediaType: 'document',
        fileName: `OFFER LETTER - ${name}.pdf`,
        caption
      }, 'file', c.id, c.name, 'Offer Letter Sent');
      if (!waSent) {
        await hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text: `${caption}\n\n📄 *Offer Letter PDF:*\n${pdfUrl}` }, 'text', c.id, c.name, 'Offer Letter Sent - Link Fallback');
      }
    }
    res.json({ ok: true, pdfUrl, waSent });
  } catch (err) {
    console.error('send-final-offer error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Read offer letter template text + show service account email
app.get('/api/hrm/offer-template-preview', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });

  // Always return service account email so user knows what to share with
  let serviceAccountEmail = null;
  try {
    const raw = process.env.GOOGLE_CREDENTIALS ? JSON.parse(process.env.GOOGLE_CREDENTIALS) : require('./credentials.json');
    serviceAccountEmail = raw.client_email || null;
  } catch {}

  try {
    const drive = await _hrmDriveClient();
    const exported = await drive.files.export(
      { fileId: HRM_OFFER_TEMPLATE_ID, mimeType: 'text/plain' },
      { responseType: 'text' }
    );
    const text = exported.data || '';
    res.json({ ok: true, serviceAccountEmail, text });
  } catch (err) { res.status(500).json({ error: err.message, serviceAccountEmail }); }
});

// Get message log. created_at_fmt is formatted in SQL (the codebase convention
// for timestamps): the DB stores IST wall-time, but mysql2 (Node on UTC) tags
// it as UTC, so a browser-side toLocaleString('en-IN', Asia/Kolkata) adds
// +5:30 AGAIN and shows times 5.5h in the future — see brain.md Section 16.
app.get('/api/hrm/messages', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [rows] = await db.query(`SELECT *, DATE_FORMAT(created_at, '%e/%c/%Y, %l:%i:%s %p') AS created_at_fmt FROM hrm_message_log WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT 500`);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Soft-delete a message log entry (hides it from the log; row stays in DB)
app.delete('/api/hrm/messages/:id', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [result] = await db.query('UPDATE hrm_message_log SET deleted_at=NOW() WHERE id=? AND deleted_at IS NULL', [req.params.id]);
    if (!result.affectedRows) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Retry failed message
app.post('/api/hrm/messages/:id/retry', requireAuth, async (req, res) => {
  if (!['admin','hod'].includes(req.session.role)) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[msg]] = await db.query('SELECT * FROM hrm_message_log WHERE id=?', [req.params.id]);
    if (!msg) return res.status(404).json({ error: 'Not found' });
    if (msg.status === 'Sent') return res.status(400).json({ ok: false, message: 'Message already sent successfully' });
    let parsed;
    try { parsed = JSON.parse(msg.payload_json); } catch { return res.status(400).json({ error: 'Payload corrupt' }); }

    let status = 'Failed', errorDetail = '';
    try {
      const fetchFn = global.fetch || (await import('node-fetch')).default;
      const resp = await fetchFn(parsed.endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': HRM_AMUFIY_API_KEY },
        body: JSON.stringify(parsed.body)
      });
      if (resp.ok) { status = 'Sent'; } else {
        const txt = await resp.text();
        errorDetail = `HTTP ${resp.status}: ${txt.slice(0,200)}`;
      }
    } catch (e) { errorDetail = e.message; }

    await db.query(
      `UPDATE hrm_message_log SET status=?, error_detail=?, retry_count=retry_count+1, last_retry_at=NOW() WHERE id=?`,
      [status, errorDetail, req.params.id]);
    res.json({ ok: status === 'Sent', status, message: status === 'Sent' ? 'Resent successfully' : 'Retry failed: '+errorDetail });
  } catch (err) { res.status(500).json({ error: err.message }); }
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