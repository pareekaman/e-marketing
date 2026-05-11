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

const app = express();
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.SESSION_SECRET || 'taskmanager_secret_2026';

const cookieParser = require('cookie-parser');
app.use(cookieParser());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

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
  // ⚠️ Shared hosting ka max_user_connections usually 5-10 hota hai.
  // Vercel serverless me multiple function instances ek saath connect karte hain,
  // isliye limit chhoti rakhi hai (2) per-instance.
  connectionLimit: Number(process.env.DB_POOL_SIZE) || 2,
  queueLimit: 0,
  connectTimeout: 30000,
  // Idle connections ko jaldi release karo (default 8hrs hai mysql me, 30s ideal)
  idleTimeout: 30000,
  enableKeepAlive: false,
  // SSL support for cloud MySQL providers (Aiven, PlanetScale, Railway, etc.)
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
};

const _rawPool = mysql.createPool(dbConfig);

// Wrap pool with retry logic for "max_user_connections" errors
// Shared hosting pe ye error aata rehta hai jab Vercel concurrent requests bhejta hai.
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
(async () => {
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_employee (employee_id),
    INDEX idx_start (start_date)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

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
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN col_letter VARCHAR(10) DEFAULT '' AFTER row_label`);
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN field_type VARCHAR(20) DEFAULT 'text' AFTER col_letter`);
  await sa(`ALTER TABLE fms_extra_rows ADD COLUMN dropdown_options TEXT DEFAULT '' AFTER field_type`);

  console.log('  ✅ DB migrations checked');

  // ── Auto-seed default admin if no users exist ─────────
  try {
    const [[{ cnt }]] = await db.query('SELECT COUNT(*) AS cnt FROM users');
    if (cnt === 0) {
      const hash = bcrypt.hashSync('password', 10);
      await db.query(
        'INSERT INTO users (name, email, password, role, department) VALUES (?,?,?,?,?)',
        ['Aman Admin', 'aman@test.com', hash, 'admin', 'Management']
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
      <p><b>${assignerName || 'Someone'}</b> ne aapko ek naya delegation task assign kiya hai:</p>
      <table style="width:100%;border-collapse:collapse;margin:20px 0;">
        <tr><td style="padding:8px;background:#f0f4f8;width:140px;"><b>Task</b></td><td style="padding:8px;">${desc}</td></tr>
        <tr><td style="padding:8px;background:#f0f4f8;"><b>Due Date</b></td><td style="padding:8px;">${dueDate}</td></tr>
        <tr><td style="padding:8px;background:#f0f4f8;"><b>Priority</b></td><td style="padding:8px;text-transform:capitalize;">${priority}</td></tr>
        <tr><td style="padding:8px;background:#f0f4f8;"><b>Approval Required</b></td><td style="padding:8px;text-transform:capitalize;">${approval}</td></tr>
        ${remarks ? `<tr><td style="padding:8px;background:#f0f4f8;"><b>Remarks</b></td><td style="padding:8px;">${remarks}</td></tr>` : ''}
      </table>
      <a href="${appUrl}" style="display:inline-block;background:#F39C12;color:#fff;text-decoration:none;padding:12px 24px;border-radius:6px;font-weight:600;">Open E-Marketing Task Manager</a>
      <p style="color:#777;font-size:12px;margin-top:30px;">Ye automated email hai — E-Marketing Task Manager se.</p>
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
    req.session = { userId: decoded.userId, role: decoded.role, name: decoded.name };
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
function requireAdminOrPC(req, res, next) {
  if (req.session.role === 'admin' || req.session.role === 'pc') return next();
  res.status(403).json({ error: 'Admin or PC only' });
}
function getTable(type) {
  return type === 'delegation' ? 'delegation_tasks' : 'checklist_tasks';
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
      phone VARCHAR(50) DEFAULT NULL, department VARCHAR(255) DEFAULT '',
      week_off VARCHAR(50) DEFAULT '', extra_off TEXT,
      exclude_from_reminder TINYINT(1) DEFAULT 0,
      profile_image LONGTEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'users table');

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
      INDEX idx_step (step_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'fms_extra_rows table');

    await sa(`CREATE TABLE IF NOT EXISTS week_plans (
      id INT AUTO_INCREMENT PRIMARY KEY, employee_id INT NOT NULL, hod_id INT,
      start_date DATE NOT NULL, target_count INT DEFAULT 0,
      improvement_pct DECIMAL(5,2) DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_employee (employee_id), INDEX idx_start (start_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, 'week_plans table');

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
          ['Aman Admin', 'aman@test.com', hash, 'admin', 'Management']
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

app.get('/api/me', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query('SELECT id,name,email,notification_email,role,phone,profile_image,department,week_off FROM users WHERE id=?', [req.session.userId]);
    if (!rows[0]) return res.status(404).json({ error: 'User not found' });
    // extra_off fetch separately — safe if column not yet added
    try {
      const [ex] = await db.query('SELECT extra_off FROM users WHERE id=?', [req.session.userId]);
      rows[0].extra_off = ex[0]?.extra_off || '';
    } catch(e) { rows[0].extra_off = ''; }
    res.json(rows[0]);
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
      if (filterEmployee && filterEmployee !== 'all') {
        userFilter = 'AND t.assigned_to = ?'; params = [filterEmployee];
      } else {
        // HOD ka department DB se fetch karo — query param pe depend mat karo
        let resolvedDept = hodDept;
        if (!resolvedDept) {
          const [meRow] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
          resolvedDept = meRow[0]?.department || '';
        }
        if (!resolvedDept) {
          // Department set nahi hai — sirf apni tasks dikhao
          userFilter = 'AND t.assigned_to = ?'; params = [uid];
        } else {
          const [deptUsers] = await db.query('SELECT id FROM users WHERE department=? AND role NOT IN (?,?)', [resolvedDept, 'admin','hod']);
          if (!deptUsers.length) {
            // Dept mein koi user nahi — apni tasks dikhao
            userFilter = 'AND t.assigned_to = ?'; params = [uid];
          } else {
            const ids = deptUsers.map(u=>u.id);
            // HOD khud bhi include karo
            if (!ids.includes(uid)) ids.push(uid);
            userFilter = `AND t.assigned_to IN (${ids.map(()=>'?').join(',')})`;
            params = ids;
          }
        }
      }
    } else {
      userFilter = 'AND t.assigned_to = ?'; params = [uid];
    }

    // Stats + Table: aaj aur usse pehle ki pending tasks (due_date <= CURDATE())
    // PC: agar date range diya hai toh woh use karo
    const dateClause = isPC && dateFrom && dateTo
      ? `AND t.due_date BETWEEN '${dateFrom}' AND '${dateTo}'`
      : `AND t.due_date <= CURDATE()`;

    const taskType = req.query.taskType || 'both';
    let pending = 0, revised = 0, completed = 0;

    if (taskType === 'delegation' || taskType === 'both') {
      const [d] = await db.query(`SELECT SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN status='revised' THEN 1 ELSE 0 END) AS revised,SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed FROM delegation_tasks t WHERE 1=1 ${userFilter} ${dateClause}`, params);
      pending += parseInt(d[0].pending)||0; revised += parseInt(d[0].revised)||0; completed += parseInt(d[0].completed)||0;
    }
    if (taskType === 'checklist' || taskType === 'both') {
      const [d] = await db.query(`SELECT SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN status='revised' THEN 1 ELSE 0 END) AS revised,SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed FROM checklist_tasks t WHERE 1=1 ${userFilter} ${dateClause}`, params);
      pending += parseInt(d[0].pending)||0; revised += parseInt(d[0].revised)||0; completed += parseInt(d[0].completed)||0;
    }

    let delegationPending = [], checklistPending = [];
    if (taskType === 'delegation' || taskType === 'both') {
      const [rows] = await db.query(`SELECT t.id,'delegation' AS type,t.description,t.status,t.assigned_to,COALESCE(t.priority,'low') AS priority,COALESCE(t.approval,'no') AS approval,COALESCE(t.waiting_approval,0) AS waiting_approval,t.remarks,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u1.name AS assignedToName,u2.name AS assignedByName FROM delegation_tasks t JOIN users u1 ON t.assigned_to=u1.id JOIN users u2 ON t.assigned_by=u2.id WHERE t.status='pending' ${dateClause} ${userFilter} ORDER BY t.due_date ASC LIMIT 500`, params);
      delegationPending = rows;
    }
    if (taskType === 'checklist' || taskType === 'both') {
      const [rows] = await db.query(`SELECT t.id,'checklist' AS type,t.description,t.status,t.assigned_to,COALESCE(t.priority,'low') AS priority,'no' AS approval,0 AS waiting_approval,t.remarks,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u1.name AS assignedToName,u2.name AS assignedByName FROM checklist_tasks t JOIN users u1 ON t.assigned_to=u1.id JOIN users u2 ON t.assigned_by=u2.id WHERE t.status='pending' ${dateClause} ${userFilter} ORDER BY t.due_date ASC LIMIT 500`, params);
      checklistPending = rows;
    }
    res.json({ pending, revised, completed, todayPending: [...delegationPending, ...checklistPending] });
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
      // "Delegate by Me" mode — sirf woh tasks jinhe MAINE assign kiya hai.
      // Role-based scoping skip — koi bhi role apne assign kiye tasks dekh sakta hai.
      where += ' AND t.assigned_by = ?';
      params.push(uid);
    } else if (isAdmin || role === 'pc') {
      // Admin/PC — sab dikhta hai
    } else if (isHod) {
      // HOD — apne department ke users ki tasks
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
      // Regular user — sirf apni tasks
      where += ' AND t.assigned_to = ?';
      params.push(uid);
    }

    // All Tasks — Delegation me upcoming/future tasks bhi dikhao (taaki kal/parso ke task pehle se visible ho aur transfer ho sakein).
    // Checklist me future wale chhupao (jaise pehle tha) — checklist usually recurring/today tak hi relevant hai.
    // Note: "Delegate by Me" view me bhi same rule — delegation me future visible.
    if (!isDeleg) {
      where += ' AND t.due_date <= CURDATE()';
    }

    const [tasks] = await db.query(`SELECT t.id,'${type||'delegation'}' AS type,t.description,t.status,t.assigned_to,t.assigned_by,COALESCE(t.priority,'low') AS priority,${isDeleg?"COALESCE(t.approval,'no') AS approval,COALESCE(t.waiting_approval,0) AS waiting_approval,t.remarks,":"'no' AS approval,0 AS waiting_approval,t.remarks,"}DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u1.name AS assignedToName,u2.name AS assignedByName FROM ${table} t JOIN users u1 ON t.assigned_to=u1.id JOIN users u2 ON t.assigned_by=u2.id ${where} ORDER BY t.due_date ASC`, params);

    // mine=1 mode me hamesha flat tasks return karte hain (grouped nahi)
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
    const { type, desc, assignedTo, approverEmail, approver, date, priority, approval, remarks } = req.body;
    const isAdmin = req.session.role === 'admin';
    const isHod   = req.session.role === 'hod';
    const isUser  = req.session.role === 'user';
    // Admin, HOD and regular users can all assign to others; fallback to self if not specified
    const targetUser = (isAdmin || isHod || isUser) && assignedTo ? parseInt(assignedTo) : req.session.userId;
    if (!desc || !date) return res.status(400).json({ error: 'Description and date required' });

    // Holiday / week-off check — auto-adjust due_date if needed
    let effectiveDate = date;
    let adjusted = false, adjustedReason = '';
    try {
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
      await db.query(`INSERT INTO delegation_tasks (description,assigned_to,assigned_by,due_date,status,priority,approval,remarks) VALUES (?,?,?,?,?,?,?,?)`, [desc, targetUser, assignedBy, effectiveDate, 'pending', priority||'low', approval||'no', remarks||'']);
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
              desc, dueDate: effectiveDate,
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
            const dueFmt = (effectiveDate||'').split('-').reverse().join('-');
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
      await db.query(`INSERT INTO checklist_tasks (description,assigned_to,assigned_by,due_date,status,priority,remarks) VALUES (?,?,?,?,?,?,?)`, [desc, targetUser, req.session.userId, effectiveDate, 'pending', priority||'low', remarks||'']);
    }
    res.json({ success: true, adjusted, effectiveDate, adjustedReason });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/tasks/bulk-checklist', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { desc, assignedTo, priority, remarks, dates } = req.body;
    if (!desc || !assignedTo || !dates || !dates.length) return res.status(400).json({ error: 'Missing fields' });

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

    const values = dates.map(date => [desc, parseInt(assignedTo), req.session.userId, date, 'pending', priority||'low', remarks||'']);
    await db.query(`INSERT INTO checklist_tasks (description,assigned_to,assigned_by,due_date,status,priority,remarks) VALUES ?`, [values]);
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
    const [rows] = await db.query(`SELECT * FROM ${table} WHERE id=?`, [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Task not found' });
    const task = rows[0];
    if (!isAdmin && !isPC && task.assigned_to !== uid) return res.status(403).json({ error: 'Not allowed' });
    if (status === 'completed' && task.waiting_approval) {
      await db.query(`DELETE FROM task_approvals WHERE task_id=? AND task_type=? AND status='pending'`, [req.params.id, type]);
      if (type === 'checklist') await db.query(`UPDATE ${table} SET status='completed' WHERE id=?`, [req.params.id]);
      else await db.query(`UPDATE ${table} SET status='completed',waiting_approval=0 WHERE id=?`, [req.params.id]);
      return res.json({ success: true, needsApproval: false });
    }
    const needsApproval = type === 'delegation' && task.approval === 'yes';
    if (needsApproval && !isAdmin && !isPC) {
      const [existing] = await db.query(`SELECT id FROM task_approvals WHERE task_id=? AND task_type=? AND status='pending'`, [req.params.id, type]);
      if (existing[0]) return res.status(400).json({ error: 'Approval already pending' });
      await db.query(`INSERT INTO task_approvals (task_id,task_type,requested_by,requested_to,action_type,status,note) VALUES (?,?,?,?,?,'pending',?)`, [req.params.id, type, uid, task.assigned_by, status, reason||'']);
      if (newDate && status === 'revised') await db.query(`UPDATE ${table} SET waiting_approval=1,due_date=? WHERE id=?`, [newDate, req.params.id]);
      else await db.query(`UPDATE ${table} SET waiting_approval=1 WHERE id=?`, [req.params.id]);
      return res.json({ success: true, needsApproval: true });
    }
    if (newDate && status === 'revised') await db.query(`UPDATE ${table} SET status=?,waiting_approval=0,due_date=? WHERE id=?`, [status, newDate, req.params.id]);
    else {
      // checklist_tasks mein waiting_approval column nahi hota
      if (type === 'checklist') await db.query(`UPDATE ${table} SET status=? WHERE id=?`, [status, req.params.id]);
      else await db.query(`UPDATE ${table} SET status=?,waiting_approval=0 WHERE id=?`, [status, req.params.id]);
    }
    res.json({ success: true, needsApproval: false });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/tasks/:id/detail', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { type } = req.query;
    const table = getTable(type||'delegation');
    const [rows] = await db.query(`SELECT t.*,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date FROM ${table} t WHERE t.id=?`, [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Task not found' });
    res.json({ task: rows[0] });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/tasks/:id/edit', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { type, desc, date, priority, approval, remarks } = req.body;
    const table = getTable(type||'delegation');
    if (type === 'delegation') await db.query(`UPDATE ${table} SET description=?,due_date=?,priority=?,approval=?,remarks=? WHERE id=?`, [desc, date, priority||'low', approval||'no', remarks||'', req.params.id]);
    else await db.query(`UPDATE ${table} SET description=?,due_date=?,remarks=? WHERE id=?`, [desc, date, remarks||'', req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/tasks/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { type } = req.query;
    await db.query(`DELETE FROM ${getTable(type||'delegation')} WHERE id=?`, [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk delete by user
app.delete('/api/tasks/user/:userId', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { type } = req.query;
    const table = getTable(type || 'delegation');
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
    // Admin/PC sees all pending approvals; others see only theirs
    const whereClause = isAdminOrPC
      ? `WHERE ta.status='pending'`
      : `WHERE ta.requested_to=? AND ta.status='pending'`;
    const params = isAdminOrPC ? [] : [req.session.userId];
    const [rows] = await db.query(`SELECT ta.*,u1.name AS requestedByName,u2.name AS requestedToName,dt.description,dt.approval AS taskApproval FROM task_approvals ta JOIN users u1 ON ta.requested_by=u1.id JOIN users u2 ON ta.requested_to=u2.id LEFT JOIN delegation_tasks dt ON ta.task_id=dt.id AND ta.task_type='delegation' ${whereClause} ORDER BY ta.created_at DESC`, params);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/approvals/count', requireAuth, async (req, res) => {
  try {
    const role = req.session.role;
    const isAdminOrPC = role === 'admin' || role === 'pc';
    const [rows] = isAdminOrPC
      ? await db.query(`SELECT COUNT(*) AS count FROM task_approvals WHERE status='pending'`)
      : await db.query(`SELECT COUNT(*) AS count FROM task_approvals WHERE requested_to=? AND status='pending'`, [req.session.userId]);
    res.json({ count: rows[0].count });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/approvals/:id', requireAuth, async (req, res) => {
  try {
    const { action, note } = req.body;
    const role = req.session.role;
    const [rows] = await db.query('SELECT * FROM task_approvals WHERE id=?', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Approval not found' });
    const appr = rows[0];
    // PC and admin can approve any; others only their own
    const canApprove = role === 'admin' || role === 'pc' || appr.requested_to === req.session.userId;
    if (!canApprove) return res.status(403).json({ error: 'Not allowed' });
    await db.query('UPDATE task_approvals SET status=?,note=? WHERE id=?', [action, note||'', req.params.id]);
    const table = getTable(appr.task_type);
    if (action === 'approved') await db.query(`UPDATE ${table} SET status=?,waiting_approval=0 WHERE id=?`, [appr.action_type, appr.task_id]);
    else await db.query(`UPDATE ${table} SET waiting_approval=0 WHERE id=?`, [appr.task_id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// MIS
// ══════════════════════════════════════════════════════
app.get('/api/mis', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'Dates required' });
    const isHod = req.session.role === 'hod';
    // HOD ke liye apne department ka filter
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
    const [delRows] = await db.query(`SELECT u.id AS userId,u.name,COUNT(*) AS total,SUM(CASE WHEN t.status='pending' THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN t.status='completed' THEN 1 ELSE 0 END) AS completed,SUM(CASE WHEN t.status='revised' THEN 1 ELSE 0 END) AS revised,SUM(CASE WHEN t.status='pending' AND t.due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue FROM delegation_tasks t JOIN users u ON t.assigned_to=u.id WHERE t.due_date BETWEEN ? AND ? ${deptFilter} GROUP BY u.id,u.name ORDER BY u.name`, deptParams);
    const [chlRows] = await db.query(`SELECT u.id AS userId,u.name,COUNT(*) AS total,SUM(CASE WHEN t.status='pending' THEN 1 ELSE 0 END) AS pending,SUM(CASE WHEN t.status='completed' THEN 1 ELSE 0 END) AS completed,0 AS revised,SUM(CASE WHEN t.status='pending' AND t.due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue FROM checklist_tasks t JOIN users u ON t.assigned_to=u.id WHERE t.due_date BETWEEN ? AND ? ${deptFilter} GROUP BY u.id,u.name ORDER BY u.name`, deptParams);
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
        const allCols = filteredSteps.flatMap(s => [colToIdx(s.plan_col), colToIdx(s.actual_col)]).filter(x => x >= 0);
        if (!allCols.length) continue;
        const maxCol = Math.max(...allCols);
        const lastCol = idxToCol(maxCol);
        const range = `${tabName}!A:${lastCol}`;

        const response = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range });
        const sheetData = response.data.values || [];
        const headers = sheetData[headerRowIdx] || [];
        const dataRows = sheetData.slice(headerRowIdx + 1);

        for (const step of steps) {
          const planIdx = colToIdx(step.plan_col);
          const actualIdx = colToIdx(step.actual_col);
          if (planIdx < 0 || actualIdx < 0) continue;

          dataRows.forEach((row, i) => {
            const planVal = (row[planIdx] || '').trim();
            const actualVal = (row[actualIdx] || '').trim();
            if (!planVal || actualVal) return; // skip if no plan or already done

            // Parse plan date — try to extract date from value
            // planVal might be a date string like "2026-04-07" or "07/04/2026" or just text
            let planDate = '';
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
            }

            // isLate: plan date is in the past and still pending
            const isLate = planDate && planDate < today;

            allRows.push({
              fmsName,
              fmsId: sheet.id,
              stepName: step.step_name,
              stepId: step.id,
              doer: step.doerNames || '—',
              planValue: planVal,
              planDate: planDate || '',
              isLate,
              rowNumber: headerRowIdx + 1 + i + 1
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

app.get('/api/mis/detail', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const { userId, type, start, end } = req.query;
    if (!userId || !start || !end) return res.status(400).json({ error: 'Missing params' });
    const table = type === 'delegation' ? 'delegation_tasks' : 'checklist_tasks';
    const [tasks] = await db.query(`SELECT t.id,t.description,t.status,DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,u2.name AS assigned_by_name FROM ${table} t JOIN users u2 ON t.assigned_by=u2.id WHERE t.assigned_to=? AND t.due_date BETWEEN ? AND ? ORDER BY t.due_date ASC`, [userId, start, end]);
    res.json({ tasks });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── All MIS — per employee combined score ──
app.get('/api/mis/all', requireAuth, requireAdminOrHod, async (req, res) => {
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
       WHERE t.due_date BETWEEN ? AND ? ${deptFilter}
       GROUP BY u.id, u.name, u.department ORDER BY u.name`, deptParams);

    const [chlRows] = await db.query(
      `SELECT u.id AS userId, u.name, u.department,
        COUNT(*) AS total,
        SUM(CASE WHEN t.status='pending' THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN t.status='completed' THEN 1 ELSE 0 END) AS completed,
        0 AS revised,
        SUM(CASE WHEN t.status='pending' AND t.due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM checklist_tasks t JOIN users u ON t.assigned_to=u.id
       WHERE t.due_date BETWEEN ? AND ? ${deptFilter}
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

    // Fetch week plan for each user — DATE_FORMAT taaki frontend ko clean YYYY-MM-DD mile (ISO timestamp nahi)
    let planMap = {};
    try {
      const [plans] = await db.query(
        `SELECT employee_id, target_count, DATE_FORMAT(start_date,'%Y-%m-%d') AS start_date, improvement_pct FROM week_plans WHERE start_date BETWEEN ? AND ? ORDER BY start_date DESC`, [start, end]);
      for (const p of plans) {
        if (!planMap[p.employee_id]) planMap[p.employee_id] = p;
      }
    } catch(e) { /* week_plans table may not exist yet */ }

    // ── FMS contribution per user ─────────────────────────────────────
    // Har user ke liye unke steps (jahan vo doer hain) ke pending/done count nikaalo.
    // FMS sirf admin ko applicable hai (HOD ke liye apne dept ke users bhi count honge).
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
                  const planVal = (row[planIdx]||'').trim();
                  const actualVal = (row[actualIdx]||'').trim();
                  if (planVal && !actualVal) stepPending++;
                  if (planVal && actualVal) stepDone++;
                });
                // Distribute counts to each doer (har doer ko poora count attribute karte hain — shared work)
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

    // Agar koi user sirf FMS me kaam karta hai (del/chl me 0 tasks) to use bhi userMap me daalo,
    // taaki All MIS me uska FMS contribution dikhe.
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

    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── FMS MIS ──
app.get('/api/mis/fms', requireAuth, requireAdminOrHod, async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'Dates required' });
    const isHod = req.session.role === 'hod';
    const uid = req.session.userId;

    // Get FMS sheets
    const [sheets] = await db.query('SELECT * FROM fms_sheets ORDER BY fms_name ASC');
    if (!sheets.length) return res.json([]);

    // HOD ka department pehle fetch karo (ek baar)
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

      // HOD: sirf woh steps jahan uske dept ke doers hain
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
    let dateFilter = 'AND t.due_date <= CURDATE()';
    if (dateFrom && dateTo) dateFilter = `AND t.due_date BETWEEN '${dateFrom}' AND '${dateTo}'`;
    const [rows] = await db.query(`
      SELECT DISTINCT u.id, u.name FROM users u
      WHERE u.id IN (
        SELECT DISTINCT assigned_to FROM delegation_tasks t WHERE status='pending' ${dateFilter}
        UNION
        SELECT DISTINCT assigned_to FROM checklist_tasks t WHERE status='pending' ${dateFilter}
      ) AND u.role NOT IN ('admin','pc')
      ORDER BY u.name ASC`);
    res.json(rows);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// USERS
// ══════════════════════════════════════════════════════
app.get('/api/users', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query('SELECT id,name,email,notification_email,role,phone,department,week_off,extra_off,COALESCE(exclude_from_reminder,0) AS exclude_from_reminder FROM users ORDER BY role DESC,name ASC');
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/users', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { name, email, notification_email, password, role, phone, department, week_off, extra_off, exclude_from_reminder } = req.body;
    if (!name || !email || !password) return res.status(400).json({ error: 'All fields required' });
    const [ex] = await db.query('SELECT id FROM users WHERE email=?', [email]);
    if (ex[0]) return res.status(400).json({ error: 'Email already exists' });
    await db.query('INSERT INTO users (name,email,notification_email,password,role,phone,department,week_off,extra_off,exclude_from_reminder) VALUES (?,?,?,?,?,?,?,?,?,?)',
      [name, email, notification_email||'', bcrypt.hashSync(password,10), role||'user', phone||null, department||'', week_off||'', extra_off||'', exclude_from_reminder?1:0]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/users/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { name, email, notification_email, role, password, phone, department, week_off, extra_off, exclude_from_reminder } = req.body;
    const exclVal = exclude_from_reminder ? 1 : 0;
    if (password) await db.query('UPDATE users SET name=?,email=?,notification_email=?,role=?,password=?,phone=?,department=?,week_off=?,extra_off=?,exclude_from_reminder=? WHERE id=?',
      [name,email,notification_email||'',role,bcrypt.hashSync(password,10),phone||null,department||'',week_off||'',extra_off||'',exclVal,req.params.id]);
    else await db.query('UPDATE users SET name=?,email=?,notification_email=?,role=?,phone=?,department=?,week_off=?,extra_off=?,exclude_from_reminder=? WHERE id=?',
      [name,email,notification_email||'',role,phone||null,department||'',week_off||'',extra_off||'',exclVal,req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/users/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    if (parseInt(req.params.id) === req.session.userId) return res.status(400).json({ error: 'Cannot delete yourself' });
    await db.query('DELETE FROM users WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk add users via CSV
app.post('/api/users/bulk', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { users } = req.body;
    if (!users || !users.length) return res.status(400).json({ error: 'No users provided' });
    let added = 0, skipped = 0, errors = [];
    for (const u of users) {
      if (!u.name || !u.email || !u.password) { errors.push(`${u.email||'?'}: missing fields`); continue; }
      const [ex] = await db.query('SELECT id FROM users WHERE email=?', [u.email]);
      if (ex[0]) { skipped++; continue; }
      await db.query('INSERT INTO users (name,email,password,role,phone,department,week_off,extra_off) VALUES (?,?,?,?,?,?,?,?)',
        [u.name, u.email, bcrypt.hashSync(u.password,10), u.role||'user', u.phone||null, u.department||'', u.week_off||'', u.extra_off||'']);
      added++;
    }
    res.json({ success: true, added, skipped, errors });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// PROFILE
// ══════════════════════════════════════════════════════
app.put('/api/profile', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const { name, email, notification_email, phone, currentPassword, newPassword, profileImage } = req.body;
    if (currentPassword) {
      const [rows] = await db.query('SELECT password FROM users WHERE id=?', [uid]);
      if (!bcrypt.compareSync(currentPassword, rows[0].password)) return res.status(400).json({ error: 'Current password is incorrect' });
      if (newPassword) await db.query('UPDATE users SET name=?,email=?,notification_email=?,phone=?,password=? WHERE id=?', [name,email,notification_email||'',phone||null,bcrypt.hashSync(newPassword,10),uid]);
      else await db.query('UPDATE users SET name=?,email=?,notification_email=?,phone=? WHERE id=?', [name,email,notification_email||'',phone||null,uid]);
    } else {
      await db.query('UPDATE users SET name=?,email=?,notification_email=?,phone=? WHERE id=?', [name,email,notification_email||'',phone||null,uid]);
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
      if (s.extraInput==='yes' && s.extraRows?.length) for (const row of s.extraRows) await conn.query('INSERT INTO fms_extra_rows (step_id,row_label,col_letter,field_type,dropdown_options) VALUES (?,?,?,?,?)', [stepId, row.label||row.col_letter||'', row.col_letter||'', row.field_type||'text', row.dropdown_options||'']);
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
      if (s.extraInput==='yes' && s.extraRows?.length) for (const row of s.extraRows) await conn.query('INSERT INTO fms_extra_rows (step_id,row_label,col_letter,field_type,dropdown_options) VALUES (?,?,?,?,?)', [stepId, row.label||row.col_letter||'', row.col_letter||'', row.field_type||'text', row.dropdown_options||'']);
    }
    await conn.commit();
    res.json({ success: true });
  } catch (err) { await conn.rollback(); res.status(500).json({ error: err.message }); } finally { conn.release(); }
});

app.delete('/api/fms/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
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
    dataRows.forEach((row, i) => {
      const planVal = planIdx >= 0 ? (row[planIdx]||'').trim() : '';
      const actualVal = actualIdx >= 0 ? (row[actualIdx]||'').trim() : '';
      if (!planVal || actualVal) return; // skip non-pending rows
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
      // Plan column always show karo — mandatory
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

    // Doer ka naam sheet mein likhna (agar doer_name_col configured hai)
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
      return res.json({ error: 'employeeId and startDate required' });
    }
    const impPct = (improvementPct !== undefined && improvementPct !== null && improvementPct !== '') ? parseInt(improvementPct) : null;
    // Upsert: insert or update if same employee+startDate exists
    await db.execute(
      `INSERT INTO week_plans (employee_id, hod_id, start_date, target_count, improvement_pct, created_at)
       VALUES (?, ?, ?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE target_count = VALUES(target_count), hod_id = VALUES(hod_id), improvement_pct = VALUES(improvement_pct), created_at = NOW()`,
      [employeeId, hodId || req.session.userId, startDate, targetCount, impPct]
    );
    res.json({ success: true });
  } catch (e) {
    // If table doesn't exist, create it first then retry
    if (e.code === 'ER_NO_SUCH_TABLE') {
      await db.execute(`
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
      await db.execute(
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
        await db.execute(`ALTER TABLE week_plans ADD COLUMN improvement_pct INT DEFAULT NULL`);
      } catch(ae) { /* already exists */ }
      const { employeeId, startDate, targetCount, hodId, improvementPct } = req.body;
      const impPct = (improvementPct !== undefined && improvementPct !== null && improvementPct !== '') ? parseInt(improvementPct) : null;
      await db.execute(
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
    const [rows] = await db.execute(
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
// DEBUG ENDPOINT (remove after fixing)
// ══════════════════════════════════════════════════════
app.get('/api/debug', async (req, res) => {
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
(async () => {
  const sa = async (sql) => { try { await db.query(sql); } catch(e){} };
  await sa(`CREATE TABLE IF NOT EXISTS clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);

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
  console.log('  ✅ Daily Task tables ready');
})();

// ── WhatsApp helper (Aumpfy API) ──────────────────────
async function sendWhatsApp(phone, text) {
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
// 📢 DAILY REMINDER — sends list of users who didn't fill today's task
// to a WhatsApp group. Excludes CXO department.
// ══════════════════════════════════════════════════════
const REMINDER_GROUP_ID = process.env.REMINDER_GROUP_ID || '120363400573269993@g.us';
const EXCLUDED_DEPARTMENTS = ['CXO']; // case-insensitive match

async function buildAndSendReminder() {
  // Today's date in IST (India Standard Time)
  const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
  const today = istNow.toISOString().split('T')[0]; // YYYY-MM-DD

  // Get all users — excluding CXO department + flagged users (case-insensitive)
  const [users] = await db.query(
    `SELECT id, name, COALESCE(department,'') AS department,
            COALESCE(week_off,'') AS week_off, COALESCE(extra_off,'') AS extra_off,
            COALESCE(exclude_from_reminder,0) AS exclude_from_reminder
     FROM users ORDER BY name ASC`
  );

  // Load holidays for today's off-check
  const holidaysSet = await loadHolidaysSet();
  const isHolidayToday = holidaysSet.has(today);

  // Filter out CXO + manually-excluded users + users whose today is week-off/holiday
  const eligible = users.filter(u =>
    !EXCLUDED_DEPARTMENTS.some(d => (u.department || '').toLowerCase() === d.toLowerCase()) &&
    !u.exclude_from_reminder &&
    !isUserOffOn(u, today, holidaysSet)
  );

  // If today is a global holiday → skip reminder entirely
  if (isHolidayToday) {
    return { ok: true, allDone: false, skipped: true, reason: 'Today is a holiday — reminder skipped', date: today };
  }

  if (!eligible.length) {
    return { ok: false, reason: 'No eligible users (everyone is CXO or no users)' };
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
    const sendRes = await sendWhatsAppRaw(REMINDER_GROUP_ID, allDoneMsg);
    return { ok: true, allDone: true, missingCount: 0, send: sendRes, date: today };
  }

  // Build the reminder message
  let message = "Hello,\n\n";
  message += "Today's Daily task report is not filled by :-\n\n";
  message += missingNames.join("\n");
  message += "\n\nPlease update today's report.";

  const sendRes = await sendWhatsAppRaw(REMINDER_GROUP_ID, message);
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

// ── Preview (admin) — see who would get reminded without actually sending ──
app.get('/api/daily-reminder/preview', requireAuth, requireAdmin, async (req, res) => {
  try {
    const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
    const today = istNow.toISOString().split('T')[0];

    const [users] = await db.query(
      `SELECT id, name, email, COALESCE(department,'') AS department,
              COALESCE(exclude_from_reminder,0) AS exclude_from_reminder
       FROM users ORDER BY name ASC`
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
// (delegation + checklist) due today or earlier.
// ══════════════════════════════════════════════════════
async function buildAndSendPendingTasksReminder() {
  // Today's date in IST
  const istNow = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
  const today = istNow.toISOString().split('T')[0];

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
  const holidaysSet = await loadHolidaysSet();
  const isHolidayToday = holidaysSet.has(today);
  if (isHolidayToday) {
    return { ok: true, sent: 0, skipped: userIds.length, total: userIds.length, reason: 'Today is a holiday — pending reminder skipped' };
  }

  let sent = 0, skipped = 0;
  const skippedDetails = [];

  for (const uid of userIds) {
    const u = userMap[uid];
    if (!u) { skipped++; skippedDetails.push({ id: uid, reason: 'user not found' }); continue; }
    if (u.exclude_from_reminder) { skipped++; skippedDetails.push({ name: u.name, reason: 'manually excluded' }); continue; }
    if (!u.phone) { skipped++; skippedDetails.push({ name: u.name, reason: 'no phone' }); continue; }
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
  if (process.env.CRON_SECRET && authHeader !== expected) {
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

// ── Cron endpoint (called by Vercel Cron at 6:10 PM IST = 12:40 PM UTC) ──
// Protected by CRON_SECRET so random visitors can't trigger it.
app.get('/api/cron/daily-reminder', async (req, res) => {
  // Vercel Cron sends header: authorization: Bearer <CRON_SECRET>
  const authHeader = req.headers['authorization'] || '';
  const expected = `Bearer ${process.env.CRON_SECRET || 'change_me_to_random_secret'}`;
  if (process.env.CRON_SECRET && authHeader !== expected) {
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

// ══════════════════════════════════════════════════════
// CLIENTS — admin manages, everyone reads
// ══════════════════════════════════════════════════════
app.get('/api/clients', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query('SELECT id, name FROM clients ORDER BY name ASC');
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/clients', requireAuth, requireAdmin, async (req, res) => {
  try {
    const name = (req.body.name || '').trim();
    if (!name) return res.status(400).json({ error: 'Client name required' });
    await db.query('INSERT INTO clients (name) VALUES (?)', [name]);
    res.json({ success: true });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'Client already exists' });
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/clients/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    await db.query('DELETE FROM clients WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk add clients via CSV
app.post('/api/clients/bulk', requireAuth, requireAdmin, async (req, res) => {
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
    res.json(rows.map(r => r.department));
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
// COMPLIANCE — Last 7 days grid (admin only)
// ══════════════════════════════════════════════════════
app.get('/api/compliance/last7', requireAuth, requireAdmin, async (req, res) => {
  try {
    // Last 7 days inclusive of today
    const dates = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      dates.push(d.toISOString().split('T')[0]);
    }

    // All users with week_off / extra_off so we can mark off-days
    const [users] = await db.query(
      `SELECT id, name, email, role, department,
              COALESCE(week_off,'') AS week_off,
              COALESCE(extra_off,'') AS extra_off
       FROM users
       WHERE role IN ('admin','hod','pc','user')
       ORDER BY name ASC`
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
        isHoliday: holidaysSet.has(d)
      }))
    }));

    res.json({ dates, users: grid, holidays: [...holidaysSet] });
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
    // Default to current month if not provided
    const now = new Date();
    const month = req.query.month || `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    if (!/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({ error: 'Invalid month format. Use YYYY-MM' });
    }
    const [year, mm] = month.split('-').map(Number);
    const fromDate = `${year}-${String(mm).padStart(2,'0')}-01`;
    // last day of month
    const lastDay = new Date(year, mm, 0).getDate();
    const toDate = `${year}-${String(mm).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;

    // All entries in this month
    const [rows] = await db.query(
      `SELECT dt.id, DATE_FORMAT(dt.entry_date,'%Y-%m-%d') AS entry_date,
              dt.client_name, dt.department, dt.description, dt.duration_min,
              dt.user_id, u.name AS doer_name, u.email AS doer_email,
              COALESCE(u.department, '') AS doer_department
       FROM daily_tasks dt
       JOIN users u ON dt.user_id = u.id
       WHERE dt.entry_date BETWEEN ? AND ?
       ORDER BY dt.entry_date ASC, u.name ASC, dt.id ASC`,
      [fromDate, toDate]
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
      month, from: fromDate, to: toDate,
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
  const [rows] = await db.query('SELECT id, role, department FROM users WHERE id=?', [userId]);
  const me = rows[0];
  if (!me) return null;
  if (me.role === 'admin') {
    // Admin's leave → another admin if available, else self (still needs explicit approval from Approvals tab)
    const [adm] = await db.query("SELECT id FROM users WHERE role='admin' AND id<>? ORDER BY id ASC LIMIT 1", [me.id]);
    if (adm[0]) return adm[0].id;
    return me.id;
  }
  if (me.role === 'hod' || me.role === 'pc') {
    const [adm] = await db.query("SELECT id FROM users WHERE role='admin' ORDER BY id ASC LIMIT 1");
    return adm[0]?.id || null;
  }
  // user → HOD of same department; fallback to admin
  if (me.department) {
    const [hods] = await db.query("SELECT id FROM users WHERE role='hod' AND department=? ORDER BY id ASC LIMIT 1", [me.department]);
    if (hods[0]) return hods[0].id;
  }
  const [adm] = await db.query("SELECT id FROM users WHERE role='admin' ORDER BY id ASC LIMIT 1");
  return adm[0]?.id || null;
}

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
      where += ' AND lr.approver_id=? AND lr.user_id<>?'; params.push(uid, uid);
    } else if (scope === 'team') {
      if (role === 'admin') {
        // no filter — all
      } else if (role === 'hod') {
        const [[me]] = await db.query('SELECT department FROM users WHERE id=?', [uid]);
        if (me?.department) {
          where += ' AND u.department=?'; params.push(me.department);
        } else {
          where += ' AND lr.user_id=?'; params.push(uid);
        }
      } else {
        where += ' AND lr.user_id=?'; params.push(uid);
      }
    }
    if (status) { where += ' AND lr.status=?'; params.push(status); }

    const [rows] = await db.query(`
      SELECT lr.id, lr.user_id, lr.leave_type, lr.status, lr.reason,
        lr.approver_id, lr.approver_note, lr.dates_json,
        DATE_FORMAT(lr.from_date,'%Y-%m-%d') AS from_date,
        DATE_FORMAT(lr.to_date,'%Y-%m-%d')   AS to_date,
        DATE_FORMAT(lr.created_at,'%Y-%m-%d %H:%i:%s') AS created_at,
        DATE_FORMAT(lr.decided_at,'%Y-%m-%d %H:%i:%s') AS decided_at,
        u.name AS user_name, u.email AS user_email, u.department AS user_department,
        ap.name AS approver_name
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
app.get('/api/leaves/pending-count', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const [[r]] = await db.query(
      "SELECT COUNT(*) AS cnt FROM leave_requests WHERE approver_id=? AND status='pending' AND user_id<>?",
      [uid, uid]
    );
    res.json({ count: r.cnt || 0 });
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
              <p><b>${me?.name || 'An employee'}</b> ne ek leave request submit ki hai aapke approval ke liye.</p>
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
      // WhatsApp to approver
      try {
        const [[apRow]] = await db.query('SELECT name, phone FROM users WHERE id=? LIMIT 1', [approverId]);
        if (apRow && apRow.phone) {
          const daysWord = cleanDates.length === 1 ? '1 day' : `${cleanDates.length} days`;
          const datesPretty = cleanDates.map(d => {
            const dd = d.date.split('-').reverse().join('-');
            return leave_type === 'extra_working' ? `${dd} (${d.hours}h)` : dd;
          }).join(', ');
          const msg = `Hello ${apRow.name || ''},\n\n🗓 *New Leave Request*\n\n` +
            `*Employee:* ${me?.name || ''}\n` +
            `*Type:* ${typeLabel}\n` +
            `*Duration:* ${daysWord}\n` +
            `*Dates:* ${datesPretty}\n` +
            `*Reason:* ${reason}\n\n` +
            `Please approve / reject from the Approvals tab.\n\n— E-Marketing Task Manager`;
          sendWhatsApp(apRow.phone, msg).catch(e => console.error('WA leave req err:', e.message));
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
    if (lr.approver_id !== uid && role !== 'admin') {
      return res.status(403).json({ error: 'Not authorized to act on this request' });
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
            <p>Aapki leave request <b>${typeLabel}</b> (${datesLine}) ko <b style="color:${color}">${newStatus}</b> kar diya gaya hai.</p>
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
        const [[apRow]] = await db.query('SELECT name FROM users WHERE id=? LIMIT 1', [uid]);
        const msg = `Hello ${reqRow.name || ''},\n\n${statusIcon} *Leave ${statusWord}*\n\n` +
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

// Returns the user's normalized off-day descriptors:
//   weekOff: array of weekday numbers (0=Sun..6=Sat)
//   extraOff: array of {day:0-6, weeks:[1..5]} (e.g. 1st & 3rd Saturday)
function _parseUserOff(user) {
  const weekOff = (user.week_off || '').split(',').map(s=>parseInt(s.trim(),10)).filter(n=>!isNaN(n));
  let extraOff = [];
  try { extraOff = user.extra_off ? JSON.parse(user.extra_off) : []; } catch { extraOff = []; }
  return { weekOff, extraOff };
}

// dateStr = 'YYYY-MM-DD'; holidaysSet = Set of YYYY-MM-DD strings
function isUserOffOn(user, dateStr, holidaysSet) {
  const ds = _toDateStr(dateStr);
  if (holidaysSet && holidaysSet.has(ds)) return true;
  if (!user) return false;
  const { weekOff, extraOff } = _parseUserOff(user);
  const d = new Date(ds + 'T00:00:00');
  const day = d.getDay();
  if (weekOff.includes(day)) return true;
  const nth = Math.ceil(d.getDate() / 7);
  if (extraOff.some(e => e.day === day && Array.isArray(e.weeks) && e.weeks.includes(nth))) return true;
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
    const cascade = await cascadeHolidayDate(date);
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
        const c = await cascadeHolidayDate(date);
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
    await db.query('DELETE FROM holidays WHERE id=?', [parseInt(req.params.id, 10)]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// On holiday add: delete checklist tasks on that date + push delegation tasks
async function cascadeHolidayDate(dateStr) {
  let deletedChecklist = 0, pushedDelegation = 0;
  try {
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

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
// Auth check is handled client-side via /api/me in init() — removing server-side
// requireAuth here prevents app.html from loading if cookie has any timing/domain issue
app.get('/app', (req, res) => res.sendFile(path.join(__dirname, 'public', 'app.html')));

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