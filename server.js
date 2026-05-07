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
  connectionLimit: 5,
  queueLimit: 0,
  connectTimeout: 30000,
  // SSL support for cloud MySQL providers (Aiven, PlanetScale, Railway, etc.)
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
};

const db = mysql.createPool(dbConfig);

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

  // ── Column additions (safe ALTERs from previous versions) ─────
  await sa(`ALTER TABLE fms_sheets ADD COLUMN fms_name VARCHAR(255) DEFAULT '' AFTER id`);
  await sa(`ALTER TABLE fms_steps ADD COLUMN show_cols TEXT DEFAULT '' AFTER extra_col`);
  await sa(`ALTER TABLE fms_steps ADD COLUMN delay_reason_col VARCHAR(10) DEFAULT '' AFTER show_cols`);
  await sa(`ALTER TABLE fms_steps ADD COLUMN doer_name_col VARCHAR(10) DEFAULT '' AFTER delay_reason_col`);
  await sa(`ALTER TABLE users ADD COLUMN department VARCHAR(255) DEFAULT '' AFTER phone`);
  await sa(`ALTER TABLE users ADD COLUMN week_off VARCHAR(50) DEFAULT '' AFTER department`);
  await sa(`ALTER TABLE users ADD COLUMN extra_off TEXT DEFAULT '' AFTER week_off`);
  await sa(`ALTER TABLE users ADD COLUMN notification_email VARCHAR(255) DEFAULT '' AFTER email`);
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
    const { type, desc, assignedTo, approverEmail, date, priority, approval, remarks } = req.body;
    const isAdmin = req.session.role === 'admin';
    const isHod   = req.session.role === 'hod';
    const isUser  = req.session.role === 'user';
    // Admin, HOD and regular users can all assign to others; fallback to self if not specified
    const targetUser = (isAdmin || isHod || isUser) && assignedTo ? parseInt(assignedTo) : req.session.userId;
    if (!desc || !date) return res.status(400).json({ error: 'Description and date required' });
    if ((type||'checklist') === 'delegation') {
      // Approver: agar approverEmail diya hai to usse dhundo, warna logged-in user
      let assignedBy = req.session.userId;
      if (approverEmail) {
        const [aprRows] = await db.query('SELECT id FROM users WHERE email=? LIMIT 1', [approverEmail]);
        if (aprRows.length) assignedBy = aprRows[0].id;
      }
      await db.query(`INSERT INTO delegation_tasks (description,assigned_to,assigned_by,due_date,status,priority,approval,remarks) VALUES (?,?,?,?,?,?,?,?)`, [desc, targetUser, assignedBy, date, 'pending', priority||'low', approval||'no', remarks||'']);
      // 📧 Send delegation email (non-blocking — fire and forget)
      (async () => {
        const target = await getNotifyTarget(targetUser);
        if (!target) return;
        const [aprRows] = await db.query('SELECT name FROM users WHERE id=? LIMIT 1', [assignedBy]);
        const assignerName = aprRows[0]?.name || 'Admin';
        await sendMail(
          target.email,
          `📋 New Task Assigned: ${(desc||'').slice(0,60)}`,
          delegationEmailHtml({
            assigneeName: target.name,
            assignerName,
            desc, dueDate: date,
            priority: priority||'low',
            approval: approval||'no',
            remarks: remarks||''
          })
        );
      })();
    } else {
      await db.query(`INSERT INTO checklist_tasks (description,assigned_to,assigned_by,due_date,status,priority,remarks) VALUES (?,?,?,?,?,?,?)`, [desc, targetUser, req.session.userId, date, 'pending', priority||'low', remarks||'']);
    }
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/tasks/bulk-checklist', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { desc, assignedTo, priority, remarks, dates } = req.body;
    if (!desc || !assignedTo || !dates || !dates.length) return res.status(400).json({ error: 'Missing fields' });
    const values = dates.map(date => [desc, parseInt(assignedTo), req.session.userId, date, 'pending', priority||'low', remarks||'']);
    await db.query(`INSERT INTO checklist_tasks (description,assigned_to,assigned_by,due_date,status,priority,remarks) VALUES ?`, [values]);
    res.json({ success: true, count: dates.length });
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
    const [rows] = await db.query('SELECT id,name,email,notification_email,role,phone,department,week_off,extra_off FROM users ORDER BY role DESC,name ASC');
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/users', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { name, email, notification_email, password, role, phone, department, week_off, extra_off } = req.body;
    if (!name || !email || !password) return res.status(400).json({ error: 'All fields required' });
    const [ex] = await db.query('SELECT id FROM users WHERE email=?', [email]);
    if (ex[0]) return res.status(400).json({ error: 'Email already exists' });
    await db.query('INSERT INTO users (name,email,notification_email,password,role,phone,department,week_off,extra_off) VALUES (?,?,?,?,?,?,?,?,?)',
      [name, email, notification_email||'', bcrypt.hashSync(password,10), role||'user', phone||null, department||'', week_off||'', extra_off||'']);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/users/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { name, email, notification_email, role, password, phone, department, week_off, extra_off } = req.body;
    if (password) await db.query('UPDATE users SET name=?,email=?,notification_email=?,role=?,password=?,phone=?,department=?,week_off=?,extra_off=? WHERE id=?',
      [name,email,notification_email||'',role,bcrypt.hashSync(password,10),phone||null,department||'',week_off||'',extra_off||'',req.params.id]);
    else await db.query('UPDATE users SET name=?,email=?,notification_email=?,role=?,phone=?,department=?,week_off=?,extra_off=? WHERE id=?',
      [name,email,notification_email||'',role,phone||null,department||'',week_off||'',extra_off||'',req.params.id]);
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
    const [sheets] = await db.query('SELECT * FROM fms_sheets WHERE id=?', [req.params.fmsId]);
    if (!sheets[0]) return res.status(404).json({ error: 'FMS not found' });
    const sheet = sheets[0];
    const [steps] = await db.query('SELECT * FROM fms_steps WHERE id=? AND fms_id=?', [req.params.stepId, req.params.fmsId]);
    if (!steps[0]) return res.status(404).json({ error: 'Step not found' });
    const step = steps[0];

    const planIdx = colToIdx(step.plan_col);
    const actualIdx = colToIdx(step.actual_col);
    let showCols = [];
    try { showCols = JSON.parse(step.show_cols||'[]'); } catch(e) {}

    const sheetsApi = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const spreadsheetId = extractSpreadsheetId(sheet.sheet_id);
    const tabName = sheet.sheet_name || 'Sheet1';

    // Optimized: fetch only up to the furthest needed column
    const maxIdx = Math.max(planIdx, actualIdx, ...(showCols.length ? showCols : [0]));
    const lastCol = maxIdx >= 0 ? idxToCol(maxIdx) : 'Z';
    const range = `${tabName}!A:${lastCol}`;

    const response = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range });
    const allRows = response.data.values || [];
    const headerRowIdx = (sheet.header_row || 1) - 1;
    const headers = allRows[headerRowIdx] || [];
    const dataRows = allRows.slice(headerRowIdx + 1);

    const matchedRows = [];
    dataRows.forEach((row, i) => {
      const planVal = planIdx >= 0 ? (row[planIdx]||'').trim() : '';
      const actualVal = actualIdx >= 0 ? (row[actualIdx]||'').trim() : '';
      if (planVal && !actualVal) {
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
          data: rowData
        });
      }
    });

    res.json({ rows: matchedRows, headers, total: matchedRows.length });
  } catch (err) {
    if (err.code === 403) return res.status(400).json({ error: 'Access denied.' });
    if (err.code === 404) return res.status(400).json({ error: 'Sheet not found.' });
    res.status(500).json({ error: err.message });
  }
});

// Mark row as done — writes actual (date only) + delay reason to sheet
app.post('/api/fms-tasks/:fmsId/steps/:stepId/done', requireAuth, async (req, res) => {
  try {
    const { rowNumber, actualValue, delayReason, extraInputs } = req.body;
    if (!rowNumber || !actualValue) return res.status(400).json({ error: 'rowNumber and actualValue required' });
    // Strip time portion — save only date (DD-MM-YYYY) to Google Sheet
    let dateOnlyValue = actualValue;
    const dtMatch = actualValue.match(/^(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})/);
    if (dtMatch) dateOnlyValue = dtMatch[1];

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
      requestBody: { values: [[dateOnlyValue]] }
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