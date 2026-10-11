// Compose Email (admin only). An admin writes an email in the app — To and CC
// from the users list or any typed address, formatted text, attachments up to
// 4 MB in total — and it goes out from the app's mailbox under the admin's
// name, with the admin's own address as Reply-To so answers reach them.
//
// Every send is written to mail_log: who sent it, to whom, CC, subject, how
// many files, when, and whether it went. All admins see that list; the body is
// not kept. There is deliberately no route that edits or deletes a log row.
const multer = require('multer');

const MAX_TOTAL_BYTES = 4 * 1024 * 1024;   // all attachments together
const MAX_FILES = 10;
const MAX_RECIPIENTS = 100;                // To + CC (room for "All users" plus a few more)
const EMAIL_RE = /^[^\s@<>,;"']+@[^\s@<>,;"']+\.[^\s@<>,;"']+$/;

module.exports = function registerComposeMailRoutes(app, deps) {
  const { db, requireAuth, requireAdmin, sendMail } = deps;
  const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_TOTAL_BYTES, files: MAX_FILES } });

  let _logTable = null;
  function ensureMailLog() {
    if (!_logTable) _logTable = db.query(`CREATE TABLE IF NOT EXISTS mail_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sent_by INT NOT NULL,
        sent_by_name VARCHAR(255) NOT NULL DEFAULT '',
        to_list TEXT NOT NULL,
        cc_list TEXT,
        subject VARCHAR(500) NOT NULL DEFAULT '',
        attachment_count INT NOT NULL DEFAULT 0,
        status VARCHAR(10) NOT NULL DEFAULT 'sent',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_mail_log_created (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`).catch(e => { _logTable = null; throw e; });
    return _logTable;
  }

  // A JSON array of addresses (or a comma list), trimmed and de-duplicated.
  function parseList(raw) {
    let list = [];
    try { list = JSON.parse(raw || '[]'); } catch (e) { list = String(raw || '').split(','); }
    if (!Array.isArray(list)) list = [];
    const seen = new Set(), out = [];
    for (const a of list) {
      const e = String(a || '').trim();
      if (e && !seen.has(e.toLowerCase())) { seen.add(e.toLowerCase()); out.push(e); }
    }
    return out;
  }

  // The body comes from the page's editor. It is never shown inside the app,
  // only mailed; still, anything that runs or loads is taken out first:
  // scripts, frames, forms, event handlers and javascript:/data: links.
  function cleanHtml(html) {
    return String(html || '')
      .replace(/<script\b[\s\S]*?<\/script\s*>/gi, '')
      .replace(/<style\b[\s\S]*?<\/style\s*>/gi, '')
      .replace(/<\/?(script|style|iframe|frame|object|embed|form|input|button|textarea|select|meta|link|base)\b[^>]*>/gi, '')
      .replace(/\son[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, '')
      .replace(/(href|src)\s*=\s*(["'])\s*(javascript|data|vbscript):[^"']*\2/gi, '$1="#"');
  }

  // multer's own errors (a file over 4 MB, more than 10 files) as plain 400s.
  function receiveFiles(req, res, next) {
    upload.array('files', MAX_FILES)(req, res, err => {
      if (!err) return next();
      if (err.code === 'LIMIT_FILE_SIZE') return res.status(400).json({ error: 'Attachments can be 4 MB in total.' });
      if (err.code === 'LIMIT_FILE_COUNT') return res.status(400).json({ error: `Attach at most ${MAX_FILES} files.` });
      return res.status(400).json({ error: err.message });
    });
  }

  app.post('/api/compose-mail/send', requireAuth, requireAdmin, receiveFiles, async (req, res) => {
    try {
      const to = parseList(req.body.to);
      // An address already in To is not copied again in CC.
      const inTo = new Set(to.map(e => e.toLowerCase()));
      const cc = parseList(req.body.cc).filter(e => !inTo.has(e.toLowerCase()));
      const subject = String(req.body.subject || '').replace(/[\r\n]+/g, ' ').trim().slice(0, 300);
      if (!to.length) return res.status(400).json({ error: 'Add at least one address in To.' });
      if (to.length + cc.length > MAX_RECIPIENTS) return res.status(400).json({ error: `At most ${MAX_RECIPIENTS} addresses in To and CC together.` });
      const bad = [...to, ...cc].filter(e => !EMAIL_RE.test(e));
      if (bad.length) return res.status(400).json({ error: `Not a valid email address: ${bad.join(', ')}` });
      if (!subject) return res.status(400).json({ error: 'Add a subject.' });
      const files = req.files || [];
      if (files.reduce((n, f) => n + f.size, 0) > MAX_TOTAL_BYTES) return res.status(400).json({ error: 'Attachments can be 4 MB in total.' });
      const html = cleanHtml(req.body.html);
      const hasText = html.replace(/<[^>]*>|&nbsp;|\s/g, '').length > 0;
      if (!hasText && !files.length) return res.status(400).json({ error: 'Write the message.' });

      const [[me]] = await db.query('SELECT name, email, notification_email FROM users WHERE id=?', [req.session.userId]);
      const name = (me && me.name) || 'E-Marketing';
      const replyTo = me && (me.notification_email || me.email);
      const ok = await sendMail(to.join(', '), subject,
        `<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111;line-height:1.6">${html}</div>`, {
          cc: cc.length ? cc.join(', ') : undefined,
          replyTo: replyTo || undefined,
          fromName: `${name} (E-Marketing)`,
          attachments: files.map(f => ({ filename: f.originalname, content: f.buffer, contentType: f.mimetype })),
        });

      // Logged whether or not it went, so a failed attempt is visible too.
      await ensureMailLog();
      await db.query(
        'INSERT INTO mail_log (sent_by, sent_by_name, to_list, cc_list, subject, attachment_count, status) VALUES (?,?,?,?,?,?,?)',
        [req.session.userId, name, JSON.stringify(to), JSON.stringify(cc), subject, files.length, ok ? 'sent' : 'failed']);
      if (!ok) return res.status(502).json({ error: 'The email could not be sent. Please try again.' });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Newest first, for every admin: who sent what (subject only) to whom, when.
  app.get('/api/compose-mail/log', requireAuth, requireAdmin, async (req, res) => {
    try {
      await ensureMailLog();
      const [rows] = await db.query(
        `SELECT id, sent_by, sent_by_name, to_list, cc_list, subject, attachment_count, status,
                DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') AS sent_at
           FROM mail_log ORDER BY id DESC LIMIT 300`);
      const list = s => { try { const a = JSON.parse(s || '[]'); return Array.isArray(a) ? a : []; } catch (e) { return []; } };
      res.json(rows.map(r => ({ ...r, to_list: list(r.to_list), cc_list: list(r.cc_list) })));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });
};
