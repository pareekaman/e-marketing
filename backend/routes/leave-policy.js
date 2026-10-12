// Leave & WFH Policy acknowledgement. Every team member signs the policy once
// (ticks the box, their name is filled from their account, and they upload an
// image of their signature). It does not block leave or WFH requests. After
// signing, the policy stays on top of the Leave Tracker as a notice. Admins see who has
// signed, when, with the signature, and who has not.
//
// POLICY_VERSION goes up when the policy text changes; everyone then signs
// again, because a signature belongs to one version. The text itself lives in
// public/js/leave-policy.js.
const POLICY_VERSION = 1;
const MAX_SIGNATURE_BYTES = 2.5 * 1024 * 1024;
const SIGNATURE_RE = /^data:image\/(png|jpeg);base64,([A-Za-z0-9+/=]+)$/;

module.exports = function registerLeavePolicyRoutes(app, deps) {
  const { db, requireAuth, requireAdmin } = deps;

  let _table = null;
  function ensureTable() {
    if (!_table) _table = db.query(`CREATE TABLE IF NOT EXISTS policy_acknowledgements (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        policy_version INT NOT NULL,
        signed_name VARCHAR(255) NOT NULL,
        signature MEDIUMTEXT NOT NULL,
        signed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_policy_ack (user_id, policy_version)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`).catch(e => { _table = null; throw e; });
    return _table;
  }

  // Mine: signed or not, and when (the signature too, for the notice).
  app.get('/api/leave-policy/me', requireAuth, async (req, res) => {
    try {
      await ensureTable();
      const [[row]] = await db.query(
        `SELECT signed_name, signature, DATE_FORMAT(signed_at, '%d-%m-%Y %H:%i') AS signed_at
           FROM policy_acknowledgements WHERE user_id=? AND policy_version=? LIMIT 1`,
        [req.session.userId, POLICY_VERSION]);
      res.json({ version: POLICY_VERSION, signed: !!row, ...(row || {}) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.post('/api/leave-policy/sign', requireAuth, async (req, res) => {
    try {
      if (req.session.impersonatedBy) return res.status(403).json({ error: 'Sign from your own login, not while viewing as someone.' });
      if (req.body.agree !== true) return res.status(400).json({ error: 'Tick the box to agree to the policy.' });
      const m = SIGNATURE_RE.exec(String(req.body.signature || ''));
      if (!m) return res.status(400).json({ error: 'Upload your signature as a JPG or PNG image.' });
      if (Buffer.from(m[2], 'base64').length > MAX_SIGNATURE_BYTES) return res.status(400).json({ error: 'The signature image can be at most 2.5 MB.' });
      const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
      if (!me) return res.status(404).json({ error: 'User not found' });
      await ensureTable();
      // Signing twice keeps the first signature.
      await db.query(
        'INSERT IGNORE INTO policy_acknowledgements (user_id, policy_version, signed_name, signature) VALUES (?,?,?,?)',
        [req.session.userId, POLICY_VERSION, me.name, req.body.signature]);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Admin: every team member (no client logins), signed or not.
  app.get('/api/leave-policy/report', requireAuth, requireAdmin, async (req, res) => {
    try {
      await ensureTable();
      const [rows] = await db.query(
        `SELECT u.id, u.name, u.department, a.signed_name, a.signature,
                DATE_FORMAT(a.signed_at, '%d-%m-%Y %H:%i') AS signed_at
           FROM users u
           LEFT JOIN policy_acknowledgements a ON a.user_id = u.id AND a.policy_version = ?
          WHERE u.role <> 'client'
          ORDER BY a.signed_at IS NULL, a.signed_at DESC, u.name`, [POLICY_VERSION]);
      res.json({ version: POLICY_VERSION, rows });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

};
