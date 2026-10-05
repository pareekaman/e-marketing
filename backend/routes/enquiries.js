// ══════════════════════════════════════════════════════
// ENQUIRY CAPTURE — the sales enquiry form, inside the app
// ══════════════════════════════════════════════════════
// The same fields as the "Enquiry Capture - eMarketing" Google Form, saved in
// the enquiries table. Access Control row "Enquiry Capture" (page 'enquiry'):
// View lists them, Editor (edit_enquiry) adds and edits, Admin (admin_enquiry)
// also deletes. It is in no role's defaults, so admins see it and everyone
// else only once granted.
module.exports = function registerEnquiryRoutes(app, deps) {
  const { db, requireAuth, userCanSee, userCanDo, archiveDeleted } = deps;

  // The Google Form's own choices, spelled exactly as the form has them so the
  // values written to its responses sheet match the ones the form writes.
  const LEAD_HANDLERS = ['Abhishek Jain', 'Simran Gurnani', 'Chetna Agrawal'];
  const PROJECT_TYPES = ['Lead Generation', 'E-commerce'];
  const PLATFORMS = ['Google Ads', 'Landing Page', 'Linkedin Management', 'Meta Ads', 'SEO', 'SM Management',
    'Website Designing & Development', 'Whatsapp Marketing', 'Youtube Ads', 'GMB Ads', 'Sales Consutation',
    'Business Automation', 'AI', 'Book Writing', 'Lead Nuturing Funnel'];
  const DATE_FIELDS = ['meeting_scheduled_date', 'meeting_done_date', 'proposal_date', 'conversion_date'];

  let _table = null;
  function ensureTable() {
    if (!_table) _table = db.query(`CREATE TABLE IF NOT EXISTS enquiries (
        id INT AUTO_INCREMENT PRIMARY KEY,
        client_name VARCHAR(255) NOT NULL,
        business_name VARCHAR(500) NOT NULL,
        mobile VARCHAR(30) DEFAULT NULL,
        lead_handle_by VARCHAR(100) DEFAULT NULL,
        project_types VARCHAR(255) DEFAULT NULL,
        platforms VARCHAR(1000) DEFAULT NULL,
        meeting_scheduled_date DATE DEFAULT NULL,
        meeting_done_date DATE DEFAULT NULL,
        meeting_url VARCHAR(1000) DEFAULT NULL,
        proposal_date DATE DEFAULT NULL,
        proposal_url VARCHAR(1000) DEFAULT NULL,
        conversion_date DATE DEFAULT NULL,
        order_value VARCHAR(100) DEFAULT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'Open',
        client_id INT DEFAULT NULL,
        sheet_row INT DEFAULT NULL,
        source VARCHAR(10) NOT NULL DEFAULT 'app',
        created_by INT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by INT DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`).catch(e => { _table = null; throw e; });
    return _table;
  }

  const clean = (v, max) => String(v == null ? '' : v).trim().slice(0, max);
  const pick = (list, v) => [...new Set((Array.isArray(v) ? v : []).map(String))].filter(x => list.includes(x));
  // Blank stays blank; anything else must be a web link (it is rendered as one).
  const link = v => { const s = clean(v, 1000); return !s || /^https?:\/\/\S+$/i.test(s) ? s : null; };

  // Validated row from a request body, or { error }.
  function readBody(b) {
    const e = {
      client_name: clean(b.client_name, 255),
      business_name: clean(b.business_name, 500),
      mobile: clean(b.mobile, 30).replace(/[^\d+\s-]/g, ''),
      lead_handle_by: LEAD_HANDLERS.includes(b.lead_handle_by) ? b.lead_handle_by : '',
      project_types: pick(PROJECT_TYPES, b.project_types).join('||'),
      platforms: pick(PLATFORMS, b.platforms).join('||'),
      meeting_url: link(b.meeting_url),
      proposal_url: link(b.proposal_url),
      order_value: clean(b.order_value, 100),
    };
    if (!e.client_name) return { error: 'Client name required' };
    if (!e.business_name) return { error: 'Business name required' };
    if (b.lead_handle_by && !e.lead_handle_by) return { error: 'Pick Lead Handle By from the list' };
    if (e.meeting_url === null || e.proposal_url === null) return { error: 'Links must start with http:// or https://' };
    for (const k of DATE_FIELDS) {
      const v = clean(b[k], 10);
      if (v && !/^\d{4}-\d{2}-\d{2}$/.test(v)) return { error: 'Enter dates as valid dates' };
      e[k] = v || null;
    }
    for (const k of Object.keys(e)) if (e[k] === '') e[k] = null;
    return { e };
  }

  const LIST_SQL = `SELECT e.id, e.client_name, e.business_name, e.mobile, e.lead_handle_by, e.project_types, e.platforms,
      DATE_FORMAT(e.meeting_scheduled_date, '%Y-%m-%d') AS meeting_scheduled_date,
      DATE_FORMAT(e.meeting_done_date, '%Y-%m-%d') AS meeting_done_date, e.meeting_url,
      DATE_FORMAT(e.proposal_date, '%Y-%m-%d') AS proposal_date, e.proposal_url,
      DATE_FORMAT(e.conversion_date, '%Y-%m-%d') AS conversion_date, e.order_value,
      e.status, e.client_id, e.source, DATE_FORMAT(e.created_at, '%Y-%m-%d %H:%i') AS created_at,
      u.name AS created_by_name
    FROM enquiries e LEFT JOIN users u ON u.id = e.created_by`;

  app.get('/api/enquiries/options', requireAuth, (req, res) => {
    res.json({ leadHandlers: LEAD_HANDLERS, projectTypes: PROJECT_TYPES, platforms: PLATFORMS });
  });

  app.get('/api/enquiries', requireAuth, async (req, res) => {
    try {
      if (!(await userCanSee(req.session, 'enquiry'))) return res.status(403).json({ error: 'You do not have access to Enquiry Capture' });
      await ensureTable();
      const [rows] = await db.query(`${LIST_SQL} ORDER BY e.created_at DESC, e.id DESC`);
      res.json(rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.post('/api/enquiries', requireAuth, async (req, res) => {
    try {
      if (!(await userCanDo(req.session, 'edit_enquiry'))) return res.status(403).json({ error: 'You do not have edit access to Enquiry Capture' });
      const { e, error } = readBody(req.body || {});
      if (error) return res.status(400).json({ error });
      await ensureTable();
      const cols = Object.keys(e);
      const [r] = await db.query(
        `INSERT INTO enquiries (${cols.join(', ')}, created_by) VALUES (${cols.map(() => '?').join(', ')}, ?)`,
        [...cols.map(k => e[k]), req.session.userId]);
      res.json({ success: true, id: r.insertId });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.delete('/api/enquiries/:id', requireAuth, async (req, res) => {
    try {
      if (!(await userCanDo(req.session, 'admin_enquiry'))) return res.status(403).json({ error: 'Only the Admin level can delete enquiries' });
      await ensureTable();
      const [doomed] = await db.query('SELECT * FROM enquiries WHERE id=?', [req.params.id]);
      if (!doomed.length) return res.status(404).json({ error: 'Enquiry not found' });
      await archiveDeleted('enquiries', doomed, req, { summary: r => `Enquiry: ${r.client_name || ''} (${r.business_name || ''})` });
      await db.query('DELETE FROM enquiries WHERE id=?', [req.params.id]);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });
};
