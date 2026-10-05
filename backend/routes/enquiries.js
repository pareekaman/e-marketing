// ══════════════════════════════════════════════════════
// ENQUIRY CAPTURE — the sales enquiry form, inside the app
// ══════════════════════════════════════════════════════
// The same fields as the "Enquiry Capture - eMarketing" Google Form, saved in
// the enquiries table. Access Control row "Enquiry Capture" (page 'enquiry'):
// View lists them, Editor (edit_enquiry) adds and edits, Admin (admin_enquiry)
// also deletes. It is in no role's defaults, so admins see it and everyone
// else only once granted.
module.exports = function registerEnquiryRoutes(app, deps) {
  const { db, requireAuth, userCanSee, userCanDo, archiveDeleted, getSheetsClient } = deps;

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
        sheet_stamp DOUBLE DEFAULT NULL,
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

  // ── The Google Form's responses sheet ─────────────────────────────────────
  // A new enquiry becomes a row of "Form responses 1", laid out the way the
  // form writes it (A timestamp … O brand name), so the Onboarding FMS and
  // anything else reading that sheet carries on. An edit rewrites B-K and M-O
  // of the same row; A (timestamp), L (the email of whoever added it), P
  // (Billing Name), Q (the form's edit link) and R (Status) are left alone.
  //
  // Two things about this sheet, found on the copy: values.append always lands
  // on the row right after the form's own responses, so a second enquiry
  // overwrote the first; and a separate repeatCell format is ignored, so dates
  // read as 46303. So rows go in with appendCells (after the last row holding
  // data, atomic on Google's side) and edits with updateCells, each cell
  // carrying its value and its date format together. Values are typed
  // (numberValue / stringValue), so text starting with "=" is never a formula.
  //
  // A row is found again by its timestamp in A: the form can insert response
  // rows above it, and people sort the sheet. Points at the user's TEST COPY
  // until ENQUIRY_SHEET_ID names the real sheet
  // (1mg9lXGz__n6DH4jVoc7M4l4t3imK7nkh0Vm7jq3aSiE). A failure never blocks the
  // save; it comes back as a warning.
  const ENQUIRY_SHEET_ID = process.env.ENQUIRY_SHEET_ID || '1rGj2WaqnsQIVU6CeV2KdlWARX_HsyiYQV4h_HM0Bx1Y';
  const ENQUIRY_SHEET_TAB = process.env.ENQUIRY_SHEET_TAB || 'Form responses 1';
  const sheetTab = () => `'${ENQUIRY_SHEET_TAB.replace(/'/g, "''")}'`;
  const serialNow = () => (Date.now() + 5.5 * 3600 * 1000) / 86400000 + 25569;   // IST, as the form stamps it
  const serialDate = ymd => {
    if (!ymd) return '';
    const [y, m, d] = String(ymd).split('-').map(Number);
    return Date.UTC(y, m - 1, d) / 86400000 + 25569;
  };
  // Mobile and order value go in as numbers when they are plain numbers, the
  // way the form stores them; anything else ("45,000 + GST") stays text.
  const asNumber = v => {
    const s = String(v || '').replace(/[\s,]/g, '');
    return /^\d+(\.\d+)?$/.test(s) && s.length <= 15 ? Number(s) : (v || '');
  };
  const joinList = v => String(v || '').split('||').filter(Boolean).join(', ');
  const STAMP_FMT = { type: 'DATE_TIME', pattern: 'dd/mm/yyyy hh:mm:ss' };
  const DATE_FMT = { type: 'DATE', pattern: 'dd/mm/yyyy' };
  const cell = (v, fmt) => {
    const c = {};
    if (typeof v === 'number') c.userEnteredValue = { numberValue: v };
    else if (v !== '' && v != null) c.userEnteredValue = { stringValue: String(v) };
    if (fmt) c.userEnteredFormat = { numberFormat: fmt };
    return c;
  };
  const cellsBtoK = e => [cell(e.client_name), cell(asNumber(e.mobile)),
    cell(serialDate(e.meeting_scheduled_date), DATE_FMT), cell(serialDate(e.meeting_done_date), DATE_FMT),
    cell(e.meeting_url), cell(serialDate(e.proposal_date), DATE_FMT), cell(e.proposal_url),
    cell(serialDate(e.conversion_date), DATE_FMT), cell(asNumber(e.order_value)), cell(joinList(e.platforms))];
  const cellsMtoO = e => [cell(e.lead_handle_by), cell(joinList(e.project_types)), cell(e.business_name)];
  const CELL_FIELDS = 'userEnteredValue,userEnteredFormat.numberFormat';
  const sameStamp = (a, b) => typeof a === 'number' && Math.abs(a - b) <= 2 / 86400;

  let _tabSheetId = null;
  async function tabSheetId(sheets) {
    if (_tabSheetId === null) {
      const meta = await sheets.spreadsheets.get({ spreadsheetId: ENQUIRY_SHEET_ID, fields: 'sheets.properties(sheetId,title)' });
      const t = (meta.data.sheets || []).find(s => s.properties.title === ENQUIRY_SHEET_TAB);
      if (!t) throw new Error(`no "${ENQUIRY_SHEET_TAB}" tab in the Enquiry Capture sheet`);
      _tabSheetId = t.properties.sheetId;
    }
    return _tabSheetId;
  }
  // The row whose A holds this timestamp: the saved row number first, then a
  // search from the bottom. null when it is not there.
  async function findRow(sheets, row, stamp) {
    if (row) {
      const a = await sheets.spreadsheets.values.get({ spreadsheetId: ENQUIRY_SHEET_ID, range: `${sheetTab()}!A${row}`, valueRenderOption: 'UNFORMATTED_VALUE' });
      if (sameStamp(a.data.values && a.data.values[0] && a.data.values[0][0], stamp)) return row;
    }
    const col = await sheets.spreadsheets.values.get({ spreadsheetId: ENQUIRY_SHEET_ID, range: `${sheetTab()}!A:A`, valueRenderOption: 'UNFORMATTED_VALUE' });
    const vals = col.data.values || [];
    for (let i = vals.length - 1; i >= 0; i--) if (sameStamp(vals[i] && vals[i][0], stamp)) return i + 1;
    return null;
  }

  // Appends the enquiry; returns the sheet row it landed on.
  async function sheetAppend(e, email, stamp) {
    const sheets = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets']);
    const sheetId = await tabSheetId(sheets);
    await sheets.spreadsheets.batchUpdate({ spreadsheetId: ENQUIRY_SHEET_ID, requestBody: { requests: [{ appendCells: {
      sheetId, fields: CELL_FIELDS,
      rows: [{ values: [cell(stamp, STAMP_FMT), ...cellsBtoK(e), cell(email), ...cellsMtoO(e)] }],
    } }] } });
    return findRow(sheets, null, stamp);
  }
  // Rewrites B-K and M-O of the enquiry's row. Returns the row, or null when
  // the row is gone.
  async function sheetUpdate(row, stamp, e) {
    const sheets = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets']);
    const sheetId = await tabSheetId(sheets);
    const at = await findRow(sheets, row, stamp);
    if (!at) return null;
    const range = (c0, c1) => ({ sheetId, startRowIndex: at - 1, endRowIndex: at, startColumnIndex: c0, endColumnIndex: c1 });
    await sheets.spreadsheets.batchUpdate({ spreadsheetId: ENQUIRY_SHEET_ID, requestBody: { requests: [
      { updateCells: { range: range(1, 11), rows: [{ values: cellsBtoK(e) }], fields: CELL_FIELDS } },
      { updateCells: { range: range(12, 15), rows: [{ values: cellsMtoO(e) }], fields: CELL_FIELDS } },
    ] } });
    return at;
  }
  // Puts one saved enquiry into the sheet: updates its row, or appends it when
  // it never got there. Returns a warning string, or null when all went well.
  async function syncToSheet(id, e, isNew) {
    try {
      const [[row]] = await db.query(
        'SELECT q.sheet_row, q.sheet_stamp, u.email FROM enquiries q LEFT JOIN users u ON u.id = q.created_by WHERE q.id=?', [id]);
      if (!row) return null;
      if (row.sheet_stamp) {
        const at = await sheetUpdate(row.sheet_row, row.sheet_stamp, e);
        if (!at) return 'Saved, but its row was not found in the Enquiry Capture sheet';
        if (at !== row.sheet_row) await db.query('UPDATE enquiries SET sheet_row=? WHERE id=?', [at, id]);
        return null;
      }
      const stamp = serialNow();
      const at = await sheetAppend(e, row.email, stamp);
      await db.query('UPDATE enquiries SET sheet_row=?, sheet_stamp=? WHERE id=?', [at, stamp, id]);
      return null;
    } catch (err) {
      console.error('Enquiry sheet sync failed:', err.message);
      return `Saved, but the Enquiry Capture sheet could not be ${isNew ? 'given the new row' : 'updated'}: ${err.message}`;
    }
  }

  const LIST_SQL = `SELECT e.id, e.client_name, e.business_name, e.mobile, e.lead_handle_by, e.project_types, e.platforms,
      DATE_FORMAT(e.meeting_scheduled_date, '%Y-%m-%d') AS meeting_scheduled_date,
      DATE_FORMAT(e.meeting_done_date, '%Y-%m-%d') AS meeting_done_date, e.meeting_url,
      DATE_FORMAT(e.proposal_date, '%Y-%m-%d') AS proposal_date, e.proposal_url,
      DATE_FORMAT(e.conversion_date, '%Y-%m-%d') AS conversion_date, e.order_value,
      e.status, e.client_id, e.source, DATE_FORMAT(e.created_at, '%Y-%m-%d %H:%i') AS created_at,
      u.name AS created_by_name, DATE_FORMAT(e.updated_at, '%Y-%m-%d %H:%i') AS updated_at, u2.name AS updated_by_name
    FROM enquiries e LEFT JOIN users u ON u.id = e.created_by LEFT JOIN users u2 ON u2.id = e.updated_by`;

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
      const warning = await syncToSheet(r.insertId, e, true);
      res.json({ success: true, id: r.insertId, ...(warning ? { warning } : {}) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Edit: the form sends every field, so the row is replaced as a whole. This
  // is how the later stages (meeting done, proposal, conversion) get filled in.
  app.put('/api/enquiries/:id', requireAuth, async (req, res) => {
    try {
      if (!(await userCanDo(req.session, 'edit_enquiry'))) return res.status(403).json({ error: 'You do not have edit access to Enquiry Capture' });
      const { e, error } = readBody(req.body || {});
      if (error) return res.status(400).json({ error });
      await ensureTable();
      const cols = Object.keys(e);
      const [r] = await db.query(
        `UPDATE enquiries SET ${cols.map(k => `${k}=?`).join(', ')}, updated_by=?, updated_at=NOW() WHERE id=?`,
        [...cols.map(k => e[k]), req.session.userId, req.params.id]);
      if (!r.affectedRows) return res.status(404).json({ error: 'Enquiry not found' });
      const warning = await syncToSheet(Number(req.params.id), e, false);
      res.json({ success: true, ...(warning ? { warning } : {}) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Deleting removes the enquiry from the app only. Its sheet row stays: the
  // FMS and other sheets read that sheet by position and by its contents.
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
