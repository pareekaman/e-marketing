// ══════════════════════════════════════════════════════
// ENQUIRY CAPTURE — the sales enquiry form, inside the app
// ══════════════════════════════════════════════════════
// The same fields as the "Enquiry Capture - eMarketing" Google Form, saved in
// the enquiries table. Access Control row "Enquiry Capture" (page 'enquiry'):
// View lists them, Editor (edit_enquiry) adds and edits, Admin (admin_enquiry)
// also deletes. It is in no role's defaults, so admins see it and everyone
// else only once granted. The status (Open / Win / Lost / Conversion) can be
// set by its Editors and by the CRMs (Client Master's crm_clients), who then
// add a converted enquiry to Client Master.
module.exports = function registerEnquiryRoutes(app, deps) {
  const { db, requireAuth, userCanSee, userCanDo, archiveDeleted, getSheetsClient } = deps;

  // The Google Form's own choices, spelled exactly as the form has them so the
  // values written to its responses sheet match the ones the form writes.
  // Yashi Jain was added in the app (2026-10-06, the user's request); the form
  // offers her only once someone adds her there too.
  const LEAD_HANDLERS = ['Abhishek Jain', 'Simran Gurnani', 'Chetna Agrawal', 'Yashi Jain'];
  const PROJECT_TYPES = ['Lead Generation', 'E-commerce'];
  const PLATFORMS = ['Google Ads', 'Landing Page', 'Linkedin Management', 'Meta Ads', 'SEO', 'SM Management',
    'Website Designing & Development', 'Whatsapp Marketing', 'Youtube Ads', 'GMB Ads', 'Sales Consutation',
    'Business Automation', 'AI', 'Book Writing', 'Lead Nuturing Funnel'];
  const DATE_FIELDS = ['meeting_scheduled_date', 'meeting_done_date', 'proposal_date', 'conversion_date'];
  // Win: the client has agreed but the work starts later. Conversion: the
  // work is about to start, and the enquiry can go into Client Master. The app
  // keeps the status; the sheet has no column for it.
  const STATUSES = ['Open', 'Win', 'Lost', 'Conversion'];

  let _table = null;
  function ensureTable() {
    if (!_table) _table = createTable().catch(e => { _table = null; throw e; });
    return _table;
  }
  async function createTable() {
    await db.query(`CREATE TABLE IF NOT EXISTS enquiries (
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
        updated_at TIMESTAMP NULL DEFAULT NULL,
        UNIQUE KEY uq_enquiry_sheet_stamp (sheet_stamp)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    // sheet_pending: the latest save has not reached the sheet yet, so the
    // next retry sends it (see /api/enquiries/sheet-retry). sheet_try_at: when
    // the last attempt started, so a failed one waits out the per-minute quota
    // and two retries never work on the same enquiry at once.
    try {
      await db.query('ALTER TABLE enquiries ADD COLUMN sheet_pending TINYINT NOT NULL DEFAULT 0, ADD COLUMN sheet_try_at DATETIME NULL DEFAULT NULL');
      // Enquiries saved before these columns whose row never got there.
      await db.query("UPDATE enquiries SET sheet_pending=1 WHERE source='app' AND sheet_stamp IS NULL");
    } catch (e) { if (e.code !== 'ER_DUP_FIELDNAME') throw e; }
  }

  const clean = (v, max) => String(v == null ? '' : v).trim().slice(0, max);
  const pick = (list, v) => [...new Set((Array.isArray(v) ? v : []).map(String))].filter(x => list.includes(x));
  // Blank stays blank; anything else must be a web link (it is rendered as one).
  const link = v => { const s = clean(v, 1000); return !s || /^https?:\/\/\S+$/i.test(s) ? s : null; };

  // Validated row from a request body, or { error }. `keep` is the stored row
  // on an edit: a value an imported response already carries (an option the
  // form has since dropped, like "2 Landing pages") stays allowed on that
  // enquiry, so editing it never quietly drops it.
  function readBody(b, keep = {}) {
    const plus = (list, extra) => [...list, ...String(extra || '').split('||').filter(Boolean)];
    const leads = keep.lead_handle_by ? [...LEAD_HANDLERS, keep.lead_handle_by] : LEAD_HANDLERS;
    const e = {
      client_name: clean(b.client_name, 255),
      business_name: clean(b.business_name, 500),
      mobile: clean(b.mobile, 30).replace(/[^\d+\s-]/g, ''),
      lead_handle_by: leads.includes(b.lead_handle_by) ? b.lead_handle_by : '',
      project_types: pick(plus(PROJECT_TYPES, keep.project_types), b.project_types).join('||'),
      platforms: pick(plus(PLATFORMS, keep.platforms), b.platforms).join('||'),
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
  // The serial goes in and comes back as the same double, so this is equality
  // give or take a millisecond. A wider window would let two enquiries added
  // in the same couple of seconds be taken for each other's rows.
  const sameStamp = (a, b) => typeof a === 'number' && Math.abs(a - b) <= 1 / 86400000;

  // Reads count against a per-minute quota this service account shares with
  // the rest of the app, so a new enquiry costs no read at all and an edit one.
  //
  // The tab's numeric id (appendCells needs it) is asked of Google once and
  // kept in app_settings, so a cold serverless start does not spend a read on
  // it. A "No grid with id" error (the tab was recreated) forgets it again.
  const TAB_ID_KEY = `enquiry_sheet_tab:${ENQUIRY_SHEET_ID}:${ENQUIRY_SHEET_TAB}`.slice(0, 100);
  let _tabSheetId = null;
  async function tabSheetId(sheets) {
    if (_tabSheetId !== null) return _tabSheetId;
    const [[saved]] = await db.query('SELECT value FROM app_settings WHERE key_name=?', [TAB_ID_KEY]);
    if (saved && /^\d+$/.test(saved.value)) return (_tabSheetId = Number(saved.value));
    const meta = await sheets.spreadsheets.get({ spreadsheetId: ENQUIRY_SHEET_ID, fields: 'sheets.properties(sheetId,title)' });
    const t = (meta.data.sheets || []).find(s => s.properties.title === ENQUIRY_SHEET_TAB);
    if (!t) throw new Error(`no "${ENQUIRY_SHEET_TAB}" tab in the Enquiry Capture sheet`);
    _tabSheetId = t.properties.sheetId;
    await db.query('INSERT INTO app_settings (key_name, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value=VALUES(value)',
      [TAB_ID_KEY, String(_tabSheetId)]).catch(err => console.error('Enquiry sheet tab id not saved:', err.message));
    return _tabSheetId;
  }
  function forgetTabSheetId() {
    _tabSheetId = null;
    db.query('DELETE FROM app_settings WHERE key_name=?', [TAB_ID_KEY]).catch(() => {});
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

  // Appends the enquiry as a new row whose A is `stamp`. Returns the row it
  // landed on, read from the write's own reply (column A comes back with it),
  // or null when that reply does not show it.
  async function sheetAppend(e, email, stamp) {
    const sheets = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets']);
    const sheetId = await tabSheetId(sheets);
    const res = await sheets.spreadsheets.batchUpdate({ spreadsheetId: ENQUIRY_SHEET_ID, requestBody: {
      requests: [{ appendCells: {
        sheetId, fields: CELL_FIELDS,
        rows: [{ values: [cell(stamp, STAMP_FMT), ...cellsBtoK(e), cell(email), ...cellsMtoO(e)] }],
      } }],
      includeSpreadsheetInResponse: true,
      responseRanges: [`${sheetTab()}!A:A`],
      responseIncludeGridData: true,
    } });
    const grid = (((res.data.updatedSpreadsheet || {}).sheets || []).find(s => s.properties && s.properties.sheetId === sheetId) || {}).data || [];
    for (const g of grid) {
      const rows = g.rowData || [];
      for (let i = rows.length - 1; i >= 0; i--) {
        const c = (rows[i].values || [])[0] || {};
        const v = (c.effectiveValue || c.userEnteredValue || {}).numberValue;
        if (sameStamp(v, stamp)) return (g.startRow || 0) + i + 1;
      }
    }
    return null;
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
  //
  // It must never add an enquiry twice. The stamp is saved BEFORE the append,
  // and sheet_row only once the row has been seen in the sheet, so a stamp
  // with no sheet_row means "an append was tried and may have landed" (a
  // failed attempt, a lost reply, or a lookup after it that hit the Sheets
  // read quota). Such an enquiry is looked for by its stamp first and only
  // appended when the search comes back empty; a failed search throws, which
  // ends this attempt without writing anything.
  //
  // A failure leaves sheet_pending set, and the enquiry is sent again by the
  // next retry, a minute or more later. Returns { warning, queued }: queued
  // when it is waiting for that retry, so the page need not show an error.
  async function syncToSheet(id, e, isNew) {
    try {
      const done = await pushToSheet(id, e);
      await db.query('UPDATE enquiries SET sheet_pending=?, sheet_try_at=NULL WHERE id=?', [done.pending ? 1 : 0, id]);
      return { warning: done.warning || null, queued: !!done.pending };
    } catch (err) {
      if (/no grid with id/i.test(err.message)) forgetTabSheetId();
      console.error('Enquiry sheet sync failed:', err.message);
      await db.query('UPDATE enquiries SET sheet_pending=1, sheet_try_at=NOW() WHERE id=?', [id])
        .catch(e2 => console.error('Enquiry sheet_pending not saved:', e2.message));
      return { queued: true, warning: /quota exceeded/i.test(err.message)
        ? 'Saved. Google Sheets is busy right now, so the Enquiry Capture sheet will get it automatically in a minute or two.'
        : `Saved, but the Enquiry Capture sheet could not be ${isNew ? 'given the new row' : 'updated'} yet; it will be tried again automatically: ${err.message}` };
    }
  }
  const sheetNote = s => s && s.warning ? { warning: s.warning, ...(s.queued ? { queued: true } : {}) } : {};
  // One attempt. Returns { pending, warning }; throws when the sheet fails.
  async function pushToSheet(id, e) {
    const [[row]] = await db.query(
      'SELECT q.sheet_row, q.sheet_stamp, u.email FROM enquiries q LEFT JOIN users u ON u.id = q.created_by WHERE q.id=?', [id]);
    if (!row) return {};
    if (row.sheet_stamp) {
      const at = await sheetUpdate(row.sheet_row, row.sheet_stamp, e);
      if (at) {
        if (at !== row.sheet_row) await db.query('UPDATE enquiries SET sheet_row=? WHERE id=?', [at, id]);
        return {};
      }
      // Seen in the sheet before and gone now: someone removed it there, and
      // sending it again would not bring it back.
      if (row.sheet_row) return { warning: 'Saved, but its row was not found in the Enquiry Capture sheet' };
    }
    let stamp = row.sheet_stamp;
    if (!stamp) {
      // Claimed only while still empty: when two saves of a new enquiry run
      // at once, one appends and the other leaves it pending for the retry.
      stamp = serialNow();
      const [claim] = await db.query('UPDATE enquiries SET sheet_stamp=? WHERE id=? AND sheet_stamp IS NULL', [stamp, id]);
      if (!claim.affectedRows) return { pending: true };
    }
    // No row number back means the row is not confirmed yet; the next save
    // then looks for it by its stamp, as above.
    const at = await sheetAppend(e, row.email, stamp);
    if (at) await db.query('UPDATE enquiries SET sheet_row=? WHERE id=?', [at, id]);
    return {};
  }

  const LIST_SQL = `SELECT e.id, e.client_name, e.business_name, e.mobile, e.lead_handle_by, e.project_types, e.platforms,
      DATE_FORMAT(e.meeting_scheduled_date, '%Y-%m-%d') AS meeting_scheduled_date,
      DATE_FORMAT(e.meeting_done_date, '%Y-%m-%d') AS meeting_done_date, e.meeting_url,
      DATE_FORMAT(e.proposal_date, '%Y-%m-%d') AS proposal_date, e.proposal_url,
      DATE_FORMAT(e.conversion_date, '%Y-%m-%d') AS conversion_date, e.order_value,
      e.status, e.client_id, e.source, e.sheet_pending, DATE_FORMAT(e.created_at, '%Y-%m-%d %H:%i') AS created_at,
      u.name AS created_by_name, DATE_FORMAT(e.updated_at, '%Y-%m-%d %H:%i') AS updated_at, u2.name AS updated_by_name,
      u3.name AS client_crm_name
    FROM enquiries e LEFT JOIN users u ON u.id = e.created_by LEFT JOIN users u2 ON u2.id = e.updated_by
      LEFT JOIN clients c ON c.id = e.client_id LEFT JOIN users u3 ON u3.id = c.added_by`;
  // client_crm_name: the CRM on the linked client, i.e. whoever added it to
  // Client Master (the CRM who pressed Add in Client Master on this enquiry,
  // or for one linked to an existing client, that client's CRM).

  app.get('/api/enquiries/options', requireAuth, (req, res) => {
    res.json({ leadHandlers: LEAD_HANDLERS, projectTypes: PROJECT_TYPES, platforms: PLATFORMS, statuses: STATUSES });
  });

  // Status and the Client Master link: Enquiry Capture's Editors, and the CRMs
  // who can see the page.
  async function canSetStatus(session) {
    if (!(await userCanSee(session, 'enquiry'))) return false;
    return (await userCanDo(session, 'edit_enquiry')) || (await userCanDo(session, 'crm_clients'));
  }
  const todayIst = () => new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);

  // Clients whose name, or brand name, is the enquiry's client or business
  // name (case and spacing ignored). This is how an enquiry that is already a
  // client is spotted before anyone adds it to Client Master a second time.
  const norm = v => String(v == null ? '' : v).toLowerCase().replace(/\s+/g, ' ').trim();
  function clientMatches(enq, clients) {
    const keys = new Set([norm(enq.client_name), norm(enq.business_name)].filter(Boolean));
    return clients.filter(c => keys.has(norm(c.name)) || (norm(c.brand_name) && keys.has(norm(c.brand_name))));
  }
  // Links every converted enquiry that has no client yet to the one client it
  // matches. An enquiry matching two clients is left for a person to decide.
  async function linkConverted() {
    const [open] = await db.query("SELECT id, client_name, business_name FROM enquiries WHERE status='Conversion' AND client_id IS NULL");
    if (!open.length) return 0;
    const [clients] = await db.query('SELECT id, name, brand_name FROM clients');
    let linked = 0;
    for (const q of open) {
      const m = clientMatches(q, clients);
      if (m.length !== 1) continue;
      const [r] = await db.query('UPDATE enquiries SET client_id=? WHERE id=? AND client_id IS NULL', [m[0].id, q.id]);
      linked += r.affectedRows;
    }
    return linked;
  }

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
      const sync = await syncToSheet(r.insertId, e, true);
      res.json({ success: true, id: r.insertId, ...sheetNote(sync) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Edit: the form sends every field, so the row is replaced as a whole. This
  // is how the later stages (meeting done, proposal, conversion) get filled in.
  app.put('/api/enquiries/:id', requireAuth, async (req, res) => {
    try {
      if (!(await userCanDo(req.session, 'edit_enquiry'))) return res.status(403).json({ error: 'You do not have edit access to Enquiry Capture' });
      await ensureTable();
      const [[cur]] = await db.query('SELECT lead_handle_by, project_types, platforms FROM enquiries WHERE id=?', [req.params.id]);
      if (!cur) return res.status(404).json({ error: 'Enquiry not found' });
      const { e, error } = readBody(req.body || {}, cur);
      if (error) return res.status(400).json({ error });
      const cols = Object.keys(e);
      const [r] = await db.query(
        `UPDATE enquiries SET ${cols.map(k => `${k}=?`).join(', ')}, updated_by=?, updated_at=NOW() WHERE id=?`,
        [...cols.map(k => e[k]), req.session.userId, req.params.id]);
      if (!r.affectedRows) return res.status(404).json({ error: 'Enquiry not found' });
      const sync = await syncToSheet(Number(req.params.id), e, false);
      res.json({ success: true, ...sheetNote(sync) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Sends the enquiries whose latest save did not reach the sheet. The page
  // calls it after loading the list. A few per call, each claimed through
  // sheet_try_at first so two calls never send the same one, and the first
  // failure ends the round: the quota is per minute, the rest would fail too.
  app.post('/api/enquiries/sheet-retry', requireAuth, async (req, res) => {
    try {
      if (!(await userCanSee(req.session, 'enquiry'))) return res.status(403).json({ error: 'You do not have access to Enquiry Capture' });
      await ensureTable();
      const due = 'sheet_pending=1 AND (sheet_try_at IS NULL OR sheet_try_at < NOW() - INTERVAL 1 MINUTE)';
      const [ids] = await db.query(`SELECT id FROM enquiries WHERE ${due} ORDER BY id LIMIT 5`);
      let sent = 0, failed = 0;
      for (const { id } of ids) {
        const [claim] = await db.query(`UPDATE enquiries SET sheet_try_at=NOW() WHERE id=? AND ${due}`, [id]);
        if (!claim.affectedRows) continue;
        const [[e]] = await db.query(`${LIST_SQL} WHERE e.id=?`, [id]);
        await syncToSheet(id, e, false);
        const [[after]] = await db.query('SELECT sheet_pending FROM enquiries WHERE id=?', [id]);
        if (after.sheet_pending) { failed++; break; }
        sent++;
      }
      const [[{ left }]] = await db.query('SELECT COUNT(*) AS `left` FROM enquiries WHERE sheet_pending=1');
      res.json({ success: true, sent, failed, left: Number(left) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Brings the Google Form's responses into the app. Safe to run again: a
  // response whose timestamp is already some enquiry's sheet_stamp (an earlier
  // import, or a row the app wrote itself) is skipped, and the unique key on
  // sheet_stamp stops a double import racing in. Dates come over as dates,
  // "Added by" is matched from the response's email, and a response with a
  // Conversion Date arrives as status Conversion.
  const sheetDate = v => {
    if (typeof v === 'number' && v > 0) return new Date(Math.round((Math.floor(v) - 25569) * 86400000)).toISOString().slice(0, 10);
    const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(String(v == null ? '' : v).trim());
    return m ? `${m[3]}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}` : null;
  };
  // A Sheets serial is the IST wall clock; this is the same wall clock as SQL text.
  const stampToSql = v => new Date(Math.round((v - 25569) * 86400000)).toISOString().slice(0, 19).replace('T', ' ');
  app.post('/api/enquiries/import-sheet', requireAuth, async (req, res) => {
    try {
      if (!(await userCanDo(req.session, 'admin_enquiry'))) return res.status(403).json({ error: 'Only the Admin level can import enquiries' });
      await ensureTable();
      const sheets = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
      const got = await sheets.spreadsheets.values.get({ spreadsheetId: ENQUIRY_SHEET_ID, range: `${sheetTab()}!A2:O`, valueRenderOption: 'UNFORMATTED_VALUE' });
      const rows = got.data.values || [];
      const [have] = await db.query('SELECT sheet_stamp FROM enquiries WHERE sheet_stamp IS NOT NULL');
      const stamps = have.map(r => Number(r.sheet_stamp));
      const [people] = await db.query(`SELECT id, LOWER(TRIM(email)) AS e, LOWER(TRIM(COALESCE(notification_email, ''))) AS n FROM users WHERE role <> 'client'`);
      const byEmail = new Map();
      for (const p of people) { if (p.n) byEmail.set(p.n, p.id); if (p.e) byEmail.set(p.e, p.id); }
      const s = v => String(v == null ? '' : v).trim();
      const list = v => s(v).split(',').map(x => x.trim()).filter(Boolean).join('||');
      const web = v => /^https?:\/\/\S+$/i.test(s(v)) ? s(v).slice(0, 1000) : null;
      let imported = 0, skipped = 0, empty = 0;
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const stamp = r[0];
        const client = s(r[1]).slice(0, 255), brand = s(r[14]).slice(0, 500);
        if (typeof stamp !== 'number' || (!client && !brand)) { empty++; continue; }
        if (stamps.some(x => sameStamp(stamp, x))) { skipped++; continue; }
        const conv = sheetDate(r[8]);
        try {
          await db.query(`INSERT INTO enquiries (client_name, business_name, mobile, lead_handle_by, project_types, platforms,
              meeting_scheduled_date, meeting_done_date, meeting_url, proposal_date, proposal_url, conversion_date, order_value,
              status, source, sheet_row, sheet_stamp, created_by, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'sheet',?,?,?,?)`,
            [client || brand, brand, s(r[2]).slice(0, 30) || null, s(r[12]).slice(0, 100) || null, list(r[13]) || null, list(r[10]) || null,
             sheetDate(r[3]), sheetDate(r[4]), web(r[5]), sheetDate(r[6]), web(r[7]), conv, s(r[9]).slice(0, 100) || null,
             conv ? 'Conversion' : 'Open', i + 2, stamp, byEmail.get(s(r[11]).toLowerCase()) || null, stampToSql(stamp)]);
          stamps.push(stamp);
          imported++;
        } catch (err) {
          if (err.code === 'ER_DUP_ENTRY') { skipped++; continue; }
          throw err;
        }
      }
      // Converted responses from before the app are mostly clients already.
      const linked = await linkConverted();
      res.json({ success: true, imported, skipped, empty, linked, total: rows.length });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Sets the status. Choosing Conversion fills an empty Conversion Date with
  // today, and that date goes to the sheet like any other edit.
  app.put('/api/enquiries/:id/status', requireAuth, async (req, res) => {
    try {
      if (!(await canSetStatus(req.session))) return res.status(403).json({ error: 'Only Enquiry Capture editors and CRMs can change the status' });
      const status = STATUSES.find(s => s === (req.body || {}).status);
      if (!status) return res.status(400).json({ error: 'Pick a status from the list' });
      await ensureTable();
      const id = Number(req.params.id);
      const [[cur]] = await db.query('SELECT status, conversion_date FROM enquiries WHERE id=?', [id]);
      if (!cur) return res.status(404).json({ error: 'Enquiry not found' });
      const fillDate = status === 'Conversion' && !cur.conversion_date;
      if (cur.status !== status) {
        await db.query(`UPDATE enquiries SET status=?, ${fillDate ? 'conversion_date=?, ' : ''}updated_by=?, updated_at=NOW() WHERE id=?`,
          [status, ...(fillDate ? [todayIst()] : []), req.session.userId, id]);
      }
      const [[row]] = await db.query(`${LIST_SQL} WHERE e.id=?`, [id]);
      const sync = fillDate && cur.status !== status ? await syncToSheet(id, row, false) : null;
      if (sync) row.sheet_pending = (await db.query('SELECT sheet_pending FROM enquiries WHERE id=?', [id]))[0][0].sheet_pending;
      res.json({ success: true, enquiry: row, ...sheetNote(sync) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // The client this enquiry looks like, if Client Master already has one.
  app.get('/api/enquiries/:id/client-match', requireAuth, async (req, res) => {
    try {
      if (!(await canSetStatus(req.session))) return res.status(403).json({ error: 'You cannot add enquiries to Client Master' });
      await ensureTable();
      const [[q]] = await db.query('SELECT client_name, business_name FROM enquiries WHERE id=?', [req.params.id]);
      if (!q) return res.status(404).json({ error: 'Enquiry not found' });
      const [clients] = await db.query('SELECT id, name, brand_name FROM clients');
      const m = clientMatches(q, clients);
      res.json({ match: m[0] || null });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // Ties the enquiry to its client in Client Master: the one just added from
  // it, or one that was already there.
  app.put('/api/enquiries/:id/client', requireAuth, async (req, res) => {
    try {
      if (!(await canSetStatus(req.session)) || !(await userCanDo(req.session, 'edit_clients'))) {
        return res.status(403).json({ error: 'You cannot add enquiries to Client Master' });
      }
      await ensureTable();
      const clientId = Number((req.body || {}).client_id);
      const [[c]] = await db.query('SELECT id, name FROM clients WHERE id=?', [clientId || 0]);
      if (!c) return res.status(400).json({ error: 'Client not found' });
      const [[q]] = await db.query('SELECT client_id FROM enquiries WHERE id=?', [req.params.id]);
      if (!q) return res.status(404).json({ error: 'Enquiry not found' });
      if (q.client_id && q.client_id !== c.id) return res.status(409).json({ error: 'This enquiry is already linked to another client' });
      await db.query('UPDATE enquiries SET client_id=?, updated_by=?, updated_at=NOW() WHERE id=?', [c.id, req.session.userId, req.params.id]);
      res.json({ success: true, client_id: c.id, client_name: c.name });
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
