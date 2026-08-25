// ══════════════════════════════════════════════════════
// LEADS ENQUIRY — four live Google Sheet sources
// ══════════════════════════════════════════════════════
// Ported from the standalone sales dashboard, which ran on its own domain with
// its own login. Nothing here touches a database: every source is a Google
// Sheet read live on each request, and every write goes straight back into the
// sheet. That is deliberate — these rows are owned by the sheets, not by us.
//
// The four sources:
//   website  — the site's enquiry form, keyed by an "Enquiry ID" column
//   meta     — Meta Lead Ads submissions, keyed by "id"
//   google   — Google Ads leads; the sheet has NO id column, so a row is found
//              by (Created Time + Phone) instead
//   manual   — sales team's own entries; append-only, no update route
//
// Both the sheet ids and tab names come from env with the dashboard's original
// values as fallbacks, so this keeps working untouched if nothing is set.
//
// Access: the standalone version hardcoded requireAdmin. Here the gate is
// userCanSee('leads') / userCanDo('edit_leads') so a grant from the Access
// Control panel actually reaches the API. Admin resolves to 'all' and passes
// either way. Using requireAdmin would have reproduced the HR Portal bug —
// the tab appears for a granted user and then every request 403s.
//
// Dependencies are passed in rather than re-required so these are the SAME
// instances server.js uses (the Sheets clients in particular are cached).
module.exports = function registerLeadsRoutes(app, deps) {
  const {
    requireAuth,
    userCanSee,
    userCanDo,
    getSheetsClient,
    extractSpreadsheetId,
    idxToCol,
  } = deps;

  const READ_SCOPE  = ['https://www.googleapis.com/auth/spreadsheets.readonly'];
  const WRITE_SCOPE = ['https://www.googleapis.com/auth/spreadsheets'];

  const ENQUIRIES_SHEET_ID  = process.env.ENQUIRIES_SHEET_ID  || '1GD-gmK4JcK8KsXUf5W_ZBeVPg0o0Yh6vb5E1qv5d2vQ';
  const ENQUIRIES_TAB       = process.env.ENQUIRIES_TAB       || 'Enquiries';
  const META_LEADS_SHEET_ID = process.env.META_LEADS_SHEET_ID || '1YZa2efJg_7UpX8GclWFO4Fod0wEnSTbSLeervZJA-Bc';
  const META_LEADS_TAB      = process.env.META_LEADS_TAB      || 'Website with otp';
  const GADS_SHEET_ID       = process.env.GADS_SHEET_ID       || '1yL6PlGqM1XkiWGOq161f9u4HWSOqmDp03VGRGQ92qeI';
  const GADS_TAB            = process.env.GADS_TAB            || 'Leads';
  const MANUAL_SHEET_ID     = process.env.MANUAL_SHEET_ID     || '1m8-AbxLza21Vj8Q184k9fcZdm0KqbpMad88IyZ-rZh4';
  const MANUAL_TAB          = process.env.MANUAL_TAB          || 'Sheet1';

  // "Submitted At" -> submitted_at, so the frontend can address columns by a
  // stable key even when someone retitles a header in the sheet.
  const toKey = h => h.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');

  const emptyPayload = () => ({ headers: [], keys: [], rows: [], count: 0, updatedAt: new Date().toISOString() });

  // Header row -> keys, remaining rows -> objects. Fully blank rows are dropped:
  // sheets routinely carry trailing empties that would otherwise render as rows.
  function tabulate(values, headers, keys) {
    return values.slice(1)
      .filter(r => r.some(c => (c || '').trim() !== ''))
      .map(r => { const o = {}; keys.forEach((k, i) => { o[k] = (r[i] || '').trim(); }); return o; });
  }

  // The sheet APIs answer a permission failure with a generic 500, which reads
  // to the user as "the app is broken" when the real cause is that the service
  // account is only a Viewer on that sheet. Detect it and say so.
  function writeFailure(res, err, verb) {
    const perm = /permission|403|forbidden|not have write|editor/i.test(err.message);
    res.status(perm ? 403 : 500).json({
      error: perm
        ? 'No write access to the sheet — please give the service account Editor access (it may currently be Viewer only).'
        : (verb + ' failed: ' + err.message),
    });
  }

  // IST, matching the "18 Aug 2026 20:15" format the sheets already use.
  const istStamp = () => new Date().toLocaleString('en-GB', {
    timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).replace(',', '');

  const canSee = async (req, res) => {
    if (await userCanSee(req.session, 'leads')) return true;
    res.status(403).json({ error: 'Forbidden' });
    return false;
  };
  const canEdit = async (req, res) => {
    if (await userCanDo(req.session, 'edit_leads')) return true;
    res.status(403).json({ error: 'Forbidden' });
    return false;
  };

  // ── Website enquiries ───────────────────────────────
  app.get('/api/enquiries', requireAuth, async (req, res) => {
    if (!(await canSee(req, res))) return;
    try {
      const sheetsApi = await getSheetsClient(READ_SCOPE);
      const resp = await sheetsApi.spreadsheets.values.get({
        spreadsheetId: extractSpreadsheetId(ENQUIRIES_SHEET_ID),
        range: ENQUIRIES_TAB,
      });
      const values = resp.data.values || [];
      if (!values.length) return res.json(emptyPayload());

      const headers = values[0].map(h => (h || '').trim());
      const keys = headers.map(toKey);
      const rows = tabulate(values, headers, keys);
      res.json({ headers, keys, rows, count: rows.length, updatedAt: new Date().toISOString() });
    } catch (err) {
      console.error('  ❌ /api/enquiries error:', err.message);
      res.status(500).json({ error: 'Enquiries load failed: ' + err.message });
    }
  });

  // Status / Notes written straight back into the sheet — "called them", a
  // remark, and so on.
  app.post('/api/enquiries/update', requireAuth, async (req, res) => {
    if (!(await canEdit(req, res))) return;
    try {
      const { enquiryId, status, notes } = req.body || {};
      if (!enquiryId) return res.status(400).json({ error: 'enquiryId required' });

      const sheetsApi = await getSheetsClient(WRITE_SCOPE);
      const spreadsheetId = extractSpreadsheetId(ENQUIRIES_SHEET_ID);
      const resp = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range: ENQUIRIES_TAB });
      const values = resp.data.values || [];
      if (!values.length) return res.status(404).json({ error: 'Sheet is empty' });

      const headers = values[0].map(h => (h || '').trim().toLowerCase());
      const col = name => headers.indexOf(name);
      const idCol = col('enquiry id'), statusCol = col('status'), notesCol = col('notes'), updCol = col('last updated');
      if (idCol < 0) return res.status(500).json({ error: 'Enquiry ID column not found in the sheet' });

      let rowNum = -1;
      for (let i = 1; i < values.length; i++) {
        if ((values[i][idCol] || '').trim() === String(enquiryId).trim()) { rowNum = i + 1; break; } // sheet rows are 1-indexed
      }
      if (rowNum < 0) return res.status(404).json({ error: 'Enquiry not found in the sheet' });

      const nowStr = istStamp();
      const data = [];
      if (statusCol >= 0 && status !== undefined) data.push({ range: `${ENQUIRIES_TAB}!${idxToCol(statusCol)}${rowNum}`, values: [[status]] });
      if (notesCol  >= 0 && notes  !== undefined) data.push({ range: `${ENQUIRIES_TAB}!${idxToCol(notesCol)}${rowNum}`,  values: [[notes]] });
      if (updCol    >= 0)                         data.push({ range: `${ENQUIRIES_TAB}!${idxToCol(updCol)}${rowNum}`,    values: [[nowStr]] });
      if (!data.length) return res.json({ ok: true });

      await sheetsApi.spreadsheets.values.batchUpdate({ spreadsheetId, requestBody: { valueInputOption: 'USER_ENTERED', data } });
      res.json({ ok: true, updatedAt: nowStr });
    } catch (err) {
      console.error('  ❌ /api/enquiries/update error:', err.message);
      writeFailure(res, err, 'Update');
    }
  });

  // ── Meta Lead Ads ───────────────────────────────────
  app.get('/api/meta-leads', requireAuth, async (req, res) => {
    if (!(await canSee(req, res))) return;
    try {
      const sheetsApi = await getSheetsClient(READ_SCOPE);
      const resp = await sheetsApi.spreadsheets.values.get({
        spreadsheetId: extractSpreadsheetId(META_LEADS_SHEET_ID),
        range: META_LEADS_TAB,
      });
      const values = resp.data.values || [];
      if (!values.length) return res.json(emptyPayload());

      // This sheet's rows run wider than its header row: the remark column that
      // follows lead_status was never given a title. Walk the widest row rather
      // than the header so those cells still get a key instead of vanishing.
      const width = values.reduce((m, r) => Math.max(m, r.length), 0);
      const rawHeaders = values[0].map(h => (h || '').trim());
      const headers = [], keys = [];
      for (let i = 0; i < width; i++) {
        if (rawHeaders[i]) { headers.push(rawHeaders[i]); keys.push(toKey(rawHeaders[i])); }
        else if (i === rawHeaders.length) { headers.push('Remark'); keys.push('remark'); }
        else { headers.push('Column ' + (i + 1)); keys.push('col_' + i); }
      }
      const rows = tabulate(values, headers, keys);
      res.json({ headers, keys, rows, count: rows.length, updatedAt: new Date().toISOString() });
    } catch (err) {
      console.error('  ❌ /api/meta-leads error:', err.message);
      res.status(500).json({ error: 'Meta leads load failed: ' + err.message });
    }
  });

  app.post('/api/meta-leads/update', requireAuth, async (req, res) => {
    if (!(await canEdit(req, res))) return;
    try {
      const { leadId, status, remark } = req.body || {};
      if (!leadId) return res.status(400).json({ error: 'leadId required' });

      const sheetsApi = await getSheetsClient(WRITE_SCOPE);
      const spreadsheetId = extractSpreadsheetId(META_LEADS_SHEET_ID);
      const resp = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range: META_LEADS_TAB });
      const values = resp.data.values || [];
      if (!values.length) return res.status(404).json({ error: 'Sheet is empty' });

      const rawHeaders = values[0].map(h => (h || '').trim().toLowerCase());
      const idCol = rawHeaders.indexOf('id');
      const statusCol = rawHeaders.indexOf('lead_status');
      const remarkCol = statusCol >= 0 ? statusCol + 1 : -1; // the unlabelled column right after lead_status
      if (idCol < 0) return res.status(500).json({ error: 'id column not found in the sheet' });

      let rowNum = -1;
      for (let i = 1; i < values.length; i++) {
        if ((values[i][idCol] || '').trim() === String(leadId).trim()) { rowNum = i + 1; break; }
      }
      if (rowNum < 0) return res.status(404).json({ error: 'Lead not found in the sheet' });

      const data = [];
      if (statusCol >= 0 && status !== undefined) data.push({ range: `${META_LEADS_TAB}!${idxToCol(statusCol)}${rowNum}`, values: [[status]] });
      if (remarkCol >= 0 && remark !== undefined) data.push({ range: `${META_LEADS_TAB}!${idxToCol(remarkCol)}${rowNum}`, values: [[remark]] });
      if (!data.length) return res.json({ ok: true });

      await sheetsApi.spreadsheets.values.batchUpdate({ spreadsheetId, requestBody: { valueInputOption: 'USER_ENTERED', data } });
      res.json({ ok: true });
    } catch (err) {
      console.error('  ❌ /api/meta-leads/update error:', err.message);
      writeFailure(res, err, 'Update');
    }
  });

  // ── Google Ads leads ────────────────────────────────
  app.get('/api/google-ads', requireAuth, async (req, res) => {
    if (!(await canSee(req, res))) return;
    try {
      const sheetsApi = await getSheetsClient(READ_SCOPE);
      const resp = await sheetsApi.spreadsheets.values.get({
        spreadsheetId: extractSpreadsheetId(GADS_SHEET_ID),
        range: GADS_TAB,
      });
      const values = resp.data.values || [];
      if (!values.length) return res.json(emptyPayload());

      const headers = values[0].map(h => (h || '').trim());
      const keys = headers.map(toKey);
      const rows = tabulate(values, headers, keys);
      res.json({ headers, keys, rows, count: rows.length, updatedAt: new Date().toISOString() });
    } catch (err) {
      console.error('  ❌ /api/google-ads error:', err.message);
      res.status(500).json({ error: 'Google Ads leads load failed: ' + err.message });
    }
  });

  // This sheet has no id column at all, so the row is located by matching
  // Created Time and Phone together. Header names vary between exports, hence
  // the regex column lookups rather than exact titles.
  app.post('/api/google-ads/update', requireAuth, async (req, res) => {
    if (!(await canEdit(req, res))) return;
    try {
      const { createdTime, phone, status, remark } = req.body || {};
      if (!createdTime && !phone) return res.status(400).json({ error: 'createdTime or phone required' });

      const sheetsApi = await getSheetsClient(WRITE_SCOPE);
      const spreadsheetId = extractSpreadsheetId(GADS_SHEET_ID);
      const resp = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range: GADS_TAB });
      const values = resp.data.values || [];
      if (!values.length) return res.status(404).json({ error: 'Sheet is empty' });

      const hdr = values[0].map(h => (h || '').trim().toLowerCase());
      const ctCol     = hdr.findIndex(h => /created\s*time|^created/.test(h));
      const phoneCol  = hdr.findIndex(h => /phone|whatsapp|mobile/.test(h));
      const statusCol = hdr.findIndex(h => /^status/.test(h));
      const remarkCol = hdr.findIndex(h => /^remark/.test(h));
      if (statusCol < 0 && remarkCol < 0) return res.status(500).json({ error: 'Status / Remarks column not found in the sheet' });

      const norm = v => String(v || '').trim();
      let rowNum = -1;
      for (let i = 1; i < values.length; i++) {
        const ctOk = ctCol    < 0 || !createdTime || norm(values[i][ctCol])    === norm(createdTime);
        const phOk = phoneCol < 0 || !phone       || norm(values[i][phoneCol]) === norm(phone);
        if (ctOk && phOk && (createdTime || phone)) { rowNum = i + 1; break; }
      }
      if (rowNum < 0) return res.status(404).json({ error: 'Lead not found in the sheet' });

      const data = [];
      if (statusCol >= 0 && status !== undefined) data.push({ range: `${GADS_TAB}!${idxToCol(statusCol)}${rowNum}`, values: [[status]] });
      if (remarkCol >= 0 && remark !== undefined) data.push({ range: `${GADS_TAB}!${idxToCol(remarkCol)}${rowNum}`, values: [[remark]] });
      if (!data.length) return res.json({ ok: true });

      await sheetsApi.spreadsheets.values.batchUpdate({ spreadsheetId, requestBody: { valueInputOption: 'USER_ENTERED', data } });
      res.json({ ok: true });
    } catch (err) {
      console.error('  ❌ /api/google-ads/update error:', err.message);
      writeFailure(res, err, 'Update');
    }
  });

  // ── Manual entries ──────────────────────────────────
  app.get('/api/manual', requireAuth, async (req, res) => {
    if (!(await canSee(req, res))) return;
    try {
      const sheetsApi = await getSheetsClient(READ_SCOPE);
      const resp = await sheetsApi.spreadsheets.values.get({
        spreadsheetId: extractSpreadsheetId(MANUAL_SHEET_ID),
        range: MANUAL_TAB,
      });
      const values = resp.data.values || [];
      if (!values.length) return res.json(emptyPayload());

      const headers = values[0].map(h => (h || '').trim());
      const keys = headers.map(toKey);
      const rows = tabulate(values, headers, keys);
      res.json({ headers, keys, rows, count: rows.length, updatedAt: new Date().toISOString() });
    } catch (err) {
      console.error('  ❌ /api/manual error:', err.message);
      res.status(500).json({ error: 'Manual entries load failed: ' + err.message });
    }
  });

  // Append-only: the sales team adds a lead, nothing edits one afterwards.
  app.post('/api/manual/add', requireAuth, async (req, res) => {
    if (!(await canEdit(req, res))) return;
    try {
      const b = req.body || {};
      const name = (b.name || '').trim();
      if (!name) return res.status(400).json({ error: 'Name is required' });

      const sheetsApi = await getSheetsClient(WRITE_SCOPE);
      const spreadsheetId = extractSpreadsheetId(MANUAL_SHEET_ID);
      // Sr No. continues the sheet's own count rather than being tracked here.
      const cur = await sheetsApi.spreadsheets.values.get({ spreadsheetId, range: `${MANUAL_TAB}!A:A` });
      const dataCount = Math.max(0, ((cur.data.values || []).length) - 1); // minus the header
      const srNo = dataCount + 1;

      const row = [
        String(srNo), name, (b.company || '').trim(), (b.contactPerson || '').trim(),
        (b.phone || '').trim(), (b.email || '').trim(), (b.avgOrder || '').toString().trim(),
        (b.currentClient || '').trim(), istStamp(),
      ];
      await sheetsApi.spreadsheets.values.append({
        spreadsheetId, range: `${MANUAL_TAB}!A1`, valueInputOption: 'USER_ENTERED',
        insertDataOption: 'INSERT_ROWS', requestBody: { values: [row] },
      });
      res.json({ ok: true, srNo });
    } catch (err) {
      console.error('  ❌ /api/manual/add error:', err.message);
      writeFailure(res, err, 'Save');
    }
  });
};
