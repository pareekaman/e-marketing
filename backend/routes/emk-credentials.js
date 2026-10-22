// EMK Credentials — the in-app version of the "EMK Client Credentials" Apps
// Script page. The Dashboard's Business Automation Credentials button opens it
// in the same window. Rows come straight from the "Credentials" tab of that
// Google Sheet (shared with the service account, viewer), so an edit in the
// sheet shows on the next open; nothing is stored here.
//
// Only the user ids in viewerIds (BA_CREDENTIALS_VIEWER_IDS in server.js) get
// it, also when an admin opens one of their dashboards with the Dashboard
// switcher (view as); only admins can do that.
const SHEET_ID = process.env.EMK_CREDENTIALS_SHEET_ID || '1rUHRNCNC0B5I98EXNtn6vVKOTp-m40S6TPy4pN2aIIU';
const TAB = 'Credentials';
const EMP_TAB = 'Employees';
// Columns A-J and M are written by Add New. K (Status) is the sheet's own
// ARRAYFORMULA — a value written into it would break the formula for every
// row — and L (Last Updated) is set to now.
const ADD_FIELDS = ['employee', 'client', 'link', 'userId', 'password', 'loginEmail',
  'emailPassword', 'hostingerId', 'hostingerPassword', 'notes'];

module.exports = function registerEmkCredentialsRoutes(app, deps) {
  const { requireAuth, getSheetsClient, viewerIds } = deps;

  app.get('/api/emk-credentials', requireAuth, async (req, res) => {
    if (!viewerIds.includes(Number(req.session.userId))) {
      return res.status(403).json({ error: 'Not available for your account' });
    }
    try {
      const sheets = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
      // Columns A-M as the sheet lays them out (CredentialsSheetSetup.gs);
      // dates come as the sheet shows them.
      const got = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID, range: `'${TAB}'!A2:M`, valueRenderOption: 'FORMATTED_VALUE',
      });
      const s = v => String(v == null ? '' : v);
      // `row` is the sheet row number, which Edit writes back to.
      const records = (got.data.values || [])
        .map((row, i) => ({ row, sheetRow: i + 2 }))
        .filter(({ row }) => s(row[1]).trim())
        .map(({ row, sheetRow }) => ({
          row: sheetRow,
          employee: s(row[0]), client: s(row[1]), link: s(row[2]),
          userId: s(row[3]), password: s(row[4]),
          loginEmail: s(row[5]), emailPassword: s(row[6]),
          hostingerId: s(row[7]), hostingerPassword: s(row[8]),
          notes: s(row[9]), status: s(row[10]), updated: s(row[11]), kickstart: s(row[12]),
        }));
      // The Employees tab feeds the Add New form's dropdown, as it does the sheet's.
      let employees = [];
      try {
        const emp = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `'${EMP_TAB}'!A2:A200` });
        employees = (emp.data.values || []).map(r => s(r[0]).trim()).filter(Boolean);
      } catch (e) { /* the list still loads without the dropdown names */ }
      res.set('Cache-Control', 'no-store');
      res.json({ records, employees });
    } catch (err) {
      if (err.code === 403 || err.code === 404) {
        return res.status(502).json({ error: 'The credentials sheet could not be read. Is it still shared with the app?' });
      }
      res.status(500).json({ error: err.message });
    }
  });

  // The fields of an Add New / Edit form, cleaned; { error } when unusable.
  function readForm(b) {
    const v = {};
    for (const f of ADD_FIELDS) v[f] = String(b[f] == null ? '' : b[f]).replace(/\r/g, '').trim().slice(0, f === 'notes' ? 2000 : 500);
    // A leading = + - @ would make the sheet run it as a formula
    for (const f of ADD_FIELDS) if (/^[=+\-@]/.test(v[f])) v[f] = "'" + v[f];
    if (!v.client) return { error: 'Client name is required.' };
    if (!v.employee) return { error: 'Pick the employee.' };
    const kick = String(b.kickstart || '').trim();
    if (kick && !/^\d{4}-\d{2}-\d{2}$/.test(kick)) return { error: 'Kickstart date is not valid.' };
    return { v, kick };
  }
  // Writes A-J and L-M of one row; K (Status) is the sheet's formula and is left alone.
  async function writeRow(sheets, rowNo, v, kick) {
    const ist = new Date(Date.now() + 5.5 * 3600e3).toISOString().replace('T', ' ').slice(0, 19);
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        valueInputOption: 'USER_ENTERED',
        data: [
          { range: `'${TAB}'!A${rowNo}:J${rowNo}`, values: [ADD_FIELDS.map(f => v[f])] },
          { range: `'${TAB}'!L${rowNo}:M${rowNo}`, values: [[ist, kick]] },
        ],
      },
    });
  }
  function sheetError(res, err) {
    if (err.code === 403) {
      return res.status(502).json({ error: 'The app can only read this sheet. Share it with the app as Editor to add or edit from here.' });
    }
    if (err.code === 404) return res.status(502).json({ error: 'The credentials sheet could not be found.' });
    res.status(500).json({ error: err.message });
  }

  // Add New: one credentials row, in the first empty row under the last client.
  // Same people as the list. Needs the sheet shared with the service account as Editor.
  app.post('/api/emk-credentials', requireAuth, async (req, res) => {
    if (!viewerIds.includes(Number(req.session.userId))) {
      return res.status(403).json({ error: 'Not available for your account' });
    }
    const form = readForm(req.body || {});
    if (form.error) return res.status(400).json({ error: form.error });
    try {
      const sheets = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets']);
      const col = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `'${TAB}'!B2:B` });
      const rowNo = (col.data.values || []).length + 2;   // the row after the last one in use
      await writeRow(sheets, rowNo, form.v, form.kick);
      res.json({ success: true, row: rowNo });
    } catch (err) { sheetError(res, err); }
  });

  // Edit: rewrites one row. The page sends the client and link it showed for
  // that row; if the row now holds something else (rows were moved or deleted
  // in the sheet since the page loaded), nothing is written.
  app.put('/api/emk-credentials/:row', requireAuth, async (req, res) => {
    if (!viewerIds.includes(Number(req.session.userId))) {
      return res.status(403).json({ error: 'Not available for your account' });
    }
    const rowNo = parseInt(req.params.row, 10);
    if (!(rowNo >= 2 && rowNo <= 5000)) return res.status(400).json({ error: 'Invalid row.' });
    const form = readForm(req.body || {});
    if (form.error) return res.status(400).json({ error: form.error });
    try {
      const sheets = await getSheetsClient(['https://www.googleapis.com/auth/spreadsheets']);
      const cur = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `'${TAB}'!B${rowNo}:C${rowNo}` });
      const [client = '', link = ''] = (cur.data.values || [[]])[0] || [];
      const same = (a, b) => String(a || '').trim().toLowerCase() === String(b || '').trim().toLowerCase();
      if (!same(client, req.body.origClient) || !same(link, req.body.origLink)) {
        return res.status(409).json({ error: 'This row has changed in the sheet since the page loaded. Refresh and try again.' });
      }
      await writeRow(sheets, rowNo, form.v, form.kick);
      res.json({ success: true, row: rowNo });
    } catch (err) { sheetError(res, err); }
  });
};
