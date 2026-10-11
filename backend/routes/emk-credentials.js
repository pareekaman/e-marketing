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
      const records = (got.data.values || [])
        .filter(row => s(row[1]).trim())
        .map(row => ({
          employee: s(row[0]), client: s(row[1]), link: s(row[2]),
          userId: s(row[3]), password: s(row[4]),
          loginEmail: s(row[5]), emailPassword: s(row[6]),
          hostingerId: s(row[7]), hostingerPassword: s(row[8]),
          notes: s(row[9]), status: s(row[10]), updated: s(row[11]), kickstart: s(row[12]),
        }));
      res.set('Cache-Control', 'no-store');
      res.json({ records });
    } catch (err) {
      if (err.code === 403 || err.code === 404) {
        return res.status(502).json({ error: 'The credentials sheet could not be read. Is it still shared with the app?' });
      }
      res.status(500).json({ error: err.message });
    }
  });
};
