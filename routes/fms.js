// ══════════════════════════════════════════════════════
// FMS — admin sheet config and the doer-facing task views
// ══════════════════════════════════════════════════════
// Lifted out of server.js unchanged — the bodies below are byte-for-byte what
// lived there, so this file versus the removed block is an empty diff.
//
// Dependencies are passed in rather than re-required: these must be the SAME
// instances server.js uses, not fresh copies. db in particular carries the
// max_user_connections retry wrapper.
module.exports = function registerFmsRoutes(app, deps) {
  const {
    db,
    requireAuth,
    requireAdmin,
    archiveDeleted,
    colToIdx,
    idxToCol,
    extractSpreadsheetId,
    getSheetsClient,
  } = deps;

// ══════════════════════════════════════════════════════
// FMS ADMIN APIs
// ══════════════════════════════════════════════════════

app.get('/api/fms', requireAuth, requireAdmin, async (req, res) => {
  try {
    // LEFT JOIN, not INNER: an FMS whose creator was later deleted must still
    // show here (an INNER JOIN silently dropped it — e.g. "Google Ads FMS"
    // appeared in FMS Tasks but vanished from FMS Admin).
    const [sheets] = await db.query(`SELECT f.*, COALESCE(u.name,'—') AS createdByName FROM fms_sheets f LEFT JOIN users u ON f.created_by=u.id ORDER BY f.created_at DESC`);
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
      if (s.extraInput==='yes' && s.extraRows?.length) for (const row of s.extraRows) await conn.query('INSERT INTO fms_extra_rows (step_id,row_label,col_letter,field_type,dropdown_options,required) VALUES (?,?,?,?,?,?)', [stepId, row.label||row.col_letter||'', row.col_letter||'', row.field_type||'text', row.dropdown_options||'', row.required===false||row.required===0?0:1]);
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
      if (s.extraInput==='yes' && s.extraRows?.length) for (const row of s.extraRows) await conn.query('INSERT INTO fms_extra_rows (step_id,row_label,col_letter,field_type,dropdown_options,required) VALUES (?,?,?,?,?,?)', [stepId, row.label||row.col_letter||'', row.col_letter||'', row.field_type||'text', row.dropdown_options||'', row.required===false||row.required===0?0:1]);
    }
    await conn.commit();
    res.json({ success: true });
  } catch (err) { await conn.rollback(); res.status(500).json({ error: err.message }); } finally { conn.release(); }
});

app.delete('/api/fms/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [doomed] = await db.query('SELECT * FROM fms_sheets WHERE id=?', [req.params.id]);
    await archiveDeleted('fms_sheets', doomed, req, { summary: r => `FMS sheet: ${r.sheet_name || ''}` });
    await db.query('DELETE FROM fms_sheets WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Fetch headers ONLY (fast — just one row from sheet) ──
// requireAdmin because this reads an arbitrary spreadsheet through the server's
// own service account: pass any sheetId and it returns that sheet's header row.
// It is a setup tool for FMS Admin, which is an admin-only page — the route just
// never said so, leaving every logged-in employee able to probe any sheet the
// service account can open.
app.post('/api/fms/fetch-headers', requireAuth, requireAdmin, async (req, res) => {
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

// Dropdown options may reference live user lists instead of hard-coded names:
//   @users          -> every non-client user
//   @dept:Meta Ads  -> every non-client user in that department
// Tokens expand in place, so "Priya,@dept:Meta Ads" keeps Priya first. Names
// already present are not repeated.
async function expandDropdownUserTokens(optionsStr) {
  const parts = String(optionsStr || '').split(',').map(o => o.trim()).filter(Boolean);
  if (!parts.some(o => /^@(users$|dept:)/i.test(o))) return optionsStr;

  const out = [], seen = new Set();
  const add = name => {
    const clean = String(name || '').trim();
    const key = clean.toLowerCase();
    if (!clean || seen.has(key)) return;
    seen.add(key);
    out.push(clean);
  };

  for (const part of parts) {
    const deptMatch = part.match(/^@dept:\s*(.+)$/i);
    if (/^@users$/i.test(part)) {
      const [users] = await db.query(
        `SELECT name FROM users WHERE role <> 'client' AND client_id IS NULL ORDER BY name ASC`
      );
      users.forEach(u => add(u.name));
    } else if (deptMatch) {
      const [users] = await db.query(
        `SELECT name FROM users
         WHERE role <> 'client' AND client_id IS NULL
           AND LOWER(TRIM(department)) = ? ORDER BY name ASC`,
        [deptMatch[1].trim().toLowerCase()]
      );
      users.forEach(u => add(u.name));
    } else {
      add(part);
    }
  }
  return out.join(',');
}

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
      for (const row of extraRows) {
        // Only the tasks view expands tokens; the builder keeps the raw text so it round-trips on save.
        if ((row.field_type || '') === 'dropdown') row.dropdown_options = await expandDropdownUserTokens(row.dropdown_options);
      }
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
    const blankClean = v => (v || '').toString().replace(/[\s ​-‍﻿]+/g, '');
    dataRows.forEach((row, i) => {
      const planVal = planIdx >= 0 ? (row[planIdx]||'').trim() : '';
      const actualVal = actualIdx >= 0 ? (row[actualIdx]||'').trim() : '';
      if (!blankClean(planVal) || blankClean(actualVal)) return; // skip non-pending rows
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
      // Plan column is always shown — mandatory
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

    // The client already refuses this — "This FMS step is not assigned to you",
    // app.html:8545, `step.isMyStep || ME.role === 'admin'` — but the route
    // asked nobody, so any authenticated user could stamp any row of any FMS
    // sheet by guessing two ids, and extraInputs below is written to the sheet
    // verbatim. This mirrors the client rule rather than inventing a new one.
    //
    // A step with NO doer rows stays open to everyone, exactly as today. Closing
    // that case would silently make such steps admin-only, and steps are
    // configured in the FMS Admin screen where leaving doers empty is allowed.
    if (req.session.role !== 'admin') {
      const [[doers]] = await db.query(
        'SELECT COUNT(*) AS total, SUM(user_id = ?) AS mine FROM fms_step_doers WHERE step_id = ?',
        [req.session.userId, step.id]);
      if (Number(doers?.total || 0) > 0 && !Number(doers?.mine || 0)) {
        return res.status(403).json({ error: 'This FMS step is not assigned to you' });
      }
    }

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

    // Write the doer's name into the sheet (if doer_name_col is configured)
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
};
