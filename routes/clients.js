// ══════════════════════════════════════════════════════
// CLIENT MASTER — client records, handlers, logins and the DMS document tree
// ══════════════════════════════════════════════════════
// Lifted out of server.js unchanged — the bodies below are byte-for-byte what
// lived there, so this file versus the removed block is an empty diff.
//
// Dependencies are passed in rather than re-required: these must be the SAME
// instances server.js uses, not fresh copies. db in particular carries the
// max_user_connections retry wrapper.
module.exports = function registerClientRoutes(app, deps) {
  const {
    db,
    express,
    bcrypt,
    requireAuth,
    requireAdmin,
    requireAdminOrHod,
    requireClientsEditor,
    requireClientEditor,
    archiveDeleted,
    userCanSee,
    userCanDo,
    isHandlerOf,
    parseSystemLinks,
    sanitizeSystemLinks,
    getDriveClient,
    dmsUpload,
    dmsCreateFolder,
    dmsCreateFile,
    dmsListFiles,
    dmsShareFolder,
    dmsUploadFile,
    dmsInitiateResumableUpload,
    _dmsLogActivity,
    _dmsIsSafeUrl,
    DMS_MIME_TYPES,
  } = deps;

// Build the WHERE clause that limits the client list to what the caller may see
// on the Client Master page. Returns null when they may see everything.
//   admin / pc → everything.
//   hod        → every client handled by someone in their department (which
//                covers the clients they handle themselves).
//   anyone else→ only the clients they personally handle.
// "Handles" means either the primary clients.handler_id or a client_handlers row.
async function clientMasterScope(req) {
  const role = req.session.role;
  // admin / PC / HOD see every client. HODs were scoped to the clients handled
  // by their own department, but handler assignments are not filled in yet, so
  // that left them with almost an empty list — they get the full list until
  // those assignments exist and the user asks to narrow it again.
  if (role === 'admin' || role === 'pc' || role === 'hod') return null;
  const uid = req.session.userId;
  // Everyone else: only the clients they personally handle, by the primary
  // handler_id or a client_handlers row.
  return {
    sql: `(c.handler_id = ?
           OR EXISTS (SELECT 1 FROM client_handlers ch
                      WHERE ch.client_id = c.id AND ch.user_id = ?))`,
    params: [uid, uid],
  };
}

app.get('/api/clients', requireAuth, async (req, res) => {
  try {
    // `?scope=master` narrows the list to what the caller may see on Client
    // Master. Without it the list stays unfiltered on purpose — the Daily Task,
    // Delegation, Checklist, Meetings and DMS pickers share this route and must
    // keep offering every client.
    const scope = req.query.scope === 'master' ? await clientMasterScope(req) : null;
    const [rows] = await db.query(
      `SELECT c.id, c.name, c.handler_id, c.logo_url, COALESCE(c.is_active,1) AS is_active,
              u.name AS handler_name,
              (SELECT GROUP_CONCAT(u2.name ORDER BY u2.name SEPARATOR '||')
               FROM client_handlers ch JOIN users u2 ON ch.user_id = u2.id
               WHERE ch.client_id = c.id) AS all_handler_names
       FROM clients c LEFT JOIN users u ON c.handler_id = u.id
       ${scope ? `WHERE ${scope.sql}` : ''}
       ORDER BY c.name ASC`, scope ? scope.params : []);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Update / clear client logo. Body: { logo: <data-URL string> | null }.
// 1.5 MB cap on payload — base64 expansion + headroom for a 256x256 JPEG.
// The five Client Master write routes below moved off requireAdminOrHod onto
// edit_clients, so that row in Access Control finally decides something. Same
// people pass today — hod carries the key by default, admin passes everything —
// and PC, which the middleware already refused, still cannot. DELETE stays
// admin-only: routing it through edit_clients would hand every hod a Remove
// button they have never had.
app.put('/api/clients/:id/logo', requireAuth, requireClientsEditor, async (req, res) => {
  try {
    const id = req.params.id;
    let { logo } = req.body;
    if (logo === undefined) return res.status(400).json({ error: 'logo field required (string or null)' });
    if (logo !== null && typeof logo === 'string') {
      if (!/^data:image\/(png|jpe?g|webp|gif);base64,/.test(logo)) {
        return res.status(400).json({ error: 'logo must be a data:image/* base64 URL' });
      }
      if (logo.length > 1_500_000) return res.status(413).json({ error: 'Logo too large — keep under 1 MB after resize' });
    } else if (logo !== null) {
      return res.status(400).json({ error: 'logo must be a string or null' });
    }
    const [[exists]] = await db.query('SELECT id FROM clients WHERE id=?', [id]);
    if (!exists) return res.status(404).json({ error: 'Client not found' });
    await db.query('UPDATE clients SET logo_url=? WHERE id=?', [logo, id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/clients', requireAuth, requireClientsEditor, async (req, res) => {
  try {
    const name = (req.body.name || '').trim();
    const handlerRaw = req.body.handler_id;
    const handlerId = handlerRaw == null || handlerRaw === '' ? null : parseInt(handlerRaw, 10);
    const loginEmail = (req.body.login_email || '').trim().toLowerCase();
    const loginPassword = req.body.login_password || '';
    if (!name) return res.status(400).json({ error: 'Client name required' });
    // Provisioning a login is optional. If asked, both fields must be present.
    if ((loginEmail && !loginPassword) || (!loginEmail && loginPassword)) {
      return res.status(400).json({ error: 'Both login email and password required to provision client login' });
    }
    const [r] = await db.query('INSERT INTO clients (name, handler_id) VALUES (?, ?)',
      [name, Number.isFinite(handlerId) ? handlerId : null]);
    const newClientId = r.insertId;
    if (loginEmail && loginPassword) {
      try {
        const hash = bcrypt.hashSync(loginPassword, 10);
        await db.query(
          `INSERT INTO users (name, email, password, role, user_role, client_id)
           VALUES (?, ?, ?, 'client', 'client', ?)`,
          [name, loginEmail, hash, newClientId]);
      } catch (e) {
        // Client row was created — surface auth provisioning error separately so
        // admin knows the client exists but login was not set up.
        return res.status(201).json({
          success: true, client_id: newClientId,
          warning: e.code === 'ER_DUP_ENTRY' ? 'Client added but login email already in use' : 'Client added but login provisioning failed: ' + e.message
        });
      }
    }
    // Auto-create Drive folder if root folder is configured (fire-and-forget, never blocks the response)
    const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    if (rootFolderId) {
      dmsCreateFolder(name, rootFolderId)
        .then(folder => db.query('UPDATE clients SET drive_folder_id=? WHERE id=?', [folder.id, newClientId]))
        .catch(e => console.error('DMS auto-folder creation failed for client', newClientId, e.message));
    }
    res.json({ success: true, client_id: newClientId });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'Client already exists' });
    res.status(500).json({ error: err.message });
  }
});

// No role middleware on purpose: the body below decides. requireAdminOrHod here
// would make the handler branch under it unreachable, which is the bug e662ea0
// set out to fix. A userCanSee('clients') gate was the other candidate and was
// left out too — f7259db reverted exactly that enforcement, and it would 403 a
// handler whose saved permissions happen to omit 'clients' on their own client.
app.put('/api/clients/:id', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    // Full editors (edit_clients permission) may change anything. A handler of
    // this client may still edit its operational fields — portal links, the
    // WhatsApp group, and the active flag — but not rename or reassign it.
    const isEditor = await userCanDo(req.session, 'edit_clients');
    let handlerOnly = false;
    if (!isEditor) {
      const [[c]] = await db.query('SELECT id, handler_id FROM clients WHERE id=?', [id]);
      if (!c || !(await isHandlerOf(req.session.userId, c))) return res.status(403).json({ error: 'Forbidden' });
      handlerOnly = true;
    }
    const name = req.body.name == null ? null : String(req.body.name).trim();
    const handlerRaw = req.body.handler_id;
    const handlerId = handlerRaw === undefined ? undefined
                    : (handlerRaw == null || handlerRaw === '') ? null
                    : parseInt(handlerRaw, 10);
    if (!handlerOnly && name === '') return res.status(400).json({ error: 'Client name cannot be empty' });
    // Only update fields that were sent.
    const sets = [], params = [];
    // Structural fields — full editors only; a handler cannot rename/reassign.
    if (!handlerOnly) {
      if (name !== null) { sets.push('name=?'); params.push(name); }
      if (handlerId !== undefined) { sets.push('handler_id=?'); params.push(handlerId); }
    }
    // Active flag — a handler may retire their own client. Not structural: it
    // records whether the client is still live, not who owns it. Deliberately
    // outside the !handlerOnly block — inside it, a handler's toggle was dropped
    // and the request still returned {noop:true}, so the UI flipped the switch
    // on a write that never happened and the row came back Active on refresh.
    if (req.body.is_active !== undefined) { sets.push('is_active=?'); params.push(req.body.is_active ? 1 : 0); }
    if (req.body.system_links !== undefined) { sets.push('system_links=?'); params.push(sanitizeSystemLinks(req.body.system_links)); }
    if (req.body.whatsapp_group_id !== undefined) {
      const g = String(req.body.whatsapp_group_id || '').trim();
      // Blank clears it (and so switches the digest off for this client).
      if (g && !/^[\w.-]+@g\.us$/.test(g)) {
        return res.status(400).json({ error: 'WhatsApp group ID must look like 1203634...@g.us' });
      }
      sets.push('whatsapp_group_id=?'); params.push(g || null);
    }
    if (!sets.length) return res.json({ success: true, noop: true });
    params.push(id);
    await db.query(`UPDATE clients SET ${sets.join(', ')} WHERE id=?`, params);
    res.json({ success: true });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'Client name already exists' });
    res.status(500).json({ error: err.message });
  }
});

// Multi-handler support — get all handlers for a client
// Gated in the body, not by middleware — see the note on PUT /api/clients/:id.
app.get('/api/clients/:id/handlers', requireAuth, async (req, res) => {
  try {
    // Managers see any client; a regular handler may open only the clients they
    // handle — so the Client Master detail card works for them, not just admins.
    if (!['admin', 'hod', 'pc'].includes(req.session.role)) {
      const [[c]] = await db.query('SELECT id, handler_id FROM clients WHERE id=?', [req.params.id]);
      if (!c || !(await isHandlerOf(req.session.userId, c))) return res.status(403).json({ error: 'Forbidden' });
    }
    const [rows] = await db.query(
      `SELECT ch.user_id AS id, u.name, COALESCE(u.department,'') AS department
       FROM client_handlers ch JOIN users u ON u.id=ch.user_id
       WHERE ch.client_id=? ORDER BY u.name`, [req.params.id]);
    res.json(rows);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Multi-handler support — replace all handlers for a client
app.put('/api/clients/:id/handlers', requireAuth, requireClientsEditor, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const userIds = Array.isArray(req.body.user_ids)
      ? req.body.user_ids.map(Number).filter(n => Number.isFinite(n) && n > 0)
      : [];
    await db.query('DELETE FROM client_handlers WHERE client_id=?', [id]);
    if (userIds.length) {
      await db.query(
        `INSERT INTO client_handlers (client_id, user_id) VALUES ${userIds.map(() => '(?,?)').join(',')}`,
        userIds.flatMap(uid => [id, uid]));
    }
    // Keep primary handler_id in sync with first selected (for backward compat)
    await db.query('UPDATE clients SET handler_id=? WHERE id=?', [userIds[0] || null, id]);
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/clients/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [doomed] = await db.query('SELECT * FROM clients WHERE id=?', [req.params.id]);
    await archiveDeleted('clients', doomed, req, { summary: r => `Client: ${r.name || ''}` });
    await db.query('DELETE FROM clients WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk add clients via CSV
app.post('/api/clients/bulk', requireAuth, requireClientsEditor, async (req, res) => {
  try {
    const { names } = req.body;
    if (!Array.isArray(names) || !names.length) {
      return res.status(400).json({ error: 'No clients to add' });
    }
    // Clean + dedupe within request
    const cleanNames = [...new Set(
      names.map(n => String(n||'').trim()).filter(n => n)
    )];
    if (!cleanNames.length) return res.status(400).json({ error: 'No valid client names' });

    let added = 0, skipped = 0;
    const skippedNames = [];
    for (const name of cleanNames) {
      try {
        await db.query('INSERT INTO clients (name) VALUES (?)', [name]);
        added++;
      } catch (e) {
        if (e.code === 'ER_DUP_ENTRY') { skipped++; skippedNames.push(name); }
        else throw e;
      }
    }
    res.json({ success: true, added, skipped, skippedNames });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Client stats — full client snapshot for the detail page. Defaults to the
// current month (IST). Accepts ?from=YYYY-MM-DD&to=YYYY-MM-DD to widen the
// window. Includes delegation/checklist task counts + meetings + recent rows.
// Gated in the body, not by middleware — see the note on PUT /api/clients/:id.
app.get('/api/clients/:id/stats', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const [[client]] = await db.query(
      `SELECT c.id, c.name, c.handler_id, c.logo_url, c.system_links, c.whatsapp_group_id,
              u.name AS handler_name, u.email AS handler_email
       FROM clients c LEFT JOIN users u ON c.handler_id = u.id WHERE c.id=?`, [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    // Managers see any client; a regular handler may open only the clients they
    // handle. Reuses the row just fetched (has id + handler_id) for the check.
    if (!['admin', 'hod', 'pc'].includes(req.session.role) && !(await isHandlerOf(req.session.userId, client))) {
      return res.status(403).json({ error: 'Forbidden' });
    }
    client.system_links = parseSystemLinks(client.system_links);

    // Login user (if provisioned) — there's at most one client login per client.
    const [[loginUser]] = await db.query(
      "SELECT id, email FROM users WHERE role='client' AND client_id=? LIMIT 1", [id]);

    // Default window — current month (IST)
    const ist = new Date(Date.now() + (5.5 * 60 * 60 * 1000));
    const y = ist.getUTCFullYear(), m = ist.getUTCMonth();
    const defaultFrom = `${y}-${String(m+1).padStart(2,'0')}-01`;
    const lastDay = new Date(Date.UTC(y, m+1, 0)).getUTCDate();
    const defaultTo = `${y}-${String(m+1).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;
    const isDate = v => /^\d{4}-\d{2}-\d{2}$/.test(v || '');
    const from = isDate(req.query.from) ? req.query.from : defaultFrom;
    const to   = isDate(req.query.to)   ? req.query.to   : defaultTo;

    const [[del]] = await db.query(
      `SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='revised'   THEN 1 ELSE 0 END) AS revised,
        SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM delegation_tasks WHERE client_id=? AND due_date BETWEEN ? AND ?`, [id, from, to]);
    const [[chl]] = await db.query(
      `SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
       FROM checklist_tasks WHERE client_id=? AND due_date BETWEEN ? AND ?`, [id, from, to]);

    // Meetings tied to this client (by client_id)
    const [[meet]] = await db.query(
      `SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN status='scheduled' THEN 1 ELSE 0 END) AS scheduled,
        SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
        SUM(CASE WHEN status='done'      THEN 1 ELSE 0 END) AS done
       FROM meetings WHERE client_id=? AND meeting_date BETWEEN ? AND ?`, [id, from, to]);
    const [meetRecent] = await db.query(
      `SELECT m.id, m.title, m.status, m.meet_link,
              DATE_FORMAT(m.meeting_date,'%Y-%m-%d') AS meeting_date,
              TIME_FORMAT(m.start_time,'%H:%i') AS start_time,
              TIME_FORMAT(m.end_time,'%H:%i')   AS end_time,
              u.name AS organizer_name
       FROM meetings m LEFT JOIN users u ON m.organizer_id = u.id
       WHERE m.client_id=? AND m.meeting_date BETWEEN ? AND ?
       ORDER BY m.meeting_date DESC, m.start_time DESC LIMIT 15`, [id, from, to]);

    // Recent activity — tasks (delegation + checklist) created in the window
    const [recentDel] = await db.query(
      `SELECT t.id, 'delegation' AS type, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u1.name AS doer, COALESCE(u2.name,'—') AS assigner,
              DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created
       FROM delegation_tasks t
       JOIN users u1 ON t.assigned_to=u1.id
       LEFT JOIN users u2 ON t.assigned_by=u2.id
       WHERE t.client_id=? AND DATE(t.created_at) BETWEEN ? AND ?
       ORDER BY t.created_at DESC LIMIT 25`, [id, from, to]);
    const [recentChl] = await db.query(
      `SELECT t.id, 'checklist' AS type, t.description, t.status, t.priority,
              DATE_FORMAT(t.due_date,'%Y-%m-%d') AS due_date,
              u1.name AS doer, COALESCE(u2.name,'—') AS assigner,
              DATE_FORMAT(t.created_at,'%Y-%m-%d') AS created
       FROM checklist_tasks t
       JOIN users u1 ON t.assigned_to=u1.id
       LEFT JOIN users u2 ON t.assigned_by=u2.id
       WHERE t.client_id=? AND DATE(t.created_at) BETWEEN ? AND ?
       ORDER BY t.created_at DESC LIMIT 25`, [id, from, to]);
    const recent = [...recentDel, ...recentChl]
      .sort((a,b) => (b.created||'').localeCompare(a.created||''))
      .slice(0, 20);

    res.json({
      client: {
        id: client.id, name: client.name, logo_url: client.logo_url,
        handler_id: client.handler_id, handler_name: client.handler_name, handler_email: client.handler_email,
        system_links: client.system_links, whatsapp_group_id: client.whatsapp_group_id
      },
      login: loginUser ? { provisioned: true, email: loginUser.email } : { provisioned: false },
      range: { from, to },
      delegation: del, checklist: chl, meetings: { ...meet, recent: meetRecent },
      recent
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Provision or update client login. Creates the users row if missing, or
// updates email/password on the existing one. Admin/HOD/PC only.
app.post('/api/clients/:id/login', requireAuth, requireClientsEditor, async (req, res) => {
  try {
    const id = req.params.id;
    const email = (req.body.email || '').trim().toLowerCase();
    const password = req.body.password || '';
    if (!email || !password) return res.status(400).json({ error: 'email and password required' });
    const [[client]] = await db.query('SELECT id, name FROM clients WHERE id=?', [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    const [[existing]] = await db.query(
      "SELECT id FROM users WHERE role='client' AND client_id=? LIMIT 1", [id]);
    const hash = bcrypt.hashSync(password, 10);
    try {
      if (existing) {
        await db.query('UPDATE users SET email=?, password=? WHERE id=?', [email, hash, existing.id]);
      } else {
        await db.query(
          `INSERT INTO users (name, email, password, role, user_role, client_id)
           VALUES (?, ?, ?, 'client', 'client', ?)`,
          [client.name, email, hash, id]);
      }
    } catch (e) {
      if (e.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'That email is already in use' });
      throw e;
    }
    res.json({ success: true, email });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// GOOGLE DRIVE STATUS — service account (GOOGLE_CREDENTIALS) must have
// Editor access on GOOGLE_DRIVE_ROOT_FOLDER_ID (share the folder with its
// client_email in the Drive UI). No OAuth consent flow needed.
// ══════════════════════════════════════════════════════
app.get('/api/google/drive-status', requireAuth, requireAdmin, async (req, res) => {
  const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
  if (!rootFolderId) return res.json({ connected: false, reason: 'GOOGLE_DRIVE_ROOT_FOLDER_ID not set' });
  try {
    const drive = await getDriveClient();
    await drive.files.get({ fileId: rootFolderId, fields: 'id', supportsAllDrives: true });
    res.json({ connected: true });
  } catch (e) { res.json({ connected: false, reason: e.message }); }
});

// ══════════════════════════════════════════════════════
// DMS — Document Management System (Google Drive)
// ══════════════════════════════════════════════════════

// Get DMS status for a client
app.get('/api/clients/:id/dms', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const id = req.params.id;
    const [[client]] = await db.query(
      'SELECT id, name, drive_folder_id FROM clients WHERE id=?', [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    const [depts] = await db.query(
      'SELECT department_name, drive_folder_id FROM client_department_folders WHERE client_id=? ORDER BY department_name',
      [id]);
    const drive_configured = !!process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    res.json({
      client_id: client.id,
      client_name: client.name,
      drive_folder_id: client.drive_folder_id || null,
      drive_configured,
      departments: depts,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Create Drive folders for every existing client that doesn't have one yet
// (one-time catch-up after GOOGLE_DRIVE_ROOT_FOLDER_ID is first configured).
app.post('/api/admin/dms/bulk-setup', requireAuth, requireAdmin, async (req, res) => {
  try {
    const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    if (!rootFolderId) return res.status(400).json({ error: 'GOOGLE_DRIVE_ROOT_FOLDER_ID env var not set' });
    const [clients] = await db.query(
      "SELECT id, name FROM clients WHERE COALESCE(is_active,1) != 0 AND (drive_folder_id IS NULL OR drive_folder_id = '')");
    let created = 0;
    const errors = [];
    for (const c of clients) {
      try {
        const folder = await dmsCreateFolder(c.name, rootFolderId);
        await db.query('UPDATE clients SET drive_folder_id=? WHERE id=?', [folder.id, c.id]);
        await _dmsLogActivity(folder.id, 'created', c.name, req, c.id);
        created++;
      } catch (e) { errors.push(`${c.name}: ${e.message}`); }
    }
    res.json({ success: true, total: clients.length, created, failed: errors.length, errors });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Live listing of the DMS root Drive folder — every client folder with its
// real modifiedTime/size/last-editor, matched back to our client_id so the
// DMS "Clients" table can look and behave exactly like Drive's own list view.
app.get('/api/admin/dms/root-files', requireAuth, requireAdmin, async (req, res) => {
  try {
    const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    if (!rootFolderId) return res.status(400).json({ error: 'GOOGLE_DRIVE_ROOT_FOLDER_ID env var not set' });
    const files = await dmsListFiles(rootFolderId);
    const [clients] = await db.query("SELECT id, drive_folder_id FROM clients WHERE drive_folder_id IS NOT NULL AND drive_folder_id != ''");
    const byFolderId = Object.fromEntries(clients.map(c => [c.drive_folder_id, c.id]));
    for (const f of files) { const cid = byFolderId[f.id]; if (cid) f.client_id = cid; }

    // Prefer "last activity anywhere in this client's folder" from our own
    // log over the Drive folder's own modifiedTime — Drive never bumps a
    // folder's timestamp when a file inside it is added/changed.
    const clientIds = Object.values(byFolderId);
    if (clientIds.length) {
      // created_at is stored in the DB server's own local time (IST here, per
      // @@session.time_zone), not UTC. Convert to an explicit UTC ISO string
      // in SQL so mysql2/the browser can't double-apply the offset — sending
      // the raw DATETIME let the frontend re-interpret an already-local value
      // as UTC and shift it by the server's offset again (3:12 PM -> 8:42 PM).
      const [rows] = await db.query(
        `SELECT client_id, user_name,
                DATE_FORMAT(CONVERT_TZ(created_at, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%SZ') AS created_at
         FROM dms_file_activity
         WHERE client_id IN (${clientIds.map(()=>'?').join(',')})
         ORDER BY created_at DESC`,
        clientIds
      ).catch(() => [[]]);
      const latestByClient = {};
      for (const r of rows) { if (!latestByClient[r.client_id]) latestByClient[r.client_id] = r; }
      for (const f of files) {
        const log = f.client_id && latestByClient[f.client_id];
        if (log) { f.modified_by = log.user_name; f.modifiedTime = log.created_at; }
      }
    }
    res.json(files);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Create the client's root Drive folder (one-time setup)
app.post('/api/clients/:id/dms/setup', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const rootFolderId = process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID;
    if (!rootFolderId) return res.status(400).json({ error: 'GOOGLE_DRIVE_ROOT_FOLDER_ID env var not set' });
    const id = req.params.id;
    const [[client]] = await db.query('SELECT id, name, drive_folder_id FROM clients WHERE id=?', [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    if (client.drive_folder_id) {
      return res.json({ success: true, drive_folder_id: client.drive_folder_id, already_exists: true });
    }
    const folder = await dmsCreateFolder(client.name, rootFolderId);
    await db.query('UPDATE clients SET drive_folder_id=? WHERE id=?', [folder.id, id]);
    await _dmsLogActivity(folder.id, 'created', client.name, req, id);
    res.json({ success: true, drive_folder_id: folder.id, web_view_link: folder.webViewLink });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add a department subfolder under the client's Drive folder
app.post('/api/clients/:id/dms/departments', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const id = req.params.id;
    const dept = (req.body.department_name || '').trim();
    if (!dept) return res.status(400).json({ error: 'department_name required' });
    const [[client]] = await db.query('SELECT id, name, drive_folder_id FROM clients WHERE id=?', [id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    if (!client.drive_folder_id) return res.status(400).json({ error: 'Set up the client Drive folder first' });
    const [[existing]] = await db.query(
      'SELECT drive_folder_id FROM client_department_folders WHERE client_id=? AND department_name=?',
      [id, dept]);
    if (existing) return res.json({ success: true, drive_folder_id: existing.drive_folder_id, already_exists: true });
    const folder = await dmsCreateFolder(dept, client.drive_folder_id);
    await db.query(
      'INSERT INTO client_department_folders (client_id, department_name, drive_folder_id) VALUES (?,?,?)',
      [id, dept, folder.id]);
    // Share with all users in this department + all admins (fire-and-forget)
    db.query(
      `SELECT DISTINCT email FROM users
       WHERE email IS NOT NULL AND email != ''
         AND (department=? OR role='admin')
         AND role != 'client'`,
      [dept]
    ).then(([members]) => {
      return Promise.all(members.map(m => dmsShareFolder(folder.id, m.email).catch(() => {})));
    }).catch(() => {});
    res.json({ success: true, drive_folder_id: folder.id, web_view_link: folder.webViewLink });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Remove a department mapping (does NOT delete the Drive folder)
app.delete('/api/clients/:id/dms/departments/:dept', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const id = req.params.id;
    const dept = decodeURIComponent(req.params.dept);
    const [doomed] = await db.query(
      'SELECT * FROM client_department_folders WHERE client_id=? AND department_name=?', [id, dept]);
    await archiveDeleted('client_department_folders', doomed, req, {
      summary: r => `DMS dept folder mapping: ${r.department_name || ''} (client ${r.client_id})`,
      reason: 'Mapping removed — the Drive folder itself is left untouched',
    });
    await db.query(
      'DELETE FROM client_department_folders WHERE client_id=? AND department_name=?', [id, dept]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// List files in a Drive folder (must belong to this client) — merged with any
// name+link entries pasted into this folder via the external-link feature,
// since those aren't real Drive objects and dmsListFiles() can't see them.
app.get('/api/clients/:id/dms/folders/:folderId/files', requireAuth, async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const files = await dmsListFiles(folderId);
    const [linkRows] = await db.query(
      `SELECT id, name, url, created_by_name,
              DATE_FORMAT(CONVERT_TZ(created_at, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%SZ') AS created_at
       FROM dms_external_links WHERE folder_id=? ORDER BY created_at DESC`,
      [folderId]
    );
    const linkFiles = linkRows.map(r => ({
      id: 'ext-' + r.id,
      name: r.name,
      mimeType: 'application/x-emk-external-link',
      webViewLink: r.url,
      modifiedTime: r.created_at,
      modified_by: r.created_by_name,
      size: null,
    }));
    const merged = [...files, ...linkFiles].sort((a, b) => {
      const aFolder = a.mimeType === 'application/vnd.google-apps.folder' ? 0 : 1;
      const bFolder = b.mimeType === 'application/vnd.google-apps.folder' ? 0 : 1;
      if (aFolder !== bFolder) return aFolder - bFolder;
      return a.name.toLowerCase().localeCompare(b.name.toLowerCase());
    });
    res.json(merged);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Create a Google Doc / Sheet / Slide in a folder
app.post('/api/clients/:id/dms/folders/:folderId/files', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const name = (req.body.name || '').trim();
    const kind = (req.body.kind || '').toLowerCase();
    if (!name) return res.status(400).json({ error: 'name required' });
    if (kind !== 'folder' && !DMS_MIME_TYPES[kind]) return res.status(400).json({ error: 'kind must be doc, sheet, slide, or folder' });
    const file = kind === 'folder' ? await dmsCreateFolder(name, folderId) : await dmsCreateFile(name, kind, folderId);
    await _dmsLogActivity(file.id, 'created', name, req, id);
    res.json({ success: true, id: file.id, web_view_link: file.webViewLink });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Upload an actual file (PDF, image, doc, etc.) into a Drive folder
app.post('/api/clients/:id/dms/folders/:folderId/upload', requireAuth, requireClientEditor, dmsUpload.single('file'), async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!req.file) return res.status(400).json({ error: 'file required' });
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const file = await dmsUploadFile(req.file.originalname, req.file.mimetype, req.file.buffer, folderId);
    await _dmsLogActivity(file.id, 'uploaded', req.file.originalname, req, id);
    res.json({ success: true, id: file.id, web_view_link: file.webViewLink });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add a simple name+link entry to a client's folder — NOT a real Drive object,
// just a DB row we merge into the file listing. No Drive API / sharing needed,
// unlike the (removed) Drive-shortcut approach — the user often doesn't control
// sharing permissions on files owned by other people.
app.post('/api/clients/:id/dms/folders/:folderId/external-link', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const name = (req.body.name || '').trim();
    const url = (req.body.url || '').trim();
    if (!name) return res.status(400).json({ error: 'Name is required' });
    if (!url || !_dmsIsSafeUrl(url)) return res.status(400).json({ error: 'A valid http(s) link is required' });
    const [result] = await db.query(
      'INSERT INTO dms_external_links (client_id, folder_id, name, url, created_by, created_by_name) VALUES (?,?,?,?,?,?)',
      [id, folderId, name, url, req.session.userId, req.session.name || '']
    );
    const linkFileId = 'ext-' + result.insertId;
    await _dmsLogActivity(linkFileId, 'created', name, req, id);
    res.json({ success: true, id: linkFileId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Step 1 of a large-file upload: get a Drive resumable-session URL.
// A direct browser PUT to this URL bypasses Vercel's ~4.5MB request cap, but
// Drive's resumable-upload response is missing CORS headers on completion,
// so the browser can't read the result even though the file was created.
// The browser instead sends chunks through /upload-chunk below, which
// proxies each one to this URL server-side (no CORS involved there).
app.post('/api/clients/:id/dms/folders/:folderId/upload-session', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const { name, mimeType, size } = req.body;
    if (!name || !size) return res.status(400).json({ error: 'name and size required' });
    const uploadUrl = await dmsInitiateResumableUpload(name, mimeType, Number(size), folderId);
    res.json({ success: true, uploadUrl });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Step 2: proxy one chunk of the file to the resumable session URL from
// step 1. Each chunk stays comfortably under Vercel's request-body cap even
// though the overall file can be far larger. Responds 308 (with the byte
// range Drive has received so far) while more chunks are expected, or the
// created file's metadata once Drive reports the upload complete.
app.post('/api/clients/:id/dms/folders/:folderId/upload-chunk', requireAuth, requireClientEditor, express.raw({ type: () => true, limit: '6mb' }), async (req, res) => {
  try {
    const { id, folderId } = req.params;
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const uploadUrl = req.query.uploadUrl;
    const contentRange = req.headers['content-range'];
    if (!uploadUrl || !contentRange) return res.status(400).json({ error: 'uploadUrl and Content-Range required' });
    const chunk = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body || '');
    const fetchFn = global.fetch || (await import('node-fetch')).default;
    const driveRes = await fetchFn(uploadUrl, {
      method: 'PUT',
      headers: { 'Content-Range': contentRange, 'Content-Length': String(chunk.length) },
      body: chunk,
      redirect: 'manual',
    });
    if (driveRes.status === 308) {
      return res.status(308).json({ incomplete: true, range: driveRes.headers.get('range') || null });
    }
    const text = await driveRes.text();
    if (!driveRes.ok) return res.status(driveRes.status).json({ error: text || `Drive chunk upload failed (${driveRes.status})` });
    let file; try { file = JSON.parse(text); } catch { file = {}; }
    if (file.id) await _dmsLogActivity(file.id, 'uploaded', file.name, req, id);
    res.json({ success: true, ...file });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

async function _dmsValidFolderIds(clientId) {
  const [[clientRow]] = await db.query('SELECT drive_folder_id FROM clients WHERE id=?', [clientId]);
  const [deptRows] = await db.query(
    'SELECT drive_folder_id FROM client_department_folders WHERE client_id=?', [clientId]);
  return new Set([clientRow?.drive_folder_id, ...deptRows.map(r => r.drive_folder_id)].filter(Boolean));
}

// A folder qualifies if it's the client's own root, a registered department
// folder, OR any nested subfolder under either (e.g. one made via the
// "New folder" right-click action, which isn't tracked in our DB at all) —
// walk up Drive's own parent chain to confirm ancestry, bounded so a bad
// folderId can't trigger an unbounded walk.
async function _dmsCanAccessFolder(clientId, folderId) {
  const validIds = await _dmsValidFolderIds(clientId);
  if (validIds.has(folderId)) return true;
  try {
    const drive = await getDriveClient();
    let current = folderId;
    for (let i = 0; i < 10; i++) {
      const res = await drive.files.get({ fileId: current, fields: 'parents', supportsAllDrives: true });
      const parents = res.data.parents || [];
      if (parents.some(p => validIds.has(p))) return true;
      if (!parents.length) return false;
      current = parents[0];
    }
  } catch (e) { console.error('DMS ancestor check failed:', e.message); }
  return false;
}

// Rename a file/folder in a client's Drive folder
app.patch('/api/clients/:id/dms/folders/:folderId/files/:fileId', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const { id, folderId, fileId } = req.params;
    const name = (req.body.name || '').trim();
    if (!name) return res.status(400).json({ error: 'name required' });
    if (fileId.startsWith('ext-')) {
      const linkId = fileId.slice(4);
      if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
      const [r] = await db.query('UPDATE dms_external_links SET name=? WHERE id=? AND folder_id=?', [name, linkId, folderId]);
      if (!r.affectedRows) return res.status(404).json({ error: 'Link not found' });
      await _dmsLogActivity(fileId, 'renamed', name, req, id);
      return res.json({ success: true });
    }
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const drive = await getDriveClient();
    await drive.files.update({ fileId, requestBody: { name }, supportsAllDrives: true });
    await _dmsLogActivity(fileId, 'renamed', name, req, id);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete (trash) a file/folder in a client's Drive folder — moves to Drive's
// Trash rather than a permanent delete, so it stays recoverable.
app.delete('/api/clients/:id/dms/folders/:folderId/files/:fileId', requireAuth, requireClientEditor, async (req, res) => {
  try {
    const { id, folderId, fileId } = req.params;
    if (fileId.startsWith('ext-')) {
      const linkId = fileId.slice(4);
      if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
      const [[linkRow]] = await db.query('SELECT * FROM dms_external_links WHERE id=? AND folder_id=?', [linkId, folderId]);
      await archiveDeleted('dms_external_links', linkRow, req, {
        summary: r => `DMS link: ${r.name || ''}`,
      });
      const [r] = await db.query('DELETE FROM dms_external_links WHERE id=? AND folder_id=?', [linkId, folderId]);
      if (!r.affectedRows) return res.status(404).json({ error: 'Link not found' });
      await _dmsLogActivity(fileId, 'deleted', linkRow?.name || null, req, id);
      return res.json({ success: true });
    }
    if (!(await _dmsCanAccessFolder(id, folderId))) return res.status(403).json({ error: 'Folder does not belong to this client' });
    const drive = await getDriveClient();
    let name = null;
    try { const meta = await drive.files.get({ fileId, fields: 'name', supportsAllDrives: true }); name = meta.data.name; } catch {}
    await drive.files.update({ fileId, requestBody: { trashed: true }, supportsAllDrives: true });
    await _dmsLogActivity(fileId, 'deleted', name, req, id);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
};
