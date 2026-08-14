// ══════════════════════════════════════════════════════
// INVENTORY MANAGEMENT — items, assignment, handover, return
// ══════════════════════════════════════════════════════
// Lifted out of server.js unchanged — the bodies below are byte-for-byte what
// lived there, so this file versus the removed block is an empty diff.
//
// Dependencies are passed in rather than re-required: these must be the SAME
// instances server.js uses, not fresh copies. db in particular carries the
// max_user_connections retry wrapper.
module.exports = function registerInventoryRoutes(app, deps) {
  const {
    db,
    requireAuth,
    userCanDo,
    userCanSee,
    archiveDeleted,
  } = deps;

// ══════════════════════════════════════════════════════
// INVENTORY MANAGEMENT
// ══════════════════════════════════════════════════════

// Why an assignment ended, and where that leaves the item. Single source of
// truth for both the valid reasons and the status each one implies — keep in
// step with the return_reason ENUM and inventory_items.status.
const INV_RETURN_REASONS = Object.freeze({
  damaged:     { label: 'Damaged',     itemStatus: 'damaged'   },
  retired:     { label: 'Retired',     itemStatus: 'retired'   },
  offboarding: { label: 'Offboarding', itemStatus: 'available' },
});
// Own-property check, not a bare lookup: reason is client-supplied, and
// `INV_RETURN_REASONS['constructor']` would otherwise pass validation and then
// blow up with an undefined itemStatus.
const invReturnReason = r =>
  (typeof r === 'string' && Object.hasOwn(INV_RETURN_REASONS, r)) ? INV_RETURN_REASONS[r] : null;
// Retiring an item is a judgement about the asset's life, so it stays with the
// custodian — the person handing kit back can only say why they're handing it
// back, not that it's finished.
const INV_HOLDER_REASONS = new Set(['offboarding', 'damaged']);

// Get all items (admin/hod see all; others see only assigned to them)
app.get('/api/inventory/items', requireAuth, async (req, res) => {
  try {
    // The read side of the same page. Every role default carries 'inventory', so
    // this refuses nobody who could reach the page yesterday — it exists so that
    // setting Inventory to No Access in the panel closes the API too, not just
    // the sidebar entry.
    if (!(await userCanSee(req.session, 'inventory'))) return res.status(403).json({ error: 'No access to Inventory' });
    const isAdmin = ['admin','hod'].includes(req.session.role);
    let rows;
    if (isAdmin) {
      [rows] = await db.query(`
        SELECT i.*, u.name AS assigned_to_name, u.id AS assigned_to_id,
               a.id AS assignment_id, a.assigned_at, a.handover_status, a.return_reason,
               cu.name AS created_by_name
        FROM inventory_items i
        LEFT JOIN inventory_assignments a ON a.item_id = i.id AND a.handover_status IN ('active','pending_handover')
        LEFT JOIN users u ON u.id = a.user_id
        LEFT JOIN users cu ON cu.id = i.created_by
        ORDER BY i.created_at DESC`);
    } else {
      [rows] = await db.query(`
        SELECT i.*, u.name AS assigned_to_name, u.id AS assigned_to_id,
               a.id AS assignment_id, a.assigned_at, a.handover_status, a.return_reason,
               cu.name AS created_by_name
        FROM inventory_items i
        JOIN inventory_assignments a ON a.item_id = i.id AND a.user_id = ? AND a.handover_status IN ('active','pending_handover')
        JOIN users u ON u.id = a.user_id
        LEFT JOIN users cu ON cu.id = i.created_by
        ORDER BY i.created_at DESC`, [req.session.userId]);
    }
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add new item (admin only)
app.post('/api/inventory/items', requireAuth, async (req, res) => {
  // Inventory's writes moved off a hardcoded ['admin','hod'] list onto the
  // panel's Editor toggle. hod carries edit_inventory in its role default, so
  // the set of people who pass is unchanged today — what changed is that an
  // admin can now widen or narrow it from Access Control instead of needing a
  // code edit.
  if (!(await userCanDo(req.session, 'edit_inventory'))) return res.status(403).json({ error: 'You do not have edit access to Inventory' });
  try {
    const { name, type, brand, model, serial_number, photo, item_condition, notes } = req.body;
    if (!name || !type) return res.status(400).json({ error: 'name and type required' });
    // Brand/model are what tell two items of the same type apart, since the
    // form has no free-text name field.
    if (!brand || !String(brand).trim()) return res.status(400).json({ error: 'Brand is required' });
    if (!model || !String(model).trim()) return res.status(400).json({ error: 'Model is required' });
    const validTypes = ['laptop','keyboard','mouse','mobile','sim','charger','other'];
    if (!validTypes.includes(type)) return res.status(400).json({ error: 'Invalid type' });
    const [r] = await db.query(
      `INSERT INTO inventory_items (name,type,brand,model,serial_number,photo,item_condition,notes,created_by)
       VALUES (?,?,?,?,?,?,?,?,?)`,
      [name, type, brand||'', model||'', serial_number||'', photo||null, item_condition||'good', notes||'', req.session.userId]);
    res.json({ ok: true, id: r.insertId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Self-report equipment an employee already has — creates the item and
// immediately assigns it to the reporting user, no admin approval step.
app.post('/api/inventory/self-add', requireAuth, async (req, res) => {
  try {
    const { name, type, brand, model, serial_number, photo, item_condition, notes } = req.body;
    if (!name || !type) return res.status(400).json({ error: 'name and type required' });
    // Brand/model are what tell two items of the same type apart, since the
    // form has no free-text name field.
    if (!brand || !String(brand).trim()) return res.status(400).json({ error: 'Brand is required' });
    if (!model || !String(model).trim()) return res.status(400).json({ error: 'Model is required' });
    const validTypes = ['laptop','keyboard','mouse','mobile','sim','charger','other'];
    if (!validTypes.includes(type)) return res.status(400).json({ error: 'Invalid type' });
    const [r] = await db.query(
      `INSERT INTO inventory_items (name,type,brand,model,serial_number,photo,item_condition,notes,status,created_by)
       VALUES (?,?,?,?,?,?,?,?,'assigned',?)`,
      [name, type, brand||'', model||'', serial_number||'', photo||null, item_condition||'good', notes||'', req.session.userId]);
    await db.query(
      `INSERT INTO inventory_assignments (item_id, user_id, assigned_by) VALUES (?,?,?)`,
      [r.insertId, req.session.userId, req.session.userId]);
    res.json({ ok: true, id: r.insertId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Update item (admin only)
app.put('/api/inventory/items/:id', requireAuth, async (req, res) => {
  // Inventory's writes moved off a hardcoded ['admin','hod'] list onto the
  // panel's Editor toggle. hod carries edit_inventory in its role default, so
  // the set of people who pass is unchanged today — what changed is that an
  // admin can now widen or narrow it from Access Control instead of needing a
  // code edit.
  if (!(await userCanDo(req.session, 'edit_inventory'))) return res.status(403).json({ error: 'You do not have edit access to Inventory' });
  try {
    const { name, brand, model, serial_number, photo, item_condition, status, notes } = req.body;
    await db.query(
      `UPDATE inventory_items SET name=COALESCE(?,name), brand=COALESCE(?,brand), model=COALESCE(?,model),
       serial_number=COALESCE(?,serial_number), photo=COALESCE(?,photo), item_condition=COALESCE(?,item_condition),
       status=COALESCE(?,status), notes=COALESCE(?,notes) WHERE id=?`,
      [name||null, brand||null, model||null, serial_number||null, photo||null,
       item_condition||null, status||null, notes||null, req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete item (admin only, only if not currently assigned)
app.delete('/api/inventory/items/:id', requireAuth, async (req, res) => {
  if (req.session.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
  try {
    const [[item]] = await db.query('SELECT * FROM inventory_items WHERE id=?', [req.params.id]);
    if (!item) return res.status(404).json({ error: 'Not found' });
    if (item.status === 'assigned') return res.status(400).json({ error: 'Cannot delete an assigned item. Return it first.' });
    await archiveDeleted('inventory_items', item, req, {
      summary: r => `Equipment: ${r.name || ''} (${r.type || ''})${r.serial_number ? ' SN:' + r.serial_number : ''}`,
    });
    await db.query('DELETE FROM inventory_items WHERE id=?', [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Assign item to user (admin only)
app.post('/api/inventory/assign', requireAuth, async (req, res) => {
  // Inventory's writes moved off a hardcoded ['admin','hod'] list onto the
  // panel's Editor toggle. hod carries edit_inventory in its role default, so
  // the set of people who pass is unchanged today — what changed is that an
  // admin can now widen or narrow it from Access Control instead of needing a
  // code edit.
  if (!(await userCanDo(req.session, 'edit_inventory'))) return res.status(403).json({ error: 'You do not have edit access to Inventory' });
  try {
    const { item_id, user_id } = req.body;
    if (!item_id || !user_id) return res.status(400).json({ error: 'item_id and user_id required' });
    const [[item]] = await db.query('SELECT status FROM inventory_items WHERE id=?', [item_id]);
    if (!item) return res.status(404).json({ error: 'Item not found' });
    if (item.status === 'assigned') return res.status(400).json({ error: 'Item already assigned' });
    await db.query(
      `INSERT INTO inventory_assignments (item_id, user_id, assigned_by) VALUES (?,?,?)`,
      [item_id, user_id, req.session.userId]);
    await db.query(`UPDATE inventory_items SET status='assigned' WHERE id=?`, [item_id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Get all assignments (admin/hod)
app.get('/api/inventory/assignments', requireAuth, async (req, res) => {
  // Inventory's writes moved off a hardcoded ['admin','hod'] list onto the
  // panel's Editor toggle. hod carries edit_inventory in its role default, so
  // the set of people who pass is unchanged today — what changed is that an
  // admin can now widen or narrow it from Access Control instead of needing a
  // code edit.
  if (!(await userCanDo(req.session, 'edit_inventory'))) return res.status(403).json({ error: 'You do not have edit access to Inventory' });
  try {
    const [rows] = await db.query(`
      SELECT a.*, i.name AS item_name, i.type AS item_type, i.brand, i.model, i.serial_number, i.photo,
             u.name AS user_name, u.department,
             ab.name AS assigned_by_name
      FROM inventory_assignments a
      JOIN inventory_items i ON i.id = a.item_id
      JOIN users u ON u.id = a.user_id
      LEFT JOIN users ab ON ab.id = a.assigned_by
      ORDER BY a.assigned_at DESC`);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Initiate handover — flags the item as pending return.
// Two ways in: an admin/HOD starting the handover when someone leaves, or the
// holder themselves saying "I'm giving this back" from My Equipment. Either
// way it only raises the intent; an admin still has to confirm physical
// receipt via /return, so this is deliberately NOT admin-only.
app.post('/api/inventory/handover/:assignment_id', requireAuth, async (req, res) => {
  try {
    const { notes, reason } = req.body;
    if (!invReturnReason(reason)) {
      return res.status(400).json({ error: 'A valid return reason is required' });
    }
    const [[a]] = await db.query('SELECT * FROM inventory_assignments WHERE id=?', [req.params.assignment_id]);
    if (!a) return res.status(404).json({ error: 'Assignment not found' });
    const isAdmin = ['admin','hod'].includes(req.session.role);
    if (!isAdmin && a.user_id !== req.session.userId) {
      return res.status(403).json({ error: 'You can only return equipment assigned to you' });
    }
    if (!isAdmin && !INV_HOLDER_REASONS.has(reason)) {
      return res.status(403).json({ error: 'Only an admin can retire an item' });
    }
    if (a.handover_status !== 'active') {
      return res.status(400).json({ error: 'This assignment is not active' });
    }
    await db.query(
      `UPDATE inventory_assignments SET handover_status='pending_handover', handover_notes=?, return_reason=? WHERE id=?`,
      [notes||'', reason, req.params.assignment_id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Complete handover — admin confirms physical receipt.
// The reason is the admin's final call (it may correct whatever the holder
// claimed when they raised the return) and decides where the item lands:
// damaged/retired take it out of circulation so it can't be assigned again,
// offboarding puts it back in available stock.
app.post('/api/inventory/return/:assignment_id', requireAuth, async (req, res) => {
  // Inventory's writes moved off a hardcoded ['admin','hod'] list onto the
  // panel's Editor toggle. hod carries edit_inventory in its role default, so
  // the set of people who pass is unchanged today — what changed is that an
  // admin can now widen or narrow it from Access Control instead of needing a
  // code edit.
  if (!(await userCanDo(req.session, 'edit_inventory'))) return res.status(403).json({ error: 'You do not have edit access to Inventory' });
  try {
    const { reason } = req.body;
    const mapped = invReturnReason(reason);
    if (!mapped) return res.status(400).json({ error: 'A valid return reason is required' });
    const [[a]] = await db.query('SELECT * FROM inventory_assignments WHERE id=?', [req.params.assignment_id]);
    if (!a) return res.status(404).json({ error: 'Assignment not found' });
    await db.query(
      `UPDATE inventory_assignments SET handover_status='returned', returned_at=NOW(), return_reason=? WHERE id=?`,
      [reason, req.params.assignment_id]);
    await db.query(`UPDATE inventory_items SET status=? WHERE id=?`, [mapped.itemStatus, a.item_id]);
    res.json({ ok: true, itemStatus: mapped.itemStatus });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
};
