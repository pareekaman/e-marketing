// ══════════════════════════════════════════════════════
// PAYMENT REQUESTS — cards, submission, approval, WhatsApp debug
// ══════════════════════════════════════════════════════
// Lifted out of server.js unchanged — both slices below are byte-for-byte
// what lived there.
//
// This group was NOT contiguous: /api/mdo-tasks (an unrelated WhatsApp-bot
// intake queue that reads the tasks table) sat between the main block and the
// wa-debug route, so the two were lifted separately and joined here in their
// original order. wa-debug therefore registers before /api/mdo-tasks now
// rather than after it, which changes nothing: Express order only matters
// between routes that can match the same request, and no /api/mdo-tasks or
// /api/feedback path can match /api/payment-requests/:id/wa-debug.
//
// Dependencies are passed in rather than re-required: these must be the SAME
// instances server.js uses. db in particular carries the max_user_connections
// retry wrapper.
module.exports = function registerPaymentRequestRoutes(app, deps) {
  const {
    db,
    requireAuth,
    requireAdmin,
    archiveDeleted,
    emailUserWaText,
    isPaymentApprover,
    sendWhatsApp,
  } = deps;

// GET /api/payment-requests/cards — card list for dropdown (all logged-in users)
app.get('/api/payment-requests/cards', requireAuth, async (req, res) => {
  try {
    // Merge manually-managed pr_cards + any cc_cards from PDF uploads
    const [rows] = await db.query(`
      SELECT bank_name, card_number, id, 'manual' AS src FROM pr_cards
      UNION
      SELECT bank_name, card_number, id, 'cc' AS src FROM cc_cards
      ORDER BY bank_name, card_number`);
    // Deduplicate by bank+card (prefer manual entry so id is available for delete)
    const seen = new Map();
    for (const r of rows) {
      const key = `${r.bank_name}|${r.card_number}`;
      if (!seen.has(key) || r.src === 'manual') seen.set(key, r);
    }
    res.json(Array.from(seen.values()));
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /api/payment-requests/cards — add card (Naman only)
app.post('/api/payment-requests/cards', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const { bank_name, card_number } = req.body;
    if (!bank_name || !card_number) return res.status(400).json({ error:'bank_name and card_number required' });
    await db.query('INSERT IGNORE INTO pr_cards (bank_name, card_number) VALUES (?,?)', [bank_name.trim(), card_number.trim()]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// DELETE /api/payment-requests/cards/:id — remove card (Naman only, pr_cards only)
app.delete('/api/payment-requests/cards/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const [doomed] = await db.query('SELECT * FROM pr_cards WHERE id=?', [req.params.id]);
    await archiveDeleted('pr_cards', doomed, req, {
      summary: r => `PR card: ${r.bank_name || ''} ${r.card_number || ''}`,
    });
    await db.query('DELETE FROM pr_cards WHERE id=?', [req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// DELETE /api/payment-requests/:id — hard delete row + its sentinels (admin only, temporary cleanup)
// Readable one-liner for an archived payment_requests row.
// The reason column is not plain text: the frontend packs the amount into it
// as "[<currency><amount>] <real reason>" (see prParseReason in app.html), and
// bill/paid sentinels are stored as fake rows under bank_name '__system__'.
// Mirror that here so the Logs page doesn't show the amount twice or print a
// raw sentinel string.
function prSummary(r) {
  const raw = String(r.reason || '');
  if (r.bank_name === '__system__') return `Payment request sentinel: ${raw}`;
  let amount = r.amount ?? '';
  let reason = raw;
  if (raw.charAt(0) === '[') {
    const close = raw.indexOf('] ');
    if (close > 1) {
      const inner = raw.slice(1, close);
      const num = parseFloat(inner.slice(1).replace(/,/g, ''));
      if (!isNaN(num) && num >= 0) { amount = inner; reason = raw.slice(close + 2); }
    }
  }
  return `Payment request: ${amount}${reason ? ' — ' + reason : ''}`;
}

app.delete('/api/payment-requests/:id', requireAuth, async (req, res) => {
  try {
    if (req.session.role !== 'admin') return res.status(403).json({ error:'Access denied' });
    const id = req.params.id;
    const [doomed] = await db.query(
      'SELECT * FROM payment_requests WHERE id=? OR (bank_name=\'__system__\' AND reason LIKE ?)', [id, `%:${id}%`]);
    await archiveDeleted('payment_requests', doomed, req, { summary: prSummary });
    await db.query('DELETE FROM payment_requests WHERE id=? OR (bank_name=\'__system__\' AND reason LIKE ?)', [id, `%:${id}%`]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /api/payment-requests — submit new request (all logged-in users)
app.post('/api/payment-requests', requireAuth, async (req, res) => {
  try {
    const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
    if (!me) return res.status(403).json({ error:'Access denied' });
    const { bank_name, card_number, amount, reason } = req.body;
    if (!bank_name || !card_number || !reason) return res.status(400).json({ error:'All fields required' });
    // Paid / cancelled / bill markers ride in on this same route as "__system__"
    // sentinel rows carrying the target request id in their reason. Marking a
    // request paid is the requester's own record-keeping step by design (see the
    // note above payStatusCell in app.html), and the UI only ever offers the
    // buttons on rows the caller already owns — but that scoping lived purely in
    // the client. Nothing here stopped any logged-in user from posting a sentinel
    // against somebody else's request. Re-check ownership server-side: the
    // submitter may mark their own, and a payment approver (admins included, via
    // the bypass in isPaymentApprover) may mark anyone's, which is what actually
    // happens today for most rows.
    if (bank_name === '__system__') {
      const m = /^__(paid|cancelled|bill)__:(\d+)(?::|$)/.exec(String(reason));
      if (!m || card_number !== `__${m[1]}__`) {
        return res.status(400).json({ error: 'Malformed payment marker' });
      }
      const [[target]] = await db.query('SELECT submitted_by FROM payment_requests WHERE id=?', [m[2]]);
      if (!target) return res.status(404).json({ error: 'Payment request not found' });
      if (Number(target.submitted_by) !== Number(req.session.userId)
          && !(await isPaymentApprover(req.session))) {
        return res.status(403).json({ error: 'You can only update your own payment requests' });
      }
    }
    try {
      await db.query(
        'INSERT INTO payment_requests (submitted_by, name, bank_name, card_number, amount, reason) VALUES (?,?,?,?,?,?)',
        [req.session.userId, me.name, bank_name, card_number, parseFloat(amount)||0, reason]
      );
    } catch(insertErr) {
      // Fallback if amount column not yet migrated (server not restarted)
      await db.query(
        'INSERT INTO payment_requests (submitted_by, name, bank_name, card_number, reason) VALUES (?,?,?,?,?)',
        [req.session.userId, me.name, bank_name, card_number, reason]
      );
    }
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Helper: pass the row to the client untouched.
// This used to un-pack the "[<currency><amount>]" prefix out of the reason and
// move the number into `amount`. Doing so threw away the currency symbol, so
// every request came back with a bare number and the client always rendered ₹,
// even for rows submitted in $ (e.g. "[$100.00] Claude Subscription"). The
// client parses the encoded reason itself (prParseReason in app.html) to get
// amount + currency, so it needs the reason raw. Keep this as a pass-through.
function parsePrRow(row) {
  return row;
}

// GET /api/payment-requests — all requests (admin + payment approvers)
app.get('/api/payment-requests', requireAuth, async (req, res) => {
  try {
    if (!(await isPaymentApprover(req.session))) return res.status(403).json({ error:'Access denied' });
    const [rows] = await db.query(
      'SELECT * FROM payment_requests ORDER BY created_at DESC'
    );
    res.json(rows.map(parsePrRow));
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// GET /api/payment-requests/my — own requests (any logged-in user)
app.get('/api/payment-requests/my', requireAuth, async (req, res) => {
  try {
    const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
    if (!me) return res.status(403).json({ error:'Access denied' });
    const [rows] = await db.query(
      'SELECT * FROM payment_requests WHERE submitted_by=? ORDER BY created_at DESC',
      [req.session.userId]
    );
    // Payment-done/cancelled/bill markers are stored as separate "__system__" sentinel
    // rows submitted by whoever actioned them (usually an admin, not this employee), so
    // the submitted_by filter above misses them — without this the employee's payment
    // status stays stuck on "Pending" forever even after an admin marks it paid. Pull in
    // only the sentinels that reference one of this employee's own request ids.
    const myIds = new Set(rows.map(r => String(r.id)));
    if (myIds.size) {
      const [sentinelRows] = await db.query(
        `SELECT * FROM payment_requests WHERE bank_name='__system__'`
      );
      const mySentinels = sentinelRows.filter(s => {
        const match = /^__(?:paid|cancelled|bill)__:(\d+)/.exec(s.reason || '');
        return match && myIds.has(match[1]);
      });
      rows.push(...mySentinels);
    }
    res.json(rows.map(parsePrRow));
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// PATCH /api/payment-requests/:id — approve or reject (admin + payment approvers)
app.patch('/api/payment-requests/:id', requireAuth, async (req, res) => {
  try {
    if (!(await isPaymentApprover(req.session))) return res.status(403).json({ error:'Access denied' });
    const { status } = req.body;
    if (!['approved','rejected'].includes(status)) return res.status(400).json({ error:'Invalid status' });
    await db.query(
      'UPDATE payment_requests SET status=?, reviewed_at=NOW() WHERE id=?',
      [status, req.params.id]
    );
    res.json({ success: true });
    // WhatsApp notification is fire-and-forget, matching the pattern used for other
    // approval flows (mdo-tasks, leave requests, meetings) — the approve/reject
    // response no longer waits on the WhatsApp round trip.
    (async () => {
      try {
        const [[pr]] = await db.query('SELECT submitted_by, reason FROM payment_requests WHERE id=?', [req.params.id]);
        if (pr && pr.submitted_by) {
          const [[submitter]] = await db.query('SELECT name FROM users WHERE id=?', [pr.submitted_by]);
          if (submitter) {
            const emoji = status === 'approved' ? '✅' : '❌';
            const statusText = status === 'approved' ? 'Approved' : 'Rejected';
            let amtStr = '', cleanReason = pr.reason || '';
            if (pr.reason) {
              const s = String(pr.reason);
              if (s.charAt(0) === '[') {
                const close = s.indexOf('] ');
                if (close > 1) {
                  const inner = s.slice(1, close);
                  const num = parseFloat(inner.slice(1).replace(/,/g, ''));
                  if (!isNaN(num)) {
                    amtStr = `\n*Amount:* ${inner.charAt(0)}${num.toFixed(2)}`;
                    cleanReason = s.slice(close + 2);
                  }
                }
              }
            }
            const msg = `${emoji} *Payment Request ${statusText}*\n\nHi ${submitter.name},\n\nYour payment request has been *${statusText.toLowerCase()}*.${amtStr}\n*Reason:* ${cleanReason}\n\n— E-Marketing`;
            await emailUserWaText(pr.submitted_by, `Payment Request ${statusText}`, msg);
          }
        }
      } catch(waErr) { console.error('payment notify email err:', waErr.message); }
    })();
  } catch(err) { res.status(500).json({ error: err.message }); }
});


// GET /api/payment-requests/:id/wa-debug
app.get('/api/payment-requests/:id/wa-debug', requireAuth, requireAdmin, async (req, res) => {
  try {
    const [[pr]] = await db.query('SELECT id, submitted_by, reason, status FROM payment_requests WHERE id=?', [req.params.id]);
    if (!pr) return res.json({ error: 'request not found' });
    let submitter = null;
    if (pr.submitted_by) {
      [[submitter]] = await db.query('SELECT id, name, phone FROM users WHERE id=?', [pr.submitted_by]);
    }
    let waResult = null;
    if (submitter && submitter.phone) {
      waResult = await sendWhatsApp(submitter.phone, `✅ Test — Payment Request #${pr.id} WA debug`);
    }
    res.json({ pr, submitter, waResult });
  } catch(e) { res.status(500).json({ error: e.message }); }
});
};
