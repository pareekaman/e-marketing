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

    // Departments the spend belongs to. Required on a real request, but NOT on
    // the "__system__" sentinel rows below — those are the paid / cancelled /
    // bill markers, they reuse this same route, and they carry no departments.
    // Demanding one from them would break marking a payment done and uploading
    // its bill.
    //
    // Names are sanitised rather than checked against the live department list:
    // that list is assembled in server.js from users plus a few extras, and
    // coupling to it would make a renamed department reject a submission. Cap
    // the count and length so a hand-made request cannot store junk.
    let departments = null;
    if (bank_name !== '__system__') {
      const raw = Array.isArray(req.body.departments) ? req.body.departments : [];
      const clean = [...new Set(
        raw.map(d => String(d || '').trim()).filter(d => d && d.length <= 100)
      )].slice(0, 20);
      if (!clean.length) return res.status(400).json({ error: 'Select at least one department' });
      departments = JSON.stringify(clean);
    }
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
      // A bill marker carries a Google Drive file id after the request id, and
      // the Payments screens build a Drive link from it — so accept only an id's
      // characters, never markup that would end up inside that link.
      if (m[1] === 'bill' && !/^__bill__:\d+:[A-Za-z0-9_-]{10,200}$/.test(String(reason))) {
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
        'INSERT INTO payment_requests (submitted_by, name, bank_name, card_number, amount, reason, departments) VALUES (?,?,?,?,?,?,?)',
        [req.session.userId, me.name, bank_name, card_number, parseFloat(amount)||0, reason, departments]
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
// PUT /api/payment-requests/:id — fix a request that was filled in wrong.
//
// Separate from the PATCH below on purpose. That route decides a request
// (approve / reject) and is gated on isPaymentApprover; this one corrects the
// contents of one still awaiting that decision, and answers to a different
// question entirely — who owns the row. Folding both into PATCH would put two
// unrelated permission models behind one endpoint, where widening either by
// accident silently widens the other.
//
// Who: the submitter, for their own. An admin, for anyone's. Deliberately NOT
// isPaymentApprover — approving somebody's spend is not the same right as
// rewriting what they asked for, and an approver who wants a change can reject
// and say why.
//
// When: only while status is 'pending'. Once a request is approved or
// rejected the decision was made against these exact numbers, so the numbers
// stop moving — a rejected one is corrected by submitting a fresh request, not
// by editing history.
app.put('/api/payment-requests/:id', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const [[row]] = await db.query(
      'SELECT id, submitted_by, status, bank_name FROM payment_requests WHERE id=?', [id]);
    if (!row) return res.status(404).json({ error: 'Payment request not found' });

    // The paid / cancelled / bill markers are rows on this same table. They are
    // not requests, carry no editable fields, and their `reason` is a parsed
    // instruction — letting this route near one would corrupt the marker.
    if (row.bank_name === '__system__') {
      return res.status(400).json({ error: 'Not an editable request' });
    }

    const isOwner = Number(row.submitted_by) === Number(req.session.userId);
    if (!isOwner && req.session.role !== 'admin') {
      return res.status(403).json({ error: 'You can only edit your own payment requests' });
    }
    if (row.status !== 'pending') {
      return res.status(400).json({ error: `Already ${row.status} — this request can no longer be edited` });
    }

    const { bank_name, card_number, amount, reason } = req.body;
    const bank = String(bank_name || '').trim();
    const card = String(card_number || '').trim();
    const amt  = parseFloat(amount);
    const why  = String(reason || '').trim();
    if (!bank || !card || !why) return res.status(400).json({ error: 'All fields required' });
    if (!Number.isFinite(amt) || amt <= 0) return res.status(400).json({ error: 'Enter a valid amount' });

    // Same sanitising as the create route — trimmed, de-duplicated, capped —
    // and deliberately not validated against the live department list, so a
    // renamed department never blocks a correction.
    const raw = Array.isArray(req.body.departments) ? req.body.departments : [];
    const departments = [...new Set(
      raw.map(d => String(d || '').trim()).filter(d => d && d.length <= 100)
    )].slice(0, 20);
    if (!departments.length) return res.status(400).json({ error: 'Select at least one department' });

    // ⚠️ The amount lives in TWO places: the `amount` column and, encoded with
    // its currency, the front of `reason` as "[₹310.00] …". Display prefers the
    // encoded copy and the approval notification parses it, so writing one
    // without the other leaves the table and the email disagreeing about how
    // much money this is. `reason` arrives already encoded from the client,
    // exactly as it does on create; the column is written from the same number.
    // `AND status='pending'` closes the gap between the check above and this
    // write: an approval landing in between must not let the numbers change
    // under a decision that has already been made.
    const [upd] = await db.query(
      "UPDATE payment_requests SET bank_name=?, card_number=?, amount=?, reason=?, departments=? WHERE id=? AND status='pending'",
      [bank, card, amt, why, JSON.stringify(departments), id]
    );
    if (!upd.affectedRows) return res.status(409).json({ error: 'This request was just decided and can no longer be edited' });
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/payment-requests/:id', requireAuth, async (req, res) => {
  try {
    if (!(await isPaymentApprover(req.session))) return res.status(403).json({ error:'Access denied' });
    const { status } = req.body;
    if (!['approved','rejected'].includes(status)) return res.status(400).json({ error:'Invalid status' });
    const [[row]] = await db.query(
      'SELECT submitted_by, bank_name FROM payment_requests WHERE id=?', [req.params.id]);
    if (!row || row.bank_name === '__system__') return res.status(404).json({ error: 'Payment request not found' });
    // Nobody approves their own spend — another approver has to. Rejecting your
    // own request (withdrawing it) stays allowed.
    if (status === 'approved' && Number(row.submitted_by) === Number(req.session.userId)) {
      return res.status(403).json({ error: 'You cannot approve your own payment request — another approver has to' });
    }
    // Only a pending request is decided, and only once: a double click or a
    // second approver acting on a stale screen changes nothing and sends no
    // second notification.
    const [upd] = await db.query(
      "UPDATE payment_requests SET status=?, reviewed_at=NOW() WHERE id=? AND status='pending'",
      [status, req.params.id]
    );
    if (!upd.affectedRows) return res.status(409).json({ error: 'This request has already been decided' });
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
