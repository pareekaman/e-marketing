// ══════════════════════════════════════════════════════
// MEETINGS / SCHEDULER ROUTES
// ══════════════════════════════════════════════════════
// Lifted out of server.js unchanged — the route bodies below are byte-for-byte
// what lived there, so this file versus that block is an empty diff.
//
// Everything the block reached for at module scope is passed in rather than
// re-required, because these must be the SAME instances server.js uses: db
// carries the max_user_connections retry wrapper, and sendMeetingNotification
// is also called by the 10-minute pre-meeting reminder cron.
//
// generateRecurrenceDates and RECURRENCE_MAX_OCCURRENCES moved with the routes;
// grep confirmed every reference to either was inside this block.
module.exports = function registerMeetingRoutes(app, deps) {
  const {
    db,
    requireAuth,
    buildMeetingSlots,
    createGoogleMeetLink,
    sendMeetingNotification,
    loadHolidaysSet,
    isLastSaturdayOfMonth,
  } = deps;

// List meetings (filter by date range / status / organizer).
// Every user is scoped to meetings they organize or are invited to — nobody
// sees someone else's private meetings here.
app.get('/api/meetings', requireAuth, async (req, res) => {
  try {
    const uid = req.session.userId;
    const { from, to, status, organizer } = req.query;
    let where = `(m.organizer_id = ? OR EXISTS
      (SELECT 1 FROM meeting_attendees ma WHERE ma.meeting_id = m.id AND ma.user_id = ?))`;
    const params = [uid, uid];
    if (from && /^\d{4}-\d{2}-\d{2}$/.test(from)) { where += ' AND m.meeting_date >= ?'; params.push(from); }
    if (to   && /^\d{4}-\d{2}-\d{2}$/.test(to))   { where += ' AND m.meeting_date <= ?'; params.push(to); }
    if (status) { where += ' AND m.status = ?'; params.push(status); }
    if (organizer && organizer !== 'all') { where += ' AND m.organizer_id = ?'; params.push(organizer); }
    const [rows] = await db.query(
      `SELECT m.id, m.title, m.agenda, m.client_id, m.organizer_id,
              DATE_FORMAT(m.meeting_date,'%Y-%m-%d') AS meeting_date,
              TIME_FORMAT(m.start_time,'%H:%i') AS start_time,
              TIME_FORMAT(m.end_time,'%H:%i')   AS end_time,
              m.meet_link, m.status, m.created_at,
              c.name AS client_name, u.name AS organizer_name
       FROM meetings m
       LEFT JOIN clients c ON m.client_id = c.id
       LEFT JOIN users   u ON m.organizer_id = u.id
       WHERE ${where}
       ORDER BY m.meeting_date ASC, m.start_time ASC
       LIMIT 500`, params);
    if (rows.length) {
      const ids = rows.map(r => r.id);
      const [atts] = await db.query(
        `SELECT ma.meeting_id, ma.user_id, u.name
         FROM meeting_attendees ma JOIN users u ON ma.user_id = u.id
         WHERE ma.meeting_id IN (${ids.map(()=>'?').join(',')})`, ids);
      const byMtg = {};
      for (const a of atts) (byMtg[a.meeting_id] = byMtg[a.meeting_id] || []).push({ id: a.user_id, name: a.name });
      for (const r of rows) r.attendees = byMtg[r.id] || [];
    }
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Slot map for a given date — used by the scheduler UI.
app.get('/api/meetings/slots', requireAuth, async (req, res) => {
  try {
    const date = req.query.date;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '')) return res.status(400).json({ error: 'date=YYYY-MM-DD required' });
    const userIds = String(req.query.userIds || '')
      .split(',').map(s => parseInt(s, 10)).filter(n => Number.isFinite(n) && n > 0);
    // Off-day check (Sunday + holidays) — return empty slots with a reason.
    const off = await (async () => {
      try {
        const holidays = await loadHolidaysSet();
        const d = new Date(date + 'T00:00:00');
        if (d.getDay() === 0) return { off: true, reason: 'Sunday' };
        if (isLastSaturdayOfMonth(date)) return { off: true, reason: 'Last Saturday (off)' };
        if (holidays.has(date)) return { off: true, reason: 'Holiday' };
        return { off: false };
      } catch { return { off: false }; }
    })();
    if (off.off) return res.json({ date, off: true, reason: off.reason, slots: [], busyRanges: {} });
    const { slots, busyRanges } = await buildMeetingSlots(date, userIds, req.session.userId);
    res.json({ date, off: false, slots, busyRanges });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Single meeting detail.
app.get('/api/meetings/:id', requireAuth, async (req, res) => {
  try {
    const [[m]] = await db.query(
      `SELECT m.id, m.title, m.agenda, m.client_id, m.organizer_id,
              DATE_FORMAT(m.meeting_date,'%Y-%m-%d') AS meeting_date,
              TIME_FORMAT(m.start_time,'%H:%i') AS start_time,
              TIME_FORMAT(m.end_time,'%H:%i')   AS end_time,
              m.meet_link, m.status,
              c.name AS client_name, u.name AS organizer_name
       FROM meetings m
       LEFT JOIN clients c ON m.client_id = c.id
       LEFT JOIN users   u ON m.organizer_id = u.id
       WHERE m.id = ?`, [req.params.id]);
    if (!m) return res.status(404).json({ error: 'not found' });
    const [atts] = await db.query(
      `SELECT u.id, u.name, u.email FROM meeting_attendees ma
       JOIN users u ON ma.user_id = u.id WHERE ma.meeting_id = ?`, [req.params.id]);
    m.attendees = atts;
    res.json(m);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Create meeting + notify.
// Expands a recurrence rule into the list of meeting_date strings it covers.
// Capped so a mistyped "repeat until 2099" can't spawn thousands of rows.
const RECURRENCE_MAX_OCCURRENCES = 120;
function generateRecurrenceDates(startDateStr, frequency, untilStr, customDays) {
  const start = new Date(startDateStr + 'T00:00:00Z');
  const until = new Date(untilStr + 'T00:00:00Z');
  const dates = [];
  if (until < start) return dates;
  if (frequency === 'monthly') {
    const cur = new Date(start);
    while (cur <= until && dates.length < RECURRENCE_MAX_OCCURRENCES) {
      dates.push(cur.toISOString().split('T')[0]);
      cur.setUTCMonth(cur.getUTCMonth() + 1);
    }
    return dates;
  }
  const customSet = new Set((customDays || []).map(d => parseInt(d, 10)));
  const startDow = start.getUTCDay();
  const cur = new Date(start);
  while (cur <= until && dates.length < RECURRENCE_MAX_OCCURRENCES) {
    const dow = cur.getUTCDay(); // 0=Sun..6=Sat
    let include = false;
    if (frequency === 'daily') include = true;
    else if (frequency === 'weekday') include = dow !== 0; // Mon–Sat
    else if (frequency === 'weekly') include = dow === startDow;
    else if (frequency === 'custom') include = customSet.has(dow);
    if (include) dates.push(cur.toISOString().split('T')[0]);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return dates;
}

app.post('/api/meetings', requireAuth, async (req, res) => {
  try {
    const { title, agenda, client_id, meeting_date, start_time, end_time, meet_link, attendee_ids,
            frequency, repeat_until, repeat_days } = req.body;
    if (!title || !meeting_date || !start_time || !end_time) {
      return res.status(400).json({ error: 'title, meeting_date, start_time, end_time required' });
    }
    const organizerId = req.session.userId;

    const freq = ['daily','weekday','weekly','monthly','custom'].includes(frequency) ? frequency : null;
    let occurrenceDates = [meeting_date];
    if (freq) {
      if (!repeat_until) return res.status(400).json({ error: 'Pick a "repeat until" date for a recurring meeting' });
      if (freq === 'custom' && !(Array.isArray(repeat_days) && repeat_days.length)) {
        return res.status(400).json({ error: 'Select at least one day to repeat on' });
      }
      occurrenceDates = generateRecurrenceDates(meeting_date, freq, repeat_until, repeat_days);
      if (!occurrenceDates.length) return res.status(400).json({ error: 'No occurrences fall in the selected range' });
    }

    // Auto-create Google Meet link if env enabled and caller didn't paste one — reused across every occurrence.
    let finalLink = meet_link || null;
    if (!finalLink) {
      const attEmails = [];
      const attIdArr = Array.isArray(attendee_ids) ? attendee_ids : [];
      if (attIdArr.length) {
        const [emails] = await db.query(
          `SELECT email FROM users WHERE id IN (${attIdArr.map(()=>'?').join(',')})`, attIdArr);
        for (const r of emails) if (r.email) attEmails.push(r.email);
      }
      finalLink = await createGoogleMeetLink({
        title, dateStr: meeting_date, startTime: start_time, endTime: end_time,
        attendeeEmails: attEmails
      });
    }

    const recurrenceGroupId = freq ? `rg_${Date.now()}_${organizerId}` : null;
    const newIds = [];
    for (const d of occurrenceDates) {
      const [result] = await db.query(
        `INSERT INTO meetings (title, agenda, client_id, organizer_id, meeting_date, start_time, end_time, meet_link, recurrence_group_id)
         VALUES (?,?,?,?,?,?,?,?,?)`,
        [title, agenda || null, client_id || null, organizerId, d, start_time, end_time, finalLink, recurrenceGroupId]);
      const newId = result.insertId;
      newIds.push(newId);
      if (Array.isArray(attendee_ids) && attendee_ids.length) {
        const values = attendee_ids.filter(n => Number.isFinite(parseInt(n))).map(uid => [newId, parseInt(uid)]);
        if (values.length) {
          await db.query(
            `INSERT IGNORE INTO meeting_attendees (meeting_id, user_id) VALUES ${values.map(()=>'(?,?)').join(',')}`,
            values.flat());
        }
      }
    }
    // One WhatsApp ping for the series (not one per occurrence) — the existing
    // 10-min pre-meeting reminder cron still fires individually for each date.
    sendMeetingNotification(newIds[0], 'created').catch(e => console.error('notify err:', e.message));
    res.json({ ok: true, id: newIds[0], meet_link: finalLink, count: newIds.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Reschedule / edit meeting + notify.
app.put('/api/meetings/:id', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const { title, agenda, client_id, meeting_date, start_time, end_time, meet_link, attendee_ids } = req.body;
    const [[existing]] = await db.query('SELECT organizer_id, meeting_date, start_time, end_time FROM meetings WHERE id=?', [id]);
    if (!existing) return res.status(404).json({ error: 'not found' });
    // Only organizer or admin can edit.
    if (existing.organizer_id !== req.session.userId && req.session.role !== 'admin') {
      return res.status(403).json({ error: 'only organizer or admin can edit' });
    }
    const rescheduled = (meeting_date && meeting_date !== String(existing.meeting_date).slice(0,10))
                    || (start_time && start_time !== String(existing.start_time).slice(0,5))
                    || (end_time   && end_time   !== String(existing.end_time).slice(0,5));
    await db.query(
      `UPDATE meetings SET
         title=COALESCE(?,title), agenda=?, client_id=?,
         meeting_date=COALESCE(?,meeting_date),
         start_time=COALESCE(?,start_time), end_time=COALESCE(?,end_time),
         meet_link=COALESCE(?,meet_link)${rescheduled ? ', reminder_sent=0' : ''}
       WHERE id=?`,
      [title, agenda || null, client_id || null, meeting_date || null, start_time || null, end_time || null, meet_link || null, id]);
    if (Array.isArray(attendee_ids)) {
      await db.query('DELETE FROM meeting_attendees WHERE meeting_id=?', [id]);
      const values = attendee_ids.filter(n => Number.isFinite(parseInt(n))).map(uid => [id, parseInt(uid)]);
      if (values.length) {
        await db.query(
          `INSERT IGNORE INTO meeting_attendees (meeting_id, user_id) VALUES ${values.map(()=>'(?,?)').join(',')}`,
          values.flat());
      }
    }
    sendMeetingNotification(id, rescheduled ? 'rescheduled' : 'created').catch(e => console.error('notify err:', e.message));
    res.json({ ok: true, rescheduled });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Change meeting status — mark done / reopen. Organizer or admin only.
app.put('/api/meetings/:id/status', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const status = String(req.body.status || '');
    if (!['scheduled', 'done', 'cancelled'].includes(status)) return res.status(400).json({ error: 'Invalid status' });
    const [[existing]] = await db.query('SELECT organizer_id FROM meetings WHERE id=?', [id]);
    if (!existing) return res.status(404).json({ error: 'not found' });
    if (existing.organizer_id !== req.session.userId && req.session.role !== 'admin') {
      return res.status(403).json({ error: 'only organizer or admin can change status' });
    }
    await db.query('UPDATE meetings SET status=? WHERE id=?', [status, id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Cancel (soft) — status flips to cancelled, notification fires.
app.delete('/api/meetings/:id', requireAuth, async (req, res) => {
  try {
    const id = req.params.id;
    const [[existing]] = await db.query('SELECT organizer_id FROM meetings WHERE id=?', [id]);
    if (!existing) return res.status(404).json({ error: 'not found' });
    if (existing.organizer_id !== req.session.userId && req.session.role !== 'admin') {
      return res.status(403).json({ error: 'only organizer or admin can cancel' });
    }
    await db.query("UPDATE meetings SET status='cancelled' WHERE id=?", [id]);
    sendMeetingNotification(id, 'cancelled').catch(e => console.error('notify err:', e.message));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
};
