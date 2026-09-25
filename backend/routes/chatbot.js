// ══════════════════════════════════════════════════════
// TASK ASSISTANT — a rule-based chat box for Admin / HOD / PC
// ══════════════════════════════════════════════════════
// Answers typed questions like "How many tasks are pending for Naman Gupta?"
// There is no AI behind it: the message is matched against the names of the
// people the asker is allowed to see, and a fixed query supplies the numbers.
// Nothing leaves the server and there is no per-question cost.
//
// Who can ask about whom mirrors GET /api/tasks:
//   admin / pc — everyone
//   hod        — people in their own department
// A name outside that scope is simply never matched, so the bot cannot be
// used to read anyone the asker could not already see on the Tasks page.
//
// "Pending" is the dashboard's definition, so the two numbers agree:
// status 'pending' AND (no due date OR due today or earlier). Delegation
// tasks due later are reported separately as "upcoming".
//
// MIS score questions ("Rahul ka last week ka MIS score") answer with the
// same numbers as the MIS page's Delegation and Checklist tabs: the same
// query shape as GET /api/mis, the shared scoreFor() formula, and the same
// weekly planned-vs-actual average. They are gated like the MIS page
// (requireMisViewer): admin or HOD holding the 'mis' page, so a PC is told
// they have no access. FMS is left out — it reads Google Sheets live.
module.exports = function registerChatbotRoutes(app, deps) {
  const {
    db,
    requireAuth,
    requireAdminOrHod,
    userCanSee,
    scoreFor,
    istMondayOf,
    addDays,
    weeklyPlanVsActual,
  } = deps;

  const MAX_MESSAGE = 300;
  const HELP = 'Ask me about someone\'s tasks, for example:\n' +
    '"How many tasks are pending for Naman Gupta?"\n' +
    '"Naman Gupta\'s MIS score last week"';

  // Any of these makes the question an MIS-score question.
  const MIS_WORDS = ['mis', 'score', 'scores', 'performance', 'rating'];

  // Words that ask for something this version cannot answer yet. Checked so
  // "completed tasks of Naman" gets an honest "not yet" rather than a pending
  // count that looks like an answer to the question asked.
  const UNSUPPORTED = [
    'completed', 'complete', 'done', 'finished', 'closed', 'report', 'rank',
    'leave', 'leaves', 'attendance', 'holiday', 'salary',
    'fms', 'meeting', 'meetings', 'client', 'clients',
  ];
  // Time words make sense for an MIS score but not for pending tasks, which
  // are a count of right now: "pending last week" would otherwise be answered
  // as "pending today".
  const TIME_WORDS = ['last', 'yesterday', 'week', 'month', 'kal', 'pichle', 'pichhle',
    'pichla', 'pichhla', 'previous', 'hafte', 'hafta', 'mahine', 'mahina'];

  const norm = s => String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();

  // Scores one user against the message. A full-name hit beats any partial
  // one; otherwise each name word (3+ letters, so "Ji" or initials cannot
  // match by accident) found as a whole word counts once.
  function scoreUser(msgNorm, msgWords, user) {
    const full = norm(user.name);
    if (!full) return 0;
    if ((' ' + msgNorm + ' ').includes(' ' + full + ' ')) return 1000 + full.length;
    let hits = 0;
    for (const w of full.split(' ')) {
      if (w.length >= 3 && msgWords.has(w)) hits++;
    }
    return hits;
  }

  async function usersInScope(session) {
    let sql = `SELECT id, name, department FROM users
               WHERE role <> 'client' AND client_id IS NULL`;
    const params = [];
    if (session.role === 'hod') {
      const [[me]] = await db.query('SELECT department FROM users WHERE id=?', [session.userId]);
      sql += ' AND department = ?';
      params.push(me?.department || '');
    }
    const [rows] = await db.query(sql + ' ORDER BY name ASC', params);
    return rows;
  }

  // Each section lists at most this many tasks and says how many more there
  // are, so a long checklist backlog cannot turn one reply into a wall.
  const SECTION_LIMIT = 10;
  const MAX_DESC = 120;

  // Every pending task of one person. Checklist rows due later are left out:
  // recurring checklists are created weeks ahead, and listing them would bury
  // the work that is actually due. Delegation tasks due later are kept, as
  // their own "Due later" section.
  async function pendingTasksFor(userId) {
    const rows = [];
    for (const [type, table] of [['Delegation', 'delegation_tasks'], ['Checklist', 'checklist_tasks']]) {
      const [r] = await db.query(
        `SELECT t.description, DATE_FORMAT(t.due_date, '%d-%m-%Y') AS due,
                DATEDIFF(CURDATE(), t.due_date) AS late, u.name AS by_name
         FROM ${table} t LEFT JOIN users u ON u.id = t.assigned_by
         WHERE t.assigned_to = ? AND t.status = 'pending'
         ${type === 'Checklist' ? 'AND (t.due_date IS NULL OR t.due_date <= CURDATE())' : ''}
         ORDER BY t.due_date IS NULL, t.due_date ASC`, [userId]);
      for (const x of r) rows.push({ ...x, type });
    }
    rows.sort((a, b) => (b.late ?? -1e9) - (a.late ?? -1e9));
    return rows;
  }

  function taskItem(t) {
    const desc = String(t.description || '(no description)').trim();
    const meta = [t.type];
    if (t.due) meta.push('Due ' + t.due);
    if (t.late > 0) meta.push(`${t.late} day${t.late === 1 ? '' : 's'} late`);
    if (t.by_name) meta.push('by ' + t.by_name);
    return { title: desc.length > MAX_DESC ? desc.slice(0, MAX_DESC - 1) + '…' : desc, meta: meta.join(' · ') };
  }

  function pendingReply(name, rows) {
    const groups = [
      ['Overdue', rows.filter(t => t.late > 0)],
      ['Due today', rows.filter(t => t.late === 0)],
      ['No due date', rows.filter(t => t.late == null)],
      ['Due later', rows.filter(t => t.late < 0)],
    ];
    const due = rows.filter(t => t.late == null || t.late >= 0);
    const deleg = due.filter(t => t.type === 'Delegation').length;
    const plural = n => n === 1 ? 'task' : 'tasks';
    const later = groups[3][1].length;

    let reply;
    if (!due.length) {
      reply = `${name} has no pending tasks.`;
      if (later) reply += ` ${later} delegation ${plural(later)} due later.`;
    } else {
      reply = `${name} has ${due.length} pending ${plural(due.length)}` +
        ` (${deleg} delegation, ${due.length - deleg} checklist).`;
      const overdue = groups[0][1].length;
      if (overdue) reply += ` ${overdue} overdue.`;
    }
    const sections = groups
      .filter(([, list]) => list.length)
      .map(([title, list]) => ({
        title: `${title} (${list.length})`,
        items: list.slice(0, SECTION_LIMIT).map(taskItem),
        more: Math.max(0, list.length - SECTION_LIMIT),
      }));
    return { reply, sections };
  }

  // ── MIS score ────────────────────────────────────────
  const istToday = () => new Date(Date.now() + 5.5 * 3600e3).toISOString().slice(0, 10);
  const dmy = ymd => ymd.split('-').reverse().join('-');

  // The period named in the message. No period means this week, which is
  // what the Race Tracker opens on.
  function periodOf(msgNorm) {
    const today = istToday();
    const thisMon = istMondayOf(new Date());
    const firstOfMonth = today.slice(0, 8) + '01';
    const PAST = '(?:last|previous|pichle|pichhle|pichla|pichhla)';
    const THIS = '(?:this|is)';
    const WEEK = '(?:week|hafte|hafta)';
    const MONTH = '(?:month|mahine|mahina)';
    const has = re => new RegExp('\\b' + re + '\\b').test(msgNorm);
    if (has(PAST + ' ' + WEEK)) {
      return { label: 'last week', start: addDays(thisMon, -7), end: addDays(thisMon, -1) };
    }
    if (has(PAST + ' ' + MONTH)) {
      const end = addDays(firstOfMonth, -1);
      return { label: 'last month', start: end.slice(0, 8) + '01', end };
    }
    if (has(THIS + ' ' + MONTH)) return { label: 'this month', start: firstOfMonth, end: today };
    return { label: 'this week', start: thisMon, end: today };
  }

  async function misFor(userId, start, end) {
    const out = {};
    for (const [key, table] of [['delegation', 'delegation_tasks'], ['checklist', 'checklist_tasks']]) {
      const [[r]] = await db.query(
        `SELECT COUNT(*) AS total,
           SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
           SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
           SUM(CASE WHEN status='revised' THEN 1 ELSE 0 END) AS revised,
           SUM(CASE WHEN status='pending' AND due_date<CURDATE() THEN 1 ELSE 0 END) AS overdue
         FROM ${table} WHERE assigned_to = ? AND due_date BETWEEN ? AND ?`, [userId, start, end]);
      const n = v => parseInt(v) || 0;
      // /api/mis forces checklist revised to 0; kept identical here.
      const revised = key === 'checklist' ? 0 : n(r.revised);
      out[key] = {
        total: n(r.total), pending: n(r.pending), completed: n(r.completed), revised, overdue: n(r.overdue),
        score: scoreFor(r.total, r.pending, r.overdue, revised),
      };
    }
    // Planned vs actual, averaged over the weeks the range touches, skipping
    // empty weeks — the same figures as the MIS page's Planned / Actual columns.
    const mondays = [];
    for (let m = istMondayOf(new Date(start + 'T00:00:00Z')); m <= end; m = addDays(m, 7)) mondays.push(m);
    const weeks = await weeklyPlanVsActual([userId], mondays, addDays(istToday(), -1), istToday());
    const avg = xs => {
      const v = xs.filter(x => x != null);
      return v.length ? Math.round(v.reduce((a, b) => a + b, 0) / v.length * 10) / 10 : null;
    };
    const ws = mondays.map(m => weeks[userId][m]);
    out.planned = avg(ws.map(w => w.committed));
    out.actual = avg(ws.map(w => w.achieved));
    return out;
  }

  function misReply(name, period, m) {
    const pct = s => (s > 0 ? '+' : '') + s.toFixed(1) + '%';
    const section = (title, x, withRevised) => {
      if (!x.total) return { title: `${title}: no tasks`, items: [], more: 0 };
      const parts = [`${x.total} total`, `${x.completed} completed`, `${x.pending} pending`, `${x.overdue} delayed`];
      if (withRevised) parts.push(`${x.revised} revised`);
      return {
        title: `${title}: ${pct(x.score)}${x.score === 0 ? ' (perfect)' : ''}`,
        items: [{ title: parts.join(' · '), meta: '' }],
        more: 0,
      };
    };
    const sections = [section('Delegation', m.delegation, true), section('Checklist', m.checklist, false)];
    if (m.planned != null || m.actual != null) {
      const f = v => v == null ? '—' : v.toFixed(1);
      sections.push({ title: 'Weekly score', items: [{ title: `Planned ${f(m.planned)} · Actual ${f(m.actual)}`, meta: '' }], more: 0 });
    }
    return {
      reply: `${name}'s MIS score for ${period.label} (${dmy(period.start)} to ${dmy(period.end)}).\n` +
        '0% is perfect, −100% is the worst.',
      sections,
    };
  }

  app.post('/api/chatbot/ask', requireAuth, requireAdminOrHod, async (req, res) => {
    try {
      const message = String(req.body?.message || '').slice(0, MAX_MESSAGE);
      const msgNorm = norm(message);
      if (!msgNorm) return res.json({ reply: HELP });
      const msgWords = new Set(msgNorm.split(' '));

      const users = await usersInScope(req.session);
      let best = 0;
      let matches = [];
      for (const u of users) {
        const s = scoreUser(msgNorm, msgWords, u);
        if (s > best) { best = s; matches = [u]; }
        else if (s && s === best) matches.push(u);
      }

      // The question is judged on the words that are not part of a matched
      // name, so a person called "Naman Score" is not an MIS question.
      const nameWords = new Set(matches.flatMap(u => norm(u.name).split(' ')));
      const asks = list => list.some(w => msgWords.has(w) && !nameWords.has(w));
      const isMis = asks(MIS_WORDS);
      const period = isMis ? periodOf(msgNorm) : null;
      const askFor = n => isMis ? `MIS score of ${n} ${period.label}` : `Pending tasks of ${n}`;

      if (!matches.length) {
        return res.json({ reply: 'I couldn\'t find a person\'s name in that.\n' + HELP });
      }
      if (matches.length > 1) {
        const names = matches.slice(0, 8).map(u => u.name);
        return res.json({
          reply: 'I found more than one person with that name. Which one did you mean?',
          suggestions: names.map(askFor),
        });
      }

      const person = matches[0];
      if (asks(UNSUPPORTED) || (!isMis && asks(TIME_WORDS))) {
        return res.json({
          reply: 'I can\'t answer that yet. For now I can show someone\'s current pending tasks or their MIS score.',
          suggestions: [`Pending tasks of ${person.name}`, `MIS score of ${person.name} this week`],
        });
      }
      if (isMis) {
        const role = req.session.role;
        if (!((role === 'admin' || role === 'hod') && await userCanSee(req.session, 'mis'))) {
          return res.json({ reply: 'You don\'t have access to MIS reports.' });
        }
        return res.json(misReply(person.name, period, await misFor(person.id, period.start, period.end)));
      }
      res.json(pendingReply(person.name, await pendingTasksFor(person.id)));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

};
