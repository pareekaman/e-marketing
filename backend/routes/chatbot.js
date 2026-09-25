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
module.exports = function registerChatbotRoutes(app, deps) {
  const {
    db,
    requireAuth,
    requireAdminOrHod,
  } = deps;

  const MAX_MESSAGE = 300;
  const HELP = 'Ask me about someone\'s pending tasks, for example:\n"How many tasks are pending for Naman Gupta?"';

  // Words that ask for something this version cannot answer yet. Checked so
  // "completed tasks of Naman" or "Rahul's MIS score last week" gets an honest
  // "not yet" rather than a pending count that looks like an answer to the
  // question asked. Time words are here too: there is no date filter, so
  // "pending last week" would otherwise be answered as "pending now".
  const UNSUPPORTED = [
    'completed', 'complete', 'done', 'finished', 'closed',
    'mis', 'score', 'scores', 'performance', 'rating', 'report', 'rank',
    'leave', 'leaves', 'attendance', 'holiday', 'salary',
    'fms', 'meeting', 'meetings', 'client', 'clients',
    'last', 'yesterday', 'week', 'month', 'kal', 'pichle', 'pichhle',
  ];

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

      if (!matches.length) {
        return res.json({ reply: 'I couldn\'t find a person\'s name in that.\n' + HELP });
      }
      if (matches.length > 1) {
        const names = matches.slice(0, 8).map(u => u.name);
        return res.json({
          reply: 'I found more than one person with that name. Which one did you mean?',
          suggestions: names.map(n => `Pending tasks of ${n}`),
        });
      }

      const person = matches[0];
      const nameWords = new Set(norm(person.name).split(' '));
      if (UNSUPPORTED.some(w => msgWords.has(w) && !nameWords.has(w))) {
        return res.json({
          reply: `I can't answer that yet. For now I can only show someone's current pending tasks.`,
          suggestions: [`Pending tasks of ${person.name}`],
        });
      }
      res.json(pendingReply(person.name, await pendingTasksFor(person.id)));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

};
