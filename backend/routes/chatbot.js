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
  // "completed tasks of Naman" gets an honest "not yet" rather than a
  // pending count that looks like an answer to the question asked.
  const UNSUPPORTED = ['completed', 'complete', 'done', 'finished', 'closed'];

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

  async function pendingFor(userId) {
    const counts = {};
    for (const [key, table] of [['delegation', 'delegation_tasks'], ['checklist', 'checklist_tasks']]) {
      const [[r]] = await db.query(
        `SELECT
           SUM(CASE WHEN due_date IS NULL OR due_date <= CURDATE() THEN 1 ELSE 0 END) AS pending,
           SUM(CASE WHEN due_date < CURDATE() THEN 1 ELSE 0 END) AS overdue,
           SUM(CASE WHEN due_date = CURDATE() THEN 1 ELSE 0 END) AS today,
           SUM(CASE WHEN due_date > CURDATE() THEN 1 ELSE 0 END) AS upcoming
         FROM ${table} WHERE assigned_to = ? AND status = 'pending'`, [userId]);
      counts[key] = {
        pending: parseInt(r.pending) || 0,
        overdue: parseInt(r.overdue) || 0,
        today: parseInt(r.today) || 0,
        upcoming: parseInt(r.upcoming) || 0,
      };
    }
    return counts;
  }

  function pendingReply(name, c) {
    const total = c.delegation.pending + c.checklist.pending;
    const overdue = c.delegation.overdue + c.checklist.overdue;
    const today = c.delegation.today + c.checklist.today;
    const plural = n => n === 1 ? 'task' : 'tasks';
    if (!total) {
      let msg = `${name} has no pending tasks.`;
      if (c.delegation.upcoming) msg += `\n${c.delegation.upcoming} delegation ${plural(c.delegation.upcoming)} due later.`;
      return msg;
    }
    const lines = [
      `${name} has ${total} pending ${plural(total)}.`,
      `• Delegation: ${c.delegation.pending}`,
      `• Checklist: ${c.checklist.pending}`,
    ];
    if (overdue) lines.push(`• Overdue: ${overdue}`);
    if (today) lines.push(`• Due today: ${today}`);
    if (c.delegation.upcoming) lines.push(`Also ${c.delegation.upcoming} delegation ${plural(c.delegation.upcoming)} due later.`);
    return lines.join('\n');
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
      if (UNSUPPORTED.some(w => msgWords.has(w))) {
        return res.json({
          reply: `For now I can only count pending tasks.`,
          suggestions: [`Pending tasks of ${person.name}`],
        });
      }
      const counts = await pendingFor(person.id);
      res.json({ reply: pendingReply(person.name, counts) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

};
