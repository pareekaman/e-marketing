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
// A name outside that scope is never matched, so the bot cannot be used to
// read anyone the asker could not already see on the Tasks page. An HOD who
// names someone from another department gets a reply saying so.
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
//
// Leave and extra working questions read leave_requests. Their audience is
// the same as the Leave Tracker's team view for these roles (admin and PC
// everyone, HOD their department), which usersInScope already enforces.
//
// MIS, leave and extra working take a period: a date range typed in the
// message ("1 sep to 15 sep", "01-09-2026 se 15-09-2026"), a month name,
// or last/this week/month. See parsePeriod.
module.exports = function registerChatbotRoutes(app, deps) {
  const {
    db,
    requireAuth,
    requireAdminOrHod,
    userCanSee,
    userCanDo,
    scoreFor,
    istMondayOf,
    addDays,
    weeklyPlanVsActual,
    isUserOffOn,
    loadHolidaysSet,
    canViewComplianceEmployee,
    isPaymentApprover,
  } = deps;

  const MAX_MESSAGE = 300;
  const HELP = 'I can tell you about anyone\'s tasks, MIS, leaves, meetings and more.\n' +
    'For example: "How many tasks are pending for Naman Gupta?"';

  const GREETING_WORDS = new Set(['hi', 'hii', 'hiii', 'hello', 'helo', 'hey', 'heyy', 'namaste', 'namaskar',
    'good', 'morning', 'afternoon', 'evening', 'there', 'bot', 'chatbot', 'ji', 'sir',
    'thanks', 'thank', 'you', 'thankyou', 'thx', 'shukriya', 'dhanyawad', 'ok', 'okay', 'so', 'much', 'very', 'lot', 'alot', 'bahut', 'bohot', 'great', 'nice', 'cool', 'got', 'it', 'u', 'ty', 'tq', 'tysm', 'thnx', 'thanx']);
  // Topics a follow-up may carry over from the previous answer.
  const TOPICS = new Set(['pending', 'mis', 'leave', 'extra', 'daily', 'compliance', 'completed', 'meetings', 'inventory', 'payments', 'profile']);
  // The ways "hi" and "hello" actually get typed: hlo, hii, helloo, hey, hy, hai.
  const GREETING_RE = /^(?:h+i+|h+y+|h+a+i+|h+e+y+|h+e*l+o+|h+e+l+o+w*|hel+o+w+)$/;

  // App pages the bot can point to, with the words people use for them. The
  // longest matching phrase wins, so "fms admin" beats "fms".
  const PAGES = [
    ['dashboard', 'Dashboard', ['dashboard', 'home']],
    ['alltasks', 'All Tasks', ['all tasks', 'all task', 'task list', 'delegation', 'checklist']],
    ['approvals', 'Approvals', ['approval', 'approvals']],
    ['daily', 'Daily Task', ['daily task', 'daily tasks']],
    ['fms-tasks', 'FMS Tasks', ['fms', 'fms task', 'fms tasks']],
    ['fms', 'FMS Admin', ['fms admin']],
    ['race', 'Race Tracker', ['race', 'race tracker']],
    ['meetings', 'Scheduler', ['scheduler', 'meeting', 'meetings', 'calendar']],
    ['users', 'Users', ['users', 'user list', 'employees']],
    ['hrm', 'HR Portal', ['hr portal', 'hrm', 'hr', 'recruitment', 'candidate', 'candidates']],
    ['leaves', 'Leave Tracker', ['leave tracker', 'leave', 'leaves', 'chutti']],
    ['compliance', 'Compliance', ['compliance']],
    ['mis', 'MIS Report', ['mis', 'mis report']],
    ['dailyreports', 'Daily Reports', ['daily report', 'daily reports']],
    ['creditcards', 'Credit Card Statement', ['credit card', 'credit cards', 'card statement']],
    ['paymentreq', 'Payment Request', ['payment request', 'payment requests', 'payment']],
    ['clients', 'Client Master', ['client master', 'clients', 'client']],
    ['dms', 'DMS', ['dms', 'documents', 'drive']],
    ['inventory', 'Inventory', ['inventory', 'equipment']],
    ['feedback', 'Escalation', ['escalation', 'escalations', 'feedback']],
    ['logs', 'Logs', ['logs', 'deleted records']],
    ['leads', 'Leads Enquiry', ['leads', 'lead', 'enquiry', 'enquiries']],
    ['profile', 'Profile', ['profile', 'my profile', 'password']],
  ];
  // "Where is the FMS tab", "leave tracker kahan hai", "open inventory".
  const NAV_WORDS = ['where', 'kahan', 'kaha', 'kidhar', 'open', 'kholo', 'khol', 'tab', 'page', 'section', 'navigate', 'milega'];
  function pageIn(msgNorm) {
    const hay = ' ' + msgNorm + ' ';
    let best = null, len = 0;
    for (const [page, label, keys] of PAGES) {
      for (const k of keys) if (k.length > len && hay.includes(' ' + k + ' ')) { best = { page, label, key: k }; len = k.length; }
    }
    return best;
  }

  // "mere / my / apne" — the asker is the person. "uska / his / unke" — the
  // person from the previous answer.
  const SELF_WORDS = new Set(['my', 'mine', 'mera', 'mere', 'meri', 'myself', 'apna', 'apne', 'apni', 'khud', 'main', 'maine']);
  const PRONOUN_WORDS = new Set(['uska', 'uski', 'uske', 'unka', 'unki', 'unke', 'iska', 'iski', 'iske', 'inka', 'inki', 'inke',
    'usne', 'unhone', 'isne', 'his', 'her', 'their', 'him', 'them', 'he', 'she', 'they']);
  // Questions about everyone at once.
  const TEAM_WORDS = ['team', 'sab', 'sabke', 'sabka', 'sabki', 'sabhi', 'everyone', 'everybody', 'all', 'employees',
    'kiske', 'kiska', 'kiski', 'whose', 'who', 'kaun', 'koi', 'anyone', 'anybody', 'kon', 'jis', 'jisne', 'jinhone', 'jinhe', 'jinka', 'kisne', 'kinhone', 'names', 'naam', 'list', 'sabse', 'most', 'zyada', 'jyada', 'maximum', 'highest', 'top', 'staff', 'log', 'logo', 'logon'];
  // Everyday words that are not names — part of the vocabulary so a
  // follow-up made of them alone ("aur last month?") is recognised as one.
  const COMMON_WORDS = ['aur', 'and', 'bhi', 'also', 'se', 'tak', 'to', 'from', 'till', 'until', 'on', 'in', 'at', 'ne', 'li',
    'le', 'liya', 'lee', 'di', 'diya', 'hua', 'hui', 'hue', 'hoga', 'thi', 'tha', 'the', 'hai', 'hain', 'was', 'were', 'did',
    'does', 'has', 'have', 'had', 'get', 'give', 'find', 'list', 'count', 'number', 'total', 'this', 'that', 'is', 'last',
    'next', 'previous', 'week', 'month', 'year', 'today', 'aaj', 'yesterday', 'kal', 'din', 'days', 'day', 'ko', 'wala',
    'wali', 'wale', 'kab', 'kyun', 'kin', 'dino', 'kis', 'kaunse', 'konse', 'kinhe', 'why', 'when', 'which', 'kaunsa', 'kaunsi', 'kaise', 'how', 'kitna', 'kitni', 'kitne',
    'abhi', 'now', 'ab', 'mahina', 'mahine', 'hafte', 'hafta', 'saal', 'se', 'ka', 'ki', 'ke', 'with', 'by', 'an', 'or',
    'ya', 'please', 'plz', 'batao', 'bata', 'bataiye', 'dikhao', 'tell', 'show', 'give', 'me', 'hlo', 'kr', 'kar', 'karo', 'karna'];

  let _vocab = null;
  function vocab() {
    if (_vocab) return _vocab;
    _vocab = new Set([
      ...MIS_WORDS, ...LEAVE_WORDS, ...EXTRA_WORDS, ...DAILY_WORDS, ...COMPLIANCE_WORDS, ...NOT_WORDS, ...FILL_WORDS,
      ...COMPLETED_WORDS, ...MEETING_WORDS, ...INVENTORY_WORDS, ...PAYMENT_WORDS, ...PROFILE_WORDS, ...PENDING_WORDS,
      ...UNSUPPORTED, ...TIME_WORDS, ...NAV_WORDS, ...TEAM_WORDS, ...COMMON_WORDS,
      ...FILLER_WORDS, ...GREETING_WORDS, ...SELF_WORDS, ...PRONOUN_WORDS,
      ...Object.keys(MONTHS), ...Object.keys(WEEKDAYS),
      ...PAGES.flatMap(p => p[2].flatMap(k => k.split(' '))),
    ]);
    return _vocab;
  }
  const VOCAB = { has: w => vocab().has(w) };

  // A word the bot does not know is swapped for the nearest one it does:
  // one slip for short words, two for long ones ("pendng" → pending,
  // "meetng" → meeting, "compliace" → compliance). Short words are left alone
  // — at three letters almost everything is one slip from something.
  function correctWord(w) {
    if (w.length < 4 || /\d/.test(w) || vocab().has(w)) return w;
    const max = w.length >= 7 ? 2 : 1;
    let bestWord = w, bestD = max + 1;
    for (const v of vocab()) {
      if (v.length < 4 || Math.abs(v.length - w.length) > max) continue;
      const d = editDistance(w, v, max);
      if (d < bestD || (d === bestD && v[0] === w[0] && bestWord[0] !== w[0])) { bestD = d; bestWord = v; }
    }
    return bestD <= max ? bestWord : w;
  }

  // The second topic a question named, if any, as a suggestion builder.
  // Pairs that are one question rather than two ("daily task nahi bhara",
  // "extra leave", "completed tasks") are not counted.
  function secondTopic(asks, intent) {
    const topics = [
      ['pending', () => asks(['pending', 'baki', 'baaki', 'bacha', 'bache', 'remaining', 'overdue']), n => `Pending tasks of ${n}`],
      ['leave', () => asks(LEAVE_WORDS), n => `Leaves of ${n} this month`],
      ['extra', () => asks(EXTRA_WORDS), n => `Extra working of ${n} this month`],
      ['mis', () => asks(MIS_WORDS), n => `MIS score of ${n} this week`],
      ['meetings', () => asks(MEETING_WORDS), n => `Meetings of ${n} this month`],
      ['inventory', () => asks(INVENTORY_WORDS), n => `Equipment of ${n}`],
      ['payments', () => asks(PAYMENT_WORDS), n => `Payment requests of ${n} this month`],
    ];
    const skip = { compliance: ['pending'], extra: ['leave'], completed: ['pending'] };
    for (const [key, test, build] of topics) {
      if (key === intent || (skip[intent] || []).includes(key)) continue;
      if (test()) return build;
    }
    return null;
  }

  // Pending tasks across everyone in scope, most first — same definition as
  // one person's pending (due today or earlier, or no date).
  async function teamPendingReply(users) {
    const ids = users.map(u => u.id);
    if (!ids.length) return { reply: 'There is no one in your team to show.' };
    const counts = new Map(ids.map(id => [id, { pending: 0, overdue: 0 }]));
    for (const table of ['delegation_tasks', 'checklist_tasks']) {
      const [rows] = await db.query(
        `SELECT assigned_to AS id,
                SUM(CASE WHEN due_date IS NULL OR due_date <= CURDATE() THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN due_date < CURDATE() THEN 1 ELSE 0 END) AS overdue
           FROM ${table} WHERE status = 'pending' AND assigned_to IN (${ids.map(() => '?').join(',')})
          GROUP BY assigned_to`, ids);
      for (const r of rows) {
        const c = counts.get(r.id);
        if (c) { c.pending += parseInt(r.pending) || 0; c.overdue += parseInt(r.overdue) || 0; }
      }
    }
    const list = users.map(u => ({ name: u.name, ...counts.get(u.id) })).filter(x => x.pending > 0)
      .sort((a, b) => b.pending - a.pending || b.overdue - a.overdue || a.name.localeCompare(b.name));
    if (!list.length) return { reply: 'No one in your team has pending tasks.' };
    const total = list.reduce((a, x) => a + x.pending, 0);
    return {
      reply: `${list.length} ${list.length === 1 ? 'person has' : 'people have'} pending tasks, ${total} in all. ${list[0].name} has the most (${list[0].pending}).`,
      sections: [{
        title: 'Pending by person',
        items: list.slice(0, 15).map(x => ({ title: `${x.name} — ${x.pending}`, meta: x.overdue ? `${x.overdue} overdue` : '' })),
        more: Math.max(0, list.length - 15),
      }],
      suggestions: list.slice(0, 3).map(x => `Pending tasks of ${x.name}`),
    };
  }

  // Who is on leave (full day, half day or WFH, not rejected) in a period,
  // across everyone in scope — "aaj kaun chutti par hai", "who was on leave
  // last week". Same day rules as one person's leave.
  async function teamLeaveReply(users, period) {
    const when = periodText(period);
    const out = [];
    for (const u of users) {
      const days = await leaveDaysFor(u.id, period.start, period.end, ['full_day', 'half_day', 'work_from_home']);
      if (days.length) out.push({ u, days });
    }
    if (!out.length) return { reply: `No one in your team is on leave ${when}.` };
    const label = { full_day: 'full day', half_day: 'half day', work_from_home: 'WFH' };
    return {
      reply: `${out.length} ${out.length === 1 ? 'person is' : 'people are'} on leave ${when}.`,
      sections: [{
        title: 'On leave',
        items: out.slice(0, 20).map(({ u, days }) => ({
          title: u.name,
          meta: days.slice(0, 6).map(d => `${shortDate(d.date)} ${label[d.type] || d.type}${d.status === 'pending' ? ' (waiting)' : ''}`).join(', ')
            + (days.length > 6 ? ` +${days.length - 6} more` : ''),
        })),
        more: Math.max(0, out.length - 20),
      }],
    };
  }

  // Who did not fill the Daily Task in a period — "jis jis ne last saturday
  // ko daily report fill nahi ki". Same day rules as one person's compliance,
  // limited to the people the asker may see on the Compliance page.
  async function teamComplianceReply(users, period, req) {
    const when = periodText(period);
    const holidays = await loadHolidaysSet();
    let working = false;
    for (let d = period.start, i = 0; d <= period.end && i < 400; d = addDays(d, 1), i++) {
      if (!isUserOffOn(null, d, holidays)) { working = true; break; }
    }
    if (!working) {
      const cap = s => s[0].toUpperCase() + s.slice(1);
      const subject = period.start === period.end
        ? (period.label ? `${cap(period.label)} (${dmy(period.start)}) was an off day` : `${dmy(period.start)} was an off day`)
        : `${cap(when)} had only off days`;
      return { reply: `${subject} (Sunday, last Saturday or a holiday), so no one had to fill the Daily Task.` };
    }
    const missed = [];
    let checked = 0;
    for (const u of users) {
      if (!(await canViewComplianceEmployee(req, u.id))) continue;
      checked++;
      const c = await complianceFor(u.id, period.start, period.end);
      if (c.missed.length) missed.push({ u, days: c.missed });
    }
    if (!missed.length) return { reply: `Everyone filled the Daily Task ${when} (${checked} ${checked === 1 ? 'person' : 'people'} checked).` };
    missed.sort((a, b) => b.days.length - a.days.length || a.u.name.localeCompare(b.u.name));
    return {
      reply: `${missed.length} of ${checked} ${checked === 1 ? 'person' : 'people'} did not fill the Daily Task ${when}.`,
      sections: [{
        title: 'Not filled',
        items: missed.slice(0, 30).map(({ u, days }) => ({
          title: u.name,
          meta: period.start === period.end ? '' : `${days.length} day${days.length === 1 ? '' : 's'}: ${dateList(days, 8)}`,
        })),
        more: Math.max(0, missed.length - 30),
      }],
    };
  }

  // Any of these picks the kind of question.
  const MIS_WORDS = ['mis', 'score', 'scores', 'performance', 'rating', 'efficiency', 'progress'];
  const LEAVE_WORDS = ['leave', 'leaves', 'chutti', 'chhutti', 'chuttiyan', 'chhuttiyan', 'chuttiya', 'wfh', 'half', 'halfday', 'home', 'absent', 'off'];
  const EXTRA_WORDS = ['extra', 'overtime', 'ot'];
  const DAILY_WORDS = ['daily', 'ghante', 'hours', 'hour', 'timesheet', 'kiya', 'kiye', 'report', 'time'];
  // Compliance = which working days the Daily Task was not filled. Needs
  // either a compliance word or "not filled" in some form, because "kitne
  // ghante daily task bhara" (hours logged) also contains "bhara".
  const COMPLIANCE_WORDS = ['compliance', 'missed', 'miss', 'skipped'];
  const NOT_WORDS = ['nahi', 'nhi', 'not', 'nahin'];
  const FILL_WORDS = ['bhara', 'bhari', 'bhare', 'fill', 'filled'];

  // Words that ask for something this version cannot answer yet. Checked so
  // "completed tasks of Naman" gets an honest "not yet" rather than a pending
  // count that looks like an answer to the question asked.
  const COMPLETED_WORDS = ['completed', 'complete', 'done', 'finished', 'closed', 'poora', 'pura', 'pure', 'poore'];
  const STRICT_PENDING = ['pending', 'baki', 'baaki', 'bacha', 'bache', 'remaining', 'overdue'];
  // Requests to change something. The bot only looks things up, so these get
  // a plain "I can't make changes" instead of an answer that looks like one.
  const ACTION_WORDS = ['delete', 'remove', 'assign', 'create', 'add', 'update', 'edit', 'change', 'modify', 'cancel',
    'send', 'transfer', 'delegate', 'hatao', 'hata', 'bhejo', 'bhej', 'banao', 'bana', 'daalo', 'dalo', 'likho', 'reassign'];
  // A request, not a question: an imperative ("karo", "kar do") or "can you"
  // around a word that changes something. "complete kar diye" (past tense)
  // is a question and is not caught: only imperatives count for those words.
  const IMPERATIVE_WORDS = ['karo', 'kardo', 'krdo', 'kro', 'karwao', 'kijiye', 'kariye', 'karna'];
  const CAN_WORDS = ['can', 'could', 'sakte', 'sakti', 'sakta'];
  const APPROVE_WORDS = ['approve', 'reject'];
  const SET_WORDS = ['approve', 'reject', 'mark', 'complete', 'done', 'schedule', 'book', 'set', 'fix', 'arrange', 'lagao', 'rakho'];
  const PROFILE_WORDS = ['who', 'kaun', 'profile', 'department', 'dept', 'role', 'designation', 'joining', 'details'];
  const PENDING_WORDS = ['pending', 'baki', 'baaki', 'bacha', 'bache', 'remaining', 'overdue', 'due',
    'task', 'tasks', 'kaam', 'work', 'status'];
  // Words that carry no question of their own, in English and Hinglish.
  const FILLER_WORDS = new Set(['ka', 'ki', 'ke', 'ko', 'kya', 'hai', 'hain', 'h', 'batao', 'bata', 'bataiye',
    'show', 'tell', 'me', 'mujhe', 'of', 'for', 'the', 'a', 'please', 'plz', 'pls', 'kitne', 'kitna', 'kitni',
    'how', 'many', 'much', 'what', 'is', 'are', 'about', 'check', 'do', 'dikhao', 'maanke', 'chalo', 'woh', 'unke', 'uske']);
  const MEETING_WORDS = ['meeting', 'meetings', 'meet', 'schedule', 'calendar'];
  const INVENTORY_WORDS = ['inventory', 'equipment', 'asset', 'assets', 'device', 'devices', 'laptop',
    'mobile', 'sim', 'charger', 'keyboard', 'mouse', 'saman', 'samaan', 'saamaan', 'saaman'];
  const PAYMENT_WORDS = ['payment', 'payments', 'reimbursement', 'expense', 'expenses', 'kharcha'];
  const UNSUPPORTED = [
    'rank', 'birthday', 'bday', 'janamdin', 'anniversary', 'phone', 'contact', 'email', 'address', 'whatsapp', 'salary',
    'attendance', 'holiday', 'salary',
    'fms', 'client', 'clients',
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
    // Each name word found exactly counts 1; one typed with a single slip
    // ("gupt", "namann") counts 0.9, so an exact match always wins a tie.
    let hits = 0;
    for (const w of full.split(' ')) {
      if (w.length < 3) continue;
      if (msgWords.has(w)) hits += 1;
      else if (w.length >= 4 && [...msgWords].some(m => m.length >= 4 && m[0] === w[0] && editDistance(m, w) <= 1)) hits += 0.9;
    }
    return hits;
  }

  // Levenshtein distance, stopping early once it passes `max`.
  function editDistance(a, b, max = 2) {
    if (Math.abs(a.length - b.length) > max) return max + 1;
    let prev = Array.from({ length: b.length + 1 }, (_, i) => i);
    for (let i = 1; i <= a.length; i++) {
      const cur = [i];
      let rowMin = i;
      for (let j = 1; j <= b.length; j++) {
        cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
        if (cur[j] < rowMin) rowMin = cur[j];
      }
      if (rowMin > max) return max + 1;
      prev = cur;
    }
    return prev[b.length];
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

  // ── Periods ──────────────────────────────────────────
  const MONTHS = {
    jan: 0, january: 0, feb: 1, february: 1, mar: 2, march: 2, apr: 3, april: 3,
    may: 4, jun: 5, june: 5, jul: 6, july: 6, aug: 7, august: 7,
    sep: 8, sept: 8, september: 8, oct: 9, october: 9, nov: 10, november: 10, dec: 11, december: 11,
  };
  // A month named on its own ("august ka") selects the whole month. Only full
  // names count, and not "may", which is an everyday English word.
  const FULL_MONTHS = ['january', 'february', 'march', 'april', 'june', 'july',
    'august', 'september', 'october', 'november', 'december'];
  const pad = n => String(n).padStart(2, '0');
  const WEEKDAYS = {
    sunday: 0, ravivar: 0, itwar: 0, itvar: 0,
    monday: 1, somvar: 1, somwar: 1,
    tuesday: 2, mangalvar: 2, mangalwar: 2,
    wednesday: 3, budhvar: 3, budhwar: 3,
    thursday: 4, guruvar: 4, guruwar: 4,
    friday: 5, shukravar: 5, shukrawar: 5,
    saturday: 6, shanivar: 6, shaniwar: 6,
  };

  // A real calendar date as YYYY-MM-DD, or null ("31 02 2026" is not one).
  function ymdOf(y, m, d) {
    const dt = new Date(Date.UTC(y, m, d));
    if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== m || dt.getUTCDate() !== d) return null;
    return `${y}-${pad(m + 1)}-${pad(d)}`;
  }

  // Every explicit date in the message, in order, as { ymd, yearTyped }.
  // norm() has already turned "01-09-2026" and "01/09/2026" into "01 09 2026".
  // Understood forms:
  //   2026 09 01 · 01 09 2026 · 1 sep [2026] · 1st september · sep 1 [2026]
  // A date without a year is put in the current year; parsePeriod decides
  // whether that needs to become last year.
  function explicitDates(words) {
    const thisYear = parseInt(istToday().slice(0, 4));
    const isYear = w => /^\d{4}$/.test(w || '');
    const dayOf = w => { const m = /^(\d{1,2})(?:st|nd|rd|th)?$/.exec(w || ''); return m ? parseInt(m[1]) : null; };
    const num = w => /^\d{1,2}$/.test(w || '') ? parseInt(w) : null;
    const out = [];
    for (let i = 0; i < words.length; i++) {
      const [a, b, c] = [words[i], words[i + 1], words[i + 2]];
      let hit = null, used = 0, yearTyped = true;
      if (isYear(a) && num(b) != null && num(c) != null) { hit = ymdOf(+a, num(b) - 1, num(c)); used = 3; }
      else if (num(a) != null && num(b) != null && isYear(c)) { hit = ymdOf(+c, num(b) - 1, num(a)); used = 3; }
      else if (dayOf(a) != null && b in MONTHS) {
        yearTyped = isYear(c);
        hit = ymdOf(yearTyped ? +c : thisYear, MONTHS[b], dayOf(a));
        used = yearTyped ? 3 : 2;
      } else if (a in MONTHS && dayOf(b) != null) {
        yearTyped = isYear(c);
        hit = ymdOf(yearTyped ? +c : thisYear, MONTHS[a], dayOf(b));
        used = yearTyped ? 3 : 2;
      }
      if (hit) { out.push({ ymd: hit, yearTyped }); i += used - 1; }
    }
    return out;
  }

  // The period a question is about. `fallback` is used when none is named:
  // 'week' for MIS (what the Race Tracker opens on), 'month' for leave and
  // extra working. `phrase` is the period in words that parsePeriod reads
  // back, used for the one-click suggestions.
  function parsePeriod(msgNorm, fallback) {
    const today = istToday();
    const thisMon = istMondayOf(new Date());
    const firstOfMonth = today.slice(0, 8) + '01';
    const words = msgNorm.split(' ');
    const has = re => new RegExp('\\b' + re + '\\b').test(msgNorm);
    const range = (start, end, label) => ({
      start, end, label,
      phrase: label || (start === end ? `on ${dmy(start)}` : `from ${dmy(start)} to ${dmy(end)}`),
    });

    // Every question here is about work already done, so a range that runs
    // past today stops at today ("1 sep to 30 sep" asked on the 25th). Only
    // when the whole range lies ahead and no year was typed ("1 dec to 31 dec"
    // asked in September) is it last year's.
    const found = explicitDates(words);
    if (found.length) {
      const lastYear = ymd => `${+ymd.slice(0, 4) - 1}${ymd.slice(4)}`;
      let ds = found.map(d => d.ymd).sort();
      if (ds[0] > today && found.every(d => !d.yearTyped)) ds = ds.map(lastYear);
      let start = ds[0], end = ds[ds.length - 1];
      // "15 sep se aaj tak" — one date plus today.
      if (found.length === 1 && has('(?:today|aaj|now|abhi)')) end = today;
      // A range that has not started yet (a future year was typed) is
      // flagged, so the answer says so instead of quietly showing today.
      if (start > today) return { ...range(start, end), future: true };
      if (end > today) end = today;
      return range(start, end);
    }
    const month = FULL_MONTHS.find(m => words.includes(m));
    if (month) {
      const idx = words.indexOf(month);
      const y = /^\d{4}$/.test(words[idx + 1] || '') ? +words[idx + 1] : parseInt(today.slice(0, 4));
      let start = ymdOf(y, MONTHS[month], 1);
      if (start > today && !/^\d{4}$/.test(words[idx + 1] || '')) start = ymdOf(y - 1, MONTHS[month], 1);
      const end = addDays(ymdOf(+start.slice(0, 4), MONTHS[month] + 1, 1) || `${+start.slice(0, 4) + 1}-01-01`, -1);
      return range(start, end > today ? today : end, `${month[0].toUpperCase() + month.slice(1)} ${start.slice(0, 4)}`);
    }

    const PAST = '(?:last|previous|pichle|pichhle|pichla|pichhla)';
    const THIS = '(?:this|is)';
    const WEEK = '(?:week|hafte|hafta)';
    const MONTH = '(?:month|mahine|mahina)';
    const YEAR = '(?:year|saal)';

    // A weekday: "last monday" / "pichle somvar" is the most recent one
    // before today; a bare "monday" / "somvar ko" may be today itself.
    const dayIdx = words.findIndex(w => w in WEEKDAYS);
    if (dayIdx >= 0) {
      const want = WEEKDAYS[words[dayIdx]];
      const past = dayIdx > 0 && new RegExp('^' + PAST + '$').test(words[dayIdx - 1]);
      const dow = new Date(today + 'T00:00:00Z').getUTCDay();
      let back = (dow - want + 7) % 7;
      if (past && back === 0) back = 7;
      const d = addDays(today, -back);
      return range(d, d);
    }

    if (has(PAST + ' ' + WEEK)) return range(addDays(thisMon, -7), addDays(thisMon, -1), 'last week');
    if (has(PAST + ' ' + MONTH)) {
      const end = addDays(firstOfMonth, -1);
      return range(end.slice(0, 8) + '01', end, 'last month');
    }
    if (has(THIS + ' ' + MONTH)) return range(firstOfMonth, today, 'this month');
    if (has(THIS + ' ' + WEEK)) return range(thisMon, today, 'this week');
    if (has(THIS + ' ' + YEAR)) return range(today.slice(0, 4) + '-01-01', today, 'this year');
    if (has('(?:yesterday)')) return range(addDays(today, -1), addDays(today, -1), 'yesterday');
    if (has('(?:today|aaj)')) return range(today, today, 'today');
    if (fallback === 'today') return range(today, today, 'today');
    if (fallback === 'yesterday') return range(addDays(today, -1), addDays(today, -1), 'yesterday');
    return fallback === 'month'
      ? range(firstOfMonth, today, 'this month')
      : range(thisMon, today, 'this week');
  }
  // The period as it reads inside a sentence: "in last month (01-08-2026 to
  // 31-08-2026)", "from 01-09-2026 to 15-09-2026", "on 08-09-2026".
  const periodText = (p, prep = 'in') =>
    (p.label === 'today' || p.label === 'yesterday') ? `${p.label} (${dmy(p.start)})`
    : p.label ? `${prep} ${p.label} (${dmy(p.start)} to ${dmy(p.end)})`
    : p.start === p.end ? `on ${dmy(p.start)}` : `from ${dmy(p.start)} to ${dmy(p.end)}`;

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
      reply: `${name}'s MIS score ${periodText(period, 'for')}.\n` +
        '0% is perfect, −100% is the worst.',
      sections,
    };
  }

  // ── Leave and extra working (both live in leave_requests) ──
  // A request is one row; its days are the dates_json list when present,
  // otherwise the from_date..to_date range (older rows) — the same rule as
  // approvedLeaveDates() in server.js. Rejected requests are ignored.
  const shortDate = ymd => `${ymd.slice(8, 10)}-${ymd.slice(5, 7)}`;
  const dateList = (ds, max = 12) => ds.length <= max
    ? ds.map(shortDate).join(', ')
    : ds.slice(0, max).map(shortDate).join(', ') + ` +${ds.length - max} more`;
  const hm = mins => {
    const h = Math.floor(mins / 60), m = mins % 60;
    return h && m ? `${h}h ${m}m` : h ? `${h}h` : `${m}m`;
  };

  async function leaveDaysFor(userId, start, end, types) {
    const [rows] = await db.query(
      `SELECT leave_type, status, dates_json,
              DATE_FORMAT(from_date,'%Y-%m-%d') AS from_date,
              DATE_FORMAT(to_date,'%Y-%m-%d') AS to_date
         FROM leave_requests
        WHERE user_id = ? AND status <> 'rejected'
          AND leave_type IN (${types.map(() => '?').join(',')})
          AND from_date <= ? AND to_date >= ?`, [userId, ...types, end, start]);
    const days = [];
    for (const r of rows) {
      let listed = null;
      if (r.dates_json) {
        try { listed = JSON.parse(r.dates_json); } catch { listed = null; }
      }
      if (Array.isArray(listed)) {
        for (const x of listed) {
          const date = String(x && x.date || x).slice(0, 10);
          days.push({ date, type: r.leave_type, status: r.status, hours: Number(x && x.hours) || 0, entries: (x && x.entries) || [] });
        }
      } else {
        for (let d = r.from_date, i = 0; d <= r.to_date && i < 400; d = addDays(d, 1), i++) {
          days.push({ date: d, type: r.leave_type, status: r.status, hours: 0, entries: [] });
        }
      }
    }
    return days.filter(d => d.date >= start && d.date <= end).sort((a, b) => a.date.localeCompare(b.date));
  }

  function leaveReply(name, period, days) {
    const pick = (status, type) => days.filter(d => d.status === status && d.type === type).map(d => d.date);
    const block = status => {
      const full = pick(status, 'full_day'), half = pick(status, 'half_day'), wfh = pick(status, 'work_from_home');
      const items = [];
      if (full.length) items.push({ title: `Full day: ${full.length}`, meta: dateList(full) });
      if (half.length) items.push({ title: `Half day: ${half.length}`, meta: dateList(half) });
      if (wfh.length) items.push({ title: `Work from home: ${wfh.length}`, meta: dateList(wfh) });
      return { items, leaveDays: full.length + half.length * 0.5 };
    };
    const ok = block('approved'), wait = block('pending');
    const when = periodText(period);
    if (!ok.items.length && !wait.items.length) return { reply: `${name} took no leave ${when}.` };
    const sections = [];
    if (ok.items.length) sections.push({ title: 'Approved', items: ok.items, more: 0 });
    if (wait.items.length) sections.push({ title: 'Waiting for approval', items: wait.items, more: 0 });
    let reply = `${name} took ${ok.leaveDays} day${ok.leaveDays === 1 ? '' : 's'} of approved leave ${when}.`;
    if (wait.leaveDays) reply += ` ${wait.leaveDays} more waiting for approval.`;
    reply += '\nA half day counts as 0.5. Work from home is not counted as leave.';
    return { reply, sections };
  }

  function extraReply(name, period, days) {
    const mins = d => Math.round(d.hours * 60);
    const block = status => {
      const list = days.filter(d => d.status === status);
      return {
        total: list.reduce((a, d) => a + mins(d), 0),
        count: new Set(list.map(d => d.date)).size,
        items: list.slice(0, 10).map(d => {
          const clients = [...new Set((d.entries || []).map(e => e && e.client).filter(Boolean))];
          return { title: `${dmy(d.date)} — ${hm(mins(d))}`, meta: clients.join(', ') };
        }),
        more: Math.max(0, list.length - 10),
      };
    };
    const ok = block('approved'), wait = block('pending');
    const when = periodText(period);
    const dayWord = n => n === 1 ? 'day' : 'days';
    if (!ok.count && !wait.count) return { reply: `${name} did no extra working ${when}.` };
    let reply = `${name} did ${hm(ok.total)} of approved extra working ${when}, on ${ok.count} ${dayWord(ok.count)}.`;
    if (wait.count) reply += ` ${hm(wait.total)} more on ${wait.count} ${dayWord(wait.count)} is waiting for approval.`;
    const sections = [];
    if (ok.count) sections.push({ title: `Approved (${hm(ok.total)})`, items: ok.items, more: ok.more });
    if (wait.count) sections.push({ title: `Waiting for approval (${hm(wait.total)})`, items: wait.items, more: wait.more });
    return { reply, sections };
  }

  // ── Daily Task hours ─────────────────────────────────
  // What someone logged on the Daily Task page, by client and by day. The
  // app shows other people's daily tasks only on Daily Reports, whose routes
  // are requireAdmin, so this answers for an admin only.
  async function dailyFor(userId, start, end) {
    const [rows] = await db.query(
      `SELECT DATE_FORMAT(entry_date,'%Y-%m-%d') AS d, client_name, department, description, duration_min
         FROM daily_tasks WHERE user_id = ? AND entry_date BETWEEN ? AND ?
        ORDER BY entry_date ASC, id ASC`, [userId, start, end]);
    return rows;
  }

  function dailyReply(name, period, rows) {
    const when = periodText(period);
    if (!rows.length) return { reply: `${name} logged no daily tasks ${when}.` };
    const sumBy = key => {
      const m = new Map();
      for (const r of rows) m.set(r[key], (m.get(r[key]) || 0) + (parseInt(r.duration_min) || 0));
      return [...m.entries()];
    };
    const total = rows.reduce((a, r) => a + (parseInt(r.duration_min) || 0), 0);
    const byClient = sumBy('client_name').sort((a, b) => b[1] - a[1]);
    const byDay = sumBy('d');
    const days = byDay.length;
    // One day is the daily report itself: every entry with what was done,
    // not just the totals.
    if (period.start === period.end) {
      return {
        reply: `${name} logged ${hm(total)} of daily tasks ${when} (${rows.length} entr${rows.length === 1 ? 'y' : 'ies'}).`,
        sections: [{
          title: 'Daily report',
          items: rows.slice(0, 25).map(r => {
            const desc = String(r.description || '').trim();
            return {
              title: `${r.client_name || '(no client)'} — ${hm(parseInt(r.duration_min) || 0)}`,
              meta: [r.department, desc.length > 200 ? desc.slice(0, 199) + '…' : desc].filter(Boolean).join(' · '),
            };
          }),
          more: Math.max(0, rows.length - 25),
        }],
      };
    }
    return {
      reply: `${name} logged ${hm(total)} of daily tasks ${when}` +
        (period.start === period.end ? '' : `, on ${days} day${days === 1 ? '' : 's'}`) +
        ` (${rows.length} entr${rows.length === 1 ? 'y' : 'ies'}).`,
      sections: [
        { title: 'By client', items: byClient.slice(0, SECTION_LIMIT).map(([c, m]) => ({ title: `${c || '(no client)'} — ${hm(m)}`, meta: '' })),
          more: Math.max(0, byClient.length - SECTION_LIMIT) },
        { title: 'By day', items: byDay.slice(0, SECTION_LIMIT).map(([d, m]) => ({ title: `${dmy(d)} — ${hm(m)}`, meta: '' })),
          more: Math.max(0, days - SECTION_LIMIT) },
      ],
    };
  }

  // ── Compliance (Daily Task filled on each working day?) ──
  // The same day rules as /api/compliance/last7: off days come from
  // isUserOffOn (Sundays, last Saturday, holidays), days before joining and
  // full-day leave that is not rejected are not expected. Today counts only
  // once it is filled, since the day is not over.
  async function complianceFor(userId, start, end) {
    const [[u]] = await db.query(
      `SELECT id, DATE_FORMAT(joining_date,'%Y-%m-%d') AS joining_date FROM users WHERE id = ?`, [userId]);
    const [filledRows] = await db.query(
      `SELECT DISTINCT DATE_FORMAT(entry_date,'%Y-%m-%d') AS d FROM daily_tasks
        WHERE user_id = ? AND entry_date BETWEEN ? AND ?`, [userId, start, end]);
    const filled = new Set(filledRows.map(r => r.d));
    const leave = new Set((await leaveDaysFor(userId, start, end, ['full_day'])).map(d => d.date));
    const holidays = await loadHolidaysSet();
    const today = istToday();
    const out = { expected: 0, filled: [], missed: [], leave: [], off: 0 };
    for (let d = start, i = 0; d <= end && i < 400; d = addDays(d, 1), i++) {
      if (u && u.joining_date && d < u.joining_date) continue;
      if (isUserOffOn(u, d, holidays)) { out.off++; continue; }
      if (filled.has(d)) { out.expected++; out.filled.push(d); continue; }
      if (leave.has(d)) { out.leave.push(d); continue; }
      if (d === today) continue;
      out.expected++;
      out.missed.push(d);
    }
    return out;
  }

  function complianceReply(name, period, c) {
    const when = periodText(period);
    if (!c.expected) return { reply: `${name} had no working days to fill ${when}.` };
    const pct = Math.round(c.filled.length / c.expected * 100);
    const sections = [];
    if (c.missed.length) sections.push({ title: `Not filled (${c.missed.length})`, items: [{ title: dateList(c.missed, 20), meta: '' }], more: 0 });
    if (c.leave.length) sections.push({ title: `On leave (${c.leave.length})`, items: [{ title: dateList(c.leave, 20), meta: '' }], more: 0 });
    return {
      reply: `${name} filled the Daily Task on ${c.filled.length} of ${c.expected} working days ${when} (${pct}%).` +
        (c.missed.length ? ` Missed ${c.missed.length}.` : ' No days missed.') +
        '\nSundays, the last Saturday, holidays and full-day leave are not counted.',
      sections,
    };
  }

  // ── Completed tasks ──────────────────────────────────
  // Tasks due in the period and how many are completed — the same base the
  // MIS page counts from (due_date in range). Delegation rows also carry
  // completed_at, so each one says whether it was finished on time.
  async function completedFor(userId, start, end) {
    const out = {};
    for (const [key, table] of [['delegation', 'delegation_tasks'], ['checklist', 'checklist_tasks']]) {
      const [[r]] = await db.query(
        `SELECT COUNT(*) AS total, SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS done
           FROM ${table} WHERE assigned_to = ? AND due_date BETWEEN ? AND ?`, [userId, start, end]);
      out[key] = { total: parseInt(r.total) || 0, done: parseInt(r.done) || 0 };
    }
    const [list] = await db.query(
      `SELECT description, DATE_FORMAT(due_date,'%Y-%m-%d') AS due,
              DATE_FORMAT(completed_at,'%Y-%m-%d') AS done_on
         FROM delegation_tasks
        WHERE assigned_to = ? AND status = 'completed' AND due_date BETWEEN ? AND ?
        ORDER BY due_date ASC`, [userId, start, end]);
    out.list = list;
    return out;
  }

  function completedReply(name, period, c) {
    const when = periodText(period);
    const total = c.delegation.total + c.checklist.total;
    const done = c.delegation.done + c.checklist.done;
    if (!total) return { reply: `${name} had no tasks due ${when}.` };
    const late = c.list.filter(t => t.done_on && t.done_on > t.due).length;
    const sections = [{
      title: 'Summary',
      items: [
        { title: `Delegation: ${c.delegation.done} of ${c.delegation.total} completed`, meta: c.list.length ? `${c.list.length - late} on time · ${late} late` : '' },
        { title: `Checklist: ${c.checklist.done} of ${c.checklist.total} completed`, meta: '' },
      ],
      more: 0,
    }];
    if (c.list.length) {
      sections.push({
        title: `Delegation tasks completed (${c.list.length})`,
        items: c.list.slice(0, SECTION_LIMIT).map(t => {
          const desc = String(t.description || '(no description)').trim();
          const meta = [`Due ${dmy(t.due)}`];
          if (t.done_on) meta.push(`done ${dmy(t.done_on)}${t.done_on > t.due ? ' (late)' : ''}`);
          return { title: desc.length > MAX_DESC ? desc.slice(0, MAX_DESC - 1) + '…' : desc, meta: meta.join(' · ') };
        }),
        more: Math.max(0, c.list.length - SECTION_LIMIT),
      });
    }
    return {
      reply: `${name} had ${total} task${total === 1 ? '' : 's'} due ${when}: ${done} completed, ${total - done} still pending.`,
      sections,
    };
  }

  // ── Meetings ─────────────────────────────────────────
  // The same counts and list as Employee 360's Meetings block: organized
  // (by status) and attended, by meeting_date. The Scheduler page shows
  // each person only their own meetings, but Employee 360 shows anyone's to
  // a compliance viewer, so this is gated the same way as compliance.
  async function meetingsFor(userId, start, end) {
    const [[mo]] = await db.query(
      `SELECT COUNT(*) AS total,
         SUM(CASE WHEN status='scheduled' THEN 1 ELSE 0 END) AS scheduled,
         SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done,
         SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled
        FROM meetings WHERE organizer_id = ? AND meeting_date BETWEEN ? AND ?`, [userId, start, end]);
    const [[ma]] = await db.query(
      `SELECT COUNT(DISTINCT m.id) AS total
         FROM meetings m JOIN meeting_attendees mt ON mt.meeting_id = m.id
        WHERE mt.user_id = ? AND m.organizer_id <> ? AND m.meeting_date BETWEEN ? AND ?`, [userId, userId, start, end]);
    const [list] = await db.query(
      `SELECT m.title, m.status, DATE_FORMAT(m.meeting_date,'%Y-%m-%d') AS d,
              TIME_FORMAT(m.start_time,'%H:%i') AS t, c.name AS client_name,
              CASE WHEN m.organizer_id = ? THEN 'Organizer' ELSE 'Attendee' END AS role
         FROM meetings m LEFT JOIN clients c ON m.client_id = c.id
        WHERE (m.organizer_id = ? OR EXISTS (SELECT 1 FROM meeting_attendees mt WHERE mt.meeting_id = m.id AND mt.user_id = ?))
          AND m.meeting_date BETWEEN ? AND ?
        ORDER BY m.meeting_date ASC, m.start_time ASC`, [userId, userId, userId, start, end]);
    const n = v => parseInt(v) || 0;
    return {
      organized: { total: n(mo.total), scheduled: n(mo.scheduled), done: n(mo.done), cancelled: n(mo.cancelled) },
      attended: n(ma.total),
      list,
    };
  }

  function meetingsReply(name, period, m) {
    const when = periodText(period);
    if (!m.list.length) return { reply: `${name} had no meetings ${when}.` };
    const o = m.organized;
    const parts = [];
    if (o.done) parts.push(`${o.done} done`);
    if (o.scheduled) parts.push(`${o.scheduled} scheduled`);
    if (o.cancelled) parts.push(`${o.cancelled} cancelled`);
    return {
      reply: `${name} had ${m.list.length} meeting${m.list.length === 1 ? '' : 's'} ${when}: ` +
        `organized ${o.total}${parts.length ? ` (${parts.join(', ')})` : ''}, attended ${m.attended}.`,
      sections: [{
        title: `Meetings (${m.list.length})`,
        items: m.list.slice(0, SECTION_LIMIT).map(x => ({
          title: x.title,
          meta: [`${dmy(x.d)} ${x.t}`, x.client_name, x.role, x.status].filter(Boolean).join(' · '),
        })),
        more: Math.max(0, m.list.length - SECTION_LIMIT),
      }],
    };
  }

  // ── Inventory ────────────────────────────────────────
  // What someone holds right now: assignments that are active or waiting for
  // handover, as on the Inventory page's assignment list. That list is
  // edit_inventory only, so the same key gates this (or asking about oneself,
  // which My Equipment already shows). No period — it is the current state.
  async function inventoryFor(userId) {
    const [rows] = await db.query(
      `SELECT i.name, i.type, i.brand, i.model, i.serial_number, a.handover_status,
              DATE_FORMAT(a.assigned_at,'%Y-%m-%d') AS since
         FROM inventory_assignments a JOIN inventory_items i ON i.id = a.item_id
        WHERE a.user_id = ? AND a.handover_status IN ('active','pending_handover')
        ORDER BY a.assigned_at ASC`, [userId]);
    return rows;
  }

  function inventoryReply(name, rows) {
    if (!rows.length) return { reply: `${name} has no equipment assigned right now.` };
    return {
      reply: `${name} has ${rows.length} item${rows.length === 1 ? '' : 's'} assigned right now.`,
      sections: [{
        title: 'Equipment',
        items: rows.map(r => ({
          title: [r.name, [r.brand, r.model].filter(Boolean).join(' ')].filter(Boolean).join(' — '),
          meta: [r.type, r.serial_number && `S/N ${r.serial_number}`, `since ${dmy(r.since)}`,
            r.handover_status === 'pending_handover' && 'handover pending'].filter(Boolean).join(' · '),
        })),
        more: 0,
      }],
    };
  }

  // ── Payment requests ─────────────────────────────────
  // Requests someone raised in the period (by created_at). Two quirks of the
  // table, both handled the way the Payment Request page handles them:
  //   - the amount usually rides inside the reason as "[₹500.00] Software"
  //     (prParseReason in public/js/payments.js); the amount column is the
  //     fallback for rows without that prefix;
  //   - "paid" and "cancelled" are separate rows under bank_name '__system__'
  //     whose reason is "__paid__:<id>" / "__cancelled__:<id>".
  // The full list is isPaymentApprover only, so that gates this; anyone can
  // ask about their own, which /api/payment-requests/my already shows them.
  function parseAmount(raw, column) {
    const s = String(raw || '');
    if (s.charAt(0) === '[') {
      const close = s.indexOf('] ');
      if (close > 1) {
        const inner = s.slice(1, close);
        const num = parseFloat(inner.slice(1).replace(/,/g, ''));
        if (!isNaN(num) && num >= 0) return { amount: num, currency: inner.charAt(0), reason: s.slice(close + 2) };
      }
    }
    const col = parseFloat(column);
    return { amount: col > 0 ? col : null, currency: '₹', reason: s };
  }

  async function paymentsFor(userId, start, end) {
    const [rows] = await db.query(
      `SELECT id, reason, amount, status, payment_done, DATE_FORMAT(created_at,'%Y-%m-%d') AS d
         FROM payment_requests
        WHERE submitted_by = ? AND bank_name <> '__system__' AND DATE(created_at) BETWEEN ? AND ?
        ORDER BY created_at ASC`, [userId, start, end]);
    const [marks] = await db.query(
      `SELECT reason FROM payment_requests WHERE bank_name = '__system__'`);
    const paid = new Set(), cancelled = new Set();
    for (const m of marks) {
      const x = /^__(paid|cancelled)__:(\d+)/.exec(m.reason || '');
      if (x) (x[1] === 'paid' ? paid : cancelled).add(Number(x[2]));
    }
    return rows.map(r => ({
      ...r, ...parseAmount(r.reason, r.amount),
      paid: !!r.payment_done || paid.has(r.id),
      cancelled: cancelled.has(r.id),
    }));
  }

  function paymentsReply(name, period, rows) {
    const when = periodText(period);
    if (!rows.length) return { reply: `${name} raised no payment requests ${when}.` };
    const money = (cur, n) => cur + n.toLocaleString('en-IN', { maximumFractionDigits: 2 });
    const count = s => rows.filter(r => r.status === s).length;
    const totals = {};
    for (const r of rows) if (r.amount != null && r.status !== 'rejected' && !r.cancelled) totals[r.currency] = (totals[r.currency] || 0) + r.amount;
    const totalText = Object.entries(totals).map(([c, n]) => money(c, n)).join(' + ');
    const parts = [['approved', count('approved')], ['pending', count('pending')], ['rejected', count('rejected')]]
      .filter(([, n]) => n).map(([s, n]) => `${n} ${s}`);
    return {
      reply: `${name} raised ${rows.length} payment request${rows.length === 1 ? '' : 's'} ${when}` +
        `${parts.length ? ` (${parts.join(', ')})` : ''}.` +
        (totalText ? ` Total ${totalText}, not counting rejected or cancelled.` : ''),
      sections: [{
        title: 'Requests',
        items: rows.slice(0, SECTION_LIMIT).map(r => {
          const reason = String(r.reason || '').trim();
          return {
            title: (r.amount != null ? money(r.currency, r.amount) + ' — ' : '') +
              (reason.length > MAX_DESC ? reason.slice(0, MAX_DESC - 1) + '…' : reason),
            meta: [dmy(r.d), r.cancelled ? 'cancelled' : r.status, r.paid && 'paid'].filter(Boolean).join(' · '),
          };
        }),
        more: Math.max(0, rows.length - SECTION_LIMIT),
      }],
    };
  }

  app.post('/api/chatbot/ask', requireAuth, requireAdminOrHod, async (req, res) => {
    try {
      const message = String(req.body?.message || '').slice(0, MAX_MESSAGE);
      const msgNorm = norm(message);
      if (!msgNorm) return res.json({ reply: HELP });
      const msgWords = new Set(msgNorm.split(' '));

      // A message made only of greeting or thanks words gets a greeting back
      // rather than "I couldn't find a person's name".
      if ([...msgWords].every(w => GREETING_WORDS.has(w) || GREETING_RE.test(w))) {
        const thanks = [...msgWords].some(w => w.startsWith('thank') || w === 'thx' || w === 'shukriya' || w === 'dhanyawad');
        // The name is read fresh rather than from the token, which keeps the
        // name the user had when they signed in.
        const [[me]] = await db.query('SELECT name FROM users WHERE id=?', [req.session.userId]);
        const name = me && me.name ? ' ' + me.name : '';
        return res.json({
          reply: thanks ? 'You\'re welcome! Ask me anything else about your team\'s work.'
            // ‑: non-breaking hyphen, keeps "E-Marketing" on one line.
            : `Hello${name}! Welcome to the E‑Marketing chatbot. How may I help you?\n` +
              'You can ask about anyone\'s tasks, MIS, leaves, meetings and more. For example: "How many tasks are pending for Naman Gupta?"',
        });
      }

      // "delete naman's tasks", "naman ko task assign karo", "approve karo":
      // the bot reads, it never writes.
      const first = msgNorm.split(' ')[0];
      const imperative = IMPERATIVE_WORDS.some(w => msgWords.has(w)) || (msgWords.has('kar') && msgWords.has('do'));
      if (ACTION_WORDS.some(w => msgWords.has(w)) || APPROVE_WORDS.includes(first) || first === 'mark'
          || (imperative && SET_WORDS.some(w => msgWords.has(w)))
          || (CAN_WORDS.some(w => msgWords.has(w)) && APPROVE_WORDS.some(w => msgWords.has(w)))) {
        return res.json({ reply: 'I can only look things up, not make changes. Please do that from the app itself.\n' + HELP });
      }

      const users = await usersInScope(req.session);
      let best = 0;
      let matches = [];
      for (const u of users) {
        const s = scoreUser(msgNorm, msgWords, u);
        if (s > best) { best = s; matches = [u]; }
        else if (s && s === best) matches.push(u);
      }

      // The question is judged on the words that are not part of a matched
      // name, so a person called "Naman Score" is not an MIS question. Words
      // are spell-corrected against the bot's own vocabulary first, so
      // "pendng", "leav" and "meetng" are understood; name words never are.
      const nameWords = new Set(matches.flatMap(u => norm(u.name).split(' ')));
      const allNameTokens = new Set(users.flatMap(u => norm(u.name).split(' ')));
      const fixed = new Set([...msgWords].map(w => nameWords.has(w) || allNameTokens.has(w) ? w : correctWord(w)));
      const asks = list => list.some(w => fixed.has(w) && !nameWords.has(w));

      // Finding a page of the app. Taken when a page is named with a "where /
      // open / tab" word and either no person is named or "tab"/"page" makes
      // it plain the page is meant ("where is naman's leave" stays a person
      // question). The button only asks the browser to open it; the page's
      // own access check still decides.
      const navPage = asks(NAV_WORDS) ? pageIn(msgNorm) : null;
      // "fms admin kidhar": "admin" also matches the user "Naman Admin", but
      // every name word in the message sits inside the page's own phrase.
      const nameInPage = navPage && matches.length && [...msgWords]
        .filter(w => nameWords.has(w))
        .every(w => navPage.key.split(' ').includes(w));
      if (navPage && (!matches.length || nameInPage || msgWords.has('tab') || msgWords.has('page'))) {
        return res.json({
          reply: `${navPage.label} is in the menu on the left (under More on a phone). Tap below to open it.`,
          open: navPage,
        });
      }

      // An HOD who names someone from another department is told so. The
      // name is scored against everyone, not just the department: "Naman
      // Jain" from an HOD whose department has only Naman Gupta must not
      // fall back to a partial match on "Naman" and answer about the wrong
      // person. If anyone outside the department matches better than the
      // best match inside it, the question is about that outsider. This runs
      // before the "mere"/follow-up fallbacks below, so an outsider's name
      // can never be answered as the asker or as the previous person.
      if (req.session.role === 'hod') {
        const [everyone] = await db.query(
          `SELECT id, name FROM users WHERE role <> 'client' AND client_id IS NULL`);
        const bestAnywhere = Math.max(0, ...everyone.map(u => scoreUser(msgNorm, msgWords, u)));
        if (bestAnywhere > best && bestAnywhere >= 1) {
          const [[me]] = await db.query('SELECT department FROM users WHERE id=?', [req.session.userId]);
          const dept = me?.department ? ` (${me.department})` : '';
          return res.json({
            reply: `As an HOD, you can ask only about yourself and the employees of your department${dept}.`,
          });
        }
      }

      // Team questions: "kiske sabse zyada pending hai", "team ke pending",
      // "sabke pending tasks". Answered for pending tasks across everyone the
      // asker may see; other topics are asked one person at a time.
      if (!matches.length && asks(TEAM_WORDS)) {
        if (asks(COMPLIANCE_WORDS) || (asks(NOT_WORDS) && asks(FILL_WORDS))) {
          if (!(await userCanSee(req.session, 'compliance'))) {
            return res.json({ reply: 'You don\'t have access to Compliance.' });
          }
          return res.json(await teamComplianceReply(users, parsePeriod(msgNorm, 'yesterday'), req));
        }
        if (asks(LEAVE_WORDS) && !asks(EXTRA_WORDS)) {
          const p = parsePeriod(msgNorm, 'today');
          return res.json(await teamLeaveReply(users, p));
        }
        if (asks(MIS_WORDS) || asks(EXTRA_WORDS) || asks(MEETING_WORDS) || asks(DAILY_WORDS) || asks(COMPLETED_WORDS)) {
          return res.json({ reply: 'For the whole team I can show pending tasks and who is on leave. For anything else, ask about one person, for example "Naman Gupta\'s leaves last month".',
            suggestions: ['Pending tasks of the team'] });
        }
        return res.json(await teamPendingReply(users));
      }

      // Who the question is about when no name is typed:
      //   "mere kitne task pending hai" / "my leaves"  → the asker;
      //   "aur uski leave?" right after a question about Naman → Naman again.
      // A follow-up is taken only with a pronoun, or when every word is one
      // the bot knows — an unknown word may be a name it could not match, and
      // answering that as the previous person would be the wrong person. The
      // id comes from the browser, so it must still be in the asker's scope.
      let personFrom = 'name';
      if (!matches.length) {
        const unknown = [...fixed].filter(w => !VOCAB.has(w) && !/^\d+$/.test(w));
        const ctxId = Number(req.body?.context);
        const ctx = ctxId ? users.find(u => u.id === ctxId) : null;
        // Self needs no unknown-word check: nothing in the message matched
        // any name in scope, and an HOD naming an outsider was stopped above.
        if ([...msgWords].some(w => SELF_WORDS.has(w))) {
          const me = users.find(u => u.id === req.session.userId);
          if (me) { matches = [me]; personFrom = 'self'; }
        } else if (ctx && ([...msgWords].some(w => PRONOUN_WORDS.has(w)) || !unknown.length)) {
          matches = [ctx]; personFrom = 'context';
        }
      }
      // What is being asked. Extra working is checked before leave because
      // both are filed on the Leave Tracker and people call it "extra leave".
      const hasDateWords = explicitDates(msgNorm.split(' ')).length > 0 || FULL_MONTHS.some(m => msgWords.has(m))
        || Object.keys(WEEKDAYS).some(d => msgWords.has(d));
      const intent = asks(PAYMENT_WORDS) ? 'payments'
        : asks(INVENTORY_WORDS) ? 'inventory'
        : asks(EXTRA_WORDS) ? 'extra'
        : asks(LEAVE_WORDS) ? 'leave'
        : asks(COMPLIANCE_WORDS) || (asks(NOT_WORDS) && asks(FILL_WORDS)) ? 'compliance'
        : asks(MEETING_WORDS) ? 'meetings'
        // "pending ... kab tak complete honge" is still a pending question:
        // an explicit pending word, with no period, beats "complete".
        : asks(STRICT_PENDING) && !asks(TIME_WORDS) && !hasDateWords ? 'pending'
        : asks(COMPLETED_WORDS) ? 'completed'
        : asks(MIS_WORDS) ? 'mis'
        : asks(DAILY_WORDS) ? 'daily'
        : asks(PROFILE_WORDS) ? 'profile'
        // "Naman ke tasks last week": pending is a count of right now, so a
        // task question with a period is answered as tasks due in that period.
        : (asks(TIME_WORDS) || explicitDates(msgNorm.split(' ')).length || FULL_MONTHS.some(m => msgWords.has(m))) && asks(PENDING_WORDS) ? 'completed'
        // A follow-up that names no topic ("last month?") keeps the previous
        // one. The browser sends it back; only known topics are accepted.
        : personFrom === 'context' && TOPICS.has(req.body?.topic) && !asks(PENDING_WORDS) ? req.body.topic
        : 'pending';
      const period = intent === 'pending' || intent === 'profile' ? null
        : parsePeriod(msgNorm, intent === 'mis' ? 'week' : 'month');
      const askFor = n => ({
        extra: `Extra working of ${n} ${period && period.phrase}`,
        leave: `Leaves of ${n} ${period && period.phrase}`,
        daily: `Daily task hours of ${n} ${period && period.phrase}`,
        compliance: `Compliance of ${n} ${period && period.phrase}`,
        completed: `Completed tasks of ${n} ${period && period.phrase}`,
        meetings: `Meetings of ${n} ${period && period.phrase}`,
        inventory: `Equipment of ${n}`,
        payments: `Payment requests of ${n} ${period && period.phrase}`,
        mis: `MIS score of ${n} ${period && period.phrase}`,
        pending: `Pending tasks of ${n}`,
        profile: `Who is ${n}`,
      })[intent];
      const hasDate = explicitDates(msgNorm.split(' ')).length > 0 || FULL_MONTHS.some(m => msgWords.has(m));

      if (!matches.length) {
        // "uski meetings" with no one asked about before: say what is missing.
        if ([...msgWords].some(w => PRONOUN_WORDS.has(w))) {
          return res.json({ reply: 'Who do you mean? Please type the person\'s name, for example "Naman Gupta\'s meetings last week".' });
        }
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
      // Every answer about one person says who, so the browser can send it back
      // as the context of a follow-up ("aur uski leave?"). A question that
      // also named a second topic ("leave aur pending") gets that one offered
      // as a one-click suggestion rather than silently dropped.
      const second = secondTopic(asks, intent);
      const reply = obj => {
        const out = { ...obj, person: { id: person.id, name: person.name }, topic: intent };
        if (second && !out.suggestions) out.suggestions = [second(person.name)];
        return res.json(out);
      };
      // Pending is the fallback, so it must be earned: a pending/task word, or
      // nothing but the name and filler ("Naman Gupta?"). Anything else —
      // "naman gupta ki position kya hai" — is a question the bot does not
      // know, and answering it with a pending count would be a wrong answer.
      const leftover = [...fixed].filter(w => !nameWords.has(w) && !FILLER_WORDS.has(w)
        && !GREETING_WORDS.has(w) && !GREETING_RE.test(w) && !SELF_WORDS.has(w) && !PRONOUN_WORDS.has(w));
      const unknownQuestion = intent === 'pending' && leftover.length > 0 && !asks(PENDING_WORDS);
      // "mobile number" is contact details, not the mobile phone in Inventory.
      const contactAsk = /\b(mobile|phone|contact|whatsapp|cell)\s+(no|number|num|nmbr)\b/.test(msgNorm);
      if (unknownQuestion || contactAsk || asks(UNSUPPORTED) || (intent === 'pending' && (asks(TIME_WORDS) || hasDate))) {
        return reply({
          reply: 'I can\'t answer that yet.\n' + HELP,
          suggestions: [`Pending tasks of ${person.name}`, `MIS score of ${person.name} this week`,
            `Leaves of ${person.name} this month`, `Extra working of ${person.name} this month`],
        });
      }
      if (period && period.future) {
        return reply({ reply: `${periodText(period).replace(/^from /, 'From ').replace(/^on /, 'On ')} is still ahead, so there is nothing to show yet.` });
      }
      if (intent === 'profile') {
        // Directory facts only — the same fields the Users list gives every
        // dropdown in the app (name, department, role).
        const [[u]] = await db.query(
          `SELECT name, department, COALESCE(user_role, role) AS role,
                  DATE_FORMAT(joining_date,'%d-%m-%Y') AS joined
             FROM users WHERE id = ?`, [person.id]);
        const roleName = { admin: 'Admin', hod: 'HOD', pc: 'PC', user: 'Employee' }[u.role] || u.role;
        const parts = [u.department && `${u.department} department`, roleName, u.joined && `joined ${u.joined}`].filter(Boolean);
        return reply({
          reply: `${u.name}${parts.length ? ' — ' + parts.join(' · ') : ''}.`,
          suggestions: [`Pending tasks of ${u.name}`, `MIS score of ${u.name} this week`, `Leaves of ${u.name} this month`],
        });
      }
      if (intent === 'payments') {
        if (person.id !== req.session.userId && !(await isPaymentApprover(req.session))) {
          return reply({ reply: 'Only an admin or a payment approver can see other people\'s payment requests.' });
        }
        return reply(paymentsReply(person.name, period, await paymentsFor(person.id, period.start, period.end)));
      }
      if (intent === 'inventory') {
        if (person.id !== req.session.userId && !(await userCanDo(req.session, 'edit_inventory'))) {
          return reply({ reply: 'You don\'t have access to other people\'s equipment (Inventory).' });
        }
        return reply(inventoryReply(person.name, await inventoryFor(person.id)));
      }
      if (intent === 'meetings') {
        if (!(await userCanSee(req.session, 'compliance')) || !(await canViewComplianceEmployee(req, person.id))) {
          return reply({ reply: `You don't have access to ${person.name}'s meetings.` });
        }
        return reply(meetingsReply(person.name, period, await meetingsFor(person.id, period.start, period.end)));
      }
      if (intent === 'completed') {
        return reply(completedReply(person.name, period, await completedFor(person.id, period.start, period.end)));
      }
      if (intent === 'compliance') {
        // Gated like the Compliance page: the 'compliance' page permission,
        // then canViewComplianceEmployee (admin anyone, HOD/PC own department).
        if (!(await userCanSee(req.session, 'compliance')) || !(await canViewComplianceEmployee(req, person.id))) {
          return reply({ reply: `You don't have access to ${person.name}'s compliance.` });
        }
        return reply(complianceReply(person.name, period, await complianceFor(person.id, period.start, period.end)));
      }
      if (intent === 'daily') {
        // Anyone can read their own — the Daily Task page shows it to them.
        if (req.session.role !== 'admin' && person.id !== req.session.userId) {
          return reply({ reply: 'Only an admin can see other people\'s daily tasks (Daily Reports).' });
        }
        return reply(dailyReply(person.name, period, await dailyFor(person.id, period.start, period.end)));
      }
      if (intent === 'leave') {
        const days = await leaveDaysFor(person.id, period.start, period.end, ['full_day', 'half_day', 'work_from_home']);
        return reply(leaveReply(person.name, period, days));
      }
      if (intent === 'extra') {
        const days = await leaveDaysFor(person.id, period.start, period.end, ['extra_working']);
        return reply(extraReply(person.name, period, days));
      }
      if (intent === 'mis') {
        const role = req.session.role;
        if (!((role === 'admin' || role === 'hod') && await userCanSee(req.session, 'mis'))) {
          return reply({ reply: 'You don\'t have access to MIS reports.' });
        }
        return reply(misReply(person.name, period, await misFor(person.id, period.start, period.end)));
      }
      reply(pendingReply(person.name, await pendingTasksFor(person.id)));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

};
