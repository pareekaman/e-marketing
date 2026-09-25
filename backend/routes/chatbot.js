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
    scoreFor,
    istMondayOf,
    addDays,
    weeklyPlanVsActual,
    isUserOffOn,
    loadHolidaysSet,
    canViewComplianceEmployee,
  } = deps;

  const MAX_MESSAGE = 300;
  const HELP = 'Ask me about someone\'s tasks, for example:\n' +
    '"How many tasks are pending for Naman Gupta?"\n' +
    '"Naman Gupta\'s MIS score last week"\n' +
    '"Naman Gupta\'s leaves from 1 Sep to 15 Sep"\n' +
    '"Naman Gupta\'s extra working last month"';

  // Any of these picks the kind of question.
  const MIS_WORDS = ['mis', 'score', 'scores', 'performance', 'rating'];
  const LEAVE_WORDS = ['leave', 'leaves', 'chutti', 'chhutti', 'chuttiyan', 'chhuttiyan', 'chuttiya', 'wfh'];
  const EXTRA_WORDS = ['extra', 'overtime'];
  const DAILY_WORDS = ['daily', 'ghante', 'hours', 'hour', 'timesheet'];
  // Compliance = which working days the Daily Task was not filled. Needs
  // either a compliance word or "not filled" in some form, because "kitne
  // ghante daily task bhara" (hours logged) also contains "bhara".
  const COMPLIANCE_WORDS = ['compliance', 'missed', 'miss', 'skipped'];
  const NOT_WORDS = ['nahi', 'nhi', 'not', 'nahin'];
  const FILL_WORDS = ['bhara', 'bhari', 'bhare', 'fill', 'filled'];

  // Words that ask for something this version cannot answer yet. Checked so
  // "completed tasks of Naman" gets an honest "not yet" rather than a pending
  // count that looks like an answer to the question asked.
  const UNSUPPORTED = [
    'completed', 'complete', 'done', 'finished', 'closed', 'report', 'rank',
    'attendance', 'holiday', 'salary',
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
      if (end > today) end = today;
      if (start > end) start = end;
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
    return fallback === 'month'
      ? range(firstOfMonth, today, 'this month')
      : range(thisMon, today, 'this week');
  }
  // The period as it reads inside a sentence: "in last month (01-08-2026 to
  // 31-08-2026)", "from 01-09-2026 to 15-09-2026", "on 08-09-2026".
  const periodText = (p, prep = 'in') => p.label ? `${prep} ${p.label} (${dmy(p.start)} to ${dmy(p.end)})`
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
      `SELECT DATE_FORMAT(entry_date,'%Y-%m-%d') AS d, client_name, duration_min
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
    return {
      reply: `${name} logged ${hm(total)} of daily tasks ${when}, on ${days} day${days === 1 ? '' : 's'}` +
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
      // What is being asked. Extra working is checked before leave because
      // both are filed on the Leave Tracker and people call it "extra leave".
      const intent = asks(EXTRA_WORDS) ? 'extra'
        : asks(LEAVE_WORDS) ? 'leave'
        : asks(COMPLIANCE_WORDS) || (asks(NOT_WORDS) && asks(FILL_WORDS)) ? 'compliance'
        : asks(DAILY_WORDS) ? 'daily'
        : asks(MIS_WORDS) ? 'mis'
        : 'pending';
      const period = intent === 'pending' ? null : parsePeriod(msgNorm, intent === 'mis' ? 'week' : 'month');
      const askFor = n => ({
        extra: `Extra working of ${n} ${period && period.phrase}`,
        leave: `Leaves of ${n} ${period && period.phrase}`,
        daily: `Daily task hours of ${n} ${period && period.phrase}`,
        compliance: `Compliance of ${n} ${period && period.phrase}`,
        mis: `MIS score of ${n} ${period && period.phrase}`,
        pending: `Pending tasks of ${n}`,
      })[intent];
      const hasDate = explicitDates(msgNorm.split(' ')).length > 0 || FULL_MONTHS.some(m => msgWords.has(m));

      // An HOD who names someone from another department is told so. The
      // name is scored against everyone, not just the department: "Naman
      // Jain" from an HOD whose department has only Naman Gupta must not
      // fall back to a partial match on "Naman" and answer about the wrong
      // person. If anyone outside the department matches better than the
      // best match inside it, the question is about that outsider.
      if (req.session.role === 'hod') {
        const [everyone] = await db.query(
          `SELECT id, name FROM users WHERE role <> 'client' AND client_id IS NULL`);
        const bestAnywhere = Math.max(0, ...everyone.map(u => scoreUser(msgNorm, msgWords, u)));
        if (bestAnywhere > best) {
          const [[me]] = await db.query('SELECT department FROM users WHERE id=?', [req.session.userId]);
          const dept = me?.department ? ` (${me.department})` : '';
          return res.json({
            reply: `As an HOD, you can ask only about yourself and the employees of your department${dept}.`,
          });
        }
      }

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
      if (asks(UNSUPPORTED) || (intent === 'pending' && (asks(TIME_WORDS) || hasDate))) {
        return res.json({
          reply: 'I can\'t answer that yet. For now I can show someone\'s pending tasks, MIS score, leaves or extra working.',
          suggestions: [`Pending tasks of ${person.name}`, `MIS score of ${person.name} this week`,
            `Leaves of ${person.name} this month`, `Extra working of ${person.name} this month`],
        });
      }
      if (intent === 'compliance') {
        // Gated like the Compliance page: the 'compliance' page permission,
        // then canViewComplianceEmployee (admin anyone, HOD/PC own department).
        if (!(await userCanSee(req.session, 'compliance')) || !(await canViewComplianceEmployee(req, person.id))) {
          return res.json({ reply: `You don't have access to ${person.name}'s compliance.` });
        }
        return res.json(complianceReply(person.name, period, await complianceFor(person.id, period.start, period.end)));
      }
      if (intent === 'daily') {
        // Anyone can read their own — the Daily Task page shows it to them.
        if (req.session.role !== 'admin' && person.id !== req.session.userId) {
          return res.json({ reply: 'Only an admin can see other people\'s daily tasks (Daily Reports).' });
        }
        return res.json(dailyReply(person.name, period, await dailyFor(person.id, period.start, period.end)));
      }
      if (intent === 'leave') {
        const days = await leaveDaysFor(person.id, period.start, period.end, ['full_day', 'half_day', 'work_from_home']);
        return res.json(leaveReply(person.name, period, days));
      }
      if (intent === 'extra') {
        const days = await leaveDaysFor(person.id, period.start, period.end, ['extra_working']);
        return res.json(extraReply(person.name, period, days));
      }
      if (intent === 'mis') {
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
