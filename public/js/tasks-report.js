// All Tasks: who gave the filtered tasks (each assigner, the doer to themselves),
// day by day on the date given, and a PDF of the same rows. tasks.js calls
// atReportRender with exactly the rows it is about to show.
(function atReportStyle() {
  const s = document.createElement('style');
  s.textContent = `
    .at-rep{display:flex;flex-wrap:wrap;gap:14px 28px;align-items:flex-start;margin:0 0 14px;padding:12px 16px;border:1px solid var(--border,#e2e8f0);border-radius:12px;background:var(--card,#fff)}
    .at-rep-col{flex:1 1 300px;min-width:0;max-height:320px;overflow:auto}
    .at-rep-closed{padding:0}
    .at-rep-head{flex:1 1 100%;display:flex;align-items:center;gap:10px;width:100%;background:none;border:0;padding:4px 0;cursor:pointer;color:inherit;font:inherit;font-size:13px;text-align:left}
    .at-rep-closed .at-rep-head{padding:12px 16px}
    .at-rep-caret{font-size:11px;opacity:.7;width:12px}
    .at-rep-sub{font-size:11px;opacity:.6;margin-left:auto}
    .at-rep-h{font-size:12px;font-weight:700;margin-bottom:6px}
    .at-rep-days{margin-top:8px;max-height:280px;overflow:auto}
    .at-rep-t{width:100%;border-collapse:collapse;font-size:12px}
    .at-rep-t th,.at-rep-t td{padding:4px 8px;border-bottom:1px solid var(--border,#e2e8f0);text-align:left;white-space:nowrap}
    .at-rep-t th{font-size:10px;text-transform:uppercase;letter-spacing:.3px;opacity:.7}
    .at-rep-t .n{text-align:right}
    .at-rep-t .tot td{font-weight:700}`;
  document.head.appendChild(s);
})();
let AT_REPORT_ROWS = [];
let AT_REPORT_DAYS_OPEN = false;
let AT_REPORT_OPEN = false;

const atIsSelf = t => t.assigned_by != null && String(t.assigned_by) === String(t.assigned_to);
const atGiverKey = t => atIsSelf(t) ? '__self' : (t.assignedByName || '—');
const atIsDone = t => t.status === 'completed';

function atReportData(rows) {
  const givers = new Map(), days = new Map();
  for (const t of rows) {
    const k = atGiverKey(t);
    if (!givers.has(k)) givers.set(k, { key: k, name: k === '__self' ? 'Self (doer to themselves)' : k, total: 0, done: 0 });
    const g = givers.get(k);
    g.total++; if (atIsDone(t)) g.done++;
    const d = t.delegated_on || '—';
    if (!days.has(d)) days.set(d, new Map());
    days.get(d).set(k, (days.get(d).get(k) || 0) + 1);
  }
  // Biggest giver first, Self last so it reads as its own line.
  const giverList = [...givers.values()].sort((a, b) => (a.key === '__self') - (b.key === '__self') || b.total - a.total || a.name.localeCompare(b.name));
  const dayList = [...days.keys()].sort((a, b) => b.localeCompare(a)).map(d => ({ day: d, counts: days.get(d) }));
  return { giverList, dayList };
}

const atFmtDay = d => /^\d{4}-\d{2}-\d{2}$/.test(d || '') ? d.split('-').reverse().join('-') : (d || '—');

function atReportTables({ giverList, dayList }, total) {
  const giverRows = giverList.map(g => `<tr><td>${dtEscape(g.name)}</td><td class="n">${g.total}</td><td class="n">${g.done}</td><td class="n">${g.total - g.done}</td></tr>`).join('');
  const head = giverList.map(g => `<th class="n">${dtEscape(g.key === '__self' ? 'Self' : g.name)}</th>`).join('');
  const dayRows = dayList.map(({ day, counts }) => {
    let sum = 0;
    const cells = giverList.map(g => { const c = counts.get(g.key) || 0; sum += c; return `<td class="n">${c || ''}</td>`; }).join('');
    return `<tr><td>${atFmtDay(day)}</td>${cells}<td class="n"><b>${sum}</b></td></tr>`;
  }).join('');
  return {
    givers: `<table class="at-rep-t"><thead><tr><th>Given by</th><th class="n">Total</th><th class="n">Done</th><th class="n">Pending</th></tr></thead>
      <tbody>${giverRows}<tr class="tot"><td>Total</td><td class="n">${total}</td><td class="n">${giverList.reduce((s, g) => s + g.done, 0)}</td><td class="n">${giverList.reduce((s, g) => s + g.total - g.done, 0)}</td></tr></tbody></table>`,
    days: `<table class="at-rep-t"><thead><tr><th>Day given</th>${head}<th class="n">Total</th></tr></thead><tbody>${dayRows}</tbody></table>`,
  };
}

function atReportRender(rows) {
  const box = document.getElementById('atReportBox');
  const pdfBtn = document.getElementById('atPdfBtn');
  const off = typeof tasksType !== 'undefined' && tasksType === 'fms';
  if (pdfBtn) pdfBtn.style.display = off ? 'none' : '';
  AT_REPORT_ROWS = off ? [] : rows;
  if (!box) return;
  if (off || !rows.length) { box.innerHTML = ''; return; }
  const data = atReportData(rows);
  const head = `<button type="button" class="at-rep-head" onclick="AT_REPORT_OPEN=!AT_REPORT_OPEN;atReportRender(AT_REPORT_ROWS)">
      <span class="at-rep-caret">${AT_REPORT_OPEN ? '▾' : '▸'}</span>
      <b>Who gave these ${rows.length} task${rows.length === 1 ? '' : 's'}</b>
      <span class="at-rep-sub">${data.giverList.length} ${data.giverList.length === 1 ? 'person' : 'people'} · click to ${AT_REPORT_OPEN ? 'hide' : 'see'}</span>
    </button>`;
  if (!AT_REPORT_OPEN) { box.innerHTML = `<div class="at-rep at-rep-closed">${head}</div>`; return; }
  const t = atReportTables(data, rows.length);
  box.innerHTML = `
    <div class="at-rep">
      ${head}
      <div class="at-rep-col">${t.givers}</div>
      <div class="at-rep-col">
        <button type="button" class="tg-mini-btn" onclick="AT_REPORT_DAYS_OPEN=!AT_REPORT_DAYS_OPEN;atReportRender(AT_REPORT_ROWS)">
          ${AT_REPORT_DAYS_OPEN ? '▾ Hide' : '▸ Show'} day by day (date given)</button>
        ${AT_REPORT_DAYS_OPEN ? `<div class="at-rep-days">${t.days}</div>` : ''}
      </div>
    </div>`;
}

// Opens a printable page; the browser's "Save as PDF" makes the file.
function atReportPdf() {
  const rows = AT_REPORT_ROWS;
  if (!rows.length) { showToast('No tasks to export', 'error'); return; }

  const t = atReportTables(atReportData(rows), rows.length);
  const val = id => document.getElementById(id)?.value || '';
  const userSel = document.getElementById('tasksUserFilter');

  const filters = [
    userSel && userSel.value !== 'all'
      ? 'Doer: ' + (userSel.options[userSel.selectedIndex]?.text || '')
      : 'All doers',
    val('tasksDateFrom') || val('tasksDateTo')
      ? `Due ${atFmtDay(val('tasksDateFrom')) || '…'} to ${atFmtDay(val('tasksDateTo')) || '…'}`
      : '',
    val('taskSearch') ? `Search: "${val('taskSearch')}"` : '',
    typeof taskStatusFilter !== 'undefined' ? 'Status: ' + taskStatusFilter : '',
  ].filter(Boolean).join(' · ');

  // Escape every dynamic value inserted into HTML.
  const esc = value => dtEscape(String(value ?? ''));

  // Use UTC dates to avoid timezone-related differences.
  const dateValue = value => {
    if (!value) return null;
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value));
    if (!match) return null;
    const date = new Date(Date.UTC(+match[1], +match[2] - 1, +match[3]));
    return Number.isNaN(date.getTime()) ? null : date;
  };

  const daysBetween = (from, to) => {
    const a = dateValue(from);
    const b = dateValue(to);
    if (!a || !b) return null;
    return Math.round((b.getTime() - a.getTime()) / 86400000);
  };

  const today = new Date();
  const todayISO = [
    today.getFullYear(),
    String(today.getMonth() + 1).padStart(2, '0'),
    String(today.getDate()).padStart(2, '0')
  ].join('-');

  const generatedAt = today.toLocaleString();
  const logoUrl = `${location.origin}/emarketing-logo.png`;

  // Task calculations.
  const completed = rows.filter(r => r.status === 'completed');
  const pending = rows.filter(r => r.status === 'pending');

  const lateTasks = completed.filter(r =>
    r.due_date &&
    r.completed_on &&
    dateValue(r.due_date) &&
    dateValue(r.completed_on) &&
    r.completed_on > r.due_date
  );

  const onTimeTasks = completed.filter(r =>
    r.due_date &&
    r.completed_on &&
    dateValue(r.due_date) &&
    dateValue(r.completed_on) &&
    r.completed_on <= r.due_date
  );

  const overdueTasks = pending.filter(r =>
    r.due_date &&
    dateValue(r.due_date) &&
    r.due_date < todayISO
  );

  const summaryCards = [
    { label: 'Total Tasks', value: rows.length, cls: 'total', icon: '▤' },
    { label: 'Completed', value: completed.length, cls: 'completed', icon: '✓' },
    { label: 'Pending', value: pending.length, cls: 'pending', icon: '◷' },
    { label: 'Completed On Time', value: onTimeTasks.length, cls: 'ontime', icon: '✓' },
    { label: 'Completed Late', value: lateTasks.length, cls: 'late', icon: '!' },
    { label: 'Overdue', value: overdueTasks.length, cls: 'overdue', icon: '⚠' }
  ];

  const summaryHTML = summaryCards.map(card => `
    <div class="summary-card ${card.cls}">
      <div class="summary-top">
        <span class="summary-label">${esc(card.label)}</span>
        <span class="summary-icon">${esc(card.icon)}</span>
      </div>
      <div class="summary-value">${esc(card.value)}</div>
    </div>
  `).join('');

  // Status labels and classes.
  const statusInfo = status => {
    switch (status) {
      case 'completed':
        return { label: 'Completed', cls: 'status-completed' };
      case 'pending':
        return { label: 'Pending', cls: 'status-pending' };
      case 'revised':
        return { label: 'Revised', cls: 'status-revised' };
      default:
        return { label: status || 'Unknown', cls: 'status-unknown' };
    }
  };

  // Main task table.
  const list = rows.map((r, i) => {
    const isLate = lateTasks.includes(r);
    const isOverdue = overdueTasks.includes(r);
    const status = statusInfo(r.status);

    let delay = '—';

    if (isLate) {
      const days = daysBetween(r.due_date, r.completed_on);
      delay = days !== null ? `${days} days late` : '—';
    } else if (onTimeTasks.includes(r)) {
      delay = 'On time';
    } else if (isOverdue) {
      const days = daysBetween(r.due_date, todayISO);
      delay = days !== null ? `${days} days overdue` : '—';
    }

    const rowClass = isLate
      ? 'row-late'
      : isOverdue
        ? 'row-overdue'
        : '';

    return `
      <tr class="${rowClass}">
        <td class="number">${esc(i + 1)}</td>
        <td class="description">${esc(r.description || '')}</td>
        <td>${esc(r.assignedToName || '')}</td>
        <td>${esc(atIsSelf(r) ? 'Self' : (r.assignedByName || ''))}</td>
        <td class="date">${esc(atFmtDay(r.delegated_on) || '—')}</td>
        <td class="date">${esc(atFmtDay(r.due_date) || '—')}</td>
        <td class="date">${esc(r.completed_on ? atFmtDay(r.completed_on) : '—')}</td>
        <td class="${isLate || isOverdue ? 'delay-warning' : ''}">${esc(delay)}</td>
        <td>${esc(r.client_name || '—')}</td>
        <td><span class="status-pill ${status.cls}">${esc(status.label)}</span></td>
      </tr>
    `;
  }).join('');

  // Dedicated late-completion section.
  const lateHTML = lateTasks.length
    ? lateTasks.map(r => {
        const days = daysBetween(r.due_date, r.completed_on);
        return `
          <tr class="row-late">
            <td class="description">${esc(r.description || '')}</td>
            <td>${esc(r.assignedToName || '')}</td>
            <td class="date">${esc(atFmtDay(r.due_date) || '—')}</td>
            <td class="date">${esc(atFmtDay(r.completed_on) || '—')}</td>
            <td class="delay-warning">${esc(days !== null ? `${days} days late` : '—')}</td>
            <td>${esc(r.remarks?.trim() || 'No reason given')}</td>
          </tr>
        `;
      }).join('')
    : `
      <tr>
        <td colspan="6" class="all-ontime">
          <span class="success-check">✓</span>
          All completed tasks were done on time.
        </td>
      </tr>
    `;

  const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>E-Marketing | Task Report</title>

<style>
  /* The footer lives in the page's own margin: only there does Chrome fill
     counter(page) / counter(pages), and a position:fixed footer landed on top
     of the repeated table header on page 2. */
  @page {
    size: A4 landscape;
    margin: 12mm 12mm 14mm;
    @bottom-left {
      content: "E-Marketing · Task Report · Generated ${String(generatedAt).replace(/["\\]/g, '')}";
      font-family: Inter, Arial, sans-serif;
      font-size: 8px;
      color: #64748b;
    }
    @bottom-right {
      content: "Page " counter(page) " of " counter(pages);
      font-family: Inter, Arial, sans-serif;
      font-size: 8px;
      color: #64748b;
    }
  }

  * {
    box-sizing: border-box;
  }

  html {
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }

  body {
    font-family: Inter, Arial, sans-serif;
    color: #0f172a;
    margin: 0;
    padding: 0 0 35px;
    font-size: 10px;
    line-height: 1.45;
    background: #fff;
  }

  /* Header */
  .report-header {
    background: #212529;
    color: #fff;
    padding: 17px 22px;
    border-radius: 7px;
    border-bottom: 4px solid #f39c12;
    margin-bottom: 18px;
  }

  .brand-row {
    display: flex;
    align-items: center;
    gap: 13px;
    margin-bottom: 13px;
  }

  .brand-logo {
    width: 48px;
    height: 48px;
    object-fit: contain;
    background: #fff;
    border-radius: 5px;
    padding: 3px;
  }

  .brand-name {
    font-size: 21px;
    font-weight: 750;
    letter-spacing: .4px;
    color: #fff;
    margin: 0;
  }

  .brand-subtitle {
    font-size: 9px;
    color: #cbd5e1;
    letter-spacing: 1.8px;
    text-transform: uppercase;
  }

  .header-divider {
    border-top: 1px solid rgba(255,255,255,.18);
    margin: 11px 0;
  }

  .report-title {
    font-size: 23px;
    font-weight: 750;
    margin: 0 0 4px;
    color: #fff;
  }

  .meta {
    color: #e2e8f0;
    font-size: 9px;
    line-height: 1.6;
    overflow-wrap: anywhere;
  }

  .meta strong {
    color: #fbbf24;
  }

  /* Section headings */
  .section-heading {
    display: flex;
    align-items: center;
    gap: 9px;
    margin: 19px 0 9px;
    font-size: 13px;
    font-weight: 750;
    color: #1e293b;
  }

  .section-heading:before {
    content: '';
    display: inline-block;
    height: 16px;
    width: 4px;
    border-radius: 4px;
    background: #f39c12;
  }

  .lbl {
    font-size: 10px;
    font-weight: 750;
    text-transform: uppercase;
    letter-spacing: .7px;
    color: #475569;
    margin: 0 0 8px;
  }

  /* Summary cards */
  .summary-grid {
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 9px;
    margin-bottom: 18px;
  }

  .summary-card {
    padding: 11px 12px;
    border: 1px solid #e2e8f0;
    border-radius: 7px;
    background: #fff;
    border-top: 3px solid #f39c12;
    min-width: 0;
  }

  .summary-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 4px;
  }

  .summary-label {
    font-size: 9px;
    font-weight: 650;
    color: #64748b;
  }

  .summary-icon {
    font-size: 13px;
    font-weight: 800;
    color: #d97706;
  }

  .summary-value {
    font-size: 23px;
    font-weight: 800;
    color: #0f172a;
    margin-top: 5px;
    font-variant-numeric: tabular-nums;
  }

  .summary-card.completed,
  .summary-card.ontime {
    border-top-color: #16a34a;
  }

  .summary-card.completed .summary-icon,
  .summary-card.ontime .summary-icon {
    color: #16a34a;
  }

  .summary-card.pending,
  .summary-card.overdue {
    border-top-color: #d97706;
  }

  .summary-card.pending .summary-icon,
  .summary-card.overdue .summary-icon {
    color: #d97706;
  }

  .summary-card.late {
    border-top-color: #dc2626;
  }

  .summary-card.late .summary-icon {
    color: #dc2626;
  }

  /* Summary tables */
  .cols {
    display: flex;
    gap: 16px;
    align-items: flex-start;
    margin-bottom: 16px;
  }

  .cols > div {
    flex: 1;
    min-width: 0;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    table-layout: auto;
  }

  th, td {
    border: 1px solid #e2e8f0;
    padding: 6px 7px;
    text-align: left;
    vertical-align: top;
    overflow-wrap: anywhere;
  }

  th {
    background: #d97706;
    color: #fff;
    font-size: 9px;
    font-weight: 750;
    letter-spacing: .15px;
  }

  tbody tr:nth-child(even) td {
    background-color: #f8fafc;
  }

  .number {
    text-align: center;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
  }

  .date {
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
  }

  .description {
    min-width: 150px;
    font-weight: 550;
  }

  /* Main task table */
  .task-table {
    font-size: 9px;
    table-layout: auto;
  }

  .task-table th {
    background: #212529;
    border-color: #374151;
    padding: 8px 6px;
    color: #fff;
  }

  .task-table td {
    padding: 7px 6px;
  }

  .task-table tbody tr:nth-child(even) td {
    background: #f8fafc;
  }

  .task-table .number {
    width: 30px;
  }

  .task-table .description {
    min-width: 175px;
    max-width: 260px;
  }

  .row-late td,
  .row-late:nth-child(even) td {
    background: #fef2f2 !important;
  }

  .row-overdue td,
  .row-overdue:nth-child(even) td {
    background: #fffbeb !important;
  }

  .delay-warning {
    color: #b91c1c;
    font-weight: 750;
    white-space: nowrap;
  }

  /* Status pills */
  .status-pill {
    display: inline-block;
    padding: 3px 8px;
    border-radius: 20px;
    font-size: 8px;
    font-weight: 750;
    white-space: nowrap;
  }

  .status-completed {
    color: #166534;
    background: #dcfce7;
    border: 1px solid #bbf7d0;
  }

  .status-pending {
    color: #92400e;
    background: #fef3c7;
    border: 1px solid #fde68a;
  }

  .status-revised {
    color: #6b21a8;
    background: #f3e8ff;
    border: 1px solid #e9d5ff;
  }

  .status-unknown {
    color: #475569;
    background: #f1f5f9;
    border: 1px solid #e2e8f0;
  }

  /* Late completion section */
  .late-table {
    font-size: 9px;
  }

  .late-table th {
    background: #b45309;
  }

  .all-ontime {
    text-align: center;
    padding: 18px !important;
    color: #166534;
    font-size: 11px;
    font-weight: 700;
    background: #f0fdf4 !important;
  }

  .success-check {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: #dcfce7;
    color: #15803d;
    border-radius: 50%;
    width: 19px;
    height: 19px;
    margin-right: 6px;
  }

  /* Printing */
  @media print {
    body {
      margin: 0;
    }

    .report-header {
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }

    th, .summary-card, .status-pill, .row-late td,
    .row-overdue td, .all-ontime {
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }

    thead {
      display: table-header-group;
    }

    tr {
      page-break-inside: avoid;
      break-inside: avoid;
    }

    .section-heading {
      page-break-after: avoid;
      break-after: avoid;
    }

    .summary-grid,
    .cols {
      page-break-inside: avoid;
      break-inside: avoid;
    }
  }

  @media screen {
    body {
      padding: 20px;
      max-width: 1500px;
      margin: auto;
    }
  }
</style>
</head>

<body>

  <div class="report-header">
    <div class="brand-row">
      <img
        class="brand-logo"
        src="${esc(logoUrl)}"
        alt="E-Marketing Logo"
      >
      <div>
        <div class="brand-name">E-Marketing</div>
        <div class="brand-subtitle">Business Automation & Task Management</div>
      </div>
    </div>

    <div class="header-divider"></div>

    <h1 class="report-title">Task Report</h1>

    <div class="meta">
      <strong>Applied Filters:</strong> ${esc(filters || 'All tasks')}
      &nbsp; · &nbsp;
      <strong>Total Tasks:</strong> ${esc(rows.length)}
      &nbsp; · &nbsp;
      <strong>Generated:</strong> ${esc(generatedAt)}
    </div>
  </div>

  <!-- Summary -->
  <div class="section-heading">Task Summary</div>

  <div class="summary-grid">
    ${summaryHTML}
  </div>

  <!-- Existing summaries -->
  <div class="cols">
    <div>
      <div class="lbl">Who gave the tasks</div>
      ${t.givers}
    </div>

    <div>
      <div class="lbl">Day by day (date given)</div>
      ${t.days}
    </div>
  </div>

  <!-- Complete task table -->
  <div class="section-heading">All Tasks</div>

  <table class="task-table">
    <thead>
      <tr>
        <th class="number">#</th>
        <th>Description</th>
        <th>Doer</th>
        <th>Given by</th>
        <th>Given on</th>
        <th>Due date</th>
        <th>Done on</th>
        <th>Delay</th>
        <th>Client</th>
        <th>Status</th>
      </tr>
    </thead>

    <tbody>
      ${list}
    </tbody>
  </table>

  <!-- Late task details -->
  <div class="section-heading">
    Completed After the Due Date
  </div>

  <table class="late-table">
    <thead>
      <tr>
        <th>Description</th>
        <th>Doer</th>
        <th>Due date</th>
        <th>Done on</th>
        <th>Days late</th>
        <th>Reason</th>
      </tr>
    </thead>

    <tbody>
      ${lateHTML}
    </tbody>
  </table>


  <script>
    window.addEventListener('load', () => {
      setTimeout(() => window.print(), 300);
    });
  <\/script>

</body>
</html>`;

  const win = window.open('', '_blank');

  if (!win) {
    showToast('Please allow pop-ups to export PDF', 'error');
    return;
  }

  win.document.open();
  win.document.write(html);
  win.document.close();

  showToast('🖨 Print dialog will open — choose "Save as PDF"');
}
