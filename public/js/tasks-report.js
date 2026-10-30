// All Tasks: who gave the filtered tasks (each assigner, the doer to themselves),
// day by day on the date given, and a PDF of the same rows. tasks.js calls
// atReportRender with exactly the rows it is about to show.
(function atReportStyle() {
  const s = document.createElement('style');
  s.textContent = `
    .at-rep{display:flex;flex-wrap:wrap;gap:14px 28px;align-items:flex-start;margin:0 0 14px;padding:12px 16px;border:1px solid var(--border,#e2e8f0);border-radius:12px;background:var(--card,#fff)}
    .at-rep-col{flex:1 1 300px;min-width:0}
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
  const t = atReportTables(atReportData(rows), rows.length);
  box.innerHTML = `
    <div class="at-rep">
      <div class="at-rep-col"><div class="at-rep-h">Who gave these ${rows.length} task${rows.length === 1 ? '' : 's'}</div>${t.givers}</div>
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
    userSel && userSel.value !== 'all' ? 'Doer: ' + (userSel.options[userSel.selectedIndex]?.text || '') : 'All doers',
    val('tasksDateFrom') || val('tasksDateTo') ? `Due ${atFmtDay(val('tasksDateFrom')) || '…'} to ${atFmtDay(val('tasksDateTo')) || '…'}` : '',
    val('taskSearch') ? `Search: "${val('taskSearch')}"` : '',
    typeof taskStatusFilter !== 'undefined' ? 'Status: ' + taskStatusFilter : '',
  ].filter(Boolean).join(' · ');
  const list = rows.map((r, i) => `<tr>
    <td class="n">${i + 1}</td><td>${dtEscape(r.description || '')}</td><td>${dtEscape(r.assignedToName || '')}</td>
    <td>${dtEscape(atIsSelf(r) ? 'Self' : (r.assignedByName || ''))}</td><td>${atFmtDay(r.delegated_on)}</td><td>${atFmtDay(r.due_date)}</td>
    <td>${r.completed_on ? atFmtDay(r.completed_on) : ''}</td><td>${dtEscape(r.client_name || '')}</td>
    <td>${dtEscape(r.remarks || '')}</td><td>${dtEscape(r.status || '')}</td></tr>`).join('');
  const html = `<!doctype html><html><head><meta charset="utf-8"><title>Task Report</title><style>
    body{font-family:Inter,Arial,sans-serif;color:#0f172a;margin:24px;font-size:11px}
    h1{font-size:18px;margin:0 0 4px} .meta{color:#475569;margin-bottom:14px}
    .lbl{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;color:#475569;margin:14px 0 6px}
    table{width:100%;border-collapse:collapse} th,td{border:1px solid #cbd5e1;padding:4px 6px;text-align:left;vertical-align:top}
    th{background:#0f172a;color:#fff} tr:nth-child(even) td{background:#f8fafc} .n{text-align:right;white-space:nowrap}
    .tot td{font-weight:700} .cols{display:flex;gap:18px;align-items:flex-start} .cols>div{flex:1}
    @media print{body{margin:10mm} th{-webkit-print-color-adjust:exact;print-color-adjust:exact}}
  </style></head><body>
    <h1>Task Report</h1>
    <div class="meta">${dtEscape(filters)} · ${rows.length} tasks · Generated ${new Date().toLocaleString()}</div>
    <div class="cols"><div><div class="lbl">Who gave the tasks</div>${t.givers}</div><div><div class="lbl">Day by day (date given)</div>${t.days}</div></div>
    <div class="lbl">Tasks</div>
    <table><thead><tr><th class="n">#</th><th>Description</th><th>Doer</th><th>Given by</th><th>Given</th><th>Due</th><th>Completed</th><th>Client</th><th>Remarks</th><th>Status</th></tr></thead><tbody>${list}</tbody></table>
    <script>window.addEventListener('load', () => setTimeout(() => window.print(), 200));<\/script>
  </body></html>`;
  const win = window.open('', '_blank');
  if (!win) { showToast('Please allow pop-ups to export PDF', 'error'); return; }
  win.document.open(); win.document.write(html); win.document.close();
  showToast('🖨 Print dialog will open — choose "Save as PDF"');
}
