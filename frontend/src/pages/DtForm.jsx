// Daily Task → the form: Daily / Extra Working mode, the clock, the entry date,
// the rows (client, department, what was done, minutes; Dup / Del), the total,
// Extra Working's proof files and Submit. Was loadDailyForm … dtSubmitExtra in
// public/js/daily.js, which still loads the clients and departments (handed
// over through dtFormSetReact each time the page opens), checks rows
// (dtCheckRows), sends (dtSubmitSend / dtSubmitExtraSend), keeps the proof
// files (EW_ATTACH.dt in leaves.js) and draws the history (dtLoadHistory).
import { useEffect, useRef, useState } from 'react';
import { api, legacy, css, me, toast } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const ATTACH_MAX = 5;
const ATTACH_BOX = 'display:none;margin:14px 0 4px;padding:14px 16px;border:1px dashed #cbd5e1;border-radius:12px;background:#fff';
// The old form showed / hid its parts by setting style.display; done the same
// way here so the style attributes read the same.
const displayed = (base, value) => el => { if (!el) return; if (base != null) el.setAttribute('style', base); if (value !== undefined) el.style.display = value; };
const two = n => String(n).padStart(2, '0');
const ymd = d => `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())}`;
const dmy = d => `${two(d.getDate())}/${two(d.getMonth() + 1)}/${d.getFullYear()}`;
const clockText = () => { const n = new Date(); return `${dmy(n)} ${two(n.getHours())}:${two(n.getMinutes())}:${two(n.getSeconds())}`; };
// Today and yesterday only.
const dateOptions = () => [0, 1].map(i => { const d = new Date(); d.setDate(d.getDate() - i); return { value: ymd(d), label: `${i ? 'Yesterday' : 'Today'} (${dmy(d)})` }; });

let rowSeq = 0;
const newRow = (prefill, departments) => {
  // Most rows are for the user's own department; a duplicated row keeps its own (even none).
  const dept = prefill ? prefill.dept : ((me() && me().department) || '');
  return { key: ++rowSeq, client: prefill ? prefill.client : '', dept: departments.includes(dept) ? dept : '', desc: prefill ? prefill.desc : '', time: prefill ? prefill.time : '' };
};

export default function DtForm() {
  const [data, setData] = useState(null);          // { clients, departments, opened }
  const [mode, setMode] = useState('daily');
  const [date, setDate] = useState(() => dateOptions()[0].value);
  const [check, setCheck] = useState(null);        // { locked, notice } after the date's check
  const [rows, setRows] = useState([]);
  const [, setAttachTick] = useState(0);
  const [busyText, setBusyText] = useState(null);  // the submit button while it works
  const [clock, setClock] = useState(clockText);
  const fileIn = useRef(null);
  const name = (me() && me().name) || '';

  useEffect(() => { const t = setInterval(() => setClock(clockText()), 1000); return () => clearInterval(t); }, []);

  // Is this date already submitted (daily) / requested (extra)? If not, a fresh row.
  const runCheck = async (m, day, depts) => {
    let locked = false, notice = null;
    if (m === 'extra') {
      await legacy('dtLoadExtraMine');
      const hit = legacy('dtExtraRequestFor', day);
      locked = !!hit;
      if (hit) notice = <>🔒 Extra Working for this date is already submitted — status: <b>{hit.status}</b>.{hit.status === 'pending' ? ' Delete it from Leave Tracker if you need to change it.' : ''}</>;
    } else {
      try { const r = await api('/api/daily-tasks/status?date=' + day); locked = !!r.submitted; } catch (e) { locked = false; }
      if (locked) notice = '🔒 You have already submitted for this date — entries are now locked.';
    }
    if (!locked) { setRows([newRow(null, depts)]); legacy('ewAttachReset', 'dt'); setAttachTick(t => t + 1); }
    setCheck(c => ({ locked, notice: notice !== null ? notice : (c && c.notice) }));
  };

  // Each time the page opens: Daily mode, today, then the check and the history.
  useEffect(() => {
    legacy('dtFormSetReact', async view => {
      setData(view);
      setMode('daily');
      const today = dateOptions()[0].value;
      setDate(today);
      await runCheck('daily', today, view.departments);
      await legacy('dtLoadHistory');
    });
    return () => legacy('dtFormSetReact', null);
  }, []);

  if (!data) return null;
  const depts = data.departments;
  const extra = mode === 'extra';
  const locked = !!(check && check.locked);

  const switchMode = async m => {
    if (m === mode) return;
    setMode(m);
    legacy('dtModeSet', m);
    await runCheck(m, date, depts);
    await legacy('dtLoadHistory');
  };
  const changeDate = d => { setDate(d); runCheck(mode, d, depts); };

  const clientOpts = selected => [{ value: '', label: '--select--' },
    ...(legacy('sortClientsForPicker', data.clients) || [])
      .filter(c => legacy('isActiveClient', c) || String(c.id) === String(selected))
      .map(c => ({ value: String(c.id), label: legacy('clientLabel', c) }))];
  const deptOpts = [{ value: '', label: '--select--' }, ...depts.map(d => ({ value: d, label: d }))];
  const setRow = (key, patch) => setRows(rs => rs.map(r => (r.key === key ? { ...r, ...patch } : r)));
  const dup = r => setRows(rs => [...rs, newRow(r, depts)]);
  const del = key => {
    if (rows.length <= 1) return toast('At least 1 row required', 'error');
    setRows(rs => rs.filter(r => r.key !== key));
  };
  const total = rows.reduce((a, r) => { const v = parseInt(r.time) || 0; return v > 0 ? a + v : a; }, 0);
  const files = legacy('ewAttachList', 'dt') || [];

  const submit = async () => {
    if (locked) return toast(extra ? 'Extra Working already submitted for this date' : 'Already submitted for this date', 'error');
    const read = legacy('dtCheckRows', rows.map(r => ({ client: r.client, dept: r.dept, desc: r.desc, time: r.time })));
    if (!read) return;
    if (extra && !files.length) return toast('Attach at least one file (a screenshot or PDF) as proof of the work.', 'error');
    setBusyText('Submitting...');
    try {
      const ok = await legacy(extra ? 'dtSubmitExtraSend' : 'dtSubmitSend', date, read, t => setBusyText(t));
      if (ok) { await runCheck(mode, date, depts); await legacy('dtLoadHistory'); }
    } finally { setBusyText(null); }
  };

  return (
    <>
      <div className="dt-header">
        <div className="dt-header-text">
          <div className="dt-title-row">
            <div className="dt-page-title" id="dtPageTitle">{extra ? 'Extra Working' : 'Daily Task'}</div>
            <div className="dt-mode-switch">
              <button type="button" className={`dt-mode-btn${extra ? '' : ' active'}`} id="dtModeBtnDaily" onClick={() => switchMode('daily')}>📋 Daily Task</button>
              <button type="button" className={`dt-mode-btn${extra ? ' active' : ''}`} id="dtModeBtnExtra" onClick={() => switchMode('extra')}>⚡ Extra Working</button>
            </div>
          </div>
          <div className="dt-page-sub" id="dtPageSub">{extra
            ? 'Log work done outside office hours by client and department. Goes to your HOD for approval.'
            : 'Log the time you spent today by client and department.'}</div>
        </div>
        <div className="dt-topbar">
          <div className="dt-time" id="dtNow">{clock}</div>
          <div className="dt-welcome">Welcome <b id="dtUserName">{name}</b></div>
        </div>
      </div>

      <div className="dt-meta-row">
        <div className="dt-meta-field">
          <label>Entry Date</label>
          <SearchSelect value={date} onChange={changeDate} options={dateOptions()} />
        </div>
        <div className="dt-meta-field">
          <label>Doer</label>
          <input type="text" id="dtDoerName" disabled value={name} readOnly />
        </div>
      </div>

      {/* Already-submitted notice (text swaps with the active mode) */}
      <div id="dtLockedNotice" className="dt-locked" ref={displayed('display:none', check ? (locked ? 'block' : 'none') : undefined)}>
        {check && check.notice ? check.notice : '🔒 You have already submitted for this date — entries are now locked.'}
      </div>

      <div className="dt-table-wrap" id="dtTableWrap" ref={displayed(null, check ? (locked ? 'none' : 'block') : undefined)}>
        <table className="dt-table">
          <thead>
            <tr>
              <th ref={css('width:24%')}>Client Name</th>
              <th ref={css('width:22%')}>Department</th>
              <th>Task Description</th>
              <th ref={css('width:90px')}>Time (min)</th>
              <th ref={css('width:72px')}>Dup</th>
              <th ref={css('width:72px')}>Del</th>
            </tr>
          </thead>
          <tbody id="dtRowsBody">
            {rows.map(r => (
              <tr key={r.key} className="dt-row">
                <td><SearchSelect className="dt-client" value={r.client} onChange={v => setRow(r.key, { client: v })} options={clientOpts(r.client)} placeholder="--select--" /></td>
                <td><SearchSelect className="dt-dept" value={r.dept} onChange={v => setRow(r.key, { dept: v })} options={deptOpts} placeholder="--select--" /></td>
                <td><textarea className="dt-desc" placeholder="What did you do?" value={r.desc} onChange={e => setRow(r.key, { desc: e.target.value })} /></td>
                <td><input type="number" min="1" className="dt-time" placeholder="0" value={r.time} onChange={e => setRow(r.key, { time: e.target.value })} /></td>
                <td><button className="dt-row-btn dt-btn-dup" onClick={() => dup(r)}>Dup</button></td>
                <td><button className="dt-row-btn dt-btn-del" onClick={() => del(r.key)}>Del</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="dt-summary">
        <span className="dt-summary-label">Total Duration</span>
        <div>
          <span className="dt-summary-val" id="dtTotalMin">{total}</span><span className="dt-summary-val-suffix">min</span>
        </div>
      </div>

      {/* Extra Working only: proof files, required (EW_ATTACH.dt in leaves.js) */}
      <div id="dtAttachGroup" ref={displayed(ATTACH_BOX, check ? (extra && !locked ? '' : 'none') : undefined)}>
        <div ref={css('font-size:12px;font-weight:700;color:#334155;text-transform:uppercase;letter-spacing:.4px;margin-bottom:6px')}>Attachments <span ref={css('color:#dc2626')}>*</span>
          {' '}<span ref={css('font-weight:500;text-transform:none;letter-spacing:0;color:#94a3b8')}>JPG, PNG or PDF · 1 to 5 files · proof of the work</span></div>
        <input type="file" id="dtAttachInput" ref={fileIn} accept=".jpg,.jpeg,.png,.pdf,image/jpeg,image/png,application/pdf" multiple
          onChange={e => { legacy('ewAttachPick', 'dt', e.target); setAttachTick(t => t + 1); }} />
        {/* No id here: leaves.js's ewAttachRender writes into #dtAttachList. */}
        <div>
          {check ? <>
            {files.map((a, i) => (
              <span key={i} ref={css('display:inline-flex;align-items:center;gap:6px;padding:4px 10px;margin:6px 6px 0 0;border:1px solid #e2e8f0;border-radius:999px;background:#f8fafc;font-size:12px')}>
                {`📎 ${a.file.name} `}<span ref={css('color:#94a3b8')}>{legacy('ewAttachSize', a.file.size)}</span>
                <button type="button" title="Remove" ref={css('border:none;background:none;color:#dc2626;cursor:pointer;font-size:13px;padding:0 2px')}
                  onClick={() => { legacy('ewAttachRemove', 'dt', i); setAttachTick(t => t + 1); }}>✕</button>
              </span>
            ))}
            <div ref={css('font-size:11px;color:#94a3b8;margin-top:6px')}>{`${files.length} of ${ATTACH_MAX} files`}</div>
          </> : null}
        </div>
      </div>

      <div className="dt-actions" id="dtActions" ref={displayed(null, check ? (locked ? 'none' : 'flex') : undefined)}>
        <button className="dt-btn dt-btn-add" onClick={() => setRows(rs => [...rs, newRow(null, depts)])}>+ Add Row</button>
        <button className="dt-btn dt-btn-submit" disabled={busyText !== null} onClick={submit}>
          {busyText !== null ? busyText : extra ? 'Submit for Approval →' : 'Submit All →'}</button>
      </div>
    </>
  );
}
