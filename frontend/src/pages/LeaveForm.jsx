// Leave Tracker → the apply form (was #leaveModal in app.html and
// openLeaveForm … saveLeave in public/js/leaves.js). openLeaveForm opens it
// through lvFormSetReact. leaves.js still loads the client list and holds the
// Extra Working proof files (EW_ATTACH.lv, uploaded by ewAttachUpload), which
// the Daily Task form shares; this page reads them through small getters.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import Modal from '../components/Modal.jsx';
import { api, me, toast, legacy, css, fmtDate } from '../lib/app.js';

const TYPES = [
  { key: 'full_day', ico: '🛌', label: 'Full Day Leave' },
  { key: 'half_day', ico: '⏱', label: 'Half Day Leave' },
  { key: 'work_from_home', ico: '🏠', label: 'Work From Home' },
  { key: 'extra_working', ico: '⚡', label: 'Extra Working' },
];
const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const pad = n => String(n).padStart(2, '0');
const dateKey = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
const newRow = () => ({ client: '', description: '', hours: '' });
const label = n => legacy('lvClientLabelOf', n) ?? n;
const ATTACH_MAX = 5;

function stamp() {
  const now = new Date();
  return `${pad(now.getDate())}-${SHORT[now.getMonth()]}-${now.getFullYear()} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

// Searchable client picker under a row's client box. It lives on <body>: in
// the modal, position:fixed re-anchors to .modal (its entrance animation
// leaves a transform behind), which sent the list into scrolled-away content.
function ClientCombo({ combo, names, onPick }) {
  const ref = useRef(null);
  const q = combo.text.trim().toLowerCase();
  // Match the brand shown or the client's own name.
  const matches = names.filter(n => !q || n.toLowerCase().includes(q) || label(n).toLowerCase().includes(q)).slice(0, 50);
  // Under the input; flip above / pull left when the window runs out.
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const r = combo.rect;
    el.style.minWidth = r.width + 'px';
    el.style.left = r.left + 'px';
    el.style.top = (r.bottom + 2) + 'px';
    const w = el.offsetWidth, h = el.offsetHeight;
    if (r.left + w > window.innerWidth - 8) el.style.left = Math.max(8, window.innerWidth - 8 - w) + 'px';
    if (r.bottom + h + 8 > window.innerHeight) el.style.top = Math.max(8, r.top - h - 2) + 'px';
  });
  if (!matches.length) return null;
  return createPortal(
    <div ref={ref} className="lv-client-combo-list" style={{ display: 'block' }}>
      {matches.map(n => (
        <div key={n} className="lv-client-combo-item" data-name={n} onMouseDown={e => { e.preventDefault(); onPick(n); }}>{label(n)}</div>
      ))}
    </div>,
    document.body
  );
}

export default function LeaveForm() {
  const [open, setOpen] = useState(false);
  const [ts, setTs] = useState('');
  const [type, setType] = useState('');
  const [picked, setPicked] = useState(() => new Set());     // date keys
  const [rows, setRows] = useState({});                       // Extra Working: key → [{client, description, hours}]
  const [view, setView] = useState(() => new Date());         // first of the month on show
  const [reason, setReason] = useState('');
  const [err, setErr] = useState('');
  const [saving, setSaving] = useState(false);
  const [saveText, setSaveText] = useState('Save');
  const [approver, setApprover] = useState('…');
  const [names, setNames] = useState([]);                     // client picker, Internal first
  const [, setAttachTick] = useState(0);
  const [combo, setCombo] = useState(null);                   // { key, idx, text, rect }
  const errRef = useRef(null);
  const savingRef = useRef(false);
  const user = me() || {};

  const loadApprover = t => {
    setApprover('…');
    api('/api/leaves/my-approvers' + (t === 'extra_working' ? '?type=extra_working' : '')).then(r => setApprover(r?.names || 'HOD')).catch(() => {
      if (user.role === 'admin') setApprover('Another Admin');
      else if (user.role === 'hod' || user.role === 'pc') setApprover('Admin');
      else setApprover(`HOD${user.department ? ' — ' + user.department : ''}`);
    });
  };

  // openLeaveForm: a fresh form every time.
  useEffect(() => {
    legacy('lvFormSetReact', () => {
      setTs(stamp()); setType(''); setPicked(new Set()); setRows({}); setReason(''); setErr('');
      setSaving(false); setSaveText('Save'); setCombo(null);
      const v = new Date(); v.setDate(1); setView(v);
      legacy('ewAttachReset', 'lv'); setAttachTick(t => t + 1);
      Promise.resolve(legacy('lvLoadClients')).then(() => setNames(legacy('lvClientNames') || []));
      loadApprover('');
      setOpen(true);
    });
    return () => legacy('lvFormSetReact', null);
  }, []);
  // Show the message where the eye is: it sits at the top, Save at the bottom.
  useEffect(() => {
    if (!err || !errRef.current) return;
    try { errRef.current.scrollIntoView({ block: 'center', behavior: 'smooth' }); } catch { errRef.current.scrollIntoView(); }
  }, [err]);

  const isExtra = type === 'extra_working';
  const rowsFor = k => rows[k] || [newRow()];
  const setRow = (k, i, field, value) => setRows(m => {
    const list = (m[k] || [newRow()]).map((r, j) => (j === i ? { ...r, [field]: value } : r));
    return { ...m, [k]: list };
  });
  const toggleDate = k => {
    setCombo(null);
    setPicked(s => { const n = new Set(s); n.has(k) ? n.delete(k) : n.add(k); return n; });
    setRows(m => { if (!(k in m)) return m; const n = { ...m }; delete n[k]; return n; });
  };
  const pickType = t => { setType(t); setCombo(null); loadApprover(t); };
  const total = k => Math.round(rowsFor(k).reduce((s, r) => s + (parseFloat(r.hours) || 0), 0) * 100) / 100;

  // ── calendar ──
  const year = view.getFullYear(), month = view.getMonth();
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const todayKey = dateKey(today);
  const minAllowed = new Date(today); minAllowed.setDate(minAllowed.getDate() - 33);
  const days = [];
  for (let i = 0; i < new Date(year, month, 1).getDay(); i++) days.push(<button key={'b' + i} type="button" className="lv-cal-day lv-cal-day-other" disabled></button>);
  for (let d = 1; d <= new Date(year, month + 1, 0).getDate(); d++) {
    const cur = new Date(year, month, d), k = dateKey(cur);
    const past = cur < minAllowed, sel = picked.has(k);
    const cls = ['lv-cal-day'];
    if (past) cls.push('lv-cal-day-disabled');
    if (k === todayKey && !sel) cls.push('lv-cal-day-today');
    if (sel) cls.push('lv-cal-day-selected');
    days.push(<button key={k} type="button" className={cls.join(' ')} disabled={past} onClick={() => toggleDate(k)}>{d}</button>);
  }
  const nav = dir => setView(v => { const n = new Date(v); n.setMonth(n.getMonth() + dir); return n; });
  const sorted = [...picked].sort();

  // ── proof files (leaves.js keeps them; the Daily Task form uses the same store) ──
  const files = legacy('ewAttachList', 'lv') || [];
  const pickFiles = input => { legacy('ewAttachPick', 'lv', input); setAttachTick(t => t + 1); };
  const removeFile = i => { legacy('ewAttachRemove', 'lv', i); setAttachTick(t => t + 1); };

  async function save() {
    setErr('');
    if (!type) return setErr('Please pick a Leave Type');
    if (!picked.size) return setErr('Select at least one date');
    const text = reason.trim();
    if (!isExtra && !text) return setErr('Reason is required');
    const dates = [];
    for (const k of sorted) {
      const item = { date: k };
      if (isExtra) {
        // Keep only rows the user actually touched; require complete rows
        const touched = rowsFor(k).filter(r => r.client || (r.description || '').trim() || r.hours);
        if (!touched.length) return setErr(`Add at least one client row for ${fmtDate(k)}`);
        const entries = [];
        for (const row of touched) {
          let client = (row.client || '').trim();
          const description = (row.description || '').trim();
          const h = parseFloat(row.hours);
          if (!client) return setErr(`Select a client for ${fmtDate(k)}`);
          // Normalize a typed name to the list entry; reject unknown names
          // (skipped when the client list failed to load, so submission never dead-ends)
          if (legacy('lvHasClients')) {
            const match = legacy('lvClientCanonicalOf', client);
            if (!match) return setErr(`"${client}" is not in the client list (${fmtDate(k)}) — pick from the suggestions`);
            client = match;
          }
          if (!description) return setErr(`Enter task description for ${client} on ${fmtDate(k)}`);
          if (!h || h <= 0) return setErr(`Enter hours for ${client} on ${fmtDate(k)}`);
          entries.push({ client, description, hours: h });
        }
        item.entries = entries;
      }
      dates.push(item);
    }
    if (isExtra && !files.length) return setErr('Attach at least one file (a screenshot or PDF) as proof of the work.');
    // A second click while the first request is still in flight used to file the
    // same leave twice. A ref, not state: two clicks can land in one render.
    if (savingRef.current) return;
    savingRef.current = true;
    setSaving(true);
    let r;
    try {
      let attachments;
      if (isExtra) {
        try { attachments = await legacy('ewAttachUpload', 'lv', (i, n, pct) => setSaveText(`Uploading ${i}/${n} (${pct}%)…`)); }
        catch (e) { return setErr('Upload failed: ' + e.message); }
      }
      r = await api('/api/leaves', 'POST', { leave_type: type, dates, reason: text, ...(isExtra ? { attachments } : {}) });
    } finally {
      savingRef.current = false;
      setSaving(false); setSaveText('Save');
    }
    if (r.error) return setErr(r.error);
    if (isExtra) legacy('ewAttachReset', 'lv');
    setOpen(false);
    toast('✅ Leave request submitted for approval');
    legacy('loadLeaves');
    legacy('loadApprovalBadge');
  }

  const openCombo = (k, i, input) => {
    const t = input.value;
    setRow(k, i, 'client', legacy('lvClientCanonicalOf', t) || t);
    setCombo({ key: k, idx: i, text: t, rect: input.getBoundingClientRect() });
  };
  // mousedown on an item fires before blur, so a pick still lands; keep the
  // list when focus moved straight into another client box.
  const blurCombo = input => setTimeout(() => setCombo(c => (c && document.activeElement !== input && !document.activeElement?.classList?.contains('lv-client-in') ? null : c)), 150);

  return (
    <Modal open={open} onClose={() => { setOpen(false); setCombo(null); }} className="lv-modal" closeX={false}>
      <div className="lv-modal-head">
        <h3 className="lv-modal-title">🗓 Leave Tracker Form</h3>
        <button className="lv-modal-close" onClick={() => setOpen(false)}>✕</button>
      </div>
      <div ref={errRef} className="alert error" style={{ display: err ? 'block' : 'none' }}>{err}</div>

      <div className="form-group">
        <label>Timestamp <span className="lv-req">*</span></label>
        <input type="text" value={ts} disabled readOnly />
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Employee Name <span className="lv-req">*</span></label>
          <input type="text" value={user.name || ''} disabled readOnly />
        </div>
        <div className="form-group">
          <label>Employee Email <span className="lv-req">*</span></label>
          <input type="text" value={user.email || ''} disabled readOnly />
        </div>
      </div>

      <div className="form-group">
        <label>Leave Type <span className="lv-req">*</span></label>
        <div className="lv-type-grid">
          {TYPES.map(t => (
            <button key={t.key} type="button" className={`lv-type-btn${type === t.key ? ' active' : ''}`} data-type={t.key} onClick={() => pickType(t.key)}>
              <span className="lv-type-ico">{t.ico}</span>{` ${t.label}`}
            </button>
          ))}
        </div>
      </div>

      <div className="form-group">
        <label>Select Dates <span className="lv-req">*</span> <span className="lv-cal-hint">📅 (click individual dates to select/deselect)</span></label>
        <div className="lv-calendar">
          <div className="lv-cal-head">
            <button type="button" className="lv-cal-nav" onClick={() => nav(-1)}>‹</button>
            <div className="lv-cal-month-label">{`${MONTHS[month]} ${year}`}</div>
            <button type="button" className="lv-cal-nav" onClick={() => nav(1)}>›</button>
          </div>
          <div className="lv-cal-weekdays">
            <span>Su</span><span>Mo</span><span>Tu</span><span>We</span><span>Th</span><span>Fr</span><span>Sa</span>
          </div>
          <div className="lv-cal-grid">{days}</div>
          <div className="lv-cal-foot">
            <span className="lv-cal-count-ico">📅</span>
            <span>{picked.size === 0 ? '0 dates selected' : `${picked.size} date${picked.size === 1 ? '' : 's'} selected`}</span>
          </div>
        </div>
      </div>

      {picked.size ? (
        <div className="form-group">
          <label>{isExtra ? 'Work Details per Date' : 'Selected Dates'}</label>
          <div className={`lv-selected-list${isExtra ? ' lv-extra-mode' : ''}`}>
            {!isExtra ? (
              <div className="lv-selected-row">
                <span ref={css('font-size:14px')}>📅</span>
                <span className="lv-selected-date">{sorted.length === 1 ? fmtDate(sorted[0]) : `${sorted.length} days leave — ${fmtDate(sorted[0])} … ${fmtDate(sorted[sorted.length - 1])}`}</span>
              </div>
            ) : sorted.map(k => (
              // Extra working — client/task rows per date (Daily Task style)
              <div key={k} className="lv-extra-date-block">
                <div className="lv-extra-date-head">
                  <span>{`📅 ${fmtDate(k)}`}</span>
                  <span className="lv-extra-date-total">{`${total(k)}h`}</span>
                  <button type="button" className="lv-selected-remove" onClick={() => toggleDate(k)}>✕</button>
                </div>
                {rowsFor(k).map((row, i) => (
                  <div key={i} className="lv-extra-row">
                    <input type="text" className="lv-client-in" placeholder="Client…" value={label(row.client || '')}
                      onChange={e => openCombo(k, i, e.target)} onFocus={e => openCombo(k, i, e.target)} onBlur={e => blurCombo(e.target)} />
                    <textarea className="lv-extra-desc" rows={2} placeholder="What did you do?" value={row.description || ''}
                      onChange={e => setRow(k, i, 'description', e.target.value)} />
                    <input type="number" min="0.5" max="24" step="0.5" placeholder="hrs" value={row.hours || ''}
                      onChange={e => setRow(k, i, 'hours', e.target.value)} />
                    <button type="button" className="lv-extra-row-del" title="Remove row" onClick={() => {
                      setCombo(null);
                      setRows(m => { const list = rowsFor(k).filter((_, j) => j !== i); return { ...m, [k]: list.length ? list : [newRow()] }; });
                    }}>✕</button>
                  </div>
                ))}
                <button type="button" className="lv-extra-add-row" onClick={() => { setCombo(null); setRows(m => ({ ...m, [k]: [...rowsFor(k), newRow()] })); }}>+ Add Client Row</button>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {!isExtra ? (
        <div className="form-group">
          <label>Reason <span className="lv-req">*</span></label>
          <textarea placeholder="Reason for leave (sick, personal work, urgent matter…)" value={reason} onChange={e => setReason(e.target.value)} />
        </div>
      ) : (
        // Extra Working only: proof files, required
        <div className="form-group">
          <label>Attachments <span className="lv-req">*</span> <span className="lv-cal-hint">JPG, PNG or PDF · 1 to 5 files · proof of the work</span></label>
          <input type="file" accept=".jpg,.jpeg,.png,.pdf,image/jpeg,image/png,application/pdf" multiple onChange={e => pickFiles(e.target)} />
          <div>
            {files.map((a, i) => (
              <span key={i} ref={css('display:inline-flex;align-items:center;gap:6px;padding:4px 10px;margin:6px 6px 0 0;border:1px solid #e2e8f0;border-radius:999px;background:#f8fafc;font-size:12px')}>
                {`📎 ${a.file.name} `}<span ref={css('color:#94a3b8')}>{legacy('ewAttachSize', a.file.size)}</span>
                <button type="button" onClick={() => removeFile(i)} title="Remove" ref={css('border:none;background:none;color:#dc2626;cursor:pointer;font-size:13px;padding:0 2px')}>✕</button>
              </span>
            ))}
            <div ref={css('font-size:11px;color:#94a3b8;margin-top:6px')}>{`${files.length} of ${ATTACH_MAX} files`}</div>
          </div>
        </div>
      )}

      <div className="lv-approver-hint" style={{ display: 'block' }}>
        ℹ️ Approval will be sent to: <b>{approver}</b>
      </div>

      <div className="modal-footer">
        <button className="btn btn-outline" onClick={() => setOpen(false)}>Cancel</button>
        <button className="lv-save-btn" disabled={saving && isExtra} onClick={save}>{saveText}</button>
      </div>

      {combo && isExtra ? <ClientCombo combo={combo} names={names} onPick={n => { setRow(combo.key, combo.idx, 'client', n); setCombo(null); }} /> : null}
    </Modal>
  );
}
