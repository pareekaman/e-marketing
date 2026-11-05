// FMS Admin → the step editor: "Configure Steps" for a new FMS and "✏️ Edit FMS"
// (were #fmsStepsModal / #fmsEditModal, built by buildStepBoxHTML and its
// helpers in public/js/fms.js). fms.js fetches the users, the FMS and the
// sheet's headers (with its toasts) and hands them over through fmsStepsSetReact:
//   { open: 'add' } or { open: 'edit', basics } — the pop-up opens, "Loading headers..."
//   { ready: true, steps, headers, users } — the boxes
// The editing is all here; saving goes through fmsSaveNewSend / fmsSaveEditSend,
// which build exactly the body the old pop-ups sent.
//   Ticking a column under "Columns to Show" on Step 1 applies it to every step.
//   Picking an extra row's column fills its label from the header.
//   Load Doers fills a step's doers from the names in its Doer Name Column.
import { Fragment, useEffect, useRef, useState } from 'react';
import { legacy, css, api } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const FONT = "'Inter',sans-serif";
const INPUT = `width:100%;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:${FONT};outline:none`;
const SMALL = `width:100%;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;font-family:${FONT};outline:none`;
const HINT = 'color:#94a3b8;font-weight:400;font-size:11px';
const CAP = 'font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px';
const SEL_EXTRA = { padding: '7px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13, fontFamily: FONT, outline: 'none' };
const SEL_ROW = { width: '100%', padding: '7px 10px', border: '1.5px solid #e2e8f0', borderRadius: 7, fontSize: 12, fontFamily: FONT, outline: 'none', background: '#fff' };
const SEL_TOKEN = { padding: '5px 8px', border: '1.5px solid #e2e8f0', borderRadius: 6, fontSize: 11, fontFamily: FONT, background: '#fff', outline: 'none' };
const FIELD_TYPES = [
  { value: 'text', label: '📝 Text' }, { value: 'number', label: '🔢 Number' }, { value: 'date', label: '📅 Date' },
  { value: 'link', label: '🔗 Link' }, { value: 'dropdown', label: '🔽 Dropdown' },
];
// A style set as the old page left it once shown from script.
const displayed = (base, value) => el => { if (!el) return; el.setAttribute('style', base); if (value !== undefined) el.style.display = value; };
const blankStep = n => ({ stepName: `Step ${n}`, doers: [], planCol: '', actualCol: '', extraInput: 'no', extraCol: '', extraRows: [], showCols: [], delayReasonCol: '', doerNameCol: '' });
const clone = x => JSON.parse(JSON.stringify(x));

function headerOpts(headers, blank) {
  return [{ value: '', label: blank }, ...headers.map(h => ({ value: h.col, label: `${h.name} (COL ${h.col})` }))];
}

// A saved column the sheet no longer has shows as the blank choice, as a <select>
// with no matching option did; the step keeps it until another is picked.
const known = (headers, v) => (headers.some(h => h.col === v) ? v : '');

// A column field: a searchable list of the sheet's headers, or a plain letter box when none loaded.
function ColField({ headers, value, onChange, blank, placeholder, flex }) {
  return headers.length
    ? <SearchSelect className="header-select" value={known(headers, value)} onChange={onChange} options={headerOpts(headers, blank)} placeholder={blank} style={flex ? { flex: 1 } : undefined} />
    : <input type="text" value={value} placeholder={placeholder} onChange={e => onChange(e.target.value)}
        ref={css(flex ? `flex:1;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:${FONT};outline:none` : INPUT)} />;
}

function Doers({ idx, doers, users, onToggle }) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState('');
  const wrap = useRef(null);
  const search = useRef(null);
  useEffect(() => {
    if (!open) return;
    const away = e => { if (wrap.current && !wrap.current.contains(e.target)) setOpen(false); };
    document.addEventListener('click', away);
    return () => document.removeEventListener('click', away);
  }, [open]);
  // Opening starts from a clean list, with the cursor already in the search box.
  const toggle = () => { if (open) { setOpen(false); return; } setQ(''); setOpen(true); setTimeout(() => search.current && search.current.focus(), 0); };
  const ql = q.trim().toLowerCase();
  const shown = users.filter(u => !ql || String(u.name).toLowerCase().includes(ql)).length;
  const ids = (doers || []).map(d => parseInt(d));
  const names = (doers || []).map(uid => { const u = users.find(x => parseInt(x.id) === parseInt(uid)); return u ? u.name : ''; }).filter(Boolean);
  return (
    <div className="multi-select-wrap" id={`fmsDoerWrap_${idx}`} ref={wrap}>
      <div className="selected-tags" id={`fmsDoerTags_${idx}`} onClick={toggle}>
        {!(doers || []).length
          ? <span ref={css('color:#94a3b8;font-size:12px')}>Select users...</span>
          : names.map((n, i) => <span key={i} className="tag-badge">{n}</span>)}
      </div>
      <div className={'multi-select-dropdown' + (open ? ' open' : '')} id={`fmsDoerDrop_${idx}`}>
        <div className="multi-select-search">
          <input type="text" id={`fmsDoerSearch_${idx}`} placeholder="Search users..." ref={search} value={q}
            onClick={e => e.stopPropagation()} onChange={e => setQ(e.target.value)} />
        </div>
        <div id={`fmsDoerList_${idx}`}>
          {users.map(u => {
            const hit = !ql || String(u.name).toLowerCase().includes(ql);
            return (
              <div key={u.id} className="multi-select-item" data-uid={u.id} ref={el => { if (el) el.style.display = hit ? '' : 'none'; }}
                onClick={e => { e.stopPropagation(); onToggle(parseInt(u.id)); }}>
                <input type="checkbox" checked={ids.includes(parseInt(u.id))} onChange={() => {}} /> {u.name}
              </div>
            );
          })}
        </div>
        <div className="multi-select-empty" id={`fmsDoerNone_${idx}`} ref={el => { if (el) { el.setAttribute('style', 'display:none'); el.style.display = shown ? 'none' : ''; } }}>No users match</div>
      </div>
    </div>
  );
}

function LoadDoersResult({ res }) {
  if (!res) return null;
  if (res.loading) return <i ref={css('color:#64748b')}>Loading...</i>;
  if (res.warn) return <span ref={css('color:#dc2626')}>{res.warn}</span>;
  if (res.error) return <span ref={css('color:#dc2626')}>{`❌ ${res.error}`}</span>;
  const r = res.r;
  return (
    <div ref={css('background:#f0fdf4;border:1px solid #86efac;color:#166534;padding:8px 12px;border-radius:6px;line-height:1.5')}>
      <b>{`✅ Loaded ${r.total_unique} unique name${r.total_unique === 1 ? '' : 's'} from Col ${res.col}`}</b><br />
      {'Matched & auto-selected: '}<b>{r.matched_count}</b>
      {r.matched_count ? <>{' '}<span ref={css('color:#475569')}>{`(${r.matched.map(m => m.user_name).join(', ')})`}</span></> : null}
      {r.unmatched_count
        ? <>
            <br /><span ref={css('color:#b45309')}>{`⚠️ Not in users DB (${r.unmatched_count}): ${r.unmatched.join(', ')}`}</span>
            <br /><span ref={css('color:#64748b;font-size:11px')}>→ Add these names in the Users tab, then click Load Doers again.</span>
          </>
        : null}
    </div>
  );
}

function ExtraRow({ idx, ri, r, headers, depts, set, remove }) {
  const isRequired = !(r.required === 0 || r.required === false || r.required === '0');
  // Appends a live-user token (@users / @dept:X); the picker goes back to its prompt.
  const addToken = token => {
    if (!token) return;
    const parts = String(r.dropdown_options || '').split(',').map(o => o.trim()).filter(Boolean);
    if (parts.some(p => p.toLowerCase() === token.toLowerCase())) return;
    parts.push(token);
    set({ dropdown_options: parts.join(',') });
  };
  return (
    <div className="extra-row-item" id={`fmsExtraRow_${idx}_${ri}`} ref={css('background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:10px;margin-bottom:10px')}>
      <div ref={css('display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:6px')}>
        <div>
          <div ref={css(CAP)}>Column</div>
          {headers.length
            ? <SearchSelect value={known(headers, r.col_letter || '')} options={headerOpts(headers, '-- Select Column --')} placeholder="-- Select Column --" style={SEL_ROW}
                onChange={v => { const h = headers.find(x => x.col === v); set({ col_letter: v, label: (h && h.name) || v }); }} />
            : <input type="text" value={r.col_letter || ''} placeholder="Col e.g. AS" onChange={e => set({ col_letter: e.target.value })} ref={css(SMALL)} />}
        </div>
        <div>
          <div ref={css(CAP)}>Label</div>
          <input type="text" value={r.label || ''} placeholder="Label (auto-filled from header)" id={`fmsExtraLabel_${idx}_${ri}`} onChange={e => set({ label: e.target.value })} ref={css(SMALL)} />
        </div>
        <div>
          <div ref={css(CAP)}>Field Type</div>
          <SearchSelect value={r.field_type || 'text'} options={FIELD_TYPES} style={SEL_ROW} onChange={v => set({ field_type: v })} />
        </div>
        <div>
          <div ref={css(CAP)}>Required?</div>
          <label ref={css(`display:flex;align-items:center;gap:6px;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;font-family:${FONT};background:#fff;cursor:pointer;height:34px;box-sizing:border-box`)}>
            <input type="checkbox" checked={isRequired} onChange={e => set({ required: e.target.checked ? 1 : 0 })} ref={css('accent-color:#4f46e5;width:14px;height:14px;cursor:pointer')} />
            <span ref={css('font-weight:600;color:#374151')}>{isRequired ? 'Required' : 'Optional'}</span>
          </label>
        </div>
      </div>
      <div ref={css('display:flex;justify-content:flex-end;margin-bottom:6px')}>
        <button className="action-btn delete" ref={css('padding:4px 12px')} onClick={remove}>✕ Remove Row</button>
      </div>
      <div id={`fmsDropOptSection_${idx}_${ri}`}>
        {r.field_type === 'dropdown'
          ? <div ref={css('margin-top:4px')}>
              <label ref={css('font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.3px')}>Dropdown Options <span ref={css('color:#94a3b8;font-weight:400')}>(comma separated, e.g. Yes,No,N/A)</span></label>
              <input type="text" value={r.dropdown_options || ''} placeholder="Yes,No,N/A or Option1,Option2,Option3" id={`fmsDropOpt_${idx}_${ri}`}
                onChange={e => set({ dropdown_options: e.target.value })} ref={css(SMALL + ';margin-top:4px')} />
              <div ref={css('display:flex;align-items:center;gap:6px;margin-top:5px')}>
                <SearchSelect value="" onChange={addToken} style={SEL_TOKEN} placeholder="+ Add a live user list…"
                  options={[{ value: '', label: '+ Add a live user list…' }, { value: '@users', label: 'All users' }, ...depts.map(d => ({ value: `@dept:${d}`, label: `${d} department` }))]} />
                <span ref={css('font-size:10px;color:#94a3b8')}>Live lists stay in sync when staff join or leave.</span>
              </div>
            </div>
          : null}
      </div>
    </div>
  );
}

function StepBox({ idx, s, headers, users, depts, kind, basics, mode, checked, onCheck, update, setAll, drag, boxRef }) {
  const [res, setRes] = useState(null);   // Load Doers' result line
  const showCols = s.showCols || [];
  const set = patch => update(st => ({ ...st, ...patch }));
  const setRow = (ri, patch) => update(st => ({ ...st, extraRows: st.extraRows.map((r, j) => (j === ri ? { ...r, ...patch } : r)) }));
  const toggleDoer = uid => update(st => {
    const d = [...(st.doers || [])];
    const i = d.indexOf(uid);
    if (i === -1) d.push(uid); else d.splice(i, 1);
    return { ...st, doers: d };
  });
  // A column ticked on Step 1 goes on every step; later steps change only themselves.
  const showCol = (ci, on) => {
    const apply = st => {
      const cols = st.showCols ? [...st.showCols] : [];
      return { ...st, showCols: on ? (cols.includes(ci) ? cols : [...cols, ci]) : cols.filter(x => x !== ci) };
    };
    if (idx === 0) setAll(all => all.map(apply)); else update(apply);
  };
  const loadDoers = async () => {
    setRes({ loading: true });
    const col = String(s.doerNameCol || s.doer_name_col || '').trim().toUpperCase();
    if (!col) { setRes({ warn: '⚠️ Please select the "Doer Name Column" first, then click this button.' }); return; }
    const src = kind === 'edit'
      ? { sheetId: String(basics.sheetId || '').trim(), tabName: String(basics.sheetName || '').trim(), headerRow: parseInt(basics.headerRow) || 1 }
      : (d => ({ sheetId: String(d.sheetId || '').trim(), tabName: String(d.sheetName || '').trim(), headerRow: parseInt(d.headerRow) || 1 }))(legacy('fmsDataGet') || {});
    if (!src.sheetId) { setRes({ warn: '⚠️ Please enter the Sheet ID above first.' }); return; }
    try {
      const params = new URLSearchParams({ sheetId: src.sheetId, tabName: src.tabName, col, headerRow: src.headerRow });
      const r = await api('/api/fms/sheet-column-values?' + params.toString());
      if (r.error) throw new Error(r.error);
      set({ doers: r.matched.map(m => m.user_id) });
      setRes({ r, col });
    } catch (e) { setRes({ error: e.message }); }
  };
  return (
    <div className={'fms-step-box' + (drag.over === idx ? ' drag-over' : '')} data-idx={idx} draggable="true" ref={boxRef}
      onDragStart={e => { drag.start(idx); e.dataTransfer.effectAllowed = 'move'; }}
      onDragOver={e => { e.preventDefault(); drag.setOver(idx); }}
      onDragLeave={() => drag.setOver(o => (o === idx ? null : o))}
      onDrop={e => { e.preventDefault(); drag.drop(idx); }}>
      <div ref={css('display:flex;align-items:center;gap:8px;margin-bottom:10px')}>
        <span className="drag-handle" title="Drag to reorder">⠿</span>
        <div className="fms-step-num">{`Step ${idx + 1}`}</div>
        {mode.del ? <input type="checkbox" className="fms-del-check" ref={css('margin-left:auto')} data-idx={idx} checked={checked.del.has(idx)} onChange={e => onCheck('del', idx, e.target.checked)} /> : null}
        {mode.dup ? <input type="checkbox" className="fms-dup-check" ref={css('margin-left:auto;accent-color:#7c3aed')} data-idx={idx} checked={checked.dup.has(idx)} onChange={e => onCheck('dup', idx, e.target.checked)} /> : null}
      </div>
      <div ref={css('display:grid;grid-template-columns:1fr 1fr;gap:12px')}>
        <div className="form-group" ref={css('margin:0')}>
          <label>Step Name</label>
          <input type="text" value={s.stepName || ''} placeholder="Step Name" onChange={e => set({ stepName: e.target.value })} ref={css(INPUT)} />
        </div>
        <div className="form-group" ref={css('margin:0')}>
          <label>Step Doer(s)</label>
          <Doers idx={idx} doers={s.doers} users={users} onToggle={toggleDoer} />
        </div>
        <div className="form-group" ref={css('margin:0')}>
          <label>Plan <span ref={css(HINT)}>{`(Plan ${idx + 1})`}</span></label>
          <ColField headers={headers} value={s.planCol || ''} onChange={v => set({ planCol: v })} blank="-- Select Column --" placeholder="Column e.g. I" />
        </div>
        <div className="form-group" ref={css('margin:0')}>
          <label>Actual <span ref={css(HINT)}>{`(Actual ${idx + 1})`}</span></label>
          <ColField headers={headers} value={s.actualCol || ''} onChange={v => set({ actualCol: v })} blank="-- Select Column --" placeholder="Column e.g. J" />
        </div>
      </div>

      <div className="form-group" ref={css('margin:10px 0 0')}>
        <label>Columns to Show in FMS Tasks <span ref={css(HINT)}>(blank = show all)</span></label>
        {headers.length
          ? <div ref={css('display:flex;flex-wrap:wrap;gap:6px;padding:8px;border:1.5px solid #e2e8f0;border-radius:8px;background:#f8fafc;max-height:160px;overflow-y:auto')}>
              {headers.map(h => (
                <label key={h.index} ref={css('display:flex;align-items:center;gap:4px;font-size:11px;font-weight:500;cursor:pointer;text-transform:none;letter-spacing:0;background:#fff;border:1px solid #e2e8f0;border-radius:6px;padding:3px 8px;white-space:nowrap')}>
                  <input type="checkbox" data-showcol={h.index} checked={showCols.includes(h.index)} onChange={e => showCol(h.index, e.target.checked)} ref={css('accent-color:#4f46e5;width:12px;height:12px')} />
                  {' '}{h.name}
                </label>
              ))}
            </div>
          : <span ref={css('color:#94a3b8;font-size:12px')}>Will show once headers are loaded</span>}
      </div>

      <div className="form-group" ref={css('margin:10px 0 0')}>
        <label>Delay Reason Column <span ref={css(HINT)}>(column where the delay reason is saved)</span></label>
        <ColField headers={headers} value={s.delayReasonCol || ''} onChange={v => set({ delayReasonCol: v })} blank="-- None (don't save delay reason) --" placeholder="e.g. K" />
      </div>

      <div className="form-group" ref={css('margin:10px 0 0')}>
        <label>Doer Name Column <span ref={css(HINT)}>(column where the doer's name is auto-saved on completion)</span></label>
        <div ref={css('display:flex;gap:8px;align-items:stretch')}>
          <ColField headers={headers} value={s.doerNameCol || s.doer_name_col || ''} onChange={v => set({ doerNameCol: v })} blank="-- None (don't save doer name) --" placeholder="e.g. L" flex />
          <button className="btn btn-sm" type="button" onClick={loadDoers} title="Auto-fill Step Doers from this column's unique values"
            ref={css('background:#10b981;color:#fff;border:none;padding:0 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap')}>
            🔄 Load Doers
          </button>
        </div>
        <div id={`fmsLoadDoersResult_${idx}`} ref={displayed('margin-top:8px;font-size:12px;display:none', res ? 'block' : undefined)}><LoadDoersResult res={res} /></div>
      </div>

      <div className="form-group" ref={css('margin:10px 0 0')}>
        <label>Extra Input</label>
        <SearchSelect value={s.extraInput || 'no'} onChange={v => set({ extraInput: v })} style={SEL_EXTRA} options={[{ value: 'no', label: 'No' }, { value: 'yes', label: 'Yes' }]} />
      </div>
      <div id={`fmsExtraSection_${idx}`} ref={css(`display:${s.extraInput === 'yes' ? 'block' : 'none'};margin-top:10px;background:#f0f4ff;border-radius:8px;padding:12px`)}>
        <div id={`fmsExtraRows_${idx}`}>
          {(s.extraRows || []).map((r, ri) => (
            <ExtraRow key={ri} idx={idx} ri={ri} r={r} headers={headers} depts={depts}
              set={patch => setRow(ri, patch)} remove={() => { setRes(null); update(st => ({ ...st, extraRows: st.extraRows.filter((_, j) => j !== ri) })); }} />
          ))}
        </div>
        {/* Adding or removing a row redrew the whole box before, clearing the Load Doers line; so does this. */}
        <button className="btn btn-outline btn-sm" ref={css('margin-top:8px')}
          onClick={() => { setRes(null); update(st => ({ ...st, extraRows: [...(st.extraRows || []), { col_letter: '', field_type: 'text', label: '', dropdown_options: '', required: 1 }] })); }}>+ Add Row</button>
      </div>
    </div>
  );
}

function Editor({ d, close }) {
  const edit = d.kind === 'edit';
  const [basics, setBasics] = useState(d.basics || {});
  const [steps, setSteps] = useState(null);           // null while the headers load
  const [headers, setHeaders] = useState([]);
  const [users, setUsers] = useState([]);
  const [mode, setMode] = useState({ del: false, dup: false });
  const [checked, setChecked] = useState({ del: new Set(), dup: new Set() });
  const [gen, setGen] = useState(0);                  // boxes start over after delete / duplicate / reorder
  const [err, setErr] = useState('');
  const [over, setOver] = useState(null);
  const dragSrc = useRef(null);
  const boxes = useRef([]);

  useEffect(() => {
    d.listen(x => { setSteps(x.steps); setHeaders(x.headers || []); setUsers(x.users || []); });
  }, []);

  const depts = [...new Set(users.map(u => (u.department || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b));
  const restart = () => { setChecked({ del: new Set(), dup: new Set() }); setGen(g => g + 1); };
  const toggleMode = k => { setMode(m => ({ ...m, [k]: !m[k] })); restart(); };
  const onCheck = (k, i, on) => setChecked(c => { const n = new Set(c[k]); if (on) n.add(i); else n.delete(i); return { ...c, [k]: n }; });
  const confirmDelete = () => {
    const list = [...checked.del];
    if (!list.length) { legacy('showToast', 'No steps selected', 'error'); return; }
    setSteps(st => st.filter((_, i) => !checked.del.has(i)));
    setMode(m => ({ ...m, del: false })); restart();
  };
  const confirmDup = () => {
    const list = [...checked.dup].sort((a, b) => a - b);
    if (!list.length) { legacy('showToast', 'No steps selected', 'error'); return; }
    // Copied in page order, onto the end.
    setSteps(st => [...st, ...list.map(i => ({ ...clone(st[i]), stepName: st[i].stepName + ' (Copy)' }))]);
    setMode(m => ({ ...m, dup: false })); restart();
    legacy('showToast', `✅ ${list.length} step(s) duplicated!`);
  };
  const drag = {
    over,
    setOver,
    start: i => { dragSrc.current = i; },
    drop: dest => {
      setOver(null);
      const src = dragSrc.current;
      if (src === null || src === dest) return;
      setSteps(st => { const next = [...st]; const moved = next.splice(src, 1)[0]; next.splice(dest, 0, moved); return next; });
      dragSrc.current = null;
      restart();
    },
  };
  const addStep = () => setSteps(st => [...st, blankStep(st.length + 1)]);
  const save = async () => {
    setErr('');
    if (edit) {
      const msg = await legacy('fmsSaveEditSend', basics, clone(steps), close);
      if (msg) setErr(msg);
    } else {
      await legacy('fmsSaveNewSend', clone(steps), close);
    }
  };
  const setB = k => e => setBasics(b => ({ ...b, [k]: e.target.value }));

  const list = steps === null
    ? <div ref={css('text-align:center;padding:20px;color:#64748b')}>⏳ Loading headers...</div>
    : steps.map((s, i) => (
        <StepBox key={`${gen}-${i}`} idx={i} s={s} headers={headers} users={users} depts={depts} kind={d.kind} basics={basics}
          mode={mode} checked={checked} onCheck={onCheck} drag={drag} boxRef={el => { boxes.current[i] = el; }}
          update={fn => setSteps(st => st.map((x, j) => (j === i ? fn(x) : x)))} setAll={setSteps} />
      ));
  const delBtns = <>
    <button className="btn btn-outline btn-sm" ref={css('color:#dc2626;border-color:#fecaca')} onClick={() => toggleMode('del')}>{mode.del ? '✕ Cancel' : '🗑 Select to Delete'}</button>
    <button className="btn btn-danger btn-sm" ref={displayed('display:none', mode.del ? 'inline-block' : 'none')} onClick={confirmDelete}>Delete Selected</button>
  </>;

  if (!edit) {
    return (
      <>
        <div ref={css('display:flex;justify-content:space-between;align-items:center;margin-bottom:18px')}>
          <h3 ref={css('margin:0')}>Configure Steps</h3>
          <button className="btn btn-outline btn-sm" onClick={addStep} disabled={steps === null}>+ Add More</button>
        </div>
        <div ref={css('display:flex;align-items:center;gap:10px;margin-bottom:14px')}>{delBtns}</div>
        <div ref={css('display:flex;flex-direction:column;gap:16px')}>{list}</div>
        <div className="modal-footer" ref={css('justify-content:space-between;margin-top:20px')}>
          <button className="btn btn-outline" onClick={close}>Cancel</button>
          <button className="btn btn-primary" onClick={save} disabled={steps === null}>💾 Save FMS</button>
        </div>
      </>
    );
  }
  return (
    <>
      <div ref={css('display:flex;justify-content:space-between;align-items:center;margin-bottom:18px')}>
        <h3 ref={css('margin:0')}>✏️ Edit FMS</h3>
        <div ref={css('display:flex;gap:6px')}>
          <button className="btn btn-outline btn-sm" onClick={addStep} disabled={steps === null}>+ Add Step</button>
          <button className="btn btn-outline btn-sm" ref={css('color:#7c3aed;border-color:#c4b5fd')} onClick={() => toggleMode('dup')}>{mode.dup ? '✕ Cancel' : '📋 Duplicate'}</button>
          <button className="btn btn-sm" ref={displayed('display:none;background:#7c3aed;color:#fff', mode.dup ? 'inline-block' : 'none')} onClick={confirmDup}>Duplicate Selected</button>
        </div>
      </div>
      <div ref={css('background:#f8fafc;border-radius:10px;padding:14px;margin-bottom:16px')}>
        <div ref={css('display:grid;grid-template-columns:1fr 1fr;gap:12px')}>
          <div className="form-group" ref={css('margin:0')}>
            <label>FMS Name <span ref={css(HINT)}>(display name)</span></label>
            <input type="text" placeholder="e.g. Production Flow" value={basics.fmsName ?? ''} onChange={setB('fmsName')} ref={css(INPUT)} />
          </div>
          <div className="form-group" ref={css('margin:0')}>
            <label>Google Sheet Tab Name</label>
            <input type="text" value={basics.sheetName ?? ''} onChange={setB('sheetName')} ref={css(INPUT)} />
          </div>
          <div className="form-group" ref={css('margin:0')}>
            <label>Sheet ID</label>
            <input type="text" value={basics.sheetId ?? ''} onChange={setB('sheetId')} ref={css(INPUT)} />
          </div>
          <div className="form-group" ref={css('margin:0')}>
            <label>Header Row</label>
            <input type="number" min="1" value={basics.headerRow ?? ''} onChange={setB('headerRow')} ref={css(INPUT)} />
          </div>
        </div>
      </div>
      <div ref={css('display:flex;align-items:center;gap:8px;margin-bottom:14px;flex-wrap:wrap')}>
        <div ref={css('display:flex;gap:6px;flex-wrap:wrap;flex:1')}>
          {(steps || []).map((s, i) => (
            <div key={i} className="fms-step-tab active" ref={css('font-size:11px;padding:4px 10px')}
              onClick={() => boxes.current[i] && boxes.current[i].scrollIntoView({ behavior: 'smooth', block: 'center' })}>{s.stepName || 'Step ' + (i + 1)}</div>
          ))}
        </div>
        <div ref={css('display:flex;gap:6px')}>{delBtns}</div>
      </div>
      <div ref={css('display:flex;flex-direction:column;gap:16px')}>{list}</div>
      <div className="alert error" ref={displayed('margin-top:12px', err ? 'block' : 'none')}>{err}</div>
      <div className="modal-footer" ref={css('justify-content:space-between;margin-top:20px')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save} disabled={steps === null}>💾 Save Changes</button>
      </div>
    </>
  );
}

export default function FmsSteps() {
  const [d, setD] = useState(null);
  const [open, setOpen] = useState(false);
  const ready = useRef(null);   // the open editor's listener for { ready } (headers can arrive before it mounts)
  const pending = useRef(null);
  useEffect(() => {
    let n = 0;
    legacy('fmsStepsSetReact', x => {
      if (x.open) {
        pending.current = null; ready.current = null;
        const key = ++n;
        setD({ kind: x.open, basics: x.basics, key, listen: fn => { ready.current = fn; if (pending.current) { fn(pending.current); pending.current = null; } } });
        setOpen(true);
      } else if (x.ready) {
        if (ready.current) ready.current(x); else pending.current = x;
      }
    });
    return () => legacy('fmsStepsSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="width:780px;max-width:96vw;max-height:90vh;overflow-y:auto">
      {d ? <Editor key={d.key} d={d} close={close} /> : null}
    </Modal>
  );
}
