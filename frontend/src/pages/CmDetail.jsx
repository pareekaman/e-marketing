// Client Master → the client detail page, a section at a time (it was all
// cmRenderDetailHtml in public/js/clients.js). Each section is its own root in
// a box that cmRenderDetailHtml leaves; cmShowDetail mounts it and hands it
// its data through cmDetData(name), and unmounts it before the page is drawn
// again (the date range, a save).
//   cmdetstats — the five stat cards, Recent Activity and the meetings list
//   cmdetsettings — the Departments, Kickstart, Platform, CRM, Brand Name,
//                   Billing Name and WhatsApp Group cards
//   cmdetlinks — System Links (cmSaveLinksSend)
//   cmdetdms — Documents: the client's Google Drive folders (was cmDmsLoad …
//              cmDmsUploadFile), talking to /api/clients/:id/dms itself
import { useEffect, useRef, useState } from 'react';
import { api, toast, legacy, css, fmtDate, canDo } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const n = v => parseInt(v) || 0;

function StatCard({ label, value, color, sub }) {
  return (
    <div ref={css('background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:16px 18px')}>
      <div ref={css('font-size:12px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.4px')}>{label}</div>
      <div ref={css(`font-size:28px;font-weight:700;color:${color};margin-top:6px`)}>{value}</div>
      {sub ? <div ref={css('font-size:11px;color:#94a3b8;margin-top:4px')}>{sub}</div> : null}
    </div>
  );
}

const TYPE = {
  checklist: ['font-size:10px;background:#f0fdf4;color:#16a34a;padding:2px 8px;border-radius:10px;font-weight:700', '✅ Checklist'],
  delegation: ['font-size:10px;background:#eff6ff;color:#1d4ed8;padding:2px 8px;border-radius:10px;font-weight:700', '📋 Delegation'],
};
const STATUS = { completed: ['#16a34a', 'Done'], revised: ['#9d174d', 'Revised'] };
const PILL = {
  cancelled: ['font-size:10px;color:#b91c1c;background:#fee2e2;padding:2px 8px;border-radius:10px;font-weight:700', 'CANCELLED'],
  done: ['font-size:10px;color:#16a34a;background:#dcfce7;padding:2px 8px;border-radius:10px;font-weight:700', 'DONE'],
  scheduled: ['font-size:10px;color:#1d4ed8;background:#dbeafe;padding:2px 8px;border-radius:10px;font-weight:700', 'SCHEDULED'],
};
const HEAD = 'padding:14px 16px;border-bottom:1px solid #e2e8f0';

export function CmDetStats() {
  const [s] = useState(() => legacy('cmDetData', 'cmdetstats') || {});
  const del = s.delegation || {}, chl = s.checklist || {}, meet = s.meetings || {};
  const total = n(del.total) + n(chl.total), pending = n(del.pending) + n(chl.pending);
  const done = n(del.completed) + n(chl.completed), overdue = n(del.overdue) + n(chl.overdue), revised = n(del.revised);
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  const recent = s.recent || [], meetings = meet.recent || [];
  return (
    <>
      <div className="cm-detail-stats" ref={css('display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:16px')}>
        <StatCard label="Total Tasks" value={total} color="#4f46e5" sub={`${del.total || 0} delegation · ${chl.total || 0} checklist`} />
        <StatCard label="Pending" value={pending} color="#ef4444" sub={overdue > 0 ? `${overdue} overdue` : 'On track'} />
        <StatCard label="Completed" value={done} color="#10b981" sub={`${pct}% completion rate`} />
        <StatCard label="Revised" value={revised} color="#f59e0b" sub={revised > 0 ? 'Needs rework' : 'None'} />
        <StatCard label="Meetings" value={n(meet.total)} color="#7c3aed" sub={`${n(meet.scheduled)} scheduled · ${n(meet.cancelled)} cancelled`} />
      </div>

      <div className="cm-detail-grid" ref={css('display:grid;grid-template-columns:1fr 360px;gap:16px')}>
        <div className="task-table-card" ref={css('padding:0')}>
          <div className="card-head" ref={css(HEAD)}>
            <div className="card-head-title">Recent Activity</div>
          </div>
          <div ref={css('overflow-x:auto')}>
            <table ref={css('width:100%;min-width:580px')}>
              <thead><tr><th>Type</th><th>Description</th><th>Doer</th><th>Date</th><th>Status</th></tr></thead>
              <tbody>
                {recent.length ? recent.map((t, i) => {
                  const [badge, label] = TYPE[t.type === 'checklist' ? 'checklist' : 'delegation'];
                  const [color, word] = STATUS[t.status] || ['#b91c1c', 'Pending'];
                  return (
                    <tr key={i}>
                      <td><span ref={css(badge)}>{label}</span></td>
                      <td>{t.description || ''}</td>
                      <td ref={css('white-space:nowrap')}>{t.doer || '—'}</td>
                      <td ref={css('white-space:nowrap')}>{fmtDate(t.due_date || '')}</td>
                      <td ref={css('white-space:nowrap')}><span ref={css(`color:${color};font-weight:700;font-size:12px`)}>{word}</span></td>
                    </tr>
                  );
                }) : <tr><td colSpan={5} className="empty" ref={css('padding:20px')}>No tasks in this window</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
        <div className="task-table-card" ref={css('padding:0')}>
          <div className="card-head" ref={css(HEAD)}>
            <div className="card-head-title">Meetings (this window)</div>
          </div>
          <div>
            {meetings.length ? meetings.map((m, i) => {
              const [pill, word] = PILL[m.status === 'cancelled' ? 'cancelled' : m.status === 'done' ? 'done' : 'scheduled'];
              return (
                <div key={i} ref={css('padding:10px 14px;border-bottom:1px solid #f1f5f9')}>
                  <div ref={css('font-weight:600;color:#0f172a;font-size:13px')}>{`${m.title} `}<span ref={css(pill)}>{word}</span></div>
                  <div ref={css('font-size:11px;color:#64748b;margin-top:3px')}>{`${fmtDate(m.meeting_date)} · ${m.start_time}–${m.end_time}${m.organizer_name ? ` · 👤 ${m.organizer_name}` : ''}`}</div>
                </div>
              );
            }) : <div className="empty" ref={css('padding:20px')}>No meetings in this window</div>}
          </div>
        </div>
      </div>
    </>
  );
}

// ── Settings cards ──
const CARD = 'padding:14px 18px;margin-top:16px';
const Card = ({ show, children }) => <div className="task-table-card" ref={css(`${show ? '' : 'display:none;'}${CARD}`)}>{children}</div>;
const SUB = 'font-weight:400;color:#94a3b8;font-size:12px';
const Title = ({ icon, name, sub }) => <div className="card-head-title" ref={css('margin-bottom:10px')}>{`${icon} ${name} `}<span ref={css(SUB)}>{`— ${sub}`}</span></div>;
const Save = ({ onClick, children = '💾 Save' }) => <button className="btn btn-primary" ref={css('padding:7px 16px;font-size:12px')} onClick={onClick}>{children}</button>;
const ROW = 'display:flex;gap:8px;flex-wrap:wrap;align-items:center';
const FIELD = 'flex:1;min-width:240px;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px';
const NOTE = 'font-size:11px;color:#94a3b8;margin-top:7px';
const UNSET = 'font-size:11px;color:#b45309;margin-top:7px';
const CRM_PICK = { minWidth: '220px', padding: '8px 10px', border: '1.5px solid #e2e8f0', borderRadius: '8px', fontSize: '13px', background: '#fff' };
const list = v => (v || '').split('||').filter(Boolean);
const toggle = (arr, v, on) => (on ? [...arr, v] : arr.filter(x => x !== v));

// Brand Name, Billing Name and WhatsApp Group: a text field and Save.
function TextCard({ show, icon, name, sub, id, value, placeholder, mono, note, save }) {
  const [v, setV] = useState(value || '');
  return (
    <Card show={show}>
      <div ref={css('display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:8px')}>
        <div className="card-head-title">{`${icon} ${name} `}<span ref={css(SUB)}>{`— ${sub}`}</span></div>
      </div>
      <div ref={css(ROW)}>
        <input type="text" id={id} value={v} onChange={e => setV(e.target.value)} placeholder={placeholder} ref={css(FIELD + (mono ? ';font-family:monospace' : ''))} />
        <Save onClick={() => save(v)} />
      </div>
      <div ref={css(NOTE)}>{note}</div>
    </Card>
  );
}

export function CmDetSettings() {
  const [d] = useState(() => legacy('cmDetData', 'cmdetsettings') || { client: {}, depts: [] });
  const { id, client } = d;
  const edit = !!legacy('cmCanEdit'), admin = canDo('admin_clients');
  const [depts, setDepts] = useState(() => list(client.departments));
  const [kick, setKick] = useState(client.kickstart_date || '');
  const [platOpts, setPlatOpts] = useState(null);
  const [plats, setPlats] = useState(() => list(client.platforms));
  const [crm, setCrm] = useState(null);   // { users, pick, name }
  // The platform list and the CRMs load after the page, as the old cards filled in.
  useEffect(() => {
    let live = true;
    if (edit) Promise.resolve(legacy('cmPlatformList')).then(all => {
      const known = all || [], cur = list(client.platforms);
      if (live) setPlatOpts([...known, ...cur.filter(p => !known.includes(p))]);
    });
    return () => { live = false; };
  }, []);
  const loadCrm = async () => {
    const users = (await legacy('cmCrmUsers')) || [];
    const c = legacy('cmClientById', id) || {};
    setCrm({ users, pick: c.added_by ? String(c.added_by) : '', name: c.added_by_name || '' });
  };
  useEffect(() => { if (admin) loadCrm(); }, []);
  const crmOpts = !crm ? [{ value: '', label: 'Loading…' }] : !crm.users.length ? [{ value: '', label: 'No one has CRM access yet' }]
    : [{ value: '', label: 'Choose a CRM…' }, ...crm.users.map(u => ({ value: String(u.id), label: u.name }))];
  const crmPick = crm && crmOpts.some(o => o.value === crm.pick) ? crm.pick : '';

  return (
    <>
      <Card show={edit}>
        <Title icon="🏢" name="Departments" sub="the teams this client belongs to" />
        <div id={`cmDepts_${id}`} ref={css('display:flex;flex-wrap:wrap;gap:6px 14px')}>
          {d.depts.map(dep => (
            <label key={dep} ref={css('display:flex;align-items:center;gap:6px;font-size:12px;font-weight:500;text-transform:none;letter-spacing:0;color:#334155;cursor:pointer')}>
              <input type="checkbox" value={dep} checked={depts.includes(dep)} onChange={e => setDepts(toggle(depts, dep, e.target.checked))} ref={css('width:14px;height:14px;margin:0;accent-color:#4f46e5')} />{dep}
            </label>
          ))}
        </div>
        <div ref={css('display:flex;justify-content:space-between;align-items:center;margin-top:10px;gap:8px;flex-wrap:wrap')}>
          <span ref={css(`font-size:11px;color:${client.departments ? '#94a3b8' : '#b45309'}`)}>{client.departments ? 'Handlers\' departments count too when filtering.' : 'Not set yet — tick the departments and save.'}</span>
          <Save onClick={() => legacy('cmSaveDepartments', id, depts)} />
        </div>
      </Card>

      <Card show={edit}>
        <Title icon="🚀" name="Kickstart Meeting Date" sub="shown on the client's portal" />
        <div ref={css(ROW)}>
          <input type="date" id={`cmKickstart_${id}`} value={kick} onChange={e => setKick(e.target.value)} ref={css('min-width:200px;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px')} />
          <Save onClick={() => legacy('cmSaveKickstart', id, kick)} />
        </div>
        {client.kickstart_date ? null : <div ref={css(UNSET)}>Not set yet — pick the date and save.</div>}
      </Card>

      <Card show={edit}>
        <Title icon="📣" name="Platform" sub="also updated in the Onboarding FMS sheet" />
        <div id={`cmPlatforms_${id}`} data-current={client.platforms || ''} ref={css('display:flex;flex-wrap:wrap;gap:6px 14px;margin-bottom:10px;font-size:13px')}>
          {platOpts ? platOpts.map(p => (
            <label key={p} ref={css('display:flex;align-items:center;gap:6px;cursor:pointer')}>
              <input type="checkbox" className={`cmPlatCb_${id}`} value={p} checked={plats.includes(p)} onChange={e => setPlats(toggle(plats, p, e.target.checked))} ref={css('width:14px;height:14px;accent-color:#4f46e5')} />
              <span>{p}</span>
            </label>
          )) : 'Loading…'}
        </div>
        <Save onClick={() => legacy('cmSavePlatforms', id, plats)} />
        {client.platforms ? null : <div ref={css(UNSET)}>Not set yet — tick the platforms and save.</div>}
      </Card>

      <Card show={admin}>
        <Title icon="🔀" name="CRM" sub="who added this client; it shows in their CRM list" />
        <div ref={css(ROW)}>
          <SearchSelect value={crmPick} onChange={v => setCrm(c => ({ ...c, pick: v }))} options={crmOpts} placeholder={crmOpts[0].label} style={CRM_PICK} />
          <Save onClick={async () => { if (await legacy('cmCrmTransferOne', id, crmPick)) loadCrm(); }}>🔀 Transfer</Save>
        </div>
        <div id={`cmCrmOneNote_${id}`} ref={css(NOTE)}>
          {crm ? <>{crm.name ? <>Currently: <b ref={css('color:#475569')}>{crm.name}</b></> : 'Not set. Pick a CRM and transfer.'}
            {crm.users.length ? null : ' · Tick "CRM access" in Access Control to list someone here.'}</> : null}
        </div>
      </Card>

      <TextCard show={edit} icon="🏷" name="Brand Name" sub="the name this client trades under" id={`cmBrandName_${id}`} value={client.brand_name}
        placeholder="e.g. Acme" note="Cannot be blanked out once set. Clients added before this field existed have none until you fill it in."
        save={v => legacy('cmSaveBrandName', id, v)} />
      <TextCard show={edit && !!legacy('cmCanSeeBilling')} icon="🧾" name="Billing Name" sub="the entity this client is invoiced as" id={`cmBillingName_${id}`} value={client.billing_name}
        placeholder="e.g. Acme Private Limited" note="Cannot be blanked out once set. Clients added before this field existed have none until you fill it in."
        save={v => legacy('cmSaveBillingName', id, v)} />
      <TextCard show={d.ops} icon="📱" name="WhatsApp Group" sub="where this client's pending-task digest is sent" id={`cmWaGroup_${id}`} value={client.whatsapp_group_id}
        placeholder="1203634...@g.us" mono note="Leave blank to switch the digest off for this client. Sent 9:30 AM, Mon–Fri, only when something is pending."
        save={v => legacy('cmSaveWaGroup', id, v)} />
    </>
  );
}

// ── System Links ──
const LINK_IN = 'padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:13px;outline:none;font-family:inherit';
const LINK_GRID = 'display:grid;grid-template-columns:1fr 150px 1.4fr 38px;gap:8px';
const LINK_COL = 'font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.4px';
const LINKS_CARD = 'padding:14px 18px;margin-top:16px;transition:box-shadow .3s,border-color .3s';
const SAVE_LINKS = 'padding:6px 16px;font-size:12px;transition:background .2s';
// The old card flashed green after a save by setting style properties, then
// cleared them; done the same way so the style attribute reads the same.
const styled = (base, props) => el => {
  if (!el) return;
  el.setAttribute('style', base);
  if (!props) return;
  for (const [k, v] of Object.entries(props)) { if (!v) el.style[k] = 'red'; el.style[k] = v; }
};

export function CmDetLinks() {
  const [d] = useState(() => legacy('cmDetData', 'cmdetlinks') || { links: [] });
  const [links, setLinks] = useState(() => (d.links || []).map(l => ({ ...l })));
  const [busy, setBusy] = useState(false);
  const [flash, setFlash] = useState(null);   // null: never saved; true for 1.6s after a save; then false
  const timer = useRef(0);
  useEffect(() => () => clearTimeout(timer.current), []);
  const edit = (i, key, v) => setLinks(ls => ls.map((l, j) => (j === i ? { ...l, [key]: v } : l)));
  const save = async () => {
    setBusy(true);
    const clean = await legacy('cmSaveLinksSend', d.id, links);
    setBusy(false);
    if (!clean) return;
    setLinks(clean.map(l => ({ ...l })));
    setFlash(true);
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setFlash(false), 1600);
  };
  const on = flash === true;
  return (
    <div className="task-table-card" id="cmLinksCard"
      ref={styled(`${d.ops ? '' : 'display:none;'}${LINKS_CARD}`, flash === null ? null : { boxShadow: on ? '0 0 0 2px #10b981' : '', borderColor: on ? '#10b981' : '' })}>
      <div ref={css('display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:8px')}>
        <div className="card-head-title">🔗 System Links <span ref={css(SUB)}>— shown on this client's portal</span></div>
        <button className="btn btn-outline" ref={css('padding:5px 11px;font-size:12px')} onClick={() => setLinks(ls => [...ls, { label: '', url: '', liveDate: '' }])}>＋ Add Link</button>
      </div>
      <div id="cmLinksList">
        {!links.length ? <div ref={css('color:#94a3b8;font-size:12px;padding:8px 2px')}>No links yet — click "Add Link" to add the systems this client should see (e.g. IMS, CRM).</div> : <>
          <div ref={css(`${LINK_GRID};margin-bottom:6px;padding:0 2px`)}>
            <div ref={css(LINK_COL)}>Name</div><div ref={css(LINK_COL)}>Live Date</div><div ref={css(LINK_COL)}>Open (URL)</div><div></div>
          </div>
          {links.map((l, i) => (
            <div key={i} ref={css(`${LINK_GRID};align-items:center;margin-bottom:8px`)}>
              <input type="text" value={l.label || ''} placeholder="e.g. IMS Store" onChange={e => edit(i, 'label', e.target.value)} ref={css(LINK_IN)} />
              <input type="date" value={l.liveDate || ''} onChange={e => edit(i, 'liveDate', e.target.value)} ref={css(LINK_IN)} />
              <input type="url" value={l.url || ''} placeholder="https://…" onChange={e => edit(i, 'url', e.target.value)} ref={css(LINK_IN)} />
              <button className="btn btn-outline" ref={css('padding:6px 8px;font-size:12px;color:#dc2626;border-color:#fca5a5')} title="Remove"
                onClick={() => setLinks(ls => ls.filter((_, j) => j !== i))}>🗑</button>
            </div>
          ))}
        </>}
      </div>
      <div ref={css('display:flex;justify-content:flex-end;margin-top:10px')}>
        <button className="btn btn-primary" id="cmSaveLinksBtn" disabled={busy} onClick={save}
          ref={styled(SAVE_LINKS, flash === null ? null : { background: on ? '#10b981' : '' })}>
          {busy ? 'Saving…' : on ? '✓ Saved!' : '💾 Save Links'}</button>
      </div>
    </div>
  );
}

// ── Documents (Google Drive) ──
// The department last looked at, kept from one client to the next as the old
// DMS_STATE did: opening another client with the same folder shows it first.
let lastDmsDept = null;
const DMS_NOTE = 'color:#94a3b8;font-size:13px;padding:12px 0';
const mimeIcon = m => (m.includes('spreadsheet') ? '📊' : m.includes('document') ? '📄' : m.includes('presentation') ? '📽️' : m.includes('folder') ? '📁' : '📎');
const DMS_BTN = (bg, color) => `padding:5px 12px;border:none;border-radius:6px;font-size:12px;background:${bg};color:${color};font-weight:600;cursor:pointer`;

export function CmDetDms() {
  const [d] = useState(() => legacy('cmDetData', 'cmdetdms') || {});
  const cid = d.id;
  const [dms, setDms] = useState(null);       // { error } or the /dms answer
  const [active, setActive] = useState(null);
  const [files, setFiles] = useState(null);   // null while loading
  const [settingUp, setSettingUp] = useState(false);
  const live = useRef(true);
  useEffect(() => () => { live.current = false; }, []);

  const loadFiles = async folderId => {
    setFiles(null);
    const r = await api(`/api/clients/${cid}/dms/folders/${folderId}/files`);
    if (live.current) setFiles(Array.isArray(r) ? r : []);
  };
  const load = async () => {
    const r = await api(`/api/clients/${cid}/dms`);
    if (!live.current) return;
    if (r.error) { setDms({ error: r.error }); return; }
    const depts = r.departments || [];
    const keep = depts.find(x => x.department_name === lastDmsDept);
    const pick = keep ? keep.department_name : (depts.length ? depts[0].department_name : null);
    lastDmsDept = pick;
    setDms({ ...r, departments: depts });
    setActive(pick);
    const dept = depts.find(x => x.department_name === pick);
    if (dept) loadFiles(dept.drive_folder_id); else setFiles([]);
  };
  useEffect(() => { load(); }, []);

  const select = name => {
    lastDmsDept = name;
    setActive(name);
    const dept = dms.departments.find(x => x.department_name === name);
    if (dept) loadFiles(dept.drive_folder_id);
  };
  const remove = async name => {
    if (!await legacy('appConfirm', `Remove "${name}" folder link? (The Google Drive folder itself is NOT deleted.)`, 'Remove Folder Link?')) return;
    const r = await api(`/api/clients/${cid}/dms/departments/${encodeURIComponent(name)}`, 'DELETE');
    if (r.error) { toast(r.error, 'error'); return; }
    load();
  };
  const addDept = async () => {
    const name = (window.prompt('Department name to add (e.g. Accounts, Audit, Compliance):') || '').trim();
    if (!name) return;
    const r = await api(`/api/clients/${cid}/dms/departments`, 'POST', { department_name: name });
    if (r.error) { toast(r.error, 'error'); return; }
    lastDmsDept = name;   // select the new one after the reload
    load();
  };
  const setup = async () => {
    setSettingUp(true);
    const r = await api(`/api/clients/${cid}/dms/setup`, 'POST');
    if (r.error) { toast(r.error, 'error'); setSettingUp(false); return; }
    load();
  };
  const createFile = async (kind, folderId) => {
    const name = (window.prompt(`Name for the new ${kind === 'sheet' ? 'Google Sheet' : 'Google Doc'}:`) || '').trim();
    if (!name) return;
    const r = await api(`/api/clients/${cid}/dms/folders/${folderId}/files`, 'POST', { name, kind });
    if (r.error) { toast(r.error, 'error'); return; }
    if (r.web_view_link) window.open(r.web_view_link, '_blank');
    loadFiles(folderId);
  };
  const upload = folderId => {
    const input = document.createElement('input');
    input.type = 'file';
    input.onchange = async () => {
      const file = input.files && input.files[0];
      if (!file) return;
      const fd = new FormData();
      fd.append('file', file);
      toast(`Uploading "${file.name}"…`);
      try {
        const res = await fetch(`/api/clients/${cid}/dms/folders/${folderId}/upload`, { method: 'POST', body: fd });
        const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
        toast('File uploaded!');
        loadFiles(folderId);
      } catch (e) { toast('Upload failed: ' + e.message, 'error'); }
    };
    input.click();
  };

  let body;
  if (!dms) body = <div ref={css('color:#94a3b8;font-size:12px;padding:8px 0')}>Loading…</div>;
  else if (dms.error) body = <div ref={css('color:#dc2626;font-size:12px;padding:8px 0')}>{`Failed to load DMS: ${dms.error}`}</div>;
  else if (!dms.drive_configured) body = (
    <div ref={css('background:#fef3c7;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;font-size:13px;color:#92400e')}>
      Share the DMS root Drive folder with the service account's email (Editor access), then set <code>GOOGLE_DRIVE_ROOT_FOLDER_ID</code> in environment variables.
    </div>);
  else if (!dms.drive_folder_id) body = (
    <div ref={css('text-align:center;padding:24px')}>
      <div ref={css('color:#64748b;font-size:13px;margin-bottom:12px')}>No Drive folder set up for this client yet.</div>
      <button className="btn btn-primary" ref={css('padding:8px 18px;font-size:13px')} disabled={settingUp} onClick={setup}>{settingUp ? 'Creating…' : '☁️ Create Drive Folder'}</button>
    </div>);
  else {
    const dept = dms.departments.find(x => x.department_name === active);
    let filesBody;
    if (!active) filesBody = <div ref={css(DMS_NOTE)}>Add a department folder above to get started.</div>;
    else if (!dept) filesBody = <div ref={css(DMS_NOTE)}>Department not found.</div>;
    else if (files === null) filesBody = <div ref={css(DMS_NOTE)}>Loading…</div>;
    else {
      const fid = dept.drive_folder_id;
      filesBody = <>
        <div ref={css('display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:8px')}>
          <div ref={css('font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.4px')}>{active}</div>
          <div ref={css('display:flex;gap:6px')}>
            <button ref={css(DMS_BTN('#eff6ff', '#1d4ed8'))} onClick={() => createFile('doc', fid)}>＋ Doc</button>
            <button ref={css(DMS_BTN('#f0fdf4', '#16a34a'))} onClick={() => createFile('sheet', fid)}>＋ Sheet</button>
            <button ref={css(DMS_BTN('#fff7ed', '#c2410c'))} onClick={() => upload(fid)}>⬆ Upload</button>
            <button title="Refresh" ref={css('padding:5px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;color:#64748b;background:#fff;cursor:pointer')} onClick={() => loadFiles(fid)}>↺</button>
          </div>
        </div>
        {files.length ? files.map((f, i) => (
          <div key={f.id || i} ref={css('display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:8px;background:#f8fafc;margin-bottom:6px')}
            onMouseOver={e => { e.currentTarget.style.background = '#f1f5f9'; }} onMouseOut={e => { e.currentTarget.style.background = '#f8fafc'; }}>
            <span ref={css('font-size:18px')}>{mimeIcon(f.mimeType || '')}</span>
            <a href={/^https?:\/\//i.test(f.webViewLink) ? f.webViewLink : '#'} target="_blank" rel="noopener"
              ref={css('font-size:13px;color:#1d4ed8;font-weight:600;text-decoration:none;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap')}>{f.name}</a>
            <span ref={css('font-size:11px;color:#94a3b8;white-space:nowrap;flex-shrink:0')}>{f.modifiedTime ? new Date(f.modifiedTime).toLocaleDateString() : ''}</span>
          </div>
        )) : <div ref={css(DMS_NOTE)}>No files yet — create one with the buttons above.</div>}
      </>;
    }
    body = <>
      <div ref={css('display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px')}>
        {dms.departments.map(x => {
          const on = x.department_name === active;
          return (
            <span key={x.department_name} ref={css('display:inline-flex;align-items:center')}>
              <button onClick={() => select(x.department_name)}
                ref={css(`padding:6px 14px;border:none;border-right:none;border-radius:6px 0 0 6px;font-size:12px;font-weight:600;cursor:pointer;${on ? 'background:#7c3aed;color:#fff' : 'background:#f1f5f9;color:#475569'}`)}>{x.department_name}</button>
              <button onClick={() => remove(x.department_name)} title="Remove"
                ref={css(`padding:6px 7px;border:none;border-radius:0 6px 6px 0;font-size:11px;cursor:pointer;${on ? 'background:#6d28d9;color:#ddd' : 'background:#e2e8f0;color:#94a3b8'}`)}>✕</button>
            </span>
          );
        })}
        <button onClick={addDept} ref={css('padding:6px 12px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;color:#7c3aed;font-weight:600;background:#fff;cursor:pointer')}>＋ Department</button>
      </div>
      <div id="cmDmsFiles">{filesBody}</div>
    </>;
  }

  return (
    <div className="task-table-card" ref={css(`${d.ops ? '' : 'display:none;'}padding:14px 18px;margin-top:16px`)}>
      <div ref={css('display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:8px')}>
        <div className="card-head-title">📁 Documents <span ref={css(SUB)}>— Google Drive</span></div>
      </div>
      <div id="cmDmsContent">{body}</div>
    </div>
  );
}
