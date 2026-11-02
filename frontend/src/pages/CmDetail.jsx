// Client Master → the client detail page, a section at a time (it was all
// cmRenderDetailHtml in public/js/clients.js). Each section is its own root in
// a box that cmRenderDetailHtml leaves; cmShowDetail mounts it and hands it
// its data through cmDetData(name), and unmounts it before the page is drawn
// again (the date range, a save).
//   cmdetstats — the five stat cards, Recent Activity and the meetings list
//   cmdetsettings — the Departments, Kickstart, Platform, CRM, Brand Name,
//                   Billing Name and WhatsApp Group cards
import { useEffect, useState } from 'react';
import { legacy, css, fmtDate, canDo } from '../lib/app.js';
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
