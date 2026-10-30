// Inventory: filters, tabs, stats, equipment cards and the assignments table
// (was loadInventory / renderInventory in public/js/inventory.js). The items,
// assignments and open tab stay in inventory.js (invSetData / invGetData /
// invSetTab): its pop-ups read them and call loadInventory() after a change,
// which comes back here. Inline style text is the old page's (css()), so the
// themes' [style*=…] rules apply unchanged.
import { useCallback, useEffect, useReducer, useState } from 'react';
import { api, me, canDo, legacy, css, fmtDate } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';
import TabGroup from '../components/TabGroup.jsx';

const TYPE_LABELS = { laptop: '💻 Laptop', keyboard: '⌨️ Keyboard', mouse: '🖱️ Mouse', mobile: '📱 Mobile', sim: '📶 SIM', charger: '🔌 Charger', other: '📦 Other' };
const TYPE_EMOJI = { laptop: '💻', keyboard: '⌨️', mouse: '🖱️', mobile: '📱', sim: '📶', charger: '🔌', other: '📦' };
const CONDITION = { new: 'New', good: 'Good', fair: 'Fair', poor: 'Poor' };
const RETURN_REASON = { damaged: '💥 Damaged', retired: '🗄 Retired', offboarding: '🚪 Offboarding' };
const STATUS_COLORS = { available: '#10b981', assigned: '#f59e0b', damaged: '#ef4444', retired: '#94a3b8' };
const FILTER_STYLE = { padding: '7px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13, fontFamily: 'inherit', outline: 'none' };
const TYPE_OPTS = [{ value: '', label: 'All Types' }, ...Object.entries(TYPE_LABELS).map(([value, label]) => ({ value, label }))];
const STATUS_OPTS = [{ value: '', label: 'All Status' }, { value: 'available', label: 'Available' }, { value: 'assigned', label: 'Assigned' }, { value: 'damaged', label: 'Damaged' }, { value: 'retired', label: 'Retired' }];
const emoji = t => TYPE_EMOJI[t] || '📦';
const isIssueApprover = () => { const u = me(); return (u && u.role === 'admin') || canDo('admin_inventory'); };
const STAT_BOX = 'background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:14px 18px';
const STAT_LABEL = 'font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.5px';

function IssueLine({ item }) {
  if (item.issue_status === 'pending') return <div ref={css('font-size:11px;color:#d97706;font-weight:600;margin-top:4px')}>{`🛠 Issue reported ${fmtDate(item.issue_date)} · awaiting approval`}</div>;
  if (item.issue_status === 'approved') return <div ref={css('font-size:11px;color:#ef4444;font-weight:600;margin-top:4px')}>{`❌ Faulty since ${fmtDate(item.issue_date)}`}</div>;
  return null;
}

function Card({ item, actionsOn, tab, user }) {
  const color = STATUS_COLORS[item.status] || '#94a3b8';
  const spec = [item.brand, CONDITION[item.item_condition]].filter(Boolean);
  // Who put it in the register, only when that is not the holder (ids, not names).
  const addedByOther = item.created_by && String(item.created_by) !== String(item.assigned_to_id);
  const acts = [];
  const btn = (cls, label, fn) => acts.push(<button key={label} className={`action-btn ${cls}`} onClick={fn}>{label}</button>);
  if (actionsOn) {
    if (item.status === 'available') btn('edit', 'Assign', () => legacy('openInvAssignModal', item.id, item.name));
    if (item.handover_status === 'active' && item.assignment_id) btn('revise', 'Handover', () => legacy('openInvHandoverModal', item.assignment_id, item.name, item.assigned_to_name || ''));
    if (item.handover_status === 'pending_handover' && item.assignment_id) btn('done', 'Mark Returned', () => legacy('openInvReturnModal', item.assignment_id, item.name, item.return_reason || ''));
    // Delete needs the page's Admin level (the route checks admin_inventory).
    if (canDo('admin_inventory')) btn('delete', 'Delete', () => legacy('deleteInvItem', item.id));
  } else if (tab === 'mine' && item.handover_status === 'active' && item.assignment_id) {
    // The holder only raises the intent to return; the custodian confirms it.
    btn('revise', 'Return', () => legacy('requestInvReturn', item.assignment_id, item.name));
  }
  // Anyone holding it can report a fault, one open issue per item.
  if (tab === 'mine' && item.assignment_id && !item.issue_id) btn('delete', 'Report Issue', () => legacy('openInvIssueModal', item.id, item.name));
  if (tab === 'all' && item.issue_status === 'approved' && isIssueApprover()) btn('done', 'Mark Repaired', () => legacy('resolveInvIssue', item.issue_id));

  return (
    <div onClick={() => legacy('openInvDetailModal', item.id)} title="Click for full details"
      ref={css('background:#fff;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06);cursor:pointer')}>
      {item.photo
        ? <img src={item.photo} alt="" ref={css('width:100%;height:140px;object-fit:cover;border-radius:8px 8px 0 0')} />
        : <div ref={css('width:100%;height:140px;background:linear-gradient(135deg,#f1f5f9,#e2e8f0);display:flex;align-items:center;justify-content:center;border-radius:8px 8px 0 0;font-size:36px')}>{emoji(item.type)}</div>}
      <div ref={css('padding:12px')}>
        <div ref={css('display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:4px')}>
          <div ref={css('font-weight:700;font-size:14px;color:#0f172a')}>{item.name}</div>
          <div ref={css(`font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:${color}1a;color:${color}`)}>{item.status}</div>
        </div>
        {spec.length ? <div ref={css('font-size:12px;color:#64748b')}>{spec.join(' · ')}</div> : null}
        {tab === 'all' && item.created_by_name && addedByOther ? <div ref={css('font-size:11px;color:#94a3b8;margin-top:3px')}>{`✎ Added by ${item.created_by_name}`}</div> : null}
        {item.assigned_to_name ? <div ref={css('font-size:11px;color:#f59e0b;margin-top:4px')}>{`👤 ${item.assigned_to_name}`}</div> : null}
        {item.handover_status === 'pending_handover'
          ? <div ref={css('font-size:11px;color:#ef4444;font-weight:600;margin-top:4px')}>{`⚠️ Handover Pending${item.return_reason ? ` · ${RETURN_REASON[item.return_reason] || item.return_reason}` : ''}`}</div> : null}
        <IssueLine item={item} />
        {acts.length ? <div onClick={e => e.stopPropagation()} ref={css('display:flex;gap:6px;flex-wrap:wrap;margin-top:10px')}>{acts}</div> : null}
      </div>
    </div>
  );
}

function Assignments({ rows }) {
  if (!rows.length) return <div className="empty">No assignments found.</div>;
  return (
    <table>
      <thead><tr><th></th><th>Item</th><th>Assigned To</th><th>Date</th><th>Status</th><th>Actions</th></tr></thead>
      <tbody>
        {rows.map(a => (
          <tr key={a.id}>
            <td>{a.photo
              ? <img src={a.photo} onClick={() => legacy('viewInvPhoto', a.photo)} ref={css('width:36px;height:36px;object-fit:cover;border-radius:6px;cursor:pointer;border:1px solid #e2e8f0')} />
              : <div ref={css('width:36px;height:36px;background:#f1f5f9;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:18px')}>{emoji(a.item_type)}</div>}</td>
            <td><div ref={css('font-weight:600')}>{a.item_name || ''}</div><div ref={css('font-size:11px;color:#64748b')}>{`${TYPE_LABELS[a.item_type] || a.item_type}${a.serial_number ? ' · SN:' + a.serial_number : ''}`}</div></td>
            <td><div ref={css('font-weight:600')}>{a.user_name || ''}</div><div ref={css('font-size:11px;color:#64748b')}>{a.department || ''}</div></td>
            <td>{new Date(a.assigned_at).toLocaleDateString('en-IN')}</td>
            <td>
              {a.handover_status === 'pending_handover' ? <span ref={css('color:#ef4444;font-weight:600')}>⚠️ Pending Return</span>
                : a.handover_status === 'returned' ? <span ref={css('color:#94a3b8')}>Returned</span>
                : <span ref={css('color:#10b981')}>Active</span>}
              {a.return_reason ? <div ref={css('font-size:11px;color:#64748b;margin-top:2px')}>{RETURN_REASON[a.return_reason] || a.return_reason}</div> : null}
            </td>
            <td><div ref={css('display:flex;gap:6px')}>
              {a.handover_status === 'active' ? <button className="action-btn revise" onClick={() => legacy('openInvHandoverModal', a.id, a.item_name || '', a.user_name || '')}>Handover</button> : null}
              {a.handover_status === 'pending_handover' ? <button className="action-btn done" onClick={() => legacy('openInvReturnModal', a.id, a.item_name || '', a.return_reason || '')}>Mark Returned</button> : null}
            </div></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default function Inventory() {
  const [, redraw] = useReducer(n => n + 1, 0);
  const [loaded, setLoaded] = useState(false);
  const [type, setType] = useState('');
  const [status, setStatus] = useState('');
  const user = me();
  // Not "is admin": may this person edit Inventory (hod has it by default).
  const isAdmin = canDo('edit_inventory');

  const load = useCallback(async () => {
    const [items, assignments] = await Promise.all([
      api('/api/inventory/items'),
      isAdmin ? api('/api/inventory/assignments') : Promise.resolve([]),
    ]);
    legacy('invSetData', Array.isArray(items) ? items : [], Array.isArray(assignments) ? assignments : []);
    setLoaded(true);
    redraw();
  }, [isAdmin]);

  useEffect(() => {
    legacy('invSetReact', { reload: load });
    load();
    return () => legacy('invSetReact', null);
  }, [load]);

  const { items = [], assignments = [], tab = 'mine' } = legacy('invGetData') || {};
  const pickTab = t => {
    legacy('invSetTab', t);
    // Status only varies across the shared pool; clear it on the way out of All.
    if (t !== 'all') setStatus('');
    redraw();
  };

  let content;
  if (!loaded) content = <div className="empty">Loading…</div>;
  else if (tab === 'assignments' && isAdmin) content = <Assignments rows={assignments} />;
  else {
    let list = tab === 'mine' ? items.filter(i => i.assigned_to_id && String(i.assigned_to_id) === String(user && user.id)) : items;
    if (type) list = list.filter(i => i.type === type);
    if (status) list = list.filter(i => i.status === status);
    // My Equipment is read-only, even for an admin: the lifecycle actions live on All.
    content = !list.length ? <div className="empty">No equipment found.</div> : (
      <div ref={css('display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:16px;padding:20px')}>
        {list.map(item => <Card key={item.id} item={item} actionsOn={isAdmin && tab !== 'mine'} tab={tab} user={user} />)}
      </div>
    );
  }

  const stat = (label, n, color, id) => (
    <div ref={css(STAT_BOX)}>
      <div ref={css(STAT_LABEL)}>{label}</div>
      <div id={id} ref={css(`font-size:26px;font-weight:700;color:${color}`)}>{n}</div>
    </div>
  );

  return (
    <>
      <div className="page-header">
        <div ref={css('display:flex;gap:8px;align-items:center;margin-left:auto')}>
          <SearchSelect value={type} onChange={setType} options={TYPE_OPTS} placeholder="All Types" style={FILTER_STYLE} />
          {tab === 'all' ? <SearchSelect value={status} onChange={setStatus} options={STATUS_OPTS} placeholder="All Status" style={FILTER_STYLE} /> : null}
          <button id="invAddBtn" className="btn btn-primary" ref={css(tab === 'assignments' ? 'display:none' : 'display:inline-flex')} onClick={() => legacy('openInvAddModal')}>＋ Add Item</button>
        </div>
      </div>

      <TabGroup id="invTabs" style="margin-bottom:16px;width:fit-content" active={tab} onPick={pickTab} tabs={[
        { key: 'mine', label: 'My Equipment' },
        { key: 'all', label: 'All Equipment', hidden: !isAdmin, style: 'display:inline-flex' },
        { key: 'assignments', label: 'Assignments', hidden: !isAdmin, style: 'display:inline-flex' },
      ]} />

      {isAdmin ? (
        <div id="invStats" ref={css('display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:18px')}>
          {stat('Total Items', items.length, 'var(--accent)', 'invStatTotal')}
          {stat('Assigned', items.filter(i => i.status === 'assigned').length, '#f59e0b', 'invStatAssigned')}
          {stat('Available', items.filter(i => i.status === 'available').length, '#10b981', 'invStatAvailable')}
          {stat('Handover Pending', items.filter(i => i.handover_status === 'pending_handover').length, '#ef4444', 'invStatHandover')}
        </div>
      ) : null}

      <div id="invContent">{content}</div>
    </>
  );
}
