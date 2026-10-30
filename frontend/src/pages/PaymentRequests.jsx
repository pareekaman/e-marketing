// Payment Request → the requests list: title, month filter and table (was
// loadMyPaymentRequests in public/js/payments.js). The create form and the
// Edit / Bill / Cancel pop-ups stay in payments.js and mdo.js; they call
// loadMyPaymentRequests(), which comes back here. Inline style text is the
// old page's (css()), so the themes' [style*=…] rules apply unchanged.
import { useCallback, useEffect, useState } from 'react';
import { api, me, canDo, legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const TH = 'padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase';
const TH_R = 'padding:9px 14px;text-align:right;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase';
const TH_C = 'padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase';
const MONTH_STYLE = { padding: '7px 10px', border: '1.5px solid #e2e8f0', borderRadius: 10, fontSize: 13, fontFamily: 'inherit', background: '#fff', minWidth: 160 };
const monthOf = r => { const d = new Date(r.created_at); return isNaN(d) ? '' : `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`; };

function StatusBadge({ s }) {
  if (s === 'approved') return <span ref={css('background:#dcfce7;color:#16a34a;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>✅ Approved</span>;
  if (s === 'rejected') return <span ref={css('background:#fee2e2;color:#dc2626;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>❌ Rejected</span>;
  return <span ref={css('background:#fef9c3;color:#a16207;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>⏳ Pending</span>;
}

function Row({ r, i, sentinels, isAdminView, canMark }) {
  const parsed = legacy('prParseReason', r.reason) || {};
  const amt = r.amount > 0 ? Number(r.amount) : parsed.amount;
  const cur = parsed.currency || '₹';
  const isDone = r.payment_done || sentinels.some(s => s.card_number === '__paid__' && s.reason === '__paid__:' + String(r.id));
  const cancel = sentinels.find(s => s.card_number === '__cancelled__' && s.reason.startsWith('__cancelled__:' + String(r.id) + ':'));
  const bill = sentinels.find(s => s.card_number === '__bill__' && s.reason.startsWith('__bill__:' + String(r.id) + ':'));
  const billFileId = bill ? bill.reason.replace('__bill__:' + String(r.id) + ':', '') : null;

  let billCell;
  if (billFileId) billCell = (
    <span ref={css('display:inline-flex;align-items:center;gap:5px;white-space:nowrap')}>
      <a href={`https://drive.google.com/file/d/${encodeURIComponent(billFileId)}/view`} target="_blank" ref={css('display:inline-flex;align-items:center;gap:4px;background:#fff;color:#16a34a;border:1.5px solid #16a34a;border-radius:7px;padding:4px 11px;font-size:12px;font-weight:600;text-decoration:none;cursor:pointer')}>👁 View</a>
      <button onClick={() => legacy('prOpenBillModal', r.id, true)} title="Change Bill" ref={css('display:inline-flex;align-items:center;justify-content:center;background:#fff;border:1.5px solid #cbd5e1;border-radius:7px;cursor:pointer;color:#64748b;font-size:13px;padding:4px 7px;line-height:1')}>🔄</button>
    </span>
  );
  else if (legacy('prIsBillUploading', r.id)) billCell = <span ref={css('color:#64748b;font-size:12px;font-weight:600')}>Processing…</span>;
  else if (isDone && r.status === 'approved' && canMark(r)) billCell = <button onClick={() => legacy('prOpenBillModal', r.id, true)} ref={css('background:none;border:none;color:#f59e0b;font-size:12px;font-weight:600;cursor:pointer;padding:0;text-decoration:underline')}>Upload Bill</button>;
  else billCell = <span ref={css('color:#94a3b8;font-size:12px')}>—</span>;

  let pay = null;
  if (r.status === 'approved') {
    if (isDone) pay = <span ref={css('font-size:20px')}>✅</span>;
    else if (cancel) pay = <span ref={css('font-size:20px')} title={cancel.reason.replace('__cancelled__:' + r.id + ':', '')}>❌</span>;
    // Marking paid (with the bill) or cancelled is the requester's own step.
    else if (!canMark(r)) pay = <span ref={css('color:#94a3b8;font-size:12px')}>Not marked yet</span>;
    else pay = (
      <span ref={css('display:inline-flex;gap:10px;align-items:center')}>
        <button onClick={() => legacy('prOpenBillModal', r.id)} title="Mark as Paid" ref={css('background:none;border:none;font-size:22px;cursor:pointer;line-height:1;padding:2px')}>✅</button>
        <button onClick={() => legacy('prOpenCancelModal', r.id)} title="Cancel Payment" ref={css('background:none;border:none;font-size:22px;cursor:pointer;line-height:1;padding:2px')}>❌</button>
      </span>
    );
  }

  return (
    <tr ref={css(i % 2 ? 'background:#f8fafc' : '')}>
      <td ref={css('padding:9px 14px;font-size:12px;color:#64748b')}>{new Date(r.created_at).toLocaleDateString('en-IN')}</td>
      {isAdminView ? <td ref={css('padding:9px 14px;font-size:13px;font-weight:600')}>{r.name}</td> : null}
      <td ref={css('padding:9px 14px;font-size:13px;font-weight:600')}>{r.bank_name}</td>
      <td ref={css('padding:9px 14px;font-size:12px')}>{r.card_number}</td>
      <td ref={css('padding:9px 14px;font-size:13px;font-weight:700;text-align:right;color:#0f172a')}>{amt ? cur + Number(amt).toLocaleString('en-IN', { minimumFractionDigits: 2 }) : '—'}</td>
      <td ref={css('padding:9px 14px;font-size:12px;color:#374151;max-width:200px')}>{parsed.reason || r.reason}</td>
      {/* prDeptCell returns escaped markup. */}
      <td ref={css('padding:9px 14px;max-width:190px')} dangerouslySetInnerHTML={{ __html: legacy('prDeptCell', r) || '' }} />
      <td ref={css('padding:9px 14px;white-space:nowrap')}>
        <StatusBadge s={r.status} />
        {r.status === 'pending' ? (
          <button onClick={() => legacy('prOpenEditModal', r.id)} title="Edit this request"
            ref={css('background:none;border:none;cursor:pointer;font-size:13px;line-height:1;padding:2px 4px;margin-left:6px;color:#64748b')}
            onMouseOver={e => { e.currentTarget.style.color = '#4f46e5'; }} onMouseOut={e => { e.currentTarget.style.color = '#64748b'; }}>✏️</button>
        ) : null}
      </td>
      <td ref={css('padding:9px 14px;text-align:center')}>{pay}</td>
      <td ref={css('padding:9px 14px;text-align:center')}>{billCell}</td>
    </tr>
  );
}

export default function PaymentRequests() {
  const user = me();
  // Admin, or Payment Request at the "Admin" level, sees everyone's requests.
  const isAdminView = (user && user.role === 'admin') || canDo('admin_paymentreq');
  // Paid / Cancel / Upload Bill: the request's owner, an admin or a payment approver.
  const canMark = r => (user && user.role === 'admin') || (user && user.canApprovePayments) || Number(r.submitted_by) === Number(user && user.id);
  const [data, setData] = useState(null);   // { rows, sentinels } | { error }
  const [month, setMonth] = useState('');

  const load = useCallback(async () => {
    try {
      const all = await api(isAdminView ? '/api/payment-requests' : '/api/payment-requests/my');
      if (!Array.isArray(all)) { setData({ rows: [], sentinels: [] }); return; }
      const rows = all.filter(r => r.bank_name !== '__system__');
      legacy('prSetMyRows', rows);   // the Edit pop-up fills itself from these
      setData({ rows, sentinels: all.filter(r => r.bank_name === '__system__') });
    } catch (e) { setData({ error: e.message }); }
  }, [isAdminView]);

  useEffect(() => {
    legacy('prSetReact', load);
    load();
    return () => legacy('prSetReact', null);
  }, [load]);

  const rows = (data && data.rows) || [];
  const months = [...new Set(rows.map(monthOf).filter(Boolean))].sort().reverse();
  // A month that has no requests any more falls back to all.
  const m = month && months.includes(month) ? month : '';
  const shown = m ? rows.filter(r => monthOf(r) === m) : rows;
  const options = [{ value: '', label: 'All months' }, ...months.map(x => {
    const [y, mo] = x.split('-');
    return { value: x, label: new Date(+y, +mo - 1, 1).toLocaleDateString('en-GB', { month: 'long', year: 'numeric' }) };
  })];

  let body;
  if (!data) body = <div ref={css('padding:24px;text-align:center;color:#94a3b8;font-size:13px')}>Loading...</div>;
  else if (data.error) body = <div ref={css('padding:20px;color:#dc2626;font-size:13px')}>{'Error: ' + data.error}</div>;
  else if (!shown.length) body = <div ref={css('padding:20px;text-align:center;color:#94a3b8;font-size:13px')}>No requests</div>;
  else body = (
    <table ref={css('width:100%;border-collapse:collapse')}>
      <thead><tr ref={css('background:#f8fafc;border-bottom:2px solid #e2e8f0')}>
        <th ref={css(TH)}>Date</th>
        {isAdminView ? <th ref={css(TH)}>Employee</th> : null}
        <th ref={css(TH)}>Bank</th>
        <th ref={css(TH)}>Card</th>
        <th ref={css(TH_R)}>Amount</th>
        <th ref={css(TH)}>Reason</th>
        <th ref={css(TH)}>Department</th>
        <th ref={css(TH)}>Status</th>
        <th ref={css(TH_C)}>Payment Status</th>
        <th ref={css(TH_C)}>Bill</th>
      </tr></thead>
      <tbody>{shown.map((r, i) => <Row key={r.id} r={r} i={i} sentinels={data.sentinels} isAdminView={isAdminView} canMark={canMark} />)}</tbody>
    </table>
  );

  return (
    <>
      <div ref={css('display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px;flex-wrap:wrap')}>
        <div ref={css('font-size:14px;font-weight:700;color:#0f172a')}>{isAdminView ? 'All Requests' : 'My Requests'}</div>
        <SearchSelect value={m} onChange={setMonth} options={options} placeholder="All months" style={MONTH_STYLE} />
      </div>
      <div id="prMyList" ref={css('background:#fff;border:1px solid #e2e8f0;border-radius:12px;overflow-x:auto')}>{body}</div>
    </>
  );
}
