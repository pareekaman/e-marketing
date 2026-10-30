// Enquiry Capture list (was ecRender in public/js/enquiry.js). The rows stay in
// enquiry.js's EC_ALL: its form, status change, import, delete and Add in
// Client Master work on them and call ecRender() / ecLoad(), which come back
// here through ecSetReact. Markup ids, classes and inline style text are the
// old page's, so the themes apply unchanged.
import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { api, canDo, legacy, css } from '../lib/app.js';

const STATUS_COLORS = { Open: ['#475569', '#f1f5f9'], Lost: ['#b91c1c', '#fee2e2'], Converted: ['#15803d', '#dcfce7'], Paused: ['#b45309', '#fef3c7'] };
const list = v => String(v || '').split('||').filter(Boolean);
const fmt = d => {
  if (!d) return '';
  const x = new Date(String(d).slice(0, 10) + 'T00:00:00');
  return isNaN(x) ? String(d) : x.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
};
const DASH = <span ref={css('color:#cbd5e1')}>—</span>;

function LinkTo({ url, label }) {
  if (!url) return null;
  return <>{' '}<a href={url} target="_blank" rel="noopener" onClick={e => e.stopPropagation()} ref={css('color:var(--accent);font-weight:600')}>{label} ↗</a></>;
}

function StatusCell({ e, statuses, canStatus, canAddClient }) {
  const st = e.status || 'Open';
  const [fg, bg] = STATUS_COLORS[st] || STATUS_COLORS.Open;
  const look = `color:${fg};background:${bg};border:1px solid ${fg}33;border-radius:999px;font-size:12px;font-weight:700`;
  return (
    <td ref={css('white-space:nowrap')}>
      {canStatus ? (
        // Uncontrolled, as before: ecSetStatus disables it while saving and
        // puts the old value back itself if the save fails. The key remounts
        // it when the saved status changes.
        <select key={st} data-no-search defaultValue={st} onChange={ev => legacy('ecSetStatus', e.id, ev.target)}
          ref={css(`${look};padding:4px 8px;cursor:pointer;outline:none`)}>
          {statuses.filter(s => s !== 'Paused' || e.client_id || st === 'Paused')   // Paused is for clients in Client Master
            .map(s => <option key={s} value={s} ref={css('color:#0f172a;background:#fff')}>{s}</option>)}
        </select>
      ) : <span ref={css(`${look};padding:3px 10px;display:inline-block`)}>{st}</span>}
      {e.client_id ? (
        <>
          <div ref={css('font-size:11px;color:#15803d;font-weight:600;margin-top:6px')}>✓ In Client Master</div>
          {e.client_crm_name ? <div ref={css('font-size:11px;color:#64748b;margin-top:1px')}>CRM: <b ref={css('color:#334155')}>{e.client_crm_name}</b></div> : null}
          {st === 'Paused' ? <div ref={css('font-size:11px;color:#b45309;font-weight:600;margin-top:1px')}>⏸ Onboarding FMS on hold</div> : null}
        </>
      ) : st !== 'Converted' ? null
        : canAddClient
          ? <button className="cm-btn-ghost" ref={css('display:block;margin-top:6px;padding:4px 10px;font-size:12px')} onClick={() => legacy('ecAddToClientMaster', e.id)}>➕ Add in Client Master</button>
          : <div ref={css('font-size:11px;color:#94a3b8;margin-top:6px')}>Not in Client Master yet</div>}
    </td>
  );
}

function Stage({ date, url, label }) {
  if (!date && !url) return DASH;
  return <div ref={css('white-space:nowrap')}>{date ? fmt(date) : <span ref={css('color:#94a3b8')}>No date</span>}<LinkTo url={url} label={label} /></div>;
}

export default function Enquiry() {
  const [, redraw] = useReducer(n => n + 1, 0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [q, setQ] = useState('');
  const seq = useRef(0);

  const load = useCallback(async () => {
    const n = ++seq.current;
    setLoading(true); setError('');
    const have = legacy('ecGetData');
    const [rows, opts] = await Promise.all([
      api('/api/enquiries'),
      have && have.opts ? Promise.resolve(have.opts) : api('/api/enquiries/options'),
    ]);
    if (n !== seq.current) return;
    setLoading(false);
    if (!Array.isArray(rows)) { setError((rows && rows.error) || 'Could not load enquiries'); return; }
    legacy('ecSetData', rows, opts);
    redraw();
    if (rows.some(e => Number(e.sheet_pending))) legacy('ecRetrySheet');
    // Clients added from here whose Onboarding FMS row is still to be written.
    legacy('cmRetryFms');
  }, []);

  useEffect(() => {
    legacy('ecSetReact', { reload: load, render: redraw });
    load();
    return () => legacy('ecSetReact', null);
  }, [load]);

  const { rows = [], opts } = legacy('ecGetData') || {};
  const month = new Date().toISOString().slice(0, 7);
  const s = q.toLowerCase().trim();
  const shown = !s ? rows : rows.filter(e => [e.client_name, e.business_name, e.mobile, e.lead_handle_by, e.platforms, e.project_types, e.status, e.client_crm_name].join(' ').toLowerCase().includes(s));
  const canDelete = canDo('admin_enquiry');
  const canEdit = canDo('edit_enquiry');
  const canStatus = canDo('edit_enquiry') || canDo('crm_clients');
  const canAddClient = canStatus && canDo('edit_clients');
  const statuses = (opts && opts.statuses) || Object.keys(STATUS_COLORS);

  let body;
  if (loading && !rows.length) body = <div className="empty">Loading enquiries…</div>;
  else if (error) body = <div className="empty" ref={css('color:#dc2626')}>{error}</div>;
  else if (!shown.length) body = <div className="empty">{rows.length ? 'No enquiries match the search.' : 'No enquiries yet.'}</div>;
  else body = (
    // Actions come first: the table is wider than most screens, and a button
    // at the far right end needs a sideways scroll to reach.
    <table className="dr-table">
      <thead><tr>
        {canEdit || canDelete ? <th></th> : null}<th>Status</th><th>Added</th><th>Client</th><th>Mobile</th><th>Lead Handle By</th><th>Platform</th>
        <th>Meeting</th><th>Proposal</th><th>Conversion</th><th>Order Value</th>
      </tr></thead>
      <tbody>
        {shown.map(e => (
          <tr key={e.id}>
            {canEdit || canDelete ? (
              <td ref={css('white-space:nowrap')}>
                {canEdit ? <button className="cm-btn-ghost" ref={css('padding:5px 12px;font-size:12px')} onClick={() => legacy('ecOpenForm', e.id)}>✏️ Edit</button> : null}
                {canEdit && canDelete ? ' ' : null}
                {canDelete ? <button className="cm-client-del" ref={css('margin-left:6px')} onClick={() => legacy('ecDelete', e.id)}>Delete</button> : null}
              </td>
            ) : null}
            <StatusCell e={e} statuses={statuses} canStatus={canStatus} canAddClient={canAddClient} />
            <td ref={css('white-space:nowrap')}>
              <div>{fmt(e.created_at)}</div>
              <div ref={css('font-size:11px;color:#94a3b8')}>{e.created_by_name || (e.source === 'sheet' ? 'Google Form' : '')}</div>
              {e.updated_at ? <div ref={css('font-size:11px;color:#94a3b8')} title={`Last edited ${e.updated_at}`}>Edited by {e.updated_by_name || '—'}</div> : null}
              {Number(e.sheet_pending) ? <div ref={css('font-size:11px;color:#b45309;font-weight:600;margin-top:2px')} title="Its latest save has not reached the Enquiry Capture sheet yet. It is sent again automatically.">⏳ Not in sheet yet</div> : null}
            </td>
            <td ref={css('min-width:180px')}>
              <b>{e.client_name}</b>
              <div ref={css('font-size:12px;color:#64748b')}>{e.business_name}</div>
              {list(e.project_types).map(p => <span key={p} className="pill-dept" ref={css('margin:3px 4px 0 0')}>{p}</span>)}
            </td>
            <td ref={css('white-space:nowrap')}>{e.mobile || '—'}</td>
            <td ref={css('white-space:nowrap')}>{e.lead_handle_by || '—'}</td>
            <td ref={css('min-width:160px')}>{list(e.platforms).length ? list(e.platforms).map(p => <span key={p} className="pill-tag" ref={css('margin:2px 4px 2px 0')}>{p}</span>) : '—'}</td>
            <td>
              {e.meeting_scheduled_date || e.meeting_done_date || e.meeting_url ? (
                <>
                  {e.meeting_scheduled_date ? <div ref={css('white-space:nowrap')}><span ref={css('color:#94a3b8')}>Scheduled</span> {fmt(e.meeting_scheduled_date)}</div> : null}
                  {e.meeting_done_date ? <div ref={css('white-space:nowrap')}><span ref={css('color:#94a3b8')}>Done</span> {fmt(e.meeting_done_date)}</div> : null}
                  <LinkTo url={e.meeting_url} label="Link" />
                </>
              ) : DASH}
            </td>
            <td><Stage date={e.proposal_date} url={e.proposal_url} label="Proposal" /></td>
            <td>{e.conversion_date ? <span ref={css('color:#15803d;font-weight:700;white-space:nowrap')}>{fmt(e.conversion_date)}</span> : DASH}</td>
            <td ref={css('white-space:nowrap')}>{e.order_value || '—'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );

  return (
    <div className="cm-card">
      <div className="cm-header">
        <div className="cm-header-text">
          <div className="cm-sub">Sales enquiries, with the same fields as the Enquiry Capture Google Form.</div>
        </div>
        <div className="cm-stats">
          <div className="cm-stat">
            <div className="cm-stat-val" id="ecStatTotal">{loading && !rows.length ? '—' : rows.length}</div>
            <div className="cm-stat-label">All Enquiries</div>
          </div>
          <div className="cm-stat">
            <div className="cm-stat-val" id="ecStatMonth">{loading && !rows.length ? '—' : rows.filter(e => String(e.created_at || '').startsWith(month)).length}</div>
            <div className="cm-stat-label">This Month</div>
          </div>
          <div className="cm-stat">
            <div className="cm-stat-val" id="ecStatConverted" ref={css('color:#16a34a')}>{loading && !rows.length ? '—' : rows.filter(e => e.conversion_date).length}</div>
            <div className="cm-stat-label">Converted</div>
          </div>
        </div>
      </div>
      <div className="cm-toolbar">
        <input className="cm-search" type="text" id="ecSearch" placeholder="🔍 Search client, business, mobile or platform..." value={q} onChange={ev => setQ(ev.target.value)} />
        <div className="cm-add-row">
          {canEdit ? <button className="cm-btn-primary" id="ecAddBtn" onClick={() => legacy('ecOpenForm')}>+ New Enquiry</button> : null}
          {canDelete ? <>{canEdit ? ' ' : null}<button className="cm-btn-ghost" id="ecImportBtn" onClick={() => legacy('ecImport')} title="Bring in the Google Form's responses; ones already here are skipped">⬇ Import from sheet</button></> : null}
        </div>
      </div>
      <div id="ecListWrap" className="dr-table-wrap" ref={css('max-height:none')}>{body}</div>
    </div>
  );
}
