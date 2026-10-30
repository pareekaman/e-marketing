// Client Escalations: the feedback cards (was loadFeedbackAdmin in public/js/portal.js).
// loadFeedbackAdmin() still clears the badge and marks it seen, then mounts
// this page once and asks it to reload after that. Inline style text is the
// old page's, so the themes' [style*=…] rules apply unchanged.
import { useCallback, useEffect, useState } from 'react';
import { api, legacy, css } from '../lib/app.js';

const stars = n => '★'.repeat(n) + '☆'.repeat(5 - n);
const ratingColor = n => (n >= 4 ? '#10b981' : n === 3 ? '#f59e0b' : '#ef4444');

function Card({ f }) {
  return (
    <div id={`fb-card-${f.id}`}
      ref={css('background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px 22px;transition:box-shadow .15s')}
      onMouseOver={e => { e.currentTarget.style.boxShadow = '0 4px 16px rgba(0,0,0,.08)'; }}
      onMouseOut={e => { e.currentTarget.style.boxShadow = ''; }}>
      <div ref={css('display:flex;justify-content:space-between;align-items:start;margin-bottom:12px')}>
        <div>
          <div ref={css('font-weight:700;color:#0f172a;font-size:15px')}>{f.client_name}</div>
          <div ref={css('font-size:12px;color:#64748b;margin-top:2px')}>{new Date(f.created_at).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })}</div>
        </div>
        <div ref={css('display:flex;align-items:start;gap:10px')}>
          <div ref={css('text-align:right')}>
            <div ref={css(`font-size:20px;color:${ratingColor(f.rating)};letter-spacing:1px`)}>{stars(f.rating)}</div>
            <div ref={css('font-size:11px;color:#94a3b8;margin-top:2px')}>{`${f.rating}/5`}</div>
          </div>
        </div>
      </div>
      {f.description ? <div ref={css('font-size:13px;color:#374151;background:#f8fafc;border-radius:8px;padding:10px 12px;margin-bottom:12px;line-height:1.5')}>{`"${f.description}"`}</div> : null}
      <div ref={css('border-top:1px solid #f1f5f9;padding-top:10px;margin-top:4px')}>
        <div ref={css('font-size:12px;color:#0f172a;font-weight:600')}>{f.employee_name}</div>
        <div ref={css('font-size:11px;color:#64748b;margin-top:2px')}>{f.department || '—'}</div>
        {f.hod_name ? <div ref={css('font-size:11px;color:#7c3aed;margin-top:4px')}>{`HOD: ${f.hod_name}`}</div> : null}
      </div>
    </div>
  );
}

export default function Feedback() {
  const [rows, setRows] = useState(null);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    setRows(null); setError('');
    try {
      const r = await api('/api/feedback');
      if (r && r.error) { setError(r.error); return; }
      setRows(Array.isArray(r) ? r : []);
    } catch (e) { setError('Failed to load: ' + e.message); }
  }, []);

  useEffect(() => {
    legacy('fbSetReactReload', load);
    load();
    return () => legacy('fbSetReactReload', null);
  }, [load]);

  let body;
  if (error) body = <div className="empty" ref={css('color:#dc2626')}>{error}</div>;
  else if (rows === null) body = <div className="empty" ref={css('padding:40px')}>Loading…</div>;
  else if (!rows.length) body = <div className="empty" ref={css('padding:40px')}>No feedback received yet.</div>;
  else body = (
    <div ref={css('display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px')}>
      {rows.map(f => <Card key={f.id} f={f} />)}
    </div>
  );

  return (
    <>
      <div ref={css('display:flex;justify-content:flex-end;align-items:center;margin-bottom:18px')}>
        <button className="btn btn-outline" onClick={() => legacy('loadFeedbackAdmin')} ref={css('font-size:12px')}>🔄 Refresh</button>
      </div>
      <div id="feedbackContent">{body}</div>
    </>
  );
}
