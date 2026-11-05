// Credit Card → the bank picker, "📄 Upload" and the upload status line (was the
// markup above #ccOverview, filled by loadCreditCards / ccSelectBank / _ccDoUpload
// in public/js/mdo.js). mdo.js hands over { banks, bank, canEdit, status } through
// ccTopSetReact; picking a bank goes to ccSelectBank and a file to ccUploadPdf.
// A locked PDF asks for its password here; Unlock & Upload runs
// ccRetryWithPassword, which reads this box (#ccPdfPwdRetry).
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const PICKER = { padding: '8px 14px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13, fontWeight: 600, color: '#0f172a', background: '#fff', outline: 'none', cursor: 'pointer', minWidth: 160 };

function Password({ wrong }) {
  const [show, setShow] = useState(false);
  const box = useRef(null);
  useEffect(() => { const t = setTimeout(() => box.current && box.current.focus(), 50); return () => clearTimeout(t); }, [wrong]);
  return (
    <div ref={css('display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:2px')}>
      {wrong ? <><span ref={css('color:#dc2626')}>❌ Wrong password.</span>{' '}</> : null}
      <span ref={css('color:#d97706')}>🔒 PDF is password protected. Enter password:</span>
      <span ref={css('position:relative;display:inline-flex;align-items:center')}>
        <input type={show ? 'text' : 'password'} id="ccPdfPwdRetry" placeholder="Enter PDF password" ref={el => { box.current = el; if (el) el.setAttribute('style', 'padding:5px 30px 5px 10px;border:1px solid #d97706;border-radius:6px;font-size:12px;width:160px;outline:none'); }}
          onKeyDown={e => { if (e.key === 'Enter') legacy('ccRetryWithPassword'); }} />
        <button type="button" title={show ? 'Hide password' : 'Show password'} aria-label={show ? 'Hide password' : 'Show password'}
          onClick={() => { setShow(s => !s); box.current && box.current.focus(); }}
          onMouseOver={e => { e.currentTarget.style.color = '#475569'; }} onMouseOut={e => { e.currentTarget.style.color = '#94a3b8'; }}
          ref={css('position:absolute;right:6px;display:flex;align-items:center;background:none;border:none;cursor:pointer;padding:2px;color:#94a3b8')}>
          <svg className="eye-on" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={show ? { display: 'none' } : undefined}><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12z" /><circle cx="12" cy="12" r="3" /></svg>
          <svg className="eye-off" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={show ? undefined : { display: 'none' }}><path d="M17.94 17.94A10.07 10.07 0 0 1 12 19c-6.5 0-10-7-10-7a18.45 18.45 0 0 1 5.06-5.94" /><path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c6.5 0 10 7 10 7a18.5 18.5 0 0 1-2.16 3.19" /><path d="M14.12 14.12a3 3 0 1 1-4.24-4.24" /><line x1="2" y1="2" x2="22" y2="22" /></svg>
        </button>
      </span>
      <button onClick={() => legacy('ccRetryWithPassword')} ref={css('padding:5px 12px;background:#d97706;color:#fff;border:none;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer')}>Unlock &amp; Upload</button>
    </div>
  );
}

function Status({ s }) {
  if (!s) return null;
  if (s.kind === 'scan') return <span ref={css('color:#4f46e5')}>⏳ Scanning PDF… (30–60 sec)</span>;
  if (s.kind === 'pwd') return <Password wrong={!!s.wrong} />;
  if (s.kind === 'err') return <span ref={css('color:#dc2626')}>{`❌ ${s.msg}`}</span>;
  return <><span ref={css('color:#16a34a')}>{s.text}</span>{s.kept ? <span ref={css('color:#d97706')}>{s.kept}</span> : null}</>;
}

export default function CcTop() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('ccTopSetReact', setV);
    return () => legacy('ccTopSetReact', null);
  }, []);
  if (!v) return null;
  const options = [{ value: '', label: '— Select Bank —' }, ...v.banks.map(b => ({ value: b, label: b }))];
  return (
    <>
      <SearchSelect value={v.bank} onChange={b => legacy('ccSelectBank', b)} options={options} placeholder="— Select Bank —" style={PICKER} />
      {/* Read-only viewers get no statement upload. */}
      <label ref={el => { if (el) { el.setAttribute('style', 'display:inline-flex;align-items:center;gap:8px;padding:8px 16px;background:var(--accent);color:#fff;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;transition:background .15s'); el.style.display = v.canEdit ? 'inline-flex' : 'none'; } }}
        onMouseOver={e => { e.currentTarget.style.background = '#1e40af'; }} onMouseOut={e => { e.currentTarget.style.background = 'var(--accent)'; }}>
        📄 Upload
        <input type="file" accept=".pdf" ref={css('display:none')} onChange={e => legacy('ccUploadPdf', e.target)} />
      </label>
      <div ref={css('font-size:12px;color:#64748b')}><Status s={v.status} /></div>
    </>
  );
}
