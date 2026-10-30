// Enquiry Capture → the New / Edit Enquiry form (was #ecModal in app.html and
// ecOpenForm / ecSave in public/js/enquiry.js). ecOpenForm(id) opens it
// through ecFormSetReact; enquiry.js keeps the rows and the form options
// (ecGetData / ecSetData) and reloads the list with ecLoad after a save.
import { useEffect, useRef, useState } from 'react';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';
import { api, toast, legacy, css } from '../lib/app.js';

const list = v => String(v || '').split('||').filter(Boolean);
const EMPTY = {
  client_name: '', business_name: '', mobile: '', lead_handle_by: '', project_types: [], platforms: [],
  meeting_scheduled_date: '', meeting_done_date: '', meeting_url: '', proposal_date: '', proposal_url: '', conversion_date: '', order_value: '',
};
const GRID = 'display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:0 16px';

function Checks({ options, picked, onToggle }) {
  return options.map(o => (
    <label key={o} ref={css('display:inline-flex;align-items:center;gap:6px;margin:0 14px 6px 0;font-size:13px;font-weight:500;color:#334155;text-transform:none;letter-spacing:0;cursor:pointer')}>
      <input type="checkbox" value={o} checked={picked.includes(o)} onChange={() => onToggle(o)} ref={css('width:15px;height:15px;margin:0;accent-color:#4f46e5')} />{o}
    </label>
  ));
}

export default function EnquiryForm() {
  const [open, setOpen] = useState(false);
  const [editId, setEditId] = useState(null);
  const [f, setF] = useState(EMPTY);
  const [opts, setOpts] = useState({ leadHandlers: [], projectTypes: [], platforms: [] });
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const nameRef = useRef(null);

  useEffect(() => {
    legacy('ecFormSetReact', async id => {
      if (!legacy('canDo', 'edit_enquiry')) return;
      let { rows, opts: o } = legacy('ecGetData') || {};
      if (!o) {
        o = await api('/api/enquiries/options');
        if (!o || o.error) { toast((o && o.error) || 'Could not load the form', 'error'); return; }
        legacy('ecSetData', rows || [], o);
      }
      const cur = id ? (rows || []).find(e => e.id === id) : null;
      if (id && !cur) return;
      setEditId(cur ? cur.id : null);
      setF(cur ? {
        ...EMPTY, ...Object.fromEntries(Object.keys(EMPTY).map(k => [k, cur[k] || ''])),
        project_types: list(cur.project_types), platforms: list(cur.platforms),
      } : EMPTY);
      setOpts(o);
      setErr(''); setBusy(false);
      setOpen(true);
      setTimeout(() => nameRef.current && nameRef.current.focus(), 50);
    });
    return () => legacy('ecFormSetReact', null);
  }, []);

  const set = (k, v) => setF(x => ({ ...x, [k]: v }));
  const field = k => ({ value: f[k], onChange: e => set(k, e.target.value) });
  const toggle = (k, o) => setF(x => ({ ...x, [k]: x[k].includes(o) ? x[k].filter(v => v !== o) : [...x[k], o] }));
  // An imported response can carry a value the form no longer offers (e.g.
  // "2 Landing pages"); it is listed too, ticked, so saving keeps it.
  const withCurrent = (options, picked) => [...options, ...picked.filter(p => !options.includes(p))];
  const leadOptions = [{ value: '', label: 'Choose…' },
    ...withCurrent(opts.leadHandlers || [], f.lead_handle_by ? [f.lead_handle_by] : []).map(n => ({ value: n, label: n }))];

  async function save() {
    setErr('');
    const t = k => String(f[k] || '').trim();
    const body = {
      client_name: t('client_name'), business_name: t('business_name'), mobile: t('mobile'),
      lead_handle_by: t('lead_handle_by'), project_types: f.project_types, platforms: f.platforms,
      meeting_scheduled_date: t('meeting_scheduled_date'), meeting_done_date: t('meeting_done_date'), meeting_url: t('meeting_url'),
      proposal_date: t('proposal_date'), proposal_url: t('proposal_url'), conversion_date: t('conversion_date'),
      order_value: t('order_value'),
    };
    if (!body.client_name) return setErr('Please enter the client name');
    if (!body.business_name) return setErr('Please enter the business name');
    setBusy(true);
    const r = editId ? await api('/api/enquiries/' + editId, 'PUT', body) : await api('/api/enquiries', 'POST', body);
    setBusy(false);
    if (!r || r.error) return setErr((r && r.error) || 'Could not save the enquiry');
    setOpen(false);
    // Saved either way; a sheet problem comes back as a warning to show, and
    // one the app retries by itself (queued) is not shown as an error.
    if (r.warning) toast(r.warning, r.queued ? 'success' : 'error');
    else toast(editId ? 'Enquiry updated' : 'Enquiry saved');
    legacy('ecLoad');
  }

  return (
    <Modal open={open} onClose={() => setOpen(false)} style="width:680px;max-width:96vw;max-height:90vh;overflow-y:auto">
      <h3>{editId ? '✏️ Edit Enquiry' : '📝 New Enquiry'}</h3>
      <div className="alert error" style={{ display: err ? 'block' : 'none' }}>{err}</div>
      <div className="form-group">
        <label>Client Name <span ref={css('color:#dc2626')}>*</span></label>
        <input type="text" maxLength={255} ref={nameRef} {...field('client_name')} />
      </div>
      <div className="form-group">
        <label>Business Name <span ref={css('color:#dc2626')}>*</span></label>
        <textarea maxLength={500} ref={css('min-height:56px')} {...field('business_name')} />
      </div>
      <div ref={css(GRID)}>
        <div className="form-group">
          <label>Mobile Number</label>
          <input type="tel" maxLength={30} {...field('mobile')} />
        </div>
        <div className="form-group">
          <label>Lead Handle By</label>
          <SearchSelect value={f.lead_handle_by} onChange={v => set('lead_handle_by', v)} options={leadOptions} />
        </div>
      </div>
      <div className="form-group">
        <label>Project Type</label>
        <div><Checks options={withCurrent(opts.projectTypes || [], f.project_types)} picked={f.project_types} onToggle={o => toggle('project_types', o)} /></div>
      </div>
      <div className="form-group">
        <label>Platform</label>
        <div><Checks options={withCurrent(opts.platforms || [], f.platforms)} picked={f.platforms} onToggle={o => toggle('platforms', o)} /></div>
      </div>
      <div ref={css(GRID)}>
        <div className="form-group"><label>Meeting Scheduled Date</label><input type="date" {...field('meeting_scheduled_date')} /></div>
        <div className="form-group"><label>Meeting Done Date</label><input type="date" {...field('meeting_done_date')} /></div>
      </div>
      <div className="form-group">
        <label>Meeting URL</label>
        <input type="url" placeholder="https://" {...field('meeting_url')} />
      </div>
      <div ref={css(GRID)}>
        <div className="form-group"><label>Proposal Date</label><input type="date" {...field('proposal_date')} /></div>
        <div className="form-group"><label>Proposal URL</label><input type="url" placeholder="https://" {...field('proposal_url')} /></div>
      </div>
      <div ref={css(GRID)}>
        <div className="form-group"><label>Conversion Date</label><input type="date" {...field('conversion_date')} /></div>
        <div className="form-group"><label>Order Value</label><input type="text" maxLength={100} {...field('order_value')} /></div>
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <button className="btn btn-outline" onClick={() => setOpen(false)}>Cancel</button>
        <button className="btn btn-primary" disabled={busy} onClick={save}>{editId ? '💾 Save Changes' : '💾 Save Enquiry'}</button>
      </div>
    </Modal>
  );
}
