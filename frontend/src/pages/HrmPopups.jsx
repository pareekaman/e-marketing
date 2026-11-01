// HRM → the candidate Details, Update Status, Joining Details, Offer Letter and
// Email pop-ups (were #hrmDetailsModal, #hrmStatusModal, #hrmJoiningDetailsModal,
// #hrmOfferViewModal and #hrmEmailModal in app.html, opened by openHrmDetailsModal /
// openHrmStatusModal / viewJoiningDetails / viewOfferLetter / openEmailModal in public/js/hrm.js;
// the joining submission is fetched there). A status pick goes through
// hrmStatusPick (an offer stage first needs the joining details, then closes
// this and opens its own form); the save is hrmStatusSend.
import { Fragment, useEffect, useRef, useState } from 'react';
import { legacy, css, toast } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const ROW = 'display:flex;gap:10px;padding:7px 0;border-bottom:1px solid #f1f5f9';
const KEY = 'min-width:130px;color:#64748b;font-size:12px;font-weight:600';
function Row({ label, value }) {
  return <div ref={css(ROW)}><span ref={css(KEY)}>{label}</span><span ref={css('flex:1;word-break:break-word;font-size:13px;color:#0f172a')}>{value ? String(value) : '—'}</span></div>;
}

function DetailsBody({ d, close }) {
  const c = d.c;
  const date = c.interview_date ? new Date(c.interview_date).toLocaleDateString('en-IN') : '—';
  return (
    <>
      <h3>👤 <span>{c.name || 'Candidate'}</span></h3>
      <div ref={css('margin-top:8px')}>
        <Row label="Name" value={c.name} /><Row label="Phone" value={c.phone} /><Row label="Position" value={c.profile_position} />
        <Row label="Department" value={c.department} /><Row label="Candidate Email" value={c.email} />
        <Row label="Interviewer Phone" value={c.interviewer_phone} /><Row label="Interviewer (HOD)" value={c.interviewer_email} />
        <Row label="Interview" value={`${date}${c.interview_time ? ' ' + c.interview_time : ''}`} />
        <Row label="Status" value={d.label} />
        {c.interview_mode === 'offline' ? <><Row label="Mode" value="Offline (at the office)" /><Row label="Venue" value={c.venue} /></>
          : c.meeting_link ? <><Row label="Mode" value="Online" />
            <div ref={css(ROW)}><span ref={css(KEY)}>Meeting Link</span><span ref={css('flex:1;word-break:break-all;font-size:13px')}><a href={c.meeting_link} target="_blank" ref={css('color:#4f46e5')}>{c.meeting_link}</a></span></div></>
            : <><Row label="Mode" value="Online" /><Row label="Meeting Link" value="" /></>}
        <Row label="Notes" value={c.notes} />
      </div>
      <div className="modal-footer"><button className="btn btn-primary" onClick={close}>Close</button></div>
    </>
  );
}

const STATUSES = [{ value: '', label: '— Select —' }, { value: 'Scheduled', label: '📅 Scheduled' }, { value: 'Rescheduled', label: '🔄 Rescheduled' },
  { value: 'Selected', label: '✅ Selected' }, { value: 'Rejected', label: '❌ Rejected' },
  { value: 'Offer Sent', label: '📄 Preliminary Offer Letter Sent' }, { value: 'Offer Letter Sent', label: '📬 Offer Letter Sent' }];

function StatusBody({ d, close }) {
  const c = d.c;
  const [status, setStatus] = useState('');
  const [date, setDate] = useState('');
  const [time, setTime] = useState('');
  const [reason, setReason] = useState('');
  const pick = v => {
    const r = legacy('hrmStatusPick', v);
    if (r === false) setStatus('');
    else if (r === 'away') close();
    else setStatus(v);
  };
  const save = () => {
    if (!status) return toast('Select a status', 'error');
    const body = { status };
    if (status === 'Rescheduled') {
      Object.assign(body, { reschedule_date: date, reschedule_time: time, reschedule_reason: reason.trim() });
      if (!date || !time) return toast('New date and time required', 'error');
    }
    legacy('hrmStatusSend', body).then(ok => { if (ok) close(); });
  };
  return (
    <>
      <h3>✏️ Update Status</h3>
      <div className="hrm-candidate-card">
        <div className="hrm-candidate-card-name">{c.name}</div>
        <div className="hrm-candidate-card-sub">{c.profile_position || ''}</div>
        <div className="hrm-candidate-card-phone">{c.phone}</div>
      </div>
      <div className="form-group">
        <label>New Status *</label>
        <SearchSelect value={status} onChange={pick} options={STATUSES} placeholder="— Select —" />
      </div>
      {status === 'Rescheduled' ? (
        <div ref={css('display: block;')}>
          <div className="form-row">
            <div className="form-group"><label>New Date *</label><input type="date" value={date} onChange={e => setDate(e.target.value)} /></div>
            <div className="form-group"><label>New Time *</label><input type="time" value={time} onChange={e => setTime(e.target.value)} /></div>
          </div>
          <div className="form-group"><label>Reason (optional)</label><textarea rows={2} placeholder="Reason for reschedule…" value={reason} onChange={e => setReason(e.target.value)} /></div>
        </div>
      ) : null}
      {status ? <div className="hrm-wa-note" ref={css('display: block; margin-top: 8px; margin-bottom: 0px;')}>ℹ️ This only updates the status. Send documents to the candidate with the 📧 Email button.</div> : null}
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>💾 Update Status</button>
      </div>
    </>
  );
}

// Joining Details (was viewJoiningDetails / hrmJoiningExtraRows): what the
// candidate sent through the joining form. d.r is that submission.
const JROW = 'display:flex;gap:12px;padding:9px 0;border-bottom:1px solid #f1f5f9';
function JRow({ label, children }) {
  return <div ref={css(JROW)}><div ref={css('width:150px;flex-shrink:0;font-size:12px;color:#64748b')}>{label}</div>
    <div ref={css('font-size:13px;color:#0f172a;font-weight:500;word-break:break-word')}>{children}</div></div>;
}
// Mobile numbers hang off the name they belong to rather than taking a row.
const withMobile = m => (m ? <span ref={css('color:#64748b;font-weight:400')}>{' · ' + m}</span> : null);
const LINK = 'color:#4f46e5;font-size:12px;font-weight:600;text-decoration:none';
const NONE = <span ref={css('color:#94a3b8;font-size:12px')}>Not uploaded</span>;
// The URLs come from the candidate's form, so only a real http(s) link gets an anchor.
function Files({ urls, name }) {
  const ok = urls.filter(u => /^https?:\/\//i.test(u || ''));
  if (!ok.length) return NONE;
  return ok.map((u, i) => <Fragment key={i}>{i ? '  ' : null}<a href={u} target="_blank" rel="noopener noreferrer" ref={css(LINK)}>{name(i, ok.length)}</a></Fragment>);
}
const sides = (i, n) => (n > 1 ? (i === 0 ? 'Front ↗' : 'Back ↗') : 'Open ↗');
const F = (...urls) => <Files urls={urls} name={sides} />;
const Head = ({ t }) => <div ref={css('font-size:11px;font-weight:700;color:#4f46e5;text-transform:uppercase;letter-spacing:.5px;margin:14px 0 4px')}>{t}</div>;

// The Fresher / Experienced part (extra_json); older submissions have none.
function JoiningExtra({ json }) {
  let x = null;
  try { x = json ? JSON.parse(json) : null; } catch (e) { x = null; }
  if (!x || !x.candidate_type) return null;
  const b = x.bank || {};
  const exp = x.total_experience || '';
  return (
    <>
      <JRow label="Candidate Type">{x.candidate_type === 'experienced' ? 'Experienced' : 'Fresher'}</JRow>
      <Head t="Bank Details" />
      <JRow label="Account Holder">{b.holder || '—'}</JRow><JRow label="Account Number">{b.account || '—'}</JRow>
      <JRow label="IFSC">{b.ifsc || '—'}</JRow><JRow label="Bank Name">{b.name || '—'}</JRow>
      {x.candidate_type === 'fresher' ? <>
        <Head t="Education" />
        <JRow label="Last Semester Marksheet">{F(x.marksheet_url)}</JRow><JRow label="Degree">{F(x.degree_url)}</JRow>
        <JRow label="Internship Certificates">{F(x.internship_url)}</JRow>
      </> : <>
        <Head t="Experience" />
        <JRow label="Total Experience">{(exp || '—') + (/^\d+(\.\d+)?$/.test(exp) ? ' years' : '')}</JRow>
        <JRow label="Last Company">{x.last_company || '—'}</JRow><JRow label="Designation">{x.designation || '—'}</JRow>
        <JRow label="Last CTC">{x.last_ctc || '—'}</JRow>
        <JRow label="Experience Letter">{F(x.experience_letter_url)}</JRow><JRow label="Relieving Letter">{F(x.relieving_letter_url)}</JRow>
        <JRow label="Salary Slips"><Files urls={x.salary_slip_urls || []} name={i => `Slip ${i + 1} ↗`} /></JRow>
      </>}
    </>
  );
}

function JoiningBody({ d, close }) {
  const { c, r } = d;
  const dob = r.dob ? new Date(r.dob).toLocaleDateString('en-IN') : '—';
  // submitted_at is stored as IST wall time, so it renders as UTC (brain.md §16).
  const at = r.submitted_at ? new Date(r.submitted_at).toLocaleString('en-IN', { timeZone: 'UTC' }) : '';
  // Relation is free text, so it goes in the label as typed.
  const guardian = (n, rel) => `Guardian ${n}${rel ? ` (${rel})` : ''}`;
  const address = [r.street, r.city, r.state, r.pincode].filter(Boolean).join(', ') || '—';
  return (
    <>
      <h3>📋 Joining Details</h3>
      <div ref={css('margin-top:12px')}>
        <div className="hrm-candidate-card">
          <div className="hrm-candidate-card-name">{c ? c.name : r.full_name}</div>
          <div className="hrm-candidate-card-sub">{(c ? (c.profile_position || '') : '') + (c && c.department ? ' · ' + c.department : '')}</div>
        </div>
        <JRow label="Name">{r.full_name || '—'}{withMobile(r.emp_mobile)}</JRow>
        <JRow label="Email">{r.email ? <a href={`mailto:${r.email}`} ref={css('color:#4f46e5;text-decoration:none')}>{r.email}</a> : '—'}</JRow>
        <JRow label={guardian(1, r.guardian1_relation)}>{r.guardian1_name || '—'}{withMobile(r.guardian1_mobile)}</JRow>
        <JRow label={guardian(2, r.guardian2_relation)}>{r.guardian2_name || '—'}{withMobile(r.guardian2_mobile)}</JRow>
        <JRow label="Date of Birth">{dob}</JRow>
        <JRow label="Address">{address}</JRow>
        <JRow label="Resume">{F(r.resume_file_url)}</JRow>
        {r.aadhaar_no ? <JRow label="Aadhaar Number">{r.aadhaar_no}</JRow> : null}
        <JRow label="Aadhaar Card">{F(r.aadhaar_file_url, r.aadhaar_file_url_2)}</JRow>
        {r.pan_no ? <JRow label="PAN Number">{r.pan_no}</JRow> : null}
        <JRow label="PAN Card">{F(r.pan_file_url, r.pan_file_url_2)}</JRow>
        <JoiningExtra json={r.extra_json} />
        {at ? <div ref={css('font-size:11px;color:#94a3b8;margin-top:10px')}>{`Submitted ${at}`}</div> : null}
      </div>
      <div className="modal-footer"><button className="btn btn-outline" onClick={close}>Close</button></div>
    </>
  );
}

// Preliminary offer letter (was viewOfferLetter): the Drive PDF once it is
// generated, else hrm.js's draft (d.draft) with a Generate button. Print reads
// the frame by its id (printOfferLetter), as the old pop-up's did.
function OfferBody({ d, close }) {
  const { c, draft } = d;
  const [busy, setBusy] = useState(false);
  const pdf = !draft;
  const driveUrl = c.offer_drive_id ? `https://drive.google.com/file/d/${c.offer_drive_id}/view` : '';
  const frame = pdf ? { src: `https://drive.google.com/file/d/${c.offer_drive_id}/preview`, allow: 'autoplay' }
    : { sandbox: 'allow-same-origin allow-scripts allow-modals', srcDoc: draft };
  return (
    <>
      <h3>📄 Offer Letter</h3>
      <div ref={css('margin-top:12px')}>
        <iframe id="offerViewFrame" data-pdf={pdf ? '1' : '0'} data-drive-url={driveUrl} {...frame}
          ref={css('width:100%;height:480px;border:1px solid #e2e8f0;border-radius:8px;display:block')} />
        {pdf ? (
          <div ref={css('margin-top:12px;padding:12px 14px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;display:flex;align-items:center;gap:10px')}>
            <span ref={css('font-size:20px')}>📂</span>
            <div ref={css('flex:1')}>
              <div ref={css('font-size:12px;font-weight:600;color:#15803d')}>Saved in Google Drive</div>
              <div ref={css('font-size:11px;color:#64748b')}>HR Offer Letters folder</div>
            </div>
            <a href={driveUrl} target="_blank" ref={css('background:#16a34a;color:#fff;padding:7px 14px;border-radius:6px;font-size:12px;font-weight:600;text-decoration:none')}>Open in Drive →</a>
          </div>
        ) : (
          <div ref={css('margin-top:12px;padding:12px 14px;background:#fefce8;border:1px solid #fde68a;border-radius:8px;display:flex;align-items:center;gap:10px')}>
            <span ref={css('font-size:18px')}>⚠️</span>
            <div ref={css('flex:1;font-size:12px;color:#92400e')}>Drive PDF hasn't been generated yet — this is a draft preview. Generate it?</div>
            <button disabled={busy} onClick={() => legacy('generateOfferDoc', c.id, setBusy)} id={`genOfferBtn-${c.id}`}
              ref={css('background:#d97706;color:#fff;padding:7px 14px;border-radius:6px;font-size:12px;font-weight:600;border:none;cursor:pointer')}>{busy ? 'Generating…' : 'Generate'}</button>
          </div>
        )}
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Close</button>
        <button className="btn btn-primary" onClick={() => legacy('printOfferLetter')}>🖨️ Print / Download PDF</button>
      </div>
    </>
  );
}

// Email to Candidate (was openEmailModal / submitEmailModal). d.type is the
// default for the candidate's stage; the send is hrmEmailSend, which for an
// offer letter closes this and opens the offer form instead.
const EMAIL_TYPES = [
  { value: 'scheduled', label: '📅 Interview Scheduled' }, { value: 'rescheduled', label: '🔄 Interview Rescheduled' },
  { value: 'selected', label: '✅ Selected' }, { value: 'rejected', label: '❌ Rejected' },
  { value: 'onboarding', label: '📋 Onboarding Form (link)' }, { value: 'preliminary', label: '📄 Preliminary Offer Letter (PDF)' },
  { value: 'offer', label: '📬 Offer Letter (PDF)' }];
const FORM_TYPES = [{ value: '', label: '— Select —' },
  { value: 'fresher', label: '🎓 Fresher — bank details + marksheet or degree' },
  { value: 'experienced', label: '💼 Experienced — bank details + last job, letters, salary slips' }];

function EmailBody({ d, close }) {
  const c = d.c;
  const [type, setType] = useState(d.type);
  // The form HR chose last time for this candidate, if any.
  const [formType, setFormType] = useState(c.joining_form_type || '');
  const [to, setTo] = useState(c.email || '');
  const [cc, setCc] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const send = () => legacy('hrmEmailSend', c.id, { type, email: to.trim(), cc: cc.trim(), formType }, { err: setErr, busy: setBusy, close });
  return (
    <>
      <h3>📧 Email to Candidate</h3>
      <div className="alert error" ref={css(err ? 'display: block;' : 'display:none')}>{err}</div>
      <div ref={css('font-size:13px;color:#475569;margin:2px 0 12px')}>{`${c.name}${c.profile_position ? ' · ' + c.profile_position : ''}`}</div>
      <div className="form-group">
        <label>What to send *</label>
        <SearchSelect value={type} onChange={setType} options={EMAIL_TYPES} />
        <div ref={css('font-size:11px;color:#94a3b8;margin-top:5px')}>
          Sending an email does not change the candidate's status — use Update Status for that.
        </div>
      </div>
      {type === 'onboarding' ? (
        <div className="form-group" ref={css('')}>
          <label>Which form *</label>
          <SearchSelect value={formType} onChange={setFormType} options={FORM_TYPES} placeholder="— Select —" />
        </div>
      ) : null}
      <div className="form-group">
        <label>To *</label>
        <input type="email" placeholder="candidate@example.com" value={to} onChange={e => setTo(e.target.value)} />
      </div>
      <div className="form-group">
        <label>CC (optional)</label>
        <input type="text" placeholder="comma-separated emails" value={cc} onChange={e => setCc(e.target.value)} />
      </div>
      <div className="modal-footer" ref={css('margin-top:14px')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" disabled={busy} onClick={send}>{busy ? 'Sending…' : '📧 Send Email'}</button>
      </div>
    </>
  );
}

const BODIES = { details: DetailsBody, status: StatusBody, joining: JoiningBody, offer: OfferBody, email: EmailBody };
const WIDTH = { joining: 'width:520px;max-width:95vw', offer: 'width:720px;max-width:95vw', email: 'width:460px' };

export default function HrmPopups() {
  const [st, setSt] = useState({});
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('hrmPopSetReact', {
      details: (c, label) => show('details', { c, label }),
      status: c => show('status', { c }),
      joining: (c, r) => show('joining', { c, r }),
      offer: (c, draft) => show('offer', { c, draft }),
      email: (c, type) => show('email', { c, type }),
    });
    return () => legacy('hrmPopSetReact', null);
  }, []);
  const closer = kind => () => setSt(s => ({ ...s, [kind]: { ...s[kind], open: false } }));
  return Object.entries(BODIES).map(([kind, Body]) => (st[kind] ? (
    <Modal key={kind} open={st[kind].open} onClose={closer(kind)} style={WIDTH[kind] || 'width:480px'}>
      <Body key={st[kind].key} d={st[kind].d} close={closer(kind)} />
    </Modal>
  ) : null));
}
