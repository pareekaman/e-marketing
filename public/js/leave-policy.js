// ══════════════════════════════════════════════════════
// 📜 LEAVE & WFH POLICY — signed once (backend/routes/leave-policy.js)
// Until someone has signed, opening the Leave Tracker shows the policy with a
// tick box, their name and a signature image upload (leave and WFH can still
// be applied without it). After signing, the policy stays on top
// of the Leave Tracker as a notice. Admins also get a list of who has signed.
// When this text changes, raise POLICY_VERSION in the route so everyone signs
// the new version.
// ══════════════════════════════════════════════════════
const LP_TITLE = 'Leave & Work From Home (WFH) Policy';
const LP_POINTS = [
  ['Prior Approval', 'All leave and WFH requests must be submitted through the Leave Tracker at least <b>24 hours in advance</b> and approved by the reporting manager.'],
  ['Same-Day Requests', 'Same-day WFH requests will not be approved under normal circumstances. Genuine emergencies will be considered at management\'s discretion.'],
  ['Unapproved WFH', 'If WFH is not approved, the employee must report to the office. Failure to do so will result in the day being marked as leave, subject to applicable leave rules.'],
  ['Accountability', 'Employees on approved WFH must remain available during office hours and complete their assigned tasks and deliverables.'],
  ['Compliance', 'Repeated violations of this policy may result in appropriate action as per company rules.'],
];
const LP_MAX_BYTES = 2.5 * 1024 * 1024;
let LP_STATE = null;          // { signed, signed_name, signature, signed_at }
let LP_SIGNATURE = '';        // data URL of the picked image
let _lpPromptedAt = 0;
let LP_REPORT = [];           // the admin list, kept for Download

function lpPolicyHtml(){
  return `<ol class="lp-list">${LP_POINTS.map(([h, t]) => `<li><b>${h}:</b> ${t}</li>`).join('')}</ol>`;
}

async function lpLoad(){
  const r = await api('/api/leave-policy/me');
  if (!r || r.error) return;
  LP_STATE = r;
  lpRenderNotice();
  if (ME.role === 'admin') lpLoadReport();
  // The popup on opening the Leave Tracker, not again on every refresh of it.
  if (!r.signed && !ME.impersonatedBy && Date.now() - _lpPromptedAt > 30000) lpOpenSign();
}

function lpRenderNotice(){
  const box = document.getElementById('lpNotice');
  if (!box || !LP_STATE) return;
  const s = LP_STATE;
  box.innerHTML = `<details class="lp-notice" ${s.signed ? '' : 'open'}>
    <summary><span>📜 ${LP_TITLE}</span>
      ${s.signed
        ? `<span class="lp-ok">✅ Signed on ${dtEscape(s.signed_at)}</span>`
        : `<span class="lp-bad">✍️ Not signed yet — <a href="#" onclick="event.preventDefault();lpOpenSign()">sign now</a></span>`}
    </summary>
    ${lpPolicyHtml()}
    ${s.signed ? `<div class="lp-signed">Signed by <b>${dtEscape(s.signed_name)}</b> on ${dtEscape(s.signed_at)}
      <img src="${dtEscape(s.signature)}" alt="Signature" title="Click to open full size" onclick="lpViewSig(this.src)"></div>` : ''}
  </details>`;
}

function lpOpenSign(){
  _lpPromptedAt = Date.now();
  LP_SIGNATURE = '';
  document.getElementById('lpSignText').innerHTML = lpPolicyHtml();
  document.getElementById('lpAgree').checked = false;
  document.getElementById('lpName').value = ME.name || '';
  document.getElementById('lpDate').value = new Date().toLocaleDateString('en-GB').replace(/\//g, '-');
  document.getElementById('lpFile').value = '';
  document.getElementById('lpPreview').innerHTML = '<span>No image chosen</span>';  document.getElementById('lpErr').style.display = 'none';
  document.getElementById('lpPolicyModal').classList.add('open');
}

function lpPickFile(input){
  const f = input.files && input.files[0];
  const err = document.getElementById('lpErr');
  err.style.display = 'none';
  LP_SIGNATURE = '';
  const prev = document.getElementById('lpPreview');
  prev.innerHTML = '<span>No image chosen</span>';
  if (!f) return;
  if (!['image/png', 'image/jpeg'].includes(f.type)) { input.value = ''; return lpErr('The signature must be a JPG or PNG image.'); }
  if (f.size > LP_MAX_BYTES) { input.value = ''; return lpErr('The signature image can be at most 2.5 MB.'); }
  const rd = new FileReader();
  rd.onload = () => { LP_SIGNATURE = rd.result; prev.innerHTML = `<img src="${LP_SIGNATURE}" alt="Signature preview">`; };
  rd.readAsDataURL(f);
}

function lpErr(msg){
  const err = document.getElementById('lpErr');
  err.textContent = msg;
  err.style.display = 'block';
}

async function lpSubmit(){
  if (!document.getElementById('lpAgree').checked) return lpErr('Tick the box to agree to the policy.');
  if (!LP_SIGNATURE) return lpErr('Upload an image of your signature.');
  const btn = document.getElementById('lpSubmitBtn');
  btn.disabled = true; btn.textContent = 'Saving…';
  const r = await api('/api/leave-policy/sign', 'POST', { agree: true, signature: LP_SIGNATURE });
  btn.disabled = false; btn.textContent = 'I Agree & Sign';
  if (!r || r.error) return lpErr((r && r.error) || 'Could not save. Please try again.');
  closeModal('lpPolicyModal');
  showToast('✅ Policy signed');
  lpLoad();
}

// Admin: who has signed (with the signature) and who has not.
async function lpLoadReport(){
  const box = document.getElementById('lpReport');
  if (!box) return;
  const r = await api('/api/leave-policy/report');
  if (!r || r.error || !Array.isArray(r.rows)) { box.innerHTML = ''; return; }
  LP_REPORT = r.rows;
  const signed = r.rows.filter(x => x.signed_at).length;
  box.innerHTML = `<details class="lp-notice">
    <summary><span>🖊 Policy Acknowledgements</span><span class="lp-count">${signed} of ${r.rows.length} signed</span>
      <button type="button" class="btn btn-outline lp-dl" onclick="event.preventDefault();lpDownload()">⬇ Download</button></summary>
    <div class="lp-table-wrap"><table class="lp-table">
      <thead><tr><th>Employee</th><th>Department</th><th>Status</th><th>Signed on</th><th>Signature</th><th></th></tr></thead>
      <tbody>${r.rows.map((x, i) => `<tr>
        <td>${dtEscape(x.name)}</td><td>${dtEscape(x.department || '-')}</td>
        <td>${x.signed_at ? '<span class="lp-ok">Signed</span>' : '<span class="lp-bad">Not signed</span>'}</td>
        <td>${dtEscape(x.signed_at || '-')}</td>
        <td>${x.signature ? `<img src="${dtEscape(x.signature)}" alt="Signature" title="Click to open full size" onclick="lpViewSig(this.src)">` : '-'}</td>
        <td>${x.signed_at ? `<button type="button" class="btn btn-outline lp-dl" title="Download this acknowledgement" onclick="lpDownloadOne(${i})">⬇</button>` : ''}</td>
      </tr>`).join('')}</tbody>
    </table></div>
  </details>`;
}

// A signature at full size, in a new tab.
function lpViewSig(src){
  const w = window.open('', '_blank');
  if (!w) return showToast('Allow pop-ups to open the signature', 'error');
  w.document.write(`<title>Signature</title><body style="margin:0;background:#f1f5f9;display:flex;align-items:center;justify-content:center;min-height:100vh"><img src="${src}" style="max-width:95vw;max-height:95vh;background:#fff"></body>`);
  w.document.close();
}

// Admin: every acknowledgement with its signature on one printable page; the
// browser's print dialog saves it as a PDF (Destination: Save as PDF).
function lpDownload(){
  if (!LP_REPORT.length) return showToast('Nothing to download yet', 'error');
  const w = window.open('', '_blank');
  if (!w) return showToast('Allow pop-ups to download the acknowledgements', 'error');
  const today = new Date().toLocaleDateString('en-GB').replace(/\//g, '-');
  const signed = LP_REPORT.filter(x => x.signed_at).length;
  const rows = LP_REPORT.map((x, i) => `<tr><td>${i + 1}</td><td>${dtEscape(x.name)}</td><td>${dtEscape(x.department || '-')}</td>
    <td>${x.signed_at ? 'Signed' : 'Not signed'}</td><td>${dtEscape(x.signed_at || '-')}</td>
    <td>${x.signature ? `<img src="${dtEscape(x.signature)}">` : '-'}</td></tr>`).join('');
  w.document.write(`<!doctype html><html><head><meta charset="utf-8">
    <title>Leave and WFH Policy Acknowledgements ${today}</title>
    <style>body{font-family:Arial,sans-serif;color:#111;margin:24px}h1{font-size:20px;margin:0 0 4px}
    .sub{color:#555;margin:0 0 14px;font-size:13px}ol{font-size:13px;line-height:1.5}
    table{width:100%;border-collapse:collapse;font-size:13px;margin-top:14px}
    th,td{border:1px solid #ccc;padding:6px 8px;text-align:left;vertical-align:middle}th{background:#f3f4f6}
    img{max-height:60px;max-width:200px;object-fit:contain}tr{page-break-inside:avoid}</style></head><body>
    <h1>${LP_TITLE}: Employee Acknowledgements</h1>
    <p class="sub">Downloaded ${today} · ${signed} of ${LP_REPORT.length} signed</p>
    ${lpPolicyHtml()}
    <table><thead><tr><th>#</th><th>Employee</th><th>Department</th><th>Status</th><th>Signed on</th><th>Signature</th></tr></thead>
    <tbody>${rows}</tbody></table></body></html>`);
  w.document.close();
  lpPrintSoon(w);
}

// A moment for the signature images to draw before the print dialog.
function lpPrintSoon(w){
  setTimeout(() => { try { w.focus(); w.print(); } catch (e) {} }, 800);
}

// Admin: one employee's signed acknowledgement, laid out like the policy form
// (policy, ticked box, name, signature, date), to save as a PDF.
function lpDownloadOne(i){
  const x = LP_REPORT[i];
  if (!x || !x.signed_at) return;
  const w = window.open('', '_blank');
  if (!w) return showToast('Allow pop-ups to download the acknowledgement', 'error');
  w.document.write(`<!doctype html><html><head><meta charset="utf-8">
    <title>Policy Acknowledgement - ${dtEscape(x.name)}</title>
    <style>body{font-family:Arial,sans-serif;color:#111;margin:40px;max-width:760px}
    h1{font-size:22px;margin:0 0 16px}h2{font-size:17px;margin:26px 0 12px}
    ol{font-size:14px;line-height:1.7}.ack{font-size:14px;margin:0 0 18px}
    .f{font-size:14px;margin:0 0 16px}.f b{display:inline-block;min-width:150px}
    img{max-height:110px;max-width:320px;object-fit:contain;vertical-align:middle;border-bottom:1px solid #999}</style></head><body>
    <h1>${LP_TITLE}</h1>
    ${lpPolicyHtml()}
    <h2>Employee Acknowledgement</h2>
    <p class="ack">☑ I have read, understood, and agree to comply with the above Leave &amp; WFH Policy.</p>
    <p class="f"><b>Employee Name:</b> ${dtEscape(x.signed_name || x.name)}</p>
    <p class="f"><b>Department:</b> ${dtEscape(x.department || '-')}</p>
    <p class="f"><b>Digital Signature:</b> <img src="${dtEscape(x.signature)}"></p>
    <p class="f"><b>Date:</b> ${dtEscape(x.signed_at)}</p>
    </body></html>`);
  w.document.close();
  lpPrintSoon(w);
}
