// All Tasks → the buttons beside the search: 🔀 Transfer, + Assign (its label follows the
// tab), 🗑 Bulk Delete (admin) and 📄 PDF. Drawn into #tasksBtnsRoot (display:contents,
// around where they sat). Their owners hand over through tasksBtnsSetReact
//   { assign, assignLabel }   loadAllTasks (hidden on FMS / Awaiting Date, and without the permission)
//   { bulk: 'inline-flex' | 'none' }   core.js at start-up
//   { pdf }                   tasks-report.js atReportRender (hidden on FMS)
// each null until its owner has decided. Their display is set with the same DOM call
// as before, so the style attributes match exactly.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const W = '\n        ';

export default function TasksButtons() {
  const [v, setV] = useState({ assign: null, assignLabel: null, bulk: null, pdf: null });
  const assign = useRef(null), bulk = useRef(null), pdf = useRef(null);
  useEffect(() => {
    legacy('tasksBtnsSetReact', setV);
    return () => legacy('tasksBtnsSetReact', null);
  }, []);
  useLayoutEffect(() => {
    if (assign.current && v.assign != null) assign.current.style.display = v.assign ? '' : 'none';
    if (bulk.current && v.bulk != null) bulk.current.style.display = v.bulk;
    if (pdf.current && v.pdf != null) pdf.current.style.display = v.pdf ? '' : 'none';
  });
  // The static style each started with in app.html, written once.
  const start = (ref, style) => el => { ref.current = el; if (el && !el._tb) { el._tb = true; if (style) el.setAttribute('style', style); } };
  return <>
    {W}
    <button className="btn btn-outline" id="transferModeBtn" onClick={() => legacy('openNewTransferModal')}
      ref={css('color:#7c3aed;border-color:#c4b5fd')}>🔀 Transfer</button>{W}
    <button className="btn btn-primary" id="tasksAssignBtn" onClick={() => legacy('openAssignForActiveTab')}
      ref={start(assign, 'display:none')}>{v.assignLabel || '+ Assign Task'}</button>{W}
    <button className="btn btn-outline" id="bulkDeleteBtn" onClick={() => legacy('openBulkDeleteModal')}
      ref={start(bulk, 'color:#dc2626;border-color:#fecaca;display:none')}>🗑 Bulk Delete</button>{W}
    <button className="btn btn-outline" id="atPdfBtn" onClick={() => legacy('atReportPdf')} ref={start(pdf, '')}>📄 PDF</button>
    {'\n      '}
  </>;
}
