// MIS Report → the 📅 Set Plan button in the page header, drawn into #misHeadRoot
// (was its static markup in app.html). core.js shows it at start-up for anyone with
// set_plan (window._misSetPlanShow, and window.MIS_SETPLAN_REACT after that); its display
// is set with the same style.display call as before. A click opens openSetPlanModal.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy } from '../lib/app.js';

export default function MisSetPlan() {
  const [show, setShow] = useState(() => !!window._misSetPlanShow);
  const btn = useRef(null);
  useEffect(() => {
    window.MIS_SETPLAN_REACT = setShow;
    return () => { window.MIS_SETPLAN_REACT = null; };
  }, []);
  useLayoutEffect(() => { if (btn.current && show) btn.current.style.display = 'inline-flex'; });
  return <>
    {'\n      '}
    <button id="setPlanBtn" className="btn btn-primary" onClick={() => legacy('openSetPlanModal')}
      ref={el => { btn.current = el; if (el && !el._sp) { el._sp = true; el.setAttribute('style', 'display:none'); } }}>📅 Set Plan</button>
    {'\n    '}
  </>;
}
