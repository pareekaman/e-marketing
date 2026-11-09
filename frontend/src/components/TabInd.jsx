// The sliding pill behind a .tab-group's active tab, drawn by React for React's tab groups
// (helpers.js's tabIndicators used to insert it into every group, these included; it now leaves
// a group alone when it already has a .tab-ind child). Placement is the same as there: the pill
// takes the active tab's size and offset, the first time and on resizes without the slide,
// otherwise with it, and it hides (opacity 0) while the group measures zero.
//   const ti = useTabInd();
//   <div className={'tab-group' + ti.cls} ref={el => { ...; ti.ref(el); }}>{ti.ind}{tabs}</div>
// ti.ind must be the group's first child, where the old helper put it.
import { useEffect, useLayoutEffect, useRef } from 'react';

export function useTabInd() {
  const group = useRef(null), ind = useRef(null), first = useRef(true);
  const place = useRef(animate => {
    const g = group.current, el = ind.current;
    if (!g || !el) return;
    const active = g.querySelector('.tab.active');
    const a = active && active.getBoundingClientRect();
    if (!active || !a.width) { el.style.opacity = '0'; return; }
    const gr = g.getBoundingClientRect();
    if (!animate) el.style.transition = 'none';
    el.style.opacity = '1';
    el.style.width = a.width + 'px';
    el.style.height = a.height + 'px';
    el.style.transform = 'translate(' + (a.left - gr.left) + 'px,' + (a.top - gr.top) + 'px)';
    if (!animate) { void el.offsetWidth; el.style.transition = ''; }
  }).current;
  useLayoutEffect(() => { place(!first.current); first.current = false; });
  useEffect(() => {
    const g = group.current;
    if (!g) return;
    // Tabs also change from outside React (class / style), as the old helper allowed for.
    const mo = new MutationObserver(muts => { if (muts.every(m => m.target === ind.current)) return; place(true); });
    mo.observe(g, { subtree: true, attributes: true, attributeFilter: ['class', 'style'] });
    const ro = window.ResizeObserver ? new ResizeObserver(() => place(false)) : null;
    if (ro) ro.observe(g);
    const onResize = () => place(false);
    window.addEventListener('resize', onResize);
    // A page's entrance animation (or a late web font) can leave the first measure a hair off;
    // measure again once it has settled.
    const onSettled = e => { if (e.target instanceof Element && e.target.contains(g)) place(false); };
    document.addEventListener('animationend', onSettled, true);
    document.addEventListener('transitionend', onSettled, true);
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(() => place(false));
    return () => {
      mo.disconnect(); if (ro) ro.disconnect(); window.removeEventListener('resize', onResize);
      document.removeEventListener('animationend', onSettled, true);
      document.removeEventListener('transitionend', onSettled, true);
    };
  }, []);
  return {
    ref: el => { group.current = el; },
    ind: <span className="tab-ind" ref={ind} />,
    cls: ' has-ind',
  };
}
