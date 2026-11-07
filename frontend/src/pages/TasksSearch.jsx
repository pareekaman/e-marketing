// All Tasks → the search box (🔍 + "Search by name, date, desc, status..."), drawn into
// #taskSearchWrap (was its static markup in app.html). The input keeps id="taskSearch"
// and holds its own value — tasks.js filterTasks and the PDF report read it, and
// taskSearchSet (used by the top search and the Dashboard's "Open in All Tasks") writes it.
// It starts with the text taskSearchInit remembers; typing goes to taskSearchTyped
// (→ filterTasks) on the native input event, as the old oninput did.
import { useRef } from 'react';
import { legacy, css } from '../lib/app.js';

const INPUT = "padding:8px 12px 8px 32px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;width:260px";

export default function TasksSearch() {
  const setup = useRef(el => {
    if (!el || el._ts) return;
    el._ts = true;
    el.setAttribute('style', INPUT);
    el.value = legacy('taskSearchInit') || '';
    el.addEventListener('input', () => legacy('taskSearchTyped', el.value));
  }).current;
  return <>
    {'\n          '}
    <span ref={css('position:absolute;left:10px;top:50%;transform:translateY(-50%);color:#94a3b8')}>🔍</span>{'\n          '}
    <input type="text" id="taskSearch" placeholder="Search by name, date, desc, status..." ref={setup} />
    {'\n        '}
  </>;
}
