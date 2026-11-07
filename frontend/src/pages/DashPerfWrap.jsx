// Dashboard → Performance & Activity → inside each of the three chart cards
// (#dashTopChartWrap / #dashBottomChartWrap / #dashActiveChartWrap): the <canvas>
// Chart.js draws on, or a message in its place ("No data in this date range",
// "Needs at least 4 people to rank"). Was written there by renderDashPerfChart /
// renderPerfNote in public/js/dashboard.js, which now hand over through
// dashPerfWrapSetReact(key, fn): the message, or null for the canvas.
// The update is flushSync'd: dashboard.js draws on the canvas right after asking for it.
import { useEffect, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy } from '../lib/app.js';

function PerfWrap({ k, canvasId }) {
  const [note, setNote] = useState(null);
  useEffect(() => {
    legacy('dashPerfWrapSetReact', k, v => flushSync(() => setNote(v)));
    return () => legacy('dashPerfWrapSetReact', k, null);
  }, []);
  return note == null
    ? <canvas key="c" id={canvasId}></canvas>
    : <div key="n" className="perf-empty">{note}</div>;
}

export const Top = () => <PerfWrap k="top" canvasId="dashTopChart" />;
export const Bottom = () => <PerfWrap k="bottom" canvasId="dashBottomChart" />;
export const Active = () => <PerfWrap k="active" canvasId="dashActiveChart" />;
