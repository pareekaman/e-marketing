// HR Portal → the number cards: the dashboard's Total Candidates / Today's Interviews / Selected /
// Offers Sent (#hrmStatGrid) and the Email Log's Sent / Failed / Total Messages (#hrmMsgGrid).
// Both were static markup whose numbers loadHRM wrote; it now hands them over through
// hrmStatsSetReact('stats' | 'msgs', fn). Same markup and whitespace; a number shows 0 until
// the first hand-over, as the markup did.
import { Fragment, useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const STATS = [
  ['blue', 'Total Candidates', 'hrm-stat-total', 'total'],
  ['amber', "Today's Interviews", 'hrm-stat-today', 'today'],
  ['green', 'Selected', 'hrm-stat-selected', 'selected'],
  ['purple', 'Offers Sent', 'hrm-stat-offers', 'offers'],
];
const MSGS = [
  ['green', 'Sent', 'hrm-msg-sent', 'sent', 'color:#10b981'],
  ['red', 'Failed', 'hrm-msg-failed', 'failed', 'color:#ef4444'],
  ['slate', 'Total Messages', 'hrm-msg-total', 'total', 'color:#64748b'],
];

function make(key, cards, valCls) {
  return function HrmStatGrid() {
    const [v, setV] = useState(() => legacy('hrmStatsView', key));
    useEffect(() => {
      legacy('hrmStatsSetReact', key, setV);
      return () => legacy('hrmStatsSetReact', key, null);
    }, []);
    return <>
      {cards.map(([color, label, id, k, style]) => <Fragment key={id}>{'\n        '}
        <div className={`hrm-stat-card ${color}`}>{'\n          '}
          <div className="hrm-stat-label">{label}</div>{'\n          '}
          <div id={id} className={valCls} ref={style ? css(style) : undefined}>{String(v ? v[k] : 0)}</div>{'\n        '}
        </div>
      </Fragment>)}{'\n      '}
    </>;
  };
}

export const HrmStatCards = make('stats', STATS, 'hrm-stat-val');
export const HrmMsgCards = make('msgs', MSGS, 'hrm-msg-stat-val');
