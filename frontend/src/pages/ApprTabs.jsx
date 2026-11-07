// Approvals → the tabs (Task / Transfer / Leave / WhatsApp / Payment / MDO / Equipment
// Issues) with their count badges, drawn into #apprTabsRoot (the .appr-tabs row).
// core.js hands over through apprTabsSetReact
//   { active, show: { transfer, wa, payment, mdo, invissue }, badges: { <badge id>: count } }
// — loadApprovals shows the tabs a person may use (apprShowTab), switchApprovalTab sets
// active (and still shows the panels), setApprovalTabBadge and the payment badge set counts.
// A tab's display and a badge's display are set with the same DOM calls as before
// (style.display = 'block' / 'inline-block' / 'none'), so the attributes match exactly.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy } from '../lib/app.js';

const BADGE = bg => `display:none;background:${bg};color:#fff;font-size:10px;font-weight:700;padding:1px 7px;border-radius:10px;margin-left:4px`;
const TABS = [
  { key: 'task', id: 'apprTabTask', label: '📋 Task Approvals', badge: 'apprTaskBadge', bg: '#E74C3C' },
  { key: 'transfer', id: 'apprTabTransfer', label: '🔀 Transfer Requests', badge: 'apprTransferBadge', bg: '#3498DB', hidden: true },
  { key: 'leave', id: 'apprTabLeave', label: '🗓 Leave Approvals', badge: 'apprLeaveBadge', bg: 'var(--brand-2)' },
  { key: 'wa', id: 'apprTabWa', label: '📱 WhatsApp Tasks', badge: 'apprWaBadge', bg: '#25d366', hidden: true },
  { key: 'payment', id: 'apprTabPayment', label: '💳 Payment Approvals', badge: 'apprPaymentBadge', bg: 'var(--accent)', hidden: true },
  { key: 'mdo', id: 'apprTabMdo', label: '🧾 MDO Approvals', hidden: true },
  { key: 'invissue', id: 'apprTabInvIssue', label: '📦 Equipment Issues', badge: 'apprInvIssueBadge', bg: '#ef4444', hidden: true },
];

export default function ApprTabs() {
  const [v, setV] = useState({ active: 'task', show: {}, badges: {} });
  const els = useRef({});
  useEffect(() => {
    legacy('apprTabsSetReact', setV);
    return () => legacy('apprTabsSetReact', null);
  }, []);
  useLayoutEffect(() => {
    for (const t of TABS) {
      const tab = els.current[t.id], b = t.badge && els.current[t.badge];
      if (tab && t.hidden && v.show[t.key]) tab.style.display = 'block';
      if (b && v.badges[t.badge] != null) b.style.display = v.badges[t.badge] > 0 ? 'inline-block' : 'none';
    }
  });
  // The static style each started with in app.html, written once.
  const keep = (id, style) => el => { els.current[id] = el; if (el && style && !el._ap) { el._ap = true; el.setAttribute('style', style); } };
  return TABS.map(t => [
    '\n      ',
    <div key={t.key} className={'tab' + (v.active === t.key ? ' active' : '')} id={t.id} ref={keep(t.id, t.hidden ? 'display:none' : '')}
      onClick={() => legacy('switchApprovalTab', t.key)}>
      {t.badge ? t.label + ' ' : t.label}
      {t.badge ? <span id={t.badge} ref={keep(t.badge, BADGE(t.bg))}>{v.badges[t.badge] != null ? v.badges[t.badge] : 0}</span> : null}
    </div>,
  ]).concat(['\n    ']);
}
