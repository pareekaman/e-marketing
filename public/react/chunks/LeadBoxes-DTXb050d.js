import{r as s,j as e}from"./vendor-DdgGs7Sa.js";import{l as i,c as l}from"./app-DYCEQstd.js";function c({v:n}){return n.stages?n.stages.map((a,r)=>e.jsxs(s.Fragment,{children:[`
    `,e.jsxs("div",{className:"funnel-row",children:[`
      `,e.jsx("div",{className:"funnel-label",children:a.label}),`
      `,e.jsxs("div",{className:"funnel-track",children:[e.jsx("div",{className:"funnel-fill",ref:l(`width:${a.width}%;background:${a.color}`)}),e.jsx("span",{className:"funnel-val",children:a.value})]}),`
    `]})]},r)):n.bars?n.bars.length?n.bars.map((a,r)=>e.jsxs(s.Fragment,{children:[`
      `,e.jsxs("div",{className:"enq-bar-row",children:[`
        `,e.jsx("div",{className:"enq-bar-label",title:a.label,children:a.label}),`
        `,e.jsx("div",{className:"enq-bar-track",children:e.jsx("div",{className:"enq-bar-fill",ref:l(`width:${a.width}%;background:${n.color}`)})}),`
        `,e.jsx("div",{className:"enq-bar-val",children:a.value}),`
      `]})]},r)):e.jsx("div",{className:"enq-bars-empty",children:"No data"}):n.legend?n.legend.map((a,r)=>e.jsxs("span",{ref:l("display:flex;align-items:center;gap:4px"),children:[e.jsx("span",{ref:l(`width:9px;height:9px;border-radius:50%;background:${a.color};display:inline-block`)}),`${a.label} (${a.count})`]},r)):n.text!==void 0?e.jsxs(e.Fragment,{children:[n.text,n.dup?e.jsxs(e.Fragment,{children:[" · ",e.jsx("span",{ref:l("color:color-mix(in srgb,var(--warning) 82%,var(--foreground))"),children:`${n.dup} duplicate${n.dup>1?"s":""}`})]}):null]}):null}function u(n){return function(){const[r,t]=s.useState(()=>i("leadBoxView",n));return s.useEffect(()=>(i("leadBoxSetReact",n,t),()=>i("leadBoxSetReact",n,null)),[]),r?e.jsx(c,{v:r}):null}}export{u as box};
