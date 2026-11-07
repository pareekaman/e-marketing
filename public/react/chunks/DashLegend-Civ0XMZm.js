import{r,j as s}from"./vendor-DdgGs7Sa.js";import{l as t,c as a}from"./app-DYCEQstd.js";const d=[["Completed","#10b981"],["Pending","#ef4444"],["Revised","#f59e0b"],["Upcoming","#8b5cf6"]],l=`
          `;function x(){const[n,c]=r.useState(null);return r.useEffect(()=>(t("dashLegendSetReact",c),()=>t("dashLegendSetReact",null)),[]),n&&n.ring?(n.items||[]).map(e=>s.jsxs("span",{className:"dash-leg",children:[`
      `,s.jsx("span",{className:"dash-leg-dot",ref:a(`background:linear-gradient(135deg,${e.a},${e.b})`)}),`
      `,s.jsxs("span",{children:[s.jsx("b",{children:e.l}),s.jsx("br",{}),`${e.n} (${e.pct}%)`]})]},"r"+e.l)):s.jsxs(s.Fragment,{children:[l,d.map(([e,i],p)=>[s.jsxs("span",{ref:a("display:flex;align-items:center;gap:4px"),children:[s.jsx("span",{ref:a(`width:9px;height:9px;border-radius:50%;background:${i};display:inline-block`)}),e]},"o"+e),p<d.length-1?l:`
        `])]})}export{x as default};
