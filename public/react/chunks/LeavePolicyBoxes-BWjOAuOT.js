import{j as n,r as c}from"./vendor-DdgGs7Sa.js";import{l as a}from"./app-DYCEQstd.js";function d(e){const[l,t]=c.useState({data:null,n:0});return c.useEffect(()=>(a(e,i=>t(s=>({data:i,n:s.n+1}))),()=>a(e,null)),[]),l}const o=e=>a("lpViewSig",e.currentTarget.src);function u(){const{data:e,n:l}=d("lpNoticeSetReact");return e?n.jsxs("details",{className:"lp-notice",open:!e.signed,children:[`
    `,n.jsxs("summary",{children:[n.jsx("span",{children:"📜 "+e.title}),`
      `,e.signed?n.jsx("span",{className:"lp-ok",children:"✅ Signed on "+(e.signed_at??"")}):n.jsxs("span",{className:"lp-bad",children:["✍️ Not signed yet — ",n.jsx("a",{href:"#",onClick:t=>{t.preventDefault(),a("lpOpenSign")},children:"sign now"})]}),`
    `]}),`
    `,n.jsx(p,{points:e.points}),`
    `,e.signed?n.jsxs("div",{className:"lp-signed",children:["Signed by ",n.jsx("b",{children:e.signed_name??""})," on "+(e.signed_at??"")+`
      `,n.jsx("img",{src:e.signature,alt:"Signature",title:"Click to open full size",onClick:o})]}):null,`
  `]},l):null}function p({points:e}){return n.jsx("ol",{className:"lp-list",children:(e||[]).map((l,t)=>n.jsxs("li",{children:[n.jsx("b",{children:l.h+":"})," ",l.parts.map((i,s)=>i.b!==void 0?n.jsx("b",{children:i.b},s):i.t)]},t))})}function g(){const{data:e,n:l}=d("lpReportSetReact");if(!e)return null;const t=e.rows||[],i=t.filter(s=>s.signed_at).length;return n.jsxs("details",{className:"lp-notice",children:[`
    `,n.jsxs("summary",{children:[n.jsx("span",{children:"🖊 Policy Acknowledgements"}),n.jsx("span",{className:"lp-count",children:`${i} of ${t.length} signed`}),`
      `,n.jsx("button",{type:"button",className:"btn btn-outline lp-dl",onClick:s=>{s.preventDefault(),a("lpDownload")},children:"⬇ Download"})]}),`
    `,n.jsx("div",{className:"lp-table-wrap",children:n.jsxs("table",{className:"lp-table",children:[`
      `,n.jsx("thead",{children:n.jsxs("tr",{children:[n.jsx("th",{children:"Employee"}),n.jsx("th",{children:"Department"}),n.jsx("th",{children:"Status"}),n.jsx("th",{children:"Signed on"}),n.jsx("th",{children:"Signature"}),n.jsx("th",{})]})}),`
      `,n.jsx("tbody",{children:t.map((s,r)=>n.jsxs("tr",{children:[`
        `,n.jsx("td",{children:s.name}),n.jsx("td",{children:s.department||"-"}),`
        `,n.jsx("td",{children:s.signed_at?n.jsx("span",{className:"lp-ok",children:"Signed"}):n.jsx("span",{className:"lp-bad",children:"Not signed"})}),`
        `,n.jsx("td",{children:s.signed_at||"-"}),`
        `,n.jsx("td",{children:s.signature?n.jsx("img",{src:s.signature,alt:"Signature",title:"Click to open full size",onClick:o}):"-"}),`
        `,n.jsx("td",{children:s.signed_at?n.jsx("button",{type:"button",className:"btn btn-outline lp-dl",title:"Download this acknowledgement",onClick:()=>a("lpDownloadOne",r),children:"⬇"}):null}),`
      `]},r))}),`
    `]})}),`
  `]},l)}export{u as L,p as P,g as a};
