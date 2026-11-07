import{r as c,j as e}from"./vendor-DdgGs7Sa.js";import{l}from"./app-DYCEQstd.js";function d({v:s}){const t=s.givers.length;return e.jsxs("button",{type:"button",className:"at-rep-head",onClick:()=>l("atReportToggle","open"),children:[`
      `,e.jsx("span",{className:"at-rep-caret",children:s.open?"▾":"▸"}),`
      `,e.jsx("b",{children:`Who gave these ${s.total} task${s.total===1?"":"s"}`}),`
      `,e.jsx("span",{className:"at-rep-sub",children:`${t} ${t===1?"person":"people"} · click to ${s.open?"hide":"see"}`}),`
    `]})}function i({v:s}){const t=s.givers.reduce((n,a)=>n+a.done,0);return e.jsxs("table",{className:"at-rep-t",children:[e.jsx("thead",{children:e.jsxs("tr",{children:[e.jsx("th",{children:"Given by"}),e.jsx("th",{className:"n",children:"Total"}),e.jsx("th",{className:"n",children:"Done"}),e.jsx("th",{className:"n",children:"Pending"})]})}),`
      `,e.jsxs("tbody",{children:[s.givers.map((n,a)=>e.jsxs("tr",{children:[e.jsx("td",{children:n.name}),e.jsx("td",{className:"n",children:n.total}),e.jsx("td",{className:"n",children:n.done}),e.jsx("td",{className:"n",children:n.total-n.done})]},a)),e.jsxs("tr",{className:"tot",children:[e.jsx("td",{children:"Total"}),e.jsx("td",{className:"n",children:s.total}),e.jsx("td",{className:"n",children:t}),e.jsx("td",{className:"n",children:s.givers.reduce((n,a)=>n+a.total-a.done,0)})]})]})]})}function h({v:s}){return e.jsxs("table",{className:"at-rep-t",children:[e.jsx("thead",{children:e.jsxs("tr",{children:[e.jsx("th",{children:"Day given"}),s.givers.map((t,n)=>e.jsx("th",{className:"n",children:t.head},n)),e.jsx("th",{className:"n",children:"Total"})]})}),e.jsx("tbody",{children:s.dayRows.map((t,n)=>e.jsxs("tr",{children:[e.jsx("td",{children:t.day}),t.cells.map((a,r)=>e.jsx("td",{className:"n",children:a||null},r)),e.jsx("td",{className:"n",children:e.jsx("b",{children:t.cells.reduce((a,r)=>a+r,0)})})]},n))})]})}function j(){const[s,t]=c.useState(null);return c.useEffect(()=>(l("atReportSetReact",t),()=>l("atReportSetReact",null)),[]),s?s.open?e.jsxs(e.Fragment,{children:[`
    `,e.jsxs("div",{className:"at-rep",children:[`
      `,e.jsx(d,{v:s}),`
      `,e.jsx("div",{className:"at-rep-col",children:e.jsx(i,{v:s})}),`
      `,e.jsxs("div",{className:"at-rep-col",children:[`
        `,e.jsx("button",{type:"button",className:"tg-mini-btn",onClick:()=>l("atReportToggle","days"),children:`
          ${s.days?"▾ Hide":"▸ Show"} day by day (date given)`}),s.days?e.jsxs(e.Fragment,{children:[`
        `,e.jsx("div",{className:"at-rep-days",children:e.jsx(h,{v:s})}),`
      `]}):`
        
      `]}),`
    `]})]}):e.jsx("div",{className:"at-rep at-rep-closed",children:e.jsx(d,{v:s})}):null}export{j as default};
