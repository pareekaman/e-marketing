import{r as a,j as e}from"./vendor-DdgGs7Sa.js";import{l as s,c as n}from"./app-DYCEQstd.js";const o="padding:8px 12px 8px 32px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;width:260px";function c(){const r=a.useRef(t=>{!t||t._ts||(t._ts=!0,t.setAttribute("style",o),t.value=s("taskSearchInit")||"",t.addEventListener("input",()=>s("taskSearchTyped",t.value)))}).current;return e.jsxs(e.Fragment,{children:[`
          `,e.jsx("span",{ref:n("position:absolute;left:10px;top:50%;transform:translateY(-50%);color:#94a3b8"),children:"🔍"}),`
          `,e.jsx("input",{type:"text",id:"taskSearch",placeholder:"Search by name, date, desc, status...",ref:r}),`
        `]})}export{c as default};
