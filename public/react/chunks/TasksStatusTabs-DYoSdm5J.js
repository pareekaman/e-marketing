import{r as i,j as t}from"./vendor-DdgGs7Sa.js";import{l as e}from"./app-DYCEQstd.js";import{u as m}from"./TabInd-C_s3CnuQ.js";const d=[["all","statusTabAll","All"],["pending","statusTabPending","Pending"],["upcoming","statusTabUpcoming","Upcoming"],["completed","statusTabCompleted","Completed"],["reasonmissing","statusTabReason","⚠ Reason missing","Delegation tasks finished after their due date with no reason given yet"]];function T(){const[n,o]=i.useState(null),s=m();i.useEffect(()=>(e("tasksStatusSetReact",o),()=>e("tasksStatusSetReact",null)),[]);const r=n?n.active:"pending";return t.jsxs(t.Fragment,{children:[`
      `,t.jsxs("div",{className:"tab-group"+s.cls,ref:s.ref,children:[s.ind,d.map(([a,l,c,u])=>[`
        `,t.jsx("div",{className:"tab"+(r===a?" active":""),id:l,title:u,onClick:()=>e("filterTaskStatus",a),children:c},a)]),`
      `]}),`
    `]})}export{T as default};
