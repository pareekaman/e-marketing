import{r as n,j as t}from"./vendor-DdgGs7Sa.js";import{l as s}from"./app-DYCEQstd.js";const u=[["all","statusTabAll","All"],["pending","statusTabPending","Pending"],["upcoming","statusTabUpcoming","Upcoming"],["completed","statusTabCompleted","Completed"],["reasonmissing","statusTabReason","⚠ Reason missing","Delegation tasks finished after their due date with no reason given yet"]];function p(){const[e,i]=n.useState(null);n.useEffect(()=>(s("tasksStatusSetReact",i),s("initTabIndicators"),()=>s("tasksStatusSetReact",null)),[]);const o=e?e.active:"pending";return t.jsxs(t.Fragment,{children:[`
      `,t.jsxs("div",{className:"tab-group",children:[u.map(([a,l,r,c])=>[`
        `,t.jsx("div",{className:"tab"+(o===a?" active":""),id:l,title:c,onClick:()=>s("filterTaskStatus",a),children:r},a)]),`
      `]}),`
    `]})}export{p as default};
