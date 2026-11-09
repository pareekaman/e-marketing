import{r as c,j as e}from"./vendor-DdgGs7Sa.js";import{l as a,c as i}from"./app-DYCEQstd.js";function r(){const[t,s]=c.useState("daily");return c.useEffect(()=>(a("cpTabsSetReact",s),a("initTabIndicators"),()=>a("cpTabsSetReact",null)),[]),e.jsxs("div",{className:"tab-group",ref:i("margin-bottom:18px"),children:[`
        `,e.jsx("div",{className:"tab"+(t==="daily"?" active":""),id:"cpTabDaily",onClick:()=>a("cpTab","daily"),children:"📅 Daily Fill (7-day)"}),`
        `,e.jsx("div",{className:"tab"+(t==="emp360"?" active":""),id:"cpTabEmp",onClick:()=>a("cpTab","emp360"),children:"👤 Employee 360"}),`
      `]})}function d(){return e.jsxs(e.Fragment,{children:[`
          `,e.jsx("button",{className:"dt-btn dt-btn-add",onClick:()=>a("loadCompliance"),children:"🔄 Refresh"}),`
        `]})}export{d as CpHead,r as default};
