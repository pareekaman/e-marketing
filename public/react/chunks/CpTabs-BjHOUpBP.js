import{r as i,j as a}from"./vendor-DdgGs7Sa.js";import{l as e,c as r}from"./app-DYCEQstd.js";import{u as l}from"./TabInd-C_s3CnuQ.js";function m(){const[s,n]=i.useState("daily"),t=l();return i.useEffect(()=>(e("cpTabsSetReact",n),()=>e("cpTabsSetReact",null)),[]),a.jsxs("div",{className:"tab-group"+t.cls,ref:c=>{r("margin-bottom:18px")(c),t.ref(c)},children:[t.ind,`
        `,a.jsx("div",{className:"tab"+(s==="daily"?" active":""),id:"cpTabDaily",onClick:()=>e("cpTab","daily"),children:"📅 Daily Fill (7-day)"}),`
        `,a.jsx("div",{className:"tab"+(s==="emp360"?" active":""),id:"cpTabEmp",onClick:()=>e("cpTab","emp360"),children:"👤 Employee 360"}),`
      `]})}function b(){return a.jsxs(a.Fragment,{children:[`
          `,a.jsx("button",{className:"dt-btn dt-btn-add",onClick:()=>e("loadCompliance"),children:"🔄 Refresh"}),`
        `]})}export{b as CpHead,m as default};
