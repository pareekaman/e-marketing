import{r as e,j as t}from"./vendor-DdgGs7Sa.js";import{l as r,c as S}from"./app-DYCEQstd.js";const g=[["blue","Total Candidates","hrm-stat-total","total"],["amber","Today's Interviews","hrm-stat-today","today"],["green","Selected","hrm-stat-selected","selected"],["purple","Offers Sent","hrm-stat-offers","offers"]],u=[["green","Sent","hrm-msg-sent","sent","color:#10b981"],["red","Failed","hrm-msg-failed","failed","color:#ef4444"],["slate","Total Messages","hrm-msg-total","total","color:#64748b"]];function l(s,o,c){return function(){const[a,d]=e.useState(()=>r("hrmStatsView",s));return e.useEffect(()=>(r("hrmStatsSetReact",s,d),()=>r("hrmStatsSetReact",s,null)),[]),t.jsxs(t.Fragment,{children:[o.map(([i,h,m,f,n])=>t.jsxs(e.Fragment,{children:[`
        `,t.jsxs("div",{className:`hrm-stat-card ${i}`,children:[`
          `,t.jsx("div",{className:"hrm-stat-label",children:h}),`
          `,t.jsx("div",{id:m,className:c,ref:n?S(n):void 0,children:String(a?a[f]:0)}),`
        `]})]},m)),`
      `]})}}const j=l("stats",g,"hrm-stat-val"),b=l("msgs",u,"hrm-msg-stat-val");export{b as HrmMsgCards,j as HrmStatCards};
