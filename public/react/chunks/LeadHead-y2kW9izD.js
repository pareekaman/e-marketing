import{r as a,a as h,j as s}from"./vendor-DdgGs7Sa.js";import{l}from"./app-DYCEQstd.js";const y={enqTable:{sort:"enqSortBy",ws:[`
            `,`
              `,`
            `,`
          `],cols:[["date","Date","white-space:nowrap"],["name","Name"],["phone","Phone"],["service","Service"],["budget","Budget"],["source","Source"],["status","Status"]]},mlTable:{sort:"mlSortBy",ws:[`
                `,"","",`
              `],cols:[["date","Date","white-space:nowrap"],["name","Name"],["phone","WhatsApp"],["city","City"],["product","Product"],["platform","Platform"],["status","Status"]]},gadTable:{sort:"gadSortBy",ws:[`
              `,"","",`
            `],cols:[["date","Date","white-space:nowrap"],["name","Name"],["phone","Phone"],["email","Email"],["form","Form"],["status","Status"]]},manTable:{sort:"manSortBy",ws:[`
            `,"","",`
          `],cols:[["sr","Sr","white-space:nowrap"],["name","Name"],["company","Company"],["contact","Contact"],["phone","Phone"],["email","E-Mail"],["avg","Avg Order"],["current","Current"]]}};function u(n){const{sort:d,cols:i,ws:r}=y[n];return function(){const[o,p]=a.useState(()=>l("leadHeadView",n)||{key:"",dir:1,painted:!1}),m=a.useRef({});a.useEffect(()=>(l("leadHeadSetReact",n,e=>h.flushSync(()=>p(e))),()=>l("leadHeadSetReact",n,null)),[]),a.useLayoutEffect(()=>{o.painted&&Object.values(m.current).forEach(e=>{e&&(e.style.cursor="pointer")})});const f=a.useRef(Object.fromEntries(i.map(([e,,c])=>[e,t=>{m.current[e]=t,t&&c&&!t._lh&&(t._lh=!0,t.setAttribute("style",c))}]))).current;return s.jsxs(s.Fragment,{children:[r[0],s.jsxs("tr",{children:[r[1],i.map(([e,c])=>s.jsxs("th",{"data-k":e,ref:f[e],onClick:()=>l(d,e),children:[c,o.painted?s.jsx("span",{className:"sort-ind",children:e===o.key?o.dir>0?" ▲":" ▼":""}):null]},e)),r[2]]}),r[3]]})}}const b=u("enqTable"),g=u("mlTable"),x=u("gadTable"),H=u("manTable");export{b as EnqHead,x as GadHead,H as ManHead,g as MlHead};
