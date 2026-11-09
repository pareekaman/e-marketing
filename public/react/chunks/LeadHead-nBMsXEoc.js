import{r as a,a as h,j as s}from"./vendor-DdgGs7Sa.js";import{l as u}from"./app-DYCEQstd.js";const S={enqTable:{sort:"enqSortBy",ws:[`
            `,`
              `,`
            `,`
          `],cols:[["date","Date","white-space:nowrap"],["name","Name"],["phone","Phone"],["service","Service"],["budget","Budget"],["source","Source"],["status","Status"]]},mlTable:{sort:"mlSortBy",ws:[`
                `,"","",`
              `],cols:[["date","Date","white-space:nowrap"],["name","Name"],["phone","WhatsApp"],["city","City"],["product","Product"],["platform","Platform"],["status","Status"]]},gadTable:{sort:"gadSortBy",ws:[`
              `,"","",`
            `],cols:[["date","Date","white-space:nowrap"],["name","Name"],["phone","Phone"],["email","Email"],["form","Form"],["status","Status"]]}};function l(r){const{sort:m,cols:i,ws:n}=S[r];return function(){const[o,p]=a.useState(()=>u("leadHeadView",r)||{key:"",dir:1,painted:!1}),d=a.useRef({});a.useEffect(()=>(u("leadHeadSetReact",r,e=>h.flushSync(()=>p(e))),()=>u("leadHeadSetReact",r,null)),[]),a.useLayoutEffect(()=>{o.painted&&Object.values(d.current).forEach(e=>{e&&(e.style.cursor="pointer")})});const f=a.useRef(Object.fromEntries(i.map(([e,,c])=>[e,t=>{d.current[e]=t,t&&c&&!t._lh&&(t._lh=!0,t.setAttribute("style",c))}]))).current;return s.jsxs(s.Fragment,{children:[n[0],s.jsxs("tr",{children:[n[1],i.map(([e,c])=>s.jsxs("th",{"data-k":e,ref:f[e],onClick:()=>u(m,e),children:[c,o.painted?s.jsx("span",{className:"sort-ind",children:e===o.key?o.dir>0?" ▲":" ▼":""}):null]},e)),n[2]]}),n[3]]})}}const x=l("enqTable"),j=l("mlTable"),H=l("gadTable");export{x as EnqHead,H as GadHead,j as MlHead};
