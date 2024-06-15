import{s as O,a as Q,u as Y,c as J,b as X,i as b,l as Z,j as p,d as i,e as z,f as C,F as T,g as H,h as E,N as ee}from"./index-1e1338df.js";import{g as te,d as re,M as v,s as ie,a as A,n as I,T as ae,B as oe,b as ne,F as se,c as le,e as ce,P as de,S as pe}from"./consts-2b53535e.js";import{r as x,R as ue}from"./react-432945ee.js";import{r as S}from"./createSvgIcon-8b0fe0c7.js";import{j as f}from"./reactDnd-707fca38.js";import{a as h,d as he,m as V,q as R,u as y,L as $,O as fe}from"./vendor-c662a477.js";import"./reactSelect-ab0ea613.js";import"./memoize-acaceb73.js";import"./PluginManagerFactory-7e671943.js";import"./DeleteOutlineOutlined-ef3b3274.js";const ge=["absolute","children","className","component","flexItem","light","orientation","role","textAlign","variant"],me=e=>{const{absolute:t,children:r,classes:o,flexItem:l,light:s,orientation:d,textAlign:c,variant:a}=e;return X({root:["root",t&&"absolute",a,s&&"light",d==="vertical"&&"vertical",l&&"flexItem",r&&"withChildren",r&&d==="vertical"&&"withChildrenVertical",c==="right"&&d!=="vertical"&&"textAlignRight",c==="left"&&d!=="vertical"&&"textAlignLeft"],wrapper:["wrapper",d==="vertical"&&"wrapperVertical"]},te,o)},ve=O("div",{name:"MuiDivider",slot:"Root",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.root,r.absolute&&t.absolute,t[r.variant],r.light&&t.light,r.orientation==="vertical"&&t.vertical,r.flexItem&&t.flexItem,r.children&&t.withChildren,r.children&&r.orientation==="vertical"&&t.withChildrenVertical,r.textAlign==="right"&&r.orientation!=="vertical"&&t.textAlignRight,r.textAlign==="left"&&r.orientation!=="vertical"&&t.textAlignLeft]}})(({theme:e,ownerState:t})=>h({margin:0,flexShrink:0,borderWidth:0,borderStyle:"solid",borderColor:(e.vars||e).palette.divider,borderBottomWidth:"thin"},t.absolute&&{position:"absolute",bottom:0,left:0,width:"100%"},t.light&&{borderColor:e.vars?`rgba(${e.vars.palette.dividerChannel} / 0.08)`:Q(e.palette.divider,.08)},t.variant==="inset"&&{marginLeft:72},t.variant==="middle"&&t.orientation==="horizontal"&&{marginLeft:e.spacing(2),marginRight:e.spacing(2)},t.variant==="middle"&&t.orientation==="vertical"&&{marginTop:e.spacing(1),marginBottom:e.spacing(1)},t.orientation==="vertical"&&{height:"100%",borderBottomWidth:0,borderRightWidth:"thin"},t.flexItem&&{alignSelf:"stretch",height:"auto"}),({ownerState:e})=>h({},e.children&&{display:"flex",whiteSpace:"nowrap",textAlign:"center",border:0,"&::before, &::after":{content:'""',alignSelf:"center"}}),({theme:e,ownerState:t})=>h({},t.children&&t.orientation!=="vertical"&&{"&::before, &::after":{width:"100%",borderTop:`thin solid ${(e.vars||e).palette.divider}`}}),({theme:e,ownerState:t})=>h({},t.children&&t.orientation==="vertical"&&{flexDirection:"column","&::before, &::after":{height:"100%",borderLeft:`thin solid ${(e.vars||e).palette.divider}`}}),({ownerState:e})=>h({},e.textAlign==="right"&&e.orientation!=="vertical"&&{"&::before":{width:"90%"},"&::after":{width:"10%"}},e.textAlign==="left"&&e.orientation!=="vertical"&&{"&::before":{width:"10%"},"&::after":{width:"90%"}})),xe=O("span",{name:"MuiDivider",slot:"Wrapper",overridesResolver:(e,t)=>{const{ownerState:r}=e;return[t.wrapper,r.orientation==="vertical"&&t.wrapperVertical]}})(({theme:e,ownerState:t})=>h({display:"inline-block",paddingLeft:`calc(${e.spacing(1)} * 1.2)`,paddingRight:`calc(${e.spacing(1)} * 1.2)`},t.orientation==="vertical"&&{paddingTop:`calc(${e.spacing(1)} * 1.2)`,paddingBottom:`calc(${e.spacing(1)} * 1.2)`})),B=x.forwardRef(function(t,r){const o=Y({props:t,name:"MuiDivider"}),{absolute:l=!1,children:s,className:d,component:c=s?"div":"hr",flexItem:a=!1,light:g=!1,orientation:m="horizontal",role:u=c!=="hr"?"separator":void 0,textAlign:_="center",variant:n="fullWidth"}=o,K=he(o,ge),w=h({},o,{absolute:l,component:c,flexItem:a,light:g,orientation:m,role:u,textAlign:_,variant:n}),L=me(w);return f.jsx(ve,h({as:c,className:J(L.root,d),role:u,ref:r,ownerState:w},K,{children:s?f.jsx(xe,{className:L.wrapper,ownerState:w,children:s}):null}))});B.muiSkipListHighlight=!0;const P=B;var N={},be=b;Object.defineProperty(N,"__esModule",{value:!0});var U=N.default=void 0,Se=be(S()),$e=f;U=N.default=(0,Se.default)((0,$e.jsx)("path",{d:"M19.3 16.9c.4-.7.7-1.5.7-2.4 0-2.5-2-4.5-4.5-4.5S11 12 11 14.5s2 4.5 4.5 4.5c.9 0 1.7-.3 2.4-.7l3.2 3.2 1.4-1.4zm-3.8.1c-1.4 0-2.5-1.1-2.5-2.5s1.1-2.5 2.5-2.5 2.5 1.1 2.5 2.5-1.1 2.5-2.5 2.5M12 20v2C6.48 22 2 17.52 2 12S6.48 2 12 2c4.84 0 8.87 3.44 9.8 8h-2.07c-.64-2.46-2.4-4.47-4.73-5.41V5c0 1.1-.9 2-2 2h-2v2c0 .55-.45 1-1 1H8v2h2v3H9l-4.79-4.79C4.08 10.79 4 11.38 4 12c0 4.41 3.59 8 8 8"}),"TravelExplore");var M={},_e=b;Object.defineProperty(M,"__esModule",{value:!0});var q=M.default=void 0,we=_e(S()),ye=f;q=M.default=(0,we.default)((0,ye.jsx)("path",{d:"M20.5 11H19V7c0-1.1-.9-2-2-2h-4V3.5C13 2.12 11.88 1 10.5 1S8 2.12 8 3.5V5H4c-1.1 0-1.99.9-1.99 2v3.8H3.5c1.49 0 2.7 1.21 2.7 2.7s-1.21 2.7-2.7 2.7H2V20c0 1.1.9 2 2 2h3.8v-1.5c0-1.49 1.21-2.7 2.7-2.7 1.49 0 2.7 1.21 2.7 2.7V22H17c1.1 0 2-.9 2-2v-4h1.5c1.38 0 2.5-1.12 2.5-2.5S21.88 11 20.5 11"}),"Extension");var D={},Ce=b;Object.defineProperty(D,"__esModule",{value:!0});var W=D.default=void 0,Re=Ce(S()),Ne=f;W=D.default=(0,Re.default)((0,Ne.jsx)("path",{d:"M12 5.9c1.16 0 2.1.94 2.1 2.1s-.94 2.1-2.1 2.1S9.9 9.16 9.9 8s.94-2.1 2.1-2.1m0 9c2.97 0 6.1 1.46 6.1 2.1v1.1H5.9V17c0-.64 3.13-2.1 6.1-2.1M12 4C9.79 4 8 5.79 8 8s1.79 4 4 4 4-1.79 4-4-1.79-4-4-4m0 9c-2.67 0-8 1.34-8 4v3h16v-3c0-2.66-5.33-4-8-4"}),"PersonOutlineOutlined");var k={},Me=b;Object.defineProperty(k,"__esModule",{value:!0});var F=k.default=void 0,De=Me(S()),ke=f;F=k.default=(0,De.default)((0,ke.jsx)("path",{d:"M2 20h20v-4H2zm2-3h2v2H4zM2 4v4h20V4zm4 3H4V5h2zm-4 7h20v-4H2zm2-3h2v2H4z"}),"Storage");function je(e){const{c:t}=e,r=V(),o=Z(),{hash:l}=R(),s="Link Copied To Clipboard";function d(){r(ie(!0)),setTimeout(()=>{var c;if(navigator!=null&&navigator.clipboard&&window.isSecureContext)(c=navigator==null?void 0:navigator.clipboard)==null||c.writeText(window.location.href).then(function(){const a=o.add(l,{data:{href:window.location.href},description:"From Shared URL"},10);r(A(a)),r(z({type:I.success,message:s}))},function(a){console.log("error on copy",a)});else{let a=document.createElement("textarea");return a.value=window.location.href,a.style.position="fixed",a.style.left="-999999px",a.style.top="-999999px",document.body.appendChild(a),a.focus(),a.select(),new Promise((g,m)=>{const u=o.add(l,{data:window.location.href,description:"From Shared URL"},10);r(A(u)),document.execCommand("copy")?g():m(),a.remove(),r(z({type:I.success,message:s}))})}},200)}return p(v,{onClick:d,disabled:!1,style:{fontSize:"12px"},children:[" ",i(re,{fontSize:"small",className:t}),i("span",{children:"Copy Link"})]})}var j={},Le=b;Object.defineProperty(j,"__esModule",{value:!0});var G=j.default=void 0,ze=Le(S()),Ae=f;G=j.default=(0,ze.default)((0,Ae.jsx)("path",{d:"M3 18h18v-2H3zm0-5h18v-2H3zm0-7v2h18V6z"}),"Menu");const Ie=e=>({color:`${e.contrast}`,overflow:"visible",fontSize:"12px",background:`${e.shadow}`,border:`1px solid ${e.accentNeutral}`,mt:1.5,"& .MuiAvatar-root":{width:32,height:32,ml:-.5,mr:1},"&:before":{content:'""',display:"block",position:"absolute",top:0,right:14,width:10,height:10,borderLeft:`1px solid ${e.accentNeutral}`,borderTop:`1px solid ${e.accentNeutral}`,bgcolor:`${e.shadow}`,transform:"translateY(-50%) rotate(45deg)",zIndex:0},"& .icon":{fontSize:"16px",marginRight:"4px",color:`${e.contrast}`},"& .item":{fontSize:"12px",color:`${e.contrast}`}}),Pe=e=>({display:"flex",justifyContent:"center",alignItems:"center",marginLeft:2,paddingLeft:0,cursor:"pointer",paddingRight:0,width:"30px",height:"30px",background:"none",borderRadius:"3px",color:`${e.accentNeutral}`,border:`1px solid ${e.accentNeutral}`});function Oe(){const{key:e}=R(),t=y(n=>n.showDataSourceSetting),r=y(n=>n.currentUser.role),o=V(),l=C(),[s,d]=ue.useState(null),c=x.useMemo(()=>!!s,[s]),[a,g]=x.useState(r||"superAdmin");x.useEffect(()=>{g(r)},[r]),x.useEffect(()=>{u()},[e]);const m=n=>{n.stopPropagation(),n.preventDefault(),d(()=>n.currentTarget)},u=n=>{n==null||n.stopPropagation(),n==null||n.preventDefault(),d(()=>{})},_=()=>{u(),o(ce(!0))};return p(T,{children:[i(oe,{sx:{display:"flex",alignItems:"center",textAlign:"center"},children:i(ae,{title:"Settings",children:i("button",{onClick:m,style:Pe(l),"aria-controls":c?"account-menu":void 0,"aria-haspopup":"true","aria-expanded":c?"true":void 0,children:i(G,{style:{width:"14px",height:"14px"}})})})}),p(ne,{id:"account-menu",anchorEl:s,open:c,onClose:u,onClick:m,PaperProps:{elevation:0,sx:Ie(l)},TransitionComponent:se,transformOrigin:{horizontal:"right",vertical:"top"},anchorOrigin:{horizontal:"right",vertical:"top"},children:[i(je,{c:"icon"}),i(P,{}),p(v,{onClick:_,className:"item",children:[i(le,{className:"icon"})," General Settings"]}),i(P,{}),i($,{to:"",children:p(v,{className:"item",children:[i(U,{className:"icon"}),"Search"]})}),i($,{to:"/plugins",children:p(v,{className:"item",children:[i(q,{className:"icon"}),"Plugins"]})}),i($,{to:"/users",children:p(v,{className:"item",children:[i(W,{className:"icon"}),"Users"]})}),t&&(a==="admin"||a==="superAdmin")&&i($,{to:"datasources",children:p(v,{className:"item",children:[i(F,{className:"icon"}),"Datasources"]})})]})]})}const Te=""+new URL("qryn-logo-cd0b42d9.png",import.meta.url).href,He=()=>{const r=R().pathname.split("/").map((o,l)=>o===""&&l===0?{name:"home",link:""}:{name:`/${o}`,link:o});return i(T,{children:r.map(({name:o,link:l},s)=>i("a",{href:l,className:"bread-link",children:o},s))})},Ee=e=>E("background:",e.shadow,";height:30px;padding:4px;display:flex;align-items:center;border-bottom:1px solid ",e.accentNeutral,";.logo-section{padding-top:4px;margin:0;.version{color:",e.contrast,";font-size:10px;margin-left:5px;margin-top:4px;}.path{color:",e.contrast,";margin-top:4px;margin-left:20px;flex:1;font-weight:bold;text-transform:uppercase;font-size:10px;letter-spacing:1px;.bread-link{cursor:pointer;&:hover{color:",e.primary,";}}}}",""),Ve=()=>{const e=C();return p("div",{className:H(Ee(e)),children:[p("div",{className:"logo-section",children:[i("img",{src:Te,style:{height:"20px"},alt:"Qryn View",height:"20px",className:"logo"}),i("p",{className:"version",children:"0.28.8"}),i("p",{className:"path",children:i(He,{})})]}),i(de,{section:"Status Bar",localProps:e}),i(Oe,{})]})},Be=e=>E("background:",e.background,";display:flex;flex-direction:column;height:100vh;flex:1;","");function Ze(){const e=C(),t=y(r=>r.settingsDialogOpen);return p("div",{className:H(Be(e)),children:[i(Ve,{}),i(fe,{}),i(ee,{}),i(pe,{open:t})]})}export{Be as MainAppStyles,Ze as default};