"use strict";(self.webpackChunkqryn_view=self.webpackChunkqryn_view||[]).push([[14],{25687:function(e,t,n){n.r(t),n.d(t,{MainAppStyles:function(){return ae},default:function(){return le}});var r=n(30168),i=n(31238),o=n(62975),a=n(47313),l=n(58467),c=n(99811),s=n(29439),d=n(87462),u=n(63366),h=n(83061),v=n(20564),p=n(63649),m=n(39028),x=n(86728),f=n(46417),g=["className","component"];var b=n(41271),Z=function(){var e=arguments.length>0&&void 0!==arguments[0]?arguments[0]:{},t=e.defaultTheme,n=e.defaultClassName,r=void 0===n?"MuiBox-root":n,i=e.generateClassName,o=(0,v.ZP)("div",{shouldForwardProp:function(e){return"theme"!==e&&"sx"!==e&&"as"!==e}})(p.Z);return a.forwardRef((function(e,n){var a=(0,x.Z)(t),l=(0,m.Z)(e),c=l.className,s=l.component,v=void 0===s?"div":s,p=(0,u.Z)(l,g);return(0,f.jsx)(o,(0,d.Z)({as:v,ref:n,className:(0,h.Z)(c,i?i(r):r),theme:a},p))}))}({defaultTheme:(0,n(68658).Z)(),defaultClassName:"MuiBox-root",generateClassName:b.Z.generate}),w=Z,j=n(24064),C=n(95765),S=n(21921),N=n(17551),z=n(88564),y=n(29394),A=n(99273),k=["absolute","children","className","component","flexItem","light","orientation","role","textAlign","variant"],H=(0,z.ZP)("div",{name:"MuiDivider",slot:"Root",overridesResolver:function(e,t){var n=e.ownerState;return[t.root,n.absolute&&t.absolute,t[n.variant],n.light&&t.light,"vertical"===n.orientation&&t.vertical,n.flexItem&&t.flexItem,n.children&&t.withChildren,n.children&&"vertical"===n.orientation&&t.withChildrenVertical,"right"===n.textAlign&&"vertical"!==n.orientation&&t.textAlignRight,"left"===n.textAlign&&"vertical"!==n.orientation&&t.textAlignLeft]}})((function(e){var t=e.theme,n=e.ownerState;return(0,d.Z)({margin:0,flexShrink:0,borderWidth:0,borderStyle:"solid",borderColor:(t.vars||t).palette.divider,borderBottomWidth:"thin"},n.absolute&&{position:"absolute",bottom:0,left:0,width:"100%"},n.light&&{borderColor:t.vars?"rgba(".concat(t.vars.palette.dividerChannel," / 0.08)"):(0,N.Fq)(t.palette.divider,.08)},"inset"===n.variant&&{marginLeft:72},"middle"===n.variant&&"horizontal"===n.orientation&&{marginLeft:t.spacing(2),marginRight:t.spacing(2)},"middle"===n.variant&&"vertical"===n.orientation&&{marginTop:t.spacing(1),marginBottom:t.spacing(1)},"vertical"===n.orientation&&{height:"100%",borderBottomWidth:0,borderRightWidth:"thin"},n.flexItem&&{alignSelf:"stretch",height:"auto"})}),(function(e){var t=e.theme,n=e.ownerState;return(0,d.Z)({},n.children&&{display:"flex",whiteSpace:"nowrap",textAlign:"center",border:0,"&::before, &::after":{position:"relative",width:"100%",borderTop:"thin solid ".concat((t.vars||t).palette.divider),top:"50%",content:'""',transform:"translateY(50%)"}})}),(function(e){var t=e.theme,n=e.ownerState;return(0,d.Z)({},n.children&&"vertical"===n.orientation&&{flexDirection:"column","&::before, &::after":{height:"100%",top:"0%",left:"50%",borderTop:0,borderLeft:"thin solid ".concat((t.vars||t).palette.divider),transform:"translateX(0%)"}})}),(function(e){var t=e.ownerState;return(0,d.Z)({},"right"===t.textAlign&&"vertical"!==t.orientation&&{"&::before":{width:"90%"},"&::after":{width:"10%"}},"left"===t.textAlign&&"vertical"!==t.orientation&&{"&::before":{width:"10%"},"&::after":{width:"90%"}})})),V=(0,z.ZP)("span",{name:"MuiDivider",slot:"Wrapper",overridesResolver:function(e,t){var n=e.ownerState;return[t.wrapper,"vertical"===n.orientation&&t.wrapperVertical]}})((function(e){var t=e.theme,n=e.ownerState;return(0,d.Z)({display:"inline-block",paddingLeft:"calc(".concat(t.spacing(1)," * 1.2)"),paddingRight:"calc(".concat(t.spacing(1)," * 1.2)")},"vertical"===n.orientation&&{paddingTop:"calc(".concat(t.spacing(1)," * 1.2)"),paddingBottom:"calc(".concat(t.spacing(1)," * 1.2)")})})),B=a.forwardRef((function(e,t){var n=(0,y.Z)({props:e,name:"MuiDivider"}),r=n.absolute,i=void 0!==r&&r,o=n.children,a=n.className,l=n.component,c=void 0===l?o?"div":"hr":l,s=n.flexItem,v=void 0!==s&&s,p=n.light,m=void 0!==p&&p,x=n.orientation,g=void 0===x?"horizontal":x,b=n.role,Z=void 0===b?"hr"!==c?"separator":void 0:b,w=n.textAlign,j=void 0===w?"center":w,C=n.variant,N=void 0===C?"fullWidth":C,z=(0,u.Z)(n,k),B=(0,d.Z)({},n,{absolute:i,component:c,flexItem:v,light:m,orientation:g,role:Z,textAlign:j,variant:N}),M=function(e){var t=e.absolute,n=e.children,r=e.classes,i=e.flexItem,o=e.light,a=e.orientation,l=e.textAlign,c={root:["root",t&&"absolute",e.variant,o&&"light","vertical"===a&&"vertical",i&&"flexItem",n&&"withChildren",n&&"vertical"===a&&"withChildrenVertical","right"===l&&"vertical"!==a&&"textAlignRight","left"===l&&"vertical"!==a&&"textAlignLeft"],wrapper:["wrapper","vertical"===a&&"wrapperVertical"]};return(0,S.Z)(c,A.V,r)}(B);return(0,f.jsx)(H,(0,d.Z)({as:c,className:(0,h.Z)(M.root,a),role:Z,ref:t,ownerState:B},z,{children:o?(0,f.jsx)(V,{className:M.wrapper,ownerState:B,children:o}):null}))})),M=n(47131),P=n(78899),R=n(38167),T=n(57585),I=n(88435),L=n(56352),U=n(79861),D=n(2135),F=n(76854),E=n(13002),W=n(52586),O=n(87975),q=n(8454),Y=n(30735),_=n(62424);function G(e){var t=e.c,n=(0,L.I0)(),r=(0,q.Z)(),i="Link Copied To Clipboard";return(0,f.jsxs)(C.Z,{onClick:function(){n((0,O.E9)(!0)),setTimeout((function(){var e,t,o;if(null===(e=navigator)||void 0===e||!e.clipboard||!window.isSecureContext){var a=document.createElement("textarea");return a.value=window.location.href,a.style.position="fixed",a.style.left="-999999px",a.style.top="-999999px",document.body.appendChild(a),a.focus(),a.select(),new Promise((function(e,t){var o=r.add({data:window.location.href,description:"From Shared URL"},10);n((0,Y.Z)(o)),document.execCommand("copy")?e():t(),a.remove(),n((0,O.el)({type:_.m.success,message:i}))}))}null===(t=navigator)||void 0===t||null===(o=t.clipboard)||void 0===o||o.writeText(window.location.href).then((function(){var e=r.add({data:window.location.href,description:"From Shared URL"},10);n((0,Y.Z)(e)),n((0,O.el)({type:_.m.success,message:i}))}),(function(e){console.log("error on copy",e)}))}),200)},disabled:!1,style:{fontSize:"12px"},children:[" ",(0,f.jsx)(W.Z,{fontSize:"small",className:t}),(0,f.jsx)("span",{children:"Copy Link"})]})}var Q,X=n(29826),J=(n(41801).Z.button(Q||(Q=(0,r.Z)(["\n    border: none;\n    background: ",";\n    border: 1px solid ",";\n    color: ",";\n    padding: 3px 12px;\n    border-radius: 3px;\n    font-size: 12px;\n    cursor: pointer;\n    user-select: none;\n    line-height: 20px;\n    display: flex;\n    align-items: center;\n    margin-left: 10px;\n    height: 26px;\n"])),(function(e){return e.theme.buttonDefault}),(function(e){return e.theme.buttonBorder}),(function(e){return e.theme.textColor})),function(e){return{color:"".concat(e.textColor),overflow:"visible",fontSize:"12px",background:"".concat(e.widgetContainer),border:"1px solid ".concat(e.buttonBorder),mt:1.5,"& .MuiAvatar-root":{width:32,height:32,ml:-.5,mr:1},"&:before":{content:'""',display:"block",position:"absolute",top:0,right:14,width:10,height:10,borderLeft:"1px solid ".concat(e.buttonBorder),borderTop:"1px solid ".concat(e.buttonBorder),bgcolor:"".concat(e.widgetContainer),transform:"translateY(-50%) rotate(45deg)",zIndex:0},"& .icon":{fontSize:"16px",marginRight:"4px",color:"".concat(e.textColor)},"& .item":{fontSize:"12px",color:"".concat(e.textColor)}}});function K(){var e=(0,L.v9)((function(e){return e.showDataSourceSetting})),t=(0,L.v9)((function(e){return e.currentUser})),n=(0,L.v9)((function(e){return e.currentUser.role})),r=(0,L.I0)(),o=(0,i.F)(),l=a.useState(null),c=(0,s.Z)(l,2),d=c[0],u=c[1],h=Boolean(d),v=(0,a.useState)(n||"superAdmin"),p=(0,s.Z)(v,2),m=p[0],x=p[1];(0,a.useEffect)((function(){x(n)}),[n]);var g=function(){u(null)};return(0,f.jsxs)(f.Fragment,{children:[(0,f.jsx)(w,{sx:{display:"flex",alignItems:"center",textAlign:"center"},children:(0,f.jsx)(P.Z,{title:"Settings",children:(0,f.jsx)(M.Z,{onClick:function(e){u(e.currentTarget)},size:"small",sx:{ml:2,color:"".concat(o.textColor)},"aria-controls":h?"account-menu":void 0,"aria-haspopup":"true","aria-expanded":h?"true":void 0,children:(0,f.jsx)(X.ZP,{name:t.name,size:"30px",round:"3px"})})})}),(0,f.jsxs)(j.Z,{anchorEl:d,id:"account-menu",open:h,onClose:g,onClick:g,PaperProps:{elevation:0,sx:J(o)},transformOrigin:{horizontal:"right",vertical:"top"},anchorOrigin:{horizontal:"right",vertical:"bottom"},children:[(0,f.jsx)(G,{c:"icon"}),(0,f.jsx)(B,{}),(0,f.jsxs)(C.Z,{onClick:function(){r((0,U.Z)(!0)),g()},className:"item",children:[(0,f.jsx)(R.Z,{className:"icon"})," General Settings"]}),(0,f.jsx)(B,{}),(0,f.jsx)(D.rU,{to:"/",children:(0,f.jsxs)(C.Z,{className:"item",children:[(0,f.jsx)(T.Z,{className:"icon"}),"Search"]})}),(0,f.jsx)(D.rU,{to:"/plugins",children:(0,f.jsxs)(C.Z,{className:"item",children:[(0,f.jsx)(I.Z,{className:"icon"}),"Plugins"]})}),(0,f.jsx)(D.rU,{to:"/users",children:(0,f.jsxs)(C.Z,{className:"item",children:[(0,f.jsx)(F.Z,{className:"icon"}),"Users"]})}),e&&("admin"===m||"superAdmin"===m)&&(0,f.jsx)(D.rU,{to:"datasources",children:(0,f.jsxs)(C.Z,{className:"item",children:[(0,f.jsx)(E.Z,{className:"icon"}),"Datasources"]})})]})]})}var $,ee,te=n.p+"static/media/qryn-logo.26a5a5a8cc98abd3f4b1.png",ne=function(e){return(0,o.iv)($||($=(0,r.Z)(["\n    background: ",";\n    height: 30px;\n    padding: 4px;\n    display: flex;\n    align-items: center;\n    border-bottom:1px solid ",";\n    .logo-section {\n        margin: 0;\n        .version {\n            color: ",";\n            font-size: 10px;\n            margin-left: 5px;\n        }\n        .path {\n            color:",";\n            margin-left:20px;\n            flex:1;\n            font-weight:bold;\n            text-transform:uppercase;\n            font-size:11px;\n            letter-spacing:1px;\n        }\n    }\n"])),e.widgetContainer,e.buttonBorder,e.textColor,e.textColor)},re=function(){var e,t=(0,i.F)(),n=(0,l.TH)();return(0,f.jsxs)("div",{className:(0,o.cx)(ne(t)),children:[(0,f.jsxs)("div",{className:"logo-section",children:[(0,f.jsx)("img",{src:te,alt:"Qryn View",height:"24px",className:"logo"}),(0,f.jsx)("p",{className:"version",children:"0.25.6"}),(0,f.jsxs)("p",{className:"path",children:[" ","/"," ",(e=n.pathname,e.replace(/\//,""))]})]}),(0,f.jsx)(c.Z,{section:"Status Bar",localProps:t}),(0,f.jsx)(K,{})]})},ie=n(25146),oe=n(79727),ae=function(e){return(0,o.iv)(ee||(ee=(0,r.Z)(["\n    background: ",";\n    display:flex;\n    flex-direction:column;\n    height:100vh;\n    flex:1;\n\n"])),e.mainBgColor)};function le(){var e=(0,i.F)(),t=(0,L.v9)((function(e){return e.settingsDialogOpen}));return(0,f.jsxs)("div",{className:(0,o.cx)(ae(e)),children:[(0,f.jsx)(re,{}),(0,f.jsx)(l.j3,{}),(0,f.jsx)(ie.P,{}),(0,f.jsx)(oe.ZP,{open:t})]})}},88435:function(e,t,n){var r=n(64836);t.Z=void 0;var i=r(n(45045)),o=n(46417),a=(0,i.default)((0,o.jsx)("path",{d:"M20.5 11H19V7c0-1.1-.9-2-2-2h-4V3.5C13 2.12 11.88 1 10.5 1S8 2.12 8 3.5V5H4c-1.1 0-1.99.9-1.99 2v3.8H3.5c1.49 0 2.7 1.21 2.7 2.7s-1.21 2.7-2.7 2.7H2V20c0 1.1.9 2 2 2h3.8v-1.5c0-1.49 1.21-2.7 2.7-2.7 1.49 0 2.7 1.21 2.7 2.7V22H17c1.1 0 2-.9 2-2v-4h1.5c1.38 0 2.5-1.12 2.5-2.5S21.88 11 20.5 11z"}),"Extension");t.Z=a},76854:function(e,t,n){var r=n(64836);t.Z=void 0;var i=r(n(45045)),o=n(46417),a=(0,i.default)((0,o.jsx)("path",{d:"M12 5.9c1.16 0 2.1.94 2.1 2.1s-.94 2.1-2.1 2.1S9.9 9.16 9.9 8s.94-2.1 2.1-2.1m0 9c2.97 0 6.1 1.46 6.1 2.1v1.1H5.9V17c0-.64 3.13-2.1 6.1-2.1M12 4C9.79 4 8 5.79 8 8s1.79 4 4 4 4-1.79 4-4-1.79-4-4-4zm0 9c-2.67 0-8 1.34-8 4v3h16v-3c0-2.66-5.33-4-8-4z"}),"PersonOutlineOutlined");t.Z=a},13002:function(e,t,n){var r=n(64836);t.Z=void 0;var i=r(n(45045)),o=n(46417),a=(0,i.default)((0,o.jsx)("path",{d:"M2 20h20v-4H2v4zm2-3h2v2H4v-2zM2 4v4h20V4H2zm4 3H4V5h2v2zm-4 7h20v-4H2v4zm2-3h2v2H4v-2z"}),"Storage");t.Z=a},57585:function(e,t,n){var r=n(64836);t.Z=void 0;var i=r(n(45045)),o=n(46417),a=(0,i.default)((0,o.jsx)("path",{d:"M19.3 16.9c.4-.7.7-1.5.7-2.4 0-2.5-2-4.5-4.5-4.5S11 12 11 14.5s2 4.5 4.5 4.5c.9 0 1.7-.3 2.4-.7l3.2 3.2 1.4-1.4-3.2-3.2zm-3.8.1c-1.4 0-2.5-1.1-2.5-2.5s1.1-2.5 2.5-2.5 2.5 1.1 2.5 2.5-1.1 2.5-2.5 2.5zM12 20v2C6.48 22 2 17.52 2 12S6.48 2 12 2c4.84 0 8.87 3.44 9.8 8h-2.07c-.64-2.46-2.4-4.47-4.73-5.41V5c0 1.1-.9 2-2 2h-2v2c0 .55-.45 1-1 1H8v2h2v3H9l-4.79-4.79C4.08 10.79 4 11.38 4 12c0 4.41 3.59 8 8 8z"}),"TravelExplore");t.Z=a}}]);