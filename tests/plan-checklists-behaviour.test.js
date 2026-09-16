/* Behaviour pass: search, Tick All / Reset, Expand All / Close All, group boxes. */
const fs=require("fs"),path=require("path"),{JSDOM}=require("jsdom");
const UP="/mnt/user-data/uploads";
const JS=path.join(UP,"1 site-data/json/dfbnb-child/assets/df-bnb-plan-checklists.js");
const DIST=path.join(UP,"GitHub/dfbnb-data/dist");
const DATA={};
for(const k of ["plan_master.json","camera_mods.json","scoreboard_art.json","underarmour.json","new_plans.json","displays.json","pennants.json"])
  DATA[k]=JSON.parse(fs.readFileSync(path.join(DIST,k),"utf8"));
let fails=0,checks=0;
const ok=(c,m)=>{checks++;if(!c){fails++;console.log("   FAIL  "+m);}};
const eq=(a,b,m)=>ok(a===b,`${m} (got ${JSON.stringify(a)}, want ${JSON.stringify(b)})`);
async function mount(url){
  const dom=new JSDOM(`<!doctype html><html><body><div id="content"></div></body></html>`,
    {url:"https://buffsnbrew.com"+url,pretendToBeVisual:true,runScripts:"outside-only"});
  const w=dom.window;
  w.dfbnbData={rest_url:"",rest_nonce:"",is_logged_in:"0"};
  w.fetch=async u=>{const b=String(u).split("/").pop();
    return DATA[b]?{ok:true,status:200,json:async()=>DATA[b]}:{ok:false,status:404,json:async()=>({})};};
  w.requestAnimationFrame=cb=>setTimeout(cb,0);
  w.eval(fs.readFileSync(JS,"utf8"));
  const api=w.__DFBNB_PLAN_SYSTEM_API;
  await api.mount(url); await new Promise(r=>setTimeout(r,30));
  return {w,doc:w.document,api};
}
const click=(w,n)=>{const e=new w.MouseEvent("click",{bubbles:true});n.dispatchEvent(e);};
const visible=doc=>[...doc.querySelectorAll(".reward")].filter(r=>!r.classList.contains("is-hidden")&&r.style.display!=="none").length;

(async()=>{
for(const [name,url,term] of [
  ["weapon","/df/plan-checklists/weapon/","Gauss Shotgun"],
  ["recipe","/df/plan-checklists/recipe/","Tea"],
  ["power-armour","/df/plan-checklists/power-armour/","T-60"],
  ["body-armour","/df/plan-checklists/body-armour/","Combat"],
  ["camp","/df/plan-checklists/camp/","Pumpkin"],
]){
  const {w,doc,api}=await mount(url);
  const total=doc.querySelectorAll(".reward").length;
  console.log(` -- ${name}: ${total} rows`);

  // Expand All / Close All act on the root level.
  click(w,doc.querySelector(".cmptBtn[data-action='expand-all']"));
  const opened=doc.querySelectorAll(".rewardGroupBody.is-open").length;
  ok(opened>0,`${name}: Expand All opens the group bodies`);
  click(w,doc.querySelector(".cmptBtn[data-action='collapse-all']"));
  eq(doc.querySelectorAll(".rewardGroupBody.is-open").length,0,`${name}: Close All closes them`);

  // Tick All / Reset move every leaf box and the group heads follow.
  click(w,doc.querySelector(".cmptBtn[data-action='tick-all']"));
  await new Promise(r=>setTimeout(r,20));
  const tickedAll=[...doc.querySelectorAll(".reward input.check")].filter(c=>c.checked).length;
  eq(tickedAll,total,`${name}: Tick All ticks every row`);
  const heads=[...doc.querySelectorAll(".rewardGroupHead > input.check")];
  eq(heads.filter(c=>c.checked).length,heads.length,`${name}: every group head follows Tick All`);
  click(w,doc.querySelector(".cmptBtn[data-action='reset']"));
  await new Promise(r=>setTimeout(r,20));
  eq([...doc.querySelectorAll(".reward input.check")].filter(c=>c.checked).length,0,`${name}: Reset clears every row`);

  // A group tick-box ticks only its own rows.
  const g=doc.querySelector(".rewardGroup");
  if(g){
    const box=g.querySelector(".rewardGroupHead > input.check");
    const mine=g.querySelectorAll(".reward input.check").length;
    box.checked=true; box.dispatchEvent(new w.Event("change",{bubbles:true}));
    await new Promise(r=>setTimeout(r,30));
    eq([...doc.querySelectorAll(".reward input.check")].filter(c=>c.checked).length,mine,
       `${name}: a group box ticks its own rows and no others`);
  }

  // Style-B search narrows, and clearing it restores.
  const box2=doc.querySelector(".cmptTopSearch");
  box2.value=term; box2.dispatchEvent(new w.Event("input",{bubbles:true}));
  await new Promise(r=>setTimeout(r,40));
  const narrowed=visible(doc);
  ok(narrowed>0&&narrowed<total,`${name}: search "${term}" narrows to ${narrowed} of ${total}`);
  const openedSubs=doc.querySelectorAll(".subExpand.is-open").length;
  ok(openedSubs>0||name==="camp",`${name}: search opens the sub-expand holding the match`);
  box2.value=""; box2.dispatchEvent(new w.Event("input",{bubbles:true}));
  await new Promise(r=>setTimeout(r,40));
  eq(visible(doc),total,`${name}: clearing search restores every row`);
}
console.log(`\n${checks-fails}/${checks} behaviour checks passed`+(fails?`  — ${fails} FAILED`:"  — all green"));
process.exit(fails?1:0);
})().catch(e=>{console.error(e);process.exit(2);});
