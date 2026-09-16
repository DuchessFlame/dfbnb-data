/* jsdom harness for df-bnb-plan-checklists.js
   Boots the real renderer over the real dist JSON and asserts what each page
   must show. No preview HTML anywhere — node + jsdom only. */
const fs = require("fs");
const path = require("path");
const { JSDOM } = require("jsdom");

const UP = "/mnt/user-data/uploads";
const JS = path.join(UP, "1 site-data/json/dfbnb-child/assets/df-bnb-plan-checklists.js");
const DIST = path.join(UP, "GitHub/dfbnb-data/dist");

const DATA = {
  "plan_master.json": null, "camera_mods.json": null, "scoreboard_art.json": null,
  "underarmour.json": null, "new_plans.json": null, "displays.json": null,
  "pennants.json": null,
};
for (const k of Object.keys(DATA)) DATA[k] = JSON.parse(fs.readFileSync(path.join(DIST, k), "utf8"));

const CHANNEL = process.argv[2] === "pts" ? "pts" : "live";
if (CHANNEL === "pts") {
  DATA["plan_master.json"] = JSON.parse(fs.readFileSync(path.join(DIST, "pts/plan_master.json"), "utf8"));
}

let fails = 0, checks = 0;
function ok(cond, msg) {
  checks++;
  if (!cond) { fails++; console.log("   FAIL  " + msg); }
}
function eq(a, b, msg) { ok(a === b, `${msg}  (got ${JSON.stringify(a)}, want ${JSON.stringify(b)})`); }

async function mountPage(url) {
  const dom = new JSDOM(`<!doctype html><html><body><div id="content"></div></body></html>`, {
    url: "https://buffsnbrew.com" + url, pretendToBeVisual: true, runScripts: "outside-only",
  });
  const w = dom.window;
  w.dfbnbData = { rest_url: "", rest_nonce: "", is_logged_in: "0" };
  w.fetch = async (u) => {
    const base = String(u).split("/").pop();
    if (DATA[base]) return { ok: true, status: 200, json: async () => DATA[base] };
    return { ok: false, status: 404, json: async () => ({}) };
  };
  w.matchMedia = w.matchMedia || (() => ({ matches: false, addListener() {}, removeListener() {} }));
  w.requestAnimationFrame = (cb) => setTimeout(cb, 0);
  w.eval(fs.readFileSync(JS, "utf8"));
  const api = w.__DFBNB_PLAN_SYSTEM_API;
  await api.mount(url);
  await new Promise(r => setTimeout(r, 30));
  return { w, doc: w.document, api };
}

/* ---- what every plan page must satisfy, whatever its shape ---- */
function commonChecks(doc, label, expectRows) {
  const rows = doc.querySelectorAll(".reward");
  eq(rows.length, expectRows, `${label}: row count`);

  // Cut content renders on New Plans and nowhere else.
  eq(doc.querySelectorAll(".reward--cut").length, 0, `${label}: no cut rows`);
  eq(doc.querySelectorAll(".pillCut").length, 0, `${label}: no cut pills`);

  // The count line and the progress total must agree with the rows on screen.
  const countLine = (doc.querySelector(".cmptTopCount") || {}).textContent || "";
  const m = countLine.match(/(\d+)/);
  eq(m ? Number(m[1]) : -1, expectRows, `${label}: count line`);

  const boxes = doc.querySelectorAll(".reward input.check");
  eq(boxes.length, expectRows, `${label}: one checkbox per row`);

  // Rows keep Item Image / How to Obtain / Technical with Technical last.
  let badOrder = 0;
  for (const r of rows) {
    const heads = [...r.querySelectorAll(".subExpandLabel, .penSubHead")]
      .map(h => (h.textContent || "").trim());
    const tech = heads.findIndex(h => /^Technical/i.test(h));
    if (tech >= 0 && tech !== heads.length - 1) badOrder++;
  }
  eq(badOrder, 0, `${label}: Technical is the last sub-expand on every row`);

  // Controls exist and are wired.
  for (const a of ["tick-all", "reset", "expand-all", "collapse-all"]) {
    ok(!!doc.querySelector(`.cmptBtn[data-action='${a}']`), `${label}: has ${a} button`);
  }
}

function groupCounts(doc) {
  const out = {};
  for (const g of doc.querySelectorAll(".rewardGroup")) {
    const label = (g.querySelector(".rewardGroupLabel") || {}).textContent || "";
    const pill = (g.querySelector(".pillCount") || {}).textContent || "";
    out[label] = { pill, rows: g.querySelectorAll(".reward").length };
  }
  return out;
}

/* ---- expected rows, computed from the data, not hardcoded ---- */
const master = DATA["plan_master.json"];
const live = master.items.filter(i => !i.cut);
const perPage = {};
for (const i of live) if (i.plan_page) perPage[i.plan_page] = (perPage[i.plan_page] || 0) + 1;

async function run() {
  console.log(`\n=== ${CHANNEL} channel — ${live.length} live plans ===`);

  const PAGES = [
    ["weapon", "/df/plan-checklists/weapon/", "weapon"],
    ["recipe", "/df/plan-checklists/recipe/", "recipe"],
    ["body-armour", "/df/plan-checklists/body-armour/", "body-armour"],
    ["power-armour", "/df/plan-checklists/power-armour/", "power-armour"],
    ["apparel", "/df/plan-checklists/apparel/", "apparel"],
    ["backpack-mod", "/df/plan-checklists/backpack-mod/", "backpack-mod"],
    ["camp", "/df/plan-checklists/camp/", "camp"],
    ["snow-globes", "/df/plan-checklists/snow-globes/", "snow-globes"],
    ["fishing-rod", "/df/plan-checklists/fishing-rod/", "fishing-rod"],
  ];

  const seenIds = new Map();

  for (const [name, url, slug] of PAGES) {
    const { doc } = await mountPage(url);
    const want = perPage[slug] || 0;
    console.log(` -- ${name}  (expect ${want})`);
    commonChecks(doc, name, want);

    // No plan may appear on two pages.
    for (const r of doc.querySelectorAll(".reward")) {
      const id = r.getAttribute("data-id") || r.id || "";
      if (!id) continue;
      if (seenIds.has(id)) { fails++; console.log(`   FAIL  ${id} also rendered on ${seenIds.get(id)}`); }
      else seenIds.set(id, name);
    }

    // Per-group head pills must match the rows inside them.
    let badPill = 0;
    for (const g of doc.querySelectorAll(".rewardGroup")) {
      const pill = (g.querySelector(".pillCount") || {}).textContent || "";
      const want2 = g.querySelectorAll(".reward").length;
      const got = Number((pill.split("/")[1] || "").trim());
      if (got !== want2) badPill++;
    }
    eq(badPill, 0, `${name}: every group head pill matches its rows`);
  }

  // Recipe: the two-level shape and the exact taxonomy.
  {
    const { doc } = await mountPage("/df/plan-checklists/recipe/");
    const gc = groupCounts(doc);
    const roots = Object.keys(gc);
    console.log(" -- recipe roots:", roots.join(", "));
    for (const r of ["Food", "Drinks", "Alcohol", "Serums", "Chems"]) {
      ok(roots.includes(r), `recipe: root "${r}" present`);
    }
    eq(roots[0], "Food", "recipe: Food is the first root (page order, not match order)");
    // Alcohol must not be empty — testing Drinks first is the trap that emptied it.
    ok((gc["Alcohol"] || {}).rows > 0, "recipe: Alcohol root is not empty");
    const subs = [...doc.querySelectorAll(".subExpandLabel, .penSubHead")]
      .map(h => (h.textContent || "").trim());
    ok(subs.some(s => /^Cakes & Pies/.test(s)), "recipe: Cakes & Pies sub-expand exists");
    ok(subs.some(s => /^Mutation Serums/.test(s)), "recipe: Mutation Serums sub-expand exists");
  }

  // Armour pages: weapon shape — Mods and Skins inside each set.
  for (const [name, url] of [["body-armour", "/df/plan-checklists/body-armour/"],
                             ["power-armour", "/df/plan-checklists/power-armour/"]]) {
    const { doc } = await mountPage(url);
    const groups = doc.querySelectorAll(".rewardGroup");
    ok(groups.length > 1, `${name}: more than one root expand`);
    let withSections = 0;
    for (const g of groups) {
      const heads = [...g.querySelectorAll(".subExpandLabel, .penSubHead")]
        .map(h => (h.textContent || "").trim());
      if (heads.some(h => /^Mods \(/.test(h)) && heads.some(h => /^Skins \(/.test(h))) withSections++;
    }
    eq(withSections, groups.length, `${name}: every set has Mods and Skins sub-expands`);
  }

  // Power Armour must no longer be empty, and Body Armour must have shed it.
  ok((perPage["power-armour"] || 0) > 300, "power-armour: page is populated");
  ok((perPage["body-armour"] || 0) < 400, "body-armour: power armour has left");

  // Dataset-backed pages.
  {
    const { doc } = await mountPage("/df/plan-checklists/scoreboard-art/");
    const rows = doc.querySelectorAll(".reward").length;
    const want = DATA["scoreboard_art.json"].groups
      .reduce((n, g) => n + g.items.length + g.groups.reduce((m, s) => m + s.items.length, 0), 0);
    eq(rows, want, "scoreboard-art: row count");
    const heads = [...doc.querySelectorAll(".subExpandLabel, .penSubHead")]
      .map(h => (h.textContent || "").trim());
    ok(heads.some(h => /rerun/i.test(h)), "scoreboard-art: the rerun sub-expand renders");
    ok([...doc.querySelectorAll(".rewardGroupLabel")].some(l => /^Season 26$/.test(l.textContent)),
       "scoreboard-art: a root expand per season");
  }
  {
    const { doc } = await mountPage("/df/plan-checklists/camera-mod/");
    const want = DATA["camera_mods.json"].groups.reduce((n, g) => n + g.items.length, 0);
    eq(doc.querySelectorAll(".reward").length, want, "camera-mod: row count");
  }

  // Regression: the pages this thread did not touch still render.
  for (const [name, url] of [["new-plans", "/df/plan-checklists/new-plans/"],
                             ["pennants", "/df/plan-checklists/pennants/"],
                             ["displays", "/df/plan-checklists/displays/"],
                             ["underarmour", "/df/plan-checklists/underarmour/"]]) {
    const { doc } = await mountPage(url);
    ok(!!doc.querySelector(".cmptTop"), `${name}: header card renders`);
    ok(!doc.body.innerHTML.includes("Failed to load plan data"), `${name}: no mount error`);
    console.log(` -- ${name}: ${doc.querySelectorAll(".reward").length} rows`);
  }

  // Every live plan is on exactly one page.
  const pagedIds = new Set(live.filter(i => i.plan_page).map(i => i.id));
  eq(pagedIds.size, live.filter(i => i.plan_page).length, "data: no duplicate ids");
  const homeless = live.filter(i => !i.plan_page);
  eq(homeless.length, 2, "data: only the 2 deliberate skips have no page");

  console.log(`\n${checks - fails}/${checks} checks passed on ${CHANNEL}` + (fails ? `  — ${fails} FAILED` : "  — all green"));
  return fails;
}

run().then(f => process.exit(f ? 1 : 0)).catch(e => { console.error(e); process.exit(2); });
