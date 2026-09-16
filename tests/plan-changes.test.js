/* Change-tracking pass: the ↻ Changed pill, the "Changed since the last build"
   block in Technical, and the New tag on a newly-added source line.

   Rows are SYNTHESISED here rather than fished out of dist/. A real change is
   by definition absent from a freshly snapshotted build, so a test that waited
   for one would pass vacuously for three months and then fail in the patch
   where it finally mattered. These assert the render contract: given a row
   carrying `changes`, what has to appear on the page. */
const fs = require("fs"), path = require("path"), { JSDOM } = require("jsdom");
const UP = process.env.DFBNB_UP || "/mnt/user-data/uploads";
const JS = path.join(UP, "1 site-data/json/dfbnb-child/assets/df-bnb-plan-checklists.js");
const DIST = path.join(UP, "GitHub/dfbnb-data/dist");

const master = JSON.parse(fs.readFileSync(path.join(DIST, "plan_master.json"), "utf8"));
let fails = 0, checks = 0;
const ok = (c, m) => { checks++; if (!c) { fails++; console.log("   FAIL  " + m); } };

/* Take three real weapon-page rows and give them changes, so everything else
   about the row — page routing, grouping, the store key — stays real. */
function fixture() {
  const doc = JSON.parse(JSON.stringify(master));
  const rows = doc.items.filter(i => i.plan_page === "weapon").slice(0, 3);
  rows[0].changes = [{ field: "tradeable", from: false, to: true,
                       text: "Now tradeable — it could not be traded or dropped before." }];
  rows[1].changes = [{ field: "routes", kind: "added", route: "Grahm's Meat Cook",
                       bucket: "Events & Activities",
                       text: "Now also comes from Grahm's Meat Cook (Events & Activities)." }];
  rows[1].obtain_routes = [{ route: "Grahm's Meat Cook", source_type: "event-quest",
                             rate: 0.05, rate_display: "5%", new: true }];
  rows[1].obtain_ledger = [{ label: "Events & Activities", routes: [0] }];
  rows[2].changes = [];
  return { doc, ids: rows.map(r => r.id) };
}

async function mount(url, data) {
  const dom = new JSDOM(`<!doctype html><html><body><div id="content"></div></body></html>`,
    { url: "https://buffsnbrew.com" + url, pretendToBeVisual: true, runScripts: "outside-only" });
  const w = dom.window;
  w.dfbnbData = { rest_url: "", rest_nonce: "", is_logged_in: "0" };
  w.fetch = async u => { const b = String(u).split("/").pop();
    return data[b] ? { ok: true, status: 200, json: async () => data[b] }
                   : { ok: false, status: 404, json: async () => ({}) }; };
  w.requestAnimationFrame = cb => setTimeout(cb, 0);
  w.eval(fs.readFileSync(JS, "utf8"));
  await w.__DFBNB_PLAN_SYSTEM_API.mount(url);
  await new Promise(r => setTimeout(r, 30));
  return w.document;
}

(async () => {
  const { doc: data, ids } = fixture();
  const document = await mount("/df/plan-checklists/weapon/",
                               { "plan_master.json": data });

  const rowFor = id => [...document.querySelectorAll(".reward")]
    .find(r => (r.querySelector(".rewardTitleBtn") || {}).title ===
               (data.items.find(i => i.id === id) || {}).name);

  const changedRow = rowFor(ids[0]);
  ok(!!changedRow, "the changed row renders at all");
  if (changedRow) {
    ok(!!changedRow.querySelector(".pillChanged"),
       "a row with changes carries the ↻ Changed pill");
    ok(!changedRow.querySelector(".pillNew"),
       "a changed row does NOT also carry the ★ NEW pill");
    const tech = changedRow.textContent;
    ok(tech.includes("Changed since the last build"),
       "Technical carries the change block heading");
    ok(tech.includes("Now tradeable"),
       "Technical states the change in words");
  }

  const routeRow = rowFor(ids[1]);
  if (routeRow) {
    const tag = routeRow.querySelector(".obtRate__new");
    ok(!!tag, "a newly-added source is tagged New on its own line in How to Obtain");
    ok(tag && /Grahm/.test(tag.parentNode.textContent),
       "the New tag sits on the route it belongs to");
    ok(routeRow.textContent.includes("Now also comes from Grahm's Meat Cook"),
       "Technical names the source that was added");
  }

  const quietRow = rowFor(ids[2]);
  if (quietRow) {
    ok(!quietRow.querySelector(".pillChanged"),
       "a row with an EMPTY changes list gets no pill");
    ok(!quietRow.textContent.includes("Changed since the last build"),
       "a row with an empty changes list gets no Technical block");
  }

  /* A page built before plan_changes existed carries no `changes` key at all.
     It has to render exactly as it did, not throw and not show empty blocks. */
  const legacy = JSON.parse(JSON.stringify(master));
  for (const it of legacy.items) delete it.changes;
  const old = await mount("/df/plan-checklists/weapon/", { "plan_master.json": legacy });
  ok(old.querySelectorAll(".reward").length > 0,
     "a dataset with no changes key still renders");
  ok(old.querySelectorAll(".pillChanged").length === 0,
     "a dataset with no changes key shows no Changed pills");

  console.log(`\n${checks - fails}/${checks} change-tracking checks passed` +
              (fails ? `  — ${fails} FAILED` : "  — all green"));
  process.exit(fails ? 1 : 0);
})();
