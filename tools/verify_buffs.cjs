/* tools/verify_buffs.cjs — structural check for the /bnb/buffs/* pages.
 *
 * Runs the REAL df-bnb-buffs.js against the REAL dist/buffs/*.json inside
 * jsdom and asserts the rules the pages are supposed to guarantee:
 *   - SPECIAL groups first, spelled S-P-E-C-I-A-L, then A-Z, "No Buff
 *     Effects" last
 *   - per-mode sub-expand order
 *   - no checkboxes and no progress bar (buff pages are not checklists)
 *   - everything closed on load; Expand All opens roots only; Close All
 *     closes everything
 *   - Style B search: hits auto-open, misses hide, clearing restores
 *   - no raw engine markup (<mag>, <ITEM1...>, undefined, NaN) on the page
 *
 * Usage:  npm i jsdom  &&  node tools/verify_buffs.cjs
 * Exits non-zero on the first failing assertion, so it can gate CI.
 */
const fs = require("fs");
const path = require("path");
const { JSDOM } = require("jsdom");

/* The renderer lives in the WordPress child theme, which is a separate
   (non-git) folder, so point at it with an env var rather than guessing:
     BUFFS_ASSETS=".../1 site-data/json/dfbnb-child/assets" node tools/verify_buffs.cjs
   Defaults assume the OneDrive layout sitting beside this repo. */
const ASSETS = process.env.BUFFS_ASSETS || path.resolve(
  __dirname, "../../../Guides and Stuff/Json Files for Website/1 site-data/json/dfbnb-child/assets");
const DIST = path.resolve(__dirname, "../dist/buffs");
const JS = fs.readFileSync(path.join(ASSETS, "df-bnb-buffs.js"), "utf8");

const PAGES = {
  "alcohol": "/bnb/buffs/alcohol/alcohol-buffs/",
  "chems": "/bnb/buffs/chems/chem-buffs/",
  "food": "/bnb/buffs/food/food-buffs/",
  "nuka-cola": "/bnb/buffs/nuka-cola-products/nuka-cola-product-buffs/",
  "magazines": "/bnb/buffs/magazines/magazine-buffs/",
  "bobbleheads": "/bnb/buffs/bobbleheads/bobblehead-buffs/",
  "mutations": "/bnb/buffs/mutations/mutation-buffs/",
  "scout-banners": "/bnb/buffs/scout-banners/scout-banner-buffs/",
};

const SPECIAL = ["Strength", "Perception", "Endurance", "Charisma",
                 "Intelligence", "Agility", "Luck"];

let failures = 0;
function check(ok, msg) {
  if (!ok) { failures++; console.log("   FAIL  " + msg); }
  return ok;
}

async function run(page, url) {
  const dom = new JSDOM(
    `<!doctype html><html><body class="logged-in"><div id="dfbnbGuideBody"></div></body></html>`,
    { url: "https://www.buffsnbrew.com" + url, runScripts: "outside-only", pretendToBeVisual: true }
  );
  const { window } = dom;
  const json = JSON.parse(fs.readFileSync(path.join(DIST, page + ".json"), "utf8"));

  window.fetch = async () => ({ ok: true, status: 200, json: async () => json });
  window.Element.prototype.scrollIntoView = function () {};

  const errors = [];
  window.console = Object.assign({}, console, { error: (...a) => errors.push(a.join(" ")) });

  window.eval(JS);
  await window.__DFBNB_BUFFS_API.mount(url);

  const d = window.document;
  const root = d.querySelector("#bnb-buffs-root");
  console.log(`\n=== ${page}  (mode ${json.mode})`);
  if (!check(!!root, "no root element rendered")) return;
  check(errors.length === 0, "console errors: " + errors.join(" | "));

  // Header card
  const title = root.querySelector(".bf-top-title");
  const total = root.querySelector(".bf-top-total");
  console.log("   title : " + (title && title.textContent));
  console.log("   count : " + (total && total.textContent));
  check(!!title && title.textContent.startsWith("Buffs - "), "title prefix wrong");

  // NOT a checklist
  check(root.querySelectorAll("input[type=checkbox]").length === 0, "checkbox present");
  check(root.querySelectorAll(".bf-progress-bar, .ci-progress-bar").length === 0, "progress bar present");

  // Buttons
  const btns = Array.from(root.querySelectorAll(".bf-btn")).map(b => b.textContent);
  check(btns.join(",") === "Expand All,Close All", "buttons: " + btns.join(","));

  const groups = Array.from(root.querySelectorAll(".bf-group"));
  const cards = Array.from(root.querySelectorAll(".bf-card"));
  console.log(`   groups: ${groups.length}   cards: ${cards.length}   items(json): ${json.items.length}`);

  if (json.mode === "effect-groups") {
    const labels = groups.map(g => g.querySelector("h2").textContent);
    console.log("   order : " + labels.join(" | "));
    // SPECIAL present must come first, in S-P-E-C-I-A-L order
    const present = SPECIAL.filter(s => labels.includes(s));
    check(labels.slice(0, present.length).join(",") === present.join(","),
          "SPECIAL not first / out of order");
    // The tail after SPECIAL must be A-Z, with "No Buff Effects" allowed last
    let tail = labels.slice(present.length);
    if (tail[tail.length - 1] === "No Buff Effects") tail = tail.slice(0, -1);
    const sorted = tail.slice().sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
    check(tail.join(",") === sorted.join(","), "A-Z tail out of order:\n     got  " + tail.join(",") + "\n     want " + sorted.join(","));
    check(labels[labels.length - 1] === "No Buff Effects" || !labels.includes("No Buff Effects"),
          "No Buff Effects is not last");
  } else {
    const names = cards.map(c => c.querySelector(".bf-card-name").textContent);
    if (json.mode === "items-abc") {
      const sorted = names.slice().sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
      check(names.join(",") === sorted.join(","), "items not A-Z");
    } else {
      // effect-rows is DECLARED order, not A-Z: the five Scout's Codes read as
      // a set in the order the banner's own description lists them.
      check(names.join(",") === json.items.map(i => i.name).join(","),
            "effect-rows not in build order");
    }
    console.log("   first : " + names.slice(0, 4).join(" | "));
  }

  // Sub-expand order per mode
  const sample = cards[0];
  const secs = Array.from(sample.querySelectorAll(".bf-section > .bf-section-head > .bf-section-label"))
    .map(n => n.textContent);
  const want = json.mode === "effect-groups"
    ? ["Item Image", "Output & Effects", "How to Obtain", "Technical"]
    : ["Item Image", "How to Obtain", "Output & Effects", "Technical"];
  check(secs.join(",") === want.join(","), `sub-expand order: ${secs.join(",")} (want ${want.join(",")})`);
  console.log("   subs  : " + secs.join(" > "));

  // Everything starts closed
  check(root.querySelectorAll(".bf-group.is-open, .bf-card.is-open, .bf-section.is-open").length === 0,
        "something is open on first render");

  // Expand All opens roots only, never nested sections
  root.querySelectorAll(".bf-btn")[0].dispatchEvent(new window.Event("click", { bubbles: true }));
  check(root.querySelectorAll(".bf-section.is-open").length === 0,
        "Expand All opened nested sub-expands");
  check(root.querySelectorAll(".bf-card.is-open").length === cards.length, "Expand All missed rows");

  // Close All closes everything
  root.querySelectorAll(".bf-card .bf-section")[0].classList.add("is-open");
  root.querySelectorAll(".bf-btn")[1].dispatchEvent(new window.Event("click", { bubbles: true }));
  check(root.querySelectorAll(".is-open").length === 0, "Close All left something open");

  // Style B search: a real item name finds itself and opens its row
  const probe = (json.items[Math.floor(json.items.length / 2)].name || "").slice(0, 12).toLowerCase();
  const search = root.querySelector(".bf-top-search");
  search.value = probe;
  search.dispatchEvent(new window.Event("input", { bubbles: true }));
  const visible = Array.from(root.querySelectorAll(".bf-card")).filter(c => c.style.display !== "none");
  check(visible.length > 0, `search "${probe}" matched nothing`);
  check(visible.every(c => c.classList.contains("is-open")), "search hit not auto-opened");
  console.log(`   search: "${probe}" -> ${visible.length} visible, count line "${root.querySelector(".bf-top-count").textContent}"`);

  // Clearing restores everything and reverts what the search opened
  search.value = "";
  search.dispatchEvent(new window.Event("input", { bubbles: true }));
  const back = Array.from(root.querySelectorAll(".bf-card")).filter(c => c.style.display !== "none");
  check(back.length === cards.length, "clearing search did not restore all rows");
  check(root.querySelectorAll(".bf-card.is-open").length === 0, "clearing search left rows open");

  // Guide links are real anchors
  const links = Array.from(root.querySelectorAll(".bf-route-link"));
  if (links.length) console.log(`   links : ${links.length} guide link(s), e.g. ${links[0].textContent} -> ${links[0].getAttribute("href")}`);

  // No raw engine markup leaked into the page
  const text = root.textContent;
  for (const bad of ["<mag>", "<MAG>", "<ITEM1", "undefined", "[object Object]", "NaN"]) {
    check(!text.includes(bad), `leaked ${bad} into the rendered page`);
  }
}

(async () => {
  for (const [page, url] of Object.entries(PAGES)) {
    try { await run(page, url); }
    catch (e) { failures++; console.log(`\n=== ${page}\n   THREW ${e && e.stack ? e.stack.split("\n").slice(0,3).join(" | ") : e}`); }
  }
  console.log(`\n${failures === 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED"}`);
  process.exit(failures ? 1 : 0);
})();
