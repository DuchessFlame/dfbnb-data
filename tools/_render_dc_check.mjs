import { JSDOM } from "jsdom";
import fs from "fs";

const RENDERER = "/sessions/friendly-zealous-darwin/mnt/1 site-data/json/dfbnb-child/assets/df-bnb-farming-non-perishable-guide.js";
const DIST = "/sessions/friendly-zealous-darwin/mnt/GitHub/dfbnb-data/dist/farming_spawns/deathclaw-egg_spawns.json";

const jsonText = fs.readFileSync(DIST, "utf-8");
const rendererJs = fs.readFileSync(RENDERER, "utf-8");

const url = "https://www.buffsnbrew.com/bnb/farming/eggs/eggs-deathclaw/deathclaw-guide/";
const dom = new JSDOM(
  `<!DOCTYPE html><html><head>
     <script src="https://x/assets/df-bnb-farming-non-perishable-guide.js"></script>
   </head><body><div id="dfbnbGuideBody">Coming soon</div></body></html>`,
  { url, runScripts: "outside-only", pretendToBeVisual: true }
);

const { window } = dom;
const errors = [];
window.addEventListener("error", e => errors.push("window error: " + e.message));
window.fetch = async () => ({ ok: true, json: async () => JSON.parse(jsonText) });
// scrollIntoView not implemented in jsdom
window.Element.prototype.scrollIntoView = function () {};

// Run the renderer IIFE in the jsdom window context.
try {
  window.eval(rendererJs);
} catch (e) {
  errors.push("eval error: " + e.stack);
}

await window.__DFBNB_FARMING_NP_GUIDE_API.mount(new window.URL(url).pathname)
  .catch(e => errors.push("mount error: " + e.stack));

const body = window.document.getElementById("dfbnbGuideBody");
const roots = [...body.querySelectorAll("details.fnpg-root-expand > summary .fnpg-expand__title")]
  .map(n => n.textContent.trim());

console.log("=== JS ERRORS ===");
console.log(errors.length ? errors.join("\n") : "(none)");

console.log("\n=== ROOT EXPANDS (in render order) ===");
roots.forEach((t, i) => console.log(`  ${i + 1}. ${t}`));

const html = body.innerHTML;
function has(label, cond) { console.log(`  [${cond ? "PASS" : "FAIL"}] ${label}`); return cond; }

console.log("\n=== CHECKS ===");
has("Random Encounters root present", roots.includes("Random Encounters"));
has("Events expand renamed to 'Activities, Events & Quests'", roots.includes("Activities, Events & Quests"));
has("Old 'Events & Activities' title gone", !roots.includes("Events & Activities"));
has("RE: Deathclaw Nest vs Super Mutants", html.includes("Deathclaw Nest vs Super Mutants"));
has("RE: Deathclaw Nest vs Scorched", html.includes("Deathclaw Nest vs Scorched"));
has("RE: Deathclaw vs Merchant Caravan", html.includes("Deathclaw vs Merchant Caravan"));
has("RE formids present (0035EBED/0035EC0F/0035EBF0)",
    html.includes("0035EBED") && html.includes("0035EC0F") && html.includes("0035EBF0"));
has("Liebowitz quest row", html.includes("Liebowitz") && html.includes("W05_MQ_002P_Radical_TylerCounty"));
has("Liebowitz note (Hunter for Hire / Tyler County)",
    html.includes("Hunter for Hire") && html.includes("Tyler County"));
has("Ella Ames' Bunker label present", html.includes("Ella Ames&#39; Bunker") || html.includes("Ella Ames' Bunker"));
has("Excelsior Model Home NOT shown", !html.includes("Excelsior Model Home"));
has("Highway Town Interior marker present", html.includes("Highway Town Interior"));
has("Enclave inaccessible note", html.toLowerCase().includes("cannot be entered"));
has("Enclave marker present", html.includes("Enclave Research Facility"));

// Tunnel of Love: 5 nest blocks
const tolMatch = html.match(/Tunnel of Love[\s\S]*?<\/details>/);
const nestBlocks = (html.match(/Deathclaw Nest #\d/g) || []);
has("Tunnel of Love present", html.includes("Tunnel of Love"));
console.log("     (nest per-spawn labels found across page: " + new Set(nestBlocks).size + " distinct #, total " + nestBlocks.length + ")");

// Count fixed-spawn markers rendered
const markerTitles = [...body.querySelectorAll("details.fnpg-marker > summary .fnpg-expand__title")].map(n => n.textContent.trim());
has("15 fixed-spawn markers rendered", markerTitles.length === 15);
console.log("     markers: " + markerTitles.join(", "));

// Directions coverage: markers with a directions block (non-'coming soon')
const dirText = [...body.querySelectorAll(".fnpg-loc-directions__text")].map(n => n.textContent.trim());
const realDirs = dirText.filter(t => t && !/coming soon/i.test(t));
console.log("     directions blocks with real text: " + realDirs.length);

console.log("\n=== middle-expand alphabetical order check ===");
const middle = roots.slice(roots.indexOf("Farming Tips and Tricks") + 1, roots.indexOf("Fixed Spawn Locations"));
console.log("  middle expands: " + middle.join(" | "));
const sorted = [...middle].sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
has("middle expands alphabetical", JSON.stringify(middle) === JSON.stringify(sorted));
