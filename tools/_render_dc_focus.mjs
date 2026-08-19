import { JSDOM } from "jsdom";
import fs from "fs";
const R = "/sessions/friendly-zealous-darwin/mnt/1 site-data/json/dfbnb-child/assets/df-bnb-farming-non-perishable-guide.js";
const D = "/sessions/friendly-zealous-darwin/mnt/GitHub/dfbnb-data/dist/farming_spawns/deathclaw-egg_spawns.json";
const jsonText = fs.readFileSync(D, "utf-8");
const url = "https://www.buffsnbrew.com/bnb/farming/eggs/eggs-deathclaw/deathclaw-guide/";
const dom = new JSDOM(`<!DOCTYPE html><html><head><script src="https://x/assets/df-bnb-farming-non-perishable-guide.js"></script></head><body><div id="dfbnbGuideBody"></div></body></html>`,
  { url, runScripts: "outside-only", pretendToBeVisual: true });
const { window } = dom;
window.fetch = async () => ({ ok: true, json: async () => JSON.parse(jsonText) });
window.Element.prototype.scrollIntoView = function () {};
window.eval(fs.readFileSync(R, "utf-8"));
await window.__DFBNB_FARMING_NP_GUIDE_API.mount(new window.URL(url).pathname);
const body = window.document.getElementById("dfbnbGuideBody");
const P = (l, c) => console.log(`  [${c ? "PASS" : "FAIL"}] ${l}`);

// Find a marker <details> by title
function marker(name) {
  return [...body.querySelectorAll("details.fnpg-marker")]
    .find(d => d.querySelector(".fnpg-expand__title")?.textContent.trim() === name);
}
const tol = marker("Tunnel of Love");
const tolNests = tol ? [...tol.querySelectorAll(".fnpg-spawn__name")].map(n => n.textContent.trim()) : [];
console.log("Tunnel of Love per-spawn blocks:", tolNests);
P("Tunnel of Love renders 5 nest blocks", tolNests.length === 5);
P("Tunnel of Love 'Getting there' marker directions", /Getting there/.test(tol?.innerHTML || "") && /pink heart lights/.test(tol?.innerHTML || ""));

const dino = marker("Dino Peaks Mini Golf");
const dinoBlocks = dino ? [...dino.querySelectorAll(".fnpg-spawn__name")].map(n => n.textContent.trim()) : [];
console.log("Dino Peaks per-spawn blocks:", dinoBlocks.length, dinoBlocks.join(", "));
P("Dino Peaks 8 spawn blocks (2 egg + 6 nest)", dinoBlocks.length === 8);

// Enclave single-layout directions text
const enc = marker("Enclave Research Facility");
P("Enclave directions rendered (single layout)", /cannot be entered/i.test(enc?.innerHTML || ""));
const hw = marker("Highway Town Interior");
P("Highway Town directions rendered", /Burning Springs interior/i.test(hw?.innerHTML || ""));
const ella = marker("Ella Ames' Bunker");
P("Ella Ames' directions rendered", /blue ute/i.test(ella?.innerHTML || ""));

// RE note rows + Liebowitz note row
const noteRows = [...body.querySelectorAll(".fnpg-vt-note")].map(n => n.textContent.trim());
console.log("note rows count:", noteRows.length);
P("RE note rows present (3)", noteRows.filter(t => /random encounter/i.test(t)).length === 3);
P("Liebowitz note row present", noteRows.some(t => /Hunter for Hire/i.test(t)));

// Liebowitz rate cell shows 'One-time hand-in'
P("Liebowitz rate 'One-time hand-in'", /One-time hand-in/.test(body.innerHTML));
