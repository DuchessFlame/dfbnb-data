// src/plan-system/build-plan-master.mjs
import fs from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const SRC = path.join(ROOT, "src", "plan-system", "plan_master.json");
const OUT_DIR = path.join(ROOT, "dist");
const OUT = path.join(OUT_DIR, "plan_master.json");

function main() {
  if (!fs.existsSync(SRC)) {
    throw new Error(`Missing source JSON: ${SRC}`);
  }

  // Validate JSON (fail the workflow if broken)
  const raw = fs.readFileSync(SRC, "utf8");
  JSON.parse(raw);

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT, raw, "utf8");

  console.log(`Wrote ${OUT}`);
}

main();