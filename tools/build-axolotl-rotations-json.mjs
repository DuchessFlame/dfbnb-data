// tools/build-axolotl-rotations-json.mjs
import fs from "fs";
import path from "path";

function readText(p) {
  return fs.readFileSync(p, "utf8");
}
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function parseTSV(tsvText) {
  const lines = tsvText
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .filter((l) => l.trim().length > 0);

  if (!lines.length) return [];

  const header = lines[0].split("\t").map((h) => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split("\t");
    const row = {};
    for (let c = 0; c < header.length; c++) {
      row[header[c]] = (parts[c] ?? "").trim();
    }
    rows.push(row);
  }
  return rows;
}

function required(row, field, ctx) {
  const v = (row[field] ?? "").trim();
  if (!v) throw new Error(`${ctx}: missing required field "${field}"`);
  return v;
}

function isValidISODate(d) {
  return /^\d{4}-\d{2}-\d{2}$/.test(d);
}

function main() {
  const repoRoot = process.cwd();
  const inPath = path.join(repoRoot, "src", "home", "axolotl-rotations.tsv");
  const outDir = path.join(repoRoot, "dist");
  const outPath = path.join(outDir, "axolotl-rotations.json");

  const tsv = readText(inPath);
  const rows = parseTSV(tsv);

  const items = rows.map((r, idx) => {
    const ctx = `axolotl-rotations.tsv:row${idx + 2}`;
    const month = required(r, "Month", ctx);
    const year = required(r, "Year", ctx);
    const title = required(r, "Title", ctx);
    const startDate = required(r, "StartDate", ctx);
    const endDate = required(r, "EndDate", ctx);

    if (!isValidISODate(startDate)) throw new Error(`${ctx} StartDate must be YYYY-MM-DD, got "${startDate}"`);
    if (!isValidISODate(endDate)) throw new Error(`${ctx} EndDate must be YYYY-MM-DD, got "${endDate}"`);

    return {
      month,
      year,
      title,
      startDate,
      endDate,
      mapUrl: (r.MapUrl ?? "") || null,
      imageUrl: (r.ImageUrl ?? "") || null,
      infoUrl: (r.InfoUrl ?? "") || null,
    };
  });

  const payload = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    items,
  };

  ensureDir(outDir);
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2) + "\n", "utf8");
  console.log(`Wrote ${outPath} (${items.length} months)`);
}

main();
