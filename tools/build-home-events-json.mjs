// tools/build-home-events-json.mjs
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

function isValidISODate(d) {
  return /^\d{4}-\d{2}-\d{2}$/.test(d);
}

function required(row, field, ctx) {
  const v = (row[field] ?? "").trim();
  if (!v) throw new Error(`${ctx}: missing required field "${field}"`);
  return v;
}

function toIntOrNull(v) {
  const s = (v ?? "").trim();
  if (!s) return null;
  const n = Number.parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}

function buildEvents(tsvRows) {
  const events = [];

  for (const r of tsvRows) {
    const id = required(r, "Id", "events.tsv");
    const title = required(r, "Title", `events.tsv:${id}`);
    const type = required(r, "Type", `events.tsv:${id}`);
    const start = required(r, "StartDate", `events.tsv:${id}`);
    const end = required(r, "EndDate", `events.tsv:${id}`);
    const url = (r.Url ?? "").trim();
    const badge = (r.Badge ?? "").trim();
    const notes = (r.Notes ?? "").trim();

    if (!isValidISODate(start)) throw new Error(`events.tsv:${id} StartDate must be YYYY-MM-DD, got "${start}"`);
    if (!isValidISODate(end)) throw new Error(`events.tsv:${id} EndDate must be YYYY-MM-DD, got "${end}"`);

    events.push({
      id,
      title,
      type,
      startDate: start,
      endDate: end,
      url: url || null,
      badge: badge || null,
      notes: notes || null,
      sort: toIntOrNull(r.Sort),
    });
  }

  // Stable ordering: sort (asc, nulls last) then startDate then title
  events.sort((a, b) => {
    const as = a.sort ?? 999999;
    const bs = b.sort ?? 999999;
    if (as !== bs) return as - bs;
    if (a.startDate !== b.startDate) return a.startDate.localeCompare(b.startDate);
    return a.title.localeCompare(b.title);
  });

  return events;
}

// Minerva deterministic config (anchor-based, no scraping)
// You compute dates at runtime, so the JSON only needs the anchor and rules.
const minervaConfig = {
  timezone: "Australia/Perth",
  anchor: {
    startDate: "2026-09-14",
    location: "Foundation",
    inventoryList: 1,
  },
  rules: {
    cycleWeeks: 4,
    // weeks 1-3 Mon->Wed; week 4 Thu->Mon
    weekStartWeekday: "MON",
    normalDurationDays: 2, // inclusive Mon..Wed is 3 days; we store durations as inclusive range in runtime.
    bigSaleDurationDays: 4, // inclusive Thu..Mon is 5 days
    locationOrder: ["Foundation", "The Whitespring", "Fort Atlas", "The Crater"],
    bigSaleEvery: 4,
  },
};

function main() {
  const repoRoot = process.cwd();

  const inPath = path.join(repoRoot, "src", "home", "events.tsv");
  const outDir = path.join(repoRoot, "dist");
  const outPath = path.join(outDir, "home-events.json");

  const tsv = readText(inPath);
  const rows = parseTSV(tsv);

  const events = buildEvents(rows);

  const payload = {
    schemaVersion: 1,
    timezone: "Australia/Perth",
    generatedAt: new Date().toISOString(),
    minerva: minervaConfig,
    events,
  };

  ensureDir(outDir);
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2) + "\n", "utf8");
  console.log(`Wrote ${outPath} (${events.length} events)`);
}

main();
