// scripts/build_df_calculators_json.mjs
import fs from "fs";
import path from "path";

function readText(p) {
  return fs.readFileSync(p, "utf8");
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function parseTSV(tsvText) {
  const lines = String(tsvText || "").split(/\r?\n/).filter(Boolean);
  if (!lines.length) return { cols: [], rows: [] };
  const cols = lines.shift().split("\t").map(s => s.trim());
  const rows = lines.map(line => {
    const parts = line.split("\t");
    const row = {};
    for (let i = 0; i < cols.length; i++) row[cols[i]] = parts[i] ?? "";
    return row;
  });
  return { cols, rows };
}

function safeText(s) {
  return String(s ?? "").trim();
}

function upperKeyed(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) out[k.trim()] = v;
  return out;
}

function stripQuotes(s) {
  return String(s ?? "").replace(/"/g, "").trim();
}

function splitPipe(s) {
  const t = safeText(s);
  if (!t) return [];
  return t.split("|").map(x => x.trim()).filter(Boolean);
}

function parseFVPA(fvpaRaw) {
  // Example: "Nuclear Material:1 | Glorybell:1 | Seesprout:1 | Candykill:2"
  const t = safeText(fvpaRaw);
  if (!t) return [];
  return t.split("|").map(x => x.trim()).filter(Boolean).map(part => {
    const m = part.match(/^(.+?):\s*(\d+)\s*$/);
    if (!m) return { name: part, qty: 1 };
    return { name: m[1].trim(), qty: parseInt(m[2], 10) };
  });
}

function isJunkEdid(edidRaw) {
  const edid = safeText(edidRaw).toUpperCase();
  if (!edid) return true;

  // Prefix junk
  if (edid.startsWith("DEL")) return true;
  if (edid.startsWith("POST")) return true;
  if (edid.startsWith("CUT")) return true;
  if (edid.startsWith("ZZZ")) return true;

  // Token junk anywhere
  if (edid.includes("NONPLAYABLE")) return true;
  if (edid.includes("CAMPPETS")) return true;

  return false;
}

/* =========================================================
   1) BUILD INSPIRATION (FLST BestBuildsTagCategories_*)
   ========================================================= */

function buildBuildInspirationJson(flstPath, outPath) {
  const { rows } = parseTSV(readText(flstPath));
  const best = rows
    .map(upperKeyed)
    .filter(r => safeText(r.FLST_EDID).startsWith("BestBuildsTagCategories_"));

  // Group by list
  const byList = new Map();
  for (const r of best) {
    const id = safeText(r.FLST_EDID);
    if (!byList.has(id)) byList.set(id, []);
    byList.get(id).push(r);
  }

  const categories = [];
  for (const [listEdid, listRows] of byList.entries()) {
    // Label: prefer FLST_FULL, fallback from EDID suffix
    const flstFull = safeText(listRows[0]?.FLST_FULL);
    const label = flstFull ? flstFull : listEdid.replace(/^BestBuildsTagCategories_/, "").toUpperCase();

    // Tags: use Entry_FULL if present, else derive from Entry_EDID
    const tags = listRows
      .filter(r => safeText(r.Entry_EDID))
      .map(r => {
        const edid = safeText(r.Entry_EDID);
        const full = safeText(r.Entry_FULL);
        let tagLabel = full;
        if (!tagLabel) {
          // BestBuilds_KeywordTag_Weathers_Gusty -> Gusty
          const parts = edid.split("_");
          tagLabel = parts.length ? parts[parts.length - 1] : edid;
        }
        // Normalize: underscores -> spaces, Title Case-ish
        tagLabel = tagLabel.replace(/_/g, " ").trim();
        return { edid, label: tagLabel };
      });

    // Stable sort A–Z by label
    tags.sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base" }));

    categories.push({ id: listEdid, label, tags });
  }

  // Stable sort categories by label
  categories.sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base" }));

  const out = {
    generatedAt: new Date().toISOString(),
    kind: "build_inspiration",
    categories
  };

  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
}

/* =========================================================
   2) OUTFIT INSPIRATION (ARMO export with BOD2 + Keywords)
   Expected columns:
     ARMO_FormID, ARMO_EDID, ARMO_FULL,
     BOD2_FirstPersonFlags, BOD2_FirstPersonFlagLabels, Keywords_EDID_Flat
   ========================================================= */

function classifyArmoItem(edid, keywordsFlat) {
  const upEdid = safeText(edid).toUpperCase();
  const kw = safeText(keywordsFlat);

  // Underarmor stacks with armor
  if (kw.includes("ObjectTypeUnderarmor") || kw.includes("ObjectTypeUnderArmor")) return "UNDERARMOR";

  // Armor sets (light/medium/heavy)
  if (kw.includes("ArmorLight") || kw.includes("ArmorMedium") || kw.includes("ArmorHeavy")) return "ARMOR";

  // Outfits
  if (kw.includes("ArmorOutfit") || upEdid.includes("_OUTFIT_")) return "OUTFIT";

  // Headwear
  if (upEdid.includes("_HEADWEAR_")) return "HEADWEAR";

  // Clothes (general)
  if (upEdid.includes("_CLOTHES_")) return "CLOTHES";

  // Backpack (best effort)
  if (kw.toLowerCase().includes("backpack") || upEdid.includes("BACKPACK")) return "BACKPACK";

  return "OTHER";
}

function deriveArmorSetKey(edidRaw, fullRaw) {
  // Best-effort: strip limb suffix patterns so pieces group into a set.
  // This is deliberately conservative; you can refine later.
  let s = safeText(edidRaw);
  if (!s) s = safeText(fullRaw);
  if (!s) return "";

  // Common suffix tokens
  s = s.replace(/_(Left|Right)(Arm|Leg)\b/i, "");
  s = s.replace(/_(L|R)(Arm|Leg)\b/i, "");
  s = s.replace(/_(ArmLeft|ArmRight|LegLeft|LegRight)\b/i, "");
  s = s.replace(/_(Chest|Torso|Helmet|Head)\b/i, "");
  s = s.replace(/_ARMO_/i, "_");
  return s;
}

function buildOutfitInspirationJson(armoPath, outPath) {
  const { rows } = parseTSV(readText(armoPath));
  const items = rows.map(upperKeyed)
    .map(r => {
      const formId = safeText(r.ARMO_FormID);
      const edid = safeText(r.ARMO_EDID);
      const full = safeText(r.ARMO_FULL);

      const flags = splitPipe(r.BOD2_FirstPersonFlags).map(x => x.trim()).filter(Boolean);
      const labels = splitPipe(r.BOD2_FirstPersonFlagLabels).map(x => x.trim()).filter(Boolean);
      const keywords = splitPipe(r.Keywords_EDID_Flat);

      return {
        formId, edid, full,
        flags, flagLabels: labels,
        keywords,
        type: classifyArmoItem(edid, r.Keywords_EDID_Flat),
        armorSetKey: ""
      };
    })
    // Must have EDID and not junk
    .filter(it => it.edid && !isJunkEdid(it.edid))
    // Only your pools
    .filter(it => {
      const up = it.edid.toUpperCase();
      return up.includes("_HEADWEAR_") || up.includes("_CLOTHES_") || up.includes("_OUTFIT_") || it.type === "UNDERARMOR" || it.type === "ARMOR" || it.type === "BACKPACK";
    });

  // Add armorSetKey only for ARMOR type
  for (const it of items) {
    if (it.type === "ARMOR") it.armorSetKey = deriveArmorSetKey(it.edid, it.full);
  }

  const out = {
    generatedAt: new Date().toISOString(),
    kind: "outfit_inspiration",
    items
  };

  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
}

/* =========================================================
   3) BIG BLOOM REWARD CRAFTING (COBJ)
   Expected columns:
     COBJ_FormID, COBJ_EDID, CNAM_FULL, GNAM_FULL, FNAM_Keywords, FVPA
   ========================================================= */

function buildBigBloomCraftingJson(cobjPath, outPath) {
  const { rows } = parseTSV(readText(cobjPath));
  const recs = rows.map(upperKeyed)
    .filter(r => {
      const edid = safeText(r.COBJ_EDID);
      if (!edid) return false;

      // Big Bloom namespace (as proven by your sample)
      if (edid.startsWith("SSE_")) return true;
      if (edid.startsWith("workshop_co_Tinkers_SSE_")) return true;

      return false;
    })
    .map(r => ({
      cobjFormId: safeText(r.COBJ_FormID),
      cobjEdid: safeText(r.COBJ_EDID),

      craftedFormId: safeText(r.CNAM_FormID),
      craftedEdid: safeText(r.CNAM_EDID),
      craftedName: safeText(r.CNAM_FULL) || safeText(r.CNAM_EDID),

      planFormId: safeText(r.GNAM_FormID),
      planEdid: safeText(r.GNAM_EDID),
      planName: safeText(r.GNAM_FULL),

      recipeKeywords: safeText(r.FNAM_Keywords),
      components: parseFVPA(r.FVPA)
    }));

  // Categorize by COBJ_EDID prefix
  function categoryFor(edid) {
    const u = String(edid || "");
    if (u.startsWith("workshop_co_Tinkers_SSE_")) return "Hybrid Flowers";
    if (u.startsWith("SSE_workshop_co_")) return "CAMP / Workshop";
    if (u.startsWith("SSE_co_Headwear_")) return "Headwear";
    if (u.startsWith("SSE_co_Clothes_")) return "Outfits";
    if (u.startsWith("SSE_co_meal_")) return "Food / Drink";
    if (u.startsWith("SSE_co_mod_")) return "Weapon Mods";
    return "Other";
  }

  for (const r of recs) r.category = categoryFor(r.cobjEdid);

  // Sort stable
  recs.sort((a, b) =>
    a.category.localeCompare(b.category) ||
    a.craftedName.localeCompare(b.craftedName, undefined, { sensitivity: "base" })
  );

  const out = {
    generatedAt: new Date().toISOString(),
    kind: "big_bloom_crafting",
    recipes: recs
  };

  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
}

/* =========================================================
   MAIN
   ========================================================= */

const FLST_TSV = process.env.FLST_TSV || "";
const ARMO_TSV = process.env.ARMO_BOD2_TSV || "";
const COBJ_TSV = process.env.COBJ_TSV || "";

const OUT_DIR = process.env.OUT_DIR || "dist/calculators";

if (!FLST_TSV || !ARMO_TSV || !COBJ_TSV) {
  console.error("Missing env vars. Required: FLST_TSV, ARMO_BOD2_TSV, COBJ_TSV (and optional OUT_DIR).");
  process.exit(1);
}

ensureDir(OUT_DIR);

buildBuildInspirationJson(FLST_TSV, path.join(OUT_DIR, "build_inspiration.json"));
buildOutfitInspirationJson(ARMO_TSV, path.join(OUT_DIR, "outfit_inspiration.json"));
buildBigBloomCraftingJson(COBJ_TSV, path.join(OUT_DIR, "big_bloom_crafting.json"));

console.log("Built calculators JSON into:", OUT_DIR);