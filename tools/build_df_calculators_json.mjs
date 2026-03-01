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

// Return the first non-empty value from a list of possible column names
function pickCol(row, keys) {
  for (const k of keys) {
    if (!k) continue;
    const v = safeText(row[k]);
    if (v) return v;
  }
  return "";
}

// Extract a human-readable FULL name from common xEdit-ish strings:
// e.g. SSE_ARMO_Headwear_Whatever "Flower Crown - Carnal Weeper" [ARMO:007ACE88]
function extractQuotedName(s) {
  const t = safeText(s);
  if (!t) return "";
  const m = t.match(/"([^"]+)"/);
  return m ? safeText(m[1]) : "";
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

  // Your rule: ignore if it CONTAINS these tokens anywhere
  const badTokens = ["NONPLAYABLE", "CAMPPETS", "DEL", "POST", "CUT", "ZZZ"];
  for (const t of badTokens) {
    if (edid.includes(t)) return true;
  }
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

   // --- Optional image overrides for Big Bloom ---
function loadBigBloomImageMap() {
  const imgPath = path.join("tsv", "big_bloom_images.json");
  if (!fs.existsSync(imgPath)) return {};
  try {
    return JSON.parse(fs.readFileSync(imgPath, "utf8"));
  } catch (e) {
    console.error("Failed to parse big_bloom_images.json:", e);
    return {};
  }
}

function buildBigBloomCraftingJson(cobjPath, outPath) {
  const { rows } = parseTSV(readText(cobjPath));

  // Attach image URLs (keyed by Created Object FULL name)
  // We also use this as an "allow list" so Big Bloom doesn't accidentally ingest the whole game.
  const imageMap = loadBigBloomImageMap();

  const imageKeys = new Set(Object.keys(imageMap).map(k => safeText(k).toLowerCase()).filter(Boolean));

  const recs = rows.map(upperKeyed)
    .filter(r => {
      // Be tolerant: different exports name these columns differently
      const edid = pickCol(r, [
        "COBJ_EDID",
        "EDID",
        "EDID - Editor ID",
        "Editor ID"
      ]);

      // Try several likely sources for the crafted item display name
      const cnamFull = pickCol(r, [
        "CNAM_FULL",
        "CNAM - Created Object",
        "CNAM",
        "Created Object",
        "Created Object FULL"
      ]);

      const craftedName =
        safeText(pickCol(r, ["CNAM_FULL"])) ||
        extractQuotedName(cnamFull) ||
        safeText(pickCol(r, ["HNAM - Build Group Name", "HNAM_BuildGroupName", "Build Group Name"])) ||
        safeText(pickCol(r, ["CNAM_EDID"])) ||
        safeText(cnamFull);

      // Always require an EDID
      if (!edid) return false;

      // NEW RULE: if the crafted item name exists in big_bloom_images.json, include it
      if (craftedName && imageKeys.has(craftedName.toLowerCase())) return true;

      // Legacy fallback
      if (edid.startsWith("SSE_")) return true;
      if (edid.startsWith("workshop_co_Tinkers_SSE_")) return true;

      return false;
    })
    .map(r => {
      const cobjEdid = pickCol(r, ["COBJ_EDID", "EDID", "EDID - Editor ID", "Editor ID"]);

      const cnamFullRaw = pickCol(r, ["CNAM_FULL", "CNAM - Created Object", "CNAM", "Created Object"]);
      const cnamEdid = pickCol(r, ["CNAM_EDID", "CNAM_EDID - Editor ID", "CNAM - EDID", "Created Object EDID"]);
      const craftedName =
        safeText(pickCol(r, ["CNAM_FULL"])) ||
        extractQuotedName(cnamFullRaw) ||
        safeText(pickCol(r, ["HNAM - Build Group Name", "HNAM_BuildGroupName", "Build Group Name"])) ||
        safeText(cnamEdid) ||
        safeText(cnamFullRaw);

      const gnamFullRaw = pickCol(r, ["GNAM_FULL", "GNAM - Learn Recipe from", "GNAM", "Learn Recipe from"]);
      const planName =
        safeText(pickCol(r, ["GNAM_FULL"])) ||
        extractQuotedName(gnamFullRaw) ||
        safeText(gnamFullRaw);

      return {
        cobjFormId: pickCol(r, ["COBJ_FormID", "FormID", "Form ID", "Record Header FormID"]),
        cobjEdid: cobjEdid,

        craftedFormId: pickCol(r, ["CNAM_FormID"]),
        craftedEdid: cnamEdid,
        craftedName: craftedName,

        planFormId: pickCol(r, ["GNAM_FormID"]),
        planEdid: pickCol(r, ["GNAM_EDID"]),
        planName: planName,

        recipeKeywords: pickCol(r, ["FNAM_Keywords", "FNAM - Category", "FNAM", "Category"]),
        components: parseFVPA(pickCol(r, ["FVPA", "FVPA - Components (sorted)", "Components", "Components (sorted)"])),
        category: ""
      };
    });

  const HYBRIDS = new Set([
    "Candykill",
    "Embergold",
    "Gigablossom",
    "Glorybell",
    "Green Invader",
    "Seesprout",
    "Starlace"
  ].map(s => s.toLowerCase()));

  const APPAREL = new Set([
    "Flower Suit",
    "Flower-Printed Sundress",
    "Wasteland Florist Apron",
    "Wasteland Florist Sunhat"
  ].map(s => s.toLowerCase()));

  const FOOD = new Set([
    "Black-Eyed Susan's Soothin'",
    "Gamma Green Tea"
  ].map(s => s.toLowerCase()));

  function categoryForCraftedName(name) {
    const n = safeText(name).trim();
    const nl = n.toLowerCase();

    if (!n) return "Other";
    if (HYBRIDS.has(nl)) return "Hybrid Flowers";
    if (n.startsWith("Flower Crown -")) return "Flower Crowns / Headwear";
    if (n.startsWith("Pot o'")) return "Flower Pots / Displays";
    if (nl.includes("glazed pot")) return "Flower Pots / Displays";
    if (nl.includes("tube") || nl.includes("display")) return "Flower Pots / Displays";
    if (APPAREL.has(nl)) return "Apparel";
    if (FOOD.has(nl)) return "Food";
    return "Other";
  }

  for (const r of recs) r.category = categoryForCraftedName(r.craftedName);

  // Case-insensitive image lookup
  const imageMapLower = {};
  for (const [k, v] of Object.entries(imageMap || {})) {
    const kk = safeText(k).toLowerCase();
    if (kk) imageMapLower[kk] = v;
  }

  for (const r of recs) {
    const key = safeText(r.craftedName).toLowerCase();
    if (key && imageMapLower[key]) {
      r.image = imageMapLower[key];
    }
  }

  // --- Build a deterministic index for recursion ---
  // Key by craftedName (case-insensitive). This matches FVPA's "Name:qty" format.
  // If you later add FVPA component IDs, you can switch this to FormID-based matching.
  const recipeByCraftedName = new Map();
  for (const r of recs) {
    const key = safeText(r.craftedName).toLowerCase();
    if (!key) continue;
    // If duplicates exist, keep the first stable one (deterministic)
    if (!recipeByCraftedName.has(key)) recipeByCraftedName.set(key, r);
  }

  function addToTotals(mapObj, name, qty) {
    const k = safeText(name);
    if (!k || !qty) return;
    mapObj[k] = (mapObj[k] || 0) + qty;
  }

  function mergeTotals(dst, src, mult = 1) {
    for (const [k, v] of Object.entries(src)) addToTotals(dst, k, v * mult);
  }

  // Recursively resolve:
  // - direct: immediate FVPA
  // - intermediate: craftable sub-items needed (and how many)
  // - base: non-craftable leaf components
  // - craftOrder: steps in dependency order
  function resolveRecipe(rootRecipe) {
    const visiting = new Set();
    const visited = new Set();

    const intermediate = {};
    const base = {};
    const craftOrder = []; // dependency-first order

    function walk(recipe, mult) {
      const key = safeText(recipe.craftedName).toLowerCase();
      if (!key) return;

      if (visiting.has(key)) {
        // Cycle guard: treat cycle edge as base leaf to avoid infinite recursion.
        addToTotals(base, recipe.craftedName, mult);
        return;
      }

      visiting.add(key);

      for (const comp of recipe.components || []) {
        const compName = safeText(comp.name);
        const compQty = Number(comp.qty || 0);
        if (!compName || !compQty) continue;

        const dep = recipeByCraftedName.get(compName.toLowerCase());
        if (dep) {
          // It's craftable: count as intermediate and recurse into its components
          addToTotals(intermediate, dep.craftedName, compQty * mult);
          walk(dep, compQty * mult);
        } else {
          // It's a leaf/base component
          addToTotals(base, compName, compQty * mult);
        }
      }

      visiting.delete(key);

      if (!visited.has(key)) {
        visited.add(key);
        craftOrder.push(recipe.craftedName);
      }
    }

    walk(rootRecipe, 1);

    return {
      direct: (rootRecipe.components || []).map(c => ({ name: c.name, qty: c.qty })),
      intermediate,
      base,
      craftOrder
    };
  }

  for (const r of recs) {
    r.resolution = resolveRecipe(r);
  }

  // Stable sort for UI dropdown/grouping
  recs.sort((a, b) =>
    a.category.localeCompare(b.category) ||
    a.craftedName.localeCompare(b.craftedName, undefined, { sensitivity: "base" })
  );

  const out = {
    generatedAt: new Date().toISOString(),
    kind: "big_bloom_crafting",
    recipes: recs,
    index: {
      byCraftedNameLower: Object.fromEntries(
        Array.from(recipeByCraftedName.entries()).map(([k, v]) => [k, v.cobjEdid])
      )
    }
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