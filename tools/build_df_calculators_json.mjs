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
  // Supports simple exports like:
  //   "Cloth:3 | Embergold:5"
  // And also xEdit-ish exports like:
  //   c_Cloth "Cloth" [CMPO:001223C7]:3 | SSE_Tier2_Embergold_MiscItem "Embergold" [MISC:007ACA4D]:5
  //
  // Output names must be clean display names so dependency matching works.

  // Strip surrounding quotes from the whole FVPA string first.
  // xEdit sometimes wraps the entire cell value in double-quotes:
  //   "\"Cloth:3 | Embergold:5\"" -> "Cloth:3 | Embergold:5"
  let t = safeText(fvpaRaw).replace(/^"+|"+$/g, "").trim();
  if (!t) return [];

  return t
    .split("|")
    .map(x => x.trim())
    .filter(Boolean)
    .map(part => {
      // Pull qty from the end: ":5" (allow trailing junk like quotes/spaces)
      const qtyMatch = part.match(/:\s*(\d+)\s*$/);
      let qty = qtyMatch ? parseInt(qtyMatch[1], 10) : 1;

      // Name is everything before the last ":<qty>"
      let nameRaw = qtyMatch ? part.slice(0, qtyMatch.index).trim() : part.trim();

      // If name contains a quoted display name, prefer that
      const quoted = extractQuotedName(nameRaw);
      if (quoted) nameRaw = quoted;

      // Some exports accidentally include ":<qty>" inside the quoted name (e.g. "Embergold:5").
      // If so, split it so recursion can match craftable items properly.
      const embedded = nameRaw.match(/^(.*?):\s*(\d+)\s*$/);
      if (embedded) {
        nameRaw = embedded[1].trim();
        if (!qtyMatch) qty = parseInt(embedded[2], 10) || qty;
      }

      // Strip surrounding quotes if it's like "Cloth"
      nameRaw = nameRaw.replace(/^"+|"+$/g, "").replace(/^'+|'+$/g, "").trim();

      // Strip trailing [FORM:ID] if it survived
      nameRaw = nameRaw.replace(/\s*\[[^\]]+\]\s*$/g, "").trim();

      // If it still looks like "EDID Name" with EDID token first, keep last chunk after first space
      // (Conservative: only do this when we see an EDID-ish token)
      const tok = nameRaw.split(/\s+/);
      if (tok.length >= 2 && /^[A-Za-z0-9_]+$/.test(tok[0]) && tok[0].includes("_")) {
        nameRaw = nameRaw.slice(tok[0].length).trim();
      }

      return { name: nameRaw || part.trim(), qty };
    })
    .filter(x => safeText(x.name) && Number(x.qty) > 0);
}

function isJunkEdid(edidRaw) {
  const edid = safeText(edidRaw).toUpperCase();
  if (!edid) return true;

  // Your rule: ignore if it CONTAINS these tokens anywhere
  const badTokens = ["NONPLAYABLE", "NOTPLAYABLE", "CAMPPETS", "DEL", "POST", "CUT", "ZZZ"];
  for (const t of badTokens) {
    if (edid.includes(t)) return true;
  }
  return false;
}

/* =========================================================
   1) BUILD INSPIRATION
   Supports two input formats:

   A) Legacy FLST export (single file) — columns:
        FLST_EDID, FLST_FULL, Entry_EDID, Entry_FULL

   B) New KYWD two-file export:
        kywd main  — FormID, EDID, FULL_Name, …
        kywd refs  — KeywordFormID, KeywordEDID, RefIndex,
                     RefFormID, RefEDID, RefSignature
      The refs table joins each keyword to its parent FLST.
   ========================================================= */

function buildBuildInspirationJson(kywdOrFlstPath, kywd_refs_path_or_outPath, outPathOrUndefined) {
  // Detect which overload was called:
  //   2-arg legacy : buildBuildInspirationJson(flstPath, outPath)
  //   3-arg new    : buildBuildInspirationJson(kywd_main, kywd_refs, outPath)
  let outPath;
  let categories;

  const isNewFormat = outPathOrUndefined !== undefined;

  if (isNewFormat) {
    // ── New KYWD two-file format ─────────────────────────────────────────
    const kywd_main_path  = kywdOrFlstPath;
    const kywd_refs_path  = kywd_refs_path_or_outPath;
    outPath               = outPathOrUndefined;

    const { rows: kywd_rows } = parseTSV(readText(kywd_main_path));
    const { rows: refs_rows  } = parseTSV(readText(kywd_refs_path));

    // Build a lookup: EDID -> keyword row (for display names)
    const kywd_by_edid = new Map();
    for (const r of kywd_rows.map(upperKeyed)) {
      const edid = safeText(r.EDID);
      if (edid) kywd_by_edid.set(edid, r);
    }

    // Filter refs to only rows that link a keyword into a BestBuildsTagCategories_* FLST,
    // sorted by RefIndex so tags come out in game-list order.
    const best_refs = refs_rows
      .map(upperKeyed)
      .filter(r =>
        safeText(r.RefSignature).toUpperCase() === "FLST" &&
        safeText(r.RefEDID).startsWith("BestBuildsTagCategories_")
      )
      .sort((a, b) => Number(a.RefIndex) - Number(b.RefIndex));

    // Group refs by the FLST they point at (i.e. by category)
    const byList = new Map();
    for (const r of best_refs) {
      const listEdid = safeText(r.RefEDID);
      if (!byList.has(listEdid)) byList.set(listEdid, []);
      byList.get(listEdid).push(r);
    }

    categories = [];
    for (const [listEdid, listRefs] of byList.entries()) {
      // Derive a human label from the EDID suffix
      // e.g. BestBuildsTagCategories_Biomes -> BIOMES
      const label = listEdid.replace(/^BestBuildsTagCategories_/, "").toUpperCase();

      const tags = listRefs
        .map(r => {
          const edid = safeText(r.KeywordEDID);
          if (!edid) return null;
          const kywd = kywd_by_edid.get(edid);

          // Prefer FULL_Name from the KYWD record, then NNAM_DisplayName,
          // then fall back to the last _-delimited EDID segment.
          let tagLabel =
            safeText(kywd?.FULL_Name) ||
            safeText(kywd?.NNAM_DisplayName) ||
            "";
          if (!tagLabel) {
            const parts = edid.split("_");
            tagLabel = parts[parts.length - 1] || edid;
          }
          tagLabel = tagLabel.replace(/_/g, " ").trim();
          return { edid, label: tagLabel };
        })
        .filter(Boolean);

      // Stable sort A–Z by label
      tags.sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base" }));

      categories.push({ id: listEdid, label, tags });
    }

  } else {
    // ── Legacy FLST single-file format ──────────────────────────────────
    outPath = kywd_refs_path_or_outPath;
    const { rows } = parseTSV(readText(kywdOrFlstPath));
    const best = rows
      .map(upperKeyed)
      .filter(r => safeText(r.FLST_EDID).startsWith("BestBuildsTagCategories_"));

    const byList = new Map();
    for (const r of best) {
      const id = safeText(r.FLST_EDID);
      if (!byList.has(id)) byList.set(id, []);
      byList.get(id).push(r);
    }

    categories = [];
    for (const [listEdid, listRows] of byList.entries()) {
      const flstFull = safeText(listRows[0]?.FLST_FULL);
      const label = flstFull ? flstFull : listEdid.replace(/^BestBuildsTagCategories_/, "").toUpperCase();

      const tags = listRows
        .filter(r => safeText(r.Entry_EDID))
        .map(r => {
          const edid = safeText(r.Entry_EDID);
          const full = safeText(r.Entry_FULL);
          let tagLabel = full;
          if (!tagLabel) {
            const parts = edid.split("_");
            tagLabel = parts.length ? parts[parts.length - 1] : edid;
          }
          tagLabel = tagLabel.replace(/_/g, " ").trim();
          return { edid, label: tagLabel };
        });

      tags.sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base" }));
      categories.push({ id: listEdid, label, tags });
    }
  }

  // Stable sort categories by label (both paths)
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

function classifyArmoItem(edid, keywordsFlat, flagLabels) {
  const upEdid = safeText(edid).toUpperCase();
  const kw = safeText(keywordsFlat);
  const labs = Array.isArray(flagLabels) ? flagLabels : [];

  // Keyword-based (most reliable when present)
  if (kw.includes("ObjectTypeUnderarmor") || kw.includes("ObjectTypeUnderArmor")) return "UNDERARMOR";
  if (kw.includes("ArmorLight") || kw.includes("ArmorMedium") || kw.includes("ArmorHeavy")) return "ARMOR";
  if (kw.includes("ArmorOutfit") || upEdid.includes("OUTFIT")) return "OUTFIT";

  // Flag-label fallback (handles items with empty keywords)
  const hasU = labs.some(l => l.startsWith("[U]"));
  const hasA = labs.some(l => l.startsWith("[A]"));
  if (hasU && !hasA) return "UNDERARMOR";
  if (hasA && !hasU) return "ARMOR";
  // Mixed [U]+[A] = power armor skeleton, skip
  if (hasU && hasA) return "OTHER";

  // EDID-based
  if (upEdid.includes("HEADWEAR")) return "HEADWEAR";
  if (upEdid.includes("CLOTHES")) return "CLOTHES";
  if (kw.toLowerCase().includes("backpack") || upEdid.includes("BACKPACK")) return "BACKPACK";

  // Ring: flagLabels contains Ring but no body/armour/underarmour slots
  if (labs.includes("Ring") && !labs.some(l =>
    l === "BODY" || l === "Coverall" || l.startsWith("[U]") || l.startsWith("[A]")
  )) return "RING";

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

// Strip body-part suffixes from an armour FULL name to get the set display name.
// e.g. "Leather Left Leg" → "Leather",  "Marine Armor Chest Piece" → "Marine Armor"
function extractArmorSetDisplayName(full) {
  let s = safeText(full).trim();
  if (!s) return s;
  s = s.replace(/\s+chest\s+piece\s*$/i, "");
  s = s.replace(/\s+(arm|leg)\s+(left|right)\s*$/i, "");
  s = s.replace(/\s+(left|right)\s+(arm|leg)\s*$/i, "");
  s = s.replace(/\s+(torso|chest|helmet|head)\s*$/i, "");
  s = s.replace(/\s+(left|right)\s*$/i, "");
  s = s.replace(/\s+(arm|leg)\s*$/i, "");
  return s.trim();
}

function buildEntmSkinItems(entmPath) {
  if (!entmPath || !fs.existsSync(entmPath)) return [];
  const { rows } = parseTSV(readText(entmPath));
  const items = [];
  for (const r of rows.map(upperKeyed)) {
    const edid = safeText(r.EDID).toUpperCase();
    const name = safeText(r.NNAM) || safeText(r.FULL);
    if (!name) continue;

    if (edid.includes("SKIN_PIPBOY") || edid.includes("PIPBOYSKIN")) {
      items.push({
        formId: safeText(r.FORMID), edid: safeText(r.EDID), full: name,
        flags: ["43"], flagLabels: ["Pipboy"],
        keywords: [], type: "PIPBOY", armorSetKey: ""
      });
    } else if (edid.includes("ENTM_SKIN_BACKPACK") || edid.includes("SKIN_BACKPACK")) {
      items.push({
        formId: safeText(r.FORMID), edid: safeText(r.EDID), full: name,
        flags: ["46"], flagLabels: ["Backpack"],
        keywords: [], type: "BACKPACK", armorSetKey: ""
      });
    }
  }
  return items;
}

function buildOutfitInspirationJson(armoPath, outPath, entmPath) {
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
        type: classifyArmoItem(edid, r.Keywords_EDID_Flat, labels),
        armorSetKey: ""
      };
    })
    // Must have EDID and not junk
    .filter(it => it.edid && !isJunkEdid(it.edid))
    // Must have a real player-facing display name (no empty or EDID-like FULL)
    .filter(it => {
      const f = it.full.trim();
      if (!f) return false;
      // Reject if name has underscores but no spaces (EDID leaked as display name)
      if (f.includes("_") && !f.includes(" ")) return false;
      return true;
    })
    // Only your pools
    .filter(it => {
      const up = it.edid.toUpperCase();
      return up.includes("HEADWEAR") || up.includes("CLOTHES") || up.includes("OUTFIT") ||
        it.type === "UNDERARMOR" || it.type === "ARMOR" ||
        it.type === "BACKPACK"   || it.type === "RING";
    });

  // Add armorSetKey and setName for ARMOR type
  for (const it of items) {
    if (it.type === "ARMOR") {
      const setDisplayName = extractArmorSetDisplayName(it.full);
      // setName is the display label shown in the calculator (e.g. "Leather")
      it.setName = setDisplayName || it.full;
      // armorSetKey is used for same-set compatibility matching
      it.armorSetKey = setDisplayName || deriveArmorSetKey(it.edid, it.full);
    }
  }

  const pipboyItems = buildEntmSkinItems(entmPath);
  const allItems = [...items, ...pipboyItems];

  const out = {
    generatedAt: new Date().toISOString(),
    kind: "outfit_inspiration",
    items: allItems
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

// CondProxy items have an empty CNAM_FULL in the TSV, so craftedName and components
// can't be derived automatically. Override them here keyed by COBJ_EDID.
//
// Also used for skin/mod items whose COBJ only stores the mod materials, not the
// base weapon that must be crafted first. Add those here so the resolver can build
// the correct full crafting chain (e.g. Combat Knife → Garden Trowel Knife).
const CONDPROXY_OVERRIDES = {
  "SSE_workshop_co_CondProxy_Displays_SmallGlazedPot":  { craftedName: "Small Glazed Pot",  components: [{ name: "Ceramic", qty: 3 }] },
  "SSE_workshop_co_CondProxy_Displays_MediumGlazedPot": { craftedName: "Medium Glazed Pot", components: [{ name: "Ceramic", qty: 3 }] },
  "SSE_workshop_co_CondProxy_Displays_LargeGlazedPot":  { craftedName: "Large Glazed Pot",  components: [{ name: "Ceramic", qty: 3 }] },
  "workshop_co_CondProxy_HoneyBeastTube":               { craftedName: "Honey Beast Tube",  components: [{ name: "Aluminum", qty: 10 }] },

  // Skin mod — the TSV only records the skin materials; base weapon must also be crafted.
  "SSE_co_mod_CombatKnife_Melee_GardenTrowel": {
    craftedName: "Garden Trowel Knife",
    components: [
      { name: "Combat Knife", qty: 1 },
      { name: "Steel",        qty: 1 },
      { name: "Wood",         qty: 1 },
    ]
  },
};

// Base weapon / item recipes referenced by skin overrides above.
// These are NOT Big Bloom rewards and won't appear in the dropdown, but they are
// loaded into the dependency resolver so crafting chains show the full step list.
const SKIN_WEAPON_PREREQS = [
  {
    craftedName: "Combat Knife",
    cobjEdid:    "co_Weapon_Melee_Knife",
    components:  [
      { name: "Adhesive", qty: 1 },
      { name: "Rubber",   qty: 3 },
      { name: "Steel",    qty: 2 },
      { name: "Screw",    qty: 1 },
    ]
  },
];

function buildBigBloomCraftingJson(cobjPath, outPath) {
  const { rows } = parseTSV(readText(cobjPath));

  // Attach image URLs (keyed by Created Object FULL name)
  // We also use this as an "allow list" so Big Bloom doesn't accidentally ingest the whole game.
  const imageMap = loadBigBloomImageMap();

  // Also always include CondProxy override names so their entries pass the filter.
  const condproxyNames = new Set(
    Object.values(CONDPROXY_OVERRIDES).map(v => v.craftedName.toLowerCase())
  );

  const imageKeys = new Set([
    ...Object.keys(imageMap).map(k => safeText(k).toLowerCase()).filter(Boolean),
    ...condproxyNames
  ]);

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
      // Strip outer quotes from cnamFull — xEdit exports "" for empty fields which is truthy in JS
      const cnamFull = stripQuotes(pickCol(r, [
        "CNAM_FULL",
        "CNAM - Created Object",
        "CNAM",
        "Created Object",
        "Created Object FULL"
      ]));

      // CNAM_FULL in your TSV often contains: EDID "Display Name" [FORM:ID]
      // We MUST extract the quoted display name first so it matches big_bloom_images.json keys.
      // CondProxy items have empty CNAM_FULL — fall back to the CONDPROXY_OVERRIDES name.
      const craftedName =
        extractQuotedName(cnamFull) ||
        safeText(cnamFull) ||
        safeText(pickCol(r, ["HNAM - Build Group Name", "HNAM_BuildGroupName", "Build Group Name"])) ||
        stripQuotes(pickCol(r, ["CNAM_EDID"])) ||
        (CONDPROXY_OVERRIDES[stripQuotes(edid)] ? CONDPROXY_OVERRIDES[stripQuotes(edid)].craftedName : "");

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

      // Strip outer quotes from cnamFullRaw — xEdit exports "" for empty fields which is truthy in JS
      const cnamFullRaw = stripQuotes(pickCol(r, ["CNAM_FULL", "CNAM - Created Object", "CNAM", "Created Object"]));
      const cnamEdid = stripQuotes(pickCol(r, ["CNAM_EDID", "CNAM_EDID - Editor ID", "CNAM - EDID", "Created Object EDID"]));
      // Same rule as the filter: extract the quoted display name first.
      // CondProxy items have empty CNAM_FULL — fall back to the CONDPROXY_OVERRIDES name.
      const craftedName =
        extractQuotedName(cnamFullRaw) ||
        safeText(cnamFullRaw) ||
        safeText(pickCol(r, ["HNAM - Build Group Name", "HNAM_BuildGroupName", "Build Group Name"])) ||
        safeText(cnamEdid) ||
        (CONDPROXY_OVERRIDES[stripQuotes(cobjEdid)] ? CONDPROXY_OVERRIDES[stripQuotes(cobjEdid)].craftedName : "");

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
        components: (CONDPROXY_OVERRIDES[stripQuotes(cobjEdid)] && CONDPROXY_OVERRIDES[stripQuotes(cobjEdid)].components)
          ? CONDPROXY_OVERRIDES[stripQuotes(cobjEdid)].components
          : parseFVPA(pickCol(r, ["FVPA", "FVPA - Components (sorted)", "Components", "Components (sorted)"])),
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

  // Register skin weapon prereqs into the resolver map so dependency chains can
  // reference them (e.g. "Combat Knife" inside Garden Trowel Knife's components).
  // These are NOT added to recs and will NOT appear in the dropdown.
  for (const prereq of SKIN_WEAPON_PREREQS) {
    const key = safeText(prereq.craftedName).toLowerCase();
    if (key && !recipeByCraftedName.has(key)) recipeByCraftedName.set(key, prereq);
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
    // Base weapon/item recipes needed to resolve skin crafting chains.
    // Not shown in the dropdown — loaded into the resolver map only.
    prereqRecipes: SKIN_WEAPON_PREREQS,
    index: {
      byCraftedNameLower: Object.fromEntries(
        Array.from(recipeByCraftedName.entries()).map(([k, v]) => [k, v.cobjEdid || ""])
      )
    }
  };

  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
}

/* =========================================================
   MAIN
   ========================================================= */

// Build inspiration input — three accepted formats, checked in priority order:
//   1. FLST_ENTRIES_TSV  — new three-file split: use the *_Entries file
//                          (columns: FLST_FormID, FLST_EDID, FLST_FULL,
//                           EntryIndex, Entry_Sig, Entry_FormID, Entry_EDID, Entry_FULL)
//   2. KYWD_TSV + KYWD_REFS_TSV — KYWD two-file format from last export
//   3. FLST_TSV          — legacy single-file FLST export (original format)
const FLST_ENTRIES_TSV = process.env.FLST_ENTRIES_TSV || "";
const KYWD_TSV         = process.env.KYWD_TSV         || "";
const KYWD_REFS_TSV    = process.env.KYWD_REFS_TSV    || "";
const FLST_TSV         = process.env.FLST_TSV         || "";

const ARMO_TSV = process.env.ARMO_BOD2_TSV || "";
const ENTM_TSV = process.env.ENTM_TSV || "";
const COBJ_TSV = process.env.COBJ_TSV || "";

const OUT_DIR = process.env.OUT_DIR || "dist/calculators";

const hasBuildInput = FLST_ENTRIES_TSV || (KYWD_TSV && KYWD_REFS_TSV) || FLST_TSV;
if (!hasBuildInput || !ARMO_TSV || !COBJ_TSV) {
  console.error(
    "Missing env vars.\n" +
    "  Build inspiration (pick one):\n" +
    "    FLST_ENTRIES_TSV              — new three-file split (Entries file)\n" +
    "    KYWD_TSV + KYWD_REFS_TSV      — KYWD two-file format\n" +
    "    FLST_TSV                      — legacy single-file FLST\n" +
    "  Always required: ARMO_BOD2_TSV, COBJ_TSV\n" +
    "  Optional:        ENTM_TSV, OUT_DIR"
  );
  process.exit(1);
}

ensureDir(OUT_DIR);

if (FLST_ENTRIES_TSV) {
  // New three-file split: Entries file has the same columns as the legacy FLST,
  // so the original 2-arg function handles it directly.
  buildBuildInspirationJson(FLST_ENTRIES_TSV, path.join(OUT_DIR, "build_inspiration.json"));
} else if (KYWD_TSV && KYWD_REFS_TSV) {
  // KYWD two-file format
  buildBuildInspirationJson(KYWD_TSV, KYWD_REFS_TSV, path.join(OUT_DIR, "build_inspiration.json"));
} else {
  // Legacy single FLST file
  buildBuildInspirationJson(FLST_TSV, path.join(OUT_DIR, "build_inspiration.json"));
}
buildOutfitInspirationJson(ARMO_TSV, path.join(OUT_DIR, "outfit_inspiration.json"), ENTM_TSV);
buildBigBloomCraftingJson(COBJ_TSV, path.join(OUT_DIR, "big_bloom_crafting.json"));

console.log("Built calculators JSON into:", OUT_DIR);