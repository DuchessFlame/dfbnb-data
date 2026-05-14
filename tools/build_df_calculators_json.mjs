// scripts/build_df_calculators_json.mjs
import fs from "fs";
import path from "path";
import { execFileSync } from "child_process";

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

// Convert a component EDID (or keyword EDID) into a clean display name.
//   c_Cloth                            -> "Cloth"
//   c_NuclearMaterial                  -> "Nuclear Material"
//   SSE_Tier5_Gigablossom_MiscItem     -> "Gigablossom"
//   SSE_Tier1_CarnalWeeper_MiscItem    -> "Carnal Weeper"
//   SSE_Tier2_GreenInvader_MiscItem    -> "Green Invader"
function prettifyComponentEdid(raw) {
  let n = String(raw || "").trim();
  if (!n) return "";

  if (/^c_/.test(n)) {
    n = n.replace(/^c_/, "");
  } else {
    const m = n.match(/^SSE_Tier\d+_(.+?)_MiscItem$/);
    if (m) {
      n = m[1];
    } else {
      n = n.replace(/^SSE_/, "").replace(/_MiscItem$/, "");
    }
  }

  n = n.replace(/_/g, " ")
       .replace(/([a-z])([A-Z])/g, "$1 $2")
       .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2")
       .replace(/\s+/g, " ")
       .trim();
  return n;
}

function parseFVPA(fvpaRaw) {
  // FVPA cell format from xEdit COBJ export:
  //   <edid>:<qty>:<groupOrEmpty>   joined by " | "
  // e.g.  c_Cloth:3:  |  SSE_Tier5_Gigablossom_MiscItem:5:
  //       c_NuclearMaterial:1:COBJ_Workshop_NuclearMaterial
  // Older / hand-edited inputs may use "<name>:<qty>" without the trailing
  // group colon — handled by the fallback regex below.

  let t = safeText(fvpaRaw).replace(/^"+|"+$/g, "").trim();
  if (!t) return [];

  return t
    .split("|")
    .map(x => x.trim())
    .filter(Boolean)
    .map(part => {
      let edid = "";
      let qty  = 0;

      let m = part.match(/^(.+?):\s*(\d+)\s*:\s*[^:]*$/);
      if (m) {
        edid = m[1].trim();
        qty  = parseInt(m[2], 10) || 0;
      } else {
        m = part.match(/^(.+?):\s*(\d+)\s*$/);
        if (m) {
          edid = m[1].trim();
          qty  = parseInt(m[2], 10) || 0;
        } else {
          edid = part.trim();
          qty  = 1;
        }
      }

      const quoted = extractQuotedName(edid);
      if (quoted) edid = quoted;
      edid = edid.replace(/^"+|"+$/g, "").replace(/^'+|'+$/g, "").trim();
      edid = edid.replace(/\s*\[[^\]]+\]\s*$/g, "").trim();

      const name = prettifyComponentEdid(edid) || edid;
      return { name, qty };
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

// Extract pip-boy skins from ENTM (they are NOT in COBJ).
function buildEntmPipboySkins(entmPath) {
  if (!entmPath || !fs.existsSync(entmPath)) return [];
  const { rows } = parseTSV(readText(entmPath));
  const items = [];
  for (const r of rows.map(upperKeyed)) {
    const edidRaw = pickCol(r, ["EDID", "EDID - Editor ID", "Editor ID"]);
    const edid = edidRaw.toUpperCase();
    const name = pickCol(r, ["NNAM", "NNAM - Name", "NNAM_DisplayName"]) || pickCol(r, ["FULL", "FULL - Name", "FULL_Name"]);
    if (!name || isJunkEdid(edidRaw)) continue;
    if (edid.includes("SKIN_PIPBOY") || edid.includes("PIPBOYSKIN")) {
      items.push({
        formId: pickCol(r, ["FORMID", "FormID", "Form ID", "Record Header FormID"]), edid: edidRaw, full: name,
        flags: ["43"], flagLabels: ["Pipboy"],
        keywords: [], type: "PIPBOY", armorSetKey: ""
      });
    }
  }
  return items;
}

// Build the outfit inspiration JSON.
// Primary source: COBJ — every craftable outfit/armour/backpack/flair item.
// COBJ CNAM_EDID is cross-referenced against ARMO SLOTS to get BOD2 flag labels,
// which drive slot classification. COBJ CNAM_FULL is the player-visible name.
// Pip-boy skins are not in COBJ so they still come from ENTM.
// Rings have no COBJ recipe so they come directly from ARMO SLOTS.
function buildOutfitInspirationJson(armoPath, outPath, entmPath, cobjPath) {
  // Build ARMO SLOTS lookup map: ARMO_EDID -> { full, flags, flagLabels }
  // Use pickCol with fallback column names so re-exported TSVs still work.
  const { rows: armoRows } = parseTSV(readText(armoPath));
  const slotsMap = new Map();
  for (const r of armoRows.map(upperKeyed)) {
    const edid = pickCol(r, ["ARMO_EDID", "EDID", "EDID - Editor ID", "Editor ID"]);
    if (!edid) continue;
    const flagLabels = splitPipe(pickCol(r, ["BOD2_FirstPersonFlagLabels", "BOD2 - Biped Body Template\\First Person Flags - Labels", "BOD2_FirstPersonFlags_Labels", "First Person Flag Labels"])).map(x => x.trim()).filter(Boolean);
    const flags = splitPipe(pickCol(r, ["BOD2_FirstPersonFlags", "BOD2 - Biped Body Template\\First Person Flags", "First Person Flags"])).map(x => x.trim()).filter(Boolean);
    const full = pickCol(r, ["ARMO_FULL", "FULL", "FULL - Name", "FULL_Name"]);
    slotsMap.set(edid, { full, flags, flagLabels });
  }

  // Classify an item from its flag labels + EDID
  function classify(cnamEdid, flagLabels) {
    const upEdid = (cnamEdid || "").toUpperCase();
    const labs = flagLabels || [];
    const hasU = labs.some(l => l.startsWith("[U]"));
    const hasA = labs.some(l => l.startsWith("[A]"));
    if (hasU && hasA) return "OTHER";
    if (hasU) return "UNDERARMOR";
    // Exclude power armour — they use [A] slots but are not wearable body armour
    if (hasA && upEdid.includes("POWERARMOR")) return "OTHER";
    if (hasA) return "ARMOR";
    // Coverall flag = definitive clothes/outfit slot (flag 57)
    if (labs.includes("Coverall")) return "CLOTHES";
    // Head/face flags = headwear (even if EDID says "Clothes_*")
    const HEAD_FLAGS = ["Hair Top","Scalp","Hair Long","Headband","Eyes","EyeOfRa","Beard","Mouth","Neck","FaceGen Head"];
    if (labs.some(l => HEAD_FLAGS.includes(l)) || upEdid.includes("HEADWEAR")) return "HEADWEAR";
    // EDID-based fallbacks
    if (upEdid.includes("CLOTHES") || upEdid.includes("OUTFIT")) return "CLOTHES";
    if (labs.includes("Backpack")) return "BACKPACK";
    if (labs.includes("Pipboy"))   return "PIPBOY";
    if (labs.includes("Ring") && !labs.some(l =>
      l === "BODY" || l.startsWith("[U]") || l.startsWith("[A]"))) return "RING";
    return "OTHER";
  }

  // Parse COBJ - use as primary source
  const { rows: cobjRows } = parseTSV(readText(cobjPath));
  const items = [];
  const seenCobjEdid = new Set();
  const MOD_KEYWORDS = ["EFFECT","LINING","REPAIR","PLATED","INSULATED","REFRIGERATED",
    "CAPACITY","PILLAGER","GROCER","CHEMIST","MISCFLAIR","SOUVENIR","DISPLAY","DEPRECATED"];

  for (const r of cobjRows.map(upperKeyed)) {
    const cobjEdid = stripQuotes(pickCol(r, ["COBJ_EDID", "EDID", "EDID - Editor ID", "Editor ID"]));
    const cnamEdid = stripQuotes(pickCol(r, ["CNAM_EDID", "CNAM - Created Object\\EDID", "CNAM_EDID - Editor ID", "CNAM - EDID", "Created Object EDID"]));
    const cnamFull = stripQuotes(pickCol(r, ["CNAM_FULL", "CNAM - Created Object\\FULL", "CNAM - Created Object", "CNAM", "Created Object", "Created Object FULL"]));
    const upCobj = cobjEdid.toUpperCase();

    if (!cobjEdid || isJunkEdid(cobjEdid)) continue;
    if (seenCobjEdid.has(cobjEdid)) continue;

    // Backpack Flair L (Flair1) — check BEFORE backpack skins so flairs with
    // CO_MOD_BACKPACK in their EDID are not misclassified as backpacks.
    if (upCobj.includes("FLAIR1") || upCobj.match(/_FLAIR_1[^0-9]/)) {
      const name = cnamFull;
      seenCobjEdid.add(cobjEdid);
      if (name) items.push({
        formId: "", edid: cobjEdid, full: name,
        flags: ["55"], flagLabels: ["Flair L"],
        keywords: [], type: "FLAIR_L", armorSetKey: ""
      });
      continue;
    }

    // Backpack Flair R (Flair2)
    if (upCobj.includes("FLAIR2") || upCobj.match(/_FLAIR_2[^0-9]/)) {
      const name = cnamFull;
      seenCobjEdid.add(cobjEdid);
      if (name) items.push({
        formId: "", edid: cobjEdid, full: name,
        flags: ["56"], flagLabels: ["Flair R"],
        keywords: [], type: "FLAIR_R", armorSetKey: ""
      });
      continue;
    }

    // Backpack skins: COBJ_EDID contains CO_ARMOR_BACKPACK or CO_MOD_BACKPACK (skin mods)
    // Placed after flair checks so flair items are not misclassified as backpacks.
    const isBackpackSkin = upCobj.includes("CO_ARMOR_BACKPACK") ||
      (upCobj.includes("CO_MOD_BACKPACK") && !MOD_KEYWORDS.some(k => upCobj.includes(k)));
    if (isBackpackSkin) {
      const name = cnamFull;
      if (name && !name.toLowerCase().startsWith("plan:")) {
        seenCobjEdid.add(cobjEdid);
        items.push({
          formId: "", edid: cobjEdid, full: name,
          flags: ["54"], flagLabels: ["Backpack"],
          keywords: [], type: "BACKPACK", armorSetKey: ""
        });
      } else { seenCobjEdid.add(cobjEdid); }
      continue;
    }

    // Wearable items: resolve via ARMO SLOTS cross-reference
    if (!cnamEdid || !cnamFull) continue;
    const slot = slotsMap.get(cnamEdid);
    if (!slot) continue; // not an ARMO record

    const type = classify(cnamEdid, slot.flagLabels);
    if (type === "OTHER" || type === "BACKPACK" || type === "PIPBOY") {
      seenCobjEdid.add(cobjEdid); continue;
    }

    const f = cnamFull.trim();
    if (!f || (f.includes("_") && !f.includes(" "))) { seenCobjEdid.add(cobjEdid); continue; }

    // Skip items with no BOD2 slot data - they can't fill any display slot
    if (!slot.flagLabels.length && !["RING"].includes(type)) { seenCobjEdid.add(cobjEdid); continue; }

    seenCobjEdid.add(cobjEdid);
    items.push({
      formId: "", edid: cnamEdid, full: f,
      flags: slot.flags, flagLabels: slot.flagLabels,
      keywords: [], type, armorSetKey: ""
    });
  }

  // Rings: only 2 in game (Wedding Ring, Old Ring), no COBJ recipe — pull from ARMO SLOTS
  for (const r of armoRows.map(upperKeyed)) {
    const edid = pickCol(r, ["ARMO_EDID", "EDID", "EDID - Editor ID", "Editor ID"]);
    const full = pickCol(r, ["ARMO_FULL", "FULL", "FULL - Name", "FULL_Name"]);
    if (!edid || isJunkEdid(edid)) continue;
    const flagLabels = splitPipe(pickCol(r, ["BOD2_FirstPersonFlagLabels", "BOD2 - Biped Body Template\\First Person Flags - Labels", "BOD2_FirstPersonFlags_Labels", "First Person Flag Labels"])).map(x => x.trim()).filter(Boolean);
    if (!flagLabels.includes("Ring")) continue;
    if (flagLabels.some(l => l.startsWith("[U]") || l.startsWith("[A]") || l === "BODY")) continue;
    const f = full.trim();
    if (!f || (f.includes("_") && !f.includes(" "))) continue;
    items.push({
      formId: pickCol(r, ["ARMO_FormID", "FormID", "Form ID", "Record Header FormID"]), edid, full: f,
      flags: splitPipe(pickCol(r, ["BOD2_FirstPersonFlags", "BOD2 - Biped Body Template\\First Person Flags", "First Person Flags"])).map(x => x.trim()).filter(Boolean),
      flagLabels, keywords: [], type: "RING", armorSetKey: ""
    });
  }

  // Set name/key for ARMOR
  for (const it of items) {
    if (it.type === "ARMOR") {
      const setDisplayName = extractArmorSetDisplayName(it.full);
      it.setName = setDisplayName || it.full;
      it.armorSetKey = setDisplayName || deriveArmorSetKey(it.edid, it.full);
    }
  }

  // Pip-boy skins from ENTM (not in COBJ)
  const pipboyItems = buildEntmPipboySkins(entmPath);
  const allItems = [...items, ...pipboyItems];

  const typeCounts = {};
  for (const it of allItems) typeCounts[it.type] = (typeCounts[it.type] || 0) + 1;
  console.log("outfit_inspiration item counts:", typeCounts);

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
const CONDPROXY_OVERRIDES = {
  "SSE_workshop_co_CondProxy_Displays_SmallGlazedPot":  { craftedName: "Small Glazed Pot",  components: [{ name: "Ceramic", qty: 3 }] },
  "SSE_workshop_co_CondProxy_Displays_MediumGlazedPot": { craftedName: "Medium Glazed Pot", components: [{ name: "Ceramic", qty: 3 }] },
  "SSE_workshop_co_CondProxy_Displays_LargeGlazedPot":  { craftedName: "Large Glazed Pot",  components: [{ name: "Ceramic", qty: 3 }] },
  "workshop_co_CondProxy_HoneyBeastTube":               { craftedName: "Honey Beast Tube",  components: [{ name: "Aluminum", qty: 10 }] },

  // Skin mod — TSV only records skin materials; base weapon must also be crafted first.
  "SSE_co_mod_CombatKnife_Melee_GardenTrowel": {
    craftedName: "Garden Trowel Knife",
    components: [
      { name: "Combat Knife", qty: 1 },
      { name: "Steel",        qty: 1 },
      { name: "Wood",         qty: 1 },
    ]
  },
};

// Base weapon recipes referenced as prerequisites by skin overrides above.
// NOT added to the dropdown — only loaded into the resolver so dependency chains work.
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

// COBJ EDIDs to explicitly exclude from the Big Bloom calculator. These are
// COBJ rows whose CNAM points at a leveled list (LL_*) rather than a real
// ARMO/MISC item, so they have no display name, no real components, and
// shouldn't appear in the user-facing dropdown.
const EXCLUDE_COBJ_EDIDS = new Set([
  "SSE_workshop_co_Displays_HybridFlowers"
]);

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

      // Explicit exclude list (e.g. COBJ rows that point at LL_* leveled lists
      // rather than real items — no display name, junk in the dropdown).
      if (EXCLUDE_COBJ_EDIDS.has(stripQuotes(edid))) return false;

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

  // Register skin weapon prereqs so the resolver can expand their crafting chains.
  // These are NOT in recs and will NOT appear in the dropdown.
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
    prereqRecipes: SKIN_WEAPON_PREREQS,
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
   4) SCORE PROGRESSION
   Reads fallout76_seasons.tsv and picks the current season
   (the row whose EndDate is furthest in the future, i.e. the
   last row in chronological order).
   Outputs a tiny JSON used by the S.C.O.R.E. Progression
   Calculator to display the correct season name and dates.
   ========================================================= */

function parseSeasonDate(raw) {
  // TSV dates are D/MM/YYYY  e.g. "3/03/2026"  or  "30/06/2020"
  const t = String(raw || "").trim();
  if (!t) return null;
  const parts = t.split("/");
  if (parts.length !== 3) return null;
  const [d, m, y] = parts.map(Number);
  if (!d || !m || !y) return null;
  // Build ISO date string YYYY-MM-DD
  return `${y}-${String(m).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
}

function buildScoreProgressionJson(seasonsTsvPath, outPath) {
  const { rows } = parseTSV(readText(seasonsTsvPath));

  if (!rows.length) throw new Error("fallout76_seasons.tsv is empty");

  // Pick the row with the latest EndDate — that is always the current/upcoming season.
  // Rows are chronological so we can just take the last non-empty one, but we parse
  // and compare properly for safety.
  let best = null;
  let bestEnd = "";

  for (const r of rows.map(upperKeyed)) {
    const endIso = parseSeasonDate(r.EndDate);
    if (!endIso) continue;
    if (!best || endIso > bestEnd) {
      best    = r;
      bestEnd = endIso;
    }
  }

  if (!best) throw new Error("No valid season row found in fallout76_seasons.tsv");

  const startIso = parseSeasonDate(best.StartDate);
  const endIso   = parseSeasonDate(best.EndDate);
  const number   = parseInt(safeText(best.SeasonNumber), 10) || null;
  const name     = safeText(best.SeasonName);
  const days     = parseInt(safeText(best.Days), 10) || null;

  const out = {
    generatedAt:  new Date().toISOString(),
    kind:         "score_progression",
    season: {
      number,
      name,
      startDate:  startIso,
      endDate:    endIso,
      days,
    }
  };

  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log(`  score_progression.json  ->  Season ${number}: ${name}  (${startIso} – ${endIso})`);
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
const COBJ_TSV    = process.env.COBJ_TSV    || "";
const SEASONS_TSV = process.env.SEASONS_TSV || "";

const OUT_DIR = process.env.OUT_DIR || "dist/calculators";

const hasBuildInput = FLST_ENTRIES_TSV || (KYWD_TSV && KYWD_REFS_TSV) || FLST_TSV;
if (!SEASONS_TSV) console.warn("Warning: SEASONS_TSV not set — score_progression.json will not be built.");
if (!hasBuildInput || !ARMO_TSV || !COBJ_TSV) {
  console.error(
    "Missing env vars.\n" +
    "  Build inspiration (pick one):\n" +
    "    FLST_ENTRIES_TSV              — new three-file split (Entries file)\n" +
    "    KYWD_TSV + KYWD_REFS_TSV      — KYWD two-file format\n" +
    "    FLST_TSV                      — legacy single-file FLST\n" +
    "  Always required: ARMO_BOD2_TSV, COBJ_TSV\n" +
    "  Optional:        ENTM_TSV, SEASONS_TSV, OUT_DIR"
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
buildOutfitInspirationJson(ARMO_TSV, path.join(OUT_DIR, "outfit_inspiration.json"), ENTM_TSV, COBJ_TSV);
buildBigBloomCraftingJson(COBJ_TSV, path.join(OUT_DIR, "big_bloom_crafting.json"));
if (SEASONS_TSV) buildScoreProgressionJson(SEASONS_TSV, path.join(OUT_DIR, "score_progression.json"));

/* =========================================================
   PATCHLOGS — build_inspiration + outfit_inspiration
   Each calculator page has its own feed file so build changes
   never appear on the outfit page and vice versa.

   Frontend contract (must match patchlog_utils.py):
     { entries: [ { ts, current, added, removed, changed } ] }

   Build feed:  feed items are flattened "CATEGORY > TAG" rows,
                keyed by tag edid.
   Outfit feed: feed items are the outfit_inspiration items, keyed
                by edid, with descriptive "changed" strings that
                include the slot delta (e.g. "+Hair Top, -Beard").
   ========================================================= */

function nowIso() {
  // Match patchlog_utils.py: UTC ISO-8601, no microseconds.
  return new Date().toISOString().replace(/\.\d{3}Z$/, "+00:00");
}

function gitShowJson(rev, relPath) {
  // Load a JSON file from a previous git revision, relative to repo root.
  // Returns null on failure (missing file, not a git repo, first commit, etc.).
  try {
    const buf = execFileSync("git", ["show", `${rev}:${relPath}`], {
      stdio: ["ignore", "pipe", "ignore"],
      timeout: 30_000,
      maxBuffer: 256 * 1024 * 1024,
    });
    return JSON.parse(buf.toString("utf8"));
  } catch {
    return null;
  }
}

function writeFeedFile(feedPath, entry) {
  const feed = { entries: [entry] };
  ensureDir(path.dirname(feedPath));
  fs.writeFileSync(feedPath, JSON.stringify(feed, null, 2) + "\n");
  console.log(
    `[patchlog] ${path.basename(feedPath)}: current=${entry.current} ` +
    `added=${entry.added.length} removed=${entry.removed.length} changed=${entry.changed.length}`
  );
}

// --- BUILD INSPIRATION DIFF ---------------------------------------------

// Flatten {categories:[{label,tags:[{edid,label}]}]} into
// [{key: edid, name: "CATEGORY > TAG"}]
function flattenBuildInspiration(data) {
  if (!data || !Array.isArray(data.categories)) return [];
  const out = [];
  for (const cat of data.categories) {
    const catLabel = String(cat?.label || cat?.id || "").trim() || "UNKNOWN";
    const tags = Array.isArray(cat?.tags) ? cat.tags : [];
    for (const t of tags) {
      const edid = String(t?.edid || "").trim();
      if (!edid) continue;
      const tagLabel = String(t?.label || edid).trim();
      out.push({ key: edid, name: `${catLabel} > ${tagLabel}` });
    }
  }
  return out;
}

function diffBuildInspiration(prevData, currData) {
  const curr = flattenBuildInspiration(currData);
  const currByKey = new Map(curr.map(x => [x.key, x]));

  // First-run guard: no previous data in git yet. Emit a clean empty diff
  // with the current count so the feed isn't flooded with thousands of
  // "added" rows on the initial build. Subsequent builds will diff normally.
  if (prevData == null) {
    return {
      ts: nowIso(),
      current: currByKey.size,
      added: [],
      removed: [],
      changed: [],
    };
  }

  const prev = flattenBuildInspiration(prevData);
  const prevByKey = new Map(prev.map(x => [x.key, x]));

  const added = [];
  const removed = [];
  const changed = [];

  for (const [k, v] of currByKey) {
    if (!prevByKey.has(k)) added.push(v.name);
  }
  for (const [k, v] of prevByKey) {
    if (!currByKey.has(k)) removed.push(v.name);
  }
  for (const [k, v] of currByKey) {
    const p = prevByKey.get(k);
    if (p && p.name !== v.name) {
      // Label rename — surface both so the reader can see the change.
      changed.push(`${p.name} → ${v.name}`);
    }
  }

  const cap = arr => arr.slice().sort((a, b) => a.localeCompare(b)).slice(0, 500);

  return {
    ts: nowIso(),
    current: currByKey.size,
    added: cap(added),
    removed: cap(removed),
    changed: cap(changed),
  };
}

// --- OUTFIT INSPIRATION DIFF --------------------------------------------

function outfitItemName(it) {
  const full = String(it?.full || "").trim();
  if (full) return full;
  const edid = String(it?.edid || "").trim();
  return edid || "Unknown";
}

function outfitItemKey(it) {
  // formId is usually empty on outfit items, so key on edid.
  const edid = String(it?.edid || "").trim();
  if (edid) return edid;
  return String(it?.formId || "").trim();
}

function asSet(arr) {
  return new Set((arr || []).map(v => String(v)));
}

function describeSlotDelta(prev, curr) {
  // Prefer human-readable flagLabels; fall back to numeric flags.
  const prevLabels = asSet(prev?.flagLabels);
  const currLabels = asSet(curr?.flagLabels);
  let addedSlots = [...currLabels].filter(x => !prevLabels.has(x));
  let removedSlots = [...prevLabels].filter(x => !currLabels.has(x));

  if (!addedSlots.length && !removedSlots.length) {
    const prevFlags = asSet(prev?.flags);
    const currFlags = asSet(curr?.flags);
    addedSlots = [...currFlags].filter(x => !prevFlags.has(x));
    removedSlots = [...prevFlags].filter(x => !currFlags.has(x));
  }

  const parts = [];
  if (addedSlots.length) parts.push("+" + addedSlots.sort().join(", +"));
  if (removedSlots.length) parts.push("-" + removedSlots.sort().join(", -"));
  return parts.length ? `slots: ${parts.join(", ")}` : "";
}

function describeOutfitChange(prev, curr) {
  // Builds a human-readable reason string. Empty if nothing tracked changed.
  const reasons = [];

  const slotDelta = describeSlotDelta(prev, curr);
  if (slotDelta) reasons.push(slotDelta);

  const prevType = String(prev?.type || "").trim();
  const currType = String(curr?.type || "").trim();
  if (prevType !== currType) {
    reasons.push(`type: ${prevType || "?"} → ${currType || "?"}`);
  }

  const prevSetKey = String(prev?.armorSetKey || "").trim();
  const currSetKey = String(curr?.armorSetKey || "").trim();
  if (prevSetKey !== currSetKey) {
    reasons.push(`set: ${prevSetKey || "?"} → ${currSetKey || "?"}`);
  }

  const prevFull = String(prev?.full || "").trim();
  const currFull = String(curr?.full || "").trim();
  if (prevFull && currFull && prevFull !== currFull) {
    reasons.push(`renamed from "${prevFull}"`);
  }

  const prevKw = asSet(prev?.keywords);
  const currKw = asSet(curr?.keywords);
  const addedKw = [...currKw].filter(x => !prevKw.has(x));
  const removedKw = [...prevKw].filter(x => !currKw.has(x));
  if (addedKw.length || removedKw.length) {
    const bits = [];
    if (addedKw.length) bits.push("+" + addedKw.sort().join(", +"));
    if (removedKw.length) bits.push("-" + removedKw.sort().join(", -"));
    reasons.push(`keywords: ${bits.join(", ")}`);
  }

  return reasons.join("; ");
}

function diffOutfitInspiration(prevData, currData) {
  const currItems = Array.isArray(currData?.items) ? currData.items : [];
  const currByKey = new Map();
  for (const it of currItems) {
    const k = outfitItemKey(it);
    if (k) currByKey.set(k, it);
  }

  // First-run guard (see diffBuildInspiration for rationale).
  if (prevData == null) {
    return {
      ts: nowIso(),
      current: currByKey.size,
      added: [],
      removed: [],
      changed: [],
    };
  }

  const prevItems = Array.isArray(prevData?.items) ? prevData.items : [];
  const prevByKey = new Map();
  for (const it of prevItems) {
    const k = outfitItemKey(it);
    if (k) prevByKey.set(k, it);
  }

  const added = [];
  const removed = [];
  const changed = [];

  for (const [k, it] of currByKey) {
    if (!prevByKey.has(k)) added.push(outfitItemName(it));
  }
  for (const [k, it] of prevByKey) {
    if (!currByKey.has(k)) removed.push(outfitItemName(it));
  }
  for (const [k, curr] of currByKey) {
    const prev = prevByKey.get(k);
    if (!prev) continue;
    const reason = describeOutfitChange(prev, curr);
    if (reason) {
      changed.push(`${outfitItemName(curr)} (${reason})`);
    }
  }

  const cap = arr => arr.slice().sort((a, b) => a.localeCompare(b)).slice(0, 500);

  return {
    ts: nowIso(),
    current: currByKey.size,
    added: cap(added),
    removed: cap(removed),
    changed: cap(changed),
  };
}

// --- WRITE FEEDS --------------------------------------------------------

function buildInspirationPatchlogs(outDir) {
  const buildRel = path.posix.join(outDir.replace(/\\/g, "/"), "build_inspiration.json");
  const outfitRel = path.posix.join(outDir.replace(/\\/g, "/"), "outfit_inspiration.json");

  const currBuild = JSON.parse(fs.readFileSync(path.join(outDir, "build_inspiration.json"), "utf8"));
  const currOutfit = JSON.parse(fs.readFileSync(path.join(outDir, "outfit_inspiration.json"), "utf8"));

  const prevBuild = gitShowJson("HEAD^", buildRel);
  const prevOutfit = gitShowJson("HEAD^", outfitRel);

  const buildEntry = diffBuildInspiration(prevBuild, currBuild);
  const outfitEntry = diffOutfitInspiration(prevOutfit, currOutfit);

  // Write alongside the data files. A workflow step later copies/renames
  // these into dist/ as patchlog_latest_df_<name>.json so the patchlog
  // manifest can point at them.
  writeFeedFile(path.join(outDir, "build_inspiration_patchlog.json"), buildEntry);
  writeFeedFile(path.join(outDir, "outfit_inspiration_patchlog.json"), outfitEntry);
}

buildInspirationPatchlogs(OUT_DIR);

console.log("Built calculators JSON into:", OUT_DIR);