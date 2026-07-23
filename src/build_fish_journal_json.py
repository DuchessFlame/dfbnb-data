#!/usr/bin/env python3
"""Build dist/fish_journal.json (+ pts twin) for the Field Journal of Appalachia page.

Parses the newest FISH_Export TSV, assigns each catchable fish to a single cascade
tier, and derives a full "field journal entry" for every fish by joining several
exports:

  FISH  -> the fish record: size / region / season / 12 fight stats / keywords /
           splash / FormID / EDID, and the raw-meal reference (FIRI).
  ALCH  -> raw-meal value (the "value" stat) + the cooked meals' food effects,
           reached through the FIRI reference.
  COBJ  -> the recipe chain (raw fish -> Filet -> prepared meals) with ingredients,
           crafting station, and any required challenge/recipe.
  CHAL  -> challenges that reward catching the fish, linked through the CNDF
           condition forms referenced by the raw meal, plus the season-wide
           "Catch a <Season> Seasonal Fish" challenges.
  GLOB  -> numeric magnitude / duration tiers used to render food effects.

The renderer (df-bnb-fishing-journal.js -> renderFieldJournal) consumes this feed
and computes the live "in season" state from seasonRule.rolloverDates.

Everything the entry card shows is generated here so the page is fully data-driven;
the only curated input is TEMPLATE_OVERRIDES (the specific fishing-hole name and the
flavour description, which are not present in the game data).
"""
import csv, re, json, os, glob, datetime

# Some reference columns (ALCH especially) are extremely wide.
csv.field_size_limit(10 * 1024 * 1024)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)                 # dfbnb-data/
TSV_DIR = os.path.join(ROOT, "tsv")
DIST = os.path.join(ROOT, "dist")

IMG_BASE = "/wp-content/uploads/guide-images/fishing"

# Confirmed server filenames (display name -> file, no extension). Extend as folders fill.
IMAGE_OVERRIDES = {
    # Local Legends — display-name files (wavy willard is lower-case on the server)
    "Glass Ghost": "local-legends/Glass Ghost",
    "Hocking Hill Hellion": "local-legends/Hocking Hill Hellion",
    "Organ Grinder": "local-legends/Organ Grinder",
    "Ryl-Tkannoth, Maw-Begotten": "local-legends/Ryl-Tkannoth, Maw-Begotten",
    "Sludge Eye": "local-legends/Sludge Eye",
    "Wavy Willard": "local-legends/wavy willard",
    # Seasonal
    "Fernskipper": "seasonal-fish-images/Fernskipper",
    "Orange Overseer": "seasonal-fish-images/Orange Overseer",
    "Fester Koi": "seasonal-fish-images/Fester Koi",
    "Bog Sucker": "seasonal-fish-images/Bog Sucker",
    # Generic (the fish-trap fish)
    "Brook Silverside": "generic-fish/Brook Silverside", "Redbelly": "generic-fish/Redbelly",
    "Sunscream": "generic-fish/Sunscream", "Chain Pickerel": "generic-fish/Chain Pickerel",
    "Ridge Trout": "generic-fish/Ridge Trout", "Smoky Salmon": "generic-fish/Smoky Salmon",
    "Walleye": "generic-fish/Walleye", "Yellow Bullhead": "generic-fish/Yellow Bullhead",
    # Common — Sawgills
    "Alpine Sawgill": "common-fish/Alpine Sawgill", "Bog Sawgill": "common-fish/Bog Sawgill",
    "Muddy Sawgill": "common-fish/Muddy Sawgill", "Noxious Sawgill": "common-fish/Noxious Sawgill",
    "Rusted Sawgill": "common-fish/Rusted Sawgill", "Sooty Sawgill": "common-fish/Sooty Sawgill",
    "Static Sawgill": "common-fish/Static Sawgill", "Timber Sawgill": "common-fish/Timber Sawgill",
    # Uncommon (Bog Lurker is lower-case on the server)
    "Armored Spinefish": "uncommon-fish/Armored Spinefish", "Ashen Ambusher": "uncommon-fish/Ashen Ambusher",
    "Blisterfish": "uncommon-fish/Blisterfish", "Bloodwhisker": "uncommon-fish/Bloodwhisker",
    "Bluefin Zapper": "uncommon-fish/Bluefin Zapper", "Bog Lurker": "uncommon-fish/bog lurker",
    "Deathjaw": "uncommon-fish/Deathjaw", "Gulpy": "uncommon-fish/Gulpy",
    "Kanawha Piranha": "uncommon-fish/Kanawha Piranha", "Leatherback": "uncommon-fish/Leatherback",
    "Potbelly Kelt": "uncommon-fish/Potbelly Kelt", "Purple Radpole": "uncommon-fish/Purple Radpole",
    "Spikesnapper": "uncommon-fish/Spikesnapper", "Stormswimmer": "uncommon-fish/Stormswimmer",
    "Brahfin": "uncommon-fish/Brahfin", "Withered Radeye": "uncommon-fish/Withered Radeye",
    # Glowing — all lower-case on the server
    "Glowing Ambusher": "glowing-fish/glowing ambusher", "Glowing Bog Lurker": "glowing-fish/glowing bog lurker",
    "Glowing Gulpy": "glowing-fish/glowing gulpy", "Glowing Kanawha Piranha": "glowing-fish/glowing kanawha piranha",
    "Glowing Potbelly Kelt": "glowing-fish/glowing potbelly kelt", "Glowing Spinefish": "glowing-fish/glowing spinefish",
    "Glowing Stormswimmer": "glowing-fish/glowing stormswimmer", "Glowing Brahfin": "glowing-fish/glowing brahfin",
    # Axolotls — mesh-name files
    "Dotted Axolotl": "axolotl-images/axolotl_dotted", "Purple Axolotl": "axolotl-images/axolotl_purple",
    "Stone Axolotl": "axolotl-images/axolotl_stone", "Clay Axolotl": "axolotl-images/axolotl_clay",
    "Striped Axolotl": "axolotl-images/axolotl_striped", "Scaled Axolotl": "axolotl-images/axolotl_scaled",
    "Shadow Axolotl": "axolotl-images/axolotl_shadow", "Speckled Axolotl": "axolotl-images/axolotl_speckled",
    "Spotted Axolotl": "axolotl-images/axolotl_spotted",
    "Charcoal Axolotl": "axolotl-images/axolotl_charcoal", "Banded Axolotl": "axolotl-images/axolotl_banded",
    "Pink Axolotl": "axolotl-images/axolotl_pink",
    # Junk & Waterlogged Gifts — both live in one server folder: /junk-and-gifts/
    "Baseball Glove": "junk-and-gifts/Baseball Glove", "Broken Camera": "junk-and-gifts/Broken Camera",
    "Dirty Pillow": "junk-and-gifts/Dirty Pillow", "Doll Head": "junk-and-gifts/Doll Head",
    "Handcuffs": "junk-and-gifts/Handcuffs", "Luxobrew Coffee Pot": "junk-and-gifts/Luxobrew Coffee Pot",
    "Military Ammo Bag": "junk-and-gifts/Military Ammo Bag", "Oil Can": "junk-and-gifts/Oil Can",
    "Pack of Duct Tape": "junk-and-gifts/Pack of Duct Tape", "Toothbrush": "junk-and-gifts/Toothbrush",
    "Large Waterlogged Gift": "junk-and-gifts/Large Waterlogged Gift",
    "Small Waterlogged Gift": "junk-and-gifts/Small Waterlogged Gift",
    "Waterlogged Gift": "junk-and-gifts/Waterlogged Gift",
}

# Curated, non-game-data fields. The hole name and the flavour description aren't in
# the export; everything else on the card is generated. Keyed by display name.
TEMPLATE_OVERRIDES = {
    # ── Seasonal Local Legends (location-locked + season-locked) ──────────
    "Glass Ghost": {
        "hole": "Glassed Cavern",
        "description": "Can only be caught at <b>Glassed Cavern</b> in the Cranberry Bog, and only "
                       "while <b>Summer</b> is in season. Once the season turns, the Glass Ghost "
                       "vanishes until next summer.",
    },
    "Sludge Eye": {
        "hole": "The Sludge Works",
        "description": "Can only be caught at <b>The Sludge Works</b> in the Ash Heap, and only "
                       "while <b>Fall</b> is in season. Once the season turns, the Sludge Eye "
                       "vanishes until next fall.",
    },
    # ── Full-time Local Legends (location-locked, year-round) ─────────────
    # Derived from GetInCurrentLocation conditions on each entry of the
    # Fishing_LLS_FishCollection_LocalLegends LVLI (00804F7F).
    "Wavy Willard": {
        "hole": "Wavy Willard's Water Park",
        "region": "Toxic Valley",
    },
    "Organ Grinder": {
        "hole": "Organ Cave",
        "region": "The Forest",
    },
    "Ryl-Tkannoth, Maw-Begotten": {
        "hole": "Big Maw",
        "region": "The Mire",
    },
    "Hocking Hill Hellion": {
        "hole": "Ash Cave",
        "region": "Burning Springs",
    },
}

DROP_NAMES = {"Peach Axolotl"}
CASCADES = [
    ("season",  "Seasonal Fish",     "Rotates with the in-game season. Each is catchable only during its window before the next swaps in."),
    ("legend",  "Local Legend",      "Unique named bosses at fixed spots. Top of the cascade — first pick of the roll, with max fight and length."),
    ("glow",    "Glowing",           "Irradiated variants, one per region. A rare shimmer that rolls just below the legends."),
    ("axolotl", "Axolotl",           "Twelve colour morphs of the prize amphibian, one per calendar month. Only rolls on Improved or Superb bait."),
    ("uncommon","Uncommon",          "Region signature fish, two per region. The middle of the cascade."),
    ("common",  "Common",            "One sawgill per region — the reliable everyday catch that fills most casts."),
    ("generic", "Generic",           "Plain fish found in any water. Bottom of the cascade, right before the junk."),
    ("junk",    "Junk",              "Old-world rubbish snagged off the bottom — not a fish. No fight, no fillet."),
    ("gift",    "Waterlogged Gifts", "Weekend event drops. A 15% pre-cascade chance on each cast; open it for a reward bundle."),
]
REGION_KW = {"Forest": "The Forest", "AshHeap": "Ash Heap", "CranberryBog": "Cranberry Bog",
    "Mire": "The Mire", "ToxicValley": "Toxic Valley", "SavageDivide": "Savage Divide",
    "SkylineValley": "Skyline Valley", "BurningSprings": "Burning Springs"}
SEASON_OF = {"Orange Overseer": "Spring", "Fernskipper": "Summer", "Glass Ghost": "Summer",
    "Fester Koi": "Fall", "Sludge Eye": "Fall", "Bog Sucker": "Winter"}

# Fish Bits yield by size (not in the export; the in-game scrap yield scales with size).
FISHBITS_BY_SIZE = {"Small": 1, "Medium": 3, "Large": 5}

# Axolotls fillet for a flat 25 Fish Bits regardless of their Small size class.
# The COBJ that makes Fish Bits from a raw axolotl is in the export, but xEdit's
# created-object count (NNAM) is not one of the exported columns, so the yield
# cannot yet be read from the game files — it is pinned here. If a future COBJ
# export adds the create count, source it from that recipe instead.
AXOLOTL_FISHBITS = 25

# Region keyword -> friendly name (same mapping as build_axolotl_guide_json.py).
LOC_KEYWORD_MAP = {
    "LocRegionBurningSprings":   "Burning Springs",
    "LocRegionMountain":         "Savage Divide",
    "LocRegionCranberryBog":     "Cranberry Bog",
    "LocRegionForestFloodlands": "Forest",
    "LocRegionStorm":            "Skyline Valley",
    "LocRegionSwampForest":      "Mire",
    "LocRegionMTR":              "Ash Heap",
    "LocRegionToxicValley":      "Toxic Valley",
}
MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]

# SCORE daily/weekly fishing "catch a fish" challenges reference this condition form.
FISH_CATCH_CNDF = "007FE614"   # Challenge_Fishing_Fish_Condition

# Region location-keyword EDID -> the region name used by region_of() (REGION_KW style,
# i.e. "The Forest" / "The Mire"), so a fish's region can be matched to a region challenge.
SCORE_REGION_KW = {
    "LocRegionStorm":            "Skyline Valley",
    "LocRegionMountain":         "Savage Divide",
    "LocRegionSwampForest":      "The Mire",
    "LocRegionMTR":              "Ash Heap",
    "LocRegionForestFloodlands": "The Forest",
    "LocRegionToxicValley":      "Toxic Valley",
    "LocRegionCranberryBog":     "Cranberry Bog",
    "LocRegionBurningSprings":   "Burning Springs",
}

# Friendly names for GMRW reward items whose EDID has no FULL in the exports.
REWARD_ITEM_NAMES = {
    "LegendaryModule":     "Legendary Module",
    "Treasury_Note":       "Treasury Note",
    "Fishing_Bait_Common": "Common Bait",
    "Fishing_Bait_Improved": "Improved Bait",
    "Fishing_Bait_Superb": "Superb Bait",
    "Caps001":             "Caps",
}

# Display names for crafting components whose EDID has no FULL anywhere in the exports.
COMPONENT_NAMES = {
    "c_Wood": "Wood", "Wood": "Wood", "CookingOil": "Cooking Oil",
    "WaterBoiled": "Boiled Water", "WaterDirty": "Dirty Water", "WaterPurified": "Purified Water",
    "DeathclawEgg": "Deathclaw Egg", "CarrotVegetable": "Carrot",
    "Cooking_Razorgrain_Flour": "Razorgrain Flour", "RazorgrainFlour": "Razorgrain Flour",
    "Salt": "Salt", "CookingFlavor_Salt": "Salt", "Sugar": "Sugar", "Flour": "Flour",
}

SPECIAL = {"Strength", "Perception", "Endurance", "Charisma", "Intelligence", "Agility", "Luck"}

# ---------------------------------------------------------------- generic helpers

def newest(directory, prefix, pts=False):
    """Newest export matching prefix in `directory`. Live names embed '<Month>_<Year>';
    PTS names embed '_PTS_<YYYY-MM-DD>_<HHMM>'. Returns a path or None."""
    months = {"Jan":1,"Feb":2,"March":3,"Mar":3,"Apr":4,"May":5,"June":6,"Jun":6,"July":7,"Jul":7,
              "Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    best, bestkey = None, (-1, -1, -1, -1)
    for p in glob.glob(os.path.join(directory, prefix + "*.tsv")):
        b = os.path.basename(p)
        if pts:
            m = re.search(r"_PTS_(\d{4})-(\d{2})-(\d{2})_(\d{4})", b)
            if not m: continue
            key = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        else:
            if "PTS" in b: continue
            m = re.search(r"_([A-Za-z]+)_(\d{4})", b)
            if not m: continue
            key = (int(m.group(2)), months.get(m.group(1), 0), 0, 0)
        if key > bestkey: bestkey, best = key, p
    return best

def read_rows(path):
    if not path or not os.path.exists(path): return []
    # Some exports carry stray non-UTF-8 bytes; decode leniently so one bad byte
    # in a wide reference column never aborts the whole build.
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))

FORMID_RE = re.compile(r"\[(?:ALCH|FISH|COBJ|CNDF|MISC|RESO|KYWD|EXPL|SNDR|GLOB):([0-9A-Fa-f]{6,8})\]")
BRACKET_FORMID_RE = re.compile(r":([0-9A-Fa-f]{6,8})\]")
COLON_FORMID_RE = re.compile(r"^([0-9A-Fa-f]{6,8}):")

def first_formid(s):
    m = FORMID_RE.search(s or "")
    return m.group(1).upper() if m else None

def all_formids(s):
    return [x.upper() for x in re.findall(r"([0-9A-Fa-f]{6,8})(?=:[A-Za-z]+\b|\])", s or "")]

# ---------------------------------------------------------------- index builders

def build_name_map(alch, misc, cobj):
    """edid -> display name, from every export that carries a FULL/display column."""
    out = dict(COMPONENT_NAMES)
    for r in alch:
        e, n = r.get("ALCH_EDID"), r.get("FULL")
        if e and n: out.setdefault(e, n)
    for r in misc:
        e = r.get("EDID") or r.get("MISC_EDID")
        n = r.get("FULL")
        if e and n: out.setdefault(e, n)
    for r in cobj:
        e, n = r.get("CNAM_EDID"), r.get("CNAM_FULL")
        if e and n: out.setdefault(e, n)
    return out

def prettify(edid, name_map):
    if not edid: return ""
    if edid in name_map: return name_map[edid]
    s = edid
    for pre in ("c_", "Cooking_", "Cooking", "SeasonalFish_Meal_", "Meal_", "co_meal_"):
        if s.startswith(pre): s = s[len(pre):]
    for suf in ("Vegetable", "Fruit", "Herb", "Meat", "Flower"):
        if s.endswith(suf) and len(s) > len(suf): s = s[:-len(suf)]
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s).replace("_", " ").strip()
    return s or edid

def build_glob(glob_rows):
    out = {}
    for r in glob_rows:
        e = r.get("EDID")
        v = r.get("FLTV")
        if e and v not in (None, ""):
            try: out[e] = float(v)
            except ValueError: pass
    return out

def build_alch_index(alch_rows):
    """formid -> {value, edid, name, cndf:[formids], cobj:[formids]}"""
    out = {}
    for r in alch_rows:
        fid = (r.get("ALCH_FormID") or "").upper()
        if not fid: continue
        refs = r.get("Refs_Flat", "") or ""
        cndf = [m for m in all_formids(refs) if ":CNDF" in refs[refs.find(m):refs.find(m)+30]]
        # Simpler: scan the flat refs string for "<id>:<edid>:CNDF"
        cndf = re.findall(r"([0-9A-Fa-f]{6,8}):[^:|]+:CNDF", refs)
        cobj = re.findall(r"([0-9A-Fa-f]{6,8}):[^:|]+:COBJ", refs)
        out[fid] = {
            "value": _intval(r.get("Value")),
            "edid": r.get("ALCH_EDID", ""),
            "name": r.get("FULL", ""),
            "cndf": [x.upper() for x in cndf],
            "cobj": [x.upper() for x in cobj],
        }
    return out

def build_alch_effects(rows, glob_map):
    """edid -> rendered effect string, e.g. '+4 Intelligence for 60 min'."""
    by_edid = {}
    for r in rows:
        by_edid.setdefault(r.get("ALCH_EDID", ""), []).append(r)
    out = {}
    for edid, effs in by_edid.items():
        parts, diet = [], None
        for r in effs:
            mg_name = r.get("MGEF_FULL") or r.get("MGEF_EDID") or ""
            # magnitude: literal EFIT_Magnitude, else the MAGG global tier value
            mag = _floatval(r.get("EFIT_Magnitude"))
            if not mag:
                mag = glob_map.get(r.get("MAGG_GLOB_EDID", ""), 0.0)
            dur = _floatval(r.get("EFIT_Duration"))
            if not dur:
                dur = glob_map.get(r.get("DURG_GLOB_EDID", ""), 0.0)
            txt = render_effect(mg_name, mag, dur)
            if txt: parts.append(txt)
        out[edid] = parts
    return out

def render_effect(mg_name, mag, dur):
    n = mg_name or ""
    # Skip the plumbing effects that aren't player-facing buffs.
    if any(k in n for k in ("SURV_Food_Effect", "Eating:", "Restore Health", "RestoreHealthFood",
                            "Radiation Damage", "DamageRadiation")):
        return None
    # SPECIAL buffs: "Fortify Intelligence Food" -> "+4 Intelligence"
    stat = next((s for s in SPECIAL if s in n), None)
    label = stat if stat else re.sub(r"\b(Food|Fortify|Restore)\b", "", n).strip()
    # Tidy leftover punctuation/whitespace, e.g. "Food: Fortify XP Bonus" -> "XP Bonus"
    label = re.sub(r"\s{2,}", " ", label.strip(" :")).strip()
    # XP Bonus magnitudes are stored as a fraction (0.05 = 5%), render as a percentage.
    is_xp = "XP" in n
    if is_xp:
        label = "XP"
    sign = "+" if (mag and mag > 0) else ("-" if (mag and mag < 0) else "")
    if is_xp:
        magtxt = ("%g%%" % (mag * 100)) if mag else ""
    else:
        magtxt = ("%g" % mag) if mag else ""
    out = (sign + magtxt + " " + label).strip()
    if dur and dur >= 1:
        mins = int(round(dur / 60.0)) if dur >= 60 else None
        out += " for " + (("%d min" % mins) if mins else ("%ds" % int(dur)))
    return out or None

def build_cobj_index(cobj_rows):
    """Two lookups over recipes:
       by_output_edid : output meal EDID -> recipe dict
       by_component   : ingredient EDID  -> [recipe dicts that consume it]
    """
    recipes, by_output, by_component = [], {}, {}
    for r in cobj_rows:
        comps = []
        for chunk in (r.get("FVPA", "") or "").split("|"):
            chunk = chunk.strip()
            if not chunk: continue
            bits = chunk.split(":")
            edid = bits[0]
            qty = bits[1] if len(bits) > 1 and bits[1] else "1"
            comps.append({"edid": edid, "qty": _intval(qty) or 1})
        rec = {
            "out_edid": r.get("CNAM_EDID", ""),
            "out_name": r.get("CNAM_FULL", ""),
            "station": r.get("BNAM_FULL", ""),
            "req_edid": r.get("GNAM_EDID", ""),
            "req_name": r.get("GNAM_FULL", ""),
            "req_fid": (r.get("GNAM_FormID") or "").upper(),
            "kw": r.get("FNAM_Keywords", "") or "",
            "comps": comps,
        }
        recipes.append(rec)
        if rec["out_edid"]:
            by_output.setdefault(rec["out_edid"], rec)
        for c in comps:
            by_component.setdefault(c["edid"], []).append(rec)
    return recipes, by_output, by_component

def _is_dead_chal(edid, full):
    """True for cut / deleted / test / placeholder challenge records that should
       never surface on the site."""
    e = (edid or "").lower()
    f = (full or "").lower()
    if e.startswith("zzz"):
        return True
    if re.search(r"(^|_)(cut|del|deleted|test|debug)(_|$)", e):
        return True
    if re.search(r"\b(cut|zzz|deleted?|test|placeholder|do not use)\b", f):
        return True
    return False

def _reward_item_name(spec, name_map):
    """spec like '005652F9:LegendaryModule:MISC' -> friendly item name."""
    parts = (spec or "").split(":")
    edid = parts[1] if len(parts) > 1 else (spec or "")
    if edid in REWARD_ITEM_NAMES:
        return REWARD_ITEM_NAMES[edid]
    if edid.startswith("Fishing_Recipe_"):
        base = re.sub(r"^Fishing_Recipe_(mod_)?", "", edid)
        return "Recipe: " + prettify(base, name_map)
    return prettify(edid, name_map)

def _glob_edid(spec):
    """'00805AC0:Fishing_LocalLegendChallenge_CapReward:GLOB' -> the EDID."""
    parts = (spec or "").split(":")
    return parts[1] if len(parts) > 1 else ""

def build_gmrw_rewards(gmrw_rows, glob_map, name_map):
    """CHAL formid -> readable reward string, sourced from the GMRW export
       (fishing challenge rewards are NOT in CHAL — they live in GMRW)."""
    out = {}
    for r in gmrw_rows:
        chal_ids = []
        for i in range(1, 21):
            v = r.get("Ref%d" % i, "") or ""
            m = re.match(r"([0-9A-Fa-f]{6,8}):[^:]*:CHAL", v)
            if m:
                chal_ids.append(m.group(1).upper())
        if not chal_ids:
            continue
        parts = []
        # Rewarded item (MISC/BOOK/LVLI etc.)
        item = r.get("RewardedItem", "") or ""
        if item:
            cnt = (r.get("RewardedItemCount", "") or "").strip()
            nm = _reward_item_name(item, name_map)
            try:
                n = int(float(cnt))
            except (TypeError, ValueError):
                n = 0
            parts.append(("%d× %s" % (n, nm)) if n > 1 else nm)
        # Caps from a GLOB (e.g. Local Legend cap reward)
        caps = r.get("NAM8_CapsGlobal", "") or ""
        if caps:
            edid = _glob_edid(caps)
            val = glob_map.get(edid)
            parts.append(("%d Caps" % int(val)) if val else "Caps")
        # Legendary item reward list
        if (r.get("QRLI_LegendaryItemRewardList", "") or "").strip():
            parts.append("Legendary item")
        if not parts:
            continue
        reward = " + ".join(parts)
        for cid in chal_ids:
            out.setdefault(cid, reward)
    return out

def build_score_fishing_challenges(chal_rows):
    """Daily/Weekly SCORE fishing 'catch a fish' challenges, split into generic
       (any region) and region-specific (matched via a LocRegion keyword)."""
    generic = {"Daily": [], "Weekly": []}
    region  = {"Daily": {}, "Weekly": {}}
    seen = {"Daily": set(), "Weekly": set()}
    for r in chal_rows:
        e = r.get("EDID", "") or ""
        if _is_dead_chal(e, r.get("FULL", "")):
            continue
        m = re.match(r"SCORE_Challenge_(Daily|Weekly)_Fishing", e)
        if not m:
            continue
        period = m.group(1)
        full = r.get("FULL", "") or ""
        conds = " ".join(v for k, v in r.items() if k and k.startswith("Cond") and v)
        if FISH_CATCH_CNDF not in conds.upper():
            continue
        if "catch" not in full.lower():        # exclude Eat / Craft / Cook variants
            continue
        reg = None
        for kw, name in SCORE_REGION_KW.items():
            if kw in conds:
                reg = name
                break
        key = (full, reg)
        if key in seen[period]:
            continue
        seen[period].add(key)
        entry = {"name": full, "reward": "Score"}
        if reg:
            region[period].setdefault(reg, []).append(entry)
        else:
            generic[period].append(entry)
    return {"generic": generic, "region": region}

def build_challenge_groups(lifetime_list, fish_region, score_ch, is_real_fish):
    """Assemble the per-fish {lifetime, daily, weekly} challenge groups."""
    if not is_real_fish:
        return {"lifetime": lifetime_list, "daily": [], "weekly": []}
    daily  = [dict(x) for x in score_ch["generic"]["Daily"]]
    weekly = [dict(x) for x in score_ch["generic"]["Weekly"]]
    def add_region(period_map, target):
        if fish_region == "All regions":
            for lst in period_map.values():
                target.extend(dict(x) for x in lst)
        elif fish_region in period_map:
            target.extend(dict(x) for x in period_map[fish_region])
    add_region(score_ch["region"]["Daily"], daily)
    add_region(score_ch["region"]["Weekly"], weekly)
    return {"lifetime": lifetime_list, "daily": daily, "weekly": weekly}

def _parent_meta_edid(edid):
    """Given a lifetime SUB/component challenge EDID, return the EDID of the META
       (collection) challenge it rolls up into, or None."""
    e = edid or ""
    # Region collections: ..._Region_<Group>_(Common|Unique|Glowing)Fish##_SUB
    m = re.match(r"(.*_Region_[A-Za-z]+)_(?:Common|Unique|Glowing)Fish\d+_SUB$", e)
    if m:
        return m.group(1) + "_META"
    # Fish-quest progress steps: ..._Progress_0#_<Fish>_SUB
    m = re.match(r"(.*_Progress_\d+)_.+_SUB$", e)
    if m:
        return m.group(1) + "_META"
    # Burning Springs: Burn_..._BurningSprings_SUB_<X>
    m = re.match(r"(.*_BurningSprings)_SUB_.+$", e)
    if m:
        return m.group(1) + "_META"
    # Axolotl months: ..._Axolotl_##_SUB  ->  ..._Axolotls_META
    if re.match(r".*_Axolotl_\d+_SUB$", e):
        return "Challenge_Lifetime_Fishing_Axolotls_META"
    # Seasonal fish (per-season Any / LocalLegend) -> the every-season META
    if "_SeasonalFish_" in e and not e.endswith("_META"):
        return "Challenge_Lifetime_Fishing_SeasonalFish_AllSeasons_META"
    return None

def build_chal_index(chal_rows):
    """List of challenges with the CNDF condition formids they reference."""
    out = []
    for r in chal_rows:
        edid = r.get("EDID", "")
        if _is_dead_chal(edid, r.get("FULL", "")): continue
        conds = []
        for k, v in r.items():
            if k and k.startswith("Cond") and v:
                conds += re.findall(r"\[CNDF:([0-9A-Fa-f]{6,8})\]", v)
        out.append({
            "fid": (r.get("FormID") or "").upper(),
            "edid": edid,
            "name": r.get("FULL", ""),
            "cndf": [c.upper() for c in conds],
        })
    return out

# ---------------------------------------------------------------- scrap yields
# Junk pulled from the water scraps into crafting components. What a junk item
# yields is a two-table join (same model as build_scrap_breakdown_json.py, kept
# inline here so the journal build stays self-contained):
#   MISC.MCQP = item  -> {component EDID : tier keyword}   (c_Plastic:ComponentQuantityLow)
#   CMPO.CVPA = comp  -> {tier keyword : amount}            (ComponentQuantityLow -> 1)
# The amount is per-component, so the same tier resolves to different counts on
# different components (Steel Bulk=30 vs Plastic Bulk=10) — always read the count
# from that component's CVPA row, never assume a fixed tier->number map.
_SCRAP_CUT_RE = re.compile(
    r"^(zz|zzz|del_|cut_|post_|test_|debug_|deprecated_|pts_)", re.IGNORECASE)

def _scrap_is_cut(edid):
    return bool(edid) and bool(_SCRAP_CUT_RE.match(edid))

def build_cmpo_tiers(cmpo_rows):
    """component EDID -> {name, tiers{tier keyword: amount}} from the CMPO CVPA table."""
    out = {}
    for r in cmpo_rows:
        edid = (r.get("CMPO_EDID") or "").strip()
        if not edid or _scrap_is_cut(edid):
            continue
        tiers = {}
        for ent in (r.get("CVPA") or "").strip().split("|"):
            if not ent:
                continue
            parts = ent.split(":")          # tier : count : curve
            if len(parts) < 2:
                continue
            tier, cnt = parts[0].strip(), _intval(parts[1])
            if tier and cnt is not None:
                tiers[tier] = cnt
        out[edid] = {"name": (r.get("FULL") or "").strip() or edid, "tiers": tiers}
    return out

def build_scrap_yields(misc_rows, cmpo_rows):
    """display name -> scrap yield string, e.g. 'Baseball Glove' -> '3x Leather, 1x Cloth'.
    First non-cut MISC row per display name wins (duplicate junk records share MCQP)."""
    comps = build_cmpo_tiers(cmpo_rows)
    out = {}
    for r in misc_rows:
        mcqp = (r.get("MCQP") or "").strip()
        if not mcqp:
            continue
        edid = (r.get("EDID") or r.get("MISC_EDID") or "").strip()
        if _scrap_is_cut(edid):
            continue
        name = (r.get("FULL") or "").strip()
        if not name or name in out:
            continue
        parts = []
        for pair in mcqp.split("|"):
            p = pair.split(":", 1)          # component EDID : tier keyword
            if len(p) < 2:
                continue
            comp_edid, tier = p[0].strip(), p[1].strip()
            cinfo = comps.get(comp_edid)
            if not cinfo:
                continue
            amount = cinfo["tiers"].get(tier)
            if amount is None:
                continue
            parts.append("%dx %s" % (amount, cinfo["name"]))
        if parts:
            out[name] = ", ".join(parts)
    return out

def _intval(s):
    try: return int(float(s))
    except (TypeError, ValueError): return None

def _floatval(s):
    try: return float(s)
    except (TypeError, ValueError): return 0.0

# ---------------------------------------------------------------- FISH parsing

def kws(r): return [r.get(k, "") for k in ("KW1","KW2","KW3","KW4","KW5")]
def has_kw(r, t): return any("FishType_%s:" % t in v for v in kws(r))
def disp(r):
    m = re.search(r'"([^"]+)"', r.get("FIRI", "")); return m.group(1) if m else r.get("EDID", "")
def size_of(r):
    for v in kws(r):
        if "FishSize_" in v: return v.split("FishSize_")[1].split(":")[0]
    return "Unknown"
def colls(r):
    out = []
    for i in range(1, 7):
        m = re.search(r":([^:]+):LVLI", r.get("Ref%d" % i, ""))
        if m: out.append(m.group(1))
    return out
def region_of(r):
    for k, v in REGION_KW.items():
        if has_kw(r, k): return v
    if has_kw(r, "Generic"): return "All regions"
    return "Special"

# Region collection token (Fishing_LLS_FishCollection_<token>_<rarity>) -> friendly
# region name. A fish that belongs to several region collections is catchable in each
# of those regions, even though its signature FishType keyword names only one — e.g.
# seasonal uncommons like the Fernskipper sit in Cranberry + Mire + Skyline.
COLL_REGION = {
    "Forest": "The Forest", "Toxic": "Toxic Valley", "SavageDivide": "Savage Divide",
    "Mire": "The Mire", "Cranberry": "Cranberry Bog", "Ash": "Ash Heap",
    "Skyline": "Skyline Valley", "BurningSprings": "Burning Springs",
}
_REGION_DISPLAY_ORDER = ["The Forest", "Toxic Valley", "Savage Divide", "The Mire",
    "Cranberry Bog", "Ash Heap", "Skyline Valley", "Burning Springs"]

def regions_of(r):
    """Every region a fish can be caught in, read from its FishCollection LVLI
    memberships (in a fixed display order). Non-region collections are ignored."""
    found = set()
    for coll in colls(r):
        m = re.match(r"Fishing_LLS_FishCollection_([A-Za-z]+)_", coll)
        if m and m.group(1) in COLL_REGION:
            found.add(COLL_REGION[m.group(1)])
    return [x for x in _REGION_DISPLAY_ORDER if x in found]
def cascade_of(r):
    cl, nm = colls(r), disp(r)
    if has_kw(r, "LocalLegend") or any("LocalLegends" in c for c in cl): return "legend"
    if any("WaterLoggedGifts" in c for c in cl) or "Gift" in nm: return "gift"
    if any("Junk" in c for c in cl): return "junk"
    if has_kw(r, "Glowing") or nm.startswith("Glowing"): return "glow"
    if has_kw(r, "Axolotl") or any("Axolotls" in c for c in cl): return "axolotl"
    # A Generic-type fish is an all-region catch even when it also sits in the
    # SeasonalFish_Uncommon pool (e.g. Ridge Trout, the only generic that does).
    # Classify by its FishType keyword / Generic collection so it lands in the
    # generic cascade, not uncommon — otherwise it wrongly shows in region tabs.
    if has_kw(r, "Generic") or any("Generic" in c for c in cl): return "generic"
    if any(c.endswith("_Uncommon") or "SeasonalFish" in c for c in cl): return "uncommon"
    if any(c.endswith("_Common") for c in cl): return "common"
    if any("Generic" in c for c in cl): return "generic"
    return None

def keywords_of(r):
    out = []
    for v in kws(r):
        if not v: continue
        parts = v.split(":")
        if len(parts) >= 2:
            out.append({"id": parts[0].upper(), "name": parts[1]})
    return out

def stats_of(r):
    def fv(k):
        try: return round(float(r.get(k, "")), 4)
        except (TypeError, ValueError): return None
    def iv(k):
        try: return int(float(r.get(k, "")))
        except (TypeError, ValueError): return None
    return {
        "startProgress": fv("FISP"), "drawInSpeed": fv("FIDS"), "escapeSpeed": fv("FIES"),
        "maxStamina": iv("FIHS"), "minStamina": iv("FILS"),
        "maxJumpDelay": fv("FIHD"), "minJumpDelay": fv("FILD"),
        "jumpSpeed": iv("FIJS"),
        "maxJumpDistance": iv("FIHJ"), "minJumpDistance": iv("FILJ"),
        "minInitialAngle": iv("FILA"), "maxInitialAngle": iv("FIHA"),
    }

def splash_of(r):
    s = r.get("FIEX", "") or ""
    return s.split("[")[0].strip() or None

def is_legendary(r):
    return "Legendary" in (r.get("FISU", "") or "")

def image_for(name):
    rel = IMAGE_OVERRIDES.get(name)
    return "%s/%s.avif" % (IMG_BASE, rel) if rel else None

def baits_for(cid):
    if cid == "axolotl": return ["Improved", "Superior"]
    if cid == "junk":    return ["Basic"]
    return ["Basic", "Improved", "Superior"]

# The four fishing weather states, from the CNDF Fishing_IsNatural*Weather conditions:
# clear fallback + Rainy + Rad Storm (the Rad/Nuke conditions are the same rad-storm
# state) + Sandstorm. Weather changes the rate, not availability, so every catch lists all four.
WEATHER = ["Clear", "Rain", "Rad Storm", "Sandstorm"]

# ---------------------------------------------------------------- recipe + challenge resolution

def recipe_dict(rec, name_map, eff_map):
    ings = []
    for c in rec["comps"]:
        ings.append({"item": prettify(c["edid"], name_map), "qty": c["qty"]})
    out_edid = rec["out_edid"]
    effs = eff_map.get(out_edid, [])
    return {
        "name": rec["out_name"] or prettify(out_edid, name_map),
        "station": rec["station"] or "",
        "effect": " · ".join(effs) if effs else "",
        "ingredients": ings,
    }

def resolve_recipes(raw_meal_edid, by_output, by_component, name_map, eff_map):
    """Follow raw fish -> Filet -> prepared meals, in cascade order, de-duplicated."""
    out, seen = [], set()
    def add(rec):
        key = rec["out_edid"] or rec["out_name"]
        if not key or key in seen: return None
        seen.add(key)
        out.append(recipe_dict(rec, name_map, eff_map))
        return rec
    if not raw_meal_edid:
        return out
    # 1) the Filet recipe(s) that consume the raw fish
    filet_recs = list(by_component.get(raw_meal_edid, []))
    filet_edids = []
    for fr in filet_recs:
        if add(fr) and fr["out_edid"]:
            filet_edids.append(fr["out_edid"])
    # 2) prepared meals that consume each Filet
    for fe in filet_edids:
        for pr in by_component.get(fe, []):
            add(pr)
    return out

def resolve_challenges(cndf_ids, season, chal_list, by_output, name_map, reward_map=None):
    """Lifetime challenges whose condition references one of this fish's CNDF forms,
       plus the season-wide 'Catch a <Season> Seasonal Fish' challenges. Reward comes
       from the GMRW export, else the recipe a challenge unlocks, else '—'."""
    out, seen = [], set()
    cndf_set = set(cndf_ids or [])
    reward_map = reward_map or {}
    # EDID -> FULL for every collection (META) challenge, to caption its sub-challenges.
    meta_names = {ch["edid"]: ch["name"] for ch in chal_list
                  if ch["edid"].endswith("_META") and ch.get("name")}

    def note_for(edid):
        parent = _parent_meta_edid(edid)
        nm = meta_names.get(parent) if parent else None
        return ("Counts toward: " + nm) if nm else ""

    def reward_for(chal_fid):
        if chal_fid in reward_map:
            return reward_map[chal_fid]
        for rec in by_output.values():
            if rec["req_fid"] and rec["req_fid"] == chal_fid:
                nm = rec["out_name"] or prettify(rec["out_edid"], name_map)
                return "Recipe: " + nm
        return "—"

    for ch in chal_list:
        hit = bool(cndf_set & set(ch["cndf"]))
        if not hit and season:
            # season-wide summary challenge, e.g. "Catch A Summer Seasonal Fish"
            if season.lower() in ch["name"].lower() and "seasonal fish" in ch["name"].lower():
                hit = True
        if not hit: continue
        if ch["name"] in seen: continue
        seen.add(ch["name"])
        entry = {"name": ch["name"], "reward": reward_for(ch["fid"])}
        note = note_for(ch["edid"])
        if note: entry["note"] = note
        out.append(entry)
    return out

# ---------------------------------------------------------------- season rule

def load_rollovers():
    for cand in ("seasonal-fish.json", "seasonal_fish_guide.json"):
        p = os.path.join(DIST, cand)
        if os.path.exists(p):
            try:
                d = json.load(open(p, encoding="utf-8"))
            except (ValueError, IOError):
                continue
            roll = (d.get("seasonRule") or {}).get("rolloverDates")
            if roll: return roll
    return []

# ---- Axolotl monthly rotation, read straight from the game LVLI export --------
# The Fishing_LLS_FishCollection_Axolotls leveled list encodes, per entry, the
# fish it points at plus the CTDA conditions that gate it: a MonthlyIndex check
# (which calendar month the colour is active) and two LocationHierarchyHasKeyword
# checks (the two regions it spawns in that month). Both facts are parsed here
# generatively, so the field journal no longer depends on dist/axolotl_guide.json.

def newest_lvli_entries(directory, pts=False):
    """Newest LVLI_Export_*_LVLI_Entries.tsv in `directory`, honouring the same
    live/PTS filename conventions as newest()."""
    months = {"Jan":1,"Feb":2,"March":3,"Mar":3,"Apr":4,"May":5,"June":6,"Jun":6,
              "July":7,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    best, bestkey = None, (-1, -1, -1, -1)
    for p in glob.glob(os.path.join(directory, "LVLI_Export_*_LVLI_Entries.tsv")):
        b = os.path.basename(p)
        if pts:
            m = re.search(r"_PTS_(\d{4})-(\d{2})-(\d{2})_(\d{4})", b)
            if not m: continue
            key = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        else:
            if "PTS" in b: continue
            m = re.search(r"_([A-Za-z]+)_(\d{4})", b)
            if not m: continue
            key = (int(m.group(2)), months.get(m.group(1), 0), 0, 0)
        if key > bestkey: bestkey, best = key, p
    return best

def parse_axolotl_rotation(path, list_edid="Fishing_LLS_FishCollection_Axolotls"):
    """month-number (1-12) -> {refFormId, regions[]} from a fishing axolotl LVLI."""
    out = {}
    for r in read_rows(path):
        if (r.get("LVLI_EDID") or "") != list_edid: continue
        ref = r.get("LVLO_Reference", "") or ""
        ref_fid = ref.split(":")[0].upper() if ref else ""
        regions, month = [], None
        for k, v in r.items():
            if not k or not k.startswith("Cond") or not v: continue
            mreg = re.search(r"LocationHierarchyHasKeyword\(.+?,\s*(LocRegion\w+)\s*\[", v)
            if mreg: regions.append(LOC_KEYWORD_MAP.get(mreg.group(1), mreg.group(1)))
            mmon = re.search(r"MonthlyIndex.+?\)\s+\S+\s+([\d.]+)", v)
            if mmon: month = int(float(mmon.group(1)))
        if month: out[month] = {"refFormId": ref_fid, "regions": regions}
    return out

def load_axolotl_rotation(directory, pts=False):
    """FormID (upper) -> {'month': 'June', 'regions': [...]} for each axolotl,
    derived from the newest LVLI entries export in `directory`. Returns an empty
    map (and the caller falls back) if no LVLI entries export is present."""
    path = newest_lvli_entries(directory, pts)
    out = {}
    if path:
        for month, rec in parse_axolotl_rotation(path).items():
            if rec["refFormId"]:
                out[rec["refFormId"]] = {"month": MONTH_NAMES[month - 1],
                                         "regions": rec["regions"]}
    return out

# ---------------------------------------------------------------- build

def build(tsv_path, ctx, axolotl_map=None):
    axolotl_map = axolotl_map or {}
    rows = read_rows(tsv_path)
    bucket = {cid: [] for cid, _, _ in CASCADES}
    seen = {cid: set() for cid, _, _ in CASCADES}

    for r in rows:
        nm = disp(r)
        if r.get("EDID", "").startswith("zzz"): continue
        if nm.startswith("zzz") or nm.startswith("Fishing_"): continue
        if nm in DROP_NAMES: continue
        cid = cascade_of(r)
        if not cid: continue

        def rec(c):
            if nm in seen[c]: return None
            seen[c].add(nm)
            casc_label = next((lab for cc, lab, _ in CASCADES if cc == c), c)
            raw_meal_fid = first_formid(r.get("FIRI", ""))
            alch = ctx["alch"].get(raw_meal_fid) if raw_meal_fid else None
            raw_meal_edid = alch["edid"] if alch else None
            season = SEASON_OF.get(nm)
            recipes = resolve_recipes(raw_meal_edid, ctx["cobj_out"], ctx["cobj_comp"],
                                      ctx["names"], ctx["effects"])
            lifetime = resolve_challenges(alch["cndf"] if alch else [], season,
                                          ctx["chal"], ctx["cobj_out"], ctx["names"],
                                          ctx.get("rewards"))
            # Effective region (template overrides win) for region-challenge matching.
            _ov = TEMPLATE_OVERRIDES.get(nm)
            eff_region = (_ov.get("region") if _ov and _ov.get("region") else region_of(r))
            challenges = build_challenge_groups(lifetime, eff_region, ctx["score_ch"],
                                                c not in ("junk", "gift"))
            o = {
                "id": (r.get("FormID") or "").upper(),
                "edid": r.get("EDID", ""),
                "name": nm,
                # Junk and waterlogged gifts carry the in-game Small size keyword,
                # but they are not fish — they don't fillet, so they have no fish
                # size class and yield no Fish Bits. (The raw keyword is still shown
                # in the Technical section via keywords_of.)
                "size": (None if c in ("junk", "gift") else size_of(r)),
                "region": region_of(r),
                "image": image_for(nm),
                "value": (alch["value"] if alch else None),
                "fishBits": (None if c in ("junk", "gift") else FISHBITS_BY_SIZE.get(size_of(r))),
                "legendary": is_legendary(r),
                "splash": splash_of(r),
                "stats": stats_of(r),
                "keywords": keywords_of(r),
                "recipes": recipes,
                "challenges": challenges,
                "weather": list(WEATHER),
                "bait": baits_for(c),
            }
            if season: o["season"] = season
            # Multi-region fish (e.g. seasonal uncommons) list every region they can be
            # caught in. Single-region fish keep the plain `region` field; axolotls carry
            # their own monthly `regions`, and junk/gifts aren't region-bound.
            if c not in ("axolotl", "junk", "gift"):
                _regs = regions_of(r)
                if len(_regs) >= 2: o["catchRegions"] = _regs
            if c == "axolotl":
                info = axolotl_map.get((r.get("FormID") or "").upper()) or {}
                if info.get("month"): o["month"] = info["month"]
                if info.get("regions"): o["regions"] = info["regions"]
                # Fixed 25 Fish Bits on fillet (not in the export; see AXOLOTL_FISHBITS).
                o["fishBits"] = AXOLOTL_FISHBITS
            if c == "junk":
                # Junk isn't a fish — it doesn't fillet. Instead it scraps into
                # crafting components (MISC.MCQP × CMPO.CVPA join), shown as the
                # "Yields" row on the fish-types guide, e.g. "3x Leather, 1x Cloth".
                o["scrapYield"] = ctx["scrap"].get(nm)
            if c == "gift":
                # Waterlogged Gifts are a limited-time event catch (Festive_WaterLoggedHolidayGift,
                # gated by GLOB LTT_WaterLoggedGifts_Toggle) -- NOT year-round. The gift tier is
                # bait-locked in the LVLI Fishing_LL_LTT_WaterLoggedGifts (FirstMatch on bait):
                #   Tier_01 Small = Basic, Tier_02 Waterlogged = Improved, Tier_03 Large = Superb.
                o["event"] = "Waterlogged Gifts event"
                tier = (o.get("edid") or "")[-2:]
                o["bait"] = {"01": ["Basic"], "02": ["Improved"], "03": ["Superior"]}.get(tier, o["bait"])
            ov = TEMPLATE_OVERRIDES.get(nm)
            if ov:
                if ov.get("hole"): o["hole"] = ov["hole"]
                if ov.get("region"): o["region"] = ov["region"]
                if ov.get("description"): o["description"] = ov["description"]
            return o

        m = rec(cid)
        if m: bucket[cid].append(m)
        if nm in SEASON_OF:
            m2 = rec("season")
            if m2: bucket["season"].append(m2)

    return {
        "generated": datetime.datetime.utcnow().isoformat() + "Z",
        "source": os.path.basename(tsv_path),
        "seasonRule": {"rolloverDates": load_rollovers()},
        "cascades": [{"id": cid, "label": lab, "blurb": bl, "fish": bucket[cid]}
                     for cid, lab, bl in CASCADES],
    }

def make_ctx(directory, pts=False):
    glob_map = build_glob(read_rows(newest(directory, "GLOB_Export_", pts)))
    alch_main = read_rows(newest(directory, "ALCH_Export_", pts))
    # ALCH main vs effects share the prefix; the effects file ends in _Effects.
    alch_main = [r for r in alch_main if "ALCH_FormID" in r]
    eff_path = None
    for p in glob.glob(os.path.join(directory, "ALCH_Export_*Effects*.tsv")):
        if pts == ("PTS" in os.path.basename(p)):
            eff_path = p
    misc = read_rows(newest(directory, "MISC_Export_", pts))
    cmpo = read_rows(newest(directory, "CMPO_Export_", pts))
    cobj = read_rows(newest(directory, "COBJ_Export_", pts))
    chal = read_rows(newest(directory, "CHAL_Export_", pts))
    gmrw = read_rows(newest(directory, "GMRW_Export_", pts))
    names = build_name_map(alch_main, misc, cobj)
    recipes, cobj_out, cobj_comp = build_cobj_index(cobj)
    return {
        "alch": build_alch_index(alch_main),
        "effects": build_alch_effects(read_rows(eff_path), glob_map),
        "names": names,
        "cobj_out": cobj_out,
        "cobj_comp": cobj_comp,
        "chal": build_chal_index(chal),
        "rewards": build_gmrw_rewards(gmrw, glob_map, names),
        "score_ch": build_score_fishing_challenges(chal),
        # Junk -> crafting-component scrap yields (MISC.MCQP × CMPO.CVPA join).
        "scrap": build_scrap_yields(misc, cmpo),
    }

def main():
    os.makedirs(DIST, exist_ok=True)
    os.makedirs(os.path.join(DIST, "pts"), exist_ok=True)

    # Live FISH export must avoid the PTS naming; reuse the same logic as the others.
    tsv = newest(TSV_DIR, "FISH_Export_")
    if not tsv: raise SystemExit("No FISH_Export TSV found in %s" % TSV_DIR)
    live_ctx = make_ctx(TSV_DIR)
    live_axo = load_axolotl_rotation(TSV_DIR)
    live_data = build(tsv, live_ctx, live_axo)
    live = os.path.join(DIST, "fish_journal.json")
    json.dump(live_data, open(live, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    # PTS twin (best-effort; falls back to live if no PTS exports exist).
    pts_dir = os.path.join(TSV_DIR, "pts")
    pts_tsv = newest(pts_dir, "FISH_Export_", pts=True)
    if pts_tsv:
        pts_ctx = make_ctx(pts_dir, pts=True)
        # PTS context may be sparse; fall back to live indices where empty.
        for k in ("alch", "effects", "names", "cobj_out", "cobj_comp", "chal",
                  "rewards", "score_ch"):
            if not pts_ctx.get(k): pts_ctx[k] = live_ctx[k]
        # PTS axolotl rotation from the PTS LVLI export; fall back to live if absent.
        pts_axo = load_axolotl_rotation(pts_dir, pts=True) or live_axo
        pts_data = build(pts_tsv, pts_ctx, pts_axo)
    else:
        pts_data = live_data
    pts = os.path.join(DIST, "pts", "fish_journal.json")
    json.dump(pts_data, open(pts, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    tot = sum(len(c["fish"]) for c in live_data["cascades"])
    print("Source:", live_data["source"])
    for c in live_data["cascades"]:
        print("  %-9s %2d" % (c["id"], len(c["fish"])))
    print("Total records:", tot)
    print("Wrote:", live, "and", pts)

if __name__ == "__main__":
    main()
