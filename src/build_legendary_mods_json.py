#!/usr/bin/env python3
"""
build_legendary_mods_json.py
============================
Builds dist/legendary_mods.json  (Legendary Mod Checklist page)
   and dist/murmrgh_legendary_mods.json  (Murmrgh's Legendary Mod List page)

Sources
-------
1. dist/legendary_mod_drop_chances.json  - curated per-star / per-type pools
   (name, star, itemType, formId, weaponCompat, description). Standard + Bounty.
2. src/build_legendary_mod_drop_chances_json.py::EFFECT_DESCRIPTIONS - curated effect text.
3. tsv/OMOD_Export_*.tsv (newest, live) - to fold in the newest effects
   not yet in the curated May set, plus EDIDs for the Technical block.
4. dist/unique_weapons/unique_weapons.json - customMods -> Unique Legendary Mods
   (effect-bearing custom mods baked onto unique named weapons; cosmetics excluded).

Output model (legendary_mods.json)
----------------------------------
{
  version, generated, count,
  groups: [ { id, title, star, tierName, note, mods: [ mod, ... ] }, ... ]
}
mod = { id, name, star, types[], effect, edid, formid, weaponCompat,
        howToObtain, sourceWeapon(optional, unique only) }

Usage:
  python build_legendary_mods_json.py
  python build_legendary_mods_json.py --tsv-root tsv --outdir dist
"""

import argparse
import csv
import glob
import importlib.util
import json
import os
import re
from datetime import date

# ── tier display names (from the retired star pages) ──────────────
TIER_NAME = {1: "Prefix", 2: "Major", 3: "Minor", 4: "Minor Extra"}
GROUP_TITLE = {
    1: "1 Star Legendary Mods (Prefix)",
    2: "2 Star Legendary Mods (Major)",
    3: "3 Star Legendary Mods (Minor)",
    4: "4 Star Legendary Mods (Minor Extra)",
    5: "5 Star Legendary Mods",
    "unique": "Unique Legendary Mods",
}

STAR_TXT = {
    "Legendary Mod ¬": 1,
    "Legendary Mod ¬¬": 2,
    "Legendary Mod ¬¬¬": 3,
    "Legendary Mod ¬¬¬¬": 4,
    "Legendary Mod ¬¬¬¬¬": 5,
}

POOL_RX = re.compile(
    r"Item Pool|Random Legendary|Bounty Legendary|<Prefix>|"
    r"Legendary Melee Mod$|Legendary RangedMod$|Legendary Armor Mod$|"
    r"Power Armor Legendary Mod$"
)

# cosmetic custom-mod suffixes to drop from the Unique list
COSMETIC_RX = re.compile(r"_(Appearance|Paint|Skin|CustomName)$", re.I)

# gameplay properties that mark a custom mod as a real "effect" (not naming/paint)
GAMEPLAY_PROPS = {"Enchantments", "Perk", "Ability", "OverrideProjectile"}

# supplemental effect text for craftable mods missing a curated description
SUPPLEMENTAL_DESC = {
    "Punishing": "Reflects a portion of melee damage back at the attacker.",
}

# curated / hand-verified effect text for unique weapon mods
UNIQUE_DESC = {
    "Meadow Breeze Sprayer": "Sprays Meadow Breeze - applies a fire damage-over-time.",
    "Prototype ABX03": "Sprays Meadow Breeze - applies a fire damage-over-time.",
    "Cosmic Knife": "Deals bonus cryo/energy damage on hit.",
    "Head Hunter": "Attacks cause bleeding (damage over time).",
    "Ice Breaker": "Adds cryo damage to attacks.",
    "Incendiary": "Rounds are incendiary, adding fire damage over time.",
    "Piercing Love": "Adds poison damage that ignores a portion of resistance.",
    "The Gutter": "Adds poison damage over time on hit.",
    "Poison": "Adds poison damage on hit.",
}

# derive a short effect from an enchantment EDID when nothing else is known
ENCH_KEYWORDS = [
    ("cryo", "Adds cryo damage."), ("fire", "Adds fire damage over time."),
    ("flamer", "Adds fire damage over time."), ("burn", "Adds fire damage over time."),
    ("poison", "Adds poison damage."), ("bleed", "Causes bleeding (damage over time)."),
    ("shock", "Adds electrical damage."), ("rad", "Adds radiation damage."),
    ("circuit", "+15% Reload Speed."),
]


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def clean_name(name):
    """Strip the ' Legendary Mod' display suffix some FULL names carry."""
    return re.sub(r"\s+Legendary Mod$", "", name).strip()


def load_drop_chance_module(src_dir):
    """Import the sibling drop-chances builder so we can call build_pools()
    in-process (generative) instead of depending on its committed dist JSON
    or on workflow step ordering."""
    path = os.path.join(src_dir, "build_legendary_mod_drop_chances_json.py")
    spec = importlib.util.spec_from_file_location("_dc", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def cat_of(edid):
    if "PowerArmor" in edid:
        return "Power Armour"
    if "Weapon" in edid:
        return "Weapon"
    if "Armor" in edid:
        return "Armour"
    return "Other"


def read_omod_live(path):
    """Return dict: (star,name) -> {types:set, desc, edid, formids:{type:fid}}"""
    out = {}
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        r = csv.reader(f, delimiter="\t")
        h = next(r)
        ix = {k: i for i, k in enumerate(h)}
        E, FID, FU, DE, AP = (ix["OMOD_EDID"], ix["OMOD_FormID"], ix["FULL"],
                              ix["DESC"], ix["AttachPoint_Name"])
        for row in r:
            def g(i):
                return row[i] if len(row) > i else ""
            edid = g(E)
            # only standard craftable legendary mods here; uniques handled separately
            if not re.search(r"Legendary_(Weapon|Armor|PowerArmor)", edid):
                continue
            if edid.startswith(("zzz_", "ZZZ_", "TEST_", "DEL_")):
                continue
            if "Custom" in edid or "CircuitBreaker" in edid:
                continue
            star = STAR_TXT.get(g(AP))
            name = clean_name(g(FU).strip())
            if not star or not name or POOL_RX.search(name):
                continue
            typ = cat_of(edid)
            key = (star, name)
            e = out.setdefault(key, {"types": set(), "desc": "", "edid": edid,
                                     "formids": {}})
            if typ != "Other":
                e["types"].add(typ)
                e["formids"].setdefault(typ, g(FID))
            if not e["desc"] and g(DE).strip():
                e["desc"] = g(DE).strip()
            if "Weapon" in edid:  # prefer a weapon EDID as representative
                e["edid"] = edid
    return out


def how_to_obtain(star):
    tier = TIER_NAME.get(star, "")
    label = f"{star}★ ({tier})" if tier else f"{star}★"
    return (
        f"{label} legendary mods are learned by scrapping legendary items that "
        f"carry the effect (a chance each scrap) at a Legendary Crafting station. "
        f"Once learned, craft the mod using Legendary Modules and a Legendary Core. "
        f"They can also appear on rolled gear bought from Purveyor Murmrgh."
    )


def build_master(pools, descs, live):
    """Merge curated pools + live OMOD into per-(star,name) mods."""
    master = {}  # (star,name) -> mod dict

    for pool in pools.values():
        star = pool["star"]
        typ = pool["itemType"]
        for m in pool["mods"]:
            name = clean_name(m["name"].strip())
            key = (star, name)
            d = master.setdefault(key, {
                "name": name, "star": star, "types": set(),
                "effect": "", "edid": "", "formid": "", "weaponCompat": None,
            })
            d["types"].add(typ)
            if not d["formid"] and m.get("formId"):
                d["formid"] = m["formId"]
            if not d["effect"]:
                d["effect"] = (m.get("description") or descs.get(name, "")
                               or SUPPLEMENTAL_DESC.get(name, ""))
            if typ == "Weapon":
                d["weaponCompat"] = m.get("weaponCompat")

    for (star, name), info in live.items():
        key = (star, name)
        d = master.get(key)
        if d is None:
            d = master[key] = {
                "name": name, "star": star, "types": set(),
                "effect": "", "edid": "", "formid": "", "weaponCompat": None,
            }
        d["types"].update(info["types"])
        if not d["effect"]:
            d["effect"] = (info["desc"] or descs.get(name, "")
                           or SUPPLEMENTAL_DESC.get(name, ""))
        if not d["edid"]:
            d["edid"] = info["edid"]
        if not d["formid"]:
            for fid in info["formids"].values():
                if fid:
                    d["formid"] = fid
                    break

    out = []
    type_order = ["Weapon", "Armour", "Power Armour"]
    for (star, name), d in master.items():
        types = [t for t in type_order if t in d["types"]]
        out.append({
            "id": f"star{star}-{slugify(name)}",
            "name": name,
            "star": star,
            "types": types,
            "effect": (d["effect"] or "").strip(),
            "edid": d["edid"],
            "formid": d["formid"],
            "weaponCompat": d["weaponCompat"],
            "howToObtain": how_to_obtain(star),
        })
    return out


def _ench_effect(cm):
    """Derive a short effect string from the mod's enchantment EDID, if any."""
    for e in (cm.get("effects") or []):
        if e.get("property") in ("Enchantments", "Perk", "Ability"):
            val = (e.get("value") or "").lower()
            for kw, txt in ENCH_KEYWORDS:
                if kw in val:
                    return txt
    return ""


def build_unique(uw_path, descs):
    data = json.load(open(uw_path, encoding="utf-8"))
    weapons = data.get("weapons", [])
    by_name = {}
    for w in weapons:
        wname = (w.get("name") or "").strip()
        for cm in (w.get("customMods") or []):
            edid = (cm.get("edid") or "").strip()
            name = (cm.get("name") or "").strip()
            if not edid or COSMETIC_RX.search(edid):
                continue  # skip cosmetics (paint / appearance / skin / custom name)
            props = {e.get("property", "") for e in (cm.get("effects") or [])}
            cur_desc = ((cm.get("description") or "").strip()
                        or UNIQUE_DESC.get(name, "") or descs.get(name, ""))
            # keep only genuine effects: a real gameplay property OR a known description
            if not (props & GAMEPLAY_PROPS) and not cur_desc:
                continue
            if not name or name == edid:
                name = re.sub(r"^.*?mod_[Cc]ustom_", "", edid)
                name = re.sub(r"_(Effect|SpecialEffect)$", "", name)
                name = re.sub(r"(?<!^)(?=[A-Z])", " ", name).strip()
            name = re.sub(r"\s+(Custom Mod|Special Effect|Custom Name)$", "", name).strip()
            effect = cur_desc or UNIQUE_DESC.get(name, "") or _ench_effect(cm)
            rec = by_name.get(name)
            if rec is None:
                rec = by_name[name] = {
                    "id": "uniq-" + slugify(name),
                    "name": name, "star": None, "types": ["Weapon"],
                    "effect": effect, "edid": edid,
                    "formid": (cm.get("formId") or "").strip(),
                    "weaponCompat": None, "sourceWeapons": set(),
                    "howToObtain": "",
                }
            if not rec["effect"] and effect:
                rec["effect"] = effect
            if wname:
                rec["sourceWeapons"].add(wname)
    out = []
    for name, rec in by_name.items():
        srcs = sorted(rec.pop("sourceWeapons"))
        rec["sourceWeapon"] = ", ".join(srcs)
        rec["howToObtain"] = (
            f"Comes pre-installed on {rec['sourceWeapon']}. This effect is unique "
            f"to that weapon - it cannot be crafted, rerolled onto other gear, or removed."
            if srcs else
            "Unique effect baked onto a named weapon; not craftable."
        )
        out.append(rec)
    return out


def build_channel(channel, base, tsv_dir, dist_dir):
    """Build legendary_mods.json + murmrgh_legendary_mods.json for one channel."""
    src_dir = os.path.join(base, "src")
    os.makedirs(dist_dir, exist_ok=True)
    print(f"  Building legendary mods -- channel: {channel}")

    # curated pools computed in-process (generative; no dist / step-order dependency)
    dc = load_drop_chance_module(src_dir)
    pools = dc.build_pools()
    descs = dict(dc.EFFECT_DESCRIPTIONS)

    cands = [f for f in glob.glob(os.path.join(tsv_dir, "OMOD_Export_*.tsv"))
             if "Properties" not in f]
    if not cands:
        print(f"  [warn] no OMOD_Export_*.tsv in {tsv_dir}; skipping {channel}")
        return
    omod_path = max(cands, key=os.path.getmtime)
    live = read_omod_live(omod_path)

    master = build_master(pools, descs, live)

    uw_path = os.path.join(dist_dir, "unique_weapons", "unique_weapons.json")
    if os.path.exists(uw_path):
        unique = build_unique(uw_path, descs)
    else:
        print(f"  [warn] {uw_path} not found; Unique group will be empty this run")
        unique = []

    groups = []
    for star in (1, 2, 3, 4):
        mods = sorted([m for m in master if m["star"] == star],
                      key=lambda m: m["name"].lower())
        groups.append({
            "id": f"star{star}", "title": GROUP_TITLE[star], "star": star,
            "tierName": TIER_NAME[star], "note": None, "mods": mods,
        })
    groups.append({
        "id": "star5", "title": GROUP_TITLE[5], "star": 5, "tierName": None,
        "note": "There are currently no 5 star mods in the game.", "mods": [],
    })
    groups.append({
        "id": "unique", "title": GROUP_TITLE["unique"], "star": None,
        "tierName": None, "note": None,
        "mods": sorted(unique, key=lambda m: m["name"].lower()),
    })

    total = sum(len(g["mods"]) for g in groups)
    out = {
        "version": 1, "generated": str(date.today()),
        "source_files": [os.path.basename(omod_path),
                         "legendary_mod_drop_chances.json",
                         "unique_weapons/unique_weapons.json"],
        "count": total, "groups": groups,
    }
    out_path = os.path.join(dist_dir, "legendary_mods.json")
    json.dump(out, open(out_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
    print(f"[legendary_mods] wrote {out_path}  ({total} mods, {len(groups)} groups)")
    for g in groups:
        print(f"    {g['title']:36s} {len(g['mods'])}")

    # ── Murmrgh's list: standard (non-bounty) craftable pool, per star ──
    std_names = {}
    for pool in pools.values():
        if pool.get("poolType") != "standard":
            continue
        std_names.setdefault(pool["star"], set())
        for m in pool["mods"]:
            std_names[pool["star"]].add(clean_name(m["name"].strip()))
    master_by_key = {(m["star"], m["name"]): m for m in master}
    murm_groups = []
    for star in (1, 2, 3, 4):
        names = sorted(std_names.get(star, set()), key=str.lower)
        mods = [master_by_key[(star, n)] for n in names if (star, n) in master_by_key]
        murm_groups.append({
            "id": f"star{star}",
            "title": f"{star} Star Legendary Mods ({TIER_NAME[star]})",
            "star": star, "mods": mods,
        })
    murm = {
        "version": 1, "generated": str(date.today()), "vendor": "Purveyor Murmrgh",
        "note": ("Legendary effects that can appear on the legendary gear Purveyor "
                 "Murmrgh sells for Legendary Scrip. Grouped by star, A-Z."),
        "count": sum(len(g["mods"]) for g in murm_groups), "groups": murm_groups,
    }
    murm_path = os.path.join(dist_dir, "murmrgh_legendary_mods.json")
    json.dump(murm, open(murm_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
    print(f"[murmrgh] wrote {murm_path}  ({murm['count']} mods)")

    # patchlog feed (keeps the patch-tracking pipeline happy)
    try:
        from patchlog_utils import write_empty_patchlog_feed
        write_empty_patchlog_feed(dist_dir, "patchlog_latest_bnb_legendary_mods.json",
                                  current_count=total)
    except Exception as e:  # non-fatal
        print(f"  [warn] patchlog feed skipped: {e}")


def main():
    ap = argparse.ArgumentParser(
        description="Build legendary_mods.json + murmrgh_legendary_mods.json "
                    "(live and/or PTS channels).")
    ap.add_argument("--channel", choices=["live", "pts", "both"], default="live",
                    help="Which channel to build (default: live). The PTS workflow "
                         "normalises tsv/pts -> tsv and relocates dist -> dist/pts, "
                         "so it calls this with the default 'live'.")
    args = ap.parse_args()

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tsv_dir = os.path.join(base, "tsv")
    dist_dir = os.path.join(base, "dist")

    if args.channel in ("live", "both"):
        build_channel("live", base, tsv_dir, dist_dir)
    if args.channel in ("pts", "both"):
        pts_tsv = os.path.join(tsv_dir, "pts")
        if os.path.isdir(pts_tsv):
            build_channel("pts", base, pts_tsv, os.path.join(dist_dir, "pts"))
        else:
            print(f"  PTS: skipping -- {pts_tsv} does not exist.")


if __name__ == "__main__":
    main()
