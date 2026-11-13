#!/usr/bin/env python3
r"""
build_world_pet_rewards_json.py - World Pets - Leveling Rewards checklist data.

  PGTR export (tools/export_pgtr_from_esm.py)  -> the 4 level-up tracks, level by level
  GMRW export                                  -> what each reward record gives
  ENTM export                                  -> cosmetic / icon / title names + store icon
  ARMO export                                  -> pet buff item names
      -> dist/world_pet_rewards.json   (--pts: dist/pts/world_pet_rewards.json)
      -> df-bnb-world-pet-rewards.js   (/df/pets/world-pets/world-pets-leveling-rewards/)

Built 13 Nov 2026. Nothing on the page is typed in: names and descriptions are the
game's own Pet Progression text (PGTR NAM1 / NAM3), levels are PGTR NAM0.

PICTURES (Duchess's rule: transparent store icon for items)
  - Pet / player apparel: the transparent store icon from the ENTM (ETIP + ETDI),
    saved as  guide-images/pets/world-pets/leveling-rewards/<texture name>.avif
  - Player icons: the Atom Shop player-icon art already on the server
    (guide-images/atom-shop/player-icons/<texture>.avif) - no second copy.
  - Titles, buffs, Pet Carrier, Pet Prowess, currency: the game's own Pet
    Progression picture (textures/interface/petprogression/wpimg*.dds), named
    after the entry's WorldPets_Img_* keyword.
  Every item also carries imageFallback = its wpimg picture.

LIVE vs PTS: when a channel has no PGTR export (World Pets not live yet) the JSON is
written with species = [] and the renderer falls back to the PTS file.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import tsv_source  # noqa: E402

SERVER = "https://theduchessflame.com/wp-content/uploads/guide-images/"
IMG_DIR = SERVER + "pets/world-pets/leveling-rewards/"
ICON_DIR = SERVER + "atom-shop/player-icons/"
SPECIES = [("cat", "Cat"), ("dog", "Dog"), ("radhog", "Radhog"), ("deathclaw", "Deathclaw")]
SP_SHORT = {"deathclaw": "dc"}

# Page "Type" groups (filter bar + pill), from the game's own category text.
TYPE_OF = {
    "Pet Buff": ("buff", "Pet Buff"),
    "Pet Buff (upgrade)": ("buff", "Pet Buff"),
    "Carry Capacity": ("buff", "Pet Carrier"),
    "Carry Capacity (upgrade)": ("buff", "Pet Carrier"),
    "Combat Upgrade": ("buff", "Pet Prowess"),
    "Pet Apparel": ("apparel", "Pet Apparel"),
    "Player Apparel": ("apparel", "Player Apparel"),
    "Player Icon": ("icon", "Player Icon"),
    "Player Title": ("title", "Player Title"),
    "Currency": ("currency", "Currency"),
    "Items": ("currency", "Items"),
}


def read_tsv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


def clean(s):
    s = (s or "").replace("�", "★").strip()
    return re.sub(r"\s+", " ", s)


def wpimg_name(kw):
    """WorldPets_Img_Deathclaw_MeatLover01 -> wpimgdcmeatlover01 (the texture's file name)."""
    if not kw:
        return ""
    n = kw.lower().replace("worldpets_img_", "").replace("_", "").replace("deathclaw", "dc")
    if n.startswith("allpetcarrier"):
        n = n[3:]  # the carrier pictures have no "all" in the file name
    return "wpimg" + n


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args(argv)
    ch = "pts" if a.pts else "live"
    out = os.path.join(ROOT, "dist", "pts" if a.pts else "", "world_pet_rewards.json")

    pgtr_p = tsv_source.newest("PGTR_Export_*.tsv", channel=ch, required=False)
    doc = {"title": "World Pets - Leveling Rewards", "channel": ch.upper(),
           "built": datetime.date.today().isoformat(), "imageBase": IMG_DIR, "species": []}
    if not pgtr_p:
        doc["note"] = "No World Pets progression tracks in this channel's game files."
        return write(out, doc)

    gmrw = {r["EDID"]: r for r in read_tsv(tsv_source.newest("GMRW_Export_*.tsv", channel=ch))}
    entm_rows = read_tsv(tsv_source.newest("ENTM_Export_*.tsv", channel=ch))
    entm = {r["EDID"]: r for r in entm_rows if r["EDID"].startswith("WorldPets_ENTM_")}
    armo_p = tsv_source.newest("ARMO_Export_*_ARMOUR.tsv", channel=ch)
    armo = {r["ARMO_FormID"]: r for r in read_tsv(armo_p)}
    pgtr = read_tsv(pgtr_p)
    doc["sources"] = [os.path.basename(p) for p in (pgtr_p, armo_p)] + [
        os.path.basename(tsv_source.newest(p, channel=ch)) for p in ("GMRW_Export_*.tsv", "ENTM_Export_*.tsv")]

    def entm_for(reward_edid, sp_name):
        """GMRW WorldPets_LvReward_<Sp>_<KIND>_<n>_<What> -> WorldPets_ENTM_<Sp>_<KIND>_<n>_<What>."""
        m = re.match(r"WorldPets_LvReward_(\w+?)_(COSM|ICON|TITL)_(.+)$", reward_edid or "")
        if not m:
            return None
        key = "WorldPets_ENTM_%s_%s_%s" % m.groups()
        if key in entm:
            return entm[key]
        if m.group(3).endswith("TamerOutfit"):  # ENTM names the outfit <Sp>Tamer / DCTamer
            pre = "WorldPets_ENTM_%s_COSM_01_" % m.group(1)
            hit = [e for k, e in entm.items() if k.startswith(pre) and k.endswith("Tamer")]
            return hit[0] if hit else None
        return None

    for key, name in SPECIES:
        rows = [r for r in pgtr if r["Track_EDID"].lower() == ("worldpets_progressiontrack_%s_new" % key)]
        if not rows:
            continue
        track = {"formid": rows[0]["Track_FormID"], "edid": rows[0]["Track_EDID"]}
        alts = {r["Level"]: r for r in rows if r["Slot"] == "alt"}
        items = []
        for r in [x for x in rows if x["Slot"] == "main"]:
            lvl = int(r["Level"])
            tkey, tlabel = TYPE_OF.get(r["Category"], ("currency", r["Category"] or "Reward"))
            wp = wpimg_name(r["Img_KYWD"])
            fallback = IMG_DIR + wp + ".avif" if wp else ""
            image = fallback
            records = [{"label": "Track entry (PGTR)", "formid": track["formid"], "edid": track["edid"] + " - Level %d" % lvl}]
            if r["Reward_EDID"]:
                records.append({"label": "Reward (GMRW)", "formid": r["Reward_FormID"], "edid": r["Reward_EDID"]})
            note = ""
            e = entm_for(r["Reward_EDID"], name)
            if e:
                records.append({"label": "Entitlement (ENTM)", "formid": e["FormID"], "edid": e["EDID"]})
                tex = (e.get("ETDI") or "").strip()
                if tkey == "apparel" and tex:
                    image = IMG_DIR + os.path.splitext(tex)[0].lower() + ".avif"
                elif tkey == "icon" and tex:
                    image = ICON_DIR + os.path.splitext(tex)[0].lower() + ".avif"
            g = gmrw.get(r["Reward_EDID"]) or {}
            item = (g.get("RewardedItem") or "").split(":")
            if len(item) >= 3 and item[2] == "ARMO" and item[0] in armo:
                records.append({"label": "Pet buff (ARMO)", "formid": item[0], "edid": item[1]})
            # the game says one thing and gives another (Radhog level 75, PTS Oct 2026)
            if r["Reward_EDID"].endswith("LegendaryModules01") and "Lunchbox" in r["Name"] + r["Desc"]:
                note = ("The game's text says 10 Lunchboxes, but the reward record gives 100 Legendary "
                        "Modules. This looks like a mistake in the PTS files.")
            alt = alts.get(r["Level"])
            items.append({
                "id": "%s:%d" % (key, lvl),
                "level": lvl,
                "name": clean(r["Name"]) or clean(r["Desc"]),
                "desc": clean(r["Desc"]),
                "category": r["Category"],
                "typeKey": tkey,
                "type": tlabel,
                "image": image,
                "imageFallback": fallback if fallback != image else "",
                "alt": {"name": clean(alt["Name"]), "desc": clean(alt["Desc"])} if alt else None,
                "note": note,
                "records": records,
            })
        items.sort(key=lambda x: x["level"])
        doc["species"].append({"key": key, "name": name, "track": track, "items": items})
    return write(out, doc)


def write(path, doc):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)
        f.write("\n")
    n = sum(len(s["items"]) for s in doc["species"])
    print("[world_pet_rewards] %d rewards, %d pets -> %s" % (n, len(doc["species"]), path))


if __name__ == "__main__":
    main()
