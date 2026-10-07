#!/usr/bin/env python3
r"""
weapon_shop_skins.py — Atom Shop and Scoreboard weapon skins on the Weapon page.

WHY
---
Duchess, 7 Oct 2026: the Weapon page only listed skins you learn from a plan,
which left most of the skins a player actually owns off the page — the Free
States, Gilded, Tricentennial and Woodland Camo paints for the 10mm alone. These
come from the Atom Shop or a Scoreboard season, not a plan, so there is nothing
to "learn": they are shown under each weapon in their own sub-expand,
"Atom Shop & Scoreboard Skins", with a greyed-out checkbox that never counts
towards progress.

WHAT IS A ROW
-------------
Read off the ENTM (entitlement) export, never a hand list:

  * an ATX_ / SCORE_ entitlement the game files as a weapon skin — its
    KEYWORDS carry ATX_Entitlement_Filter_Store_Skin_Weapons, or its EditorID
    says Skin_WeaponSkin / Skin_WeaponModel
  * not dev / cut (zzz, DEL_, DEBUG, TEMPLATE, REUSE) and not a Nuclear Winter
    (Babylon_) reward — those are Legacy NW plans and rewards, and show on the
    Weapon page through add_weapon_groups' also_pages tag instead
  * it unlocks at least one COBJ (ReferencedBy) that makes a weapon OMOD. One
    row per weapon the skin fits, so a paint sold for three guns sits under all
    three. The COBJ and its OMOD are the row's cobj / cnam, which is what lets
    add_weapon_groups find the weapon exactly as it does for a plan paint.
  * no live plan already teaches that same recipe or OMOD — then the skin is
    an In Game Skin and the plan row already shows it.

`kind` stays "plan" (selectPlanRows filters on it) and the id is
SKIN_<ENTM FormID>_<COBJ FormID>, stable across builds.

Usage (library): plan_recipe_rows.attach() calls attach(items, tsv_dir).
"""

import collections
import csv
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_plan_obtain_json as bpo
import plan_sources

csv.field_size_limit(10 ** 9)

_RX_SKIN_EDID = re.compile(r"skin_?weapon(skin|model)|_weaponskin_|_weaponmodel_", re.I)
_RX_DEV = re.compile(r"^(zzz|del_|de_|debug|template|reuse|cut_|test)|_test_|nonplayable", re.I)
_RX_NW = re.compile(r"^babylon_", re.I)
_RX_SEASON = re.compile(r"^SCORE_S(\d+)_", re.I)
_RX_MINI = re.compile(r"^SCORE_MiniSeason_", re.I)
SKIN_KW = "ATX_Entitlement_Filter_Store_Skin_Weapons"
# The store files camera, fishing-rod and power-armour paints under the weapon
# skin filter too; they belong to their own pages, never the Weapon page.
_RX_NOT_WEAPON = re.compile(r"camera|fishing|power_?armou?r|_armou?r_|backpack|photomode",
                            re.I)

LEAD = ("Not a plan — this skin comes from the {where}. It unlocks on your "
        "account, so it is listed here for reference and does not count "
        "towards your progress.")


def _source(edid):
    """("Atom" | "Score", plain-English unlock sentence, where-phrase)."""
    m = _RX_SEASON.match(edid)
    if m:
        n = int(m.group(1))
        return ("Score", f"Scoreboard reward — Season {n}.", f"Season {n} Scoreboard")
    if _RX_MINI.match(edid):
        return ("Score", "Scoreboard reward — mini-season.", "mini-season Scoreboard")
    return ("Atom", "Sold in the Atom Shop.", "Atom Shop")


def _refs(cell):
    """[(FormID, EDID)] for the COBJ entries in a ReferencedBy cell."""
    out = []
    for part in (cell or "").split("|"):
        bits = part.split(":")
        if len(bits) >= 3 and bits[-1].strip().upper() == "COBJ":
            out.append((bits[0].strip().upper(), bits[1].strip()))
    return out


def build(items, tsv_dir="tsv", stats=None):
    stats = stats if stats is not None else collections.Counter()
    tsv_dir = os.path.abspath(tsv_dir)
    bpo.TSV = tsv_dir
    if not bpo.SIG_INDEX:
        bpo.SIG_INDEX = bpo.build_sig_index()
    cobj_idx = bpo.build_cobj_index()
    path = bpo.newest("ENTM_Export_*.tsv", tsv_dir)
    if not path:
        stats["no_entm_export"] += 1
        return [], stats

    live = [it for it in items if not it.get("cut") and not it.get("shop_skin")]
    covered_co = {((it.get("cobj") or {}).get("formid") or "").upper() for it in live}
    covered_om = {((it.get("cnam") or {}).get("formid") or "").upper() for it in live}
    covered_ent = {((it.get("nw_entitlement") or {}).get("edid") or "").lower() for it in live}
    covered_co.discard(""); covered_om.discard(""); covered_ent.discard("")

    rows, seen = [], set()
    for r in bpo.read_rows(path):
        edid = (r.get("EDID") or "").strip()
        full = (r.get("FULL") or "").strip()
        kws = r.get("KEYWORDS") or ""
        if not edid or not full:
            continue
        if not (SKIN_KW in kws or _RX_SKIN_EDID.search(edid)):
            continue
        if _RX_DEV.search(edid) or _RX_NW.match(edid):
            stats["skipped_dev_or_nw"] += 1
            continue
        if edid.lower() in covered_ent:
            continue
        fid = (r.get("FormID") or "").strip().upper()
        tag, sentence, where = _source(edid)
        tex = os.path.splitext((r.get("ETDI") or "").strip())[0].lower()
        for co_fid, co_edid in _refs(r.get("ReferencedBy")):
            c = cobj_idx.get(co_fid) or {}
            om = (c.get("cnam_fid") or "").upper()
            if bpo.SIG_INDEX.get(om) != "OMOD":
                continue
            if _RX_NOT_WEAPON.search((c.get("cnam_edid") or "") + " " + edid):
                stats["not_a_weapon_skin"] += 1
                continue
            if co_fid in covered_co or om in covered_om:
                stats["covered_by_a_plan"] += 1
                continue
            rid = f"SKIN_{fid}_{co_fid}"
            if rid in seen:
                continue
            seen.add(rid)
            row = {
                "kind": "plan", "shop_skin": True, "brand": "df", "type": "weapon",
                "id": rid, "name": full,
                "has_image_box": True, "image_dir": "weapons",
                "obtain": LEAD.format(where=where),
                "category_label": "Weapon skin (" + ("Scoreboard" if tag == "Score" else "Atom Shop") + ")",
                "obtain_routes": [], "obtain_unlocks": [sentence],
                "plan_item": None,
                "cobj": {"formid": co_fid, "edid": co_edid or c.get("edid") or ""},
                "cnam": {"formid": om, "edid": c.get("cnam_edid") or "", "sig": "OMOD"},
                # texture: the storefront art name (ETDI), read by plan_images.
                "entitlement": {"formid": fid, "edid": edid, "name": full,
                                "texture": tex},
                # Account-bound: an entitlement cannot change hands.
                "tradeable": False, "stops_dropping": None, "effects": None,
                "cut": False, "cut_reason": None, "changes": [],
                "shop_source": tag, "source_tag": tag,
            }
            row["obtain_ledger"] = plan_sources.obtain_ledger(row)
            rows.append(row)
            stats["emitted_" + tag] += 1
    return rows, stats


def attach(items, tsv_dir="tsv", stats=None):
    """Replace the shop-skin rows in *items* with a fresh set."""
    stats = stats if stats is not None else collections.Counter()
    items[:] = [it for it in items if not it.get("shop_skin")]
    rows, stats = build(items, tsv_dir, stats)
    items.extend(rows)
    return stats


if __name__ == "__main__":
    import json
    path = sys.argv[1] if len(sys.argv) > 1 else "dist/plan_master.json"
    d = json.load(open(path, encoding="utf-8"))
    rows, st = build(d["items"], "tsv")
    print(dict(st))
    for r in rows[:20]:
        print(r["id"], r["name"], r["cnam"]["edid"])
