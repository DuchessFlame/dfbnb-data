#!/usr/bin/env python3
r"""
weapon_unique_versions.py — unique weapons listed under their base weapon on the
Weapon plan checklist (/df/plan-checklists/weapon/).

WHY
---
Duchess, 11 Oct 2026: the Weapon page was missing the "reskin versions" — the
unique named weapons that are a base weapon with its own look and effect
(Tempest on the Assaultron Blade, The Pipe on the Pipe Gun, Holy Fire ...).
They have no plan, so they are listed under their base weapon in their own
sub-expand, "Unique Versions", with a greyed-out checkbox that never counts
towards progress — the same treatment as the Atom Shop & Scoreboard skins.

WHERE THE LIST COMES FROM
-------------------------
Never a hand list. The Unique Weapons & Armour page already resolves every
unique from the game files (build_unique_weapons_armour_json.py), with its
base weapon, its WEAP record and its How to Obtain routes. This module reads
that finished JSON for the same channel and turns each WEAPON entry into a
row. A unique that already has a plan row on the Weapon page (The Fixer,
V63 Zweihaender, Ultracite Terror Sword ...) is skipped — it is already there.

ROW SHAPE
---------
kind "plan" (selectPlanRows filters on it), type "weapon", plan_page "weapon",
unique_version True, id UNIQ_<slug of the unique's name>. Its routes become
plain unlock sentences worded so plan_sources' ledger bucketer files each one
under the same route the uniques page shows. add_weapon_groups gives it
weapon_role "unique" and finds its weapon from the WEAP EditorID.

Usage (library): add_weapon_groups.attach() calls attach(items).
"""

import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

import plan_sources

# Art: the weapon page's own upload folder, matched on the server listing so a
# row only points at a file that is actually there.
IMG_DIR = "guide-images/plan-checklist/weapons/"
IMG_URL = "/wp-content/uploads/" + IMG_DIR

# One word for the source pill, per uniques-page route.
ROUTE_TAG = {"Quests": "Quest", "Events & Activities": "Events",
             "Scoreboard": "Score", "Caps": "Vendor", "Stamps": "Vendor",
             "Gold Bullion": "Gold", "Atom Shop": "Atom",
             "Limited Time Bundle": "Atom", "Challenges": "CHAL"}

LEAD = ("Not a plan — {name} is a unique version of the {base}. You get it "
        "ready-made, so it is listed here for reference and does not count "
        "towards your progress.")


def _slug(s):
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", (s or "").lower())).strip("-")


def _norm(s):
    s = re.sub(r"^\s*(plan|schematic)\s*:\s*", "", s or "", flags=re.I)
    return re.sub(r"[^a-z0-9]", "", s.lower().replace("the ", ""))


def uniques_path(tsv_dir):
    """The uniques JSON for the channel tsv_dir belongs to (tsv/pts -> dist/pts)."""
    pts = os.path.basename(os.path.normpath(tsv_dir or "")).lower() == "pts"
    sub = os.path.join("dist", "pts") if pts else "dist"
    return os.path.join(ROOT, sub, "unique_weapons_armour", "unique_weapons_armour.json")


def server_images():
    """Set of file names in the weapon image folder, from data/server_listing.tsv."""
    out = set()
    path = os.path.join(ROOT, "data", "server_listing.tsv")
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("#"):
                    continue
                p = line.split("\t", 1)[0].strip()
                if p.startswith(IMG_DIR):
                    out.add(p[len(IMG_DIR):])
    except OSError:
        pass
    return out


def image_for(name, files, extra=()):
    """First hosted .avif whose name matches the item, or None."""
    for stem in [_slug(name), _slug(name) + "-paint", *map(_slug, extra)]:
        if stem and stem + ".avif" in files:
            return IMG_URL + stem + ".avif"
    return None


def _sentence(route, lines):
    src = ""
    for l in lines:
        if l.lower().startswith("source:"):
            src = l.split(":", 1)[1].strip()
            break
    gone = any("no longer obtainable" in l.lower() for l in lines)
    if gone:
        return f"No longer obtainable: {src or 'retired'}. Player trading only.", True
    if route == "Quests":
        return f"Quest reward: {src}.", False
    if route == "Scoreboard":
        return f"Scoreboard: {src}.", False
    if route == "Challenges":
        return f"Challenge reward: {src}.", False
    if route == "Caps":
        return f"Sold for caps: {src}.", False
    if route == "Gold Bullion":
        return f"Sold for gold bullion: {src}.", False
    if route == "Stamps":
        return f"Sold for stamps: {src}.", False
    if route in ("Atom Shop", "Limited Time Bundle"):
        return f"Atom Shop: {src}.", False
    return f"Drops from: {src}." if src else "Drops in game.", False


def build(items, tsv_dir="tsv"):
    path = uniques_path(tsv_dir)
    try:
        doc = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return [], {"no_uniques_json": 1}

    on_page = {_norm(it.get("display_name") or it.get("name"))
               for it in items
               if (it.get("plan_page") == "weapon" or (it.get("type") == "weapon"))
               and not it.get("unique_version")}
    files = server_images()
    rows, stats, seen = [], {"emitted": 0, "has_plan_row": 0, "with_image": 0}, set()
    for u in doc.get("items") or []:
        if (u.get("kind") or "") != "Weapon" or u.get("isCut") or u.get("cosmeticOnly"):
            continue
        name = (u.get("name") or "").strip()
        if not name:
            continue
        if _norm(name) in on_page:
            stats["has_plan_row"] += 1
            continue
        rid = "UNIQ_" + _slug(name).upper().replace("-", "_")
        if rid in seen:
            continue
        seen.add(rid)
        base = (u.get("base") or "").strip() or "base weapon"
        unlocks, retired, tag = [], False, None
        for r in u.get("obtainRoutes") or []:
            if not r.get("populated"):
                continue
            s, gone = _sentence(r.get("route"), r.get("lines") or [])
            unlocks.append(s)
            retired = retired or gone
            tag = tag or ROUTE_TAG.get(r.get("route"))
        img = image_for(name, files)
        row = {
            "kind": "plan", "brand": "df", "type": "weapon",
            "unique_version": True, "id": rid, "name": name,
            "has_image_box": True, "image_dir": "weapons",
            "images": [img] if img else [], "image_source": "server" if img else "",
            "obtain": LEAD.format(name=name, base=base),
            "category_label": "Unique weapon (no plan)",
            "obtain_routes": [], "obtain_unlocks": unlocks,
            "plan_item": None, "cobj": None,
            "cnam": {"formid": u.get("formId") or "", "edid": u.get("edid") or "", "sig": "WEAP"},
            "unique_base": base,
            "unique_effect": u.get("inherentEffect") or "",
            # Shown in italics under the picture (Item Image).
            "desc": u.get("inherentEffect") or "",
            "tradeable": u.get("tradeable"), "stops_dropping": None, "effects": None,
            "cut": False, "cut_reason": None, "changes": [],
            "plan_page": "weapon", "plan_page_group": None,
        }
        if retired and len(unlocks) == 1:
            row["not_obtainable"] = True
        elif tag:
            row["source_tag"] = tag
        row["obtain_ledger"] = plan_sources.obtain_ledger(row)
        rows.append(row)
        stats["emitted"] += 1
        stats["with_image"] += bool(img)
    return rows, stats


def attach(items, tsv_dir="tsv"):
    """Replace the unique-version rows in *items* with a fresh set."""
    items[:] = [it for it in items if not it.get("unique_version")]
    rows, stats = build(items, tsv_dir)
    items.extend(rows)
    return stats


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "dist", "plan_master.json")
    d = json.load(open(path, encoding="utf-8"))
    rows, st = build(d["items"], os.path.join(ROOT, "tsv"))
    print(st)
    for r in rows:
        print(r["id"], "|", r["name"], "|", r["unique_base"], "|", r.get("source_tag"), r["obtain_unlocks"], r["images"])
