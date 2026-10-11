#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
activities_checklist.py - the "Unique Rewards Checklist" for every
/df/activities/ All Rewards page (Oct 2026).

Called by build_activities_rewards_json.py (add_to_pages) just before the
image-index step, so every build writes page.activityData.checklist. It can
also be run on its own to add the checklist to the dist JSON that is already
committed, without re-running the whole (memory-hungry) activities build:

    python3 src/activities_checklist.py            # monolith + by_page slices
    python3 src/activities_checklist.py --slices   # just the by_page slices

WHAT GOES ON THE CHECKLIST (read from the page's own rewardTree - the same
tree the page draws, so the checklist can never list something the page
does not):

  every reward list the activity awards (each root of rewardTree) ...
    EXCEPT the shared lists every activity gives - "Activity Rewards"
      (RA_LL_Rewards_Activities) and, defensively, "Public Event Rewards" /
      "Mutated Events Rewards". Any list found INSIDE those shared lists is
      treated as shared wherever else it turns up on the page (e.g. Distant
      Thunder's quest reward re-rolls the shared Power Armour plans list -
      every activity can give those, so they stay off).
    EXCEPT regional loot pools (EditorID contains "AllRegions", or the
      "Regional Loot Pool" wrapper) - the regional aid / grab-bag loot.

  ... and from those lists only the items worth collecting:
      BOOK  plans, recipes, Player / CAMP titles
      WEAP  weapons
      ARMO  armour and apparel
      MISC  weapon mod boxes (EditorID miscmod_*) - junk and scrap stay off
  Ammo, aid / food / alcohol, currency and random legendary rolls (LGDI)
  never count (Duchess's rule: ammo, food and alcohol are not unique rewards).

RATES: each row's rate is the same per-completion chance the page's tables
multiply out (df-bnb-activities.js flattenTreeItems): the root list's GMRW
rate x every sub-list entry rate down the path x the item's own rate. Where
an item is reachable by more than one path in one list, the best path is
shown (same rule as the mutated / seasonal checklists). No new rate maths -
every number is already in the tree.

SOURCES: one line per reward list the item drops from, named the way the
page names it (unique lists are drawn under "Unique Activity Rewards"; the
Enclave Urban Scout Armour and Enclave Plasma Gun Mod Boxes lists are drawn
inside "Enclave Activity Rewards"). Tiered pages (the Power Plant repair
tiers - synthetic tier roots) list every tier, 0% where a tier never drops
the item, so readers can see none were missed.
"""

import json
import os
import re
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
DIST_DIR = REPO_ROOT / "dist" / "activities"
PLAN_MASTER = REPO_ROOT / "dist" / "plan_master.json"

UPLOADS = "/wp-content/uploads/"
PLAN_IMG_BASE = UPLOADS + "guide-images/plan-checklist/"
ACT_IMG_BASE = UPLOADS + "guide-images/activities/"

ACTIVITY_PATH = "/df/activities/"

# The shared lists every activity (or public event) awards.
SHARED_ROOT_EDIDS = {"RA_LL_Rewards_Activities"}
SHARED_ROOT_LABELS = {"activity rewards", "public event rewards", "mutated events rewards"}

UNIQUE_SECTION = "Unique Activity Rewards"
ENCLAVE_SECTION = "Enclave Activity Rewards"
# df-bnb-activities.js renderRewardTree moves these two lists inside the
# Enclave Activity Rewards expand, so the checklist names them that way too.
ENCLAVE_MOVED_RE = re.compile(r"enclave urban scout|enclave plasma gun", re.I)

CATEGORY_ORDER = ["Titles", "Plans & Recipes", "Weapons", "Armour & Apparel", "Weapon Mods"]

_TITLE_RE = re.compile(r"^\s*(player|camp)\s+title\s*:", re.I)


# ── helpers ─────────────────────────────────────────────────────────────────

def _num(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _clean_conds(conds):
    out = []
    for c in conds or []:
        s = str(c or "").strip()
        if s and not re.search(r"GetRandomPercent", s, re.I) and s not in out:
            out.append(s)
    return out


def _is_shared_root(node):
    return ((node.get("edid") or "") in SHARED_ROOT_EDIDS
            or (node.get("label") or "").strip().lower() in SHARED_ROOT_LABELS)


def _collect_edids(node, out):
    e = node.get("edid")
    if e:
        out.add(e)
    for c in node.get("children") or []:
        _collect_edids(c, out)


def _is_regional(node):
    edid = node.get("edid") or ""
    return ("allregions" in edid.lower()
            or (node.get("label") or "").strip().lower() == "regional loot pool")


def _is_tier_root(node):
    return bool(node.get("isTierWrapper")) or str(node.get("edid") or "").startswith("_synthetic_tier")


def category_for(item):
    name = item.get("name") or ""
    sig = (item.get("sig") or "").upper()
    edid = (item.get("edid") or "").lower()
    if sig == "BOOK":
        return "Titles" if _TITLE_RE.match(name) else "Plans & Recipes"
    if sig == "WEAP":
        return "Weapons"
    if sig == "ARMO":
        return "Armour & Apparel"
    if sig == "MISC" and edid.startswith("miscmod_"):
        return "Weapon Mods"
    return None


def _source_name(root, path_labels):
    """The section the page draws this root under."""
    if any((l or "").strip().lower() == ENCLAVE_SECTION.lower() for l in path_labels):
        return ENCLAVE_SECTION
    label = (root.get("label") or "").strip()
    if ENCLAVE_MOVED_RE.search(label):
        return ENCLAVE_SECTION
    if _is_tier_root(root):
        return label or UNIQUE_SECTION
    if root.get("isUniqueReward"):
        return UNIQUE_SECTION
    return label or UNIQUE_SECTION


# ── the walk ────────────────────────────────────────────────────────────────

def _walk(node, rate, conds, labels, shared, hits):
    """Yield (item, rate, conds, labels) for every item under node."""
    if node.get("type") == "leaf":
        r = rate * (_num(node.get("dropRate"), 100.0) / 100.0)
        hits.append((node, r, _clean_conds(conds + list(node.get("conditions") or [])), labels))
        return
    for it in node.get("items") or []:
        r = rate * (_num(it.get("dropRate"), 100.0) / 100.0)
        hits.append((it, r, _clean_conds(conds + list(it.get("conditions") or [])), labels))
    for ch in node.get("children") or []:
        if (ch.get("edid") or "") in shared or _is_regional(ch):
            continue
        cr = rate * (_num(ch.get("entryRate"), 100.0) / 100.0)
        _walk(ch, cr, conds + list(ch.get("conditions") or []),
              labels + [ch.get("label") or ""], shared, hits)


def _merge_conds(a, b):
    """Two paths to one reward: it drops if EITHER path's conditions are met,
    so only what both require is kept (df-bnb-activities.js mergeAltPathConds)."""
    return [c for c in a if c in b]


def build_checklist(page):
    """The checklist rows for one page (list, maybe empty)."""
    tree = ((page or {}).get("activityData") or {}).get("rewardTree") or []
    if not tree:
        return []

    shared = set()
    for root in tree:
        if _is_shared_root(root):
            _collect_edids(root, shared)

    tier_names = []
    rows, order = {}, []
    for root in tree:
        if _is_shared_root(root) or _is_regional(root):
            continue
        if root.get("type") != "leaf" and (root.get("edid") or "") in shared:
            continue
        src = _source_name(root, [])
        if _is_tier_root(root) and src not in tier_names:
            tier_names.append(src)
        base = _num(root.get("gmrwDropRate"), None)
        if base is None:
            base = _num(root.get("entryRate"), 100.0)
        hits = []
        _walk(root, base / 100.0, list(root.get("conditions") or []),
              [root.get("label") or ""], shared, hits)
        for it, rate, conds, labels in hits:
            cat = category_for(it)
            fid = str(it.get("formid") or "").upper()
            if not cat or not fid or rate <= 0:
                continue
            name = (it.get("name") or "").strip()
            if not name:
                continue
            sname = _source_name(root, labels[1:]) if not _is_tier_root(root) else src
            # A named unique shares its base item's FormID (Last Bastion is an
            # Urban Scout chest piece with a custom mod), so the mod is part
            # of the key - otherwise one would swallow the other.
            key = fid + "|" + str(it.get("customModName") or "")
            row = rows.get(key)
            if row is None:
                row = {
                    "formid": fid,
                    "edid": it.get("edid") or "",
                    "name": name,
                    "sig": (it.get("sig") or "").upper(),
                    "category": cat,
                    "conditions": conds,
                    "_src": {},
                }
                for k in ("tradeable", "unsellable", "customModName", "customModDescription"):
                    if it.get(k) is not None:
                        row[k] = it[k]
                if it.get("modSlots"):
                    row["modSlots"] = it["modSlots"]
                rows[key] = row
                order.append(key)
            else:
                row["conditions"] = _merge_conds(row["conditions"], conds)
                if name != row["name"]:
                    # One armour piece listed under its weight variants
                    # ("Robot Left Arm (Light)" / "(Sturdy)" / "(Heavy)") is
                    # one item - show it once, without the weight.
                    row["name"] = re.sub(r"\s*\((light|sturdy|heavy)\)\s*$", "", row["name"], flags=re.I)
            prev = row["_src"].get(sname)
            if prev is None or rate > prev:
                row["_src"][sname] = rate

    out = []
    for key in order:
        row = rows[key]
        if row.get("customModName"):
            row["key"] = row["formid"] + "-" + re.sub(r"[^a-z0-9]+", "-", row["customModName"].lower()).strip("-")
        srcs = row.pop("_src")
        sources = []
        if tier_names and any(n in srcs for n in tier_names):
            for n in tier_names:
                sources.append({"name": n, "rate": round(srcs.get(n, 0.0) * 100, 6),
                                **({"notDropped": True} if n not in srcs else {})})
        for n, r in srcs.items():
            if n not in tier_names:
                sources.append({"name": n, "rate": round(r * 100, 6)})
        row["sources"] = sources
        row["dropRate"] = max((s["rate"] for s in sources), default=0)
        out.append(row)

    rank = {c: i for i, c in enumerate(CATEGORY_ORDER)}
    out.sort(key=lambda r: (rank.get(r["category"], 99), r["name"].lower()))
    return out


# ── images + tradeable (plan checklists are the source of truth) ────────────

def _plan_master_index():
    try:
        with open(PLAN_MASTER, encoding="utf-8") as f:
            pm = json.load(f)
    except (OSError, ValueError) as e:
        print("  [activities_checklist] plan_master.json not read ({}) - plan art skipped".format(e))
        return {}
    out = {}
    for it in pm.get("items") or []:
        fid = str(it.get("id") or "").split("_")[-1].upper()
        if fid:
            out[fid] = it
    return out


def _slug(s):
    s = re.sub(r"^\s*(plan|recipe|schematic)\s*:\s*", "", s or "", flags=re.I)
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def event_slug(url_or_slug):
    seg = str(url_or_slug or "").strip("/").split("/")
    # /df/activities/<event>/<event>-all-rewards -> <event>
    if len(seg) >= 3 and seg[0] == "df":
        return seg[2]
    return re.sub(r"-all-rewards$", "", seg[-1] if seg else "")


def add_images(rows, ev_slug, pm=None, index=None, stats=None):
    """images[] on every row: shared library first, then the plan checklists'
    own art, then this activity's folder (guide-images/activities/<slug>/),
    then the placeholder (blank title tag / yellow mod box). image_index keeps
    only URLs the server really has, so a row can never point at a 404."""
    pm = pm if pm is not None else {}
    folder = ACT_IMG_BASE + ev_slug + "/"
    for r in rows:
        prop = []
        p = pm.get(r["formid"]) or {}
        for img in p.get("images") or []:
            if img:
                prop.append(img if str(img).startswith("/")
                            else PLAN_IMG_BASE + (p.get("image_dir") or "") + "/" + img + ".avif")
        prop.append(folder + _slug(r["name"]) + ".avif")
        r["images"] = prop
        if r["sig"] == "BOOK" and isinstance(p.get("tradeable"), bool):
            r["tradeable"] = p["tradeable"]
    idx = None
    try:
        if str(SRC_DIR) not in sys.path:
            sys.path.insert(0, str(SRC_DIR))
        import image_index
        idx = index or image_index.load(verbose=False)
        idx.apply(rows, event_dirs=[folder], page="activities/" + ev_slug,
                  stats=stats if stats is not None else {})
    except Exception as e:                # images are never fatal
        print("  [activities_checklist] image index skipped: {}".format(e))
    mod_box = ""
    try:
        mod_box = (idx.placeholders or {}).get("weaponMod") or ""
    except Exception:                     # noqa: BLE001
        pass
    for r in rows:
        r["images"] = [u for u in (r.get("images") or []) if u]
        # Weapon mod boxes use the site's shared yellow mod box, same as the
        # weapon mod plans (image_index placeholder rule).
        if not r["images"] and r.get("category") == "Weapon Mods" and mod_box:
            r["images"] = [mod_box]
            if stats is not None:
                stats["weapon-mod"] = stats.get("weapon-mod", 0) + 1
                stats["missing"] = max(0, stats.get("missing", 0) - 1)
    return rows


def _is_activity_key(key, page):
    k = str(key or "")
    if k.startswith(ACTIVITY_PATH):
        return True
    return False


def add_to_pages(by_page, verbose=True):
    """Builder hook: give every /df/activities/ page its checklist in place.
    by_page aliases one page under several keys - each page is done once."""
    pm = _plan_master_index()
    done, total, stats = set(), 0, {}
    empty = []
    for key, page in by_page.items():
        if not isinstance(page, dict) or id(page) in done:
            continue
        if not _is_activity_key(key, page):
            continue
        done.add(id(page))
        ad = page.setdefault("activityData", {})
        rows = build_checklist(page)
        add_images(rows, event_slug(key), pm=pm, stats=stats)
        ad["checklist"] = rows
        total += len(rows)
        if not rows:
            empty.append(event_slug(key))
    # The by_page map also holds the bare-slug alias of each activity page;
    # it is the same object, so it already has its checklist.
    if verbose:
        print("[activities_checklist] {} pages, {} checklist rows; images: {}".format(
            len(done), total, ", ".join("{} {}".format(v, k) for k, v in sorted(stats.items())) or "-"))
        if empty:
            print("[activities_checklist] no unique rewards on: {}".format(", ".join(sorted(empty))))
    return len(done), total


# ── stand-alone: patch the committed dist JSON ──────────────────────────────

def _patch_dist(do_slices=True, do_mono=True):
    mono = DIST_DIR / "activities_rewards_by_page.json"
    slices = DIST_DIR / "by_page"
    pm = _plan_master_index()
    stats = {}
    built = {}       # event slug -> rows

    def rows_for(key, page):
        es = event_slug(key)
        if es not in built:
            r = build_checklist(page)
            add_images(r, es, pm=pm, stats=stats)
            built[es] = r
        return built[es]

    # by_page slices (what the live renderer reads)
    idx_path = slices / "_index.json"
    n_slices = 0
    if do_slices and idx_path.exists():
        with open(idx_path, encoding="utf-8") as f:
            index = json.load(f)
        files = {}
        for k, fname in (index.get("pages") or {}).items():
            if k.startswith(ACTIVITY_PATH):
                files[fname] = k
        for fname, k in sorted(files.items()):
            p = slices / fname
            with open(p, encoding="utf-8") as f:
                payload = json.load(f)
            page = payload.get("page") or {}
            page.setdefault("activityData", {})["checklist"] = rows_for(k, page)
            tmp = p.with_suffix(".json.part")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"))
            with open(tmp, encoding="utf-8") as f:
                json.load(f)
            os.replace(tmp, p)
            n_slices += 1

    # monolith (fallback for an older shell)
    if do_mono and mono.exists():
        with open(mono, encoding="utf-8") as f:
            data = json.load(f)
        bp = data.get("byPage") or {}
        # match the bare-slug alias to its /df/activities/ page
        act_slugs = {}
        for k, page in bp.items():
            if k.startswith(ACTIVITY_PATH):
                page.setdefault("activityData", {})["checklist"] = rows_for(k, page)
                act_slugs[k.strip("/").rsplit("/", 1)[-1]] = k
        for k, page in bp.items():
            if "/" not in k and k in act_slugs:
                page.setdefault("activityData", {})["checklist"] = rows_for(act_slugs[k], page)
        tmp = mono.with_suffix(".json.part")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        with open(tmp, encoding="utf-8") as f:
            json.load(f)
        os.replace(tmp, mono)

    print("[activities_checklist] {} activity pages, {} slices patched; images: {}".format(
        len(built), n_slices, ", ".join("{} {}".format(v, k) for k, v in sorted(stats.items())) or "-"))
    for es in sorted(built):
        print("  {:<34} {:>3} rows".format(es, len(built[es])))


if __name__ == "__main__":
    # --slices / --monolith run just that half (the 70 MB monolith is slow to
    # rewrite on a synced folder); no flag runs both.
    _a = set(sys.argv[1:])
    _patch_dist(do_slices=not _a or "--slices" in _a, do_mono=not _a or "--monolith" in _a)
