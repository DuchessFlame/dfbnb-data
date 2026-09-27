#!/usr/bin/env python3
r"""
legacy_nw.py — which plans are Legacy Nuclear Winter plans, and what they were.

/df/plan-checklists/legacy-nuclear-winter-plans/

WHAT A LEGACY NUCLEAR WINTER PLAN IS
------------------------------------
Nuclear Winter (EditorID code "Babylon") was the battle-royale mode, retired in
September 2021. Its rewards were entitlements — Overseer Rank unlocks and the
Halloween 2019 / Christmas 2019 / Survivors 2020 challenge sets. When the mode
closed, Bethesda put most of those items back into Adventure as PLANS: Treasure
Hunter pails, Festive gifts, Encryptid, A Colossal Problem, Minerva and so on.
Those plans are this page.

They are ordinary plan_master rows — the drop routes and rates already come out
of the normal build — so nothing here touches a route or a rate. This module
only answers three questions:

  1. Is this plan a Legacy Nuclear Winter plan?          -> `legacy_nw: true`
  2. Which Nuclear Winter entitlement is it the plan of? -> `nw_entitlement`
  3. How was that item earned in Nuclear Winter?         -> `nw_origin`
  and, for the art, which texture names the picture is saved under (`art_stems`).

HOW IT KNOWS (generative — no hand list)
-----------------------------------------
Bethesda built the plans three different ways, so there are three signals, and
a plan needs only one:

  * The plan, its recipe or its created object carries a Babylon EditorID
    (Babylon_co_Clothes_Headwear_CenturionHelmet).
  * The plan's EditorID ends `_NW` (Recipe_mod_Minigun_Paint_Bats_NW) or names
    the NW stash (Recipe_Workshop_MediumNWStashBox).
  * The plan's recipe is a "condition proxy" COBJ that the original Babylon
    recipe points at (co_CondProxy_mod_weapon_LaserGun_material_CamoBlue is
    referenced by co_mod_LaserGun_Weapon_Paint_Babylon_CamoBlue). Neither the
    plan nor its recipe says "Babylon" — only the COBJ reference graph does.

The entitlement link comes from the same graph: the Babylon ENTM lists the
Babylon COBJ in its ReferencedBy, and the plan's recipe is that COBJ or its
proxy. A plan whose link is not in the graph falls back to its display name,
and then to the "Only while you do not own X" drop condition the build already
wrote on its routes.

A new NW plan in a later export is picked up by the next build. Nothing to edit.

Used by plan_images.load()/attach(), so the full build, add_plan_images.py and
reenrich_plan_master.py all tag the rows in the same pass that decides their
page folder.
"""

from __future__ import annotations

import csv
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
try:
    import tsv_source
except ImportError:                                  # pragma: no cover
    tsv_source = None

csv.field_size_limit(10 ** 9)

# The server folder (and the plan_images page folder) for this page.
FOLDER = "legacy-nuclear-winter"

_RX_BABYLON = re.compile(r"babylon", re.I)
# Case-sensitive on purpose: "NW" between underscores, or welded to the stash.
_RX_NW_EDID = re.compile(r"(?:^|_)NW(?:_|$)|NWStash|TrackSuitNW")
_RX_OVERSEER = re.compile(r"OverseerRank_?0*(\d+)", re.I)
_RX_NW_EVENT = re.compile(r"Babylon_Challenge_([A-Za-z]+?)(\d{4})_", re.I)
_RX_DDS = re.compile(r"([^|\s/\\]+?)\.dds", re.I)
_RX_PLAN_PREFIX = re.compile(r"^\s*(plan|recipe|schematic)\s*:\s*", re.I)
_RX_NOT_OWN = re.compile(r"do not own (.+)$", re.I)

EVENT_NAMES = {
    "halloween": "Halloween",
    "christmas": "Holiday",
    "survivors": "Survivors",
}


def _norm(s):
    s = _RX_PLAN_PREFIX.sub("", str(s or "")).lower()
    # "Paints" / "Paint Set" / "Skin" are the same thing said three ways.
    s = re.sub(r"\b(paint\s*set|paints|paint|skin)\b", "paint", s)
    return re.sub(r"[^a-z0-9]+", "", s)


def _stem(tex):
    return os.path.splitext(os.path.basename(str(tex or "").strip()))[0].lower()


def _refs(flat):
    """'FID:EDID:SIG|…' -> [(FID, EDID, SIG)]"""
    out = []
    for part in str(flat or "").split("|"):
        bits = part.strip().split(":")
        if len(bits) >= 3:
            out.append((bits[0].upper(), bits[1], bits[-1].upper()))
    return out


def _newest(tsv_dir, pattern, exclude=None):
    path = os.path.join(tsv_dir, pattern)
    if tsv_source is not None:
        try:
            return tsv_source.newest(path, required=False, exclude=exclude)
        except Exception:                            # noqa: BLE001 - never fatal
            pass
    import glob
    hits = sorted(glob.glob(path))
    if exclude:
        hits = [h for h in hits if exclude.lower() not in os.path.basename(h).lower()]
    return hits[-1] if hits else None


def origin_sentence(gmrw_edids):
    """How the item was earned in Nuclear Winter, in one plain sentence."""
    ranks = sorted({int(m.group(1)) for e in gmrw_edids
                    for m in [_RX_OVERSEER.search(e)] if m})
    if ranks:
        return (f"Originally a Nuclear Winter reward for reaching Overseer Rank "
                f"{ranks[0]}. Nuclear Winter was retired in September 2021.")
    for e in gmrw_edids:
        m = _RX_NW_EVENT.search(e)
        if m:
            ev = EVENT_NAMES.get(m.group(1).lower(), m.group(1).title())
            return (f"Originally a reward from the Nuclear Winter {ev} {m.group(2)} "
                    f"challenges. Nuclear Winter was retired in September 2021.")
    return "Originally a Nuclear Winter reward. Nuclear Winter was retired in September 2021."


def nw_route(gmrw_edids):
    """The Nuclear Winter row for the How to Obtain ledger — how it was earned.

    Only rows on the Legacy Nuclear Winter page carry it, so the ledger row
    appears on that page and nowhere else.
    """
    ranks = sorted({int(m.group(1)) for e in gmrw_edids
                    for m in [_RX_OVERSEER.search(e)] if m})
    lines = []
    if ranks:
        lines.append(f"Reach Overseer Rank {ranks[0]}")
    else:
        seen = set()
        for e in gmrw_edids:
            m = _RX_NW_EVENT.search(e)
            if m and (m.group(1), m.group(2)) not in seen:
                seen.add((m.group(1), m.group(2)))
                ev = EVENT_NAMES.get(m.group(1).lower(), m.group(1).title())
                lines.append(f"Complete the {ev} {m.group(2)} challenges")
    if not lines:
        lines.append("Nuclear Winter reward")
    return {"label": "Nuclear Winter", "lines": lines,
            "retired": "Retired September 2021"}


class Index:
    """Everything needed to tag a plan row, read once from the exports."""

    def __init__(self):
        self.entm = {}            # ENTM EDID -> record dict
        self.by_cobj = {}         # COBJ FormID -> ENTM EDID (real recipe OR its proxy)
        self.by_cnam = {}         # created-object FormID -> ENTM EDID
        self.by_name = {}         # normalised name -> ENTM EDID
        self.babylon_cobj = set() # COBJ FormIDs whose EDID says Babylon
        self.sources = []

    def __bool__(self):
        return bool(self.entm)

    # ── the three questions ────────────────────────────────────────────────
    def entitlement_for(self, item):
        cobj = (item.get("cobj") or {}).get("formid") or ""
        cnam = (item.get("cnam") or {}).get("formid") or ""
        hit = self.by_cobj.get(cobj.upper()) or self.by_cnam.get(cnam.upper())
        if hit:
            return hit
        hit = self.by_name.get(_norm(item.get("name")))
        if hit:
            return hit
        for r in item.get("obtain_routes") or []:
            for c in r.get("conditions") or []:
                m = _RX_NOT_OWN.search(str(c))
                if m:
                    hit = self.by_name.get(_norm(m.group(1)))
                    if hit:
                        return hit
        return ""

    def is_legacy(self, item):
        plan = item.get("plan_item") or {}
        cobj = item.get("cobj") or {}
        cnam = item.get("cnam") or {}
        edids = [plan.get("edid") or "", cobj.get("edid") or "", cnam.get("edid") or ""]
        if any(_RX_BABYLON.search(e) for e in edids):
            return True
        if any(_RX_NW_EDID.search(e) for e in edids[:2]):
            return True
        # Proxy recipe pointed at by a Babylon recipe — the graph signal.
        cfid = (cobj.get("formid") or "").upper()
        if cfid and cfid in self.by_cobj and cfid not in self.babylon_cobj:
            return True
        # A drop condition on the plan's own routes naming an NW entitlement.
        for r in item.get("obtain_routes") or []:
            for c in r.get("conditions") or []:
                m = _RX_NOT_OWN.search(str(c))
                if m and _norm(m.group(1)) in self.by_name:
                    return True
        return False

    def tag(self, item):
        """Write the legacy fields onto one row. Returns True when it is one."""
        if str(item.get("id") or "").startswith(REWARD_ID_PREFIX):
            return True                   # a reward row carries its own fields
        for k in ("legacy_nw", "nw_entitlement", "nw_origin", "nw_route", "art_stems",
                  "not_obtainable"):
            item.pop(k, None)
        if (item.get("kind") or "plan") != "plan" or not self.is_legacy(item):
            return False
        item["legacy_nw"] = True
        # Not obtainable, generatively: a Legacy NW plan the build resolved no
        # drop route and no unlock for. Six of them (the leather / combat /
        # Gatling paints) only sit in a Minerva "Backlog" list that no live
        # rotation uses, so the game never hands them out. The pill drops by
        # itself the day a real route resolves.
        item["not_obtainable"] = not (item.get("obtain_routes") or item.get("obtain_unlocks"))
        edid = self.entitlement_for(item)
        rec = self.entm.get(edid)
        if rec:
            item["nw_entitlement"] = {"formid": rec["formid"], "edid": edid,
                                      "name": rec["name"]}
            item["nw_origin"] = origin_sentence(rec["gmrw"])
            item["nw_route"] = nw_route(rec["gmrw"])
            item["art_stems"] = rec["art"]
        else:
            item["nw_origin"] = origin_sentence([])
            item["nw_route"] = nw_route([])
        return True


def load(tsv_dir="tsv", verbose=True):
    """Read the ENTM + COBJ exports. Never raises — an empty Index tags nothing
    beyond what the EditorIDs alone say."""
    idx = Index()
    entm_path = _newest(tsv_dir, "ENTM_Export_*.tsv")
    if not entm_path or not os.path.exists(entm_path):
        # PTS exports usually skip ENTM; the live copy is the same record set.
        entm_path = _newest("tsv", "ENTM_Export_*.tsv")
    if not entm_path:
        if verbose:
            print("  legacy_nw: no ENTM export — EditorID signals only", file=sys.stderr)
        return idx

    with open(entm_path, encoding="utf-8", errors="replace") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            edid = (r.get("EDID") or "").strip()
            # Babylon_ENTM_* are the Nuclear Winter rewards. ATX_Babylon_ENTM_*
            # are the same items' Atom Shop records (the NW Tracksuit is only
            # linked that way) — read for plan links and art, but never given a
            # reward row of their own, since they were sold, not earned.
            low = edid.lower()
            atx = low.startswith("atx_babylon")
            if not (low.startswith("babylon") or atx):
                continue
            refs = _refs(r.get("ReferencedBy"))
            main = _stem(r.get("ETDI"))
            extra = []
            for k, v in r.items():
                if k and k.startswith("ECIL_") and k != "ECIL_Count" and v:
                    extra += [_stem(m) for m in _RX_DDS.findall(v)]
            art = {"main": [s for s in ([main + "_l", main] if main else [])],
                   "extra": [s for s in dict.fromkeys(extra) if s and s != main]}
            idx.entm[edid] = {
                "formid": (r.get("FormID") or "").strip().upper(),
                "name": (r.get("FULL") or "").strip(),
                "desc": (r.get("DESC") or "").strip(),
                "refs": refs,
                "gmrw": [e for _f, e, s in refs if s == "GMRW"],
                "cobj": [f for f, _e, s in refs if s == "COBJ"],
                "art": art,
                "atx": atx,
            }
            # The NW reward record always wins a key over its Atom Shop twin.
            def _put(table, key):
                cur = table.get(key)
                if cur is None or (not atx and idx.entm.get(cur, {}).get("atx")):
                    table[key] = edid
            for f, e, s in refs:
                if s == "COBJ":
                    _put(idx.by_cobj, f)
                    if _RX_BABYLON.search(e):
                        idx.babylon_cobj.add(f)
            for nm in (r.get("FULL"), r.get("NNAM")):
                key = _norm(nm)
                if len(key) >= 4:
                    _put(idx.by_name, key)
    idx.sources.append(os.path.basename(entm_path))

    cobj_path = _newest(tsv_dir, "COBJ_Export_*.tsv") or _newest("tsv", "COBJ_Export_*.tsv")
    if cobj_path and os.path.exists(cobj_path):
        real = dict(idx.by_cobj)          # only the ENTM's own recipes seed proxies
        with open(cobj_path, encoding="utf-8", errors="replace") as fh:
            for r in csv.DictReader(fh, delimiter="\t"):
                fid = (r.get("COBJ_FormID") or "").strip().upper()
                if not fid:
                    continue
                cnam = (r.get("CNAM_FormID") or "").strip().upper()
                if fid in real and cnam:
                    idx.by_cnam.setdefault(cnam, real[fid])
                for rf, _re, rs in _refs(r.get("ReferencedBy_Flat")):
                    if rs == "COBJ" and rf in real and fid not in idx.by_cobj:
                        idx.by_cobj[fid] = real[rf]
        idx.sources.append(os.path.basename(cobj_path))

    if verbose:
        print(f"  legacy_nw: {len(idx.entm)} Nuclear Winter entitlements, "
              f"{len(idx.by_cobj)} linked recipes ({', '.join(idx.sources)})",
              file=sys.stderr)
    return idx


# ── Nuclear Winter rewards that never came back as a plan ────────────────────
# Player icons, photo frames, the NW trophies and statues, the Overseer Chair …
# Duchess wants them on the page too (27 Sep 2026), marked Not obtainable.
#
# GENERATIVE, both ways:
#   * A reward row is emitted for every Babylon entitlement that NO plan row
#     links to. The day Bethesda ships a plan for one, that plan's row links to
#     the entitlement and the reward row simply stops being emitted — the plan
#     takes its place with its real drop routes.
#   * `not_obtainable` is decided from the entitlement's own references: any
#     live source (a leveled list, container, NPC, quest or challenge reward
#     that is not an editor/NW record) makes it obtainable and the pill drops.
#     Workshop build-menu lists (workshop_LL_*) are where a CAMP item sits in
#     the build menu once owned — they hand nothing out, so they don't count.
_ROUTE_SIGS = {"LVLI", "CONT", "NPC_", "QUST", "GMRW", "TERM", "REFR"}
_RX_DEAD = re.compile(r"babylon|(^|_)(zzz\w*|cut|del|post|deprecated|debug)(_|$)|^zzz", re.I)
_RX_BUILD_MENU = re.compile(r"(^|_)workshop_LL_", re.I)
REWARD_ID_PREFIX = "NWREWARD_"

# What the reward IS, from its EditorID — the category label and page type.
_KINDS = [
    (re.compile(r"PlayerIcon", re.I), "Player Icon"),
    (re.compile(r"Photomode_Frame", re.I), "Photo Frame"),
    (re.compile(r"Skin_PowerArmor|PowerArmor", re.I), "Power Armour Paint"),
    (re.compile(r"ArmorSkin", re.I), "Armour Paint"),
    (re.compile(r"WeaponSkin", re.I), "Weapon Paint"),
    (re.compile(r"Apparel|Headwear|Outfit|Underarmor", re.I), "Apparel"),
    (re.compile(r"CAMP", re.I), "C.A.M.P."),
]


def reward_kind(edid):
    for rx, label in _KINDS:
        if rx.search(edid):
            return label
    return "Nuclear Winter Reward"


def live_sources(rec):
    """[(sig, edid)] of references that could still hand this reward out."""
    out = []
    for _fid, edid, sig in rec.get("refs") or []:
        if sig not in _ROUTE_SIGS or not edid:
            continue
        if _RX_DEAD.search(edid) or _RX_BUILD_MENU.search(edid):
            continue
        out.append((sig, edid))
    return out


def reward_rows(items, idx):
    """Replace the reward rows in a plan_master item list. Returns the count.

    Idempotent: previous reward rows are removed first, so a re-run after a
    plan appears (or a reward gains a live source) converges.
    """
    items[:] = [i for i in items if not str(i.get("id") or "").startswith(REWARD_ID_PREFIX)]
    if not idx:
        return 0
    linked = set()
    for it in items:
        # Undo last run's duplicate call before re-deciding (see below).
        if it.pop("nw_duplicate", None):
            it["cut"] = False
            it["cut_reason"] = None
        if idx.tag(it):
            ent = (it.get("nw_entitlement") or {}).get("edid")
            if ent:
                linked.add(ent)
    # Two plan books for one NW item (Hellfire Prototype, the Medium stash box):
    # Duchess, 27 Sep 2026 — merge them, it is the same plan. The copy that
    # actually drops is kept as THE row; a copy nothing gives out is folded into
    # it: listed under Technical ("Also the same plan": FormID / EDID / recipe)
    # and marked cut so it leaves the page. Generative — the merge is decided
    # every build, so if the live copy stops dropping both rows come back.
    by_ent = {}
    for it in items:
        it.pop("merged_plans", None)
        ent = (it.get("nw_entitlement") or {}).get("edid")
        if it.get("legacy_nw") and ent and it.get("plan_item") and not it.get("cut"):
            by_ent.setdefault(ent, []).append(it)
    for rows in by_ent.values():
        live = [r for r in rows if not r.get("not_obtainable")]
        if len(rows) < 2 or not live:
            continue
        keep = max(live, key=lambda r: len(r.get("obtain_routes") or []) + len(r.get("obtain_unlocks") or []))
        for r in rows:
            if r is keep or not r.get("not_obtainable"):
                continue
            keep.setdefault("merged_plans", []).append({
                "name": r.get("name"),
                "plan_item": r.get("plan_item"),
                "cobj": r.get("cobj"),
            })
            r["cut"] = True
            r["cut_reason"] = f"merged into {keep.get('name')} — the same Nuclear Winter item"
            r["nw_duplicate"] = True

    labeler = None
    added = 0
    for edid, rec in sorted(idx.entm.items(), key=lambda kv: kv[1]["name"].lower()):
        if edid in linked or not rec["name"] or rec.get("atx"):
            continue
        live = live_sources(rec)
        routes = []
        if live:
            if labeler is None:
                labeler = _source_labeler()
            routes = sorted({labeler(e) or e for _s, e in live})
        kind = reward_kind(edid)
        obtain = ("Not obtainable. This was a Nuclear Winter reward and nothing in the "
                  "current game files gives it out — there is no plan for it."
                  if not routes else
                  "No plan for this one — the reward itself is given out by the sources below.")
        items.append({
            "kind": "plan", "brand": "both", "type": "legacy-nw-reward",
            "id": REWARD_ID_PREFIX + rec["formid"],
            "name": rec["name"],
            "has_image_box": True,
            "category_label": f"{kind} (Nuclear Winter reward, no plan)",
            "obtain": obtain,
            "obtain_routes": [],
            "obtain_unlocks": [f"Found in: {r}" for r in routes],
            "obtain_ledger": ([{"label": "Events & Activities",
                                "unlocks": list(range(len(routes)))}] if routes else []),
            "plan_item": None, "cobj": None, "cnam": None,
            "entitlement_only": True,
            "not_obtainable": not routes,
            "tradeable": False,
            "stops_dropping": None,
            "effects": None,
            "cut": False, "cut_reason": None,
            "desc": rec.get("desc") or "",
            "source_tag": kind.split()[0] if routes else "",
            "images": [], "image_source": "",
            # legacy fields (tag() would find nothing to link — set directly)
            "legacy_nw": True,
            "nw_entitlement": {"formid": rec["formid"], "edid": edid, "name": rec["name"]},
            "nw_origin": origin_sentence(rec["gmrw"]),
            "nw_route": nw_route(rec["gmrw"]),
            "art_stems": rec["art"],
        })
        added += 1
    return added


def _source_labeler():
    """plan_sources.source_label with the quest names loaded, or a no-op."""
    try:
        import plan_sources
        q = plan_sources.QuestNames()
        path = _newest("tsv", "QUEST_Export_*.tsv")
        if path:
            q.load(path)
        return lambda e: plan_sources.source_label(e, q)
    except Exception:                                # noqa: BLE001
        return lambda e: None


def tag_all(items, idx):
    return sum(1 for it in items if idx.tag(it))


if __name__ == "__main__":
    import json
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("dist", "plan_master.json")
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    ix = load("tsv")
    rows = [i for i in doc.get("items") or [] if ix.tag(i)]
    for i in sorted(rows, key=lambda x: x["name"]):
        ent = (i.get("nw_entitlement") or {}).get("edid") or "(no entitlement link)"
        print(f"  {'CUT ' if i.get('cut') else '    '}{i['name']:55s} {ent}")
    live = [i for i in rows if not i.get("cut")]
    print(f"{len(live)} live legacy NW plans ({len(rows) - len(live)} cut), "
          f"{sum(1 for i in live if i.get('nw_entitlement'))} linked to an entitlement")


# ── NW art for OTHER pages ───────────────────────────────────────────────────
# The Legacy Nuclear Winter page owns the art for these items (Duchess, 27 Sep
# 2026): any other page that shows a Legacy NW plan or reward — Seasonal Events,
# Daily Ops, New Plans — reads it from here BEFORE building a URL into its own
# wp-content folder. New Plans and Daily Ops copy plan_master rows verbatim, so
# they inherit it for free; builders that invent their own URLs (Seasonal
# Events) ask nw_image_map().
PLAN_IMG_BASE = "/wp-content/uploads/guide-images/plan-checklist/"


def row_image_urls(item):
    """A plan_master row's images as absolute URLs (bare stems resolved)."""
    base = PLAN_IMG_BASE + (item.get("image_dir") or item.get("type") or "") + "/"
    out = []
    for s in item.get("images") or []:
        s = str(s)
        out.append(s if s.startswith("/") else base + s + ".avif")
    return out


def nw_image_map(plan_master_path):
    """{KEY -> [absolute urls]} for every Legacy NW row that has art.

    Keys (upper-cased): the plan's FormID, the plan's EditorID, the NW
    entitlement's FormID and EditorID — whichever the calling page carries.
    Empty when plan_master is missing; callers then keep their own URLs.
    """
    import json
    try:
        with open(plan_master_path, encoding="utf-8") as fh:
            items = json.load(fh).get("items") or []
    except Exception:                                # noqa: BLE001
        return {}
    out = {}
    for it in items:
        if not it.get("legacy_nw"):
            continue
        urls = row_image_urls(it)
        if not urls:
            continue
        for rec in (it.get("plan_item") or {}, it.get("nw_entitlement") or {}):
            for k in (rec.get("formid"), rec.get("edid")):
                if k:
                    out.setdefault(str(k).strip().upper(), urls)
    return out
