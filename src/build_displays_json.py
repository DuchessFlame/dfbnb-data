#!/usr/bin/env python3
"""
build_displays_json.py
----------------------
Build dist/displays.json for the DF/BNB "Plan Checklists - Display Checklist"
page (/df/plan-checklists/displays/).

ROSTER SOURCE — the keyword, not a leveled list.
    Every in-game display surface carries PlayerDisplayCaseKeyword (003F51E3).
    Walking the keyword's refs in KYWD_Export_*_Refs.tsv picks up all of them in
    one pass: base-game craftables, Atom Shop, scoreboard and event displays
    alike. ATX_workshop_LL_DisplayCases was rejected as the roster source — it
    only holds the 21 Atom Shop cases, so it would silently drop two thirds of
    the page.

ART — resolved through the entitlement, never guessed from the EDID.
    An ACTI display EDID does NOT predict its texture stem (ATX_DisplayCase_
    BeerSteins_Cultist -> atx_camp_displaycase_beersteins_cultist: the game
    inserts a "camp" infix the EDID has no trace of). So art is resolved:
        ACTI  --HasEntitlement condition in LVLI_Export_*_LVLI_Entries.tsv-->
        ENTM  --ETDI / ECIL_n columns in ENTM_Export_*.tsv-->  texture stems
    ETDI stem + "_l" is the transparent tile (the row thumb and the first
    Item Image frame); each ECIL stem is a carousel shot. This matches the
    resource-producers contract in build_camp_items_json.py.

    Fallback when no LVLI condition names the entitlement: match an ENTM whose
    FULL is exactly the ACTI's display name, preferring the candidate sharing
    the most EDID characters. Recovers ~21 further items; anything still
    unresolved is a base-game craftable that never had a storefront tile.

IMAGES — ONLY the transparent `<ETDI stem>_l` tile. The `_c1/_c2/...` ECIL
    carousel shots are opaque in-CAMP photos and mixing them with the cut-out
    tiles made the page look inconsistent, so they are never emitted.
    `images` lists the tile only when its .avif is actually staged, so the
    front end never points at a 404. An empty list renders the dashed
    placeholder slot and needs no code change when art lands later.

HOW TO OBTAIN — resolved from Bethesda's own gating, never guessed from names.
    A display is placed through a workshop leveled list (or straight from a
    COBJ). Each LVLI entry for the ACTI carries the condition that unlocks it:
        HasEntitlement(ENTM)      -> Atom Shop / Season N scoreboard / Mini-Season
        HasLearnedRecipe(COBJ)    -> that recipe's GNAM:
                                      BOOK  -> In-Game Plan (drop routes copied
                                               from dist/plan_master.json)
                                      CHAL  -> Challenge reward
                                      recipe_Dummy_LearnedViaWorkshopClaim / Script
        no condition              -> the COBJ that creates the list; an empty GNAM
                                     there means the display is a Default Unlock
    Every route found is published in `obtain_routes` (a display can have more
    than one) and the first one sets `source` / `unlock_kind`. A display the
    game data does not gate at all falls back to the old EditorID inference,
    flagged `unlock_kind: "inferred"`.

Usage:
    python build_displays_json.py --tsv-root tsv --avif-dir "<staged avif>" \
        --outdir dist [--pts]
"""

import argparse
import collections
import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

from cut_content import cut_obtain   # cut rows get the standard "Cut content" line
import tsv_source                    # export selection by filename date, never mtime
import reusable_images               # art the site already hosts (scoreboard tiles first)

KEYWORD_FORMID = "003F51E3"          # PlayerDisplayCaseKeyword
DDS_RE  = re.compile(r"([A-Za-z0-9_\-\.]+?\.dds)", re.I)
ENT_RE  = re.compile(r'HasEntitlement\(.*?([A-Za-z0-9_]+)\s+"(.*?)"\s*\[ENTM:([0-9A-Fa-f]{8})\]')
ACTI_RE = re.compile(r"([0-9A-Fa-f]{8}):([^:]+):ACTI")
LVLI_RE = re.compile(r"([0-9A-Fa-f]{8}):([^:]+):LVLI")
RECIPE_RE = re.compile(r"HasLearnedRecipe\(.*?([A-Za-z0-9_]+)\s+\[COBJ:([0-9A-Fa-f]{8})\]")
CAROUSEL_RE = re.compile(r"_c\d+$", re.I)   # _c1/_c2/... = opaque carousel shot, never emitted
CUT_PREFIXES = ("ZZZ", "TEST", "CUT", "REUSE", "TEMPLATE", "DEL", "POST")


def latest(tsv_root, pattern, required=True):
    """Newest export by the date in its FILENAME (tsv_source.export_key).

    Never by mtime: actions/checkout stamps every file with the checkout time,
    so an mtime pick is arbitrary in CI while looking right on a dev box."""
    try:
        return tsv_source.newest(os.path.join(tsv_root, pattern), required=required)
    except tsv_source.NoExportFound:
        sys.exit("No TSV matching {} in {}".format(pattern, tsv_root))


def read_tsv(path):
    with open(path, encoding="utf-8", errors="ignore") as fh:
        yield from csv.DictReader(fh, delimiter="\t")


def is_cut(edid):
    """Cut content = dev/test/placeholder records the page must not list.

    Matched as a PREFIX (ZZZ_, REUSE_, TEMPLATE_ ...) *and*, for "TEST", anywhere
    in the EDID: the marker is not always leading — WorkshopDisplay_TestAllWeapons01
    carries it mid-string and would otherwise render as a real display."""
    s = (edid or "").strip().upper()
    if "TEST" in s:
        return True
    return any(s.startswith(p) for p in CUT_PREFIXES)


def classify_source(edid):
    """(source label, unlock hint) from the EDID prefix."""
    s = (edid or "")
    m = re.match(r"(?i)^(?:zzz_?)?SCORE_S(\d+)_", s)
    if m:
        n = m.group(1)
        return ("Season {}".format(n),
                "Earned on the Season {} scoreboard. Once claimed it is yours permanently.".format(n))
    if re.match(r"(?i)^(?:zzz_?)?SCORE_MiniSeason_(\d+)", s):
        y = re.match(r"(?i)^(?:zzz_?)?SCORE_MiniSeason_(\d+)", s).group(1)
        return ("Mini-Season {}".format(y),
                "Earned during the {} mini-season event.".format(y))
    if re.match(r"(?i)^(?:zzz)?ATX_", s):
        return ("Atom Shop", "Bought from the Atom Shop, either on its own or inside a bundle.")
    if re.match(r"(?i)^(?:zzz_?)?Fishing_", s):
        return ("Fishing", "Unlocked through the fishing system.")
    if re.match(r"(?i)^(Moth_|MOON_|MN2_|SSE_|Meat_|DE\d{4}_)", s):
        return ("Seasonal Event", "Awarded during a limited-time seasonal event.")
    return ("Base Game", "Craftable at a C.A.M.P. or workshop once the plan is known.")


def ecil_stems(row):
    """ECIL_1 packs every carousel frame into one unseparated run of .dds names,
    so scan each ECIL_n for filenames rather than splitting on a delimiter."""
    out = []
    try:
        count = int((row.get("ECIL_Count") or "0") or 0)
    except ValueError:
        count = 0
    for i in range(1, max(count, 1) + 1):
        for m in DDS_RE.finditer(row.get("ECIL_{}".format(i)) or ""):
            stem = m.group(1)[:-4]
            if stem not in out:
                out.append(stem)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# How-to-obtain resolution
# ─────────────────────────────────────────────────────────────────────────────
# GNAM dummies Bethesda uses when a recipe is not taught by a plan book. Same
# EditorIDs plan_unlocks.py keys on; kept local so this builder does not pull in
# the WEAP/ARMO/OMOD/GLOB loads that module needs for scrap-to-learn.
_GNAM_DUMMY = {
    "recipe_dummy_learnedviaworkshopclaim": "workshop",
    "recipe_dummy_learnedviascriptrecipes": "script",
    "recipe_dummy_uncraftable_item_nocraft": "uncraftable",
}
MAX_DROPS = 6   # plan drop routes shown per display; the plan page has the rest


def entitlement_route(edid, name):
    """A HasEntitlement gate -> route dict, labelled by the ENTM's own prefix."""
    e = edid or ""
    shown = '"{}"'.format(name) if name else "this item"
    m = re.match(r"(?i)^(?:zzz_?)?SCORE_S(\d+)_", e)
    if m:
        n = int(m.group(1))
        # The Scoreboard became a ticket shop at Season 16 (Duel with the Devil).
        verb = "Claim" if n <= 15 else "Purchase with tickets"
        return {"kind": "scoreboard", "label": "Season {}".format(n),
                "text": "{} {} from the Season {} Scoreboard.".format(verb, shown, n)}
    m = re.match(r"(?i)^(?:zzz_?)?SCORE_MiniSeason_(\d+)", e)
    if m:
        return {"kind": "mini-season", "label": "Mini-Season {}".format(m.group(1)),
                "text": "Earned as {} during the {} mini-season.".format(shown, m.group(1))}
    if re.match(r"(?i)^(?:zzz_?)?ATX_", e):
        return {"kind": "atom-shop", "label": "Atom Shop",
                "text": "Bought from the Atom Shop as {}, either on its own or inside a bundle.".format(shown)}
    return {"kind": "entitlement", "label": "Entitlement",
            "text": "Unlocked by the {} entitlement.".format(shown)}


class RouteIndex:
    """COBJ + CHAL + BOOK + plan_master, read once, answering "what unlocks this"."""

    def __init__(self, tsv_root, plan_master_path):
        self.cobj, self.cobj_by_cnam = {}, {}
        for r in read_tsv(latest(tsv_root, "COBJ_Export_*.tsv")):
            fid = (r.get("COBJ_FormID") or "").strip().upper()
            if not fid:
                continue
            self.cobj[fid] = r
            cn = (r.get("CNAM_FormID") or "").strip().upper()
            if cn:
                self.cobj_by_cnam.setdefault(cn, []).append(fid)
        self.chal = {(r.get("FormID") or "").strip().upper(): (r.get("FULL") or "").strip()
                     for r in read_tsv(latest(tsv_root, "CHAL_Export_*.tsv"))}
        book_f = latest(tsv_root, "BOOK_Export_*.tsv")
        self.books = {(r.get("FormID") or "").strip().upper() for r in read_tsv(book_f)}
        self.plans, self.plan_by_cobj, self.plan_by_name = {}, {}, {}
        if plan_master_path and os.path.isfile(plan_master_path):
            with open(plan_master_path, encoding="utf-8") as fh:
                for it in (json.load(fh).get("items") or []):
                    self.plans[it.get("id") or ""] = it
                    cf = ((it.get("cobj") or {}).get("formid") or "").upper()
                    if cf:
                        self.plan_by_cobj.setdefault(cf, it)
                    nm = (it.get("name") or "").strip().lower()
                    if nm:
                        self.plan_by_name.setdefault(nm, []).append(it)
        else:
            print("  WARNING: no plan_master.json at {!r} — plan routes will carry no drop "
                  "sources".format(plan_master_path), file=sys.stderr)

    def plan_drops(self, plan):
        """(drops, notes) copied verbatim from a plan_master row — no rate maths here."""
        if not plan:
            return [], []
        seen, drops = set(), []
        for r in sorted(plan.get("obtain_routes") or [], key=lambda r: -(r.get("rate") or 0)):
            label = (r.get("route") or "").strip()
            if not label or label.lower() in seen:
                continue
            seen.add(label.lower())
            drops.append({"route": label, "rate": r.get("rate_display") or ""})
            if len(drops) >= MAX_DROPS:
                break
        return drops, [u for u in (plan.get("obtain_unlocks") or []) if u]

    def plan_route(self, plan, plan_name=None):
        name = plan_name or (plan or {}).get("name") or "the plan"
        drops, notes = self.plan_drops(plan)
        text = "Learn {}.".format(name)
        if not drops and not notes:
            text += " No drop source for this plan resolves in the current game files."
        return {"kind": "plan", "label": "In-Game Plan", "text": text,
                "plan": name, "drops": drops, "notes": notes}

    def recipe_route(self, cobj_fid):
        """Route for a recipe, read from its GNAM. None when it says nothing useful."""
        row = self.cobj.get((cobj_fid or "").upper())
        if row is None:
            return None
        gid = (row.get("GNAM_FormID") or "").strip().upper()
        gedid = (row.get("GNAM_EDID") or "").strip()
        gfull = (row.get("GNAM_FULL") or "").strip()
        if not gid:
            cedid = (row.get("COBJ_EDID") or "").strip()
            m = re.match(r"(?i)^(?:zzz_?)?SCORE_S?(\d+)_", cedid)
            if m:
                # No GNAM, but the recipe is a scoreboard one: the gate sits on
                # the entitlement, which this recipe does not name. Inference,
                # so worded as such.
                return {"kind": "scoreboard", "label": "Season {}".format(int(m.group(1))),
                        "text": "Named in the game files as a Season {} Scoreboard reward."
                                .format(int(m.group(1)))}
            if not re.match(r"(?i)^workshop_", cedid):
                # A content-coded recipe (ATX_, Fishing_, MOON_, SSE_ ...) with no
                # GNAM is gated somewhere this export cannot see. Saying "default"
                # would be a guess, so say nothing and let the other routes speak.
                return None
            return {"kind": "default", "label": "Default Unlock",
                    "text": "Known by default. Build it from the C.A.M.P. menu without learning a plan."}
        dummy = _GNAM_DUMMY.get(gedid.lower())
        if dummy == "workshop":
            return {"kind": "workshop", "label": "Workshop",
                    "text": "Learned by claiming a public workshop."}
        if dummy == "script":
            return {"kind": "script", "label": "Quest / Script",
                    "text": "Granted by a quest or script rather than a plan."}
        if dummy == "uncraftable":
            return None
        if gid in self.chal:
            return {"kind": "challenge", "label": "Challenge",
                    "text": 'Reward for completing the challenge "{}".'.format(self.chal[gid] or gedid)}
        if gid in self.books or gfull.lower().startswith(("plan:", "recipe:")):
            plan = self.plans.get("PLAN_" + gid) or self.plan_by_cobj.get(cobj_fid.upper())
            return self.plan_route(plan, gfull or None)
        return {"kind": "item", "label": "In-Game",
                "text": 'Unlocked by owning "{}".'.format(gfull or gedid)}

    def plan_named(self, display_name):
        """The one live plan named "Plan: <display name>" whose recipe is a display.

        The recipe check matters: "Plan: Magazine Rack" teaches a shelf
        container (workshop_co_Shelves_MagazineRackContainer02), not the display
        rack that shares its name."""
        hits = [p for p in self.plan_by_name.get("plan: " + (display_name or "").lower(), [])
                if not p.get("cut")
                and "display" in ((p.get("cobj") or {}).get("edid") or "").lower()]
        return hits[0] if len(hits) == 1 else None


def merge_routes(routes, acti_edid):
    """Dedupe, drop a Default Unlock that another route contradicts, keep order."""
    out, seen = [], set()
    for r in routes:
        if not r:
            continue
        key = (r["kind"], r["label"], r["text"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    gated = [r for r in out if r["kind"] != "default"]
    # "Default" only stands when nothing else gates the display AND its EditorID
    # is not a storefront/scoreboard record — an ungated ATX_ list entry means
    # the gate sits on the recipe, which this export does not carry.
    if gated or re.match(r"(?i)^(?:zzz_?)?(ATX_|SCORE_)", acti_edid or ""):
        out = gated
    return out


def tile_images(stem, staged, overrides, hosted=None, edid=""):
    """[the transparent `_l` tile] or [].

    Order: a hand override in data/checklists/displays.json, then a SCOREBOARD
    tile the site already serves from season_images/season-N/ (found by
    entitlement EDID or texture stem through reusable_images), then the local
    staged copy in guide-images/plan-checklist/display/. Scoreboard art wins
    over a local re-upload for the same reason it does on the CAMP pages: that
    URL is known to resolve and one tile should not be hosted twice.

    Only season_images hits are reused. The Atom Shop request tiles in the same
    index are storefront renders, not the cut-out `_l` tile this page shows.
    """
    s = (stem or "").lower()
    if CAROUSEL_RE.search(s):
        return []
    tile = (s if s.endswith("_l") else s + "_l") if s else ""
    if tile and tile in overrides:
        return [overrides[tile]]
    if hosted is not None and (edid or s):
        hit = hosted.find(edid=edid or "", texture=s)
        if hit and "/season_images/" in hit and not CAROUSEL_RE.search(
                os.path.splitext(os.path.basename(hit))[0]):
            return [hit]
    return [tile] if tile and tile in staged else []


def build(tsv_root, avif_dir, overrides_path="", plan_master_path=""):
    kywd_path = latest(tsv_root, "KYWD_Export_*_Refs.tsv")
    lvli_path = latest(tsv_root, "LVLI_Export_*_LVLI_Entries.tsv")
    entm_path = latest(tsv_root, "ENTM_Export_*.tsv")
    for p in (kywd_path, lvli_path, entm_path):
        print("  source: {}".format(os.path.basename(p)), file=sys.stderr)

    roster = [
        {"formid": (r.get("RefFormID") or "").strip(),
         "edid":   (r.get("RefEDID") or "").strip(),
         "name":   (r.get("RefName") or "").strip()}
        for r in read_tsv(kywd_path)
        if (r.get("KeywordFormID") or "").strip().upper() == KEYWORD_FORMID
        and (r.get("RefSignature") or "").strip() == "ACTI"
    ]
    print("  roster from PlayerDisplayCaseKeyword: {}".format(len(roster)), file=sys.stderr)

    acti_to_entm = {}
    acti_entries = collections.defaultdict(list)   # ACTI -> [(LVLI FormID, cond text)]
    for r in read_tsv(lvli_path):
        m = ACTI_RE.match(r.get("LVLO_Reference") or "")
        if not m:
            continue
        cond = " ".join((r.get("Cond{}".format(i)) or "") for i in range(1, 11))
        acti_entries[m.group(1).upper()].append(
            ((r.get("LVLI_FormID") or "").strip().upper(), cond))
        me = ENT_RE.search(cond)
        if me:
            acti_to_entm.setdefault(m.group(1).upper(),
                                    {"edid": me.group(1), "name": me.group(2),
                                     "formid": me.group(3).upper()})

    entm_rows = list(read_tsv(entm_path))
    idx = RouteIndex(tsv_root, plan_master_path)
    hosted = reusable_images.build_index(os.path.dirname(plan_master_path) if plan_master_path else "dist")
    print("  hosted art: {}".format(hosted.summary()), file=sys.stderr)

    def resolve(acti_fid, acti_edid, name, ent_fallback):
        """Every unlock route for one display, in priority order."""
        routes = []
        entries = acti_entries.get(acti_fid, [])
        for lvli_fid, cond in entries:
            for me in ENT_RE.finditer(cond):
                routes.append(entitlement_route(me.group(1), me.group(2)))
            for mr in RECIPE_RE.finditer(cond):
                routes.append(idx.recipe_route(mr.group(2)))
        for lvli_fid, cond in entries:
            if cond.strip():
                continue
            for cf in idx.cobj_by_cnam.get(lvli_fid, []):      # the list's own recipe
                routes.append(idx.recipe_route(cf))
        for cf in idx.cobj_by_cnam.get(acti_fid, []):            # built straight from a COBJ
            routes.append(idx.recipe_route(cf))
        if ent_fallback and not any(r and r["kind"] in
                                    ("atom-shop", "scoreboard", "mini-season", "entitlement")
                                    for r in routes):
            routes.append(entitlement_route(ent_fallback["edid"], ent_fallback["name"]))
        # A plan sharing the display's name (e.g. a Gold Bullion plan) is only
        # trusted when the game data resolved nothing else but entitlements —
        # "Plan: Magazine Rack" teaches a shelf container, not the display rack.
        plan = idx.plan_named(name)
        if plan and all(r is None or r["kind"] in
                        ("atom-shop", "scoreboard", "mini-season", "entitlement")
                        for r in routes):
            routes.append(idx.plan_route(plan))
        return merge_routes(routes, acti_edid)
    by_fid  = {(r.get("FormID") or "").strip().upper(): r for r in entm_rows}
    by_full = collections.defaultdict(list)
    for r in entm_rows:
        full = (r.get("FULL") or "").strip()
        if full:
            by_full[full.lower()].append(r)

    overrides = {}
    extra_pat = []
    _cfg_staged = []
    if overrides_path and os.path.isfile(overrides_path):
        with open(overrides_path, encoding="utf-8") as fh:
            _cfg = json.load(fh)
        overrides = {k.lower(): v for k, v in
                     (_cfg.get("image_overrides") or {}).items()}
        extra_pat = _cfg.get("extra_roster_entm") or []
        _cfg_staged = _cfg.get("staged_images") or []
        print("  image overrides: {}".format(len(overrides)), file=sys.stderr)

    # Which .avif stems exist in guide-images/plan-checklist/displays/.
    #
    # The staging folder lives on a local drive, so CI cannot see it. Passing
    # --avif-dir scans that folder AND persists the stem list into the config as
    # "staged_images"; every later run (CI included) rebuilds from that list.
    # Without this, a CI run would find an empty folder, emit images: [] for
    # every row and silently blank the page.
    #
    # So: run locally with --avif-dir whenever art is added or removed, then
    # commit the config alongside dist/displays.json.
    scanned = avif_dir and os.path.isdir(avif_dir)
    if scanned:
        staged = {os.path.splitext(f)[0].lower()
                  for f in os.listdir(avif_dir) if f.lower().endswith(".avif")}
        print("  staged .avif stems: {} (scanned {})".format(len(staged), avif_dir),
              file=sys.stderr)
    else:
        staged = {s.lower() for s in (_cfg_staged or [])}
        if avif_dir:
            print("  WARNING: --avif-dir {!r} is not a directory".format(avif_dir),
                  file=sys.stderr)
        print("  staged .avif stems: {} (from config)".format(len(staged)), file=sys.stderr)
        if not staged:
            print("  WARNING: no staged stems — every row will have images: []",
                  file=sys.stderr)

    # Stem -> absolute wp-content path. Art already hosted elsewhere on the site
    # (season_images/season-N, atom-shop tiles) is reused verbatim instead of a
    # second copy being staged here; the renderer passes any value starting with
    # "/" straight through. Season art takes priority over a local re-upload.
    items = []
    n_ent = n_img = 0
    n_route = collections.Counter()
    for it in roster:
        fid = it["formid"].upper()
        ent = acti_to_entm.get(fid)
        row = by_fid.get(ent["formid"]) if ent else None
        if row is None:
            cands = by_full.get(it["name"].lower(), [])
            if cands:
                target = set((it["edid"] or "").lower())
                row = max(cands, key=lambda c: len(set((c.get("EDID") or "").lower()) & target))
                ent = {"edid": (row.get("EDID") or "").strip(),
                       "name": (row.get("FULL") or "").strip(),
                       "formid": (row.get("FormID") or "").strip().upper()}
        if ent:
            n_ent += 1

        etdi = ((row.get("ETDI") or "").strip() if row else "")
        stem = etdi[:-4] if etdi.lower().endswith(".dds") else etdi
        # The transparent `_l` tile ONLY — never the opaque _c carousel shots.
        # Only a tile with a staged .avif (or a hosted override) is emitted.
        images = tile_images(stem, staged, overrides, hosted, (ent or {}).get("edid", ""))
        if images:
            n_img += 1

        routes = resolve(fid, it["edid"], it["name"], ent)
        if routes:
            source, hint, lead, kind = routes[0]["label"], "", routes[0]["text"], routes[0]["kind"]
        else:
            source, hint = classify_source(it["edid"])
            art = "an" if source[:1].upper() in "AEIOU" else "a"
            lead, kind = "Named in the game files as {} {} item.".format(art, source), "inferred"
        n_route[kind] += 1
        items.append({
            "id": "DISPLAY_" + it["formid"].upper(),
            "name": it["name"] or it["edid"],
            "source": source,
            "obtain": cut_obtain(lead, is_cut(it["edid"])),
            "unlock_hint": hint,
            "unlock_kind": kind,
            "obtain_routes": routes,
            "desc": ((row.get("DESC") or "").strip() if row else ""),
            "added": "",
            "images": images,
            "entitlement": ({"edid": ent["edid"], "formid": ent["formid"]} if ent else {}),
            "items": [{"label": it["name"] or it["edid"], "edid": it["edid"],
                       "formid": it["formid"], "kind": "activator", "texture": stem}],
            "cut": is_cut(it["edid"]),
        })

    items.sort(key=lambda x: (x["name"].lower(), x["id"]))
    print("  with entitlement: {}   with staged art: {}".format(n_ent, n_img), file=sys.stderr)
    print("  unlock routes: {}".format(dict(n_route)), file=sys.stderr)
    # --- second roster pass: ENTM-driven families -------------------------
    # Mannequins and power-armour displays are NPC_ records, not ACTI, so the
    # PlayerDisplayCaseKeyword sweep above cannot see them however much art
    # exists. The KYWD export is also an incomplete source here (it lists only
    # the female mannequins), so the entitlement export drives these rows: any
    # ENTM whose EDID matches a configured pattern and is not cut content.
    if extra_pat:
        pats = [re.compile(p, re.I) for p in extra_pat]
        seen = {i["id"] for i in items}
        n_extra = 0
        for row in entm_rows:
            edid = (row.get("EDID") or "").strip()
            if not edid or is_cut(edid):
                continue
            if not any(p.search(edid) for p in pats):
                continue
            fid = (row.get("FormID") or "").strip().upper()
            did = "DISPLAY_" + fid
            if did in seen:
                continue
            etdi = (row.get("ETDI") or "").strip()
            stem = etdi[:-4] if etdi.lower().endswith(".dds") else etdi
            imgs = tile_images(stem, staged, overrides, hosted, edid)
            name = (row.get("FULL") or "").strip() or edid
            route = entitlement_route(edid, name)
            items.append({
                "id": did,
                "name": name,
                "source": route["label"],
                "obtain": route["text"],
                "unlock_hint": "",
                "unlock_kind": route["kind"],
                "obtain_routes": [route],
                "desc": (row.get("DESC") or "").strip(),
                "added": "",
                "images": imgs,
                "entitlement": {"edid": edid, "formid": fid},
                "items": [{"label": name, "edid": edid, "formid": fid,
                           "kind": "npc", "texture": stem}],
                "cut": False,
            })
            seen.add(did)
            n_extra += 1
            if imgs:
                n_img += 1
        print("  extra ENTM roster rows: {}".format(n_extra), file=sys.stderr)

    # Persist a freshly scanned list so CI can rebuild without the staging folder.
    if scanned and overrides_path and os.path.isfile(overrides_path):
        with open(overrides_path, encoding="utf-8") as fh:
            _out = json.load(fh)
        _new = sorted(staged)
        if _out.get("staged_images") != _new:
            _out["staged_images"] = _new
            with open(overrides_path, "w", encoding="utf-8", newline="\n") as fh:
                json.dump(_out, fh, ensure_ascii=False, indent=2)
                fh.write("\n")
            print("  wrote {} staged stems into {}".format(len(_new), overrides_path),
                  file=sys.stderr)

    items.sort(key=lambda x: x["name"].lower())

    return {
        "version": 1,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(items),
        "source_files": {"kywd": os.path.basename(kywd_path),
                         "lvli": os.path.basename(lvli_path),
                         "entm": os.path.basename(entm_path)},
        "displays": items,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv-root", default="tsv")
    ap.add_argument("--avif-dir", default="")
    ap.add_argument("--overrides", default=os.path.join("data", "checklists", "displays.json"))
    ap.add_argument("--outdir", default="dist")
    ap.add_argument("--plan-master", default="",
                    help="plan_master.json for plan drop routes (default: <outdir>/plan_master.json)")
    ap.add_argument("--pts", action="store_true", help="write to dist/pts/ instead")
    a = ap.parse_args()
    data = build(a.tsv_root, a.avif_dir, a.overrides,
                 a.plan_master or os.path.join(a.outdir, "plan_master.json"))
    outdir = os.path.join(a.outdir, "pts") if a.pts else a.outdir
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "displays.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    print("Wrote {} ({} displays)".format(out, data["count"]), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
