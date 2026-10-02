#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_mutated_events_json.py - Mutated Events All Rewards (Oct 2026)

Writes (one JSON for the one page, so a bad build can never blank another page):
    dist/mutated_events/mutated_events_all_rewards.json
        -> /df/mutated-events/mutated-events-all-rewards/
           rendered by df-bnb-mutated-events.js

Laid out like the Hunt for the Treasure Hunter All Rewards page: one root
expand per reward pack (the Treasure Hunter page has one per pail), each
holding a category expand per reward list the pack rolls, then the unique
rewards listed once as a tickable checklist at the bottom.

EVERYTHING IS READ FROM THE GAME FILES - nothing below is typed in by hand:

    LL_MutatedEvents_Rewards (LVLI)       the list every mutated public event's
        |                                 GMRW row awards (GMRW export -> the
        |                                 page's "Public Events" line)
        |-- one reward list per pack      gated by GetPublicEventHasMutation
        |     (Single / Double, and the   (which mutations) and
        |      Fallout 1st "Party Pack"   Active Players.IsPlayerFO1Member vs
        |      twins)                     MutatedEvents_LCP_Fallout1stRewardsThreshold
        |       |-- Legendary Scrip, Treasury Notes   (paid with the pack)
        |       '-- the pack itself (ALCH)
        |             '-- ALCH effect (MGEF)  -> ALCH Effects export
        |                   '-- contents LVLI -> LVLI Refs export (the LVLI the
        |                                        MGEF script points at)
        '-- Player Title (BOOK)            stops once learned (HasLearnedRecipe)

Pack names, descriptions, scrip / treasury-note amounts, the mutation lists,
the Fallout 1st player threshold, every item, quantity and rate come from the
exports, so a patch that changes any of them updates the page on the next run.

RATES (drop-rate-engine): the shared rng76 engine only. Each row's rate is
Rng76Resolver.appearance_prob(contents list, item) - the chance the item is
in the pack when you open it (For Each re-rolls and UseAll max_count rules
applied by the engine). Category headers carry the chance of getting anything
from that list. No rate maths is re-implemented here.

NO --pts MODE, ON PURPOSE (same as build_daily_ops_rewards_json.py):
dfbnb-pts-build.yml normalises tsv/pts/ over tsv/, runs this script normally,
then relocates dist/ -> dist/pts/. A --pts flag here could only cross channels.

Usage:
    python3 src/build_mutated_events_json.py
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from rng76 import Rng76Data, newest, read_tsv, prettify_lvli_label   # noqa: E402
import build_seasonal_events_json as sev    # import-safe: helpers only, writes nothing

if "--pts" in sys.argv:
    raise SystemExit(
        "build_mutated_events_json.py has no --pts mode.\n"
        "The PTS twin is built by dfbnb-pts-build.yml, which normalises\n"
        "tsv/pts/ over tsv/, runs this script normally, then relocates\n"
        "dist/ -> dist/pts/."
    )

TSV_ROOT = str(_REPO_ROOT / "tsv")
OUT_DIR = _REPO_ROOT / "dist" / "mutated_events"
OUT_FILE = OUT_DIR / "mutated_events_all_rewards.json"

PAGE_SLUG = "mutated-events-all-rewards"
PAGE_URL = "/df/mutated-events/mutated-events-all-rewards/"
IMAGE_BASE = "/wp-content/uploads/guide-images/mutated-events/reward-images/"
# Plans and apparel reuse the plan-checklist art (same folders the Daily Ops
# checklist reads); titles fall back to the blank name-tag placeholder until
# their own render is uploaded.
PLAN_IMG_BASE = "/wp-content/uploads/guide-images/plan-checklist/"
PLAN_MASTER = _REPO_ROOT / "dist" / "plan_master.json"
TITLE_PLACEHOLDER = "/wp-content/uploads/guide-images/seasonal-events/invaders-from-beyond/reward-images/player-title-invader.avif"

# The one anchor record. Found by EDID first (survives a FormID change),
# FormID as the fallback.
ROOT_LIST_EDID = "LL_MutatedEvents_Rewards"
ROOT_LIST_FID = "0067F389"

# Page copy. Only words live here - every number on the page is from the data.
PAGE_NAME = "Mutated Public Events"
CATEGORY_NAME = "Mutated Events"
DESCRIPTION = ("Finish a public event while it is mutated to earn a reward pack, "
               "then open it for a legendary item and a chance at a rare plan or outfit.")

# Category display order (rewards-style-guide category constant, adapted).
CATEGORY_ORDER = [
    "completion", "legendary", "ultraRare", "rare", "weapons", "serums",
    "aid", "treasureMaps", "ammo", "junk",
]
CATEGORY_LABELS = {
    "completion":   "Event Completion",
    "legendary":    "Legendary Item",
    "ultraRare":    "Ultra Rare Rewards",
    "rare":         "Rare Rewards",
    "weapons":      "Weapon Rewards",
    "serums":       "Serums",
    "aid":          "Aid",
    "treasureMaps": "Treasure Maps",
    "ammo":         "Contextual Ammo",
    "junk":         "Junk & Scrap",
}
# How a category's items are awarded -> the subtitle template
# (rewards-style-guide blurb templates).
CATEGORY_MODE = {
    "completion":   "independent",
    "legendary":    "pickOne",
    "ultraRare":    "pickOne",
    "rare":         "pickOne",
    "weapons":      "pickOne",
    "serums":       "pickOne",
    "aid":          "independent",
    "treasureMaps": "pickOne",
    "ammo":         "ammo",
    "junk":         "independent",
}
UNIQUE_KEYS = {"ultraRare", "rare"}

# EDID / signature -> category for one entry of a pack's contents list.
# Checked in order; anything that matches nothing gets a prettified label and
# a [WARN] so a new list never slips through silently.
ENTRY_RULES = [
    (lambda e, s: s == "LGDI",                                  "legendary"),
    (lambda e, s: re.search(r"Weapon", e, re.I),                "weapons"),
    (lambda e, s: re.search(r"Serum", e, re.I),                 "serums"),
    (lambda e, s: re.search(r"TreasureMap", e, re.I),           "treasureMaps"),
    (lambda e, s: re.search(r"Ammo", e, re.I) or s == "AMMO",   "ammo"),
    (lambda e, s: re.search(r"Stimpak|Chem|Aid|Food", e, re.I) or s == "ALCH", "aid"),
    (lambda e, s: re.search(r"Scrap|Component|Screws|Junk", e, re.I) or s == "MISC", "junk"),
]

MIN_RATE = 1e-9


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _fid(ref):
    return (ref or "").split(":")[0].strip().upper()


def _sig(ref):
    parts = (ref or "").split(":")
    return parts[-1].strip().upper() if len(parts) >= 3 else ""


def _edid(ref):
    parts = (ref or "").split(":")
    return parts[1].strip() if len(parts) >= 2 else ""


def _pct(p):
    return round(max(0.0, min(1.0, float(p))) * 100.0, 6)


def _rows(pattern, exclude=None):
    try:
        return read_tsv(newest(os.path.join(TSV_ROOT, pattern), exclude_substrings=exclude))
    except FileNotFoundError:
        print("  [WARN] no export matching {}".format(pattern))
        return []


def _entry_conditions(entry):
    out = []
    try:
        n = int(float(entry.get("CondCount") or 0))
    except ValueError:
        n = 0
    for i in range(1, max(n, 0) + 1):
        c = (entry.get("Cond{}".format(i)) or "").strip()
        if c:
            out.append(c)
    return out


def _entry_qty(entry):
    try:
        return max(1, int(round(float(entry.get("LVIV_Quantity") or 1))))
    except ValueError:
        return 1


def _clean_name(name):
    # FULL names occasionally carry editor junk (e.g. a "¬¬¬ " prefix).
    return re.sub(r"^[^\w\"'(]+", "", name or "").strip()


def _join(words):
    words = [w for w in words if w]
    if len(words) <= 1:
        return "".join(words)
    return ", ".join(words[:-1]) + " and " + words[-1]


# ---------------------------------------------------------------------------
# conditions on the pack lists
# ---------------------------------------------------------------------------

_MUTATION_RE = re.compile(r'GetPublicEventHasMutation\([^"]*"([^"]+)"')
_FO1_RE = re.compile(r'IsPlayerFO1Member\(.*\)\s+([01]{8})\s+\S+\s*\[GLOB:([0-9A-Fa-f]{8})\]')


def _pack_gate(conds, globs):
    """Read a pack list's gate: which mutations, double or single, and the
    Fallout 1st player threshold (operator + GLOB value)."""
    mutations, fo1 = [], None
    for c in conds:
        m = _MUTATION_RE.search(c)
        if m:
            name = m.group(1)
            name = re.sub(r"\s*\(Double\)\s*$", "", name).strip()
            if name not in mutations:
                mutations.append(name)
            continue
        f = _FO1_RE.search(c)
        if f:
            bits, gfid = f.group(1), f.group(2).upper()
            op = int(bits[0]) + 2 * int(bits[1]) + 4 * int(bits[2])
            val = globs.value(gfid)
            fo1 = {"op": op, "value": int(round(val)) if val is not None else None}
    is_double = any("(Double)" in c or "DoubleMutation" in c for c in conds)
    return sorted(mutations), is_double, fo1


def _fo1_text(fo1):
    if not fo1 or fo1.get("value") is None:
        return ""
    n, op = fo1["value"], fo1["op"]
    if op == 3:
        return "{} or more players taking part have Fallout 1st".format(n)
    if op == 2:
        return "more than {} players taking part have Fallout 1st".format(n)
    if op == 4:
        return "fewer than {} players taking part have Fallout 1st".format(n)
    if op == 5:
        return "{} or fewer players taking part have Fallout 1st".format(n)
    return ""


# ---------------------------------------------------------------------------
# pack contents
# ---------------------------------------------------------------------------

def _category_for(ref):
    e, s = _edid(ref), _sig(ref)
    for test, key in ENTRY_RULES:
        if test(e, s):
            return key
    return None


def _rarity_key(edid):
    if re.search(r"UltraRare", edid, re.I):
        return "ultraRare"
    if re.search(r"Rare|Chase", edid, re.I):
        return "rare"
    return None


# ---------------------------------------------------------------------------
# named weapons (LLKC filter keyword -> ObjectTemplate combination)
# ---------------------------------------------------------------------------
# A weapon list like LL_MutatedEvents_Rewards_Weapon_SniperRifle points at the
# base WEAP and carries an LLKC filter keyword (if_tmp_Longshot). The game
# uses that keyword to pick the WEAP's ObjectTemplate combination tagged with
# it - the named version (Longshot: custom mod + fixed legendary stars).
#
# Neither export carries the keyword itself (the LVLI List LLKC column and the
# ObjectTemplate combination keywords are blank), but the KYWD Refs export
# lists every record that uses each if_tmp_* keyword - the LVLI that filters
# on it and the WEAP whose template carries it. The combination is then the
# one whose mod_Custom_<Name> matches the keyword's <Name>
# (if_tmp_Longshot -> mod_Custom_Longshot). Keywords with no custom mod
# (if_tmp_Fancy) are cosmetic presets and leave the weapon as it is.

def _load_tmpl_keywords():
    """{keyword EDID: set(FormIDs that reference it)} for if_tmp_* keywords."""
    out = {}
    for r in _rows("KYWD_Export_*_Refs.tsv"):
        kw = (r.get("KeywordEDID") or r.get("KYWD_EDID") or "").strip()
        if not kw.lower().startswith("if_tmp_"):
            continue
        ref = (r.get("RefFormID") or "").strip().upper()
        if ref:
            out.setdefault(kw, set()).add(ref)
    return out


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _norm_keep_us(s):
    return re.sub(r"[^a-z0-9_]", "", (s or "").lower())


_OMOD_POOL_CACHE = None


def _omod_pool(omod_fid):
    """(EDID, [effect names]) of an OMOD mod collection, from the OMOD
    export's Includes_Flat (mod_Legendary_* members only)."""
    global _OMOD_POOL_CACHE
    if _OMOD_POOL_CACHE is None:
        _OMOD_POOL_CACHE = {}
        for r in _rows("OMOD_Export_*.tsv", exclude=["_Properties"]):
            fid = (r.get("OMOD_FormID") or "").strip().upper()
            flat = r.get("Includes_Flat") or ""
            if fid and "mod_Legendary_" in flat:
                members = re.findall(r'(mod_Legendary_\w+)\s+"+([^"]+)"+', flat)
                _OMOD_POOL_CACHE[fid] = ((r.get("OMOD_EDID") or "").strip(), members)
    return _OMOD_POOL_CACHE.get((omod_fid or "").upper(), ("", []))


def _twin_weapon_combo(sig, base_fid, paint_ref):
    """The named twin of a base weapon: another WEAP whose template carries the
    same preset paint (Fancy Single Action Revolver 0034CBD8 for the plain
    revolver's if_tmp_Fancy). Returns (twin fid, twin FULL, include refs)."""
    paint = re.split(r'["\[]', paint_ref)[0].strip().lower()
    col = sig.upper() + "_FormID"
    found = {}
    for r in sev._ot_rows(sig):
        f = (r.get(col) or "").upper()
        if not f or f == base_fid.upper():
            continue
        found.setdefault((f, r.get("CombinationIndex") or "0"),
                         {"full": r.get(sig.upper() + "_FULL") or "", "refs": []})["refs"].append(
            r.get("Include_Mod") or "")
    for (f, _ci), v in sorted(found.items()):
        if any(re.split(r'["\[]', x)[0].strip().lower() == paint for x in v["refs"]):
            return f, v["full"], v["refs"]
    return None, "", []


def _template_mod_slots(fid, sig, keyword):
    """Mod slots of the ObjectTemplate combination a template keyword selects,
    in the same shape as sev._resolve_mod_slots (custom mod + 1-4 star rows)."""
    want = _norm(re.sub(r"^if_tmp_", "", keyword, flags=re.I))
    if not want:
        return None
    col = sig.upper() + "_FormID"
    combos = {}
    for r in sev._ot_rows(sig):
        if (r.get(col) or r.get("FormID") or "").upper() != fid.upper():
            continue
        ref = r.get("Include_Mod") or ""
        try:
            ci = int(r.get("CombinationIndex") or 0)
        except ValueError:
            ci = 0
        if ref:
            combos.setdefault(ci, []).append(ref)
    hit = None
    for ci in sorted(combos):
        for ref in combos[ci]:
            _lab, _val, edid_lower = sev._ot_classify(ref)
            m = re.search(r"mod_custom_(.+)$", edid_lower)
            if m and _norm(m.group(1)) == want:
                hit = ci
                break
        if hit is not None:
            break
    paint_only = False
    if hit is None:
        # No custom mod: a cosmetic preset (if_tmp_Fancy). Its combination is
        # the one carrying that name's paint (MTNL01_..._Paint_Fancy), which
        # also adds the dn_Weapon_<Name> name keyword -> "Fancy <weapon>".
        for ci in sorted(combos):
            if any(re.search(r"paint_" + re.escape(want) + r"$", _norm_keep_us(sev._ot_classify(ref)[2]))
                   for ref in combos[ci]):
                hit, paint_only = ci, True
                break
    if hit is None:
        return None
    refs = list(combos[hit])
    twin = None
    pools = {}
    if paint_only:
        # The preset only paints the base weapon. The named twin record that
        # carries the same paint (MTNL01_SingleActionRevolver_Fancy) holds the
        # legendary part - e.g. MTNL01_modcol_Legendary_Weapons_Fancy
        # (OMOD 0014E5C0), a 1-star-only roll list. Use the twin's template.
        paint_ref = next((x for x in refs if re.search(r"paint_" + re.escape(want) + r"$",
                                                         _norm_keep_us(sev._ot_classify(x)[2]))), "")
        tfid, tfull, trefs = _twin_weapon_combo(sig, fid, paint_ref)
        if tfid:
            twin = {"formid": tfid, "name": tfull}
            refs = trefs
    custom_name, custom_desc, stars, extras = "", "", {}, []
    for ref in refs:
        lab, value, edid_lower = sev._ot_classify(ref)
        if (value or "").strip().lower() in sev._OT_JUNK_VALUES:
            continue
        if "mod_custom_" in edid_lower:
            custom_name = value or custom_name
            mfid = re.search(r"\[OMOD:([0-9A-Fa-f]+)\]", ref)
            if mfid:
                d = sev._omod_desc_by_fid().get(mfid.group(1).upper(), "")
                if d and d.strip().lower() not in sev._OT_JUNK_VALUES:
                    custom_desc = d.strip()
            continue
        coll = re.search(r"\[OMOD:([0-9A-Fa-f]+)\]", ref)
        if edid_lower.startswith(("modcol_", "mtnl01_modcol_")) or "_modcol_legendary" in edid_lower:
            pool_edid, members = _omod_pool(coll.group(1) if coll else "")
            ranks = {int(m.group(1)) for e, _n in members
                     for m in [re.search(r"_Weapon(\d)_", e)] if m}
            if members and len(ranks) == 1 and "crafting" not in edid_lower:
                st = ranks.pop()
                stars[st] = "Random Legendary Mod"
                pools[st] = {"omod": coll.group(1).upper(), "edid": pool_edid,
                             "effects": sorted({n for e, n in members if "_Melee_" not in e},
                                               key=str.lower)}
                continue
        rnd = re.search(r"legendary_crafting_(?:weapon|armor|powerarmor)(\d)", edid_lower)
        if rnd:
            # A rolled star (modcol_Legendary_Crafting_Weapon2) - same wording
            # as the activity pages.
            stars[int(rnd.group(1))] = value or "Random Legendary Mod"
        elif lab and "Legendary" in lab:
            stars[int(re.search(r"(\d)", lab).group(1))] = value
        elif lab in ("Lining", "Appearance"):
            extras.append((lab, value))
    slots = []
    if stars:
        for st in range(1, max(4, max(stars)) + 1):
            slots.append({"label": "{}★ Legendary".format(st), "value": stars.get(st)})
    for lab, value in extras:
        slots.append({"label": lab, "value": value})
    out = {"modSlots": slots, "templateKeyword": keyword, "templateIndex": hit}
    if paint_only:
        out["namePrefix"] = re.sub(r"^if_tmp_", "", keyword, flags=re.I)
    if twin:
        out["twin"] = twin
    if pools:
        out["rollPools"] = pools
    if custom_name:
        out["customModName"] = custom_name
    if custom_desc:
        out["customModDescription"] = custom_desc
    return out


class PackBuilder:
    def __init__(self, data):
        self.data = data
        self.r = data.resolver
        self.lvli = data.lvli
        self.non_trade, self.unsellable = sev._load_kywd_flags()
        self.unknown = []
        self.tmpl_kw = _load_tmpl_keywords()
        self.named = {}          # (weapon fid, list fid) -> template keyword

    def subtree_lists(self, list_fid, seen=None):
        """Every LVLI reachable from list_fid, list_fid included."""
        seen = seen if seen is not None else set()
        if list_fid in seen:
            return seen
        seen.add(list_fid)
        for e in self.entries(list_fid):
            ref = e.get("LVLO_Reference") or ""
            if _sig(ref) == "LVLI":
                self.subtree_lists(_fid(ref), seen)
        return seen

    def template_keyword(self, ref, weap_fid):
        """The if_tmp_* keyword that names this weapon when it comes out of the
        list `ref` (the LVLI filters on it AND the WEAP's template carries it)."""
        if _sig(ref) != "LVLI":
            return None
        lists = self.subtree_lists(_fid(ref))
        for kw, users in self.tmpl_kw.items():
            if weap_fid in users and users & lists:
                return kw
        return None

    def entries(self, list_fid):
        return self.lvli.entries_by_list.get(list_fid, [])

    def entry_chance(self, list_fid, entry):
        """Chance one entry of a UseAll (max_count 0) list fires - straight from
        the engine's own pick-weight x ChanceNone helper. None for any other
        list mode (the caller then falls back to appearance_prob)."""
        flags = self.lvli.flags_for(list_fid)
        if not flags.get("use_all"):
            return None
        if self.lvli.max_count_for(list_fid, self.data.globs, self.data.curvs) != 0:
            return None
        math = self.lvli.math_by_entry.get((list_fid, entry.get("EntryIndex")))
        if not math:
            return None
        grp = self.r.extract_grp_chance(_entry_conditions(entry))
        if grp is not None:
            return grp
        pw, cn = self.r._entry_pick_and_cn(math, entry, list_fid)
        return pw * cn

    def split_entries(self, contents_fid):
        """[(category key, ref, entry, chance)] for the pack's contents list.
        Rare / chase lists are opened one level so each rarity gets its own
        category; chance = the chance that entry (path) fires, or None."""
        out = []
        for e in self.entries(contents_fid):
            ref = e.get("LVLO_Reference") or ""
            ch = self.entry_chance(contents_fid, e)
            key = _category_for(ref)
            if key is None and _sig(ref) == "LVLI":
                children = self.entries(_fid(ref))
                kids = [(_rarity_key(_edid(c.get("LVLO_Reference") or "")), c) for c in children]
                if kids and all(k for k, _ in kids):
                    for k, c in kids:
                        cc = self.entry_chance(_fid(ref), c)
                        both = None if (ch is None or cc is None) else ch * cc
                        out.append((k, c.get("LVLO_Reference") or "", c, both))
                    continue
                key = _rarity_key(_edid(ref))
            if key is None:
                label = prettify_lvli_label(_edid(ref)) or _edid(ref)
                key = "x-" + re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
                CATEGORY_LABELS.setdefault(key, label)
                CATEGORY_MODE.setdefault(key, "independent")
                self.unknown.append(_edid(ref))
            out.append((key, ref, e, ch))
        return out

    def via_entry(self, ref, entry, chance, fid):
        """Chance `fid` comes out of this one entry: entry chance x the item's
        appearance in the sub-list (re-rolled qty times for a For Each list)."""
        if _sig(ref) != "LVLI":
            return chance if _fid(ref) == fid else 0.0
        sub = _fid(ref)
        a = self.r.appearance_prob(sub, fid)
        q = _entry_qty(entry)
        if q > 1 and self.lvli.flags_for(sub).get("for_each"):
            a = 1.0 - (1.0 - min(a, 1.0)) ** q
        return chance * min(a, 1.0)

    def leaves(self, ref, entry):
        """Resolved leaves under one entry: [{formid, name, edid, sig, qty, conditions}]."""
        fid, sig = _fid(ref), _sig(ref)
        qty = _entry_qty(entry)
        conds = _entry_conditions(entry)
        if sig == "LVLI":
            rows = []
            for it in self.r.resolve_deep(fid):
                if it.get("dropRate", 0) <= MIN_RATE:
                    continue
                wfid = (it.get("formid") or "").upper()
                tk = self.template_keyword(ref, wfid) if (it.get("sig") or "").upper() == "WEAP" else None
                rows.append({
                    "template": tk,
                    "formid": (it.get("formid") or "").upper(),
                    "name": it.get("name") or "",
                    "edid": it.get("edid") or "",
                    "sig": (it.get("sig") or "").upper(),
                    "qty": int(it.get("qty") or 1) * qty,
                    "conditions": conds + list(it.get("conditions") or []),
                })
            return rows
        return [{
            "formid": fid, "name": self.data.names.resolve(fid, _edid(ref)),
            "edid": _edid(ref), "sig": sig, "qty": qty, "conditions": conds,
        }]

    def item_dict(self, leaf, rate, qmin, qmax):
        fid, sig = leaf["formid"], leaf["sig"]
        name = _clean_name(leaf["name"])
        if sig == "AMMO":
            name = sev._th_ammo_full(TSV_ROOT, fid) or sev._th_ammo_name(name)
        if sig == "LGDI":
            name = "Legendary Item"
        name = sev._DISPLAY_NAME_OVERRIDES.get(fid.lower(), name)
        it = {"name": name, "formid": fid, "edid": leaf["edid"], "sig": sig,
              "qty": qmin, "rate": rate}
        if qmax != qmin:
            it["qtyMax"] = qmax
        if sig in ("ARMO", "BOOK", "WEAP"):
            it["tradeable"] = fid.lower() not in self.non_trade
            if fid.lower() in self.unsellable:
                it["unsellable"] = True
        # Named weapons: only when the list's filter keyword selects a template
        # with a custom mod (see _template_mod_slots). Never the WEAP's "best"
        # combination - that would be a guess, not what the pack gives.
        if sig == "WEAP" and leaf.get("template"):
            mods = _template_mod_slots(fid, sig, leaf["template"])
            if mods:
                it["modSlots"] = mods["modSlots"]
                it["templateKeyword"] = mods["templateKeyword"]
                if mods.get("customModName"):
                    it["customModName"] = mods["customModName"]
                if mods.get("customModDescription"):
                    it["customModDescription"] = mods["customModDescription"]
                if mods.get("rollPools"):
                    it["rollPools"] = mods["rollPools"]
                if mods.get("twin"):
                    it["templateWeapon"] = mods["twin"]
                if mods.get("namePrefix") and not it["name"].startswith(mods["namePrefix"]):
                    it["name"] = mods["namePrefix"] + " " + it["name"]
        conds = sev._simplify_conditions(leaf.get("conditions") or [])
        if conds:
            it["conditions"] = conds
        return it

    def categories(self, contents_fid):
        groups = {}
        for key, ref, entry, ch in self.split_entries(contents_fid):
            g = groups.setdefault(key, {"entries": [], "leaves": []})
            g["entries"].append((ref, entry, ch))
            for lf in self.leaves(ref, entry):
                lf["_src"] = (ref, entry, ch)
                g["leaves"].append(lf)

        out = []
        keys = sorted(groups, key=lambda k: (CATEGORY_ORDER.index(k)
                                             if k in CATEGORY_ORDER else 50, k))
        for key in keys:
            g = groups[key]
            by_fid = {}
            for lf in g["leaves"]:
                if not lf["formid"] or sev.is_excluded(lf["edid"]):
                    continue
                b = by_fid.setdefault(lf["formid"], {"leaf": lf, "q": [], "src": []})
                b["q"].append(lf["qty"])
                if lf["_src"] not in b["src"]:
                    b["src"].append(lf["_src"])
            if not by_fid:
                continue
            exact = all(ch is not None for _, _, ch in g["entries"])
            items = []
            for fid, b in by_fid.items():
                if exact:
                    # Entries of a UseAll max-0 list roll independently, so the
                    # item's chance in THIS category is the union of its entries.
                    miss = 1.0
                    for ref, entry, ch in b["src"]:
                        miss *= 1.0 - self.via_entry(ref, entry, ch, fid)
                    p = 1.0 - miss
                else:
                    p = self.r.appearance_prob(contents_fid, fid)
                if p <= MIN_RATE:
                    continue
                it = self.item_dict(b["leaf"], _pct(p), min(b["q"]), max(b["q"]))
                # Chance the item is in the pack by ANY route (an outfit can sit
                # in two lists) - used by the checklist, dropped before writing.
                it["_packRate"] = _pct(self.r.appearance_prob(contents_fid, fid))
                items.append(it)
            if not items:
                continue
            items.sort(key=lambda x: x["name"].lower())
            mode = CATEGORY_MODE.get(key, "independent")
            if mode == "independent":
                head = None
            elif exact:
                miss = 1.0
                for _, _, ch in g["entries"]:
                    miss *= 1.0 - ch
                head = _pct(1.0 - miss)
            else:
                head = _pct(self.r.appearance_prob(contents_fid, list(by_fid.keys())))
            cat = {
                "key": key,
                "label": CATEGORY_LABELS.get(key, key),
                "mode": mode,
                "rate": head,
                "isUnique": key in UNIQUE_KEYS,
                "items": items,
            }
            if key == "legendary":
                cat["note"] = ("One legendary item, picked from armour, melee "
                               "weapons, ranged weapons and power armour.")
            if key == "treasureMaps":
                cat["note"] = "The map is for the region the event takes place in."
            out.append(cat)
        return out


# ---------------------------------------------------------------------------
# packs
# ---------------------------------------------------------------------------

def _load_alch():
    alch = {}
    for r in _rows("ALCH_Export_*.tsv", exclude=["_Effects"]):
        fid = (r.get("ALCH_FormID") or "").strip().upper()
        if fid:
            alch[fid] = r
    effects = {}
    for r in _rows("ALCH_Export_*_Effects.tsv"):
        fid = (r.get("ALCH_FormID") or "").strip().upper()
        mg = (r.get("MGEF_FormID") or "").strip().upper()
        if fid and mg:
            effects.setdefault(fid, []).append(mg)
    return alch, effects


def _lvli_by_mgef():
    """MGEF FormID -> the LVLI it references (LVLI Refs export: a contents
    list's only referrer is the pack effect that hands it out)."""
    out = {}
    for r in _rows("LVLI_Export_*_LVLI_Refs.tsv"):
        lfid = (r.get("LVLI_FormID") or "").strip().upper()
        for k, v in r.items():
            if k and k.startswith("Ref") and v and v.endswith(":MGEF"):
                out.setdefault(_fid(v), lfid)
    return out


def _find_root(lvli):
    for fid, row in lvli.list_by_formid.items():
        if (row.get("LVLI_EDID") or "") == ROOT_LIST_EDID:
            return fid
    return ROOT_LIST_FID if ROOT_LIST_FID in lvli.list_by_formid else None


def _public_events(root_fid):
    names = []
    for r in _rows("GMRW_Export_*.tsv"):
        if _fid(r.get("RewardedItem")) != root_fid:
            continue
        if sev.is_excluded(r.get("EDID") or "") or (r.get("EDID") or "").lower().startswith("zzz"):
            continue
        n = re.sub(r"^Event:\s*", "", (r.get("ParentQuestDisplay") or "").strip())
        if n and n not in names:
            names.append(n)
    return sorted(names, key=str.lower)


def build():
    print("[build_mutated_events] Loading rng76 engine...")
    data = Rng76Data.from_tsv_root(TSV_ROOT)
    lvli = data.lvli
    root = _find_root(lvli)
    if not root:
        raise SystemExit("[build_mutated_events] {} not found in the LVLI export".format(ROOT_LIST_EDID))
    print("  root list {} ({})".format(ROOT_LIST_EDID, root))

    alch, alch_effects = _load_alch()
    mgef_to_lvli = _lvli_by_mgef()
    pb = PackBuilder(data)

    packs, title_items = [], []
    for e in lvli.entries_by_list.get(root, []):
        ref = e.get("LVLO_Reference") or ""
        sig = _sig(ref)
        conds = _entry_conditions(e)
        if sig != "LVLI":
            # Rewards on the root list itself (the Player Title) - every pack.
            leaf = {"formid": _fid(ref), "name": data.names.resolve(_fid(ref), _edid(ref)),
                    "edid": _edid(ref), "sig": sig, "qty": _entry_qty(e), "conditions": conds}
            it = pb.item_dict(leaf, 100.0, leaf["qty"], leaf["qty"])
            if any("HasLearnedRecipe" in c for c in conds):
                it["dropPersistence"] = "stops"
            title_items.append(it)
            continue

        list_fid = _fid(ref)
        mutations, is_double, fo1 = _pack_gate(conds, data.globs)
        package, paid = None, []
        for pe in lvli.entries_by_list.get(list_fid, []):
            pref = pe.get("LVLO_Reference") or ""
            if _sig(pref) == "ALCH" and _fid(pref) in alch_effects:
                package = (pref, pe)
            else:
                paid.append((pref, pe))
        if not package:
            print("  [WARN] {} has no pack (ALCH with an effect) - skipped".format(_edid(ref)))
            continue

        pfid = _fid(package[0])
        prow = alch.get(pfid, {})
        contents = None
        for mg in alch_effects.get(pfid, []):
            if mg in mgef_to_lvli:
                contents = mgef_to_lvli[mg]
                break
        if not contents:
            print("  [WARN] pack {} - no contents list found via its effect".format(pfid))
            continue

        completion = []
        for pref, pe in paid:
            for lf in pb.leaves(pref, pe):
                completion.append(pb.item_dict(lf, 100.0, lf["qty"], lf["qty"]))
        completion += [dict(t) for t in title_items]   # filled below if the title comes later

        cats = pb.categories(contents)
        name = _clean_name(prow.get("FULL") or "") or sev.humanize_edid(_edid(package[0]))
        packs.append({
            "key": re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-"),
            "name": name,
            "formid": pfid,
            "edid": prow.get("ALCH_EDID") or _edid(package[0]),
            "description": (prow.get("DESC") or "").strip(),
            "rewardList": {"formid": list_fid, "edid": _edid(ref)},
            "contentsList": {"formid": contents, "edid": lvli.edid_for(contents)},
            "mutationType": "double" if is_double else "single",
            "mutations": mutations,
            "fallout1st": fo1,
            "fallout1stText": _fo1_text(fo1),
            "listCount": len(lvli.entries_by_list.get(contents, [])),
            "completion": completion,
            "categories": cats,
        })
        print("  pack {:<28} contents {} - {} categories".format(name, contents, len(cats)))

    # The title sits after the pack lists on the root, so add it to any pack
    # built before it was seen.
    for p in packs:
        have = {c["formid"] for c in p["completion"]}
        for t in title_items:
            if t["formid"] not in have:
                p["completion"].append(dict(t))
        p["completion"].sort(key=lambda x: (x.get("sig") == "BOOK", x["name"].lower()))
        p["completionCategory"] = {
            "key": "completion", "label": CATEGORY_LABELS["completion"],
            "mode": "independent", "rate": None, "isUnique": False,
            "items": p["completion"],
        }
        p["categories"].insert(0, p.pop("completionCategory"))
        p.pop("completion")
        for c in p["categories"]:
            # Ultracite next to its standard round, same as the TH page.
            if c["key"] == "ammo":
                c["items"].sort(key=lambda x: (re.sub(r"^Ultracite\s+", "", x["name"]).lower(),
                                               x["name"].startswith("Ultracite")))

    if pb.unknown:
        print("  [WARN] lists not sorted into a category: {}".format(", ".join(sorted(set(pb.unknown)))))

    # Pack order: single before double, standard before Party Pack.
    packs.sort(key=lambda p: (p["fallout1st"] is not None and p["fallout1st"].get("op") in (2, 3),
                              p["mutationType"] == "double"))
    return data, root, packs, title_items


def build_legendary_effects(packs):
    """Every effect a "Random Legendary Mod" star can roll on these weapons.

    The random stars on the pack weapons (and the Fancy 1-star) are the mod
    collections modcol_Legendary_Crafting_Weapon1..4. Each collection's pool
    is listed in the LGDI Mods export (ModPool_Refs); names and effect text
    come from the OMOD export. All the pack weapons are guns, so melee-only
    effects are left out. Reference list only - not a reward."""
    used = set()
    special = {}        # own roll lists (Fancy revolver: OMOD 0014E5C0)
    for p in packs:
        for c in p["categories"]:
            if c["key"] != "weapons":
                continue
            for it in c["items"]:
                own = {int(k): v for k, v in (it.get("rollPools") or {}).items()}
                for st, pool in own.items():
                    special.setdefault(pool["omod"], {
                        "label": "{} - {}★".format(it["name"], st),
                        "star": st, "omod": pool["omod"], "edid": pool["edid"],
                        "effects": [{"name": n, "desc": ""} for n in pool["effects"]],
                    })
                for sl in it.get("modSlots") or []:
                    m = re.match(r"(\d)★", sl.get("label") or "")
                    if (m and sl.get("value") == "Random Legendary Mod"
                            and int(m.group(1)) not in own):
                        used.add(int(m.group(1)))
    if not used and not special:
        return None
    omod = {}
    for r in _rows("OMOD_Export_*.tsv", exclude=["_Properties"]):
        e = (r.get("OMOD_EDID") or "").strip()
        if e.startswith("mod_Legendary_"):
            omod[e] = ((r.get("FULL") or "").strip(), (r.get("DESC") or "").strip())
    pools = {}
    for r in _rows("LGDI_Export_*_Mods.tsv"):
        m = re.match(r"^modcol_Legendary_Crafting_Weapon(\d)$", (r.get("OMOD_EDID") or "").strip())
        if not m or int(m.group(1)) not in used or int(m.group(1)) in pools:
            continue
        star = int(m.group(1))
        eff = {}
        for edid in re.findall(r"(mod_Legendary_\w+) \[OMOD:", r.get("ModPool_Refs") or ""):
            if "_Melee_" in edid:
                continue
            name, desc = omod.get(edid, ("", ""))
            if name:
                eff.setdefault(name, desc)
        pools[star] = [{"name": n, "desc": d} for n, d in sorted(eff.items(), key=lambda kv: kv[0].lower())]
    if not pools and not special:
        return None
    return {"stars": [{"star": st, "effects": pools[st]} for st in sorted(pools)],
            "special": list(special.values())}


def build_checklist(packs, title_items):
    """Every unique reward once, with its rate in each pack (0% where a pack
    never drops it, so readers see none were missed)."""
    rows = {}
    order = []
    for t in title_items:
        rows[t["formid"]] = dict(t, category="Titles", packs={})
        order.append(t["formid"])
    for p in packs:
        for c in p["categories"]:
            if not c["isUnique"] and c["key"] != "completion":
                continue
            for it in c["items"]:
                if c["key"] == "completion" and it["formid"] not in rows:
                    continue
                fid = it["formid"]
                if fid not in rows:
                    row = {k: v for k, v in it.items() if k not in ("rate", "qty", "qtyMax", "_packRate")}
                    row["rarityPool"] = c["key"]
                    row["category"] = ("Apparel" if it["sig"] == "ARMO" else
                                       "Weapons" if it["sig"] == "WEAP" else "Plans & Recipes")
                    row["packs"] = {}
                    rows[fid] = row
                    order.append(fid)
                elif c["key"] == "ultraRare" and rows[fid].get("rarityPool") == "rare":
                    rows[fid]["rarityPool"] = "ultraRare"   # the harder pull wins
                rate = it.get("_packRate", it["rate"])
                prev = rows[fid]["packs"].get(p["key"])
                if prev is None or rate > prev["rate"]:
                    rows[fid]["packs"][p["key"]] = {"rate": rate, "qty": it.get("qty", 1)}
    out = []
    for fid in order:
        row = rows[fid]
        tiers = []
        for p in packs:
            hit = row["packs"].get(p["key"])
            if hit:
                tiers.append({"tier": p["name"], "qty": hit["qty"], "rate": hit["rate"]})
            else:
                tiers.append({"tier": p["name"], "qty": None, "rate": 0, "notDropped": True})
        row.pop("packs")
        row["tiers"] = tiers
        row["dropRate"] = max(t["rate"] for t in tiers) if tiers else 0
        row.setdefault("dropPersistence", "keeps")
        out.append(row)
    rank = {"Titles": 0}
    out.sort(key=lambda r: (rank.get(r["category"], 1), r["name"].lower()))
    return out


def _plan_master_index():
    try:
        with open(PLAN_MASTER, encoding="utf-8") as f:
            pm = json.load(f)
    except (OSError, ValueError) as e:
        print("  [WARN] plan_master.json not read ({}) - plan images skipped".format(e))
        return {}
    out = {}
    for it in pm.get("items") or []:
        fid = str(it.get("id") or "").split("_")[-1].upper()
        if fid:
            out[fid] = it
    return out


def _load_hosted_index():
    """The site-wide "hosted first" image index (plan_images.py): scoreboard
    art, titles, player icons, CAMP pages, Atom Shop, bundles and the season
    upload manifests - every picture the site already serves. Reusing those
    URLs means nothing gets uploaded twice."""
    try:
        import plan_images
    except ImportError as e:
        print("  [WARN] plan_images not importable ({}) - hosted art skipped".format(e))
        return None, None
    cwd = os.getcwd()
    try:
        os.chdir(_REPO_ROOT)            # plan_images reads tsv/, dist/, data/ relative
        idx, _staged = plan_images.load(dist_dir="dist", tsv_dir="tsv", verbose=False)
        return plan_images, idx
    except Exception as e:              # noqa: BLE001 - images are never fatal
        print("  [WARN] hosted image index failed: {}".format(e))
        return None, None
    finally:
        os.chdir(cwd)


def _image_list(row, pm, pi, idx, stats):
    """Ordered image URLs for a checklist row, first hit first:
        1. art the site already hosts (by FormID, EditorID, then name)
        2. for a plan: the hosted art of the thing it builds, then the plan
           checklist's own staged art (plan_master)
        3. this page's own folder (reward-images/)
        4. titles: the blank name-tag placeholder
    The renderer tries each in turn, then the letter placeholder."""
    urls = []
    source = "own"
    if pi and idx:
        url, src = pi.hosted_url(idx, fids=[row["formid"]], edids=[row.get("edid")],
                                 names=[row["name"]])
        if url:
            urls.append(url)
            source = src
    p = pm.get(row["formid"]) or {}
    if p:
        if pi and idx and not urls:
            url, src = idx.lookup(p)
            if url:
                urls.append(url)
                source = src
        generic = set((getattr(pi, "GENERIC_ART", {}) or {}).values()) if pi else set()
        # Weapon mod plans use the site's shared yellow mod-box art
        # (plan_images.GENERIC_ART), same as the plan checklists and scoreboard
        # pages. Only when the plan really builds a mod (an OMOD) - plan_master
        # also tags a few non-mods (e.g. Radioactive Barrel, an ACTI) that way.
        # Ask the plan checklists' own rule (plan_images.generic_kind) live,
        # rather than trusting plan_master's stored images, so a fix to that
        # rule (paints / skins are not mods) lands here on the next build.
        try:
            builds_mod = pi.generic_kind(p) == "weapon-mod"
        except Exception:                  # noqa: BLE001
            builds_mod = ((p.get("cnam") or {}).get("sig") or "").upper() == "OMOD"
        for img in p.get("images") or []:
            if not img:
                continue
            u = img if img.startswith("/") else PLAN_IMG_BASE + (p.get("image_dir") or "") + "/" + img + ".avif"
            if u in generic and not builds_mod:
                continue
            urls.append(u)
            if source == "own":
                source = "weapon-mod" if u in generic else "plan-checklist"
    urls.append(IMAGE_BASE + sev.slugify_item(row["name"]) + ".avif")
    if re.match(r"^(Player|Camp) Title:", row["name"]):
        urls.append(TITLE_PLACEHOLDER)
    stats[source] = stats.get(source, 0) + 1
    seen, out = set(), []
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def main():
    data, root, packs, title_items = build()
    if not packs:
        raise SystemExit("[build_mutated_events] no packs built - refusing to write an empty page")
    checklist = build_checklist(packs, title_items)
    pm = _plan_master_index()
    pi, idx = _load_hosted_index()
    img_stats = {}
    for row in checklist:
        row["images"] = _image_list(row, pm, pi, idx, img_stats)
        row["imageUrl"] = row["images"][0]
        p = pm.get(row["formid"]) or {}
        if isinstance(p.get("tradeable"), bool) and row.get("sig") == "BOOK":
            row["tradeable"] = p["tradeable"]    # plan checklists' source of truth
    for p in packs:
        for c in p["categories"]:
            for it in c["items"]:
                it.pop("_packRate", None)

    fo1_values = [p["fallout1st"]["value"] for p in packs
                  if p.get("fallout1st") and p["fallout1st"].get("value") is not None]
    page = {
        "name": PAGE_NAME,
        "categoryName": CATEGORY_NAME,
        "slug": PAGE_SLUG,
        "url": PAGE_URL,
        "description": DESCRIPTION,
        "rootList": {"formid": root, "edid": data.lvli.edid_for(root)},
        "publicEvents": _public_events(root),
        "fallout1stThreshold": fo1_values[0] if fo1_values else None,
        "packs": packs,
        "checklist": checklist,
        "legendaryEffects": build_legendary_effects(packs),
        "imageBase": IMAGE_BASE,
    }
    out = {
        "version": 1,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": {
            "lvli": os.path.basename(newest(os.path.join(TSV_ROOT, "LVLI_Export_*_LVLI_Entries.tsv"))),
        },
        "byPage": {PAGE_SLUG: page},
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = OUT_FILE.with_suffix(".json.part")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1, ensure_ascii=False)
    with open(tmp, encoding="utf-8") as f:
        json.load(f)                       # parse-check before it replaces the live copy
    os.replace(tmp, OUT_FILE)
    print("[build_mutated_events] checklist art: {}".format(
        ", ".join("{} {}".format(v, k) for k, v in sorted(img_stats.items()))))
    print("[build_mutated_events] {} packs, {} checklist items, {} public events".format(
        len(packs), len(checklist), len(page["publicEvents"])))
    print("[build_mutated_events] Written: {} ({} bytes)".format(OUT_FILE, OUT_FILE.stat().st_size))


if __name__ == "__main__":
    main()
