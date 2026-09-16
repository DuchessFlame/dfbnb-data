#!/usr/bin/env python3
"""
add_armour_groups.py — group the two armour plan checklists by armour set.

WHY
---
/df/plan-checklists/power-armour/ rendered EMPTY for the life of the category
while its 383 plans sat on the Body Armour page, because both pages were
selecting the `armour` bucket and that bucket holds body armour and power
armour together. plan_subpages now routes them apart on the classification
plan_images already makes. This module gives both pages their shape.

Duchess's instruction, verbatim:

    "for the most part the pages will be one root expand per item but for the
     armour, put the main armour as the root and then have the mods and skins
     as sub expands like we have for weapons"

So both pages get the weapon page's shape: one root expand per armour SET, the
set's own pieces loose at the top of it, then a Mods sub-expand and a Skins
sub-expand. The data has the same shape as weapons, which is why this is the
weapon resolver with ARMO where WEAP was:

    cnam.sig == "ARMO"   the plan makes a piece of the armour   -> base
    cnam.sig == "OMOD"   the plan makes a mod or a paint        -> mod / skin

and the set is resolved by EditorID SEGMENT matching, earliest match wins,
longest breaking a tie at the same position — the same rule and for the same
reason as the weapon page: an EditorID names what a thing belongs to before it
names what it does.

Every row on either armour page gains:

  armour_role        "base" | "mod" | "skin"
  armour_group       display name of the set ("T-60", "Secret Service")
  armour_group_key   slug of the same, for DOM ids and stable sorting
  armour_group_solo  true when the group is this one plan and nothing else

A plan with no set — the Cultist robes, the Cowboy Duster, the one-off helmets
and hoods, 49 of them — becomes its own root expand named after itself, exactly
as a grenade does on the weapon page. That is not a gap; those genuinely are
standalone pieces with no set behind them.

Runs in about a second: it is a pure join against exports plan_master already
lists, no rng76 and no drop-rate work, so it is a post-pass that can be re-run
whenever a new export lands without touching the 80-minute build.

Usage:
    python3 src/add_armour_groups.py src/plan-system/plan_master.json
    python3 src/add_armour_groups.py --report-only src/plan-system/plan_master.json
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import add_weapon_groups as awg   # segs / slug / newest / read_tsv / BAD_EDID

SCHEMA = 1

PAGES = ("body-armour", "power-armour")


# ─── where a set name comes from ────────────────────────────────────────────
# ANCHORED, not scanned. The weapon resolver scans a whole EditorID for any
# family token because a weapon's name can turn up anywhere in it. Armour is
# more regular than that — the set always sits immediately after `Armor_` or
# `PowerArmor_` — and scanning got it badly wrong: the content prefixes every
# EditorID starts with are perfectly good segment matches, so `XPD_AC_mod_armor_
# Muni_Torso` filed under a family called "Xpd" instead of Civil Engineer, and
# `Storm_Clothes_Recipe_LostMediumHelmet` joined the STORM power armour set.
# Anchoring makes the position part of the rule rather than a tie-break.
#
# DLC prefixes are stepped over because they say which add-on shipped a set,
# not which set it is: Armor_DLC03_Marine_LegLeft is Marine armour.
_ANCHOR    = re.compile(r"(?:^|_)armou?r_", re.I)
_PA_ANCHOR = re.compile(r"(?:^|_)power_?armou?r_", re.I)
# Helmets and hoods are stored as Headwear/Clothes rather than Armor, so the
# Marine Armor Helmet's EditorID never says "armor" at all. A token found this
# way is only ever ACCEPTED if it already names a set known from an Armor_
# record — otherwise every one-off hood would invent a family out of its own
# first word.
_WEAR_ANCHOR = re.compile(r"(?:^|_)(?:headwear|clothes)_", re.I)

# Words that sit where a set name sits but are not one. Two kinds:
#
#   STRUCTURE — the slot, the layer, the kind of mod. Armour EditorIDs put
#   these straight after the anchor when a record belongs to no set.
#
#   CONTENT PREFIXES — the studio's own tag for which release shipped a record,
#   inserted BETWEEN the anchor and the set: Armor_XPD_AC_Muni_ArmRight is the
#   Civil Engineer torso from the Atlantic City expedition. Left unblocked,
#   "XPD" seeded a family of its own and swallowed all 38 Civil Engineer mods
#   and paints under a root expand called "Xpd". They are prefixes, not sets,
#   so the walk below steps over them exactly as it steps over DLC tags.
BLOCK = {
    # structure
    "headwear", "clothes", "armor", "armour", "mod", "recipe", "material",
    "paint", "skin", "all", "misc", "lining", "limb", "limbarm", "limbleg",
    "torso", "helmet", "arm", "armleft", "armright", "leg", "legleft",
    "legright", "underarmor", "underarmour", "light", "sturdy", "heavy",
    # content prefixes
    "xpd", "ac", "w05", "moon", "sdow", "atx", "herd", "post", "prc",
    "general", "do", "cut", "del", "zzz",
}
_DLC = re.compile(r"^dlc\d+$", re.I)


# A `_Set_<X>_` segment names a VARIANT of the set in front of it and is the
# more specific answer: Armor_PowerArmor_Ultracite_Set_V94_Torso is the
# Strangler Heart chest piece, not an Ultracite one. Without this the six
# Strangler Heart pieces sat inside Ultracite and that set showed twelve
# "base" plans for a six-piece armour.
_SET = re.compile(r"_set_([A-Za-z0-9]+)_", re.I)


def _parts(text, anchor):
    """The underscore-separated parts that follow `anchor` in `text`.

    Kept whole rather than segmented, because "AlienInvaderV2" is one token and
    splitting it into three would stop it ever matching its own family.
    """
    m = anchor.search(text or "")
    return text[m.end():].split("_") if m else []


def _norm(token: str) -> str:
    """"SecretService" and "secretservice" are the same set."""
    return "".join(awg.segs(token))


def humanise(token: str) -> str:
    return " ".join(w.capitalize() if w.islower() else w
                    for w in awg.segs(token)).strip()


# Where the game files spell a set differently from the name the game prints,
# keyed by the normalised token. Read off the records and the plan names, not
# guessed. Anything not here is humanised from its own token, which is right
# more often than not ("SecretService" -> "Secret Service").
#
# PER PAGE, because `Raider` is a body armour set AND a power armour set and
# they are not the same thing. One table would have merged them.
ALIASES = {
    "body-armour": {
        "combat": "Combat Armor", "metal": "Metal Armor", "leather": "Leather Armor",
        "robot": "Robot Armor", "marine": "Marine Armor", "trapper": "Trapper Armor",
        "raider": "Raider Armor", "raidermod": "Raider Armor", "wood": "Wood Armor",
        "bosinfantry": "Brotherhood Recon", "muni": "Civil Engineer",
        "secretservice": "Secret Service", "secretservicedrifter": "Secret Service Drifter",
        "chinesestealtharmor": "Chinese Stealth Armor",
        "hazmatsuit": "Hazmat Suit", "vault63hazmatsuit": "Vault 63 Hazmat Suit",
        "botsmith": "Bot Smith", "scout": "Forest Scout Armor",
    },
    "power-armour": {
        "t45": "T-45", "t51": "T-51b", "t60": "T-60", "t65": "T-65", "x01": "X-01",
        "enclavevulcan": "Vulcan", "alieninvaderv2": "Deep-Space Alien",
        "raider": "Raider Power Armor", "storm": "Storm",
        "ultracite": "Ultracite", "hellcat": "Hellcat", "excavator": "Excavator",
        "union": "Union", "v94": "Strangler Heart",
    },
}

# A mod is a "skin" when it changes how the armour looks and nothing else. Same
# two signals the weapon resolver uses: an appearance attach point, or one of
# the cosmetic words in the EditorID.
SKIN_ATTACH = {"ap_armor_Appearance", "ap_PowerArmor_Appearance",
               "ap_armor_Material", "ap_PowerArmor_Material"}
SKIN_WORDS = re.compile(r"(material|paint|skin)", re.I)

# Paints that apply across every power armour set. The OMOD a recipe happens to
# point at is only one set of several, so filing the plan under that set would
# be a guess dressed up as a fact — the same reason the weapon page carves out
# its loadout paints. They get a root expand of their own.
PA_UNIVERSAL = re.compile(r"power_?armou?r_(?:material_)?paint_", re.I)
PA_UNIVERSAL_GROUP = "Power Armour Paints (all sets)"


class ArmourResolver:
    def __init__(self, items):
        self.armo = {}
        self.omod = {}
        self.plan2cobj = {}
        self.families = {"body-armour": {}, "power-armour": {}}
        self.sources = []
        self._load_armo()
        self._load_omod()
        self._load_cobj()
        self._seed(items)

    # -- exports --------------------------------------------------------
    def _load_armo(self):
        # _SLOTS / _ObjectTemplate / _Mods are companion sheets, not the
        # record list, and they carry no ARMO_EDID at all. The PTS export from
        # 2026-09-05 shipped the SLOTS sheet WITHOUT its ARMOUR sheet, so an
        # unfiltered glob picked a file with no records in it and the PTS
        # armour pages grouped against an empty table without erroring.
        path = awg.newest("ARMO_Export_*.tsv",
                          exclude=r"_(SLOTS|ObjectTemplate|Properties|Refs|Mods)\.tsv$")
        self.sources.append(os.path.basename(path))
        for r in awg.read_tsv(path):
            fid = (r.get("ARMO_FormID") or "").strip()
            if fid:
                self.armo[fid] = {"edid": (r.get("ARMO_EDID") or "").strip(),
                                  "full": (r.get("ARMO_FULL") or "").strip()}

    def _load_omod(self):
        # ..._Properties.tsv is a companion sheet, not the record list.
        path = awg.newest("OMOD_Export_*.tsv", exclude=r"_Properties\.tsv$")
        self.sources.append(os.path.basename(path))
        for r in awg.read_tsv(path):
            fid = (r.get("OMOD_FormID") or "").strip()
            if fid:
                self.omod[fid] = {"edid": (r.get("OMOD_EDID") or "").strip(),
                                  "ap": (r.get("AttachPoint_EDID") or "").strip(),
                                  "mnam": (r.get("MNAM_TargetKWDs") or "").strip()}

    def _load_cobj(self):
        path = awg.newest("COBJ_Export_*.tsv")
        self.sources.append(os.path.basename(path))
        for r in awg.read_tsv(path):
            gnam = (r.get("GNAM_EDID") or "").strip()
            if gnam:
                self.plan2cobj.setdefault(gnam, (r.get("COBJ_EDID") or "").strip())

    # -- family table ---------------------------------------------------
    def _add(self, page, token):
        key = _norm(token)
        if not key or key in BLOCK or key.isdigit() or _DLC.match(token or ""):
            return
        self.families[page].setdefault(
            key, ALIASES[page].get(key) or humanise(token))

    @staticmethod
    def _seed_token(text, anchor):
        """The first part after the anchor that is not structure or a prefix."""
        for part in _parts(text, anchor)[:4]:
            key = _norm(part)
            if key and key not in BLOCK and not key.isdigit() and not _DLC.match(part):
                return part
        return None

    def _seed(self, items):
        # 1. The plans that make a physical piece. These are the sets a player
        #    can actually learn, and the seeds worth trusting most.
        for it in items:
            page = it.get("plan_page")
            if page not in self.families or (it.get("cnam") or {}).get("sig") != "ARMO":
                continue
            for src in ((it.get("plan_item") or {}).get("edid", ""),
                        (it.get("cnam") or {}).get("edid", "")):
                if not src or awg.BAD_EDID.search(src):
                    continue
                tok = (self._seed_token(src, _PA_ANCHOR) if page == "power-armour" else None) \
                    or self._seed_token(src, _ANCHOR)
                if tok:
                    self._add(page, tok)
                    break
        # 2. The ARMO export itself, for sets with no learnable piece — X-01 and
        #    Union power armour have mod plans but no plan for the armour, so
        #    without this pass every one of their mods would be a solo group.
        for rec in self.armo.values():
            edid = rec["edid"]
            if not edid or awg.BAD_EDID.search(edid):
                continue
            tok = self._seed_token(edid, _PA_ANCHOR)
            if tok:
                self._add("power-armour", tok)
                continue
            tok = self._seed_token(edid, _ANCHOR)
            if tok:
                self._add("body-armour", tok)
        # 3. Aliases. Each was read off a real record, so an alias also SEEDS
        #    its set — the Deep-Space Alien power armour is paint-only, with no
        #    ARMO record and no physical plan behind it, so nothing else would
        #    ever have created a family for it and its five paints each became a
        #    root expand of their own.
        for page, table in ALIASES.items():
            for key, display in table.items():
                self.families[page][key] = display

    def token(self, page, text):
        """The set this EditorID names, or None.

        ANCHORED, then walked. Anchoring on `Armor_` / `PowerArmor_` is what
        keeps a content prefix at the front of an EditorID from being read as a
        set — scanning the whole string for any known token filed
        `Storm_Clothes_Recipe_LostMediumHelmet` under the STORM power armour.
        Walking forward from the anchor is what gets past the prefixes that sit
        INSIDE it. Over-walking is safe because only a name already in the
        family table is accepted; a slot word cannot match one.
        """
        if not text:
            return None
        fams = self.families[page]
        anchors = (_PA_ANCHOR, _ANCHOR, _WEAR_ANCHOR) if page == "power-armour" \
            else (_ANCHOR, _WEAR_ANCHOR)
        m = _SET.search(text)
        if m and _norm(m.group(1)) in fams:
            return _norm(m.group(1))
        for anchor in anchors:
            for part in _parts(text, anchor):
                key = _norm(part)
                if key in fams:
                    return key
        return None

    # -- per row --------------------------------------------------------
    def classify(self, item):
        page = item.get("plan_page")
        cnam = item.get("cnam") or {}
        plan_edid = (item.get("plan_item") or {}).get("edid", "")
        omod = self.omod.get(cnam.get("formid", "")) if cnam.get("sig") == "OMOD" else None
        cobj_edid = (item.get("cobj") or {}).get("edid", "") or self.plan2cobj.get(plan_edid, "")
        omod_edid = (omod or {}).get("edid", "")
        armo_edid = (self.armo.get(cnam.get("formid", "")) or {}).get("edid", "")
        attach = (omod or {}).get("ap", "")

        if cnam.get("sig") == "ARMO":
            role = "base"
        elif attach in SKIN_ATTACH or SKIN_WORDS.search(plan_edid or "") \
                or SKIN_WORDS.search(omod_edid or "") or SKIN_WORDS.search(cobj_edid or ""):
            role = "skin"
        else:
            role = "mod"

        # The set. The OMOD's own EditorID first, because it is the record that
        # actually belongs to the set; the plan's next; the created record last.
        key = None
        for src in (omod_edid, plan_edid, cobj_edid, armo_edid):
            key = self.token(page, src)
            if key:
                break
        if not key and omod:
            for kw in re.findall(r"\b(ma_[A-Za-z0-9_]+)", omod["mnam"]):
                k = _norm(kw[3:])
                if k in self.families[page]:
                    key = k
                    break
        if key:
            return role, self.families[page][key]

        # No set matched. A power armour paint that names no set is one of the
        # paints that goes on every set — they are a real group, not thirteen
        # groups of one.
        if page == "power-armour" and (PA_UNIVERSAL.search(plan_edid or "")
                                       or PA_UNIVERSAL.search(omod_edid or "")):
            return "skin", PA_UNIVERSAL_GROUP
        return role, None


def attach(items):
    """Write the armour_* fields onto both armour pages' rows."""
    rows = [i for i in items if i.get("plan_page") in PAGES]
    # Prune anything that has since moved off an armour page.
    for i in items:
        if i.get("plan_page") not in PAGES:
            for f in ("armour_role", "armour_group", "armour_group_key", "armour_group_solo"):
                i.pop(f, None)
    if not rows:
        return {}

    res = ArmourResolver(rows)
    tally = collections.Counter()
    for it in rows:
        role, family = res.classify(it)
        if not family:
            # No set behind it — a standalone piece. Its own root expand, named
            # after itself, exactly as a grenade is on the weapon page.
            family = re.sub(r"^Plan:\s*", "", it.get("name") or it.get("id") or "").strip()
            tally["solo"] += 1
        it["armour_role"] = role
        it["armour_group"] = family
        it["armour_group_key"] = awg.slug(family)
        tally[f"{it['plan_page']}:{role}"] += 1

    # A group holding one plan and no mods or skins is flagged so the page can
    # render it without pretending it has sections.
    for page in PAGES:
        mine = [i for i in rows if i["plan_page"] == page]
        sizes = collections.Counter(i["armour_group_key"] for i in mine)
        for it in mine:
            it["armour_group_solo"] = sizes[it["armour_group_key"]] == 1

    groups = {p: len({i["armour_group_key"] for i in rows if i["plan_page"] == p})
              for p in PAGES}
    return {"groups": groups, "tally": dict(tally), "sources": res.sources}


def report(stats, where=""):
    if not stats:
        print(f"[add_armour_groups] {where}no armour rows, nothing to do")
        return
    print(f"[add_armour_groups] {where}"
          + ", ".join(f"{p}={n} groups" for p, n in stats["groups"].items())
          + "  from " + ", ".join(stats["sources"]))
    print("  rows: " + ", ".join(f"{k}={v}" for k, v in sorted(stats["tally"].items())))


def enrich(path, report_only=False):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    stats = attach(doc.get("items") or [])
    if not stats:
        return doc, {}
    doc["armour_groups_schema"] = SCHEMA
    doc["armour_groups_sources"] = stats["sources"]
    if not report_only:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=2)
    return doc, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--tsv-dir", default="")
    args = ap.parse_args()
    if args.tsv_dir:
        awg.set_tsv_dir(args.tsv_dir)
    for path in args.paths:
        if not os.path.exists(path):
            print(f"[add_armour_groups] missing: {path}", file=sys.stderr)
            continue
        doc, stats = enrich(path, args.report_only)
        report(stats, f"{path}: ")
        if not (args.report_only and stats):
            continue
        for page in PAGES:
            mine = [i for i in doc["items"]
                    if i.get("plan_page") == page and not i.get("cut")]
            by = collections.defaultdict(collections.Counter)
            for i in mine:
                by[i["armour_group"]][i["armour_role"]] += 1
            print(f"  == {page} — {len(mine)} rows, {len(by)} groups")
            for name in sorted(by):
                c = by[name]
                print(f"      {name:36s} base={c['base']:3d} mods={c['mod']:3d} skins={c['skin']:3d}")


if __name__ == "__main__":
    main()
