#!/usr/bin/env python3
r"""
uwa_discover.py — which items are unique weapons / armour, read from the game
files every build. No hand list.

Duchess, 11 Oct 2026: "as generative as possible, nothing hard or static
unless we really really have to", with a fallback for uniques Bethesda forgets
to add to its own list.

FOUR SIGNALS, NONE COMPLETE ON ITS OWN (Oct 2026 live export, 107 page items)
------------------------------------------------------------------------------
  official   Bethesda's own rosters: FLST WeaponsUniqueNamedList ("Unique Named
             Weapons for Data Validation") and the leveled lists
             LL_Weapon_Unique_All / LL_Armor_Unique_All. Misses new and
             unreleased items (The Pipe, Zeta's Revenge, Chaos Engine...).
  keyword    KYWD CustomItemName_<Name>. Misses ~50.
  custommod  an OMOD that IS a unique's identity: EditorID mod_custom_* or
             attach point ap_customName ("Unique Effect"), or carrying the
             keyword. Its FULL / EditorID name the unique. Also catches
             legendary-effect and bounty mods, so it never counts alone.
  combo      a custom mod sitting in a WEAP ObjectTemplate combination with
             the SAME name ("The Pipe" on PipeGun) — the item exists in game.

ACCEPTED when:  official
            or  combo
            or  keyword AND a custom mod that sits on a real weapon
A unique's own custom PAINT (mod_custom_*_Paint) links a name to its
weapon but is never enough alone — shop model swaps look the same.
A custom mod on no weapon at all is a record, not an item (Overkill, the
bounty copies) — that replaced the hand "excluded" table.
Anything seen by one weak signal only is returned as `possible` so the build
log can list it for a human, never published.

Names: official WEAP entries use the WEAP FULL; official leveled lists use the
last EditorID segment that is not a weapon / list word
(LL_Weapon_Ranged_MissileLauncher_BunkerBuster -> Bunker Buster).
"""

import re
from collections import defaultdict

# Words that never name a unique: list plumbing and weapon / slot types.
_PLUMBING = re.compile(
    r"^(LL|LLI|LLS|LVLI|Weapon|Weapons|Armor|Armour|Ranged|Melee|Thrown|Rewards?|"
    r"MutatedEvents|Unique|All|Torso|ArmLeft|ArmRight|LegLeft|LegRight|Helmet|"
    r"Urban|Forest|Copy\d*|\d+[A-Za-z]{0,3}|[A-Z]{1,3}\d+[A-Za-z]?|Invention|MQ\d+|"
    r"Pool|Item|Items|SniperRifle|Rifle|Pistol|Shotgun)$", re.I)
_SUFFIX = re.compile(r"\s*(paint|skin|effect|appearance|custom ?name|custom|special ?effect|"
                     r"bounty|copy\s*\d*|name mod)\s*$", re.I)


def camel(s):
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s or "")
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)
    return re.sub(r"\s+", " ", s.replace("_", " ")).strip()


def key(name):
    s = _SUFFIX.sub("", name or "")
    s = re.sub(r"^the\s+", "", s.strip(), flags=re.I)
    s = re.sub(r"\bvault\s*-?\s*63\b", "v63", s, flags=re.I)
    # Possessives are spelled every way in the files (Zeta's / Zetas / Luca's /
    # Luca): drop apostrophes, then a word-final "s", so they all meet.
    s = re.sub(r"['\u2019]", "", s)
    s = re.sub(r"(?<=[a-z])s\b", "", s.lower())
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _name_from_list(edid, weapon_words):
    segs = [x for x in (edid or "").split("_") if x]
    for s in reversed(segs):
        if _PLUMBING.match(s) or s.lower() in weapon_words:
            continue
        return camel(s)
    return ""


def discover(pick, read_tsv, strip_q, norm_formid, omod_by_fid, cn_weapon,
             weap_base, weap_combos, weap_host, weapon_words=()):
    weapon_words = {w.lower() for w in weapon_words}
    c = defaultdict(lambda: {"name": "", "src": set(), "omods": set(), "weap": "", "lists": set(),
                             "on_item": False})

    # Which source names an item best: the in-game FULL of its WEAP, then its
    # custom mod's FULL, then a named combination, then names rebuilt from
    # EditorIDs (keyword / list tails), which lose hyphens and apostrophes.
    PRIORITY = {"official-weap": 0, "custommod-full": 1, "custompaint-full": 2, "combo": 1,
                "keyword": 4, "official": 5, "custommod": 6, "custompaint": 6}

    def add(name, src, omod="", weap="", lst="", on_item=False):
        k = key(name)
        if len(k) < 4:
            return
        e = c[k]
        pr = PRIORITY.get(src, 9)
        clean = re.sub(_SUFFIX, "", name).strip()
        if clean and pr < e.get("_pr", 99):
            e["name"], e["_pr"] = clean, pr
        e["src"].add(src.split("-")[0])
        if omod:
            e["omods"].add(omod)
        if weap and not e["weap"]:
            e["weap"] = weap
        if lst:
            e["lists"].add(lst)
        if on_item:
            e["on_item"] = True

    # official: FLST + LL_*_Unique_All
    flst = pick("FLST_Export_*_Entries.tsv")
    for r in (read_tsv(flst) if flst else []):
        if strip_q(r.get("FLST_EDID", "")) != "WeaponsUniqueNamedList":
            continue
        sig, edid = strip_q(r.get("Entry_Sig", "")), strip_q(r.get("Entry_EDID", ""))
        full, fid = strip_q(r.get("Entry_FULL", "")), norm_formid(r.get("Entry_FormID", ""))
        if sig == "WEAP" and full:
            add(full, "official-weap", weap=fid)
        else:
            add(_name_from_list(edid, weapon_words), "official", lst=edid)
    lv = pick("LVLI_Export_*_LVLI_Entries.tsv")
    for r in (read_tsv(lv) if lv else []):
        if strip_q(r.get("LVLI_EDID", "")) not in ("LL_Weapon_Unique_All", "LL_Armor_Unique_All"):
            continue
        ref = strip_q(r.get("LVLO_Reference", "")).split(":")
        if len(ref) < 3:
            continue
        if ref[2] == "WEAP":
            full = strip_q(weap_base.get(norm_formid(ref[0]), {}).get("WEAP_FULL", ""))
            if full:
                add(full, "official-weap", weap=norm_formid(ref[0]))
        else:
            add(_name_from_list(ref[1], weapon_words), "official", lst=ref[1])

    # keyword
    kw = pick("KYWD_Export_*.tsv", exclude="_Refs")
    for r in (read_tsv(kw) if kw else []):
        ed = strip_q(list(r.values())[1] if len(r) > 1 else "")
        if ed.startswith("CustomItemName_"):
            add(camel(ed[len("CustomItemName_"):]), "keyword")

    # custommod + combo
    for fid, r in omod_by_fid.items():
        ed, full = strip_q(r.get("OMOD_EDID", "")), strip_q(r.get("FULL", ""))
        if re.match(r"(zzz|DEL_|CUT_|TEST|DEBUG)", ed, re.I):
            continue
        ap = strip_q(r.get("AttachPoint_EDID", ""))
        if not (re.search(r"(^|_)mod_custom_", ed, re.I) or ap == "ap_customName" or fid in cn_weapon):
            continue
        # A custom mod that sits on a real WEAP / ARMO (any ObjectTemplate
        # combination) is a mod of an item; one on nothing is just a record.
        # A unique's own paint (appearance slot) is its look: a unique signal.
        hosted = bool(weap_host.get(fid))
        # Only a mod_custom_* paint is a unique's own look. Atom Shop model
        # swaps carry CustomItem_SpeciallyNamed too (The Nighthawk, The Station
        # Pump) — those stay custommod and never count as a unique on their own.
        paint = ("appearance" in ap.lower() or "paint" in ap.lower()) \
            and bool(re.search(r"(^|_)mod_custom_", ed, re.I))
        src = "custompaint" if paint else "custommod"
        if full:
            add(full, src + "-full", omod=fid, on_item=hosted)
        m = re.search(r"mod_custom_(?:[A-Za-z0-9]+_)?([A-Za-z0-9-]+?)"
                      r"(?:_(?:Paint|Appearance|Effect|customName|custom|SpecialEffect|Skin))?$", ed, re.I)
        if m:
            add(camel(m.group(1)), src, omod=fid, on_item=hosted)
        for wf, ci in weap_host.get(fid, []):
            cf = weap_combos[wf][ci]["full"]
            if cf and full and key(cf) == key(full):
                add(cf, "combo", omod=fid, weap=wf)

    accepted, possible = {}, {}
    for k, e in c.items():
        s = e["src"]
        # custompaint: a unique-specific paint (mod_custom_*_Paint) — the
        # unique's own look, never a legendary effect.
        ok = ("official" in s or "combo" in s
              or ("keyword" in s and ("custommod" in s or "custompaint" in s) and e["on_item"]))
        if "custommod" in s and "custompaint" not in s and not (
                "official" in s or "combo" in s or e["on_item"]):
            ok = False
        (accepted if ok else possible)[k] = e
    return accepted, possible


def match(k, keys):
    """Exact key, else a prefix match of 6+ characters either way."""
    if k in keys:
        return k
    for o in keys:
        if len(k) >= 6 and len(o) >= 6 and (o.startswith(k) or k.startswith(o)):
            return o
    return None
