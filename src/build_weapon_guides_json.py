#!/usr/bin/env python3
r"""
build_weapon_guides_json.py - one-weapon guide pages under /bnb/weapons-mods/.

Written 21 Oct 2026 for The Pipe. Each page is one named weapon: its base gun's
stat line and mods (the Chainsaw engine's build_weapon_stats / build_mods), the
named weapon's own template (pre-installed parts, legendary stars), its unique
effect read from the custom OMOD's PERK, the challenge and perk card that effect
depends on, and How to Obtain.

    dist/weapon_guides/<slug>.json      one doc per page
    dist/weapon_guides.json             index (one card per page)

Page:  /bnb/weapons-mods/<slug>-guide/   (renderer: df-bnb-weapon-guide.js)

SEEDS - per page, only what the files cannot tell us:
  * weap           the base WEAP FormID
  * template_kw    the named weapon's instantiation keyword (if_tmp_*)
  * obtain         the way in. Anything traced from the files wins; a by-hand
                   entry is only for routes the SERVER decides (see The Pipe).
  * encounter      the random-encounter quest FormID, when the way in is one.
Everything else - stats, mods, effect, perk ranks, challenge target, the
encounter's corpse / note / spell, the maps - is read from the committed
exports and data/server_listing.tsv on every build.

THE PIPE (Duchess, 21 Oct 2026): sniffing the Unusual Pipe Object random
encounter (W05_RE_ObjectZW04 00564513) has a chance to put The Pipe straight
into your inventory. A full scan of SeventySix.esm (live and PTS, every record,
compressed ones included) finds nothing but the Pipe Gun itself pointing at
if_tmp_ThePipe / mod_Custom_ThePipe, and nothing in the encounter's quest,
activator, spell or corpse pointing at the gun - the award is server-side, so
the page shows the chance as N/A and never invents a number.

MAPS: one copy on the server, never copied per guide. The Unusual Pipe is an
Appalachia-pool Object encounter, so the page links only
guide-images/random-encounters/object/maps/ - never the Skyline Valley or
Burning Springs Object points (render_random_encounter_maps.py writes that layout). Only files on data/server_listing.tsv are linked.

Usage:
  python src/build_weapon_guides_json.py           # live
  python src/build_weapon_guides_json.py --pts     # dist/pts/ from tsv/pts/
"""
import csv, datetime, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import tsv_source
from spawns_configs import chainsaws as weapon_engine

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

URL_BASE = "/bnb/weapons-mods/"
SERVER_IMG = "/wp-content/uploads/"
RE_PAGES = "/df/random-encounters/random-encounter-guides/"

PAGES = [
    {
        "slug": "the-pipe",
        "name": "The Pipe",
        "weap": "00024F55",
        "template_kw": "if_tmp_ThePipe",
        "image": "guide-images/plan-checklist/weapons/the-pipe.avif",
        "blurb": ("The Pipe in Fallout 76: the named pipe pistol with the A True Believer "
                  "effect. How to get it from the Unusual Pipe random encounter, its full "
                  "stat line, every mod it takes and how to max out its damage bonus."),
        "encounter": {"quest": "00564513", "type": "object", "pool": "appalachia"},
        "obtain": [{
            "route": "Random Encounters",
            "drops": "Ready-made weapon",
            "source": "Random Encounter: Unusual Pipe - a chance each time you sniff the pipe",
            "rate": "N/A",
            "rate_note": ("The chance is set on Bethesda's servers. Nothing in the game files "
                          "hands The Pipe out, so there is no percentage to show."),
            "conditions": ["Sniff the pipe yourself - the pipe's script only fires for players",
                           "The Unusual Pipe is one of many Object random encounters, so it "
                           "won't be at every encounter point you visit",
                           "Appalachia encounter points only - Skyline Valley and Burning "
                           "Springs run their own encounter pools, so it never spawns there"],
        }],
    },
]

# ranged stat line - DNAM column -> label (suffix)
RANGED_STATS = [
    ("DNAM_Ammo",               "Ammo",                    None),
    ("DNAM_Capacity",           "Magazine size",           None),
    ("DNAM_ActionPointCost",    "AP cost per shot",        None),
    ("DNAM_AttackDelaySeconds", "Delay between shots",     "s"),
    ("DNAM_MinRange",           "Range (full damage)",     None),
    ("DNAM_MaxRange",           "Range (falloff ends)",    None),
    ("DNAM_DamageOutOfRangeMult", "Damage past max range", "×"),
    ("DNAM_Weight",             "Weight",                  None),
    ("DNAM_Value",              "Value (caps)",            None),
    ("DNAM_Health",             "Durability (health)",     None),
    ("DNAM_Stagger",            "Stagger",                 None),
    ("DNAM_SoundLevel",         "Sound level",             None),
    ("CRDT_CritDamageMult",     "Critical damage",         "×"),
    ("WSAM_SneakAttackMult",    "Sneak attack",            "×"),
]

REF_RE = re.compile(r'([A-Za-z0-9_]+)\s*(?:""?([^"\[]*)""?)?\s*\[(\w{4}):([0-9A-Fa-f]{8})\]')


class Exports:
    def __init__(self, channel):
        self.channel = channel
        self._cache = {}

    def rows(self, pattern, exclude=None):
        key = (pattern, exclude)
        if key not in self._cache:
            path = tsv_source.newest(pattern, channel=self.channel, exclude=exclude, required=False)
            if path is None and self.channel == "pts":
                path = tsv_source.newest(pattern, channel="live", exclude=exclude, required=False)
            rows = []
            if path:
                with open(path, encoding="utf-8", errors="replace", newline="") as f:
                    rows = list(csv.DictReader(f, delimiter="\t"))
            self._cache[key] = rows
        return self._cache[key]

    def find(self, pattern, col, value, exclude=None):
        v = value.upper()
        return [r for r in self.rows(pattern, exclude) if (r.get(col) or "").upper() == v]


def clean(s):
    return re.sub(r"\s+", " ", (s or "").replace("\n", " ")).strip()


def strip_markup(s):
    return clean(re.sub(r"<[^>]+>", " ", s or ""))


def fnum(s):
    try:
        return float(str(s).strip())
    except Exception:
        return None


def fmt(v, suffix=None):
    v = clean(v)
    if not v or v.upper().startswith("NULL"):
        return None
    m = re.match(r'^\S+\s+"(.*)"\s+\[', v)          # Ammo38Caliber ".38 Round" [AMMO:..]
    if m:
        v = m.group(1)
    n = fnum(v)
    if n is not None:
        v = f"{n:g}"
    return f"{v}{suffix}" if suffix else v


COLON_REF_RE = re.compile(r"([0-9A-Fa-f]{8}):([A-Za-z0-9_]+):([A-Z_]{4})")


def colon_refs(cell):
    """'ALCO=00564510:RE_ObjectZW04_PipeSC:SCOL ; ...' -> refs (QUST2 / ACTI exports)."""
    return [{"edid": m.group(2), "name": "", "sig": m.group(3), "form_id": m.group(1).upper()}
            for m in COLON_REF_RE.finditer(cell or "")]


def refs(cell):
    return [{"edid": m.group(1), "name": (m.group(2) or "").strip(), "sig": m.group(3),
             "form_id": m.group(4).upper()} for m in REF_RE.finditer(cell or "")]


# ── named weapon: template, unique effect ────────────────────────────────────
def load_template(ex, weap, kw):
    rows = [r for r in ex.find("WEAP_Export_*_ObjectTemplate.tsv", "WEAP_FormID", weap)
            if kw.lower() in (r.get("OBTS_Keywords") or "").lower()]
    if not rows:
        raise LookupError(f"no {kw} combination on WEAP {weap} in the ObjectTemplate export")
    parts, stars, custom = [], 0, None
    for r in sorted(rows, key=lambda r: int(r.get("IncludeIndex") or 0)):
        ref = (refs(r.get("Include_Mod")) or [{}])[0]
        if not ref:
            continue
        if ref["edid"].lower().startswith("modcol_legendary"):
            stars += 1
        elif ref["edid"].lower().startswith("mod_custom_"):
            custom = ref
        else:
            parts.append(ref)
    return {"combination": clean(rows[0].get("Combination_FULL")),
            "combination_index": rows[0].get("CombinationIndex"),
            "level_min": int(fnum(rows[0].get("OBTS_LevelMin")) or 0),
            "level_max": int(fnum(rows[0].get("OBTS_LevelMax")) or 0),
            "parts": parts, "legendary_stars": stars, "custom_mod": custom}


def condition_text(cond, names):
    """'[tab 0] HasPerk(LicensedPlumber01 [PERK:00338438]) ...' -> reader text."""
    m = re.search(r"HasPerk\(([^ ]+) \[PERK:([0-9A-F]{8})\]", cond or "", re.I)
    if m:
        rank = re.search(r"(\d+)$", m.group(1))
        return {"kind": "perk", "form_id": m.group(2).upper(), "edid": m.group(1),
                "rank": int(rank.group(1)) if rank else None,
                "name": names.get(m.group(2).upper(), m.group(1))}
    m = re.search(r"HasCompletedChallenge\(([^ ]+) \[CHAL:([0-9A-F]{8})\]", cond or "", re.I)
    if m:
        return {"kind": "challenge", "form_id": m.group(2).upper(), "edid": m.group(1),
                "name": names.get(m.group(2).upper(), m.group(1))}
    return {"kind": "other", "text": clean(cond)}


ENTRY_POINT_TEXT = {"Mod Weapon DMG Bonus Mult": "damage"}


def unique_effect(ex, custom):
    omod = (ex.find("OMOD_Export_*.tsv", "OMOD_FormID", custom["form_id"], exclude="_Properties") or [{}])[0]
    props = ex.find("OMOD_Export_*_Properties.tsv", "OMOD_FormID", custom["form_id"])
    perk_ids = [r["form_id"] for p in props for r in refs(p.get("Value1"))
                if r["sig"] == "PERK"]
    perk_ids = list(dict.fromkeys(perk_ids))
    names = {}
    for r in ex.rows("PERK_Export_*.tsv"):
        fid = (r.get("FormID") or r.get("PERK_FormID") or "").upper()
        if fid:
            names.setdefault(fid, clean(r.get("FULL") or r.get("PERK_FULL")))
    for r in ex.rows("CHAL_Export_*.tsv"):
        if r.get("FormID"):
            names.setdefault(r["FormID"].upper(), clean(r.get("FULL")))
    bonuses = []
    for pid in perk_ids:
        for r in ex.find("PERKEP_Export_*.tsv", "PERK_FormID", pid):
            ep, func, amt = clean(r.get("EntryPoint")), clean(r.get("Function")), fnum(r.get("Value"))
            if ep not in ENTRY_POINT_TEXT or amt is None:
                continue
            bonuses.append({"stat": ENTRY_POINT_TEXT[ep], "function": func, "amount": amt,
                            "percent": round(amt * 100, 2) if func == "Add Value" else None,
                            "condition": condition_text(r.get("Conditions"), names)})
    return {"name": clean(omod.get("FULL")) or custom.get("name"),
            "description": clean(omod.get("DESC")),
            "omod": custom, "perk_form_ids": perk_ids, "bonuses": bonuses}


def perk_card(ex, perk_fid):
    """The PCRD whose ranks include this PERK."""
    for r in ex.rows("PCRD_Export_*.tsv"):
        flat = r.get("Ranks_Flat") or ""
        if perk_fid.upper() not in flat.upper():
            continue
        ranks = []
        for n in range(1, int(fnum(r.get("RankCount")) or 0) + 1):
            pfid = (r.get(f"Rank_{n}_MalePerk_FormID") or "").upper()
            prow = (ex.find("PERK_Export_*.tsv", "PERK_FormID", pfid) or [{}])[0]
            ranks.append({"rank": n, "form_id": pfid, "name": clean(prow.get("FULL")),
                          "description": clean(prow.get("DESC"))})
        return {"form_id": (r.get("PCRD_FormID") or "").upper(), "edid": r.get("PCRD_EDID"),
                "name": ranks[0]["name"] if ranks else "",
                "special": clean(r.get("DATA_Special")),
                "min_level": int(fnum(r.get("DATA_MinLevel")) or 0) or None,
                "ranks": ranks, "tagline": clean(r.get("DESC"))}
    return None


def challenge(ex, chal_fid):
    r = (ex.find("CHAL_Export_*.tsv", "FormID", chal_fid) or [None])[0]
    if not r:
        return None
    target = None
    g = refs(r.get("HNAM") or "")
    for ref in g:
        if ref["sig"] == "GLOB":
            grow = (ex.find("GLOB_Export_*.tsv", "FormID", ref["form_id"]) or [{}])[0]
            target = int(fnum(grow.get("FLTV")) or 0) or None
    parent = refs(r.get("ANAM") or "")
    cond = clean(r.get("Cond1"))
    kw = re.search(r"HasKeyword\|(?:[^|]*\|)*?([A-Za-z0-9_]+) \[KYWD", cond)
    # which player weapons carry the keyword the challenge counts (no test / creature /
    # deleted / babylon records - the same filters the chainsaw engine uses, plus cr*/DEL_)
    weapons = []
    if kw:
        for w in ex.rows("WEAP_Export_*_Base.tsv"):
            edid = w.get("WEAP_EDID") or ""
            if kw.group(1) not in (w.get("Keywords") or ""):
                continue
            if weapon_engine.DEV_EDID_RE.search(edid) or re.match(r"^(cr|DEL_|zzz)", edid):
                continue
            name = clean(w.get("WEAP_FULL"))
            if name and name not in weapons and not re.search(r"'s ", name):
                weapons.append(name)
    return {"form_id": chal_fid, "edid": r.get("EDID"), "name": clean(r.get("FULL")),
            "counts_weapons": weapons,
            "type": clean(r.get("CNAM")), "category": clean(r.get("ENAM")),
            "counts": clean(r.get("SNAM")), "target": target,
            "keyword": kw.group(1) if kw else "",
            "parent": parent[0]["name"] if parent else ""}


# ── the encounter ────────────────────────────────────────────────────────────
def encounter(ex, quest_fid, re_type):
    q = (ex.find("QUEST_Export_*.tsv", "FormID", quest_fid) or [{}])[0]
    aliases = ex.find("QUST2_Export_*_ItemAliases.tsv", "QuestFormID", quest_fid)
    out = {"name": clean(q.get("FULL - Name")), "edid": q.get("EDID"), "form_id": quest_fid,
           "type": re_type.title(), "aliases": [], "spell": None, "note": None,
           "player_only": False}
    corpse = activator = None
    for a in aliases:
        linked = colon_refs((a.get("Links") or "").split(" ; ")[0])
        base = next((r for r in linked if r["sig"] in ("SCOL", "NPC_", "ACTI", "MSTT")), None)
        if not base:
            continue
        out["aliases"].append({"alias": a.get("AliasName"), "base": base["edid"], "sig": base["sig"]})
        if base["sig"] == "NPC_":
            corpse = base
        if base["sig"] == "ACTI":
            activator = base
    if activator:
        arow = (ex.find("ACTI_Export_*_ACTI.tsv", "FormID", activator["form_id"])
                or ex.find("ACTI_Export_*_ACTI.tsv", "ACTI_FormID", activator["form_id"]) or [{}])[0]
        scripts = " ".join(v for v in arow.values() if v and "::" in v)
        out["player_only"] = "PlayerOnly=True" in scripts
        spell = re.search(r"TriggerSpell=([0-9A-F]{8}):", scripts)
        if spell:
            effects = []
            for e in ex.find("SPEL_Export_*_EFFECTS.tsv", "SPEL_FormID", spell.group(1)):
                effects.append({"name": clean(e.get("EFID_MGEF_FULL")),
                                "edid": clean(e.get("EFID_MGEF_EDID")),
                                "magnitude": fnum(e.get("EFIT_Magnitude")),
                                "duration": fnum(e.get("EFIT_Duration"))})
            out["spell"] = {"form_id": spell.group(1), "effects": effects}
    if corpse:
        for b in ex.rows("BOOK_Export_*.tsv", exclude="_Locations"):
            if corpse["form_id"] in " ".join(v for v in b.values() if v):
                out["note"] = {"title": clean(b.get("FULL")), "text": strip_markup(b.get("DESC")),
                               "form_id": b.get("FormID")}
                break
    return out


def maps_for(re_type, listing_path, pool="appalachia"):
    """The ONE pool's maps. Skyline Valley and Burning Springs run their own encounter
    pools (render_random_encounter_maps.py), so an Appalachia encounter links only
    the Appalachia maps. Server layout (Duchess, 21 Oct 2026): Appalachia lives in
    the type folder - random-encounters/<type>/maps/ - and each expansion pool has its
    own folder holding every type - random-encounters/<pool>/maps/<type>-<pool>-*."""
    folder = (f"guide-images/random-encounters/{re_type}/maps/" if pool == "appalachia"
              else f"guide-images/random-encounters/{pool}/maps/")
    files = []
    if os.path.exists(listing_path):
        for line in open(listing_path, encoding="utf-8"):
            path = line.split("\t")[0]
            if path.startswith(folder) and "/" not in path[len(folder):] and path.endswith(".jpg"):
                files.append(path)
    stem = f"{re_type}-{pool}"
    full = next((f for f in files if f.endswith(f"/{stem}-full-map.jpg")), None)
    numbered = next((f for f in files if f.endswith(f"/{stem}-numbered-map.jpg")), None)
    regions = sorted(f for f in files if f.endswith("-spawn-map.jpg")) if pool == "appalachia" else []
    pretty = lambda f: os.path.basename(f)[:-len("-spawn-map.jpg")].replace("-", " ").title()
    return {"folder": SERVER_IMG + folder, "pool": pool,
            "full": SERVER_IMG + full if full else None,
            "numbered": SERVER_IMG + numbered if numbered else None,
            "regions": [{"region": pretty(f), "image": SERVER_IMG + f} for f in regions],
            "on_server": bool(full or numbered or regions)}



# ── gun mods: effects through Includes, and how each one is learned ──────────
# A FO76 gun mod usually carries no stats of its own: it INCLUDES shared template
# mods (PARENT_mod_WEAPON_* "TEMPLATE: ...") that hold the numbers - the .45 Receiver
# is three includes and zero properties. The chainsaw engine reads only the mod's own
# rows, so for a gun the includes are walked here (OMOD Includes_Flat, recursive).
GUN_PROPS = [   # PropertyName -> label, kind ('pct' MUL+ADD/ADD multiplier, 'flat', 'set')
    ("AttackDamage", "Damage", "pct"), ("DamageBonusMult", "Damage bonus", "pct"),
    ("DamageTypeValues", None, "dtype"), ("Ammo", "Ammo", "set"),
    ("IsAutomatic", "Full-auto", "flag"), ("AmmoCapacity", "Magazine size", "pct"),
    ("Speed", "Fire rate", "pct"), ("AttackDelaySec", "Delay between shots", "pct"),
    ("AttackActionPointCost", "AP cost", "pct"), ("CriticalDamageMult", "Critical damage", "pct"),
    ("SneakAttackMult", "Sneak attack damage", "pct"), ("SecondaryDamage", "Bash damage", "pct"),
    ("MaxRange", "Range", "pct"), ("AccuracyBonus", "Accuracy", "flat"),
    ("AimModelRecoilMaxDegPerShot", "Recoil", "pct"),
    ("AimModelMinConeDegrees", "Hip-fire spread", "pct"),
    ("AimModelConeIronSightsMultiplier", "Aimed spread", "pct"),
    ("AimModelBaseStability", "Stability", "pct"), ("ReloadSpeed", "Reload speed", "pct"),
    ("SightedTransitionSeconds", "Time to aim", "pct"), ("HasScope", "Adds a scope", "flag"),
    ("Enchantments", "Adds effect", "ench"), ("ActorValues", None, "av"),
    ("Durability", "Durability", "pct"), ("Weight", "Weight", "pct"), ("Value", "Value", "pct"),
]


def gun_mod_effects(ex, fid):
    omods = ex._cache.get("_omod_idx")
    if omods is None:
        omods = {(r.get("OMOD_FormID") or "").upper(): r
                 for r in ex.rows("OMOD_Export_*.tsv", exclude="_Properties")}
        props = {}
        for r in ex.rows("OMOD_Export_*_Properties.tsv"):
            props.setdefault((r.get("OMOD_FormID") or "").upper(), []).append(r)
        ex._cache["_omod_idx"], ex._cache["_omod_props"] = omods, props
    props = ex._cache["_omod_props"]
    rows, seen, stack = [], set(), [fid.upper()]
    while stack:
        f = stack.pop()
        if f in seen:
            continue
        seen.add(f)
        rows += props.get(f, [])
        stack += [m.upper() for m in re.findall(r"\[OMOD:([0-9A-Fa-f]{8})\]",
                                                (omods.get(f) or {}).get("Includes_Flat") or "")]
    agg = {}
    for r in rows:
        name, fn = clean(r.get("PropertyName")), clean(r.get("FunctionType")).upper()
        key = (name, fn, refs(r.get("Value1"))[0]["form_id"] if name in ("DamageTypeValues", "ActorValues", "Enchantments", "Ammo") and refs(r.get("Value1")) else "")
        a = agg.setdefault(key, {"row": r, "v1": 0.0, "v2": 0.0})
        a["v1"] += fnum(r.get("Value1")) or 0.0
        a["v2"] += fnum(r.get("Value2")) or 0.0
    out = []
    for prop, label, kind in GUN_PROPS:
        for (name, fn, ref_id), a in agg.items():
            if name != prop:
                continue
            r = a["row"]
            ref = (refs(r.get("Value1")) or [{"name": "", "edid": ""}])[0]
            if kind == "pct":
                v = a["v1"]
                if "MUL" not in fn and "ADD" in fn and prop not in ("DamageBonusMult", "CriticalDamageMult"):
                    continue
                if v:
                    out.append({"label": label, "value": f"{v * 100:+.0f}%".replace("+-", "-")})
            elif kind == "flat" and a["v1"]:
                out.append({"label": label, "value": f"{a['v1']:+g}"})
            elif kind == "set" and ref.get("name"):
                out.append({"label": label, "value": ref["name"]})
            elif kind == "flag" and clean(r.get("Value1")).lower() in ("1", "true"):
                out.append({"label": label, "value": "Yes"})
            elif kind == "ench":
                nm = ref.get("name") or weapon_engine._prettify(ref.get("edid"))
                out.append({"label": label, "value": re.sub(r"^Ench\s+", "", nm)})
            elif kind in ("dtype", "av") and a["v2"]:
                nm = ref.get("name") or weapon_engine._prettify(ref.get("edid"))
                lab = (nm if nm.lower().endswith("damage") else f"{nm} damage") if kind == "dtype" else nm
                val = f"{a['v2']:+g}" if kind == "av" or "MUL" not in fn else f"{a['v2'] * 100:+.0f}%"
                out.append({"label": lab, "value": val.replace("+-", "-")})
    # an enchantment that only restates a stat row (Armor Penetration, Ammo Capacity)
    labels = {e["label"].lower() for e in out}
    if "magazine size" in labels:
        labels.add("ammo capacity")
    out = [e for e in out if not (e["label"] == "Adds effect" and e["value"].lower() in labels)]
    seen_l, dedup = set(), []
    for e in out:
        if e["label"] not in seen_l:
            seen_l.add(e["label"]); dedup.append(e)
    return dedup


def learn_index(ex):
    """mod FormID -> how it is learned (COBJ2: LRNM + LRNC chance GLOB)."""
    glob = {(r.get("FormID") or "").upper(): fnum(r.get("FLTV")) for r in ex.rows("GLOB_Export_*.tsv")}
    out = {}
    for r in ex.rows("COBJ2_Export_*.tsv"):
        cn = (r.get("CNAM_FormID") or "").upper()
        if not cn or weapon_engine.DEV_EDID_RE.search(r.get("COBJ_EDID") or ""):
            continue
        g = re.match(r"([0-9A-Fa-f]{8}):", r.get("LRNC_LearnChance") or "")
        out.setdefault(cn, {"method": clean(r.get("LRNM_LearnMethod")),
                            "chance": glob.get(g.group(1).upper()) if g else None})
    return out

# ── a page ───────────────────────────────────────────────────────────────────
def build_page(cfg, ex, generated, channel):
    tsv_source_channel = channel
    base, dnam = weapon_engine.load_weapon(cfg["weap"])
    ws = weapon_engine.build_weapon_stats(base, dnam)
    ws["stats"] = [{"label": lab, "value": v} for col, lab, suf in RANGED_STATS
                   for v in [fmt(dnam.get(col), suf)] if v]
    ws["mod_slots"] = [s for s in ws["mod_slots"] if not s["edid"].lower().startswith("ap_legendary")]

    tmpl = load_template(ex, cfg["weap"], cfg["template_kw"])
    pre = {p["form_id"] for p in tmpl["parts"]}

    mods = weapon_engine.build_mods(base, cfg["weap"])
    learned = learn_index(ex)
    for g in mods["groups"]:
        for m in g["mods"]:
            m["effects"] = gun_mod_effects(ex, m["form_id"])
            ln = learned.get(m["form_id"]) or {}
            meth = ln.get("method", "")
            if m["plan"]:
                m["learn"] = {"how": "plan", "text": m["plan"]}
            elif meth == "Learned by scrapping":
                ch = ln.get("chance")
                m["learn"] = {"how": "scrap", "chance": ch,
                              "text": f"Scrap a {ws['name'] or 'weapon'} to learn it"
                                      + (f" - {ch:g}% chance per scrap" if ch else "")}
            elif meth.startswith("Known by default"):
                m["learn"] = {"how": "default", "text": "Known by default"}
            elif meth == "Learned when picked up or by script":
                m["learn"] = {"how": "pickup", "text": "Learned when you pick the mod up (or from a script)"}
            elif meth:
                m["learn"] = {"how": "other", "text": meth}
            else:
                m["learn"] = {"how": "none", "text": "No crafting recipe - comes on the weapon only"}
    for g in mods["groups"]:
        if g["slot"].lower() == "no muzzle":
            g["slot"] = "Muzzle"
        for m in g["mods"]:
            if m["slot"].lower() == "no muzzle":
                m["slot"] = "Muzzle"
            m["preinstalled"] = m["form_id"] in pre
    mods["note"] = (f"Every mod the base {ws['name'] or 'weapon'} takes - {cfg['name']} takes the "
                    "same ones. Mods marked 'Comes on " + cfg["name"] + "' are the parts it drops with. "
                    "Each mod's numbers include the shared template mods it pulls in, and a "
                    "scrap chance is the chance to learn that mod each time you scrap one. "
                    "Legendary effects are rolled separately and are not listed here.")

    eff = unique_effect(ex, tmpl["custom_mod"]) if tmpl["custom_mod"] else None
    card = chal = None
    if eff:
        perk_b = next((b for b in eff["bonuses"] if b["condition"]["kind"] == "perk"), None)
        chal_b = next((b for b in eff["bonuses"] if b["condition"]["kind"] == "challenge"), None)
        if perk_b:
            card = perk_card(ex, perk_b["condition"]["form_id"])
        if chal_b:
            chal = challenge(ex, chal_b["condition"]["form_id"])
        perk_total = sum(b["percent"] or 0 for b in eff["bonuses"] if b["condition"]["kind"] == "perk")
        chal_total = sum(b["percent"] or 0 for b in eff["bonuses"] if b["condition"]["kind"] == "challenge")
        eff["max_total_percent"] = round(perk_total + chal_total, 2)
        eff["perk_total_percent"] = round(perk_total, 2)
        eff["challenge_total_percent"] = round(chal_total, 2)

    enc = maps = None
    if cfg.get("encounter"):
        enc = encounter(ex, cfg["encounter"]["quest"], cfg["encounter"]["type"])
        enc["guide_url"] = f"{RE_PAGES}{cfg['encounter']['type']}-random-encounters/"
        maps = maps_for(cfg["encounter"]["type"], os.path.join(REPO, "data", "server_listing.tsv"),
                        cfg["encounter"].get("pool", "appalachia"))

    doc = {
        "_meta": {"generated": generated, "channel": channel,
                  "source": ("Game-file exports (WEAP/OMOD/PERK/PCRD/CHAL/GLOB/QUST/ACTI/SPEL/BOOK) "
                             "+ data/server_listing.tsv"),
                  "exports": tsv_source.observed() if hasattr(tsv_source, "observed") else ""},
        "slug": cfg["slug"], "name": cfg["name"],
        "page_title": f"Weapons - {cfg['name']} Guide",
        "url": f"{URL_BASE}{cfg['slug']}-guide/",
        "blurb": cfg["blurb"],
        "image": SERVER_IMG + cfg["image"] if cfg.get("image") else "",
        "overview": {
            "base_weapon": ws["name"], "base_form_id": ws["form_id"], "base_edid": ws["edid"],
            "template": tmpl["combination"], "template_keyword": cfg["template_kw"],
            "legendary_stars": tmpl["legendary_stars"],
            "legendary_note": (f"Drops with {tmpl['legendary_stars']} legendary stars, each one "
                               "rolled at random." if tmpl["legendary_stars"] else ""),
            "level_requirement": ws["level_requirement"],
            "custom_mod": tmpl["custom_mod"],
        },
        "obtain": cfg["obtain"],
        "unique_effect": eff,
        "perk_card": card,
        "challenge": chal,
        "encounter": enc,
        "maps": maps,
        "weapon_stats": ws,
        "preinstalled": tmpl["parts"],
        "mods": mods,
    }
    return doc


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    channel = "pts" if "--pts" in argv else "live"
    dist = os.path.join(REPO, "dist", "pts") if channel == "pts" else os.path.join(REPO, "dist")
    out_dir = os.path.join(dist, "weapon_guides")
    os.makedirs(out_dir, exist_ok=True)
    ex = Exports(channel)
    generated = datetime.date.today().isoformat()
    index = []
    for cfg in PAGES:
        doc = build_page(cfg, ex, generated, channel)
        path = os.path.join(out_dir, f"{cfg['slug']}.json")
        json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        e = doc["unique_effect"] or {}
        print(f"  {cfg['slug']:<12} mods:{doc['mods']['count']:>3}  stars:{doc['overview']['legendary_stars']}"
              f"  effect:{e.get('name')} max +{e.get('max_total_percent')}%"
              f"  maps on server:{(doc['maps'] or {}).get('on_server')}")
        index.append({"slug": cfg["slug"], "name": cfg["name"], "url": doc["url"],
                      "blurb": cfg["blurb"], "image": doc["image"]})
    json.dump({"_meta": {"generated": generated, "channel": channel}, "pages": index},
              open(os.path.join(dist, "weapon_guides.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(f"[weapon_guides] wrote {len(index)} page(s) to {out_dir}")


if __name__ == "__main__":
    main()
