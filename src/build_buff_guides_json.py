#!/usr/bin/env python3
"""
build_buff_guides_json.py - data for the seven BnB Buffs guide pages (one page
each, three parts: 1. find the right one for your build, 2. compare up to 3,
3. every item with its buffs, formulas, conditions and records).

    /bnb/buffs/alcohol/alcohol-buffs/                         alcohol
    /bnb/buffs/chems/chem-buffs/                              chems
    /bnb/buffs/food/food-buffs/                               food
    /bnb/buffs/bobbleheads/bobblehead-buffs/                  bobbleheads
    /bnb/buffs/magazines/magazine-buffs/                      magazines
    /bnb/buffs/mutations/mutation-buffs/                      mutations
    /bnb/buffs/nuka-cola-products/nuka-cola-product-buffs/    nuka-cola
    renderer: df-bnb-buffs.js / .css (theme assets), "guide" mode

OUTPUT
  dist/buff_guides/<page>.json   one per page: the page's words + every item
  dist/buff_guides/shared.json   the finder rules (the Recommended Food Buff
                                 Calculator's rules.json + goals), mechanics,
                                 player weapons and what each damage buff
                                 reaches, SPECIAL descriptions

EVERY NUMBER COMES FROM THE GAME FILES. The only hand-kept file is
data/buff_guides/pages.json: titles, blurbs, the basics lines, which guide
links to show and the "Good to know" notes - words only, numbers come in as
tokens ({dur}, {perkpct:EDID}, ...) filled from the exports at build time.

HOW THE PIECES ARE WORKED OUT
  * Buffs (fx): the Recommended Food Buff Calculator's items (same slots,
    magnitudes, durations, diet and Party Girl multipliers) - run
    build_food_buff_calculator_json.py first. Mutations are read here from the
    SPEL export (normal + Strange In Numbers "super" versions, negatives).
  * Every effect of an item: dist/buffs/<page>.json (build_buffs_json.py) or,
    for items that page doesn't list, the same builder's build_alch_item().
  * Which weapons a damage buff reaches: the hidden perk STAT_DamagePerk
    applies every weapon-damage actor value with a weapon keyword condition
    (Mod Weapon DMG Bonus Mult, Add Actor Value Mult). Each player weapon's
    keywords are run through those conditions (and the CNDF they call).
  * When a buff works: BUFFCOND (item + effect conditions, read from
    SeventySix.esm by tools/esm_buff_guides_extract.py) and the perk entry
    point conditions (PERKEP), turned into plain words.
  * Pickup messages: MESGTXT (Bobblehead_<Type>_Msg, Magazine_<Series><NN>_Msg).
  * Bobblehead: Caps: the caps stash tin's leveled list swaps to a better one
    when you have Bobblehead_CapsPerk; both are First Match lists, so the
    rates come from rng76.first_match_rates() and the caps inside from the
    stash containers' leveled lists (CONTITEMS + LVLI).

INPUTS (tsv/, newest export of each)
  ALCH_Export_*_Effects  MGEF_Export  MGEFKW_Export  PERKEP_Export  PERK_Export
  BUFFCOND_Export  MESGTXT_Export  CONTITEMS_Export  CNDF_Export  AVIF_Export
  WEAP_Export_*_Base  SPEL_Export_*_EFFECTS  LVLI_Export_*_LVLI_Entries/_List
  guide_index.tsv
  dist/calculators/food_buff_calculator.json  dist/buffs/<page>.json
  dist/specials_stats.json (goals)            data/buff_guides/pages.json

Usage:
  python src/build_buff_guides_json.py
  python src/build_buff_guides_json.py --data-dir tsv --outdir dist
"""
from __future__ import annotations

import argparse
import csv
import datetime
import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

try:
    import tsv_source                                         # noqa: E402
    _key = tsv_source.export_key
except Exception:                                             # pragma: no cover
    def _key(p):
        return (os.path.getmtime(p),)

WORDS_PATH = os.path.join(REPO, "data", "buff_guides", "pages.json")
SITE = "https://theduchessflame.com"


def warn(msg):
    print(f"[buff_guides] WARNING: {msg}", file=sys.stderr)


def newest(data_dir, pattern, exclude=None):
    c = [p for p in glob.glob(os.path.join(data_dir, pattern)) if not (exclude and exclude in os.path.basename(p))]
    return max(c, key=_key) if c else None


def read_tsv(path, raw=False):
    """raw: no csv quote handling (text that starts with a quote mark, like a pickup message)"""
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8-sig", newline="") as f:
        if raw:
            return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))
        return list(csv.DictReader(f, delimiter="\t"))


def need(data_dir, pattern, exclude=None):
    p = newest(data_dir, pattern, exclude)
    if not p:
        raise SystemExit(f"[buff_guides] FATAL: no {pattern} in {data_dir}")
    return p


def fnum(v, d=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def num(v):
    v = float(v)
    r = round(v, 3)
    return str(int(r)) if r == int(r) else ("%g" % r)


def minutes(sec):
    m = int(round(sec / 60.0))
    if m >= 60:
        h, r = divmod(m, 60)
        return f"{h} hour{'s' if h != 1 else ''}" + (f" {r} min" if r else "")
    return f"{m} min"


def edid_of(ref):
    """'Name "Full" [SIG:0001]' -> 'Name'"""
    return re.split(r"[\s\[]", (ref or "").strip(), 1)[0]


def full_of(ref):
    m = re.search(r'"([^"]+)"', ref or "")
    return m.group(1) if m else ""


def camel(s):
    s = re.sub(r"^(ActorType|WeaponType|ObjectType|IngredientType|MealType|SURV_EffectType|ma_|STAT_|Perks_Weap_|NewWeather)", "", s or "")
    s = s.replace("_", " ")
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s).strip()


def article(w):
    return "an" if (w or "x")[0].lower() in "aeiou" else "a"


# ---------------------------------------------------------------------------
# Conditions -> plain words
# ---------------------------------------------------------------------------

OPS = {"100": "=", "001": "<", "101": "<=", "010": ">", "110": ">=", "000": "!="}


def parse_pas(c):
    """CNDF / BUFFCOND condition 'Type|Value|Function|unused|..|Param1|Param2|..|..|RunOn|Ref'."""
    p = (c or "").split("|")
    if len(p) < 6:
        return None
    t = p[0]
    return {"op": OPS.get(t[:3], "="), "or": len(t) > 3 and t[3] == "1", "val": fnum(p[1], 0.0),
            "fn": p[2], "p1": p[5] if len(p) > 5 else "", "p2": p[6] if len(p) > 6 else "",
            "on": p[9] if len(p) > 9 else "Subject", "raw": c}


PERK_COND_RE = re.compile(r"\[tab (\d+)\] (\w+)\((.*?)\)\s+([01]+)\s+([-\d.]+)")


def parse_perk_conds(s):
    out = []
    for c in (s or "").split(" | "):
        m = PERK_COND_RE.match(c.strip())
        if not m:
            continue
        typ = m.group(4)
        out.append({"tab": int(m.group(1)), "fn": m.group(2), "p1": m.group(3), "op": OPS.get(typ[:3], "="),
                    "or": len(typ) > 3 and typ[3] == "1", "val": float(m.group(5)), "raw": c.strip()})
    return out


def parse_spel_cond(c):
    """SPEL effects 'Subject.Func(P1, P2) = 1.000000 AND'"""
    m = re.match(r"(\w+)\.(\w+)\((.*)\)\s*(==|!=|>=|<=|=|<|>)\s*([-\d.]+)\s*(AND|OR)?", (c or "").strip())
    if not m:
        return None
    args = m.group(3)
    p1 = args.rsplit(", ", 1)[0] if ", " in args else args
    op = {"==": "="}.get(m.group(4), m.group(4))
    return {"on": m.group(1), "fn": m.group(2), "p1": p1, "op": op, "val": float(m.group(5)),
            "or": m.group(6) == "OR", "raw": c.strip()}


def is_true(c):
    """condition reads as 'yes' (== 1 / != 0 / > 0)"""
    v, op = c["val"], c["op"]
    return (op == "=" and v != 0) or (op == "!=" and v == 0) or (op in (">", ">=") and v > 0)


TABS = {
    "Mod Weapon DMG Bonus Mult": ["you", "your weapon", "your target"],
    "Mod Weapon Attack Damage": ["you", "your weapon", "your target"],
    "Mod Incoming Weapon Damage": ["you", "the attacker", "the attacker's weapon"],
    "Mod Incoming Explosion Damage": ["you", "the attacker", "the explosion"],
    "Mod Incoming Limb Damage": ["you", "the attacker", "the attacker's weapon"],
    "Mod Spell Magnitude": ["you", "the item you use", "the target", "the effect"],
    "Mod Ingestible Magnitude": ["you", "the item you use", "the effect"],
    "Mod Item Weight": ["you", "the item"],
    "Mod Item Condition Loss": ["you", "the item"],
    "Mod Ammo Health Mult": ["you", "the ammo"],
}


def thing(p1):
    """the readable name of a condition's parameter"""
    return full_of(p1) or camel(edid_of(p1))


def cond_words(c, who="you", cndf=None):
    """one condition -> a short plain sentence, or None when it isn't worth saying"""
    fn, yes, p = c["fn"], is_true(c), c.get("p1", "")
    name = thing(p)
    You = who[0].upper() + who[1:]
    are = "are" if who == "you" else "is"
    if fn in ("IsMemberOfAPlayerTeam", "IsPlayerTeammate"):
        return "You are in a team" if yes else "You are not in a team (solo)"
    if fn == "IsSneaking":
        return "You are sneaking" if yes else "You are not sneaking"
    if fn in ("IsSwimming", "IsInWater"):
        return "You are swimming" if yes else "You are not swimming"
    if fn == "IsUnderWater":
        return "You are under water" if yes else "You are not under water"
    if fn == "HasMagicEffectKeyword":
        kw = edid_of(p)
        if kw == "AlcoholEffect":
            return "You have a drink active (any beer, spirit or wine)" if yes else "You don't have a drink active"
        return f"You have {article(camel(kw))} {camel(kw).lower()} effect on you" if yes else f"You don't have {article(camel(kw))} {camel(kw).lower()} effect on you"
    if fn == "GetGlobalValue" and "GameHour" in p:
        return None                                   # night: collapsed below
    if fn == "IsInWeather":
        return f"The weather is {name.lower()}" if yes else f"The weather is not {name.lower()}"
    if fn == "HasPerk":
        return f"You have the {name} perk" if yes else f"You don't have the {name} perk"
    if fn == "HasSpell":
        return f"You have {name}" if yes else f"You don't have {name}"
    if fn in ("IsSpellTarget", "HasMagicEffect", "HasActiveMagicEffect"):
        return f"{name} is active on you" if yes else f"{name} is not active on you"
    if fn == "GetIsPlayerGhoul":
        return "You are a ghoul" if yes else "You are not a ghoul"
    if fn == "GetIsPlayer":
        return None if yes else "Not on players"
    if fn in ("HasKeyword", "WornHasKeyword"):
        kw = camel(edid_of(p))
        if fn == "WornHasKeyword":
            return f"You have {article(kw)} {kw} equipped" if yes else f"You don't have {article(kw)} {kw} equipped"
        return f"{You} {are} {kw}" if yes else f"{You} {are} not {kw}"
    if fn == "GetRandomPercent":
        return f"{num(c['val'])}% chance each time"
    if fn == "IsTrueForConditionForm" and cndf is not None:
        sub = [cond_words(x, who, None) for x in cndf.get(edid_of(p), [])]
        sub = [s for s in sub if s]
        return "; ".join(sub) if sub and yes else None
    if fn == "EPIsCalculatingBaseDamage":
        return None
    if fn == "GetInIronSights":
        return "You are aiming down the sights" if yes else "You are not aiming down the sights"
    if fn == "GetHealthPercentage":
        return f"Your health is {c['op']} {num(c['val'] * 100)}%"
    return None


def words_for(conds, who_by_tab=None, cndf=None):
    """a list of conditions -> plain lines ('..., or' joins OR groups); night collapsed"""
    out, raw_night = [], False
    for c in conds:
        if c["fn"] == "GetGlobalValue" and "GameHour" in c.get("p1", ""):
            raw_night = True
            continue
        who = (who_by_tab or ["you"])[c.get("tab", 0)] if who_by_tab and c.get("tab", 0) < len(who_by_tab) else "you"
        w = cond_words(c, who, cndf)
        if w and w not in out:
            out.append(w + (" OR" if c["or"] else ""))
    if raw_night:
        out.insert(0, "At night (9pm to 6am game time)")
    if out:
        out[-1] = re.sub(r" OR$", "", out[-1])
    return out


def eval_conds(conds, test):
    """AND of OR-groups. test(cond) -> True / False / None (None = can't tell, counts as passing)."""
    groups, cur = [], []
    for c in conds:
        cur.append(c)
        if not c["or"]:
            groups.append(cur)
            cur = []
    if cur:
        groups.append(cur)
    for g in groups:
        ok = False
        for c in g:
            r = test(c)
            if r is None or r:
                ok = True
                break
        if not ok:
            return False
    return True


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

class Src:
    def __init__(self, data_dir, dist_dir):
        self.data_dir, self.dist_dir = data_dir, dist_dir
        self.paths = {}

        def p(key, pattern, exclude=None, required=True):
            path = need(data_dir, pattern, exclude) if required else newest(data_dir, pattern, exclude)
            self.paths[key] = os.path.basename(path) if path else None
            return path

        self.calc = self._json(os.path.join(dist_dir, "calculators", "food_buff_calculator.json"), True)
        self.specials = self._json(os.path.join(dist_dir, "specials_stats.json")) or {}
        with open(WORDS_PATH, encoding="utf-8") as f:
            self.words = json.load(f)

        self.alch_eff = {}
        for r in read_tsv(p("alch_eff", "ALCH_Export_*_Effects.tsv")):
            self.alch_eff.setdefault(r["ALCH_FormID"].upper(), []).append(r)
        self.mgef = {r["MGEF_FormID"].upper(): r for r in read_tsv(p("mgef", "MGEF_Export_*.tsv"))}
        self.mgef_by_edid = {r["EDID"]: r for r in self.mgef.values()}
        self.mgefkw, self.mgef_detr = {}, {}
        for r in read_tsv(p("mgefkw", "MGEFKW_Export_*.tsv")):
            fid = r["MGEF_FormID"].upper()
            self.mgefkw[fid] = re.findall(r"([A-Za-z_][\w]*)\s*(?:\"[^\"]*\"\s*)?\[", r.get("Keywords_Flat") or "")
            self.mgef_detr[fid] = str(r.get("Detrimental", "0")).strip() == "1"
        self.perkep = {}
        for r in read_tsv(p("perkep", "PERKEP_Export_*.tsv")):
            self.perkep.setdefault(r["PERK_EDID"], []).append(r)
        self.perk_full = {}
        for r in read_tsv(p("perk", "PERK_Export_*.tsv", "PERKEP", required=False)):
            self.perk_full.setdefault(r["PERK_EDID"], r.get("FULL") or "")
        self.buffcond_alch, self.buffcond_mgef = {}, {}
        for r in read_tsv(p("buffcond", "BUFFCOND_Export_*.tsv")):
            conds = [r.get(f"Cond{i}", "") for i in range(1, int(r.get("CondCount") or 0) + 1)]
            if r["Scope"] == "ALCH":
                self.buffcond_alch[(r["FormID"].upper(), int(r["EffectIndex"] or 0))] = conds
            else:
                self.buffcond_mgef[r["FormID"].upper()] = conds
        self.mesg = {r["MESG_EDID"]: r for r in read_tsv(p("mesg", "MESGTXT_Export_*.tsv"), raw=True)}
        self.cont = {}
        for r in read_tsv(p("cont", "CONTITEMS_Export_*.tsv")):
            self.cont.setdefault(r["CONT_EDID"], []).append(r)
        self.cndf_raw, self.cndf = {}, {}
        for r in read_tsv(p("cndf", "CNDF_Export_*.tsv")):
            cs = [r.get(f"Cond{i}", "") for i in range(1, int(r.get("CondCount") or 0) + 1)]
            self.cndf_raw[r["EDID"]] = cs
            self.cndf[r["EDID"]] = [x for x in (parse_pas(c) for c in cs) if x]
        self.avif = {}
        for r in read_tsv(p("avif", "AVIF_Export_*.tsv")):
            self.avif[r["EDID"]] = {"full": r.get("FULL") or "", "desc": r.get("DESC") or ""}
        self.weap = read_tsv(p("weap", "WEAP_Export_*_Base.tsv"))
        self.spel_eff = {}
        for r in read_tsv(p("spel_eff", "SPEL_Export_*_EFFECTS.tsv")):
            self.spel_eff.setdefault(r["SPEL_EDID"], []).append(r)
        self.lvli_entries, self.lvli_list = {}, {}
        for r in read_tsv(p("lvli_e", "LVLI_Export_*_LVLI_Entries.tsv")):
            self.lvli_entries.setdefault(r["LVLI_EDID"], []).append(r)
        for r in read_tsv(p("lvli_l", "LVLI_Export_*_LVLI_List.tsv")):
            self.lvli_list[r["LVLI_EDID"]] = r
        gi = os.path.join(data_dir, "guide_index.tsv")
        self.guides = {r["id"]: r for r in read_tsv(gi)}
        self.buff_pages = {}
        for k in ("alcohol", "chems", "food", "nuka-cola", "bobbleheads", "magazines", "mutations"):
            self.buff_pages[k] = self._json(os.path.join(dist_dir, "buffs", f"{k}.json"), True)
        self._B = None

    @staticmethod
    def _json(path, required=False):
        if not os.path.exists(path):
            if required:
                raise SystemExit(f"[buff_guides] FATAL: {path} missing - build it first")
            return None
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def B(self):
        """build_buffs_json's Sources, only loaded when a page needs an item its JSON doesn't list"""
        if self._B is None:
            import build_buffs_json as B          # noqa: E402
            self._B = (B, B.Sources(self.data_dir, self.dist_dir))
        return self._B


# ---------------------------------------------------------------------------
# Effects: how each one works (technical) + when it works (plain words)
# ---------------------------------------------------------------------------

def effect_detail(S, mg, mag):
    """kind (Add / Multiply / Chance / Subtract / Text only / Script), a plain 'how' and the entry points"""
    fid = mg["MGEF_FormID"].upper()
    arch, av = mg.get("Archetype") or "", mg.get("ActorValue") or ""
    av = edid_of(av)
    perk = edid_of(mg.get("PerkToApply") or "")
    if S.mgef_detr.get(fid) and av and arch in ("Value Modifier", "Peak Value Modifier"):
        return {"kind": "Subtract", "ep": [{"rec": f"MGEF {arch} (detrimental)", "ep": av, "fn": "Subtract", "val": num(mag)}]}
    if arch in ("Peak Value Modifier", "Value Modifier") and av:
        return {"kind": "Add", "ep": [{"rec": f"MGEF {arch}", "ep": av, "fn": "Add", "val": num(mag)}]}
    rows, kinds = [], set()
    for e in S.perkep.get(perk, []):
        ep, fn = e.get("EntryPoint") or "", e.get("Function") or ""
        if not ep:
            continue
        conds = parse_perk_conds(e.get("Conditions"))
        if fn.startswith("Add"):
            k = "Add"
        elif any(c["fn"] == "GetRandomPercent" for c in conds):
            k = "Chance"
        elif fn.startswith("Multiply"):
            k = "Multiply"
        elif fn.startswith("Set"):
            k = "Set"
        else:
            k = fn
        kinds.add(k)
        rows.append({"rec": f"PERK {perk} [{e.get('PERK_FormID')}]", "ep": ep, "fn": fn, "val": num(fnum(e.get("Value"), 0)),
                     "conds": words_for(conds, TABS.get(ep, ["you", "the item / weapon", "the target", "the effect"]), S.cndf),
                     "raw": e.get("Conditions") or ""})
    if rows:
        return {"kind": " + ".join(sorted(kinds)), "ep": rows}
    if "UIDummy" in mg["EDID"] or "TextDummy" in mg["EDID"]:
        return {"kind": "Text only", "ep": []}
    return {"kind": "Script", "ep": []}


def tech_effect(S, mgef_fid, mag, dur, mag_glob, dur_glob, raw_conds, spel_conds=None):
    mg = S.mgef.get(mgef_fid.upper())
    if not mg:
        return None
    d = effect_detail(S, mg, mag)
    pc = [x for x in (parse_pas(c) for c in raw_conds) if x]
    words = words_for(pc, None, S.cndf) + (spel_conds or [])
    return {
        "mgef": mg["EDID"], "mgefId": mgef_fid.upper(), "full": mg.get("FULL") or "", "dnam": mg.get("DNAM_MagicItemDescription") or "",
        "mag": mag, "dur": int(dur or 0), "magGlob": mag_glob or "", "durGlob": dur_glob or "",
        "arch": mg.get("Archetype") or "", "av": edid_of(mg.get("ActorValue") or ""), "perk": edid_of(mg.get("PerkToApply") or ""),
        "kw": ", ".join(S.mgefkw.get(mgef_fid.upper(), [])), "condWords": words, "condRaw": raw_conds,
        **d,
    }


def alch_effects(S, fid):
    out = []
    for r in S.alch_eff.get(fid.upper(), []):
        idx = int(r.get("EffectIndex") or 0)
        mfid = r["MGEF_FormID"].upper()
        raw = (S.buffcond_mgef.get(mfid) or []) + (S.buffcond_alch.get((fid.upper(), idx)) or [])
        e = tech_effect(S, mfid, fnum(r.get("EFIT_Magnitude"), 0.0), fnum(r.get("EFIT_Duration"), 0.0),
                        r.get("MAGG_GLOB_EDID"), r.get("DURG_GLOB_EDID"), raw)
        if e:
            out.append(e)
    return out


WHEN_FN = {"IsMemberOfAPlayerTeam": "team", "IsSneaking": "sneak", "IsSwimming": "swim", "HasMagicEffectKeyword": "alcohol"}


def when_from_effect(e):
    """a buff that only works sometimes -> the calculator's 'w' code (team / night / sneak / swim / alcohol / weather)"""
    pc = [x for x in (parse_pas(c) for c in e["condRaw"]) if x]
    wx = [thing(c["p1"]).lower() for c in pc if c["fn"] == "IsInWeather" and is_true(c)]
    if wx:
        return "weather", "in " + (", ".join(wx[:-1]) + " or " + wx[-1] if len(wx) > 1 else wx[0]) + " weather"
    for c in pc:
        if c["fn"] == "GetGlobalValue" and "GameHour" in c["p1"]:
            return "night"
        if c["fn"] in WHEN_FN and is_true(c):
            return WHEN_FN[c["fn"]]
    return None


# ---------------------------------------------------------------------------
# Player weapons and the damage buffs that reach them
# ---------------------------------------------------------------------------

def player_weapons(S):
    """every weapon a player can use: has the legendary-crafting 'WEAPONS' keyword. One per name."""
    by_name = {}
    for r in S.weap:
        kws = [edid_of(k) for k in (r.get("Keywords") or "").split("|") if k.strip()]
        name = (r.get("WEAP_FULL") or "").strip()
        if not name or "ma_legendarycrafting_weapon" not in kws or "_" in name or name[0].islower():
            continue
        ed = r.get("WEAP_EDID") or ""
        bad = bool(re.match(r"(zzz|DEL_|CUT_|cr|HTO_|POST_|W05_)", ed))
        cur = by_name.get(name)
        if cur is None or (cur["bad"] and not bad):
            by_name[name] = {"edid": ed, "kw": set(kws), "bad": bad}
    return by_name


def stat_damage_rules(S, av_slot):
    """slot -> list of condition lists from STAT_DamagePerk (any one list passing = the buff reaches the weapon)"""
    out = {}
    for e in S.perkep.get("STAT_DamagePerk", []):
        if e.get("EntryPoint") != "Mod Weapon DMG Bonus Mult":
            continue
        m = re.search(r"Actor Value=(\w+)", e.get("Raw") or "")
        if not m:
            continue
        slot = av_slot.get(m.group(1)) or av_slot.get(re.sub(r"_Formula$", "", m.group(1)))
        if not slot:
            continue
        out.setdefault(slot, []).append({"av": m.group(1), "conds": parse_perk_conds(e.get("Conditions"))})
    return out


def weapon_passes(S, kws, edid, conds):
    def test(c):
        if c.get("tab", 1) != 1:
            return None                               # about you, not the weapon: situational
        fn = c["fn"]
        if fn == "HasKeyword":
            has = edid_of(c["p1"]) in kws
            return has if is_true(c) else not has
        if fn == "GetIsID":
            same = edid_of(c["p1"]) == edid
            return same if is_true(c) else not same
        if fn == "IsTrueForConditionForm":
            sub = [dict(x, tab=1) for x in S.cndf.get(edid_of(c["p1"]), [])]
            r = eval_conds(sub, test)
            return r if is_true(c) else not r
        return None
    return eval_conds(conds, test)


def build_weapons(S, av_slot):
    rules = stat_damage_rules(S, av_slot)
    weapons = {}
    for name, w in sorted(player_weapons(S).items()):
        cats = [s for s, lists in rules.items()
                if any(weapon_passes(S, w["kw"], w["edid"], L["conds"]) for L in lists)]
        if "WeaponTypeExplosive" in w["kw"] or "WeaponTypeThrown" in w["kw"] or "WeaponTypeExplosiveHybrid" in w["kw"]:
            cats.append("dmg_explosive")
        short = sorted(k[10:] for k in w["kw"] if k.startswith("WeaponType"))
        weapons[name] = {"edid": w["edid"], "c": sorted(set(cats)), "heavy": "WeaponTypeHeavyGun" in w["kw"], "kw": short,
                         "ma": sorted(k for k in w["kw"] if k.startswith("ma_") and "legendary" not in k.lower())}
    rule_text = {s: [{"av": L["av"], "conds": [c["raw"] for c in L["conds"]]} for L in lists] for s, lists in rules.items()}
    return weapons, rule_text


# ---------------------------------------------------------------------------
# Bobblehead: Caps - stash odds
# ---------------------------------------------------------------------------

def caps_inside(S, cont_edid, depth=0):
    """caps a stash container holds: (min, max, average) over its pick-one leveled lists"""
    vals = []

    def walk(lvli, d):
        for e in S.lvli_entries.get(lvli, []):
            ref = e.get("LVLO_Reference") or ""
            parts = ref.split(":")
            if len(parts) >= 3 and parts[2] == "LVLI" and d < 6:
                walk(parts[1], d + 1)
            elif len(parts) >= 3 and parts[2] == "CNCY":
                vals.append(fnum(e.get("LVIV_Quantity"), 1.0))
    for it in S.cont.get(cont_edid, []):
        if it["Item_Sig"] == "LVLI":
            walk(it["Item_EDID"], 0)
        elif it["Item_Sig"] == "CNCY":
            vals.append(fnum(it["Count"], 1.0))
    if not vals:
        return None
    return min(vals), max(vals), sum(vals) / len(vals)


def caps_odds(S, perk_edid):
    """the tin list that swaps on the perk -> tiers with normal / with-bobblehead rates (rng76, First Match)"""
    import rng76                                  # noqa: E402
    swap = None
    for lv, rows in S.lvli_entries.items():
        for e in rows:
            if perk_edid in (e.get("Cond1") or "") and "HasPerk" in (e.get("Cond1") or ""):
                swap = lv
        if swap:
            break
    if not swap:
        warn(f"no leveled list switches on {perk_edid} - caps odds left out")
        return None
    rows = sorted(S.lvli_entries[swap], key=lambda e: int(e.get("EntryIndex") or 0))
    lists = [r["LVLO_Reference"].split(":")[1] for r in rows]       # [with perk, normal]
    try:
        res = rng76.Rng76Resolver(rng76.LvliIndex(), rng76.GlobIndex(), rng76.ItemNameIndex())
    except Exception as ex:                                          # pragma: no cover
        warn(f"rng76 resolver: {ex}")
        return None

    def tiers(lv):
        es = sorted(S.lvli_entries.get(lv, []), key=lambda e: int(e.get("EntryIndex") or 0))
        conds = [[e.get(f"Cond{i}") for i in range(1, int(e.get("CondCount") or 0) + 1) if e.get(f"Cond{i}")] for e in es]
        rates = res.first_match_rates(conds) or []
        return [(e["LVLO_Reference"].split(":")[1], rates[i] if i < len(rates) else 0.0) for i, e in enumerate(es)]
    bob, base = tiers(lists[0]), tiers(lists[-1])
    base_rate = dict(base)
    out = []
    for cont, r in bob:
        c = caps_inside(S, cont)
        out.append({"name": re.sub(r"^.*_", "", cont), "cont": cont, "min": c[0] if c else None, "max": c[1] if c else None,
                    "avg": round(c[2], 2) if c else None, "base": round(base_rate.get(cont, 0.0), 6), "bob": round(r, 6)})
    avg = lambda k: round(sum((t["avg"] or 0) * t[k] for t in out), 2)
    return {"list": swap, "lists": lists, "tiers": out, "avgBase": avg("base"), "avgBob": avg("bob")}


# ---------------------------------------------------------------------------
# Items
# ---------------------------------------------------------------------------

UTIL_BY_AV = {"STAT_Lockpicking": "lockpick", "PABatteryDamageRate": "pa_drain"}
UTIL_BY_PERK = {"Bobblehead_CapsPerk": "loot_caps", "Bobblehead_SciencePerk": "hacking"}


def lines_from_buffs(effects):
    return [{"t": e.get("display") or e.get("name") or "", "p": e.get("polarity") or "", "d": e.get("durationDisplay") or ""}
            for e in effects if (e.get("display") or e.get("name"))]


def obtain_lines(item):
    return [{"route": r.get("route"), "lines": r.get("lines") or []} for r in (item.get("obtainRoutes") or []) if r.get("populated")]


def tidy_fx(S, it, effects):
    """calculator fx -> the guide's fx: utility slots, Repair split, when-codes from the real conditions"""
    by_e = {e["mgef"]: e for e in effects}
    out = []
    for f in it.get("fx") or []:
        f = dict(f)
        e = by_e.get(f.get("e"))
        if e:
            if f["s"] == "other":
                s = UTIL_BY_AV.get(e["av"]) or UTIL_BY_PERK.get(e["perk"])
                if s:
                    f["s"] = s
                    f.pop("t", None)          # the slot's own words ("Better caps stashes")
            if not f.get("w"):
                w = when_from_effect(e)
                if isinstance(w, tuple):
                    f["w"], f["rt"] = w
                elif w:
                    f["w"] = w
            if f["s"] == "fusion_core":
                for p in e["ep"]:
                    if p["ep"] == "Mod Ammo Health Mult" and "Gatling Laser" in " ".join(p.get("conds") or []):
                        f["m"] = round((fnum(p["val"], 1.0) - 1) * 100, 2)
                        f["req"], f["rt"] = {"weapon": "gatling"}, "with a Gatling Laser"
        out.append(f)
    have = {f.get("e") for f in out}
    for e in effects:                                  # effects the calculator doesn't slot (e.g. PA core drain)
        s = UTIL_BY_AV.get(e["av"]) or UTIL_BY_PERK.get(e["perk"])
        if s and e["mgef"] not in have and e["mag"]:
            m = round(e["mag"] * 100, 2) if e["kind"] == "Subtract" and e["mag"] <= 1 else e["mag"]
            d = next((x["d"] for x in out if x.get("d")), e["dur"])
            f = {"s": s, "m": m, "d": d, "st": e["mgef"], "e": e["mgef"]}
            if s == "pa_drain":
                f["req"], f["rt"] = {"armour": "power"}, "in Power Armour"
            out.append(f)
    return out


def special_of_slot(rules):
    return {s: d["label"] for s, d in rules["slots"].items() if d.get("group") == "SPECIAL"}


def msg_for(S, kind, it):
    norm = lambda x: re.sub(r"[^a-z]", "", (x or "").lower())
    if kind == "magazine":
        m = S.mesg.get(re.sub(r"_Potion$", "_Msg", it["edid"]))
    else:
        base = norm(re.sub(r"^(Glowing )?Bobblehead:\s*", "", it["gameName"] or it["n"]))
        m = next((v for k, v in S.mesg.items() if k.startswith("Bobblehead_") and k.endswith("_Msg")
                  and (norm(k[11:-4]).startswith(base) or base.startswith(norm(k[11:-4])))), None)
    if not m:
        return None
    text = (m.get("DESC") or "").replace("\r", "")
    q = re.match(r'\s*"(.*?)"\s*(.*)', text, re.S)
    quote, rest = (q.group(1), q.group(2)) if q else ("", text)
    rest = re.sub(r"^You've collected an issue of [^!]+!\s*", "", rest.strip())
    return {"quote": quote, "text": re.sub(r"\s+", " ", rest).strip(), "edid": m["MESG_EDID"], "formId": m["MESG_FormID"]}


class Tok:
    """fills {tokens} in the hand-kept words from the exports"""
    def __init__(self, S, it=None, page=None):
        self.S, self.it, self.page = S, it, page

    def __call__(self, text):
        return re.sub(r"\{([a-z_0-9]+)(?::([^}]+))?\}", self._one, text or "")

    def _one(self, m):
        k, a = m.group(1), m.group(2)
        S, it, pg = self.S, self.it, self.page or {}
        try:
            if k == "fx0":
                f = next(f for f in it["fx"] if not f.get("neg"))
                return num(f["m"])
            if k == "perkpct":
                v = fnum(S.perkep[a][0]["Value"])
                return num(abs(1 - v) * 100)
            if k == "mgefpct":
                e = next(e for e in it["effects"] if e["mgef"] == a)
                return num(e["mag"] * 100)
            if k == "mgefx":
                e = next(e for e in it["effects"] if e["mgef"] == a)
                return num(round(1 / (1 - e["mag"]), 1))
            if k in pg.get("tokens", {}):
                return pg["tokens"][k]
        except Exception as ex:
            warn(f"token {{{k}:{a}}}: {ex}")
            return m.group(0)
        warn(f"unknown token {{{k}}}")
        return m.group(0)


def note_for(S, words, it):
    notes = words.get("notes") or {}
    key = it["edid"]
    n = notes.get(key) or notes.get(re.sub(r"^GHL_Glowing", "", key)) or \
        next((v for k, v in notes.items() if k.lower() == re.sub(r"^GHL_Glowing", "", key).lower()), None)
    if not n:
        return None
    return [n[0], Tok(S, it)(n[1])]


def base_item(S, src, page_key, kind, calc_it=None):
    """one item from dist/buffs/<page>.json (src) and the calculator (calc_it)"""
    fid = (src or {}).get("formId") or calc_it["id"]
    edid = (src or {}).get("edid") or calc_it["edid"]
    kw = next((x[10:] for x in ((src or {}).get("technical") or {}).get("ids", []) if x.startswith("Keywords:")), "")
    effects = alch_effects(S, fid)
    it = {
        "id": fid, "edid": edid, "t": kind,
        "n": (src or {}).get("name") or calc_it["n"], "gameName": (src or {}).get("gameName") or (calc_it or {}).get("n") or "",
        "desc": (src or {}).get("description") or "",
        "img": (SITE + src["imageUrl"]) if (src or {}).get("imageUrl") else "",
        "value": (src or {}).get("value"), "weight": (src or {}).get("weight"), "kw": kw,
        "diet": (src or {}).get("diet") if (src or {}).get("diet") not in (None, "None") else "",
        "bnb": bool(calc_it and calc_it.get("menu") and calc_it["menu"].get("pc") is not None),
        "lines": lines_from_buffs((src or {}).get("effects") or []),
        "obtain": obtain_lines(src or {}),
        "effects": effects,
        "dg": (calc_it or {}).get("dgAll") or [],
    }
    if calc_it and calc_it.get("sup"):
        it["sup"] = 1
    it["fx"] = tidy_fx(S, calc_it or {}, effects)
    return it


def extra_items(S, page_key, ids, kind, calc_by_id):
    """calculator items the buffs page doesn't list: built with build_buffs_json's own item builder"""
    if not ids:
        return []
    B, BS = S.B()
    out = []
    for fid in ids:
        rec = BS.alch.get(fid)
        if not rec:
            continue
        src = B.build_alch_item(BS, rec, page_key, "effect-groups")
        out.append(base_item(S, src, page_key, kind, calc_by_id[fid]))
    return out


def build_alch_page(S, page_key, spec, calc_by_id):
    kind = spec["kind"]
    page = S.buff_pages[page_key]
    skip = set()
    for other in spec.get("not_in", []):
        skip |= {i["formId"] for i in S.buff_pages[other]["items"]}
    items, seen = [], set()
    for src in page["items"]:
        if src["formId"] in skip or src.get("status") == "cut":
            continue
        items.append(base_item(S, src, page_key, kind, calc_by_id.get(src["formId"])))
        seen.add(src["formId"])
    extra = [i["id"] for i in S.calc["items"] if i["t"] in spec.get("calc_types", []) and i["id"] not in seen and i["id"] not in skip]
    items += extra_items(S, page_key, extra, kind, calc_by_id)
    return items


def finish_bobbleheads(S, items):
    rules = S.calc["rules"]
    spec = special_of_slot(rules)
    glowing, out = {}, []
    for it in items:
        it["base"] = re.sub(r"^(Glowing )?Bobblehead:\s*", "", it["gameName"] or it["n"])
        it["msg"] = msg_for(S, "bobblehead", it)
        s = next((f["s"] for f in it["fx"] if f["s"] in spec), None)
        if s:
            it["avDesc"] = S.avif.get(spec[s], {}).get("desc") or ""
        if it["edid"].startswith("GHL_"):
            glowing[it["base"]] = it
        else:
            out.append(it)
    for it in out:
        g = glowing.get(it["base"])
        if g:
            it["glowing"] = {"id": g["id"], "edid": g["edid"], "n": g["n"]}
    return out


def finish_magazines(S, items):
    for it in items:
        it["series"] = re.sub(r"\s+\d+$", "", it["n"])
        m = re.search(r"(\d+)$", it["n"])
        it["no"] = int(m.group(1)) if m else 0
        it["msg"] = msg_for(S, "magazine", it)
    return items


# ---------------------------------------------------------------------------
# Mutations (SPEL)
# ---------------------------------------------------------------------------

def build_mutations(S, av_slot):
    rules = S.calc["rules"]
    page = S.buff_pages["mutations"]
    neg_kw = "AbilityTypeMutation_NegativeEffect"
    out = []
    for src in page["items"]:
        rows = S.spel_eff.get(src["edid"], [])
        effects, fx, texts = [], {}, {"normal": [], "super": []}
        for r in rows:
            mfid = (r.get("EFID_MGEF_FormID") or "").upper()
            conds = [parse_spel_cond(c) for c in (r.get("Conditions") or "").split(" | ") if c.strip()]
            conds = [c for c in conds if c]
            ver = "both"
            for c in conds:
                if c["fn"] == "IsTrueForConditionForm":
                    if "UseSuperVersion" in c["p1"]:
                        ver = "super"
                    elif "UseNormalVersion" in c["p1"]:
                        ver = "normal"
            serum = any(c["fn"] == "IsSpellTarget" and "Serum_" in c["p1"] and is_true(c) for c in conds)
            radx = any(c["fn"] == "IsSpellTarget" and "RadX" in c["p1"] and is_true(c) for c in conds)
            team = None
            for c in conds:
                if c["fn"] == "IsMemberOfAPlayerTeam":
                    team = "team" if is_true(c) else "solo"
                if c["fn"] == "IsTrueForConditionForm":
                    sub = S.cndf.get(edid_of(c["p1"]), [])
                    # the Strange In Numbers version checks carry a team test of their own - that is
                    # the "super" version, not a team-only buff (Herd Mentality's own check is)
                    if any(x["fn"] == "HasPerk" and "StrangeInNumbers" in x["p1"] for x in sub):
                        continue
                    for x in sub:
                        if x["fn"] == "IsMemberOfAPlayerTeam" and is_true(x):
                            team = "team"
            words = words_for([dict(c, tab=0) for c in conds if not (c["fn"] == "IsTrueForConditionForm" and "Version" in c["p1"])], None, S.cndf)
            if ver == "super":
                words.insert(0, "Strange In Numbers version: you have the Strange In Numbers perk and a mutated teammate")
            e = tech_effect(S, mfid, fnum(r.get("EFIT_Magnitude"), 0.0), fnum(r.get("EFIT_Duration"), 0.0),
                            r.get("MAGG_Global"), r.get("DURG_Global"), [], spel_conds=words)
            if not e:
                continue
            e["condRaw"] = [c["raw"] for c in conds]
            e["ver"] = ver
            effects.append(e)
            if serum or radx or e["mgef"] in ("abMutationCount",) or "Treated" in e["mgef"]:
                continue
            neg = neg_kw in S.mgefkw.get(mfid, []) or S.mgef_detr.get(mfid, False)
            slot = av_slot.get(e["av"])
            if not slot:
                txt = re.sub(r"<[^>]+>", num(e["mag"]), e["dnam"] or "").strip()
                if txt and not neg and "Reduce" not in e["mgef"]:
                    texts["super" if ver == "super" else "normal"].append(txt)
                continue
            g = fx.setdefault((slot, bool(neg)), {"normal": [], "super": None, "e": e["mgef"]})
            if ver in ("normal", "both"):
                g["normal"].append((e["mag"], team))
            if ver in ("super", "both"):
                g["super"] = e["mag"]
        fxl = []
        for (slot, neg), g in fx.items():
            rows_n = g["normal"] or [(0.0, None)]
            for mag, team in rows_n:
                ms = g["super"] if g["super"] is not None else mag
                if not mag and not ms:
                    continue
                f = {"s": slot, "m": mag, "ms": ms, "d": 0, "st": g["e"], "e": g["e"]}
                if neg:
                    f["neg"] = 1
                if team:
                    f["w"] = team
                fxl.append(f)
        if not any(not f.get("neg") for f in fxl):
            sup = texts["super"]
            for i, t in enumerate(dict.fromkeys(texts["normal"])):
                fxl.append({"s": "other", "m": 0, "d": 0, "t": t, "ts": sup[i] if i < len(sup) else t})
        it = {"id": src["formId"], "edid": src["edid"], "t": "mutation", "n": re.sub(r"\s+Mutation$", "", src["name"]),
              "gameName": src["name"], "desc": src.get("description") or "",
              "img": (SITE + src["imageUrl"]) if src.get("imageUrl") else "", "kw": "",
              "lines": lines_from_buffs(src.get("effects") or []), "obtain": obtain_lines(src),
              "effects": effects, "fx": fxl, "dg": []}
        out.append(it)
    return out


def mutation_mech(S):
    cf = []
    for i in range(1, 6):
        rows = S.perkep.get(f"ClassFreak0{i}") or []
        v = next((fnum(r["Value"]) for r in rows if r.get("EntryPoint") == "Mod Spell Magnitude"), None)
        if v is None:
            break
        cf.append(v)
    return {"classFreak": cf, "classFreakPerk": "ClassFreak01"}


# ---------------------------------------------------------------------------
# Page words
# ---------------------------------------------------------------------------

def guide_links(S, spec):
    out = []
    for g in spec.get("guides") or []:
        if "id" in g:
            r = S.guides.get(g["id"])
            if r and r.get("visibility") == "public":
                out.append([g.get("label") or r["title"], r["url"]])
            elif not r:
                warn(f"guide link {g['id']} not in guide_index.tsv")
        elif "children_of" in g:
            kids = [r for r in S.guides.values() if r.get("parentId") == g["children_of"] and r.get("visibility") == "public"
                    and r.get("nodeType") == "page" and re.search(g.get("match", "."), r["title"])]
            kids.sort(key=lambda r: (int(r.get("menuOrder") or 0) if (r.get("menuOrder") or "").isdigit() else 0, r["title"]))
            for r in kids:
                t = r["title"]
                for pre in g.get("strip", []):
                    t = t.replace(pre, "")
                out.append([t, r["url"]])
    return out


def page_tokens(S, kind, items):
    mech = S.calc["mechanics"]
    t = {}
    durs = [f["d"] for it in items for f in it.get("fx", []) if f.get("d")]
    if durs:
        base = max(set(durs), key=durs.count)
        t["dur"] = minutes(base)
        if kind in ("magazine", "bobblehead"):
            cm, col = mech["curator"]["mult"], mech["collectors"]["byPieces"]
            t["curator_mult"] = num(cm)
            t["curator_dur"] = minutes(base * cm)
            t["collectors_list"] = ", ".join(f"{i + 1} piece{'s' if i else ''} {minutes(base * v)}" for i, v in enumerate(col))
            t["both_dur"] = minutes(base * cm * col[-1])
            t["collectors_pieces"] = str(len(col))
    pg = mech.get("partyGirl") or []
    if pg:
        t["party1"], t["party2"] = num(pg[0]), num(pg[-1])
    t["fiend"] = num(mech.get("chemFiend") or 2)
    t["fiend_pct"] = num(((mech.get("chemFiend") or 2) - 1) * 100)
    diet = mech.get("diet") or {}
    for k, short in (("carnivore", "carn"), ("herbivore", "herb")):
        xs = [r["x"] for r in (diet.get(k) or {}).get("rules", []) if r.get("x")]
        if xs:
            t[short] = num(min(xs))
            t[short + "_super"] = num(max(xs))
    cf = mutation_mech(S)["classFreak"]
    if cf:
        t["classfreak"] = ", ".join(f"rank {i + 1} {num((1 - v) * 100)}%" for i, v in enumerate(cf))
    t["count"] = str(len(items))
    return t


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

PAGES = {
    "alcohol":     {"kind": "alcohol", "calc_types": ["alcohol"], "not_in": ["nuka-cola"]},
    "chems":       {"kind": "chem", "calc_types": ["chem"]},
    "food":        {"kind": "food", "calc_types": ["food", "gum"], "not_in": ["nuka-cola"]},
    "nuka-cola":   {"kind": "nuka", "calc_types": []},
    "bobbleheads": {"kind": "bobblehead", "calc_types": []},
    "magazines":   {"kind": "magazine", "calc_types": []},
    "mutations":   {"kind": "mutation"},
}


def shared_json(S, av_slot, weapons, wrules):
    R = S.calc["rules"]
    keep = ("questions", "base_weights", "goals", "mirror", "when_share", "damage_first", "team_play", "linked", "penalties", "show_threshold",
            "slots", "display_order", "exclude_items")
    spec = special_of_slot(R)
    mech = dict(S.calc["mechanics"])
    mech.update(mutation_mech(S))
    # only slots some weapon can pass on its own keywords (scoped damage depends on a scope mod, so it's left out)
    dmg_slots = sorted({c for w in weapons.values() for c in w["c"]})
    return {
        "version": datetime.date.today().isoformat(),
        "generated": datetime.datetime.now().isoformat(timespec="seconds"),
        "rules": {k: R[k] for k in keep if k in R},
        "goals": S.specials.get("goals") or {},
        "mech": mech,
        "weapons": weapons,
        "weaponRules": {"perk": "STAT_DamagePerk", "slots": dmg_slots, "conds": wrules},
        "specialDesc": {s: S.avif.get(label, {}).get("desc") or "" for s, label in spec.items()},
        "sources": S.paths,
    }


def build(data_dir, dist_dir):
    S = Src(data_dir, dist_dir)
    import build_food_buff_calculator_json as FC          # noqa: E402  (one AV -> slot table)
    av_slot = dict(FC.AV_SLOT)
    av_slot.update(FC.MAGBOB_AV_SLOT)
    calc_by_id = {i["id"]: i for i in S.calc["items"]}
    weapons, wrules = build_weapons(S, av_slot)
    out_dir = os.path.join(dist_dir, "buff_guides")
    os.makedirs(out_dir, exist_ok=True)
    shared = shared_json(S, av_slot, weapons, wrules)

    for key, spec in PAGES.items():
        words = S.words["pages"][key]
        kind = spec["kind"]
        if kind == "mutation":
            items = build_mutations(S, av_slot)
        else:
            items = build_alch_page(S, key, spec, calc_by_id)
            if kind == "bobblehead":
                items = finish_bobbleheads(S, items)
            elif kind == "magazine":
                items = finish_magazines(S, items)
        for it in items:
            n = note_for(S, words, it)
            if n:
                it["odd"] = n
        items.sort(key=lambda i: re.sub(r"\d+", lambda m: m.group(0).zfill(4), i["n"].lower()))
        tok = page_tokens(S, kind, items)
        pt = Tok(S, None, {"tokens": tok})
        extra = {}
        if kind == "bobblehead":
            perk = next((e["perk"] for it in items for e in it["effects"] if e["perk"] and UTIL_BY_PERK.get(e["perk"]) == "loot_caps"), None)
            if perk:
                extra["caps"] = caps_odds(S, perk)
        body = {
            "version": datetime.date.today().isoformat(),
            "generated": datetime.datetime.now().isoformat(timespec="seconds"),
            "page": key, "kind": kind, "url": words["url"],
            "title": words["title"], "blurb": pt(words["blurb"]),
            "copy": words["copy"], "count": pt(words.get("count") or ""),
            "basics": {"title": words.get("basics_title") or "", "lines": [pt(x) for x in words.get("basics") or []],
                       "guidesTitle": words.get("guides_title") or "", "guides": guide_links(S, words)},
            "perkbar": words.get("perkbar") or [],
            "items": items, **extra,
        }
        path = os.path.join(out_dir, f"{key}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(body, f, ensure_ascii=False, separators=(",", ":"))
        nfx = sum(1 for i in items if any(not f.get("neg") for f in i["fx"]))
        print(f"[buff_guides] {key:12s} {len(items):4d} items ({nfx} with buffs the finder can use) -> {path}")
    path = os.path.join(out_dir, "shared.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(shared, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[buff_guides] shared: {len(weapons)} player weapons -> {path}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Build the BnB Buffs guide page JSON.")
    ap.add_argument("--data-dir", default=os.path.join(REPO, "tsv"))
    ap.add_argument("--outdir", default=os.path.join(REPO, "dist"))
    a = ap.parse_args(argv)
    build(a.data_dir, a.outdir)


if __name__ == "__main__":
    main()
