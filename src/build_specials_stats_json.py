#!/usr/bin/env python3
"""
build_specials_stats_json.py
============================
Builds dist/specials_stats.json — the ONE file that knows what every SPECIAL
and stat does. It feeds:

  * the SPECIALs & Stats pages (dfbnb-child/assets/df-bnb-specials-stats.js),
    /bnb/specials-stats/<stat>/<stat>-guide/ plus the Overview and the
    Hunger & Thirst Bar Explained pages;
  * the Recommended Food Buff Calculator (goals section — "Do you die a lot?"
    = health_bar in HP + health_regen in HP/sec).

WHERE EVERY NUMBER COMES FROM (never typed in)
----------------------------------------------
  GMST   tsv/GMST_Export_*.tsv            fAVDCarryWeightBase, fAVDHealthStartEndMult ...
  CURV   tsv/CURV_Export_*_POINTS.tsv     CT_LuckVATSCriticalCharge, ScrapBonusINT ...
  STATREC tsv/STATREC_Export_*.tsv        RACE / Player PRPS (Health 205 + 40 ...),
                                          AVIF min / max / default. Written by the
                                          xEdit script "!!!Wordpress - ExportStatRecordsToTSV.pas"
                                          or tools/esm_stat_records_extract.py.
  PERK   tsv/PERK_Export_*.tsv            hidden player perks (STAT_DamagePerk ...) and
                                          every perk card's entry points / abilities
  PERKEP tsv/PERKEP_Export_*.tsv          ability links
  PCRD   tsv/PCRD_Export_*.tsv            perk card -> perk per rank
  SPEL   tsv/SPEL_Export_*_EFFECTS.tsv    ability / spell effects (hunger, thirst, teams ...)
  MGEF   tsv/MGEF_Export_*.tsv            effect -> actor value + archetype
  GLOB   tsv/GLOB_Export_*.tsv            hunger / thirst thresholds
  OMOD   tsv/OMOD_Export_*(_Properties).tsv  armour / power armour / backpack mods
  ENCH   tsv/ENCH_Export_*.tsv            what a mod's enchantment does
  dist/calculators/food_buff_calculator.json   food / chem / drink items + diet multipliers
  dist/calculators/healing_calculator.json     HP regen sources (reused, not copied)
  dist/buffs/*.json                       chems, drinks, bobbleheads, magazines, mutations ...
  dist/builds/builds_data.json            perk card rank text + legendary effects
  dist/buff-stations.json                 CAMP buff stations

WHAT-TO-SHOW (hand kept, no numbers): data/specials_stats/stats.json and
data/specials_stats/pages.json (Overview text).

Usage:
  python src/build_specials_stats_json.py
  python src/build_specials_stats_json.py --pts        (reads tsv/pts, writes dist/pts)
"""
import argparse, csv, datetime, json, math, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

try:
    import tsv_source                                          # noqa: E402
except Exception:                                              # pragma: no cover
    tsv_source = None
try:
    from patchlog_utils import write_empty_patchlog_feed       # noqa: E402
except Exception:                                              # pragma: no cover
    write_empty_patchlog_feed = None

DATA_DIR = os.path.join(REPO, "data", "specials_stats")
WARNINGS = []


def warn(msg):
    WARNINGS.append(msg)
    print(f"[specials_stats] WARNING: {msg}", file=sys.stderr)


# ───────────────────────────── loading ─────────────────────────────

class Src:
    def __init__(self, channel):
        self.channel = channel
        self.used = {}

    def tsv(self, pattern, required=True):
        path = None
        if tsv_source is not None:
            try:
                path = tsv_source.newest(pattern, channel=self.channel, required=False)
            except TypeError:
                path = tsv_source.newest(pattern, channel=self.channel)
            except Exception:
                path = None
        if not path:
            import glob
            root = os.path.join(REPO, "tsv", "pts" if self.channel == "pts" else "")
            hits = sorted(glob.glob(os.path.join(root, pattern)))
            path = hits[-1] if hits else None
        if not path:
            if required:
                raise SystemExit(f"[specials_stats] FATAL: no tsv matches {pattern}")
            warn(f"no tsv matches {pattern}")
            return []
        self.used[pattern] = os.path.basename(path)
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            return list(csv.DictReader(f, delimiter="\t"))

    def dist(self, rel, required=True):
        base = os.path.join(REPO, "dist")
        cands = [os.path.join(base, "pts", rel)] if self.channel == "pts" else []
        cands.append(os.path.join(base, rel))
        for p in cands:
            if os.path.exists(p):
                self.used[rel] = os.path.relpath(p, REPO).replace("\\", "/")
                with open(p, encoding="utf-8") as f:
                    return json.load(f)
        if required:
            raise SystemExit(f"[specials_stats] FATAL: missing dist/{rel}")
        warn(f"missing dist/{rel}")
        return None


def fnum(v, d=None):
    try:
        x = float(str(v).strip())
        return x if math.isfinite(x) else d
    except Exception:
        return d


def clean(v):
    x = float(v)
    if abs(x - round(x)) < 1e-9:
        return int(round(x))
    return round(x, 6)


def first_edid(v):
    """'Strength "Strength" [AVIF:000002C2]' -> 'Strength'"""
    m = re.match(r"\s*([A-Za-z_][\w]*)", str(v or ""))
    return m.group(1) if m else ""


def ref_fid(v):
    m = re.search(r"\[\w{4}:([0-9A-Fa-f]{8})\]", str(v or ""))
    return m.group(1).upper() if m else ""


# ───────────────────────────── records ─────────────────────────────

class Records:
    def __init__(self, S):
        self.S = S
        self.gmst = {r["EDID"]: r for r in S.tsv("GMST_Export_*.tsv")}
        self.glob = {}
        for r in S.tsv("GLOB_Export_*.tsv"):
            self.glob[r.get("EDID", "")] = r
        self.curv_ids = {r["EDID"]: r["FormID"] for r in S.tsv("CURV_Export_*_CURV.tsv")}
        self.curves = {}
        for r in S.tsv("CURV_Export_*_POINTS.tsv"):
            x, y = fnum(r.get("X")), fnum(r.get("Y"))
            if x is None or y is None:
                continue
            self.curves.setdefault(r["EDID"], []).append([clean(x), clean(y)])
        for k in self.curves:
            self.curves[k].sort()
        self.statrec = S.tsv("STATREC_Export_*.tsv", required=False)
        if not self.statrec:
            warn("STATREC export missing - run '!!!Wordpress - ExportStatRecordsToTSV.pas' or "
                 "tools/esm_stat_records_extract.py; base HP / AP regen will be blank")
        self.perk_rows = S.tsv("PERK_Export_*.tsv")
        self.perk_by = {}
        for r in self.perk_rows:
            self.perk_by.setdefault(r["PERK_EDID"], []).append(r)
        self.perkep = {}
        for r in S.tsv("PERKEP_Export_*.tsv"):
            self.perkep.setdefault(r["PERK_EDID"], []).append(r)
        self.pcrd = S.tsv("PCRD_Export_*.tsv")
        self.spel_fx = {}
        for r in S.tsv("SPEL_Export_*_EFFECTS.tsv"):
            self.spel_fx.setdefault(r["SPEL_EDID"], []).append(r)
        self.mgef = {}
        for r in S.tsv("MGEF_Export_*.tsv"):
            self.mgef[r["EDID"]] = {
                "av": first_edid(r.get("ActorValue")),
                "arch": (r.get("Archetype") or "").strip(),
                "full": (r.get("FULL") or "").strip(),
                "desc": (r.get("DNAM_MagicItemDescription") or "").strip(),
                "fid": r.get("MGEF_FormID", ""),
            }
        self.ench = {}
        for r in S.tsv("ENCH_Export_*.tsv"):
            fx = []
            i = 0
            while f"Effect_{i + 1}_MGEF_EID" in r:     # every effect slot the export has (was 1-12 only)
                i += 1
                e = (r.get(f"Effect_{i}_MGEF_EID") or "").strip()
                if not e:
                    continue
                fx.append({"mgef": e, "mag": fnum(r.get(f"Effect_{i}_Magnitude"), 0.0),
                           "curve": (r.get(f"Effect_{i}_CURV_EID") or "").strip(),
                           # MAGA = the actor value the curve is keyed on (ENCH export columns added 20 Oct 2026)
                           "inFid": (r.get(f"Effect_{i}_MAGA_FID") or "").strip().upper(),
                           "inEdid": (r.get(f"Effect_{i}_MAGA_EID") or "").strip()})
            self.ench[r["ENCH_EDID"]] = {"name": (r.get("ENCH_FULL") or "").strip(), "fx": fx,
                                         "fid": r.get("ENCH_FormID", "")}
        self.av_name = {}
        for r in S.tsv("AVIF_Export_*.tsv"):
            self.av_name[r.get("EDID", "")] = (r.get("FULL") or "").strip()
        self.omod = {r["OMOD_EDID"]: r for r in S.tsv("OMOD_Export_*[0-9].tsv", required=False)
                     or S.tsv("OMOD_Export_*.tsv")}
        self.omod_props = S.tsv("OMOD_Export_*_Properties.tsv")
        # what each curve is keyed on, read straight from SeventySix.esm by tools/esm_curve_inputs_extract.py
        # (ENCH / SPEL MAGA + PERK EPF4). Optional: the ENCH / SPEL / PERKEP exports carry the same thing.
        self.curvin = S.tsv("CURVIN_Export_*.tsv", required=False)

    def label(self, m):
        """Readable name for a magic effect: its FULL, or the actor value's name."""
        f = (m or {}).get("full", "")
        if f and not re.match(r"^(SURV_|GHL_|ab|Ab)[A-Za-z]", f):
            return f
        av = (m or {}).get("av", "")
        return self.av_name.get(av) or f or av

    def curve_max(self, edid):
        pts = self.curves.get(edid) or []
        return pts[-1][1] if pts else None

    # ---- single value lookups (each returns (value, src)) ----
    def g(self, edid):
        r = self.gmst.get(edid)
        if not r:
            warn(f"GMST {edid} not found")
            return None, {"type": "GMST", "edid": edid, "missing": True}
        return clean(fnum(r["Value"], 0)), {"type": "GMST", "edid": edid, "formId": r["FormID"]}

    def gl(self, edid):
        r = self.glob.get(edid)
        if not r:
            return None, {"type": "GLOB", "edid": edid, "missing": True}
        return clean(fnum(r.get("FLTV"), 0)), {"type": "GLOB", "edid": edid, "formId": r.get("FormID", "")}

    def curve(self, edid):
        pts = self.curves.get(edid)
        if not pts:
            warn(f"CURV {edid} has no points")
            return None, {"type": "CURV", "edid": edid, "missing": True}
        return pts, {"type": "CURV", "edid": edid, "formId": self.curv_ids.get(edid, "")}

    def prps(self, sig, edid, av):
        for r in self.statrec:
            if r["Signature"] == sig and r["EDID"] == edid and r["Field"] == "PRPS" and r["Ref_EDID"] == av:
                return clean(fnum(r["Value"], 0)), {"type": sig + " PRPS", "edid": edid, "formId": r["FormID"], "av": av}
        warn(f"{sig} {edid} has no PRPS {av}")
        return None, {"type": sig + " PRPS", "edid": edid, "av": av, "missing": True}

    def avif(self, edid, field):
        for r in self.statrec:
            if r["Signature"] == "AVIF" and r["EDID"] == edid and r["Field"] == field:
                v = fnum(r["Value"], 0)
                if abs(v) > 1e30:
                    v = None
                return (clean(v) if v is not None else None), {"type": "AVIF " + field, "edid": edid, "formId": r["FormID"]}
        return None, {"type": "AVIF " + field, "edid": edid, "missing": True}

    def perk_av(self, perk, av):
        for r in self.perk_by.get(perk, []):
            if first_edid(r.get("EPF3_ActorValue")) == av:
                v = fnum(re.sub(r"^Float=", "", r.get("EPFD_Float") or ""), None)
                if v is not None:
                    return clean(v), {"type": "PERK", "edid": perk, "formId": r["PERK_FormID"], "av": av,
                                      "entry": r.get("EP_EntryPoint", "")}
        warn(f"PERK {perk} has no entry on {av}")
        return None, {"type": "PERK", "edid": perk, "av": av, "missing": True}

    def perk_cond(self, perk, entry):
        for r in self.perk_by.get(perk, []):
            if (r.get("EP_EntryPoint") or "") == entry:
                m = re.search(r"\|\s*([-\d.]+)\s*\|\s*GetValue", r.get("PerkConditions_Flat") or "")
                if m:
                    return clean(float(m.group(1))), {"type": "PERK condition", "edid": perk, "formId": r["PERK_FormID"], "entry": entry}
        warn(f"PERK {perk} has no condition value on {entry}")
        return None, {"type": "PERK condition", "edid": perk, "entry": entry, "missing": True}

    def resolve(self, ref, defs, resist=None):
        """ref = {"gmst": ...} | {"curve": ...} | ... -> {"value"/"points", "src"}"""
        scale = ref.get("scale", 1)
        if "gmst" in ref:
            edid = resist if ref["gmst"] == "@resist" else ref["gmst"]
            v, s = self.g(edid)
        elif "glob" in ref:
            v, s = self.gl(ref["glob"])
        elif "curve" in ref:
            pts, s = self.curve(ref["curve"])
            return {"points": pts, "src": s}
        elif "prps" in ref:
            v, s = self.prps(*ref["prps"])
        elif "avif" in ref:
            v, s = self.avif(*ref["avif"])
        elif "perk" in ref:
            v, s = self.perk_av(*ref["perk"])
        elif "perkcond" in ref:
            v, s = self.perk_cond(*ref["perkcond"])
        elif "ref" in ref:
            return self.resolve(defs[ref["ref"]], defs, resist)
        elif "const" in ref:
            return {"value": ref["const"], "src": {"type": "engine", "why": ref.get("why", "")}}
        else:
            raise SystemExit(f"[specials_stats] FATAL: unknown input {ref}")
        if v is not None and scale != 1:
            v = clean(v * scale)
        out = {"value": v, "src": s}
        if scale != 1:
            out["scale"] = scale
        return out


# ───────────────────────────── formula maths (mirrors the JS) ─────────────────────────────

def curve_at(pts, x):
    if not pts:
        return 0
    if x <= pts[0][0]:
        return pts[0][1]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x0 <= x <= x1:
            return y0 if x1 == x0 else y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return pts[-1][1]


def evaluate(expr, inputs, vars_):
    env = {"min": min, "max": max, "pow": pow, "ceil": math.ceil, "floor": math.floor, "round": round,
           "clamp": lambda v, lo, hi: max(lo, min(hi, v)), "abs": abs}
    curves = {}
    for k, i in inputs.items():
        if "points" in i:
            curves[k] = i["points"]
            env[k] = k
        else:
            env[k] = i.get("value") or 0
    env["curve"] = lambda name, x: curve_at(curves.get(name), x)
    env.update(vars_)
    if re.search(r"[^\w\s.+\-*/(),]", expr):
        raise SystemExit(f"[specials_stats] FATAL: bad characters in formula {expr!r}")
    return eval(expr, {"__builtins__": {}}, env)  # noqa: S307  (hand-kept, character-checked)


def table_rows(f):
    v0 = f["vars"][0]
    lo, hi = v0.get("min", 1), v0.get("max", 100)
    if v0["name"] in ("STR", "PER", "END", "CHA", "INT", "AGI", "LCK"):
        xs = list(range(1, 16)) + [20, 25, 30, 40, 50, 75, 100]
    elif v0["name"] == "res":
        xs = [0, 50, 100, 150, 200, 250, 300, 350, 400, 500, 600, 700, 800, 1000, 1500, 2000]
    else:
        step = max(1, (hi - lo) // 10)
        xs = list(range(lo, hi + 1, step))
    base = {v["name"]: v.get("default", 0) for v in f["vars"]}
    rows = []
    for x in xs:
        vv = dict(base)
        vv[v0["name"]] = x
        try:
            y = evaluate(f["expr"], f["inputs"], vv)
        except ZeroDivisionError:
            y = None
        rows.append([x, None if y is None else round(y, f.get("decimals", 2) + 1)])
    return rows


# ───────────────────────────── boost sources ─────────────────────────────

STAT_EP = {
    # perk-card entry point -> stat page keys it boosts (abilities are matched by actor value instead)
    "Mod Item Weight": ["carry"], "Mod Exp": [], "Mod Kill Experience": [],
    "Mod Sprint AP Drain Rate": ["ap", "end"], "Mod VATS Hit Chance": ["vats_acc"],
    "Mod My Critical Hit Damage Mult": ["crit_dmg"], "Mod VATS Critical Count": ["crit_dmg"],
    "Mod VATS Critical Charge": ["crit_dmg", "luck"], "Mod Buy Prices": ["cha"], "Mod Sell Prices": ["cha"],
    "Mod Barter Charisma": ["cha"], "Mod Detection Sneak Skill": ["sneak"], "Mod Detection Light": ["sneak"],
    "Mod Detection Movement": ["sneak"], "Mod Sneak Attack Mult": ["sneak"],
    "Mod Crafting Return Quantity": ["int"], "Mod Power Attack Action Points": ["ap"],
    "Mod VATS Attack Action Points": ["ap"], "Mod Max Consecutive Hits Allowed": [],
}
SPECIAL_KEYS = {"Strength": "str", "Perception": "per", "Endurance": "end", "Charisma": "cha",
                "Intelligence": "int", "Agility": "agi", "Luck": "luck"}


def mgef_kind(m):
    a = (m or {}).get("arch", "")
    if a == "Peak Value Modifier":
        return "fortify"
    if a == "Value Modifier":
        return "restore"
    return a.lower() or "other"


def page_wants(stat, av, kind):
    """Does an effect on actor value `av` (fortify / restore) belong on this stat page?"""
    k = stat["key"]
    if not av:
        return False
    wanted = set(stat["avs"])
    if k == "max_hp":
        wanted.add("HealthBonus")
    if av not in wanted:
        return False
    if k == "max_hp" and kind == "restore":
        return False
    if k == "heal_rate" and av == "Health":
        return kind == "restore"
    return True


# ───────────────────────────── curve inputs ─────────────────────────────
# A few curves are keyed on actor values that are built into the engine and have NO record in
# SeventySix.esm. xEdit still knows their names (the SPEL export carries them: 00000392 HealthPercent,
# 0000032C PlayerLevel, 00000394 JunkItemCount, 00000399 Kill Streak ...), so the build learns the
# names from there (R.engine_names) and this table only adds a readable label + one line of help.
# Checked against every curve that uses each one (tools/esm_curve_inputs_extract.py -> CURVIN_Export).
ENGINE_AV = {
    # Unyielding, Bolstering, Nerd Rage, Last Bastion, *InverseHealth / *ProportionalHealth - curves run 0 - 1
    "HealthPercent": ("Health (fraction of max HP)",
                      "The game feeds in your current health as a fraction of max: 1 = full health, "
                      "0.2 = 20% health, 0 = none."),
    "PlayerLevel": ("Player level", "The game feeds in your character level."),
    # Junk Shield (JunkShieldDRBonus) - curve runs 1 - 100
    "JunkItemCount": ("Junk items carried", "The game feeds in how many junk items you are carrying."),
    # Adrenaline, Adrenal Reaction, Adrenal - curves run 0 - 20
    "Kill Streak": ("Kill streak", "The game feeds in your current kill streak (the Adrenaline stack)."),
    "ConsecutiveHitCount": ("Consecutive hits", "The game feeds in how many hits in a row you have landed."),
    "AmmoSpenderStacks": ("Ammo spent (stacks)", "The game feeds in your current ammo-spent stacks."),
    # No xEdit name seen yet. Aristocrat's, *_AddDamageViaCaps, *_ResistancesProportionalCaps - curves run 0 - 40,000
    "00000393": ("Caps carried", "The game feeds in the number of caps you are carrying."),
}
OMOD_EXPLAIN = ("Armour, underarmour and backpack mod curves have no input field. The game reads them "
                "at the level of the item the mod is on (the same Universal tier curves sit on the base "
                "armour records), so a level 50 piece gets the last row.")


def _pretty_av(edid):
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", re.sub(r"^(LGND|STAT|SURV)_", "", edid)).replace("_", " ").strip()


def curve_inputs(R):
    """curve EDID -> what the game feeds in as X, and what the curve drives, straight from the records.

    Sources, first one wins: SPEL effect MAGA, ENCH effect MAGA (export columns added 20 Oct 2026),
    PERK entry point EPF4 (PERKEP Raw), CURVIN (read from the ESM), then OMOD property curves
    (item level). Sets R.curve_input {curve: {label, kind, src, explain}} and
    R.curve_output {curve: [(actor value, effect label)]}."""
    R.curve_input, R.curve_output = {}, {}
    # built-in (engine) actor values: no AVIF record, but the xEdit exports name them
    known = set(R.av_name)
    R.engine_names = {}
    for rows in R.spel_fx.values():
        for fx in rows:
            f, e = (fx.get("MAGA_AVIF_FormID") or "").strip().upper(), (fx.get("MAGA_AVIF_EDID") or "").strip()
            if f and e and e not in known:
                R.engine_names.setdefault(f, e)
    for e in R.ench.values():
        for fx in e["fx"]:
            if fx.get("inFid") and fx.get("inEdid") and fx["inEdid"] not in known:
                R.engine_names.setdefault(fx["inFid"], fx["inEdid"])

    def put(cv, fid, edid, kind, src):
        cv, fid, edid = (cv or "").strip(), (fid or "").strip().upper(), (edid or "").strip()
        if not cv or cv in R.curve_input or not (fid or edid):
            return
        if not edid and fid in R.engine_names:
            edid = R.engine_names[fid]                 # built-in actor value xEdit knows the name of
        if edid in ENGINE_AV:
            lab, ex = ENGINE_AV[edid]
        elif edid:
            lab, ex = R.av_name.get(edid) or _pretty_av(edid), ""
        elif fid in ENGINE_AV:
            lab, ex = ENGINE_AV[fid]
        else:
            lab, ex = f"Engine value {fid}", "This input is built into the game engine and has no record to name it."
        R.curve_input[cv] = {"label": lab, "kind": kind, "explain": ex,
                             "src": dict(src, av=edid or fid)}

    def out(cv, mgef_edid):
        cv = (cv or "").strip()
        m = R.mgef.get((mgef_edid or "").strip())
        if not cv or not m or "description" in mgef_edid.lower():
            return
        pair = (m["av"], R.label(m))
        if pair not in R.curve_output.setdefault(cv, []):
            R.curve_output[cv].append(pair)

    for edid, rows in R.spel_fx.items():
        for fx in rows:
            cv = fx.get("CVTO_CURV_EDID")
            out(cv, fx.get("EFID_MGEF_EDID") or "")
            put(cv, fx.get("MAGA_AVIF_FormID"), fx.get("MAGA_AVIF_EDID"), "SPEL",
                {"type": "SPEL MAGA", "edid": edid, "formId": fx.get("SPEL_FormID", "")})
    for edid, e in R.ench.items():
        for fx in e["fx"]:
            out(fx["curve"], fx["mgef"])
            put(fx["curve"], fx.get("inFid"), fx.get("inEdid"), "ENCH",
                {"type": "ENCH MAGA", "edid": edid, "formId": e.get("fid", "")})
    for perk, rows in R.perkep.items():
        for r in rows:
            raw = r.get("Raw") or ""
            c = re.search(r"Curve Table=\s*([A-Za-z_][\w]*)\s*\[CURV:", raw)
            a = re.search(r"EPF4 - Actor value 2=\s*([A-Za-z_][\w]*)?\s*\[AVIF:([0-9A-Fa-f]{8})\]", raw)
            if c and a:
                ep = re.sub(r"^Mod ", "", (r.get("EntryPoint") or "").strip())
                ep = {"Spell Magnitude": "Effect strength"}.get(ep, ep)
                fn = r.get("Function") or ""
                if ep:
                    pair = ("", ep + (" (\u00d7)" if "Multiply" in fn else " (+)" if "Add" in fn else ""))
                    if pair not in R.curve_output.setdefault(c.group(1), []):
                        R.curve_output[c.group(1)].append(pair)
                put(c.group(1), a.group(2), a.group(1) or "", "PERK",
                    {"type": "PERK EPF4", "edid": perk, "formId": r.get("PERK_FormID", "")})
    for r in R.curvin:
        kind = r.get("Source_Sig", "")
        out(r.get("Curve_EDID"), r.get("Effect_EDID") or "")
        put(r.get("Curve_EDID"), r.get("Input_FormID"), r.get("Input_EDID"), kind,
            {"type": f"{kind} {r.get('Field', '')}".strip(), "edid": r.get("Source_EDID", ""),
             "formId": r.get("Source_FormID", "")})
    dt_stat = {"dtPhysical": ("dr", "Damage Resistance"), "dtEnergy": ("er", "Energy Resistance"),
               "dtFire": ("fire_res", "Fire Resistance"), "dtCryo": ("cryo_res", "Cryo Resistance"),
               "dtFrost": ("cryo_res", "Cryo Resistance"), "dtPoison": ("poison_res", "Poison Resistance"),
               "dtRadiationExposure": ("rad_res", "Radiation Resistance")}
    for p in R.omod_props:
        ct = p.get("CurveTable") or ""
        if "CURV:" in ct:
            cv = first_edid(ct)
            v1 = first_edid(p.get("Value1"))
            if p.get("PropertyName") == "Damage Type Value" and v1 in dt_stat:
                pair = dt_stat[v1]                       # (stat key, label) - matched on the page by key
            elif p.get("PropertyName") in ("Actor Values", "ActorValues") and v1:
                pair = (v1, R.av_name.get(v1) or _pretty_av(v1))
            else:
                pair = None
            if pair and cv and pair not in R.curve_output.setdefault(cv, []):
                R.curve_output[cv].append(pair)
            if cv and cv not in R.curve_input:
                R.curve_input[cv] = {"label": "Item level", "kind": "OMOD", "explain": OMOD_EXPLAIN,
                                     "src": {"type": "OMOD property", "edid": p.get("OMOD_EDID", ""),
                                             "formId": p.get("OMOD_FormID", "")}}


class Boost:
    def __init__(self, S, R, stats):
        self.S, self.R, self.stats = S, R, stats
        self.calc = S.dist("calculators/food_buff_calculator.json")
        self.buffs = {}
        for name in ("food", "chems", "alcohol", "nuka-cola", "bobbleheads", "magazines", "mutations", "scout-banners"):
            self.buffs[name] = S.dist(f"buffs/{name}.json", required=False) or {"items": []}
        self.builds = S.dist("builds/builds_data.json")
        self.cards = S.dist("curves/perk_cards.json", required=False) or {"perks": []}
        self.stations = S.dist("buff-stations.json", required=False) or {"items": []}
        self.healing = S.dist("calculators/healing_calculator.json", required=False)
        self._card_avs = None
        self.card_curves = {}

    # ---- food table (diet multipliers from the calculator) ----
    def food_table(self, stat):
        slot = stat["key"]
        if slot not in self.calc["rules"].get("slots", {}):
            return None
        rows = []
        for it in self.calc["items"]:
            if it.get("t") != "food":
                continue
            for f in it.get("fx", []):
                if f.get("s") != slot or f.get("neg"):
                    continue
                dm = f.get("dm") or {}
                base = f["m"]
                for diet, key in (("Carnivore", "c"), ("Herbivore", "h")):
                    mult = dm.get(key)
                    if mult is None and dm:
                        mult = [1.0, 1.0]      # other diet's dish, not touched by this diet
                    if mult is None:
                        mult = [1.0, 1.0]      # no diet keyword at all (milk, candy ...)
                    if mult[0] == 0:
                        continue               # this diet can't use it (x0)
                    menu = it.get("menu") or {}
                    rows.append({
                        "diet": diet, "dish": it["n"], "id": it["id"], "edid": it.get("edid", ""),
                        "durationSec": f.get("d", 0), "base": clean(base),
                        "mutation": clean(base * mult[0]), "sin": clean(base * mult[1]),
                        "bnb": menu.get("px") is not None, "price": menu.get("px"),
                        "seasonal": bool(menu.get("av")), "stacks": f.get("st", ""),
                    })
        if not rows:
            return {"rows": [], "empty": "No food buffs in game."}
        # Recommended = strongest per diet per duration bucket (60+ min / 30-59 min)
        def bucket(r):
            return "60" if r["durationSec"] >= 3600 else ("30" if r["durationSec"] >= 1800 else "short")

        def rank_key(r):
            return (-r["sin"], -r["base"], not r["bnb"], r["seasonal"], r["price"] if r["price"] is not None else 9999, r["dish"])
        best = {}
        for r in sorted(rows, key=rank_key):
            k = (r["diet"], bucket(r))
            if k[1] != "short" and k not in best:
                best[k] = r["id"]
        for r in rows:
            r["recommended"] = best.get((r["diet"], bucket(r))) == r["id"]
            r["category"] = r["diet"] + (" - Recommended" if r["recommended"] else " - Alternative")
        rows.sort(key=lambda r: (r["diet"] != "Carnivore", not r["recommended"], -r["durationSec"], -r["sin"], r["dish"]))
        stack_names = sorted({r["stacks"] for r in rows if r["stacks"]})
        return {"rows": rows, "stackGroups": stack_names,
                "mechanics": {"mutation": "×2 with the matching diet mutation (Carnivore: meat, Herbivore: veg / herb / fruit)",
                              "sin": "×2.5 with the mutation + Strange In Numbers in a team"}}

    # ---- ALCH-like items from the buff pages, matched by actor value ----
    def alch_items(self, stat, cat, kinds=("fortify",)):
        out = []
        for it in self.buffs[cat].get("items", []):
            hits = []
            for e in it.get("effects", []):
                m = self.R.mgef.get(e.get("edid", ""))
                if not m:
                    continue
                kind = mgef_kind(m)
                if e.get("polarity") == "debuff" or kind not in kinds:
                    continue
                if (e.get("magnitude") or 0) <= 0 and not e.get("magGlob"):
                    continue
                if page_wants(stat, m["av"], kind):
                    hits.append({"text": e.get("display") or e.get("name"), "duration": e.get("durationDisplay"),
                                 "magnitude": e.get("magnitude"), "kind": kind, "mgef": e.get("edid")})
            if hits:
                out.append({"name": it.get("name"), "id": it.get("formId"), "edid": it.get("edid"),
                            "image": it.get("imageUrl"), "effects": hits,
                            "other": [e.get("display") for e in it.get("effects", []) if e.get("display") not in {h["text"] for h in hits}][:6]})
        out.sort(key=lambda r: (-(r["effects"][0]["magnitude"] or 0), r["name"] or ""))
        return out

    # ---- perk cards ----
    def card_avs(self):
        """PCRD EDID -> {stat keys it boosts} (from abilities' actor values + mapped entry points)."""
        if self._card_avs is not None:
            return self._card_avs
        res = {}
        for c in self.R.pcrd:
            keys = set()
            perks = re.findall(r"\|[0-9A-F]{8}:([^:|]+):", c.get("Ranks_Flat") or "")
            for p in perks:
                for r in self.R.perkep.get(p, []):
                    if r.get("EffectType") == "Ability" and r.get("Ability_EDID"):
                        for fx in self.R.spel_fx.get(r["Ability_EDID"], []):
                            m = self.R.mgef.get(fx.get("EFID_MGEF_EDID", ""))
                            if not m:
                                continue
                            for st in self.stats:
                                if page_wants(st, m["av"], mgef_kind(m)):
                                    keys.add(st["key"])
                                    cv = (fx.get("CVTO_CURV_EDID") or "").strip()
                                    if cv:
                                        self.card_curves.setdefault(st["key"], {})[cv] = {
                                            "why": "Perk card " + (c.get("MNAM_MaleName") or c["PCRD_EDID"]),
                                            "x": self.R.av_name.get(fx.get("MAGA_AVIF_EDID") or "") or (fx.get("MAGA_AVIF_EDID") or "")}
                    for k in STAT_EP.get(r.get("EntryPoint") or "", []):
                        keys.add(k)
                for r in self.R.perk_by.get(p, []):
                    av = first_edid(r.get("EPF3_ActorValue"))
                    for st in self.stats:
                        if av and av in st["avs"]:
                            keys.add(st["key"])
            res[c["PCRD_EDID"]] = keys
        self._card_avs = res
        return res

    def perk_cards(self, stat, legendary=False):
        src = self.builds.get("legendaryPerks" if legendary else "perks", [])
        meta = {p["pcrdEdid"]: p for p in self.cards.get("perks", [])}
        avs = self.card_avs()
        out = []
        for p in src:
            pid = p.get("id", "")
            own = stat.get("special") and p.get("special") == stat["special"]
            boosts = stat["key"] in avs.get(pid, set())
            if not (own or boosts):
                continue
            m = meta.get(pid, {})
            out.append({"name": p.get("name"), "id": pid, "special": p.get("special"),
                        "minLevel": p.get("minLevel") or m.get("minLevel"), "race": p.get("race"),
                        "ranks": p.get("ranks", []), "boosts": bool(boosts),
                        "ghoulOnly": m.get("raceRestriction") == "Ghoul", "humanOnly": bool(m.get("humanOnly"))})
        out.sort(key=lambda r: (not r["boosts"], r["name"] or ""))
        return out

    # ---- legendary effects (armour / weapon stars) ----
    def legendary(self, stat):
        out = []
        for le in self.builds.get("legendaryEffects", []):
            recs = [r for r in le.get("records", []) if r.get("av") in stat["avs"]]
            if not recs:
                continue
            out.append({"name": le["name"], "star": le.get("star"), "weapon": le.get("weapon"),
                        "armour": le.get("armour"), "pa": le.get("pa"), "effect": le.get("effect"),
                        "detail": [r.get("desc") or r.get("name") for r in recs]})
        out.sort(key=lambda r: (r["star"] or 9, r["name"]))
        return out

    # ---- armour / power armour / backpack mods ----
    DMGT_STAT = {"dtPhysical": "dr", "dtEnergy": "er", "dtFire": "fire_res", "dtCryo": "cryo_res",
                 "dtFrost": "cryo_res", "dtPoison": "poison_res", "dtRadiationExposure": "rad_res"}

    def mods(self, stat):
        rows = {}
        bad = re.compile(r"(^test|^zzz|^CUT_|^cr[A-Z]|DLC01Bot|_NPC|Creature|^Dummy|Debug|^mod_Legendary)", re.I)
        R = self.R
        if not hasattr(R, "omod_props_by"):
            R.omod_props_by = {}
            for p in R.omod_props:
                R.omod_props_by.setdefault(p.get("OMOD_EDID", ""), []).append(p)

        def props_of(edid, seen):
            """A mod's own properties plus those of every mod it includes (Pocketed -> PARENT_..._Low)."""
            if edid in seen:
                return []
            seen.add(edid)
            out = list(R.omod_props_by.get(edid, []))
            for inc in re.findall(r"([A-Za-z_][\w]*)[^\[|]*\[OMOD:", (R.omod.get(edid) or {}).get("Includes_Flat") or ""):
                for q in props_of(inc, seen):
                    q = dict(q)
                    q["OMOD_EDID"] = edid
                    out.append(q)
            return out
        all_props = []
        for edid, o in R.omod.items():
            if (o.get("FULL") or "").strip():
                all_props.extend(props_of(edid, set()))
        for p in all_props:
            name = p.get("PropertyName", "")
            edid = p.get("OMOD_EDID", "")
            if bad.search(edid):
                continue
            o = R.omod.get(edid) or {}
            if (o.get("FormType") or "").strip() not in ("Armor", ""):
                continue
            full = re.sub(r"^TEMPLATE:\s*", "", (o.get("FULL") or "").strip())
            if not full or "MUL" in (p.get("FunctionType") or "") or "legendary" in (o.get("AttachPoint_Name") or "").lower():
                continue
            effects = []
            curve = first_edid(p.get("CurveTable")) if "CURV:" in (p.get("CurveTable") or "") else ""
            if name == "Enchantments":
                e = R.ench.get(first_edid(p.get("Value1")))
                if not e:
                    continue
                for fx in e["fx"]:
                    m = R.mgef.get(fx["mgef"])
                    if not m or "jetpack" in (m["full"] + fx["mgef"]).lower():
                        continue
                    if page_wants(stat, m["av"], mgef_kind(m)):
                        mag = fx["mag"] if not fx["curve"] else R.curve_max(fx["curve"])
                        if mag and mag > 0:
                            effects.append((R.label(m), mag, fx["curve"] or False))
            elif name in ("Actor Values", "ActorValues"):
                av = first_edid(p.get("Value1"))
                if av in stat["avs"]:
                    mag = R.curve_max(curve) if curve else fnum(p.get("Value2"), 0)
                    if mag and mag > 0:
                        effects.append((R.av_name.get(av) or av, mag, curve or False))
            elif name == "Damage Type Value":
                dt = first_edid(p.get("Value1"))
                if self.DMGT_STAT.get(dt) == stat["key"]:
                    mag = R.curve_max(curve) if curve else fnum(p.get("Value2"), 0)
                    if mag and mag > 0:
                        effects.append((stat["label"], mag, curve or False))
            for label, mag, from_curve in effects:
                if mag < 1 and stat["key"] != "heal_rate":
                    continue                      # multipliers / tiny paint effects, not flat stat points
                if full == "Random Legendary Mod":
                    continue
                kw = [k.replace("_", " ").strip() for k in
                      re.findall(r"ma_(?:armor_|Armor_)?([A-Za-z0-9_]+?)(?:\s|\"|$)", o.get("MNAM_TargetKWDs") or "")]
                key = (full, label, round(mag, 3))
                r = rows.setdefault(key, {"name": full, "effect": label, "magnitude": clean(mag), "upTo": bool(from_curve),
                                          "curve": from_curve or "",
                                          "slot": (o.get("AttachPoint_Name") or "").strip(), "fits": set(),
                                          "desc": (o.get("DESC") or "").strip(), "template": "TEMPLATE" in (o.get("FULL") or "")})
                r["fits"].update(k for k in kw if k.upper() != "TEMPLATE")
        out = []
        for r in rows.values():
            r["fits"] = sorted(r["fits"])[:24]
            pieces = {("Chest" if "Torso" in f else "Arms / legs" if re.search(r"Arm|Leg|Limb", f) else "") for f in r["fits"]} - {""}
            r["piece"] = " / ".join(sorted(pieces)) if len(pieces) == 1 else ""
            out.append(r)
        out.sort(key=lambda r: (-(r["magnitude"] or 0), r["name"]))
        return out

    # ---- every curve behind this stat (generated tables on the page) ----
    def curves(self, stat, formulas, boost):
        R = self.R
        found = {}
        if not hasattr(R, "curve_input"):
            curve_inputs(R)

        def add(edid, why, x=None, meta=None):
            edid = (edid or "").strip()
            if not edid or edid in found or not R.curves.get(edid):
                return
            found[edid] = {"why": why, "x": x, "meta": meta}
        for f in formulas:
            for i in f["inputs"].values():
                if "points" in i:
                    v0 = f["vars"][0]["label"] if f.get("vars") else None
                    add(i["src"]["edid"], "Formula: " + f["label"], v0)
        cards = {p["pcrdEdid"]: p for p in self.cards.get("perks", [])}
        listed = {r["id"]: r for b in boost if b["kind"] == "perks" for r in b["rows"]}
        for pid, row in listed.items():
            for cv in (cards.get(pid) or {}).get("curves", []):
                keyed_here = stat.get("special") and cv.get("xLabel") == stat["special"]
                if row.get("boosts") or keyed_here:
                    add(cv["edid"], "Perk card " + row["name"], None, cv)
        for cv, info in self.card_curves.get(stat["key"], {}).items():
            add(cv, info["why"], info["x"])
        for b in boost:
            for r in b.get("rows", []):
                if b["kind"] == "mods" and r.get("curve"):
                    add(r["curve"], "Mod " + r["name"])
                if b["kind"] == "spell" and r.get("curve"):
                    add(r["curve"], r.get("source", "") + " — " + (r.get("effect") or ""))
        for le in self.builds.get("legendaryEffects", []):
            for rec in le.get("records") or []:
                if rec.get("av") in stat["avs"] and rec.get("curve"):
                    add(rec["curve"], "Legendary effect " + le["name"])
        out = []
        for edid, info in found.items():
            m = info["meta"] or {}
            guessed = {"Player Level", "Input Value", "Input", "Rank", ""}
            ci = R.curve_input.get(edid)
            xl = (ci or {}).get("label") or info["x"] or (m.get("xLabel") if m.get("xLabel") not in guessed else None) or "Input"
            # output: the effect this curve drives ON THIS PAGE (Unyielding's one curve drives all six SPECIALs)
            outs = R.curve_output.get(edid) or []
            mine = [lab for av, lab in outs if av in stat["avs"] or av == stat["key"]]
            yl = (mine or [lab for _, lab in outs] or [None])[0]
            explain = (ci or {}).get("explain", "") or m.get("explain", "")
            out.append({"edid": edid, "formId": R.curv_ids.get(edid, ""), "why": info["why"],
                        "explain": explain,
                        "xFrom": (ci or {}).get("kind") or ("formula" if info["x"] else "unknown"),
                        "xSrc": (ci or {}).get("src"),
                        "xLabel": xl,
                        "yLabel": yl or (m.get("yLabel") if m.get("yLabel") not in ("Output Value", "Bonus Value", "Perk Bonus %") else None) or "Value",
                        "unit": "" if yl else m.get("unit", ""),
                        "points": R.curves[edid]})
        return out

    # ---- CAMP buff stations ----
    def camp(self, stat):
        want = {"str": "strength", "per": "perception", "end": "endurance", "cha": "charisma",
                "int": "intelligence", "agi": "agility", "luck": "luck", "ap_regen": "welltuned",
                "well_rested": "wellrested"}.get(stat["key"])
        if not want:
            return []
        out = []
        for it in self.stations.get("items", []):
            if want in (it.get("buffTypes") or []) and not it.get("cutContent"):
                out.append({"name": it.get("displayName"), "effect": it.get("outputInfo"),
                            "obtain": it.get("howToObtain"), "image": it.get("imageUrl")})
        out.sort(key=lambda r: r["name"] or "")
        return out

    # ---- abilities the game gives everyone (hunger / thirst / public teams) ----
    def spell_effects(self, stat, spells, label):
        out = []
        for sp in spells:
            for fx in self.R.spel_fx.get(sp, []):
                m = self.R.mgef.get(fx.get("EFID_MGEF_EDID", ""))
                if not m or not page_wants(stat, m["av"], mgef_kind(m)):
                    continue
                mag = fnum(fx.get("EFIT_Magnitude"), 0)
                curve = fx.get("CVTO_CURV_EDID") or ""
                if not mag and not curve:
                    continue
                out.append({"source": label, "spell": sp, "effect": self.R.label(m), "magnitude": clean(mag),
                            "upTo": self.R.curve_max(curve) if curve else None,
                            "curve": curve, "conditions": readable_conditions(fx.get("Conditions") or "", self.R)})
        return out


# ───────────────────────────── conditions -> words ─────────────────────────────

TIER_WORDS = {"FullyFed": "Fully Fed", "WellFed": "Well Fed", "Fed": "Fed", "PartiallyFed": "Partially Fed",
              "Hungry": "Hungry", "FullyHydrated": "Fully Hydrated", "WellHydrated": "Well Hydrated",
              "Hydrated": "Hydrated", "PartiallyHydrated": "Partially Hydrated", "Thirsty": "Thirsty"}


def _cond_text(p, R):
    m = re.search(r"GetIsPlayerGhoul\([^)]*\)\s*=\s*([\d.]+)", p)
    if m:
        return {"text": "Ghoul" if float(m.group(1)) else "Human (not a ghoul)"}
    m = re.search(r"GetValue\((SURV_Hunger|SURV_Thirst|GHL_SURV_Feral) \[[^\]]+\][^)]*\)\s*(>=|<=|<|>|=)\s*([A-Za-z_]\w*)", p)
    if m:
        bar = {"SURV_Hunger": "Hunger", "SURV_Thirst": "Thirst", "GHL_SURV_Feral": "Feral meter"}[m.group(1)]
        g = m.group(3)
        v, _ = R.gl(g)
        tier = re.sub(r"^(SURV_(New)?(Hunger|Thirst)|GHL_SURV_Feral)Threshold_\d_", "", g)
        word = {">=": "at or past", "<": "before", "<=": "at or before", ">": "past"}.get(m.group(2), m.group(2))
        return {"text": f"{bar} {word} {TIER_WORDS.get(tier, tier)}", "glob": g, "value": v, "bar": bar,
                "op": m.group(2), "tier": TIER_WORDS.get(tier, tier)}
    m = re.search(r"HasPerk\((\w+)[^)]*\)[^=]*=\s*([\d.]+)", p)
    if m:
        base = re.sub(r"0?(\d)$", "", m.group(1)).replace("GHL_", "")
        rk = re.search(r"(\d)$", m.group(1))
        nm = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", base) + (f" rank {rk.group(1)}" if rk else "")
        return {"text": ("Has " if float(m.group(2)) else "Does not have ") + nm, "perk": m.group(1), "has": bool(float(m.group(2)))}
    m = re.search(r"GetValue\(LGND_RejuvenatorTier[^)]*\)[^>]*>=\s*([\d.]+)", p)
    if m:
        return {"text": "Wearing Rejuvenator's power armour"}
    m = re.search(r"GetValue\(Rads[^)]*\)\s*(>=|<=|<|>)\s*([\d.]+)", p)
    if m:
        return {"text": f"Rads {m.group(1)} {clean(float(m.group(2)))}", "rads": [m.group(1), clean(float(m.group(2)))]}
    m = re.search(r"GetPlayerTeammateCount\([^)]*\)\s*(>=|<)\s*([\d.]+)", p)
    if m:
        return {"text": "In a team" if m.group(1) == ">=" else "Not in a team"}
    m = re.search(r"GetValuePercent\(Health[^)]*\)\s*(>=|<=|<|>)\s*([\d.]+)", p)
    if m:
        return {"text": f"Health {m.group(1)} {clean(float(m.group(2)) * 100)}%"}
    return {"text": re.sub(r"\s*\[[A-Z]{4}:[0-9A-F]{8}\]", "", re.sub(r"Subject\.", "", p))[:120], "raw": True}


def readable_conditions(text, R):
    """Condition list -> AND-list of OR-groups (Bethesda: consecutive ORs bind first)."""
    groups, cur = [], []
    for part in [p.strip() for p in str(text or "").split("|") if p.strip()]:
        op = "OR" if re.search(r"\sOR$", part) else "AND"
        p = re.sub(r"\s+(AND|OR)$", "", part)
        cur.append(_cond_text(p, R))
        if op == "AND":
            groups.append(cur)
            cur = []
    if cur:
        groups.append(cur)
    return groups


# ───────────────────────────── build ─────────────────────────────

def headline(s):
    """One generated line for the Overview card when a stat has no simple per-point value."""
    if s["perPoint"]:
        return None
    for f in s["formulas"]:
        if f.get("example") is None:
            continue
        v0 = f["vars"][0]
        return f"{f['label']}: {f['example']:g} {f['unit']} at {v0.get('default')} {v0['label']}"
    return None


def fill(text, consts):
    def rep(m):
        v = consts.get(m.group(1))
        if v is None:
            return m.group(0)
        if isinstance(v, float):
            v = round(v, 4)
        return str(v)
    return re.sub(r"\{(\w+)\}", rep, text)


def build(channel):
    S = Src(channel)
    R = Records(S)
    with open(os.path.join(DATA_DIR, "stats.json"), encoding="utf-8") as f:
        D = json.load(f)
    pages_path = os.path.join(DATA_DIR, "pages.json")
    P = {}
    if os.path.exists(pages_path):
        with open(pages_path, encoding="utf-8") as f:
            P = json.load(f)
    defs = {"units_per_foot": D["units_per_foot"]}
    stats_def = D["stats"]
    B = Boost(S, R, stats_def)

    stats = {}
    for sd in stats_def:
        k = sd["key"]
        resist = sd.get("resist")
        consts, lim_src = {}, {}
        for name, ref in (sd.get("limits") or {}).items():
            r = R.resolve(ref, defs)
            consts[name] = r.get("value")
            lim_src[name] = r["src"]
        fdefs = sd.get("formulas") or []
        if fdefs == "resist":
            fdefs = [dict(D["resistFormula"], id="resist")]
        formulas = []
        per_point = []
        for fd in fdefs:
            inputs = {n: R.resolve(ref, defs, resist) for n, ref in fd.get("inputs", {}).items()}
            f = {"id": fd["id"], "label": fd["label"], "expr": fd["expr"], "unit": fd.get("unit", ""),
                 "decimals": fd.get("decimals", 2), "vars": fd["vars"], "inputs": inputs}
            if fd.get("note"):
                f["note"] = fill(fd["note"], consts)
            missing = [n for n, i in inputs.items() if i.get("src", {}).get("missing")]
            if missing:
                f["missing"] = missing
            else:
                f["table"] = table_rows(f)
                f["example"] = round(evaluate(fd["expr"], inputs, {v["name"]: v.get("default", 0) for v in fd["vars"]}), fd.get("decimals", 2) + 1)
            formulas.append(f)
            pp = fd.get("perPoint")
            if pp and not missing:
                if "input" in pp:
                    val = (inputs[pp["input"]]["value"] or 0) * pp.get("scale", 1)
                    src = inputs[pp["input"]]["src"]
                else:
                    val = evaluate(pp["expr"], inputs, {})
                    src = {"type": "formula", "of": [i["src"].get("edid") for i in inputs.values()]}
                per_point.append({"label": pp["unit"], "value": round(val, 4), "src": src, "formula": fd["id"]})
        settings = []
        for s in sd.get("settings", []):
            r = R.resolve(s["input"], defs)
            settings.append({"label": s["label"], "value": r.get("value"), "src": r["src"]})

        boost = []
        ft = B.food_table(sd)
        if ft is not None:
            boost.append({"id": "food", "title": "Food Buffs", "kind": "food-table", **ft})
        elif sd["key"] not in ("cha",):
            extra = B.alch_items(sd, "food")
            if extra:
                boost.append({"id": "food", "title": "Food Buffs", "kind": "items", "rows": extra})
        if sd["key"] == "heal_rate":
            heal = B.alch_items(sd, "food", kinds=("restore",))
            if heal:
                boost.append({"id": "food-heal", "title": "Food That Heals Over Time", "kind": "items", "rows": heal,
                              "blurb": "These restore health a little every second for a short time — and they keep working in combat."})
        if sd["key"] == "ap":
            refill = B.alch_items(sd, "food", kinds=("restore",)) + B.alch_items(sd, "alcohol", kinds=("restore",)) + B.alch_items(sd, "nuka-cola", kinds=("restore",))
            if refill:
                boost.append({"id": "ap-refill", "title": "AP Refill", "kind": "items", "rows": refill,
                              "blurb": "Instantly refill your AP bar. AP refills stack — you can take several in a row."})
        for cat, title in (("chems", "Chems"), ("alcohol", "Alcohol"), ("nuka-cola", "Nuka-Cola & Drinks"),
                           ("mutations", "Mutations"), ("bobbleheads", "Bobbleheads"), ("magazines", "Magazines"),
                           ("scout-banners", "Pioneer Scout Banners")):
            rows = B.alch_items(sd, cat)
            if rows:
                boost.append({"id": cat, "title": title, "kind": "items", "rows": rows})
        pc = B.perk_cards(sd)
        if pc:
            boost.append({"id": "perks", "title": ("All " + sd["label"] + " Perk Cards") if sd.get("special") else "Perk Cards",
                          "kind": "perks", "rows": pc})
        lpc = B.perk_cards(sd, legendary=True)
        if lpc:
            boost.append({"id": "legendary-perks", "title": "Legendary Perk Cards", "kind": "perks", "rows": lpc})
        lg = B.legendary(sd)
        if lg:
            boost.append({"id": "legendary", "title": "Legendary Effects (Armour & Weapons)", "kind": "legendary", "rows": lg})
        md = B.mods(sd)
        if md:
            boost.append({"id": "mods", "title": "Armour, Power Armour & Backpack Mods", "kind": "mods", "rows": md})
        cp = B.camp(sd)
        if cp:
            boost.append({"id": "camp", "title": "CAMP Buff Stations", "kind": "camp", "rows": cp})
        ht = B.spell_effects(sd, ["SURV_Hunger_Ability", "SURV_Thirst_Ability"], "Hunger & Thirst")
        if ht:
            boost.append({"id": "hunger-thirst", "title": "Hunger & Thirst Bars", "kind": "spell", "rows": ht,
                          "link": P.get("hunger", {}).get("path")})
        teams = [s for s in R.spel_fx if s.startswith("PT_") and s.endswith("TeamBonus")]
        tm = B.spell_effects(sd, sorted(teams), "Public Team")
        if tm:
            boost.append({"id": "teams", "title": "Public Teams", "kind": "spell", "rows": tm})

        stats[k] = {
            "key": k, "label": sd["label"], "group": sd["group"], "special": sd.get("special"),
            "path": sd["path"], "title": sd["title"], "h1": D["category"] + " - " + sd["title"],
            "blurb": sd["blurb"], "avs": sd["avs"], "isNew": bool(sd.get("isNew")),
            "calcSlot": k if k in B.calc["rules"].get("slots", {}) else None,
            "intro": fill(sd.get("intro", ""), consts),
            "details": [fill(t, consts) for t in sd.get("details", [])],
            "constants": {n: {"value": consts[n], "src": lim_src[n]} for n in consts},
            "perPoint": per_point, "formulas": formulas, "settings": settings,
            "observed": sd.get("observed"), "boost": boost, "curves": B.curves(sd, formulas, boost),
        }

    # ---- goals for the food buff calculator ----
    goals = {}
    for gk, g in D["goals"].items():
        out = {"label": g["label"], "unit": g["unit"], "why": g.get("why", ""), "slots": {}}
        for slot, rule in g["slots"].items():
            if rule.get("from") == "formula":
                f = next((x for x in stats[rule["stat"]]["formulas"] if x["id"] == rule["formula"]), None)
                inp = (f or {}).get("inputs", {}).get(rule["per"], {})
                v = inp.get("value")
                if v is not None and rule.get("scale"):
                    v = v * rule["scale"]
                out["slots"][slot] = {"per_point": v, "unit": rule.get("unit", g["unit"]), "src": inp.get("src"),
                                      "explain": f"1 {stats[rule['stat']]['label']} = {clean(v) if v is not None else '?'}"
                                                 + ("" if str(rule.get('unit', g['unit'])).startswith('%') else " ")
                                                 + f"{rule.get('unit', g['unit'])}"}
            elif "curve" in rule:
                pts, src = R.curve(rule["curve"])
                out["slots"][slot] = {"curve": pts, "unit": rule.get("unit"), "src": src}
            elif "per_point_of_max_hp" in rule:
                cm, csrc = R.g(rule["combat"].split(":", 1)[1])
                out["slots"][slot] = {"per_point_of_max_hp": rule["per_point_of_max_hp"], "combatMult": cm, "combatSrc": csrc,
                                      "explain": "1 Heal Rate = 1% of max HP per second, out of combat only"}
            else:
                out["slots"][slot] = {"per_point": rule.get("per_point", 1), "unit": rule.get("unit", g["unit"])}
        if g.get("healing_calculator") and B.healing:
            out["healingCalculator"] = {"file": "dist/calculators/healing_calculator.json",
                                        "sections": [s for s in g["healing_calculator"] if s in B.healing.get("sources", {})]}
        goals[gk] = out
    hp = stats["end"]["formulas"][0]
    goals["health_bar"]["base"] = {"expr": hp["expr"], "inputs": hp["inputs"],
                                   "explain": "Max HP with no buffs = player + race + Endurance × per point"}

    # ---- hunger & thirst page ----
    hunger = None
    if P.get("hunger"):
        hp_ = P["hunger"]
        tiers = []
        for bar, pre in (("Hunger", "SURV_NewHungerThreshold_"), ("Thirst", "SURV_NewThirstThreshold_")):
            for g in sorted(e for e in R.glob if e.startswith(pre)):
                v, src = R.gl(g)
                tiers.append({"bar": bar, "tier": TIER_WORDS.get(re.sub(r"^\w+_\d_", "", g), g), "value": v, "src": src})
        effects = []
        for sp in ("SURV_Hunger_Ability", "SURV_Thirst_Ability"):
            for fx in R.spel_fx.get(sp, []):
                m = R.mgef.get(fx.get("EFID_MGEF_EDID", ""), {})
                mag = fnum(fx.get("EFIT_Magnitude"), 0)
                if m.get("av") in ("SURV_Hunger", "SURV_Thirst"):
                    continue
                effects.append({"bar": "Hunger" if "Hunger" in sp else "Thirst", "effect": R.label(m) or fx.get("EFID_MGEF_EDID"),
                                "upTo": R.curve_max(fx.get("CVTO_CURV_EDID")) if fx.get("CVTO_CURV_EDID") else None,
                                "curvePoints": R.curves.get(fx.get("CVTO_CURV_EDID") or "") or None,
                                "av": m.get("av"), "magnitude": clean(mag), "curve": fx.get("CVTO_CURV_EDID") or "",
                                "conditions": readable_conditions(fx.get("Conditions") or "", R),
                                "src": {"type": "SPEL", "edid": sp, "formId": fx.get("SPEL_FormID", "")}})
        feral = []
        for fx in R.spel_fx.get("GHL_SURV_Feral_Ability", []):
            m = R.mgef.get(fx.get("EFID_MGEF_EDID", ""), {})
            if not m.get("av") or m.get("av") in ("GHL_SURV_Feral",) or not fnum(fx.get("EFIT_Magnitude"), 0):
                continue
            feral.append({"effect": R.label(m) or fx.get("EFID_MGEF_EDID"), "magnitude": clean(fnum(fx.get("EFIT_Magnitude"), 0)),
                          "conditions": readable_conditions(fx.get("Conditions") or "", R)})
        hunger = dict(hp_, tiers=tiers, effects=effects, feral=feral,
                      h1=D["category"] + " - " + hp_["title"])

    overview = None
    if P.get("overview"):
        ov = P["overview"]
        overview = dict(ov, h1=D["category"] + " - " + ov["title"],
                        stats=[{"key": s["key"], "label": s["label"], "group": s["group"], "path": s["path"],
                                "perPoint": s["perPoint"][:3], "headline": headline(s)} for s in stats.values()])

    pages = [{"path": s["path"], "kind": "stat", "key": k} for k, s in stats.items()]
    if hunger:
        pages.append({"path": hunger["path"], "kind": "hunger"})
    if overview:
        pages.append({"path": overview["path"], "kind": "overview"})

    out = {
        "version": 1,
        "generated": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        "channel": channel,
        "category": D["category"],
        "sources": S.used,
        "player": {
            "baseHealth": {n: R.resolve(r, defs) for n, r in (("player", {"prps": ["NPC_", "Player", "Health"]}),
                                                               ("race", {"prps": ["RACE", "HumanRace", "Health"]}),
                                                               ("perEndurance", {"gmst": "fAVDHealthStartEndMult"}),
                                                               ("levelMult", {"gmst": "fStatsHealthLevelMult"}))},
            "special": {"min": R.avif("Strength", "NAM5")[0], "max": R.avif("Strength", "NAM6")[0]},
        },
        "pages": pages,
        "stats": stats,
        "goals": goals,
        "hunger": hunger,
        "overview": overview,
        "warnings": WARNINGS,
    }
    dist = os.path.join(REPO, "dist", "pts" if channel == "pts" else "")
    os.makedirs(dist, exist_ok=True)
    path = os.path.join(dist, "specials_stats.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[specials_stats] wrote {path} ({len(stats)} stats, {os.path.getsize(path)//1024} KB, {len(WARNINGS)} warnings)")
    if write_empty_patchlog_feed and channel == "live":
        try:
            write_empty_patchlog_feed(dist, "patchlog_latest_bnb_specials_stats.json", current_count=len(stats))
        except Exception as e:  # pragma: no cover
            warn(f"patchlog feed not written: {e}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args()
    build("pts" if a.pts else "live")


if __name__ == "__main__":
    main()
