"""
boss_mechanics.py - "Special mechanics" for the Head Hunt Boss Stats pages
(22 Nov 2026). Read by build_boss_stats_json.py.

Everything comes from the NPCSP export (tools/export_from_esm.py NPCSP):
  _ScriptProps     the values a boss's script is given on its record (VMAD)
  _ScriptDefaults  the client .pex defaults for any property the record leaves
                   unset, + ClientCodeLines (0 = stub: the script body runs on
                   Bethesda's servers)
  _Abilities / _Perks         what each actor carries (template Use Spell List followed)
  _Spells / _PerkEntries      every effect / perk entry they reach, incl. the spell an
                              aura (cloak) puts on others
  _Messages / _Objects        screen messages, spawned actors, devices, hazards
plus MGEF (effect names) and ENCH (what applies an effect) for conditions.

The WORDS are below, one small function per script / perk / ability EDID.
Every number in them is read from those files. A script, perk or ability on a
Head Hunt actor with no wording here is printed as a build warning (and gets
a plain fallback line) so a new patch can't slip one past unnoticed.

FLAGGED: things that look like a bug go into data["reviewFlags"] and the bug
backlog, never onto the page as fact (Amadi's x3 damage, Karen's hit spell).
"""
from __future__ import annotations

import csv
import re

csv.field_size_limit(2**31 - 1)

SWITCH_GLOB = "Burn_Bounty_HeadHuntEffects"

IGNORE_PERKS = {"crModNormalizedLevelPerk_25", "crNoFallDamage"}
IGNORE_SPELLS = {"RTSV_Bounty_Stabitha_VFX_SP", "RTSV_Bounty_CultistTier_VFX_SP"}
IGNORE_SCRIPTS = set()


# ── small helpers ──────────────────────────────────────────────────────────

def _rows(path):
    if not path:
        return []
    with open(path, encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fmt(v, dp=2):
    """1.0 -> '1', 1.5 -> '1.5', 5000 -> '5,000'."""
    if v is None:
        return "?"
    if abs(v - round(v)) < 1e-9:
        return f"{int(round(v)):,}"
    return f"{v:,.{dp}f}".rstrip("0").rstrip(".")


def mult(v):
    return "×" + fmt(v)


def pct_of(v):
    """0.01 -> '1%', 0.3 -> '30%'."""
    return fmt(v * 100) + "%"


def and_list(items):
    items = [x for x in items if x]
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def or_list(items):
    items = [x for x in items if x]
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " or " + items[-1]


NUM_WORDS = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six"}


IRREGULAR = {"Wolf": "Wolves"}


def plural(name, n=2):
    if n == 1 or not name:
        return name
    last = name.split(" ")[-1]
    if last in IRREGULAR:
        return name[: -len(last)] + IRREGULAR[last]
    return name if name.endswith("s") else name + "s"


def camel_words(s):
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s).strip()


# ── perk-entry conditions (from PERKEP Raw) ────────────────────────────────

OPS = {"=": "=", ">=": ">=", "<=": "<=", ">": ">", "<": "<", "!=": "!="}


def op_from_type(b):
    if b[0] == "1" and b[1] == "1":
        return ">="
    if b[0] == "1" and b[2] == "1":
        return "<="
    if b[0] == "1":
        return "="
    if b[1] == "1":
        return ">"
    if b[2] == "1":
        return "<"
    return "!="


def parse_conds(raw):
    """[{tab, op, or, func, param, value}] from a PERKEP Raw cell."""
    out, tab, cur = [], 0, None
    for tok in (raw or "").split(" ;; "):
        k, _, v = tok.partition("=")
        if k == "PRKC - Run On (Tab Index)":
            tab = int(v)
        elif k == "Type" and re.fullmatch(r"[01]{8}", v):
            cur = {"tab": tab, "op": op_from_type(v), "or": v[3] == "1", "func": "", "param": "", "value": None}
            out.append(cur)
        elif cur is not None:
            if k == "Comparison Value":
                cur["value"] = num(v) if num(v) is not None else v
            elif k == "Function":
                cur["func"] = v
            elif k in ("Actor Value", "Parameter #1") and not cur["param"]:
                cur["param"] = v
    return out


def ref_edid(s):
    return (s or "").split(" [", 1)[0].strip()


# ── data access ────────────────────────────────────────────────────────────

class Mech:
    def __init__(self, pick):
        g = lambda suf, req=False: pick("NPCSP_" + suf, f"NPCSP_Export_*_{suf}.tsv", required=req)
        self.props, self.defaults, self.code = {}, {}, {}
        for r in _rows(g("ScriptProps", True)):
            self.props.setdefault(r["NPCEDID"], {}).setdefault(r["Script"], []).append(r)
        for r in _rows(g("ScriptDefaults")):
            self.code[r["Script"]] = int(r["ClientCodeLines"] or 0)
            if r["Property"]:
                self.defaults.setdefault(r["Script"], {})[r["Property"]] = r["Default"]
        self.abil, self.perks = {}, {}
        for r in _rows(g("Abilities")):
            self.abil.setdefault(r["NPCEDID"], []).append(r["SpellEDID"])
        for r in _rows(g("Perks")):
            self.perks.setdefault(r["NPCEDID"], []).append(r["PerkEDID"])
        self.spells, self.spell_name = {}, {}
        for r in _rows(g("Spells")):
            self.spells.setdefault(r["SpellEDID"], []).append(r)
            self.spell_name[r["SpellEDID"]] = r["SpellName"]
        self.pe = {}
        for r in _rows(g("PerkEntries")):
            if not any(x["EffectIndex"] == r["EffectIndex"] for x in self.pe.get(r["PERK_EDID"], [])):
                r = dict(r, conds=parse_conds(r.get("Raw", "")), val=num(r.get("Value")))
                self.pe.setdefault(r["PERK_EDID"], []).append(r)
        self.msgs = {r["MESGEDID"]: (r["DESC"] or r["FULL"]) for r in _rows(g("Messages"))}
        self.msg_full = {r["MESGEDID"]: r["FULL"] for r in _rows(g("Messages"))}
        self.objs = {r["ObjectEDID"]: r for r in _rows(g("Objects"))}
        self.mgef = {r["EDID"]: r for r in _rows(pick("MGEF", "MGEF_Export_*.tsv", exclude="Explosions", required=False))}
        self.ench_by_mgef = {}
        for r in _rows(pick("ENCH", "ENCH_Export_*.tsv", required=False)):
            for i in range(1, 10):
                e = r.get(f"Effect_{i}_MGEF_EID")
                if e:
                    self.ench_by_mgef.setdefault(e, []).append((r["ENCH_FULL"], num(r.get(f"Effect_{i}_Duration"))))
        self.glob = {}
        self.warnings = []

    # script props ---------------------------------------------------------
    def scripts(self, npc):
        return list(self.props.get(npc, {}))

    def prop(self, npc, script, name):
        rows = [r for r in self.props.get(npc, {}).get(script, []) if r["Property"] == name]
        if rows:
            r = rows[0]
            if r["Type"] == "obj":
                return r
            return num(r["Value"]) if num(r["Value"]) is not None else r["Value"]
        d = self.defaults.get(script, {}).get(name)
        if d is None:
            return None
        if d in ("True", "False"):
            return d == "True"
        return num(d) if num(d) is not None else d

    def any_arr(self, script, name):
        """The same script property from whichever actor sets it (a boss's
        side-kicks can carry the list the boss's script uses)."""
        for npc, scr in self.props.items():
            if any(r["Property"] == name for r in scr.get(script, [])):
                return self.arr(npc, script, name)
        return []

    def arr(self, npc, script, name):
        rows = [r for r in self.props.get(npc, {}).get(script, []) if r["Property"] == name]
        rows.sort(key=lambda r: int(r["Index"] or 0))
        return [r if r["Type"] == "obj" else num(r["Value"]) for r in rows]

    def obj_edid(self, r):
        return (r or {}).get("Value", "").split(":")[1] if r and r.get("Type") == "obj" else ""

    def glob_val(self, r):
        return num((r or {}).get("GlobValue"))

    def markers(self, npc, script):
        return sum(1 for r in self.props.get(npc, {}).get(script, [])
                   if re.match(r"Burn_Bounty_PublicPlacementMarker\d+$", r["Property"]))

    def server_side(self, script):
        return self.code.get(script, 0) == 0

    # spells / perks -------------------------------------------------------
    def eff(self, spell, i=0):
        rows = self.spells.get(spell, [])
        for r in rows:
            if int(r["EffectIndex"]) == i:
                return r
        return None

    def assoc_spell(self, spell):
        r = self.eff(spell)
        a = (r or {}).get("AssocItem", "")
        return a.split(":")[1] if a.endswith(":SPEL") else ""

    def ptap(self, spell):
        r = self.eff(spell)
        return ((r or {}).get("PerkToApply") or ":").split(":")[1]

    def entries(self, perk):
        return self.pe.get(perk, [])

    def has_switch(self, perk=None, spell=None):
        if perk:
            return any(c["func"] == "GetGlobalValue" for e in self.entries(perk) for c in e["conds"]) \
                and SWITCH_GLOB in " ".join(e.get("Raw", "") for e in self.entries(perk))
        if spell:
            return any(SWITCH_GLOB in (r.get("Conditions") or "") for r in self.spells.get(spell, []))
        return False

    def mgef_name(self, edid):
        return (self.mgef.get(edid) or {}).get("FULL", "") or edid

    def perk_spell(self, perk):
        for e in self.entries(perk):
            m = re.search(r"Spell=(\w+) \[SPEL", e.get("Raw", ""))
            if m:
                return m.group(1)
            if e.get("Ability_EDID"):
                return e["Ability_EDID"]
        return ""

    def who_applies(self, spell):
        """NPC perks whose entry applies this spell."""
        return [p for p, es in self.pe.items() if any(f"Spell={spell} [" in e.get("Raw", "") for e in es)]


# ── wording: one function per EDID ─────────────────────────────────────────
# Each returns {"title", "lines": [[label, text], ...], "note"?, "switch"?}

def _th(vals):
    return and_list([fmt(v) + "%" for v in vals if v is not None])


def s_underminer(M, npc, S):
    th = M.arr(npc, S, "HealthThresholds")
    n = M.prop(npc, S, "MoleRatsToSpawn")
    t = M.prop(npc, S, "TimeFromTelegraphToUndermine")
    msg = M.msgs.get(M.obj_edid(M.prop(npc, S, "RTSV_BountyMessage_Underminer")), "")
    quake = M.obj_edid(M.prop(npc, S, "Burn_E02_EarthquakeSpell"))
    qd = num((M.eff(quake) or {}).get("Duration"))
    rat = M.objs.get(M.obj_edid(M.prop(npc, S, "RD01_Enc02_D3_MoleRatBomb")), {})
    mk = M.markers(npc, S)
    lines = [["When", f"at {_th(th)} of his health, so {NUM_WORDS.get(len(th), len(th))} waves in a full fight."]]
    warn = []
    if msg:
        warn.append(f'a screen message, "{msg}"')
    if qd:
        warn.append(f"a {fmt(qd)}-second camera shake")
    w = "Warning first: " if False else ""
    lines.append(["Warning first", w + ", plus ".join(warn) + (f". The waves hit {fmt(t)} seconds after the warning." if t else ".")])
    spawn = f"{fmt(n)} {plural(rat.get('Name') or 'mole rats', n)} per wave"
    if mk:
        spawn += f", at the hunt's {NUM_WORDS.get(mk, mk)} spawn markers around the area"
    spawn += "."
    if rat.get("ObjectEDID", "").startswith("RD01_Enc02"):
        spawn += (" These are the same bomb-vest mole rats as the drill section of the Gleaming Depths raid,"
                  " so they run at you and blow up.")
    lines.append(["What spawns", spawn])
    return {"title": "Mole rat waves", "lines": lines}


def s_jammer(M, npc, S):
    th = M.arr(npc, S, "HealthThresholds")
    n = M.prop(npc, S, "NumberOfGeneratorsToPlace")
    dev = M.objs.get(M.obj_edid(M.prop(npc, S, "RTSV_BountyObject_Jammer")), {}).get("Name") or "jammer"
    msg = M.msgs.get(M.obj_edid(M.prop(npc, S, "RTSV_BountyMessage_Jammer")), "")
    end = M.msgs.get(M.obj_edid(M.prop(npc, S, "RTSV_BountyMessage_JammerEnd")), "")
    jam = M.prop(npc, S, "JamDuration")
    sp = M.obj_edid(M.prop(npc, S, "InhibitingSpell"))
    e = M.eff(sp) or {}
    perk = (e.get("PerkToApply") or ":").split(":")[1]
    cut = next((x["val"] for x in M.entries(perk) if x["EntryPoint"] == "Mod Weapon Attack Damage"), None)
    lines = [["When", f"at {_th(th)} of his health."]]
    if msg:
        lines.append(["Warning", f'a screen message, "{msg}"'])
    lines.append(["What spawns", f"{fmt(n)} {plural(dev, n)} around the hunt area."])
    eff = f'while they work, {M.spell_name.get(sp) or "an effect"}'
    if e.get("MGEF_Desc"):
        eff += f' ("{e["MGEF_Desc"]}")'
    if cut is not None:
        eff += f" cuts your weapon damage to {pct_of(cut)} of normal"
    if num(e.get("Duration")):
        eff += f"; each dose lasts {fmt(num(e['Duration']))} seconds"
    lines.append(["Effect", eff + "."])
    if jam:
        lines.append(["How long", f"up to {fmt(jam)} seconds" + (f', then "{end}"' if end else "") + "."])
    return {"title": "Damage jammers", "lines": lines,
            "note": "How the devices are placed, and whether destroying them ends the jam early, is handled on Bethesda's servers."}


def s_winter(M, npc, S):
    th = M.arr(npc, S, "HealthPercentageThresholds")
    laser = M.prop(npc, S, "LaserDuration")
    return {"title": "Orbital strikes", "lines": [
        ["When", f"at {_th(th)} of his health."],
        ["Warning first", f"an alarm sounds and a targeting laser runs for {fmt(laser)} seconds, then the strike explodes."],
    ], "note": "Where the strikes land is decided on Bethesda's servers."}


def s_weather(M, npc, S):
    if M.prop(npc, S, "isLackey"):
        return {"title": "Lightning channel", "lines": [["Effect", "the side-kick half of the boss's lightning mechanic."]]}
    msg = M.msgs.get(M.obj_edid(M.prop(npc, S, "RTSV_BountyMessage_Weather")), "")
    ch = M.prop(npc, S, "ChannelTime")
    stag = M.prop(npc, S, "DamageRequiredToStagger")
    buff = M.prop(npc, S, "BuffTime")
    cs = M.obj_edid(M.prop(npc, S, "LightningCastingSpell"))
    dr = next((num(r["Magnitude"]) for r in M.spells.get(cs, []) if r["ActorValue"] == "DamageResist"), None)
    bs = M.obj_edid(M.prop(npc, S, "LightningBuffSpell"))
    bperk = M.ptap(bs)
    bm = next((x["val"] for x in M.entries(bperk) if x["EntryPoint"] == "Mod Weapon Attack Damage"), None)
    lines = []
    if msg:
        lines.append(["Warning", f'a screen message, "{msg}"'])
    c = f"a channel takes {fmt(ch)} seconds"
    if dr:
        c += f"; the caster glows blue and gains {fmt(dr)} Damage Resistance while channelling"
    if stag:
        c += f". Deal {fmt(stag)} damage to stagger it"
    lines.append(["Channel", c + "."])
    if buff:
        lines.append(["Weather Buff", f"lasts {fmt(buff)} seconds" + (f" and gives {mult(bm)} weapon damage" if bm else "") + "."])
    return {"title": "Lightning crew", "lines": lines,
            "note": "Who channels, who gets the buff and when it starts are decided on Bethesda's servers."}


def s_florist(M, npc, S):
    n = M.prop(npc, S, "NumberOfFlowersToPlace")
    hz = M.objs.get(M.obj_edid(M.prop(npc, S, "RTSV_BountyHazard_Florist")), {})
    sp = (re.search(r"Spell=(\w+) \[", hz.get("Details", "")) or [None, ""])[1]
    e = M.eff(sp) or {}
    lines = [["What spawns", f"{fmt(n)} flower patches scattered around the fight."]]
    if e:
        lines.append(["Effect", f"standing in one deals {fmt(num(e['Magnitude']))} poison damage per second"
                      + (f' ({M.spell_name.get(sp) or e.get("MGEF_Name")})' if M.spell_name.get(sp) or e.get("MGEF_Name") else "") + "."])
    return {"title": "Poison flower patches", "lines": lines,
            "note": "When the patches appear is decided on Bethesda's servers."}


def s_captain(M, npc, S):
    ranks = [M.msg_full.get(M.obj_edid(r), "") for r in (M.arr(npc, S, "RankNamesOrdered") or M.any_arr(S, "RankNamesOrdered"))]
    weps = []
    for r in (M.arr(npc, S, "PirateWeapons") or M.any_arr(S, "PirateWeapons")):
        m = re.search(r"Weapon_Ranged_(\w+?)_Captain", M.obj_edid(r))
        w = camel_words(m.group(1)) if m else ""
        if w and w not in weps:
            weps.append(w)
    msg = M.msgs.get(M.obj_edid(M.prop(npc, S, "PromotionMessage")), "")
    sh = M.obj_edid(M.prop(npc, S, "PromotionShader"))
    shd = num((M.eff(sh) or {}).get("Duration"))
    if not M.prop(npc, S, "IsFirstCaptain"):
        txt = "can be promoted"
        if weps:
            txt += f" and pirate weapon list ({or_list(weps)})"
        return {"title": "Field promotions", "lines": [["Script", txt + "."]]}
    bits = []
    if msg:
        bits.append(f'a screen message, "{msg}"')
    bits.append("a pirate hat")
    if shd:
        bits.append(f"a {fmt(shd)}-second glow")
    if weps:
        bits.append(f"a new weapon ({or_list(weps)})")
    lines = [["Promotion", "when a ranked pirate dies, a Crewmate is promoted: " + and_list(bits) + "."]]
    if any(ranks):
        lines.append(["Ranks", "top down:", [x for x in ranks if x]])
    return {"title": "Field promotions", "lines": lines,
            "note": "The exact promotion order is decided on Bethesda's servers."}


def s_cultist(M, npc, S):
    perks = [M.obj_edid(r) for r in M.arr(npc, S, "ResistancePerks")]
    vals = []
    for p in perks:
        v = next((x["val"] for x in M.entries(p) if x["EntryPoint"] == "Mod Incoming Weapon Damage"), None)
        vals.append(v)
    red = M.msgs.get(M.obj_edid(M.prop(npc, S, "BossResistanceReduced")), "")
    rem = M.msgs.get(M.obj_edid(M.prop(npc, S, "BossResistanceRemoved")), "")
    lines = []
    if any(v is not None for v in vals):
        v = sorted(x for x in vals if x is not None)
        lines.append(["Shield", "while his disciples are alive he takes reduced weapon and explosion damage, stepping up as they die off:",
                      [f"{pct_of(x)} of normal damage" for x in v]])
    if red:
        lines.append(["Each disciple death", f'"{red}"'])
    if rem:
        lines.append(["All disciples dead", f'"{rem}"'])
    return {"title": "Disciple shield", "lines": lines,
            "note": f"There are {NUM_WORDS.get(len(vals), len(vals))} shield levels, tied to how many disciples are alive; "
                    "the switch between them is handled on Bethesda's servers."}


def s_eliminator(M, npc, S):
    lk = M.objs.get(M.obj_edid(M.prop(npc, S, "LackeyToSpawn")), {}).get("Name") or "lackeys"
    mx = M.glob_val(M.prop(npc, S, "MaxLackeys"))
    extra = M.prop(npc, S, "EnableExtraLackeysOnPlayerDown")
    lines = [["Backup", f"he brings in {plural(lk)}" + (f", up to {fmt(mx)} at a time" if mx else "") + "."]]
    if extra:
        lines.append(["Player down", "extra ones can spawn when a player goes down."])
    return {"title": f"{plural(lk)} backup", "lines": lines,
            "note": "The spawn timing is decided on Bethesda's servers."}


def s_special_agent(M, npc, S):
    return {"title": "Special Agent Damage", "lines": [
        ["Script", "a Special Agent Damage effect that watches his health."]],
        "note": "What it does is handled on Bethesda's servers, so we can't say yet."}


def s_target_rich(M, npc, S):
    return {"title": "Target the Rich", "lines": [
        ["Effect", "Target the Rich, which reads the Caps you carry."]],
        "switch": True}


def s_bluefang(M, npc, S):
    fear = M.prop(npc, S, "ChanceToFearOnHit")
    crip = M.prop(npc, S, "ChanceToCrippleOnHit")
    if M.prop(npc, S, "IsWolf"):
        return {"title": "Crippling bite", "lines": [["On hit", f"{fmt(crip)}% chance to cripple one of your legs."]],
                "note": "The roll is made on Bethesda's servers."}
    return {"title": "Blue howl", "lines": [["On hit", f"{fmt(fear)}% chance to let out a blue howl that makes you flee in fear."]],
            "note": "The roll is made on Bethesda's servers."}


SCRIPTS = {
    "RTSV_BountyScript_Underminer": s_underminer,
    "RTSV_BountyScript_Jammer": s_jammer,
    "RTSV_BountyScript_Winter": s_winter,
    "RTSV_BountyScript_Weather": s_weather,
    "RTSV_BountyScript_Florist": s_florist,
    "RTSV_BountyScript_Captain": s_captain,
    "RTSV_BountyScript_CultistDefense": s_cultist,
    "RTSV_BountyScript_Eliminator": s_eliminator,
    "RTSV_BountyScript_SpecialAgentDamage": s_special_agent,
    "Burn_Bounty_TargetTheRich_Script": s_target_rich,
    "Burn_Bounty_BlueFangIntimidation": s_bluefang,
}


# perks ---------------------------------------------------------------------

def _bound(conds, func="GetValue"):
    lo = hi = None
    for c in conds:
        if c["func"] != func or c["tab"] != 2 or not isinstance(c["value"], float):
            continue
        if c["op"] in (">=",):
            lo = (c["value"], True)
        elif c["op"] == ">":
            lo = (c["value"], False)
        elif c["op"] == "<":
            hi = (c["value"], False)
        elif c["op"] == "<=":
            hi = (c["value"], True)
        elif c["op"] == "=":
            lo = hi = (c["value"], True)
    return lo, hi


def _tiers(M, perk, unit, ep="Mod Weapon Attack Damage"):
    out = []
    for e in M.entries(perk):
        if e["EntryPoint"] != ep:
            continue
        lo, hi = _bound(e["conds"])
        if lo and hi and lo[0] == hi[0]:
            r = f"{fmt(lo[0])} {unit}"
        elif lo and hi:
            top = hi[0] - (0 if hi[1] else 1)
            bot = lo[0] + (0 if lo[1] else 1)
            r = f"{fmt(bot)}–{fmt(top)} {unit}"
        elif lo:
            r = f"{fmt(lo[0] + (0 if lo[1] else 1))}+ {unit}"
        elif hi:
            r = f"{'under ' + fmt(hi[0]) if not hi[1] else fmt(hi[0]) + ' or fewer'} {unit}"
        else:
            r = "always"
        out.append((e["val"], r, lo[0] if lo else -1))
    out.sort(key=lambda x: x[2])
    return [f"{mult(v)} with {r}" for v, r, _ in out]


def p_caps_rich(M, perk):
    return {"title": "Anti-Rich", "lines": [["Damage", "hits harder the more caps you carry:", _tiers(M, perk, "caps")]],
            "switch": M.has_switch(perk=perk)}


def p_caps_poor(M, perk):
    return {"title": "Anti-Poor", "lines": [["Damage", "hits harder the fewer caps you carry:", _tiers(M, perk, "caps")]],
            "switch": M.has_switch(perk=perk)}


def p_mutants(M, perk):
    t = [x.replace(" 1 mutations", " 1 mutation") for x in _tiers(M, perk, "mutations")]
    return {"title": "Anti-Mutant", "lines": [["Damage", "hits harder the more mutations you have:", t]]}


def p_hungry(M, perk):
    e = M.entries(perk)[0]
    c = next(x for x in e["conds"] if x["func"] == "GetValue")
    return {"title": "Feeds on hunger", "lines": [["Damage", f"{mult(e['val'])} damage to you when your hunger value is "
                                                   f"{fmt(c['value'])} or more (the game's internal hunger counter)."]],
            "switch": M.has_switch(perk=perk)}


def p_irradiated(M, perk):
    e = M.entries(perk)[0]
    c = next(x for x in e["conds"] if x["func"] == "GetValue")
    ghoul = any(x["func"] == "GetIsPlayerGhoul" for x in e["conds"])
    return {"title": "Ghoul hunter", "lines": [["Damage", f"{mult(e['val'])} damage to you if you carry {fmt(c['value'])}+ rads"
                                                + (" or play as a ghoul" if ghoul else "") + "."]],
            "switch": M.has_switch(perk=perk)}


def p_quickhands(M, perk):
    e = M.entries(perk)[0]
    c = next((x for x in e["conds"] if x["func"] == "GetRandomPercent"), None)
    return {"title": "Quick hands", "lines": [["Reload", f"{fmt(c['value']) if c else '?'}% chance the magazine refills itself."]]}


def p_poison_hit(M, perk):
    sp = M.perk_spell(perk)
    rows = M.spells.get(sp, [])
    poi = next((r for r in rows if "Poison" in r["MGEF_EDID"] and num(r["Magnitude"])), None)
    rad = next((r for r in rows if "Radiation" in r["MGEF_EDID"] and num(r["Magnitude"])), None)
    t = f"hits apply {M.spell_name.get(sp) or 'poison'}"
    bits = []
    if poi:
        bits.append(f"{fmt(num(poi['Magnitude']))} poison damage a second for {fmt(num(poi['Duration']))} seconds")
    if rad:
        bits.append(f"{fmt(num(rad['Magnitude']))} radiation")
    return {"title": "Poisoned hits", "lines": [["On hit", t + (": " + " plus ".join(bits) if bits else "") + "."]]}


def p_toxic_blood(M, perk):
    sp = M.perk_spell(perk)
    return {"title": "Toxic Blood", "lines": [["On death", f"a {M.spell_name.get(sp) or 'toxic explosion'}."]]}


def p_swift(M, perk):
    sp = M.perk_spell(perk)
    rows = M.spells.get(sp, [])
    an = next((num(r["Magnitude"]) for r in rows if r["ActorValue"] == "AnimationMult"), None)
    rl = next((num(r["Magnitude"]) for r in rows if r["ActorValue"] == "WeapReloadSpeedMult"), None)
    bits = []
    if an:
        bits.append(f"+{fmt(an)}% movement and attack speed")
    if rl:
        bits.append(f"+{pct_of(rl)} reload speed")
    return {"title": M.spell_name.get(sp) or "Swift-Footed", "lines": [["Speed", and_list(bits) + "."]]}


def p_immune(M, perk):
    return None   # merged in mechanics_for()


def p_explode_melee(M, perk):
    e = M.entries(perk)[0]
    ch = next((x["value"] for x in e["conds"] if x["func"] == "GetRandomPercent"), None)
    sp = M.perk_spell(perk)
    r = M.eff(sp) or {}
    dmg = num(r.get("Magnitude"))
    t = (f"{fmt(ch)}% chance a" if ch else "every") + " melee hit explodes"
    if dmg and dmg > 1:
        t += f" ({M.spell_name.get(sp)}, {fmt(dmg)} physical damage)"
    else:
        t += f" ({M.spell_name.get(sp)})" if M.spell_name.get(sp) else ""
    return {"title": "Exploding melee", "lines": [["On hit", t[0].upper() + t[1:] + "."]], "switch": M.has_switch(perk=perk)}


def p_tankkiller(M, perk):
    es = M.entries(perk)
    dr = next((e["val"] for e in es if e["EntryPoint"] == "Mod Target Damage Resistance"), None)
    st = next((e for e in es if e["EntryPoint"] == "Apply Combat Hit Spell"), None)
    ch = next((x["value"] for x in (st or {}).get("conds", []) if x["func"] == "GetRandomPercent"), None)
    lines = []
    if dr:
        lines.append(["Armour", f"rifle, shotgun and pistol hits ignore {pct_of(1 - dr)} of your Damage Resistance."])
    if ch:
        lines.append(["Stagger", f"automatic ones have a {fmt(ch)}% chance to stagger you."])
    return {"title": "Tank Killer", "lines": lines}


def p_dmg_poisoned(M, perk):
    e = M.entries(perk)[0]
    return {"title": "Picks on the poisoned", "lines": [["Damage", f"{mult(e['val'])} damage to targets that are poisoned."]]}


def p_ability_value(M, perk):
    sp = M.perk_spell(perk)
    r = M.eff(sp) or {}
    av, mg = r.get("ActorValue"), num(r.get("Magnitude"))
    if av == "STAT_DmgLimbs":
        return {"title": "Limb damage", "lines": [["Damage", f"+{fmt(mg)}% damage to limbs."]]}
    if av == "ArmorPenetration":
        return {"title": "Armour piercing", "lines": [["Damage", f"ignores {fmt(mg)}% of your armour."]]}
    return None


def p_crippled(M, perk):
    e = M.entries(perk)[0]
    return {"title": "Picks on the crippled", "lines": [["Damage", f"{mult(e['val'])} damage to targets the game flags with a VATS target height of 1."]],
            "note": "It's named for crippled targets; we haven't confirmed that's exactly what it checks."}


def p_radio(M, perk):
    e = M.entries(perk)[0]
    return {"title": "Hates your radio", "lines": [["Damage", f"{mult(e['val'])} damage to you while your Pip-Boy radio is on."]]}


def p_wrecking(M, perk):
    e = M.entries(perk)[0]
    return {"title": "Wrecking Ball", "lines": [["Damage", f"{mult(e['val'])} damage to turrets and objects."]]}


def p_from_vault(M, perk):
    e = M.entries(perk)[0]
    return {"title": "Vault-Tec weakness", "lines": [["Takes", f"{mult(e['val'])} damage from players wearing Vault clothing."]]}


def p_to_vault(M, perk):
    e = M.entries(perk)[0]
    n = sum(1 for c in e["conds"] if c["func"] == "GetEquipped")
    return {"title": "Vault-Tec grudge", "lines": [["Deals", f"{mult(e['val'])} damage to anyone in a vault suit"
                                                    + (f" or one of {n} Vault-Tec outfits and hazmat suits" if n else "") + "."]]}


def p_medic(M, perk):
    e = M.entries(perk)[0]
    return {"title": "Medic's weakness", "lines": [["Takes", f"{mult(e['val'])} damage from Medic's weapons."]]}


def p_has_spell_dmg(M, perk):
    e = M.entries(perk)[0]
    c = next((x for x in e["conds"] if x["func"] in ("HasSpell", "HasActiveMagicEffect")), None)
    tgt = ref_edid(c["param"]) if c else ""
    name = M.spell_name.get(tgt) or M.mgef_name(tgt)
    src = ""
    if c and c["func"] == "HasActiveMagicEffect":
        en = M.ench_by_mgef.get(tgt, [])
        if en:
            src = f" (from his {en[0][0]}" + (f", {fmt(en[0][1])} seconds" if en[0][1] else "") + ")"
    else:
        ap = M.who_applies(tgt)
        if ap:
            r = M.eff(tgt) or {}
            src = " (his side-kicks' hits give it" + (f", {fmt(num(r['Duration']))} seconds" if num(r.get("Duration")) else "") + ")"
    return {"title": f"Exploits {name}", "lines": [["Damage", f"{mult(e['val'])} damage to anyone with {name}{src}."]]}


def p_apply_weakness(M, perk):
    sp = M.perk_spell(perk)
    r = M.eff(sp) or {}
    return {"title": M.spell_name.get(sp) or "Weakness", "lines": [["On hit", f"their hits give you {M.spell_name.get(sp)}"
                                                                     + (f" for {fmt(num(r['Duration']))} seconds" if num(r.get('Duration')) else "")
                                                                     + ", which lowers your Damage Resistance."]],
            "note": "How much it lowers it is decided on Bethesda's servers."}


def p_incisor(M, perk):
    e = M.entries(perk)[0]
    return {"title": "Incisor", "lines": [["Armour", f"melee and unarmed hits ignore {pct_of(1 - e['val'])} of your armour."]]}


def p_moving(M, perk):
    tiers = []
    for e in M.entries(perk):
        if e["EntryPoint"] != "Mod Incoming Weapon Damage":
            continue
        lo = next((c["value"] for c in e["conds"] if c["op"] in (">", ">=")), None)
        hi = next((c["value"] for c in e["conds"] if c["op"] in ("<", "<=")), None)
        r = f"speed over {fmt(lo)}" if hi is None else f"speed {fmt(lo)}–{fmt(hi)}"
        tiers.append((e["val"], r))
    tiers.sort()
    floor = min((c["value"] for e in M.entries(perk) for c in e["conds"] if c["op"] == ">"), default=None)
    t = [f"{mult(v)} damage at {r}" for v, r in tiers]
    if floor:
        t.append(f"full damage at speed {fmt(floor)} or slower")
    return {"title": "Hard to hit on the move", "lines": [["Takes", "the faster she moves, the less damage she takes:", t],
                                                          ["Covers", "weapon, explosion, limb and spell damage."]]}


PERKS = {
    "Burn_Bounty_AntiRich": p_caps_rich,
    "Burn_Bounty_AntiPoors": p_caps_poor,
    "Burn_Bounty_AntiMutants": p_mutants,
    "Burn_Bounty_DamageToHungry": p_hungry,
    "Burn_Bounty_DamageToIrradiated": p_irradiated,
    "Burn_Bounty_QuickHands03": p_quickhands,
    "Burn_Bounty_PoisonDamagePerk": p_poison_hit,
    "DailyOps_MutationPerk_ToxicBlood": p_toxic_blood,
    "DailyOps_MutationPerk_SwiftFooted": p_swift,
    "ImmuneToRadiation": p_immune,
    "ImmuneToPoison": p_immune,
    "Burn_Bounty_MechExplosions": p_explode_melee,
    "Burn_Bounty_BoxerExplosions": p_explode_melee,
    "Burn_Bounty_TankKiller_PK": p_tankkiller,
    "RTSV_Bounty_IncreaseDamageToPoisoned": p_dmg_poisoned,
    "RTSV_Bounty_LimbDamage": p_ability_value,
    "RTSV_Bounty_ArmorPenetration": p_ability_value,
    "RTSV_Bounty_DamageToCrippled": p_crippled,
    "RTSV_BountyPerk_Conductor": p_radio,
    "WreckingBall01": p_wrecking,
    "RTSV_Bounty_IncreaseDamageFromVaultTec": p_from_vault,
    "RTSV_Bounty_IncreaseDamageToVaultTec": p_to_vault,
    "RTSV_Bounty_MedicModWeakness": p_medic,
    "RTSV_Bounty_MikeExtraDamage": p_has_spell_dmg,
    "RTSV_Bounty_SubjugatorDmgBonus": p_has_spell_dmg,
    "RTSV_Bounty_ApplyBossWeakness": p_apply_weakness,
    "RTSV_Bounty_Incisor": p_incisor,
    "RTSV_Bounty_DamageReductionWhileMoving": p_moving,
}


# abilities -------------------------------------------------------------------

def a_frenzy(M, sp):
    inner = M.assoc_spell(sp)
    conds = " ".join(r.get("Conditions", "") for r in M.spells.get(inner, []))
    who = "other NPCs near him"
    if "GetIsPlayer" in conds and "BountyTarget" in conds:
        who = "other NPCs near him (not players, not other bounty targets)"
    return {"title": "Frenzy aura", "lines": [["Aura", f"{who} go into a frenzy and attack anything."]]}


def a_cloak_perk(M, sp, title, what):
    inner = M.assoc_spell(sp)
    return inner


def a_tb(M, sp):
    inner = M.assoc_spell(sp)
    perk = M.ptap(inner)
    sub = p_poison_hit(M, perk) if perk else None
    t = f'his aura ("{(M.eff(inner) or {}).get("MGEF_Name", "")}") hands out a perk that makes ' + (sub["lines"][0][1] if sub else "hits poison")
    return {"title": "Toxic Blood aura", "lines": [["Aura", t]],
            "note": "Exactly who receives it is decided on Bethesda's servers."}


def a_isotope_cloak(M, sp):
    inner = M.assoc_spell(sp)
    perk = M.ptap(inner)
    ab = next((e.get("Ability_EDID") for e in M.entries(perk) if e.get("Ability_EDID")), "")
    d = next((r["MGEF_Desc"] for r in M.spells.get(ab, []) if r["MGEF_Desc"]), "")
    return {"title": "Unstable Isotope", "lines": [["Aura", f'everyone near her gets Unstable Isotope' + (f': "{d}".' if d else ".")]]}


def a_isotope_self(M, sp):
    d = next((r["MGEF_Desc"] for r in M.spells.get(sp, []) if r["MGEF_Desc"]), "")
    return {"title": "Unstable Isotope", "lines": [["Self", "she has it herself" + (f': "{d}".' if d else ".")]]}


def a_plague(M, sp):
    inner = M.assoc_spell(sp)
    r = M.eff(inner) or {}
    d = (M.eff(sp) or {}).get("MGEF_Desc", "")
    return {"title": "Plague Walker", "lines": [["Aura", f"{fmt(num(r.get('Magnitude')))} poison damage a second to anyone near her"
                                                 + (f' ("{d}")' if d else "") + "."]]}


def a_swift_self(M, sp):
    r = M.eff(sp) or {}
    return {"title": "Swift Foot", "lines": [["Speed", f"+{fmt(num(r.get('Magnitude')))}% movement and attack speed."]]}


def a_ap(M, sp):
    inner = M.assoc_spell(sp)
    perk = M.ptap(inner)
    es = [e for e in M.entries(perk) if e["EntryPoint"] == "Mod VATS Attack Action Points"]
    v = es[0]["val"] if es else None
    return {"title": "VATS penalty aura", "lines": [["VATS", f"players near him take {mult(v)} VATS AP costs for every War Drum stack, counted up to {len(es)} stacks."]]}


def a_intimidate(M, sp):
    inner = M.assoc_spell(sp)
    r = M.eff(inner) or {}
    return {"title": M.spell_name.get(inner) or "War Drum", "lines": [["VATS", f"standing near one adds a War Drum stack"
                                                                       + (f" for {fmt(num(r['Duration']))} seconds" if num(r.get('Duration')) else "")
                                                                       + (f' ("{r["MGEF_Desc"]}")' if r.get("MGEF_Desc") else "") + "."]]}


def a_astro(M, sp):
    inner = M.assoc_spell(sp)
    perk = M.ptap(inner)
    ab = next((e.get("Ability_EDID") for e in M.entries(perk) if e.get("Ability_EDID")), "")
    rows = M.spells.get(ab, [])
    fall = next((r for r in rows if r["ActorValue"] == "FallSpeedMult"), None)
    jump = next((r for r in rows if r["ActorValue"] == "JumpHeightMult"), None)
    bits = []
    if fall:
        bits.append(f"fall slower (Fall Speed {fmt(num(fall['Magnitude']))})")
    if jump:
        bits.append(f"jump higher (Jump Height {fmt(num(jump['Magnitude']))})")
    return {"title": M.spell_name.get(ab) or "Low Gravity", "lines": [["Aura", "players near him " + and_list(bits) + "."]],
            "note": "Skipped for anyone who already has Bird Bones (fall) or Marsupial (jump)."}


def a_boxer(M, sp):
    inner = M.assoc_spell(sp)
    perk = M.ptap(inner)
    ab = next((e.get("Ability_EDID") for e in M.entries(perk) if e.get("Ability_EDID")), "")
    rows = M.spells.get(ab, [])
    spd = next((r for r in rows if r["ActorValue"] == "SpeedMult"), None)
    return {"title": M.spell_name.get(ab) or "No Running Away", "lines": [["Aura", "players right next to her"
                                                                          + (f" move {fmt(num(spd['Magnitude']))}% slower," if spd else "")
                                                                          + " jump lower and fall faster."]]}


def a_chameleon(M, sp):
    return {"title": "Chameleon", "lines": [["Stealth", "turns invisible while not attacking."]], "switch": M.has_switch(spell=sp)}


def a_faction(M, sp):
    inner = M.assoc_spell(sp)
    r = M.eff(inner) or {}
    return {"title": "Recruiting aura", "lines": [["Aura", f'runs "{r.get("MGEF_Name", "")}" on others nearby'
                                                   + (f" for {fmt(num(r['Duration']))} seconds" if num(r.get('Duration')) else "") + "."]],
            "note": "The name says it adds enemies to her side; who's affected is decided on Bethesda's servers."}


def a_fisher(M, sp):
    inner = M.assoc_spell(sp)
    r = M.eff(inner) or {}
    return {"title": "Hooking range", "lines": [["Aura", f'getting close puts "{r.get("MGEF_Name", "")}" on you.']],
            "note": "What the hook does is handled on Bethesda's servers."}


def a_swift_cloak(M, sp):
    return {"title": "Swift Foot aura", "lines": [["Aura", "hands out the Bounty Swift Foot effect to those nearby."]],
            "note": "What the effect does is handled on Bethesda's servers."}


def a_wound_salter(M, sp):
    r = M.eff(sp) or {}
    return {"title": M.spell_name.get(sp) or "Wound Salter", "lines": [["Damage", f"+{fmt(num(r.get('Magnitude')))}% damage to bleeding targets."]]}


def a_heal(M, sp):
    r = M.eff(sp) or {}
    return {"title": "Self-heal", "lines": [["Healing", f'a "{r.get("MGEF_Name", "")}" on his Health (magnitude {fmt(num(r.get("Magnitude")))}) while alive.']]}


def a_stealthy(M, sp):
    r = M.eff(sp) or {}
    m = re.search(r"GetHealthPercentage\([^)]*\) >= ([\d.]+)", r.get("Conditions", ""))
    return {"title": "Stealth Field", "lines": [["Stealth", "invisible" + (f" while above {pct_of(float(m.group(1)))} health" if m else "") + "."]]}


def a_volatile(M, sp):
    return {"title": "Volatile", "lines": [["On death", "explodes when it dies."]]}


def a_gunner(M, sp):
    inner = M.assoc_spell(sp)
    perk = M.ptap(inner)
    v = next((e["val"] for e in M.entries(perk) if e["EntryPoint"] == "Mod Weapon Attack Damage"), None)
    return {"title": "Gun-crew aura", "lines": [["Aura", f"players near him deal {mult(v)} damage with ranged ballistic weapons."]],
            "note": "Exactly who receives it is decided on Bethesda's servers."}


def a_scout(M, sp):
    perk = M.ptap(sp)
    hit = M.perk_spell(perk)
    r = M.eff(hit) or {}
    return {"title": "Scout camouflage", "lines": [["On hit", f"her hits carry a {fmt(num(r.get('Duration')))}-second Stealth Field (invisibility)."]],
            "note": "Whether it's her or you who turns invisible needs checking in game."}


def a_time(M, sps):
    rings = []
    for sp in sps:
        r = M.eff(sp) or {}
        inner = M.assoc_spell(sp)
        ir = M.eff(inner) or {}
        rings.append((num(r.get("Magnitude")), num(ir.get("Magnitude")), ir.get("MGEF_Desc", "")))
    rings.sort(key=lambda x: -(x[0] or 0))
    items, tot = [], 0
    for size, slow, desc in rings:
        tot += slow or 0
        items.append(f"inside ring {fmt(size)}: {fmt(tot)}% slower" + (f' ("{desc}")' if desc else ""))
    return {"title": "Time bubble", "lines": [
        ["Aura", f"the closer you get, the slower you move ({NUM_WORDS.get(len(rings), len(rings))} rings):", items]]}


SPELLS = {
    "Burn_Bounty_AB_ForceFrenzyCloak": a_frenzy,
    "Burn_Bounty_TBCloakSpell": a_tb,
    "Burn_Bounty_UnstableIsotopeCloakSpell": a_isotope_cloak,
    "Burn_Bounty_Mutation_UnstableIsotope": a_isotope_self,
    "Burn_Bounty_Mutation_PlagueWalker": a_plague,
    "Burn_Bounty_SwiftFootEffect_SP_Self": a_swift_self,
    "Burn_Bounty_APCloak_SP": a_ap,
    "Burn_Bounty_IntimidationCloak_SP": a_intimidate,
    "Burn_Bounty_AstroCloakSpell": a_astro,
    "Burn_Bounty_BoxerCloakSpell": a_boxer,
    "Burn_Bounty_Mutation_Chameleon": a_chameleon,
    "Burn_Bounty_AB_ForceFactionCloak": a_faction,
    "Burn_Bounty_FishermanCloak": a_fisher,
    "Burn_Bounty_SwiftFootCloak_SP": a_swift_cloak,
    "RTSV_Bounty_WoundSalter_SP": a_wound_salter,
    "RTSV_Bounty_HealingEffectSelf_SP": a_heal,
    "RTSV_Bounty_AssualtronStealthy_SP": a_stealthy,
    "DailyOps_Mutation_Volatile": a_volatile,
    "RTSV_Bounty_Gunner_DamageCloak_Spell": a_gunner,
    "Burn_Bounty_ScoutPerk_SP": a_scout,
}

# Looks like a bug -> reviewFlags + bug backlog, never on the page as fact.
def _flag_damage_up(M, p):
    return (f"{mult(M.entries(p)[0]['val'])} weapon damage whose only condition is that the boss is alive "
            f"({p}). Looks unintended; check in game before it goes on the page.")


FLAG_PERKS = {
    "Burn_BountyHunting_DamageUpPerk": _flag_damage_up,      # live name
    "BountyHunting_Burn_DamageUpPerk": lambda M, p: (
        f"{mult(M.entries(p)[0]['val'])} weapon damage whose only condition is that the boss is alive "
        f"({p}). Looks unintended; check in game before it goes on the page."),
}


# ── assemble ───────────────────────────────────────────────────────────────

def mechanics_for(M, npc, region_label=""):
    """[(mechanic dict)], [review flags] for one actor."""
    out, flags = [], []
    if not npc:
        return out, flags
    for S in M.scripts(npc):
        if S in IGNORE_SCRIPTS:
            continue
        fn = SCRIPTS.get(S)
        if fn:
            m = fn(M, npc, S)
            if m:
                m["source"] = "script"
                out.append(m)
        else:
            M.warnings.append(f"{npc}: script {S} has no wording")
    ab = M.abil.get(npc, [])
    time = [s for s in ab if re.match(r"RTSV_BountySpell_TimeCloak\d+$", s)]
    if time:
        m = a_time(M, time)
        m["source"] = "aura"
        out.append(m)
    for sp in ab:
        if sp in IGNORE_SPELLS or sp in time:
            continue
        fn = SPELLS.get(sp)
        if fn:
            m = fn(M, sp)
            if m:
                m["source"] = "aura" if "Cloak" in sp else "ability"
                out.append(m)
        else:
            M.warnings.append(f"{npc}: ability {sp} has no wording")
    imm = []
    for pk in M.perks.get(npc, []):
        if pk in IGNORE_PERKS:
            continue
        if pk in ("ImmuneToRadiation", "ImmuneToPoison"):
            imm.append("radiation" if "Radiation" in pk else "poison")
            continue
        if pk in FLAG_PERKS:
            flags.append(FLAG_PERKS[pk](M, pk))
            continue
        fn = PERKS.get(pk)
        if fn:
            m = fn(M, pk)
            if m:
                m["source"] = "perk"
                out.append(m)
        else:
            M.warnings.append(f"{npc}: perk {pk} has no wording")
    merged = []
    for m in out:                      # same title twice (Unstable Isotope self + aura) -> one block
        prev = next((x for x in merged if x["title"] == m["title"]), None)
        if prev:
            prev["lines"] += m.get("lines", [])
        else:
            merged.append(m)
    out = merged
    if imm:
        out.append({"title": "Immunities", "lines": [["Immune to", and_list(sorted(imm)) + "."]], "source": "perk"})
    if any(m.get("title") == "Scout camouflage" for m in out):
        flags.append("Scout Leader Karen (and Pioneer Posers): Burn_Bounty_ScoutCloak_PK applies Burn_Bounty_ScoutCamo_SP "
                     "(Stealth Field 10 s) as an Apply Combat Hit Spell, which normally lands on the target - the player. "
                     "Check in game who turns invisible.")
    for m in out:
        m["lines"] = [l for l in m.get("lines", []) if l[1] or (len(l) > 2 and l[2])]
        # Kat, 22 Nov 2026: no disclaimer notes on the page (server-side / unconfirmed).
        # Only a note that IS part of the mechanic stays, as a line.
        n = m.pop("note", None)
        if n and n.startswith("Skipped for anyone"):
            m["lines"].append(["Exception", n[0].lower() + n[1:]])
        if not m.get("switch"):
            m.pop("switch", None)
    return out, flags


def side_kick_entry(name, mine, boss_mechs):
    """One side-kick block; mechanics identical to the boss's are named, not repeated."""
    lines = []
    boss_titles = {m["title"]: m for m in boss_mechs}
    for m in mine:
        b = boss_titles.get(m["title"])
        if b and b.get("lines") == m.get("lines"):
            lines.append([m["title"], "same as the boss."])
        else:
            text = " ".join((l[1][0].upper() + l[1][1:]) if l[1] else "" for l in m["lines"])
            items = [x for l in m["lines"] if len(l) > 2 for x in (l[2] or [])]
            lines.append([m["title"], text] + ([items] if items else []))
    ent = {"name": plural(name), "lines": lines}
    notes = [m["note"] for m in mine if m.get("note") and not (boss_titles.get(m["title"]) or {}).get("lines") == m.get("lines")]
    if notes:
        ent["note"] = " ".join(dict.fromkeys(notes))
    if any(m.get("switch") for m in mine):
        ent["switch"] = True
    return ent
