"""What a custom weapon mod actually does, in numbers, from the game files.

Added 15 Nov 2026 for the V63-Z Custom recipe rows on the Brain in a Jar reward
checklist (build_brain_in_a_jar_json.mod_details). Works for any weapon whose mods
hang off one mod-association keyword (ma_*).

Chain followed (all from the patch exports, nothing typed by hand):
  OMOD _PARENT_ include  -> property 116 = the mod's PERK
  PERKEP entry points    -> plain values (Multiply / Add Value) or spells (Apply Combat ... Spell, Ability)
  SPEL _EFFECTS          -> magnitude / duration / area per magic effect, + effect conditions
  MGEF                   -> archetype + actor value (what the effect changes)
  MGEF _Explosions       -> the effect's explosion: damage type + curve table (level-scaled damage)
  CURV + data/curvetables/json -> the damage at the weapon's level

Each line is sorted into "own" (works with just this mod fitted) or "with <other mod>"
(the entry's conditions need another of the weapon's mods fitted or active). Lines
tied to cut mods (zzz) are dropped (cut content lives on its own page only).
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))
from esm_conditions import op_from_type  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CURVE_DIR = os.path.join(REPO, "data", "curvetables", "json")
LEVEL = 50

_AV = {  # actor value -> (label, as percent)
    "weaponSpeedMult": ("Attack speed", True),
    "SpeedMult": ("Move speed", "pp"),
    "STAT_DmgMelee": ("Melee damage", "pp"),
    "DamageResist": ("Damage resistance", False),
    "EnergyResist": ("Energy resistance", False),
    "MeleeDamage": ("Melee damage", False),
}
_EP = {  # entry point -> when it happens
    "Apply Combat Melee Spell": "On melee hits",
    "Apply Combat Hit Spell": "On hits",
    "Apply Combat Hit Spell Taken": "When you're hit",
}
_REF = re.compile(r"(HasPerk|HasActiveMagicEffect)\((\S+)[^\[]*\[(PERK|MGEF):([0-9A-F]{8})\][^)]*\)\s*"
                  r"(?:([01]{8})\s+|=\s*)([\d.]+)")


def _fl(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _n(v):
    return f"{v:g}" if v != int(v) else str(int(v))


def _curve_at(path, x=LEVEL):
    p = os.path.join(CURVE_DIR, *path.replace("\\", "/").lower().split("/"))
    try:
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
    except (OSError, ValueError):
        return None
    pts = sorted((q["x"], q["y"]) for q in (d.get("curve", d) if isinstance(d, dict) else d))
    if not pts:
        return None
    if x <= pts[0][0]:
        return pts[0][1]
    for (a, b), (c, e) in zip(pts, pts[1:]):
        if a <= x <= c:
            return b + (e - b) * (x - a) / (c - a)
    return pts[-1][1]


def _conds(text, perkep=True):
    """[(kind, fid, op, value)] for the HasPerk / HasActiveMagicEffect checks."""
    out = []
    for m in _REF.finditer(text or ""):
        op = op_from_type(m.group(5)) if m.group(5) else "="
        out.append((m.group(3), m.group(4), op, _fl(m.group(6)), m.group(2)))
    return out


def arch_of(mg):
    return mg.get("Archetype", "")


def _flags(text):
    t = text or ""
    return ("In V.A.T.S." if "GetIsInVATS()" in t and not re.search(r"GetIsInVATS\(\)[^|]*\s0\.0", t) else "",
            "critical hits" if "GetLastHitCritical" in t else "")


class ModEffects:
    def __init__(self, rows):
        """rows(pattern) -> list of dict rows from the newest export for the channel."""
        self.perkep = {}
        for r in rows("PERKEP_Export_*.tsv"):
            self.perkep.setdefault(r.get("PERK_FormID"), []).append(r)
        self.spel = {}
        for r in rows("SPEL_Export_*_EFFECTS.tsv"):
            self.spel.setdefault(r.get("SPEL_FormID"), []).append(r)
        self.mgef = {r.get("MGEF_FormID"): r for r in rows("MGEF_Export_*.tsv")}
        self.expl = {r.get("MGEF_FormID"): r for r in rows("MGEF_Export_*_Explosions.tsv")}
        self.curv = {r.get("EDID"): r.get("JASF_Path") for r in rows("CURV_Export_*.tsv")}
        self.spel_head = {r.get("SPEL_FormID"): r for r in rows("SPEL_Export_*_HEADER.tsv")}
        self.ench = {}
        for r in rows("ENCH_Export_*.tsv"):
            effs, n = [], 1
            while r.get(f"Effect_{n}_MGEF_FID"):
                effs.append({"mgef": r[f"Effect_{n}_MGEF_FID"], "mag": _fl(r.get(f"Effect_{n}_Magnitude")),
                             "area": _fl(r.get(f"Effect_{n}_Area")), "dur": _fl(r.get(f"Effect_{n}_Duration")),
                             "curve": r.get(f"Effect_{n}_CURV_EID", ""), "conds": ""})
                n += 1
            self.ench[r.get("ENCH_FormID")] = effs
        # Script effects that cast a spell (VMAD property pointing at a SPEL), e.g. the
        # Kinetic Amplifier's block check casting its power-up.
        self.vmad_spells = {fid: re.findall(r"=([0-9A-F]{8}):[^:|]*:SPEL", r.get("VMAD_Scripts", ""))
                            for fid, r in self.mgef.items() if ":SPEL" in (r.get("VMAD_Scripts") or "")}
        self.codename = ""

    def _spel_effects(self, sp):
        return [{"mgef": e.get("EFID_MGEF_FormID"), "mag": _fl(e.get("EFIT_Magnitude")), "area": _fl(e.get("EFIT_Area")),
                 "dur": _fl(e.get("EFIT_Duration")), "curve": e.get("CVTO_CURV_EDID", ""), "conds": e.get("Conditions", "")}
                for e in self.spel.get(sp, [])]

    def _expand(self, eff, depth=0, seen=None):
        """The effect, plus what it sets off: spells a script effect casts, and the
        enchantment an explosion puts on what it hits (marked 'blast')."""
        seen = seen or set()
        yield eff
        if depth >= 3:
            return
        for sp in self.vmad_spells.get(eff["mgef"], []):
            if sp in seen:
                continue
            seen.add(sp)
            head = self.spel_head.get(sp, {})
            for e in self._spel_effects(sp):
                e["conds"] = " | ".join(x for x in (e["conds"], eff["conds"]) if x)
                e["via"] = (head.get("SPEL_FULL") or "").strip()
                e["self"] = head.get("SPIT_Delivery") == "Self"
                yield from self._expand(e, depth + 1, seen)
        x = self.expl.get(eff["mgef"])
        if x and x.get("EITM_FormID"):
            for e in self.ench.get(x["EITM_FormID"], []):
                e = dict(e, conds=eff["conds"], blast=True)
                yield from self._expand(e, depth + 1, seen)

    def _mag(self, e):
        if e.get("curve") and self.curv.get(e["curve"]):
            v = _curve_at(self.curv[e["curve"]])
            if v is not None:
                return round(v)
        return e["mag"]

    # -- what an entry casts ------------------------------------------------
    def _spells(self, ep):
        if ep.get("Ability_FormID"):
            return [ep["Ability_FormID"]]
        return re.findall(r"Spell=[^;]*?\[SPEL:([0-9A-F]{8})\]", ep.get("Raw", ""))

    def _effect_line(self, e):
        mg = self.mgef.get(e["mgef"], {})
        name = (mg.get("FULL") or "").strip()
        if self.codename:
            name = re.sub(r"\b%s\b\s*" % re.escape(self.codename), "", name, flags=re.I).strip()
        name = re.sub(r"\bDoT\b", "burn", name)
        mag, dur, area = self._mag(e), e["dur"], e["area"]
        if e.get("blast"):
            name += " (from the blast)"
        elif e.get("self") and arch_of(mg) == "Damage":
            name += " (on you)"
        arch, av = mg.get("Archetype", ""), mg.get("ActorValue", "")
        x = self.expl.get(e["mgef"])
        if x and x.get("DamageTypes_Flat"):
            # EXPL DATA Damage is the blast's own physical damage; DAMA adds typed damage on top
            # (same engine rule as Fallout 4). 0.01 placeholders (e.g. Electrician's) are skipped.
            phys = _fl(x.get("Damage"))
            parts = [f"{_n(phys)} physical damage"] if phys >= 1 else []
            for t in x["DamageTypes_Flat"].split(" | "):
                dt, _amt, cv = (t.split(":") + ["", "", ""])[:3]
                v = _curve_at(self.curv.get(cv, "")) if cv else None
                if v:
                    parts.append(f"{round(v)} {re.sub(r'^dt', '', dt).lower()} damage")
            if parts:
                return f"{name}: {' + '.join(parts)} at level {LEVEL}" + (f", {_n(area)} ft area" if area else "")
        if arch == "Damage" and mag > 0:
            return f"{name}: {_n(mag)} damage" + (f" per second for {_n(dur)} s" if dur > 1 else "") \
                + (f" at level {LEVEL}" if e.get("curve") else "")
        if arch in ("Value Modifier", "Peak Value Modifier") and av in _AV and mag:
            lab, pct = _AV[av]
            val = f"{mag:+g}%" if pct == "pp" else (f"{mag * 100:+.0f}%" if pct else f"{mag:+g}")
            return f"{lab} {val}" + (f" for {_n(dur)} s" if dur else "")
        if arch == "Cloak" and mag > 0:
            return f"Burst around you, {_n(mag)} unit radius"
        return None

    # -- the mod ------------------------------------------------------------
    def describe(self, own_perks, all_perks, perk_mod):
        """own_perks: this mod's PERK FormIDs; all_perks: {fid: edid} for every mod on the weapon;
        perk_mod: {perk fid: mod name}. Returns {"own": [[label, text]], "with": [[mod, text]]}."""
        owner = {}  # MGEF fid -> perk fid that applies it
        for pk in all_perks:
            for ep in self.perkep.get(pk, []):
                for sp in self._spells(ep):
                    for e0 in self._spel_effects(sp):
                        for e in self._expand(e0):
                            owner.setdefault(e["mgef"], pk)
        toks = [re.match(r"^(?:zzz)?[A-Za-z0-9]+_([A-Za-z0-9]+)_", ed) for ed in all_perks.values()]
        toks = [t.group(1) for t in toks if t]
        self.codename = max(set(toks), key=toks.count) if toks else ""
        stems = {pk: re.sub(r"(Perk|er)$", "", re.sub(r"^.*?_[A-Za-z]+_", "", ed)) for pk, ed in all_perks.items()}

        def mod_of(kind, fid):
            if kind == "PERK":
                return fid if fid in all_perks else None
            if fid in owner:
                return owner[fid]
            ed = self.mgef.get(fid, {}).get("EDID", "")
            tail = re.sub(r"^.*?_[A-Za-z]+_", "", ed)
            return next((pk for pk, st in stems.items() if len(st) >= 5 and tail.startswith(st[:5])), None)

        def classify(texts, states=None):
            need = set()
            for t in texts:
                for kind, fid, op, val, ed in _conds(t):
                    if ed.startswith("zzz") and op in ("=", ">=", ">") and val >= 1:
                        return None
                    pk = mod_of(kind, fid)
                    if kind == "PERK" and all_perks.get(fid, "").startswith("zzz") and op == "=" and val == 1:
                        return None
                    if kind == "MGEF" and pk in own_perks and op in ("=", ">=", ">") and val >= 1:
                        nm = (self.mgef.get(fid, {}).get("FULL") or "").strip()
                        if nm and states is not None:
                            states.add(nm)
                    if pk and pk not in own_perks and op in ("=", ">=", ">") and val >= 1:
                        if all_perks.get(pk, "").startswith("zzz"):
                            return None
                        need.add(perk_mod.get(pk, ""))
            return need

        own, withs, seen = [], [], set()
        for pk in own_perks:
            for ep in self.perkep.get(pk, []):
                need = classify([ep.get("Conditions", "")])
                if need is None:
                    continue
                vats, crit = _flags(ep.get("Conditions", ""))
                when = "In V.A.T.S." if vats else ("On critical hits" if crit else _EP.get(ep.get("EntryPoint", ""), ""))
                lines = []
                if ep.get("Function") in ("Multiply Value", "Add Value") and ep.get("Value"):
                    v = _fl(ep["Value"])
                    lab = re.sub(r"^Mod ", "", ep.get("EntryPoint", "")).replace("VATS", "V.A.T.S.")
                    lines.append((lab, f"×{_n(v)}" if ep["Function"] == "Multiply Value" else f"{v:+g}", need))
                for sp in self._spells(ep):
                    effs = [e for e0 in self._spel_effects(sp) for e in self._expand(e0)]
                    texts = {}
                    for e in effs:
                        ct = [e["conds"], ep.get("Conditions", "")]
                        st = set()
                        n2 = classify(ct, st)
                        if n2 is None:
                            continue
                        t = self._effect_line(e)
                        if t:
                            v2, c2 = _flags(" | ".join(ct))
                            w2 = ("In V.A.T.S." if v2 else ("On critical hits" if c2 else
                                  ("Always" if ep.get("Ability_FormID") else when)))
                            if e.get("via") and w2 == "Always":
                                w2 = f"When {e['via']} triggers"
                            if st:
                                w2 = "While " + " / ".join(sorted(st)) + ("" if w2 == "Always" else ", " + w2[0].lower() + w2[1:])
                            key = (w2, t, frozenset(need | n2))
                            texts[key] = texts.get(key, 0) + 1
                    for (w2, t, nd), cnt in texts.items():
                        if cnt > 1:
                            t += f" (stacks up to {cnt})"
                        lines.append((w2, t, set(nd)))
                for lab, t, nd in lines:
                    key = (lab, t, tuple(sorted(nd)))
                    if key in seen:
                        continue
                    seen.add(key)
                    if nd:
                        withs.append([" or ".join(sorted(x for x in nd if x)) or "Another mod", lab, t])
                    else:
                        own.append([lab or "Effect", t])
        def merge(rows, li, keyf):
            """Same effect under two 'when' labels -> one line, labels joined."""
            out, idx = [], {}
            for r in rows:
                k = keyf(r)
                if k in idx:
                    o = out[idx[k]]
                    if r[li] and r[li] not in o[li]:
                        o[li] += " / " + r[li]
                    continue
                idx[k] = len(out)
                out.append(list(r))
            return out
        withs = [[m, f"{lab}: {t}" if lab else t] for m, lab, t in merge(withs, 1, lambda r: (r[0], r[2]))]
        return {"own": merge(own, 0, lambda r: r[1]), "with": withs}
