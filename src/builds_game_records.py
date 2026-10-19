"""
builds_game_records.py
======================
Reads the game records the Builds calculator needs straight from the xEdit
exports, so nothing on the page is typed in by hand or copied from a wiki.

  game_rules()          perk-coin rank-up costs      CURV LegendaryPerks_Upgrades
                        legendary perk slot levels   CURV LegendaryPerkSlotCount
                        Legendary SPECIAL bonuses    SPEL AbLgnPerk<Stat> (+ HasPerk rank conditions)
  perk_rank_effects()   what each perk card rank actually does to damage:
                        its PERK entry points (Mod Target Damage Resistance,
                        Mod Sneak Attack Mult, Mod My Critical Hit Damage Mult,
                        Mod Weapon Attack Damage, Apply Combat Hit Spell ...) and
                        the effects of its Ability spell, with each effect's
                        rank condition (HasPerk Xnn) and weapon condition
                        (WornHasKeyword ...) resolved per rank
  mutation_effects()    every mutation effect with the conditions the game puts
                        on it: normal vs team "super" version
                        (Mutation_Check_UseSuperVersion = team + Strange In
                        Numbers + a mutated teammate), human vs ghoul
                        (GetIsPlayerGhoul), removed by its serum, Class Freak rank

Effects are grouped by the magic effect (MGEF) or entry point they use: the
game adds up everything that raises the same effect, so a group total is the
real number. HOW the different groups combine with each other is done in the
game's code (not in any record), so the page shows each group on its own.
"""
import csv, re
from collections import defaultdict
import tsv_source
from ench_rows import normalize as normalize_ench

csv.field_size_limit(10 ** 9)


def mgef_actor_value(mrow):
    """MGEF ActorValue column (filled by the fixed MGEF export):
    'ArmorPenetration "Armor Penetration" [AVIF:00097341]' -> 'ArmorPenetration'.
    Empty on older exports."""
    s = str((mrow or {}).get("ActorValue") or "").strip().strip('"')
    if not s or s.upper().startswith("NULL"):
        return ""
    return s.split(" ")[0].strip('"')
SPECIALS = ["Strength", "Perception", "Endurance", "Charisma", "Intelligence", "Agility", "Luck"]


def _read(pattern, exclude=None):
    path = tsv_source.newest(pattern, exclude=exclude, required=False)
    if not path:
        return []
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _q(s):
    return str(s or "").strip().strip('"').strip()


def _num(v, d=None):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return d


_COND = re.compile(r"(Subject|Target)\.(\w+)\(([^,)]*)[^)]*\)\s*(=|!=|>=|<=|>|<)\s*([-\d.]+)")


def parse_conditions(flat):
    """'Subject.HasPerk(Slugger02 "Slugger" [PERK:..], 00 00 00 00) = 1.000000 AND | ...'
    -> [(runOn, function, firstParamEditorID, op, value), ...]"""
    out = []
    for part in str(flat or "").split("|"):
        m = _COND.search(part)
        if not m:
            continue
        arg = m.group(3).strip()
        arg = arg.split(" ")[0].strip('"') if arg else ""
        out.append((m.group(1), m.group(2), arg, m.group(4), float(m.group(5))))
    return out


class Records:
    def __init__(self):
        self.spel = defaultdict(list)          # SPEL_EDID -> effect rows
        for r in _read("SPEL_Export_*_EFFECTS.tsv"):
            self.spel[_q(r.get("SPEL_EDID"))].append(r)
        self.perk = defaultdict(list)          # PERK_EDID -> effect rows
        for r in _read("PERK_Export_*.tsv"):
            self.perk[_q(r.get("PERK_EDID"))].append(r)
        self.mgef = {}
        for r in _read("MGEF_Export_*.tsv"):
            self.mgef[_q(r.get("MGEF_FormID")).upper()] = r
        self.curv_points = defaultdict(list)
        for r in _read("CURV_Export_*_POINTS.tsv"):
            self.curv_points[_q(r.get("EDID"))].append((_num(r.get("X"), 0), _num(r.get("Y"), 0)))
        self.glob = {}                         # GLOB FormID -> (EDID, value)
        for r in _read("GLOB_Export_*.tsv"):
            self.glob[_q(r.get("FormID")).upper()] = (_q(r.get("EDID")), _num(r.get("FLTV"), None))
        # spells that check HasPerk(X) = 1 on any of their effects, by perk EDID,
        # so a card with no effects of its own (Bringing the Big Guns) still
        # finds the spells that switch on when you have it
        self.spells_by_perk = defaultdict(set)
        for sp, rows in self.spel.items():
            for r in rows:
                for _, fn, arg, op, val in parse_conditions(r.get("Conditions")):
                    if fn == "HasPerk" and val == 1.0 and op == "=":
                        self.spells_by_perk[arg].add(sp)
        self.cndf = {}
        for r in _read("CNDF_Export_*.tsv"):
            self.cndf[_q(r.get("EDID"))] = r
        self.pcrd = {}
        for r in _read("PCRD_Export_*.tsv"):
            self.pcrd[_q(r.get("PCRD_FormID")).upper()] = r
        self.omod_props = defaultdict(list)    # OMOD_EDID -> ActorValues / Enchantments rows
        for r in _read("OMOD_Export_*_Properties.tsv"):
            if _q(r.get("PropertyName")) in ("ActorValues", "Actor Values", "Enchantments"):
                self.omod_props[_q(r.get("OMOD_EDID"))].append(r)
        self.ench = {}
        for r in _read("ENCH_Export_*.tsv"):
            self.ench[_q(r.get("ENCH_EDID"))] = r

    # ------------------------------------------------------------ game rules
    def game_rules(self):
        rules, sources = {}, {}
        up = sorted(self.curv_points.get("LegendaryPerks_Upgrades", []))
        if up:
            # X = current rank - 1 (0 => rank 1 -> 2). coinCost[n] = cost from rank n to n+1.
            rules["coinCost"] = [0] + [int(y) for _, y in up]
            sources["coinCost"] = "CURV LegendaryPerks_Upgrades"
        slots = sorted(self.curv_points.get("LegendaryPerkSlotCount", []))
        if slots:
            rules["legendarySlotLevels"] = [int(y) for _, y in slots]
            sources["legendarySlotLevels"] = "CURV LegendaryPerkSlotCount"
        bonus = self.legendary_special_bonus()
        if bonus:
            rules["legendarySpecialBonus"] = bonus
            sources["legendarySpecialBonus"] = "SPEL AbLgnPerk<Stat> (Fortify <Stat> per rank)"
        rules["_sources"] = sources
        return rules

    def legendary_special_bonus(self):
        """SPEL AbLgnPerkStrength: an unconditional +1 then +1 / +1 / +2 gated by
        HasPerk(LGN_LegendaryStrength_Perk02/03/04). Ranks stack (rank 4 also has
        the rank 2 and 3 perks), so rank n = sum of every effect whose rank <= n."""
        per_stat = {}
        for stat in SPECIALS:
            rows = self.spel.get(f"AbLgnPerk{stat}")
            if not rows:
                continue
            tot = [0.0] * 4
            for r in rows:
                mag = _num(r.get("EFIT_Magnitude"), 0) or 0
                need = 1
                for _, fn, arg, op, val in parse_conditions(r.get("Conditions")):
                    m = re.search(r"_Perk0?(\d)$", arg)
                    if fn == "HasPerk" and m and val == 1.0:
                        need = int(m.group(1))
                for n in range(need, 5):
                    tot[n - 1] += mag
            per_stat[stat] = [int(x) for x in tot]
        if not per_stat:
            return None
        vals = list(per_stat.values())
        return vals[0] if all(v == vals[0] for v in vals) else per_stat

    # ------------------------------------------------------------ perk cards
    def card_rank_perks(self, card_formid):
        row = self.pcrd.get(str(card_formid).upper(), {})
        out = []
        for n in range(1, 6):
            e = _q(row.get(f"Rank_{n}_MalePerk_EDID"))
            if e:
                out.append(e)
        return out

    def _effect_applies(self, conds, rank, rank_perks):
        """Does an effect with these conditions apply at `rank`?
        HasPerk on this card's own rank perks is resolved; the player holds
        every rank perk up to `rank`. Other conditions are kept as notes."""
        notes = []
        for run_on, fn, arg, op, val in conds:
            if fn == "HasPerk" and arg in rank_perks:
                held = rank_perks.index(arg) < rank
                want = (val == 1.0) if op == "=" else (val != 1.0)
                if held != want:
                    return False, []
            elif fn == "HasPerk" and val == 1.0 and op == "=":
                # needs ANOTHER card (Bringing the Big Guns' +10 stacks sits in
                # Bullet Storm's spell): it's counted on that card instead
                return False, []
            elif fn == "WornHasKeyword" and val == 1.0 and re.search(r"CustomItemName|^dn_", arg):
                # only on one named unique weapon / armour piece
                return False, []
            elif fn == "WornHasKeyword" and val == 1.0:
                notes.append(("weapon", arg))
            elif fn == "WornHasKeyword" and val == 0.0:
                notes.append(("notweapon", arg))
            elif fn == "IsInPowerArmor" or fn == "IsWearingPowerArmor":
                notes.append(("pa", int(val)))
            elif fn == "GetIsPlayerGhoul":
                notes.append(("ghoul", int(val)))
            elif fn in ("GetHealthPercentage", "GetActorValuePercent"):
                notes.append(("health", f"{op} {val:g}"))
            elif fn in ("IsSneaking",):
                notes.append(("sneak", int(val)))
        return True, notes

    def perk_rank_effects(self, card_formid):
        """-> list per rank of [{group, value, unit, scales, cond[]}] for the
        damage-related effects of that rank."""
        rank_perks = self.card_rank_perks(card_formid)
        if not rank_perks:
            return []
        per_rank = []
        for rank in range(1, len(rank_perks) + 1):
            fx = []
            spells_seen = set()
            for pe in rank_perks[:rank]:
                for r in self.perk.get(pe, []):
                    typ = _q(r.get("PRKE_Type"))
                    ep = _q(r.get("EP_EntryPoint"))
                    fval = _num(re.sub(r"^Float=", "", _q(r.get("EPFD_Float"))), None)
                    # entry points only count on the rank perk that owns them (the
                    # game replaces the lower rank's entry point when ranked up)
                    own = (pe == rank_perks[rank - 1])
                    if typ == "Entry Point" and own and fval is not None:
                        e = entry_point_effect(ep, fval, r.get("EP_Function"))
                        if e:
                            fx.append(e)
                    sp = _q(r.get("Spell_EDID"))
                    if sp and sp not in spells_seen and (typ == "Ability" or ep in ("Apply Combat Hit Spell",)):
                        spells_seen.add(sp)
                        on_target = ep == "Apply Combat Hit Spell"
                        for s in self.spel.get(sp, []):
                            ok, notes = self._effect_applies(parse_conditions(s.get("Conditions")), rank, rank_perks)
                            if not ok:
                                continue
                            e = self.spell_effect(s, notes, on_target)
                            if e:
                                fx.append(e)
            # spells that switch on because you HAVE one of this card's rank perks
            # (HasPerk(...) = 1 in their conditions), e.g. Bringing the Big Guns
            for pe in rank_perks[:rank]:
                for sp in sorted(self.spells_by_perk.get(pe, ())):
                    if sp in spells_seen:
                        continue
                    spells_seen.add(sp)
                    for srow in self.spel.get(sp, []):
                        conds = parse_conditions(srow.get("Conditions"))
                        if not any(fn == "HasPerk" and arg in rank_perks for _, fn, arg, op, val in conds):
                            continue
                        ok, notes = self._effect_applies(conds, rank, rank_perks)
                        if not ok:
                            continue
                        e = self.spell_effect(srow, notes)
                        if e:
                            e["via"] = sp
                            fx.append(e)
            per_rank.append(fx)
        return per_rank

    def spell_effect(self, s, notes, on_target=False):
        mg_edid = _q(s.get("EFID_MGEF_EDID"))
        mg_name = _q(s.get("EFID_MGEF_FULL")) or mg_edid
        # the stat the effect changes (MGEF ActorValue, once the fixed MGEF
        # export is in) catches damage effects whose names don't say so
        av = mgef_actor_value(self.mgef.get(_q(s.get("EFID_MGEF_FormID")).upper()))
        # stack effects (Bullet Storm max stacks, raised by Bringing the Big
        # Guns) are counts, not damage %, but they set how big the damage gets
        stack = bool(re.search(r"stacks?$", mg_edid, re.I)) and not _q(s.get("MAGA_AVIF_EDID")) and not on_target
        if not stack and not re.search(r"dmg|damage|power ?attack", mg_edid + " " + mg_name + " " + av, re.I):
            return None
        if stack:
            mag = _num(s.get("EFIT_Magnitude"), 0) or 0
            if not mag:
                return None
            e = {"group": f"{mg_name} max stacks", "effect": mg_edid, "value": round(mag, 2), "unit": " stacks", "op": "+"}
            cond = [f"{k}:{v}" for k, v in notes]
            if cond:
                e["cond"] = cond
            return e
        if re.search(r"resist|reduc|taken.*less|incoming", mg_edid, re.I) and not on_target:
            return None
        mag = _num(s.get("EFIT_Magnitude"), 0) or 0
        src = ""
        magg = _q(s.get("MAGG_Global"))
        if magg:
            # "00340F52 Perk_WeaponBonus1" -> the GLOB's value is the magnitude
            g = self.glob.get(magg.split(" ")[0].upper())
            if g and g[1] is not None:
                mag, src = g[1], "GLOB " + g[0]
        scales = _q(s.get("MAGA_AVIF_EDID"))
        curve = _q(s.get("CVTO_CURV_EDID"))
        if on_target:
            # effects put ON the enemy: only ones that raise the damage it takes
            # count (Tenderizer "Increase Damage Taken"); Suppressor-style effects
            # lower the enemy's own damage and aren't part of yours
            if re.search(r"reduc|suppress|outgoing|output", mg_edid + " " + mg_name, re.I):
                return None
            group = (mg_name or "Damage taken") + " (on the enemy)"
            # stored as a fraction on some records (0.1) and a percent on others (20)
            val = mag * 100 if 0 < mag < 1 else mag
        else:
            group = mg_name
            val = mag
        e = {"group": group, "effect": mg_edid, "value": round(val, 2), "unit": "%"}
        if src:
            e["from"] = src
        if scales:
            e["scales"] = scales
        if curve:
            e["curve"] = curve
        if av:
            e["av"] = av
        cond = [f"{k}:{v}" for k, v in notes]
        if cond:
            e["cond"] = cond
        return e

    # ------------------------------------------------------------ mutations
    def mutation_effects(self, spell_edid):
        """Effects of Mutation_<Name> with their conditions decoded."""
        out = []
        for s in self.spel.get(spell_edid, []):
            mg = _q(s.get("EFID_MGEF_EDID"))
            if mg in ("Mutation_Treated_Effect", "abMutationCount"):
                continue
            conds = parse_conditions(s.get("Conditions"))
            flags = {}
            for _, fn, arg, op, val in conds:
                if fn == "IsTrueForConditionForm" and arg == "Mutation_Check_UseSuperVersion":
                    flags["version"] = "team"
                elif fn == "IsTrueForConditionForm" and arg.startswith("Mutation_Check_UseNormalVersion"):
                    flags["version"] = "normal"
                    # the Herd Mentality form adds IsMemberOfAPlayerTeam = 1 (CNDF)
                    cf = self.cndf.get(arg, {})
                    if "IsMemberOfAPlayerTeam|" in "".join(str(v) for v in cf.values()) and "|1.000000|IsMemberOfAPlayerTeam" in "".join(str(v) for v in cf.values()):
                        flags["when"] = "team"
                elif fn == "GetIsPlayerGhoul":
                    flags["race"] = "ghoul" if val == 1.0 else "human"
                elif fn == "IsSpellTarget" and arg.startswith("Serum_") and val == 0.0:
                    flags["serumRemoves"] = True
                elif fn == "IsSpellTarget" and arg.startswith("Serum_") and val == 1.0:
                    flags["serumOnly"] = True
                elif fn == "IsMemberOfAPlayerTeam":
                    flags["when"] = "team" if val == 1.0 else "solo"
                elif fn == "HasPerk" and arg.startswith("ClassFreak"):
                    flags.setdefault("classFreak", []).append(f"{arg}={int(val)}")
            out.append({
                "mgef": _q(s.get("EFID_MGEF_FormID")).upper(),
                "effect": mg,
                "magnitude": _num(s.get("EFIT_Magnitude"), 0),
                **flags,
            })
        return out


def _link_edid(cell):
    """'ArmorPenetration ""Armor Penetration"" [AVIF:00097341]' -> 'ArmorPenetration'"""
    return str(cell or "").strip().split(" ")[0].strip('"')


AV_GROUP = {
    "ArmorPenetration": "Armour penetration (legendary)",
    "CritDamageMult": "Critical hit damage",
}


def legendary_mod_effects(rec, omod_edid):
    """What a legendary mod really does: the actor values its OMOD adds and the
    effects of the enchantment it attaches (magnitude, and the curve when the
    strength depends on something like your health or caps)."""
    out = []
    for r in rec.omod_props.get(omod_edid, []):
        prop = _q(r.get("PropertyName"))
        tgt = _link_edid(r.get("Value1"))
        if prop in ("ActorValues", "Actor Values"):
            v = _num(r.get("Value2"), None)
            if v is None:
                continue
            e = {"av": tgt, "value": round(v, 2), "op": _q(r.get("FunctionType"))}
            if tgt in AV_GROUP:
                e["group"] = AV_GROUP[tgt]
            out.append(e)
        elif prop == "Enchantments":
            en = rec.ench.get(tgt)
            if not en:
                out.append({"ench": tgt})
                continue
            # ench_rows.normalize reads both ENCH export layouts (old exports
            # joined FormID:EditorID in one cell and shifted the rest left)
            en = normalize_ench(dict(en))
            for i in range(1, 41):
                fid = _q(en.get(f"Effect_{i}_MGEF_FID")).upper()
                if f"Effect_{i}_MGEF_FID" not in en:
                    break
                if not fid:
                    continue
                mrow = rec.mgef.get(fid, {})
                if not mrow:
                    continue
                edid = _q(en.get(f"Effect_{i}_MGEF_EID")) or _q(mrow.get("EDID"))
                e = {"ench": tgt, "effect": edid, "name": _q(mrow.get("FULL")) or edid,
                     "magnitude": _num(en.get(f"Effect_{i}_Magnitude"), 0)}
                curve = _q(en.get(f"Effect_{i}_CURV_EID"))
                if curve:
                    e["curve"] = curve
                av = mgef_actor_value(mrow)
                if av:
                    e["av"] = av
                desc = _q(mrow.get("DNAM_MagicItemDescription"))
                if desc:
                    e["desc"] = desc
                out.append(e)
    return out


def ep_function_kind(fn):
    """EP_Function as xEdit names it ('Multiply Value', 'Add Value',
    'Multiply 1 + Actor Value Mult' ...) or as the raw byte the ESM stores
    (1 Set, 2 Add, 3 Multiply, 5 Add AV Mult, 12 Set to AV Mult,
    13 Multiply AV Mult, 14 Multiply 1 + AV Mult, 10 Select Spell)."""
    s = str(fn or "").strip().strip('"').lower()
    if not s:
        return None
    raw = {"1": "set", "2": "add", "3": "mult", "5": "av_add", "12": "av_set",
           "13": "av_mult", "14": "av_mult1", "10": "spell"}
    if s in raw:
        return raw[s]
    if "spell" in s:
        return "spell"
    if "actor value" in s or " av " in f" {s} ":
        if "1 +" in s or "1+" in s:
            return "av_mult1"
        if s.startswith("multiply"):
            return "av_mult"
        if s.startswith("set"):
            return "av_set"
        return "av_add"
    if s.startswith("multiply"):
        return "mult"
    if s.startswith("add"):
        return "add"
    if s.startswith("set"):
        return "set"
    return None


# entry points whose value is the damage itself (a multiplier on it is a %,
# an Add is flat damage) vs ones whose value is already a bonus fraction
_EP_DAMAGE = {
    "Mod Weapon Attack Damage": "Weapon attack damage",
    "Mod Typed Weapon Attack Damage": "Weapon attack damage",
    "Mod Bashing Damage": "Bash damage",
    "Mod Outgoing Limb Damage": "Limb damage",
    "Mod Weak Body Part Damage Mult": "Weak spot damage",
    "Mod Player Explosion Damage": "Explosion damage",
}
_EP_BONUS = {
    "Mod Weapon DMG Bonus Mult": "Weapon damage bonus",
    "Mod My Critical Hit Damage Mult": "Critical hit damage",
    "Mod Sneak Attack Mult": "Sneak attack damage",
}


def entry_point_effect(ep, v, fn=None):
    """Turn a PERK entry point + its float (+ its function, when the PERK
    export has EP_Function) into a readable damage effect. `op` on the result
    says how the game applies it: "x" multiplies, "+" adds."""
    kind = ep_function_kind(fn)
    if kind is not None:
        return _entry_point_effect_fn(ep, v, kind)
    return _entry_point_effect_legacy(ep, v)


def _entry_point_effect_fn(ep, v, kind):
    if kind in ("set", "spell", "av_set") or v is None:
        return None
    av = kind.startswith("av_")
    e = None
    if ep == "Mod Target Damage Resistance":
        if kind == "mult":
            e = {"group": "Ignore target armour (perk)", "value": round((1 - v) * 100, 2), "unit": "%", "op": "x"}
        elif kind == "add":
            e = {"group": "Target armour (flat)", "value": round(v, 2), "unit": "", "op": "+"}
        elif kind == "av_mult1":
            e = {"group": "Ignore target armour (perk)", "value": round(-v * 100, 2), "unit": "%", "op": "x"}
    elif ep in _EP_DAMAGE:
        name = _EP_DAMAGE[ep]
        if kind == "mult":
            e = {"group": name + " (multiplier)", "value": round((v - 1) * 100, 2), "unit": "%", "op": "x"}
        elif kind == "add":
            e = {"group": name + " (flat)", "value": round(v, 2), "unit": "", "op": "+"}
        elif kind in ("av_mult1", "av_mult"):
            e = {"group": name + " (multiplier)", "value": round(v * 100, 2), "unit": "%", "op": "x"}
        elif kind == "av_add":
            e = {"group": name + " (flat)", "value": round(v, 2), "unit": "", "op": "+"}
    elif ep in _EP_BONUS:
        name = _EP_BONUS[ep]
        if kind in ("add", "av_add"):
            e = {"group": name, "value": round(v * 100, 2), "unit": "%", "op": "+"}
        elif kind in ("mult", "av_mult"):
            e = {"group": name + " (multiplier)", "value": round((v - 1) * 100, 2), "unit": "%", "op": "x"}
        elif kind == "av_mult1":
            e = {"group": name + " (multiplier)", "value": round(v * 100, 2), "unit": "%", "op": "x"}
    if not e or not e["value"]:
        return None
    e["effect"] = ep
    if av:
        e["scales"] = "actor value"
    return e


def _entry_point_effect_legacy(ep, v):
    """Older PERK exports have no EP_Function: use the function the game
    uses for most perks on each entry point."""
    if ep == "Mod Target Damage Resistance":
        return {"group": "Ignore target armour (perk)", "effect": ep, "value": round((1 - v) * 100, 2), "unit": "%"}
    if ep == "Mod Sneak Attack Mult":
        return {"group": "Sneak attack damage", "effect": ep, "value": round(v * 100, 2), "unit": "%"}
    if ep == "Mod My Critical Hit Damage Mult":
        return {"group": "Critical hit damage", "effect": ep, "value": round(v * 100, 2), "unit": "%"}
    if ep in ("Mod Weapon Attack Damage", "Mod Typed Weapon Attack Damage"):
        return {"group": "Weapon attack damage (multiplier)", "effect": ep, "value": round((v - 1) * 100, 2), "unit": "%"}
    if ep == "Mod Weapon DMG Bonus Mult":
        return {"group": "Weapon damage bonus", "effect": ep, "value": round(v * 100 if abs(v) < 5 else v, 2), "unit": "%"}
    if ep == "Mod Bashing Damage":
        return {"group": "Bash damage (multiplier)", "effect": ep, "value": round((v - 1) * 100, 2), "unit": "%"}
    if ep == "Mod Outgoing Limb Damage" and v < 100:
        return {"group": "Limb damage (multiplier)", "effect": ep, "value": round((v - 1) * 100, 2), "unit": "%"}
    return None
