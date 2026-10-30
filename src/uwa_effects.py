"""Plain-English text for what a unique mod does, read from the game files.

Most unique mods (mod_Custom_*) have no DESC, so the page showed the mod name
and nothing under it. The effect is still in the files — it just lives in the
mod's properties (OMOD_Export_*_Properties.tsv) and the records they point at:

  Perk          -> PERK.DESC                  ("Attacks Restore Teammates Health")
  Enchantments  -> each MGEF.DNAM description ("Sneak damage increased by 50%")
  ActorValues   -> AVIF.DESC, <VALUE> filled  ("+15 Damage to Distant Targets")
  DamageTypeValues / AttackDamage / ReloadSpeed / Speed / AP cost ... -> numbers
  CritEffect, OverrideProjectile, Ammo        -> short lines

Nothing is per-item. A mod whose properties only add keywords, sounds, models
or paint has no combat effect — the item is "just a named weapon".

DESC wins when it states a number. A DESC with no number is flavour text
("Sprays Burning Meadow Breeze"), so the property lines are added after it.

30 Oct 2026 (Duchess): three kinds of line are added even after a DESC that
states a number, because the DESC never says them:
  - the game's own effect description (MGEF.DNAM) - Circuit Breaker's energy
    blast, Ice Breaker's Cryo Hazard, Super-Heated's DR cut;
  - curve scaling and caps (ENCH effect CURV + its input actor value, MAGA):
    "+5 at 1 END ... +50 at 100 END", "+2% per STR (up to +200% at 100 STR)";
  - how long damage over time lasts ("Adds Poison damage over 38 seconds").
Generic: read from the ENCH / MGEF / CURV exports, nothing per item.
"""
import csv
import re

NAMED_ONLY_WEAPON = "Named weapon only — no unique effect"
NAMED_ONLY_ARMOUR = "Named armour only — no unique effect"

_FID = re.compile(r"\[([A-Z]{4}):([0-9A-Fa-f]{8})\]")
_QUOTED = re.compile(r'""(.*?)""')
_TOKEN = re.compile(r"<[^>]*>")

# Float properties that change the weapon's numbers. Label is what the reader
# sees; MUL+ADD values are fractions (0.45 -> +45%).
_FLOAT_LABEL = {
    "AttackDamage": "damage",
    "Speed": "attack speed",
    "ReloadSpeed": "reload speed",
    "AttackActionPointCost": "AP cost",
    "SneakAttackMult": "sneak attack damage",
    "CriticalDamageMult": "critical damage",
    "CriticalChargeBonus": "critical meter fill",
    "Rate of Fire": "fire rate",
    "AttackDelaySec": "attack delay",
    "MinRange": "range", "MaxRange": "range",
    "Weight": "weight",
}
_ACCURACY = re.compile(r"^AimModel", re.I)
# Visual / bookkeeping properties — never an effect.
_SKIP_PROPS = {"Keywords", "Value", "MaterialSwaps", "ModelSwap", "AttackSound",
               "IdleSound", "ImpactDataSet", "NPCAmmoList", "Health",
               "AttackSoundMono", "EquipSound", "UnEquipSound", "ZoomData"}
# Effects that are only a visual (fire shader) or carry no reader-facing text.
_VISUAL_MGEF = re.compile(r"^FX|Visuals?$|TextDummy$|Shader", re.I)
_DT_MGEF = re.compile(r"^dt(Fire|Poison|Cryo|Energy|Radiation|Bleed|Electrical)", re.I)
_DT_NAME = {"electrical": "Energy"}


def _read(path):
    if not path:
        return []
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        return list(csv.reader(fh, delimiter="\t"))


def _fid(text, sig=None):
    for m in _FID.finditer(text or ""):
        if sig is None or m.group(1) == sig:
            return m.group(2).upper()
    return ""


def _quoted(text):
    m = _QUOTED.search(text or "")
    return (m.group(1) if m else "").strip()


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fmt(v):
    v = round(v, 2)
    return str(int(v)) if v == int(v) else ("%g" % v)


def _clean_desc(text, value=None, mag=None, dur=None):
    """Fill the tokens we can; drop the 'Currently <...>' half we can't."""
    s = (text or "").replace('"', "").strip()
    if not s or s.startswith("<CALL="):
        return ""
    # "(Currently <+MAG>)" / ". Currently <...>%" — a live value, not ours to show.
    s = re.sub(r"\s*\(?\s*\.?\s*Currently\b[^)]*\)?\s*$", "", s, flags=re.I)
    if value is not None:
        s = re.sub(r"<\+VALUE>", ("+" if value >= 0 else "") + _fmt(value), s)
        s = s.replace("<VALUE>", _fmt(value))
    if mag is not None:
        s = re.sub(r"<\+MAG>", "+" + _fmt(mag), s).replace("<MAG>", _fmt(mag))
    if dur:
        s = s.replace("<DUR>", _fmt(dur) + " seconds")
    s = re.sub(r"\s*up to <\+?MAG\.MAX>%?", "", s, flags=re.I)
    if _TOKEN.search(s):
        s = re.sub(r"\+?<[^>]*>%?\s*", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip(" .,")
    return s


def _pct_chance(s):
    """'+30 Chance to ...' / '+10 Deflect Chance' are percentages."""
    if re.search(r"chance", s, re.I):
        s = re.sub(r"(?<![\d.%])(\+?\d+(?:\.\d+)?)(?![\d.%])", r"\1%", s, count=1)
    return s


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


_SPECIAL = {"strength": "STR", "perception": "PER", "endurance": "END", "charisma": "CHA",
            "intelligence": "INT", "agility": "AGI", "luck": "LCK"}


def _curve_shape(pts):
    """'binary' (inputs 0 and 1 only), 'linear' (y = r*x), else 'points'."""
    xs = sorted({x for x, _ in pts})
    if xs == [0.0, 1.0]:
        return "binary"
    pos = [(x, y) for x, y in pts if x > 0]
    if pos and all(y == 0 for x, y in pts if x == 0):
        r = pos[0][1] / pos[0][0]
        if r and all(abs(y - r * x) < 1e-6 * max(1.0, abs(y)) for x, y in pos):
            return "linear"
    return "points"


def _human(edid):
    s = re.sub(r"^[A-Za-z0-9]{2,6}_(?=Projectile)", "", edid or "")
    s = re.sub(r"^Projectile|Projectile", "", s)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s.replace("_", " "))
    return re.sub(r"\s+", " ", s).strip()


class EffectIndex:
    def __init__(self, pick, channel):
        p = lambda pat, ex=None: (pick(pat, channel, exclude=ex) if ex else pick(pat, channel))
        self.props = {}
        rows = _read(p("OMOD_Export_*_Properties.tsv"))
        for r in rows[1:]:
            if len(r) >= 9:
                self.props.setdefault(r[0].upper(), []).append(r)
        self.omod, self.omod_by_full = {}, {}
        rows = _read(p("OMOD_Export_*.tsv", "_Properties"))
        if rows:
            h = rows[0]
            i_inc = h.index("Includes_Flat")
            for r in rows[1:]:
                if len(r) <= i_inc:
                    continue
                fid = r[0].upper()
                inc = [m.group(2).upper() for m in _FID.finditer(r[i_inc]) if m.group(1) == "OMOD"]
                self.omod[fid] = (r[1], r[2].strip('"'), r[3].strip('"'), inc)
                self.omod_by_full.setdefault(_norm(r[2]), []).append(fid)
        self.perk = {}
        for r in _read(p("PERK_Export_*.tsv"))[1:]:
            if len(r) >= 4:
                old = self.perk.get(r[0].upper())
                # the export repeats a perk once per rank row; keep the filled one
                if not old or (not old[1] and r[2].strip('"')) or (not old[2] and r[3]):
                    self.perk[r[0].upper()] = (r[1], r[2].strip('"') or (old or ("", "", ""))[1],
                                               r[3] or (old or ("", "", ""))[2])
        self.avif = {}
        for r in _read(p("AVIF_Export_*.tsv"))[1:]:
            if len(r) >= 4:
                self.avif[r[0].upper()] = (r[1], r[2].strip('"'), r[3])
        self.mgef = {}
        self.mgef_perk = {}   # MGEF -> the perk it applies (EDID), e.g. MoM_VoiceofSetPerk
        rows = _read(p("MGEF_Export_*.tsv"))
        if rows:
            h = rows[0]
            i_ed, i_full, i_desc = h.index("EDID"), h.index("FULL"), h.index("DNAM_MagicItemDescription")
            i_pk = h.index("PerkToApply") if "PerkToApply" in h else None
            for r in rows[1:]:
                if len(r) > i_desc:
                    self.mgef[r[0].upper()] = (r[i_ed], r[i_full].strip('"'), r[i_desc])
                    if i_pk is not None and len(r) > i_pk and r[i_pk].strip():
                        self.mgef_perk[r[0].upper()] = r[i_pk].strip().split(" ")[0]
        self.ench = {}
        rows = _read(p("ENCH_Export_*.tsv"))
        if rows:
            h = rows[0]
            i_cnt = h.index("Effects_Count")
            # Columns per effect, from the header (8 since the Python export:
            # MGEF_FID, MGEF_EID, Magnitude, Area, Duration, CURV_EID, MAGA_FID,
            # MAGA_EID). Reading 6 a time misread every effect after the first.
            e1 = [c for c in h if re.match(r"Effect_1_", c)]
            stride = len(e1) or 6
            col = {c[len("Effect_1_"):]: k for k, c in enumerate(e1)}
            for r in rows[1:]:
                if len(r) <= i_cnt:
                    continue
                n = int(_num(r[i_cnt]) or 0)
                effs, base = [], i_cnt + 1
                for k in range(n):
                    c = r[base + k * stride: base + (k + 1) * stride] + [""] * stride
                    if not c[0]:
                        continue
                    if ":" in c[0]:
                        # older export: "FID:EID" in one column, the rest one column early
                        fid, eid = c[0].split(":", 1)
                        mag, dur, curve, maga = _num(c[1]), _num(c[3]), "", ""
                    elif col:
                        g = lambda nm: c[col[nm]] if nm in col else ""
                        fid, eid = g("MGEF_FID"), g("MGEF_EID")
                        mag, dur = _num(g("Magnitude")), _num(g("Duration"))
                        curve, maga = g("CURV_EID").strip(), g("MAGA_EID").strip()
                    else:
                        fid, eid, mag, dur, curve, maga = c[0], c[1], _num(c[2]), _num(c[4]), "", ""
                    if "NULL" in curve:
                        curve = ""
                    effs.append((fid.upper(), eid, mag, dur, curve, maga))
                self.ench[r[0].upper()] = (r[1], r[2].strip('"'), effs)
        # Curve points by EditorID: {EDID: [(x, y), ...]} (CURV POINTS export).
        self.curve = {}
        for r in _read(p("CURV_Export_*_POINTS.tsv"))[1:]:
            if len(r) >= 4 and _num(r[2]) is not None and _num(r[3]) is not None:
                self.curve.setdefault(r[1], []).append((_num(r[2]), _num(r[3])))
        for k in self.curve:
            self.curve[k].sort()
        self.avif_by_edid = {v[0]: v[1] for v in self.avif.values()}
        # A perk's Ability spells, and each spell's effects with their conditions
        # (Voice of Set: one description without Eye of Ra worn, one with it).
        self.perk_abil = {}
        rows = _read(p("PERKEP_Export_*.tsv"))
        if rows:
            h = rows[0]
            if {"PERK_EDID", "EffectType", "Ability_FormID"} <= set(h):
                i_e, i_t, i_a = h.index("PERK_EDID"), h.index("EffectType"), h.index("Ability_FormID")
                for r in rows[1:]:
                    if len(r) > i_a and r[i_t] == "Ability" and r[i_a].strip():
                        self.perk_abil.setdefault(r[i_e], []).append(r[i_a].strip().upper())
        self.spel_eff = {}
        rows = _read(p("SPEL_Export_*_EFFECTS.tsv"))
        if rows:
            h = rows[0]
            if {"SPEL_FormID", "EFID_MGEF_FormID", "Conditions"} <= set(h):
                i_s, i_m, i_c = h.index("SPEL_FormID"), h.index("EFID_MGEF_FormID"), h.index("Conditions")
                for r in rows[1:]:
                    if len(r) > i_c:
                        self.spel_eff.setdefault(r[i_s].upper(), []).append((r[i_m].upper(), r[i_c]))
        # Keyword -> the name of the item that carries it (MoMEyeOfRaItemKeyword -> "Eye of Ra")
        self.kw_item = {}
        rows = _read(p("ARMO_Export_*_ARMOUR.tsv"))
        if rows:
            h = rows[0]
            kcols = [i for i, c in enumerate(h) if re.match(r"KW\d+$", c)]
            i_full = h.index("ARMO_FULL") if "ARMO_FULL" in h else None
            for r in rows[1:]:
                full = r[i_full].strip('"').strip() if i_full is not None and len(r) > i_full else ""
                if not full:
                    continue
                for i in kcols:
                    c = r[i].strip() if i < len(r) else ""
                    if c:
                        # "004E60E2:MoMEyeOfRaItemKeyword:KYWD" (or a bare EDID)
                        bits = c.split(":")
                        self.kw_item.setdefault(bits[1] if len(bits) >= 3 else c.split(" ")[0], full)
        self._curve_unit = {}
        self.last_kinds = {}

    # --- one property -> zero or more lines ---------------------------------
    def _perk_lines(self, fid):
        rec = self.perk.get(fid)
        if not rec:
            return []
        d = _clean_desc(rec[2])
        return [d] if d else []

    # --- curve-driven effects --------------------------------------------
    def _input_label(self, maga):
        """What the curve's input actor value is, in reader words."""
        e = (maga or "").strip()
        low = e.lower()
        if low in _SPECIAL:
            return _SPECIAL[low]
        if low == "healthpercent":
            return "health"
        if low == "speedmult":
            return "speed"
        full = self.avif_by_edid.get(e, "")
        if full:
            return full
        e = re.sub(r"^(STAT|MoM|AV)_", "", e)
        e = re.sub(r"Count$", "s", e)   # AddictionCount -> "Addictions"
        return re.sub(r"(?<=\s)(Of|The|And|With)\b", lambda m: m.group(1).lower(), _human(e))

    def _x_text(self, x, maga, label):
        low = (maga or "").lower()
        if low == "healthpercent":
            return f"{_fmt(x * 100)}% health"
        if low == "speedmult":
            return f"{_fmt(x)}% speed"
        return f"{_fmt(x)} {label}"

    def _scale_line(self, pts, maga, unit):
        label = self._input_label(maga)
        sign = lambda y: ("+" if y > 0 else "") + _fmt(y) + (unit if y else "")
        shape = _curve_shape(pts)
        ymax = max(y for _, y in pts)
        xmax = max(x for x, y in pts if y == ymax)
        if shape == "linear":
            r = next(y / x for x, y in pts if x > 0)
            return (f"{sign(r)} per {label} (up to {sign(ymax)} at "
                    f"{self._x_text(xmax, maga, label)})")
        return "Scales: " + ", ".join(f"{sign(y)} at {self._x_text(x, maga, label)}"
                                      for x, y in pts)

    def _perk_ability_lines(self, perk_edid):
        """Descriptions on the perk's Ability spells. One gated on a worn item
        (WornHasKeyword = 1) reads "With <item> worn: ..."; its != 1 twin is the
        plain line."""
        out = []
        for sp in self.perk_abil.get(perk_edid, []):
            for mfid, cond in self.spel_eff.get(sp, []):
                d = _clean_desc(self.mgef.get(mfid, ("", "", ""))[2])
                if not d:
                    continue
                m = re.search(r"WornHasKeyword\((\w+)[^)]*\)\s*(!=|==|=)\s*1(?:\.0+)?\b", cond or "")
                if m and m.group(2) != "!=":
                    item = self.kw_item.get(m.group(1)) or _human(re.sub(r"(Item)?Keyword$", "", m.group(1)))
                    d = f"With {item} worn: {d}"
                out.append(d)
        return out

    def _ench_lines(self, fid, hint=""):
        """[(text, kind)]; kind: mgef / scale / dot / plain."""
        rec = self.ench.get(fid)
        if not rec:
            return []
        out, dots = [], {}
        for mfid, eid, mag, dur, curve, maga in rec[2]:
            m = self.mgef.get(mfid, (eid, "", ""))
            pts = self.curve.get(curve) if curve and maga else None
            if pts and _curve_shape(pts) != "binary" and max(abs(y) for _, y in pts) <= 1:
                # fractions (0.05 = 5%): Quick Fix's swing speed multiplier
                pts = [(x, round(y * 100, 4)) for x, y in pts]
                self._curve_unit.setdefault(curve, "%")
            if pts:
                # A curve with an input (MAGA): the number moves with a stat,
                # health, speed, a stack count or another item being worn.
                unit = self._curve_unit.get(curve)
                if unit is None:
                    has_tok = re.search(r"<\+?MAG", m[2] or "")
                    unit = "%" if (re.search(r"MAG(\.MAX)?>%", m[2] or "")
                                   or (not has_tok and "%" in (m[2] or hint))) else ""
                    self._curve_unit[curve] = unit
                ymax = max(y for _, y in pts)
                mx = _fmt(ymax) + unit
                desc = re.sub(r"<(\+?)MAG\.MAX>%?", lambda t: t.group(1) + mx, m[2] or "")
                desc = re.sub(r"\s*\(?\s*\.?\s*Currently\b[^)]*\)?\s*$", "", desc, flags=re.I)
                if _curve_shape(pts) == "binary":
                    y0 = next(y for x, y in pts if x == 0)
                    y1 = next(y for x, y in pts if x == 1)
                    d = _clean_desc(desc, mag=y0, dur=dur)
                    with_ = (f" ({'+' if y1 > 0 else ''}{_fmt(y1)}{unit} with "
                             f"{self._input_label(maga)})")
                    if d:
                        out.append((d + with_, "mgef"))
                    else:
                        out.append((f"{'+' if y0 > 0 else ''}{_fmt(y0)}{unit}{with_}".strip(), "scale"))
                    continue
                if re.search(r"<\+?MAG>", desc):
                    desc = re.sub(r"<\+MAG>", "up to +" + _fmt(ymax), desc)
                    desc = desc.replace("<MAG>", "up to " + _fmt(ymax))
                d = _clean_desc(desc, dur=dur)
                if d:
                    out.append((d, "mgef"))
                out.append((self._scale_line(pts, maga, unit), "scale"))
                continue
            d = _clean_desc(m[2], mag=mag, dur=dur)
            if d:
                out.append((d, "mgef"))
                continue
            pk = self.mgef_perk.get(mfid)
            if pk:
                pl = self._perk_ability_lines(pk)
                if pl:
                    out += [(l, "mgef") for l in pl]
                    continue
            if _VISUAL_MGEF.search(m[0] or eid):
                continue
            dt = _DT_MGEF.match(m[0] or eid)
            if dt:
                kind = _DT_NAME.get(dt.group(1).lower(), dt.group(1).title())
            elif re.search(r"bleed", (m[0] or eid) + " " + rec[1], re.I):
                kind = "Bleed"
            else:
                continue
            # Damage over time: keep the longest, the damage itself climbs with level.
            dots[kind] = max(dots.get(kind, 0), dur or 0)
        for kind, dur in dots.items():
            verb = "Causes" if kind == "Bleed" else "Adds"
            if dur:
                out.append((f"{verb} {kind} damage over {_fmt(dur)} seconds", "dot"))
            else:
                out.append((f"{verb} {kind} damage", "plain"))
        return out

    def _av_lines(self, row):
        fid = _fid(row[6], "AVIF")
        val, curve = _num(row[7]), "NULL" not in (row[8] or "NULL")
        rec = self.avif.get(fid)
        if not rec:
            return []
        edid, full, desc = rec
        if desc.strip().startswith("<CALL="):
            return [f"Gives the {full} effect"] if full else []
        if val == 0 and curve:
            return [f"Bonus {full} (goes up with level)"] if full else []
        d = _clean_desc(desc, value=val)
        if d:
            return [_pct_chance(d)]
        if not full or val is None:
            return []
        if row[4] == "SET":
            return [f"{full}: {_fmt(val)}"]
        return [_pct_chance(f"{'+' if val >= 0 else ''}{_fmt(val)} {full}")]

    def _dmgt_line(self, row):
        name = _quoted(row[6]) or _human(row[6].split(" ")[0])
        val, curve = _num(row[7]), "NULL" not in (row[8] or "NULL")
        if val is None:
            return []
        if row[4] == "SET" and val == 0:
            return [f"No {name}"]
        if row[4] == "MUL+ADD":
            return [f"{'+' if val >= 0 else ''}{_fmt(val * 100)}% {name}"]
        tail = " (goes up with level)" if curve else ""
        return [f"+{_fmt(val)} {name}{tail}"] if val else []

    def lines_for(self, omod_fid, depth=0):
        """Every effect line for one OMOD, in property order, de-duplicated."""
        out, dmg_kw = [], []
        accuracy = None
        kinds = {}
        o = self.omod.get((omod_fid or "").upper(), ("", "", "", []))
        hint = (o[2] or "") + " " + (o[1] or "")
        for r in self.props.get((omod_fid or "").upper(), []):
            fn, prop, v1, v2 = r[4], r[5], r[6], r[7]
            if prop == "Keywords":
                m = re.search(r"dn_UniqueEffect(\w+?)Damage", v1)
                if m and fn == "ADD":
                    dmg_kw.append(m.group(1))
                elif fn == "ADD" and re.match(r"\s*dn_", v1) and _quoted(v1):
                    out.append(_quoted(v1))   # description keyword: in-game text
                continue
            if prop in _SKIP_PROPS:
                continue
            if prop in ("116", "Perk"):
                out += self._perk_lines(_fid(v1, "PERK"))
            elif prop == "Enchantments":
                for text, kind in self._ench_lines(_fid(v1, "ENCH"), hint):
                    out.append(text)
                    kinds[text] = kind
            elif prop in ("ActorValues", "Actor Values"):
                out += self._av_lines(r)
            elif prop == "DamageTypeValues":
                out += self._dmgt_line(r)
            elif prop == "CritEffect":
                spel = re.match(r"\s*(\S+)", v1)
                kind = _human(re.sub(r"^Crit", "", spel.group(1))) if spel else ""
                if kind:
                    out.append(f"Critical hits cause {kind.lower()} damage")
            elif prop == "OverrideProjectile":
                name = _human(re.match(r"\s*(\S+)", v1).group(1))
                out.append("Fires incendiary rounds" if "incendiary" in name.lower()
                           else "Fires its own special projectile")
            elif prop == "Ammo" and fn == "SET":
                ammo = _quoted(v1)
                if ammo:
                    out.append(f"Uses {ammo} ammo")
            elif _ACCURACY.match(prop):
                accuracy = accuracy if accuracy is not None else _num(v1)
            elif prop in _FLOAT_LABEL:
                val = _num(v1)
                if not val:
                    continue
                label = _FLOAT_LABEL[prop]
                if prop == "SneakAttackMult" and "sneak" in " ".join(out).lower():
                    continue   # an enchant line already says it ("Sneak damage increased by 50%")
                if fn == "MUL+ADD":
                    out.append(f"{'+' if val >= 0 else ''}{_fmt(val * 100)}% {label}")
                elif fn == "ADD":
                    pct = val * 100 if prop == "SneakAttackMult" else val
                    out.append(f"+{_fmt(pct)}{'%' if prop == 'SneakAttackMult' else ''} {label}")
        # A mod that is only a set of variants (SDOW_Mod_Custom_RelicReaper
        # includes six _CanDo / _PharmaFarma... mods, each adding one perk):
        # the item rolls one of them.
        if not out and depth < 2:
            subs = [f for f in self.omod.get((omod_fid or "").upper(), ("", "", "", []))[3]
                    if not self.omod.get(f, ("",))[0].startswith("_PARENT_")]
            perks, other = [], []
            for f in subs:
                pk = [self.perk.get(_fid(r[6], "PERK"), ("", "", ""))[1] or _quoted(r[6])
                      for r in self.props.get(f, []) if r[5] in ("116", "Perk")]
                pk = [x for x in pk if x]
                if pk:
                    perks += [x for x in pk if x not in perks]
                else:
                    other += [l for l in self.lines_for(f, depth + 1) if l not in other]
            if len(perks) > 1 and len(subs) > 1:
                out.append("Grants one perk, picked at random: " + ", ".join(perks))
            elif perks:
                out.append("Grants the " + perks[0] + " perk")
            out += other
        # Shared templates the mod includes (_PARENT_mod_WEAPON_Cursed carries
        # "Cursed: +35% Damage, ..."). TEMPLATE:/restriction parents say nothing.
        for inc in ([] if out else self.omod.get((omod_fid or "").upper(), ("", "", "", []))[3]):
            ed, full, desc, _ = self.omod.get(inc, ("", "", "", []))
            if full.upper().startswith("TEMPLATE") or not desc:
                continue
            d = _clean_desc(desc)
            if d:
                out.insert(0, d)
        if accuracy:
            # AimModel cone/recoil multipliers: bigger cone = less accurate.
            out.append(f"{'+' if accuracy > 0 else '-'}{_fmt(abs(accuracy) * 100)}% spread and recoil")
        said = " ".join(out).lower()
        for k in dmg_kw:
            if k.lower() not in said:
                out.append(f"Deals {k} damage")
        seen, uniq = set(), []
        joined = " ".join(out).lower()
        for s in out:
            m = re.match(r"(?:Adds|Deals|Causes) (\w+) damage$", s)
            if m and re.search(r"\d\S* " + m.group(1).lower() + " damage", joined):
                continue   # "+16 Poison Damage" already says it
            m = re.match(r"(?:Adds|Causes) (\w+) damage over (\S+) seconds$", s)
            if m and re.search(r"\d\S* " + m.group(1).lower() + " damage", joined):
                # the number is already there - just say how long it lasts
                t = f"{m.group(1)} damage lasts {m.group(2)} seconds"
                kinds[t] = kinds.get(s, "dot")
                s = t
            if s.lower() not in seen:
                seen.add(s.lower())
                uniq.append(s)
        if depth == 0:
            self.last_kinds = {k: v for k, v in kinds.items() if k in uniq}
        return uniq


def describe(idx, omod_fid, desc):
    """Text for one unique mod: DESC if it states a number, else DESC + lines."""
    desc = (desc or "").strip()
    lines = idx.lines_for(omod_fid)
    kinds = dict(idx.last_kinds)
    # A curve's "+R per X (up to ...)" line repeats a rate another line or the
    # DESC already gives - keep only the cap.
    said = (desc + " " + " ".join(l for l in lines if kinds.get(l) != "scale")).lower()
    if " per " in said:
        short = []
        for l in lines:
            m = re.match(r"\S+ per (.+) \(up to (\S+) at (.+)\)$", l)
            if m and kinds.get(l) == "scale":
                t = f"Up to {m.group(2)} at {m.group(3)}"
                kinds[t] = "scale"
                l = t
            short.append(l)
        lines = short
    if desc and re.search(r"\d", desc):
        base = _clean_desc(desc)
        # Lines the DESC never says: the game's own effect text, curve caps,
        # damage-over-time length (30 Oct 2026).
        extra = []
        for l in lines:
            k = kinds.get(l)
            if k not in ("mgef", "scale", "dot") or _norm(l) in _norm(base):
                continue
            m = re.match(r"\S+ per (.+) \(up to (\S+) at (.+)\)$", l)
            if m and " per " in base.lower():
                l = f"Up to {m.group(2)} at {m.group(3)}"   # the rate is already in the DESC
            extra.append(l)
        if extra:
            return base.rstrip(" .") + "; " + "; ".join(extra)
        return base + ("." if desc.rstrip().endswith(".") else "")
    if desc and lines:
        return desc.rstrip(" .") + ": " + "; ".join(lines)
    return desc or "; ".join(lines)


def apply(items, pick, channel):
    """Fill uniqueMods[].desc and inherentEffect for every item; flag named-only."""
    idx = EffectIndex(pick, channel)
    st = {"described": 0, "named_only": 0}
    for it in items:
        uniques = [m for m in (it.get("uniqueMods") or []) if m.get("kind") == "unique"]
        if not uniques:
            # The item's own custom mod, found by its in-game name
            # (EN06_mod_melee_Shovel_Custom_Cursed = "Cursed Shovel").
            for fid in idx.omod_by_full.get(_norm(it.get("name")), []):
                ed, full, desc, _ = idx.omod[fid]
                if re.search(r"custom", ed, re.I) and not re.search(r"(^|_)zzz|^bounty_", ed, re.I):
                    m = {"name": full, "edid": ed, "formId": fid, "kind": "unique", "desc": desc}
                    it.setdefault("uniqueMods", []).insert(0, m)
                    uniques.append(m)
                    st["mods_found"] = st.get("mods_found", 0) + 1
                    break
        # A fitted part named after the item (Vox Syringe Barrel on the Vox
        # Syringer) is what makes it special, not a standard part.
        words = {w for w in re.findall(r"[a-z0-9]+", (it.get("name") or "").lower()) if len(w) > 2}
        own_parts = [m for m in (it.get("uniqueMods") or []) if m.get("kind") == "preinstalled"
                     and (m.get("desc") or "").strip()
                     and words & set(re.findall(r"[a-z0-9]+", (m.get("name") or "").lower()))]
        any_effect = bool(own_parts)
        for m in uniques:
            text = describe(idx, m.get("formId"), m.get("desc"))
            if text and text != (m.get("desc") or "").strip():
                st["described"] += 1
            m["desc"] = text
            any_effect = any_effect or bool(text)
        inh = (it.get("inherentEffect") or "").strip()
        best = next((m["desc"] for m in uniques if m["desc"]), "") or \
            next((_clean_desc(m["desc"]) for m in own_parts), "")
        if best and (not inh or _norm(inh) in _norm(best) or "<" in inh):
            it["inherentEffect"] = best
        it.pop("namedOnly", None)
        if it.get("cosmeticOnly"):
            continue
        if not any_effect and not (it.get("inherentEffect") or "").strip():
            note = NAMED_ONLY_ARMOUR if it.get("kind") == "Armour" else NAMED_ONLY_WEAPON
            it["namedOnly"] = True
            it["inherentEffect"] = note
            for m in uniques:
                m["desc"] = note
            st["named_only"] += 1
    return st


# ── Tradeable, from the game files ──────────────────────────────────────────
_LEGACY_REC = re.compile(r"^(Survival|NW|NuclearWinter|DEPRECATED)_", re.I)


def _npt_from_mod(idx, fid, depth=0):
    """True when the mod adds NonPlayerTradable, False when it removes it,
    None when it says nothing (its included parents are asked too)."""
    res = None
    for r in idx.props.get((fid or "").upper(), []):
        if r[5] == "Keywords" and "NonPlayerTradable" in r[6]:
            res = r[4] == "ADD"
    if res is None and depth < 2:
        for inc in idx.omod.get((fid or "").upper(), ("", "", "", []))[3]:
            x = _npt_from_mod(idx, inc, depth + 1)
            if x is not None:
                res = x
    return res


def apply_tradeable(items, pick, channel, idx=None):
    """Set `tradeable` on every unique from the game files (Duchess: the files
    are the source of truth). An item can't be traded when the NonPlayerTradable
    keyword is on it — added by its unique mod (the Gleaming Depths raid
    weapons: RD01_Mod_Custom_*_CustomName) or carried by its own WEAP/ARMO
    record — unless the unique mod removes it (Last Stand). A retired copy's
    record (Survival_…) is not the item and is ignored.
    Returns the list of (name, old, new) that changed."""
    idx = idx or EffectIndex(pick, channel)
    rec_kw = {}
    for pat, fcol, ecol in (("WEAP_Export_*_Base.tsv", "WEAP_FormID", "WEAP_EDID"),
                            ("ARMO_Export_*_ARMOUR.tsv", "ARMO_FormID", "ARMO_EDID")):
        rows = _read(pick(pat, channel))
        if not rows:
            continue
        h = rows[0]
        if "Keywords" not in h:
            continue
        i_f, i_e, i_k = h.index(fcol), h.index(ecol), h.index("Keywords")
        for r in rows[1:]:
            if len(r) > i_k:
                rec_kw[r[i_f].strip('"').upper()[-8:].zfill(8)] = (r[i_e].strip('"'), r[i_k])
    changed = []
    for it in items:
        mods = [_npt_from_mod(idx, m.get("formId")) for m in it.get("uniqueMods") or []
                if m.get("kind") == "unique"]
        recs = {(f or "").upper() for f in [it.get("formId")] + list(it.get("recordIds") or []) if f}
        rec_npt = any("NonPlayerTradable" in rec_kw.get(f, ("", ""))[1]
                      and not _LEGACY_REC.match(rec_kw.get(f, ("", ""))[0]) for f in recs)
        if any(x is True for x in mods):
            trade = False
        elif any(x is False for x in mods):
            trade = True
        else:
            trade = not rec_npt
        if it.get("tradeable") != trade:
            changed.append((it.get("name"), it.get("tradeable"), trade))
        it["tradeable"] = trade
        for r in it.get("obtainRoutes") or []:
            if r.get("populated"):
                r["tradeable"] = trade
    return changed
