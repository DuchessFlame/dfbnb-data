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
        rows = _read(p("MGEF_Export_*.tsv"))
        if rows:
            h = rows[0]
            i_ed, i_full, i_desc = h.index("EDID"), h.index("FULL"), h.index("DNAM_MagicItemDescription")
            for r in rows[1:]:
                if len(r) > i_desc:
                    self.mgef[r[0].upper()] = (r[i_ed], r[i_full].strip('"'), r[i_desc])
        self.ench = {}
        rows = _read(p("ENCH_Export_*.tsv"))
        if rows:
            h = rows[0]
            i_cnt = h.index("Effects_Count")
            for r in rows[1:]:
                if len(r) <= i_cnt:
                    continue
                n = int(_num(r[i_cnt]) or 0)
                effs, base = [], i_cnt + 1
                # Each effect: "FID:EID", magnitude, area, duration, curve. The
                # header names are one column out from the data, so go by position.
                for k in range(n):
                    c = r[base + k * 6: base + k * 6 + 6] + [""] * 6
                    if not c[0]:
                        continue
                    if ":" in c[0]:
                        # older export: "FID:EID" in one column, the rest one column early
                        fid, eid = c[0].split(":", 1)
                        mag, dur = _num(c[1]), _num(c[3])
                    else:
                        fid, eid, mag, dur = c[0], c[1], _num(c[2]), _num(c[4])
                    effs.append((fid.upper(), eid, mag, dur))
                self.ench[r[0].upper()] = (r[1], r[2].strip('"'), effs)

    # --- one property -> zero or more lines ---------------------------------
    def _perk_lines(self, fid):
        rec = self.perk.get(fid)
        if not rec:
            return []
        d = _clean_desc(rec[2])
        return [d] if d else []

    def _ench_lines(self, fid):
        rec = self.ench.get(fid)
        if not rec:
            return []
        out = []
        for mfid, eid, mag, dur in rec[2]:
            m = self.mgef.get(mfid, (eid, "", ""))
            d = _clean_desc(m[2], mag=mag, dur=dur)
            if d:
                out.append(d)
                continue
            if _VISUAL_MGEF.search(m[0] or eid):
                continue
            dt = _DT_MGEF.match(m[0] or eid)
            if dt:
                kind = _DT_NAME.get(dt.group(1).lower(), dt.group(1).title())
                out.append(f"Adds {kind} damage")
            elif re.search(r"bleed", (m[0] or eid) + " " + rec[1], re.I):
                out.append("Causes Bleed damage")
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
                out += self._ench_lines(_fid(v1, "ENCH"))
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
            m = re.match(r"(?:Adds|Deals) (\w+) damage$", s)
            if m and re.search(r"\d\S* " + m.group(1).lower() + " damage", joined):
                continue   # "+16 Poison Damage" already says it
            if s.lower() not in seen:
                seen.add(s.lower())
                uniq.append(s)
        return uniq


def describe(idx, omod_fid, desc):
    """Text for one unique mod: DESC if it states a number, else DESC + lines."""
    desc = (desc or "").strip()
    lines = idx.lines_for(omod_fid)
    if desc and re.search(r"\d", desc):
        return _clean_desc(desc) + ("." if desc.rstrip().endswith(".") else "")
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
