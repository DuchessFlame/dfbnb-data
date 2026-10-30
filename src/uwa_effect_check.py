"""Check every unique's effect text against the numbers its records really use.

Duchess (30 Oct 2026): Bethesda don't always update descriptions, so the page
goes by the item's real stats. A description is only used as-is when its numbers
match the records; if they don't, the page says what the files say and the item
goes on a "description doesn't match" list in the build log (a bug report for
Bethesda); if the files hold nothing to check it against, the page says so.

The claims are the description texts: the unique mod's own DESC, the static
text of its effects (MGEF descriptions with no <MAG>-style token - a token is
filled from the record, so it is real), its perks' DESC, and the descriptions on
those perks' Ability spells.

The real numbers come from every record the effect uses, nothing per item:
  - the mod's properties (fractions also as %: 0.15 -> 15)
  - its enchantments: magnitude, duration, curve points (and per-unit rates),
    the global a magnitude reads (MAGG), "1 per" when a magnitude is an actor
    value with no curve (Rage stacks, kill streak)
  - every perk it grants (props 116 / Perk, and MGEF PerkToApply): entry point
    values (a Multiply 0.95 is also "5% less"), their curve tables and condition
    values (Daisycutter's 1000 RAD), their Ability spells, and the spells their
    entry points cast
  - spells gated on the item itself: SPEL effects whose conditions ask for one
    of the keywords the mod adds (WornHasKeyword - Foundation's Vengeance,
    Valkyrie, Tone Death, The Action Hero)
  - perk-rank spells are read with their HasPerk conditions worked out from the
    perks the mod grants, so a mod that hands out all three Demolition Expert
    ranks counts as rank 3 (+60%), not rank 1.

Result on each unique mod: effectCheck = {"status": confirmed | partial |
mismatch | unconfirmed, "claimed": [...], "notFound": [...], "files": [...]}.
"""
import csv
import re

import uwa_effects as U

_NUM = re.compile(r"(?<![A-Za-z0-9.])(\d+(?:\.\d+)?)")
_TOKEN = re.compile(r"<[^>]*>")
_HASPERK = re.compile(r"HasPerk\((\w+)[^)]*\)\s*(!=|==|=)\s*(\d+(?:\.\d+)?)")
_WORNKW = re.compile(r"WornHasKeyword\((\w+)")
_SPELL_IN_RAW = re.compile(r"Spell=\S+ \[SPEL:([0-9A-Fa-f]{8})\]")
_CURVE_IN_RAW = re.compile(r"Curve Table=(\S+)")
_COND_VAL = re.compile(r"\)\s+[01]{8}\s+(-?\d+(?:\.\d+)?)"            # perk: ") 11000000 1000.0"
                       r"|\)\s*(?:<=|>=|<|>|!=|==|=)\s*(-?\d+\.\d+)")    # spell: ") >= 0.250000"
_EDID_NUM = re.compile(r"[A-Za-z](\d{1,3})$")                            # ParalyzeEffect25

STATUS_NOTE = {
    "partial": "not confirmed in the game files: {nf}",
    "mismatch": "the game files say: {files}",
    "unconfirmed": "not confirmed in the game files",
}


def _camel(s):
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", (s or "").replace("_", " ")).strip()


def _sign(v, pct=False):
    t = U._fmt(v * 100 if pct else v) + ("%" if pct else "")
    return t if t.startswith("-") else "+" + t


def _rows(path):
    if not path:
        return []
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        rd = csv.reader(fh, delimiter="\t")
        h = next(rd, None)
        return [dict(zip(h, r)) for r in rd] if h else []


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _add(nums, v, frac=False):
    """Add a value and the forms a description might print it in."""
    if v is None:
        return
    nums.add(round(abs(v), 4))
    if frac or abs(v) < 1:
        nums.add(round(abs(v) * 100, 4))
        if 0 < v < 1:
            nums.add(round((1 - v) * 100, 4))   # Multiply 0.95 = "5% less"
        if 1 < v < 2:
            nums.add(round((v - 1) * 100, 4))   # Multiply 1.3 = "+30%"


class Checker:
    def __init__(self, idx, pick, channel):
        self.idx = idx
        p = lambda pat: pick(pat, channel)
        self.perkep = {}
        for r in _rows(p("PERKEP_Export_*.tsv")):
            self.perkep.setdefault(r.get("PERK_EDID", ""), []).append(r)
        self.perk_by_fid = {fid: rec[0] for fid, rec in idx.perk.items()}
        self.perk_desc = {rec[0]: rec[2] for rec in idx.perk.values()}
        self.spel = {}
        self.spel_by_kw = {}
        for r in _rows(p("SPEL_Export_*_EFFECTS.tsv")):
            sf = (r.get("SPEL_FormID") or "").upper()
            self.spel.setdefault(sf, []).append(r)
            for kw in _WORNKW.findall(r.get("Conditions") or ""):
                self.spel_by_kw.setdefault(kw, []).append(r)
        self.glob = {r.get("EDID", ""): _f(r.get("FLTV")) for r in _rows(p("GLOB_Export_*.tsv"))}
        # MAGA by (enchantment, effect index): the export gives the FormID even when
        # the actor value is a hard-coded one with no EditorID (Rage stacks).
        self.maga = {}
        for r in _rows(p("ENCH_Export_*.tsv")):
            ef = (r.get("ENCH_FormID") or "").upper()
            for k in range(1, 21):
                if r.get(f"Effect_{k}_MAGA_FID"):
                    self.maga[(ef, k - 1)] = r[f"Effect_{k}_MAGA_FID"]
        self.enchfx = {}
        for r in _rows(p("ENCHFX_Export_*.tsv")):
            self.enchfx.setdefault((r.get("ENCH_FormID") or "").upper(), []).append(r)

    # ---- the real numbers ----------------------------------------------
    def _curve(self, name, nums, files, label=""):
        pts = self.idx.curve.get((name or "").split(" ")[0])
        if not pts:
            return
        frac = max(abs(y) for _, y in pts) <= 2
        nums.add(round(max(x for x, _ in pts), 4))   # where the curve stops ("Max 10")
        for x, y in pts:
            _add(nums, y, frac)
            if x > 0 and y:
                _add(nums, y / x, frac)
        if label:
            files.append(f"{label}: curve " + ", ".join(f"{U._fmt(y)} at {U._fmt(x)}" for x, y in pts))

    def _perks_held(self, perks, cond):
        """False when a HasPerk condition rules this row out for these perks."""
        for pk, op, v in _HASPERK.findall(cond or ""):
            has = pk in perks
            want = float(v) == 1.0
            if (op == "!=" and has == want) or (op != "!=" and has != want):
                return False
        return True

    def _spell(self, sf, perks, nums, files, keyword=None):
        for r in self.spel.get((sf or "").upper(), []) if keyword is None else self.spel_by_kw.get(keyword, []):
            if not self._perks_held(perks, r.get("Conditions")):
                continue
            mag, dur = _f(r.get("EFIT_Magnitude")), _f(r.get("EFIT_Duration"))
            name = r.get("EFID_MGEF_FULL") or U._human(r.get("EFID_MGEF_EDID") or "")
            _add(nums, mag)
            _add(nums, dur)
            for a, b in _COND_VAL.findall(r.get("Conditions") or ""):
                _add(nums, _f(a or b))
            m = _EDID_NUM.search(r.get("EFID_MGEF_EDID") or "")
            if m:
                _add(nums, _f(m.group(1)))
            if r.get("CVTO_CURV_EDID"):
                self._curve(r["CVTO_CURV_EDID"], nums, files, name)
            elif r.get("MAGA_AVIF_EDID") and not mag:
                nums.add(1.0)   # magnitude = the actor value: 1 per stack / kill
                files.append(f"{name}: 1 per {U._human(r['MAGA_AVIF_EDID'])}")
            if mag:
                files.append(f"{name} {_sign(mag)}" + (f" for {U._fmt(dur)} seconds" if dur else ""))

    def _perk(self, pk, perks, nums, files, claims, seen):
        if pk in seen:
            return
        seen.add(pk)
        d = self.perk_desc.get(pk, "")
        if d:
            claims.append(d)
        for r in self.perkep.get(pk, []):
            v = _f(r.get("Value"))
            if v is not None:
                _add(nums, v)
                ep = (r.get("EntryPoint") or "").replace("Mod ", "").lower()
                fn = r.get("Function") or ""
                if fn == "Add Value":
                    files.append(f"{ep} {_sign(v, abs(v) < 1)}")
                elif fn == "Multiply Value":
                    files.append(f"{ep} x{U._fmt(v)}" + (f" ({_sign(v - 1, True)})" if v else ""))
                elif "Actor Value Mult" in fn:
                    files.append(f"{ep} {_sign(v, abs(v) < 1)} per stack")
                else:
                    files.append(f"{ep} {fn} {U._fmt(v)}".strip())
            for a, b in _COND_VAL.findall(r.get("Conditions") or ""):
                _add(nums, _f(a or b))
            raw = r.get("Raw") or ""
            m = _CURVE_IN_RAW.search(raw)
            if m:
                self._curve(m.group(1), nums, files, (r.get("EntryPoint") or "").replace("Mod ", ""))
            m = _SPELL_IN_RAW.search(raw)
            if m:
                self._spell(m.group(1), perks, nums, files)
            if r.get("Ability_FormID"):
                af = r["Ability_FormID"].strip().upper()
                for sr in self.spel.get(af, []):
                    if self._perks_held(perks, sr.get("Conditions")):
                        dd = self.idx.mgef.get((sr.get("EFID_MGEF_FormID") or "").upper(), ("", "", ""))[2]
                        if dd and not _TOKEN.search(dd):
                            claims.append(dd)
                self._spell(af, perks, nums, files)

    def collect(self, omod_fid):
        """(claims, real numbers, file summary) for one unique mod."""
        idx, fid = self.idx, (omod_fid or "").upper()
        claims, nums, files = [], set(), []
        o = idx.omod.get(fid)
        if o and o[2]:
            claims.append(o[2])
        props = idx.props.get(fid, [])
        perks, kws = set(), []
        for r in props:
            prop, v1 = r[5], r[6]
            if prop in ("116", "Perk"):
                # the EditorID as the property prints it ("DemolitionExpert02 ...")
                m = re.match(r"\s*(\w+)", v1)
                pk = self.perk_by_fid.get(U._fid(v1, "PERK")) or (m.group(1) if m else "")
                if pk:
                    perks.add(pk)
            elif prop == "Keywords" and r[4] == "ADD":
                m = re.match(r"\s*(\S+)", v1)
                if m:
                    kws.append(m.group(1))
        for r in props:
            fn, prop, v1, v2 = r[4], r[5], r[6], r[7]
            if prop in ("Keywords", "MaterialSwaps", "ModelSwap", "Value", "116", "Perk"):
                continue
            if prop == "Enchantments":
                ef = U._fid(v1, "ENCH")
                rec = idx.ench.get(ef)
                for ei, (mfid, eid, mag, dur, curve, maga) in enumerate(rec[2] if rec else []):
                    _add(nums, mag)
                    _add(nums, dur)
                    maga = maga or self.maga.get((ef, ei), "")
                    if curve:
                        self._curve(curve, nums, files, U._human(eid))
                    elif maga and not mag:
                        nums.add(1.0)
                        files.append(f"{U._human(eid)}: 1 per {U._human(maga)}")
                    elif mag:
                        files.append(f"{U._human(eid)} {U._fmt(mag)}" + (f" for {U._fmt(dur)} seconds" if dur else ""))
                    d = idx.mgef.get(mfid, ("", "", ""))[2]
                    if d and not _TOKEN.search(d):
                        claims.append(d)
                    pk = idx.mgef_perk.get(mfid)
                    if pk:
                        perks.add(pk)
                for fx in self.enchfx.get(ef, []):
                    g = self.glob.get(fx.get("MAGG_EDID") or "")
                    if g is not None:
                        _add(nums, g)
                        files.append(f"{U._human(fx.get('MGEF_EDID') or '')} {U._fmt(g)} (global {fx.get('MAGG_EDID')})")
                continue
            for v in (v1, v2):
                x = _f(v)
                if x is not None and x != 0:
                    # a float multiplier prints as a % (VATS crit 2.0 = 200%)
                    _add(nums, x, frac=(fn == "MUL+ADD" or (prop in ("ActorValues", "Actor Values")
                                                           and "." in str(v) and abs(x) <= 10)))
            x = _f(v1)
            if x:
                files.append(f"{_camel(prop).lower()} " + (_sign(x, True) if fn == "MUL+ADD"
                                                            else ("set to " + U._fmt(x) if fn == "SET" else _sign(x))))
            if "NULL" not in (r[8] or "NULL"):
                self._curve(r[8], nums, files, prop)
        seen = set()
        for pk in sorted(perks):
            self._perk(pk, perks, nums, files, claims, seen)
        for kw in kws:
            if kw.startswith(("dn_Has", "s_", "ma_", "Anims", "WeaponType", "ObjectType")):
                continue
            self._spell(None, perks, nums, files, keyword=kw)
        return claims, nums, list(dict.fromkeys(files))

    def check(self, omod_fid):
        claims, nums, files = self.collect(omod_fid)
        claimed, nf = [], []
        for c in claims:
            text = U._clean_desc(c)
            for m in _NUM.finditer(text):
                v = float(m.group(1))
                tail = text[m.end():m.end() + 12].strip()
                word = re.match(r"(%|ft|seconds?|s\b|RAD|x\b|[A-Za-z]+)?", tail).group(0)
                label = (m.group(1) + ("" if word == "%" else " ") + word).strip()
                if not word:   # "(Max 10)": say what the number is by the word before it
                    before = re.findall(r"([A-Za-z]+)\W*$", text[:m.start()])
                    label = ((before[0] + " ") if before else "") + m.group(1)
                claimed.append(label)
                if not any(abs(v - n) < 0.011 for n in nums):
                    nf.append(label)
        claimed, nf = list(dict.fromkeys(claimed)), list(dict.fromkeys(nf))
        if not claimed:
            status = "none"          # nothing with a number to check
        elif not nums:
            status = "unconfirmed"
        elif not nf:
            status = "confirmed"
        elif len(nf) == len(claimed):
            status = "mismatch"
        else:
            status = "partial"
        return {"status": status, "claimed": claimed, "notFound": nf, "files": files[:12]}


def apply(items, pick, channel, log=print, idx=None):
    """Check each unique mod; add the page note; print the build-log lists."""
    ck = Checker(idx or U.EffectIndex(pick, channel), pick, channel)
    out = {"confirmed": [], "partial": [], "mismatch": [], "unconfirmed": []}
    for it in items:
        notes = []
        for m in [m for m in (it.get("uniqueMods") or []) if m.get("kind") == "unique"]:
            res = ck.check(m.get("formId"))
            m["effectCheck"] = res
            st = res["status"]
            if st == "none":
                continue
            out[st].append((it.get("name"), res))
            if st in STATUS_NOTE:
                note = STATUS_NOTE[st].format(nf=", ".join(res["notFound"]),
                                              files="; ".join(res["files"][:4]) or "no numbers")
                m["desc"] = (m.get("desc") or "").rstrip(" .") + f" ({note})"
                notes.append(note)
        if notes and it.get("inherentEffect") and not it.get("namedOnly"):
            eff = it["inherentEffect"].rstrip(" .")
            for n in notes:
                if f"({n})" not in eff:
                    eff += f" ({n})"
            it["inherentEffect"] = eff
    log(f"  effect check vs game files: " + ", ".join(f"{k} {len(v)}" for k, v in out.items()))
    for st, head in (("mismatch", "DESCRIPTION DOESN'T MATCH THE GAME FILES (bug report?)"),
                     ("partial", "SOME NUMBERS NOT IN THE GAME FILES (check / ask a dev)"),
                     ("unconfirmed", "NOTHING IN THE GAME FILES TO CHECK AGAINST")):
        if out[st]:
            log(f"    {head}:")
            for name, res in out[st]:
                log(f"      {name}: says {', '.join(res['claimed'])}; not found {', '.join(res['notFound']) or '-'}"
                    f" | files: {'; '.join(res['files'][:5]) or '-'}")
    return {k: len(v) for k, v in out.items()}
