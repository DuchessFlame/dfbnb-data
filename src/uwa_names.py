"""The name a unique shows in game, read from its naming rules (INNR).

A unique's custom mod adds a keyword (CustomItemName_V63GatlingLaser). The
weapon's naming rules (INNR dn_CommonGun, dn_CommonMelee, dn_CommonArmor…)
turn that keyword into the text the player sees:

    CustomItemName_V63GatlingLaser      -> "V63-HELGA"      (mod FULL said "Vault 63 Gatling Laser")
    RD01_CustomItemName_StrikeBreaker   -> "Flatliner"      (EditorID said Strike Breaker)
    CustomItemName_GunthersRevolver     -> "Gunther's Big Iron"

So the INNR text is the name, not the mod FULL and not the EditorID. Rules
are read from tsv/INNR_Export_<Month>_<Year>.tsv, written straight from the
same pass as the WEAP export ("!!!Wordpress - ExportWEAPToTSV.pas" writes it
as a fourth file), and from the WEAP Base column INRD_NamingRules (which rules
each weapon uses — the Chainsaw names its unique "Cauterizer" through
dn_CommonGun, the melee rules say "The Cauterizer").

Only a rule whose keyword list is exactly that one CustomItemName keyword is
a name. A rule that pairs it with a part keyword (RD01_CustomItemName_
BoilingPoint + dn_HasBarrel_Long -> "Extended") is a part word, not the name.
First rule wins, as in game (Anchorage Ace before Sneaky Time).
"""
import csv
import re
import sys
from collections import defaultdict

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

_KW = re.compile(r"(\S+) \[KYWD:([0-9A-Fa-f]{8})\]")
_NAMEKW = re.compile(r"CustomItemName", re.I)


def _rows(path, enc=None):
    """xEdit saves some exports as UTF-8 and some as ANSI; try UTF-8 first."""
    if not path:
        return []
    raw = open(path, "rb").read()
    if enc is None:
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("cp1252", errors="replace")
    else:
        text = raw.decode(enc, errors="replace")
    import io
    return list(csv.DictReader(io.StringIO(text, newline=""), delimiter="\t"))


def norm(s):
    s = re.sub(r"^\s*the\s+", "", (s or "").lower())
    s = s.replace("vault 63", "v63")
    return re.sub(r"[^a-z0-9]", "", s)


def kw_tail(edid):
    """CustomItemName_V63GatlingLaser -> V63GatlingLaser; RD01_..._Copy01 trimmed."""
    t = re.sub(r"^.*?CustomItemName_", "", edid or "", flags=re.I)
    return re.sub(r"_Copy\d+$", "", t, flags=re.I)


class Names:
    def __init__(self, pick):
        """pick(pattern, exclude=None) -> newest export path for this channel."""
        self.ok = False
        rules_path = pick("INNR_Export_*.tsv")
        # keyword edid -> [(innr fid, ruleset, rule, text)]
        self.by_kw = defaultdict(list)
        self.prefixes = set()      # "Cursed" (dn_HasCustomMod_Cursed): prefix + base name
        for r in _rows(rules_path):
            pk = _KW.findall(r.get("Keywords") or "")
            pt = re.sub(r"<ID=[0-9A-Fa-f]+>", "", r.get("Text") or "").strip()
            if len(pk) == 1 and pt and re.match(r"^dn_HasCustomMod_", pk[0][0], re.I):
                self.prefixes.add(pt)
            kws = _KW.findall(r.get("Keywords") or "")
            # PTS string tables carry debug tags: "<ID=6102ACE2>Abraxo Gun"
            text = re.sub(r"<ID=[0-9A-Fa-f]+>", "", r.get("Text") or "").strip()
            if len(kws) != 1 or not text or not _NAMEKW.search(kws[0][0]):
                continue
            try:
                key = (int(r.get("RulesetIndex") or 0), int(r.get("RuleIndex") or 0))
            except ValueError:
                key = (99, 99)
            self.by_kw[kws[0][0]].append((r.get("INNR_FormID", "").upper(), key, text))
        for v in self.by_kw.values():
            v.sort(key=lambda x: x[1])
        self.innr_of = {}
        self.full_of = {}
        for r in _rows(pick("WEAP_Export_*_Base.tsv")):
            fid = (r.get("WEAP_FormID") or "").strip().upper()
            m = re.search(r"\[INNR:([0-9A-Fa-f]{8})\]", r.get("INRD_NamingRules") or "")
            if m:
                self.innr_of[fid] = m.group(1).upper()
            self.full_of[fid] = (r.get("WEAP_FULL") or "").strip()
        for r in _rows(pick("ARMO_Export_*_ARMOUR.tsv")):
            self.full_of[(r.get("ARMO_FormID") or "").strip().upper()] = (r.get("ARMO_FULL") or "").strip()
        # mod fid -> CustomItemName keyword edids it adds
        self.mod_kws = defaultdict(set)
        self.kw_mods = defaultdict(set)
        props = pick("OMOD_Export_*_Properties.tsv")
        for r in _rows(props, "latin-1"):
            if (r.get("PropertyName") or "") != "Keywords":
                continue
            m = re.match(r"\s*(\S+)", r.get("Value1") or "")
            if m and _NAMEKW.search(m.group(1)):
                fid = (r.get("OMOD_FormID") or "").strip().upper()
                self.mod_kws[fid].add(m.group(1))
                self.kw_mods[m.group(1)].add(fid)
        # mod fid -> its own FULL (the custom mod's name)
        self.mod_full = {}
        for r in _rows(pick("OMOD_Export_*.tsv", exclude="_Properties"), "latin-1"):
            fid = (r.get("OMOD_FormID") or "").strip().upper()
            if fid in self.mod_kws:
                self.mod_full[fid] = (r.get("FULL") or "").strip()
        # every name keyword by its normalised tail and by its INNR text
        self.kw_by_key = defaultdict(set)
        for kw in set(self.kw_mods) | set(self.by_kw):
            self.kw_by_key[norm(kw_tail(kw))].add(kw)
            for _i, _k, text in self.by_kw.get(kw, ()):
                self.kw_by_key[norm(text)].add(kw)
        self.ok = bool(self.by_kw)

    def keywords_for(self, mod_fids=(), names=()):
        kws = set()
        for f in mod_fids:
            kws |= self.mod_kws.get((f or "").upper(), set())
        if not kws:
            for n in names:
                kws |= self.kw_by_key.get(norm(n), set())
        return {k for k in kws if not re.match(r"^(zzz|DEL_)", k, re.I)}

    def name_for(self, keywords, host_fid=""):
        """The in-game name, or "" when no rule names these keywords."""
        host_innr = self.innr_of.get((host_fid or "").upper(), "")
        best = None
        for kw in sorted(keywords):
            for innr, key, text in self.by_kw.get(kw, ()):
                rank = (0 if innr == host_innr else 1, key)
                if best is None or rank < best[0]:
                    best = (rank, text)
        return best[1] if best else ""

    def mod_name_over_placeholder(self, mod_fids, keywords, text):
        if not any(norm(text) in (norm(kw_tail(k)), norm(kw_tail(k).split("_")[-1]))
                   for k in keywords):
            return ""
        def ok(n):
            return (len(n.split()) >= 2 and not re.search(r"custom|paint|bounty|skin", n, re.I)
                    and norm(n) != norm(text) and not norm(n).startswith(norm(text)))
        for f in mod_fids:
            f = (f or "").upper()
            if not (self.mod_kws.get(f, set()) & set(keywords)):
                continue
            n = self.mod_full.get(f, "")
            if ok(n):
                return n
        # No mod on the item yet (a fresh build has only the name): the mods
        # that carry the keyword, when they all agree on one clean name.
        # Boomstick's keyword is also on Minty Breather's mod, but Boomstick's
        # own mod is a "Custom Mod"-style name, so nothing is renamed there.
        if not any(mod_fids):
            names_ = {self.mod_full.get(f, "") for k in keywords for f in self.kw_mods.get(k, ())}
            clean = {n for n in names_ if ok(n)}
            if len(clean) == 1 and all(ok(n) or re.search(r"bounty|custom|paint|skin", n, re.I) or not n
                                       for n in names_):
                return clean.pop()
        return ""


def apply(items, names, log=print):
    """Rename every item to its in-game name. The old name goes to `aka` so
    images, search and the plan pages still find it."""
    if not names.ok:
        log("  in-game names: no INNR export found — names left as they were")
        return []
    changed = []
    for it in items:
        mods = list(it.get("_cmod_fids") or []) + [
            m.get("formId") for m in it.get("uniqueMods") or [] if m.get("kind") == "unique"]
        known = [it.get("name")] + list(it.get("_altNames") or [])
        kws = names.keywords_for(mods, known)
        if len(kws) > 1:
            # A mod can carry another unique's keyword (Minty Breather's mod is
            # mod_Custom_Overkill_Copy01 and adds CustomItemName_Boomstick).
            # Keep the keyword that is this item's own; if none is, and the
            # rules disagree, rename nothing rather than guess.
            keys = {norm(n) for n in known if n}
            own = {k for k in kws if norm(kw_tail(k)) in keys
                   or any(norm(t) in keys for _i, _k, t in names.by_kw.get(k, ()))}
            if own:
                kws = own
            elif len({names.name_for({k}, it.get("formId")) for k in kws}) > 1:
                log(f"    ambiguous name keywords for {it.get('name')}: {sorted(kws)} — left as is")
                continue
        if kws:
            it["_nameKeywords"] = sorted(kws)
        new = names.name_for(kws, it.get("formId"))
        if not new:
            # No naming rule, but the item's own record spells it properly
            # (Survival_PipeWrench_MechanicsBestFriend: "Mechanic's Best Friend").
            # Only a spelling fix — same letters — never a different name.
            for rid in it.get("recordIds") or []:
                full = names.full_of.get((rid or "").upper(), "")
                if full and norm(full) == norm(it.get("name")) and full != it.get("name"):
                    new = full
                    break
        if not new:
            # Prefix rules (dn_HasCustomMod_Cursed -> "Cursed") put the word in
            # front of the base weapon's own name: "Cursed Pickaxe", not the
            # EditorID's "Cursed Pick Axe" (Duchess's screenshot, 13 Oct 2026).
            pre = next((p for p in names.prefixes
                        if (it.get("name") or "").lower().startswith(p.lower() + " ")), None)
            if pre and it.get("base"):
                new = f"{pre} {it['base']}"
        # A rule whose text is only its keyword spelled out ("Abraxo Gun" for
        # CustomItemName_Plasma_AbraxoGun) is a placeholder: in game the item
        # shows its custom mod's own name ("Meadow Breeze Sprayer", "Prototype
        # ABX03" - Duchess's screenshots, 13 Oct 2026). Only when that mod name
        # is a real multi-word name and not just the rule text spelled longer.
        if new:
            alt = names.mod_name_over_placeholder(mods, kws, new)
            if alt:
                new = alt
        if not new or new == it.get("name"):
            continue
        old = it["name"]
        alts = list(it.get("_altNames") or [])
        if old not in alts:
            alts.append(old)
        it["_altNames"] = alts
        aka = [a for a in it.get("aka") or [] if a != new]
        if old not in aka:
            aka.append(old)
        it["aka"] = aka
        it["name"] = new
        changed.append((old, new))
    log(f"  in-game names (INNR): {len(changed)} renamed")
    for old, new in changed:
        log(f"    {old} -> {new}")
    return changed
