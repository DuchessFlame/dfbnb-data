#!/usr/bin/env python3
"""
buffs_effects.py — the effect normalisation engine for the BnB Buffs pages.

WHY THIS IS A SEPARATE MODULE
─────────────────────────────
`build_buffs_json.py` has to answer one question over and over:

    "given a raw MGEF row, what player-facing buff is this, and which
     root expand does it belong under?"

Every buff page (alcohol / chems / food / nuka-cola / magazines /
bobbleheads / mutations / scout banners) asks it, and the healing
calculator + comparison calculators will want the same answer later.
Keeping it in one importable engine — the `rng76.py` pattern — means the
grouping can never drift between pages.

THE THREE THINGS THIS MODULE DOES
─────────────────────────────────
1. display_name(eff)  → the best player-facing name for one effect row.
   Priority: EDID override → cleaned FULL (unless it's an engine-generic
   string like "Duration" / "Fortify" / "Regen") → MGEF DNAM description
   → prettified EDID. This is what shows in "Output & Effects".

2. group_for(eff)     → (key, label) or None.
   None means the effect does NOT get a root expand. Per the page spec the
   survival plumbing (hunger, thirst, rads, disease chance, addiction
   chance) and the pure-engine rows (UI dummies, duration setters, script
   appliers) are excluded from grouping — they still DISPLAY, they just
   don't create a group nobody would browse by.

3. sort_groups(keys)  → SPECIAL first, spelled S-P-E-C-I-A-L, then every
   other group A–Z. This is the page's root-expand order, and it is the
   whole reason the module exists.

POLARITY
────────
A buff and a debuff of the same stat share a group — Nukashine belongs in
BOTH "Strength" (+3) and "Intelligence" (−3). The row carries
`polarity: "debuff"` so the renderer can flag it; it does not get its own
"Reduce Intelligence" group, which would split the stat in two and defeat
the point of browsing by stat.

Usage:
    from buffs_effects import Effect, display_name, group_for, sort_groups
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

__all__ = [
    "Effect",
    "SPECIAL_ORDER",
    "SPECIAL_LABELS",
    "display_name",
    "group_for",
    "groups_for",
    "sort_groups",
    "group_label",
    "is_hidden",
    "is_generic_full",
    "polarity_of",
    "format_duration",
    "format_magnitude",
    "substitute_mag",
]


# ---------------------------------------------------------------------------
# The effect record
# ---------------------------------------------------------------------------

class Effect(dict):
    """A single MGEF row as the builders assemble it.

    Recognised keys (all optional except one of full/edid):
        full        MGEF_FULL           e.g. "Alcohol: Fortify Strength"
        edid        MGEF_EDID           e.g. "Nukashine_FortifyStrength"
        dnam        MGEF DNAM desc      e.g. "Rads <mag> /s"
        magnitude   float               EFIT_Magnitude
        duration    float (seconds)     EFIT_Duration
        mag_glob    GLOB EDID           MAGG_GLOB_EDID (magnitude fallback)
        dur_glob    GLOB EDID           DURG_GLOB_EDID (duration fallback)
        form_id     MGEF FormID
    """

    def __getattr__(self, k):           # eff.full as well as eff["full"]
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k)


def _s(eff: Any, key: str) -> str:
    if isinstance(eff, dict):
        return str(eff.get(key) or "").strip()
    return str(getattr(eff, key, "") or "").strip()


def _f(eff: Any, key: str) -> Optional[float]:
    v = eff.get(key) if isinstance(eff, dict) else getattr(eff, key, None)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 1 — SPECIAL
# ---------------------------------------------------------------------------

SPECIAL_ORDER: List[str] = [
    "strength", "perception", "endurance", "charisma",
    "intelligence", "agility", "luck",
]

SPECIAL_LABELS: Dict[str, str] = {
    "strength":     "Strength",
    "perception":   "Perception",
    "endurance":    "Endurance",
    "charisma":     "Charisma",
    "intelligence": "Intelligence",
    "agility":      "Agility",
    "luck":         "Luck",
}

# Word-boundary match so "Fortify Strength" hits but "Strengthen" does not,
# and so "Fortify Charisma Food" / "XCell: Fortify Charisma" both land.
_SPECIAL_RE = {
    k: re.compile(rf"\b{lbl}\b", re.I) for k, lbl in SPECIAL_LABELS.items()
}


# ---------------------------------------------------------------------------
# 2 — Effects that never create a group
# ---------------------------------------------------------------------------
# These still render inside "Output & Effects" (they are real, visible
# effects). They just don't earn a root expand, because a group containing
# ~every item on the page is not a filter — it's noise.

_NON_GROUPING = [
    # Survival meters
    re.compile(r"^SURV_(Food|Drink)_Effect$", re.I),
    re.compile(r"^GHL_SURV_Chem_Effect$", re.I),
    re.compile(r"\bSatisf(y|ies) Hunger\b", re.I),
    re.compile(r"\bQuench(es)? Thirst\b", re.I),
    re.compile(r"^Chems?: Thirst$", re.I),
    re.compile(r"^SURV_AddHunger", re.I),
    re.compile(r"^SURV_AddThirst", re.I),
    re.compile(r"\bIncrease (Hunger|Thirst)\b", re.I),
    # Radiation TAKEN from eating/drinking (not rad resistance — that groups)
    re.compile(r"^Eating: Radiation Damage$", re.I),
    re.compile(r"^Radiation Exposure$", re.I),
    re.compile(r"^Rads$", re.I),
    # Disease chance (catching one) — Disease RESISTANCE does group
    re.compile(r"\bDisease Chance\b", re.I),
    re.compile(r"^SURV_DiseaseVector", re.I),
    re.compile(r"^SURV_ReduceDiseaseResistance", re.I),
    # Addiction chance — "Cure Addiction" DOES group (it's why you take Addictol)
    re.compile(r"^Chance Addiction\b", re.I),
    re.compile(r"\bAddictionChance\b", re.I),
]

# Pure engine plumbing — hidden from the page entirely.
_HIDDEN = [
    re.compile(r"^UI Text Dummy", re.I),
    re.compile(r"_UIDummy", re.I),
    re.compile(r"^Apply effect script$", re.I),
    re.compile(r"^Suppress negative effects$", re.I),
    re.compile(r"Apply Client Effect$", re.I),
    re.compile(r"^_?Duration$", re.I),          # *_Duration rows just set a timer
    re.compile(r"_Duration$", re.I),
    re.compile(r"^Mutation Count$", re.I),
    re.compile(r"^abMutationCount$", re.I),
    re.compile(r"Transition ?Effect$", re.I),
    re.compile(r"^Stealth Boy Transition$", re.I),
    re.compile(r"^Battle Banner Proc", re.I),
    re.compile(r"ProcVFX", re.I),
    re.compile(r"^Apply Battle Banner Perks$", re.I),
    re.compile(r"^Highlight Enemies: ", re.I),
]

# FULL strings the engine reuses across unrelated effects. When we see one
# of these the FULL tells us nothing, so display_name() falls through to the
# DNAM description or the EDID.
_GENERIC_FULLS = {
    "duration", "fortify", "reduce", "increase", "decrease", "regen",
    "restore", "effect", "mutation", "add onhit perk",
    "", "-", "none", "ui text dummy",
}


def is_generic_full(full: str) -> bool:
    """True when a FULL string tells us nothing about what the effect does.

    Callers with extra context add their own: the mutation builder treats an
    effect FULL that merely repeats the mutation's own name ("Twisted
    Muscles") as generic too, because it names the source, not the effect.
    """
    return (full or "").strip().lower() in _GENERIC_FULLS


# ---------------------------------------------------------------------------
# 3 — Name cleanup
# ---------------------------------------------------------------------------
# Ported from build_farming_guides_json._EFFECT_RENAME and extended for the
# chem / magazine / bobblehead prefixes the farming pages never see.

_RENAME_EXACT: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"^Eating:\s*Radiation Damage$", re.I),             "Rads"),
    (re.compile(r"^Radiation Exposure$", re.I),                     "Rads"),
    (re.compile(r"^SURV_Food_Effect$", re.I),                       "Satisfy Hunger"),
    (re.compile(r"^SURV_Drink_Effect$", re.I),                      "Quench Thirst"),
    (re.compile(r"^GHL_SURV_Chem_Effect$", re.I),                   "Quench Thirst"),
    (re.compile(r"^Chems?:\s*Thirst$", re.I),                       "Increase Thirst"),
    (re.compile(r"^SURV_DiseaseVector_\w+_Effect$", re.I),          "Disease Chance"),
    (re.compile(r"^SURV_AddHunger_Potion_Effect$", re.I),           "Increase Hunger"),
    (re.compile(r"^SURV_AddThirst_Potion_Effect_NotChem$", re.I),   "Increase Thirst"),
    (re.compile(r"^SURV_DiseaseCure.*Effect.*$", re.I),             "Cure Disease"),
    (re.compile(r"^SURV_IncreaseDiseaseResistance.*Effect$", re.I), "Disease Resistance"),
    (re.compile(r"^SURV_ReduceDiseaseResistance.*Effect$", re.I),   "Reduce Disease Resistance"),
    (re.compile(r"^Mutation:\s*", re.I),                            ""),   # handled as strip below
]

# Prefixes the engine bolts onto an otherwise clean effect name. Stripping
# them is what turns eleven separate "X: Fortify Strength" rows into one
# Strength group.
_STRIP_PREFIX = re.compile(
    r"^(Alcohol|Alchohol|Chem|Chems|Psycho|Food|MedX|Med-X|XCell|X-Cell|Calmex|"
    r"DaddyO|Daddy-O|Daytripper|Fury|Overdrive|Buffout|Jet|Mentats|Stimpak|"
    r"Magazine|Bobblehead|Mutation|Serum|Banner)\s*[:\-]\s*", re.I)

# "Happy-Go-Lucky - Fortify Luck", "Live & Love - Fortify Luck"
_STRIP_TITLE_PREFIX = re.compile(r"^[A-Z][\w'&.\- ]{2,28}\s+-\s+(?=Fortify|Reduce|Restore|Increase)")

# "Hidden" marks an effect the HUD doesn't draw — it is not part of the name.
_STRIP_SUFFIX = re.compile(r"\s+(Food|Drink|Eating|Effect|Chem|Potion|Hidden)$", re.I)
_STRIP_TRAILING_DIGITS = re.compile(r"0?\d$")


def _clean(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    for rx, repl in _RENAME_EXACT:
        if repl:
            if rx.match(s):
                return repl
        else:
            s = rx.sub("", s).strip()
    s = _STRIP_TITLE_PREFIX.sub("", s)
    s = _STRIP_PREFIX.sub("", s)
    prev = None
    while prev != s:                       # "Fortify Luck Food Effect" → "Fortify Luck"
        prev = s
        s = _STRIP_SUFFIX.sub("", s).strip()
    return s.strip()


def _prettify_edid(edid: str) -> str:
    """`Nukashine_FortifyStrength` → `Fortify Strength`."""
    s = re.sub(r"^(zzz_|ZZZ_|SCORE_|Mutation_|DLC\d+_|E\d+[A-Z]?_|D\d+[A-Z]?_|W\d+_|MoM_)",
               "", edid or "")
    s = s.replace("_", " ")
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)      # camelCase → words
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\b(ME|EF|Effect|Spell|Perk)\b$", "", s).strip()
    return s


def is_hidden(eff: Any, keep_ui_dummy: bool = False) -> bool:
    """True when the row is engine plumbing that should not render at all.

    `keep_ui_dummy=True` is the MUTATIONS page. A mutation's SPECIAL swing
    lives ONLY on its `Mutation_<name>_UIDummy` /
    `Mutation_<name>_UIDummyWithSerum` rows — that pair is how the game shows
    "+2 Strength, +2.5 with a serum". Hiding them everywhere else is right
    (they are duplicate scaffolding on consumables); hiding them on the
    mutations page would delete the only numbers it has.
    """
    full = _s(eff, "full")
    edid = _s(eff, "edid")
    for rx in _HIDDEN:
        if keep_ui_dummy and rx.pattern in (r"^UI Text Dummy", r"_UIDummy"):
            continue
        if rx.search(full) or rx.search(edid):
            return True
    return False


def display_name(eff: Any) -> str:
    """Best player-facing name for one effect row."""
    full = _s(eff, "full")
    edid = _s(eff, "edid")
    dnam = _s(eff, "dnam")

    if full and full.strip().lower() not in _GENERIC_FULLS:
        cleaned = _clean(full)
        if cleaned and cleaned.strip().lower() not in _GENERIC_FULLS:
            return cleaned

    # FULL was generic or empty — the DNAM description is the real label,
    # but only once its substitution tokens are gone. "<+MAG> <ITEM1.ABBR>"
    # is 100% markup and reduces to nothing, so the EDID has to answer.
    if dnam:
        d = _ANY_TOKEN.sub(" ", dnam).strip(" .")
        d = re.sub(r"\s+", " ", d)
        if d:
            return d

    pretty = _clean(_prettify_edid(edid))
    return pretty or (full or edid or "Unknown Effect")


# ---------------------------------------------------------------------------
# 4 — Group resolution
# ---------------------------------------------------------------------------
# Ordered: the FIRST pattern that matches wins, so put the specific ones
# above the general ones (Energy Resistance before Energy Damage, etc.).

_GROUP_RULES: List[Tuple[re.Pattern, str]] = [
    # --- Resistances (before the damage rules, which share keywords) ---
    (re.compile(r"\bDisease Resist", re.I),                       "Disease Resistance"),
    (re.compile(r"\b(Rad|Radiation) Resist", re.I),               "Radiation Resistance"),
    (re.compile(r"\bResist Radiation", re.I),                     "Radiation Resistance"),
    (re.compile(r"\bEnergy Resist", re.I),                        "Energy Resistance"),
    (re.compile(r"\bCryo Resist", re.I),                          "Cryo Resistance"),
    (re.compile(r"\bFire Resist", re.I),                          "Fire Resistance"),
    (re.compile(r"\bPoison Resist", re.I),                        "Poison Resistance"),
    (re.compile(r"\bDamage Resist", re.I),                        "Damage Resistance"),

    # --- Cures / removal ---
    (re.compile(r"\bCure Addiction\b|\bCure All Addictions\b", re.I), "Cure Addiction"),
    (re.compile(r"\bCure Disease\b", re.I),                       "Cure Disease"),
    (re.compile(r"\bRemove Radiation\b|\bReduce Rads\b", re.I),    "Radiation Removal"),

    # --- Action points ---
    # No leading \b on the AP_?Regen / Sprint ?AP forms: the EDID spells them
    # `Alcohol_APRegen` / `LeadChampagne_IncreaseSprintAP`, and `_` is a word
    # character, so \b never fires after it.
    (re.compile(r"AP_? ?Regen\b|\bAction Point Regen\b", re.I),   "Action Point Regen"),
    (re.compile(r"Sprint_? ?AP\b", re.I),                         "Sprint AP Cost"),
    (re.compile(r"\bAction Points?\b|\bFortify AP\b", re.I),      "Action Points"),

    # --- Health ---
    (re.compile(r"\bHeal ?(ing)? Rate\b", re.I),                  "Healing Rate"),
    (re.compile(r"\bHealth Regen\b", re.I),                       "Health Regen"),
    (re.compile(r"\bRestore Health\b|\bFortify Health\b|\bHeal Player Team\b|\bFirst Aid\b", re.I),
                                                                   "Health"),

    # --- Damage output ---
    (re.compile(r"\bSneak Attack\b", re.I),                       "Sneak Attack Damage"),
    (re.compile(r"\bCrit(ical)? D(a?m)g?", re.I),                 "Critical Damage"),
    (re.compile(r"\b(Fists|Unarmed) Damage\b", re.I),             "Unarmed Damage"),
    (re.compile(r"\bMelee Damage\b", re.I),                       "Melee Damage"),
    (re.compile(r"\bBallistic Damage\b", re.I),                   "Ballistic Damage"),
    (re.compile(r"\bEnergy Damage\b", re.I),                      "Energy Damage"),
    (re.compile(r"\bExtra Damage\b|\bFortify All Damage\b", re.I), "All Damage"),
    (re.compile(r"\bDamage Vs\.? Animals\b", re.I),               "Damage vs Animals"),

    # --- Utility ---
    (re.compile(r"\bCarry Weight\b", re.I),                       "Carry Weight"),
    (re.compile(r"\bLimb Damage\b", re.I),                        "Limb Damage Reduction"),
    (re.compile(r"\bWater Breathing\b", re.I),                    "Water Breathing"),
    (re.compile(r"\bStealth Field\b|\bChameleon\b|\bActive Camouflage\b", re.I), "Stealth"),
    (re.compile(r"\bDetect Life\b|\bHighlight (Living )?(Enemies|Targets)\b", re.I), "Detect Life"),
    (re.compile(r"\b(VATS|Gun) Accuracy\b|\bScope Stability\b", re.I), "Accuracy"),
    (re.compile(r"\bXP\b|\bExperience Bonus\b", re.I),            "XP Bonus"),
    (re.compile(r"\bWeapon Condition\b|\bRepair (Equipped )?Weapon\b", re.I), "Weapon Condition"),
    (re.compile(r"\bArmou?r (CND|Condition)\b|\bRepair .*Armou?r\b", re.I),   "Armour Condition"),
    (re.compile(r"\bCondition Cost\b", re.I),                     "Weapon Condition Cost"),
    (re.compile(r"\bBarter\b", re.I),                             "Barter"),
    (re.compile(r"\bWell Rested\b|\bRested\b", re.I),             "Well Rested"),
    (re.compile(r"\bWell Tuned\b", re.I),                         "Well Tuned"),
    (re.compile(r"Fall ?(Speed|Damage)\b", re.I),                 "Fall Speed & Damage"),
    (re.compile(r"\bBobber\b|\bTime To Hook\b", re.I),            "Fishing"),
    (re.compile(r"\bInsect Repellent\b|\bdamage from insects\b", re.I), "Insect Repellent"),
    (re.compile(r"\bScorched Repellent\b|\bdamage from Scorched\b", re.I), "Scorched Repellent"),
    (re.compile(r"\bAuto-?revive\b|\bSecond Wind\b", re.I),       "Auto-Revive"),
    (re.compile(r"Slow ?Hunger|NoHungerNoThirst|"
                r"Hunger (&|and) thirst do not increase", re.I),  "Slow Hunger & Thirst"),

    # LAST: the generic on-hit perk appliers. Deliberately below Health Regen /
    # Melee Damage so a specific on-hit effect keeps its own stat group and only
    # the untyped "Add OnHit Perk" rows collect here.
    (re.compile(r"AddPerk\b|\bOn ?Hit\b", re.I),                  "On-Hit Effects"),
]


# Effects that are ALWAYS bad news regardless of sign — the game stores the
# rads you take and the disease you might catch as positive magnitudes.
_ALWAYS_DEBUFF = re.compile(
    r"^(Rads?|Disease Chance|Increase (Hunger|Thirst)|Chance Addiction\b|"
    r"Reduce Disease Resistance|Weapon Condition Cost)", re.I)


def polarity_of(eff: Any) -> str:
    """"buff" | "debuff". A negative magnitude, a Reduce/Lower verb, or an
    effect that is inherently a cost (rads, disease, addiction, hunger)."""
    if _ALWAYS_DEBUFF.match(display_name(eff)):
        return "debuff"
    text = f"{_s(eff, 'full')} {_s(eff, 'edid')} {_s(eff, 'dnam')}"
    # Mutations state their downside as a plain sentence rather than a verb:
    # "No benefits from Veggies", "-2 SPECIAL solo".
    if re.search(r"\bNo benefits\b|\bno longer\b|\bcannot\b|\bunable\b|"
                 r"^\s*-\d|\bNo Veggies\b", text, re.I):
        return "debuff"
    if re.search(r"\b(Reduce[sd]?|Lower[s]?|Decrease[sd]?|Penalt(y|ies))\b", text, re.I):
        # "Reduce Sprint AP Cost" and "Reduces Limb Damage" are GOOD things.
        if not re.search(r"\bReduce[sd]?\s+(Sprint AP|Limb Damage|Hunger|Thirst|Rads?|Time)", text, re.I):
            return "debuff"
    mag = _f(eff, "magnitude")
    if mag is not None and mag < 0:
        return "debuff"
    return "buff"


def group_for(eff: Any) -> Optional[Tuple[str, str]]:
    """Return (key, label) for the root expand this effect belongs under,
    or None when the effect does not create a group."""
    if is_hidden(eff):
        return None

    full = _s(eff, "full")
    edid = _s(eff, "edid")
    for rx in _NON_GROUPING:
        if rx.search(full) or rx.search(edid):
            return None

    haystack = f"{full} {edid} {_s(eff, 'dnam')}"

    # SPECIAL wins over everything — "XCell: Fortify Strength" is Strength,
    # not "All Damage", even though X-Cell also boosts damage elsewhere.
    for key in SPECIAL_ORDER:
        if _SPECIAL_RE[key].search(haystack):
            return (key, SPECIAL_LABELS[key])

    for rx, label in _GROUP_RULES:
        if rx.search(haystack):
            return (_key(label), label)

    # Fall through: a genuinely unique named buff (Liquid Courage, Perfect
    # Bubblegum, Mystery Treat...). Its own group, sorted into the A–Z tail.
    name = display_name(eff)
    if not name or name.lower() in _GENERIC_FULLS:
        return None
    name = _title(name)
    key = _key(name)
    # Register it so group_label()/sort_groups() can recover the real label
    # instead of un-slugifying the key (which loses "&", "%", "-" and case).
    _LABELS_BY_KEY.setdefault(key, name)
    return (key, name)


def groups_for(effects: Sequence[Any]) -> List[Tuple[str, str]]:
    """De-duplicated, page-ordered group list for one item's effects."""
    seen: Dict[str, str] = {}
    for e in effects:
        g = group_for(e)
        if g and g[0] not in seen:
            seen[g[0]] = g[1]
    return [(k, seen[k]) for k in sort_groups(list(seen))]


def _key(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (label or "").lower()).strip("-")


def _title(s: str) -> str:
    small = {"a", "an", "and", "at", "by", "for", "from", "in", "of", "on", "or",
             "the", "to", "vs", "with"}
    words = re.split(r"(\s+)", (s or "").strip())
    out = []
    for i, w in enumerate(words):
        if not w.strip():
            out.append(w)
            continue
        lw = w.lower()
        if i > 0 and lw in small:
            out.append(lw)
        elif w.isupper() and len(w) <= 4:
            out.append(w)                       # AP, XP, VATS, HP
        else:
            out.append(w[:1].upper() + w[1:])
    return "".join(out)


_LABELS_BY_KEY: Dict[str, str] = {}
for _k, _l in SPECIAL_LABELS.items():
    _LABELS_BY_KEY[_k] = _l
for _rx, _l in _GROUP_RULES:
    _LABELS_BY_KEY.setdefault(_key(_l), _l)


def group_label(key: str) -> str:
    return _LABELS_BY_KEY.get(key, _title((key or "").replace("-", " ")))


def sort_groups(keys: Sequence[str]) -> List[str]:
    """SPECIAL spelled S-P-E-C-I-A-L first, everything else A–Z after.

    This is THE page order rule. Nothing else in the pipeline is allowed to
    re-sort a group list.
    """
    specials = [k for k in SPECIAL_ORDER if k in set(keys)]
    rest = sorted((k for k in keys if k not in SPECIAL_LABELS),
                  key=lambda k: group_label(k).lower())
    return specials + rest


# ---------------------------------------------------------------------------
# 5 — Value formatting
# ---------------------------------------------------------------------------

def format_duration(seconds: Optional[float]) -> Optional[str]:
    """25 → "25s"; 1800 → "30 min"; 7200 → "2 hours"."""
    if seconds is None or seconds <= 0:
        return None
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    hours, mins = s // 3600, (s % 3600) // 60
    parts = []
    if hours == 1:
        parts.append("1 hour")
    elif hours > 1:
        parts.append(f"{hours} hours")
    if mins:
        parts.append(f"{mins} min")
    return " ".join(parts) if parts else f"{s}s"


def format_magnitude(v: Optional[float]) -> Optional[str]:
    """1.8 → "1.8"; 45.0 → "45"; None → None."""
    if v is None:
        return None
    return f"{round(float(v), 2):g}"


_MAG_TOKEN = re.compile(r"<\s*([+-]?)\s*(mag|nag|magnitude|dur|duration)\s*>", re.I)

# Any game-text substitution token: <mag>, <+MAG>, <ITEM1.ABBR>, <ITEM1.NAME>.
# The dot is why a \w+ pattern misses the ITEM ones.
_ANY_TOKEN = re.compile(r"<[^>]*>")


def substitute_mag(dnam: str, magnitude: Optional[float],
                   duration: Optional[float] = None) -> str:
    """Fill a DNAM template: "Rads <mag> /s" + 3 → "Rads 3 /s".

    The exports contain a typo'd `<+NAG>` on High Voltage Hefe — treat it as
    `<mag>` rather than leaving raw markup on the page.
    """
    if not dnam:
        return ""

    def repl(m: re.Match) -> str:
        sign, tok = m.group(1), m.group(2).lower()
        val = duration if tok.startswith("dur") else magnitude
        if val is None:
            return ""
        txt = format_magnitude(val) or ""
        if sign == "+" and val > 0 and not txt.startswith("+"):
            txt = "+" + txt
        return txt

    out = _MAG_TOKEN.sub(repl, dnam)
    return re.sub(r"\s{2,}", " ", out).strip()
