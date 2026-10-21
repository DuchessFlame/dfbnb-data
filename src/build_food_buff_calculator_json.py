#!/usr/bin/env python3
"""
build_food_buff_calculator_json.py — data for the Recommended Food Buff Calculator
    /bnb/buffs/food/recommended-food-buff-calculator/
    renderer: df-bnb-food-buff-calculator.js (theme assets)

OUTPUT
  dist/calculators/food_buff_calculator.json
    rules      data/food_buff_calculator/rules.json, validated (the WHY)
    mechanics  mutation / Party Boy-Girl / Chem Fiend multipliers, read from
               the perk entry points (the HOW MUCH)
    items      every food, drink, chem and alcohol with a timed buff: each
               buff mapped to a calculator slot, its stack group, magnitude,
               duration, and the diet / perk multipliers that apply to it
    menu       Buffs n Brew name, prices and order limits per item

HOW CARNIVORE / HERBIVORE ACTUALLY WORK (all from the ESM, not hand-typed)
  The mutation SPELs hand out hidden perks (MGEF PerkToApply). Each perk is a
  "Mod Ingestible Magnitude" (entry point 29) multiply with two condition sets:
  one on the ITEM's keywords, one on the EFFECT's keywords.
    Carnivore  IngredientTypeMeat                   x2  (x2.5 "super")
               IngredientTypeVegetable              x0
    Herbivore  IngredientTypeVegetable/Herb/Fruit   x2  (x2.5 "super")
               IngredientTypeMeat                   x0
  and only on effects keyworded SURV_EffectTypeFoodBuff / FoodHunger /
  FoodHealing. "Super" = Strange in Numbers with mutated teammates
  (CNDF Mutation_Check_UseSuperVersion).
  Consequences the page relies on:
    * Carnivore only zeroes VEGETABLE — Herb and Fruit dishes (Company Tea,
      Sweet Mutfruit Tea...) still work at x1 for a Carnivore while a
      Herbivore gets them x2. Duchess calls these HYBRID dishes.
    * A dish with BOTH Meat and Vegetable keywords is zeroed by BOTH.
    * Food disease resistance etc. without the FoodBuff keyword is NOT
      doubled.
  Nothing above is hard-coded: change the perks in a patch and the next
  build follows. The rules are parsed from the condition text by EDID.

INPUTS
  tsv/ALCH_Export_*.tsv + _Effects.tsv     items, keywords, effects
  tsv/MGEF_Export_*.tsv                    ActorValue, PerkToApply
  tsv/GLOB_Export_*.tsv                    magnitude / duration globals
  tsv/SPEL_Export_*_EFFECTS.tsv            mutation -> MGEF, Chem Fiend
  tsv/MGEFKW_Export_*.tsv                  MGEF keywords + detrimental flag
  tsv/PERKEP_Export_*.tsv                  perk entry points + conditions
      (both written by the two new xEdit scripts OR by
       tools/esm_buff_rules_extract.py straight from SeventySix.esm)
  tsv/BnB_Menu_Items.tsv                   Buffs n Brew names / prices / limits
  data/food_buff_calculator/rules.json     HAND-KEPT recommendation rules

Usage:
  python src/build_food_buff_calculator_json.py
  python src/build_food_buff_calculator_json.py --data-dir tsv --outdir dist
"""
from __future__ import annotations

import argparse, csv, datetime, glob, json, math, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

import build_buffs_json as B                                  # noqa: E402  (effect resolution + categories)
try:
    import tsv_source                                         # noqa: E402
    _key = tsv_source.export_key
except Exception:                                             # pragma: no cover
    def _key(p):
        return (os.path.getmtime(p),)

RULES_PATH = os.path.join(REPO, "data", "food_buff_calculator", "rules.json")
PAGE_URL = "/bnb/buffs/food/recommended-food-buff-calculator/"

# ActorValue -> calculator slot. Effects on any other AV are shown in the
# item's effect list but never drive a recommendation.
AV_SLOT = {
    "Strength": "str", "Perception": "per", "Endurance": "end", "Charisma": "cha",
    "Intelligence": "int", "Agility": "agi", "Luck": "luck",
    "STAT_DmgAll": "dmg_all", "STAT_DmgMelee": "dmg_melee", "STAT_DmgMeleeUnarmed": "dmg_unarmed",
    "STAT_DmgBallistic": "dmg_ballistic", "STAT_DmgEnergy": "dmg_energy", "STAT_DmgVsAnimals": "dmg_animals",
    "STAT_CritDmgAll": "crit_dmg", "STAT_SneakAttackBonus": "sneak_attack",
    "STAT_VATSAccuracy": "vats_acc", "STAT_GunAccuracy": "gun_acc",
    "DamageResist": "dr", "EnergyResist": "er", "FireResist": "fire_res", "PoisonResist": "poison_res",
    "RadResistExposure": "rad_res", "STAT_ResistRadIngestion": "rad_res",
    "SURV_DiseaseResistance": "disease_res",
    "Health": "max_hp", "HealRate": "heal_rate",
    "ActionPoints": "ap", "ActionPointsRate": "ap_regen", "CarryWeight": "carry",
    "STAT_XPMult": "xp", "SpeedMult": "move_speed", "Invisibility": "invis",
    # added 20 Oct 2026 for bobbleheads / magazines (Kat)
    "STAT_DmgHeavyGuns": "dmg_heavy", "STAT_DmgExplosive": "dmg_explosive",
    "STAT_DmgMeleeWeapons": "dmg_melee_weapons", "STAT_DmgScoped": "dmg_scoped",
    "STAT_CritDmgAll": "crit_dmg", "STAT_CritDmgBallistic": "crit_ballistic",
    "STAT_CritDmgLaser": "crit_energy", "STAT_CritDmgPlasma": "crit_energy", "STAT_CritDmgEnergy": "crit_energy",
    "STAT_CritDmgHeavyGuns": "crit_heavy", "STAT_CritDmgMelee": "crit_melee",
    "STAT_Sneak": "sneak", "STAT_SneakLight": "sneak", "STAT_SneakSound": "sneak",
    "STAT_HealMultStimpak": "stimpak_heal",
}
# Item types beyond food / chem / alcohol (20 Oct 2026): one bobblehead and one
# magazine at a time (BobbleheadDispelEffects / MagazineDispelEffects), bubblegum.
BOBBLE_ITEM = re.compile(r"^(GHL_Glowing)?Bobble[Hh]ead_.+_Potion$")
MAGAZINE_ITEM = re.compile(r"^Magazine_.+_Potion$")
GUM_ITEM = re.compile(r"Bubblegum", re.I)
# Hunger & thirst slowers have no actor value; matched by effect EDID. Bubblegum and
# Perfect Bubblegum are treated as ONE effect (100% already covers 50%), so the
# Bubblegum section picks one of them plus the best Perk Bubblegum.
HUNGER_EDID = re.compile(r"SlowHungerThirst|NoHungerNoThirst", re.I)
BIG_GUM_EDID = {"BubblegumSlowHungerThirstEffect", "SCORE_Bubblegum_NoHungerNoThirst_Effect"}

# ── Magazines / bobbleheads: EVERY effect (Kat 20 Oct 2026: "ALL of it") ──────
# Actor-value effects map through AV_SLOT / MAGBOB_AV_SLOT. Perk-based effects
# (no actor value) map by the effect's own description text (MGEF DNAM, the
# words the game shows); the number is the item's EFIT magnitude. The perks
# behind them (PERKEP October: Mod Weapon DMG Bonus Mult, Mod Incoming Weapon
# Damage, ...) were checked against these numbers. Anything else becomes an
# "other" line with the game's own text so it still shows on the page.
MAGBOB_AV_SLOT = {
    "STAT_DmgVsScorched": "dmgvs_scorched", "STAT_DmgVsSuperMutants": "dmgvs_supermutants",
    "STAT_DmgVsRobots": "dmgvs_robots", "STAT_DmgVsGhouls": "dmgvs_ghouls",
    "STAT_DmgVsMirelurks": "dmgvs_mirelurks", "STAT_DmgVsYaoGuai": "dmgvs_yaoguai",
    "STAT_DmgVsLiberators": "dmgvs_liberators", "STAT_DmgVsHumans": "dmgvs_humans",
    "FrostResist": "cryo_res",
}
MAGBOB_DESC_SLOT = [                                  # first match wins
    (r"Cryolator Dmg", "dmg_cryolator"), (r"Alien Blaster Dmg", "dmg_alien"),
    (r"Throwing Weapon Dmg", "dmg_throwing"), (r"Fists & Knife Dmg", "dmg_fistknife"),
    (r"Unscoped Weapon Dmg", "dmg_unscoped"),
    (r"Dmg\. vs\. the (Wendigo|Mothman|Grafton Monster|Snallygaster|Flatwoods)", "dmgvs_cryptids"),
    (r"(Less Dmg\.|Damage) from Robots", "lessdmg_robots"), (r"Less Dmg\. from Insects", "lessdmg_insects"),
    (r"Less Dmg\. from Animals", "lessdmg_animals"), (r"Plasma Damage Taken", "lessdmg_plasma"),
    (r"Damage from Players", "lessdmg_players"),
    (r"Avoid Dmg\. From Scorched", "avoid_scorched"), (r"Avoid Dmg\. From Explosions", "avoid_explosion"),
    (r"Avoid Melee Dmg", "avoid_melee"), (r"Avoid Energy Dmg", "avoid_energy"), (r"Chance to Avoid Dmg", "avoid_all"),
    (r"Fusion Core Duration", "fusion_core"), (r"Heavy Weapon Ammo Consumption", "heavy_ammo"),
    (r"Harder to Detect", "sneak"),
]
# extra text for actor-value effects that have no slot and no description
MAGBOB_AV_TEXT = {
    "STAT_Lockpicking": "+{v} Lockpicking", "STAT_ItemDegradation": "-{v}% Item Condition Loss",
    "SURV_DiseaseChanceMult_Generic": "-{v}% Disease Chance", "SURV_DiseaseChanceMult_Combat": "-{v}% Disease Chance from Combat",
    "WaterBreathing": "Breathe Underwater", "STAT_ExplosionRadius": "+{v}% Explosion Radius",
    "STAT_SprintAPCost": "-{v}% Sprint AP Cost", "STAT_StealthBoyDuration": "+{v}% Stealth Boy Duration",
}
# conditions spelled out in the description -> when the buff counts
MAGBOB_WHEN = [(r"in a team", "team"), (r"at Night", "night"), (r"While Sneaking", "sneak"),
               (r"Under the Influence of Alcohol", "alcohol"), (r"While Swimming", "swim")]


def _num(v):
    return ("%g" % round(v, 3))


def magbob_text(desc, mag):
    t = desc or ""
    t = re.sub(r"<\+MAG>", "+" + _num(mag), t, flags=re.I)
    t = re.sub(r"<MAG>", _num(mag), t, flags=re.I)
    t = re.sub(r"<[^>]+>", "", t)
    t = re.sub(r"\s+", " ", t).strip().rstrip(".")
    if re.sub(r"[^A-Za-z]", "", t).isupper():          # "+30 LOCKPICK SWEETSPOT" -> "+30 Lockpick Sweetspot"
        t = re.sub(r"[A-Za-z]+", lambda m: m.group(0).capitalize(), t)
    return t


def magbob_when(desc):
    for rx, w in MAGBOB_WHEN:
        if re.search(rx, desc or "", re.I):
            return w
    return None
# Effects that are bookkeeping, perk-gated or one-shot — never a buff slot.
SKIP_EDID = re.compile(r"(_Duration$|UIDummy|PerkHappyGoLucky|Transition|ApplyClient|_AddPerk$|"
                       r"MagazineLiveLove|Restore(?!ActionPoints)|^SURV_(Food|Drink)_Effect$|VisualEffect|Visual_Player)", re.I)
# (20 Oct 2026: magazine effects are no longer skipped - magazines are their own type.
#  The Live & Love luck effect that rides on beers / spirits stays skipped.)
SKIP_EFFECT_KW = {"PerkHappyGoLuckyEffect", "LiveLoveLuckMagEffect"}
SKIP_ITEM_EDID = re.compile(r"(^AAA_|^zzz|^CUT_|DoNotUse|ResourceGenerator|TEMPLATE|_Test|Ferm$|FermDUPLICATE)", re.I)
REF_RE = re.compile(r"([A-Za-z_][\w]*)\s*(?:\"[^\"]*\"\s*)?\[(\w{4}):([0-9A-Fa-f]{8})\]")


def warn(msg):
    print(f"[food_buff_calc] WARNING: {msg}", file=sys.stderr)


def newest(data_dir, pattern, exclude=None):
    hits = [p for p in glob.glob(os.path.join(data_dir, pattern))
            if not (exclude and re.search(exclude, os.path.basename(p)))]
    return max(hits, key=lambda p: (_key(p), os.path.basename(p))) if hits else None


def read_tsv(path):
    if not path:
        return []
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(path, encoding=enc, newline="") as f:
                return list(csv.DictReader(f, delimiter="\t"))
        except UnicodeDecodeError:
            continue
    return []


def fnum(v, default=None):
    try:
        return float(str(v).strip().strip('"'))
    except (TypeError, ValueError):
        return default


def first_edid(v):
    m = re.match(r"\s*([A-Za-z_][\w]*)", str(v or ""))
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Perk entry points -> multiplier rules
# ---------------------------------------------------------------------------

def parse_cond(text):
    """'[tab 1] Subject.Func693(IngredientTypeMeat [KYWD:0013486B]) = 1 AND'
    (python extractor) or '... HasKeyword(IngredientTypeMeat [KYWD:..]) 10000000 1.000000'
    (xEdit). Returns (refs[(edid,sig)], negated, or_flag).
    Bethesda conditions: an OR-flagged line ORs with the next one; AND binds
    looser. So consecutive OR lines (plus the line that closes them) form an
    any-of group; plain AND lines are each required."""
    refs = [(m.group(1), m.group(2).upper()) for m in REF_RE.finditer(text)]
    tail = text[text.rfind(")") + 1:] if ")" in text else text
    m = re.search(r"(?<![\d.])([01]{8})\s+(-?[\d.]+)", tail) or \
        re.search(r"(!=|>=|<=|=|>|<)?\s*(-?[\d.]+)", tail)
    neg, is_or = False, bool(re.search(r"\bOR\s*$", tail.strip()))
    if m:
        op, val = m.group(1) or "=", fnum(m.group(2), 1.0)
        if re.fullmatch(r"[01]{8}", op):
            is_or = is_or or op[3] == "1"
            op = "=" if op[0] == "1" else "!="
        neg = (op == "=" and val == 0) or (op == "!=" and val == 1)
    return refs, neg, is_or


def conds_from_raw(raw):
    """The xEdit PERKEP script also writes a Raw column: every leaf under the
    perk effect as 'Name=Value ;; Name=Value'. When its Conditions column is
    empty (Oct 2026 xEdit build), rebuild the same condition lines from Raw:
      PRKC - Run On (Tab Index)=N   starts a condition tab
      Type=<8 bits>                 starts a condition (bit 4 = OR)
      Function= / Parameter #1= / Parameter #2= / Comparison Value=
    """
    out, tab, cur = [], None, None

    def flush():
        if cur is not None:
            params = ", ".join(p for p in cur["params"] if p and p not in ("00 00 00 00", "-1", "0"))
            out.append(f"[tab {tab}] {cur.get('fn', '')}({params}) {cur['type']} {cur.get('cmp', '1')}")

    for part in (raw or "").split(" ;; "):
        k, _, v = part.partition("=")
        k, v = k.strip(), v.strip()
        if k.startswith("PRKC"):
            flush(); cur = None
            tab = v
        elif k == "Type" and re.fullmatch(r"[01]{8}", v) and tab is not None:
            flush()
            cur = {"type": v, "params": []}
        elif cur is None:
            continue
        elif k == "Function":
            cur["fn"] = v
        elif k == "Comparison Value":
            cur["cmp"] = v
        elif k.startswith("Parameter #"):
            cur["params"].append(v)
        elif k.startswith("EPF"):
            flush(); cur = None
    flush()
    return out


def perk_rules(perkep_rows, perk_edid, item_kws, effect_kws):
    """Ingestible-magnitude multiply rules for one perk."""
    out = []
    for r in perkep_rows:
        if r.get("PERK_EDID") != perk_edid:
            continue
        ep, fn = str(r.get("EntryPoint", "")), str(r.get("Function", ""))
        # Entry point 29. The ESM reader writes the number; xEdit (FO76 defs)
        # writes its name, "Mod Spell Magnitude".
        if not (ep == "29" or "ingestible" in ep.lower() or "spell magnitude" in ep.lower()):
            continue
        if not (fn == "3" or "multiply" in fn.lower()):
            continue
        val = fnum(r.get("Value"))
        if val is None:
            m = re.search(r"EPFD[^=]*=\s*(-?[\d.]+)", r.get("Raw", "") or "")
            val = fnum(m.group(1)) if m else None
        if val is None:
            continue
        rule = {"mult": val, "version": None, "item_all": set(), "item_any": set(), "item_none": set(),
                "eff_all": set(), "eff_any": set(), "eff_none": set()}
        prev_or = {"item": False, "eff": False}
        cond_lines = [c for c in (r.get("Conditions") or "").split(" | ") if c.strip()]
        if not cond_lines:
            cond_lines = conds_from_raw(r.get("Raw"))
        for c in cond_lines:
            refs, neg, is_or = parse_cond(c)
            for edid, sig in refs:
                if sig == "CNDF" and "Mutation_Check_Use" in edid:
                    rule["version"] = "super" if "Super" in edid else "normal"
                elif sig == "KYWD":
                    cls = "item" if edid in item_kws else "eff" if edid in effect_kws else None
                    if not cls:
                        continue
                    if neg:
                        rule[cls + "_none"].add(edid)
                    elif is_or or prev_or[cls]:
                        rule[cls + "_any"].add(edid)
                    else:
                        rule[cls + "_all"].add(edid)
                    prev_or[cls] = is_or and not neg
        out.append(rule)
    return out


def rule_applies(rule, item_kw, eff_kw):
    if not rule["item_all"] <= item_kw or not rule["eff_all"] <= eff_kw:
        return False
    if rule["item_any"] and not (item_kw & rule["item_any"]):
        return False
    if item_kw & rule["item_none"]:
        return False
    if rule["eff_any"] and not (eff_kw & rule["eff_any"]):
        return False
    if eff_kw & rule["eff_none"]:
        return False
    return True


def mult_for(rules, item_kw, eff_kw, version):
    m = 1.0
    for r in rules:
        if r["version"] and r["version"] != version:
            continue
        if rule_applies(r, item_kw, eff_kw):
            m *= r["mult"]
    return round(m, 4)


def rules_json(rules):
    return [{"x": r["mult"], "version": r["version"],
             "itemAll": sorted(r["item_all"]), "itemAny": sorted(r["item_any"]), "itemNone": sorted(r["item_none"]),
             "effectAll": sorted(r["eff_all"]), "effectAny": sorted(r["eff_any"]),
             "effectNone": sorted(r["eff_none"])} for r in rules]


# ---------------------------------------------------------------------------
# Rules file
# ---------------------------------------------------------------------------

def load_rules():
    with open(RULES_PATH, encoding="utf-8") as f:
        rules = json.load(f)
    slots = rules.get("slots") or {}
    bad = []
    for qk, q in (rules.get("questions") or {}).items():
        for ok, o in (q.get("options") or {}).items():
            for part in ("weights", "add", "mult", "fallback_add"):
                for s in (o.get(part) or {}):
                    if s not in slots:
                        bad.append(f"{qk}.{ok}.{part}.{s}")
            for s in o.get("avoid") or []:
                if s not in slots:
                    bad.append(f"{qk}.{ok}.avoid.{s}")
    if bad:
        raise SystemExit("[food_buff_calc] FATAL: rules.json names unknown slots: " + ", ".join(bad))
    # goals come from the SPECIALs & Stats build (dist/specials_stats.json -> goals)
    sp_path = os.path.join(REPO, "dist", "specials_stats.json")
    if os.path.exists(sp_path):
        with open(sp_path, encoding="utf-8") as f:
            sp_goals = (json.load(f) or {}).get("goals") or {}
        refs = ((rules.get("goals") or {}).get("refs") or {})
        for qk, q in (rules.get("questions") or {}).items():
            for ok, o in (q.get("options") or {}).items():
                for g in (o.get("goals") or {}):
                    if g not in sp_goals:
                        warn(f"rules.json {qk}.{ok}.goals.{g}: not a goal in specials_stats.json")
                    if g not in refs:
                        warn(f"rules.json {qk}.{ok}.goals.{g}: no goals.refs entry")
    rules.pop("_readme", None)
    return rules


# ---------------------------------------------------------------------------
# Main build
# ---------------------------------------------------------------------------

def build(data_dir, dist_dir):
    S = B.Sources(data_dir, dist_dir)
    rules = load_rules()

    # MGEF extras: ActorValue + PerkToApply (main export) and keywords (MGEFKW).
    mgef_av, mgef_perk, mgef_desc = {}, {}, {}
    for r in read_tsv(S.mgef_path):
        fid = B.norm_fid(r.get("MGEF_FormID", ""))
        mgef_av[fid] = (r.get("ActorValue") or "").strip()
        mgef_desc[fid] = (r.get("DNAM_MagicItemDescription") or "").strip()
        mgef_perk[(r.get("EDID") or "").strip()] = first_edid(r.get("PerkToApply"))
    kw_path = newest(data_dir, "MGEFKW_Export_*.tsv")
    ep_path = newest(data_dir, "PERKEP_Export_*.tsv")
    if not kw_path or not ep_path:
        raise SystemExit("[food_buff_calc] FATAL: need tsv/MGEFKW_Export_*.tsv and tsv/PERKEP_Export_*.tsv - "
                         "run the two new xEdit scripts or tools/esm_buff_rules_extract.py")
    mgef_kw, mgef_detr = {}, {}
    for r in read_tsv(kw_path):
        fid = B.norm_fid(r.get("MGEF_FormID", ""))
        mgef_kw[fid] = set(re.findall(r"([A-Za-z_][\w]*)\s*\[", r.get("Keywords_Flat") or ""))
        mgef_detr[fid] = str(r.get("Detrimental", "0")).strip() == "1"
    perkep = read_tsv(ep_path)

    all_item_kws = set()
    for rec in S.alch.values():
        all_item_kws |= set(rec.get("keyword_edids") or [])
    all_eff_kws = set()
    for v in mgef_kw.values():
        all_eff_kws |= v

    # Mutation -> perks, from the SPEL effects and MGEF PerkToApply.
    diet_perks = {"carnivore": [], "herbivore": []}
    for fid, rows in S.spel_effects.items():
        sp = (S.spel.get(fid) or {}).get("SPEL_EDID", "")
        key = {"Mutation_Carnivore": "carnivore", "Mutation_Herbivore": "herbivore"}.get(sp)
        if not key:
            continue
        for r in rows:
            p = mgef_perk.get((r.get("EFID_MGEF_EDID") or "").strip())
            if p and p not in diet_perks[key]:
                diet_perks[key].append(p)
    diet_rules = {k: [rr for p in v for rr in perk_rules(perkep, p, all_item_kws, all_eff_kws)]
                  for k, v in diet_perks.items()}
    for k, v in diet_rules.items():
        if not v:
            warn(f"no ingestible rules found for {k} (perks {diet_perks[k]}) - diet multipliers will be x1")
    party = {"rank1": perk_rules(perkep, "PartyGirl01", all_item_kws, all_eff_kws),
             "rank2": perk_rules(perkep, "PartyGirl02", all_item_kws, all_eff_kws)}

    chem_fiend = 2.0
    for fid, rows in S.spel_effects.items():
        if (S.spel.get(fid) or {}).get("SPEL_EDID") == "AbPerkChemFiend":
            m = fnum(rows[0].get("EFIT_Magnitude"), 1.0) if rows else 1.0
            chem_fiend = 1 + (m if m <= 10 else m / 100.0)

    # Categories (same rules as the buff pages / item categories).
    cats = {}
    for cat in ("food", "canned", "prewar_candy", "chems", "alcohol", "nuka_cola", "serums"):
        items, _ = B.resolve_category(cat, B.CATEGORIES[cat], S.kywd, S.alch)
        for it in items:
            cats.setdefault(B.norm_fid(it["form_id"]), set()).add(cat)

    # Buffs n Brew menu.
    menu_by_fid, menu_by_edid, menu_by_name = {}, {}, {}
    for r in read_tsv(os.path.join(data_dir, "BnB_Menu_Items.tsv")):
        def price(v):
            n = fnum(v)
            return int(n) if n is not None else None
        av = (r.get("availability") or "").strip()
        row = {"n": (r.get("name") or "").strip(), "cat": (r.get("menu category") or "").strip(),
               "px": price(r.get("price_xbox")), "pp": price(r.get("price_ps")), "pc": price(r.get("price_pc")),
               "lim": price(r.get("limit_new")), "limX": price(r.get("limit_existing")),
               "av": av}
        if not row["n"]:
            continue
        if B.norm_fid(r.get("form_id") or ""):
            menu_by_fid[B.norm_fid(r["form_id"])] = row
        if (r.get("edid") or "").strip():
            menu_by_edid[r["edid"].strip()] = row
        menu_by_name.setdefault(row["n"].lower(), row)

    items = []
    for fid, rec in S.alch.items():
        edid = rec.get("edid") or ""
        name = B.clean(rec.get("name"))
        if not name or SKIP_ITEM_EDID.search(edid) or B.edid_is_cut(edid):
            continue
        kw = set(rec.get("keyword_edids") or [])
        c = cats.get(fid, set())
        if "serums" in c:
            continue
        if BOBBLE_ITEM.search(edid):
            typ = "bobblehead"
        elif MAGAZINE_ITEM.search(edid):
            typ = "magazine"
        elif GUM_ITEM.search(edid):
            typ = "gum"
        elif "alcohol" in c or "DrinkTypeAlcohol" in kw:
            typ = "alcohol"
        elif "chems" in c:
            typ = "chem"
        elif c & {"food", "canned", "prewar_candy", "nuka_cola"} or kw & {"ObjectTypeFood", "ObjectTypeDrink"}:
            typ = "food"
        else:
            continue

        fx, sup, feral = [], False, 0.0
        all_dg = set()          # dispel keywords on ANY effect of the item (what it wipes when taken)
        for r in S.alch_effects.get(fid, []):
            e = S.build_effect(mgef_form_id=r["MGEF_FormID"], mgef_edid=r["MGEF_EDID"], mgef_full=r["MGEF_FULL"],
                               magnitude=r["EFIT_Magnitude"], duration=r["EFIT_Duration"],
                               mag_glob=r["MAGG_GLOB_EDID"], dur_glob=r["DURG_GLOB_EDID"])
            mfid = e["formId"]
            ek = mgef_kw.get(mfid, set())
            # FortifyLuckMagazineLiveLove (on beers / spirits) is a Fallout 4 leftover: its
            # conditions need PerkMagLiveNLove05 (never given in 76) AND an active companion,
            # so it never switches on and its MagazineDispelEffects keyword never fires
            # (checked in SeventySix.esm, 21 Oct 2026; Kat confirms drinks don't remove magazines).
            if not (ek & SKIP_EFFECT_KW or (e.get("edid") or "") == "FortifyLuckMagazineLiveLove"):
                all_dg |= {k for k in ek if "Dispel" in k}
            av = mgef_av.get(mfid, "")
            mag, dur = e.get("magnitude") or 0.0, e.get("duration") or 0.0
            # A magnitude / duration GLOBAL is what the game reads when it is set;
            # a non-zero EFIT value next to it is a leftover (e.g. Tasty Mutant Hound
            # Stew EFIT 25 vs SURV_Food_Effect_FortifyMeleeDamage_Mag_3_High = 85,
            # the same global Tasty Yao Guai Roast uses with EFIT 0).
            mg, dg = (r.get("MAGG_GLOB_EDID") or "").strip(), (r.get("DURG_GLOB_EDID") or "").strip()
            if mg and mg in S.globs:
                mag = S.globs[mg]
            if dg and dg in S.globs and S.globs[dg] > 0:
                dur = S.globs[dg]
            eed = e.get("edid") or ""
            if "Mutations Suppressed" in (e.get("dnam") or "") or "Mutations Suppressed" in (e.get("display") or ""):
                sup = True
            # side effects first
            if "RadiationInjestion" in ek or (av == "Rads" and eed.startswith("Damage")):
                slot, neg = "rads", True
            elif "SURV_EffectTypeDiseaseVector" in ek:
                slot, neg = "disease", True
            elif eed.startswith("AddictionOdds"):
                slot, neg = "addiction", True
            elif eed == "GHL_SURV_Chem_Effect" or "GHL_SURV_EffectTypeFeral" in ek:
                feral = max(feral, mag)
                continue
            elif HUNGER_EDID.search(eed):
                slot, neg = "hunger_thirst", False
                if not mag or dur < 60:
                    continue
            elif eed.startswith("RestoreActionPoints"):
                slot, neg = "ap_refill", False          # AP per second while it lasts
                if not mag or dur < 60:
                    continue
            else:
                slot = AV_SLOT.get(av)
                if typ in ("magazine", "bobblehead") and dur >= 60:
                    desc = mgef_desc.get(mfid, "")
                    slot = slot or MAGBOB_AV_SLOT.get(av)
                    mb_when, mb_text = magbob_when(desc), None
                    if not slot:
                        slot = next((sl for rx, sl in MAGBOB_DESC_SLOT if re.search(rx, desc, re.I)), None)
                        if slot:
                            mb_text = magbob_text(desc, mag)     # keep the game's words ("+15% Dmg. vs. the Wendigo")
                    if not slot:
                        mb_text = magbob_text(desc, mag) if desc else \
                            (MAGBOB_AV_TEXT[av].replace("{v}", _num(mag)) if av in MAGBOB_AV_TEXT else None)
                        if not mb_text:
                            continue
                        slot = "other"
                    neg = False
                    f = {"s": slot, "m": round(mag, 4), "d": int(round(dur)), "st": eed, "e": eed}
                    if mb_text:
                        f["t"] = mb_text
                    if mb_when:
                        f["w"] = mb_when
                    dgs = sorted(k for k in ek if "Dispel" in k)
                    if dgs:
                        f["dg"] = dgs
                    fx.append(f)
                    continue
                if not slot or SKIP_EDID.search(eed) or ek & SKIP_EFFECT_KW or not mag:
                    continue
                if dur < (10 if slot == "invis" else 60):
                    continue
                neg = mgef_detr.get(mfid, False) or eed.startswith(("Reduce", "Damage"))
            if slot == "xp" and 0 < mag < 1:
                mag *= 100.0
            if slot == "invis":
                mag = 1.0
            stack = next((k for k in sorted(ek) if k.startswith("Stack")), None) or \
                next((k for k in sorted(ek) if k.startswith("FoodDispelEffect_")), None) or eed
            dm = {}
            for d, rr in diet_rules.items():
                n, s = mult_for(rr, kw, ek, "normal"), mult_for(rr, kw, ek, "super")
                if n != 1 or s != 1:
                    dm[d[0]] = [n, s]
            pm = []
            if typ == "alcohol" and party["rank1"]:
                p1 = mult_for(party["rank1"], kw, ek, None)
                p2 = mult_for(party["rank2"], kw, ek, None) if party["rank2"] else p1
                if (p1 != 1 or p2 != 1) and not neg:
                    pm = [p1, p2]
            f = {"s": slot, "m": round(mag, 4), "d": int(round(dur)), "st": stack,
                 "e": "Gum_BigHungerThirst" if eed in BIG_GUM_EDID else eed}
            # DISPEL GROUPS — the real stacking rule (checked against the whole ESM,
            # 20 Oct 2026: these keywords are referenced by nothing except the effects
            # that carry them, so the engine works off the keyword itself). Taking an
            # item replaces every active effect that shares one of its dispel keywords:
            #   FoodDispelEffect_<Buff>  one per food buff type -> foods of DIFFERENT
            #                            buff types stack, same type replaces
            #   AlcoholDispelEffect      on (almost) every alcohol buff -> ONE drink at a time
            #   ChemDispelEffects        on (almost) every chem buff    -> ONE chem at a time
            #   ChemDispelRadX           Rad-X family, its own slot
            # An effect with no dispel keyword is never replaced by another item.
            dg = sorted(k for k in ek if "Dispel" in k)
            if dg:
                f["dg"] = dg
            if neg:
                f["neg"] = 1
            if dm:
                f["dm"] = dm
            if pm:
                f["pm"] = pm
            fx.append(f)

        if typ in ("magazine", "bobblehead"):
            # A description-only effect that names a condition ("+5% Damage When in a
            # Team") describes the item's real actor-value effect: copy the condition
            # onto those effects; drop the description when it only repeats the number.
            mapped = [f for f in fx if f["s"] != "other" and not f.get("w")]
            for f in [f for f in fx if f.get("w") and (f["s"] == "other" or "UIDummy" in f["e"])]:
                for g in mapped:
                    g["w"] = f["w"]
                real = [g for g in fx if g["s"] != "other" and g is not f]
                if mapped or any(abs(g["m"] - f["m"]) < 1e-6 for g in real) or f["s"] != "other":
                    fx.remove(f)
            # the same buff listed twice (e.g. Guns and Bullets 6: two DR-at-night effects) -> once
            seen, keep = set(), []
            for f in fx:
                k = (f["s"], f["m"], f.get("t"))
                if k in seen:
                    continue
                seen.add(k)
                f.pop("dummy", None)
                keep.append(f)
            fx = keep
            if not fx:
                continue
        elif not any(f["s"] in rules["slots"] and not f.get("neg") for f in fx):
            continue
        # An "alcohol" whose buffs are all FOOD buffs (FoodDispelEffect_* stack) is a
        # food as far as stacking goes: it overwrites the Perception food, not an
        # alcohol buff. Sweetwater Special Blend is the live case.
        buff_stacks = [f["st"] for f in fx if not f.get("neg") and f["s"] in rules["slots"]]
        if typ != "food" and buff_stacks and all(st.startswith("FoodDispelEffect_") for st in buff_stacks):
            typ = "food"
            for f in fx:
                f.pop("pm", None)
        diet = sorted(k.replace("IngredientType", "") for k in kw
                      if k in ("IngredientTypeMeat", "IngredientTypeVegetable", "IngredientTypeHerb", "IngredientTypeFruit"))
        menu = menu_by_fid.get(fid) or menu_by_edid.get(edid) or menu_by_name.get(name.lower())
        it = {"id": fid, "edid": edid, "n": name, "t": typ, "diet": diet, "fx": fx}
        if all_dg:
            it["dgAll"] = sorted(all_dg)
        if sup:
            it["sup"] = 1
        if feral:
            it["feral"] = round(feral, 2)
        if menu:
            it["menu"] = menu
        items.append(it)

    # One row per (type, name): prefer the Buffs n Brew one, then the most effects.
    best = {}
    for it in items:
        k = (it["t"], it["n"].lower())
        cur = best.get(k)
        score = (1 if it.get("menu") and (it["menu"].get("pc") is not None) else 0, len(it["fx"]), -int(it["id"], 16))
        if not cur or score > cur[0]:
            best[k] = (score, it)
    items = sorted((v[1] for v in best.values()), key=lambda i: (i["t"], i["n"].lower()))

    # ── Seasonal fish dishes (Kat, 20 Oct 2026) ─────────────────────────────
    # A dish is seasonal when EVERY recipe for it (COBJ, followed through crafted
    # parts like Summer Filet) needs a SeasonalFish_* part. The season comes from
    # the part's EDID (SeasonalFish_Meal_SummerFillet) or, for raw fish, from
    # dist/seasonal_fish_guide.json (fish name -> season). The page uses
    # mechanics.fishSeasons (the guide's rollover dates) to know the current season.
    guide_path = os.path.join(REPO, "dist", "seasonal_fish_guide.json")
    guide = {}
    if os.path.exists(guide_path):
        with open(guide_path, encoding="utf-8") as f:
            guide = json.load(f)
    else:
        warn("dist/seasonal_fish_guide.json not found - seasonal fish dishes can't be dated")
    fish_season = {}
    for sea in guide.get("seasons") or []:
        for fsh in sea.get("fish") or []:
            fish_season[re.sub(r"[^a-z]", "", fsh.get("name", "").lower())] = sea["key"]
    cobj_path = newest(data_dir, "COBJ_Export_*.tsv")
    recipes = {}
    for r in read_tsv(cobj_path):
        out_e = (r.get("CNAM_EDID") or "").strip()
        comps = [c.split(":")[0].strip() for c in (r.get("FVPA") or "").split("|") if c.strip()]
        if out_e and comps:
            recipes.setdefault(out_e, []).append(comps)

    def part_season(e):
        if not e.startswith("SeasonalFish_"):
            return None
        for sk in ("spring", "summer", "fall", "winter"):
            if sk.capitalize() in e:
                return sk
        tail = re.sub(r"[^a-z]", "", e.split("_")[-1].lower())
        return fish_season.get(tail)

    def dish_season(e, seen=()):
        ps = part_season(e)
        if ps or e in seen:
            return ps
        found = []
        for comps in recipes.get(e, []):
            rs = next((x for x in (dish_season(c, seen + (e,)) for c in comps) if x), None)
            if not rs:
                return None          # a recipe without seasonal fish -> not seasonal
            found.append(rs)
        return found[0] if found else None

    n_seasonal = 0
    for it in items:
        sea = dish_season(it["edid"])
        if sea:
            it["season"] = sea
            n_seasonal += 1
    if not cobj_path:
        warn("no COBJ_Export_*.tsv - seasonal fish dishes not tagged")

    # rules.json "prefer" names must be real items (e.g. Bows & Throwing -> Wine)
    names = {(i["t"], i["n"]) for i in items}
    for ok, o in ((rules.get("questions") or {}).get("build") or {}).get("options", {}).items():
        for t, lst in (o.get("prefer") or {}).items():
            for n in lst:
                if (t, n) not in names:
                    warn(f"rules.json build.{ok}.prefer.{t}: no {t} item called '{n}'")
    item_names = {i["n"] for i in items}
    for n in rules.get("exclude_items") or []:
        if n not in item_names:
            warn(f"rules.json exclude_items: no item called '{n}'")

    mech = {
        "diet": {k: {"perks": diet_perks[k], "rules": rules_json(v)} for k, v in diet_rules.items()},
        "partyGirl": [party["rank1"][0]["mult"] if party["rank1"] else 2.0,
                      party["rank2"][0]["mult"] if party["rank2"] else 3.0],
        "chemFiend": round(chem_fiend, 3),
    }
    # Magazine / bobblehead duration perks (20 Oct 2026): entry point 30 (spell
    # duration), multiply, conditioned on BobbleheadKeyword / MagazineKeyword.
    #   Curator01                 Intelligence card, x2
    #   Legendary_CollectorsPerk  Collector's legendary armour, one row per piece count
    def ep_values(perk_edid):
        rows = [r for r in perkep if (r.get("PERK_EDID") or "").strip() == perk_edid
                and (r.get("EntryPoint") or "").strip() in ("30", "Mod Spell Duration")]   # v3 export names it
        rows.sort(key=lambda r: int(fnum(r.get("EffectIndex"), 0)))
        return [round(fnum(r.get("Value"), 1.0), 3) for r in rows]
    cur = ep_values("Curator01")
    col = ep_values("Legendary_CollectorsPerk")
    if cur:
        mech["curator"] = {"mult": cur[0], "perk": "Curator01", "card": "Curator", "special": "Intelligence"}
    else:
        warn("Curator01 not found in PERKEP - magazine/bobblehead duration perk missing")
    if col:
        mech["collectors"] = {"byPieces": col, "perk": "Legendary_CollectorsPerk"}   # index 0 = 1 piece
    else:
        warn("Legendary_CollectorsPerk not found in PERKEP")
    sr = guide.get("seasonRule") or {}
    if sr.get("rolloverDates"):
        mech["fishSeasons"] = {
            "rollover": sr["rolloverDates"],            # [{date, season}] - season starts 16:00 UTC that day
            "rolloverHourUTC": 16,
            "months": {sea["key"]: sea.get("months") for sea in guide.get("seasons") or []},
            "labels": {sea["key"]: sea.get("label") for sea in guide.get("seasons") or []},
            "rule": sr.get("description", ""),
            "src": "dist/seasonal_fish_guide.json",
        }
    out = {
        "version": 1,
        "generated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "page": PAGE_URL,
        "sources": {k: os.path.basename(v) for k, v in {
            "alch": S.alch_path, "alchEffects": S.alch_eff_path, "mgef": S.mgef_path,
            "mgefKeywords": kw_path, "perkEntryPoints": ep_path, "cobj": cobj_path}.items() if v},
        "rules": rules,
        "mechanics": mech,
        "counts": {t: sum(1 for i in items if i["t"] == t) for t in ("food", "chem", "alcohol", "gum", "magazine", "bobblehead")},
        "items": items,
    }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.path.join(REPO, "tsv"))
    ap.add_argument("--outdir", default=os.path.join(REPO, "dist"))
    a = ap.parse_args()
    prev = os.getcwd()
    os.chdir(REPO)
    try:
        out = build(a.data_dir, a.outdir)
    finally:
        os.chdir(prev)
    path = os.path.join(a.outdir, "calculators", "food_buff_calculator.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[food_buff_calc] wrote {path}: {out['counts']}  diet perks: "
          f"{ {k: v['perks'] for k, v in out['mechanics']['diet'].items()} }  "
          f"partyGirl {out['mechanics']['partyGirl']}  chemFiend x{out['mechanics']['chemFiend']}")


if __name__ == "__main__":
    main()
