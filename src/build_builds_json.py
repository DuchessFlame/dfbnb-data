#!/usr/bin/env python3
"""
build_builds_json.py
====================
Builds the data for the BnB Builds category: the build calculator
("Make Your Own Build") and every preset build page (Heavy Guns, Melee,
Pistol, Rifle, Bows & Throwing, Raid, Ghoul, and the weapon-built pages).

    dist/builds/builds_data.json       everything the calculator chooses from
    dist/builds/build_templates.json   the preset pages (validated copy of the
                                       hand-kept data/builds/build_templates.json)

Renderer: df-bnb-builds.js (theme assets) reads both files from
raw.githubusercontent.com/DuchessFlame/dfbnb-data/main/dist/builds/.

DATA SOURCES (all already in the repo)
--------------------------------------
  tsv/PCRD_Export_*.tsv                perk card point cost per rank (CardRank)
  dist/perk_cards.json                 perk card names, SPECIAL, rank text, race
  tsv/WEAP_Export_*_Base.tsv           player weapons, keywords (class)
  tsv/WEAP_Export_*_DNAM.tsv           ammo, AP cost, capacity, weight
  tsv/OMOD_Export_*.tsv                weapon mods (attach point, target ma_ keyword)
  tsv/OMOD_Export_*_Properties.tsv     mod properties (quiet / auto / damage)
  dist/legendary_mods.json             1-4 star legendary effects + unique mods
  dist/unique_weapons_armour/...json   unique versions of a base weapon
  dist/armour.json                     armour + power armour set names
  dist/buffs/*.json                    mutations, food, drink, chems, alcohol,
                                       magazines, bobbleheads, nuka-cola
  data/builds/build_templates.json     HAND-KEPT: preset builds, tag overrides,
                                       armour set notes, game rules (coin costs,
                                       legendary slot levels)

TAGS
----
Everything the calculator can pick (perk cards, legendary effects, weapon mods,
mutations, consumables, armour sets) is tagged from its own text by the rules
in this file, then corrected by the overrides in build_templates.json. The page
scores each candidate against what the player picked (weapon class, PA, VATS,
health style, playstyle) using those tags. To change WHY something gets picked,
change a tag override — not the page script.

Usage:
  python src/build_builds_json.py
"""

import csv, json, os, re, sys, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tsv_source  # noqa: E402
from builds_game_records import Records, legendary_mod_effects  # noqa: E402

SRC_DIR   = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
DIST      = REPO_ROOT / "dist"
OUT_DIR   = DIST / "builds"
TEMPLATES = REPO_ROOT / "data" / "builds" / "build_templates.json"

csv.field_size_limit(10 ** 9)

SPECIALS = ["Strength", "Perception", "Endurance", "Charisma", "Intelligence", "Agility", "Luck"]
SPECIAL_KEY = {s: s[0] for s in SPECIALS}  # S P E C I A L


# ---------------------------------------------------------------- helpers
def read_tsv(path):
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_json(p):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def q(s):
    return str(s or "").strip().strip('"').strip()


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", str(s).lower()).strip("-")


def num(v, d=None):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return d


def kw_edids(cell):
    """'WeaponTypeRifle "Rifle" [KYWD:0004A0A1]|ma_X [KYWD:..]' -> ['WeaponTypeRifle', 'ma_X']"""
    out = []
    for part in str(cell or "").split("|"):
        part = part.strip()
        if not part:
            continue
        out.append(part.split(" ")[0].split("[")[0].strip())
    return out


def add_tags(tags, *new):
    for t in new:
        if t and t not in tags:
            tags.append(t)


# ---------------------------------------------------------------- tag rules
# (regex, tags) — run against lower-cased text. Order doesn't matter; every
# matching rule adds its tags.
TEXT_RULES = [
    # weapon classes
    (r"heavy gun|big gun", ["heavy"]),
    (r"small gun", ["rifle", "pistol", "shotgun"]),
    (r"\brifle", ["rifle"]),
    (r"pistol", ["pistol"]),
    (r"shotgun|3 or more projectiles|per projectile", ["shotgun"]),
    (r"melee|power attack", ["melee"]),
    (r"\bfists?\b|unarmed", ["unarmed"]),
    (r"\bbows?\b|arrow|crossbow", ["bow"]),
    (r"throw(n|ing)", ["thrown"]),
    (r"explosive|grenade|mini-nuke", ["explosive"]),
    (r"energy (weapon|gun)|energy damage", ["energy"]),
    (r"ballistic", ["ballistic"]),
    (r"ranged", ["ranged"]),
    (r"scoped|scope", ["scoped"]),
    (r"silenced", ["silenced"]),
    (r"pipe weapon", ["pipe"]),
    (r"power tool|chainsaw", ["powertool"]),
    (r"cryo", ["cryo"]),
    (r"\bfire damage|flame", ["fire"]),
    # play modes
    (r"v\.a\.t\.s|\bvats\b|critical meter", ["vats"]),
    (r"hip ?fire|outside v\.a\.t\.s|aiming down sights|while aiming", ["nonvats"]),
    (r"sneak|stealth|detect", ["sneak"]),
    (r"\(no power armor\)|not wearing armor", ["nopa_only"]),
    (r"(in|wearing|your|while in) power armor|fusion core", ["pa_only"]),
    (r"teammate|team\b|on a team|nearby ghouls on your team|revive", ["team"]),
    (r"not on a team", ["solo"]),
    (r"health is low|below \d+% health|lower your health|low health", ["lowhp"]),
    (r"critical", ["crit"]),
    (r"kill streak", ["killstreak"]),
    (r"\bglow\b|feral", ["ghoulmech"]),
    # roles
    (r"damage", ["damage"]),
    (r"ignores? \d+% (of )?(your target's |target's )?armor|ignore.*armor", ["armorpen", "damage"]),
    (r"take (increased|more) damage|enemies take", ["debuff", "damage"]),
    (r"resist|deflect|evade|harder to stagger|less damage|block|damage reduction|immune to stagger", ["defense"]),
    (r"health|hp\b|heal|stimpak|regen", ["heal"]),
    (r"action point|\bap\b", ["ap"]),
    (r"weigh|carry weight", ["carry"]),
    (r"find more|reap|harvest|extra meat|canned|bottle caps|caps\b|meat from", ["farm"]),
    (r"craft|repair|break .* slower|durability|condition|scrap", ["craft"]),
    (r"lockpick|picklock", ["lockpick"]),
    (r"hacking|terminal", ["hack"]),
    (r"hunger|thirst|disease|food|drink|rads?\b|radiation", ["survival"]),
    (r"movement speed|run(ning)? speed|sprint", ["speed"]),
    (r"\bxp\b|experience", ["xp"]),
    (r"accuracy", ["accuracy"]),
    (r"reload", ["reload"]),
    (r"stagger", ["stagger"]),
    (r"limb", ["limb"]),
    (r"bleed", ["bleed"]),
    (r"mutation", ["mutation"]),
    (r"bobblehead|magazine", ["farm"]),
    (r"alcohol|liquor|nuka-cola", ["consumable"]),
    (r"damage (to|vs\.?|against) (humans|animals|robots|ghouls|scorched|super mutants|mirelurks|insects|bugs)|at night|during the day|outdoors|max caps|glowing enemies", ["situational"]),
]
STAT_RULES = [(rf"\b({s}|{s[:3]})\b", f"stat_{SPECIAL_KEY[s]}") for s in SPECIALS]
WEAPON_CLASSES = ["heavy", "rifle", "pistol", "shotgun", "melee", "unarmed", "bow", "thrown", "explosive"]


def tag_text(text):
    t = (text or "").lower()
    tags = []
    for rx, tg in TEXT_RULES:
        if re.search(rx, t):
            add_tags(tags, *tg)
    for rx, tg in STAT_RULES:
        if re.search(rx, t, re.I):
            add_tags(tags, tg)
    return tags


def apply_override(tags, ov):
    if not ov:
        return tags
    if ov.get("set") is not None:
        tags = list(ov["set"])
    for t in ov.get("remove", []):
        if t in tags:
            tags.remove(t)
    add_tags(tags, *ov.get("add", []))
    return tags


def pct_values(text):
    """'+25% ranged damage' -> [25]. Used for the damage summary."""
    return [float(x) for x in re.findall(r"\+?(\d+(?:\.\d+)?)\s*%", text or "")]


def dmg_bucket(text):
    """Which bucket a damage effect lands in. Additive bonuses sum together;
    the separate buckets multiply against them (and each other)."""
    t = (text or "").lower()
    if not re.search(r"damage|dmg", t):
        return None
    if re.search(r"ignore|armor|armour", t) and re.search(r"ignore", t):
        return "armorpen"
    if re.search(r"sneak", t):
        return "sneak"
    if re.search(r"critical", t):
        return "crit"
    if re.search(r"take (increased|more) damage|enemies take", t):
        return "target"
    if re.search(r"weak spot|weak point", t):
        return "weakspot"
    if re.search(r"bleed|poison|fire|cryo|energy dmg|explode", t):
        return "extra"
    return "additive"


# ---------------------------------------------------------------- perks
def build_perks(tpl):
    pc = load_json(DIST / "perk_cards.json")
    pcrd_path = tsv_source.newest("PCRD_Export_*.tsv")
    pcrd = {q(r.get("PCRD_FormID")).upper(): r for r in read_tsv(pcrd_path)}
    overrides = tpl.get("perkTagOverrides", {})
    perks, legendary, skipped = [], [], []
    for c in pc["cards"]:
        name = c["name"]
        alltext = " ".join([c.get("desc", "")] + [r.get("desc", "") for r in c.get("ranks", [])])
        if "deprecated" in alltext.lower():
            skipped.append(name)
            continue
        row = pcrd.get(str(c.get("formId", "")).upper(), {})
        ranks = []
        for r in c.get("ranks", []):
            if r.get("desc"):
                # unresolved game text variables, e.g. "<+GLOBAL=0x00835A6B>%" -> "extra"
                r = dict(r, desc=re.sub(r"<\+?GLOBAL=[^>]*>%?", "extra", r["desc"]))
            n = r.get("rank", len(ranks) + 1)
            cost = num(row.get(f"Rank_{n}_CardRank"), None)
            ranks.append({
                "rank": n,
                "cost": int(cost) if cost else n,
                "desc": r.get("desc", "") or "",
            })
        # fill blank rank text with the nearest known rank so the page never shows an empty line
        last = next((r["desc"] for r in ranks if r["desc"]), "")
        for r in ranks:
            if r["desc"]:
                last = r["desc"]
            else:
                r["desc"] = last
        rank_text = " ".join(r["desc"] for r in ranks)
        tags = tag_text(name + " " + rank_text)
        tags = apply_override(tags, overrides.get(name))
        section = {"Legendary Perk Cards": "legendary", "Ghoul Perk Cards": "ghoul"}.get(c["section"], "human")
        race = "ghoul" if section == "ghoul" else ("human" if c.get("humanOnly") else "both")
        # what each rank really does to damage, from the PERK entry points and
        # the card's Ability spell (rank + weapon conditions resolved)
        for i, fx in enumerate(REC.perk_rank_effects(c.get("formId", "")) if REC else []):
            if i < len(ranks) and fx:
                # an effect whose strength is set outside the records (magnitude 0,
                # no scaling stat) keeps the card's own number, marked as card text
                for e in fx:
                    if not e.get("value") and not e.get("scales"):
                        vals = pct_values(ranks[i]["desc"])
                        if vals:
                            e["value"], e["src"] = vals[0], "card text"
                ranks[i]["fx"] = [e for e in fx if e.get("value") or e.get("scales")]
        for r in ranks:
            if "fx" not in r and "damage" in tags:
                b = dmg_bucket(r["desc"])
                vals = pct_values(r["desc"])
                if b and vals:
                    grp = {"additive": "Damage bonus", "armorpen": "Ignore target armour (perk)", "sneak": "Sneak attack damage",
                           "crit": "Critical hit damage", "target": "Increase Damage Taken (on the enemy)",
                           "weakspot": "Fortify Weak Spot Damage", "extra": "Extra damage"}[b]
                    r["fx"] = [{"group": grp, "value": vals[0], "unit": "%", "src": "card text"}]
        entry = {
            "id": c["id"],
            "name": name,
            "special": c["special"] if c["special"] in SPECIALS else None,
            "section": section,
            "race": race,
            "minLevel": int(c.get("minLevel") or 1),
            "ranks": ranks,
            "tags": tags,
        }
        if section == "legendary":
            legendary.append(entry)
        else:
            if not entry["special"]:
                skipped.append(name)
                continue
            perks.append(entry)
    return perks, legendary, skipped


# ---------------------------------------------------------------- weapons
NPC_EDID = re.compile(r"^(cr[A-Z_]|zzz|[Dd][Ee][Ll]_|HTO_|[Dd]ebug|POST_|W05_|[Tt]est|ATX_|Bounty_|[Dd]ummy|Gutsy)|_NONPLAYABLE|[Vv]ertibird|_NPC|[Tt]urret")
CLASS_KW = [
    ("heavy", {"WeaponTypeHeavyGun"}),
    ("melee", {"WeaponTypeMelee1H", "WeaponTypeMelee2H", "WeaponTypeMeleeGeneral"}),
    ("unarmed", {"WeaponTypeUnarmed", "WeaponTypeHandToHand", "WeaponTypeGloves"}),
    ("bow", {"WeaponTypeBow"}),
    ("thrown", {"WeaponTypeThrown"}),
    ("explosive", {"WeaponTypeExplosive", "WeaponTypeGrenadeLauncher", "WeaponTypeMissileLauncher"}),
    ("shotgun", {"WeaponTypeShotgun"}),
    ("pistol", {"WeaponTypePistol"}),
    ("rifle", {"WeaponTypeRifle", "WeaponTypeSniper", "WeaponTypeAssaultRifle"}),
]
SUB_KW = [
    ("energy", {"WeaponTypeEnergy", "WeaponTypeLaser", "WeaponTypePlasma"}),
    ("ballistic", {"WeaponTypeBallistic"}),
    ("automatic", {"WeaponTypeAutomatic"}),
    ("pipe", {"WeaponTypePipe"}),
    ("noPA", {"WeaponNotAllowedInPA"}),
    ("silenced", {"HasSilencer"}),
    ("blade", {"WeaponTypeBladed"}),
    ("blunt", {"WeaponTypeBlunt"}),
]
SKIP_AP = re.compile(r"appearance|paint|skin|material|legendary|color|colour|decal|camo|_misc_|flag|charm|ap_atx|lights?$|ap_weapon_Fx", re.I)
SKIP_MOD = re.compile(r"^(zzz|cr|del_|debug|bounty_|atx_|test)|_npc|unobtainable|paint|skin", re.I)
GENERIC_MA = re.compile(r"^ma_(CanHave|legendarycrafting|Misc_Legendarycrafting|Gun_Appearance|Melee_Appearance|All_|BallisticGun|Handmade_Nonshotgun)", re.I)

# Mod name words -> tags. Standard FO76 naming is shared across weapons,
# so the name says what the mod is for even when the record carries the stat
# in a keyword rather than a property row.
MOD_WORDS = [
    (r"\bpowerful\b", ["dmg3"]), (r"\bhardened\b", ["dmg2"]), (r"\bsevere\b", ["dmg2", "crit"]),
    (r"\bcalibrated\b", ["crit"]), (r"\bvicious\b", ["crit", "hip"]), (r"\bfierce\b", ["crit", "firerate"]),
    (r"\btuned\b", ["dmg1", "firerate"]), (r"hair trigger|\bspeedy\b", ["firerate"]),
    (r"\bprime\b", ["dmg3", "ultracite"]), (r"armor[- ]piercing|\bpiercing\b", ["armorpen"]),
    (r"automatic", ["auto"]), (r"\b(focused|true|refined|aligned|precise)\b", ["hip"]),
    (r"\b(stabilized|forceful|ported|vented)\b", ["recoil"]), (r"long barrel", ["range"]),
    (r"suppressor|silencer", ["quiet"]), (r"compensator", ["recoil", "hip"]), (r"muzzle brake", ["recoil"]),
    (r"(large|drum|extra large|big) mag", ["capacity"]), (r"(quick|swift) mag", ["reload"]),
    (r"reflex", ["reflex"]), (r"recon", ["recon"]), (r"night vision", ["nightvision"]),
    (r"long scope|sniper", ["longscope"]), (r"short scope", ["shortscope"]), (r"bayonet", ["bayonet"]),
    (r"(heated|incendiary|flaming|searing|hot)\b", ["elemental", "fire"]), (r"electrif|shock|tesla", ["elemental"]),
    (r"poison|toxic", ["elemental"]), (r"cryo|frozen|freezing", ["elemental", "cryo"]),
    (r"(spiked|bladed|barbed|serrated|puncturing|sharp|razor|jagged|studded|nailed|bleed)", ["dmg2", "bleed"]),
    (r"\bheavy\b|\bweighted\b|rocket|hot rod|reinforced", ["dmg2"]),

    (r"pistol grip|\bgrip\b", ["grip"]), (r"\bstock\b", ["stock"]),
]


def mod_tags(name, edid, props):
    t = []
    text = (name or "") + " " + re.sub(r"[_-]", " ", edid or "")
    for rx, tg in MOD_WORDS:
        if re.search(rx, text, re.I):
            add_tags(t, *tg)
    if re.search(r"accelerated|tri-barrel|tri barrel|triple barrel", name or "", re.I):
        add_tags(t, "firerate")
    if re.search(r"\bstandard\b", name or "", re.I):
        add_tags(t, "standard")
    for tok in re.split(r"[_\-]", edid or ""):
        tl = tok.lower()
        if tl == "damage":
            add_tags(t, "dmg2")
        elif tl == "critdmg":
            add_tags(t, "crit")
        elif tl == "hipaccuracy":
            add_tags(t, "hip")
        elif tl in ("auto", "automatic"):
            add_tags(t, "auto")
        elif tl == "fasttrigger":
            add_tags(t, "firerate")
        elif tl == "armorpen":
            add_tags(t, "armorpen")
        elif tl == "recoil":
            add_tags(t, "recoil")
        elif tl == "reload":
            add_tags(t, "reload")
        elif tl == "ammo":
            add_tags(t, "capacity")
    if props.get("quiet"):
        add_tags(t, "quiet")
    if props.get("auto"):
        add_tags(t, "auto")
    if (props.get("dmgMul") or 0) > 0:
        add_tags(t, "dmg2" if props["dmgMul"] < 0.3 else "dmg3")
    if props.get("elemental"):
        add_tags(t, "elemental")
    if props.get("addsKw"):
        for k in props["addsKw"]:
            for cls, kws in CLASS_KW:
                if k in kws:
                    add_tags(t, "makes_" + cls)
    return t


def ap_label(ap_edid, ap_name):
    name = q(ap_name)
    if name and name.lower() not in ("no muzzle", ""):
        return name
    e = re.sub(r"^ap_(gun_|melee_|weapon_)?", "", ap_edid or "", flags=re.I)
    e = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", e).replace("_", " ").strip()
    m = {"Mag": "Magazine", "Scope": "Sights", "Grip": "Grip / Stock", "Muzzle": "Muzzle",
         "Receiver": "Receiver", "Barrel": "Barrel"}
    return m.get(e, e.title() or "Mod")


def build_weapons(tpl):
    names_map = tpl.get("weaponNames", {})
    hide = set(tpl.get("hideWeapons", []))
    base = read_tsv(tsv_source.newest("WEAP_Export_*_Base.tsv"))
    dnam = {q(r["WEAP_FormID"]): r for r in read_tsv(tsv_source.newest("WEAP_Export_*_DNAM.tsv"))}
    omod_path = tsv_source.newest("OMOD_Export_*.tsv", exclude="Properties")
    omods = read_tsv(omod_path)
    props_rows = read_tsv(tsv_source.newest("OMOD_Export_*_Properties.tsv"))

    # mod properties we care about
    props = {}
    for r in props_rows:
        e = q(r.get("OMOD_EDID"))
        p = props.setdefault(e, {})
        pn = q(r.get("PropertyName"))
        v1 = q(r.get("Value1"))
        if pn == "SoundLevel" and v1.lower() in ("quiet", "silent"):
            p["quiet"] = True
        elif pn == "IsAutomatic" and v1.lower() in ("true", "1", "1.000000"):
            p["auto"] = True
        elif pn == "AttackDamage":
            p["dmgMul"] = (p.get("dmgMul") or 0) + (num(v1, 0) or 0)
        elif pn in ("DamageTypeValues", "Damage Type Value"):
            p["elemental"] = True
        elif pn == "Keywords" and q(r.get("FunctionType")) == "ADD":
            k = v1.split(" ")[0]
            if k.startswith("WeaponType"):
                p.setdefault("addsKw", []).append(k)
                if k == "WeaponTypeAutomatic":
                    p["auto"] = True

    # mods by target keyword
    mods_by_kw = {}
    for r in omods:
        e = q(r.get("OMOD_EDID"))
        full = q(r.get("FULL"))
        if not full or SKIP_MOD.search(e) or SKIP_MOD.search(full):
            continue
        ap_e = q(r.get("AttachPoint_EDID"))
        if not ap_e or SKIP_AP.search(ap_e):
            continue
        if not q(r.get("LNAM_LooseMod")):
            continue  # no craftable mod item -> not a player mod
        targets = kw_edids(r.get("MNAM_TargetKWDs"))
        m = {"name": full, "edid": e, "slot": ap_label(ap_e, r.get("AttachPoint_Name")), "ap": ap_e,
             "tags": mod_tags(full, e, props.get(e, {}))}
        for t in targets:
            mods_by_kw.setdefault(t, []).append(m)

    uwa = load_json(DIST / "unique_weapons_armour" / "unique_weapons_armour.json")
    uniques_by_base = {}
    for it in uwa.get("items", []):
        if it.get("kind") == "Weapon" and not it.get("isCut"):
            uniques_by_base.setdefault(it.get("base"), []).append({
                "name": it["name"],
                "effect": it.get("inherentEffect") or "",
                "stars": [s.get("name") for s in it.get("starEffects", []) if s.get("fixed")],
            })

    by_name = {}
    for r in base:
        e = q(r["WEAP_EDID"])
        full = q(r["WEAP_FULL"])
        if not full or NPC_EDID.search(e) or "[" in full:
            continue
        full = names_map.get(full, full)
        if full in hide:
            continue
        kws = kw_edids(r.get("Keywords"))
        if "ma_legendarycrafting_weapon" not in kws:
            continue  # not a player legendary-capable weapon
        classes = [c for c, ks in CLASS_KW if ks & set(kws)]
        subs = [c for c, ks in SUB_KW if ks & set(kws)]
        if not classes and "WeaponTypeRanged" in kws:
            # pistol-or-rifle guns (Pipe, Laser, Plasma...): the grip decides
            classes = ["pistol", "rifle"]
        ma = [k for k in kws if re.search(r"(^|_)ma_", k) and not GENERIC_MA.search(k)]
        mods = []
        seen = set()
        for k in ma:
            for m in mods_by_kw.get(k, []):
                key = (m["slot"], m["name"])
                if key in seen:
                    continue
                seen.add(key)
                mods.append(m)
        d = dnam.get(q(r["WEAP_FormID"]), {})
        ammo = re.sub(r"\s*\[.*$", "", q(d.get("DNAM_Ammo"))).split('"')
        ammo_name = ammo[1] if len(ammo) > 1 else (ammo[0] if ammo[0] and ammo[0] != "NULL - Null Reference" else "")
        w = {
            "id": slug(full),
            "name": full,
            "edid": e,
            "formId": q(r["WEAP_FormID"]),
            "classes": classes or (["melee"] if "melee" in " ".join(kws).lower() else []),
            "subs": subs,
            "ammo": ammo_name,
            "ap": num(d.get("DNAM_ActionPointCost")),
            "capacity": int(num(d.get("DNAM_Capacity"), 0) or 0) or None,
            "weight": num(d.get("DNAM_Weight")),
            "minLevel": int(num(r.get("EILV_Level"), 1) or 1),
            "mods": mods,
        }
        if not w["classes"]:
            continue
        prev = by_name.get(full)
        if prev is None or len(w["mods"]) > len(prev["mods"]):
            by_name[full] = w

    weapons = []
    for w in sorted(by_name.values(), key=lambda x: x["name"]):
        u = uniques_by_base.get(w["name"])
        if u:
            w["uniques"] = u
        # group mods by slot, slot order is the usual workbench order
        order = ["Receiver", "Barrel", "Grip / Stock", "Magazine", "Sights", "Muzzle"]
        slots = {}
        for m in w["mods"]:
            slots.setdefault(m["slot"], []).append({"name": m["name"], "tags": m["tags"]})
        w["mods"] = [{"slot": s, "options": slots[s]} for s in sorted(slots, key=lambda s: (order.index(s) if s in order else 99, s))]
        weapons.append(w)
    return weapons


# ---------------------------------------------------------------- legendary effects
def build_legendary_effects(tpl):
    lm = load_json(DIST / "legendary_mods.json")
    ov = tpl.get("effectTagOverrides", {})
    out, uniques = [], []
    for g in lm["groups"]:
        for m in g.get("mods", []):
            if g["id"] == "unique":
                uniques.append({"name": m["name"], "effect": m.get("effect", "")})
                continue
            types = m.get("types", [])
            tags = tag_text(m["name"] + " " + (m.get("effect") or ""))
            tags = apply_override(tags, ov.get(m["name"]))
            e = {
                "name": m["name"],
                "star": m.get("star") or g.get("star"),
                "weapon": "Weapon" in types,
                "armour": "Armour" in types,
                "pa": "Power Armour" in types,
                "compat": m.get("weaponCompat"),
                "effect": re.sub(r"\s*\(new\)\s*$", "", m.get("effect") or ""),
                "tags": tags,
            }
            rec_fx = legendary_mod_effects(REC, m.get("edid", "")) if REC else []
            if rec_fx:
                e["records"] = rec_fx
            out.append(e)
    return out, uniques


# ---------------------------------------------------------------- armour
def build_armour(tpl):
    a = load_json(DIST / "armour.json")
    notes = tpl.get("armourSets", {})
    sets = []
    for key, kind in (("power-armour", "pa"), ("body-armour", "armour")):
        for s in a["pages"].get(key, {}).get("sets", []):
            n = s["setName"]
            meta = notes.get(n, {})
            sets.append({
                "name": n if kind == "armour" or "Armor" in n else n + " Power Armor",
                "short": n,
                "kind": kind,
                "tags": meta.get("tags", []),
                "note": meta.get("note", ""),
                "rank": meta.get("rank", 0),
            })
    return sets


# ---------------------------------------------------------------- mutations + buffs
GROUP_TAGS = {
    "all-damage": ["damage"], "critical-damage": ["crit", "damage"], "melee-damage": ["melee", "damage"],
    "fortify-melee-weapon-damage": ["melee", "damage"], "twisted-muscles": ["melee", "damage"],
    "unarmed-damage": ["unarmed", "damage"], "fortify-fists-knife-damage": ["unarmed", "damage"],
    "ballistic-damage": ["ballistic", "damage"], "energy-damage": ["energy", "damage"],
    "fortify-heavy-gun-damage": ["heavy", "damage"], "explosive-dmg": ["explosive", "damage"],
    "fortify-explosion-radius": ["explosive"], "fortify-throwing-weapon-damage": ["thrown", "damage"],
    "fortify-scoped-gun-damage": ["scoped", "damage"], "sneak-attack-damage": ["sneak", "damage"],
    "fortify-sneak": ["sneak"], "stealth": ["sneak"], "harder-to-detect": ["sneak"], "fortify-sneak-light": ["sneak"],
    "fortify-sneak-audio": ["sneak"], "action-points": ["ap"], "action-point-regen": ["ap"], "sprint-ap-cost": ["ap"],
    "carry-weight": ["carry"], "damage-resistance": ["defense"], "energy-resistance": ["defense"],
    "fortify-damage-resist": ["defense"], "fortify-energy-resist": ["defense"], "limb-damage-reduction": ["defense"],
    "health-regen": ["heal"], "healing-rate": ["heal"], "stimpak-healing": ["heal"],
    "xp-bonus": ["xp"], "bottlecap-collector": ["farm"], "increased-food-from-plants": ["farm"],
    "search-meat-from-animals": ["farm"], "fortify-lockpicking": ["lockpick"], "accuracy": ["accuracy"],
    "fortify-reload-speed": ["reload"], "fortify-move-speed": ["speed"], "kill-streak": ["killstreak", "damage"],
    "strength": ["stat_S"], "perception": ["stat_P"], "endurance": ["stat_E"], "charisma": ["stat_C"],
    "intelligence": ["stat_I"], "agility": ["stat_A"], "luck": ["stat_L"], "fortify-agility": ["stat_A"],
    "fortify-perception": ["stat_P"], "fortify-intelligence": ["stat_I"], "fortify-charisma": ["stat_C"],
    "magazine-fortify-crit-damage-all": ["crit", "damage"], "fortify-damage-vs-players": [],
}
BUFF_FILES = [("food", "Food"), ("chems", "Chem"), ("alcohol", "Alcohol"), ("nuka-cola", "Nuka-Cola"),
              ("magazines", "Magazine"), ("bobbleheads", "Bobblehead")]


def build_mutations(tpl):
    m = load_json(DIST / "buffs" / "mutations.json")
    meta = tpl.get("mutations", {})
    out = []
    for it in m["items"]:
        name = re.sub(r"\s*Mutation$", "", it["name"]).strip()
        good = [e["display"] for e in it["effects"] if e.get("display") and e.get("polarity") == "buff"
                and not re.search(r"text$", e.get("full", ""), re.I)]
        bad = [e["display"] for e in it["effects"] if e.get("display") and e.get("polarity") != "buff"]
        mm = meta.get(name, {})
        # race / team / serum / Class Freak versions, matched to the display text
        # by magic effect + magnitude
        variants = []
        if REC:
            disp = {}
            for e in it["effects"]:
                disp.setdefault((str(e.get("formId", "")).upper(), round(float(e.get("magnitude") or 0), 3)), e.get("display"))
            recs = REC.mutation_effects(it.get("edid") or "")
            team_mag = {}
            for v in recs:
                if v.get("version") == "team":
                    team_mag[v["mgef"]] = v["magnitude"]
            seen = set()
            for v in recs:
                if v.get("version") == "team" or v.get("serumOnly"):
                    continue
                d = disp.get((v["mgef"], round(float(v["magnitude"] or 0), 3)))
                if not d:
                    continue
                # the Buffs data calls the bigger number "with a serum"; the records
                # say it's the team version (Mutation_Check_UseSuperVersion: in a
                # team + Strange In Numbers + a mutated teammate)
                d = re.sub(r"\s*\([^)]*(?:with a serum|in a team with Strange In Numbers|at most)\)", "", d).strip()
                key = (v["mgef"], v.get("race"), d)
                if key in seen:
                    continue
                seen.add(key)
                vv = {"text": d}
                if v.get("race"):
                    vv["race"] = v["race"]
                if v.get("serumRemoves"):
                    vv["downside"] = True
                if v.get("classFreak"):
                    vv["classFreak"] = True
                if v.get("when"):
                    vv["when"] = v["when"]
                tm = team_mag.get(v["mgef"])
                if tm and v.get("magnitude") and tm != v["magnitude"]:
                    vv["team"] = round(tm, 2)
                    vv["normal"] = round(v["magnitude"], 2)
                variants.append(vv)
        out.append({
            "name": name,
            "good": good,
            "bad": bad,
            "tags": mm.get("tags") or tag_text(" ".join(good)),
            "badTags": mm.get("badTags", []),
            "excludes": mm.get("excludes", []),
            "variants": variants,
            "image": it.get("imageUrl"),
        })
    return out


def build_consumables(tpl):
    out = []
    for fname, kind in BUFF_FILES:
        p = DIST / "buffs" / f"{fname}.json"
        if not p.exists():
            continue
        d = load_json(p)
        for it in d["items"]:
            effects = [e for e in it.get("effects", []) if e.get("polarity") == "buff" and e.get("groupKey")]
            tags = []
            for e in effects:
                add_tags(tags, *GROUP_TAGS.get(e["groupKey"], []))
            if not tags:
                continue
            k = kind
            if fname == "food":
                k = "Drink" if (it.get("objectType") or "") == "Drink" else "Food"
            nm = it.get("gameName") or it["name"]
            if any(o["name"] == nm for o in out):
                continue
            out.append({
                "name": nm,
                "kind": k,
                "diet": it.get("diet"),
                "effects": [e["display"] for e in effects if e.get("display")][:4],
                "groups": sorted({e["groupKey"] for e in effects}),
                "tags": tags,
                "image": it.get("imageUrl"),
            })
    return out


# ---------------------------------------------------------------- templates
def validate_templates(tpl, data):
    names = {p["name"] for p in data["perks"]} | {p["name"] for p in data["legendaryPerks"]}
    weapons = {w["name"] for w in data["weapons"]}
    effects = {e["name"] for e in data["legendaryEffects"]}
    sets = {s["short"] for s in data["armourSets"]} | {s["name"] for s in data["armourSets"]}
    muts = {m["name"] for m in data["mutations"]}
    problems = []
    for t in tpl.get("templates", []):
        tid = t.get("id")
        for c in t.get("coreCards", []):
            if c not in names:
                problems.append(f"{tid}: core card not found: {c}")
        for c in t.get("avoidCards", []):
            if c not in names:
                problems.append(f"{tid}: avoid card not found: {c}")
        for w in t.get("weapons", []):
            if w not in weapons:
                problems.append(f"{tid}: weapon not found: {w}")
        for where in ("weaponEffects", "armourEffects"):
            for star, lst in (t.get(where) or {}).items():
                for e in lst:
                    if e not in effects:
                        problems.append(f"{tid}: {where} {star}* effect not found: {e}")
        for s in t.get("armourSets", []) + t.get("paSets", []):
            if s not in sets:
                problems.append(f"{tid}: armour set not found: {s}")
        for m in t.get("mutations", []):
            if m not in muts:
                problems.append(f"{tid}: mutation not found: {m}")
    for n in tpl.get("perkTagOverrides", {}):
        if n not in names:
            problems.append(f"perkTagOverrides: card not found: {n}")
    return problems


REC = None


def main():
    global REC
    tpl = load_json(TEMPLATES)
    REC = Records()
    game_rules = REC.game_rules()
    rules = dict(tpl.get("rules", {}))
    for k, v in game_rules.items():
        rules[k] = v   # game records win over anything typed in the templates file
    perks, legendary, skipped = build_perks(tpl)
    weapons = build_weapons(tpl)
    effects, unique_mods = build_legendary_effects(tpl)
    data = {
        "meta": {
            "generated": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "sources": tsv_source.observed() if hasattr(tsv_source, "observed") else "",
            "counts": {},
        },
        "rules": rules,
        "specials": SPECIALS,
        "perks": perks,
        "legendaryPerks": legendary,
        "weapons": weapons,
        "legendaryEffects": effects,
        "uniqueMods": unique_mods,
        "armourSets": build_armour(tpl),
        "mutations": build_mutations(tpl),
        "consumables": build_consumables(tpl),
    }
    data["meta"]["counts"] = {k: len(v) for k, v in data.items() if isinstance(v, list)}
    problems = validate_templates(tpl, data)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_DIR / "builds_data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    out_tpl = {
        "generated": data["meta"]["generated"],
        "templates": tpl.get("templates", []),
        "problems": problems,
    }
    with open(OUT_DIR / "build_templates.json", "w", encoding="utf-8") as f:
        json.dump(out_tpl, f, ensure_ascii=False, indent=1)
    print("builds_data.json:", data["meta"]["counts"])
    print("rules from game records:", {k: v for k, v in game_rules.items() if k != "_sources"})
    if skipped:
        print("skipped perk cards:", ", ".join(skipped))
    if problems:
        print("\nTEMPLATE PROBLEMS (fix data/builds/build_templates.json):")
        for p in problems:
            print("  -", p)
    else:
        print("templates OK:", len(out_tpl["templates"]))


if __name__ == "__main__":
    main()
