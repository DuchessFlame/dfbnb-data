#!/usr/bin/env python3
r"""
spawns_configs.chainsaws — the CHAINSAW family driver (BNB brand, /bnb/weapons/chainsaws/).

The first WEAPON family on the shared spawns engine. Everything structural is the
existing pipeline (spawn-guide skill §9k): one LVLI up-closure per item, placements
resolved through Mappalachia, sources routed into the standard root expands. The only
weapon-specific work lives in this file:

  * Used For is replaced by a **Weapon Stats** expand   — WEAP Base + DNAM, with the
    real level-scaled base damage read from the weapon's CVT0 damage CURV (a FO76 melee
    weapon stores DNAM_BaseDamage = 0; all of its damage comes from the curve).
  * A **Mods & Plans** expand sits directly under it    — every OMOD that targets one of
    the weapon's own mod archetypes (`ma_*`), grouped by attach point, with each mod's
    actual property deltas (damage / speed / AP / weight / durability / value), the
    effect it adds, the plan that teaches it and the loose mod it drops as.
  * Farming Tips is NOT emitted (a chainsaw is not a consumable) — the renderer skips it.

PAGES
    /bnb/weapons/chainsaws/                              <- HUB  (dist/chainsaws.json)
    /bnb/weapons/chainsaws/chainsaw-location-guide/      <- PAGE (dist/chainsaws/chainsaw.json)

SEEDS — the ONE curated value is the weapon's WEAP FormID in `CHAINSAWS` below.
Mods, plans, unique variants, stats, spawn points, container types, event pools and
vendors are all resolved from the committed exports. No hardcoded mod or LVLI FormIDs.

UNIQUE VARIANTS (e.g. the Cauterizer) are derived, not typed: every COBJ whose CNAM is
this weapon and whose GNAM resolves to a BOOK plan names a craftable variant; the
matching `*ma_<Variant>` keyword is then picked up as an extra mod archetype, so the
variant's own paints/mods list alongside the base weapon's.

FIXED SPAWNS need a LOCAL Mappalachia DB pass. Run once with MAPPALACHIA_DB set to seed
data/chainsaw_spawns/geo_cache.json, commit the cache, and every later CI run rebuilds
the same page from the cache + TSVs with no DB.

Usage:
    python src/build_spawns.py chainsaws [--pts] [slug ...]
    python src/build_chainsaw_spawns_json.py [--pts]

    # Seeding the coordinates (two steps, on purpose — see seed_geo_cache):
    #   1. with the DB, cheap:   python src/build_chainsaw_spawns_json.py --geo-only
    #   2. without it, normal:   python src/build_chainsaw_spawns_json.py --pts
"""

import os, re, csv, sys, json, sqlite3, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from spawns_engine.geo import Geo
from spawns_engine import sources as esources
from spawns_engine import build as ebuild
from spawns_engine import events as eevents
from spawns_engine.classify import weapon_classify
from nuka_cola_spawns_config import ALL_REGIONS
import tsv_source

# ── family constants ─────────────────────────────────────────────────────────
FAMILY = "chainsaws"
URL_BASE = "/bnb/weapons/chainsaws/"
URL_OF = lambda slug: f"{URL_BASE}{slug}-location-guide/"
SOURCE_TAG = ("Game-file exports (WEAP/OMOD/COBJ/BOOK/CURV/LVLI/CONT) + Mappalachia "
              "Position (cached for CI)")

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
TSV = esources.TSV

# The roster. `weap` is the ONE seed per page — everything else is resolved.
CHAINSAWS = [
    {
        "slug": "chainsaw",
        "name": "Chainsaw",
        "weap": "0004DF01",
        "page_title": "Weapons - Chainsaw Location Guide",
        "blurb": ("Every place the Chainsaw spawns in Fallout 76 — its fixed world "
                  "spawn points, the containers and reward pools that can roll it, "
                  "its full stat line and every mod, plan and paint it takes."),
    },
]

# Cosmetic attach points — mods here change how the weapon LOOKS, not how it plays.
# Matched as a substring so every appearance / model-swap / custom-name point is caught
# (ap_gun_Appearance, ap_melee_Appearance, ap_customName …) without listing each one.
COSMETIC_AP_RE = re.compile(r"(appearance|customname|modelswap|paint|skin)", re.I)

# A mod that IS the empty / factory state of its slot: no effects, and an EDID that says
# so. Flagged rather than hidden — "Standard Bar" is the baseline players compare against.
DEFAULT_MOD_RE = re.compile(r"(nomod|_base$|_default$|standard)", re.I)

# Dev / cut-content OMODs and COBJs that place nothing live.
DEV_EDID_RE = re.compile(r"(^zzz|^test|_test|debug|babylon|nonplayable)", re.I)


# ── file helpers ─────────────────────────────────────────────────────────────
def _newest(pattern, exclude=None, required=False):
    return tsv_source.newest(os.path.join(TSV, pattern), exclude=exclude, required=required)


def _rows(path):
    if not path:
        return []
    with open(path, encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _num(s):
    try:
        return float(str(s).strip())
    except Exception:
        return None


def _clean(s):
    """Collapse the newlines xEdit leaves inside multi-value cells."""
    return re.sub(r"\s+", " ", (s or "").replace("\n", " ")).strip()


_REF_RE = re.compile(r'^\s*([^"\[]+?)\s*(?:""(.*?)""|"(.*?)")?\s*(?:\[(\w+):([0-9A-Fa-f]{8})\])?\s*$')


def _parse_ref(token):
    """'EnchWeapModBleed_Chainsaw ""Bleed"" [ENCH:00822A84]' ->
       {'edid': 'EnchWeapModBleed_Chainsaw', 'name': 'Bleed', 'sig': 'ENCH', 'form_id': '00822A84'}"""
    m = _REF_RE.match(_clean(token))
    if not m:
        return {"edid": _clean(token), "name": "", "sig": "", "form_id": ""}
    edid, n1, n2, sig, fid = m.groups()
    return {"edid": (edid or "").strip(), "name": (n1 or n2 or "").strip(),
            "sig": sig or "", "form_id": (fid or "").upper()}


def _split_multi(cell):
    return [p for p in re.split(r"\s*\|\s*", _clean(cell)) if p]


def _prettify(edid):
    """A readable label for a record with no FULL.
    'ench_ChainsawFire'        -> 'Chainsaw Fire'
    'enchModArmorPenetration'  -> 'Armor Penetration'
    'ap_gun_Barrel'            -> 'Barrel'
    Leading plumbing tokens (ench / mod / ap / dn / ma / omod) are stripped whether they
    are underscore-separated or camelCased, so an internal EDID never leaks 'Ench Mod'
    into a player-facing label."""
    s = (edid or "").replace("_", " ")
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)          # de-camel first
    s = re.sub(r"^(?:(?:ench|mod|omod|dn|ma|ap|gun)\s+)+", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:1].upper() + s[1:] if s else (edid or "")


def _workbench_label(bnam_full, bnam_edid):
    """'Workbench_Crafting_Weapon' -> 'Weapon Workbench'. Prefers the record's own FULL
    when the export carries one."""
    full = _clean(bnam_full)
    if full:
        return full
    e = _clean(bnam_edid)
    if not e:
        return ""
    m = re.match(r"(?i)^workbench[_ ]crafting[_ ](.+)$", e)
    if m:
        return f"{_prettify(m.group(1))} Workbench"
    return _prettify(e)


# ── WEAP: base record + DNAM stat line ───────────────────────────────────────
def load_weapon(formid):
    """The weapon's Base row (keywords, level, equip type, mod slots) + DNAM row."""
    fid = formid.upper()
    base = next((r for r in _rows(_newest("WEAP_Export_*_Base.tsv"))
                 if (r.get("WEAP_FormID") or "").upper() == fid), None)
    dnam = next((r for r in _rows(_newest("WEAP_Export_*_DNAM.tsv"))
                 if (r.get("WEAP_FormID") or "").upper() == fid), None)
    if not base:
        raise LookupError(f"WEAP {fid} not found in the committed WEAP_Export_*_Base.tsv")
    return base, (dnam or {})


# ── CURV: the real base damage (a melee WEAP stores DNAM_BaseDamage = 0) ─────
_CURVE_CACHE = {}


def load_curve(curve_id):
    """{'edid', 'points': [{x, y}], 'display': [{x, y}]} for a CURV, from the committed
    dist/curves chunks (the same data the Curve Tables pages render). Returns None when
    the curve isn't in the export — the page then shows the curve name with no table
    rather than inventing numbers."""
    cid = (curve_id or "").upper()
    if not cid:
        return None
    if cid in _CURVE_CACHE:
        return _CURVE_CACHE[cid]
    idx_path = os.path.join(REPO, "dist", "curves", "index.json")
    out = None
    try:
        idx = json.load(open(idx_path, encoding="utf-8"))
        for chunk in [c for group in idx.get("chunks", {}).values() for c in group]:
            data = json.load(open(os.path.join(REPO, "dist", "curves", chunk), encoding="utf-8"))
            curves = data if isinstance(data, list) else data.get("curves", [])
            for c in curves:
                if (c.get("id") or "").upper() == cid:
                    out = c
                    break
            if out:
                break
    except Exception:
        out = None
    _CURVE_CACHE[cid] = out
    return out


# Player levels worth showing on a weapon page: the level gate, the round tens, cap.
_DAMAGE_LEVELS = (1, 10, 20, 30, 40, 50)


def damage_table(curve, level_req):
    """[{level, damage}] at the display levels, read off the curve's displayTable
    (which is the game's own per-level interpolation), never re-derived here."""
    if not curve:
        return []
    table = {int(p["x"]): p["y"] for p in (curve.get("displayTable") or curve.get("points") or [])
             if isinstance(p, dict) and p.get("x") is not None}
    if not table:
        return []
    levels = sorted({l for l in _DAMAGE_LEVELS if l in table} | ({level_req} if level_req in table else set()))
    return [{"level": l, "damage": round(float(table[l]), 2)} for l in levels]


# ── the Weapon Stats block ───────────────────────────────────────────────────
_STAT_FIELDS = [
    ("DNAM_WeaponType",         "Weapon type",        None),
    ("DNAM_Value",              "Value (caps)",       None),
    ("DNAM_Weight",             "Weight",             None),
    ("DNAM_Speed",              "Speed multiplier",   None),
    ("DNAM_AnimAttackSeconds",  "Attack animation",   "s"),
    ("DNAM_AttackDelaySeconds", "Attack delay",       "s"),
    ("DNAM_ActionPointCost",    "AP cost per swing",  None),
    ("DNAM_Reach",              "Reach",              None),
    ("DNAM_Health",             "Durability (health)", None),
    ("DNAM_Stagger",            "Stagger",            None),
    ("CRDT_CritDamageMult",     "Critical damage",    "×"),
    ("WSAM_SneakAttackMult",    "Sneak attack",       "×"),
    ("DNAM_OnHit",              "On hit",             None),
]


def _fmt_stat(raw, suffix):
    v = _clean(raw)
    if not v or v.upper().startswith("NULL"):
        return None
    n = _num(v)
    if n is not None:
        v = f"{n:g}"
    if suffix == "×":
        return f"{v}×"
    return f"{v}{suffix}" if suffix else v


def build_weapon_stats(base, dnam):
    """The Weapon Stats expand — WEAP/DNAM facts plus the damage curve, nothing invented."""
    level_req = int(_num(base.get("EILV_Level")) or 0)
    curve_ref = _parse_ref(dnam.get("CVT0_DamageCurve", ""))
    curve = load_curve(curve_ref.get("form_id"))
    stats = []
    for col, label, suffix in _STAT_FIELDS:
        val = _fmt_stat(dnam.get(col), suffix)
        if val:
            stats.append({"label": label, "value": val})

    kwds = [_parse_ref(k) for k in _split_multi(base.get("Keywords", ""))]
    slots = [_parse_ref(k) for k in _split_multi(base.get("APPR_Slots", ""))]
    dmg = damage_table(curve, level_req)
    return {
        "form_id": (base.get("WEAP_FormID") or "").upper(),
        "edid": base.get("WEAP_EDID", ""),
        "name": base.get("WEAP_FULL", ""),
        "level_requirement": level_req or None,
        "equip_type": _parse_ref(base.get("ETYP_EquipType", "")).get("edid", ""),
        "base_damage": {
            "curve": curve_ref.get("edid", ""),
            "curve_form_id": curve_ref.get("form_id", ""),
            "note": ("Base damage is not stored on the weapon — it scales with your "
                     "character level off the curve below."),
            "by_level": dmg,
            "min": dmg[0]["damage"] if dmg else None,
            "max": dmg[-1]["damage"] if dmg else None,
        },
        "stats": stats,
        "mod_slots": [{"edid": s["edid"],
                       "name": _slot_label(s["edid"], s["name"] if s["name"] != "No Appearance" else "")}
                      for s in slots if s.get("edid")],
        "keywords": [k["edid"] for k in kwds if k.get("edid")],
    }


# ── mod archetypes: which OMODs belong to THIS weapon ────────────────────────
def _weapon_stem(edid):
    """'Chainsaw_76' -> 'chainsaw' — the token a weapon's own ma_ keywords carry."""
    return re.sub(r"[^a-z]", "", re.split(r"_\d", (edid or "").lower())[0])


def unique_variants(weap_fid, cobj_rows, book_names):
    """Craftable unique variants of this weapon, derived from COBJ: any recipe whose
    CNAM is this weapon and whose GNAM resolves to a BOOK plan. Returns
    [{'name', 'plan', 'plan_form_id', 'cobj_edid'}] — e.g. the raid Cauterizer."""
    out = []
    for r in cobj_rows:
        if (r.get("CNAM_FormID") or "").upper() != weap_fid:
            continue
        if DEV_EDID_RE.search(r.get("COBJ_EDID", "")):
            continue
        gnam_fid = (r.get("GNAM_FormID") or "").upper()
        plan = book_names.get(gnam_fid)
        if not plan:
            continue
        # 'Plan: Cauterizer' -> 'Cauterizer'
        name = re.sub(r"^(Plan|Recipe)\s*:\s*", "", plan).strip()
        out.append({"name": name, "plan": plan, "plan_form_id": gnam_fid,
                    "cobj_edid": r.get("COBJ_EDID", "")})
    return out


def mod_archetypes(base, variants):
    """The `ma_*` keywords that mean 'a mod for THIS weapon'.

    Two sources, both derived: (1) the weapon's own ma_ keywords whose name carries the
    weapon's EDID stem — this picks ma_Chainsaw while leaving the shared
    ma_legendarycrafting_* / ma_Gun_Appearance archetypes (which every gun has) out;
    (2) any ma_ keyword naming a unique variant found on the COBJ pass, so the
    Cauterizer's own paints are attributed to this weapon too."""
    stem = _weapon_stem(base.get("WEAP_EDID", ""))
    arche = set()
    for k in _split_multi(base.get("Keywords", "")):
        ref = _parse_ref(k)
        e = (ref.get("edid") or "")
        if e.lower().startswith("ma_") and stem and stem in re.sub(r"[^a-z]", "", e.lower()):
            arche.add(e)
    variant_tokens = {re.sub(r"[^a-z]", "", v["name"].lower()) for v in variants if v.get("name")}
    return arche, variant_tokens


# ── OMOD properties -> readable effect lines ─────────────────────────────────
_PCT_PROPS = {
    "AttackDamage": "Damage",
    "Value": "Value",
    "Weight": "Weight",
    "AttackActionPointCost": "AP cost",
    "Durability": "Durability",
    "Speed": "Speed",
    "Range": "Range",
    "AttackDelaySeconds": "Attack delay",
    "Reach": "Reach",
    "CritDamageMult": "Critical damage",
}
# Properties that carry no player-facing meaning on their own.
_SKIP_PROPS = {"Keywords", "MaterialSwaps", "ModelSwaps", "InstanceNaming", "SoundLevel"}


def _fmt_pct(v):
    n = _num(v)
    if n is None or n == 0:
        return None
    return f"{n * 100:+.0f}%".replace("+-", "-")


def _fmt_flat(v):
    n = _num(v)
    if n is None or n == 0:
        return None
    return f"{n:+g}"


def mod_effects(props):
    """[{label, value, kind}] for one OMOD's property rows. MUL+ADD rows read as a
    percentage (Value1 is the multiplier delta); plain ADD rows read as a flat number."""
    out = []
    for p in props:
        name = _clean(p.get("PropertyName"))
        if not name or name in _SKIP_PROPS:
            continue
        fn = _clean(p.get("FunctionType")).upper()
        vt = _clean(p.get("ValueType"))
        v1, v2 = p.get("Value1"), p.get("Value2")

        if name == "Enchantments":
            ref = _parse_ref(v1)
            label = ref.get("name") or _prettify(ref.get("edid"))
            out.append({"label": "Adds effect", "value": label, "kind": "effect"})
            continue
        if name == "ActorValues":
            ref = _parse_ref(v1)
            amount = _fmt_flat(v2)
            out.append({"label": ref.get("name") or _prettify(ref.get("edid")),
                        "value": amount or "", "kind": "stat"})
            continue
        if name == "DamageBonusMult":
            val = _fmt_pct(v1)
            if val:
                out.append({"label": "Damage bonus", "value": val, "kind": "stat"})
            continue

        label = _PCT_PROPS.get(name, _prettify(name))
        if "MUL" in fn:
            val = _fmt_pct(v1)
            # a MUL+ADD row can also carry a flat Value2 term (Long Bow Bar: -10% and -0.1)
            flat = _fmt_flat(v2)
            if val:
                out.append({"label": label, "value": val, "kind": "stat"})
            elif flat:
                out.append({"label": label, "value": flat, "kind": "stat"})
        elif "Float" in vt:
            val = _fmt_flat(v1)
            if val:
                out.append({"label": label, "value": val, "kind": "stat"})
        else:
            val = _fmt_flat(v1)
            if val:
                out.append({"label": label, "value": val, "kind": "stat"})

    # An enchantment that only applies an actor value shows up twice (Bow Bar:
    # "Adds effect: Armor Penetration" + "Armor Penetration +15"). Keep the numbered
    # stat row and drop the naked restatement.
    stat_labels = {e["label"].lower() for e in out if e["kind"] == "stat"}
    out = [e for e in out
           if not (e["kind"] == "effect" and e["value"].lower() in stat_labels)]
    # Damage first, then the rest in the order the record lists them.
    out.sort(key=lambda e: 0 if e["label"].lower().startswith("damage") else 1)
    return out


def _slot_label(ap_edid, ap_name):
    if ap_name:
        return ap_name
    s = re.sub(r"^ap_(gun_)?", "", ap_edid or "")
    return _prettify(s) or "Other"


def build_mods(base, weap_fid):
    """The Mods & Plans expand: every OMOD targeting one of this weapon's archetypes,
    grouped by attach-point slot, each with its property deltas, the plan that teaches
    it, the workbench it is crafted at and the loose mod it drops as."""
    omod_rows = _rows(_newest("OMOD_Export_*.tsv", exclude="_Properties"))
    prop_rows = _rows(_newest("OMOD_Export_*_Properties.tsv"))
    cobj_rows = _rows(_newest("COBJ_Export_*.tsv"))
    book_names = {(r.get("FormID") or r.get("BOOK_FormID") or "").upper():
                  _clean(r.get("FULL") or r.get("BOOK_FULL"))
                  for r in _rows(_newest("BOOK_Export_*.tsv", exclude="_Locations"))}
    book_names = {k: v for k, v in book_names.items() if k and v}

    variants = unique_variants(weap_fid, cobj_rows, book_names)
    arche, variant_tokens = mod_archetypes(base, variants)

    # OMOD FormID -> its crafting recipe (plan name + workbench)
    by_omod = {}
    for r in cobj_rows:
        cn = (r.get("CNAM_FormID") or "").upper()
        if not cn or DEV_EDID_RE.search(r.get("COBJ_EDID", "")):
            continue
        gnam_fid = (r.get("GNAM_FormID") or "").upper()
        plan = book_names.get(gnam_fid) or ""
        by_omod.setdefault(cn, {
            "plan": plan,
            "plan_form_id": gnam_fid if plan else "",
            "workbench": _workbench_label(r.get("BNAM_FULL"), r.get("BNAM_EDID")),
            "cobj_edid": r.get("COBJ_EDID", ""),
        })

    props_by_omod = {}
    for p in prop_rows:
        props_by_omod.setdefault((p.get("OMOD_FormID") or "").upper(), []).append(p)

    groups, seen = {}, set()
    for r in omod_rows:
        fid = (r.get("OMOD_FormID") or "").upper()
        edid = r.get("OMOD_EDID", "")
        if not fid or fid in seen or DEV_EDID_RE.search(edid):
            continue
        targets = [_parse_ref(t).get("edid", "") for t in _split_multi(r.get("MNAM_TargetKWDs", ""))]
        tset = {t for t in targets if t}
        variant = next((v["name"] for v in variants
                        if any(re.sub(r"[^a-z]", "", t.lower()).endswith(
                            re.sub(r"[^a-z]", "", v["name"].lower())) for t in tset)), None)
        if not (tset & arche) and not variant:
            continue
        seen.add(fid)

        ap_edid = r.get("AttachPoint_EDID", "")
        ap_name = _clean(r.get("AttachPoint_Name"))
        cosmetic = bool(COSMETIC_AP_RE.search(ap_edid or "")) or bool(COSMETIC_AP_RE.search(edid))
        recipe = by_omod.get(fid, {})
        loose = _clean(r.get("LNAM_LooseMod"))
        effects = mod_effects(props_by_omod.get(fid, []))
        mod = {
            "form_id": fid,
            "edid": edid,
            "name": _clean(r.get("FULL")) or _prettify(edid),
            "description": _clean(r.get("DESC")),
            "slot": _slot_label(ap_edid, ap_name if ap_name != "No Appearance" else ""),
            "attach_point": ap_edid,
            "cosmetic": cosmetic,
            # The factory state of its slot: no property deltas AND an EDID that says so.
            "default": not effects and bool(DEFAULT_MOD_RE.search(edid)),
            "unique_variant": variant,
            "plan": recipe.get("plan", ""),
            "plan_form_id": recipe.get("plan_form_id", ""),
            "workbench": recipe.get("workbench", ""),
            "loose_mod": re.sub(r"\[[0-9A-Fa-f]{8}\]$", "", loose).strip() if loose else "",
            "effects": effects,
        }
        groups.setdefault(mod["slot"], []).append(mod)

    # Functional slots first (they change how it plays), cosmetics after, each A–Z.
    def _group_key(item):
        slot, mods = item
        return (1 if all(m["cosmetic"] for m in mods) else 0, slot.lower())

    out_groups = []
    for slot, mods in sorted(groups.items(), key=_group_key):
        mods.sort(key=lambda m: (m["default"], m["cosmetic"], m["name"].lower()))
        out_groups.append({"slot": slot, "cosmetic": all(m["cosmetic"] for m in mods),
                           "mods": mods})

    return {
        "archetypes": sorted(arche),
        "unique_variants": variants,
        "count": sum(len(g["mods"]) for g in out_groups),
        "groups": out_groups,
        "note": ("Legendary effects are not listed here — they are shared melee-weapon "
                 "mods rather than Chainsaw mods, and roll or are crafted separately."),
    }


# ── drop-rate blocks (containers / camp producers / vendors) ─────────────────
def _rng76():
    """(resolver, appearance_fn) or (None, None). Optional: without it the container
    and event rates stay blank rather than the build failing."""
    try:
        import rng76
        res = rng76.Rng76Data.from_tsv_root(TSV).resolver
        return res, (lambda list_id, targets: res.appearance_prob(list_id, targets))
    except Exception as e:
        print(f"[chainsaws] [warn] rng76 unavailable ({e}); container/event rates blank.")
        return None, None


def build_containers(closure, targets, appearance_fn, tables):
    """Container TYPE -> rate rows (spawn-guide §9k). Reuses the shared helper so the
    chainsaw page and every farming page compute rates the same way."""
    if not appearance_fn:
        return {"types": []}
    try:
        from build_farming_used_for import container_types, _load_cont_names
    except Exception as e:
        print(f"[chainsaws] [warn] container types unavailable ({e}).")
        return {"types": []}
    cont_names = _load_cont_names(TSV)
    types = container_types(closure, targets, appearance_fn, cont_names,
                            tables.get("lvli_refs", {}), tables.get("parent_edid", {}))
    return {"types": types}


def build_camp_producers(targets):
    """Collectron / resource-generator cards, joined from the committed camp-item
    exports — never hand-typed. Empty lists are a real answer and still render."""
    try:
        from build_farming_used_for import _load_camp_producers, _producer_entries
    except Exception:
        return {"entries": []}, {"entries": []}
    dist = os.path.join(REPO, "dist")
    try:
        producers = _load_camp_producers(dist)
    except Exception:
        return {"entries": []}, {"entries": []}
    coll = _producer_entries(producers.get("collectrons", []), targets)
    res = _producer_entries(producers.get("resource_producers", []), targets)
    return {"entries": coll}, {"entries": res}


def build_vendors(closure, targets, appearance_fn):
    """Every vendor whose stock list intersects the weapon's LVLI closure, from the
    committed vendor master (spawn-guide §9j)."""
    path = os.path.join(REPO, "dist", "vendors.json")
    try:
        vendors = json.load(open(path, encoding="utf-8")).get("vendors", [])
    except Exception:
        return []
    cl = {str(c).upper() for c in closure}
    out = []
    for v in vendors:
        sells = {str(s).upper() for s in (v.get("sells_formids") or [])}
        hit = sells & (cl | {t.upper() for t in targets})
        if not hit:
            continue
        rate = None
        if appearance_fn:
            try:
                rate = max((appearance_fn(h, targets) or 0) for h in hit)
            except Exception:
                rate = None
        out.append({
            "name": v.get("full") or v.get("name") or v.get("edid", ""),
            "edid": v.get("edid", ""),
            "marker": v.get("marker", ""),
            "region": v.get("region", ""),
            "tier": "guaranteed" if rate and rate >= 0.999 else "chance",
            "rate": rate,
            "rate_display": (f"{rate * 100:.2f}%".rstrip("0").rstrip(".") if rate else ""),
        })
    out.sort(key=lambda v: (v["region"], v["name"].lower()))
    return out


# ── page build ───────────────────────────────────────────────────────────────
def build_page(cfg, tables, geo, cur, cache, db_ok, generated, resolver, appearance_fn,
               dist_dir):
    weap_fid = cfg["weap"].upper()
    base, dnam = load_weapon(weap_fid)

    out_dir = os.path.join(dist_dir, FAMILY)
    path = os.path.join(out_dir, cfg["slug"] + ".json")
    keep = ebuild.load_existing(path)

    records = [{"formid": weap_fid, "sig": "WEAP", "edid": base.get("WEAP_EDID", ""),
                "world_source_type": "direct"}]
    src = esources.get_sources(records, tables, weapon_classify,
                               placed_sigs=esources.PLACED_SIGS_DEFAULT,
                               place_item_bases=True)
    seen, lists_n = ebuild.resolve_placements(src, geo, cur, cache, db_ok)
    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        seen, ALL_REGIONS, keep)
    # Shared-loot-pool points held back by group_regions — names only (group_chance).
    chance_spawns = ebuild.group_chance(seen, ALL_REGIONS)
    _label_spawns(regions_out, cfg["name"])

    targets = {weap_fid}
    closure = src["lvli_closure"]

    # NOTE: the closure also reaches MILE_LL_MysteryCrate_AshHeap_*. Those are CUT
    # CONTENT — the crates were never shipped — so they are deliberately NOT surfaced
    # here, and `mystery_?crate` must stay out of the shared events vocabulary. The
    # Chainsaw's only live reward route is the Gleaming Depths raid pool.
    events_activities = eevents.detect(closure, tables["parent_edid"], c2p=tables["c2p"])
    eevents.resolve_event_rates(events_activities, targets, appearance_fn)

    collectrons, resource_generators = build_camp_producers(targets)
    doc = {
        "_meta": {
            "generated": generated,
            "source": SOURCE_TAG,
            "weapon_form_id": weap_fid,
            "lists_in_closure": lists_n,
            "source_totals": src_totals,
            "unresolved": unresolved,
        },
        "set": FAMILY,
        "slug": cfg["slug"],
        "name": cfg["name"],
        "page_title": cfg["page_title"],
        "url": URL_OF(cfg["slug"]),
        "blurb": cfg["blurb"],
        # Used For is replaced by these two on a weapon page; farming_tips stays null.
        "weapon_stats": build_weapon_stats(base, dnam),
        "mods": build_mods(base, weap_fid),
        "farming_tips": None,
        "drop_rates": {
            "containers": build_containers(closure, targets, appearance_fn, tables),
            "collectrons": collectrons,
            "resource_generators": resource_generators,
            "creatures": {
                "entries": [],
                # The closure reaches the shared LegendaryItems_LL_* pools, which are
                # applied by the legendary system rather than sitting in any creature's
                # own death list — so there is nothing to map, but it IS a real route
                # and saying so is better than an unexplained empty expand.
                "note": (f"No creature carries the {cfg['name']} in its own death-drop "
                         f"list. It can still drop from a legendary enemy: the shared "
                         f"legendary melee-weapon pools reach it."
                         if _legendary_in(closure, tables) else ""),
            },
        },
        "vendor_list": build_vendors(closure, targets, appearance_fn),
        "events_activities": events_activities,
        "regions": regions_out,
        "chance_spawns": chance_spawns,
        "fixed_spawn_totals": {"markers": total, "placements": placements},
    }

    # spawn-guide §9k: one photo/direction slot set per placement, always.
    bad = [(r["region"], l["marker"]) for r in doc["regions"] for l in r["locations"]
           if len(l.get("spawns") or []) != l["count"]]
    if bad:
        raise AssertionError(f"[{cfg['slug']}] spawns/count mismatch at {bad[:5]}")

    os.makedirs(out_dir, exist_ok=True)
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return doc, path


def _legendary_in(closure, tables):
    pe = tables["parent_edid"]
    return any("legendaryitems" in (pe.get(c, "") or "").lower() for c in closure)


def _label_spawns(regions_out, item_name):
    """Name each per-spawn block within its marker: 'Chainsaw #1', 'Chainsaw #2' …
    (spawn-guide §9k — the label names the thing standing there, numbered per type)."""
    for reg in regions_out:
        for loc in reg.get("locations", []):
            spawns = loc.get("spawns") or []
            per_type = {}
            counts = {}
            for sp in spawns:
                counts[sp.get("source_type", "")] = counts.get(sp.get("source_type", ""), 0) + 1
            for sp in spawns:
                st = sp.get("source_type", "")
                per_type[st] = per_type.get(st, 0) + 1
                nm = item_name if st in ("direct", "static") else f"{item_name} ({st})"
                sp["label"] = nm if counts[st] == 1 else f"{nm} #{per_type[st]}"


# ── entry point ──────────────────────────────────────────────────────────────
def seed_geo_cache(pages, tables, geo_cache_path):
    """The DB pass, on its own. Resolve every placement through Mappalachia, write the
    geo cache, write nothing else.

    Deliberately separate from the doc build so the two heavy things never run in the
    same process: rng76 holds the whole LVLI export in memory (~260 MB) and the
    Mappalachia DB is a ~459 MB SQLite scan. Together on a laptop that is enough to
    start swapping. This pass loads NEITHER rng76 nor the camp-item / vendor / curve
    exports — just the TSV closure and the DB. Run it once, then run the normal
    (DB-free) build, which reads the cache this wrote.
    """
    geo = Geo(MAPPALACHIA_DB)
    con = sqlite3.connect(MAPPALACHIA_DB)
    cur = con.cursor()
    cache = ebuild.load_cache(geo_cache_path)
    before = len(cache)
    try:
        for cfg in pages:
            base, _ = load_weapon(cfg["weap"].upper())
            records = [{"formid": cfg["weap"].upper(), "sig": "WEAP",
                        "edid": base.get("WEAP_EDID", ""), "world_source_type": "direct"}]
            src = esources.get_sources(records, tables, weapon_classify,
                                       placed_sigs=esources.PLACED_SIGS_DEFAULT,
                                       place_item_bases=True)
            seen, _ = ebuild.resolve_placements(src, geo, cur, cache, True)
            print(f"  {cfg['slug']:<14} resolved {len(seen)} placement(s)")
    finally:
        con.close()
    ebuild.save_cache(cache, geo_cache_path)
    print(f"[chainsaws] geo cache saved: {len(cache)} placements "
          f"({len(cache) - before:+d}) -> {geo_cache_path}")
    print("[chainsaws] Now run the normal build (no DB needed) to write the pages:")
    print("             python src/build_chainsaw_spawns_json.py --pts")


def run(argv=None):
    argv = list(argv or [])
    pts = "--pts" in argv
    geo_only = "--geo-only" in argv
    slug_filter = {a for a in argv if not a.startswith("-") and a not in (FAMILY, "chainsaw-family")}
    pages = [c for c in CHAINSAWS if not slug_filter or c["slug"] in slug_filter]
    if not pages:
        print(f"[chainsaws] no page matched {sorted(slug_filter)}.")
        return

    dist_dir = os.path.join(REPO, "dist", "pts") if pts else os.path.join(REPO, "dist")
    geo_cache_path = os.environ.get(
        "CHAINSAW_GEO_CACHE",
        os.path.join(REPO, "data", "chainsaw_spawns", "geo_cache.json"))

    tables = esources.load_tables()

    if geo_only:
        if not os.path.exists(MAPPALACHIA_DB):
            print(f"[chainsaws] --geo-only needs the Mappalachia DB, but "
                  f"{MAPPALACHIA_DB!r} does not exist. Set MAPPALACHIA_DB and retry.")
            return
        print("[chainsaws] geo-only pass — Mappalachia DB, no rng76, no page writes.")
        seed_geo_cache(pages, tables, geo_cache_path)
        return

    resolver, appearance_fn = _rng76()
    if resolver:
        print("[chainsaws] rng76 loaded — container / event rates will be computed.")

    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    cache = ebuild.load_cache(geo_cache_path)
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB)
        cur = con.cursor()
        print("[chainsaws] Mappalachia DB found — resolving placements and refreshing geo cache.")
        print("[chainsaws] [note] this run holds rng76 AND the Mappalachia DB at once "
              "(~700 MB peak). On a machine that is tight on RAM, use the two-step "
              "instead: `--geo-only` with the DB, then a normal run without it.")
    elif cache:
        print(f"[chainsaws] No DB — rebuilding from committed geo cache ({len(cache)} placements).")
    else:
        print("[chainsaws] No Mappalachia DB and no geo cache — Fixed Spawn Locations will "
              "be empty. Run once locally with MAPPALACHIA_DB set to seed "
              "data/chainsaw_spawns/geo_cache.json, then commit it.")

    generated = datetime.date.today().isoformat()
    hub = []
    for cfg in pages:
        doc, path = build_page(cfg, tables, geo, cur, cache, db_ok, generated,
                               resolver, appearance_fn, dist_dir)
        t = doc["fixed_spawn_totals"]
        print(f"  {cfg['slug']:<14} mods:{doc['mods']['count']:>3}  "
              f"containers:{len(doc['drop_rates']['containers']['types']):>3}  "
              f"events:{len(doc['events_activities']):>3}  "
              f"vendors:{len(doc['vendor_list']):>3}  "
              f"fixed:{t['placements']:>4} placements / {t['markers']} markers")
        hub.append({
            "slug": cfg["slug"], "name": cfg["name"], "url": URL_OF(cfg["slug"]),
            "blurb": cfg["blurb"],
            "counts": {"mods": doc["mods"]["count"],
                       "fixed_spawns": t["placements"],
                       "markers": t["markers"],
                       "events": len(doc["events_activities"])},
        })

    if db_ok:
        ebuild.save_cache(cache, geo_cache_path)
        print(f"[chainsaws] geo cache saved ({len(cache)} placements) for DB-free CI rebuilds.")
    if con:
        con.close()

    hub_doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG},
        "set": FAMILY,
        "name": "Chainsaws",
        "page_title": "Weapons - Chainsaws",
        "url": URL_BASE,
        "blurb": ("Every chainsaw in Fallout 76 — where it spawns, what it rolls from, "
                  "and every mod, plan and paint it takes. Pick a chainsaw below."),
        "chainsaws": hub,
    }
    hub_path = os.path.join(dist_dir, "chainsaws.json")
    os.makedirs(dist_dir, exist_ok=True)
    json.dump(hub_doc, open(hub_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"[chainsaws] wrote {hub_path} ({len(hub)} pages) + per-page docs in "
          f"{os.path.join(dist_dir, FAMILY)}")


def main(argv=None):
    run(argv if argv is not None else sys.argv[1:])


if __name__ == "__main__":
    main()
