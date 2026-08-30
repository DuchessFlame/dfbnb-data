#!/usr/bin/env python3
r"""
spawns_configs.cryptids — the Cryptid family driver for the DF/BNB guide pipeline.

Cryptids are CREATURE-seeded, not item-seeded, so this driver seeds from RACE -> NPC
records (ActorTypeCryptid keyword 00331AC2, plus the editorial Honey Beast and the two
NPC-level cryptid tags) instead of an ALCH/LVLI item closure. It reuses the shared
spawns_engine for the fiddly parts — geo.Geo (Mappalachia region/marker resolution),
build.group_regions (placement -> region/marker grouping with per-spawn photo slots),
events.detect / events.resolve_event_rates (Events & Activities), rng76 (drop rates) —
and assembles one hub index (dist/cryptids.json) plus one doc per cryptid page
(dist/cryptids/<slug>.json).

PAGES (DF brand, under /df/cryptids/) — root-expand order per spawn-guide:
  1. Used For              — challenges that NAME this cryptid (challenges.json join)
  2. Farming Tips and Tricks — EMPTY placeholder (Duchess fills later)
  3. Activities, Events & Quests — QUEST/GMRW registry + event-keyword pass over the
                             up-closure of the cryptid's death lists
  4. Drops                 — death-drop loot (INAM leveled item lists) resolved via rng76
  5. Random Encounters     — cryptid REs from challenges.json encounter_pages
  6. Fixed Spawn Locations — ambush markers + static NPC placements + Mappalachia
                             npcName spawns, grouped region -> marker, one photo-slot
                             set per spawn (spawn-guide §9k)

ROUTING IS KEYWORD / EDID / RACE DRIVEN — NO hardcoded FormIDs for routing. The only
per-page curation is the race EDID seed set + the display tokens derived from it.

Usage:
    python src/build_spawns.py cryptids [slug ...]
    python src/build_cryptids_json.py            # (thin wrapper -> run())
"""

import os, re, csv, sys, glob, json, sqlite3, datetime
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(HERE)
REPO = os.path.dirname(SRC)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from spawns_engine.geo import Geo
from spawns_engine import sources as esources
from spawns_engine import build as ebuild
from spawns_engine import events as eevents
import tsv_source          # one resolver for every export selection
from prune_outputs import prune_outputs

# ── constants ────────────────────────────────────────────────────────────────
CRYPTID_KEYWORD = "00331AC2"          # ActorTypeCryptid (roster authority)
DUMMY_RACE = "DummyActorRace"          # excluded (dummy actor, no live spawns)

TSV = esources.TSV
DIST = os.path.join(REPO, "dist")
OUT_DIR = os.path.join(DIST, "cryptids")
HUB_FILE = os.path.join(DIST, "cryptids.json")
MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
APPALACHIA_SPACE = 2480661   # the only worldspace with regions/markers in the DB
GEO_CACHE = os.environ.get(
    "CRYPTIDS_GEO_CACHE",
    os.path.join(REPO, "data", "cryptid_spawns", "geo_cache.json"))
CHALLENGES_JSON = os.path.join(DIST, "challenges", "challenges.json")
SOURCE_TAG = ("Game-file exports (KYWD/NPC/LVLI) + challenges.json + Mappalachia "
              "Position/Entity (cached for CI)")

ALL_REGIONS = [
    "Ash Heap", "Atlantic City", "Burning Springs", "Cranberry Bog", "Forest",
    "Savage Divide", "Skyline Valley", "The Mire", "The Pitt", "Toxic Valley",
]

# Dev / cut / audio-template NPC editor IDs that inherit records but never place a
# live creature — filtered from the roster and the Fixed Spawn placement pass.
DEV_NPC_RE = re.compile(r"(^test|_test|^audiotemplate|^dummy|debug|^qa|zzz_)", re.I)
# Cut / test / non-spawn ambush markers to skip in the Fixed Spawn sweep.
DEV_MARKER_RE = re.compile(r"(nonspawn|tobedelete|^test|zzz_|lowhealth)", re.I)


# ── page roster ──────────────────────────────────────────────────────────────
# Each page folds one or more RACEs. `races` are the RNAM_EDID discriminators; the
# display / death-list / ambush tokens are DERIVED from the race names so routing
# stays keyword-driven. `extra_npcs` folds NPC-level cryptid tags onto a page.
# `mappalachia` names join the Mappalachia NPC table. `hold` = decision pending
# (built to disk for review, NOT wired into a guide_index page row yet).
PAGES = [
    {"slug": "bigfoot", "name": "Bigfoot", "races": ["BigfootRace"],
     "tokens": ["bigfoot"], "mappalachia": []},
    {"slug": "blue-devil", "name": "Blue Devil", "races": ["BlueDevilRace"],
     "tokens": ["bluedevil"], "mappalachia": []},
    {"slug": "flatwoods-monster", "name": "Flatwoods Monster",
     "races": ["FlatwoodsMonsterRace"], "tokens": ["flatwoods"], "mappalachia": [],
     "chance_note": ("The Flatwoods Monster is a random night encounter — after roughly "
                     "6&nbsp;pm in-game it has about a 2.5% chance to appear at almost any "
                     "location on the map. It has no fixed spawn points, so there is nothing "
                     "to map; the Invaders From Beyond event and the Queen of the Hunt daily "
                     "are the reliable ways to find one.")},
    {"slug": "grafton-monster", "name": "Grafton Monster",
     "races": ["GraftonMonsterRace"], "tokens": ["grafton"],
     "mappalachia": ["Grafton Monster"]},
    {"slug": "jersey-lesser-devils", "name": "Jersey and Lesser Devils",
     "races": ["XPD_JerseyDevilRace", "XPD_LesserDevilRace"],
     "tokens": ["jerseydevil", "lesserdevil"], "mappalachia": []},
    {"slug": "mothman", "name": "Mothman", "races": ["MothmanRace"],
     "extra_npcs": ["004EC627"],  # Stalking Mothman (EncMothman01Defender)
     "tokens": ["mothman"], "mappalachia": [],
     "chance_note": ("These are NIGHT spawns — the Mothman only appears after roughly "
                     "6&nbsp;pm in-game, and each is a chance (not guaranteed) spawn. See the "
                     "separate night-spawn map for where they can appear. Its one guaranteed "
                     "fixed spawn (Enclave Research Facility) is in Fixed Spawn Locations above.")},
    {"slug": "ogua", "name": "Ogua", "races": ["OguaRace"],
     "tokens": ["ogua"], "mappalachia": []},
    {"slug": "sheepsquatch", "name": "Sheepsquatch", "races": ["SheepsquatchRace"],
     "tokens": ["sheepsquatch"], "mappalachia": []},
    {"slug": "snallygaster", "name": "Snallygaster", "races": ["SnallyGasterRace"],
     "tokens": ["snallygaster"], "mappalachia": ["Snallygaster"]},
    {"slug": "wendigo", "name": "Wendigo",
     "races": ["WendigoRace", "WendigoColossusRace"],
     "tokens": ["wendigo"], "mappalachia": []},
    # ── new pages (Megasloth + Aliens) ──
    {"slug": "megasloth", "name": "Megasloth", "races": ["MegaSlothRace"],
     "tokens": ["megasloth"], "mappalachia": ["Mega Sloth"]},
    {"slug": "aliens", "name": "Aliens",
     "races": ["AlienRace", "ZetanInvaderRace"],
     "tokens": ["alien", "zetaninvader"], "mappalachia": []},
    # ── decision pending (built to disk, no guide_index row) ──
    # Angler is kept built-to-disk pending its future home under a Meat category,
    # NOT the cryptid hub. Honey Beast now lives on the Honeycomb non-perishable
    # farming page (build_honeycomb_spawns_json.py), not here. The Beast of Beckley
    # is a one-off quest wolf boss and is intentionally omitted entirely.
    {"slug": "angler", "name": "Angler", "races": ["DLC03_AnglerRace"],
     "tokens": ["angler"], "mappalachia": ["Angler"], "hold": True},
]

URL_BASE = "/df/cryptids/"


# ── file helpers ─────────────────────────────────────────────────────────────
def _newest(pattern, exclude=None):
    # `exclude` filters sibling exports that share the same date token — a bare
    # "NPC_Export_*.tsv" otherwise also matches "..._Refs"/"..._PRPS" and the
    # date-tie sort can return the wrong one (no INAM column -> zero races).
    return tsv_source.newest(os.path.join(TSV, pattern), exclude=exclude, required=False)


def pct(v, places=2):
    """Probability (0..1) -> display string. A computed 0 is a real answer -> '0%'."""
    if v is None:
        return ""
    if v <= 0:
        return "0%"
    if v >= 0.99995:
        return "100%"
    if v * 100 < 0.01:
        return "<0.01%"
    s = f"{v * 100:.{places}f}".rstrip("0").rstrip(".")
    return (s or "0") + "%"


def r6(v):
    return None if v is None else round(float(v), 6)


# ── roster verification (KYWD refs) ──────────────────────────────────────────
def verify_roster():
    """Confirm every configured RACE is tagged ActorTypeCryptid (or is the editorial
    Honey Beast / an NPC-level tag). Prints a warning on drift — never hardcodes."""
    refs = _newest("KYWD_Export_*_Refs.tsv")
    tagged_races, tagged_npcs = set(), set()
    if refs:
        with open(refs, encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                if (r.get("KeywordFormID") or "").strip().upper() != CRYPTID_KEYWORD:
                    continue
                sig = (r.get("RefSignature") or "").strip()
                edid = (r.get("RefEDID") or "").strip()
                fid = (r.get("RefFormID") or "").strip().upper()
                if sig == "RACE":
                    tagged_races.add(edid)
                elif sig == "NPC_":
                    tagged_npcs.add(fid)
    editorial = {"HoneyBeastRace"}   # Honey Beast is untagged in game data
    for pg in PAGES:
        for rc in pg.get("races", []):
            if rc not in tagged_races and rc not in editorial:
                print(f"[cryptids] [warn] race {rc} on page {pg['slug']} is NOT "
                      f"ActorTypeCryptid-tagged (editorial addition?)")
    return tagged_races, tagged_npcs


# ── NPC roster ───────────────────────────────────────────────────────────────
def load_npcs():
    """Return {RNAM_EDID -> [npc rows]} and {FormID -> npc row} from the NPC export.
    npc row = dict of the columns we need."""
    path = _newest("NPC_Export_*.tsv", exclude=["_Refs", "_PRPS"])
    if not path:
        raise FileNotFoundError("no NPC_Export_*.tsv")
    by_race, by_fid = defaultdict(list), {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            row = {
                "formid": (r.get("FormID") or "").strip().upper(),
                "edid": (r.get("EDID") or "").strip(),
                "full": (r.get("FULL") or "").strip(),
                "race": (r.get("RNAM_EDID") or "").strip(),
                "inam_fid": (r.get("INAM_FormID") or "").strip().upper(),
                "inam_edid": (r.get("INAM_EDID") or "").strip(),
                "tplt_edid": (r.get("TPLT_EDID") or "").strip(),
                "level": (r.get("ACBS_Level") or "").strip(),
                "min_lvl": (r.get("ACBS_CalcMinLvl") or "").strip(),
                "max_lvl": (r.get("ACBS_CalcMaxLvl") or "").strip(),
                "health": (r.get("DNAM_CalcHealth") or "").strip(),
            }
            if row["race"]:
                by_race[row["race"]].append(row)
            if row["formid"]:
                by_fid[row["formid"]] = row
    return by_race, by_fid


def page_npcs(pg, by_race, by_fid):
    """Live NPCs on a page: race members + any extra NPC-level tags, minus dev/test."""
    out, seen = [], set()
    for rc in pg.get("races", []):
        for row in by_race.get(rc, []):
            if row["formid"] in seen or DEV_NPC_RE.search(row["edid"]):
                continue
            seen.add(row["formid"]); out.append(row)
    for fid in pg.get("extra_npcs", []):
        row = by_fid.get(fid.upper())
        if row and row["formid"] not in seen:
            seen.add(row["formid"]); out.append(row)
    return out


def npc_summary(npcs):
    """Small header stat block: NPC count + observed level / health band."""
    def _ints(key):
        vals = []
        for n in npcs:
            try:
                v = int(float(n[key]))
                if v > 0:
                    vals.append(v)
            except (ValueError, TypeError):
                pass
        return vals
    lv = _ints("min_lvl") + _ints("max_lvl") + _ints("level")
    hp = _ints("health")
    return {
        "npc_count": len(npcs),
        "level_min": min(lv) if lv else None,
        "level_max": max(lv) if lv else None,
        "health_max": max(hp) if hp else None,
    }


# ── Drops (death-drop LVLIs via rng76) ───────────────────────────────────────
def page_death_lists(pg, npcs):
    """Distinct (form_id, edid) death lists whose EDID carries one of the page's own
    creature tokens — so generic shared lists (Deathclaw / DailyOps / Wolf / Eyebot)
    attached to a stray test NPC are excluded and each page shows only its own loot.
    Returns (lists, inherited_only) — inherited_only flags the Q3 case where every
    live NPC's loot comes via a TPLT/LChar chain rather than a direct INAM list."""
    toks = [t.lower() for t in pg.get("tokens", [])]
    lists, seen = [], set()
    for n in npcs:
        ed = (n["inam_edid"] or "").lower()
        if not n["inam_fid"] or n["inam_fid"] in seen:
            continue
        if ed.startswith("zzz_") or "test" in ed:
            continue
        if any(t in ed for t in toks):
            seen.add(n["inam_fid"])
            lists.append({"form_id": n["inam_fid"], "edid": n["inam_edid"]})
    # inherited_only: the page has NPCs but none of their INAM lists matched the
    # cryptid's own token (loot handled by a template / quest script instead).
    inherited_only = bool(npcs) and not lists
    return lists, inherited_only


_POOL_PREFIX_RE = re.compile(r"^(?:LLD?|LLS|LLI|LL|LVLI)_(?:Creature_)?", re.I)


_SIZE_POOL_RE = re.compile(r"^(?:LLS_Creature_)?(?:Very)?(?:Large|Small|Medium|Tiny|Huge)$", re.I)


def _prettify_pool(edid):
    """Friendly name for a generic sub-pool EDID (display only, never routing)."""
    if not edid:
        return "Loot pool"
    # generic size-graded creature loot pools read as bare 'Very Large' etc. — label
    # them as what they are so the row isn't cryptic.
    if _SIZE_POOL_RE.match(edid):
        size = re.sub(r"^LLS_Creature_", "", edid, flags=re.I)
        size = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", size)
        return f"Generic creature loot ({size.strip().lower()})"
    s = _POOL_PREFIX_RE.sub("", edid)
    s = re.sub(r"_+", " ", s)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or edid


def _list_entry_rates(resolver, list_id):
    """The DIRECT entries of a leveled list with each entry's own drop rate — one
    level only, so a creature's death list shows its real structure (a few guaranteed
    parts + a handful of generic sub-pools) instead of a 500-leaf explosion.

    The per-entry rate MATH is rng76's (pick-weight / ChanceNone / GetRandomPercent /
    max_count all read via the resolver's own helpers); this only replicates the mode
    DISPATCH from rng76.resolve_simple so numbers stay identical to the tree pages.
    Returns [{rate, sub, ref, qty}]."""
    lvli = resolver.lvli
    flags = lvli.flags_for(list_id)
    is_use_all = flags["use_all"]
    is_first = flags["first_match"]
    raw = []
    for e in lvli.entries_by_list.get(list_id, []):
        idx = e.get("EntryIndex")
        if idx is None:
            continue
        math = lvli.math_by_entry.get((list_id, idx))
        if not math:
            continue
        pw, cn = resolver._entry_pick_and_cn(math, e, list_id)
        conds = resolver._entry_conditions(e)
        if is_use_all and conds:
            grp = resolver.extract_grp_chance(conds)
            if grp is not None:
                pw, cn = grp, 1.0
        raw.append({"pw": pw, "cn": cn,
                    "sub": (math.get("SubLVLI_FormID") or "").strip(),
                    "ref": (e.get("LVLO_Reference") or "").strip(),
                    "qty": resolver._entry_qty(e), "conds": conds})
    if is_first:
        _fm = resolver.first_match_rates([r["conds"] for r in raw])
        if _fm is not None:
            for i, r in enumerate(raw):
                r["rate"] = _fm[i]
        else:
            cum = 1.0
            for r in raw:
                s = r["pw"] * r["cn"]; r["rate"] = s * cum; cum *= (1.0 - s)
    elif is_use_all:
        mc = lvli.max_count_for(list_id, resolver.globs, resolver.curvs)
        if mc == 1:
            cum = 1.0
            for r in raw:
                d = r["pw"] * r["cn"]; r["rate"] = d * cum; cum *= (1.0 - d)
        else:
            for r in raw:
                r["rate"] = r["pw"] * r["cn"]
    else:
        tot = sum(r["pw"] for r in raw)
        for r in raw:
            r["rate"] = (r["pw"] / tot) * r["cn"] if tot > 0 else 0.0
    return raw


def resolve_drops(pg, npcs, resolver):
    """Build the Drops expand node. Each death list is shown ONE LEVEL DEEP: direct
    item drops individually, and each generic sub-pool as a single summarised row
    (name + item count + the pool's own fire chance + a few top items). Rates come
    from rng76 (drop-rate-engine rules) — a computed 0% is published as 0%."""
    lists, inherited_only = page_death_lists(pg, npcs)
    out_lists = []
    if resolver is not None:
        for dl in lists:
            rows = []
            try:
                entries = _list_entry_rates(resolver, dl["form_id"])
            except Exception as e:
                print(f"[cryptids] [warn] entry-rate failed for {dl['edid']}: {e}")
                entries = []
            for en in entries:
                rv = r6(en["rate"])
                if en["sub"]:
                    try:
                        deep = resolver.resolve_deep(en["sub"]) or []
                    except Exception:
                        deep = []
                    # dedupe by FormID keeping the best rate, then take the top 5
                    best = {}
                    for it in deep:
                        k = (it.get("formid") or "").upper() or it.get("name")
                        if k not in best or (it.get("dropRate") or 0) > (best[k].get("dropRate") or 0):
                            best[k] = it
                    top = sorted(best.values(),
                                 key=lambda i: -(i.get("dropRate") or 0))[:5]
                    sub_ed = resolver.lvli.edid_for(en["sub"]) or ""
                    rows.append({
                        "kind": "pool", "name": _prettify_pool(sub_ed),
                        "sub_id": en["sub"].upper(), "edid": sub_ed,
                        "item_count": len(deep),
                        "rate_value": rv, "rate_display": pct(rv),
                        "top_items": [{"name": t.get("name") or t.get("edid"),
                                       "rate_display": pct(r6(t.get("dropRate")))}
                                      for t in top],
                    })
                else:
                    ref = en["ref"]
                    fid = ref.split(":")[0].upper() if ":" in ref else ref.upper()
                    edid = ref.split(":")[1] if ref.count(":") >= 2 else ""
                    sig = ref.split(":")[-1].upper() if ref.count(":") >= 2 else ""
                    name = resolver.names.resolve(fid, edid) or edid or fid
                    rows.append({
                        "kind": "item", "name": name, "form_id": fid,
                        "edid": edid, "sig": sig, "qty": en["qty"],
                        "rate_value": rv, "rate_display": pct(rv),
                    })
            # merge duplicate direct item rows (same FormID via >1 entry -> combine
            # independently), keep pools as-is
            by_fid, ordered = {}, []
            for r in rows:
                if r["kind"] == "item" and r["form_id"]:
                    prev = by_fid.get(r["form_id"])
                    if prev:
                        a = prev["rate_value"] or 0.0
                        b = r["rate_value"] or 0.0
                        prev["rate_value"] = r6(1.0 - (1.0 - a) * (1.0 - b))
                        prev["rate_display"] = pct(prev["rate_value"])
                        prev["qty"] = max(prev["qty"], r["qty"])
                        continue
                    by_fid[r["form_id"]] = r
                ordered.append(r)
            ordered.sort(key=lambda r: (-(r["rate_value"] or 0), r["name"].lower()))
            direct_items = sum(1 for r in ordered if r["kind"] == "item")
            pool_count = sum(1 for r in ordered if r["kind"] == "pool")
            leaf_total = direct_items + sum(r["item_count"] for r in ordered
                                            if r["kind"] == "pool")
            out_lists.append({
                "edid": dl["edid"], "form_id": dl["form_id"],
                "row_count": len(ordered), "direct_items": direct_items,
                "pool_count": pool_count, "item_count": leaf_total, "rows": ordered,
            })
    empty_reason = "" if out_lists else ("script" if inherited_only else "none")
    return {"lists": out_lists, "inherited_only": inherited_only,
            "empty_reason": empty_reason}


# ── Activities, Events & Quests ──────────────────────────────────────────────
_QUEST_CACHE = {}
# Quest rows that are really RE scaffolding / dialogue / cut — surfaced under
# Random Encounters instead, so they don't double up here.
_QUEST_SKIP_RE = re.compile(
    r"(creaturedialog|deleted_|\bvs\b|\bvs\.|returning to cave|totem worship|"
    r"campfire stories|cryptid stories|patrol$)", re.I)


def _load_quests():
    if "rows" in _QUEST_CACHE:
        return _QUEST_CACHE["rows"]
    path = _newest("QUEST_Export_*.tsv")
    rows = []
    if path:
        with open(path, encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                rows.append({
                    "form_id": (r.get("FormID") or "").strip().upper(),
                    "edid": (r.get("EDID") or "").strip(),
                    "full": (r.get("FULL - Name") or "").strip(),
                    "type": (r.get("Quest Type") or "").strip(),
                    "location": (r.get("LNAM - Location") or "").strip(),
                })
    _QUEST_CACHE["rows"] = rows
    return rows


def resolve_events(pg, death_list_fids, tbls, appearance_fn):
    """Activities, Events & Quests that FEATURE this cryptid. Two passes, both
    keyword/registry driven (no hardcoded FormIDs):

      1. QUEST name pass — quests / public events / activities whose display name
         carries one of the cryptid's tokens and are a real quest (Quest Type set,
         not the RE-scene / dialogue scaffolding which lives under Random Encounters).
      2. LVLI reward-closure pass — event/activity reward pools reachable UP from
         the cryptid's death lists (shared events engine). Usually empty for a
         creature, kept so nested reward bags are never missed.
    """
    toks = _page_tokens(pg) + [t.lower() for t in pg.get("event_tokens", [])
                               if len(t) >= 4]
    out, seen = [], set()
    for q in _load_quests():
        if not q["full"] or (q["type"] or "").lower() == "none":
            continue
        if _QUEST_SKIP_RE.search(q["full"]):
            continue
        hay = (q["full"] + " " + q["edid"]).lower()
        if not any(re.search(r"\b" + re.escape(t) + r"\b", hay) for t in toks):
            continue
        key = q["form_id"] or q["edid"]
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": q["full"], "edid": q["edid"], "type": q["type"] or "Quest",
                    "location": q["location"], "form_id": q["form_id"]})

    # LVLI reward-closure pass (nested event reward bags)
    if death_list_fids:
        c2p = tbls["c2p"]
        closure, stack = set(), list(death_list_fids)
        while stack:
            n = stack.pop()
            if n in closure:
                continue
            closure.add(n)
            for p in c2p.get(n, ()):
                if p not in closure:
                    stack.append(p)
        ev = eevents.detect(closure, tbls["parent_edid"], c2p=c2p)
        eevents.resolve_event_rates(ev, set(death_list_fids), appearance_fn)
        for row in ev:
            key = row.get("list_id") or row.get("edid")
            if key not in seen:
                seen.add(key)
                out.append({"name": row.get("name"), "edid": row.get("edid", ""),
                            "type": row.get("type", "Event / Activity"),
                            "location": "", "form_id": "",
                            "rate_display": row.get("rate_display", "")})
    out.sort(key=lambda r: (r["type"], (r["name"] or "").lower()))
    return out


# ── Used For (challenges that name the cryptid) ──────────────────────────────
_CHAL_CACHE = {}


def load_challenges():
    if "d" in _CHAL_CACHE:
        return _CHAL_CACHE["d"]
    try:
        d = json.load(open(CHALLENGES_JSON, encoding="utf-8"))
    except Exception as e:
        print(f"[cryptids] [warn] challenges.json unreadable ({e}); Used For blank.")
        d = {}
    _CHAL_CACHE["d"] = d
    return d


def _chal_type(page_key, chal):
    if str(page_key).startswith("season:"):
        return "Mini Season"
    return (chal.get("scope") or chal.get("group") or
            str(page_key).replace("-", " ").title() or "Challenge")


def _page_tokens(pg):
    toks = [t.lower() for t in pg.get("tokens", [])] + [pg["name"].lower()]
    for rc in pg.get("races", []):
        toks.append(rc.lower().replace("race", "").replace("xpd_", "")
                    .replace("dlc03_", ""))
    return [t for t in toks if len(t) >= 4]


def used_for(pg, cur_url):
    """Challenges naming this cryptid: matched by the challenge's own guides[] link
    (already curated in the challenges pipeline) OR by name/EDID/condition tokens for
    the cryptid. Cut challenges are excluded. Row = {type, name, required, edid, page}."""
    d = load_challenges()
    pages = d.get("pages", {})
    toks = _page_tokens(pg)
    out, seen = [], set()
    for pk, plist in pages.items():
        items = plist if isinstance(plist, list) else (
            plist.get("challenges") or plist.get("items") or [])
        for c in items or []:
            if not isinstance(c, dict) or c.get("is_cut"):
                continue
            edid = c.get("edid", "")
            key = c.get("form_id") or edid
            if key in seen:
                continue
            linked = any((g.get("url") or "").rstrip("/") == cur_url.rstrip("/")
                         for g in (c.get("guides") or []))
            hay = (c.get("full", "") + " " + edid + " " +
                   " ".join(c.get("conditions_display") or [])).lower()
            named = any(re.search(r"\b" + re.escape(t) + r"\b", hay) for t in toks)
            if not (linked or named):
                continue
            seen.add(key)
            out.append({"type": _chal_type(pk, c), "name": c.get("full") or edid,
                        "required": c.get("required") or 1, "edid": edid, "page": pk})
    # dedupe by name, preferring a Mini Season labelling
    out.sort(key=lambda r: (r["name"].lower(), r["type"] != "Mini Season"))
    dedup, names = [], set()
    for r in out:
        if r["name"].lower() in names:
            continue
        names.add(r["name"].lower()); dedup.append(r)
    dedup.sort(key=lambda r: (r["type"], r["name"].lower()))
    return dedup


# ── Random Encounters (challenges.json encounter_pages) ──────────────────────
def random_encounters(pg):
    d = load_challenges()
    eps = d.get("encounter_pages", {})
    toks = _page_tokens(pg)
    out, seen = [], set()
    for pk, page in eps.items():
        for it in (page.get("items") if isinstance(page, dict) else []) or []:
            if not isinstance(it, dict):
                continue
            hay = (it.get("full", "") + " " + it.get("edid", "") + " " +
                   it.get("desc", "")).lower()
            if not any(t in hay for t in toks):
                continue
            key = it.get("form_id") or it.get("edid")
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "form_id": it.get("form_id", ""), "edid": it.get("edid", ""),
                "name": it.get("full") or it.get("edid", ""),
                "type": (page.get("title") if isinstance(page, dict) else pk),
                "location": it.get("location", ""),
            })
    out.sort(key=lambda r: r["name"].lower())
    return out


# ── Fixed Spawn Locations (ambush + placement + Mappalachia npcName) ──────────
def sweep_ambush_markers(pages, cur):
    """Entity sweep: cryptid-specific ambush / spawn / corpse markers -> a
    {entityFormID(int) -> (edid, page_slug)} routing map. Keyword driven, no FormIDs."""
    if cur is None:
        return {}
    rows = cur.execute(
        "SELECT entityFormID, editorID FROM Entity WHERE "
        "editorID LIKE '%Ambush%' OR editorID LIKE '%SpawnMarker%' "
        "OR editorID LIKE '%CorpseEating%' OR editorID LIKE '%SpawnAmbush%'").fetchall()
    out = {}
    for fid, edid in rows:
        e = (edid or "").lower()
        if DEV_MARKER_RE.search(e):
            continue
        for pg in pages:
            atoks = pg.get("ambush_tokens") or pg.get("tokens", [])
            axcl = pg.get("ambush_exclude") or []
            if any(t in e for t in atoks) and not any(x in e for x in axcl):
                out[int(fid)] = (edid, pg["slug"])
                break
    return out


def sweep_nest_markers(pages, cur):
    """Entity sweep for NEST objects (EDID contains 'Nest'), matched to a page's
    `nest_tokens` -> {entityFormID(int) -> (edid, page_slug)}. Only pages that opt in
    via `nest_tokens` get nests, so this never changes a page that didn't ask for it.
    A nest is a distinct source_type ('nest') per spawn-guide §9k — never merged into
    the creature-spawn count."""
    active = [pg for pg in pages if pg.get("nest_tokens")]
    if cur is None or not active:
        return {}
    rows = cur.execute("SELECT entityFormID, editorID, signature FROM Entity "
                       "WHERE editorID LIKE '%Nest%'").fetchall()
    out = {}
    for fid, edid, sig in rows:
        if sig not in ("CONT", "STAT", "FURN", "ACTI", "MSTT"):
            continue
        e = (edid or "").lower()
        if DEV_MARKER_RE.search(e):
            continue
        for pg in active:
            if any(t in e for t in pg["nest_tokens"]):
                out[int(fid)] = (edid, pg["slug"])
                break
    return out


def _chunks(seq, n=900):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _record(cache, seen, slug, inst, space, ref, x, y, region, marker, stype,
            weight=None, shared=None, variant=None, uniq=None):
    # PAGE-SCOPED cache key: a single Mappalachia spawn-point instance can belong to
    # more than one cryptid (a leveled point that can spawn e.g. Snallygaster OR
    # Angler is listed under both npcNames). A global inst key would let a later page
    # clobber an earlier one, so CI (cache-only) wouldn't reproduce the DB run. Scoping
    # the key by page keeps each page's placements independent and faithful.
    #
    # weight / shared / variant power the guaranteed-vs-weighted tiering (spawn-guide):
    #   weight  — the creature's Mappalachia spawnWeight at this NPC-table point (its
    #             share of that point's weighted pool). None for ambush/placement.
    #   shared  — True when the point's pool holds >1 creature (weighted, not guaranteed).
    #   variant — the specific variant display name for a `placement` (e.g. "Burning
    #             Radscorpion"); lets the Chance-to-Spawn list name the variants.
    rec = {"page": slug, "base": ref, "space": space,
           "x": round(x, 1) if x is not None else None,
           "y": round(y, 1) if y is not None else None,
           "region": region, "marker": marker, "source_type": stype}
    if weight is not None:
        rec["weight"] = round(weight, 4)
    if shared is not None:
        rec["shared"] = bool(shared)
    if variant:
        rec["variant"] = variant
    if uniq is not None:
        rec["unique_placement"] = bool(uniq)
    cache[f"{slug}:{inst}"] = rec
    seen[slug][inst] = (x, y, region, marker, stype)


def resolve_fixed_spawns(pages, npcs_by_page, geo, cur, cache, db_ok):
    """Resolve every fixed-spawn placement to (region, marker) and return
    {slug -> seen{instanceFormID -> (x, y, region, marker, source_type)}}.

    Three source types:
      ambush     — Position.referenceFormID = ambush-marker Entity.entityFormID
      placement  — Position.referenceFormID = a cryptid NPC base (static world spawn)
      spawn      — Mappalachia NPC table (npcName) join (working query from spawn-guide)

    DB present -> query + refresh cache (keyed by instanceFormID, tagged with page +
    source_type). DB absent -> rebuild from the committed cache.
    """
    seen = {pg["slug"]: {} for pg in pages}

    if not db_ok:
        for key, e in cache.items():
            slug = e.get("page")
            # page-scoped key "<slug>:<inst>"; fall back to the value's page + bare key
            inst = key.rsplit(":", 1)[-1]
            if slug in seen and inst.isdigit():
                seen[slug][int(inst)] = (e.get("x"), e.get("y"),
                                         e.get("region", ""), e.get("marker", ""),
                                         e.get("source_type", "spawn"))
        return seen

    # 1) ambush markers
    marker_map = sweep_ambush_markers(pages, cur)   # entityFormID -> (edid, slug)
    if marker_map:
        for chunk in _chunks(list(marker_map.keys())):
            q = ("SELECT x, y, instanceFormID, spaceFormID, referenceFormID FROM Position "
                 "WHERE referenceFormID IN (%s)" % ",".join("?" * len(chunk)))
            for x, y, inst, space, ref in cur.execute(q, tuple(chunk)):
                edid, slug = marker_map[int(ref)]
                region, marker, _ = geo.resolve(space, x, y)
                _record(cache, seen, slug, inst, space, ref, x, y, region, marker, "ambush")

    # 1b) nest markers (opt-in via nest_tokens) — a DISTINCT 'nest' source_type,
    # never merged into the creature-spawn count (spawn-guide §9k).
    nest_map = sweep_nest_markers(pages, cur)       # entityFormID -> (edid, slug)
    if nest_map:
        for chunk in _chunks(list(nest_map.keys())):
            q = ("SELECT x, y, instanceFormID, spaceFormID, referenceFormID FROM Position "
                 "WHERE referenceFormID IN (%s)" % ",".join("?" * len(chunk)))
            for x, y, inst, space, ref in cur.execute(q, tuple(chunk)):
                edid, slug = nest_map[int(ref)]
                if inst in seen[slug]:
                    continue
                region, marker, _ = geo.resolve(space, x, y)
                _record(cache, seen, slug, inst, space, ref, x, y, region, marker, "nest")

    # 2) static NPC-base placements
    base_slug = {}
    base_full = {}   # base FormID -> variant display name (e.g. "Burning Radscorpion")
    for pg in pages:
        for n in npcs_by_page[pg["slug"]]:
            try:
                fid = int(n["formid"], 16)
            except ValueError:
                continue
            base_slug[fid] = pg["slug"]
            base_full[fid] = n.get("full") or pg["name"]
    if base_slug:
        # gather placement rows first so we can count how many times each base is
        # placed on the map — a base placed exactly once is a UNIQUE INDIVIDUAL (e.g.
        # a named story cat), which is a guaranteed spawn; a base placed many times is
        # a leveled/ambient variant (spawn-guide: weighted). Pages opt in to promoting
        # unique individuals to guaranteed via `promote_unique_placements`.
        prows = []
        ref_count = defaultdict(int)
        for chunk in _chunks(list(base_slug.keys())):
            q = ("SELECT x, y, instanceFormID, spaceFormID, referenceFormID FROM Position "
                 "WHERE referenceFormID IN (%s)" % ",".join("?" * len(chunk)))
            for x, y, inst, space, ref in cur.execute(q, tuple(chunk)):
                prows.append((x, y, inst, space, ref))
                ref_count[int(ref)] += 1
        for x, y, inst, space, ref in prows:
            slug = base_slug.get(int(ref))
            if not slug or inst in seen[slug]:
                continue
            region, marker, _ = geo.resolve(space, x, y)
            # `placement` = a world placement of a specific variant base. Carry the
            # variant name (so the Chance-to-Spawn list can name it) and a `uniq` flag
            # = this base is placed exactly once (a named individual, not a leveled
            # variant) — used by tiering when the page opts in.
            _record(cache, seen, slug, inst, space, ref, x, y, region, marker,
                    "placement", variant=base_full.get(int(ref)),
                    uniq=(ref_count.get(int(ref)) == 1))

    # 3) Mappalachia npcName spawns — with spawnWeight + shared-pool tiering.
    # A single spawn point (instanceFormID) carries a WEIGHTED POOL of creatures; the
    # creature is guaranteed there only when its spawnWeight >= 1.0 AND it's the only
    # creature in the pool. Otherwise it's a weighted "possible" spawn (spawn-guide).
    npc_names = sorted({nm for pg in pages for nm in pg.get("mappalachia", [])})
    if npc_names:
        # Pool size per instance (how many distinct creatures can roll at that point).
        pool_size = {}
        pq = ("SELECT instanceFormID, COUNT(DISTINCT npcName) FROM NPC "
              "WHERE spaceFormID = ? GROUP BY instanceFormID")
        for inst, n in cur.execute(pq, (APPALACHIA_SPACE,)):
            pool_size[inst] = n
        for pg in pages:
            for nm in pg.get("mappalachia", []):
                q = ("SELECT n.instanceFormID, n.spaceFormID, p.x, p.y, n.spawnWeight "
                     "FROM NPC n JOIN Position p "
                     "ON p.instanceFormID = n.instanceFormID AND p.spaceFormID = n.spaceFormID "
                     "WHERE n.npcName = ?")
                for inst, space, x, y, sw in cur.execute(q, (nm,)):
                    if inst in seen[pg["slug"]]:
                        continue
                    region, marker, _ = geo.resolve(space, x, y)
                    shared = (pool_size.get(inst, 1) or 1) > 1
                    _record(cache, seen, pg["slug"], inst, space, None, x, y,
                            region, marker, "spawn", weight=sw, shared=shared)
    return seen


def label_spawns(regions_out, name):
    """Name each per-spawn block, numbered within its source type at each marker."""
    for reg in regions_out:
        for loc in reg["locations"]:
            typed = defaultdict(list)
            for sp in loc.get("spawns") or []:
                typed[sp.get("source_type", "spawn")].append(sp)
            label_word = {"ambush": f"{name} ambush", "placement": f"{name} spawn",
                          "spawn": f"{name} spawn", "nest": f"{name} nest"}
            counts = defaultdict(int)
            for sp in loc.get("spawns") or []:
                st = sp.get("source_type", "spawn")
                counts[st] += 1
                single = len(typed[st]) <= 1
                base = label_word.get(st, f"{name} spawn")
                sp["label"] = base if single else f"{base} #{counts[st]}"


def _pct(w):
    """0.0714 -> '7.1%', 1.0 -> '100%', 0.083 -> '8.3%'."""
    s = f"{w * 100:.1f}".rstrip("0").rstrip(".")
    return s + "%"


def tier_spawns(seen_slug, cache, slug, page_name, promote_unique=False):
    """Split a page's resolved placements into two tiers (spawn-guide):

      GUARANTEED — ambush / nest markers, and NPC-table points where the creature's
                   spawnWeight >= 1.0 AND it's the only creature in the point's pool.
                   These stay in Fixed Spawn Locations (photos / directions / map).
      WEIGHTED   — everything else: weighted shared spawn-pool points (real % chance
                   from spawnWeight) and leveled/encounter `placement` points. These
                   go to the new "Chance to Spawn Locations" list (deduped by marker,
                   naming the variants that can spawn there).

    Returns (guaranteed_seen{inst: tuple}, chance_spawns[list]). chance_spawns is
    sorted A-Z by marker; each row = {marker, region, chance_value, chance_display,
    variants[], count}.
    """
    guaranteed = {}
    weighted = []
    for inst, tup in seen_slug.items():
        x, y, region, marker, stype = tup
        rec = cache.get(f"{slug}:{inst}", {})
        weight = rec.get("weight")
        shared = rec.get("shared", False)
        variant = rec.get("variant") or page_name
        is_guaranteed = (stype in ("ambush", "nest")) or (
            stype == "spawn" and weight is not None and weight >= 0.999 and not shared)
        # opt-in: a `placement` that is a UNIQUE NAMED INDIVIDUAL (placed exactly once,
        # with its own FULL name distinct from the page) is a guaranteed static spawn —
        # e.g. the named story cats. Generic/leveled placements (variant == page name,
        # or placed many times) stay weighted.
        if promote_unique and stype == "placement" and rec.get("unique_placement") \
                and variant and variant != page_name:
            is_guaranteed = True
        if is_guaranteed:
            guaranteed[inst] = tup
        else:
            weighted.append({"region": region, "marker": marker, "weight": weight,
                             "variant": variant, "source_type": stype})
    by = {}
    for w in weighted:
        marker = w["marker"] or "Unknown location"
        e = by.setdefault((w["region"], marker),
                          {"region": w["region"], "marker": marker,
                           "weights": [], "variants": set(), "count": 0})
        e["count"] += 1
        if w["weight"]:
            e["weights"].append(w["weight"])
        e["variants"].add(w["variant"])
    rows = []
    for e in by.values():
        wmax = max(e["weights"]) if e["weights"] else None
        rows.append({"marker": e["marker"], "region": e["region"],
                     "chance_value": round(wmax, 4) if wmax else None,
                     "chance_display": _pct(wmax) if wmax else "possible",
                     "variants": sorted(e["variants"]), "count": e["count"]})
    rows.sort(key=lambda r: r["marker"].lower())
    return guaranteed, rows


def attach_breakdowns(regions_out):
    for reg in regions_out:
        for loc in reg["locations"]:
            bd = []
            for st, n in sorted((loc.get("sources") or {}).items()):
                word = {"ambush": "ambush point", "placement": "static spawn",
                        "spawn": "spawn point", "nest": "nest"}.get(st, st)
                bd.append({"label": word + ("" if n == 1 else "s"), "count": n,
                           "source_type": st})
            if bd:
                loc["breakdown"] = bd


def _blurb(pg, npcs, drops, placements):
    n = len(npcs)
    di = sum(l["item_count"] for l in drops["lists"])
    tail = []
    if n:
        tail.append(f"{n} NPC variant{'s' if n != 1 else ''}")
    if placements:
        tail.append(f"{placements} known spawn point{'s' if placements != 1 else ''}")
    if di:
        tail.append(f"{di} possible drop{'s' if di != 1 else ''}")
    head = f"The {pg['name']} in Fallout 76"
    return head + (" — " + ", ".join(tail) + "." if tail else ".")


# ── reusable single-page bundle (shared with the Honeycomb page) ─────────────
def _load_rng76():
    """Return (resolver, appearance_fn) or (None, None) if rng76 is unavailable."""
    try:
        import rng76
        resolver = rng76.Rng76Data.from_tsv_root(TSV).resolver
        appearance_fn = (lambda lid, t:
                         resolver.appearance_prob(lid, next(iter(t)) if t else ""))
        return resolver, appearance_fn
    except Exception as e:
        print(f"[cryptids] [warn] rng76 unavailable ({e}); rates blank.")
        return None, None


def build_ctx():
    """Load the shared, page-independent resources ONCE (NPC roster, LVLI tables,
    rng76 engine, Mappalachia DB handle). Pass the result to compute_bundle(ctx=...)
    when building many pages in a row, so the expensive rng76/table load isn't repeated
    per page. Caller must call close_ctx() when done."""
    by_race, by_fid = load_npcs()
    tbls = esources.load_tables()
    resolver, appearance_fn = _load_rng76()
    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
    return {"by_race": by_race, "by_fid": by_fid, "tbls": tbls, "resolver": resolver,
            "appearance_fn": appearance_fn, "db_ok": db_ok, "geo": geo,
            "con": con, "cur": cur}


def close_ctx(ctx):
    if ctx and ctx.get("con"):
        ctx["con"].close()


def compute_bundle(pg, geo_cache_path, keep=None, ctx=None):
    """Compute one cryptid's full content bundle (used_for challenges, events,
    drops, random encounters, fixed-spawn regions) WITHOUT it being a listed page.
    Used by the Honeycomb non-perishable page (Honey Beast) and the Meat family.

    geo_cache_path : this creature's own committed geo cache (CI reads it, no DB).
    keep           : optional {(region,marker): {...spawns:{ref:{slots}}}} to preserve
                     hand-authored photography across rebuilds.
    ctx            : optional shared resources from build_ctx() (reused across many
                     pages). When None, this call loads + closes its own.
    """
    own = ctx is None
    if own:
        ctx = build_ctx()
    by_race, by_fid = ctx["by_race"], ctx["by_fid"]
    tbls, resolver, appearance_fn = ctx["tbls"], ctx["resolver"], ctx["appearance_fn"]
    db_ok, geo, cur = ctx["db_ok"], ctx["geo"], ctx["cur"]

    npcs = page_npcs(pg, by_race, by_fid)

    cache = ebuild.load_cache(geo_cache_path)
    valid = {pg["slug"]}
    for k in [k for k, v in cache.items()
              if isinstance(v, dict) and v.get("page") not in valid]:
        cache.pop(k, None)

    seen = resolve_fixed_spawns([pg], {pg["slug"]: npcs}, geo, cur, cache, db_ok)
    # Tier: Fixed Spawn Locations = GUARANTEED only; Chance to Spawn = weighted rest.
    guaranteed, chance_spawns = tier_spawns(seen[pg["slug"]], cache, pg["slug"], pg["name"],
                                            promote_unique=pg.get("promote_unique_placements", False))
    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        guaranteed, ALL_REGIONS, keep or {})
    label_spawns(regions_out, pg["name"])
    attach_breakdowns(regions_out)
    # Name guaranteed placements after the individual (e.g. the named story cats) when
    # the cache carries a distinct variant name — additive, only affects promoted
    # unique placements (other pages' guaranteed spawns carry no variant).
    for reg in regions_out:
        for loc in reg["locations"]:
            for sp in loc.get("spawns") or []:
                try:
                    rec = cache.get(f"{pg['slug']}:{int(sp['ref'], 16)}")
                except (ValueError, TypeError):
                    rec = None
                if rec and rec.get("variant") and rec["variant"] != pg["name"]:
                    sp["variant"] = rec["variant"]
                    sp["label"] = rec["variant"]

    drops = resolve_drops(pg, npcs, resolver)
    events = resolve_events(pg, [dl["form_id"] for dl in drops["lists"]],
                            tbls, appearance_fn)
    uf = used_for(pg, pg.get("used_for_url", ""))
    res = random_encounters(pg)

    if db_ok:
        ebuild.save_cache(cache, geo_cache_path)
    if own:
        close_ctx(ctx)

    return {
        "name": pg["name"],
        "npc_summary": npc_summary(npcs),
        "used_for": uf,
        "events_activities": events,
        "drops": drops,
        "random_encounters": res,
        "fixed_spawns": {"regions": regions_out, "total_markers": total,
                         "total_placements": placements},
        "chance_spawns": {"locations": chance_spawns,
                          "total": len(chance_spawns),
                          "note": pg.get("chance_note")},
        "_meta": {"source_totals": src_totals, "unresolved": unresolved,
                  "placements": placements},
    }


# ── entry point ──────────────────────────────────────────────────────────────
def run(argv=None):
    os.makedirs(OUT_DIR, exist_ok=True)
    generated = datetime.date.today().isoformat()

    slug_filter = {a for a in (argv or []) if not a.startswith("-") and a != "cryptids"}
    pages = [pg for pg in PAGES if not slug_filter or pg["slug"] in slug_filter]

    verify_roster()
    by_race, by_fid = load_npcs()
    npcs_by_page = {pg["slug"]: page_npcs(pg, by_race, by_fid) for pg in pages}

    tbls = esources.load_tables()

    resolver = appearance_fn = None
    try:
        import rng76
        _data = rng76.Rng76Data.from_tsv_root(TSV)
        resolver = _data.resolver
        appearance_fn = (lambda lid, t:
                         resolver.appearance_prob(lid, next(iter(t)) if t else ""))
        print("[cryptids] rng76 loaded — drop + event rates computed.")
    except Exception as e:
        print(f"[cryptids] [warn] rng76 unavailable ({e}); rates blank.")

    db_ok = os.path.exists(MAPPALACHIA_DB)
    cache = ebuild.load_cache(GEO_CACHE)
    # prune cache entries for pages no longer in the roster (e.g. a page moved to
    # another guide) so the committed cache never carries orphan placements.
    valid_slugs = {pg["slug"] for pg in PAGES}
    for k in [k for k, v in cache.items()
              if isinstance(v, dict) and v.get("page") not in valid_slugs]:
        cache.pop(k, None)
    geo = con = cur = None
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
        print("[cryptids] Mappalachia DB found — resolving placements, refreshing cache.")
    elif cache:
        print(f"[cryptids] No DB — rebuilding from committed geo cache ({len(cache)} placements).")
    else:
        print("[cryptids] No Mappalachia DB and no geo cache — Fixed Spawn Locations "
              "will be empty. Run once locally with MAPPALACHIA_DB set to seed the cache.")

    seen_by_page = resolve_fixed_spawns(pages, npcs_by_page, geo, cur, cache, db_ok)

    hub = []
    for pg in pages:
        npcs = npcs_by_page[pg["slug"]]
        cur_url = f"{URL_BASE}{pg['slug']}/"
        keep = ebuild.load_existing(os.path.join(OUT_DIR, pg["slug"] + ".json"))
        # Tier: Fixed Spawn Locations = GUARANTEED only; Chance to Spawn = weighted rest.
        guaranteed, chance_spawns = tier_spawns(
            seen_by_page[pg["slug"]], cache, pg["slug"], pg["name"])
        regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
            guaranteed, ALL_REGIONS, keep)
        label_spawns(regions_out, pg["name"])
        attach_breakdowns(regions_out)

        drops = resolve_drops(pg, npcs, resolver)
        dl_fids = [dl["form_id"] for dl in drops["lists"]]
        events = resolve_events(pg, dl_fids, tbls, appearance_fn)
        uf = used_for(pg, cur_url)
        res = random_encounters(pg)

        doc = {
            "_meta": {"generated": generated, "source": SOURCE_TAG,
                      "races": [{"edid": rc} for rc in pg.get("races", [])],
                      "source_totals": src_totals, "unresolved": unresolved,
                      "hold": bool(pg.get("hold"))},
            "set": "cryptids", "slug": pg["slug"], "name": pg["name"],
            "page_title": f"Cryptids - {pg['name']}", "url": cur_url,
            "blurb": _blurb(pg, npcs, drops, placements),
            "npc_summary": npc_summary(npcs),
            "used_for": uf,
            "farming_tips": None,          # EMPTY placeholder (Duchess fills later)
            "events_activities": events,
            "drops": drops,
            "random_encounters": res,
            "fixed_spawns": {"regions": regions_out, "total_markers": total,
                             "total_placements": placements},
            "chance_spawns": {"locations": chance_spawns, "total": len(chance_spawns),
                              "note": pg.get("chance_note")},
        }

        # ── assertion: spawns[] must cover every placement (spawn-guide §9k) ──
        bad = [(r["region"], l["marker"]) for r in regions_out for l in r["locations"]
               if len(l.get("spawns") or []) != l["count"]]
        if bad:
            raise AssertionError(f"[{pg['slug']}] spawns/count mismatch at {bad[:5]}")

        out_path = os.path.join(OUT_DIR, pg["slug"] + ".json")
        json.dump(doc, open(out_path, "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)

        drop_rows = sum(l["row_count"] for l in drops["lists"])
        print(f"  {pg['slug']:<20} npcs:{len(npcs):>3} drop-lists:{len(drops['lists'])} "
              f"drop-rows:{drop_rows:>2} used-for:{len(uf):>2} events:{len(events):>2} "
              f"REs:{len(res):>2} fixed:{placements:>4}/{total}m"
              + ("  [HOLD]" if pg.get("hold") else ""))

        hub.append({
            "slug": pg["slug"], "name": pg["name"], "url": cur_url,
            "hold": bool(pg.get("hold")),
            "counts": {"npcs": len(npcs), "drop_rows": drop_rows,
                       "challenges": len(uf), "fixed_spawns": placements,
                       "events": len(events), "random_encounters": len(res)},
        })

    hub_doc = {
        "_meta": {"generated": generated, "source": SOURCE_TAG},
        "name": "Cryptids", "page_title": "Cryptids", "url": URL_BASE,
        "blurb": ("Every cryptid in Fallout 76 — where each one spawns, what it drops, "
                  "and the challenges and events that involve it. Pick a cryptid below."),
        "cryptids": [h for h in hub if not h["hold"]],
        "cryptids_pending": [h for h in hub if h["hold"]],
    }
    # Prune before the hub is written. NOTE: the geo-cache prune further up
    # (valid_slugs) only tidies data/ -- it never touched dist/, which is why
    # beast-of-beckley and honey-beast were still being served after their pages
    # were dropped from PAGES. Keyed on PAGES, not the filtered `pages`.
    prune_outputs(OUT_DIR, [pg["slug"] for pg in PAGES],
                  tag="[cryptids]", skip=bool(slug_filter), also_keep=())

    json.dump(hub_doc, open(HUB_FILE, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(f"[cryptids] wrote {HUB_FILE} ({len(hub_doc['cryptids'])} live, "
          f"{len(hub_doc['cryptids_pending'])} pending) + {len(pages)} page docs.")

    if db_ok:
        ebuild.save_cache(cache, GEO_CACHE)
        print(f"[cryptids] geo cache saved ({len(cache)} placements) for DB-free CI rebuilds.")
    if con:
        con.close()

    try:
        from patchlog_utils import write_empty_patchlog_feed
        write_empty_patchlog_feed("dist", "patchlog_latest_df_cryptids.json",
                                  current_count=len(hub_doc["cryptids"]))
    except Exception:
        pass


def main(argv=None):
    run(argv)


if __name__ == "__main__":
    run(sys.argv)
