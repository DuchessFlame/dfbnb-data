#!/usr/bin/env python3
r"""
build_npc_spawn_guides.py — the DF / Score Challenges "NPC Spawns" pages, built in
the Deathclaw Egg layout (spawn-guide skill, §9k), so they render through the same
renderer as every farming page (df-bnb-farming-non-perishable-guide.js).

  out: dist/farming_spawns/npc-<slug>_spawns.json   (+ the dist/pts/ copy)
  geo: data/npc_spawns/geo/<slug>.json               (committed; CI needs no DB)
  url: /df/score-challenges/npc-spawns/<slug>-spawn-locations/

FIXED vs CHANCE (the dedication rule, spawn-guide §9k):

  Fixed Spawn Locations  = 100% this enemy.
      * a placed actor whose base is DEDICATED to the enemy (its race / faction
        says so — LvlBloodEagle, LvlSupermutantMelee, LvlProtectron_Hostile ...),
        corpses, friendlies, vendors, captives and dev records excluded
      * an Encounter Spawn System point (Mappalachia NPC table) where this enemy is
        the ONLY thing in the pool, at weight 1.0
  Chance to Spawn Locations = the point is real, the enemy is a roll.
      * an Encounter Spawn System point whose pool can roll other enemies too
        (a location that is 40% Scorched / 60% Super Mutants)
      * a placed leveled actor that can roll this enemy OR something else — declared
        per page in `chance_bases` (LvlFeralGhoul can roll a Glowing One)

Nothing about WHERE is typed: markers, regions and interiors come from the
Mappalachia Position / NPC tables through spawns_engine.geo, exactly like the meat
and cryptid pages. The per-page curation is only the roster rule (race / faction /
EDID) and the challenge tokens.

  python src/build_npc_spawn_guides.py              # every page
  python src/build_npc_spawn_guides.py scorched     # named pages only
  MAPPALACHIA_DB=... python src/build_npc_spawn_guides.py
"""
import collections
import csv
import html
import datetime
import json
import os
import re
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from spawns_configs import cryptids as C
from spawns_engine import build as ebuild
from spawns_engine.geo import Geo

csv.field_size_limit(10 ** 9)

DIST = os.path.join(REPO, "dist")
OUT_DIR = os.path.join(DIST, "farming_spawns")
PTS_OUT_DIR = os.path.join(DIST, "pts", "farming_spawns")
GEO_DIR = os.path.join(REPO, "data", "npc_spawns", "geo")
OLD_NPC_JSON = os.path.join(DIST, "npc_spawns.json")
URL_ROOT = "/df/score-challenges/npc-spawns/"
MAP_ROOT = "/wp-content/uploads/guide-images/score-challenges/"
MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", C.MAPPALACHIA_DB)
ALL_REGIONS = C.ALL_REGIONS
APPALACHIA_SPACE = C.APPALACHIA_SPACE

# ── roster rules ─────────────────────────────────────────────────────────────
# races / factions: an NPC base matching EITHER is on the page (after the global
# exclusions below). edid: extra EDID regex that also selects. exclude: EDID/race
# regex that removes. pools: Mappalachia NPC-table names (Encounter Spawn System).
# chance_bases: EDID regex of placed leveled bases that can roll this enemy OR
# something else -> Chance to Spawn, never Fixed. tokens: challenge condition /
# name tokens (lower-case substrings) that name THIS page. stats: challenge SNAM
# stats that count this page. group: challenges that name the group (human /
# robot / ghoul) apply to every page in it, unless a sibling's token is present.
HUMAN = "human"
HUMAN_RACES = {"HumanRace", "GhoulRace", "HumanChildRace", "GHL_PlayerGhoulRace"}
ROBOT = "robot"
PAGES = [
    # ── humans ──
    {"slug": "blood-eagle", "name": "Blood Eagle", "plural": "Blood Eagles", "group": HUMAN,
     "factions": ["BloodEagleFaction"], "exclude": r"dog|turret|eyebot|gutsy|protectron",
     "tokens": ["actortypebloodeagle", "bloodeagle", "blood eagle"]},
    {"slug": "cultist", "name": "Cultist", "plural": "Cultists", "group": HUMAN,
     "factions": ["CultistFaction", "StormCultistFaction"], "exclude": r"dog",
     "tokens": ["actortypecultist", "cultist"]},
    {"slug": "fanatic", "name": "Fanatic", "plural": "Fanatics", "group": HUMAN,
     "factions": ["FanaticFaction"], "exclude": r"turret",
     "tokens": ["fanaticfaction", "actortypefanatic", "fanaticconqueror", "fanatic"]},
    {"slug": "communist", "name": "Communist", "plural": "Communists", "group": HUMAN,
     "factions": ["PRCFaction", "Storm_Communist_Faction"], "exclude": r"dog|yaoguai",
     "tokens": ["prcghoul", "communist", "commie"]},
    {"slug": "rust-raider", "name": "Rust Raider", "plural": "Rust Raiders", "group": HUMAN,
     "factions": ["Burn_RustRaiderFaction"], "exclude": r"deathclaw|dog",
     "tokens": ["rustraider", "rust raider"]},
    {"slug": "pint-sized-phantom", "name": "Pint-Sized Phantom",
     "plural": "Pint-Sized Phantoms", "group": HUMAN,
     "edid": r"^SDOW_(Enc|Lvl)SlasherFan", "factions": [],
     "tokens": ["slasherfan", "slasherboss", "pint-sized phantom", "pint-sized slasher",
                "partycrasher"],
     "old_slug": "pint-sized-phantom-spawn-locations"},
    # ── mutants, ghouls and the rest ──
    {"slug": "glowing-one", "name": "Glowing One", "plural": "Glowing Ones",
     "edid": r"GlowingOne|FeralGhoulGlowing", "races": [],
     "chance_bases": r"^(Burn_)?LvlFeralGhoul(Boss)?(_Workshop|_Soldier|_Cop)?$|^GHL_LvlFeralGhoulPointRepose$|^SH003_V51_FeralGhoul$",
     "pools": ["Ghoul"], "pool_chance_only": True,
     "not_words": ["trog", "cryptid", "communist"],
     "tokens": ["actortypeglowing", "glowing one", "glowing creature", "glowing enemy",
                "feralghoulglowingrace", "feral ghoul", "ghoul's"]},
    {"slug": "lost", "name": "Lost", "plural": "Lost",
     "races": ["LostRace", "LostFeralSuiciderRace"], "factions": ["Storm_LostFaction"],
     "pools": ["Lost"], "tokens": ["lostrace", "lostferal", "feral lost", "a lost", "kill lost"]},
    {"slug": "feral-ghoul", "name": "Feral Ghoul", "plural": "Feral Ghouls", "group": "ghoul",
     "races": ["FeralGhoulRace", "FeralGhoulGlowingRace"], "factions": ["FeralGhoulFaction"],
     "exclude": r"lost", "pools": ["Ghoul"], "not_words": ["communist"],
     "tokens": ["feralghoulrace", "feralghoulclass", "actortypeferalghoul", "feral ghoul"],
     "stats": ["Feral Ghouls Killed"]},
    {"slug": "super-mutant", "name": "Super Mutant", "plural": "Super Mutants",
     "races": ["SuperMutantRace", "SupermutantBehemothRace", "SupermutantBehemothBossRace"],
     "factions": ["SuperMutantFaction"], "exclude": r"chicken|protectron|vendor",
     "pools": ["Super Mutant"],
     "tokens": ["supermutantrace", "supermutantclass", "behemoth", "super mutant",
                "actortypesupermutant"],
     "stats": ["Super Mutants Killed"]},
    {"slug": "mole-miner", "name": "Mole Miner", "plural": "Mole Miners",
     "races": ["MoleMinerRace"], "factions": ["MoleMinerFaction"], "exclude": r"vendor",
     "pools": ["Mole Miner"],
     "tokens": ["moleminerrace", "moleminerclass", "moleminertreasurehunter", "mole miner",
                "treasure hunter"],
     "stats": ["Mole Miners Killed"]},
    {"slug": "scorched", "name": "Scorched", "plural": "Scorched",
     "races": ["ScorchedRace"], "edid": r"Scorched",
     "exclude": r"scorchbeast|scorchtongue|critter|^loot_|mirelurk|fogcrawler|cavecricket|noscorch",
     "pools": ["Scorched"],
     "tokens": ["scorchedrace", "actortypescorched", "spookyscorched", "festivescorched",
                "scorched"],
     "stats": ["Scorched Killed"]},
    {"slug": "floater", "name": "Floater", "plural": "Floaters",
     "races": ["FloaterRace"], "factions": ["FloaterFaction"],
     "tokens": ["floaterrace", "floater"]},
    {"slug": "overgrown", "name": "Overgrown", "plural": "Overgrown",
     "races": ["XPD_OvergrownThornRace", "XPD_OvergrownElderRace",
               "XPD_OvergrownPollinatorRace"], "factions": ["XPD_OvergrownFaction"],
     "tokens": ["actortypeovergrown", "overgrown"]},
    {"slug": "trog", "name": "Trog", "plural": "Trogs",
     "races": ["TrogRace"], "factions": ["TrogFaction"],
     "tokens": ["trograce", "actortypetrog", "trog"]},
    # ── robots ──
    {"slug": "assaultron", "name": "Assaultron", "plural": "Assaultrons", "group": ROBOT,
     "races": ["AssaultronRace"], "pools": ["Robot"], "pool_chance_only": True,
     "tokens": ["assaultron"], "stats": ["Assaultrons Destroyed"]},
    {"slug": "sentry-bot", "name": "Sentry Bot", "plural": "Sentry Bots", "group": ROBOT,
     "races": ["SentryBotRace"], "pools": ["Robot"], "pool_chance_only": True,
     "tokens": ["sentrybot", "sentry bot", "senty bot"], "stats": ["Sentry Bots Destroyed"]},
    {"slug": "protectron", "name": "Protectron", "plural": "Protectrons", "group": ROBOT,
     "races": ["ProtectronRace", "ProtectronFastRace"], "exclude": r"vendor",
     "pools": ["Robot"], "pool_chance_only": True, "tokens": ["protectron"]},
    {"slug": "eyebot", "name": "Eyebot", "plural": "Eyebots", "group": ROBOT,
     "races": ["EyeBotRace"], "pools": ["Robot"], "pool_chance_only": True,
     "tokens": ["eyebot"], "stats": ["Eyebots Destroyed"]},
    {"slug": "liberator", "name": "Liberator", "plural": "Liberators", "group": ROBOT,
     "races": ["LiberatorRace", "E09D_LiberatorRace"], "factions": ["LiberatorFaction"],
     "pools": ["Liberator"], "tokens": ["liberator"]},
    {"slug": "robobrain", "name": "Robobrain", "plural": "Robobrains", "group": ROBOT,
     "races": ["DLC01RoboBrainRace"], "pools": ["Robot"], "pool_chance_only": True,
     "tokens": ["robobrain"]},
    {"slug": "mr-handy", "name": "Mr. Handy", "plural": "Mr. Handys and Other Robots",
     "title": "Mr. Handy & Other Robot Spawn Locations", "group": ROBOT,
     "races": ["HandyRace", "VertibirdRace", "VertibotRace", "GuardianBotRace",
               "TurretBubbleRace", "TurretTripodRace", "TurretMilitaryRace",
               "TurretAntiAirRace", "TurretWorkshopRace"],
     "exclude": r"^workshop|essential|nonhostile|turretbubblerobots$|_empty$",
     "pools": ["Robot"], "pool_chance_only": True,
     "tokens": ["handyrace", "mr. handy", "mr handy", "gutsy", "vertibot", "cargobot",
                "turret"],
     "stats": ["Mr. Handys Destroyed", "Turrets Destroyed"]},
]

# Global exclusions — never a hostile, killable spawn.
FRIENDLY_FAC = re.compile(
    r"PlayerFriendFaction|PlayerAllyFaction|Vendor|COMP_|Workshop|W05_Settler|"
    r"CaptiveFaction|BoundCaptive|Denizen_Settler|Civilian|76CharGen|Friendly|"
    r"WhitespringFaction|EN05_|Settler|Dialogue|NWOT_|Carnival|Merchant|Companion|Ally", re.I)
DEAD_EDID = re.compile(r"corpse|_dead|dead_|deactivated|^loot_", re.I)
DEV_EDID = re.compile(r"^(cut|zzz|del|test|qa|debug|dummy|audiotemplate|donotuse)|_test|_delete|delete_|donotuse", re.I)

# Challenge sources on the page: the Daily / Weekly boards (Epic included — those
# are Daily/Weekly records) plus the event and mini-season boards.
CHAL_PAGES = re.compile(r"^(daily|weekly|events|pint-sized-phantoms|season:.*)$")
KILL_WORDS = re.compile(r"kill|destroy|defeat|slay|cripple|critical|damage|headshot|"
                        r"melt|smash|punch|hunt", re.I)
KILL_STATS = re.compile(r"kill|slaughter|crippled|damage|critical|destroyed", re.I)


def slug_url(pg):
    return URL_ROOT + pg["slug"] + "-spawn-locations/"


# ── roster ───────────────────────────────────────────────────────────────────
def load_npc_rows():
    path = C._newest("NPC_Export_*.tsv", exclude=["_Refs", "_PRPS"])
    rows = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            rows.append({
                "formid": (r.get("FormID") or "").strip().upper(),
                "edid": (r.get("EDID") or "").strip(),
                "full": (r.get("FULL") or "").strip(),
                "race": (r.get("RNAM_EDID") or "").strip(),
                "fac": (r.get("Factions_Flat") or ""),
                "inam_fid": (r.get("INAM_FormID") or "").strip().upper(),
                "inam_edid": (r.get("INAM_EDID") or "").strip(),
                "tplt_edid": (r.get("TPLT_EDID") or "").strip(),
                "level": (r.get("ACBS_Level") or "").strip(),
                "min_lvl": (r.get("ACBS_CalcMinLvl") or "").strip(),
                "max_lvl": (r.get("ACBS_CalcMaxLvl") or "").strip(),
                "health": (r.get("DNAM_CalcHealth") or "").strip(),
            })
    print(f"[npc] roster source: {os.path.basename(path)} ({len(rows)} NPC records)")
    return rows


def factions_of(row):
    return [f.split("[")[0].strip() for f in row["fac"].split("|") if f.strip()]


def select_npcs(pg, rows):
    """-> (dedicated rows, chance rows). Race/faction/EDID rule, minus friendlies,
    corpses and dev records. `chance_bases` rows are split out: they are real
    placements that may or may not be this enemy."""
    races = set(pg.get("races") or [])
    facs = set(pg.get("factions") or [])
    edid_re = re.compile(pg["edid"], re.I) if pg.get("edid") else None
    excl = re.compile(pg["exclude"], re.I) if pg.get("exclude") else None
    chance_re = re.compile(pg["chance_bases"], re.I) if pg.get("chance_bases") else None
    ded, chn = [], []
    for r in rows:
        ed = r["edid"]
        if DEV_EDID.search(ed) or DEAD_EDID.search(ed):
            continue
        if FRIENDLY_FAC.search(r["fac"]):
            continue
        if chance_re and chance_re.search(ed):
            chn.append(r)
            continue
        hit = (r["race"] in races) or bool(facs & set(factions_of(r))) or \
              bool(edid_re and edid_re.search(ed))
        if not hit:
            continue
        if excl and (excl.search(ed) or excl.search(r["race"])):
            continue
        # Human factions also field robots, turrets and dogs (Blood Eagle Assaultron,
        # Fanatic Protectron). Those are counted on their own robot / creature pages;
        # a human page keeps only human (and ghoul) race records. Leveled human bases
        # carry a placeholder GhoulRace, which is why ghoul is allowed here.
        if pg.get("group") == HUMAN and r["race"] not in HUMAN_RACES:
            continue
        ded.append(r)
    return ded, chn


# ── placements ───────────────────────────────────────────────────────────────
def resolve(pg, ded, chn, geo, cur, cache):
    """Fill the page's geo cache from the DB and return (fixed_seen, chance_seen),
    each {instance: (x, y, region, marker, source_type)}. Without a DB, rebuild
    both straight from the committed cache."""
    slug = pg["slug"]
    fixed, chance = {}, {}
    if cur is None:
        for key, e in cache.items():
            inst = key.rsplit(":", 1)[-1]
            if not inst.isdigit():
                continue
            tup = (e.get("x"), e.get("y"), e.get("region", ""), e.get("marker", ""),
                   e.get("source_type", "placement"))
            (chance if e.get("tier") == "chance" else fixed)[int(inst)] = tup
        return fixed, chance

    cache.clear()

    def put(inst, space, x, y, stype, tier, ref=None, variant=None, weight=None):
        region, marker, _ = geo.resolve(space, x, y)
        rec = {"page": slug, "base": ref, "space": space,
               "x": round(x, 1) if x is not None else None,
               "y": round(y, 1) if y is not None else None,
               "region": region, "marker": marker, "source_type": stype, "tier": tier}
        if variant:
            rec["variant"] = variant
        if weight is not None:
            rec["weight"] = round(weight, 4)
        cache[f"{slug}:{inst}"] = rec
        tup = (rec["x"], rec["y"], region, marker, stype)
        if tier == "chance":
            chance[inst] = tup
        else:
            fixed[inst] = tup

    def placements(rowset, tier):
        base = {}
        for n in rowset:
            try:
                base[int(n["formid"], 16)] = n.get("full") or ""
            except ValueError:
                pass
        for chunk in C._chunks(list(base)):
            q = ("SELECT x, y, instanceFormID, spaceFormID, referenceFormID FROM Position "
                 "WHERE referenceFormID IN (%s)" % ",".join("?" * len(chunk)))
            for x, y, inst, space, ref in cur.execute(q, tuple(chunk)):
                if inst in fixed or inst in chance:
                    continue
                full = base.get(int(ref)) or ""
                put(inst, space, x, y, "placement" if tier == "fixed" else "chance",
                    tier, ref=int(ref), variant=full or None)

    placements(ded, "fixed")
    placements(chn, "chance")

    # Encounter Spawn System points. One point carries a weighted pool; the enemy is
    # guaranteed there only when it is the pool's sole member at weight 1.0.
    pools = pg.get("pools") or []
    if pools:
        pool_size = dict(cur.execute(
            "SELECT instanceFormID, COUNT(DISTINCT npcName) FROM NPC GROUP BY instanceFormID"))
        for nm in pools:
            q = ("SELECT n.instanceFormID, n.spaceFormID, p.x, p.y, n.spawnWeight "
                 "FROM NPC n JOIN Position p ON p.instanceFormID = n.instanceFormID "
                 "AND p.spaceFormID = n.spaceFormID WHERE n.npcName = ?")
            for inst, space, x, y, sw in cur.execute(q, (nm,)):
                if inst in fixed or inst in chance:
                    continue
                sole = (pool_size.get(inst, 1) or 1) == 1 and sw >= 0.999
                tier = "fixed" if (sole and not pg.get("pool_chance_only")) else "chance"
                put(inst, space, x, y, "spawn" if tier == "fixed" else "chance", tier,
                    weight=sw)
    return fixed, chance


def label_regions(regions_out, pg, cache):
    """Name every per-spawn block after the thing standing there (variant FULL name
    when the base has one, else the enemy), numbered within that name per marker,
    and give each marker its breakdown line."""
    slug, name = pg["slug"], pg["name"]
    for reg in regions_out:
        for loc in reg["locations"]:
            spawns = loc.get("spawns") or []
            names = []
            for sp in spawns:
                try:
                    rec = cache.get(f"{slug}:{int(sp['ref'], 16)}") or {}
                except (TypeError, ValueError):
                    rec = {}
                v = rec.get("variant") or name
                if sp.get("source_type") == "spawn":
                    v = f"{name} spawn point"
                names.append(v)
            tot = collections.Counter(names)
            seen = collections.Counter()
            for sp, v in zip(spawns, names):
                seen[v] += 1
                sp["label"] = f"{v} #{seen[v]}" if tot[v] > 1 else v
            loc["breakdown"] = [{"label": v, "count": n, "source_type": "placement"}
                                for v, n in sorted(tot.items(), key=lambda kv: (-kv[1], kv[0]))]


# ── Used For (challenges) ────────────────────────────────────────────────────
def challenge_rows():
    d = C.load_challenges()
    out = []
    for pk, plist in (d.get("pages") or {}).items():
        if not CHAL_PAGES.match(pk):
            continue
        items = plist if isinstance(plist, list) else (
            plist.get("challenges") or plist.get("items") or [])
        for c in items or []:
            if not isinstance(c, dict) or c.get("is_cut"):
                continue
            name = c.get("full") or ""
            snam = c.get("snam") or ""
            if not (KILL_WORDS.search(name) or KILL_STATS.search(snam)):
                continue
            if re.search(r"camera|photo|picture|collect|consume|craft|build|^complete", name, re.I):
                continue
            hay = " ".join([name, c.get("edid", "")] + list(c.get("conditions_display") or [])
                           + list(c.get("conditions") or [])).lower()
            out.append((pk, c, hay, snam))
    return out


def _has(hay, toks):
    return any(t in hay for t in toks)


def used_for(pg, rows):
    siblings = [p for p in PAGES if p is not pg]
    sib_toks = [t for p in siblings for t in p.get("tokens", [])
                if t not in pg.get("tokens", [])]
    group = pg.get("group")
    group_toks = {HUMAN: ["humanrace", "human enemy", "kill a human", "human's"],
                  ROBOT: ["actortyperobot", "robot"],
                  "ghoul": ["ghoulrace", "ghoul"]}.get(group, [])
    group_stats = {ROBOT: ["Total Robots Killed"]}.get(group, [])
    out, seen = [], set()
    for pk, c, hay, snam in rows:
        mine = _has(hay, pg.get("tokens", [])) or snam in (pg.get("stats") or [])
        grp = (group_toks and _has(hay, group_toks)) or snam in group_stats
        if not mine:
            # A group challenge ("Kill a Human", "Destroy a Robot") counts on every
            # page in the group — unless it names a sibling (a Cultist challenge
            # reads Cultist AND (Human OR Ghoul), which is not a Blood Eagle).
            if not grp or _has(hay, sib_toks):
                continue
        if pg.get("not_words") and any(w in (c.get("full") or "").lower()
                                       for w in pg["not_words"]):
            continue
        key = c.get("form_id") or c.get("edid")
        if key in seen:
            continue
        seen.add(key)
        out.append({"type": C._chal_type(pk, c), "name": c.get("full") or c.get("edid"),
                    "required": c.get("required") or 1, "edid": c.get("edid", ""),
                    "page": pk})
    order = {"Daily": 0, "Weekly": 1, "Event": 2, "Mini Season": 3}
    out.sort(key=lambda r: (order.get(r["type"], 9), r["name"].lower(), r["required"]))
    dedup, keys = [], set()
    for r in out:
        k = (r["type"], r["name"].lower(), r["required"])
        if k in keys:
            continue
        keys.add(k)
        dedup.append(r)
    return dedup


# ── drops (Creatures expand) ─────────────────────────────────────────────────
def flat_drops(drops):
    out, seen = [], set()
    for lst in (drops or {}).get("lists", []):
        for r in lst.get("rows", []):
            if r.get("kind") != "item" or r.get("name") in seen:
                continue
            seen.add(r.get("name"))
            out.append({"name": r.get("name"), "qty": r.get("qty") or 1,
                        "rate_display": r.get("rate_display", ""), "note": ""})
    return out


def variant_rows(ded):
    """The named enemy types on the page (NPC records with a display name) — what
    the Creatures expand shows on an enemy page. Levels are left off: the NPC
    export's ACBS level is a multiplier on leveled actors, not a level."""
    names = sorted({n.get("full") for n in ded if n.get("full")
                    and not re.search(r"template", n.get("full"), re.I)}, key=str.lower)
    return [{"name": nm, "qty": 1, "rate_display": "", "note": ""} for nm in names]


# ── the old npc_spawns.json (hand notes + Pint-Sized Phantom takeover list) ──
def old_pages():
    try:
        return json.load(open(OLD_NPC_JSON, encoding="utf-8")).get("npcs", {})
    except Exception:
        return {}


def phantom_chance(old):
    """Pint-Sized Phantoms are never placed actors: they take over locations tagged
    SDOW_LocEncMainSlashers (read from LCTN by tools/build_npc_spawns.py). Those
    locations carry no map refs, so the chance list is names only, no maps."""
    by = collections.defaultdict(list)
    for loc in (old or {}).get("locations", []):
        by[loc.get("region") or "Unknown"].append({"name": loc.get("name"), "placements": 1})
    regions = [{"region": r, "markers": sorted(m, key=lambda x: (x["name"] or "").lower()),
                "placements": len(m)} for r, m in sorted(by.items())]
    return regions


# ── one page ─────────────────────────────────────────────────────────────────
def existing_slots(path):
    keep = {}
    if not os.path.exists(path):
        return keep
    try:
        doc = json.load(open(path, encoding="utf-8"))
    except Exception:
        return keep
    for reg in doc.get("regions", []):
        for loc in reg.get("locations", []):
            k = (reg.get("region"), loc.get("marker"))
            keep[k] = {f: loc.get(f, "") for f in ("image_top", "directions", "image_bottom")}
            keep[k]["spawns"] = {sp["ref"]: {f: sp.get(f, "") for f in
                                             ("image_top", "directions", "image_bottom")}
                                 for sp in loc.get("spawns", []) if sp.get("ref")}
    return keep


def build_one(pg, ctx):
    slug = pg["slug"]
    out_path = os.path.join(OUT_DIR, f"npc-{slug}_spawns.json")
    geo_path = os.path.join(GEO_DIR, f"{slug}.json")
    keep = existing_slots(out_path)

    ded, chn = select_npcs(pg, ctx["rows"])
    cache = ebuild.load_cache(geo_path)
    fixed, chance = resolve(pg, ded, chn, ctx["geo"], ctx["cur"], cache)
    if ctx["cur"] is not None:
        ebuild.save_cache(cache, geo_path)

    regions_out, src_totals, unresolved, total, placements = ebuild.group_regions(
        fixed, ALL_REGIONS, keep, exclude_types=())
    # Label first: the breakdown line is counted from the per-spawn entries, which a
    # dense page then strips (only blank placeholders go; authored slots stay).
    label_regions(regions_out, pg, cache)
    if placements > ebuild.DENSE_PAGE:
        ebuild.compact_spawns(regions_out)

    chance_block = ebuild.group_chance(chance, ALL_REGIONS, chance_types=("chance",))
    old = ctx["old"].get(pg.get("old_slug") or f"{slug}-spawn-locations") or {}
    if slug == "pint-sized-phantom":
        chance_block = {"regions": phantom_chance(old), "no_maps": True}
        chance_block["total_markers"] = sum(len(r["markers"]) for r in chance_block["regions"])
        chance_block["total"] = chance_block["total_markers"]

    # drops + events + random encounters: the cryptid engine's own functions, fed
    # this page's NPCs (death lists are filtered to the page tokens).
    cpg = {"slug": slug, "name": pg["name"], "tokens": [t.replace(" ", "") for t in
                                                         pg.get("tokens", [])]}
    npcs = [{**n} for n in ded]
    drops = C.resolve_drops(cpg, npcs, ctx["resolver"]) if ctx["resolver"] else {"lists": []}
    try:
        events = C.resolve_events(cpg, [dl["form_id"] for dl in drops["lists"]],
                                  ctx["tbls"], ctx["appearance_fn"]) if ctx["tbls"] else []
    except Exception as e:
        print(f"  [warn] events failed for {slug}: {e}")
        events = []
    res = C.random_encounters({"name": pg["name"], "tokens": pg.get("tokens", []),
                               "races": []})

    name, plural = pg["name"], pg.get("plural") or pg["name"]
    doc = collections.OrderedDict()
    doc["_meta"] = {"generated": datetime.date.today().isoformat(),
                    "source": "Mappalachia Position/NPC tables (cached in "
                              "data/npc_spawns/geo/) + NPC/CHAL exports "
                              "(src/build_npc_spawn_guides.py)",
                    "roster": {"dedicated_bases": len(ded), "chance_bases": len(chn)},
                    "source_totals": src_totals, "unresolved": unresolved}
    doc["set"] = "npc"
    doc["slug"] = "npc-" + slug
    doc["name"] = plural
    doc["page_title"] = pg.get("title") or f"{name} Spawn Locations"
    n_fixed = placements
    doc["blurb"] = (f"Every guaranteed {name} spawn in Fallout 76, grouped by region, "
                    f"plus the places where {plural} share a spawn pool with other enemies.")
    doc["map_base"] = MAP_ROOT + slug + "/"
    doc["breakdown_lead"] = "Spawns here:"

    items = variant_rows(ded)
    doc["drop_rates"] = collections.OrderedDict([
        ("creatures", {"note": (f"The {name} types you can meet:"
                                if items else ""),
                       "items": items}),
        ("collectrons", None),
        ("resource_generators", None),
        ("containers", {"types": []}),
    ])
    doc["used_for"] = collections.OrderedDict([
        ("challenges", used_for(pg, ctx["chal"])),
        ("recipes", None),
    ])
    doc["vendor_list"] = []
    doc["events_activities"] = events
    doc["random_encounters"] = res
    notes = [n for n in (old.get("notes") or [])]
    doc["info_notes"] = ([{"title": "Spawn Notes",
                           "body": "<br>".join(html.escape(n) for n in notes)}] if notes else [])
    doc["regions"] = regions_out
    doc["chance_spawns"] = chance_block
    nm = chance_block.get("total_markers") or 0
    if nm:
        doc["chance_spawns"]["lead"] = (
            f"{plural} can spawn at {nm} location{'' if nm == 1 else 's'} where the "
            f"spawn is shared with other enemies, so what you find there changes between "
            f"visits. They are listed by name only"
            + ("." if chance_block.get("no_maps") else " — open a region map to place them."))
    # NPC score-challenge pages keep their chance-map links. Farming guides show
    # none unless a page opts in with this same key (Duchess, Sept 2026).
    if nm and not chance_block.get("no_maps"):
        doc["chance_spawns"]["show_maps"] = True
    doc["treasure_maps"] = {"maps": []}

    # map links only for regions that actually get a tile (exterior fixed spawns /
    # exterior chance points) — an interior-only region has no tile and would 404.
    tile_regions = sorted({reg["region"] for reg in regions_out for loc in reg["locations"]
                           if any((cache.get(f"{slug}:{int(r, 16)}") or {}).get("space")
                                  == APPALACHIA_SPACE for r in loc.get("refs", []))})
    doc["map_regions"] = tile_regions
    doc["full_map"] = MAP_ROOT + slug + "/" + slug + "-spawn-map-4k.jpg" if n_fixed else ""

    txt = json.dumps(doc, ensure_ascii=False, indent=1)
    os.makedirs(OUT_DIR, exist_ok=True)
    open(out_path, "w", encoding="utf-8").write(txt)
    os.makedirs(PTS_OUT_DIR, exist_ok=True)
    open(os.path.join(PTS_OUT_DIR, os.path.basename(out_path)), "w", encoding="utf-8").write(txt)
    ok = all(len(l.get("spawns") or []) == l["count"] or l.get("spawns_compacted")
             for r in regions_out for l in r["locations"])
    print(f"  npc-{slug:<20} bases:{len(ded):>3}/{len(chn):<3} fixed:{n_fixed:>5} "
          f"markers:{total:>4} chance-markers:{nm:>4} challenges:{len(doc['used_for']['challenges']):>3} "
          f"drops:{len(items):>3} events:{len(events):>2} res:{len(res):>2} "
          f"unresolved:{sum(unresolved.values())} slots-ok:{ok}")
    return out_path


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    want = {a.lower() for a in argv if not a.startswith("-")}
    pages = [p for p in PAGES if not want or p["slug"] in want]
    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(f"file:{MAPPALACHIA_DB}?mode=ro&immutable=1", uri=True)
        cur = con.cursor()
    else:
        print("[npc] no Mappalachia DB — rebuilding from the committed geo caches")
    resolver = appearance_fn = tbls = None
    if "--no-rates" not in argv:
        resolver, appearance_fn = C._load_rng76()
        try:
            from spawns_engine import sources as esources
            tbls = esources.load_tables()
        except Exception as e:
            print(f"[npc] [warn] LVLI tables unavailable ({e}); events blank.")
    ctx = {"rows": load_npc_rows(), "geo": geo, "cur": cur, "old": old_pages(),
           "chal": challenge_rows(), "resolver": resolver,
           "appearance_fn": appearance_fn, "tbls": tbls}
    print(f"[npc] {len(pages)} page(s)")
    for pg in pages:
        build_one(pg, ctx)
    if con:
        con.close()


if __name__ == "__main__":
    main()
