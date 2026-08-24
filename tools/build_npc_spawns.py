#!/usr/bin/env python3
"""
build_npc_spawns.py  —  one generator for the DF / Score Challenges "NPC Spawns" pages.

Produces a single JSON (npc_spawns.json) that the df-bnb-npc-spawns.js renderer reads.
Add a new creature/NPC by dropping its Mappalachia spawn-dig .txt into the digs folder
and adding one entry to NPCS below. Re-run the script — that's it.

Inputs (override any with an env var of the same name):
  MAPPALACHIA_DB   Mappalachia SQLite db        default D:\\Mappalachia\\data\\mappalachia.db
  CHAL_TSV         latest CHAL_Export_*.tsv     default <repo>/tsv/CHAL_Export_June_2026.tsv
  DIG_DIR          folder holding the dig .txts default <repo>/data/npc_spawns/digs
  OUT_JSON         output json                  default <site-data>/json/npc-spawns/npc_spawns.json

Data sources & why:
  - Spawn COUNTS come from the dig .txt (the authoritative "Mappalachia dig"), parsed verbatim.
  - REGION per location is resolved from Mappalachia map-marker coords via point-in-polygon
    against the 8 main region tilings (Forest, Toxic Valley, ... , Burning Springs).
  - COMPANION counts (attack dogs / deathclaws) are tallied from the Mappalachia Position table
    by nearest map marker.
  - "Used For" challenge lists are generated from the CHAL export by keyword/race logic:
    a creature satisfies a challenge whose Target conditions reference one of its keywords/race,
    excluding challenges that require a different faction's actor-type keyword. Cut/dev/atom
    duplicates (CUT_, zzz_, ATOMS_, HTO_) are dropped.
  - SPAWN TYPE (quest gating) per interior is read from the LCTN "LCSR" export
    (LCSR_TSV): placed actors tied to quest combat -> "Quest area"; actors that are
    only extra-during / removed-during a quest get an explanatory note. Open-world
    spawns aren't cell-named in that export, so they stay "Always available".
"""

import os, re, json, sqlite3, datetime
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

_MONTHS = {
  "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
  "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
  "aug": 8, "august": 8, "sep": 9, "september": 9, "oct": 10, "october": 10,
  "nov": 11, "november": 11, "dec": 12, "december": 12,
}

def _pick_latest_export(prefix, suffix=".tsv", fallback=None):
  """Return the newest tsv/<prefix><Month>_<Year><suffix> by (year, month).

  Datamined exports are named per month (e.g. CHAL_Export_July_2026.tsv) and the
  previous month's file is deleted on rollover, so a hardcoded month breaks the
  build every month. Resolve the latest available file at runtime instead. Falls
  back to `fallback` if none match (keeps behaviour explicit if tsv/ is empty)."""
  tsv_dir = os.path.join(REPO, "tsv")
  pat = re.compile(r"^" + re.escape(prefix) + r"([A-Za-z]+)_(\d{4})" + re.escape(suffix) + r"$")
  best = None  # ((year, month), path)
  try:
    for fn in os.listdir(tsv_dir):
      m = pat.match(fn)
      if not m:
        continue
      mon = _MONTHS.get(m.group(1).lower())
      if mon is None:
        continue
      key = (int(m.group(2)), mon)
      if best is None or key > best[0]:
        best = (key, os.path.join(tsv_dir, fn))
  except FileNotFoundError:
    pass
  return best[1] if best else fallback

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
CHAL_TSV       = os.environ.get("CHAL_TSV",       _pick_latest_export("CHAL_Export_", ".tsv",      os.path.join(REPO, "tsv", "CHAL_Export_June_2026.tsv")))
NPC_TSV        = os.environ.get("NPC_TSV",        _pick_latest_export("NPC_Export_",  ".tsv",      os.path.join(REPO, "tsv", "NPC_Export_June_2026.tsv")))
LCSR_TSV       = os.environ.get("LCSR_TSV",       _pick_latest_export("LCTN_Export_", "_LCSR.tsv", os.path.join(REPO, "tsv", "LCTN_Export_June_2026_LCSR.tsv")))
DIG_DIR        = os.environ.get("DIG_DIR",        os.path.join(REPO, "data", "npc_spawns", "digs"))
OUT_JSON       = os.environ.get("OUT_JSON",       os.path.join(REPO, "dist", "npc_spawns.json"))
# DB-derived per-location geo (region + companions) is cached here so the patch
# build can rebuild every 6h without the (large, local-only) Mappalachia DB.
GEO_CACHE      = os.environ.get("GEO_CACHE",      os.path.join(REPO, "data", "npc_spawns", "geo_cache.json"))

CREDIT = "With thanks to Nerditbabe and Mappalachia."

# Shared "Human" identity (Blood Eagles, Rust Raiders, Cultists ... are all Human race).
HUMANRACE, HUMANLIKE, HUMANKW = "00013746", "005F1198", "0002CB72"
HUMAN_IDS = {HUMANRACE, HUMANLIKE, HUMANKW}

# ---- per-NPC config. Add new creatures here. ----------------------------------
NPCS = [
  {
    "slug": "blood-eagle-spawn-locations",
    "name": "Blood Eagle",
    "page_title": "Blood Eagle Spawn Locations",
    "dig_file": "BloodEagle_RustRaider_spawns.txt",
    "dig_header": "# BLOOD EAGLE",
    "blurb": "Every location where Blood Eagles spawn, ordered from most to fewest.",
    "category": "Score Challenges",
    # keywords (Race + Faction) are derived generatively from the NPC export — see derive_keywords()
    "companion_label": "Attack dogs",
    "companion_match": ["bloodeagledog"],          # all substrings must be in the form editorID
    "enemy_lcsr": ("lvlbloodeagle", "dog"),        # (match, exclude) for the LCSR quest-gating pass
    "usedfor_keywords": ["00571D9F"],              # ActorTypeBloodEagle
    "usedfor_name_match": [],
    "interior_region_overrides": {"Little Rob's Hideout": "Savage Divide", "High Knob Fire Tower": "Skyline Valley"},
    "notes": [
      "Most Blood Eagle spawns show a Blood Eagle icon on the map.",
      "Most Blood Eagle camps spawn in two waves. Clear the first wave undetected and the second won't spawn. To force the second wave, let a Blood Eagle reach the alarm bell and ring it.",
    ],
  },
  {
    "slug": "rust-raider-spawn-locations",
    "name": "Rust Raider",
    "page_title": "Rust Raider Spawn Locations",
    "dig_file": "BloodEagle_RustRaider_spawns.txt",
    "dig_header": "# RUST RAIDER",
    "blurb": "Every location where Rust Raiders spawn, ordered from most to fewest.",
    "category": "Score Challenges",
    # keywords (Race + Faction) are derived generatively from the NPC export — see derive_keywords()
    "companion_label": "Deathclaws",
    "companion_match": ["rustraider", "deathclaw"],
    "enemy_lcsr": ("lvlrustraider", "deathclaw"),  # (match, exclude) for the LCSR quest-gating pass
    "usedfor_keywords": [],
    "usedfor_name_match": ["rust raider"],
    "interior_region_overrides": {"Rust Kingdom Arena": "Burning Springs"},
    "notes": [
      "Rust Raiders only spawn in the Burning Springs region.",
    ],
  },
  {
    # PRE-STAGED: no Mappalachia dig exists yet. The build loop skips any entry
    # whose dig_file is absent, so this emits NO page until
    # data/npc_spawns/digs/PintSizedPhantom_spawns.txt is added. The instant that
    # dig exists (with a "# PINT-SIZED PHANTOM" section header) this page builds.
    "slug": "pint-sized-phantom-spawn-locations",
    "name": "Pint-Sized Phantom",
    "page_title": "Pint-Sized Phantom Spawn Locations",
    "dig_file": "PintSizedPhantom_spawns.txt",
    "dig_header": "# PINT-SIZED PHANTOM",
    "blurb": "Every location where Pint-Sized Phantoms spawn, ordered from most to fewest.",
    "category": "Score Challenges",
    # Race + Faction are derived generatively from the NPC export. The family's
    # internal identity is "Slasher" (SDOW_EncSlasherFan* / SDOW_LvlSlasherFan*).
    "npc_name_match": ["slasher"],
    # Pint-Sized Phantoms have no tracked companion creatures; sentinel matches
    # nothing so companion_counts() returns empty (never pass [] here — that
    # matches every entity).
    "companion_label": "Companions",
    "companion_match": ["__none__"],
    # ActorType keywords: SDOW_ActorTypeSlasherFan / SDOW_ActorTypeSlasherBoss.
    "usedfor_keywords": ["008E065B", "008E0665"],
    "usedfor_name_match": [],
    "interior_region_overrides": {},
    "notes": [
      "Pint-Sized Phantoms are the \"Slasher\" enemy family (NPC records SDOW_EncSlasherFan* / SDOW_LvlSlasherFan*).",
    ],
  },
]

# ---- Mappalachia: regions + markers + companion tally -------------------------
REGION_FAMILIES = {
  "ForestSubRegion": "Forest", "ToxicValleySubRegion": "Toxic Valley",
  "CranberrySubRegion": "Cranberry Bog", "SwampSubRegion": "The Mire",
  "MountainSubRegion": "Savage Divide", "MountainRemovalSubRegion": "Ash Heap",
  "StormSubRegion": "Skyline Valley", "BurningSpringsSubRegion": "Burning Springs",
}
WORLDSPACE = 2480661

def load_mappalachia():
  con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
  fam_ids = defaultdict(list)
  for fid, eid in cur.execute("SELECT regionFormID, regionEditorID FROM Region"):
    m = re.match(r"([A-Za-z]+SubRegion)\d", eid or "")
    if m and m.group(1) in REGION_FAMILIES:
      fam_ids[REGION_FAMILIES[m.group(1)]].append(fid)
  rings = {}
  for reg, ids in fam_ids.items():
    rr = defaultdict(list)
    q = ("SELECT regionFormID, subRegionIndex, coordIndex, x, y FROM RegionPoints "
         f"WHERE regionFormID IN ({','.join(map(str, ids))}) ORDER BY regionFormID, subRegionIndex, coordIndex")
    for rfid, si, ci, x, y in cur.execute(q):
      rr[(rfid, si)].append((x, y))
    rings[reg] = [v for v in rr.values() if len(v) >= 3]
  markers = [(l, x, y) for (x, y, l) in
             cur.execute("SELECT x, y, label FROM MapMarker WHERE spaceFormID=? AND label<>''", (WORLDSPACE,))]
  return con, cur, rings, markers

def pip(rings_list, px, py):
  inside = False
  for ring in rings_list:
    n = len(ring); j = n - 1
    for i in range(n):
      xi, yi = ring[i]; xj, yj = ring[j]
      if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / ((yj - yi) or 1e-9) + xi):
        inside = not inside
      j = i
  return inside

def marker_xy(cur, name):
  r = cur.execute("SELECT x, y FROM MapMarker WHERE lower(trim(label))=lower(trim(?)) AND spaceFormID=?",
                  (name, WORLDSPACE)).fetchone()
  return r

def region_for(cur, rings, name, overrides):
  if name in overrides:
    return overrides[name]
  xy = marker_xy(cur, name)
  if xy:
    for reg, rl in rings.items():
      if pip(rl, xy[0], xy[1]):
        return reg
  return overrides.get(name, "")

def companion_counts(cur, markers, match_subs):
  forms = [f for f, e in cur.execute("SELECT entityFormID, editorID FROM Entity")
           if e and all(s in e.lower() for s in match_subs)]
  if not forms:
    return Counter()
  def nearest(px, py):
    best, bd = None, 1e30
    for l, x, y in markers:
      d = (x - px) ** 2 + (y - py) ** 2
      if d < bd: bd, best = d, l
    return best
  c = Counter()
  qs = ",".join("?" * len(forms))
  for x, y in cur.execute(f"SELECT x, y FROM Position WHERE spaceFormID=? AND referenceFormID IN ({qs})",
                          (WORLDSPACE, *forms)):
    c[nearest(x, y)] += 1
  return c

# ---- quest gating (from the location-reference / LCSR export) -----------------
# A spawn page lists every place an enemy is *placed*, but some of those placed
# actors only matter during a quest (or are removed during one). We read the LCTN
# "LCSR" export (placed-actor location references) and, per interior cell, decide
# whether the spawns there are normal, quest-tied, or quest-removed. Open-world
# spawns aren't cell-named in this export, so only interiors are classified;
# everything else stays "Always available".
QUEST_NAMES = [
  ("BS02",               "Steel Reign"),
  ("BS01",               "Steel Dawn"),
  ("COMP_Outro_Beckett", "Beckett's questline"),
  ("AC_SQ",              "a side quest"),
  ("Storm",              "a Skyline Valley quest"),
]
_QUEST_RE = re.compile(r"^(BS0\d|AC_SQ\d|COMP_Outro|Storm_|W0\d|EN\d|E0\d|MQ\d|SQ\d)")

def _quest_name(refs):
  for pre, nm in QUEST_NAMES:
    if any(r.startswith(pre) for r in refs):
      return nm
  return "a quest"

def quest_flags(match_sub, exclude_sub):
  """Return {interior_cell_name: {"spawn_type":.., "spawn_note":..}} for one enemy.
     - 'Quest area'  : the majority of placed actors here are tied to quest combat.
     - 'Always available' + note : a minority are quest extras, OR the actors are
       only *removed* during a quest (disable refs)."""
  import csv
  try:
    f = open(LCSR_TSV, encoding="utf-8", errors="replace")
  except Exception as e:
    print(f"[npc_spawns] WARN: no LCSR export ({LCSR_TSV}) — skipping quest gating: {e}")
    return {}
  cells = defaultdict(lambda: {"all": set(), "active": set(), "disable": set(),
                               "arefs": set(), "drefs": set()})
  rd = csv.reader(f, delimiter="\t"); next(rd, None)
  for row in rd:
    if len(row) < 7:
      continue
    loctype, disp = row[4], row[6]
    low = disp.lower()
    if match_sub not in low or exclude_sub in low:
      continue
    cm = re.search(r'in [A-Za-z0-9_]+ "([^"]+)"', disp)   # quoted interior cell name
    if not cm:
      continue                                            # open-world ref, no named cell
    cell = cm.group(1)
    am = re.search(r"\[(?:ACHR|REFR):([0-9A-F]+)\]", disp)
    fid = am.group(1) if am else disp[:18]
    d = cells[cell]; d["all"].add(fid)
    if not _QUEST_RE.match(loctype):
      continue
    tl = loctype.lower()
    if "nonquest" in tl:                                  # explicitly the non-quest population
      continue
    if "disable" in tl:
      d["disable"].add(fid); d["drefs"].add(loctype)
    else:
      d["active"].add(fid);  d["arefs"].add(loctype)
  out = {}
  for cell, d in cells.items():
    tot, act, dis = len(d["all"]), len(d["active"]), len(d["disable"])
    if act and tot and act / tot >= 0.5:
      q = _quest_name(d["arefs"])
      out[cell] = {"spawn_type": "Quest area",
                   "spawn_note": f"This is part of the {q} questline — these spawns may only "
                                 f"appear during the quest, and the area may not be freely "
                                 f"accessible otherwise."}
    elif act:
      q = _quest_name(d["arefs"])
      out[cell] = {"spawn_type": "Always available",
                   "spawn_note": f"Most spawns here are always present; some extra appear during {q}."}
    elif dis:
      q = _quest_name(d["drefs"])
      out[cell] = {"spawn_type": "Always available",
                   "spawn_note": f"These spawns are temporarily removed during {q}."}
  return out

# ---- dig parsing -------------------------------------------------------------
def parse_dig(path, header):
  text = open(path, encoding="utf-8", errors="replace").read()
  block = text.split(header, 1)[1] if header in text else ""
  # cut at the next "# " section header
  nxt = re.search(r"\n#{3,}\n# ", block)
  if nxt: block = block[:nxt.start()]
  out = {"open": [], "interior": [], "total": 0}
  m = re.search(r"Total placed spawn references in the world:\s*(\d+)", block)
  if m: out["total"] = int(m.group(1))
  section = None
  for line in block.splitlines():
    if "OPEN-WORLD SPAWNS" in line: section = "open"; continue
    if "INTERIOR" in line and "LOCATIONS" in line: section = "interior"; continue
    if line.startswith("BASE NPC FORMS"): section = None; continue
    mm = re.match(r"\s+(\d+)\s{2,}(.+?)\s*$", line)
    if mm and section:
      out[section].append((mm.group(2).strip(), int(mm.group(1))))
  return out

# ---- CHAL "Used For" ---------------------------------------------------------
def load_chal():
  import csv
  return list(csv.DictReader(open(CHAL_TSV, encoding="utf-8", errors="replace"), delimiter="\t"))

def conds(r):
  return [r.get(f"Cond{i}") or "" for i in range(1, 53) if r.get(f"Cond{i}")]

def humanize(token):
  """Split a camelCase / digit-run editor token into spaced words.
     'ScienceOfLove' -> 'Science Of Love', 'RustRaider' -> 'Rust Raider'."""
  words = re.findall(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|\d+", token or "")
  return " ".join(words).strip() or (token or "")

def season_from_edid(ed):
  """Mini-season / seasonal-event name lives in the challenge EDID, e.g.
     ATX_DE2025_Halloween_Week2_Challenge_...  -> 'Halloween'
     DE2026_SockHop_Challenge_...              -> 'Sock Hop'
     ATX_DE2024_ScienceOfLove_Week1_...        -> 'Science Of Love'
     Returns '' when no DE<year>_<Event> token is present."""
  m = re.search(r"DE\d{4}_([A-Za-z0-9]+)", ed or "")
  return humanize(m.group(1)) if m else ""

# ---- generative keywords (Race + Faction) from the NPC export -----------------
def load_npc_tsv():
  import csv
  try:
    return list(csv.DictReader(open(NPC_TSV, encoding="utf-8", errors="replace"), delimiter="\t"))
  except Exception as e:
    print(f"[npc_spawns] WARN: could not read NPC_TSV ({NPC_TSV}): {e}")
    return []

# variants/companions/corpses we don't want skewing the creature's own Race
_KW_EXCLUDE = ("corpse", "attack dog", "deathclaw", " dog")

def derive_keywords(npc_rows, name, name_match=None, exclude=_KW_EXCLUDE):
  """Race + Faction for a creature, read straight from the NPC export.
     - rows are matched by name (defaults to the creature name, lowercased)
     - the creature's own faction = the most common Factions_Flat among matches
     - Race = most common RNAM_Name among rows carrying that faction
     - Faction is humanised: strip a leading region prefix (e.g. 'Burn_') and the
       trailing 'Faction', then camelCase-split. 'Burn_RustRaiderFaction' -> 'Rust Raider'."""
  matches = [m.lower() for m in (name_match or [name.lower()])]
  rows = []
  for r in npc_rows:
    full = (r.get("FULL") or "").lower()
    if not any(m in full for m in matches):
      continue
    if any(x in full for x in exclude):
      continue
    rows.append(r)
  if not rows:
    return []
  def fac_edid(r):
    return (r.get("Factions_Flat") or "").split("[")[0].split(",")[0].strip()
  fac_counter = Counter(fac_edid(r) for r in rows if fac_edid(r))
  faction_edid = fac_counter.most_common(1)[0][0] if fac_counter else ""
  race_counter = Counter(
    (r.get("RNAM_Name") or "").strip()
    for r in rows
    if (not faction_edid or fac_edid(r) == faction_edid) and (r.get("RNAM_Name") or "").strip()
  )
  race = race_counter.most_common(1)[0][0] if race_counter else ""
  fac = re.sub(r"Faction$", "", faction_edid)
  fac = re.sub(r"^[A-Za-z][A-Za-z0-9]*_", "", fac)   # drop a leading region/DLC prefix e.g. 'Burn_'
  faction = humanize(fac)
  return [k for k in (race, faction) if k]

def used_for(chal, keywords, name_match):
  allowed = set(keywords) | HUMAN_IDS
  def human_ok(r):
    human = foreign = False
    for c in conds(r):
      if "Target" not in c: continue
      fn = c.split("|")[2] if len(c.split("|")) > 2 else ""
      if fn not in ("HasKeyword", "GetIsRace"): continue
      for t in re.findall(r"\[(?:KYWD|RACE):([0-9A-F]{8})\]", c):
        if t in HUMAN_IDS: human = True
        elif t in keywords: pass
        elif ("ActorType" in c) or fn == "GetIsRace":
          if t not in allowed: foreign = True
    return human and not foreign
  def kw_hit(r):
    return any("Target" in c and any(k in c for k in keywords) for c in conds(r))
  def cat(ed):
    e = ed or ""
    if e.startswith(("CUT_", "zzz_", "ATOMS_", "HTO_")): return None
    if e.startswith("SCORE_Challenge_Daily") or e.startswith("Burn_Challenge_Daily"): return "Score Challenge — Daily"
    if e.startswith("SCORE_Challenge_Weekly") or e.startswith("Burn_Challenge_Weekly"): return "Score Challenge — Weekly"
    if "Lifetime" in e: return "Lifetime Challenge"
    if e.startswith("ATX_") or "SockHop" in e or e.startswith("DE20"): return "Challenge Events"
    return None
  groups = defaultdict(list)
  for r in chal:
    ed, full = r.get("EDID") or "", r.get("FULL") or ""
    if not full: continue
    match = kw_hit(r) or human_ok(r) or any(nm in full.lower() for nm in name_match)
    if not match: continue
    g = cat(ed)
    if not g: continue
    epic = full.lower().startswith("epic -") or ed.endswith("_Epic")
    nm = re.sub(r"^[Ee]pic - ", "", full).strip()
    try:
      count = int(float(r.get("TNAM") or 0))
    except (TypeError, ValueError):
      count = 0
    season = season_from_edid(ed) if g == "Challenge Events" else ""
    entry = {"name": nm, "epic": epic, "count": count, "season": season}
    if entry not in groups[g]:
      groups[g].append(entry)
  order = ["Score Challenge — Daily", "Score Challenge — Weekly", "Lifetime Challenge", "Challenge Events"]
  return {k: groups[k] for k in order if k in groups}

# ---- geo cache (region + companions) -----------------------------------------
def load_geo_cache():
  try:
    return json.load(open(GEO_CACHE, encoding="utf-8"))
  except Exception:
    return {}

def save_geo_cache(cache):
  os.makedirs(os.path.dirname(GEO_CACHE), exist_ok=True)
  json.dump(cache, open(GEO_CACHE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

# ---- build -------------------------------------------------------------------
def main():
  # The Mappalachia DB is large and local-only. When present (local runs) we
  # compute regions/companions fresh AND refresh geo_cache.json. When absent
  # (GitHub Actions) we fall back to the committed cache so the patch build can
  # still rebuild dig counts + "Used For" every 6h.
  db_ok = os.path.isfile(MAPPALACHIA_DB)
  if db_ok:
    con, cur, rings, markers = load_mappalachia()
    print(f"[npc_spawns] Mappalachia DB found — computing fresh regions/companions, refreshing geo cache.")
  else:
    cur = rings = markers = None
    print(f"[npc_spawns] Mappalachia DB not found at {MAPPALACHIA_DB} — CI mode: using committed geo cache ({GEO_CACHE}).")
  cache = load_geo_cache()
  chal = load_chal()
  npc_rows = load_npc_tsv()
  out = {"_meta": {"generated": datetime.date.today().isoformat(),
                   "source": "Mappalachia dig (counts) + Mappalachia DB (regions, companions) + CHAL export (Used For) + LCSR export (quest gating)"},
         "npcs": {}}
  for cfg in NPCS:
    slug = cfg["slug"]
    dig_path = os.path.join(DIG_DIR, cfg["dig_file"])
    if not os.path.isfile(dig_path):
      print(f"[npc_spawns] SKIP {slug}: dig file not found ({dig_path}). "
            f"Add the Mappalachia dig to build this page.")
      continue
    dig = parse_dig(dig_path, cfg["dig_header"])
    comp = companion_counts(cur, markers, cfg["companion_match"]) if db_ok else None
    qflags = quest_flags(*cfg["enemy_lcsr"]) if cfg.get("enemy_lcsr") else {}
    slug_cache = cache.get(slug, {})
    fresh_cache = {}
    locs = []
    for section, typ in (("open", "Open World"), ("interior", "Interior")):
      for nm, ct in dig[section]:
        if db_ok:
          region = region_for(cur, rings, nm, cfg["interior_region_overrides"])
          companions = comp.get(nm, 0)
        else:
          cached = slug_cache.get(nm, {})
          region = cached.get("region", cfg["interior_region_overrides"].get(nm, ""))
          companions = cached.get("companions", 0)
        fresh_cache[nm] = {"region": region, "companions": companions}
        qinfo = qflags.get(nm) if typ == "Interior" else None
        locs.append({"name": nm, "type": typ, "region": region,
                     "count": ct, "companions": companions, "image": "",
                     "spawn_type": (qinfo or {}).get("spawn_type", "Always available"),
                     "spawn_note": (qinfo or {}).get("spawn_note", "")})
    locs.sort(key=lambda x: -x["count"])
    cache[slug] = fresh_cache if db_ok else slug_cache
    out["npcs"][slug] = {
      "name": cfg["name"], "page_title": cfg["page_title"], "blurb": cfg["blurb"],
      "category": cfg.get("category", "Score Challenges"),
      "keywords": cfg.get("keywords") or derive_keywords(npc_rows, cfg["name"], cfg.get("npc_name_match")),
      "credit": CREDIT, "companion_label": cfg["companion_label"],
      "total": dig["total"], "notes": cfg["notes"],
      "used_for": used_for(chal, cfg["usedfor_keywords"], cfg["usedfor_name_match"]),
      "locations": locs,
    }
  if db_ok:
    save_geo_cache(cache)
  os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
  json.dump(out, open(OUT_JSON, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
  for slug, d in out["npcs"].items():
    miss = [l["name"] for l in d["locations"] if not l["region"]]
    print(f"{slug}: {len(d['locations'])} locations, total {d['total']}, "
          f"{sum(1 for l in d['locations'] if l['companions'])} with {d['companion_label'].lower()}, "
          f"used-for groups {[(g, len(v)) for g, v in d['used_for'].items()]}"
          + (f"  MISSING REGION: {miss}" if miss else ""))
  print("wrote", OUT_JSON)

if __name__ == "__main__":
  main()
