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
"""

import os, re, json, sqlite3, datetime
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
CHAL_TSV       = os.environ.get("CHAL_TSV",       os.path.join(REPO, "tsv", "CHAL_Export_June_2026.tsv"))
DIG_DIR        = os.environ.get("DIG_DIR",        os.path.join(REPO, "data", "npc_spawns", "digs"))
OUT_JSON       = os.environ.get("OUT_JSON",       r"C:\Users\Duche\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\json\npc-spawns\npc_spawns.json")

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
    "blurb": "Every location where Blood Eagles spawn, ordered from most to fewest. Blood Eagles are Human enemies, so these spots also work for any Human challenge.",
    "companion_label": "Attack dogs",
    "companion_match": ["bloodeagledog"],          # all substrings must be in the form editorID
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
    "blurb": "Every location where Rust Raiders spawn, ordered from most to fewest. Rust Raiders are Human enemies and only appear in the Burning Springs region.",
    "companion_label": "Deathclaws",
    "companion_match": ["rustraider", "deathclaw"],
    "usedfor_keywords": [],
    "usedfor_name_match": ["rust raider"],
    "interior_region_overrides": {"Rust Kingdom Arena": "Burning Springs"},
    "notes": [
      "Rust Raiders only spawn in the Burning Springs region.",
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
    if not any(n == nm and e == epic for n, e in groups[g]):
      groups[g].append((nm, epic))
  order = ["Score Challenge — Daily", "Score Challenge — Weekly", "Lifetime Challenge", "Challenge Events"]
  return {k: groups[k] for k in order if k in groups}

# ---- build -------------------------------------------------------------------
def main():
  con, cur, rings, markers = load_mappalachia()
  chal = load_chal()
  out = {"_meta": {"generated": datetime.date.today().isoformat(),
                   "source": "Mappalachia dig (counts) + Mappalachia DB (regions, companions) + CHAL export (Used For)"},
         "npcs": {}}
  for cfg in NPCS:
    dig = parse_dig(os.path.join(DIG_DIR, cfg["dig_file"]), cfg["dig_header"])
    comp = companion_counts(cur, markers, cfg["companion_match"])
    locs = []
    for nm, ct in dig["open"]:
      locs.append({"name": nm, "type": "Open World",
                   "region": region_for(cur, rings, nm, cfg["interior_region_overrides"]),
                   "count": ct, "companions": comp.get(nm, 0), "image": ""})
    for nm, ct in dig["interior"]:
      locs.append({"name": nm, "type": "Interior",
                   "region": region_for(cur, rings, nm, cfg["interior_region_overrides"]),
                   "count": ct, "companions": comp.get(nm, 0), "image": ""})
    locs.sort(key=lambda x: -x["count"])
    out["npcs"][cfg["slug"]] = {
      "name": cfg["name"], "page_title": cfg["page_title"], "blurb": cfg["blurb"],
      "credit": CREDIT, "companion_label": cfg["companion_label"],
      "total": dig["total"], "notes": cfg["notes"],
      "used_for": used_for(chal, cfg["usedfor_keywords"], cfg["usedfor_name_match"]),
      "locations": locs,
    }
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
