#!/usr/bin/env python3
"""
build_bounty_hunting_json.py — Bounty Hunting Challenges
Reads the latest CHAL_Export_*.tsv and produces:
  dist/challenges/bounty_hunting_challenges.json

Structure mirrors the grunt-hunt-rewards and head-hunt-rewards page slugs.
Groups: chain challenges, META→SUB nesting, CAMP challenges, cut content.
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT       = SCRIPT_DIR.parent
TSV_DIR    = ROOT / "tsv"
OUT_DIR    = ROOT / "dist" / "challenges"
OUT_FILE   = OUT_DIR / "bounty_hunting_challenges.json"


# ── Cut-content detection ─────────────────────────────────────────────────────
_CUT_PREFIXES = ("zzz", "zzzz", "cut_", "del_", "post_")

def is_cut_edid(edid: str) -> bool:
    return str(edid).lower().startswith(_CUT_PREFIXES)


# ── Hardcoded FormID structure ─────────────────────────────────────────────────
# The TSV supplies name / targetCount / type / category.
# The hierarchy (what belongs on which page, parent→child) is fixed game data.

# ---- Grunt Hunt page (grunt-hunt-rewards) ----

GRUNT_DISCOVER    = ["00848D01"]          # Become a Bounty Hunter in Highway Town
GRUNT_CHAIN       = [                      # Complete Grunt Hunts (ordered tiers)
    "00843137",   # 10
    "0084313D",   # 50
    "00843104",   # 76
    "0084312B",   # 100
    "0084312C",   # 250
]
GRUNT_CAMP        = [                      # CAMP challenges — kept separate despite zzz prefix
    "008433CA",   # Build Bounty Posters
    "008433CC",   # Build Bounty Hunting Flare Mortar
]
GRUNT_CUT         = [                      # zzz-prefixed cut content for grunt hunts
    "0084314A",   # Complete Grunt Hunts ×15
    "00843138",   # Complete 1★ Grunt Hunts ×50
    "0084313C",   # Complete 2★ Grunt Hunts ×50
]

# ---- Head Hunt page (head-hunt-rewards) ----

HEAD_START_CHAIN    = [                    # Start Head Hunts (ordered tiers)
    "00843119",   # 1
    "00843110",   # 5
    "0084312D",   # 7
    "0084313A",   # 15
    "00843146",   # 76
]
HEAD_COMPLETE_CHAIN = [                    # Complete Head Hunts (ordered tiers)
    "00843147",   # 10
    "0084313F",   # 50
    "0084310D",   # 76
    "0084312E",   # 100
    "00843115",   # 250
]

# Hunt Locations: META → SUBs (same META covers both grunt + head, placed on head page)
LOCATION_META     = "00843106"
LOCATION_SUBS     = [
    "00843143",   # Athens (covers Athens Armory + Athens Ruins via CNDF)
    "00843139",   # Ash Cave
    "00843127",   # The Chop Shop
    "00843144",   # Dino Peaks Mini Golf
    "0084311B",   # Fort Steuben
    "00843109",   # Railroad Service Yard
    "00843122",   # The Rust Kingdom
    "0084310E",   # Super Duper Mart
]

# Head Hunt Groups: All-META → Group METAs → Kill-target SUBs
HEAD_GROUPS_ALL_META = "0084312F"
HEAD_GROUPS = [
    # (groupNum, META_formId, [sub_formIds in display order])
    (1, "0084310A", [
        "00843141",   # The Pale Horseman  (Death)    gangId 0
        "00843111",   # The Black Horseman (Famine)   gangId 6
        "00843107",   # The White Horseman (Pest)     gangId 10
        "00843120",   # The Red Rider      (War)      gangId 12
    ]),
    (2, "00843134", [
        "0084310F",   # The Cleaner              (Abraxo)       gangId 26
        "00843117",   # Anna The Nuka-Queen       (Nuka)         gangId 9
        "00843133",   # The Malpractitioner       (Quack Docs)   gangId 4
        "0084311D",   # Irene The Irradiated      (Rad Cult)     gangId 1
        "00843118",   # Cletus Brimstone          (Rad Cleaners) gangId 3
    ]),
    (3, "00843145", [
        "00843113",   # The Space Ranger       (Astronauts)  gangId 2
        "00843131",   # The Proletariat Punisher (Commies)   gangId 28
        "0084313E",   # Ragtime Randy          (Jazz)        gangId 15
        "00843114",   # The Ace                (Pilot)       gangId 11
        "00843108",   # The Chief Researcher   (Vault-Tec)   gangId 19
    ]),
    (4, "0084311A", [
        "00843132",   # Baron Boris Wazie      (Old Money)   gangId 8
        "0084310B",   # Corporal Jane          (Patriots)    gangId 13
        "00843124",   # Chief Engineer Lewis   (Robobrains)  gangId 20
        "00843105",   # Tincan Toni            (Robots)      gangId 16
    ]),
    (5, "00843121", [
        "00843136",   # Richie Finesse         (Big Gun)     gangId 25
        "0084313B",   # Becca The Heavyweight  (Boxer)       gangId 23
        "0084310C",   # Cowgirl Janine         (Gunslinger)  gangId 22
        "00843125",   # Charlie Half-Cocked    (Sniper)      gangId 24
    ]),
    (6, "0084311C", [
        "00843129",   # The Devil of Defiance  (Blue Devils) gangId 5
        "0084311E",   # Vito "The Vic" Bronco  (Gambler)     gangId 17
        "00843149",   # Granny Dolores         (Golden)      gangId 7
        "00843148",   # Colt the Bolt          (Hunter)      gangId 29
    ]),
    (7, "0084311F", [
        "00843142",   # Gentle Gary            (Crusher)     gangId 27
        "00843116",   # Amadi the Piranha      (Fisherman)   gangId 21
        "00843123",   # The Foreman            (Miner)       gangId 18
        "00843126",   # Scout Leader Karen     (Scout)       gangId 14
    ]),
]

HEAD_CUT = [
    "00843130",   # Complete Head Hunts ×76 (zzz duplicate)
    "0084312A",   # Complete Bounty Hunts ×3
    "00843140",   # Complete Bounty Hunts ×5
    "00843128",   # Complete Bounty Hunts ×7
    "00843135",   # Complete Bounty Hunts ×15
    "00843112",   # Complete Bounty Hunts ×76
]


# ── TSV helpers ───────────────────────────────────────────────────────────────

def find_latest_tsv(pattern: str) -> Path | None:
    files = sorted(glob.glob(str(TSV_DIR / pattern)))
    return Path(files[-1]) if files else None


def _to_int(v: str) -> int:
    try:
        return int(str(v or "0").strip())
    except (ValueError, TypeError):
        return 0


def _parse_reward(jasf: str) -> str | None:
    """Extract reward EDID from JASF field.  Format: Reward0:FormID:EDID:GMRW"""
    if not jasf or str(jasf).strip() in ("", "NONE", " NONE"):
        return None
    m = re.search(r"Reward\d+:[0-9A-Fa-f]+:([^:\s]+):", str(jasf))
    return m.group(1) if m else None


def parse_chal_tsv(path: Path) -> dict[str, dict]:
    """Return {FORMID_UPPER: row_dict} for every row in the TSV."""
    records: dict[str, dict] = {}
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            fid = str(row.get("FormID", "") or "").strip().upper()
            if not fid:
                continue
            records[fid] = {
                "formId":      fid,
                "edid":        str(row.get("EDID",  "") or "").strip(),
                "name":        str(row.get("FULL",  "") or "").strip(),
                "snam":        str(row.get("SNAM",  "") or "").strip(),
                "targetCount": _to_int(row.get("TNAM", "0")),
                "type":        str(row.get("CNAM",  "") or "").strip(),
                "category":    str(row.get("ENAM",  "") or "").strip(),
                "reward":      _parse_reward(row.get("JASF", "")),
            }
    return records


# ── Challenge object builder ──────────────────────────────────────────────────

def make_challenge(row: dict, force_live: bool = False) -> dict:
    """Build a normalised challenge object from a TSV row.
    force_live=True: treat as live content regardless of EDID prefix (e.g. CAMP items)."""
    cut = False if force_live else is_cut_edid(row["edid"])
    return {
        "formId":      row["formId"],
        "edid":        row["edid"],
        "name":        row["name"] or row["edid"],
        "snam":        row["snam"],       # stat label e.g. "Quests Completed"
        "targetCount": row["targetCount"],
        "type":        row["type"],       # "Lifetime" / "Daily" / "Weekly"
        "category":    row["category"],   # "Burning Springs" / "Sub Challenge (Unsorted)"
        "cutContent":  cut,
        "reward":      row["reward"],     # EDID of reward GMRW, or null
    }


def get_row(records: dict, fid: str) -> dict | None:
    return records.get(fid.upper())


# ── Structure builder ─────────────────────────────────────────────────────────

def build_output(records: dict) -> dict:

    def resolve(fid: str, force_live: bool = False) -> dict | None:
        row = get_row(records, fid)
        if row is None:
            print(f"  [WARN] FormID {fid} not found in TSV — skipping", file=sys.stderr)
            return None
        return make_challenge(row, force_live=force_live)

    def resolve_list(fids: list[str], force_live: bool = False) -> list[dict]:
        return [c for fid in fids if (c := resolve(fid, force_live=force_live)) is not None]

    # ── Grunt Hunt page ──────────────────────────────────────────────────────
    grunt_discover = resolve_list(GRUNT_DISCOVER)
    grunt_chain    = resolve_list(GRUNT_CHAIN)
    grunt_camp     = resolve_list(GRUNT_CAMP, force_live=True)  # zzz prefix but live
    grunt_cut      = resolve_list(GRUNT_CUT)

    # ── Head Hunt page ───────────────────────────────────────────────────────
    head_start    = resolve_list(HEAD_START_CHAIN)
    head_complete = resolve_list(HEAD_COMPLETE_CHAIN)

    # Location META + subs
    loc_meta_base = resolve(LOCATION_META)
    if loc_meta_base:
        loc_meta = {
            **loc_meta_base,
            "isMeta": True,
            "subs": resolve_list(LOCATION_SUBS),
        }
    else:
        loc_meta = None

    # Head groups: All-META → Group METAs → Kill-target SUBs
    all_meta_base = resolve(HEAD_GROUPS_ALL_META)
    group_metas = []
    for (gnum, meta_fid, sub_fids) in HEAD_GROUPS:
        grp_base = resolve(meta_fid)
        if grp_base is None:
            continue
        grp = {
            **grp_base,
            "isMeta":   True,
            "groupNum": gnum,
            "subs":     resolve_list(sub_fids),
        }
        group_metas.append(grp)

    if all_meta_base:
        all_groups_meta = {
            **all_meta_base,
            "isMeta": True,
            "subs":   group_metas,
        }
    else:
        all_groups_meta = None

    head_cut = resolve_list(HEAD_CUT)

    # ── Compose output ───────────────────────────────────────────────────────
    def group(gid: str, label: str, challenges: list,
              is_chain: bool = False, cut_content: bool = False) -> dict:
        return {
            "id":         gid,
            "label":      label,
            "isChain":    is_chain,
            "cutContent": cut_content,
            "challenges": [c for c in challenges if c is not None],
        }

    return {
        "generated":  datetime.now(timezone.utc).isoformat(),
        "byPage": {
            "grunt-hunt-rewards": {
                "name":   "Grunt Hunt Challenges",
                "groups": [
                    group("discover",    "Getting Started",     grunt_discover),
                    group("grunt-chain", "Complete Grunt Hunts", grunt_chain,  is_chain=True),
                    group("camp",        "C.A.M.P. Challenges",  grunt_camp),
                    group("cut-content", "Cut Content",          grunt_cut,    cut_content=True),
                ],
            },
            "head-hunt-rewards": {
                "name":   "Head Hunt Challenges",
                "groups": [
                    group("start-chain",    "Start Head Hunts",    head_start,    is_chain=True),
                    group("complete-chain", "Complete Head Hunts",  head_complete, is_chain=True),
                    group("head-groups",    "Head Hunt Groups",
                          [all_groups_meta] if all_groups_meta else []),
                    group("hunt-locations", "Hunt Locations",
                          [loc_meta] if loc_meta else []),
                    group("cut-content",    "Cut Content",          head_cut,      cut_content=True),
                ],
            },
        },
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    tsv_path = find_latest_tsv("CHAL_Export_*.tsv")
    if tsv_path is None:
        print("[build_bounty_hunting_json] ERROR: no CHAL_Export_*.tsv in tsv/", file=sys.stderr)
        sys.exit(1)

    print(f"[build_bounty_hunting_json] TSV: {tsv_path.name}")
    records = parse_chal_tsv(tsv_path)
    print(f"[build_bounty_hunting_json] Parsed {len(records):,} challenge records")

    data = build_output(records)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)

    grunt_groups = data["byPage"]["grunt-hunt-rewards"]["groups"]
    head_groups  = data["byPage"]["head-hunt-rewards"]["groups"]
    grunt_total  = sum(len(g["challenges"]) for g in grunt_groups)
    head_total   = sum(len(g["challenges"]) for g in head_groups)
    print(f"[build_bounty_hunting_json] grunt-hunt-rewards: {grunt_total} top-level challenges")
    print(f"[build_bounty_hunting_json] head-hunt-rewards:  {head_total} top-level challenges")
    print(f"[build_bounty_hunting_json] Written → {OUT_FILE}")


if __name__ == "__main__":
    main()
