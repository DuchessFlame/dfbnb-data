#!/usr/bin/env python3
"""
src/build_fishing_equipment_json.py
Reads OMOD/BOOK/GMRW/CHAL TSVs + season_rewards.tsv and emits
dist/fishing_equipment.json describing fishing rod skins (RodBase mods)
and fishing bobbers + floats (RodBobber mods).

Output is consumed by the /df/fishing/fishing-bobbers-floats/ page on
buffsnbrew.com (template: item-hub).

How to Obtain is built in layers:
  1. Prefix-based default (always set):
     ATX_ -> Atomic Shop, SCORE_S## -> Season ## Scoreboard,
     SCORE_MiniSeason_YYYY -> Mini-Season event, Burn_ -> Burning
     Springs, ZZZ_ -> Unreleased, otherwise base game.
  2. CHAL/GMRW chain: OMOD -> BOOK (recipe) -> GMRW (challenge reward)
     -> CHAL. When resolved, surfaces the challenge name + target count.
  3. season_rewards.tsv: when a Scoreboard item matches, surfaces the
     page number and S.C.O.R.E. point cost.

No external dependencies — runs on stdlib only.
"""

import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TSV_DIR = os.path.join(SCRIPT_DIR, "..", "tsv")
DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist")
DIST_FILE = os.path.join(DIST_DIR, "fishing_equipment.json")

IMAGE_BASE_ROD_SKIN = (
    "https://www.buffsnbrew.com/wp-content/uploads/"
    "guide-images/fishing/rod-skins/"
)
IMAGE_BASE_BOBBER = (
    "https://www.buffsnbrew.com/wp-content/uploads/"
    "guide-images/fishing/bobbers-floats/"
)


# ─────────────────── TSV helpers ───────────────────


def find_latest_tsv(pattern):
    files = sorted(
        f for f in glob.glob(os.path.join(TSV_DIR, pattern))
        if "Locations" not in f  # skip *_Locations sub-files
    )
    return files[-1] if files else None


def read_tsv(filepath):
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc) as f:
                for line in f:
                    yield line.rstrip("\n").rstrip("\r").split("\t")
            return
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {filepath}")


def load_tsv(filepath):
    rows = iter(read_tsv(filepath))
    header = next(rows, [])
    idx = {col: i for i, col in enumerate(header)}
    return idx, rows


# ─────────────────── EDID classification ───────────────────


SEASON_PREFIX_RE = re.compile(r"^SCORE_S(\d+)_", re.IGNORECASE)
MINI_SEASON_PREFIX_RE = re.compile(
    r"^SCORE_MiniSeason_(\d{4})_([A-Za-z0-9]+)_", re.IGNORECASE
)


def classify_edid(edid):
    s = edid or ""
    if s.startswith("ZZZ_"):
        return {
            "method": "unknown", "seasonNumber": None,
            "eventCode": None, "eventYear": None,
            "display": "Pre-release content. Not currently available in-game.",
            "badge": "Unreleased",
        }
    if s.startswith("ATX_"):
        return {
            "method": "atom", "seasonNumber": None,
            "eventCode": None, "eventYear": None,
            "display": "Available in the Atomic Shop.",
            "badge": "Atomic Shop",
        }
    m = MINI_SEASON_PREFIX_RE.match(s)
    if m:
        year, code = int(m.group(1)), m.group(2)
        return {
            "method": "mini-season", "seasonNumber": None,
            "eventCode": code, "eventYear": year,
            "display": f"Available via the {year} Mini-Season event scoreboard ({code}).",
            "badge": f"Mini-Season {year}",
        }
    m = SEASON_PREFIX_RE.match(s)
    if m:
        season = int(m.group(1))
        return {
            "method": "scoreboard", "seasonNumber": season,
            "eventCode": None, "eventYear": None,
            "display": f"Available via the Season {season} Scoreboard.",
            "badge": f"Season {season}",
        }
    if s.startswith("Burn_"):
        return {
            "method": "burning-springs", "seasonNumber": None,
            "eventCode": None, "eventYear": None,
            "display": "Tied to the Burning Springs region content.",
            "badge": "Burning Springs",
        }
    return {
        "method": "default", "seasonNumber": None,
        "eventCode": None, "eventYear": None,
        "display": "Available in the base game.",
        "badge": "Base Game",
    }


PREFIX_STRIP_RE = re.compile(
    r"^(ATX_|SCORE_S\d+_|SCORE_MiniSeason_\d{4}_[A-Za-z0-9]+_|Burn_|ZZZ_(?:SCORE_S\d+_)?)",
    re.IGNORECASE,
)


def image_filename(edid):
    s = PREFIX_STRIP_RE.sub("", edid or "")
    parts = s.split("_")
    tail = parts[-1] if parts else ""
    return f"{tail}.avif" if tail else ""


def is_rod_skin(edid):
    return "_RodBase" in (edid or "")


def is_bobber(edid):
    return "_RodBobber" in (edid or "")


# ─────────────────── CHAL / GMRW / BOOK chain ───────────────────


OMOD_TAIL_RE = re.compile(r"^(?:zzz_?)?.*?_mod_FishingRod_Weapon_(.+)$", re.IGNORECASE)
BOOK_TAIL_RE = re.compile(r"^(?:zzz_?)?.*?_Recipe_mod_FishingRod_(.+)$", re.IGNORECASE)


def normalise_tail(s):
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"rodbase\d+_", "rodbase_", s)
    return s


def load_book_map():
    candidates = sorted(
        f for f in glob.glob(os.path.join(TSV_DIR, "BOOK_Export_*.tsv"))
        if "Locations" not in f
    )
    path = candidates[-1] if candidates else None
    if not path:
        return {}
    idx, rows = load_tsv(path)
    if "FormID" not in idx or "EDID" not in idx:
        return {}
    out = {}
    for row in rows:
        if len(row) <= idx["EDID"]:
            continue
        edid = (row[idx["EDID"]] or "").strip()
        if "_Recipe_mod_FishingRod_" not in edid:
            continue
        m = BOOK_TAIL_RE.match(edid)
        if not m:
            continue
        tail = normalise_tail(m.group(1))
        if tail:
            out[tail] = {
                "formId": (row[idx["FormID"]] or "").strip(),
                "edid": edid,
                "name": (row[idx["FULL"]] or "").strip() if "FULL" in idx else "",
            }
    return out


def load_gmrw_to_chal_map():
    path = find_latest_tsv("GMRW_Export_*.tsv")
    if not path:
        return {}
    _idx, rows = load_tsv(path)
    out = {}
    for row in rows:
        book_fid = None
        chal_fid = None
        for cell in row:
            if not cell:
                continue
            if cell.endswith(":BOOK") and "_Recipe_mod_FishingRod_" in cell:
                book_fid = cell.split(":", 1)[0].strip().lower()
            elif cell.endswith(":CHAL"):
                chal_fid = cell.split(":", 1)[0].strip().upper()
        if book_fid and chal_fid:
            out[book_fid] = chal_fid
    return out


def load_chal_map():
    path = find_latest_tsv("CHAL_Export_*.tsv")
    if not path:
        return {}
    idx, rows = load_tsv(path)
    if "FormID" not in idx:
        return {}
    out = {}
    for row in rows:
        if len(row) <= idx["FormID"]:
            continue
        fid = (row[idx["FormID"]] or "").strip().upper()
        if not fid:
            continue
        out[fid] = {
            "full": (row[idx["FULL"]] or "").strip() if "FULL" in idx else "",
            "snam": (row[idx["SNAM"]] or "").strip() if "SNAM" in idx else "",
            "tnam": (row[idx["TNAM"]] or "").strip() if "TNAM" in idx else "",
        }
    return out


def build_unlock_text(chal):
    full = chal.get("full") or ""
    snam = chal.get("snam") or ""
    tnam = chal.get("tnam") or ""
    try:
        n = int(tnam)
    except (TypeError, ValueError):
        n = None
    bits = []
    if full:
        bits.append(f'Complete the "{full}" challenge')
    if n and snam:
        bits.append(f"{n} {snam}")
    elif n:
        bits.append(f"target: {n}")
    return " — ".join(bits).strip() or full or ""


def lookup_unlock(omod_edid, book_map, gmrw_to_chal, chal_map):
    m = OMOD_TAIL_RE.match(omod_edid or "")
    if not m:
        return None
    tail = normalise_tail(m.group(1))
    book = book_map.get(tail)
    if not book:
        return None
    book_fid = (book.get("formId") or "").lower()
    if not book_fid:
        return None
    chal_fid = gmrw_to_chal.get(book_fid)
    if not chal_fid:
        return None
    chal = chal_map.get(chal_fid.upper())
    if not chal:
        return None
    chal = dict(chal)
    chal["chalFormId"] = chal_fid
    chal["bookFormId"] = book.get("formId")
    chal["bookEdid"] = book.get("edid")
    chal["bookName"] = book.get("name")
    return chal


# ─────────────────── season_rewards.tsv enrichment ───────────────────


def load_season_rewards_map():
    """Return {entitlement_lower -> {seasonNumber, page, cost, name, description}}."""
    path = os.path.join(TSV_DIR, "season_rewards.tsv")
    if not os.path.isfile(path):
        return {}
    idx, rows = load_tsv(path)
    needed = ("seasonNumber", "page", "name", "cost", "description", "storefrontEntitlement")
    for n in needed:
        if n not in idx:
            return {}
    out = {}
    for row in rows:
        if len(row) <= idx["storefrontEntitlement"]:
            continue
        ent = (row[idx["storefrontEntitlement"]] or "").strip().lower()
        if not ent:
            continue
        out[ent] = {
            "seasonNumber": (row[idx["seasonNumber"]] or "").strip(),
            "page":         (row[idx["page"]]         or "").strip(),
            "cost":         (row[idx["cost"]]         or "").strip(),
            "name":         (row[idx["name"]]         or "").strip(),
            "description":  (row[idx["description"]]  or "").strip(),
        }
    return out


def omod_to_possible_entm_edids(edid):
    """OMOD EDIDs like SCORE_S24_Fishing_mod_FishingRod_Weapon_RodBobber_UFOBobber
    are granted by ENTM records with `_ENTM_Weapons_` inserted between the
    SCORE_S## / ATX_ / MiniSeason_ prefix and `Fishing_mod_FishingRod_...`."""
    if not edid:
        return []
    out = [edid]
    m = re.match(
        r"^(SCORE_S\d+_|ATX_|SCORE_MiniSeason_\d{4}_[A-Za-z0-9]+_|Burn_|ZZZ_(?:SCORE_S\d+_)?)(Fishing_mod_FishingRod_.*)$",
        edid,
    )
    if m:
        out.append(m.group(1) + "ENTM_Weapons_" + m.group(2))
    return out


# ─────────────────── Builder ───────────────────


def build():
    omod_path = find_latest_tsv("OMOD_Export_*.tsv")
    if not omod_path:
        print("[fishing-equipment] No OMOD_Export_*.tsv found", file=sys.stderr)
        sys.exit(1)

    idx, rows = load_tsv(omod_path)
    col_form = "OMOD_FormID" if "OMOD_FormID" in idx else "FormID"
    col_edid = "OMOD_EDID" if "OMOD_EDID" in idx else "EDID"
    col_full = "FULL"
    for col in (col_form, col_edid, col_full):
        if col not in idx:
            print(f"[fishing-equipment] OMOD TSV missing column {col!r}", file=sys.stderr)
            sys.exit(1)

    book_map = load_book_map()
    gmrw_to_chal = load_gmrw_to_chal_map()
    chal_map = load_chal_map()
    season_map = load_season_rewards_map()

    rod_skins = []
    bobbers = []
    enriched_chal = 0
    enriched_score = 0

    for row in rows:
        if len(row) <= idx[col_full]:
            continue
        form_id = (row[idx[col_form]] or "").strip()
        edid = (row[idx[col_edid]] or "").strip()
        full = (row[idx[col_full]] or "").strip()
        if not edid or "_mod_FishingRod_Weapon_" not in edid:
            continue
        if not (is_rod_skin(edid) or is_bobber(edid)):
            continue

        obtain = classify_edid(edid)

        # Layer 2 — CHAL/GMRW chain
        chal = lookup_unlock(edid, book_map, gmrw_to_chal, chal_map)
        if chal:
            unlock_text = build_unlock_text(chal)
            if unlock_text:
                obtain["challenge"] = {
                    "name": chal.get("full"),
                    "counter": chal.get("snam"),
                    "target": chal.get("tnam"),
                    "chalFormId": chal.get("chalFormId"),
                    "bookFormId": chal.get("bookFormId"),
                    "bookEdid": chal.get("bookEdid"),
                    "bookName": chal.get("bookName"),
                }
                obtain["display"] = unlock_text + ". " + obtain["display"]
                enriched_chal += 1

        # Layer 3 — season_rewards.tsv enrichment
        for cand in omod_to_possible_entm_edids(edid):
            sr = season_map.get(cand.lower())
            if not sr:
                continue
            obtain["scoreboard"] = sr
            page = sr.get("page")
            cost = sr.get("cost")
            season_no = sr.get("seasonNumber") or (obtain.get("seasonNumber") or "")
            bits = []
            if season_no:
                bits.append(f"Season {season_no} Scoreboard")
            if page:
                bits.append(f"page {page}")
            if cost:
                bits.append(f"costs {cost} S.C.O.R.E.")
            if bits:
                obtain["display"] = (
                    "Available via the " + bits[0]
                    + (", " + ", ".join(bits[1:]) if len(bits) > 1 else "")
                    + "."
                )
            enriched_score += 1
            break

        item = {
            "formId": form_id,
            "edid": edid,
            "name": full or edid,
            "imageFilename": image_filename(edid),
            "imageUrl": "",
            "howToObtain": obtain,
            "cutContent": obtain["method"] == "unknown",
        }

        if is_rod_skin(edid):
            rod_skins.append(item)
        else:
            bobbers.append(item)

    method_order = {
        "default": 0, "burning-springs": 1, "scoreboard": 2,
        "mini-season": 3, "atom": 4, "unknown": 9,
    }

    def sort_key(it):
        return (method_order.get(it["howToObtain"]["method"], 8), it["name"].lower())

    rod_skins.sort(key=sort_key)
    bobbers.sort(key=sort_key)

    payload = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "schemaVersion": 1,
        "imageBases": {
            "rodSkin": IMAGE_BASE_ROD_SKIN,
            "bobber": IMAGE_BASE_BOBBER,
        },
        "rodSkins": rod_skins,
        "bobbersAndFloats": bobbers,
    }

    os.makedirs(DIST_DIR, exist_ok=True)
    with open(DIST_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(
        f"[fishing-equipment] OK: {len(rod_skins)} rod skins, "
        f"{len(bobbers)} bobbers & floats "
        f"(CHAL chain enriched: {enriched_chal}, "
        f"Scoreboard enriched: {enriched_score}) -> {DIST_FILE}"
    )


if __name__ == "__main__":
    build()
