#!/usr/bin/env python3
"""
build_upcoming_rewards_json.py
------------------------------
Generative "Upcoming Rewards" builder for the DF scoreboard pages.

Discovers every SCORE_S{N}_ENTM_* entitlement in the ENTM export and emits a
flat, A-Z "upcoming rewards" catalogue per PTS season — one file per season
that is NOT yet curated in season_rewards.tsv. This is the datamined preview
that feeds /df/scoreboards/season-{N}/upcoming-rewards/ (df-bnb-upcoming-rewards.js).

Season assignment is FULLY AUTOMATIC from the EDID pattern
    SCORE_S{N}_ENTM_{Category}_{Name}   (regex: SCORE_S(\\d+)_ENTM_(.+))
group 1 = season number. No hand-tagging, no per-season pool config, so future
seasons auto-populate the moment their entitlements appear in a PTS export.

Per item it resolves, straight from the ENTM record (never fabricated):
    name          FULL (fallback NNAM, fallback EDID suffix)
    description   DESC (raw; the renderer cleans Bethesda's "- ... -" bullets)
    category      KEYWORDS ATX_Entitlement_Category_Store_* (display-mapped)
    subCategory   KEYWORDS ATX_Entitlement_Filter_Store_*   (display-mapped)
    rarity        KEYWORDS ATX_ItemRarity_*  (Superior|Rare|Standard; "None" -> "")
    falloutFirst  XALG_Flags == "Premium"
    formId        FormID
    ddsHandle     ETIP + ETDI  (texture path + .dds display icon)
    edid          full EDID

Optional (only where the game actually shows one — never invented):
    output        item output for resource generators, taken VERBATIM from the
                  in-game DESC ("Generates ..." / "Produces ..." sentence) or
                  from tsv/score_buff_output_overrides.tsv if present.
    buff          buff effect, only from the overrides TSV (LVLI/rng76 hook).

Weight/value and true LVLI-resolved Buff/Output apply to a tiny handful of
cosmetics; rather than guess, they are left to the optional overrides TSV so
nothing is ever fabricated. A per-item DISPLAY image is hand-authored later
(the game only gives the .dds name) — the JSON carries ddsHandle + formId so
the .webp can be mapped/uploaded afterwards; the renderer shows a placeholder
until then.

STATUS: active
INPUT:  ENTM export TSV  (tsv/ENTM_Export_*.tsv  or  tsv/pts/ENTM_Export_PTS_*.tsv)
        fallout76_seasons.tsv     (tsv/fallout76_seasons.tsv  -- season names/meta)
        season_rewards.tsv        (tsv/season_rewards.tsv     -- detect curated seasons)
        score_buff_output_overrides.tsv (tsv/, OPTIONAL -- curated buff/output)
OUTPUT: dist/calculators/upcoming_rewards_s{N}.json
USAGE:  python src/build_upcoming_rewards_json.py
        python src/build_upcoming_rewards_json.py --season 26
        python src/build_upcoming_rewards_json.py --entm-tsv tsv/pts/ENTM_Export_PTS_...tsv
"""

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import tsv_source          # one resolver for every export selection

# Shared drop-rate engine — SAME source of truth used by build_drop_rates.py and
# build_camp_items_json.py. Resource-generator OUTPUT lists are resolved through
# this (never a standalone re-implementation, per the drop-rate-engine skill's
# "Standalone Copy Rule"). Import is best-effort: if rng76 can't load, the build
# still runs and simply omits the resolved output drop list.
try:
    from rng76 import Rng76Data
except Exception:  # pragma: no cover - resolver is optional
    Rng76Data = None

REPO_ROOT = Path(__file__).resolve().parents[1]
TSV_DIR = REPO_ROOT / "tsv"
DIST_DIR = REPO_ROOT / "dist" / "calculators"
SEASONS_TSV = TSV_DIR / "fallout76_seasons.tsv"
REWARDS_TSV = TSV_DIR / "season_rewards.tsv"
LEGACY_TSV = TSV_DIR / "legacy_seasons.tsv"
OVERRIDES_TSV = TSV_DIR / "score_buff_output_overrides.tsv"
TAG = "[build_upcoming_rewards]"

EDID_RE = re.compile(r"^(?:zzz_?|ZZZ_?)?SCORE_S(\d+)_ENTM_(.+)$", re.IGNORECASE)
DISABLED_RE = re.compile(r"^(?:zzz|ZZZ)_")

# ------- Category display names (from ATX_Entitlement_Category_Store_*) -------
CATEGORY_DISPLAY = {
    "CAMP": "C.A.M.P.",
    "Skin": "Skin",
    "Apparel": "Apparel",
    "Emotes": "Emote",
    "Rewards": "Reward",
    "AvatarIcons": "Player Icon",
    "Photomode": "Photomode",
    "Music": "Music",
}

# ------- EDID-suffix fallback for entitlements with no Category keyword -------
EDID_CATEGORY_FALLBACK = [
    ("Account_ScoreBoost",        "S.C.O.R.E. Booster"),
    ("Account_PremiumBattlePass", "Season Access"),
    ("Account_",                  "Account Reward"),
    ("PlayerIcon_",               "Player Icon"),
    ("PlayerTitles_",             "Player Title"),
    ("CAMPTitles_",               "C.A.M.P. Title"),
    ("Emotes_",                   "Emote"),
    ("Apparel_",                  "Apparel"),
    ("Skin_",                     "Skin"),
    ("Weapons_",                  "Reward"),
    ("CAMP_",                     "C.A.M.P."),
]

# ------- Non-collectible entitlements EXCLUDED from the upcoming pages --------
# The Fallout First "Season Pass" entitlement (Season Access) and the S.C.O.R.E.
# multiplier boosts are account perks, not collectible scoreboard rewards, so
# they are dropped from /upcoming-rewards/ (the ticket-calculator / scoreboard
# builders still keep them). This is applied to the UPCOMING build only.
#
# Matched by EDID stem first (most robust + future-proof: any SCORE_S{N}_ENTM_
# season auto-drops these), then by resolved display category, then a display-
# name fallback in case a future export changes the category/EDID signal.
EXCLUDE_EDID_STEMS = (
    "Account_ScoreBoost",         # 5% / 10% S.C.O.R.E. Boost multipliers
    "Account_PremiumBattlePass",  # Fallout First Season Pass (Season Access)
)
EXCLUDE_CATEGORIES = {
    "S.C.O.R.E. Booster",
    "Season Access",
}
EXCLUDE_NAME_RE = re.compile(
    r"(S\.?C\.?O\.?R\.?E\.?\s*Boost|Season\s*Pass|Season\s*Access)",
    re.IGNORECASE,
)

# ------- Sub-category prettifier (from ATX_Entitlement_Filter_Store_*) --------
SUBCAT_WORD_FIX = {
    "PowerArmor": "Power Armor",
    "WeaponSkin": "Weapon Skin",
    "Weapons": "Weapon Skin",
    "FloorDecor": "Floor Decor",
    "WallDecor": "Wall Decor",
    "AvatarIcons": "Player Icons",
    "Headwear": "Headwear",
    "Outfits": "Outfits",
    "Machinery": "Machinery",
    "Structures": "Structures",
    "Decorations": "Decorations",
    "Utility": "Utility",
    "Furniture": "Furniture",
    "Doors": "Doors",
    "Kits": "C.A.M.P. Kit",
}

# ------- Rarity ordering (for optional secondary sort / reference) ------------
RARITY_RANK = {"Superior": 0, "Rare": 1, "Standard": 2, "": 3}

# ------- Output detection (verbatim, from the game's own DESC) ----------------
OUTPUT_SENTENCE_RE = re.compile(
    r"((?:Generates|Produces|Creates|Yields)\b[^-.!]*[.!]?)", re.IGNORECASE
)

NOTES = {
    "purpose": (
        "Upcoming (PTS) scoreboard reward preview for TheDuchessFlame.com. "
        "Auto-generated from PTS ENTM exports by build_upcoming_rewards_json.py. "
        "Rendered A-Z by df-bnb-upcoming-rewards.js on "
        "/df/scoreboards/season-{N}/upcoming-rewards/."
    ),
    "pts_notice": (
        "This list is datamined from the current PTS build. Items, rarities, "
        "images, and even which rewards appear can be added, removed, or changed "
        "by Bethesda before the season goes live. Nothing here is final until "
        "release."
    ),
    "generated_by": "build_upcoming_rewards_json.py -- do not hand-edit.",
}


def legacy_rerun_seasons(path):
    """Season numbers that have a legacy re-release listed in legacy_seasons.tsv."""
    out = set()
    if not path.exists():
        return out
    for row in read_tsv(path):
        try:
            out.add(int((row.get("SeasonNumber") or "").strip()))
        except (TypeError, ValueError):
            continue
    return out


def load_uploaded_images(season_num):
    """Entitlement (lowercased) -> the artwork URL actually uploaded for it.

    Read from dist/season_images/season_{N}_images.json, which
    build_season_reward_images.py writes when it resolves each reward's texture.
    That file records the OUTPUT filename, which is named after the curated
    reward row rather than the source .dds - so it is the only reliable map from
    an entitlement to the image that exists on the site.
    """
    path = REPO_ROOT / "dist" / "season_images" / f"season_{season_num}_images.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out = {}
    for img in data.get("images", []):
        ent = (img.get("entitlement") or "").strip().lower()
        name = (img.get("outAvif") or "").strip()
        upload = (img.get("uploadTo") or "").strip()
        if ent and name and upload:
            out[ent] = upload + name
    return out


def read_tsv(path):
    for enc in ("utf-8-sig", "latin-1"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
                return list(reader)
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Cannot decode {path} with any known encoding")


def parse_date_dmy(s):
    if not s:
        return ""
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return s


def safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def find_entm_tsv(tsv_dir):
    hit = tsv_source.newest(str(Path(tsv_dir) / "ENTM_Export_*.tsv"), required=False)
    return Path(hit) if hit else None


def parse_keywords(kw):
    """Return (category_raw, subcat_raw, rarity_raw) parsed from KEYWORDS."""
    cat = sub = rar = None
    for tok in (kw or "").split("|"):
        parts = tok.split(":")
        name = parts[1] if len(parts) > 1 else tok
        if "Entitlement_Category_Store_" in name:
            cat = name.split("Category_Store_", 1)[1]
        elif "Entitlement_Filter_Store_" in name:
            sub = name.split("Filter_Store_", 1)[1]
        elif "ItemRarity_" in name:
            rar = name.split("ItemRarity_", 1)[1]
    return cat, sub, rar


def display_category(cat_raw, edid_suffix):
    if cat_raw and cat_raw in CATEGORY_DISPLAY:
        return CATEGORY_DISPLAY[cat_raw]
    if cat_raw:
        return cat_raw
    for prefix, label in EDID_CATEGORY_FALLBACK:
        if edid_suffix.startswith(prefix):
            return label
    return "Other"


def is_excluded(edid_suffix, category, name):
    """True for non-collectible entitlements that must NOT appear on the
    Upcoming Rewards pages (season pass + S.C.O.R.E. boosts).

    Robust, future-proof and layered so a single changed signal can't leak an
    item through:
      1. EDID stem  (primary; season-agnostic, so S27/28+ auto-drop)
      2. resolved display category
      3. display-name fallback
    """
    stem = (edid_suffix or "")
    for s in EXCLUDE_EDID_STEMS:
        if stem.lower().startswith(s.lower()):
            return True
    if category in EXCLUDE_CATEGORIES:
        return True
    if name and EXCLUDE_NAME_RE.search(name):
        return True
    return False


def display_subcategory(sub_raw, cat_raw):
    """Strip the leading '{Category}_' scope and prettify what's left."""
    if not sub_raw:
        return ""
    tail = sub_raw
    if cat_raw and tail.startswith(cat_raw + "_"):
        tail = tail[len(cat_raw) + 1:]
    if tail in SUBCAT_WORD_FIX:
        return SUBCAT_WORD_FIX[tail]
    # Split CamelCase / underscores into spaced words
    tail = tail.replace("_", " ")
    tail = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", tail)
    return tail.strip()


def clean_rarity(rar_raw):
    if not rar_raw:
        return ""
    r = rar_raw.strip()
    if r.lower() in ("none", "standardnone"):
        return ""
    # Guard against a truncated "Standa" style token from long KEYWORDS strings
    if r.lower().startswith("standa"):
        return "Standard"
    if r.lower().startswith("super"):
        return "Superior"
    if r.lower().startswith("rare"):
        return "Rare"
    return r


# =====================================================================
# Resource-generator OUTPUT drop-list resolution
# ---------------------------------------------------------------------
# A resource generator (Altar of Bones, Creepy Cultist Well, ...) is a
# workshop object backed by a RESO (resource generator) record. The RESO's
# NAM2_Produce field points at the PRODUCTION leveled list (LVLI); that list
# is resolved through the shared rng76 engine — exactly the way the activity /
# reward / camp-item builders do it — to a full per-item drop-rate list.
#
# Linkage:  ENTM entitlement  --(EDID name key + season)-->  RESO record
#           RESO.NAM2_Produce --(LVLI FormID)-->  rng76.resolve_deep()  -> drops
#
# Never bare ChanceNone, never hardcoded FormIDs. Where rng76 cannot resolve a
# list to clean (non-negative) rates — e.g. a First-Match graduated production
# list whose cascade goes non-monotonic — the drops are still listed but with
# chance=None and ratesResolved=False so the renderer shows the produced items
# honestly WITHOUT fabricating a percentage.
# =====================================================================

_LVLI_REF_RE = re.compile(r"\[LVLI:([0-9A-Fa-f]{8})\]")
_GLOB_REF_RE = re.compile(r"\[GLOB:([0-9A-Fa-f]{8})\]")
_SEASON_RE = re.compile(r"SCORE[_-]?S(\d+)[_-]", re.IGNORECASE)
_RESO_NOISE = set((
    "score workshop co camp entm reso resource resources collector collectron "
    "utility generators generator decorations decoration containers container "
    "empty atx community f1 the copy default all blood bones morbid"
).split())


def _reso_name_key(edid):
    """Distinctive lower-case name key for a RESO/ENTM EDID: split camelCase,
    drop structural/category words and numeric tokens, keep the rest joined.
    Mirrors build_camp_items_json.recipe_name_key so matching agrees."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", edid or "")
    s = re.sub(r"[^A-Za-z0-9]", " ", s).lower()
    out = []
    for w in s.split():
        if w in _RESO_NOISE:
            continue
        if re.fullmatch(r"s?\d+|w\d+|\d+", w):
            continue
        if len(w) >= 3:
            out.append(w)
    return "".join(out)


def _season_of_edid(edid):
    m = _SEASON_RE.search(edid or "")
    return int(m.group(1)) if m else None


def _extract_lvli_formid(field):
    if not field:
        return None
    m = _LVLI_REF_RE.search(field)
    if m:
        return m.group(1).upper()
    m = re.match(r"^([0-9A-Fa-f]{8}):", field.strip())
    return m.group(1).upper() if m else None


def _extract_glob_formid(field):
    if not field:
        return None
    m = _GLOB_REF_RE.search(field)
    if m:
        return m.group(1).upper()
    m = re.match(r"^([0-9A-Fa-f]{8}):", field.strip())
    return m.group(1).upper() if m else None


def _interval_display(hours):
    """Human production-interval string from a float hours value."""
    if hours is None:
        return None
    total_seconds = hours * 3600.0
    if total_seconds <= 0:
        return None
    if total_seconds % 3600 == 0:
        h = int(total_seconds // 3600)
        return "{} hour{}".format(h, "" if h == 1 else "s")
    mins = int(total_seconds // 60)
    secs = int(round(total_seconds % 60))
    if mins == 0:
        return "{} sec".format(secs)
    if secs == 0:
        return "{} min".format(mins)
    return "{} min {} sec".format(mins, secs)


def _consolidate_output_drops(drops):
    """Merge duplicate FormIDs (duplicate entry = quantity, not extra
    probability — use max), sort by chance desc then name. Identical convention
    to build_camp_items_json.consolidate_drops so the two pages agree."""
    merged = {}
    for d in drops:
        key = d["formId"]
        if key in merged:
            merged[key]["chance"] = round(max(merged[key]["chance"], d["chance"]), 5)
        else:
            merged[key] = dict(d)
    return sorted(merged.values(), key=lambda x: (-x["chance"], (x.get("name") or "").lower()))


def find_reso_tsv(tsv_dir):
    hit = tsv_source.newest(str(Path(tsv_dir) / "RESO_Export_*.tsv"), required=False)
    return Path(hit) if hit else None


class GeneratorOutputResolver:
    """Resolves a resource-generator ENTM to its production drop list via rng76.

    Built once per build from the same TSV directory the ENTM export came from
    (so PTS builds resolve PTS production lists). Degrades gracefully: if rng76
    or the RESO export is unavailable, match() returns None for everything and
    the build falls back to the DESC-derived prose output only.
    """

    def __init__(self, entm_tsv_path):
        self.ok = False
        self._reso_by_key = {}    # (season, name_key) -> reso row dict
        self._reso_keys = []      # [(season, name_key, reso_row)] for containment match
        self._resolver = None
        self._globs = None
        tsv_dir = Path(entm_tsv_path).parent
        if Rng76Data is None:
            print(TAG + " [WARN] rng76 unavailable -- output drop lists will be omitted.")
            return
        reso_tsv = find_reso_tsv(tsv_dir)
        if reso_tsv is None:
            print(TAG + " [WARN] No RESO_Export_*.tsv in " + str(tsv_dir)
                  + " -- output drop lists will be omitted.")
            return
        try:
            data = Rng76Data.from_tsv_root(str(tsv_dir))
            self._resolver = data.resolver
            self._globs = getattr(data, "globs", None)
        except Exception as exc:  # pragma: no cover
            print(TAG + " [WARN] rng76 failed to load (" + str(exc)
                  + ") -- output drop lists will be omitted.")
            return
        for row in read_tsv(reso_tsv):
            edid = (row.get("EDID") or "").strip()
            if not edid:
                continue
            key = _reso_name_key(edid)
            if not key:
                continue
            season = _season_of_edid(edid)
            self._reso_keys.append((season, key, row))
        print(TAG + " Loaded RESO: " + reso_tsv.name + " (" + str(len(self._reso_keys))
              + " producers) + rng76 engine for output drops")
        self.ok = True

    def _find_reso(self, entm_edid):
        """Best RESO match for an ENTM EDID: prefer same season, then longest
        shared name-key containment (a specific producer wins over a generic)."""
        suffix = re.sub(r"^(?:zzz_?|ZZZ_?)?SCORE_S\d+_ENTM_", "", entm_edid or "",
                        flags=re.IGNORECASE)
        ik = _reso_name_key(suffix)
        if len(ik) < 4:
            return None
        want_season = _season_of_edid(entm_edid)
        best, best_len = None, 0
        for season, rk, row in self._reso_keys:
            if not rk:
                continue
            if not (ik in rk or rk in ik):
                continue
            season_ok = (want_season is None or season is None or season == want_season)
            if not season_ok:
                continue
            L = min(len(ik), len(rk))
            # same-season matches outrank cross-season; then longer shared key wins
            score = L + (1000 if season == want_season else 0)
            if score > best_len:
                best, best_len = row, score
        return best

    def match(self, entm_edid):
        """Return an output-drops payload for a resource-generator ENTM, or None
        when the ENTM is not backed by a resolvable production list."""
        if not self.ok:
            return None
        reso = self._find_reso(entm_edid)
        if not reso:
            return None
        lvli_fid = _extract_lvli_formid(reso.get("NAM2_Produce") or "")
        if not lvli_fid:
            return None
        raw = []
        for it in self._resolver.resolve_deep(lvli_fid):
            raw.append({
                "name":   it.get("name") or it.get("edid") or it.get("formid") or "",
                "formId": (it.get("formid") or "").upper(),
                "chance": round(float(it.get("dropRate") or 0.0) * 100.0, 5),
                "qty":    it.get("qty") or 1,
            })
        if not raw:
            return None
        drops = _consolidate_output_drops(raw)
        rates_ok = all(d["chance"] >= 0 for d in drops)
        # Honest display: never surface a fabricated / non-monotonic negative
        # rate. When the list can't be resolved cleanly, list the produced items
        # with chance=None so the renderer shows them WITHOUT a percentage.
        if not rates_ok:
            for d in drops:
                d["chance"] = None
        # Production interval (NAM4 GLOB -> hours), best-effort.
        interval_display = None
        gfid = _extract_glob_formid(reso.get("NAM4_Interval") or "")
        if gfid and self._globs is not None:
            try:
                fltv = self._globs.value(gfid)
            except Exception:
                fltv = None
            interval_display = _interval_display(fltv)
        return {
            "produceEdid": (reso.get("EDID") or "").strip(),
            "produceLvli": lvli_fid,
            "drops": drops,
            "ratesResolved": rates_ok,
            "intervalDisplay": interval_display,
        }


def detect_output(desc):
    """Verbatim item-output sentence from the game's DESC (resource generators).
    Never invents text — returns '' when the DESC states no output."""
    if not desc:
        return ""
    m = OUTPUT_SENTENCE_RE.search(desc)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()


def load_overrides(path):
    """Optional curated buff/output map keyed by EDID (LVLI/rng76 hook).
    Columns: EDID, Buff, Output (any missing -> '')."""
    if not path.exists():
        return {}
    rows = read_tsv(path)
    out = {}
    for r in rows:
        e = (r.get("EDID") or "").strip()
        if not e:
            continue
        out[e] = {
            "buff": (r.get("Buff") or "").strip(),
            "output": (r.get("Output") or "").strip(),
        }
    return out


def load_season_metadata(path):
    if not path.exists():
        return {}
    rows = read_tsv(path)
    meta = {}
    for row in rows:
        num = safe_int(row.get("SeasonNumber", ""), -1)
        if num < 1:
            continue
        meta[num] = {
            "seasonName": (row.get("SeasonName") or "").strip(),
            "startDate": parse_date_dmy(row.get("StartDate", "")),
            "endDate": parse_date_dmy(row.get("EndDate", "")),
            "unlockRequiredCount": safe_int(row.get("UnlockRequiredCount") or "", 0),
            "unlockRankRequired": safe_int(row.get("UnlockRankRequired") or "", 0),
            "unlockLineText": (row.get("UnlockLineText") or "").strip(),
        }
    return meta


def curated_seasons(path):
    if not path.exists():
        return set()
    rows = read_tsv(path)
    return {safe_int(r.get("seasonNumber", ""), -1) for r in rows} - {-1}


def is_past_season(season_num, meta):
    """An 'upcoming' page only makes sense for a season that has NOT started yet.
    A season whose StartDate is today-or-earlier is live or finished, so skip it
    (those already have curated season_tickets + a /rewards/ page). Seasons with
    no metadata (future seasons not yet in fallout76_seasons.tsv) are treated as
    upcoming so they auto-populate the moment their entitlements appear."""
    sm = meta.get(season_num)
    if not sm:
        return False
    start = sm.get("startDate", "")
    if not start:
        return False
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return start <= today


def make_item_id(season_num, edid_suffix):
    slug = re.sub(r"[^a-z0-9]+", "_", edid_suffix.lower()).strip("_")
    return "S" + str(season_num) + "_" + slug


def extract_seasons(entm_tsv, overrides, target_season=None, gen_resolver=None):
    rows = read_tsv(entm_tsv)
    seasons = defaultdict(list)
    excluded = defaultdict(int)
    generators = []          # (season, name, edid, payload) for the build summary

    for row in rows:
        edid = (row.get("EDID") or "").strip()
        m = EDID_RE.match(edid)
        if not m or DISABLED_RE.match(edid):
            continue

        snum = int(m.group(1))
        suffix = m.group(2)
        if target_season is not None and snum != target_season:
            continue

        # Register the season the moment a valid entitlement is seen, so a
        # season whose every item is excluded (e.g. a future season that only
        # has the pass + S.C.O.R.E. boosts so far) still emits an empty-items
        # file rather than leaving a stale one behind.
        seasons.setdefault(snum, [])

        full_name = (row.get("FULL") or "").strip()
        nnam = (row.get("NNAM") or "").strip()
        desc = (row.get("DESC") or "").strip()
        xalg = (row.get("XALG_Flags") or "").strip()
        form_id = (row.get("FormID") or "").strip()
        etip = (row.get("ETIP") or "").strip()
        etdi = (row.get("ETDI") or "").strip()

        cat_raw, sub_raw, rar_raw = parse_keywords(row.get("KEYWORDS"))
        name = full_name or nnam or suffix

        item = {
            "id": make_item_id(snum, suffix),
            "name": name,
            "description": desc,
            "category": display_category(cat_raw, suffix),
            "subCategory": display_subcategory(sub_raw, cat_raw),
            "rarity": clean_rarity(rar_raw),
            "falloutFirst": xalg.lower() == "premium",
            "formId": form_id,
            "ddsHandle": (etip + etdi),
            "edid": edid,
        }

        # Drop non-collectible entitlements (season pass + S.C.O.R.E. boosts)
        # from the upcoming pages. Kept out here so nothing downstream sees them.
        if is_excluded(suffix, item["category"], name):
            excluded[snum] += 1
            continue

        # Optional buff/output — overrides win; else derive output verbatim
        ov = overrides.get(edid, {})
        buff = ov.get("buff", "")
        output = ov.get("output", "") or detect_output(desc)
        if buff:
            item["buff"] = buff
        if output:
            item["output"] = output

        # Resource-generator OUTPUT drop list (rng76-resolved). Only attached
        # where the ENTM is backed by a resolvable production LVLI. The prose
        # `output`/`description` above are left untouched — this adds the full
        # per-item drop list the renderer shows in the Output expand.
        if gen_resolver is not None:
            payload = gen_resolver.match(edid)
            if payload:
                item["outputDrops"] = payload["drops"]
                item["outputRatesResolved"] = payload["ratesResolved"]
                item["outputProduceEdid"] = payload["produceEdid"]
                item["outputProduceLvli"] = payload["produceLvli"]
                if payload["intervalDisplay"]:
                    item["outputInterval"] = payload["intervalDisplay"]
                generators.append((snum, name, edid, payload))

        seasons[snum].append(item)

    if excluded:
        summ = ", ".join("S%d (%d)" % (n, excluded[n]) for n in sorted(excluded))
        print(TAG + " Excluded non-collectible entitlements: " + summ)

    if generators:
        print(TAG + " Resource generators with resolved OUTPUT lists:")
        for snum, name, _edid, payload in generators:
            n = len(payload["drops"])
            state = "clean %d-entry rate list" % n if payload["ratesResolved"] \
                else "%d items listed WITHOUT rates (rng76 non-monotonic)" % n
            print(TAG + "   S%d  %-32s %s" % (snum, name, state))

    return dict(seasons)


def build_season_json(season_num, items, meta, source_name, observed):
    sm = meta.get(season_num, {})

    # Resolve each item's real uploaded artwork.
    #
    # The renderer used to build the image path from the .dds basename, which is
    # wrong whenever Bethesda reuses a texture: the Tree Branch Chandelier ships
    # under score_s1_camp_lights_treebranchchandelier.dds, and Season 4's upload
    # is named score_s4_camp_lights_treebranchchandelier.avif after the curated
    # row. The guess 404s and the reward shows "Image not yet uploaded" while
    # the scoreboard page displays it fine.
    #
    # The season image manifest is the only thing that knows the uploaded name,
    # so take it from there, keyed on entitlement. Items with no manifest entry
    # keep no imageUrl and the renderer falls back to the .dds guess.
    art = load_uploaded_images(season_num)
    for item in items:
        url = art.get((item.get("edid") or "").lower())
        if url:
            item["imageUrl"] = url

    # A-Z by name (case-insensitive); rarity as a stable secondary key
    items.sort(key=lambda x: (x["name"].lower(), RARITY_RANK.get(x["rarity"], 9)))

    output = {
        "_notes": NOTES,
        "seasonNumber": season_num,
        "seasonName": sm.get("seasonName") or ("Season " + str(season_num)),
        "generated": observed,
        "sourceFile": source_name,
        "count": len(items),
    }
    if sm.get("startDate"):
        output["startDate"] = sm["startDate"]
    if sm.get("endDate"):
        output["endDate"] = sm["endDate"]
    if sm.get("unlockRequiredCount"):
        output["unlockRequiredCount"] = sm["unlockRequiredCount"]
    if sm.get("unlockRankRequired"):
        output["unlockRankRequired"] = sm["unlockRankRequired"]
    if sm.get("unlockLineText"):
        output["unlockLineText"] = sm["unlockLineText"]

    output["items"] = items
    return output


def main():
    parser = argparse.ArgumentParser(
        description="Generate Upcoming (PTS) scoreboard reward JSON from ENTM exports."
    )
    parser.add_argument("--season", type=int, default=None,
        help="Only build a specific season number (default: auto-discover all).")
    parser.add_argument("--force", action="store_true",
        help="Build even if the season already has curated data in season_rewards.tsv.")
    parser.add_argument("--entm-tsv", type=str, default=None,
        help="Path to the ENTM export TSV (default: auto-detect in tsv/ then tsv/pts/).")
    parser.add_argument("--pts", action="store_true",
        help="Prefer the PTS ENTM export (tsv/pts/) over the live export. Upcoming "
             "(PTS) reward previews are datamined from PTS builds, so future-season "
             "SCORE_S{N}_ENTM entitlements (e.g. S26) only appear in the PTS export "
             "-- the live export carries just the pass + S.C.O.R.E. boosts.")
    parser.add_argument("--out-dir", type=str, default=None,
        help="Output directory (default: dist/calculators/).")
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DIST_DIR

    print(TAG + " Starting Upcoming Rewards build...")

    if args.entm_tsv:
        entm_tsv = Path(args.entm_tsv)
        if not entm_tsv.exists():
            raise SystemExit(TAG + " [ERROR] Specified ENTM TSV not found: " + str(entm_tsv))
    elif args.pts:
        # PTS channel: read tsv/pts/ first (that's where upcoming-season
        # entitlements land). Fall back to the live export only if no PTS
        # export is present, so the step still no-ops instead of erroring.
        pts_dir = TSV_DIR / "pts"
        entm_tsv = find_entm_tsv(pts_dir) if pts_dir.exists() else None
        if entm_tsv is None:
            entm_tsv = find_entm_tsv(TSV_DIR)
        if entm_tsv is None:
            print(TAG + " [WARN] No ENTM_Export_*.tsv found -- nothing to do.")
            return
    else:
        # Default to the PTS export, falling back to the live one.
        #
        # These pages exist to show what is coming, and only the PTS export has
        # it. Preferring the live export here silently rebuilt every upcoming
        # page from shipped data: Season 26 dropped from 66 rewards to 0, and
        # Season 4's re-release lost the five titles Bethesda added to it — the
        # exact rewards the page is there to reveal. Defaulting the other way
        # costs nothing, because a season with no PTS entitlements simply has no
        # rows to emit.
        pts_dir = TSV_DIR / "pts"
        entm_tsv = find_entm_tsv(pts_dir) if pts_dir.exists() else None
        if entm_tsv is None:
            entm_tsv = find_entm_tsv(TSV_DIR)
        if entm_tsv is None:
            print(TAG + " [WARN] No ENTM_Export_*.tsv found -- nothing to do.")
            return
    print(TAG + " Using ENTM: " + entm_tsv.name)

    observed = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    meta = load_season_metadata(SEASONS_TSV)
    print(TAG + " Loaded metadata for " + str(len(meta)) + " season(s)")

    overrides = load_overrides(OVERRIDES_TSV)
    if overrides:
        print(TAG + " Loaded " + str(len(overrides)) + " buff/output override(s)")

    curated = curated_seasons(REWARDS_TSV)
    if curated:
        clist = ", ".join("S" + str(n) for n in sorted(curated))
        print(TAG + " Curated seasons in season_rewards.tsv: " + clist)

    print(TAG + " Loading resource-generator output resolver (rng76 + RESO)...")
    gen_resolver = GeneratorOutputResolver(entm_tsv)

    season_items = extract_seasons(entm_tsv, overrides, target_season=args.season,
                                   gen_resolver=gen_resolver)
    if not season_items:
        print(TAG + " [WARN] No SCORE_S*_ENTM entries found -- nothing to do.")
        return

    found_list = ", ".join(
        "S" + str(n) + " (" + str(len(items)) + ")"
        for n, items in sorted(season_items.items())
    )
    print(TAG + " Found ENTM entries for: " + found_list)

    out_dir.mkdir(parents=True, exist_ok=True)
    built = 0

    # Seasons queued for a legacy re-release. A re-run is genuinely upcoming even
    # though the season is curated and finished years ago: it has a live
    # /upcoming-rewards/ page showing what the re-released board will hold, and
    # that page is how players spot the rewards Bethesda ADDED to the re-run.
    # Without this both skips below would fire and the page would be served a
    # stale file forever.
    relegacy = legacy_rerun_seasons(LEGACY_TSV)
    if relegacy:
        print(TAG + " Legacy re-runs (built despite being curated/past): "
              + ", ".join("S" + str(n) for n in sorted(relegacy)))

    for snum in sorted(season_items):
        rerun = snum in relegacy

        if snum in curated and not args.force and not rerun:
            print(TAG + " S" + str(snum) + ": skipped (curated data exists; use --force to override)")
            continue

        if is_past_season(snum, meta) and not args.force and not rerun:
            print(TAG + " S" + str(snum) + ": skipped (season already started/finished -- not upcoming)")
            continue

        items = season_items[snum]
        output = build_season_json(snum, items, meta, entm_tsv.name, observed)
        out_path = out_dir / ("upcoming_rewards_s" + str(snum) + ".json")

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            f.write("\n")

        print(TAG + " S" + str(snum) + ": written " + out_path.name
              + "  (" + str(len(items)) + " items, A-Z)")
        built += 1

    if built == 0:
        print(TAG + " No seasons built (all are curated or no data found).")
    else:
        print(TAG + " Done -- built " + str(built) + " season(s).")


if __name__ == "__main__":
    main()
