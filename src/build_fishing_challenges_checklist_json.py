#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
src/build_fishing_challenges_checklist_json.py
----------------------------------------------
Builds the data feed for the DF/BNB "Fishing Challenges Checklist" page
(/df/fishing/challenges-checklist/), rendered in the mini-season style by
renderFishingChallengesChecklist() in df-bnb-fishing.js.

The page groups every LIVE fishing challenge into four root expands:

    Daily                     - SCORE_Challenge_Daily_*  (scoreboard dailies)
    Weekly                    - SCORE_Challenge_Weekly_* (scoreboard weeklies)
    Big Fish in a Little Pond - Challenge_Lifetime_Fishing_CompleteDailies_*
                                (complete the "Big Fish in a Small Pond" daily
                                 7 / 14 / 28 times -> plushie plans)
    Lifetime                  - every other Challenge_Lifetime_Fishing_*

Daily + Weekly are NOT ticked (they repeat every reset). Big Fish + Lifetime get
checkboxes + a progress bar.

DATA SOURCES
  dist/challenges/challenges.json  -> base fishing items. build_challenges_json_v3.py
      already selects the fishing CHAL rows, leaf-expands their conditions
      (conditions_display) and flags METAs, so this script reuses that vetted work
      rather than re-parsing CHAL/CNDF. THIS SCRIPT MUST RUN AFTER build_challenges_json_v3.py.
  tsv/GMRW_Export_*.tsv (newest)   -> reward auto-derivation (ChallengeReward_<EDID>).
  tsv/MISC_Export_*.tsv (newest)   -> friendly item names for rewards.
  dist/fishing_big_fish.json       -> the Big Fish daily quest metadata (intro card).
  src/fishing_challenges_checklist_rewards.json -> hand-maintained reward overlay.

REWARD PRIORITY (per challenge):
  1. overlay[edid]            (authoritative; the plushie plans live here)
  2. "Score"                 (SCORE_ daily / weekly)
  3. non-zzz GMRW ChallengeReward record (Intro bait, LocalLegend bundle, Progress
     recipes, Burn bait...). The zzz_ChallengeReward_* records are DEPRECATED
     placeholders (all read "5x Bait - Common") and are IGNORED.
  4. ""  -> renderer shows "Reward not yet documented".

Two modes:
  (default / live)  reads dist/            -> dist/fishing_challenges_checklist.json
  --pts             reads dist/pts/ twins  -> dist/pts/fishing_challenges_checklist.json
                    (there is no PTS challenges base, so the challenge set is the
                     live base; only the Big Fish quest metadata is read from the
                     PTS twin.)

Env overrides (used by the in-session sandbox verifier, ignored in CI):
  FCC_OUT       override the output file path

No external dependencies (stdlib only).
"""

import os
import re
import sys
import csv
import io
import glob
import json
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(SCRIPT_DIR, "..")
DIST_DIR = os.path.join(ROOT, "dist")
TSV_DIR = os.path.join(ROOT, "tsv")

PTS = "--pts" in sys.argv


# ------------------------------------------------------------------ helpers
def newest(pattern):
    files = glob.glob(os.path.join(TSV_DIR, pattern))
    if not files:
        return None
    files.sort(key=os.path.getmtime)
    return files[-1]


def read_tsv(path):
    if not path or not os.path.exists(path):
        return []

    def _read(enc):
        with open(path, encoding=enc, errors="replace", newline="") as f:
            raw = f.read().replace("\x00", "")
        return list(csv.DictReader(io.StringIO(raw), delimiter="\t"))

    try:
        return _read("utf-8-sig")
    except UnicodeDecodeError:
        return _read("cp1252")


def load_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def norm_fid(v):
    return (str(v or "").split(":")[0]).strip().upper().lstrip("0").zfill(8)


def camel_words(s):
    s = re.sub(r"[_]+", " ", str(s))
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ------------------------------------------------------------------ reward DESC
# A reward's in-game description (flavour / effect text) lives in the DESC field
# of whichever record the reward item is (OMOD rod-mods, ALCH food, BOOK plans,
# ENTM/KEYM). FURN / MISC / WEAP carry no DESC, so cosmetic CAMP decor simply has
# none and the checklist shows nothing under the image (per design).
def _clean_desc(v):
    v = (v or "").strip()
    if not v or v.upper() in ("NONE", "NULL", "<P>", "</P>"):
        return ""
    return v


_DESC_SIDECAR_RE = re.compile(
    r"_(Properties|Effects|Locations|Refs|ObjectTemplate|SLOTS|HEADER|ACTI)\.tsv$", re.I)


def build_desc_lookup():
    """Normalised FormID -> DESC, merged from every DESC-bearing record export.
    Newest export wins (files read oldest-first, later overwrites)."""
    out = {}
    files = []
    for pat in ("OMOD_Export_*.tsv", "ALCH_Export_*.tsv", "BOOK_Export_*.tsv",
                "ENTM_Export_*.tsv", "KEYM_Export_*.tsv"):
        files += glob.glob(os.path.join(TSV_DIR, pat))
    files = [f for f in files if not _DESC_SIDECAR_RE.search(f)]
    files.sort(key=os.path.getmtime)   # newest last -> newest wins
    for path in files:
        rows = read_tsv(path)
        if not rows or "DESC" not in rows[0]:
            continue  # no player-facing DESC column in this export
        for row in rows:
            fid = norm_fid(row.get("FormID") or row.get("OMOD_FormID")
                           or row.get("ALCH_FormID") or "")
            desc = _clean_desc(row.get("DESC"))
            if fid and fid != "00000000" and desc:
                out[fid] = desc
    return out


def build_cobj_desc(desc_lookup):
    """Challenge FormID -> DESC of the item its COBJ crafts (first non-empty).
    A bobber gates both a display MISC (no DESC) and a weapon-mod OMOD (has DESC);
    we keep the first non-empty so the meaningful text wins."""
    out = {}
    for row in read_tsv(newest("COBJ_Export_*.tsv")):
        g = norm_fid(row.get("GNAM_FormID"))
        if not g or g == "00000000" or g in out:
            continue
        desc = desc_lookup.get(norm_fid(row.get("CNAM_FormID")), "")
        if desc:
            out[g] = desc
    return out


def build_gmrw_desc(desc_lookup):
    """Challenge-key -> DESC of the GMRW RewardedItem (matches build_gmrw_rewards keys)."""
    out = {}
    for row in read_tsv(newest("GMRW_Export_*.tsv")):
        m = re.match(r"^(zzz_|Burn_)?ChallengeReward_(.+)$", row.get("EDID") or "")
        if not m or (m.group(1) or "") == "zzz_":
            continue
        item = (row.get("RewardedItem") or "").strip()
        if not item:
            continue
        desc = desc_lookup.get(norm_fid(item.split(":")[0]), "")
        if desc:
            out.setdefault(m.group(2), desc)
    return out


# ------------------------------------------------------------------ guide links
# "Read the guide index, find the challenge's SUBJECT in one of the guides, link
# it." We resolve the site's fishing guide pages from tsv/guide_index.tsv (so URLs
# stay correct) and map each challenge's subject signals to the guide(s) that
# cover that subject. Topical only — challenges with no subject match stay blank.
_GUIDE_INDEX_PATH = os.path.join(TSV_DIR, "guide_index.tsv")


def build_guide_by_slug():
    out = {}
    for row in read_tsv(_GUIDE_INDEX_PATH):
        if (row.get("status") or "") != "published":
            continue
        if (row.get("visibility") or "") != "public":
            continue
        slug = (row.get("slug") or "").strip().lower()
        url = (row.get("url") or "").strip()
        title = (row.get("title") or "").strip()
        if slug and url and title:
            out.setdefault(slug, {"title": title, "url": url})
    return out


def guides_for(edid, name, reward, conds, guide_by_slug):
    e = (edid or "").lower()
    n = (name or "").lower()
    r = (reward or "").lower()
    hay = " ".join([e, n, r, " ".join(conds or [])]).lower()
    picks = []  # (priority, slug)

    # -- specific fishing subjects --
    if "axolotl" in hay:
        picks.append((1, "axolotl-guide"))
    if ("seasonalfish" in e or "seasonal" in n or "every season" in n):
        picks.append((1, "seasonal-fish-guide"))
    if "locallegend" in e or "local legend" in n:
        picks.append((1, "local-legends-guide"))
    if "completedailies" in e or "big fish" in n:
        picks.append((1, "big-fish-small-pond"))

    # -- reward-driven subjects --
    if re.search(r"\b(hook|reel|drag|bearing|handle)\b", r) or "gear ratio" in r \
            or "fish quest" in n or "_progress_" in e:
        picks.append((2, "reel-talk"))
    if re.search(r"\b(bobber|float|skin)\b", r):
        picks.append((2, "fishing-bobbers-floats"))
    if "fishbit" in e or "fish bit" in n or "fishbits" in n or "chum" in hay:
        picks.append((2, "linda-lee-chum-trough-rewards"))

    # -- conditions: bait / weather --
    if "bait" in hay or "rain" in hay or "sandstorm" in hay or "storm" in n:
        picks.append((3, "whats-biting"))

    # -- fish types / species / regions --
    if ("_region_" in e or "commonfish" in e or "uniquefish" in e
            or "glowingfish" in e or "glowing" in n or "sawgill" in n
            or "regional fish" in n or "catch any fish" in n
            or "small fish" in n or "medium fish" in n or "any fish" in n
            or ("region" in n and "in the" in n)):
        picks.append((3, "fish-types-guide"))

    # -- burning springs --
    if "burningsprings" in e or "burning springs" in n:
        picks.append((3, "springs"))

    # -- broad how-to (intro) --
    if "_intro" in e or "learn how to fish" in n:
        picks.append((4, "fishing-basics"))
        picks.append((5, "hook-line-ledger"))

    # generic fishing action with no specific subject -> Fishing Basics
    if not picks and "fish" in hay:
        picks.append((4, "fishing-basics"))

    picks.sort(key=lambda x: x[0])
    out, seen = [], set()
    for _, slug in picks:
        if slug in seen:
            continue
        g = guide_by_slug.get(slug)
        if not g:
            continue
        seen.add(slug)
        out.append({"title": g["title"], "url": g["url"]})
        if len(out) >= 4:
            break
    return out


# ------------------------------------------------------------------ item names
def build_name_lookup():
    names = {}
    for row in read_tsv(newest("MISC_Export_*.tsv")):
        fid = norm_fid(row.get("FormID"))
        full = (row.get("FULL") or row.get("FULL - Name") or "").strip()
        if fid and full and len(full) < 60:
            names[fid] = full
    return names


def item_name(itemref, names):
    """itemref like '007AC1AF:Fishing_Bait_Common:MISC'."""
    if not itemref:
        return ""
    parts = itemref.split(":")
    fid = norm_fid(parts[0])
    if fid in names:
        return names[fid]
    edid = parts[1] if len(parts) > 1 else itemref
    # Friendly-name a recipe / book edid.
    edid = re.sub(r"^(zzz_|Burn_)?Fishing_Recipe_mod_", "", edid)
    edid = re.sub(r"^(zzz_|Burn_)?Recipe_", "", edid)
    return camel_words(edid)


# ------------------------------------------------------------------ GMRW rewards
def build_gmrw_rewards(names):
    """
    Map challenge-EDID -> reward label, from NON-zzz ChallengeReward_* GMRW records.
    zzz_ records are deprecated placeholders and are skipped.
    """
    out = {}
    for row in read_tsv(newest("GMRW_Export_*.tsv")):
        edid = row.get("EDID") or ""
        m = re.match(r"^(zzz_|Burn_)?ChallengeReward_(.+)$", edid)
        if not m:
            continue
        prefix, key = m.group(1) or "", m.group(2)
        if prefix == "zzz_":
            continue  # deprecated placeholder
        item = (row.get("RewardedItem") or "").strip()
        cnt = (row.get("RewardedItemCount") or "").strip()
        cur = (row.get("QRCO_CurrencyObject") or "").strip()
        label = ""
        if item:
            nm = item_name(item, names)
            typ = item.split(":")[-1]
            noun = "Plan" if typ == "BOOK" else None
            if noun and not re.match(r"^(Plan|Recipe)\b", nm, re.I):
                label = "%s: %s" % (noun, nm)
            else:
                label = nm
            try:
                n = int(cnt)
                if n > 1:
                    label = "%dx %s" % (n, label)
            except (ValueError, TypeError):
                pass
        elif cur and "Caps001" in cur:
            label = "Caps"
        if label:
            out.setdefault(key, label)  # first non-zzz wins
    return out


# ------------------------------------------------------------------ COBJ unlocks
def _cobj_label(cobj_edid, cnam_full):
    """
    Turn a COBJ recipe (gated behind a challenge) into a reward label.
    Craftable items carry a CNAM_FULL ('Mirelurk Plushie', 'Rocket Bobber',
    'Jellied Fish'). Title unlocks have no item name — the title word lives in the
    COBJ EDID (…PlayerTitle_co_CondProxy_Suffix_OfTheMonth → 'Of The Month').
    """
    full = (cnam_full or "").strip()
    if full:
        return "Plan: " + full
    m = re.search(r"(?:PlayerTitle|CAMPTitle)_co_CondProxy_"
                  r"(?:Prefix|Suffix|Both)_([A-Za-z0-9]+)", cobj_edid or "")
    if m:
        return "Title: " + camel_words(m.group(1))
    m = re.search(r"_co_CondProxy_(?:[A-Za-z]+_)?([A-Za-z0-9]+)$", cobj_edid or "")
    if m:
        return "Plan: " + camel_words(m.group(1))
    return ""


def build_cobj_unlocks():
    """
    challenge FormID -> [reward labels] it unlocks, from COBJ GNAM references.
    A COBJ's GNAM points at the challenge that gates the recipe, so completing the
    challenge unlocks whatever the COBJ crafts (plushies, floats/bobbers, displays,
    cooking recipes, player/CAMP titles). This is the authoritative, generative
    reward link — no hand-mapping.
    """
    out = {}
    for row in read_tsv(newest("COBJ_Export_*.tsv")):
        g = norm_fid(row.get("GNAM_FormID"))
        if not g or g == "00000000":
            continue
        label = _cobj_label(row.get("COBJ_EDID"), row.get("CNAM_FULL"))
        if label:
            out.setdefault(g, [])
            if label not in out[g]:
                out[g].append(label)
    return out


# ------------------------------------------------------------------ GMRW rewards (by Ref)
def _gmrw_label(row, names):
    """Reward label for a GMRW ChallengeReward row (item / count / caps)."""
    item = (row.get("RewardedItem") or "").strip()
    cnt = (row.get("RewardedItemCount") or "").strip()
    cur = (row.get("QRCO_CurrencyObject") or "").strip()
    if item:
        nm = item_name(item, names)
        typ = item.split(":")[-1]
        if typ == "BOOK" and not re.match(r"^(Plan|Recipe)\b", nm, re.I):
            label = "Plan: " + nm
        else:
            label = nm
        try:
            n = int(cnt)
            if n > 1:
                label = "%dx %s" % (n, label)
        except (ValueError, TypeError):
            pass
        return label
    if cur and "Caps001" in cur:
        return "Caps"
    return ""


def build_gmrw_ref_rewards(names):
    """
    Challenge FormID -> reward label, resolved from the *Ref* columns of every
    non-zzz ``*ChallengeReward*`` GMRW record.

    The EDID-name matcher (build_gmrw_rewards) only catches records named
    ``ChallengeReward_<challenge EDID>``. Many rewards are named by the ITEM they
    grant (``Burn_ChallengeReward_Fishing_BaitImproved_3``,
    ``ChallengeReward_Challenge_Lifetime_Fishing_LocalLegend_Modules``) and link to
    the challenge through their Ref columns instead. This reads those, so Burning
    Springs / Sandstorm bait tiers and the Local Legend caps/modules/notes rewards
    resolve. Fishing-named records are processed first so a shared mega-reward can
    never steal a fishing tier's specific reward.
    """
    rows = read_tsv(newest("GMRW_Export_*.tsv"))
    rows.sort(key=lambda r: 0 if "Fishing" in (r.get("EDID") or "") else 1)
    out = {}
    for row in rows:
        edid = row.get("EDID") or ""
        if "ChallengeReward" not in edid or edid.startswith("zzz"):
            continue
        label = _gmrw_label(row, names)
        if not label:
            continue
        for key, val in row.items():
            if key and str(key).startswith("Ref") and ":CHAL" in (val or ""):
                out.setdefault(norm_fid(val.split(":")[0]), label)
    return out


# ------------------------------------------------------------------ PLYT titles
def build_plyt_titles():
    """
    Challenge FormID -> ``Title: <name>``, from Player-Title (PLYT) records whose
    unlock condition is ``HasCompletedChallenge(... [CHAL:xxxx])``. These are the
    reward for the top tier of several fishing ladders (e.g. Sandstorm 76 -> the
    "Dusty" prefix) and are invisible to GMRW/COBJ.
    """
    out = {}
    for row in read_tsv(newest("PLYT_Export_*.tsv")):
        title = (row.get("ANAM - Male Title") or row.get("ANAM")
                 or row.get("BNAM - Female Title") or "").strip()
        if not title:
            continue
        for key, val in row.items():
            if not val:
                continue
            for mm in re.finditer(
                    r"HasCompletedChallenge\([^)]*\[CHAL:([0-9A-Fa-f]{8})\]", str(val)):
                out.setdefault(norm_fid(mm.group(1)), "Title: " + title)
    return out


# ------------------------------------------------------------------ base items
def load_base_items():
    path = os.path.join(DIST_DIR, "challenges", "challenges.json")
    data = load_json(path)
    if not data:
        raise SystemExit(
            "[fishing-cc] dist/challenges/challenges.json not found. "
            "Run build_challenges_json_v3.py first."
        )
    try:
        return data["pages"]["fishing"]["items"]
    except (KeyError, TypeError):
        raise SystemExit("[fishing-cc] no pages.fishing.items in challenges.json")


# ------------------------------------------------------------------ grouping
GROUP_ORDER = ["daily", "weekly", "bigfish", "lifetime"]
GROUP_LABELS = {
    "daily": "Daily",
    "weekly": "Weekly",
    "bigfish": "Big Fish in a Little Pond",
    "lifetime": "Lifetime",
}
GROUP_BLURBS = {
    "daily": "",
    "weekly": "",
    "bigfish": "",
    "lifetime": "",
}
GROUP_TRACKABLE = {"daily": False, "weekly": False, "bigfish": True, "lifetime": True}


def group_key_for(it):
    g = (it.get("group") or "").strip()
    edid = it.get("edid") or ""
    if g == "Daily":
        return "daily"
    if g == "Weekly":
        return "weekly"
    if g == "Event":
        return None  # excluded
    if g == "Lifetime":
        if "CompleteDailies" in edid:
            return "bigfish"
        return "lifetime"
    return None


def type_label_for(gk, it):
    if it.get("is_meta"):
        return "META"
    if it.get("is_sub"):
        return "Sub challenge"
    return {"daily": "Daily", "weekly": "Weekly",
            "bigfish": "Big Fish", "lifetime": "Lifetime"}.get(gk, "Challenge")


# ------------------------------------------------------------------ build
def resolve_reward(edid, form_id, gk, overlay, cobj, gmrw,
                   gmrw_ref=None, plyt=None):
    """
    Generative reward resolution, in priority order:
      1. optional manual override (src overlay — empty by default)
      2. COBJ unlock (the plan / title the challenge gates) — the real reward
      3. "Score" for SCORE_ daily / weekly scoreboard challenges
      4. a non-zzz GMRW ChallengeReward record (Intro bait, Progress recipes,
         Local Legend caps/modules bundle)
      5. blank — individual fish steps that only feed their META
    """
    if edid in overlay and str(overlay[edid]).strip():
        return overlay[edid].strip(), "overlay"
    fid = norm_fid(form_id)
    if fid in cobj:
        return "; ".join(cobj[fid]), "cobj"
    if gk in ("daily", "weekly"):
        return "Score", "score"
    for key in (edid, re.sub(r"_META$", "", edid)):
        if key in gmrw:
            return gmrw[key], "gmrw"
    gmrw_ref = gmrw_ref or {}
    plyt = plyt or {}
    if fid in gmrw_ref:
        return gmrw_ref[fid], "gmrw"
    if fid in plyt:
        return plyt[fid], "title"
    return "", "none"


def make_item(it, gk, overlay, cobj, gmrw, images,
              cobj_desc=None, gmrw_desc=None, guide_by_slug=None,
              gmrw_ref=None, plyt=None):
    edid = it.get("edid") or ""
    reward, rsrc = resolve_reward(edid, it.get("form_id"), gk, overlay, cobj, gmrw,
                                 gmrw_ref, plyt)
    cobj_desc = cobj_desc or {}
    gmrw_desc = gmrw_desc or {}
    if rsrc == "cobj":
        reward_desc = cobj_desc.get(norm_fid(it.get("form_id")), "")
    elif rsrc == "gmrw":
        reward_desc = (gmrw_desc.get(edid)
                       or gmrw_desc.get(re.sub(r"_META$", "", edid), ""))
    else:
        reward_desc = ""
    guides = guides_for(
        edid, it.get("full") or it.get("name") or edid, reward,
        it.get("conditions_display") or it.get("conditions_human") or [],
        guide_by_slug or {})
    return {
        "form_id": it.get("form_id") or "",
        "edid": edid,
        "name": it.get("full") or it.get("name") or edid,
        "required": it.get("required") or 0,
        "counter": it.get("snam") or "",
        "type_label": type_label_for(gk, it),
        "group_key": gk,
        "reward": reward,
        "reward_source": rsrc,
        "reward_desc": reward_desc,
        "image_url": (images.get(edid) or "").strip(),
        "conditions_display": it.get("conditions_display")
            or it.get("conditions_human") or [],
        "is_meta": bool(it.get("is_meta")),
        "is_sub": False,
        "children": [],
        "guides": guides,
        "trackable": GROUP_TRACKABLE[gk],
    }


def nest_metas(items):
    """
    Attach *_SUB rows as children of their *_META row.

    Matches on the META's edid base (edid minus ``_META``) as a prefix, with two
    tolerances the raw game data needs:
      * plural/singular slip — ``Axolotls_META`` owns ``Axolotl_01_SUB`` (drop a
        trailing 's' from the base);
      * ``_SUB`` may sit mid-edid (``BurningSprings_SUB_Sawgill``), so we test for
        ``_SUB`` anywhere rather than as a suffix.
    Returns the flattened root list with children populated.
    """
    metas = [x for x in items if x["is_meta"]]
    # longest base first so a shorter META can't steal a longer one's subs.
    metas.sort(key=lambda m: len(m["edid"]), reverse=True)
    claimed = set()
    for meta in metas:
        base = re.sub(r"_META$", "", meta["edid"])
        prefixes = [base + "_"]
        if base.endswith("s"):
            prefixes.append(base[:-1] + "_")   # Axolotls -> Axolotl_
        for x in items:
            if x is meta or x["is_meta"] or id(x) in claimed:
                continue
            e = x["edid"]
            if "_SUB" in e and any(e.startswith(p) for p in prefixes):
                x["is_sub"] = True
                x["type_label"] = "Sub challenge"
                x["trackable"] = meta["trackable"]
                meta["children"].append(x)
                claimed.add(id(x))
        # order a META's children by their trailing tier / name for a clean read
        meta["children"].sort(key=lambda c: (_tier_num(c["edid"]), (c["name"] or "").lower()))
    roots = [x for x in items if id(x) not in claimed]
    return roots


# ------------------------------------------------------------------ ordering
REGION_SEQ = ["Forest", "Mire", "SavageDivide", "AshHeap", "CranBog",
              "ToxicValley", "SkylineValley"]


def _tier_num(edid):
    """Trailing two-digit tier (…_01 / …_02_META / …_03_SUB) -> int, else 0."""
    m = re.search(r"_(\d{2})(?:_[A-Za-z]+)?$", edid)
    return int(m.group(1)) if m else 0


def _lifetime_rank(it):
    """(family_rank, sub_rank) so the Lifetime group reads as a progression."""
    e = it["edid"]
    if e.endswith("_Intro"):
        return (0, 0)
    if "_Progress_" in e:                       # Fish Quest I -> IV
        return (10, _tier_num(e))
    if "SeasonalFish" in e:
        if e.endswith("_META"):
            return (60, 3)
        return (60, 0 if "Summer" in e else (1 if "Fall" in e else 2))
    if "LocalLegend" in e:
        return (70, _tier_num(e))
    if "Axolotl" in e:                          # Catch Any Axolotl before Catch All
        return (50, 0 if "CatchAxolotls" in e else 1)
    if "_FishInSandStorm_" in e:
        return (86, _tier_num(e))
    if "BurningSprings" in e and "_Any_" in e:  # Burning Springs "catch any" tiers
        return (85, _tier_num(e))
    if "AllRegions" in e:                       # capstone, last of the region block
        return (40, 99)
    if "BurningSprings_META" in e:
        return (40, 90)
    if "_Region_Common_" in e:
        return (40, 0)
    m = re.search(r"_Region_(\w+?)_META", e)
    if m:
        rn = m.group(1)
        return (40, REGION_SEQ.index(rn) + 1 if rn in REGION_SEQ else 50)
    if "AnyGlowing" in e:                        # Catch Any Glowing Fish tiers
        return (30, _tier_num(e))
    if re.search(r"_Any_\d", e):                 # Catch Any Fish tiers
        return (20, _tier_num(e))
    return (100, 0)


def sort_key(it, gk):
    """Ascending progression order per group (fixes 28→14→7 reversal)."""
    req = int(it["required"] or 0)
    name = (it["name"] or "").lower()
    if gk == "lifetime":
        rank, sub = _lifetime_rank(it)
        return (rank, sub, req, name)
    # bigfish / daily / weekly — plain ascending by required count
    return (req, name)


def build(is_pts):
    names = build_name_lookup()
    gmrw = build_gmrw_rewards(names)
    gmrw_ref = build_gmrw_ref_rewards(names)
    plyt = build_plyt_titles()
    cobj = build_cobj_unlocks()
    desc_lookup = build_desc_lookup()
    cobj_desc = build_cobj_desc(desc_lookup)
    gmrw_desc = build_gmrw_desc(desc_lookup)
    guide_by_slug = build_guide_by_slug()
    overlay_full = load_json(os.path.join(SCRIPT_DIR,
               "fishing_challenges_checklist_rewards.json"), {}) or {}
    overlay = overlay_full.get("rewards", {})
    images = overlay_full.get("images", {})
    base = load_base_items()

    big_fish_path = os.path.join(DIST_DIR, "pts" if is_pts else "",
                                 "fishing_big_fish.json")
    big = load_json(big_fish_path, {}) or {}
    q = big.get("quest", {}) or {}

    # bucket
    buckets = {k: [] for k in GROUP_ORDER}
    for it in base:
        edid = it.get("edid") or ""
        # Drop challenges that only mention fish but belong to other pages:
        # World Pet "catch a fish with an active pet Cat" tiers, and the
        # "Kill a Fisherman" bounty-hunt sub.
        if "WorldPets" in edid or "BurnBounty" in edid:
            continue
        gk = group_key_for(it)
        if not gk:
            continue
        buckets[gk].append(make_item(it, gk, overlay, cobj, gmrw, images,
                                     cobj_desc, gmrw_desc, guide_by_slug,
                                     gmrw_ref, plyt))

    groups = []
    for gk in GROUP_ORDER:
        roots = nest_metas(buckets[gk])
        # Ascending progression order (Big Fish 7→14→28; Lifetime by family).
        roots.sort(key=lambda x: sort_key(x, gk))
        # count trackable leaves (roots + children) for the progress bar
        track_ids = []
        if GROUP_TRACKABLE[gk]:
            for r in roots:
                track_ids.append(r["edid"])
                for c in r["children"]:
                    track_ids.append(c["edid"])
        groups.append({
            "key": gk,
            "label": GROUP_LABELS[gk],
            "blurb": GROUP_BLURBS[gk],
            "trackable": GROUP_TRACKABLE[gk],
            "count": len(roots),
            "track_ids": track_ids,
            "items": roots,
        })

    out = {
        "version": 1,
        "generated": datetime.now(timezone.utc).isoformat(),
        "isPts": is_pts,
        "source": {
            "base": "dist/challenges/challenges.json",
            "gmrwTsv": os.path.basename(newest("GMRW_Export_*.tsv") or ""),
            "overlay": "src/fishing_challenges_checklist_rewards.json",
        },
        "page": {
            "category": "Fishing",
            "title": "Fishing Challenges Checklist",
            "url": "/df/fishing/challenges-checklist/",
            "blurb": "Track every fishing challenge in Appalachia — daily and "
                     "weekly scoreboard challenges, the Big Fish daily-quest "
                     "milestones, and the one-time lifetime challenges.",
        },
        "bigFishQuest": {
            "name": q.get("name") or "Big Fish in a Small Pond",
            "gameName": q.get("gameName") or "",
            "npc": q.get("npc") or "Captain Raymond",
            "location": q.get("location") or "Fisherman's Rest",
            "fishRequired": q.get("fishRequired") or 7,
            "description": q.get("description") or "",
            "formId": q.get("formId") or "",
            "edid": q.get("edid") or "Fishing_BigFish",
        },
        "groups": groups,
        "gallery": [],
    }
    return out


def main():
    out = build(PTS)
    if os.environ.get("FCC_OUT"):
        out_path = os.environ["FCC_OUT"]
    else:
        sub = "pts" if PTS else ""
        out_path = os.path.join(DIST_DIR, sub, "fishing_challenges_checklist.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    n_track = sum(len(g["track_ids"]) for g in out["groups"])
    print("[fishing-cc] wrote %s" % out_path)
    for g in out["groups"]:
        nchild = sum(len(i["children"]) for i in g["items"])
        print("  %-26s %2d root rows (+%d nested)%s"
              % (g["label"], g["count"], nchild,
                              "  [trackable]" if g["trackable"] else ""))
    print("[fishing-cc] trackable leaves: %d" % n_track)


if __name__ == "__main__":
    main()
