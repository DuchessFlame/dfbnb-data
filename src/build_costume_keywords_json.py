#!/usr/bin/env python3
"""
build_costume_keywords_json.py
==============================
Builds the Costume Keyword Index for /df/score-challenges/costume-keyword-index/.

Score challenges like "Kill a Creature while Wearing a Costume" test for one
keyword: ClothingTypeCostume ("COSTUME"). Anything you wear that carries that
keyword counts; anything that doesn't, doesn't -- however much it looks like a
costume. This script lists every wearable that carries it, and every live
challenge that asks for it.

Reads from (channel-explicit -- a --pts build reads tsv/pts/ only):
  - KYWD_Export_*_Refs.tsv   which ARMO records carry ClothingTypeCostume, plus
                             every other keyword on those records. The ARMO
                             exports themselves carry no keyword column, so the
                             "who references this keyword" dump is the source,
                             the same one build_apparel_formids_json.py uses.
  - ARMO_Export_*_SLOTS.tsv  biped slots, which decide the Outfit / Hat / Mask /
                             Headwear / Eyewear pill.
  - CHAL_Export_*.tsv        Cond1..CondN, for the "Used For" challenge list.

Outputs:
  - dist/costume_keywords.json        (live)
  - dist/pts/costume_keywords.json    (--pts)

Output shape:
  {
    "keyword": "ClothingTypeCostume",
    "challenges": [ { "type": "Daily", "text": "Kill a Creature while Wearing a Costume",
                      "epic": false }, ... ],
    "items": [ { "name": "Alien Mothman Mask", "type": "Mask" }, ... ],
    "typeCounts": { "Outfit": N, "Hat": N, ... },
    "_meta": { "built": ..., "channel": "live", "source": ..., "slotsSource": ...,
               "challengeSource": ..., "itemCount": N, "challengeCount": N }
  }

Items are A-Z by name, one row per display name. Challenges are grouped
Daily / Weekly / Event / Lifetime, A-Z inside each.

No external dependencies -- stdlib only.

Usage:  python src/build_costume_keywords_json.py
PTS:    python src/build_costume_keywords_json.py --pts
"""

import csv
import io
import json
import os
import re
import sys
from collections import Counter
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import build_spin_wheel_json as spin  # noqa: E402  (shared is_cut() -- one cut rule site-wide)
import tsv_source                     # noqa: E402  (one resolver for every export selection)

ROOT = os.path.dirname(HERE)
PTS = "--pts" in sys.argv
CHANNEL = "pts" if PTS else "live"
OUT_DIR = os.path.join(ROOT, "dist", "pts") if PTS else os.path.join(ROOT, "dist")
OUT_NAME = "costume_keywords.json"

KEYWORD = "ClothingTypeCostume"

csv.field_size_limit(10**9)


def read_tsv(path):
    """xEdit writes Windows-1252 (e-acute in item names) and the PTS pull is not
    always re-encoded. UTF-8 first, cp1252 fallback -- only names are affected."""
    raw = open(path, "rb").read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")
    return list(csv.DictReader(io.StringIO(text, newline=""), delimiter="\t"))


# ─────────────────────────────────────────────────────────────────────────────
#  Which records are real, wearable items
# ─────────────────────────────────────────────────────────────────────────────
# On top of the shared cut rule (CUT_/ZZZ_/DEL_/TEST_...), these EDIDs are the
# NPC copies of player items -- same display name, worn by NPCs, never in a
# player's inventory. Most player items have one, so a page that kept them would
# list half the costumes twice; and where ONLY the NPC copy exists the item is
# not obtainable at all.
_NPC_COPY_EDID = [
    re.compile(r"NON_?PLAYABLE", re.I),
    re.compile(r"UN_?PLAYABLE", re.I),
    re.compile(r"_TEST(?:_|$)", re.I),
    re.compile(r"(?:^|_)NPC(?:_|$)", re.I),
    re.compile(r"^Creature_", re.I),        # Creature_Spooky_* -- Halloween NPC dress-up
]

# Display names that are not a player item even though the record is clean.
_SKIP_NAMES = set()


def is_npc_copy(edid):
    return any(p.search(edid or "") for p in _NPC_COPY_EDID)


# ─────────────────────────────────────────────────────────────────────────────
#  The pill: Outfit / Hat / Mask / Headwear / Eyewear / Underarmour / Accessory
# ─────────────────────────────────────────────────────────────────────────────
# The item's own name decides first, so the pill never argues with the name a
# player reads ("Cardboard Robot Helmet" is a helmet, whatever slots it takes).
# Only when the name says nothing do the biped slots decide. ClothingType*
# keywords are NOT used: the game stacks Hat + Mask + Headwear on the same
# record far too often for them to sort anything (a Fasnacht mask has all three).
#
#   Underarmour  ObjectTypeUnderarmor
#   Outfit       takes the body (BODY / Coverall)
#   -- by name --
#   Eyewear      glasses / goggles / eyepatch / monocle
#   Mask         mask (incl. gas masks and the sidekick eye masks)
#   Headwear     helmet / helm
#   Hat          hat / cap / beanie / bonnet / fedora / hood / headband / ears / horns
#   -- by slots, when the name says none of those --
#   Eyewear      eyes only
#   Headwear     the top of the head AND the face -- mascot heads and the like
#   Hat          the top of the head only
#   Mask         the face only
#   Accessory    anything else (a collar, an effect)
#
# _TYPE_OVERRIDE fixes a single item by display name if the rule reads it wrong.
_TYPE_OVERRIDE = {
    # "Item Name": "Hat",
}

_BODY = {"BODY", "Coverall"}
_TOP = {"Hair Top", "Hair Long", "Headband", "FaceGen Head"}
_FACE = {"Beard", "Mouth"}
TYPE_ORDER = ["Outfit", "Hat", "Headwear", "Mask", "Eyewear", "Underarmour", "Accessory"]


_NAME_TYPE = [
    ("Eyewear",  re.compile(r"\b(glasses|sunglasses|goggles|eyepatch|monocle|spectacles)\b", re.I)),
    ("Mask",     re.compile(r"\bmasks?\b", re.I)),
    ("Headwear", re.compile(r"\bhelm(et)?s?\b", re.I)),
    ("Hat",      re.compile(r"\b(hats?|cap|beanie|bonnet|fedora|hood|headband|headwrap|ears|horns|halo)\b", re.I)),
]


def item_type(name, slots, kws):
    if name in _TYPE_OVERRIDE:
        return _TYPE_OVERRIDE[name]
    if "ObjectTypeUnderarmor" in kws:
        return "Underarmour"
    if slots & _BODY:
        return "Outfit"
    for label, rx in _NAME_TYPE:
        if rx.search(name):
            return label
    top, face, eyes = bool(slots & _TOP), bool(slots & _FACE), "Eyes" in slots
    if eyes and not top and not face:
        return "Eyewear"
    if top and (face or eyes):
        return "Headwear"
    if top:
        return "Hat"
    if face:
        return "Mask"
    return "Accessory"


def collect_items():
    refs_path = tsv_source.newest("KYWD_Export_*_Refs.tsv", channel=CHANNEL)
    slots_path = tsv_source.newest("ARMO_Export_*_SLOTS.tsv", channel=CHANNEL)
    print(f"  Reading keywords from: {os.path.basename(refs_path)}")
    print(f"  Reading slots from:    {os.path.basename(slots_path)}")

    kws, names, edids = {}, {}, {}
    for row in read_tsv(refs_path):
        if (row.get("RefSignature") or "").strip() != "ARMO":
            continue
        fid = (row.get("RefFormID") or "").strip().upper()
        kw = (row.get("KeywordEDID") or "").strip()
        if not fid or not kw:
            continue
        kws.setdefault(fid, set()).add(kw)
        names.setdefault(fid, (row.get("RefName") or "").strip())
        edids.setdefault(fid, (row.get("RefEDID") or "").strip())

    slots = {}
    for row in read_tsv(slots_path):
        fid = (row.get("ARMO_FormID") or "").strip().upper()
        labels = (row.get("BOD2_FirstPersonFlagLabels") or "").strip()
        slots[fid] = {s.strip() for s in labels.split("|") if s.strip()}

    tagged = [f for f, k in kws.items() if KEYWORD in k]
    print(f"  ARMO records carrying {KEYWORD}: {len(tagged)}")

    by_name = {}
    dropped = Counter()
    for fid in tagged:
        name, edid = names.get(fid, ""), edids.get(fid, "")
        if not name:
            dropped["no name"] += 1
            continue
        if spin.is_cut(edid):
            dropped["cut"] += 1
            continue
        if is_npc_copy(edid):
            dropped["NPC copy"] += 1
            continue
        if name in _SKIP_NAMES or name.lower().startswith("npc "):
            dropped["skip name"] += 1
            continue
        t = item_type(name, slots.get(fid, set()), kws[fid])
        by_name.setdefault(name, []).append(t)

    print(f"  Dropped: {dict(dropped)}")
    items = []
    for name, types in by_name.items():
        # Same display name on several records (re-releases, season copies).
        # They are one item to a player; the commonest reading of its slots wins.
        t = Counter(types).most_common(1)[0][0]
        items.append({"name": name, "type": t})
    items.sort(key=lambda it: it["name"].lower())
    print(f"  Costume items (unique names): {len(items)}")
    return items, os.path.basename(refs_path), os.path.basename(slots_path)


# ─────────────────────────────────────────────────────────────────────────────
#  Used For -- the live challenges that test for the keyword
# ─────────────────────────────────────────────────────────────────────────────
#   Cond1 : 10000000|1.000000|WornHasKeyword|00 00 00|00 00|
#           ClothingTypeCostume "COSTUME" [KYWD:0044D49B]|...
#
# Full-token match: ClothingTypeCostumeUnstoppables is a different keyword (the
# Unstoppables-only challenges) and must not answer for this one.
_COND_RE = re.compile(r"\|" + KEYWORD + r"(?![A-Za-z0-9_])")
_EPIC_RE = re.compile(r"^\s*epic\s*-\s*", re.I)
CHAL_TYPE_ORDER = ["Daily", "Weekly", "Event", "Lifetime"]


def collect_challenges():
    path = tsv_source.newest("CHAL_Export_*.tsv", channel=CHANNEL, required=False)
    if not path:
        print("  [WARN] No CHAL_Export_*.tsv found -- Used For omitted", file=sys.stderr)
        return [], None
    print(f"  Reading challenges from: {os.path.basename(path)}")

    out, seen = [], set()
    for row in read_tsv(path):
        edid = (row.get("EDID") or "").strip()
        text = (row.get("FULL") or "").strip()
        if not text or text.upper() == "NONE" or spin.is_cut(edid):
            continue
        hit = any(
            col and col.startswith("Cond") and col != "CondCount" and val and _COND_RE.search(val)
            for col, val in row.items()
        )
        if not hit:
            continue
        ctype = (row.get("CNAM") or "").strip() or "Other"
        epic = bool(_EPIC_RE.match(text))
        clean = _EPIC_RE.sub("", text).strip()
        # The same line ships as several records (tiers, re-runs) -- one row each.
        sig = (ctype, clean.lower(), epic)
        if sig in seen:
            continue
        seen.add(sig)
        out.append({"type": ctype, "text": clean, "epic": epic})

    order = {t: i for i, t in enumerate(CHAL_TYPE_ORDER)}
    out.sort(key=lambda c: (order.get(c["type"], len(order)), c["text"].lower(), c["epic"]))
    print(f"  Challenges using {KEYWORD}: {len(out)}")
    return out, os.path.basename(path)


def main():
    print("=" * 60)
    print("  Building Costume Keyword Index JSON")
    print(f"  Mode: {CHANNEL.upper()}")
    print("=" * 60)

    items, source, slots_source = collect_items()
    challenges, chal_source = collect_challenges()

    if len(items) < 100:
        # A healthy export has several hundred. Never publish a near-empty page
        # over a good one.
        print(f"ERROR: only {len(items)} costume items found -- not writing", file=sys.stderr)
        return 1

    counts = Counter(it["type"] for it in items)
    output = {
        "keyword": KEYWORD,
        "challenges": challenges,
        "items": items,
        "typeCounts": {t: counts[t] for t in TYPE_ORDER if counts.get(t)},
        "_meta": {
            "built": date.today().isoformat(),
            "channel": CHANNEL,
            "source": source,
            "slotsSource": slots_source,
            "challengeSource": chal_source,
            "itemCount": len(items),
            "challengeCount": len(challenges),
        },
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, OUT_NAME)
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"\n  Written to: {out_path}")
    print(f"  Items: {len(items)}  {dict(output['typeCounts'])}")
    print(f"  Challenges: {len(challenges)}")
    print("  Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
