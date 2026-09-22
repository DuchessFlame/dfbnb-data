#!/usr/bin/env python3
"""
build_random_encounters_json.py - Random Encounter guide pages (standalone).

Split out of build_challenges_json_v3.py (Sept 2026). The encounter code used to
ride along inside the challenges build; it now lives here on its own because the
encounter pages carry far more than a challenge row does (photo slots, hand-written
descriptions, a Technical expand) and that build was already 2,000 lines.

OUTPUT
    dist/random_encounters/random_encounters.json

    { "generated": ..., "source": {...},
      "pages": { "<guide-index id>": {
          "title", "url", "kind": "type" | "region" | "limited",
          "count", "encounters": [ {
              "id", "name", "game_name", "numeral",
              "type", "content", "description", "description_source",
              "image", "edid", "form_id", "quest_type", "location",
              "trigger", "rewards": [...], "merged": [ {edid, form_id} ... ] } ] } } }

WHICH PAGES
    The "... Random Encounters" / "Whitespring Refuge Encounters" guide pages and the two "Limited Time Random
    Encounters" pages in tsv/guide_index.tsv. The six "... Locations" pages are
    map pages and are NOT built here.

    Type pages (Assault / CAMP / Object / Scene / Travel) take every live encounter
    of that type, from every content pack, EXCEPT the limited-time ones (Zetan =
    Invaders from Beyond, SDOW = Shadows of the Dead of Winter) and the Whitespring
    ones, which have their own pages. Mining encounters sit on the Object page.
    Region / update pages (Burning Springs, Skyline Valley, Steel Dawn, Once in a
    Blue Moon) are the same encounters again, filtered by their EDID prefix — an
    encounter is on its type page AND its region page.

NAMING
    Random encounters have no player-facing name in the game; FULL is the
    designer's working title ("Camp - Chem Dealer", "[Bandit Bot]"). It is tidied
    (brackets, "Camp - " style prefixes, a trailing " RE" dropped, first letter
    capitalised) and can be replaced per encounter in the overrides TSV.

    Encounters that share a name on a page are ALL listed, each as its own expand,
    numbered I, II, III ... in EDID order ("Blood Eagle Hostile Camp I").
    The one exception is a `_GQ` twin (RE_ObjectMP11 / RE_ObjectMP11_GQ): that is
    the same encounter's quest-tracking copy, so it is folded into its base
    encounter and listed under "merged" in the Technical expand.

DESCRIPTIONS - in this order
    1. tsv/random_encounter_overrides.tsv  (Duchess's own text - always wins)
    2. the game's own DESC, where the record has one (18 of ~460 do)
    3. a drafted line where the NAME already says what happens ("X vs. Y")
    4. nothing -> the page shows "Description coming soon."

    Fill descriptions / names / photos in tsv/random_encounter_overrides.tsv
    (one row per encounter, keyed by EDID). New encounters get a row when you run
        python src/build_random_encounters_json.py --sync-overrides
    `description_source` records which one was used, so drafts can be found and
    replaced.

CUT CONTENT
    zzz / ZZZ / DEL / DELETED / CUT / _CUT / DoNotUse / Template / _Test / _Debug /
    _Dialogue records are dropped, not shown - none of them is an encounter a
    player can meet, so there is no page for a "Cut content" line to sit on.
"""

import csv
import io
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import tsv_source          # the one resolver for every export selection

CHANNEL = (os.environ.get("DFBNB_CHANNEL") or "live").strip().lower()
OUT_DIR = os.path.join(REPO, "dist", "pts" if CHANNEL == "pts" else "", "random_encounters")
OUT_PATH = os.path.join(OUT_DIR, "random_encounters.json")
GUIDE_INDEX = os.path.join(REPO, "tsv", "guide_index.tsv")
OVERRIDES = os.path.join(REPO, "tsv", "random_encounter_overrides.tsv")

csv.field_size_limit(2 ** 31 - 1)


# ---------------------------------------------------------------- io

def read_tsv(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as fh:
        raw = fh.read().replace("\x00", "")
    return list(csv.DictReader(io.StringIO(raw), delimiter="\t"))


def pick(row, *keys):
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


# ---------------------------------------------------------------- pages

# guide_index id -> (kind, rule). The rule decides which encounters the page holds.
PAGES = {
    "df-random-assault":                 ("type",    {"types": {"Assault"}}),
    "df-random-camp":                    ("type",    {"types": {"Camp"}}),
    "df-random-object":                  ("type",    {"types": {"Object", "Mining"}}),
    "df-random-scene":                   ("type",    {"types": {"Scene"}}),
    "df-random-travel":                  ("type",    {"types": {"Travel"}}),
    "df-random-whitespring":             ("type",    {"types": {"WhitespringAssault"}}),
    # The Refuge's indoor daily encounters (XPD_HubRE_*) - their own page, split
    # off the Whitespring (Golf Club assault) page Sept 2026.
    "df-random-whitespring-refuge":      ("type",    {"types": {"Hub"}}),
    "df-random-guides-burning-springs":  ("region",  {"content": "Burning Springs"}),
    "df-random-guides-skyline-valley":   ("region",  {"content": "Skyline Valley"}),
    "df-random-guides-blue-moon":        ("region",  {"content": "Once in a Blue Moon"}),
    "df-random-guides-steel-dawn":       ("region",  {"content": "Steel Dawn"}),
    "df-random-limited-slasher":         ("limited", {"content": "Shadows of the Dead of Winter"}),
    "df-random-limited-invaders":        ("limited", {"content": "Invaders from Beyond"}),
}

# Page-specific blurbs; pages not listed use the renderer's default for their kind.
BLURBS = {
    "df-random-whitespring":
        "The attacks on the Whitespring Golf Club, A to Z. Open one for a photo, what "
        "happens, and the technical details from the game files.",
    "df-random-whitespring-refuge":
        "The encounters inside the Whitespring Refuge, A to Z. These are daily jobs for the "
        "Refuge's residents, not encounters out in the world. Open one for a photo, what "
        "happens, and the technical details from the game files.",
}

# Content that has its own LIMITED page and so stays off the type pages.
LIMITED_CONTENT = {"Shadows of the Dead of Winter", "Invaders from Beyond"}

TYPE_LABELS = {
    "Assault": "Assault", "Camp": "CAMP", "Object": "Object", "Scene": "Scene",
    "Travel": "Travel", "Mining": "Mining", "WhitespringAssault": "Whitespring Assault",
    "Hub": "Whitespring Refuge",
}

# Trigger activator each type fires from (see render_random_encounter_maps.py).
TRIGGER_TYPES = {"Assault", "Camp", "Object", "Scene", "Travel", "Mining", "WhitespringAssault"}


# ---------------------------------------------------------------- classification

RE_TYPE = re.compile(r"(?:^|_)RE_(WhitespringAssault|Assault|Camp|Object|Scene|Travel|Mining)")


def is_dropped(edid, full):
    e, u, f = edid, edid.upper(), full.strip()
    if u.startswith(("CUT", "DEL", "ZZZ", "POST")) or "_CUT" in u:
        return True
    if any(t in u for t in ("DONOTUSE", "TEMPLATE", "_TEST", "_DEBUG", "_DIALOGUE")):
        return True
    if e.startswith(("COMP_", "CreatureDialogue")) or e == "RE_Parent":
        return True
    if not f or f.startswith(("[Template", "[Prototype", "TEMPLATE")):
        return True
    return False


def encounter_type(edid):
    if "HubRE" in edid:
        return "Hub"
    m = RE_TYPE.search(edid)
    return m.group(1) if m else ""


def content_of(edid):
    """Which update / region the encounter belongs to."""
    if "_Zetan" in edid:
        return "Invaders from Beyond"
    if edid.startswith("SDOW_"):
        return "Shadows of the Dead of Winter"
    if "_MOON_" in edid:
        return "Once in a Blue Moon"
    if edid.startswith("Burn_"):
        return "Burning Springs"
    if edid.startswith("Storm_"):
        return "Skyline Valley"
    if edid.startswith("BS_"):
        return "Steel Dawn"
    if edid.startswith("W05_"):
        return "Wastelanders"
    if edid.startswith("XPD_"):
        return "Whitespring Refuge"
    return "Appalachia (launch)"


def trigger_for(edid, etype):
    if etype not in TRIGGER_TYPES:
        return ""
    pre = "Burn_" if edid.startswith("Burn_") else "Storm_" if edid.startswith("Storm_") else ""
    return f"{pre}RETrigger{etype}"


def on_page(enc, kind, rule):
    if kind == "type":
        if enc["type"] not in rule["types"]:
            return False
        # Whitespring page takes its own encounters whatever the content;
        # the five type pages leave the limited-time ones to their own pages.
        if rule["types"] & {"WhitespringAssault", "Hub"}:
            return True
        return enc["content"] not in LIMITED_CONTENT
    return enc["content"] == rule["content"]


# ---------------------------------------------------------------- names + descriptions

_PREFIX = re.compile(r"^(?:Camp|Scene|Travel|Object|Assault|Event)\s*[-:]\s*", re.I)


def tidy_name(full, edid=""):
    n = full.strip()
    if n.startswith("[") and n.endswith("]"):
        n = n[1:-1].strip()
    n = _PREFIX.sub("", n)
    n = re.sub(r"\s+RE$", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    n = n[:1].upper() + n[1:] if n else n
    # RE_SceneKMK16_Nuke .. 21 are titled just "Behemoth", "Deathclaw" ... - the
    # nuke-zone variants. Say so, the way their siblings already do ("Nuked: ...").
    if edid.endswith("_Nuke") and "nuke" not in n.lower():
        n = f"Nuked: {n}"
    return n


_VS = re.compile(r"^(.+?)\s+(?:vs\.?|v)\s+(.+)$", re.I)


def drafted_description(name, etype):
    """Only when the NAME already says what happens. Anything else stays empty
    rather than guessing."""
    if etype in ("Assault", "Travel", "WhitespringAssault"):
        m = _VS.match(name)
        if m:
            a, b = m.group(1).strip(), m.group(2).strip()
            # "Zetans Vs Appalachia" is an invasion, not a two-sided fight.
            if "appalachia" in (a + " " + b).lower():
                return ""
            return f"{a} and {b} fighting each other."
    return ""


def game_description(desc):
    d = re.sub(r"^\s*:\s*", "", desc or "").strip()
    if not d or d.lower().startswith("debug"):
        return ""
    d = d[:1].upper() + d[1:]
    return d if d.endswith((".", "!", "?")) else d + "."


ROMAN = ["", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
         "XI", "XII", "XIII", "XIV", "XV"]


# ---------------------------------------------------------------- rewards

def load_names():
    names = {}
    for pat, fid_col in (("ALCH_Export_*.tsv", "ALCH_FormID"),
                         ("MISC_Export_*.tsv", "FormID"),
                         ("BOOK_Export_*.tsv", "FormID")):
        p = tsv_source.newest(pat, channel=CHANNEL, required=False,
                              exclude=("_Locations", "_Effects", "_EFFECTS"))
        for r in read_tsv(p):
            fid, full = pick(r, fid_col, "FormID"), pick(r, "FULL", "FULL - Name")
            if fid and full:
                names[fid.upper()] = full
    return names


def humanise_edid(edid):
    e = re.sub(r"^(?:LLS?|LL|QuestReward)_+", "", edid or "", flags=re.I)
    e = re.sub(r"([a-z])([A-Z])", r"\1 \2", e.replace("_", " "))
    return re.sub(r"\s+", " ", e).strip()


def load_rewards(names):
    """quest EDID -> [reward labels] from the GMRW export (ParentQuestLink)."""
    p = tsv_source.newest("GMRW_Export_*.tsv", channel=CHANNEL, required=False)
    out = defaultdict(list)
    for r in read_tsv(p):
        link = pick(r, "ParentQuestLink")
        if not link:
            continue
        q = link.split(":")[1] if ":" in link else link
        labels = []
        if pick(r, "NAM7_XPGlobal", "XPCT_XPCurveTable"):
            labels.append("XP")
        if pick(r, "NAM8_CapsGlobal") or pick(r, "QRCO_CurrencyObject").endswith(":Caps001:CNCY"):
            labels.append("Caps")
        item = pick(r, "RewardedItem")
        if item:
            parts = item.split(":")
            # A named item shows its name; a leveled list has none, so show its
            # editor ID as-is (this is the Technical expand) rather than a
            # mangled guess like "Stimpak 1 3".
            nm = names.get(parts[0].upper()) or (parts[1] if len(parts) > 1 else parts[0])
            cnt = pick(r, "RewardedItemCount")
            labels.append(f"{nm} x{cnt}" if cnt and cnt not in ("0", "1") else nm)
        for lb in labels:
            if lb not in out[q]:
                out[q].append(lb)
    return out


# ---------------------------------------------------------------- overrides

OVR_COLS = ["edid", "page", "game_name", "name", "description", "image"]


def load_overrides():
    return {pick(r, "edid"): r for r in read_tsv(OVERRIDES) if pick(r, "edid")}


def write_overrides(existing, rows):
    """Keep every hand-filled cell; add a row for any encounter not listed yet,
    so the file is always the full to-do list for descriptions and photos."""
    seen = set(existing)
    merged = [existing[k] for k in existing]
    for r in rows:
        if r["edid"] not in seen:
            merged.append(r)
            seen.add(r["edid"])
    merged.sort(key=lambda r: (pick(r, "page"), pick(r, "game_name").lower(), pick(r, "edid")))
    with open(OVERRIDES, "w", encoding="utf-8", newline="") as fh:
        fh.write("\t".join(OVR_COLS) + "\n")
        for r in merged:
            fh.write("\t".join(str(r.get(c, "") or "").replace("\t", " ").replace("\n", " ")
                               for c in OVR_COLS) + "\n")


# ---------------------------------------------------------------- build

def main():
    quest_path = tsv_source.newest("QUEST_Export_*.tsv", channel=CHANNEL)
    quests = read_tsv(quest_path)
    names = load_names()
    rewards = load_rewards(names)
    ovr = load_overrides()
    guide = {pick(r, "id"): r for r in read_tsv(GUIDE_INDEX)}

    # 1. every live encounter record
    by_edid = {}
    for r in quests:
        edid = pick(r, "EDID")
        if not ("_RE_" in edid or edid.startswith("RE_") or "HubRE" in edid):
            continue
        full = pick(r, "FULL - Name", "FULL")
        if is_dropped(edid, full):
            continue
        etype = encounter_type(edid)
        if not etype:
            continue
        by_edid[edid] = {
            "edid": edid, "form_id": pick(r, "FormID"), "game_name": full,
            "type": etype, "content": content_of(edid),
            "desc": pick(r, "DESC - Description", "DESC"),
            "quest_type": pick(r, "Quest Type"),
            "location": pick(r, "LNAM - Location"),
            "merged": [],
        }

    # 2. fold _GQ quest-tracking twins into their base encounter
    for edid in list(by_edid):
        if edid.endswith("_GQ") and edid[:-3] in by_edid:
            base, twin = by_edid[edid[:-3]], by_edid.pop(edid)
            base["merged"].append({"edid": twin["edid"], "form_id": twin["form_id"]})
            base.setdefault("reward_edids", [base["edid"]]).append(twin["edid"])
            if not base["desc"] and twin["desc"]:
                base["desc"] = twin["desc"]

    # 3. pages
    pages_out = {}
    ovr_rows = []
    for gid, (kind, rule) in PAGES.items():
        g = guide.get(gid, {})
        encs = [dict(e) for e in by_edid.values() if on_page(e, kind, rule)]

        for e in encs:
            o = ovr.get(e["edid"], {})
            e["name"] = pick(o, "name") or tidy_name(e["game_name"], e["edid"])

        # same name on this page -> I, II, III in EDID order
        groups = defaultdict(list)
        for e in encs:
            groups[e["name"].lower()].append(e)
        for grp in groups.values():
            grp.sort(key=lambda e: e["edid"].lower())
            for i, e in enumerate(grp, 1):
                e["numeral"] = ROMAN[i] if len(grp) > 1 and i < len(ROMAN) else ""

        out = []
        for e in encs:
            o = ovr.get(e["edid"], {})
            name = e["name"] + (f" {e['numeral']}" if e["numeral"] else "")
            if pick(o, "description"):
                desc, src = pick(o, "description"), "override"
            elif game_description(e["desc"]):
                desc, src = game_description(e["desc"]), "game"
            elif drafted_description(e["name"], e["type"]):
                desc, src = drafted_description(e["name"], e["type"]), "draft"
            else:
                desc, src = "", "none"
            rw = []
            for qe in e.get("reward_edids", [e["edid"]]):
                for lb in rewards.get(qe, []):
                    if lb not in rw:
                        rw.append(lb)
            out.append({
                "id": re.sub(r"[^a-z0-9]+", "-", e["edid"].lower()).strip("-"),
                "name": name, "game_name": e["game_name"], "numeral": e["numeral"],
                "type": TYPE_LABELS.get(e["type"], e["type"]),
                "content": e["content"],
                "description": desc, "description_source": src,
                "image": pick(o, "image"),
                "edid": e["edid"], "form_id": e["form_id"],
                "quest_type": e["quest_type"] if e["quest_type"] not in ("", "None") else "",
                "location": e["location"],
                "trigger": trigger_for(e["edid"], e["type"]),
                "rewards": rw,
                "merged": e["merged"],
            })
            ovr_rows.append({"edid": e["edid"], "page": gid, "game_name": e["game_name"],
                             "name": "", "description": "", "image": ""})

        # A-Z by the name the reader sees (numerals sort naturally: I < II < III ...)
        out.sort(key=lambda r: (r["name"].lower().rstrip(" ivx") if r["numeral"] else r["name"].lower(),
                                ROMAN.index(r["numeral"]) if r["numeral"] in ROMAN else 0))
        pages_out[gid] = {
            "title": pick(g, "title") or gid,
            "url": pick(g, "url"),
            "slug": pick(g, "slug"),
            "top_category": pick(g, "topCategory") or "Random Encounters",
            "kind": kind,
            "blurb": BLURBS.get(gid, ""),
            "count": len(out),
            "encounters": out,
        }

    # The overrides TSV is hand-edited, so CI never rewrites it. Run locally with
    # --sync-overrides to add rows for new encounters (existing cells are kept).
    if "--sync-overrides" in sys.argv or not os.path.exists(OVERRIDES):
        write_overrides(ovr, ovr_rows)
        print(f"[random-encounters] synced {os.path.relpath(OVERRIDES, REPO)}")

    doc = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": {"quest_export": os.path.basename(quest_path), "channel": CHANNEL},
        "pages": pages_out,
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=1)

    print(f"[random-encounters] {os.path.basename(quest_path)} -> {os.path.relpath(OUT_PATH, REPO)}")
    for gid, p in pages_out.items():
        srcs = defaultdict(int)
        for e in p["encounters"]:
            srcs[e["description_source"]] += 1
        print(f"  {p['title']:40} {p['count']:4}  descriptions: {dict(srcs)}")
    if not pages_out or not any(p["count"] for p in pages_out.values()):
        raise SystemExit("[random-encounters] every page came out empty - refusing to continue")


if __name__ == "__main__":
    main()
