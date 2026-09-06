#!/usr/bin/env python3
"""
build_displays_json.py
----------------------
Build dist/displays.json for the DF/BNB "Plan Checklists - Display Checklist"
page (/df/plan-checklists/displays/).

ROSTER SOURCE — the keyword, not a leveled list.
    Every in-game display surface carries PlayerDisplayCaseKeyword (003F51E3).
    Walking the keyword's refs in KYWD_Export_*_Refs.tsv picks up all of them in
    one pass: base-game craftables, Atom Shop, scoreboard and event displays
    alike. ATX_workshop_LL_DisplayCases was rejected as the roster source — it
    only holds the 21 Atom Shop cases, so it would silently drop two thirds of
    the page.

ART — resolved through the entitlement, never guessed from the EDID.
    An ACTI display EDID does NOT predict its texture stem (ATX_DisplayCase_
    BeerSteins_Cultist -> atx_camp_displaycase_beersteins_cultist: the game
    inserts a "camp" infix the EDID has no trace of). So art is resolved:
        ACTI  --HasEntitlement condition in LVLI_Export_*_LVLI_Entries.tsv-->
        ENTM  --ETDI / ECIL_n columns in ENTM_Export_*.tsv-->  texture stems
    ETDI stem + "_l" is the transparent tile (the row thumb and the first
    Item Image frame); each ECIL stem is a carousel shot. This matches the
    resource-producers contract in build_camp_items_json.py.

    Fallback when no LVLI condition names the entitlement: match an ENTM whose
    FULL is exactly the ACTI's display name, preferring the candidate sharing
    the most EDID characters. Recovers ~21 further items; anything still
    unresolved is a base-game craftable that never had a storefront tile.

IMAGES — `images` lists only stems whose .avif is actually staged, so the
    front end never points at a 404. An empty list renders the dashed
    placeholder slot and needs no code change when art lands later.

Usage:
    python build_displays_json.py --tsv-root tsv --avif-dir "<staged avif>" \
        --outdir dist [--pts]
"""

import argparse
import collections
import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

KEYWORD_FORMID = "003F51E3"          # PlayerDisplayCaseKeyword
DDS_RE  = re.compile(r"([A-Za-z0-9_\-\.]+?\.dds)", re.I)
ENT_RE  = re.compile(r'HasEntitlement\(.*?([A-Za-z0-9_]+)\s+"(.*?)"\s*\[ENTM:([0-9A-Fa-f]{8})\]')
ACTI_RE = re.compile(r"([0-9A-Fa-f]{8}):([^:]+):ACTI")
CUT_PREFIXES = ("ZZZ", "TEST", "CUT", "REUSE", "TEMPLATE", "DEL", "POST")


def latest(tsv_root, pattern):
    """Newest file by mtime — new exports are picked up just by dropping them in."""
    hits = glob.glob(os.path.join(tsv_root, pattern))
    if not hits:
        sys.exit("No TSV matching {} in {}".format(pattern, tsv_root))
    return max(hits, key=os.path.getmtime)


def read_tsv(path):
    with open(path, encoding="utf-8", errors="ignore") as fh:
        yield from csv.DictReader(fh, delimiter="\t")


def is_cut(edid):
    """Cut content = dev/test/placeholder records the page must not list.

    Matched as a PREFIX (ZZZ_, REUSE_, TEMPLATE_ ...) *and*, for "TEST", anywhere
    in the EDID: the marker is not always leading — WorkshopDisplay_TestAllWeapons01
    carries it mid-string and would otherwise render as a real display."""
    s = (edid or "").strip().upper()
    if "TEST" in s:
        return True
    return any(s.startswith(p) for p in CUT_PREFIXES)


def classify_source(edid):
    """(source label, unlock hint) from the EDID prefix."""
    s = (edid or "")
    m = re.match(r"(?i)^(?:zzz_?)?SCORE_S(\d+)_", s)
    if m:
        n = m.group(1)
        return ("Season {}".format(n),
                "Earned on the Season {} scoreboard. Once claimed it is yours permanently.".format(n))
    if re.match(r"(?i)^(?:zzz_?)?SCORE_MiniSeason_(\d+)", s):
        y = re.match(r"(?i)^(?:zzz_?)?SCORE_MiniSeason_(\d+)", s).group(1)
        return ("Mini-Season {}".format(y),
                "Earned during the {} mini-season event.".format(y))
    if re.match(r"(?i)^(?:zzz)?ATX_", s):
        return ("Atom Shop", "Bought from the Atom Shop, either on its own or inside a bundle.")
    if re.match(r"(?i)^(?:zzz_?)?Fishing_", s):
        return ("Fishing", "Unlocked through the fishing system.")
    if re.match(r"(?i)^(Moth_|MOON_|MN2_|SSE_|Meat_|DE\d{4}_)", s):
        return ("Seasonal Event", "Awarded during a limited-time seasonal event.")
    return ("Base Game", "Craftable at a C.A.M.P. or workshop once the plan is known.")


def ecil_stems(row):
    """ECIL_1 packs every carousel frame into one unseparated run of .dds names,
    so scan each ECIL_n for filenames rather than splitting on a delimiter."""
    out = []
    try:
        count = int((row.get("ECIL_Count") or "0") or 0)
    except ValueError:
        count = 0
    for i in range(1, max(count, 1) + 1):
        for m in DDS_RE.finditer(row.get("ECIL_{}".format(i)) or ""):
            stem = m.group(1)[:-4]
            if stem not in out:
                out.append(stem)
    return out


def build(tsv_root, avif_dir):
    kywd_path = latest(tsv_root, "KYWD_Export_*_Refs.tsv")
    lvli_path = latest(tsv_root, "LVLI_Export_*_LVLI_Entries.tsv")
    entm_path = latest(tsv_root, "ENTM_Export_*.tsv")
    for p in (kywd_path, lvli_path, entm_path):
        print("  source: {}".format(os.path.basename(p)), file=sys.stderr)

    roster = [
        {"formid": (r.get("RefFormID") or "").strip(),
         "edid":   (r.get("RefEDID") or "").strip(),
         "name":   (r.get("RefName") or "").strip()}
        for r in read_tsv(kywd_path)
        if (r.get("KeywordFormID") or "").strip().upper() == KEYWORD_FORMID
        and (r.get("RefSignature") or "").strip() == "ACTI"
    ]
    print("  roster from PlayerDisplayCaseKeyword: {}".format(len(roster)), file=sys.stderr)

    acti_to_entm = {}
    for r in read_tsv(lvli_path):
        m = ACTI_RE.match(r.get("LVLO_Reference") or "")
        if not m:
            continue
        cond = " ".join((r.get("Cond{}".format(i)) or "") for i in range(1, 11))
        me = ENT_RE.search(cond)
        if me:
            acti_to_entm.setdefault(m.group(1).upper(),
                                    {"edid": me.group(1), "name": me.group(2),
                                     "formid": me.group(3).upper()})

    entm_rows = list(read_tsv(entm_path))
    by_fid  = {(r.get("FormID") or "").strip().upper(): r for r in entm_rows}
    by_full = collections.defaultdict(list)
    for r in entm_rows:
        full = (r.get("FULL") or "").strip()
        if full:
            by_full[full.lower()].append(r)

    staged = {os.path.splitext(f)[0].lower()
              for f in os.listdir(avif_dir) if f.lower().endswith(".avif")} if avif_dir and os.path.isdir(avif_dir) else set()
    print("  staged .avif stems: {}".format(len(staged)), file=sys.stderr)

    items = []
    n_ent = n_img = 0
    for it in roster:
        fid = it["formid"].upper()
        ent = acti_to_entm.get(fid)
        row = by_fid.get(ent["formid"]) if ent else None
        if row is None:
            cands = by_full.get(it["name"].lower(), [])
            if cands:
                target = set((it["edid"] or "").lower())
                row = max(cands, key=lambda c: len(set((c.get("EDID") or "").lower()) & target))
                ent = {"edid": (row.get("EDID") or "").strip(),
                       "name": (row.get("FULL") or "").strip(),
                       "formid": (row.get("FormID") or "").strip().upper()}
        if ent:
            n_ent += 1

        etdi = ((row.get("ETDI") or "").strip() if row else "")
        stem = etdi[:-4] if etdi.lower().endswith(".dds") else etdi
        # Transparent tile first (row thumb + lead Item Image frame), then the
        # carousel shots. Only stems with a staged .avif are emitted.
        ordered = ([stem + "_l"] if stem else []) + (ecil_stems(row) if row else [])
        images = [s.lower() for s in ordered if s.lower() in staged]
        if images:
            n_img += 1

        source, hint = classify_source(it["edid"])
        items.append({
            "id": "DISPLAY_" + it["formid"].upper(),
            "name": it["name"] or it["edid"],
            "source": source,
            "obtain": (ent or {}).get("name") and
                      "Unlocked by the \"{}\" entitlement.".format(ent["name"]) or
                      "Craftable in a C.A.M.P. or workshop.",
            "unlock_hint": hint,
            "desc": ((row.get("DESC") or "").strip() if row else ""),
            "added": "",
            "images": images,
            "entitlement": ({"edid": ent["edid"], "formid": ent["formid"]} if ent else {}),
            "items": [{"label": it["name"] or it["edid"], "edid": it["edid"],
                       "formid": it["formid"], "kind": "activator", "texture": stem}],
            "cut": is_cut(it["edid"]),
        })

    items.sort(key=lambda x: (x["name"].lower(), x["id"]))
    print("  with entitlement: {}   with staged art: {}".format(n_ent, n_img), file=sys.stderr)
    return {
        "version": 1,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(items),
        "source_files": {"kywd": os.path.basename(kywd_path),
                         "lvli": os.path.basename(lvli_path),
                         "entm": os.path.basename(entm_path)},
        "displays": items,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv-root", default="tsv")
    ap.add_argument("--avif-dir", default="")
    ap.add_argument("--outdir", default="dist")
    ap.add_argument("--pts", action="store_true", help="write to dist/pts/ instead")
    a = ap.parse_args()
    data = build(a.tsv_root, a.avif_dir)
    outdir = os.path.join(a.outdir, "pts") if a.pts else a.outdir
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "displays.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    print("Wrote {} ({} displays)".format(out, data["count"]), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
