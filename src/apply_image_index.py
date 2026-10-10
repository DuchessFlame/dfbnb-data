#!/usr/bin/env python3
r"""
apply_image_index.py - point every reward page at the shared image library, and
write the missing-images to-do list. Works on the JSON already in dist/, so no
page has to be rebuilt (the full seasonal build runs out of memory locally).

    python src/apply_image_index.py              # update the pages + write the report
    python src/apply_image_index.py --report     # report only, change nothing
    python src/apply_image_index.py --pages seasonal daily-ops

It runs exactly the functions the builders call before they write
(image_index.seasonal_page / mutated_page / daily_ops_page /
unique_rewards_page / treasure_maps_page), so a later full build produces the
same images this does.

The plan checklists are not rewritten here - run src/add_plan_images.py for
those (it re-resolves plan_master.json in seconds, no rate walk). They ARE in
the report.

REPORT
  dist/missing_images.json      machine-readable, grouped by page
  audits/missing_images.md      the to-do list: every item with no picture
                                anywhere, grouped by page, with the folder and
                                file name to upload it as
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sys
from collections import OrderedDict, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
REPO = os.path.dirname(HERE)

import image_index as ii                                    # noqa: E402

TAG = "[apply_image_index]"
DIST = os.path.join(REPO, "dist")

# file, how json.dump wrote it (kept so a re-write does not reformat the file)
FILES = {
    "seasonal":      ("seasonal_events/seasonal_events_rewards_by_page.json", {"indent": 2}),
    "mutated":       ("mutated_events/mutated_events_all_rewards.json", {"indent": 1}),
    "daily-ops":     ("daily_ops/daily_ops_all_rewards.json", {"separators": (",", ":")}),
    "activities":    ("activities/activities_rewards_by_page.json", {"separators": (",", ":")}),
    "public-events": ("events/events_rewards_by_page.json", {"separators": (",", ":")}),
    "treasure-maps": ("treasure_maps.json", {"indent": 2}),
}

PLAN_PAGE_LABEL = {
    "apparel": "Apparel", "body-armour": "Body Armour", "backpack": "Backpack Mods",
    "recipes": "Recipes", "weapons": "Weapons", "mines-and-grenades": "Mines and Grenades",
    "underarmour": "Underarmour", "workshop": "Workshop (C.A.M.P.)", "camera": "Camera Mods",
    "photomode": "Photo Mode", "power-armour": "Power Armour", "fishing-rod": "Fishing Rod",
    "snowglobe": "Snow Globes", "legacy-nuclear-winter": "Legacy Nuclear Winter",
    "display": "Displays", "pts-pennants": "Pennants", "scoreboard-art": "Scoreboard Art",
}


# Report section order, and the categories whose renderers draw no reward
# thumbnails (their rows go in a lower-priority second part of the report).
CATEGORY_ORDER = ["plan-checklists", "seasonal-events", "mutated-events", "daily-ops",
                  "activities", "public-events", "treasure-maps"]
NO_THUMBS = {"activities", "public-events", "treasure-maps"}
CATEGORY_LABEL = {"plan-checklists": "Plan Checklists", "seasonal-events": "Seasonal Events",
                  "mutated-events": "Mutated Events", "daily-ops": "Daily Ops",
                  "activities": "Activities", "public-events": "Public Events",
                  "treasure-maps": "Treasure Maps"}


def page_label(page):
    """'seasonal-events/holiday-scorched-all-rewards' -> 'Seasonal Events - Holiday Scorched'."""
    cat, _, slug = page.partition("/")
    if cat == "plan-checklists":
        name = PLAN_PAGE_LABEL.get(slug, slug)
    else:
        name = slug.replace("-all-rewards", "").replace("-", " ").title()
    return "{} - {}".format(CATEGORY_LABEL.get(cat, cat), name)


def log(msg):
    print("{} {}".format(TAG, msg))


def load_json(rel):
    path = os.path.join(DIST, rel)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json(rel, data, fmt):
    path = os.path.join(DIST, rel)
    tmp = path + ".part"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, **fmt)
    with open(tmp, encoding="utf-8") as f:
        json.load(f)                                       # parse-check first
    os.replace(tmp, path)


def fingerprint(obj):
    """Every image field in obj, to tell whether apply() changed anything."""
    out = []

    def walk(o):
        if isinstance(o, dict):
            if "imageUrl" in o or "images" in o:
                out.append((ii.item_fid(o), o.get("imageUrl"), tuple(o.get("images") or ())))
            for v in o.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(obj)
    return out


def run_pages(which, write, idx):
    misses, totals = [], defaultdict(int)
    for key in which:
        rel, fmt = FILES[key]
        if not os.path.exists(os.path.join(DIST, rel)):
            log("  {}: {} not found - skipped".format(key, rel))
            continue
        data = load_json(rel)
        before = fingerprint(data)
        stats = defaultdict(int)

        def add(st, ms):
            for k, v in st.items():
                stats[k] += v
            misses.extend(ms)

        if key == "seasonal":
            for slug, page in (data.get("byPage") or {}).items():
                if "/" in slug or not isinstance(page, dict):
                    continue
                add(*ii.seasonal_page(page, slug, index=idx))
            # the same page object is also stored under its URL keys
            for slug, page in (data.get("byPage") or {}).items():
                if "/" in slug and isinstance(page, dict):
                    ii.seasonal_page(page, slug, index=idx)
        elif key == "mutated":
            for slug, page in (data.get("byPage") or {}).items():
                add(*ii.mutated_page(page, slug, index=idx))
        elif key == "daily-ops":
            add(*ii.daily_ops_page(data, data.get("slug") or "daily-ops-all-rewards", index=idx))
        elif key in ("activities", "public-events"):
            cat = "activities" if key == "activities" else "public-events"
            pages = data.get("byPage") or {}
            done = set()
            for slug, page in pages.items():
                if not isinstance(page, dict):
                    continue
                st, ms = ii.unique_rewards_page(page, slug.strip("/").rsplit("/", 1)[-1], cat, index=idx)
                for k, v in st.items():
                    stats[k] += v
                if id(page) not in done and "/" not in slug:
                    misses.extend(ms)
                done.add(id(page))
        elif key == "treasure-maps":
            add(*ii.treasure_maps_page(data, index=idx))

        old = set(before)
        changed = sum(1 for x in fingerprint(data) if x not in old)
        for k, v in stats.items():
            totals[k] += v
        log("  {:14s} {}  - {} image fields changed".format(
            key, ", ".join("{} {}".format(v, k) for k, v in sorted(stats.items())) or "no items",
            changed))
        if write and changed:
            save_json(rel, data, fmt)
            log("    wrote dist/{}".format(rel))
            if key in ("activities", "public-events"):
                # The site fetches the per-page slices, not the monolith.
                from by_page_slices import write_by_page_slices
                fam = "activities" if key == "activities" else "events"
                n, size = write_by_page_slices(os.path.dirname(os.path.join(DIST, rel)),
                                               data.get("byPage") or {}, name=fam)
                log("    re-sliced {} pages into dist/{}/by_page/".format(n, os.path.dirname(rel)))
        del data
    return misses, totals


def plan_checklist_misses(idx):
    """Plan checklist rows with no picture anywhere (the mod box counts as a
    picture - it is the intended art for a weapon mod plan)."""
    try:
        items = load_json("plan_master.json").get("items") or []
    except (OSError, ValueError) as exc:
        log("  plan_master.json not read ({}) - plan checklists left out".format(exc))
        return []
    out = []
    for it in items:
        cnam = (it.get("cnam") or {}).get("formid") or ""
        plan = (it.get("plan_item") or {}).get("formid") or ""
        current = []
        for s in it.get("images") or []:
            s = str(s or "")
            if s:
                current.append(s if s.startswith("/") else
                               ii.PLAN_IMG_BASE + (it.get("image_dir") or "") + "/" + s + ".avif")
        urls, src = idx.resolve(cnam or plan, (it.get("cnam") or {}).get("edid") or "",
                                it.get("name") or "", current)
        if not urls and plan and cnam:
            urls, src = idx.resolve(plan, "", it.get("name") or "", current)
        if urls:
            continue
        folder = it.get("image_dir") or it.get("type") or ""
        out.append({"page": "plan-checklists/" + folder, "name": it.get("name") or "",
                    "formid": (cnam or plan).upper(), "planFormid": plan.upper(),
                    "edid": (it.get("cnam") or {}).get("edid") or "",
                    "folder": ii.PLAN_IMG_BASE + folder + "/" if folder else "",
                    "cut": bool(it.get("cut")), "isOutfit": _is_outfit(it)})
    return out


def _is_outfit(it):
    blob = " ".join([it.get("name") or "", (it.get("cnam") or {}).get("edid") or ""]).lower()
    return "outfit" in blob or "costume" in blob or "uniform" in blob


_RX_LIBRARY_KIND = re.compile(r"^\s*(plan|recipe|schematic)\s*:|recipe_|outfit|apparel|headwear|"
                              r"mask\b|\bhat\b|backpack|paint|skin_", re.I)


# Item types that always belong in the shared library.
_LIBRARY_SIGS = {"ARMO", "WEAP", "BOOK", "OMOD", "APPAREL", "WEAPON", "PLAN", "ARMOR", "ARMOUR"}


def upload_target(m, plan_by_fid):
    """(folder, filename) a missing item should be uploaded as.

    Anything the plan checklists know (a plan, or the item a plan builds) goes
    in the shared library under its plan-checklist type folder; anything else
    is an odd reward that exists nowhere else, so it goes in its own page's
    reward folder. Named after the FormID of the thing in the picture.
    """
    p = plan_by_fid.get(m["formid"])
    fid = m["formid"]
    folder = m.get("folder") or ""
    if p:
        fid = ((p.get("cnam") or {}).get("formid") or fid).upper()
        if p.get("image_dir"):
            folder = ii.PLAN_IMG_BASE + p["image_dir"] + "/"
    elif (_RX_LIBRARY_KIND.search(m.get("name") or "") or _RX_LIBRARY_KIND.search(m.get("edid") or "")
          or (m.get("kind") or "").upper() in _LIBRARY_SIGS):
        # A plan / recipe / outfit / weapon the plan checklists do not carry
        # still belongs in the shared library, in the folder their own rule
        # (plan_images.page_folder) picks for it.
        try:
            import plan_images
            kind = (m.get("kind") or "").upper()
            lib = plan_images.page_folder({
                "name": m.get("name") or "",
                "type": {"ARMO": "apparel", "APPAREL": "apparel", "WEAP": "weapon",
                         "WEAPON": "weapon"}.get(kind, ""),
                "cnam": {"edid": m.get("edid") or "", "sig": kind if len(kind) == 4 else ""}})
        except Exception:                                   # noqa: BLE001
            lib = ""
        if lib:
            folder = ii.PLAN_IMG_BASE + lib + "/"
    name = fid + ("_go.avif" if m.get("isOutfit") or (p and _is_outfit(p)) else ".avif")
    return folder.replace(ii.UPLOADS, "wp-content/uploads/"), name


# The local staging copy spells two folders differently from the server.
LOCAL_FOLDER = {"legacy-nuclear-winter": "legacy nuclear winter",
                "mines-and-grenades": "Mines and Grenades"}


def local_staged():
    r"""{folder: {stem}} from data/plan_images.json - the scan of the local
    "Guides and Stuff\.Plan Checklist" staging copy (add_plan_images.py
    --avif-dir refreshes it)."""
    try:
        with open(os.path.join(REPO, "data", "plan_images.json"), encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, ValueError):
        return {}
    return {k: set(str(x).lower() for x in v) for k, v in (cfg.get("staged_images") or {}).items()}


def local_copy(m, plan_row, local):
    """(folder, filename) of a copy already in the local staging folder, or None.

    These were staged on this computer but never uploaded (or uploaded under
    another name): the page has been pointing at a 404.
    """
    if not local:
        return None
    stems = []
    if plan_row is not None:
        try:
            import plan_images
            stems = plan_images.candidate_stems(plan_row)
        except Exception:                                   # noqa: BLE001
            stems = []
    import re as _re
    base = _re.sub(r"^\s*(plan|recipe|schematic)\s*:\s*", "", m.get("name") or "", flags=_re.I).lower()
    if base:
        stems += [_re.sub(r"[^a-z0-9]+", "-", base).strip("-"), _re.sub(r"[^a-z0-9]+", "_", base).strip("_")]
    want = (plan_row or {}).get("image_dir") or ""
    folders = [want] + [f for f in local if f != want] if want else list(local)
    for st in stems:
        for f in folders:
            if st and st in local.get(f, ()):
                return f, st + ".avif"
    return None


def write_report(misses, idx, totals, report_dir=None):
    plan_by_fid = {}
    try:
        for it in load_json("plan_master.json").get("items") or []:
            for f in ((it.get("plan_item") or {}).get("formid"), (it.get("cnam") or {}).get("formid")):
                if f:
                    plan_by_fid.setdefault(f.upper(), it)
    except (OSError, ValueError):
        pass

    local = local_staged()
    groups = OrderedDict()
    seen = set()
    for m in misses:
        if (m["page"], m["formid"]) in seen:           # one row per item per page
            continue
        seen.add((m["page"], m["formid"]))
        m["uploadTo"], m["fileName"] = upload_target(m, plan_by_fid)
        have = local_copy(m, plan_by_fid.get(m["formid"]) or plan_by_fid.get(m.get("planFormid") or ""), local)
        if have:
            # Upload the file you already have, under its own name - the
            # index finds it by name. FormID names are for NEW files.
            m["localCopy"] = ".Plan Checklist/{}/{}".format(LOCAL_FOLDER.get(have[0], have[0]), have[1])
            m["uploadTo"] = "wp-content/uploads/guide-images/plan-checklist/{}/".format(have[0])
            m["fileName"] = have[1]
        groups.setdefault(m["page"], []).append(m)
    for page in groups:
        groups[page].sort(key=lambda m: m["name"].lower())
    rows_kept = [m for ms in groups.values() for m in ms]
    distinct = {m["formid"] for m in rows_kept}
    have_local = {m["formid"] for m in rows_kept if m.get("localCopy")}

    when = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    listing = idx.data.get("listing") or {}
    json_path = os.path.join(report_dir or DIST, "missing_images.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"generated": when, "listing": listing, "distinctItems": len(distinct),
                   "rows": len(rows_kept), "byPage": groups}, f, ensure_ascii=False, indent=1)

    L = ["# Missing images", "",
         "Every item on a reward page with no picture anywhere: not in the shared library, "
         "not in its own event folder, not on any other page. Built {} from the server listing of {}.".format(
             when[:10], listing.get("generated") or "(no listing yet)"), "",
         "**{} items have no picture on the server**: {} are already on your computer and just "
         "need uploading, {} need art found ({} rows - the same item can appear on several "
         "pages; one upload fixes every page it is on).".format(
             len(distinct), len(have_local), len(distinct) - len(have_local), len(rows_kept)), "",
         "Upload each file to the folder shown, named exactly as shown. Outfits: the folded "
         "item-only render is the `_go` file; a mannequin view, if you have one, is "
         "`<FormID>_c1.avif` beside it. Never upload a mannequin-only picture as the main image.", "",
         "Titles are not listed - they show the blank name tag until their art is uploaded "
         "(see `dist/titles/title_art_missing.json`). Weapon mod plans are not listed - "
         "they use the mod box.", ""]
    def cat_rank(page):
        cat = page.split("/", 1)[0]
        return (CATEGORY_ORDER.index(cat) if cat in CATEGORY_ORDER else 99, page_label(page).lower())
    # Part 1: files already on this computer that the server does not have.
    have = OrderedDict()
    for m in rows_kept:
        if m.get("localCopy"):
            have.setdefault(m["uploadTo"], set()).add(m["fileName"])
    if have:
        L.append("# Upload what you already have ({} files)".format(sum(len(v) for v in have.values())))
        L.append("")
        L.append("These pictures are in your local `.Plan Checklist` staging folder but are NOT on the "
                 "server, so the pages that use them have been showing a broken image. Upload them as "
                 "they are (same names) into the folder shown, then re-run the listing.")
        L.append("")
        for folder in sorted(have):
            L.append("**`{}`**".format(folder))
            L.append("")
            for fn in sorted(have[folder]):
                L.append("- `{}`".format(fn))
            L.append("")
        L.append("---")
        L.append("")
        L.append("# Pictures to find")
        L.append("")
    order = sorted(groups, key=cat_rank)
    hidden_done = False
    for page in order:
        rows = groups[page]
        if page.split("/", 1)[0] in NO_THUMBS and not hidden_done:
            hidden_done = True
            L.append("---")
            L.append("")
            L.append("# Pages that do not show reward pictures yet")
            L.append("")
            L.append("Activities, Public Events and Treasure Maps carry the picture in their data "
                     "(Unique Rewards items) but their pages do not draw reward thumbnails, so "
                     "these are lower priority. Many are generic loot (armour pieces, base weapons).")
            L.append("")
        L.append("## {} ({})".format(page_label(page), len(rows)))
        L.append("")
        L.append("| Item | Upload to | File name |")
        L.append("|---|---|---|")
        for m in rows:
            note = " *(cut)*" if m.get("cut") else ""
            if m.get("localCopy"):
                note += " *(already on your computer - see the top)*"
            L.append("| {}{} | `{}` | `{}` |".format(m["name"].replace("|", "/"), note,
                                                  m["uploadTo"], m["fileName"]))
        L.append("")
    path = os.path.join(report_dir or os.path.join(REPO, "audits"), "missing_images.md")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(L))
    log("report: {} distinct items, {} rows on {} pages -> {}, {}".format(
        len(distinct), len(rows_kept), len(groups), os.path.relpath(path, REPO), os.path.relpath(json_path, REPO)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", nargs="*", default=list(FILES), choices=list(FILES))
    ap.add_argument("--report", action="store_true", help="report only, change nothing")
    ap.add_argument("--index", default=ii.INDEX_PATH, help="image index to use")
    ap.add_argument("--report-dir", default=None,
                    help="where the two report files go (default: dist/ and audits/)")
    args = ap.parse_args()

    idx = ii.load(args.index)
    if not idx.have_listing:
        sys.exit("{} dist/image_index.json has no listing - run tools/list_server_images.py "
                 "then src/build_image_index.py first".format(TAG))
    os.chdir(REPO)
    log("index: {} FormIDs, listing of {}".format(len(idx.by_fid), (idx.data.get("listing") or {}).get("generated")))
    misses, totals = run_pages(args.pages, not args.report, idx)
    misses = plan_checklist_misses(idx) + misses
    write_report(misses, idx, totals, args.report_dir)


if __name__ == "__main__":
    main()
