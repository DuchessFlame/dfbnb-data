#!/usr/bin/env python3
r"""
collectable_guide.py - shared pieces for the collectable quest guides
(Duchess, 22 Nov 2026: one template for every collectable guide).

  /df/collectables/brain-in-a-jar/guide/             build_brain_in_a_jar_json.py
  /df/collectables/abraxodyne-intel-briefcases/guide/ build_abraxodyne_guide_json.py
  /df/collectables/pint-sized-slasher-masks/guide/   build_slasher_masks_guide_json.py

All three write the same JSON shape, mounted by df-bnb-collectable-guide.js:
  {title, subtitle, pills:[{label,url}], image_dims,
   sections:[{title, html, item_image, images:[url...], closed, spoiler}],
   missing_images:[{section, name, url}]}
Section order (template): Finding Them / How to Start {quest} / {quest} Walkthrough /
[Handing In or batch table] / [Quest Rewards] / FAQ and Important Info /
The Finale (closed, spoilers) / Lore (closed, spoilers). Maps + Gallery come
from the page shell.

Pictures: a section lists picture names; each is turned into a full URL and kept
ONLY when it is on data/server_listing.tsv, so the page never shows a broken
picture. Every picture not on the server yet goes into missing_images, which is
the "photos still needed" list.

Numbers: everything countable comes from the game files (helpers below).
"""
from __future__ import annotations

import csv
import html as _h
import json
import os
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LISTING = REPO / "data" / "server_listing.tsv"
csv.field_size_limit(1 << 30)

_LIST = None


def on_server(url):
    global _LIST
    if _LIST is None:
        _LIST = set()
        if LISTING.exists():
            with open(LISTING, encoding="utf-8") as fh:
                _LIST = {ln.split("\t", 1)[0].strip() for ln in fh if not ln.startswith("#")}
    return url.replace("/wp-content/uploads/", "", 1) in _LIST


def rows(path):
    if not path:
        return
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as fh:
        yield from csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)


def newest(tsv_dir, pattern, skip=()):
    """The newest export by the date in its file name (tsv_source rules), never
    by file time: a GitHub checkout gives every file the same time, so mtime
    picked an old _ScriptProps there and "{cache_batches}" came out "some"
    (28 Nov 2026)."""
    import sys
    if str(Path(__file__).resolve().parent) not in sys.path:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
    import tsv_source
    hits = tsv_source.all_matching(os.path.join(str(tsv_dir), pattern))
    hits = [h for h in hits if not any(os.path.basename(h).endswith(s) for s in skip)]
    return hits[-1] if hits else None


# ── game-file facts ─────────────────────────────────────────────────────────
def quest_batches(tsv_dir, quest_edid):
    """(quest name, [CollectablesNeededPerBatch...]) from QUST2 _ScriptProps."""
    name, out = "", []
    for r in rows(newest(tsv_dir, "QUST2_Export_*_ScriptProps.tsv")):
        if (r.get("QuestEDID") or "") != quest_edid:
            continue
        name = name or (r.get("QuestName") or "").strip()
        if (r.get("Property") or "") == "CollectablesNeededPerBatch":
            try:
                out.append(int(float(r.get("Value") or 0)))
            except ValueError:
                pass
    return name, out


def numbered_names(tsv_dir, pattern, edid_col, prefix, skip=()):
    """FULL names of records whose EditorID is prefix+N, in N order."""
    got = []
    for r in rows(newest(tsv_dir, pattern, skip)):
        ed = (r.get(edid_col) or "").strip()
        if not ed.startswith(prefix):
            continue
        m = re.search(r"(\d+)\s*$", ed)
        if m:
            got.append((int(m.group(1)), (r.get("FULL") or ed).strip()))
    return [n for _, n in sorted(set(got))]


def glob_value(tsv_dir, edid):
    for r in rows(newest(tsv_dir, "GLOB_Export_*.tsv")):
        v = list(r.values())
        if len(v) > 2 and v[1] == edid:
            try:
                return int(float(v[2]))
            except ValueError:
                return None
    return None


def gmrw_reward(tsv_dir, gmrw_edid):
    """{caps, xp, xp_curve, items:[EDID...], lists:[EDID...]} for a quest reward (GMRW)."""
    out = {"caps": None, "xp": None, "xp_curve": None, "items": [], "lists": []}
    p = newest(tsv_dir, "GMRW_Export_*.tsv", skip=("_Refs.tsv",))
    for r in rows(p):
        if (r.get("EDID") or "") != gmrw_edid:
            continue
        c = (r.get("XPCT_XPCurveTable") or "").split(":")
        if len(c) >= 2 and out["xp_curve"] is None:
            out["xp_curve"] = c[1]   # XP that grows with level (curve_base_xp)
        for key, col in (("xp", "NAM7_XPGlobal"), ("caps", "NAM8_CapsGlobal")):
            g = (r.get(col) or "").split(":")
            if len(g) >= 2 and out[key] is None:
                out[key] = glob_value(tsv_dir, g[1])
        it = (r.get("RewardedItem") or "").split(":")
        if len(it) >= 3:
            (out["lists"] if it[-1] == "LVLI" else out["items"]).append(it[1])
    return out


def record_name(tsv_dir, edid, patterns=("BOOK_Export_*.tsv", "MISC_Export_*.tsv", "NOTE_Export_*.tsv")):
    for pat in patterns:
        for r in rows(newest(tsv_dir, pat, skip=("_Locations.tsv",))):
            for k in ("EDID", "BOOK_EDID", "MISC_EDID", "NOTE_EDID"):
                if (r.get(k) or "") == edid and (r.get("FULL") or "").strip():
                    return r["FULL"].strip()
    return ""


def _num(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _prop_value(r):
    """A script-property row's number: the value itself, or a GLOB's value."""
    t = r.get("Type") or ""
    return _num(r.get("GlobValue")) if t in ("obj", "structobj") else _num(r.get("Value"))


def hand_in_rewards(tsv_dir, quest_edid, list_prop="RewardsList"):
    """Caps and XP each hand-in pays, in order: the quest script's RewardsList[i]
    .CapsReward / .XPReward (a number or a GLOB; QUST2 _ScriptProps struct rows,
    28 Nov 2026) -> [{"caps": 20, "xp": 250}, ...]. Hand-ins that pay nothing
    (0 / AAAIgnoreMe) are kept as 0 so the caller can skip the table."""
    got = {}
    for r in rows(newest(tsv_dir, "QUST2_Export_*_ScriptProps.tsv")):
        if (r.get("QuestEDID") or "") != quest_edid:
            continue
        m = re.fullmatch(re.escape(list_prop) + r"\[(\d+)\]\.(\w+)", r.get("Property") or "")
        if not m:
            continue
        key = {"capsreward": "caps", "xpreward": "xp"}.get(m.group(2).lower())
        if key:
            got.setdefault(int(m.group(1)), {"caps": 0, "xp": 0})[key] = _prop_value(r) or 0
    return [got[i] for i in sorted(got)]


def misc_script_value(tsv_dir, misc_edid, prop):
    """A number on a MISC's script (MISCSP export, 28 Nov 2026), e.g. the XP a
    picked-up collectable gives: AmountOfXPToGive."""
    for r in rows(newest(tsv_dir, "MISCSP_Export_*.tsv")):
        if (r.get("EDID") or "") == misc_edid and (r.get("Property") or "") == prop:
            return _prop_value(r)
    return None


def acti_script_value(tsv_dir, acti_edid, prop):
    """A number on an ACTI's script (ACTI export, last column: Script::Prop=value##...)."""
    for r in rows(newest(tsv_dir, "ACTI_Export_*_ACTI.tsv")):
        if (r.get("ACTI_EDID") or "") != acti_edid:
            continue
        for v in r.values():
            if isinstance(v, str) and "::" + prop + "=" in v:
                m = re.search(r"::" + re.escape(prop) + r"=([-\d.]+)", v)
                if m:
                    return _num(m.group(1))
    return None


def curve_base_xp(tsv_dir, curve_edid):
    """The level-1 value of an XP curve (CURV _POINTS): the base XP a quest gives
    when it pays on a level scale (Duchess, 28 Nov 2026: say "base XP")."""
    pts = []
    for r in rows(newest(tsv_dir, "CURV_Export_*_POINTS.tsv")):
        if (r.get("EDID") or "") == curve_edid:
            x, y = _num(r.get("X")), _num(r.get("Y"))
            if x is not None and y is not None:
                pts.append((x, y))
    return min(pts)[1] if pts else None


def challenge_steps(tsv_dir, chal_prefix):
    """Counts the lifetime collect challenges ask for (CHAL TNAM), sorted. The
    prefix leaves out cut tiers (zzz...)."""
    out = set()
    for r in rows(newest(tsv_dir, "CHAL_Export_*.tsv")):
        if (r.get("EDID") or "").startswith(chal_prefix):
            n = _num(r.get("TNAM"))
            if n and 0 < n < 9999999:
                out.add(n)
    return sorted(out)


# ── html pieces ─────────────────────────────────────────────────────────────
def struct_batches(tsv_dir, quest_edid, prop, member="BatchNumber"):
    """Hand-in numbers held in a struct-array script property, e.g. Dirty
    Laundry's CacheStageAndBatchNumber[i].BatchNumber (QUST2 _ScriptProps rows
    'Prop[i].Member', 28 Nov 2026) -> sorted ints [7, 11, 14]."""
    out = []
    for r in rows(newest(tsv_dir, "QUST2_Export_*_ScriptProps.tsv")):
        if (r.get("QuestEDID") or "") != quest_edid:
            continue
        pn = r.get("Property") or ""
        if pn.startswith(prop + "[") and pn.endswith("]." + member):
            try:
                out.append(int(float(r.get("Value") or 0)))
            except ValueError:
                pass
    return sorted(set(out))


def and_list(items):
    """['7', '11', '14'] -> '7, 11 and 14'."""
    items = [str(x) for x in items]
    return items[0] if len(items) == 1 else (", ".join(items[:-1]) + " and " + items[-1]) if items else ""


def batch_table(batches, receive, col_first="Hand-in", col_count="Count", extra=None):
    """Hand-in | <count> | Running total, with a darker Receive row under each
    line (Duchess, 22 Nov 2026). receive[i] = what batch i+1 gives.
    extra = {hand-in number: [(label, text), ...]} adds more darker rows under
    that hand-in (Dirty Laundry: the Abraxodyne Cache unlocks, 28 Nov 2026)."""
    E = _h.escape
    body, run = [], 0
    for i, n in enumerate(batches, 1):
        run += n
        body.append(f"<tr class=\"abg-hand\"><td>{i}</td><td>{n}</td><td>{run}</td></tr>")
        if i - 1 < len(receive) and receive[i - 1]:
            body.append(f"<tr class=\"abg-receive\"><td>Receive</td><td colspan=\"2\">{E(receive[i - 1])}</td></tr>")
        for label, text in (extra or {}).get(i, []):
            body.append(f"<tr class=\"abg-receive abg-cache\"><td>{E(label)}</td><td colspan=\"2\">{E(text)}</td></tr>")
    return ("<table class=\"abg-table abg-table--handin\"><thead><tr>"
            f"<th>{E(col_first)}</th><th>{E(col_count)}</th><th>Running total</th></tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>") if body else ""


def rewards_table(rewards, col_first="Hand-in"):
    """Hand-in | Caps | XP for each hand-in (Quest Rewards, Duchess 28 Nov 2026).
    Empty when no hand-in pays caps or XP."""
    if not any(r["caps"] or r["xp"] for r in rewards):
        return ""
    body = "".join(f"<tr><td>{i}</td><td>{r['caps']:,}</td><td>{r['xp']:,}</td></tr>"
                   for i, r in enumerate(rewards, 1))
    return ("<table class=\"abg-table abg-table--handin abg-table--rewards\"><thead><tr>"
            f"<th>{_h.escape(col_first)}</th><th>Caps</th><th>XP</th></tr></thead>"
            f"<tbody>{body}</tbody></table>")


def finding_sentence(total, by_region, plural, pages_label, page_count, everywhere_at=4):
    """The first line of Finding Them - the same words on every guide (Duchess,
    23 Nov 2026); only the numbers and names change, all from the spawn data:
      There are <b>N</b> {plural} in Region. / : <b>a</b> in R1 and <b>b</b> in R2. /
      (region names not bold - Duchess, 27 Nov 2026)
      all over Appalachia (4+ regions).
      Each one is a fixed spawn, so it's always in the same place."""
    E = _h.escape
    regs = sorted(((r, n) for r, n in (by_region or {}).items() if n), key=lambda kv: -kv[1])
    if len(regs) >= everywhere_at:
        where = " all over Appalachia"
    elif len(regs) == 1:
        where = f" in {E(regs[0][0])}"
    elif regs:
        parts = [f"<strong>{n:,}</strong> in {E(r)}" for r, n in regs]
        where = ": " + (", ".join(parts[:-1]) + " and " + parts[-1])
    else:
        where = ""
    # No "The ... Locations pages have every one." line (Duchess, 23 Nov 2026).
    return (f"There are <strong>{int(total):,}</strong> {E(plural)}{where}. "
            f"Each one is a fixed spawn, so it's always in the same place.")


# ── build ───────────────────────────────────────────────────────────────────
def build(src_path, out_path, fill, image_base):
    """Read the hand-written guide (src_path), fill {placeholders} with fill(),
    turn picture names into server URLs (only those on the server), write out_path.
    A picture name with a '/' in it is already a full /wp-content/... URL."""
    with open(src_path, encoding="utf-8") as fh:
        g = json.load(fh)
    dims, missing, secs = {}, [], []
    for s in g["sections"]:
        title = fill(s.get("title", ""))
        imgs = []
        for n in s.get("images") or []:
            url = n if "/" in n else f"{image_base}{n}.avif"
            if on_server(url):
                imgs.append(url)
                d = (g.get("image_dims") or {}).get(n)
                if d:
                    dims[url] = d
            else:
                missing.append({"section": title, "name": n, "url": url})
        # item_image: ONE photo of the item where it spawns, shown small beside
        # the text at the top of the section (Duchess, 23 Nov 2026: Finding Them).
        item = s.get("item_image")
        item_url = ""
        if item:
            url = item if "/" in item else f"{image_base}{item}.avif"
            if on_server(url):
                item_url = url
                d = (g.get("image_dims") or {}).get(item)
                if d:
                    dims[url] = d
            else:
                missing.append({"section": title, "name": item, "url": url})
        html = fill(s.get("html", ""))
        # inline {img:name,name}: keep only pictures on the server
        def inline(m):
            keep = []
            for n in m.group(1).split(","):
                n = n.strip()
                url = n if "/" in n else f"{image_base}{n}.avif"
                if url in imgs:
                    keep.append(url)
            return "{img:" + ",".join(keep) + "}" if keep else ""
        html = re.sub(r"\{img:([^}]*)\}", inline, html)
        out = {"title": title, "html": html}
        if item_url:
            out["item_image"] = item_url
        if imgs:
            out["images"] = imgs
        for k in ("closed", "spoiler"):
            if s.get(k):
                out[k] = True
        secs.append(out)
    doc = {"_meta": {"built_from": str(Path(src_path).relative_to(REPO)).replace("\\", "/"),
                     "note": "Built file - edit the source above, not this."},
           "title": fill(g["title"]), "subtitle": fill(g.get("subtitle", "")),
           "pills": [{"label": fill(p["label"]), "url": p["url"]} for p in g.get("pills", [])],
           "sections": secs, "image_dims": dims, "missing_images": missing}
    if g.get("footer"):
        doc["footer"] = fill(g["footer"])
    # Maps root expand + Gallery (28 Nov 2026, Captain Bee's Diary first):
    #   "maps":    [{"name": file stem, "title", "desc", "download"}]  (JPEG, guide-maps-expand)
    #   "gallery": [{"name": file stem or URL, "caption"}]            (AVIF)
    # Only files on the server get through; the rest go to missing_images.
    def _pass(items, ext, section):
        keep = []
        for it in items or []:
            n = it["name"]
            url = n if "/" in n else f"{image_base}{n}{ext}"
            if on_server(url):
                out = {k: fill(v) if isinstance(v, str) else v for k, v in it.items() if k != "name"}
                out["url"] = url
                d = (g.get("image_dims") or {}).get(n)
                if d:
                    dims[url] = d
                keep.append(out)
            else:
                missing.append({"section": section, "name": n, "url": url})
        return keep
    if g.get("maps"):
        doc["maps"] = _pass(g["maps"], ".jpg", "Maps")
    if g.get("gallery"):
        doc["gallery"] = _pass(g["gallery"], ".avif", "Gallery")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    return doc
