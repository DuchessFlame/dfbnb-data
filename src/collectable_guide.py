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
    hits = [p for p in Path(tsv_dir).glob(pattern) if not any(p.name.endswith(s) for s in skip)]
    return str(max(hits, key=lambda p: p.stat().st_mtime)) if hits else None


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
    """{caps, xp, items:[EDID...], lists:[EDID...]} for a quest reward (GMRW)."""
    out = {"caps": None, "xp": None, "items": [], "lists": []}
    p = newest(tsv_dir, "GMRW_Export_*.tsv", skip=("_Refs.tsv",))
    for r in rows(p):
        if (r.get("EDID") or "") != gmrw_edid:
            continue
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


# ── html pieces ─────────────────────────────────────────────────────────────
def batch_table(batches, receive, col_first="Hand-in", col_count="Count"):
    """Hand-in | <count> | Running total, with a darker Receive row under each
    line (Duchess, 22 Nov 2026). receive[i] = what batch i+1 gives."""
    E = _h.escape
    body, run = [], 0
    for i, n in enumerate(batches, 1):
        run += n
        body.append(f"<tr class=\"abg-hand\"><td>{i}</td><td>{n}</td><td>{run}</td></tr>")
        if i - 1 < len(receive) and receive[i - 1]:
            body.append(f"<tr class=\"abg-receive\"><td>Receive</td><td colspan=\"2\">{E(receive[i - 1])}</td></tr>")
    return ("<table class=\"abg-table abg-table--handin\"><thead><tr>"
            f"<th>{E(col_first)}</th><th>{E(col_count)}</th><th>Running total</th></tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>") if body else ""


def finding_sentence(total, by_region, plural, pages_label, page_count, everywhere_at=4):
    """The first line of Finding Them - the same words on every guide (Duchess,
    23 Nov 2026); only the numbers and names change, all from the spawn data:
      There are <b>N</b> {plural} in <b>Region</b>. / : <b>a</b> in <b>R1</b> and <b>b</b> in <b>R2</b>. /
      all over Appalachia (4+ regions).
      Each one is a fixed spawn, so it's always in the same place.
      The {pages_label} page(s) has/have every one."""
    E = _h.escape
    regs = sorted(((r, n) for r, n in (by_region or {}).items() if n), key=lambda kv: -kv[1])
    if len(regs) >= everywhere_at:
        where = " all over Appalachia"
    elif len(regs) == 1:
        where = f" in <strong>{E(regs[0][0])}</strong>"
    elif regs:
        parts = [f"<strong>{n:,}</strong> in <strong>{E(r)}</strong>" for r, n in regs]
        where = ": " + (", ".join(parts[:-1]) + " and " + parts[-1])
    else:
        where = ""
    pages = f"The {E(pages_label)} page{'s have' if page_count != 1 else ' has'} every one."
    return (f"There are <strong>{int(total):,}</strong> {E(plural)}{where}. "
            f"Each one is a fixed spawn, so it's always in the same place. {pages}")


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
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    return doc
