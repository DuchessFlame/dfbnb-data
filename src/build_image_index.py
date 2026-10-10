#!/usr/bin/env python3
r"""
build_image_index.py - write dist/image_index.json, the one image map every builder reads.

INPUT
  data/server_listing.tsv   what is really on the server (tools/list_server_images.py)
  dist/plan_master.json     the plan checklists' resolved art
  dist/*.json               every page that already hosts art (plan_images.SOURCES:
                            scoreboard, CAMP pages, titles, Atom Shop, bundles ...)
  tsv/                      the newest game exports, for FormID / EditorID / name

OUTPUT
  dist/image_index.json     FormID -> wp-content path (+ carousel extras), the
                            placeholders, the title affixes, the weapon-mod set,
                            and the folder listing builders check URLs against
  audits/image_cleanup.md   files to tidy on the server: originals left beside
                            their AVIF, event-folder copies of library art,
                            Nuclear Winter art copied into event folders,
                            duplicate uploads, and library files nothing uses

HOW A FILE IS TIED TO A FormID (first hit per FormID wins)
  0. Legacy Nuclear Winter art (plan_master rows in legacy-nuclear-winter/)
  1. a library file NAMED after the FormID      0052A1F3.avif / 0052A1F3_go.avif
  2. the plan checklists' own resolved art      plan_master.json (plan + created item)
  3. art another page already hosts             plan_images.ImageIndex (FormID / EditorID)
  4. a library file named after the record      editor ID, editor ID + _l, the
                                                entitlement texture, the name slug
  5. an event-folder file named after the FormID
  6. an event-folder file named after the record (reuse across pages)

Every URL in the index is in the listing - nothing that 404s gets in.

USAGE
  python src/build_image_index.py
  python src/build_image_index.py --listing data/server_listing.tsv --dist dist
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
REPO = os.path.dirname(HERE)

import image_index as ii                                    # noqa: E402

TAG = "[build_image_index]"
csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

# Record exports with an item-like FormID / EditorID / name. Columns differ per
# export, so each names its own (FormID, EDID, FULL) headers, plus the sibling
# exports its glob would also catch (BOOK_Export_*.tsv matches *_Locations).
RECORD_EXPORTS = [
    ("ARMO", "ARMO_Export_*_ARMOUR.tsv", "ARMO_FormID", "ARMO_EDID", "ARMO_FULL", None),
    ("WEAP", "WEAP_Export_*_Base.tsv",   "WEAP_FormID", "WEAP_EDID", "WEAP_FULL", None),
    ("BOOK", "BOOK_Export_*.tsv",        "FormID", "EDID", "FULL", ["_Locations"]),
    ("OMOD", "OMOD_Export_*.tsv",        "OMOD_FormID", "OMOD_EDID", "FULL", ["_Properties"]),
    ("MISC", "MISC_Export_*.tsv",        "FormID", "EDID", "FULL", None),
    ("ALCH", "ALCH_Export_*.tsv",        "ALCH_FormID", "ALCH_EDID", "FULL", ["_Effects"]),
    ("FURN", "FURN_Export_*_FURN.tsv",   "FURN_FormID", "FURN_EDID", "FURN_FULL", None),
    ("ACTI", "ACTI_Export_*_ACTI.tsv",   "ACTI_FormID", "ACTI_EDID", "ACTI_FULL", None),
    ("NOTE", "NOTE_Export_*.tsv",        "NOTE_FormID", "NOTE_EDID", "FULL", None),
    ("KEYM", "KEYM_Export_*.tsv",        "FormID", "EDID", "FULL", ["_Locations"]),
    ("ENTM", "ENTM_Export_*.tsv",        "FormID", "EDID", "FULL", None),
]
_REF_RE = re.compile(r"\[([A-Z_]{4}):([0-9A-Fa-f]{8})\]")
_PLAN_PREFIX = re.compile(r"^\s*(plan|recipe|schematic)\s*:\s*", re.I)

LIBRARY_ORDER = ("guide-images/plan-checklist/", "season_images/", "guide-images/titles/",
                 "guide-images/atom-shop/", "guide-images/camp-items/")


def log(msg):
    print("{} {}".format(TAG, msg))


def newest(pattern, tsv_dir, exclude=None):
    """Newest export by the month in its name (tsv_source's rule)."""
    import tsv_source
    full = pattern if os.path.abspath(tsv_dir) == os.path.join(REPO, "tsv") else os.path.join(tsv_dir, pattern)
    return tsv_source.newest(full, exclude=exclude, required=False) or ""


def read_records(tsv_dir):
    """[(sig, fid, edid, full)] from every item export, plus ENTM textures."""
    recs, entm = [], []
    for sig, pattern, fc, ec, nc, exclude in RECORD_EXPORTS:
        path = newest(pattern, tsv_dir, exclude)
        if not path:
            log("  no {} export - skipped".format(pattern))
            continue
        n = 0
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            rd = csv.DictReader(f, delimiter="\t")
            cols = rd.fieldnames or []
            ec = ec if ec in cols else None
            nc = nc if nc in cols else None
            if fc not in cols:
                log("  {} has no FormID column - skipped".format(os.path.basename(path)))
                continue
            for r in rd:
                fid = (r.get(fc) or "").strip().upper()
                if not ii.FID_RE.match(fid):
                    continue
                edid = (r.get(ec) or "").strip() if ec else ""
                full = (r.get(nc) or "").strip() if nc else ""
                recs.append((sig, fid, edid, full))
                n += 1
                if sig == "ENTM":
                    tex = (r.get("ETDI") or "").strip().replace("\\", "/").rsplit("/", 1)[-1]
                    grants = [m.group(2).upper() for k, v in r.items()
                              if k and k.startswith("ECIL_") and v
                              for m in _REF_RE.finditer(v)]
                    entm.append((fid, tex, grants))
        log("  {:5d} {} records  ({})".format(n, sig, os.path.basename(path)))
    return recs, entm


class Builder:
    def __init__(self, rows):
        self.rows = rows                                      # [(rel, size)]
        self.size = {rel: size for rel, size in rows}
        self.files = defaultdict(list)                       # folder/ -> [names]
        self.ci = {}
        for rel, _ in rows:
            folder, _, name = rel.rpartition("/")
            self.files[folder + "/"].append(name)
            self.ci.setdefault(rel.lower(), rel)
        # stem -> [rel] per tier (library / event / nw), reward files only
        self.stems = {"library": defaultdict(list), "event": defaultdict(list),
                      "nw": defaultdict(list)}
        for rel, _ in rows:
            tier = ii.tier_of(rel)
            if tier == "other" or (tier == "event" and ii.is_event_own_art(rel)):
                continue
            stem = os.path.splitext(rel.rsplit("/", 1)[-1])[0].lower()
            self.stems[tier][stem].append(rel)
        self.by_fid = {}
        self.by_edid = {}
        self.used = set()                                    # rels some FormID points at
        self.ambiguous = {}                                  # stem -> [rels]
        self.via_counts = defaultdict(int)

    # ---- helpers ----------------------------------------------------------
    def find(self, url):
        rel = ii.norm_rel(url)
        if not rel:
            return ""
        return self.ci.get(rel.lower()) or self.ci.get(re.sub(r"\.webp$", ".avif", rel, flags=re.I).lower(), "")

    def extras_for(self, rel):
        """Carousel siblings in the same folder: stem_c1.., stem-2.., and for a
        texture tile (_l) or folded render (_go) the base's _cN frames."""
        folder, _, name = rel.rpartition("/")
        stem, ext = os.path.splitext(name)
        base = re.sub(r"_(l|go)$", "", stem, flags=re.I)
        out = []
        for cand in ["{}_c{}".format(base, n) for n in range(1, 10)] + \
                    ["{}-{}".format(stem, n) for n in range(2, 10)]:
            hit = self.ci.get("{}/{}{}".format(folder, cand, ext).lower()) or \
                self.ci.get("{}/{}.avif".format(folder, cand).lower())
            if hit and hit != rel and hit not in out:
                out.append(hit)
        return out[:3]

    def put(self, fid, rel, via, extras=None):
        fid = (fid or "").strip().upper()
        if not ii.FID_RE.match(fid) or fid in self.by_fid or not rel:
            return False
        ex = extras if extras is not None else self.extras_for(rel)
        e = {"url": ii.to_url(rel), "tier": ii.tier_of(rel), "via": via}
        if ex:
            e["extras"] = [ii.to_url(x) for x in ex if x != rel][:3]
        self.by_fid[fid] = e
        self.used.add(rel)
        for x in ex:
            self.used.add(x)
        self.via_counts[via] += 1
        return True

    def put_edid(self, edid, rel, via):
        k = (edid or "").strip().lower()
        if k and rel and k not in self.by_edid:
            e = {"url": ii.to_url(rel), "tier": ii.tier_of(rel), "via": via}
            ex = self.extras_for(rel)
            if ex:
                e["extras"] = [ii.to_url(x) for x in ex]
            self.by_edid[k] = e
            self.used.add(rel)

    def stem_hit(self, tier, stem):
        """The one file a stem names in a tier, or "" if none / ambiguous.

        Two copies of the same picture (same size) in different folders still
        identify it - the library-order folder wins and the copy is reported.
        Two DIFFERENT files sharing a name do not identify anything.
        """
        rels = self.stems[tier].get(stem)
        if not rels:
            return ""
        if len(rels) == 1:
            return rels[0]
        if len({self.size.get(r, 0) for r in rels}) == 1:
            return sorted(rels, key=_library_rank)[0]
        self.ambiguous[stem] = rels
        return ""


def _library_rank(rel):
    low = rel.lower()
    for i, p in enumerate(LIBRARY_ORDER):
        if low.startswith(p):
            return (i, low)
    return (len(LIBRARY_ORDER), low)


def name_stems(name):
    base = _PLAN_PREFIX.sub("", name or "").strip().lower()
    if len(re.sub(r"[^a-z0-9]", "", base)) < 4:
        return []
    out = [re.sub(r"[^a-z0-9]+", "-", base).strip("-"),
           re.sub(r"[^a-z0-9]+", "_", base).strip("_")]
    # Regional recipe variants share one file ("Healing Salve (Ash Heap)").
    b2 = re.sub(r"\s*\([^)]*\)\s*$", "", base)
    if b2 != base and len(b2) >= 4:
        out.append(re.sub(r"[^a-z0-9]+", "-", b2).strip("-"))
    return out


def edid_stems(edid):
    e = (edid or "").strip().lower()
    if not e:
        return []
    out = [e + "_l", e, e + "_go"]
    no_entm = re.sub(r"_?entm_?", "_", e).strip("_")
    if no_entm != e:
        out += [no_entm + "_l", no_entm]
    return out


def tex_stems(tex):
    t = (tex or "").strip().lower()
    if not t:
        return []
    t = re.sub(r"\.(dds|png|tga)$", "", t)
    base = re.sub(r"_l$", "", t)
    return [base + "_l", base, re.sub(r"^score_s0+(\d)", r"score_s\1", base)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listing", default=ii.LISTING_PATH)
    ap.add_argument("--dist", default=os.path.join(REPO, "dist"))
    ap.add_argument("--tsv", default=os.path.join(REPO, "tsv"))
    ap.add_argument("--out", default=ii.INDEX_PATH)
    ap.add_argument("--audit", default=os.path.join(REPO, "audits", "image_cleanup.md"))
    args = ap.parse_args()

    rows = ii.read_listing(args.listing)
    if rows is None:
        sys.exit("{} no listing at {} - run tools/list_server_images.py first".format(TAG, args.listing))
    if not rows:
        sys.exit("{} the listing has no image files - refusing to write an empty index".format(TAG))
    log("listing: {} image files".format(len(rows)))
    b = Builder(rows)

    os.chdir(REPO)                    # plan_images / tsv_source read tsv/, dist/, data/ relative

    # ---- plan_master ---------------------------------------------------------
    pm_items = []
    try:
        with open(os.path.join(args.dist, "plan_master.json"), encoding="utf-8") as f:
            pm_items = json.load(f).get("items") or []
    except (OSError, ValueError) as exc:
        log("WARNING: plan_master.json not read ({}) - plan art skipped".format(exc))

    try:
        import plan_images
    except Exception as exc:                                # noqa: BLE001
        plan_images = None
        log("WARNING: plan_images not importable ({})".format(exc))
    generic = set()
    if plan_images is not None:
        generic = {ii.norm_rel(u).lower() for u in (plan_images.GENERIC_ART or {}).values()}

    def pm_urls(it):
        out = []
        for s in it.get("images") or []:
            s = str(s or "")
            if not s:
                continue
            u = s if s.startswith("/") else ii.PLAN_IMG_BASE + (it.get("image_dir") or "") + "/" + s + ".avif"
            rel = b.find(u)
            if rel and rel.lower() not in generic:
                out.append(rel)
        return out

    def pm_fids(it):
        fids = [(it.get("plan_item") or {}).get("formid"), (it.get("cnam") or {}).get("formid")]
        rid = str(it.get("id") or "")
        if "_" in rid:
            fids.append(rid.rsplit("_", 1)[-1])
        return [f for f in fids if f]

    weapon_mod = set()
    nw_rows, other_rows = [], []
    for it in pm_items:
        if plan_images is not None:
            try:
                if plan_images.generic_kind(it) == "weapon-mod":
                    weapon_mod.update(x.upper() for x in pm_fids(it))
            except Exception:                               # noqa: BLE001
                pass
        (nw_rows if (it.get("image_dir") == "legacy-nuclear-winter" or it.get("legacy_nw"))
         else other_rows).append(it)

    # 0. Legacy Nuclear Winter
    for it in nw_rows:
        urls = pm_urls(it)
        if urls:
            for fid in pm_fids(it):
                b.put(fid, urls[0], "nuclear-winter", urls[1:])

    # 1. FormID-named library files
    # (the folded "_go" render beats a plain one; _cN files are carousel extras)
    for tier in ("nw", "library"):
        named = {}
        for stem, rels in b.stems[tier].items():
            m = ii.FID_FILE_RE.match(stem)
            if m and m.group("suffix") in (None, "_go"):
                rank = 0 if m.group("suffix") == "_go" else 1
                cand = (rank, sorted(rels, key=_library_rank)[0])
                fid = m.group("fid").upper()
                named[fid] = min(named.get(fid, cand), cand)
        for fid, (_rank, rel) in sorted(named.items()):
            b.put(fid, rel, "formid-file")

    # 2. plan checklist art
    for it in other_rows:
        urls = pm_urls(it)
        if urls:
            for fid in pm_fids(it):
                b.put(fid, urls[0], "plan-checklist", urls[1:])

    # 3. art another page already hosts
    if plan_images is not None:
        try:
            hidx, _staged = plan_images.load(dist_dir=args.dist, tsv_dir=args.tsv, verbose=False)
            for fid, (url, src) in hidx.by_fid.items():
                rel = b.find(asset_route(url))
                if rel:
                    b.put(fid, rel, "hosted:" + src)
            for key, (url, src) in hidx.by_edid.items():
                rel = b.find(asset_route(url))
                if rel:
                    b.put_edid(key, rel, "hosted:" + src)
        except Exception as exc:                            # noqa: BLE001
            log("WARNING: hosted index failed ({}) - page art skipped".format(exc))

    # titles: affix + type for the blank name-tag placeholder, and their art
    titles = {}
    for kind, fname in (("player", "titles_player.json"), ("camp", "titles_camp.json")):
        try:
            with open(os.path.join(args.dist, fname), encoding="utf-8") as f:
                items = json.load(f).get("items") or []
        except (OSError, ValueError):
            continue
        for t in items:
            aff = (t.get("affixType") or "").lower()
            affix = aff if aff in ("prefix", "suffix") else "both"
            info = {"type": kind, "affix": affix}
            if t.get("formId"):
                titles[str(t["formId"]).upper()] = info
            if t.get("edid"):
                titles[str(t["edid"]).lower()] = info
            for u in [t.get("imageUrl")] + list(t.get("images") or []):
                rel = b.find(u) if u else ""
                if rel and "blank" not in rel.lower():
                    b.put(t.get("formId"), rel, "titles")
                    b.put_edid(t.get("edid"), rel, "titles")
                    break

    # 4. library files named after the record. A weapon mod plan is matched by
    #    editor ID only: its display name is often just the weapon's name
    #    ("Sheepsquatch Staff"), and the weapon's picture on a mod plan is
    #    exactly what the mod box exists to avoid.
    recs, entm = read_records(args.tsv)
    rec_names = {fid: full for _sig, fid, _edid, full in recs if full}
    for sig, fid, edid, full in recs:
        if fid in b.by_fid:
            continue
        for kind, stems in (("edid", edid_stems(edid)),
                            ("name", [] if fid in weapon_mod else name_stems(full))):
            rel = next((r for r in (b.stem_hit("nw", st) or b.stem_hit("library", st)
                                    for st in stems) if r), "")
            if rel:
                b.put(fid, rel, kind + ":library")
                break
    # Entitlement textures. Bethesda reuses one placeholder texture across
    # several entitlements (five Fasnacht masks all point at the Frog mask's
    # DDS), so a texture only identifies a picture when every entitlement using
    # it has the same name.
    tex_names = defaultdict(set)
    ent_name = {fid: full for sig, fid, edid, full in recs if sig == "ENTM"}
    for fid, tex, grants in entm:
        if tex:
            tex_names[tex.lower()].add(re.sub(r"[^a-z0-9]", "", (ent_name.get(fid) or fid).lower()))
    shared_tex = {t for t, names in tex_names.items() if len(names) > 1}
    if shared_tex:
        log("  {} entitlement textures shared by differently-named items - not used to match".format(
            len(shared_tex)))
    for fid, tex, grants in entm:
        if tex.lower() in shared_tex:
            continue
        for stem in tex_stems(tex):
            rel = b.stem_hit("nw", stem) or b.stem_hit("library", stem)
            if rel:
                b.put(fid, rel, "texture:library")
                for g in grants:
                    b.put(g, rel, "texture:library")
                break

    link_aliases(b, [pm_fids(it) for it in pm_items], entm, weapon_mod, rec_names)

    # 5. FormID-named event files, 6. event files named after the record
    event_fids = defaultdict(set)                           # event rel -> fids it shows
    for stem, rels in b.stems["event"].items():
        m = ii.FID_FILE_RE.match(stem)
        if m and m.group("suffix") in (None, "_go"):
            for rel in rels:
                event_fids[rel].add(m.group("fid").upper())
            b.put(m.group("fid"), sorted(rels)[0], "formid-file:event")
    for sig, fid, edid, full in recs:
        for stem in edid_stems(edid) + name_stems(full):
            rels = b.stems["event"].get(stem) or []
            for rel in rels:
                event_fids[rel].add(fid)
            if rels and fid not in b.by_fid:
                b.put(fid, sorted(rels)[0], "name:event")
            if rels:
                break

    link_aliases(b, [pm_fids(it) for it in pm_items], entm, weapon_mod, rec_names)

    # ---- placeholders -------------------------------------------------------------
    def first_existing(names, under="guide-images/titles/"):
        for n in names:
            for rel in [under + n] + [r for r in b.ci.values()
                                      if r.lower().startswith(under) and r.rsplit("/", 1)[-1].lower() == n.lower()]:
                hit = b.ci.get(rel.lower())
                if hit:
                    return ii.to_url(hit)
        return ""

    ph = {"title": {}}
    for kind, label in (("player", "Player"), ("camp", "CAMP")):
        ph["title"][kind] = {}
        for affix, word in (("prefix", "Prefix"), ("suffix", "Suffix"), ("both", "Prefix-Suffix")):
            url = first_existing(["{} Title {} Blank.avif".format(label, word)])
            if url:
                ph["title"][kind][affix] = url
    mod_box = ""
    if plan_images is not None:
        u = (plan_images.GENERIC_ART or {}).get("weapon-mod", "")
        rel = b.find(u)
        mod_box = ii.to_url(rel) if rel else ""
    if mod_box:
        ph["weaponMod"] = mod_box
    else:
        log("WARNING: the weapon mod box image is not in the listing")

    # ---- write --------------------------------------------------------------------
    listing_when = ""
    with open(args.listing, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("# generated"):
                listing_when = line.split("\t", 1)[-1].strip()
                break
    out = {
        "version": 1,
        "generated": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "note": "Built by src/build_image_index.py from data/server_listing.tsv. "
                "Read it through src/image_index.py - never hand-edit.",
        "listing": {"file": "data/server_listing.tsv", "generated": listing_when, "images": len(rows)},
        "counts": {"formIds": len(b.by_fid), "edids": len(b.by_edid),
                   "via": dict(sorted(b.via_counts.items()))},
        "placeholders": ph,
        "weaponModFormIds": sorted(weapon_mod),
        "titles": titles,
        "byFormId": dict(sorted(b.by_fid.items())),
        "byEdid": dict(sorted(b.by_edid.items())),
        "files": {k: sorted(v) for k, v in sorted(b.files.items())},
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    tmp = args.out + ".part"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    with open(tmp, encoding="utf-8") as f:
        json.load(f)
    os.replace(tmp, args.out)
    log("wrote {}: {} FormIDs, {} EditorIDs ({})".format(
        os.path.relpath(args.out, REPO), len(b.by_fid), len(b.by_edid),
        ", ".join("{} {}".format(v, k) for k, v in sorted(b.via_counts.items()))))

    write_audit(args.audit, b, event_fids, listing_when)


def _words(name):
    return frozenset(re.findall(r"[a-z0-9]+", _PLAN_PREFIX.sub("", name or "").lower()))


def link_aliases(b, plan_groups, entm, weapon_mod=frozenset(), names=None):
    """One picture, every FormID that means the same thing.

    A plan and the item it builds, and an entitlement plus the items it grants,
    show the same picture, so a file found for one is given to the others. A
    file uploaded under the created item's FormID therefore also lights up the
    plan on every reward page.

    A plan is only linked to its created item when the two names have the same
    words ("Plan: Vault 63 Recon Outfit Burnt" / "Burnt Vault 63 Recon Outfit").
    Some mod recipes name the WEAPON as their created item ("Plan: Burning
    Sheepsquatch Staff" creates SheepsquatchStaff), and that plan must not get
    the plain weapon's picture.
    """
    names = names or {}
    groups = []
    for g in plan_groups:
        g = [x.upper() for x in g if x]
        if len(set(g)) > 1:
            wsets = {_words(names.get(x)) for x in set(g) if names.get(x)}
            if len(wsets) != 1:
                continue
        groups.append(g)
    groups += [[fid] + list(grants) for fid, _t, grants in entm]
    for g in groups:
        g = [x.upper() for x in g if x]
        hit = next((b.by_fid[x] for x in g if x in b.by_fid), None)
        if not hit:
            continue
        for x in g:
            # never hand a weapon's name-matched picture to a weapon mod
            if x in weapon_mod and hit["via"].startswith("name:library"):
                continue
            if x not in b.by_fid and ii.FID_RE.match(x):
                e = dict(hit)
                e["via"] = "alias:" + hit["via"].split(":")[-1]
                b.by_fid[x] = e
                b.via_counts["alias"] += 1


def asset_route(url):
    try:
        import asset_paths
        return asset_paths.asset_url(url)
    except Exception:                                        # noqa: BLE001
        return url


# ── the cleanup audit ──────────────────────────────────────────────────────

def write_audit(path, b, event_fids, listing_when):
    # a. an original left beside its AVIF
    originals = []
    for folder, names in b.files.items():
        stems = {os.path.splitext(n)[0].lower() for n in names if n.lower().endswith(".avif")}
        for n in names:
            st, ext = os.path.splitext(n)
            if ext.lower() != ".avif" and st.lower() in stems:
                originals.append(folder + n)
    # b/c. event-folder reward files that are copies of library / NW art
    nw_copies, lib_copies = [], []
    for rel, fids in sorted(event_fids.items()):
        tiers = {b.by_fid[f]["tier"] for f in fids if f in b.by_fid}
        lib = [b.by_fid[f]["url"] for f in fids if f in b.by_fid and b.by_fid[f]["tier"] in ("nw", "library")]
        if "nw" in tiers:
            nw_copies.append((rel, lib[0]))
        elif "library" in tiers:
            lib_copies.append((rel, lib[0]))
    # d. same picture uploaded to two library folders
    dupes = []
    for tier in ("library", "nw"):
        for stem, rels in b.stems[tier].items():
            if len(rels) > 1 and len({b.size.get(r, 0) for r in rels}) == 1:
                dupes.append(sorted(rels, key=_library_rank))
    # e. library files nothing points at
    orphans = sorted(rel for tier in ("library", "nw") for rels in b.stems[tier].values()
                     for rel in rels if rel not in b.used)

    L = ["# Image library cleanup", "",
         "Written by `src/build_image_index.py` from the server listing of {}. "
         "Nothing here is deleted automatically; tick these off in FileZilla.".format(listing_when or "(unknown date)"),
         "", "Guide walkthrough images, maps, gallery shots, reward checklists and covers are never listed.", ""]

    def section(title, why, items, fmt):
        L.append("## {} ({})".format(title, len(items)))
        L.append("")
        L.append(why)
        L.append("")
        for x in items:
            L.append(fmt(x))
        L.append("")

    section("Originals left beside their AVIF", "AVIF is the only copy. Delete these.",
            sorted(originals), lambda r: "- `{}`".format(r))
    section("Nuclear Winter art copied into an event folder",
            "Babylon / Nuclear Winter art lives in the legacy Nuclear Winter folder only. "
            "Delete the event copy (left); pages already use the legacy file (right).",
            nw_copies, lambda x: "- `{}` -> `{}`".format(x[0], x[1].replace(ii.UPLOADS, "")))
    section("Event-folder copies of shared-library art",
            "The shared library already has this item, and every page now uses the library copy. "
            "Check it is the same picture, then delete the event copy (left).",
            lib_copies, lambda x: "- `{}` -> `{}`".format(x[0], x[1].replace(ii.UPLOADS, "")))
    section("Same picture uploaded to two library folders",
            "Same name and size. Keep the first, delete the rest.",
            dupes, lambda rs: "- " + " | ".join("`{}`".format(r) for r in rs))
    section("Different pictures sharing one name",
            "These names could not be tied to an item because two different files use them. "
            "Rename them to the item's FormID.",
            sorted(b.ambiguous.items()), lambda kv: "- `{}`: ".format(kv[0]) + " | ".join("`{}`".format(r) for r in kv[1]))
    section("Library files no item points at",
            "Not matched to any FormID, editor ID, texture or name. Either the item was renamed "
            "(rename the file to its FormID) or the file is no longer used.",
            orphans, lambda r: "- `{}`".format(r))

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(L))
    log("wrote {}: {} originals, {} NW copies, {} event copies, {} duplicate sets, "
        "{} ambiguous names, {} unused library files".format(
            os.path.relpath(path, REPO), len(originals), len(nw_copies), len(lib_copies),
            len(dupes), len(b.ambiguous), len(orphans)))


if __name__ == "__main__":
    main()
