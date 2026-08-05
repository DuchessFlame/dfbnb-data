#!/usr/bin/env python3
r"""
build_vendors_json.py — build the reusable vendor master, dist/vendors.json.

A generative master of EVERY merchant NPC in the game: who they are, where they
stand, and what leveled stock they sell.  Item pages join their own vendor LVLIs
against each vendor's `sells` closure to show REAL vendor names (not just a
marker + type heuristic).  See the spawn-guide skill §9f.

INPUTS (all committed to tsv/, newest-glob; tsv/pts/ under --pts)
  * NPC2_Vendors_*.tsv            one row per vendor NPC (identity, merchant
       faction, buy/sell list, MerchantContainerRef + base CONT).  Produced by
       the xEdit script !!!Wordpress - ExportNPC2VendorsToTSV.pas.
  * LVLI_Export_*_LVLI_Entries.tsv  parent→child leveled-list tree.
  * LVLI_Export_*_LVLI_Refs.tsv     per-LVLI ReferencedBy (finds the CONT that
       holds each stock list — this is the reverse container→LVLI link, since
       the CONT export carries no contents column).

LOCATION (no dependence on the xEdit placement coords — those are cell-local for
interiors and blank-celled in this build).  Instead we look the placed
MerchantContainerRef up in Mappalachia's Position table and resolve it with the
same shared Geo resolver the farming/nuka spawn builders use:
    Position(instanceFormID = container ref) → (space, x, y)
    Geo.resolve(space, x, y)                 → (region, marker)
Interiors resolve by space name (Whitespring, Storm→Skyline, Burn→Burning
Springs, …); Atlantic City / The Pitt resolve to their instanced region; genuinely
unmapped instanced vendors fall through to region/marker = "" automatically.
Coordinates are cached to data/vendors/geo_cache.json so CI rebuilds DB-free.

SELLS.  A vendor's for-sale stock is the leveled lists held by its merchant
container.  We find each container's ROOT stock lists by reverse-scanning
LVLI_Refs (an LVLI whose ReferencedBy contains the container base CONT), then
walk DOWN the LVLI tree to the full descendant closure.  `sells_formids` (and the
friendly `sells` EDIDs) are that closure — an item page matches when ANY LVLI in
its own upward closure is in this set.

OUTPUT  dist/vendors.json (or dist/pts/vendors.json under --pts):
  {
    "_meta": { generated, source, counts… },
    "vendors": [
      { name, edid, form_id, faction, buysell, container_ref, container_base,
        marker, region, coords, resolved_by, sells:[EDID…], sells_formids:[FID…] }
    ]
  }

Env:
    MAPPALACHIA_DB      default D:\Mappalachia\data\mappalachia.db
    VENDORS_GEO_CACHE   default data/vendors/geo_cache.json

Usage:
    python src/build_vendors_json.py
    python src/build_vendors_json.py --pts
"""

import os, sys, json, csv, glob, sqlite3, datetime, argparse, re
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from nuka_cola_spawns_geo import Geo
import farming_spawns_sources as sources
from patchlog_utils import write_empty_patchlog_feed

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
SQL_CHUNK = 900

# NPC2 main-file column indices (0-based), locked to the .pas Header_Main order.
C_NPC_FID, C_NPC_EDID, C_NPC_FULL, C_NPC_SHRT = 0, 1, 2, 3
C_FAC_EDID = 7
C_BUYSELL_EDID = 10
C_CONTREF_FID = 12          # MerchantContainerRef_FormID (the placed stall REFR)
C_CONTBASE_FID = 13         # MerchantContainerBase_FormID (the CONT)
C_CONTBASE_EDID = 14        # MerchantContainerBase_EDID


# ── file helpers ──────────────────────────────────────────────────────────────

def _newest(pattern, tsv_root, exclude_suffix=None):
    hits = sorted(glob.glob(os.path.join(tsv_root, pattern)), key=os.path.getmtime, reverse=True)
    if exclude_suffix:
        hits = [h for h in hits if not h.endswith(exclude_suffix)]
    return hits[0] if hits else None


def _chunks(seq, n=SQL_CHUNK):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ── NPC2 vendor parse ─────────────────────────────────────────────────────────

def load_vendors(tsv_root):
    """Parse the NPC2 vendor export → list of raw vendor dicts (real vendors only:
    those carrying a merchant container base)."""
    path = _newest("NPC2_Vendors_*.tsv", tsv_root, exclude_suffix="_Placements.tsv")
    if not path:
        raise FileNotFoundError(f"no NPC2_Vendors_*.tsv in {tsv_root}")
    out = []
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        rd = csv.reader(f, delimiter="\t")
        next(rd, None)  # header
        for row in rd:
            if len(row) <= C_CONTBASE_EDID:
                continue
            cont_base_edid = row[C_CONTBASE_EDID].strip()
            if not cont_base_edid:
                continue  # not a real vendor (no merchant container)
            out.append({
                "form_id": row[C_NPC_FID].strip().upper(),
                "edid": row[C_NPC_EDID].strip(),
                "full": row[C_NPC_FULL].strip(),
                "faction": row[C_FAC_EDID].strip(),
                "buysell": row[C_BUYSELL_EDID].strip(),
                "container_ref": row[C_CONTREF_FID].strip().upper(),
                "container_base": row[C_CONTBASE_FID].strip().upper(),
                "container_base_edid": cont_base_edid,
            })
    return os.path.basename(path), out


# ── SELLS: reverse container→root LVLIs, then downward closure ─────────────────

def _invert_c2p(c2p):
    """child→{parents}  ⟶  parent→{children}."""
    p2c = defaultdict(set)
    for child, parents in c2p.items():
        for p in parents:
            p2c[p].add(child)
    return p2c


def _container_roots(lvli_refs):
    """container CONT FormID → {root LVLI FormID it holds}, from LVLI ReferencedBy."""
    roots = defaultdict(set)
    for lv, refs in lvli_refs.items():
        for rf, _edid, sig in refs:
            if sig == "CONT":
                roots[rf].add(lv)
    return roots


def _down_closure(seeds, p2c):
    """All LVLI FormIDs reachable DOWN from the seed lists (seeds included)."""
    seen, stack = set(), list(seeds)
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        for c in p2c.get(n, ()):
            if c not in seen:
                stack.append(c)
    return seen


def build_sells(vendors, tables):
    """Attach sells_formids / sells (EDIDs) to each vendor from its container."""
    p2c = _invert_c2p(tables["c2p"])
    parent_edid = tables["parent_edid"]
    cont_roots = _container_roots(tables["lvli_refs"])
    # Keep only LVLI children in the downward walk (the join is over LVLIs).
    lvli_ids = set(tables["c2p"].keys()) | set(parent_edid.keys())
    for p, kids in list(p2c.items()):
        p2c[p] = {k for k in kids if k in lvli_ids}

    resolved = 0
    for v in vendors:
        roots = cont_roots.get(v["container_base"], set())
        closure = _down_closure(roots, p2c) if roots else set()
        v["sells_formids"] = sorted(closure)
        v["sells"] = sorted({parent_edid[f] for f in closure if parent_edid.get(f)})
        if closure:
            resolved += 1
    return resolved


# ── LOCATION: Mappalachia Position + Geo ──────────────────────────────────────

def pull_positions(cur, inst_ints):
    """instanceFormID(int) → (x, y, spaceFormID) from Mappalachia Position."""
    out = {}
    for chunk in _chunks(inst_ints):
        q = ("SELECT instanceFormID, x, y, spaceFormID FROM Position "
             "WHERE instanceFormID IN (%s)" % ",".join("?" * len(chunk)))
        for inst, x, y, space in cur.execute(q, tuple(chunk)).fetchall():
            out.setdefault(inst, (x, y, space))
    return out


def load_cache(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump(cache, open(path, "w", encoding="utf-8"), ensure_ascii=False)


def _to_int(fid):
    try:
        return int(fid, 16)
    except (TypeError, ValueError):
        return None


def resolve_locations(vendors, geo, cur, cache, db_ok):
    """Populate marker/region/coords/resolved_by on each vendor.

    Location key preference: the placed MerchantContainerRef (the stall) first,
    then the NPC form id as a fallback.  DB results refresh the cache; without a
    DB we read straight from the committed cache (keyed by instanceFormID str)."""
    resolved = 0
    if db_ok:
        want = set()
        for v in vendors:
            for fid in (v["container_ref"], v["form_id"]):
                i = _to_int(fid)
                if i is not None:
                    want.add(i)
        pos = pull_positions(cur, want)
        for inst, (x, y, space) in pos.items():
            region, marker, how = geo.resolve(space, x, y)
            cache[str(inst)] = {"space": space, "x": round(x, 1), "y": round(y, 1),
                                "region": region, "marker": marker, "how": how}

    for v in vendors:
        entry = None
        for fid in (v["container_ref"], v["form_id"]):
            i = _to_int(fid)
            if i is not None and str(i) in cache:
                entry = cache[str(i)]
                break
        if entry:
            v["marker"] = entry.get("marker", "")
            v["region"] = entry.get("region", "")
            v["coords"] = ([entry["x"], entry["y"]]
                           if entry.get("x") is not None and entry.get("y") is not None else [])
            v["resolved_by"] = entry.get("how", "")
            if v["region"] or v["marker"]:
                resolved += 1
        else:
            v["marker"], v["region"], v["coords"], v["resolved_by"] = "", "", [], "unplaced"
    return resolved


# ── name fallback ─────────────────────────────────────────────────────────────

# Load-order / DLC / role prefixes and noise tokens stripped when deriving a
# readable name from an EDID (generic — no per-vendor mapping).
_EDID_PREFIX_RE = re.compile(
    r"^(?:W05|E05|ATX(?:_COMP)?|BS01|LC\d+|POI\d+|SCORE(?:_S\d+)?|XPD(?:_AC)?|"
    r"Burn\w*|LGV\d*|MILE|NWOT|Storm|COMP|76QA\w*|QA|RE|Debug\w*)_+", re.I)
_EDID_NOISE_RE = re.compile(
    r"\b(?:COMP|Actor|Vendor|Visitor|Faction|Merchant|Robot|Generic|Chest)\b", re.I)


def _pretty_edid(edid):
    """Best-effort readable label from an EDID: drop DLC/role prefixes and noise
    tokens, split camelCase, tidy separators. Generic, not a lookup table."""
    s = _EDID_PREFIX_RE.sub("", edid or "")
    s = s.replace("_", " ")                       # underscores → spaces first, so
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)    # camelCase → words, then \b noise
    s = _EDID_NOISE_RE.sub(" ", s)                # tokens match on real boundaries
    s = re.sub(r"\s+", " ", s).strip()
    return s


def vendor_name(v):
    """FULL name, else '{marker} Vendor', else a cleaned EDID label — never blank."""
    if v["full"]:
        return v["full"]
    if v.get("marker"):
        return f"{v['marker']} Vendor"
    pretty = _pretty_edid(v["edid"]) or _pretty_edid(v["container_base_edid"])
    return f"{pretty} Vendor" if pretty else "Unknown Vendor"


# ── build ─────────────────────────────────────────────────────────────────────

def build(tsv_root, dist_dir, cache_path):
    npc2_name, vendors = load_vendors(tsv_root)
    tables = sources.load_tables(tsv_root)
    sells_n = build_sells(vendors, tables)

    db_ok = os.path.exists(MAPPALACHIA_DB)
    geo = con = cur = None
    cache = load_cache(cache_path)
    if db_ok:
        geo = Geo(MAPPALACHIA_DB)
        con = sqlite3.connect(MAPPALACHIA_DB); cur = con.cursor()
        print(f"[vendors] Mappalachia DB found — resolving locations, refreshing geo cache.")
    elif cache:
        print(f"[vendors] No DB — resolving locations from committed geo cache ({len(cache)} refs).")
    else:
        print(f"[vendors] No Mappalachia DB and no geo cache — locations will be blank. "
              f"Run once locally with MAPPALACHIA_DB set to seed data/vendors/geo_cache.json.")
    loc_n = resolve_locations(vendors, geo, cur, cache, db_ok)

    out = []
    for v in sorted(vendors, key=lambda d: (d.get("region", ""), d.get("marker", ""), vendor_name(d).lower())):
        out.append({
            "name": vendor_name(v),
            "edid": v["edid"],
            "form_id": v["form_id"],
            "faction": v["faction"],
            "buysell": v["buysell"],
            "container_ref": v["container_ref"],
            "container_base": v["container_base_edid"],
            "marker": v.get("marker", ""),
            "region": v.get("region", ""),
            "coords": v.get("coords", []),
            "resolved_by": v.get("resolved_by", ""),
            "sells": v["sells"],
            "sells_formids": v["sells_formids"],
        })

    doc = {
        "_meta": {
            "generated": datetime.date.today().isoformat(),
            "source": f"{npc2_name} + LVLI exports (sells) + Mappalachia Position (locations, cached for CI)",
            "vendors": len(out),
            "with_location": loc_n,
            "with_sells": sells_n,
        },
        "vendors": out,
    }
    os.makedirs(dist_dir, exist_ok=True)
    path = os.path.join(dist_dir, "vendors.json")
    json.dump(doc, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    write_empty_patchlog_feed(dist_dir, "patchlog_latest_df_vendors.json", current_count=len(out))

    print(f"[vendors] Wrote {os.path.relpath(path, REPO)} — "
          f"{len(out)} vendors, {loc_n} located, {sells_n} with stock.")

    if db_ok:
        save_cache(cache, cache_path)
        print(f"[vendors] geo cache saved ({len(cache)} refs) for DB-free CI rebuilds.")
    if con:
        con.close()
    return path


def main(argv=None):
    ap = argparse.ArgumentParser(description="Build the vendor master dist/vendors.json")
    ap.add_argument("--pts", action="store_true",
                    help="Build from PTS data (tsv/pts/) into dist/pts/")
    args = ap.parse_args(argv)

    tsv_root = os.path.join(REPO, "tsv", "pts") if args.pts else os.path.join(REPO, "tsv")
    dist_dir = os.path.join(REPO, "dist", "pts") if args.pts else os.path.join(REPO, "dist")
    cache_path = os.environ.get(
        "VENDORS_GEO_CACHE", os.path.join(REPO, "data", "vendors", "geo_cache.json"))
    build(tsv_root, dist_dir, cache_path)


if __name__ == "__main__":
    main()
