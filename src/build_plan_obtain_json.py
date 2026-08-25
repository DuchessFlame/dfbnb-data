#!/usr/bin/env python3
r"""
build_plan_obtain_json.py — Plan Checklists "How to Obtain" pipeline.

Builds dist/plan_master.json for the DF/BNB plan-checklist pages
(apparel / armour / backpack-mod / recipe / weapon), rendered by
df-bnb-plan-checklists.js (renderChecklist). Every rate is resolved with
rng76 (never a bare ChanceNone, never a hardcoded FormID).

ROSTER
------
Learnable plans are BOOK records whose FULL name starts "Plan: " or
"Recipe: " (KYWD ObjectTypeRecipe). The BOOK FormID is what actually drops
in leveled lists, so it is the rng76 TARGET.

BOOK -> created object (category + image box)
--------------------------------------------
The LVLI entry that yields a plan carries a
`Target.HasLearnedRecipe(... co_X [COBJ:xxxxxxxx] ...)` condition. That COBJ
is the recipe the plan teaches; its CNAM (created object) FormID resolved to a
record signature tells us physical-vs-mod:
  * created OMOD (or a mod/paint recipe)          -> MOD    -> NO image box
  * created WEAP/ARMO/FURN/CONT/STAT/MSTT/ACTI/MISC -> PHYSICAL -> image box
  * Workshop/CAMP recipe (co_Workshop_*, CondProxy) -> PHYSICAL -> image box
  * consumable (ALCH food/chem)                    -> no placeable -> NO image box
When the created object can't be resolved, has_image_box is False (safe: no
broken image slot) and the fact is reported.

MULTI-ROUTE OBTAIN (container-type -> drop%, generalised)
---------------------------------------------------------
Reuses spawns_engine.sources.get_sources (LVLI up-closure + placed holders)
and the farming Containers resolver. Every distinct source/route is a line
with the plan's rng76-resolved appearance chance for that source. Distinct
rates are listed separately, identical rates dedupe, 0% is dropped, sorted by
rate desc. Buckets: container / vendor / creature / event-quest / fixed.

FLAGS (Technical)
-----------------
tradeable          : BOOK KYWDs. NonPlayerTradable or NonDroppable -> False,
                     otherwise True. (UnsellableObject alone only blocks vendor
                     SALE, not player trade, so it does not set False.)
stops_dropping     : True  if >=1 drop entry gates on HasLearnedRecipe (removed
                            from the pool once learnt);
                     False if it appears in leveled lists but NONE gate on it;
                     None  if it appears in no leveled list (purchase/quest only
                            — unresolved, rendered honestly as "Unknown").
"""
import os, re, csv, glob, json, argparse, collections, sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TSV  = os.path.join(REPO, "tsv")
DIST = os.path.join(REPO, "dist")
sys.path.insert(0, HERE)

import rng76
from spawns_engine import sources as ssrc
# reuse the farming Containers resolver + rate helpers + rng76 wrapper
import build_farming_used_for as bfu

# ── export selection ─────────────────────────────────────────────────────────
def newest(pat, root=None):
    fs = glob.glob(os.path.join(root or TSV, pat))
    return max(fs, key=os.path.getmtime) if fs else None

def read_rows(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        yield from csv.DictReader(f, delimiter="\t")

# ── FormID -> signature index (for CNAM created-object classification) ────────
SIG_EXPORTS = {
    "WEAP":"WEAP_Export_*Base.tsv","ARMO":"ARMO_Export_*ARMOUR.tsv","OMOD":"OMOD_Export_*.tsv",
    "FURN":"FURN_Export_*FURN.tsv","CONT":"CONT_Export_*.tsv","ACTI":"ACTI_Export_*ACTI.tsv",
    "MISC":"MISC_Export_*.tsv","STAT":"STAT_Export_*.tsv","MSTT":"MSTT_Export_*.tsv",
    "ALCH":"ALCH_Export_*.tsv","BOOK":"BOOK_Export_*.tsv","KEYM":"KEYM_Export_*.tsv",
    "AMMO":"AMMO_Export_*.tsv","NOTE":"NOTE_Export_*.tsv",
}
def build_sig_index():
    out = {}
    for sig, pat in SIG_EXPORTS.items():
        f = newest(pat)
        if not f: continue
        with open(f, encoding="utf-8", errors="replace") as fh:
            r = csv.reader(fh, delimiter="\t"); hdr = next(r, None)
            if not hdr: continue
            fidcol = 0
            for i, h in enumerate(hdr):
                if h.strip() == "FormID" or h.strip().endswith("_FormID"):
                    fidcol = i; break
            for row in r:
                if len(row) > fidcol and row[fidcol].strip():
                    out.setdefault(row[fidcol].strip().upper(), sig)
    return out

# ── COBJ: FormID -> {edid, cnam_fid, cnam_edid, cnam_full, bnam_edid} ─────────
def build_cobj_index():
    f = newest("COBJ_Export_*.tsv"); out = {}
    if not f: return out
    for row in read_rows(f):
        fid = (row.get("COBJ_FormID") or "").strip().upper()
        if fid:
            out[fid] = {
                "edid": (row.get("COBJ_EDID") or "").strip(),
                "cnam_fid": (row.get("CNAM_FormID") or "").strip().upper(),
                "cnam_edid": (row.get("CNAM_EDID") or "").strip(),
                "cnam_full": (row.get("CNAM_FULL") or "").strip(),
                "bnam_edid": (row.get("BNAM_EDID") or "").strip(),
            }
    return out

# ── LVLI entries: BOOK -> [entry dicts]; + a global list of entries per list ──
_HLR = re.compile(r"HasLearnedRecipe\([^)]*\[COBJ:([0-9A-Fa-f]{8})\]", re.I)
def build_book_entry_index():
    f = newest("LVLI_Export_*_LVLI_Entries.tsv")
    book_entries = collections.defaultdict(list)
    for row in read_rows(f):
        ref = (row.get("LVLO_Reference") or "")
        if ":BOOK" not in ref.upper():
            continue
        book_fid = ref.split(":")[0].strip().upper()
        conds = " ".join((row.get(f"Cond{i}") or "") for i in range(1, 11))
        hlr = [m.group(1).upper() for m in _HLR.finditer(conds)]
        book_entries[book_fid].append({
            "list": (row.get("LVLI_FormID") or "").strip().upper(),
            "list_edid": (row.get("LVLI_EDID") or "").strip(),
            "has_learned_recipe": bool(hlr),
            "recipe_cobj": hlr[0] if hlr else "",
        })
    return book_entries

# ── categorisation ───────────────────────────────────────────────────────────
PHYS_SIGS = {"WEAP","ARMO","FURN","CONT","STAT","MSTT","ACTI","MISC","KEYM"}
def classify_plan(book_edid, cobj):
    """Return (category, has_image_box, cnam_sig, cnam_fid, cnam_edid)."""
    e = (book_edid or "").lower()
    cnam_fid = (cobj or {}).get("cnam_fid", "")
    cnam_edid = (cobj or {}).get("cnam_edid", "")
    co_edid = (cobj or {}).get("edid", "").lower()
    sig = SIG_INDEX.get(cnam_fid, "")
    blob = e + " " + co_edid + " " + cnam_edid.lower()

    # mods / paints — no image box
    is_mod = (sig == "OMOD") or ("recipe_mod_" in e) or ("_mod_" in co_edid) \
             or ("paint" in blob) or ("skin" in blob)
    if is_mod:
        if "backpack" in blob:                     cat = "backpack-mod"
        elif any(k in blob for k in ("armor","armour","powerarmor","power_armor","pa_")):
            cat = "armour"
        elif any(k in blob for k in ("weapon","melee","ranged","gun","rifle","pistol")):
            cat = "weapon"
        else:                                       cat = "recipe"
        return cat, False, sig, cnam_fid, cnam_edid

    # physical created object
    if sig == "WEAP":
        return "weapon", True, sig, cnam_fid, cnam_edid
    if sig == "ARMO":
        cat = "apparel" if any(k in blob for k in ("outfit","apparel","underarmor","under_armor","dress","costume","uniform")) else "armour"
        return cat, True, sig, cnam_fid, cnam_edid
    if sig in PHYS_SIGS or "recipe_workshop" in e or "co_workshop" in co_edid or "workshop_co" in co_edid:
        return "recipe", True, (sig or "FURN"), cnam_fid, cnam_edid   # CAMP/workshop placeable
    if sig == "ALCH":
        return "recipe", False, sig, cnam_fid, cnam_edid              # food/chem, no placeable

    # unresolved created object -> default recipe bucket, NO image box (honest)
    return "recipe", False, sig, cnam_fid, cnam_edid

# ── tradeable / stops_dropping ───────────────────────────────────────────────
def resolve_tradeable(kw_blob):
    k = (kw_blob or "").lower()
    if "nonplayertradable" in k or "nondroppable" in k:
        return False
    if "objecttyperecipe" in k or "unsellableobject" in k:
        return True   # standard plan: sellable-block only, still player-tradeable
    return None       # unknown -> honest "Unknown"

_CAT_NOUN = {"apparel":"Apparel","armour":"Armour","backpack-mod":"Backpack Mod",
             "recipe":"CAMP / Recipe","weapon":"Weapon"}
def category_label(cat, has_img, cnam_sig):
    noun = _CAT_NOUN.get(cat, cat.title())
    if not has_img:
        if cnam_sig == "OMOD" or cat == "backpack-mod":
            return f"{noun} Mod / Paint"
        if cnam_sig == "ALCH":
            return "Consumable Recipe"
        return f"{noun} (mod/recipe)"
    return f"{noun} (physical plan)"

def resolve_stops_dropping(entries):
    if not entries:
        return None
    if any(en["has_learned_recipe"] for en in entries):
        return True
    return False

# ── route resolution (container-type -> drop%, generalised) ──────────────────
def humanize(edid):
    s = re.sub(r"^(LL[ESV]?_|LLD_|LL_|co_|Recipe_|recipe_)", "", edid or "")
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s).replace("_", " ")
    return re.sub(r"\s+", " ", s).strip() or (edid or "Source")

def plan_classify(sig, edid, via_edid):
    e = (edid or "").lower() + " " + (via_edid or "").lower()
    if "vendorchest" in e or "vendor" in e or "vend" in e: return "vendor"
    if "questreward" in e or "quest_reward" in e or "_reward" in e or "gmrw" in e \
       or "systemic" in e or "quest" in e: return "event-quest"
    if sig == "NPC_" or "creature" in e or "lle_" in e or "_npc_" in e: return "creature"
    if sig == "REFR": return "fixed"
    if sig == "CONT": return "container"
    return "loot-list"

BUCKET_LABEL = {"container":"container","vendor":"vendor","creature":"creature",
                "event-quest":"event / quest","fixed":"fixed spawn","loot-list":"loot pool"}

# Collapse a variant-heavy source name to a family (e.g. all Deathclaw NPC
# variants -> "Deathclaw") so creature/event routes are distinct-rate rows, not
# a location/enemy dump.
_STOPWORDS = {"lvl","scorched","assault","consort","glowing","legendary","mq07",
              "e01","kk05","bs01","bs02","mq02","audio","template","01","02","03",
              "stage9000","stage","missing","ll"}
def family_name(name):
    toks = [t for t in re.split(r"[\s_]+", name or "") if t]
    keep = [t for t in toks if t.lower() not in _STOPWORDS and not re.fullmatch(r"[0-9]+", t)]
    # take the last 1-2 meaningful tokens (the noun), else fall back to the whole
    if not keep:
        return (name or "Source").strip()
    return " ".join(keep[-2:]) if len(keep) >= 2 and keep[-1].istitle() else keep[-1]

def resolve_routes(target_fid, tables, rates, cont_names):
    target = {target_fid}
    src = ssrc.get_sources([{"formid": target_fid, "sig": "BOOK"}], tables, plan_classify)
    closure = src["lvli_closure"]
    lvli_refs = tables["lvli_refs"]; parent_edid = tables["parent_edid"]

    # One appearance computation per list, shared by containers + the loop below.
    _memo = {}
    def app(L):
        k = str(L).upper()
        if k not in _memo:
            _memo[k] = rates.appearance([L], target)
        return _memo[k]

    routes = []
    # 1) Container types (reuse the farming resolver verbatim, memoised)
    try:
        conts = bfu.container_types(closure, target,
                                    lambda L, t: app(L[0] if isinstance(L,(list,tuple)) and L else L),
                                    cont_names, lvli_refs, parent_edid)
    except Exception:
        conts = []
    for c in conts:
        routes.append({"route": c["name"], "source_type": "container",
                       "rate": round(c["rate"], 6), "rate_display": c["rate_display"]})

    # 2) Non-container routes — resolve each closure list's rng76 appearance and
    #    name it by the list's own semantic (its leveled-list edid). This keeps
    #    the "distinct rate per source" model without dumping every placed holder
    #    (e.g. all Deathclaw NPC variants collapse to one "Deathclaw" row).
    #    Vendors stay distinct by name; creature/event/loot collapse by
    #    (bucket, family, rate) so identical-rate variants dedupe.
    seen_c = set((r["source_type"], r["route"], round(r["rate"], 4)) for r in routes)  # containers
    seen_n = {}  # (bucket, family, rate4) -> route dict (collapse variants)
    for L in closure:
        via = parent_edid.get(L, "")
        holders = lvli_refs.get(L) or lvli_refs.get(str(L).upper()) or ()
        # bucket from the list's own edid + any non-CONT holder edid (CHEAP — before rng76)
        holder_blob = " ".join((redid or "") for rf, redid, rsig in holders if rsig != "CONT")
        bucket = plan_classify("", (via or "") + " " + holder_blob, via)
        if bucket in ("container", "loot-list"):
            continue  # containers handled above; loot-list = internal plumbing, not a world source
        rate = app(L)                       # only now do the rng76 resolve
        if not rate or rate <= 0:
            continue
        # name: a vendor keeps its CONT/holder name; others use the list family
        vend_name = None
        for rf, redid, rsig in holders:
            if plan_classify(rsig, redid, via) == "vendor":
                vend_name = cont_names.get((rf or "").upper()) or humanize(redid); break
        if bucket == "vendor" and vend_name:
            name = vend_name
            key = ("vendor", name, round(rate, 4))
            if key not in seen_c:
                seen_c.add(key)
                routes.append({"route": name, "source_type": "vendor",
                               "rate": round(rate, 6), "rate_display": bfu._fmt_rate(rate)})
            continue
        fam = family_name(humanize(via or L))
        k = (bucket, fam.lower(), round(rate, 4))
        if k not in seen_n:
            seen_n[k] = {"route": fam, "source_type": bucket,
                         "rate": round(rate, 6), "rate_display": bfu._fmt_rate(rate)}
    routes.extend(seen_n.values())

    routes.sort(key=lambda r: (-(r["rate"] or 0), r["source_type"], r["route"].lower()))
    return routes[:12]   # cap: a plan's most-likely dozen sources, highest rate first

# ── main ─────────────────────────────────────────────────────────────────────
SIG_INDEX = {}
def main(argv=None):
    global TSV, DIST, SIG_INDEX
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="cap roster (0=all) for testing")
    ap.add_argument("--offset", type=int, default=0, help="skip the first N of the roster (for chunked builds)")
    ap.add_argument("--only", default="", help="only plans whose name contains this substring")
    ap.add_argument("--data-dir", default=TSV, help="TSV export root (PTS build points this at the PTS tsvs)")
    ap.add_argument("--outdir", default=DIST, help="output dir (PTS build relocates dist/ -> dist/pts/)")
    ap.add_argument("--out", default="", help="explicit output file (overrides --outdir/plan_master.json)")
    ap.add_argument("--report", default="", help="write an unresolved-flags report here")
    ap.add_argument("--no-routes", action="store_true",
                    help="skip rng76 route resolution (fast full-roster flag pass)")
    args = ap.parse_args(argv)

    TSV = args.data_dir
    DIST = args.outdir
    if not args.out:
        args.out = os.path.join(DIST, "plan_master.json")
    print("[plan-obtain] loading exports ...")
    SIG_INDEX = build_sig_index()
    cobj_idx  = build_cobj_index()
    book_ent  = build_book_entry_index()
    tables = rates = cont_names = None
    if not args.no_routes:
        tables    = ssrc.load_tables(TSV)
        rates     = bfu.VendorRates(rng76.Rng76Data.from_tsv_root(TSV))
        cont_names = bfu._load_cont_names(TSV)

    bf = newest("BOOK_Export_*.tsv")
    roster = []
    for row in read_rows(bf):
        full = (row.get("FULL") or "").strip()
        if not (full.startswith("Plan: ") or full.startswith("Recipe: ")):
            continue
        # BOOK ReferencedBy Ref1..RefN → first :COBJ is the recipe it teaches
        # (fallback when the plan has no HasLearnedRecipe drop entry).
        co_ref = ""
        for j in range(1, 46):
            v = (row.get(f"Ref{j}") or "").strip()
            if v.upper().endswith(":COBJ"):
                co_ref = v.split(":")[0].strip().upper(); break
        row["_co_ref"] = co_ref
        roster.append(row)
    print(f"[plan-obtain] roster: {len(roster)} plan/recipe BOOKs")

    if args.only:
        roster = [r for r in roster if args.only.lower() in (r.get("FULL") or "").lower()]
    if args.offset:
        roster = roster[args.offset:]
    if args.limit:
        roster = roster[:args.limit]
    print(f"[plan-obtain] building {len(roster)} plans ...")

    items = []
    unresolved = {"created_object": [], "tradeable": [], "stops_dropping": [], "no_routes": []}
    for i, row in enumerate(roster):
        fid  = (row.get("FormID") or "").strip().upper()
        edid = (row.get("EDID") or "").strip()
        name = (row.get("FULL") or "").strip()
        kwblob = " ".join((row.get(f"KW{j}") or "") for j in range(1, 8))
        entries = book_ent.get(fid, [])

        # created object: prefer the HasLearnedRecipe COBJ (authoritative), else
        # the BOOK's own ReferencedBy :COBJ link (vendor-only / no-drop plans).
        co_fid = ""
        for en in entries:
            if en["recipe_cobj"]:
                co_fid = en["recipe_cobj"]; break
        if not co_fid:
            co_fid = row.get("_co_ref") or ""
        cobj = cobj_idx.get(co_fid) if co_fid else None
        # CondProxy recipes carry no CNAM; hop to the real recipe if it points on.
        if cobj and not cobj.get("cnam_fid") and "condproxy" in cobj.get("edid","").lower():
            # find a sibling COBJ sharing the stem after 'CondProxy_' with a CNAM
            stem = re.sub(r".*condproxy_?", "", cobj["edid"], flags=re.I).lower()
            if stem:
                for cfid, c in cobj_idx.items():
                    ce = c.get("edid","").lower()
                    if c.get("cnam_fid") and stem in ce and "condproxy" not in ce:
                        co_fid, cobj = cfid, c; break
        cat, has_img, cnam_sig, cnam_fid, cnam_edid = classify_plan(edid, cobj)
        if not cnam_sig and not (cobj or {}).get("edid"):
            unresolved["created_object"].append(name)

        tradeable = resolve_tradeable(kwblob)
        if tradeable is None: unresolved["tradeable"].append(name)
        stops = resolve_stops_dropping(entries)
        if stops is None: unresolved["stops_dropping"].append(name)

        routes = [] if args.no_routes else resolve_routes(fid, tables, rates, cont_names)
        if not args.no_routes and not routes:
            unresolved["no_routes"].append(name)

        cat_label = category_label(cat, has_img, cnam_sig)
        obtain_text = ("Learned from a plan. Drops from the sources below, each with its "
                       "resolved chance.") if routes else \
                      ("Learned from a plan. No random-loot drop routes were resolved — "
                       "see Technical for the recipe details.")

        item = {
            "kind": "plan", "brand": "df", "type": cat,
            "id": f"PLAN_{fid}", "name": name,
            "has_image_box": has_img, "image_dir": cat,
            "obtain": obtain_text, "category_label": cat_label,
            "obtain_routes": routes,
            "plan_item": {"formid": fid, "edid": edid},
            "cobj": ({"formid": co_fid, "edid": cobj["edid"]} if cobj else None),
            "cnam": ({"formid": cnam_fid, "edid": cnam_edid, "sig": cnam_sig} if cnam_fid else None),
            "tradeable": tradeable, "stops_dropping": stops,
            "cut": False,
        }
        items.append(item)
        if (i+1) % 250 == 0:
            print(f"   ... {i+1}/{len(roster)}")

    out = {
        "version": 1,
        "generated": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "source_files": {k: os.path.basename(newest(v) or "") for k, v in SIG_EXPORTS.items()},
        "items": items,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[plan-obtain] wrote {args.out}  ({len(items)} plans)")

    # category + flag summary
    from collections import Counter
    print("  categories:", dict(Counter(it["type"] for it in items)))
    print("  image box :", dict(Counter(it["has_image_box"] for it in items)))
    print("  tradeable :", dict(Counter(it["tradeable"] for it in items)))
    print("  stops_drop:", dict(Counter(it["stops_dropping"] for it in items)))
    print("  UNRESOLVED created_object:", len(unresolved["created_object"]),
          "| tradeable:", len(unresolved["tradeable"]),
          "| stops_dropping:", len(unresolved["stops_dropping"]),
          "| no_routes:", len(unresolved["no_routes"]))
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(unresolved, f, ensure_ascii=False, indent=2)
    return out

if __name__ == "__main__":
    main()
