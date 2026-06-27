#!/usr/bin/env python3
"""build_fishing_equipment_json.py — see header in repo."""
import glob, json, os, re, sys
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TSV_DIR = os.path.join(SCRIPT_DIR, "..", "tsv")
DIST_DIR = os.path.join(SCRIPT_DIR, "..", "dist")
DIST_FILE = os.path.join(DIST_DIR, "fishing_equipment.json")

IMAGE_BASE_ROD_SKIN = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/fishing/rod-skins/"
IMAGE_BASE_BOBBER   = "https://www.buffsnbrew.com/wp-content/uploads/guide-images/fishing/bobbers-floats/"

# -----------------------------------------------------------------------
# First-seen persistence — tracks when each FormID was first observed by
# the build pipeline. Used to compute `isNew` (30-day rolling flag / NEW
# pill) without requiring manual release dates. Mirrors the pattern in
# build_collectables_json.py and build_titles_json.py.
#
# Schema for tsv/fishing_equipment_first_seen.json:
#   { "schema": 1, "byFormId": { "007AE10B": "2026-06-26", ... } }
# -----------------------------------------------------------------------
_FIRST_SEEN_FILENAME = "fishing_equipment_first_seen.json"
_NEW_CUTOFF_DAYS = 30

def load_first_seen():
    path = os.path.join(TSV_DIR, _FIRST_SEEN_FILENAME)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return (json.load(f) or {}).get("byFormId", {})
    except Exception as e:
        print(f"[fishing-equipment] WARNING: could not load {_FIRST_SEEN_FILENAME}: {e}", file=sys.stderr)
        return {}

def update_first_seen(first_seen, all_formids, bootstrap):
    """New FormIDs get today's date; on bootstrap (empty file) existing
    items are seeded far in the past so nothing shows as new. Existing
    entries are never overwritten."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    seed = "2020-01-01"
    for fid in all_formids:
        if fid and fid not in first_seen:
            first_seen[fid] = seed if bootstrap else today
    return first_seen

def save_first_seen(first_seen):
    path = os.path.join(TSV_DIR, _FIRST_SEEN_FILENAME)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"schema": 1, "byFormId": dict(sorted(first_seen.items()))}, f, indent=2, ensure_ascii=False)
        f.write("\n")

def compute_new_cutoff():
    return (datetime.now(timezone.utc) - timedelta(days=_NEW_CUTOFF_DAYS)).strftime("%Y-%m-%d")

def find_latest_tsv(pattern):
    files = sorted(f for f in glob.glob(os.path.join(TSV_DIR, pattern)) if "Locations" not in f)
    return files[-1] if files else None

def read_tsv(filepath):
    for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
        try:
            with open(filepath,"r",encoding=enc) as f:
                for line in f: yield line.rstrip("\n").rstrip("\r").split("\t")
            return
        except UnicodeDecodeError: continue
    raise ValueError(f"Could not decode {filepath}")

def load_tsv(filepath):
    rows = iter(read_tsv(filepath))
    header = next(rows, [])
    return {c:i for i,c in enumerate(header)}, rows

SEASON_PREFIX_RE = re.compile(r"^SCORE_S(\d+)_", re.IGNORECASE)
MINI_SEASON_PREFIX_RE = re.compile(r"^SCORE_MiniSeason_(\d{4})_([A-Za-z0-9]+)_", re.IGNORECASE)
SEASON_YEAR = {14:2023,15:2023,16:2023,17:2024,18:2024,19:2024,20:2024,21:2024,22:2025,23:2025,24:2025,25:2025,26:2026,27:2026}

# Mini-season event code -> full event name. Mini-seasons use short codes in
# the EDID (e.g. MMMFE) that don't prettify cleanly, so map them explicitly.
# Mirror new codes here as Bethesda ships them.
MINI_SEASON_NAMES = {
    "MMMFE": "Marshal Mallow's Marvelous Fishing Excursion",
}

def load_season_names():
    """season number -> SeasonName, parsed from tsv/fallout76_seasons.tsv.
    Same source the titles pipeline uses, so the scoreboard prose reads
    identically (e.g. 'Gone Fission Scoreboard (Season 21)')."""
    path = os.path.join(TSV_DIR, "fallout76_seasons.tsv")
    out = {}
    if not os.path.isfile(path):
        return out
    try:
        idx, rows = load_tsv(path)
        if "SeasonNumber" not in idx or "SeasonName" not in idx:
            return out
        for row in rows:
            if len(row) <= max(idx["SeasonNumber"], idx["SeasonName"]):
                continue
            num = (row[idx["SeasonNumber"]] or "").strip()
            name = (row[idx["SeasonName"]] or "").strip()
            if num.isdigit() and name:
                out[int(num)] = name
    except Exception as e:
        print(f"[fishing-equipment] WARNING: could not load season names: {e}", file=sys.stderr)
    return out

def classify_edid(edid, season_names=None, mini_names=None):
    season_names = season_names or {}
    mini_names = mini_names or MINI_SEASON_NAMES
    s = edid or ""
    if s.startswith("ZZZ_"):
        return {"method":"unknown","seasonNumber":None,"seasonName":None,"eventCode":None,"eventName":None,"eventYear":None,"releaseYear":None,"tradeable":False,"display":"Pre-release content. Not currently available in-game.","badge":"Unreleased"}
    if s.startswith("ATX_"):
        return {"method":"atom","seasonNumber":None,"seasonName":None,"eventCode":None,"eventName":None,"eventYear":None,"releaseYear":None,"tradeable":False,"display":"Can be purchased with certain bundles from the Atom Shop.","badge":"Atomic Shop"}
    m = MINI_SEASON_PREFIX_RE.match(s)
    if m:
        year, code = int(m.group(1)), m.group(2)
        name = mini_names.get(code.upper())
        if name:
            display = f"Purchase with tickets from the {name} Mini-Season scoreboard ({year})."
        else:
            display = f"Purchase with tickets from the {year} Mini-Season event scoreboard ({code})."
        return {"method":"mini-season","seasonNumber":None,"seasonName":None,"eventCode":code,"eventName":name,"eventYear":year,"releaseYear":year,"tradeable":False,"display":display,"badge":f"Mini-Season {year}"}
    m = SEASON_PREFIX_RE.match(s)
    if m:
        season = int(m.group(1))
        sname = season_names.get(season)
        if sname:
            display = f"Purchase with tickets from the {sname} Scoreboard (Season {season})."
        else:
            display = f"Purchase with tickets from the Season {season} Scoreboard."
        return {"method":"scoreboard","seasonNumber":season,"seasonName":sname,"eventCode":None,"eventName":None,"eventYear":None,"releaseYear":SEASON_YEAR.get(season),"tradeable":False,"display":display,"badge":f"Season {season}"}
    if s.startswith("Burn_"):
        return {"method":"burning-springs","seasonNumber":None,"seasonName":None,"eventCode":None,"eventName":None,"eventYear":None,"releaseYear":2025,"tradeable":False,"display":"Earned through the Burning Springs region content, introduced in the Blood x Rust update (Season 23).","badge":"Burning Springs"}
    return {"method":"default","seasonNumber":None,"seasonName":None,"eventCode":None,"eventName":None,"eventYear":None,"releaseYear":2024,"tradeable":False,"display":"Unlocked by default — available to all players in the base game.","badge":"Base Game"}

PREFIX_STRIP_RE = re.compile(r"^(ATX_|SCORE_S\d+_|SCORE_MiniSeason_\d{4}_[A-Za-z0-9]+_|Burn_|ZZZ_(?:SCORE_S\d+_)?)", re.IGNORECASE)

def image_filename(edid):
    s = PREFIX_STRIP_RE.sub("", edid or "")
    parts = s.split("_")
    tail = parts[-1] if parts else ""
    return f"{tail}.avif" if tail else ""

def is_rod_skin(edid): return "_RodBase" in (edid or "")
def is_bobber(edid):   return "_RodBobber" in (edid or "")

OMOD_TAIL_RE = re.compile(r"^(?:zzz_?)?.*?_mod_FishingRod_Weapon_(.+)$", re.IGNORECASE)
BOOK_TAIL_RE = re.compile(r"^(?:zzz_?)?.*?_Recipe_mod_FishingRod_(.+)$", re.IGNORECASE)

def normalise_tail(s):
    if not s: return ""
    s = s.lower()
    return re.sub(r"rodbase\d+_", "rodbase_", s)

def load_book_map():
    cands = sorted(f for f in glob.glob(os.path.join(TSV_DIR,"BOOK_Export_*.tsv")) if "Locations" not in f)
    path = cands[-1] if cands else None
    if not path: return {}
    idx, rows = load_tsv(path)
    if "FormID" not in idx or "EDID" not in idx: return {}
    out = {}
    for row in rows:
        if len(row) <= idx["EDID"]: continue
        edid = (row[idx["EDID"]] or "").strip()
        if "_Recipe_mod_FishingRod_" not in edid: continue
        m = BOOK_TAIL_RE.match(edid)
        if not m: continue
        tail = normalise_tail(m.group(1))
        if tail:
            out[tail] = {"formId":(row[idx["FormID"]] or "").strip(),"edid":edid,"name":(row[idx["FULL"]] or "").strip() if "FULL" in idx else ""}
    return out

def load_gmrw_to_chal_map():
    path = find_latest_tsv("GMRW_Export_*.tsv")
    if not path: return {}
    _idx, rows = load_tsv(path)
    out = {}
    for row in rows:
        book_fid = None; chal_fid = None
        for cell in row:
            if not cell: continue
            if cell.endswith(":BOOK") and "_Recipe_mod_FishingRod_" in cell:
                book_fid = cell.split(":",1)[0].strip().lower()
            elif cell.endswith(":CHAL"):
                chal_fid = cell.split(":",1)[0].strip().upper()
        if book_fid and chal_fid: out[book_fid] = chal_fid
    return out

def load_chal_map():
    path = find_latest_tsv("CHAL_Export_*.tsv")
    if not path: return {}
    idx, rows = load_tsv(path)
    if "FormID" not in idx: return {}
    out = {}
    for row in rows:
        if len(row) <= idx["FormID"]: continue
        fid = (row[idx["FormID"]] or "").strip().upper()
        if not fid: continue
        out[fid] = {"full":(row[idx["FULL"]] or "").strip() if "FULL" in idx else "","snam":(row[idx["SNAM"]] or "").strip() if "SNAM" in idx else "","tnam":(row[idx["TNAM"]] or "").strip() if "TNAM" in idx else ""}
    return out

def load_cobj_map():
    """OMOD FormID (upper) -> {cobjFormId, cobjEdid, gateFormId, gateEdid,
    gateFull}. The craftable bobber/float/rod recipe (COBJ 'co_CondProxy_…')
    carries a GNAM gate pointing at the CHAL/QUST that unlocks it — this is the
    real source of truth for challenge-/quest-locked items. Also exposes the
    COBJ FormID/EDID for the Technical block."""
    path = find_latest_tsv("COBJ_Export_*.tsv")
    out = {}
    if not path:
        return out
    idx, rows = load_tsv(path)
    need = ("COBJ_FormID","COBJ_EDID","CNAM_FormID","CNAM_EDID","GNAM_FormID","GNAM_EDID","GNAM_FULL")
    if any(c not in idx for c in need):
        return out
    for row in rows:
        if len(row) <= idx["CNAM_EDID"]:
            continue
        cnam_edid = (row[idx["CNAM_EDID"]] or "")
        if not re.search(r"_mod_FishingRod_Weapon_(RodBobber|RodBase)", cnam_edid):
            continue
        omod = (row[idx["CNAM_FormID"]] or "").strip().upper()
        if not omod:
            continue
        g = lambda k: (row[idx[k]].strip() if idx[k] < len(row) and row[idx[k]] else "")
        out[omod] = {"cobjFormId": g("COBJ_FormID"), "cobjEdid": g("COBJ_EDID"),
                     "gateFormId": g("GNAM_FormID"), "gateEdid": g("GNAM_EDID"),
                     "gateFull": g("GNAM_FULL")}
    return out

def gate_type(edid):
    e = (edid or "").lower()
    if "challenge" in e: return "challenge"
    if "quest" in e or "_mq" in e or e.endswith(":qust"): return "quest"
    return "challenge"

def build_unlock_text(chal):
    full = chal.get("full") or ""
    snam = chal.get("snam") or ""
    tnam = chal.get("tnam") or ""
    try: n = int(tnam)
    except (TypeError, ValueError): n = None
    bits = []
    if full: bits.append(f'Complete the "{full}" challenge')
    if n and snam: bits.append(f"{n} {snam}")
    elif n: bits.append(f"target: {n}")
    return " — ".join(bits).strip() or full or ""

def lookup_unlock(omod_edid, book_map, gmrw_to_chal, chal_map):
    m = OMOD_TAIL_RE.match(omod_edid or "")
    if not m: return None
    tail = normalise_tail(m.group(1))
    book = book_map.get(tail)
    if not book: return None
    book_fid = (book.get("formId") or "").lower()
    if not book_fid: return None
    chal_fid = gmrw_to_chal.get(book_fid)
    if not chal_fid: return None
    chal = chal_map.get(chal_fid.upper())
    if not chal: return None
    chal = dict(chal)
    chal["chalFormId"] = chal_fid
    chal["bookFormId"] = book.get("formId")
    chal["bookEdid"] = book.get("edid")
    chal["bookName"] = book.get("name")
    return chal

def load_season_rewards_map():
    path = os.path.join(TSV_DIR, "season_rewards.tsv")
    if not os.path.isfile(path): return {}
    idx, rows = load_tsv(path)
    needed = ("seasonNumber","page","name","cost","description","storefrontEntitlement")
    for n in needed:
        if n not in idx: return {}
    out = {}
    for row in rows:
        if len(row) <= idx["storefrontEntitlement"]: continue
        ent = (row[idx["storefrontEntitlement"]] or "").strip().lower()
        if not ent: continue
        out[ent] = {"seasonNumber":(row[idx["seasonNumber"]] or "").strip(),"page":(row[idx["page"]] or "").strip(),"cost":(row[idx["cost"]] or "").strip(),"name":(row[idx["name"]] or "").strip(),"description":(row[idx["description"]] or "").strip()}
    return out

def omod_to_possible_entm_edids(edid):
    if not edid: return []
    out = [edid]
    m = re.match(r"^(SCORE_S\d+_|ATX_|SCORE_MiniSeason_\d{4}_[A-Za-z0-9]+_|Burn_|ZZZ_(?:SCORE_S\d+_)?)(Fishing_mod_FishingRod_.*)$", edid)
    if m: out.append(m.group(1) + "ENTM_Weapons_" + m.group(2))
    return out

def build():
    omod_path = find_latest_tsv("OMOD_Export_*.tsv")
    if not omod_path:
        print("[fishing-equipment] No OMOD_Export_*.tsv found", file=sys.stderr); sys.exit(1)
    idx, rows = load_tsv(omod_path)
    col_form = "OMOD_FormID" if "OMOD_FormID" in idx else "FormID"
    col_edid = "OMOD_EDID" if "OMOD_EDID" in idx else "EDID"
    col_full = "FULL"
    for col in (col_form, col_edid, col_full):
        if col not in idx:
            print(f"[fishing-equipment] OMOD TSV missing column {col!r}", file=sys.stderr); sys.exit(1)

    book_map = load_book_map()
    gmrw_to_chal = load_gmrw_to_chal_map()
    chal_map = load_chal_map()
    season_map = load_season_rewards_map()
    season_names = load_season_names()
    cobj_map = load_cobj_map()

    rod_skins, bobbers = [], []
    enriched_chal = 0; enriched_score = 0; skipped_cut = 0

    for row in rows:
        if len(row) <= idx[col_full]: continue
        form_id = (row[idx[col_form]] or "").strip()
        edid    = (row[idx[col_edid]] or "").strip()
        full    = (row[idx[col_full]] or "").strip()
        if not edid or "_mod_FishingRod_Weapon_" not in edid: continue
        if not (is_rod_skin(edid) or is_bobber(edid)): continue

        obtain = classify_edid(edid, season_names, MINI_SEASON_NAMES)
        # Skip cut / pre-release / disabled content entirely — page should
        # only list items players can actually obtain in-game.
        if obtain["method"] == "unknown":
            skipped_cut += 1
            continue

        chal = lookup_unlock(edid, book_map, gmrw_to_chal, chal_map)
        if chal:
            unlock_text = build_unlock_text(chal)
            if unlock_text:
                obtain["challenge"] = {"name":chal.get("full"),"counter":chal.get("snam"),"target":chal.get("tnam"),"chalFormId":chal.get("chalFormId"),"bookFormId":chal.get("bookFormId"),"bookEdid":chal.get("bookEdid"),"bookName":chal.get("bookName")}
                obtain["display"] = unlock_text + ". " + obtain["display"]
                enriched_chal += 1

        for cand in omod_to_possible_entm_edids(edid):
            sr = season_map.get(cand.lower())
            if not sr: continue
            obtain["scoreboard"] = sr
            # Keep the rich season-name prose from classify_edid as the headline
            # source line; the structured scoreboard{} (page / cost / description)
            # is rendered separately by the page so we don't flatten it into one
            # terse sentence. Only synthesise a display if classify produced none.
            season_no = sr.get("seasonNumber") or (obtain.get("seasonNumber") or "")
            if not obtain.get("display"):
                sname = season_names.get(int(season_no)) if str(season_no).isdigit() else None
                if sname:
                    obtain["display"] = f"Purchase with tickets from the {sname} Scoreboard (Season {season_no})."
                elif season_no:
                    obtain["display"] = f"Purchase with tickets from the Season {season_no} Scoreboard."
            if season_no and not obtain.get("seasonNumber"):
                try: obtain["seasonNumber"] = int(season_no)
                except (TypeError, ValueError): pass
            if not obtain.get("seasonName") and str(season_no).isdigit():
                obtain["seasonName"] = season_names.get(int(season_no))
            enriched_score += 1
            break

        # ---- COBJ gate (challenge / quest unlock) + Technical FormID/EDID ----
        cobj = cobj_map.get((form_id or "").upper())
        cobj_form = cobj.get("cobjFormId") if cobj else None
        cobj_edid = cobj.get("cobjEdid") if cobj else None
        gate_full = (cobj.get("gateFull") if cobj else "") or ""
        gate_fid  = (cobj.get("gateFormId") if cobj else "") or ""
        gate_edid = (cobj.get("gateEdid") if cobj else "") or ""

        if obtain["method"] == "default":
            # Fishing shipped with Season 21 (June 2025).
            obtain["releaseYear"] = 2025
            if gate_full:
                gt = gate_type(gate_edid)
                obtain["method"] = "challenge"
                obtain["badge"] = "Challenge"
                obtain["display"] = f'Unlocked by completing the "{gate_full}" {gt}.'
                obtain["challenge"] = {"name": gate_full, "chalFormId": gate_fid or None}
            else:
                obtain["display"] = "Unlocked by default — available to every player once fishing is unlocked."
        elif obtain["method"] == "burning-springs" and gate_full:
            gt = gate_type(gate_edid)
            obtain["display"] = f'Unlocked by completing the "{gate_full}" {gt}.'
            obtain["challenge"] = {"name": gate_full, "chalFormId": gate_fid or None}
            enriched_chal += 1

        item = {"formId":form_id,"edid":edid,"name":full or edid,"imageFilename":image_filename(edid),"imageUrl":"","cobjFormId":cobj_form,"cobjEdid":cobj_edid,"howToObtain":obtain,"isNew":False,"cutContent":False}
        if is_rod_skin(edid): rod_skins.append(item)
        else: bobbers.append(item)

    method_order = {"default":0,"challenge":0,"burning-springs":1,"scoreboard":2,"mini-season":3,"atom":4}
    def sort_key(it): return (method_order.get(it["howToObtain"]["method"], 8), it["name"].lower())
    rod_skins.sort(key=sort_key); bobbers.sort(key=sort_key)

    # ---- First-seen persistence & isNew (NEW pill) ----
    all_items = rod_skins + bobbers
    first_seen_path = os.path.join(TSV_DIR, _FIRST_SEEN_FILENAME)
    bootstrap = not os.path.exists(first_seen_path)
    first_seen = load_first_seen()
    update_first_seen(first_seen, [it["formId"] for it in all_items], bootstrap)
    save_first_seen(first_seen)
    cutoff = compute_new_cutoff()
    new_count = 0
    for it in all_items:
        seen = first_seen.get(it["formId"], "2020-01-01")
        it["isNew"] = (seen >= cutoff)
        if it["isNew"]:
            new_count += 1

    payload = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "schemaVersion": 3,
        "newCutoffDays": _NEW_CUTOFF_DAYS,
        "imageBases": {"rodSkin": IMAGE_BASE_ROD_SKIN, "bobber": IMAGE_BASE_BOBBER},
        "rodSkins": rod_skins,
        "bobbersAndFloats": bobbers,
    }
    os.makedirs(DIST_DIR, exist_ok=True)
    with open(DIST_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False); f.write("\n")
    boot_note = " [bootstrap: seeded existing as not-new]" if bootstrap else ""
    print(f"[fishing-equipment] OK: {len(rod_skins)} rod skins, {len(bobbers)} bobbers & floats (CHAL: {enriched_chal}, Scoreboard: {enriched_score}, skipped cut: {skipped_cut}, NEW: {new_count}){boot_note} -> {DIST_FILE}")

if __name__ == "__main__":
    build()
