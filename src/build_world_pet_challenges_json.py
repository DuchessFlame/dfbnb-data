#!/usr/bin/env python3
"""
build_world_pet_challenges_json.py

Builds dist/world_pet_challenges.json — the data feed for the World Pets ->
World Pet Challenges page (/df/pets/world-pets/world-pet-challenges/).

Per the challenge-style-guide decision (June 2026), the 20 lifetime World Pet
challenges were moved OFF the hand-authored ch()/CHALLENGES_LIST array in
df-bnb-world-pets.js and onto a generated contract, so new tiers appear from a
TSV re-run. Everything factual (FormID, name, required count, conditions) comes
from the CHAL/CNDF exports; only the editorial how-to `desc` and `reward`
summary live in the OVERLAY below (ported verbatim from the old hand-authored
list). Conditions are leaf-expanded through CNDF condition-forms exactly like
build_challenges_json_v3.py (e.g. species check -> HasKeyword CampPets_Cat).

Architecture (matches every other world-pet page):
  xEdit TSV exports -> this script -> dist/world_pet_challenges.json -> df-bnb-world-pets.js

Usage:
  python build_world_pet_challenges_json.py [--tsv-dir tsv] [--out-dir dist]
"""

import csv, glob, os, re, json, argparse, datetime
from pathlib import Path
import tsv_source          # one resolver for every export selection

parser = argparse.ArgumentParser()
parser.add_argument("--tsv-dir", default="tsv",  help="Folder containing TSV exports")
parser.add_argument("--out-dir", default="dist", help="Output folder for JSON files")
args = parser.parse_args()
TSV_DIR = Path(args.tsv_dir)
OUT_DIR = Path(args.out_dir)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# TSV helpers
# ---------------------------------------------------------------------------
_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

def _filename_date_key(path):
    """Chronological sort key for an export filename.

    Delegates to tsv_source so all 22 copies of this helper agree, and so PTS
    filenames (ACTI_Export_PTS_2026-08-22_0925.tsv) stop scoring as "undated".
    """
    return tsv_source.export_key(path)

_SIDECAR_RE = re.compile(r'_\d{4}_.+\.tsv$', re.IGNORECASE)

def _drop_sidecars(pattern, files):
    """Exclude sidecar tables unless the pattern actually asked for one.

    KYWD_Export_June_2026.tsv (FormID / EDID / FULL_Name) and
    KYWD_Export_June_2026_Refs.tsv (KeywordFormID / RefIndex / …) sort equal on
    year+month, so the mtime tiebreaker picked the Refs file — which has no
    FormID column, so kywd_by_fid came out EMPTY and every species check
    shipped as HasKeyword(0079AF1C [FORM:0079AF1C]) instead of HasKeyword(Dog).
    This build has no inline-name fallback: its species checks are raw
    little-endian byte refs resolved through FUNC_INDEX, so the table is the
    only source of the name.

    A pattern naming its own suffix (…_*_Entries.tsv) is left alone."""
    if re.search(r'_\*_[A-Za-z]', pattern) or not pattern.endswith("*.tsv"):
        return files
    main = [f for f in files if not _SIDECAR_RE.search(os.path.basename(f))]
    return main or files

def newest(pattern):
    """Pick the most recent file matching *pattern*.
    Primary sort: parsed year+month from filename (reliable on GitHub Actions
    where git checkout mtimes vary by checkout order, not commit date).
    Tiebreaker: file mtime (useful on local machines)."""
    fs = _drop_sidecars(pattern, glob.glob(str(TSV_DIR / pattern)))
    if not fs:
        return None
    fs.sort(key=lambda x: (_filename_date_key(x), os.path.basename(x)))
    return fs[-1]

def read_tsv(path):
    if not path or not os.path.exists(path): return []
    with open(path, encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f, delimiter="\t"))

def pick(row, *keys, default=""):
    for k in keys:
        if k in row and row[k] not in (None, ""): return row[k]
    return default

def safe_int(v, d=None):
    try: return int(str(v).strip())
    except Exception: return d

CHAL = read_tsv(newest("CHAL_Export_*.tsv"))
CNDF = read_tsv(newest("CNDF_Export_*.tsv"))
KYWD = read_tsv(newest("KYWD_Export_*.tsv"))
ENTM = read_tsv(newest("ENTM_Export_*.tsv"))
MISC = read_tsv(newest("MISC_Export_*.tsv"))

kywd_by_fid = {pick(r, "FormID").upper(): r for r in KYWD if pick(r, "FormID")}
cndf_by_fid = {pick(r, "FormID").upper(): r for r in CNDF if pick(r, "FormID")}
entm_by_fid = {pick(r, "FormID"): r for r in ENTM if pick(r, "FormID")}
misc_by_fid = {pick(r, "FormID"): r for r in MISC if pick(r, "FormID")}

KNOWN_FID_NAMES = {"0000000F": "Caps"}

def resolve_name_from_fid(fid):
    if not fid: return ""
    fid = fid.strip().upper()
    if fid in KNOWN_FID_NAMES: return KNOWN_FID_NAMES[fid]
    if fid in kywd_by_fid: return pick(kywd_by_fid[fid], "FULL_Name", "EDID") or fid
    if fid in misc_by_fid: return pick(misc_by_fid[fid], "FULL", "EDID") or fid
    if fid in entm_by_fid: return pick(entm_by_fid[fid], "FULL", "NNAM") or fid
    return fid

# ---------------------------------------------------------------------------
# xEdit-format condition decode with CNDF leaf-expansion
# (kept identical to build_challenges_json_v3.conditions_display)
# ---------------------------------------------------------------------------
FUNC_INDEX = {"940": "HasKeyword", "941": "HasKeyword"}

def _fid_from_bytes(b):
    parts = str(b or "").strip().split()
    if len(parts) == 4 and all(re.fullmatch(r"[0-9A-Fa-f]{2}", p) for p in parts):
        return "".join(reversed([p.upper() for p in parts]))
    return None

def _fmt_cond_value(v):
    try:
        f = float(v); return str(int(f)) if f == int(f) else f"{f:.6f}"
    except Exception:
        return str(v or "").strip()

def _cond_param_label(param):
    p = str(param or "").strip()
    ref_m = re.search(r'\[(\w+):([0-9A-Fa-f]+)\]', p)
    if ref_m:
        rec, fid = ref_m.group(1), ref_m.group(2).upper()
        qm = re.search(r'"([^"]+)"', p)
        em = re.match(r'([A-Za-z0-9_]+)\s*[\["]', p)
        hint = qm.group(1) if qm else (em.group(1) if em else "")
        resolved = resolve_name_from_fid(fid)
        disp = resolved if (resolved and resolved != fid) else hint
        return (disp or hint or fid), f"[{rec}:{fid}]"
    fid = _fid_from_bytes(p)
    if fid:
        resolved = resolve_name_from_fid(fid)
        sig = "KYWD" if fid in kywd_by_fid else "FORM"
        return (resolved if resolved and resolved != fid else fid), f"[{sig}:{fid}]"
    return p, ""

def _cndf_conditions(row):
    out = []
    count = safe_int(pick(row, "CondCount"), 76) or 76
    for i in range(1, min(count + 1, 77)):
        c = pick(row, f"Cond{i}")
        if c: out.append(c)
    return out

def _is_null_leaf(line):
    return bool(re.search(r'\[(?:FORM|KYWD):0+\]', line) or re.search(r'\(0+\s', line))

def decode_condition(cond_str, _seen=None):
    if _seen is None: _seen = set()
    s = str(cond_str or "").strip()
    if not s: return []
    if "|" not in s:
        return [re.sub(r'^(Top|More|And|Or):', '', s).strip()]
    parts = s.split("|")
    value = _fmt_cond_value(parts[1] if len(parts) > 1 else "")
    func = FUNC_INDEX.get((parts[2] if len(parts) > 2 else "").strip(),
                          (parts[2] if len(parts) > 2 else "").strip())
    param = parts[5] if len(parts) > 5 else ""
    runon = (parts[9].strip() if len(parts) > 9 else "") or "Subject"
    prefix = "." if runon == "Subject" else f"{runon}."
    if func == "IsTrueForConditionForm":
        ref_m = re.search(r'\[CNDF:([0-9A-Fa-f]+)\]', param)
        if ref_m:
            cfid = ref_m.group(1).upper()
            if cfid not in _seen and cfid in cndf_by_fid:
                _seen.add(cfid)
                leaves = []
                for ic in _cndf_conditions(cndf_by_fid[cfid]):
                    leaves.extend(decode_condition(ic, _seen))
                meaningful = [l for l in leaves if not _is_null_leaf(l)]
                if meaningful:
                    return meaningful
    disp, ref = _cond_param_label(param)
    if ref:  return [f"{prefix}{func}({disp} {ref}) = {value}"]
    if disp: return [f"{prefix}{func}({disp}) = {value}"]
    return [f"{prefix}{func}() = {value}"]

def conditions_display(conditions):
    out, seen = [], set()
    for c in (conditions or []):
        cs = str(c or "")
        if "IsFalloutWorlds" in cs or "GetIsForm" in cs:
            continue
        for line in decode_condition(cs):
            if line and line not in seen:
                seen.add(line); out.append(line)
    return out

def extract_conditions(row):
    conds = []
    count = safe_int(pick(row, "CondCount"), 52) or 52
    for i in range(1, min(count + 1, 53)):
        c = pick(row, f"Cond{i}")
        if c: conds.append(c)
    return conds

# ---------------------------------------------------------------------------
# Editorial overlay
# ---------------------------------------------------------------------------
OVERLAY = {
    "AnyPet_ActivatePet01":   ("Activate a World Pet from your Pip-Boy. Only one pet can be active at a time.", "10 Caps"),
    "AnyPet_GivePetCommand01":("Issue a command to your active pet.", "10 Caps"),
    "AnyPet_HealPet01":       ("Heal your active pet when it takes damage.", ""),
    "AnyPet_HealPet02":       ("Heal your active pet when it takes damage.", ""),
    "AnyPet_HealPet03":       ("Heal your active pet when it takes damage.", ""),
    "AnyPet_PetKill01":       ("Kill hostiles while your World Pet is active and out with you.", ""),
    "AnyPet_PetKill02":       ("Kill hostiles while your World Pet is active and out with you.", ""),
    "AnyPet_PetKill03":       ("Kill hostiles while your World Pet is active and out with you.", ""),
    "Cat_CatchFish01":        ("Catch fish while a Pet Cat is your active World Pet.", "100 Caps"),
    "Cat_CatchFish02":        ("Catch fish while a Pet Cat is your active World Pet.", ""),
    "Cat_CatchFish03":        ("Catch fish while a Pet Cat is your active World Pet.", ""),
    "Dog_BountyHunt01":       ("Complete Bounty Hunts while a Pet Dog is active.", "100 Caps"),
    "Dog_BountyHunt02":       ("Complete Bounty Hunts while a Pet Dog is active.", ""),
    "Dog_BountyHunt03":       ("Complete Bounty Hunts while a Pet Dog is active.", ""),
    "Radhog_CollectFlux01":   ("Collect Flux while a Pet Radhog is active.", "100 Caps"),
    "Radhog_CollectFlux02":   ("Collect Flux while a Pet Radhog is active.", ""),
    "Radhog_CollectFlux03":   ("Collect Flux while a Pet Radhog is active.", ""),
    "Deathclaw_InfestationKills01": ("Kill Infestation enemies while a Pet Deathclaw is active.", "100 Caps"),
    "Deathclaw_InfestationKills02": ("Kill Infestation enemies while a Pet Deathclaw is active.", ""),
    "Deathclaw_InfestationKills03": ("Kill Infestation enemies while a Pet Deathclaw is active.", ""),
}

SPECIES = {
    "AnyPet":    ("Any Pet",   "Any World Pet"),
    "Cat":       ("Cat",       "Pet Cat"),
    "Dog":       ("Dog",       "Pet Dog"),
    "Radhog":    ("Radhog",    "Pet Radhog"),
    "Deathclaw": ("Deathclaw", "Pet Deathclaw"),
}
GROUP_ORDER = ["Any Pet", "Cat", "Dog", "Radhog", "Deathclaw"]
ROMAN = {1: "I", 2: "II", 3: "III"}
PREFIX = "WorldPets_Challenge_Lifetime_"

def species_key(edid):
    tail = edid[len(PREFIX):]
    return tail.split("_", 1)[0]

def line_base(edid):
    return re.sub(r"\d+$", "", edid)

def tier_num(edid):
    m = re.search(r"(\d+)$", edid)
    return safe_int(m.group(1)) if m else None

# ---------------------------------------------------------------------------
# Build the 20 items
# ---------------------------------------------------------------------------
wp_rows = [r for r in CHAL if pick(r, "EDID").startswith(PREFIX)]

line_counts = {}
for r in wp_rows:
    line_counts[line_base(pick(r, "EDID"))] = line_counts.get(line_base(pick(r, "EDID")), 0) + 1

groups_map = {label: [] for label in GROUP_ORDER}
total = 0
for r in wp_rows:
    edid = pick(r, "EDID")
    tail = edid[len(PREFIX):]
    sp = species_key(edid)
    group_label, active_pet = SPECIES.get(sp, (sp, sp))
    multi = line_counts[line_base(edid)] > 1
    tnum = tier_num(edid)
    tier = tnum if (multi and tnum in (1, 2, 3)) else 0
    type_label = f"Tier {ROMAN[tier]}" if tier else "Lifetime"
    desc, reward = OVERLAY.get(tail, ("", ""))
    # MNAM (Reward Display) was added to the CHAL export in July 2026, after this
    # build was written, so the page was showing "No rewards listed" on 14 of 20
    # rows while the record itself named a real reward — every tier-3 challenge
    # pays a title (Player Title: Veterinarian, C.A.M.P. Title: Cattery, …).
    # Per the challenge-style-guide, MNAM is the record's own reward text and
    # wins; the editorial OVERLAY is the fallback. Guard the corrupt-MNAM case
    # the same way the main build does — reject a string with no letter or digit
    # rather than printing mojibake.
    mnam = pick(r, "MNAM")
    if mnam and re.search(r"[A-Za-z0-9]", mnam):
        reward = mnam
    required = pick(r, "TNAM")
    item = {
        "form_id":   pick(r, "FormID"),
        "edid":      edid,
        "record":    "CHAL",
        "name":      pick(r, "FULL"),
        "group":     group_label,
        "active_pet": active_pet,
        "tier":      tier,
        "type_label": type_label,
        "required":  safe_int(required, required) if required not in ("", "0") else required,
        "counter":   pick(r, "SNAM").lstrip(),
        "desc":      desc,
        "reward":    reward,
        "conditions_display": conditions_display(extract_conditions(r)),
    }
    groups_map.setdefault(group_label, []).append(item)
    total += 1

groups = []
for label in GROUP_ORDER:
    items = sorted(groups_map.get(label, []), key=lambda x: x["edid"])
    if items:
        groups.append({"name": label, "challenges": items})

output = {
    "generated": datetime.datetime.utcnow().isoformat() + "Z",
    "source": "CHAL/CNDF TSV exports (build_world_pet_challenges_json.py)",
    "total": total,
    "groups": groups,
}

out_path = OUT_DIR / "world_pet_challenges.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print(f"[world-pet-challenges] wrote {out_path}  ({total} challenges, {len(groups)} groups)")
for g in groups:
    print(f"    {g['name']}: {len(g['challenges'])}")
