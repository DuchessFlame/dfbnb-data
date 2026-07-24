#!/usr/bin/env python3
"""
src/build_emotes_json.py
========================
Reads the newest tsv/EMOT_Export_*.tsv (the emote records) and, for name
polish, the newest tsv/ENTM_Export_*.tsv (entitlements), and writes
dist/emotes.json.

GENERATIVE. The emote list that drives /df/atom-shop/emotes/ used to be a
hand-maintained EMOTE_DATA / EMOTE_PET_DATA / EMOTE_CATEGORIES set inside
df-bnb-atom-shop.js, parsed by hand from a monthly export. That meant a new
emote in the game files never appeared on the page (live or PTS) until
someone edited the JS by hand — e.g. the May snapshot missed Shimmy Dance,
Made in the Shade and Taking Notes. It is now derived from the newest EMOT
export on every patch/PTS build, so new emotes show up automatically.

PTS: this builder needs NO PTS awareness. dfbnb-pts-build.yml normalizes the
PTS pull into tsv/, wipes dist/, runs the SAME builders, then relocates
dist/ -> dist/pts/. So writing dist/emotes.json here yields dist/pts/emotes.json
on the PTS channel for free.

The EMOT export has one row per EMOT record with columns:
    FormID  EDID  FULL  RENT  SNAM  CNAM  DNAM
  - RENT = the ENTM entitlement EDID (how you obtain / unlock it)
  - SNAM = the animation symbol name inside emoteslibrary.swf (the GIF key)
  - CNAM = 'EmoteCategory_X "Display Name" [ECAT:formId]' (empty for pets/dev)
  - DNAM = the underlying animation record

Output JSON (shape matches what the renderer consumes):
  {
    "_generated": "YYYY-MM-DDTHH:MM:SSZ",
    "_source":    "EMOT_Export_<Mon>_<Year>.tsv",
    "categories": [ {"formId","edid","name"}, ... ],   # sorted by display name
    "catOrder":   ["Angry", ... "Yes", "Pets"],         # A-Z then Pets last
    "emotes":     [ {"formId","edid","name","rent","snam","catEdid","dnam"}, ... ],
    "petEmotes":  [ {"formId","edid","name","rent","snam","dnam"}, ... ]
  }
"""

import os
import re
import csv
import sys
import json
import glob
import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TSV_ROOT = os.path.join(SCRIPT_DIR, "..", "tsv")
DIST = os.path.join(SCRIPT_DIR, "..", "dist", "emotes.json")

# ── Newest-TSV selection (chronological, NOT alphabetical) ───────────
# Same rule as build_atom_shop_json.py: a plain sorted() orders "May" after
# "July", so it would silently read a stale export (and, on PTS, make
# dist/pts byte-identical to live). Sort by parsed (year, month) then mtime.
_MONTH_ORDER = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def _filename_date_key(path):
    base = os.path.basename(path).lower()
    m = re.search(r"_([a-z]+)_(\d{4})", base)
    if m:
        month_num = _MONTH_ORDER.get(m.group(1), 0)
        if month_num:
            return (int(m.group(2)), month_num)
    return (0, 0)


def _newest_tsv(prefix):
    paths = glob.glob(os.path.join(TSV_ROOT, prefix + "_Export_*.tsv"))
    if not paths:
        return None
    paths.sort(key=lambda p: (_filename_date_key(p), os.path.getmtime(p)))
    return paths[-1]


# ── Exclusion rules (mirror the documented inline filter) ────────────
# Excluded: debug/dev (DEBUG_, TEMPLATE_, zzz_), disabled entitlements
# (RENT = UTILITY_ENTM...), internal/promo dupes (Emote_PlayerFear,
# Emote_ImSelling1, *_E3_* promo copies, CAMPPets_Pet_Payer_Pair 1st/3rd person).
_DEV_PREFIXES = ("debug_", "template_", "zzz_")
_INTERNAL_EDIDS = {"Emote_PlayerFear", "Emote_ImSelling1"}


def _is_dev(edid, rent):
    low = edid.lower()
    if low.startswith(_DEV_PREFIXES):
        return True
    if rent.startswith("UTILITY_ENTM"):
        return True
    if edid in _INTERNAL_EDIDS:
        return True
    if "_E3_" in edid:                 # E3 promo duplicate of a shipping emote
        return True
    if "Pet_Payer_Pair" in edid:       # 1st/3rd person petting-a-pet internals
        return True
    return False


def _is_pet(edid):
    return edid.startswith("PETS_Emote_Trick") or "CAMPPets" in edid


# ── CNAM parsing ─────────────────────────────────────────────────────
# 'EmoteCategory_Hello "Hello" [ECAT:00005741]'  ->  (edid, name, formId)
_CNAM_RE = re.compile(r'^\s*(\S+)\s+"([^"]*)"\s*\[ECAT:([0-9A-Fa-f]+)\]')


def parse_cnam(cnam):
    m = _CNAM_RE.match(cnam or "")
    if not m:
        return None
    return {"edid": m.group(1), "name": m.group(2), "formId": m.group(3).upper()}


# ── Display-name resolution ──────────────────────────────────────────
# Prefer EMOT.FULL when it looks like a real display name. Some records carry
# a code-like FULL (e.g. "SaluteRaider", "GrelokYell1"); for those fall back to
# the ENTM entitlement's FULL/NNAM (via RENT), then to a de-camelCased form.
def _looks_codey(s):
    if not s:
        return True
    if " " in s:
        return False
    # single token, no spaces: treat CamelCase / trailing-digit tokens as code
    return bool(re.match(r"^[A-Za-z][A-Za-z0-9']*$", s)) and (
        s[-1].isdigit() or re.search(r"[a-z][A-Z]", s) is not None
    )


def _decamel(s):
    s = re.sub(r"(\d+)$", "", s)                  # drop trailing index digits
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)     # split camelCase
    return s.strip()


def resolve_name(emot_full, rent, entm_by_edid):
    if not _looks_codey(emot_full):
        return emot_full
    ent = entm_by_edid.get(rent)
    if ent:
        cand = (ent.get("FULL") or ent.get("NNAM") or "").strip()
        if cand and not _looks_codey(cand):
            return cand
    return _decamel(emot_full) or emot_full


# ── Pet display names ────────────────────────────────────────────────
# Pet emotes carry no FULL, so their names are editorial. Known ones are
# mapped explicitly; anything new falls back to "<Animal>: <Action>" derived
# from the EDID so a future pet still renders sensibly.
_PET_NAME_OVERRIDES = {
    "PETS_Emote_Trick1": "Pet Trick: Sit!",
    "PETS_Emote_Trick2": "Pet Trick: Speak!",
    "PETS_Emote_Trick3": "Pet Trick: Roll Over!",
    "PETS_Emote_Trick4": "Pet Trick: Play Dead!",
    "ATX_Emote_CAMPPets_Cat_HighFive": "Cat: Wave",
}
_PET_ANIMAL = {"radhogcp": "Rad Hog"}
_PET_ACTION = {"highfive": "Wave"}


def pet_name(edid):
    if edid in _PET_NAME_OVERRIDES:
        return _PET_NAME_OVERRIDES[edid]
    m = re.search(r"CAMPPets_([A-Za-z]+)_([A-Za-z]+)", edid)
    if m:
        animal = _PET_ANIMAL.get(m.group(1).lower(), _decamel(m.group(1)))
        action = _PET_ACTION.get(m.group(2).lower(), _decamel(m.group(2)))
        return f"{animal}: {action}"
    return _decamel(edid)


def main():
    emot_path = _newest_tsv("EMOT")
    if not emot_path:
        print("[emotes] No EMOT_Export_*.tsv found in tsv/", file=sys.stderr)
        sys.exit(1)
    entm_path = _newest_tsv("ENTM")

    entm_by_edid = {}
    if entm_path:
        with open(entm_path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                entm_by_edid[(r.get("EDID") or "").strip()] = r

    emotes, pets = [], []
    categories = {}  # edid -> {formId, edid, name}

    with open(emot_path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            edid = (r.get("EDID") or "").strip()
            if not edid:
                continue
            rent = (r.get("RENT") or "").strip()
            snam = (r.get("SNAM") or "").strip()
            dnam = (r.get("DNAM") or "").strip()
            full = (r.get("FULL") or "").strip()
            formId = (r.get("FormID") or "").strip().upper()

            if _is_pet(edid):
                if "Pet_Payer_Pair" in edid:      # internal, not a real pet emote
                    continue
                pets.append({
                    "formId": formId, "edid": edid, "name": pet_name(edid),
                    "rent": rent, "snam": snam, "dnam": dnam,
                })
                continue

            cat = parse_cnam(r.get("CNAM"))
            if not cat:
                continue                          # uncategorised = dev/internal
            if _is_dev(edid, rent):
                continue

            categories.setdefault(cat["edid"], {
                "formId": cat["formId"], "edid": cat["edid"], "name": cat["name"],
            })
            emotes.append({
                "formId": formId, "edid": edid,
                "name": resolve_name(full, rent, entm_by_edid),
                "rent": rent, "snam": snam,
                "catEdid": cat["edid"], "dnam": dnam,
            })

    # Categories sorted A-Z by display name; catOrder appends "Pets" last.
    cat_list = sorted(categories.values(), key=lambda c: c["name"].lower())
    cat_order = [c["name"] for c in cat_list] + (["Pets"] if pets else [])

    # Stable output order: emotes by category order then name; pets by name.
    cat_index = {c["edid"]: i for i, c in enumerate(cat_list)}
    emotes.sort(key=lambda e: (cat_index.get(e["catEdid"], 999), e["name"].lower()))
    pets.sort(key=lambda e: e["name"].lower())

    out = {
        "_generated": datetime.datetime.now(datetime.timezone.utc)
                              .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source": os.path.basename(emot_path),
        "categories": cat_list,
        "catOrder": cat_order,
        "emotes": emotes,
        "petEmotes": pets,
    }

    os.makedirs(os.path.dirname(DIST), exist_ok=True)
    with open(DIST, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)

    print(f"[emotes] {os.path.basename(emot_path)} -> dist/emotes.json | "
          f"{len(emotes)} emotes, {len(pets)} pets, {len(cat_list)} categories")


if __name__ == "__main__":
    main()
