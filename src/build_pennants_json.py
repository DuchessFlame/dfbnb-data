from __future__ import annotations

"""
build_pennants_json.py
======================
Generates dist/pennants.json for the DF Plan Checklists -> Pennants page
(/df/plan-checklists/pennants/).

Pennants are CAMP wall-decor items. Almost all of them are granted by an
entitlement (ENTM) and placed in-world via the leveled list
ATX_workshop_LL_WallDecor_Pennant, where every STAT entry is gated by a
"Subject.HasEntitlement(...)" condition. One entitlement usually grants BOTH
a framed and an unframed pennant (sometimes two designs => 4 STATs).

Pipeline (all from the xEdit TSV exports in tsv/):
  LVLI_Export_*_LVLI_Entries.tsv  -> STAT entries + their HasEntitlement condition
  ENTM_Export_*.tsv               -> entitlement display name / description / store flag
  BOOK_Export_*.tsv               -> the one craftable plan (Vault-Tec University Pennant)
  COBJ_Export_*.tsv               -> craft recipe for the VTU pennant

Output:
  dist/pennants.json -> {
    "version": "YYYY-MM-DD",
    "generated": "<ISO-8601 UTC>",
    "count": N,
    "pennants": [ { id, name, source, obtain, desc, condition,
                    entitlement:{formid,edid}, items:[{label,kind,formid,edid,texture}],
                    cut } ]
  }

Usage:
  python build_pennants_json.py
  python build_pennants_json.py --data-dir /path/to/tsvs --outdir /path/to/dist
"""

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import sys

LL_EDID = "ATX_workshop_LL_WallDecor_Pennant"

# The Vault-Tec University pennant is a Nuclear Winter / atom-shop reward, NOT a
# Public Test Server (PTS) pennant — so it is excluded from this PTS checklist.
INCLUDE_VTU = False

# Texture root inside the extracted game files (fo76-tools/textures/...).
TEX_ROOT = "textures/atx/setdressing/"

# Map an entitlement EDID suffix -> a friendly theme name. Anything not listed
# falls back to a theme parsed from the ENTM description, then to the EDID.
THEME_BY_ENTM = {
    "Babylon_ENTM_CAMP_Decoration_VTU_Pennant": "Vault-Tec University",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS": "Public Test Server",
    "ATX_ENTM_CAMP_WallDeco_Pennant_Patch24PTS": "Public Test Server (Vault Girl)",
    "ATX_ENTM_CAMP_WallDeco_Pennant_Patch26PTS": "Public Test Server (Patch 26)",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_BoS": "Brotherhood of Steel – Rahmani & Shin",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_Worlds": "Fallout Worlds",
    "ATX_ENTM_CAMP_WallDeco_Pennant_NuclearWinter": "Nuclear Winter",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_Mothman": "Mothman Equinox",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_Invaders": "Invaders from Beyond",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_TestYourMetal": "Test Your Metal",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_PITT": "Expedition: The Pitt",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_NWOT": "Nuka-World on Tour",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P42": "Mutated Events",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P44": "Blue Moon",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P46": "Americana",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P48": "Nuka-World on Tour (P48)",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P50": "Public Test Server (Patch 50)",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P52": "Skyline Valley",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P54": "Burning Springs",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P56": "Skyline Valley (P56)",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P58": "Public Test Server (Patch 58)",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P58_Copy01": "Player Ghoul",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P60": "Fishing",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P62": "Rebuilding the Heartland",
    "MILE_ENTM_CAMP_WallDeco_Pennant_Caravans": "Caravans",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P64": "Public Test Server (Patch 64)",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P66": "Rediscovering Appalachia",
    "ATX_ENTM_CAMP_WallDeco_Pennant_PTS_P68": "Rediscovering Appalachia (Pet)",
}

# Best-effort STAT EDID token -> diffuse texture filename (relative to TEX_ROOT).
# Tokens are checked in order; first match wins. Unmatched STATs get "".
TEX_BY_TOKEN = [
    ("Mothman", "atx_pennant_mothman/atx_pennant_mothman_d.dds"),
    ("Invaders", "atx_pennant_invaders/atx_pennant_invaders_d.dds"),
    ("Worlds", "atx_pennant_worlds/atx_pennant_worlds_d.dds"),
    ("NWOT", "atx_pts_pennant/atx_pts_pennant_nwot_d.dds"),
    ("NW_", "atx_pennant_nw/atx_pennant_nw_d.dds"),
    ("Rahmani", "atx_pts_pennant/atx_pts_pennant_rahmani_d.dds"),
    ("Shin", "atx_pts_pennant/atx_pts_pennant_shin_d.dds"),
    ("TestMetal", "atx_pts_pennant/atx_pts_pennant_testmetal_d.dds"),
    ("ThePitt", "atx_pts_pennant/atx_pts_pennant_thepitt_d.dds"),
    ("Caravans", "atx_caravansshutdown_pennant/p64_caravansshutdown_pennant_d.dds"),
    ("P44", "atx_pts_pennant/atx_pts_pennant_bluemoon.dds"),
    ("P46", "atx_pts_pennant/atx_pts_pennant_p46_d.dds"),
    ("P54", "atx_pts_pennant/atx_pts_pennant_p54_d.dds"),
    ("P56", "atx_pts_pennant/atx_pts_pennant_p56_d.dds"),
    ("P58", "atx_pts_pennant/atx_pts_pennant_p58_d.dds"),
    ("P60", "atx_pts_pennant/atx_pts_pennant_p60_d.dds"),
    ("P62", "atx_pts_pennant/atx_pts_pennant_p62_d.dds"),
    ("P64", "atx_pts_pennant/atx_pts_pennant_p64_d.dds"),
    ("P66", "atx_pts_pennant/atx_pts_pennant_p66_d.dds"),
    ("P42", "atx_pts_pennant/atx_pts_pennant_mutatedevents_d.dds"),
    ("Pennant_03", "atx_pts_pennant/atx_pts_pennant_03_d.dds"),
    ("Pennant_02", "atx_pts_pennant/atx_pts_pennant_02_d.dds"),
    ("Pennant_01", "atx_pts_pennant/atx_pts_pennant_d.dds"),
]

# Source classification + a default obtain blurb per source.
# Human-readable unlock hint shown when there is no richer CNDF breakdown
# (pennant STATs are only gated by a HasEntitlement check).
def unlock_hint(source: str) -> str:
    return {
        "PTS": "Log into the Public Test Server (PTS) while this pennant's test cycle is active to unlock it. Once granted it carries over to your live account.",
        "Nuclear Winter": "Earned for taking part in Nuclear Winter. With that mode retired it can no longer be obtained.",
        "Milestone": "Granted automatically as a milestone / login reward.",
        "Atom Shop": "Purchased from the Atom Shop with Atoms.",
    }.get(source, "Unlock requirements are unknown.")


def classify_source(entm_edid: str, flag: str) -> tuple[str, str]:
    e = entm_edid
    if e.startswith("Babylon_"):
        # Vault-Tec University pennant - craftable plan (see VTU handling below).
        return ("Nuclear Winter", "Nuclear Winter reward. Craftable once the plan is learned.")
    if "NuclearWinter" in e:
        return ("Nuclear Winter", "Reward for participating in Nuclear Winter (ZAX's experiment).")
    if e.startswith("MILE_"):
        return ("Milestone", "Milestone / login reward.")
    if "PTS" in e or "Patch" in e:
        return ("PTS", "Reward for helping test on the Fallout 76 Public Test Server (PTS).")
    if flag.strip().lower() == "premium":
        return ("Atom Shop", "Available from the Atom Shop (Atoms).")
    return ("Other", "")


def latest(data_dir: str, pattern: str) -> str | None:
    # Newest by modification time (xEdit exports keep older months around).
    files = glob.glob(os.path.join(data_dir, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def read_tsv(path: str) -> tuple[list[str], list[list[str]]]:
    raw = open(path, "rb").read()
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("latin-1", errors="replace")
    rows = list(csv.reader(text.splitlines(), delimiter="\t", quotechar='"'))
    if not rows:
        return [], []
    return rows[0], rows[1:]


def col(header: list[str], name: str) -> int:
    return header.index(name) if name in header else -1


ENTM_RE = re.compile(r"\[ENTM:([0-9A-Fa-f]{8})\]")


def theme_from_desc(desc: str) -> str:
    d = re.sub(r"\s*-\s*C\.A\.M\.P\..*$", "", desc or "").strip()
    m = re.search(r"to (?:celebrate|commemorate) (?:the )?(.+?)\.?$", d)
    if m:
        return m.group(1).strip()
    return ""


def tex_for(stat_edid: str) -> str:
    for token, path in TEX_BY_TOKEN:
        if token in stat_edid:
            return TEX_ROOT + path
    return ""


def kind_for(stat_edid: str) -> tuple[str, str]:
    low = stat_edid.lower()
    base = stat_edid
    if "noframe" in low:
        kind = "unframed"
    elif "framed" in low or low.endswith("_frame"):
        kind = "framed"
    else:
        kind = "variant"
    # Build a human label: design hint (Rahmani/Shin/VaultGirl) + frame state.
    design = ""
    for d in ("Rahmani", "Shin", "VaultGirl"):
        if d in stat_edid:
            design = re.sub(r"(?<!^)(?=[A-Z])", " ", d).strip()
    label = {
        "framed": "Framed",
        "unframed": "Unframed",
        "variant": "Variant",
    }[kind]
    if kind == "variant":
        m = re.search(r"_(\d+)(?:_|$)", stat_edid)
        if m:
            label = f"Design {int(m.group(1))}"
    if design:
        label = f"{design} – {label}"
    return kind, label


def main() -> int:
    ap = argparse.ArgumentParser()
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(here)
    ap.add_argument("--data-dir", default=os.path.join(repo, "tsv"))
    ap.add_argument("--outdir", default=os.path.join(repo, "dist"))
    args = ap.parse_args()

    lvli_path = latest(args.data_dir, "LVLI_Export_*_LVLI_Entries.tsv")
    entm_path = latest(args.data_dir, "ENTM_Export_*.tsv")
    if not lvli_path or not entm_path:
        print(f"[pennants] Missing TSVs. LVLI={lvli_path} ENTM={entm_path}", file=sys.stderr)
        return 1

    # --- ENTM lookup ------------------------------------------------------
    eh, erows = read_tsv(entm_path)
    c_fid, c_edid, c_full, c_desc, c_flag = (
        col(eh, "FormID"), col(eh, "EDID"), col(eh, "FULL"),
        col(eh, "DESC"), col(eh, "XALG_Flags"),
    )
    entm = {}
    for r in erows:
        if c_fid >= len(r):
            continue
        fid = r[c_fid].strip().upper()
        if not fid:
            continue
        entm[fid] = {
            "edid": r[c_edid] if c_edid < len(r) else "",
            "full": r[c_full] if c_full < len(r) else "",
            "desc": r[c_desc] if c_desc < len(r) else "",
            "flag": r[c_flag] if c_flag < len(r) else "",
        }

    # --- LVLI pennant entries --------------------------------------------
    lh, lrows = read_tsv(lvli_path)
    c_ledid, c_ref, c_cond1 = col(lh, "LVLI_EDID"), col(lh, "LVLO_Reference"), col(lh, "Cond1")

    # entitlement formid -> list of stat dicts (preserve LL order)
    groups: dict[str, dict] = {}
    order: list[str] = []
    for r in lrows:
        if c_ledid >= len(r) or r[c_ledid] != LL_EDID:
            continue
        ref = r[c_ref] if c_ref < len(r) else ""
        m_ref = re.match(r"([0-9A-Fa-f]{8}):([^:]+):STAT", ref)
        if not m_ref:
            continue
        stat_fid, stat_edid = m_ref.group(1).upper(), m_ref.group(2)
        cond = r[c_cond1] if c_cond1 < len(r) else ""
        m_ent = ENTM_RE.search(cond)
        ent_fid = m_ent.group(1).upper() if m_ent else ""
        kind, label = kind_for(stat_edid)
        if ent_fid not in groups:
            groups[ent_fid] = {"items": []}
            order.append(ent_fid)
        groups[ent_fid]["items"].append({
            "label": label,
            "kind": kind,
            "formid": stat_fid,
            "edid": stat_edid,
            "texture": tex_for(stat_edid),
        })

    pennants = []
    for ent_fid in order:
        meta = entm.get(ent_fid, {})
        edid = meta.get("edid", "")
        flag = meta.get("flag", "")
        desc = re.sub(r"\s+", " ", meta.get("desc", "")).strip()
        # Skip debug/duplicate ENTM rows (zzzzz_*) - keep the clean one.
        if edid.startswith("zzzzz"):
            continue
        source, obtain = classify_source(edid, flag)
        theme = THEME_BY_ENTM.get(edid) or theme_from_desc(desc) or edid
        # Friendly display name
        if theme.lower().endswith("pennant"):
            name = theme
        elif source == "PTS" and theme.startswith("Public Test Server"):
            name = f"{theme} Pennant"
        else:
            name = f"{theme} Pennant"
        items = groups[ent_fid]["items"]
        cond_txt = (
            f"Requires entitlement: {edid} [ENTM:{ent_fid}]"
            if edid else f"Requires entitlement [ENTM:{ent_fid}]"
        )
        pennants.append({
            "id": "PENNANT_" + (edid or ent_fid).replace("ATX_ENTM_CAMP_WallDeco_Pennant_", "").replace("ATX_ENTM_CAMP_Decoration_", "").upper(),
            "name": name,
            "source": source,
            "obtain": obtain,
            "unlock_hint": unlock_hint(source),
            "desc": desc,
            "condition": cond_txt,
            "entitlement": {"formid": ent_fid, "edid": edid},
            "items": items,
            "cut": False,
        })

    # --- Vault-Tec University pennant (craftable plan, no LL entry) -------
    _bk = [f for f in sorted(glob.glob(os.path.join(args.data_dir, "BOOK_Export_*.tsv"))) if "Locations" not in f]
    book_path = _bk[-1] if _bk else None
    vtu_added = any(p["entitlement"]["edid"].startswith("Babylon_") for p in pennants)
    if INCLUDE_VTU and book_path and not vtu_added:
        bh, brows = read_tsv(book_path)
        b_fid, b_edid, b_full = col(bh, "FormID"), col(bh, "EDID"), col(bh, "FULL")
        for r in brows:
            if b_full < len(r) and "Pennant" in (r[b_full] or "") and "Vault-Tec" in (r[b_full] or ""):
                plan_fid = r[b_fid].strip().upper()
                pennants.insert(0, {
                    "id": "PENNANT_VAULT_TEC_UNIVERSITY",
                    "name": "Vault-Tec University Pennant",
                    "source": "Nuclear Winter",
                    "obtain": "Nuclear Winter reward. Learn the plan, then craft at a workshop bench (2x Wood).",
                    "desc": "Put your school spirit on display with this Vault-Tec University flag!",
                    "condition": f"Learn plan: {r[b_edid]} [BOOK:{plan_fid}]",
                    "entitlement": {"formid": "00569B78", "edid": "Babylon_ENTM_CAMP_Decoration_VTU_Pennant"},
                    "items": [{
                        "label": "Pennant",
                        "kind": "single",
                        "formid": "00569B81",
                        "edid": "Babylon_VaultTecUniversityPennant",
                        "texture": "",
                    }],
                    "plan": {"formid": plan_fid, "edid": r[b_edid]},
                    "cut": False,
                })
                break

    out = {
        "version": dt.date.today().isoformat(),
        "generated": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(pennants),
        "source_files": {
            "lvli": os.path.basename(lvli_path),
            "entm": os.path.basename(entm_path),
            "book": os.path.basename(book_path) if book_path else "",
        },
        "pennants": pennants,
    }

    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, "pennants.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"[pennants] Wrote {out_path}  ({len(pennants)} pennants)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
