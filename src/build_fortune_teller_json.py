#!/usr/bin/env python3
"""
build_fortune_teller_json.py
============================
Generates dist/fortune_teller.json — the data feed for the DF/BNB
"Lady G Fortune Teller — Luck Buff" guide page (Nuka-World on Tour, Patch 14).

Consumed by the JS module df-bnb-fortune-teller.js on the website. Everything
the page prints (caps cost, magnitude, duration, cooldown, the 44 Major-Arcana
fortune blurbs, the Nuka-flavour pool) is read from the newest xEdit TSV exports
so the page auto-updates whenever fresh game data lands.

Sources (newest matching file wins, dated exports sort last):
  BOOK  export  -> the 44 fortune notes (card name, orientation, tarot blurb)
  GLOB  export  -> caps cost, magnitude (major/minor), duration, cooldown
  SPEL  EFFECTS -> the live Major spells + their baked (entry) duration
  FLST  Entries -> the "Possible Nuka Flavors" pool (0066D172)
  ALCH  export  -> resolves the flavour form IDs to display names

Design decision (per site owner): the page shows the DURATION from the config
global NWOT_Fortune_Effect_Duration (1800s / 30 min), NOT the baked 30s magic-
effect entry. The baked value is still emitted under `spells.baked_*` so the
page can show it as a technical footnote.

Usage:
  python build_fortune_teller_json.py
  python build_fortune_teller_json.py --data-dir tsv/pts --out dist/fortune_teller.json
"""

import argparse
import csv
import datetime
import json
import os
import re
import sys

# ---- form IDs we care about (stable across exports) -------------------------
GLOB_IDS = {
    "0066D177": "caps_cost",
    "0066D175": "mag_major",
    "0066D174": "mag_minor",
    "0066D176": "duration_seconds",
    "0066D173": "cooldown_seconds",
}
SPELL_POSITIVE_MAJOR = "0066D18A"   # NWOT_FortuneSpell_PositiveMajor -> FortifyLuck
SPELL_NEGATIVE_MAJOR = "0066D189"   # NWOT_FortuneSpell_NegativeMajor -> ReduceLuck
FLAVOR_FLST_ID = "0066D172"         # NWOT_FortuneTeller_PossibleNukaFlavors
FORTUNE_BOT_FORMID = "0066D17B"     # NWOT_FortuneTeller_Bot
DIALOGUE_QUEST_FORMID = "0066933A"  # NWOT_FortuneTeller_Dialogue

TAG_RE = re.compile(r"<[^>]+>")
FLAVOR_TAIL_RE = re.compile(r"\s*Your Lucky Nuka flavor is:.*$", re.IGNORECASE)
TITLE_RE = re.compile(r"^Fortune:\s*(.*?)\s*-\s*(Upright|Reversed)\s*$", re.IGNORECASE)


def find_tsv(directory, *keywords, exclude=None):
    """Newest TSV whose name contains ALL keywords (and none of `exclude`)."""
    exclude = [e.lower() for e in (exclude or [])]
    kws = [k.lower() for k in keywords]
    for fname in sorted(os.listdir(directory), reverse=True):
        low = fname.lower()
        if not low.endswith(".tsv"):
            continue
        if any(k not in low for k in kws):
            continue
        if any(e in low for e in exclude):
            continue
        return os.path.join(directory, fname)
    raise FileNotFoundError(f'No TSV matching {keywords} in {directory}')


def read_rows(path):
    with open(path, encoding="latin-1") as f:
        for row in csv.reader(f, delimiter="\t"):
            yield row


def clean_blurb(desc):
    txt = TAG_RE.sub("", desc or "")
    txt = FLAVOR_TAIL_RE.sub("", txt)
    return re.sub(r"\s+", " ", txt).strip()


def build_cards(book_path):
    """Return the 44 live Major-Arcana fortunes, split into upright/reversed pools."""
    cards = []
    for row in read_rows(book_path):
        if len(row) < 4:
            continue
        formid, edid, full, desc = row[0].strip(), row[1].strip(), row[2].strip(), row[3]
        if not edid.startswith("NWOT_FortuneMajor_"):
            continue  # skips zzz_ Minor / Neutral / Test and everything else
        m = TITLE_RE.match(full)
        displayed = (m.group(2).capitalize() if m else "")
        card_name = (m.group(1).strip() if m else full)
        polarity = "Positive" if edid.endswith("_Positive") else "Negative"
        effect = "Lucky Fortune" if polarity == "Positive" else "Un-Lucky Fortune"
        luck = 2 if polarity == "Positive" else -2
        expected_orient = "Upright" if polarity == "Positive" else "Reversed"
        cards.append({
            "card": card_name,
            "formid": formid,
            "edid": edid,
            "polarity": polarity,             # mechanical pool (drives the buff)
            "displayedOrientation": displayed, # what the note title reads in-game
            "effect": effect,
            "luck": luck,
            "labelSwapped": bool(displayed) and displayed != expected_orient,
            "blurb": clean_blurb(desc),
        })
    # stable, human order: card name then orientation
    cards.sort(key=lambda c: (c["card"].lower(), c["polarity"] != "Positive"))
    return cards


def build_globals(glob_path):
    out = {}
    for row in read_rows(glob_path):
        if len(row) < 3:
            continue
        fid = row[0].strip().upper()
        if fid in GLOB_IDS:
            try:
                val = float(row[2])
            except ValueError:
                continue
            out[GLOB_IDS[fid]] = int(val) if val.is_integer() else val
    return out


def build_spells(spel_effects_path):
    """Live Major spells + their baked entry duration (col EFIT_Duration = index 13)."""
    info = {"positive_major": {}, "negative_major": {}}
    for row in read_rows(spel_effects_path):
        if len(row) < 14:
            continue
        fid = row[0].strip().upper()
        target = None
        if fid == SPELL_POSITIVE_MAJOR.upper():
            target = "positive_major"
        elif fid == SPELL_NEGATIVE_MAJOR.upper():
            target = "negative_major"
        if not target:
            continue
        mgef_edid = row[5].strip()
        # only capture the Luck effect index (skip the Cooldown sub-effect)
        if "Luck" not in mgef_edid:
            continue

        def num(s):
            s = (s or "").strip()
            try:
                return int(float(s))
            except ValueError:
                return None

        info[target] = {
            "spellFormId": row[0].strip(),
            "spellEdid": row[1].strip(),
            "spellName": row[2].strip(),
            "mgefEdid": mgef_edid,
            "mgefName": row[6].strip(),
            "bakedMagnitude": num(row[11]),
            "bakedDuration": num(row[13]),
        }
    return info


def build_flavors(flst_entries_path, alch_path):
    flavor_ids = []
    for row in read_rows(flst_entries_path):
        if len(row) < 6:
            continue
        if row[0].strip().upper() == FLAVOR_FLST_ID.upper():
            flavor_ids.append(row[5].strip().upper())
    names = {}
    for row in read_rows(alch_path):
        if len(row) < 3:
            continue
        fid = row[0].strip().upper()
        if fid in flavor_ids:
            names[fid] = row[2].strip()
    return [names[i] for i in flavor_ids if i in names]


def human_minutes(seconds):
    if seconds is None:
        return None
    if seconds % 3600 == 0:
        h = seconds // 3600
        return f"{h} hour" + ("s" if h != 1 else "")
    if seconds % 60 == 0:
        m = seconds // 60
        return f"{m} minute" + ("s" if m != 1 else "")
    return f"{seconds} seconds"


def main():
    ap = argparse.ArgumentParser(description="Build dist/fortune_teller.json")
    ap.add_argument("--data-dir", default="tsv/pts", help="Folder with the xEdit TSV exports")
    ap.add_argument("--out", default="dist/fortune_teller.json", help="Output JSON path")
    args = ap.parse_args()

    d = args.data_dir
    book_path = find_tsv(d, "BOOK_Export", exclude=["locations"])
    glob_path = find_tsv(d, "GLOB_Export")
    spel_eff_path = find_tsv(d, "SPEL_Export", "EFFECTS")
    flst_ent_path = find_tsv(d, "FLST_Export", "Entries")
    alch_path = find_tsv(d, "ALCH_Export", exclude=["effects"])

    print(f"BOOK: {book_path}")
    print(f"GLOB: {glob_path}")
    print(f"SPEL: {spel_eff_path}")
    print(f"FLST: {flst_ent_path}")
    print(f"ALCH: {alch_path}")

    cards = build_cards(book_path)
    globs = build_globals(glob_path)
    spells = build_spells(spel_eff_path)
    flavors = build_flavors(flst_ent_path, alch_path)

    upright = [c for c in cards if c["polarity"] == "Positive"]
    reversed_ = [c for c in cards if c["polarity"] == "Negative"]

    duration_s = globs.get("duration_seconds")
    cooldown_s = globs.get("cooldown_seconds")

    data = {
        "version": "1.0.0",
        "generated": datetime.date.today().isoformat(),
        "source": os.path.basename(book_path),
        "npc": {
            "name": "Lady G the Fortune Teller",
            "formId": FORTUNE_BOT_FORMID,
            "edid": "NWOT_FortuneTeller_Bot",
            "dialogueQuestFormId": DIALOGUE_QUEST_FORMID,
        },
        "config": {
            "capsCost": globs.get("caps_cost"),
            "magnitudeMajor": globs.get("mag_major"),
            "magnitudeMinor": globs.get("mag_minor"),
            "durationSeconds": duration_s,
            "durationHuman": human_minutes(duration_s),
            "cooldownSeconds": cooldown_s,
            "cooldownHuman": human_minutes(cooldown_s),
        },
        "spells": {
            "positiveMajor": spells.get("positive_major", {}),
            "negativeMajor": spells.get("negative_major", {}),
            # baked entry duration on the magic effect — differs from the config
            # global above; surfaced so the page can footnote the discrepancy.
            "bakedDurationSeconds": (spells.get("positive_major", {}) or {}).get("bakedDuration"),
        },
        "flavors": flavors,
        "counts": {
            "totalFortunes": len(cards),
            "upright": len(upright),
            "reversed": len(reversed_),
            "flavors": len(flavors),
        },
        "fortunes": {
            "upright": upright,
            "reversed": reversed_,
        },
    }

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)

    size_kb = os.path.getsize(args.out) / 1024
    swapped = [c["card"] for c in cards if c["labelSwapped"]]
    print(f"\nSaved -> {args.out}  ({size_kb:.1f} KB)")
    print(f"  fortunes: {len(cards)}  (upright {len(upright)} / reversed {len(reversed_)})")
    print(f"  flavours: {len(flavors)} -> {', '.join(flavors)}")
    print(f"  config: cost={data['config']['capsCost']}  mag=+/-{data['config']['magnitudeMajor']}"
          f"  duration={data['config']['durationHuman']}  cooldown={data['config']['cooldownHuman']}")
    print(f"  baked spell duration (footnote): {data['spells']['bakedDurationSeconds']}s")
    if swapped:
        print(f"  label-swapped cards flagged: {', '.join(swapped)}")
    if len(cards) != 44:
        print(f"  WARNING: expected 44 live Major fortunes, got {len(cards)}", file=sys.stderr)


if __name__ == "__main__":
    main()
