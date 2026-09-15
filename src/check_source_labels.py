#!/usr/bin/env python3
r"""
check_source_labels.py — lint every route name the plan pages could publish,
without building anything.

WHY
---
A full plan_master build is hours, and the only way a bad route NAME used to be
found was to pay for the build and read the output. That is a terrible feedback
loop for a naming change: fix one, rebuild, discover the next.

Naming is pure — it reads an EditorID and returns a string, with no drop rates
and no rng76 anywhere near it. So every name the site could ever print can be
generated in a few seconds by running plan_sources.source_label() over every
leveled list, container and NPC in the export. That is what this does.

It caught 227 real leaks on its first run that a 600-plan sample had missed:
`LLI` appearing mid-name rather than as a prefix (MTR_LLI_Motherlode ->
"MTR LLI Motherlode"), plus NONPLAYABLE, Placeholder and Babylon riding along
into published labels.

WHAT IT FLAGS
-------------
Anything that reaches a label and still looks like editor wiring — dev markers,
engine flags, unstripped list prefixes, or an unresolved <Alias=...> token from
a quest name. A record that source_label() rejects outright (returns None) is
not a leak: the builder drops those routes entirely.

Run it after dropping new TSVs and BEFORE starting a build:

    python3 src/check_source_labels.py                 # live
    python3 src/check_source_labels.py --data-dir tsv/pts
    python3 src/check_source_labels.py --list          # print every label

Exits non-zero when it finds a leak, so CI can gate on it.
"""
import argparse, csv, os, re, sys, collections

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import plan_sources
import tsv_source

# Where a route name can come from: the leveled list itself, and the container
# or NPC that holds it (the vendor and creature naming paths).
SOURCES = (
    ("LVLI_Export_*_LVLI_List.tsv", "LVLI_EDID"),
    ("CONT_Export_*.tsv",           "EDID"),
    ("NPC_Export_*.tsv",            "EDID"),
)

# A published label containing any of this is wiring that escaped the filters.
LEAK = re.compile(
    r"(?i)(^|\W)(cut|zzz+|deleted|deprecated|debug|babylon|placeholder|nonplayable)(\W|$)"
    r"|<|\[|NEED A NAME"                   # unresolved alias / placeholder title
    r"|vendor vendor"                       # doubled by the vendor shaper
    r"|\b(LL|LLI|LLS|LLV|LLD|LLE|LLQ|LPI)\b"
    r"|RESTRICTED")


# Quest titles are written by humans, so they are linted for editor scaffolding
# rather than for wiring tokens. See plan_sources.usable_quest_name().
TITLE_LEAK = re.compile(
    r"(?<![A-Za-z])(deprecated|debug|zzz+|deleted|placeholder|nonplayable)(?![A-Za-z])"
    r"|<|\[", re.I)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.path.join(os.path.dirname(HERE), "tsv"))
    ap.add_argument("--list", action="store_true", help="print every distinct label")
    args = ap.parse_args(argv)

    qf = tsv_source.newest(os.path.join(args.data_dir, "QUEST_Export_*.tsv"), required=False)
    quests = plan_sources.QuestNames(qf) if qf else plan_sources.QuestNames()
    print(f"[labels] quest names from {os.path.basename(qf) if qf else '(none)'}")

    records = []
    for pat, col in SOURCES:
        path = tsv_source.newest(os.path.join(args.data_dir, pat), required=False)
        if not path:
            print(f"[labels] WARNING: no match for {pat}")
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                e = (row.get(col) or "").strip()
                if e:
                    records.append(e)

    labels, leaks, dropped = collections.Counter(), collections.Counter(), 0
    for edid in records:
        if plan_sources.is_dev_record(edid):
            dropped += 1
            continue
        label = plan_sources.source_label(edid, quests)
        if label is None:
            dropped += 1
            continue
        labels[label] += 1
        if LEAK.search(label):
            leaks[label] += 1

    print(f"[labels] {len(records)} records | {len(labels)} distinct labels "
          f"| {dropped} dropped as editor-only")

    if args.list:
        for l in sorted(labels):
            print(f"    {labels[l]:5}  {l}")

    # The unlock sentences are a second way text reaches the page, and they get
    # their titles from a different place — the QUEST export's FULL column, GMRW
    # ParentQuestDisplay, and the QuestName column of BOOK_*_Locations. Linting
    # only route labels missed two of these on live pages, so every quest title
    # that could become a sentence is checked here too.
    titles = 0
    for pat, col in (("QUEST_Export_*.tsv", "FULL - Name"),
                     ("GMRW_Export_*.tsv", "ParentQuestDisplay"),
                     ("BOOK_Export_*_Locations.tsv", "QuestName")):
        path = tsv_source.newest(os.path.join(args.data_dir, pat), required=False)
        if not path:
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                full = (row.get(col) or "").strip()
                if not full:
                    continue
                name = plan_sources.usable_quest_name(full)
                if not name:
                    continue                       # rejected, never printed
                titles += 1
                # A human title gets a human test, not LEAK — that pattern is
                # tuned for EditorID-derived labels and would reject the real
                # quest "Making the Cut" for containing the word cut.
                if TITLE_LEAK.search(name):
                    leaks[f"(quest title) {name}"] += 1
    print(f"[labels] {titles} quest titles reach the unlock sentences")

    if leaks:
        print(f"[labels] FAIL — {sum(leaks.values())} leaked name(s):")
        for l, n in leaks.most_common():
            print(f"    {n:5}  {l}")
        return 1

    print("[labels] PASS — no editor wiring reached a published name")
    return 0


if __name__ == "__main__":
    sys.exit(main())
