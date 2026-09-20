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

    # Challenge names are the THIRD way text reaches a page, and they were the
    # next leak: every ChallengeReward GMRW whose challenge could not be named
    # from CHAL was humanised into its own record path — "Challenge Lifetime
    # Burning Springs Bounty Complete Grunt Hunts" shipped on 18 live plans
    # while the route-label sweep passed clean, because a sentence is not a
    # label. Both halves are checked here: the titles CHAL publishes, and every
    # reward record's fallback.
    chal_titles = chal_rejected = 0
    chal_path = tsv_source.newest(
        os.path.join(args.data_dir, "CHAL_Export_*.tsv"), required=False)
    if chal_path:
        with open(chal_path, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                name = plan_sources.usable_challenge_name(
                    plan_sources.usable_quest_name((row.get("FULL") or "").strip()))
                if not name:
                    continue
                chal_titles += 1
                if TITLE_LEAK.search(name):
                    leaks[f"(challenge title) {name}"] += 1

    gmrw_path = tsv_source.newest(
        os.path.join(args.data_dir, "GMRW_Export_*.tsv"), required=False)
    if gmrw_path:
        idx = plan_sources.UnlockIndex(
            args.data_dir, lambda pat, root: tsv_source.newest(
                os.path.join(root, pat), required=False))
        with open(gmrw_path, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                edid = (row.get("EDID") or "").strip()
                if not plan_sources._RX_CHAL_REWARD.search(edid):
                    continue
                stem = plan_sources._RX_CHAL_REWARD.sub("", edid)
                title = idx._chal_title(stem)
                if title:
                    if TITLE_LEAK.search(title):
                        leaks[f"(challenge reward) {title}"] += 1
                    continue
                raw = plan_sources.source_label(stem, idx.quest_names) or ""
                if plan_sources.usable_challenge_name(raw):
                    # It will be printed, so it has to survive the same test.
                    if LEAK.search(raw):
                        leaks[f"(challenge reward) {raw}"] += 1
                else:
                    chal_rejected += 1
    print(f"[labels] {chal_titles} challenge titles reach the unlock sentences "
          f"({chal_rejected} reward record(s) fall back to the generic sentence)")

    # Scrap-to-learn sentences are the FOURTH way text reaches a page: the item
    # you scrap is a WEAP/ARMO FULL name, and it is printed verbatim in "Scrap a
    # <item> to learn this plan." Those are human game names, so they get the
    # human TITLE_LEAK test, and the sentence is checked too in case the item
    # name ever comes through empty or wired.
    import plan_unlocks
    scrap_sentences = 0
    try:
        ru = plan_unlocks.RecipeUnlocks(
            args.data_dir, lambda pat, root: tsv_source.newest(
                os.path.join(root, pat), required=False))
        for u in ru.by_cobj.values():
            if u.get("kind") != plan_unlocks.SCRAP:
                continue
            sent = ru.sentence(u)
            if not sent:
                continue
            scrap_sentences += 1
            item = (u.get("item") or "").strip()
            if not item or TITLE_LEAK.search(item) or LEAK.search(sent):
                leaks[f"(scrap to learn) {sent}"] += 1
    except Exception as exc:                       # noqa: BLE001 — never fatal
        print(f"[labels] WARNING scrap sentence check skipped: {exc}")
    print(f"[labels] {scrap_sentences} scrap-to-learn sentence(s) checked")

    # Content-type parentheticals ("... Out of the Shadows (Infestations)") are a
    # FIFTH way text reaches a page. The type is derived generatively from the
    # quest EDID (plan_sources.QUEST_EDID_TYPE), so a typo there would silently
    # stop annotating with no other symptom. Two checks: every type the
    # derivation produces over the whole QUEST export must be one of the declared
    # plan_sources.CONTENT_TYPES (a wired or misspelt token cannot ride in inside
    # the brackets), and the SDOW/Slasher phases the feature was written for must
    # still resolve to their expected type.
    REQUIRED_CT = {
        "The Slasher: Out of the Shadows":    "Infestations",
        "The Slasher: Blood Will Have Blood": "Head Hunts",
        "The Slasher: Secrets to the Grave":  "Grave Digging",
        "The Slasher: Masked Truth":          "Quest",
    }
    ct_emitted = 0
    if qf:
        with open(qf, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                edid = (row.get("EDID") or "").strip()
                full = (row.get("FULL - Name") or row.get("FULL") or "").strip()
                if not edid or not full:
                    continue
                ct = plan_sources._derive_content_type(edid, full)
                if not ct:
                    continue
                ct_emitted += 1
                if ct not in plan_sources.CONTENT_TYPES:
                    leaks[f"(content type) {full} -> {ct!r} not in CONTENT_TYPES"] += 1
    for name, want in REQUIRED_CT.items():
        got = quests.content_type_for(name)
        if got != want:
            leaks[f"(content type) {name} resolved {got!r}, expected {want!r}"] += 1
    print(f"[labels] {ct_emitted} quest(s) carry a content-type parenthetical")

    if leaks:
        print(f"[labels] FAIL — {sum(leaks.values())} leaked name(s):")
        for l, n in leaks.most_common():
            print(f"    {n:5}  {l}")
        return 1

    print("[labels] PASS — no editor wiring reached a published name")
    return 0


if __name__ == "__main__":
    sys.exit(main())
