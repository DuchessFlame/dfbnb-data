#!/usr/bin/env python3
"""Unit tests for the plan_changes coherence rule.

    python3 tests/plan_changes_test.py

The rule decides whether a change is HONEST to publish, so getting it wrong is
not a crash — it is the page confidently telling a reader something false, or
silently telling them nothing forever. Neither shows up in the JS harness, which
only checks that a change renders once the builder has decided to publish it.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import plan_changes as pc                                       # noqa: E402

FAILS = []


def check(label, got, want):
    got = got or "PUBLISHABLE"
    ok = got == want
    if not ok:
        FAILS.append(f"{label}: got {got}, want {want}")
    print(f"  {'ok  ' if ok else 'FAIL'} {label:44s} {got}")


def env(dep, was_file, now_file, was_ok, now_ok, field="routes"):
    """One dependency under test; every other dependency settled and identical."""
    deps = pc.FIELD_DEPENDS[field]
    e = {d: f"{d}_Export_September_2026.tsv" for d in deps}
    c = {d: True for d in deps}
    return (dict(c, **{dep: was_ok}), dict(c, **{dep: now_ok}),
            dict(e, **{dep: was_file}), dict(e, **{dep: now_file}))


print("coherence rule — one dependency at a time")

# 1. The case that made the rule generative. CONT has been stale since July and
#    lags for months at a time; if a stale export blocked regardless of whether
#    it MOVED, it would suppress route reporting forever while protecting
#    against a false positive that cannot occur.
wc, nc, we, ne = env("CONT", "CONT_Export_July_2026.tsv",
                     "CONT_Export_July_2026.tsv", False, False)
check("stale but identical on both sides",
      pc._publishable("routes", wc, nc, we, ne), "PUBLISHABLE")

# 2. It really moved, and both sides were reading data older than their roster.
#    This is the flood case: the export catching up makes sources "appear".
wc, nc, we, ne = env("CONT", "CONT_Export_May_2026.tsv",
                     "CONT_Export_July_2026.tsv", False, False)
check("moved while stale on both sides",
      pc._publishable("routes", wc, nc, we, ne), "CONT")

# 3. The snapshot could not see what this build can. Absence then proves nothing.
wc, nc, we, ne = env("CONT", "CONT_Export_July_2026.tsv",
                     "CONT_Export_September_2026.tsv", False, True)
check("moved, snapshot side was stale",
      pc._publishable("routes", wc, nc, we, ne), "CONT")

# 4. A normal patch: the export moved forward and both builds were coherent.
wc, nc, we, ne = env("CONT", "CONT_Export_August_2026.tsv",
                     "CONT_Export_September_2026.tsv", True, True)
check("moved, coherent on both sides",
      pc._publishable("routes", wc, nc, we, ne), "PUBLISHABLE")

# 5. A missing export is not the same as an unchanged one. Two empty strings
#    must NOT read as "identical, therefore fine".
wc, nc, we, ne = env("CONT", "", "", False, False)
check("missing on both sides is not 'unchanged'",
      pc._publishable("routes", wc, nc, we, ne), "CONT")

print("\nfield dependencies")
# tradeable and name come off BOOK, which defines the roster, so they are never
# gated — that is what makes them the two fields that report on a stale export set.
for field in ("tradeable", "name"):
    check(f"{field} depends on nothing",
          "PUBLISHABLE" if not pc.FIELD_DEPENDS[field] else "gated", "PUBLISHABLE")

print("\nfilename stamps")
# The rule that stops mtime deciding. Both spellings have to parse, or PTS falls
# through to mtime and picks whichever file the filesystem returned first.
check("month-name stamp parses",
      str(pc._stamp("BOOK_Export_September_2026.tsv")), "(2026, 9, 0)")
check("ISO stamp parses",
      str(pc._stamp("BOOK_Export_PTS_2026-09-16_1847.tsv")), "(2026, 9, 16)")
check("September sorts after July",
      str(pc._stamp("X_Export_September_2026.tsv") > pc._stamp("X_Export_July_2026.tsv")),
      "True")

# The export prefix is the one the FILES use. A "QUST" entry here would match
# nothing, be permanently stale, and gate every route change forever.
check("quest prefix is QUEST, not QUST",
      "QUEST" if "QUEST" in pc.RECORD_TYPES and "QUST" not in pc.RECORD_TYPES else "wrong",
      "QUEST")

print()
if FAILS:
    print(f"{len(FAILS)} FAILED")
    for f in FAILS:
        print("   " + f)
    raise SystemExit(1)
print("all coherence-rule checks passed  — all green")
