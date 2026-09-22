#!/usr/bin/env python3
"""
plan_pins_test.py — the manual New-Plans pin overlay (src/plan_pins.py).

Asserts the behaviour the pin mechanism has to guarantee:
  * an active pin attaches a `changes` entry in the exact shape plan_changes.py
    emits (so it reuses the ↻ Changed render path, not a bolted-on one) and
    flags the matching route `new`;
  * a pin whose window has passed attaches nothing (auto-expiry);
  * a pin is de-duped against a change the diff already found;
  * a pin is skipped on the wrong channel (live pin, pts build);
  * a bad row is reported, not silently applied.

Run:  python3 tests/plan_pins_test.py
"""
import datetime
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))

import plan_pins

fails = 0
checks = 0


def ok(cond, msg, got=""):
    global fails, checks
    checks += 1
    tag = "ok  " if cond else "FAIL"
    if not cond:
        fails += 1
    print(f"  {tag} {msg:52s} {got}")


PINS = """\
PlanId\tKind\tRoute\tBucket\tStartDate\tEndDate\tChannel\tNote
PLAN_A\troute-added\tThe Slasher - Daily Ops\tEvents & Activities\t15/9/2026\t1/12/2026\tlive\tactive live pin
PLAN_B\troute-added\tOld Event\tEvents & Activities\t1/1/2026\t1/2/2026\tlive\texpired pin
PLAN_C\troute-added\tThe Slasher - Daily Ops\tEvents & Activities\t15/9/2026\t1/12/2026\tpts\tpts-only pin
PLAN_D\troute-added\tNoSuchRoute\tCaps\t15/9/2026\t1/12/2026\tlive\troute not on the plan
BADROW\tnonsense-kind\t\t\t15/9/2026\t1/12/2026\tlive\tunknown kind
"""


def fresh_items():
    return [
        {"id": "PLAN_A", "changes": [],
         "obtain_routes": [{"route": "Vendor", "source_type": "vendor"},
                           {"route": "The Slasher - Daily Ops",
                            "source_type": "event-quest"}]},
        {"id": "PLAN_B", "changes": [], "obtain_routes": []},
        {"id": "PLAN_C", "changes": [], "obtain_routes": []},
        {"id": "PLAN_D", "changes": [], "obtain_routes": [{"route": "Vendor"}]},
    ]


def main():
    with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False,
                                     encoding="utf-8") as fh:
        fh.write(PINS)
        path = fh.name
    day = datetime.date(2026, 9, 22)

    # ---- live channel -------------------------------------------------------
    items = fresh_items()
    by = {it["id"]: it for it in items}
    st = plan_pins.apply(items, tsv_dir="tsv", path=path, day=day)

    a = by["PLAN_A"]["changes"]
    ok(len(a) == 1, "active pin attaches exactly one change", f"got {len(a)}")
    if a:
        c = a[0]
        ok(c.get("field") == "routes" and c.get("kind") == "added",
           "change shape is routes/added", f"{c.get('field')}/{c.get('kind')}")
        ok(c.get("text") == "Now also comes from The Slasher - Daily Ops "
                            "(Events & Activities).",
           "text matches plan_changes wording", repr(c.get("text")))
        ok(c.get("pinned") is True, "change is tagged pinned")
    routeflag = [r for r in by["PLAN_A"]["obtain_routes"] if r.get("new")]
    ok(len(routeflag) == 1 and routeflag[0]["route"] == "The Slasher - Daily Ops",
       "matching route is flagged new")

    ok(by["PLAN_B"]["changes"] == [], "expired pin attaches nothing (auto-expiry)")
    ok(by["PLAN_C"]["changes"] == [], "pts-only pin skipped on live build")
    ok(st["off_channel"] == 1, "one pin counted off-channel", st["off_channel"])
    ok(("PLAN_D", "NoSuchRoute") in st["missing_route"],
       "pin whose route is absent is reported")
    ok(any("BADROW" in r[2] or r[1].startswith("unknown Kind")
           for r in st["errors"]), "bad row reported as an error")

    # ---- de-dupe ------------------------------------------------------------
    items2 = fresh_items()
    items2[0]["changes"] = [{"field": "routes", "kind": "added",
                             "route": "The Slasher - Daily Ops",
                             "bucket": "Events & Activities",
                             "text": "Now also comes from The Slasher - Daily "
                                     "Ops (Events & Activities).",
                             "since": "the previous build"}]
    st2 = plan_pins.apply(items2, tsv_dir="tsv", path=path, day=day)
    ok(len(items2[0]["changes"]) == 1,
       "pin de-dupes against a change the diff already found",
       f"{len(items2[0]['changes'])}")
    ok(st2["deduped"] >= 1, "de-dupe counted", st2["deduped"])

    # ---- pts channel --------------------------------------------------------
    items3 = fresh_items()
    by3 = {it["id"]: it for it in items3}
    plan_pins.apply(items3, tsv_dir="tsv/pts", path=path, day=day)
    ok(by3["PLAN_A"]["changes"] == [], "live pin skipped on pts build")
    ok(by3["PLAN_C"]["changes"] and by3["PLAN_C"]["changes"][0]["route"]
       == "The Slasher - Daily Ops", "pts pin applies on pts build")

    # ---- future date: everything expired -----------------------------------
    items4 = fresh_items()
    st4 = plan_pins.apply(items4, tsv_dir="tsv", path=path,
                          day=datetime.date(2027, 1, 1))
    ok(st4["applied"] == 0 and all(it["changes"] == [] for it in items4),
       "after every end date, nothing is pinned")

    os.unlink(path)
    print(f"\n{checks - fails}/{checks} pin checks passed"
          + ("  — all green" if not fails else f"  — {fails} FAILED"))
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
