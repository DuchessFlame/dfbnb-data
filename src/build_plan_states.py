#!/usr/bin/env python3
"""
build_plan_states.py — one source for every plan's state (6 Nov 2026)
====================================================================
Output: dist/plan_states.json

Six places used to each decide a piece of "is this plan live / bugged / cut /
unreleased / legacy / learnt": plan_status.json, plan_sources.py,
plan_learn_routes.py, build_bugged_plans_json.py, build_cut_content_json.py and
build_minerva.py. This step reads what they have ALREADY worked out and writes
ONE row per plan with ONE answer, so every reader (the Current Bugged Plans
page first; later Cut Content, the Item Finder and Brewmaster) says the same
thing.

It decides nothing new. It is a pure join over finished files — no rates, no
game-file walk, seconds to run:
    dist/plan_master.json   after plan_learn_routes.py (cut / learnt / legacy /
                            status_checked / quest_learned / plan_duplicate ...)
    data/plan_status.json   the Duchess's calls
    dist/bugged_plans.json  the bug checks, parked lists and the fixed history

So it runs in build-bugged-plans.yml straight AFTER build_bugged_plans_json.py.

PRECEDENCE (first match wins — claude/one-source-plan-states-plan.md)
  1. the Duchess's call in plan_status.json       (bugged / unreleased / legacy /
                                                    learnt / cut; `leave` falls through)
  2. marked cut in the files (dev prefix + no live ref, cut families, a cut-only source)
  3. learnt automatically (a quest teaches the recipe, or known by default)
  4. spare / duplicate / unused copy (cut)
  5. a bug check found it (bugged, with severity)
  6. no source and no call                         (not_checked)
  7. only source is a parked / unused list, no call (decide)
  8. otherwise live

STATES  live · bugged · not_checked · unreleased · decide · learnt · legacy · cut

PHASE 1: nothing reads this file yet except the new bugged-plans page. The
build prints every plan where this file disagrees with bugged_plans.json, so a
person can check the merge before the other readers move over.

Live only. The PTS bugged list has no calls, so there is no PTS plan_states.
"""
import argparse
import collections
import datetime
import json
import os
import re
import sys

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

STATES = ("live", "bugged", "not_checked", "unreleased", "decide", "learnt", "legacy", "cut")
CALLS = ("bugged", "unreleased", "legacy", "learnt", "cut")   # `leave` = no override
SEV_GROUP = {"unobtainable": "unobtainable", "route_broken": "route_broken",
             "broken_item": "broken_item"}


def load(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def strip_prefix(name):
    return re.sub(r"^(plan|recipe)\s*:\s*", "", name or "", flags=re.I).strip()


def sort_key(name):
    return strip_prefix(name).lower()


def nice_date(iso):
    try:
        d = datetime.date.fromisoformat(iso)
        return f"{d.day} {d.strftime('%b %Y')}"
    except (TypeError, ValueError):
        return iso or ""


def plan_fid(it):
    return ((it.get("plan_item") or {}).get("formid") or it.get("id") or "").upper()


def book_edid(it):
    pi = it.get("plan_item") or {}
    return pi.get("edid") or pi.get("editor_id") or ""


def has_source(it):
    return bool(it.get("obtain_routes") or it.get("obtain_unlocks"))


def decide_row(it, call, bug, parked_names):
    """(state, why, by, note) for one plan_master row. Follows PRECEDENCE."""
    name = it.get("name") or ""
    # 1. the Duchess's call
    if call and call.get("status") in CALLS:
        st = call["status"]
        note = call.get("note") or ""
        by = call.get("by") or "Duchess"
        if st == "unreleased" and has_source(it):
            pass   # a patch gave it a source: the call is out of date, fall through
        elif st == "bugged" and has_source(it) and not bug:
            pass   # same — it drops now; let the checks decide
        else:
            why = {
                "bugged": "the Duchess has checked it: bugged, not dropping",
                "unreleased": "never released — in the game files, but nothing gives it out",
                "legacy": "no longer drops — old copies can still be traded",
                "learnt": "learnt in game, so there is no plan to find",
                "cut": "cut content",
            }[st]
            return st, why, by, note
    # 2. cut in the files (and 4. spare / duplicate / unused copies, also cut)
    if it.get("cut"):
        if it.get("plan_duplicate"):
            return "cut", "a spare copy of another plan — the same plan in game", "plan_learn_routes: duplicate", it.get("cut_reason") or ""
        if it.get("unused_plan"):
            return "cut", "not a real plan — nothing gives it out and it teaches nothing", "plan_learn_routes: unused", it.get("cut_reason") or ""
        return "cut", it.get("cut_reason") or "cut content", "game files", ""
    # 3. learnt automatically
    if it.get("quest_learned") or it.get("learnt"):
        q = (it.get("learn_quest") or {}).get("name") or ""
        if it.get("learnt") == "default":
            return "learnt", "known from the start", "plan_learn_routes", ""
        return "learnt", (f"learnt from the quest {q}" if q else "learnt from a quest"), "plan_learn_routes: quest learned", q
    if it.get("legacy") or it.get("legacy_nw"):
        return "legacy", "no longer drops — old copies can still be traded", "game files", ""
    # 5. a bug check found it
    if bug:
        b0 = (bug.get("bugs") or [{}])[0]
        if bug.get("status") == "not_checked":
            return "not_checked", "no source found yet — not checked", "build_bugged_plans_json", ""
        return "bugged", b0.get("title") or "bugged", "build_bugged_plans_json: " + (b0.get("kind") or "check"), ""
    # 6. no source, no call
    if not has_source(it) and not it.get("recipe_only") and it.get("tradeable") is not False \
            and it.get("status_checked") is False:
        return "not_checked", "no source found yet — not checked", "plan_learn_routes", ""
    # 7. only on a parked / unused list
    if not has_source(it) and name in parked_names:
        return "decide", "only on a list nothing uses — waiting on the Duchess's call", "build_bugged_plans_json: parked list", parked_names[name]
    return "live", "", "", ""


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=os.path.join(REPO, "dist", "plan_master.json"))
    ap.add_argument("--status", default=os.path.join(REPO, "data", "plan_status.json"))
    ap.add_argument("--bugged", default=os.path.join(REPO, "dist", "bugged_plans.json"))
    ap.add_argument("--out", default=os.path.join(REPO, "dist", "plan_states.json"))
    a = ap.parse_args(argv)

    master = load(a.master, {}) or {}
    rows = master.get("items", master) if isinstance(master, dict) else master
    status = {k.upper(): v for k, v in (load(a.status, {}) or {}).items() if not k.startswith("_")}
    bugged = load(a.bugged, {}) or {}
    if bugged.get("channel") not in (None, "live"):
        sys.exit("[plan_states] bugged_plans.json is not the live list")
    bug_by_id = {x["id"].upper(): x for x in bugged.get("items", [])}
    parked_names = {}
    for p in bugged.get("parked_lists", []):
        for n in p.get("plans", []):
            parked_names.setdefault(n, p.get("source") or (p.get("list") or {}).get("edid") or "")

    plans = {}
    for it in rows or []:
        fid = plan_fid(it)
        if not fid or fid in plans:
            continue
        call = status.get(fid)
        bug = bug_by_id.get(fid)
        st, why, by, note = decide_row(it, call, bug, parked_names)
        row = {
            "name": it.get("name") or fid,
            "kind": "recipe" if it.get("recipe_only") else "plan",
            "edid": book_edid(it) or (call or {}).get("edid") or "",
            "state": st,
            "why": why,
            "by": by,
        }
        if note:
            row["note"] = note
        if call:
            row["call"] = {k: call[k] for k in ("status", "decided", "by") if call.get(k)}
        if it.get("tradeable") is not None:
            row["tradeable"] = it["tradeable"]
        if st == "learnt" and (it.get("learn_quest") or {}).get("name"):
            row["quest"] = it["learn_quest"]["name"]
        if bug and st in ("bugged", "not_checked"):
            row["bug"] = bug_detail(bug)
        plans[fid] = row

    # bug rows that aren't plan_master rows (titles, scrap-to-learn mods, backpack
    # skins kept out of plan_master on purpose). Same precedence: a call wins.
    for fid, bug in bug_by_id.items():
        if fid in plans:
            continue
        call = status.get(fid) or {}
        if call.get("status") in CALLS and call["status"] != "bugged":
            continue
        st = "not_checked" if bug.get("status") == "not_checked" else "bugged"
        plans[fid] = {
            "name": bug.get("name") or fid,
            "kind": bug.get("kind") or "plan",
            "edid": ((bug.get("technical") or {}).get("plan") or {}).get("edid") or "",
            "state": st,
            "why": (bug.get("bugs") or [{}])[0].get("title") or "bugged",
            "by": "build_bugged_plans_json",
            "bug": bug_detail(bug),
        }

    # the Duchess's calls on plans plan_master doesn't hold (BOOKs dropped upstream)
    for fid, call in status.items():
        if fid in plans or call.get("status") not in ("unreleased", "learnt"):
            continue
        plans[fid] = {"name": call.get("name") or fid, "kind": "plan", "edid": call.get("edid") or "",
                      "state": call["status"], "by": call.get("by") or "Duchess",
                      "why": "never released — in the game files, but nothing gives it out"
                             if call["status"] == "unreleased" else "learnt in game, so there is no plan to find",
                      "note": call.get("note") or "",
                      "call": {k: call[k] for k in ("status", "decided", "by") if call.get(k)}}

    # fixed since the last update: the bugged list's own history, minus anything
    # that is still (or again) bugged here
    fixed = [f for f in bugged.get("fixed", [])
             if (plans.get(f["id"].upper()) or {}).get("state") not in ("bugged", "not_checked")]

    counts = collections.Counter(r["state"] for r in plans.values())
    page = {
        "bugged": {g: [] for g in ("unobtainable", "route_broken", "broken_item", "not_checked")},
        "fixed": [f["id"].upper() for f in sorted(fixed, key=lambda f: sort_key(f["name"]))],
        "unreleased": [],
    }
    for fid, r in sorted(plans.items(), key=lambda kv: sort_key(kv[1]["name"])):
        if r["state"] == "not_checked":
            page["bugged"]["not_checked"].append(fid)
        elif r["state"] == "bugged":
            sev = (r.get("bug") or {}).get("severity") or "unobtainable"
            page["bugged"][SEV_GROUP.get(sev, "unobtainable")].append(fid)
        elif r["state"] in ("unreleased", "decide") or (r["state"] == "learnt" and r["kind"] == "plan"):
            page["unreleased"].append(fid)

    # ---- phase 1 check: where this file and bugged_plans.json disagree ----
    disagree = []
    old_bug = set(bug_by_id)
    new_bug = {f for f, r in plans.items() if r["state"] in ("bugged", "not_checked")}
    for f in sorted(old_bug - new_bug):
        disagree.append({"id": f, "name": bug_by_id[f]["name"], "bugged_plans": "bugged",
                         "plan_states": plans.get(f, {}).get("state", "missing"),
                         "why": plans.get(f, {}).get("why", "")})
    for f in sorted(new_bug - old_bug):
        disagree.append({"id": f, "name": plans[f]["name"], "bugged_plans": "not listed",
                         "plan_states": plans[f]["state"], "why": plans[f]["why"]})
    old_un = {u["id"].upper() for u in bugged.get("unreleased", [])}
    new_un = {f for f, r in plans.items() if r["state"] == "unreleased"}
    for f in sorted(old_un ^ new_un):
        disagree.append({"id": f, "name": plans.get(f, {}).get("name") or f,
                         "bugged_plans": "unreleased" if f in old_un else "not unreleased",
                         "plan_states": plans.get(f, {}).get("state", "missing"),
                         "why": plans.get(f, {}).get("why", "")})

    doc = {
        "_generated_by": "src/build_plan_states.py",
        "_readme": "One row per plan (BOOK FormID; recipe-only rows by their own id). `state` is the one "
                   "answer every page should give: live / bugged / not_checked / unreleased / decide / "
                   "learnt / legacy / cut. `why` is plain words, `by` says who decided. `page` is the "
                   "Current Bugged Plans layout, already grouped and sorted A-Z without the Plan:/Recipe: prefix.",
        "generated": datetime.date.today().isoformat(),
        "channel": "live",
        "update": bugged.get("generated") or "",
        "exports": bugged.get("exports") or {},
        "counts": {s: counts.get(s, 0) for s in STATES},
        "page_counts": {"bugged": sum(len(v) for v in page["bugged"].values()),
                        "fixed": len(page["fixed"]), "unreleased": len(page["unreleased"])},
        "page": page,
        "fixed": {f["id"].upper(): {"name": f["name"], "fixed_on": f.get("fixed_on") or ""} for f in fixed},
        "plans": plans,
        "disagreements": disagree,
    }
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1, ensure_ascii=False)
    print(f"[plan_states] {len(plans)} rows {dict(doc['counts'])} -> {a.out}")
    print(f"[plan_states] page: {doc['page_counts']}")
    print(f"[plan_states] {len(disagree)} disagreement(s) with bugged_plans.json")
    for d in disagree:
        print(f"   {d['id']}  {d['name']}: bugged_plans={d['bugged_plans']}  plan_states={d['plan_states']}  ({d['why']})")


def bug_detail(bug):
    """What the page needs from a bugged_plans row."""
    b = bug.get("bugs") or []
    recs, seen = [], set()
    tp = (bug.get("technical") or {}).get("plan")
    for r in ([tp] if tp else []) + [r for x in b for r in (x.get("records") or [])]:
        k = (r.get("form_id"), r.get("type"))
        if r.get("form_id") and k not in seen:
            seen.add(k)
            recs.append({"form_id": r["form_id"], "edid": r.get("edid") or "", "type": r.get("type") or ""})
    out = {
        "severity": bug.get("severity") or "unobtainable",
        "status": bug.get("status") or "",
        "problems": [{"title": x.get("title") or "", "summary": x.get("summary") or "",
                      "detail": x.get("detail") or ""} for x in b],
        "still_from": bug.get("still_from") or [],
        "records": recs,
        "confirmed": bool(bug.get("confirmed")),
        "first_seen": bug.get("first_seen") or "",
        "is_new": bool(bug.get("is_new")),
    }
    if bug.get("review_note"):
        out["review_note"] = bug["review_note"]
    return out


if __name__ == "__main__":
    main()
