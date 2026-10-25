"""bug_report_text.py — Bethesda bug reports from Current Bugged Plans rows.

The text matches the Bug Report Template page (initBugReportTemplate() in
df-bnb-data-mining.js), the bug-report-bethesda skill and Brewmaster's
bugReport.js, character for character.

build_bugged_plans_json.py calls fields_for(item) and stores the result on the
row as item["report"] (no player details: the repo is public). The PC tool
tools/write_bug_reports.py adds the player details and writes the .txt into
Guides and Stuff\\.Bethesda Bug Reports, where Brewmaster picks it up.

A report is only made for a row Bethesda can act on without a guess:
confidence "strong" (a structural fact in the files) or marked "confirmed" in
data/bugged_plans_review.json. Rows marked "check" need an in-game test first.
"""
import re

FOOTER = "Report built using the Bug Report Template on theduchessflame.com"
RX_PLAN_WORD = re.compile(r"^\s*(plan|recipe)\s*:\s*", re.I)


def wants_report(item):
    return item.get("confidence") == "strong" or bool(item.get("confirmed"))


def slug(text, words=6):
    w = re.findall(r"[a-z0-9]+", (text or "").lower())
    return "-".join(w[:words]) or "plan-bug"


def _sources(item):
    src = [s for s in item.get("still_from") or [] if s][:4]
    return " / ".join(src) if src else "any source"


def _records(bug):
    return ", ".join(f"{r['edid']} [{r['type']}:{r['form_id']}]"
                     for r in bug.get("records") or [] if r.get("form_id"))


def fields_for(item):
    """dict(title, description, steps, expected, actual, dept, cause, filename)
    for the row's first bug, or None when no report should be made."""
    if not wants_report(item) or not item.get("bugs"):
        return None
    bug = item["bugs"][0]
    name = item["name"]
    short = RX_PLAN_WORD.sub("", name).strip() or name
    k = bug["kind"]
    if k == "wrong_item":
        want, got = bug["should_teach"], bug["teaches"]
        title = f"{name} teaches {got} instead of the crafting recipe for {want}"
        steps = (f"1. Get {name} ({_sources(item)})\n"
                 f"2. Learn the plan\n"
                 f"3. Go to a Weapons Workbench and look for {want}")
        expected = f"Learning {name} unlocks crafting {want}."
        actual = f"It unlocks {got}. {want} can't be crafted, and there is no other way to learn it."
        cause = (f"The plan's recipe was switched to {got}. {want}'s own recipe "
                 f"({bug['old_recipe']}) now makes nothing and is unlocked by a deleted record. "
                 f"Pointing the plan back at the recipe for {want} should fix it.")
        if bug.get("stale_checks"):
            cause += (" These condition forms still check the old recipe, so they can never "
                      "pass for anyone: " + ", ".join(bug["stale_checks"]) + ".")
        dept = "System Design"
    elif k == "no_recipe":
        title = f"{name} teaches nothing when learned"
        steps = f"1. Get {name} ({_sources(item)})\n2. Learn the plan\n3. Check the workbench for {short}"
        expected = f"Learning {name} unlocks {short}."
        actual = "Nothing is unlocked."
        cause, dept = "No recipe is linked to this plan.", "System Design"
    elif k == "wrong_tier":
        title = f"{name} unlocks the wrong tier"
        steps = f"1. Get {name} ({_sources(item)})\n2. Learn the plan\n3. Check what the workbench unlocks"
        expected = f"Learning {name} unlocks {short}."
        actual = bug.get("detail") or bug.get("text") or ""
        cause, dept = "", "System Design"
    else:
        title = f"{name} never drops ({bug.get('title', '').lower() or 'broken reward'})"
        steps = f"1. Run the content that should give {name} many times\n2. It never drops"
        expected = f"{name} can drop from its listed source."
        actual = "It never drops."
        cause, dept = "", "System Design"
    detail = bug.get("detail") or bug.get("text") or bug.get("summary") or ""
    recs = _records(bug)
    if recs:
        cause = (cause + "\n\nRecords: " + recs).strip()
    return {"title": title, "description": detail, "steps": steps,
            "expected": expected, "actual": actual, "dept": dept, "cause": cause,
            "filename": f"bethesda-bug-report-{slug(short, 4)}-{slug(bug['kind'].replace('_', ' '), 2)}.txt"}


def render(f, player):
    """Full report text. player: username, platform, mule, patch, buid."""
    def miss(v, ph):
        return v if (v or "").strip() else ph
    out = [
        "TITLE", miss(f.get("title"), "(missing — please add a one-line headline)"), "",
        "WHERE", f"Server: {f.get('channel') or 'Live Servers'}",
        f"World:  {f.get('world') or 'Public Adventure'}", "",
        "DESCRIPTION", miss(f.get("description"), "(missing — please describe what happened)"), "",
        "STEPS TO REPRODUCE", miss(f.get("steps"), "(missing — please list the steps)"), "",
        "EXPECTED", miss(f.get("expected"), "(missing — what should have happened?)"), "",
        "ACTUAL", miss(f.get("actual"), "(missing — what actually happened?)"), "",
        "PLAYER DETAILS",
        f"Bethesda.net username: {miss(player.get('username'), '(missing — please add your Bethesda.net username)')}",
        f"Platform: {miss(player.get('platform'), '(missing — PC, Xbox or PlayStation?)')}",
        f"Mule / character name: {miss(player.get('mule'), '(missing — which character were you on?)')}", "",
        "PATCH", player.get("patch") or "", "",
        "REPRO RATE", f.get("rate") or "10/10", "",
    ]
    if (f.get("dept") or "").strip():
        out += ["AREA / DEPARTMENT", f["dept"], ""]
    if (f.get("cause") or "").strip():
        out += ["POSSIBLE CAUSE / SUGGESTED FIX", f["cause"], ""]
    out += ["BETHESDA.NET ID", f"Buid:     {player.get('buid') or ''}",
            f"Username: {player.get('username') or ''}", "", "---", FOOTER]
    return "\n".join(out) + "\n"
