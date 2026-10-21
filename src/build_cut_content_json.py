#!/usr/bin/env python3
"""
build_cut_content_json.py — Data Mining > Cut Content
=====================================================
Page: /df/data-mining/cut-content/        Output: dist/cut_content.json
                                                  (dist/pts/cut_content.json)

ONE page for everything that is in the game files but cannot be got in game
(Duchess, 21 Oct 2026): "instead of having cut root and sub expands on all the
pages, we just remove all the cut content from the pages and ... it can just
get dumped into there". Every other page leaves cut content out; this page is
where it goes.

Nothing here decides what is cut. Each builder already does, through the one
site-wide test plan_sources.is_cut_content() (confirmed cut families +
zzz / CUT / DEL records). This only collects their cut rows:

    dist/plan_master.json                         plans & recipes  (cut: true)
    dist/unique_weapons_armour/cut_uniques.json   unique weapons & armour
    dist/titles_player.json, titles_camp.json     player / C.A.M.P. titles (cutContent)
    dist/challenges/challenges.json               challenges (page "cut") + quests (is_cut)
    plan_sources.CONFIRMED_CUT_SOURCE_PATTERNS    the cut families themselves

Grouped by TYPE (Duchess: "by type"), each row: name, type, why it is cut,
and where it used to come from. No tick boxes, no progress, no dates.

    python src/build_cut_content_json.py          # live
    python src/build_cut_content_json.py --pts    # PTS twin (reads dist/pts/)
"""
import argparse, collections, datetime, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import plan_sources                    # noqa: E402

MARKED = "Marked cut in the game files"


def _load(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def why_for(*edids, reason=""):
    """Plain reason: the cut family by name, else 'marked cut'."""
    for e in edids:
        name = plan_sources.cut_source_name(e or "")
        if name:
            return f"{name[0].upper()}{name[1:]} is cut content"
    m = re.search(r"—\s*(.+? is cut content)", reason or "")
    if m:
        return m.group(1)[0].upper() + m.group(1)[1:]
    return MARKED


def _plain_name(s):
    return re.sub(r"^(plan|recipe)\s*:\s*", "", s or "", flags=re.I).lower()


# ── plans ────────────────────────────────────────────────────────────────────
BUCKET_TITLE = {"backpack-mod": "Backpack Skin & Flair", "recipe": "CAMP",
                "weapon": "Weapon", "armour": "Body Armour", "apparel": "Apparel",
                "underarmour": "Underarmour"}


def _singular(group):
    return re.sub(r"s$", "", group) if group.endswith(("Plans", "Recipes", "Weapons")) else group


def plan_rows(pm):
    """{group title: [rows]} for every cut plan, by the page it would be on."""
    pages = (pm.get("plan_subpages") or {}) if pm else {}
    out = collections.defaultdict(list)
    for it in (pm or {}).get("items", []):
        if not it.get("cut") or (it.get("kind") or "plan") != "plan":
            continue
        slug = it.get("plan_page") or ""
        title = (pages.get(slug) or {}).get("title")
        if not title:
            # A cut plan no page claims (the Backpack Mod page skips skins and
            # flair, for one): name the group after its record bucket.
            title = BUCKET_TITLE.get((it.get("type") or "").lower(), "Other")
        group = title if title.lower().endswith(("plans", "recipes")) else f"{title} Plans"
        was = []
        for r in (it.get("retired_routes") or []) + (it.get("obtain_routes") or []):
            if r.get("route") and r["route"] not in was:
                was.append(r["route"])
        edids = [(it.get(k) or {}).get("edid") for k in ("plan_item", "cobj", "cnam")]
        out[group].append({
            "name": it.get("name") or it.get("id"),
            "type": it.get("plan_page_group") or _singular(group),
            "why": why_for(*edids, reason=it.get("cut_reason") or ""),
            "was": was,
            "id": it.get("id"),
        })
    return out


# ── uniques ──────────────────────────────────────────────────────────────────
def unique_rows(doc):
    out = collections.defaultdict(list)
    for x in (doc or {}).get("items", []):
        kind = (x.get("kind") or "").lower()
        group = "Unique Armour" if kind.startswith("arm") else "Unique Weapons"
        why = x.get("why") or MARKED
        if why.startswith("its records are marked cut"):
            why = MARKED
        elif why == "confirmed cut in game":
            why = "Confirmed cut in game"
        else:
            why = why[0].upper() + why[1:]
        out[group].append({"name": x.get("name"), "type": x.get("subType") or x.get("kind") or "",
                           "why": why, "was": x.get("was") or [], "id": x.get("formId") or ""})
    return out


# ── titles ───────────────────────────────────────────────────────────────────
def title_rows(player, camp):
    out = collections.defaultdict(list)
    for doc, group in ((player, "Player Titles"), (camp, "C.A.M.P. Titles")):
        for t in (doc or {}).get("items", []):
            if not t.get("cutContent"):
                continue
            how = (t.get("howToObtain") or "").strip()
            was = [] if not how or how.lower().startswith(("cut", "this title is cut")) else [how]
            out[group].append({"name": t.get("title") or t.get("titleMale") or t.get("edid"),
                               "type": t.get("affixType") or "",
                               "why": why_for(t.get("edid")),
                               "was": was, "id": t.get("formId") or ""})
    return out


# ── challenges + quests ──────────────────────────────────────────────────────
def challenge_rows(doc):
    out = collections.defaultdict(list)
    if not doc:
        return out
    seen = set()
    for pg, pd in (doc.get("pages") or {}).items():
        for it in (pd.get("items") if isinstance(pd, dict) else pd) or []:
            if not it.get("is_cut") or it.get("form_id") in seen:
                continue
            seen.add(it.get("form_id"))
            out["Challenges"].append({"name": it.get("full") or it.get("edid"),
                                      "type": it.get("scope") or it.get("group") or "",
                                      "why": why_for(it.get("edid")), "was": [],
                                      "id": it.get("form_id") or ""})
    for q in doc.get("cut_quests") or []:
        if q.get("form_id") in seen:
            continue
        seen.add(q.get("form_id"))
        out["Quests"].append({"name": q.get("full") or q.get("edid"),
                              "type": q.get("quest_type") or "",
                              "why": why_for(q.get("edid")), "was": [],
                              "id": q.get("form_id") or ""})
    for page in (doc.get("quest_pages") or {}).values():
        for sec in page.values() if isinstance(page, dict) else []:
            for grp in sec if isinstance(sec, list) else []:
                for q in (grp.get("items") or []) if isinstance(grp, dict) else []:
                    if not q.get("is_cut") or q.get("form_id") in seen:
                        continue
                    seen.add(q.get("form_id"))
                    out["Quests"].append({"name": q.get("full") or q.get("edid"),
                                          "type": q.get("quest_type") or "",
                                          "why": why_for(q.get("edid")), "was": [],
                                          "id": q.get("form_id") or ""})
    return out


# ── collectables + CAMP items ────────────────────────────────────────────────
CAMP_FILES = ("camp", "pets", "pet-furniture", "pet-apparel", "cryos", "fridges",
              "dispensers", "repair-bots", "allies", "weather_stations", "buff-stations")


def _pretty_edid(edid):
    e = re.sub(r"^(zzz_?|CUT_|DEL_|POST_)+", "", edid or "", flags=re.I)
    e = re.sub(r"^(ATX_ENTM_|ATX_|MILE_|SCORE_S\d+_)", "", e, flags=re.I)
    return re.sub(r"[_]+", " ", re.sub(r"(?<=[a-z])(?=[A-Z])", " ", e)).strip()


def collectable_camp_rows(dist):
    out = collections.defaultdict(list)
    for path in sorted(__import__("glob").glob(os.path.join(dist, "collectables_*.json"))):
        d = _load(path) or {}
        kind = str(d.get("type") or "").replace("_", " ").title()
        for x in d.get("cutContent") or []:
            out["Collectables"].append({"name": x.get("name") or _pretty_edid(x.get("edid")),
                                        "type": (x.get("group") or kind).rstrip("s") if kind else "",
                                        "why": why_for(x.get("edid")), "was": [],
                                        "id": x.get("formId") or ""})
    seen = set()
    for stem in CAMP_FILES:
        d = _load(os.path.join(dist, stem + ".json")) or {}
        for x in d.get("items") or []:
            if not (x.get("cutContent") or x.get("status") == "cut" or x.get("isCut")):
                continue
            if x.get("formId") in seen:
                continue
            seen.add(x.get("formId"))
            name = x.get("displayName") or x.get("name") or ""
            if not name or name.upper() == "TBD":
                name = _pretty_edid(x.get("edid"))
            out["CAMP Items"].append({"name": name,
                                      "type": str(x.get("pageType") or stem).replace("-", " ").title(),
                                      "why": why_for(x.get("edid")), "was": [],
                                      "id": x.get("formId") or ""})
    return out


# ── the cut families themselves ──────────────────────────────────────────────
def cut_sources():
    """Every confirmed-cut family, by name, once. No dates (Duchess)."""
    seen, out = set(), []
    for _rx, name, why in plan_sources.CONFIRMED_CUT_SOURCE_PATTERNS:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        # The group blurb already says these are cut; the name is the line.
        out.append({"name": name[0].upper() + name[1:], "note": ""})
    return sorted(out, key=lambda x: _plain_name(x["name"].removeprefix("The ")))


GROUP_ORDER = ["Weapon Plans", "Mines and Grenades Plans", "Body Armour Plans",
               "Power Armour Plans", "Underarmour Plans", "Apparel Plans",
               "Backpack Mod Plans", "Backpack Skin & Flair Plans", "CAMP Plans", "Snow Globes Plans",
               "Fishing Rod Plans", "Camera Mod Plans", "Recipe Plans",
               "Legacy Nuclear Winter Plans",
               "Unique Weapons", "Unique Armour",
               "CAMP Items", "Collectables",
               "Player Titles", "C.A.M.P. Titles", "Quests", "Challenges"]


def build(dist):
    pm = _load(os.path.join(dist, "plan_master.json"))
    groups = collections.defaultdict(list)
    for part in (plan_rows(pm),
                 unique_rows(_load(os.path.join(dist, "unique_weapons_armour", "cut_uniques.json"))),
                 title_rows(_load(os.path.join(dist, "titles_player.json")),
                            _load(os.path.join(dist, "titles_camp.json"))),
                 challenge_rows(_load(os.path.join(dist, "challenges", "challenges.json"))),
                 collectable_camp_rows(dist)):
        for g, rows in part.items():
            groups[g].extend(rows)
    order = {g: i for i, g in enumerate(GROUP_ORDER)}
    out_groups = []
    for g in sorted(groups, key=lambda g: (order.get(g, 50), g)):
        rows = sorted(groups[g], key=lambda r: (_plain_name(r["name"]), r.get("id") or ""))
        out_groups.append({"key": re.sub(r"[^a-z0-9]+", "-", g.lower()).strip("-"),
                           "title": g, "count": len(rows), "items": rows})
    return {
        "_generated_by": "src/build_cut_content_json.py",
        "generated": datetime.date.today().isoformat(),
        "count": sum(g["count"] for g in out_groups),
        "sources": cut_sources(),
        "groups": out_groups,
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--pts", action="store_true")
    a = ap.parse_args(argv)
    dist = os.path.join(REPO, "dist", "pts") if a.pts else os.path.join(REPO, "dist")
    doc = build(dist)
    out = os.path.join(dist, "cut_content.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1, ensure_ascii=False)
    print(f"[cut_content] {doc['count']} cut item(s) in {len(doc['groups'])} group(s), "
          f"{len(doc['sources'])} cut source famil(ies) -> {out}")
    for g in doc["groups"]:
        print(f"    {g['title']:32s} {g['count']}")


if __name__ == "__main__":
    main()
