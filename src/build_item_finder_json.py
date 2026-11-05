#!/usr/bin/env python3
"""
build_item_finder_json.py  —  Data Mining > Item Finder  (/df/data-mining/item-finder/)

Builds a SMALL search index plus answer shards so the website never has to load
the 31 MB plan_master.json in the browser:

  dist/item_finder/index.json        every searchable name (plans, no-plan apparel,
                                     titles, legendary mods, unique weapons/armour)
  dist/item_finder/answers_NN.json   the finished answer for each item, split into
                                     SHARDS files by a hash of the item key

The WordPress endpoint (inc/item-finder.php) reads these, does the name matching
on the server (so the daily search limit can't be skipped) and hands the browser
one answer at a time.

Answers are worded the same way as Brewmaster's plan lookup in Discord
(planSearch.js: planAnswer / titleAnswer / rateText / LEDGER_TITLES / CUT_ROUTES):
the lines are stored in the same Discord-style markdown (**bold**, *(italic)*,
"-# " footer) and the page draws them.

No AI anywhere: plain name matching and copied text.

Inputs (dist/ + tsv/ of this repo):
  dist/plan_master.json, dist/no_plan_apparel.json, dist/titles_camp.json,
  dist/titles_player.json, dist/legendary_mods.json,
  dist/unique_weapons_armour/unique_weapons_armour.json, dist/bugged_plans.json,
  tsv/LVLI_Export_<newest>_LVLI_Entries.tsv  (reputation ranks on vendor stock)
"""
import json, os, re, sys, zlib, glob, datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DIST = os.environ.get("ITEM_FINDER_DIST_IN") or os.path.join(ROOT, "dist")
TSV = os.environ.get("ITEM_FINDER_TSV") or os.path.join(ROOT, "tsv")
OUT = os.environ.get("ITEM_FINDER_OUT") or os.path.join(ROOT, "dist", "item_finder")
SHARDS = 32
SCHEMA = 1

# ── Brewmaster rules (planSearch.js) ─────────────────────────────────────────
CUT_ROUTES = ["mole miner mystery crate"]          # PLAN_CUT_ROUTES default
LEDGER_TITLES = {
    "Caps": "💰 Bought with Caps",
    "Gold Bullion": "🪙 Bought with Gold Bullion",
    "Stamps": "📮 Bought with Stamps",
    "Events & Activities": "🎪 Events & Activities",
    "Quests": "📜 Quests",
    "Containers": "📦 Containers",
    "Enemies": "💀 Enemies",
    "Challenges": "🏆 Challenges",
    "Scrap to Learn": "🔧 Scrap to Learn",
    "Badges": "🎖️ Bought with Badges",
    "Atom Shop": "⚛️ Atom Shop",
    "Scoreboard": "🏅 Scoreboard",
}
CLIP_AT, CLIP_TO = 1990, 1960                       # planSearch.js clip()
CLIP_NOTE = "*…and more, see the plan checklist pages.*"

# Plan status / bug wording — copied from Brewmaster:
#   planStatus.js WORDS / ICON / learntText() / lineFor()   (the Duchess, 1–2 Nov 2026)
#   buggedPlans.js SEV_HEAD / bugNote()
WORDS = {
    "bugged": "That plan is bugged and isn't dropping right now. It's been reported.",
    "unreleased": "That one was never released. It's in the game files, but nothing gives it out.",
    "legacy": "It doesn't drop any more. The only way to get it now is trading for an old copy.",
    "not_checked": "No known source yet. The Duchess is looking into it.",
    "learnt": "There's no plan to find for that one. Every character learns the recipe without it.",
    "cut": "That one was cut. It's still in the game files, but it was never given out and can't be got.",
}
ICON = {"bugged": "🐛", "unreleased": "🚫", "legacy": "📦", "not_checked": "🔎", "learnt": "📖", "cut": "✂️"}
SEV_HEAD = {
    "unobtainable": "Currently bugged — this can't be obtained right now.",
    "route_broken": "Currently bugged — one of its drop routes is broken.",
    "broken_item": "Currently bugged — the plan itself doesn't work.",
}
BUGGED_PAGE = "/df/data-mining/current-bugged-plans/"


def stripPrefix(n):
    return re.sub(r"^\s*(plan|recipe|blueprint)\s*:\s*", "", str(n or ""), flags=re.I)


def norm(s):
    s = str(s or "").lower()
    s = re.sub(r"[‘’']", "", s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()


def compact(s):
    return norm(s).replace(" ", "")


def pct(n):
    if n >= 100:
        return "100%"
    return f"{n:.4f}%"


def reformatPct(t):
    return re.sub(r"(\d+(?:\.\d+)?)\s*%", lambda m: pct(float(m.group(1))), str(t))


def rateText(r):
    if isinstance(r.get("rate"), (int, float)) and not isinstance(r.get("rate"), bool):
        return pct(r["rate"] * 100)
    return reformatPct(r["rate_display"]) if r.get("rate_display") else "?"


def is_cut_route(r):
    return any(str(r.get("route") or "").lower().startswith(c) for c in CUT_ROUTES)


def drop_cut_routes(routes, ledger):
    keep, mp, cut_names = [], {}, []
    for i, r in enumerate(routes):
        if is_cut_route(r):
            c = next(c for c in CUT_ROUTES if str(r["route"]).lower().startswith(c))
            if c not in cut_names:
                cut_names.append(c)
            continue
        mp[i] = len(keep)
        keep.append(r)
    if not cut_names:
        return routes, ledger, []
    led = []
    for g in ledger:
        g2 = dict(g)
        g2["routes"] = [mp[i] for i in (g.get("routes") or []) if i in mp]
        if g2["routes"] or g2.get("unlocks"):
            led.append(g2)
    cut = []
    for c in cut_names:
        r = next(r for r in routes if str(r["route"]).lower().startswith(c))
        cut.append(re.sub(r"\s*\(.*\)\s*$", "", r["route"]))
    return keep, led, cut


# ── reputation ranks (planSearch.js loadRepTiers / addRep) ───────────────────
REP_FACTION = {"crater": "Raiders", "foundation": "Settlers", "raider": "Raiders", "settler": "Settlers"}
REP_COND = re.compile(r"Rep_Tier_([A-Za-z]+)_(\d)_([A-Za-z]+)_Condition")
REP_SUFFIX = re.compile(r"_(Raider|Settler)_.*_(\d)_(Cautious|Neutral|Cooperative|Friendly|Neighborly|Ally)$", re.I)
MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]


def newest_export(sig, suffix=""):
    best, best_score = None, -1
    rx = re.compile(rf"^{sig}_Export_([A-Za-z]+)_(\d{{4}}){suffix}\.tsv$")
    for f in os.listdir(TSV) if os.path.isdir(TSV) else []:
        m = rx.match(f)
        if not m:
            continue
        mi = MONTHS.index(m.group(1)[:3].lower()) if m.group(1)[:3].lower() in MONTHS else -1
        if mi < 0:
            continue
        score = int(m.group(2)) * 12 + mi
        if score > best_score:
            best_score, best = score, os.path.join(TSV, f)
    return best


def load_rep_tiers():
    f = newest_export("LVLI", "_LVLI_Entries")
    if not f:
        print("[item finder] no LVLI export, reputation ranks are off")
        return None
    entries = {}
    with open(f, encoding="utf-8-sig") as fh:
        head = fh.readline().rstrip("\r\n").split("\t")
        iId, iRef = head.index("LVLI_FormID"), head.index("LVLO_Reference")
        iCond = head.index("CondCount") if "CondCount" in head else -1
        for line in fh:
            c = line.rstrip("\r\n").split("\t")
            if len(c) <= iRef:
                continue
            parts = (c[iRef].split(":") + ["", "", ""])[:3]
            entries.setdefault(c[iId].upper(), []).append(
                {"fid": parts[0].upper(), "edid": parts[1], "sig": parts[2],
                 "cond": "\t".join(c[iCond:]) if iCond >= 0 else ""})
    cache = {}

    def walk(lv, tier, out, seen):
        if lv in seen:
            return
        seen.add(lv)
        for e in entries.get(lv, []):
            t = tier
            m = REP_COND.search(e["cond"])
            if m:
                t = {"n": int(m.group(2)), "rank": m.group(3), "faction": REP_FACTION.get(m.group(1).lower(), m.group(1))}
            elif not t and e["sig"] == "LVLI":
                s = REP_SUFFIX.search(e["edid"])
                if s:
                    t = {"n": int(s.group(2)), "rank": s.group(3), "faction": REP_FACTION.get(s.group(1).lower())}
            if e["sig"] == "LVLI":
                walk(e["fid"], t, out, seen)
            elif t and (e["fid"] not in out or t["n"] < out[e["fid"]]["n"]):
                out[e["fid"]] = t
        seen.discard(lv)

    def rep_of(lvlis, fid):
        for lv in lvlis or []:
            k = str(lv).upper()
            if k not in cache:
                out = {}
                walk(k, None, out, set())
                cache[k] = out
            t = cache[k].get(fid)
            if t:
                return t
        return None
    print(f"[item finder] reputation ranks from {os.path.basename(f)}")
    return rep_of


def add_rep(routes, pid, rep_of):
    if not rep_of:
        return routes
    fid = re.sub(r"^[A-Z]+_", "", str(pid or "")).upper()
    out = []
    for r in routes:
        if r.get("source_type") != "vendor" or not r.get("lvli"):
            out.append(r)
            continue
        rep = rep_of(r["lvli"], fid)
        out.append(dict(r, rep=rep) if rep else r)
    return out


def rep_text(r):
    rep = r.get("rep")
    return f" · needs **{rep['rank']}** reputation with the {rep['faction']}" if rep else ""


def unlock_rows(text):
    t = re.sub(r"\.\s*$", "", str(text or "").strip())
    body = re.sub(r"^drops?\s+from\s*:\s*", "", t, flags=re.I)
    items, end = [], 0
    rx = re.compile(r"\s*(.+?)\s+[—–-]\s+(\d+(?:\.\d+)?)\s*%\s*(?:,|$)")
    pos = 0
    while True:
        m = rx.match(body, pos)
        if not m:
            break
        items.append(f"• {m.group(1).strip()} - {pct(float(m.group(2)))}")
        pos = end = m.end()
        if pos >= len(body):
            break
    if items and end >= len(body):
        return items
    return [f"• {t}"]


def clip(lines):
    out = "\n".join(lines)
    if len(out) > CLIP_AT:
        out = re.sub(r"\n[^\n]*$", "", out[:CLIP_TO]) + "\n" + CLIP_NOTE
    return out.split("\n")


# ── plans ────────────────────────────────────────────────────────────────────
def learnt_text(p):
    q = (p.get("learnQuest") or {}).get("name")
    if q:
        return f"There's no plan to find for that one. Finishing the quest {q} teaches the recipe."
    if p.get("learnt") == "default":
        return "There's no plan to find for that one. Every character knows the recipe from the start."
    return WORDS["learnt"]


def status_line(p):
    """planStatus.js lineFor() — the Duchess's call on a plan with no source."""
    if p.get("apparel"):
        return None, None
    call = p.get("planStatus")
    if call == "learnt" or (not call and p.get("learnt")):
        return "learnt", f"{ICON['learnt']} **{learnt_text(p)}**"
    if p["routes"] or p["unlocks"]:
        return None, None
    if call and call in WORDS:
        return call, f"{ICON[call]} **{WORDS[call]}**"
    if call:
        return None, None                                   # 'leave'
    if not p.get("cut") and p.get("statusChecked") is False:
        return "not_checked", f"{ICON['not_checked']} **{WORDS['not_checked']}**"
    return None, None


def bug_note(p, bugs):
    """buggedPlans.js bugNote() — on the live bugged list?"""
    cur = bugs.get("_label")
    info = bugs.get(p["fid"])
    if not info:
        return None
    if info["st"] == "bugged":
        lines = [f"🐛 **{WORDS['bugged']}**"]
    elif info["st"] == "not_checked":
        lines = [f"🔎 **{WORDS['not_checked']}**"]
    else:
        lines = [f"🐛 **{SEV_HEAD.get(info['sev'], 'This plan is on the bugged list right now.')}**" + (f" {info['why']}" if info["why"] else "")]
    if info["sev"] == "route_broken" and info["still"]:
        lines.append("It still drops from: " + ", ".join(info["still"]))
    lines.append(f"-# On the bugged list as of the {cur} game update · [Current Bugged Plans]({BUGGED_PAGE})")
    return {"sev": info["sev"], "st": info["st"], "lines": lines}


def plan_answer(p, bugs):
    lines = [f"{'👕' if p.get('apparel') else '📜'} **{p['name']}**"]
    st, st_text = status_line(p)
    bug0 = None if p.get("cut") else bug_note(p, bugs)
    # the bug checks found real evidence → their explanation wins over "not checked yet"
    if st == "not_checked" and bug0 and not bug0["st"]:
        st, st_text = None, None
    bug = None if st_text else bug0
    if st_text:
        lines.append(st_text)
    if bug:
        lines.extend(bug["lines"])
    if p.get("cut") and st != "unreleased":
        lines.append("✕ This is cut content, it was never released and can't be obtained.")
    nw = p.get("nw")
    if nw:
        lines.append(f"☢️ **Legacy {nw.get('label')} plan:** {' · '.join(nw.get('lines') or [])}"
                     + (f" ({nw['retired']})" if nw.get("retired") else ""))
    used = set()
    for g in p["ledger"]:
        title = LEDGER_TITLES.get(g.get("label"), g.get("label"))
        rows = []
        for i in g.get("routes") or []:
            if i >= len(p["routes"]):
                continue
            r = p["routes"][i]
            used.add(i)
            cond = f" *({', '.join(r['conditions'])})*" if r.get("conditions") else ""
            rows.append(f"• {r['route']} - {rateText(r)}{rep_text(r)}{cond}")
        for i in g.get("unlocks") or []:
            if i < len(p["unlocks"]) and p["unlocks"][i]:
                rows.extend(unlock_rows(p["unlocks"][i]))
        if rows:
            lines.append(f"**{title}**")
            lines.extend(rows)
    loose = [r for i, r in enumerate(p["routes"]) if i not in used]
    if loose:
        lines.append("**Other sources**")
        lines.extend(f"• {r['route']} - {rateText(r)}{rep_text(r)}" for r in loose)
    if not p["ledger"] and p["unlocks"]:
        for u in p["unlocks"]:
            lines.extend(unlock_rows(u))
    only_cut = bool(p["cutSources"]) and not p["routes"] and not p["unlocks"]
    if p["cutSources"]:
        names = " / ".join(p["cutSources"])
        lines.append(f"✕ It only came from the {names}, which is cut content now — it can't be obtained right now."
                     if only_cut else f"-# The {names} is cut content now, so it's left off this list.")
    if (not only_cut and not p["ledger"] and not p["routes"] and not p["unlocks"] and not nw
            and not p.get("cut") and not st_text and (bug or {}).get("sev") != "unobtainable"):
        lines.append("This one can't currently be obtained in the game." if p.get("notObtainable")
                     else (p.get("obtain") or "I don't have a source for this one yet."))
    foot = [p.get("category"),
            "Tradeable" if p.get("tradeable") is True else "Not tradeable" if p.get("tradeable") is False else None,
            "Stops dropping once learned" if p.get("stopsDropping") else None]
    foot = [f for f in foot if f]
    if foot:
        lines.append("-# " + " · ".join(foot))
    return clip(lines)


def plan_top3(p):
    st, _ = status_line(p)
    if st == "unreleased":
        return [["Never released — nothing gives it out", "", ""]]
    if p.get("cut"):
        return [["Cut content — can't be obtained", "", ""]]
    rated = [r for r in p["routes"] if isinstance(r.get("rate"), (int, float))]
    rated.sort(key=lambda r: -r["rate"])   # stable: page order on ties
    out = []
    seen = set()
    for r in rated:
        k = r["route"].lower()
        if k in seen:
            continue
        seen.add(k)
        cond = ", ".join(r.get("conditions") or [])
        if r.get("rep"):
            cond = (cond + "; " if cond else "") + f"needs {r['rep']['rank']} reputation with the {r['rep']['faction']}"
        out.append([r["route"], rateText(r), cond])
        if len(out) == 3:
            return out
    extra = []
    for u in p["unlocks"]:
        for row in unlock_rows(u):
            m = re.match(r"^• (.+?) - (\d[\d.]*%)$", row)
            extra.append([m.group(1), m.group(2), ""] if m else [row[2:], "", ""])
    extra.sort(key=lambda r: -_pnum(r[1]))          # highest drop rate first
    out.extend(extra[:3 - len(out)])
    if not out:
        st, txt = status_line(p)
        out.append([re.sub(r"[*]", "", txt) if txt else "No known source yet", "", ""])
    return out


def thing_for_plan(p):
    t = p.get("type") or ""
    if p.get("apparel"):
        return "piece of apparel"
    if re.match(r"^\s*recipe\s*:", p["name"], re.I):
        return "recipe"
    if t == "apparel":
        return "apparel plan"
    return "plan"


# ── titles (planSearch.js build + titleAnswer) ───────────────────────────────
def build_titles(dist):
    by = {}
    for file, sub, label in (("titles_camp.json", "camp", "CAMP Title"), ("titles_player.json", "player", "Player Title")):
        path = os.path.join(dist, file)
        if not os.path.exists(path):
            continue
        for t in json.load(open(path, encoding="utf-8")).get("items") or []:
            if not t.get("title"):
                continue
            affix = t.get("affixType") or ("Prefix" if t.get("isPrefix") else "Suffix" if t.get("isSuffix") else "")
            k = f"t:{sub}:{t['title'].lower()}:{affix.lower()}"
            e = by.get(k) or {"key": k, "kind": "title", "sub": sub, "name": t["title"], "bare": t["title"],
                              "female": t["titleFemale"] if t.get("titleFemale") and t.get("titleFemale") != t["title"] else None,
                              "label": f"{label} {affix}".strip(), "ways": [], "cutWays": []}
            dr = t.get("dropRate")
            (e["cutWays"] if t.get("cutContent") else e["ways"]).append({
                "how": t.get("howToObtain") or "", "source": t.get("source") or "",
                "dropRate": str(dr) if dr and dr != "N/A" else None,
                "tradeable": t.get("tradeable"), "stopsDropping": t.get("stopsDropping"),
                "released": t.get("releaseLabel") or ""})
            by[k] = e
    out = list(by.values())
    for e in out:
        e["cut"] = not e["ways"]
    return out


def title_answer(t):
    lines = [f"🏷️ **{t['name']}**" + (f" / **{t['female']}**" if t.get("female") else "") + f" — {t['label']}"]
    if t["cut"]:
        lines.append("✕ This is cut content, it can't be obtained.")
        return clip(lines)
    many = len(t["ways"]) > 1
    for i, w in enumerate(t["ways"]):
        if many:
            lines.append(f"**{w['source'] or f'Way {i + 1}'}**")
        if w["how"]:
            lines.append("• " + re.sub(r"\s*\n\s*", " ", w["how"]))
        if w["dropRate"]:
            parts = [x.strip() for x in re.split(r"\r?\n", reformatPct(w["dropRate"])) if x.strip()]
            if len(parts) == 1:
                lines.append(f"• Drop chance: {parts[0]}")
            else:
                lines.append("• Drop chance:")
                lines.extend(f"  ◦ {x}" for x in parts)
    w0 = t["ways"][0]
    foot = [None if many else w0["source"],
            "Tradeable" if w0["tradeable"] is True else "Not tradeable" if w0["tradeable"] is False else None,
            "Stops dropping once unlocked" if any(w["stopsDropping"] for w in t["ways"]) else None,
            f"Released {w0['released']}" if not many and w0["released"] else None]
    foot = [f for f in foot if f]
    if foot:
        lines.append("-# " + " · ".join(foot))
    return clip(lines)


def title_top3(t):
    if t["cut"]:
        return [["Cut content — can't be obtained", "", ""]]
    out = []
    for w in t["ways"][:3]:
        rate = ""
        if w["dropRate"]:
            parts = [x.strip() for x in re.split(r"\r?\n", reformatPct(w["dropRate"])) if x.strip()]
            rate = parts[0] if len(parts) == 1 else "; ".join(parts)
        out.append([re.sub(r"\s*\n\s*", " ", w["how"] or w["source"] or "—"), rate, ""])
    return out


# ── legendary mods (website legendary_mods.json; not in Brewmaster) ──────────
def build_legmods(dist):
    path = os.path.join(dist, "legendary_mods.json")
    if not os.path.exists(path):
        return []
    out = []
    for g in json.load(open(path, encoding="utf-8")).get("groups") or []:
        for m in g.get("mods") or []:
            star = m.get("star")
            unique = g.get("id") == "unique"
            label = "Unique legendary mod" if unique else f"{star}★ Legendary mod"
            lines = [f"⭐ **{m['name']}**", f"{'Unique' if unique else f'{star}★'} · {' · '.join(m.get('types') or [])}"]
            if m.get("effect"):
                lines.append(m["effect"])
            routes = m.get("obtainRoutes") or []
            if routes:
                lines.append("**🔧 How to get it**")
                for r in routes:
                    lines.append(f"• {r['route']}" + (f" — {r['note']}" if r.get("note") else ""))
            lines.append("-# Legendary mod · Learn it by scrapping legendary items, then craft it at a Legendary Crafting station")
            out.append({"key": "l:" + m["id"], "kind": "legmod", "name": m["name"], "bare": m["name"], "label": label,
                        "thing": "legendary mod", "cut": False, "star": star,
                        "answer": {"lines": lines, "star": star, "types": m.get("types") or [], "effect": m.get("effect") or ""},
                        "top": [[r["route"], "", ""] for r in routes[:3]] or [["No known source yet", "", ""]]})
    return out


# ── unique weapons / armour (website unique_weapons_armour.json) ─────────────
def load_uniques(dist):
    path = os.path.join(dist, "unique_weapons_armour", "unique_weapons_armour.json")
    return json.load(open(path, encoding="utf-8")).get("items") or [] if os.path.exists(path) else []


def unique_stat_block(u):
    """The site's legendary stat block data (style-guide → Legendary Stat Block)."""
    ladder = []
    se = {s.get("star"): s for s in u.get("starEffects") or []}
    for star in range(1, 6):
        s = se.get(star)
        ladder.append("N/A" if not s else ("Random roll" if not s.get("fixed") else s.get("name") or "Random roll"))
    um = next((x for x in u.get("uniqueMods") or [] if x.get("kind") == "unique"), None)
    # "uwa" = just the fields window.dfbnbLegendary.fromUwa() reads, so the page
    # draws the site's own stat block (style-guide → Legendary Stat Block)
    uwa = {"starEffects": [{"star": s.get("star"), "name": s.get("name"), "fixed": s.get("fixed")} for s in u.get("starEffects") or []],
           "uniqueMods": [{"name": m.get("name"), "kind": m.get("kind"), "desc": m.get("desc") or "", "edid": m.get("edid") or ""}
                          for m in u.get("uniqueMods") or [] if m.get("kind") != "preinstalled"],
           "inherentEffect": u.get("inherentEffect") or "",
           "stats": {"minLevel": (u.get("stats") or {}).get("minLevel")},
           "obtainRoutes": [{"populated": True, "conditions": r.get("conditions") or [], "conditionsChecked": r.get("conditionsChecked")}
                            for r in u.get("obtainRoutes") or [] if r.get("populated")]}
    return {"uwa": uwa, "ladder": ladder, "um": um.get("name") if um else None,
            "ud": (um or {}).get("desc") or u.get("inherentEffect") or "",
            "ml": (u.get("stats") or {}).get("minLevel"), "tr": u.get("tradeable"),
            "base": u.get("base") or "", "weapon": u.get("kind") == "Weapon"}


def build_uniques(uniques, skip_names):
    """Uniques with NO plan_master row (the rest are answered from plan_master,
    the same rows Brewmaster answers from, with this stat block added)."""
    out = []
    for u in uniques:
        if u["name"].lower() in skip_names:
            continue
        is_w = u.get("kind") == "Weapon"
        groups, rows = [], []
        for r in u.get("obtainRoutes") or []:
            if not r.get("populated"):
                continue
            src = [l for l in r.get("lines") or [] if l.startswith("Source:")]
            if not src:
                continue
            title = LEDGER_TITLES.get(r["route"], r["route"])
            g_lines = []
            for l in src:
                body = re.sub(r"^Source:\s*", "", l)
                parts = re.split(r"\s+—\s+", body, maxsplit=1)
                name, rate = parts[0], (parts[1] if len(parts) > 1 else "")
                rate = reformatPct(rate) if rate else ""
                g_lines.append((name, rate))
                rows.append([name, rate, ""])
            g_lines.sort(key=lambda x: -_pnum(x[1]))
            groups.append([title, [f"• {n}" + (f" - {r}" if r else "") for n, r in g_lines]])
        if not groups and u.get("howToObtain"):
            groups.append(["🔧 How to get it", ["• " + u["howToObtain"]]])
            rows.append([u["howToObtain"], "", ""])
        rows = [r for r in rows if not r[0].startswith("Legacy")]
        rows.sort(key=lambda r: -_pnum(r[1]))
        name = u["name"]
        lines = [f"{'⚔️' if is_w else '🛡️'} **{name}**", f"Unique {'weapon' if is_w else 'armour'} · based on the {u.get('base') or '—'}"]
        if u.get("isCut"):
            lines.append("✕ This is cut content, it can't be obtained.")
        for title, gl in groups:
            lines.append(f"**{title}**")
            lines.extend(gl)
        foot = ["Unique weapon" if is_w else "Unique armour",
                "Tradeable" if u.get("tradeable") is True else "Not tradeable" if u.get("tradeable") is False else None]
        lines.append("-# " + " · ".join(f for f in foot if f))
        key = "u:" + compact(name)
        out.append({"key": key, "kind": "unique", "name": name, "bare": name,
                    "label": "Unique weapon" if is_w else "Unique armour",
                    "thing": "unique weapon" if is_w else "unique armour piece", "cut": bool(u.get("isCut")),
                    "answer": {"lines": lines, "lg": unique_stat_block(u)},
                    "top": rows[:3] or [["No known source yet", "", ""]]})
    return out


def _pnum(s):
    m = re.search(r"([\d.]+)\s*%", str(s or ""))
    return float(m.group(1)) if m else -1


# ── main ─────────────────────────────────────────────────────────────────────
def main():
    pm = json.load(open(os.path.join(DIST, "plan_master.json"), encoding="utf-8"))
    try:
        rep_of = load_rep_tiers()
    except Exception as e:                       # never fail the build over ranks
        print("[item finder] reputation ranks not loaded:", e)
        rep_of = None

    bugs = {}
    bp = os.path.join(DIST, "bugged_plans.json")
    if os.path.exists(bp):
        bd = json.load(open(bp, encoding="utf-8"))
        if (bd.get("channel") or "live") == "live":
            m = re.search(r"_([A-Za-z]+)_(\d{4})", str((bd.get("exports") or {}).get("BOOK") or ""))
            bugs["_label"] = f"{m.group(1).capitalize()} {m.group(2)}" if m else (bd.get("generated") or "latest")
            for it in bd.get("items") or []:
                if it.get("id"):
                    bugs[str(it["id"]).upper()] = {
                        "sev": it.get("severity") or "",
                        "why": next((b.get("summary") for b in it.get("bugs") or [] if b.get("summary")), ""),
                        "still": it.get("still_from") if isinstance(it.get("still_from"), list) else [],
                        "st": it.get("status") or ""}

    entries = []

    uniques = load_uniques(DIST)
    uniq_by_name = {u["name"].lower(): u for u in uniques}
    unique_plan_names = set()

    def add_plan(p):
        ans = {"lines": plan_answer(p, bugs)}
        label = p["category"] or ("Apparel" if p.get("apparel") else "Plan")
        if p.get("group") and p["group"].lower() not in p["name"].lower():
            label += " · " + p["group"]              # tells same-named mods apart
        thing, kind = thing_for_plan(p), "plan"
        u = uniq_by_name.get(p["name"].lower()) if p.get("uniqueVersion") else None
        if u:
            unique_plan_names.add(p["name"].lower())
            ans["lg"] = unique_stat_block(u)
            is_w = u.get("kind") == "Weapon"
            label = "Unique weapon" if is_w else "Unique armour"
            thing = "unique weapon" if is_w else "unique armour piece"
            kind = "unique"
        entries.append({"key": p["key"], "kind": kind, "name": p["name"], "bare": p["bare"],
                        "label": label, "thing": thing, "cut": bool(p.get("cut")),
                        "answer": ans, "top": plan_top3(p)})

    for x in pm.get("items") or []:
        if not x.get("name"):
            continue
        routes, ledger, cut = drop_cut_routes(add_rep(x.get("obtain_routes") or [], x.get("id"), rep_of), x.get("obtain_ledger") or [])
        add_plan({"key": "p:" + x["id"], "fid": re.sub(r"^[A-Z]+_", "", x["id"]).upper(), "name": x["name"], "bare": stripPrefix(x["name"]),
                  "category": x.get("category_label") or "", "tradeable": x.get("tradeable"), "cut": x.get("cut"),
                  "notObtainable": x.get("not_obtainable"), "obtain": x.get("obtain") or "", "routes": routes,
                  "unlocks": x.get("obtain_unlocks") or [], "ledger": ledger, "cutSources": cut, "nw": x.get("nw_route"),
                  "stopsDropping": x.get("stops_dropping"), "type": x.get("type") or "",
                  "planStatus": x.get("plan_status"), "statusChecked": x.get("status_checked"), "learnQuest": x.get("learn_quest"),
                  "uniqueVersion": x.get("unique_version"), "learnt": x.get("learnt"),
                  "group": x.get("weapon_group") or x.get("armour_group") or ""})

    npa_path = os.path.join(DIST, "no_plan_apparel.json")
    if os.path.exists(npa_path):
        for g in json.load(open(npa_path, encoding="utf-8")).get("groups") or []:
            for a in g.get("items") or []:
                if not a.get("name"):
                    continue
                routes, ledger, cut = drop_cut_routes(add_rep(a.get("obtain_routes") or [], a.get("id"), rep_of), a.get("obtain_ledger") or [])
                add_plan({"key": "a:" + a["id"], "fid": a["id"], "apparel": True, "name": a["name"], "bare": a["name"],
                          "category": "Apparel (no plan, drops ready-made)", "tradeable": a.get("tradeable"), "cut": a.get("cut"),
                          "notObtainable": a.get("not_obtainable"), "obtain": a.get("obtain") or "", "routes": routes,
                          "unlocks": a.get("obtain_unlocks") or [], "ledger": ledger, "cutSources": cut, "nw": None,
                          "stopsDropping": a.get("stops_dropping"), "type": "apparel-without-plans"})

    for t in build_titles(DIST):
        entries.append({"key": t["key"], "kind": "title", "name": t["name"], "bare": t["bare"], "label": t["label"],
                        "thing": "title", "cut": t["cut"], "answer": {"lines": title_answer(t)}, "top": title_top3(t)})
    entries.extend(build_legmods(DIST))
    entries.extend(build_uniques(uniques, unique_plan_names))

    # ── entitlements Brewmaster answers (data/brewmaster_entitlements.json) ──
    # Brewmaster builds these from the ENTM / GMRW / CHAL exports on the bot
    # laptop and commits the finished list here (game data only), so anything
    # he can answer in Discord the page can answer too. Same rules as his
    # search(): an entitlement linked to a plan answers AS that plan.
    plan_keys = {e["key"]: e for e in entries}
    aliases = []
    ent_path = os.environ.get("ITEM_FINDER_ENTM") or os.path.join(ROOT, "data", "brewmaster_entitlements.json")
    if os.path.exists(ent_path):
        try:
            ents = json.load(open(ent_path, encoding="utf-8")).get("items") or []
        except Exception as e:
            print("[item finder] brewmaster_entitlements.json not read:", e)
            ents = []
        for e in ents:
            name = str(e.get("name") or "").strip()
            if not name:
                continue
            linked = plan_keys.get(e.get("linked") or "")
            if linked:
                aliases.append((linked, name))        # "find X" → the plan's own answer
                continue
            srcs = [str(x) for x in e.get("sources") or [] if x]
            lines = [f"🎟️ **Entitlement: {name}**"]
            lines.extend(f"• {x}" for x in srcs) if srcs else lines.append("I don't have a source for this one yet.")
            if e.get("category"):
                lines.append(f"-# {e['category']}")
            entries.append({"key": "e:" + compact(name), "kind": "entm", "name": name, "bare": name,
                            "label": "Entitlement" + (f" · {e['category']}" if e.get("category") else ""),
                            "thing": "entitlement", "cut": False, "answer": {"lines": clip(lines)},
                            "top": [[x, "", ""] for x in srcs[:3]] or [["No known source yet", "", ""]]})
        print(f"[item finder] entitlements: {len(ents)} read, {len(aliases)} answer as their plan")

    # de-dupe keys (same plan listed twice keeps the first)
    seen, uniq = set(), []
    for e in entries:
        if e["key"] in seen:
            continue
        seen.add(e["key"])
        uniq.append(e)
    entries = uniq

    os.makedirs(OUT, exist_ok=True)
    shards = [dict() for _ in range(SHARDS)]
    index = []
    for e in entries:
        s = zlib.crc32(e["key"].encode("utf-8")) % SHARDS
        a = dict(e["answer"])
        a.update({"key": e["key"], "kind": e["kind"], "name": e["name"], "label": e["label"], "thing": e["thing"], "top": e["top"]})
        shards[s][e["key"]] = a
        index.append([e["key"], e["name"], norm(e["bare"]), compact(e["bare"]), e["kind"], e["label"], e["thing"], 1 if e["cut"] else 0, s])
    # extra search names (an entitlement's name pointing at its plan's answer)
    seen_alias = set()
    for e, name in aliases:
        k = (e["key"], compact(name))
        if k in seen_alias or compact(name) == compact(e["bare"]):
            continue
        seen_alias.add(k)
        index.append([e["key"], e["name"], norm(name), compact(name), e["kind"], e["label"], e["thing"],
                      1 if e["cut"] else 0, zlib.crc32(e["key"].encode("utf-8")) % SHARDS])

    # "generated" is a hash of the content, not the clock, so a rebuild with
    # nothing new writes identical files and the workflow has nothing to commit.
    # The website uses it to keep its cached answer files in step with the index.
    import hashlib
    h = hashlib.sha1(json.dumps([index, shards], ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    stamp = h
    with open(os.path.join(OUT, "index.json"), "w", encoding="utf-8") as fh:
        json.dump({"schema": SCHEMA, "generated": stamp, "shards": SHARDS,
                   "fields": ["key", "name", "n", "c", "kind", "label", "thing", "cut", "shard"],
                   "count": len(index), "items": index}, fh, ensure_ascii=False, separators=(",", ":"))
    for i, sh in enumerate(shards):
        with open(os.path.join(OUT, f"answers_{i:02d}.json"), "w", encoding="utf-8") as fh:
            json.dump({"schema": SCHEMA, "generated": stamp, "items": sh}, fh, ensure_ascii=False, separators=(",", ":"))
    kinds = {}
    for e in entries:
        kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
    print(f"[item finder] {len(index)} items {kinds} → {OUT}")


if __name__ == "__main__":
    main()
