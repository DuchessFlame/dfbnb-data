#!/usr/bin/env python3
r"""
build_personal_terminal_json.py — CAMP · Personal Terminal (/df/camp/personal-terminal/)

WHAT IT BUILDS
==============
dist/personal_terminal.json — everything the My Personal Terminal C.A.M.P. item
shows, read from the game's own TERM records:

  * Daily Quests       (My Daily Planner)  — one entry per friend, with the
                                             text for each appointment state
  * Alerts Page        (Automated Alerts)  — Charleston Herald alerts, their
                                             report text and coordinates button
  * My Personal Stats  (My Stats)          — every stat section, its stat rows
                                             and its sub-pages (fish lists, ...)

Nothing is hard-coded per entry. The builder starts at the terminal's home
screen and follows the menu links, so when Bethesda adds a fish, a friend, an
alert, a stat or a whole new stats section, it appears on the next build.

INPUT
=====
The newest TERM export written by
    xedit scripts\!!!Wordpress - ExportTERMToTSV.pas
    LIVE: tsv/TERM_Export_<Month>_<Year>.tsv
    PTS : tsv/pts/TERM_Export_PTS_<yyyy-mm-dd_hhnn>.tsv
resolved through tsv_source (chronological, channel-aware). The PTS workflow
normalizes tsv/pts into tsv/ first, so this script needs no PTS switch.

plus its companion  TERM_Export_..._Quests.tsv  (the quests the terminal's
conditions use). tools/export_term_from_esm.py writes the same two files
straight from SeventySix.esm when xEdit isn't open.

Alerts get a "What it does" block from those quests: the map marker the
coordinates button sets, the Pip-Boy objective it starts, and the quest or event
it leads to (e.g. Suspicious Person -> Bucket List, the camera quest).

If no TERM export exists the build exits 0 and leaves the committed JSON alone,
so a missing export can never blank the live page.

NEW PILLS
=========
tsv/personal_terminal_first_seen.json records the date each entry was first
built. Entries first seen within NEW_DAYS days are flagged "new": true. The
first run seeds every current entry as a baseline (not new). The patch build
already commits tsv/*_first_seen.json.

USAGE
=====
    python src/build_personal_terminal_json.py
    python src/build_personal_terminal_json.py --tsv tsv/TERM_Export_October_2026.tsv
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

OUT_DEFAULT = os.path.join(REPO, "dist", "personal_terminal.json")
LEDGER_DEFAULT = os.path.join(REPO, "tsv", "personal_terminal_first_seen.json")
EXPORT_PATTERN = "TERM_Export_*.tsv"
QUESTS_PATTERN = "TERM_Export_*_Quests.tsv"

HOME_EDID = "X01X_PlayerTerminal_Home"
NEW_DAYS = 30
BASELINE = "baseline"

# ANAM menu item types (TERM). 4 = opens a submenu, 8 = shows display text.
T_SUBMENU, T_DISPLAY = 4, 8

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


# ---------------------------------------------------------------- input ----
def _unescape(s: str) -> str:
    """Undo the .pas escaping: \\n newline, \\t tab, \\\\ backslash."""
    if not s or "\\" not in s:
        return s or ""
    out, i = [], 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            n = s[i + 1]
            out.append({"n": "\n", "t": "\t", "\\": "\\"}.get(n, "\\" + n))
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _item_type(v: str) -> int:
    v = (v or "").strip()
    if re.fullmatch(r"-?\d+", v):
        return int(v)
    lv = v.lower()
    if "submenu" in lv:
        return T_SUBMENU
    if "display" in lv:
        return T_DISPLAY
    return 0


def _hex8(v: str) -> str | None:
    v = (v or "").strip()
    if not v:
        return None
    m = re.search(r"([0-9A-Fa-f]{8})\]?\s*$", v) or re.search(r"\b([0-9A-Fa-f]{8})\b", v)
    return m.group(1).upper() if m else None


def _conds(raw: str) -> list:
    """'Func|Op|Value|ParamFormID|ParamEDID|ParamSig && ...' -> [dict]."""
    out = []
    for c in (raw or "").split(" && "):
        p = [x.strip() for x in c.split("|")]
        if len(p) < 3 or not p[0]:
            continue
        p += [""] * (6 - len(p))
        out.append({"func": p[0], "op": p[1], "value": p[2], "param": p[3].upper(),
                    "param_edid": p[4], "param_sig": p[5].upper()})
    return out


def load_quests(path: str | None) -> dict:
    """Companion _Quests.tsv -> {form_id: {edid, name, objectives[], log, referenced}}."""
    if not path or not os.path.exists(path):
        return {}
    out = {}
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            fid = _hex8(r.get("QUST_FormID"))
            if not fid:
                continue
            objs = [o.strip() for o in _unescape(r.get("Objectives") or "").split(" || ") if o.strip()]
            out[fid] = {"form_id": fid, "edid": (r.get("QUST_EDID") or "").strip(),
                        "name": _unescape(r.get("FULL") or "").strip(), "objectives": objs,
                        "log": _unescape(r.get("FirstLog") or "").strip(),
                        "referenced": (r.get("Referenced") or "").strip() == "1"}
    return out


def load_tsv(path: str) -> dict:
    """TERM export TSV -> {form_id: record} (raw dump shape)."""
    recs: dict[str, dict] = {}
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
        rd = csv.DictReader(f, delimiter="\t")
        need = {"TERM_FormID", "TERM_EDID", "Row", "Text"}
        missing = need - set(rd.fieldnames or [])
        if missing:
            raise SystemExit(f"[personal-terminal] {os.path.basename(path)} is missing columns {sorted(missing)}"
                             " - re-export with the current ExportTERMToTSV.pas")
        for r in rd:
            fid = _hex8(r.get("TERM_FormID"))
            if not fid:
                continue
            rec = recs.setdefault(fid, {"form_id": fid, "edid": (r.get("TERM_EDID") or "").strip(),
                                        "header": "", "body": [], "items": []})
            kind = (r.get("Row") or "").strip().upper()
            text = _unescape(r.get("Text") or "")
            conds = _conds(_unescape(r.get("Conditions") or ""))
            if kind == "HEADER":
                rec["header"] = text
            elif kind == "BODY":
                rec["body"].append({"text": text, "conds": conds})
            elif kind == "ITEM":
                rec["items"].append({"label": text, "type": _item_type(r.get("ItemType")),
                                     "submenu": _hex8(r.get("Target_FormID")),
                                     "display": _unescape(r.get("Display") or ""), "conds": conds})
    return recs


# ---------------------------------------------------------------- parse ----
BANNER = re.compile(r"^\s*(O\s*-[\s-]*O|-\s+~.*~\s+-|_{3,}|\|.*\||-{10,}.*|={2}\s*\[\[.*\]\]\s*={2})\s*$")
STAT = re.compile(r"^(.*?)\.{2,}\s*<STAT=([^>]+)>\s*$")
STATE = re.compile(r"^\s*==\s*(.+?)\s*==\s*$")
BANNER_TITLE = re.compile(r"^\s*-\s+~[\s~]*(.+?)[\s~!]*~\s+-\s*$")
FORMID_STAT = re.compile(r"^(.+?):\s*([0-9A-Fa-f]{8})$")
STATE_ORDER = ["READY TO MEET", "In Progress", "Done for Today", "All Completed"]


def _lines(t):
    return [l.rstrip() for l in (t or "").replace("\r", "").split("\n")]


def _clean(t):
    return [l for l in _lines(t) if l.strip() and not BANNER.match(l)]


def _banner_title(t):
    for l in _lines(t):
        m = BANNER_TITLE.match(l)
        if m:
            return m.group(1).strip(" ~!")
    return ""


def _stat_rows(t):
    rows = []
    for l in _lines(t):
        m = STAT.match(l.strip())
        if not m:
            continue
        label, stat = m.group(1).strip(), m.group(2).strip()
        row = {"id": "stat:" + stat, "label": label, "stat": stat}
        fm = FORMID_STAT.match(stat)
        if fm:
            row["stat_kind"], row["form_id"] = fm.group(1).strip(), fm.group(2).upper()
        rows.append(row)
    return rows


def _group_title(t, fallback):
    for l in _lines(t):
        m = re.match(r"^-{3,}\s*(.+?)\s*-{3,}$", l.strip())
        if m:
            return m.group(1)
    return fallback


def _text_lines(t):
    """Plain prose lines of a text block (banners, stat rows and dashes removed)."""
    return [l.strip() for l in _clean(t) if not STAT.match(l.strip()) and not STATE.match(l)]


class Terminal:
    def __init__(self, recs: dict, quests: dict | None = None):
        self.recs = recs
        self.quests = quests or {}
        self.by_edid = {r["edid"]: r for r in recs.values()}
        self.warnings: list[str] = []
        self.used: set[str] = set()

    def get(self, fid):
        r = self.recs.get((fid or "").upper())
        if fid and r is None:
            self.warnings.append(f"menu links to TERM {fid}, which is not in the export")
        if r:
            self.used.add(r["form_id"])
        return r

    # ---- Daily ------------------------------------------------------------
    def daily(self, rec):
        out = {"form_id": rec["form_id"], "edid": rec["edid"], "title": _banner_title(rec["body"][0]["text"]) if rec["body"] else "",
               "empty_text": "", "friends": []}
        seen = {}
        for it in rec["items"]:
            if it["type"] == T_DISPLAY and not it["submenu"]:
                out["empty_text"] = out["empty_text"] or it["display"].strip()
                continue
            if not it["submenu"]:
                continue
            fid = it["submenu"]
            if fid in seen:
                continue
            sub = self.get(fid)
            if not sub:
                continue
            name = it["label"].split(" - ")[0].strip() or _banner_title(sub["body"][0]["text"] if sub["body"] else "")
            states = []
            for b in sub["body"]:
                st, txt = None, []
                for l in _clean(b["text"]):
                    m = STATE.match(l)
                    if m and st is None:
                        st = m.group(1).strip()
                    else:
                        txt.append(l.strip())
                states.append({"state": st or "Info", "text": " ".join(txt).strip()})
            states.sort(key=lambda s: STATE_ORDER.index(s["state"]) if s["state"] in STATE_ORDER else 99)
            meet = next((i["label"] for i in sub["items"] if i["label"]), "")
            seen[fid] = True
            out["friends"].append({"id": "friend:" + sub["edid"], "name": name, "form_id": fid,
                                   "edid": sub["edid"], "meet_button": meet, "states": states})
        return out

    # ---- Alerts -----------------------------------------------------------
    @staticmethod
    def _parse_alert(text):
        report, review, quote = "", "", []
        for l in _clean(text):
            s = l.strip()
            if s.upper().startswith("REPORT:"):
                report = s[7:].strip()
            elif s.startswith("[") and s.endswith("]"):
                review = s[1:-1].strip()
            elif s.startswith("Automated Posting") or s.startswith("Coordinates have"):
                continue
            else:
                quote.append(s)
        return report, review, " ".join(quote).strip().strip('"').strip()

    # Quest text is player-facing only when it is not a [bracketed dev name]
    # and carries no unresolved <Alias=...> / <Variable=...> tokens.
    @staticmethod
    def _clean_text(s):
        return bool(s) and not s.startswith("[") and "<" not in s

    def what_it_does(self, conds_lists, coords):
        qids = []
        for conds in conds_lists:
            for c in conds:
                if c["param_sig"] == "QUST" and c["param"] and c["param"] not in qids:
                    qids.append(c["param"])
        qs = [self.quests[q] for q in qids if q in self.quests]
        out = {}
        if coords:
            m = re.match(r"^Download Coordinates\s*-\s*(.+)$", coords.strip("[] "), re.I)
            if m:
                out["marker"] = m.group(1).strip()
        pointer = next((q for q in qs if q["name"].startswith("[") and q["objectives"]), None)
        if pointer:
            out["objectives"] = [o for o in pointer["objectives"] if self._clean_text(o)]

        def stem(e):
            parts = e.split("_")
            if len(parts) > 1 and "misc" in parts[-1].lower():
                parts = parts[:-1]
            return "_".join(parts)

        lead = next((q for q in qs if self._clean_text(q["name"])), None)
        if not lead:
            stems = [stem(q["edid"]) for q in qs]
            fam = [q for q in self.quests.values() if self._clean_text(q["name"])
                   and any(q["edid"] == st or q["edid"].startswith(st + "_") for st in stems)]
            fam.sort(key=lambda q: (q["edid"] not in stems, len(q["edid"])))
            lead = fam[0] if fam else None
        if lead:
            name = lead["name"]
            kind = "Event" if name.lower().startswith("event:") else "Quest"
            out["leads_to"] = {"kind": kind, "name": re.sub(r"^Event:\s*", "", name), "edid": lead["edid"],
                               "summary": lead["log"] if self._clean_text(lead["log"]) else "",
                               "objectives": [o for o in lead["objectives"] if self._clean_text(o)][:4]}
        return out or None

    def alerts(self, rec):
        out = {"form_id": rec["form_id"], "edid": rec["edid"], "alerts": []}
        linked = set()
        for it in rec["items"]:
            if it["submenu"]:
                sub = self.get(it["submenu"])
                if not sub:
                    continue
                linked.add(sub["form_id"])
                rep, rev, note = self._parse_alert(sub["body"][0]["text"] if sub["body"] else "")
                bi = next((i for i in sub["items"] if i["label"]), None)
                btn = bi["label"] if bi else ""
                out["alerts"].append({"id": "alert:" + sub["edid"], "title": it["label"], "report": rep,
                                      "review": rev, "note": note, "coords": btn.strip("[] ") or None,
                                      "does": self.what_it_does([it["conds"], bi["conds"] if bi else []], btn),
                                      "in_menu": True, "form_id": sub["form_id"], "edid": sub["edid"]})
            elif it["type"] == T_DISPLAY:
                rep, rev, note = self._parse_alert(it["display"])
                slug = re.sub(r"[^a-z0-9]+", "-", it["label"].lower()).strip("-")
                out["alerts"].append({"id": "alert:" + rec["edid"] + ":" + slug, "title": it["label"], "report": rep,
                                      "review": rev, "note": note, "coords": None, "in_menu": True,
                                      "does": self.what_it_does([it["conds"]], ""),
                                      "form_id": None, "edid": None})
        # Alert terminals that exist but are not linked from the menu.
        prefix = rec["edid"] + "_"
        for r in sorted(self.recs.values(), key=lambda r: r["edid"]):
            if r["edid"].startswith(prefix) and r["form_id"] not in linked:
                self.used.add(r["form_id"])
                rep, rev, note = self._parse_alert(r["body"][0]["text"] if r["body"] else "")
                bi = next((i for i in r["items"] if i["label"]), None)
                btn = bi["label"] if bi else ""
                out["alerts"].append({"id": "alert:" + r["edid"], "title": rep.rstrip(".") or r["edid"], "report": rep,
                                      "review": rev, "note": note, "coords": btn.strip("[] ") or None,
                                      "does": self.what_it_does([bi["conds"] if bi else []], btn),
                                      "in_menu": False, "form_id": r["form_id"], "edid": r["edid"]})
        return out

    # ---- Stats ------------------------------------------------------------
    def _stat_page(self, rec, label, conds, depth=0):
        body = "\n".join(b["text"] for b in rec["body"])
        page = {"id": "section:" + rec["edid"], "section": label, "form_id": rec["form_id"], "edid": rec["edid"],
                "conditional": bool(conds), "rows": _stat_rows(body), "groups": []}
        for it in rec["items"]:
            if it["submenu"] and depth < 4:
                sub = self.get(it["submenu"])
                if sub:
                    child = self._stat_page(sub, it["label"], it["conds"], depth + 1)
                    rows = child["rows"] + [r for g in child["groups"] for r in g["rows"]]
                    page["groups"].append({"id": "group:" + sub["edid"], "name": it["label"], "rows": rows,
                                           "conditional": bool(it["conds"])})
            elif it["type"] == T_DISPLAY:
                page["groups"].append({"id": "group:" + rec["edid"] + ":" + it["label"],
                                       "name": it["label"],
                                       "rows": _stat_rows(it["display"]), "conditional": bool(it["conds"])})
        page["count"] = len(page["rows"]) + sum(len(g["rows"]) for g in page["groups"])
        return page

    def stats(self, rec):
        out = {"form_id": rec["form_id"], "edid": rec["edid"], "sections": []}
        for it in rec["items"]:
            if not it["submenu"]:
                continue
            sub = self.get(it["submenu"])
            if sub:
                out["sections"].append(self._stat_page(sub, it["label"], it["conds"]))
        return out

    # ---- Anything Bethesda adds to the home menu that isn't one of the three
    def generic(self, rec, label):
        entries = []
        for it in rec["items"]:
            sub = self.get(it["submenu"]) if it["submenu"] else None
            text = it["display"] if it["type"] == T_DISPLAY else ("\n".join(b["text"] for b in sub["body"]) if sub else "")
            entries.append({"id": "entry:" + rec["edid"] + ":" + it["label"], "title": it["label"],
                            "lines": _text_lines(text), "rows": _stat_rows(text)})
        return {"form_id": rec["form_id"], "edid": rec["edid"], "title": label,
                "intro": _text_lines("\n".join(b["text"] for b in rec["body"])), "entries": entries}


def classify(label: str, edid: str) -> str:
    s = (edid + " " + label).lower()
    if "daily" in s or "planner" in s:
        return "daily"
    if "alert" in s:
        return "alerts"
    if "stat" in s:
        return "stats"
    return "other"


def build(recs: dict, quests: dict | None = None) -> dict:
    t = Terminal(recs, quests)
    home = t.by_edid.get(HOME_EDID)
    if not home:
        raise SystemExit(f"[personal-terminal] {HOME_EDID} not found in the export")
    t.used.add(home["form_id"])
    out = {"home": {"form_id": home["form_id"], "edid": home["edid"], "menu": []},
           "daily": None, "alerts": None, "stats": None, "other": []}
    for it in home["items"]:
        if not it["submenu"]:
            continue
        rec = t.get(it["submenu"])
        if not rec:
            continue
        kind = classify(it["label"], rec["edid"])
        out["home"]["menu"].append({"label": it["label"], "kind": kind, "form_id": rec["form_id"]})
        if kind == "daily" and out["daily"] is None:
            out["daily"] = t.daily(rec)
        elif kind == "alerts" and out["alerts"] is None:
            out["alerts"] = t.alerts(rec)
        elif kind == "stats" and out["stats"] is None:
            out["stats"] = t.stats(rec)
        else:
            out["other"].append(t.generic(rec, it["label"]))
    for k in ("daily", "alerts", "stats"):
        if out[k] is None:
            t.warnings.append(f"home menu has no {k} page")
    unused = sorted(r["edid"] for r in recs.values()
                    if r["form_id"] not in t.used and r["edid"].startswith("X01X_PlayerTerminal"))
    if unused:
        t.warnings.append("player-terminal records not reachable from the menu: " + ", ".join(unused))
    out["_warnings"] = t.warnings
    return out


# ------------------------------------------------------------- ledger ------
def _all_ids(d):
    ids = []
    if d.get("daily"):
        ids += [f["id"] for f in d["daily"]["friends"]]
    if d.get("alerts"):
        ids += [a["id"] for a in d["alerts"]["alerts"]]
    if d.get("stats"):
        for s in d["stats"]["sections"]:
            ids.append(s["id"])
            ids += [r["id"] for r in s["rows"]]
            for g in s["groups"]:
                ids.append(g["id"])
                ids += [r["id"] for r in g["rows"]]
    for o in d.get("other", []):
        ids += [e["id"] for e in o["entries"]]
    return ids


def apply_new(d, ledger_path, today):
    try:
        ledger = json.load(open(ledger_path, encoding="utf-8"))
    except (OSError, ValueError):
        ledger = None
    seeding = not ledger
    ledger = ledger or {}
    for i in _all_ids(d):
        if i not in ledger:
            ledger[i] = BASELINE if seeding else today.isoformat()

    def is_new(i):
        v = ledger.get(i)
        if not v or v == BASELINE:
            return False
        try:
            return (today - datetime.date.fromisoformat(v)).days <= NEW_DAYS
        except ValueError:
            return False

    def mark(o):
        if is_new(o["id"]):
            o["new"] = True

    n = 0
    if d.get("daily"):
        for f in d["daily"]["friends"]:
            mark(f)
    if d.get("alerts"):
        for a in d["alerts"]["alerts"]:
            mark(a)
    if d.get("stats"):
        for s in d["stats"]["sections"]:
            mark(s)
            for r in s["rows"]:
                mark(r)
            for g in s["groups"]:
                mark(g)
                for r in g["rows"]:
                    mark(r)
    for o in d.get("other", []):
        for e in o["entries"]:
            mark(e)
    n = sum(1 for i in _all_ids(d) if is_new(i))
    os.makedirs(os.path.dirname(ledger_path), exist_ok=True)
    with open(ledger_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(dict(sorted(ledger.items())), f, indent=1, ensure_ascii=False)
        f.write("\n")
    return n, seeding


# --------------------------------------------------------------- main ------
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tsv", help="TERM export to read (default: newest via tsv_source)")
    ap.add_argument("--quests", help="companion _Quests.tsv (default: next to --tsv, else newest)")
    ap.add_argument("--out", default=OUT_DEFAULT)
    ap.add_argument("--ledger", default=LEDGER_DEFAULT)
    ap.add_argument("--today", help="override today's date (YYYY-MM-DD) for the NEW window")
    a = ap.parse_args(argv)

    today = datetime.date.fromisoformat(a.today) if a.today else datetime.date.today()
    path, qpath = a.tsv, a.quests
    if not path:
        import tsv_source
        # The companions (_Quests, _Placements) match the same glob; never pick one as the main file.
        path = tsv_source.newest(EXPORT_PATTERN, exclude=["Quests", "Placements", "Probe"], required=False)
    if not path or not os.path.exists(path):
        print(f"[personal-terminal] no {EXPORT_PATTERN} export yet - keeping the committed "
              f"{os.path.relpath(a.out, REPO)} unchanged.")
        return 0
    if not qpath:
        sib = path[:-4] + "_Quests.tsv"
        if os.path.exists(sib):
            qpath = sib
        else:
            import tsv_source
            qpath = tsv_source.newest(QUESTS_PATTERN, required=False)
    recs, source = load_tsv(path), os.path.basename(path)
    quests = load_quests(qpath)
    if not quests:
        print("[personal-terminal] no _Quests.tsv companion - alerts will have no 'What it does' block")

    d = build(recs, quests)
    warnings = d.pop("_warnings")
    n_new, seeded = apply_new(d, a.ledger, today)

    stats = d["stats"]["sections"] if d.get("stats") else []
    totals = {"friends": len(d["daily"]["friends"]) if d.get("daily") else 0,
              "alerts": len(d["alerts"]["alerts"]) if d.get("alerts") else 0,
              "stat_sections": len(stats), "stats": sum(s["count"] for s in stats), "new": n_new}
    if totals["stats"] == 0 and totals["friends"] == 0 and totals["alerts"] == 0:
        raise SystemExit("[personal-terminal] built an empty terminal - refusing to overwrite the page")

    payload = {"page": "/df/camp/personal-terminal/", "title": "CAMP - Personal Terminal",
               "source": source, "built": today.isoformat(), "totals": totals, **d}
    if warnings:
        payload["warnings"] = warnings
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=1, ensure_ascii=False)
        f.write("\n")
    print(f"[personal-terminal] {source} -> {os.path.relpath(a.out, REPO)}  {json.dumps(totals)}"
          + ("  (first-seen ledger seeded)" if seeded else ""))
    for w in warnings:
        print("  ! " + w)
    return 0


if __name__ == "__main__":
    sys.exit(main())
