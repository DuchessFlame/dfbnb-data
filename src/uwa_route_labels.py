"""
uwa_route_labels.py — last pass over the Unique Weapons & Armour routes:
player-facing names for what the earlier steps found (Oct 2026, Duchess).

Everything is read from the game files each build; nothing here is a list of
items or names.

1. Raids: "Raids - Scorchtongue Module" is the stage's editor name. The game's
   own challenges name each encounter ("Gleaming Depths - Defeat Ultracite
   Terror"), and the raid quest names the raid ("Raid: Gleaming Depths"), so the
   line reads "Raid: Gleaming Depths - Ultracite Terror".

2. Raid plans: a raid stage's reward list holds the unique AND its plan
   (RD01_LLS_Raids_Rewards_Enc02_Weapons_DrillFist). uwa_entries gives the
   plan its own entry, rated on (stage list, plan BOOK).

3. Event types: every Events & Activities source that is a quest says what
   kind it is, the way the quest's own title does — "Public Event: Eviction
   Notice", "Activity: Fly Swatter", "Enclave Activity: Bots on Parade". Events
   with a Seasonal Events page on the site (dist/seasonal_events) read
   "Seasonal Event: …".

4. Scoreboard / Atom Shop weapons are crafting unlocks. Their entitlement says
   so ("ITEM IS CRAFTABLE AT WEAPON WORKBENCHES"), so those routes say
   "Unlocks crafting (Weapon Workbench)", not "Ready-made weapon".
"""
import json
import re
from collections import defaultdict
from pathlib import Path

import plan_sources

_REF = re.compile(r"^([0-9A-Fa-f]{8}):([^:]*):([A-Z_]{4})$")
_RAID_GMRW = re.compile(r"^(RD\d+)_QuestReward_Enc(\d+)$", re.I)
_DEAD = re.compile(r"(^|_)(zzz|CUT|DONOTUSE|DEL|DEPRECATED|TEST|Debug)(_|$|\d)|^zzz|^DEL_", re.I)
_ALIAS = re.compile(r"\s*-?\s*<Alias=[^>]*>\s*$")
_BENCH = re.compile(r"CRAFTABLE AT ([A-Z']+) WORKBENCH", re.I)


def _q(v):
    return str(v or "").strip().strip('"').strip()


def _f(v):
    v = _q(v).split(":")[0]
    return v.upper().zfill(8) if v else ""


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", re.sub(r"^\s*the\s+", "", str(s or "").lower()))


class Labels:
    def __init__(self, pick, read_tsv, repo_root):
        self.pick, self.read_tsv, self.root = pick, read_tsv, Path(repo_root)
        self._load()

    def _rows(self, pattern, exclude=None):
        p = self.pick(pattern, exclude=exclude) if exclude else self.pick(pattern)
        return self.read_tsv(p) if p else []

    def _load(self):
        # --- leveled-list children (for "is the plan under this stage?")
        self.kids = defaultdict(set)
        for r in self._rows("LVLI_Export_*_LVLI_Entries.tsv"):
            m = _REF.match(_q(r.get("LVLO_Reference")))
            if m:
                self.kids[_f(r.get("LVLI_FormID"))].add(_f(m.group(1)))

        # --- plan BOOKs by name (an item whose plan the earlier steps did not
        # link: Flatliner's is RD01_Recipe_Weapon_Ranged_StrikeBreaker)
        self.book = {}
        for r in self._rows("BOOK_Export_*.tsv", exclude="Locations"):
            ed, full = _q(r.get("EDID") or r.get("BOOK_EDID")), _q(r.get("FULL") or r.get("BOOK_FULL"))
            fid = _f(r.get("FormID") or r.get("BOOK_FormID"))
            if full.lower().startswith("plan:") and fid and not _DEAD.search(ed):
                self.book.setdefault(_norm(full[5:]), set()).add(fid)

        # --- quests: title kind, and the raid names
        self.kind = {}                    # bare title (normalised) -> shown title
        raid_name = {}                    # RD01 -> "Gleaming Depths"
        quests = [(_q(r.get("EDID")), _q(r.get("FULL - Name")), _q(r.get("Quest Type")))
                  for r in self._rows("QUEST_Export_*.tsv")]
        quests += [(_q(r.get("EDID")), _q(r.get("FULL")), _q(r.get("QuestType")))
                   for r in self._rows("QUST2_Export_*_Quests.tsv")]
        for ed, full, qtype in quests:
            full = _ALIAS.sub("", full)
            if not full or "<" in full or "[" in full or _DEAD.search(ed):
                continue
            m = re.match(r"^(Event|Activity|Enclave Activity|Daily|Raid)\s*:\s*(.+)$", full)
            if not m:
                # A story quest found by the Events walk (Oathbreaker:
                # "Over and Out") says it is a quest.
                if (full and ":" not in full and not _DEAD.search(ed)
                        and qtype.lower() in ("primary", "secondary", "side quest")):
                    self.kind.setdefault(_norm(full), f"Quest: {full}")
                continue
            pre, bare = m.group(1), m.group(2).strip()
            if pre == "Raid":
                code = ed.split("_", 1)[0]
                raid_name.setdefault(code.upper(), bare)
                continue
            if pre == "Event" and qtype.lower() == "public event":
                pre = "Public Event"
            self.kind.setdefault(_norm(bare), f"{pre}: {bare}")
        # Seasonal events: the ones the site has a Seasonal Events page for.
        sp = self.root / "dist" / "seasonal_events" / "seasonal_events_rewards_by_page.json"
        try:
            pages = json.loads(sp.read_text(encoding="utf-8")).get("byPage", {})
        except (OSError, ValueError):
            pages = {}
        for key, v in pages.items():
            if key.startswith("/df/seasonal-events/") and key.rstrip("/").endswith("-all-rewards"):
                nm = _q((v or {}).get("name"))
                if nm:
                    self.kind[_norm(nm)] = f"Seasonal Event: {nm}"

        # --- raid encounters, named by the game's own challenges (shared with
        # the plan pages: plan_sources.raid_encounter_names)
        enc = plan_sources.raid_encounter_names(
            [(e, f) for e, f, _t in quests],
            [(_q(r.get("EDID")), _q(r.get("FULL"))) for r in self._rows("CHAL_Export_*.tsv")])
        self.raid = {}                     # "Raids - Scorchtongue Module" -> (label, [reward lists])
        stage_lists = defaultdict(set)
        stage_name = {}
        for r in self._rows("GMRW_Export_*.tsv"):
            m = _RAID_GMRW.match(_q(r.get("EDID")))
            disp = _q(r.get("ParentQuestDisplay"))
            if not m or not disp:
                continue
            key = (m.group(1).upper(), int(m.group(2)))
            stage_name[key] = disp
            item = _q(r.get("RewardedItem"))
            if item.endswith(":LVLI"):
                stage_lists[key].add(_f(item))
        for key, disp in stage_name.items():
            if key in enc:
                raid, who = enc[key]
                self.raid[f"Raids - {disp}"] = (f"Raid: {raid} - {who}", stage_lists[key])
            elif raid_name.get(key[0]):
                self.raid[f"Raids - {disp}"] = (f"Raid: {raid_name[key[0]]} - {disp}", stage_lists[key])

        # --- entitlements that unlock crafting
        self.craft = {}                    # (norm name, "Scoreboard"/"Atom Shop") -> bench
        for r in self._rows("ENTM_Export_*.tsv"):
            ed = _q(r.get("EDID"))
            # _ENTM_Skin_WeaponModel_: a whole weapon sold as a "skin" (The
            # Invader - "WEAPON IS CRAFTABLE AT WEAPON WORKBENCHES").
            if _DEAD.search(ed) or not re.search(r"_ENTM_(Weapons?|Armor|Armour|Skin_WeaponModel)_", ed, re.I):
                continue
            b = _BENCH.search(_q(r.get("DESC")))
            if not b:
                continue
            route = ("Scoreboard" if re.match(r"^SCORE_", ed, re.I) else
                     "Atom Shop" if re.match(r"^ATX_", ed, re.I) else None)
            if not route:
                continue
            bench = {"WEAPON": "Weapon Workbench", "TINKER": "Tinker's Workbench",
                     "TINKER'S": "Tinker's Workbench", "ARMOR": "Armor Workbench",
                     "ARMOUR": "Armor Workbench"}.get(b.group(1).upper(),
                                                      b.group(1).title() + " Workbench")
            names = {re.sub(r"\s+Crafting$", "", _q(r.get("FULL")), flags=re.I),
                     re.split(r"_ENTM_(?:Weapons?|Armor|Armour)_", ed, flags=re.I)[-1]}
            for n in names:
                if _norm(n):
                    self.craft.setdefault((_norm(n), route), bench)

    def _under(self, top_lists, target):
        seen, stack = set(), list(top_lists)
        while stack:
            f = stack.pop()
            if f in seen:
                continue
            seen.add(f)
            kids = self.kids.get(f, ())
            if target in kids:
                return True
            stack.extend(kids)
        return False


def _events_out_of_quests(it, changed):
    """The other way round: "Event: Encryptid" filed under Quests (The Fixer's
    plan) belongs in Events & Activities."""
    _move(it, changed, "Quests", "Events & Activities",
          r"^Source: (Event|Public Event|Activity|Seasonal Event): ",
          lambda l: re.sub(r"^Source: Event: ", "Source: Public Event: ", l),
          "event moved out of Quests")


def _quests_out_of_events(it, changed):
    _move(it, changed, "Events & Activities", "Quests", r"^Source: (Quest|Daily): ",
          lambda l: re.sub(r"^Source: Quest: ", "Source: ", l), "quest moved out of Events")


def _move(it, changed, src_route, dst_route, rx, rename, tag):
    """A quest or a daily is not an event (Duchess, 24 Oct 2026: Oathbreaker
    showed "Daily: Heart of the Enemy" and "Quest: Over and Out" under Events
    & Activities). Their Source lines move to a Quests entry - "Quest: " is
    dropped there, the way the Quests row already reads ("Tracking Unknowns",
    "Daily: Waste Not") - keeping their Drops line, conditions, tradeable pill
    and the lists behind them (uwa_rates)."""
    routes = it.get("obtainRoutes") or []
    moved = []                         # (source route, lines to move)
    for r in routes:
        if r.get("route") != src_route or not r.get("populated"):
            continue
        go = [l for l in r["lines"] if re.match(rx, l)]
        if go:
            moved.append((r, go))
    for r, go in moved:
        lp = r.get("_linePaths") or {}
        drops = next((l for l in r["lines"] if l.startswith("Drops:")), "")
        new_lines, new_lp = [], {}
        moved_paths = []
        for l in go:
            nl = rename(l)
            new_lines.append(nl)
            if l in lp:
                new_lp[nl] = lp.pop(l)
                moved_paths += [p for p in new_lp[nl] if p not in moved_paths]
        # the lists behind the moved lines go with them, so drop conditions
        # (read after this step) belong to the right entry
        if r.get("paths") is not None and moved_paths:
            keep = [p for p in r["paths"] if p not in moved_paths]
            r["paths"] = keep
        r["lines"] = [l for l in r["lines"] if l not in go]
        if not any(l.startswith("Source:") for l in r["lines"]):
            # nothing left under Events: the entry itself is the quest's
            r["lines"] = [l for l in r["lines"] if not l.startswith(("Drops:", "Plan:"))]
            if not r["lines"]:
                r.update({"populated": False, "lines": [], "tradeable": None, "dropRate": None})
                r.pop("conditions", None)
        rest = [l for l in r["lines"] if not l.startswith(("Drops:", "Source:"))] \
            if r.get("populated") else []
        q_idx = [i for i, x in enumerate(routes) if x.get("route") == dst_route]
        same = next((routes[i] for i in q_idx if routes[i].get("populated")
                     and drops in routes[i]["lines"]), None)
        if same is not None:
            at = max(i for i, l in enumerate(same["lines"]) if l.startswith(("Drops:", "Source:")))
            same["lines"][at + 1:at + 1] = [l for l in new_lines if l not in same["lines"]]
            same.setdefault("_linePaths", {}).update(new_lp)
            same.setdefault("paths", []).extend(p for p in moved_paths if p not in same["paths"])
        else:
            entry = {"route": dst_route, "populated": True,
                     "lines": [drops] + new_lines + [l for l in rest if l.startswith("Plan:")],
                     "tradeable": r.get("tradeable"), "dropRate": r.get("dropRate") or "",
                     "conditions": list(r.get("conditions") or []),
                     "conditionsChecked": r.get("conditionsChecked", True),
                     "_linePaths": new_lp, "paths": list(moved_paths)}
            blank = next((i for i in q_idx if not routes[i].get("populated")), None)
            if blank is not None:
                routes[blank] = entry
            else:
                routes.insert(max(q_idx) + 1 if q_idx else len(routes), entry)
        changed[tag].append(f"{it['name']}: {', '.join(new_lines)}")


def apply(items, labels, log=print):
    changed = defaultdict(list)
    for it in items:
        renames = {}
        names = [it.get("name")] + list(it.get("aka") or []) + [it.get("modOf")]
        for r in it.get("obtainRoutes") or []:
            if not r.get("populated"):
                continue
            lines = r["lines"]
            lp = r.get("_linePaths") or {}

            def _moved(old_line, new_line):
                # a renamed source line keeps the lists behind it (uwa_rates)
                if old_line in lp and old_line != new_line:
                    lp.setdefault(new_line, []).extend(lp.pop(old_line))
            for i, l in enumerate(lines):
                if not l.startswith("Source: "):
                    continue
                src = l[8:]
                # 1 + 2. raids
                if src in labels.raid:
                    new, tops = labels.raid[src]
                    lines[i] = f"Source: {new}"
                    _moved(l, lines[i])
                    renames[src] = new
                    renames[src.split(" - ", 1)[1]] = new     # summary says "Event: Drill Complex Module"
                    changed["raid name"].append(f"{it['name']}: {new}")
                    # the plan's own entry is made by uwa_entries (17 Oct 2026)
                    continue
                # 3. event kind
                if r.get("route") != "Events & Activities":
                    continue
                head, tail = re.match(r"^(.*?)(\s*\(.*\))?$", src).groups()
                if re.match(r"^[A-Za-z ]+:\s", head):
                    continue                      # already says what it is
                shown = labels.kind.get(_norm(head))
                if shown and shown != head:
                    lines[i] = f"Source: {shown}{tail or ''}"
                    _moved(l, lines[i])
                    renames[head] = shown
                    changed["event kind"].append(f"{it['name']}: {shown}")
            # 4. crafting unlocks
            if r.get("route") in ("Scoreboard", "Atom Shop"):
                bench = next((labels.craft[(_norm(n), r["route"])] for n in names
                              if n and (_norm(n), r["route"]) in labels.craft), None)
                if bench:
                    for j, d in enumerate(lines):
                        if d.startswith("Drops: Ready-made "):
                            lines[j] = f"Drops: Unlocks crafting ({bench})"
                            changed["crafting unlock"].append(f"{it['name']} ({r['route']})")
            # the same source can now appear twice ("Breach and Clear" +
            # "Activity: Breach and Clear")
            r["lines"] = list(dict.fromkeys(lines))
        _quests_out_of_events(it, changed)
        _events_out_of_quests(it, changed)
        # one-line summary and refs follow the new names
        for old, new in renames.items():
            for k in ("howToObtain", "sourceRef"):
                v = it.get(k)
                if not v or new in v:
                    continue
                v = re.sub(rf"(^|/ )(?:Event: |Score: |Source: )?{re.escape(old)}(?= /|$| \()",
                           lambda m: m.group(1) + new, v)
                it[k] = v
    for k, v in changed.items():
        log(f"  route labels — {k}: {len(v)}")
        for x in sorted(set(v))[:60]:
            log(f"    {x}")
    return changed


def quest_availability(items, tsv_dir, log=print):
    """A quest/event source that only runs during a season or event says so
    (Duchess, 24 Oct 2026: "new players are gonna be like what is Disturbed
    Grave and think they can do it all the time"). The text is the quest's own
    log line "(This quest is only available ...)", read by
    plan_quest_guides.Guides - the same reader the plan pages use."""
    try:
        import plan_quest_guides as pqg
        g = pqg.Guides(tsv_dir)
    except Exception as exc:                             # noqa: BLE001
        log(f"  quest availability: skipped ({exc})")
        return 0
    if not g.ok:
        return 0
    n = 0
    for it in items:
        for r in it.get("obtainRoutes") or []:
            if not r.get("populated"):
                continue
            notes = []
            for l in r["lines"]:
                if not l.startswith("Source: "):
                    continue
                src = re.sub(r"\s+—\s+[\d.]+%.*$", "", l[8:])
                fids = set()
                for key in {pqg.route_key(src),
                            pqg.route_key(re.sub(r"^[A-Za-z ]+:\s+", "", src))}:
                    for fid in g.by_name.get(key, ()):
                        fids.add(fid)
                    # quest titles keep their kind ("Repeatable: Disturbed Grave")
                for fid, q in g.quests.items():
                    if q["name"] and pqg.route_key(q["name"]) in {pqg.route_key(src)}:
                        fids.add(fid)
                avs = {g.availability(f) for f in fids if f in g.quests} - {None}
                if len(avs) == 1:
                    av = avs.pop().rstrip(".")
                    note = f"Note: {src} — {av[0].lower() + av[1:]}."
                    if note not in r["lines"] and note not in notes:
                        notes.append(note)
            if notes:
                r["lines"] += notes
                n += 1
    log(f"  quest availability notes: {n} route(s)")
    return n
