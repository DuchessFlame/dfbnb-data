"""Does a unique's one-line source actually hand that unique out?

Added 12 Oct 2026. The old hand-built unique list (dist/unique_weapons/
unique_weapons.json) carries a "Quest: …" / "Event: …" line for most items,
and the build trusted any line that was not blank or vague. Some were copied
from the BASE weapon (Meadow Breeze Sprayer and Prototype ABX03: "Shadow over
Appalachia", a Plasma Gun line) and some were simply out of date.

The test, read from the game files every build: find the named quest in the
QUEST exports, take everything its GMRW rewards hand out and every leveled
list below them, and see whether that reaches the unique's OWN leveled list
(LL_Weapon_Ranged_EldersMark …). Reached = linked. The quest exists but never
reaches the list = CONTRADICTED: the files say that quest does not give it.

Only a contradiction is acted on. A name the files do not know as a quest
(event pages, "Skyline Valley (Vault 63)", raids) or an item with no own
list proves nothing either way and is left alone — the 11 Oct 2026 lesson
was that guessing over a hand line renames good sources.
"""
import re
from collections import defaultdict


def _n(s):
    s = re.sub(r"^\s*(Event|Quest|Daily|Activity)\s*:\s*", "", (s or "").strip(), flags=re.I)
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)       # "(Vault 79)", "(Brotherhood of Steel)"
    return re.sub(r"[^a-z0-9]", "", s.lower())


class SourceLinks:
    def __init__(self, pick, read_tsv, strip_q, norm_formid):
        self.qfid = defaultdict(set)          # quest name -> quest FormIDs
        self.qedid = defaultdict(set)         # quest name -> quest EDIDs
        for pat, col in (("QUEST_Export_*.tsv", "FULL - Name"),
                         ("QUST2_Export_*_Quests.tsv", "FULL")):
            p = pick(pat)
            for r in (read_tsv(p) if p else []):
                full = strip_q(r.get(col, ""))
                if full:
                    self.qfid[_n(full)].add(norm_formid(r.get("FormID", "")).upper())
                    self.qedid[_n(full)].add(strip_q(r.get("EDID", "")))
        self.gives = defaultdict(set)         # quest FormID -> rewarded FormIDs
        p = pick("GMRW_Export_*.tsv")
        for r in (read_tsv(p) if p else []):
            item = strip_q(r.get("RewardedItem", "")).split(":")[0].upper()
            if not item:
                continue
            qs = {strip_q(r.get("ParentQuestLink", "")).split(":")[0].upper()}
            for k, v in r.items():
                v = strip_q(v or "")
                if (k or "").startswith("Ref") and v.endswith(":QUST"):
                    qs.add(v.split(":")[0].upper())
            for q in qs - {""}:
                self.gives[q].add(item)
        self.entries = defaultdict(set)
        self.ed2fid = {}
        p = pick("LVLI_Export_*_LVLI_Entries.tsv")
        for r in (read_tsv(p) if p else []):
            lf = norm_formid(r.get("LVLI_FormID", "")).upper()
            self.ed2fid.setdefault(strip_q(r.get("LVLI_EDID", "")), lf)
            ref = strip_q(r.get("LVLO_Reference", "")).split(":")[0].upper()
            if ref:
                self.entries[lf].add(ref)
        # A quest can also hold a list directly (an alias or script hands it
        # over, no GMRW): Meteorite Sword's list is referenced by
        # MTR05_Mother_Breach, "Activity: Breach and Clear".
        p = pick("LVLI_Export_*_LVLI_Refs.tsv")
        for r in (read_tsv(p) if p else []):
            lf = norm_formid(r.get("LVLI_FormID", "")).upper()
            for k, v in r.items():
                v = strip_q(v or "")
                if (k or "").startswith("Ref") and v.endswith(":QUST"):
                    self.gives[v.split(":")[0].upper()].add(lf)
        self._reach = {}

    def _closure(self, qname):
        if qname not in self._reach:
            seen = set()
            todo = [i for f in self.qfid.get(qname, ()) for i in self.gives.get(f, ())]
            seen.update(todo)
            while todo:
                x = todo.pop()
                for y in self.entries.get(x, ()):
                    if y not in seen:
                        seen.add(y)
                        todo.append(y)
            self._reach[qname] = seen
        return self._reach[qname]

    def check(self, how, own_list):
        """{'linked': [...], 'contradicted': [...], 'unknown': [...]} for the
        names in a one-line source, tested against the item's own list."""
        out = {"linked": [], "contradicted": [], "unknown": []}
        if not re.match(r"^\s*(Quest|Event)\s*:", how or "") or "legacy" in (how or "").lower():
            return out
        lf = self.ed2fid.get(own_list or "")
        for name in [p.strip() for p in re.split(r"\s*/\s*", how.split(":", 1)[1]) if p.strip()]:
            q = _n(name)
            if not lf or q not in self.qfid:
                out["unknown"].append(name)
            elif lf in self._closure(q):
                out["linked"].append(name)
            # The roster list can carry the event's own code (Cursed
            # Broadsider: E06_Colossus_LLS_…, A Colossal Problem is E06_…),
            # which Duchess confirmed in game: that is a link too.
            elif any(own_list.split("_")[0] == e.split("_")[0] for e in self.qedid[q] if e):
                out["linked"].append(name)
            else:
                out["contradicted"].append(name)
        return out
