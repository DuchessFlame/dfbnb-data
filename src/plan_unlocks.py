#!/usr/bin/env python3
r"""
plan_unlocks.py — COBJ.GNAM, Bethesda's own "what unlocks this recipe" field.

WHY THIS EXISTS
---------------
Kevin, 17 Sep 2026, on the Pint-Sized Slasher radio and fishing bobber being
published as cut content:

    "those were the book forms. I disabled them because the cobj is taught
     directly by completing the respective challenge."

He is right, and the pipeline had no way to know it. Every question this repo
asks about a plan — is it cut, where does it drop, what teaches it — has been
asked of the **BOOK** record, the physical plan item you find and trade. But the
thing a player actually ends up owning is the **COBJ**, the recipe. A BOOK is
only one of the ways a COBJ gets taught, and when Bethesda teaches a recipe
some other way they disable the BOOK and leave it behind with a `zzz_` prefix.

`cut_reason()` then sees a dev prefix with no references and calls it cut. The
recipe is live; its plan book is the leftover.

COBJ carries a column that answers this outright. **GNAM is the record that
unlocks the recipe** and it is not always a plan:

    COBJ SDOW_co_Fishing_..._RodBobber_SlasherBobber
      CNAM Pint-Sized Slasher Bobber
      GNAM SDOW_Challenge_Lifetime_Collect_SlasherClue_03
           "(Seasonal) Collect Even More Pint-Sized Slasher Masks"

That is the game stating the route in its own data. On the September 2026 live
export 5,275 of 14,765 recipes carry a GNAM, and they break down as:

    3,101  a plan BOOK teaches it            <- the ordinary case
    1,429  an item you own teaches it        <- weapon mods, crop seeds
      407  recipe_Dummy_Uncraftable_Item_NOCRAFT
      190  recipe_Dummy_LearnedViaScriptRecipes
       99  a CHALLENGE teaches it            <- the class that was invisible
        9  recipe_Dummy_LearnedViaWorkshopClaim

The standing rule applies: when the game files carry Bethesda's own
authoritative list of something, build from it rather than inferring membership
from names or patterns. This module reads that column and nothing else infers
what it says.

WHAT COUNTS AS PROOF OF LIFE
----------------------------
Only `challenge` and `workshop-claim`. Both name an action a player performs,
so they settle the question `cut_reason()` is asking: something in the game
files gives this out without the plan book.

The others deliberately do NOT rescue a plan:

  * `plan` — the recipe is taught by a BOOK, which is the case `cut_reason()`
    already handles through references. Worse, the GNAM often points at a
    DIFFERENT plan: twelve weapon-specific plans create a record all called
    "Short Night Vision Scope". Letting one rescue the others would un-cut a
    generic leftover on the strength of its namesakes.
  * `script` — `LearnedViaScriptRecipes` says a script grants it, not that the
    script still runs. It is not evidence either way, the same call
    `meaningful_refs()` makes about a REFR with no EditorID.
  * `uncraftable`, `item`, none — say nothing about obtaining a plan.

ONE CRAFTABLE, SEVERAL RECIPES
------------------------------
A single thing a player builds is often several COBJ rows — one per workbench,
plus `..._CondProxy_...` variants — and Bethesda hangs the GNAM on only one of
them. The Angler, Crab Boat and Mirelurk plushies are the plain case: two
recipes each, both making the same FURN record, and only the
`workshop_co_CondProxy_FloorDecor_…` one names "Complete Daily Fishing Quests".
Whichever of the pair a plan happens to be linked to decides whether the
pipeline can see the route, which is not a distinction the game is drawing.

So an unlock is shared across every recipe that creates the SAME record (CNAM
FormID), onto recipes that carry none of their own. It is keyed on the created
record rather than on an EditorID stem because that is the thing the player
ends up with, and because a stem match here would have to guess which of
`Tinkers_Souvenir_Plushie_AnglerPlushie` and `FloorDecor_AnglerPlushie` stands
in for the other — a question with no answer, since both are real recipes.

A recipe with its own GNAM is never overwritten: a direct statement outranks one
inherited from a sibling.

The same sharing runs a second time keyed on the created record's NAME, because
the plushies turn out not to share a record at all — `FloorDecor_AnglerPlushie`
and `Tinkers_Souvenir_Plushie_AnglerPlushie` build two different FURN records
both called "Angler Plushie", and "Complete Daily Fishing Quests" hands over the
pair. The name pass demands **exactly one** donor among the recipes sharing a
name, which is what keeps it off the Night Vision Scope family: twelve recipes
share that name and not one of them is challenge-unlocked, so there is no donor
and nothing happens.

LINKING A PLAN TO ITS RECIPE
----------------------------
`cut_reason()` can only consult the recipe if the plan is linked to one, and an
orphaned plan is exactly the case that goes wrong: the Slasher bobber's BOOK has
`ReferencedByCount 0`, so the builder's two existing links (a HasLearnedRecipe
drop entry, then the BOOK's own ReferencedBy `:COBJ`) both come up empty and
`cobj` publishes as null. 221 of 2,901 live plans are in that state.

`link()` adds two fallbacks, in order, and **both refuse an ambiguous match**:

  1. EditorID stem — structural words dropped (`recipe`, `co`, `mod`,
     `workshop`, `CondProxy`, the dev prefix) and what is left compared as a
     whole. Resolves 112.
  2. Created-record name — the BOOK's FULL minus "Plan: " against CNAM_FULL.
     Resolves 25 more, including the Slasher radio, whose EditorID carries a
     `Radio` token the COBJ's does not.

Ambiguity is the guard rail, not a nicety. The Night Vision Scope family returns
twelve candidates on the name key; all ten ambiguous cases are rejected and stay
`cobj: null`, which is the honest answer. A wrong recipe would be published on
the page as fact, and would carry a wrong unlock sentence with it.
"""

import collections
import csv
import os
import re

# ─────────────────────────────────────────────────────────────────────────────
# Unlock kinds
# ─────────────────────────────────────────────────────────────────────────────
PLAN = "plan"
CHALLENGE = "challenge"
WORKSHOP_CLAIM = "workshop-claim"
SCRIPT = "script"
UNCRAFTABLE = "uncraftable"
ITEM = "item"

#: Kinds that settle cut_reason() on their own — see the module docstring.
PROOF_OF_LIFE = frozenset({CHALLENGE, WORKSHOP_CLAIM})

_DUMMY = {
    "recipe_dummy_learnedviaworkshopclaim": WORKSHOP_CLAIM,
    "recipe_dummy_learnedviascriptrecipes": SCRIPT,
    "recipe_dummy_uncraftable_item_nocraft": UNCRAFTABLE,
}

# Structural words in a COBJ / BOOK EditorID that say what KIND of record it is,
# never WHICH one. Dropping them is what lets
#   zzz_SDOW_Fishing_Recipe_mod_FishingRod_RodBobber_SlasherBobber   (BOOK)
#   SDOW_co_Fishing_mod_FishingRod_Weapon_RodBobber_SlasherBobber    (COBJ)
# reduce to the same key.
_STRUCTURAL = frozenset({
    "recipe", "recipes", "co", "cobj", "plan", "mod", "mods", "weapon",
    "workshop", "armor", "armour", "fishing", "condproxy", "conditionproxy",
    "misc", "display", "static", "the", "a", "of",
})
# The dev prefix is not always underscore-separated: the Burning Springs set is
# `zzzBurn_Workshop_...`, so requiring `zzz_` leaves "zzzburn" in the key and the
# plan never matches its own recipe. Strip the prefix when it is followed by a
# separator OR by a capital — the same shape plan_sources.CUT_PREFIXES matches,
# without swallowing the start of a real word.
_DEV_PREFIX = re.compile(r"^(?:z{2,}|cut|del|post|deprecated|debug)(?=[_\W]|[A-Z])[_\W]*")


def _stem(edid):
    """The distinguishing tokens of an EditorID, dev prefix and structure gone."""
    e = _DEV_PREFIX.sub("", (edid or "").strip())
    return tuple(p for p in re.split(r"[^a-z0-9]+", e.lower()) if p and p not in _STRUCTURAL)


_PROXY = re.compile(r"cond(?:ition)?proxy", re.I)


def _proxy_stem(edid):
    """What a `..._CondProxy_<stem>` EditorID is a proxy FOR."""
    m = _PROXY.search(edid or "")
    if not m:
        return ""
    return re.sub(r"^[_\W]+", "", (edid or "")[m.end():]).lower()


def _norm(name):
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _item_name(full):
    """"Plan: Rocket Bobber" -> "rocketbobber"."""
    return _norm(re.sub(r"^(?:Plan|Recipe)\s*:\s*", "", (full or "").strip()))


def _rows(path):
    if not path or not os.path.exists(path):
        return
    # errors="replace", matching build_plan_obtain_json.read_rows(). Exports are
    # not reliably clean UTF-8 — a PTS CHAL export carries a stray 0xAC — and a
    # decode error here would take out cut detection for a whole channel over one
    # byte in a challenge description nothing reads.
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            yield row


class RecipeUnlocks:
    """Reads COBJ + CHAL + BOOK once, answers "what unlocks this recipe".

    `newest(pattern, root)` is passed in rather than imported so this module
    stays usable from any builder and from a test harness — the same shape
    UnlockIndex takes in plan_sources.py.
    """

    def __init__(self, tsv_root, newest):
        self.root = tsv_root
        self.exports = {}
        self._chal = {}
        self._books = set()
        self.by_cobj = {}            # COBJ FormID -> unlock dict
        self.cobj_rows = {}          # COBJ FormID -> the raw row we care about
        self._by_stem = collections.defaultdict(list)
        self._by_name = collections.defaultdict(list)
        self._by_gnam = collections.defaultdict(list)
        self.link_stats = collections.Counter()

        chal_f = newest("CHAL_Export_*.tsv", tsv_root)
        book_f = newest("BOOK_Export_*.tsv", tsv_root)
        cobj_f = newest("COBJ_Export_*.tsv", tsv_root)
        self.exports = {
            "CHAL": os.path.basename(chal_f or ""),
            "BOOK": os.path.basename(book_f or ""),
            "COBJ": os.path.basename(cobj_f or ""),
        }

        for row in _rows(chal_f):
            fid = (row.get("FormID") or "").strip().upper()
            if fid:
                self._chal[fid] = row
        for row in _rows(book_f):
            fid = (row.get("FormID") or "").strip().upper()
            if fid:
                self._books.add(fid)

        for row in _rows(cobj_f):
            fid = (row.get("COBJ_FormID") or "").strip().upper()
            if not fid:
                continue
            edid = (row.get("COBJ_EDID") or "").strip()
            cnam = (row.get("CNAM_FULL") or "").strip()
            self.cobj_rows[fid] = {"edid": edid, "cnam_full": cnam}
            self._by_stem[_stem(edid)].append(fid)
            if _norm(cnam):
                self._by_name[_norm(cnam)].append(fid)

            self.cobj_rows[fid]["cnam_fid"] = (row.get("CNAM_FormID") or "").strip().upper()
            self.cobj_rows[fid]["is_proxy"] = bool(_PROXY.search(edid))

            gid = (row.get("GNAM_FormID") or "").strip().upper()
            if not gid:
                continue
            self._by_gnam[gid].append(fid)
            unlock = self._classify(gid, (row.get("GNAM_EDID") or "").strip(),
                                    (row.get("GNAM_FULL") or "").strip())
            if unlock:
                self.by_cobj[fid] = unlock

        self._bridge_created_record()

    def _bridge_created_record(self):
        """Share a proof-of-life unlock across every recipe making the same record."""
        by_record = collections.defaultdict(list)
        for fid, row in self.cobj_rows.items():
            if row.get("cnam_fid"):
                by_record[row["cnam_fid"]].append(fid)

        by_name = collections.defaultdict(list)
        for fid, row in self.cobj_rows.items():
            key = _norm(row.get("cnam_full"))
            if key:
                by_name[key].append(fid)

        for groups, tag in ((by_record, "record"), (by_name, "name")):
            for _, fids in groups.items():
                if len(fids) < 2:
                    continue
                donors = [f for f in fids
                          if (self.by_cobj.get(f) or {}).get("kind") in PROOF_OF_LIFE]
                if len(donors) != 1:
                    continue
                donor = donors[0]
                for f in fids:
                    if f == donor or f in self.by_cobj:
                        continue
                    self.by_cobj[f] = dict(self.by_cobj[donor],
                                           via_sibling=self.cobj_rows[donor]["edid"])
                    self.link_stats["shared_by_" + tag] += 1

    # ── classification ──────────────────────────────────────────────────────
    def _classify(self, gid, gedid, gfull):
        dummy = _DUMMY.get(gedid.lower())
        if dummy:
            return {"kind": dummy, "form_id": gid, "edid": gedid, "full": gfull}

        if gid in self._chal:
            ch = self._chal[gid]
            full = (ch.get("FULL") or gfull or "").strip()
            required = (ch.get("TNAM") or "").strip()
            counter = (ch.get("SNAM") or "").strip()
            return {
                "kind": CHALLENGE,
                "form_id": gid,
                "edid": gedid,
                "full": full,
                # The game tags a seasonal challenge in its display name. The
                # challenge pages strip it the same way, so the two agree.
                "name": re.sub(r"^\((?:Seasonal|Daily|Weekly)\)\s*", "", full).strip(),
                "required": int(required) if required.isdigit() else None,
                # "Script" is a placeholder counter on challenges driven by a
                # quest script, not something a player counts. Treated as absent.
                "counter": "" if counter.lower() == "script" else counter,
                "scope": (ch.get("CNAM") or "").strip(),
                "reward": (ch.get("MNAM") or "").strip(),
            }

        if gid in self._books:
            return {"kind": PLAN, "form_id": gid, "edid": gedid, "full": gfull}
        return {"kind": ITEM, "form_id": gid, "edid": gedid, "full": gfull}

    # ── lookups ─────────────────────────────────────────────────────────────
    def unlock_for(self, cobj_fid):
        return self.by_cobj.get((cobj_fid or "").upper())

    def proof_of_life(self, cobj_fid):
        """The unlock that makes this recipe obtainable without a plan, or None."""
        u = self.unlock_for(cobj_fid)
        return u if u and u["kind"] in PROOF_OF_LIFE else None

    def link(self, book_fid, book_edid, book_full, existing=""):
        """(cobj_fid, how) for a plan. `how` is one of
        existing / gnam / stem / name / ambiguous / none."""
        if existing:
            self.link_stats["existing"] += 1
            return existing.upper(), "existing"

        # The game naming the plan as the unlock is as strong as it gets.
        named = [f for f in self._by_gnam.get((book_fid or "").upper(), [])]
        if len(named) == 1:
            self.link_stats["gnam"] += 1
            return named[0], "gnam"

        for key, index, how in (
            (_stem(book_edid), self._by_stem, "stem"),
            (_item_name(book_full), self._by_name, "name"),
        ):
            if not key:
                continue
            hits = index.get(key) or []
            if len(hits) == 1:
                self.link_stats[how] += 1
                return hits[0], how
            if len(hits) > 1:
                # Ambiguous on this key — but one kind of tie breaks cleanly. If
                # exactly one candidate is a recipe the game says how to unlock,
                # that is the one a checklist row is about; the others are
                # workbench or souvenir variants with nothing to say. This is
                # what reaches the Angler / Crab Boat / Mirelurk plushies, whose
                # name matches two recipes apiece.
                marked = [f for f in hits
                          if (self.by_cobj.get(f) or {}).get("kind") in PROOF_OF_LIFE]
                # "Exactly one" has to mean one ANSWER, not one row. The three
                # fishing plushies are two recipes each — a souvenir and a floor
                # decoration — and "Complete Daily Fishing Quests" hands over
                # both, so both are marked and both say the same thing. Prefer
                # the recipe carrying the GNAM itself over one that inherited it.
                direct = [f for f in marked
                          if not (self.by_cobj[f] or {}).get("via_sibling")]
                pick = None
                if len(direct) == 1:
                    pick = direct[0]
                elif marked and len({self.by_cobj[f]["form_id"] for f in marked}) == 1:
                    pick = (direct or marked)[0]
                if pick:
                    self.link_stats["%s_unlocked" % how] += 1
                    return pick, "%s+unlock" % how
                # Otherwise fall through to the next key rather than picking —
                # and if neither key is unique, publish nothing.
                self.link_stats["ambiguous_%s" % how] += 1

        self.link_stats["unlinked"] += 1
        return "", "none"

    # ── wording ─────────────────────────────────────────────────────────────
    def sentence(self, unlock):
        """A plain-English unlock sentence, or None.

        Matches the voice of plan_sources.UnlockIndex — "Reward for completing
        the …" — so the two lists read as one on the page.
        """
        if not unlock:
            return None
        kind = unlock["kind"]
        if kind == WORKSHOP_CLAIM:
            return "Learned by claiming a workshop."
        if kind != CHALLENGE:
            return None
        name = unlock.get("name") or unlock.get("full")
        if not name:
            return None
        required, counter = unlock.get("required"), unlock.get("counter")
        if required and required > 1 and counter:
            return (f"Reward for completing the challenge: {name} "
                    f"— {required} {counter}.")
        return f"Reward for completing the challenge: {name}."

    # ── reporting ───────────────────────────────────────────────────────────
    def report(self):
        kinds = collections.Counter(u["kind"] for u in self.by_cobj.values())
        bits = ", ".join(f"{k} {v}" for k, v in sorted(kinds.items()))
        return (f"[unlocks] {self.exports['COBJ']} | {len(self.by_cobj)} recipes "
                f"carry a GNAM unlock ({bits})")
