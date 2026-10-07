#!/usr/bin/env python3
r"""
plan_sources.py — cut-content detection, readable source names, and non-drop
route resolution for the plan checklist pages.

This module holds the three things build_plan_obtain_json.py used to do badly or
not at all. It owns no rate maths: rng76 / build_farming_used_for still resolve
every percentage, and nothing here multiplies, divides or reweights one. What it
changes is which routes survive the walk, and what they are called.

    1. CUT CONTENT   — is this plan a dev leftover?
    2. NAMING        — turn a leveled-list EditorID into something a player reads
    3. UNLOCKS       — the routes that are not random loot at all


1. CUT CONTENT
--------------
build_plan_obtain_json.py shipped `"cut": False` as a literal on every row for
its whole life, so Bethesda's own dev leftovers — prefixed zzz_ / CUT_ / DEL_ /
POST_ — rendered on the checklist pages as though a player could go and get
them, and counted toward every reader's progress bar.

The prefix list is deliberately the SAME one build_titles_json.py uses
(CUT_PREFIXES / CUT_SUFFIXES there), so a zzz_ plan and a zzz_ title are called
cut by the same rule on the same site. Do not fork it.

BOTH tests must agree: the EditorID carries a dev prefix AND nothing in the game
files gives the plan out. The prefix alone is not enough — see cut_reason() for
the ten live Burning Springs plans it would wrongly bury. A resolved reference
beats the naming convention, the same way a resolved bucket beats a word in an
EditorID everywhere else in this pipeline.

A clean EditorID is never called cut on a missing reference alone: a plan sold
by a vendor whose stock lives outside the BOOK's ReferencedBy would be a false
positive, and mislabelling a live plan as cut is the worse error of the two.


2. NAMING
---------
The old source_label() carried a hand-maintained AREA_CODE map whose comment
admitted the problem: "Every entry below was read off the game data (quest EDID
-> FULL name in the QUEST export), not guessed." That transcription is exactly
what a build script should do for itself, so QuestNames does it — it reads the
same QUEST export the map was copied out of, every build, and therefore knows
about content that shipped after the map was last edited.

    E09A_Launcher_Recipes             -> Event: Seismic Activity
    SDOW_MQ01_LL_QuestRewards         -> The Slasher: Masked Truth
    LC060_Whitespring_VendorChest_BoS -> Whitespring (Brotherhood of Steel vendor)
    LLV_Faction_BoS                   -> Brotherhood of Steel vendor

Lookup walks the EditorID's leading tokens longest-first and takes the first
prefix that names EXACTLY ONE quest. A prefix shared by quests with different
names is ambiguous and is skipped rather than guessed at — so `Burn_SQ04`,
which covers three different cache quests, resolves nothing and falls through to
the AREA_CODE backstop ("Burning Springs"). That is the intended shape: be
specific when the data is specific, general when it is not, and never invent.

The QUEST export and AREA_CODE compete, and whichever matched MORE of the
EditorID wins, ties going to the curated map. That tiebreak is load-bearing:
"DailyOps" is the unique prefix of one incidental side quest, so without it
every Daily Ops drop on the site got renamed "Breaking Radio Silence".

FAMILY NAMES cover the middle case. A code like SDOW owns a dozen quests, but
its four Primary ones are all "The Slasher: <something>" — a common prefix
before the colon. Where the *story* quests of a code agree on such a stem, that
stem becomes the family name ("The Slasher"), which beats the AREA_CODE map but
loses to an exact single-quest match.

AREA_CODE survives as the backstop for region and system codes that are not
quests at all (ATX, SCORE, Fishing, Workshop) or whose quests disagree. It is
smaller than it was, because everything the QUEST export can answer has been
deleted from it.

ACRONYMS is the other half of readable. The camel splitter used to turn "BoS"
into "Bo S" — 325 routes on the live site read "Faction Bo S" — because
splitting on a lower->upper boundary cuts inside any mixed-case acronym. The
splitter now protects a known-acronym list first, then expands it, so "BoS"
becomes "Brotherhood of Steel" and never "Bo S".


3. UNLOCKS
----------
A plan you buy, or are handed for finishing a quest, has no drop rate — there is
no leveled list and no percentage, and the old builder said so with "No random-
loot drop routes were resolved", which is true and useless. UnlockIndex resolves
those routes from the records that DO describe them, and returns sentences
rather than rate rows:

    GMRW  the reward record itself, by BOOK ReferencedBy and by RewardedItem.
          Its ParentQuestDisplay is the quest's real name, and its own EditorID
          says whether it is a quest reward or a challenge reward.
    QUST  a direct quest reference -> that quest's FULL name.
    TERM  a prize terminal (the Nuka-Cade tiers).
    CONT  a named container that is not an editor test chest.
    REFR  a fixed world placement -> BOOK_Export_*_Locations.tsv gives the cell,
          which parse_ext_cell_location() turns into a place name.

Those five are RESOLVED — a record links the plan to the source. The sixth is
not, and is worded differently for that reason:

    EDID  ATX_ / SCORE_S24_ / _StampVendor / _GoldVendor in the plan's own
          EditorID. This is Bethesda's naming convention, not a link, so these
          read "named as ... in the game files" and never claim a resolved
          route. A convention is good evidence and bad proof.

Output goes in a new `obtain_unlocks` list of strings, NOT in `obtain_routes`.
Keeping them apart is the point: obtain_routes stays a table of sources and
percentages that every one of those percentages came out of rng76, and nothing
without a real resolved rate is ever given a row in it.

Used by: build_plan_obtain_json.py.
"""
import os, re, csv, glob, collections

# parse_ext_cell_location / is_test_cell_name already exist and are already the
# site's answer for turning a CELL EditorID into a place name — the notes and
# keys pages render locations with them. Importing rather than re-implementing
# keeps one behaviour, so a fix to Mc/Mac handling or the test-cell list reaches
# the plan pages too.
from build_collectables_json import parse_ext_cell_location, is_test_cell_name


# ─────────────────────────────────────────────────────────────────────────────
# 1. CUT CONTENT
# ─────────────────────────────────────────────────────────────────────────────
# Same tuple as build_titles_json.py. If you add one there, add it here.
CUT_PREFIXES = ("DEL", "POST", "CUT", "ZZZ", "ZZZZ")
CUT_SUFFIXES = ("_COPY01",)


# Records that referencing a plan proves nothing. A plan's own COBJ is the
# recipe it teaches and EVERY plan has one; BabylonExcludeList is an editor
# form-list that every cut plan is on; the ZW / QA / Test / Debug chests are
# developer containers. Counting these is what made the first cut of this
# function report "Plan: Mattress — still referenced by 1 record" when that one
# record was BabylonExcludeList, i.e. the game data agreeing it is dead.
_EMPTY_REF_SIGS = {"COBJ", "FLST"}
_RX_DEV_RECORD = re.compile(
    r"^(babylon|zw|zz|test|qa|debug|cut|del|deleted|post|template)"
    r"|(test|debug|_qa_|excludelist)", re.I)


def is_dev_record(edid):
    """True when an EditorID names something that only exists in the editor.

    Used to stop dev records being NAMED, which is a different question from
    whether a plan is cut. source_label() already returns None for these, but
    the route resolver falls back to humanize() when it gets None — and that
    fallback happily published an NPC called CUT_LvlSubBoss as the route
    "CUT Lvl Sub Boss". Anything this returns True for must be dropped, never
    prettified.
    """
    e = (edid or "").strip()
    if not e:
        return False
    if any(e.upper().startswith(p) for p in CUT_PREFIXES):
        return True
    parts = [p for p in re.split(r"[_\-]", e) if p]
    return any(p.lower() in HARD_DEV_CODES for p in parts)


# Sources that ARE wired up in the game files but never shipped to players.
# The data cannot prove these cut: the Mole Miner Mystery Crate quest
# (MILE_MoleMiner_MysteryCrate, titled only "[Quest for MoleMiner Mystery
# Crate]") holds the eight MILE_LL_MysteryCrate_*_MainRoll lists, has dialogue
# INFOs, cost GLOBs and an ACHR, so the reachability scan calls every list
# live and the route resolver names it "Mole Miner Mystery Crate (...)". It is
# cut content (Duchess, 6 Oct 2026; already excluded from the spawn guides in
# spawns_engine/events.py since Sep 2026). A bracketed title alone is NOT a
# usable signal — 366 live-export quests have one, RE_ObjectTS06 among them.
#
# Matched as EditorID prefixes, case-insensitive. Value = the reason shown on
# the retired route and on any plan left with nothing else. Keep this list
# SHORT and every entry confirmed by a person — a wrong entry buries a live
# source. prune_dead_routes / build_bugged_plans_json read it through
# cut_source(); naming is unaffected, so the route still resolves and is moved
# to `retired_routes`, not silently dropped.
CONFIRMED_CUT_SOURCES = (
    ("MILE_MoleMiner_MysteryCrate", "the Mole Miner Mystery Crate is cut content"),
    ("MILE_LL_MysteryCrate_",       "the Mole Miner Mystery Crate is cut content"),
)


def cut_source(edid):
    """Reason string when this EditorID is a confirmed-cut source, else None."""
    e = (edid or "").strip().lower()
    if not e:
        return None
    for prefix, why in CONFIRMED_CUT_SOURCES:
        if e.startswith(prefix.lower()):
            return why
    return None


def meaningful_refs(refs):
    """The references that would actually show a plan is still obtainable.

    `refs` is the BOOK's ReferencedBy list as exported, "FORMID:EDID:SIG".
    """
    out = []
    for ref in refs or []:
        bits = (ref or "").split(":")
        if len(bits) < 3:
            continue
        redid, rsig = ":".join(bits[1:-1]).strip(), bits[-1].strip().upper()
        if rsig in _EMPTY_REF_SIGS:
            continue
        # A reference the export gives no EditorID for cannot be checked, so it
        # is not evidence either way. Both St. Valentine's Day Massacre recipes
        # have exactly one of these, and it points into a cell the export could
        # not resolve either ("[CELL:00260D53]") on a quest called
        # P01E_Heart_FlaggedToNotExport. Counting a bare REFR as proof of life
        # would have kept that whole cut event on the pages.
        if not redid:
            continue
        if _RX_DEV_RECORD.search(redid):
            continue
        out.append(ref)
    return out


def dead_plan_reason(refs, has_recipe, recipe_unlock=None):
    """Why a plan with a CLEAN EditorID is still not a real plan, or None.

    Two facts, both read off the records, both required:
      * no recipe (COBJ) uses this BOOK, so reading it would teach nothing, and
      * nothing in the game files gives it out — no meaningful reference and no
        non-plan unlock of a recipe.
    "Plan: Veracio Hat" (Recipe_XPD_AC_Headwear_) and "Plan: Spy Outfit" are
    the Apparel cases (Duchess, 4 Oct 2026); the Dino Peaks plans are the CAMP
    ones — their items ship as SCORE_S23 scoreboard rewards and these books are
    the leftovers. A clean EditorID with a recipe is never called dead on
    missing references alone — a script-granted plan would be buried.
    """
    if has_recipe or recipe_unlock or meaningful_refs(refs):
        return None
    return ("no recipe uses this plan and nothing in the game files gives it "
            "out, so it teaches nothing and cannot be obtained")


def cut_reason(edid, refs=None, recipe_unlock=None):
    """Why this plan is cut content, or None if it is not.

    BOTH tests have to agree: the EditorID carries a dev prefix AND nothing in
    the game files gives the plan out.

    "Gives it out" has TWO halves, and for a long time this function only knew
    the first. A reference to the BOOK is one way. The other is the recipe the
    plan teaches being unlocked by something that is not a plan at all —
    `recipe_unlock`, from plan_unlocks.RecipeUnlocks.proof_of_life(). Kevin put
    the case plainly on 17 Sep 2026, about the Pint-Sized Slasher radio and
    bobber shipping here as cut content:

        "those were the book forms. I disabled them because the cobj is taught
         directly by completing the respective challenge."

    A disabled BOOK with no references is exactly what that looks like from the
    BOOK side, and it is indistinguishable from a genuine leftover until you
    read COBJ.GNAM. It rescues 22 live plans, among them every fishing bobber
    and float this repo previously wrote off as an Atom Shop leftover — they are
    rewards for the "Catch All Regional Fish" challenges. The prefix alone is not enough, and that
    is not caution for its own sake — on the live roster it is wrong ten times:

        zzzBurn_Workshop_Recipe_BountyBoard
            <- ChallengeReward_Challenge_Lifetime_BurnBounty_CompleteHeadHuntGroup_01
        zzzBurn_Workshop_Recipe_AbraxodyneSign
            <- Burn_ChallengeReward_Challenge_Lifetime_Abraxodyne_HQWallSign
        zzzBurn_Recipe_mod_FishingRod_RodBobber_DollHead
            <- Burn_ChallengeReward_Fishing_BurningSprings_Region_Collection
        ...and seven more of the Burning Springs set

    Bethesda shipped those with a zzz prefix and then wired them to real
    challenge rewards. They are obtainable, players have them, and calling them
    cut would strike them off every checklist AND remove them from the progress
    total — a bar that silently says "of 2,560" when it should say "of 2,570".

    So the resolved reference wins over the naming convention, which is the same
    ladder the grouping code uses: a link the builder actually resolved beats
    any word in an EditorID. The prefix only decides where nothing was resolved.

    Which references count is the whole question, and meaningful_refs() answers
    it — "Plan: Vendors" looks like it has seven until you notice six are its
    own POST_-prefixed COBJs and the seventh is BabylonExcludeList.
    """
    e = (edid or "").strip().upper()
    if not e:
        return None

    hit = next((p for p in CUT_PREFIXES if e.startswith(p)), None)
    if not hit:
        hit = next((s for s in CUT_SUFFIXES if e.endswith(s)), None)
        if not hit:
            return None
        why = f"its EditorID ends {hit}"
    else:
        why = f"its EditorID starts {hit}_"

    # The recipe being unlocked by a challenge or a workshop claim is the game
    # stating the route in its own data, which outranks the naming convention
    # exactly as a resolved reference does.
    if recipe_unlock:
        return None
    if refs is None:
        return why
    if meaningful_refs(refs):
        return None          # the data says it is live; the prefix is stale
    return f"{why} and nothing in the game files gives it out"


# ─────────────────────────────────────────────────────────────────────────────
# 2. NAMING
# ─────────────────────────────────────────────────────────────────────────────
# Mixed-case acronyms the camel splitter must not cut inside, mapped to what a
# reader should see. Keys are matched case-insensitively as whole tokens.
#
# "BoS" is the one that mattered: 325 live routes read "Faction Bo S" because
# the lower->upper split fires between the o and the S. Protect first, expand
# second — the order is the fix.
ACRONYMS = {
    "mischief":   "Mischief Night",       # the seasonal event, never the word alone
    "bos":        "Brotherhood of Steel",
    "pa":         "Power Armor",
    "nw":         "Nuclear Winter",
    "atx":        "Atom Shop",
    "ac":         "Atlantic City",
    "xpd":        "Expedition",
    "dlc":        "",          # DLC04_... is a file-layout artefact, not a place
    "rsvp":       "",
    "npe":        "",          # new-player experience wrapper
    "smart":      "S.M.A.R.T.",
    "camp":       "C.A.M.P.",
    "surv":       "Survival",
}

# Region and system codes the QUEST export cannot answer — either they are not
# quests (ATX, SCORE, Fishing, Workshop) or their quests disagree on a name.
# Everything a quest CAN name has been removed from here; QuestNames resolves
# those live, so new content does not need this map edited.
AREA_CODE = {
    "ac":            "Atlantic City",
    "xpd":           "Expeditions",
    "xpd_ac":        "Atlantic City Expedition",
    "mutatedevents": "Mutated Public Events",
    "dailyops":      "Daily Ops",
    "rd01":          "Raids",
    "hto":           "Infestations",
    "burn":          "Burning Springs",
    "storm":         "Skyline Valley",
    "p62":           "The Drifter",
    "moon":          "Milepost Zero",
    "w05":           "Wastelanders",
    "bs01":          "Steel Dawn",
    "bs02":          "Steel Reign",
    "treasurehunt":  "Treasure Hunter",     # the seasonal event; see LOOT_BAG_SUFFIX
    "v94":           "Vault 94",
    "v96":           "Vault 96",
    "atx":           "Atom Shop",
    "score":         "Season Scoreboard",
    "fishing":       "Fishing",
    "workshop":      "Workshop",
    "legendary":     "Legendary",
    "nw":            "Nuclear Winter",
}

# ─────────────────────────────────────────────────────────────────────────────
# 2a. CONTENT-TYPE PARENTHETICAL
# ─────────────────────────────────────────────────────────────────────────────
# Some sources render as a bare quest/phase NAME that tells a reader nothing
# about what to actually DO — "The Slasher: Out of the Shadows" is an Infestation
# hunt, "Blood Will Have Blood" is Head Hunts. annotate_content_type() appends a
# " (Type)" so the label says the activity, e.g. "The Slasher: Out of the
# Shadows (Infestations)".
#
# The type is resolved GENERATIVELY, keyed on the source's quest EDID or name —
# NOT a per-plan list. The SDOW/Slasher event phases name their own type in the
# quest EDID (SDOW_MQ04_Infestations, SDOW_MQ05_Headhunt, SDOW_MQ02_Graves), so
# any content that follows Bethesda's convention self-annotates. QUEST_EDID_TYPE
# maps those EDID activity tokens to the words the site already uses ("hto" is
# "Infestations" and "dailyops" is "Daily Ops" in AREA_CODE above), matched as a
# whole underscore-delimited token, case-insensitively.
QUEST_EDID_TYPE = {
    "headhunt":      "Head Hunts",
    "headhunts":     "Head Hunts",
    "infestation":   "Infestations",
    "infestations":  "Infestations",
    "grave":         "Grave Digging",
    "graves":        "Grave Digging",
    "partycrasher":  "Party Crashers",
    "partycrashers": "Party Crashers",
    # NB: "dailyops" is deliberately NOT here. It is the leading token of
    # unrelated misc quests (DailyOps_VernonDodge_MiscQuest -> "Breaking Radio
    # Silence", the DailyOps_Mode0N ops instances), and typing those "(Daily
    # Ops)" would be wrong — the same trap source_label()'s tiebreak guards
    # against. The one quest that legitimately needs it, "The Way of the
    # Wicked", is handled by the curated CONTENT_TYPE_BY_NAME map below.
}

# The handful of named sources whose type the EDID does NOT carry, keyed on the
# display name (lowercased). Small and explicit so a wrong entry is obvious. This
# is the ONE place a judgement call lives — SDOW_MQ01_Bodies ("Masked Truth") is
# a one-time investigation main quest, not a repeatable activity like its three
# siblings, so it reads "(Quest)". Change the value here to "Slasher Masks" if
# the mask-hunt framing is preferred; nothing else needs touching.
CONTENT_TYPE_BY_NAME = {
    "the slasher: masked truth":         "Quest",
    "masked truth":                      "Quest",
    "mask of truth":                     "Quest",
    # Cut as a standalone quest (zzz_CUT_SDOW_MQ03_DailyOps) so it should never
    # reach a route, but its EDID already carries "dailyops"; named here too in
    # case it ever surfaces from another record.
    "the slasher: the way of the wicked": "Daily Ops",
    "the way of the wicked":              "Daily Ops",
}

# Every value the two maps above can produce. The linter checks that a published
# parenthetical is one of these, so a typo or a wired token cannot ride in inside
# the brackets. Extend this set in the same commit as the maps.
CONTENT_TYPES = set(QUEST_EDID_TYPE.values()) | set(CONTENT_TYPE_BY_NAME.values())


def _derive_content_type(edid, full):
    """Content type for one quest, from its EDID token then its name. "" if none.

    Generative: the EDID activity token is tried first (so new SDOW-style content
    self-annotates), then the curated name map for the cases the token can't
    answer. Never guesses from the quest TYPE column — a "Primary" quest is not a
    content type a player would recognise.
    """
    for tok in re.split(r"[_\-]", edid or ""):
        t = QUEST_EDID_TYPE.get(tok.lower())
        if t:
            return t
    for key in (usable_quest_name(full).lower(), _clean_name(full).lower()):
        if key in CONTENT_TYPE_BY_NAME:
            return CONTENT_TYPE_BY_NAME[key]
    return ""


def annotate_content_type(label, quests, name=None):
    """Append " (Type)" to a source label whose name has a known content type.

    `name` is the specific name to resolve the type from (the resolved quest
    head, or the bare quest title in an unlock sentence); it defaults to the
    whole label. Returns the label unchanged when no type resolves, when the
    type is already present, or when the label already carries a parenthetical
    for it — so it is safe to call more than once on the same string.
    """
    if not label or quests is None:
        return label
    ct = quests.content_type_for(name if name is not None else label)
    if not ct:
        return label
    low = label.lower()
    if f"({ct.lower()})" in low:
        return label                       # already annotated
    # Redundant when the label already spells the activity out — "The Slasher -
    # Daily Ops" must not become "... Daily Ops (Daily Ops)", and "Bounty
    # Hunting: Head Hunt" already says head hunt. Compared on singular/plural-
    # folded word sets so "Head Hunt" suppresses "Head Hunts" and "Party
    # Crasher" suppresses "Party Crashers"; needs ALL of the type's words, so
    # "Secrets to the Grave" (has "grave", not "digging") still gains "(Grave
    # Digging)".
    def _stem(s):
        return {re.sub(r"s$", "", w) for w in re.split(r"\W+", s.lower()) if w}
    if _stem(ct) <= _stem(label):
        return label
    return f"{label} ({ct})"


# Lists that exist only inside the editor. A route named from one of these is
# dropped outright — it is not somewhere a player can go.
DEV_CODES = {"cut", "debug", "deleted", "del", "deprecated", "zzz", "zzzatx",
             "test", "unused", "obsolete", "xx", "template", "placeholder",
             "post", "backup", "qa", "babylon"}

# The subset of DEV_CODES that can only ever mean "dead", wherever it appears in
# the name. DEV_CODES as a whole is checked only at the FRONT of an EditorID,
# because several of its words are legitimate mid-name: "Event: Test Your Metal"
# is a real event and a blanket "test" ban would delete it. These five have no
# innocent reading, so a list carrying one anywhere is dropped —
# LLS_Loot_Weapons_CUT_Lvl_SubBoss was surfacing as "CUT Lvl Sub Boss".
HARD_DEV_CODES = {"cut", "deleted", "deprecated", "zzz", "debug", "babylon"}

# Wiring tokens: true of every list, so they carry no information for a reader.
# NB "items" is deliberately NOT here — "Chase Items" and "Unique Items" are
# things a player recognises, unlike "Rewards" or "Tranche".
PLUMBING = {"ll", "lls", "lld", "lle", "lli", "llv", "llq", "lpi", "list", "lists",
            "reward", "rewards", "questreward", "questrewards", "loot",
            "lootlist", "pool", "table", "tier", "tranche", "sub", "shared",
            "generic", "misc", "all", "any", "main",
            "stage", "stage9000", "lvl", "audio", "missing",
            "entry", "entries", "set", "group", "co", "recipe", "recipes",
            "enc", "star", "cr", "re", "ref", "refs", "quest", "quests",
            # The row already carries a source_type badge saying "creature", so
            # the word inside the label is restating the pill.
            "creature", "creatures", "npc",
            "condproxy", "proxy", "chest", "special", "target", "faction",
            "container", "containers", "restricted",
            # "XX" is a dev marker, but only disqualifying when it LEADS the
            # name (DEV_CODES already rejects that). Mid-name it is just noise
            # riding along on a real route — 92 rows read "The Drifter - XX".
            "xx",
            # Engine flags that ride along mid-name and mean nothing to a reader.
            "nonplayable", "playable", "placeholder",
            "temp", "tmp", "todo", "wip", "backlog", "old", "new2"}

# Words that mean "this is somewhere you spend currency", and the faction /
# currency words that qualify one. Vendor rows are the single biggest naming
# problem on the live site — 325 of them read "Faction Bo S" and another 170
# render the same trader twice — so they get shaped explicitly rather than left
# to generic token-joining. See _shape_vendor().
VENDOR_WORDS = {"vendor", "vend", "trader", "merchant"}
VENDOR_QUALIFIER = {
    "gold":          "Gold Bullion",
    "goldvendor":    "Gold Bullion",
    "bullion":       "Gold Bullion",
    "stamp":         "Stamp",
    "stamps":        "Stamp",
    "treasury":      "Treasury",
    "caps":          "Caps",
    "score":         "Season",
    "expedition":    "Expedition",
}
# Matched as phrases against the joined label, longest first, because a faction
# is often two words by the time ACRONYMS has expanded it ("BoS" -> "Brotherhood
# of Steel") and token-by-token matching splits it in half.
# Variant -> the one spelling published. The singular and plural both occur in
# EditorIDs, and without canonicalising them the same trader renders twice —
# "Wastelanders - Molly (Raider vendor)" AND "… (Raiders vendor)" — which
# collapse_routes cannot merge, because to it those are different words.
FACTIONS = {
    "Brotherhood of Steel": "Brotherhood of Steel",
    "Secret Service":       "Secret Service",
    "Free States":          "Free States",
    "Responders":           "Responders",
    "Responder":            "Responders",
    "Settlers":             "Settlers",
    "Settler":              "Settlers",
    "Raiders":              "Raiders",
    "Raider":               "Raiders",
    "Enclave":              "Enclave",
    "Showmen":              "Showmen",
    "Mobster":              "Mobster",
    "Muni":                 "Muni",
    "Blood Eagles":         "Blood Eagles",
    "Neutral":              "Neutral",
}

# Map-cell codes like TW006 / LC129 / WL020 name an interior, not a place a
# player would call by that name. Three or more digits distinguishes them from
# the two-digit content codes (RD01, BS02, W05, V94), which ARE meaningful.
RX_CELL_CODE = re.compile(r"^[A-Za-z]{2,4}\d{3,}$")

_RX_LIST_PFX = re.compile(
    r"^(cr)?(LL[SDEVQI]?|LPI|co|Recipe|recipe|QuestRewards?)_", re.I)
_RX_TRAILNUM = re.compile(r"^([A-Za-z]{3,})(\d+)$")   # Tranche05 -> Tranche | 05


def _split_camel(tok):
    """CamelCase -> words, without cutting inside a known acronym.

    'VendorChestBoS' -> ['Vendor', 'Chest', 'BoS'] and never [..., 'Bo', 'S'].
    Acronyms are lifted out behind a sentinel, the rest is split normally, then
    they are put back — which is why the order in this function is the whole
    point of it.
    """
    if not tok:
        return []
    held = []

    def _hold(m):
        held.append(m.group(0))
        # Spaces around the sentinel: the restore step matches whole
        # whitespace-separated words, so a sentinel glued to the next word
        # ("\x000\x00Helmet") would never be recognised and the control
        # characters would land in the published label.
        return f" \x00{len(held) - 1}\x00 "

    # Longest first so "BoS" is taken before a shorter key could match inside it.
    #
    # The trailing lookahead is the fiddly part. "(?![a-z])" alone is too loose:
    # "AC" is an acronym, so ACTI would match and split as AC|TI. Requiring the
    # next thing to be the end, a digit, or a real camel boundary ([A-Z][a-z])
    # keeps PAHelmet -> PA|Helmet and Faction_BoS -> BoS while leaving ACTI,
    # ACTIVITY and friends alone.
    #
    # Case-insensitivity is scoped to the keys with (?i:...) rather than passed
    # as a flag. A whole-pattern re.I also folds the [A-Z][a-z] lookahead, which
    # then matches ANY two letters — and ACTI went back to splitting as AC|TI.
    keys = sorted(ACRONYMS, key=len, reverse=True)
    if keys:
        # `[A-Za-z]\d` covers an acronym welded to a content code: BoSz01 and
        # BoSr01 are Brotherhood quest codes, and without it they split as
        # "Bo Sz01" — the same "Bo S" failure this whole function exists to stop,
        # just one character further along.
        tok = re.sub(r"(?<![A-Za-z])((?i:" + "|".join(re.escape(k) for k in keys) +
                     r"))(?=$|[0-9]|[A-Z][a-z]|[A-Za-z]\d)",
                     _hold, tok)

    tok = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", tok)
    tok = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", tok)
    out = []
    for w in tok.split():
        m = re.fullmatch(r"\x00(\d+)\x00", w)
        out.append(held[int(m.group(1))] if m else w)
    return out


def tokens(edid):
    """EditorID -> word tokens, acronyms intact, welded numbers split off."""
    s = _RX_LIST_PFX.sub("", edid or "")
    out = []
    for raw in s.replace("-", "_").split("_"):
        for t in _split_camel(raw):
            if not t:
                continue
            # Split a word welded to a number (Tranche05, Tier01) so the word can
            # be judged as plumbing and the number dropped as a bare index. Area
            # codes (RD01, BS02, V94, W05) are short and stay whole — the {3,}.
            m = _RX_TRAILNUM.match(t)
            out.extend([m.group(1), m.group(2)] if m else [t])
    return out


def expand(tok):
    """One token as a reader should see it ('' means drop it)."""
    v = ACRONYMS.get(tok.lower())
    return tok if v is None else v


class QuestNames:
    """Prefix -> quest name, read out of the QUEST export every build.

    Two lookups, in this order:

      exact  — the longest leading-token prefix that names EXACTLY ONE quest.
               Ambiguous prefixes are skipped, never guessed at.
      family — where a code's STORY quests (Primary / Public Event / Side Quest
               / Expedition / Raid / Daily Ops) share a stem before a colon,
               that stem names the code. SDOW's four Primary quests are all
               "The Slasher: <something>", so SDOW is "The Slasher".

    Bracketed names ("[Dialogue Quest]") and names that just restate the
    EditorID are not names and are never indexed.
    """

    STORY_TYPES = {"primary", "public event", "side quest", "secondary",
                   "expedition", "raid", "daily ops", "event", "caravan"}
    EVENT_TYPES = {"public event", "event"}
    MAX_PREFIX = 4        # tokens; SDOW_MQ01 is 2, XPD_AC_Mission_Tier is 4

    def __init__(self, path=None):
        self.exact = {}     # prefix -> name, or None when ambiguous
        self.family = {}    # leading code -> family stem
        self.content_type = {}   # quest-name (lower) -> content-type word
        if path:
            self.load(path)

    def content_type_for(self, name):
        """The content-type word for a resolved source name, or "".

        Keyed on the display name (raw FULL or its cleaned form) so it hits
        whether the caller passes the route head ("The Slasher: Out of the
        Shadows") or the cleaned unlock-sentence title. Falls back to the curated
        CONTENT_TYPE_BY_NAME so a name absent from the QUEST export still resolves.
        """
        n = (name or "").strip().lower()
        if not n:
            return ""
        return (self.content_type.get(n)
                or CONTENT_TYPE_BY_NAME.get(n)
                or CONTENT_TYPE_BY_NAME.get(_clean_name(name).lower(), ""))

    def load(self, path):
        by_prefix = collections.defaultdict(set)
        by_code   = collections.defaultdict(set)
        type_of   = {}
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    edid = (row.get("EDID") or "").strip()
                    full = (row.get("FULL - Name") or row.get("FULL") or "").strip()
                    qtype = (row.get("Quest Type") or "").strip().lower()
                    if not edid or not full:
                        continue
                    if full == edid or not usable_quest_name(full):
                        continue                       # a placeholder, not a name
                    if not usable_quest_name(full):
                        continue
                    # Content type is derived here so it is available under both
                    # the raw and cleaned name (source_label passes the raw head,
                    # unlock sentences pass the cleaned title). Done before the
                    # CUT skip because the derivation reads the EDID, which the
                    # naming index deliberately ignores for cut quests.
                    ct = _derive_content_type(edid, full)
                    if ct:
                        self.content_type[full.lower()] = ct
                        self.content_type[_clean_name(full).lower()] = ct
                    if any(edid.upper().startswith(p) for p in CUT_PREFIXES):
                        continue
                    parts = [p for p in edid.split("_") if p]
                    type_of[full] = qtype
                    for n in range(1, min(self.MAX_PREFIX, len(parts)) + 1):
                        by_prefix["_".join(parts[:n]).lower()].add(full)
                    if qtype in self.STORY_TYPES and parts:
                        by_code[parts[0].lower()].add(full)
        except FileNotFoundError:
            return

        for k, names in by_prefix.items():
            self.exact[k] = next(iter(names)) if len(names) == 1 else None
            # A bare content code shared by one EVENT and only non-story
            # scaffolding (a radio quest, a misc tracker) is that event:
            # MN2_Quest_Mischief "Event: Mischief Night" shares MN2 with
            # "Mischief Night Radio", so MN2 resolved to nothing and every
            # Mischief Night reward published as the raw code "MN2".
            if self.exact[k] is None and "_" not in k:
                story = [n for n in names if type_of.get(n) in self.STORY_TYPES]
                if len(story) == 1 and type_of.get(story[0]) in self.EVENT_TYPES:
                    self.exact[k] = story[0]

        for code, names in by_code.items():
            # "The Slasher: Masked Truth" and three siblings all say the code is
            # called The Slasher. Wrappers are stripped first, or SDOW's
            # "Repeatable: Disturbed Grave" would count as a second, competing
            # stem and veto the name the other four agree on.
            stems = collections.Counter()
            for n in names:
                c = _clean_name(n)
                if ":" in c:
                    stems[c.split(":", 1)[0].strip()] += 1
            # One stem, claimed by at least two quests AND by most of the code's
            # story quests. A lone colon is a subtitle, not a family, and two
            # competing stems mean the code does not name one thing — both fall
            # through to the AREA_CODE backstop.
            #
            # The majority test is what keeps Burning Springs from being renamed
            # "Bounty Hunting": two Burn quests are titled "Bounty Hunting: ..."
            # and none of its other dozen story quests use a colon at all, so
            # without it a two-quest minority would rename the whole region.
            if len(stems) == 1:
                stem, n_quests = stems.most_common(1)[0]
                if n_quests >= 2 and n_quests * 2 >= len(names) and len(stem) > 3:
                    self.family[code] = stem

    def lookup(self, parts):
        """(name, tokens_consumed) for a list of raw EditorID segments."""
        for n in range(min(self.MAX_PREFIX, len(parts)), 0, -1):
            hit = self.exact.get("_".join(parts[:n]).lower())
            if hit:
                return hit, n
        if parts:
            fam = self.family.get(parts[0].lower())
            if fam:
                return fam, 1
        return None, 0


def usable_quest_name(full):
    """A quest title fit to print, or "" if it is really editor scaffolding.

    The single gate for every path that turns a quest title into text — the
    QuestNames index, GMRW's ParentQuestDisplay, a direct QUST reference, and
    the QuestName column of BOOK_*_Locations. It used to be duplicated and
    weaker in places (`full.startswith("[")`), which let two sentences through
    onto live pages:

        Reward for completing the quest: Enclave Activity: A Real Blast
                                         - <Alias=TargetLocationActual>
        Reward for completing the quest: [Tier 1 Dialogue for Caravan
                                         Decorator and companion robot]

    "<" is an alias the game substitutes at runtime and the export cannot
    resolve; "[" is a developer note, wherever in the string it appears
    (Bethesda ships a quest called "Vault 96: [NEED A NAME]").
    """
    n = (full or "").strip()
    if not n or "<" in n or "[" in n:
        return ""
    # Dev markers, as whole words. "cut" and "test" are NOT on this list on
    # purpose: "Making the Cut" and "Test Your Metal" are real quests, and the
    # blunt EditorID-shaped filter that rejects those would cost more than it
    # saves. These six have no innocent reading in a quest title.
    if re.search(r"(?<![A-Za-z])(deprecated|debug|zzz+|deleted|placeholder|nonplayable)"
                 r"(?![A-Za-z])", n, re.I):
        return ""
    # A "title" that is really an EditorID with the underscores knocked out —
    # "W05 RE Scene JN01 Debug". A bare content code among the words gives it
    # away; no real quest title contains one.
    if any(re.fullmatch(r"[A-Z]{1,4}\d{2,}", w) for w in n.split()):
        return ""
    # Real titles are capitalised. An all-lowercase fragment like "cut re" is a
    # working note somebody left in the FULL field.
    if not any(c.isupper() for c in n):
        return ""
    return n


def _clean_name(name):
    """Strip the wrapper the QUEST export puts round some display names."""
    n = usable_quest_name(name)
    n = re.sub(r"^\((?:Seasonal|Repeatable|Daily|Weekly)\)\s*", "", n)
    n = re.sub(r"^(?:Event|Quest|Daily|Repeatable):\s*", "", n)
    return n.strip()


def _split_tokens(parts):
    """Raw EditorID segments -> word tokens, numbers peeled off welded words.

    A map-cell code is emitted WHOLE so the caller's RX_CELL_CODE test can still
    see it. Peeling the digits off "Burn014" first would leave a bare "Burn",
    which then reads as the Burning Springs area code and prefixes a label that
    already names the place ("Burn Hocking Hills Station").
    """
    out = []
    for raw in parts:
        if RX_CELL_CODE.match(raw):
            out.append(raw)
            continue
        for t in _split_camel(raw):
            if not t:
                continue
            m = _RX_TRAILNUM.match(t)
            out.extend([m.group(1), m.group(2)] if m else [t])
    return out


# Burning Springs bounty targets: the NPC's own FULL ("The Reborn Pint-Sized
# Slasher") says nothing about WHERE you meet it. _BIG_ targets are the
# three-star Head Hunt bosses, _REG_/_SML_ the one/two-star Grunt Hunt targets
# (Default Three/Two/One Star Target templates). Duchess, 23 Sep 2026.
_RX_BOUNTY_NPC = re.compile(r"BountyTarget_(BIG|REG|SML)(?:_|$)", re.I)


def bounty_npc_label(npc_edid, full):
    """'Bounty Hunting: Head Hunt - <name>' for a bounty-target NPC, else None."""
    m = _RX_BOUNTY_NPC.search(npc_edid or "")
    if not m or not full:
        return None
    kind = "Head Hunt" if m.group(1).upper() == "BIG" else "Grunt Hunt"
    return f"Bounty Hunting: {kind} - {full}"


# EditorID fragment -> the loot bag its list is the contents of.
LOOT_BAG_SUFFIX = {"treasurehunt": "Mole Miner Pail"}

# Vendors named on their own, with no "<update/region> - " prefix.
TRAVELLING_TRADERS = {"minerva"}


def _shape_vendor(head, words):
    """Render a vendor route as a person and a currency, not a pile of tokens.

    'BS02_SpecialVendor_Minerva_LLS_GoldVendor_21' has everything a reader needs
    and puts none of it in a readable order — the live site shows "Steel Reign -
    Special Vendor Minerva Gold Vendor", and shows it twice because the chest
    and the stock list are separate records. What a reader wants is "Minerva
    (Gold Bullion vendor)".

    So: pull the currency qualifier out, pull the faction out, and treat
    whatever proper noun is left as the trader's name. A vendor with no name
    falls back to place and faction, which is how the faction vendor chests read
    ("Whitespring - Brotherhood of Steel vendor"). Returns None when there is
    nothing vendor-shaped here, so the caller carries on with the generic path.
    """
    qual = None
    rest = []
    for w in words:
        low = w.lower().replace(" ", "").replace(".", "")
        if low in VENDOR_WORDS:
            continue
        if low in VENDOR_QUALIFIER and not qual:
            qual = VENDOR_QUALIFIER[low]
            continue
        rest.append(w)

    joined = " ".join(rest).strip()
    faction = None
    for f in sorted(FACTIONS, key=len, reverse=True):
        m = re.search(r"(?<![A-Za-z])" + re.escape(f) + r"(?![A-Za-z])", joined, re.I)
        if m:
            faction = FACTIONS[f]          # canonical spelling, not what matched
            joined = (joined[:m.start()] + " " + joined[m.end():])
            break

    name = re.sub(r"\s+", " ", joined).strip()
    if name and name.lower() in TRAVELLING_TRADERS:
        # A trader who moves around has no place worth naming, and the head here
        # is only the update she shipped in ("Steel Reign"). Duchess, 23 Sep
        # 2026: "just the Minerva gold thing is fine".
        lead, place = name, None
    elif name:
        lead, place = name, head
    elif faction:
        # No trader name — these are the faction vendor chests, best identified
        # by where they stand and who runs them.
        lead, faction, place = faction, None, head
    elif head:
        lead, place = head, None
    else:
        return None

    bits = [b for b in (faction, qual) if b]
    label = f"{lead} ({' '.join(bits)} vendor)" if bits else f"{lead} vendor"
    if place and place.lower() not in label.lower():
        label = f"{place} - {label}"
    return label


# Names the EditorID can't produce on its own. Keyed by EditorID (list OR NPC).
# The Mischief Night party crasher: the Uninvited Pint-Sized Phantom crashes
# Mischief Night, and its drop list is SDOW_LLD_Creature_SlasherFan_PartyCrasher,
# which says neither "Mischief" nor "Night" — the live site read "The Slasher -
# Fan Party Crasher" (Duchess, 23 Sep 2026). The NPC override is read by
# build_plan_obtain_json before the NPC's own FULL name.
CURATED_LABELS = {
    "SDOW_LLD_Creature_SlasherFan_PartyCrasher":             "Mischief Night - Party Crasher",
    "SDOW_MischiefPartyCrasherRewards_Rare":                 "Mischief Night - Party Crasher",
    "SDOW_LvlSlasherFanBossPowerArmorHeavyAuto_PartyCrasher": "Mischief Night - Uninvited Pint-Sized Phantom",
}


# ─────────────────────────────────────────────────────────────────────────────
# Shared reward POOLS — named for the player, not the data-miner
# ─────────────────────────────────────────────────────────────────────────────
# A handful of list families are pools many sources draw from. Read off the
# EditorID they came out as "Schematic Weapon Burning Springs Melee", "Regions
# Grab Bag", "Mods Regions GRP" or "MILE Mystery Crate Ash Heap Roll", which
# means nothing to someone who doesn't datamine (Duchess, Oct 2026). The
# family is a fixed Bethesda convention, so the shape is matched and the words
# a player would use are put back. Only the region, size or item type is taken
# from the EditorID; nothing is per-plan.
REGION_WORDS = {
    "ashheap": "Ash Heap", "burningsprings": "Burning Springs",
    "cranberrybog": "Cranberry Bog", "forest": "The Forest", "mire": "The Mire",
    "savagedivide": "Savage Divide", "skylinevalley": "Skyline Valley",
    "toxicvalley": "Toxic Valley",
}
_POOL_KIND = {"weapon": "weapon", "weapons": "weapon", "armor": "armour", "cooking": "recipe",
              "aid": "aid", "mods": "mod", "mod": "mod", "powerarmor": "power armour",
              "workshop": "workshop", "all": "", "any": ""}
_RX_REGIONS = "|".join(REGION_WORDS)
_POOL_RULES = [
    # QuestReward_LLS_BurningSprings_All / _Any -> "Burning Springs quest rewards"
    (re.compile(rf"^QuestReward_LLS_(?P<r>{_RX_REGIONS})_(?:All|Any)$", re.I),
     lambda m: f"{REGION_WORDS[m['r'].lower()]} quest rewards"),
    (re.compile(r"^QuestReward_LLS_AllRegions_(?:All|Any)$", re.I),
     lambda m: "Quest rewards (any region)"),
    # ..._GrabBag: the extra random roll quests, events and encounters hand out
    (re.compile(rf"^QuestReward_LLS_(?P<r>{_RX_REGIONS})_GrabBag$", re.I),
     lambda m: f"{REGION_WORDS[m['r'].lower()]} bonus rewards"),
    (re.compile(r"^QuestReward_LLS_AllRegions_GrabBag$", re.I),
     lambda m: "Bonus rewards (any region)"),
    # QuestReward_LLS_Schematic_Weapon_BurningSprings_Melee and friends
    (re.compile(rf"^QuestReward_LLS_Schematic(?:_(?P<k>Weapon|Armor|Aid|Cooking|All))?"
                rf"(?:_(?P<r>{_RX_REGIONS}))(?:_(?P<x>Melee|Ranged))?(?:_All)?$", re.I),
     lambda m: f"{REGION_WORDS[m['r'].lower()]} quest rewards - "
               f"{(((m['x'] or '').lower() + ' ') if m['x'] else '')}"
               f"{_POOL_KIND.get((m['k'] or 'all').lower(), '') or 'any'} plans".replace("any plans", "plans").replace("recipe plans", "recipes")),
    # MILE_LL_MysteryCrate_AshHeap_MainRoll -> "Mole Miner Mystery Crate (Ash Heap)"
    (re.compile(rf"^MILE_LL_MysteryCrate_(?P<r>{_RX_REGIONS}|Basic)_(?:Main|Sub)Roll$", re.I),
     lambda m: "Mole Miner Mystery Crate" + ("" if m["r"].lower() == "basic"
                                            else f" ({REGION_WORDS[m['r'].lower()]})")),
    # LPI_Recipes_Mods_Weapons_AllRegions_GRP / LPI_Recipes_Weapons_Any_RegionMire_GRP:
    # the plans placed loose in the world, rolled per spot
    (re.compile(rf"^LPI_(?:Recipes_)?(?P<k>[A-Za-z_]+?)_(?:Any_)?(?:AllRegions|Region(?P<r>{_RX_REGIONS}))_GRP$", re.I),
     lambda m: "Plans lying in the world" + (f" ({REGION_WORDS[m['r'].lower()]})" if m["r"] else "")),
    # Any other plan spot placed in the world (LPI = a placed loot spot):
    # LPI_Recipes_Cooking_Tasty, LPI_Recipes_SURV_DiseaseCures,
    # Bunker_LPI_Recipes_Mod_Weapon_AlienBlaster_AmmoConv ...
    (re.compile(r"^(?:[A-Za-z0-9]+_)?LPI_Recipes_.+$", re.I),
     lambda m: "Plans lying in the world"),
    # LLE_Creature_Boss_Small(_Dynamic) -> boss enemies, by loot size
    (re.compile(r"^LLE_Creature_Boss_(?P<s>Small|Medium|Large)(?:_Dynamic)?$", re.I),
     lambda m: f"Boss enemies ({m['s'].lower()} loot)"),
    # RA_LL_Rewards_(Enclave)Activities: every Activity: quest pays from these
    (re.compile(r"^RA_LL_Rewards_Activities$", re.I), lambda m: "Activity rewards"),
    (re.compile(r"^RA_LL_Rewards_EnclaveActivities$", re.I), lambda m: "Enclave activity rewards"),
    (re.compile(r"^QuestReward_LLS_Schematic_Armor_Raider$", re.I),
     lambda m: "Raider quest rewards - armour plans"),
    # Treasure map mounds
    (re.compile(r"^LL_TreasureMap_Reward$", re.I), lambda m: "Treasure Map dig spots"),
]


_RX_SCHEMATIC_ANY = re.compile(r"^QuestReward_LLS_Schematic_(?P<rest>.+)$", re.I)


def pool_label(edid, quests=None):
    """Player-facing name for a shared reward pool, or None."""
    e = (edid or "").strip()
    for rx, fn in _POOL_RULES:
        m = rx.match(e)
        if m:
            return re.sub(r"\s+", " ", fn(m)).strip()
    # Any other schematic pool belongs to one quest or event: name that, and
    # say it pays plans. QuestReward_LLS_Schematic_CB15_Special -> CB15 is
    # Scorched Earth in the QUEST export.
    m = _RX_SCHEMATIC_ANY.match(e)
    if m:
        rest = re.sub(r"_(?:Special|All|Any)$", "", m["rest"], flags=re.I)
        head = source_label(rest, quests)
        if head and not head.lower().startswith("schematic"):
            return f"{head} - plans"
    return None


# Leftover editor wording in a finished label: a bare content code ("CB04",
# "GQ10", "MILE") or a pool word. Only used to decide whether the quest that
# pays the list out would name it better — never to drop a route.
_RX_WIRING_WORDS = re.compile(
    r"\b(?:Schematic|GRP|Grab Bag|MainRoll|Roll|Priority|Regions)\b"
    r"|(?<![\w.])[A-Z]{2,5}\d{0,3}[A-Z]?\d*(?![\w.])"
    r"|(?<![\w.])[A-Z][A-Za-z]{0,3}\d{2,3}(?![\w.])")
_WIRING_OK = {"ATLAS", "NWOT", "ENB", "BOS", "PA", "XP", "AI", "UFO", "TV", "OK"}


def looks_like_wiring(label):
    """True when a label still reads like an EditorID to a player."""
    for m in _RX_WIRING_WORDS.finditer(label or ""):
        if m.group(0).upper() not in _WIRING_OK:
            return True
    return False


_RX_ALIAS_TAIL = re.compile(r"\s*[-:,]?\s*<Alias=[^>]*>\s*$")


def strip_alias_suffix(title):
    """'Enclave Activity: Dropped Connection - <Alias=ParentLocation>' ->
    'Enclave Activity: Dropped Connection'. Only a TRAILING alias (the place
    the game fills in at runtime) is cut; an alias mid-title still fails
    usable_quest_name(), because the sentence around it would not read."""
    t = (title or "").strip()
    while True:
        u = _RX_ALIAS_TAIL.sub("", t).strip()
        if u == t:
            return u
        t = u


def direct_quest_label(entries, gmrw_quests, quest_titles):
    """The quest(s) whose reward records roll this list directly, or None.

    `entries` are the list's own non-list holders. Up to three titles are
    named; more than that is a shared pool and the pool's own name is kept.
    """
    names = []
    for rf, _redid, rsig in entries or ():
        rf = (rf or "").upper()
        t = gmrw_quests.get(rf) if rsig == "GMRW" else (
            quest_titles.get(rf) if rsig == "QUST" else None)
        t = strip_alias_suffix(t)
        if t and usable_quest_name(t):
            t = re.sub(r"^\(?Repeatable\)?:?\s+", "", t).strip()
            if t and t not in names:
                names.append(t)
    if not names or len(names) > 3:
        return None
    return ", ".join(sorted(names))


_RX_TIER_TOKEN = re.compile(r"^(?:Lvl|Level|Tier|Tranche|Alt)0*(\d+)$", re.I)


def tier_suffixes(rows_lists, parent_edid):
    """Tell apart rows that share a label but pay at different rates.

    `rows_lists` is one list of LVLI FormIDs per row. Returns one suffix per
    row ("" when nothing tells it apart). Radiation Rumble pays from three
    lists, ..._RewardList_Lvl2/3/4, at three rates; all three named
    "Event: Radiation Rumble", so the page printed the event twice at 2.5% and
    1.25% with nothing to say which was which. The token that differs between
    the lists is the difference, so it is what gets said.
    """
    toks = []
    for lists in rows_lists:
        t = set()
        for L in lists:
            t |= {p for p in re.split(r"[_\-]", parent_edid.get(L, "") or "") if p}
        toks.append(t)
    common = set.intersection(*toks) if toks else set()
    out = []
    for t in toks:
        own = sorted(t - common)
        word = ""
        for p in own:
            m = _RX_TIER_TOKEN.match(p)
            if m:
                word = f"tier {int(m.group(1))}"
                break
        if not word and len(own) == 1 and own[0].isalpha() and own[0].lower() not in PLUMBING:
            word = " ".join(_split_camel(own[0])).lower()
        out.append(word)
    # Only useful if it actually separates the rows.
    if len(set(out)) < len(out) or not all(out):
        return [""] * len(out)
    return out


def source_label(edid, quests=None):
    """Readable name for a leveled list, or None if it is not a real source.

    Returns e.g. 'Event: Seismic Activity', 'The Slasher: Masked Truth',
    'Minerva (Gold Bullion vendor)'. Lists whose leading code is editor-only
    return None so the caller can drop the route entirely.
    """
    raw = (edid or "").strip()
    if raw in CURATED_LABELS:
        return CURATED_LABELS[raw]
    if not raw:
        return None
    pooled = pool_label(raw, quests)
    if pooled:
        return pooled

    parts = [p for p in _RX_LIST_PFX.sub("", raw).replace("-", "_").split("_") if p]
    if not parts:
        return None
    if any(p.lower() in DEV_CODES for p in parts[:2]):
        return None
    if any(p.lower() in HARD_DEV_CODES for p in parts):
        return None

    # LLV is Bethesda's prefix for a vendor's stock list, and it is the ONLY
    # thing marking one as such on the faction lists — LLV_Faction_BoS has no
    # "vendor" token anywhere in it, which is why 325 live routes render as
    # "Faction Bo S" instead of naming a trader. The prefix is stripped before
    # tokenising, so the signal has to be taken from the raw EditorID here.
    is_vendor = bool(re.search(r"(?:^|_)LLV(?:_|$)", raw, re.I))

    # Two naming sources, and the one that matched MORE of the EditorID wins.
    #
    #   QUEST export  — specific, live, knows content shipped after any map edit
    #   AREA_CODE     — curated region and system names, hand-verified
    #
    # Whichever consumed more leading tokens understood more of the name, so it
    # is the better answer. Ties go to AREA_CODE, and that tiebreak is the whole
    # point of doing it this way: LL_DailyOps_Rewards_HighLVL_Chase_Rare is the
    # Daily Ops reward pool, but "DailyOps" also happens to be the unique prefix
    # of one incidental misc quest — DailyOps_VernonDodge_MiscQuest, "Breaking
    # Radio Silence". Letting the quest win renamed EVERY Daily Ops drop after a
    # single side quest. Both matched one token, so the curated name holds.
    #
    # A genuinely more specific quest still wins: Burn_MQ03 matches two tokens
    # against Burn's one, so that route is "Might Makes Right" and not the
    # region it happens to sit in.
    q_head, q_used = (quests.lookup(parts) if quests else (None, 0))

    a_head, a_used = None, 0
    low = [p.lower() for p in parts]
    for n in (3, 2, 1):
        if len(low) < n:
            continue
        for key in ("_".join(low[:n]), "".join(low[:n])):
            if key in AREA_CODE:
                a_head, a_used = AREA_CODE[key], n
                break
        if a_head:
            break

    if q_head and q_used > a_used:
        head, used = q_head, q_used
    elif a_head:
        head, used = a_head, a_used
    else:
        head, used = q_head, q_used

    rest_parts = parts[used:] if head else parts
    if head:
        head = re.sub(r"^\((?:Seasonal|Repeatable)\)\s*", "", head).strip()

    # 3. Whatever is left, cleaned: quest-set collapse, plumbing, cell codes,
    #    bare numbers, acronym expansion.
    kind, keep = None, []
    # "Repeat" / "RepeatTier" / "Repeatable" says WHEN the list pays out (repeat
    # runs, after the first clear of the day), not where. It is kept out of the
    # words and put back as a " (Repeatable)" suffix, so
    #     SDOW_DailyOps_LL_Rewards_RepeatTier -> "The Slasher - Daily Ops (Repeatable)"
    # sits next to the first-clear row instead of being merged into it
    # (Duchess, 23 Sep 2026; route_key no longer strips the word).
    repeatable = False
    toks = list(_split_tokens(rest_parts))
    # "Event Specific" (and Bethesda's "Recipies") is how a list says it is the
    # event's own pool — restating the head it sits under. MN2_LL_EventSpecific-
    # Recipies printed as a second row "... Event Specific Recipies" beside the
    # event's own row at the same rate.
    _drop = set()
    for i, t in enumerate(toks):
        lt = t.lower()
        if lt == "specific" and i and toks[i - 1].lower() == "event":
            _drop.update({i - 1, i})
        elif lt in ("eventspecific", "recipies"):
            _drop.add(i)
    toks = [t for i, t in enumerate(toks) if i not in _drop]
    for t in toks:
        if re.fullmatch(r"(SQ|MQ|DQ)\d*", t, re.I):
            # The data does not say WHICH side quest; pretending it does would
            # be worse than collapsing them.
            kind = {"sq": "Side Quests", "mq": "Main Quests",
                    "dq": "Daily Quests"}[t[:2].lower()]
            continue
        low = t.lower()
        if low in ("repeat", "repeats", "repeatable", "repeattier"):
            repeatable = True
            continue
        if low in ("sidequest", "sidequests"):
            kind = "Side Quests"; continue
        if low in ("mainquest", "mainquests"):
            kind = "Main Quests"; continue
        if low in VENDOR_WORDS:
            is_vendor = True
        # HARD_DEV_CODES, not DEV_CODES: the wider set contains "test", and a
        # blanket mid-name ban turns "Event: Test Your Metal" into "Event Your
        # Metal". A list whose name genuinely leads with a dev code was already
        # rejected outright above.
        if low in PLUMBING or low in HARD_DEV_CODES:
            continue
        if RX_CELL_CODE.match(t) or t.isdigit():
            continue
        # A lone letter is always wreckage from a split, never a word a reader
        # wants: "W05_Wayward_002P_LL_AnchorFarm" breaks "002P" into "002" and
        # "P", the digits drop, and the orphan P rides along into the label as
        # "Wastelanders - Wayward P Anchor Farm".
        if len(t) == 1:
            continue
        # A content code can appear AFTER the head as well as at it —
        # "Infestations_LL_Rewards_SpecialEvents_SDOW" is the Slasher variant of
        # the Infestation pool, and leaving the raw SDOW in the label loses the
        # one thing that row was telling the reader.
        w = (AREA_CODE.get(low)
             or (quests.family.get(low) if quests else None)
             or expand(t))
        if w:
            keep.append(w)

    # Collapse repeats the expansion can create ("Vendor ... Vendor").
    dedup = []
    for w in keep:
        if not dedup or dedup[-1].lower() != w.lower():
            dedup.append(w)
    # A word already inside the resolved head is noise after it.
    if head:
        hl = head.lower()
        dedup = [w for w in dedup if w.lower() not in hl]

    # A resolved quest name that already says "vendor" is the vendor's name —
    # the QUEST export really does carry one called "Camp - Clinic Vendor" — so
    # shaping it again appends a second one and publishes "Camp - Clinic Vendor
    # vendor". When the head has said it, leave the wording to the head.
    head_is_vendor = bool(head) and any(
        re.search(r"(?<![A-Za-z])" + w + r"s?(?![A-Za-z])", head, re.I)
        for w in VENDOR_WORDS)

    if is_vendor and not head_is_vendor:
        shaped = _shape_vendor(head, dedup)
        if shaped:
            return shaped

    tail = " ".join(dedup).strip()
    if kind:
        tail = f"{tail} {kind}".strip() if tail else kind

    if head and tail:
        label = f"{head} - {tail}"
    else:
        label = head or tail or None
    if label and repeatable and "(repeatable)" not in label.lower():
        label = f"{label} (Repeatable)"
    # Lists that are the CONTENTS of a loot bag the player opens, so the page
    # says the plan comes out of the bag, not off the enemy (Duchess, 23 Sep
    # 2026). Treasure Hunter's reward lists open from the Mole Miner Pails you
    # dig up (c_TreasureHunt_EmptyChest_Tier_01-03 = Dusty / plain / Ornate).
    if label:
        for code, bag in LOOT_BAG_SUFFIX.items():
            if code in raw.lower() and f"({bag.lower()})" not in label.lower():
                label = f"{label} ({bag})"
    # Append the activity type for a bare quest/phase name ("... Out of the
    # Shadows" -> "... (Infestations)"). Keyed on the resolved head so the type
    # is looked up from the quest, not the whole assembled label.
    return annotate_content_type(label, quests, name=head or label)


# Words that name how RARE a pool is, not where it is. Two rows differing only
# by one of these are the same place to go, and the resolved percentage already
# says which tier you landed in — so they collapse when their rates match.
RARITY_WORDS = {"rare", "common", "uncommon", "ultra", "ultrarare",
                "high", "low", "best", "good", "great", "base",
                "bonus", "extra"}
# "repeat"/"repeatable" are deliberately NOT rarity words any more (23 Sep 2026):
# a repeat-run reward list is a different way to get the plan, so "The Slasher -
# Daily Ops" and "... (Repeatable)" stay as two rows.

_KEY_STOP = {"vendor", "chest", "the", "of", "and", "a", "an", "for", "from"}


_GENERIC_QUEST_KIND = re.compile(r"\b(Side|Main|Daily) Quests\b")


def gmrw_quests_for(lvli_fid, lvli_refs, gmrw_quests, depth=3):
    """Quest titles of the GMRW rewards that hand out this leveled list.

    Walks UP the list's ReferencedBy (list -> parent list -> ... -> GMRW), at
    most `depth` steps, and returns the ParentQuestDisplay of every GMRW it
    reaches, de-duplicated and sorted. Only titles that passed
    usable_quest_name() are in `gmrw_quests`, so nothing unprintable leaks.
    """
    found, seen = set(), set()
    frontier = [str(lvli_fid).upper()]
    for _ in range(depth):
        nxt = []
        for fid in frontier:
            if fid in seen:
                continue
            seen.add(fid)
            for rf, _redid, rsig in (lvli_refs.get(fid) or ()):
                rf = (rf or "").upper()
                if rsig == "GMRW":
                    q = gmrw_quests.get(rf)
                    if q:
                        found.add(q)
                elif rsig == "LVLI":
                    nxt.append(rf)
        frontier = nxt
        if not frontier:
            break
    return sorted(found)


def quest_route_label(fam, lvli_fid, lvli_refs, gmrw_quests):
    """Swap a generic "Side Quests" / "Main Quests" / "Daily Quests" route name
    for the real quest title(s), or return None to keep `fam`.

    The EditorID only says SQ01, so source_label() can only print "Side
    Quests" — which tells a reader nothing ("what side quest?!", Duchess,
    23 Sep 2026). The GMRW that pays the list out DOES name the quest, in
    ParentQuestDisplay:
        SDOW_LL_SQ01_RepeatableRewards <- SDOW_SQ01_QuestRewards (GMRW)
            "Repeatable: Disturbed Grave"
        -> "The Slasher - Disturbed Grave (Repeatable)"
    The area/family prefix before " - " is kept; everything after it is
    replaced by the quest name(s). A "Repeatable: " title prefix becomes the
    usual " (Repeatable)" suffix. Up to three quests are listed; more than
    that and the generic name is kept rather than printing a wall of titles.
    """
    if not fam or not _GENERIC_QUEST_KIND.search(fam):
        return None
    names = gmrw_quests_for(lvli_fid, lvli_refs, gmrw_quests)
    if not names or len(names) > 3:
        return None
    repeatable = fam.endswith("(Repeatable)")
    clean = []
    for n in names:
        m = re.match(r"^\(?Repeatable\)?:?\s+(.*)$", n)
        if m:
            repeatable = True
            n = m.group(1).strip()
        if n and n not in clean:
            clean.append(n)
    if not clean:
        return None
    head = fam.split(" - ", 1)[0] if " - " in fam else ""
    label = ", ".join(clean)
    if head:
        label = head + " - " + label
    if repeatable:
        label += " (Repeatable)"
    return label


def route_key(label):
    """Normalised identity for a route, for collapsing duplicate rows.

    The same vendor reaches a plan as both a CONT (its chest) and an LVLI (its
    stock list) and used to render twice — "Minerva Gold Vendor Chest" and
    "Minerva LLV Gold Vendor" at the same rate. Reduce both to their content
    words and the pair becomes one row.

    Rarity words go too, so "The Slasher - Daily Ops", "... Daily Ops Rare" and
    "... Daily Ops Repeat" — three rows, all 11.11%, all meaning "do the event's
    Daily Op" — become one. The caller only merges rows whose RATES also match,
    so two genuinely different pools that happen to share a tier word are never
    folded together.
    """
    words = [w for w in re.split(r"\W+", (label or "").lower()) if w]
    words = [w for w in words
             if w not in PLUMBING and w not in _KEY_STOP and w not in RARITY_WORDS]
    return " ".join(sorted(set(words)))


# ─────────────────────────────────────────────────────────────────────────────
# 3. UNLOCKS
# ─────────────────────────────────────────────────────────────────────────────
_RX_CHAL_REWARD = re.compile(r"^ChallengeReward[_-]", re.I)

# A humanised EditorID that still opens with the scaffolding word is editor
# wiring in prose, not a name. 18 live plans published "Reward for completing
# the challenge: Challenge Lifetime Burning Springs Bounty Complete Grunt
# Hunts", which is the record's path with the underscores taken out.
#
# There is nothing better to say from the EditorID alone: the reward records are
# named `..._CompleteGruntHunts_CarStashBox01` while the challenges they belong
# to are `..._CompletedGrunt_03`, so no prefix of one names the other. The
# honest answer is the generic sentence — and 15 of the 18 also carry a real
# COBJ.GNAM sentence ("Reward for completing the challenge: Complete Grunt
# Hunts"), so suppressing this one loses nothing and removes a contradiction.
_RX_CHAL_SCAFFOLD = re.compile(
    r"^\s*challenge(?:s)?\b|\bchallenge\s+(?:lifetime|daily|weekly|event|seasonal)\b",
    re.I)


def usable_challenge_name(name):
    """A challenge name fit to print, or "" — the CHAL analogue of
    usable_quest_name()."""
    n = (name or "").strip()
    if not n or "<" in n or "[" in n:
        return ""
    return "" if _RX_CHAL_SCAFFOLD.search(n) else n
_RX_QUEST_REWARD = re.compile(r"QuestReward", re.I)


# Update/expansion names the EditorID-derived labels lead with ("Wastelanders -
# Molly"). They say when a trader shipped, not who or where they are, so they go
# once the trader is named after the NPC (Duchess, 23 Sep 2026, on Minerva).
_UPDATE_HEADS = ("Wastelanders", "Steel Dawn", "Steel Reign")
_STOPWORDS = {"the", "of", "and", "a", "an", "in"}
_RX_GENERIC_VENDOR_FULL = re.compile(r"\bvendor(?: bot)?$", re.I)


class VendorNames:
    """Vendor chest (CONT) FormID -> the vendor NPC's in-game name.

    Read from the NPC2_Vendors export, which joins each vendor NPC to its
    merchant faction's chest (MerchantContainerBase). Before this, vendor routes
    were named from EditorIDs: "E05 Caravan Carver vendor" for Carver
    Timmerman, "GQ10 Travelling Workshops vendor" for Grahm, and Watoga's
    Vendor Bot Phoenix as "Location based PA System in Watago - Watoga
    (Brotherhood of Steel vendor)" (Duchess, 6 Oct 2026: use the names players
    see).

    A chest is named after its NPC only when the export gives exactly ONE
    usable name for it. A name is not usable when it is blank, when it ends in
    "Vendor"/"Vendor Bot" ("Raiders Vendor", "Watoga Vendor Bot" — a
    description, not a name), or when several different NPC records carry it
    ("Vendor Bot Responder" stands at six stations). Those chests keep their
    place-and-faction label, which already says more than the NPC would.
    """

    def __init__(self, tsv_root=None, newest=None):
        self.by_chest = {}
        path = None
        if tsv_root:
            if newest:
                path = newest("NPC2_Vendors_*.tsv", tsv_root)
            else:
                cands = sorted(glob.glob(os.path.join(tsv_root, "NPC2_Vendors_*.tsv")))
                cands = [c for c in cands if not c.endswith("_Placements.tsv")]
                path = cands[-1] if cands else None
        if not path or path.endswith("_Placements.tsv") or not os.path.exists(path):
            return
        rows = []
        with open(path, encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                chest = (r.get("MerchantContainerBase_FormID") or "").strip().upper()
                full = (r.get("NPC_FULL") or "").strip()
                nedid = (r.get("NPC_EDID") or "").strip()
                if chest and full and not is_dev_record(nedid):
                    rows.append((chest, full, nedid))
        npcs_per_full = collections.defaultdict(set)
        for chest, full, nedid in rows:
            npcs_per_full[full].add(nedid)
        names = collections.defaultdict(set)
        for chest, full, nedid in rows:
            if _RX_GENERIC_VENDOR_FULL.search(full) or len(npcs_per_full[full]) > 1:
                names[chest].add(None)
            elif usable_quest_name(full):
                names[chest].add(full)
        for chest, s in names.items():
            if len(s) == 1 and None not in s:
                self.by_chest[chest] = next(iter(s))

    def name(self, chest_fid):
        return self.by_chest.get((chest_fid or "").strip().upper())

    _GENERIC_TOKENS = {"vendor", "vendors", "ll", "llv", "lls", "chest", "vendorchest",
                       "master", "recipes", "recipe", "merchant", "the", "of", "hq"}

    @staticmethod
    def _tokens(text):
        out = set()
        for part in re.split(r"[_\s\-]+", text or ""):
            for t in re.findall(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+", part):
                out.add(t.lower())
        return out

    def orphan_chest(self, list_edid, chest_edid_of):
        """The chest a vendor stock list with NO references belongs to, by its
        EditorID: Vendor_MILE_CryptidHunter -> MILE_HQ_CryptidHunter_VendorChest
        (Windy), NWOT_LL_Vendor_Chloe -> NWOT_Clown_VendorChest (Chloe the
        Clown). Needs one clear winner sharing two words, or a word of the
        NPC's own name; otherwise None and the list keeps its EditorID label."""
        lt = self._tokens(list_edid) - self._GENERIC_TOKENS
        if not lt:
            return None
        scored = []
        for chest, npc in self.by_chest.items():
            ct = self._tokens(chest_edid_of.get(chest, "")) - self._GENERIC_TOKENS
            nt = self._tokens(npc) - self._GENERIC_TOKENS
            hit = lt & (ct | nt)
            ok = len(hit) >= 2 or bool(lt & nt)
            if ok:
                scored.append((len(hit), chest))
        if not scored:
            return None
        scored.sort(reverse=True)
        if len(scored) > 1 and scored[0][0] == scored[1][0]:
            return None
        return scored[0][1]

    def label(self, chest_fid, base):
        """'{NPC} ({what the old label said, minus the NPC and the codes})'.

        "Camden Park (Responders vendor)"  -> "Vendor Bot Chad (Camden Park, Responders vendor)"
        "Wastelanders - Molly (Raiders vendor)" -> "Molly (Raiders vendor)"
        "GQ10 Travelling Workshops vendor" -> "Grahm (Travelling Workshops vendor)"
        Returns `base` unchanged when the chest has no usable NPC name.
        """
        npc = self.name(chest_fid)
        if not npc or not base:
            return base
        m = re.match(r"^(.*?)\s*\(([^()]*)\)\s*$", base)
        head, qual = (m.group(1), m.group(2)) if m else (re.sub(r"\s*\bvendor\s*$", "", base, flags=re.I), "vendor")
        for u in _UPDATE_HEADS:
            head = re.sub(r"^" + re.escape(u) + r"\s*-\s*", "", head)
        # A heading segment that reads as a sentence is a quest's working title
        # borrowed by QuestNames ("Location based PA System in Watago - Watoga"
        # for LC170), not a place: keep the short segments only.
        segs = head.split(" - ")
        head = " - ".join([g for g in segs[:-1] if len(g.split()) <= 3] + segs[-1:])
        npc_words = {w.lower() for w in re.findall(r"[A-Za-z0-9'.]+", npc)}
        keep = []
        for w in re.split(r"\s+", head.replace(" - ", " ")):
            if not w or w == "-" or w.lower() in npc_words or looks_like_wiring(w):
                continue
            # the EditorID's spelling of the same person ("Reginald" for Regs)
            lw = w.lower()
            if len(lw) >= 3 and any(len(n) >= 4 and (lw.startswith(n[:-1]) or n.startswith(lw[:-1]))
                                    for n in npc_words):
                continue
            keep.append(w)
        while keep and keep[0].lower() in _STOPWORDS:
            keep.pop(0)
        while keep and keep[-1].lower() in _STOPWORDS:
            keep.pop()
        place = " ".join(keep).strip(" ,-")
        if qual.lower() == "vendor":
            inner = f"{place} vendor" if place else "vendor"
        else:
            inner = f"{place}, {qual}" if place else qual
        return f"{npc} ({inner})"


class UnlockIndex:
    """Non-drop routes for a plan: bought, quested, challenged, or placed.

    Built once per run from the newest GMRW / QUEST / CONT / TERM exports and
    the BOOK_*_Locations companion, then queried per plan.
    """

    def __init__(self, tsv_root, newest):
        self._quests_by_fid = {}
        self._gmrw_by_fid = {}
        self.gmrw_quests = {}   # GMRW FormID -> printable ParentQuestDisplay
        self._gmrw_rewards = collections.defaultdict(list)
        self._names = {}          # FormID -> FULL/EDID for CONT, TERM, NPC_
        self._locations = {}
        self._chal_names = {}     # CHAL EditorID (lowered) -> its display title
        self.quest_names = QuestNames()

        # The challenge's own title. A ChallengeReward GMRW is named after the
        # CHAL record it belongs to, so stripping the prefix leaves an EditorID
        # that matches one exactly — and until this index existed, humanize()
        # turned it into prose: the four fishing reels published "Reward for
        # completing the challenge: Challenge Lifetime Fishing Progress", when
        # the game calls it Fish Quest I-IV. An EditorID reaching a published
        # sentence is the leak check_source_labels.py exists to catch.
        cf = newest("CHAL_Export_*.tsv", tsv_root)
        if cf:
            with open(cf, encoding="utf-8", errors="replace") as f:
                for r in csv.DictReader(f, delimiter="\t"):
                    ce = (r.get("EDID") or "").strip().lower()
                    title = (r.get("FULL") or "").strip()
                    if ce and title and usable_quest_name(title):
                        self._chal_names[ce] = _clean_name(title)

        qf = newest("QUEST_Export_*.tsv", tsv_root)
        if qf:
            self.quest_names.load(qf)
            with open(qf, encoding="utf-8", errors="replace") as f:
                for r in csv.DictReader(f, delimiter="\t"):
                    fid = (r.get("FormID") or "").strip().upper()
                    full = (r.get("FULL - Name") or "").strip()
                    if fid and usable_quest_name(full):
                        self._quests_by_fid[fid] = full

        gf = newest("GMRW_Export_*.tsv", tsv_root)
        if gf:
            with open(gf, encoding="utf-8", errors="replace") as f:
                for r in csv.DictReader(f, delimiter="\t"):
                    fid = (r.get("FormID") or "").strip().upper()
                    rec = {"edid": (r.get("EDID") or "").strip(),
                           "quest": (r.get("ParentQuestDisplay") or "").strip()}
                    if fid:
                        self._gmrw_by_fid[fid] = rec
                        # The quest that actually USES the reward (its
                        # ReferencedBy QUST) beats ParentQuestDisplay: the Hells
                        # Eagles reward's parent field points at Custodial
                        # Compulsions, but only the Hells Eagles quest uses it.
                        qname = ""
                        for k, v in r.items():
                            if k and k.startswith("Ref") and k[3:].isdigit() and v:
                                parts = v.split(":")
                                if len(parts) >= 3 and parts[-1].strip() == "QUST":
                                    qname = self._quests_by_fid.get(parts[0].strip().upper(), "")
                                    if qname:
                                        break
                        qname = qname or rec["quest"]
                        if usable_quest_name(qname):
                            self.gmrw_quests[fid] = qname
                    item = (r.get("RewardedItem") or "").strip()
                    if item:
                        self._gmrw_rewards[item.split(":")[0].strip().upper()].append(rec)

        for pat, cols in (("CONT_Export_*.tsv", ("FormID", "EDID", "FULL")),
                          ("NPC_Export_*.tsv",  ("FormID", "EDID", "FULL"))):
            f = newest(pat, tsv_root)
            if not f:
                continue
            with open(f, encoding="utf-8", errors="replace") as fh:
                for r in csv.DictReader(fh, delimiter="\t"):
                    fid = (r.get(cols[0]) or "").strip().upper()
                    if fid and fid not in self._names:
                        self._names[fid] = ((r.get(cols[2]) or "").strip(),
                                            (r.get(cols[1]) or "").strip())

        lf = newest("BOOK_Export_*_Locations.tsv", tsv_root)
        if lf:
            with open(lf, encoding="utf-8", errors="replace") as f:
                for r in csv.DictReader(f, delimiter="\t"):
                    fid = (r.get("BOOK_FormID") or "").strip().upper()
                    if fid:
                        self._locations.setdefault(fid, []).append(
                            ((r.get("LocationName") or "").strip(),
                             (r.get("LocationSource") or "").strip(),
                             (r.get("QuestName") or "").strip()))

    # A container's FULL name is what the player sees on the world object, and
    # for most of them it is a bare noun — every safe in the game is called
    # "Safe". On its own that makes a useless route ("Found in Safe"), so where
    # the name is one of these the EditorID is used to say WHICH one.
    GENERIC_CONTAINERS = {"safe", "locker", "chest", "container", "box", "crate",
                          "footlocker", "toolbox", "cooler", "duffle bag",
                          "ammo box", "first aid box", "desk", "cabinet", "bag"}

    # ── helpers ─────────────────────────────────────────────────────────────
    def display_name(self, fid, fallback_edid=""):
        """Best readable name for a referenced record.

        A generic container name is qualified with whatever the EditorID adds,
        so RSVP03_Container_Safe_Miguel reads "Safe (Miguel)" rather than the
        useless "Safe" its FULL name gives on its own.
        """
        full, edid = self._names.get((fid or "").upper(), ("", ""))
        src = edid or fallback_edid
        derived = source_label(src, self.quest_names) or ""
        if full and full.strip().lower() not in self.GENERIC_CONTAINERS:
            return full
        if full and derived:
            extra = " ".join(w for w in derived.split()
                             if w.lower() not in full.lower().split())
            return f"{full} ({extra})" if extra else full
        return full or derived or src

    def _gmrw_sentence(self, rec):
        edid, quest = rec.get("edid", ""), _clean_name(rec.get("quest"))
        if any(edid.upper().startswith(p) for p in CUT_PREFIXES):
            return None
        if _RX_CHAL_REWARD.search(edid):
            stem = _RX_CHAL_REWARD.sub("", edid)
            # The challenge's own title first — the reward record is named after
            # it, so this is a lookup, not a guess. source_label() is the
            # fallback for a reward whose challenge the export does not carry.
            title = self._chal_title(stem)
            what = title or usable_challenge_name(source_label(stem, self.quest_names))
            return f"Reward for completing the challenge: {what}" if what else \
                   "Reward for completing a challenge."
        if quest:
            return ("Reward for completing the quest: "
                    f"{annotate_content_type(quest, self.quest_names)}")
        if _RX_QUEST_REWARD.search(edid):
            what = source_label(edid, self.quest_names)
            return f"Quest reward: {what}" if what else "Awarded as a quest reward."
        return None

    def _chal_title(self, stem):
        """The challenge title behind a ChallengeReward EditorID stem.

        The reward and the challenge do not always spell the name identically:
        `ChallengeReward_Challenge_Lifetime_Fishing_Progress_01` belongs to
        `Challenge_Lifetime_Fishing_Progress_01_META`, the game's Fish Quest I.
        So: exact, then the `_META` form, then a prefix that names exactly one
        challenge. An ambiguous prefix resolves to nothing and falls through to
        source_label() — the same rule QuestNames already follows, and for the
        same reason.
        """
        key = (stem or "").strip().lower()
        if not key:
            return ""
        hit = self._chal_names.get(key) or self._chal_names.get(key + "_meta")
        if hit:
            return hit
        matches = {v for k, v in self._chal_names.items() if k.startswith(key)}
        return matches.pop() if len(matches) == 1 else ""

    # ── the query ───────────────────────────────────────────────────────────
    def unlocks_for(self, book_fid, book_edid, refs, has_routes=False):
        """Plain-English unlock routes for one plan.

        `refs` is the BOOK's ReferencedBy list as exported, each entry
        "FORMID:EDID:SIG". Returns a de-duplicated list of sentences, resolved
        ones first; convention-derived ones are worded so they cannot be read as
        resolved.

        `has_routes` suppresses the convention-derived hints. They exist to say
        something where nothing was resolved, and once a real route IS resolved
        they can only contradict it: "Plan: Cannery Recipe Bundle #1" is named
        SCORE_S25_Recipe_CanneryG1_StampVendor but actually resolves to Settler
        Samuel's gold-bullion stock at 100%. The EditorID is stale, the resolved
        route is not, and printing both would tell a reader to walk to the wrong
        vendor.
        """
        fid = (book_fid or "").upper()
        out, seen = [], set()

        def add(s):
            if s and s.lower() not in seen:
                seen.add(s.lower())
                out.append(s)

        parsed = []
        for ref in refs or []:
            bits = (ref or "").split(":")
            if len(bits) >= 3:
                parsed.append((bits[0].strip().upper(), ":".join(bits[1:-1]).strip(),
                               bits[-1].strip().upper()))

        # ── resolved: a record links this plan to a source ──────────────────
        for rfid, redid, rsig in parsed:
            if any(redid.upper().startswith(p) for p in CUT_PREFIXES):
                continue
            if rsig == "GMRW":
                add(self._gmrw_sentence(self._gmrw_by_fid.get(rfid, {"edid": redid})))
            elif rsig == "QUST":
                q = self._quests_by_fid.get(rfid) or _clean_name(
                    source_label(redid, self.quest_names) or "")
                if q:
                    add("Reward for completing the quest: "
                        + annotate_content_type(_clean_name(q), self.quest_names))
            elif rsig == "TERM":
                nm = source_label(redid, self.quest_names)
                if nm and not is_test_cell_name(nm):
                    add(f"Redeemed at the {nm}")
            elif rsig == "CONT":
                if any(w in redid.lower() for w in ("test", "debug", "qa", "zw")):
                    continue
                nm = self.display_name(rfid, redid)
                if nm and not is_test_cell_name(nm):
                    add(f"Found in {nm}")

        for rec in self._gmrw_rewards.get(fid, []):
            add(self._gmrw_sentence(rec))

        for loc_name, loc_src, quest in self._locations.get(fid, []):
            if quest:
                add("Reward for completing the quest: "
                    + annotate_content_type(_clean_name(quest), self.quest_names))
            if not loc_name or is_test_cell_name(loc_name):
                continue
            place = parse_ext_cell_location(loc_name) if loc_src == "ExtCell" else loc_name
            if place and not is_test_cell_name(place):
                add(f"Found at a fixed spawn point: {place}")

        # ── convention: the EditorID says where it came from, no link ───────
        if not has_routes:
            for s in self._edid_hints(book_edid):
                add(s)
        return out

    def _edid_hints(self, edid):
        """Routes Bethesda's naming convention implies. Worded as inference.

        One sentence, not a stack of them. A plan called
        SCORE_S26_Recipe_Cryolator_ColdSurgeMuzzle_StampVendor matches both the
        season pattern and the vendor pattern, and two "Named as ..." lines
        saying half a fact each read worse than one saying the whole one.
        """
        e = edid or ""
        where = season = None

        if re.search(r"_StampVendor\b", e, re.I):
            where = "stamp-vendor stock (Giuseppe Delcavo, Whitespring Refuge)"
        elif re.search(r"_GoldVendor\b|_Bullion\b", e, re.I):
            where = "gold-bullion vendor stock"
        elif re.search(r"(^|_)ATX_", e, re.I):
            where = "an Atom Shop item"

        m = re.search(r"SCORE_S(\d+)_", e, re.I)
        mm = re.search(r"SCORE_MiniSeason_([A-Za-z]+)", e, re.I)
        if m:
            season = f"a Season {int(m.group(1))} reward"
        elif mm:
            season = f"a {' '.join(_split_camel(mm.group(1)))} mini-season reward"

        if season and where:
            return [f"Named in the game files as {season}, sold as {where}."]
        if season:
            return [f"Named as {season} in the game files."]
        if where:
            return [f"Named as {where} in the game files."]
        return []


# ════════════════════════════════════════════════════════════════════════════
# 4. THE OBTAIN LEDGER
# ════════════════════════════════════════════════════════════════════════════
r"""
The fixed route table every plan page prints under "How to Obtain".

It is the same ledger the CAMP item pages use (camp-item-expands skill, section
2): a fixed list of routes in a fixed order, and EVERY row is printed, including
the ones that do not apply. The N/A rows are the point — a reader who sees two
lines cannot tell whether the other eight sources were checked or forgotten,
while a reader who sees eight N/As knows the plan genuinely is not sold, not on
a scoreboard and not in a bundle.

The JSON carries only the rows that apply; the renderer holds the fixed order
and fills the gaps with N/A. Same page, a quarter of the bytes.

This owns no maths and resolves nothing new. It is a pure re-sort of what the
builder already worked out — obtain_routes (the rng76-resolved percentages,
which stay on the row, one line per source) and obtain_unlocks (the plain
sentences for routes that are not random loot) — into ten labelled buckets.

Plans get one row the CAMP pages do not: Containers. Most plans in this game are
world loot out of a container, and folding that into "Events & Activities" would
have told the reader to go and do an event for something that sits in a locker.
"""

LEDGER_ROWS = ["Caps", "Stamps", "Scoreboard", "Gold Bullion", "Atom Shop",
               "Limited Time Bundle", "Containers", "Enemies", "Scrap to Learn",
               "Events & Activities", "Quests", "Challenges"]
# "Enemies" (Oct 2026): creature drops used to fall through to Events &
# Activities, so a plan that drops off a Deathclaw read as an event. Keep this
# list and OBTAIN_LEDGER_ROWS in df-bnb-plan-checklists.js in the same order.

# How many sources one row lists before it is cut short with "and N more".
# Eight is where the faction vendor lists stop reading as a source list and
# start reading as a wall — the row says WHERE, it does not enumerate the pool.
LEDGER_MAX_ROWS = 8

_RX_LED_GOLD  = re.compile(r"gold[\s-]*bullion|bullion", re.I)
# The currency is read off the ROUTE'S LABEL, and a named trader's label does
# not carry one: the Expeditions vendor resolves as "Expeditions - Giuseppe
# vendor", which says "vendor" and never says "stamp", so all 133 plans on his
# shelf were filed under Caps. Giuseppe Delcavo trades stamps — this file
# already says so, in the _edid_hints sentence for _StampVendor plans — so the
# name is named here too, where the ledger can see it.
#
# This is the RESOLVED route being read correctly, not the EditorID convention
# being trusted: a plan whose EditorID says _StampVendor but that resolves to
# Settler Samuel's gold-bullion stock still lands in Gold Bullion, because the
# label is what is tested and his label says bullion.
_RX_LED_STAMP = re.compile(r"\bstamps?\b|stamp[\s-]*vendor|\bgiuseppe\b", re.I)
_RX_LED_SHOP  = re.compile(r"vendor|trader|merchant|shop", re.I)


def _ledger_route_bucket(route):
    """Which ledger row one resolved drop route belongs in."""
    label = route.get("route") or ""
    st    = (route.get("source_type") or "").lower()

    if st == "vendor":
        # Order matters — a gold-bullion or stamp trader is a vendor too, and
        # the currency is the thing the reader came to the row for.
        if _RX_LED_GOLD.search(label):
            return "Gold Bullion"
        if _RX_LED_STAMP.search(label):
            return "Stamps"
        # "Atom Shop - Ally Lawson vendor" is a CAPS route: the ALLY was bought
        # from the Atom Shop, the plan on their shelf is bought with caps. It
        # stays out of the Atom Shop row on purpose.
        if _RX_LED_SHOP.search(label):
            return "Caps"
        # Holders like "Locker" that classified as vendor plumbing are world
        # loot, not a shop.
        return "Containers"

    if st in ("container", "fixed"):
        return "Containers"

    # `kind` is set by plan_route_kinds.py off the records themselves: the
    # Quest Type of the quest whose reward pays the list out, and the NPC that
    # carries a creature list. Without it, every quest reward read as an event.
    kind = (route.get("kind") or "").lower()
    if st == "creature":
        return "Containers" if kind == "corpse" else "Enemies"
    if st == "event-quest" and kind == "quest":
        return "Quests"

    # events, and anything a later builder adds.
    return "Events & Activities"


def _ledger_unlock_bucket(sentence):
    """Which ledger row one plain-English unlock sentence belongs in."""
    s = (sentence or "").lower()
    # "Scrap a <item> to learn this plan." — a route of its own, tested first so
    # the word "plan" in the sentence cannot pull it into Quests.
    if s.startswith("scrap ") or "to learn this plan" in s:
        return "Scrap to Learn"
    if "challenge" in s:
        return "Challenges"
    if "quest" in s:
        return "Quests"
    if s.startswith("found "):
        return "Containers"
    if s.startswith("redeemed"):
        return "Events & Activities"
    # The "Named in the game files as ..." hints. Where one names both a season
    # and a shop ("a Season 26 reward, sold as stamp-vendor stock") the SHOP
    # wins — that is where the player actually walks.
    if _RX_LED_GOLD.search(s):
        return "Gold Bullion"
    if _RX_LED_STAMP.search(s):
        return "Stamps"
    if "atom shop" in s:
        return "Atom Shop"
    if "season" in s or "scoreboard" in s:
        return "Scoreboard"
    if "bundle" in s:
        return "Limited Time Bundle"
    return "Events & Activities"


# ── tradeable, decided by the ROUTE ─────────────────────────────────────────
# Two routes settle tradeability on their own, whatever the BOOK keywords say.
# The keyword test in build_plan_obtain_json (NonPlayerTradable / NonDroppable
# -> False, else True) can only describe an ITEM, so it answers True by default
# for a plan that is not an item you can hold:
#
#   Scrap to Learn  — there is no plan book at all. Nothing exists to trade,
#                     sell or drop, so "Unknown" sends a reader hunting a vendor
#                     for a plan that cannot be listed.
#   Challenges      — the reward is bound to the character that earned it. 47
#                     challenge plans carried neither keyword and so published
#                     as "Tradeable", which is the wrong way round to be wrong:
#                     it tells someone to go buy one.
#
# Both are decided from the same bucketer the ledger uses, so a route that
# reclassifies moves this with it and there is no second list to keep in step.
# A row the game has ALREADY marked untradeable is untouched — this only ever
# moves a value to False, never back.
UNTRADEABLE_ROUTES = ("Scrap to Learn", "Challenges")


def unlock_buckets(item):
    """The ledger labels this row's non-loot unlock sentences fall into."""
    return {_ledger_unlock_bucket(u) for u in (item.get("obtain_unlocks") or [])}


def apply_tradeable_rules(items, stats=None):
    """Force `tradeable: False` on rows whose route can't be traded.

    Runs over EVERY row (book-backed and recipe-only) rather than at row
    construction, so it cannot be skipped by whichever builder made the row.
    """
    counts = {}
    for it in items:
        if it.get("tradeable") is False:
            continue
        hit = ""
        if it.get("scrap_learn"):
            hit = "Scrap to Learn"
        else:
            buckets = unlock_buckets(it)
            for label in UNTRADEABLE_ROUTES:
                if label in buckets:
                    hit = label
                    break
        if not hit:
            continue
        it["tradeable"] = False
        key = "untradeable_by_route:" + hit.lower().replace(" ", "_")
        counts[key] = counts.get(key, 0) + 1
    if stats is not None:
        for k, v in counts.items():
            stats[k] = stats.get(k, 0) + v
    return counts


def _dedupe_route_indexes(routes, idxs):
    """Drop repeat sources, best chance first, returning route indexes.

    A vendor reaches a plan as both its CONT (the chest) and its LVLI (the
    stock), so an undeduped row lists Minerva twice at the same rate.
    """
    seen, out = set(), []
    for i in idxs:
        r = routes[i]
        key = ((r.get("route") or "").strip().lower(), r.get("rate_display"))
        if key in seen:
            continue
        seen.add(key)
        out.append(i)
    out.sort(key=lambda i: (-(routes[i].get("rate") or 0.0),
                            (routes[i].get("route") or "")))
    return out


def obtain_ledger(item):
    """The fixed route table for one plan row. See the section note above.

    Returns one dict per route that APPLIES, in LEDGER_ROWS order:

        {"label":   "Gold Bullion",
         "routes":  [5, 2],      # indexes into item["obtain_routes"], in
                                 #   display order: deduped, best chance first
         "unlocks": [0],         # indexes into item["obtain_unlocks"]
         "drop":    "N/A"}       # drop-pill text; absent when the row has rates

    Routes are referenced BY INDEX, never copied. Copying them doubled
    plan_master.json — 2 MB of duplicated percentages on a file the browser
    downloads — to say something the row already said.

    The renderer draws the full LEDGER_ROWS list and prints N/A for every label
    missing here, so the reader still sees that each source was checked.

    The resolved percentages stay in this table: they are the whole reason a
    reader opens How to Obtain, and moving them to Technical would bury them.
    """
    routes  = item.get("obtain_routes")  or []
    unlocks = item.get("obtain_unlocks") or []

    by_route  = {label: [] for label in LEDGER_ROWS}
    by_unlock = {label: [] for label in LEDGER_ROWS}

    for i, r in enumerate(routes):
        by_route[_ledger_route_bucket(r)].append(i)
    seen_lines = set()
    for i, u in enumerate(unlocks):
        key = (u or "").strip().lower()
        if key in seen_lines:
            continue
        seen_lines.add(key)
        by_unlock[_ledger_unlock_bucket(u)].append(i)

    out = []
    for label in LEDGER_ROWS:
        ridx = _dedupe_route_indexes(routes, by_route[label])
        uidx = by_unlock[label]
        if not ridx and not uidx:
            continue                      # the renderer prints this one as N/A
        row = {"label": label}
        if ridx:
            row["routes"] = ridx
        if uidx:
            row["unlocks"] = uidx
        if not ridx:
            # Nothing random about this route, so there is no percentage to
            # print. Say so in the pill rather than leaving a gap, which reads
            # as "we could not work it out".
            row["drop"] = "N/A"
        out.append(row)
    return out


# ── Slasher-gated routes ─────────────────────────────────────────────────────
# Some drop lists are only rolled while the player wears the Slasher head
# (The Slasher seasonal event, EditorID SDOW_). The route inherits its holder's
# name — "Scorched Spooky" — which reads exactly like the ordinary Mischief
# Night Spooky Scorched drops, and readers do not open Drop Conditions.
# Duchess, 27 Sep 2026: every page names these "… (Slasher Seasonal)". Only the
# Slasher-gated routes; the normal Spooky Scorched routes are untouched.
#
# Duchess, 2 Oct 2026: the Spooky Scorched ones get their own name. In the game
# files LLD_Creature_Scorched_Spooky only rolls SDOW_LLD_SpookyScorched when
# LCP_SDOW_Slasher == 1 AND the scorched itself WornHasKeyword
# SDOW_ClothingTypeSlasherHead — i.e. it is wearing a Slasher mask. It is corpse
# loot: Spooky_TreatBag_Loot -> LL_Spooky_TreatBag_Loot never reaches these
# lists. "Scorched Spooky (Slasher Seasonal)" sent people farming treat bags
# and unmasked scorched, so the route is named after the enemy that drops it
# and the condition line says where it does NOT come from. The engine's own
# sentence ("Only while wearing SDOW Clothing Type Slasher Head") reads as if
# the PLAYER wears it — the subject of that condition is the scorched.
SLASHER_SUFFIX = " (Slasher Seasonal)"
SLASHER_MASKED_LABEL = "Slasher Masked Spooky Scorched"
SLASHER_MASKED_NOTE = ("Only from Spooky Scorched wearing a Slasher mask, while the "
                       "Slasher seasonal event is on — not from Spooky Treat Bags")
_RX_SLASHER_COND = re.compile(r"slasher", re.I)
_RX_SPOOKY_SCORCHED = re.compile(r"^\s*(?:scorched\s+spooky|spooky\s+scorched)\b", re.I)
_RX_MASK_COND = re.compile(r"slasher\s*head", re.I)


def _is_masked_spooky_scorched(label, conds):
    """A Spooky Scorched route gated on the Slasher mask (already-tagged or not)."""
    if label == SLASHER_MASKED_LABEL:
        return True
    base = label[:-len(SLASHER_SUFFIX)] if label.endswith(SLASHER_SUFFIX) else label
    if not _RX_SPOOKY_SCORCHED.match(base):
        return False
    return (label.endswith(SLASHER_SUFFIX)
            or any(_RX_MASK_COND.search(str(c)) for c in conds))


def tag_slasher_routes(items):
    """Name routes gated on the Slasher head. Idempotent. Returns a count."""
    n = 0
    for it in items:
        for r in it.get("obtain_routes") or []:
            label = r.get("route") or ""
            if not label:
                continue
            conds = list(r.get("conditions") or [])
            if _is_masked_spooky_scorched(label, conds):
                changed = False
                if label != SLASHER_MASKED_LABEL:
                    r["route"] = SLASHER_MASKED_LABEL
                    changed = True
                keep = [c for c in conds
                        if c != SLASHER_MASKED_NOTE and not _RX_MASK_COND.search(str(c))]
                new_conds = [SLASHER_MASKED_NOTE] + keep
                if new_conds != conds:
                    r["conditions"] = new_conds
                    changed = True
                n += changed
                continue
            if "slasher" in label.lower():
                continue
            if any(_RX_SLASHER_COND.search(str(c)) for c in conds):
                r["route"] = label + SLASHER_SUFFIX
                n += 1
    return n
