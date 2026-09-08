#!/usr/bin/env python3
"""
asset_paths.py
--------------
THE routing rule for DF/BNB reward art. One rule, one place.

Art that appears on more than one season is stored ONCE in a shared folder and
every page points at that copy. Only art unique to a season sits under
season-{N}:

    utilities (caps, atoms, lunchboxes ...)  -> /season_images/utility/
    C.A.M.P. titles                          -> /guide-images/titles/titles-camp/
    player titles                            -> /guide-images/titles/titles-player/
    player icons                             -> /guide-images/atom-shop/player-icons/
    emotes                                   -> /guide-images/atom-shop/emotes/
    everything unique to one season          -> /season_images/season-{N}/

Art shared by several rewards is stored once too. The S.C.O.R.E. Boost is the
worked example: 5%, 10% and the second 10% are one texture, so all three route
to utility/score_s24_account_scoreboost_1.avif on every season page.

This is the Python twin of dfbnbAssetUrl() in the four renderers
(df-bnb-titles.js, df-bnb-seasons.js, df-bnb-calculators.js,
df-bnb-upcoming-rewards.js). The two MUST agree. If you change the rule, change
it in all five and re-run the parity check at the bottom of this file:

    python src/asset_paths.py --check

WHY IT EXISTS
The rule was previously spelled out in ~8 generators and 3 renderers, each
carrying a slightly different half of it. When the shared folders were moved
under /guide-images/, the folders moved and the scripts did not, and every
title image on the site 404'd - 149 of 162 on the Player Titles checklist -
while the scoreboard pages quietly kept working because their renderer
happened to rewrite paths a different way.

Routing is decided by the FILENAME, never by the root the caller happens to
hold or by the page doing the asking, so the same file resolves to the same
place from every generator and every page.
"""

from __future__ import annotations

import re

UPLOADS = "/wp-content/uploads/"

SHARED = {
    "utility":       UPLOADS + "season_images/utility/",
    "titles_camp":   UPLOADS + "guide-images/titles/titles-camp/",
    "titles_player": UPLOADS + "guide-images/titles/titles-player/",
    "player_icons":  UPLOADS + "guide-images/atom-shop/player-icons/",
    "emotes":        UPLOADS + "guide-images/atom-shop/emotes/",
}

SEASON_ROOT = UPLOADS + "season_images/season-{n}/"

_WEBP_RE = re.compile(r"\.webp$", re.IGNORECASE)
# Emote art is the ONE asset class that stays .webp. The files are animated
# (19 frames for "Watching You"), shared with the Atom Shop emotes page, and
# keyed by display name rather than editor ID - so they are matched on their
# FOLDER, not their filename, and skip the .webp -> .avif rewrite. Flattening
# one to .avif would drop every frame but the first.
_EMOTE_DIR_RE = re.compile(r"/atom-shop/emotes/", re.IGNORECASE)
# Bethesda zero-pads the season in some texture names (SCORE_S04_) and not in
# others (SCORE_S4_). Every upload is unpadded, so normalise once, here.
_PAD_RE = re.compile(r"^(score_s)0+(\d)", re.IGNORECASE)
_SEASON_RE = re.compile(r"^score_s0*(\d+)_", re.IGNORECASE)
# Every S.C.O.R.E. Boost tier - 5%, 10%, 10% again - is the SAME in-game
# texture (textures/atx/storefront/utility/score_account_scoreboost.dds). It was
# exported under three names, only _1 was ever uploaded, and the data files
# scattered the rest across three folders, so the 10% rows 404'd on almost every
# season. One texture, one file: collapse the tier and force the shared folder.
_BOOST_RE = re.compile(r"account_scoreboost", re.IGNORECASE)
_BOOST_TIER_RE = re.compile(r"(_account_scoreboost)_\d+", re.IGNORECASE)
BOOST_FILE = "score_s24_account_scoreboost_1.avif"


def asset_url(url: str) -> str:
    """Route any reward-art URL to where the file actually lives.

    Idempotent: routing an already-routed URL returns it unchanged.
    """
    u = (url or "").strip()
    if not u:
        return ""

    # Emotes first: they must keep .webp, so this runs before the rewrite.
    if _EMOTE_DIR_RE.search(u):
        return SHARED["emotes"] + u.rsplit("/", 1)[-1]

    u = _WEBP_RE.sub(".avif", u)

    file = _PAD_RE.sub(r"\1\2", u.rsplit("/", 1)[-1])
    name = file.lower()

    # One texture for all three boost tiers - see _BOOST_RE above. Checked
    # before the folder rules because the older data files put the same art
    # under season_images/, season-24/ and utility/.
    if _BOOST_RE.search(name):
        return SHARED["utility"] + _BOOST_TIER_RE.sub(r"\1_1", file)

    if "/utility/" in u:
        return SHARED["utility"] + file
    if "camptitles" in name:
        return SHARED["titles_camp"] + file
    if "playertitles" in name:
        return SHARED["titles_player"] + file
    if "playericon" in name:
        return SHARED["player_icons"] + file

    # Unique season art. The season comes from the FILENAME, never from the
    # season being generated - a reused texture keeps the season it was
    # uploaded under, which is how S4's board shows an S3 floor tile.
    m = _SEASON_RE.match(name)
    if m:
        return SEASON_ROOT.format(n=m.group(1)) + file

    # Anything else (atom-shop request items, bundle art) is already absolute
    # and belongs where it is.
    return u


def season_url(filename: str, season_num: int) -> str:
    """Explicit per-season path, for art with no season in its filename."""
    return SEASON_ROOT.format(n=int(season_num)) + filename


# --------------------------------------------------------------------------
# Emotes: the one class routing alone cannot reach.
# --------------------------------------------------------------------------
#
# asset_url() can route an emote URL it is HANDED, because it matches on the
# folder. It cannot BUILD one, because emote files are stored under the display
# name and that name is not derivable from the editor ID:
# SCORE_S12_ENTM_Emotes_OnceAgain is "One More Time". So a generator that only
# has an entitlement has nothing to route, and the .dds-name guess 404s on
# every emote a season has ever given out.
#
# dist/emotes.json closes it: `rent` is the entitlement, `name` is the display
# name. Both season builders resolve through here so they cannot drift.

# The uploads drop trailing "!" - "Absolutely!" is stored as Absolutely.webp.
_EMOTE_PUNCT_RE = re.compile(r"[!?]+$")


def emote_url(display_name: str) -> str:
    """The shared URL for one emote, from its DISPLAY NAME."""
    name = _EMOTE_PUNCT_RE.sub("", (display_name or "").strip()).strip()
    if not name:
        return ""
    return SHARED["emotes"] + name + ".webp"


def load_emote_images(repo_root=None) -> dict:
    """Entitlement (lowercased) -> shared emote artwork URL.

    The PTS copy is read as a fallback so an unreleased season's emote resolves
    before it ships; the live file is read first and wins once it does.
    """
    import json
    from pathlib import Path

    root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[1]
    out: dict = {}
    for rel in ("emotes.json", "pts/emotes.json"):
        path = root / "dist" / rel
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for e in (data.get("emotes") or []) + (data.get("petEmotes") or []):
            ent = (e.get("rent") or "").strip().lower()
            url = emote_url(e.get("name") or "")
            if ent and url:
                out.setdefault(ent, url)
    return out


def fill_emote_images(items, key="edid", repo_root=None) -> int:
    """Fill in emote artwork on any item that has none. Returns how many.

    Gap-fill only: a curated imageUrl always wins, because the curated row
    records what was actually uploaded.
    """
    art = load_emote_images(repo_root)
    if not art:
        return 0
    n = 0
    for item in items:
        if (item.get("imageUrl") or "").strip():
            continue
        url = art.get((item.get(key) or "").strip().lower())
        if url:
            item["imageUrl"] = url
            n += 1
    return n


# --------------------------------------------------------------------------
# Parity check against the JavaScript twin.
# --------------------------------------------------------------------------

CASES = [
    ("/wp-content/uploads/season_images/utility/score_currency_atoms.webp",
     "/wp-content/uploads/season_images/utility/score_currency_atoms.avif"),
    ("/wp-content/uploads/storefront/titles-camp/score_s26_camptitles_prefix_eerie.avif",
     "/wp-content/uploads/guide-images/titles/titles-camp/score_s26_camptitles_prefix_eerie.avif"),
    ("/wp-content/uploads/season_images/season-26/score_s26_camptitles_prefix_eerie.webp",
     "/wp-content/uploads/guide-images/titles/titles-camp/score_s26_camptitles_prefix_eerie.avif"),
    ("/wp-content/uploads/storefront/atx_playertitles_prefix_gleaming.webp",
     "/wp-content/uploads/guide-images/titles/titles-player/atx_playertitles_prefix_gleaming.avif"),
    ("/wp-content/uploads/season_images/season-26/score_s26_playericon_battamer.webp",
     "/wp-content/uploads/guide-images/atom-shop/player-icons/score_s26_playericon_battamer.avif"),
    ("/wp-content/uploads/guide-images/atom-shop/player-icons/atx_playericon_score_22.avif",
     "/wp-content/uploads/guide-images/atom-shop/player-icons/atx_playericon_score_22.avif"),
    ("/wp-content/uploads/season_images/score_s4_camp_floor_coldsteel.webp",
     "/wp-content/uploads/season_images/season-4/score_s4_camp_floor_coldsteel.avif"),
    ("/wp-content/uploads/season_images/score_s3_camp_floor_shelters_vaulttile_checkered.webp",
     "/wp-content/uploads/season_images/season-3/score_s3_camp_floor_shelters_vaulttile_checkered.avif"),
    ("/wp-content/uploads/season_images/score_s04_camp_walldecor_endofseasonart.avif",
     "/wp-content/uploads/season_images/season-4/score_s4_camp_walldecor_endofseasonart.avif"),
    ("/wp-content/uploads/guide-images/atom-shop/request-item-images/ATX_Camp_Display_Mannequin_Male_Clean.avif",
     "/wp-content/uploads/guide-images/atom-shop/request-item-images/ATX_Camp_Display_Mannequin_Male_Clean.avif"),
    ("/wp-content/uploads/storefront/titles-player/ATX_PlayerTitles_Prefix_Contessa.avif",
     "/wp-content/uploads/guide-images/titles/titles-player/ATX_PlayerTitles_Prefix_Contessa.avif"),
    # All three S.C.O.R.E. Boost tiers collapse to the one uploaded file,
    # whichever folder or tier the data row happens to carry.
    ("/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_1.webp",
     "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_1.avif"),
    ("/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_2.avif",
     "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_1.avif"),
    ("/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_3.avif",
     "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_1.avif"),
    ("/wp-content/uploads/season_images/season-24/score_s24_account_scoreboost_3.avif",
     "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_1.avif"),
    ("/wp-content/uploads/season_images/score_s24_account_scoreboost_1.webp",
     "/wp-content/uploads/season_images/utility/score_s24_account_scoreboost_1.avif"),
    # Emotes keep .webp and their display-name filename, and route on folder.
    ("/wp-content/uploads/guide-images/atom-shop/emotes/Watching You.webp",
     "/wp-content/uploads/guide-images/atom-shop/emotes/Watching You.webp"),
    ("/wp-content/uploads/guide-images/atom-shop/emotes/The Hills Are Alive.webp",
     "/wp-content/uploads/guide-images/atom-shop/emotes/The Hills Are Alive.webp"),
    ("", ""),
]


def _self_check() -> int:
    failed = 0
    for src, want in CASES:
        got = asset_url(src)
        if got != want:
            failed += 1
            print(f"FAIL  in   {src}\n      want {want}\n      got  {got}")
        # idempotence: routing twice must not move it again
        if asset_url(got) != got:
            failed += 1
            print(f"NOT IDEMPOTENT  {got} -> {asset_url(got)}")
    print(f"{len(CASES) - failed} of {len(CASES)} cases pass")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(_self_check())
