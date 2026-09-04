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
    everything unique to one season          -> /season_images/season-{N}/

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
}

SEASON_ROOT = UPLOADS + "season_images/season-{n}/"

_WEBP_RE = re.compile(r"\.webp$", re.IGNORECASE)
# Bethesda zero-pads the season in some texture names (SCORE_S04_) and not in
# others (SCORE_S4_). Every upload is unpadded, so normalise once, here.
_PAD_RE = re.compile(r"^(score_s)0+(\d)", re.IGNORECASE)
_SEASON_RE = re.compile(r"^score_s0*(\d+)_", re.IGNORECASE)


def asset_url(url: str) -> str:
    """Route any reward-art URL to where the file actually lives.

    Idempotent: routing an already-routed URL returns it unchanged.
    """
    u = (url or "").strip()
    if not u:
        return ""
    u = _WEBP_RE.sub(".avif", u)

    file = _PAD_RE.sub(r"\1\2", u.rsplit("/", 1)[-1])
    name = file.lower()

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
