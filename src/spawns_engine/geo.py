#!/usr/bin/env python3
r"""
nuka_cola_spawns_geo.py — resolve a placed spawn (space + x/y) to (region, sub-location).

Kept separate from the build script on purpose: this is the fiddly geometry, and it's
the file you'll tweak when a placement lands in the wrong region. The build script just
calls resolve().

Resolution rules, in order:
  1. Appalachia exterior worldspace (2480661) -> region via polygon + nearest map marker
     (reuses the exact functions the collectables pipeline uses: crossref_mappalachia_markers).
  2. Atlantic City instanced spaces (spaceEditorID starts 'XPDAC')  -> region "Atlantic City",
     sub-location = the space's display name (e.g. "The Boardwalk", "The Neapolitan Casino").
  3. The Pitt instanced spaces      (spaceEditorID starts 'XPDPitt') -> region "The Pitt",
     sub-location = the space's display name (e.g. "The Foundry", "The Pitt - Trench").
  4. Any other interior (Appalachia buildings/vaults) -> sub-location = the interior's display
     name; region resolved by matching that name to an exterior map marker (exact, then the
     longest marker label contained in the name), then INTERIOR_REGION_OVERRIDES, else "" so
     it surfaces for a hand glance instead of guessing.

Interior coords in the DB are cell-local, so we resolve interiors by NAME, not coordinates.
"""

import os, sqlite3
from crossref_mappalachia_markers import (
    load_mappalachia, region_for_xy, nearest_marker, MARKER_REGION_OVERRIDES,
    load_lctn_regions, lctn_region_for, geo_snapshot_spaces,
)

MAPPALACHIA_DB = os.environ.get("MAPPALACHIA_DB", r"D:\Mappalachia\data\mappalachia.db")
APPALACHIA_SPACE = 2480661

# Expansion interiors share a spaceEditorID prefix and all sit in one region, so a prefix
# rule resolves them in bulk (their interior names don't match exterior markers).
#   Storm* = the Skyline Valley (Storm) update interiors (Vault 63, Dark Hollow Manor, …)
#   Burn*  = the Burning Springs update interiors (Highway Town, Rust Kingdom, Chop Shop, …)
SPACE_PREFIX_REGIONS = {
    "Storm": "Skyline Valley",
    "Burn": "Burning Springs",
}

# Interiors whose display name doesn't match any exterior marker and aren't covered by a
# prefix rule. Add rows as they surface in a build's "unresolved" list. Keep it short.
INTERIOR_REGION_OVERRIDES = {
    "Eta Psi House": "Forest",
    "The Burrows": "The Mire",
    # --- Added Aug 2026 from the Nuka Cola build's unresolved list ---
    # High confidence:
    "Enclave Research Facility": "Savage Divide",   # Whitespring Enclave bunker
    "Black Bear Lodge": "Toxic Valley",
    "Hornwright Industrial HQ": "Forest",            # Charleston
    "Valley Galleria": "Cranberry Bog",              # Watoga shopping plaza
    # --- Added Aug 2026 from the vendor build's unresolved list (DB-verified) ---
    "The Rusty Pick": "Ash Heap",                    # marker exists but sits in a polygon gap
    "Duncan & Duncan Robotics": "Cranberry Bog",     # Watoga (all Watoga markers resolve here)
    "Gold Vault Operations": "Savage Divide",        # Regs — inside Vault 79 (marker = Savage Divide)
    # --- Added Aug 2026: name-variant interiors the LCTN map mis-tags or misses.
    #     These EXACT overrides beat the LCTN map (checked first) — see _resolve_interior. ---
    "Garrahan Mining HQ": "Ash Heap",                # LCTN tags LocRegionMountain (Savage Divide) — WRONG; map zone is Ash Heap (Mount Blair)
    "Thunder Mountain Power": "The Mire",            # LCTN "Thunder Mountain Power Plant"
    "AMS Headquarters": "Cranberry Bog",             # LCTN "AMS Corporate Headquarters"
    "AMS Headquarters Basement Lab": "Cranberry Bog",
    "RobCo Auto-Cache #001": "Forest",               # LCTN "RobCo Experimental Auto-Supply Cache #0001"
    "Shadowbreeze Apartments": "Forest",             # Morgantown apartments
    # --- Ghoul Within / Atlantic City update locations too new for the LCTN/geo export ---
    "Radiant Hills": "Savage Divide",                # Ghoul Within update — north Savage Divide (user-confirmed; fallout.wiki)
    "Hillside Cavern": "Savage Divide",              # Ghoul Within update — Savage Divide (fallout.wiki/wiki/Hillside_Cavern)
    "Little Rob's Hideout": "Forest",                # door is at The Crosshair (LocForestTheCrosshairLocation) — Atlantic City update
    "Cavern": "The Mire",                            # sole interior named "Cavern" = Sam Blackwell's Deathclaw Cave (LocSwampSamBlackwellBunker)
    # Best-guess — PLEASE VERIFY (a wrong region is a one-line fix here):
    "Belching Betty": "Savage Divide",               # mine, Mount Blair area
    "FEV Production Facility": "Savage Divide",       # West Tek
    "Missile Silo Alpha": "Savage Divide",
    "Missile Silo Bravo": "Savage Divide",
    "Missile Silo Charlie": "Cranberry Bog",
    "Point Repose": "The Mire",
    "Spruce Knob Boat Rental": "Savage Divide",
    "Van Lowe Taxidermy": "Forest",
    "Vault-Tec Ag Research Center": "Savage Divide",
    # Still UNRESOLVED on purpose — need your call (some may be instanced player
    # Shelters with no real region, which is correct to leave off the page):
    #   Blue Ridge Office · Orwell Bomb Shelter · Radiant Hills · Trailer Interior
}

# Substring version of the above — any interior whose display name CONTAINS the key resolves
# to the region. Handy for families of sub-areas (all the Whitespring entrances/golf club, etc.).
# Checked longest-key-first so a more specific key wins.
INTERIOR_REGION_CONTAINS = {
    "Whitespring": "Savage Divide",
    "Poseidon Energy Plant": "Forest",
    "Watoga": "Cranberry Bog",
}


class Geo:
    def __init__(self, db=MAPPALACHIA_DB):
        self.rings, self.markers = load_mappalachia()
        # marker label -> (x, y); plus a length-sorted label list for substring matching.
        self.marker_xy = {}
        for lbl, x, y in self.markers:
            self.marker_xy.setdefault(lbl, (x, y))
        self.labels_by_len = sorted((l for l in self.marker_xy), key=len, reverse=True)
        # spaceFormID -> (editorID, displayName). The last thing in this class that
        # needed the 480 MB DB — 204 rows. It now comes from the committed geo
        # snapshot when the DB is absent, so the spawn generators can run in CI.
        if os.path.isfile(db):
            con = sqlite3.connect(db); cur = con.cursor()
            self.spaces = {sid: (eid or "", nm or "")
                           for sid, eid, nm in cur.execute(
                               "SELECT spaceFormID, spaceEditorID, spaceDisplayName FROM Space")}
            con.close()
        else:
            self.spaces = geo_snapshot_spaces()
            if not self.spaces:
                print("[geo] no Mappalachia DB and no 'spaces' in the geo snapshot — "
                      "worldspace names will be blank. Run once locally with the DB "
                      "to refresh data/mappalachia_geo.json.")
        # LCTN-derived region maps (POI/interior name + editorID core -> region),
        # the authoritative coordinate-free region tag from the game's Location
        # records. Consulted before the hand-maintained INTERIOR_REGION_OVERRIDES.
        self.lctn_by_name, self.lctn_by_core = load_lctn_regions()

    def _region_of_marker_label(self, label):
        xy = self.marker_xy.get(label)
        if not xy:
            return ""
        r = region_for_xy(self.rings, xy[0], xy[1])
        if not r and label in MARKER_REGION_OVERRIDES:
            r = MARKER_REGION_OVERRIDES[label]
        return r

    def _resolve_interior(self, eid, display):
        if not display:
            return "", ""
        # 1) exact marker name (most reliable) — only accept if it yields a real region
        if display in self.marker_xy:
            r = self._region_of_marker_label(display)
            if r:
                return r, display
        # 2) expansion-space prefix rule (Storm -> Skyline Valley, Burn -> Burning Springs)
        for pref, region in SPACE_PREFIX_REGIONS.items():
            if eid.startswith(pref):
                return region, display
        # 3) EXACT manual overrides — deliberate human corrections, authoritative:
        #    they beat the LCTN map (which mis-tags a few, e.g. Garrahan Mining HQ).
        if display in INTERIOR_REGION_OVERRIDES:
            return INTERIOR_REGION_OVERRIDES[display], display
        # 4) LCTN location-keyword map (game data) — by display name, then editorID
        #    core. Resolves most interiors AND polygon-gap landmarks with no hand
        #    maintenance.
        r = lctn_region_for(self.lctn_by_name, self.lctn_by_core, display, eid)
        if r:
            return r, display
        # 5) substring override table (families of sub-areas).
        for key in sorted(INTERIOR_REGION_CONTAINS, key=len, reverse=True):
            if key in display:
                return INTERIOR_REGION_CONTAINS[key], display
        # 4) longest marker label that is a substring of the interior name (fuzzy fallback) —
        #    only accept a non-empty region, else keep looking / fall through to unresolved.
        for lbl in self.labels_by_len:
            if lbl and lbl in display:
                r = self._region_of_marker_label(lbl)
                if r:
                    return r, display
        return "", display  # unresolved region, keep the name as the sub-location

    def resolve(self, space_formid, x, y):
        """Return (region, sub_location, resolved_by)."""
        eid, name = self.spaces.get(space_formid, ("", ""))

        if space_formid == APPALACHIA_SPACE:
            region = region_for_xy(self.rings, x, y)
            marker, mx, my = nearest_marker(self.markers, x, y)
            if not region and mx is not None:
                region = region_for_xy(self.rings, mx, my)
            if not region and marker in MARKER_REGION_OVERRIDES:
                region = MARKER_REGION_OVERRIDES[marker]
            # LCTN map by nearest-marker name (fixes polygon-gap outdoor landmarks
            # like Graninger Farm / Black Bear Lodge without coordinates).
            if not region and marker:
                region = lctn_region_for(self.lctn_by_name, self.lctn_by_core, marker, "")
            # Final coordinate fallback: assign the CLOSEST region polygon (the point
            # sits in a road/water/border gap outside every polygon).
            if not region:
                region = region_for_xy(self.rings, x, y, nearest=True)
            return region, marker, "exterior"

        if eid.startswith("XPDAC"):
            return "Atlantic City", name, "atlantic-city"
        if eid.startswith("XPDPitt"):
            return "The Pitt", name, "the-pitt"

        region, sub = self._resolve_interior(eid, name)
        return region, sub, ("interior" if region else "interior-unresolved")
