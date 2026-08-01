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
}

# Substring version of the above — any interior whose display name CONTAINS the key resolves
# to the region. Handy for families of sub-areas (all the Whitespring entrances/golf club, etc.).
INTERIOR_REGION_CONTAINS = {
    "Whitespring": "Savage Divide",
    "Poseidon Energy Plant": "Forest",
}


class Geo:
    def __init__(self, db=MAPPALACHIA_DB):
        self.rings, self.markers = load_mappalachia()
        # marker label -> (x, y); plus a length-sorted label list for substring matching.
        self.marker_xy = {}
        for lbl, x, y in self.markers:
            self.marker_xy.setdefault(lbl, (x, y))
        self.labels_by_len = sorted((l for l in self.marker_xy), key=len, reverse=True)
        # spaceFormID -> (editorID, displayName)
        con = sqlite3.connect(db); cur = con.cursor()
        self.spaces = {sid: (eid or "", nm or "")
                       for sid, eid, nm in cur.execute(
                           "SELECT spaceFormID, spaceEditorID, spaceDisplayName FROM Space")}
        con.close()

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
        # 1) exact marker name (most reliable)
        if display in self.marker_xy:
            return self._region_of_marker_label(display), display
        # 2) expansion-space prefix rule (Storm -> Skyline Valley, Burn -> Burning Springs)
        for pref, region in SPACE_PREFIX_REGIONS.items():
            if eid.startswith(pref):
                return region, display
        # 3) longest marker label that is a substring of the interior name
        for lbl in self.labels_by_len:
            if lbl and lbl in display:
                return self._region_of_marker_label(lbl), display
        # 4) manual overrides — exact then substring
        if display in INTERIOR_REGION_OVERRIDES:
            return INTERIOR_REGION_OVERRIDES[display], display
        for key, region in INTERIOR_REGION_CONTAINS.items():
            if key in display:
                return region, display
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
            return region, marker, "exterior"

        if eid.startswith("XPDAC"):
            return "Atlantic City", name, "atlantic-city"
        if eid.startswith("XPDPitt"):
            return "The Pitt", name, "the-pitt"

        region, sub = self._resolve_interior(eid, name)
        return region, sub, ("interior" if region else "interior-unresolved")
