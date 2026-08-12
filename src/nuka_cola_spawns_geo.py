#!/usr/bin/env python3
r"""Back-compat shim — the Geo class moved into spawns_engine/geo.py (one source of
truth). Kept so existing importers (build_vendors_json.py, etc.) keep working."""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
from spawns_engine.geo import (  # noqa: F401
    Geo, MAPPALACHIA_DB, APPALACHIA_SPACE,
    SPACE_PREFIX_REGIONS, INTERIOR_REGION_OVERRIDES, INTERIOR_REGION_CONTAINS,
)
