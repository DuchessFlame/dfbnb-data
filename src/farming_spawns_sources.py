#!/usr/bin/env python3
r"""Back-compat adapter — farming source resolution moved into spawns_engine/.
Preserves the surface used by build_vendors_json.py and build_farming_used_for.py:
    load_tables(tsv_root=None)
    get_sources(items_cfg, tables, extra_world_bases=None)   # farming classifier + FLOR sig
    classify(sig, edid, via_edid)
"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
from spawns_engine.sources import (  # noqa: F401
    TSV, load_tables, PLACED_SIGS_FLORA as PLACED_SIGS,
    get_sources as _engine_get_sources,
)
from spawns_engine.classify import farming_classify as classify  # noqa: F401


def get_sources(items_cfg, tables, extra_world_bases=None):
    return _engine_get_sources(items_cfg, tables, classify,
                               extra_world_bases=extra_world_bases,
                               placed_sigs=PLACED_SIGS)
