#!/usr/bin/env python3
r"""Back-compat shim — Nuka-Cola source resolution moved into spawns_engine/ and
the DRINK_ALCH seed map into spawns_configs/nuka_cola.py. Re-exported here so any
older reference (classify, get_sources, DRINK_ALCH, TSV) still resolves."""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
from spawns_engine.sources import (  # noqa: F401
    TSV, load_tables, PLACED_SIGS_DEFAULT as PLACED_SIGS,
    get_sources as _engine_get_sources,
)
from spawns_engine.classify import nuka_classify as classify  # noqa: F401
from spawns_configs.nuka_cola import DRINK_ALCH  # noqa: F401


def get_sources(flavour_slug, extra_seed_formids=None, tables=None):
    """Legacy signature preserved. Seeds from DRINK_ALCH[flavour] + extra formids,
    routes with the Nuka 12-case classifier."""
    t = tables or load_tables()
    item_records = [{"formid": f, "sig": "ALCH"} for f in DRINK_ALCH.get(flavour_slug, [])]
    return _engine_get_sources(item_records, t, classify,
                               extra_closure_seeds=extra_seed_formids,
                               placed_sigs=PLACED_SIGS)
