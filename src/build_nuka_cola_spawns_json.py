#!/usr/bin/env python3
r"""Back-compat entry point — delegates to the shared engine's Nuka-Cola driver.

The real logic now lives in spawns_configs/nuka_cola.py on spawns_engine/. This
shim preserves the original CLI:
    python src/build_nuka_cola_spawns_json.py            # all variants
    python src/build_nuka_cola_spawns_json.py wild dark  # only these slugs
"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
from spawns_configs.nuka_cola import run

if __name__ == "__main__":
    run(sys.argv)
