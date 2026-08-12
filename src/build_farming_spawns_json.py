#!/usr/bin/env python3
r"""Back-compat entry point — delegates to the shared engine's farming driver.

The real logic now lives in spawns_configs/farming.py on spawns_engine/. This
shim preserves the original CLI and the `from build_farming_spawns_json import main`
import used by build_cream_spawns_json.py / build_deathclaw_egg_spawns_json.py:
    python src/build_farming_spawns_json.py --item cream
    python src/build_farming_spawns_json.py --all --pts
"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
from spawns_configs.farming import main   # re-exported for the thin wrapper scripts

if __name__ == "__main__":
    main()
