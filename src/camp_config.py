"""Per-page config for the CAMP item builders.

The hand-maintained override tables — image maps, exclusions, name/how/ENTM
overrides — live in ``data/camp/<page>.json`` rather than inside the build
scripts. They gain an entry every season while the surrounding logic does not,
so keeping them out of the Python is what stops the builders growing without
limit. Edit the JSON; the build script reads it.
"""

from pathlib import Path
import json

_DIR = Path(__file__).resolve().parent.parent / "data" / "camp"


def load(page, refs=None):
    """Return the config dict for one page.

    A string value of the form ``"@NAME"`` is replaced with ``refs["NAME"]``, so
    a table can point at a constant the build script owns (``"@ATX_HOW"``)
    instead of duplicating its text and drifting from it.
    """
    data = json.loads((_DIR / f"{page}.json").read_text(encoding="utf-8"))
    return _resolve(data, refs or {})


def _resolve(node, refs):
    if isinstance(node, str) and node.startswith("@"):
        name = node[1:]
        if name not in refs:
            raise KeyError(f"config reference @{name} has no value supplied")
        return refs[name]
    if isinstance(node, dict):
        return {k: _resolve(v, refs) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve(v, refs) for v in node]
    return node
