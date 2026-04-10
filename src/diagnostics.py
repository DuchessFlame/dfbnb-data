from __future__ import annotations

"""
diagnostics.py
==============
Shared diagnostics helper for dfbnb-data builders.

Every builder can import this module, call `report()` as problems are found,
and call `save()` at the end of its run. The helper maintains a single
`dist/diagnostics.json` file that is merged across builders — each builder
owns its own section and its section is fully replaced on every run (so old
entries disappear automatically as they are fixed).

Shape of dist/diagnostics.json:
{
  "generated": "<ISO-8601 UTC>",
  "sections": {
    "<source>": {
      "generated": "<ISO-8601 UTC>",
      "counts": {"error": N, "warning": N, "info": N},
      "entries": [
        {
          "severity": "error" | "warning" | "info",
          "code": "<short.machine.code>",
          "message": "<human readable>",
          "detail": "<optional longer string>",
          "context": { ... optional extra fields ... }
        },
        ...
      ]
    },
    ...
  }
}

Usage:
    from diagnostics import Diagnostics

    diag = Diagnostics(source="menu_items", outdir="dist")
    diag.error("menu.missing_price", "No price for item", detail="Berry Mentats")
    diag.warning("menu.orphan_override", "Override exists but item not in COBJ", context={"name": "Old Item"})
    diag.info("menu.build_ok", "312 menu items written")
    diag.save()
"""

import datetime as dt
import json
import os
import sys
from typing import Any, Dict, List, Optional

SEVERITIES = ("error", "warning", "info")


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


class Diagnostics:
    """Collects diagnostic entries for a single builder run and merges into
    dist/diagnostics.json on save().
    """

    def __init__(self, source: str, outdir: str = "dist", filename: str = "diagnostics.json") -> None:
        if not source:
            raise ValueError("Diagnostics source must be a non-empty string")
        self.source = source
        self.outdir = outdir
        self.path = os.path.join(outdir, filename)
        self.entries: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def report(
        self,
        severity: str,
        code: str,
        message: str,
        detail: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        sev = severity.lower().strip()
        if sev not in SEVERITIES:
            raise ValueError(f"Unknown severity {severity!r}; expected one of {SEVERITIES}")
        entry: Dict[str, Any] = {
            "severity": sev,
            "code": str(code),
            "message": str(message),
        }
        if detail is not None:
            entry["detail"] = str(detail)
        if context:
            entry["context"] = context
        self.entries.append(entry)

    def error(self, code: str, message: str, detail: Optional[str] = None, context: Optional[Dict[str, Any]] = None) -> None:
        self.report("error", code, message, detail, context)

    def warning(self, code: str, message: str, detail: Optional[str] = None, context: Optional[Dict[str, Any]] = None) -> None:
        self.report("warning", code, message, detail, context)

    def info(self, code: str, message: str, detail: Optional[str] = None, context: Optional[Dict[str, Any]] = None) -> None:
        self.report("info", code, message, detail, context)

    # ------------------------------------------------------------------
    # Counts / summaries
    # ------------------------------------------------------------------

    def counts(self) -> Dict[str, int]:
        out = {s: 0 for s in SEVERITIES}
        for e in self.entries:
            out[e["severity"]] = out.get(e["severity"], 0) + 1
        return out

    def has_errors(self) -> bool:
        return any(e["severity"] == "error" for e in self.entries)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load_existing(self) -> Dict[str, Any]:
        if not os.path.isfile(self.path):
            return {"generated": _now_iso(), "sections": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {"generated": _now_iso(), "sections": {}}
            if "sections" not in data or not isinstance(data["sections"], dict):
                data["sections"] = {}
            return data
        except Exception as exc:
            print(f"WARN: could not read existing {self.path}: {exc}", file=sys.stderr)
            return {"generated": _now_iso(), "sections": {}}

    def save(self) -> None:
        os.makedirs(self.outdir, exist_ok=True)
        data = self._load_existing()
        data["sections"][self.source] = {
            "generated": _now_iso(),
            "counts": self.counts(),
            "entries": self.entries,
        }
        data["generated"] = _now_iso()
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as exc:
            print(f"ERROR writing {self.path}: {exc}", file=sys.stderr)
            raise

        c = self.counts()
        print(
            f"[diagnostics:{self.source}] errors={c['error']} warnings={c['warning']} info={c['info']} -> {self.path}",
            file=sys.stderr,
        )


# ----------------------------------------------------------------------
# Convenience: clear a section without touching the rest of the file.
# Useful for ad-hoc cleanup scripts.
# ----------------------------------------------------------------------

def clear_section(source: str, outdir: str = "dist", filename: str = "diagnostics.json") -> None:
    path = os.path.join(outdir, filename)
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return
    if not isinstance(data, dict) or "sections" not in data:
        return
    if source in data["sections"]:
        del data["sections"][source]
        data["generated"] = _now_iso()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
