#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

JsonObj = Dict[str, Any]

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

def normalize_history(raw: Any) -> JsonObj:
    # Accept:
    # - {"entries":[...]}
    # - [...]
    # - {} (empty)
    if raw is None:
        return {"entries": []}
    if isinstance(raw, dict):
        entries = raw.get("entries")
        if isinstance(entries, list):
            return {"entries": entries}
        # If dict but no entries, treat as empty history
        return {"entries": []}
    if isinstance(raw, list):
        return {"entries": raw}
    return {"entries": []}

def normalize_latest(raw: Any) -> JsonObj:
    # Whatever the generator produced, we store it under "latest"
    # but we still enforce a dict container to keep history stable.
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    # If latest is weird (array/string), wrap it
    return {"value": raw}

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--latest", required=True, help="Path to the latest patchlog JSON produced this run")
    ap.add_argument("--history", required=True, help="Path to the append-only history JSON to write")
    ap.add_argument("--kind", required=True, help="Logical feed kind, eg: titles, challenges, guides, etc.")
    args = ap.parse_args()

    latest_path = args.latest
    hist_path = args.history
    kind = str(args.kind).strip()

    if not os.path.exists(latest_path):
        raise SystemExit(f"[append_patchlog_history] latest file missing: {latest_path}")

    latest_raw = read_json(latest_path)
    latest = normalize_latest(latest_raw)

    # Metadata from GitHub Actions env (safe if missing)
    run_id = os.environ.get("GITHUB_RUN_ID", "") or ""
    sha = os.environ.get("GITHUB_SHA", "") or ""
    actor = os.environ.get("GITHUB_ACTOR", "") or ""
    workflow = os.environ.get("GITHUB_WORKFLOW", "") or ""

    # Prefer generator timestamp if present, else now
    ts = ""
    if isinstance(latest, dict):
        ts = str(latest.get("generatedAt") or latest.get("generated_at") or "") or ""
    ts = ts if ts else utc_now_iso()

    # Load existing history (append-only, newest-first)
    if os.path.exists(hist_path):
        hist_raw = read_json(hist_path)
        hist = normalize_history(hist_raw)
    else:
        hist = {"entries": []}

    entry: JsonObj = {
        "ts": ts,
        "kind": kind,
        "runId": run_id,
        "sha": sha,
        "actor": actor,
        "workflow": workflow,
        "latest": latest
    }

    # Non-negotiable requirement: every run creates a new entry.
    # So we ALWAYS prepend.
    hist["entries"] = [entry] + (hist.get("entries") or [])

    write_json(hist_path, hist)

    print(f"[append_patchlog_history] wrote {hist_path} (entries={len(hist['entries'])})")

if __name__ == "__main__":
    main()
