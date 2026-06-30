#!/usr/bin/env python3
"""Deprecated shim. The canonical builder is build_fish_journal_json.py.

This file only exists because the editing session that created it could not
delete it afterwards (cloud-sync permission). It simply runs the canonical
script, so `python build_fish_journal_run.py` == `python build_fish_journal_json.py`.
Safe to delete.
"""
import os, runpy

runpy.run_path(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "build_fish_journal_json.py"),
    run_name="__main__",
)
