"""Temporary shim for running build scripts on Python 3.10 (sandbox).
Import this before running any build script that uses datetime.UTC (3.11+).
NOT needed in GitHub Actions (which uses 3.11).
"""
import datetime as dt
if not hasattr(dt, "UTC"):
    dt.UTC = dt.timezone.utc
