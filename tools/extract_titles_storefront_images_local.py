# tools/extract_titles_storefront_images_local.py
# Deterministic local extractor for Fallout 76 title storefront images.
#
# Reads:   dist/titles_images_manifest.json
# Finds:   extracted .dds files inside your extracted textures folder
# Converts: DDS -> PNG (texconv) -> WEBP lossless (cwebp)
# Outputs: export/storefront/<entitlement_edid_lower>.webp
#
# Notes:
# - No BA2 parsing. No BSArch. No archive extraction.
# - Skips existing WEBP outputs.
# - Continues on errors and prints a summary.

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass
class Counts:
    created: int = 0
    skipped: int = 0
    failed: int = 0


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def ensure_exe(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    if path.is_dir():
        raise IsADirectoryError(f"{label} is a directory, expected an .exe file: {path}")


def normalize_manifest_dds_path(dds_path: str) -> str:
    """
    Manifest paths are typically like:
      textures/atx/storefront/player/playertitles/foo.dds
    Your extracted texture root is commonly already the "textures" directory.
    So we strip a leading "textures/" or "textures\\" to avoid double textures/textures.
    """
    p = dds_path.strip().lstrip("/\\")
    p = p.replace("\\", "/")
    if p.lower().startswith("textures/"):
        p = p[len("textures/") :]
    return p.replace("/", os.sep)


def build_dds_full_path(extracted_textures_root: Path, manifest_dds_path: str) -> Path:
    rel = normalize_manifest_dds_path(manifest_dds_path)
    return extracted_textures_root / rel


def safe_run(cmd: List[str]) -> Tuple[int, str, str]:
    """
    Returns (returncode, stdout, stderr). Never raises on failure.
    """
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=False,
        )
        return proc.returncode, proc.stdout or "", proc.stderr or ""
    except Exception as ex:
        return 999, "", f"Exception running command: {ex}"


def texconv_dds_to_png(
    texconv_exe: Path, dds_file: Path, png_out_dir: Path
) -> Tuple[bool, Optional[Path], str]:
    """
    Uses texconv to convert DDS -> PNG into png_out_dir.
    Returns (ok, png_path, message).
    """
    png_out_dir.mkdir(parents=True, exist_ok=True)

    # texconv output filename is based on input basename unless flags change it.
    # We'll look for <stem>.png after running.
    expected_png = png_out_dir / (dds_file.stem + ".png")

    cmd = [
        str(texconv_exe),
        "-ft",
        "png",
        "-y",                 # overwrite if exists
        "-o",
        str(png_out_dir),
        str(dds_file),
    ]

    code, out, err = safe_run(cmd)
    if code != 0:
        msg = f"texconv failed (code {code}) for {dds_file}\n{err.strip()}\n{out.strip()}".strip()
        return False, None, msg

    if expected_png.exists():
        return True, expected_png, "ok"

    # Sometimes texconv can output with slightly different naming (rare). As a fallback, search newest PNG.
    pngs = sorted(png_out_dir.glob(dds_file.stem + "*.png"), key=lambda p: p.stat().st_mtime, reverse=True)
    if pngs:
        return True, pngs[0], "ok (fallback match)"

    return False, None, f"texconv reported success but no PNG found for {dds_file}"


def cwebp_png_to_webp_lossless(
    cwebp_exe: Path, png_file: Path, webp_out_file: Path
) -> Tuple[bool, str]:
    """
    Uses cwebp to convert PNG -> WEBP (lossless).
    Returns (ok, message).
    """
    webp_out_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(cwebp_exe),
        "-lossless",
        "-quiet",
        str(png_file),
        "-o",
        str(webp_out_file),
    ]

    code, out, err = safe_run(cmd)
    if code != 0:
        msg = f"cwebp failed (code {code}) for {png_file}\n{err.strip()}\n{out.strip()}".strip()
        return False, msg

    if not webp_out_file.exists():
        return False, f"cwebp reported success but WEBP not found: {webp_out_file}"

    return True, "ok"


def iter_tasks(manifest: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list):
        return []
    for t in tasks:
        if isinstance(t, dict):
            yield t


def coerce_str_list(value: Any) -> List[str]:
    if isinstance(value, list):
        out: List[str] = []
        for v in value:
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
        return out
    return []


def choose_first_existing_dds(
    extracted_textures_root: Path, dds_paths: List[str]
) -> Optional[Path]:
    """
    Deterministic: try dds_paths in order; pick the first that exists on disk.
    """
    for dds_rel in dds_paths:
        candidate = build_dds_full_path(extracted_textures_root, dds_rel)
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def process_manifest(
    manifest_path: Path,
    extracted_textures_root: Path,
    texconv_exe: Path,
    cwebp_exe: Path,
    out_dir: Path,
    tmp_png_dir: Path,
    keep_png: bool,
) -> Counts:
    counts = Counts()

    manifest = load_json(manifest_path)

    for task in iter_tasks(manifest):
        entitlement_edids = coerce_str_list(task.get("entitlementEdids"))
        dds_paths = coerce_str_list(task.get("ddsPaths"))

        if not entitlement_edids:
            continue
        if not dds_paths:
            # No DDS paths means every entitlement in this task cannot be built.
            for edid in entitlement_edids:
                eprint(f"[FAIL] {edid}: no ddsPaths in task")
                counts.failed += 1
            continue

        # Determine which DDS to use for this task (first existing in order).
        # This keeps naming deterministic and avoids producing multiple files per entitlement.
        dds_file = choose_first_existing_dds(extracted_textures_root, dds_paths)

        for edid in entitlement_edids:
            edid_lower = edid.strip().lower()
            if not edid_lower:
                continue

            out_file = out_dir / f"{edid_lower}.webp"

            if out_file.exists():
                counts.skipped += 1
                continue

            if dds_file is None:
                eprint(f"[FAIL] {edid}: no matching DDS found on disk (checked {len(dds_paths)} paths)")
                counts.failed += 1
                continue

            ok_png, png_path, msg_png = texconv_dds_to_png(texconv_exe, dds_file, tmp_png_dir)
            if not ok_png or png_path is None:
                eprint(f"[FAIL] {edid}: {msg_png}")
                counts.failed += 1
                continue

            ok_webp, msg_webp = cwebp_png_to_webp_lossless(cwebp_exe, png_path, out_file)
            if not ok_webp:
                eprint(f"[FAIL] {edid}: {msg_webp}")
                counts.failed += 1
                continue

            counts.created += 1

            if not keep_png:
                try:
                    png_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                except Exception:
                    pass

    return counts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extract FO76 title storefront textures based on titles_images_manifest.json and convert to WEBP."
    )

    p.add_argument(
        "--manifest",
        type=str,
        default=str(Path("dist") / "titles_images_manifest.json"),
        help="Path to dist/titles_images_manifest.json",
    )
    p.add_argument(
        "--textures-root",
        type=str,
        default=r"C:\Users\allma\OneDrive\Guides and Stuff\Json Files for Website\1 site-data\textures\textures",
        help="Root folder that contains extracted DDS files (your extracted textures folder).",
    )
    p.add_argument(
        "--texconv",
        type=str,
        default=r"E:\FO76_Tools\texconv.exe",
        help="Path to texconv.exe",
    )
    p.add_argument(
        "--cwebp",
        type=str,
        default=r"E:\FO76_Tools\cwebp.exe",
        help="Path to cwebp.exe",
    )
    p.add_argument(
        "--out",
        type=str,
        default=str(Path("export") / "storefront"),
        help="Output folder for WEBP files (export/storefront).",
    )
    p.add_argument(
        "--tmp-png",
        type=str,
        default=str(Path("export") / "_tmp_png"),
        help="Temporary PNG folder.",
    )
    p.add_argument(
        "--keep-png",
        action="store_true",
        help="Keep intermediate PNGs (default deletes them after successful WEBP creation).",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()

    manifest_path = Path(args.manifest).resolve()
    extracted_textures_root = Path(args.textures_root).resolve()
    texconv_exe = Path(args.texconv).resolve()
    cwebp_exe = Path(args.cwebp).resolve()
    out_dir = Path(args.out).resolve()
    tmp_png_dir = Path(args.tmp_png).resolve()
    keep_png = bool(args.keep_png)

    try:
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")
        if not extracted_textures_root.exists():
            raise FileNotFoundError(f"Extracted textures root not found: {extracted_textures_root}")
        ensure_exe(texconv_exe, "texconv.exe")
        ensure_exe(cwebp_exe, "cwebp.exe")

        out_dir.mkdir(parents=True, exist_ok=True)
        tmp_png_dir.mkdir(parents=True, exist_ok=True)

        counts = process_manifest(
            manifest_path=manifest_path,
            extracted_textures_root=extracted_textures_root,
            texconv_exe=texconv_exe,
            cwebp_exe=cwebp_exe,
            out_dir=out_dir,
            tmp_png_dir=tmp_png_dir,
            keep_png=keep_png,
        )

        print("")
        print("Summary")
        print(f"created: {counts.created}")
        print(f"skipped: {counts.skipped}")
        print(f"failed:  {counts.failed}")

        return 0 if counts.failed == 0 else 2

    except Exception as ex:
        eprint(f"Fatal error: {ex}")
        return 1
    finally:
        # Leave tmp folder in place (deterministic and useful for debugging). User can delete anytime.
        pass


if __name__ == "__main__":
    raise SystemExit(main())