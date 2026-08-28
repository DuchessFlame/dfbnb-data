#!/usr/bin/env python3
"""
build_season_reward_images.py
-----------------------------
Turns a raw BAE texture extraction into the per-season AVIF reward art the
scoreboard pages consume.

Three stages, each independently runnable:

  --sort      Resolve every reward in dist/season_images/season_{N}_images.json
              against the extracted textures tree and copy the matching DDS into
                  .Season Images\\Season {N}\\{outAvif stem}.dds
              Only the TRANSPARENT variant is taken. Bethesda ships each reward as
              several textures:
                  *_l.dds       transparent "large" inventory icon   <- the one we want
                  *_c1/_c2.dds  opaque storefront/carousel product shots
                  *_d/_n/_r     diffuse / normal / roughness map source art
              so the resolver tries "{stem}_l.dds" before the bare name, and any
              file whose alpha channel turns out to be fully opaque is rejected.

  --convert   Convert each sorted DDS to alpha-preserving AVIF in place, verify the
              result decodes and still has alpha, then delete the DDS.

  --prune     Delete the extracted textures tree once conversion has verified.

Run with no stage flags to do sort + convert (the normal path). --prune is never
implied: deleting the extraction is destructive and must be asked for.

WHY THE MANIFESTS DRIVE THIS: the manifests already join each curated reward to its
ENTM texture path, so filenames come out matching what df-bnb-seasons.js expects
(resolveImageUrl rewrites the reward imageUrl to /season_images/season-{N}/{stem}.avif).
Sorting by hand would not.

STATUS: active
INPUT:  dist/season_images/season_*_images.json, a BAE textures/ extraction
OUTPUT: .Season Images/Season {N}/*.avif
USAGE:
  python tools/build_season_reward_images.py --textures "C:\\...\\.Season Images\\textures" \\
      --dest "C:\\...\\.Season Images" --dry-run
  python tools/build_season_reward_images.py --textures ... --dest ...
  python tools/build_season_reward_images.py --dest ... --convert
  python tools/build_season_reward_images.py --textures ... --prune

Requires: pip install pillow pillow-avif-plugin
"""

import argparse
import json
import os
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_DIR = REPO_ROOT / "dist" / "season_images"

TAG = "[season_reward_images]"

# AVIF quality. 72 matches tools/convert_season_images_to_avif.py; the reward art is
# small flat-shaded icon work, so this is visually lossless at a fraction of the size.
QUALITY = 72

# Variant suffixes that are never the transparent inventory icon.
OPAQUE_SUFFIX_RE = re.compile(r"_(c\d+|d|n|r|s|ao|m)$", re.IGNORECASE)

# Player icons are SHARED, not per-season: the same icon recurs across seasons and
# also appears in bundles / the Atom Shop / request items. They stage into one folder
# named by source texture (atx_playericon_score_01.avif), matching the imageUrl that
# src/route_shared_images.py writes, and upload to
# /wp-content/uploads/storefront/player-icons/.
ICON_DIR_NAME = "_player-icons"
ICON_ENT_RE = re.compile(r"playericon", re.IGNORECASE)
VARIANT_RE = re.compile(r"_(l|c\d+|d|n|r)$", re.IGNORECASE)


def log(msg: str) -> None:
    print(f"{TAG} {msg}")


# ---------------------------------------------------------------------------
# Stage 1: sort
# ---------------------------------------------------------------------------

def index_textures(root: Path) -> tuple[dict, dict]:
    """Index every .dds by normalised relative path and by bare filename stem."""
    by_rel: dict[str, Path] = {}
    by_stem: dict[str, list[Path]] = defaultdict(list)
    for dirpath, _, files in os.walk(root):
        for f in files:
            if not f.lower().endswith(".dds"):
                continue
            full = Path(dirpath) / f
            rel = "textures/" + str(full.relative_to(root)).replace("\\", "/").lower()
            by_rel[rel] = full
            by_stem[Path(f).stem.lower()].append(full)
    return by_rel, by_stem


def resolve_texture(dds_path: str, by_rel: dict, by_stem: dict) -> tuple[Path | None, str]:
    """Find the transparent texture for a manifest ddsPath.

    Tries, in order: the exact path with "_l" appended, the exact path, then the same
    two by bare filename (the extraction's folder layout does not always match ENTM's
    recorded ETIP). Returns (path, how) or (None, "").
    """
    d = dds_path.lower()
    stem_full = d[:-4] if d.endswith(".dds") else d
    base = os.path.basename(stem_full)

    if stem_full + "_l.dds" in by_rel:
        return by_rel[stem_full + "_l.dds"], "path_l"
    if d in by_rel:
        return by_rel[d], "path"
    if base + "_l" in by_stem:
        return by_stem[base + "_l"][0], "stem_l"
    if base in by_stem:
        return by_stem[base][0], "stem"
    return None, ""


def has_alpha(path: Path) -> bool | None:
    """True if the image carries real (non-uniform, not fully opaque) transparency.

    Returns None if the file cannot be decoded at all.
    """
    try:
        from PIL import Image
        with Image.open(path) as im:
            if im.mode not in ("RGBA", "LA", "PA"):
                return False
            alpha = im.convert("RGBA").getchannel("A")
            lo, hi = alpha.getextrema()
            return lo < 255            # something is at least partly transparent
    except Exception:
        return None


def stage_sort(textures: Path, dest_root: Path, dry: bool, keep_opaque: bool) -> int:
    by_rel, by_stem = index_textures(textures)
    log(f"indexed {len(by_rel)} .dds files under {textures}")

    manifests = sorted(
        MANIFEST_DIR.glob("season_*_images.json"),
        key=lambda p: int(re.search(r"season_(\d+)_", p.name).group(1)),
    )

    total = copied = unresolved = opaque = undecodable = 0
    report: list[tuple] = []

    for mf in manifests:
        man = json.loads(mf.read_text(encoding="utf-8"))
        season = man["seasonNumber"]
        images = man.get("images") or []
        if not images:
            continue

        out_dir = dest_root / f"Season {season}"
        if not dry:
            out_dir.mkdir(parents=True, exist_ok=True)

        s_copy = s_unres = s_op = 0
        for img in images:
            total += 1
            src, _how = resolve_texture(img["ddsPath"], by_rel, by_stem)
            if src is None:
                unresolved += 1
                s_unres += 1
                continue

            alpha = has_alpha(src)
            if alpha is None:
                undecodable += 1
                log(f"  [WARN] S{season} cannot decode {src.name} - skipped")
                continue
            if not alpha and not keep_opaque:
                # An opaque match means we found a product shot, not the icon.
                opaque += 1
                s_op += 1
                continue

            # Player icons go to the shared folder, named by source texture, so the
            # same icon is not duplicated into every season that awarded it.
            if ICON_ENT_RE.search(img.get("entitlement", "")):
                icon_dir = dest_root / ICON_DIR_NAME
                if not dry:
                    icon_dir.mkdir(parents=True, exist_ok=True)
                stem = VARIANT_RE.sub("", src.stem).lower()
                out = icon_dir / (stem + ".dds")
                if out.exists():
                    continue          # already staged by an earlier season
            else:
                out = out_dir / (Path(img["outAvif"]).stem + ".dds")

            if not dry:
                shutil.copy2(src, out)
            copied += 1
            s_copy += 1

        report.append((season, s_copy, len(images), s_unres, s_op))

    log("")
    log(f"{'season':>8}  {'copied':>7}  {'of':>4}  {'unresolved':>11}  {'opaque-only':>12}")
    for season, c, t, u, o in report:
        log(f"{'S'+str(season):>8}  {c:>7}  {t:>4}  {u:>11}  {o:>12}")
    log("")
    log(f"TOTAL {copied}/{total} copied | {unresolved} unresolved | "
        f"{opaque} opaque-only | {undecodable} undecodable")
    if dry:
        log("dry run - nothing written")
    return copied


# ---------------------------------------------------------------------------
# Stage 2: convert
# ---------------------------------------------------------------------------

def stage_convert(dest_root: Path, dry: bool) -> int:
    try:
        import pillow_avif  # noqa: F401  (registers the AVIF encoder)
        from PIL import Image
    except ImportError:
        sys.exit(f"{TAG} [ERROR] pip install pillow pillow-avif-plugin")

    season_dirs = sorted(
        [p for p in dest_root.glob("Season *") if p.is_dir()],
        key=lambda p: int(p.name.split()[-1]),
    )
    # The shared player-icons staging folder converts on the same terms.
    icons = dest_root / ICON_DIR_NAME
    if icons.is_dir():
        season_dirs.append(icons)

    done = failed = dropped = 0
    for sd in season_dirs:
        dds = sorted(sd.glob("*.dds"))
        if not dds:
            continue
        ok = drop = 0
        for src in dds:
            # A folder populated by --sort is already filtered, but a folder the user
            # dropped a raw extraction into is not. Re-check here so opaque product
            # shots and _d/_n/_r map source art never become AVIF: they are deleted
            # as "extra images" instead.
            if not dry:
                alpha = has_alpha(src)
                if alpha is None:
                    failed += 1
                    log(f"  [FAIL] {sd.name}/{src.name}: cannot decode")
                    continue
                if not alpha or OPAQUE_SUFFIX_RE.search(src.stem):
                    try:
                        src.unlink()
                        drop += 1
                    except OSError as e:
                        log(f"  [WARN] could not remove opaque {src.name}: {e}")
                    continue

            out = src.with_suffix(".avif")
            if dry:
                ok += 1
                continue
            try:
                with Image.open(src) as im:
                    im.convert("RGBA").save(out, format="AVIF", quality=QUALITY)
                # Verify before dropping the source: it must decode and keep alpha.
                with Image.open(out) as chk:
                    chk.load()
                    if chk.convert("RGBA").getchannel("A").getextrema()[0] == 255:
                        log(f"  [WARN] {sd.name}/{out.name} lost transparency")
                src.unlink()
                ok += 1
            except Exception as e:
                failed += 1
                log(f"  [FAIL] {sd.name}/{src.name}: {e}")
        done += ok
        dropped += drop
        log(f"{sd.name:>10}: {ok} converted"
            + (f", {drop} opaque/map source deleted" if drop else ""))

    log(f"TOTAL {done} AVIF written, {dropped} opaque deleted, {failed} failed"
        + (" (dry run)" if dry else ""))
    return failed


# ---------------------------------------------------------------------------
# Stage 3: prune
# ---------------------------------------------------------------------------

def dir_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def stage_prune(textures: Path, dest_root: Path, dry: bool) -> None:
    # Refuse to prune while any season still holds unconverted DDS - that would
    # destroy the only copy of art that has not been converted yet.
    leftover = ([p for p in dest_root.glob("Season */*.dds")]
                + [p for p in (dest_root / ICON_DIR_NAME).glob("*.dds")])
    if leftover:
        sys.exit(f"{TAG} [ERROR] {len(leftover)} unconverted .dds remain "
                 f"(e.g. {leftover[0]}). Run --convert first; refusing to prune.")

    if not textures.exists():
        log(f"nothing to prune - {textures} does not exist")
        return

    size = dir_bytes(textures)
    log(f"textures tree: {size/1024/1024:.0f} MB at {textures}")
    if dry:
        log("dry run - not deleted")
        return
    shutil.rmtree(textures)
    log(f"deleted textures tree - reclaimed {size/1024/1024:.0f} MB")


# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--textures", help="Root of the BAE extraction (contains atx/).")
    ap.add_argument("--dest", required=True, help="The .Season Images folder.")
    ap.add_argument("--sort", action="store_true")
    ap.add_argument("--convert", action="store_true")
    ap.add_argument("--prune", action="store_true",
                    help="Delete the textures tree. Never implied.")
    ap.add_argument("--keep-opaque", action="store_true",
                    help="Also keep matches with no transparency (default: skip).")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    dest = Path(args.dest)
    if not dest.is_dir():
        sys.exit(f"{TAG} [ERROR] --dest not a directory: {dest}")

    # No stage flags = the normal path: sort then convert. Pruning stays opt-in.
    stages = (args.sort, args.convert, args.prune)
    if not any(stages):
        args.sort = args.convert = True

    if args.sort:
        if not args.textures:
            sys.exit(f"{TAG} [ERROR] --sort needs --textures")
        tex = Path(args.textures)
        if not tex.is_dir():
            sys.exit(f"{TAG} [ERROR] --textures not a directory: {tex}")
        log("=== SORT ===")
        stage_sort(tex, dest, args.dry_run, args.keep_opaque)

    if args.convert:
        log("=== CONVERT ===")
        if stage_convert(dest, args.dry_run):
            sys.exit(f"{TAG} [ERROR] conversion failures - not safe to prune")

    if args.prune:
        if not args.textures:
            sys.exit(f"{TAG} [ERROR] --prune needs --textures")
        log("=== PRUNE ===")
        stage_prune(Path(args.textures), dest, args.dry_run)

    log("Done.")


if __name__ == "__main__":
    main()
