from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
os.chdir(ROOT_DIR)

from dataset_sources import build_sources, list_sources, source_dataset_path, source_plot_path, source_label
from nba_winpct_franchise import summarize_latest_results

BACKUP_DIR = Path("backups")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh one or more published datasets with a pre-refresh zip backup."
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=list_sources(),
        default=list_sources(),
        help="Dataset sources to refresh. Default: all sources.",
    )
    parser.add_argument(
        "--mode",
        choices=("rebuild-dataset", "force-refresh"),
        default="rebuild-dataset",
        help=(
            "rebuild-dataset reuses cached upstream source data where possible; "
            "force-refresh ignores upstream caches and pulls fresh source data."
        ),
    )
    parser.add_argument(
        "--skip-plot",
        action="store_true",
        help="Skip regenerating static PNGs.",
    )
    parser.add_argument(
        "--keep-last",
        type=int,
        default=2,
        help="How many refresh backup zips to retain. Default: 2.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating a pre-refresh backup zip.",
    )
    return parser.parse_args()


def published_paths_for_sources(sources: list[str]) -> list[Path]:
    paths: list[Path] = []
    for source in sources:
        dataset_path = Path(source_dataset_path(source))
        plot_path = Path(source_plot_path(source))
        paths.append(dataset_path)
        paths.append(plot_path)
    unique_paths: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        unique_paths.append(path)
    return unique_paths


def create_backup_zip(paths: list[Path], sources: list[str], mode: str, keep_last: int) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    source_slug = "-".join(sources)
    zip_path = BACKUP_DIR / f"{stamp}_{source_slug}_{mode}.zip"
    manifest = {
        "created_at_utc": stamp,
        "sources": sources,
        "mode": mode,
        "files": [str(path) for path in existing],
    }

    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest, indent=2))
        for path in existing:
            archive.write(path, arcname=str(path))

    prune_old_backups(keep_last)
    return zip_path


def restore_from_backup(zip_path: Path, paths: list[Path]) -> None:
    if not zip_path.exists():
        raise FileNotFoundError(f"Backup zip not found: {zip_path}")
    with ZipFile(zip_path, "r") as archive:
        names = set(archive.namelist())
        for path in paths:
            archive_name = str(path)
            if archive_name not in names:
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            archive.extract(archive_name, path=".")


def prune_old_backups(keep_last: int) -> None:
    if keep_last < 1 or not BACKUP_DIR.exists():
        return
    backups = sorted(BACKUP_DIR.glob("*.zip"), key=lambda path: path.stat().st_mtime, reverse=True)
    for stale in backups[keep_last:]:
        stale.unlink(missing_ok=True)


def print_build_summary(source: str, dataset_rows: int, latest_rows: int) -> None:
    print(f"\n=== {source_label(source)} ===")
    print(f"Dataset path: {source_dataset_path(source)}")
    print(f"Rows: {dataset_rows}")
    print(f"Teams summarized: {latest_rows}")


def existing_team_count(dataset_path: Path) -> int:
    if not dataset_path.exists():
        return 0
    dataset = pd.read_parquet(dataset_path)
    if dataset.empty:
        return 0
    return int(dataset["entity_abbreviation"].nunique())


def main() -> None:
    args = parse_args()
    sources = list(args.sources)
    selected_paths = published_paths_for_sources(sources)
    baseline_team_counts = {
        source: existing_team_count(Path(source_dataset_path(source)))
        for source in sources
    }
    backup_path: Path | None = None

    if not args.no_backup:
        backup_path = create_backup_zip(selected_paths, sources, args.mode, args.keep_last)
        if backup_path is not None:
            print(f"Created backup: {backup_path}")
        else:
            print("No existing published files found; skipped backup.")

    built = build_sources(
        sources,
        force_refresh=args.mode == "force-refresh",
        rebuild_dataset=True,
        skip_plot=args.skip_plot,
    )

    for source in sources:
        dataset = built[source]
        latest = summarize_latest_results(dataset) if not dataset.empty else dataset
        baseline_team_count = baseline_team_counts[source]
        current_team_count = len(latest)
        if baseline_team_count and current_team_count < baseline_team_count:
            if backup_path is not None:
                restore_from_backup(backup_path, selected_paths)
            raise RuntimeError(
                f"{source_label(source)} refresh produced {current_team_count} teams; "
                f"baseline was {baseline_team_count}. Published files were restored from backup."
            )
        print_build_summary(source, len(dataset), len(latest))

    prune_old_backups(args.keep_last)


if __name__ == "__main__":
    main()
