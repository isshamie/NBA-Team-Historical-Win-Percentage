from __future__ import annotations

import argparse

from dataset_sources import (
    BREF_SOURCE,
    NBA_API_SOURCE,
    build_sources,
    list_sources,
    source_dataset_path,
    source_label,
)
from nba_winpct_franchise import summarize_latest_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build one or more cached NBA win percentage datasets."
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=list_sources(),
        default=[NBA_API_SOURCE],
        help="Dataset sources to build. Default: nba_api.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help=(
            "Force a fresh source pull. For nba_api this re-pulls stats.nba.com; "
            "for Basketball-Reference this ignores cached page HTML."
        ),
    )
    parser.add_argument(
        "--rebuild-dataset",
        action="store_true",
        help="Recompute the requested dataset(s) from cached upstream source data where possible.",
    )
    parser.add_argument(
        "--skip-plot",
        action="store_true",
        help="Skip generating static PNGs after building the datasets.",
    )
    return parser.parse_args()


def print_progress(current: int, total: int, team_name: str) -> None:
    print(f"[{current:02d}/{total:02d}] {team_name}", flush=True)


def main() -> None:
    args = parse_args()
    built = build_sources(
        args.sources,
        force_refresh=args.force_refresh,
        rebuild_dataset=args.rebuild_dataset or args.force_refresh,
        skip_plot=args.skip_plot,
        progress_callback=print_progress if args.rebuild_dataset or args.force_refresh else None,
    )

    for source in args.sources:
        dataset = built[source]
        print(f"\n=== {source_label(source)} ===")
        print(f"Dataset path: {source_dataset_path(source)}")
        print(f"Built dataset rows: {len(dataset)}")
        if dataset.empty:
            print("Dataset is empty.")
            continue

        latest = summarize_latest_results(dataset)
        print(f"Teams in dataset: {latest['entity_abbreviation'].nunique()}")
        print("Top 5 latest cumulative win percentages:")
        print(
            latest[["entity_abbreviation", "record", "win_pct", "GAME_DATE"]]
            .head(5)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
