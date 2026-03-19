from __future__ import annotations

import os
from typing import Callable, Iterable, List, Optional

import pandas as pd

from bball_ref import ScrapeConfig, build_and_save_outputs, dataset_output_path as bref_dataset_output_path
from nba_winpct_franchise import (
    PullConfig,
    league_dataset_path as nba_api_dataset_output_path,
    load_or_build_league_dataset,
    plot_league_dataset,
)

DEFAULT_START_MODE = "game1"
DEFAULT_FRANCHISE_MODE = True
DEFAULT_BREF_CONFIG = ScrapeConfig()

NBA_API_SOURCE = "nba_api"
BREF_SOURCE = "basketball_reference"
SOURCE_ORDER = (NBA_API_SOURCE, BREF_SOURCE)

SOURCE_LABELS = {
    NBA_API_SOURCE: "NBA API",
    BREF_SOURCE: "Basketball-Reference",
}

SOURCE_PULL_DESCRIPTIONS = {
    NBA_API_SOURCE: "stats.nba.com team game logs",
    BREF_SOURCE: "Basketball-Reference schedule pages",
}

ProgressCallback = Optional[Callable[[int, int, str], None]]


def source_label(source: str) -> str:
    return SOURCE_LABELS[source]


def list_sources() -> List[str]:
    return list(SOURCE_ORDER)


def source_dataset_path(source: str) -> str:
    if source == NBA_API_SOURCE:
        return nba_api_dataset_output_path(DEFAULT_START_MODE, DEFAULT_FRANCHISE_MODE)
    if source == BREF_SOURCE:
        return bref_dataset_output_path(DEFAULT_BREF_CONFIG)
    raise ValueError(f"Unsupported source: {source}")


def source_plot_path(source: str) -> str:
    if source == NBA_API_SOURCE:
        return "./outputs/nba_all_teams_winpct.png"
    if source == BREF_SOURCE:
        return "./outputs/nba_all_teams_winpct_basketball_reference.png"
    raise ValueError(f"Unsupported source: {source}")


def source_exists(source: str) -> bool:
    return os.path.exists(source_dataset_path(source))


def read_nonempty_parquet(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    return None if df.empty else df


def source_dataset_ready(source: str) -> bool:
    return read_nonempty_parquet(source_dataset_path(source)) is not None


def should_auto_build_when_missing(source: str) -> bool:
    return source == NBA_API_SOURCE


def load_or_build_source_dataset(
    source: str,
    rebuild_dataset: bool = False,
    force_refresh: bool = False,
    progress_callback: ProgressCallback = None,
    allow_missing: bool = False,
) -> pd.DataFrame:
    if source == NBA_API_SOURCE:
        return load_or_build_league_dataset(
            start_mode=DEFAULT_START_MODE,
            franchise_mode=DEFAULT_FRANCHISE_MODE,
            pull_cfg=PullConfig(
                sleep_s=0.6 if force_refresh else 0.0,
                cache=True,
                force_refresh=force_refresh,
            ),
            rebuild_dataset=rebuild_dataset or force_refresh,
            progress_callback=progress_callback,
        )

    if source == BREF_SOURCE:
        path = source_dataset_path(source)
        cached = None if rebuild_dataset or force_refresh else read_nonempty_parquet(path)
        if cached is not None:
            return cached
        if allow_missing and not rebuild_dataset and not force_refresh:
            return pd.DataFrame()
        _, _, dataset = build_and_save_outputs(
            ScrapeConfig(force_refresh=force_refresh)
        )
        return dataset

    raise ValueError(f"Unsupported source: {source}")


def build_sources(
    sources: Iterable[str],
    force_refresh: bool = False,
    rebuild_dataset: bool = False,
    skip_plot: bool = False,
    progress_callback: ProgressCallback = None,
) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    for source in sources:
        dataset = load_or_build_source_dataset(
            source,
            rebuild_dataset=rebuild_dataset,
            force_refresh=force_refresh,
            progress_callback=progress_callback,
        )
        outputs[source] = dataset
        if not skip_plot and not dataset.empty:
            plot_league_dataset(
                dataset,
                save_plot_path=source_plot_path(source),
                show_plot=False,
            )
    return outputs
