from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from nba_api.stats.endpoints import franchisehistory, leaguegamefinder
from nba_api.stats.static import teams as static_teams

from team_branding import get_team_branding

NBA_LEAGUE_ID = "00"
SEASON_TYPE = "Regular Season"

CACHE_DIR = "./nba_team_games_cache"
OUTPUT_DIR = "./outputs"

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

ProgressCallback = Optional[Callable[[int, int, str], None]]


@dataclass
class PullConfig:
    sleep_s: float = 0.6
    max_seasons_per_team: Optional[int] = None
    cache: bool = True
    force_refresh: bool = False


def mode_name(franchise_mode: bool) -> str:
    return "franchise" if franchise_mode else "team"


def cache_path(team_id: int) -> str:
    return os.path.join(CACHE_DIR, f"team_{team_id}_regular_season_games.parquet")


def league_dataset_path(start_mode: str, franchise_mode: bool) -> str:
    return os.path.join(
        OUTPUT_DIR,
        f"league_winpct_{mode_name(franchise_mode)}_{start_mode}.parquet",
    )


def season_label_from_id(season_id: object) -> str:
    season_str = str(season_id)
    start_year = int(season_str[-4:])
    end_suffix = str((start_year + 1) % 100).zfill(2)
    return f"{start_year}-{end_suffix}"


def get_current_nba_teams() -> pd.DataFrame:
    """
    Returns the active NBA teams from nba_api's static list plus branding metadata.
    """
    teams_df = pd.DataFrame(static_teams.get_teams()).copy()
    teams_df["id"] = teams_df["id"].astype(int)

    branding = teams_df.apply(
        lambda row: get_team_branding(int(row["id"]), str(row["abbreviation"])),
        axis=1,
        result_type="expand",
    )
    out = pd.concat([teams_df, branding], axis=1)
    out = out[
        [
            "id",
            "full_name",
            "abbreviation",
            "nickname",
            "city",
            "state",
            "year_founded",
            "primary_color",
            "secondary_color",
            "logo_url",
        ]
    ].copy()
    return out.sort_values("abbreviation").reset_index(drop=True)


def fetch_team_games_full_history(team_id: int) -> pd.DataFrame:
    """
    Pull a full team history via LeagueGameFinder.
    Returns one row per completed regular-season game from the team perspective.
    """
    lgf = leaguegamefinder.LeagueGameFinder(
        league_id_nullable=NBA_LEAGUE_ID,
        team_id_nullable=team_id,
        season_type_nullable=SEASON_TYPE,
        player_or_team_abbreviation="T",
    )
    df = lgf.get_data_frames()[0]
    if not df.empty:
        df = df[df["WL"].isin(["W", "L"])].copy()
    return df


def normalize_team_games(df: pd.DataFrame, max_seasons_per_team: Optional[int] = None) -> pd.DataFrame:
    """Standardize team game logs from either cache or live pulls."""
    if df.empty:
        return df

    out = df.copy()
    out = out[out["WL"].isin(["W", "L"])].copy()
    out["TEAM_ID"] = out["TEAM_ID"].astype(int)
    out["GAME_DATE"] = pd.to_datetime(out["GAME_DATE"])
    out = out.sort_values(["GAME_DATE", "GAME_ID"]).reset_index(drop=True)

    if max_seasons_per_team is not None:
        ordered_seasons = (
            out.groupby("SEASON_ID")["GAME_DATE"].min().sort_values().index.tolist()
        )
        keep = set(ordered_seasons[-max_seasons_per_team:])
        out = out[out["SEASON_ID"].isin(keep)].reset_index(drop=True)

    return out


def load_or_pull_team_games(team_id: int, pull_cfg: PullConfig) -> pd.DataFrame:
    """
    Pull all available completed regular-season games for a team_id and cache locally.
    """
    path = cache_path(team_id)
    if pull_cfg.cache and not pull_cfg.force_refresh and os.path.exists(path):
        return normalize_team_games(
            pd.read_parquet(path),
            max_seasons_per_team=pull_cfg.max_seasons_per_team,
        )

    out = pd.DataFrame()
    try:
        out = fetch_team_games_full_history(team_id)
    except Exception as exc:
        print(f"[WARN] team_id={team_id} failed: {exc}")

    if pull_cfg.sleep_s:
        time.sleep(pull_cfg.sleep_s)

    out = normalize_team_games(out, max_seasons_per_team=pull_cfg.max_seasons_per_team)

    if pull_cfg.cache:
        out.to_parquet(path, index=False)

    return out


def build_franchise_map(teams_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Build a mapping of TEAM_ID -> FRANCHISE_ID using FranchiseHistory.

    Older nba_api responses expose FRANCHISE_ID directly. Newer responses key active
    franchise history to the current TEAM_ID, so in that shape we synthesize
    FRANCHISE_ID=TEAM_ID.
    """
    try:
        fh = franchisehistory.FranchiseHistory()
        frames = fh.get_data_frames()

        for df in frames:
            cols = set(df.columns)
            if {"TEAM_ID", "FRANCHISE_ID"}.issubset(cols):
                out = df.copy()
                out["TEAM_ID"] = out["TEAM_ID"].astype(int)
                out["FRANCHISE_ID"] = out["FRANCHISE_ID"].astype(int)
                return out

        for df in frames:
            if "TEAM_ID" not in df.columns:
                continue
            out = df.copy()
            out["TEAM_ID"] = out["TEAM_ID"].astype(int)
            out["FRANCHISE_ID"] = out["TEAM_ID"]
            return out
    except Exception as exc:
        if teams_df is None:
            raise
        print(f"[WARN] FranchiseHistory unavailable, falling back to TEAM_ID continuity: {exc}")

    if teams_df is not None:
        fallback = teams_df[["id", "full_name", "city", "nickname", "abbreviation"]].copy()
        fallback = fallback.rename(
            columns={
                "id": "TEAM_ID",
                "full_name": "TEAM_NAME",
                "city": "TEAM_CITY",
            }
        )
        fallback["FRANCHISE_ID"] = fallback["TEAM_ID"]
        return fallback

    raise RuntimeError("Could not build a franchise map.")


def compute_cumulative_win_pct(games: pd.DataFrame, start_mode: str = "game1") -> pd.DataFrame:
    """
    Adds season numbering, cumulative record, and cumulative win percentage.
    """
    if games.empty:
        return games

    df = games.copy()
    df = df.sort_values(["GAME_DATE", "GAME_ID"]).reset_index(drop=True)

    first_seen = (
        df.groupby("SEASON_ID")["GAME_DATE"].min().sort_values().index.astype(str).tolist()
    )
    season_to_num = {sid: i + 1 for i, sid in enumerate(first_seen)}
    df["season_num"] = df["SEASON_ID"].astype(str).map(season_to_num)

    if start_mode == "after_first_season":
        df = df[df["season_num"] >= 2].copy()
        df = df.sort_values(["GAME_DATE", "GAME_ID"]).reset_index(drop=True)
        first_seen = (
            df.groupby("SEASON_ID")["GAME_DATE"].min().sort_values().index.astype(str).tolist()
        )
        season_to_num = {sid: i + 1 for i, sid in enumerate(first_seen)}
        df["season_num"] = df["SEASON_ID"].astype(str).map(season_to_num)

    df["is_win"] = (df["WL"] == "W").astype(int)
    df["is_loss"] = (df["WL"] == "L").astype(int)
    df["game_num_overall"] = range(1, len(df) + 1)
    df["cum_wins"] = df["is_win"].cumsum()
    df["cum_losses"] = df["is_loss"].cumsum()
    df["win_pct"] = df["cum_wins"] / (df["cum_wins"] + df["cum_losses"])

    return df


def season_boundary_xticks(df: pd.DataFrame) -> Tuple[List[int], List[str]]:
    if df.empty:
        return [], []
    starts = df.groupby("season_num")["game_num_overall"].min().sort_index()
    return starts.tolist(), [f"S{int(s)}" for s in starts.index.tolist()]


def pull_all_current_team_games(
    pull_cfg: PullConfig,
    progress_callback: ProgressCallback = None,
) -> Tuple[pd.DataFrame, Dict[int, pd.DataFrame]]:
    """
    Pull regular-season games for every active NBA team.
    """
    teams_df = get_current_nba_teams()
    total = len(teams_df)
    team_games: Dict[int, pd.DataFrame] = {}

    for idx, row in teams_df.iterrows():
        tid = int(row["id"])
        team_games[tid] = load_or_pull_team_games(tid, pull_cfg)
        if progress_callback is not None:
            progress_callback(idx + 1, total, str(row["full_name"]))

    return teams_df, team_games


def aggregate_by_franchise(
    team_games: Dict[int, pd.DataFrame],
    franchise_map_df: pd.DataFrame,
) -> Dict[int, pd.DataFrame]:
    """
    Combine TEAM_ID game logs into FRANCHISE_ID series.
    """
    team_to_franchise = dict(zip(franchise_map_df["TEAM_ID"], franchise_map_df["FRANCHISE_ID"]))
    buckets: Dict[int, List[pd.DataFrame]] = {}

    for team_id, games_df in team_games.items():
        if games_df.empty:
            continue
        franchise_id = team_to_franchise.get(team_id)
        if franchise_id is None:
            continue
        buckets.setdefault(int(franchise_id), []).append(games_df)

    out: Dict[int, pd.DataFrame] = {}
    for franchise_id, frames in buckets.items():
        merged = pd.concat(frames, ignore_index=True)
        merged["GAME_DATE"] = pd.to_datetime(merged["GAME_DATE"])
        merged = merged.sort_values(["GAME_DATE", "GAME_ID"]).reset_index(drop=True)
        out[franchise_id] = merged
    return out


def build_league_dataset(
    start_mode: str = "game1",
    franchise_mode: bool = True,
    pull_cfg: Optional[PullConfig] = None,
    progress_callback: ProgressCallback = None,
    save_to_cache: bool = True,
) -> pd.DataFrame:
    """
    Build a long-form dataset for every active team or franchise.
    """
    cfg = pull_cfg or PullConfig()
    teams_df, team_games = pull_all_current_team_games(cfg, progress_callback=progress_callback)

    franchise_map_df = build_franchise_map(teams_df=teams_df) if franchise_mode else None
    franchise_games = (
        aggregate_by_franchise(team_games, franchise_map_df)
        if franchise_mode and franchise_map_df is not None
        else {}
    )

    rows: List[pd.DataFrame] = []
    for _, team in teams_df.iterrows():
        team_id = int(team["id"])
        franchise_id = team_id
        games_df = team_games.get(team_id, pd.DataFrame())

        if franchise_mode and franchise_map_df is not None:
            franchise_series = franchise_map_df.loc[
                franchise_map_df["TEAM_ID"] == team_id,
                "FRANCHISE_ID",
            ]
            if not franchise_series.empty:
                franchise_id = int(franchise_series.iloc[0])
                games_df = franchise_games.get(franchise_id, pd.DataFrame())

        series_df = compute_cumulative_win_pct(games_df, start_mode=start_mode)
        if series_df.empty:
            continue

        series_df = series_df.copy()
        series_df["data_source"] = "nba_api"
        series_df["entity_id"] = franchise_id if franchise_mode else team_id
        series_df["entity_abbreviation"] = str(team["abbreviation"])
        series_df["entity_name"] = str(team["full_name"])
        series_df["team_id"] = team_id
        series_df["franchise_id"] = franchise_id
        series_df["primary_color"] = str(team["primary_color"])
        series_df["secondary_color"] = str(team["secondary_color"])
        series_df["logo_url"] = str(team["logo_url"])
        series_df["mode"] = mode_name(franchise_mode)
        series_df["start_mode"] = start_mode
        series_df["season_start_year"] = series_df["SEASON_ID"].map(lambda value: int(str(value)[-4:]))
        series_df["season_label"] = series_df["SEASON_ID"].map(season_label_from_id)
        rows.append(series_df)

    dataset = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if not dataset.empty:
        dataset["GAME_DATE"] = pd.to_datetime(dataset["GAME_DATE"])
        dataset = dataset.sort_values(
            ["entity_abbreviation", "GAME_DATE", "GAME_ID"]
        ).reset_index(drop=True)

    if save_to_cache and cfg.cache and cfg.max_seasons_per_team is None:
        dataset.to_parquet(league_dataset_path(start_mode, franchise_mode), index=False)

    return dataset


def load_or_build_league_dataset(
    start_mode: str = "game1",
    franchise_mode: bool = True,
    pull_cfg: Optional[PullConfig] = None,
    rebuild_dataset: bool = False,
    progress_callback: ProgressCallback = None,
) -> pd.DataFrame:
    """
    Load a cached long-form dataset, or build it from the cached team histories / NBA API.
    """
    cfg = pull_cfg or PullConfig()
    path = league_dataset_path(start_mode, franchise_mode)
    use_dataset_cache = (
        cfg.cache
        and cfg.max_seasons_per_team is None
        and not cfg.force_refresh
        and not rebuild_dataset
        and os.path.exists(path)
    )
    if use_dataset_cache:
        return pd.read_parquet(path)

    return build_league_dataset(
        start_mode=start_mode,
        franchise_mode=franchise_mode,
        pull_cfg=cfg,
        progress_callback=progress_callback,
        save_to_cache=cfg.cache,
    )


def summarize_latest_results(dataset: pd.DataFrame) -> pd.DataFrame:
    if dataset.empty:
        return dataset

    first_games = (
        dataset.groupby("entity_abbreviation", as_index=False)["GAME_DATE"]
        .min()
        .rename(columns={"GAME_DATE": "first_game_date"})
    )

    latest = (
        dataset.sort_values(["entity_abbreviation", "GAME_DATE", "GAME_ID"])
        .groupby("entity_abbreviation", as_index=False)
        .tail(1)
        .copy()
    )
    latest = latest.merge(first_games, on="entity_abbreviation", how="left")
    latest["record"] = latest["cum_wins"].astype(int).astype(str) + "-" + latest["cum_losses"].astype(int).astype(str)
    return latest[
        [
            "entity_abbreviation",
            "entity_name",
            "team_id",
            "franchise_id",
            "primary_color",
            "secondary_color",
            "logo_url",
            "record",
            "win_pct",
            "game_num_overall",
            "season_num",
            "first_game_date",
            "GAME_DATE",
        ]
    ].sort_values(["win_pct", "entity_abbreviation"], ascending=[False, True]).reset_index(drop=True)


def filter_dataset_by_entities(dataset: pd.DataFrame, plot_entities: Optional[List[str]] = None) -> pd.DataFrame:
    if dataset.empty or not plot_entities:
        return dataset
    keep = {abbr.upper() for abbr in plot_entities}
    return dataset[dataset["entity_abbreviation"].isin(keep)].copy()


def plot_league_dataset(
    dataset: pd.DataFrame,
    plot_entities: Optional[List[str]] = None,
    save_plot_path: Optional[str] = None,
    show_plot: bool = False,
) -> None:
    import matplotlib.pyplot as plt

    if dataset.empty:
        raise ValueError("No data available to plot.")

    plot_df = filter_dataset_by_entities(dataset, plot_entities=plot_entities)
    if plot_df.empty:
        raise ValueError("No data left after filtering the requested teams.")

    if save_plot_path:
        plot_dir = os.path.dirname(save_plot_path) or OUTPUT_DIR
        os.makedirs(plot_dir, exist_ok=True)

    order = plot_entities or plot_df["entity_abbreviation"].drop_duplicates().tolist()

    plt.figure(figsize=(16, 9))
    for abbr in order:
        team_df = plot_df[plot_df["entity_abbreviation"] == abbr].copy()
        if team_df.empty:
            continue
        color = str(team_df["primary_color"].iloc[0])
        plt.plot(
            team_df["game_num_overall"],
            team_df["win_pct"],
            label=abbr,
            color=color,
            linewidth=1.9,
            alpha=0.95,
        )

    mode = plot_df["mode"].iloc[0]
    start_mode = plot_df["start_mode"].iloc[0]
    plt.ylim(0, 1)
    plt.xlabel("Game number (overall, chronological)")
    plt.ylabel("Cumulative winning percentage")
    plt.title(
        f"Cumulative win% over time (Regular Season) | start_mode={start_mode} | {mode}_mode"
    )
    plt.legend(ncol=3 if len(order) > 12 else 2, fontsize=9)

    ref_abbr = next((abbr for abbr in order if abbr in plot_df["entity_abbreviation"].values), None)
    if ref_abbr is not None:
        ref_df = plot_df[plot_df["entity_abbreviation"] == ref_abbr]
        xticks, labels = season_boundary_xticks(ref_df)
        if len(xticks) <= 25:
            plt.xticks(xticks, labels)

    plt.tight_layout()

    if save_plot_path:
        plt.savefig(save_plot_path, dpi=200)
        print(f"Saved plot -> {save_plot_path}")

    if show_plot:
        plt.show()
    else:
        plt.close()


def main(
    plot_entities: Optional[List[str]] = None,
    start_mode: str = "game1",
    franchise_mode: bool = True,
    pull_cfg: Optional[PullConfig] = None,
    save_plot_path: Optional[str] = "./outputs/nba_all_teams_winpct.png",
    show_plot: bool = False,
    rebuild_dataset: bool = False,
) -> None:
    dataset = load_or_build_league_dataset(
        start_mode=start_mode,
        franchise_mode=franchise_mode,
        pull_cfg=pull_cfg,
        rebuild_dataset=rebuild_dataset,
    )
    plot_league_dataset(
        dataset,
        plot_entities=plot_entities,
        save_plot_path=save_plot_path,
        show_plot=show_plot,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and plot NBA cumulative win percentage histories.")
    parser.add_argument(
        "--teams",
        nargs="*",
        default=None,
        help="Current team abbreviations to plot. Default: all active teams.",
    )
    parser.add_argument(
        "--start-mode",
        choices=["game1", "after_first_season"],
        default="game1",
    )
    parser.add_argument(
        "--team-mode",
        action="store_true",
        help="Use current team histories instead of franchise continuity.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore cached team parquet files and re-pull data from stats.nba.com.",
    )
    parser.add_argument(
        "--rebuild-dataset",
        action="store_true",
        help="Recompute the long-form league parquet from cached team histories.",
    )
    parser.add_argument(
        "--sleep-s",
        type=float,
        default=0.6,
        help="Sleep between team API calls when live-pulling data.",
    )
    parser.add_argument(
        "--save-plot-path",
        default="./outputs/nba_all_teams_winpct.png",
    )
    parser.add_argument(
        "--show-plot",
        action="store_true",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        plot_entities=args.teams,
        start_mode=args.start_mode,
        franchise_mode=not args.team_mode,
        pull_cfg=PullConfig(
            sleep_s=args.sleep_s,
            cache=True,
            force_refresh=args.force_refresh,
        ),
        save_plot_path=args.save_plot_path,
        show_plot=args.show_plot,
        rebuild_dataset=args.rebuild_dataset,
    )
