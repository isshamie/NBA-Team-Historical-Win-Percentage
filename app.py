from __future__ import annotations

import html
import math
import os
from datetime import datetime
from typing import Iterable, List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dataset_sources import (
    BREF_SOURCE,
    NBA_API_SOURCE,
    list_sources,
    load_or_build_source_dataset,
    should_auto_build_when_missing,
    source_dataset_path,
    source_dataset_ready,
    source_label,
)
from nba_winpct_franchise import get_current_nba_teams, summarize_latest_results

DEFAULT_START_MODE = "game1"
DEFAULT_FRANCHISE_MODE = True
CARD_COLUMNS = 4
DEFAULT_SOURCE = BREF_SOURCE
REFRESH_WORKFLOW_URL = "https://github.com/isshamie/NBA-Team-Historical-Win-Percentage/actions/workflows/refresh-data.yml"


def apply_app_chrome() -> None:
    st.set_page_config(
        page_title="NBA Franchise Win% Explorer",
        page_icon="🏀",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@500&display=swap');

        :root {
            --paper: #f4efe6;
            --ink: #131313;
            --muted: #5f5a54;
            --line: rgba(19, 19, 19, 0.08);
        }

        html, body, [class*="css"] {
            font-family: "Space Grotesk", sans-serif;
        }

        header[data-testid="stHeader"] {
            background: rgba(0, 0, 0, 0);
            height: 0;
        }

        [data-testid="stToolbar"] {
            right: 0.8rem;
            top: 0.75rem;
        }

        .block-container {
            padding-top: 1.35rem;
            padding-bottom: 3rem;
        }

        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(245, 132, 38, 0.18), transparent 28%),
                radial-gradient(circle at top right, rgba(12, 35, 64, 0.18), transparent 28%),
                linear-gradient(180deg, #fbf7f0 0%, var(--paper) 100%);
            color: var(--ink);
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(12, 35, 64, 0.95), rgba(19, 19, 19, 0.95));
        }

        [data-testid="stSidebar"] * {
            color: #f7f4ee;
        }

        [data-testid="stSidebar"] .stButton > button {
            width: 100%;
            background: rgba(255, 255, 255, 0.1);
            color: #f7f4ee;
            border: 1px solid rgba(255, 255, 255, 0.18);
        }

        [data-testid="stSidebar"] .stButton > button:hover {
            border-color: rgba(245, 132, 38, 0.55);
            color: #fff;
            background: rgba(245, 132, 38, 0.14);
        }

        .hero {
            padding: 1.4rem 1.6rem;
            border-radius: 24px;
            background: linear-gradient(135deg, rgba(12, 35, 64, 0.98), rgba(30, 41, 59, 0.98));
            color: #f8f5ef;
            box-shadow: 0 24px 60px rgba(12, 35, 64, 0.22);
            overflow: hidden;
        }

        .hero h1 {
            margin: 0;
            font-size: clamp(2rem, 3vw, 3.4rem);
            line-height: 1.02;
            letter-spacing: -0.03em;
        }

        .hero p {
            margin: 0.75rem 0 0;
            max-width: 64rem;
            color: rgba(248, 245, 239, 0.82);
            font-size: 1rem;
        }

        .meta-strip {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-top: 1rem;
        }

        .meta-pill {
            padding: 0.9rem 1rem;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.12);
        }

        .meta-pill span {
            display: block;
            color: rgba(248, 245, 239, 0.64);
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .meta-pill strong {
            display: block;
            margin-top: 0.35rem;
            font-size: 1.05rem;
        }

        .section-kicker {
            margin: 1.2rem 0 0.6rem;
            font-family: "IBM Plex Mono", monospace;
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
        }

        .team-card {
            position: relative;
            padding: 1rem;
            min-height: 214px;
            border-radius: 22px;
            background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(255,255,255,0.84));
            border: 1px solid var(--line);
            box-shadow: 0 12px 30px rgba(19, 19, 19, 0.08);
            overflow: hidden;
        }

        .team-card::before {
            content: "";
            position: absolute;
            inset: 0 0 auto 0;
            height: 7px;
            background: linear-gradient(90deg, var(--primary), var(--secondary));
        }

        .team-head {
            display: flex;
            align-items: center;
            gap: 0.8rem;
            margin-top: 0.25rem;
        }

        .team-head img {
            width: 46px;
            height: 46px;
            object-fit: contain;
            flex-shrink: 0;
        }

        .team-code {
            font-family: "IBM Plex Mono", monospace;
            font-size: 0.84rem;
            letter-spacing: 0.08em;
            color: var(--primary);
        }

        .team-name {
            font-size: 1rem;
            font-weight: 700;
            color: var(--ink);
            line-height: 1.05;
        }

        .team-metrics {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 0.6rem;
            margin-top: 0.95rem;
        }

        .team-metric {
            padding: 0.72rem 0.75rem;
            border-radius: 14px;
            background: rgba(19, 19, 19, 0.04);
        }

        .team-metric span {
            display: block;
            color: var(--muted);
            font-size: 0.68rem;
            text-transform: uppercase;
            letter-spacing: 0.07em;
        }

        .team-metric strong {
            display: block;
            margin-top: 0.35rem;
            font-size: 0.96rem;
            color: var(--ink);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_timestamp(path: str) -> str:
    if not os.path.exists(path):
        return "Not built yet"
    return datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")


def selection_state_key(source: str) -> str:
    return f"selected_teams_{source}"


def ensure_selection_state(source: str, all_teams: List[str]) -> str:
    key = selection_state_key(source)
    if key not in st.session_state:
        st.session_state[key] = list(all_teams)
    else:
        selected = [team for team in st.session_state[key] if team in all_teams]
        st.session_state[key] = selected
    return key


def load_dataset(
    source: str,
    rebuild_dataset: bool,
    force_refresh: bool,
    show_progress: bool,
    allow_missing: bool,
) -> pd.DataFrame:
    progress_holder = st.sidebar.empty()
    status_holder = st.sidebar.empty()

    def callback(current: int, total: int, team_name: str) -> None:
        progress_holder.progress(
            current / total,
            text=f"Collecting {team_name} ({current}/{total})",
        )
        status_holder.caption(
            "Live pull in progress from stats.nba.com"
            if source == NBA_API_SOURCE
            else "Live pull in progress from Basketball-Reference"
        )

    if show_progress and source == BREF_SOURCE:
        status_holder.caption("Building from Basketball-Reference schedule pages")

    dataset = load_or_build_source_dataset(
        source,
        rebuild_dataset=rebuild_dataset or force_refresh,
        force_refresh=force_refresh,
        progress_callback=callback if show_progress and source == NBA_API_SOURCE else None,
        allow_missing=allow_missing,
    )

    progress_holder.empty()
    status_holder.empty()
    return dataset


def chunked(items: List[object], size: int) -> Iterable[List[object]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def format_optional_pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "No games"
    return f"{float(value):.3f}"


def format_optional_text(value: object, fallback: str = "No games") -> str:
    if value is None or pd.isna(value):
        return fallback
    return str(value)


def team_card_html(row: object) -> str:
    return f"""
    <div class="team-card" style="--primary:{row.primary_color}; --secondary:{row.secondary_color};">
        <div class="team-head">
            <img src="{html.escape(row.logo_url)}" alt="{html.escape(row.entity_abbreviation)} logo" />
            <div>
                <div class="team-code">{html.escape(row.entity_abbreviation)}</div>
                <div class="team-name">{html.escape(row.entity_name)}</div>
            </div>
        </div>
        <div class="team-metrics">
            <div class="team-metric">
                <span>Range Win%</span>
                <strong>{format_optional_pct(getattr(row, "range_win_pct", None))}</strong>
            </div>
            <div class="team-metric">
                <span>All-time Win%</span>
                <strong>{row.win_pct:.3f}</strong>
            </div>
            <div class="team-metric">
                <span>Range record</span>
                <strong>{html.escape(format_optional_text(getattr(row, "range_record", None)))}</strong>
            </div>
            <div class="team-metric">
                <span>All-time record</span>
                <strong>{html.escape(row.record)}</strong>
            </div>
        </div>
    </div>
    """


def render_team_cards(summary_df: pd.DataFrame) -> None:
    if summary_df.empty:
        st.info("No teams selected.")
        return

    rows = list(summary_df.itertuples())
    for row_group in chunked(rows, CARD_COLUMNS):
        columns = st.columns(CARD_COLUMNS)
        for column, row in zip(columns, row_group):
            with column:
                st.markdown(team_card_html(row), unsafe_allow_html=True)


def build_team_cards_summary(
    all_time_summary_df: pd.DataFrame,
    range_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if all_time_summary_df.empty:
        return all_time_summary_df

    merged = all_time_summary_df.copy()
    range_fields = (
        range_summary_df[
            [
                "entity_abbreviation",
                "record",
                "win_pct",
                "GAME_DATE",
            ]
        ]
        .rename(
            columns={
                "record": "range_record",
                "win_pct": "range_win_pct",
                "GAME_DATE": "range_last_game_date",
            }
        )
        if not range_summary_df.empty
        else pd.DataFrame(columns=["entity_abbreviation", "range_record", "range_win_pct", "range_last_game_date"])
    )
    merged = merged.merge(range_fields, on="entity_abbreviation", how="left")
    merged["range_sort"] = merged["range_win_pct"].fillna(-1.0)
    merged = merged.sort_values(
        ["range_sort", "win_pct", "entity_abbreviation"],
        ascending=[False, False, True],
    ).drop(columns=["range_sort"])
    return merged.reset_index(drop=True)


def make_trace(team_df: pd.DataFrame, x_column: str, x_label: str) -> go.Scatter:
    team_color = team_df["primary_color"].iloc[0]
    secondary_color = team_df["secondary_color"].iloc[0]
    abbr = team_df["entity_abbreviation"].iloc[0]
    return go.Scatter(
        x=team_df[x_column],
        y=team_df["win_pct"],
        mode="lines",
        name=abbr,
        line={"color": team_color, "width": 2.4},
        opacity=0.9,
        hoverlabel={
            "bgcolor": "rgba(255,255,255,0.96)",
            "bordercolor": secondary_color,
            "font": {"color": "#131313"},
        },
        customdata=team_df[
            ["entity_name", "season_label", "cum_wins", "cum_losses", "GAME_DATE"]
        ],
        hovertemplate=(
            "<b>%{customdata[0]}</b> (%{fullData.name})<br>"
            f"{x_label} %{{x}}<br>"
            "Win pct %{y:.3f}<br>"
            "Record %{customdata[2]}-%{customdata[3]}<br>"
            "Season %{customdata[1]}<br>"
            "Date %{customdata[4]|%Y-%m-%d}<extra></extra>"
        ),
    )


def apply_base_chart_layout(fig: go.Figure, xaxis_title: str) -> None:
    fig.add_hline(y=0.5, line_dash="dot", line_color="rgba(19,19,19,0.22)")
    fig.update_layout(
        height=720,
        margin={"l": 24, "r": 24, "t": 42, "b": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.82)",
        font={"family": "Space Grotesk, sans-serif", "color": "#131313"},
        legend={
            "orientation": "v",
            "x": 1.02,
            "xanchor": "left",
            "y": 1,
            "yanchor": "top",
            "bgcolor": "rgba(255,255,255,0.72)",
        },
        hovermode="closest",
        hoverdistance=18,
        xaxis_title=xaxis_title,
        yaxis_title="Cumulative winning percentage",
        yaxis={"range": [0, 1], "tickformat": ".0%"},
    )


def make_game_number_chart(filtered_df: pd.DataFrame, selected_teams: List[str]) -> go.Figure:
    fig = go.Figure()
    if filtered_df.empty:
        return fig

    for abbr in selected_teams:
        team_df = filtered_df[filtered_df["entity_abbreviation"] == abbr].copy()
        if team_df.empty:
            continue
        fig.add_trace(make_trace(team_df, "game_num_overall", "Game"))

    apply_base_chart_layout(fig, "Game number (chronological)")
    return fig


def season_tick_frame(filtered_df: pd.DataFrame) -> pd.DataFrame:
    ticks = (
        filtered_df.groupby("season_label", as_index=False)["GAME_DATE"]
        .min()
        .sort_values("GAME_DATE")
        .reset_index(drop=True)
    )
    if len(ticks) > 24:
        step = math.ceil(len(ticks) / 24)
        ticks = ticks.iloc[::step].reset_index(drop=True)
    return ticks


def ordered_season_labels(filtered_df: pd.DataFrame) -> List[str]:
    if filtered_df.empty:
        return []
    return (
        filtered_df.groupby("season_label", as_index=False)["GAME_DATE"]
        .min()
        .sort_values("GAME_DATE")
        ["season_label"]
        .tolist()
    )


def ensure_season_range_state(source: str, season_labels: List[str]) -> tuple[str, str] | None:
    if not season_labels:
        return None
    key = f"season_range_{source}"
    default = (season_labels[0], season_labels[-1])
    current = st.session_state.get(key)
    if (
        not isinstance(current, (list, tuple))
        or len(current) != 2
        or current[0] not in season_labels
        or current[1] not in season_labels
        or season_labels.index(current[0]) > season_labels.index(current[1])
    ):
        st.session_state[key] = default
    start, end = st.select_slider(
        "Season range for chronology + selected teams",
        options=season_labels,
        value=tuple(st.session_state[key]),
        key=key,
    )
    return str(start), str(end)


def filter_df_to_season_range(filtered_df: pd.DataFrame, season_range: tuple[str, str] | None) -> pd.DataFrame:
    if filtered_df.empty or season_range is None:
        return filtered_df
    season_labels = ordered_season_labels(filtered_df)
    if not season_labels:
        return filtered_df
    start, end = season_range
    start_idx = season_labels.index(start)
    end_idx = season_labels.index(end)
    allowed = set(season_labels[start_idx : end_idx + 1])
    return filtered_df[filtered_df["season_label"].isin(allowed)].copy()


def ensure_game_range_state(source: str, filtered_df: pd.DataFrame) -> tuple[int, int] | None:
    if filtered_df.empty:
        return None
    min_game = int(filtered_df["game_num_overall"].min())
    max_game = int(filtered_df["game_num_overall"].max())
    key = f"game_range_{source}"
    default = (min_game, max_game)
    current = st.session_state.get(key)
    if (
        not isinstance(current, (list, tuple))
        or len(current) != 2
        or int(current[0]) < min_game
        or int(current[1]) > max_game
        or int(current[0]) > int(current[1])
    ):
        st.session_state[key] = default
    start, end = st.slider(
        "Game number range",
        min_value=min_game,
        max_value=max_game,
        value=tuple(int(v) for v in st.session_state[key]),
        key=key,
    )
    return int(start), int(end)


def filter_df_to_game_range(filtered_df: pd.DataFrame, game_range: tuple[int, int] | None) -> pd.DataFrame:
    if filtered_df.empty or game_range is None:
        return filtered_df
    start, end = game_range
    return filtered_df[
        filtered_df["game_num_overall"].between(start, end, inclusive="both")
    ].copy()


def recompute_window_cumulative_metrics(filtered_df: pd.DataFrame) -> pd.DataFrame:
    if filtered_df.empty:
        return filtered_df.copy()

    ordered = filtered_df.sort_values(
        ["entity_abbreviation", "GAME_DATE", "game_num_overall", "GAME_ID"],
        kind="mergesort",
    ).copy()
    grouped = ordered.groupby("entity_abbreviation", sort=False)
    ordered["cum_wins"] = grouped["is_win"].cumsum()
    ordered["cum_losses"] = grouped["is_loss"].cumsum()
    totals = ordered["cum_wins"] + ordered["cum_losses"]
    ordered["win_pct"] = ordered["cum_wins"] / totals.where(totals != 0, pd.NA)
    ordered["wins_after_game"] = ordered["cum_wins"]
    ordered["losses_after_game"] = ordered["cum_losses"]
    ordered["win_pct_after_game"] = ordered["win_pct"]
    ordered["computed_games_after_game"] = totals
    ordered["game_number_matches_record"] = True
    return ordered


def make_chronology_chart(filtered_df: pd.DataFrame, selected_teams: List[str]) -> go.Figure:
    fig = go.Figure()
    if filtered_df.empty:
        return fig

    for abbr in selected_teams:
        team_df = filtered_df[filtered_df["entity_abbreviation"] == abbr].copy()
        if team_df.empty:
            continue
        fig.add_trace(make_trace(team_df, "GAME_DATE", "Date"))

    ticks = season_tick_frame(filtered_df)
    apply_base_chart_layout(fig, "Season chronology")
    fig.update_xaxes(
        tickmode="array",
        tickvals=ticks["GAME_DATE"],
        ticktext=ticks["season_label"],
        tickangle=-45,
    )
    return fig


def main() -> None:
    apply_app_chrome()

    teams_df = get_current_nba_teams()
    team_options = teams_df["abbreviation"].tolist()
    team_name_map = dict(zip(teams_df["abbreviation"], teams_df["full_name"]))

    st.sidebar.title("Controls")
    source = st.sidebar.radio(
        "Dataset source",
        options=list_sources(),
        index=list_sources().index(DEFAULT_SOURCE),
        format_func=source_label,
        help="Choose which historical data source powers the charts and tables.",
    )
    selected_key = ensure_selection_state(source, team_options)

    if st.sidebar.button("Select all teams", use_container_width=True):
        st.session_state[selected_key] = list(team_options)
    if st.sidebar.button("Clear all teams", use_container_width=True):
        st.session_state[selected_key] = []

    st.sidebar.multiselect(
        "Teams",
        options=team_options,
        key=selected_key,
        format_func=lambda abbr: f"{abbr} | {team_name_map[abbr]}",
        help="All 30 teams are selected by default. Remove any teams you do not want on the charts.",
    )
    st.sidebar.markdown("### Data refresh")
    st.sidebar.caption(
        "This app is read-only. Refresh datasets from GitHub Actions or run "
        "`python scripts/refresh_data.py` locally, then redeploy."
    )
    st.sidebar.markdown(f"[Open refresh workflow ↗]({REFRESH_WORKFLOW_URL})")

    dataset_path = source_dataset_path(source)
    dataset_missing = not source_dataset_ready(source)
    auto_build_missing = dataset_missing and should_auto_build_when_missing(source)
    requested_rebuild = auto_build_missing

    with st.spinner("Building league dataset..." if requested_rebuild else "Loading dataset..."):
        dataset = load_dataset(
            source=source,
            rebuild_dataset=False,
            force_refresh=False,
            show_progress=requested_rebuild,
            allow_missing=not requested_rebuild,
        )

    selected_teams = st.session_state[selected_key]
    filtered_df = dataset[dataset["entity_abbreviation"].isin(selected_teams)].copy()
    summary_df = summarize_latest_results(filtered_df)
    selected_count = len(summary_df)
    latest_date = (
        summary_df["GAME_DATE"].max().date().isoformat() if not summary_df.empty else "n/a"
    )

    st.markdown(
        f"""
        <section class="hero">
            <h1>NBA Franchise Win% Explorer</h1>
            <p>
                Current team branding is applied across each franchise continuity line, and the charts now start
                from game 1 by default. Hovering is trace-specific, so the tooltip follows the line under your cursor
                instead of summarizing every team at the same x-position.
            </p>
            <div class="meta-strip">
                <div class="meta-pill">
                    <span>Dataset source</span>
                    <strong>{html.escape(source_label(source))}</strong>
                </div>
                <div class="meta-pill">
                    <span>History mode</span>
                    <strong>Franchise continuity</strong>
                </div>
                <div class="meta-pill">
                    <span>Teams shown</span>
                    <strong>{selected_count}</strong>
                </div>
                <div class="meta-pill">
                    <span>Dataset built</span>
                    <strong>{html.escape(format_timestamp(dataset_path))}</strong>
                </div>
                <div class="meta-pill">
                    <span>Latest completed game</span>
                    <strong>{html.escape(latest_date)}</strong>
                </div>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )

    if dataset.empty and dataset_missing and source == BREF_SOURCE and not requested_rebuild:
        st.info(
            "The Basketball-Reference dataset has not been built in this deployment yet. "
            "Run the GitHub refresh workflow or `python scripts/refresh_data.py --sources basketball_reference` first."
        )
    elif dataset.empty and source == BREF_SOURCE:
        st.warning(
            "The Basketball-Reference dataset is currently empty. "
            "That usually means the scrape failed or the source rate-limited the build."
        )

    st.markdown('<div class="section-kicker">Season chronology</div>', unsafe_allow_html=True)
    st.caption("This slider drives both the chronology chart and the selected-team cards below. The running win% is recomputed from the first game inside the selected season window.")
    chronology_df = filtered_df
    cards_summary_df = summary_df
    if not filtered_df.empty:
        season_labels = ordered_season_labels(filtered_df)
        season_range = ensure_season_range_state(source, season_labels)
        chronology_df = recompute_window_cumulative_metrics(
            filter_df_to_season_range(filtered_df, season_range)
        )
        cards_summary_df = build_team_cards_summary(summary_df, summarize_latest_results(chronology_df))
        st.plotly_chart(make_chronology_chart(chronology_df, selected_teams), use_container_width=True)

    st.markdown('<div class="section-kicker">Selected teams</div>', unsafe_allow_html=True)
    st.caption("Cards are sorted by the selected season range. Range Win% reflects only the active season window; All-time Win% remains the full franchise baseline.")
    render_team_cards(cards_summary_df)

    st.markdown('<div class="section-kicker">Game progression</div>', unsafe_allow_html=True)
    st.caption("Hover a line directly to inspect that team only. The running win% is recomputed from the first game inside the selected game-number window.")
    game_progression_df = filtered_df
    if filtered_df.empty:
        st.warning("Select at least one team to render the charts.")
    else:
        game_range = ensure_game_range_state(source, filtered_df)
        game_progression_df = recompute_window_cumulative_metrics(
            filter_df_to_game_range(filtered_df, game_range)
        )
        st.plotly_chart(make_game_number_chart(game_progression_df, selected_teams), use_container_width=True)

    st.markdown('<div class="section-kicker">Latest record snapshot</div>', unsafe_allow_html=True)
    if summary_df.empty:
        st.info("No team summary available.")
    else:
        table = summary_df[
            [
                "entity_abbreviation",
                "entity_name",
                "record",
                "win_pct",
                "first_game_date",
                "GAME_DATE",
            ]
        ].copy()
        table = table.rename(
            columns={
                "entity_abbreviation": "Team",
                "entity_name": "Name",
                "record": "Record",
                "win_pct": "Win %",
                "first_game_date": "First game",
                "GAME_DATE": "Last completed game",
            }
        )
        table["Win %"] = table["Win %"].map(lambda value: f"{value:.3f}")
        table["First game"] = table["First game"].dt.strftime("%Y-%m-%d")
        table["Last completed game"] = table["Last completed game"].dt.strftime("%Y-%m-%d")
        st.dataframe(table, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
