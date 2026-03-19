"""
Build a historical NBA franchise running win% dataset from Basketball-Reference.

Key points:
- `year=1947` means the 1946-47 season.
- The raw scrape is one row per team-game, so each actual game appears twice:
  once from each team's perspective.
- A validation table checks whether the final running W-L record from the
  schedule page matches the record shown in the page metadata.
- A normalized parquet is also written in the same long-form shape used by the
  current Streamlit app, while keeping the existing `nba_api` dataset separate.
"""

from __future__ import annotations

import argparse
import os
import re
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass
from io import StringIO
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from lxml import html

from nba_api.stats.static import teams as static_teams

from team_branding import get_team_branding

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import Response as PlaywrightResponse
    from playwright.sync_api import sync_playwright
except ImportError:
    PlaywrightError = Exception
    PlaywrightResponse = object
    sync_playwright = None


OUTPUT_DIR = "./outputs"
CACHE_DIR = "./bref_page_cache"

START_YEAR = 1947
END_YEAR = 2025
INCLUDE_ABA = False
ACTIVE_FRANCHISES_ONLY = True
SLEEP_BETWEEN_TEAMS = 0.8
SLEEP_BETWEEN_SEASONS = 1.5
MAX_RETRIES = 3
REQUEST_TIMEOUT_S = 30
MAX_RATE_LIMIT_SLEEP_S = int(os.getenv("BREF_MAX_RATE_LIMIT_SLEEP_S", "60"))
DEFAULT_FETCH_BACKEND = os.getenv("BREF_FETCH_BACKEND", "auto")
DEFAULT_CHROME_PATH = os.getenv("BREF_CHROME_PATH", "")
DEFAULT_BROWSER_PATHS = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
)
MODE_NAME = "franchise"
START_MODE = "game1"
DATA_SOURCE = "basketball_reference"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)


FRANCHISE_ALIASES: Dict[str, Dict[str, tuple[str, ...]]] = {
    "ATL": {
        "abbrs": ("ATL", "STL", "MLH", "TRI"),
        "names": (
            "Atlanta Hawks",
            "St. Louis Hawks",
            "Saint Louis Hawks",
            "Milwaukee Hawks",
            "Tri-Cities Blackhawks",
            "Tri Cities Blackhawks",
        ),
    },
    "BKN": {
        "abbrs": ("BKN", "NJN", "NYN", "NJA"),
        "names": (
            "Brooklyn Nets",
            "New Jersey Nets",
            "New York Nets",
            "New Jersey Americans",
        ),
    },
    "BOS": {"abbrs": ("BOS",), "names": ("Boston Celtics",)},
    "CHA": {
        "abbrs": ("CHA", "CHH", "CHO"),
        "names": (
            "Charlotte Hornets",
            "Charlotte Bobcats",
        ),
    },
    "CHI": {"abbrs": ("CHI",), "names": ("Chicago Bulls",)},
    "CLE": {"abbrs": ("CLE",), "names": ("Cleveland Cavaliers",)},
    "DAL": {"abbrs": ("DAL",), "names": ("Dallas Mavericks",)},
    "DEN": {
        "abbrs": ("DEN", "DNA", "DNR"),
        "names": (
            "Denver Nuggets",
            "Denver Rockets",
        ),
    },
    "DET": {
        "abbrs": ("DET", "FTW"),
        "names": (
            "Detroit Pistons",
            "Fort Wayne Pistons",
        ),
    },
    "GSW": {
        "abbrs": ("GSW", "SFW", "PHW", "GS"),
        "names": (
            "Golden State Warriors",
            "San Francisco Warriors",
            "Philadelphia Warriors",
        ),
    },
    "HOU": {
        "abbrs": ("HOU", "SDR"),
        "names": (
            "Houston Rockets",
            "San Diego Rockets",
        ),
    },
    "IND": {"abbrs": ("IND", "INA"), "names": ("Indiana Pacers",)},
    "LAC": {
        "abbrs": ("LAC", "SDC", "BUF"),
        "names": (
            "Los Angeles Clippers",
            "San Diego Clippers",
            "Buffalo Braves",
        ),
    },
    "LAL": {
        "abbrs": ("LAL",),
        "names": (
            "Los Angeles Lakers",
            "Minneapolis Lakers",
        ),
    },
    "MEM": {
        "abbrs": ("MEM", "VAN"),
        "names": (
            "Memphis Grizzlies",
            "Vancouver Grizzlies",
        ),
    },
    "MIA": {"abbrs": ("MIA",), "names": ("Miami Heat",)},
    "MIL": {"abbrs": ("MIL",), "names": ("Milwaukee Bucks",)},
    "MIN": {"abbrs": ("MIN",), "names": ("Minnesota Timberwolves",)},
    "NOP": {
        "abbrs": ("NOP", "NOH", "NOK"),
        "names": (
            "New Orleans Pelicans",
            "New Orleans Hornets",
            "New Orleans/Oklahoma City Hornets",
            "New Orleans Oklahoma City Hornets",
        ),
    },
    "NYK": {"abbrs": ("NYK",), "names": ("New York Knicks",)},
    "OKC": {
        "abbrs": ("OKC", "SEA"),
        "names": (
            "Oklahoma City Thunder",
            "Seattle SuperSonics",
        ),
    },
    "ORL": {"abbrs": ("ORL",), "names": ("Orlando Magic",)},
    "PHI": {
        "abbrs": ("PHI", "SYR"),
        "names": (
            "Philadelphia 76ers",
            "Syracuse Nationals",
        ),
    },
    "PHX": {"abbrs": ("PHX", "PHO"), "names": ("Phoenix Suns",)},
    "POR": {"abbrs": ("POR",), "names": ("Portland Trail Blazers",)},
    "SAC": {
        "abbrs": ("SAC", "KCK", "KCO", "CIN", "ROC"),
        "names": (
            "Sacramento Kings",
            "Kansas City Kings",
            "Kansas City-Omaha Kings",
            "Cincinnati Royals",
            "Rochester Royals",
        ),
    },
    "SAS": {
        "abbrs": ("SAS", "TEX", "DLC"),
        "names": (
            "San Antonio Spurs",
            "Texas Chaparrals",
            "Dallas Chaparrals",
        ),
    },
    "TOR": {"abbrs": ("TOR",), "names": ("Toronto Raptors",)},
    "UTA": {
        "abbrs": ("UTA", "NOJ"),
        "names": (
            "Utah Jazz",
            "New Orleans Jazz",
        ),
    },
    "WAS": {
        "abbrs": ("WAS", "WSB", "CAP", "BAL", "CHZ", "CHP"),
        "names": (
            "Washington Wizards",
            "Washington Bullets",
            "Capital Bullets",
            "Baltimore Bullets",
            "Chicago Zephyrs",
            "Chicago Packers",
        ),
    },
}


@dataclass
class ScrapeConfig:
    start_year: int = START_YEAR
    end_year: int = END_YEAR
    include_aba: bool = INCLUDE_ABA
    active_franchises_only: bool = ACTIVE_FRANCHISES_ONLY
    sleep_between_teams: float = SLEEP_BETWEEN_TEAMS
    sleep_between_seasons: float = SLEEP_BETWEEN_SEASONS
    max_retries: int = MAX_RETRIES
    request_timeout_s: int = REQUEST_TIMEOUT_S
    max_rate_limit_sleep_s: int = MAX_RATE_LIMIT_SLEEP_S
    fetch_backend: str = DEFAULT_FETCH_BACKEND
    chrome_executable_path: str = DEFAULT_CHROME_PATH
    team_limit: Optional[int] = None
    cache_responses: bool = True
    force_refresh: bool = False


def season_label(end_year: int) -> str:
    start = end_year - 1
    return f"{start}-{str(end_year)[-2:]}"


def season_id_from_end_year(end_year: int) -> str:
    return f"2{end_year - 1}"


def league_codes_for_year(end_year: int, include_aba: bool) -> List[str]:
    if end_year <= 1949:
        return ["BAA"]
    if include_aba and 1968 <= end_year <= 1976:
        return ["NBA", "ABA"]
    return ["NBA"]


def normalize_name(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    replacements = {
        ".": "",
        ",": "",
        "'": "",
        "-": " ",
        "/": " ",
        "&": "and",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return " ".join(text.split())


def normalize_location(token: Optional[str]) -> str:
    text = str(token or "").strip().lower()
    if text in {"@", "away", "road"}:
        return "away"
    return "home"


def normalize_result(value: Optional[str]) -> str:
    text = str(value or "").strip().lower()
    if text in {"win", "w"}:
        return "W"
    if text in {"loss", "l"}:
        return "L"
    return str(value or "").strip().upper()


def coerce_optional_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return None


def normalize_whitespace(text: str) -> str:
    return " ".join(str(text).split())


def get_current_nba_teams() -> pd.DataFrame:
    teams_df = pd.DataFrame(static_teams.get_teams()).copy()
    teams_df["id"] = teams_df["id"].astype(int)
    branding = teams_df.apply(
        lambda row: get_team_branding(int(row["id"]), str(row["abbreviation"])),
        axis=1,
        result_type="expand",
    )
    out = pd.concat([teams_df, branding], axis=1)
    return out[
        [
            "id",
            "full_name",
            "abbreviation",
            "nickname",
            "city",
            "year_founded",
            "primary_color",
            "secondary_color",
            "logo_url",
        ]
    ].sort_values("abbreviation").reset_index(drop=True)


def build_alias_lookups() -> tuple[Dict[str, str], Dict[str, str]]:
    name_lookup: Dict[str, str] = {}
    abbr_lookup: Dict[str, str] = {}

    for current_abbr, values in FRANCHISE_ALIASES.items():
        for name in values["names"]:
            name_lookup[normalize_name(name)] = current_abbr
        for abbr in values["abbrs"]:
            abbr_lookup[abbr.upper()] = current_abbr

    return name_lookup, abbr_lookup


def resolve_current_abbreviation(
    source_team_abbr: str,
    source_team_name: str,
    name_lookup: Dict[str, str],
    abbr_lookup: Dict[str, str],
) -> Optional[str]:
    normalized_name = normalize_name(source_team_name)
    if normalized_name in name_lookup:
        return name_lookup[normalized_name]
    return abbr_lookup.get(str(source_team_abbr).strip().upper())


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def playwright_available() -> bool:
    return sync_playwright is not None


def chrome_available(path: str) -> bool:
    return bool(path) and os.path.exists(path)


def resolve_browser_executable_path(configured_path: str) -> Optional[str]:
    if chrome_available(configured_path):
        return configured_path
    for candidate in DEFAULT_BROWSER_PATHS:
        if chrome_available(candidate):
            return candidate
    return None


def resolve_fetch_backend(fetch_backend: str, chrome_executable_path: str) -> str:
    backend = fetch_backend.strip().lower()
    if backend not in {"auto", "requests", "playwright"}:
        raise ValueError(f"Unsupported fetch backend: {fetch_backend}")
    if backend == "requests":
        return "requests"
    if backend == "playwright":
        if not playwright_available():
            raise RuntimeError("Playwright backend requested, but the playwright package is not installed.")
        return "playwright"
    if playwright_available() and resolve_browser_executable_path(chrome_executable_path):
        return "playwright"
    return "requests"


class HtmlFetcher(AbstractContextManager["HtmlFetcher"]):
    def fetch(self, url: str, timeout_s: int) -> tuple[Optional[int], Dict[str, str], Optional[str]]:
        raise NotImplementedError

    def close(self) -> None:
        return None

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.close()
        return None


class RequestsFetcher(HtmlFetcher):
    def __init__(self) -> None:
        self.session = make_session()

    def fetch(self, url: str, timeout_s: int) -> tuple[Optional[int], Dict[str, str], Optional[str]]:
        response = self.session.get(url, timeout=timeout_s)
        return response.status_code, dict(response.headers), response.text

    def close(self) -> None:
        self.session.close()


class PlaywrightFetcher(HtmlFetcher):
    def __init__(self, chrome_executable_path: str) -> None:
        if sync_playwright is None:
            raise RuntimeError("Playwright is not installed.")
        self._manager = sync_playwright().start()
        browser_path = resolve_browser_executable_path(chrome_executable_path)
        launch_kwargs = {"headless": True}
        if browser_path:
            launch_kwargs["executable_path"] = browser_path
        self._browser = self._manager.chromium.launch(**launch_kwargs)
        self._context = self._browser.new_context(user_agent=USER_AGENT)
        self._context.route(
            "**/*",
            lambda route, request: route.continue_()
            if request.resource_type == "document"
            else route.abort(),
        )
        self._page = self._context.new_page()

    def fetch(self, url: str, timeout_s: int) -> tuple[Optional[int], Dict[str, str], Optional[str]]:
        response = self._page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
        headers = dict(response.headers) if response is not None else {}
        status = response.status if response is not None else None
        return status, headers, self._page.content()

    def close(self) -> None:
        self._page.close()
        self._context.close()
        self._browser.close()
        self._manager.stop()


def create_fetcher(fetch_backend: str, chrome_executable_path: str) -> HtmlFetcher:
    backend = resolve_fetch_backend(fetch_backend, chrome_executable_path)
    if backend == "playwright":
        return PlaywrightFetcher(chrome_executable_path)
    return RequestsFetcher()


def cache_path_for_url(url: str) -> str:
    trimmed = re.sub(r"^https?://", "", url).strip("/")
    safe = trimmed.replace("/", "__")
    return os.path.join(CACHE_DIR, f"{safe}.html")


def fetch_url_text(
    url: str,
    fetcher: HtmlFetcher,
    max_retries: int,
    timeout_s: int,
    max_rate_limit_sleep_s: int,
    cache_responses: bool = True,
    force_refresh: bool = False,
) -> Optional[str]:
    path = cache_path_for_url(url)
    if cache_responses and not force_refresh and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read()

    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            status_code, headers, text = fetcher.fetch(url, timeout_s=timeout_s)
            if status_code == 404:
                return None
            if status_code == 429:
                retry_after = coerce_optional_int(headers.get("Retry-After"))
                if retry_after is not None and retry_after > max_rate_limit_sleep_s:
                    raise RuntimeError(
                        "Basketball-Reference rate-limited the request with "
                        f"Retry-After={retry_after}s, which exceeds the configured cap of "
                        f"{max_rate_limit_sleep_s}s. Rerun later or raise "
                        "BREF_MAX_RATE_LIMIT_SLEEP_S if you really want to wait that long."
                    )
                sleep_for = min(retry_after if retry_after is not None else 5 * attempt, max_rate_limit_sleep_s)
                print(f"[warn] GET {url} hit 429; sleeping {sleep_for}s before retry")
                time.sleep(sleep_for)
                continue
            if status_code is not None and status_code >= 400:
                raise RuntimeError(f"GET {url} failed with status {status_code}")
            if cache_responses:
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(text or "")
            return text
        except (requests.RequestException, PlaywrightError, RuntimeError) as exc:
            last_err = exc
            print(f"[warn] GET {url} failed on attempt {attempt}/{max_retries}: {exc}")
            time.sleep(2 * attempt)
    if last_err is not None:
        raise RuntimeError(f"Could not fetch {url}") from last_err
    return None


def league_page_url(league_code: str, year: int) -> str:
    return f"https://www.basketball-reference.com/leagues/{league_code}_{year}.html"


def team_schedule_url(source_team_abbr: str, year: int) -> str:
    return f"https://www.basketball-reference.com/teams/{source_team_abbr}/{year}_games.html"


def extract_team_links(league_html: str, year: int) -> List[Tuple[str, str]]:
    tree = html.fromstring(league_html)
    selectors = (
        "//table[@id='per_game-team']//tbody/tr[not(contains(@class,'thead'))]/td[@data-stat='team']/a",
        "//table[@id='advanced-team']//tbody/tr[not(contains(@class,'thead'))]/td[@data-stat='team']/a",
    )

    links: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for selector in selectors:
        for node in tree.xpath(selector):
            href = node.get("href") or ""
            match = re.search(rf"/teams/([^/]+)/{year}\.html$", href)
            if not match:
                continue
            abbr = match.group(1).upper()
            name = normalize_whitespace("".join(node.itertext()))
            key = (abbr, name)
            if key in seen:
                continue
            seen.add(key)
            links.append(key)
    return links


def extract_schedule_meta(schedule_html: str, fallback_name: str) -> tuple[str, Optional[int], Optional[int]]:
    tree = html.fromstring(schedule_html)
    meta_text = normalize_whitespace(" ".join(tree.xpath("//div[@id='meta']//text()")))
    heading_text = normalize_whitespace(" ".join(tree.xpath("//div[@id='meta']//h1//text()")))

    team_name = fallback_name
    heading_match = re.search(r"\d{4}-\d{2}\s+(.*?)\s+Schedule and Results", heading_text)
    if heading_match:
        team_name = heading_match.group(1).strip()
    elif heading_text.endswith("Schedule and Results"):
        team_name = heading_text.replace("Schedule and Results", "").strip()

    record_match = re.search(r"Record:\s*(\d+)-(\d+)", meta_text)
    wins = int(record_match.group(1)) if record_match else None
    losses = int(record_match.group(2)) if record_match else None
    return team_name, wins, losses


def first_text(node: html.HtmlElement, data_stat: str) -> Optional[str]:
    values = node.xpath(f".//*[@data-stat='{data_stat}']//text()")
    if not values:
        return None
    text = normalize_whitespace(" ".join(values))
    return text or None


def first_href(node: html.HtmlElement, data_stat: str) -> Optional[str]:
    values = node.xpath(f".//*[@data-stat='{data_stat}']//a/@href")
    if not values:
        return None
    return str(values[0])


def parse_boxscore_id(boxscore_href: Optional[str]) -> Optional[str]:
    if not boxscore_href:
        return None
    match = re.search(r"/boxscores/([^.]+)\.html$", boxscore_href)
    return match.group(1) if match else None


def parse_opponent_abbr(opponent_href: Optional[str], year: int) -> Optional[str]:
    if not opponent_href:
        return None
    match = re.search(rf"/teams/([^/]+)/{year}\.html$", opponent_href)
    return match.group(1).upper() if match else None


def build_game_id(
    boxscore_id: Optional[str],
    source_team_abbr: str,
    year: int,
    game_number: Optional[int],
    game_date: Optional[pd.Timestamp],
) -> str:
    if boxscore_id:
        return boxscore_id
    date_part = game_date.strftime("%Y%m%d") if game_date is not None and not pd.isna(game_date) else str(year)
    game_part = str(game_number or 0).zfill(3)
    return f"bref_{source_team_abbr}_{date_part}_{game_part}"


def find_schedule_result_column(df: pd.DataFrame) -> Optional[str]:
    for column in df.columns:
        if str(column) in {"G", "Date", "Opponent", "Tm", "Opp", "W", "L", "Streak", "Attend.", "LOG", "Notes"}:
            continue
        values = {
            str(value).strip().upper()
            for value in df[column].dropna().tolist()
            if str(value).strip()
        }
        if values and values.issubset({"W", "L"}):
            return str(column)
    return None


def find_schedule_location_column(df: pd.DataFrame) -> Optional[str]:
    for column in df.columns:
        values = {
            str(value).strip()
            for value in df[column].dropna().tolist()
            if str(value).strip()
        }
        if values and values.issubset({"@"}):
            return str(column)
    return None


def parse_schedule_rows(
    schedule_html: str,
    year: int,
    source_team_abbr: str,
) -> List[dict]:
    try:
        tables = pd.read_html(StringIO(schedule_html), attrs={"id": "games"})
    except ValueError:
        return []
    if not tables:
        return []

    df = tables[0].copy()
    if "G" not in df.columns or "Date" not in df.columns:
        return []

    df["G_numeric"] = pd.to_numeric(df["G"], errors="coerce")
    df = df[df["G_numeric"].notna()].copy()
    if df.empty:
        return []

    result_col = find_schedule_result_column(df)
    location_col = find_schedule_location_column(df)
    parsed_rows: List[dict] = []
    for _, row in df.iterrows():
        game_number = coerce_optional_int(row.get("G_numeric"))
        date_text = row.get("Date")
        game_date = pd.to_datetime(date_text, errors="coerce")
        if pd.isna(game_date):
            continue

        result = normalize_result(row.get(result_col) if result_col else None)
        wins = coerce_optional_int(row.get("W"))
        losses = coerce_optional_int(row.get("L"))
        opponent_name = None if pd.isna(row.get("Opponent")) else str(row.get("Opponent"))
        opponent_abbr = None
        location = normalize_location(row.get(location_col) if location_col else None)

        parsed_rows.append(
            {
                "game_number": game_number,
                "date": game_date,
                "location": location,
                "opponent_name": opponent_name,
                "opponent_abbr": opponent_abbr,
                "result": result,
                "points_scored": coerce_optional_int(row.get("Tm")),
                "points_allowed": coerce_optional_int(row.get("Opp")),
                "wins_after_game": wins,
                "losses_after_game": losses,
                "streak_after_game": None if pd.isna(row.get("Streak")) else str(row.get("Streak")),
                "game_id": build_game_id(
                    boxscore_id=None,
                    source_team_abbr=source_team_abbr,
                    year=year,
                    game_number=game_number,
                    game_date=game_date,
                ),
            }
        )

    return parsed_rows


def build_matchup(team_abbr: str, opponent_abbr: Optional[str], opponent_name: Optional[str], location: str) -> str:
    opponent = opponent_abbr or opponent_name or "UNK"
    if location == "away":
        return f"{team_abbr} @ {opponent}"
    return f"{team_abbr} vs. {opponent}"


def fetch_season_records(
    year: int,
    cfg: ScrapeConfig,
    current_teams_df: pd.DataFrame,
    name_lookup: Dict[str, str],
    abbr_lookup: Dict[str, str],
    fetcher: HtmlFetcher,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    teams_by_abbr = current_teams_df.set_index("abbreviation")
    game_rows: List[dict] = []
    validation_rows: List[dict] = []

    print(f"\n=== Pulling season {season_label(year)} ===")

    seen_teams: set[Tuple[str, str]] = set()
    for league_code in league_codes_for_year(year, cfg.include_aba):
        season_page = fetch_url_text(
            league_page_url(league_code, year),
            fetcher=fetcher,
            max_retries=cfg.max_retries,
            timeout_s=cfg.request_timeout_s,
            max_rate_limit_sleep_s=cfg.max_rate_limit_sleep_s,
            cache_responses=cfg.cache_responses,
            force_refresh=cfg.force_refresh,
        )
        if not season_page:
            print(f"[warn] No season page found for {league_code} {year}")
            continue

        season_teams = extract_team_links(season_page, year)
        if not season_teams:
            print(f"[warn] No teams found on {league_code} {year} season page")
            continue

        processed_teams = 0
        for source_team_abbr, seed_team_name in season_teams:
            if (source_team_abbr, seed_team_name) in seen_teams:
                continue
            seen_teams.add((source_team_abbr, seed_team_name))

            current_abbr = resolve_current_abbreviation(
                source_team_abbr,
                seed_team_name,
                name_lookup=name_lookup,
                abbr_lookup=abbr_lookup,
            )
            if cfg.active_franchises_only and current_abbr is None:
                print(f"  -> skipping inactive/unmapped franchise {source_team_abbr} {seed_team_name}")
                continue

            current_meta = (
                teams_by_abbr.loc[current_abbr]
                if current_abbr is not None and current_abbr in teams_by_abbr.index
                else None
            )

            schedule_page = fetch_url_text(
                team_schedule_url(source_team_abbr, year),
                fetcher=fetcher,
                max_retries=cfg.max_retries,
                timeout_s=cfg.request_timeout_s,
                max_rate_limit_sleep_s=cfg.max_rate_limit_sleep_s,
                cache_responses=cfg.cache_responses,
                force_refresh=cfg.force_refresh,
            )
            if not schedule_page:
                print(f"[warn] No schedule page found for {source_team_abbr} {year}")
                continue

            source_team_name, season_wins, season_losses = extract_schedule_meta(
                schedule_page,
                fallback_name=seed_team_name,
            )
            print(
                "  -> "
                f"{source_team_abbr} {source_team_name}"
                + (f" => {current_abbr}" if current_abbr else "")
            )

            raw_games = parse_schedule_rows(schedule_page, year=year, source_team_abbr=source_team_abbr)
            if season_wins is not None and season_losses is not None:
                regular_season_games = season_wins + season_losses
                raw_games = [
                    game
                    for game in raw_games
                    if (
                        coerce_optional_int(game.get("wins_after_game")) is not None
                        and coerce_optional_int(game.get("losses_after_game")) is not None
                        and (
                            coerce_optional_int(game.get("wins_after_game"))
                            + coerce_optional_int(game.get("losses_after_game"))
                        )
                        <= regular_season_games
                    )
                ]
            if not raw_games:
                print(f"[warn] No regular-season rows parsed for {source_team_abbr} {year}")
                continue

            last_reg_wins = None
            last_reg_losses = None
            reg_games = 0

            for raw_game in raw_games:
                reg_games += 1
                last_reg_wins = raw_game["wins_after_game"]
                last_reg_losses = raw_game["losses_after_game"]

                row = {
                    "data_source": DATA_SOURCE,
                    "season_end_year": year,
                    "season_start_year": year - 1,
                    "season": season_label(year),
                    "season_id": season_id_from_end_year(year),
                    "league": league_code,
                    "source_team_abbr": source_team_abbr,
                    "source_team_name": source_team_name,
                    "entity_abbreviation": current_abbr,
                    "entity_name": None if current_meta is None else str(current_meta["full_name"]),
                    "team_id": None if current_meta is None else int(current_meta["id"]),
                    "franchise_id": None if current_meta is None else int(current_meta["id"]),
                    "primary_color": None if current_meta is None else str(current_meta["primary_color"]),
                    "secondary_color": None if current_meta is None else str(current_meta["secondary_color"]),
                    "logo_url": None if current_meta is None else str(current_meta["logo_url"]),
                    "game_number": raw_game["game_number"],
                    "date": raw_game["date"],
                    "location": raw_game["location"],
                    "opponent_abbr": raw_game["opponent_abbr"],
                    "opponent_name": raw_game["opponent_name"],
                    "result": raw_game["result"],
                    "points_scored": raw_game["points_scored"],
                    "points_allowed": raw_game["points_allowed"],
                    "wins_after_game": raw_game["wins_after_game"],
                    "losses_after_game": raw_game["losses_after_game"],
                    "streak_after_game": raw_game["streak_after_game"],
                    "playoffs": False,
                    "game_id": raw_game["game_id"],
                    "matchup": build_matchup(
                        source_team_abbr,
                        raw_game["opponent_abbr"],
                        raw_game["opponent_name"],
                        raw_game["location"],
                    ),
                }
                game_rows.append(row)

            validation_rows.append(
                {
                    "data_source": DATA_SOURCE,
                    "season_end_year": year,
                    "season": season_label(year),
                    "league": league_code,
                    "source_team_abbr": source_team_abbr,
                    "source_team_name": source_team_name,
                    "entity_abbreviation": current_abbr,
                    "entity_name": None if current_meta is None else str(current_meta["full_name"]),
                    "reg_games_seen": reg_games,
                    "last_game_wins": last_reg_wins,
                    "last_game_losses": last_reg_losses,
                    "season_summary_wins": season_wins,
                    "season_summary_losses": season_losses,
                    "record_matches_summary": (
                        (last_reg_wins == season_wins) and (last_reg_losses == season_losses)
                        if season_wins is not None and season_losses is not None
                        else pd.NA
                    ),
                }
            )

            processed_teams += 1
            time.sleep(cfg.sleep_between_teams)
            if cfg.team_limit is not None and processed_teams >= cfg.team_limit:
                break

        if cfg.team_limit is not None and processed_teams >= cfg.team_limit:
            break

    games_df = pd.DataFrame(game_rows)
    validation_df = pd.DataFrame(validation_rows)

    if not games_df.empty:
        games_df = games_df.sort_values(
            ["season_end_year", "source_team_abbr", "date", "game_number"]
        ).reset_index(drop=True)

    return games_df, validation_df


def fetch_many_seasons(cfg: ScrapeConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    current_teams_df = get_current_nba_teams()
    name_lookup, abbr_lookup = build_alias_lookups()
    backend = resolve_fetch_backend(cfg.fetch_backend, cfg.chrome_executable_path)
    print(f"Using Basketball-Reference fetch backend: {backend}")
    if backend == "playwright":
        browser_path = resolve_browser_executable_path(cfg.chrome_executable_path)
        if browser_path:
            print(f"Using browser executable: {browser_path}")
        else:
            print("Using Playwright-managed browser")

    all_games: List[pd.DataFrame] = []
    all_validation: List[pd.DataFrame] = []

    with create_fetcher(backend, cfg.chrome_executable_path) as fetcher:
        for year in range(cfg.start_year, cfg.end_year + 1):
            try:
                games_df, validation_df = fetch_season_records(
                    year,
                    cfg=cfg,
                    current_teams_df=current_teams_df,
                    name_lookup=name_lookup,
                    abbr_lookup=abbr_lookup,
                    fetcher=fetcher,
                )
                all_games.append(games_df)
                all_validation.append(validation_df)
            except Exception as exc:
                print(f"[error] Failed season {year} ({season_label(year)}): {exc}")

            time.sleep(cfg.sleep_between_seasons)

    games = pd.concat(all_games, ignore_index=True) if all_games else pd.DataFrame()
    validation = pd.concat(all_validation, ignore_index=True) if all_validation else pd.DataFrame()
    return games, validation


def add_extra_checks(games: pd.DataFrame) -> pd.DataFrame:
    if games.empty:
        return games

    out = games.copy()
    out["wins_after_game"] = pd.to_numeric(out["wins_after_game"], errors="coerce")
    out["losses_after_game"] = pd.to_numeric(out["losses_after_game"], errors="coerce")
    out["game_number"] = pd.to_numeric(out["game_number"], errors="coerce")
    out["computed_games_after_game"] = out["wins_after_game"] + out["losses_after_game"]
    out["game_number_matches_record"] = out["computed_games_after_game"] == out["game_number"]
    out["win_pct_after_game"] = out["wins_after_game"] / (
        out["wins_after_game"] + out["losses_after_game"]
    )
    return out


def add_season_numbers(df: pd.DataFrame) -> pd.DataFrame:
    ordered = (
        df.groupby("SEASON_ID")["GAME_DATE"]
        .min()
        .sort_values()
        .index.astype(str)
        .tolist()
    )
    season_to_num = {season_id: idx + 1 for idx, season_id in enumerate(ordered)}
    out = df.copy()
    out["season_num"] = out["SEASON_ID"].astype(str).map(season_to_num)
    return out


def build_normalized_dataset(games: pd.DataFrame) -> pd.DataFrame:
    if games.empty:
        return pd.DataFrame()

    mapped = games[games["entity_abbreviation"].notna()].copy()
    if mapped.empty:
        return pd.DataFrame()

    mapped["GAME_DATE"] = pd.to_datetime(mapped["date"], errors="coerce")
    mapped = mapped[mapped["result"].isin(["W", "L"])].copy()
    mapped["GAME_ID"] = mapped["game_id"].astype(str)
    mapped["SEASON_ID"] = mapped["season_id"].astype(str)
    mapped["TEAM_ID"] = mapped["team_id"].astype("Int64")
    mapped["TEAM_ABBREVIATION"] = mapped["entity_abbreviation"].astype(str)
    mapped["TEAM_NAME"] = mapped["entity_name"].astype(str)
    mapped["MATCHUP"] = mapped["matchup"].astype(str)
    mapped["WL"] = mapped["result"].astype(str)
    mapped["PTS"] = pd.to_numeric(mapped["points_scored"], errors="coerce")
    mapped["MIN"] = pd.NA
    mapped["PLUS_MINUS"] = pd.NA
    mapped["is_win"] = (mapped["WL"] == "W").astype(int)
    mapped["is_loss"] = (mapped["WL"] == "L").astype(int)

    mapped = mapped.sort_values(
        ["entity_abbreviation", "GAME_DATE", "GAME_ID", "source_team_abbr"]
    ).reset_index(drop=True)

    frames: List[pd.DataFrame] = []
    for _, entity_df in mapped.groupby("entity_abbreviation", sort=True):
        entity_df = entity_df.copy()
        entity_df = add_season_numbers(entity_df)
        entity_df["game_num_overall"] = range(1, len(entity_df) + 1)
        entity_df["cum_wins"] = entity_df["is_win"].cumsum()
        entity_df["cum_losses"] = entity_df["is_loss"].cumsum()
        entity_df["win_pct"] = entity_df["cum_wins"] / (
            entity_df["cum_wins"] + entity_df["cum_losses"]
        )
        entity_df["entity_id"] = entity_df["franchise_id"].astype("Int64")
        entity_df["mode"] = MODE_NAME
        entity_df["start_mode"] = START_MODE
        entity_df["season_start_year"] = pd.to_numeric(
            entity_df["season_start_year"],
            errors="coerce",
        ).astype("Int64")
        entity_df["season_label"] = entity_df["season"].astype(str)
        frames.append(entity_df)

    dataset = pd.concat(frames, ignore_index=True)
    dataset = dataset.sort_values(
        ["entity_abbreviation", "GAME_DATE", "GAME_ID"]
    ).reset_index(drop=True)

    preferred_columns = [
        "data_source",
        "SEASON_ID",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "TEAM_NAME",
        "GAME_ID",
        "GAME_DATE",
        "MATCHUP",
        "WL",
        "MIN",
        "PTS",
        "PLUS_MINUS",
        "season_num",
        "is_win",
        "is_loss",
        "game_num_overall",
        "cum_wins",
        "cum_losses",
        "win_pct",
        "entity_id",
        "entity_abbreviation",
        "entity_name",
        "team_id",
        "franchise_id",
        "primary_color",
        "secondary_color",
        "logo_url",
        "mode",
        "start_mode",
        "season_start_year",
        "season_label",
        "source_team_abbr",
        "source_team_name",
        "opponent_abbr",
        "opponent_name",
        "location",
        "points_allowed",
        "wins_after_game",
        "losses_after_game",
        "streak_after_game",
        "win_pct_after_game",
        "game_number",
        "computed_games_after_game",
        "game_number_matches_record",
        "league",
    ]
    existing_columns = [column for column in preferred_columns if column in dataset.columns]
    return dataset[existing_columns].copy()


def output_stem(cfg: ScrapeConfig) -> str:
    scope = "active_franchises" if cfg.active_franchises_only else "all_teams"
    return f"bref_{cfg.start_year}_{cfg.end_year}_{scope}"


def raw_games_output_path(cfg: Optional[ScrapeConfig] = None) -> str:
    config = cfg or ScrapeConfig()
    return os.path.join(OUTPUT_DIR, f"{output_stem(config)}_team_game_records.csv")


def validation_output_path(cfg: Optional[ScrapeConfig] = None) -> str:
    config = cfg or ScrapeConfig()
    return os.path.join(OUTPUT_DIR, f"{output_stem(config)}_team_season_validation.csv")


def dataset_output_path(cfg: Optional[ScrapeConfig] = None) -> str:
    config = cfg or ScrapeConfig()
    return os.path.join(
        OUTPUT_DIR,
        f"{output_stem(config)}_league_winpct_{MODE_NAME}_{START_MODE}.parquet",
    )


def build_and_save_outputs(
    cfg: Optional[ScrapeConfig] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    config = cfg or ScrapeConfig()
    games, validation = fetch_many_seasons(config)
    games = add_extra_checks(games)
    dataset = build_normalized_dataset(games)

    games.to_csv(raw_games_output_path(config), index=False)
    validation.to_csv(validation_output_path(config), index=False)
    dataset.to_parquet(dataset_output_path(config), index=False)
    return games, validation, dataset


def parse_args() -> ScrapeConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape Basketball-Reference directly and build an app-compatible "
            "historical franchise dataset."
        )
    )
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    parser.add_argument("--include-aba", action="store_true", default=INCLUDE_ABA)
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Keep historical teams that do not map to a current active NBA franchise.",
    )
    parser.add_argument("--sleep-between-teams", type=float, default=SLEEP_BETWEEN_TEAMS)
    parser.add_argument("--sleep-between-seasons", type=float, default=SLEEP_BETWEEN_SEASONS)
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    parser.add_argument("--request-timeout-s", type=int, default=REQUEST_TIMEOUT_S)
    parser.add_argument("--max-rate-limit-sleep-s", type=int, default=MAX_RATE_LIMIT_SLEEP_S)
    parser.add_argument(
        "--fetch-backend",
        choices=["auto", "requests", "playwright"],
        default=DEFAULT_FETCH_BACKEND,
    )
    parser.add_argument(
        "--chrome-executable-path",
        default=DEFAULT_CHROME_PATH,
        help="Chrome binary path used for the Playwright backend.",
    )
    parser.add_argument("--team-limit", type=int, default=None)
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()

    return ScrapeConfig(
        start_year=args.start_year,
        end_year=args.end_year,
        include_aba=args.include_aba,
        active_franchises_only=not args.include_inactive,
        sleep_between_teams=args.sleep_between_teams,
        sleep_between_seasons=args.sleep_between_seasons,
        max_retries=args.max_retries,
        request_timeout_s=args.request_timeout_s,
        max_rate_limit_sleep_s=args.max_rate_limit_sleep_s,
        fetch_backend=args.fetch_backend,
        chrome_executable_path=args.chrome_executable_path,
        team_limit=args.team_limit,
        force_refresh=args.force_refresh,
    )


def main() -> None:
    cfg = parse_args()
    games, validation, dataset = build_and_save_outputs(cfg)
    raw_games_out = raw_games_output_path(cfg)
    validation_out = validation_output_path(cfg)
    dataset_out = dataset_output_path(cfg)

    print("\nDone.")
    print(f"Saved raw team-game rows: {raw_games_out}")
    print(f"Saved validation table: {validation_out}")
    print(f"Saved normalized dataset: {dataset_out}")
    print("The existing nba_api dataset remains separate.")

    if not validation.empty:
        bad = validation.loc[validation["record_matches_summary"] == False]
        print(f"\nValidation rows: {len(validation)}")
        print(f"Mismatches: {len(bad)}")
        if len(bad):
            print("\nFirst mismatches:")
            print(bad.head(20).to_string(index=False))

    if not dataset.empty:
        latest = (
            dataset.sort_values(["entity_abbreviation", "GAME_DATE", "GAME_ID"])
            .groupby("entity_abbreviation", as_index=False)
            .tail(1)
            .copy()
        )
        latest["record"] = (
            latest["cum_wins"].astype(int).astype(str)
            + "-"
            + latest["cum_losses"].astype(int).astype(str)
        )
        print("\nLatest franchise snapshots:")
        print(
            latest[
                [
                    "entity_abbreviation",
                    "entity_name",
                    "season_label",
                    "record",
                    "win_pct",
                    "GAME_DATE",
                ]
            ]
            .sort_values(["win_pct", "entity_abbreviation"], ascending=[False, True])
            .head(10)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
