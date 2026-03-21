# NBA cumulative win% explorer

This project can build regular-season cumulative win% histories from two separate sources:

- `stats.nba.com` via `nba_api`
- Basketball-Reference schedule pages

It exposes the results in two ways:

- reusable cached data pipelines for all 30 active teams
- a Streamlit app with all teams selected by default, team colors, and NBA logo assets

## What it does

- Pulls full regular-season histories for every active NBA team
- Caches raw team histories to `./nba_team_games_cache/*.parquet`
- Caches Basketball-Reference HTML pages to `./bref_page_cache/*.html`
- Builds source-specific long-form league datasets under `./outputs/`
- Defaults to `game1` so the full franchise history is visible immediately
- Supports franchise continuity mode and current-team mode
- Can force-refresh the raw collection step from either upstream source
- Supports a manual GitHub refresh workflow with zipped pre-refresh backups

## Install

```bash
pip install -r requirements.txt
```

For Basketball-Reference scraping, the Playwright path now works best. By default it will use `BREF_CHROME_PATH` if set, otherwise it will look for a local Chrome or Chromium binary and fall back to the Playwright-managed browser if you explicitly request `--fetch-backend playwright`.

## Build the data

This builds the default `nba_api` dataset and also writes a static all-teams PNG.

```bash
python build_data.py
```

Build one or more sources explicitly:

```bash
python build_data.py --sources nba_api
python build_data.py --sources basketball_reference
python build_data.py --sources nba_api basketball_reference
```

Run the Basketball-Reference scraper directly on a small slice:

```bash
python bball_ref.py --start-year 1947 --end-year 1947 --team-limit 3 --fetch-backend playwright --force-refresh
```

Force a fresh upstream pull for the selected source set:

```bash
python build_data.py --sources nba_api --force-refresh
python build_data.py --sources basketball_reference --force-refresh
```

## Run the Streamlit app

```bash
streamlit run app.py
```

The app:

- defaults to all 30 teams selected
- lets you choose `NBA API` or `Basketball-Reference` as the active dataset source
- lets you remove teams from the sidebar
- supports `Desktop` and `Mobile` view modes, with mobile tabs and a 4-team chart cap for readability
- starts from game 1 by default with no separate start-series toggle
- colors each line with the team primary color
- shows official NBA CDN logos in the team cards
- has a second chronology chart that uses actual dates and season labels on the x-axis
- is read-only in hosted mode; refreshes happen through the manual GitHub workflow or the local refresh script

## Deploy for public viewing

Use Streamlit Community Cloud for the public app. This repo is structured for that flow already:

1. Push this repo to GitHub.
2. In Streamlit Community Cloud, create a new app from `isshamie/NBA-Team-Historical-Win-Percentage`.
3. Set the main file path to `app.py`.
4. Deploy from the `main` branch.

This is a better fit than Vercel because the app is a long-lived Streamlit server that reads committed dataset files from the repo. Vercel's Python model is request/response functions, which is the wrong shape for a stateful Streamlit app and for file-based refresh/backup operations.

## Refresh published data

The intended refresh path is outside the UI:

- GitHub Actions: run the `Refresh Data` workflow manually and choose `nba_api`, `basketball_reference`, or `all`
- Local CLI: run `python scripts/refresh_data.py --sources ...`

Examples:

```bash
python scripts/refresh_data.py --sources nba_api
python scripts/refresh_data.py --sources basketball_reference --mode force-refresh
python scripts/refresh_data.py --sources nba_api basketball_reference --mode rebuild-dataset
```

Each refresh:

- creates a zip backup of the currently published dataset/plot files before rebuilding
- stores the backup under `./backups/`
- keeps only the 2 most recent backup zips
- rewrites the committed `./outputs/` files that the Streamlit app reads
- restores the previous published files automatically if a refresh comes back with worse team coverage than the current baseline

## Run the static plot script

This now defaults to an all-teams plot:

```bash
python nba_winpct_franchise.py
```

Useful flags:

```bash
python nba_winpct_franchise.py --teams NYK ATL
python nba_winpct_franchise.py --team-mode
python nba_winpct_franchise.py --force-refresh
python nba_winpct_franchise.py --rebuild-dataset
```

## Docker

Build:

```bash
docker build -t nba-winpct-franchise .
```

Run the app, prebuilding the default `nba_api` dataset if needed:

```bash
docker run --rm -p 8501:8501 nba-winpct-franchise
```

Run a dataset build job inside Docker:

```bash
docker run --rm nba-winpct-franchise build-datasets --sources nba_api
docker run --rm nba-winpct-franchise build-datasets --sources basketball_reference
docker run --rm nba-winpct-franchise build-datasets --sources nba_api basketball_reference
```

Optional environment variables for `docker run ... nba-winpct-franchise`:

- `BUILD_SOURCES=nba_api`
  Sources to build before the app starts. Use `basketball_reference`, `nba_api basketball_reference`, `all`, or `none`.
- `FORCE_REFRESH=1`
  Force a live upstream refresh before the app starts.
- `REBUILD_DATASET=1`
  Recompute the selected dataset(s) from cached source data before the app starts.
- `SKIP_PLOT=1`
  Skip writing static PNGs during container startup builds.
- `PORT=8501`
  Override the Streamlit port inside the container.

Example:

```bash
docker run --rm -p 8501:8501 -e BUILD_SOURCES="nba_api basketball_reference" nba-winpct-franchise
```

## Outputs

- Raw team cache: `./nba_team_games_cache/*.parquet`
- Basketball-Reference page cache: `./bref_page_cache/*.html`
- Published league datasets: `./outputs/league_winpct_franchise_game1.parquet` and `./outputs/bref_1947_2025_active_franchises_league_winpct_franchise_game1.parquet`
- Published static plots: `./outputs/nba_all_teams_winpct.png` and `./outputs/nba_all_teams_winpct_basketball_reference.png`
- Refresh backups: `./backups/*.zip`

## Notes

- `nba_api` calls `stats.nba.com`, so a full force-refresh depends on live NBA endpoint availability.
- Basketball-Reference can throttle aggressive rebuilds. The new page cache reduces repeated hits, but a force-refresh can still be rate-limited.
- The Playwright backend is the only Basketball-Reference path I have validated end to end; the plain `requests` fallback is still more likely to get throttled.
- The script filters out unfinished games where `WL` is missing, so the cumulative series only reflects completed games.
- The Streamlit app uses franchise continuity mode by default because that is the more useful long-history view for current NBA teams.
