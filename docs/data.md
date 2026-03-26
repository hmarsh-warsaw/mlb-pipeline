# Data

## Database

All data is stored in a single DuckDB database at `data/mlb.db`.

---

## Tables

### `batting_stats`
Raw season-level batting stats pulled from Fangraphs via pybaseball.

- **Source:** Fangraphs
- **Ingestion:** `ingestion/batting_stats.py`
- **Grain:** One row per player per season
- **Coverage:** 2020–2024, minimum 50 PA

| Column | Type | Description |
|--------|------|-------------|
| season | INTEGER | MLB season year |
| player_name | VARCHAR | Player full name |
| team | VARCHAR | Team abbreviation |
| g | INTEGER | Games played |
| ab | INTEGER | At bats |
| pa | INTEGER | Plate appearances |
| h | INTEGER | Hits |
| doubles | INTEGER | Doubles |
| triples | INTEGER | Triples |
| hr | INTEGER | Home runs |
| bb | INTEGER | Walks |
| so | INTEGER | Strikeouts |
| sb | INTEGER | Stolen bases |
| avg | DOUBLE | Batting average |
| obp | DOUBLE | On-base percentage |
| slg | DOUBLE | Slugging percentage |
| ops | DOUBLE | OPS |
| woba | DOUBLE | Weighted on-base average |
| wrc_plus | DOUBLE | Weighted runs created+ |
| war | DOUBLE | Wins above replacement |
| playerid | VARCHAR | Fangraphs player ID |

---

### `stg_batting_stats`
Cleaned and filtered version of `batting_stats`. Use this for analysis and modeling.

- **Source:** dbt model at `mlb_transforms/models/batting/stg_batting_stats.sql`
- **Grain:** One row per player per season
- **Filters:** Minimum 50 PA

---

### `pitching_stats`
Raw season-level pitching stats pulled from Fangraphs via pybaseball.

- **Source:** Fangraphs
- **Ingestion:** `ingestion/pitching_stats.py`
- **Grain:** One row per pitcher per season
- **Coverage:** 2020–2024, minimum 30 IP

| Column | Type | Description |
|--------|------|-------------|
| season | INTEGER | MLB season year |
| player_name | VARCHAR | Pitcher full name |
| team | VARCHAR | Team abbreviation |
| g | INTEGER | Games pitched |
| gs | INTEGER | Games started |
| ip | DOUBLE | Innings pitched |
| k_pct | DOUBLE | Strikeout percentage |
| bb_pct | DOUBLE | Walk percentage |
| hr_per_9 | DOUBLE | Home runs per 9 innings |
| babip | DOUBLE | Batting average on balls in play |
| lob_pct | DOUBLE | Left on base percentage |
| era | DOUBLE | Earned run average |
| xfip | DOUBLE | Expected FIP |
| fip | DOUBLE | Fielding independent pitching |
| whip | DOUBLE | Walks + hits per inning pitched |
| h_per_9 | DOUBLE | Hits allowed per 9 innings |
| war | DOUBLE | Wins above replacement |
| playerid | VARCHAR | Fangraphs pitcher ID |

---

### `prop_lines`
Daily prop bet lines pulled from The Odds API. Refreshed each run — always reflects the latest lines for today.

- **Source:** The Odds API
- **Ingestion:** `ingestion/prop_lines.py`
- **Grain:** One row per player per market per side per bookmaker per day
- **Markets:** batter_hits, batter_home_runs, batter_rbis, batter_strikeouts, pitcher_strikeouts

| Column | Type | Description |
|--------|------|-------------|
| date_pulled | DATE | Date lines were pulled |
| game_date | DATE | Game date |
| home_team | VARCHAR | Home team name |
| away_team | VARCHAR | Away team name |
| bookmaker | VARCHAR | Sportsbook key (e.g. draftkings) |
| market | VARCHAR | Prop market type |
| player_name | VARCHAR | Player full name |
| line | DOUBLE | The prop line (e.g. 0.5 hits) |
| side | VARCHAR | Over or Under |
| odds | INTEGER | American odds |

---

### `starting_pitchers`
Today's probable starting pitchers pulled from the MLB Stats API. Refreshed each run.

- **Source:** MLB Stats API (free, no key required)
- **Ingestion:** `ingestion/lineups.py`
- **Grain:** One row per pitcher per game per day

| Column | Type | Description |
|--------|------|-------------|
| game_date | DATE | Game date |
| game_pk | BIGINT | MLB game ID |
| home_team | VARCHAR | Home team name |
| away_team | VARCHAR | Away team name |
| side | VARCHAR | home or away |
| pitcher_name | VARCHAR | Pitcher full name |
| pitcher_mlb_id | INTEGER | MLB player ID |
| throws | VARCHAR | Handedness — L or R |

---

### `statcast_pitches`
Pitch-level Statcast data. Currently empty — ingestion not yet run.

- **Source:** Baseball Savant via pybaseball
- **Ingestion:** `ingestion/statcast.py`
- **Grain:** One row per pitch
