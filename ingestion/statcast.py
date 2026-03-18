"""
Ingestion script: pulls Statcast pitch-level data via pybaseball and loads into DuckDB.
"""

import duckdb
import pybaseball
from datetime import date

DB_PATH = "data/mlb.db"


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS statcast_pitches (
            pitch_date DATE,
            player_name VARCHAR,
            batter INTEGER,
            pitcher INTEGER,
            events VARCHAR,
            description VARCHAR,
            pitch_type VARCHAR,
            release_speed DOUBLE,
            release_spin_rate DOUBLE,
            launch_speed DOUBLE,
            launch_angle DOUBLE,
            estimated_ba_using_speedangle DOUBLE,
            estimated_woba_using_speedangle DOUBLE,
            woba_value DOUBLE,
            home_team VARCHAR,
            away_team VARCHAR,
            inning INTEGER,
            inning_topbot VARCHAR,
            balls INTEGER,
            strikes INTEGER,
            outs_when_up INTEGER,
            stand VARCHAR,
            p_throws VARCHAR,
            game_date VARCHAR,
            game_pk BIGINT
        )
    """)


def ingest_statcast(start_date: str, end_date: str) -> None:
    print(f"Fetching Statcast data: {start_date} → {end_date}")
    pybaseball.cache.enable()
    df = pybaseball.statcast(start_dt=start_date, end_dt=end_date)

    if df.empty:
        print("No data returned.")
        return

    # Keep only columns we care about
    columns = [
        "game_date", "player_name", "batter", "pitcher", "events", "description",
        "pitch_type", "release_speed", "release_spin_rate", "launch_speed",
        "launch_angle", "estimated_ba_using_speedangle", "estimated_woba_using_speedangle",
        "woba_value", "home_team", "away_team", "inning", "inning_topbot",
        "balls", "strikes", "outs_when_up", "stand", "p_throws", "game_pk"
    ]
    df = df[[c for c in columns if c in df.columns]].copy()
    df = df.rename(columns={"game_date": "pitch_date"})

    with duckdb.connect(DB_PATH) as conn:
        init_db(conn)
        conn.execute("DELETE FROM statcast_pitches WHERE pitch_date BETWEEN ? AND ?", [start_date, end_date])
        conn.execute("INSERT INTO statcast_pitches SELECT * FROM df")
        count = conn.execute("SELECT COUNT(*) FROM statcast_pitches WHERE pitch_date BETWEEN ? AND ?", [start_date, end_date]).fetchone()[0]
        print(f"Loaded {count:,} pitches into DuckDB.")


if __name__ == "__main__":
    # Example: pull one week of the 2024 season
    ingest_statcast("2024-04-01", "2024-04-07")
