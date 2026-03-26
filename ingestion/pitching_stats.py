"""
Ingestion script: pulls season-level pitching stats from Fangraphs via pybaseball.
"""

import duckdb
import pybaseball

DB_PATH = "data/mlb.db"


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pitching_stats (
            season INTEGER,
            player_name VARCHAR,
            team VARCHAR,
            g INTEGER,
            gs INTEGER,
            ip DOUBLE,
            k_pct DOUBLE,
            bb_pct DOUBLE,
            hr_per_9 DOUBLE,
            babip DOUBLE,
            lob_pct DOUBLE,
            era DOUBLE,
            xfip DOUBLE,
            fip DOUBLE,
            whip DOUBLE,
            h_per_9 DOUBLE,
            war DOUBLE,
            playerid VARCHAR
        )
    """)


def ingest_pitching_stats(start_season: int, end_season: int) -> None:
    print(f"Fetching Fangraphs pitching stats: {start_season}–{end_season}")
    df = pybaseball.pitching_stats(start_season, end_season, qual=30)

    if df.empty:
        print("No data returned.")
        return

    column_map = {
        "Season": "season", "Name": "player_name", "Team": "team",
        "G": "g", "GS": "gs", "IP": "ip",
        "K%": "k_pct", "BB%": "bb_pct", "HR/9": "hr_per_9",
        "BABIP": "babip", "LOB%": "lob_pct",
        "ERA": "era", "xFIP": "xfip", "FIP": "fip",
        "WHIP": "whip", "H/9": "h_per_9",
        "WAR": "war", "IDfg": "playerid"
    }

    df = df.rename(columns=column_map)
    available = [v for k, v in column_map.items() if k in df.columns or v in df.columns]
    df = df[[c for c in column_map.values() if c in df.columns]]

    with duckdb.connect(DB_PATH) as conn:
        init_db(conn)
        conn.execute("DELETE FROM pitching_stats WHERE season BETWEEN ? AND ?", [start_season, end_season])
        conn.execute("INSERT INTO pitching_stats SELECT * FROM df")
        count = conn.execute("SELECT COUNT(*) FROM pitching_stats WHERE season BETWEEN ? AND ?", [start_season, end_season]).fetchone()[0]
        print(f"Loaded {count:,} pitcher-seasons into DuckDB.")


if __name__ == "__main__":
    ingest_pitching_stats(2020, 2024)
