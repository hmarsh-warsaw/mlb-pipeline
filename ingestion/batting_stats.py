"""
Ingestion script: pulls season-level batting stats from Fangraphs via pybaseball.
"""

import duckdb
import pybaseball

DB_PATH = "data/mlb.db"


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS batting_stats (
            season INTEGER,
            player_name VARCHAR,
            team VARCHAR,
            g INTEGER,
            ab INTEGER,
            pa INTEGER,
            h INTEGER,
            doubles INTEGER,
            triples INTEGER,
            hr INTEGER,
            r INTEGER,
            rbi INTEGER,
            bb INTEGER,
            so INTEGER,
            sb INTEGER,
            cs INTEGER,
            avg DOUBLE,
            obp DOUBLE,
            slg DOUBLE,
            ops DOUBLE,
            woba DOUBLE,
            wrc_plus DOUBLE,
            war DOUBLE,
            playerid VARCHAR
        )
    """)


def ingest_batting_stats(start_season: int, end_season: int) -> None:
    print(f"Fetching Fangraphs batting stats: {start_season}–{end_season}")
    df = pybaseball.batting_stats(start_season, end_season, qual=50)

    if df.empty:
        print("No data returned.")
        return

    column_map = {
        "Season": "season", "Name": "player_name", "Team": "team",
        "G": "g", "AB": "ab", "PA": "pa", "H": "h", "2B": "doubles",
        "3B": "triples", "HR": "hr", "R": "r", "RBI": "rbi", "BB": "bb",
        "SO": "so", "SB": "sb", "CS": "cs", "AVG": "avg", "OBP": "obp",
        "SLG": "slg", "OPS": "ops", "wOBA": "woba", "wRC+": "wrc_plus",
        "WAR": "war", "IDfg": "playerid"
    }
    df = df.rename(columns=column_map)[[c for c in column_map.values() if c in [column_map[k] for k in column_map if k in df.columns]]]

    with duckdb.connect(DB_PATH) as conn:
        init_db(conn)
        conn.execute("DELETE FROM batting_stats WHERE season BETWEEN ? AND ?", [start_season, end_season])
        conn.execute("INSERT INTO batting_stats SELECT * FROM df")
        count = conn.execute("SELECT COUNT(*) FROM batting_stats WHERE season BETWEEN ? AND ?", [start_season, end_season]).fetchone()[0]
        print(f"Loaded {count:,} player-seasons into DuckDB.")


if __name__ == "__main__":
    ingest_batting_stats(2020, 2024)
