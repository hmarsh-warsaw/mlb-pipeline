"""
Ingestion script: pulls today's probable starting pitchers from the MLB Stats API.
"""

import duckdb
import requests
import pandas as pd
from datetime import date

DB_PATH = "data/mlb.db"
MLB_API = "https://statsapi.mlb.com/api/v1"


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS starting_pitchers (
            game_date DATE,
            game_pk BIGINT,
            home_team VARCHAR,
            away_team VARCHAR,
            side VARCHAR,
            pitcher_name VARCHAR,
            pitcher_mlb_id INTEGER,
            throws VARCHAR
        )
    """)


def get_handedness(pitcher_id: int) -> str:
    resp = requests.get(f"{MLB_API}/people/{pitcher_id}")
    if resp.status_code != 200:
        return "U"
    person = resp.json().get("people", [{}])[0]
    return person.get("pitchHand", {}).get("code", "U")


def ingest_lineups(game_date: str = None) -> None:
    if game_date is None:
        game_date = date.today().isoformat()

    print(f"Fetching probable starters for {game_date}...")
    resp = requests.get(f"{MLB_API}/schedule", params={
        "sportId": 1,
        "date": game_date,
        "hydrate": "probablePitcher"
    })
    resp.raise_for_status()

    games = resp.json().get("dates", [{}])[0].get("games", [])
    if not games:
        print("No games found.")
        return

    rows = []
    for game in games:
        game_pk = game["gamePk"]
        home_team = game["teams"]["home"]["team"]["name"]
        away_team = game["teams"]["away"]["team"]["name"]

        for side in ["home", "away"]:
            pitcher = game["teams"][side].get("probablePitcher")
            if not pitcher:
                continue
            throws = get_handedness(pitcher["id"])
            rows.append({
                "game_date": game_date,
                "game_pk": game_pk,
                "home_team": home_team,
                "away_team": away_team,
                "side": side,
                "pitcher_name": pitcher["fullName"],
                "pitcher_mlb_id": pitcher["id"],
                "throws": throws,
            })

    df = pd.DataFrame(rows)

    with duckdb.connect(DB_PATH) as conn:
        init_db(conn)
        conn.execute("DELETE FROM starting_pitchers WHERE game_date = ?", [game_date])
        conn.execute("INSERT INTO starting_pitchers SELECT * FROM df")
        print(f"Loaded {len(df)} probable starters for {game_date}.")
        print(df[["home_team", "away_team", "side", "pitcher_name", "throws"]].to_string(index=False))


if __name__ == "__main__":
    ingest_lineups()
