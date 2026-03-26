import os
import requests
import duckdb
import pandas as pd
from datetime import date
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ODDS_API_KEY")
SPORT = "baseball_mlb"
MARKETS = "batter_hits,batter_home_runs,batter_rbis,batter_strikeouts,pitcher_strikeouts"
REGIONS = "us"
ODDS_FORMAT = "american"

def fetch_prop_lines():
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events"
    events_resp = requests.get(url, params={"apiKey": API_KEY})
    events_resp.raise_for_status()
    events = events_resp.json()

    if not events:
        print("No events found.")
        return pd.DataFrame()

    rows = []
    for event in events:
        event_id = event["id"]
        home = event["home_team"]
        away = event["away_team"]
        commence = event["commence_time"]

        props_url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event_id}/odds"
        resp = requests.get(props_url, params={
            "apiKey": API_KEY,
            "regions": REGIONS,
            "markets": MARKETS,
            "oddsFormat": ODDS_FORMAT,
        })
        if resp.status_code != 200:
            print(f"  Skipping {home} vs {away}: {resp.status_code} {resp.text[:100]}")
            continue

        data = resp.json()
        for bookmaker in data.get("bookmakers", []):
            book = bookmaker["key"]
            for market in bookmaker.get("markets", []):
                market_key = market["key"]
                for outcome in market.get("outcomes", []):
                    rows.append({
                        "date_pulled": date.today().isoformat(),
                        "game_date": commence[:10],
                        "home_team": home,
                        "away_team": away,
                        "bookmaker": book,
                        "market": market_key,
                        "player_name": outcome.get("description", ""),
                        "line": outcome.get("point"),
                        "side": outcome["name"],
                        "odds": outcome["price"],
                    })

    return pd.DataFrame(rows)


def load_to_db(df):
    conn = duckdb.connect("data/mlb.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prop_lines (
            date_pulled DATE,
            game_date DATE,
            home_team VARCHAR,
            away_team VARCHAR,
            bookmaker VARCHAR,
            market VARCHAR,
            player_name VARCHAR,
            line DOUBLE,
            side VARCHAR,
            odds INTEGER
        )
    """)
    conn.execute("DELETE FROM prop_lines WHERE date_pulled = ?", [date.today().isoformat()])
    conn.execute("INSERT INTO prop_lines SELECT * FROM df")
    count = conn.execute("SELECT COUNT(*) FROM prop_lines WHERE date_pulled = ?", [date.today().isoformat()]).fetchone()[0]
    print(f"Loaded {count} prop lines for today into DuckDB.")
    conn.close()


if __name__ == "__main__":
    print("Fetching MLB prop lines...")
    df = fetch_prop_lines()
    if not df.empty:
        load_to_db(df)
    else:
        print("No data to load.")
