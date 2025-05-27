import os
import pandas as pd
from typing import Optional, Dict
from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats
from sqlalchemy import text
from tqdm import tqdm
from requests.exceptions import ReadTimeout

# emulate a browser UA so the NBA servers don’t throttle/block us
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )
}

# List of target players
TARGET_PLAYERS = [
    "LeBron James","Stephen Curry","Kevin Durant","Giannis Antetokounmpo",
    "Kawhi Leonard","James Harden","Nikola Jokic","Luka Doncic",
    "Damian Lillard","Joel Embiid","Anthony Davis","Kyrie Irving",
    "Chris Paul","Russell Westbrook","Paul George","Klay Thompson",
    "Jimmy Butler","Draymond Green","Devin Booker","Jayson Tatum",
    "Ben Simmons","Karl-Anthony Towns","Bradley Beal","DeMar DeRozan",
    "Zach LaVine","Ja Morant","Trae Young","Donovan Mitchell",
    "Jaylen Brown","Shai Gilgeous-Alexander"
]

def fetch_player_stats(name: str) -> Optional[Dict]:
    """Fetch profile + latest season stats for a given player name."""
    matches = players.find_players_by_full_name(name)
    if not matches:
        print(f"⚠️ Player '{name}' not found.")
        return None

    pid = matches[0]["id"]

    # 1) Retry profile fetch up to 3 times, with a longer timeout
    profile = None
    for attempt in range(1, 4):
        try:
            df = commonplayerinfo.CommonPlayerInfo(
                player_id=pid,
                timeout=60,       # 60 s socket timeout
                headers=HEADERS
            ).get_data_frames()[0]
            profile = df.iloc[0][[
                "DISPLAY_FIRST_LAST",
                "TEAM_ABBREVIATION",
                "POSITION",
                "HEIGHT",
                "WEIGHT"
            ]].to_dict()
            break
        except ReadTimeout:
            print(f"⏳ Timeout fetching profile for {name}, retry {attempt}/3")
        except Exception as e:
            print(f"⚠️ Failed to fetch profile for {name}: {e}")
            return None
    if profile is None:
        print(f"⚠️ Could not fetch profile for {name} after 3 tries.")
        return None

    # 2) Retry career stats up to 5 times
    season_stats = {}
    for attempt in range(1, 6):
        try:
            df = playercareerstats.PlayerCareerStats(
                player_id=pid,
                timeout=60,
                headers=HEADERS
            ).get_data_frames()[0]
            latest = df.iloc[-1]
            season_stats = latest[[
                "SEASON_ID", "GP", "PTS", "REB", "AST",
                "FG_PCT", "FG3_PCT", "FT_PCT"
            ]].to_dict()
            # convert to proper percentages
            season_stats["FG_PCT"]  *= 100
            season_stats["FG3_PCT"] *= 100
            season_stats["FT_PCT"]  *= 100
            break
        except ReadTimeout:
            print(f"⏳ Timeout fetching stats for {name}, retry {attempt}/5")
        except Exception as e:
            print(f"⚠️ Failed to fetch stats for {name}: {e}")
            return None
    else:
        print(f"⚠️ Could not fetch stats for {name} after 5 tries.")
        return None

    return {**profile, **season_stats}

def fetch_and_load(engine):
    """Main entrypoint: fetch all player stats and load into Redshift."""
    player_data = []
    for name in tqdm(TARGET_PLAYERS, desc="Fetching NBA stats"):
        data = fetch_player_stats(name)
        if data:
            player_data.append(data)

    if not player_data:
        print("⚠️ No data fetched, exiting.")
        return

    df = pd.DataFrame(player_data)
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM nba_player_stats"))
        df.to_sql(
            name="nba_player_stats",
            con=engine,
            index=False,
            if_exists="append",
            method="multi"
        )
    print(f"✅ Loaded {len(df)} records into Redshift")
