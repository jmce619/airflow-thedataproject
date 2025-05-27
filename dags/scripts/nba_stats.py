import os
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats
from sqlalchemy import create_engine, text
from tqdm import tqdm
from requests.exceptions import ReadTimeout

# Load your Redshift URL from an environment variable
REDSHIFT_URL = os.getenv("REDSHIFT_URL")
if not REDSHIFT_URL:
    raise RuntimeError("REDSHIFT_URL environment variable is not set")
''
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

def fetch_player_stats(name: str) -> dict | None:
    matches = players.find_players_by_full_name(name)
    if not matches:
        print(f"Player '{name}' not found.")
        return None

    pid = matches[0]["id"]
    try:
        info_df = commonplayerinfo.CommonPlayerInfo(player_id=pid).get_data_frames()[0]
        profile = info_df.iloc[0][[
            "DISPLAY_FIRST_LAST","TEAM_ABBREVIATION","POSITION","HEIGHT","WEIGHT"
        ]].to_dict()
    except Exception as e:
        print(f"Failed to fetch profile for {name}: {e}")
        return None

    season_stats = {}
    for attempt in range(3):
        try:
            career_df = playercareerstats.PlayerCareerStats(player_id=pid).get_data_frames()[0]
            latest = career_df.iloc[-1]
            season_stats = latest[[
                "SEASON_ID","GP","PTS","REB","AST","FG_PCT","FG3_PCT","FT_PCT"
            ]].to_dict()
            # convert to percentages
            season_stats["FG_PCT"]  *= 100
            season_stats["FG3_PCT"] *= 100
            season_stats["FT_PCT"]  *= 100
            break
        except ReadTimeout:
            print(f"Timeout fetching stats for {name}, retry {attempt+1}/3")
        except Exception as e:
            print(f"Failed to fetch stats for {name}: {e}")
            return None

    return {**profile, **season_stats}

def fetch_and_load():
    player_data = []
    for name in tqdm(TARGET_PLAYERS, desc="Fetching NBA stats"):
        data = fetch_player_stats(name)
        if data:
            player_data.append(data)

    if not player_data:
        print(" No data fetched, exiting.")
        return

    df = pd.DataFrame(player_data)
    engine = create_engine(REDSHIFT_URL)
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM nba_player_stats"))
        df.to_sql(
            name="nba_player_stats",
            con=engine,
            index=False,
            if_exists="append",
            method="multi"
        )

    print(f"Loaded {len(df)} records into Redshift")

if __name__ == "__main__":
    fetch_and_load()
