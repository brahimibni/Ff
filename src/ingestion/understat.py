import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import html
import json
import time
from difflib import SequenceMatcher

UNDERSTAT_COLUMNS = [
    "id", "player_name", "games", "time", "goals", "xG",
    "assists", "xA", "shots", "key_passes", "yellow_cards",
    "red_cards", "position", "team_title", "npg", "npxG",
    "xGChain", "xGBuildup"
]

async def _fetch_understat():
    from understat import Understat
    # The understat library handles its own aiohttp session if not provided
    async with Understat() as u:
        players = await u.get_league_players("EPL", 2024)
        return pd.DataFrame(players)

def fetch_understat_data():
    """Fetch xG and xA for Premier League players from Understat using the understat library."""
    try:
        import asyncio
        df = asyncio.run(_fetch_understat())
        if df.empty:
            raise ValueError("Fetched DataFrame is empty")
        
        # Ensure numeric conversion for key columns
        for col in ["xG", "xA", "npxG", "xGChain", "xGBuildup", "games"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df
    except Exception as e:
        print(f"[understat] Error fetching data: {e}")
        print("[understat] WARNING: Returning empty DataFrame with schema. XG data skipped.")
        return pd.DataFrame(columns=UNDERSTAT_COLUMNS)

def match_understat_to_fpl(understat_df, fpl_players_df):
    matches = []
    # If understat_df is returning the full schema but no rows, iterrows() is safe.
    # If it's the "bare" empty df from before, iterrows() is also safe but matches will be empty.
    for _, under_row in understat_df.iterrows():
        under_name = under_row.get("player_name", "")
        if not under_name:
            continue
            
        best_match = None
        best_score = 0.6
        for _, fpl_row in fpl_players_df.iterrows():
            fpl_name = fpl_row["name"]
            score = SequenceMatcher(None, str(under_name).lower(), fpl_name.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = fpl_row["id"]
        if best_match:
            matches.append({
                "player_id": best_match,
                "xG": under_row.get("xG", 0),
                "xA": under_row.get("xA", 0),
                "games_understat": under_row.get("games", 0)
            })
    return pd.DataFrame(matches, columns=["player_id", "xG", "xA", "games_understat"])