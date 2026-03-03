import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from difflib import SequenceMatcher

def fetch_understat_data():
    """Fetch xG and xA for Premier League players from Understat."""
    url = "https://understat.com/league/EPL"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    scripts = soup.find_all("script")
    for script in scripts:
        if "playersData" in script.text:
            json_text = re.search(r"playersData\s*=\s*(\[.*?\]);", script.text, re.DOTALL)
            if json_text:
                import json
                data = json.loads(json_text.group(1))
                df = pd.DataFrame(data)
                df = df[["player_name", "xG", "xA", "games"]]
                df["xG"] = pd.to_numeric(df["xG"], errors="coerce")
                df["xA"] = pd.to_numeric(df["xA"], errors="coerce")
                df["games"] = pd.to_numeric(df["games"], errors="coerce")
                return df
    raise Exception("Could not find player data on Understat page.")

def match_understat_to_fpl(understat_df, fpl_players_df):
    matches = []
    for _, under_row in understat_df.iterrows():
        under_name = under_row["player_name"]
        best_match = None
        best_score = 0.6
        for _, fpl_row in fpl_players_df.iterrows():
            fpl_name = fpl_row["name"]
            score = SequenceMatcher(None, under_name.lower(), fpl_name.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = fpl_row["id"]
        if best_match:
            matches.append({
                "player_id": best_match,
                "xG": under_row["xG"],
                "xA": under_row["xA"],
                "games_understat": under_row["games"]
            })
    return pd.DataFrame(matches)