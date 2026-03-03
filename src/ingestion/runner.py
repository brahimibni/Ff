import os
import time
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import requests
from datetime import datetime

# Local imports
from src.ingestion.understat import fetch_understat_data, match_understat_to_fpl
from src.analysis.metrics import compute_all_metrics
from src.analysis.recommendations import generate_recommendations

DATABASE_URL = os.getenv("DATABASE_URL")
FPL_API_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

def fetch_fpl_data():
    response = requests.get(FPL_API_URL)
    response.raise_for_status()
    return response.json()

def upsert_players(conn, players):
    cursor = conn.cursor()
    data = []
    for p in players:
        data.append((
            p['id'], p['web_name'], p['team'], p['element_type'],
            p['now_cost'], p['total_points'], p['points_per_game'],
            p['selected_by_percent'], p['form'], p['goals_scored'],
            p['assists'], p['clean_sheets']
        ))
    insert_sql = """
        INSERT INTO players (
            id, name, team_id, position, now_cost, total_points,
            points_per_game, selected_by_percent, form, goals_scored,
            assists, clean_sheets, updated_at
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            team_id = EXCLUDED.team_id,
            position = EXCLUDED.position,
            now_cost = EXCLUDED.now_cost,
            total_points = EXCLUDED.total_points,
            points_per_game = EXCLUDED.points_per_game,
            selected_by_percent = EXCLUDED.selected_by_percent,
            form = EXCLUDED.form,
            goals_scored = EXCLUDED.goals_scored,
            assists = EXCLUDED.assists,
            clean_sheets = EXCLUDED.clean_sheets,
            updated_at = CURRENT_TIMESTAMP;
    """
    execute_values(cursor, insert_sql, data)
    conn.commit()
    cursor.close()

def upsert_teams(conn, teams):
    cursor = conn.cursor()
    data = [(t['id'], t['name'], t['short_name'], t['strength']) for t in teams]
    insert_sql = """
        INSERT INTO teams (id, name, short_name, strength, updated_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            short_name = EXCLUDED.short_name,
            strength = EXCLUDED.strength,
            updated_at = CURRENT_TIMESTAMP;
    """
    execute_values(cursor, insert_sql, data)
    conn.commit()
    cursor.close()

def upsert_gameweeks(conn, events):
    cursor = conn.cursor()
    data = [(e['id'], e['name'], e['deadline_time'], e['finished']) for e in events]
    insert_sql = """
        INSERT INTO gameweeks (id, name, deadline_time, finished, updated_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            deadline_time = EXCLUDED.deadline_time,
            finished = EXCLUDED.finished,
            updated_at = CURRENT_TIMESTAMP;
    """
    execute_values(cursor, insert_sql, data)
    conn.commit()
    cursor.close()
    print("Gameweeks updated.")

def fetch_player_history(player_id):
    url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("history", [])

def update_player_history(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM players")
    player_ids = [row[0] for row in cursor.fetchall()]

    for pid in player_ids:
        try:
            history = fetch_player_history(pid)
            for gw in history:
                cursor.execute("""
                    INSERT INTO player_history (
                        player_id, gameweek, minutes, goals_scored, assists,
                        clean_sheets, goals_conceded, own_goals, penalties_saved,
                        penalties_missed, yellow_cards, red_cards, saves, bonus,
                        bps, influence, creativity, threat, ict_index, total_points,
                        in_dreamteam
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (player_id, gameweek) DO UPDATE SET
                        minutes = EXCLUDED.minutes,
                        goals_scored = EXCLUDED.goals_scored,
                        assists = EXCLUDED.assists,
                        clean_sheets = EXCLUDED.clean_sheets,
                        goals_conceded = EXCLUDED.goals_conceded,
                        own_goals = EXCLUDED.own_goals,
                        penalties_saved = EXCLUDED.penalties_saved,
                        penalties_missed = EXCLUDED.penalties_missed,
                        yellow_cards = EXCLUDED.yellow_cards,
                        red_cards = EXCLUDED.red_cards,
                        saves = EXCLUDED.saves,
                        bonus = EXCLUDED.bonus,
                        bps = EXCLUDED.bps,
                        influence = EXCLUDED.influence,
                        creativity = EXCLUDED.creativity,
                        threat = EXCLUDED.threat,
                        ict_index = EXCLUDED.ict_index,
                        total_points = EXCLUDED.total_points,
                        in_dreamteam = EXCLUDED.in_dreamteam,
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    pid, gw["round"], gw["minutes"], gw["goals_scored"],
                    gw["assists"], gw["clean_sheets"], gw["goals_conceded"],
                    gw["own_goals"], gw["penalties_saved"], gw["penalties_missed"],
                    gw["yellow_cards"], gw["red_cards"], gw["saves"], gw["bonus"],
                    gw["bps"], gw["influence"], gw["creativity"], gw["threat"],
                    gw["ict_index"], gw["total_points"], gw["in_dreamteam"]
                ))
            conn.commit()
            print(f"Updated history for player {pid}")
            time.sleep(0.5)  # be gentle to API
        except Exception as e:
            print(f"Error updating history for player {pid}: {e}")
            conn.rollback()
    cursor.close()

def fetch_and_store_fixtures(conn):
    url = "https://fantasy.premierleague.com/api/fixtures/"
    response = requests.get(url)
    response.raise_for_status()
    fixtures = response.json()
    cursor = conn.cursor()
    for f in fixtures:
        cursor.execute("""
            INSERT INTO fixtures (id, event, team_h, team_a, team_h_difficulty,
                                  team_a_difficulty, finished, kickoff_time, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET
                event = EXCLUDED.event,
                team_h = EXCLUDED.team_h,
                team_a = EXCLUDED.team_a,
                team_h_difficulty = EXCLUDED.team_h_difficulty,
                team_a_difficulty = EXCLUDED.team_a_difficulty,
                finished = EXCLUDED.finished,
                kickoff_time = EXCLUDED.kickoff_time,
                updated_at = CURRENT_TIMESTAMP;
        """, (
            f["id"], f.get("event"), f["team_h"], f["team_a"],
            f["team_h_difficulty"], f["team_a_difficulty"],
            f["finished"], f["kickoff_time"]
        ))
    conn.commit()
    cursor.close()
    print("Fixtures updated.")

def update_xg_data(conn):
    players_df = pd.read_sql("SELECT id, name FROM players", conn)
    understat_df = fetch_understat_data()
    xg_df = match_understat_to_fpl(understat_df, players_df)
    cursor = conn.cursor()
    for _, row in xg_df.iterrows():
        cursor.execute("""
            INSERT INTO player_xg (player_id, xG, xA, games_understat, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (player_id) DO UPDATE SET
                xG = EXCLUDED.xG,
                xA = EXCLUDED.xA,
                games_understat = EXCLUDED.games_understat,
                updated_at = CURRENT_TIMESTAMP;
        """, (row["player_id"], row["xG"], row["xA"], row["games_understat"]))
    conn.commit()
    cursor.close()
    print("xG/xA data updated.")

def compute_team_fdr(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(event) FROM fixtures WHERE finished = true")
    current_gw = cursor.fetchone()[0] or 0
    next_gws = list(range(current_gw+1, current_gw+6))
    teams = pd.read_sql("SELECT id FROM teams", conn)
    ratings = []
    for _, row in teams.iterrows():
        team_id = row["id"]
        query = f"""
            SELECT team_h_difficulty as diff FROM fixtures
            WHERE event IN ({','.join(map(str, next_gws))}) AND team_h = {team_id}
            UNION ALL
            SELECT team_a_difficulty as diff FROM fixtures
            WHERE event IN ({','.join(map(str, next_gws))}) AND team_a = {team_id}
        """
        diffs = pd.read_sql(query, conn)["diff"].tolist()
        avg_fdr = sum(diffs) / len(diffs) if diffs else 3.0
        ratings.append((team_id, avg_fdr))
    for team_id, fdr in ratings:
        cursor.execute("""
            INSERT INTO team_fixture_ratings (team_id, next_5_fdr, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (team_id) DO UPDATE SET
                next_5_fdr = EXCLUDED.next_5_fdr,
                updated_at = CURRENT_TIMESTAMP;
        """, (team_id, fdr))
    conn.commit()
    cursor.close()
    print("Team FDR updated.")

def store_metrics(conn):
    metrics_df = compute_all_metrics(conn)
    cursor = conn.cursor()
    for _, row in metrics_df.iterrows():
        cursor.execute("""
            INSERT INTO player_metrics (player_id, ppm, form, minutes_stability, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (player_id) DO UPDATE SET
                ppm = EXCLUDED.ppm,
                form = EXCLUDED.form,
                minutes_stability = EXCLUDED.minutes_stability,
                updated_at = CURRENT_TIMESTAMP;
        """, (row["player_id"], row["ppm"], row["form"], row["minutes_stability"]))
    conn.commit()
    cursor.close()
    print("Player metrics stored.")

def store_recommendations(conn):
    recs_df = generate_recommendations(conn)
    cursor = conn.cursor()
    for _, row in recs_df.iterrows():
        cursor.execute("""
            INSERT INTO recommendations (player_id, recommendation, confidence, reasons, fdr_next_5, updated_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (player_id) DO UPDATE SET
                recommendation = EXCLUDED.recommendation,
                confidence = EXCLUDED.confidence,
                reasons = EXCLUDED.reasons,
                fdr_next_5 = EXCLUDED.fdr_next_5,
                updated_at = CURRENT_TIMESTAMP;
        """, (row["player_id"], row["recommendation"], row["confidence"], row["reasons"], row["fdr_next_5"]))
    conn.commit()
    cursor.close()
    print("Recommendations stored.")

def main():
    print(f"{datetime.now()} - Starting FPL data ingestion")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        # 1. Bootstrap data
        data = fetch_fpl_data()
        upsert_players(conn, data['elements'])
        upsert_teams(conn, data['teams'])
        upsert_gameweeks(conn, data['events'])
        print("Bootstrap data updated.")

        # 2. Player history (gameweek data)
        update_player_history(conn)
        print("Player history updated.")

        # 3. Fixtures
        fetch_and_store_fixtures(conn)
        print("Fixtures updated.")

        # 4. xG/xA from Understat
        update_xg_data(conn)
        print("xG/xA data updated.")

        # 5. Compute team fixture difficulty ratings
        compute_team_fdr(conn)
        print("Team FDR updated.")

        # 6. Compute player metrics
        store_metrics(conn)
        print("Player metrics stored.")

        # 7. Generate and store recommendations
        store_recommendations(conn)
        print("Recommendations stored.")

        print(f"{datetime.now()} - Ingestion completed successfully.")
    except Exception as e:
        print(f"{datetime.now()} - Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()