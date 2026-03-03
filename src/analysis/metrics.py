import pandas as pd

def compute_ppm(total_points, games_played):
    return total_points / games_played if games_played else 0

def compute_form(player_id, conn, last_n=5):
    query = f"""
        SELECT total_points FROM player_history
        WHERE player_id = {player_id}
        ORDER BY gameweek DESC
        LIMIT {last_n}
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return 0
    return df["total_points"].mean()

def compute_minutes_stability(player_id, conn):
    query = f"""
        SELECT minutes FROM player_history
        WHERE player_id = {player_id}
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return 0
    total_minutes = df["minutes"].sum()
    games_played = len(df)
    possible_minutes = games_played * 90
    return total_minutes / possible_minutes if possible_minutes else 0

def compute_all_metrics(conn):
    players = pd.read_sql("SELECT id, total_points FROM players", conn)
    metrics = []
    for _, row in players.iterrows():
        pid = row["id"]
        form = compute_form(pid, conn)
        stability = compute_minutes_stability(pid, conn)
        games_played = pd.read_sql(f"SELECT COUNT(*) FROM player_history WHERE player_id = {pid}", conn).iloc[0,0]
        ppm = compute_ppm(row["total_points"], games_played)
        metrics.append({
            "player_id": pid,
            "ppm": ppm,
            "form": form,
            "minutes_stability": stability
        })
    return pd.DataFrame(metrics)