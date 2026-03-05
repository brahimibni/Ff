import pandas as pd

def generate_recommendations(conn):
    players = pd.read_sql("""
        SELECT p.id, p.name, p.position, p.now_cost/10.0 as cost,
               p.total_points, p.form as current_form, p.selected_by_percent,
               p.team_id, t.short_name as team, t.strength
        FROM players p
        JOIN teams t ON p.team_id = t.id
    """, conn)

    metrics = pd.read_sql("SELECT * FROM player_metrics", conn)
    if not metrics.empty and "player_id" in metrics.columns:
        players = players.merge(metrics, left_on="id", right_on="player_id", how="left")
    else:
        print("[recommendations] Warning: metrics table empty or missing player_id. Skipping merge.")
        for col in ["ppm", "form", "minutes_stability"]:
            players[col] = 0

    xg = pd.read_sql("SELECT player_id, xG, xA FROM player_xg", conn)
    if not xg.empty and "player_id" in xg.columns:
        players = players.merge(xg, left_on="id", right_on="player_id", how="left")
    else:
        print("[recommendations] Warning: xG table empty or missing player_id. Skipping merge.")
        for col in ["xG", "xA"]:
            players[col] = 0

    fdr = pd.read_sql("SELECT team_id, next_5_fdr FROM team_fixture_ratings", conn)
    if not fdr.empty and "team_id" in fdr.columns:
        players = players.merge(fdr, left_on="team_id", right_on="team_id", how="left")
    else:
        print("[recommendations] Warning: FDR table empty or missing team_id. Skipping merge.")
        players["next_5_fdr"] = 3.0

    players.fillna({
        "form": 0,
        "ppm": 0,
        "minutes_stability": 0,
        "xG": 0,
        "xA": 0,
        "next_5_fdr": 3.0
    }, inplace=True)

    recommendations = []

    for _, row in players.iterrows():
        reasons = []
        confidence = 0
        rec_type = "HOLD"

        buy_score = 0
        if row["form"] > 6:
            buy_score += 2
            reasons.append("excellent form")
        if row["next_5_fdr"] < 3:
            buy_score += 2
            reasons.append("favourable fixtures")
        if row["selected_by_percent"] < 10:
            buy_score += 1
            reasons.append("low ownership")
        if row["xG"] > 0.3 and row["xA"] > 0.2:
            buy_score += 1
            reasons.append("high xG/xA")
        if row["cost"] < 6.0 and row["ppm"] > 4:
            buy_score += 1
            reasons.append("value pick")

        sell_score = 0
        if row["form"] < 3:
            sell_score += 2
            reasons.append("poor form")
        if row["next_5_fdr"] > 4:
            sell_score += 2
            reasons.append("difficult fixtures")
        if row["minutes_stability"] < 0.5:
            sell_score += 2
            reasons.append("rotation risk")
        if row["selected_by_percent"] > 30 and row["form"] < 4:
            sell_score += 1
            reasons.append("high ownership, underperforming")

        if buy_score > sell_score and buy_score >= 3:
            rec_type = "BUY"
            confidence = min(buy_score / 5, 1.0)
        elif sell_score > buy_score and sell_score >= 3:
            rec_type = "SELL"
            confidence = min(sell_score / 5, 1.0)
        else:
            rec_type = "HOLD"
            confidence = 0.5

        recommendations.append({
            "player_id": row["id"],
            "player_name": row["name"],
            "team": row["team"],
            "position": row["position"],
            "recommendation": rec_type,
            "confidence": confidence,
            "reasons": ", ".join(reasons[:3]),
            "cost": row["cost"],
            "form": row["form"],
            "fdr_next_5": row["next_5_fdr"]
        })

    return pd.DataFrame(recommendations)