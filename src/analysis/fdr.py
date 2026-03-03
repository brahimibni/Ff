import pandas as pd

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