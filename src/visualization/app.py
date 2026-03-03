import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page config
st.set_page_config(page_title="FPL Analytics", page_icon="⚽", layout="wide")

# Load database URL from secrets
DATABASE_URL = st.secrets["passwords"]["DATABASE_URL"]

# Connection function with caching
@st.cache_resource
def init_connection():
    return psycopg2.connect(DATABASE_URL)

conn = init_connection()

# Data loading functions with caching
@st.cache_data(ttl=3600)
def load_players():
    query = """
        SELECT 
            p.id,
            p.name,
            t.name as team,
            CASE p.position 
                WHEN 1 THEN 'GKP'
                WHEN 2 THEN 'DEF'
                WHEN 3 THEN 'MID'
                WHEN 4 THEN 'FWD'
            END as position,
            p.now_cost/10.0 as cost,
            p.total_points,
            p.points_per_game,
            p.selected_by_percent as selected_by,
            p.form,
            p.goals_scored,
            p.assists,
            p.clean_sheets,
            p.updated_at
        FROM players p
        JOIN teams t ON p.team_id = t.id
        ORDER BY p.total_points DESC;
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def load_teams():
    query = """
        SELECT 
            id,
            name,
            short_name,
            strength
        FROM teams
        ORDER BY strength DESC;
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def load_top_scorers():
    query = """
        SELECT 
            p.name,
            t.short_name as team,
            p.goals_scored,
            p.assists,
            p.total_points
        FROM players p
        JOIN teams t ON p.team_id = t.id
        WHERE p.goals_scored > 0 OR p.assists > 0
        ORDER BY p.goals_scored DESC
        LIMIT 20;
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def load_recommendations():
    query = """
        SELECT 
            r.player_id,
            p.name,
            t.short_name as team,
            CASE p.position 
                WHEN 1 THEN 'GKP'
                WHEN 2 THEN 'DEF'
                WHEN 3 THEN 'MID'
                WHEN 4 THEN 'FWD'
            END as position,
            r.recommendation,
            r.confidence,
            r.reasons,
            p.now_cost/10.0 as cost,
            p.form,
            r.fdr_next_5
        FROM recommendations r
        JOIN players p ON r.player_id = p.id
        JOIN teams t ON p.team_id = t.id
        ORDER BY 
            CASE r.recommendation 
                WHEN 'BUY' THEN 1
                WHEN 'HOLD' THEN 2
                WHEN 'SELL' THEN 3
            END,
            r.confidence DESC;
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def get_next_deadline():
    # Using gameweeks table now (more direct)
    query = """
        SELECT deadline_time FROM gameweeks
        WHERE finished = false
        ORDER BY id ASC
        LIMIT 1
    """
    df = pd.read_sql(query, conn)
    if not df.empty:
        return pd.to_datetime(df.iloc[0,0])
    return None

# Load data
players_df = load_players()
teams_df = load_teams()
top_scorers_df = load_top_scorers()
recs_df = load_recommendations()

# Sidebar filters
st.sidebar.header("🔍 Filters")

# Position filter
positions = ["All"] + sorted(players_df['position'].unique().tolist())
selected_position = st.sidebar.selectbox("Position", positions)

# Team filter
teams = ["All"] + sorted(players_df['team'].unique().tolist())
selected_team = st.sidebar.selectbox("Team", teams)

# Cost range filter
min_cost = float(players_df['cost'].min())
max_cost = float(players_df['cost'].max())
cost_range = st.sidebar.slider(
    "Cost (millions)",
    min_value=min_cost,
    max_value=max_cost,
    value=(min_cost, max_cost),
    step=0.5
)

# Form filter
min_form = float(players_df['form'].min())
max_form = float(players_df['form'].max())
form_range = st.sidebar.slider(
    "Form",
    min_value=min_form,
    max_value=max_form,
    value=(min_form, max_form),
    step=0.1
)

# Apply filters
filtered_df = players_df.copy()
if selected_position != "All":
    filtered_df = filtered_df[filtered_df['position'] == selected_position]
if selected_team != "All":
    filtered_df = filtered_df[filtered_df['team'] == selected_team]
filtered_df = filtered_df[
    (filtered_df['cost'] >= cost_range[0]) &
    (filtered_df['cost'] <= cost_range[1]) &
    (filtered_df['form'] >= form_range[0]) &
    (filtered_df['form'] <= form_range[1])
]

# Next deadline display
next_deadline = get_next_deadline()
if next_deadline:
    time_remaining = next_deadline - datetime.now()
    st.sidebar.info(f"⏳ Next deadline: {next_deadline.strftime('%Y-%m-%d %H:%M')} ({time_remaining.days}d {time_remaining.seconds//3600}h)")

# Main content
st.title("⚽ Fantasy Premier League Analytics")
st.markdown("---")

# Top metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Players", len(players_df))
with col2:
    avg_points = round(players_df['total_points'].mean(), 1)
    st.metric("Avg Points", avg_points)
with col3:
    most_selected = players_df.loc[players_df['selected_by'].idxmax(), 'name'] if not players_df.empty else "N/A"
    st.metric("Most Selected", most_selected)
with col4:
    top_scorer = players_df.loc[players_df['total_points'].idxmax(), 'name'] if not players_df.empty else "N/A"
    st.metric("Top Scorer", top_scorer)

st.markdown("---")

# Create tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Players", "🏆 Teams", "⚡ Top Performers", "📈 Compare", "💡 Recommendations"])

with tab1:
    st.subheader("Player Database")
    st.write(f"Showing {len(filtered_df)} players")
    st.dataframe(
        filtered_df.style.format({
            'cost': '£{:.1f}m',
            'selected_by': '{:.1f}%',
            'points_per_game': '{:.2f}',
            'form': '{:.2f}'
        }),
        use_container_width=True,
        height=600
    )
    st.subheader("Cost vs Total Points")
    fig = px.scatter(
        filtered_df,
        x='cost',
        y='total_points',
        color='position',
        hover_data=['name', 'team'],
        labels={'cost': 'Cost (£m)', 'total_points': 'Total Points'},
        title="Player Value Analysis"
    )
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Team Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Team Strength")
        fig = px.bar(
            teams_df,
            x='name',
            y='strength',
            color='strength',
            labels={'strength': 'Strength Rating', 'name': 'Team'},
            title="FPL Team Strength"
        )
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        team_stats = players_df.groupby('team').agg({
            'total_points': 'sum',
            'goals_scored': 'sum',
            'assists': 'sum',
            'clean_sheets': 'sum'
        }).reset_index().sort_values('total_points', ascending=False)
        st.subheader("Team Total Points")
        fig = px.bar(
            team_stats,
            x='team',
            y='total_points',
            color='total_points',
            labels={'total_points': 'Total Points', 'team': 'Team'},
            title="Total Points by Team"
        )
        st.plotly_chart(fig, use_container_width=True)
    st.subheader("Top Players by Team")
    selected_team_detail = st.selectbox("Select a team", teams_df['name'].tolist())
    team_players = players_df[players_df['team'] == selected_team_detail].sort_values('total_points', ascending=False).head(10)
    st.dataframe(
        team_players[['name', 'position', 'cost', 'total_points', 'goals_scored', 'assists']].style.format({
            'cost': '£{:.1f}m'
        }),
        use_container_width=True
    )

with tab3:
    st.subheader("Top Performers")
    col1, col2 = st.columns(2)
    with col1:
        top_goals = players_df.nlargest(10, 'goals_scored')[['name', 'team', 'goals_scored', 'assists', 'total_points']]
        st.subheader("⚽ Top Goalscorers")
        st.dataframe(top_goals, use_container_width=True)
    with col2:
        top_assists = players_df.nlargest(10, 'assists')[['name', 'team', 'goals_scored', 'assists', 'total_points']]
        st.subheader("🎯 Top Assisters")
        st.dataframe(top_assists, use_container_width=True)
    st.subheader("🌟 Most Selected Players")
    most_selected = players_df.nlargest(10, 'selected_by')[['name', 'team', 'position', 'selected_by', 'total_points', 'form']]
    st.dataframe(
        most_selected.style.format({'selected_by': '{:.1f}%', 'form': '{:.2f}'}),
        use_container_width=True
    )
    st.subheader("🔥 Players in Form")
    form_leaders = players_df.nlargest(10, 'form')[['name', 'team', 'position', 'form', 'total_points', 'selected_by']]
    st.dataframe(
        form_leaders.style.format({'form': '{:.2f}', 'selected_by': '{:.1f}%'}),
        use_container_width=True
    )

with tab4:
    st.subheader("Player Comparison")
    player_options = players_df['name'].tolist()
    selected_players = st.multiselect(
        "Choose 2-5 players to compare",
        options=player_options,
        max_selections=5
    )
    if len(selected_players) >= 2:
        compare_df = players_df[players_df['name'].isin(selected_players)].set_index('name')
        compare_df = compare_df[['team', 'position', 'cost', 'total_points', 'goals_scored', 'assists', 'clean_sheets', 'form', 'selected_by']]
        st.dataframe(
            compare_df.T.style.format({
                'cost': '£{:.1f}m',
                'selected_by': '{:.1f}%',
                'form': '{:.2f}'
            }),
            use_container_width=True
        )
        categories = ['cost', 'total_points', 'goals_scored', 'assists', 'clean_sheets', 'form']
        radar_data = compare_df[categories].copy()
        for col in radar_data.columns:
            radar_data[col] = (radar_data[col] - radar_data[col].min()) / (radar_data[col].max() - radar_data[col].min() + 0.001)
        fig = go.Figure()
        for player in radar_data.index:
            fig.add_trace(go.Scatterpolar(
                r=radar_data.loc[player].values,
                theta=categories,
                fill='toself',
                name=player
            ))
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            showlegend=True,
            title="Player Comparison (Normalized)"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Select at least 2 players to compare")

with tab5:
    st.subheader("AI-Powered Recommendations")
    rec_filter = st.selectbox("Show", ["All", "BUY", "HOLD", "SELL"])
    if rec_filter != "All":
        filtered_recs = recs_df[recs_df["recommendation"] == rec_filter]
    else:
        filtered_recs = recs_df

    def color_rec(val):
        color = ""
        if val == "BUY":
            color = "background-color: #90EE90"
        elif val == "SELL":
            color = "background-color: #FFB6C1"
        elif val == "HOLD":
            color = "background-color: #FFE4B5"
        return color

    styled = filtered_recs.style.applymap(color_rec, subset=["recommendation"])
    styled.format({
        "cost": "£{:.1f}m",
        "confidence": "{:.0%}",
        "form": "{:.2f}",
        "fdr_next_5": "{:.1f}"
    })
    st.dataframe(styled, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🔝 Top 10 Buys")
        top_buys = recs_df[recs_df["recommendation"] == "BUY"].head(10)
        st.dataframe(top_buys[["name", "team", "position", "confidence", "cost", "form", "fdr_next_5", "reasons"]].style.format({
            "cost": "£{:.1f}m",
            "confidence": "{:.0%}",
            "form": "{:.2f}",
            "fdr_next_5": "{:.1f}"
        }), use_container_width=True)
    with col2:
        st.subheader("🔻 Top 10 Sells")
        top_sells = recs_df[recs_df["recommendation"] == "SELL"].head(10)
        st.dataframe(top_sells[["name", "team", "position", "confidence", "cost", "form", "fdr_next_5", "reasons"]].style.format({
            "cost": "£{:.1f}m",
            "confidence": "{:.0%}",
            "form": "{:.2f}",
            "fdr_next_5": "{:.1f}"
        }), use_container_width=True)

    st.caption("⚠️ Recommendations are based on statistical models and are for informational purposes only. Always do your own research.")

# Footer
st.markdown("---")
st.caption(f"Data last updated: {players_df['updated_at'].max() if not players_df.empty else 'N/A'} | Source: Fantasy Premier League")