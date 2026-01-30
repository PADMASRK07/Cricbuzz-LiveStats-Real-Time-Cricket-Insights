import streamlit as st
import pandas as pd
import requests
import psycopg2 as db
import json

# ---------------------------
# Configuration & Constants
# ---------------------------
st.set_page_config(page_title="Cricket SQL Analytics", page_icon="📊", layout="wide")

Live_url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/live"
Sorecard_url = "https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/scard"

Player_search = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/search"
Player_status = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}"
Player_batting = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/batting"
Player_bowling = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/bowling"

headers = {
    "x-rapidapi-key": "a958600c89msh2a8c6e7d121682dp14711ejsn2bbc4ce6bead",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

# ---------------------------
# Database Connection
# ---------------------------
def get_connection():
    return db.connect(
        host="localhost",
        port="5432",
        dbname="Cricbuzz",
        user="postgres",
        password="Tanvikha@1998"
    )

# ---------------------------
# Sidebar / Navigation
# ---------------------------
st.sidebar.title("🏏 Cricket Dashboard")
page = st.sidebar.selectbox(
    "Choose a page:",
    ["Live Scores", "Player Stats", "SQL Analytics", "CRUD Operations"]
)
show_debug = st.sidebar.checkbox("Show Debug Info")

# ---------------------------
# Helper: API functions
# ---------------------------
def search_player(name: str):
    params = {"plrN": name}
    try:
        r = requests.get(Player_search, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("player", [])
    except requests.exceptions.RequestException as e:
        st.error(f"API Error (search): {e}")
    except Exception as e:
        st.error(f"Error (search): {e}")
    return []

def get_player_stats(player_id: str):
    url = Player_status.format(player_id=player_id)
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API Error (stats): {e}")
    except Exception as e:
        st.error(f"Error (stats): {e}")
    return None

def get_player_batting(player_id):
    url = Player_batting.format(player_id=player_id)
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API Error (batting): {e}")
    except Exception as e:
        st.error(f"Error (batting): {e}")
    return None

def get_player_bowling(player_id):
    """Fixed bowling function with proper JSON parsing"""
    url = Player_bowling.format(player_id=player_id)
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()

        # Get the raw text first
        response_text = r.text

        # Try to parse as JSON
        try:
            data = json.loads(response_text)
            return data
        except json.JSONDecodeError as json_err:
            st.error(f"JSON Decode Error: {json_err}")
            if show_debug:
                st.write("Raw response:", response_text[:500])
            return None

    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error (bowling): {e.response.status_code}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Request Error (bowling): {e}")
        return None
    except Exception as e:
        st.error(f"Unexpected Error (bowling): {e}")
        return None

def get_live_matches():
    try:
        r = requests.get(Live_url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Error (live): {e}")
    return None

def get_score_card(match_id: str):
    url = Sorecard_url.format(match_id=match_id)
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Error (scorecard): {e}")
    return None

# -------------------------------------------------------
# Data extractors
# -------------------------------------------------------

def extract_batsmen_table(batsmanData):
    rows = []
    for player in batsmanData:
        rows.append({
            "Name": player.get("name"),
            "Runs": player.get("runs"),
            "Balls": player.get("balls"),
            "4s": player.get("fours"),
            "6s": player.get("sixes"),
            "SR": player.get("strkrate")
        })
    return pd.DataFrame(rows)


def extract_bowlers_table(bowlerData):
    rows = []
    for player in bowlerData:
        rows.append({
            "Bowler": player.get("name"),
            "Overs": player.get("overs"),
            "Runs": player.get("runs"),
            "Wkts": player.get("wickets"),
            "Econ": player.get("economy"),
            "Maidens": player.get("maidens")
        })
    return pd.DataFrame(rows)

# =====================================================
# PAGE 1 — LIVE SCORES
# =====================================================
if page == "Live Scores":

    st.markdown(
        """
        <h1 style='text-align: center; color:#008000; font-size:40px;'>🏏 Live Cricket Scoreboard</h1>
        """,
        unsafe_allow_html=True
    )

    show_debug = st.checkbox("Debug: Show Raw API Response")

    data = get_live_matches()
    match_list = []
    match_id_map = {}
    match_info_map = {}

    if data:
        for p in data.get("typeMatches", []):
            for s in p.get("seriesMatches", []):
                wrapper = s.get("seriesAdWrapper", {})

                for m in wrapper.get("matches", []):
                    info = m.get("matchInfo", {})
                    match_id = info.get("matchId")
                    status = info.get("status", "-")
                    matchDesc = info.get("matchDesc","-")
                    matchFormat = info.get("matchFormat","-")
                    venueInfo = info.get("venueInfo",{})
                    ground = venueInfo.get("ground","-")
                    city = venueInfo.get("city","-")
                    seriesname = info.get("seriesName","-")

                    team1 = info.get("team1", {}).get("teamName", "Team A")
                    team2 = info.get("team2", {}).get("teamName", "Team B")

                    match_display = f"{team1} 🆚 {team2} — {status}"
                    match_list.append(match_display)
                    match_id_map[match_display] = match_id

                    match_info_map[match_display] = {
                        "team1": team1,
                        "team2": team2,
                        "series": seriesname,
                        "desc": matchDesc,
                        "format": matchFormat,
                        "ground": ground,
                        "city": city
                    }

    if match_list:
        selected_display = st.selectbox("🏟 Select a Live Match:", match_list)
        selected_match_id = match_id_map[selected_display]
        sel = match_info_map[selected_display]

        score_data = get_score_card(selected_match_id)
        status = score_data.get("status","-")

        st.markdown(
            f"""
            <div style="background:#eef7ff;padding:18px;border-radius:12px;margin-top:15px;">
                <h2>{sel['team1']} vs {sel['team2']}</h2>
                <p><b>🏆 Series:</b> {sel['series']}</p>
                <p><b>📘 Match:</b> {sel['desc']}</p>
                <p><b>📄 Format:</b> {sel['format']}</p>
                <p><b>🏟️ Ground:</b> {sel['ground']}</p>
                <p><b>📍 City:</b> {sel['city']}</p>
                <p><b>🔊 Status:</b> {status}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if score_data and "scorecard" in score_data:
            scorecards = score_data.get("scorecard", [])

            for inn in scorecards:
                bat_team = inn.get("batteamname", "Unknown")
                inning_name = inn.get("inningsid", "-")
                team1 = sel["team1"]
                team2 = sel["team2"]
                if bat_team == team1:
                    bowl_team = team2
                else:
                    bowl_team = team1
                runs = inn.get("score", "-")
                wkts = inn.get("wickets", "-")
                overs = inn.get("overs", "-")
                rr = inn.get("runrate", "-")

                st.markdown(
                    f"""
                    <div style="background:#e8ffe8; padding:15px; border-radius:10px; margin-top:12px;">
                        <h3>🏏 <span style="color:#006400;"></span> Innings — {inning_name}</h3>
                        <p style="font-size:22px; font-weight:bold; margin:0;">
                            {runs}/{wkts} &nbsp; | &nbsp; Overs: {overs}
                        </p>
                        <p style="font-size:18px; margin-top:4px;">
                            Run Rate: {rr}
                        </p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                batsman = inn.get("batsman", [])
                if batsman:
                    st.subheader(f"🟢 Batting - {bat_team}")
                    batsman_df = extract_batsmen_table(batsman)
                    st.table(batsman_df)
                else:
                    st.info("No batting data")

                st.subheader(f"🔵 Bowling - {bowl_team}")
                bowlers = inn.get("bowler", [])
                if bowlers:
                    bowlers_df = extract_bowlers_table(bowlers)
                    st.table(bowlers_df)
                else:
                    st.info("No bowling data")

                st.markdown("<hr style='border:1px solid #d3d3d3;'>", unsafe_allow_html=True)

        else:
            st.warning("⚠ Scorecard not available for this match.")

        if show_debug:
            st.expander("Debug: Score API Raw").write(data)

    else:
        st.error("⚠ No live matches available right now.")


# =====================================================
# PAGE 2 — PLAYER STATS
# =====================================================
elif page == "Player Stats":
    st.title("🧑‍💼 Cricket Player Statistics")

    if "player_list" not in st.session_state:
        st.session_state.player_list = []
    if "player_options" not in st.session_state:
        st.session_state.player_options = {}
    if "player_dropdown" not in st.session_state:
        st.session_state.player_dropdown = None

    with st.form("search_form"):
        pname = st.text_input("Enter player name:", placeholder="e.g., MS Dhoni")
        submitted = st.form_submit_button("🔍 Search")

    if submitted and pname.strip():
        results = search_player(pname)
        if results:
            st.session_state.player_list = results
            st.session_state.player_options = {
                f"{p.get('name')} ({p.get('teamName','Unknown')})": p.get("id") for p in results
            }
            st.session_state.player_dropdown = list(st.session_state.player_options.keys())[0]
        else:
            st.warning("No players found.")

    if st.session_state.player_list:
        st.subheader("Search Results:")

        selected_player = st.selectbox(
            "Select a Player",
            list(st.session_state.player_options.keys()),
            key="player_dropdown"
        )

        player_id = st.session_state.player_options.get(selected_player)

        if player_id:
            st.info(f"Fetching stats for player ID: {player_id}")
            stats = get_player_stats(player_id)

            if show_debug:
                st.expander("Raw player stats").write(stats)

            if stats and isinstance(stats, dict):
                st.markdown(f"## 📊 {stats.get('name','Player')} - Player Profile")
                st.markdown(f"### {stats.get('name','N/A')}")
                st.markdown(f"**Nickname:** {stats.get('nickName','N/A')}")
                st.write("---")

                tab1, tab2, tab3 = st.tabs(["👤 Profile", "🏏 Batting Stats", "🎯 Bowling Stats"])

                # -------- Profile tab ----------
                with tab1:
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.markdown("### 🏏 Cricket Details")
                        st.write(f"**Id:** {stats.get('id')}")
                        st.write(f"**Role:** {stats.get('role')}")
                        st.write(f"**Batting:** {stats.get('bat') or stats.get('battingStyle') or 'N/A'}")
                        st.write(f"**Bowling:** {stats.get('bowl') or stats.get('bowlingStyle') or 'N/A'}")
                        st.write(f"**Team:** {stats.get('intlTeam') or stats.get('teamName') or 'N/A'}")

                    with col2:
                        st.markdown("### 📌 Personal Details")
                        dob = stats.get('DoB') or stats.get('dob') or stats.get('birthDate')
                        st.write(f"**DOB:** {dob or 'N/A'}")
                        st.write(f"**Birth Place:** {stats.get('birthPlace') or stats.get('birthplace') or 'N/A'}")
                        st.write(f"**Height:** {stats.get('height') or 'N/A'}")

                    with col3:
                        st.markdown("### 🏆 Teams Played For")
                        teams = stats.get("teams") or stats.get("teamList") or []
                        if isinstance(teams, list):
                            for t in teams:
                                st.write(f"• {t}")
                        elif isinstance(teams, str):
                            for t in teams.split(","):
                                st.write(f"• {t.strip()}")
                        else:
                            st.write("N/A")

                # -------- Batting tab ----------
                with tab2:
                    st.subheader("🏏 Batting Career Statistics")

                    batting_json = get_player_batting(player_id)

                    if show_debug:
                        st.expander("Raw batting API response").write(batting_json)

                    if batting_json and isinstance(batting_json, dict) and "headers" in batting_json and "values" in batting_json:
                        headers_list = batting_json["headers"]
                        rows = batting_json["values"]
                        formats = headers_list[1:]

                        stats_map = {}
                        for row in rows:
                            vals = row.get("values", [])
                            if not vals:
                                continue
                            stat_name = vals[0]
                            stat_values = vals[1:]
                            stats_map[stat_name] = stat_values

                        df = pd.DataFrame.from_dict(stats_map, orient="index", columns=formats)
                        st.markdown("### 📋 Career Overview")
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.warning("No batting statistics available for this player.")

                # ---------- BOWLING TAB - FINAL FIX WITH PROPER COLUMN MATCHING ----------
                with tab3:
                    st.subheader("🎯 Bowling Career Statistics")

                    bowl = get_player_bowling(player_id)

                    if show_debug:
                        st.expander("Raw bowling API response").write(bowl)

                    # Process bowling data
                    if bowl is None:
                        st.info("Unable to fetch bowling data.")
                    elif not isinstance(bowl, dict):
                        st.info("This player does not have bowling statistics.")
                    elif "headers" not in bowl or "values" not in bowl:
                        st.info("This player does not have bowling statistics.")
                    else:
                        try:
                            # Extract headers and values
                            api_headers = bowl["headers"]
                            rows = bowl["values"]

                            if show_debug:
                                st.write("**Debug - API Headers:**", api_headers)
                                st.write("**Debug - First Row:**", rows[0] if rows else "No rows")

                            # Process headers properly
                            # Headers format: ['ROWHEADER', 'Test', 'ODI', 'T20', 'IPL']
                            # We want: ['Test', 'ODI', 'T20', 'IPL']
                            format_headers = [h for h in api_headers if h != "ROWHEADER"]

                            if show_debug:
                                st.write("**Debug - Processed Headers:**", format_headers)

                            # Build stats map
                            stats_map = {}

                            for row in rows:
                                # Extract values from row
                                if isinstance(row, dict) and "values" in row:
                                    vals = row["values"]
                                elif isinstance(row, list):
                                    vals = row
                                else:
                                    continue

                                if not vals or len(vals) < 2:
                                    continue

                                # First value is the stat name (e.g., "Matches")
                                stat_name = str(vals[0])
                                # Rest are the values for each format
                                stat_values = vals[1:]

                                # IMPORTANT: Ensure stat_values length matches format_headers length
                                if len(stat_values) != len(format_headers):
                                    if show_debug:
                                        st.warning(f"Mismatch for {stat_name}: {len(stat_values)} values vs {len(format_headers)} headers")
                                    # Pad or truncate to match
                                    if len(stat_values) < len(format_headers):
                                        stat_values = stat_values + ['0'] * (len(format_headers) - len(stat_values))
                                    else:
                                        stat_values = stat_values[:len(format_headers)]

                                stats_map[stat_name] = stat_values

                            if show_debug:
                                st.write("**Debug - Stats Map:**", stats_map)

                            if stats_map:
                                # Create DataFrame
                                df_bowling = pd.DataFrame.from_dict(
                                    stats_map,
                                    orient="index",
                                    columns=format_headers
                                )

                                st.markdown("### 📋 Bowling Overview")
                                st.dataframe(df_bowling, use_container_width=True)
                            else:
                                st.info("No bowling statistics data available.")

                        except Exception as e:
                            st.error(f"Error processing bowling data: {str(e)}")
                            if show_debug:
                                import traceback
                                st.code(traceback.format_exc())

            else:
                st.error("No stats found for this player.")
        else:
            st.error("Selected player ID not found in options.")
    else:
        st.info("Search for a player to see results here.")


# =====================================================
# PAGE 3 — SQL ANALYTICS
# =====================================================
elif page == "SQL Analytics":
    st.title("🏏 Cricket SQL Analytics")
    st.markdown("### 🧠 Database Query Questions")

    query_bank = {
        "1) List of players who represent India": """
            SELECT PlayerId, PlayerName, BattingStyle, BowlingStyle, PlayingRole
            FROM indianPlayers_1;
        """,
        "2) Cricket matches that were played in the last Few days": """
            SELECT * FROM cricket_matches_2 ORDER BY start_date DESC;
        """,
        "3) Top 10 highest run scorers in ODI cricket": """
            SELECT player_id, player_name, total_runs, batting_avg, centuries
             FROM odi_player_info_3 ORDER BY total_runs DESC LIMIT 10;
        """,
        "4) All cricket venues that have a seating capacity of more than 30,000 spectators": """
           SELECT ground,city, country, seating_capacity FROM cricket_matches_2 WHERE seating_capacity > 30000;
        """,
        "5) No of matches each team has won": """
           SELECT team_id, team_name, win_count FROM team_wins_5 ORDER BY win_count DESC;
        """ ,
        "6) How many players belong to each playing role": """
           SELECT  PlayingRole, COUNT(*) AS player_count FROM indianPlayers_1 GROUP BY PlayingRole ORDER BY player_count DESC;
        """,
        "7) The highest individual batting score achieved in each cricket format" : """
           SELECT 'Test' AS format, MAX(test_score) AS highest_score FROM player_highest_scores_7
UNION ALL
SELECT 'ODI' AS format, MAX(odi_score) AS highest_score FROM player_highest_scores_7
UNION ALL
SELECT 'T20I' AS format, MAX(t20i_score) AS highest_score FROM player_highest_scores_7
UNION ALL
SELECT 'IPL' AS format, MAX(ipl_score) AS highest_score FROM player_highest_scores_7;

        """ ,
        "8) All cricket series that started in the year 2024": """
          SELECT *FROM cricket_series_8 WHERE start_date LIKE '%2024%';
        """,
        "9) All-rounder players who have scored more than 1000 runs AND taken more than 50 wickets in their career": """
          SELECT   player_name,    total_runs,    total_wickets,    cricket_format FROM player_career_stats_9
WHERE role = 'Batting Allrounder'   AND total_runs > 1000   AND total_wickets > 50;
        """,
         "10) Details of the last 20 completed matches": """
          SELECT      match_description,    team1_name,    team2_name,    winning_team,    victory_margin,    victory_type,    venue_name
FROM recent_completed_matches_10 ORDER BY match_end_date DESC LIMIT 20;
        """,
         "11) Players who have played at least 2 different formats": """
         SELECT  pcs.player_id, pcs.player_name,
    SUM(CASE WHEN pcs.cricket_format = 'TEST' THEN pcs.total_runs ELSE 0 END) AS test_runs,
    SUM(CASE WHEN pcs.cricket_format = 'ODI' THEN pcs.total_runs ELSE 0 END) AS odi_runs,
    SUM(CASE WHEN pcs.cricket_format = 'T20' THEN pcs.total_runs ELSE 0 END) AS t20_runs,

    ROUND(AVG(opi.batting_avg), 2) AS overall_batting_average FROM player_career_stats_9 pcs JOIN odi_player_info_3 opi
    ON pcs.player_id = CAST(opi.player_id AS INTEGER) GROUP BY pcs.player_id, pcs.player_name HAVING COUNT(DISTINCT pcs.cricket_format) >= 2;
        """,
         "12) Each international team's performance when playing at home versus playing away": """
SELECT t.team_name, SUM(CASE
            WHEN v.country_name = t.country_name
                 AND r.winning_team = t.team_name
            THEN 1 ELSE 0
        END
    ) AS home_wins,  SUM(
        CASE
            WHEN v.country_name <> t.country_name
                 AND r.winning_team = t.team_name
            THEN 1 ELSE 0
        END
    ) AS away_wins
FROM recent_completed_matches_10 r JOIN team_country_mapping_12 t ON t.team_name IN (r.team1_name, r.team2_name) JOIN venue_country_mapping_12 v
    ON r.venue_name = v.venue_name GROUP BY t.team_name ORDER BY t.team_name;
        """,
         "13) Batting partnerships where two consecutive batsmen scored a combined total of 100 ": """
SELECT player1, player2, innings, partnership_runs FROM batting_partnerships_13 ORDER BY partnership_runs DESC;
        """,
        "14) Examine bowling performance at different venues": """SELECT
    bowler_name,     venue,     COUNT(*) AS matches_played,     ROUND(SUM(runs_conceded) / SUM(overs_bowled), 2) AS avg_economy,
    SUM(wickets_taken) AS total_wickets FROM bowling_match_stats_14  WHERE overs_bowled >= 4 GROUP BY bowler_name, venue HAVING COUNT(*) >= 3;
        """,
         "15) Players who perform exceptionally well in close matches ": """SELECT
    player_name,
    average_runs_close_matches,
    close_matches_played,
    close_matches_won_when_batting
FROM close_match_player_performance_15
WHERE average_runs_close_matches > 20
ORDER BY average_runs_close_matches DESC;
        """,
         "16) Players batting performance changes over different years ": """SELECT
    player_name,
    year,
    matches_played,
    average_runs_per_match,
    average_strike_rate
FROM player_yearly_batting_performance_16
WHERE year >= 2020
  AND matches_played >= 5
ORDER BY year, average_runs_per_match DESC;

        """,
         "17) Whether winning the toss gives teams an advantage in winning matches ": """SELECT
    toss_decision,
    COUNT(*) AS total_matches,
    SUM(
        CASE 
            WHEN toss_winner = match_winner THEN 1 
            ELSE 0 
        END
    ) AS matches_won_after_winning_toss,
    ROUND(
        SUM(
            CASE 
                WHEN toss_winner = match_winner THEN 1 
                ELSE 0 
            END
        ) * 100.0 / COUNT(*),
        2
    ) AS win_percentage
FROM win_pecentage_17
GROUP BY toss_decision;
        """,
        "18) The most economical bowlers in limited-overs cricket ": """SELECT player_id, player_name, matches, overs, wickets, economy
FROM impact_bowlers_18
ORDER BY economy ASC;
        """,
        "19) Batsmen Who are most consistent in their scoring ": """SELECT
    id,
    player_name,
    format,
    innings,
    total_runs,
    average_runs,
    std_deviation
FROM batting_consistency_19; """,
       "20) Players Who has played in different cricket formats and their batting average in each format" : """SELECT
    player_name,

    SUM(CASE WHEN format = 'TEST' THEN matches_played ELSE 0 END) AS test_matches,
    MAX(CASE WHEN format = 'TEST' THEN batting_average END) AS test_batting_avg,

    SUM(CASE WHEN format = 'ODI' THEN matches_played ELSE 0 END) AS odi_matches,
    MAX(CASE WHEN format = 'ODI' THEN batting_average END) AS odi_batting_avg,

    SUM(CASE WHEN format = 'T20' THEN matches_played ELSE 0 END) AS t20_matches,
    MAX(CASE WHEN format = 'T20' THEN batting_average END) AS t20_batting_avg

FROM cricket_player_stats_20
GROUP BY player_name
HAVING SUM(matches_played) >= 20
ORDER BY player_name;
 """,
 "21) Comprehensive performance ranking system for players" :"""SELECT
    player_id,
    format,
    batting_points,
    bowling_points,
    total_score,
    RANK() OVER (
        PARTITION BY format
        ORDER BY total_score DESC
    ) AS performance_rank
FROM player_performance_21;
 """,
 "22) Head-to-head match prediction analysis between teams" :""" SELECT
    team_a,
    team_b,
    total_matches,
    team_a_wins,
    team_b_wins,
    team_a_win_percentage,
    team_b_win_percentage,
    avg_margin_team_a,
    avg_margin_team_b,
    batting_first_wins_team_a,
    bowling_first_wins_team_a,
    batting_first_wins_team_b,
    bowling_first_wins_team_b,
    venue,
    analysis_period_years
FROM head_to_head_analysis_22
ORDER BY total_matches DESC, team_a;
 """,
 "23) Recent player form and momentum" : """SELECT
    player_id,
    player_name,
    avg_runs_last_5,
    avg_runs_last_10,
    strike_rate_last_5,
    strike_rate_last_10,
    fifties_last_10,
    runs_std_dev,
    consistency_score,
    form_category
FROM player_recent_form_23
ORDER BY
    CASE form_category
        WHEN 'Excellent Form' THEN 1
        WHEN 'Good Form' THEN 2
        WHEN 'Average Form' THEN 3
        ELSE 4
    END,
    avg_runs_last_5 DESC;
 """,
 "24) Successful batting partnerships to identify the best player combinations ": """SELECT * FROM batting_partnership_analysis_24 """,
 "25) A time-series analysis of player performance evolution" : """WITH quarterly_changes AS (
    SELECT
        player_id,
        player_name,
        year,
        quarter,
        avg_runs,
        avg_strike_rate,
        LAG(avg_runs) OVER (PARTITION BY player_id ORDER BY year, quarter) AS prev_avg_runs,
        LAG(avg_strike_rate) OVER (PARTITION BY player_id ORDER BY year, quarter) AS prev_strike_rate
    FROM player_quarterly_performance_25
),
trend_analysis AS (
    SELECT
        player_id,
        player_name,
        year,
        quarter,
        avg_runs,
        avg_strike_rate,
        CASE
            WHEN avg_runs > prev_avg_runs THEN 'Improving'
            WHEN avg_runs < prev_avg_runs THEN 'Declining'
            ELSE 'Stable'
        END AS quarterly_trend
    FROM quarterly_changes
),
career_summary AS (
    SELECT
        player_id,
        player_name,
        COUNT(*) AS total_quarters,
        AVG(avg_runs) AS career_avg_runs,
        AVG(avg_strike_rate) AS career_avg_sr,
        SUM(CASE WHEN quarterly_trend = 'Improving' THEN 1 ELSE 0 END) AS improving_quarters,
        SUM(CASE WHEN quarterly_trend = 'Declining' THEN 1 ELSE 0 END) AS declining_quarters
    FROM trend_analysis
    GROUP BY player_id, player_name
    HAVING COUNT(*) >= 6
)
SELECT
    player_id,
    player_name,
    total_quarters,
    career_avg_runs,
    career_avg_sr,
    CASE
        WHEN improving_quarters > declining_quarters THEN 'Career Ascending'
        WHEN declining_quarters > improving_quarters THEN 'Career Declining'
        ELSE 'Career Stable'
    END AS career_phase
FROM career_summary
ORDER BY career_avg_runs DESC;
 """

    }

    selected_question = st.selectbox("Choose a query", list(query_bank.keys()))

    st.subheader(selected_question)
    with st.expander("View SQL Query"):
        st.code(query_bank[selected_question], language="sql")

    if st.button("🚀 Execute Query"):
        try:
            conn = get_connection()
            cur = conn.cursor()
            sql = query_bank[selected_question]
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=cols)
            st.success("Query executed successfully!")
            st.dataframe(df, use_container_width=True)
            cur.close()
            conn.close()
        except Exception as e:
            st.error(f"❌ Error executing query: {e}")


# =====================================================
# PAGE 4 — CRUD OPERATIONS
# =====================================================
elif page == "CRUD Operations":
    st.title("🛠️ CRUD Operations")
    st.markdown("### ✨ Add / View / Update / Delete")

    operation = st.selectbox(
        "Choose Operation",
        ["📖 Read", "➕ Create", "✏️ Update", "🗑️ Delete"]
    )

    # ----------------- READ -----------------
    if "Read" in operation:
        if st.button("Load All Players"):
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM CRUD_operation;")
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                df = pd.DataFrame(rows, columns=cols)
                st.dataframe(df, use_container_width=True)
                cur.close()
                conn.close()
            except Exception as e:
                st.error(e)

        name_search = st.text_input("Search by name")
        if name_search:
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("""
                    SELECT * FROM CRUD_operation
                    WHERE PlayerName ILIKE %s
                """, (f"%{name_search}%",))
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                st.dataframe(pd.DataFrame(rows, columns=cols))
                cur.close()
                conn.close()
            except Exception as e:
                st.error(e)

    # ----------------- CREATE -----------------
    elif "Create" in operation:
        st.subheader("Add New Player")

        with st.form("add_player_form"):
            col1, col2, col3 = st.columns(3)
            with col1:
                pid = st.number_input("Player ID", min_value=0)
                matches = st.number_input("Matches", min_value=0)
            with col2:
                name = st.text_input("Player Name")
                innings = st.number_input("Innings", min_value=0)
            with col3:
                runs = st.number_input("Runs", min_value=0)
                avg = st.number_input("Average", min_value=0.0)

            submit = st.form_submit_button("Add Player")

        if submit:
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO CRUD_operation
                    (PlayerId, PlayerName, Runs, Matches, Innings, Average)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (pid, name, runs, matches, innings, avg))
                conn.commit()
                st.success("Player Added!")
                cur.close()
                conn.close()
            except Exception as e:
                st.error(e)

    # ----------------- UPDATE -----------------
    elif "Update" in operation:
        pid = st.number_input("Enter Player ID", min_value=1)

        if st.button("Fetch Player"):
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM CRUD_operation WHERE PlayerId=%s", (pid,))
                row = cur.fetchone()
                cur.close()
                conn.close()

                if row:
                    st.session_state.player_data = row
                    st.success("Player Found!")
                else:
                    st.error("Player Not Found")

            except Exception as e:
                st.error(e)

        if "player_data" in st.session_state and st.session_state.player_data:
            row = st.session_state.player_data

            new_name = st.text_input("Player Name", row[1])
            new_runs = st.number_input("Runs", value=row[2])
            new_matches = st.number_input("Matches", value=row[3])
            new_innings = st.number_input("Innings", value=row[4])
            new_avg = st.number_input("Average", value=row[5])

            if st.button("Update Now"):
                try:
                    conn = get_connection()
                    cur = conn.cursor()
                    cur.execute("""
                        UPDATE CRUD_operation
                        SET PlayerName=%s, Runs=%s, Matches=%s, Innings=%s, Average=%s
                        WHERE PlayerId=%s
                    """, (new_name, new_runs, new_matches, new_innings, new_avg, pid))
                    conn.commit()
                    st.success("Player Updated!")
                    cur.close()
                    conn.close()
                    st.session_state.player_data = None
                except Exception as e:
                    st.error(e)

    # ----------------- DELETE -----------------
    elif "Delete" in operation:
        pid = st.number_input("Enter Player ID", min_value=0)

        if st.button("Delete Player"):
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("DELETE FROM CRUD_operation WHERE PlayerId=%s", (pid,))
                conn.commit()
                st.success("Player Deleted!")
                cur.close()
                conn.close()
            except Exception as e:
                st.error(e)