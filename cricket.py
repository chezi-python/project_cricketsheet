import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px


class MySQLQueryRunner:
    def __init__(self, host, user, password, database):
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4'
        )
        self.cursor = self.conn.cursor(dictionary=True)

    def run_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()


def get_queries(table):
    return {
        "Total runs scored by each team": f"""
            SELECT team, SUM(runs_total) AS total_runs
            FROM {table}
            GROUP BY team
            ORDER BY total_runs DESC;
        """,
        "Total runs scored by each batter": f"""
            SELECT batter, SUM(runs_batter) AS total_runs
            FROM {table}
            GROUP BY batter
            ORDER BY total_runs DESC
            LIMIT 10;
        """,
        "Total wickets fallen for each team": f"""
            SELECT team, COUNT(wicket_player_out) AS total_wickets
            FROM {table}
            WHERE wicket_player_out IS NOT NULL
            GROUP BY team
            ORDER BY total_wickets DESC;
        """,
        "Number of wickets by kind": f"""
            SELECT wicket_kind, COUNT(*) AS count
            FROM {table}
            WHERE wicket_kind IS NOT NULL
            GROUP BY wicket_kind
            ORDER BY count DESC;
        """,
        "Top bowlers by number of wickets taken": f"""
            SELECT bowler, COUNT(wicket_player_out) AS wickets
            FROM {table}
            WHERE wicket_player_out IS NOT NULL
            GROUP BY bowler
            ORDER BY wickets DESC
            LIMIT 10;
        """,

        "Most common dismissal types": f"""
            SELECT wicket_kind, COUNT(*) AS frequency
            FROM {table}
            WHERE wicket_kind IS NOT NULL
            GROUP BY wicket_kind
            ORDER BY frequency DESC;
        """,
 
        "Number of deliveries bowled by each bowler": f"""
            SELECT bowler, COUNT(*) AS deliveries_bowled
            FROM {table}
            GROUP BY bowler
            ORDER BY deliveries_bowled DESC;
        """,
        "Batters caught out by specific fielder": f"""
            SELECT wicket_player_out, wicket_fielders
            FROM {table}
            WHERE wicket_kind = 'caught'
            AND wicket_fielders LIKE '%Smith%';
        """,
        "Total extras conceded by each team": f"""
            SELECT team, SUM(runs_extras) AS extras_conceded
            FROM {table}
            GROUP BY team
            ORDER BY extras_conceded DESC;
        """,
        "Highest caught the fielder": f"""

            SELECT
            TRIM(wicket_fielders) AS fielder,
            COUNT(*) AS catches
            FROM {table}
            WHERE wicket_kind = 'caught'
            AND COALESCE(wicket_fielders, '') <> ''
            GROUP BY fielder
            ORDER BY catches DESC
            LIMIT 20;

        """,
        "Highest catches taken ceicketer": f"""
            SELECT fielder, COUNT(*) AS catches
            FROM (
            SELECT TRIM(wicket_fielders) AS fielder FROM test_matches WHERE wicket_kind='caught' AND COALESCE(wicket_fielders,'')<>''
            UNION ALL
            SELECT TRIM(wicket_fielders) FROM odi_matches  WHERE wicket_kind='caught' AND COALESCE(wicket_fielders,'')<>''
            UNION ALL
            SELECT TRIM(wicket_fielders) FROM t20_matches  WHERE wicket_kind='caught' AND COALESCE(wicket_fielders,'')<>''
            UNION ALL
            SELECT TRIM(wicket_fielders) FROM ipl_matches  WHERE wicket_kind='caught' AND COALESCE(wicket_fielders,'')<>''
            ) x
            GROUP BY fielder
            ORDER BY catches DESC
            LIMIT 10;
        """
    }


def query_tab_ui(table, key_prefix):
    if 'runner' not in st.session_state:
        st.warning("Please connect to the database first.")
        return
    
    runner = st.session_state['runner']
    queries = get_queries(table)

    selected_query = st.selectbox("Select Analysis", list(queries.keys()), key=f"{key_prefix}_select")

    if "fielder" in selected_query.lower():
        fielder_name = st.text_input("Enter fielder name", "Smith", key=f"{key_prefix}_fielder")
        queries[selected_query] = queries[selected_query].replace("Smith", fielder_name)

    if st.button("Run Analysis", key=f"{key_prefix}_run"):
        try:
            results = runner.run_query(queries[selected_query])
            df = pd.DataFrame(results)

            st.subheader("Data Preview")
            st.dataframe(df.head(20))


            st.subheader("Visualizations")
            if "runs scored by each team" in selected_query.lower():
                fig = px.bar(df, x='team', y='total_runs', title=selected_query)
                st.plotly_chart(fig, use_container_width=True)
                fig2 = px.pie(df, names='team', values='total_runs', title="Team Run Distribution")
                st.plotly_chart(fig2, use_container_width=True)

            elif "runs scored by each batter" in selected_query.lower():
                fig = px.bar(df, x='batter', y='total_runs', title=selected_query, color='total_runs')
                st.plotly_chart(fig, use_container_width=True)

            elif "wickets by kind" in selected_query.lower():
                fig = px.pie(df, names='wicket_kind', values='count', title=selected_query)
                st.plotly_chart(fig, use_container_width=True)

            elif "top bowlers" in selected_query.lower():
                fig = px.bar(df, x='bowler', y='wickets', title=selected_query, color='wickets')
                st.plotly_chart(fig, use_container_width=True)

            elif "extras type" in selected_query.lower():
                fig = px.bar(df, x='extras_type', y='deliveries_count', title=selected_query)
                st.plotly_chart(fig, use_container_width=True)

            elif "dismissal types" in selected_query.lower():
                fig = px.bar(df, x='wicket_kind', y='frequency', title=selected_query)
                st.plotly_chart(fig, use_container_width=True)


            elif "deliveries bowled" in selected_query.lower():
                fig = px.bar(df.head(20), x='bowler', y='deliveries_bowled', title="Top 20 Bowlers by Deliveries")
                st.plotly_chart(fig, use_container_width=True)

            elif "extras conceded" in selected_query.lower():
                fig = px.bar(df, x='team', y='extras_conceded', title=selected_query)
                st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Error executing query: {e}")


st.set_page_config(page_title="Cricket Analytics Dashboard", layout="wide")
st.title("🏏  Cricket Match Analysis Dashboard")

with st.sidebar:
    st.header("Database Connection")
    host = st.text_input("Host", "localhost")
    user = st.text_input("Username", "root")
    password = st.text_input("Password", type="password")
    database = st.text_input("Database", "crickets_db")

    if st.button("Connect"):
        try:
            st.session_state['runner'] = MySQLQueryRunner(host, user, password, database)
            st.success("Connected successfully!")
        except Exception as e:
            st.error(f"Connection failed: {e}")

    if st.button("Disconnect"):
        if 'runner' in st.session_state:
            st.session_state['runner'].close()
            del st.session_state['runner']
            st.success("Disconnected successfully")

tab1, tab2, tab3, tab4 = st.tabs(["Test Matches", "ODI Matches", "T20 Matches", "IPL Matches"])

with tab1:
    query_tab_ui("test_matches", "tab1")

with tab2:
    query_tab_ui("odi_matches", "tab2")

with tab3:
    query_tab_ui("t20_matches","tab3")

with tab4:
    query_tab_ui("ipl_matches","tab4")


