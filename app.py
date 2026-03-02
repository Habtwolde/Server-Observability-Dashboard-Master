import streamlit as st
from services.servers_service import load_servers
from ui.overview_tab import render_overview
from ui.report_tab import render_report_tab
from ui.expensive_queries_tab import render_expensive_queries_tab

st.set_page_config(
    page_title="Server Observability Dashboard",
    layout="wide"
)

st.title("Server Observability Dashboard")

# -------------------------
# Load servers (from view)
# -------------------------
servers_df = load_servers()

if servers_df.empty or "server_name" not in servers_df.columns:
    st.warning("No servers found in v_latest_sql_diagnostics.")
    st.stop()

server_list = servers_df["server_name"].dropna().astype(str).tolist()

# -------------------------
# Search + Select
# -------------------------
col_search, col_select = st.columns([1.2, 1.8])

with col_search:
    search_text = st.text_input("Search Server", placeholder="Type to filter...")

filtered_servers = [
    s for s in server_list
    if search_text.lower() in s.lower()
] if search_text else server_list

with col_select:
    selected_server = st.selectbox(
        "Select Server",
        filtered_servers
    )

# -------------------------
# Tabs (Overview + Report)
# -------------------------
tab_overview, tab_expensive, tab_report = st.tabs(
    ["Overview", "Most Expensive Queries", "Generate Server health assessment report"]
)

with tab_overview:
    render_overview(selected_server)

with tab_expensive:
    render_expensive_queries_tab(selected_server)

with tab_report:
    render_report_tab(selected_server)
