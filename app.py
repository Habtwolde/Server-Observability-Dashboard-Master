import streamlit as st
from services.servers_service import load_servers, get_ingestion_dates
from ui.overview_tab import render_overview
from ui.report_tab import render_report_tab
from ui.expensive_queries_tab import render_expensive_queries_tab

st.set_page_config(
    page_title="Server Observability Dashboard",
    layout="wide"
)

st.title("Server Observability Dashboard")

# -------------------------
# Load servers
# -------------------------
servers_df = load_servers()

if servers_df.empty or "server_name" not in servers_df.columns:
    st.warning("No servers found in v_latest_sql_diagnostics.")
    st.stop()

server_list = servers_df["server_name"].dropna().astype(str).tolist()

# -------------------------
# Search + Server select
# -------------------------
col_search, col_server, col_ingestion = st.columns([1.1, 1.5, 1.4])

with col_search:
    search_text = st.text_input("Search Server", placeholder="Type to filter...")

filtered_servers = [
    s for s in server_list
    if search_text.lower() in s.lower()
] if search_text else server_list

if not filtered_servers:
    st.warning("No servers match the current search.")
    st.stop()

with col_server:
    default_server = st.session_state.get("selected_server")
    if default_server not in filtered_servers:
        default_server = filtered_servers[0]

    selected_server = st.selectbox(
        "Select Server",
        filtered_servers,
        index=filtered_servers.index(default_server) if default_server in filtered_servers else 0,
    )

st.session_state["selected_server"] = selected_server

# -------------------------
# Ingestion date select
# -------------------------
ingestion_dates = get_ingestion_dates(selected_server)

if ingestion_dates is None or len(ingestion_dates) == 0:
    st.warning(f"No ingestion dates found for server '{selected_server}'.")
    st.stop()

ingestion_date_options = [str(d) for d in ingestion_dates]

with col_ingestion:
    default_ingestion_date = st.session_state.get("selected_ingestion_date")
    if default_ingestion_date not in ingestion_date_options:
        default_ingestion_date = ingestion_date_options[0]

    selected_ingestion_date = st.selectbox(
        "Select Ingestion Date",
        ingestion_date_options,
        index=ingestion_date_options.index(default_ingestion_date)
        if default_ingestion_date in ingestion_date_options else 0,
    )

st.session_state["selected_ingestion_date"] = selected_ingestion_date

# Optional small status line
st.caption(
    f"Selected server: {selected_server} | Ingestion date: {selected_ingestion_date}"
)

# -------------------------
# Tabs
# -------------------------
tab_overview, tab_expensive, tab_report = st.tabs([
    "Overview",
    "Most Expensive Queries",
    "Generate Server health assessment report",
])

with tab_overview:
    render_overview(selected_server)

with tab_expensive:
    render_expensive_queries_tab(selected_server)

with tab_report:
    render_report_tab(selected_server)