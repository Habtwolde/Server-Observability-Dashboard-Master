import streamlit as st
from services.servers_service import load_servers, get_ingestion_dates
from ui.overview_tab import render_overview
from ui.report_tab import render_report_tab
from ui.expensive_queries_tab import render_expensive_queries_tab
from ui.windows_events_tab import render_windows_events_tab

st.set_page_config(
    page_title="Server Observability Dashboard",
    layout="wide"
)

st.title("Server Observability Dashboard")

# --------------------------------------------------
# Session state bootstrap
# --------------------------------------------------
if "app_scope_applied" not in st.session_state:
    st.session_state["app_scope_applied"] = False

if "selected_server" not in st.session_state:
    st.session_state["selected_server"] = None

if "selected_ingestion_date" not in st.session_state:
    st.session_state["selected_ingestion_date"] = None

# --------------------------------------------------
# Load servers
# --------------------------------------------------
servers_df = load_servers()

if servers_df.empty or "server_name" not in servers_df.columns:
    st.warning("No servers found in sql_diagnostics_files_delta.")
    st.stop()

server_list = servers_df["server_name"].dropna().astype(str).tolist()

# --------------------------------------------------
# Search + server selection UI
# --------------------------------------------------
col_search, col_server, col_ingestion, col_action = st.columns([1.0, 1.4, 1.2, 0.8])

with col_search:
    search_text = st.text_input("Search Server", placeholder="Type to filter...")

filtered_servers = [
    s for s in server_list
    if search_text.lower() in s.lower()
] if search_text else server_list

with col_server:
    if filtered_servers:
        server_placeholder = "Select a server"
        selected_server_ui = st.selectbox(
            "Select Server",
            options=[""] + filtered_servers,
            format_func=lambda x: server_placeholder if x == "" else x,
            index=0 if not st.session_state.get("selected_server") else (
                ([""] + filtered_servers).index(st.session_state["selected_server"])
                if st.session_state["selected_server"] in filtered_servers else 0
            ),
        )
    else:
        selected_server_ui = ""
        st.selectbox(
            "Select Server",
            options=[""],
            format_func=lambda x: "No matching servers",
            index=0,
            disabled=True,
        )

# --------------------------------------------------
# Ingestion date selection depends on chosen server
# --------------------------------------------------
if selected_server_ui:
    ingestion_dates = get_ingestion_dates(selected_server_ui)
else:
    ingestion_dates = []

ingestion_date_options = [str(d) for d in ingestion_dates] if ingestion_dates else []

with col_ingestion:
    if selected_server_ui and ingestion_date_options:
        selected_ingestion_date_ui = st.selectbox(
            "Select Ingestion Date",
            options=[""] + ingestion_date_options,
            format_func=lambda x: "Select ingestion date" if x == "" else x,
            index=0 if not st.session_state.get("selected_ingestion_date") else (
                ([""] + ingestion_date_options).index(st.session_state["selected_ingestion_date"])
                if st.session_state["selected_ingestion_date"] in ingestion_date_options else 0
            ),
        )
    else:
        selected_ingestion_date_ui = ""
        st.selectbox(
            "Select Ingestion Date",
            options=[""],
            format_func=lambda x: "Select server first" if not selected_server_ui else "No ingestion dates found",
            index=0,
            disabled=True,
        )

with col_action:
    st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
    apply_scope = st.button("Load", use_container_width=True)

# --------------------------------------------------
# Apply scope only on explicit user action
# --------------------------------------------------
if apply_scope:
    if not selected_server_ui:
        st.warning("Please select a server.")
        st.stop()

    if not selected_ingestion_date_ui:
        st.warning("Please select an ingestion date.")
        st.stop()

    st.session_state["selected_server"] = selected_server_ui
    st.session_state["selected_ingestion_date"] = selected_ingestion_date_ui
    st.session_state["app_scope_applied"] = True
    st.rerun()

# --------------------------------------------------
# Hold app until scope is explicitly applied
# --------------------------------------------------
if not st.session_state["app_scope_applied"]:
    st.info("Select a server and ingestion date, then click Load to launch the dashboard.")
    st.stop()

selected_server = st.session_state["selected_server"]
selected_ingestion_date = st.session_state["selected_ingestion_date"]

st.caption(
    f"Selected server: {selected_server} | Ingestion date: {selected_ingestion_date}"
)

# --------------------------------------------------
# Tabs render only after scope is applied
# --------------------------------------------------
tab_overview, tab_windows, tab_expensive, tab_report = st.tabs([
    "Overview",
    "Windows Events",
    "Most Expensive Queries",
    "Generate Server health assessment report",
])

with tab_overview:
    render_overview(selected_server, selected_ingestion_date)

with tab_windows:
    render_windows_events_tab(selected_server)

with tab_expensive:
    render_expensive_queries_tab(selected_server, selected_ingestion_date)

with tab_report:
    render_report_tab(selected_server, selected_ingestion_date)