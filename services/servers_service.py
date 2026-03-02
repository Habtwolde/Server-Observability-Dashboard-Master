import streamlit as st
from db.connection import run_query

@st.cache_data(ttl=300)

def load_servers():
    q = """
    SELECT DISTINCT server_name
    FROM btris_dbx.observability.v_latest_sql_diagnostics
    ORDER BY server_name
    """
    return run_query(q)

def resolve_latest_snapshot(server_name: str) -> str | None:
    q = f"""
    SELECT snapshot_date
    FROM btris_dbx.observability.v_latest_sql_diagnostics
    WHERE server_name = '{server_name}'
    LIMIT 1
    """
    df = run_query(q)
    if df.empty:
        return None
    return df["snapshot_date"].iloc[0]