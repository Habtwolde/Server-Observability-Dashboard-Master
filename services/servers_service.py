import streamlit as st
from db.connection import run_query
import pandas as pd


@st.cache_data(ttl=300)
def load_servers() -> pd.DataFrame:
    q = """
    SELECT DISTINCT server_name
    FROM btris_dbx.observability.sql_diagnostics_files_delta
    ORDER BY server_name
    """
    return run_query(q)


@st.cache_data(ttl=300)
def get_ingestion_dates(server_name: str) -> list[str]:
    safe_server = str(server_name).replace("'", "''")
    q = f"""
    SELECT DISTINCT ingestion_date
    FROM btris_dbx.observability.sql_diagnostics_files_delta
    WHERE server_name = '{safe_server}'
    ORDER BY ingestion_date DESC
    """
    df = run_query(q)

    if df.empty or "ingestion_date" not in df.columns:
        return []

    return [row for row in df["ingestion_date"].tolist() if row is not None]