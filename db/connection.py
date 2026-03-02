import os
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql

# Databricks Apps internal identity
@st.cache_resource
def get_workspace_client():
    return WorkspaceClient()

# warehouse id must come from ENV not secrets
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()
if not WAREHOUSE_ID:
    WAREHOUSE_ID = "47bde9279fec4222"  # fallback


@st.cache_data(ttl=60)
def run_query(query: str) -> pd.DataFrame:
    print("EXECUTING SQL:", query)
    w = get_workspace_client()

    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s",
    )

    state = resp.status.state if resp.status else None
    if state != dbsql.StatementState.SUCCEEDED:
        return pd.DataFrame()

    rows = resp.result.data_array if (resp.result and resp.result.data_array) else []

    cols = []
    if resp.manifest and resp.manifest.schema and resp.manifest.schema.columns:
        cols = [c.name for c in resp.manifest.schema.columns]

    if not cols:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=cols)