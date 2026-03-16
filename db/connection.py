import os
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql


@st.cache_resource
def get_workspace_client():
    """Return a cached Databricks workspace client."""
    return WorkspaceClient()


# Warehouse ID should preferably come from environment variables.
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()
if not WAREHOUSE_ID:
    WAREHOUSE_ID = "47bde9279fec4222"  # fallback for development only


@st.cache_data(ttl=60)
def run_query(query: str) -> pd.DataFrame:
    """
    Execute a SQL query against the configured Databricks SQL Warehouse
    and return the result as a pandas DataFrame.

    Raises:
        ValueError: If the query is empty.
        RuntimeError: If the warehouse execution fails.
    """
    if not query or not str(query).strip():
        raise ValueError("Query must not be empty.")

    print("EXECUTING SQL:", query)
    w = get_workspace_client()

    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s",
    )

    state = resp.status.state if resp.status else None
    if state != dbsql.StatementState.SUCCEEDED:
        error_message = "Databricks SQL query failed."
        if resp.status and getattr(resp.status, "error", None):
            error_message = f"{error_message} {resp.status.error}"
        raise RuntimeError(error_message)

    rows = resp.result.data_array if (resp.result and resp.result.data_array) else []

    cols = []
    if resp.manifest and resp.manifest.schema and resp.manifest.schema.columns:
        cols = [c.name for c in resp.manifest.schema.columns]

    if not cols:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=cols)