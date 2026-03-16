import io
from db.connection import get_workspace_client, run_query


def get_latest_file_path(server_name: str, ingestion_date: str | None = None) -> str:
    """
    Return SQLDiagnostics file path for the selected server and ingestion snapshot.
    """

    if ingestion_date:
        query = f"""
        SELECT file_path
        FROM btris_dbx.observability.sql_diagnostics_files_delta
        WHERE server_name = '{server_name}'
        AND ingestion_date = DATE('{ingestion_date}')
        ORDER BY snapshot_date DESC
        LIMIT 1
        """
    else:
        query = f"""
        SELECT file_path
        FROM btris_dbx.observability.sql_diagnostics_files_delta
        WHERE server_name = '{server_name}'
        ORDER BY snapshot_date DESC
        LIMIT 1
        """

    df = run_query(query)

    if df.empty:
        return None

    return df.iloc[0]["file_path"]


def load_file_bytes(path: str) -> bytes:
    """
    Download file from DBFS or UC Volume using Databricks Files API
    """
    w = get_workspace_client()

    # dbfs:/ → API path
    if path.startswith("dbfs:/"):
        api_path = path.replace("dbfs:/", "/")
    else:
        api_path = path  # already /Volumes/...

    resp = w.files.download(api_path)

    # resp.contents is a binary stream
    return resp.contents.read()