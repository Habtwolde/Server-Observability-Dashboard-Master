# services/sheet_resolver.py
from __future__ import annotations

import re
from typing import Iterable, Optional, List

from db.connection import run_query


def list_available_sheets(server_name: str, snapshot: str) -> List[str]:
    safe_server = str(server_name).replace("'", "''")
    safe_snapshot = str(snapshot).replace("'", "''")

    q = f"""
    SELECT DISTINCT sheet_name
    FROM btris_dbx.observability.sql_diagnostics_bronze
    WHERE server_name = '{safe_server}'
    AND CAST(snapshot_date AS string) = '{safe_snapshot}'
    """
    df = run_query(q)
    if df.empty or "sheet_name" not in df.columns:
        return []
    return df["sheet_name"].dropna().astype(str).tolist()


def resolve_sheet_name(available: Iterable[str], patterns: List[str]) -> Optional[str]:
    """
    Return the first available sheet_name that matches any regex pattern (case-insensitive).
    """
    avail = list(available)
    for pat in patterns:
        rx = re.compile(pat, flags=re.IGNORECASE)
        for s in avail:
            if rx.search(s):
                return s
    return None