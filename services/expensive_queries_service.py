# services/expensive_queries_service.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

from services import metrics_service


# ----------------------------
# Domain model
# ----------------------------
@dataclass(frozen=True)
class QueryTypeOption:
    label: str          # user friendly label shown in dropdown
    sheet_name: str     # exact bronze sheet_name
    kind: str           # cpu | io | elapsed | other


# ----------------------------
# Sheet discovery
# ----------------------------
_EXPENSIVE_SHEET_PATTERNS: List[Tuple[str, str]] = [
    # (regex, kind)
    (r"\bTop\s+Worker\s+Time\s+Queries\b", "cpu"),
    (r"\bTop\s+Logical\s+Reads\s+Queries\b", "io"),
    (r"\bTop\s+Avg\s+Elapsed\s+Time\b", "elapsed"),
    (r"\bTop\s+IO\s+Statements\b", "io"),
    # broader catch-alls (still expensive queries)
    (r"\bTop\s+.*\bQueries\b", "other"),
    (r"\bTop\s+.*\bStatements\b", "other"),
]


def _friendly_label(sheet_name: str, kind: str) -> str:
    s = str(sheet_name or "").strip()
    if kind == "cpu":
        return "Most expensive queries by CPU (Worker Time)"
    if kind == "io":
        # "logical reads" and "io statements" both land here
        if re.search(r"logical\s+reads", s, flags=re.IGNORECASE):
            return "Most expensive queries by IO (Logical Reads)"
        if re.search(r"\bIO\s+Statements\b", s, flags=re.IGNORECASE):
            return "Most expensive queries by IO (IO Statements)"
        return "Most expensive queries by IO"
    if kind == "elapsed":
        return "Slowest queries (Avg Elapsed Time)"
    # fall back to the raw sheet title for anything else
    return s


@st.cache_data(show_spinner=False, ttl=300)
def list_expensive_query_types(server_name: str) -> List[QueryTypeOption]:
    """
    Discover which 'Top ...' expensive-query-related sheets exist for a server.
    This is intentionally data-driven from delta, not hard-coded to 2 types.
    """
    if not server_name:
        return []

    available = metrics_service.list_available_sheets_any(server_name)
    if not available:
        return []

    picked: List[QueryTypeOption] = []
    seen = set()

    for sheet in available:
        sheet_str = str(sheet)
        kind = None
        for pat, k in _EXPENSIVE_SHEET_PATTERNS:
            if re.search(pat, sheet_str, flags=re.IGNORECASE):
                kind = k
                break
        if not kind:
            continue

        # de-dup exact sheet_name
        if sheet_str.lower() in seen:
            continue
        seen.add(sheet_str.lower())

        picked.append(
            QueryTypeOption(
                label=_friendly_label(sheet_str, kind),
                sheet_name=sheet_str,
                kind=kind,
            )
        )

    # Sorting: show the most important buckets first, then alphabetically
    priority = {"cpu": 0, "io": 1, "elapsed": 2, "other": 3}
    picked.sort(key=lambda o: (priority.get(o.kind, 9), o.label.lower(), o.sheet_name.lower()))
    return picked


# ----------------------------
# Data access
# ----------------------------
@st.cache_data(show_spinner=False, ttl=300)
def fetch_latest_expensive_queries(server_name: str, sheet_name: str) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Fetch the latest snapshot rows for the expensive-query sheet. Expands row_json.
    Returns (df, snapshot_used).
    """
    if not server_name or not sheet_name:
        return pd.DataFrame(), None

    df, snap = metrics_service._fetch_sheet_latest(server_name, sheet_name)
    if df is None or df.empty:
        return pd.DataFrame(), snap
    return df, snap


def pick_query_text_column(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty:
        return None
    candidates = [
        "Short Query Text",
        "Query Text",
        "Statement Text",
        "SQL Text",
        "Text",
        "query_text",
        "short_query_text",
        "statement_text",
    ]
    return metrics_service._pick_column(list(df.columns), candidates)


def pick_sort_metric_column(df: pd.DataFrame, kind: str) -> Optional[str]:
    """
    Pick a metric column to sort by for 'top' ordering depending on query kind.
    """
    if df is None or df.empty:
        return None

    if kind == "cpu":
        cands = ["Total Worker Time", "Avg Worker Time", "Worker Time", "total_worker_time", "avg_worker_time"]
    elif kind == "io":
        cands = ["Total Logical Reads", "Avg Logical Reads", "Logical Reads", "total_logical_reads", "avg_logical_reads",
                 "Total Physical Reads", "Avg Physical Reads", "Physical Reads"]
    elif kind == "elapsed":
        cands = ["Avg Elapsed Time", "Total Elapsed Time", "Elapsed Time", "avg_elapsed_time", "total_elapsed_time"]
    else:
        cands = ["Total Worker Time", "Total Logical Reads", "Avg Elapsed Time", "Execution Count"]

    return metrics_service._pick_column(list(df.columns), cands)


def build_query_dropdown_items(df: pd.DataFrame, *, query_col: str, limit: int = 200) -> List[str]:
    """
    Build stable dropdown display strings for each row, keeping the row index.
    """
    if df is None or df.empty or not query_col:
        return []

    # Keep original ordering; caller may sort df prior to passing it here
    items: List[str] = []
    for idx, v in enumerate(df[query_col].astype(str).fillna("").tolist()):
        v = v.strip()
        if not v or v.lower() == "nan":
            v = "<blank query text>"
        # truncate but keep readable
        v_short = (v[:160] + "…") if len(v) > 160 else v
        items.append(f"{idx+1:03d} — {v_short}")
        if len(items) >= int(limit):
            break
    return items
