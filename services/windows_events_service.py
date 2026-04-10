# services/windows_events_service.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import streamlit as st

from db.connection import run_query

AGENT_ALERTS_SHEET = "11-SQL Server Agent Alerts"
CPU_HISTORY_SHEET = "45-CPU Utilization History"

@dataclass(frozen=True)
class EventThresholds:
    cpu_warning: float = 85.0
    cpu_critical: float = 95.0

def _get_latest_snapshot(server: str) -> Optional[str]:
    q = f'''
    SELECT CAST(snapshot_date AS string) AS snapshot_date
    FROM btris_dbx.observability.v_latest_sql_diagnostics
    WHERE server_name = '{server}'
    LIMIT 1
    '''
    df = run_query(q)
    if df.empty or "snapshot_date" not in df.columns:
        return None
    return str(df["snapshot_date"].iloc[0])

def _fetch_sheet(server: str, snapshot: str, sheet_name: str) -> pd.DataFrame:
    if not server or not snapshot or not sheet_name:
        return pd.DataFrame()
    q = f'''
    SELECT *
    FROM btris_dbx.observability.sql_diagnostics_bronze
    WHERE server_name = '{server}'
      AND CAST(snapshot_date AS string) = '{snapshot}'
      AND sheet_name = '{sheet_name}'
    '''
    df = run_query(q)
    if df.empty:
        return df
    if "row_json" not in df.columns:
        return df
    records: List[dict] = []
    for _, r in df.iterrows():
        raw = r.get("row_json", None)
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            records.append({})
            continue
        if isinstance(raw, dict):
            records.append(raw)
            continue
        try:
            records.append(json.loads(raw))
        except Exception:
            records.append({"_row_json_parse_error": str(raw)})

    expanded = pd.DataFrame.from_records(records)
    meta_cols = [c for c in ["server_name", "snapshot_date", "sheet_name", "ingested_ts"] if c in df.columns]
    meta = df[meta_cols].reset_index(drop=True) if meta_cols else pd.DataFrame(index=df.index)

    for c in list(expanded.columns):
        if c in meta.columns:
            expanded.rename(columns={c: f"{c}__value"}, inplace=True)

    return pd.concat([meta.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)

def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        key = cand.lower()
        if key in lower_map:
            return lower_map[key]
    return None

def _map_severity_to_level(v: Any) -> str:
    if v is None:
        return "Info"
    try:
        if isinstance(v, str):
            s = v.strip().lower()
            if s in {"critical", "crit", "high", "error", "severe"}:
                return "Error"
            if s in {"warning", "warn", "medium"}:
                return "Warning"
            if s in {"info", "information", "low"}:
                return "Info"
            if s.isdigit():
                v = float(s)
            else:
                return "Info"
        if isinstance(v, (int, float)):
            if float(v) >= 20:
                return "Error"
            if float(v) >= 10:
                return "Warning"
            return "Info"
    except Exception:
        return "Info"
    return "Info"

@st.cache_data(show_spinner=False, ttl=300)
def fetch_windows_events(server: str, thresholds: EventThresholds = EventThresholds()) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    q = f"""
    SELECT
      message,
      event_id,
      provider_name,
      log_name,
      servername,
      time_created,
      container_log,
      level_display_name,
      created_date,
      source_file_name,
      ingestion_date,
      ingested_ts
    FROM btris_dbx.observability.windows_events_bronze
    WHERE lower(servername) = lower('{server}')
    """

    df = run_query(q)

    if df.empty:
        return pd.DataFrame(), {
            "snapshot": None,
            "sources": ["windows_events_bronze"],
            "alerts_total": 0,
            "alerts_error": 0,
            "alerts_warning": 0,
            "alerts_info": 0,
            "cpu_max": None,
            "cpu_spikes_warning": 0,
            "cpu_spikes_critical": 0,
            "server_found": False,
            "requested_server": server,
        }

    events = pd.DataFrame({
        "time_created": df["time_created"].astype(str) if "time_created" in df.columns else "",
        "level": df["level_display_name"].fillna("Info").astype(str) if "level_display_name" in df.columns else "Info",
        "provider": df["provider_name"].astype(str) if "provider_name" in df.columns else "",
        "id": df["event_id"].astype(str) if "event_id" in df.columns else "",
        "message": df["message"].astype(str) if "message" in df.columns else "",
        "snapshot_date": df["ingestion_date"].astype(str) if "ingestion_date" in df.columns else "",
        "log_name": df["log_name"].astype(str) if "log_name" in df.columns else "",
        "container_log": df["container_log"].astype(str) if "container_log" in df.columns else "",
        "servername": df["servername"].astype(str) if "servername" in df.columns else "",
    })

    if not events.empty and "time_created" in events.columns:
        events = events.sort_values(by=["time_created"], ascending=[False], kind="mergesort").reset_index(drop=True)

    def _count_level(df_: pd.DataFrame, lvl: str) -> int:
        if df_.empty:
            return 0
        return int(df_["level"].astype(str).str.lower().eq(lvl.lower()).sum())

    summary = {
        "snapshot": None,
        "sources": ["windows_events_bronze"],
        "alerts_total": int(len(events)),
        "alerts_error": _count_level(events, "Error"),
        "alerts_warning": _count_level(events, "Warning"),
        "alerts_info": _count_level(events, "Information") + _count_level(events, "Info"),
        "cpu_max": None,
        "cpu_spikes_warning": 0,
        "cpu_spikes_critical": 0,
    }

    summary["server_found"] = True
    summary["requested_server"] = server

    return events, summary

def build_summary_context(summary: Dict[str, Any]) -> str:
    total = summary.get("alerts_total", 0)
    err = summary.get("alerts_error", 0)
    warn = summary.get("alerts_warning", 0)
    info = summary.get("alerts_info", 0)

    return (
        f"Windows Events found: {total}. "
        f"Errors: {err}. Warnings: {warn}. Informational: {info}. "
        f"Source: uploaded Windows Events CSV."
    )