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
    snapshot = _get_latest_snapshot(server)
    if not snapshot:
        return pd.DataFrame(), {
            "snapshot": None, "sources": [],
            "alerts_total": 0, "alerts_error": 0, "alerts_warning": 0, "alerts_info": 0,
            "cpu_max": None, "cpu_spikes_warning": 0, "cpu_spikes_critical": 0,
        }

    alerts_df = _fetch_sheet(server, snapshot, AGENT_ALERTS_SHEET)
    alerts_events = pd.DataFrame()

    if not alerts_df.empty:
        cols = list(alerts_df.columns)
        name_col = _pick_col(cols, ["name", "alert name", "alert"])
        provider_col = _pick_col(cols, ["event_source", "event source", "source", "provider"])
        severity_col = _pick_col(cols, ["severity", "alert severity", "error severity", "level"])
        date_col = _pick_col(cols, ["last_occurrence_date", "last occurrence date", "last_occurrence", "last occurrence"])
        time_col = _pick_col(cols, ["last_occurrence_time", "last occurrence time", "occurrence time", "time"])

        if date_col and time_col:
            time_created = alerts_df[date_col].astype(str).str.strip() + " " + alerts_df[time_col].astype(str).str.strip()
        elif date_col:
            time_created = alerts_df[date_col].astype(str)
        else:
            time_created = pd.Series([snapshot] * len(alerts_df))

        levels = alerts_df[severity_col].map(_map_severity_to_level) if severity_col else pd.Series(["Info"] * len(alerts_df))
        provider = alerts_df[provider_col].astype(str) if provider_col else pd.Series(["SQL Agent"] * len(alerts_df))
        alert_name = alerts_df[name_col].astype(str) if name_col else pd.Series(["SQL Agent Alert"] * len(alerts_df))

        alerts_events = pd.DataFrame({
            "time_created": time_created,
            "level": levels,
            "provider": provider,
            "id": alert_name,
            "message": "SQL Agent Alert: " + alert_name,
            "source_sheet": AGENT_ALERTS_SHEET,
            "snapshot_date": snapshot,
        })

    cpu_df = _fetch_sheet(server, snapshot, CPU_HISTORY_SHEET)
    cpu_events = pd.DataFrame()
    cpu_max = None
    cpu_warn = 0
    cpu_crit = 0

    if not cpu_df.empty:
        cols = list(cpu_df.columns)
        cpu_col = _pick_col(cols, ["SQL Server Process CPU Utilization", "sql server process cpu utilization", "sql cpu", "sqlserver cpu"])
        time_col = _pick_col(cols, ["Event Time", "event time", "time", "timestamp"])

        if cpu_col:
            cpu_series = pd.to_numeric(cpu_df[cpu_col], errors="coerce")
            cpu_max = float(cpu_series.max()) if cpu_series.notna().any() else None

            level = pd.Series(["Info"] * len(cpu_df))
            level = level.where(cpu_series < thresholds.cpu_warning, "Warning")
            level = level.where(cpu_series < thresholds.cpu_critical, "Error")

            is_spike = cpu_series >= thresholds.cpu_warning
            cpu_warn = int(((cpu_series >= thresholds.cpu_warning) & (cpu_series < thresholds.cpu_critical)).sum())
            cpu_crit = int((cpu_series >= thresholds.cpu_critical).sum())

            when = cpu_df[time_col].astype(str) if time_col else pd.Series([snapshot] * len(cpu_df))
            cpu_events = pd.DataFrame({
                "time_created": when[is_spike].reset_index(drop=True),
                "level": level[is_spike].reset_index(drop=True),
                "provider": ["Performance Monitor"] * int(is_spike.sum()),
                "id": ["CPU_SPIKE"] * int(is_spike.sum()),
                "message": (cpu_series[is_spike].round(2).astype(str).radd("SQL Server CPU reached ").add("%")).reset_index(drop=True),
                "source_sheet": CPU_HISTORY_SHEET,
                "snapshot_date": snapshot,
            })

    events = pd.concat([alerts_events, cpu_events], ignore_index=True)
    if not events.empty and "time_created" in events.columns:
        events = events.sort_values(by=["time_created", "level"], ascending=[False, True], kind="mergesort").reset_index(drop=True)

    def _count_level(df: pd.DataFrame, lvl: str) -> int:
        if df.empty:
            return 0
        return int((df["level"] == lvl).sum())

    alerts_total = int(len(alerts_events)) if not alerts_events.empty else 0
    summary = {
        "snapshot": snapshot,
        "sources": [s for s, ok in [
            (AGENT_ALERTS_SHEET, alerts_total > 0),
            (CPU_HISTORY_SHEET, (cpu_warn + cpu_crit) > 0),
        ] if ok],
        "alerts_total": alerts_total,
        "alerts_error": _count_level(alerts_events, "Error"),
        "alerts_warning": _count_level(alerts_events, "Warning"),
        "alerts_info": _count_level(alerts_events, "Info"),
        "cpu_max": cpu_max,
        "cpu_spikes_warning": cpu_warn,
        "cpu_spikes_critical": cpu_crit,
    }

    return events, summary

def build_summary_context(summary: Dict[str, Any]) -> str:
    snap = summary.get("snapshot")
    if not snap:
        return "No snapshot was found for this server in v_latest_sql_diagnostics."

    alerts_total = summary.get("alerts_total", 0)
    a_err = summary.get("alerts_error", 0)
    a_warn = summary.get("alerts_warning", 0)
    cpu_max = summary.get("cpu_max", None)
    cpu_w = summary.get("cpu_spikes_warning", 0)
    cpu_c = summary.get("cpu_spikes_critical", 0)

    parts: List[str] = []
    parts.append(f"Latest snapshot: {snap}.")
    parts.append(f"SQL Agent alerts: {alerts_total} (Error: {a_err}, Warning: {a_warn}).")

    if cpu_max is None:
        parts.append("CPU telemetry was not available for this snapshot.")
    else:
        parts.append(f"Max SQL Server CPU observed: {cpu_max:.2f}%.")
        if (cpu_w + cpu_c) == 0:
            parts.append("No CPU spike incidents were detected using the current thresholds.")
        else:
            parts.append(f"CPU spike incidents detected: {cpu_w + cpu_c} (Critical: {cpu_c}, Warning: {cpu_w}).")

    parts.append("Note: This tab synthesizes an Event Viewer-style view from the available diagnostics sheets; it is not a direct Windows Event Log ingest.")
    return " ".join(parts)
