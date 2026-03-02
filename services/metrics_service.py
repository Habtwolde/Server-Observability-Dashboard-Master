# services/metrics_service.py

from __future__ import annotations

import json
import re
from typing import Optional, Dict, Any, List

import pandas as pd
import streamlit as st

from db.connection import run_query


# ----------------------------
# Helper utilities
# ----------------------------
def _pick_column(existing_cols: List[str], candidates: List[str]) -> Optional[str]:
    """Return first candidate that exists in existing_cols (case-insensitive)."""
    lower_map = {c.lower(): c for c in existing_cols}
    for cand in candidates:
        if not cand:
            continue
        key = cand.lower()
        if key in lower_map:
            return lower_map[key]
    return None


def _get_latest_snapshot(server: str) -> Optional[str]:
    q = f"""
    SELECT snapshot_date
    FROM btris_dbx.observability.v_latest_sql_diagnostics
    WHERE server_name = '{server}'
    LIMIT 1
    """
    df = run_query(q)
    if df.empty or "snapshot_date" not in df.columns:
        return None
    return df["snapshot_date"].iloc[0]

def _get_latest_snapshot_for_sheet(server_name: str, sheet_name: str) -> Optional[str]:
    """Return latest snapshot_date (as string) where a given sheet has rows for this server."""
    q = f"""
    SELECT MAX(CAST(snapshot_date AS string)) AS snapshot_date
    FROM btris_dbx.observability.sql_diagnostics_bronze
    WHERE server_name = '{server_name}'
      AND sheet_name = '{sheet_name}'
    """
    df = run_query(q)
    if df.empty or "snapshot_date" not in df.columns:
        return None
    v = df["snapshot_date"].iloc[0]
    if v is None:
        return None
    return str(v)


def list_available_sheets_any(server_name: str) -> List[str]:
    """List distinct sheet names available for a server across all snapshots."""
    q = f"""
    SELECT DISTINCT sheet_name
    FROM btris_dbx.observability.sql_diagnostics_bronze
    WHERE server_name = '{server_name}'
    """
    df = run_query(q)
    if df.empty or "sheet_name" not in df.columns:
        return []
    return df["sheet_name"].dropna().astype(str).tolist()


def _fetch_sheet_latest(server_name: str, sheet_name: str) -> tuple[pd.DataFrame, Optional[str]]:
    """Fetch latest rows for a server+sheet across all snapshots. Returns (df, snapshot_used)."""
    snap = _get_latest_snapshot_for_sheet(server_name, sheet_name)
    if not snap:
        return pd.DataFrame(), None
    try:
        df = _fetch_sheet(server_name, snap, sheet_name)
        return df, snap
    except Exception:
        return pd.DataFrame(), snap



def list_available_sheets(server_name: str, snapshot: str) -> List[str]:
    q = f"""
    SELECT DISTINCT sheet_name
    FROM btris_dbx.observability.sql_diagnostics_bronze
    WHERE server_name = '{server_name}'
      AND CAST(snapshot_date AS string) = '{snapshot}'
    """
    df = run_query(q)
    if df.empty or "sheet_name" not in df.columns:
        return []
    return df["sheet_name"].dropna().astype(str).tolist()


def resolve_sheet_name(available: List[str], patterns: List[str]) -> Optional[str]:
    """Return first sheet name matching any regex pattern (case-insensitive)."""
    for pat in patterns:
        rx = re.compile(pat, flags=re.IGNORECASE)
        for s in available:
            if rx.search(s):
                return s
    return None


def _safe_max_numeric(series: pd.Series) -> Optional[float]:
    try:
        s = pd.to_numeric(series, errors="coerce")
        if s.dropna().empty:
            return None
        return float(s.max())
    except Exception:
        return None


def _parse_version_blob(v: str) -> dict:
    """
    Normalize SQL Server banner into clean executive format.
    Example output:
      SQL Server 2022 CU23
    """
    if not v or not isinstance(v, str):
        return {"sql_banner": None}

    first_line = v.strip().splitlines()[0].strip()

    # Extract major version year
    year_match = re.search(r"SQL Server\s+(\d{4})", first_line, flags=re.IGNORECASE)
    year = year_match.group(1) if year_match else ""

    # Extract CU number if present
    cu_match = re.search(r"CU(\d+)", first_line, flags=re.IGNORECASE)
    cu = f"CU{cu_match.group(1)}" if cu_match else ""

    if year and cu:
        return {"sql_banner": f"SQL Server {year} {cu}"}
    if year:
        return {"sql_banner": f"SQL Server {year}"}

    return {"sql_banner": "SQL Server"}


# ----------------------------
# Bronze sheet fetch (expands row_json)
# ----------------------------
def _fetch_sheet(server: str, snapshot: str, sheet_name: str) -> pd.DataFrame:
    """
    Fetch all rows for a specific sheet for a server+snapshot from bronze.

    IMPORTANT:
    Bronze stores the original Excel rows inside a JSON string column (row_json),
    so we expand it into real DataFrame columns here.
    """
    if not sheet_name:
        return pd.DataFrame()

    q = f"""
    SELECT *
    FROM btris_dbx.observability.sql_diagnostics_bronze
    WHERE server_name = '{server}'
      AND CAST(snapshot_date AS string) = '{snapshot}'
      AND sheet_name = '{sheet_name}'
    """
    df = run_query(q)
    if df.empty:
        return df

    # Future-proof: if already flattened, return as-is
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

    # keep key metadata columns too
    meta_cols = [c for c in ["server_name", "snapshot_date", "sheet_name", "ingested_ts"] if c in df.columns]
    meta = df[meta_cols].reset_index(drop=True) if meta_cols else pd.DataFrame(index=df.index)

    # avoid collisions
    for c in list(expanded.columns):
        if c in meta.columns:
            expanded.rename(columns={c: f"{c}__value"}, inplace=True)

    out = pd.concat([meta.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)
    return out


# ----------------------------
# Metric extractors
# ----------------------------
def _extract_cpu_max(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    cols = list(df.columns)
    cpu_candidates = [
        "SQL Server Process CPU Utilization",
        "sql_server_process_cpu_utilization",
        "sql server process cpu utilization",
        "sqlservercpu",
        "cpu_percent",
        "cpu",
        "max_cpu_utilization",
        "max_cpu",
        "SQL Server CPU",
    ]
    cpu_col = _pick_column(cols, cpu_candidates)
    if not cpu_col:
        return None
    return _safe_max_numeric(df[cpu_col])


def _extract_memory_pct(df_proc_mem: pd.DataFrame, df_sys_mem: pd.DataFrame) -> Optional[float]:
    if df_proc_mem.empty or df_sys_mem.empty:
        return None

    proc_cols = list(df_proc_mem.columns)
    sys_cols = list(df_sys_mem.columns)

    proc_candidates = [
        "SQL Server Memory Usage (MB)",
        "sql_server_memory_usage_mb",
        "sql_server_memory_mb",
        "sql_memory_mb",
        "SQL Server Memory Usage",
        "sql_memory_usage_mb",
        "sql_memory_used_mb",
    ]
    phys_candidates = [
        "Physical Memory (MB)",
        "physical_memory_mb",
        "total_physical_memory_mb",
        "Total Physical Memory (MB)",
        "physical_memory",
    ]

    proc_col = _pick_column(proc_cols, proc_candidates)
    phys_col = _pick_column(sys_cols, phys_candidates)
    if not proc_col or not phys_col:
        return None

    try:
        sql_used = pd.to_numeric(df_proc_mem[proc_col], errors="coerce").dropna()
        phys_total = pd.to_numeric(df_sys_mem[phys_col], errors="coerce").dropna()
        if sql_used.empty or phys_total.empty:
            return None
        sql_val = float(sql_used.iloc[-1])
        phys_val = float(phys_total.iloc[-1])
        if phys_val == 0:
            return None
        return float(sql_val / phys_val * 100.0)
    except Exception:
        return None


def _extract_ple(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    cols = list(df.columns)
    ple_candidates = ["Page Life Expectancy", "page_life_expectancy", "PLE", "ple_seconds"]
    ple_col = _pick_column(cols, ple_candidates)
    if not ple_col:
        return None
    try:
        s = pd.to_numeric(df[ple_col], errors="coerce").dropna()
        if s.empty:
            return None
        return float(s.min())
    except Exception:
        return None


def _extract_instance_info(
    df_server_props: pd.DataFrame,
    df_hw: pd.DataFrame,
    df_host: pd.DataFrame,
    df_version: pd.DataFrame,
) -> Dict[str, Any]:
    out = {
        "sql_banner": None,
        "edition": None,
        "cpu_count": None,
        "total_ram_mb": None,
        "os_name": None,
    }

    # Version banner from 1-Version Info
    if isinstance(df_version, pd.DataFrame) and not df_version.empty:
        for c in df_version.columns:
            if str(c).strip().lower() == "sql server and os version info":
                blob = df_version[c].dropna().astype(str)
                if not blob.empty:
                    out.update(_parse_version_blob(blob.iloc[0]))
                break

    # Edition from Server Properties
    if isinstance(df_server_props, pd.DataFrame) and not df_server_props.empty:
        cols = {str(c).strip().lower(): c for c in df_server_props.columns}
        if "edition" in cols:
            v = df_server_props[cols["edition"]].dropna()
            if not v.empty:
                raw = str(v.iloc[-1])
                clean = raw.split(":")[0].strip()  # hard-trim after colon

                if "Enterprise" in clean:
                    out["edition"] = "Enterprise Edition"
                elif "Standard" in clean:
                    out["edition"] = "Standard Edition"
                elif "Developer" in clean:
                    out["edition"] = "Developer Edition"
                else:
                    out["edition"] = clean

    # Hardware (CPU/RAM) from Hardware Info
    if isinstance(df_hw, pd.DataFrame) and not df_hw.empty:
        cols = {str(c).strip().lower(): c for c in df_hw.columns}

        # CPU
        for key in ["logical cpu count", "scheduler count", "physical core count"]:
            if key in cols:
                s = pd.to_numeric(df_hw[cols[key]], errors="coerce").dropna()
                if not s.empty:
                    out["cpu_count"] = int(s.iloc[-1])
                    break

        # RAM
        if "physical memory (mb)" in cols:
            s = pd.to_numeric(df_hw[cols["physical memory (mb)"]], errors="coerce").dropna()
            if not s.empty:
                out["total_ram_mb"] = int(s.iloc[-1])

    # OS from Host Info
    if isinstance(df_host, pd.DataFrame) and not df_host.empty:
        cols = {str(c).strip().lower(): c for c in df_host.columns}
        if "host_distribution" in cols:
            v = df_host[cols["host_distribution"]].dropna()
            if not v.empty:
                out["os_name"] = str(v.iloc[-1])

    return out


def _extract_workload_summary(df_meq: pd.DataFrame) -> Dict[str, Any]:
    out = {"top_query_count": 0, "max_duration_s": None, "max_logical_reads": None}
    if df_meq.empty:
        return out

    out["top_query_count"] = int(df_meq.shape[0])

    cols = list(df_meq.columns)
    dur_cands = ["duration_seconds", "avg_duration", "avg_duration_seconds", "duration_s", "avg_duration_ms"]
    reads_cands = ["logical_reads", "logical_read", "logical_reads_count", "reads"]

    dur_col = _pick_column(cols, dur_cands)
    reads_col = _pick_column(cols, reads_cands)

    if dur_col:
        try:
            s = pd.to_numeric(df_meq[dur_col], errors="coerce").dropna()
            if not s.empty:
                val = float(s.max())
                if val > 10000:  # likely ms
                    val = val / 1000.0
                out["max_duration_s"] = val
        except Exception:
            out["max_duration_s"] = None

    if reads_col:
        try:
            s = pd.to_numeric(df_meq[reads_col], errors="coerce").dropna()
            if not s.empty:
                out["max_logical_reads"] = int(s.max())
        except Exception:
            out["max_logical_reads"] = None

    return out


# ----------------------------
# Public API
# ----------------------------
@st.cache_data(ttl=30)

# ----------------------------
# I/O extractor (safe, flattened columns)
# ----------------------------
def _extract_io_stats(
    df_drive: pd.DataFrame,
    df_file: pd.DataFrame,
    df_db: pd.DataFrame,
) -> Dict[str, Any]:

    out = {
        "drive_max_overall_latency_ms": None,
        "drive_avg_overall_latency_ms": None,
        "avg_read_latency_ms": None,
        "avg_write_latency_ms": None,
        "avg_io_latency_ms": None,
        "total_read_io_mb": None,
        "total_write_io_mb": None,
        "total_io_mb": None,
    }

    # ---- 29-Drive Level Latency ----
    if not df_drive.empty:
        if "Overall Latency" in df_drive.columns:
            s = pd.to_numeric(df_drive["Overall Latency"], errors="coerce").dropna()
            if not s.empty:
                out["drive_max_overall_latency_ms"] = float(s.max())
                out["drive_avg_overall_latency_ms"] = float(s.mean())

    # ---- 30-IO Latency by File ----
    if not df_file.empty:
        for col, key in [
            ("avg_read_latency_ms", "avg_read_latency_ms"),
            ("avg_write_latency_ms", "avg_write_latency_ms"),
            ("avg_io_latency_ms", "avg_io_latency_ms"),
        ]:
            if col in df_file.columns:
                s = pd.to_numeric(df_file[col], errors="coerce").dropna()
                if not s.empty:
                    out[key] = float(s.mean())

    # ---- 37-IO Usage By Database ----
    if not df_db.empty:
        for col, key in [
            ("Read I/O (MB)", "total_read_io_mb"),
            ("Write I/O (MB)", "total_write_io_mb"),
            ("Total I/O (MB)", "total_io_mb"),
        ]:
            if col in df_db.columns:
                s = pd.to_numeric(df_db[col], errors="coerce").dropna()
                if not s.empty:
                    out[key] = float(s.sum())

    return out




def _extract_waits(server_name: str, snapshot: str | None = None, available_sheets: Optional[List[str]] = None) -> pd.DataFrame:
    """Return Top Waits as a DataFrame, selecting the most recent non-empty waits sheet.

    Important: The dashboard should not depend on a single snapshot_date. This function:
      1) Finds candidate wait-related sheet names for the server (prefer provided available_sheets)
      2) Chooses the candidate whose *latest* snapshot_date is most recent (tie-breaker: row count)
      3) Loads rows from that latest snapshot for that sheet
    """
    import json

    try:
        # Candidate waits sheets
        candidates: List[str] = []
        if available_sheets:
            candidates = [s for s in available_sheets if isinstance(s, str) and "wait" in s.lower()]
        if not candidates:
            # fallback: discover across all snapshots
            candidates = [s for s in list_available_sheets_any(server_name) if "wait" in s.lower()]

        if not candidates:
            return pd.DataFrame()

        # Choose best candidate by latest snapshot_date, then by row count on that snapshot
        best_sheet = None
        best_snap = None
        best_rows = -1

        for s in candidates:
            snap = _get_latest_snapshot_for_sheet(server_name, s)
            if not snap:
                continue
            try:
                q_cnt = f"""
                SELECT COUNT(*) AS n
                FROM btris_dbx.observability.sql_diagnostics_bronze
                WHERE server_name = '{server_name}'
                  AND CAST(snapshot_date AS string) = '{snap}'
                  AND sheet_name = '{s}'
                """
                df_cnt = run_query(q_cnt)
                n = int(df_cnt["n"].iloc[0]) if (not df_cnt.empty and "n" in df_cnt.columns and df_cnt["n"].iloc[0] is not None) else 0
            except Exception:
                n = 0

            if best_snap is None or str(snap) > str(best_snap) or (str(snap) == str(best_snap) and n > best_rows):
                best_sheet = s
                best_snap = snap
                best_rows = n

        if not best_sheet or not best_snap or best_rows <= 0:
            return pd.DataFrame()

        # Load rows for the chosen sheet/snapshot
        q = f"""
        SELECT row_json
        FROM btris_dbx.observability.sql_diagnostics_bronze
        WHERE server_name = '{server_name}'
          AND CAST(snapshot_date AS string) = '{best_snap}'
          AND sheet_name = '{best_sheet}'
        """
        df = run_query(q)
        if df.empty or "row_json" not in df.columns:
            return pd.DataFrame()

        rows: List[Dict[str, Any]] = []
        for raw in df["row_json"]:
            if raw is None:
                continue
            if isinstance(raw, dict):
                rows.append(raw)
                continue
            if isinstance(raw, str):
                s = raw.strip()
                if not s:
                    continue
                try:
                    rows.append(json.loads(s))
                except Exception:
                    continue

        if not rows:
            return pd.DataFrame()

        waits_df = pd.DataFrame(rows)
        if waits_df.empty:
            return pd.DataFrame()

        # Normalize expected column names (based on your bronze row_json)
        waits_df = waits_df.rename(
            columns={
                "WaitType": "wait_type",
                "Wait Type": "wait_type",
                "Wait Percentage": "wait_pct",
                "Wait%": "wait_pct",
                "AvgWait_Sec": "avg_wait_s",
                "AvgWait_Sec ": "avg_wait_s",
                "AvgRes_Sec": "avg_resource_s",
                "AvgSig_Sec": "avg_signal_s",
            }
        )

        for c in ["wait_pct", "avg_wait_s", "avg_resource_s", "avg_signal_s"]:
            if c in waits_df.columns:
                waits_df[c] = pd.to_numeric(waits_df[c], errors="coerce")

        if "wait_pct" in waits_df.columns:
            waits_df = waits_df.sort_values("wait_pct", ascending=False)
        elif "avg_wait_s" in waits_df.columns:
            waits_df = waits_df.sort_values("avg_wait_s", ascending=False)

        keep = [c for c in ["wait_type", "wait_pct", "avg_wait_s", "avg_resource_s", "avg_signal_s"] if c in waits_df.columns]
        if keep:
            waits_df = waits_df[keep]

        return waits_df.head(10).reset_index(drop=True)

    except Exception:
        return pd.DataFrame()


def build_server_profile(server_name: str) -> Dict[str, Any]:
    profile: Dict[str, Any] = {
        "server": server_name,
        "snapshot": None,
        "instance": {},
        "utilization": {"max_cpu_pct": None, "max_memory_pct": None, "cache_ple_seconds": None},
        "pressure": {"min_ple": None, "memory_grants_pending": None},
        "configuration": {"maxdop": None, "cost_threshold": None, "max_server_memory_mb": None},
        "workload": {},
        "io_stats": None,
        "waits_df": None,
        "notes": [],
    }

    snapshot = _get_latest_snapshot(server_name)
    profile["snapshot"] = snapshot
    if not snapshot:
        profile["notes"].append("No latest snapshot found")
        return profile

    # --- Available sheets for this server+snapshot ---
    available = list_available_sheets_any(server_name)

    # --- Server Summary sheets (dynamic) ---
    server_props_sheet = resolve_sheet_name(available, [r"server\s+properties"])
    hardware_sheet = resolve_sheet_name(available, [r"hardware\s+info"])
    host_sheet = resolve_sheet_name(available, [r"host\s+info"])
    version_sheet = resolve_sheet_name(available, [r"version\s+info"])

    df_server = _fetch_sheet_latest(server_name, server_props_sheet)[0] if server_props_sheet else pd.DataFrame()
    df_hw = _fetch_sheet_latest(server_name, hardware_sheet)[0] if hardware_sheet else pd.DataFrame()
    df_host = _fetch_sheet_latest(server_name, host_sheet)[0] if host_sheet else pd.DataFrame()
    df_version = _fetch_sheet_latest(server_name, version_sheet)[0] if version_sheet else pd.DataFrame()


    profile["instance"] = _extract_instance_info(df_server, df_hw, df_host, df_version)

    # --- Other sheets (still using stable names for now) ---
    sheet_map = {
        "system_memory": "14-System Memory",
        "process_memory": "6-Process Memory",
        "cpu_history": "45-CPU Utilization History",
        "ple": "47-PLE by NUMA Node",
        "most_expensive": "Most Expensive Queries",
    }

    df_sys_mem = _fetch_sheet_latest(server_name, sheet_map["system_memory"])[0]
    df_proc_mem = _fetch_sheet_latest(server_name, sheet_map["process_memory"])[0]
    df_cpu = _fetch_sheet_latest(server_name, sheet_map["cpu_history"])[0]
    df_ple = _fetch_sheet_latest(server_name, sheet_map["ple"])[0]
    df_meq = _fetch_sheet_latest(server_name, sheet_map["most_expensive"])[0]

    # --- I/O sheets ---
    df_drive = _fetch_sheet_latest(server_name, "29-Drive Level Latency")[0]
    df_io_file = _fetch_sheet_latest(server_name, "30-IO Latency by File")[0]
    df_io_db = _fetch_sheet_latest(server_name, "37-IO Usage By Database")[0]

    # --- Utilization ---
    cpu_max = _extract_cpu_max(df_cpu)
    profile["utilization"]["max_cpu_pct"] = cpu_max

    mem_pct = _extract_memory_pct(df_proc_mem, df_sys_mem)
    profile["utilization"]["max_memory_pct"] = mem_pct

    ple_sec = _extract_ple(df_ple)
    profile["utilization"]["cache_ple_seconds"] = ple_sec

    # --- Pressure ---
    profile["pressure"]["min_ple"] = ple_sec

    # Memory Grants Pending (best-effort)
    try:
        q = f"""
        SELECT sheet_name, MAX(CAST(snapshot_date AS string)) AS latest_snapshot
        FROM btris_dbx.observability.sql_diagnostics_bronze
        WHERE server_name = '{server_name}'
          AND lower(sheet_name) LIKE '%memory grant%'
        GROUP BY sheet_name
        ORDER BY latest_snapshot DESC
        """
        df_sheets = run_query(q)
        mg_pending_val = None
        if not df_sheets.empty and "sheet_name" in df_sheets.columns:
            sheet_n = df_sheets["sheet_name"].iloc[0]
            df_mg = _fetch_sheet_latest(server_name, sheet_n)[0]
            cands = ["Memory Grants Pending", "memory_grants_pending", "memory_grants_pending_count", "grants_pending"]
            col = _pick_column(list(df_mg.columns), cands)
            if col:
                v = pd.to_numeric(df_mg[col], errors="coerce").dropna()
                if not v.empty:
                    mg_pending_val = int(v.max())
        profile["pressure"]["memory_grants_pending"] = mg_pending_val
    except Exception:
        profile["pressure"]["memory_grants_pending"] = None

    # --- Configuration (deterministic from "Configuration Values") ---
    try:
        config_sheet = resolve_sheet_name(available, [r"configuration\s+values", r"\bconfig\b"])
        df_conf = _fetch_sheet_latest(server_name, config_sheet)[0] if config_sheet else pd.DataFrame()

        if isinstance(df_conf, pd.DataFrame) and not df_conf.empty:
            cols = {c.lower(): c for c in df_conf.columns}
            name_col = cols.get("name")
            viu_col = cols.get("value_in_use") or cols.get("value in use")
            val_col = cols.get("value")

            if name_col and (viu_col or val_col):
                use_col = viu_col or val_col
                tmp = df_conf[[name_col, use_col]].copy()
                tmp[name_col] = tmp[name_col].astype(str).str.strip().str.lower()

                def _get_exact(name: str):
                    m = tmp[tmp[name_col] == name]
                    if m.empty:
                        return None
                    return m[use_col].iloc[-1]

                profile["configuration"]["maxdop"] = _get_exact("max degree of parallelism")
                profile["configuration"]["cost_threshold"] = _get_exact("cost threshold for parallelism")
                profile["configuration"]["max_server_memory_mb"] = _get_exact("max server memory (mb)")
    except Exception:
        pass

    # --- Workload summary ---
    profile["workload"] = _extract_workload_summary(df_meq)

    # --- I/O stats ---
    try:
        profile["io_stats"] = _extract_io_stats(df_drive, df_io_file, df_io_db)
    except Exception:
        profile["io_stats"] = None

    
    # --- Waits Breakdown ---
    try:
        profile["waits_df"] = _extract_waits(server_name, snapshot, available_sheets=available)
    except Exception:
        profile["waits_df"] = None

# --- Notes ---
    notes: List[str] = []
    if cpu_max is None:
        notes.append("CPU metric not found in worksheet '45-CPU Utilization History'.")
    if mem_pct is None:
        notes.append("Memory percent could not be computed (missing SQL memory or physical memory columns).")
    if ple_sec is None:
        notes.append("Page Life Expectancy not found; cache health unavailable.")
    profile["notes"] = notes

    return profile