# ui/overview_tab.py
# Executive Overview Tab (Modern SaaS UI - Option C)

from __future__ import annotations

import pandas as pd
import streamlit as st

from services.metrics_service import build_server_profile
from services.file_service import get_latest_file_path, load_file_bytes

_CSS = r"""
<style>
/* =========================
   Overview Tab UI (Option C)
   - Modern SaaS / glassy panels
   - Section-based layout (no "one big container" feel)
   - Light, consistent spacing and typography
   ========================= */

:root{
  --radius-xl: 22px;
  --radius-lg: 18px;
  --radius-md: 14px;

  --border: rgba(0,0,0,0.08);
  --border-strong: rgba(0,0,0,0.10);

  --shadow-sm: 0 1px 2px rgba(0,0,0,0.04);
  --shadow-md: 0 10px 30px rgba(0,0,0,0.06);

  --glass: rgba(255,255,255,0.60);
  --glass-strong: rgba(255,255,255,0.74);
  --glass-solid: rgba(255,255,255,0.92);

  --text-dim: rgba(0,0,0,0.74);
  --text-mid: rgba(0,0,0,0.82);
  --text-strong: rgba(0,0,0,0.92);
}

/* Streamlit spacing: keep safe top padding but do not over-control */
div[data-testid="stAppViewContainer"] > .main { padding-top: 1.6rem !important; }
.block-container { padding-top: 1.6rem !important; padding-bottom: 1.2rem !important; }

/* Typography */
h1, h2, h3, h4 { margin: 0.35rem 0 0.6rem 0 !important; letter-spacing: -0.01em; }
p { margin: 0.25rem 0 !important; }

/* Pills */
.health-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 2px 10px;
  border-radius: 999px;
  border: 1px solid var(--border);
  font-size: 0.78rem;
  color: var(--text-mid);
  background: var(--glass-solid);
  vertical-align: middle;
  margin-left: 10px;
}
.health-pill.ok { background: rgba(24, 201, 100, 0.12); }
.health-pill.warn { background: rgba(255, 170, 0, 0.14); }
.health-pill.bad { background: rgba(255, 70, 70, 0.12); }

/* Section divider */
.hr {
  height: 1px;
  background: rgba(0,0,0,0.08);
  margin: 12px 0 14px 0;
}

/* Panel: glassy but crisp */
.panel {
  border: 1px solid var(--border);
  border-radius: var(--radius-xl);
  padding: 14px 16px;
  background: linear-gradient(180deg, var(--glass-strong) 0%, var(--glass) 100%);
  box-shadow: var(--shadow-sm);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
}
.panel-title {
  display: flex;
  align-items: center;
  gap: 10px;
  font-weight: 820;
  font-size: 1.02rem;
  color: var(--text-strong);
  margin-bottom: 6px;
}
.panel-subtle {
  color: var(--text-dim);
  font-size: 0.88rem;
}

/* KPI tile */
.kpi {
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 12px 14px 10px 14px;
  background: var(--glass-solid);
  box-shadow: var(--shadow-sm);
}
.kpi .label { font-size: 0.82rem; opacity: 0.78; font-weight: 720; }
.kpi .value { font-size: 1.65rem; font-weight: 860; line-height: 1.10; margin-top: 2px; color: var(--text-strong); }
.kpi .hint  { font-size: 0.80rem; opacity: 0.70; margin-top: 2px; color: var(--text-dim); }

.kpi.ok   { border-color: rgba(24, 201, 100, 0.22); }
.kpi.warn { border-color: rgba(255, 170, 0, 0.26); }
.kpi.bad  { border-color: rgba(255, 70, 70, 0.24); }

/* Insight strip (lightweight) */
.insight {
  border: 1px solid var(--border);
  border-radius: var(--radius-xl);
  padding: 12px 14px;
  background: linear-gradient(180deg, rgba(255,255,255,0.78) 0%, rgba(255,255,255,0.62) 100%);
  box-shadow: var(--shadow-sm);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
}
.insight-title { font-weight: 860; color: var(--text-strong); display:flex; align-items:center; gap:8px; }
.insight-text  { margin-top: 2px; color: var(--text-mid); opacity: 0.92; }

/* Waits table with modern bars */
.waits-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 8px;
}
.waits-table th, .waits-table td {
  padding: 8px 10px;
  border-top: 1px solid rgba(0,0,0,0.07);
  font-size: 0.88rem;
  vertical-align: middle;
}
.waits-table thead th {
  border-top: none;
  opacity: 0.70;
  font-weight: 760;
  background: rgba(0,0,0,0.03);
}
.wait-type { font-weight: 740; color: var(--text-strong); }
.wait-type.top { font-weight: 900; }
.badge-mini {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 0.72rem;
  border: 1px solid var(--border);
  background: rgba(255,255,255,0.75);
  color: var(--text-mid);
  margin-left: 6px;
}
.bar-wrap {
  height: 10px;
  background: rgba(0,0,0,0.06);
  border-radius: 999px;
  overflow: hidden;
}
.bar-fill {
  height: 100%;
  border-radius: 999px;
  background: linear-gradient(90deg, rgba(0,0,0,0.12) 0%, rgba(0,0,0,0.34) 100%);
}

/* Dataframe header subtle (if used elsewhere) */
div[data-testid="stDataFrame"] thead tr th { background: rgba(0,0,0,0.03) !important; }
</style>
"""


def _fmt_pct(v):
    return f"{v:.1f}%" if isinstance(v, (int, float)) else "—"


def _fmt_int(v):
    try:
        return f"{int(float(v))}"
    except Exception:
        return "—"


def _fmt_s(v):
    try:
        return f"{int(float(v))}s"
    except Exception:
        return "—"


def _mb_to_gb(v):
    try:
        return f"{float(v) / 1024:.1f} GB"
    except Exception:
        return "—"


def _health(cpu, mem, ple):
    score = 0
    if isinstance(cpu, (int, float)):
        score += 2 if cpu >= 85 else (1 if cpu >= 65 else 0)
    if isinstance(mem, (int, float)):
        score += 2 if mem >= 85 else (1 if mem >= 65 else 0)
    if isinstance(ple, (int, float)):
        score += 2 if ple <= 300 else (1 if ple <= 600 else 0)

    if score >= 4:
        return "Attention", "bad"
    if score >= 2:
        return "Watch", "warn"
    return "Healthy", "ok"


def _kpi_class_for_pct(v, warn_at, bad_at):
    if not isinstance(v, (int, float)):
        return "ok"
    if v >= bad_at:
        return "bad"
    if v >= warn_at:
        return "warn"
    return "ok"


def _kpi_class_for_leq(v, warn_at, bad_at):
    if not isinstance(v, (int, float)):
        return "ok"
    if v <= bad_at:
        return "bad"
    if v <= warn_at:
        return "warn"
    return "ok"


def _kpi_class_for_int_geq(v, warn_at, bad_at):
    try:
        vv = int(float(v))
    except Exception:
        return "ok"
    if vv >= bad_at:
        return "bad"
    if vv >= warn_at:
        return "warn"
    return "ok"


def _kpi_tile_html(label, value, hint, klass="ok"):
    return f"""<div class="kpi {klass}">
        <div class="label">{label}</div>
        <div class="value">{value}</div>
        <div class="hint">{hint}</div>
    </div>"""


def _download_button(selected_server: str):
    selected_ingestion_date = st.session_state.get("selected_ingestion_date")
    file_path = get_latest_file_path(selected_server, selected_ingestion_date)

    if not file_path:
        return
    try:
        file_bytes = load_file_bytes(file_path)
        st.download_button(
            "Download Server Information",
            data=file_bytes,
            file_name=f"{selected_server}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    except Exception:
        pass


def _build_exec_insight(cpu_pct, mem_pct, ple_s, io_stats: dict):
    signals = []
    risks = []

    if isinstance(cpu_pct, (int, float)):
        if cpu_pct >= 85:
            risks.append("CPU saturation risk (peak CPU ≥ 85%).")
        elif cpu_pct >= 65:
            signals.append("CPU utilization elevated but not critical.")
        else:
            signals.append("CPU headroom looks healthy.")

    if isinstance(mem_pct, (int, float)):
        if mem_pct >= 85:
            risks.append("High memory utilization (peak memory ≥ 85%).")
        elif mem_pct >= 65:
            signals.append("Memory utilization moderately high.")
        else:
            signals.append("Memory utilization within a safe range.")

    if isinstance(ple_s, (int, float)):
        if ple_s <= 300:
            risks.append("Low PLE (≤ 300s) indicates cache churn / memory pressure.")
        elif ple_s <= 600:
            signals.append("PLE borderline—monitor for cache churn.")
        else:
            signals.append("PLE indicates stable buffer cache behavior.")

    rd = io_stats.get("avg_read_latency_ms")
    wr = io_stats.get("avg_write_latency_ms")
    if isinstance(rd, (int, float)):
        if rd >= 20:
            risks.append("Read latency is elevated (avg read ≥ 20ms).")
        elif rd >= 10:
            signals.append("Read latency moderately high.")
    if isinstance(wr, (int, float)):
        if wr >= 20:
            risks.append("Write latency is elevated (avg write ≥ 20ms).")
        elif wr >= 10:
            signals.append("Write latency moderately high.")

    if not signals and not risks:
        return "Performance Insight", "Metrics available, but not enough signals to summarize confidently."

    parts = []
    if risks:
        parts.append("⚠ " + " ".join(risks))
    if signals:
        parts.append("• " + " ".join(signals))
    return "Performance Insight", " ".join(parts)


def _render_waits_table(waits_df: pd.DataFrame):
    df = waits_df.copy()

    if "wait_pct" in df.columns:
        df["wait_pct"] = pd.to_numeric(df["wait_pct"], errors="coerce").fillna(0.0)
    else:
        df["wait_pct"] = 0.0

    if "avg_wait_s" in df.columns:
        df["avg_wait_ms"] = (pd.to_numeric(df["avg_wait_s"], errors="coerce") * 1000).round(2)
    else:
        df["avg_wait_ms"] = pd.NA

    if "avg_signal_s" in df.columns:
        df["signal_ms"] = (pd.to_numeric(df["avg_signal_s"], errors="coerce") * 1000).round(2)
    else:
        df["signal_ms"] = pd.NA

    df = df.sort_values("wait_pct", ascending=False).head(10).reset_index(drop=True)

    max_pct = float(df["wait_pct"].max()) if len(df) else 0.0
    max_pct = max(max_pct, 1.0)

    rows_html = []
    for i, r in df.iterrows():
        wt = str(r["wait_type"]) if "wait_type" in df.columns else f"WAIT_{i + 1}"
        pct = float(r["wait_pct"]) if pd.notna(r["wait_pct"]) else 0.0
        wms = r["avg_wait_ms"]
        sms = r["signal_ms"]

        width = int(round((pct / max_pct) * 100))
        is_top = i == 0

        wt_class = "wait-type top" if is_top else "wait-type"
        top_badge = "<span class='badge-mini'>Top</span>" if is_top else ""

        wms_txt = f"{float(wms):.2f}" if pd.notna(wms) else "—"
        sms_txt = f"{float(sms):.2f}" if pd.notna(sms) else "—"

        rows_html.append(
            f"""<tr>
                <td><span class="{wt_class}">{wt}</span>{top_badge}</td>
                <td style="width:34%">
                    <div class="bar-wrap"><div class="bar-fill" style="width:{width}%"></div></div>
                </td>
                <td style="text-align:right">{pct:.2f}%</td>
                <td style="text-align:right">{wms_txt}</td>
                <td style="text-align:right">{sms_txt}</td>
            </tr>"""
        )

    table = f"""
<table class="waits-table">
  <thead>
    <tr>
      <th>Wait Type</th>
      <th>Contribution</th>
      <th style="text-align:right">Wait %</th>
      <th style="text-align:right">Avg Wait (ms)</th>
      <th style="text-align:right">Signal (ms)</th>
    </tr>
  </thead>
  <tbody>
    {''.join(rows_html)}
  </tbody>
</table>
"""
    st.markdown(table, unsafe_allow_html=True)


def _render_ai_assistant(selected_server: str, selected_ingestion_date: str | None) -> None:
    from services.ai_service import ask_server_ai

    scope_key = f"{selected_server}::{selected_ingestion_date}"
    q_key = f"ai_question::{scope_key}"
    h_key = f"ai_history::{scope_key}"
    r_key = f"ai_result::{scope_key}"

    history = st.session_state.setdefault(h_key, [])
    result = st.session_state.get(r_key)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    st.markdown("### 🤖 AI Diagnostic Assistant")

    st.markdown(
        """
        <div class="panel">
        <div class="panel-title">🤖 AI Server Assistant</div>
        <div class="panel-subtle">
            Ask about this server snapshot, compare ingestions, or compare servers.
        </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    q_col, clear_col = st.columns([6.0, 1.2], gap="small")
    with q_col:
        question = st.text_input(
            "Ask about this server",
            placeholder="Why is CPU high? Compare latest and previous ingestion. What waits dominate hc1dbsq36pv?",
            key=q_key,
            label_visibility="visible",
        )
    with clear_col:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        if st.button("Clear", key=f"clear_ai::{scope_key}", use_container_width=True):
            st.session_state[q_key] = ""
            st.session_state[r_key] = None
            st.session_state[h_key] = []
            st.rerun()


    #     q_col, clear_col = st.columns([6.0, 1.2], gap="small")
    # with q_col:
    #     question = st.text_input(
    #         "Ask about this server",
    #         placeholder="Why is CPU high? Compare latest and previous ingestion. What waits dominate hc1dbsq36pv?",
    #         key=q_key,
    #         label_visibility="visible",
    #     )

    # with clear_col:
    #     st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
    #     if st.button("Clear", key=f"clear_ai::{scope_key}", use_container_width=True):
    #         st.session_state[q_key] = ""
    #         st.session_state[r_key] = None
    #         st.session_state[h_key] = []
    #         st.rerun()

    ai_progress_slot = st.empty()

    if question and question.strip():
        if not result or result.get("question") != question.strip():
            with ai_progress_slot.container():
                st.markdown(
                    """
<div class="insight">
  <div class="insight-title">⏳ Analyzing your question...</div>
  <div class="insight-text">
    Searching diagnostics, resolving scope, and preparing analysis.
  </div>
</div>
                    """,
                    unsafe_allow_html=True,
                )

            with st.spinner("Analyzing diagnostics with AI..."):
                response = ask_server_ai(
                    server_name=selected_server,
                    ingestion_date=selected_ingestion_date,
                    question=question.strip(),
                )

            ai_progress_slot.empty()

            result = {
                "question": question.strip(),
                **response,
            }
            st.session_state[r_key] = result

            history.append(
                {
                    "question": result["question"],
                    "answer": result["answer"],
                    "found": result["found"],
                    "mode": result["mode"],
                    "resolved_server": result.get("resolved_server"),
                    "resolved_ingestion_date": result.get("resolved_ingestion_date"),
                    "compare_servers": result.get("compare_servers", []),
                    "compare_dates": result.get("compare_dates", []),
                }
            )
            st.session_state[h_key] = history[-5:]
    result = st.session_state.get(r_key)

    if result:
        scope_parts = []

        if result.get("mode") == "compare":
            compare_servers = result.get("compare_servers") or []
            compare_dates = result.get("compare_dates") or []

            if compare_servers:
                scope_parts.append(f"**Servers:** {', '.join(compare_servers)}")
            if compare_dates:
                scope_parts.append(f"**Dates:** {', '.join(compare_dates)}")
        else:
            if result.get("resolved_server"):
                scope_parts.append(f"**Server:** {result['resolved_server']}")
            if result.get("resolved_ingestion_date"):
                scope_parts.append(f"**Ingestion Date:** {result['resolved_ingestion_date']}")

        if scope_parts:
            st.markdown(
                f"""
<div class="insight">
  <div class="insight-title">Resolved Scope</div>
  <div class="insight-text">{' &nbsp;•&nbsp; '.join(scope_parts)}</div>
</div>
                """,
                unsafe_allow_html=True,
            )
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        if result.get("found"):
            st.markdown(
                """
<div class="panel">
  <div class="panel-title">🧠 AI Analysis</div>
</div>
                """,
                unsafe_allow_html=True,
            )
            st.markdown(result["answer"])
        else:
            st.info(result["answer"])

    if history:
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        with st.expander("Recent AI questions", expanded=False):
            for i, item in enumerate(reversed(history[-5:]), start=1):
                st.markdown(f"**{i}. {item['question']}**")
                if item.get("found"):
                    preview = item["answer"][:300] + ("..." if len(item["answer"]) > 300 else "")
                    st.markdown(preview)
                else:
                    st.caption(item["answer"])
                st.markdown("---")


def render_overview(selected_server: str):
    selected_ingestion_date = st.session_state.get("selected_ingestion_date")
    st.markdown(_CSS, unsafe_allow_html=True)

    cache = st.session_state.setdefault("_overview_profile_cache", {})
    cache_key = f"{selected_server}_{selected_ingestion_date}"

    if cache_key in cache:
        profile = cache[cache_key]
    else:
        with st.spinner("Loading server snapshot from Delta tables..."):
            profile = build_server_profile(
                selected_server,
                selected_ingestion_date
            )
        cache[cache_key] = profile

    instance = profile.get("instance") or {}
    util = profile.get("utilization") or {}
    pressure = profile.get("pressure") or {}
    conf = profile.get("configuration") or {}
    workload = profile.get("workload") or {}
    io_stats = profile.get("io_stats") or {}
    waits_df = profile.get("waits_df")

    sql_banner = instance.get("sql_banner") or "SQL Server"
    edition = instance.get("edition") or ""
    cpu_count = instance.get("cpu_count")
    ram_mb = instance.get("total_ram_mb")
    os_name = instance.get("os_name") or ""

    cpu_pct = util.get("max_cpu_pct")
    mem_pct = util.get("max_memory_pct")
    ple_s = util.get("cache_ple_seconds")
    grants_pending = pressure.get("memory_grants_pending")

    health_label, health_class = _health(cpu_pct, mem_pct, ple_s)

    left, right = st.columns([4.5, 1.2])
    with left:
        st.markdown(
            f"### {sql_banner} • {edition} "
            f"<span class='health-pill {health_class}'>{health_label}</span>",
            unsafe_allow_html=True,
        )
        ram_gb = int(ram_mb / 1024) if isinstance(ram_mb, (int, float)) else "?"
        st.caption(f"{cpu_count or '?'} cores • {ram_gb} GB RAM • {os_name}")
    with right:
        _download_button(selected_server)

    insight_title, insight_text = _build_exec_insight(cpu_pct, mem_pct, ple_s, io_stats)
    st.markdown(
        f"""<div class="insight">
              <div class="insight-title">🧠 {insight_title}</div>
              <div class="insight-text">{insight_text}</div>
            </div>""",
        unsafe_allow_html=True,
    )

    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)

    cpu_class = _kpi_class_for_pct(cpu_pct, warn_at=65, bad_at=85)
    mem_class = _kpi_class_for_pct(mem_pct, warn_at=65, bad_at=85)
    ple_class = _kpi_class_for_leq(ple_s, warn_at=600, bad_at=300)
    grants_class = _kpi_class_for_int_geq(grants_pending, warn_at=1, bad_at=5)

    cpu_hint = "CPU headroom OK" if cpu_class == "ok" else ("Elevated CPU load" if cpu_class == "warn" else "CPU at risk")
    mem_hint = "Stable memory use" if mem_class == "ok" else ("Memory trending high" if mem_class == "warn" else "Memory pressure risk")
    ple_hint = "Healthy cache" if ple_class == "ok" else ("Borderline cache churn" if ple_class == "warn" else "Low PLE (churn)")
    gp_hint = "No pressure" if grants_class == "ok" else ("Monitor grants" if grants_class == "warn" else "Memory grants backlog")

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(_kpi_tile_html("Max CPU", _fmt_pct(cpu_pct), cpu_hint, cpu_class), unsafe_allow_html=True)
    with k2:
        st.markdown(_kpi_tile_html("Max Memory", _fmt_pct(mem_pct), mem_hint, mem_class), unsafe_allow_html=True)
    with k3:
        st.markdown(_kpi_tile_html("PLE", _fmt_s(ple_s), ple_hint, ple_class), unsafe_allow_html=True)
    with k4:
        st.markdown(_kpi_tile_html("Grants Pending", _fmt_int(grants_pending), gp_hint, grants_class), unsafe_allow_html=True)

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    # _render_ai_assistant(selected_server, selected_ingestion_date)

    st.markdown("#### Performance & Bottlenecks")
    colA, colB = st.columns([1.1, 1.0])

    with colA:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='panel-title'> Workload (Top Queries)</div>", unsafe_allow_html=True)
        w1, w2, w3 = st.columns(3)
        w1.metric("Top Queries", _fmt_int(workload.get("top_query_count")))
        w2.metric(
            "Max Query",
            f"{workload.get('max_duration_s'):.1f}s"
            if isinstance(workload.get("max_duration_s"), (int, float))
            else "—",
        )
        w3.metric("Max Reads", _fmt_int(workload.get("max_logical_reads")))
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='panel-title'>⏳ Waits Breakdown</div>", unsafe_allow_html=True)
        if isinstance(waits_df, pd.DataFrame) and not waits_df.empty:
            _render_waits_table(waits_df)
        else:
            st.caption("No wait statistics available for this snapshot.")
        st.markdown("</div>", unsafe_allow_html=True)

    with colB:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='panel-title'>I/O Stats</div>", unsafe_allow_html=True)

        i1, i2 = st.columns(2)
        i1.metric(
            "Avg Read Lat (ms)",
            f"{io_stats.get('avg_read_latency_ms'):.1f}"
            if isinstance(io_stats.get("avg_read_latency_ms"), (int, float))
            else "—",
        )
        i2.metric(
            "Avg Write Lat (ms)",
            f"{io_stats.get('avg_write_latency_ms'):.1f}"
            if isinstance(io_stats.get("avg_write_latency_ms"), (int, float))
            else "—",
        )

        i3, i4 = st.columns(2)
        i3.metric(
            "Drive Max Lat (ms)",
            f"{io_stats.get('drive_max_overall_latency_ms'):.1f}"
            if isinstance(io_stats.get("drive_max_overall_latency_ms"), (int, float))
            else "—",
        )

        total_mb = io_stats.get("total_io_mb")
        if isinstance(total_mb, (int, float)):
            if total_mb >= 1_000_000:
                total_val = f"{total_mb / 1_000_000:.1f} TB"
            elif total_mb >= 1_000:
                total_val = f"{total_mb / 1_000:.1f} GB"
            else:
                total_val = f"{int(total_mb)} MB"
        else:
            total_val = "—"

        i4.metric("Total I/O", total_val)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

        st.markdown("#### System Configuration")
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='panel-title'>⚙ Configuration</div>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        c1.metric("MaxDOP", _fmt_int(conf.get("maxdop")))
        c2.metric("Cost Th.", _fmt_int(conf.get("cost_threshold")))
        c3.metric("Max Mem", _mb_to_gb(conf.get("max_server_memory_mb")))
        st.markdown("</div>", unsafe_allow_html=True)

    # st.caption("Source: Latest ingested weekly SQL diagnostics workbook")
    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    # =====================================
    # AI Server Assistant (Deep Analysis)
    # =====================================
    _render_ai_assistant(selected_server, selected_ingestion_date)

    st.caption("Source: Latest ingested weekly SQL diagnostics workbook")
    
    