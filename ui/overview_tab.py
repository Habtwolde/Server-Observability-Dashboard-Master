# ui/overview_tab.py
# Executive Overview Tab (Executive Dashboard UI)

from __future__ import annotations

import pandas as pd
import streamlit as st

from services.metrics_service import build_server_profile
from services.file_service import get_latest_file_path, load_file_bytes

_CSS = r"""
<style>
:root{
  --radius-xl: 24px;
  --radius-lg: 18px;
  --radius-md: 14px;
  --radius-sm: 10px;

  --border: rgba(15, 23, 42, 0.08);
  --border-strong: rgba(15, 23, 42, 0.12);

  --surface: #ffffff;
  --surface-soft: #f8fafc;
  --surface-muted: #f1f5f9;
  --surface-accent: #eef2ff;

  --text-strong: #0f172a;
  --text-mid: #334155;
  --text-dim: #64748b;

  --ok-bg: rgba(22, 163, 74, 0.10);
  --warn-bg: rgba(245, 158, 11, 0.12);
  --bad-bg: rgba(239, 68, 68, 0.10);

  --ok-border: rgba(22, 163, 74, 0.22);
  --warn-border: rgba(245, 158, 11, 0.24);
  --bad-border: rgba(239, 68, 68, 0.24);

  --shadow-sm: 0 1px 2px rgba(15, 23, 42, 0.04);
  --shadow-md: 0 10px 30px rgba(15, 23, 42, 0.06);
}

div[data-testid="stAppViewContainer"] > .main {
  padding-top: 1.35rem !important;
}
.block-container {
  padding-top: 1.35rem !important;
  padding-bottom: 1.2rem !important;
  max-width: 1400px;
}

h1, h2, h3, h4 {
  color: var(--text-strong);
  letter-spacing: -0.02em;
}

p, li, label {
  color: var(--text-mid);
}

.exec-hero {
  border: 1px solid rgba(15, 23, 42, 0.05);
  border-radius: 16px;
  background: #ffffff;
  padding: 18px 20px;
  box-shadow: none;
}

.exec-header-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 18px;
}

.exec-title {
  font-size: 1.95rem;
  font-weight: 820;
  color: var(--text-strong);
  line-height: 1.15;
  margin: 0;
}

.exec-subtitle {
  margin-top: 6px;
  font-size: 0.95rem;
  color: var(--text-dim);
}

.health-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 12px;
  border-radius: 999px;
  border: 1px solid var(--border);
  font-size: 0.78rem;
  font-weight: 700;
  color: var(--text-mid);
  background: #fff;
}
.health-pill.ok {
  background: var(--ok-bg);
  border-color: var(--ok-border);
}
.health-pill.warn {
  background: var(--warn-bg);
  border-color: var(--warn-border);
}
.health-pill.bad {
  background: var(--bad-bg);
  border-color: var(--bad-border);
}

.section-divider {
  height: 1px;
  background: var(--border);
  margin: 18px 0 16px 0;
}

.section-heading {
  font-size: 1.08rem;
  font-weight: 800;
  color: var(--text-strong);
  margin: 0 0 10px 0;
}

.panel {
  border: 1px solid var(--border);
  border-radius: var(--radius-xl);
  padding: 16px 18px;
  background: var(--surface);
  box-shadow: var(--shadow-sm);
}

.panel-soft {
  border: 1px solid var(--border);
  border-radius: var(--radius-xl);
  padding: 16px 18px;
  background: linear-gradient(180deg, var(--surface-soft) 0%, #ffffff 100%);
  box-shadow: var(--shadow-sm);
}

.panel-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 800;
  font-size: 1.0rem;
  color: var(--text-strong);
  margin-bottom: 6px;
}

.panel-subtle {
  color: var(--text-dim);
  font-size: 0.9rem;
}

.exec-insight {
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 14px 16px;
  background: linear-gradient(180deg, #ffffff 0%, var(--surface-soft) 100%);
  box-shadow: var(--shadow-sm);
}

.exec-insight-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 800;
  color: var(--text-strong);
  margin-bottom: 4px;
}

.exec-insight-text {
  color: var(--text-mid);
  line-height: 1.5;
}

.kpi {
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 14px 16px 12px 16px;
  background: var(--surface);
  box-shadow: var(--shadow-sm);
  min-height: 116px;
}
.kpi .label {
  font-size: 0.82rem;
  color: var(--text-dim);
  font-weight: 750;
}
.kpi .value {
  font-size: 1.9rem;
  font-weight: 860;
  line-height: 1.08;
  margin-top: 6px;
  color: var(--text-strong);
}
.kpi .hint {
  font-size: 0.82rem;
  color: var(--text-dim);
  margin-top: 6px;
}

.kpi.ok { border-color: var(--ok-border); }
.kpi.warn { border-color: var(--warn-border); }
.kpi.bad { border-color: var(--bad-border); }

.metric-panel {
  border: 1px solid rgba(15, 23, 42, 0.05);
  border-radius: 14px;
  padding: 16px 18px;
  background: var(--surface);
  box-shadow: none;
  height: 100%;
}

.metric-block-title {
  font-weight: 800;
  color: var(--text-strong);
  margin-bottom: 10px;
}

.waits-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 8px;
}
.waits-table th, .waits-table td {
  padding: 9px 10px;
  border-top: 1px solid rgba(15, 23, 42, 0.08);
  font-size: 0.88rem;
  vertical-align: middle;
}
.waits-table thead th {
  border-top: none;
  font-weight: 760;
  color: var(--text-dim);
  background: var(--surface-muted);
}
.wait-type {
  font-weight: 760;
  color: var(--text-strong);
}
.wait-type.top {
  font-weight: 900;
}
.badge-mini {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 700;
  border: 1px solid var(--border);
  background: #fff;
  color: var(--text-dim);
  margin-left: 6px;
}
.bar-wrap {
  height: 10px;
  background: rgba(15, 23, 42, 0.08);
  border-radius: 999px;
  overflow: hidden;
}
.bar-fill {
  height: 100%;
  border-radius: 999px;
  background: linear-gradient(90deg, rgba(59, 130, 246, 0.20) 0%, rgba(59, 130, 246, 0.62) 100%);
}

.ai-shell {
  border: 1px solid rgba(15, 23, 42, 0.05);
  border-radius: 14px;
  padding: 14px 16px;
  background: #ffffff;
  box-shadow: none;
}

.ai-history {
  max-height: 420px;
  overflow-y: auto;
  overflow-x: hidden;
  border: 1px solid rgba(15, 23, 42, 0.04);
  border-radius: 12px;
  padding: 8px 10px;
  background: rgba(248, 250, 252, 0.55);
}

.ai-turn {
  padding: 0 0 8px 0;
  margin-bottom: 8px;
  border-bottom: 1px solid var(--border);
}
.ai-turn:last-child {
  border-bottom: none;
  margin-bottom: 0;
  padding-bottom: 0;
}

.ai-turn-role {
  font-weight: 800;
  color: var(--text-strong);
  margin-bottom: 5px;
}

.ai-scope {
  margin-top: 6px;
  font-size: 0.84rem;
  color: var(--text-dim);
}

.ai-answer-label {
  margin-top: 8px;
  font-weight: 800;
  color: var(--text-strong);
}

.ai-input-note {
  font-size: 0.8rem;
  color: var(--text-dim);
  margin-top: 4px;
}

div[data-testid="stDataFrame"] thead tr th {
  background: var(--surface-muted) !important;
}
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
    input_key = f"ai_input::{scope_key}"
    turns_key = f"ai_turns::{scope_key}"
    clear_flag_key = f"{input_key}__clear"

    turns = st.session_state.setdefault(turns_key, [])

    if st.session_state.get(clear_flag_key):
        st.session_state[input_key] = ""
        st.session_state[clear_flag_key] = False

    st.markdown("### 🤖 AI Diagnostic Assistant")

    st.markdown(
        """
        <div class="ai-shell">
          <div class="panel-title">🤖 AI Server Assistant</div>
          <div class="panel-subtle">
            Ask targeted questions about this snapshot, compare ingestions, or compare servers.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    if turns:
        st.markdown('<div class="ai-history">', unsafe_allow_html=True)

        for turn in turns:
            question = str(turn.get("question") or "")
            answer = str(turn.get("answer") or "")
            found = bool(turn.get("found", False))
            mode = str(turn.get("mode") or "single")

            st.markdown('<div class="ai-turn">', unsafe_allow_html=True)
            st.markdown('<div class="ai-turn-role">🧑 You</div>', unsafe_allow_html=True)
            st.write(question)

            scope_parts = []
            if mode == "compare":
                compare_servers = turn.get("compare_servers") or []
                compare_dates = turn.get("compare_dates") or []
                if compare_servers:
                    scope_parts.append(f"Servers: {', '.join(compare_servers)}")
                if compare_dates:
                    scope_parts.append(f"Dates: {', '.join(compare_dates)}")
            elif mode == "single":
                if turn.get("resolved_server"):
                    scope_parts.append(f"Server: {turn['resolved_server']}")
                if turn.get("resolved_ingestion_date"):
                    scope_parts.append(f"Ingestion Date: {turn['resolved_ingestion_date']}")

            if scope_parts:
                st.markdown(
                    f'<div class="ai-scope">Resolved Scope: {" • ".join(scope_parts)}</div>',
                    unsafe_allow_html=True,
                )            

            if mode == "chat":
                answer_label = "💬 Assistant"
            elif mode == "general":
                answer_label = "💬 Response"
            elif mode == "compare":
                answer_label = "📊 Comparison"
            else:
                answer_label = "🧠 Diagnostic Insight"

            st.markdown(
                f'<div class="ai-answer-label">{answer_label}</div>',
                unsafe_allow_html=True,
            )            
            if found:
                st.markdown(answer)
            else:
                st.info(answer)

            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    entry_col, action_col = st.columns([6.0, 1.25], gap="small")

    with entry_col:
        st.text_input(
            "Ask about this server",
            placeholder="Why is CPU high? Compare latest and previous ingestion. What waits dominate hc1dbsq36pv?",
            key=input_key,
            label_visibility="visible",
        )
        st.caption("Use concise, server-specific questions for best results.")

    with action_col:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        ask_clicked = st.button("Ask AI", key=f"ask_ai::{scope_key}", use_container_width=True)
        clear_clicked = st.button("Clear", key=f"clear_ai::{scope_key}", use_container_width=True)

    if clear_clicked:
        st.session_state[turns_key] = []
        st.session_state[clear_flag_key] = True
        st.rerun()

    q = (st.session_state.get(input_key) or "").strip()

    if ask_clicked:
        if not q:
            st.info("Please enter a question.")
            return

        with st.spinner("Analyzing diagnostics with AI..."):
            response = ask_server_ai(
                server_name=selected_server,
                ingestion_date=selected_ingestion_date,
                question=q,
            )

        turns.append(
            {
                "question": q,
                "answer": response.get("answer", ""),
                "found": response.get("found", False),
                "mode": response.get("mode", "single"),
                "resolved_server": response.get("resolved_server"),
                "resolved_ingestion_date": response.get("resolved_ingestion_date"),
                "compare_servers": response.get("compare_servers", []),
                "compare_dates": response.get("compare_dates", []),
            }
        )

        st.session_state[turns_key] = turns[-10:]
        st.session_state[clear_flag_key] = True
        st.rerun()


def render_overview(selected_server: str, selected_ingestion_date: str | None):
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

    st.markdown('<div class="exec-hero">', unsafe_allow_html=True)
    left, right = st.columns([4.7, 1.15])
    with left:
        st.markdown(
            f"""
            <div class="exec-title">
              {sql_banner} • {edition}
              <span class="health-pill {health_class}">{health_label}</span>
            </div>
            <div class="exec-subtitle">{cpu_count or '?'} cores • {int(ram_mb / 1024) if isinstance(ram_mb, (int, float)) else '?'} GB RAM • {os_name}</div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    insight_title, insight_text = _build_exec_insight(cpu_pct, mem_pct, ple_s, io_stats)
    st.markdown(
        f"""
        <div class="exec-insight">
          <div class="exec-insight-title">🧠 {insight_title}</div>
          <div class="exec-insight-text">{insight_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

    cpu_class = _kpi_class_for_pct(cpu_pct, warn_at=65, bad_at=85)
    mem_class = _kpi_class_for_pct(mem_pct, warn_at=65, bad_at=85)
    ple_class = _kpi_class_for_leq(ple_s, warn_at=600, bad_at=300)
    grants_class = _kpi_class_for_int_geq(grants_pending, warn_at=1, bad_at=5)

    cpu_hint = "CPU headroom OK" if cpu_class == "ok" else ("Elevated CPU load" if cpu_class == "warn" else "CPU at risk")
    mem_hint = "Stable memory use" if mem_class == "ok" else ("Memory trending high" if mem_class == "warn" else "Memory pressure risk")
    ple_hint = "Healthy cache" if ple_class == "ok" else ("Borderline cache churn" if ple_class == "warn" else "Low PLE")
    gp_hint = "No pressure" if grants_class == "ok" else ("Monitor grants" if grants_class == "warn" else "Grant backlog")

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(_kpi_tile_html("Max CPU", _fmt_pct(cpu_pct), cpu_hint, cpu_class), unsafe_allow_html=True)
    with k2:
        st.markdown(_kpi_tile_html("Max Memory", _fmt_pct(mem_pct), mem_hint, mem_class), unsafe_allow_html=True)
    with k3:
        st.markdown(_kpi_tile_html("PLE", _fmt_s(ple_s), ple_hint, ple_class), unsafe_allow_html=True)
    with k4:
        st.markdown(_kpi_tile_html("Grants Pending", _fmt_int(grants_pending), gp_hint, grants_class), unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    st.markdown('<div class="section-heading">Performance & Bottlenecks</div>', unsafe_allow_html=True)
    colA, colB = st.columns([1.1, 1.0], gap="medium")

    with colA:
        st.markdown('<div class="metric-panel">', unsafe_allow_html=True)
        st.markdown('<div class="metric-block-title">Workload (Top Queries)</div>', unsafe_allow_html=True)
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

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        st.markdown('<div class="metric-panel">', unsafe_allow_html=True)
        st.markdown('<div class="metric-block-title">⏳ Waits Breakdown</div>', unsafe_allow_html=True)
        if isinstance(waits_df, pd.DataFrame) and not waits_df.empty:
            _render_waits_table(waits_df)
        else:
            st.caption("No wait statistics available for this snapshot.")
        st.markdown("</div>", unsafe_allow_html=True)

    with colB:
        st.markdown('<div class="metric-panel">', unsafe_allow_html=True)
        st.markdown('<div class="metric-block-title">I/O Stats</div>', unsafe_allow_html=True)

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

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        st.markdown('<div class="metric-panel">', unsafe_allow_html=True)
        st.markdown('<div class="metric-block-title">⚙ Configuration</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        c1.metric("MaxDOP", _fmt_int(conf.get("maxdop")))
        c2.metric("Cost Th.", _fmt_int(conf.get("cost_threshold")))
        c3.metric("Max Mem", _mb_to_gb(conf.get("max_server_memory_mb")))
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    _render_ai_assistant(selected_server, selected_ingestion_date)