from __future__ import annotations

import streamlit as st

from services.report_service import (
    build_report_plan,
    generate_report_docx_bytes,
    get_report_filename,
)


def render_report_tab(selected_server: str) -> None:
    selected_ingestion_date = st.session_state.get("selected_ingestion_date")
    st.markdown(
        """
<style>
.report-hero {
  padding: 16px 18px;
  border: 1px solid rgba(0,0,0,0.08);
  border-radius: 14px;
  background: rgba(255,255,255,0.78);
}
.report-title { font-size: 1.35rem; font-weight: 750; margin: 0 0 4px 0; }
.report-sub { opacity: 0.88; font-size: 0.92rem; margin: 0; }
.pill {
  display:inline-block; padding: 2px 10px; border-radius: 999px;
  border: 1px solid rgba(0,0,0,0.12); font-size: 0.78rem; opacity: 0.95;
}
.pill-ok { background: rgba(0, 200, 0, 0.08); }
.pill-warn { background: rgba(255, 170, 0, 0.14); }
.pill-bad { background: rgba(255, 0, 0, 0.10); }
.kpi-card {
  padding: 10px 12px;
  border: 1px solid rgba(0,0,0,0.08);
  border-radius: 12px;
  background: rgba(255,255,255,0.68);
}
.kpi-label { font-size: 0.82rem; opacity: 0.75; margin-bottom: 2px; }
.kpi-val { font-size: 1.45rem; font-weight: 750; line-height: 1.15; }
.kpi-sub { font-size: 0.8rem; opacity: 0.7; }
.notice {
  padding: 10px 12px;
  border-radius: 12px;
  border: 1px solid rgba(0,0,0,0.08);
  background: rgba(0, 120, 255, 0.07);
  font-size: 0.9rem;
}
.done {
  padding: 12px 12px;
  border-radius: 12px;
  border: 1px solid rgba(0,0,0,0.08);
  background: rgba(0, 200, 0, 0.07);
}
.stage-grid {
  display:grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}
.stage-card {
  border: 1px solid rgba(0,0,0,0.08);
  border-radius: 12px;
  padding: 10px 12px;
  background: rgba(255,255,255,0.62);
}
.stage-num { font-size: 0.8rem; opacity: 0.7; }
.stage-title { font-weight: 700; margin-top: 2px; }
.stage-sub { font-size: 0.82rem; opacity: 0.74; margin-top: 2px; }
.step-btn button {
  border-radius: 12px !important;
  font-weight: 600 !important;
  height: 46px !important;
  transition: all 0.15s ease-in-out;
}
.step-primary button {
  background: linear-gradient(135deg, #1f6feb, #2f81f7) !important;
  color: white !important;
  border: none !important;
}
.step-primary button:hover {
  transform: translateY(-1px);
  box-shadow: 0 6px 16px rgba(0,0,0,0.12);
}
.step-secondary button {
  background: rgba(0,0,0,0.06) !important;
  border: 1px solid rgba(0,0,0,0.1) !important;
}
.step-success button {
  background: linear-gradient(135deg, #1a7f37, #2da44e) !important;
  color: white !important;
  border: none !important;
}
.global-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  background: rgba(255,255,255,0.72);
  backdrop-filter: blur(2px);
  z-index: 9999;
  display: flex;
  align-items: center;
  justify-content: center;
}
.global-overlay-card {
  border: 1px solid rgba(0,0,0,0.10);
  border-radius: 16px;
  background: rgba(255,255,255,0.92);
  padding: 16px 18px;
  box-shadow: 0 10px 30px rgba(0,0,0,0.08);
  min-width: 300px;
}
.global-spinner {
  width: 54px;
  height: 54px;
  border: 6px solid rgba(0,0,0,0.10);
  border-top: 6px solid #1f6feb;
  border-radius: 50%;
  animation: spin 0.9s linear infinite;
  margin: 0 auto 10px auto;
}
.global-spinner-text {
  font-weight: 650;
  opacity: 0.86;
  text-align: center;
}
.global-spinner-sub {
  font-size: 0.86rem;
  opacity: 0.72;
  text-align: center;
  margin-top: 2px;
}
@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}
</style>
        """,
        unsafe_allow_html=True,
    )

    overlay = st.empty()

    def _show_overlay(title: str, subtitle: str = "Please wait…") -> None:
        overlay.markdown(
            f"""
<div class="global-overlay">
  <div class="global-overlay-card">
    <div class="global-spinner"></div>
    <div class="global-spinner-text">{title}</div>
    <div class="global-spinner-sub">{subtitle}</div>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

    def _hide_overlay() -> None:
        overlay.empty()

    st.subheader("Generate Server health assessment report")

    if not selected_server:
        st.info("Select a server first.")
        return

    # Reset server-specific report state when selection changes.
    previous_server = st.session_state.get("_report_selected_server")
    if previous_server != selected_server:
        st.session_state["_report_selected_server"] = selected_server
        st.session_state.pop("report_plan_md", None)
        st.session_state.pop("report_docx_bytes", None)
        st.session_state.pop("_report_plan_server", None)
        st.session_state.pop("_report_docx_server", None)
        st.session_state.pop("_report_error", None)

    profile = None
    try:
        from services.metrics_service import build_server_profile

        profile_cache = st.session_state.setdefault("_report_profile_cache", {})
        cache_key = f"{selected_server}_{selected_ingestion_date}"

        if cache_key in profile_cache:
            profile = profile_cache[cache_key]
        else:
            _show_overlay("Loading server snapshot", "Querying Delta tables…")
            try:
                profile = build_server_profile(
                  selected_server,
                  selected_ingestion_date
              )
            finally:
                _hide_overlay()
            profile_cache[cache_key] = profile
    except Exception as e:
        _hide_overlay()
        profile = None
        st.session_state["_report_error"] = f"Failed to load server profile: {e!r}"

    inst = (profile or {}).get("instance") or {}
    util = (profile or {}).get("utilization") or {}
    io_stats = (profile or {}).get("io_stats") or {}
    snapshot = (profile or {}).get("snapshot") or "—"

    sql_banner = inst.get("sql_banner") or "SQL Server"
    edition = inst.get("edition") or "—"
    cpu = inst.get("cpu_count")
    ram_mb = inst.get("total_ram_mb")
    os_name = inst.get("os_name") or "—"

    cpu_peak = util.get("max_cpu_pct")
    mem_peak = util.get("max_memory_pct")
    ple_s = util.get("cache_ple_seconds") or util.get("ple_sec")
    io_total = io_stats.get("total_io_str") if isinstance(io_stats.get("total_io_str"), str) else "—"

    cpu_str = f"{int(cpu)} cores" if isinstance(cpu, (int, float)) else "—"
    ram_str = f"{(float(ram_mb) / 1024):.0f} GB RAM" if isinstance(ram_mb, (int, float)) else "—"

    pill_cls = "pill-ok"
    pill_txt = "Ready"
    if isinstance(cpu_peak, (int, float)) and cpu_peak >= 85:
        pill_cls, pill_txt = "pill-warn", "High CPU"
    if isinstance(mem_peak, (int, float)) and mem_peak >= 85:
        pill_cls, pill_txt = "pill-warn", "High Memory"
    if isinstance(ple_s, (int, float)) and ple_s <= 300:
        pill_cls, pill_txt = "pill-warn", "Low PLE"

    left, right = st.columns([3.3, 1.3], gap="small")
    with left:
        st.markdown(
            f"""
<div class="report-hero">
  <div class="report-title">Server Health Assessment</div>
  <p class="report-sub">
    <b>Server:</b> <code>{selected_server}</code> &nbsp;•&nbsp;
    <b>Snapshot:</b> <code>{snapshot}</code> &nbsp;•&nbsp;
    <b>SQL:</b> {sql_banner} &nbsp;•&nbsp;
    <b>Edition:</b> {edition}
    &nbsp;&nbsp;<span class="pill {pill_cls}">{pill_txt}</span>
  </p>
  <p class="report-sub">{cpu_str} • {ram_str} • {os_name}</p>
</div>
            """,
            unsafe_allow_html=True,
        )
    with right:
        st.caption("Workflow")
        st.markdown(
            """
<div class="stage-grid">
  <div class="stage-card"><div class="stage-num">Step 1</div><div class="stage-title">Build plan</div><div class="stage-sub">Preview the section assembly flow.</div></div>
  <div class="stage-card"><div class="stage-num">Step 2</div><div class="stage-title">Generate</div><div class="stage-sub">Run the report pipeline.</div></div>
  <div class="stage-card"><div class="stage-num">Step 3</div><div class="stage-title">Download</div><div class="stage-sub">Export the DOCX.</div></div>
</div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    k1, k2, k3, k4 = st.columns(4, gap="small")
    with k1:
        st.markdown(
            f"""<div class="kpi-card"><div class="kpi-label">CPU Peak</div><div class="kpi-val">{cpu_peak:.1f}%</div><div class="kpi-sub">max utilization</div></div>"""
            if isinstance(cpu_peak, (int, float))
            else """<div class="kpi-card"><div class="kpi-label">CPU Peak</div><div class="kpi-val">—</div></div>""",
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(
            f"""<div class="kpi-card"><div class="kpi-label">Memory Peak</div><div class="kpi-val">{mem_peak:.1f}%</div><div class="kpi-sub">max utilization</div></div>"""
            if isinstance(mem_peak, (int, float))
            else """<div class="kpi-card"><div class="kpi-label">Memory Peak</div><div class="kpi-val">—</div></div>""",
            unsafe_allow_html=True,
        )
    with k3:
        st.markdown(
            f"""<div class="kpi-card"><div class="kpi-label">PLE</div><div class="kpi-val">{int(ple_s)}s</div><div class="kpi-sub">cache health proxy</div></div>"""
            if isinstance(ple_s, (int, float))
            else """<div class="kpi-card"><div class="kpi-label">PLE</div><div class="kpi-val">—</div></div>""",
            unsafe_allow_html=True,
        )
    with k4:
        st.markdown(
            f"""<div class="kpi-card"><div class="kpi-label">Total I/O</div><div class="kpi-val">{io_total}</div><div class="kpi-sub">database I/O volume</div></div>""",
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    if st.session_state.get("_report_error"):
        st.error(st.session_state["_report_error"])

    st.markdown('<div class="step-btn step-primary">', unsafe_allow_html=True)
    build_clicked = st.button("Build plan", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if build_clicked:
        st.session_state.pop("_report_error", None)
        _show_overlay("Building plan", "Analyzing evidence, mapping sections, and preparing the report execution plan…")
        try:
            plan_md = build_report_plan(
              selected_server,
              selected_ingestion_date
            )
            st.session_state["report_plan_md"] = plan_md
            st.session_state["_report_plan_server"] = selected_server
            st.session_state.pop("report_docx_bytes", None)
            st.session_state.pop("_report_docx_server", None)
        except Exception as e:
            st.session_state["_report_error"] = f"Plan generation failed: {e!r}"
        finally:
            _hide_overlay()

    plan_md = (
        st.session_state.get("report_plan_md")
        if st.session_state.get("_report_plan_server") == selected_server
        else None
    )

    if plan_md:
        st.markdown(
            '<div class="notice"><b>Plan generated.</b> Review the execution plan carefully. It now shows how evidence, tables, LLM narrative blocks, and fixed sections will be assembled before the DOCX is built.</div>',
            unsafe_allow_html=True,
        )
        with st.expander("View detailed report build plan", expanded=True):
            st.markdown(plan_md)
    else:
        st.caption("Click **Build plan** to see the real section-by-section execution plan the system will follow before generating the DOCX.")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    can_generate = bool(plan_md)
    btn_class = "step-primary" if can_generate else "step-secondary"
    st.markdown(f'<div class="step-btn {btn_class}">', unsafe_allow_html=True)
    generate_clicked = st.button(
        "Generate report",
        use_container_width=True,
        disabled=not can_generate,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if generate_clicked:
        st.session_state.pop("_report_error", None)
        _show_overlay("Generating report", "Composing the DOCX from extracted evidence…")
        try:
            docx_bytes = generate_report_docx_bytes(
              selected_server,
              selected_ingestion_date
            )
            st.session_state["report_docx_bytes"] = docx_bytes
            st.session_state["_report_docx_server"] = selected_server
        except Exception as e:
            st.session_state["_report_error"] = f"Report generation failed: {e!r}"
            st.session_state.pop("report_docx_bytes", None)
            st.session_state.pop("_report_docx_server", None)
        finally:
            _hide_overlay()

    docx_bytes = (
        st.session_state.get("report_docx_bytes")
        if st.session_state.get("_report_docx_server") == selected_server
        else None
    )

    if docx_bytes:
        st.markdown(
            '<div class="done"><b>Report generated.</b> Download the DOCX below and compare the cover page, TOC, headings, and table layout against the sample report.</div>',
            unsafe_allow_html=True,
        )
        st.markdown('<div class="step-btn step-success">', unsafe_allow_html=True)
        st.download_button(
            "Download Server Health Assessment (.docx)",
            data=docx_bytes,
            file_name=get_report_filename(selected_server),
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            use_container_width=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)