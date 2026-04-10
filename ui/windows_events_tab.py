# ui/windows_events_tab.py
from __future__ import annotations

import streamlit as st

from services.windows_events_service import fetch_windows_events, build_summary_context, EventThresholds


_OVERLAY_CSS = """<style>
/* Global overlay spinner (for long-running work within this tab) */
.we-overlay {
  position: fixed;
  top: 0; left: 0;
  width: 100vw; height: 100vh;
  background: rgba(255,255,255,0.72);
  backdrop-filter: blur(2px);
  z-index: 9999;
  display: flex;
  align-items: center;
  justify-content: center;
}
.we-overlay-card {
  border: 1px solid rgba(0,0,0,0.10);
  border-radius: 16px;
  background: rgba(255,255,255,0.92);
  padding: 16px 18px;
  box-shadow: 0 10px 30px rgba(0,0,0,0.08);
  min-width: 300px;
}
.we-spinner {
  width: 54px;
  height: 54px;
  border: 6px solid rgba(0,0,0,0.10);
  border-top: 6px solid #1f6feb;
  border-radius: 50%;
  animation: we-spin 0.9s linear infinite;
  margin: 0 auto 10px auto;
}
.we-spinner-text {
  font-weight: 650;
  opacity: 0.86;
  text-align: center;
}
.we-spinner-sub {
  font-size: 0.86rem;
  opacity: 0.72;
  text-align: center;
  margin-top: 2px;
}
@keyframes we-spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}
</style>"""


def render_windows_events_tab(server_name: str) -> None:
    st.markdown("## Windows & Operational Events")
    st.caption("Event Viewer-style view from the uploaded Windows Events CSV, filtered by selected server.")

    # Inject CSS once
    if not st.session_state.get("_we_overlay_css", False):
        st.markdown(_OVERLAY_CSS, unsafe_allow_html=True)
        st.session_state["_we_overlay_css"] = True

    # Overlay placeholder (show/hide around long work)
    overlay = st.empty()

    def _show_overlay(title: str, subtitle: str = "Please wait…") -> None:
        overlay.markdown(
            f"""
<div class="we-overlay">
  <div class="we-overlay-card">
    <div class="we-spinner"></div>
    <div class="we-spinner-text">{title}</div>
    <div class="we-spinner-sub">{subtitle}</div>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

    def _hide_overlay() -> None:
        overlay.empty()

    with st.expander("Filters", expanded=True):
        col1, col2, col3 = st.columns([1.2, 1.0, 1.2])
        with col1:
            level = st.selectbox("Level", ["All", "Error", "Warning", "Information", "Info"], index=0)
        with col2:
            rows = st.selectbox("Rows", [25, 50, 100, 250], index=1)
        with col3:
            keyword = st.text_input("Search", placeholder="provider / message / event id ...")


    thresholds = EventThresholds()
    
    # Load events (latest snapshot) with a visible overlay loader
    _show_overlay("Loading Windows Events", "Querying Delta (latest snapshot)…")
    try:
        events_df, summary = fetch_windows_events(server_name, thresholds)
    finally:
        _hide_overlay()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Alerts", summary.get("alerts_total", 0))
    c2.metric("Alert Errors", summary.get("alerts_error", 0))
    c3.metric("Alert Warnings", summary.get("alerts_warning", 0))

    cpu_max = summary.get("cpu_max", None)
    c4.metric("Max SQL CPU", "-" if cpu_max is None else f"{cpu_max:.1f}%")
    c5.metric("CPU Spikes", summary.get("cpu_spikes_warning", 0) + summary.get("cpu_spikes_critical", 0))

    st.divider()

    if events_df.empty:
        st.info("No event-like records were found for this server in the latest snapshot.")
        st.markdown("### Summary context")
        st.write(build_summary_context(summary))
        return

    df = events_df.copy()

    if level != "All":
        df = df[df["level"] == level]

    if keyword:
        k = keyword.strip().lower()
        mask = (
            df["provider"].astype(str).str.lower().str.contains(k, na=False)
            | df["message"].astype(str).str.lower().str.contains(k, na=False)
            | df["id"].astype(str).str.lower().str.contains(k, na=False)
        )
        df = df[mask]

    show_cols = ["time_created", "level", "provider", "id", "message"]
    show_cols = [c for c in show_cols if c in df.columns]
    st.dataframe(df[show_cols].head(int(rows)), use_container_width=True, hide_index=True)

    st.markdown("### Summary context")
    st.write(build_summary_context(summary))