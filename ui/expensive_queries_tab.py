# ui/expensive_queries_tab.py
from __future__ import annotations

import json
from typing import Optional, Dict, Any

import pandas as pd
import streamlit as st

from services.expensive_queries_service import (
    list_expensive_query_types,
    fetch_latest_expensive_queries,
    pick_query_text_column,
    pick_sort_metric_column,
    build_query_dropdown_items,
    QueryTypeOption,
)
from services.llm_service import chat_completion

from hashlib import sha1

def _chat_key(server_name: str, sheet_name: str, row_index: int) -> str:
    raw = f"{server_name}||{sheet_name}||{row_index}"
    return "exp_q_chat::" + sha1(raw.encode("utf-8")).hexdigest()


def _build_followup_messages(*, base_messages: list[dict], chat_history: list[dict], question: str) -> list[dict]:
    # base_messages are the original system+user messages for the analysis
    # chat_history holds alternating user/assistant turns
    msgs = list(base_messages)
    msgs.extend(chat_history)
    msgs.append(
        {
            "role": "user",
            "content": (
                "Follow-up question about the SAME query. "
                "Answer directly and technically.\n\n"
                f"Question: {question}"
            ),
        }
    )
    return msgs


def _to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _build_llm_prompt(*, server_name: str, query_text: str, row: Dict[str, Any], query_type_label: str) -> list[dict]:
    """
    Structured prompt so we get deterministic, production-usable output.
    """
    # Keep the row compact (avoid sending huge blobs)
    compact_row = {}
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, (int, float, str, bool)):
            compact_row[k] = v
        else:
            # convert to string safely
            compact_row[k] = str(v)

    system = (
        "You are a senior SQL Server performance engineer. "
        "Your job is to analyze a single SQL query and propose optimizations. "
        "Be concrete, technical, and safe. Avoid hallucinating unknown schema details. "
        "If table/index names are unknown, describe patterns and what to inspect, "
        "and propose generic but correct rewrites."
    )

    user = f"""
Context:
- Server: {server_name}
- Query bucket: {query_type_label}

Query (as observed):
{query_text}

Observed row metrics (from SQL diagnostics export; may be partial):
{json.dumps(compact_row, ensure_ascii=False, indent=2)}

Instructions:
Return a Markdown response with EXACTLY these sections and headings:

## Why this query is expensive
- Use the provided metrics when possible (worker time, reads, elapsed time, execution count, missing index flag, etc.).
- Explain likely root causes (scans, poor cardinality estimates, parameter sniffing, spills, blocking, chatty execution, etc.).

## What to change (ranked)
- Provide 5–10 actionable steps, ranked by impact.
- Include indexing guidance, statistics maintenance, parameterization tips, and query-shape improvements.

## Optimized query (best-effort)
- Provide a rewritten SQL query in a single ```sql``` code block.
- If you cannot safely rewrite without schema details, provide a "template rewrite" that demonstrates the optimization patterns.

## Validation checklist
- Provide a short checklist of how to validate the improvement (SET STATISTICS IO/TIME, actual plan, baseline capture, regression risk).
""".strip()

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def render_expensive_queries_tab(selected_server: str) -> None:
    selected_ingestion_date = st.session_state.get("selected_ingestion_date")
    st.subheader("Most Expensive Queries")

    if not selected_server:
        st.info("Select a server to view expensive queries.")
        return

    # -------------------------
    # Dropdown 1: Query Type
    # -------------------------
    options = list_expensive_query_types(selected_server)

    if not options:
        st.warning("No 'Top …' expensive query sheets were found for this server in the bronze table.")
        return

    label_to_opt = {o.label: o for o in options}
    type_label = st.selectbox(
        "Query Type",
        [o.label for o in options],
        key=f"exp_q_type::{selected_server}",
    )
    opt: QueryTypeOption = label_to_opt[type_label]

    # -------------------------
    # Load data for chosen sheet
    # -------------------------
    with st.spinner("Loading expensive queries from Delta…"):
        df, snap = fetch_latest_expensive_queries(
            selected_server,
            opt.sheet_name,
            selected_ingestion_date
        )

    if df.empty:
        st.warning(f"No rows found for sheet '{opt.sheet_name}' (latest snapshot: {snap or 'unknown'}).")
        return

    # Prefer a reasonable sort metric, then keep top rows for UI responsiveness
    metric_col = pick_sort_metric_column(df, opt.kind)
    if metric_col and metric_col in df.columns:
        df = df.copy()
        df["_sort_metric"] = _to_number(df[metric_col])
        df = df.sort_values(by="_sort_metric", ascending=False, na_position="last").drop(columns=["_sort_metric"])
    else:
        metric_col = None

    # -------------------------
    # Dropdown 2: Query selection
    # -------------------------
    query_col = pick_query_text_column(df)
    if not query_col:
        st.error(
            "I couldn't find a query text column in this sheet. "
            "Expected something like 'Short Query Text' or 'Query Text'."
        )
        st.caption(f"Detected columns: {', '.join(list(df.columns)[:40])}{' …' if len(df.columns) > 40 else ''}")
        return

    items = build_query_dropdown_items(df, query_col=query_col, limit=200)
    if not items:
        st.warning("No query text rows were found for this sheet.")
        return

    sel_item = st.selectbox(
        "Select Query",
        items,
        key=f"exp_q_query::{selected_server}::{opt.sheet_name}",
    )

    # Parse index from "NNN — ..."
    try:
        sel_idx = int(sel_item.split("—", 1)[0].strip()) - 1
    except Exception:
        sel_idx = 0

    sel_idx = max(0, min(sel_idx, len(df) - 1))
    row = df.iloc[sel_idx].to_dict()
    query_text = str(df.iloc[sel_idx][query_col]) if query_col in df.columns else ""

    # -------------------------
    # Display: selected row (table)
    # -------------------------
    st.caption(
        f"Sheet: **{opt.sheet_name}** | Snapshot: **{snap or 'unknown'}**"
        + (f" | Sorted by: **{metric_col}**" if metric_col else "")
    )

    # show a compact view first, then full table expandable
    important_cols = [c for c in [
        "Database Name",
        query_col,
        "Execution Count",
        "Total Worker Time",
        "Avg Worker Time",
        "Total Logical Reads",
        "Avg Logical Reads",
        "Avg Elapsed Time",
        "Total Elapsed Time",
        "Has Missing Index",
    ] if c in df.columns]

    if important_cols:
        st.dataframe(pd.DataFrame([row])[important_cols], use_container_width=True, hide_index=True)
    else:
        st.dataframe(pd.DataFrame([row]), use_container_width=True, hide_index=True)

    with st.expander("Show full Top list for this Query Type", expanded=False):
        st.dataframe(df.head(50), use_container_width=True, hide_index=True)

    # -------------------------
    # Analyze + Recommend
    # -------------------------
    if st.button("Analyze and Recommend", key=f"exp_q_analyze::{selected_server}::{opt.sheet_name}::{sel_idx}"):
        if not query_text or query_text.strip().lower() == "nan":
            st.error("Selected row has no query text to analyze.")
            return

        with st.spinner("Analyzing query with the model…"):
            messages = _build_llm_prompt(
                server_name=selected_server,
                query_text=query_text,
                row=row,
                query_type_label=opt.label,
            )
            try:
                out = chat_completion(messages)
            except Exception as e:
                st.error(f"Model call failed: {e}")
                return

        st.markdown(out)

        # Persist context for follow-up Q&A
        chat_key = _chat_key(selected_server, opt.sheet_name, sel_idx)
        st.session_state[chat_key + "::base_messages"] = messages
        st.session_state[chat_key + "::last_answer"] = out
        st.session_state.setdefault(chat_key + "::history", [])

    # -------------------------
    # Follow-up questions (LLM)
    # -------------------------
    st.divider()
    st.subheader("Follow-up questions (LLM)")

    chat_key = _chat_key(selected_server, opt.sheet_name, sel_idx)
    base_messages = st.session_state.get(chat_key + "::base_messages")
    history = st.session_state.get(chat_key + "::history", [])

    if not base_messages:
        st.info("Click **Analyze, and Recommend** first, then ask follow-up questions here.")
        return

    if history:
        with st.expander("Conversation history", expanded=False):
            for turn in history:
                role = turn.get("role", "assistant")
                content = turn.get("content", "")
                if role == "user":
                    st.markdown(f"**You:** {content}")
                else:
                    st.markdown(f"**LLM:** {content}")

    # Handle clear action BEFORE the widget is instantiated (Streamlit restriction)
    clear_flag = chat_key + "::clear_requested"
    if st.session_state.get(clear_flag, False):
        st.session_state[chat_key + "::history"] = []
        st.session_state[chat_key + "::question"] = ""
        st.session_state[clear_flag] = False

    q = st.text_area(
        "Ask a follow-up question about this query",
        placeholder="e.g., Can you propose the best index strategy and explain trade-offs?",
        key=chat_key + "::question",
        height=90,
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        ask = st.button("Ask follow-up", key=chat_key + "::ask")
    with col_b:
        clear = st.button("Clear conversation", key=chat_key + "::clear")
    if clear:
        # Do not modify the text_area value after instantiation; request clear and rerun.
        st.session_state[clear_flag] = True
        st.rerun()

    if ask:
        q = (q or "").strip()
        if not q:
            st.warning("Type a follow-up question first.")
            st.stop()

        with st.spinner("Thinking…"):
            msgs = _build_followup_messages(base_messages=base_messages, chat_history=history, question=q)
            try:
                ans = chat_completion(msgs)
            except Exception as e:
                st.error(f"Model call failed: {e}")
                st.stop()

        history.append({"role": "user", "content": q})
        history.append({"role": "assistant", "content": ans})
        st.session_state[chat_key + "::history"] = history

        st.markdown(ans)