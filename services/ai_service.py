import json
import os
import re
from difflib import get_close_matches
from typing import Optional, List, Dict, Any

try:
    from databricks.vector_search.client import VectorSearchClient
except ImportError:
    VectorSearchClient = None

from db.connection import run_query
from services.llm_service import chat_completion


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
VECTOR_ENDPOINT = os.getenv(
    "VECTOR_SEARCH_ENDPOINT",
    "sql-observability-vector-endpoint",
).strip()

VECTOR_INDEX = os.getenv(
    "VECTOR_SEARCH_INDEX",
    "btris_dbx.observability.sql-diag-vector-index",
).strip()


# ---------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------
def _get_all_servers() -> List[str]:
    df = run_query("""
        SELECT DISTINCT server_name
        FROM btris_dbx.observability.sql_diagnostics_files_delta
        ORDER BY server_name
    """)
    if df.empty or "server_name" not in df.columns:
        return []
    return [str(x) for x in df["server_name"].dropna().astype(str).tolist()]


def _get_ingestion_dates_for_server(server_name: str) -> List[str]:
    df = run_query(f"""
        SELECT DISTINCT CAST(ingestion_date AS STRING) AS ingestion_date
        FROM btris_dbx.observability.sql_diagnostics_files_delta
        WHERE server_name = '{server_name}'
        ORDER BY ingestion_date DESC
    """)
    if df.empty or "ingestion_date" not in df.columns:
        return []
    return [str(x) for x in df["ingestion_date"].dropna().astype(str).tolist()]


def _get_global_ingestion_dates() -> List[str]:
    df = run_query("""
        SELECT DISTINCT CAST(ingestion_date AS STRING) AS ingestion_date
        FROM btris_dbx.observability.sql_diagnostics_files_delta
        ORDER BY ingestion_date DESC
    """)
    if df.empty or "ingestion_date" not in df.columns:
        return []
    return [str(x) for x in df["ingestion_date"].dropna().astype(str).tolist()]


# ---------------------------------------------------------
# Text normalization / parsing
# ---------------------------------------------------------
def _normalize_text(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _extract_explicit_dates(question: str) -> List[str]:
    if not question:
        return []
    return re.findall(r"\b(20\d{2}-\d{2}-\d{2})\b", question)


# ---------------------------------------------------------
# Server resolution
# ---------------------------------------------------------
def _resolve_server_from_question(question: str, selected_server: Optional[str]) -> Optional[str]:
    servers = _get_all_servers()
    if not servers:
        return selected_server

    q = question or ""
    q_lower = q.lower()
    q_norm = _normalize_text(q)

    # 1) Exact contains match
    exact_matches = [s for s in servers if s.lower() in q_lower]
    if exact_matches:
        exact_matches.sort(key=len, reverse=True)
        return exact_matches[0]

    # 2) Normalized contains match
    normalized_matches = [s for s in servers if _normalize_text(s) in q_norm]
    if normalized_matches:
        normalized_matches.sort(key=len, reverse=True)
        return normalized_matches[0]

    # 3) Token / partial match
    tokens = re.findall(r"[a-zA-Z0-9_-]+", q_lower)
    partial_hits = []
    for s in servers:
        s_lower = s.lower()
        score = 0
        for t in tokens:
            if len(t) >= 4 and t in s_lower:
                score += len(t)
        if score > 0:
            partial_hits.append((score, s))
    if partial_hits:
        partial_hits.sort(reverse=True)
        return partial_hits[0][1]

    # 4) Fuzzy match
    server_norm_map = {_normalize_text(s): s for s in servers}
    fuzzy = get_close_matches(q_norm, list(server_norm_map.keys()), n=1, cutoff=0.55)
    if fuzzy:
        return server_norm_map[fuzzy[0]]

    return selected_server


def _resolve_servers_for_compare(question: str, selected_server: Optional[str]) -> List[str]:
    servers = _get_all_servers()
    if not servers:
        return [selected_server] if selected_server else []

    q_lower = (question or "").lower()
    found: List[str] = []

    for s in servers:
        if s.lower() in q_lower and s not in found:
            found.append(s)

    if len(found) < 2:
        norm_map = {_normalize_text(s): s for s in servers}
        tokens = re.findall(r"[a-zA-Z0-9_-]+", q_lower)
        for token in tokens:
            if len(token) < 4:
                continue
            fuzzy = get_close_matches(_normalize_text(token), list(norm_map.keys()), n=2, cutoff=0.65)
            for f in fuzzy:
                s = norm_map[f]
                if s not in found:
                    found.append(s)

    if not found and selected_server:
        found = [selected_server]

    return found[:2]


# ---------------------------------------------------------
# Date resolution
# ---------------------------------------------------------
def _resolve_single_ingestion_date(
    question: str,
    resolved_server: Optional[str],
    selected_ingestion_date: Optional[str],
) -> Optional[str]:
    q = (question or "").lower()
    explicit_dates = _extract_explicit_dates(question)

    if explicit_dates:
        return explicit_dates[0]

    dates = (
        _get_ingestion_dates_for_server(resolved_server)
        if resolved_server else _get_global_ingestion_dates()
    )
    if not dates:
        return selected_ingestion_date

    if "latest ingestion" in q or "latest snapshot" in q or re.search(r"\blatest\b", q):
        return dates[0]

    if "previous ingestion" in q or "previous snapshot" in q or "prior ingestion" in q:
        return dates[1] if len(dates) > 1 else dates[0]

    if "last week" in q:
        from datetime import datetime
        try:
            latest_dt = datetime.strptime(dates[0], "%Y-%m-%d")
            for d in dates[1:]:
                dt = datetime.strptime(d, "%Y-%m-%d")
                if (latest_dt - dt).days >= 7:
                    return d
            return dates[1] if len(dates) > 1 else dates[0]
        except Exception:
            return dates[1] if len(dates) > 1 else dates[0]

    return selected_ingestion_date or (dates[0] if dates else None)


def _resolve_compare_dates(
    question: str,
    resolved_server: Optional[str],
    selected_ingestion_date: Optional[str],
) -> Optional[List[str]]:
    q = (question or "").lower()
    explicit_dates = _extract_explicit_dates(question)

    if "compare" in q and len(explicit_dates) >= 2:
        return explicit_dates[:2]

    dates = (
        _get_ingestion_dates_for_server(resolved_server)
        if resolved_server else _get_global_ingestion_dates()
    )
    if len(dates) < 2:
        return None

    if "compare" in q and ("latest" in q or "previous" in q or "last week" in q):
        if "last week" in q:
            second = _resolve_single_ingestion_date("last week", resolved_server, selected_ingestion_date)
            if second and second != dates[0]:
                return [dates[0], second]
        return [dates[0], dates[1]]

    return None


# ---------------------------------------------------------
# Intent detection + semantic sheet weighting
# ---------------------------------------------------------
def _detect_query_intent(question: str) -> str:
    q = (question or "").lower()

    if "compare" in q:
        return "compare"
    if any(x in q for x in ["cpu", "scheduler", "worker time"]):
        return "cpu"
    if any(x in q for x in ["io", "latency", "read", "write", "disk", "iops"]):
        return "io"
    if any(x in q for x in ["wait", "blocking", "lock", "latch", "cxpacket", "cxconsumer"]):
        return "waits"
    if any(x in q for x in ["query", "queries", "procedure", "stored procedure", "index", "logical reads", "elapsed"]):
        return "queries"
    if any(x in q for x in ["memory", "ple", "grant", "buffer", "cache"]):
        return "memory"
    if "tempdb" in q:
        return "tempdb"
    if any(x in q for x in ["config", "maxdop", "cost threshold", "server memory", "setting"]):
        return "config"

    return "general"


def _sheet_boost_keywords(intent: str) -> List[str]:
    mapping = {
        "cpu": ["worker", "cpu", "scheduler"],
        "io": ["io", "logical reads", "physical reads", "latency", "write", "read"],
        "waits": ["wait", "blocking", "latch", "lock"],
        "queries": ["query", "queries", "statement", "procedure", "stored procedure", "index"],
        "memory": ["memory", "ple", "grant", "buffer", "cache"],
        "tempdb": ["tempdb"],
        "config": ["config", "maxdop", "cost threshold", "server memory"],
        "general": [],
        "compare": [],
    }
    return mapping.get(intent, [])


def _sheet_weight(sheet_name: str, intent: str) -> int:
    s = (sheet_name or "").lower()
    score = 0

    for kw in _sheet_boost_keywords(intent):
        if kw in s:
            score += 10

    if "top" in s:
        score += 2
    if "wait" in s:
        score += 2
    if "query" in s or "statement" in s or "procedure" in s:
        score += 2
    if "memory" in s:
        score += 1
    if "config" in s:
        score += 1

    return score


# ---------------------------------------------------------
# Vector retrieval
# ---------------------------------------------------------
def _search_vector_index(
    question: str,
    filters: Dict[str, Any],
    num_results: int = 12,
) -> List[Dict[str, Any]]:
    if VectorSearchClient is None:
        return []

    host = os.getenv("DATABRICKS_HOST", "").strip()
    if host and not host.startswith(("http://", "https://")):
        host = f"https://{host}"

    token = os.getenv("DATABRICKS_TOKEN", "").strip()
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "").strip()
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "").strip()

    if not host:
        raise RuntimeError("DATABRICKS_HOST is not set in the app environment.")

    if not VECTOR_INDEX:
        raise RuntimeError("VECTOR_SEARCH_INDEX is not configured in the app environment.")

    if not VECTOR_ENDPOINT:
        raise RuntimeError("VECTOR_SEARCH_ENDPOINT is not configured in the app environment.")

    if client_id and client_secret:
        vsc = VectorSearchClient(
            workspace_url=host,
            service_principal_client_id=client_id,
            service_principal_client_secret=client_secret,
            disable_notice=True,
        )
    elif token:
        vsc = VectorSearchClient(
            workspace_url=host,
            personal_access_token=token,
            disable_notice=True,
        )
    else:
        raise RuntimeError(
            "Vector Search auth is not configured. "
            "Expected app-provided service principal credentials or DATABRICKS_TOKEN."
        )

    index = vsc.get_index(
        endpoint_name=VECTOR_ENDPOINT,
        index_name=VECTOR_INDEX,
    )

    resp = index.similarity_search(
        query_text=question,
        columns=[
            "server_name",
            "snapshot_date",
            "ingestion_date",
            "sheet_name",
            "content",
        ],
        filters=filters,
        num_results=num_results,
    )

    result_rows: List[Dict[str, Any]] = []
    manifest_cols: List[str] = []

    if isinstance(resp, dict):
        manifest = resp.get("manifest", {})
        cols = manifest.get("columns", [])
        manifest_cols = [c.get("name") for c in cols if isinstance(c, dict)]

        data = resp.get("result", {}).get("data_array", [])
        for row in data:
            if manifest_cols and len(manifest_cols) == len(row):
                result_rows.append(dict(zip(manifest_cols, row)))

    return result_rows


def _rerank_rows_by_intent(rows: List[Dict[str, Any]], intent: str, top_k: int = 6) -> List[Dict[str, Any]]:
    scored = []
    for r in rows:
        score = _sheet_weight(str(r.get("sheet_name") or ""), intent)
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in scored[:top_k]]


# ---------------------------------------------------------
# Prompt builders with diagnostic reasoning chains
# ---------------------------------------------------------
def _build_prompt_single_scope(
    question: str,
    intent: str,
    resolved_server: Optional[str],
    resolved_ingestion_date: Optional[str],
    rows: List[Dict[str, Any]],
) -> str:
    context = []
    for r in rows:
        context.append(
            {
                "server_name": r.get("server_name"),
                "snapshot_date": r.get("snapshot_date"),
                "ingestion_date": r.get("ingestion_date"),
                "sheet_name": r.get("sheet_name"),
                "content": r.get("content"),
            }
        )

    return f"""
You are a senior SQL Server performance engineer.

Use ONLY the retrieved diagnostics context below.
Do not invent metrics, waits, queries, or configuration values.
If the evidence is partial, say so clearly.

Diagnostic reasoning chain:
1. Determine the query intent.
2. Identify the most relevant sheets and evidence.
3. Explain the likely cause.
4. Recommend the most practical next action.

Resolved retrieval scope:
- Server: {resolved_server or "not specified"}
- Ingestion Date: {resolved_ingestion_date or "not specified"}
- Detected Intent: {intent}

User question:
{question}

Retrieved diagnostics context:
{json.dumps(context, ensure_ascii=False, indent=2)}

Return:
1. Short answer
2. Evidence from diagnostics
3. Likely cause
4. Recommended action
5. Confidence level (High / Medium / Low)
""".strip()


def _build_prompt_compare_scope(
    question: str,
    intent: str,
    compare_servers: List[str],
    compare_dates: List[str],
    left_rows: List[Dict[str, Any]],
    right_rows: List[Dict[str, Any]],
) -> str:
    left_context = []
    right_context = []

    for r in left_rows:
        left_context.append(
            {
                "server_name": r.get("server_name"),
                "snapshot_date": r.get("snapshot_date"),
                "ingestion_date": r.get("ingestion_date"),
                "sheet_name": r.get("sheet_name"),
                "content": r.get("content"),
            }
        )

    for r in right_rows:
        right_context.append(
            {
                "server_name": r.get("server_name"),
                "snapshot_date": r.get("snapshot_date"),
                "ingestion_date": r.get("ingestion_date"),
                "sheet_name": r.get("sheet_name"),
                "content": r.get("content"),
            }
        )

    return f"""
You are a senior SQL Server performance engineer.

Use ONLY the retrieved diagnostics context below.
Do not invent metrics, waits, queries, or configuration values.
If the evidence is partial, say so clearly.

Diagnostic reasoning chain:
1. Compare the evidence sets.
2. Identify what changed materially.
3. Explain likely workload or operational reasons.
4. Recommend the most practical next action.

Comparison scope:
- Servers: {", ".join(compare_servers) if compare_servers else "not specified"}
- Date A: {compare_dates[0] if len(compare_dates) > 0 else "not specified"}
- Date B: {compare_dates[1] if len(compare_dates) > 1 else "not specified"}
- Detected Intent: {intent}

User question:
{question}

Retrieved diagnostics for scope A:
{json.dumps(left_context, ensure_ascii=False, indent=2)}

Retrieved diagnostics for scope B:
{json.dumps(right_context, ensure_ascii=False, indent=2)}

Return:
1. Executive comparison
2. What changed
3. Evidence from diagnostics
4. Likely explanation
5. Recommended action
6. Confidence level (High / Medium / Low)
""".strip()


# ---------------------------------------------------------
# LLM call
# ---------------------------------------------------------
def _call_llm(prompt: str) -> str:
    messages = [
        {
            "role": "system",
            "content": (
                "You are a senior SQL Server performance engineer. "
                "Use only the provided diagnostics evidence. "
                "Do not invent metrics, waits, queries, dates, or settings."
            ),
        },
        {
            "role": "user",
            "content": prompt,
        },
    ]

    try:
        return chat_completion(
            messages,
            temperature=0.1,
            max_tokens=1800,
        )
    except Exception as e:
        return f"AI analysis could not be generated from the retrieved diagnostics. Error: {e}"


# ---------------------------------------------------------
# Public API
# ---------------------------------------------------------
def ask_server_ai(
    server_name: str,
    ingestion_date: str,
    question: str,
    num_results: int = 12,
) -> Dict[str, Any]:
    if VectorSearchClient is None:
        return {
            "answer": "Vector Search dependency is not installed in the app environment.",
            "found": False,
            "mode": "error",
            "resolved_server": server_name,
            "resolved_ingestion_date": ingestion_date,
            "compare_servers": [],
            "compare_dates": [],
        }

    if not question or not question.strip():
        return {
            "answer": "Please enter a question.",
            "found": False,
            "mode": "empty",
            "resolved_server": server_name,
            "resolved_ingestion_date": ingestion_date,
            "compare_servers": [],
            "compare_dates": [],
        }

    intent = _detect_query_intent(question)

    # -----------------------------------------------------
    # Comparison mode
    # -----------------------------------------------------
    if "compare" in question.lower():
        compare_servers = _resolve_servers_for_compare(question, server_name)
        compare_dates = _resolve_compare_dates(
            question,
            compare_servers[0] if compare_servers else server_name,
            ingestion_date,
        )

        # Compare two servers on one date
        if len(compare_servers) == 2 and not (compare_dates and len(compare_dates) == 2):
            compare_date = _resolve_single_ingestion_date(question, compare_servers[0], ingestion_date)

            left_filters: Dict[str, Any] = {"server_name": compare_servers[0]}
            right_filters: Dict[str, Any] = {"server_name": compare_servers[1]}
            if compare_date:
                left_filters["ingestion_date"] = compare_date
                right_filters["ingestion_date"] = compare_date

            left_rows = _search_vector_index(question, left_filters, num_results=num_results)
            right_rows = _search_vector_index(question, right_filters, num_results=num_results)

            left_rows = _rerank_rows_by_intent(left_rows, intent)
            right_rows = _rerank_rows_by_intent(right_rows, intent)

            if not left_rows and not right_rows:
                return {
                    "answer": (
                        "I couldn’t find enough relevant diagnostics for that comparison. "
                        "Try mentioning the full server names, a specific ingestion date, "
                        "or asking about a narrower topic such as waits, CPU, I/O, or queries."
                    ),
                    "found": False,
                    "mode": "compare",
                    "resolved_server": None,
                    "resolved_ingestion_date": compare_date,
                    "compare_servers": compare_servers,
                    "compare_dates": [compare_date] if compare_date else [],
                }

            prompt = _build_prompt_compare_scope(
                question=question,
                intent=intent,
                compare_servers=compare_servers,
                compare_dates=[compare_date or "not specified", compare_date or "not specified"],
                left_rows=left_rows,
                right_rows=right_rows,
            )

            return {
                "answer": _call_llm(prompt),
                "found": True,
                "mode": "compare",
                "resolved_server": None,
                "resolved_ingestion_date": compare_date,
                "compare_servers": compare_servers,
                "compare_dates": [compare_date or "not specified", compare_date or "not specified"],
            }

        # One server, two dates
        resolved_server = compare_servers[0] if compare_servers else server_name
        if compare_dates and len(compare_dates) == 2:
            left_filters = {"server_name": resolved_server, "ingestion_date": compare_dates[0]}
            right_filters = {"server_name": resolved_server, "ingestion_date": compare_dates[1]}

            left_rows = _search_vector_index(question, left_filters, num_results=num_results)
            right_rows = _search_vector_index(question, right_filters, num_results=num_results)

            left_rows = _rerank_rows_by_intent(left_rows, intent)
            right_rows = _rerank_rows_by_intent(right_rows, intent)

            if not left_rows and not right_rows:
                return {
                    "answer": (
                        "I couldn’t find enough relevant diagnostics for that comparison. "
                        "Try a different date pair, or ask about a specific performance area."
                    ),
                    "found": False,
                    "mode": "compare",
                    "resolved_server": resolved_server,
                    "resolved_ingestion_date": None,
                    "compare_servers": [resolved_server],
                    "compare_dates": compare_dates,
                }

            prompt = _build_prompt_compare_scope(
                question=question,
                intent=intent,
                compare_servers=[resolved_server],
                compare_dates=compare_dates,
                left_rows=left_rows,
                right_rows=right_rows,
            )

            return {
                "answer": _call_llm(prompt),
                "found": True,
                "mode": "compare",
                "resolved_server": resolved_server,
                "resolved_ingestion_date": None,
                "compare_servers": [resolved_server],
                "compare_dates": compare_dates,
            }

    # -----------------------------------------------------
    # Single scope
    # -----------------------------------------------------
    resolved_server = _resolve_server_from_question(question, server_name)
    resolved_ingestion_date = _resolve_single_ingestion_date(
        question=question,
        resolved_server=resolved_server,
        selected_ingestion_date=ingestion_date,
    )

    filters: Dict[str, Any] = {}
    if resolved_server:
        filters["server_name"] = resolved_server
    if resolved_ingestion_date:
        filters["ingestion_date"] = resolved_ingestion_date

    rows = _search_vector_index(question, filters, num_results=num_results)
    rows = _rerank_rows_by_intent(rows, intent, top_k=6)

    if not rows:
        return {
            "answer": (
                "I couldn’t find enough matching diagnostics for that question. "
                "Try mentioning the full server name, an ingestion date like 2026-03-15, "
                "or a narrower topic such as CPU, waits, I/O, memory, or queries."
            ),
            "found": False,
            "mode": "single",
            "resolved_server": resolved_server,
            "resolved_ingestion_date": resolved_ingestion_date,
            "compare_servers": [],
            "compare_dates": [],
        }

    prompt = _build_prompt_single_scope(
        question=question,
        intent=intent,
        resolved_server=resolved_server,
        resolved_ingestion_date=resolved_ingestion_date,
        rows=rows,
    )

    return {
        "answer": _call_llm(prompt),
        "found": True,
        "mode": "single",
        "resolved_server": resolved_server,
        "resolved_ingestion_date": resolved_ingestion_date,
        "compare_servers": [],
        "compare_dates": [],
    }