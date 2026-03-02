# services/llm_service.py
from __future__ import annotations

import os
from typing import Any, Dict, List

import streamlit as st
from databricks.sdk import WorkspaceClient

DEFAULT_ENDPOINT = os.getenv(
    "MODEL_ENDPOINT_NAME",
    "databricks-meta-llama-3-3-70b-instruct",
).strip()


@st.cache_resource
def _w() -> WorkspaceClient:
    return WorkspaceClient()


def _pick_text_from_response(d: Any) -> str:
    # SDK response objects sometimes have as_dict()
    if hasattr(d, "as_dict"):
        try:
            d = d.as_dict()
        except Exception:
            pass

    if isinstance(d, dict):
        # OpenAI-style
        if "choices" in d and d["choices"]:
            ch0 = d["choices"][0]
            if isinstance(ch0, dict):
                msg = ch0.get("message")
                if isinstance(msg, dict) and msg.get("content"):
                    return str(msg["content"])
                if ch0.get("text"):
                    return str(ch0["text"])

        # MLflow-style
        if "predictions" in d and d["predictions"]:
            p0 = d["predictions"][0]
            if isinstance(p0, dict):
                if p0.get("content"):
                    return str(p0["content"])
                if p0.get("text"):
                    return str(p0["text"])

        # Generic keys
        for k in ("output", "result", "response"):
            if isinstance(d.get(k), str):
                return d[k]

    return ""


def chat_completion(
    messages: List[Dict[str, str]],
    *,
    endpoint_name: str = DEFAULT_ENDPOINT,
    temperature: float = 0.2,
    max_tokens: int = 1400,
) -> str:
    """
    Call Databricks model serving via REST invocations.

    We try BOTH path forms because some Databricks App runtimes mount api_client
    at /api/2.0 already, while others require the explicit /api/2.0 prefix.
    """

    w = _w()

    payload: Dict[str, Any] = {
        "messages": messages,
        "temperature": float(temperature),
        "max_tokens": int(max_tokens),
    }

    # Try both variants (do not use serving_endpoints.query because it wraps "inputs")
    paths = [
        f"/api/2.0/serving-endpoints/{endpoint_name}/invocations",
        f"/serving-endpoints/{endpoint_name}/invocations",
    ]

    last_err: Exception | None = None

    for path in paths:
        try:
            try:
                resp = w.api_client.do("POST", path, body=payload)
            except TypeError:
                # some SDK builds don't accept body=
                resp = w.api_client.do("POST", path, payload)

            text = _pick_text_from_response(resp)
            if not text:
                raise RuntimeError(f"LLM returned empty response. Raw response: {resp}")
            return text

        except Exception as e:
            last_err = e

    # If both failed, surface the real last error (no fallback to query())
    raise last_err if last_err else RuntimeError("LLM call failed for unknown reason.")