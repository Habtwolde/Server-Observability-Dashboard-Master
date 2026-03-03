# services/report_service.py
from __future__ import annotations

import copy
import json
import os
import re
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from docx import Document
from docx.text.paragraph import Paragraph

from db.connection import run_query
from services.metrics_service import build_server_profile
from services.llm_service import chat_completion


# =========================
# Template + style loading
# =========================
def _load_style_prompt() -> Dict[str, Any]:
    """Load services/style_prompt.json (if present)."""
    here = os.path.dirname(__file__)
    path = os.path.join(here, "style_prompt.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "report_title_template": "Server Health Assessment Report — {server_name}",
            "docx_filename_template": "{server_name}_health_assessment_report.docx",
            "hard_rules": [
                "Use ONLY the provided evidence JSON. If a metric/table is missing, write 'Not available in this snapshot' and continue.",
                "Do not mention sheets by name in the report output.",
                "Do not fabricate numbers, wait types, query names, or configuration values.",
            ],
            "section_order": [
                "1. Executive Summary",
                "2. Environment Overview",
                "3. Observed Performance Characteristics",
                "4. Key Findings",
                "5. Remediation Plan",
                "6. Resource Optimization and Cost Strategy",
                "7. Expected Outcomes",
                "8. Conclusion",
            ],
            "tables": {},
        }


def _load_template_docx_bytes() -> bytes:
    """
    Loads assets/report_template.docx from the repo folder.
    Databricks Apps cwd is typically project root (same level as app.py).
    """
    template_path = Path(__file__).resolve().parents[1] / "assets" / "report_template.docx"
    return template_path.read_bytes()



def _extract_template_headings(template_bytes: bytes) -> List[str]:
    """Extract ordered section headings from the DOCX template itself.

    We treat any paragraph that looks like:
      - '1. Executive Summary'
      - '2.1 Infrastructure'
    as a heading, and we preserve the order as it appears in the template.
    """
    doc = Document(BytesIO(template_bytes))
    headings: List[str] = []

    # Match: 1. ...  OR 2.1 ... OR 3.6 ...
    rx = re.compile(r"^\d+(?:\.\d+)?\.\s")  # Only headings like "1. " or "2.1. "

    for p in doc.paragraphs:
        t = (p.text or "").strip()
        if not t:
            continue
        if rx.match(t):
            headings.append(t)

    # De-dup while preserving order
    seen = set()
    out: List[str] = []
    for h in headings:
        if h not in seen:
            seen.add(h)
            out.append(h)

    # Safety: if template has only major headings, still return them
    return out


def get_report_filename(server_name: str) -> str:
    style = _load_style_prompt()
    tpl = style.get("docx_filename_template", "{server_name}_health_assessment_report.docx")
    try:
        return tpl.format(server_name=server_name)
    except Exception:
        return f"{server_name}_health_assessment_report.docx"


# =========================
# JSON-safety helper
# =========================
def _to_jsonable(x: Any) -> Any:
    try:
        import numpy as np  # type: ignore
        if isinstance(x, np.generic):
            return x.item()
    except Exception:
        pass

    try:
        if isinstance(x, pd.Timestamp):
            return x.isoformat()
    except Exception:
        pass

    if isinstance(x, dict):
        return {str(k): _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_to_jsonable(v) for v in x]
    if isinstance(x, tuple):
        return [_to_jsonable(v) for v in x]
    return x


# =========================
# Snapshot helpers
# =========================
def _get_latest_snapshot(server_name: str) -> Optional[str]:
    q = f"""
    SELECT CAST(snapshot_date AS string) AS snapshot
    FROM btris_dbx.observability.v_latest_sql_diagnostics
    WHERE server_name = '{server_name}'
    LIMIT 1
    """
    df = run_query(q)
    if df.empty or "snapshot" not in df.columns:
        return None
    v = df["snapshot"].iloc[0]
    s = str(v).strip() if v is not None else ""
    return s or None


# =========================
# Evidence (expandable)
# =========================
def _safe_top_waits_from_profile(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Use the robust waits extraction already built into metrics_service profile."""
    waits_df = profile.get("waits_df")
    if isinstance(waits_df, pd.DataFrame) and not waits_df.empty:
        return waits_df.to_dict(orient="records")
    return []


def _compact_context(profile: Dict[str, Any]) -> Dict[str, Any]:
    inst = profile.get("instance") or {}
    util = profile.get("utilization") or {}
    pressure = profile.get("pressure") or {}
    conf = profile.get("configuration") or {}
    io_stats = profile.get("io_stats") or {}
    workload = profile.get("workload") or {}

    return {
        "server": profile.get("server"),
        "snapshot": profile.get("snapshot"),
        "instance": {
            "sql_banner": inst.get("sql_banner"),
            "edition": inst.get("edition"),
            "cpu_count": inst.get("cpu_count"),
            "total_ram_mb": inst.get("total_ram_mb"),
            "os_name": inst.get("os_name"),
        },
        "utilization": {
            "max_cpu_pct": util.get("max_cpu_pct"),
            "max_memory_pct": util.get("max_memory_pct"),
            "cache_ple_seconds": util.get("cache_ple_seconds"),
        },
        "pressure": {
            "min_ple": pressure.get("min_ple"),
            "memory_grants_pending": pressure.get("memory_grants_pending"),
        },
        "configuration": {
            "maxdop": conf.get("maxdop"),
            "cost_threshold": conf.get("cost_threshold"),
            "max_server_memory_mb": conf.get("max_server_memory_mb"),
        },
        "io_stats": io_stats,
        "workload": workload,
        "top_waits": _safe_top_waits_from_profile(profile),
        "notes": profile.get("notes") or [],
    }


# =========================
# LLM drafting (STRICT JSON)
# =========================
def _draft_report_json(server_name: str, ctx: Dict[str, Any], section_order: List[str]) -> Dict[str, Any]:
    """
    Ask the LLM for a STRICT JSON payload.
    Determinism strategy:
      - The DOCX structure comes from the template (not the model).
      - The model only fills text for known headings.
    """
    style = _load_style_prompt()
    # IMPORTANT: use headings derived from the DOCX template for determinism
    section_order: List[str] = list(section_order or [])

    system = (
        "You are a principal SQL Server performance and reliability engineer.\n"
        "You MUST use ONLY the provided evidence JSON.\n"
        "If a value is missing, write exactly: 'Not available in this snapshot.'\n"
        "Return ONLY valid JSON (no markdown, no code fences)."
    )

    user_payload = {
        "server_name": server_name,
        "hard_rules": style.get("hard_rules", []),
        "section_order": section_order,
        "tables_spec": style.get("tables", {}),
        "evidence": ctx,
        "output_contract": {
            "type": "json",
            "schema": {
                "title": "string",
                "subtitle_lines": ["string"],
                "sections": [
                    {
                        "heading": "string (must match a section_order item exactly)",
                        "paragraphs": ["string"],
                        "bullets": ["string"],
                        "tables": [
                            {
                                "title": "string",
                                "columns": ["string"],
                                "rows": [["string"]]
                            }
                        ]
                    }
                ],
            },
            "requirements": [
                "Include ALL headings from section_order in order.",
                "Each section must have >=1 paragraph. Use short paragraphs.",
                "Use bullets heavily where appropriate (Key Findings, Remediation).",
                "Use concrete evidence fields: utilization, configuration, io_stats, top_waits, workload.",
            ],
        },
    }

    raw = chat_completion(
        [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)[:12000]},
        ],
        temperature=0.1,
        max_tokens=1700,
    ).strip()

    # unwrap accidental fences
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    return json.loads(raw)


# =========================
# DOCX deterministic rendering (TEMPLATE-DRIVEN)
# =========================
def _is_major_heading(text: str) -> bool:
    return bool(re.match(r"^\d+\.\s", (text or "").strip()))


def _is_sub_heading(text: str) -> bool:
    return bool(re.match(r"^\d+\.\d+\s", (text or "").strip()))


def _clone_paragraph_after(anchor: Paragraph, template_para: Paragraph, text: str) -> Paragraph:
    """Deep-copy a paragraph's XML (preserves numbering/indent/spacing), then set text."""
    new_p = copy.deepcopy(template_para._p)
    anchor._p.addnext(new_p)
    new_para = Paragraph(new_p, anchor._parent)

    if new_para.runs:
        new_para.runs[0].text = text
        for r in new_para.runs[1:]:
            r.text = ""
    else:
        new_para.add_run(text)
    return new_para


def _remove_paragraph(p: Paragraph) -> None:
    el = p._element
    el.getparent().remove(el)
    p._p = p._element = None  # type: ignore


def _find_heading_positions(doc: Document) -> Dict[str, int]:
    """Map exact paragraph text -> index (first occurrence)."""
    out: Dict[str, int] = {}
    for i, p in enumerate(doc.paragraphs):
        t = (p.text or "").strip()
        if t and t not in out:
            out[t] = i
    return out


def _ensure_missing_headings(doc: Document, wanted: List[str]) -> None:
    """
    Your template is an example report; it does NOT include every heading in style_prompt.json.
    We insert missing headings *in the correct location* using exemplar heading formatting.
    """
    pos = _find_heading_positions(doc)

    # Exemplars from template
    major_ex = None
    sub_ex = None
    for p in doc.paragraphs:
        t = (p.text or "").strip()
        if major_ex is None and _is_major_heading(t):
            major_ex = p
        if sub_ex is None and _is_sub_heading(t):
            sub_ex = p
        if major_ex and sub_ex:
            break
    if major_ex is None:
        major_ex = doc.paragraphs[0]
    if sub_ex is None:
        sub_ex = major_ex

    def insert_after_heading(after_text: str, new_heading: str) -> None:
        pos2 = _find_heading_positions(doc)
        if new_heading in pos2:
            return
        if after_text not in pos2:
            # if anchor missing too, append at end
            anchor = doc.paragraphs[-1]
        else:
            anchor = doc.paragraphs[pos2[after_text]]
        tpl = sub_ex if _is_sub_heading(new_heading) else major_ex
        _clone_paragraph_after(anchor, tpl, new_heading)
        # also add a blank paragraph after heading (template style)
        _clone_paragraph_after(doc.paragraphs[pos2.get(after_text, len(doc.paragraphs)-1)+1 if after_text in pos2 else -1], doc.paragraphs[2] if len(doc.paragraphs) > 2 else tpl, "")

    missing = [h for h in wanted if h not in pos]
    # Deterministic anchors for known missing items
    for h in missing:
        if h == "3.3 Memory":
            insert_after_heading("3.2 I/O and tempdb", h)
        elif h == "3.5 High-Impact Workload Hotspots":
            insert_after_heading("3.4 Indexing and Query Patterns", h)
        elif h == "6. Resource Optimization and Cost Strategy":
            insert_after_heading("5. Remediation Plan", h)
        else:
            # safest: append near end (before Conclusion if exists)
            anchor = "7. Expected Outcomes" if "7. Expected Outcomes" in pos else (doc.paragraphs[-1].text or "")
            insert_after_heading(anchor, h)


def _section_boundaries(doc: Document, headings_in_doc: List[str]) -> Dict[str, Tuple[int, int]]:
    """
    Returns {heading: (start_index, end_index_exclusive)} where end is next heading start.
    """
    idx = _find_heading_positions(doc)
    ordered = [h for h in headings_in_doc if h in idx]
    ordered.sort(key=lambda h: idx[h])
    bounds: Dict[str, Tuple[int, int]] = {}
    for i, h in enumerate(ordered):
        s = idx[h]
        e = idx[ordered[i + 1]] if i + 1 < len(ordered) else len(doc.paragraphs)
        bounds[h] = (s, e)
    return bounds


def _pick_exemplars(doc: Document, start: int, end: int, global_body: Paragraph, global_bullet: Paragraph) -> Tuple[Paragraph, Paragraph]:
    body_ex = None
    bullet_ex = None
    for p in doc.paragraphs[start + 1 : end]:
        txt = (p.text or "").strip()
        if not txt:
            continue
        is_numbered = p._p.pPr is not None and p._p.pPr.numPr is not None  # type: ignore
        if bullet_ex is None and is_numbered:
            bullet_ex = p
        if body_ex is None and (not is_numbered):
            body_ex = p
        if body_ex and bullet_ex:
            break
    return (body_ex or global_body, bullet_ex or global_bullet)


def _fmt(v: Any, suffix: str = "") -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "Not available in this snapshot."
    try:
        if isinstance(v, (int, float)) and suffix:
            return f"{float(v):.2f}{suffix}"
        return str(v)
    except Exception:
        return "Not available in this snapshot."


def _has_any_metric(ctx: Dict[str, Any]) -> bool:
    if not isinstance(ctx, dict):
        return False
    util = ctx.get("utilization") or {}
    pressure = ctx.get("pressure") or {}
    cfg = ctx.get("configuration") or {}
    instance = ctx.get("instance") or {}
    def any_val(d: Any, keys: list[str]) -> bool:
        if not isinstance(d, dict):
            return False
        for k in keys:
            v = d.get(k)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                return True
        return False
    return any([
        any_val(util, ["max_cpu_pct", "max_memory_pct", "cache_ple_seconds"]),
        any_val(pressure, ["min_ple", "memory_grants_pending"]),
        any_val(cfg, ["maxdop", "cost_threshold", "max_server_memory_mb"]),
        any_val(instance, ["edition", "product_version", "product_level", "sql_start_time", "host_name"]),
    ])

def _build_tables_from_evidence(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Return deterministic tables to embed in sections."""
    util = (ctx.get("utilization") or {}) if isinstance(ctx, dict) else {}
    cfg = (ctx.get("configuration") or {}) if isinstance(ctx, dict) else {}
    pressure = (ctx.get("pressure") or {}) if isinstance(ctx, dict) else {}
    instance = (ctx.get("instance") or {}) if isinstance(ctx, dict) else {}

    tables: Dict[str, Any] = {}

    tables["key_metrics"] = {
        "title": "Key metrics (latest snapshot)",
        "columns": ["Metric", "Value"],
        "rows": [
            ["Max CPU (%)", _fmt(util.get("max_cpu_pct"), "%") if util.get("max_cpu_pct") is not None else "Not available in this snapshot."],
            ["Max Memory (%)", _fmt(util.get("max_memory_pct"), "%") if util.get("max_memory_pct") is not None else "Not available in this snapshot."],
            ["Cache PLE (sec)", _fmt(util.get("cache_ple_seconds"))],
            ["Min PLE (sec)", _fmt(pressure.get("min_ple"))],
            ["Memory grants pending", _fmt(pressure.get("memory_grants_pending"))],
        ],
    }

    tables["configuration"] = {
        "title": "SQL Server configuration (selected)",
        "columns": ["Setting", "Value"],
        "rows": [
            ["MAXDOP", _fmt(cfg.get("maxdop"))],
            ["Cost threshold for parallelism", _fmt(cfg.get("cost_threshold"))],
            ["Max server memory (MB)", _fmt(cfg.get("max_server_memory_mb"))],
        ],
    }

    if isinstance(instance, dict) and instance:
        rows = []
        for k in ["edition", "product_version", "product_level", "sql_start_time", "host_name"]:
            if k in instance:
                rows.append([k.replace("_", " ").title(), _fmt(instance.get(k))])
        if rows:
            tables["instance"] = {"title": "Instance / host summary", "columns": ["Field", "Value"], "rows": rows}

    return tables


def _derive_key_findings(ctx: Dict[str, Any]) -> List[str]:
    util = (ctx.get("utilization") or {}) if isinstance(ctx, dict) else {}
    pressure = (ctx.get("pressure") or {}) if isinstance(ctx, dict) else {}
    cfg = (ctx.get("configuration") or {}) if isinstance(ctx, dict) else {}

    findings: List[str] = []
    cpu = util.get("max_cpu_pct")
    mem = util.get("max_memory_pct")
    ple = util.get("cache_ple_seconds") or pressure.get("min_ple")
    grants = pressure.get("memory_grants_pending")

    try:
        if cpu is not None and float(cpu) >= 85:
            findings.append(f"CPU pressure observed: max CPU reached {float(cpu):.1f}%.")
        elif cpu is not None:
            findings.append(f"CPU utilization is within normal bounds: max CPU {float(cpu):.1f}%.")

        if mem is not None and float(mem) >= 85:
            findings.append(f"Memory pressure observed: max memory reached {float(mem):.1f}%.")
        elif mem is not None:
            findings.append(f"Memory utilization is within normal bounds: max memory {float(mem):.1f}%.")

        if ple is not None and float(ple) < 300:
            findings.append(f"Buffer cache pressure: PLE is low ({float(ple):.0f}s).")
        elif ple is not None:
            findings.append(f"Buffer cache stability: PLE {float(ple):.0f}s.")

        if grants is not None and float(grants) > 0:
            findings.append(f"Memory grants pending is non-zero ({float(grants):.0f}); potential query memory pressure.")
    except Exception:
        pass

    maxdop = cfg.get("maxdop")
    ctfp = cfg.get("cost_threshold")
    if maxdop is not None:
        findings.append(f"MAXDOP configured to {maxdop}.")
    if ctfp is not None:
        findings.append(f"Cost threshold for parallelism configured to {ctfp}.")

    if not findings:
        findings.append("Not available in this snapshot.")
    return findings


def _derive_remediation(ctx: Dict[str, Any]) -> List[str]:
    util = (ctx.get("utilization") or {}) if isinstance(ctx, dict) else {}
    pressure = (ctx.get("pressure") or {}) if isinstance(ctx, dict) else {}

    recs: List[str] = []
    cpu = util.get("max_cpu_pct")
    mem = util.get("max_memory_pct")
    ple = util.get("cache_ple_seconds") or pressure.get("min_ple")
    grants = pressure.get("memory_grants_pending")

    try:
        if cpu is not None and float(cpu) >= 85:
            recs.append("Investigate top CPU consumers (expensive queries, compilation, parallelism); validate indexes and query plans for regressions.")
        if mem is not None and float(mem) >= 85:
            recs.append("Review max server memory configuration and OS headroom; validate buffer pool and plan cache behavior.")
        if ple is not None and float(ple) < 300:
            recs.append("Investigate memory pressure drivers (large scans, missing indexes, suboptimal joins); consider targeted indexing and query tuning.")
        if grants is not None and float(grants) > 0:
            recs.append("Identify queries with large memory grants and reduce spill risk (statistics, indexing, row estimates).")
    except Exception:
        pass

    if not recs:
        recs.append("Continue baseline monitoring and trend analysis; no immediate remediation indicated from this snapshot.")
    return recs


def _enrich_payload(payload: Dict[str, Any], ctx: Dict[str, Any], section_order: List[str]) -> Dict[str, Any]:
    """Hybrid enrichment:
    - If the snapshot has metrics, inject deterministic tables + evidence-driven findings.
    - If metrics are missing, do NOT force 'Not available' everywhere; let the LLM provide narrative placeholders.
    """
    has_metrics = _has_any_metric(ctx)
    tables = _build_tables_from_evidence(ctx) if has_metrics else {}
    findings = _derive_key_findings(ctx) if has_metrics else []
    remediation = _derive_remediation(ctx) if has_metrics else []

    sec_list = payload.get("sections") or []
    if not isinstance(sec_list, list):
        return payload

    def _get_sec(h: str) -> Optional[Dict[str, Any]]:
        for s in sec_list:
            if isinstance(s, dict) and str(s.get("heading","")).strip() == h:
                return s
        return None

    s1 = _get_sec("1. Executive Summary")
    if s1 is not None and "key_metrics" in tables:
        s1["tables"] = (s1.get("tables") or []) + [tables["key_metrics"]]

    s2 = _get_sec("2. Environment Overview")
    if s2 is not None and "configuration" in tables:
        add = [tables["configuration"]]
        if "instance" in tables:
            add.insert(0, tables["instance"])
        s2["tables"] = (s2.get("tables") or []) + add

    s3 = _get_sec("3. Observed Performance Characteristics")
    if s3 is not None and "key_metrics" in tables:
        s3["tables"] = (s3.get("tables") or []) + [tables["key_metrics"]]

    s4 = _get_sec("4. Key Findings")
    if s4 is not None and findings:
        existing = s4.get("bullets") or []
        if not isinstance(existing, list):
            existing = [str(existing)]
        s4["bullets"] = findings + existing

    s5 = _get_sec("5. Remediation Plan")
    if s5 is not None and remediation:
        existing = s5.get("bullets") or []
        if not isinstance(existing, list):
            existing = [str(existing)]
        s5["bullets"] = remediation + existing

    return payload


def _render_into_template(template_bytes: bytes, payload: Dict[str, Any], *, server_name: str, snapshot: str, wanted_order: List[str]) -> bytes:
    doc = Document(BytesIO(template_bytes))

    # 1) Cover/title area = first two paragraphs in your template.
    title = str(payload.get("title") or f"Server Health Assessment Report — {server_name}").strip()
    subtitle_lines = payload.get("subtitle_lines") or []
    if not isinstance(subtitle_lines, list):
        subtitle_lines = [str(subtitle_lines)]
    subtitle = "\n".join([str(x) for x in subtitle_lines if str(x).strip()]).strip()
    if not subtitle:
        subtitle = f"Server: {server_name}\nSnapshot: {snapshot}"

    # Replace cover text but KEEP formatting runs (first run formatting)
    for i, text in [(0, title), (1, subtitle)]:
        if i >= len(doc.paragraphs):
            doc.add_paragraph("")
        p = doc.paragraphs[i]
        if p.runs:
            p.runs[0].text = text
            for r in p.runs[1:]:
                r.text = ""
        else:
            p.add_run(text)

    style = _load_style_prompt()
    # IMPORTANT: we render ONLY headings that exist in the template.
    # wanted_order is derived from the template itself by caller.
    wanted_order = list(wanted_order or [])

    # 3) Build heading -> content from payload
    sections = payload.get("sections") or []
    sec_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(sections, list):
        for s in sections:
            if isinstance(s, dict) and s.get("heading"):
                sec_map[str(s["heading"]).strip()] = s

    # 4) Determine section boundaries *in the actual document*
    # IMPORTANT: we mutate doc.paragraphs while filling sections. To avoid index drift,
    # we compute boundaries once and then process sections from bottom → top.
    heading_positions = _find_heading_positions(doc)
    headings_in_doc = [h for h in wanted_order if h in heading_positions]
    bounds = _section_boundaries(doc, headings_in_doc)

    # Global exemplars (first body paragraph after Executive Summary, first bullet)
    global_body = doc.paragraphs[4] if len(doc.paragraphs) > 4 else doc.paragraphs[0]
    global_bullet = doc.paragraphs[5] if len(doc.paragraphs) > 5 else global_body

    # 5) For each section: delete existing content between headings, then insert new content using exemplars
    # Process bottom → top so deletions/insertions do not invalidate indices for yet-to-be-processed headings.
    ordered = [h for h in headings_in_doc if h in bounds]
    ordered.sort(key=lambda h: bounds[h][0], reverse=True)
    for heading in ordered:
        if heading not in bounds:
            continue
        start, end = bounds[heading]
        # identify exemplars BEFORE deleting
        body_ex, bullet_ex = _pick_exemplars(doc, start, end, global_body, global_bullet)

        # delete paragraphs between (start+1 .. end-1)
        # iterate from end-1 downwards for safe removal
        for pi in range(end - 1, start, -1):
            try:
                _remove_paragraph(doc.paragraphs[pi])
            except Exception:
                pass

        # insert new content immediately after heading
        if start < 0 or start >= len(doc.paragraphs):
            continue
        anchor = doc.paragraphs[start]

        sec = sec_map.get(heading) or {}
        paras = sec.get("paragraphs") or []
        bullets = sec.get("bullets") or []

        if not isinstance(paras, list):
            paras = [str(paras)]
        if not isinstance(bullets, list):
            bullets = [str(bullets)]

        inserted_any = False

        for t in [str(x).strip() for x in paras if str(x).strip()]:
            anchor = _clone_paragraph_after(anchor, body_ex, t)
            inserted_any = True

        for b in [str(x).strip() for x in bullets if str(x).strip()]:
            anchor = _clone_paragraph_after(anchor, bullet_ex, b)
            inserted_any = True

        # Tables (optional)
        tables = sec.get("tables") or []
        if isinstance(tables, dict):
            tables = [tables]
        if isinstance(tables, list) and tables:
            for tb in tables:
                if not isinstance(tb, dict):
                    continue
                title = str(tb.get("title") or "").strip()
                cols = tb.get("columns") or []
                rows = tb.get("rows") or []
                if not isinstance(cols, list) or not cols:
                    continue
                if not isinstance(rows, list):
                    continue

                # Add a table title (as body paragraph)
                if title:
                    anchor = _clone_paragraph_after(anchor, body_ex, title)
                    inserted_any = True

                # Insert a Word table right after the current anchor paragraph
                try:
                    table = doc.add_table(rows=1, cols=len(cols))
                    # Style names vary by template; set only if available
                    try:
                        table.style = "Table Grid"
                    except Exception:
                        pass
                    hdr_cells = table.rows[0].cells
                    for j, c in enumerate(cols):
                        hdr_cells[j].text = str(c)

                    for r in rows:
                        if not isinstance(r, list):
                            continue
                        row_cells = table.add_row().cells
                        for j in range(len(cols)):
                            val = r[j] if j < len(r) else ""
                            row_cells[j].text = "" if val is None else str(val)

                    anchor._p.addnext(table._tbl)
                    inserted_any = True
                except Exception:
                    # If table insertion fails, fall back to a simple note
                    anchor = _clone_paragraph_after(anchor, bullet_ex, "(Table rendering failed — falling back to text.)")
                    inserted_any = True

        if not inserted_any:
            anchor = _clone_paragraph_after(anchor, body_ex, "Not available in this snapshot.")

        # add a blank spacer paragraph (matches template rhythm)
        _clone_paragraph_after(anchor, body_ex, "")

    out = BytesIO()
    doc.save(out)
    return out.getvalue()


# =========================
# Public API
# =========================
def build_report_plan(server_name: str) -> str:
    """
    Keep existing behavior: plan is markdown (preview only).
    """
    style = _load_style_prompt()
    snapshot = _get_latest_snapshot(server_name)
    profile = build_server_profile(server_name)
    ctx = _to_jsonable(_compact_context(profile))

    system = (
        "You are a senior SQL Server performance engineer. "
        "Produce deterministic, structured plans that map evidence to report sections."
    )

    user_payload = {
        "task": "Build a report-generation plan for a Server Health Assessment report.",
        "server_name": server_name,
        "rules": style.get("hard_rules", []),
        "section_order": style.get("section_order", []),
        "tables": style.get("tables", {}),
        "output_format": {
            "type": "markdown",
            "must_include": [
                "A 2-3 line approach summary",
                "A numbered plan (<=12 steps)",
                "A section-by-section mapping (use evidence keys, not sheet names)",
            ],
        },
        "evidence_json": ctx,
    }

    plan = chat_completion(
        [{"role": "system", "content": system},
         {"role": "user", "content": json.dumps(user_payload, indent=2)[:9000]}],
        temperature=0.1,
        max_tokens=900,
    ).strip()

    return plan or "(LLM returned empty plan.)"


def generate_report_docx_bytes(server_name: str) -> bytes:
    """
    Deterministic report generation:
      - Evidence is deterministic (metrics_service profile).
      - Model output is constrained to strict JSON.
      - DOCX layout is controlled by the template sections (we replace content in-place).
    """
    snapshot = _get_latest_snapshot(server_name) or ""
    profile = build_server_profile(server_name)
    ctx = _to_jsonable(_compact_context(profile))

    tpl_bytes = _load_template_docx_bytes()

    # Derive section headings from the template itself (deterministic structure).
    wanted_order = _extract_template_headings(tpl_bytes)

    # If LLM fails, we still render with 'Not available...' but in TEMPLATE STRUCTURE.
    try:
        t0 = time.time()
        payload = _draft_report_json(server_name, ctx, wanted_order)
        payload = _enrich_payload(payload, ctx, wanted_order)
        _ = time.time() - t0
    except Exception as e:
        style = _load_style_prompt()
        # Build a minimal payload that still fills all sections deterministically
        payload = {
            "title": style.get("report_title_template", "Server Health Assessment Report — {server_name}").format(server_name=server_name),
            "subtitle_lines": [f"Server: {server_name}", f"Snapshot: {snapshot}"],
            "sections": [
                {"heading": h, "paragraphs": ["Not available in this snapshot."], "bullets": []}
                for h in (wanted_order or [])
            ],
            "_error": repr(e),
        }
        try:
            payload = _enrich_payload(payload, ctx, wanted_order)
        except Exception:
            pass

    return _render_into_template(tpl_bytes, payload, server_name=server_name, snapshot=snapshot, wanted_order=wanted_order)
