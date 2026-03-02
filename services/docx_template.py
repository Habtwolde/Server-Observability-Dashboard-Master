# services/docx_template.py
from __future__ import annotations

from io import BytesIO
from typing import Dict

from docx import Document


def _iter_all_paragraphs(doc: Document):
    # Body paragraphs
    for p in doc.paragraphs:
        yield p
    # Table paragraphs
    for t in doc.tables:
        for row in t.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    yield p
    # Headers/footers paragraphs (all sections)
    for s in doc.sections:
        for p in s.header.paragraphs:
            yield p
        for p in s.footer.paragraphs:
            yield p


def _replace_in_paragraph_runs(paragraph, mapping: Dict[str, str]) -> None:
    """
    Replace placeholders in a paragraph while preserving the existing run formatting as much as possible.

    Note: Exact, formatting-preserving replacement in Word is tricky if placeholders are split across runs.
    This function merges runs' text logically, then writes back into the first run and clears the rest.
    This approach preserves paragraph style and the first run's formatting.
    """
    if not paragraph.runs:
        return

    full_text = "".join(r.text for r in paragraph.runs)
    new_text = full_text

    for k, v in mapping.items():
        if k in new_text:
            new_text = new_text.replace(k, v)

    if new_text == full_text:
        return

    # Write everything into first run to keep formatting stable
    paragraph.runs[0].text = new_text
    for r in paragraph.runs[1:]:
        r.text = ""


def render_docx_from_template(template_bytes: bytes, mapping: Dict[str, str]) -> bytes:
    """
    Load DOCX template bytes, replace placeholders everywhere (body + tables + header/footer),
    and return new DOCX bytes.
    """
    src = BytesIO(template_bytes)
    doc = Document(src)

    # Replace placeholders in all paragraphs (including inside table cells + header/footer)
    for p in _iter_all_paragraphs(doc):
        _replace_in_paragraph_runs(p, mapping)

    out = BytesIO()
    doc.save(out)
    return out.getvalue()