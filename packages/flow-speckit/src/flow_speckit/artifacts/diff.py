from __future__ import annotations

import difflib
import json
from typing import Any

from deepdiff import DeepDiff
from pydantic import BaseModel


class ArtifactDiff(BaseModel):
    a: str
    b: str
    structured: dict[str, Any]
    text: str


def compute_diff(
    address_a: str,
    content_a: dict[str, Any],
    body_md_a: str | None,
    address_b: str,
    content_b: dict[str, Any],
    body_md_b: str | None,
) -> ArtifactDiff:
    structured = json.loads(DeepDiff(content_a, content_b, ignore_order=False).to_json())
    text = "".join(
        difflib.unified_diff(
            (body_md_a or "").splitlines(keepends=True),
            (body_md_b or "").splitlines(keepends=True),
            fromfile=address_a,
            tofile=address_b,
        )
    )
    return ArtifactDiff(a=address_a, b=address_b, structured=structured, text=text)
