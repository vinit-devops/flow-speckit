import hashlib
import json
from collections.abc import Mapping
from typing import Any


def canonical_hash(content: Mapping[str, Any]) -> str:
    payload = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()
