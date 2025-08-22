from typing import List, Mapping
import json
from ..utils import dot_get


def parse_json(payload: bytes, record_path: str | None) -> List[Mapping]:
    js = json.loads(payload.decode("utf-8", errors="ignore") or "{}")
    node = dot_get(js, record_path) if record_path else js
    if isinstance(node, list):
        return [x if isinstance(x, dict) else {"value": x} for x in node]
    if isinstance(node, dict):
        return [node]
    return [{"value": node}]