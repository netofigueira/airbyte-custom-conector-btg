from typing import List, Mapping
import xmltodict
from ..utils import dot_get


def parse_xml(payload: bytes, record_path: str | None) -> List[Mapping]:
    doc = xmltodict.parse(payload)
    if not record_path:
        return [{"xml_raw": xmltodict.unparse(doc)}]
    node = dot_get(doc, record_path)
    if node is None:
        return []
    if isinstance(node, list):
        return [x if isinstance(x, dict) else {"value": x} for x in node]
    return [node if isinstance(node, dict) else {"value": node}]