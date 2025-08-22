from datetime import datetime, timedelta
from typing import Iterable, Any




def daterange(start: str, end: str | None, step_days: int = 1) -> Iterable[datetime]:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d") if end else datetime.now()
    d = timedelta(days=step_days)
    cur = s
    while cur <= e:
        yield cur
        cur += d



def dot_get(obj: Any, path: str, default=None):
    if path is None or path == "":
        return obj
    cur = obj
    for part in path.split('.'):
        if isinstance(cur, list):
            cur = cur[0] if cur else default
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur



def expand_templates(data: Any, ctx: dict):
    """Expande placeholders {chave} recursivamente em dict/list/str."""

    if isinstance(data, dict):
        return {k: expand_templates(v, ctx) for k, v in data.items()}
    if isinstance(data, list):
        return [expand_templates(v, ctx) for v in data]
    if isinstance(data, str):
        try:
            return data.format(**ctx)
        except Exception:
            return data
    return data