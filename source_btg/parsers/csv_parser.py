from typing import List, Mapping
import pandas as pd
from io import BytesIO


def parse_csv(payload: bytes, sep: str = ",", encoding: str = "utf-8", header: bool = True) -> List[Mapping]:
    opts = {"sep": sep, "dtype": str}
    try:
        df = pd.read_csv(BytesIO(payload), encoding=encoding, **opts) if header else pd.read_csv(BytesIO(payload), encoding=encoding, header=None, **opts)
    except Exception:
        # fallback para latin1
        df = pd.read_csv(BytesIO(payload), encoding="latin1", **opts) if header else pd.read_csv(BytesIO(payload), encoding="latin1", header=None, **opts)
    return df.fillna("").to_dict(orient="records")