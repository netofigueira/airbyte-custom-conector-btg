# source_btg/__init__.py

"""
BTG Pactual Source Connector for Airbyte.

This package provides a multi-category connector for extracting data
from BTG Pactual's investment platform APIs.

Supported categories:
- GESTORA: Fund management data
- ALL: General investment data  
- LIQUIDOS: Liquid investment products
- CE: Corporate entities
- DL: Data lake integration

Features:
- Multi-category authentication
- Asynchronous job processing
- XML/JSON/ZIP response parsing
- Incremental data extraction
- Robust error handling and retries
"""

from .source import SourceBtg

__version__ = "0.1.0"
__all__ = ["SourceBtg"]


