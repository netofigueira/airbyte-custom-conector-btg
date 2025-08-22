"""
Stream implementations for BTG Pactual API integration.

This module contains the core stream classes that handle:
- Asynchronous job submission and polling
- Multi-format response parsing (XML, JSON, ZIP)
- Template-based parameter expansion
- Category-aware data extraction
"""

from .base_async import AsyncJobStream

__all__ = ["AsyncJobStream"]
