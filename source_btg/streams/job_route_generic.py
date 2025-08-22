from typing import Mapping
from .base_async import AsyncJobStream


class JobRouteGeneric(AsyncJobStream):
    """Stream genérica configurada via item em `routes` do config."""
    def __init__(self, config: Mapping, token_provider, route: Mapping):
        super().__init__(config, token_provider, route)
