from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import ConnectorSpecification
from typing import Any, List, Mapping, Tuple

from .streams.base_async import AsyncJobStream
from .auth import BTGTokenProvider
from .streams.endpoint_configs import ENDPOINT_CONFIGS


class SourceBtg(AbstractSource):

    # ---------- SPEC ----------
    def spec(self, logger) -> ConnectorSpecification:
        # personas removido; categories e sync_schedule passam a ser opcionais
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/sources/btg",
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "BTG Source Spec",
                "type": "object",
                "required": ["base_url", "auth", "endpoints"],
                "additionalProperties": False,
                "properties": {
                    "base_url": {
                        "type": "string",
                        "title": "Base URL",
                        "description": "Ex.: https://api.seubtg.com/v1",
                        "examples": ["https://api.seubtg.com/v1"]
                    },
                    "auth": {
                        "type": "object",
                        "title": "Auth",
                        "required": ["client_id", "client_secret"],
                        "additionalProperties": False,
                        "properties": {
                            "client_id": {"type": "string", "title": "Client ID"},
                            "client_secret": {"type": "string", "title": "Client Secret", "airbyte_secret": True}
                        }
                    },
                    "categories": {
                        "type": "object",
                        "title": "Categories (opcional)",
                        "description": "Overrides de credencial por categoria; deixe vazio para herdar de 'auth'.",
                        "patternProperties": {
                            "^[a-zA-Z_]+$": {
                                "type": "object",
                                "required": ["enabled"],
                                "additionalProperties": False,
                                "properties": {
                                    "enabled": {"type": "boolean", "default": True},
                                    "client_id": {"type": "string"},
                                    "client_secret": {"type": "string", "airbyte_secret": True}
                                }
                            }
                        },
                        "additionalProperties": False
                    },
                    "sync_schedule": {
                        "type": "object",
                        "title": "Date window (opcional; usado só por rotas que aceitam data)",
                        "required": [],
                        "additionalProperties": False,
                        "properties": {
                            "start_date": {"type": "string", "format": "date"},
                            "end_date": {"type": "string", "format": "date"},
                            "date_step_days": {"type": "integer", "default": 1, "minimum": 1}
                        }
                    },
                    "endpoints": {
                        "type": "object",
                        "title": "Endpoints",
                        "description": "Ative rotas e passe parâmetros. Valores podem ser string ou lista de strings.",
                        "patternProperties": {
                            "^[a-zA-Z_]+$": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "enabled": {"type": "boolean", "default": True},
                                    "params": {
                                        "type": "object",
                                        "additionalProperties": {
                                            "oneOf": [
                                                {"type": "string"},
                                                {"type": "array", "items": {"type": "string"}}
                                            ]
                                        }
                                    }
                                }
                            }
                        },
                        "additionalProperties": False
                    },
                    "technical": {
                        "type": "object",
                        "title": "Technical",
                        "additionalProperties": False,
                        "properties": {
                            "stream_strategy": {
                                "type": "string",
                                "enum": ["category_endpoint", "split_by_category"],
                                "default": "category_endpoint"
                            },
                            "max_retries": {"type": "integer", "default": 3, "minimum": 0},
                            "timeout_seconds": {"type": "integer", "default": 300, "minimum": 1}
                        }
                    }
                }
            }
        )

    # ---------- helpers ----------
    def _effective_auth(self, config: Mapping[str, Any], category_cfg: Mapping[str, Any]) -> dict:
        auth_cfg = config.get("auth", {}) or {}
        return {
            "client_id": category_cfg.get("client_id", auth_cfg.get("client_id")),
            "client_secret": category_cfg.get("client_secret", auth_cfg.get("client_secret")),
        }

    def _make_token_provider(self, config: Mapping[str, Any], category_name: str, category_cfg: Mapping[str, Any]) -> BTGTokenProvider:
        base_url = config["base_url"]
        creds = self._effective_auth(config, category_cfg)
        return BTGTokenProvider({**creds, "base_url": base_url}, category_name)

    # ---------- check ----------
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            if not config.get("base_url"):
                return False, "Missing 'base_url' in config"

            # fallback se categories vier vazio
            categories = config.get("categories", {}) or {"DEFAULT": {"enabled": True}}
            errors = []

            for category_name, category_cfg in categories.items():
                if not category_cfg.get("enabled", False):
                    continue
                try:
                    tk = self._make_token_provider(config, category_name, category_cfg)
                    _ = tk.get()
                    logger.info(f"✅ {category_name.upper()}: Connection successful")
                except Exception as e:
                    msg = f"{category_name.upper()}: {e}"
                    logger.error(f"❌ {msg}")
                    errors.append(msg)

            return (False, "; ".join(errors)) if errors else (True, None)

        except Exception as e:
            return False, str(e)

    # ---------- streams ----------
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams: List[Stream] = []

        base_url = config["base_url"]
        categories = config.get("categories", {}) or {"DEFAULT": {"enabled": True}}
        endpoints_cfg = config.get("endpoints", {}) or {}

        for category_name, category_cfg in categories.items():
            if not category_cfg.get("enabled", False):
                continue

            token_provider = self._make_token_provider(config, category_name, category_cfg)

            for endpoint_name, ep_cfg in endpoints_cfg.items():
                if not ep_cfg.get("enabled", True):
                    continue
                if endpoint_name not in ENDPOINT_CONFIGS:
                    print(f"⚠️ endpoint '{endpoint_name}' não existe no ENDPOINT_CONFIGS")
                    continue

                # merge de parâmetros: defaults do ENDPOINT_CONFIGS > params do usuário
                defaults = ENDPOINT_CONFIGS[endpoint_name].get("parameters", {}) or {}
                user_params = ep_cfg.get("params", {}) or {}
                merged_params = {**defaults, **user_params}

                route = self._create_route_config(endpoint_name, category_name)
                stream_name = f"{category_name}_{endpoint_name}"

                merged_config = {
                    **config,
                    "base_url": base_url,
                    "category_auth": self._effective_auth(config, category_cfg),
                    "current_endpoint": endpoint_name,
                    "current_category": category_name,
                    "endpoint_params": merged_params,
                }

                stream = CategoryAsyncJobStream(
                    config=merged_config,
                    token_provider=token_provider,
                    route={**route, "name": stream_name},
                    category=category_name,
                    endpoint=endpoint_name,
                )
                streams.append(stream)
                print(f"✅ Created stream: {stream_name}")

        print(f"📊 Created {len(streams)} streams total")
        return streams

    def _create_route_config(self, endpoint: str, category: str) -> dict:
        base_config = ENDPOINT_CONFIGS.get(endpoint, {})
        return {
            **base_config,
            "name": endpoint,
            "category": category,
            "ticket_path": "/reports/Ticket",
            "ticket_auth": "xsecure",
            "download_auth": "xsecure",
        }


class CategoryAsyncJobStream(AsyncJobStream):
    """AsyncJobStream com suporte a múltiplas categorias e endpoints"""

    def __init__(self, config, token_provider, route, category, endpoint):
        self.category = category
        self.endpoint = endpoint
        self._name = route.get("name", f"{category}_{endpoint}")  # garante name
        super().__init__(config, token_provider, route)

    @property
    def name(self) -> str:
        return self._name

    def read_records(self, stream_slice=None, **kwargs):
        for record in super().read_records(stream_slice, **kwargs):
            record["_category"] = self.category
            record["_endpoint"] = self.endpoint
            record["_source_category"] = self.category.upper()
            record["_api_endpoint"] = self.route.get("submit_path")
            yield record
