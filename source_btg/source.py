from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import ConnectorSpecification
from typing import Any, List, Mapping, Tuple

from .streams.base_async import AsyncJobStream
from .auth import BTGTokenProvider
from .streams.endpoint_configs import ENDPOINT_CONFIGS
import logging

class SourceBtg(AbstractSource):

    # ---------- SPEC ----------
    def spec(self, logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/sources/btg",
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "BTG API Source",
                "type": "object",
                "required": ["base_url", "auth"],
                "additionalProperties": False,
                "properties": {
                    "base_url": {
                        "type": "string",
                        "title": "Base URL",
                        "description": "BTG API base URL",
                        "default": "https://funds.btgpactual.com"
                    },
                    "auth": {
                        "type": "object",
                        "title": "Authentication",
                        "required": ["client_id", "client_secret"],
                        "additionalProperties": False,
                        "properties": {
                            "client_id": {"type": "string", "title": "Client ID"},
                            "client_secret": {"type": "string", "title": "Client Secret", "airbyte_secret": True}
                        }
                    },
                    
                    # === ENDPOINTS COMO CHECKBOXES SIMPLES ===
                    "enable_cadastro_fundos": {
                        "type": "boolean",
                        "title": "Enable Cadastro Fundos",
                        "description": "Sync fund registry data",
                        "default": True
                    },
                    "enable_fluxo_caixa": {
                        "type": "boolean", 
                        "title": "Enable Fluxo Caixa",
                        "description": "Sync cashflow data (requires dates)",
                        "default": True
                    },
                    "enable_movimentacao_fundo_d0": {
                        "type": "boolean",
                        "title": "Enable Movimentacao Fundo D0", 
                        "description": "Sync fund movements D0",
                        "default": False
                    },
                    "enable_carteira": {
                        "type": "boolean",
                        "title": "Enable Carteira",
                        "description": "Sync portfolio data",
                        "default": False
                    },
                    "enable_renda_fixa": {
                        "type": "boolean",
                        "title": "Enable Renda Fixa",
                        "description": "Sync fixed income data", 
                        "default": False
                    },
                    "enable_extrato_cc": {
                        "type": "boolean",
                        "title": "Enable Extrato CC",
                        "description": "Sync checking account statements",
                        "default": False
                    },
                    "enable_money_market": {
                        "type": "boolean",
                        "title": "Enable Money Market",
                        "description": "Sync money market data",
                        "default": False
                    },
                    "enable_taxa_performance": {
                        "type": "boolean",
                        "title": "Enable Taxa Performance",
                        "description": "Sync performance rate data",
                        "default": False
                    },
                    
                    # === MOVIMENTACAO FUNDO D0 PARAMS ===
                    "movimentacao_consult_types": {
                        "type": "string",
                        "title": "Movimentacao Consult Types",
                        "description": "Tipo de consulta para mov. fundo d0 ex: LANCAMENTO",
                        "default": "LANCAMENTO",
                        "examples": ["LANCAMENTO"]
                    },
                    "movimentacao_status": {
                        "type": "string", 
                        "title": "Movimentacao Status",
                        "description": "Status da consulta de mov. fundo d0.",
                        "default": "TODOS",
                        "examples": ["TODOS", "LIQUIDADO"]
                    },
                    
                    # ===== PARAMETRO DE TIPO DE REPORT DA CARTEIRA ========

                    "carteira_type_report":{

                        "type":"integer",
                        "title": "typeReport - rota Carteira",
                        "description": "define o tipo de report do endpoint de carteira.",
                        "default": 3,
                        "examples": [1,2,3]
                    },

                    "fund_name": {

                        "type":"string",
                        "title":"fundName - usado nas rotas carteira e taxa performance",
                        "description": "define o nome do fundo para requisicao",
                        "default": "RIZA MEYENII RFX FIM",
                        "examples": ["RIZA MEYENII RFX FIM"]
                    },


                    # === DATE RANGE ===
                    "start_date": {
                        "type": "string",
                        "format": "date", 
                        "title": "Start Date",
                        "description": "Start date for date-enabled endpoints",
                        "examples": ["2024-01-15"]
                    },
                    "end_date": {
                        "type": "string",
                        "format": "date",
                        "title": "End Date", 
                        "description": "End date for date-enabled endpoints",
                        "examples": ["2024-01-17"]
                    },
                    "date_step_days": {
                        "type": "integer",
                        "title": "Date Step Days",
                        "description": "Days per sync step",
                        "default": 1,
                        "minimum": 1
                    },
                    
                    # === CATEGORY (SIMPLE) ===
                    "category": {
                        "type": "string",
                        "title": "Category",
                        "description": "BTG API Category", 
                        "enum": [ "GESTORA", "ALL", "LIQUIDOS", "CE", "DL"],
                        "default": "GESTORA"
                    },
                    
                    # === ADVANCED ===
                    "max_retries": {
                        "type": "integer",
                        "title": "Max Retries",
                        "default": 3,
                        "minimum": 0
                    },
                    "timeout_seconds": {
                        "type": "integer", 
                        "title": "Timeout Seconds",
                        "default": 300,
                        "minimum": 30
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
        logger = logging.getLogger("airbyte")

        base_url = config["base_url"]
        
        # === CONVERSÃO UI → ESTRUTURA QUE SEU CÓDIGO ESPERA ===
        
        # Manter auth como está (compatibilidade)
        if "auth" not in config:
            # Fallback se vier no formato antigo
            config["auth"] = {
                "client_id": config.get("client_id"),
                "client_secret": config.get("client_secret") 
            }
        
        # Converter category simples → categories object
        category = config.get("category", "DEFAULT")
        categories = config.get("categories", {}) or {category: {"enabled": True}}
        
        # Converter checkboxes → endpoints object
        endpoints_cfg = config.get("endpoints", {})
        if not endpoints_cfg:  # Se não tem endpoints no config, usar checkboxes
            endpoints_cfg = {}
            

            fund_name = config.get("fund_name", "RIZA MEYENII RFX FIM")

            if config.get("enable_carteira", False):
                endpoints_cfg["carteira"] = {
                    "enabled": True,
                    "params": {
                        "report_type": config.get("carteira_type_report", 3),
                        "fund_name": fund_name  # Reutiliza
                    }
                }

            if config.get("enable_taxa_performance", False):
                endpoints_cfg["taxa_performance"] = {
                    "enabled": True,
                    "params": {
                        "fund_name": fund_name  # Mesmo campo
                    }
                }

            if config.get("enable_cadastro_fundos", True):
                endpoints_cfg["cadastro_fundos"] = {"enabled": True}
                
            if config.get("enable_fluxo_caixa", True):
                endpoints_cfg["fluxo_caixa"] = {"enabled": True}
                
            if config.get("enable_movimentacao_fundo_d0", False):
                # Converter strings "1,2,3" → arrays [1,2,3]
                consult_types_str = config.get("movimentacao_consult_types", "LANCAMENTO")
                status_str = config.get("movimentacao_status", "TODOS")
                
                consult_types = [x.strip() for x in consult_types_str.split(",") if x.strip()]
                status_list = [x.strip() for x in status_str.split(",") if x.strip()]
                
                endpoints_cfg["movimentacao_fundo_d0"] = {
                    "enabled": True,
                    "params": {
                        "consult_type": consult_types,
                        "status": status_list
                    }
                }
                
            if config.get("enable_carteira", False):
                endpoints_cfg["carteira"] = {"enabled": True}
                
            if config.get("enable_renda_fixa", False):
                endpoints_cfg["renda_fixa"] = {"enabled": True}
                
            if config.get("enable_extrato_cc", False):
                endpoints_cfg["extrato_cc"] = {"enabled": True}
                
            if config.get("enable_money_market", False):
                endpoints_cfg["money_market"] = {"enabled": True}
                
            if config.get("enable_taxa_performance", False):
                endpoints_cfg["taxa_performance"] = {"enabled": True}
        
        # Construir sync_schedule se não existir
        if "sync_schedule" not in config:
            config["sync_schedule"] = {
                "start_date": config.get("start_date"),
                "end_date": config.get("end_date"), 
                "date_step_days": config.get("date_step_days", 1)
            }
        
        # Atualizar config com estruturas convertidas
        config["categories"] = categories
        config["endpoints"] = endpoints_cfg
        
        # === RESTO DO CÓDIGO EXATAMENTE IGUAL AO ATUAL ===
        
        for category_name, category_cfg in categories.items():
            if not category_cfg.get("enabled", False):
                continue

            token_provider = self._make_token_provider(config, category_name, category_cfg)

            for endpoint_name, ep_cfg in endpoints_cfg.items():
                if not ep_cfg.get("enabled", True):
                    continue
                if endpoint_name not in ENDPOINT_CONFIGS:
                    logger.warning(f"endpoint '{endpoint_name}' não existe no ENDPOINT_CONFIGS")
                    continue

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
                logger.info(f"Created stream: {stream_name}")

        logger.info(f"Created {len(streams)} streams total")
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
