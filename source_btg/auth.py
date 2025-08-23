# source_btg/auth.py
import logging
import requests
import time
from typing import Mapping, Any, Optional

log = logging.getLogger("airbyte")


class BTGTokenProvider:
    """Provider de token com suporte a múltiplas categorias BTG"""

    DEFAULT_AUTH_URLS = {
        "gestora":  "https://funds.btgpactual.com/connect/token",
        "all":      "https://funds.btgpactual.com/connect/token",
        "liquidos": "https://funds.btgpactual.com/connect/token",
        "ce":       "https://funds.btgpactual.com/connect/token",
        "dl":       "https://funds.btgpactual.com/connect/token",
    }

    def __init__(self, config: Mapping[str, Any], category: str = "gestora"):
        self.config = dict(config or {})
        self.category = category or "gestora"
        self.category_key = self.category.lower()

        self.token: Optional[str] = None
        self.token_expires_at: float = 0.0

        # ordem de preferência para o endpoint de token
        self.auth_url = (
            self.config.get("auth_url")
            or self.DEFAULT_AUTH_URLS.get(self.category_key)
            or self._derive_auth_from_base(self.config.get("base_url"))
        )

        log.info(f"🔐 BTG auth init category={self.category} auth_url={self.auth_url}")

    @staticmethod
    def _derive_auth_from_base(base_url:Optional[str]) -> str:
        if not base_url:
            return "https://funds.btgpactual.com/connect/token"
        base = base_url.rstrip("/")
        # se o base_url vier com /reports/... removemos o sufixo para montar /connect/token
        if "/reports" in base:
            base = base.split("/reports")[0]
        return base.rstrip("/") + "/connect/token"

    def get(self) -> str:
        """Retorna token válido (renova se estiver a <5min de expirar)."""
        if self.token and time.time() < (self.token_expires_at - 300):
            return self.token
        self._refresh_token()
        return self.token  # type: ignore[return-value]

    def _refresh_token(self) -> None:
        client_id = self.config.get("client_id")
        client_secret = self.config.get("client_secret")
        if not client_id or not client_secret:
            raise Exception(f"Missing client_id/client_secret for category {self.category}")

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        try:
            log.info(f"🔄 Refreshing token category={self.category}")
            r = requests.post(
                self.auth_url,
                data=payload,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
                timeout=30,
            )
            log.debug(f"Auth status={r.status_code} body={r.text[:300]}")
            r.raise_for_status()

            data = r.json()
            token = data.get("access_token")
            if not token:
                raise Exception("No access_token in response")

            expires_in = int(data.get("expires_in", 3600))
            self.token = token
            self.token_expires_at = time.time() + expires_in

            log.info(f"✅ Token ok category={self.category} expires_in={expires_in}s (tok:{token[:12]}…)")

        except requests.RequestException as e:
            msg = f"Auth request failed: {e}"
            log.error(msg)
            raise Exception(msg) from e

    def invalidate(self) -> None:
        self.token = None
        self.token_expires_at = 0
        log.info(f"🗑️  Token invalidated for {self.category}")


class BTGMultiCategoryAuthManager:
    """Gerencia vários providers por categoria (opcional)."""

    def __init__(self, categories_config: Mapping[str, Any]):
        self.providers: dict[str, BTGTokenProvider] = {}
        for category, cfg in (categories_config or {}).items():
            if cfg.get("enabled"):
                self.providers[category] = BTGTokenProvider(cfg, category)
        log.info(f"🔐 Auth providers prontos: {list(self.providers.keys())}")

    def get_provider(self, category: str) -> BTGTokenProvider:
        if category not in self.providers:
            raise Exception(f"Category {category} not configured or not enabled")
        return self.providers[category]

    def get_token(self, category: str) -> str:
        return self.get_provider(category).get()

    def invalidate_all(self) -> None:
        for p in self.providers.values():
            p.invalidate()
        log.info("🗑️  All tokens invalidated")
