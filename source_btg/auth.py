# source_btg/auth.py - Versão multi-categorias

import requests
import time
from typing import Mapping, Any

class BTGTokenProvider:
    """Provider de token com suporte a múltiplas categorias BTG"""
    
    def __init__(self, config: Mapping[str, Any], category: str = "gestora"):
        self.config = config
        self.category = category
        self.token = None
        self.token_expires_at = 0
        
        # URLs diferentes por categoria (se necessário)
        self.auth_urls = {
            "gestora": "https://funds.btgpactual.com/connect/token",
            "all": "https://funds.btgpactual.com/connect/token", 
            "liquidos": "https://funds.btgpactual.com/connect/token",
            "ce": "https://funds.btgpactual.com/connect/token",
            "dl": "https://funds.btgpactual.com/connect/token"
        }
        
        print(f"🔐 Initialized BTG auth for category: {category.upper()}")

    def get(self) -> str:
        """Retorna token válido, renovando se necessário"""
        
        # Verificar se token ainda é válido (com margem de 5 minutos)
        if self.token and time.time() < (self.token_expires_at - 300):
            return self.token
        
        # Renovar token
        self._refresh_token()
        return self.token

    def _refresh_token(self):
        """Renova o token de autenticação usando OAuth2 Client Credentials"""
        
        auth_url = self.auth_urls.get(self.category, self.auth_urls["gestora"])
        
        # Credenciais específicas da categoria (OAuth2 Client Credentials)
        client_id = self.config.get("client_id")
        client_secret = self.config.get("client_secret")
        
        if not all([client_id, client_secret]):
            raise Exception(f"Missing client_id or client_secret for category {self.category}")
        
        print(f"🔄 Refreshing token for {self.category.upper()}...")
        
        # Payload OAuth2 Client Credentials
        auth_payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        
        try:
            response = requests.post(
                auth_url,
                data=auth_payload,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json"
                },
                timeout=30
            )
            
            print(f"🔐 Auth response status: {response.status_code}")
            print(f"🔐 Auth response: {response.text}")
            
            if response.status_code == 200:
                auth_data = response.json()
                
                self.token = auth_data.get("access_token")
                expires_in = auth_data.get("expires_in", 3600)  # Default 1 hora
                self.token_expires_at = time.time() + expires_in
                
                print(f"✅ Token refreshed for {self.category.upper()}")
                print(f"   Expires in: {expires_in}s")
                print(f"   Token: {self.token[:20]}..." if self.token else "   Token: None")
                
                if not self.token:
                    raise Exception("No access_token in response")
                    
            else:
                error_msg = f"Auth failed: {response.status_code} - {response.text}"
                print(f"❌ {error_msg}")
                raise Exception(error_msg)
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Auth request failed: {str(e)}"
            print(f"❌ {error_msg}")
            raise Exception(error_msg)

    def invalidate(self):
        """Invalida o token atual"""
        self.token = None
        self.token_expires_at = 0
        print(f"🗑️  Token invalidated for {self.category.upper()}")


class BTGMultiCategoryAuthManager:
    """Gerenciador de autenticação para múltiplas categorias"""
    
    def __init__(self, categories_config: Mapping[str, Any]):
        self.providers = {}
        
        for category, config in categories_config.items():
            if config.get("enabled", False):
                self.providers[category] = BTGTokenProvider(config, category)
                
        print(f"🔐 Initialized auth for {len(self.providers)} categories: {list(self.providers.keys())}")
    
    def get_token(self, category: str) -> str:
        """Retorna token para categoria específica"""
        if category not in self.providers:
            raise Exception(f"Category {category} not configured or not enabled")
            
        return self.providers[category].get()
    
    def get_provider(self, category: str) -> BTGTokenProvider:
        """Retorna provider para categoria específica"""
        if category not in self.providers:
            raise Exception(f"Category {category} not configured or not enabled")
            
        return self.providers[category]
    
    def test_all_categories(self) -> dict:
        """Testa autenticação em todas as categorias"""
        results = {}
        
        for category, provider in self.providers.items():
            try:
                token = provider.get()
                results[category] = {"status": "success", "token_length": len(token)}
                print(f"✅ {category.upper()}: Auth successful")
                
            except Exception as e:
                results[category] = {"status": "failed", "error": str(e)}
                print(f"❌ {category.upper()}: Auth failed - {e}")
        
        return results
    
    def invalidate_all(self):
        """Invalida tokens de todas as categorias"""
        for provider in self.providers.values():
            provider.invalidate()
        print("🗑️  All tokens invalidated")


# Exemplo de uso e teste
if __name__ == "__main__":
    # Configuração de exemplo
    config_example = {
        "categories": {
            "gestora": {
                "enabled": True,
                "client_id": "gestora_client_id", 
                "client_secret": "gestora_secret",
                "username": "gestora_user",
                "password": "gestora_pass"
            },
            "liquidos": {
                "enabled": True,
                "client_id": "liquidos_client_id",
                "client_secret": "liquidos_secret", 
                "username": "liquidos_user",
                "password": "liquidos_pass"
            },
            "all": {
                "enabled": False,  # Desabilitado
                "client_id": "all_client_id",
                "client_secret": "all_secret",
                "username": "all_user", 
                "password": "all_pass"
            }
        }
    }
    
    # Testar autenticação
    auth_manager = BTGMultiCategoryAuthManager(config_example["categories"])
    results = auth_manager.test_all_categories()
    
    print("\n📊 Test Results:")
    for category, result in results.items():
        status = "✅" if result["status"] == "success" else "❌"
        print(f"   {status} {category.upper()}: {result}")