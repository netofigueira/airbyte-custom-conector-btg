# BTG Airbyte Source

## Configuration
```json
{
  "categories": {
    "gestora": {
      "enabled": true,
      "client_id": "your_client_id",
      "client_secret": "your_secret"
    }
  },
  "sync_schedule": {
    "start_date": "2025-08-15",
    "end_date": "2025-08-15"
  },
  "endpoints": {
    "movimentacao_fundo_d0": {"enabled": true, "params": {...}},
    "renda_fixa": {"enabled": true, "params": {...}}
  }
}