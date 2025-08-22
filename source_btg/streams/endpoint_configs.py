ENDPOINT_CONFIGS = {
    "cadastro_fundos": {
        "submit_path": "/reports/Fund",
        "submit_method": "POST",
        "submit_auth": "xsecure",
        "submit_body": {
            "contract":{
                
            }
        }
    },
    
    "renda_fixa": {
        "submit_path": "/reports/FixedIncome", 
        "submit_method": "POST",
        "submit_auth": "xsecure",

        "submit_body": {
            "contract": {
                "date": "{{date_iso}}",
            }
        }
    },
    
    "extrato_cc": {
        "submit_path": "/reports/Cash/FundAccountStatement",
        "submit_auth": "xsecure",
        "submit_method": "POST", 
        "submit_body": {
            "contract": {
                "startDate": "{{date_iso}}",
                "endDate": "{{date_iso}}"
            }
        }
    },
    
    "fluxo_caixa": {
        "submit_path": "/reports/Cash/Cashflow",
        "submit_auth": "xsecure",
        "submit_method": "POST",
        "submit_body": {
            "contract": {
                "startDate": "{{date_iso}}",
                "endDate": "{{date_iso}}"
            }
        }
    },
    
    "money_market": {
        "submit_path": "/reports/Cash/MoneyMarket",
        "submit_auth": "xsecure",
        "submit_method": "POST",
        "submit_body": {
            "contract": {
                 "date": "{{date_iso}}"
            }
        }
    },
    
    "movimentacao_passivo": {
        "submit_path": "/reports/RTA/FundFlow",
        "submit_method": "POST",
        "submit_auth": "xsecure",
        "submit_body": {                      
                "contract": {
                    "startDate": "{{date_iso}}",
                    "endDate": "{{date_iso}}"
                }
            } 
        
    },
    
    "movimentacao_fundo_d0": {
        "submit_path": "/reports/RTA/ConsultTrade",
        "submit_method": "POST",
        "submit_auth": "xsecure",
        "submit_body": {
                    "contract": {
                        "startDate": "{{date_iso}}",
                        "endDate": "{{date_iso}}",
                        "consultType": "{{consult_type}}",
                        "status": "{{status}}"
                    }
                
        }
    },
    
    "carteira": {
        "submit_path": "/reports/Portfolio",
        "submit_method": "POST",
        "submit_auth": "xsecure",
        "submit_body": {
            "contract": {
                "startDate": "{{date_iso}}",
                "endDate": "{{date_iso}}",
                "typeReport": "{{report_type}}",
                "fundName": "{{fund_name}}"
            }
        },
        "parameters": {
            "report_types": [1, 2, 3, 4, 5],
            "fund_names": ["RIZA STATHEROS FIC FIM CP", "BTG ABSOLUTO FIC FIM"]
        }
    },
    
    "taxa_performance": {
        "submit_path": "/reports/RTA/PerformanceFee",
        "submit_method": "POST",
        "submit_body": {
            "contract": {
                "queryDate": "{{date_iso}}",
                "fundName": "{{fund_name}}"
            }
        }
    }
}