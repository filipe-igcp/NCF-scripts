import pandas as pd
trade_params_df = pd.DataFrame([
    {
        "counterparty": "MSLN FFT",
        "notional": 250_000_000,
        "settlement_date": "2025-06-27",
        "maturity_date": "2040-06-27",
        "basis_spread": 0.0009,
        "transaction_number": "163.626",
        "period":15,
        "curve_pay": "ESTR",
        "curve_receive": "Euribor 3M",
        "type":"basis swap"  
        
    },
    {
        "counterparty": "MSLN FFT",
        "notional": 250_000_000,
        "settlement_date": "2025-07-02",
        "maturity_date": "2040-07-02",
        "basis_spread": 0.000875,
        "transaction_number": "163.979",
        "period":15,
        "curve_pay": "ESTR",
        "curve_receive": "Euribor 3M",
        "type":"basis swap" 
    },
    {
        "counterparty": "MSLN FFT",
        "notional": 125_000_000,
        "settlement_date": "2025-07-07",
        "maturity_date": "2040-07-07",
        "basis_spread": 0.00085,
        "transaction_number": "164.338",
        "period":15,
       "curve_pay": "ESTR",
        "curve_receive": "Euribor 3M",
        "type":"basis swap" 
    },
    {
        "counterparty": "MSLN FFT",
        "notional": 125_000_000,
        "settlement_date": "2025-07-07",
        "maturity_date": "2040-07-07",
        "basis_spread": 0.00085,
        "transaction_number": "164.347",
        "period":15,
        "curve_pay": "ESTR",
        "curve_receive": "Euribor 3M",
        "type":"basis swap"  
    },
    {
        "counterparty": "MSLN FFT",
        "notional": 125_000_000,
        "settlement_date": "2025-07-24",
        "maturity_date": "2040-07-24",
        "basis_spread": 0.00094,
        "transaction_number": "165.805",
        "period":15,
        "curve_pay": "ESTR",
        "curve_receive": "Euribor 3M",
        "type":"basis swap" 
    },
    {
        "counterparty": "MSLN FFT",
        "notional": 125_000_000,
        "settlement_date": "2025-07-30",
        "maturity_date": "2040-07-30",
        "basis_spread": 0.00093,
        "transaction_number": "166.181",
        "period":15,
        "curve_pay": "ESTR",
        "curve_receive": "Euribor 3M",
        "type":"basis swap"
    },
    {
        "counterparty": "BNPA PAR",
        "notional": 125_000_000,
        "settlement_date": "2025-07-09",
        "maturity_date": "2040-07-09",
        "basis_spread": 0.000849,
        "transaction_number": "164.584",
        "period":15,
        "curve_pay": "ESTR",
        "curve_receive": "Euribor 3M",
        "type":"basis swap"  
    },
    {
        "counterparty": "CALY PAR",
        "notional": 125_000_000,
        "settlement_date": "2025-07-09",
        "maturity_date": "2040-07-09",
        "basis_spread": 0.00086,
        "transaction_number": "164.585",
        "period":15,
        "curve_pay": "ESTR",
        "curve_receive": "Euribor 3M",
        "type":"basis swap"  
    }
])