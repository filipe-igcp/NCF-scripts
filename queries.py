import pandas as pd
from datetime import datetime, timedelta
import sqlite3

def fetch_data(queries, db_paths):
    
    data = {label: [] for label in queries}  

    for db_path in db_paths:
        try:
            with sqlite3.connect(db_path) as con:
                
                cursor = con.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                existing_tables = {t[0] for t in cursor.fetchall()}  

                for label, query in queries.items():
                    try:
                      
                        table_name = query.split("FROM")[1].split()[0].strip()

                        
                        if table_name not in existing_tables:
                            continue

                       
                        df = pd.read_sql_query(query, con)
                        data[label].append(df)  
                    
                    except:
                        continue  

        except:
            continue  

    return data

queries = {

    "query_3month": "SELECT * FROM swap_3month",
    "query_ois":"SELECT * FROM ois",
    "query_estr_fixings":"SELECT * FROM ois WHERE Tenor = 'O/N'",
    "query_euribor_3m_fixings":"SELECT * FROM swap_3month WHERE Tenor = '3M'",
    "query_historical_basis_spread_15y_swap": "SELECT * FROM historical_basis_spread_15y_swap",
    "query_historical_zero_rates_3m":"SELECT * FROM zero_rates_3m",
    "query_historical_zero_rates_ois":"SELECT * FROM zero_rates_ois"
    }
