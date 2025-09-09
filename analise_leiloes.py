"""
Created on Tue Mar 25 10:25:29 2025

@author: ftome
"""
#Libraries

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
from scipy.stats import norm, skew, kurtosis
import math
import warnings
warnings.simplefilter(action='ignore', category=Warning)
import matplotlib.pyplot as plt
today_date= today_date = datetime.today().strftime('%Y%m%d')

QUERIES = {

    "spread_variations": "SELECT * FROM spread_variations",

"portugal": f"""
    SELECT TICKER, SECURITY_NAME, MATURITY_ANOMESDIA 
    FROM AAA_instruments 
    WHERE MATURITY_ANOMESDIA >= '{today_date}' 
    AND COUNTRY = 'PO' 
    AND (CPN_TYP = 'FIXED' OR CPN_TYP = 'ZERO COUPON') 
    ORDER BY MATURITY_ANOMESDIA ASC;
""",

"spain": f"""
    SELECT TICKER, SECURITY_NAME, MATURITY_ANOMESDIA 
    FROM AAA_instruments
    WHERE MATURITY_ANOMESDIA >= '{today_date}'
    AND COUNTRY = 'SP' 
    AND (CPN_TYP = 'FIXED' OR CPN_TYP = 'ZERO COUPON')
    ORDER BY MATURITY_ANOMESDIA ASC;
""",

"italy": f"""
    SELECT TICKER, SECURITY_NAME, MATURITY_ANOMESDIA 
    FROM AAA_instruments
    WHERE MATURITY_ANOMESDIA >= '{today_date}'
    AND COUNTRY = 'IT' 
    AND (CPN_TYP = 'FIXED' OR CPN_TYP = 'ZERO COUPON')
    ORDER BY MATURITY_ANOMESDIA ASC;
""",

"germany": f"""
    SELECT TICKER, SECURITY_NAME, MATURITY_ANOMESDIA 
    FROM AAA_instruments
    WHERE MATURITY_ANOMESDIA >= '{today_date}'
    AND COUNTRY = 'GE' 
    AND (CPN_TYP = 'FIXED' OR CPN_TYP = 'ZERO COUPON')
    ORDER BY MATURITY_ANOMESDIA ASC;
"""
}
db_paths = [r"\\igcp-fs\NCF-GER\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\BLOOMBERG_DATABASE\V1.0\db_bond_prices_v2.db", 
            r"\\igcp-fs\NCF-GER\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\DASHBOARD_DIARIO\DB\leiloes.db"]

#Main Functions

# -*- coding: utf-8 -*-
"""
Created on Tue Mar 25 10:12:22 2025

@author: ftome
"""
#Libraries

def get_business_day(date, offset):
    current_date = date
    step = 1 if offset > 0 else -1
    
    while offset != 0:
        current_date += timedelta(days=step)
        if current_date.weekday() < 5:  # Monday to Friday are business days
            offset -= step
    
    
    return current_date.replace(hour=date.hour, minute=date.minute, second=date.second)

def portugal_instruments(df, instrument, security, maturity, prints = False):
    
    if instrument == 'BT' :
    
        
        if security == 'principal':
        
            matching_security = df.loc[df['MATURITY_ANOMESDIA'] == maturity]
            
            if matching_security.empty:
                
                if prints:
                    
                    print(f"No securities with MATURITY_ANOMESDIA = {maturity} found.")
                
                else:
                    pass
            else:
                pass
            
            if prints:
                print(f"Selected Portugal security {security}: {matching_security.SECURITY_NAME} ")
            else:
                pass
    
        elif security == 'esq':
    
            matching_columns = df[df['MATURITY_ANOMESDIA'] < maturity]
            
            if matching_columns.empty:
                
                if prints:
                    
                    print(f"No securities with MATURITY_ANOMESDIA < {maturity} found.")
            
            else:
                
                first_security = matching_columns.iloc[-1]
            
                
                if all(matching_columns['SECURITY_NAME'].str.startswith('PORTB')):
                    matching_security = matching_columns.iloc[-1]
                else:
                    
                    matching_security = matching_columns.iloc[-1]
                    
                    while not matching_security['SECURITY_NAME'].startswith('PORTB'):
                        
                        matching_columns = matching_columns.iloc[:-1]
            
                        if matching_columns.empty:
                            
                            if prints:
                                print("No matching 'PORTB' security found. Selecting the first row.")
                            else:
                                pass
                            matching_security = first_security  
                            break
                        
                        matching_security = matching_columns.iloc[-1]
            
            if prints:   
                print(f"Selected Portugal security {security}: {matching_security.SECURITY_NAME}")
        
        else:
            
            matching_columns = df[df['MATURITY_ANOMESDIA'] > maturity]
            if matching_columns.empty:
                if prints:
                    print(f"No securities with MATURITY_ANOMESDIA < {maturity} found.")
                else:
                    return None
            else:
                
                first_security = matching_columns.iloc[0]
            
                
                if all(matching_columns['SECURITY_NAME'].str.startswith('PORTB')):
                    matching_security = matching_columns.iloc[0]
                else:
                    
                    matching_security = matching_columns.iloc[0]
                    while not matching_security['SECURITY_NAME'].startswith('PORTB'):
                        matching_columns = matching_columns.iloc[1:]
            
                        if matching_columns.empty:
                            if prints:
                                print("No matching 'PORTB' security found. Selecting the first row.")
                            else:
                                pass
                            matching_security = first_security  
                            break
                        
                        matching_security = matching_columns.iloc[0]
            if prints:
            
                print(f"Selected Portugal security {security}: {matching_security.SECURITY_NAME}")
            else:
                pass
                    
    else:
        
        if security == 'principal':
        
            matching_security = df.loc[df['MATURITY_ANOMESDIA'] == maturity]
            
            if matching_security.empty:
                if prints:
                    print(f"No securities with MATURITY_ANOMESDIA = {maturity} found.")
                else:
                    pass
                return None
            else:
                pass
            if prints:
                print(f"Selected Portugal security {security}: {matching_security.SECURITY_NAME}")
            else:
                pass
            
        elif security == 'esq':
    
            matching_columns = df[df['MATURITY_ANOMESDIA'] < maturity]
            if matching_columns.empty:
                if prints:
                    print(f"No securities with MATURITY_ANOMESDIA < {maturity} found.")
                else:
                    pass
                return None
            else:
                
                first_security = matching_columns.iloc[-1]
            
                
                if all(matching_columns['SECURITY_NAME'].str.startswith('PGB')):
                    matching_security = matching_columns.iloc[-1]
                else:
                    
                    matching_security = matching_columns.iloc[-1]
                    while not matching_security['SECURITY_NAME'].startswith('PGB'):
                        matching_columns = matching_columns.iloc[:-1]
            
                        if matching_columns.empty:
                            if prints:
                                print("No matching 'PGB' security found. Selecting the first row.")
                            else:
                                pass
                            matching_security = first_security  
                            break
                        
                        matching_security = matching_columns.iloc[-1]
            
                if prints:
                    print(f"Selected Portugal security {security}: {matching_security.SECURITY_NAME}")
                else:
                    pass
        else:
            
            matching_columns = df[df['MATURITY_ANOMESDIA'] > maturity]
            if matching_columns.empty:
                if prints:
                    print(f"No securities with MATURITY_ANOMESDIA < {maturity} found.")
                else:
                    pass
                return None
            else:
                
                first_security = matching_columns.iloc[0]
            
                
                if all(matching_columns['SECURITY_NAME'].str.startswith('PGB')):
                    matching_security = matching_columns.iloc[0]
                else:
                    
                    matching_security = matching_columns.iloc[0]
                    while not matching_security['SECURITY_NAME'].startswith('PGB'):
                        matching_columns = matching_columns.iloc[1:]
            
                        if matching_columns.empty:
                            if prints:
                                print("No matching 'PORTB' security found. Selecting the first row.")
                            else:
                                pass
                            matching_security = first_security  
                            break
                        
                        matching_security = matching_columns.iloc[0]
            if prints:
                print(f"Selected Portugal security {security}: {matching_security.SECURITY_NAME}")
            else:
                pass
        
    return matching_security

def closest_date(df, df_portugal, instrument, security, maturity, country, prints = False):
    
    portugal_principal = portugal_instruments(df_portugal, instrument = instrument, security = security, maturity = maturity)
    
    if portugal_principal is None or portugal_principal.empty:
        return None
    else:
    
        close_date=[]
    
        for dates in df['MATURITY_ANOMESDIA']:

            if security == 'principal':

                diff = (pd.to_datetime(portugal_principal.MATURITY_ANOMESDIA.iloc[0]) - pd.to_datetime(dates)).days

            else:
                diff = (pd.to_datetime(portugal_principal.MATURITY_ANOMESDIA) - pd.to_datetime(dates)).days

            close_date.append(diff)


        index = np.argmin(np.abs(close_date))

        if prints:

            print(f"closest {country} security chosen is {df['SECURITY_NAME'].iloc[index]} with index {index} ")
        else:
            pass

        return df['MATURITY_ANOMESDIA'].iloc[index], df['SECURITY_NAME'].iloc[index], df['TICKER'].iloc[index], index 

def pick_spain_italy_instruments(df,df_portugal, instrument, security, maturity, country, prints = False):
    
    df_ = df.copy()
    
    matching_instrument = None
    
    close_date = closest_date(df_,df_portugal, instrument=instrument, security=security, maturity=maturity, country = country, prints = prints)
    
    if security == 'principal':
        
        portugal = portugal_instruments(df_portugal, instrument = instrument, security = security, maturity = maturity, prints = prints).SECURITY_NAME.iloc[0]
    
    else:
        portugal = portugal_instruments(df_portugal, instrument = instrument, security = security, maturity = maturity, prints = prints)
        if portugal is None or portugal.empty:

            return None
        else:
            portugal = portugal_instruments(df_portugal, instrument = instrument, security = security, maturity = maturity, prints = prints).SECURITY_NAME
    
    if portugal.startswith('PGB'):
        
        if (close_date[1].strip().startswith('BTPS') or close_date[1].strip().startswith('SPGB') or close_date[1].strip().startswith('DBR') or close_date[1].strip().startswith('OBL')):
            
            matching_instrument = close_date

            if prints:
                
                print(f"Selected {country} security {security}: {matching_instrument[1]} with maturity {matching_instrument[0]}")
            
            else:
                pass
            
        else:
            
            first_security = close_date
            
            while not (close_date[1].strip().startswith('BTPS') or close_date[1].strip().startswith('SPGB') or close_date[1].strip().startswith('DBR') or close_date[1].strip().startswith('OBL')):
                
                
                df_ = df_.drop(closest_date(df_,df_portugal, instrument=instrument, security=security, maturity=maturity, country = country)[3])
                
                
                df_ = df_.reset_index(drop = True)
                
                close_date = closest_date(df_,df_portugal, instrument=instrument, security=security, maturity=maturity,  country = country)
                
                    
                if close_date==():
                    print("No matching 'PORTB' security found. Selecting the first row.")
                    matching_instrument = first_security  
                    break
                else:
                    pass
                    
                matching_instrument = close_date
            if prints:
                print(f"Selected {country} security {security}:{matching_instrument[1]} with maturity {matching_instrument[0]}")
            else:
                pass
    else:
        
        #if (close_date[1][0:4] == 'BOTS' or close_date[1][0:4] == 'SGLT' or close_date[1][0:4] == 'BKO' or close_date[1].startswith('BUBILL')):
        if (close_date[1].strip().startswith('BOTS') or close_date[1].strip().startswith('SGLT') or close_date[1].strip().startswith('BKO') or close_date[1].strip().startswith('BUBILL')):    
            matching_instrument = close_date
            
            
            
        else:
            first_security = close_date
            
            while not (close_date[1].strip().startswith('BOTS') or close_date[1].strip().startswith('SGLT') or close_date[1].strip().startswith('BKO') or close_date[1].strip().startswith('BUBILL')):
                
                if prints:
                    print(df_.head(20))
                else:
                    pass
                
                
                df_ = df_.drop(closest_date(df_,df_portugal, instrument=instrument, security=security, maturity=maturity, country = country)[3])
                
                df_.reset_index(drop=True)
                
                close_date = closest_date(df_,df_portugal, instrument=instrument, security=security, maturity=maturity, country = country)
                
                if close_date == ():
                    
                    if country == 'IT':
                        print("No matching 'BOTS' security found. Selecting the first row.")
                    elif country == 'GE':
                        print("No matching 'BKO' or 'BUBILL' security found. Selecting the first row.")
                    else :
                        print("No matching 'SGLT' security found. Selecting the first row.")
                    
                    matching_instrument = first_security  
                
                    break
                
                else:
                    pass
                
                matching_instrument = close_date
                
            if prints:
                print(f"Selected {country} security {security}:{matching_instrument[1]} with maturity {matching_instrument[0]} ")
            else:
                pass
                
            if prints:
                print(f"Selected {country} security {security}:{matching_instrument[1]} with maturity {matching_instrument[0]} ")
            else:
                pass
    
    return matching_instrument

def tickers_list(df_it, df_sp, df_pt, df_ge, instrument, security, maturity):
    italy = pick_spain_italy_instruments(df_it, df_pt, instrument=instrument, security=security, maturity=maturity, country='IT', prints=False)
    spain = pick_spain_italy_instruments(df_sp, df_pt, instrument=instrument, security=security, maturity=maturity, country='SP', prints=False)
    germany = pick_spain_italy_instruments(df_ge, df_pt, instrument=instrument, security=security, maturity=maturity, country='GE', prints=False)
    portugal = portugal_instruments(df_pt, instrument=instrument, security=security, maturity=maturity)

    if portugal is None:
        if security == 'principal':
            country_dict = {
                "PT_principal": portugal,
                "SP_principal": spain,
                "IT_principal": italy,
                "GE_principal": germany
            }
        elif security == 'esq':
            country_dict = {
                "PT_esq": portugal,
                "SP_esq": spain,
                "IT_esq": italy,
                "GE_esq": germany
            }
        else:
            country_dict = {
                "PT_dir": portugal,
                "SP_dir": spain,
                "IT_dir": italy,
                "GE_dir": germany
            }
        return country_dict

    else:
        if security == 'principal':
            country_dict = {
                "PT_principal": portugal.TICKER.iloc[0],
                "SP_principal": spain[2],
                "IT_principal": italy[2],
                "GE_principal": germany[2]
            }
        elif security == 'esq':
            country_dict = {
                "PT_esq": portugal.TICKER,
                "SP_esq": spain[2],
                "IT_esq": italy[2],
                "GE_esq": germany[2]
            }
        else:
            country_dict = {
                "PT_dir": portugal.TICKER,
                "SP_dir": spain[2],
                "IT_dir": italy[2],
                "GE_dir": germany[2]
            }
        return country_dict


def all_tickers(df_it, df_sp, df_pt, df_ge, instrument='OT', maturity='20251015'):
    
    principal = tickers_list(df_it, df_sp, df_pt, df_ge, instrument=instrument, security='principal', maturity=maturity )
    
    esquerda = tickers_list(df_it, df_sp, df_pt, df_ge, instrument=instrument, security='esq', maturity=maturity )
    
    direita = tickers_list(df_it, df_sp, df_pt, df_ge, instrument=instrument, security='dir', maturity=maturity )
    
    principal.update(esquerda)
    
    principal.update(direita)
    
    return principal 

def calculate_auction_dates(df_italy, df_spain, df_portugal,df_germany,auction_date,announcement_date, spread_fixing, books_closing, pricing, auction_type, maturity):
    
    tickers_list = all_tickers(df_italy, df_spain, df_portugal,df_germany, instrument=auction_type, maturity = maturity)
     
    if isinstance(auction_date, str):
        auction_date = datetime.strptime(auction_date, "%Y-%m-%d %H:%M:%S")
    if isinstance(announcement_date,str):
        announcement_date = datetime.strptime(announcement_date, "%Y-%m-%d %H:%M:%S")
    
    
    two_days_before_announcement =  get_business_day(announcement_date, -2)
    three_days_after_auction = get_business_day(auction_date, 3)
    
    results = {"auction_date":auction_date.strftime("%Y-%m-%d %H:%M:%S"),
                "announcement_date": announcement_date.strftime("%Y-%m-%d %H:%M:%S"),
                "two_days_before_announcement":two_days_before_announcement.strftime("%Y-%m-%d %H:%M:%S"),
                "three_days_after_auction":three_days_after_auction.strftime("%Y-%m-%d %H:%M:%S"),
                "Pricing":pricing,
                "Spread_Fixing": spread_fixing,
                "Books_Closing": books_closing
                  }
    results.update(tickers_list)
    return results 

def fetch_data(queries, db_paths, today_date):
    
    data = {label: [] for label in queries}  # Initialize dictionary with empty lists

    for db_path in db_paths:
        try:
            with sqlite3.connect(db_path) as con:
                # Fetch existing tables
                cursor = con.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                existing_tables = {t[0] for t in cursor.fetchall()}  # Convert to a set

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


def data_transformation(auction_name, tickers, calculation_dates, db_paths,check_dates=False):

    dfs = {}

    
    reference_data_hora = None
    con = sqlite3.connect(db_paths[0])
    for bond_name, ticker in tickers.items():
        try:
            if not ticker:
                raise ValueError("Ticker is None or empty.")

            query = f"""
                SELECT 
                    DATA_HORA, 
                    LAST, 
                    LAST_YIELD 
                FROM 
                    {ticker} 
                WHERE 
                    DATA_HORA >= '{calculation_dates["two_days_before_announcement"]}' 
                    AND DATA_HORA <= '{calculation_dates["three_days_after_auction"]}' 
                ORDER BY 
                    DATA_HORA ASC
            """
            
            df = pd.read_sql_query(query, con)
            df = df.drop_duplicates()
            
            dfs[bond_name] = df
            
            
            if "Emissão" not in auction_name:
                if reference_data_hora is None and not df.empty:
                
                    reference_data_hora = df["DATA_HORA"]
            else:
                if reference_data_hora is None and calculation_dates["two_days_before_announcement"] in df["DATA_HORA"].values:
                    reference_data_hora = df["DATA_HORA"]
            
        except Exception as e:
            print(f"Skipping bond '{bond_name}' due to error: {e}")
            
            dfs[bond_name] = pd.DataFrame(columns=["DATA_HORA", "LAST_YIELD"])
    con.close()
    if reference_data_hora is None:
        raise ValueError("No valid data to build reference DATA_HORA.")

    df_ = pd.DataFrame({"DATA_HORA": reference_data_hora})

    for bond_name, df_temp in dfs.items():
        
        df_temp = df_temp[["DATA_HORA", "LAST_YIELD"]].copy()
        df_ = df_.merge(df_temp, on="DATA_HORA", how="left", suffixes=("", f"_{bond_name}"))
        df_.rename(columns={"LAST_YIELD": bond_name}, inplace=True)

        if df_[bond_name].isna().all():
            print(f"Column for bond {bond_name} is fully NaN — likely missing or failed query.")
    
    if check_dates:
        
        return df_
    else:
        
        if ("Sindicato" in auction_name or "Emissão" in auction_name) and df_["PT_principal"].isna().any():
            
            df_["PT_principal"].ffill(inplace=True)
            for col in df_.columns:
                if col not in ["DATA_HORA", "PT_principal"] and df_[col].isna().any():
                    df_[col].bfill(inplace=True)
                    df_[col].ffill(inplace=True)
            
        else:
            df_.bfill(inplace=True)
            df_.ffill(inplace=True)
            

        return df_
    
def spreads(db, auction_name):
    db_ = db.copy()
    db_["Spread_SP_pri"] = db_["PT_principal"] - db_["SP_principal"]
    db_["Spread_SP_esq"] = db_["PT_esq"] - db_["SP_esq"]
    db_["Spread_SP_dir"] = db_["PT_dir"] - db_["SP_dir"]
    db_["Spread_IT_pri"] = db_["PT_principal"] - db_["IT_principal"]
    db_["Spread_IT_esq"] = db_["PT_esq"] - db_["IT_esq"]
    db_["Spread_IT_dir"] = db_["PT_dir"] - db_["IT_dir"]
    db_["Spread_GE_pri"] = db_["PT_principal"] - db_["GE_principal"]
    db_["Spread_GE_esq"] = db_["PT_esq"] - db_["GE_esq"]
    db_["Spread_GE_dir"] = db_["PT_dir"] - db_["GE_dir"]
    db_["Var Spread_SP"] = db_["Spread_SP_pri"] - 0.5*(db_["Spread_SP_esq"] + db_["Spread_SP_dir"])
    db_["Var Spread_IT"] = db_["Spread_IT_pri"] - 0.5*(db_["Spread_IT_esq"] + db_["Spread_IT_dir"])
    db_["Var Spread_GE"] = db_["Spread_GE_pri"] - 0.5*(db_["Spread_GE_esq"] + db_["Spread_GE_dir"])
    db_["Med_Spread_SP"] = (db_["Spread_SP_dir"] + db_["Spread_SP_esq"])/2
    db_["Med_Spread_IT"] = (db_["Spread_IT_dir"] + db_["Spread_IT_esq"])/2
    
    db_["DATA_HORA"] = pd.to_datetime(db_["DATA_HORA"])
    db_["Auction"] = auction_name
    
    return db_

def prepare_spread_data(db,spread,auction_name):
    """Fetch and transform spread data into a melted DataFrame."""
    
    df_melted = spread.melt(
        id_vars=["Auction", "DATA_HORA"],
        value_vars=["Spread_SP_pri", "Spread_SP_esq", "Spread_SP_dir", "Spread_IT_pri", "Spread_IT_esq", "Spread_IT_dir","Spread_GE_pri", "Spread_GE_esq", "Spread_GE_dir"],
        var_name="Yield_Type", value_name="Yield_Spread"
    )
    df_melted['Unique_ID'] = df_melted['DATA_HORA'].factorize()[0]
    return df_melted

def assign_event_labels(df_melted, calculation_dates):
    """Assign event labels based on event dates."""
    date_labels = {
        "Two_Days_Before_Announcement": calculation_dates["two_days_before_announcement"],
        "Announcement": calculation_dates["announcement_date"],
        "Auction_Date": calculation_dates["auction_date"],
        "Three_Days_After_Auction": calculation_dates["three_days_after_auction"],
        "Pricing":calculation_dates["Pricing"],
        "Spread_Fixing": calculation_dates["Spread_Fixing"],
        "Books_Closing": calculation_dates["Books_Closing"]
    }
    for event, date in date_labels.items():
        df_melted[event] = None 
        df_melted.loc[df_melted['DATA_HORA'] == date, event] = df_melted.loc[df_melted['DATA_HORA'] == date, 'Unique_ID']
    
    return df_melted

def map_bond_types(df_melted, df_portugal, tickers, calculation_dates):
    
    def safe_lookup(ticker):
        if not ticker:
            return None
        result = df_portugal.loc[df_portugal["TICKER"] == ticker, "SECURITY_NAME"]
        if not result.empty:
            return result.iloc[0]
        return None

    bond_mapping = {
        "pri": safe_lookup(tickers.get("PT_principal")),
        "esq": safe_lookup(tickers.get("PT_esq")),
        "dir": safe_lookup(tickers.get("PT_dir")),
    }

    df_melted["T_Bond_Bill"] = df_melted["Yield_Type"].apply(
        lambda x: bond_mapping.get(x.split('_')[-1], None)
    )

    df_melted["Data_Leilao"] = calculation_dates["auction_date"]

    return df_melted


def process_spread_data(db, spread, auction_name, calculation_dates, df_portugal, tickers):
    """Main function to process spread data."""
    df_melted = prepare_spread_data(db, spread, auction_name)
    df_melted = assign_event_labels(df_melted, calculation_dates)
    df_melted = map_bond_types(df_melted, df_portugal, tickers, calculation_dates)
    return df_melted

def process_yield_and_spread_by_date(db, spread, auction_name, calculation_dates):
    
    date_labels = {
        "Two_Days_Before_Announcement": calculation_dates["two_days_before_announcement"],
        "Announcement": calculation_dates["announcement_date"],
        "Auction_Date": calculation_dates["auction_date"],
        "Three_Days_After_Auction": calculation_dates["three_days_after_auction"],
        "Pricing":calculation_dates["Pricing"],
        "Spread_Fixing": calculation_dates["Spread_Fixing"],
        "Books_Closing": calculation_dates["Books_Closing"]
    }
    
    db_yields = pd.concat([spread.loc[spread["DATA_HORA"].isin([date])].assign(Label=label) for label, date in date_labels.items()])
    db_yields["Auction"] = auction_name
    db_yields=db_yields.melt(id_vars=["Auction", "DATA_HORA", "Label"], value_vars=db_yields.columns[1:], var_name="Yield_Type", value_name="Yield")
    db_yields[~db_yields["Yield_Type"].isin(["Med_Spread_SP", "Med_Spread_IT"])]
    db_yields["DATA_HORA"] = date_labels["Auction_Date"]
    return db_yields

def process_variations(db,spread,auction_name,calculation_dates):
    if "Sindicato" in auction_name:
        date_labels = {
            "Two Days Before Annoucement": calculation_dates["two_days_before_announcement"],
            "Announcement Date": calculation_dates["announcement_date"],
            "Pricing": calculation_dates["Pricing"],
            "Three Days After Auction": calculation_dates["three_days_after_auction"]
        }
        
        var = pd.concat([
            spread.loc[db["DATA_HORA"].isin([date])].assign(Label=label)
            for label, date in date_labels.items()
        ])
        var = var.drop_duplicates()
        spread_columns = var.columns[1:-2]
        var[spread_columns] = var[spread_columns].diff()
        var = var.dropna(how="all", subset=spread_columns)
        df_melted_var = var.melt(
            id_vars=["Auction", "DATA_HORA", "Label"],
            value_vars=spread_columns,
            var_name="Yield_Type", value_name="Yield_Variation"
        )        
        
        replace_dict = {
            "Announcement Date": "Two days bef - Announcement",
            "Pricing": "Announcement - Pricing",
            "Three Days After Auction": "Pricing - Three Days After"
        }
        df_melted_var['Label'] = df_melted_var['Label'].replace(replace_dict)
        df_melted_var['DATA_HORA'] = date_labels["Pricing"]
    
    else:    
        date_labels = {
            "Two Days Before Annoucement": calculation_dates["two_days_before_announcement"],
            "Announcement Date": calculation_dates["announcement_date"],
             "Auction Date": calculation_dates["auction_date"],
            "Three Days After Auction": calculation_dates["three_days_after_auction"]
        }
        
        var = pd.concat([
            spread.loc[db["DATA_HORA"].isin([date])].assign(Label=label)
            for label, date in date_labels.items()
        ])
        var = var.drop_duplicates()
        spread_columns = var.columns[1:-2]
        var[spread_columns] = var[spread_columns].diff()
        var = var.dropna(how="all", subset=spread_columns)
        df_melted_var = var.melt(
            id_vars=["Auction", "DATA_HORA", "Label"],
            value_vars=spread_columns,
            var_name="Yield_Type", value_name="Yield_Variation"
        )
        
        replace_dict = {
            "Announcement Date": "Two days bef - Announcement",
            "Auction Date": "Announcement - Auction Date",
            "Three Days After Auction": "Auction Date - Three Days After"
        }
        df_melted_var['Label'] = df_melted_var['Label'].replace(replace_dict)
        df_melted_var['DATA_HORA'] = date_labels["Auction Date"]
        
    return df_melted_var

def create_bond_mapping(df, tickers, country):
    """Creates a mapping of yield types to security names for a given country."""
    bond_map = {}
    for suffix in ["principal", "esq", "dir"]:
        key = f"{country}_{suffix}"
        ticker = tickers.get(key)
        
        if ticker is None:
            bond_map[key] = None
            continue

        match = df.loc[df["TICKER"] == ticker, "SECURITY_NAME"]
        bond_map[key] = match.iloc[0] if not match.empty else None

    return bond_map

def generate_bond_dataframe(tickers, auction_name, df_portugal, df_spain, df_italy, df_germany, calculation_dates):
    """Generates a DataFrame mapping yield types to security names."""
    dfs = {"PT": df_portugal, "SP": df_spain, "IT": df_italy, "GE": df_germany}
    bond_mapping = {k: v for country, df in dfs.items() for k, v in create_bond_mapping(df, tickers, country).items()}
    df_tickers = pd.DataFrame(tickers.items(), columns=["Yield_Type", "ISIN"])
    df_tickers["Instrument"] = df_tickers["Yield_Type"].map(bond_mapping.get)
    df_tickers["Auction"] = auction_name
    df_tickers["DATA_HORA"] = calculation_dates["auction_date"]
    df_tickers["Two_Days_Before_Announcement"] = calculation_dates["two_days_before_announcement"]
    df_tickers["Announcement"] = calculation_dates["announcement_date"]
    df_tickers["Three_Days_After_Auction"] = calculation_dates["three_days_after_auction"]
    df_tickers["Pricing"] = calculation_dates["Pricing"]
    df_tickers["Spread_Fixing"] = calculation_dates["Spread_Fixing"]
    df_tickers["Books_Closing"] = calculation_dates["Books_Closing"]
    
    
    return df_tickers

def calculate_histogram(df, auction_name, current_date,window = 100, num_bins=17,rolling_mean = True):
    """
    Computes a histogram and a scaled normal distribution for the 'Yield_Variation' column.
    Uses historical data (df) to create arrays of bin centers, counts, and normal distribution values.
    The current_date (a constant for the event) is repeated for each bin.
    """
    # Convert Yield_Variation to numeric, dropping non-numeric entries
    df["Yield_Variation"] = pd.to_numeric(df["Yield_Variation"], errors='coerce')
    df = df.dropna(subset=["Yield_Variation"])
    if rolling_mean:
        df = df.drop_duplicates().iloc[-window:]
    else:
        df = df.drop_duplicates()
    mean = df["Yield_Variation"].mean()
    std = df["Yield_Variation"].std()
    
    counts, bin_edges = np.histogram(df["Yield_Variation"], bins=num_bins)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    norm_dist_scaled = norm.pdf(bin_centers, mean, std) * len(df) * (bin_edges[1] - bin_edges[0])
    
    # Create DataFrame using constant current_date for each row
    return pd.DataFrame({
        "Bin": bin_centers,
        "Frequency": counts,
        "NormDist": norm_dist_scaled,
        "Auction": auction_name,
        "DATA_HORA": [current_date] * len(bin_centers)
    })

def calculate_histogram_for_event(data_dict, event,calculation_dates,window = 100, num_bins=17,rolling_mean = True):
    """
    For a given auction event, filter the full dataset for historical data (rows with a DATA_HORA prior to the event)
    that match the event's Label and Yield_Type. Then, compute the histogram and attach the event's yield variation.
    """
    
    df = data_dict.get("spread_variations")[0]
    current_date = pd.to_datetime(event['DATA_HORA'])
    label = event['Label']
    yield_type = event['Yield_Type']
    auction = event['Auction']
    
    # Determine auction type (OT or BT) if needed
    if "OT" in auction:
        auction_type = "OT"
    elif "BT" in auction:
        auction_type = "BT"
    else:
        auction_type = "Unknown"
    
    # Use the full dataset for historical filtering
    historical_data = df[
        (pd.to_datetime(df["DATA_HORA"]) < current_date) &
        (df["Label"] == label) &
        (df["Yield_Type"] == yield_type) &
        (df["Auction"].str.contains(auction_type, na=False))  # Ensure only BT auctions for BT cases
        ]
    
    hist_df = calculate_histogram(historical_data, auction, current_date,window = window, num_bins=num_bins,rolling_mean = rolling_mean)
    # Append additional columns for clarity
    hist_df["Label"] = label
    hist_df["Yield_Type"] = yield_type
    hist_df["Auction_Type"] = auction_type
    # Add the event's Yield Variation as a constant column in each histogram row
    hist_df["Yield_Var"] = event["Yield_Variation"]
    hist_df['DATA_HORA'] = calculation_dates["auction_date"]
    hist_df = hist_df[["Bin","Frequency","NormDist", "Auction","DATA_HORA", "Label","Yield_Type","Auction_Type","Yield_Var"]]
    
    return hist_df

def process_histogram(data_dict, variations, calculation_dates, window = 100, num_bins=17,rolling_mean = True):
    """
    Process multiple events and compute their histograms, returning a concatenated DataFrame.
    """
    results = []
    filtered_variations = variations[variations["Yield_Type"].isin(["Var Spread_SP", "Var Spread_IT", "Var Spread_GE"])]
    for _, event in filtered_variations.iterrows():
        hist_df = calculate_histogram_for_event(data_dict, event, calculation_dates,window = window, num_bins=17,rolling_mean = True)
        results.append(hist_df)
    
    return pd.concat(results, ignore_index=True)

def calculate_stats_for_event(data_dict, event,calculation_dates, window=100,rolling_mean=True):
    """
    For a given auction event, filter the full dataset for historical data (rows with a DATA_HORA
    prior to the event) that match the event's Label and Yield_Type, then compute the historical
    average, standard deviation, skewness, kurtosis, and the percentile of the event's yield variation.
    Returns a dictionary with these statistics.
    """

    df = data_dict.get("spread_variations")[0]
    current_date = pd.to_datetime(event['DATA_HORA'])
    label = event['Label']
    yield_type = event['Yield_Type']
    auction = event['Auction']
    

    if "OT" in auction:
        auction_type = "OT"
    elif "BT" in auction:
        auction_type = "BT"
    else:
        auction_type = "Unknown"
    
   
    historical_data = df[
        (pd.to_datetime(df["DATA_HORA"]) < current_date) &
        (df["Label"] == label) &
        (df["Yield_Type"] == yield_type) &
        (df["Auction"].str.contains(auction_type, na=False))  
        ]
    
    
    historical_data["Yield_Variation"] = pd.to_numeric(historical_data["Yield_Variation"], errors='coerce')
    historical_data = historical_data.dropna(subset=["Yield_Variation"])
    
    
    
    if historical_data.empty:
        mean_val = np.nan
        std_val = np.nan
        skew_val = np.nan
        kurt_val = np.nan
        percentile_val = np.nan
    else:
        if rolling_mean:
            mean_val = historical_data["Yield_Variation"].drop_duplicates().iloc[-window:].mean()
            std_val = historical_data["Yield_Variation"].drop_duplicates().iloc[-window:].std()
            skew_val = skew(historical_data["Yield_Variation"].drop_duplicates().iloc[-window:])
            kurt_val = kurtosis(historical_data["Yield_Variation"].drop_duplicates().iloc[-window:])

            event_yield = pd.to_numeric(event["Yield_Variation"], errors='coerce')
            percentile_val = norm.cdf(event_yield, loc=mean_val, scale=std_val) * 100
        else:
            mean_val = historical_data["Yield_Variation"].drop_duplicates().mean()
            std_val = historical_data["Yield_Variation"].drop_duplicates().std()
            skew_val = skew(historical_data["Yield_Variation"].drop_duplicates())
            kurt_val = kurtosis(historical_data["Yield_Variation"].drop_duplicates())

            event_yield = pd.to_numeric(event["Yield_Variation"], errors='coerce')
            percentile_val = norm.cdf(event_yield, loc=mean_val, scale=std_val) * 100
        
    
    return {
        "Auction": auction,
        "DATA_HORA": calculation_dates["auction_date"],
        "Label": label,
        "Yield_Type": yield_type,
        "Auction_Type": auction_type,
        "Yield_Var": event["Yield_Variation"],
        "Historical_Mean": mean_val,
        "Historical_Std": std_val,
        "Skewness": skew_val,
        "Kurtosis": kurt_val,
        "Percentile": percentile_val
    }


def process_stats(data_dict, variations, calculation_dates,window=100,rolling_mean=True):
    """
    Process multiple events and compute their statistical properties, returning a DataFrame.
    """
    stats_list = []
    filtered_variations = variations[variations["Yield_Type"].isin(["Var Spread_SP", "Var Spread_IT", "Var Spread_GE"])]
    for _, event in filtered_variations.iterrows():
        stats = calculate_stats_for_event(data_dict, event, calculation_dates,window=window,rolling_mean=rolling_mean)
        stats_list.append(stats)
    
    return pd.DataFrame(stats_list)

def fetch_data(queries, db_paths, today_date):
    """
    Fetches data from multiple databases for all queries and organizes it by query name.
    
    Parameters:
        queries (dict): Dictionary where keys are labels and values are SQL queries.
        db_paths (list): List of database file paths.
        today_date (str): Current date.
    
    Returns:
        dict: A dictionary where keys are query labels (e.g., "users") and values are lists of DataFrames.
    """
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

def calculate_eixos(spread, country_code):
    
    max_pri = spread[f"Spread_{country_code}_pri"].max()
    min_pri = spread[f"Spread_{country_code}_pri"].min()
    max_esq = spread[f"Spread_{country_code}_esq"].max()
    min_esq = spread[f"Spread_{country_code}_esq"].min()
    max_dir = spread[f"Spread_{country_code}_dir"].max()
    min_dir = spread[f"Spread_{country_code}_dir"].min()
    
    
    amplitude_pri = np.abs(min_pri - max_pri)
    amplitude_esq = np.abs(min_esq - max_esq)
    amplitude_dir = np.abs(min_dir - max_dir)
    
    
    max_amplitude = max(amplitude_pri, amplitude_esq, amplitude_dir)
    rounded_max_amplitude = math.ceil((max_amplitude + 0.1) * 10) / 10
    
    
    eixo_max_pri = round(max_pri + np.abs(rounded_max_amplitude - amplitude_pri) / 2, 1)
    eixo_min_pri = round(min_pri - np.abs(rounded_max_amplitude - amplitude_pri) / 2, 1)
    eixo_max_esq = round(max_esq + np.abs(rounded_max_amplitude - amplitude_esq) / 2, 1)
    eixo_min_esq = round(min_esq - np.abs(rounded_max_amplitude - amplitude_esq) / 2, 1)
    eixo_max_dir = round(max_dir + np.abs(rounded_max_amplitude - amplitude_dir) / 2, 1)
    eixo_min_dir = round(min_dir - np.abs(rounded_max_amplitude - amplitude_dir) / 2, 1)
    
    return eixo_max_pri, eixo_min_pri, eixo_max_esq, eixo_min_esq, eixo_max_dir, eixo_min_dir

def process_data(spread, auction_name, calculation_dates):
    eixo_values_esp = calculate_eixos(spread, "SP")
    eixo_values_ita = calculate_eixos(spread, "IT")
    eixo_values_ger = calculate_eixos(spread, "GE")
    
    
    
    data = {
        'Leilao': [auction_name] * 9,  
        'Data_Leilao': [calculation_dates["auction_date"]] * 9,  
        'min': [eixo_values_esp[1], eixo_values_ita[1],eixo_values_ger[1], eixo_values_esp[3], eixo_values_ita[3],eixo_values_ger[3], eixo_values_esp[5], eixo_values_ita[5], eixo_values_ger[5]],
        'max': [eixo_values_esp[0], eixo_values_ita[0],eixo_values_ger[0], eixo_values_esp[2], eixo_values_ita[2],eixo_values_ger[2], eixo_values_esp[4], eixo_values_ita[4], eixo_values_ger[4]],
        'T_Bond': ['Principal', 'Principal','Principal','Esquerda', 'Esquerda','Esquerda','Direita', 'Direita','Direita'],  
        'Pais': ['Espanha', 'Italia','Alemanha', 'Espanha', 'Italia', "Alemanha", 'Espanha', 'Italia', "Alemanha"]
    }
    
    
    return pd.DataFrame(data)

def store_main_data(auction_name, spread_data,filtered_yields,variations,df_tickers, axis, histogram,statistics,db_path):
    """Stores computed statistics in the database."""
    with sqlite3.connect(db_path) as conn:
        #cursor = conn.cursor()
        spread_data.to_sql('yield_spreads_', conn, if_exists='append', index=False)
        print(f"Rows inserted in 'yield_spreads_': {len(spread_data)}")
        filtered_yields.to_sql('yields', conn, if_exists='append', index=False)
        print(f"Rows inserted in 'yields': {len(filtered_yields)}")
        variations.to_sql('spread_variations', conn, if_exists='append', index=False)
        print(f"Rows inserted in 'spread_variations': {len(variations)}")
        df_tickers.to_sql('info_leilao', conn, if_exists='append', index=False)
        print(f"Rows inserted in 'info_leilao': {len(df_tickers)}")
        axis.to_sql('axis', conn, if_exists='append', index=False)
        print(f"Rows inserted in 'axis': {len(axis)}")
        histogram.to_sql('histogramas', conn, if_exists='append', index=False)
        print(f"Rows inserted in 'histogramas': {len(histogram)}")
        statistics.to_sql('estatisticas', conn, if_exists='append', index=False)
        print(f"Rows inserted in 'estatisticas': {len(statistics)}")
            
        conn.commit()

def plot_histograma(auction_name, histogram,statistics,country = "SP"):
    if "Emissão" not in auction_name: 
        histogram = histogram.copy()
        statistics = statistics.copy()
        histogram = histogram.loc[(histogram["Label"]=="Two days bef - Announcement")&(histogram["Yield_Type"]==f"Var Spread_{country}")]
        statistics = statistics.loc[(statistics["Label"]=="Two days bef - Announcement")&(statistics["Yield_Type"]==f"Var Spread_{country}")]
        plt.bar(histogram['Bin'], histogram['Frequency'],color='orange', width=0.015, alpha=0.6, label='Histogram     (Frequency)',align='center')
        plt.plot(histogram['Bin'], histogram['NormDist'] , color='red', label='Normal Distribution')
        plt.xlabel('Bin')
        plt.ylabel('Frequency / Scaled Density')
        plt.axvline(x=statistics["Yield_Var"].iloc[0], color='purple', linestyle='--', linewidth=2, label='Yield_Var')
        plt.title(f'Histogram with Normal Distribution for {country}')
        plt.legend()
        plt.grid(False)
        plt.tight_layout()
        plt.show()
        print("Info:")
        print(statistics.iloc[0])
    else:
        histogram = histogram.copy()
        statistics = statistics.copy()
        histogram = histogram.loc[(histogram["Label"]=="Auction Date - Three Days After")&(histogram["Yield_Type"]==f"Var Spread_{country}")]
        statistics = statistics.loc[(statistics["Label"]=="Auction Date - Three Days After")&(statistics["Yield_Type"]==f"Var Spread_{country}")]
        plt.bar(histogram['Bin'], histogram['Frequency'],color='orange', width=0.015, alpha=0.6, label='Histogram     (Frequency)',align='center')
        plt.plot(histogram['Bin'], histogram['NormDist'] , color='red', label='Normal Distribution')
        plt.xlabel('Bin')
        plt.ylabel('Frequency / Scaled Density')
        plt.axvline(x=statistics["Yield_Var"].iloc[0], color='purple', linestyle='--', linewidth=2, label='Yield_Var')
        plt.title(f'Histogram with Normal Distribution for {country}')
        plt.legend()
        plt.grid(False)
        plt.tight_layout()
        plt.show()
        print("Info:")
        print(statistics.iloc[0])
        
        

def plot_data(df, spread_data, spread_str="SP-IT", instrumento="principal", tipo="spread"):
    country1 = spread_str[:2]
    country2 = spread_str[3:5]  
    fig, ax = plt.subplots(figsize=(12,6))
    
    
    if tipo == "spread":
        ax.plot(df.index, df[f"Spread_{country1}_{instrumento}"], label=f"{country1}_{instrumento}", color='blue', linewidth=1, alpha=1)
        ax2 = ax.twinx()
        ax2.plot(df.index, df[f"Spread_{country2}_{instrumento}"], label=f"{country2}_{instrumento}", color='red', linewidth=1, alpha=1)
    else:
        ax.plot(df.index, df[f"{country1}_{instrumento}"], label=f"{country1}_{instrumento}", color='blue', linewidth=1, alpha=1)
        ax2 = ax.twinx()
        ax2.plot(df.index, df[f"{country2}_{instrumento}"], label=f"{country2}_{instrumento}", color='red', linewidth=1, alpha=1)

    
    tdb = spread_data["Two_Days_Before_Announcement"].dropna().unique()[0]
    ann = spread_data["Announcement"].dropna().unique()[0]
    auc = spread_data["Auction_Date"].dropna().unique()[0]
    after = spread_data["Three_Days_After_Auction"].dropna().unique()[0]

    
    ax.axvline(tdb, color='green', linestyle='--', linewidth=2)
    ax.text(tdb, ax.get_ylim()[1], 'Two Days Before', color='black', rotation=360, verticalalignment='top')

    ax.axvline(ann, color='orange', linestyle='--', linewidth=2)
    ax.text(ann, ax.get_ylim()[1], 'Announcement', color='black', rotation=360, verticalalignment='top')

    ax.axvline(auc, color='purple', linestyle='--', linewidth=2)
    ax.text(auc, ax.get_ylim()[1], 'Auction Date', color='black', rotation=360, verticalalignment='top')

    ax.axvline(after, color='red', linestyle='--', linewidth=2)
    ax.text(after, ax.get_ylim()[1], 'Three Days After', color='black', rotation=360, verticalalignment='top')

    
    ax.set_xlabel("DATA")
    ax.set_ylabel(f"{tipo}_{country1}_{instrumento}", color='blue')
    ax2.set_ylabel(f"{tipo}_{country2}_{instrumento}", color='red')

    
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines + lines2, labels + labels2, loc='upper left')

    plt.title("Spread")
    plt.grid(False)
    plt.tight_layout()
    plt.show()

def Missing_data(auction_name,tickers,calculation_dates,db_paths):
    db = data_transformation(auction_name,tickers,calculation_dates,db_paths,check_dates=True)
    country_id = []
    missing_dates = []
    for i in db.columns[1:]:
        mask = db[["DATA_HORA", f"{i}"]].isna().any(axis=1)
        if db.loc[mask, f"{i}"].empty:
            pass
        else:
            missing_dates.append(db.loc[mask]["DATA_HORA"].astype(str).str[:10].unique())
            country_id.append(i)
    df_missing = pd.DataFrame({
        "Country_ID": country_id,
        "Missing_Dates": missing_dates
    })
    if df_missing.empty:
        print("No missing data")
        return None  
    else:
        return df_missing