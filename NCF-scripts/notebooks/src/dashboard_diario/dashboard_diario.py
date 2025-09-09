import os
import pandas as pd
from datetime import datetime,timedelta
import sqlite3
import xlwings as xw
import shutil
import tempfile
import glob
import warnings
import numpy as np
from pypdf import PdfReader
import re
import mysql.connector

def repair_and_save_excel(file_path):
    app = None
    try:
        app = xw.App(visible=False)
        app.display_alerts = False
        
        wb = app.books.open(file_path, corrupt_load=xw.constants.CorruptLoad.xlRepairFile)
        print("File repaired successfully.")

        try:
            wb.save()
            
        except Exception as save_error:
            
            temp_dir = tempfile.gettempdir()
            temp_path = os.path.join(temp_dir, "RepairedFile.xlsx")
            wb.save(temp_path)
            shutil.copy(temp_path, file_path)
            
        finally:
            wb.close()

    except Exception as e:
        print(f"An error occurred while processing {file_path}: {e}")

    finally:
        if app:
            app.quit()


def repair_last_file(today, base_path, file_pattern_prefix):
    days_to_subtract = 1 if today.weekday() > 0 else 3
    yesterday = today - timedelta(days=days_to_subtract)
    date_str = yesterday.strftime("%Y%m%d")
    print(yesterday)

    file_pattern = f"{file_pattern_prefix}_{date_str}_*.xlsx"
    file_paths = glob.glob(os.path.join(base_path, file_pattern))

    if not file_paths:
        print(f"No files found matching pattern: {file_pattern}")
        return []

    for file_path in file_paths:
        print(f"Processing file: {file_path}")
        repair_and_save_excel(file_path)

    return file_paths


def process_files_for_patterns(today, excel_dir, patterns):
    all_excel_files = []

    for prefix in patterns:
        repaired_files = repair_last_file(today, excel_dir, prefix)
        print(f"Repaired files for pattern '{prefix}': {repaired_files}")

        if not repaired_files:
            continue

        last_file = max(repaired_files, key=os.path.getmtime)
        try:
            excel_file = pd.ExcelFile(last_file)
            all_excel_files.append(excel_file)
        except Exception as e:
            print(f"Failed to load {last_file}: {e}")

    return all_excel_files


#Indicadores de Risco

def refinancing(df):
    df = df.drop(df.index[-1])
    df=df.drop(columns = {"Row Id","(IGCP) AUX","Cashflow Type"})
    df=df.rename(columns = {"Risk Period":"Risk_Period","Instrument Type":"Instrument_Type",
                                       "(IGCP) Valor Refinanciar":"Valor_Refinanciar",
                                        "(IGCP) Total Refinanciar":"Total_Refinanciar",
                                        "(IGCP) Percentagem Refinanciar":"Percentagem_Refinanciar","(IGCP) Generic Type":"Generic_Type"})
    df["Data_Avaliacao"] = yesterday_
    return df

def refixing(df):
    df = df.drop(df.index[-1])
    df=df.drop(columns = {"Row Id","Cashflow Type","(IGCP) Aux2","(IGCP) Aggregate"})
    df=df.rename(columns = {"(IGCP) Risk Period":"Risk_Period","Instrument Type":"Instrument_Type",
                                       "(IGCP) Valor Refixar":"Valor_Refixar",
                                        "(IGCP) Total a Refixar":"Total_Refixar",
                                        "(IGCP) Percentagem a refixar":"Percentagem_Refixar","(IGCP) Generic Type":"Generic_Type"})
    df["Data_Avaliacao"] = yesterday_
    return df


def indicadores_risco(df_refinanciamento,df_refixing_weekly,df_limites):
    short_term_periods = ["0-3M", "3-6M", "6-9M", "9-12M"]
    _0_1Y = df_refinanciamento[df_refinanciamento["Risk Period"].isin(short_term_periods)]["(IGCP) Percentagem Refinanciar"].sum()
    _0_2Y = df_refinanciamento[df_refinanciamento["Risk Period"].isin(["1-2Y"])]["(IGCP) Percentagem Refinanciar"].sum() + _0_1Y
    _0_3Y= df_refinanciamento[df_refinanciamento["Risk Period"].isin(["2-3Y"])]["(IGCP) Percentagem Refinanciar"].sum() + _0_2Y
    _0_3m = df_refinanciamento[df_refinanciamento["Risk Period"].isin(["0-3M"])]["(IGCP) Percentagem Refinanciar"].sum()
    max_26W = df_refixing_weekly[df_refixing_weekly["(IGCP) Risk Period"] != "+26W"].groupby("(IGCP) Risk Period").sum()["(IGCP) Percentagem a refixar"].max()
    _1W = df_refixing_weekly[df_refixing_weekly["(IGCP) Risk Period"].isin(["1W"])]["(IGCP) Percentagem a refixar"].sum()
    _0_1Y_guideline_proposta = df_limites.iloc[-1]["IGCP Refinancing Risk 1Y"]
    _0_5Y_guideline_proposta= df_limites.iloc[-1]["IGCP Refinancing Risk 5Y"]
    average_maturity = df_limites.iloc[-1]["IGCP Average Life"]
    taxa_variável = df_limites.iloc[-1]["IGCP Float %"]
    df_limites_ = pd.DataFrame({
        "Indicador_Risco": ["Refinanciamento","Refinanciamento","Refinanciamento","Refinanciamento","Refinanciamento","Taxa de Juro", "Taxa de Juro", "Refixing","Refixing"],
        "Label": ["0-1Y", "0-2Y", "0-3Y", "0-5Y","0-3M (ICR)","Maturidade Média", "Taxa Variável", "1 semana (ICR)", "Máximo nas próximas 26 semanas (ICR)"],
        "Guidelines_Atuais": [_0_1Y,
                              _0_2Y, 
                              _0_3Y, 
                              "NA", 
                              _0_3m,
                              "NA", 
                              "NA",
                              _1W,
                              max_26W
                               ],
        "Limite_Guidelines_Atuais":["25%", "40%", "50%", "NA","10%","NA","NA","4%","4%"], 
        "Guidelines_Propostas": [_0_1Y_guideline_proposta, "NA", "NA",_0_5Y_guideline_proposta,"NA",average_maturity, taxa_variável, "NA", "NA"],
        "Limite_Guidelines_Propostas":["15%", "NA", "NA", "45%","NA",">=7","<=17,5%", "NA","NA"], #Mudar limites guidelines
        "Data_Avaliação":[yesterday_, yesterday_, yesterday_, yesterday_, yesterday_,yesterday_, yesterday_,yesterday_,yesterday_]
    })
    return df_limites_

def maturidade_guidelines_esdm(df_limites):
    df = pd.DataFrame({"Data_avaliacao":[yesterday_],
                            "Carteira":["Carteira ESDM (excluindo CEDIC/CEDIM)"],
                            "Tipo":["Maturity"],
                            "Valor":[df_limites.iloc[-1]["IGCP Average Life"]]})
    return df
    

def transformar_exposicao_cambial(df,is_carteira_ajustada = True):
    
    df = df.drop(columns= {"Row Id"})
    df = df.drop(df.index[-1])
    df["Percentagem_Antes_Derivados"] = df["(IGCP) Primary"] /np.sum(df["(IGCP) Primary"])
    df["Percentagem_Total"] = df["(IGCP) Net (EUR)"] / np.sum(df["(IGCP) Net (EUR)"])
    df=df.drop(columns ={"(IGCP) Primary","(IGCP) Net (EUR)"})
    if is_carteira_ajustada:
        df["Carteira"] = "Carteira Ajustada"
    else:
        df["Carteira"] = "Carteira ESDM"
    
    return df

def merge_exposição_cambial(df_ajustada, df_esdm):
    
    merged_df = pd.concat([df_ajustada, df_esdm], ignore_index=True)
    merged_df["Data_Avaliacao"] = yesterday_    
    merged_df = merged_df[["Data_Avaliacao","Carteira","Currency","Percentagem_Total","Percentagem_Antes_Derivados"]]
    return merged_df

def excel_serial_to_date(serial):
    # Excel's date system starts on January 1, 1900, so we adjust accordingly
    excel_start_date = datetime(1900, 1, 1) - timedelta(days=2)  # Account for Excel's leap year bug
    return (excel_start_date + timedelta(days=serial)).strftime('%Y-%m-%d')


def process_limites_table(df):
    df = df.drop(columns={"Row Id"}, errors='ignore')
    
    df = df.rename(columns={
        "(IGCP) Ocup Maturidade": "IGCP_Ocup_Maturidade",
        "(IGCP) Rating": "IGCP_Rating",
        "(IGCP) Valuation_Date": "Valuation_Date",
        "Nominal Amount": "Nominal_Amount",
        "(IGCP) Nominal Balance": "Nominal_Balance",
        "(IGCP) Nominal Forward": "Nominal_Forward",
        "(IGCP) Nominal Amount": "Total_Nominal_Amount"
    })
    
    df['Valuation_Date'] = df['Valuation_Date'].apply(
        lambda x: excel_serial_to_date(float(str(x).replace(",", "."))) if pd.notna(x) else None
    )
    
    return df

def process_posicoes_table(df):
    
    df = df.drop(columns={"Row Id"}, errors='ignore')
   
    df = df.rename(columns={"(IGCP) Valuation_Date": "Valuation_Date"})
    
    df['Valuation_Date'] = df['Valuation_Date'].apply(
        lambda x: excel_serial_to_date(float(str(x).replace(",", "."))) if pd.notna(x) else None
    )
    
    return df

def process_depos_outros(df_depos,df_outros_dep, yesterday_):
    
    df_depos = df_depos.drop(columns={"Row Id"}, errors='ignore')
    df_depos["Data_Avaliacao"] = yesterday_
    df_depos['Counterparty'] = df_depos['Counterparty'].replace('IGCP', 'Banco de Portugal')
    df_depos = df_depos.rename(columns={"Nominal Amount": "Nominal_Balance"})
    df_depos["Total_Nominal"] = df_depos["Nominal_Balance"]
    df_outros_dep = df_outros_dep.drop(columns={"Row Id"}, errors='ignore')
    df_outros_dep["Data_Avaliacao"] = yesterday_
    df_outros_dep = df_outros_dep.rename(columns={
        "Total Nominal":"Total_Nominal",
        "(IGCP) Nominal Balance": "Nominal_Balance",
        "(IGCP) Nominal Forward": "Nominal_Forward",
        "(IGCP) Maturidade":"Maturidade",
        "(IGCP) Limite Maturidade":"Limite_Maturidade",
        "(IGCP) Ocup. Maturidade":"Ocup_Maturidade",
        "(IGCP) Limite Exposicao":"Limite_Exposicao",
        "(IGCP) Ocup. Exposicao": "Ocup_Exposicao"

        
    })
    df_combined = pd.concat([df_outros_dep, df_depos], ignore_index=True, sort=False).fillna(0)
    df_combined= df_combined[["Counterparty","Total_Nominal","Nominal_Balance","Nominal_Forward","Limite_Exposicao","Ocup_Exposicao","Maturidade","Limite_Maturidade","Ocup_Maturidade","Data_Avaliacao"]]
    
    return df_combined


def process_duration_data(df_ajustada,df_cash,df_esdm,df_carteira_total, yesterday):
    df_concat = {}

    df_cash = df_cash.drop(columns={"Row Id"})
    df_ajustada = df_ajustada.drop(columns={"Row Id"})

    df_cash = df_cash[df_cash["Instrument"] == "Total"]
    df_ajustada = df_ajustada[df_ajustada["Instrument"] == "TOTAL"]
    duration_cash_ajustada = df_ajustada["(IGCP) Aux Duration"].iloc[0] / (df_ajustada["Present Value"].iloc[0] + df_cash["Present Value"].iloc[0])
    average_maturity_cash_ajustada = (df_ajustada["(IGCP) Aux Maturity"].iloc[0] + df_cash["(IGCP) Aux Maturity"].iloc[0]) / (df_ajustada["(IGCP) Cashflow Amount"].iloc[0] + df_cash["(IGCP) Cashflow Amount"].iloc[0])

    df_result = pd.DataFrame({
        "Data_avaliacao": [yesterday],
        "DurationWOcash": [df_ajustada["(IGCP) Effective Duration"].iloc[0]],
        "Duration_cash": [duration_cash_ajustada],
        "Maturity": [average_maturity_cash_ajustada],
        "MaturityWOcash": [df_ajustada["(IGCP) Aux Maturity"].iloc[0] / df_ajustada["(IGCP) Cashflow Amount"].iloc[0]],
        "Carteira": ["Carteira Ajustada"]
    })

    df_concat["CarteiraAjustada"] = df_result

    df_esdm = df_esdm.drop(columns={"Row Id"})
    df_esdm = df_esdm.loc[df_esdm["Instrument"] == "TOTAL"]

    duration_esdm_cash = df_esdm["(IGCP) Aux Duration"].iloc[0] / (df_esdm["Present Value"].iloc[0] + df_cash["Present Value"].iloc[0])
    average_maturity_esdm_cash = (df_esdm["(IGCP) Aux Maturity"].iloc[0] + df_cash["(IGCP) Aux Maturity"].iloc[0]) / (df_esdm["(IGCP) Cashflow Amount"].iloc[0] + df_cash["(IGCP) Cashflow Amount"].iloc[0])

    df_result_esdm = pd.DataFrame({
        "Data_avaliacao": [yesterday],
        "DurationWOcash": [df_esdm["(IGCP) Effective Duration"].iloc[0]],
        "Duration_cash": [duration_esdm_cash],
        "Maturity": [average_maturity_esdm_cash],
        "MaturityWOcash": [df_esdm["(IGCP) Aux Maturity"].iloc[0] / df_esdm["(IGCP) Cashflow Amount"].iloc[0]],
        "Carteira": ["Carteira ESDM"]
    })

    df_concat["Carteira ESDM"] = df_result_esdm

    df_carteira_total = df_carteira_total[df_carteira_total["Row Id"] == "Total"]

    duration_cash_carteira_total = df_carteira_total["(IGCP) Aux Duration"].iloc[0] / (df_carteira_total["Present Value"].iloc[0] + df_cash["Present Value"].iloc[0])
    carteira_total_average_maturity_cash = (df_carteira_total["(IGCP) Aux Maturity"].iloc[0] + df_cash["(IGCP) Aux Maturity"].iloc[0]) / (df_carteira_total["(IGCP) Cashflow Amount"].iloc[0] + df_cash["(IGCP) Cashflow Amount"].iloc[0])

    df_result_total = pd.DataFrame({
        "Data_avaliacao": [yesterday],
        "DurationWOcash": [df_carteira_total["IGCP Effective Duration"].iloc[0]],
        "Duration_cash": [duration_cash_carteira_total],
        "Maturity": [carteira_total_average_maturity_cash],
        "MaturityWOcash": [df_carteira_total["(IGCP) Average Maturity"].iloc[0]],
        "Carteira": ["Carteira Total"]
    })

    df_concat["Carteira Total"] = df_result_total

    merged_df = pd.concat(df_concat.values(), ignore_index=True)
    df_final = merged_df.melt(id_vars=['Data_avaliacao', 'Carteira'], var_name='Tipo', value_name='Valor')

    return df_final

    
def process_estrutura_carteira(estrutura_carteira, date_1, date_2):
    estrutura_carteira_today = estrutura_carteira.loc[estrutura_carteira["Date"] == int(date_1)]
    estrutura_carteira_yesterday = estrutura_carteira.loc[estrutura_carteira["Date"] == int(date_2)]

    keys = ["Grupo", "Instrumento", "Currency"]
    df_merged = pd.merge(
        estrutura_carteira_today,
        estrutura_carteira_yesterday,
        on=keys,
        how="outer",
        suffixes=('_curr', '_prev')
    )

    cols_to_diff = [
        "TaxaVariavelReceber",
        "TaxaVariavelPagar",
        "TaxaFixaReceber",
        "TaxaFixaPagar"
    ]

    for col in cols_to_diff:
        df_merged[col] = (
            pd.to_numeric(df_merged[f"{col}_curr"], errors="coerce").fillna(0)
            - pd.to_numeric(df_merged[f"{col}_prev"], errors="coerce").fillna(0)
        )

    df_merged = df_merged[
        ["Date_curr", "Grupo", "Instrumento", "Currency"] + cols_to_diff
    ]

    return df_merged

def insert_multiple_to_sql(dataframe_table_pairs, conn, if_exists='append'):
    """
    Inserts multiple DataFrames into SQL tables using a list of (DataFrame, table_name) pairs.

    Parameters:
    - dataframe_table_pairs (list of tuples): Each tuple is (DataFrame, 'table_name')
    - conn: SQLAlchemy or SQLite connection object
    - if_exists (str): 'append', 'replace', or 'fail'
    """
    for df, table_name in dataframe_table_pairs:
        if not df.empty:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        else:
            print(f"Skipped '{table_name}': DataFrame is empty.")

def sum_next_12_months_esdm(df,start_date, years=1):
    end_date = start_date + pd.DateOffset(years=years)
    mask = (df["Payment Date" ] > start_date) & (df["Payment Date" ] <= end_date)
    return df.loc[mask, "payment_amount_wo_bt_aforro"].sum()

def sum_next_12_months_aforro(df,start_date,years =1):
    end_date = start_date + pd.DateOffset(years=years)
    mask = (df["DTREEMBOLSO"] > start_date) & (df["DTREEMBOLSO"] <= end_date)
    return df.loc[mask, "Capital + juros"].sum()

def process_refinancing_forward(esdm, aforro, date,db_paths,conn):
    
    df_aforro_ = aforro.copy() 
    df_esdm_ = esdm.copy()
    df_aforro_ = df_aforro_.sort_values(by="DTREEMBOLSO", ascending=True)
    df_aforro["Capital + juros"]=df_aforro_["CAPITAL"] + df_aforro_["JUROS"]
    df_aforro_ = df_aforro.loc[df_aforro["DTREEMBOLSO"] > date.date()]
    df_esdm_['Payment Date'] = df_esdm_['Payment Date'].apply(
        lambda x: excel_serial_to_date(float(str(x).replace(",", "."))) if pd.notna(x) else None)
    df_esdm_["Payment Date"] = pd.to_datetime(df_esdm_["Payment Date"])
    mask = df_esdm_["Instrument"].str.startswith(("BT", "R-CA", "R-CT", "CALL"))
    df_esdm_["payment_amount_wo_bt_aforro"] = df_esdm_["Payment Amount"].where(~mask, 0)
    stock_bt = df_esdm_.loc[esdm["Instrument"].str.startswith(("BT"))]["Payment Amount"].sum()
    stock_aforro = -df_aforro_["Capital + juros"].sum()
    stock_divida_excl_cedic_cedim = df_esdm_["payment_amount_wo_bt_aforro"].sum() + stock_bt + stock_aforro
    years = range(date.year, date.year + 7)
    dates = [datetime(year, 12, 31) for year in years]
    df = pd.DataFrame({"dates": dates})
    df["refinancing_risk_1y_wo_bt_aforro"] = df["dates"].apply(lambda d: sum_next_12_months_esdm(df_esdm_,d,years=1) )/stock_divida_excl_cedic_cedim
    df["bt_refinancing"] = stock_bt/stock_divida_excl_cedic_cedim
    df["aforro_1y_refinancing"] = df["dates"].apply(lambda d:sum_next_12_months_aforro(df_aforro_,d,years=1))
    df["refinancing_risk_1y_total"] = df["refinancing_risk_1y_wo_bt_aforro"] + df["bt_refinancing"] - df["aforro_1y_refinancing"]/stock_divida_excl_cedic_cedim 
    df["refinancing_risk_5y_wo_bt_aforro"] = df["dates"].apply(lambda d: sum_next_12_months_esdm(df_esdm_,d,years=5) )/stock_divida_excl_cedic_cedim
    df["aforro_5y_refinancing"] = df["dates"].apply(lambda d:sum_next_12_months_aforro(df_aforro_,d,years=5))
    df["refinancing_risk_5y_total"] = df["refinancing_risk_5y_wo_bt_aforro"] + df["bt_refinancing"] - df["aforro_5y_refinancing"]/stock_divida_excl_cedic_cedim
    previous_year_str = datetime(date.year -1, 12, 31).strftime("%Y-%m-%d")
    df_limites_guidelines = pd.read_sql(
        f"SELECT * FROM limites_guidelines WHERE Data_Avaliação = '{previous_year_str}'",
        conn
        )
    previous_end_year_1y_value = df_limites_guidelines.loc[(df_limites_guidelines["Label"] == "0-1Y")&(df_limites_guidelines["Indicador_Risco"] == "Refinanciamento")].Guidelines_Propostas.iloc[0]
    previous_end_year_5y_value = df_limites_guidelines.loc[(df_limites_guidelines["Label"] == "0-5Y")&(df_limites_guidelines["Indicador_Risco"] == "Refinanciamento")].Guidelines_Propostas.iloc[0]
    new_row = {
    "dates": previous_year_str,
    "refinancing_risk_1y_total": previous_end_year_1y_value,
    "refinancing_risk_5y_total": previous_end_year_5y_value
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df["dates"] = pd.to_datetime(df["dates"], errors="coerce")
    df = df.sort_values("dates").reset_index(drop=True)
    df["reference_date"] = date.strftime("%Y-%m-%d")
    
    return df