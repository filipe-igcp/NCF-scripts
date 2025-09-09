import os
import pandas as pd
from datetime import datetime,timedelta
import sqlite3
import warnings
from pypdf import PdfReader
import re
import numpy as np
warnings.simplefilter(action='ignore', category=Warning)

def convert_excel_serial_to_datetime(x):
    if pd.isna(x):
        return pd.NaT

    if isinstance(x, (int, float)):
        
        return pd.to_datetime('1899-12-30') + pd.to_timedelta(int(x), 'D')

    if isinstance(x, str):
        
        try:
            return datetime.strptime(x, '%d/%b/%Y@%H:%M:%S')
        except ValueError:
            pass
        try:
            return datetime.strptime(x.title(), '%d/%b/%Y')
        except ValueError:
            pass

    return pd.to_datetime(x, errors='coerce')

def generate_file_path(base_dir, filename):
    """
    
    Generate potential file paths with valid extensions.
    Returns a dictionary of file paths to check.
    """
    valid_extensions = [".xlsx",".XLS", ".xls", ".pdf"]
    return {ext: os.path.join(base_dir, f"{filename}{ext}") for ext in valid_extensions}


def clean_number(value):
    """Remove commas, replace empty values with 0, and convert to float."""
    if value in [None, "", "-"]:
        return 0.0  
    return float(value.replace(".", "").replace(",", "."))


def load_processed_files_log(log_path, clear=False):
    if os.path.exists(log_path):
        with open(log_path, 'r') as file:
            processed_files = set(file.read().splitlines())
        if clear:
            processed_files.clear()
    else:
        processed_files = set()
    return processed_files

def save_processed_files_log(log_path, processed_files):
    with open(log_path, 'w') as file:
        file.write('\n'.join(processed_files))

def get_resolved_path(paths):
    for ext, path in paths.items():
        if os.path.exists(path):
            return path
    return None

def load_excel_file(path):
    file_extension = os.path.splitext(path)[1].lower()
    if file_extension in ['.xlsx', '.xls', '.xlrd']:
        engine = 'openpyxl' if file_extension == '.xlsx' else 'xlrd'
        return pd.ExcelFile(path, engine=engine)
    elif file_extension == '.pdf':
        return "pdf"
    else:
        return None

def process_repos_bred(excel_file, today):
    df = pd.read_excel(excel_file, sheet_name="PORTUG EUR")
    df = df[["ISIN Code", "Nominal","Settled Amount","Billing","Clean Price","Dirty Price", "Exposure"]]  
    df = df[["ISIN Code", "Nominal","Settled Amount","Billing","Clean Price","Dirty Price", "Exposure"]].rename(columns={
        "ISIN Code": "ISIN",
        "Nominal": "Nominal",
        "Settled Amount": "Start_cash",
        "Billing": "Accrued_interest_cash",
        "Clean Price": "Clean_price",
        "Dirty Price": "Dirty_price",
        "Exposure": "MTM"
    })
    df["Accrued_interest_bond"] = (df["Dirty_price"] - df["Clean_price"]) / 100 * df["Nominal"]
    df["Contraparte"] = "BRED PAR"
    df["Position_date"] = today.strftime('%Y-%m-%d')
    return df

def process_repos_bsch(excel_file, today):
    df = pd.read_excel(excel_file, sheet_name="Sheet1", skiprows=7)
    df["Nominal"] = -df["Nominal"]
    df["Repo Accrued Interest"] = -df["Repo Accrued Interest"]
    df["Accrued_interest_bond"] = df["Bond Accrued Interest"]/100 * df["Nominal"]
    df = df[["Trade Date", "Value Date", "Maturity Date",
                       "Principal","Underlying.Product Code.ISIN","Nominal",
                       "MTM Base ccy","Dirty Price","Repo Accrued Interest", 
                       "Clean Price","Accrued_interest_bond"]]
    df.columns = ["Opening_date","Value_date","Expiry_date",
                            "Start_cash","ISIN", "Nominal", "MTM", "Dirty_price",
                            "Accrued_interest_cash", "Clean_price", "Accrued_interest_bond"]
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "BSCH MAD"
    df=df.dropna()
    
    return df

def process_repos_caly(excel_file,today):
    
    df = pd.read_excel(excel_file, sheet_name="PORTFOLIO", skiprows=5)
    df = df[["Trade Date", "Maturity Date", "Exchanged Notional_1 Amount", "Underlying",
                            "MtM_Native Currency Amount","Strike Price"]]
    df.columns = ["Value_date","Expiry_date","Exchanged_Notional", "Underlying",
                                "MtM_Native_Currency","Clean_price"]
    f_rows = df[df['Underlying'].str.startswith('F ', na=False)].reset_index(drop=True)
    
    title_rows = df[df['Underlying'].str.match(r'^[A-Z0-9]+$', na=False)].reset_index(drop=True)
    
    assert len(f_rows) == len(title_rows)

    df_ = pd.DataFrame({
        'Value_date': f_rows['Value_date'],
        'Expiry_date': f_rows['Expiry_date'],
        'Contraparte': "CALY PAR",
        'Position_date': today.strftime('%Y-%m-%d'),
        'Nominal': f_rows['Exchanged_Notional'],
        'ISIN': title_rows['Underlying'],
        'MTM': f_rows['MtM_Native_Currency'] + title_rows['MtM_Native_Currency'],
        'Clean_price':title_rows['Clean_price']
        
    })
    
    return df_

def process_repos_bnp(excel_file,today):
    
    df = pd.read_excel(excel_file, sheet_name="BNPPRepoExposureStatement", skiprows=13)
    df = df[["Trade Start","Trade Execution Date","Trade End","Mkt Dirty Price",
                        "Instr Identifier","Start Cash","Interest on Starting Cash","Exposure Amount",
                            "Notional","Mkt Clean Price"]]
    df=df.dropna()
    df["Start Cash"]=-df["Start Cash"]
    df["Interest on Starting Cash"]=-df["Interest on Starting Cash"]
    df["Accrued_interest_bond"] = (df["Mkt Dirty Price"] - df["Mkt Clean Price"])/100 * df["Notional"]
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "BNPA PAR"
    df = df.rename(columns={
        "Trade Start": "Value_date",
        "Trade Execution Date": "Opening_date",
        "Trade End": "Expiry_date",
        "Mkt Dirty Price": "Dirty_price",
        "Instr Identifier": "ISIN",
        "Start Cash": "Start_cash",
        "Interest on Starting Cash": "Accrued_interest_cash",
        "Exposure Amount": "MTM",
        "Notional": "Nominal",
        "Mkt Clean Price": "Clean_price"
    })
    
    return df

def process_repos_citi(excel_file,today):
    
    first_sheet_name = excel_file.sheet_names[0]
    df = pd.read_excel(excel_file, sheet_name=first_sheet_name)
    index_start = df[df.iloc[:, 0] == "Activity Type"].index[0]
    df = df.iloc[index_start:]
    df.columns = df.iloc[0]
    df= df[["On Date","Off Date","ISIN",
                                "Orig Face","Mkt Price","Full Price",
                                "Market Value (Call CCY)",
                                "Principal (Call CCY)",
                              "Financing Interest (Call CCY)",
                                "Margin Valuation (Call CCY)"
                                ]]
    df = df.iloc[1:] 
    first_nan_pos = df.index.get_loc(df[df.isna().any(axis=1)].index[0])
    df = df.iloc[:first_nan_pos]
    df = df.rename(columns={
        "On Date": "Value_date",
        "Off Date": "Expiry_date",
        "Orig Face": "Nominal",
        "Mkt Price": "Clean_price",
        "Full Price": "Dirty_price",
        "Market Value (Call CCY)": "Valor_Mercado_Títulos",
        "Principal (Call CCY)": "Start_cash",
        "Margin Valuation (Call CCY)": "MTM",
        "Financing Interest (Call CCY)": "Accrued_interest_cash"
    })
    
    df["Accrued_interest_bond"] = (df["Dirty_price"] - df["Clean_price"])/100 * df["Nominal"]
    df["MTM"] = -df["MTM"]
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "CITI FFT"
    df = df.drop(columns=["Valor_Mercado_Títulos"])
    
    return df

def process_repos_cgd(excel_file, today):
    
    sheet_names = excel_file.sheet_names
    df = pd.read_excel(excel_file, sheet_name=sheet_names[0], skiprows=0)
    df = df[["Trade Date","Maturity Date","Notional1","MTM"]]
    df.columns = ["Opening_date","Expiry_date","Nominal","MTM"]
    df["Start_cash"] = df["Nominal"]
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "CGDI LIS"
    
    return df

def process_repos_bcom(excel_file, today):
    
    sheet_names = excel_file.sheet_names
    df = pd.read_excel(excel_file, sheet_name=sheet_names[0], skiprows=3)
    df = df[["Trade Date","Effective Date","Maturity Date",
             "Exchanged Notional 1 Amount","Net Exposure"]]
    df = df.dropna()
    df.columns = ["Opening_date","Value_date","Expiry_date","Start_cash","MTM"]
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "BCOM LIS"
    
    return df

def process_repos_hsbc(excel_file,today):
    
    df = pd.read_excel(excel_file, sheet_name="pNonCSV_1", skiprows=0)  

    header_row = None
    for i in range(5, 20):  
        row_values = df.iloc[i].tolist()  
        if all(col in row_values for col in ["Effective Date", "Maturity Date", "Notional 1", "ISIN","Start Cash", "Total Cash", "Clean Price", "Dirty Price", "Haircut", "Repo Rate", "Agreement Ccy", "Exposure (Agree Ccy)"]):  # Look for known columns
            header_row = i
            break

    if header_row is None:
        raise ValueError("Could not find the correct header row.")

    df = pd.read_excel(excel_file, sheet_name="pNonCSV_1", skiprows=header_row + 1)
    df = df.dropna()
    df["Accrued_interest_cash"] = df["Total Cash"]-df["Start Cash"]
    df["Accrued_interest_bond"] = (df["Dirty Price"]-df["Clean Price"])/100 * df["Notional 1"]
    
    df = df[["Effective Date","Maturity Date","Notional 1", "ISIN","Start Cash","Accrued_interest_cash",
                        "Accrued_interest_bond","Clean Price", "Dirty Price","Exposure (Agree Ccy)"]]
    df.columns = ["Value_date","Expiry_date","Nominal", "ISIN", "Start_cash","Accrued_interest_cash",
                             "Accrued_interest_bond","Clean_price","Dirty_price","MTM"]
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "HSBC PAR"
    
    return df

def process_repos_bkbk(excel_file,today):
    
    first_sheet_name = excel_file.sheet_names[0]
    df = pd.read_excel(excel_file, sheet_name=first_sheet_name)
    df=df[["ISIN","F. ida","F.vuelta","Nominal","Price Clean","Pata: cash","Pata: bono","Intereses","Neto + Intereses"]]
    df.columns = ["ISIN", "Value_date", "Expiry_date",
                  "Nominal", "Clean_price","Start_cash",
                  "Valor_Mercado_Títulos", "Accrued_interest_cash", "MTM" ]
    df["Start_cash"] = -df["Start_cash"]
    df["Accrued_interest_cash"]= -df["Accrued_interest_cash"]
    df["Dirty_price"] = (df["Valor_Mercado_Títulos"]/df["Nominal"])*100
    df["Accrued_interest_bond"] = (df["Dirty_price"]-df["Clean_price"])/100 * df["Nominal"]
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "BKBK MAD"
    df= df.drop(columns = {"Valor_Mercado_Títulos"})
    
    return df

def process_repos_nomu(excel_file,today):
    
    first_sheet_name = excel_file.sheet_names[0]
    df = pd.read_excel(excel_file, sheet_name=first_sheet_name)
    df= df[["ISIN", "Nominal","Clean Price","Dirty Price","Start Cash","On Date","Off Date","Trade Date",
            "Repo Interest","Collateral MTM (EUR)"]]
    df["Accrued_interest_bond"] = (df["Dirty Price"] - df["Clean Price"])/100 * df["Nominal"]
    df = df.dropna()
    df=df.rename(columns = {"Collateral MTM (EUR)":"MTM", "Clean Price": "Clean_price", "Dirty Price": "Dirty_price", "Start Cash": "Start_cash", 
                            "On Date": "Value_date", "Off Date": "Expiry_date", "Trade Date": "Opening_date", "Repo Interest":"Accrued_interest_cash"})
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "NOMU FFT"
    
    return df

def process_repos_bbva(resolved_path,today):
    
    reader = PdfReader(resolved_path)

    text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
    
    columns = [
        "Trade ID", "Trade Date", "Effective Date", "Maturity Date",
        "Not1Ccy", "Notional (1)", "Not2Ccy", "Notional (2)", "ProductID",
        "Buy/Sell", "Issue", "Underlying Name", "Fair Price",
        "Accrual", "PV", "PV Ccy", "CACcyPV", "CACcy"
    ]
    
    trade_pattern = (
        r"(\d+)\s+"                                 # Trade ID
        r"(\d{2}/\w{3}/\d{4})\s+"                   # Trade Date
        r"(\w{3} \d{1,2}, \d{4})\s+"                # Effective Date
        r"(\d{2}/\w{3}/\d{4})\s+"                   # Maturity Date
        r"([A-Z]{3})\s+([\d.,]+)\s+"                # Not1Ccy + Notional (1)
        r"([A-Z]{3})\s+([\d.,]+)\s+"                # Not2Ccy + Notional (2)
        r"(\S+)\s+"                                 # ProductID
        r"(\w+)\s+"                                 # Buy/Sell
        r"(\S+)\s+"                                 # Issue
        r"(\S+)\s+"                                 # Underlying Name
        r"([-\d.,]+)\s+"                            # Fair Price (required)
        r"(?:([-\d.,]+)\s+)?"                       # Accrual (optional group with spacing)
        r"([-\d.,]+)\s+"                            # PV (required)
        r"([A-Z]{3})?\s+"                           # PV Ccy (optional)
        r"([-\d.,]+)?\s+"                           # CACcyPV (optional)
        r"([A-Z]{3})?"                              # CACcy (optional)
        )

    
    matches = re.findall(trade_pattern, text, re.MULTILINE)
    
    if matches:
        trade_data = [list(match) for match in matches]  
        
        
        for row in trade_data:
            for i in range(len(row)):
                row[i] = row[i].strip() if row[i] else None
        
        df = pd.DataFrame(trade_data, columns=columns)  
        df["Accrual"] = df["Accrual"].replace("", None)
        df["PV"] = df.apply(lambda row: row["Accrual"] if row["PV"] is None else row["PV"], axis=1)
        df["Accrual"] = df.apply(lambda row: None if row["PV"] == row["Accrual"] else row["Accrual"], axis=1)
        df["Notional (1)"] = df["Notional (1)"].apply(clean_number)
        df["Fair Price"] = df["Fair Price"].apply(clean_number)
        df["Accrual"] = df["Accrual"].apply(lambda x: clean_number(x) if x is not None else None)  
        df["CACcyPV"] = df["CACcyPV"].apply(clean_number)
        df["Dirty_price"] = df["Fair Price"] + (df["Accrual"].fillna(0)) 
        df["Accrued_interest_bond"] = (df["Notional (1)"] * df["Accrual"]/100).fillna(0)
        df["Contraparte"] = "BBVA MAD"
        df["Position_date"] = today.strftime('%Y-%m-%d')
    
        df = df.rename(columns={
            "Issue": "ISIN",
            "Notional (1)": "Nominal",
            "CACcyPV": "MTM",
            "Trade Date": "Opening_date",
            "Effective Date": "Value_date",
            "Maturity Date": "Expiry_date",
            "Fair Price": "Clean_price",
            
        })
        df = df[[  "Opening_date", "Value_date", "Expiry_date",
                    "ISIN","Nominal", "Contraparte", "Position_date", 
                    "Clean_price",
                    "Dirty_price","MTM",
                     "Accrued_interest_bond"
            ]]
        
        return df
        
    else:
        print("\n No trade data found for BBVA, Check formatting.")

        
def process_repos_nati(excel_file,today):
    
    first_sheet_name = excel_file.sheet_names[0]
    df = pd.read_excel(excel_file, sheet_name=first_sheet_name, skiprows=26)
    df = df[["Start Date","End Date","Issue",
             "Face","Start Cash","Clean Price",
             "Accrued","Dirty Price","Repo Interest",
            "MTM"]]
    df.columns = ["Value_date","Expiry_date","ISIN",
                  "Nominal","Start_cash","Clean_price",
                  "Accrued","Dirty_price","Accrued_interest_cash",
                 "MTM"]
    df["Accrued_interest_bond"] = df["Nominal"] * df["Accrued"]/100
    df["Position_date"] = today.strftime('%Y-%m-%d')
    df["Contraparte"] = "NATI PAR"
    df = df.drop(columns={"Accrued"})
    df = df.dropna()
    
    return df

def process_der_ms(excel_file):
    df = pd.read_excel(excel_file, sheet_name="SWAPS", header=0)  
    df["counterparty"] = "MSLN FFT"
    df=df.groupby("counterparty", as_index=False)["collat_req_in_rpt_ccy"].sum()
    df.rename(columns={"collat_req_in_rpt_ccy": "mv_ctpy"}, inplace=True)
    return df

def process_der_bnp(excel_file):
    df = pd.read_excel(excel_file, sheet_name="Exposure Statement", header=7)  
    df["counterparty"] = "BNPA PAR"
    df = df.groupby("counterparty", as_index=False)["Exposure Amount"].sum()
    df.rename(columns={"Exposure Amount": "mv_ctpy"}, inplace=True)
    df["mv_ctpy"] = df["mv_ctpy"]*-1 
    return df

def process_der_caly(excel_file):
    df = pd.read_excel(excel_file, sheet_name="PORTFOLIO", header=5)  
    df["counterparty"] = "CALY PAR"
    df = df.groupby("counterparty", as_index=False)["MTM_Base Currency Amount"].sum()
    df.rename(columns={"MTM_Base Currency Amount": "mv_ctpy"}, inplace=True)
    df["mv_ctpy"] = df["mv_ctpy"]*-1 
    return df

def process_repos_counterparty(counterparty, resolved_path, today):
    
    file_extension = os.path.splitext(resolved_path)[1].lower()
    if file_extension == '.pdf' and counterparty == "BBVA":
        return process_bbva(resolved_path, today)
    else:
        excel_file = load_excel_file(resolved_path)
        if excel_file == "pdf":
            print(f"{counterparty} file is a PDF.")
            return None
        if excel_file is None:
            print(f"Unsupported file type for {counterparty}: {file_extension}")
            return None
        
        if counterparty == "BRED":
            return process_repos_bred(excel_file, today)
        elif counterparty == "BSCH":
            return process_repos_bsch(excel_file, today)
        elif counterparty == "CALY":
            return process_repos_caly(excel_file, today)
        elif counterparty == "BNPA":
            return process_repos_bnp(excel_file, today)
        elif counterparty == "CITI":
            return process_repos_citi(excel_file, today)
        elif counterparty == "CGD":
            return process_repos_cgd(excel_file, today)
        elif counterparty == "BCOM":
            return process_repos_bcom(excel_file, today)
        elif counterparty == "HSBC":
            return process_repos_hsbc(excel_file, today)
        elif counterparty == "BKBK":
            return process_repos_bkbk(excel_file, today)
        elif counterparty == "NOMU":
            return process_repos_nomu(excel_file, today)
        elif counterparty == "NATI" :
            return process_repos_nati(excel_file, today)
        
    return None

def process_der_counterparty(counterparty, resolved_path):
    
    file_extension = os.path.splitext(resolved_path)[1].lower()
    if file_extension == '.pdf' and counterparty == "BBVA":
        return process_bbva(resolved_path, today)
    else:
        excel_file = load_excel_file(resolved_path)
        if excel_file == "pdf":
            print(f"{counterparty} file is a PDF.")
            return None
        if excel_file is None:
            print(f"Unsupported file type for {counterparty}: {file_extension}")
            return None
    
        elif counterparty == "CALY":
            return process_der_caly(excel_file)
        elif counterparty == "BNPA":
            return process_der_bnp(excel_file)
        elif counterparty == "MS":
            return process_der_ms(excel_file)
        
        
    return None

def atualizar_bd_avaliacoes_contrapartes_repos(file_paths: dict,date: str,excel_dir: str, db_paths: str, con, salvar: bool = False, clear: bool = False,nome_tabela: str = "avaliacoes_contrapartes") -> pd.DataFrame:
    processed_files_log = "email_files.txt"
    today = date
    
    processed_files = load_processed_files_log(processed_files_log, clear)

    all_data = []
    required_columns = ["Contraparte", "Position_date", "Opening_date", "Value_date", "Expiry_date",
                        "ISIN", "Start_cash", "Nominal", "Clean_price", "Dirty_price",
                        "Accrued_interest_cash", "Accrued_interest_bond", "MTM"]

    for counterparty, paths in file_paths.items():
        resolved_path = get_resolved_path(paths)
        if not resolved_path:
            print(f"[INFO] No valid file found for {counterparty}")
            continue

        if resolved_path in processed_files:
            print(f"[INFO] Skipping already processed file: {resolved_path}")
            continue

        try:
            df = process_counterparty(counterparty, resolved_path, today)
            if df is not None and not df.empty:
                
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None  
                df = df[required_columns]
                all_data.append(df)

                processed_files.add(resolved_path)  
            else:
                print(f"[WARNING] Empty or null DataFrame for {counterparty}")
        except Exception as e:
            print(f"[ERROR] Processing failed for {counterparty}: {e}")

    save_processed_files_log(processed_files_log, processed_files)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        
        date_columns = ['Opening_date', 'Position_date', 'Value_date', 'Expiry_date']
        for col in date_columns:
            final_df[col] = final_df[col].apply(convert_excel_serial_to_datetime)
        if salvar and con is not None:
            final_df.to_sql(nome_tabela, con, if_exists="append", index=False)
            print(f"Dados alimentados na tabela {nome_tabela}")
    else:
        print("[INFO] No new data collected.")
    return final_df

def process_der_valuation_files(file_paths):
    all_data = []
    for counterparty, paths in file_paths.items():
        resolved_path = get_resolved_path(paths)
        all_data.append(process_der_counterparty(counterparty, resolved_path))
    
    return  pd.concat(all_data, ignore_index=True)

def atualizar_bd_var_multifator(df:pd.DataFrame,con,salvar: bool = False,horizon:str ="10D",nome_tabela: str ="VaR",) -> pd.DataFrame:
   
    df = df.copy()
    df["horizon"] = horizon
    df = df.reset_index()
    df = df.rename(columns={"index": "counterparty", "VaR Multi-Factor": "VaR"})
    df["VaR"] = df["VaR"] * -1
    df["expected_shortfall"] = df["VaR"] * 1.25404034359605
    df = df[["counterparty", "reference_date", "horizon", "VaR", "expected_shortfall"]]
    
    if salvar and con is not None:
        df.to_sql(nome_tabela,con,if_exists='append', index=False )
        print(f"Dados alimentados na tabela {nome_tabela}")
    
    return df

def atualizar_bd_monitorizacao_derivados(df_derivados: pd.DataFrame,final_df: pd.DataFrame,VaR_parametrico_multi_factor: pd.DataFrame,
                                             date: str,excel_serial_to_date,trade_params_df: pd.DataFrame,
                                             con,salvar: bool = False,nome_tabela: str = "mon_limites_derivados") -> pd.DataFrame:
    
    df_derivados.rename(
        columns={
            "Counterparty": "counterparty",
            "(IGCP) Rating": "rating",
            "(IGCP) Max Maturity": "maturidade_maxima",
            "(IGCP) Ocup Maturidade": "ocup_maturidade",
            "(IGCP) Limite Exposicao": "limite_exposicao",
            "Valuation Date": "reference_date",
            "Expiry/Maturity": "maturity_date",
            "(IGCP) Limite Maturidade": "limite_maturidade",
            "(IGCP) Derivatives MV": "igcp_derivatives_mv",
            "(IGCP) Collateral Nominal": "collateral_nominal",
            "(IGCP) Collateral MV": "collateral_mv",
        },
        inplace=True,
    )

    df_monitorizacao_swaps = df_derivados.copy()
    df_monitorizacao_swaps = df_monitorizacao_swaps.drop(columns={"Row Id","maturity_date"})
    df_monitorizacao_swaps = df_monitorizacao_swaps.dropna()

    df_monitorizacao_swaps["reference_date"] = (
        df_monitorizacao_swaps["reference_date"].str.replace(",", ".").astype(float)
    )
    df_monitorizacao_swaps["reference_date"] = df_monitorizacao_swaps[
        "reference_date"
    ].apply(excel_serial_to_date)

    df_monitorizacao_swaps = pd.merge(
        df_monitorizacao_swaps, final_df, on="counterparty", how="outer"
    )

    VaR_parametrico_multi_factor = VaR_parametrico_multi_factor.reset_index()
    VaR_parametrico_multi_factor = VaR_parametrico_multi_factor.rename(
        columns={"VaR Multi-Factor": "VaR", "index": "counterparty"}
    )
    VaR_parametrico_multi_factor["VaR"] = VaR_parametrico_multi_factor["VaR"] * -1
    
    df_monitorizacao_swaps = pd.merge(
        df_monitorizacao_swaps,
        VaR_parametrico_multi_factor[["counterparty", "VaR"]],
        on="counterparty",
        how="inner",
    )
    
    df_monitorizacao_swaps = pd.merge(
        df_monitorizacao_swaps,
        pd.DataFrame(trade_params_df.groupby(["counterparty"])["notional"].sum()).reset_index(),
        on="counterparty",
        how="inner",
    )
    df_monitorizacao_swaps.rename(columns = {"notional":"nominal"}, inplace = True)
    
    df_monitorizacao_swaps["exposicao"] = np.maximum(
        0,
        df_monitorizacao_swaps["mv_ctpy"]
        - df_monitorizacao_swaps["collateral_nominal"].clip(lower=0)
        - df_monitorizacao_swaps["VaR"],
    )

    df_monitorizacao_swaps["ocup_exposicao"] = (
        df_monitorizacao_swaps["exposicao"] / df_monitorizacao_swaps["limite_exposicao"]
    )

    df_monitorizacao_swaps.drop(
        columns={
            "igcp_derivatives_mv",
            "mv_ctpy",
            "VaR",
            "collateral_nominal",
            "collateral_mv",
        },
        inplace=True,
    )

    df_monitorizacao_swaps["reference_date"] = date
    
    df_monitorizacao_swaps = df_monitorizacao_swaps[["counterparty","reference_date","rating","maturidade_maxima","limite_maturidade","ocup_maturidade","limite_exposicao","exposicao","ocup_exposicao","nominal"]]
    
    if salvar and con is not None:
        df_monitorizacao_swaps.to_sql(nome_tabela, con, if_exists='append', index=False)
        print(f"Dados alimentados na tabela {nome_tabela}")
    
    return df_monitorizacao_swaps

def atualizar_avaliacao_der_bd(final_df: pd.DataFrame, df_derivados: pd.DataFrame, date: str, con, salvar: bool = False, nome_tabela: str = "avaliacao_derivados") -> pd.DataFrame:
    
    avaliacao_swaps = final_df.copy()
    avaliacao_swaps["reference_date"] = date

    avaliacao_swaps = pd.merge(
        avaliacao_swaps,
        df_derivados[["counterparty", "igcp_derivatives_mv", "collateral_nominal", "collateral_mv"]],
        on="counterparty",
        how="outer"
    )

    avaliacao_swaps["mv_diff"] = avaliacao_swaps["igcp_derivatives_mv"] - avaliacao_swaps["mv_ctpy"]

    avaliacao_swaps["movimento"] = np.where(
        (avaliacao_swaps["collateral_nominal"] - avaliacao_swaps["mv_ctpy"].round(-5)).abs() > 1_000_000,
        avaliacao_swaps["collateral_nominal"] - avaliacao_swaps["mv_ctpy"].round(-5),
        0
    )

    avaliacao_swaps.dropna(inplace=True)

    avaliacao_swaps = avaliacao_swaps[[
        "counterparty",
        "reference_date",
        "collateral_nominal",
        "collateral_mv",
        "igcp_derivatives_mv",
        "mv_ctpy",
        "mv_diff",
        "movimento"
    ]]

    if salvar and con is not None:
        avaliacao_swaps.to_sql(nome_tabela, con, if_exists='append', index=False)
        print(f"Dados alimentados na tabela {nome_tabela}")

    return avaliacao_swaps
    
    
