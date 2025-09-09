import sys
sys.path.append(r"I:\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\NCF-Scripts\notebooks\src")
from colateral.processar_avaliacoes_repos_derivados import *
from IPython.display import display
warnings.simplefilter(action='ignore', category=Warning)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
today = datetime.today()
db_paths = r"\\igcp-fs\NCF-GER\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\DASHBOARD_DIARIO\DB\Colateral_Repos.db"
con = sqlite3.connect(db_paths)
excel_dir_emails = r"\\igcp-fs\ncf-ger\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\DASHBOARD_DIARIO\emails_contrapartes"
days_to_subtract = 1 if today.weekday() > 0 else 3
close_business = (today - timedelta(days=days_to_subtract)).strftime("%Y%m%d")
file_paths = {
    "BRED": generate_file_path(excel_dir_emails, f"BRED_Valuation_{close_business}"),
    "BSCH": generate_file_path(excel_dir_emails, f"ISMA-BSTE-TSPT_BSCH_{close_business}"),
    "CALY": generate_file_path(excel_dir_emails, f"CALY_Valuation_{close_business}"),
    "BNPA": generate_file_path(excel_dir_emails, f"BNP_Valuation_{close_business}"),
    "HSBC": generate_file_path(excel_dir_emails, f"HSBC_Valuation_{close_business}"),
    "CITI": generate_file_path(excel_dir_emails, f"CITI_Valuation_{close_business}"),
    "CGD": generate_file_path(excel_dir_emails, f"CGD_Valuation_{close_business}"),
    "BCOM": generate_file_path(excel_dir_emails, f"Millenium_Valuation_{close_business}"),
    "BKBK": generate_file_path(excel_dir_emails, f"Bankinter_Valuation_{close_business}"),
    "BBVA": generate_file_path(excel_dir_emails, f"BBVA_Valuation_{close_business}"),
    "NOMU": generate_file_path(excel_dir_emails, f"Nomura_Valuation_{close_business}"),
    "NATI": generate_file_path(excel_dir_emails, f"Nati_Valuation_{close_business}")
}

avaliacoes = atualizar_bd_avaliacoes_contrapartes_repos(file_paths,today,excel_dir_emails, db_paths, con, clear = True,nome_tabela = "avaliacoes_contrapartes")
display("tabela ctpy avaliações de repos:",avaliacoes)
avaliacoes.to_sql("avaliacoes_contrapartes", conn, if_exists="append", index=False)
print("Dados inseridos na Base de Dados")

