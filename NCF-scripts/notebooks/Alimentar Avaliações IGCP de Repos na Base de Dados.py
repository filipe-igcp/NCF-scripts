import sys
sys.path.append(r"I:\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\NCF-Scripts\notebooks\src")
from colateral.processar_avaliacoes_repos_derivados import *
from IPython.display import display
warnings.simplefilter(action='ignore', category=Warning)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
excel_dir = "\\\\igcp.pt\\wss\\Import_Export\\Export\\PRD\\Dashboard_Export\\"
today = datetime.today()
days_to_subtract = 1 if today.weekday() > 0 else 3
yesterday = (today - timedelta(days=days_to_subtract)).strftime('%Y%m%d')
file_name = f"NCF-REPOS_{yesterday}_*.xlsx"
file_path_ = os.path.join(excel_dir, file_name)
file_paths = glob.glob(file_path_)
excel_file = pd.ExcelFile(file_paths[-1])
df_avaliacoes_table = pd.read_excel(excel_file, sheet_name="Avaliacoes", skiprows=1)
df_precos_table = pd.read_excel(excel_file, sheet_name="Coll_prices", skiprows=1)
db_paths = r"\\igcp-fs\NCF-GER\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\DASHBOARD_DIARIO\DB\Colateral_Repos.db"
con = sqlite3.connect(db_paths)
atualizar_bd_avaliacoes_igcp_repos(df_avaliacoes_table,today, con, nome_tabela = "avaliacoes_igcp_repos", salvar = False)
atualizar_precos_igcp_repos(df_precos_table, con, nome_tabela = "preco_instrumentos_coll", salvar = False)
#display("tabela avaliações IGCP repos:",atualizar_bd_avaliacoes_igcp_repos(df_avaliacoes_table,today, con, nome_tabela = "avaliacoes_igcp_repos", salvar = False))
#display("tabela preços dos instrumentos IGCP repos:",atualizar_precos_igcp_repos(df_precos_table, con, nome_tabela = "preco_instrumentos_coll", salvar = False))

