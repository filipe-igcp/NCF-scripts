import sys
sys.path.append(r"I:\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\NCF-Scripts\notebooks\src")
from var.avaliacao_swaps import *
from var.swap_trades import trade_params_df 
from colateral.processar_avaliacoes_repos_derivados import *
from var.queries import *
from dashboard_diario.dashboard_diario import process_files_for_patterns,repair_last_file,repair_and_save_excel
from dateutil.relativedelta import relativedelta
import os
from IPython.display import display
warnings.simplefilter(action='ignore', category=Warning)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
reference_date = datetime.today()
reference_date_str = reference_date.strftime('%Y%m%d')
reference_date_str_2 = reference_date.strftime('%Y-%m-%d')
bbg_date= ql.TARGET().advance(ql.Date.todaysDate(), -1, ql.Days)
ql.Settings.instance().evaluationDate = bbg_date
curve_date = bbg_date.to_date()
curve_date_str = curve_date.strftime('%Y-%m-%d')
ctpy_valuation_date = curve_date.strftime('%Y%m%d')
db_paths = [r"\\igcp-fs\NCF-GER\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\BLOOMBERG_DATABASE\swaps.db",
            r"\\igcp-fs\NCF-GER\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\DASHBOARD_DIARIO\DB\DashboardDiario_database.db"]
data_dict = fetch_data(queries, db_paths)
con_swaps = sqlite3.connect(db_paths[0])
con_dashboard = sqlite3.connect(db_paths[1])
df_3month = data_dict.get("query_3month")[0]
df_ois = data_dict.get("query_ois")[0]
df_fixings_estr = data_dict.get("query_estr_fixings")[0]
df_fixings_euri = data_dict.get("query_euribor_3m_fixings")[0]
zero_rate_ois = data_dict.get("query_historical_zero_rates_ois")[0]
zero_rate_3m = data_dict.get("query_historical_zero_rates_3m")[0]
zero_rate_ois["reference_date"] = pd.to_datetime(zero_rate_ois["reference_date"])
zero_rate_3m["reference_date"] = pd.to_datetime(zero_rate_3m["reference_date"])
zero_rate_ois = zero_rate_ois.sort_values("reference_date")
zero_rate_3m  = zero_rate_3m.sort_values("reference_date")
excel_dir_emails = r"\\igcp-fs\ncf-ger\Colateral 030-25-04\emails contrapartes derivados"
excel_dir_wss = excel_dir = "\\\\igcp.pt\\wss\\Import_Export\\Export\\PRD\\Dashboard_Export\\"
file_paths = {
    "MS": generate_file_path(excel_dir_emails, f"MS_Valuation_{ctpy_valuation_date}"),
    "CALY": generate_file_path(excel_dir_emails, f"CALY_Valuation_{ctpy_valuation_date}"),
    "BNPA": generate_file_path(excel_dir_emails, f"BNP_Valuation_{ctpy_valuation_date}"),
    }
file_patterns = [
        "NCF-DERIVADOS"
        ]
final_df=process_der_valuation_files(file_paths)
loaded_excels = process_files_for_patterns(reference_date, excel_dir_wss, file_patterns)
excel_derivados_data = loaded_excels[0]
df_derivados = excel_derivados_data.parse(sheet_name="derivatives", skiprows=1)
ois_curve, discount_handle, tenors_ois, quotes_ois = build_estr_curve(curve_date_str,df_ois)
euribor_curve, tenors_euribor, quotes_euribor = build_euribor_curve(curve_date_str,df_3month, df_ois)
sensibilidades = swaps_krr_data(bbg_date,reference_date,trade_params_df,df_ois,df_3month,df_fixings_estr,df_fixings_euri,ois_curve, discount_handle, tenors_ois, quotes_ois,euribor_curve, tenors_euribor, quotes_euribor, shift = 0.0001, payments=3)
VaR_parametrico_multi_fator = multi_factor_parametric_var(reference_date_str,curve_date_str, zero_rate_ois, zero_rate_3m, sensibilidades, horizon = "10D",alpha = 5/100,n_obs=3*30, choose_data = True)
atualizar_bd_monitorizacao_derivados(df_derivados, final_df, VaR_parametrico_multi_fator, reference_date_str_2, excel_serial_to_date,trade_params_df,con_dashboard,salvar = False )
atualizar_avaliacao_der_bd(final_df, df_derivados,reference_date_str_2 ,con_dashboard,salvar = False)
display("tabela bd mon_limites_derivados:", atualizar_bd_monitorizacao_derivados(df_derivados, final_df, VaR_parametrico_multi_fator, reference_date_str_2, excel_serial_to_date,trade_params_df,con_dashboard,salvar = False ))
display("tabela bd avaliacao_derivados:", atualizar_avaliacao_der_bd(final_df, df_derivados,reference_date_str_2 ,con_dashboard,salvar = False))


