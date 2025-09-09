import sys
sys.path.append(r"I:\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\NCF-Scripts\notebooks\src")
from var.avaliacao_swaps import *
from var.queries import *
from var.swap_trades import trade_params_df 
import re
from scipy.stats import norm
from dateutil.relativedelta import relativedelta
from IPython.display import display
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:,.4f}'.format)
warnings.simplefilter(action='ignore', category=Warning)
bbg_date = ql.TARGET().advance(ql.Date.todaysDate(), -1, ql.Days)
ql.Settings.instance().evaluationDate = bbg_date
curve_date = bbg_date.to_date()
curve_date_str = curve_date.strftime('%Y-%m-%d')
db_paths = [r"\\igcp-fs\NCF-GER\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\BLOOMBERG_DATABASE\swaps.db"]
data_dict = fetch_data(queries, db_paths)
con_swaps = sqlite3.connect(db_paths[0])
df_3month = data_dict.get("query_3month")[0]
df_ois = data_dict.get("query_ois")[0]
df_fixings_estr = data_dict.get("query_estr_fixings")[0]
df_fixings_euri = data_dict.get("query_euribor_3m_fixings")[0]
ois_curve, discount_handle, tenors_ois, quotes_ois = build_estr_curve(curve_date_str,df_ois)
euribor_curve, tenors_euribor, quotes_euribor = build_euribor_curve(curve_date_str,df_3month, df_ois)
zero_ois = extract_zero_rates(ois_curve, tenors_ois,bbg_date,curve_date_str)
zero_3m = extract_zero_rates(euribor_curve, tenors_euribor,bbg_date,curve_date_str)
display("3 month zero rates:", zero_3m)
display("ois zero rates:",zero_ois)
zero_ois.to_sql("zero_rates_ois",con_swaps, if_exists='append', index=False)
zero_3m.to_sql("zero_rates_3m",con_swaps, if_exists='append', index=False)


