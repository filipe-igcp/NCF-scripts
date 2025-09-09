import sys
sys.path.append(r"I:\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\NCF-Scripts\notebooks\src")
from var.avaliacao_swaps import *
from var.swap_trades import trade_params_df 
from var.queries import *
from colateral.processar_avaliacoes_repos_derivados import atualizar_bd_var_multifator
from dateutil.relativedelta import relativedelta
from IPython.display import display
warnings.simplefilter(action='ignore', category=Warning)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:,.4f}'.format)
reference_date = datetime.today()
reference_date_str = reference_date.strftime('%Y-%m-%d')
bbg_date= ql.TARGET().advance(ql.Date.todaysDate(), -1, ql.Days)
ql.Settings.instance().evaluationDate = bbg_date
curve_date = bbg_date.to_date()
curve_date_str = curve_date.strftime('%Y-%m-%d')
db_paths = [r"\\igcp-fs\NCF-GER\Projetos 010-01-01\1_DESENVOLVIMENTO_PROJECTOS_INTERNOS\BLOOMBERG_DATABASE\swaps.db"]
con = sqlite3.connect(db_paths[0])
data_dict = fetch_data(queries, db_paths)
con_swaps = sqlite3.connect(db_paths[0])
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
ois_curve, discount_handle, tenors_ois, quotes_ois = build_estr_curve(curve_date_str,df_ois)
euribor_curve, tenors_euribor, quotes_euribor = build_euribor_curve(curve_date_str,df_3month, df_ois)
sensibilidades = swaps_krr_data(bbg_date,reference_date_str,trade_params_df,df_ois,df_3month,df_fixings_estr,df_fixings_euri,ois_curve, discount_handle, tenors_ois, quotes_ois,euribor_curve, tenors_euribor, quotes_euribor, shift = 0.0001, payments=3)

# ### Parâmetros do modelo
# 
# - **horizon**  
#   Define o horizonte do VaR:  
#   - `1D` → 1 dia;
#   - `10D` → 10 dias;
#   - `1M` → 1 mês;
#   - `1Y` → 1 ano;
# 
# - **alpha**  
#   Nível de significância estatística.  
#   - Exemplo: `alpha = 0.05` corresponde a um intervalo de confiança de 95%.  
#   - Interpretação em VaR: probabilidade máxima de perdas além do valor calculado.  
# 
# - **n_obs**  
#   Número de observações históricas (em dias).  
#   - Define quantos dias de dados passados serão considerados para calcular o VaR.  
#   - Exemplo: `n_obs = 250` → aproximadamente 1 ano de curvas zero cupão.  
# 
# - **choose_data**  
#   Define a forma de usar os dados históricos:  
#   - `True` → Usar **janela móvel**: só os últimos `n_obs` dias entram no cálculo (atualiza a cada dia).  
#   - `False` → Usar **todo o histórico disponível**: considera todos os dados carregados.  
# 
# ---
# ### Ouput do modelo
# 
# 
# | Counterparty | Reference Date | VaR Multi-Factor |
# |--------------|----------------|-----------------|
# | ...          |     hoje       | 1,203,045.74    |
# | ...          |     hoje       | 150,664.25      |
# | ...          |     hoje       | 150,594.80      |
# 

VaR_paramétrico_multi_factor = multi_factor_parametric_var(reference_date_str,curve_date_str, zero_rate_ois, zero_rate_3m, sensibilidades, horizon = "10D",alpha = 5/100,n_obs=3*30, choose_data = True)
display("tabela VaR",atualizar_bd_var_multifator(VaR_paramétrico_multi_factor,con,salvar = False,horizon="10D",nome_tabela ="VaR"))
