import pandas as pd
import seaborn as sns
import QuantLib as ql
import numpy as np
import sqlite3
from datetime import datetime,timedelta
from pandas.api.types import CategoricalDtype
import warnings
import glob
import time
from scipy.stats import norm

def excel_serial_to_date(serial):
    if pd.isna(serial):
        return pd.NaT

    excel_start_date = datetime(1900, 1, 1) - timedelta(days=2)  
    return (excel_start_date + timedelta(days=serial)).strftime('%Y-%m-%d')
    
def string_to_ql_date(date_str):
    year, month, day = map(int, date_str.split('-'))
    return ql.Date(day, month, year)

def build_estr_curve(today_str, df_ois, settlement_days = 2):
    df_ois_ = df_ois.copy()
    df_ois_= df_ois_.loc[(df_ois_["Reference_Date"] == today_str)&(df_ois_["Tenor"] != "O/N")]
    index = ql.Estr()
    helpers = []
    quotes = []
    tenors=[]
    calendar = ql.TARGET()
    day_count = ql.Actual360()
    for tenor, rate in zip(df_ois_['Tenor'], df_ois_['Mid_Yield']):
        rate = rate / 100.0  
        quote = ql.SimpleQuote(rate)
        quotes.append(quote)
        tenors.append(tenor)
        period = ql.Period(tenor)
        helpers.append(ql.OISRateHelper(settlement_days,  
                                  period,
                                  ql.QuoteHandle(quote),
                                  index,
                                  paymentFrequency=ql.Annual))
    curve = ql.PiecewiseLinearZero(0,calendar, helpers, day_count)
    curve.enableExtrapolation() 
    discount_handle = ql.YieldTermStructureHandle(curve)
    
    return curve, discount_handle, tenors, quotes

def build_euribor_curve(today_str, df_3month, df_ois, months=3, settlement_days=2):
    df_ois_ = df_ois.copy()
    df_ois_= df_ois_.loc[(df_ois_["Reference_Date"] == today_str)&(df_ois_["Tenor"] != "O/N")]
    df_3month_ = df_3month.copy()
    df_3month_= df_3month_.loc[df_3month_["Reference_Date"] == today_str]
    discount_handle = build_estr_curve(today_str,df_ois_)[1]
    index = ql.Euribor(ql.Period(months, ql.Months))
    calendar = ql.TARGET()
    convention = ql.ModifiedFollowing
    day_count_deposit = ql.Actual360()
    day_count_swap= ql.Thirty360(ql.Thirty360.BondBasis)
    zero_time = ql.Period(0, ql.Days)
    helpers = []
    tenors = []
    quotes = []

    for tenor, rate in zip(df_3month_["Tenor"], df_3month_["Mid_Yield"]):
        quote = ql.SimpleQuote(rate / 100)
        quotes.append(quote)
        tenors.append(tenor)

        if 'Y' not in tenor:
            if tenor == "3M":
                helpers.append(
                    ql.DepositRateHelper(ql.QuoteHandle(quote),ql.Period(tenor),settlement_days,
                        calendar,convention,False,day_count_deposit))
            else:
                helpers.append(
                    ql.FraRateHelper(ql.QuoteHandle(quote),int(tenor.split("M")[0]) - 3,int(tenor.split("M")[0]),
                        settlement_days,calendar,convention,False,day_count_deposit))
        else:
            helpers.append(
                ql.SwapRateHelper(ql.QuoteHandle(quote),ql.Period(tenor),calendar,ql.Annual,
                    ql.Unadjusted,day_count_swap,index,ql.QuoteHandle(),
                    zero_time,discount_handle))

    curve = ql.PiecewiseLinearZero(0, ql.TARGET(), helpers, day_count_deposit)
    curve.enableExtrapolation() 
    return curve, tenors, quotes
    
def apply_fixings(today, discount_handle, euribor_curve, settlement_date, df_fixings_estr, df_fixings_euri, euribor_months=3):
    
    calendar = ql.TARGET()
    estr = ql.Estr(discount_handle)
    
    fixings_estr_map = df_fixings_estr.set_index("Reference_Date")["Mid_Yield"].to_dict()
    
    settlement_date_ = settlement_date
    while settlement_date_ < today:
        date_str = settlement_date_.to_date().strftime('%Y-%m-%d')
        if date_str in fixings_estr_map:
            fixing = fixings_estr_map[date_str]
            estr.addFixing(settlement_date_, fixing / 100)
        settlement_date_ = calendar.advance(settlement_date_, 1, ql.Days)
    
    curve_handle = ql.YieldTermStructureHandle(euribor_curve)
    euribor = ql.Euribor(ql.Period(euribor_months, ql.Months), curve_handle)
    
    fixing_date = calendar.advance(settlement_date, -2, ql.Days)
    fixing_str = fixing_date.to_date().strftime('%Y-%m-%d')
    
    fixings_euri_map = df_fixings_euri.set_index("Reference_Date")["Mid_Yield"].to_dict()
    if fixing_str in fixings_euri_map:
        fixing_rate = fixings_euri_map[fixing_str] / 100
        euribor.addFixing(fixing_date, fixing_rate)
    
    return estr, euribor

def build_swap(estr, euribor, discount_handle, settlement_date, notional, basis_spread, expiry, payments=3):
    period = ql.Period(expiry, ql.Years)
    maturity = settlement_date + period

    schedule_estr = ql.Schedule(settlement_date, maturity, ql.Period(payments, ql.Months),
                            ql.TARGET(), ql.ModifiedFollowing, ql.ModifiedFollowing,
                            ql.DateGeneration.Forward, False)

    schedule_3m = ql.Schedule(settlement_date, maturity, euribor.tenor(),
                              ql.TARGET(), ql.ModifiedFollowing, ql.ModifiedFollowing,
                              ql.DateGeneration.Forward, False)

    euribor_leg = ql.IborLeg([notional], schedule_3m, euribor)
    estr_leg = ql.OvernightLeg([notional], schedule_estr, estr, spreads=[basis_spread])

    swap = ql.Swap(estr_leg, euribor_leg)
    engine = ql.DiscountingSwapEngine(discount_handle)
    swap.setPricingEngine(engine)

    return swap

    
def calculate_key_rate_risk_estr(tenors_ois, quotes_ois, df_ois, settlement_date,estr, euribor, discount_handle, basis_spread=0.00085,notional=125_000_000, expiry=15, shift=0.0001, payments=3):
    results = []
    original_values = [q.value() for q in quotes_ois]

    swap = build_swap(estr, euribor, discount_handle, settlement_date, notional, basis_spread, expiry, payments=3)
 
    base_total = swap.NPV()

    for i, q in enumerate(quotes_ois):
        q.setValue(original_values[i] + shift)
        bumped_total = swap.NPV()
        q.setValue(original_values[i])

        results.append({
            "tenor": tenors_ois[i],
            "pay": bumped_total - base_total
        })

    return pd.DataFrame(results)

def calculate_key_rate_risk_euribor(
    tenors_euribor, quotes_euribor, df_ois, settlement_date,
    estr, euribor, discount_handle,
    basis_spread=0.00085, notional=125_000_000,
    expiry=15, shift=0.0001, payments=3
):
    results = []
    
    original_values = [q.value() for q in quotes_euribor]
    
    swap = build_swap(estr, euribor, discount_handle, settlement_date, notional, basis_spread, expiry, payments=3)
    
    base_total = swap.NPV()

    for i, q in enumerate(quotes_euribor):
        q.setValue(original_values[i] + shift)
        bumped_total = swap.NPV()
        q.setValue(original_values[i])  

        results.append({
            "tenor": tenors_euribor[i],
            "receive": bumped_total - base_total
        })

    return pd.DataFrame(results)

def process_dataframe_swap_mv(today_str,total_npv, leg0_npv, leg1_npv, counterparty, notional):
    
    data = [
    {
        "counterparty": counterparty,
        "nominal": notional,
        "reference_date": today_str,
        "IGCP_swap_mv": total_npv,
        "IGCP_leg0_mv": leg0_npv,
        "IGCP_leg1_mv": leg1_npv
    }]
    
    return pd.DataFrame(data)

def process_dataframe_krr(reference_date, counterparty, transaction_number,value_date, expiry_date,curve_pay,curve_receive, krr_estr,krr_estr_euribor):
    df = pd.merge(krr_estr,krr_estr_euribor, on = "tenor", how = "inner" )
    df["net"] = df["pay"] + df["receive"]
    df["counterparty"] = counterparty
    df["trans_number"] = transaction_number
    df["reference_date"] = reference_date
    df["value_date"] = value_date
    df["expiry_date"] = expiry_date
    df["curve_pay"] = curve_pay
    df["curve_receive"] = curve_receive
    df = df[["counterparty","trans_number","reference_date","value_date","expiry_date","tenor","pay","receive","net","curve_pay", "curve_receive"]]
    return df 


def swaps_krr_data(today,reference_date,trade_params_df,df_ois,df_3month,df_fixings_estr,df_fixings_euri,ois_curve, discount_handle, tenors_ois, quotes_ois,euribor_curve, tenors_euribor, quotes_euribor, shift = 0.0001, payments=3):
    dfs = []
    
    for row in trade_params_df.itertuples(index=False):
        settlement_date = string_to_ql_date(row.settlement_date)
        estr, euribor= apply_fixings(today,discount_handle,euribor_curve,settlement_date,df_fixings_estr, df_fixings_euri)
        krr_estr = calculate_key_rate_risk_estr(tenors_ois, quotes_ois, df_ois,settlement_date, estr, euribor,                     discount_handle,row.basis_spread, row.notional,row.period, shift = shift)
        krr_euribor = calculate_key_rate_risk_euribor(tenors_euribor, quotes_euribor, df_ois,settlement_date, estr, euribor, discount_handle,row.basis_spread, row.notional,row.period,shift = shift)
        df =  process_dataframe_krr(reference_date, row.counterparty, row.transaction_number,row.settlement_date, row.maturity_date, row.curve_pay, row.curve_receive, krr_estr,krr_euribor )
        dfs.append(df)
    
    krr = pd.concat(dfs)
    agg_krr = pd.concat(dfs).groupby(["counterparty"]).sum().reset_index()
    return krr, agg_krr

def mv_data(today, today_str, trade_params_df, df_ois, df_3month, df_fixings_estr, df_fixings_euri, ois_curve, discount_handle, euribor_curve, payments=3):
    dfs_mv = []
    for row in trade_params_df.itertuples(index=False):
        settlement_date = string_to_ql_date(row.settlement_date)
        estr, euribor= apply_fixings(today,discount_handle,euribor_curve,settlement_date, df_fixings_estr, df_fixings_euri)
        swap = build_swap(estr, euribor, discount_handle, settlement_date, row.notional, row.basis_spread, row.period, payments=payments)   
        total_npv = swap.NPV()
        leg0_npv = swap.legNPV(0)
        leg1_npv = swap.legNPV(1)
        df = process_dataframe_swap_mv(today_str,total_npv, leg0_npv, leg1_npv, row.counterparty, row.notional)
        dfs_mv.append(df)
        
    mv = pd.concat(dfs_mv).groupby("counterparty", as_index=False).agg({"nominal":"first","reference_date": "first",       
                                                                                            "IGCP_swap_mv": "sum","IGCP_leg0_mv": "sum",
                                                                                            "IGCP_leg1_mv": "sum"})
    
    return mv


def compute_var_es(trade_params_df, historical_spreads, data, today_str,
                   z_score=1.645, past_days=6*30, choose_data=False):
    hs = historical_spreads.copy()
    hs = hs.loc[hs["reference_date"] <= f"{today_str} 00:00:00"]
    if choose_data:
        hs = hs.iloc[-past_days:]
        
    _1d_changes = hs["spread"].diff(1).dropna()
    _10d_changes = hs["spread"].diff(10).dropna()
    _1m_changes = hs["spread"].diff(21).dropna()
    _1y_changes = hs["spread"].diff(252).dropna()
    
    vols = {
        "1D": _1d_changes.std(),
        "10D": _10d_changes.std(),
        "1M": _1m_changes.std(),
        "1Y": _1y_changes.std()
    }

    results = []

    for _, row in trade_params_df.iterrows():
        
        exposure = data[0].loc[
            (data[0]["counterparty"] == row["counterparty"]) &
            (data[0]["reference_date"] == today_str) &
            (data[0]["trans_number"] == row["transaction_number"]),
            "pay"
        ].sum()
        
        for horizon, vol in vols.items():
            var = vol * z_score * exposure
            es = var * es_multiplier
            results.append({
                "counterparty": row["counterparty"],
                "horizon": horizon,
                "Parametric_VaR": var  
            })

    results_df = pd.DataFrame(results)
    results_df["reference_date"] = today_str

    results_df_grouped = (
        results_df.groupby(["counterparty", "horizon"])
        .agg({
            "transaction_number": "first",
            "nominal": "sum",
            "Parametric_VaR": "sum",
            "expected_shortfall": "sum",
            "volatility": "first"
        })
        .reset_index()
    )

    return results_df, vols["10D"], results_df_grouped

def backtesting_var(today,py_date, trade_params_df, df_fixings_estr, df_fixings_euri, df_3month, df_ois, historical_spreads, obs_npv=1000,obs_numeric_var = 6*21,percentile = 0.05, horizon = "10D"):
    horizon_dict = {"1D":1,
                    "10D":10,
                    "1M": 21,
                    "1Y": 252}
    historical_npv_diff = [] 
    parametric_var = []
    date = py_date - timedelta(obs_npv) 
    while date <= py_date:
        date_str = date.strftime("%Y-%m-%d")
        if date.weekday() < 5 and not df_ois[df_ois['Reference_Date'] == date_str].empty and not df_3month[df_3month['Reference_Date'] == date_str].empty:
            print(f"Evaluating: {date}")
            ql_eval_date = ql.Date(date.day, date.month, date.year)
            ql.Settings.instance().evaluationDate = ql_eval_date
            ois_curve, discount_handle, tenors_ois, quotes_ois = build_estr_curve(date_str,df_ois)
            euribor_curve, tenors_euribor, quotes_euribor = build_euribor_curve(date_str,df_3month, df_ois)
            try:
                historical_npv_diff.append(mv_data(
                        today=today,
                        today_str=date_str,
                        trade_params_df=trade_params_df,
                        df_ois=df_ois,
                        df_3month=df_3month,
                        df_fixings_estr=df_fixings_estr,
                        df_fixings_euri=df_fixings_euri,
                        ois_curve=ois_curve,
                        discount_handle=discount_handle,
                        euribor_curve=euribor_curve,
                        payments=3
                    ))
                data = swaps_krr_data(
                    today, date_str, trade_params_df, df_ois, df_3month, df_fixings_estr,
                    df_fixings_euri, ois_curve, discount_handle, tenors_ois, quotes_ois,
                    euribor_curve, tenors_euribor, quotes_euribor, shift=0.0001, payments=3
                )
                parametric_var.append(compute_var_es(trade_params_df,historical_spreads, data, date_str, z_score=1.645, past_days =obs_numeric_var,choose_data = True)[0])
                
            except Exception as e:
                print(f" Error on {date_str}: {e}")

           
            
        date += timedelta(days=1)
    results_numeric_var = pd.concat(historical_npv_diff) 
    results_parametric_var = pd.concat(parametric_var) 
    
    results_numeric_var= results_numeric_var.groupby(["counterparty","reference_date"]).max()
    results_numeric_var["day_diff"] = results_numeric_var.groupby(level=0)["IGCP_swap_mv"].diff(horizon_dict[horizon])
    results_numeric_var["empirical_data"] = results_numeric_var.groupby(level="counterparty").apply(get_empirical_data, obs=obs_numeric_var).reset_index(level=0, drop=True)
    results_numeric_var["numerical_var"] = results_numeric_var["empirical_data"].apply(
    lambda x: np.quantile(x, percentile) if x is not pd.NA else pd.NA)
    results_numeric_var["Comparison_values"] = results_numeric_var.groupby(level=0)["day_diff"].shift(-horizon_dict[horizon])
    results_parametric_var=results_parametric_var.loc[results_parametric_var["horizon"] == horizon].groupby(["counterparty","reference_date"]).sum()
    merged = pd.merge(
    results_numeric_var, results_parametric_var, 
    left_index=True, 
    right_index=True, 
    how='outer',  
    suffixes=('_df1', '_df2')  
)
        
    return merged

def get_empirical_data(group, obs = 2, horizon="10D"):
    results = []
    for i in range(len(group)):
        last_values = group.iloc[max(0, i-obs):i][f"day_diff_{horizon}"].tolist()
        if len(last_values) < obs or any(pd.isna(v) for v in last_values):
            results.append(pd.NA)
        else:
            results.append(last_values)
    return pd.Series(results, index=group.index)

def historical_npv(today,py_date, trade_params_df, df_fixings_estr, df_fixings_euri, df_3month, df_ois, obs_npv=1000):
    historical_npv_diff = []
    date = py_date - pd.tseries.offsets.BDay(obs_npv)
    while date <= py_date:
        date_str = date.strftime("%Y-%m-%d")
        if date.weekday() < 5 and not df_ois[df_ois['Reference_Date'] == date_str].empty and not df_3month[df_3month['Reference_Date'] == date_str].empty:
            print(f"Evaluating: {date}")
            ql_eval_date = ql.Date(date.day, date.month, date.year)
            ql.Settings.instance().evaluationDate = ql_eval_date
            ois_curve, discount_handle, tenors_ois, quotes_ois = build_estr_curve(date_str,df_ois)
            euribor_curve, tenors_euribor, quotes_euribor = build_euribor_curve(date_str,df_3month, df_ois)
            try:
                historical_npv_diff.append(mv_data(
                        today=today,
                        today_str=date_str,
                        trade_params_df=trade_params_df,
                        df_ois=df_ois,
                        df_3month=df_3month,
                        df_fixings_estr=df_fixings_estr,
                        df_fixings_euri=df_fixings_euri,
                        ois_curve=ois_curve,
                        discount_handle=discount_handle,
                        euribor_curve=euribor_curve,
                        payments=3
                    ))
            except Exception as e:
                print(f" Error on {date_str}: {e}")
        date += timedelta(days=1)
    df = pd.concat(historical_npv_diff)
    return df

def historical_sim_VaR(today,py_date, trade_params_df, df_fixings_estr, df_fixings_euri, df_3month, df_ois, horizon = "10D",obs_numeric_var=6*21,obs_npv = 100, percentile = 0.05):
    horizon_dict = {"1D":1,
                    "10D":10,
                    "1M": 21,
                    "1Y": 252}
    df = historical_npv(today,py_date, trade_params_df, df_fixings_estr, df_fixings_euri, df_3month, df_ois, obs_npv)
    df= df.groupby(["counterparty","reference_date"]).max()
    df[f"day_diff_{horizon}"] = df.groupby(level=0)["IGCP_swap_mv"].diff(horizon_dict[horizon])
    df[f"empirical_data_{horizon}"] = df.groupby(level="counterparty").apply(get_empirical_data, obs=obs_numeric_var, horizon = horizon).reset_index(level=0, drop=True)
    df[f"numerical_var_{horizon}"] = df[f"empirical_data_{horizon}"].apply(
    lambda x: np.quantile(x, percentile) if x is not pd.NA else pd.NA)
    
    return df
        
def extract_zero_rates(curve, tenors,today,today_str):
    zero_rates = []
    for tenor in tenors:
        period = ql.Period(tenor)
        maturity_date = today + period
        zero_rate = curve.zeroRate(maturity_date, ql.Actual360(), ql.Continuous)
        zero_rates.append(zero_rate.rate())
    df_zero = pd.DataFrame({
    'reference_date':today_str,
    'tenor': tenors,
    'zero_rate': zero_rates
    })
    return df_zero

def historical_zero_rates(py_date, df_ois,df_3month, obs_npv=1000):
    df_zero_rates_ois = []
    df_zero_rates_eur = []
    date = py_date - pd.tseries.offsets.BDay(obs_npv)
    while date <= py_date:
        date_str = date.strftime("%Y-%m-%d")
        if date.weekday() < 5 and not df_ois[df_ois['Reference_Date'] == date_str].empty and not df_3month[df_3month['Reference_Date'] == date_str].empty:
            ql_eval_date = ql.Date(date.day, date.month, date.year)
            print(f"Evaluating: {date} and {ql_eval_date}")
            ql.Settings.instance().evaluationDate = ql_eval_date
            ois_curve, discount_handle, tenors_ois, quotes_ois = build_estr_curve(date_str,df_ois)
            euribor_curve, tenors_euribor, quotes_euribor = build_euribor_curve(date_str,df_3month, df_ois)
            zero_rates_ois = extract_zero_rates(ois_curve, tenors_ois,ql_eval_date,date_str)
            zero_rates_eur = extract_zero_rates(euribor_curve, tenors_euribor,ql_eval_date,date_str)
            df_zero_rates_eur.append(zero_rates_eur)
            df_zero_rates_ois.append(zero_rates_ois)
        date += timedelta(days=1)
    return pd.concat(df_zero_rates_ois), pd.concat(df_zero_rates_eur)

def multi_factor_parametric_var(reference_date, today_str, zero_rate_ois, zero_rate_3m, data, horizon = "10D",alpha = 5/100,n_obs=6*30, choose_data = False):
    results = {}
    horizon_dict = {"1D":1,
                    "10D":10,
                    "1M": 21,
                    "1Y": 252}
    zero_rate_ois_ = zero_rate_ois.copy() 
    zero_rate_3m_ = zero_rate_3m.copy()
    zero_rate_ois_.rename(columns={"zero_rate":"ESTR"}, inplace=True) 
    zero_rate_3m_.rename(columns={"zero_rate":"Euribor 3M"}, inplace=True) 
    df = pd.merge(zero_rate_ois_,zero_rate_3m_,on = ["reference_date","tenor"],how="inner") 
    df = df.pivot(index='reference_date', columns='tenor', values=['ESTR',"Euribor 3M"])
    df=df.loc[df.index <= today_str]
    past_days = pd.to_datetime(today_str) - timedelta(n_obs)
    past_days_str = past_days.strftime('%Y-%m-%d')
    print("VaR Paramétrico Multifator com dados históricos considerados até:", past_days_str)
    if choose_data:
        df=df.loc[(df.index >= past_days_str)]
       
    dac = df.diff(horizon_dict[horizon]).dropna()
    dac = dac *10000
    mean= dac.describe().iloc[1] 
    std = dac.describe().iloc[2] 
    submean_dac = (dac - dac.mean()) 
    standard_dac = (dac - dac.mean())/dac.std() 
    V = submean_dac.cov() 
    C = standard_dac.corr() 
    eur_curve = data[0].groupby(["counterparty","tenor"]).agg({"receive": "sum","curve_receive": "first"}).reset_index()
    ois_curve = data[0].groupby(["counterparty","tenor"]).agg({"pay":"sum","curve_pay": "first"}).reset_index()
    ois_curve["tenor"] = ois_curve["tenor"] + '.' + ois_curve['curve_pay']
    eur_curve["tenor"] = eur_curve["tenor"] + '.' + eur_curve['curve_receive']
    ois_curve=ois_curve.groupby(["counterparty", "tenor"]).agg({
    "pay": "first",
    "curve_pay": "first"
    })

    eur_curve=eur_curve.groupby(["counterparty", "tenor"]).agg({
        "receive": "first",
        "curve_receive": "first"
    })
    ois_curve=ois_curve.rename(columns = {"pay":"PV01"})
    ois_curve=ois_curve.drop(columns = {"curve_pay"})
    eur_curve=eur_curve.rename(columns = {"receive":"PV01"})
    eur_curve=eur_curve.drop(columns = {"curve_receive"})
    vect = pd.concat([ois_curve,eur_curve])
    for ctpy in data[0]["counterparty"].unique():
        vect.loc[ctpy]["PV01"]
        expected_PNL = np.matmul(mean.values,vect.loc[ctpy]["PV01"])
        std_PNL = np.sqrt(np.matmul(np.matmul(vect.loc[ctpy]["PV01"],V.values),vect.loc[ctpy]["PV01"]))
        Param_VaR = abs( expected_PNL + norm.ppf(alpha) * std_PNL)
        
        results[ctpy] = {
            "reference_date": reference_date,
            "VaR Multi-Factor": Param_VaR
        }

    results_df = pd.DataFrame.from_dict(results, orient="index")
    
    return results_df

