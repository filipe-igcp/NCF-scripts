#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Analise_leiloes import *
from pprint import pprint


# # Inputs

# In[2]:


auction_date = '2025-07-16 10:30:00'
announcement_date = '2025-07-11 16:00:00'
spread_fixing = None
books_closing = None
pricing = None
auction_type = 'BT'
auction_name = 'Emissão BT 17 JUL 2026'
maturity = '20260717'
data_dict = fetch_data(QUERIES, db_paths, today_date) 
df_portugal = data_dict.get("portugal")[0]
df_spain = data_dict.get("spain")[0]
df_italy =data_dict.get("italy")[0]
df_germany = data_dict.get("germany")[0]
calculation_dates = calculate_auction_dates(df_italy, df_spain, df_portugal,df_germany, auction_date,announcement_date,spread_fixing, books_closing, pricing,auction_type, maturity )
tickers = {k: calculation_dates[k] for k in  [
    'PT_principal', 'SP_principal', 'IT_principal', 'GE_principal',
    'PT_esq', 'SP_esq', 'IT_esq', 'GE_esq',
    'PT_dir', 'SP_dir', 'IT_dir', 'GE_dir'
]}


# # Ver Tickers Datas a Utilizar

# In[3]:


print("\n Auction Information:")
pprint(calculation_dates, sort_dicts=False)


# # Analisar tickers a usar

# In[4]:


#miss=Missing_data(auction_name,tickers,calculation_dates,db_paths)


# In[5]:


#iss


# In[6]:


db = data_transformation(auction_name,tickers,calculation_dates,db_paths,check_dates=False)


# In[7]:


db[["PT_principal","PT_esq","PT_dir",
    "SP_principal","SP_esq","PT_dir",
    "IT_principal","IT_esq","IT_dir",
   "GE_principal","GE_esq","GE_dir"]].mean().plot(kind='bar', color='skyblue', edgecolor='black')
print("\n Yield Mean:")
print(db[["PT_principal","PT_esq","PT_dir",
    "SP_principal","SP_esq","PT_dir",
    "IT_principal","IT_esq","IT_dir",
   "GE_principal","GE_esq","GE_dir"]].mean().to_string())


# In[8]:


spread = spreads(db,auction_name)
spread_data= process_spread_data(db,spread,auction_name, calculation_dates, df_portugal, tickers)
filtered_yields=process_yield_and_spread_by_date(db, spread, auction_name,calculation_dates)
variations=process_variations(db,spread,auction_name,calculation_dates)
histogram = process_histogram(data_dict, variations,calculation_dates, window = 100, num_bins=17,rolling_mean = True)
statistics = process_stats(data_dict, variations, calculation_dates,window = 100, rolling_mean = True)


# # Visualizar Spreads e Yields

# In[9]:


plot_data(spread, spread_data, spread_str="SP-IT", instrumento="pri",tipo = "spread" )


# In[10]:


plot_data(spread, spread_data, spread_str="SP-IT", instrumento="esq",tipo = "spread" )


# In[11]:


plot_data(spread, spread_data, spread_str="SP-IT", instrumento="dir",tipo = "spread" )


# # Visualizar Histograma

# In[12]:


plot_histograma(auction_name,histogram,statistics,country = "IT")


# In[13]:


plot_histograma(auction_name,histogram,statistics,country = "SP")


# ### Big variations

# In[99]:


spread_ = spread.copy()


# In[100]:


spread.columns


# In[101]:


spread_=spread_.set_index("DATA_HORA")


# In[102]:


spread_ = spread_[["Spread_SP_pri","Spread_IT_pri","Spread_GE_pri","Spread_SP_esq","Spread_IT_esq","Spread_GE_esq","Spread_SP_dir","Spread_IT_dir","Spread_GE_dir"]]


# In[103]:


spread_ = spread_.diff(-30)


# In[104]:


spread_ = spread_.iloc[:-1]


# In[105]:


spread_


# In[106]:


top_n = 5 

for col in spread_.columns:
    print(f"\n Top {top_n} absolute changes for: {col}")
    top_changes = spread_[col].abs().nlargest(top_n)
    
    print(top_changes)


# ### Bar plot of max absolute diff in Spreads

# In[107]:


import pandas as pd
import matplotlib.pyplot as plt

spread_.index = pd.to_datetime(spread_.index)
daily_sum = spread_.abs().groupby(spread_.index.date).max()
daily_sum.T.plot(kind='bar', stacked=True, figsize=(14, 7))

plt.title("Daily Total Absolute Variation per Ticker (stacked by day)")
plt.ylabel("Total Abs Variation")
plt.xlabel("Ticker")
plt.legend(title="Date", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()


# ### Heat Map of max absolute diff in Spreads

# In[109]:


import seaborn as sns
import matplotlib.pyplot as plt


spread_.index = pd.to_datetime(spread_.index)
daily_diff = spread_.abs().groupby(spread_.index.date).max()  
daily_diff = daily_diff.T  
plt.figure(figsize=(14, 6))
sns.heatmap(daily_diff, cmap="Reds", cbar_kws={'label': 'Daily Max Abs Change'})
plt.title("Daily Max Absolute Spread Change per Ticker")
plt.xlabel("Date")
plt.ylabel("Ticker")
plt.tight_layout()
plt.show()


# In[ ]:





# In[ ]:





# In[ ]:




