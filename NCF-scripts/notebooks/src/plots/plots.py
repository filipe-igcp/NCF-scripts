from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter

def plot_var_vs_npv(df, date_col='reference_date', pnl_col='monthly_diff_mv',
                    var_cols=['Parametric_VaR', 'Another_VaR'],
                    title='Daily VaR vs Actual P&L', figsize=(15, 8)):

    df_clean = df.copy()
    df_clean[date_col] = pd.to_datetime(df_clean[date_col])

    
    for var in var_cols:
        df_clean[f'breach_{var}'] = df_clean[pnl_col] < df_clean[var]

  
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_title(title)


    ax.bar(df_clean[date_col], df_clean[pnl_col], color='blue', alpha=0.3, label='Actual P&L')

    line_colors = ['black', 'green', 'purple', 'orange']
    breach_colors = ['red', 'darkorange', 'magenta', 'cyan']

    for i, var in enumerate(var_cols):
        ax.plot(df_clean[date_col], df_clean[var],
                color=line_colors[i % len(line_colors)],
                label=f'{var} Threshold', linewidth=2)
        ax.scatter(
            df_clean[date_col][df_clean[f'breach_{var}']],
            df_clean[var][df_clean[f'breach_{var}']],
            color=breach_colors[i % len(breach_colors)],
            label=f'Breach {var}', zorder=5
        )

    ax.set_xlabel('Date')
    ax.set_ylabel('Value')
    ax.grid(True, alpha=0.3)

    
    total_points = len(df_clean)
    step = max(1, total_points // 10)
    ax.set_xticks(df_clean[date_col][::step])
    ax.set_xticklabels(df_clean[date_col].dt.strftime('%Y-%m-%d')[::step], rotation=45)

    ax.legend()
    plt.tight_layout()
    plt.show()

    
    total_days = len(df_clean)
    avg_pnl = df_clean[pnl_col].mean()
    max_loss = df_clean[pnl_col].min()
    max_gain = df_clean[pnl_col].max()

    
    print("\n" + "="*50)
    print("GLOBAL PERFORMANCE SUMMARY")
    print("="*50)
    print(f"Total Observations: {total_days}")
    print(f"Average PnL: {avg_pnl:,.2f}")
    print(f"Max Loss: {max_loss:,.2f}")
    print(f"Max Gain: {max_gain:,.2f}")

    
    for var in var_cols:
        breaches = df_clean[f'breach_{var}'].sum()
        safe_days = total_days - breaches
        breach_rate = (breaches / total_days * 100) if total_days > 0 else 0
        var_avg = df_clean[var].mean()

        print("\n" + "="*50)
        print(f"{var.upper()} PERFORMANCE SUMMARY")
        print("="*50)
        print(f"Total Observations: {total_days}")
        print(f"{var} Breaches: {breaches}")
        print(f"{var} Safe Days: {safe_days}")
        print(f"{var} Breach Rate Percent: {breach_rate:.2f}%")
        print(f"{var} Average: {var_avg:,.2f}")
        