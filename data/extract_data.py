import pandas as pd
import numpy as np
import json

wr = pd.read_csv('weekly_revenue_report.csv')
wr['Week'] = pd.to_datetime(wr['Week'])

# Quarterly
wr['Quarter'] = wr['Week'].dt.to_period('Q')
q = wr.groupby('Quarter').agg({
    'VA_Revenue': 'sum', 'PC_Revenue': 'sum',
    'Total_Revenue': 'sum', 'Cost': 'sum', 'Profit': 'sum'
}).reset_index()
q['Quarter'] = q['Quarter'].astype(str)

print("=== QUARTERLY ===")
for _, r in q.iterrows():
    print(f"{r.Quarter}: VA={r.VA_Revenue:.0f} PC={r.PC_Revenue:.0f} Total={r.Total_Revenue:.0f} Cost={r.Cost:.0f} Profit={r.Profit:.0f}")

# Monthly 2025+
wr['Month'] = wr['Week'].dt.to_period('M')
m = wr[wr['Week'] >= '2025-01-01'].groupby('Month').agg({
    'VA_Revenue': 'sum', 'PC_Revenue': 'sum',
    'Total_Revenue': 'sum', 'Cost': 'sum', 'Profit': 'sum'
}).reset_index()
m['Month'] = m['Month'].astype(str)

print("\n=== MONTHLY 2025+ ===")
for _, r in m.iterrows():
    print(f"{r.Month}: VA={r.VA_Revenue:.0f} PC={r.PC_Revenue:.0f} Total={r.Total_Revenue:.0f} Cost={r.Cost:.0f} Profit={r.Profit:.0f}")

# Run rates
recent13 = wr.tail(13)
recent26 = wr.tail(26)
print(f"\n=== RUN RATES ===")
print(f"Last 13wk avg weekly total: {recent13.Total_Revenue.mean():.0f}")
print(f"Last 13wk avg weekly VA: {recent13.VA_Revenue.mean():.0f}")
print(f"Last 13wk avg weekly PC: {recent13.PC_Revenue.mean():.0f}")
print(f"Last 13wk avg weekly profit: {recent13.Profit.mean():.0f}")
print(f"Last 13wk annualized total: {recent13.Total_Revenue.mean()*52:.0f}")
print(f"Last 13wk annualized profit: {recent13.Profit.mean()*52:.0f}")
print(f"Last 26wk avg weekly total: {recent26.Total_Revenue.mean():.0f}")
print(f"Last 26wk annualized total: {recent26.Total_Revenue.mean()*52:.0f}")

# Growth: compare H1 2025 vs H2 2025
h1 = wr[(wr['Week'] >= '2025-01-01') & (wr['Week'] < '2025-07-01')]
h2 = wr[(wr['Week'] >= '2025-07-01') & (wr['Week'] < '2026-01-01')]
print(f"\n=== HALF-YEAR COMPARISON ===")
print(f"H1 2025 avg weekly: {h1.Total_Revenue.mean():.0f}")
print(f"H2 2025 avg weekly: {h2.Total_Revenue.mean():.0f}")
print(f"Growth H1->H2: {((h2.Total_Revenue.mean() - h1.Total_Revenue.mean()) / h1.Total_Revenue.mean() * 100):.1f}%")

# Monthly charges data for chart
charges = pd.read_csv('Charges Export CSV.csv')

def clean_curr(x):
    if isinstance(x, str):
        x = x.replace('$', '').replace(',', '').strip()
        if '(' in x and ')' in x:
            x = '-' + x.replace('(', '').replace(')', '')
    return pd.to_numeric(x, errors='coerce')

charges['Date Of Service'] = pd.to_datetime(charges['Date Of Service'], errors='coerce')
charges['SCA'] = charges['Service Charge Amount'].apply(clean_curr)

monthly_charges = charges[charges['Date Of Service'] >= '2024-01-01'].groupby(
    pd.Grouper(key='Date Of Service', freq='MS')
)['SCA'].sum()

print("\n=== MONTHLY CHARGES (2024+) ===")
for dt, val in monthly_charges.items():
    print(f"{dt.strftime('%Y-%m')}: {val:.0f}")
