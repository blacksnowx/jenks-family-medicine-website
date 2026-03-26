"""
Weekly Revenue Report
Provides a week-by-week breakdown of VA, Primary Care, and Total revenue.

Run directly: python -m data.weekly_revenue_report
"""

import pandas as pd
from datetime import datetime
import os
import sys

# Allow running as a script or as a module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import data_loader

# ============================================
# 1. LOAD DATA (using shared data_loader)
# ============================================
print("Loading data...")

va_df = data_loader.load_va_data()
charges_df = data_loader.load_pc_data()

print(f"VA records: {len(va_df)}")
print(f"Primary Care records: {len(charges_df)}")

# ============================================
# 2. SUPPLEMENTAL VA EXAM REVENUE
# ============================================
# Additional VA exam payments not captured in 201 Bills
supplemental_va_data = [
    ('01/15/2026', 1365.00),
    ('01/02/2026', 1500.00),
    ('12/15/2025', 1855.00),
    ('12/01/2025', 390.00),
    ('11/17/2025', 195.00),
    ('11/03/2025', 645.00),
    ('10/15/2025', 1525.00),
    ('10/01/2025', 2060.00),
    ('09/15/2025', 420.00),
    ('09/02/2025', 2220.00),
    ('08/15/2025', 690.00),
    ('08/01/2025', 1650.00),
    ('07/15/2025', 1715.00),
    ('07/01/2025', 3100.00),
    ('06/16/2025', 2275.00),
    ('06/02/2025', 850.00),
    ('05/15/2025', 875.00),
    ('05/01/2025', 3150.00),
    ('04/15/2025', 1915.00),
    ('04/01/2025', 3460.00),
    ('03/17/2025', 1310.00),
    ('03/03/2025', 1285.00),
    ('02/18/2025', 700.00),
    ('02/03/2025', 1185.00),
    ('01/15/2025', 2265.00),
    ('01/02/2025', 420.00),
    ('12/02/2024', 455.00),
    ('11/01/2024', 2093.00),
    ('10/15/2024', 735.00),
    ('10/01/2024', 1410.00),
    ('09/16/2024', 1415.00),
    ('09/03/2024', 1480.00),
    ('08/15/2024', 1210.00),
    ('08/01/2024', 790.00),
    ('07/15/2024', 210.00),
    ('07/01/2024', 140.00),
    ('06/17/2024', 150.00),
    ('05/15/2024', 430.00),
    ('04/01/2024', 280.00),
    ('02/15/2024', 430.00),
    ('02/01/2024', 175.00),
    ('01/02/2024', 455.00),
    ('12/15/2023', 465.00),
    ('12/01/2023', 1095.00),
    ('11/15/2023', 500.00),
    ('11/01/2023', 530.00),
    ('10/16/2023', 330.00),
    ('10/02/2023', 1965.00),
    ('09/15/2023', 965.00),
    ('09/01/2023', 2930.00),
]

# ============================================
# 3. CREATE WEEKLY AGGREGATIONS
# ============================================

# VA weekly (already has 'Week' from data_loader)
va_weekly = va_df.groupby('Week').agg(VA_Revenue=('VA_Revenue', 'sum')).reset_index()

# Process supplemental VA data
supplemental_df = pd.DataFrame(supplemental_va_data, columns=['Date', 'Amount'])
supplemental_df['Date'] = pd.to_datetime(supplemental_df['Date'])
supplemental_df['Week'] = data_loader.get_week_ending_friday(supplemental_df['Date'])
supplemental_weekly = supplemental_df.groupby('Week')['Amount'].sum().reset_index()
supplemental_weekly.columns = ['Week', 'Supplemental_VA']
print(f"Supplemental VA records: {len(supplemental_df)}")
print(f"Total Supplemental VA: ${supplemental_weekly['Supplemental_VA'].sum():,.2f}")

# Merge supplemental VA into main VA weekly
va_weekly = pd.merge(va_weekly, supplemental_weekly, on='Week', how='outer').fillna(0)
va_weekly['VA_Revenue'] = va_weekly['VA_Revenue'] + va_weekly['Supplemental_VA']
va_weekly = va_weekly[['Week', 'VA_Revenue']]

# PC weekly (already has 'Week' from data_loader)
pc_weekly = charges_df.groupby('Week').agg(
    PC_Charges=('Service Charge Amount', 'sum'),
    PC_Revenue=('PC_Revenue', 'sum'),
).reset_index()

# Merge both datasets
weekly_report = pd.merge(va_weekly, pc_weekly, on='Week', how='outer')
weekly_report = weekly_report.fillna(0).sort_values('Week').reset_index(drop=True)

# For recent weeks, estimate PC Revenue as % of charges (collection lag)
COLLECTION_RATE = 0.33
NUM_RECENT_WEEKS = 10
if len(weekly_report) >= NUM_RECENT_WEEKS:
    weekly_report.loc[weekly_report.index[-NUM_RECENT_WEEKS:], 'PC_Revenue'] = (
        weekly_report.loc[weekly_report.index[-NUM_RECENT_WEEKS:], 'PC_Charges'] * COLLECTION_RATE
    )

weekly_report['Total_Revenue'] = weekly_report['VA_Revenue'] + weekly_report['PC_Revenue']

# Assign weekly costs by quarter
QUARTERLY_COSTS = {
    (2025, 1): 9569,
    (2025, 2): 11223,
    (2025, 3): 13752,
    (2025, 4): 13063,
    (2026, 1): 13063,
}
DEFAULT_COST = 9569

def get_quarterly_cost(week_date):
    year = week_date.year
    quarter = (week_date.month - 1) // 3 + 1
    return QUARTERLY_COSTS.get((year, quarter), DEFAULT_COST)

weekly_report['Cost'] = weekly_report['Week'].apply(get_quarterly_cost)
weekly_report['Profit'] = weekly_report['Total_Revenue'] - weekly_report['Cost']

# 8-week moving averages
weekly_report['VA_Revenue_8wk_MA'] = weekly_report['VA_Revenue'].rolling(window=8, min_periods=1).mean()
weekly_report['Total_Revenue_8wk_MA'] = weekly_report['Total_Revenue'].rolling(window=8, min_periods=1).mean()
weekly_report['Profit_8wk_MA'] = weekly_report['Profit'].rolling(window=8, min_periods=1).mean()

# ============================================
# 4. DISPLAY REPORT
# ============================================
print("\n" + "=" * 80)
print("WEEKLY REVENUE REPORT")
print("=" * 80)
print(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

display_df = weekly_report.copy()
display_df['Week'] = display_df['Week'].dt.strftime('%Y-%m-%d')
for col in ['VA_Revenue', 'PC_Charges', 'PC_Revenue', 'Total_Revenue', 'Cost', 'Profit',
            'VA_Revenue_8wk_MA', 'Total_Revenue_8wk_MA', 'Profit_8wk_MA']:
    display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
display_df.columns = ['Week Ending', 'VA Revenue', 'PC Charges', 'PC Revenue', 'Total Revenue',
                       'Cost', 'Profit', 'VA 8wk MA', 'Total 8wk MA', 'Profit 8wk MA']

pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
print(display_df.to_string(index=False))

# ============================================
# 5. SUMMARY
# ============================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

total_va = weekly_report['VA_Revenue'].sum()
total_pc = weekly_report['PC_Revenue'].sum()
total_all = weekly_report['Total_Revenue'].sum()

print(f"\nTotal VA Revenue:           ${total_va:,.2f}")
print(f"Total Primary Care Revenue: ${total_pc:,.2f}")
print(f"Total Combined Revenue:     ${total_all:,.2f}")
print(f"\nAverage Weekly VA Revenue:           ${weekly_report['VA_Revenue'].mean():,.2f}")
print(f"Average Weekly Primary Care Revenue: ${weekly_report['PC_Revenue'].mean():,.2f}")
print(f"Average Weekly Total Revenue:        ${weekly_report['Total_Revenue'].mean():,.2f}")
print(f"\nNumber of Weeks: {len(weekly_report)}")
print(f"Date Range: {weekly_report['Week'].min().strftime('%Y-%m-%d')} to {weekly_report['Week'].max().strftime('%Y-%m-%d')}")

# ============================================
# 6. SAVE TO CSV
# ============================================
output_file = os.path.join(data_loader.BASE_DIR, 'weekly_revenue_report.csv')
weekly_report.to_csv(output_file, index=False)
print(f"\nReport saved to: {output_file}")
