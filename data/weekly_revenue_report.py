"""
Weekly Revenue Report
Provides a week-by-week breakdown of:
- VA (201 Bills and Payments) revenue
- Primary Care revenue  
- Total revenue
"""

import pandas as pd
from datetime import datetime

# ============================================
# 1. LOAD DATA
# ============================================
print("Loading data...")

# Load VA/DBQ data (201 Bills and Payments)
va_df = pd.read_csv('201 Bills and Payments.csv')

# Load Primary Care charges
charges_df = pd.read_csv('Charges Export CSV.csv')

print(f"VA records: {len(va_df)}")
print(f"Primary Care records: {len(charges_df)}")

# ============================================
# 2. CLEAN VA DATA
# ============================================
va_df = va_df.rename(columns={'Sarah Suggs ': 'Provider'})
va_df['Date of Service'] = pd.to_datetime(va_df['Date of Service'], errors='coerce')
va_df = va_df.dropna(subset=['Date of Service'])

# Fee structure for VA DBQ work
fees = {
    'GenMed': {'0': 0, '1-5': 180, '6-10': 360, '11-15': 520, '16+': 700},
    'Focused': {'0': 0, '1-5': 200, '6-10': 280, '11-15': 370, '16+': 480},
    'TBI': 250,
    'R-TBI': 250,
    'IMO': {'0': 0, '1-3': 150, '4-6': 260, '7-9': 330, '10-12': 350, '13-15': 465, '16-18': 525, '19+': 600},
    'No_Show': {'0': 0, '1': 60}
}

def fee_calc(row):
    """Calculate total fee for a VA DBQ encounter"""
    total = 0
    
    # Focused DBQs
    focused = str(row.get('Focused DBQs', '0')).strip()
    if focused.lower() in ['lab', 'visual', 'visual screen', 'bp check', '']:
        focused = '0'
    for key, val in fees['Focused'].items():
        if focused == key:
            total += val
            break
    
    # IMO (Routine IMOs)
    imo = str(row.get('Routine IMOs', '0')).strip()
    if imo in ['', '0', 'nan']:
        imo = '0'
    for key, val in fees['IMO'].items():
        if imo == key:
            total += val
            break
    
    # TBI
    tbi = str(row.get('TBI', '0')).strip().lower()
    if tbi in ['1', 'r-tbi', 'tbi']:
        total += fees['TBI']
    
    # Gen Med DBQs
    genmed = str(row.get('Gen Med DBQs', '0')).strip()
    if genmed.lower() in ['bp check', '']:
        genmed = '0'
    for key, val in fees['GenMed'].items():
        if genmed == key:
            total += val
            break
    
    # No Show
    no_show = str(row.get('No Show', '0')).strip()
    if no_show == '1':
        total += 60
    
    return total

va_df['VA_Revenue'] = va_df.apply(fee_calc, axis=1)

# ============================================
# 2B. SUPPLEMENTAL VA EXAM REVENUE
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

# Store supplemental data - will be processed after get_week_ending_friday is defined
# (Processing moved to section 4 with weekly aggregation)

# ============================================
# 3. CLEAN PRIMARY CARE DATA
# ============================================
charges_df['Date Of Service'] = pd.to_datetime(charges_df['Date Of Service'], errors='coerce')

def clean_currency(x):
    """Clean currency strings to float"""
    if pd.isna(x):
        return 0
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x).replace('$', '').replace(',', '').strip())
    except:
        return 0

# Calculate allowed amount (what we actually get paid)
charges_df['Service Charge Amount'] = charges_df['Service Charge Amount'].apply(clean_currency)
charges_df['Pri Ins Insurance Contract Adjustment'] = charges_df['Pri Ins Insurance Contract Adjustment'].apply(clean_currency)
charges_df['Sec Ins Insurance Contract Adjustment'] = charges_df['Sec Ins Insurance Contract Adjustment'].apply(clean_currency)
charges_df['Pri Ins Insurance Payment'] = charges_df['Pri Ins Insurance Payment'].apply(clean_currency)
charges_df['Sec Ins Insurance Payment'] = charges_df['Sec Ins Insurance Payment'].apply(clean_currency)
charges_df['Other Ins Insurance Payment'] = charges_df['Other Ins Insurance Payment'].apply(clean_currency)
charges_df['Pat Payment Amount'] = charges_df['Pat Payment Amount'].apply(clean_currency)
charges_df['Other Adjustment'] = charges_df['Other Adjustment'].apply(clean_currency)

# Calculate actual revenue collected (Primary + Secondary + Tertiary + Patient payments)
# Note: Other Adjustment can be positive (additional) or negative (write-off)
charges_df['PC_Revenue'] = (
    charges_df['Pri Ins Insurance Payment'] + 
    charges_df['Sec Ins Insurance Payment'] + 
    charges_df['Other Ins Insurance Payment'] +
    charges_df['Pat Payment Amount']
)

# Note: All charges from the Charges Report are counted as Primary Care revenue
# VA revenue comes only from the 201 Bills and Payments file

# ============================================
# 4. CREATE WEEKLY AGGREGATIONS
# ============================================

# Add week columns (week ending Friday)
# Calculate days until Friday: Friday = 4, so add (4 - dayofweek) % 7, but if already Fri, keep it
def get_week_ending_friday(date_series):
    """Calculate the Friday that ends the week for each date"""
    dow = date_series.dt.dayofweek  # Monday=0, Sunday=6, Friday=4
    # Days to add to get to Friday
    days_to_friday = (4 - dow) % 7
    # If it's Saturday (5) or Sunday (6), we need next Friday
    days_to_friday = days_to_friday.where(dow <= 4, (4 - dow + 7))
    return (date_series + pd.to_timedelta(days_to_friday, unit='D')).dt.normalize()

va_df['Week'] = get_week_ending_friday(va_df['Date of Service'])
charges_df['Week'] = get_week_ending_friday(charges_df['Date Of Service'])

# Aggregate VA revenue by week
va_weekly = va_df.groupby('Week').agg({
    'VA_Revenue': 'sum'
}).reset_index()
va_weekly.columns = ['Week', 'VA_Revenue']

# Process supplemental VA data (now that get_week_ending_friday is defined)
supplemental_df = pd.DataFrame(supplemental_va_data, columns=['Date', 'Amount'])
supplemental_df['Date'] = pd.to_datetime(supplemental_df['Date'])
supplemental_df['Week'] = get_week_ending_friday(supplemental_df['Date'])
supplemental_weekly = supplemental_df.groupby('Week')['Amount'].sum().reset_index()
supplemental_weekly.columns = ['Week', 'Supplemental_VA']
print(f"Supplemental VA records: {len(supplemental_df)}")
print(f"Total Supplemental VA: ${supplemental_weekly['Supplemental_VA'].sum():,.2f}")

# Merge supplemental VA into main VA weekly
va_weekly = pd.merge(va_weekly, supplemental_weekly, on='Week', how='outer').fillna(0)
va_weekly['VA_Revenue'] = va_weekly['VA_Revenue'] + va_weekly['Supplemental_VA']
va_weekly = va_weekly[['Week', 'VA_Revenue']]  # Drop supplemental column after combining

# Aggregate Primary Care charges and revenue by week
pc_weekly = charges_df.groupby('Week').agg({
    'Service Charge Amount': 'sum',  # Billed amount
    'PC_Revenue': 'sum'              # Collected amount
}).reset_index()
pc_weekly.columns = ['Week', 'PC_Charges', 'PC_Revenue']

# Merge both datasets
weekly_report = pd.merge(va_weekly, pc_weekly, on='Week', how='outer')
weekly_report = weekly_report.fillna(0)
weekly_report = weekly_report.sort_values('Week').reset_index(drop=True)

# For the last 10 completed weeks, set PC Revenue = 23.46% of PC Charges
# (to account for collection lag on recent charges)
COLLECTION_RATE = 0.33
NUM_RECENT_WEEKS = 10

# Get the last 10 weeks (excluding current partial week if any)
if len(weekly_report) >= NUM_RECENT_WEEKS:
    weekly_report.loc[weekly_report.index[-NUM_RECENT_WEEKS:], 'PC_Revenue'] = (
        weekly_report.loc[weekly_report.index[-NUM_RECENT_WEEKS:], 'PC_Charges'] * COLLECTION_RATE
    )

# Total Revenue = VA Revenue + PC Revenue (collections only, not charges)
weekly_report['Total_Revenue'] = weekly_report['VA_Revenue'] + weekly_report['PC_Revenue']

# Assign weekly costs by quarter
def get_quarterly_cost(week_date):
    """Return the weekly cost based on quarter"""
    year = week_date.year
    quarter = (week_date.month - 1) // 3 + 1
    
    if year == 2025:
        if quarter == 1:
            return 9569
        elif quarter == 2:
            return 11223
        elif quarter == 3:
            return 13752
        elif quarter == 4:
            return 13063
    elif year == 2026:
        if quarter == 1:
            return 13063
    # Default to latest known cost for other periods
    return 9569

weekly_report['Cost'] = weekly_report['Week'].apply(get_quarterly_cost)

# Calculate Profit = Total Revenue - Cost
weekly_report['Profit'] = weekly_report['Total_Revenue'] - weekly_report['Cost']

# Calculate 8-week moving averages
weekly_report['VA_Revenue_8wk_MA'] = weekly_report['VA_Revenue'].rolling(window=8, min_periods=1).mean()
weekly_report['Total_Revenue_8wk_MA'] = weekly_report['Total_Revenue'].rolling(window=8, min_periods=1).mean()
weekly_report['Profit_8wk_MA'] = weekly_report['Profit'].rolling(window=8, min_periods=1).mean()

# ============================================
# 5. DISPLAY REPORT
# ============================================
print("\n" + "="*80)
print("WEEKLY REVENUE REPORT")
print("="*80)
print(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n")

# Format for display
display_df = weekly_report.copy()
display_df['Week'] = display_df['Week'].dt.strftime('%Y-%m-%d')
display_df['VA_Revenue'] = display_df['VA_Revenue'].apply(lambda x: f"${x:,.2f}")
display_df['PC_Charges'] = display_df['PC_Charges'].apply(lambda x: f"${x:,.2f}")
display_df['PC_Revenue'] = display_df['PC_Revenue'].apply(lambda x: f"${x:,.2f}")
display_df['Total_Revenue'] = display_df['Total_Revenue'].apply(lambda x: f"${x:,.2f}")
display_df['Cost'] = display_df['Cost'].apply(lambda x: f"${x:,.2f}")
display_df['Profit'] = display_df['Profit'].apply(lambda x: f"${x:,.2f}")
display_df['VA_Revenue_8wk_MA'] = display_df['VA_Revenue_8wk_MA'].apply(lambda x: f"${x:,.2f}")
display_df['Total_Revenue_8wk_MA'] = display_df['Total_Revenue_8wk_MA'].apply(lambda x: f"${x:,.2f}")
display_df['Profit_8wk_MA'] = display_df['Profit_8wk_MA'].apply(lambda x: f"${x:,.2f}")
display_df.columns = ['Week Ending', 'VA Revenue', 'PC Charges', 'PC Revenue', 'Total Revenue', 'Cost', 'Profit', 'VA 8wk MA', 'Total 8wk MA', 'Profit 8wk MA']

# Show all weeks
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
print(display_df.to_string(index=False))

# ============================================
# 6. SUMMARY STATISTICS
# ============================================
print("\n" + "="*80)
print("SUMMARY")
print("="*80)

total_va = weekly_report['VA_Revenue'].sum()
total_pc = weekly_report['PC_Revenue'].sum()
total_all = weekly_report['Total_Revenue'].sum()

avg_weekly_va = weekly_report['VA_Revenue'].mean()
avg_weekly_pc = weekly_report['PC_Revenue'].mean()
avg_weekly_total = weekly_report['Total_Revenue'].mean()

print(f"\nTotal VA Revenue:           ${total_va:,.2f}")
print(f"Total Primary Care Revenue: ${total_pc:,.2f}")
print(f"Total Combined Revenue:     ${total_all:,.2f}")

print(f"\nAverage Weekly VA Revenue:           ${avg_weekly_va:,.2f}")
print(f"Average Weekly Primary Care Revenue: ${avg_weekly_pc:,.2f}")
print(f"Average Weekly Total Revenue:        ${avg_weekly_total:,.2f}")

print(f"\nNumber of Weeks: {len(weekly_report)}")
print(f"Date Range: {weekly_report['Week'].min().strftime('%Y-%m-%d')} to {weekly_report['Week'].max().strftime('%Y-%m-%d')}")

# ============================================
# 7. SAVE TO CSV
# ============================================
output_file = 'weekly_revenue_report.csv'
weekly_report.to_csv(output_file, index=False)
print(f"\nReport saved to: {output_file}")
