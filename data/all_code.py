# ==========================================
# 1. SETUP (Load Data for this Notebook)
# ==========================================
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime

# Load Data
try:
    df = pd.read_csv('201 Bills and Payments.csv')
    pc = pd.read_csv('All Encounters 2026-01-01 17_20_08.csv')
    charges = pd.read_csv('260106 Charges Report.csv')
    print("Data Loaded.")
except FileNotFoundError:
    print("Error: Files not found.")


# --- QUICK RECALCULATION OF REALIZATION RATE ---
# (We need this to value the clinical visits accurately)
def clean_currency(x):
    if isinstance(x, str):
        x = x.replace('$', '').replace(',', '').strip()
        if '(' in x and ')' in x: x = '-' + x.replace('(', '').replace(')', '')
    return pd.to_numeric(x, errors='coerce')


charges['Service Charge Amount'] = charges['Service Charge Amount'].apply(clean_currency).fillna(0)
charges['Total_Contract_Adj'] = (
        charges['Pri Ins Insurance Contract Adjustment'].apply(clean_currency).fillna(0) +
        charges['Sec Ins Insurance Contract Adjustment'].apply(clean_currency).fillna(0) +
        charges['Other Ins Insurance Contract Adjustment'].apply(clean_currency).fillna(0)
)
charges['Calculated_Allowed'] = charges['Service Charge Amount'] - charges['Total_Contract_Adj']

total_billed = charges['Service Charge Amount'].sum()
total_allowed = charges['Calculated_Allowed'].sum()
REALIZATION_FACTOR = (total_allowed / total_billed) if total_billed > 0 else 0.40

print(f"Using Realization Rate: {REALIZATION_FACTOR * 100:.2f}%")

# ==========================================
# 1B. DATA CLEANING: FUTURE DATE FIXER (CORRECTED)
# ==========================================
def fix_future_dates(df, date_cols):
    """
    Scans date columns for entries in the future.
    Fixes them by setting the year to the current year.
    If that is STILL in the future, sets it to the previous year.
    """
    # 1. Define 'Today' using Pandas (Avoids datetime import errors)
    today = pd.Timestamp.now().normalize()
    current_year = today.year

    # 2. Iterate through columns
    for col in date_cols:
        if col in df.columns:
            # Ensure it is datetime format first
            df[col] = pd.to_datetime(df[col], errors='coerce')

            # Find future dates
            future_mask = df[col] > today

            if future_mask.any():
                print(f"   - Found {future_mask.sum()} future dates in '{col}'. Fixing...")

                # Logic: Create a temporary series with the current year
                def adjust_date(d):
                    if pd.isnull(d) or d <= today:
                        return d

                    # Try setting to this year
                    try:
                        fixed_this_year = d.replace(year=current_year)
                    except ValueError:
                        # Handle leap year edge cases
                        fixed_this_year = d - pd.DateOffset(years=d.year - current_year)

                    # If fixing to this year is STILL in the future, go back one more year
                    if fixed_this_year > today:
                        return fixed_this_year.replace(year=current_year - 1)
                    else:
                        return fixed_this_year

                # Apply the fix
                df.loc[future_mask, col] = df.loc[future_mask, col].apply(adjust_date)

    return df

# --- APPLY THE FIX ---
print("Scanning for Future Dates...")

# Fix Charges Report
if 'charges' in locals():
    charges = fix_future_dates(charges, ['Date Of Service', 'Pri Ins Adjudication Date', 'Pat Last Bill Date', 'Posting Date'])

# Fix Clinical Reports
if 'pc' in locals():
    pc = fix_future_dates(pc, ['Date of Service'])
if 'df' in locals():
    df = fix_future_dates(df, ['Date of Service'])

print("Future dates fixed.")

# ==========================================
# 2. PREPARE DATA FOR TRENDS
# ==========================================

# --- A. CLINICAL DATA ---
pc = pc.rename(columns={'Rendering Provider': 'Provider', 'Total Charge Amount': 'Total Charges'})
pc['Billed'] = pc['Total Charges'].astype(str).str.replace(r'[$,]', '', regex=True).astype(float)
pc['Fees'] = pc['Billed'] * REALIZATION_FACTOR
pc['Date of Service'] = pd.to_datetime(pc['Date of Service'])
pc['Provider'] = pc['Provider'].replace({
    'Jenks, Anne ': 'Anne Jenks', 'IRVIN, EHRIN ': 'Ehrin Irvin',
    'SUGGS, SARAH ': 'Sarah Suggs', 'REDD, DAVID ': 'Anne Jenks'
})
clinical_data = pc[['Date of Service', 'Provider', 'Fees']].copy()

# --- B. VA/DBQ DATA ---
df = df.rename(columns={'Sarah Suggs ': 'Provider'})
df['Date of Service'] = pd.to_datetime(df['Date of Service'].ffill(), errors='coerce')
df = df.dropna(subset=['Date of Service']).fillna('0')
df['Provider'] = df['Provider'].str.strip().replace({
    '0': 'Anne Jenks', 'sArah Suggs': 'Sarah Suggs', 'SArah Suggs': 'Sarah Suggs'
})

# Recalculate Fixed Fees (Simplified for Trends)
# (Copying the Fee Logic from previous notebook)
fees = {
    'GM_DBQs': {'0': 0, '1-5': 250, '6-10': 330, '11-15': 410, '16+': 550},
    'Focused': {'0': 0, '1-5': 200, '6-10': 280, '11-15': 370, '16+': 480},
    'TBI': 250,
    'IMO': {'0': 0, '1-3': 150, '4-6': 260, '7-9': 330, '10-12': 350, '13-15': 465, '16-18': 525, '19+': 600},
    'No_Show': {'0': 0, '1': 60}
}


def fee_calc(row):
    # Quick cleaning helper within the lambda
    clean = lambda x: str(x).strip().replace('.0', '')

    f_fee = fees['Focused'].get(clean(row.get('Focused DBQs', '0')), 0)
    imo_fee = fees['IMO'].get(clean(row.get('Routine IMOs', '0')), 0)
    gmed_fee = fees['GM_DBQs'].get(clean(row.get('Gen Med DBQs', '0')), 0)
    noshow_fee = fees['No_Show'].get(clean(row.get('No Show', '0')), 0)
    tbi_fee = 250 if str(row.get('TBI', '0')).lower() not in ['0', 'nan', '', 'no', 'false'] else 0
    return f_fee + imo_fee + gmed_fee + noshow_fee + tbi_fee


df['Fees'] = df.apply(fee_calc, axis=1)
va_data = df[['Date of Service', 'Provider', 'Fees']].copy()

# --- C. MERGE ---
master_trend = pd.concat([clinical_data, va_data])
master_trend = master_trend.sort_values('Date of Service')

print("Trend Data Prepared.")

# ==========================================
# 3. GENERATE TREND REPORT (Monthly)
# ==========================================

# 1. Group by Month and Provider
# pd.Grouper(freq='ME') groups by Month End
monthly_revenue = master_trend.groupby([pd.Grouper(key='Date of Service', freq='ME'), 'Provider'])[
    'Fees'].sum().unstack().fillna(0)

# 2. Filter to relevant history (e.g., last 6 months)
# You can adjust '2025-06-01' to whatever start date you want
monthly_revenue = monthly_revenue[monthly_revenue.index >= '2024-07-01']

# 3. Add a "Total" Column for the practice
monthly_revenue['TOTAL PRACTICE'] = monthly_revenue.sum(axis=1)

# 4. Clean up the Date Format for display (YYYY-MM)
monthly_revenue.index = monthly_revenue.index.strftime('%Y-%B')

print("\n" + "=" * 60)
print("             MONTHLY REVENUE TRENDS")
print("=" * 60)
print(monthly_revenue.to_string(float_format="${:,.0f}".format))
print("-" * 60)

# ==========================================
# 4. VISUALIZATION (Line Chart)
# ==========================================

plt.figure(figsize=(12, 6))
# Drop the 'TOTAL PRACTICE' column for the chart so it doesn't skew the scale
plot_data = monthly_revenue.drop(columns=['TOTAL PRACTICE'])

sns.lineplot(data=plot_data, markers=True, dashes=False, linewidth=2.5)

plt.title('Provider Revenue Trends', fontsize=16)
plt.ylabel('Estimated Revenue ($)', fontsize=12)
plt.xlabel('Month', fontsize=6)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(title='Provider', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()

# Show the chart
plt.show()

# ==========================================
# FINAL RPV TREND REPORT (Merged Encounters)
# ==========================================
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- CONFIGURATION ---
START_DATE = '2025-01-01'
END_DATE   = '2025-12-31'
MIN_VISIT_VALUE = 25  # Filter out low-value admin visits (nurse visits, etc)
PENDING_DAYS = 30     # Filter out recent claims waiting for insurance

if 'charges' not in locals():
    print("Please run the Data Loading cell first.")
else:
    # 1. SETUP & CLEANING
    # -------------------
    # Clean Provider Names
    charges['Rendering Provider'] = charges['Rendering Provider'].astype(str).str.upper().str.strip()
    provider_map = {
        'EHRIN IRVIN, FNP': 'Ehrin Irvin',
        'ANNE JENKS, APRN': 'Anne Jenks',
        'SARAH SUGGS, NP':  'Sarah Suggs',
        'DAVID REDD, MD':   'Anne Jenks',
        'HEATHER MAYO, FNP': 'Heather Mayo'
    }
    charges['Normalized_Provider'] = charges['Rendering Provider'].map(provider_map).fillna('Other')
    target_providers = ['Anne Jenks', 'Ehrin Irvin', 'Sarah Suggs', 'Heather Mayo']

    # Financial Cleaning
    def clean_curr(x):
        if isinstance(x, str):
            x = x.replace('$', '').replace(',', '').strip()
            if '(' in x and ')' in x: x = '-' + x.replace('(', '').replace(')', '')
        return pd.to_numeric(x, errors='coerce')

    charges['Service Charge Amount'] = charges['Service Charge Amount'].apply(clean_curr).fillna(0)

    # Calculate Truth (Allowed = Billed - Writeoffs)
    adj_cols = ['Pri Ins Insurance Contract Adjustment', 'Sec Ins Insurance Contract Adjustment', 'Other Ins Insurance Contract Adjustment']
    charges['Total_Contract_Adj'] = 0
    for col in adj_cols:
        if col in charges.columns:
            charges['Total_Contract_Adj'] += charges[col].apply(clean_curr).fillna(0)

    charges['Allowed Amount'] = charges['Service Charge Amount'] - charges['Total_Contract_Adj']
    charges['Date Of Service'] = pd.to_datetime(charges['Date Of Service'])

    # 2. FILTERING
    # ------------
    df_trend = charges[
        (charges['Normalized_Provider'].isin(target_providers)) &
        (charges['Date Of Service'] >= pd.to_datetime(START_DATE)) &
        (charges['Date Of Service'] <= pd.to_datetime(END_DATE))
        ].copy()

    # REMOVE PENDING CLAIMS (<45 days old + No Adjudication)
    today = pd.Timestamp.now()
    if 'Pri Ins Adjudication Date' in df_trend.columns:
        df_trend['Pri Ins Adjudication Date'] = pd.to_datetime(df_trend['Pri Ins Adjudication Date'], errors='coerce')
        pending_mask = (df_trend['Date Of Service'] > (today - pd.Timedelta(days=PENDING_DAYS))) & (pd.isnull(df_trend['Pri Ins Adjudication Date']))
        df_trend = df_trend[~pending_mask]

    # 3. THE "MERGE" (Group by Patient + Date)
    # ---------------------------------------
    # This matches the logic that got us the correct $111-$156 numbers
    if 'Patient Name' not in df_trend.columns and 'Patient First Name' in df_trend.columns:
        df_trend['Patient Name'] = df_trend['Patient First Name'] + " " + df_trend['Patient Last Name']

    # Create the "Clean Encounter" Dataset
    encounters = df_trend.groupby(['Normalized_Provider', 'Date Of Service', 'Patient Name'])['Allowed Amount'].sum().reset_index()

    # Filter out noise (visits < $25)
    valid_encounters = encounters[encounters['Allowed Amount'] > MIN_VISIT_VALUE].copy()
    valid_encounters = valid_encounters.sort_values('Date Of Service')

    print(f"Generating Trends from {len(valid_encounters)} merged clinical encounters...")

    # 4. CHART 1: PRACTICE WIDE TREND
    # -------------------------------
    valid_encounters['Practice_Trend'] = valid_encounters['Allowed Amount'].rolling(window=100, min_periods=10).mean()
    avg_rpv = valid_encounters['Allowed Amount'].mean()

    # DYNAMIC SCALING LOGIC
    y_limit = valid_encounters['Allowed Amount'].quantile(0.98)

    plt.figure(figsize=(12, 6))
    plt.plot(valid_encounters['Date Of Service'], valid_encounters['Practice_Trend'], color='darkblue', linewidth=3, label='Trend (50-Visit Rolling Avg)')
    plt.scatter(valid_encounters['Date Of Service'], valid_encounters['Allowed Amount'], alpha=0.1, color='gray', s=10)

    plt.title(f'True Practice Revenue Per Visit (Merged Encounters)', fontsize=16)
    plt.ylabel('Allowed Amount ($)', fontsize=12)
    plt.axhline(avg_rpv, color='red', linestyle='--', label=f'Avg: ${avg_rpv:.0f}')

    # APPLY THE FIX
    plt.ylim(0, y_limit - 200)  # Set scale from 0 to the 98th percentile + padding

    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()

    print(f"Practice Wide Average: ${avg_rpv:,.2f}")
    print(f"Chart Scale Set to: $0 - ${y_limit + 50:.0f} (Hides top 2% outliers for readability)")

    # 5. CHART 2: PROVIDER TRENDS
    # ---------------------------
    plt.figure(figsize=(12, 6))

    for provider in target_providers:
        p_data = valid_encounters[valid_encounters['Normalized_Provider'] == provider].copy().sort_values('Date Of Service')

        if len(p_data) > 10:
            p_data['Smooth_Trend'] = p_data['Allowed Amount'].rolling(window=30, min_periods=5).mean()
            plt.plot(p_data['Date Of Service'], p_data['Smooth_Trend'], linewidth=2.5, label=f"{provider} (${p_data['Allowed Amount'].mean():.0f})")

    plt.title('True Revenue Per Visit by Provider', fontsize=16)
    plt.ylabel('Avg Revenue per Encounter ($)', fontsize=12)
    plt.xlabel('Date', fontsize=12)

    # APPLY THE SAME FIX
    plt.ylim(0, y_limit - 200)

    plt.legend(title='Provider (Avg RPV)')
    plt.grid(True, alpha=0.3)
    plt.show()

    # ==========================================
# RPV DEEP DIVE: PATIENT-LEVEL GROUPING (CORRECTED)
# ==========================================
import pandas as pd
import matplotlib.pyplot as plt

# --- CONFIGURATION ---
START_DATE = '2025-01-01'
END_DATE = '2025-12-31'
MIN_VISIT_VALUE = 25

if 'charges' not in locals():
    print("Please run the Data Loading cell first.")
else:
    # 1. CLEANING & SETUP
    # -------------------
    # Clean Provider Names
    charges['Rendering Provider'] = charges['Rendering Provider'].astype(str).str.upper().str.strip()
    provider_map = {
        'EHRIN IRVIN, FNP': 'Ehrin Irvin',
        'ANNE JENKS, APRN': 'Anne Jenks',
        'SARAH SUGGS, NP': 'Sarah Suggs',
        'DAVID REDD, MD': 'Anne Jenks',
        'HEATHER MAYO, FNP': 'Heather Mayo'
    }
    charges['Normalized_Provider'] = charges['Rendering Provider'].map(provider_map).fillna('Other')
    target_providers = ['Anne Jenks', 'Ehrin Irvin', 'Sarah Suggs', 'Heather Mayo']


    # Financial Cleaning
    def clean_curr(x):
        if isinstance(x, str):
            x = x.replace('$', '').replace(',', '').strip()
            if '(' in x and ')' in x: x = '-' + x.replace('(', '').replace(')', '')
        return pd.to_numeric(x, errors='coerce')


    # Apply Cleaning
    charges['Service Charge Amount'] = charges['Service Charge Amount'].apply(clean_curr).fillna(0)

    # Calculate Contract Adjustments
    adj_cols = ['Pri Ins Insurance Contract Adjustment', 'Sec Ins Insurance Contract Adjustment',
                'Other Ins Insurance Contract Adjustment']
    charges['Total_Contract_Adj'] = 0
    for col in adj_cols:
        if col in charges.columns:
            charges['Total_Contract_Adj'] += charges[col].apply(clean_curr).fillna(0)

    # Calculate Truth
    charges['Allowed Amount'] = charges['Service Charge Amount'] - charges['Total_Contract_Adj']
    charges['Date Of Service'] = pd.to_datetime(charges['Date Of Service'])

    # 2. FILTERING
    # ------------
    df_rpv = charges[
        (charges['Normalized_Provider'].isin(target_providers)) &
        (charges['Date Of Service'] >= pd.to_datetime(START_DATE)) &
        (charges['Date Of Service'] <= pd.to_datetime(END_DATE))
        ].copy()

    # REMOVE PENDING CLAIMS (The 45-day logic)
    today = pd.Timestamp.now()
    if 'Pri Ins Adjudication Date' in df_rpv.columns:
        df_rpv['Pri Ins Adjudication Date'] = pd.to_datetime(df_rpv['Pri Ins Adjudication Date'], errors='coerce')
        pending_mask = (df_rpv['Date Of Service'] > (today - pd.Timedelta(days=45))) & (
            pd.isnull(df_rpv['Pri Ins Adjudication Date']))
        df_rpv = df_rpv[~pending_mask]

    # --- SAFETY FIX: Ensure 'Claim ID' column exists ---
    if 'claim ID' in df_rpv.columns:
        df_rpv.rename(columns={'claim ID': 'Claim ID'}, inplace=True)

    # 3. THE "SUPER GROUPER" (Patient + Date)
    # ---------------------------------------
    if 'Patient Name' not in df_rpv.columns and 'Patient First Name' in df_rpv.columns:
        df_rpv['Patient Name'] = df_rpv['Patient First Name'] + " " + df_rpv['Patient Last Name']

    # Aggregation
    visits = df_rpv.groupby(['Normalized_Provider', 'Date Of Service', 'Patient Name']).agg({
        'Service Charge Amount': 'sum',  # Total Billed
        'Allowed Amount': 'sum',  # Total Allowed
        'Claim ID': 'nunique'  # Count how many claims were merged
    }).reset_index()

    # 4. EXCLUDE "NOISE" VISITS
    # -------------------------
    zero_visits = visits[visits['Allowed Amount'] <= 0]
    valid_visits = visits[visits['Allowed Amount'] > MIN_VISIT_VALUE].copy()  # Using $25 floor

    # 5. ANALYSIS
    # -----------
    avg_billed = valid_visits['Service Charge Amount'].mean()
    avg_allowed = valid_visits['Allowed Amount'].mean()
    realization = (avg_allowed / avg_billed * 100) if avg_billed > 0 else 0

    print("\n" + "=" * 60)
    print(f"      TRUE REVENUE PER VISIT (Merged by Patient)")
    print("=" * 60)
    print(f"Total Raw Encounters:   {len(visits)}")
    print(
        f"Excluded Zero/Low Visits: {len(visits) - len(valid_visits)} (Likely denials or nurse visits <${MIN_VISIT_VALUE})")
    print(f"Valid Clinical Visits:  {len(valid_visits)}")
    print("-" * 60)
    print(f"AVG BILLED PER VISIT:   ${avg_billed:,.2f}")
    print(f"AVG ALLOWED PER VISIT:  ${avg_allowed:,.2f}")
    print(f"REALIZATION RATE:       {realization:.1f}%")
    print("-" * 60)

    # 6. PROVIDER BREAKDOWN
    # ---------------------
    prov_stats = valid_visits.groupby('Normalized_Provider').agg({
        'Allowed Amount': ['count', 'mean', 'sum'],
        'Service Charge Amount': 'mean'
    }).reset_index()

    # Flatten columns
    prov_stats.columns = ['Provider', 'Visit Count', 'Avg Allowed (RPV)', 'Total Rev', 'Avg Billed']

    print("\nPROVIDER BREAKDOWN:")
    print(
        prov_stats.to_string(float_format=lambda x: f"${x:,.2f}" if x > 100 else f"{x:.0f}" if x > 10 else f"{x:.2f}"))

    # 7. HISTOGRAM
    plt.figure(figsize=(10, 5))
    plt.hist(valid_visits['Allowed Amount'], bins=30, color='skyblue', edgecolor='black')
    plt.axvline(avg_allowed, color='red', linestyle='dashed', linewidth=1, label=f'Avg: ${avg_allowed:.0f}')
    plt.title('Distribution of Visit Values (Allowed Amounts)')
    plt.xlabel('Revenue ($)')
    plt.ylabel('Number of Visits')
    plt.legend()
    plt.grid(alpha=0.3)
    plt.show()

    # ==========================================
# REVENUE FORECASTING (HOLT-WINTERS)
# ==========================================
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import timedelta

# --- CONFIGURATION ---
FORECAST_WEEKS = 26  # Predict next 6 months
SEASONAL_PERIOD = 4  # Assume a 4-week (monthly) billing cycle pattern

# 1. LOAD & CLEAN DATA
# --------------------
if 'charges' not in locals():
    # Fallback loader if run independently
    try:
        charges = pd.read_csv('260106 Charges Report CSV.csv')
    except:
        print("Error: Could not find charges file.")

# Safety Column Fixes
if 'claim ID' in charges.columns: charges.rename(columns={'claim ID': 'Claim ID'}, inplace=True)


# Financial Cleaning
def clean_curr(x):
    if isinstance(x, str):
        x = x.replace('$', '').replace(',', '').strip()
        if '(' in x and ')' in x: x = '-' + x.replace('(', '').replace(')', '')
    return pd.to_numeric(x, errors='coerce')


charges['Service Charge Amount'] = charges['Service Charge Amount'].apply(clean_curr).fillna(0)
adj_cols = ['Pri Ins Insurance Contract Adjustment', 'Sec Ins Insurance Contract Adjustment',
            'Other Ins Insurance Contract Adjustment']
charges['Total_Contract_Adj'] = 0
for col in adj_cols:
    if col in charges.columns:
        charges['Total_Contract_Adj'] += charges[col].apply(clean_curr).fillna(0)

# Calculate Truth (Allowed Amount)
charges['Allowed Amount'] = charges['Service Charge Amount'] - charges['Total_Contract_Adj']
charges['Date Of Service'] = pd.to_datetime(charges['Date Of Service'])

# 2. PREPARE TIME SERIES (WEEKLY AGGREGATION)
# -------------------------------------------
# Filter for 2025 only to ensure clean trend lines
ts_data = charges[
    (charges['Date Of Service'] >= '2024-01-01') &
    (charges['Date Of Service'] <= '2025-12-12')  # Stop at "Today"
    ].copy()

# Group by Week (Monday Start)
weekly_rev = ts_data.groupby(pd.Grouper(key='Date Of Service', freq='W-MON'))['Allowed Amount'].sum()

# Fill missing weeks with 0 (essential for time series models)
weekly_rev = weekly_rev.asfreq('W-MON', fill_value=0)

# 3. RUN THE HOLT-WINTERS MODEL
# -----------------------------
# We use 'add' for trend (linear growth) and 'add' for seasonality (stable fluctuations)
# seasonal_periods=4 implies we expect a pattern to repeat every 4 weeks (monthly)
try:
    model = ExponentialSmoothing(
        weekly_rev,
        trend='add',
        seasonal='add',
        seasonal_periods=SEASONAL_PERIOD,
        damped_trend=True  # Prevents the forecast from shooting to infinity
    ).fit()

    # Generate Forecast
    forecast = model.forecast(FORECAST_WEEKS)

    # Create specific dates for the forecast
    last_date = weekly_rev.index[-1]
    forecast_dates = [last_date + timedelta(weeks=x) for x in range(1, FORECAST_WEEKS + 1)]
    forecast_series = pd.Series(forecast.values, index=forecast_dates)

    # 4. VISUALIZATION
    # ----------------
    plt.figure(figsize=(14, 7))

    # Plot Historical
    plt.plot(weekly_rev.index, weekly_rev, label='Actual 2025 Revenue', color='#1f77b4', linewidth=2)

    # Plot Forecast
    plt.plot(forecast_series.index, forecast_series, label='6-Month Forecast', color='#ff7f0e', linestyle='--',
             linewidth=2.5)

    # Add Trend Line (Smoothed History) to show the "Signal" amidst the noise
    plt.plot(weekly_rev.index, model.fittedvalues, color='green', alpha=0.3, label='Model Fit (Smoothed)', linewidth=1)

    plt.title(f'Weekly Revenue Forecast (Next 6 Months)', fontsize=16)
    plt.ylabel('Weekly Revenue ($)', fontsize=12)
    plt.xlabel('Date', fontsize=12)

    # Add a zero line
    plt.axhline(0, color='black', linewidth=0.5)

    # Format Currency on Y-Axis
    current_values = plt.gca().get_yticks()
    plt.gca().set_yticklabels(['${:,.0f}'.format(x) for x in current_values])

    plt.legend(loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.show()

    # 5. PRINT SUMMARY
    # ----------------
    avg_hist = weekly_rev.mean()
    avg_forecast = forecast_series.mean()
    growth = ((avg_forecast - avg_hist) / avg_hist) * 100

    print("\n" + "=" * 50)
    print("      FORECAST SUMMARY (Holt-Winters)")
    print("=" * 50)
    print(f"Historical Avg Weekly Revenue: ${avg_hist:,.2f}")
    print(f"Projected Avg Weekly Revenue:  ${avg_forecast:,.2f}")
    print(f"Projected Growth/Trend:        {growth:+.1f}%")
    print("-" * 50)
    print("Top 3 Predicted Weeks (Cash Flow Spikes):")
    top_weeks = forecast_series.sort_values(ascending=False).head(3)
    for date, val in top_weeks.items():
        print(f"  Week of {date.date()}: ${val:,.2f}")

except ValueError as e:
    print("Not enough data to run seasonal Holt-Winters (Need at least 2 full cycles/8 weeks).")
    print("Try running closer to the end of the year or reducing seasonality.")
    print(f"Error Details: {e}")

    # ==========================================
# TOTAL PRACTICE FORECAST (CLINICAL + VA)
# ==========================================
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import timedelta

# --- CONFIGURATION ---
FORECAST_WEEKS = 52  # Predict next 6 months
SEASONAL_PERIOD = 4  # Monthly cycle pattern

# 1. LOAD DATA
# ------------
if 'charges' not in locals() or 'df' not in locals():
    print("Loading Data...")
    try:
        charges = pd.read_csv('260106 Charges Report CSV.csv')
        df = pd.read_csv('201 Bills and Payments.csv')  # The VA Data
    except:
        print("Error: Files not found.")

# 2. PROCESS CLINICAL REVENUE (Standard Logic)
# --------------------------------------------
# Safety Name Fix
if 'claim ID' in charges.columns: charges.rename(columns={'claim ID': 'Claim ID'}, inplace=True)


# Clean Currency
def clean_curr(x):
    if isinstance(x, str):
        x = x.replace('$', '').replace(',', '').strip()
        if '(' in x and ')' in x: x = '-' + x.replace('(', '').replace(')', '')
    return pd.to_numeric(x, errors='coerce')


charges['Service Charge Amount'] = charges['Service Charge Amount'].apply(clean_curr).fillna(0)
adj_cols = ['Pri Ins Insurance Contract Adjustment', 'Sec Ins Insurance Contract Adjustment',
            'Other Ins Insurance Contract Adjustment']
charges['Total_Contract_Adj'] = 0
for col in adj_cols:
    if col in charges.columns:
        charges['Total_Contract_Adj'] += charges[col].apply(clean_curr).fillna(0)

# Calculate Truth (Allowed)
charges['Allowed Amount'] = charges['Service Charge Amount'] - charges['Total_Contract_Adj']
charges['Date Of Service'] = pd.to_datetime(charges['Date Of Service'])

# Create Clinical Stream
clinical_stream = charges[['Date Of Service', 'Allowed Amount']].rename(columns={'Allowed Amount': 'Revenue'}).copy()
clinical_stream['Source'] = 'Clinical'

# 3. PROCESS VA REVENUE (Fee Schedule Logic)
# ------------------------------------------
# Clean Dates
df = df.rename(columns={'Unnamed: 0': '201 ID', 'Sarah Suggs ': 'Provider'})
df['Date of Service'] = df['Date of Service'].ffill()
df['Date of Service'] = pd.to_datetime(df['Date of Service'], errors='coerce')
df = df.dropna(subset=['Date of Service']).fillna('0')

# Clean Columns
for col in ['Focused DBQs', 'Routine IMOs', 'Gen Med DBQs', 'No Show', 'TBI']:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

# Mappings
dbq_map = {'Visual': '0', 'r-tbi': '0', '1--5': '1-5', 'bp check': '0', 'nan': '0', '': '0'}
imo_map = {'7/9': '7-9', '1/3': '1-3', '0': '0', '1-5': '1-3', '1': '1-3', '4': '4-6', '7': '7-9', '6-10': '4-6'}
gen_med_map = {'0': '0', 'BP CHECK': '0', '11-13': '11-15', '1-6': '1-5', '7': '6-10', '6': '6-10', '??': '0'}

df['Focused DBQs'] = df['Focused DBQs'].replace(dbq_map)
df['Routine IMOs'] = df['Routine IMOs'].replace(imo_map)
df['Gen Med DBQs'] = df['Gen Med DBQs'].replace(gen_med_map)

# Fee Schedule
fees = {
    'GM_DBQs': {'0': 0, '1-5': 250, '6-10': 330, '11-15': 410, '16+': 550},
    'Focused': {'0': 0, '1-5': 200, '6-10': 280, '11-15': 370, '16+': 480},
    'TBI': 250,
    'IMO': {'0': 0, '1-3': 150, '4-6': 260, '7-9': 330, '10-12': 350, '13-15': 465, '16-18': 525, '19+': 600},
    'No_Show': {'0': 0, '1': 60}
}


def fee_calc(row):
    f_fee = fees['Focused'].get(row['Focused DBQs'], 0)
    imo_fee = fees['IMO'].get(row['Routine IMOs'], 0)
    gmed_fee = fees['GM_DBQs'].get(row['Gen Med DBQs'], 0)
    noshow_fee = fees['No_Show'].get(row['No Show'], 0)
    tbi_fee = 250 if row['TBI'] not in ['0', 'nan', '', 'no', 'false'] else 0
    return f_fee + imo_fee + gmed_fee + noshow_fee + tbi_fee


df['Fees'] = df.apply(fee_calc, axis=1)

# Create VA Stream
va_stream = df[['Date of Service', 'Fees']].rename(
    columns={'Date of Service': 'Date Of Service', 'Fees': 'Revenue'}).copy()
va_stream['Source'] = 'VA/DBQ'

# 4. MERGE & AGGREGATE
# --------------------
total_stream = pd.concat([clinical_stream, va_stream], ignore_index=True)

# Filter for 2025 Analysis Period
analysis_start = '2024-07-01'
analysis_end = '2025-12-12'

ts_data = total_stream[
    (total_stream['Date Of Service'] >= analysis_start) &
    (total_stream['Date Of Service'] <= analysis_end)
    ].copy()

# Group by Week (Monday Start)
weekly_rev = ts_data.groupby(pd.Grouper(key='Date Of Service', freq='W-MON'))['Revenue'].sum()
weekly_rev = weekly_rev.asfreq('W-MON', fill_value=0)  # Fill gaps

print(f"Total Revenue Analyzed (2025 YTD): ${weekly_rev.sum():,.2f}")

# 5. RUN FORECAST (HOLT-WINTERS)
# ------------------------------
try:
    model = ExponentialSmoothing(
        weekly_rev,
        trend='add',
        seasonal='add',
        seasonal_periods=SEASONAL_PERIOD,
        damped_trend=True
    ).fit()

    forecast = model.forecast(FORECAST_WEEKS)

    # Create forecast dates
    last_date = weekly_rev.index[-1]
    forecast_dates = [last_date + timedelta(weeks=x) for x in range(1, FORECAST_WEEKS + 1)]
    forecast_series = pd.Series(forecast.values, index=forecast_dates)

    # 6. VISUALIZATION
    # ----------------
    plt.figure(figsize=(14, 7))

    # Plot Historical
    plt.plot(weekly_rev.index, weekly_rev, label='Actual Revenue (Clinical + VA)', color='#1f77b4', linewidth=2)

    # Plot Forecast
    plt.plot(forecast_series.index, forecast_series, label='6-Month Forecast', color='#ff7f0e', linestyle='--',
             linewidth=2.5)

    # Plot Trend Line (Model Fit)
    plt.plot(weekly_rev.index, model.fittedvalues, color='green', alpha=0.3, label='Smoothed Trend', linewidth=1)

    plt.title(f'Total Practice Revenue Forecast (Clinical + VA)', fontsize=16)
    plt.ylabel('Weekly Revenue ($)', fontsize=12)
    plt.xlabel('Date', fontsize=12)
    plt.axhline(0, color='black', linewidth=0.5)

    # Format Y Axis
    current_values = plt.gca().get_yticks()
    plt.gca().set_yticklabels(['${:,.0f}'.format(x) for x in current_values])

    plt.legend(loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.show()

    # 7. SUMMARY METRICS
    # ------------------
    avg_hist = weekly_rev.mean()
    avg_forecast = forecast_series.mean()
    growth = ((avg_forecast - avg_hist) / avg_hist) * 100

    print("\n" + "=" * 50)
    print("      TOTAL PRACTICE FORECAST SUMMARY")
    print("=" * 50)
    print(f"Avg Weekly Revenue (2025):    ${avg_hist:,.2f}")
    print(f"Forecasted Weekly Revenue:    ${avg_forecast:,.2f}")
    print(f"Projected Growth Trend:       {growth:+.1f}%")
    print("-" * 50)
    print("Top 3 Predicted Revenue Weeks:")
    top_weeks = forecast_series.sort_values(ascending=False).head(3)
    for date, val in top_weeks.items():
        print(f"  Week of {date.date()}: ${val:,.2f}")

except Exception as e:
    print(f"Forecasting Error: {e}")
    print("Ensure you have at least 8 weeks of combined data.")

    # ==========================================
# WEEKLY PRIMARY CARE CHARGES FORECAST
# (Unadjusted Service Charge Amount)
# ==========================================

# --- CONFIGURATION ---
FORECAST_WEEKS = 26  # Predict next 6 months
SEASONAL_PERIOD = 4  # Assume a 4-week (monthly) billing cycle pattern

# 1. LOAD DATA
# ------------
try:
    charges = pd.read_csv('260101 Charges Report.csv')
except:
    print("Error: Could not find charges file.")

# Safety Column Fixes
if 'claim ID' in charges.columns:
    charges.rename(columns={'claim ID': 'Claim ID'}, inplace=True)


# Clean Currency
def clean_curr(x):
    if isinstance(x, str):
        x = x.replace('$', '').replace(',', '').strip()
        if '(' in x and ')' in x:
            x = '-' + x.replace('(', '').replace(')', '')
    return pd.to_numeric(x, errors='coerce')


charges['Service Charge Amount'] = charges['Service Charge Amount'].apply(clean_curr).fillna(0)
charges['Date Of Service'] = pd.to_datetime(charges['Date Of Service'])

# 2. PREPARE TIME SERIES (WEEKLY AGGREGATION)
# -------------------------------------------
# Filter for analysis period
ts_data = charges[
    (charges['Date Of Service'] >= '2024-01-01') &
    (charges['Date Of Service'] <= '2025-12-12')
].copy()

# Group by Week (Monday Start) - Using SERVICE CHARGE AMOUNT (unadjusted)
weekly_charges = ts_data.groupby(pd.Grouper(key='Date Of Service', freq='W-MON'))['Service Charge Amount'].sum()

# Fill missing weeks with 0
weekly_charges = weekly_charges.asfreq('W-MON', fill_value=0)

print(f"Total Primary Care Charges Analyzed: ${weekly_charges.sum():,.2f}")

# 3. RUN THE HOLT-WINTERS MODEL
# -----------------------------
try:
    model = ExponentialSmoothing(
        weekly_charges,
        trend='add',
        seasonal='add',
        seasonal_periods=SEASONAL_PERIOD,
        damped_trend=True
    ).fit()

    # Generate Forecast
    forecast = model.forecast(FORECAST_WEEKS)

    # Create forecast dates
    last_date = weekly_charges.index[-1]
    forecast_dates = [last_date + timedelta(weeks=x) for x in range(1, FORECAST_WEEKS + 1)]
    forecast_series = pd.Series(forecast.values, index=forecast_dates)

    # 4. VISUALIZATION
    # ----------------
    plt.figure(figsize=(14, 7))

    # Plot Historical
    plt.plot(weekly_charges.index, weekly_charges, label='Actual Weekly Charges', color='#2ca02c', linewidth=2)

    # Plot Forecast
    plt.plot(forecast_series.index, forecast_series, label='6-Month Forecast', color='#d62728', linestyle='--',
             linewidth=2.5)

    # Plot Trend Line
    plt.plot(weekly_charges.index, model.fittedvalues, color='orange', alpha=0.3, label='Model Fit (Smoothed)',
             linewidth=1)

    plt.title('Weekly Primary Care Charges Forecast (Unadjusted)', fontsize=16)
    plt.ylabel('Weekly Charges ($)', fontsize=12)
    plt.xlabel('Date', fontsize=12)
    plt.axhline(0, color='black', linewidth=0.5)

    # Format Y Axis
    current_values = plt.gca().get_yticks()
    plt.gca().set_yticklabels(['${:,.0f}'.format(x) for x in current_values])

    plt.legend(loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.show()

    # 5. SUMMARY METRICS
    # ------------------
    avg_hist = weekly_charges.mean()
    avg_forecast = forecast_series.mean()
    growth = ((avg_forecast - avg_hist) / avg_hist) * 100

    print("\n" + "=" * 50)
    print("  WEEKLY PRIMARY CARE CHARGES FORECAST SUMMARY")
    print("=" * 50)
    print(f"Historical Avg Weekly Charges: ${avg_hist:,.2f}")
    print(f"Projected Avg Weekly Charges:  ${avg_forecast:,.2f}")
    print(f"Projected Growth/Trend:        {growth:+.1f}%")
    print("-" * 50)
    print("Top 3 Predicted Weeks:")
    top_weeks = forecast_series.sort_values(ascending=False).head(3)
    for date, val in top_weeks.items():
        print(f"  Week of {date.date()}: ${val:,.2f}")

except Exception as e:
    print(f"Forecasting Error: {e}")
    print("Ensure you have at least 8 weeks of data.")


# ==========================================
# MONTHLY BREAKDOWN - LAST 12 MONTHS
# (Total Service Charge Amounts)
# ==========================================

# 1. LOAD DATA (if not already loaded)
# -------------------------------------
try:
    if 'charges' not in locals():
        charges = pd.read_csv('260101 Charges Report.csv')
except:
    print("Error: Could not find charges file.")


# Clean Currency
def clean_curr(x):
    if isinstance(x, str):
        x = x.replace('$', '').replace(',', '').strip()
        if '(' in x and ')' in x:
            x = '-' + x.replace('(', '').replace(')', '')
    return pd.to_numeric(x, errors='coerce')


# Safety Column Fixes
if 'claim ID' in charges.columns:
    charges.rename(columns={'claim ID': 'Claim ID'}, inplace=True)

charges['Service Charge Amount'] = charges['Service Charge Amount'].apply(clean_curr).fillna(0)
charges['Date Of Service'] = pd.to_datetime(charges['Date Of Service'])

# 2. FILTER LAST 12 MONTHS
# -------------------------
end_date = charges['Date Of Service'].max()
start_date = end_date - pd.DateOffset(months=12)

last_12_months = charges[
    (charges['Date Of Service'] >= start_date) &
    (charges['Date Of Service'] <= end_date)
].copy()

# 3. GROUP BY MONTH
# ------------------
monthly_charges = last_12_months.groupby(
    pd.Grouper(key='Date Of Service', freq='MS')
)['Service Charge Amount'].sum()

# 4. CREATE DISPLAY TABLE
# ------------------------
monthly_df = pd.DataFrame({
    'Month': monthly_charges.index.strftime('%B %Y'),
    'Total Charges': monthly_charges.values
})
monthly_df['Total Charges'] = monthly_df['Total Charges'].apply(lambda x: f"${x:,.2f}")

print("=" * 50)
print("  MONTHLY SERVICE CHARGES - LAST 12 MONTHS")
print("=" * 50)
print(monthly_df.to_string(index=False))
print("-" * 50)
print(f"Grand Total: ${monthly_charges.sum():,.2f}")
print(f"Monthly Average: ${monthly_charges.mean():,.2f}")
print("=" * 50)

# 5. VISUALIZATION
# -----------------
plt.figure(figsize=(14, 6))

bars = plt.bar(
    range(len(monthly_charges)),
    monthly_charges.values,
    color='#2ca02c',
    edgecolor='darkgreen',
    alpha=0.8
)

# Add value labels on bars
for i, bar in enumerate(bars):
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2., height,
             f'${height / 1000:.1f}K',
             ha='center', va='bottom', fontsize=9, fontweight='bold')

# X-axis labels
plt.xticks(range(len(monthly_charges)),
           monthly_charges.index.strftime('%b %Y'),
           rotation=45, ha='right')

# Formatting
plt.title('Monthly Service Charges - Last 12 Months', fontsize=16, fontweight='bold')
plt.ylabel('Total Charges ($)', fontsize=12)
plt.xlabel('Month', fontsize=12)

# Format Y axis
current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels(['${:,.0f}'.format(x) for x in current_values])

# Add average line
avg = monthly_charges.mean()
plt.axhline(y=avg, color='red', linestyle='--', linewidth=2, label=f'Monthly Avg: ${avg:,.0f}')
plt.legend()

plt.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.show()


# ==========================================
# MONTHLY REVENUE ANALYSIS - LAST 12 MONTHS
# Charges, Adjustments, and Collections
# ==========================================
import pandas as pd

# 1. LOAD DATA
# -------------
charges = pd.read_csv('260106 Charges Report CSV.csv')

# Safety Column Fixes
if 'claim ID' in charges.columns:
    charges.rename(columns={'claim ID': 'Claim ID'}, inplace=True)


# Clean Currency Function
def clean_curr(x):
    if isinstance(x, str):
        x = x.replace('$', '').replace(',', '').strip()
        if '(' in x and ')' in x:
            x = '-' + x.replace('(', '').replace(')', '')
    return pd.to_numeric(x, errors='coerce')


# 2. CLEAN ALL FINANCIAL COLUMNS
# -------------------------------
charges['Service Charge Amount'] = charges['Service Charge Amount'].apply(clean_curr).fillna(0)
charges['Date Of Service'] = pd.to_datetime(charges['Date Of Service'])

# Adjustment columns (Contract adjustments)
adj_cols = ['Pri Ins Insurance Contract Adjustment', 'Sec Ins Insurance Contract Adjustment',
            'Other Ins Insurance Contract Adjustment']
charges['Total_Contract_Adj'] = 0
for col in adj_cols:
    if col in charges.columns:
        charges['Total_Contract_Adj'] += charges[col].apply(clean_curr).fillna(0)

# Payment columns - Insurance (CORRECTED column names)
ins_payment_cols = ['Pri Ins Insurance Payment', 'Sec Ins Insurance Payment', 'Other Ins Insurance Payment']
charges['Total_Ins_Payments'] = 0
for col in ins_payment_cols:
    if col in charges.columns:
        charges['Total_Ins_Payments'] += charges[col].apply(clean_curr).fillna(0)

# Patient payments
if 'Pat Payment Amount' in charges.columns:
    charges['Pat_Payments'] = charges['Pat Payment Amount'].apply(clean_curr).fillna(0)
else:
    charges['Pat_Payments'] = 0

# Total collected (Insurance + Patient)
charges['Total_Collected'] = charges['Total_Ins_Payments'] + charges['Pat_Payments']

# Adjusted Amount (Service Charge - Adjustment = Allowed/Expected)
charges['Adjusted_Amount'] = charges['Service Charge Amount'] - charges['Total_Contract_Adj']

# 3. FILTER LAST 12 MONTHS
# -------------------------
end_date = charges['Date Of Service'].max()
start_date = end_date - pd.DateOffset(months=12)

last_12_months = charges[
    (charges['Date Of Service'] >= start_date) &
    (charges['Date Of Service'] <= end_date)
].copy()

# 4. GROUP BY MONTH
# ------------------
monthly = last_12_months.groupby(
    pd.Grouper(key='Date Of Service', freq='MS')
).agg({
    'Service Charge Amount': 'sum',
    'Total_Contract_Adj': 'sum',
    'Adjusted_Amount': 'sum',
    'Total_Collected': 'sum'
}).reset_index()

# 5. CALCULATE PERCENTAGES
# -------------------------
monthly['Adjustment %'] = (monthly['Total_Contract_Adj'] / monthly['Service Charge Amount'] * 100).round(1)
monthly['Collected % of Charges'] = (monthly['Total_Collected'] / monthly['Service Charge Amount'] * 100).round(1)
monthly['Collected % of Adjusted'] = (monthly['Total_Collected'] / monthly['Adjusted_Amount'] * 100).round(1)

# 6. FORMAT FOR DISPLAY
# ----------------------
display_df = pd.DataFrame({
    'Month': monthly['Date Of Service'].dt.strftime('%b %Y'),
    'Svc Charges': monthly['Service Charge Amount'].apply(lambda x: f"${x:,.0f}"),
    'Adjustment': monthly['Total_Contract_Adj'].apply(lambda x: f"${x:,.0f}"),
    'Adj %': monthly['Adjustment %'].apply(lambda x: f"{x:.1f}%"),
    'Adj Charges': monthly['Adjusted_Amount'].apply(lambda x: f"${x:,.0f}"),
    'Collected': monthly['Total_Collected'].apply(lambda x: f"${x:,.0f}"),
    '% of Svc': monthly['Collected % of Charges'].apply(lambda x: f"{x:.1f}%"),
    '% of Adj': monthly['Collected % of Adjusted'].apply(lambda x: f"{x:.1f}%")
})

# 7. PRINT TABLE
# ---------------
print("=" * 110)
print("  MONTHLY REVENUE ANALYSIS - LAST 12 MONTHS")
print("=" * 110)
print(display_df.to_string(index=False))
print("-" * 110)

# Totals
total_charges = monthly['Service Charge Amount'].sum()
total_adj = monthly['Total_Contract_Adj'].sum()
total_adjusted = monthly['Adjusted_Amount'].sum()
total_collected = monthly['Total_Collected'].sum()

print(f"{'TOTALS:':<10} ${total_charges:>10,.0f}  ${total_adj:>10,.0f}  "
      f"{(total_adj/total_charges*100):>5.1f}%  ${total_adjusted:>10,.0f}  ${total_collected:>10,.0f}  "
      f"{(total_collected/total_charges*100):>5.1f}%  "
      f"{(total_collected/total_adjusted*100):>5.1f}%")
print("=" * 110)
