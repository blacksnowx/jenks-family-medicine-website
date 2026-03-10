# ===========================================
# REVENUE DASHBOARD - Flask Web Application
# ===========================================
from flask import Flask, render_template
import pandas as pd
import numpy as np
from datetime import timedelta
import json
from statsmodels.tsa.holtwinters import ExponentialSmoothing

app = Flask(__name__)

# --- CONFIGURATION ---
CLINICAL_FILE = 'Charges Export CSV.csv'
VA_FILE = '201 Bills and Payments.csv'
FORECAST_WEEKS = 26
SEASONAL_PERIOD = 4

# Fees for VA Data
VA_FEES = {
    'GM_DBQs': {'0': 0, '1-5': 250, '6-10': 330, '11-15': 410, '16+': 550},
    'Focused': {'0': 0, '1-5': 200, '6-10': 280, '11-15': 370, '16+': 480},
    'TBI': 250,
    'IMO': {'0': 0, '1-3': 150, '4-6': 260, '7-9': 330, '10-12': 350, '13-15': 465, '16-18': 525, '19+': 600},
    'No_Show': {'0': 0, '1': 60}
}
DATA_CUTOFF = '2026-01-16'     # For Forecast Models (Lag adjustment)
REPORTING_CUTOFF = '2026-01-16' # For Historical Tables/Charts (Last Full Week)
REPORTING_START = '2025-01-01'  # Strict Start Date (No Dec 2024 included)


def clean_currency(x):
    """Clean currency strings to numeric values"""
    if isinstance(x, str):
        x = x.replace('$', '').replace(',', '').strip()
        if '(' in x and ')' in x:
            x = '-' + x.replace('(', '').replace(')', '')
    return pd.to_numeric(x, errors='coerce')


def load_clinical_data():
    """Load and clean Clinical Charges Report"""
    try:
        df = pd.read_csv(CLINICAL_FILE)
    except FileNotFoundError:
        return pd.DataFrame()

    if 'claim ID' in df.columns:
        df.rename(columns={'claim ID': 'Claim ID'}, inplace=True)

    # Clean Financials
    df['Service Charge Amount'] = df['Service Charge Amount'].apply(clean_currency).fillna(0)
    df['Date Of Service'] = pd.to_datetime(df['Date Of Service'], errors='coerce')

    # Adjustments
    adj_cols = ['Pri Ins Insurance Contract Adjustment', 'Sec Ins Insurance Contract Adjustment',
                'Other Ins Insurance Contract Adjustment']
    df['Total_Contract_Adj'] = 0
    for col in adj_cols:
        if col in df.columns:
            df['Total_Contract_Adj'] += df[col].apply(clean_currency).fillna(0)

    # Payments
    pay_cols = ['Pri Ins Insurance Payment', 'Sec Ins Insurance Payment', 'Other Ins Insurance Payment']
    df['Total_Ins_Payments'] = 0
    for col in pay_cols:
        if col in df.columns:
            df['Total_Ins_Payments'] += df[col].apply(clean_currency).fillna(0)

    if 'Pat Payment Amount' in df.columns:
        df['Pat_Payments'] = df['Pat Payment Amount'].apply(clean_currency).fillna(0)
    else:
        df['Pat_Payments'] = 0

    # Calculated Fields
    df['Total_Collected'] = df['Total_Ins_Payments'] + df['Pat_Payments']
    df['Allowed Amount'] = df['Service Charge Amount'] - df['Total_Contract_Adj']
    df['Adjusted_Amount'] = df['Allowed Amount']  # Alias for consistency

    # Provider Normalization
    df['Rendering Provider'] = df['Rendering Provider'].astype(str).str.upper().str.strip()
    provider_map = {
        'EHRIN IRVIN, FNP': 'Ehrin Irvin',
        'ANNE JENKS, APRN': 'Anne Jenks',
        'SARAH SUGGS, NP': 'Sarah Suggs',
        'DAVID REDD, MD': 'Anne Jenks',
        'HEATHER MAYO, FNP': 'Heather Mayo'
    }
    df['Normalized_Provider'] = df['Rendering Provider'].map(provider_map).fillna('Other')

    return df


def load_va_data():
    """Load and process VA Bills and Payments"""
    try:
        df = pd.read_csv(VA_FILE)
    except FileNotFoundError:
        return pd.DataFrame()

    df = df.rename(columns={'Unnamed: 0': '201 ID', 'Sarah Suggs ': 'Provider'})
    df['Date of Service'] = pd.to_datetime(df['Date of Service'].ffill(), errors='coerce')
    df = df.dropna(subset=['Date of Service']).fillna('0')

    # Clean Provider Names
    df['Provider'] = df['Provider'].str.strip().replace({
        '0': 'Anne Jenks', 'sArah Suggs': 'Sarah Suggs', 'SArah Suggs': 'Sarah Suggs'
    })

    # Fee Calculation Logic
    for col in ['Focused DBQs', 'Routine IMOs', 'Gen Med DBQs', 'No Show', 'TBI']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    dbq_map = {'Visual': '0', 'r-tbi': '0', '1--5': '1-5', 'bp check': '0', 'nan': '0', '': '0'}
    imo_map = {'7/9': '7-9', '1/3': '1-3', '0': '0', '1-5': '1-3', '1': '1-3', '4': '4-6', '7': '7-9', '6-10': '4-6'}
    gen_med_map = {'0': '0', 'BP CHECK': '0', '11-13': '11-15', '1-6': '1-5', '7': '6-10', '6': '6-10', '??': '0'}

    if 'Focused DBQs' in df.columns: df['Focused DBQs'] = df['Focused DBQs'].replace(dbq_map)
    if 'Routine IMOs' in df.columns: df['Routine IMOs'] = df['Routine IMOs'].replace(imo_map)
    if 'Gen Med DBQs' in df.columns: df['Gen Med DBQs'] = df['Gen Med DBQs'].replace(gen_med_map)

    def calculate_fees(row):
        f_fee = VA_FEES['Focused'].get(row.get('Focused DBQs', '0'), 0)
        imo_fee = VA_FEES['IMO'].get(row.get('Routine IMOs', '0'), 0)
        gmed_fee = VA_FEES['GM_DBQs'].get(row.get('Gen Med DBQs', '0'), 0)
        noshow_fee = VA_FEES['No_Show'].get(row.get('No Show', '0'), 0)
        tbi_fee = 250 if row.get('TBI', '0') not in ['0', 'nan', '', 'no', 'false'] else 0
        return f_fee + imo_fee + gmed_fee + noshow_fee + tbi_fee

    df['Fees'] = df.apply(calculate_fees, axis=1)
    
    # Calculate Service Type for Analytics
    def get_service_type(row):
        if row.get('TBI') not in ['0', 'nan', '', 'no', 'false']: return 'TBI'
        if row.get('Focused DBQs') != '0' or row.get('Gen Med DBQs') != '0': return 'DBQ'
        if row.get('Routine IMOs') != '0': return 'IMO'
        return 'Other'
    
    df['Service_Type'] = df.apply(get_service_type, axis=1)
    
    return df


def get_4_quarter_summary(clinical_df, va_df):
    """Calculate KPIs for all 4 relevant quarters (Q1-Q4 2025)"""
    cutoff = pd.to_datetime(REPORTING_CUTOFF).replace(hour=23, minute=59, second=59)
    # Always show 2025 quarters for annual performance review
    target_year = 2025
    
    quarters = []
    # Loop Q1, Q2, Q3, Q4
    for q in [1, 2, 3, 4]:
        q_start = pd.Timestamp(target_year, (q - 1) * 3 + 1, 1)
        q_end = (q_start + pd.DateOffset(months=3)) - pd.Timedelta(seconds=1)
        
        # Filter Data
        clin_data = clinical_df[(clinical_df['Date Of Service'] >= q_start) & (clinical_df['Date Of Service'] <= q_end)]
        va_data = pd.DataFrame()
        if not va_df.empty:
            va_data = va_df[(va_df['Date of Service'] >= q_start) & (va_df['Date of Service'] <= q_end)]
        
        # Calculate Metrics
        clin_rev = clin_data['Allowed Amount'].sum()
        va_rev = va_data['Fees'].sum() if not va_data.empty else 0
        total_rev = clin_rev + va_rev
        
        clin_charges = clin_data['Service Charge Amount'].sum()
        
        # KPI: Collection Rating (Net / Gross for Clinical)
        # Or Total Revenue per week maybe? Let's stick to Revenue + Coll Rate
        coll_rate = (clin_rev / clin_charges * 100) if clin_charges > 0 else 0
        
        quarters.append({
            'label': f"Q{q} '{str(target_year)[2:]}",
            'revenue': total_rev,
            'coll_rate': coll_rate,
            'is_future': q_start > cutoff # Mark future quarters if applicable
        })
        
    return quarters

def get_va_analytics(va_df):
    """VA Specific Stats (Last Full Month Cutoff)"""
    if va_df.empty: return [], {}, []
    
    # Enforce Reporting Cutoff
    cutoff = pd.to_datetime(REPORTING_CUTOFF)
    start_date = pd.to_datetime(REPORTING_START)
    
    filtered = va_df[(va_df['Date of Service'] >= start_date) & (va_df['Date of Service'] <= cutoff)].copy()

    # Monthly Trend (Chart)
    monthly = filtered.groupby(pd.Grouper(key='Date of Service', freq='ME'))['Fees'].sum()
    trend = [{'date': d.strftime('%Y-%m'), 'value': v} for d, v in monthly.items()]
    
    # Provider Quarterly Breakdown (Table)
    # Group by Quarter (e.g., 2025Q1, 2025Q2)
    filtered['Quarter'] = filtered['Date of Service'].dt.to_period('Q').astype(str)
    
    # Pivot: Provider x Quarter
    prov_qtly = filtered.groupby(['Provider', 'Quarter'])['Fees'].sum().unstack(fill_value=0)
    
    # Convert to list of dicts for easy template rendering
    # Structure: {'Provider': 'Name', 'Q1': 123, 'Q2': 456...}
    cols = prov_qtly.columns.tolist()
    prov_data = []
    for prov in prov_qtly.index:
        row = {'Provider': prov}
        for col in cols:
            row[col] = prov_qtly.loc[prov, col]
        row['Total'] = prov_qtly.loc[prov].sum()
        prov_data.append(row)
        
    # Sort by Total
    prov_data.sort(key=lambda x: x['Total'], reverse=True)
    
    return trend, prov_data, cols


def get_merged_rpv_data(charges_df):
    """Create merged encounters (Patient + Date) for RPV Analysis"""
    df = charges_df.copy()
    
    # Enforce Cutoff
    cutoff = pd.to_datetime(REPORTING_CUTOFF)
    end_date = cutoff
    start_date = pd.to_datetime(REPORTING_START)
    
    target_providers = ['Anne Jenks', 'Ehrin Irvin', 'Sarah Suggs', 'Heather Mayo']
    df = df[(df['Normalized_Provider'].isin(target_providers)) & 
            (df['Date Of Service'] >= start_date) &
            (df['Date Of Service'] <= end_date)].copy()

    today = pd.Timestamp.now()
    if 'Pri Ins Adjudication Date' in df.columns:
        df['Pri Ins Adjudication Date'] = pd.to_datetime(df['Pri Ins Adjudication Date'], errors='coerce')
        pending_mask = (df['Date Of Service'] > (today - pd.Timedelta(days=45))) & (pd.isnull(df['Pri Ins Adjudication Date']))
        df = df[~pending_mask]

    # Group by Encounter ID (or Claim ID if Encounter is missing) for RPV
    group_cols = ['Normalized_Provider', 'Date Of Service']
    if 'Encounter ID' in df.columns:
        group_cols.append('Encounter ID')
    elif 'Claim ID' in df.columns:
        group_cols.append('Claim ID')
    
    encounters = df.groupby(group_cols).agg({
        'Allowed Amount': 'sum',
        'Service Charge Amount': 'sum'
    }).reset_index()

    valid_encounters = encounters[encounters['Allowed Amount'] > 25].copy()
    
    prov_stats = valid_encounters.groupby('Normalized_Provider').agg({
        'Allowed Amount': ['count', 'mean', 'sum'],
        'Service Charge Amount': 'mean'
    }).reset_index()
    prov_stats.columns = ['Provider', 'Visit_Count', 'Avg_RPV', 'Total_Rev', 'Avg_Billed']
    
    prov_stats['Avg_RPV'] = prov_stats['Avg_RPV'].round(0)
    prov_stats['Avg_Billed'] = prov_stats['Avg_Billed'].round(0)
    prov_stats['Total_Rev'] = prov_stats['Total_Rev'].round(0)

    weekly_trend = valid_encounters.groupby(pd.Grouper(key='Date Of Service', freq='W-MON'))['Allowed Amount'].mean()
    
    return prov_stats.to_dict('records'), weekly_trend


def get_total_forecast(clinical_df, va_df):
    """Combine Clinical and VA revenue for total practice forecast"""
    clinical = clinical_df[['Date Of Service', 'Allowed Amount']].rename(columns={'Allowed Amount': 'Revenue'}).copy()
    
    if not va_df.empty:
        va = va_df[['Date of Service', 'Fees']].rename(columns={'Date of Service': 'Date Of Service', 'Fees': 'Revenue'}).copy()
        total = pd.concat([clinical, va], ignore_index=True)
    else:
        total = clinical

    # DATA CUTOFF APPLIED HERE for forecast accuracy
    cutoff = pd.to_datetime(DATA_CUTOFF)
    end_date = cutoff
    start_date = end_date - pd.DateOffset(months=18)
    
    ts_data = total[(total['Date Of Service'] >= start_date) & (total['Date Of Service'] <= end_date)]
    weekly_rev = ts_data.groupby(pd.Grouper(key='Date Of Service', freq='W-MON'))['Revenue'].sum().asfreq('W-MON', fill_value=0)

    try:
        model = ExponentialSmoothing(
            weekly_rev, trend='add', seasonal='add', seasonal_periods=4, damped_trend=True
        ).fit()
        forecast = model.forecast(FORECAST_WEEKS)
        
        last_date = weekly_rev.index[-1]
        dates = [last_date + timedelta(weeks=x) for x in range(1, FORECAST_WEEKS + 1)]
        forecast_series = pd.Series(forecast.values, index=dates)

        return (
            [{'date': d.strftime('%Y-%m-%d'), 'value': round(v, 2)} for d, v in weekly_rev.items()],
            [{'date': d.strftime('%Y-%m-%d'), 'value': round(v, 2)} for d, v in forecast_series.items()],
            {'avg_hist': weekly_rev.mean(), 'avg_fore': forecast_series.mean()}
        )
    except:
        return [], [], {'avg_hist': 0, 'avg_fore': 0}


def get_provider_trends(clinical_df, va_df):
    """Monthly revenue by provider (Clinical + VA)"""
    clinical = clinical_df[['Date Of Service', 'Normalized_Provider', 'Allowed Amount']].copy()
    clinical.columns = ['Date', 'Provider', 'Revenue']
    
    if not va_df.empty:
        va = va_df[['Date of Service', 'Provider', 'Fees']].copy()
        va.columns = ['Date', 'Provider', 'Revenue']
        combined = pd.concat([clinical, va])
    else:
        combined = clinical



    # Enforce Cutoff
    cutoff = pd.to_datetime(REPORTING_CUTOFF)
    start_date = pd.to_datetime(REPORTING_START)
    combined = combined[(combined['Date'] >= start_date) & (combined['Date'] <= cutoff)]

    monthly = combined.groupby([pd.Grouper(key='Date', freq='ME'), 'Provider'])['Revenue'].sum().unstack(fill_value=0)
    monthly.index = monthly.index.strftime('%Y-%m')
    
    return monthly.to_dict('index')


def get_monthly_analysis(df):
    # Enforce Cutoff
    cutoff = pd.to_datetime(REPORTING_CUTOFF)
    end_date = cutoff
    start_date = pd.to_datetime(REPORTING_START)
    filtered = df[(df['Date Of Service'] >= start_date) & (df['Date Of Service'] <= end_date)].copy()
    
    # Determine Encounter Column
    enc_col = 'Claim ID' if 'Claim ID' in filtered.columns else None
    if 'Encounter ID' in filtered.columns: enc_col = 'Encounter ID'
    
    agg_dict = {
        'Service Charge Amount': 'sum', 'Total_Contract_Adj': 'sum', 
        'Adjusted_Amount': 'sum', 'Total_Collected': 'sum'
    }
    if enc_col: agg_dict[enc_col] = 'nunique'

    monthly = filtered.groupby(pd.Grouper(key='Date Of Service', freq='MS')).agg(agg_dict).reset_index()
    
    if enc_col:
        monthly.rename(columns={enc_col: 'Encounters'}, inplace=True)
    else:
        monthly['Encounters'] = 0

    monthly['Adj_Pct'] = (monthly['Total_Contract_Adj'] / monthly['Service Charge Amount'] * 100).round(1)
    monthly['Collected_Pct_Charges'] = (monthly['Total_Collected'] / monthly['Service Charge Amount'] * 100).round(1)
    monthly['Collected_Pct_Adj'] = (monthly['Total_Collected'] / monthly['Adjusted_Amount'] * 100).round(1)
    monthly['Charges_Per_Encounter'] = (monthly['Service Charge Amount'] / monthly['Encounters']).fillna(0).round(0)
    monthly['Month'] = monthly['Date Of Service'].dt.strftime('%b %Y')
    
    totals = {
        'total_charges': monthly['Service Charge Amount'].sum(),
        'total_adj': monthly['Total_Contract_Adj'].sum(),
        'total_adjusted': monthly['Adjusted_Amount'].sum(),
        'total_collected': monthly['Total_Collected'].sum(),
        'total_encounters': monthly['Encounters'].sum()
    }
    for k in ['total_charges', 'total_adjusted', 'total_encounters']: 
        if totals[k] == 0: totals[k] = 1 

    totals['adj_pct'] = totals['total_adj'] / totals['total_charges'] * 100
    totals['collected_pct_charges'] = totals['total_collected'] / totals['total_charges'] * 100
    totals['collected_pct_adj'] = totals['total_collected'] / totals['total_adjusted'] * 100
    totals['avg_charges_per_encounter'] = totals['total_charges'] / totals['total_encounters']

    return monthly.to_dict('records'), totals


def get_collections_deep_dive(df):
    """Analyze Payers and Codes for Root Cause of Low Collections"""
    # Enforce Cutoff
    cutoff = pd.to_datetime(REPORTING_CUTOFF)
    start_date = pd.to_datetime(REPORTING_START)
    filtered = df[(df['Date Of Service'] >= start_date) & (df['Date Of Service'] <= cutoff)].copy()
    
    enc_col = 'Claim ID' if 'Claim ID' in filtered.columns else 'Encounter ID' if 'Encounter ID' in filtered.columns else None

    def aggregate_and_score(groupby_col, limit=15):
        agg_dict = {
            'Service Charge Amount': 'sum', 'Total_Contract_Adj': 'sum', 
            'Allowed Amount': 'sum', 'Total_Collected': 'sum'
        }
        if enc_col: agg_dict[enc_col] = 'nunique'
        
        grouped = filtered.groupby(groupby_col).agg(agg_dict).reset_index()
        
        # Rename and Clean
        if enc_col: grouped.rename(columns={enc_col: 'Encounters'}, inplace=True)
        else: grouped['Encounters'] = 0
            
        grouped.rename(columns={groupby_col: 'Name'}, inplace=True)
        
        # Metrics
        grouped['Adj_Pct'] = (grouped['Total_Contract_Adj'] / grouped['Service Charge Amount'] * 100).fillna(0).round(1)
        grouped['Collection_Pct_Net'] = (grouped['Total_Collected'] / grouped['Allowed Amount'] * 100).fillna(0).round(1)
        grouped['Avg_Allowed'] = (grouped['Allowed Amount'] / grouped['Encounters']).fillna(0).round(0)
        
        # Sort by Volume (Charges)
        grouped = grouped.sort_values('Service Charge Amount', ascending=False).head(limit)
        
        return grouped.to_dict('records')

    payer_stats = aggregate_and_score('Pri Ins Company Name')
    code_stats = aggregate_and_score('Procedure Code', limit=20)
    
    return payer_stats, code_stats


def get_primary_care_forecast(df):
    # Forecasting Service Charge Amount
    # DATA CUTOFF APPLIED HERE
    cutoff = pd.to_datetime(DATA_CUTOFF)
    end_date = cutoff
    start_date = end_date - pd.DateOffset(months=18)
    filtered = df[(df['Date Of Service'] >= start_date) & (df['Date Of Service'] <= end_date)]
    
    weekly = filtered.groupby(pd.Grouper(key='Date Of Service', freq='W-MON'))['Service Charge Amount'].sum().asfreq('W-MON', fill_value=0)
    
    try:
        model = ExponentialSmoothing(weekly, trend='add', seasonal='add', seasonal_periods=4, damped_trend=True).fit()
        forecast = model.forecast(FORECAST_WEEKS)
        dates = [weekly.index[-1] + timedelta(weeks=x) for x in range(1, FORECAST_WEEKS + 1)]
        return (
            [{'date': d.strftime('%Y-%m-%d'), 'value': round(v, 2)} for d, v in weekly.items()],
            [{'date': d.strftime('%Y-%m-%d'), 'value': round(v, 2)} for d, v in pd.Series(forecast.values, index=dates).items()]
        )
    except:
        return [], []


@app.route('/')
def dashboard():
    clinical_df = load_clinical_data()
    va_df = load_va_data()

    # 1. Executive Summary
    exec_summary = get_4_quarter_summary(clinical_df, va_df)
    
    # 2. Primary Care (Clinical)
    # --------------------------
    monthly_data, totals = get_monthly_analysis(clinical_df)
    pc_hist, pc_fore = get_primary_care_forecast(clinical_df)
    rpv_stats, rpv_trend_series = get_merged_rpv_data(clinical_df)
    rpv_trend = [{'date': d.strftime('%Y-%m-%d'), 'value': round(v, 0)} for d, v in rpv_trend_series.items()]
    
    # Chart Data for PC
    pc_chart_labels = [m['Month'] for m in monthly_data]
    pc_chart_values = [m['Service Charge Amount'] for m in monthly_data]
    pc_chart_avg = sum(pc_chart_values) / len(pc_chart_values) if pc_chart_values else 0

    # 2b. Collections Deep Dive
    payer_stats, code_stats = get_collections_deep_dive(clinical_df)

    # 3. VA Analytics
    # ---------------
    va_trend, va_prov, va_quarters = get_va_analytics(va_df)

    # 4. Combined View
    # ----------------
    prov_trends = get_provider_trends(clinical_df, va_df)
    tot_hist, tot_fore, tot_summary = get_total_forecast(clinical_df, va_df)

    return render_template('dashboard.html',
                           # Executive
                           exec_summary=exec_summary,
                           
                           # Primary Care
                           monthly_data=monthly_data, totals=totals,
                           pc_chart_labels=json.dumps(pc_chart_labels), pc_chart_values=json.dumps(pc_chart_values), pc_chart_avg=pc_chart_avg,
                           pc_hist=json.dumps(pc_hist), pc_fore=json.dumps(pc_fore),
                           rpv_stats=rpv_stats, rpv_trend=json.dumps(rpv_trend),
                           payer_stats=payer_stats, code_stats=code_stats,
                           
                           # VA
                           va_trend=json.dumps(va_trend), va_prov=va_prov, va_quarters=va_quarters,
                           
                           # Combined
                           prov_trends=json.dumps(prov_trends),
                           tot_hist=json.dumps(tot_hist), tot_fore=json.dumps(tot_fore), tot_summary=tot_summary)

if __name__ == '__main__':
    print("Starting CEO Dashboard...")
    app.run(debug=True, port=5000)
