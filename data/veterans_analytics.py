import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from scipy import stats as scipy_stats
import os

try:
    from . import data_loader
except ImportError:
    import data_loader

IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'images')
if not os.path.exists(IMAGES_DIR):
    os.makedirs(IMAGES_DIR)

def get_combined_veterans_volume(df_va, chart_start_date='2025-01-01', provider_cap=18):
    """
    Computes combined weekly veteran volume for Sarah Suggs and Heather Mayo.
    Returns dictionary with aggregate stats, weekly dataframe, and monthly dataframe.
    """
    # Ensure standard schema if 'Patient_ID' is missing, fallback to whatever is first column in data loader
    if 'Patient_ID' not in df_va.columns and len(df_va.columns) > 0:
        # Standardize 'Patient_ID' to evaluate appointments count
        patient_col = [c for c in df_va.columns if 'Provider' not in c and 'Date' not in c and 'Week' not in c][0]
        df_va = df_va.rename(columns={patient_col: 'Patient_ID'})
        
    df_va['Is_Suggs'] = df_va['Provider'].str.lower().str.contains('sarah suggs', na=False)
    df_va['Is_Mayo'] = df_va['Provider'].str.lower().str.contains('heather mayo', na=False)
    
    if 'No Show' in df_va.columns:
        df_va['Is_NoShow'] = df_va['No Show'].apply(lambda x: str(x).strip() in ['1', '1.0'] if pd.notna(x) else False)
    else:
        df_va['Is_NoShow'] = False
        
    chart_start = pd.Timestamp(chart_start_date)
    df_period = df_va[df_va['Date of Service'] >= chart_start].copy()
    
    # Deduplicate: one row per patient per date per provider = one appointment
    if 'Patient_ID' in df_period.columns:
        df_deduped = df_period.drop_duplicates(subset=['Patient_ID', 'Date of Service', 'Provider'], keep='first').copy()
    else:
        # fallback if patient ID wasn't properly assigned, just don't deduplicate by it
        df_deduped = df_period.copy()
        
    # We already have a 'Week' column ending Friday from data_loader
    if 'Week' not in df_deduped.columns:
         df_deduped['Week'] = data_loader.get_week_ending_friday(df_deduped['Date of Service'])
         
    # Suggs weekly
    suggs_wk = df_deduped[df_deduped['Is_Suggs']].groupby('Week').agg(
        Suggs_Appts=('Date of Service', 'count'),
        Suggs_NoShows=('Is_NoShow', 'sum'),
    ).reset_index()
    
    # Mayo weekly
    mayo_wk = df_deduped[df_deduped['Is_Mayo']].groupby('Week').agg(
        Mayo_Appts=('Date of Service', 'count'),
        Mayo_NoShows=('Is_NoShow', 'sum'),
    ).reset_index()
    
    # All providers weekly
    all_wk = df_deduped.groupby('Week').agg(
        All_Appts=('Date of Service', 'count'),
    ).reset_index()
    
    # Merge
    combined = pd.merge(all_wk, suggs_wk, on='Week', how='left')
    combined = pd.merge(combined, mayo_wk, on='Week', how='left')
    combined = combined.fillna(0).sort_values('Week')
    
    if len(combined) == 0:
        return {'total_suggs': 0, 'total_mayo': 0, 'total_combined': 0, 'total_all': 0, 'avg_week': 0.0, 'n_weeks': 0}, pd.DataFrame(), pd.DataFrame()
        
    combined['Suggs_Raw'] = combined['Suggs_Appts'].copy()
    combined['Mayo_Raw'] = combined['Mayo_Appts'].copy()
    combined['Suggs_Appts'] = combined['Suggs_Appts'].clip(upper=provider_cap)
    combined['Mayo_Appts'] = combined['Mayo_Appts'].clip(upper=provider_cap)
    
    combined['Combined_Appts'] = combined['Suggs_Appts'] + combined['Mayo_Appts']
    combined['Combined_NoShows'] = combined['Suggs_NoShows'] + combined['Mayo_NoShows']
    combined['Others'] = combined['All_Appts'] - combined['Suggs_Raw'] - combined['Mayo_Raw']
    
    # Monthly
    combined['Month'] = combined['Week'].dt.to_period('M')
    monthly = combined.groupby('Month').agg(
        Suggs=('Suggs_Appts', 'sum'),
        Mayo=('Mayo_Appts', 'sum'),
        All=('All_Appts', 'sum'),
    ).reset_index()
    monthly['Combined'] = monthly['Suggs'] + monthly['Mayo']
    
    stats = {
        'total_suggs': int(combined['Suggs_Appts'].sum()),
        'total_mayo': int(combined['Mayo_Appts'].sum()),
        'total_combined': int(combined['Combined_Appts'].sum()),
        'total_all': int(combined['All_Appts'].sum()),
        'avg_week': float(combined['Combined_Appts'].mean()),
        'n_weeks': len(combined)
    }
    
    return stats, combined, monthly

def get_va_forecast(df_va):
    """
    Groups VA revenue into half-month blocks (H1: 1-15, H2: 16-end) to forecast 
    the four upcoming Mid-Month and Month-End payments.
    """
    df = df_va.copy()
    if 'VA_Revenue' not in df.columns:
        df['VA_Revenue'] = df.apply(data_loader.calculate_va_fee, axis=1)
        
    df['Month_Period'] = df['Date of Service'].dt.to_period('M')
    df['Day'] = df['Date of Service'].dt.day
    df['HalfMonth'] = df.apply(
        lambda r: f"{r['Date of Service'].year}-{r['Date of Service'].month:02d}-H1" if r['Day'] <= 15 
        else f"{r['Date of Service'].year}-{r['Date of Service'].month:02d}-H2", axis=1
    )
    
    hm_aggs = df.groupby('HalfMonth')['VA_Revenue'].sum().reset_index().sort_values('HalfMonth')
    return hm_aggs

def plot_veterans_volume(combined, monthly, cap=28):
    """
    Generates the stacked bar chart for weekly veteran volume and saves it securely.
    """
    if len(combined) == 0:
        return None
        
    fig, axes = plt.subplots(3, 2, figsize=(18, 17))
    fig.suptitle('Combined (Suggs + Mayo) — Weekly Veteran Patient Volume', fontsize=16, fontweight='bold', y=0.99)
    
    # Chart A: Stacked weekly by provider
    ax = axes[0, 0]
    ax.bar(combined['Week'], combined['Suggs_Appts'], color='#3b82f6', alpha=0.8, label='Suggs', width=pd.Timedelta(days=5))
    ax.bar(combined['Week'], combined['Mayo_Appts'], bottom=combined['Suggs_Appts'], color='#10b981', alpha=0.8, label='Mayo', width=pd.Timedelta(days=5))
    if len(combined) >= 4:
        ma = combined['Combined_Appts'].rolling(4, min_periods=1).mean()
        ax.plot(combined['Week'], ma, color='#f59e0b', linewidth=2.5, label='4-wk MA')
    ax.axhline(y=cap, color='#ef4444', linewidth=2, linestyle='--', alpha=0.7, label=f'Cap ({cap})')
    ax.set_xlabel('Week')
    ax.set_ylabel('Appointments')
    ax.legend()
    
    save_path = os.path.join(IMAGES_DIR, 'combined_veterans_weekly.png')
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    return save_path
