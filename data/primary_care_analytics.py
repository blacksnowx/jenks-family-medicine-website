import pandas as pd
import numpy as np
from scipy import stats as scipy_stats
import os

try:
    from . import data_loader
except ImportError:
    import data_loader

IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'images')

def calculate_patient_metrics(df_pc):
    """
    Computes first visits, cumulative patient panel, and aggregated monthly metrics.
    Assumes df_pc is the output of data_loader.load_pc_data()
    """
    df = df_pc.dropna(subset=['Date Of Service', 'Patient ID']).copy()
    
    # Determine each patient's first visit
    first_visit = df.groupby('Patient ID')['Date Of Service'].min().reset_index()
    first_visit.columns = ['Patient ID', 'First Visit']
    first_visit['First Visit Week'] = data_loader.get_week_ending_friday(first_visit['First Visit'])
    
    # Count new patients per week and build cumulative total
    new_patients_weekly = first_visit.groupby('First Visit Week').size().reset_index(name='New_Patients')
    new_patients_weekly.rename(columns={'First Visit Week': 'Week'}, inplace=True)
    new_patients_weekly = new_patients_weekly.sort_values('Week')
    new_patients_weekly['Cumulative_Patients'] = new_patients_weekly['New_Patients'].cumsum()
    
    if 'Week' not in df.columns:
        df['Week'] = data_loader.get_week_ending_friday(df['Date Of Service'])
        
    # Merge with Native Weekly Metrics
    metrics_weekly = df.groupby('Week').agg({
        'PC_Allowed': 'sum',
        'PC_Revenue': 'sum',
        'PC_Revenue_Model': 'sum',
        'Service Charge Amount': 'sum'
    }).rename(columns={'Service Charge Amount': 'PC_Charges'}).reset_index()
    
    df_weekly = pd.merge(new_patients_weekly, metrics_weekly, on='Week', how='outer')
    df_weekly = df_weekly.sort_values('Week')
    
    # Forward-fill cumulative patients to maintain panel size during weeks without new patients
    df_weekly['Cumulative_Patients'] = df_weekly['Cumulative_Patients'].ffill().fillna(0)
    df_weekly['New_Patients'] = df_weekly['New_Patients'].fillna(0)
    df_weekly['PC_Allowed'] = df_weekly['PC_Allowed'].fillna(0)
    df_weekly['PC_Revenue'] = df_weekly['PC_Revenue'].fillna(0)
    df_weekly['PC_Revenue_Model'] = df_weekly['PC_Revenue_Model'].fillna(0)
    df_weekly['PC_Charges'] = df_weekly['PC_Charges'].fillna(0)
    
    # Monthly aggregation
    df_weekly['Month'] = df_weekly['Week'].dt.to_period('M')
    df_monthly = df_weekly.groupby('Month').agg(
        PC_Revenue=('PC_Revenue', 'sum'),
        PC_Revenue_Model=('PC_Revenue_Model', 'sum'),
        PC_Charges=('PC_Charges', 'sum'),
        PC_Allowed=('PC_Allowed', 'sum'),
        New_Patients=('New_Patients', 'sum'),
        Cumulative_Patients_EOM=('Cumulative_Patients', 'last'),  # End-of-month panel size
        Weeks_In_Month=('Week', 'count')
    ).reset_index()
    
    df_monthly['Month_dt'] = df_monthly['Month'].dt.to_timestamp()
    
    # Handle division by zero
    df_monthly['Rev_Per_Active_Patient'] = np.where(
        df_monthly['Cumulative_Patients_EOM'] > 0, 
        df_monthly['PC_Revenue_Model'] / df_monthly['Cumulative_Patients_EOM'], 
        0
    )
    
    return df_weekly, df_monthly

def get_revenue_drivers_regressions(df_monthly):
    """
    Computes linear regressions comparing PC Revenue to Panel Size and New Patients.
    """
    # Filter incomplete/startup months
    df_m = df_monthly[df_monthly['Weeks_In_Month'] >= 3].copy().reset_index(drop=True)
    if len(df_m) > 4:
        df_m = df_m.iloc[4:].copy().reset_index(drop=True)
        
    stats_out = {}
    
    if len(df_m) < 3:
        return stats_out
        
    x_active = df_m['Cumulative_Patients_EOM'].values
    y_rev = df_m['PC_Revenue_Model'].values
    x_new = df_m['New_Patients'].values
    
    slope_a, intercept_a, r_a, p_a, se_a = scipy_stats.linregress(x_active, y_rev)
    stats_out['panel'] = {
        'slope': slope_a, 'intercept': intercept_a, 'r2': r_a**2, 'p': p_a
    }
    
    slope_b, intercept_b, r_b, p_b, se_b = scipy_stats.linregress(x_new, y_rev)
    stats_out['new_pts'] = {
        'slope': slope_b, 'intercept': intercept_b, 'r2': r_b**2, 'p': p_b
    }
    
    return stats_out
