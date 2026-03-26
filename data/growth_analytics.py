import pandas as pd
import numpy as np
from scipy import stats as scipy_stats

try:
    from . import data_loader
except ImportError:
    import data_loader

def analyze_patient_growth(df_pc, exclude_outlier_weeks=True):
    """
    Computes new patient growth stats, rolling averages, and Monte Carlo risk simulations.
    """
    if 'Date Of Service' not in df_pc.columns or 'Patient ID' not in df_pc.columns:
        return {}
        
    df = df_pc.dropna(subset=['Date Of Service', 'Patient ID']).copy()
    if len(df) == 0:
        return {}
        
    first_visit = df.groupby('Patient ID')['Date Of Service'].min().reset_index()
    first_visit.columns = ['Patient ID', 'First Visit']
    first_visit['Week'] = data_loader.get_week_ending_friday(first_visit['First Visit'])
    
    new_patients_weekly = first_visit.groupby('Week').size().reset_index(name='New_Patients')
    new_patients_weekly = new_patients_weekly.sort_values('Week')
    new_patients_weekly['Cumulative_Patients'] = new_patients_weekly['New_Patients'].cumsum()
    
    if len(new_patients_weekly) == 0:
        return {}
        
    end_date = new_patients_weekly['Week'].max()
    start_date = end_date - pd.DateOffset(weeks=52)
    last_52 = new_patients_weekly[new_patients_weekly['Week'] >= start_date].copy()
    
    if exclude_outlier_weeks:
        exclude_weeks = pd.to_datetime(['2025-05-02', '2025-05-09', '2025-09-19'])
        last_52 = last_52[~last_52['Week'].isin(exclude_weeks)].copy()
        
    last_52['Growth_Rate'] = (last_52['New_Patients'] / last_52['Cumulative_Patients'] * 100).fillna(0)
    last_52['MA_4wk'] = last_52['New_Patients'].rolling(window=4, min_periods=1).mean()
    
    if len(last_52) < 2:
        return {'weekly_data': last_52}
        
    weeks_numeric = np.arange(len(last_52))
    
    try:
        slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(weeks_numeric, last_52['New_Patients'].values)
    except ValueError:
        slope, intercept, r_value = 0, last_52['New_Patients'].iloc[0], 0
        
    # Run simple monte carlo for "risk of sustained drop below 5"
    np.random.seed(42)
    n_simulations = 2000
    sustained_threshold = 5
    sustained_weeks = 4
    
    residuals = last_52['New_Patients'].values - (slope * weeks_numeric + intercept)
    residual_std = residuals.std()
    
    drop_count = 0
    # Add a check for std to prevent errors if all same
    if residual_std > 0:
        future_weeks = np.arange(len(last_52), len(last_52) + 52)
        base_trend = slope * future_weeks + intercept
        
        for _ in range(n_simulations):
            noise = np.random.normal(0, residual_std, 52)
            future_vals = np.maximum(0, base_trend + noise)
            
            below = future_vals < sustained_threshold
            # count consecutive Trues
            max_conseq = 0
            curr_conseq = 0
            for b in below:
                if b:
                    curr_conseq += 1
                    if curr_conseq > max_conseq: max_conseq = curr_conseq
                else:
                    curr_conseq = 0
            if max_conseq >= sustained_weeks:
                drop_count += 1
                
    prob_sustained_drop = (drop_count / n_simulations * 100) if residual_std > 0 else 0
    
    return {
        'weekly_data': last_52,
        'slope': float(slope),
        'intercept': float(intercept),
        'r2': float(r_value**2) if r_value else 0.0,
        'prob_drop': float(prob_sustained_drop)
    }
