import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import timedelta

try:
    from . import data_loader
except ImportError:
    import data_loader

def forecast_primary_care_charges(df_pc, forecast_weeks=26, seasonal_period=4):
    """
    Predicts future weekly Primary Care service charges using a Holt-Winters Exponential Smoothing model.
    """
    if 'Date Of Service' not in df_pc.columns:
        return None, None
        
    ts_data = df_pc[df_pc['Date Of Service'] >= '2024-01-01'].copy()
    
    if len(ts_data) == 0:
        return None, None
        
    weekly_charges = ts_data.groupby(pd.Grouper(key='Date Of Service', freq='W-MON'))['Service Charge Amount'].sum()
    weekly_charges = weekly_charges.asfreq('W-MON', fill_value=0)
    
    if len(weekly_charges) < 8:
        # Not enough data for Holt-Winters seasonal modeling
        return None, None
        
    try:
        model = ExponentialSmoothing(
            weekly_charges,
            trend='add',
            seasonal='add',
            seasonal_periods=seasonal_period,
            damped_trend=True
        ).fit()
        
        forecast = model.forecast(forecast_weeks)
        last_date = weekly_charges.index[-1]
        forecast_dates = [last_date + timedelta(weeks=x) for x in range(1, forecast_weeks + 1)]
        forecast_series = pd.Series(forecast.values, index=forecast_dates)
        
        stats = {
            'avg_hist': float(weekly_charges.mean()),
            'avg_forecast': float(forecast_series.mean()),
            'growth_pct': float(((forecast_series.mean() - weekly_charges.mean()) / weekly_charges.mean()) * 100) if weekly_charges.mean() > 0 else 0,
            'top_weeks': {k.strftime('%Y-%m-%d'): v for k, v in forecast_series.sort_values(ascending=False).head(3).items()}
        }
        
        return forecast_series, stats
    except Exception as e:
        return None, None

def calculate_va_ar_pipeline(df_va, current_date=pd.Timestamp('2026-02-28'), lag_days=35):
    """
    Simulates the accounts receivable (AR) pipeline for VA DBQ exams, given a standard
    lag period between execution and expected payment.
    """
    df = df_va.copy()
    if 'VA_Revenue' not in df.columns:
        df['VA_Revenue'] = df.apply(data_loader.calculate_va_fee, axis=1)
        
    cutoff_date = current_date - pd.Timedelta(days=lag_days)
    
    # Calculate executed but strictly unpaid based on the 35 day lag cutoff
    unpaid_estimated = df[(df['Date of Service'] > cutoff_date) & (df['Date of Service'] <= current_date)]['VA_Revenue'].sum()
    
    pipeline = {
        'current_date': current_date,
        'cutoff_date': cutoff_date,
        'total_executed_unpaid_estimated': float(unpaid_estimated),
        'blocks': {}
    }
    
    for i in range(3):
        block_start = cutoff_date + pd.Timedelta(days=15 * i)
        block_end = cutoff_date + pd.Timedelta(days=15 * (i+1))
        
        amt = df[(df['Date of Service'] > block_start) & (df['Date of Service'] <= block_end)]['VA_Revenue'].sum()
        pipeline['blocks'][f"Block {i+1}"] = {
            'start': block_start.strftime('%Y-%m-%d'),
            'end': block_end.strftime('%Y-%m-%d'),
            'expected_revenue': float(amt)
        }
        
    return pipeline
