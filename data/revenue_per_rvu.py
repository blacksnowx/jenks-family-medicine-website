"""
Revenue per RVU analytics.

Computes the actual $/RVU ratio by pairing real revenue with earned RVUs:
  - PC (Tebra): PC_Allowed is the allowed amount after contract adjustments.
    For cash-pay charges (no adjustments, no payments) it falls back to
    full charge amount × 0.3287 as an estimate of the allowed rate.
  - VA: VA_Revenue from the fee schedule (calculate_va_fee).
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
from datetime import date

try:
    from . import data_loader
    from . import rvu_analytics
except ImportError:
    import data_loader
    import rvu_analytics

ESTIMATED_RATE = 80.0   # The benchmark used elsewhere in the dashboard
DATA_START = '2025-10-01'


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------

def _load_pc_revenue_rvu():
    """Return a DataFrame of (Date, Provider, RVU, Revenue, Week) for PC charges."""
    pc = data_loader.load_pc_data()
    if pc is None or pc.empty:
        return pd.DataFrame()

    pc = pc.copy()
    pc['Provider'] = pc['Rendering Provider'].apply(data_loader.normalize_provider)
    pc['Date Of Service'] = pd.to_datetime(pc['Date Of Service'], errors='coerce')
    pc = pc.dropna(subset=['Date Of Service'])
    pc = pc[pc['Date Of Service'] >= DATA_START].copy()

    # Ensure procedure code column exists (needed by _get_clinical_rvu_val)
    if 'Procedure Codes with Modifiers' not in pc.columns:
        pc['Procedure Codes with Modifiers'] = pc.get('Procedure Code', '')

    pc['RVU'] = pc.apply(rvu_analytics._get_clinical_rvu_val, axis=1)

    # PC_Allowed is already computed by load_pc_data(); use it as revenue proxy.
    # (For cash-pay rows with no adjustments/payments, load_pc_data already
    # applied the 0.3287 multiplier inside calculate_allowed.)
    revenue_col = 'PC_Allowed' if 'PC_Allowed' in pc.columns else 'Service Charge Amount'

    result = pc[['Date Of Service', 'Provider', 'RVU', revenue_col, 'Week']].copy()
    result = result.rename(columns={'Date Of Service': 'Date', revenue_col: 'Revenue'})
    result['Source'] = 'PC'
    return result


def _load_va_revenue_rvu():
    """Return a DataFrame of (Date, Provider, RVU, Revenue, Week) for VA encounters."""
    va = data_loader.load_va_data()
    if va is None or va.empty:
        return pd.DataFrame()

    va = va.copy()
    va['Date of Service'] = pd.to_datetime(va['Date of Service'], errors='coerce')
    va = va.dropna(subset=['Date of Service'])
    va = va[va['Date of Service'] >= DATA_START].copy()

    if 'Provider' not in va.columns:
        va['Provider'] = 'UNKNOWN'
    va['Provider'] = va['Provider'].apply(data_loader.normalize_provider)

    va['RVU'] = va.apply(rvu_analytics._get_va_rvu_val, axis=1)
    # VA_Revenue is computed by load_va_data via calculate_va_fee
    rev_col = 'VA_Revenue' if 'VA_Revenue' in va.columns else 'VA_Revenue'

    result = va[['Date of Service', 'Provider', 'RVU', rev_col, 'Week']].copy()
    result = result.rename(columns={'Date of Service': 'Date', rev_col: 'Revenue'})
    result['Source'] = 'VA'
    return result


def get_revenue_rvu_dataset():
    """Combine PC and VA into one DataFrame filtered to VALID_PROVIDERS."""
    parts = [df for df in [_load_pc_revenue_rvu(), _load_va_revenue_rvu()] if not df.empty]
    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)
    df['RVU'] = pd.to_numeric(df['RVU'], errors='coerce').fillna(0)
    df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce').fillna(0)
    df = df[df['Provider'].isin(data_loader.VALID_PROVIDERS)]
    df = df.sort_values('Date')
    return df


# ---------------------------------------------------------------------------
# Report data
# ---------------------------------------------------------------------------

def get_revenue_per_rvu_report():
    """Return a dict with weekly, by_provider, quarterly, and summary sections."""
    df = get_revenue_rvu_dataset()
    if df.empty:
        return {'error': 'No revenue/RVU data available.'}

    # --- Weekly breakdown ---
    weekly = (
        df.groupby('Week')
        .agg(total_rvus=('RVU', 'sum'), total_revenue=('Revenue', 'sum'))
        .reset_index()
        .sort_values('Week')
    )
    weekly['dollars_per_rvu'] = weekly.apply(
        lambda r: round(r['total_revenue'] / r['total_rvus'], 2) if r['total_rvus'] > 0 else 0.0,
        axis=1,
    )

    # --- By provider ---
    by_provider = (
        df.groupby('Provider')
        .agg(total_rvus=('RVU', 'sum'), total_revenue=('Revenue', 'sum'))
        .reset_index()
    )
    by_provider['dollars_per_rvu'] = by_provider.apply(
        lambda r: round(r['total_revenue'] / r['total_rvus'], 2) if r['total_rvus'] > 0 else 0.0,
        axis=1,
    )

    # --- Quarterly summary (current calendar quarter) ---
    today = date.today()
    q = (today.month - 1) // 3
    quarter_start = date(today.year, q * 3 + 1, 1)
    quarter_label = f"Q{q + 1} {today.year}"

    qdf = df[df['Date'] >= pd.Timestamp(quarter_start)]
    quarterly_prov = (
        qdf.groupby('Provider')
        .agg(q_rvus=('RVU', 'sum'), q_revenue=('Revenue', 'sum'))
        .reset_index()
    )
    quarterly_prov['q_dollars_per_rvu'] = quarterly_prov.apply(
        lambda r: round(r['q_revenue'] / r['q_rvus'], 2) if r['q_rvus'] > 0 else 0.0,
        axis=1,
    )

    # --- Overall summary ---
    total_rvus = float(df['RVU'].sum())
    total_revenue = float(df['Revenue'].sum())
    actual_rate = round(total_revenue / total_rvus, 2) if total_rvus > 0 else 0.0

    return {
        'weekly': [
            {
                'week': row['Week'].strftime('%Y-%m-%d'),
                'total_rvus': round(float(row['total_rvus']), 1),
                'total_revenue': round(float(row['total_revenue']), 2),
                'dollars_per_rvu': float(row['dollars_per_rvu']),
            }
            for _, row in weekly.iterrows()
        ],
        'by_provider': [
            {
                'provider': row['Provider'].title(),
                'total_rvus': round(float(row['total_rvus']), 1),
                'total_revenue': round(float(row['total_revenue']), 2),
                'dollars_per_rvu': float(row['dollars_per_rvu']),
            }
            for _, row in by_provider.iterrows()
        ],
        'quarterly': {
            'label': quarter_label,
            'providers': [
                {
                    'provider': row['Provider'].title(),
                    'q_rvus': round(float(row['q_rvus']), 1),
                    'q_revenue': round(float(row['q_revenue']), 2),
                    'q_dollars_per_rvu': float(row['q_dollars_per_rvu']),
                }
                for _, row in quarterly_prov.iterrows()
            ],
        },
        'summary': {
            'total_rvus': round(total_rvus, 1),
            'total_revenue': round(total_revenue, 2),
            'actual_rate': actual_rate,
            'estimated_rate': ESTIMATED_RATE,
            'rate_delta': round(actual_rate - ESTIMATED_RATE, 2),
        },
    }


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------

def generate_revenue_per_rvu_chart():
    """Weekly $/RVU trend chart compared to $80 estimate. Returns PNG bytes."""
    df = get_revenue_rvu_dataset()
    if df.empty:
        return _error_chart('No revenue data available.')

    weekly = (
        df.groupby('Week')
        .agg(total_rvus=('RVU', 'sum'), total_revenue=('Revenue', 'sum'))
        .reset_index()
        .sort_values('Week')
    )
    weekly['dollars_per_rvu'] = weekly.apply(
        lambda r: r['total_revenue'] / r['total_rvus'] if r['total_rvus'] > 0 else 0.0,
        axis=1,
    )

    x = weekly['Week'].dt.strftime('%Y-%m-%d').tolist()
    y = weekly['dollars_per_rvu'].tolist()

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(x, y, marker='o', color='#37a4db', linewidth=2.5, markersize=8, label='Actual $/RVU')
    ax.axhline(y=ESTIMATED_RATE, color='#ef4444', linestyle='--', linewidth=2,
               label=f'Estimate (${ESTIMATED_RATE:.0f}/RVU)')

    ax.set_title('Revenue per RVU — Weekly Trend', fontsize=14, fontweight='bold')
    ax.set_xlabel('Week Ending', fontsize=11, fontweight='bold')
    ax.set_ylabel('$/RVU', fontsize=11, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.6)
    fig.autofmt_xdate(rotation=45)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.25), ncol=2)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def _error_chart(msg):
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.text(0.5, 0.5, msg, ha='center', va='center', fontsize=14, color='red')
    ax.axis('off')
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()
