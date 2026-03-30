"""
New Patients analytics.

A "new patient" is defined as the first time a hashed Patient ID appears
in the charges data (Charges Export.csv + Draft Charges.csv if available),
ordered by Date Of Service.  Only counts are ever exposed — no individual
patient data leaves this module.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io

try:
    from . import data_loader
except ImportError:
    import data_loader

DATA_START = '2025-10-01'
GOOGLE_ADS_START = '2025-12-29'


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------

def _load_all_charges():
    """
    Load confirmed charges (Charges Export.csv) and draft charges if available.
    Returns a DataFrame with columns: PatientID, Date.
    Patient IDs are already hashed (pid_ prefix) by the upload pipeline.
    """
    dfs = []

    # Confirmed charges
    pc = data_loader.load_pc_data()
    if pc is not None and not pc.empty:
        if 'Patient ID' in pc.columns and 'Date Of Service' in pc.columns:
            sub = pc[['Patient ID', 'Date Of Service']].copy()
            sub = sub.rename(columns={'Patient ID': 'PatientID', 'Date Of Service': 'Date'})
            dfs.append(sub)

    # Draft charges (non-fatal if absent)
    try:
        draft_blob = data_loader.get_csv_from_db('Draft Charges.csv')
        if draft_blob is not None:
            draft = pd.read_csv(draft_blob)
            if 'Patient ID' in draft.columns and 'Date Of Service' in draft.columns:
                draft['Date Of Service'] = pd.to_datetime(draft['Date Of Service'], errors='coerce')
                sub = draft[['Patient ID', 'Date Of Service']].copy()
                sub = sub.rename(columns={'Patient ID': 'PatientID', 'Date Of Service': 'Date'})
                dfs.append(sub)
    except Exception:
        pass

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    combined['Date'] = pd.to_datetime(combined['Date'], errors='coerce')
    combined = combined.dropna(subset=['Date', 'PatientID'])
    # Drop rows where PatientID is empty / null string
    combined = combined[combined['PatientID'].astype(str).str.strip() != '']
    return combined


def get_new_patients_dataset():
    """
    Return a DataFrame of new-patient events since DATA_START.
    Each row is one unique patient with their first Date Of Service and Week.
    """
    df = _load_all_charges()
    if df.empty:
        return pd.DataFrame()

    # First appearance of each hashed patient ID across ALL history
    first_seen = df.groupby('PatientID')['Date'].min().reset_index()
    first_seen.columns = ['PatientID', 'FirstDate']

    # Filter to the reporting window
    first_seen = first_seen[first_seen['FirstDate'] >= DATA_START].copy()
    if first_seen.empty:
        return pd.DataFrame()

    first_seen['Week'] = data_loader.get_week_ending_friday(first_seen['FirstDate'])
    return first_seen


# ---------------------------------------------------------------------------
# Report data
# ---------------------------------------------------------------------------

def get_new_patients_report():
    """
    Return a dict with weekly new-patient counts, 4-week MA, and cumulative total.
    Contains COUNTS ONLY — no individual patient identifiers are included.
    """
    df = get_new_patients_dataset()
    if df.empty:
        return {'error': 'No patient data available.'}

    weekly = (
        df.groupby('Week')
        .size()
        .reset_index(name='new_patients')
        .sort_values('Week')
    )
    weekly['Week'] = pd.to_datetime(weekly['Week'])
    weekly['ma4'] = weekly['new_patients'].rolling(4, min_periods=1).mean().round(1)
    weekly['cumulative'] = weekly['new_patients'].cumsum()

    total = int(weekly['new_patients'].sum())
    avg = round(float(weekly['new_patients'].mean()), 1)

    # Google Ads comparison (started 12/29/2025)
    ads_date = pd.Timestamp(GOOGLE_ADS_START)
    pre_ads = weekly[weekly['Week'] < ads_date]
    post_ads = weekly[weekly['Week'] >= ads_date]

    pre_total = int(pre_ads['new_patients'].sum()) if not pre_ads.empty else 0
    post_total = int(post_ads['new_patients'].sum()) if not post_ads.empty else 0
    pre_weeks = len(pre_ads) if not pre_ads.empty else 0
    post_weeks = len(post_ads) if not post_ads.empty else 0
    pre_avg = round(pre_total / pre_weeks, 1) if pre_weeks > 0 else 0
    post_avg = round(post_total / post_weeks, 1) if post_weeks > 0 else 0
    pct_change = round((post_avg - pre_avg) / pre_avg * 100, 1) if pre_avg > 0 else 0

    return {
        'weekly': [
            {
                'week': row['Week'].strftime('%Y-%m-%d'),
                'new_patients': int(row['new_patients']),
                'ma4': float(row['ma4']),
                'cumulative': int(row['cumulative']),
            }
            for _, row in weekly.iterrows()
        ],
        'total_new_patients': total,
        'avg_per_week': avg,
        'google_ads_comparison': {
            'ads_start_date': GOOGLE_ADS_START,
            'pre_ads_weeks': pre_weeks,
            'post_ads_weeks': post_weeks,
            'pre_ads_total': pre_total,
            'post_ads_total': post_total,
            'pre_ads_avg_per_week': pre_avg,
            'post_ads_avg_per_week': post_avg,
            'percent_change': pct_change,
        },
    }


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------

def generate_new_patients_chart():
    """
    Weekly bar chart of new patients with 4-week MA overlay.
    Returns PNG bytes.
    """
    df = get_new_patients_dataset()
    if df.empty:
        return _error_chart('No patient data available.')

    weekly = (
        df.groupby('Week')
        .size()
        .reset_index(name='new_patients')
        .sort_values('Week')
    )
    weekly['Week'] = pd.to_datetime(weekly['Week'])
    weekly['ma4'] = weekly['new_patients'].rolling(4, min_periods=1).mean()

    x = list(range(len(weekly)))
    x_labels = weekly['Week'].dt.strftime('%Y-%m-%d').tolist()

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(x, weekly['new_patients'].tolist(), color='#37a4db', alpha=0.75,
           label='New Patients', width=0.6)
    ax.plot(x, weekly['ma4'].tolist(), color='#f39c12', linewidth=2.5,
            marker='o', markersize=6, label='4-Week Moving Avg')

    # Google Ads vertical marker
    ads_date = pd.Timestamp(GOOGLE_ADS_START)
    ads_x = None
    for i, w in enumerate(weekly['Week']):
        if w >= ads_date:
            ads_x = i
            break
    if ads_x is not None:
        ax.axvline(x=ads_x - 0.5, color='#e74c3c', linestyle='--', linewidth=2, alpha=0.8)
        ax.text(ads_x - 0.5, ax.get_ylim()[1] * 0.95, '  Google Ads\n  Started',
                fontsize=9, color='#e74c3c', fontweight='bold', va='top')
        # Pre/post averages
        pre_vals = weekly.iloc[:ads_x]['new_patients']
        post_vals = weekly.iloc[ads_x:]['new_patients']
        if not pre_vals.empty:
            ax.text(ads_x * 0.35, ax.get_ylim()[1] * 0.82,
                    f'Avg: {pre_vals.mean():.1f}/wk', fontsize=10, color='#555',
                    ha='center', style='italic')
        if not post_vals.empty:
            ax.text(ads_x + (len(weekly) - ads_x) * 0.5, ax.get_ylim()[1] * 0.82,
                    f'Avg: {post_vals.mean():.1f}/wk', fontsize=10, color='#27ae60',
                    ha='center', fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=9)
    ax.set_title('New Patients per Week (since Oct 1, 2025)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Week Ending', fontsize=11, fontweight='bold')
    ax.set_ylabel('New Patients', fontsize=11, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.4, axis='y')
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.3), ncol=2)
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
