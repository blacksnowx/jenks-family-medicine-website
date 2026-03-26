import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import re
from datetime import date

try:
    from . import data_loader
except ImportError:
    import data_loader

def _get_clinical_rvu_val(row):
    services = str(row.get('Procedure Codes with Modifiers', row.get('Procedure Code'))).upper()
    charges = row.get('Service Charge Amount', 0)
    if "FUNCTIONAL" in services and "INITIAL" in services: return 3.75
    if "FUNCTIONAL" in services: return 2.50
    if "NEXUS" in services or charges == 200.00: return 2.5
    if "DOT" in services or ("PHYSICAL" in services and "DOT" in str(row).upper()): return 1.0
    if re.search(r'992[01][1-5]', services): return 1.3
    if re.search(r'993[89][0-9]', services): return 1.3
    if "ANNUAL" in services or "PHYSICAL" in services: return 1.3
    if re.search(r'90[4-7][0-9]{2}', services) or "VACCINE" in services or "ADMIN" in services: return 0.5
    return 0.0

def _get_clinical_rvu_cat(row):
    services = str(row.get('Procedure Codes with Modifiers', row.get('Procedure Code'))).upper()
    charges = row.get('Service Charge Amount', 0)
    if "FUNCTIONAL" in services and "INITIAL" in services: return "Func Med Initial"
    if "FUNCTIONAL" in services: return "Func Med Subsequent"
    if "NEXUS" in services or charges == 200.00: return "Nexus Letter"
    if "DOT" in services or ("PHYSICAL" in services and "DOT" in str(row).upper()): return "DOT Physical"
    if re.search(r'992[01][1-5]', services): return "Primary Care Visit"
    if re.search(r'993[89][0-9]', services): return "Primary Care Visit"
    if "ANNUAL" in services or "PHYSICAL" in services: return "Primary Care Visit"
    if re.search(r'90[4-7][0-9]{2}', services) or "VACCINE" in services or "ADMIN" in services: return "Vaccine Only"
    return "Other/Unclassified"

def _get_va_rvu_val(row):
    if str(row['No Show']).strip() == '1': return 0.75
    imo_str = str(row['Routine IMOs']).strip()
    if imo_str in ['0', 'nan', '']: return 2.5
    if imo_str in ['1-3', '1-5', '1', '2', '3'] or (imo_str and imo_str[0] in ['1', '2', '3'] and len(imo_str) == 1): return 4.5
    return 6.5

def _get_va_rvu_cat(row):
    if str(row['No Show']).strip() == '1': return "No Show"
    imo_str = str(row['Routine IMOs']).strip()
    if imo_str in ['0', 'nan', '']: return "VA Exam (0 IMOs)"
    if imo_str in ['1-3', '1-5', '1', '2', '3'] or (imo_str and imo_str[0] in ['1', '2', '3'] and len(imo_str) == 1): return "VA Exam (1-3 IMOs)"
    return "VA Exam (4+ IMOs)"

def get_rvu_dataset():
    """Generates the master consolidated dataset with RVU mappings."""
    # 1. Load PC Data
    pc = data_loader.load_pc_data()
    if pc is None or pc.empty:
        pc_df = pd.DataFrame()
    else:
        pc['Provider'] = pc['Rendering Provider'].apply(data_loader.normalize_provider)
        pc['Date Of Service'] = pd.to_datetime(pc['Date Of Service'], errors='coerce')
        pc['Total Charge Amount'] = pc.get('Service Charge Amount', pc.get('Total Charge Amount', 0))
        if pc['Total Charge Amount'].dtype == object:
            pc['Total Charge Amount'] = pc['Total Charge Amount'].astype(str).str.replace(r'[$,]', '', regex=True).astype(float)
        
        # Apply RVU Logic
        # Fix missing Procedure Codes
        if 'Procedure Codes with Modifiers' not in pc.columns:
             if 'Procedure Code' in pc.columns:
                 pc['Procedure Codes with Modifiers'] = pc['Procedure Code']
             else:
                 pc['Procedure Codes with Modifiers'] = ''
             
        pc['RVU'] = pc.apply(_get_clinical_rvu_val, axis=1)
        pc['Category'] = pc.apply(_get_clinical_rvu_cat, axis=1)
            
        pc_df = pc[['Date Of Service', 'Provider', 'RVU', 'Category']].copy()

    # 2. Load VA Data
    va = data_loader.load_va_data()
    if va is None or va.empty:
        va_df = pd.DataFrame()
    else:
        va = va.rename(columns={'Sarah Suggs ': 'Provider'})
        va['Date of Service'] = pd.to_datetime(va['Date of Service'], errors='coerce')
        va['Provider'] = va['Provider'].apply(data_loader.normalize_provider)
        
        # Apply RVU Logic
        va['RVU'] = va.apply(_get_va_rvu_val, axis=1)
        va['Category'] = va.apply(_get_va_rvu_cat, axis=1)
            
        va_df = va[['Date of Service', 'Provider', 'RVU', 'Category']].copy()
        va_df = va_df.rename(columns={'Date of Service': 'Date Of Service'})

    # 3. Combine and Filter (>= 2025-10-01)
    if pc_df.empty and va_df.empty:
        return pd.DataFrame()
        
    master_df = pd.concat([pc_df, va_df], ignore_index=True)
    master_df['RVU'] = pd.to_numeric(master_df['RVU'], errors='coerce').fillna(0)
    master_df = master_df.dropna(subset=['Date Of Service'])
    master_df = master_df[master_df['Date Of Service'] >= '2025-10-01']
    # Removed hardcoded upper bound to automatically include latest data
    
    # 4. Add Week Grouping
    master_df['Week'] = data_loader.get_week_ending_friday(master_df['Date Of Service'])
    master_df['Week'] = pd.to_datetime(master_df['Week'])
    return master_df

def generate_rvu_chart(view_type):
    """Generates the required RVU chart (Company Wide, Individual, or Comparison) and returns the PNG bytes."""
    df = get_rvu_dataset()
    if df.empty:
        return _generate_error_chart("No RVU data available.")

    # Sort data by Week explicitly
    df = df.sort_values('Week')

    # Add 216 extra RVUs divided evenly across all available weeks for Anne Jenks
    unique_weeks = df['Week'].unique()
    if len(unique_weeks) > 0:
        extra_per_week = 216.0 / len(unique_weeks)
        dummy_rows = pd.DataFrame({
            'Week': unique_weeks,
            'Provider': 'ANNE JENKS',
            'RVU': extra_per_week,
            'Category': 'Manual Adjustment'
        })
        df = pd.concat([df, dummy_rows], ignore_index=True)
        # Re-sort after adding dummy rows
        df = df.sort_values('Week')

    fig, ax = plt.subplots(figsize=(12, 6))

    valid_providers = data_loader.VALID_PROVIDERS
    df = df[df['Provider'].isin(valid_providers)]

    if view_type == 'Company Wide':
        # Sum all RVUs per week
        weekly = df.groupby('Week')['RVU'].sum().reset_index()
        weekly = weekly.sort_values('Week')
        
        ax.plot(weekly['Week'].dt.strftime('%Y-%m-%d').tolist(), weekly['RVU'].astype(float).tolist(), marker='o', color='#37a4db', linewidth=2.5, markersize=8, label='Total Earned RVUs')
        ax.axhline(y=196, color='red', linestyle='--', linewidth=2, label='Company Breakeven (196)')
        ax.axhline(y=328, color='orange', linestyle='--', linewidth=2, label='Industry Standard (328)')
        
        ax.set_title('Company-Wide Weekly RVUs', fontsize=14, fontweight='bold')
        
    elif view_type == 'Provider Comparison':
        pivot_df = pd.pivot_table(df, values='RVU', index='Week', columns='Provider', aggfunc='sum', fill_value=0)
        pivot_df = pivot_df.sort_index()
        x_vals = pivot_df.index.strftime('%Y-%m-%d').tolist()
        
        colors = ['#37a4db', '#2ecc71', '#f39c12', '#9b59b6']
        for i, provider in enumerate(valid_providers):
            if provider in pivot_df.columns:
                y_vals = pivot_df[provider].astype(float).tolist()
                ax.plot(x_vals, y_vals, marker='o', linewidth=2, markersize=6, label=provider, color=colors[i % len(colors)])
                
        ax.axhline(y=49, color='red', linestyle='--', linewidth=2, label='Individual Breakeven (49)')
        ax.axhline(y=82, color='orange', linestyle='--', linewidth=2, label='Industry Standard (82)')
        ax.set_title('Provider Weekly RVU Comparison', fontsize=14, fontweight='bold')

    elif view_type in [p.title() for p in valid_providers] or view_type.upper() in valid_providers:
        # Individual Provider
        provider_upper = view_type.upper()
        # Find raw display name
        display_name = next((p.title() for p in valid_providers if p == provider_upper), view_type)
        
        prov_data = df[df['Provider'] == provider_upper].groupby('Week')['RVU'].sum().reset_index().sort_values('Week')
        if prov_data.empty:
            return _generate_error_chart(f"No RVU data found for {display_name}.")
            
        ax.plot(prov_data['Week'].dt.strftime('%Y-%m-%d').tolist(), prov_data['RVU'].astype(float).tolist(), marker='o', color='#2ecc71', linewidth=2.5, markersize=8, label=f'{display_name} Earned RVUs')
        ax.axhline(y=49, color='red', linestyle='--', linewidth=2, label='Individual Breakeven (49)')
        ax.axhline(y=82, color='orange', linestyle='--', linewidth=2, label='Industry Standard (82)')
        ax.set_title(f'{display_name} - Weekly RVUs', fontsize=14, fontweight='bold')
        
    else:
        return _generate_error_chart("Invalid chart view type requested.")

    # Formatting touches
    ax.set_xlabel('Week Ending', fontsize=11, fontweight='bold')
    ax.set_ylabel('Total RVUs', fontsize=11, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.6)
    fig.autofmt_xdate(rotation=45)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.25), ncol=3)
    plt.tight_layout()

    # Save to memory buffer
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=100)
    plt.close(fig)
    img_buffer.seek(0)
    return img_buffer.getvalue()

def calculate_bonus(rvus):
    """Calculate quarterly bonus for a given RVU total.

    Tiers:
      $25 per RVU over 689 (up to 735)
      $30 per RVU over 735 (up to 827)
      $35 per RVU over 827
    """
    if rvus <= 689:
        return 0.0
    bonus = 0.0
    # Tier 1: 690–735 at $25/RVU
    tier1 = min(rvus, 735) - 689
    bonus += tier1 * 25
    # Tier 2: 736–827 at $30/RVU
    if rvus > 735:
        tier2 = min(rvus, 827) - 735
        bonus += tier2 * 30
    # Tier 3: 828+ at $35/RVU
    if rvus > 827:
        tier3 = rvus - 827
        bonus += tier3 * 35
    return bonus


def get_quarterly_bonus_report(today=None):
    """Return per-provider bonus data for the current calendar quarter.

    Returns a dict with 'quarter_label', 'days_elapsed', 'days_in_quarter',
    and 'providers' (list of per-provider dicts).
    """
    if today is None:
        today = date.today()

    # Determine current quarter boundaries
    q = (today.month - 1) // 3
    quarter_start = date(today.year, q * 3 + 1, 1)
    if q < 3:
        quarter_end = date(today.year, (q + 1) * 3 + 1, 1)
    else:
        quarter_end = date(today.year + 1, 1, 1)

    days_elapsed = (today - quarter_start).days + 1  # inclusive of today
    days_in_quarter = (quarter_end - quarter_start).days
    quarter_label = f"Q{q + 1} {today.year}"

    # Load RVU dataset and filter to current quarter
    df = get_rvu_dataset()
    if df.empty:
        return {
            'quarter_label': quarter_label,
            'days_elapsed': days_elapsed,
            'days_in_quarter': days_in_quarter,
            'providers': [],
        }

    mask = (
        (df['Date Of Service'] >= pd.Timestamp(quarter_start))
        & (df['Date Of Service'] < pd.Timestamp(quarter_end))
    )
    qdf = df[mask]

    # Include the Anne Jenks manual 216-RVU adjustment, prorated to this quarter
    # (get_rvu_dataset spreads it across ALL weeks since 2025-10-01, so we
    #  compute the per-quarter share based on weeks in this quarter vs total)
    all_weeks = df['Week'].unique()
    quarter_weeks = qdf['Week'].unique()
    if len(all_weeks) > 0 and len(quarter_weeks) > 0:
        adjustment_this_quarter = 216.0 * len(quarter_weeks) / len(all_weeks)
    else:
        adjustment_this_quarter = 0.0

    # Sum RVUs per provider (from raw data, without the chart adjustment)
    provider_rvus = qdf.groupby('Provider')['RVU'].sum()

    providers = []
    for prov in data_loader.VALID_PROVIDERS:
        rvus_earned = float(provider_rvus.get(prov, 0.0))

        # Add Anne Jenks manual adjustment
        if prov == 'ANNE JENKS':
            rvus_earned += adjustment_this_quarter

        bonus_earned = calculate_bonus(rvus_earned)

        # Extrapolate to full quarter
        if days_elapsed > 0:
            projected_rvus = rvus_earned * days_in_quarter / days_elapsed
        else:
            projected_rvus = 0.0
        projected_bonus = calculate_bonus(projected_rvus)

        providers.append({
            'provider': prov.title(),
            'rvus_earned': round(rvus_earned, 1),
            'bonus_earned': round(bonus_earned, 2),
            'projected_rvus': round(projected_rvus, 1),
            'projected_bonus': round(projected_bonus, 2),
        })

    return {
        'quarter_label': quarter_label,
        'days_elapsed': days_elapsed,
        'days_in_quarter': days_in_quarter,
        'providers': providers,
    }


def _generate_error_chart(message):
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.text(0.5, 0.5, message, horizontalalignment='center', verticalalignment='center', fontsize=14, color='red')
    ax.axis('off')
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png')
    plt.close(fig)
    img_buffer.seek(0)
    return img_buffer.getvalue()
