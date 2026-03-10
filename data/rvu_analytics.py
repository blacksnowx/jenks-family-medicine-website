import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import re

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
        # Standardize Provider Names for PC
        provider_map_pc = {
            'Jenks, Anne ': 'Anne Jenks',
            'IRVIN, EHRIN ': 'Ehrin Irvin',
            'SUGGS, SARAH ': 'Sarah Suggs',
            'REDD, DAVID ': 'Anne Jenks',
            'Anne Jenks, APRN': 'Anne Jenks',
            'DAVID REDD, MD': 'Anne Jenks',
            'EHRIN IRVIN, FNP': 'Ehrin Irvin',
            'SARAH SUGGS, NP': 'Sarah Suggs'
        }
        pc['Provider'] = pc['Rendering Provider'].replace(provider_map_pc).str.upper().str.strip()
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
        va['Provider'] = va['Provider'].str.strip().replace({
            '0': 'Anne Jenks', 'sArah Suggs': 'Sarah Suggs',
            'SArah Suggs': 'Sarah Suggs', 'Sarah Suggs': 'Sarah Suggs'
        }).str.upper().str.strip()
        
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
    master_df = master_df[master_df['Date Of Service'] <= '2026-02-27']
    
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

    # Standardize names for filtering
    valid_providers = ['ANNE JENKS', 'EHRIN IRVIN', 'HEATHER MAYO', 'SARAH SUGGS']
    df = df[df['Provider'].isin(valid_providers)]

    if view_type == 'Company Wide':
        # Sum all RVUs per week
        weekly = df.groupby('Week')['RVU'].sum().reset_index()
        weekly = weekly.sort_values('Week')
        
        ax.plot(weekly['Week'].dt.strftime('%Y-%m-%d').tolist(), weekly['RVU'].astype(float).tolist(), marker='o', color='#37a4db', linewidth=2.5, markersize=8, label='Total Earned RVUs')
        ax.axhline(y=196, color='red', linestyle='--', linewidth=2, label='Company Breakeven (196)')
        ax.axhline(y=375, color='orange', linestyle='--', linewidth=2, label='Industry Standard (375)')
        
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
        ax.axhline(y=93.75, color='orange', linestyle='--', linewidth=2, label='Industry Standard (93.75)')
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
        ax.axhline(y=93.75, color='orange', linestyle='--', linewidth=2, label='Industry Standard (93.75)')
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

def _generate_error_chart(message):
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.text(0.5, 0.5, message, horizontalalignment='center', verticalalignment='center', fontsize=14, color='red')
    ax.axis('off')
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png')
    plt.close(fig)
    img_buffer.seek(0)
    return img_buffer.getvalue()
