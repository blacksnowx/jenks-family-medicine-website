import math
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

# Provider-count cutover. Week ending 2026-04-24 and later runs with 3
# providers; earlier weeks ran with 4. Thresholds before/after reflect that.
PROVIDER_CUTOVER_WEEK = pd.Timestamp('2026-04-24')

BREAKEVEN_COMPANY_PRE = 196
INDUSTRY_COMPANY_PRE = 328
BREAKEVEN_INDIVIDUAL_PRE = 49   # 196 / 4
INDUSTRY_INDIVIDUAL_PRE = 82    # 328 / 4

BREAKEVEN_COMPANY_POST = 146
INDUSTRY_COMPANY_POST = 278
BREAKEVEN_INDIVIDUAL_POST = 49  # round(146 / 3)
INDUSTRY_INDIVIDUAL_POST = 93   # round(278 / 3)


def _threshold_series(weeks, pre_value, post_value):
    """Return per-week threshold values, pre-cutover on earlier weeks."""
    return [
        pre_value if pd.Timestamp(w) < PROVIDER_CUTOVER_WEEK else post_value
        for w in weeks
    ]


def _get_clinical_rvu_val(row):
    _svc_raw = row.get('Procedure Codes with Modifiers', row.get('Procedure Code'))
    # row.get() can return a Series if the DataFrame has duplicate column names;
    # extract the first scalar value in that case.
    if isinstance(_svc_raw, pd.Series):
        _svc_raw = _svc_raw.iloc[0] if not _svc_raw.empty else ''
    services = str(_svc_raw).upper()
    _chg_raw = row.get('Service Charge Amount', 0)
    charges = float(_chg_raw.iloc[0]) if isinstance(_chg_raw, pd.Series) and not _chg_raw.empty else (float(_chg_raw) if _chg_raw else 0.0)
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
    _svc_raw = row.get('Procedure Codes with Modifiers', row.get('Procedure Code'))
    if isinstance(_svc_raw, pd.Series):
        _svc_raw = _svc_raw.iloc[0] if not _svc_raw.empty else ''
    services = str(_svc_raw).upper()
    _chg_raw = row.get('Service Charge Amount', 0)
    charges = float(_chg_raw.iloc[0]) if isinstance(_chg_raw, pd.Series) and not _chg_raw.empty else (float(_chg_raw) if _chg_raw else 0.0)
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

def _process_pc_charges(pc):
    """Apply provider normalization and RVU mapping to a PC charges DataFrame."""
    pc = pc.copy()
    pc['Provider'] = pc['Rendering Provider'].apply(data_loader.normalize_provider)
    pc['Date Of Service'] = pd.to_datetime(pc['Date Of Service'], errors='coerce')
    pc['Total Charge Amount'] = pc.get('Service Charge Amount', pc.get('Total Charge Amount', 0))
    if pc['Total Charge Amount'].dtype == object:
        pc['Total Charge Amount'] = pc['Total Charge Amount'].astype(str).str.replace(r'[$,]', '', regex=True).astype(float)
    if 'Procedure Codes with Modifiers' not in pc.columns:
        if 'Procedure Code' in pc.columns:
            pc['Procedure Codes with Modifiers'] = pc['Procedure Code']
        else:
            pc['Procedure Codes with Modifiers'] = ''
    pc['RVU'] = pc.apply(_get_clinical_rvu_val, axis=1)
    pc['Category'] = pc.apply(_get_clinical_rvu_cat, axis=1)
    return pc[['Date Of Service', 'Provider', 'RVU', 'Category']].copy()


def get_rvu_dataset(data_source='all', include_pipeline=False):
    """Generates the master consolidated dataset with RVU mappings.

    Args:
        data_source: 'all' (default) — both PC and VA data;
                     'pc'            — Primary Care only (skip VA);
                     'va'            — VA only (skip PC).
        include_pipeline: When True, also load draft charges from
                          'Draft Charges.csv' and append them with
                          Source='pipeline'.  Confirmed rows get
                          Source='confirmed'.
    """
    # 1. Load PC Data
    if data_source == 'va':
        pc_df = pd.DataFrame()
    else:
        pc = data_loader.load_pc_data()
        if pc is None or pc.empty:
            pc_df = pd.DataFrame()
        else:
            pc_df = _process_pc_charges(pc)

    # 2. Load VA Data
    if data_source == 'pc':
        va_df = pd.DataFrame()
    else:
        va = data_loader.load_va_data()
        if va is None or va.empty:
            va_df = pd.DataFrame()
        else:
            # Provider column is already normalized to 'Provider' by load_va_data()
            if 'Sarah Suggs ' in va.columns:
                va = va.rename(columns={'Sarah Suggs ': 'Provider'})
            va['Date of Service'] = pd.to_datetime(va['Date of Service'], errors='coerce')
            if 'Provider' not in va.columns:
                va['Provider'] = 'UNKNOWN'
            va['Provider'] = va['Provider'].apply(data_loader.normalize_provider)

            # Apply RVU Logic
            va['RVU'] = va.apply(_get_va_rvu_val, axis=1)
            va['Category'] = va.apply(_get_va_rvu_cat, axis=1)

            va_df = va[['Date of Service', 'Provider', 'RVU', 'Category']].copy()
            va_df = va_df.rename(columns={'Date of Service': 'Date Of Service'})

    # 3. Combine and Filter (>= 2025-10-01)
    if pc_df.empty and va_df.empty:
        confirmed_df = pd.DataFrame()
    else:
        confirmed_df = pd.concat([pc_df, va_df], ignore_index=True)
        confirmed_df['RVU'] = pd.to_numeric(confirmed_df['RVU'], errors='coerce').fillna(0)
        confirmed_df = confirmed_df.dropna(subset=['Date Of Service'])
        confirmed_df = confirmed_df[confirmed_df['Date Of Service'] >= '2025-10-01']

    if include_pipeline:
        confirmed_df['Source'] = 'confirmed'

    # 4. Load pipeline (draft) data when requested
    #    Draft charges come exclusively from Tebra (Primary Care).
    #    They are irrelevant when viewing VA-only data.
    pipeline_df = pd.DataFrame()
    if include_pipeline and data_source != 'va':
        try:
            draft_blob = data_loader.get_csv_from_db("Draft Charges.csv")
            if draft_blob is not None:
                draft_raw = pd.read_csv(draft_blob)
                if not draft_raw.empty and 'Rendering Provider' in draft_raw.columns:
                    # Dedup: remove draft charges that already exist in confirmed
                    # data (charge was drafted, then approved between syncs).
                    if 'Encounter Procedure ID' in draft_raw.columns:
                        confirmed_blob = data_loader.get_csv_from_db("Charges Export.csv")
                        if confirmed_blob is not None:
                            confirmed_raw = pd.read_csv(confirmed_blob)
                            if 'Encounter Procedure ID' in confirmed_raw.columns:
                                confirmed_epids = set(confirmed_raw['Encounter Procedure ID'].dropna().astype(str))
                                before = len(draft_raw)
                                draft_raw = draft_raw[~draft_raw['Encounter Procedure ID'].astype(str).isin(confirmed_epids)]
                                removed = before - len(draft_raw)
                                if removed > 0:
                                    import logging
                                    logging.getLogger(__name__).warning(
                                        "Pipeline dedup: removed %d draft charges already in confirmed data "
                                        "(Encounter Procedure ID overlap — prevented double-counting)",
                                        removed,
                                    )
                    if not draft_raw.empty:
                        draft_processed = _process_pc_charges(draft_raw)
                        draft_processed = draft_processed.dropna(subset=['Date Of Service'])
                        draft_processed = draft_processed[draft_processed['Date Of Service'] >= '2025-10-01']
                        draft_processed['Source'] = 'pipeline'
                        pipeline_df = draft_processed
        except Exception:
            pass  # Missing draft data is non-fatal; pipeline is supplementary

    # 5. Combine confirmed + pipeline
    parts = [df for df in [confirmed_df, pipeline_df] if not df.empty]
    if not parts:
        return pd.DataFrame()

    master_df = pd.concat(parts, ignore_index=True)
    master_df['RVU'] = pd.to_numeric(master_df['RVU'], errors='coerce').fillna(0)

    # 6. Add Week Grouping
    master_df['Week'] = data_loader.get_week_ending_friday(master_df['Date Of Service'])
    master_df['Week'] = pd.to_datetime(master_df['Week'])
    return master_df

def generate_rvu_chart(view_type, data_source='all', include_pipeline=False, exclude_providers=None):
    """Generates the required RVU chart (Company Wide, Individual, or Comparison) and returns the PNG bytes."""
    df = get_rvu_dataset(data_source=data_source, include_pipeline=include_pipeline)
    if df.empty:
        return _generate_error_chart("No RVU data available.")

    has_pipeline = include_pipeline and 'Source' in df.columns and (df['Source'] == 'pipeline').any()

    # Sort data by Week explicitly
    df = df.sort_values('Week')

    # Split into confirmed and pipeline before adding manual adjustment
    if has_pipeline:
        confirmed_only = df[df['Source'] == 'confirmed'].copy()
        pipeline_only = df[df['Source'] == 'pipeline'].copy()
    else:
        confirmed_only = df.copy()
        pipeline_only = pd.DataFrame()

    # Add 216 extra RVUs divided evenly across all available weeks for Anne Jenks
    # (applied to confirmed data only)
    unique_weeks = confirmed_only['Week'].unique() if not confirmed_only.empty else df['Week'].unique()
    if len(unique_weeks) > 0:
        extra_per_week = 216.0 / len(unique_weeks)
        dummy_rows = pd.DataFrame({
            'Week': unique_weeks,
            'Provider': 'ANNE JENKS',
            'RVU': extra_per_week,
            'Category': 'Manual Adjustment',
            **({'Source': 'confirmed'} if has_pipeline else {}),
        })
        confirmed_only = pd.concat([confirmed_only, dummy_rows], ignore_index=True)
        confirmed_only = confirmed_only.sort_values('Week')

    fig, ax = plt.subplots(figsize=(12, 6))

    valid_providers = data_loader.VALID_PROVIDERS
    confirmed_only = confirmed_only[confirmed_only['Provider'].isin(valid_providers)]
    if not pipeline_only.empty:
        pipeline_only = pipeline_only[pipeline_only['Provider'].isin(valid_providers)]

    x_labels = []

    if view_type == 'Company Wide':
        # Sum confirmed RVUs per week
        weekly = confirmed_only.groupby('Week')['RVU'].sum().reset_index().sort_values('Week')
        x_labels = weekly['Week'].dt.strftime('%Y-%m-%d').tolist()

        ax.plot(x_labels, weekly['RVU'].astype(float).tolist(), marker='o', color='#37a4db', linewidth=2.5, markersize=8, label='Total Earned RVUs')

        if has_pipeline and not pipeline_only.empty:
            pipe_weekly = pipeline_only.groupby('Week')['RVU'].sum().reset_index()
            # Build confirmed+pipeline combined series aligned to confirmed weeks
            weekly_combined = weekly.set_index('Week')['RVU'].copy()
            for _, row in pipe_weekly.iterrows():
                if row['Week'] in weekly_combined.index:
                    weekly_combined[row['Week']] += row['RVU']
                else:
                    weekly_combined[row['Week']] = row['RVU']
            weekly_combined = weekly_combined.sort_index()
            pipe_x = weekly_combined.index.strftime('%Y-%m-%d').tolist()
            ax.plot(pipe_x, weekly_combined.astype(float).tolist(), marker='o', color='#37a4db',
                    linewidth=1.5, markersize=6, linestyle='--', alpha=0.55, label='Pipeline (draft charges)')

        be_vals = _threshold_series(weekly['Week'], BREAKEVEN_COMPANY_PRE, BREAKEVEN_COMPANY_POST)
        tg_vals = _threshold_series(weekly['Week'], INDUSTRY_COMPANY_PRE, INDUSTRY_COMPANY_POST)
        ax.plot(x_labels, be_vals, color='red', linestyle='--', linewidth=2,
                label='Company Breakeven', drawstyle='steps-post')
        ax.plot(x_labels, tg_vals, color='orange', linestyle='--', linewidth=2,
                label='Industry Standard', drawstyle='steps-post')
        ax.set_title('Company-Wide Weekly RVUs', fontsize=14, fontweight='bold')

    elif view_type == 'Provider Comparison':
        comparison_providers = [p for p in valid_providers if not (exclude_providers and p in exclude_providers)]
        pivot_df = pd.pivot_table(confirmed_only, values='RVU', index='Week', columns='Provider', aggfunc='sum', fill_value=0)
        pivot_df = pivot_df.sort_index()
        x_labels = pivot_df.index.strftime('%Y-%m-%d').tolist()

        colors = ['#37a4db', '#2ecc71', '#f39c12', '#9b59b6']
        pipeline_legend_added = False
        for i, provider in enumerate(comparison_providers):
            color = colors[i % len(colors)]
            if provider in pivot_df.columns:
                y_vals = pivot_df[provider].astype(float).tolist()
                ax.plot(x_labels, y_vals, marker='o', linewidth=2, markersize=6, label=provider, color=color)

            if has_pipeline and not pipeline_only.empty and provider in pipeline_only['Provider'].values:
                pipe_prov = pipeline_only[pipeline_only['Provider'] == provider].groupby('Week')['RVU'].sum()
                confirmed_prov = pivot_df[provider] if provider in pivot_df.columns else pd.Series(dtype=float)
                combined = confirmed_prov.add(pipe_prov, fill_value=0).sort_index()
                pipe_x = combined.index.strftime('%Y-%m-%d').tolist()
                pipe_label = 'Pipeline (draft charges)' if not pipeline_legend_added else '_nolegend_'
                pipeline_legend_added = True
                ax.plot(pipe_x, combined.astype(float).tolist(), marker='o', linewidth=1.5, markersize=5,
                        linestyle='--', alpha=0.55, color=color, label=pipe_label)

        be_vals = _threshold_series(pivot_df.index, BREAKEVEN_INDIVIDUAL_PRE, BREAKEVEN_INDIVIDUAL_POST)
        tg_vals = _threshold_series(pivot_df.index, INDUSTRY_INDIVIDUAL_PRE, INDUSTRY_INDIVIDUAL_POST)
        ax.plot(x_labels, be_vals, color='red', linestyle='--', linewidth=2,
                label='Individual Breakeven', drawstyle='steps-post')
        ax.plot(x_labels, tg_vals, color='orange', linestyle='--', linewidth=2,
                label='Industry Standard', drawstyle='steps-post')
        ax.set_title('Provider Weekly RVU Comparison', fontsize=14, fontweight='bold')

    elif view_type in [p.title() for p in valid_providers] or view_type.upper() in valid_providers:
        # Individual Provider
        provider_upper = view_type.upper()
        if exclude_providers and provider_upper in exclude_providers:
            return _generate_error_chart("Access denied.")
        display_name = next((p.title() for p in valid_providers if p == provider_upper), view_type)

        prov_data = confirmed_only[confirmed_only['Provider'] == provider_upper].groupby('Week')['RVU'].sum().reset_index().sort_values('Week')
        if prov_data.empty:
            return _generate_error_chart(f"No RVU data found for {display_name}.")

        x_labels = prov_data['Week'].dt.strftime('%Y-%m-%d').tolist()
        ax.plot(x_labels, prov_data['RVU'].astype(float).tolist(), marker='o', color='#2ecc71', linewidth=2.5, markersize=8, label=f'{display_name} Earned RVUs')

        if has_pipeline and not pipeline_only.empty:
            pipe_prov = pipeline_only[pipeline_only['Provider'] == provider_upper].groupby('Week')['RVU'].sum()
            if not pipe_prov.empty:
                confirmed_prov = prov_data.set_index('Week')['RVU']
                combined = confirmed_prov.add(pipe_prov, fill_value=0).sort_index()
                pipe_x = combined.index.strftime('%Y-%m-%d').tolist()
                ax.plot(pipe_x, combined.astype(float).tolist(), marker='o', color='#2ecc71',
                        linewidth=1.5, markersize=6, linestyle='--', alpha=0.55, label='Pipeline (draft charges)')

        be_vals = _threshold_series(prov_data['Week'], BREAKEVEN_INDIVIDUAL_PRE, BREAKEVEN_INDIVIDUAL_POST)
        tg_vals = _threshold_series(prov_data['Week'], INDUSTRY_INDIVIDUAL_PRE, INDUSTRY_INDIVIDUAL_POST)
        ax.plot(x_labels, be_vals, color='red', linestyle='--', linewidth=2,
                label='Individual Breakeven', drawstyle='steps-post')
        ax.plot(x_labels, tg_vals, color='orange', linestyle='--', linewidth=2,
                label='Industry Standard', drawstyle='steps-post')
        ax.set_title(f'{display_name} - Weekly RVUs', fontsize=14, fontweight='bold')

    else:
        return _generate_error_chart("Invalid chart view type requested.")

    # Shade last 3 weeks with "recent data may be incomplete" annotation
    n = len(x_labels)
    if n >= 3:
        shade_start = max(n - 3 - 0.5, -0.5)
        shade_end = n - 1 + 0.5
        ax.axvspan(shade_start, shade_end, alpha=0.07, color='#f97316', zorder=0)
        ymin, ymax = ax.get_ylim()
        ax.text(
            n - 2, ymax * 0.98,
            'Recent weeks may be incomplete\n— claims still processing',
            ha='center', va='top', fontsize=7.5, color='#92400e',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#fff7ed', edgecolor='#f97316', alpha=0.85),
            zorder=5,
        )

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


_DRAFT_CONVERSION_RATE_DEFAULT = 0.85


def _compute_draft_conversion_rate(df):
    """Estimate what fraction of draft/pipeline RVUs will become confirmed.

    Uses the ratio of confirmed to total (confirmed + pipeline) RVUs across
    the full dataset.  Because old quarters are fully confirmed (pipeline≈0
    after dedup), this ratio is anchored near 1.0 by history and dips only
    slightly due to the current quarter's pending drafts — giving a realistic
    empirical conversion estimate.  Falls back to 0.85 when no Source column
    or insufficient data is available.
    """
    if df is None or df.empty or 'Source' not in df.columns:
        return _DRAFT_CONVERSION_RATE_DEFAULT
    confirmed = df[df['Source'] == 'confirmed']['RVU'].sum()
    pipeline = df[df['Source'] == 'pipeline']['RVU'].sum()
    total = confirmed + pipeline
    if total == 0:
        return _DRAFT_CONVERSION_RATE_DEFAULT
    return max(0.5, min(1.0, confirmed / total))


def get_quarterly_bonus_report(today=None, data_source='all', include_pipeline=False):
    """Return per-provider bonus data for the current calendar quarter.

    Returns a dict with 'quarter_label', 'days_elapsed', 'days_in_quarter',
    and 'providers' (list of per-provider dicts).

    Args:
        data_source:      'all' (default), 'pc', or 'va' — passed to get_rvu_dataset().
        include_pipeline: When True, each provider dict gains 'pipeline_rvus'
                          and 'total_with_pipeline' columns showing draft charges
                          not yet approved.
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

    # Always load pipeline so we can compute draft estimates for the current
    # quarter regardless of whether the caller requested pipeline columns.
    df = get_rvu_dataset(data_source=data_source, include_pipeline=True)
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

    # Source column is always present because we loaded with include_pipeline=True
    qdf_confirmed = qdf[qdf['Source'] == 'confirmed'] if 'Source' in qdf.columns else qdf
    qdf_pipeline  = qdf[qdf['Source'] == 'pipeline']  if 'Source' in qdf.columns else pd.DataFrame()

    # Include the Anne Jenks manual 216-RVU adjustment, prorated to this quarter
    # (applied to confirmed data only)
    all_weeks     = df[df['Source'] == 'confirmed']['Week'].unique() if 'Source' in df.columns else df['Week'].unique()
    quarter_weeks = qdf_confirmed['Week'].unique()
    if len(all_weeks) > 0 and len(quarter_weeks) > 0:
        adjustment_this_quarter = 216.0 * len(quarter_weeks) / len(all_weeks)
    else:
        adjustment_this_quarter = 0.0

    # Sum confirmed RVUs per provider
    provider_rvus = qdf_confirmed.groupby('Provider')['RVU'].sum()

    # Sum pipeline (draft) RVUs per provider — always computed
    provider_pipeline_rvus = (
        qdf_pipeline.groupby('Provider')['RVU'].sum()
        if not qdf_pipeline.empty else pd.Series(dtype=float)
    )

    # Empirical draft-to-confirmed conversion rate from full dataset history
    draft_conversion_rate = _compute_draft_conversion_rate(df)

    providers = []
    for prov in data_loader.VALID_PROVIDERS:
        rvus_raw = float(provider_rvus.get(prov, 0.0))

        # Add Anne Jenks manual adjustment before flooring
        if prov == 'ANNE JENKS':
            rvus_raw += adjustment_this_quarter

        # Floor to nearest whole number before bonus calculation
        rvus_earned = math.floor(rvus_raw)

        bonus_earned = calculate_bonus(rvus_earned)

        # Extrapolate to full quarter (project from floored base, then floor again)
        if days_elapsed > 0:
            projected_rvus = rvus_earned * days_in_quarter / days_elapsed
        else:
            projected_rvus = 0.0
        projected_bonus = calculate_bonus(math.floor(projected_rvus))

        # Draft (pipeline) RVUs for current quarter — always returned
        draft_rvus_raw = float(provider_pipeline_rvus.get(prov, 0.0))
        estimated_additional = math.floor(draft_rvus_raw * draft_conversion_rate)
        estimated_total_rvus = rvus_earned + estimated_additional

        entry = {
            'provider': prov.title(),
            'rvus_earned': rvus_earned,
            'bonus_earned': round(bonus_earned, 2),
            'projected_rvus': round(projected_rvus, 1),
            'projected_bonus': round(projected_bonus, 2),
            'draft_rvus': round(draft_rvus_raw, 1),
            'estimated_total_rvus': estimated_total_rvus,
        }

        if include_pipeline:
            entry['pipeline_rvus'] = round(draft_rvus_raw, 1)
            entry['total_with_pipeline'] = rvus_earned + round(draft_rvus_raw, 1)

        providers.append(entry)

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
