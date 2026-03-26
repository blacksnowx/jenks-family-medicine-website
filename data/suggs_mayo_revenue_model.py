"""
Suggs + Mayo Combined Revenue Model — 2-Year Forecast
=====================================================
Two versions:
  V1: Status quo — extrapolate current combined VA + PC revenue trends
  V2: Modified — Sarah capped at 10 VA/week + growing PC panel
                  Heather capped at 18 VA/week, no PC
                  Both providers get 4 weeks off per year

Uses:
  - VA exam revenue from 201 Bills and Payments.csv (fee schedule via data_loader)
  - PC revenue from Charges Export.csv (via data_loader)
  - PC revenue driver slope from pc_revenue_drivers regression
  - Historical weekly exam distributions for variance modeling

Run directly: python -m data.suggs_mayo_revenue_model
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from scipy import stats as scipy_stats
from datetime import timedelta
import os
import sys

np.random.seed(42)

# Allow running as a script or as a module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import data_loader

# ============================================
# 1. LOAD AND PREPARE DATA (using shared data_loader)
# ============================================
print("Loading data...")

# --- VA data ---
va_df = data_loader.load_va_data()
va_df['DOS'] = va_df['Date of Service']

# Assign Patient_ID from first non-standard column if needed
if 'Patient_ID' not in va_df.columns and len(va_df.columns) > 0:
    patient_col = [c for c in va_df.columns if c not in ('Provider', 'Date of Service', 'Week', 'VA_Revenue', 'DOS')][0]
    va_df = va_df.rename(columns={patient_col: 'Patient_ID'})

# Provider flags
va_df['Is_Suggs'] = va_df['Provider'].str.contains('sarah suggs', case=False, na=False)
va_df['Is_Mayo'] = va_df['Provider'].str.contains('heather mayo', case=False, na=False)

# --- PC data ---
charges = data_loader.load_pc_data()
charges = charges.dropna(subset=['Date Of Service', 'Patient ID'])

# PC_Revenue already computed by data_loader

# Weekly revenue report (for historical monthly totals)
weekly_rev = pd.read_csv(os.path.join(data_loader.BASE_DIR, 'weekly_revenue_report.csv'))
weekly_rev['Week'] = pd.to_datetime(weekly_rev['Week'])
for col in ['VA_Revenue', 'PC_Revenue', 'PC_Charges']:
    if col in weekly_rev.columns:
        weekly_rev[col] = weekly_rev[col].apply(data_loader.clean_currency)

# ============================================
# 2. COMPUTE KEY PARAMETERS
# ============================================

# --- VA revenue per exam (by provider) ---
# Aggregate VA revenue per exam for each provider from the fee schedule
suggs_va = va_df[va_df['Is_Suggs']].copy()
mayo_va = va_df[va_df['Is_Mayo']].copy()

# Per-exam revenue (averaged across all exams)
# Use weekly revenue report total VA / total exam count for a more accurate avg
# Filter to 2025+ for relevance
va_2025 = va_df[va_df['DOS'] >= '2025-01-01'].copy()
rev_2025 = weekly_rev[weekly_rev['Week'] >= '2025-01-01']
total_va_rev_2025 = rev_2025['VA_Revenue'].sum()
total_exams_2025 = len(va_2025.drop_duplicates(subset=['Patient_ID', 'DOS', 'Provider']))
VA_REV_PER_EXAM = total_va_rev_2025 / total_exams_2025
print(f"\nVA revenue per exam (2025+): ${VA_REV_PER_EXAM:.2f}")
print(f"  Total VA revenue 2025+: ${total_va_rev_2025:,.0f}")
print(f"  Total deduped exams 2025+: {total_exams_2025}")

# --- PC revenue per patient per month (slope from regression) ---
# From pc_revenue_drivers.py: PC_Revenue = 16.18 * Panel_Size + intercept
# User says: ignore intercept, just use slope for incremental revenue
PC_SLOPE = 16.18  # $ per panel patient per month (from regression)

# --- Company-wide avg PC revenue per visit ---
PC_REV_PER_VISIT = charges['PC_Revenue'].mean()
print(f"\nCompany-wide avg PC revenue per visit: ${PC_REV_PER_VISIT:.2f}")

# --- Sarah's current PC panel ---
prov_col = 'Rendering Provider' if 'Rendering Provider' in charges.columns else 'Provider'
suggs_pc = charges[charges[prov_col].str.contains('SUGGS|Sarah Suggs', case=False, na=False)]
SARAH_CURRENT_PANEL = suggs_pc['Patient ID'].nunique()
print(f"Sarah's current cumulative PC panel: {SARAH_CURRENT_PANEL}")

# --- Historical weekly VA exam distributions (2025+, per provider) ---
va_2025_deduped = va_2025.drop_duplicates(subset=['Patient_ID', 'DOS', 'Provider'], keep='first')
va_2025_deduped['Week'] = va_2025_deduped['DOS'].dt.to_period('W')

suggs_weekly_hist = va_2025_deduped[va_2025_deduped['Is_Suggs']].groupby('Week').size().values
mayo_weekly_hist = va_2025_deduped[va_2025_deduped['Is_Mayo']].groupby('Week').size().values

# Cap at 18/provider/week as we determined
suggs_weekly_hist = np.clip(suggs_weekly_hist, 0, 18)
mayo_weekly_hist = np.clip(mayo_weekly_hist, 0, 18)

print(f"\nSuggs weekly VA exams: mean={suggs_weekly_hist.mean():.1f}, std={suggs_weekly_hist.std():.1f}")
print(f"Mayo weekly VA exams:  mean={mayo_weekly_hist.mean():.1f}, std={mayo_weekly_hist.std():.1f}")

# ============================================
# 3. HISTORICAL MONTHLY REVENUE (Suggs + Mayo)
# ============================================

# Monthly VA revenue by provider (using fee_calc on each row, then sum per month)
va_2025_deduped['Month'] = va_2025_deduped['DOS'].dt.to_period('M')

# For provider-specific VA revenue, sum fee_calc output per month
# But fee_calc may undercount on deduped rows. Use proportion instead.
# Suggs + Mayo share of total exams * total VA revenue
suggs_exam_monthly = va_2025_deduped[va_2025_deduped['Is_Suggs']].groupby('Month').size()
mayo_exam_monthly = va_2025_deduped[va_2025_deduped['Is_Mayo']].groupby('Month').size()
all_exam_monthly = va_2025_deduped.groupby('Month').size()

# Monthly VA revenue from weekly report
weekly_rev['Month'] = weekly_rev['Week'].dt.to_period('M')
va_monthly_total = weekly_rev[weekly_rev['Week'] >= '2025-01-01'].groupby('Month').agg(
    VA_Revenue=('VA_Revenue', 'sum'),
    PC_Revenue=('PC_Revenue', 'sum'),
    Weeks=('Week', 'count'),
).reset_index()

# Filter to full months
va_monthly_total = va_monthly_total[va_monthly_total['Weeks'] >= 3].copy()

# Calculate Suggs+Mayo share of VA revenue each month
hist_months = []
for _, row in va_monthly_total.iterrows():
    m = row['Month']
    s_exams = suggs_exam_monthly.get(m, 0)
    m_exams = mayo_exam_monthly.get(m, 0)
    total_exams = all_exam_monthly.get(m, 1)
    sm_share = (s_exams + m_exams) / total_exams if total_exams > 0 else 0

    # Apply 18/provider/week cap to exam counts before computing share
    s_capped = min(s_exams, 18 * row['Weeks'])
    m_capped = min(m_exams, 18 * row['Weeks'])
    sm_capped = s_capped + m_capped

    hist_months.append({
        'Month': m,
        'Weeks': int(row['Weeks']),
        'Suggs_Exams': s_exams,
        'Mayo_Exams': m_exams,
        'SM_VA_Revenue': sm_capped * VA_REV_PER_EXAM,
        'Total_VA_Revenue': row['VA_Revenue'],
        'Total_PC_Revenue': row['PC_Revenue'],
    })

hist_df = pd.DataFrame(hist_months)

# Sarah's PC revenue by month
suggs_pc_2025 = suggs_pc[suggs_pc['Date Of Service'] >= '2025-01-01'].copy()
suggs_pc_2025['Month'] = suggs_pc_2025['Date Of Service'].dt.to_period('M')
sarah_pc_monthly = suggs_pc_2025.groupby('Month').agg(
    Sarah_PC_Revenue=('PC_Revenue', 'sum')
).reset_index()

hist_df = hist_df.merge(sarah_pc_monthly, on='Month', how='left')
hist_df['Sarah_PC_Revenue'] = hist_df['Sarah_PC_Revenue'].fillna(0)

# Combined revenue for Suggs + Mayo
hist_df['SM_Total_Revenue'] = hist_df['SM_VA_Revenue'] + hist_df['Sarah_PC_Revenue']

# ============================================
# 4. PARAMETERS
# ============================================
FORECAST_MONTHS = 24
WEEKS_PER_YEAR = 48  # 4 weeks off
WEEKS_OFF_PER_YEAR = 4

# V2 caps
V2_SUGGS_VA_CAP = 10  # exams/week
V2_MAYO_VA_CAP = 18   # exams/week
V2_PC_GROWTH_RATE = 12  # new patients/week

# Forecast start: March 2026 (first full month after data)
forecast_start = pd.Period('2026-03', freq='M')

# ============================================
# 5. VERSION 1 — STATUS QUO (FLAT DEMAND)
# ============================================
print(f"\n{'=' * 70}")
print("  VERSION 1 — STATUS QUO (FLAT VA DEMAND)")
print(f"{'=' * 70}")

# Use last 6 months of actual data as the steady-state baseline
recent_hist = hist_df.tail(6).copy()

# Flat VA and PC baselines — last 6 months average
va_avg = recent_hist['SM_VA_Revenue'].mean()
va_std = recent_hist['SM_VA_Revenue'].std()
pc_avg = recent_hist['Sarah_PC_Revenue'].mean()
pc_std = recent_hist['Sarah_PC_Revenue'].std()

print(f"\n  Baseline (last 6 months avg):")
print(f"    VA revenue/month (Suggs+Mayo): ${va_avg:,.0f} (σ=${va_std:,.0f})")
print(f"    PC revenue/month (Sarah):     ${pc_avg:,.0f} (σ=${pc_std:,.0f})")
print(f"    Combined monthly avg:         ${va_avg + pc_avg:,.0f}")

# Generate V1 forecast — flat demand with realistic variance
v1_forecast = []

for i in range(FORECAST_MONTHS):
    month = forecast_start + i

    # Flat baseline with historical variance
    va_rev = va_avg + np.random.normal(0, va_std * 0.7)
    va_rev = max(0, va_rev)
    pc_rev = pc_avg + np.random.normal(0, pc_std * 0.7)
    pc_rev = max(0, pc_rev)

    v1_forecast.append({
        'Month': month,
        'VA_Revenue': va_rev,
        'PC_Revenue': pc_rev,
        'Total_Revenue': va_rev + pc_rev,
    })

v1_df = pd.DataFrame(v1_forecast)

print(f"\n  V1 Forecast Summary (24 months):")
print(f"    Year 1 total: ${v1_df.head(12)['Total_Revenue'].sum():,.0f}")
print(f"    Year 2 total: ${v1_df.tail(12)['Total_Revenue'].sum():,.0f}")
print(f"    Monthly avg:  ${v1_df['Total_Revenue'].mean():,.0f}")

# ============================================
# 6. VERSION 2 — CAPPED VA + PC GROWTH
# ============================================
print(f"\n{'=' * 70}")
print("  VERSION 2 — CAPPED VA + PC PANEL GROWTH")
print(f"{'=' * 70}")

print(f"\n  Parameters:")
print(f"    Sarah VA cap:      {V2_SUGGS_VA_CAP} exams/week")
print(f"    Heather VA cap:    {V2_MAYO_VA_CAP} exams/week")
print(f"    Sarah PC growth:   {V2_PC_GROWTH_RATE} new patients/week")
print(f"    Working weeks/yr:  {WEEKS_PER_YEAR}")
print(f"    Starting panel:    {SARAH_CURRENT_PANEL}")
print(f"    PC slope:          ${PC_SLOPE:.2f}/patient/month")

# Week-by-week simulation
total_weeks = FORECAST_MONTHS * 52 // 12  # ~104 weeks in 24 months

# Build historical demand distributions for sampling
# Separate distributions
suggs_demand_mean = suggs_weekly_hist.mean()
suggs_demand_std = suggs_weekly_hist.std()
mayo_demand_mean = mayo_weekly_hist.mean()
mayo_demand_std = mayo_weekly_hist.std()

print(f"\n  Historical demand (uncapped):")
print(f"    Suggs: μ={suggs_demand_mean:.1f}, σ={suggs_demand_std:.1f}")
print(f"    Mayo:  μ={mayo_demand_mean:.1f}, σ={mayo_demand_std:.1f}")

# Simulate week by week
v2_weeks = []
sarah_panel = SARAH_CURRENT_PANEL
suggs_overflow = 0  # overflow from previous week
mayo_overflow = 0

# Determine vacation weeks (distribute 4 per year evenly)
# Weeks off: 1 in each quarter roughly
vacation_weeks_per_half_year = 2
total_sim_weeks = int(FORECAST_MONTHS * 4.33)

# Create vacation schedule
vacation_weeks = set()
for year_offset in range(3):  # cover 2+ years
    base_week = year_offset * 52
    # Distribute 4 vacation weeks per year
    for q in range(4):
        vac_week = base_week + q * 13 + np.random.randint(10, 13)
        if vac_week < total_sim_weeks:
            vacation_weeks.add(vac_week)

for w in range(total_sim_weeks):
    is_vacation = w in vacation_weeks

    if is_vacation:
        # No exams, no new PC patients during vacation
        v2_weeks.append({
            'Week': w,
            'Vacation': True,
            'Suggs_Demand': 0,
            'Mayo_Demand': 0,
            'Suggs_VA_Exams': 0,
            'Mayo_VA_Exams': 0,
            'Suggs_Overflow_Lost': 0,
            'Mayo_Overflow_Lost': 0,
            'Sarah_Panel': sarah_panel,
            'Suggs_VA_Revenue': 0,
            'Mayo_VA_Revenue': 0,
            'Sarah_PC_Revenue': 0,
            'Total_Revenue': 0,
        })
        continue

    # Sample demand from historical distribution (truncated at 0)
    suggs_demand = max(0, round(np.random.normal(suggs_demand_mean, suggs_demand_std)))
    mayo_demand = max(0, round(np.random.normal(mayo_demand_mean, mayo_demand_std)))

    # Add overflow from previous week (partial: 50% of excess carries over)
    suggs_demand_total = suggs_demand + suggs_overflow
    mayo_demand_total = mayo_demand + mayo_overflow

    # Apply caps
    suggs_exams = min(suggs_demand_total, V2_SUGGS_VA_CAP)
    mayo_exams = min(mayo_demand_total, V2_MAYO_VA_CAP)

    # Calculate overflow
    suggs_excess = max(0, suggs_demand_total - V2_SUGGS_VA_CAP)
    mayo_excess = max(0, mayo_demand_total - V2_MAYO_VA_CAP)

    # Some of Sarah's overflow goes to Heather (if she has capacity)
    heather_capacity = V2_MAYO_VA_CAP - mayo_exams
    suggs_to_heather = min(suggs_excess, heather_capacity)
    mayo_exams += suggs_to_heather
    suggs_excess -= suggs_to_heather

    # Remaining excess: 50% carries to next week, 50% lost
    suggs_overflow = int(suggs_excess * 0.5)
    mayo_overflow = int(mayo_excess * 0.5)
    suggs_lost = suggs_excess - suggs_overflow
    mayo_lost = mayo_excess - mayo_overflow

    # VA Revenue
    suggs_va_rev = suggs_exams * VA_REV_PER_EXAM
    mayo_va_rev = mayo_exams * VA_REV_PER_EXAM

    # PC Revenue — Sarah's panel grows
    sarah_panel += V2_PC_GROWTH_RATE  # 12 new patients per working week
    # PC revenue = slope * panel_size (ignore intercept per user)
    sarah_pc_rev_monthly = PC_SLOPE * sarah_panel
    sarah_pc_rev_weekly = sarah_pc_rev_monthly / 4.33  # weekly portion

    # Total week revenue
    total_rev = suggs_va_rev + mayo_va_rev + sarah_pc_rev_weekly

    v2_weeks.append({
        'Week': w,
        'Vacation': False,
        'Suggs_Demand': suggs_demand,
        'Mayo_Demand': mayo_demand,
        'Suggs_VA_Exams': suggs_exams,
        'Mayo_VA_Exams': mayo_exams,
        'Suggs_Overflow_Lost': suggs_lost,
        'Mayo_Overflow_Lost': mayo_lost,
        'Sarah_Panel': sarah_panel,
        'Suggs_VA_Revenue': suggs_va_rev,
        'Mayo_VA_Revenue': mayo_va_rev,
        'Sarah_PC_Revenue': sarah_pc_rev_weekly,
        'Total_Revenue': total_rev,
    })

v2_weekly = pd.DataFrame(v2_weeks)

# Aggregate to monthly (roughly 4.33 weeks/month)
v2_weekly['Month_Num'] = v2_weekly['Week'] // 4  # approximate months
v2_monthly_agg = v2_weekly.groupby('Month_Num').agg(
    Weeks=('Week', 'count'),
    Working_Weeks=('Vacation', lambda x: (~x).sum()),
    Suggs_VA_Exams=('Suggs_VA_Exams', 'sum'),
    Mayo_VA_Exams=('Mayo_VA_Exams', 'sum'),
    Suggs_VA_Revenue=('Suggs_VA_Revenue', 'sum'),
    Mayo_VA_Revenue=('Mayo_VA_Revenue', 'sum'),
    Sarah_PC_Revenue=('Sarah_PC_Revenue', 'sum'),
    Total_Revenue=('Total_Revenue', 'sum'),
    Suggs_Lost=('Suggs_Overflow_Lost', 'sum'),
    Mayo_Lost=('Mayo_Overflow_Lost', 'sum'),
    Panel_End=('Sarah_Panel', 'last'),
).reset_index()

# Assign proper month labels
v2_monthly_agg['Month'] = [forecast_start + i for i in range(len(v2_monthly_agg))]
v2_monthly_agg = v2_monthly_agg.head(FORECAST_MONTHS)  # trim to 24 months

v2_monthly_agg['VA_Revenue'] = v2_monthly_agg['Suggs_VA_Revenue'] + v2_monthly_agg['Mayo_VA_Revenue']

print(f"\n  V2 Forecast Summary (24 months):")
print(f"    Year 1 VA revenue:  ${v2_monthly_agg.head(12)['VA_Revenue'].sum():,.0f}")
print(f"    Year 1 PC revenue:  ${v2_monthly_agg.head(12)['Sarah_PC_Revenue'].sum():,.0f}")
print(f"    Year 1 total:       ${v2_monthly_agg.head(12)['Total_Revenue'].sum():,.0f}")
print(f"    Year 2 VA revenue:  ${v2_monthly_agg.tail(12)['VA_Revenue'].sum():,.0f}")
print(f"    Year 2 PC revenue:  ${v2_monthly_agg.tail(12)['Sarah_PC_Revenue'].sum():,.0f}")
print(f"    Year 2 total:       ${v2_monthly_agg.tail(12)['Total_Revenue'].sum():,.0f}")
print(f"    Panel at Year 1 end: {int(v2_monthly_agg.iloc[11]['Panel_End']):,}")
print(f"    Panel at Year 2 end: {int(v2_monthly_agg.iloc[-1]['Panel_End']):,}")

# Total exams lost
total_suggs_lost = int(v2_monthly_agg['Suggs_Lost'].sum())
total_mayo_lost = int(v2_monthly_agg['Mayo_Lost'].sum())
print(f"    Total Suggs exams lost: {total_suggs_lost}")
print(f"    Total Mayo exams lost:  {total_mayo_lost}")

# ============================================
# 7. COMPARISON TABLE
# ============================================
print(f"\n{'=' * 70}")
print("  MONTHLY FORECAST COMPARISON — V1 vs V2")
print(f"{'=' * 70}")
print(f"\n  {'Month':<10} {'V1 Total':>10} {'V2 VA':>10} {'V2 PC':>10} {'V2 Total':>10} {'Panel':>8} {'Δ(V2-V1)':>10}")
print(f"  {'-' * 68}")

for i in range(FORECAST_MONTHS):
    m = str(v1_df.iloc[i]['Month'])
    v1_total = v1_df.iloc[i]['Total_Revenue']
    v2_va = v2_monthly_agg.iloc[i]['VA_Revenue']
    v2_pc = v2_monthly_agg.iloc[i]['Sarah_PC_Revenue']
    v2_total = v2_monthly_agg.iloc[i]['Total_Revenue']
    panel = int(v2_monthly_agg.iloc[i]['Panel_End'])
    delta = v2_total - v1_total

    print(f"  {m:<10} ${v1_total:>9,.0f} ${v2_va:>9,.0f} ${v2_pc:>9,.0f} ${v2_total:>9,.0f} {panel:>7,} ${delta:>+9,.0f}")

# Cumulative comparison
v1_cum = v1_df['Total_Revenue'].cumsum()
v2_cum = v2_monthly_agg['Total_Revenue'].cumsum()

# Find crossover month
crossover_month = None
for i in range(FORECAST_MONTHS):
    if v2_cum.iloc[i] >= v1_cum.iloc[i]:
        crossover_month = i + 1
        break

print(f"\n  V1 Cumulative (24 mo): ${v1_cum.iloc[-1]:,.0f}")
print(f"  V2 Cumulative (24 mo): ${v2_cum.iloc[-1]:,.0f}")
print(f"  Difference:            ${v2_cum.iloc[-1] - v1_cum.iloc[-1]:+,.0f}")
if crossover_month:
    print(f"  V2 exceeds V1 at month: {crossover_month} ({str(forecast_start + crossover_month - 1)})")
else:
    print(f"  V2 does NOT exceed V1 within 24 months")

# ============================================
# 8. CHARTS
# ============================================
fig, axes = plt.subplots(2, 2, figsize=(18, 14))
fig.suptitle('Suggs + Mayo Combined Revenue Model — 2-Year Forecast\nV1 (Status Quo, Flat VA Demand) vs V2 (Capped VA + PC Growth)',
             fontsize=16, fontweight='bold', y=0.98)

months_v1 = [str(m) for m in v1_df['Month']]
months_v2 = [str(m) for m in v2_monthly_agg['Month']]

# Historical months for the combined chart
hist_months_str = [str(m) for m in hist_df['Month']]
hist_revenue = hist_df['SM_Total_Revenue'].values

# Chart A: Monthly Total Revenue — V1 vs V2
ax = axes[0, 0]

# Plot historical
x_hist = np.arange(len(hist_df))
ax.bar(x_hist, hist_revenue, color='#94a3b8', alpha=0.6, label='Historical', width=0.8)

# Plot V1 and V2
x_fc = np.arange(len(hist_df), len(hist_df) + FORECAST_MONTHS)
ax.plot(x_fc, v1_df['Total_Revenue'], color='#ef4444', linewidth=2.5, marker='o',
        markersize=4, label='V1: Status Quo', zorder=5)
ax.plot(x_fc, v2_monthly_agg['Total_Revenue'].values, color='#10b981', linewidth=2.5,
        marker='s', markersize=4, label='V2: Capped + PC Growth', zorder=5)

# Divider line
ax.axvline(x=len(hist_df) - 0.5, color='black', linestyle='--', alpha=0.5, linewidth=1)
ax.text(len(hist_df) - 0.3, ax.get_ylim()[1] * 0.95, '← Actual | Forecast →',
        fontsize=8, ha='center', va='top', style='italic')

all_labels = hist_months_str + months_v1
tick_positions = list(range(0, len(all_labels), 3))
ax.set_xticks(tick_positions)
ax.set_xticklabels([all_labels[i] for i in tick_positions], rotation=45, ha='right', fontsize=7)
ax.set_ylabel('Monthly Revenue ($)', fontsize=11)
ax.set_title('A) Monthly Total Revenue — V1 vs V2', fontsize=13, fontweight='bold')
ax.legend(fontsize=9, loc='upper left')
ax.grid(True, alpha=0.3, axis='y')
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))

# Chart B: Cumulative Revenue — V1 vs V2
ax = axes[0, 1]
hist_cum = hist_df['SM_Total_Revenue'].cumsum().values
v1_cum_vals = v1_cum.values + hist_cum[-1]
v2_cum_vals = v2_cum.values + hist_cum[-1]

ax.plot(x_hist, hist_cum, color='#94a3b8', linewidth=2, label='Historical')
ax.plot(x_fc, v1_cum_vals, color='#ef4444', linewidth=2.5, label='V1: Status Quo')
ax.plot(x_fc, v2_cum_vals, color='#10b981', linewidth=2.5, label='V2: Capped + PC Growth')

if crossover_month:
    cross_x = len(hist_df) + crossover_month - 1
    cross_y = v2_cum_vals[crossover_month - 1]
    ax.axvline(x=cross_x, color='#10b981', linestyle=':', alpha=0.5)
    ax.annotate(f'V2 overtakes V1\n(Month {crossover_month})',
                xy=(cross_x, cross_y), xytext=(cross_x + 3, cross_y * 0.85),
                arrowprops=dict(arrowstyle='->', color='#10b981'),
                fontsize=9, color='#10b981', fontweight='bold')

ax.axvline(x=len(hist_df) - 0.5, color='black', linestyle='--', alpha=0.5)
ax.set_xticks(tick_positions)
ax.set_xticklabels([all_labels[i] for i in tick_positions], rotation=45, ha='right', fontsize=7)
ax.set_ylabel('Cumulative Revenue ($)', fontsize=11)
ax.set_title('B) Cumulative Revenue — V1 vs V2', fontsize=13, fontweight='bold')
ax.legend(fontsize=9, loc='upper left')
ax.grid(True, alpha=0.3, axis='y')
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))

# Chart C: V2 Revenue Breakdown (VA vs PC)
ax = axes[1, 0]
x_fc_c = np.arange(FORECAST_MONTHS)
va_vals = v2_monthly_agg['VA_Revenue'].values
pc_vals = v2_monthly_agg['Sarah_PC_Revenue'].values

ax.bar(x_fc_c, va_vals, color='#3b82f6', alpha=0.8, label='VA Revenue (Suggs + Mayo)')
ax.bar(x_fc_c, pc_vals, bottom=va_vals, color='#f59e0b', alpha=0.8, label='PC Revenue (Sarah)')

# Add panel size on secondary axis
ax2 = ax.twinx()
ax2.plot(x_fc_c, v2_monthly_agg['Panel_End'].values, color='#ef4444', linewidth=2,
         linestyle='--', marker='.', markersize=4, label='Sarah Panel Size')
ax2.set_ylabel('Panel Size', fontsize=11, color='#ef4444')
ax2.tick_params(axis='y', labelcolor='#ef4444')

ax.set_xticks(range(0, FORECAST_MONTHS, 3))
ax.set_xticklabels([months_v2[i] for i in range(0, FORECAST_MONTHS, 3)], rotation=45, ha='right', fontsize=7)
ax.set_ylabel('Monthly Revenue ($)', fontsize=11)
ax.set_title('C) V2 Revenue Breakdown — VA vs PC', fontsize=13, fontweight='bold')
ax.legend(fontsize=9, loc='upper left')
ax2.legend(fontsize=9, loc='upper right')
ax.grid(True, alpha=0.3, axis='y')
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))

# Chart D: Key Metrics Summary
ax = axes[1, 1]
ax.axis('off')

v1_y1 = v1_df.head(12)['Total_Revenue'].sum()
v1_y2 = v1_df.tail(12)['Total_Revenue'].sum()
v2_y1 = v2_monthly_agg.head(12)['Total_Revenue'].sum()
v2_y2 = v2_monthly_agg.tail(12)['Total_Revenue'].sum()
v2_y1_va = v2_monthly_agg.head(12)['VA_Revenue'].sum()
v2_y2_va = v2_monthly_agg.tail(12)['VA_Revenue'].sum()
v2_y1_pc = v2_monthly_agg.head(12)['Sarah_PC_Revenue'].sum()
v2_y2_pc = v2_monthly_agg.tail(12)['Sarah_PC_Revenue'].sum()

summary_text = (
    f"VERSION 1 — STATUS QUO\n"
    f"{'─' * 40}\n"
    f"  Year 1 total:     ${v1_y1:>12,.0f}\n"
    f"  Year 2 total:     ${v1_y2:>12,.0f}\n"
    f"  2-Year total:     ${v1_y1 + v1_y2:>12,.0f}\n"
    f"  Monthly avg:      ${v1_df['Total_Revenue'].mean():>12,.0f}\n"
    f"\n"
    f"VERSION 2 — CAPPED VA + PC GROWTH\n"
    f"{'─' * 40}\n"
    f"  Sarah: 10 VA/wk + 12 new PC pts/wk\n"
    f"  Heather: 18 VA/wk\n"
    f"  Starting panel: {SARAH_CURRENT_PANEL}\n"
    f"\n"
    f"  Year 1:  VA ${v2_y1_va:>10,.0f}  PC ${v2_y1_pc:>10,.0f}\n"
    f"           Total ${v2_y1:>10,.0f}\n"
    f"  Year 2:  VA ${v2_y2_va:>10,.0f}  PC ${v2_y2_pc:>10,.0f}\n"
    f"           Total ${v2_y2:>10,.0f}\n"
    f"  2-Year:  ${v2_y1 + v2_y2:>12,.0f}\n"
    f"\n"
    f"  Panel at Y1 end:  {int(v2_monthly_agg.iloc[11]['Panel_End']):>6,}\n"
    f"  Panel at Y2 end:  {int(v2_monthly_agg.iloc[-1]['Panel_End']):>6,}\n"
    f"\n"
    f"{'─' * 40}\n"
    f"  2-Year Δ (V2−V1):  ${(v2_y1+v2_y2)-(v1_y1+v1_y2):>+12,.0f}\n"
)

if crossover_month:
    summary_text += f"  V2 overtakes V1:   Month {crossover_month}\n"
else:
    summary_text += f"  V2 does NOT exceed V1 in 24 mo\n"

ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, fontsize=10,
        fontfamily='monospace', verticalalignment='top',
        bbox=dict(boxstyle='round,pad=0.8', facecolor='#f8fafc', edgecolor='#64748b',
                  linewidth=2, alpha=0.95))

plt.tight_layout(rect=[0, 0, 1, 0.95])
output_path = os.path.join(data_loader.BASE_DIR, 'images', 'suggs_mayo_revenue_model.png')
os.makedirs(os.path.dirname(output_path), exist_ok=True)
plt.savefig(output_path, dpi=150, bbox_inches='tight')
print(f"\nChart saved to: {output_path}")
plt.close()

# ============================================
# 9. DETAILED TEXT OUTPUT
# ============================================
print(f"\n{'=' * 70}")
print("  KEY ASSUMPTIONS")
print(f"{'=' * 70}")
print(f"  VA revenue per exam:           ${VA_REV_PER_EXAM:,.2f}")
print(f"  PC slope (per patient/month):  ${PC_SLOPE:.2f}")
print(f"  Company PC rev/visit:          ${PC_REV_PER_VISIT:.2f}")
print(f"  Working weeks/year:            {WEEKS_PER_YEAR}")
print(f"  Sarah starting PC panel:       {SARAH_CURRENT_PANEL}")
print(f"  V2 Sarah VA cap:               {V2_SUGGS_VA_CAP}/week")
print(f"  V2 Heather VA cap:             {V2_MAYO_VA_CAP}/week")
print(f"  V2 PC growth rate:             {V2_PC_GROWTH_RATE} patients/week")

print(f"\n{'=' * 70}")
print("  END OF REPORT")
print(f"{'=' * 70}")
