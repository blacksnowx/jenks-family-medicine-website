"""
Owner-only analytics for Jenks Family Medicine admin dashboard.
Computes Provider Scorecards, Trend Analysis, Breakeven Analysis,
Monthly Aggregation, and Recommendations Engine.
"""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

try:
    from . import rvu_analytics
except ImportError:
    import rvu_analytics

PROVIDERS = ['ANNE JENKS', 'EHRIN IRVIN', 'HEATHER MAYO', 'SARAH SUGGS']
PROVIDER_COLORS = {
    'Anne Jenks': '#37a4db',
    'Ehrin Irvin': '#2ecc71',
    'Heather Mayo': '#f39c12',
    'Sarah Suggs': '#9b59b6',
}


def _threshold_arrays(weeks):
    """Return per-week threshold arrays for the given weeks.

    Weeks before PROVIDER_CUTOVER_WEEK use 4-provider thresholds;
    weeks on/after use 3-provider thresholds.
    """
    post = pd.DatetimeIndex(weeks) >= rvu_analytics.PROVIDER_CUTOVER_WEEK
    return {
        'ind_breakeven': np.where(post, rvu_analytics.BREAKEVEN_INDIVIDUAL_POST, rvu_analytics.BREAKEVEN_INDIVIDUAL_PRE),
        'ind_industry':  np.where(post, rvu_analytics.INDUSTRY_INDIVIDUAL_POST,  rvu_analytics.INDUSTRY_INDIVIDUAL_PRE),
        'co_breakeven':  np.where(post, rvu_analytics.BREAKEVEN_COMPANY_POST,    rvu_analytics.BREAKEVEN_COMPANY_PRE),
        'co_industry':   np.where(post, rvu_analytics.INDUSTRY_COMPANY_POST,     rvu_analytics.INDUSTRY_COMPANY_PRE),
    }


def _get_adjusted_df():
    """Return RVU dataset with Anne Jenks +216 adj applied, excluding the
    current week (t-7 days) since that data hasn't been processed through
    insurance yet and would skew analytics."""
    df = rvu_analytics.get_rvu_dataset()
    if df.empty:
        return df
    # Exclude the most recent 7 days — current week data is incomplete
    cutoff = pd.Timestamp(datetime.now() - timedelta(days=7))
    df = df[pd.to_datetime(df['Week']) <= cutoff]
    if df.empty:
        return df
    unique_weeks = df['Week'].unique()
    if len(unique_weeks) > 0:
        extra_per_week = 216.0 / len(unique_weeks)
        dummy = pd.DataFrame({
            'Week': unique_weeks,
            'Provider': 'ANNE JENKS',
            'RVU': extra_per_week,
            'Category': 'Manual Adjustment'
        })
        df = pd.concat([df, dummy], ignore_index=True)
    return df[df['Provider'].isin(PROVIDERS)].copy()


def _weekly_by_provider(df):
    """Return {provider: weekly_series} and sorted all_weeks list."""
    all_weeks = sorted([pd.Timestamp(w) for w in df['Week'].unique()])
    prov_series = {}
    for p in PROVIDERS:
        pdata = df[df['Provider'] == p].groupby('Week')['RVU'].sum()
        pdata.index = pd.DatetimeIndex(pdata.index)
        pdata = pdata.reindex(pd.DatetimeIndex(all_weeks), fill_value=0.0)
        prov_series[p] = pdata
    return prov_series, all_weeks


def get_scorecards():
    df = _get_adjusted_df()
    if df.empty:
        return {}
    prov_weekly, all_weeks = _weekly_by_provider(df)
    thresholds = _threshold_arrays(all_weeks)
    ind_be_arr = thresholds['ind_breakeven']
    ind_ind_arr = thresholds['ind_industry']
    result = {}
    for p in PROVIDERS:
        arr = prov_weekly[p].values
        n = len(arr)
        if n == 0:
            continue
        recent = float(arr[-4:].mean()) if n >= 4 else float(arr.mean())
        prior = float(arr[-8:-4].mean()) if n >= 8 else float(arr.mean())
        trend_pct = (recent - prior) / prior * 100 if prior > 0 else 0.0
        trend = 'Improving' if trend_pct > 5 else ('Declining' if trend_pct < -5 else 'Stable')
        display = p.title()
        result[display] = {
            'total_rvus': round(float(arr.sum()), 1),
            'avg_weekly': round(float(arr.mean()), 1),
            'breakeven_hit_rate': round(float((arr >= ind_be_arr).mean() * 100), 1),
            'industry_hit_rate': round(float((arr >= ind_ind_arr).mean() * 100), 1),
            'best_week': round(float(arr.max()), 1),
            'worst_week': round(float(arr.min()), 1),
            'std_dev': round(float(arr.std(ddof=1)), 1) if n > 1 else 0.0,
            'trend': trend,
            'trend_pct': round(trend_pct, 1),
            'weeks_count': n,
            'color': PROVIDER_COLORS.get(display, '#888'),
        }
    return result


def get_trends():
    df = _get_adjusted_df()
    if df.empty:
        return {}
    prov_weekly, all_weeks = _weekly_by_provider(df)
    weeks_str = [w.strftime('%Y-%m-%d') for w in all_weeks]
    providers_out = {}
    for p in PROVIDERS:
        s = prov_weekly[p]
        display = p.title()
        providers_out[display] = {
            'weekly': [round(float(v), 1) for v in s.values],
            'rolling4': [round(float(v), 1) for v in s.rolling(4, min_periods=1).mean().values],
            'cumulative': [round(float(v), 1) for v in s.cumsum().values],
            'color': PROVIDER_COLORS.get(display, '#888'),
        }
    company = df.groupby('Week')['RVU'].sum()
    company.index = pd.DatetimeIndex(company.index)
    company = company.reindex(pd.DatetimeIndex(all_weeks), fill_value=0.0)
    wow = company.pct_change().fillna(0).clip(-2, 2) * 100
    thresholds = _threshold_arrays(all_weeks)
    return {
        'weeks': weeks_str,
        'providers': providers_out,
        'company': {
            'weekly': [round(float(v), 1) for v in company.values],
            'wow_pct': [round(float(v), 1) for v in wow.values],
        },
        'benchmarks': {
            'ind_breakeven': [int(v) for v in thresholds['ind_breakeven']],
            'ind_industry':  [int(v) for v in thresholds['ind_industry']],
            'co_breakeven':  [int(v) for v in thresholds['co_breakeven']],
            'co_industry':   [int(v) for v in thresholds['co_industry']],
        },
    }


def get_breakeven_analysis():
    df = _get_adjusted_df()
    if df.empty:
        return {}
    prov_weekly, all_weeks = _weekly_by_provider(df)
    weeks_str = [w.strftime('%Y-%m-%d') for w in all_weeks]
    company = df.groupby('Week')['RVU'].sum()
    company.index = pd.DatetimeIndex(company.index)
    company = company.reindex(pd.DatetimeIndex(all_weeks), fill_value=0.0)
    thresholds = _threshold_arrays(all_weeks)
    co_be_arr = thresholds['co_breakeven']
    ind_be_arr = thresholds['ind_breakeven']
    ind_ind_arr = thresholds['ind_industry']
    co_dist = [round(float(v - t), 1) for v, t in zip(company.values, co_be_arr)]
    providers_out = {}
    for p in PROVIDERS:
        arr = prov_weekly[p].values
        display = p.title()
        dist = [round(float(v - t), 1) for v, t in zip(arr, ind_be_arr)]
        providers_out[display] = {
            'distances': dist,
            'hit_rate': round(float((arr >= ind_be_arr).mean() * 100), 1),
            'industry_hit_rate': round(float((arr >= ind_ind_arr).mean() * 100), 1),
            'avg_weekly': round(float(arr.mean()), 1),
            'color': PROVIDER_COLORS.get(display, '#888'),
        }
    return {
        'weeks': weeks_str,
        'company_distances': co_dist,
        'providers': providers_out,
    }


def get_monthly():
    df = _get_adjusted_df()
    if df.empty:
        return {}
    df = df.copy()
    df['Month'] = pd.to_datetime(df['Week']).dt.to_period('M')
    all_months = sorted(df['Month'].unique())
    months_str = [m.strftime('%b %Y') for m in all_months]
    month_weeks = {
        m: sorted(pd.DatetimeIndex(df[df['Month'] == m]['Week'].unique()))
        for m in all_months
    }
    company = df.groupby('Month')['RVU'].sum().reindex(all_months, fill_value=0.0)
    providers_out = {}
    for p in PROVIDERS:
        s = df[df['Provider'] == p].groupby('Month')['RVU'].sum().reindex(all_months, fill_value=0.0)
        display = p.title()
        providers_out[display] = {
            'values': [round(float(v), 1) for v in s.values],
            'color': PROVIDER_COLORS.get(display, '#888'),
        }

    def _sum_monthly(pre_value, post_value):
        totals = []
        for m in all_months:
            weeks_in_month = month_weeks.get(m, [])
            if not weeks_in_month:
                totals.append(4 * post_value)
                continue
            totals.append(sum(
                post_value if w >= rvu_analytics.PROVIDER_CUTOVER_WEEK else pre_value
                for w in weeks_in_month
            ))
        return totals

    return {
        'months': months_str,
        'providers': providers_out,
        'company_total': [round(float(v), 1) for v in company.values],
        'company_breakeven': _sum_monthly(rvu_analytics.BREAKEVEN_COMPANY_PRE, rvu_analytics.BREAKEVEN_COMPANY_POST),
        'company_industry':  _sum_monthly(rvu_analytics.INDUSTRY_COMPANY_PRE,  rvu_analytics.INDUSTRY_COMPANY_POST),
    }


def get_recommendations():
    df = _get_adjusted_df()
    if df.empty:
        return [{'type': 'info', 'text': 'No data available. Upload CSV files to generate recommendations.'}]

    prov_weekly, all_weeks = _weekly_by_provider(df)
    company = df.groupby('Week')['RVU'].sum()
    company.index = pd.DatetimeIndex(company.index)
    company = company.reindex(pd.DatetimeIndex(all_weeks), fill_value=0.0)
    insights = []
    co_avg = float(company.mean())

    # Use current (post-cutover) thresholds for go-forward recommendations.
    co_breakeven_current = rvu_analytics.BREAKEVEN_COMPANY_POST
    ind_breakeven_current = rvu_analytics.BREAKEVEN_INDIVIDUAL_POST
    ind_industry_current = rvu_analytics.INDUSTRY_INDIVIDUAL_POST

    # Company status
    if co_avg >= co_breakeven_current:
        insights.append({'type': 'success',
                         'text': f"Company-wide production is on target at {co_avg:.1f} avg RVUs/week (breakeven: {co_breakeven_current})."})
    else:
        gap = co_breakeven_current - co_avg
        insights.append({'type': 'warning',
                         'text': f"Company-wide average of {co_avg:.1f} RVUs/week is {gap:.1f} below the {co_breakeven_current} breakeven target."})

    prov_avgs = {}
    for p in PROVIDERS:
        arr = prov_weekly[p].values
        n = len(arr)
        display = p.title()
        avg = float(arr.mean())
        prov_avgs[display] = avg

        if avg < ind_breakeven_current:
            gap = ind_breakeven_current - avg
            insights.append({'type': 'alert',
                             'text': f"{display} is averaging {avg:.1f} RVUs/week — {gap:.1f} below the individual breakeven ({ind_breakeven_current})."})
        elif avg >= ind_industry_current:
            insights.append({'type': 'success',
                             'text': f"{display} is at or above the industry standard ({ind_industry_current} RVUs/week), averaging {avg:.1f}."})

        if n >= 8:
            recent = float(arr[-4:].mean())
            prior = float(arr[-8:-4].mean())
            if prior > 0:
                pct = (recent - prior) / prior * 100
                if pct <= -20:
                    insights.append({'type': 'alert',
                                     'text': f"{display}'s production has declined {abs(pct):.0f}% over the last 4 weeks vs the prior 4 weeks."})
                elif pct >= 20:
                    insights.append({'type': 'success',
                                     'text': f"{display} is trending up {pct:.0f}% over the last 4 weeks vs the prior 4 weeks."})

        if n > 2:
            std = float(arr.std(ddof=1))
            cv = std / avg if avg > 0 else 0
            if cv > 0.6:
                insights.append({'type': 'info',
                                 'text': f"{display} shows high week-to-week variability (std dev: {std:.1f} RVUs). Consistent scheduling may help."})

    if prov_avgs:
        top = max(prov_avgs, key=prov_avgs.get)
        bot = min(prov_avgs, key=prov_avgs.get)
        if top != bot:
            insights.append({'type': 'info',
                             'text': f"Top performer: {top} ({prov_avgs[top]:.1f} avg RVUs/week). Lowest: {bot} ({prov_avgs[bot]:.1f} avg RVUs/week)."})

    # Detect holiday/low-production weeks
    threshold = co_avg * 0.5
    low_weeks = company[company < threshold]
    if len(low_weeks) > 0:
        dates = []
        for w in low_weeks.index[:3]:
            dt = pd.Timestamp(w)
            dates.append(f"{dt.strftime('%b')} {dt.day}")
        extra = f" (+{len(low_weeks) - 3} more)" if len(low_weeks) > 3 else ""
        insights.append({'type': 'info',
                         'text': f"Low-production weeks detected (likely holidays/closures): {', '.join(dates)}{extra}."})

    return insights


def get_all_analytics():
    cutoff = (datetime.now() - timedelta(days=7)).strftime('%b %d, %Y')
    return {
        'data_cutoff': cutoff,
        'scorecards': get_scorecards(),
        'trends': get_trends(),
        'breakeven': get_breakeven_analysis(),
        'monthly': get_monthly(),
        'recommendations': get_recommendations(),
    }
