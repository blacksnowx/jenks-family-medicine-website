import pandas as pd

try:
    from . import rvu_analytics
except ImportError:
    import rvu_analytics

# ---------------------------------------------------------------------------
# Bonus configuration
# ---------------------------------------------------------------------------
VALID_PROVIDERS = ['ANNE JENKS', 'EHRIN IRVIN', 'HEATHER MAYO', 'SARAH SUGGS']

BREAKEVEN_RVUS_PER_WEEK = 49    # individual weekly breakeven (matches chart reference line)
BONUS_RATE_PER_RVU = 10.0       # $ bonus earned per RVU above quarterly threshold
ANNE_JENKS_ADJUSTMENT = 216     # manual RVU credit added to Anne Jenks


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _apply_anne_jenks_adjustment(df):
    """Spread 216 extra RVUs evenly across every week present in df for Anne Jenks."""
    unique_weeks = df['Week'].unique()
    if len(unique_weeks) == 0:
        return df
    extra_per_week = ANNE_JENKS_ADJUSTMENT / len(unique_weeks)
    dummy_rows = pd.DataFrame({
        'Date Of Service': unique_weeks,
        'Week': unique_weeks,
        'Provider': 'ANNE JENKS',
        'RVU': extra_per_week,
        'Category': 'Manual Adjustment',
    })
    return pd.concat([df, dummy_rows], ignore_index=True)


def _quarter_date_range(year, quarter):
    """Return (start_date, end_date) Timestamps for the given year/quarter."""
    starts = {1: '01-01', 2: '04-01', 3: '07-01', 4: '10-01'}
    ends   = {1: '03-31', 2: '06-30', 3: '09-30', 4: '12-31'}
    return (
        pd.Timestamp(f'{year}-{starts[quarter]}'),
        pd.Timestamp(f'{year}-{ends[quarter]}'),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_available_quarters(df=None):
    """Return quarter labels (e.g. '2026Q1') present in the dataset, newest first."""
    if df is None:
        df = rvu_analytics.get_rvu_dataset()
    if df is None or df.empty:
        return []
    df = df.copy()
    df['Quarter'] = df['Date Of Service'].dt.to_period('Q')
    quarters = sorted(df['Quarter'].dropna().unique(), reverse=True)
    return [str(q) for q in quarters]


def get_quarterly_bonus_report(year, quarter):
    """Calculate per-provider RVU totals and bonus earned for a historical quarter.

    Returns::

        {
            'quarter_label':     'Q1 2026',
            'available_quarters': ['2026Q1', '2025Q4', ...],
            'providers': [
                {
                    'name':           'Anne Jenks',
                    'total_rvus':     850.0,
                    'threshold_rvus': 637,
                    'bonus_earned':   2130.00,
                },
                ...
            ],
        }

    Bonus formula:
        threshold_rvus = (distinct weeks in quarter) * BREAKEVEN_RVUS_PER_WEEK
        bonus_earned   = max(0, (total_rvus - threshold_rvus) * BONUS_RATE_PER_RVU)
    """
    quarter_label = f'Q{quarter} {year}'
    df = rvu_analytics.get_rvu_dataset()
    available = get_available_quarters(df)

    if df is None or df.empty:
        return {'quarter_label': quarter_label, 'providers': [], 'available_quarters': available}

    # Apply Anne Jenks manual adjustment (spread over ALL weeks in dataset)
    df = _apply_anne_jenks_adjustment(df)

    # Restrict to recognised providers
    df = df[df['Provider'].isin(VALID_PROVIDERS)]

    # Slice to the requested quarter
    start, end = _quarter_date_range(year, quarter)
    quarter_df = df[(df['Date Of Service'] >= start) & (df['Date Of Service'] <= end)].copy()

    # Count weeks that actually appear in that quarter's data for threshold calculation
    num_weeks = int(quarter_df['Week'].nunique()) if not quarter_df.empty else 0
    threshold_rvus = num_weeks * BREAKEVEN_RVUS_PER_WEEK

    # Aggregate RVUs per provider
    if not quarter_df.empty:
        provider_rvus = quarter_df.groupby('Provider')['RVU'].sum()
    else:
        provider_rvus = pd.Series(dtype=float)

    result_providers = []
    for p in VALID_PROVIDERS:
        total_rvus = float(provider_rvus.get(p, 0.0))
        bonus = max(0.0, (total_rvus - threshold_rvus) * BONUS_RATE_PER_RVU)
        result_providers.append({
            'name': p.title(),
            'total_rvus': round(total_rvus, 1),
            'threshold_rvus': threshold_rvus,
            'bonus_earned': round(bonus, 2),
        })

    return {
        'quarter_label': quarter_label,
        'providers': result_providers,
        'available_quarters': available,
    }
