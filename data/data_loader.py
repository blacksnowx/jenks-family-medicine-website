import pandas as pd
import os
import io

# ---------------------------------------------------------------------------
#  Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VA_CSV = os.path.join(BASE_DIR, '201 Bills and Payments.csv')
CHARGES_CSV = os.path.join(BASE_DIR, 'Charges Export.csv')
BANK_CSV = os.path.join(BASE_DIR, 'bank.csv')

# ---------------------------------------------------------------------------
#  Provider Registry — single source of truth for name normalization
# ---------------------------------------------------------------------------
VALID_PROVIDERS = ['ANNE JENKS', 'EHRIN IRVIN', 'HEATHER MAYO', 'SARAH SUGGS']

# Maps raw CSV values → canonical uppercase names
PROVIDER_ALIASES = {
    'Jenks, Anne ':          'ANNE JENKS',
    'REDD, DAVID ':          'ANNE JENKS',
    'Anne Jenks, APRN':      'ANNE JENKS',
    'DAVID REDD, MD':        'ANNE JENKS',
    'Anne Renee Jenks, APRN': 'ANNE JENKS',  # Tebra RenderingProviderName format
    'IRVIN, EHRIN ':         'EHRIN IRVIN',
    'Irvin, Ehrin':          'EHRIN IRVIN',
    'EHRIN IRVIN, FNP':      'EHRIN IRVIN',
    'EHRIN M. IRVIN, FNP':   'EHRIN IRVIN',  # Tebra RenderingProviderName format
    'Ehrin Irvin, FNP-C':    'EHRIN IRVIN',
    'Ehrin Irvin, APRN':     'EHRIN IRVIN',
    'Ehrin Irvin, NP':       'EHRIN IRVIN',
    'Ehrin Irvin, ARNP':     'EHRIN IRVIN',
    'Ehrin Irvin':           'EHRIN IRVIN',
    'SUGGS, SARAH ':         'SARAH SUGGS',
    'SARAH SUGGS, NP':       'SARAH SUGGS',
    'SARAH F SUGGS, NP':     'SARAH SUGGS',  # Tebra RenderingProviderName format
    'Sarah Suggs':           'SARAH SUGGS',
    'Sarah Suggs ':          'SARAH SUGGS',
    'sArah Suggs':           'SARAH SUGGS',
    'SArah Suggs':           'SARAH SUGGS',
    'Heather Mayo':          'HEATHER MAYO',
    '0':                     'ANNE JENKS',  # legacy VA data quirk
}

# VA fee schedule
VA_FEES = {
    'GenMed':  {'0': 0, '1-5': 180, '6-10': 360, '11-15': 520, '16+': 700},
    'Focused': {'0': 0, '1-5': 200, '6-10': 280, '11-15': 370, '16+': 480},
    'TBI':     250,
    'IMO':     {'0': 0, '1-3': 150, '4-6': 260, '7-9': 330, '10-12': 350,
                '13-15': 465, '16-18': 525, '19+': 600},
}

# ---------------------------------------------------------------------------
#  Database helpers
# ---------------------------------------------------------------------------

def get_database_url():
    """Return the SQLAlchemy-compatible database URL."""
    if "DATABASE_URL" not in os.environ:
        project_root = os.path.dirname(BASE_DIR)
        db_path = os.path.join(project_root, 'instance', 'site.db')
        return f"sqlite:///{db_path}"
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def get_csv_from_db(filename):
    """Attempt to load a CSV from the ReferenceData DB table."""
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(get_database_url())
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT data FROM reference_data WHERE filename = :f"),
                {"f": filename},
            ).fetchone()
            if result and result[0]:
                return io.BytesIO(result[0])
    except Exception:
        pass
    return None


def normalize_provider(name):
    """Normalize a provider name to its canonical uppercase form."""
    if isinstance(name, pd.Series):
        name = name.iloc[0] if not name.empty else None
    if pd.isna(name):
        return name
    raw = str(name).strip()
    if raw in PROVIDER_ALIASES:
        return PROVIDER_ALIASES[raw]
    upper = raw.upper().strip()
    if upper in VALID_PROVIDERS:
        return upper
    # Check partial matches for common variations
    for alias, canonical in PROVIDER_ALIASES.items():
        if alias.upper().strip() == upper:
            return canonical
    return upper

def get_week_ending_friday(date_series):
    """Calculate the Friday that ends the week for each date (matching historical code)."""
    dow = date_series.dt.dayofweek  # Monday=0, Sunday=6, Friday=4
    days_to_friday = (4 - dow) % 7
    days_to_friday = days_to_friday.where(dow <= 4, (4 - dow + 7))
    return (date_series + pd.to_timedelta(days_to_friday, unit='D')).dt.normalize()

def clean_currency(x):
    """Clean currency strings like '$1,234.56' to float"""
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x).replace('$', '').replace(',', '').strip())
    except:
        return 0.0

def _match_fee(value_str, schedule):
    """Look up a value in a ranged fee schedule dict. Returns the matched fee."""
    if isinstance(schedule, (int, float)):
        # Flat fee (e.g. TBI) — return it if value indicates presence
        return schedule
    for key, val in schedule.items():
        if value_str == key:
            return val
    return 0


def calculate_va_fee(row):
    """Calculate total fee for a VA DBQ encounter based on the VA fee schedule."""
    total = 0.0

    focused = str(row.get('Focused DBQs', '0')).strip()
    if focused.lower() in ['lab', 'visual', 'visual screen', 'bp check', '']:
        focused = '0'
    total += _match_fee(focused, VA_FEES['Focused'])

    imo = str(row.get('Routine IMOs', '0')).strip()
    if imo in ['', '0', 'nan', 'NaN']:
        imo = '0'
    total += _match_fee(imo, VA_FEES['IMO'])

    tbi = str(row.get('TBI', '0')).strip().lower()
    if tbi in ['1', 'r-tbi', 'tbi']:
        total += VA_FEES['TBI']

    genmed = str(row.get('Gen Med DBQs', '0')).strip()
    if genmed.lower() in ['bp check', '']:
        genmed = '0'
    total += _match_fee(genmed, VA_FEES['GenMed'])

    no_show = str(row.get('No Show', '0')).strip()
    if no_show == '1':
        total += 60.0

    return total

def load_va_data(csv_path=VA_CSV):
    """Load and process VA data (201 Bills and Payments.csv)."""
    db_file = get_csv_from_db('201 Bills and Payments.csv')
    try:
        if db_file:
            df = pd.read_csv(db_file)
        else:
            df = pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()
    # Remove duplicate column names — same fix as load_pc_data
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    # Normalize provider column: the original CSV uses 'Sarah Suggs ' as header,
    # while sheets_sync uses 'Provider'.  Handle both, plus the case where a
    # bad merge left us with BOTH columns (one has NaN for some rows).
    provider_col = None
    for candidate in ['Sarah Suggs ', 'Sarah Suggs']:
        if candidate in df.columns:
            provider_col = candidate
            break

    if provider_col and 'Provider' in df.columns:
        # Both exist — fill NaNs in one from the other, then drop the extra
        df['Provider'] = df['Provider'].fillna(df[provider_col])
        df = df.drop(columns=[provider_col])
    elif provider_col:
        df = df.rename(columns={provider_col: 'Provider'})
    # else: 'Provider' already exists or neither exists — leave as-is

    # Final dedup safety net
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    # Date processing
    df['Date of Service'] = pd.to_datetime(df['Date of Service'], errors='coerce')
    df = df.dropna(subset=['Date of Service']).copy()
    
    # Calculate VA Revenue according to fee schedule
    df['VA_Revenue'] = df.apply(calculate_va_fee, axis=1)
    
    # Calculate standard Week column (ending Friday)
    df['Week'] = get_week_ending_friday(df['Date of Service'])
    return df

def load_pc_data(csv_path=CHARGES_CSV):
    """Load and process Primary Care data (Charges Export.csv)."""
    db_file = get_csv_from_db('Charges Export.csv')
    try:
        if db_file:
            df = pd.read_csv(db_file)
        else:
            df = pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()
    # Remove duplicate column names — the merged CSV may have duplicates if the
    # original manually-uploaded CSV had repeated column headers.  Keeping only
    # the first occurrence is safe: load_pc_data only uses the canonical column
    # names, so a duplicate would just carry redundant (or misaligned) data.
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    df['Date Of Service'] = pd.to_datetime(df['Date Of Service'], errors='coerce')
    
    # Clean currency columns
    currency_cols = [
        'Service Charge Amount', 'Pri Ins Insurance Contract Adjustment', 
        'Sec Ins Insurance Contract Adjustment', 'Other Ins Insurance Contract Adjustment',
        'Pri Ins Insurance Payment', 'Sec Ins Insurance Payment', 
        'Other Ins Insurance Payment', 'Pat Payment Amount', 'Other Adjustment'
    ]
    for col in currency_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_currency)
            
    # Standardize missing columns
    for col in currency_cols:
        if col not in df.columns:
            df[col] = 0.0
            
    df['Total Adjustments'] = df['Pri Ins Insurance Contract Adjustment'] + df['Sec Ins Insurance Contract Adjustment'] + df['Other Ins Insurance Contract Adjustment']
    df['Total Payments'] = df['Pat Payment Amount'] + df['Pri Ins Insurance Payment'] + df['Sec Ins Insurance Payment'] + df['Other Ins Insurance Payment']
    
    def calculate_allowed(row):
        chg = row['Service Charge Amount']
        adj = row['Total Adjustments']
        pay = row['Total Payments']
        if isinstance(chg, pd.Series): chg = chg.iloc[0] if not chg.empty else 0.0
        if isinstance(adj, pd.Series): adj = adj.iloc[0] if not adj.empty else 0.0
        if isinstance(pay, pd.Series): pay = pay.iloc[0] if not pay.empty else 0.0
        if adj == 0 and pay == 0 and chg > 0:
            return chg * 0.3287
        return chg - adj
        
    df['PC_Allowed'] = df.apply(calculate_allowed, axis=1)

            
    # Calculate proxy PC_Revenue (90% of allowed, mapped from historical trend used in pc_revenue_drivers)
    # or actual collections. Several scripts use PC_Allowed * 0.90 for standard modeling.
    df['PC_Revenue'] = df['Total Payments'].fillna(0.0)
    df['PC_Revenue_Model'] = df['PC_Allowed'] * 0.90
    df['Week'] = get_week_ending_friday(df['Date Of Service'])
    return df

def load_bank_data(csv_path=BANK_CSV):
    """Load and process bank balance data (bank.csv)."""
    db_file = get_csv_from_db('bank.csv')
    try:
        if db_file:
            df = pd.read_csv(db_file)
        else:
            if not os.path.exists(csv_path):
                return pd.DataFrame()
            df = pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    if 'Running Bal.' in df.columns:
        df['Running Bal.'] = df['Running Bal.'].apply(clean_currency)
    if 'Amount' in df.columns:
        df['Amount'] = df['Amount'].apply(clean_currency)
    return df
