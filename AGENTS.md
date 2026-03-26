# Jenks Family Medicine Website — AI Agent Context

This document provides critical architectural, environmental, and infrastructure context for any AI agents (like Antigravity) working on this codebase. **Read this fully before making architectural changes.**

## 1. Core Infrastructure & Deployment
*   **Framework System:** Python 3 standard Flask web application.
*   **Hosting:** Deployed to Heroku (`jenks-family-medicine-site`).
*   **Ephemeral Filesystem (CRITICAL):** Heroku utilizes an ephemeral filesystem. Any files written dynamically to disk (like uploaded CSVs or generated images) will be permanently deleted whenever the dyno restarts (every 24 hours or on deployment). **Do not build features that rely on persistent local file storage.**
*   **Database:** PostgreSQL is used in production (Heroku Postgres). Locally, the app falls back to SQLite (`sqlite:///instance/site.db`) if `DATABASE_URL` is omitted. Models are defined via Flask-SQLAlchemy in `models.py`.

## 2. Authentication & Roles (RBAC)
*   **Roles:** There are three strict user tiers: `Owner`, `Admin`, and `Provider`.
    *   `Owner`: Full system access, including the ability to upload raw backend analytical data.
    *   `Admin`: Can modify external website elements (like the Homepage Banner) but lacks backend data control.
    *   `Provider`: Strictly read-only access (currently sees an empty or highly-restricted dashboard).
*   **Password Policy:** 10–18 characters, allowing alphanumeric, space, and special characters. Users are forced to reset their passwords globally every 30 days. This logic lives in the `User` model and `admin_dashboard` routes.

## 3. Data Analytics & Reporting Engine
A massive consolidation refactor took place to streamline legacy scripts. All analytics logic is contained within the `data/` directory.

### Secure File Loading
*   **Reference CSVs:** The application fundamentally relies on three files: `201 Bills and Payments.csv`, `Charges Export.csv`, and `bank.csv`.
*   **Git Security:** These CSVs are strictly ignored in `.gitignore` to prevent leaking PHI/sensitive data into GitHub.
*   **Postgres Storage Bypass:** To counter Heroku's ephemeral filesystem, `Owner`s upload these CSVs directly through the admin dashboard where they are saved as `LargeBinary` blobs into the `ReferenceData` SQL table.
*   **`data_loader.py`:** **All** data modules must import `data_loader.py` to get dataframes. This loader automatically attempts to fetch the CSV directly from the PostgreSQL database in production, seamlessly falling back to local disk files for local development.

### Analytical Modules
Do NOT create disjointed, one-off analytic scripts. Integrate new logic into the existing consolidated domain modules inside `data/`:
1.  `veterans_analytics.py`: Analyzes VA volume and applies fee-schedule based modeling.
2.  `primary_care_analytics.py`: Linear revenue drivers, panel sizes, and new patient cohort metrics.
3.  `revenue_forecasting.py`: Incorporates `statsmodels` Holt-Winters exponential smoothing and VA Accounts Receivable (AR) delay pipeline trackers.
4.  `provider_analytics.py`: Provider-specific economics and strict face-to-face encounter tracking logic.
5.  `growth_analytics.py`: Monte-Carlo downside risk simulations and growth rate tracking.

All generated chart images (`matplotlib`) MUST be explicitly saved into the `data/images/` directory.

## 4. Testing
*   A robust mock-data testing suite (`pytest`) is located in `data/tests/`.
*   When altering data processing or forecasting logic, ensure unit tests covering those modules are updated and executed successfully.
