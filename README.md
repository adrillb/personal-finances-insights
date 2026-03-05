# Personal Finance Insights Dashboard

Interactive Streamlit dashboard to analyze personal finance data from an Excel workbook.

This project ingests a workbook with the same structure as `data/Mock Personal finances.xlsx`, normalizes the key sheets, computes finance metrics, and presents them in a filterable dashboard.

## What You Get

- Transaction-level analytics from the `RAW` sheet
- Monthly summary analytics from `GENERAL`, `EXPENSES`, and `INCOME`
- Interactive filters (month range, year, categories)
- Visual insights for:
  - Income vs expenses
  - Spending breakdown and trends
  - Budget vs actual
  - Savings and investments
  - Transaction search/exploration

## Project Layout

```text
Personal Finances/
├── app.py
├── requirements.txt
├── README.md
├── data/
│   └── Mock Personal finances.xlsx
└── src/
    ├── __init__.py
    ├── data_loader.py
    └── insights.py
```

## Tech Stack

- Python 3.12+
- `streamlit` for UI
- `pandas` for data wrangling
- `plotly` for charts
- `openpyxl` for Excel parsing

## Quick Start

### 1) Create and activate a virtual environment

If `.venv` already exists, you can reuse it.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

### 3) Place your workbook

Default expected path:

```text
data/Mock Personal finances.xlsx
```

You can either:
- Keep this filename, or
- Replace the file contents with your own workbook preserving the same structure.

### 4) Run the dashboard

```bash
streamlit run app.py
```

Then open the local URL shown by Streamlit (typically `http://localhost:8501`).

## Dashboard Sections

`app.py` renders 6 tabs:

1. **Overview**
   - KPI cards: total income, expenses, net cash flow, savings rate
   - Monthly income vs expenses line chart
   - Average daily spending and investments summary

2. **Spending Analysis**
   - Spending breakdown pie chart
   - Top categories bar chart
   - Monthly stacked category chart
   - Rolling trend chart
   - Daily spending heatmap

3. **Budget vs Actual**
   - Projected vs actual comparison by category
   - Delta (actual - projected) visualization
   - Detailed budget table

4. **Income**
   - Income by source pie chart
   - Monthly income trend
   - Income table

5. **Savings & Investments**
   - Monthly savings/investments bars
   - Cumulative curves for savings and investments
   - Category averages by year (from `EXPENSES`)

6. **Transaction Explorer**
   - Searchable transaction table
   - Filtered by current sidebar filters

## Data Sources and Parsing Rules

The parser logic lives in `src/data_loader.py`.

### Workbook resolution

`resolve_workbook_path()` tries:
1. `data/Mock Personal finances.xlsx`
2. `Mock Personal finances.xlsx`

You can also pass a custom path into loader functions.

### Loaded DataFrames

`load_all_data()` returns a `WorkbookData` dataclass with:

- `raw_transactions`
- `general_summary`
- `expenses_by_category`
- `budget`
- `income_by_source`
- `category_averages`

### Sheet mapping details

#### `RAW` -> `load_raw_transactions()`

- Reads columns A:D from row 2 onward
- Keeps rows with valid date, category, and amount
- Drops placeholder/future rows with year >= 2100
- Normalizes:
  - `amount` = absolute value (positive spend)
  - `signed_amount` = original signed value
  - adds `year` and month-start `month`

Output columns:
- `date`, `category`, `amount`, `signed_amount`, `description`, `year`, `month`

#### `GENERAL` -> `load_general_summary()`

- Uses "All Info" section (header at row 20)
- Reads source rows 21-24: `INCOME`, `EXPENSES`, `INVESTMENTS`, `SAVINGS`
- Produces long format with month/source/amount

Output columns:
- `month`, `source`, `amount`, `status`

#### `EXPENSES` (historical totals) -> `load_expenses_by_category()`

- Uses month header row 48 (`jan2024` style)
- Uses category names from rows 22-34 (excluding `TOTAL`)
- Reads corresponding monthly category values from rows 50+

Output columns:
- `month`, `category`, `amount`

#### `EXPENSES` (current budget block) -> `load_budget()`

- Uses current section rows 4-17
- Reads:
  - `projected` (col C)
  - `actual` (col D)
  - `difference` (col G)
- Infers current month/year from row 3-4

Output columns:
- `month`, `category`, `projected`, `actual`, `difference`, `is_total`

#### `INCOME` -> `load_income_by_source()`

- Uses "All Info" section (header row 23)
- Reads rows 24-28 (`TOTAL` included and flagged)

Output columns:
- `month`, `source`, `amount`, `status`, `is_total`

#### `EXPENSES` averages -> `load_category_averages()`

- Uses row 38 for year headers (`2024`, `2025`, etc.)
- Reads categories from rows 40-44

Output columns:
- `category`, `year`, `average_amount`

## Metrics and Insight Functions

All calculations are in `src/insights.py`.

Main functions:

- `monthly_totals()`
- `compute_kpis()`
- `spending_breakdown()`
- `monthly_category_spending()`
- `top_categories()`
- `daily_spending()`
- `average_daily_spending()`
- `budget_adherence()`
- `rolling_spending()`
- `income_breakdown()`
- `monthly_income_trend()`
- `cumulative_savings_investments()`

Notes:
- Savings rate and budget percentage calculations safely handle division-by-zero cases.
- Income breakdown/trend excludes helper `TOTAL` rows where applicable.

## Assumptions and Constraints

- Workbook structure (sheet names and key row layouts) matches the provided mock file.
- Month labels are parseable (e.g., `jan2024`, `Feb 2024`, etc.).
- Expense amounts in `RAW` are negative and are displayed as positive values in the dashboard.
- This project currently focuses on one workbook at a time.

## Using Your Real File

To switch from mock to real data:

1. Back up your original file.
2. Replace `data/Mock Personal finances.xlsx` with your real workbook (same structure).
3. Restart Streamlit.

If your structure differs, update parsing offsets/ranges in `src/data_loader.py`.

## Google Sheets Cloud Setup

You can run the dashboard using a Google Sheet as the primary source and keep a local fallback.

### 1) Create Google Cloud credentials

1. Open [Google Cloud Console](https://console.cloud.google.com)
2. Create/select a project
3. Enable:
   - Google Sheets API
   - Google Drive API
4. Create a Service Account
5. Create and download a JSON key for that Service Account
6. Save it in the project root as:

```text
credentials.json
```

7. Share your Google Sheet with the Service Account email and grant **Editor** permission

### 2) Configure environment variables

Create a `.env` file in the project root (see `.env.example`):

```bash
GOOGLE_CREDENTIALS_PATH=credentials.json
SPREADSHEET_NAME=Personal finances
MONEFY_FOLDER=/mnt/c/Users/TU_USUARIO/OneDrive/Monefy
```

### 3) Run the app

```bash
streamlit run app.py
```

The app will try cloud loading first and fall back to local workbook loading if cloud config is missing or fails.

## Monefy CSV Sync (OneDrive Drop Folder)

This project supports a lightweight Monefy automation flow:

1. Export CSV from Monefy on mobile
2. Share the CSV to your OneDrive folder (`MONEFY_FOLDER`)
3. In the Streamlit sidebar, click **Sync Monefy**
4. The app will:
   - Detect unprocessed CSV files
   - Normalize their data
   - Append rows into the `MonefyCSV` sheet in your Google Sheet
   - Track processed files in `data/.monefy_processed.json` to prevent duplicates

Use **Refresh from Cloud** in the sidebar to clear cache and force a fresh cloud download.

## Common Troubleshooting

### Streamlit command not found

Run through the virtual environment:

```bash
.venv/bin/streamlit run app.py
```

### Import errors (`ModuleNotFoundError`)

Reinstall dependencies:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### Empty charts or missing metrics

Check:
- Workbook path exists and is readable
- Sheet names are unchanged
- Required sections still live in expected rows/columns

### Port already in use

Use another port:

```bash
streamlit run app.py --server.port 8503
```

## Extending the Project

Good next enhancements:

- Export filtered insights to CSV/PDF
- Add anomaly detection for unusual spending
- Add forecast models for expenses/cash flow
- Add account-level views from `MonefyCSV`
- Add unit tests around parsing and metrics

## Privacy Note

This project is intended for local analysis of personal financial data. Avoid committing sensitive real financial files to public repositories.

