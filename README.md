# Bi-Dashboard Analysis

This project visualizes business intelligence metrics including Revenue, Paid Orders, Trial Conversions, and Cohort Retention.

## Project Structure

### Data Analysis (Python)
The backend logic queries a remote MySQL database via SSH tunnel.

- **`config.py`**: Shared configuration for Remote DB credentials, SSH settings, and the list of `VALID_PRODUCTS`.
- **`run_all_analysis.py`**: Master Entry Point. Run this script to execute all analytical queries and output a unified JSON object.
- **Modular Query Scripts**:
  - `query_revenue.py`: Calculates Monthly Revenue and Valid Paid Orders.
  - `query_first_period.py`: Identifies distinct first-time purchases.
  - `query_monthly_trials.py`: Counts monthly trial orders.
  - `query_cohort_trials.py`: Performs detailed cohort analysis (conversion rates).
  - `query_lag_analysis.py`: detailed registration-to-trial lag distribution.

### Frontend Dashboard (Vue/Vite)
Located in `bi-dashboard/`.

- **`src/main.js`**: Contains the core logic. It uses the JSON output from `run_all_analysis.py` to render charts using Chart.js.
- **`index.html`**: The main layout file defining the structure of charts and tables.

## How to Run

1. **Install Dependencies**:
   ```bash
   pip install pymysql sshtunnel
   ```

2. **Run Analysis**:
   ```bash
   python3 run_all_analysis.py
   ```
   *Output*: A JSON object printed to stdout (and/or saved to file).

3. **Update Dashboard**:
   - Copy the JSON output.
   - Update the data arrays in `bi-dashboard/src/main.js`.
   - Run `npm run dev` to serve locally or `npm run build` to deploy.
