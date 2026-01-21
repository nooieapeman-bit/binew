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

### Cleaning and Migrating `fact_order` (Optimized)

The following optimized SQL groups orders by `order_id`, taking the latest record (MAX id) and including the `subscription_id` link:

```sql
INSERT INTO fact_order (
    order_id, subscription_id, app_id, region_id, uid, uuid, product_name, amount, cny_amount,
    order_status, pay_time, order_submit_time, model_code,
    product_cycle_unit, product_cycle_time
)
SELECT 
    o.order_id, o.subscribe_id, 1 as app_id, 1 as region_id, o.uid, o.uuid, 
    COALESCE(sm.name, '') as product_name,
    CAST(o.amount AS DECIMAL(10,2)) as amount,
    CAST(COALESCE(oai.amount_cny, 0) - COALESCE(oai.transaction_fee_cny, 0) AS DECIMAL(10,2)) as cny_amount,
    o.status as order_status,
    FROM_UNIXTIME(o.pay_time) as pay_time,
    FROM_UNIXTIME(o.submit_time) as order_submit_time,
    COALESCE(dev.model_code, '') as model_code,
    COALESCE(sm.time_unit, '') as product_cycle_unit,
    COALESCE(sm.time, 0) as product_cycle_time
FROM (
    SELECT MAX(id) as latest_id
    FROM `order`
    WHERE order_id IS NOT NULL AND order_id != ''
    GROUP BY order_id
) t
JOIN `order` o ON t.latest_id = o.id
LEFT JOIN set_meal sm ON o.product_id = sm.code
LEFT JOIN device dev ON o.uuid = dev.uuid
LEFT JOIN order_amount_info oai ON o.id = oai.order_int_id;
```

### Updating `dim_subscription.first_paid_time`

Once `fact_order` is populated with `subscription_id`, we update the dimension table to include the earliest actual payment time:

```sql
UPDATE dim_subscription ds
JOIN (
    SELECT subscription_id, MIN(pay_time) as first_time
    FROM fact_order
    WHERE amount > 0 AND order_status = 1 AND subscription_id != ''
    GROUP BY subscription_id
) t ON ds.subscription_id = t.subscription_id
SET ds.first_paid_time = t.first_time;
```

### Last Synchronized ID (Incremental Tracking)

To facilitate incremental updates, the following ID represents the last record processed from the remote `order` table:

- **Last Processed Order ID**: `689061` (Synchronized at 2026-01-08)
- **Last Processed Cloud Info ID**: `579871` (Synchronized at 2026-01-08)


### Cleaning and Migrating `dim_user`

The following SQL logic illustrates the migration and cleanup of user data into the `dim_user` dimension table:

```sql
INSERT INTO dim_user (app_id, region_id, uid, register_time, country_id)
SELECT 
    1 as app_id,           -- Hardcoded for EU region in script
    1 as region_id,        -- Hardcoded for EU region in script
    uid,
    FROM_UNIXTIME(register_time) as register_time,
    CAST(COALESCE(NULLIF(register_country, ''), '0') AS UNSIGNED) as country_id
FROM `user`
ON DUPLICATE KEY UPDATE 
    register_time = VALUES(register_time), 
    country_id = VALUES(country_id);
```

### Cleaning and Migrating `dim_device`

```sql
INSERT INTO dim_device (app_id, region_id, uuid, model_code, create_time)
SELECT 
    1 as app_id,           -- Hardcoded for EU region in script
    1 as region_id,        -- Hardcoded for EU region in script
    uuid,
    COALESCE(model_code, '') as model_code,
    FROM_UNIXTIME(NULLIF(create_time, 0)) as create_time
FROM `device`
ON DUPLICATE KEY UPDATE 
    model_code = VALUES(model_code), 
    create_time = VALUES(create_time);
```

### Cleaning and Migrating `dim_user_device`

```sql
INSERT INTO dim_user_device (
    uid, uuid, app_id, region_id, model_code, device_type, bind_type, status, 
    first_bind_time, last_bind_time, delete_time
)
SELECT 
    uid, 
    uuid, 
    1 as app_id, 
    1 as region_id, 
    COALESCE(model_code, '') as model_code, 
    COALESCE(device_type, 0) as device_type, 
    COALESCE(bind_type, 1) as bind_type, 
    COALESCE(status, 1) as status, 
    FROM_UNIXTIME(NULLIF(first_time, 0)) as first_bind_time, 
    FROM_UNIXTIME(NULLIF(bind_time, 0)) as last_bind_time, 
    FROM_UNIXTIME(NULLIF(delete_time, 0)) as delete_time
FROM `user_device`;
```

### Cleaning and Migrating `dim_subscription` (Optimized)

The following optimized SQL groups subscriptions by `subscribe_id`, taking the earliest record for initial fields and the latest record for current status/identity (including `status`):

```sql
INSERT INTO dim_subscription (
    app_id, region_id, subscription_id, uid, product_id, status,
    initial_time, cancel_time, next_billing_at, last_upgrade_time, first_paid_time
)
SELECT 
    1 as app_id, 1 as region_id,
    t.subscribe_id,
    s_last.uid,
    s_last.product_id,
    s_last.status,
    FROM_UNIXTIME(NULLIF(s_first.initial_payment_time, 0)) as initial_time,
    FROM_UNIXTIME(NULLIF(s_last.cancel_time, 0)) as cancel_time,
    FROM_UNIXTIME(NULLIF(s_last.next_billing_at, 0)) as next_billing_at,
    FROM_UNIXTIME(NULLIF(s_last.initial_payment_time, 0)) as last_upgrade_time,
    NULL as first_paid_time
FROM (
    -- Identify the first and last record ID for each subscription (Preserves data lineage)
    SELECT subscribe_id, MIN(id) as first_id, MAX(id) as last_id
    FROM `subscribe`
    WHERE subscribe_id != ''
    GROUP BY subscribe_id
) t
JOIN `subscribe` s_first ON t.first_id = s_first.id
JOIN `subscribe` s_last ON t.last_id = s_last.id;
```
### Cleaning and Migrating `fact_cloud` (Two-Phase approach)

#### Phase 1: Direct Mapping from `cloud_info`
Synchronizes core service cycle records from the remote database.

```sql
INSERT INTO fact_cloud (
    app_id, region_id, uid, uuid, order_id, 
    cycle_start_time, cycle_end_time, cycle_is_delete
)
SELECT 
    1 as app_id, 1 as region_id,
    uid, uuid, COALESCE(order_id, '') as order_id,
    FROM_UNIXTIME(NULLIF(start_time, 0)), 
    FROM_UNIXTIME(NULLIF(end_time, 0)),
    is_delete
FROM `cloud_info`;
```

#### Phase 2: Local Enrichment and Business Logic

**Step A: Dimension Enrichment**
Populates financial data, subscription details, and calculates initial business metrics.

```sql
UPDATE fact_cloud fc
JOIN fact_order fo ON fc.order_id = fo.order_id
LEFT JOIN dim_subscription ds ON fo.subscription_id = ds.subscription_id
SET 
    -- Dimensions
    fc.product_name = COALESCE(fo.product_name, ''),
    fc.amount = COALESCE(fo.amount, 0.00),
    fc.cny_amount = COALESCE(fo.cny_amount, 0.00),
    fc.order_status = COALESCE(fo.order_status, 0),
    fc.pay_time = fo.pay_time,
    fc.subscription_id = COALESCE(fo.subscription_id, ''),
    fc.subscription_status = COALESCE(ds.status, 0),
    fc.subscription_initial_time = ds.initial_time,
    fc.subscription_first_paid_time = ds.first_paid_time,
    fc.model_code = COALESCE(fo.model_code, ''),
    fc.plan_cycle_unit = COALESCE(fo.product_cycle_unit, ''),
    fc.plan_cycle_time = COALESCE(fo.product_cycle_time, 1),
    
    -- Business Logic
    fc.is_sixteen_plan = IF(fo.product_name IN (...), 1, 0),
    fc.plan_type = IF(fo.product_cycle_unit = 'YEAR', 2, 1),
    fc.has_trial = IF(ds.initial_time IS NOT NULL AND ds.first_paid_time IS NOT NULL,
                     IF(ABS(TIMESTAMPDIFF(MINUTE, ds.initial_time, ds.first_paid_time)) <= 10, 0, 1), 1),
    fc.cycle_counts = COALESCE(ROUND(DATEDIFF(fc.cycle_end_time, ds.first_paid_time) / 30), 1);
```

**Step B: Lifecycle Status (`after_status`)**
Identifies the outcome of each subscription cycle (Active, Renewed, Trial Converted, Close).

1.  **Grouping**: Data is grouped by `uid`, `uuid`, and `subscription_id` and ordered by `cycle_end_time DESC` (Latest first).
2.  **Status 1 (Active/Pending)**: The latest record (row 1) where `cycle_end_time > '2025-12-31 23:59:59'`.
3.  **Status 4 (Close)**: The latest record (row 1) where `cycle_end_time <= '2025-12-31 23:59:59'`.
4.  **Status 3 (Trial Converted)**: Historical records (row 2..N) where `amount = 0`.
5.  **Status 2 (Renewed)**: Historical records (row 2..N) where `amount > 0`.

```sql
-- 1. Reset all status
UPDATE fact_cloud SET after_status = 0;

-- 2. Performance-optimized update using Window Functions (MySQL 8.0+)
UPDATE fact_cloud f
JOIN (
    SELECT 
        id,
        ROW_NUMBER() OVER (
            PARTITION BY uid, uuid, subscription_id 
            ORDER BY cycle_end_time DESC
        ) as row_idx,
        cycle_end_time,
        amount
    FROM fact_cloud
    WHERE cycle_end_time >= '2024-12-01 00:00:00'
) t ON f.id = t.id
SET f.after_status = CASE 
    -- Latest record in the sequence
    WHEN t.row_idx = 1 THEN 
        IF(t.cycle_end_time > '2025-12-31 23:59:59', 1, 4)
    -- Historical records
    ELSE 
        IF(t.amount = 0, 3, 2)
END;
```


### Cleansing `user.first_bind_time`

To ensure the accuracy of the user's first device-binding timestamp, we execute an enriched update on the `dim_user` table.

This process:
1.  **Extracts Bind Times**: Gathers all binding records from `dim_user_device`.
2.  **Identifies Earliest Time**: For each user (`uid`), it finds the absolute earliest binding moment by prioritizing `first_bind_time` but falling back to `last_bind_time` if necessary.
3.  **Updates Dimension**: Synchronizes this calculated minimum time back to `dim_user.first_bind_time`.

```sql
-- Ensure optimization index exists
CREATE INDEX idx_uid ON dim_user(uid);

-- Multi-table update with aggregation
UPDATE dim_user u
JOIN (
    SELECT uid, MIN(COALESCE(first_bind_time, last_bind_time)) as min_bind_time
    FROM dim_user_device
    WHERE first_bind_time IS NOT NULL OR last_bind_time IS NOT NULL
    GROUP BY uid
) d ON u.uid = d.uid
SET u.first_bind_time = d.min_bind_time;
```

### Cleansing `user.first_trial_time`

To synchronize the earliest trial start time for users, we update the `dim_user` table using data from the `fact_cloud` table with specific business filters.

This process:
1.  **Filters Trial Records**: Selects records from `fact_cloud` where `amount = 0`, `product_name` is in the valid products list, and `pay_time` is after **2024-10-01**.
2.  **Resets Fields**: First sets all `first_trial_time` to `NULL` to ensure a clean re-run.
3.  **Identifies Earliest Trial**: Finds the minimum `pay_time` for each user matching the criteria.
4.  **Updates Dimension**: Updates `dim_user.first_trial_time` with the earliest valid trial timestamp.

```sql
-- Ensure optimization index exists for faster joining
CREATE INDEX idx_uid_amount ON fact_cloud(uid, amount);

-- 1. Reset
UPDATE dim_user SET first_trial_time = NULL;

-- 2. Update with collation handling and business filters
UPDATE dim_user u
JOIN (
    SELECT uid, MIN(pay_time) as min_trial_time
    FROM fact_cloud
    WHERE amount = 0 
      AND pay_time >= '2024-10-01'
      AND product_name IN ('14-day history...', '...') -- VALID_PRODUCTS list
    GROUP BY uid
) t ON u.uid COLLATE utf8mb4_unicode_ci = t.uid
SET u.first_trial_time = t.min_trial_time;
```
