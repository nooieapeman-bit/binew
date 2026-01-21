import pymysql
from config import VALID_PRODUCTS

LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def enrich_fact_cloud_step_a():
    conn = pymysql.connect(**LOCAL_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            print("Step A: Enriching fact_cloud with dimensions and business logic (with NULL handling)...")
            
            # Formatting product list for SQL
            product_list_sql = ", ".join([f"'{p}'" for p in VALID_PRODUCTS])
            
            sql = f"""
            UPDATE fact_cloud fc
            JOIN fact_order fo ON fc.order_id = fo.order_id
            LEFT JOIN dim_subscription ds ON fo.subscription_id = ds.subscription_id
            SET 
                -- 1. Base Dimensions from Order
                fc.product_name = COALESCE(fo.product_name, ''),
                fc.amount = COALESCE(fo.amount, 0.00),
                fc.cny_amount = COALESCE(fo.cny_amount, 0.00),
                fc.order_status = COALESCE(fo.order_status, 0),
                fc.pay_time = fo.pay_time,
                fc.order_submit_time = fo.order_submit_time,
                fc.model_code = COALESCE(fo.model_code, ''),
                fc.plan_cycle_unit = COALESCE(fo.product_cycle_unit, ''),
                fc.plan_cycle_time = COALESCE(fo.product_cycle_time, 1),
                
                -- 2. Base Dimensions from Subscription
                fc.subscription_id = COALESCE(fo.subscription_id, ''),
                fc.subscription_status = COALESCE(ds.status, 0),
                fc.subscription_initial_time = ds.initial_time,
                fc.subscription_cancel_time = ds.cancel_time,
                fc.subscription_next_billing_at = ds.next_billing_at,
                fc.subscription_first_paid_time = ds.first_paid_time,
                
                -- 3. Business Logic
                fc.is_sixteen_plan = IF(fo.product_name IN ({product_list_sql}), 1, 0),
                fc.plan_type = IF(fo.product_cycle_unit = 'YEAR', 2, 1),
                fc.has_trial = IF(ds.initial_time IS NOT NULL AND ds.first_paid_time IS NOT NULL,
                                 IF(ABS(TIMESTAMPDIFF(MINUTE, ds.initial_time, ds.first_paid_time)) <= 10, 0, 1),
                                 1), -- Default to having trial if timestamps missing
                fc.cycle_counts = COALESCE(ROUND(DATEDIFF(fc.cycle_end_time, ds.first_paid_time) / 30), 1);
            """
            
            rows_affected = cur.execute(sql)
            conn.commit()
            print(f"Update completed. Affected rows: {rows_affected}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    enrich_fact_cloud_step_a()
