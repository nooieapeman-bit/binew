import pymysql
import time
from config import VALID_PRODUCTS

LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def cleanse_user_first_trial_time():
    print("Connecting to local database...")
    conn = pymysql.connect(**LOCAL_DB_CONFIG)
    cur = conn.cursor()

    try:
        # 1. Reset first_trial_time to NULL
        print("Resetting all dim_user.first_trial_time to NULL...")
        cur.execute("UPDATE dim_user SET first_trial_time = NULL")
        conn.commit()

        # 2. Optimize: Ensure indexes exist
        print("Checking index on dim_user(uid)...")
        cur.execute("SHOW INDEX FROM dim_user WHERE Key_name = 'idx_uid'")
        if not cur.fetchone():
            cur.execute("CREATE INDEX idx_uid ON dim_user(uid)")
            print("Index created on dim_user.")

        # 3. Update first_trial_time with refined logic
        print("Updating dim_user.first_trial_time based on refined fact_cloud criteria...")
        
        start_time = time.time()
        
        # Prepare valid products string for SQL IN clause
        valid_products_sql = "', '".join(VALID_PRODUCTS)
        
        update_sql = f"""
        UPDATE dim_user u
        JOIN (
            SELECT uid, MIN(pay_time) as min_trial_time
            FROM fact_cloud
            WHERE amount = 0 
              AND pay_time >= '2024-10-01'
              AND product_name IN ('{valid_products_sql}')
            GROUP BY uid
        ) t ON u.uid COLLATE utf8mb4_unicode_ci = t.uid
        SET u.first_trial_time = t.min_trial_time
        """
        
        affected_rows = cur.execute(update_sql)
        conn.commit()
        
        duration = time.time() - start_time
        print(f"Update completed in {duration:.2f} seconds.")
        print(f"Updated {affected_rows} rows.")

    except Exception as e:
        print(f"Error executing update: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    cleanse_user_first_trial_time()
