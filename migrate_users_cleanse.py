import pymysql
import time
from config import LOCAL_BIND_PORT, DB_USER, DB_PASS, DB_NAME

LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def cleanse_user_first_bind_time():
    print("Connecting to local database...")
    conn = pymysql.connect(**LOCAL_DB_CONFIG)
    cur = conn.cursor()

    try:
        # 1. Optimize: Ensure User UID index exists
        print("Checking index on dim_user(uid)...")
        cur.execute("SHOW INDEX FROM dim_user WHERE Key_name = 'idx_uid'")
        if not cur.fetchone():
            print("Index missing. Creating index 'idx_uid' on dim_user(uid)...")
            cur.execute("CREATE INDEX idx_uid ON dim_user(uid)")
            print("Index created.")
        else:
            print("Index 'idx_uid' exists.")

        # 2. Update first_bind_time
        print("Updating dim_user.first_bind_time based on dim_user_device...")
        
        # We use a multi-table UPDATE with a subquery
        # Logic: Min of first_bind_time. If null, use last_bind_time.
        # COALESCE(first_bind_time, last_bind_time) returns first non-null.
        # So MIN(COALESCE(...)) gives the earliest effective bind time.
        
        start_time = time.time()
        
        update_sql = """
        UPDATE dim_user u
        JOIN (
            SELECT uid, MIN(COALESCE(first_bind_time, last_bind_time)) as min_bind_time
            FROM dim_user_device
            WHERE first_bind_time IS NOT NULL OR last_bind_time IS NOT NULL
            GROUP BY uid
        ) d ON u.uid = d.uid
        SET u.first_bind_time = d.min_bind_time
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
    cleanse_user_first_bind_time()
