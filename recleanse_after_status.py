import pymysql
import time

LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def cleanse_after_status():
    print("Connecting to local database...")
    conn = pymysql.connect(**LOCAL_DB_CONFIG)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    try:
        # 1. Reset after_status to 0
        print("Resetting all after_status to 0...")
        cur.execute("UPDATE fact_cloud SET after_status = 0")
        conn.commit()

        # 2. Get the data subset
        print("Fetching data subset (cycle_end_time >= 2024-12-01)...")
        sql_fetch = """
            SELECT id, uid, uuid, subscription_id, cycle_end_time, amount
            FROM fact_cloud
            WHERE cycle_end_time >= '2024-12-01 00:00:00'
            ORDER BY uid, uuid, subscription_id, cycle_end_time DESC
        """
        cur.execute(sql_fetch)
        rows = cur.fetchall()
        print(f"Total rows to process: {len(rows)}")

        updates = []
        current_group = None
        
        # Threshold for status 1 vs 4
        cutoff_date = '2025-12-31 23:59:59'
        
        # Logic:
        # Group by uid, uuid, subscription_id.
        # Within each group, rows are DESC by cycle_end_time.
        # First row is index 0.
        
        group_row_index = 0
        
        for row in rows:
            group_key = (row['uid'], row['uuid'], row['subscription_id'])
            
            if group_key != current_group:
                # New group
                current_group = group_key
                group_row_index = 0
            else:
                group_row_index += 1
            
            status = 0
            if group_row_index == 0:
                # First row in group (Latest end time)
                if str(row['cycle_end_time']) > cutoff_date:
                    status = 1
                else:
                    status = 4
            else:
                # 2nd to n-th row
                if row['amount'] == 0:
                    status = 3
                else:
                    status = 2
            
            updates.append((status, row['id']))
            
            # Batch update every 10,000 rows
            if len(updates) >= 10000:
                cur.executemany("UPDATE fact_cloud SET after_status = %s WHERE id = %s", updates)
                conn.commit()
                updates = []
                print(f"Updated {group_row_index + 1} records in current batch...")

        if updates:
            cur.executemany("UPDATE fact_cloud SET after_status = %s WHERE id = %s", updates)
            conn.commit()

        print("Cleansing completed successfully.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    start = time.time()
    cleanse_after_status()
    print(f"Total time: {time.time() - start:.2f} seconds")
