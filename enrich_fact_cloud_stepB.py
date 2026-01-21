import pymysql

LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def enrich_fact_cloud_step_b():
    conn = pymysql.connect(**LOCAL_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            print("Step B: Calculating after_status (Pending, Renewed, Close)...")
            
            # Mapping based on table comment: 1-pending, 2-renewed, 3-paid fail, 4-close
            # 1. Initialize all records to 'Renewed' (2)
            print("Initializing after_status to 2 (Renewed)...")
            cur.execute("UPDATE fact_cloud SET after_status = 2")
            conn.commit()
            
            # 2. Identify the last record for each subscription and update its status
            # Logic: 
            # - If end_time > 2025-12-31 -> 1 (Pending)
            # - Else if subscription_status = 0 OR end_time < (2025-12-31 - 90 days) -> 4 (Close)
            # - Else -> 2 (Renewed)
            
            print("Updating final status for the latest record of each subscription...")
            sql = """
            UPDATE fact_cloud fc
            INNER JOIN (
                SELECT subscription_id, MAX(cycle_end_time) as max_end
                FROM fact_cloud
                WHERE subscription_id != ''
                GROUP BY subscription_id
            ) last_record ON fc.subscription_id = last_record.subscription_id 
                         AND fc.cycle_end_time = last_record.max_end
            SET fc.after_status = CASE 
                WHEN fc.cycle_end_time > '2025-12-31 23:59:59' THEN 1  -- Pending
                WHEN fc.subscription_status = 0 THEN 4                -- Close
                WHEN DATEDIFF('2025-12-31', fc.cycle_end_time) > 90 THEN 4 -- Close
                ELSE 2 -- Renewed
            END;
            """
            
            rows_affected = cur.execute(sql)
            conn.commit()
            print(f"Update completed. Affected rows: {rows_affected}")
            
            # Check Results
            cur.execute("SELECT after_status, COUNT(*) FROM fact_cloud GROUP BY after_status")
            results = cur.fetchall()
            print("\nFinal after_status distribution:")
            for status, count in results:
                status_name = {1: 'Pending', 2: 'Renewed', 4: 'Close', 0: 'Unknown'}.get(status, f'Code {status}')
                print(f" - {status_name}: {count}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    enrich_fact_cloud_step_b()
