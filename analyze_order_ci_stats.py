import datetime
from config import create_ssh_tunnel, get_db_connection

def analyze_recent_order_ci_stats():
    ts_cutoff = int(datetime.datetime(2024, 10, 1).timestamp())
    print(f"Analyzing SUCCESSFUL orders submitted after 2024-10-01 (TS >= {ts_cutoff})...")
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 1. Total Successful Orders
                cur.execute("SELECT COUNT(*) FROM `order` WHERE submit_time >= %s AND status = 1", (ts_cutoff,))
                total_success = cur.fetchone()[0]
                print(f"Total Successful Orders: {total_success}")

                # 2. Orders with NO cloud_info (1:0)
                # Using NOT EXISTS or LEFT JOIN check
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM `order` o
                    WHERE o.submit_time >= %s AND o.status = 1
                      AND NOT EXISTS (SELECT 1 FROM cloud_info ci WHERE ci.order_id = o.order_id)
                """, (ts_cutoff,))
                count_1_0 = cur.fetchone()[0]
                print(f"Orders with NO cloud_info (1:0): {count_1_0}")

                # 3. Orders with EXACTLY ONE cloud_info (1:1)
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT o.order_id
                        FROM `order` o
                        JOIN cloud_info ci ON o.order_id = ci.order_id
                        WHERE o.submit_time >= %s AND o.status = 1
                        GROUP BY o.order_id
                        HAVING COUNT(*) = 1
                    ) t
                """, (ts_cutoff,))
                count_1_1 = cur.fetchone()[0]
                print(f"Orders with EXACTLY ONE cloud_info (1:1): {count_1_1}")

                # 4. Of the 1:1, how many have time=0
                if count_1_1 > 0:
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM (
                            SELECT o.order_id
                            FROM `order` o
                            JOIN cloud_info ci ON o.order_id = ci.order_id
                            WHERE o.submit_time >= %s AND o.status = 1
                            GROUP BY o.order_id
                            HAVING COUNT(*) = 1
                        ) t
                        JOIN cloud_info ci2 ON t.order_id = ci2.order_id
                        WHERE ci2.start_time = 0 OR ci2.end_time = 0
                    """, (ts_cutoff,))
                    count_1_1_zero = cur.fetchone()[0]
                    print(f"  -> Within 1:1, orders with start_time or end_time = 0: {count_1_1_zero}")

                # 5. Orders with MULTIPLE cloud_info (1:N)
                count_1_n = total_success - count_1_0 - count_1_1
                print(f"Orders with MULTIPLE cloud_info (1:N): {count_1_n}")

        finally:
            conn.close()

if __name__ == "__main__":
    analyze_recent_order_ci_stats()
