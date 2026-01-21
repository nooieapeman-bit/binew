import datetime
from config import create_ssh_tunnel, get_db_connection

def analyze_sample_month_stats():
    # Nov 2024
    start_ts = int(datetime.datetime(2024, 11, 1).timestamp())
    end_ts = int(datetime.datetime(2024, 12, 1).timestamp())
    
    print(f"Analyzing SUCCESSFUL orders in November 2024 ({start_ts} to {end_ts})...")
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 1. Total Successful Orders
                cur.execute("SELECT COUNT(*) FROM `order` WHERE submit_time >= %s AND submit_time < %s AND status = 1", (start_ts, end_ts))
                total_success = cur.fetchone()[0]
                print(f"Total Successful Orders: {total_success}")

                # 2. Relationship counts
                sql = """
                    SELECT ci_count, COUNT(*) 
                    FROM (
                        SELECT o.order_id, COUNT(ci.id) as ci_count
                        FROM `order` o
                        LEFT JOIN cloud_info ci ON o.order_id = ci.order_id
                        WHERE o.submit_time >= %s AND o.submit_time < %s AND o.status = 1
                        GROUP BY o.order_id
                    ) t
                    GROUP BY ci_count
                """
                cur.execute(sql, (start_ts, end_ts))
                rel_stats = cur.fetchall()
                print("\nRelationship Statistics for Nov 2024:")
                rel_map = {row[0]: row[1] for row in rel_stats}
                for count_val, frequency in sorted(rel_map.items()):
                    print(f"  {count_val} records: {frequency} orders")

                # 3. 1:1 time validity
                if 1 in rel_map:
                    sql_1_1_zero = """
                        SELECT COUNT(*)
                        FROM (
                            SELECT o.order_id
                            FROM `order` o
                            JOIN cloud_info ci ON o.order_id = ci.order_id
                            WHERE o.submit_time >= %s AND o.submit_time < %s AND o.status = 1
                            GROUP BY o.order_id
                            HAVING COUNT(*) = 1
                        ) t
                        JOIN cloud_info ci2 ON t.order_id = ci2.order_id
                        WHERE ci2.start_time = 0 OR ci2.end_time = 0
                    """
                    cur.execute(sql_1_1_zero, (start_ts, end_ts))
                    count_1_1_zero = cur.fetchone()[0]
                    print(f"\nOf the {rel_map[1]} 1:1 orders:")
                    print(f"  start_time or end_time = 0: {count_1_1_zero}")
                    print(f"  valid non-zero periods: {rel_map[1] - count_1_1_zero}")

        finally:
            conn.close()

if __name__ == "__main__":
    analyze_sample_month_stats()
