from config import create_ssh_tunnel, get_db_connection
from datetime import datetime

def check_empty_product_end_times():
    end_of_2024 = 1735689600
    
    print("Analyzing end_time distribution for active records with empty product names...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT FROM_UNIXTIME(ci.end_time, '%%Y-%%m') as end_month, COUNT(*) as count
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.start_time < %s AND ci.end_time >= %s
                      AND (o.product_name IS NULL OR o.product_name = '')
                    GROUP BY end_month
                    ORDER BY end_month
                """
                cur.execute(sql, (end_of_2024, end_of_2024))
                results = cur.fetchall()
                
                print(f"{'End Month':<15} | {'Count':<10}")
                print("-" * 30)
                for month, count in results:
                    print(f"{str(month):<15} | {count:<10}")

        finally:
            conn.close()

if __name__ == "__main__":
    check_empty_product_end_times()
