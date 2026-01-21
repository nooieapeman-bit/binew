from config import create_ssh_tunnel, get_db_connection
from datetime import datetime

def check_empty_product_id_distribution():
    end_of_2024 = 1735689600
    
    print("Checking product_id distribution for records where product_name is empty...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT o.product_id, COUNT(DISTINCT ci.id) as count
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.start_time < %s AND ci.end_time >= %s
                      AND (o.product_name IS NULL OR o.product_name = '')
                    GROUP BY o.product_id
                    ORDER BY count DESC
                """
                cur.execute(sql, (end_of_2024, end_of_2024))
                results = cur.fetchall()
                
                print(f"{'Product ID':<60} | {'Count':<10}")
                print("-" * 75)
                for pid, count in results:
                    print(f"{str(pid):<60} | {count:<10}")

        finally:
            conn.close()

if __name__ == "__main__":
    check_empty_product_id_distribution()
