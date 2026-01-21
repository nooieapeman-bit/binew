from config import create_ssh_tunnel, get_db_connection, VALID_PRODUCTS
from datetime import datetime

def check_invalid_product_distribution():
    end_of_2024 = int(datetime(2025, 1, 1).timestamp())
    
    print(f"Analyzing product_name distribution for 396 non-valid records at end of 2024...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT o.product_name, COUNT(DISTINCT ci.id) as count
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.start_time < %s 
                      AND ci.end_time >= %s 
                      AND o.product_name NOT IN %s
                    GROUP BY o.product_name
                    ORDER BY count DESC
                """
                cur.execute(sql, (end_of_2024, end_of_2024, VALID_PRODUCTS))
                results = cur.fetchall()
                
                print(f"{'Product Name':<60} | {'Count':<10}")
                print("-" * 75)
                for name, count in results:
                    print(f"{str(name):<60} | {count:<10}")

        finally:
            conn.close()

if __name__ == "__main__":
    check_invalid_product_distribution()
