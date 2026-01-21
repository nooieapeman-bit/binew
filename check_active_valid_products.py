from config import create_ssh_tunnel, get_db_connection, VALID_PRODUCTS
from datetime import datetime

def count_active_valid_products():
    end_of_2024 = int(datetime(2025, 1, 1).timestamp())
    
    print(f"Checking VALID_PRODUCTS overlap for subscriptions active at end of 2024...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Use a query that join cloud_info and order and filters by the valid product list
                sql = """
                    SELECT COUNT(DISTINCT ci.id)
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.start_time < %s 
                      AND ci.end_time >= %s 
                      AND o.product_name IN %s
                """
                cur.execute(sql, (end_of_2024, end_of_2024, VALID_PRODUCTS))
                count = cur.fetchone()[0]
                print(f"Number of active records (filtered by VALID_PRODUCTS): {count}")
                
                # Also check how many have NO matching product name in that list
                sql_all_joined = """
                    SELECT COUNT(DISTINCT ci.id)
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.start_time < %s 
                      AND ci.end_time >= %s 
                """
                cur.execute(sql_all_joined, (end_of_2024, end_of_2024))
                joined_count = cur.fetchone()[0]
                print(f"Total active records that have any order record: {joined_count}")

        finally:
            conn.close()

if __name__ == "__main__":
    count_active_valid_products()
