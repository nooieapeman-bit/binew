from config import create_ssh_tunnel, get_db_connection
from datetime import datetime

def check_1_to_n_2025_starts():
    end_of_2024 = 1735689600
    
    print("Checking 1:N relationships for cloud_info records starting in 2025 or later...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Count records starting in 2025 with multiple orders
                sql = """
                    SELECT COUNT(*) FROM (
                        SELECT ci.id
                        FROM cloud_info ci
                        JOIN `order` o ON ci.order_id = o.order_id
                        WHERE ci.start_time >= %s
                        GROUP BY ci.id
                        HAVING COUNT(o.id) > 1
                    ) as t
                """
                cur.execute(sql, (end_of_2024,))
                count = cur.fetchone()[0]
                print(f"Number of 2025+ cloud_info records with 1:N order relationship: {count}")
                
        finally:
            conn.close()

if __name__ == "__main__":
    check_1_to_n_2025_starts()
