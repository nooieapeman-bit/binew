from config import create_ssh_tunnel, get_db_connection
from datetime import datetime

def check_1_to_n_active_2024():
    end_of_2024 = 1735689600
    
    print("Analyzing 1:N relationships for subscriptions active at end of 2024...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # One cloud_info mapping to multiple orders
                sql = """
                    SELECT COUNT(*) FROM (
                        SELECT ci.id
                        FROM cloud_info ci
                        JOIN `order` o ON ci.order_id = o.order_id
                        WHERE ci.start_time < %s AND ci.end_time >= %s
                        GROUP BY ci.id
                        HAVING COUNT(o.id) > 1
                    ) as t
                """
                cur.execute(sql, (end_of_2024, end_of_2024))
                count = cur.fetchone()[0]
                print(f"Number of cloud_info records with 1:N order relationship: {count}")
                
                if count > 0:
                    print("\nSample of 1:N records:")
                    sql_samples = """
                        SELECT ci.id, ci.order_id, COUNT(o.id) as o_count
                        FROM cloud_info ci
                        JOIN `order` o ON ci.order_id = o.order_id
                        WHERE ci.start_time < %s AND ci.end_time >= %s
                        GROUP BY ci.id
                        HAVING o_count > 1
                        LIMIT 5
                    """
                    cur.execute(sql_samples, (end_of_2024, end_of_2024))
                    samples = cur.fetchall()
                    for ci_id, oid, cnt in samples:
                        print(f"CI_ID: {ci_id} | OrderID: {oid} | Linked Orders: {cnt}")
        finally:
            conn.close()

if __name__ == "__main__":
    check_1_to_n_active_2024()
