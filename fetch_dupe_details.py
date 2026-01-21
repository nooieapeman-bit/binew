from config import create_ssh_tunnel, get_db_connection
import datetime

def fetch_dupe_details():
    start_2025 = 1735689600
    end_2025 = 1767225599
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Find the cloud_info records that link to multiple orders
                sql = """
                    SELECT ci.id, ci.order_id, COUNT(o.id) as order_count
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.end_time >= %s AND ci.end_time <= %s
                      AND ci.order_id IS NOT NULL AND ci.order_id != ''
                    GROUP BY ci.id, ci.order_id
                    HAVING order_count > 1
                """
                cur.execute(sql, (start_2025, end_2025))
                ci_dupes = cur.fetchall()
                
                print(f"Found {len(ci_dupes)} cloud_info records with 1:N order relationship in 2025.\n")
                
                for ci_id, ci_order_id, o_count in ci_dupes:
                    # Handle binary order_id if it's varbinary
                    oid_str = ci_order_id.decode('utf-8', errors='ignore') if isinstance(ci_order_id, bytes) else ci_order_id
                    print(f"=== CloudInfo ID: {ci_id} | OrderID: {oid_str} (Linked to {o_count} orders) ===")
                    
                    # 1. CloudInfo details (just this specific record)
                    cur.execute("SELECT id, uid, start_time, end_time, is_event, is_delete, created_at FROM cloud_info WHERE id = %s", (ci_id,))
                    ci_row = cur.fetchone()
                    print(f"  [CI Details] UID: {ci_row[1]}, Start: {ci_row[2]}, End: {ci_row[3]}, Event: {ci_row[4]}, Del: {ci_row[5]}, Created: {ci_row[6]}")
                    
                    # 2. Linked Orders details
                    cur.execute("SELECT id, status, amount, product_name, pay_time, submit_time FROM `order` WHERE order_id = %s", (ci_order_id,))
                    orders = cur.fetchall()
                    print("  [Linked Orders]")
                    for o in orders:
                        print(f"    - Order Int ID: {o[0]}, Status: {o[1]}, Amount: {o[2]}, Product: {o[3]}, PayTime: {o[4]}, SubmitTime: {o[5]}")
                    print("-" * 60)
        finally:
            conn.close()

if __name__ == "__main__":
    fetch_dupe_details()
