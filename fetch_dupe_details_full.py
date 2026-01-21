from config import create_ssh_tunnel, get_db_connection
import datetime

def format_ts(ts):
    try:
        if ts is None or ts == 0: return "0"
        return datetime.datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(ts)

def fetch_all_dupe_details():
    start_2025 = 1735689600
    end_2025 = 1767225599
    
    print(f"Fetching ALL 1:N relationships for CloudInfo ending in 2025...\n")
    
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
                
                print(f"Total CloudInfo records found: {len(ci_dupes)}\n")
                
                for idx, (ci_id, ci_order_id, o_count) in enumerate(ci_dupes, 1):
                    oid_str = ci_order_id.decode('utf-8', errors='ignore') if isinstance(ci_order_id, bytes) else ci_order_id
                    print(f"[{idx}] CloudInfo ID: {ci_id} | OrderID STR: {oid_str}")
                    
                    # 1. CloudInfo details
                    cur.execute("SELECT uid, start_time, end_time, is_event, is_delete, created_at FROM cloud_info WHERE id = %s", (ci_id,))
                    ci = cur.fetchone()
                    print(f"    - UID: {ci[0]}")
                    print(f"    - Service Period: {format_ts(ci[1])}  TO  {format_ts(ci[2])}")
                    print(f"    - Flags: is_event={ci[3]}, is_delete={ci[4]} | Record Created: {ci[5]}")
                    
                    # 2. Linked Orders details
                    cur.execute("SELECT id, status, amount, product_name, pay_time, submit_time FROM `order` WHERE order_id = %s", (ci_order_id,))
                    orders = cur.fetchall()
                    print(f"    - Linked Orders ({len(orders)}):")
                    for o in orders:
                        print(f"      > Order INT ID: {o[0]}")
                        print(f"        Status: {o[1]} | Amount: {o[2]} | Product: {o[3]}")
                        print(f"        PayTime:    {format_ts(o[4])}")
                        print(f"        SubmitTime: {format_ts(o[5])}")
                    print("-" * 80)
        finally:
            conn.close()

if __name__ == "__main__":
    fetch_all_dupe_details()
