from config import create_ssh_tunnel, get_db_connection

def count_orders():
    print("Counting total and unique orders in remote database...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Total rows
                cur.execute("SELECT COUNT(*) FROM `order`")
                total_rows = cur.fetchone()[0]
                
                # Unique order_ids
                cur.execute("SELECT COUNT(DISTINCT order_id) FROM `order` WHERE order_id IS NOT NULL AND order_id != ''")
                unique_order_ids = cur.fetchone()[0]
                
                print(f"Total rows in 'order' table: {total_rows}")
                print(f"Unique order_ids: {unique_order_ids}")
                print(f"Total duplicates to be removed: {total_rows - unique_order_ids}")
        finally:
            conn.close()

if __name__ == "__main__":
    count_orders()
