from config import create_ssh_tunnel, get_db_connection

def check_one_to_one():
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                print("Checking order_amount_info uniqueness (by order_int_id)...")
                # order_amount_info.order_int_id is PRI, so it's already guaranteed unique per row.
                # But let's verify if there are any duplicates just in case some weirdness exists.
                cur.execute("SELECT order_int_id, COUNT(*) FROM order_amount_info GROUP BY order_int_id HAVING COUNT(*) > 1 LIMIT 5")
                dupes_oai = cur.fetchall()
                if dupes_oai:
                    print(f"Found duplicates in order_amount_info: {dupes_oai}")
                else:
                    print("Confirmed: Every order has at most ONE order_amount_info record.")

                print("\nChecking cloud_info uniqueness (by order_id)...")
                cur.execute("""
                    SELECT order_id, COUNT(*) 
                    FROM cloud_info 
                    WHERE order_id IS NOT NULL AND order_id != '' 
                    GROUP BY order_id 
                    HAVING COUNT(*) > 1 
                    LIMIT 5
                """)
                dupes_ci = cur.fetchall()
                if dupes_ci:
                    print(f"FAILED: Found orders with MULTIPLE cloud_info records: {dupes_ci}")
                else:
                    print("Confirmed: Every order has at most ONE cloud_info record.")
        finally:
            conn.close()

if __name__ == "__main__":
    check_one_to_one()
