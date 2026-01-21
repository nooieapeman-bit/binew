from config import create_ssh_tunnel, get_db_connection

def check_fact_cloud_schema():
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                print("Checking fact_cloud schema...")
                try:
                    cur.execute("DESCRIBE `fact_cloud`")
                    for row in cur.fetchall():
                        print(row)
                except Exception as e:
                    print(f"Error checking fact_cloud: {e}")
                    print("Checking fact_order schema instead...")
                    try:
                        cur.execute("DESCRIBE `fact_order`")
                        for row in cur.fetchall():
                            print(row)
                    except Exception as e2:
                        print(f"Error checking fact_order: {e2}")

        finally:
            conn.close()

if __name__ == "__main__":
    check_fact_cloud_schema()
