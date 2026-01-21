from config import create_ssh_tunnel, get_db_connection

def check_remote_schemas():
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                tables = ['order', 'subscribe', 'cloud_info', 'order_amount_info']
                for tbl in tables:
                    print(f"\n--- {tbl} ---")
                    try:
                        cur.execute(f"DESCRIBE `{tbl}`")
                        rows = cur.fetchall()
                        for row in rows:
                            print(row)
                    except Exception as e:
                        print(f"Error describing {tbl}: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    check_remote_schemas()
