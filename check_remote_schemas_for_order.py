from config import create_ssh_tunnel, get_db_connection

def check_remote_schemas():
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                print("Checking 'device' table schema...")
                cur.execute("DESCRIBE `device`")
                for row in cur.fetchall():
                    print(row)
                
                print("\nChecking 'set_meal' table schema...")
                cur.execute("DESCRIBE `set_meal`")
                for row in cur.fetchall():
                    print(row)
        finally:
            conn.close()

if __name__ == "__main__":
    check_remote_schemas()
