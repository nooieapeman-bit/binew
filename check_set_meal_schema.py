from config import create_ssh_tunnel, get_db_connection

def check_set_meal_schema():
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("DESCRIBE `set_meal`")
                for row in cur.fetchall():
                    print(row)
        finally:
            conn.close()

if __name__ == "__main__":
    check_set_meal_schema()
