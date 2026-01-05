import pymysql
from sshtunnel import SSHTunnelForwarder
from config import create_ssh_tunnel, get_db_connection

def check_structure():
    print("Checking user_device structure...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DESCRIBE `user_device`")
                for r in cursor.fetchall():
                    print(r)
                
                print("\nChecking bind_type sample:")
                cursor.execute("SELECT bind_type, COUNT(*) FROM user_device GROUP BY bind_type LIMIT 10")
                for r in cursor.fetchall():
                    print(r)
        finally:
            conn.close()

if __name__ == "__main__":
    check_structure()
