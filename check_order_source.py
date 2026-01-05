from config import get_db_connection, create_ssh_tunnel

def check_schema():
    with create_ssh_tunnel() as tunnel:
        print(f"Tunnel active on port {tunnel.local_bind_port}")
        conn = get_db_connection(local_port=tunnel.local_bind_port)
        try:
            with conn.cursor() as cursor:
                # Get columns
                cursor.execute("DESCRIBE `order`")
                columns = [row[0] for row in cursor.fetchall()]
                print("Columns:", columns)
                
                # Check distinct values for potential candidates
                check_cols = ['partner_code', 'pay_type']
                for col in check_cols:
                    if col in columns:
                        print(f"Checking DISTINCT {col} (limit 20)...")
                        try:
                            cursor.execute(f"SELECT DISTINCT {col} FROM `order` LIMIT 20")
                            print(f"Values for {col}:", cursor.fetchall())
                        except:
                            pass
        finally:
            conn.close()

if __name__ == "__main__":
    check_schema()
