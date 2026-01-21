from config import get_db_connection, create_ssh_tunnel
from query_active_retention import get_active_retention_analysis
import json

def main():
    try:
        conn = get_db_connection(3307)
        print("Connected to Local DB on port 3307...")
    except Exception as e:
        print(f"Direct connection failed: {e}. Attempting to create tunnel...")
        tunnel = create_ssh_tunnel()
        tunnel.start()
        print(f"SSH Tunnel started on port {tunnel.local_bind_port}")
        conn = get_db_connection(tunnel.local_bind_port)

    with conn.cursor() as cursor:
        data = get_active_retention_analysis(cursor)
        
        print("\n--- ACTIVE RETENTION DATA ---")
        print(json.dumps(data, indent=4))

    conn.close()

if __name__ == "__main__":
    main()
