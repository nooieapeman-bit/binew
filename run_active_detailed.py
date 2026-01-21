from config import get_db_connection, create_ssh_tunnel
from query_active_detailed import get_detailed_active_subscriptions
import json

def main():
    try:
        # Try connecting directly first (users tunnel might be up)
        conn = get_db_connection(3307)
        print("Connected to Local DB on port 3307...")
    except Exception as e:
        print(f"Direct connection failed: {e}. Attempting to create tunnel...")
        tunnel = create_ssh_tunnel()
        tunnel.start()
        print(f"SSH Tunnel started on port {tunnel.local_bind_port}")
        conn = get_db_connection(tunnel.local_bind_port)

    with conn.cursor() as cursor:
        data = get_detailed_active_subscriptions(cursor)
        
        print("\n--- ACTIVE SUB DETAILED DATA ---")
        print(json.dumps(data, indent=4))

    conn.close()

if __name__ == "__main__":
    main()
