import json
import pymysql
from config import DB_USER, DB_PASS, DB_NAME
from query_direct_buyer_history import get_direct_buyer_history

LOCAL_PORT = 3307

def run_history_query():
    print(f"Connecting to Local DB on port {LOCAL_PORT}...")
    conn = pymysql.connect(host='127.0.0.1', port=LOCAL_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME)
    try:
        with conn.cursor() as cursor:
            history_data = get_direct_buyer_history(cursor)
            print("\n--- BUYER HISTORY DATA ---")
            print(json.dumps(history_data, indent=4))
    finally:
        conn.close()

if __name__ == "__main__":
    run_history_query()
