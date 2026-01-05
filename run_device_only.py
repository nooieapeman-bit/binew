import json
import pymysql
from config import DB_USER, DB_PASS, DB_NAME
from query_monthly_devices import get_monthly_device_data

LOCAL_PORT = 3307

def run_device_query():
    print(f"Connecting to Local DB on port {LOCAL_PORT}...")
    conn = pymysql.connect(host='127.0.0.1', port=LOCAL_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME)
    try:
        with conn.cursor() as cursor:
            device_data = get_monthly_device_data(cursor)
            print("\n--- DEVICE DATA ---")
            print(json.dumps(device_data, indent=4))
    finally:
        conn.close()

if __name__ == "__main__":
    run_device_query()
