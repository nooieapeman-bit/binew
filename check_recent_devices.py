import pymysql
import datetime
import time
from config import create_ssh_tunnel, DB_USER, DB_PASS, DB_NAME

def get_recent_device_distribution():
    print("Connecting to remote database...")
    with create_ssh_tunnel() as server:
        conn = pymysql.connect(
            host='127.0.0.1', 
            port=server.local_bind_port, 
            user=DB_USER, 
            password=DB_PASS, 
            database=DB_NAME
        )
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                # Calculate timestamp for 30 days ago
                thirty_days_ago = int(time.time()) - (30 * 24 * 60 * 60)
                thirty_days_ago_dt = datetime.datetime.fromtimestamp(thirty_days_ago)
                print(f"Querying devices created after: {thirty_days_ago_dt} (TS: {thirty_days_ago})")

                sql = """
                    SELECT model_code, COUNT(*) as count
                    FROM device
                    WHERE create_time >= %s
                    GROUP BY model_code
                    ORDER BY count DESC
                """
                cur.execute(sql, (thirty_days_ago))
                rows = cur.fetchall()

                print("\n--- Model Code Distribution (Last 30 Days) ---")
                total = sum(row['count'] for row in rows)
                print(f"{'Model Code':<30} | {'Count':<10} | {'Percentage':<10}")
                print("-" * 55)
                for row in rows:
                    pct = (row['count'] / total * 100) if total > 0 else 0
                    print(f"{row['model_code']:<30} | {row['count']:<10} | {pct:>8.2f}%")
                print("-" * 55)
                print(f"{'Total':<30} | {total:<10} | 100.00%")

        finally:
            conn.close()

if __name__ == "__main__":
    get_recent_device_distribution()
