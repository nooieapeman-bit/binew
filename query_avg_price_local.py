import pymysql
from config import VALID_PRODUCTS

LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def get_avg_prices():
    print("Connecting to local database to query fact_cloud...")
    try:
        conn = pymysql.connect(**LOCAL_DB_CONFIG)
    except Exception as e:
        print(f"Failed to connect to local DB: {e}")
        return

    try:
        with conn.cursor() as cur:
            # Check if fact_cloud exists first
            cur.execute("SHOW TABLES LIKE 'fact_cloud'")
            if not cur.fetchone():
                print("Table fact_cloud does not exist in local DB 'bi'.")
                return

            print("Querying average prices for 2025...")
            
            placeholders = ', '.join(['%s'] * len(VALID_PRODUCTS))
            sql = f"""
                SELECT product_name, AVG(cny_amount)
                FROM fact_cloud
                WHERE pay_time >= '2025-01-01 00:00:00' 
                  AND pay_time < '2026-01-01 00:00:00'
                  AND cny_amount > 0
                  AND product_name IN ({placeholders})
                GROUP BY product_name
            """
            
            cur.execute(sql, tuple(VALID_PRODUCTS))
            results = cur.fetchall()
            
            print("\n--- 2025 Average Prices (rounded) ---")
            print(f"{'Product Name':<50} | {'Avg Price (CNY)':<10}")
            print("-" * 65)
            
            found_products = set()
            for row in results:
                product_name = row[0]
                avg_price = row[1]
                rounded_price = int(round(float(avg_price)))
                print(f"{product_name:<50} | {rounded_price:<10}")
                found_products.add(product_name)
            
            # Check for missing products
            missing = set(VALID_PRODUCTS) - found_products
            if missing:
                print("\nMissing/No Data Objects:")
                for p in missing:
                    print(f"- {p}")

    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    get_avg_prices()
