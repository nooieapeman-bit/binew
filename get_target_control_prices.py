
import pymysql
from config import create_ssh_tunnel, get_db_connection

# Targeted packages and their associated currencies
target_configs = [
    ('14-day video history event recording annually', 'GBP'),
    ('14-day video history event recording annually', 'EUR'),
    ('14-day history event recording monthly', 'EUR'),
    ('14-day history event recording monthly', 'GBP'),
    ('30-day video history event recording annually', 'GBP'),
    ('30-day video history event recording annually', 'EUR'),
    ('30-day video history event recording monthly', 'GBP'),
    ('30-day video history event recording monthly', 'EUR')
]

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            print(f"Fetching prices for specific Control Group packages...")
            
            # Extract unique package names for the query
            pkg_names = list(set(config[0] for config in target_configs))
            placeholders = ', '.join(['%s'] * len(pkg_names))
            
            sql = f"""
                SELECT sm.name, smp.currency, smp.price
                FROM set_meal_price smp
                JOIN set_meal sm ON smp.set_meal_code = sm.code
                WHERE sm.name IN ({placeholders})
                  AND smp.currency IN ('GBP', 'EUR')
            """
            cur.execute(sql, pkg_names)
            results = cur.fetchall()
            
            # Map results for easy lookup: (name, currency) -> price
            price_map = {}
            for name, curr, price in results:
                price_map[(name, curr)] = float(price)
            
            print(f"\nTarget Package Prices:")
            print(f"{'Package Name':<50} | {'Currency':<10} | {'Price':<10}")
            print("-" * 75)
            
            for name, curr in target_configs:
                price = price_map.get((name, curr), "N/A")
                print(f"{name:<50} | {curr:<10} | {price:<10}")
                
    finally:
        conn.close()
