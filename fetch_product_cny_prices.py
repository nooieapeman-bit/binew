from config import create_ssh_tunnel, get_db_connection, VALID_PRODUCTS
import pymysql

def fetch_prices():
    print(f"Fetching prices for {len(VALID_PRODUCTS)} products...")
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Check set_meal
                placeholders = ','.join(['%s'] * len(VALID_PRODUCTS))
                sql_set_meal = f"SELECT name, price, currency FROM set_meal WHERE name IN ({placeholders})"
                cur.execute(sql_set_meal, tuple(VALID_PRODUCTS))
                set_meal_results = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
                
                # Check actual order amounts (approximate current CNY price)
                # calculating average amount_cny for recent orders
                price_map = {}
                for product in VALID_PRODUCTS:
                    sql_order = """
                        SELECT oai.amount_cny 
                        FROM `order` o
                        JOIN order_amount_info oai ON o.id = oai.order_int_id
                        WHERE o.product_name = %s 
                          AND o.amount > 0 
                          AND o.pay_time > UNIX_TIMESTAMP('2025-01-01')
                        LIMIT 20
                    """
                    cur.execute(sql_order, (product,))
                    amounts = [row[0] for row in cur.fetchall()]
                    if amounts:
                        # Get the most common amount (mode) to avoid exchange rate fluctuations/partial refunds affecting avg
                        from collections import Counter
                        most_common = Counter(amounts).most_common(1)[0][0]
                        price_map[product] = float(most_common)
                    else:
                        price_map[product] = None
                
                return set_meal_results, price_map
                
        finally:
            conn.close()

if __name__ == "__main__":
    set_meal_res, order_res = fetch_prices()
    
    print("\n--- RESULTS ---")
    header = f"{'Product Name':<50} | {'Set Meal Price':<15} | {'Order CNY Amount (Mode 2025)':<25}"
    print(header)
    print("-" * len(header))
    
    for p in VALID_PRODUCTS:
        sm = set_meal_res.get(p, ("N/A", "N/A"))
        oa = order_res.get(p, "N/A")
        print(f"{p:<50} | {sm[0]} {sm[1]:<5} | {oa}")
