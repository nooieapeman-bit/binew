from config import create_ssh_tunnel, get_db_connection

PRODUCTS = [
    '14-day history event recording monthly',
    '30-day video history event recording pro monthly'
]

def fetch_prices():
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                for product in PRODUCTS:
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
                        from collections import Counter
                        most_common = Counter(amounts).most_common(1)[0][0]
                        print(f"{product}: {most_common}")
                    else:
                        print(f"{product}: N/A")
        finally:
            conn.close()

if __name__ == "__main__":
    fetch_prices()
