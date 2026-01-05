from config import VALID_PRODUCTS

def get_revenue_data(cursor):
    print("Calculating Revenue & Valid Orders...")
    revenue_data = [0] * 12
    valid_orders_data = [0] * 12
    
    sql_revenue = """
        SELECT 
            FROM_UNIXTIME(o.pay_time, '%%c') as month_idx, 
            SUM(oai.amount_cny - oai.transaction_fee_cny) as net_revenue,
            COUNT(o.id) as order_count
        FROM `order` o
        JOIN order_amount_info oai ON o.id = oai.order_int_id
        WHERE o.amount > 0 
          AND o.status = 1
          AND o.pay_time >= UNIX_TIMESTAMP('2025-01-01 00:00:00')
          AND o.pay_time < UNIX_TIMESTAMP('2026-01-01 00:00:00')
          AND o.product_name IN %s
        GROUP BY month_idx
    """
    cursor.execute(sql_revenue, (VALID_PRODUCTS,))
    rows = cursor.fetchall()
    
    for r in rows:
        m_idx = int(r[0]) - 1 # 1-based to 0-based
        revenue_data[m_idx] = float(r[1])
        valid_orders_data[m_idx] = int(r[2])
        
    return revenue_data, valid_orders_data
