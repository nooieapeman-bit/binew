from config import VALID_PRODUCTS

def get_trial_orders_data(cursor):
    print("Calculating Monthly Trial Orders...")
    trial_orders_data = [0] * 12
    
    sql_trials = """
        SELECT 
            FROM_UNIXTIME(pay_time, '%%c') as month_idx,
            COUNT(*) as count
        FROM `order`
        WHERE amount = 0
          AND status = 1
          AND pay_time >= UNIX_TIMESTAMP('2025-01-01 00:00:00')
          AND pay_time < UNIX_TIMESTAMP('2026-01-01 00:00:00')
          AND product_name IN %s
        GROUP BY month_idx
    """
    cursor.execute(sql_trials, (VALID_PRODUCTS,))
    rows = cursor.fetchall()
    for r in rows:
        m_idx = int(r[0]) - 1
        trial_orders_data[m_idx] = int(r[1])
        
    return trial_orders_data
