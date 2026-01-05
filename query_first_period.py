from config import VALID_PRODUCTS

def get_first_period_data(cursor):
    print("Calculating First Period Orders...")
    first_period_data = [0] * 12
    
    # Logic: Min pay_time per subscribe_id
    sql_first = """
        SELECT 
            FROM_UNIXTIME(first_paid_time, '%%c') as month_idx,
            COUNT(*) as count
        FROM (
            SELECT 
                subscribe_id,
                MIN(pay_time) as first_paid_time
            FROM `order`
            WHERE amount > 0 
              AND status = 1
              AND subscribe_id != ''
              AND product_name IN %s
            GROUP BY subscribe_id
        ) t
        WHERE first_paid_time >= UNIX_TIMESTAMP('2025-01-01 00:00:00')
          AND first_paid_time < UNIX_TIMESTAMP('2026-01-01 00:00:00')
        GROUP BY month_idx
    """
    cursor.execute(sql_first, (VALID_PRODUCTS,))
    rows = cursor.fetchall()
    for r in rows:
        m_idx = int(r[0]) - 1
        first_period_data[m_idx] = int(r[1])
        
    return first_period_data
