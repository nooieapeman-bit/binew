import datetime
import calendar
from config import VALID_PRODUCTS

def get_first_period_reg_dist(cursor):
    print("Analyzing First Period Paid Users Registration Distribution...")
    
    # Define months for 2025
    months_2025 = []
    for m in range(1, 13):
        months_2025.append(f"2025-{m:02d}")

    # Results container
    analysis = []

    for month_str in months_2025:
        y, m = map(int, month_str.split('-'))
        start_dt = datetime.datetime(y, m, 1)
        last_day = calendar.monthrange(y, m)[1]
        end_dt = datetime.datetime(y, m, last_day, 23, 59, 59)
        
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        # 1. Get unique uids who made their first payment (initial_payment_time) in this month
        # We check the subscribe table for initial_payment_time
        # and join with order to ensure it's a paid order for VALID_PRODUCTS
        sql = """
            SELECT DISTINCT s.uid
            FROM subscribe s
            JOIN `order` o ON s.subscribe_id = o.subscribe_id
            WHERE s.initial_payment_time >= %s AND s.initial_payment_time <= %s
              AND o.status = 1 AND o.amount > 0
              AND o.product_name IN %s
        """
        cursor.execute(sql, (start_ts, end_ts, VALID_PRODUCTS))
        uids = [r[0] for r in cursor.fetchall() if r[0]]
        
        user_count = len(uids)
        reg_dist = {m: 0 for m in months_2025}
        reg_dist['Earlier'] = 0

        if uids:
            # 2. Fetch registration times for these users
            # Using chunks to avoid large IN clause issues
            chunk_size = 1000
            for i in range(0, len(uids), chunk_size):
                chunk = uids[i:i+chunk_size]
                fmt = ','.join(['%s'] * len(chunk))
                sql_reg = f"SELECT register_time FROM `user` WHERE uid IN ({fmt})"
                cursor.execute(sql_reg, chunk)
                rows = cursor.fetchall()
                for (register_time,) in rows:
                    if register_time:
                        reg_dt = datetime.datetime.fromtimestamp(register_time)
                        reg_month = reg_dt.strftime("%Y-%m")
                        if reg_month in reg_dist:
                            reg_dist[reg_month] += 1
                        else:
                            reg_dist['Earlier'] += 1

        analysis.append({
            'month': month_str,
            'userCount': user_count,
            'regDist': reg_dist
        })

    return analysis
