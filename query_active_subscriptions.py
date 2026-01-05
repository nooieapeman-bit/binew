import datetime
import calendar
from config import VALID_PRODUCTS

def get_active_subscriptions_data(cursor):
    print("Calculating Monthly Active Subscriptions (Valid Period)...")
    sub_data = []

    for month_idx in range(1, 13):
        # Time Window for the MONTH
        first_day = datetime.datetime(2025, month_idx, 1)
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        last_day_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        month_start_ts = int(first_day.timestamp())
        month_end_ts = int(last_day_dt.timestamp())
        month_str = f"2025-{month_idx:02d}"
        
        # Valid Subscription:
        # 1. cloud_info.is_delete = 0
        # 2. product_name IN VALID_PRODUCTS (via order join)
        # 3. Active at month end: start_time <= month_end AND end_time >= month_end
        
        sql = f"""
            SELECT COUNT(DISTINCT c.id)
            FROM cloud_info c
            JOIN `order` o ON c.order_id = o.order_id
            WHERE c.is_delete = 0
            AND c.start_time <= %s
            AND c.end_time >= %s
            AND o.product_name IN %s
        """
        
        cursor.execute(sql, (month_end_ts, month_end_ts, VALID_PRODUCTS))
        count = cursor.fetchone()[0]
        
        sub_data.append({
            'month': month_str,
            'activeSubscriptions': count
        })
        
    return sub_data
