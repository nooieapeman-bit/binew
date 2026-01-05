import datetime
import calendar
from config import VALID_PRODUCTS

def get_business_matrix(cursor):
    print("Calculating Business Performance Matrix (Direct)...")
    matrix_data = []
    
    # Pre-fetch user registration times could be heavy if we fetch ALL users.
    # Better to fetch users involved in orders.
    
    for month_idx in range(1, 13):
        # Time Window for ORDERS (Revenue Timing)
        first_day = datetime.datetime(2025, month_idx, 1)
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        last_day_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        start_ts = int(first_day.timestamp())
        end_ts = int(last_day_dt.timestamp())
        month_str = f"2025-{month_idx:02d}"
        
        # Fetch Direct Paid Orders in this month
        # Criteria: amount > 0, status=1, partner_code='', VALID_PRODUCT
        # We need distinct UIDs and their earliest pay_time IN THIS MONTH (if multiple orders, just count user once for the month)
        # Actually, for "Registration Distribution of Purchasers", if a user buys twice in Jan, they are one "Purchaser".
        # We need their register_time.
        
        sql = f"""
            SELECT o.uid, u.register_time, MIN(o.pay_time)
            FROM `order` o
            JOIN `user` u ON o.uid = u.uid
            WHERE o.pay_time >= %s AND o.pay_time <= %s
            AND o.amount > 0 AND o.status = 1
            AND (o.partner_code = '' OR o.partner_code IS NULL)
            AND o.product_name IN %s
            GROUP BY o.uid
        """
        
        cursor.execute(sql, (start_ts, end_ts, VALID_PRODUCTS))
        rows = cursor.fetchall()
        
        total_buyers = len(rows)
        
        # Initialize buckets
        buckets = {i: 0 for i in range(1, 13)}
        bucket_gt_12 = 0
        
        for r in rows:
            uid = r[0]
            reg_time = r[1]
            pay_time = r[2] # Earliest pay time in this month
            
            if not reg_time:
                continue
                
            diff_seconds = pay_time - reg_time
            diff_days = diff_seconds / 86400.0
            
            # Month bucket logic (approx 30 days per month)
            if diff_days <= 0:
                bucket_idx = 1 # Immediate purchase
            else:
                import math
                bucket_idx = math.ceil(diff_days / 30)
                if bucket_idx == 0: bucket_idx = 1
            
            if bucket_idx <= 12:
                buckets[bucket_idx] += 1
            else:
                bucket_gt_12 += 1
                
        # Aggregate buckets
        lag_1m = buckets[1]
        lag_2_4m = buckets[2] + buckets[3] + buckets[4]
        lag_5_7m = buckets[5] + buckets[6] + buckets[7]
        lag_8_12m = buckets[8] + buckets[9] + buckets[10] + buckets[11] + buckets[12]
        lag_gt_12m = bucket_gt_12
        
        row_data = {
            'month': month_str,
            'totalDirectBuyers': total_buyers,
            'lag1m': lag_1m,
            'lag1mPct': round(lag_1m / total_buyers * 100, 2) if total_buyers > 0 else 0,
            'lag2to4m': lag_2_4m,
            'lag2to4mPct': round(lag_2_4m / total_buyers * 100, 2) if total_buyers > 0 else 0,
            'lag5to7m': lag_5_7m,
            'lag5to7mPct': round(lag_5_7m / total_buyers * 100, 2) if total_buyers > 0 else 0,
            'lag8to12m': lag_8_12m,
            'lag8to12mPct': round(lag_8_12m / total_buyers * 100, 2) if total_buyers > 0 else 0,
            'lagGt12m': lag_gt_12m,
            'lagGt12mPct': round(lag_gt_12m / total_buyers * 100, 2) if total_buyers > 0 else 0
        }
        
        matrix_data.append(row_data)
        
    return matrix_data
