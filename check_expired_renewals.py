import datetime
import calendar
from config import create_ssh_tunnel, get_db_connection, VALID_PRODUCTS, get_plan_type

def get_paid_active_subs_at(cursor, ts):
    sql = """
        SELECT ci.uid, ci.uuid, ci.end_time, o.product_name, ci.start_time
        FROM cloud_info ci
        JOIN `order` o ON ci.order_id = o.order_id
        WHERE ci.start_time <= %s AND ci.end_time > %s
          AND ci.is_delete = 0
          AND o.amount > 0
          AND o.product_name IN %s
    """
    cursor.execute(sql, (ts, ts, VALID_PRODUCTS))
    rows = cursor.fetchall()
    result = []
    for r in rows:
        plan = get_plan_type(r[3] if r[3] else "")
        result.append({'uid': r[0], 'uuid': r[1], 'end_time': r[2], 'plan': plan})
    return result

def get_detailed_paid_starts_in_month(cursor, start_ts, end_ts):
    sql = """
        SELECT ci.uid, ci.uuid, ci.start_time, o.product_name, ci.end_time, ci.order_id, o.subscribe_id
        FROM cloud_info ci
        JOIN `order` o ON ci.order_id = o.order_id
        WHERE ci.start_time >= %s AND ci.start_time <= %s
          AND ci.is_delete = 0
          AND o.amount > 0
          AND o.product_name IN %s
    """
    cursor.execute(sql, (start_ts, end_ts, VALID_PRODUCTS))
    rows = cursor.fetchall()
    
    latest_map = {}
    for r in rows:
        uid, uuid, start_time = r[0], r[1], r[2]
        key = (uid, uuid)
        if key not in latest_map or start_time > latest_map[key]['start_time']:
            latest_map[key] = {
                'uid': uid, 
                'uuid': uuid, 
                'start_time': start_time, 
                'end_time': r[4],
                'product_name': r[3],
                'order_id': r[5]
            }
    return list(latest_map.values())

def run():
    tunnel = create_ssh_tunnel()
    tunnel.start()
    try:
        conn = get_db_connection(tunnel.local_bind_port)
        cursor = conn.cursor()
        
        # Target: November 2025
        month_idx = 11
        month_start_dt = datetime.datetime(2025, month_idx, 1)
        start_ts = int(month_start_dt.timestamp())
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        month_end_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        end_ts = int(month_end_dt.timestamp())
        
        # Previous Month End
        prev_end_ts = start_ts - 1
        
        print(f"Analyzing Renewals for 2025-11 (Ending at {month_end_dt})...")
        
        # 1. Expected keys (Active at end of Oct)
        prev_active = get_paid_active_subs_at(cursor, prev_end_ts)
        exp_keys = set()
        for item in prev_active:
            # Only those due for renewal in Nov
            if start_ts <= item['end_time'] <= end_ts:
                exp_keys.add((item['uid'], item['uuid']))
        
        print(f"Expected to renew in Nov: {len(exp_keys)}")
        
        # 2. Starts in Nov
        month_starts = get_detailed_paid_starts_in_month(cursor, start_ts, end_ts)
        
        # 3. Filtering Renewals and checking their end_time
        renewal_count = 0
        renewed_but_already_expired = [] # Store full objects
        
        for s in month_starts:
            key = (s['uid'], s['uuid'])
            if key in exp_keys:
                renewal_count += 1
                if s['end_time'] <= end_ts:
                    renewed_but_already_expired.append(s)

        print(f"\nResults:")
        print(f"Total Renewals Found: {renewal_count}")
        print(f"Renewals whose end_time < Nov End (Already Expired): {len(renewed_but_already_expired)}")
        
        if renewed_but_already_expired:
            print("\n--- Detailed Info for Expired Renewals ---")
            for item in renewed_but_already_expired:
                st = datetime.datetime.fromtimestamp(item['start_time']).strftime('%Y-%m-%d %H:%M:%S')
                et = datetime.datetime.fromtimestamp(item['end_time']).strftime('%Y-%m-%d %H:%M:%S')
                # Determine prev end_time from exp_keys logic
                # (Need to fetch it again or store it in exp_keys)
                print(f"UID: {item['uid']}")
                print(f"  Start: {st}")
                print(f"  End:   {et}")
                print(f"  Product: {item['product_name']}")
                print(f"  Order ID: {item['order_id']}")
                print("-" * 30)

    finally:
        tunnel.stop()

if __name__ == "__main__":
    run()
