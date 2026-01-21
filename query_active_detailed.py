import datetime
import calendar
from config import VALID_PRODUCTS, get_plan_type

def get_detailed_active_subscriptions(cursor):
    print("Analyzing Detailed Active Subscriptions (Month-End)...")
    detailed_data = []

    for month_idx in range(1, 13):
        try:
            # Snapshot time: Last second of the month
            last_day_val = calendar.monthrange(2025, month_idx)[1]
            snapshot_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
            snapshot_ts = int(snapshot_dt.timestamp())
            month_str = f"2025-{month_idx:02d}"
            
            # 1. Get ALL Active Cloud Subs at this snapshot
            sql_active = """
                SELECT uid, order_id, start_time, end_time
                FROM cloud_info
                WHERE start_time <= %s AND end_time > %s
                  AND is_delete = 0
            """
            cursor.execute(sql_active, (snapshot_ts, snapshot_ts))
            rows = cursor.fetchall()
            
            total_active_subs_count = len(rows)
            
            if total_active_subs_count == 0:
                detailed_data.append({
                    'month': month_str,
                    'totalActiveSubs': 0,
                    'paidActiveSubs': 0,
                    'users1': 0,
                    'users2': 0,
                    'usersMany': 0,
                    'monthlyCount': 0,
                    'monthlyPct': 0,
                    'yearlyCount': 0,
                    'yearlyPct': 0
                })
                continue

            # 2. Link with Order table to check if PAID (amount != 0)
            order_ids = set()
            for r in rows:
                oid = r[1]
                if oid:
                    order_ids.add(oid)
            
            order_info_map = {} # order_id -> (amount, product_name)
            if order_ids:
                order_id_list = list(order_ids)
                chunk_size = 5000
                for i in range(0, len(order_id_list), chunk_size):
                    chunk = order_id_list[i:i+chunk_size]
                    fmt = ','.join(['%s'] * len(chunk))
                    sql_orders = f"SELECT order_id, amount, product_name FROM `order` WHERE order_id IN ({fmt})"
                    cursor.execute(sql_orders, chunk)
                    o_rows = cursor.fetchall()
                    for o_row in o_rows:
                        oid_key = o_row[0]
                        if isinstance(oid_key, bytes):
                            oid_key = oid_key.decode('utf-8')
                        order_info_map[oid_key] = (o_row[1], o_row[2])

            # Filter rows into PAID rows
            paid_rows = []
            for r in rows:
                oid = r[1]
                is_paid = False
                if oid:
                    if isinstance(oid, bytes):
                        oid = oid.decode('utf-8')
                    info = order_info_map.get(oid)
                    if info and info[0] > 0: # amount > 0
                        is_paid = True
                
                if is_paid:
                    paid_rows.append(r)
            
            total_paid_active_subs = len(paid_rows)

            # 3. User Breakdown (By PAID Active Subs)
            user_paid_sub_counts = {}
            for pr in paid_rows:
                uid = pr[0]
                user_paid_sub_counts[uid] = user_paid_sub_counts.get(uid, 0) + 1
            
            users_1 = 0
            users_2 = 0
            users_many = 0
            for count in user_paid_sub_counts.values():
                if count == 1:
                    users_1 += 1
                elif count == 2:
                    users_2 += 1
                else:
                    users_many += 1

            # 4. Plan Distribution (By PAID Active Subs)
            monthly_count = 0
            yearly_count = 0
            for pr in paid_rows:
                oid = pr[1]
                start = pr[2]
                end = pr[3]
                
                plan_found = False
                if oid:
                    if isinstance(oid, bytes):
                        oid = oid.decode('utf-8')
                    info = order_info_map.get(oid)
                    if info and info[1]: # product_name
                        p_type = get_plan_type(info[1])
                        if p_type == 'Yearly':
                            yearly_count += 1
                        else:
                            monthly_count += 1
                        plan_found = True
                
                if not plan_found:
                    duration_days = (end - start) / 86400
                    if duration_days > 300:
                        yearly_count += 1
                    else:
                        monthly_count += 1

            monthly_pct = round(monthly_count / total_paid_active_subs * 100, 2) if total_paid_active_subs > 0 else 0
            yearly_pct = round(yearly_count / total_paid_active_subs * 100, 2) if total_paid_active_subs > 0 else 0

            detailed_data.append({
                'month': month_str,
                'totalActiveSubs': total_active_subs_count,
                'paidActiveSubs': total_paid_active_subs,
                'users1': users_1,
                'users2': users_2,
                'usersMany': users_many,
                'monthlyCount': monthly_count,
                'monthlyPct': monthly_pct,
                'yearlyCount': yearly_count,
                'yearlyPct': yearly_pct
            })
        except Exception as e:
            print(f"Error processing month {month_idx}: {e}")
            detailed_data.append({'month': f"2025-{month_idx:02d}", 'totalActiveSubs': 0, 'paidActiveSubs': 0, 'users1': 0, 'users2': 0, 'usersMany': 0, 'monthlyCount': 0, 'monthlyPct': 0, 'yearlyCount': 0, 'yearlyPct': 0})

    return detailed_data
