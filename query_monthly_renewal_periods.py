import datetime
import calendar
import math
from config import VALID_PRODUCTS, get_plan_type

def get_monthly_renewal_periods(cursor):
    print("Analyzing Monthly Renewal Period Distribution...")
    
    # Get all potential SIDs for the year 2025 to cache initial_payment_time
    print("Caching initial_payment_times for 2025...")
    sql_all_sids = """
        SELECT DISTINCT o.subscribe_id
        FROM cloud_info ci
        JOIN `order` o ON ci.order_id = o.order_id
        WHERE ci.start_time <= 1767225599 AND ci.end_time >= 1735689600
          AND ci.is_delete = 0
          AND o.amount > 0
    """
    cursor.execute(sql_all_sids)
    all_year_sids = [r[0] for r in cursor.fetchall() if r[0]]
    sid_to_initial = {}
    if all_year_sids:
        for j in range(0, len(all_year_sids), 1000):
            chunk = all_year_sids[j:j+1000]
            fmt = ','.join(['%s'] * len(chunk))
            sql_init = f"SELECT subscribe_id, initial_payment_time FROM subscribe WHERE subscribe_id IN ({fmt})"
            cursor.execute(sql_init, chunk)
            for sid, init_time in cursor.fetchall():
                if isinstance(sid, bytes): sid = sid.decode('utf-8')
                sid_to_initial[sid] = init_time

    # Helper to get PAID active set at a timestamp with plan type
    def get_paid_active_subs_at(ts):
        sql = """
            SELECT ci.uid, ci.uuid, ci.end_time, o.product_name, ci.start_time, o.subscribe_id
            FROM cloud_info ci
            JOIN `order` o ON ci.order_id = o.order_id
            WHERE ci.start_time <= %s AND ci.end_time > %s
              AND ci.is_delete = 0
              AND o.amount > 0
        """
        cursor.execute(sql, (ts, ts))
        rows = cursor.fetchall()
        result = []
        for r in rows:
            plan = get_plan_type(r[3] if r[3] else "")
            if not r[3]:
                duration = (r[2] - r[4]) / 86400
                plan = 'Yearly' if duration > 300 else 'Monthly'
            
            sid = r[5]
            if isinstance(sid, bytes): sid = sid.decode('utf-8')
            result.append({
                'uid': r[0], 'uuid': r[1], 'end_time': r[2], 
                'plan': plan, 'start_time': r[4], 'subscribe_id': sid
            })
        return result

    # Helper to get all PAID starts in a month
    def get_detailed_paid_starts_in_month(start_ts, end_ts):
        sql = """
            SELECT ci.uid, ci.uuid, ci.start_time, o.product_name, ci.end_time, ci.order_id, o.subscribe_id
            FROM cloud_info ci
            JOIN `order` o ON ci.order_id = o.order_id
            WHERE ci.start_time >= %s AND ci.start_time <= %s
              AND ci.end_time > %s
              AND ci.is_delete = 0
              AND o.amount > 0
        """
        cursor.execute(sql, (start_ts, end_ts, end_ts))
        rows = cursor.fetchall()
        
        latest_map = {}
        for r in rows:
            uid, uuid, start_time = r[0], r[1], r[2]
            key = (uid, uuid)
            if key not in latest_map or start_time > latest_map[key]['start_time']:
                plan = get_plan_type(r[3] if r[3] else "")
                if not r[3]:
                    duration = (r[4] - r[2]) / 86400
                    plan = 'Yearly' if duration > 300 else 'Monthly'
                
                oid_key = r[5]
                if isinstance(oid_key, bytes): oid_key = oid_key.decode('utf-8')
                sid_key = r[6]
                if isinstance(sid_key, bytes): sid_key = sid_key.decode('utf-8')

                latest_map[key] = {
                    'uid': uid, 
                    'uuid': uuid, 
                    'start_time': start_time, 
                    'end_time': r[4],
                    'plan': plan, 
                    'order_id': oid_key, 
                    'subscribe_id': sid_key
                }
        return list(latest_map.values())

    period_analysis = []
    
    # Initialise with Dec 2024
    dec_2024_end_dt = datetime.datetime(2024, 12, 31, 23, 59, 59)
    dec_2024_end_ts = int(dec_2024_end_dt.timestamp())
    prev_active_list = get_paid_active_subs_at(dec_2024_end_ts)

    for month_idx in range(1, 13):
        # ... (rest of the month loop)
        month_start_dt = datetime.datetime(2025, month_idx, 1)
        start_ts = int(month_start_dt.timestamp())
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        month_end_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        end_ts = int(month_end_dt.timestamp())
        month_str = f"2025-{month_idx:02d}"

        # Expected Monthly Renewals
        expected_renewals = []
        exp_keys_set = set()
        for item in prev_active_list:
            if start_ts <= item['end_time'] <= end_ts and item['plan'] == 'Monthly':
                expected_renewals.append(item)
                exp_keys_set.add((item['uid'], item['uuid']))
        
        # Actual Starts in this month
        month_starts = get_detailed_paid_starts_in_month(start_ts, end_ts)
        
        # Identify Actual Monthly Renewals
        actual_renewals = []
        for s in month_starts:
            key = (s['uid'], s['uuid'])
            if key in exp_keys_set:
                actual_renewals.append(s)
        
        # Distribution logic
        keys = ['P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'P10', 'P11', 'P12_plus']
        exp_bins = {k: 0 for k in keys}
        act_bins = {k: 0 for k in keys}

        def get_bin(start_time, init_time):
            diff_sec = start_time - init_time
            if diff_sec < 0: diff_sec = 0
            period = math.floor(diff_sec / (30 * 86400)) + 1
            if period <= 1: return 'P1'
            elif period == 2: return 'P2'
            elif period == 3: return 'P3'
            elif period == 4: return 'P4'
            elif period == 5: return 'P5'
            elif period == 6: return 'P6'
            elif period == 7: return 'P7'
            elif period == 8: return 'P8'
            elif period == 9: return 'P9'
            elif period == 10: return 'P10'
            elif period == 11: return 'P11'
            return 'P12_plus'

        # Map actual renewals to a set for fast lookup
        renewed_keys = set()
        for r in actual_renewals:
            renewed_keys.add((r['uid'], r['uuid']))

        for r in expected_renewals:
            init = sid_to_initial.get(r['subscribe_id'])
            if init:
                b = get_bin(r['start_time'], init)
                exp_bins[b] += 1
                
                # If this specific expected sub actually renewed
                if (r['uid'], r['uuid']) in renewed_keys:
                    act_bins[b] += 1

        period_analysis.append({
            'month': month_str,
            'expM': len(expected_renewals),
            'actM': len(actual_renewals),
            'rateM': round(len(actual_renewals) / len(expected_renewals) * 100, 2) if expected_renewals else 0,
            'exp_bins': exp_bins,
            'act_bins': act_bins
        })
        
        # Update prev_active for next month
        prev_active_list = get_paid_active_subs_at(end_ts)

    return period_analysis
