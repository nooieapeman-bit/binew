import datetime
import calendar
from config import VALID_PRODUCTS, get_plan_type

def get_active_retention_analysis(cursor):
    print("Analyzing PAID Active Subscription Retention & Renewal with Plan and New User Breakdown...")
    retention_data = []

    # Helper to get PAID active set at a timestamp with plan type
    def get_paid_active_subs_at(ts):
        sql = """
            SELECT ci.uid, ci.uuid, ci.end_time, o.product_name, ci.start_time
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
            # If product_name is missing, fallback to duration
            if not r[3]:
                duration = (r[2] - r[4]) / 86400
                plan = 'Yearly' if duration > 300 else 'Monthly'
            result.append({'uid': r[0], 'uuid': r[1], 'end_time': r[2], 'plan': plan})
        return result

    # Helper to get all PAID starts in a month with plan type and order_id/subscribe_id info
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
        
        # Deduplicate by (uid, uuid), keeping the one with max start_time
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

    # Initialise with Dec 2024
    dec_2024_end_dt = datetime.datetime(2024, 12, 31, 23, 59, 59)
    dec_2024_end_ts = int(dec_2024_end_dt.timestamp())
    prev_active_list = get_paid_active_subs_at(dec_2024_end_ts)

    for month_idx in range(1, 13):
        month_start_dt = datetime.datetime(2025, month_idx, 1)
        start_ts = int(month_start_dt.timestamp())
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        month_end_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        end_ts = int(month_end_dt.timestamp())
        month_str = f"2025-{month_idx:02d}"

        # 1. Total PAID Active Subs (End of Month)
        current_active_list = get_paid_active_subs_at(end_ts)
        total_active_end = len(current_active_list)

        # 2. Categorize Prev Active: No Need Renew vs Expected Renew
        no_need_m = 0
        no_need_y = 0
        exp_m_keys = set()
        exp_y_keys = set()
        
        for item in prev_active_list:
            if start_ts <= item['end_time'] <= end_ts:
                # Needs renewal
                if item['plan'] == 'Yearly':
                    exp_y_keys.add((item['uid'], item['uuid']))
                else:
                    exp_m_keys.add((item['uid'], item['uuid']))
            elif item['end_time'] > end_ts:
                # Carried over
                if item['plan'] == 'Yearly':
                    no_need_y += 1
                else:
                    no_need_m += 1

        # 3. Actual Renewals
        month_starts = get_detailed_paid_starts_in_month(start_ts, end_ts)
        
        # Separate starts for renewal check
        renewed_keys_set = set() # (uid, uuid) that renewed
        
        act_m_keys = set()
        act_y_keys = set()
        
        # A sub is renewed if (uid, uuid) was expected to renew and started ANY paid plan this month
        all_exp_keys = exp_m_keys.union(exp_y_keys)
        
        # Track which starts are renewals
        renewal_starts = []
        new_growth_starts = []
        
        for s in month_starts:
            key = (s['uid'], s['uuid'])
            if key in all_exp_keys:
                renewed_keys_set.add(key)
                renewal_starts.append(s)
            else:
                new_growth_starts.append(s)
        
        # Recalculate act_m, act_y based on what was *expected*
        # If an Expected Monthly sub renewed (as either M or Y), it counts as a Monthly renewal
        act_m = len(exp_m_keys.intersection(renewed_keys_set))
        act_y = len(exp_y_keys.intersection(renewed_keys_set))

        # 4. Renewal Rates
        rate_m = round(act_m / len(exp_m_keys) * 100, 2) if exp_m_keys else 0
        rate_y = round(act_y / len(exp_y_keys) * 100, 2) if exp_y_keys else 0
        rate_total = round((act_m + act_y) / (len(exp_m_keys) + len(exp_y_keys)) * 100, 2) if (exp_m_keys or exp_y_keys) else 0

        # 5. New PAID Subscriptions Breakdown (First Period vs Non-First Period)
        # Definition of "New" here is everything that wasn't a renewal from prev month active.
        # total_active_end = no_need + renewed + new_subs
        # So new_subs = total_active_end - (no_need_m + no_need_y) - len(renewed_keys_set)
        
        # However, it's better to analyze the actual new_growth_starts records.
        # But wait, some new_growth_starts might not be active at the END of the month (expired within same month).
        # snapshot current_active_list to find which "New" starts survived.
        current_active_uids_uuids = set([(item['uid'], item['uuid']) for item in current_active_list])
        
        final_new_growth_records = [s for s in new_growth_starts if (s['uid'], s['uuid']) in current_active_uids_uuids]
        
        # For these final_new_growth_records, determine if First Period.
        new_first_period = 0
        new_non_first_period = 0
        
        if final_new_growth_records:
            # Collect subscribe_ids
            sid_to_order_ids = {} # sid -> list of order_ids
            # Group by sid
            for r in final_new_growth_records:
                sid = r['subscribe_id']
                if sid:
                    if sid not in sid_to_order_ids:
                        sid_to_order_ids[sid] = []
                    sid_to_order_ids[sid].append(r['order_id'])
            
            # For each SID, find the first paid order
            if sid_to_order_ids:
                sids = list(sid_to_order_ids.keys())
                chunk_size = 1000
                first_order_map = {} # sid -> first_order_id
                
                for j in range(0, len(sids), chunk_size):
                    chunk_sids = sids[j:j+chunk_size]
                    fmt = ','.join(['%s'] * len(chunk_sids))
                    sql_first = f"""
                        SELECT subscribe_id, order_id
                        FROM (
                            SELECT subscribe_id, order_id, ROW_NUMBER() OVER(PARTITION BY subscribe_id ORDER BY pay_time ASC) as rn
                            FROM `order`
                            WHERE subscribe_id IN ({fmt}) AND amount > 0
                        ) t
                        WHERE rn = 1
                    """
                    cursor.execute(sql_first, chunk_sids)
                    f_rows = cursor.fetchall()
                    for f_row in f_rows:
                        sid_key = f_row[0]
                        if isinstance(sid_key, bytes): sid_key = sid_key.decode('utf-8')
                        oid_val = f_row[1]
                        if isinstance(oid_val, bytes): oid_val = oid_val.decode('utf-8')
                        first_order_map[sid_key] = oid_val
                
                for r in final_new_growth_records:
                    sid = r['subscribe_id']
                    if sid:
                        if first_order_map.get(sid) == r['order_id']:
                            new_first_period += 1
                        else:
                            new_non_first_period += 1
                    else:
                        # No subscribe_id (maybe single purchase/IAP?), count as First Period if it's the only one for this user?
                        # Or just count as Non-First for caution.
                        # Let's count as Non-First for consistency with "subscribe_id" logic.
                        new_non_first_period += 1
            else:
                # No subscribe_ids found in the new growth starts
                new_non_first_period = len(final_new_growth_records)

        retention_data.append({
            'month': month_str,
            'totalActiveEnd': total_active_end,
            'noNeedM': no_need_m,
            'noNeedY': no_need_y,
            'expM': len(exp_m_keys),
            'expY': len(exp_y_keys),
            'actM': act_m,
            'actY': act_y,
            'rateM': rate_m,
            'rateY': rate_y,
            'rateTotal': rate_total,
            'newSubs': len(final_new_growth_records),
            'newFirst': new_first_period,
            'newNonFirst': new_non_first_period
        })
        
        prev_active_list = current_active_list

    return retention_data
