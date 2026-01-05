import datetime
import calendar
from config import VALID_PRODUCTS

def get_direct_buyer_history(cursor):
    print("Analyzing Direct Buyer History (First Period & Direct Only)...")
    history_data = []
    
    for month_idx in range(1, 13):
        # Time Window for CURRENT Month Orders
        first_day = datetime.datetime(2025, month_idx, 1)
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        last_day_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        start_ts = int(first_day.timestamp())
        end_ts = int(last_day_dt.timestamp())
        month_str = f"2025-{month_idx:02d}"
        
        # 1. Identify FIRST PERIOD Direct Buyers in this month
        # Criteria:
        # - Min(pay_time) for the subscription falls in this month.
        # - Direct: partner_code is empty/null.
        # - Paid: amount > 0.
        # - Logic: Group by subscribe_id.
        
        sql_first_period = f"""
            SELECT subscribe_id, MIN(pay_time), uid
            FROM `order`
            WHERE amount > 0 AND status = 1
              AND product_name IN %s
            GROUP BY subscribe_id
            HAVING MIN(pay_time) >= %s AND MIN(pay_time) <= %s
        """
        cursor.execute(sql_first_period, (VALID_PRODUCTS, start_ts, end_ts))
        # rows: (subscribe_id, first_pay_time, uid)
        fp_rows = cursor.fetchall()
        
        # Filter out those who came from TRIAL
        # i.e., check if this subscribe_id has a prior order with amount=0
        
        final_direct_uids = []
        uid_paytime_map = {} # For active sub check: uid -> pay_time
        
        if fp_rows:
            all_subs = [r[0] for r in fp_rows]
            sub_uid_map = {r[0]: (r[2], r[1]) for r in fp_rows} # sub_id -> (uid, pay_time)
            
            chunk_size = 2000
            for i in range(0, len(all_subs), chunk_size):
                chunk_subs = all_subs[i:i+chunk_size]
                if not chunk_subs: continue
                
                fmt = ','.join(['%s'] * len(chunk_subs))
                
                # Find subs with PRIOR TRIAL
                # Logic: has order with same subscribe_id, amount=0, status=1, pay_time < current_first_pay ??
                # Actually, simply: count valid trial orders for these subscribe_ids. 
                # If a sub has ANY trial order, it's a "Trial Conversion", not "Direct First Period".
                # (Assuming Trial always precedes Payment or happens same chain).
                
                sql_check_trial = f"""
                    SELECT DISTINCT subscribe_id
                    FROM `order`
                    WHERE subscribe_id IN ({fmt})
                      AND amount = 0 AND status = 1
                """
                cursor.execute(sql_check_trial, chunk_subs)
                trial_subs = {r[0] for r in cursor.fetchall()}
                
                # Keep only those NOT in trial_subs
                for s_id in chunk_subs:
                    if s_id not in trial_subs:
                        u, pt = sub_uid_map[s_id]
                        final_direct_uids.append(u)
                        # Identify strict "purchase time" for this user
                        # (If user bought multiple subs same month, pick one? We just need ANY ref time)
                        uid_paytime_map[u] = pt

        # Distinct UIDs (in case user started 2 direct subs in same month)
        final_direct_uids = list(set(final_direct_uids))
        
        total_buyers = len(final_direct_uids)
        existing_user_count = 0
        active_sub_user_count = 0
        
        if total_buyers > 0:
            chunk_size = 2000
            for i in range(0, total_buyers, chunk_size):
                chunk_uids = final_direct_uids[i:i+chunk_size]
                if not chunk_uids: continue
                
                fmt = ','.join(['%s'] * len(chunk_uids))
                
                # 2. Check for PRIOR valid paid orders (Identify Existing Users)
                # STRICTLY BEFORE start_ts of the month? Or before the specific order time?
                # "Prior Purchase" usually means before this month's cohort event. 
                # Let's clean it by checking < start_ts (Before this Month).
                
                sql_history = f"""
                    SELECT DISTINCT uid
                    FROM `order`
                    WHERE uid IN ({fmt})
                      AND pay_time < %s
                      AND amount > 0 AND status = 1
                      AND product_name IN %s
                """
                args = list(chunk_uids) + [start_ts, VALID_PRODUCTS]
                cursor.execute(sql_history, args)
                existing_batch = {r[0] for r in cursor.fetchall()}
                
                existing_user_count += len(existing_batch)
                
                # 3. For Existing Users, check active subscription in cloud_info
                # Logic: start_time < (pay_time - 600) < end_time AND is_delete = 0
                if existing_batch:
                    existing_uids_list = list(existing_batch)
                    fmt_exist = ','.join(['%s'] * len(existing_uids_list))
                    
                    sql_cloud = f"""
                        SELECT uid, start_time, end_time
                        FROM cloud_info
                        WHERE uid IN ({fmt_exist})
                          AND is_delete = 0
                    """
                    cursor.execute(sql_cloud, existing_uids_list)
                    cloud_rows = cursor.fetchall()
                    
                    user_cloud_map = {}
                    for cr in cloud_rows:
                        c_uid, c_start, c_end = cr
                        if c_uid not in user_cloud_map:
                            user_cloud_map[c_uid] = []
                        user_cloud_map[c_uid].append((c_start, c_end))
                    
                    active_uids = set()
                    for eu in existing_uids_list:
                        ref_time = uid_paytime_map[eu]
                        check_time = ref_time - 600 # 10 minutes before
                        
                        if eu in user_cloud_map:
                            for c_start, c_end in user_cloud_map[eu]:
                                if c_start < check_time < c_end:
                                    active_uids.add(eu)
                                    break
                    
                    active_sub_user_count += len(active_uids)

        history_data.append({
            'month': month_str,
            'totalDirectBuyers': total_buyers,
            'existingUsers': existing_user_count,
            'existingUsersPct': round(existing_user_count / total_buyers * 100, 2) if total_buyers > 0 else 0,
            'activeSubUsers': active_sub_user_count,
            'activeSubUsersPct': round(active_sub_user_count / existing_user_count * 100, 2) if existing_user_count > 0 else 0
        })
        
    return history_data
