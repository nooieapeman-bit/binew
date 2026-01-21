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
              AND subscribe_id != ''
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
                        # Store tuple (uid, pay_time) for every valid subscription
                        # Do NOT deduplicate by UID here, as we want Subscription counts
                        final_direct_uids.append((u, pt))
                        uid_paytime_map[u] = pt # Still need strict map for active check (approx ok)

        # final_direct_uids is now list of (uid, pay_time)
        
        total_buyers = len(final_direct_uids)
        existing_user_count = 0
        active_sub_user_count = 0
        
        if total_buyers > 0:
            chunk_size = 2000
            for i in range(0, total_buyers, chunk_size):
                chunk_items = final_direct_uids[i:i+chunk_size]
                if not chunk_items: continue
                
                # Extract clean UIDs for SQL query
                chunk_uids_for_sql = list({x[0] for x in chunk_items})
                
                fmt = ','.join(['%s'] * len(chunk_uids_for_sql))
                
                # 2. Check for PRIOR valid paid orders (Identify Existing Users in Batch)
                sql_history = f"""
                    SELECT DISTINCT uid
                    FROM `order`
                    WHERE uid IN ({fmt})
                      AND pay_time < %s
                      AND amount > 0 AND status = 1
                      AND product_name IN %s
                """
                args = list(chunk_uids_for_sql) + [start_ts, VALID_PRODUCTS]
                cursor.execute(sql_history, args)
                existing_batch_set = {r[0] for r in cursor.fetchall()}
                
                # 3. For Existing Users, check active subscription
                active_batch_set = set()
                if existing_batch_set:
                    existing_uids_list = list(existing_batch_set)
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
                    
                    for eu in existing_uids_list:
                        ref_time = uid_paytime_map[eu]
                        check_time = ref_time - 600
                        if eu in user_cloud_map:
                            for c_start, c_end in user_cloud_map[eu]:
                                if c_start < check_time < c_end:
                                    active_batch_set.add(eu)
                                    break
                
                # Now calculate counts based on the Subscriptions (chunk_items)
                for u, pt in chunk_items:
                    if u in existing_batch_set:
                        existing_user_count += 1
                        if u in active_batch_set:
                            active_sub_user_count += 1

        history_data.append({
            'month': month_str,
            'totalDirectBuyers': total_buyers,
            'existingUsers': existing_user_count,
            'existingUsersPct': round(existing_user_count / total_buyers * 100, 2) if total_buyers > 0 else 0,
            'activeSubUsers': active_sub_user_count,
            'activeSubUsersPct': round(active_sub_user_count / existing_user_count * 100, 2) if existing_user_count > 0 else 0
        })
        
    return history_data
