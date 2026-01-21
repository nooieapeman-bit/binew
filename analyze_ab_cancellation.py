
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Timestamps (UTC)
start_dt = datetime.datetime(2026, 1, 6, 2, 0, 0, tzinfo=datetime.timezone.utc)
end_dt = datetime.datetime(2026, 1, 13, 2, 0, 0, tzinfo=datetime.timezone.utc)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

platinum_codes = set([
    'c22f95e0eb3856e083ab265a97b5be9f', 
    '50e5b771de60f1816e964a7ef097f120'
])

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Fetch Users
            print('Fetching all user IDs in range...')
            cur.execute(f"SELECT uid FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            all_uids_set = set([r[0] for r in cur.fetchall()])
            print(f'Total Users: {len(all_uids_set)}')

            # 2. Fetch AB test participants
            print('Fetching AB test participants...')
            sql_users = f"""
                SELECT u.uid, sc.set_meal_code
                FROM user u
                JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """
            cur.execute(sql_users)
            
            user_codes = {}
            ab_participants = set()
            for row in cur.fetchall():
                uid = row[0]
                code = row[1]
                ab_participants.add(uid)
                if uid not in user_codes: user_codes[uid] = set()
                user_codes[uid].add(code)
                
            group_a_uids = [] # No Platinum
            group_b_uids = [] # Has Platinum
            
            for uid, codes in user_codes.items():
                if codes.isdisjoint(platinum_codes):
                    group_a_uids.append(uid)
                else:
                    group_b_uids.append(uid)
            
            control_uids = list(all_uids_set - ab_participants)
            
            print(f'Group A: {len(group_a_uids)}')
            print(f'Group B: {len(group_b_uids)}')
            print(f'Control: {len(control_uids)}')

            chunk_size = 1000
            
            def analyze_cancellation(uids, group_name):
                if not uids: return
                
                # 1. Find Trial Orders -> Get Subscribe IDs
                # We need to link UID -> Order -> Subscribe ID
                trial_sub_map = {} # sub_id -> uid
                
                for i in range(0, len(uids), chunk_size):
                    chunk = uids[i:i+chunk_size]
                    placeholders = ', '.join(['%s'] * len(chunk))
                    
                    # Get subscribe_id for valid trial orders
                    sql_orders = f"""
                        SELECT DISTINCT uid, subscribe_id
                        FROM `order`
                        WHERE uid IN ({placeholders})
                          AND status = 1
                          AND amount = 0
                          AND subscribe_id != ''
                    """
                    cur.execute(sql_orders, chunk)
                    for row in cur.fetchall():
                        trial_sub_map[row[1]] = row[0]
                
                total_trial_subs = len(trial_sub_map)
                if total_trial_subs == 0:
                    print(f'\n--- {group_name} ---')
                    print("No trial subscriptions found.")
                    return

                # 2. Check Cancellation Status in Subscribe Table
                # Strategy: Fetch all records for these sub_ids, process in python to find latest status
                
                all_sub_ids = list(trial_sub_map.keys())
                sub_status_map = {} # sub_id -> {'cancel_time': val, 'id': val}
                
                for i in range(0, len(all_sub_ids), chunk_size):
                    chunk = all_sub_ids[i:i+chunk_size]
                    placeholders = ', '.join(['%s'] * len(chunk))
                    
                    sql_subs = f"""
                        SELECT id, subscribe_id, cancel_time
                        FROM subscribe
                        WHERE subscribe_id IN ({placeholders})
                    """
                    cur.execute(sql_subs, chunk)
                    
                    for row in cur.fetchall():
                        rid, sid, ctime = row
                        
                        # Logic: We want the status of the LATEST record (by ID)
                        if sid not in sub_status_map:
                            sub_status_map[sid] = {'id': rid, 'cancel_time': ctime}
                        else:
                            if rid > sub_status_map[sid]['id']:
                                sub_status_map[sid] = {'id': rid, 'cancel_time': ctime}
                
                active_count = 0
                cancelled_count = 0
                
                for sid, info in sub_status_map.items():
                    # Check cancel_time
                    # If cancel_time > 0, it is cancelled.
                    # Usually 0 or None means active
                    ctime = info['cancel_time']
                    if ctime and ctime > 0:
                        cancelled_count += 1
                    else:
                        active_count += 1
                        
                print(f'\n--- {group_name} ---')
                print(f'Total Trial Subs: {total_trial_subs}')
                print(f'Active (No Cancel Time): {active_count} ({(active_count/total_trial_subs*100):.2f}%)')
                print(f'Cancelled (Has Cancel Time): {cancelled_count} ({(cancelled_count/total_trial_subs*100):.2f}%)')

            analyze_cancellation(group_a_uids, "Group A")
            analyze_cancellation(group_b_uids, "Group B")
            analyze_cancellation(control_uids, "Control Group")

    finally:
        conn.close()
