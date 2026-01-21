
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
            # 1. Fetch Users with Reg Time
            print('Fetching user registration times...')
            cur.execute(f"SELECT uid, register_time FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            user_reg_map = {r[0]: r[1] for r in cur.fetchall()}
            total_users = len(user_reg_map)
            print(f'Total Users: {total_users}')

            # 2. Fetch AB test participants
            print('Fetching AB test participants...')
            sql_ab = f"""
                SELECT DISTINCT u.uid, sc.set_meal_code
                FROM user u
                JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """
            cur.execute(sql_ab)
            
            user_codes = {}
            for row in cur.fetchall():
                uid = row[0]
                code = row[1]
                if uid not in user_codes: user_codes[uid] = set()
                user_codes[uid].add(code)
                
            group_a_uids = [] # No Platinum
            group_b_uids = [] # Has Platinum
            
            for uid, codes in user_codes.items():
                if codes.isdisjoint(platinum_codes):
                    group_a_uids.append(uid)
                else:
                    group_b_uids.append(uid)
            
            # Control Group
            ab_uids_set = set(user_codes.keys())
            control_uids = [u for u in user_reg_map.keys() if u not in ab_uids_set]
            
            print(f'Group A: {len(group_a_uids)}')
            print(f'Group B: {len(group_b_uids)}')
            print(f'Control: {len(control_uids)}')
            
            chunk_size = 1000
            
            def analyze_timeframes(uids, group_name):
                if not uids: return 
                
                # We need to find the FIRST qualified trial order for each user
                # Qualified: status=1, amount=0. For control, we exclude FreeCloud99 if desired, but request says "开通试用的人"
                # To be consistent with previous steps, let's stick to strict Trial definition for AB, and generic for Control?
                # Actually, standardizing on "status=1 AND amount=0 AND description in ('Trial...', 'Promo...')" is safest for comparisons unless control specifically needs broader.
                # However, previous steps showed Control has extra 'FreeCloud'. Let's include ONLY 'Trial/Promo' for fair comparison, OR just strict amount=0.
                # Let's use strict: status=1, amount=0.
                
                within_24h = 0
                within_60h = 0
                total_trial_users = 0
                
                # Fetch min pay_time for trial orders for these users
                for i in range(0, len(uids), chunk_size):
                    chunk = uids[i:i+chunk_size]
                    placeholders = ', '.join(['%s'] * len(chunk))
                    
                    sql = f"""
                        SELECT uid, MIN(pay_time)
                        FROM `order`
                        WHERE uid IN ({placeholders})
                          AND status = 1
                          AND amount = 0
                        GROUP BY uid
                    """
                    cur.execute(sql, chunk)
                    
                    for row in cur.fetchall():
                        uid = row[0]
                        pay_time = row[1] # datetime object or timestamp? pymysql usually returns datetime if column is datetime, or int if timestamp. 
                        # `order` table pay_time is int (unix timestamp) based on previous context.
                        # Wait, in fact_order migration it was converted. In raw `order` table it is int.
                        
                        reg_time = user_reg_map.get(uid)
                        
                        if pay_time and reg_time:
                            total_trial_users += 1
                            diff_hours = (pay_time - reg_time) / 3600.0
                            
                            if diff_hours <= 24:
                                within_24h += 1
                            if diff_hours <= 60:
                                within_60h += 1
                                
                print(f'\n--- {group_name} ---')
                print(f'Total Trial Users: {total_trial_users}')
                print(f'Within 24h: {within_24h} ({(within_24h/total_trial_users*100) if total_trial_users else 0:.2f}%)')
                print(f'Within 60h: {within_60h} ({(within_60h/total_trial_users*100) if total_trial_users else 0:.2f}%)')

            analyze_timeframes(group_a_uids, "Group A")
            analyze_timeframes(group_b_uids, "Group B")
            analyze_timeframes(control_uids, "Control Group")

    finally:
        conn.close()
