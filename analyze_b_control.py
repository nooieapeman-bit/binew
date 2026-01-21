
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
            # 1. Fetch ALL users in time range
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
                
            # 3. Classify Groups
            group_b_uids = [] # Has Platinum
            
            for uid, codes in user_codes.items():
                if not codes.isdisjoint(platinum_codes):
                    group_b_uids.append(uid)
            
            # Control Group = All Users - AB Participants
            control_uids = list(all_uids_set - ab_participants)
            
            print(f'Group B Size: {len(group_b_uids)}')
            print(f'Control Group Size: {len(control_uids)}')

            chunk_size = 1000
            
            def query_uids(uids, status_val, check_desc=False):
                if not uids: return 0
                total_count = 0
                for i in range(0, len(uids), chunk_size):
                    chunk = uids[i:i+chunk_size]
                    placeholders = ', '.join(['%s'] * len(chunk))
                    
                    if check_desc:
                        sql = f"""
                            SELECT COUNT(DISTINCT uid) 
                            FROM `order`
                            WHERE uid IN ({placeholders})
                              AND status = {status_val}
                              AND amount = 0
                              AND description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                        """
                    else:
                        sql = f"""
                            SELECT COUNT(DISTINCT uid) 
                            FROM `order`
                            WHERE uid IN ({placeholders})
                              AND status = {status_val}
                              AND amount = 0
                        """
                    cur.execute(sql, chunk)
                    total_count += cur.fetchone()[0]
                return total_count

            # Analyze Group B
            print('\n--- Group B Analysis ---')
            gb_s1_desc = query_uids(group_b_uids, 1, True)
            gb_s1_any = query_uids(group_b_uids, 1, False)
            print(f'Metric 1 [Desc match] (Status=1): {gb_s1_desc}')
            print(f'Metric 2 [Any 0 order] (Status=1): {gb_s1_any}')

            # Analyze Control Group
            print('\n--- Control Group Analysis ---')
            gc_s1_desc = query_uids(control_uids, 1, True)
            gc_s1_any = query_uids(control_uids, 1, False)
            print(f'Metric 1 [Desc match] (Status=1): {gc_s1_desc}')
            print(f'Metric 2 [Any 0 order] (Status=1): {gc_s1_any}')
            
    finally:
        conn.close()
