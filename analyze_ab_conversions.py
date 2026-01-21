
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
            # 1. Identify Group A UIDs
            print("Fetching Group A UIDs...")
            sql_users = f"""
                SELECT u.uid, sc.set_meal_code
                FROM user u
                JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """
            cur.execute(sql_users)
            
            user_codes = {}
            for row in cur.fetchall():
                uid = row[0]
                code = row[1]
                if uid not in user_codes: user_codes[uid] = set()
                user_codes[uid].add(code)
                
            group_a_uids = []
            for uid, codes in user_codes.items():
                if codes.isdisjoint(platinum_codes):
                    group_a_uids.append(uid)
                    
            print(f'Group A Size: {len(group_a_uids)}')
            
            if not group_a_uids:
                print('No Group A users found.')
                exit()

            # Metric 1: status=0, amount=0, specific description
            # Use batching to be safe
            
            chunk_size = 1000
            
            def query_uids(uids, status_val, check_desc=False):
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

            print('Querying Metric 1 (Desc match, status=0)...')
            m1_s0 = query_uids(group_a_uids, status_val=0, check_desc=True)
            
            print('Querying Metric 1 (Desc match, status=1)...')
            m1_s1 = query_uids(group_a_uids, status_val=1, check_desc=True)

            print('Querying Metric 2 (Any amount=0, status=0)...')
            m2_s0 = query_uids(group_a_uids, status_val=0, check_desc=False)
            
            print('Querying Metric 2 (Any amount=0, status=1)...')
            m2_s1 = query_uids(group_a_uids, status_val=1, check_desc=False)

            print('-' * 30)
            print(f'Metric 1 [Desc match] (Status=0): {m1_s0}')
            print(f'Metric 1 [Desc match] (Status=1): {m1_s1}')
            print(f'Metric 2 [Any 0 order] (Status=0): {m2_s0}')
            print(f'Metric 2 [Any 0 order] (Status=1): {m2_s1}')
    finally:
        conn.close()
