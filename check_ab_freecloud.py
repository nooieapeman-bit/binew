
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
            # 1. Fetch AB test participants
            print('Fetching AB test participants...')
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
                
            group_a_uids = [] # No Platinum
            group_b_uids = [] # Has Platinum
            
            for uid, codes in user_codes.items():
                if codes.isdisjoint(platinum_codes):
                    group_a_uids.append(uid)
                else:
                    group_b_uids.append(uid)
            
            print(f'Group A Size: {len(group_a_uids)}')
            print(f'Group B Size: {len(group_b_uids)}')
            
            chunk_size = 1000
            
            def count_freecloud(uids):
                if not uids: return 0
                total_count = 0
                for i in range(0, len(uids), chunk_size):
                    chunk = uids[i:i+chunk_size]
                    placeholders = ', '.join(['%s'] * len(chunk))
                    
                    sql = f"""
                        SELECT COUNT(DISTINCT uid) 
                        FROM `order`
                        WHERE uid IN ({placeholders})
                          AND status = 1
                          AND amount = 0
                          AND description = 'FreeCloud: 99 Year'
                    """
                    cur.execute(sql, chunk)
                    total_count += cur.fetchone()[0]
                return total_count

            print('\nChecking Group A for "FreeCloud: 99 Year"...')
            cnt_a = count_freecloud(group_a_uids)
            print(f'Group A count: {cnt_a}')

            print('\nChecking Group B for "FreeCloud: 99 Year"...')
            cnt_b = count_freecloud(group_b_uids)
            print(f'Group B count: {cnt_b}')
            
    finally:
        conn.close()
