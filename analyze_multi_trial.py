
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
            # 1. Fetch Group A UIDs
            print('Fetching Group A users...')
            cur.execute(f"""
                SELECT u.uid, sc.set_meal_code
                FROM user u
                JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """)
            user_codes = {}
            for row in cur.fetchall():
                uid, code = row
                if uid not in user_codes: user_codes[uid] = set()
                user_codes[uid].add(code)
            
            group_a_uids = []
            for uid, codes in user_codes.items():
                if codes.isdisjoint(platinum_codes):
                    group_a_uids.append(uid)
            
            multi_trial_users = 0
            total_orders = 0
            chunk_size = 1000
            
            print(f'Group A Users: {len(group_a_uids)}')
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # Check for multiple orders per user
                sql = f"""
                    SELECT uid, COUNT(*) 
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND status = 1
                      AND amount = 0
                    GROUP BY uid
                    HAVING COUNT(*) > 1
                """
                cur.execute(sql, chunk)
                rows = cur.fetchall()
                for row in rows:
                    multi_trial_users += 1
                    total_orders += row[1]
                    # print(f'User {row[0]} has {row[1]} trial orders')
            
            print(f'Users with >1 Trial Order: {multi_trial_users}')
            print(f'Total extra orders from these users: {total_orders - multi_trial_users}')
            
            # Also calculate total orders for context
            total_all_orders = 0
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                sql_sum = f"""
                    SELECT COUNT(*) 
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND status = 1
                      AND amount = 0
                """
                cur.execute(sql_sum, chunk)
                total_all_orders += cur.fetchone()[0]

            print(f'Total Trial Orders (Status=1, Amt=0): {total_all_orders}')

    finally:
        conn.close()
