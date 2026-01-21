
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
            
            # 2. Identify the 19 Cancelled Users/Subs
            print(f'Scanning cancellations for {len(group_a_uids)} users...')
            
            chunk_size = 1000
            cancelled_uids = set()
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # Get SubIDs for standard trials
                sql = f"""
                    SELECT DISTINCT subscribe_id, uid
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND amount = 0
                      AND status = 1
                      AND description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                      AND subscribe_id != ''
                """
                cur.execute(sql, chunk)
                rows = cur.fetchall()
                if not rows: continue
                
                sub_uid_map = {r[0]: r[1] for r in rows}
                sids = list(sub_uid_map.keys())
                
                # Check cancellation in Subscribe table
                s_ph = ', '.join(['%s'] * len(sids))
                sql_check = f"SELECT subscribe_id, MAX(cancel_time) FROM subscribe WHERE subscribe_id IN ({s_ph}) GROUP BY subscribe_id"
                cur.execute(sql_check, sids)
                
                for r in cur.fetchall():
                    sid = r[0]
                    ctime = r[1]
                    if ctime and ctime > 0:
                        cancelled_uids.add(sub_uid_map[sid])
            
            print(f'Total Cancelled Users (Standard): {len(cancelled_uids)}')
            
            if not cancelled_uids:
                exit()
            
            # 3. Check for PAID orders (amount > 0, status = 1) for these specific users
            # Logic: Did they have ANY valid paid order, potentially after the trial?
            # Or just any paid order at all (maybe upgrade).
            
            print('Checking for paid conversions among cancelled users...')
            
            paid_users = []
            c_list = list(cancelled_uids)
            placeholders = ', '.join(['%s'] * len(c_list))
            
            sql_paid = f"""
                SELECT DISTINCT uid, amount, pay_time, product_name
                FROM `order`
                WHERE uid IN ({placeholders})
                  AND amount > 0
                  AND status = 1
            """
            cur.execute(sql_paid, c_list)
            
            # Group by user to see details
            user_paid_map = {}
            for r in cur.fetchall():
                uid = r[0]
                amount = r[1]
                pt = r[2]
                pname = r[3]
                if uid not in user_paid_map: user_paid_map[uid] = []
                user_paid_map[uid].append(f"{amount} ({pname}) at {datetime.datetime.fromtimestamp(pt)}")
            
            print(f'Users who cancelled trial but have paid orders: {len(user_paid_map)}')
            
            for uid, orders in user_paid_map.items():
                print(f'\nUser {uid}:')
                for o in orders:
                    print(f'  - Paid: {o}')

    finally:
        conn.close()
