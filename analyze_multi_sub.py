
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
            
            # 2. Find users with >1 Subscribe ID
            target_uids = []
            chunk_size = 1000
            
            print(f'Scanning {len(group_a_uids)} Group A users for multi-subscriptions...')
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT uid, COUNT(DISTINCT subscribe_id)
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND amount = 0
                      AND status = 1
                      AND subscribe_id != ''
                    GROUP BY uid
                    HAVING COUNT(DISTINCT subscribe_id) > 1
                """
                cur.execute(sql, chunk)
                rows = cur.fetchall()
                for r in rows:
                    target_uids.append(r[0])
            
            print(f'Found {len(target_uids)} users with multiple subscriptions.')
            
            if not target_uids:
                exit()
                
            # 3. Detail Analysis
            placeholders = ', '.join(['%s'] * len(target_uids))
            
            # Get details: UID, SubID, ProductName, PayTime
            sql_details = f"""
                SELECT o.uid, o.subscribe_id, s.name, MIN(o.pay_time) as first_pay
                FROM `order` o
                JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid IN ({placeholders})
                  AND o.amount = 0
                  AND o.status = 1
                GROUP BY o.uid, o.subscribe_id, s.name
                ORDER BY o.uid, first_pay
            """
            cur.execute(sql_details, target_uids)
            
            curr_uid = None
            for row in cur.fetchall():
                uid, sid, pname, pt = row
                pt_dt = datetime.datetime.fromtimestamp(pt)
                
                if uid != curr_uid:
                    print(f'\n--- User {uid} ---')
                    curr_uid = uid
                print(f'- {pt_dt} | {pname} | SubID: {sid}')

    finally:
        conn.close()
