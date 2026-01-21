
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

            # 2. Find 10 users with multiple orders
            target_uids = []
            chunk_size = 1000
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT uid, COUNT(*) 
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND status = 1
                      AND amount = 0
                    GROUP BY uid
                    HAVING COUNT(*) > 1
                    LIMIT 10
                """
                cur.execute(sql, chunk)
                rows = cur.fetchall()
                for r in rows:
                    target_uids.append(r[0])
                    if len(target_uids) >= 10: break
                if len(target_uids) >= 10: break
            
            print(f'Selected {len(target_uids)} users: {target_uids}')
            
            # 3. Fetch Details
            placeholders = ', '.join(['%s'] * len(target_uids))
            sql_details = f"""
                SELECT o.uid, o.amount, o.description, o.pay_time, o.status, s.name
                FROM `order` o
                JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid IN ({placeholders})
                  AND o.status = 1
                  AND o.amount = 0
                ORDER BY o.uid, o.pay_time
            """
            
            cur.execute(sql_details, target_uids)
            
            current_uid = None
            for row in cur.fetchall():
                uid, amt, desc, pt, stat, pname = row
                pt_dt = datetime.datetime.fromtimestamp(pt)
                
                if uid != current_uid:
                    print(f'\n--- User {uid} ---')
                    current_uid = uid
                
                print(f'- {pt_dt} | {pname:<16} | Amt: {amt} | Status: {stat} | {desc}')

    finally:
        conn.close()
