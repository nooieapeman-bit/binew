
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
            
            # 2. Strict Trial Analysis
            # Find users with >1 DISTINCT subscribe_id WHERE amount=0 AND status=1
            
            print(f'Checking {len(group_a_uids)} users for multiple TRIAL subscriptions...')
            
            chunk_size = 1000
            scan_result = []
            
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
                    scan_result.append(r[0])
            
            print(f'Users with >1 Distinct Trial Subscription: {len(scan_result)}')
            
            if scan_result:
                print('Users:', scan_result)
                
                # Print details for them
                ids = ', '.join(['%s'] * len(scan_result))
                sql_det = f"""
                    SELECT o.uid, o.subscribe_id, s.name, o.pay_time, o.amount
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({ids})
                      AND o.amount = 0
                      AND o.status = 1
                    ORDER BY o.uid, o.pay_time
                """
                cur.execute(sql_det, scan_result)
                
                curr = None
                for row in cur.fetchall():
                    if row[0] != curr:
                        print(f'\n--- User {row[0]} ---')
                        curr = row[0]
                    print(f'{datetime.datetime.fromtimestamp(row[3])} | {row[2]} | SubID: {row[1]} | Amt: {row[4]}')

    finally:
        conn.close()
