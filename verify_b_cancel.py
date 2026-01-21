
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
            # 1. Fetch Group B UIDs
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
            
            group_b_uids = [] # Has Platinum
            for uid, codes in user_codes.items():
                if not codes.isdisjoint(platinum_codes):
                    group_b_uids.append(uid)
            
            print(f'Checking {len(group_b_uids)} Group B users for STANDARD cancellations...')
            
            # Logic RE-VERIFICATION:
            # 1. Get DISTINCT subscribe_id for orders meeting criteria.
            # 2. Check subscribe table for cancel_time > 0.
            
            chunk_size = 1000
            cancelled_count = 0
            pkg_breakdown = {}
            
            for i in range(0, len(group_b_uids), chunk_size):
                chunk = group_b_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql_get_subs = f"""
                    SELECT DISTINCT o.subscribe_id, s.name
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.amount = 0
                      AND o.status = 1
                      # STRICT STANDARD TRIAL FILTER
                      AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                      AND o.subscribe_id != ''
                """
                cur.execute(sql_get_subs, chunk)
                rows = cur.fetchall()
                
                if not rows: continue
                
                sub_pkg_map = {r[0]: r[1] for r in rows}
                sids = list(sub_pkg_map.keys())
                
                sid_ph = ', '.join(['%s'] * len(sids))
                sql_check = f"""
                    SELECT subscribe_id, MAX(cancel_time) 
                    FROM subscribe 
                    WHERE subscribe_id IN ({sid_ph})
                    GROUP BY subscribe_id
                """
                cur.execute(sql_check, sids)
                
                for r in cur.fetchall():
                    sid = r[0]
                    ctime = r[1]
                    if ctime and ctime > 0:
                        cancelled_count += 1
                        pname = sub_pkg_map[sid]
                        if pname not in pkg_breakdown: pkg_breakdown[pname] = 0
                        pkg_breakdown[pname] += 1
            
            print(f'Total Cancelled Standard Subscriptions: {cancelled_count}')
            print('Breakdown:')
            for k, v in pkg_breakdown.items():
                print(f'- {k}: {v}')
            
            # Print SQL for manual verification
            print('\nSQL for Verification:')
            print("""
SELECT s.name as package_name, COUNT(DISTINCT sub.subscribe_id) as cancelled_count
FROM subscribe sub
JOIN `order` o ON sub.subscribe_id = o.subscribe_id
JOIN set_meal s ON o.product_id = s.code
JOIN user u ON o.uid = u.uid
JOIN set_meal_user_rule smur ON u.uid = smur.uid
WHERE u.register_time >= 1767664800 AND u.register_time < 1768269600
  AND smur.set_meal_code IN ('c22f95e0eb3856e083ab265a97b5be9f', '50e5b771de60f1816e964a7ef097f120') -- Group B Logic
  AND o.amount = 0 
  AND o.status = 1
  AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
  AND sub.cancel_time > 0
GROUP BY s.name;
            """)

    finally:
        conn.close()
