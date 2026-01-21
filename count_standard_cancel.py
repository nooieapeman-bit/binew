
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
            
            # 2. Check cancellation for STANDARD trials
            # Logic: 
            # 1. Find DISTINCT subscribe_id from order table (A group uids, amount=0, status=1, desc IN (...))
            # 2. Check subscribe table for these IDs. If cancel_time > 0 -> COUNT.
            
            print(f'Checking {len(group_a_uids)} Group A users for STANDARD cancellations...')
            
            chunk_size = 1000
            cancelled_count = 0
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # Using a comprehensive SQL to do it in one go if possible, or python logic.
                # Here we use python to be precise about "latest status".
                
                sql_get_subs = f"""
                    SELECT DISTINCT subscribe_id
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND amount = 0
                      AND status = 1
                      AND description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                      AND subscribe_id != ''
                """
                cur.execute(sql_get_subs, chunk)
                sids = [r[0] for r in cur.fetchall()]
                
                if not sids: continue
                
                # Check status
                sid_ph = ', '.join(['%s'] * len(sids))
                sql_check = f"""
                    SELECT subscribe_id, MAX(cancel_time) 
                    FROM subscribe 
                    WHERE subscribe_id IN ({sid_ph})
                    GROUP BY subscribe_id
                """
                cur.execute(sql_check, sids)
                
                for r in cur.fetchall():
                    if r[1] and r[1] > 0:
                        cancelled_count += 1
            
            print(f'Total Cancelled Standard Subscriptions: {cancelled_count}')
            
            print('\nEquivalent SQL Logic (Concept):')
            print("""
SELECT COUNT(DISTINCT s.subscribe_id)
FROM subscribe s
JOIN `order` o ON s.subscribe_id = o.subscribe_id
WHERE o.uid IN (...GROUP_A_UIDS...)
  AND o.amount = 0 
  AND o.status = 1
  AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
  AND s.cancel_time > 0
            """)

    finally:
        conn.close()
