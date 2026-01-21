
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
            print('Fetching Group A UIDs...')
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
            
            # 2. Find ALL cancelled 0-amount orders (Broad Set, N=20)
            print(f'Analzying cancellations for {len(group_a_uids)} users...')
            
            chunk_size = 1000
            broad_cancelled_subs = set()
            broad_sub_details = {} # sid -> (uid, desc)
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # Get SubIDs
                sql = f"""
                    SELECT DISTINCT subscribe_id, uid, description
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND amount = 0
                      AND status = 1
                      AND subscribe_id != ''
                """
                cur.execute(sql, chunk)
                rows = cur.fetchall()
                
                if not rows: continue
                
                sub_map = {}
                for r in rows:
                    if r[0] not in sub_map:
                        sub_map[r[0]] = {'uid': r[1], 'desc': r[2]} # first seen desc
                
                sids = list(sub_map.keys())
                sph = ', '.join(['%s'] * len(sids))
                
                # Check cancellation
                sql_check = f"SELECT subscribe_id, MAX(cancel_time) FROM subscribe WHERE subscribe_id IN ({sph}) GROUP BY subscribe_id"
                cur.execute(sql_check, sids)
                
                for r in cur.fetchall():
                    sid = r[0]
                    ctime = r[1]
                    if ctime and ctime > 0:
                        broad_cancelled_subs.add(sid)
                        broad_sub_details[sid] = sub_map[sid]
            
            print(f'Total Cancelled (Broad): {len(broad_cancelled_subs)}')
            
            # 3. Find STANDARD cancelled orders (Filtered Set, N=19)
            # Filter condition: desc IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
            standard_cancelled_subs = set()
            for sid, details in broad_sub_details.items():
                desc = details['desc']
                if desc in ['Trial: 14 DAY', 'Promotion: 14 DAY']:
                    standard_cancelled_subs.add(sid)
            
            print(f'Total Cancelled (Standard): {len(standard_cancelled_subs)}')
            
            # 4. Find the Diff
            diff = broad_cancelled_subs - standard_cancelled_subs
            print(f'Difference: {len(diff)}')
            
            for sid in diff:
                d = broad_sub_details[sid]
                print(f'\n--- The Missing One ---')
                print(f"UID: {d['uid']}")
                print(f"SubscribeID: {sid}")
                print(f"Description: '{d['desc']}'")

    finally:
        conn.close()
