
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
            
            # 2. Get Cancelled Subscriptions Details
            print(f'Scanning cancellations for {len(group_a_uids)} Group A users...')
            
            # Logic:
            # 1. Get `order` (status=1, amt=0) -> subscribe_id
            # 2. Check `subscribe` table for that ID. If cancel_time > 0 -> Cancelled.
            # 3. Get `set_meal` name.
            
            chunk_size = 1000
            cancelled_stats = {}
            
            total_cancelled = 0
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # We need to join subscribe table to check status
                # Note: A user might have multiple rows in subscribe table for same ID (history).
                # We want the LATEST status.
                # However, usually if ANY row has cancel_time set, it was cancelled at least once.
                # Or we can just check the row with max id.
                
                # Simplified but robust approach:
                # Get usage of subscribe_id from order -> then check subscribe table
                
                sql_orders = f"""
                    SELECT DISTINCT o.subscribe_id, s.name
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.amount = 0 
                      AND o.status = 1
                      # We stick to the description filter to match your 'Valid Trial' definition
                      AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                """
                cur.execute(sql_orders, chunk)
                sub_pkg_map = {row[0]: row[1] for row in cur.fetchall()}
                
                if not sub_pkg_map: continue
                
                sids = list(sub_pkg_map.keys())
                
                # Now check cancellation for these sids
                # We fetch ID, SID, CANCEL_TIME
                # We sort by ID desc in python to get latest status
                
                sid_placeholders = ', '.join(['%s'] * len(sids))
                sql_subs = f"""
                    SELECT id, subscribe_id, cancel_time
                    FROM subscribe
                    WHERE subscribe_id IN ({sid_placeholders})
                """
                cur.execute(sql_subs, sids)
                
                sub_status = {} # sid -> {id, cancel_time}
                for r in cur.fetchall():
                    rid, sid, ctime = r
                    if sid not in sub_status or rid > sub_status[sid]['id']:
                        sub_status[sid] = {'id': rid, 'cancel_time': ctime}
                
                # Count cancellations
                for sid, info in sub_status.items():
                    if info['cancel_time'] and info['cancel_time'] > 0:
                        pname = sub_pkg_map[sid]
                        if pname not in cancelled_stats: cancelled_stats[pname] = 0
                        cancelled_stats[pname] += 1
                        total_cancelled += 1

            print(f'\nTotal Cancelled Subscriptions (Group A): {total_cancelled}')
            print('Distribution by Package:')
            sorted_stats = sorted(cancelled_stats.items(), key=lambda x: x[1], reverse=True)
            for name, count in sorted_stats:
                print(f"'{name}': {count}")

    finally:
        conn.close()
