
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
            
            # 2. Find orders with empty description AND amount=0 AND status=1
            print(f'Checking {len(group_a_uids)} Group A users for empty description orders...')
            
            target_uids = set()
            chunk_size = 1000
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT DISTINCT uid
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND amount = 0
                      AND status = 1
                      AND (description IS NULL OR description = '' OR description NOT IN ('Trial: 14 DAY', 'Promotion: 14 DAY'))
                """
                cur.execute(sql, chunk)
                for r in cur.fetchall():
                    target_uids.add(r[0])
            
            print(f'Found {len(target_uids)} users with non-standard trial orders.')
            
            if not target_uids:
                exit()
            
            # 3. Print ALL trial orders for these users
            t_uids_list = list(target_uids)
            placeholders = ', '.join(['%s'] * len(t_uids_list))
            
            # Also get cancellation info to highlight which one was cancelled
            sql_print = f"""
                SELECT o.uid, o.pay_time, s.name, o.subscribe_id, o.amount, o.description, o.status
                FROM `order` o
                JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid IN ({placeholders})
                  AND o.amount = 0
                  AND o.status = 1
                ORDER BY o.uid, o.pay_time
            """
            cur.execute(sql_print, t_uids_list)
            
            rows = cur.fetchall()
            
            # Fetch cancel times for these subscribe_ids
            sub_ids = set([r[3] for r in rows if r[3]])
            sub_cancel_map = {}
            if sub_ids:
                s_placeholders = ', '.join(['%s'] * len(sub_ids))
                cur.execute(f"SELECT subscribe_id, MAX(cancel_time) FROM subscribe WHERE subscribe_id IN ({s_placeholders}) GROUP BY subscribe_id", list(sub_ids))
                for r in cur.fetchall():
                    sub_cancel_map[r[0]] = r[1]
            
            curr_uid = None
            for row in rows:
                uid, pt, pname, sid, amt, desc, stat = row
                pt_dt = datetime.datetime.fromtimestamp(pt)
                
                cancel_ts = sub_cancel_map.get(sid)
                is_cancelled = " [CANCELLED]" if (cancel_ts and cancel_ts > 0) else ""
                
                if uid != curr_uid:
                    print(f'\n=== User {uid} ===')
                    curr_uid = uid
                    
                print(f'{pt_dt} | {pname:<16} | SubID: {sid:<20} | Desc: [{desc}] {is_cancelled}')

    finally:
        conn.close()
