
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

            # 2. Re-Analyze the same 10 users but show order_id
            target_uids = ['eul0m1m6a1l0q9l7', 'eul0m3m7r1l2c4f1', 'eul0m7w2f1l0q1s7', 'eul0q0r3c1l1t1c4', 
                           'eul0s4w0a1l1a8r4', 'eul0s8l2c1l0s6s7', 'eul0t1w0m1l1t9l2', 'eul0w0a3a1l1l4q2', 
                           'eul1a0m8w1l1f2t2', 'eul1a5f9l1l0q3c5']
            
            # 3. Fetch Details including order_id
            placeholders = ', '.join(['%s'] * len(target_uids))
            sql_details = f"""
                SELECT o.uid, o.order_id, o.amount, o.description, o.pay_time, o.status, s.name
                FROM `order` o
                JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid IN ({placeholders})
                  AND o.amount = 0
                  AND o.status = 1
                ORDER BY o.uid, o.pay_time
            """
            
            cur.execute(sql_details, target_uids)
            
            current_uid = None
            for row in cur.fetchall():
                uid, oid, amt, desc, pt, stat, pname = row
                pt_dt = datetime.datetime.fromtimestamp(pt)
                
                if uid != current_uid:
                    print(f'\n--- User {uid} ---')
                    current_uid = uid
                
                print(f'- OrderID: {oid} | {pt_dt} | {pname:<16} | Amt: {amt} | Status: {stat}')
            
            # 4. Check if we have ANY case where order_id is duplicated for status=1
            print('\nChecking for duplicate order_id in Group A status=1 results...')
            # NOTE: order_id is usually unique or primary key? Or logic id?
            # If it's the logic ID, we check if multiple rows share it.
            
            placeholders_all = ', '.join(['%s'] * len(group_a_uids))
            sql_dupe_check = f"""
                SELECT order_id, COUNT(*)
                FROM `order`
                WHERE uid IN ({placeholders_all})
                  AND status = 1
                  AND amount = 0
                GROUP BY order_id
                HAVING COUNT(*) > 1
            """
            # Wait, execute on all ~1700 uids might be large query string but okay.
            # We must batch if too large. 1700 UIDs * ~20 chars = 34KB query. Safe.
            
            # Splitting just in case
            chunk_size = 1000
            dupe_order_ids = 0
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                if not chunk: continue
                p_chunk = ', '.join(['%s'] * len(chunk))
                sql = f"""
                    SELECT order_id, COUNT(*)
                    FROM `order`
                    WHERE uid IN ({p_chunk})
                      AND status = 1
                      AND amount = 0
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                """
                cur.execute(sql, chunk)
                dupe_order_ids += len(cur.fetchall())
                
            print(f'Order IDs with multiple status=1 records: {dupe_order_ids}')

    finally:
        conn.close()
