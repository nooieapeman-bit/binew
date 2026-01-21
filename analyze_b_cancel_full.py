
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
            
            print(f'Group B Users: {len(group_b_uids)}')
            
            # --- Part 1: Cancellation Distribution (Standard Trials) ---
            print('Analyzing Group B standard trial cancellations...')
            
            chunk_size = 1000
            cancelled_uids = set()
            pkg_cancel_counts = {}
            total_cancelled = 0
            
            for i in range(0, len(group_b_uids), chunk_size):
                chunk = group_b_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # Get SubIDs for standard trials
                sql = f"""
                    SELECT DISTINCT subscribe_id, uid, s.name
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.amount = 0
                      AND o.status = 1
                      AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                      AND o.subscribe_id != ''
                """
                cur.execute(sql, chunk)
                rows = cur.fetchall()
                if not rows: continue
                
                sub_info_map = {r[0]: {'uid': r[1], 'pname': r[2]} for r in rows}
                sids = list(sub_info_map.keys())
                
                # Check cancellation
                s_ph = ', '.join(['%s'] * len(sids))
                sql_check = f"SELECT subscribe_id, MAX(cancel_time) FROM subscribe WHERE subscribe_id IN ({s_ph}) GROUP BY subscribe_id"
                cur.execute(sql_check, sids)
                
                for r in cur.fetchall():
                    sid = r[0]
                    ctime = r[1]
                    if ctime and ctime > 0:
                        info = sub_info_map[sid]
                        uid = info['uid']
                        pname = info['pname']
                        
                        cancelled_uids.add(uid)
                        total_cancelled += 1
                        if pname not in pkg_cancel_counts: pkg_cancel_counts[pname] = 0
                        pkg_cancel_counts[pname] += 1
            
            print(f'\nTotal Cancelled Standard Subscriptions (Group B): {total_cancelled}')
            print('Distribution by Package:')
            sorted_stats = sorted(pkg_cancel_counts.items(), key=lambda x: x[1], reverse=True)
            for name, count in sorted_stats:
                print(f"'{name}': {count}")
                
            # --- Part 2: Paid Conversion Check ---
            if not cancelled_uids:
                print('\nNo cancellations to check for conversion.')
                exit()
                
            print(f'\nChecking {len(cancelled_uids)} cancelled users for paid conversion...')
            
            c_list = list(cancelled_uids)
            placeholders = ', '.join(['%s'] * len(c_list))
            
            sql_paid = f"""
                SELECT DISTINCT o.uid, o.amount, o.pay_time, s.name, o.subscribe_id
                FROM `order` o
                JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid IN ({placeholders})
                  AND o.amount > 0
                  AND o.status = 1
            """
            cur.execute(sql_paid, c_list)
            
            paid_rows = cur.fetchall()
            
            # Check Active Status of Paid Subs
            paid_subs_ids = set([r[4] for r in paid_rows])
            sub_status_map = {}
            if paid_subs_ids:
                sph = ', '.join(['%s'] * len(paid_subs_ids))
                cur.execute(f"SELECT subscribe_id, MAX(cancel_time) FROM subscribe WHERE subscribe_id IN ({sph}) GROUP BY subscribe_id", list(paid_subs_ids))
                for r in cur.fetchall():
                    sub_status_map[r[0]] = r[1]

            converted_map = {}
            
            for r in paid_rows:
                uid, amt, pt, pname, sid = r
                cancel_ts = sub_status_map.get(sid)
                status = "ACTIVE" if (not cancel_ts or cancel_ts == 0) else "CANCELLED"
                
                if uid not in converted_map: converted_map[uid] = []
                converted_map[uid].append(f"{amt} ({pname}) [{status}]")
            
            print(f'Users with Paid Orders: {len(converted_map)}')
            for uid, details in converted_map.items():
                print(f'- User {uid}: {", ".join(details)}')

    finally:
        conn.close()
