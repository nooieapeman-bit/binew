
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
            print('Fetching Group A users...')
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
            
            print(f'Group A Users: {len(group_a_uids)}')
            
            # 2. Analyze package distribution by subscribing_id
            # Distinct subscribe_id -> product_name
            # Since multiple orders share same subscribe_id and same product (as seen in single user drilldown),
            # we can GROUP BY subscribe_id and pick any product_name (or check distinct).
            
            chunk_size = 1000
            pkg_counts = {}
            
            print('Analyzing package distribution (Group by Subscribe_ID)...')
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT s.name, COUNT(DISTINCT o.subscribe_id)
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.amount = 0
                      AND o.status = 1
                      AND o.subscribe_id != ''
                    GROUP BY s.name
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    pname = row[0]
                    count = row[1]
                    if pname not in pkg_counts:
                        pkg_counts[pname] = 0
                    pkg_counts[pname] += count
            
            print('\nGroup A Subscription Distribution (Unique Subscriptions):')
            sorted_pkg = sorted(pkg_counts.items(), key=lambda x: x[1], reverse=True)
            total_subs = 0
            for name, count in sorted_pkg:
                print(f"'{name}': {count}")
                total_subs += count
            
            print(f'\nTotal Subscriptions: {total_subs}')
            print(f'Total Group A Users: {len(group_a_uids)}')
            # If Total Subs > Total Users, some users have >1 Subscription (not just >1 orders).

    finally:
        conn.close()
