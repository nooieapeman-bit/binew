
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
            # 1. Fetch Group A participants
            print('Fetching Group A participants...')
            sql_users = f"""
                SELECT u.uid, sc.set_meal_code
                FROM user u
                JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """
            cur.execute(sql_users)
            user_codes = {}
            for row in cur.fetchall():
                uid, code = row
                if uid not in user_codes: user_codes[uid] = set()
                user_codes[uid].add(code)
            group_a_uids = [uid for uid, codes in user_codes.items() if codes.isdisjoint(platinum_codes)]
            
            print(f'Group A Size: {len(group_a_uids)}')

            # 2. Analyze package distribution for GBP trial orders
            print('Analyzing package distribution for GBP Standard Trial orders...')
            
            package_dist = {}
            chunk_size = 1000
            total_unique_subs = 0
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # We group by set_meal name and count distinct subscribe_id
                sql = f"""
                    SELECT s.name, COUNT(DISTINCT o.subscribe_id)
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.currency = 'GBP'
                      AND o.status = 1
                      AND o.amount = 0
                      AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                    GROUP BY s.name
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    name = row[0]
                    count = row[1]
                    package_dist[name] = package_dist.get(name, 0) + count
                    total_unique_subs += count

            print('\nGroup A Standard Trial (GBP) Package Distribution (by Unique Subs):')
            print(f"{'Package Name':<40} | {'Unique Subs':<12}")
            print("-" * 55)
            # Sorting by count descending
            for name, count in sorted(package_dist.items(), key=lambda x: x[1], reverse=True):
                print(f"{name:<40} | {count:<12}")
            
            print(f"\nTotal Unique GBP Trial Subs: {total_unique_subs}")
                
    finally:
        conn.close()
