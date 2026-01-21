
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
            
            # 2. Breakdown counts
            print(f'Breaking down Plus duplicate counts...')
            
            chunk_size = 1000
            breakdown = {
                'Plus monthly': {},
                'Plus yearly': {}
            }
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT s.name, o.subscribe_id, COUNT(*)
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.amount = 0
                      AND o.status = 1
                      AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                      AND s.name IN ('Plus monthly', 'Plus yearly')
                    GROUP BY s.name, o.subscribe_id
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    pname = row[0]
                    cnt = row[2]
                    
                    if cnt not in breakdown[pname]:
                        breakdown[pname][cnt] = 0
                    breakdown[pname][cnt] += 1

            print('\nPlus Duplicate Count Distribution:')
            for pname, counts in breakdown.items():
                print(f'\n--- {pname} ---')
                sorted_counts = sorted(counts.items())
                total_subs = 0
                for c, num in sorted_counts:
                    print(f'{c} orders: {num} subscriptions')
                    total_subs += num
                print(f'Total: {total_subs}')

    finally:
        conn.close()
