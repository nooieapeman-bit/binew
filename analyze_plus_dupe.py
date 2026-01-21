
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
            
            # 2. Find Plus Subscriptions
            # We want to check if users who chose 'Plus monthly' or 'Plus yearly' have multiple orders for that same sub_id.
            
            print(f'Checking Plus subscriptions in {len(group_a_uids)} Group A users...')
            
            chunk_size = 1000
            plus_sub_stats = {
                'Plus monthly': {'total_subs': 0, 'multi_order_subs': 0, 'total_orders': 0},
                'Plus yearly':  {'total_subs': 0, 'multi_order_subs': 0, 'total_orders': 0}
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
                    sid = row[1]
                    cnt = row[2]
                    
                    plus_sub_stats[pname]['total_subs'] += 1
                    plus_sub_stats[pname]['total_orders'] += cnt
                    if cnt > 1:
                        plus_sub_stats[pname]['multi_order_subs'] += 1

            print('\nPlus Package Multi-Order Analysis:')
            for name, stats in plus_sub_stats.items():
                total = stats['total_subs']
                multi = stats['multi_order_subs']
                orders = stats['total_orders']
                pct = (multi / total * 100) if total else 0
                avg = (orders / total) if total else 0
                
                print(f"'{name}':")
                print(f"  Total Subscriptions: {total}")
                print(f"  Subs with >1 Order:  {multi}")
                print(f"  Duplicate Rate:      {pct:.2f}%")
                print(f"  Avg Orders per Sub:  {avg:.2f}")

    finally:
        conn.close()
