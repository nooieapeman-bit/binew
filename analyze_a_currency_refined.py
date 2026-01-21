
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
            
            # 2. Analyze standard trial orders
            print(f'Analyzing currency for Standard Trial orders in Group A (Size: {len(group_a_uids)})...')
            
            # Standard Trial: amount=0, status=1, description in ('Trial: 14 DAY', 'Promotion: 14 DAY')
            chunk_size = 1000
            order_curr_dist = {}
            sub_curr_dist = {}
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT currency, COUNT(*), COUNT(DISTINCT subscribe_id)
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND status = 1
                      AND amount = 0
                      AND description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                    GROUP BY currency
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    curr = row[0]
                    orders = row[1]
                    subs = row[2]
                    order_curr_dist[curr] = order_curr_dist.get(curr, 0) + orders
                    sub_curr_dist[curr] = sub_curr_dist.get(curr, 0) + subs
            
            print('\nGroup A Standard Trial Currency Distribution:')
            print(f"{'Currency':<10} | {'Orders':<10} | {'Unique Subs':<12}")
            print("-" * 35)
            for curr in sorted(order_curr_dist.keys()):
                print(f"{curr:<10} | {order_curr_dist[curr]:<10} | {sub_curr_dist[curr]:<12}")
                
    finally:
        conn.close()
