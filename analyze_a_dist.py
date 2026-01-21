
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
            # 1. Fetch AB test participants
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
                uid = row[0]
                code = row[1]
                if uid not in user_codes: user_codes[uid] = set()
                user_codes[uid].add(code)
                
            group_a_uids = [] # No Platinum
            for uid, codes in user_codes.items():
                if codes.isdisjoint(platinum_codes):
                    group_a_uids.append(uid)
            
            print(f'Group A Size: {len(group_a_uids)}')
            
            if not group_a_uids:
                print("No Group A users found.")
                exit()
            
            chunk_size = 1000
            
            # 2. Get Trial Order Distribution
            # We need to link order.product_id -> set_meal.name
            
            product_counts = {}
            
            print('Analyzing trial order distribution...')
            
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # Using JOIN on product_id = set_meal.code (usually product_id in 'order' table is the code string)
                # Let's verify 'order' table structure or assume product_id is the code.
                # In migration scripts, we saw 'order.product_id' being used.
                # If set_meal has 'code' column and no 'id' column, then join on code.
                
                sql = f"""
                    SELECT s.name, COUNT(*) as cnt
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.status = 1
                      AND o.amount = 0
                    GROUP BY s.name
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    pname = row[0]
                    count = row[1]
                    if pname not in product_counts:
                        product_counts[pname] = 0
                    product_counts[pname] += count
            
            print('\nGroup A Trial Order Distribution:')
            sorted_products = sorted(product_counts.items(), key=lambda x: x[1], reverse=True)
            for name, count in sorted_products:
                print(f"'{name}': {count}")
                
    finally:
        conn.close()
