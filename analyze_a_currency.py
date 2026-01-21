
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Timestamps (UTC) for the A/B test period
start_dt = datetime.datetime(2026, 1, 6, 2, 0, 0, tzinfo=datetime.timezone.utc)
end_dt = datetime.datetime(2026, 1, 13, 2, 0, 0, tzinfo=datetime.timezone.utc)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

# Platinum codes used to distinguish Group B
platinum_codes = set([
    'c22f95e0eb3856e083ab265a97b5be9f', 
    '50e5b771de60f1816e964a7ef097f120'
])

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Fetch Group A participants (registered in period, have rules, no Platinum)
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
                
            group_a_uids = []
            for uid, codes in user_codes.items():
                if codes.isdisjoint(platinum_codes):
                    group_a_uids.append(uid)
            
            print(f'Group A Size: {len(group_a_uids)}')
            
            if not group_a_uids:
                print("No Group A users found.")
                exit()
            
            # 2. Check the column names in the order table to be sure about currency field
            cur.execute("SHOW COLUMNS FROM `order` LIKE '%currency%'")
            currency_columns = [r[0] for r in cur.fetchall()]
            if not currency_columns:
                # Try common names if no match
                cur.execute("SHOW COLUMNS FROM `order`")
                all_cols = [r[0] for r in cur.fetchall()]
                print(f"All columns in order table: {all_cols}")
                # Looking for anything that sounds like currency
                currency_field = next((c for c in all_cols if 'curr' in c.lower() or 'coin' in c.lower()), None)
            else:
                currency_field = currency_columns[0]
            
            print(f"Using currency field: {currency_field}")
            
            if not currency_field:
                print("Could not find a currency field in the order table.")
                exit()

            # 3. Analyze trial order currency distribution for Group A
            print('Analyzing currency distribution for trial orders...')
            currency_dist = {}
            chunk_size = 1000
            for i in range(0, len(group_a_uids), chunk_size):
                chunk = group_a_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT `{currency_field}`, COUNT(*) as cnt
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND status = 1
                      AND amount = 0
                    GROUP BY `{currency_field}`
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    curr = row[0]
                    count = row[1]
                    currency_dist[curr] = currency_dist.get(curr, 0) + count
            
            print('\nGroup A Trial Order Currency Distribution:')
            for curr, count in sorted(currency_dist.items(), key=lambda x: x[1], reverse=True):
                print(f"{curr}: {count}")
                
    finally:
        conn.close()
