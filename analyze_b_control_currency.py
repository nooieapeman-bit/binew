
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
            # 1. Fetch participants registered in period and their rule records
            sql_users = f"""
                SELECT u.uid, sc.set_meal_code
                FROM user u
                LEFT JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """
            cur.execute(sql_users)
            
            user_data = {} # uid -> codes set
            for row in cur.fetchall():
                uid, code = row
                if uid not in user_data: user_data[uid] = set()
                if code: user_data[uid].add(code)
            
            group_b_uids = []
            control_uids = []
            
            for uid, codes in user_data.items():
                if not codes:
                    control_uids.append(uid)
                elif not codes.isdisjoint(platinum_codes):
                    group_b_uids.append(uid)
            
            print(f'Group B Size: {len(group_b_uids)}')
            print(f'Control Group Size: {len(control_uids)}')

            def analyze_currency(group_name, uids):
                if not uids:
                    print(f"\nNo users for {group_name}")
                    return

                print(f'\nAnalyzing currency for {group_name} Standard Trial orders...')
                chunk_size = 1000
                curr_dist = {}
                total_subs = 0
                
                for i in range(0, len(uids), chunk_size):
                    chunk = uids[i:i+chunk_size]
                    placeholders = ', '.join(['%s'] * len(chunk))
                    
                    sql = f"""
                        SELECT currency, COUNT(DISTINCT subscribe_id)
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
                        count = row[1]
                        curr_dist[curr] = curr_dist.get(curr, 0) + count
                        total_subs += count
                
                print(f"{group_name} Currency Distribution (by Unique Subs):")
                print(f"{'Currency':<10} | {'Unique Subs':<12}")
                print("-" * 25)
                for curr, count in sorted(curr_dist.items(), key=lambda x: x[1], reverse=True):
                    print(f"{curr:<10} | {count:<12}")
                print(f"Total Unique Subs: {total_subs}")

            analyze_currency("Group B", group_b_uids)
            analyze_currency("Control Group", control_uids)
                
    finally:
        conn.close()
