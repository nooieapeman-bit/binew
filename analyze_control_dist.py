
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Timestamps (UTC)
start_dt = datetime.datetime(2026, 1, 6, 2, 0, 0, tzinfo=datetime.timezone.utc)
end_dt = datetime.datetime(2026, 1, 13, 2, 0, 0, tzinfo=datetime.timezone.utc)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

# Group A Logic: Not having Platinum
platinum_codes = set([
    'c22f95e0eb3856e083ab265a97b5be9f', 
    '50e5b771de60f1816e964a7ef097f120'
])

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Fetch Control (No Set Meal Code)
            # Logic: Users in timeframe who do NOT have any record in set_meal_user_rule
            print('Fetching Control Group UIDs...')
            
            # Get all UIDs in timeframe
            cur.execute(f"SELECT uid FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            all_uids = set([r[0] for r in cur.fetchall()])
            
            # Get A/B UIDs (those in rule table)
            cur.execute(f"SELECT DISTINCT uid FROM set_meal_user_rule")
            rule_uids = set([r[0] for r in cur.fetchall()])
            
            # Control = All - Rule
            control_uids = list(all_uids - rule_uids)
            print(f'Control Group Users: {len(control_uids)}')
            
            # 2. Analyze Trial Dist
            print('Analyzing Control group trial distribution...')
            
            chunk_size = 1000
            pkg_counts = {}
            
            for i in range(0, len(control_uids), chunk_size):
                chunk = control_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT s.name, COUNT(DISTINCT o.subscribe_id)
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.amount = 0
                      AND o.status = 1
                      # Standard Trial Filter
                      AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                      AND o.subscribe_id != ''
                    GROUP BY s.name
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    pname = row[0]
                    count = row[1]
                    if pname not in pkg_counts: pkg_counts[pname] = 0
                    pkg_counts[pname] += count
            
            print(f'\nControl Group Trial Distribution:')
            sorted_pkg = sorted(pkg_counts.items(), key=lambda x: x[1], reverse=True)
            total_trials = 0
            for name, count in sorted_pkg:
                print(f"'{name}': {count}")
                total_trials += count
            
            print(f'\nTotal Valid Trial Subscriptions (Control): {total_trials}')

    finally:
        conn.close()
