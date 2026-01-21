
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Timestamps (UTC) for EU period
start_dt = datetime.datetime(2026, 1, 6, 2, 0, 0, tzinfo=datetime.timezone.utc)
end_dt = datetime.datetime(2026, 1, 13, 2, 0, 0, tzinfo=datetime.timezone.utc)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Fetch Control participants (registered in period, no rules)
            print('Identifying Control Group users...')
            # First, get all users in period
            cur.execute(f"SELECT uid FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            all_uids = [r[0] for r in cur.fetchall()]
            
            # Get members in set_meal_user_rule
            cur.execute(f"SELECT DISTINCT uid FROM set_meal_user_rule")
            rule_uids = set(r[0] for r in cur.fetchall())
            
            control_uids = [uid for uid in all_uids if uid not in rule_uids]
            print(f"Control Users: {len(control_uids)}")

            if not control_uids:
                print("No Control users found.")
                exit()

            # 2. Analyze trial orders
            print('Analyzing trial distribution for Control group...')
            chunk_size = 1000
            dist = {} # (pkg_name) -> { (currency, amount) -> count }
            
            for i in range(0, len(control_uids), chunk_size):
                chunk = control_uids[i:i+chunk_size]
                ph = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT s.name, o.currency, o.amount, COUNT(DISTINCT o.subscribe_id)
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({ph})
                      AND o.status = 1
                      AND o.amount = 0
                      AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                    GROUP BY s.name, o.currency, o.amount
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    pkg_name, curr, amt, cnt = row
                    if pkg_name not in dist:
                        dist[pkg_name] = {}
                    key = (curr, float(amt))
                    dist[pkg_name][key] = dist[pkg_name].get(key, 0) + cnt

            print('\nEU Control Group Trial Detail Distribution:')
            print(f"{'Package Name':<45} | {'Currency':<10} | {'Amount':<8} | {'Unique Subs':<12}")
            print("-" * 85)
            
            # Sort by total package count descending
            pkg_totals = {pkg: sum(counts.values()) for pkg, counts in dist.items()}
            for pkg_name, total in sorted(pkg_totals.items(), key=lambda x: x[1], reverse=True):
                sub_counts = dist[pkg_name]
                for (curr, amt), count in sorted(sub_counts.items(), key=lambda x: x[1], reverse=True):
                    print(f"{pkg_name[:45]:<45} | {curr:<10} | {amt:<8.2f} | {count:<12}")
                
    finally:
        conn.close()
