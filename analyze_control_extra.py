
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Timestamps (UTC)
start_dt = datetime.datetime(2026, 1, 6, 2, 0, 0, tzinfo=datetime.timezone.utc)
end_dt = datetime.datetime(2026, 1, 13, 2, 0, 0, tzinfo=datetime.timezone.utc)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Fetch ALL users in time range
            print('Fetching all user IDs in range...')
            cur.execute(f"SELECT uid FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            all_uids_set = set([r[0] for r in cur.fetchall()])
            print(f'Total Users: {len(all_uids_set)}')

            # 2. Fetch AB test participants
            print('Fetching AB test participants...')
            sql_users = f"""
                SELECT DISTINCT u.uid
                FROM user u
                JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """
            cur.execute(sql_users)
            ab_participants = set([r[0] for r in cur.fetchall()])
            
            # 3. Identify Control Group
            control_uids = list(all_uids_set - ab_participants)
            print(f'Control Group Size: {len(control_uids)}')

            if not control_uids:
                print("No control group users found.")
                exit()

            # 4. Analyze descriptions for the "extra" 28 users
            # status=1, amount=0, description NOT IN target list
            
            chunk_size = 1000
            description_counts = {}
            
            print('Querying breakdown of non-standard descriptions...')
            
            for i in range(0, len(control_uids), chunk_size):
                chunk = control_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT description, COUNT(*) as cnt
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND status = 1
                      AND amount = 0
                      AND description NOT IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                    GROUP BY description
                """
                cur.execute(sql, chunk)
                rows = cur.fetchall()
                for desc, count in rows:
                    if desc not in description_counts:
                        description_counts[desc] = 0
                    description_counts[desc] += count

            print('\nBreakdown of non-standard descriptions:')
            if not description_counts:
                print("None found.")
            else:
                sorted_desc = sorted(description_counts.items(), key=lambda x: x[1], reverse=True)
                for desc, count in sorted_desc:
                    print(f"'{desc}': {count}")

    finally:
        conn.close()
