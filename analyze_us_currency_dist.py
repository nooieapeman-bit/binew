
import pymysql
import datetime
from sshtunnel import SSHTunnelForwarder

# US Config
SSH_HOST = '34.197.218.151'
SSH_PORT = 18822
SSH_USER = 'ssh_tunnel_user'
SSH_PASS = 'z6TOa8uKwBDtlki4q10BVFuJ'

RDS_HOST = 'osaio-us-bicenter.c5iooxepal2e.us-east-1.rds.amazonaws.com'
RDS_PORT = 3306
DB_USER = 'readonly'
DB_PASS = 'WHFOWEIF##$#$...'
DB_NAME = 'bi_center'

# Codes for US
PLATINUM_CODES = set(['50e5b771de60f1816e964a7ef097f120', 'c22f95e0eb3856e083ab265a97b5be9f'])

# Timeframe (UTC)
start_dt = datetime.datetime(2026, 1, 5, 7, 0, 0, tzinfo=datetime.timezone.utc)
end_dt = datetime.datetime(2026, 1, 12, 7, 0, 0, tzinfo=datetime.timezone.utc)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

print(f"Timeframe: {start_dt} to {end_dt}")

with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASS,
        remote_bind_address=(RDS_HOST, RDS_PORT),
        allow_agent=False
    ) as tunnel:
    
    conn = pymysql.connect(
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    
    try:
        with conn.cursor() as cur:
            # 1. Fetch Users and Rules
            print("Fetching Users and Rules...")
            cur.execute(f"SELECT uid FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            uids = [r[0] for r in cur.fetchall()]
            
            if not uids:
                print("No users found.")
                exit()
            
            chunk_size = 2000
            user_rules = {} 
            for i in range(0, len(uids), chunk_size):
                chunk = uids[i:i+chunk_size]
                ph = ', '.join(['%s'] * len(chunk))
                cur.execute(f"SELECT uid, set_meal_code FROM set_meal_user_rule WHERE uid IN ({ph})", chunk)
                for r in cur.fetchall():
                    if r[0] not in user_rules: user_rules[r[0]] = set()
                    user_rules[r[0]].add(r[1])
            
            group_a_uids = []
            group_b_uids = []
            control_uids = []
            
            for uid in uids:
                rules = user_rules.get(uid, set())
                if not rules:
                    control_uids.append(uid)
                elif not rules.isdisjoint(PLATINUM_CODES):
                    group_b_uids.append(uid)
                else:
                    group_a_uids.append(uid)
            
            print(f"Group A Users: {len(group_a_uids)}")
            print(f"Group B Users: {len(group_b_uids)}")
            print(f"Control Users: {len(control_uids)}")

            def analyze_currency(name, group_uids):
                if not group_uids:
                    print(f"\n{name}: No users.")
                    return
                
                print(f"\nAnalyzing {name} Currency Dist...")
                curr_dist = {}
                total_subs = 0
                
                for i in range(0, len(group_uids), chunk_size):
                    chunk = group_uids[i:i+chunk_size]
                    ph = ', '.join(['%s'] * len(chunk))
                    
                    sql = f"""
                        SELECT currency, COUNT(DISTINCT subscribe_id)
                        FROM `order`
                        WHERE uid IN ({ph})
                          AND amount = 0
                          AND status = 1
                          AND subscribe_id != ''
                        GROUP BY currency
                    """
                    cur.execute(sql, chunk)
                    
                    for row in cur.fetchall():
                        curr = row[0]
                        count = row[1]
                        curr_dist[curr] = curr_dist.get(curr, 0) + count
                        total_subs += count
                
                for curr, count in sorted(curr_dist.items(), key=lambda x: x[1], reverse=True):
                    print(f"  {curr}: {count}")
                print(f"  Total Trial Subs: {total_subs}")

            analyze_currency("Group A", group_a_uids)
            analyze_currency("Group B", group_b_uids)
            analyze_currency("Control Group", control_uids)

    finally:
        conn.close()
