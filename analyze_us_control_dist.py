
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

# Codes for grouping
US_PKG_CODES = {
    'Plus yearly': '3c0c2022ee0b12491352182c7057829c',
    'Plus monthly': '43c7756d70231fe227b32591b81aa68d',
    'Platinum yearly': '50e5b771de60f1816e964a7ef097f120',
    'Platinum monthly': 'c22f95e0eb3856e083ab265a97b5be9f',
}
plus_codes = set([US_PKG_CODES['Plus yearly'], US_PKG_CODES['Plus monthly']])
platinum_codes = set([US_PKG_CODES['Platinum yearly'], US_PKG_CODES['Platinum monthly']])

# Timeframe
start_dt = datetime.datetime(2026, 1, 5, 7, 0, 0, tzinfo=datetime.timezone.utc)
end_dt = datetime.datetime(2026, 1, 12, 7, 0, 0, tzinfo=datetime.timezone.utc)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

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
            # 1. Fetch UIDs and Rules
            cur.execute(f"SELECT uid FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            uids = [r[0] for r in cur.fetchall()]
            
            chunk_size = 2000
            user_rules = {} 
            for i in range(0, len(uids), chunk_size):
                chunk = uids[i:i+chunk_size]
                ph = ', '.join(['%s'] * len(chunk))
                cur.execute(f"SELECT uid, set_meal_code FROM set_meal_user_rule WHERE uid IN ({ph})", chunk)
                for r in cur.fetchall():
                    if r[0] not in user_rules: user_rules[r[0]] = set()
                    user_rules[r[0]].add(r[1])
            
            control_uids = []
            for uid in uids:
                if uid not in user_rules:
                    control_uids.append(uid)
            
            print(f'Control Users: {len(control_uids)}')
            
            # 2. Analyze Trial Dist
            print('Analyzing Control group trial distribution...')
            
            chunk_size = 1000
            pkg_counts = {}
            total_trials = 0
            
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
                      AND o.subscribe_id != ''
                    GROUP BY s.name
                """
                cur.execute(sql, chunk)
                
                for row in cur.fetchall():
                    pname = row[0]
                    count = row[1]
                    if pname not in pkg_counts: pkg_counts[pname] = 0
                    pkg_counts[pname] += count
                    total_trials += count
            
            print(f'\nUS Control Group Trial Distribution (Total: {total_trials}):')
            sorted_pkg = sorted(pkg_counts.items(), key=lambda x: x[1], reverse=True)
            for name, count in sorted_pkg:
                share = (count / total_trials * 100) if total_trials else 0
                print(f"{name:<50} | {count} ({share:.1f}%)")

    finally:
        conn.close()
