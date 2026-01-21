
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

# Codes
US_PKG_CODES = {
    'Plus yearly': '3c0c2022ee0b12491352182c7057829c',
    'Plus monthly': '43c7756d70231fe227b32591b81aa68d',
    'Platinum yearly': '50e5b771de60f1816e964a7ef097f120',
    'Platinum monthly': 'c22f95e0eb3856e083ab265a97b5be9f',
    'Enhanced yearly': 'dec01e518ba8c6f38ca635441fb09d81',
    'Enhanced monthly': 'eb92b6b1cfba8d5ca02fa45bd097d788',
}
plus_codes = set([US_PKG_CODES['Plus yearly'], US_PKG_CODES['Plus monthly']])
platinum_codes = set([US_PKG_CODES['Platinum yearly'], US_PKG_CODES['Platinum monthly']])

# Timeframe (UTC)
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
            # 1. Re-fetch UIDs (Fast)
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
            
            group_a_uids = []
            group_b_uids = []
            control_uids = []
            
            for uid in uids:
                rules = user_rules.get(uid, set())
                if not rules:
                    control_uids.append(uid)
                else:
                    if not rules.isdisjoint(platinum_codes):
                        group_b_uids.append(uid)
                    else:
                        group_a_uids.append(uid)
            
            # 2. Analyze Cancellations
            def analyze_cancellations(name, group_uids):
                if not group_uids:
                    print(f"\n{name}: No users.")
                    return
                
                print(f"\nAnalyzing Cancellations for {name}...")
                
                chunk_size = 1000
                cancelled_count = 0
                total_trials = 0
                
                for i in range(0, len(group_uids), chunk_size):
                    chunk = group_uids[i:i+chunk_size]
                    ph = ', '.join(['%s'] * len(chunk))
                    
                    # 1. Get Trial SubIDs
                    sql = f"""
                        SELECT DISTINCT subscribe_id
                        FROM `order`
                        WHERE uid IN ({ph})
                          AND amount = 0
                          AND status = 1
                          AND subscribe_id != ''
                    """
                    cur.execute(sql, chunk)
                    sids = [r[0] for r in cur.fetchall()]
                    
                    if not sids: continue
                    
                    # Count distinct trial subs (users might have multiple, but roughly = user count)
                    # Let's count cancelled subs directly
                    total_trials += len(sids)
                    
                    sph = ', '.join(['%s'] * len(sids))
                    sql_check = f"""
                        SELECT subscribe_id, MAX(cancel_time)
                        FROM subscribe
                        WHERE subscribe_id IN ({sph})
                        GROUP BY subscribe_id
                    """
                    cur.execute(sql_check, sids)
                    
                    for r in cur.fetchall():
                        if r[1] and r[1] > 0:
                            cancelled_count += 1
                
                rate = (cancelled_count / total_trials * 100) if total_trials else 0
                print(f"  Total Trial Subs: {total_trials}")
                print(f"  Cancelled Subs:   {cancelled_count} ({rate:.2f}%)")

            analyze_cancellations("Group A", group_a_uids)
            analyze_cancellations("Group B", group_b_uids)
            analyze_cancellations("Control Group", control_uids)

    finally:
        conn.close()
