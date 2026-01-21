
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
enhanced_codes = set([US_PKG_CODES['Enhanced yearly'], US_PKG_CODES['Enhanced monthly']])

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
            # 1. Fetch UIDs and Rules for Registration Period
            print("Fetching Users and Rules...")
            
            # Get Users registered in timeframe
            cur.execute(f"SELECT uid, register_time FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            user_reg_map = {r[0]: r[1] for r in cur.fetchall()}
            total_users = len(user_reg_map)
            print(f"Total Registered Users: {total_users}")
            
            if total_users == 0: exit()
            
            # Get Rules (only for these users)
            uids = list(user_reg_map.keys())
            chunk_size = 2000
            user_rules = {} # uid -> set of codes
            
            for i in range(0, len(uids), chunk_size):
                chunk = uids[i:i+chunk_size]
                ph = ', '.join(['%s'] * len(chunk))
                cur.execute(f"SELECT uid, set_meal_code FROM set_meal_user_rule WHERE uid IN ({ph})", chunk)
                for r in cur.fetchall():
                    if r[0] not in user_rules: user_rules[r[0]] = set()
                    user_rules[r[0]].add(r[1])
            
            # Group Logic: 
            # A: Has Plus OR (Has Enhanced AND NOT Platinum) ? 
            # B: Has Platinum OR (Has Enhanced AND NOT Plus) ?
            # Wait, standard logic is usually:
            # If has Platinum -> Group B
            # If has Plus -> Group A
            # If has Enhanced -> Both have it.
            # Let's check overlap.
            # Simplified Logic from previous convo:
            # Group A: NO Platinum Code allowed.
            # Group B: HAS Platinum Code.
            # Control: Not in rule table.
            
            group_a_uids = []
            group_b_uids = []
            control_uids = []
            
            for uid in uids:
                rules = user_rules.get(uid, set())
                if not rules:
                    control_uids.append(uid)
                else:
                    # Check for Platinum
                    if not rules.isdisjoint(platinum_codes):
                        group_b_uids.append(uid)
                    else:
                        group_a_uids.append(uid)
            
            print(f"Group A Users: {len(group_a_uids)}")
            print(f"Group B Users: {len(group_b_uids)}")
            print(f"Control Users: {len(control_uids)}")
            
            # 2. Analyze Trials
            def analyze_group_trials(name, group_uids):
                if not group_uids:
                    print(f"\n{name}: No users.")
                    return uids
                
                print(f"\nAnalyzing {name} ({len(group_uids)} users)...")
                
                trial_uids = set()
                trial_24h = 0
                trial_60h = 0
                
                for i in range(0, len(group_uids), chunk_size):
                    chunk = group_uids[i:i+chunk_size]
                    ph = ', '.join(['%s'] * len(chunk))
                    
                    # Get FIRST valid trial order time for each user
                    sql = f"""
                        SELECT uid, MIN(pay_time)
                        FROM `order`
                        WHERE uid IN ({ph})
                          AND amount = 0
                          AND status = 1
                          # AND description IN ('Trial: 14 DAY', 'Promotion: 14 DAY') # Use strict? Or broad?
                          # Let's use Broad first to capture all trials, or strictly match US descriptions?
                          # US descriptions might be different. Let's assume amount=0 status=1 is trial.
                        GROUP BY uid
                    """
                    cur.execute(sql, chunk)
                    
                    for r in cur.fetchall():
                        uid = r[0]
                        trial_time = r[1]
                        reg_time = user_reg_map[uid]
                        
                        trial_uids.add(uid)
                        
                        diff_hours = (trial_time - reg_time) / 3600.0
                        if diff_hours <= 24:
                            trial_24h += 1
                        if diff_hours <= 60:
                            trial_60h += 1
                
                print(f"  Total Valid Trials: {len(trial_uids)} ({(len(trial_uids)/len(group_uids)*100):.2f}%)")
                print(f"  Within 24h: {trial_24h} ({(trial_24h/len(group_uids)*100):.2f}%)")
                print(f"  Within 60h: {trial_60h} ({(trial_60h/len(group_uids)*100):.2f}%)")
            
            analyze_group_trials("Group A", group_a_uids)
            analyze_group_trials("Group B", group_b_uids)
            analyze_group_trials("Control Group", control_uids)

    finally:
        conn.close()
