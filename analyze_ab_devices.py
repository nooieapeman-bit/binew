
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
            # 1. Fetch UIDs for Groups
            cur.execute(f"""
                SELECT u.uid, sc.set_meal_code
                FROM user u
                JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """)
            user_codes = {}
            for row in cur.fetchall():
                uid, code = row
                if uid not in user_codes: user_codes[uid] = set()
                user_codes[uid].add(code)
            
            group_a_uids = []
            group_b_uids = []
            
            for uid, codes in user_codes.items():
                if codes.isdisjoint(platinum_codes):
                    group_a_uids.append(uid)
                else:
                    group_b_uids.append(uid)
            
            print(f'Group A Users: {len(group_a_uids)}')
            print(f'Group B Users: {len(group_b_uids)}')
            
            # 2. Analyze Device Bindings
            def analyze_devices(uids, group_name):
                print(f'\nAnalyzing {group_name} ({len(uids)} users)...')
                chunk_size = 1000
                multi_device_count = 0
                total_devices = 0
                max_devices = 0
                
                device_dist = {} # count -> num_users
                
                for i in range(0, len(uids), chunk_size):
                    chunk = uids[i:i+chunk_size]
                    ph = ', '.join(['%s'] * len(chunk))
                    
                    # Count valid devices (bind_type=1 usually means owner/active bind)
                    # Note: Need to check if is_delete/unbind logic applies. Assuming active snapshot or simple count.
                    # Usually user_device records history. Assuming we want current active?
                    # Or just "ever bound"? The prompt implies "bound", let's check count of records with bind_type=1.
                    # Better to check distinct did/uuid if possible, but count(*) with bind_type=1 is standard start.
                    
                    sql = f"""
                        SELECT uid, COUNT(*)
                        FROM user_device
                        WHERE uid IN ({ph})
                          AND bind_type = 1
                          # AND is_delete = 0  Let's assume we want current active bindings? or all attempts?
                          # Standard practice: check active bindings usually implies is_delete=0 or checking separate device status.
                          # Let's stick to prompt: 'bind_type = 1'
                        GROUP BY uid
                    """
                    cur.execute(sql, chunk)
                    
                    for r in cur.fetchall():
                        uid = r[0]
                        cnt = r[1]
                        
                        total_devices += cnt
                        if cnt > max_devices: max_devices = cnt
                        
                        if cnt > 1:
                            multi_device_count += 1
                        
                        if cnt not in device_dist: device_dist[cnt] = 0
                        device_dist[cnt] += 1
                        
                avg = total_devices / len(uids) if uids else 0
                pct_multi = (multi_device_count / len(uids) * 100) if uids else 0
                
                print(f"  Avg Devices/User: {avg:.2f}")
                print(f"  Max Devices:      {max_devices}")
                print(f"  Users with >1 Dev:{multi_device_count} ({pct_multi:.2f}%)")
                print(f"  Distribution: {sorted(device_dist.items())}")

            analyze_devices(group_a_uids, "Group A")
            analyze_devices(group_b_uids, "Group B")

    finally:
        conn.close()
