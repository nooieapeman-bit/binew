
# Need to calculate weighted avg.
# Group A/B detailed breakdown are NOT fully available yet from previous steps (only aggregates). 
# Need to fetch breakdown first for A/B like we did for Control.

import pymysql
import datetime
from sshtunnel import SSHTunnelForwarder

# Config
SSH_HOST = '34.197.218.151'
SSH_PORT = 18822
SSH_USER = 'ssh_tunnel_user'
SSH_PASS = 'z6TOa8uKwBDtlki4q10BVFuJ'

RDS_HOST = 'osaio-us-bicenter.c5iooxepal2e.us-east-1.rds.amazonaws.com'
RDS_PORT = 3306
DB_USER = 'readonly'
DB_PASS = 'WHFOWEIF##$#$...'
DB_NAME = 'bi_center'

US_PKG_CODES = {
    'Plus yearly': '3c0c2022ee0b12491352182c7057829c',
    'Plus monthly': '43c7756d70231fe227b32591b81aa68d',
    'Platinum yearly': '50e5b771de60f1816e964a7ef097f120',
    'Platinum monthly': 'c22f95e0eb3856e083ab265a97b5be9f',
}
plus_codes = set([US_PKG_CODES['Plus yearly'], US_PKG_CODES['Plus monthly']])
platinum_codes = set([US_PKG_CODES['Platinum yearly'], US_PKG_CODES['Platinum monthly']])

start_ts = 1767600000 # Approx start Jan 5
end_ts = 1768204800   # Approx end Jan 12 (Use actual from previous step)
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
            # 1. Fetch UIDs
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
            
            # 2. Analyze Price for Groups
            def calc_avg_price(name, group_uids):
                if not group_uids: return
                
                print(f"\nCalculating Avg Price for {name} ({len(group_uids)} users)...")
                
                # Fetch trial orders with product code
                chunk_size = 1000
                total_val = 0
                total_cnt = 0
                
                # We need prices. Let's fetch distinct prices once.
                # Or fetch price via JOIN in real-time.
                # JOIN set_meal_price is best.
                
                for i in range(0, len(group_uids), chunk_size):
                    chunk = group_uids[i:i+chunk_size]
                    ph = ', '.join(['%s'] * len(chunk))
                    
                    sql = f"""
                        SELECT s.name, smp.price
                        FROM `order` o
                        JOIN set_meal s ON o.product_id = s.code
                        JOIN set_meal_price smp ON s.code = smp.set_meal_code
                        WHERE o.uid IN ({ph})
                          AND o.amount = 0
                          AND o.status = 1
                          AND o.subscribe_id != ''
                          AND smp.currency = 'USD' # Ensure USD
                    """
                    # Note: One order might match multiple prices if price changed? No, usually one entry per currency.
                    # But join might duplicate if multiple price rows.
                    # Better to fetch DISTINCT pids and map prices in python to avoid join explosion.
                    
                    # Safer approach: Get Product IDs, then fetch prices.
                    sql_prods = f"""
                        SELECT DISTINCT o.subscribe_id, o.product_id
                        FROM `order` o
                        WHERE o.uid IN ({ph})
                          AND o.amount = 0
                          AND o.status = 1
                          AND o.subscribe_id != ''
                    """
                    cur.execute(sql_prods, chunk)
                    orders = cur.fetchall()
                    if not orders: continue
                    
                    sub_ids = [r[0] for r in orders]
                    pids = list(set([r[1] for r in orders]))
                    
                    # Fetch prices for pids
                    if pids:
                        pph = ', '.join(['%s'] * len(pids))
                        cur.execute(f"SELECT set_meal_code, price FROM set_meal_price WHERE set_meal_code IN ({pph}) AND currency='USD'", pids)
                        price_map = {r[0]: float(r[1]) for r in cur.fetchall()}
                        
                        for sub_id, pid in orders:
                            price = price_map.get(pid, 0)
                            total_val += price
                            total_cnt += 1
                
                avg = total_val / total_cnt if total_cnt else 0
                print(f"  Total Orders: {total_cnt}")
                print(f"  Avg Price:    {avg:.2f} USD")

            calc_avg_price("Group A", group_a_uids)
            calc_avg_price("Group B", group_b_uids)
            calc_avg_price("Control Group", control_uids)

    finally:
        conn.close()
