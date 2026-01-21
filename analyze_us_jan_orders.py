
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

# Jan 2026 Timeframe (Assuming UTC)
# Start: 2026-01-01 00:00:00 UTC
# End:   2026-02-01 00:00:00 UTC (Current date is around mid-Jan but let's just query >= Jan 1)

start_ts = 1767225600 # 2026-01-01 00:00:00 UTC
end_ts = 1769904000   # 2026-02-01 00:00:00 UTC

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
            print("Analyzing US Orders for Jan 2026...")
            
            # Query Logic:
            # Check distribution of product_name for orders in Jan 2026
            # Assuming we care about paid or ALL? Let's check status=1 (success)
            
            sql = f"""
                SELECT s.name, COUNT(*), SUM(IF(o.amount=0, 1, 0)) as trial_cnt, SUM(IF(o.amount>0, 1, 0)) as paid_cnt
                FROM `order` o
                JOIN set_meal s ON o.product_id = s.code
                WHERE o.pay_time >= {start_ts} AND o.pay_time < {end_ts}
                  AND o.status = 1
                GROUP BY s.name
                ORDER BY COUNT(*) DESC
            """
            cur.execute(sql)
            
            print(f"{'Product Name':<50} | {'Total':<8} | {'Trial (0)':<10} | {'Paid (>0)':<10}")
            print("-" * 85)
            
            total_orders = 0
            for row in cur.fetchall():
                pname = row[0]
                total = row[1]
                trials = row[2]
                paid = row[3]
                total_orders += total
                
                print(f"{pname[:50]:<50} | {total:<8} | {trials:<10} | {paid:<10}")
            
            print(f"\nTotal Jan Orders: {total_orders}")

    finally:
        conn.close()
