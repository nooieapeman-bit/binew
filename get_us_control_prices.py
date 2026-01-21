
import pymysql
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

pkg_names = [
    '30-day video history event recording monthly',
    '14-day history event recording monthly',
    '14-day video history event recording annually',
    '30-day video history event recording annually',
    '30-day video history event recording AI monthly',
    '7-day video history CVR recording monthly',
    '14-day video history CVR recording monthly',
    '14-day video history CVR recording pro annually',
    '30-day video history event recording AI annually',
    '30-day video history event recording pro annually',
    '30-day video history event recording pro monthly',
    '14-day video history CVR recording AI annually',
    '14-day video history CVR recording AI monthly',
    '14-day video history CVR recording annually',
    '7-day video history CVR recording annually'
]

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
            print(f"Fetching USD prices for {len(pkg_names)} US packages...")
            
            # Use LIKE to match names
            placeholders = ' OR '.join([f"sm.name LIKE '{n}%'" for n in pkg_names])
            
            sql = f"""
                SELECT sm.name, smp.price
                FROM set_meal_price smp
                JOIN set_meal sm ON smp.set_meal_code = sm.code
                WHERE ({placeholders})
                  AND smp.currency = 'USD'
            """
            cur.execute(sql)
            results = cur.fetchall()
            
            # Map names
            price_map = {}
            for r in results:
                db_name = r[0].strip()
                price = float(r[1])
                price_map[db_name] = price
            
            print(f"\nUS Package Prices (USD):")
            print(f"{'Package Name':<50} | {'Price (USD)':<10}")
            print("-" * 65)
            
            for p in pkg_names:
                match_price = 'N/A'
                # Fuzzy match back
                for k, v in price_map.items():
                    if k.startswith(p) or p.startswith(k):
                        match_price = v
                        break
                print(f"{p:<50} | {match_price:<10}")

    finally:
        conn.close()
