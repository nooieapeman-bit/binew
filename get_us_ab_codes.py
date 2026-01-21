
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

# Target Package Names
# Note: 'Premium' was also seen in list, but user asked for Enhanced, Plus, Platinum.
target_names = [
    'Enhanced monthly',
    'Enhanced yearly',
    'Plus monthly',
    'Plus yearly',
    'Platinum monthly',
    'Platinum yearly'
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
            print("Fetching US Product Codes...")
            
            # Using LIKE to avoid minor whitespace issues
            placeholders = ' OR '.join([f"name LIKE '{n}%'" for n in target_names])
            sql = f"SELECT name, code FROM set_meal WHERE {placeholders}"
            
            cur.execute(sql)
            
            print(f"{'Product Name':<30} | {'Code'}")
            print("-" * 65)
            
            code_map = {}
            for row in cur.fetchall():
                name = row[0].strip()
                code = row[1]
                # Filter to only keep exact matches or very close
                if name in target_names:
                    print(f"{name:<30} | {code}")
                    code_map[name] = code
            
            # Print Python Dict for copy-paste
            print("\nPYTHON_DICT = {")
            for k, v in code_map.items():
                print(f"    '{k}': '{v}',")
            print("}")

    finally:
        conn.close()
