
import pymysql
from sshtunnel import SSHTunnelForwarder

# US Config provided by user
SSH_HOST = '34.197.218.151'
SSH_PORT = 18822
SSH_USER = 'ssh_tunnel_user'
SSH_PASS = 'z6TOa8uKwBDtlki4q10BVFuJ'

RDS_HOST = 'osaio-us-bicenter.c5iooxepal2e.us-east-1.rds.amazonaws.com'
RDS_PORT = 3306
DB_USER = 'readonly'
# Using the same password string as seen in config.py
DB_PASS = 'WHFOWEIF##$#$...'
DB_NAME = 'bi_center'

def test_connection():
    print("Attempting to connect to US Database via SSH...")
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASS,
            remote_bind_address=(RDS_HOST, RDS_PORT),
            allow_agent=False
        ) as tunnel:
            print(f"SSH Tunnel established. Local bind port: {tunnel.local_bind_port}")
            
            conn = pymysql.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME
            )
            print("Database connection established!")
            
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                ver = cur.fetchone()
                print(f"Server Version: {ver[0]}")
                
            conn.close()
            print("Connection closed successfully.")
            
    except Exception as e:
        print(f"Connection Failed: {e}")

if __name__ == '__main__':
    test_connection()
