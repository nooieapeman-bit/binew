import pymysql
from config import create_ssh_tunnel, DB_USER, DB_PASS, DB_NAME

def describe_remote():
    with create_ssh_tunnel() as server:
        remote_conn = pymysql.connect(host='127.0.0.1', port=server.local_bind_port, user=DB_USER, password=DB_PASS, database=DB_NAME)
        with remote_conn.cursor() as cur:
            cur.execute("DESCRIBE user_device")
            rows = cur.fetchall()
            for r in rows:
                print(r)

if __name__ == "__main__":
    describe_remote()
