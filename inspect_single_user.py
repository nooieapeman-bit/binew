
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            uid = 'eul7l2r1a1l1r1a9'
            
            # Using proper SQL syntax with aliases
            sql = f"""
                SELECT o.pay_time, s.name, o.subscribe_id, o.amount, o.description, o.status
                FROM `order` o
                LEFT JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid = '{uid}'
                ORDER BY o.pay_time
            """
            cur.execute(sql)
            
            print(f'Order History for {uid}:')
            for row in cur.fetchall():
                pt, pname, sid, amt, desc, stat = row
                pt_dt = datetime.datetime.fromtimestamp(pt) if pt else 'None'
                print(f'{pt_dt} | {pname:<16} | Amt: {amt} | Stat: {stat} | Desc: {desc}')

            # 2. Check cancellation status of subscriptions
            print('\nSubscription Status:')
            cur.execute(f"SELECT subscribe_id, cancel_time FROM subscribe WHERE uid = '{uid}'")
            for row in cur.fetchall():
                sid, ctime = row
                c_dt = datetime.datetime.fromtimestamp(ctime) if ctime else 'Active'
                print(f'SubID: {sid} | Cancelled: {c_dt}')

    finally:
        conn.close()
