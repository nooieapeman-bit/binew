
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            uid = 'eul6w5w0w1l0r6t1'
            
            # 1. Order History
            sql = f"""
                SELECT o.pay_time, s.name, o.subscribe_id, o.amount, o.description, o.status
                FROM `order` o
                LEFT JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid = '{uid}'
                ORDER BY o.pay_time
            """
            cur.execute(sql)
            
            print(f'Order History for {uid}:')
            sub_ids = set()
            for row in cur.fetchall():
                pt, pname, sid, amt, desc, stat = row
                pt_dt = datetime.datetime.fromtimestamp(pt) if pt else 'None'
                print(f'{pt_dt} | {pname:<16} | Amt: {amt} | Stat: {stat} | Desc: {desc}')
                if sid: sub_ids.add(sid)

            # 2. Subscription Status
            print('\nSubscription Status:')
            if sub_ids:
                sph = ', '.join(['%s'] * len(sub_ids))
                cur.execute(f"SELECT subscribe_id, cancel_time FROM subscribe WHERE subscribe_id IN ({sph})", list(sub_ids))
                # Note: multiple rows possible per sub_id
                sub_status_map = {}
                for r in cur.fetchall():
                    sid, ctime = r
                    # Logic: if any row has cancel_time, it was cancelled? 
                    # Usually we want the latest state. 
                    # If multiple rows, we usually assume the latest one matters.
                    # But if a sub is cancelled, it stays cancelled.
                    if sid not in sub_status_map or (ctime and ctime > 0):
                        sub_status_map[sid] = ctime
                
                for sid, ctime in sub_status_map.items():
                    c_dt = datetime.datetime.fromtimestamp(ctime) if ctime and ctime > 0 else 'Active'
                    print(f'SubID: {sid} | Cancelled: {c_dt}')

    finally:
        conn.close()
