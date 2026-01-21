
import pymysql
from config import create_ssh_tunnel, get_db_connection

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Safer SQL with params
            sql = "SELECT order_id, subscribe_id FROM `order` WHERE uid = %s AND amount=0 AND status=1"
            cur.execute(sql, ('eul0q0r3c1l1t1c4',))
            rows = cur.fetchall()
            
            print('Orders for user eul0q0r3c1l1t1c4:')
            sub_ids = set()
            
            for r in rows:
                oid = r[0]
                sid = r[1]
                sub_ids.add(sid)
                print(f'Order: {oid} | SubscribeID: {sid}')
                
            if len(sub_ids) == 1:
                print('\nYES, they all share the SAME subscribe_id.')
            else:
                print(f'\nNO, there are {len(sub_ids)} different subscribe_ids.')
    finally:
        conn.close()
