
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            uids = ['eul1s9r0s1l0t3s9', 'eul3m7m6s1l1c0l5', 'eul6s3a1s1l2f1q9', 'eul7w1m0q1l1r8w5']
            
            placeholders = ', '.join(['%s'] * len(uids))
            
            # Using table alias `o` properly for table `order`
            sql = f"""
                SELECT o.uid, o.pay_time, s.name, o.subscribe_id, o.amount, o.description, o.status
                FROM `order` o
                JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid IN ({placeholders})
                  AND o.amount = 0
                  AND o.status = 1
                ORDER BY o.uid, o.pay_time
            """
            cur.execute(sql, uids)
            
            curr_uid = None
            for row in cur.fetchall():
                uid, pt, pname, sid, amt, desc, stat = row
                pt_dt = datetime.datetime.fromtimestamp(pt)
                
                if uid != curr_uid:
                    print(f'\n=== User {uid} ===')
                    curr_uid = uid
                    
                print(f'{pt_dt} | {pname:<16} | SubID: {sid:<20} | Desc: {desc}')
    finally:
        conn.close()
