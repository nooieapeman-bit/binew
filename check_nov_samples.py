import datetime
from config import get_db_connection

def check_overlap_sample():
    conn = get_db_connection(3307)
    cursor = conn.cursor()
    
    start_ts = 1730419200 # 2025-11-01
    end_ts = 1733011199   # 2025-11-30
    oct_end_ts = 1730419199
    
    print("Finding sample orders that are 'First Period Paid' but OVERLAP with Oct active...")
    sql = """
        SELECT o.order_id, o.uid, o.subscribe_id, o.pay_time, ci.uuid, ci.start_time, ci.end_time
        FROM (
            SELECT order_id, uid, subscribe_id, amount, pay_time,
                   ROW_NUMBER() OVER(PARTITION BY subscribe_id ORDER BY pay_time ASC) as rn
            FROM `order`
            WHERE amount > 0 AND status = 1 
        ) o
        JOIN cloud_info ci ON o.order_id = ci.order_id
        JOIN (
            SELECT uid, uuid FROM cloud_info 
            WHERE start_time <= %s AND end_time > %s AND is_delete = 0
        ) oct ON ci.uid = oct.uid AND ci.uuid = oct.uuid
        WHERE o.rn = 1 AND o.pay_time >= %s AND o.pay_time <= %s
        LIMIT 5
    """
    cursor.execute(sql, (oct_end_ts, oct_end_ts, start_ts, end_ts))
    samples = cursor.fetchall()
    
    for s in samples:
        print(f"\nOrder ID: {s[0]}")
        print(f"UID: {s[1]}, UUID: {s[4]}")
        print(f"Pay Time: {datetime.datetime.fromtimestamp(s[3])}")
        
        # Check Oct active record for this user/device
        sql_check = "SELECT order_id, start_time, end_time, is_delete FROM cloud_info WHERE uid=%s AND uuid=%s AND start_time <= %s AND end_time > %s"
        cursor.execute(sql_check, (s[1], s[4], oct_end_ts, oct_end_ts))
        prev = cursor.fetchone()
        if prev:
            p_oid = prev[0]
            if isinstance(p_oid, bytes): p_oid = p_oid.decode('utf-8')
            print(f"REPLACES Oct Active Order: {p_oid}")
            print(f"Oct Active Expired at: {datetime.datetime.fromtimestamp(prev[2])}")
            
            # Check if that prev order was a trial
            sql_order = "SELECT amount FROM `order` WHERE order_id = %s"
            cursor.execute(sql_order, (p_oid,))
            o_info = cursor.fetchone()
            if o_info:
                print(f"Oct Active Order Amount: {o_info[0]} (0 means it was a Trial)")

    conn.close()

if __name__ == "__main__":
    check_overlap_sample()
