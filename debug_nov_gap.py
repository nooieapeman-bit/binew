import datetime
from config import get_db_connection

def analyze_nov_gap():
    conn = get_db_connection(3307)
    cursor = conn.cursor()
    
    # Nov 2025 Window
    start_ts = 1730419200 # 2025-11-01 00:00:00
    end_ts = 1733011199   # 2025-11-30 23:59:59
    
    print("Step 1: Fetching all 1st Period Paid Orders in Nov 2025...")
    # SQL to find 1st Period Paid Orders (abs first paid order for subscribe_id)
    sql_first = """
        SELECT o.order_id, o.uid, o.subscribe_id, o.amount, o.pay_time
        FROM (
            SELECT order_id, uid, subscribe_id, amount, pay_time,
                   ROW_NUMBER() OVER(PARTITION BY subscribe_id ORDER BY pay_time ASC) as rn
            FROM `order`
            WHERE amount > 0 AND status = 1 
        ) o
        WHERE o.rn = 1 AND o.pay_time >= %s AND o.pay_time <= %s
    """
    cursor.execute(sql_first, (start_ts, end_ts))
    first_orders = cursor.fetchall()
    print(f"Total 1st Period Paid Orders found in Nov: {len(first_orders)}")

    # Map order_id to (uid, subscribe_id)
    first_order_ids = []
    for r in first_orders:
        oid = r[0]
        if isinstance(oid, bytes): oid = oid.decode('utf-8')
        first_order_ids.append(oid)

    # Step 2: Check cloud_info existence
    print("\nStep 2: Checking cloud_info for these orders...")
    chunk_size = 500
    cloud_info_records = []
    for i in range(0, len(first_order_ids), chunk_size):
        chunk = first_order_ids[i:i+chunk_size]
        fmt = ','.join(['%s'] * len(chunk))
        sql_ci = f"SELECT order_id, uid, uuid, start_time, end_time, is_delete FROM cloud_info WHERE order_id IN ({fmt})"
        cursor.execute(sql_ci, chunk)
        cloud_info_records.extend(cursor.fetchall())
    
    print(f"Total cloud_info records found: {len(cloud_info_records)}")
    
    # Analyze cloud_info status
    deleted = 0
    not_active_at_month_end = 0
    for r in cloud_info_records:
        if r[5] == 1: deleted += 1
        elif r[4] <= end_ts: # Expired before Nov 30
            not_active_at_month_end += 1
            
    print(f" - is_delete = 1: {deleted}")
    print(f" - Expired before end of Nov (not in month-end count): {not_active_at_month_end}")

    # Step 3: Check overlap with Oct Active (Renewals)
    print("\nStep 3: Checking if any of these are 'Renewals' (Replacing Oct Active)...")
    oct_end_ts = 1730419199 # 2025-10-31 23:59:59
    sql_oct_active = """
        SELECT uid, uuid, end_time FROM cloud_info 
        WHERE start_time <= %s AND end_time > %s AND is_delete = 0
    """
    cursor.execute(sql_oct_active, (oct_end_ts, oct_end_ts))
    oct_active = cursor.fetchall()
    oct_active_keys = set([(r[0], r[1]) for r in oct_active])
    
    renewal_count = 0
    for r in cloud_info_records:
        if r[5] == 0: # Not deleted
            key = (r[1], r[2]) # (uid, uuid)
            if key in oct_active_keys:
                renewal_count += 1
                
    print(f" - Overlap with Oct Active (Counted as 'Renewed'): {renewal_count}")

    conn.close()

if __name__ == "__main__":
    analyze_nov_gap()
