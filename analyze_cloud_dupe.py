
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Get the 5 orders for this user
            sql = "SELECT order_id FROM `order` WHERE uid = %s AND amount=0 AND status=1"
            cur.execute(sql, ('eul0q0r3c1l1t1c4',))
            rows = cur.fetchall()
            order_ids = [r[0] for r in rows]
            
            print(f'User eul0q0r3c1l1t1c4 has {len(order_ids)} orders.')
            
            # 2. Join with cloud_info
            # cloud_info table has order_id ? Or usually linked via uid/subscription?
            # User request says: "order_id关联cloud_info表中的order_id"
            # Let's check cloud_info columns first to be sure
            cur.execute("SHOW COLUMNS FROM cloud_info")
            cols = [r[0] for r in cur.fetchall()]
            if 'order_id' not in cols:
                print(f"Warning: cloud_info does not appear to have 'order_id' column. Columns: {cols}")
                # It might be 'order_no' or linked via something else?
                # But let's assume user is right or logic connects them.
                # In migration script we used: `cloud_info` JOIN `fact_order` (local) but remote `cloud_info` structure?
                # Let's try standard query first.
            else:
                pass
            
            print('\nFetching cloud_info details for these orders...')
            
            placeholders = ', '.join(['%s'] * len(order_ids))
            sql_cloud = f"""
                SELECT order_id, start_time, end_time
                FROM cloud_info
                WHERE order_id IN ({placeholders})
            """
            
            # Note: order_ids are bytes in previous output?
            # pymysql usually handles bytes/str conversion if configured, but let's see.
            # If they are strings in DB but returned as bytes, passing bytes back is usually fine.
            
            cur.execute(sql_cloud, order_ids)
            cloud_rows = cur.fetchall()
            
            found_orders = set()
            
            for row in cloud_rows:
                oid = row[0]
                start_ts = row[1]
                end_ts = row[2]
                
                found_orders.add(oid)
                
                # Convert timestamps
                # Assuming int timestamps based on column names (time)
                try:
                    s_dt = datetime.datetime.fromtimestamp(start_ts) if start_ts else "None"
                    e_dt = datetime.datetime.fromtimestamp(end_ts) if end_ts else "None"
                except Exception as e:
                    s_dt = f"Error: {start_ts}"
                    e_dt = f"Error: {end_ts}"
                    
                print(f'Order: {oid}')
                print(f'  Start: {s_dt}')
                print(f'  End:   {e_dt}')
                print('-' * 20)
                
            # Check if any orders missing
            for oid in order_ids:
                if oid not in found_orders:
                    print(f'Order {oid} has NO record in cloud_info.')

    finally:
        conn.close()
