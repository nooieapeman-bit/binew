import datetime
from config import create_ssh_tunnel, get_db_connection, get_plan_type

def get_paid_active_subs_at(cursor, ts):
    sql = """
        SELECT ci.uid, ci.uuid, ci.end_time, o.product_name, ci.start_time
        FROM cloud_info ci
        JOIN `order` o ON ci.order_id = o.order_id
        WHERE ci.start_time <= %s AND ci.end_time > %s
          AND ci.is_delete = 0
          AND o.amount > 0
    """
    cursor.execute(sql, (ts, ts))
    rows = cursor.fetchall()
    result = []
    for r in rows:
        plan = get_plan_type(r[3] if r[3] else "")
        # If product_name is missing, fallback to duration
        if not r[3]:
            duration = (r[2] - r[4]) / 86400
            plan = 'Yearly' if duration > 300 else 'Monthly'
        result.append({'uid': r[0], 'uuid': r[1], 'end_time': r[2], 'plan': plan})
    return result

def run():
    tunnel = create_ssh_tunnel()
    tunnel.start()
    try:
        conn = get_db_connection(tunnel.local_bind_port)
        cursor = conn.cursor()
        
        # 2025-11-30 23:59:59
        target_dt = datetime.datetime(2025, 11, 30, 23, 59, 59)
        ts = int(target_dt.timestamp())
        
        print(f"Querying PAID Active Subscriptions at {target_dt} (TS: {ts})...")
        active_subs = get_paid_active_subs_at(cursor, ts)
        
        print(f"Result: {len(active_subs)} paid active subscriptions found.")
        
        # Breakdown by plan
        monthly = sum(1 for s in active_subs if s['plan'] == 'Monthly')
        yearly = sum(1 for s in active_subs if s['plan'] == 'Yearly')
        print(f"  Monthly: {monthly}")
        print(f"  Yearly: {yearly}")
        
        cursor.close()
        conn.close()
    finally:
        tunnel.stop()

if __name__ == "__main__":
    run()
