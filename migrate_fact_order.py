import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Local DB Config
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def migrate_fact_order_optimized():
    print("Connecting to remote database via SSH...")
    with create_ssh_tunnel(local_port=13411) as server:
        from config import DB_USER, DB_PASS, DB_NAME
        remote_conn = pymysql.connect(host='127.0.0.1', port=13411, user=DB_USER, password=DB_PASS, database=DB_NAME)
        try:
            print("Connecting to local database...")
            local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
            try:
                with local_conn.cursor() as local_cur:
                    print("Cleaning and fetching optimized order data...")
                    # Optimized Join Logic including subscription_id
                    sql = """
                        SELECT 
                            o.order_id, o.subscribe_id, o.uid, o.uuid, 
                            o.amount, o.status, o.pay_time, o.submit_time,
                            sm.name as product_name, sm.time_unit, sm.time as product_cycle_time,
                            dev.model_code,
                            oai.amount_cny, oai.transaction_fee_cny
                        FROM (
                            SELECT MAX(id) as latest_id
                            FROM `order`
                            WHERE order_id IS NOT NULL AND order_id != ''
                            GROUP BY order_id
                        ) t
                        JOIN `order` o ON t.latest_id = o.id
                        LEFT JOIN set_meal sm ON o.product_id = sm.code
                        LEFT JOIN device dev ON o.uuid = dev.uuid
                        LEFT JOIN order_amount_info oai ON o.id = oai.order_int_id
                    """
                    
                    with remote_conn.cursor(pymysql.cursors.DictCursor) as remote_cur:
                        remote_cur.execute(sql)
                        rows = remote_cur.fetchall()
                    
                    print(f"Fetched {len(rows)} unique orders. Preparing for insertion...")
                    
                    def ts_to_dt(ts):
                        if ts and ts > 0:
                            try:
                                return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                return None
                        return None

                    insert_sql = """
                        INSERT INTO fact_order (
                            order_id, subscription_id, app_id, region_id, uid, uuid, product_name, amount, cny_amount,
                            order_status, pay_time, order_submit_time, model_code,
                            product_cycle_unit, product_cycle_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    batch_data = []
                    for r in rows:
                        cny = 0.0
                        if r['amount_cny'] is not None:
                            fee = r['transaction_fee_cny'] if r['transaction_fee_cny'] is not None else 0.0
                            cny = float(r['amount_cny']) - float(fee)

                        batch_data.append((
                            r['order_id'],
                            r['subscribe_id'],
                            1, # app_id
                            1, # region_id
                            r['uid'],
                            r['uuid'],
                            r['product_name'] if r['product_name'] else '',
                            float(r['amount']) if r['amount'] is not None else 0.0,
                            cny,
                            r['status'],
                            ts_to_dt(r['pay_time']),
                            ts_to_dt(r['submit_time']),
                            r['model_code'] if r['model_code'] else '',
                            r['time_unit'] if r['time_unit'] else '',
                            int(r['product_cycle_time']) if r['product_cycle_time'] is not None else 0
                        ))
                    
                    print(f"Inserting into local DB...")
                    batch_size = 2000
                    for i in range(0, len(batch_data), batch_size):
                        local_cur.executemany(insert_sql, batch_data[i:i+batch_size])
                        local_conn.commit()
                        if (i // batch_size) % 10 == 0:
                            print(f"Inserted {i + len(batch_data[i:i+batch_size])} records...")
                    
                    print(f"Migration completed. Total records migrated: {len(batch_data)}")
                    
            finally:
                local_conn.close()
        finally:
            remote_conn.close()

if __name__ == "__main__":
    migrate_fact_order_optimized()
