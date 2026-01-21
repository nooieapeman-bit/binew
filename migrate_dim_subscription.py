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

def migrate_dim_subscription():
    print("Connecting to remote database via SSH...")
    # Use a specific port to avoid any potential conflict
    with create_ssh_tunnel(local_port=13366) as server:
        from config import DB_USER, DB_PASS, DB_NAME
        remote_conn = pymysql.connect(host='127.0.0.1', port=13366, user=DB_USER, password=DB_PASS, database=DB_NAME)
        try:
            print("Connecting to local database...")
            local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
            try:
                with local_conn.cursor() as local_cur:
                    print("Cleaning and fetching optimized subscribe data...")
                    # Optimized Join Logic
                    sql = """
                        SELECT 
                            t.subscribe_id,
                            s_last.uid,
                            s_last.product_id,
                            s_last.status,
                            s_first.initial_payment_time as initial_time,
                            s_last.cancel_time,
                            s_last.next_billing_at,
                            s_last.initial_payment_time as last_upgrade_time
                        FROM (
                            SELECT subscribe_id, MIN(id) as first_id, MAX(id) as last_id
                            FROM `subscribe`
                            WHERE subscribe_id != ''
                            GROUP BY subscribe_id
                        ) t
                        JOIN `subscribe` s_first ON t.first_id = s_first.id
                        JOIN `subscribe` s_last ON t.last_id = s_last.id
                    """
                    
                    with remote_conn.cursor(pymysql.cursors.DictCursor) as remote_cur:
                        remote_cur.execute(sql)
                        rows = remote_cur.fetchall()
                    
                    print(f"Fetched {len(rows)} unique subscriptions. Preparing for insertion...")
                    
                    def ts_to_dt(ts):
                        if ts and ts > 0:
                            try:
                                return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                return None
                        return None

                    insert_sql = """
                        INSERT INTO dim_subscription (
                            app_id, region_id, subscription_id, uid, product_id, status,
                            initial_time, cancel_time, next_billing_at, last_upgrade_time, first_paid_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    batch_data = []
                    for r in rows:
                        batch_data.append((
                            1, 1, r['subscribe_id'], r['uid'], r['product_id'], r['status'],
                            ts_to_dt(r['initial_time']),
                            ts_to_dt(r['cancel_time']),
                            ts_to_dt(r['next_billing_at']),
                            ts_to_dt(r['last_upgrade_time']),
                            None
                        ))

                    
                    print(f"Inserting into local DB...")
                    batch_size = 2000
                    for i in range(0, len(batch_data), batch_size):
                        local_cur.executemany(insert_sql, batch_data[i:i+batch_size])
                        local_conn.commit()
                        if (i // batch_size) % 5 == 0:
                            print(f"Inserted {i + len(batch_data[i:i+batch_size])} records...")
                    
                    print(f"Migration completed. Total records migrated: {len(batch_data)}")
                    
            finally:
                local_conn.close()
        finally:
            remote_conn.close()

if __name__ == "__main__":
    migrate_dim_subscription()
