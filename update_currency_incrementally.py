
import pymysql
from config import create_ssh_tunnel, get_db_connection

# Local DB Config
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def update_currency():
    with create_ssh_tunnel() as server:
        remote_conn = get_db_connection()
        local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
        
        try:
            with local_conn.cursor() as l_cur:
                # 1. Add currency column to local tables if they don't exist
                print("Checking/Adding currency columns to local tables...")
                
                l_cur.execute("SHOW COLUMNS FROM `fact_order` LIKE 'currency'")
                if not l_cur.fetchone():
                    print("Adding currency to fact_order...")
                    l_cur.execute("ALTER TABLE `fact_order` ADD COLUMN `currency` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT '' AFTER `cny_amount`")
                
                l_cur.execute("SHOW COLUMNS FROM `fact_cloud` LIKE 'currency'")
                if not l_cur.fetchone():
                    print("Adding currency to fact_cloud...")
                    l_cur.execute("ALTER TABLE `fact_cloud` ADD COLUMN `currency` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT '原货币币种' AFTER `cny_amount`")
                
                local_conn.commit()

                # 2. Fetch all order_id -> currency mappings from remote
                print("Fetching order_id -> currency mapping from remote...")
                # We only need to fetch for orders currently in our fact_order table
                l_cur.execute("SELECT order_id FROM fact_order WHERE currency = '' OR currency IS NULL")
                local_order_ids = [r[0] for r in l_cur.fetchall()]
                
                print(f"Found {len(local_order_ids)} orders needing currency update.")
                
                if local_order_ids:
                    chunk_size = 1000
                    for i in range(0, len(local_order_ids), chunk_size):
                        chunk = local_order_ids[i:i+chunk_size]
                        placeholders = ', '.join(['%s'] * len(chunk))
                        
                        # Fetch from remote
                        with remote_conn.cursor() as r_cur:
                            r_cur.execute(f"SELECT order_id, currency FROM `order` WHERE order_id IN ({placeholders})", chunk)
                            remote_data = r_cur.fetchall()
                            
                        # Update local fact_order
                        if remote_data:
                            update_sql = "UPDATE fact_order SET currency = %s WHERE order_id = %s"
                            # Note: execute bulk update
                            # Reordering remote_data to match (currency, order_id)
                            update_params = [(r[1], r[0]) for r in remote_data]
                            l_cur.executemany(update_sql, update_params)
                            local_conn.commit()
                            print(f"  Processed chunk {i//chunk_size + 1}, updated {len(remote_data)} rows.")

                # 3. Propagate to fact_cloud
                print("Propagating currency from fact_order to fact_cloud...")
                l_cur.execute("""
                    UPDATE fact_cloud fc
                    JOIN fact_order fo ON fc.order_id = fo.order_id
                    SET fc.currency = fo.currency
                    WHERE (fc.currency = '' OR fc.currency IS NULL) AND fo.currency != ''
                """)
                affected = l_cur.rowcount
                local_conn.commit()
                print(f"Propagated currency to {affected} rows in fact_cloud.")

            print("Incremental update completed.")
            
        finally:
            local_conn.close()
            remote_conn.close()

if __name__ == "__main__":
    update_currency()
