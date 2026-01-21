import pymysql
import datetime
from decimal import Decimal
from config import create_ssh_tunnel, get_db_connection

# User specified product list
SPECIFIED_VALID_PRODUCTS = [
    '14-day history event recording monthly',
    '30-day video history event recording pro monthly ',
    '7-day video history CVR recording monthly ',
    '30-day video history event recording monthly',
    '14-day video history CVR recording pro monthly',
    '30-day video history event recording AI monthly',
    '14-day video history event recording annually  ',
    '14-day video history CVR recording monthly  ',
    '30-day video history event recording pro annually',
    '7-day video history CVR recording annually',
    '14-day video history CVR recording pro annually   ',
    '30-day video history event recording annually',
    '30-day video history event recording AI annually',
    '14-day video history CVR recording AI monthly',
    '14-day video history CVR recording AI annually',
    '14-day video history CVR recording annually '
]

# Local DB Config
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def migrate_orders(limit=None):
    # Cutoff: 2024-11-29 00:00:00 UTC
    TS_CUTOFF = 1732838400
    
    print("Connecting to remote database...")
    with create_ssh_tunnel() as server:
        remote_conn = get_db_connection()
        try:
            print(f"Connecting to local database '{LOCAL_DB_CONFIG['database']}'...")
            local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
            try:
                # Use DictCursor for readability as requested
                with local_conn.cursor() as local_cur:
                    batch_size = 2000
                    offset = 0
                    total_migrated = 0
                    
                    if limit and limit < batch_size:
                        batch_size = limit

                    while True:
                        print(f"Fetching batch: offset {offset}...")
                        with remote_conn.cursor(pymysql.cursors.DictCursor) as remote_cur:
                            sql = f"""
                                WITH RankedData AS (
                                    SELECT 
                                        ci.id as ci_id, ci.uid, ci.uuid, ci.order_id as ci_oid, 
                                        ci.start_time as ci_start, ci.end_time as ci_end, ci.is_delete as ci_del,
                                        o.id as o_int_id, o.product_name, o.amount, o.status as o_status,
                                        o.pay_time, o.submit_time as o_submit, o.subscribe_id,
                                        s.status as s_status, s.initial_payment_time as s_initial,
                                        s.cancel_time as s_cancel, s.next_billing_at as s_next,
                                        s.cycles_unit, s.cycles_time,
                                        oai.amount_cny, oai.transaction_fee_cny, oai.model_code,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY ci.id 
                                            ORDER BY 
                                                CASE WHEN o.id IS NULL THEN 1 ELSE 0 END, 
                                                ABS(CAST(ci.start_time AS SIGNED) - CAST(o.pay_time AS SIGNED)) ASC,
                                                CASE WHEN s.id IS NULL THEN 1 ELSE 0 END, 
                                                s.cancel_time DESC,
                                                o.id DESC,
                                                s.id DESC
                                        ) as final_rank
                                    FROM cloud_info ci
                                    LEFT JOIN `order` o ON ci.order_id = o.order_id
                                    LEFT JOIN subscribe s ON o.subscribe_id = s.subscribe_id
                                    LEFT JOIN order_amount_info oai ON o.id = oai.order_int_id
                                    WHERE ci.end_time > {TS_CUTOFF}
                                )
                                SELECT * FROM RankedData 
                                WHERE final_rank = 1
                                LIMIT %s OFFSET %s
                            """
                            remote_cur.execute(sql, (batch_size, offset))
                            batch_rows = remote_cur.fetchall()
                        
                        if not batch_rows: break
                        
                        batch_data = []
                        for row in batch_rows:
                            def ts_to_dt(ts):
                                try:
                                    if ts is None: return None
                                    iv = int(ts)
                                    return datetime.datetime.fromtimestamp(iv).strftime('%Y-%m-%d %H:%M:%S') if iv > 0 else None
                                except: return None
                            
                            def safe_decode(val):
                                if isinstance(val, bytes):
                                    return val.decode('utf-8', errors='ignore')
                                return val

                            # Map readable values
                            uid = row['uid']
                            uuid = row['uuid']
                            order_id = safe_decode(row['ci_oid'])
                            product_name = row['product_name'] if row['product_name'] else ''
                            
                            # Decimal format for amounts
                            amount = Decimal(str(row['amount'] or 0.0))
                            net_cny = Decimal('0.00')
                            if row['amount_cny'] is not None and row['transaction_fee_cny'] is not None:
                                net_cny = Decimal(str(row['amount_cny'])) - Decimal(str(row['transaction_fee_cny']))

                            # logic for is_sixteen_plan
                            is_sixteen_plan = 1 if product_name in SPECIFIED_VALID_PRODUCTS else 0
                            
                            # logic for plan_type based on cycles_unit
                            plan_cycle_unit = row['cycles_unit'] if row['cycles_unit'] else ''
                            plan_type = 0
                            if plan_cycle_unit in ['DAY', 'MONTH']:
                                plan_type = 1
                            elif plan_cycle_unit == 'YEAR':
                                plan_type = 2
                            
                            # cycle_counts placeholder, to be updated later
                            cycle_counts = 0

                            batch_data.append((
                                1, 1, uid, uuid, order_id, product_name, amount, net_cny,
                                row['o_status'] if row['o_status'] is not None else 0,
                                ts_to_dt(row['pay_time']), ts_to_dt(row['o_submit']),
                                row['subscribe_id'] if row['subscribe_id'] else '',
                                row['s_status'] if row['s_status'] is not None else 0,
                                ts_to_dt(row['s_initial']), ts_to_dt(row['s_cancel']), ts_to_dt(row['s_next']),
                                row['model_code'] if row['model_code'] else '',
                                is_sixteen_plan, plan_type,
                                plan_cycle_unit, 
                                int(row['cycles_time']) if row['cycles_time'] is not None else 1,
                                cycle_counts,
                                ts_to_dt(row['ci_start']), ts_to_dt(row['ci_end']), 1 if row['ci_del'] else 0,
                                0 # after_status
                            ))
                        
                        insert_sql = """
                            INSERT INTO `fact_cloud` (
                                app_id, region_id, uid, uuid, order_id, product_name, amount, cny_amount,
                                order_status, pay_time, order_submit_time,
                                subscription_id, subscription_status, subscription_initial_time, 
                                subscription_cancel_time, subscription_next_billing_at,
                                model_code, is_sixteen_plan, plan_type,
                                plan_cycle_unit, plan_cycle_time, cycle_counts,
                                cycle_start_time, cycle_end_time, cycle_is_delete,
                                after_status
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        local_cur.executemany(insert_sql, batch_data)
                        local_conn.commit()
                        total_migrated += len(batch_rows)
                        offset += batch_size
                        if limit and total_migrated >= limit: break
                    
                    print(f"Migration completed. Total records migrated: {total_migrated}")
            finally: local_conn.close()
        finally: remote_conn.close()

if __name__ == "__main__":
    # Full migration
    migrate_orders(limit=None)
