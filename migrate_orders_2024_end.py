import pymysql
import datetime
from decimal import Decimal
from config import create_ssh_tunnel, get_db_connection, VALID_PRODUCTS, PRODUCT_CODE_MAPPING

# Local DB Config
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def migrate_2024_end_orders(limit=None):
    # End of 2024: 2025-01-01 00:00:00 UTC
    END_2024 = 1735689600
    
    print("Connecting to remote database...")
    with create_ssh_tunnel() as server:
        remote_conn = get_db_connection()
        try:
            print(f"Connecting to local database '{LOCAL_DB_CONFIG['database']}'...")
            local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
            try:
                with local_conn.cursor() as local_cur:
                    batch_size = 1000
                    offset = 0
                    total_migrated = 0
                    
                    if limit and limit < batch_size:
                        batch_size = limit

                    while True:
                        print(f"Fetching batch: offset {offset}...")
                        with remote_conn.cursor(pymysql.cursors.DictCursor) as remote_cur:
                            # 1. Fetch cloud_info with associated order (closest pay_time to start_time)
                            # Joining with set_meal for product_name as fallback
                            ci_sql = f"""
                                WITH RankedOrders AS (
                                    SELECT 
                                        ci.id as ci_id, ci.uid, ci.uuid, ci.order_id as ci_oid, 
                                        ci.start_time as ci_start, ci.end_time as ci_end, ci.is_delete as ci_del,
                                        o.id as o_int_id, o.product_id as o_pid, o.amount as o_amt, o.status as o_status,
                                        o.pay_time as o_pay, o.submit_time as o_sub, o.subscribe_id as s_id,
                                        sm.name as sm_name, sm.time_unit as sm_unit, sm.time as sm_time,
                                        oai.amount_cny, oai.transaction_fee_cny, oai.model_code,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY ci.id 
                                            ORDER BY 
                                                CASE WHEN o.id IS NULL THEN 1 ELSE 0 END, 
                                                ABS(CAST(ci.start_time AS SIGNED) - CAST(o.pay_time AS SIGNED)) ASC,
                                                o.id DESC
                                        ) as o_rank
                                    FROM cloud_info ci
                                    LEFT JOIN `order` o ON ci.order_id = o.order_id
                                    LEFT JOIN set_meal sm ON o.product_id = sm.code
                                    LEFT JOIN order_amount_info oai ON o.id = oai.order_int_id
                                    WHERE ci.start_time < {END_2024} AND ci.end_time >= {END_2024}
                                )
                                SELECT * FROM RankedOrders WHERE o_rank = 1
                                LIMIT %s OFFSET %s
                            """
                            remote_cur.execute(ci_sql, (batch_size, offset))
                            ci_batch = remote_cur.fetchall()

                        if not ci_batch: break
                        
                        # Collect subscribe_ids to fetch history
                        s_ids = [row['s_id'] for row in ci_batch if row['s_id']]
                        history_map = {}
                        sub_agg_map = {}
                        
                        if s_ids:
                            with remote_conn.cursor(pymysql.cursors.DictCursor) as remote_cur:
                                # 2. Fetch Order History for has_trial and first_paid_time
                                s_ids_str = "', '".join([str(i) for i in s_ids])
                                hist_sql = f"""
                                    SELECT subscribe_id, amount, pay_time, submit_time
                                    FROM `order`
                                    WHERE status = 1 AND pay_time > 0 AND subscribe_id IN ('{s_ids_str}')
                                    ORDER BY subscribe_id, submit_time ASC
                                """
                                remote_cur.execute(hist_sql)
                                orders_hist = remote_cur.fetchall()
                                
                                # Process history per sub_id
                                temp_hist = {}
                                for oh in orders_hist:
                                    sid = oh['subscribe_id']
                                    if sid not in temp_hist: temp_hist[sid] = []
                                    temp_hist[sid].append(oh)
                                
                                for sid, hist in temp_hist.items():
                                    has_trial = 1 if hist[0]['amount'] == 0 else 0
                                    first_paid = None
                                    for oh in hist:
                                        if oh['amount'] > 0:
                                            first_paid = oh['pay_time']
                                            break
                                    history_map[sid] = {'has_trial': has_trial, 'first_paid': first_paid}

                                # 3. Fetch Subscribe Aggregations
                                # User: min index, max cancel, max next_billing
                                sub_sql = f"""
                                    SELECT subscribe_id, 
                                           MIN(initial_payment_time) as min_init, 
                                           MAX(cancel_time) as max_cancel, 
                                           MAX(next_billing_at) as max_next,
                                           SUBSTRING_INDEX(GROUP_CONCAT(status ORDER BY create_time DESC SEPARATOR '|'), '|', 1) as latest_status
                                    FROM subscribe
                                    WHERE subscribe_id IN ('{s_ids_str}')
                                    GROUP BY subscribe_id
                                """
                                remote_cur.execute(sub_sql)
                                subs_agg = remote_cur.fetchall()
                                for sa in subs_agg:
                                    sub_agg_map[sa['subscribe_id']] = sa

                        # 4. Final Batch Processing
                        batch_data = []
                        for row in ci_batch:
                            def ts_to_dt(ts):
                                try:
                                    if ts is None or ts == 0: return None
                                    iv = int(ts)
                                    return datetime.datetime.fromtimestamp(iv).strftime('%Y-%m-%d %H:%M:%S')
                                except: return None
                            
                            sid = row['s_id']
                            h_info = history_map.get(sid, {'has_trial': 0, 'first_paid': None})
                            s_info = sub_agg_map.get(sid, {})
                            
                            # product_name logic
                            pid = row['o_pid']
                            # If product_id in mapping, use that; else fallback to set_meal name
                            p_name = PRODUCT_CODE_MAPPING.get(pid, row['sm_name'] if row['sm_name'] else '')

                            # cycle_counts calculation
                            cc = 0
                            if h_info['first_paid'] and row['ci_end']:
                                days = (row['ci_end'] - h_info['first_paid']) / 86400.0
                                cc = int(round(days / 30.0))
                            
                            # plan_type and cycles (NOW FROM SET_MEAL.time)
                            plan_unit = row['sm_unit'] if row['sm_unit'] else ''
                            plan_time = int(row['sm_time']) if row['sm_time'] is not None else 1
                            
                            p_type = 0
                            if plan_unit in ['DAY', 'MONTH']: p_type = 1
                            elif plan_unit == 'YEAR': p_type = 2
                            
                            is_sixteen = 1 if p_name in VALID_PRODUCTS else 0
                            
                            # cny_amount
                            cny = (float(row['amount_cny']) - float(row['transaction_fee_cny'])) if (row['amount_cny'] is not None and row['transaction_fee_cny'] is not None) else 0.0

                            batch_data.append((
                                1, 1, row['uid'], row['uuid'], row['ci_oid'], p_name, float(row['o_amt']) if row['o_amt'] else 0.0, cny,
                                row['o_status'] if row['o_status'] is not None else 0, ts_to_dt(row['o_pay']), ts_to_dt(row['o_sub']),
                                sid if sid else '', int(s_info.get('latest_status', 0)), ts_to_dt(s_info.get('min_init')), 
                                ts_to_dt(s_info.get('max_cancel')), ts_to_dt(s_info.get('max_next')),
                                row['model_code'] if row['model_code'] else '',
                                is_sixteen, p_type, plan_unit, plan_time,
                                cc, ts_to_dt(row['ci_start']), ts_to_dt(row['ci_end']), 1 if row['ci_del'] else 0,
                                h_info['has_trial'], ts_to_dt(h_info['first_paid'])
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
                                has_trial, subscription_first_paid_time
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """
                        local_cur.executemany(insert_sql, batch_data)
                        local_conn.commit()
                        total_migrated += len(ci_batch)
                        offset += batch_size
                        if limit and total_migrated >= limit: break
                    
                    print(f"Migration completed. Total records migrated: {total_migrated}")
            finally: local_conn.close()
        finally: remote_conn.close()

if __name__ == "__main__":
    # Test with 100 rows
    migrate_2024_end_orders(limit=100)
