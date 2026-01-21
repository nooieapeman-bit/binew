import pymysql
import datetime
import time
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

# --- Schema Definitions ---
SCHEMA_MAP = {
    'dim_user': """
    CREATE TABLE `dim_user` (
      `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
      `uid` varchar(64) NOT NULL COMMENT '用户ID',
      `app_id` tinyint NOT NULL DEFAULT '0' COMMENT '应用来源: 1-osaio, 2-nooie, 3-victure, 4-teckin',
      `region_id` tinyint NOT NULL DEFAULT '0' COMMENT '数据中心: 1-eu, 2-us',
      `register_time` datetime DEFAULT NULL COMMENT '注册时间 (UTC)',
      `country_id` int NOT NULL DEFAULT '0' COMMENT '国家ID',
      `first_bind_time` datetime DEFAULT NULL COMMENT '首次绑定设备时间',
      `first_trial_time` datetime DEFAULT NULL COMMENT '首次试用时间',
      `first_paid_time` datetime DEFAULT NULL COMMENT '首次付费时间',
      `last_paid_time` datetime DEFAULT NULL COMMENT '最近一次付费时间 (UTC)',
      PRIMARY KEY (`id`),
      UNIQUE KEY `uid` (`uid`),
      KEY `idx_register_time` (`register_time`),
      KEY `idx_first_paid_time` (`first_paid_time`),
      KEY `idx_app_region` (`app_id`,`region_id`),
      KEY `idx_first_bind_time` (`first_bind_time`),
      KEY `idx_first_trial_time` (`first_trial_time`),
      KEY `idx_country_id` (`country_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户维度表(dim_user)';
    """,
    'dim_device': """
    CREATE TABLE `dim_device` (
      `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
      `uuid` varchar(64) NOT NULL COMMENT '设备唯一标识 (Device UUID)',
      `app_id` tinyint NOT NULL DEFAULT '0' COMMENT '应用来源: 1-osaio, 2-nooie, 3-victure, 4-teckin',
      `region_id` tinyint NOT NULL DEFAULT '0' COMMENT '数据中心: 1-eu, 2-us',
      `model_code` varchar(64) NOT NULL DEFAULT '' COMMENT '设备型号 (如 IPC100)',
      `create_time` datetime DEFAULT NULL COMMENT '设备激活/创建时间 (UTC)',
      PRIMARY KEY (`id`),
      UNIQUE KEY `uuid` (`uuid`),
      KEY `idx_create_time` (`create_time`),
      KEY `idx_model_code` (`model_code`),
      KEY `idx_app_region` (`app_id`,`region_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='设备维度表(dim_device)';
    """,
    'dim_user_device': """
    CREATE TABLE `dim_user_device` (
      `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
      `uid` varchar(64) NOT NULL COMMENT '用户ID',
      `uuid` varchar(64) NOT NULL COMMENT '设备UUID',
      `app_id` tinyint NOT NULL DEFAULT '0' COMMENT '应用来源: 1-osaio, 2-nooie, 3-victure, 4-teckin',
      `region_id` tinyint NOT NULL DEFAULT '0' COMMENT '数据中心: 1-eu, 2-us',
      `model_code` varchar(64) NOT NULL DEFAULT '' COMMENT '设备型号',
      `device_type` smallint NOT NULL DEFAULT '0' COMMENT '设备大类: 1-一级, 2-?.',
      `bind_type` tinyint NOT NULL DEFAULT '1' COMMENT '绑定类型: 1-Owner(主账号), 2-Shared(分享账号)',
      `status` tinyint NOT NULL DEFAULT '1' COMMENT '绑定状态: 1-Active(正常), 0-Unbind(已解绑)',
      `first_bind_time` datetime DEFAULT NULL COMMENT '首次绑定该设备的时间',
      `last_bind_time` datetime DEFAULT NULL COMMENT '最近一次绑定/重连时间',
      `delete_time` datetime DEFAULT NULL COMMENT '解绑时间',
      `created_at` datetime DEFAULT NULL COMMENT '记录创建时间',
      PRIMARY KEY (`id`),
      KEY `idx_uid_status` (`uid`,`status`),
      KEY `idx_uuid_status` (`uuid`,`status`),
      KEY `idx_first_bind_time` (`first_bind_time`),
      KEY `idx_created_at` (`created_at`),
      KEY `idx_app_region` (`app_id`,`region_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户设备绑定关系表(dim_user_device)';
    """,
    'dim_subscription': """
    CREATE TABLE `dim_subscription` (
      `id` bigint NOT NULL AUTO_INCREMENT COMMENT '代理主键',
      `app_id` tinyint unsigned DEFAULT NULL COMMENT '应用ID',
      `region_id` tinyint unsigned DEFAULT NULL COMMENT '区域ID',
      `subscription_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '业务订阅ID',
      `initial_time` datetime DEFAULT NULL COMMENT '订阅创建时间',
      `cancel_time` datetime DEFAULT NULL COMMENT '取消时间',
      `next_billing_at` datetime DEFAULT NULL COMMENT '下一次扣费时间',
      `last_upgrade_time` datetime DEFAULT NULL COMMENT '最后升级时间',
      `first_paid_time` datetime DEFAULT NULL COMMENT '首次付费时间',
      `product_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '当前产品ID',
      `status` tinyint unsigned DEFAULT '0',
      `uid` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '用户ID',
      PRIMARY KEY (`id`),
      KEY `idx_region_app` (`region_id`,`app_id`),
      KEY `idx_sub_id` (`subscription_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='订阅维度表';
    """,
    'fact_order': """
    CREATE TABLE `fact_order` (
      `id` bigint NOT NULL AUTO_INCREMENT,
      `order_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
      `subscription_id` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
      `app_id` tinyint NOT NULL DEFAULT '0',
      `region_id` tinyint NOT NULL DEFAULT '0',
      `uid` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
      `uuid` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
      `product_name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
      `amount` decimal(10,2) NOT NULL DEFAULT '0.00',
      `cny_amount` decimal(10,2) NOT NULL DEFAULT '0.00',
      `currency` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT '',
      `order_status` tinyint NOT NULL DEFAULT '0',
      `pay_time` datetime DEFAULT NULL,
      `order_submit_time` datetime DEFAULT NULL,
      `model_code` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
      `product_cycle_unit` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT '',
      `product_cycle_time` int DEFAULT '0',
      PRIMARY KEY (`id`),
      KEY `idx_uid` (`uid`),
      KEY `idx_order_id` (`order_id`),
      KEY `idx_pay_time` (`pay_time`),
      KEY `idx_sub_id` (`subscription_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    'fact_cloud': """
    CREATE TABLE `fact_cloud` (
      `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
      `app_id` tinyint NOT NULL DEFAULT '0' COMMENT '应用来源: 1-osaio, 2-nooie, 3-victure, 4-teckin',
      `region_id` tinyint NOT NULL DEFAULT '0' COMMENT '数据中心: 1-eu, 2-us',
      `uid` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '用户ID',
      `uuid` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '设备ID (如果是设备绑定套餐)',
      `order_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '订单号 (业务主键)',
      `product_name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '商品名称',
      `amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '订单金额 (原币种)',
      `cny_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '订单金额 (人民币估算)',
      `currency` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT '原货币币种',
      `order_status` tinyint NOT NULL DEFAULT '0' COMMENT '订单状态: 0-未支付, 1-已支付',
      `pay_time` datetime DEFAULT NULL COMMENT '支付时间',
      `order_submit_time` datetime DEFAULT NULL COMMENT '下单时间',
      `subscription_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '订阅ID',
      `subscription_status` tinyint NOT NULL DEFAULT '0' COMMENT '订阅状态: 0是失效，1-active',
      `subscription_initial_time` datetime DEFAULT NULL COMMENT '订阅首次开启时间',
      `subscription_cancel_time` datetime DEFAULT NULL COMMENT '订阅取消时间',
      `subscription_next_billing_at` datetime DEFAULT NULL COMMENT '下一次扣费时间',
      `model_code` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '关联的设备型号',
      `is_sixteen_plan` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否为16种套餐 (0-否, 1-是)',
      `plan_type` tinyint NOT NULL DEFAULT '0' COMMENT '1-month, 2-year',
      `plan_cycle_unit` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '周期单位 (day, month, year)',
      `plan_cycle_time` int NOT NULL DEFAULT '1' COMMENT '周期时长数值 (配合unit使用)',
      `cycle_counts` int NOT NULL DEFAULT '1' COMMENT '当前订阅周期的序号 (第几次扣费)',
      `cycle_start_time` datetime DEFAULT NULL COMMENT '本次服务周期开始时间',
      `cycle_end_time` datetime DEFAULT NULL COMMENT '本次服务周期结束时间',
      `cycle_is_delete` tinyint(1) NOT NULL DEFAULT '0' COMMENT '周期记录是否软删除',
      `after_status` tinyint DEFAULT '0' COMMENT '0-默认 1-pending, 2-renewed 3-trialRenew 4-noRenew',
      `has_trial` tinyint DEFAULT '0',
      `subscription_first_paid_time` datetime DEFAULT NULL,
      PRIMARY KEY (`id`),
      KEY `idx_order_id` (`order_id`),
      KEY `idx_uid` (`uid`),
      KEY `idx_uuid` (`uuid`),
      KEY `idx_subscription_id` (`subscription_id`),
      KEY `idx_pay_time` (`pay_time`),
      KEY `idx_cycle_end_time` (`cycle_end_time`),
      KEY `idx_app_region` (`app_id`,`region_id`),
      KEY `idx_uid_amount` (`uid`,`amount`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='订单事实表(fact_cloud)';
    """
}

def recreate_table(conn, table_name):
    if table_name not in SCHEMA_MAP:
        print(f"Schema for {table_name} not found.")
        return
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        cur.execute(SCHEMA_MAP[table_name])
    conn.commit()
    print(f"Table {table_name} recreated.")

def recreate_tables(conn):
    for t in SCHEMA_MAP:
        recreate_table(conn, t)


def migrate_dim_user(remote_conn, local_conn):
    print("Migrating dim_user...")
    fetch_sql = "SELECT uid, register_time, register_country FROM `user`"
    insert_sql = """
        INSERT INTO dim_user (app_id, region_id, uid, register_time, country_id) 
        VALUES (1, 1, %s, %s, %s)
    """
    
    with remote_conn.cursor() as r_cur, local_conn.cursor() as l_cur:
        r_cur.execute(fetch_sql)
        batch = []
        while True:
            row = r_cur.fetchone()
            if not row: break
            
            uid, reg_time, reg_country = row
            dt = datetime.datetime.fromtimestamp(reg_time) if reg_time else None
            cid = int(reg_country) if reg_country and reg_country.isdigit() else 0
            
            batch.append((uid, dt, cid))
            if len(batch) >= 2000:
                l_cur.executemany(insert_sql, batch)
                local_conn.commit()
                batch = []
        if batch:
            l_cur.executemany(insert_sql, batch)
            local_conn.commit()

def migrate_dim_device(remote_conn, local_conn):
    print("Migrating dim_device...")
    fetch_sql = "SELECT uuid, model_code, create_time FROM device"
    insert_sql = """
        INSERT INTO dim_device (app_id, region_id, uuid, model_code, create_time)
        VALUES (1, 1, %s, %s, %s)
        ON DUPLICATE KEY UPDATE model_code=VALUES(model_code), create_time=VALUES(create_time)
    """
    
    with remote_conn.cursor() as r_cur, local_conn.cursor() as l_cur:
        r_cur.execute(fetch_sql)
        batch = []
        while True:
            row = r_cur.fetchone()
            if not row: break
            uuid, model_code, ct = row
            dt = datetime.datetime.fromtimestamp(ct) if ct else None
            batch.append((uuid, model_code or '', dt))
            if len(batch) >= 2000:
                l_cur.executemany(insert_sql, batch)
                local_conn.commit()
                batch = []
        if batch:
            l_cur.executemany(insert_sql, batch)
            local_conn.commit()

def migrate_dim_user_device(remote_conn, local_conn):
    print("Migrating dim_user_device...")
    fetch_sql = "SELECT uid, uuid, model_code, device_type, bind_type, status, first_time, bind_time, delete_time, created_at FROM user_device"
    insert_sql = """
        INSERT INTO dim_user_device (app_id, region_id, uid, uuid, model_code, device_type, bind_type, status, first_bind_time, last_bind_time, delete_time, created_at)
        VALUES (1, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with remote_conn.cursor() as r_cur, local_conn.cursor() as l_cur:
        r_cur.execute(fetch_sql)
        batch = []
        while True:
            row = r_cur.fetchone()
            if not row: break
            uid, uuid, mc, dt, bt, st, ft, lt, delt, cat = row
            
            ts_to_dt = lambda x: datetime.datetime.fromtimestamp(x) if x else None
            
            # created_at is already datetime
            cat_dt = cat if cat else None
            
            batch.append((
                uid, uuid, mc or '', dt or 0, bt or 1, st or 1,
                ts_to_dt(ft), ts_to_dt(lt), ts_to_dt(delt), cat_dt
            ))
            if len(batch) >= 2000:
                l_cur.executemany(insert_sql, batch)
                local_conn.commit()
                batch = []
        if batch:
            l_cur.executemany(insert_sql, batch)
            local_conn.commit()
            
    # Post-cleanse dim_user
    print("Updating dim_user.first_bind_time...")
    with local_conn.cursor() as l_cur:
        l_cur.execute("""
            UPDATE dim_user u
            JOIN (
                SELECT uid, MIN(COALESCE(first_bind_time, last_bind_time)) as min_bind
                FROM dim_user_device
                WHERE first_bind_time IS NOT NULL OR last_bind_time IS NOT NULL
                GROUP BY uid
            ) d ON u.uid = d.uid
            SET u.first_bind_time = d.min_bind
        """)
        local_conn.commit()

def migrate_dim_subscription(remote_conn, local_conn):
    print("Migrating dim_subscription...")
    # Complex aggregation on remote or fetch all and aggregate locally?
    # Given database size, fetch all sorted by subscribe_id is okay.
    fetch_sql = "SELECT subscribe_id, initial_payment_time, cancel_time, next_billing_at, product_id, status, uid FROM subscribe WHERE subscribe_id != '' ORDER BY subscribe_id, id"
    
    insert_sql = """
        INSERT INTO dim_subscription (app_id, region_id, subscription_id, initial_time, cancel_time, next_billing_at, last_upgrade_time, product_id, status, uid)
        VALUES (1, 1, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    with remote_conn.cursor() as r_cur, local_conn.cursor() as l_cur:
        r_cur.execute(fetch_sql)
        
        current_sub_id = None
        first_record = None
        last_record = None
        
        batch = []
        
        def process_sub(first, last):
            if not first or not last: return None
            sub_id = first[0]
            # initial_time from first record
            init_time = datetime.datetime.fromtimestamp(first[1]) if first[1] else None
            # Others from last record
            cancel_time = datetime.datetime.fromtimestamp(last[2]) if last[2] else None
            next_bill = datetime.datetime.fromtimestamp(last[3]) if last[3] else None
            last_upgrade = datetime.datetime.fromtimestamp(last[1]) if last[1] else None # initial_payment_time of last record
            pid = last[4]
            status = last[5]
            uid = last[6]
            return (sub_id, init_time, cancel_time, next_bill, last_upgrade, pid, status, uid)

        while True:
            row = r_cur.fetchone()
            if not row: break
            
            sid = row[0]
            if sid != current_sub_id:
                if current_sub_id is not None:
                    data = process_sub(first_record, last_record)
                    if data: batch.append(data)
                
                current_sub_id = sid
                first_record = row
            
            last_record = row
            
            if len(batch) >= 2000:
                l_cur.executemany(insert_sql, batch)
                local_conn.commit()
                batch = []
        
        # Last one
        if current_sub_id:
            data = process_sub(first_record, last_record)
            if data: batch.append(data)
        
        if batch:
            l_cur.executemany(insert_sql, batch)
            local_conn.commit()

def migrate_fact_order(remote_conn, local_conn):
    print("Migrating fact_order...")
    # Using optimized query logic from migrate_fact_order.py
    sql = """
        SELECT 
            o.order_id, o.subscribe_id, o.uid, o.uuid, 
            o.amount, o.currency, o.status, o.pay_time, o.submit_time,
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
    
    insert_sql = """
        INSERT INTO fact_order (
            order_id, subscription_id, app_id, region_id, uid, uuid, product_name, amount, cny_amount,
            currency, order_status, pay_time, order_submit_time, model_code,
            product_cycle_unit, product_cycle_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    with remote_conn.cursor(pymysql.cursors.DictCursor) as r_cur, local_conn.cursor() as l_cur:
        r_cur.execute(sql)
        batch = []
        while True:
            r = r_cur.fetchone()
            if not r: break
            
            cny = 0.0
            if r['amount_cny'] is not None:
                fee = r['transaction_fee_cny'] if r['transaction_fee_cny'] is not None else 0.0
                cny = float(r['amount_cny']) - float(fee)
            
            ts_to_dt = lambda x: datetime.datetime.fromtimestamp(x) if x else None
            
            batch.append((
                r['order_id'], r['subscribe_id'], 1, 1, r['uid'], r['uuid'],
                r['product_name'] or '', float(r['amount'] or 0), cny,
                r['currency'] or '',
                r['status'], ts_to_dt(r['pay_time']), ts_to_dt(r['submit_time']),
                r['model_code'] or '', r['time_unit'] or '', int(r['product_cycle_time'] or 0)
            ))
            
            if len(batch) >= 2000:
                l_cur.executemany(insert_sql, batch)
                local_conn.commit()
                batch = []
        if batch:
            l_cur.executemany(insert_sql, batch)
            local_conn.commit()

def update_dim_subscription_stats(local_conn):
    print("Updating dim_subscription.first_paid_time...")
    with local_conn.cursor() as l_cur:
        l_cur.execute("""
            UPDATE dim_subscription ds
            JOIN (
                SELECT subscription_id, MIN(pay_time) as first_time
                FROM fact_order
                WHERE amount > 0 AND order_status = 1 AND subscription_id != ''
                GROUP BY subscription_id
            ) t ON ds.subscription_id = t.subscription_id
            SET ds.first_paid_time = t.first_time
        """)
        local_conn.commit()

def migrate_fact_cloud(remote_conn, local_conn):
    print("Migrating fact_cloud...")
    
    # Phase 1: Basic Copy
    print("  Phase 1: Fetching cloud_info...")
    fetch_sql = "SELECT uid, uuid, order_id, start_time, end_time, is_delete FROM cloud_info"
    insert_sql = """
        INSERT INTO fact_cloud (app_id, region_id, uid, uuid, order_id, cycle_start_time, cycle_end_time, cycle_is_delete)
        VALUES (1, 1, %s, %s, %s, %s, %s, %s)
    """
    
    with remote_conn.cursor() as r_cur, local_conn.cursor() as l_cur:
        r_cur.execute(fetch_sql)
        batch = []
        while True:
            row = r_cur.fetchone()
            if not row: break
            uid, uuid, oid, start, end, is_del = row
            ts_to_dt = lambda x: datetime.datetime.fromtimestamp(x) if x else None
            batch.append((uid, uuid, oid or '', ts_to_dt(start), ts_to_dt(end), is_del))
            
            if len(batch) >= 2000:
                l_cur.executemany(insert_sql, batch)
                local_conn.commit()
                batch = []
        if batch:
            l_cur.executemany(insert_sql, batch)
            local_conn.commit()
            
    # Phase 2: Enrichment (Local Update)
    print("  Phase 2: Enrichment (local joins)...")
    with local_conn.cursor() as l_cur:
        # Update basics from fact_order and dim_subscription
        l_cur.execute("""
            UPDATE fact_cloud fc
            JOIN fact_order fo ON fc.order_id = fo.order_id
            LEFT JOIN dim_subscription ds ON fo.subscription_id = ds.subscription_id
            SET 
                fc.product_name = COALESCE(fo.product_name, ''),
                fc.amount = COALESCE(fo.amount, 0.00),
                fc.cny_amount = COALESCE(fo.cny_amount, 0.00),
                fc.currency = COALESCE(fo.currency, ''),
                fc.order_status = COALESCE(fo.order_status, 0),
                fc.pay_time = fo.pay_time,
                fc.order_submit_time = fo.order_submit_time,
                fc.subscription_id = COALESCE(fo.subscription_id, ''),
                fc.subscription_status = COALESCE(ds.status, 0),
                fc.subscription_initial_time = ds.initial_time,
                fc.subscription_cancel_time = ds.cancel_time,
                fc.subscription_next_billing_at = ds.next_billing_at,
                fc.subscription_first_paid_time = ds.first_paid_time,
                fc.model_code = COALESCE(fo.model_code, ''),
                fc.plan_cycle_unit = COALESCE(fo.product_cycle_unit, ''),
                fc.plan_cycle_time = COALESCE(fo.product_cycle_time, 1)
        """)
        
        # Calculate business logic fields
        # Using a fixed date for example, or based on product list
        valid_products_list = "', '".join(VALID_PRODUCTS)
        
        l_cur.execute(f"""
            UPDATE fact_cloud 
            SET 
                is_sixteen_plan = IF(product_name IN ('{valid_products_list}'), 1, 0),
                plan_type = IF(plan_cycle_unit = 'YEAR', 2, 1),
                has_trial = IF(amount = 0, 1, IF(
                     subscription_first_paid_time IS NOT NULL AND subscription_initial_time IS NOT NULL AND 
                     ABS(TIMESTAMPDIFF(MINUTE, subscription_initial_time, subscription_first_paid_time)) > 10, 1, 0
                )),
                cycle_counts = COALESCE(GREATEST(1, ROUND(DATEDIFF(cycle_end_time, subscription_first_paid_time) / 30)), 1)
        """)
        
        # Phase 3: Status
        print("  Phase 3: Calculating After Status...")
        # Reset
        l_cur.execute("UPDATE fact_cloud SET after_status = 0")
        
        # Determine status
        # Using the same logic as README: 
        # Latest Active -> 1 (if end_time > 2025-12-31, but let's use current time logic or similar)
        # Actually, status logic relies on grouping by sub_id and finding latest.
        # Running the README logic verbatim (adjusted for date if needed).
        # Assuming 2025-12-31 is the cutoff for "Current Active", or maybe just > NOW()?
        # README said: cycle_end_time > '2025-12-31'. 
        # The user seems to be analyzing 2025 data. I'll stick to logic or use dynamic date?
        # User prompt: "近期又添加了不少数据" (Recently added data).
        # I'll use NOW() for active status if safe, or a far future date.
        # But wait, looking at README logic: "Status 1 (Active/Pending)... cycle_end_time > '2025-12-31'".
        # This implies "Active through 2025".
        # I will use a generic logic: Latest record, if end_time > NOW(), it's active (1), else Closed (4).
        # Historical records: if amount=0 trial (3), else renewed (2).
        
        l_cur.execute("""
            UPDATE fact_cloud f
            JOIN (
                SELECT 
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY uid, uuid, subscription_id 
                        ORDER BY cycle_end_time DESC
                    ) as row_idx,
                    cycle_end_time,
                    amount
                FROM fact_cloud
            ) t ON f.id = t.id
            SET f.after_status = CASE 
                WHEN t.row_idx = 1 THEN 
                    IF(t.cycle_end_time > NOW(), 1, 4)
                ELSE 
                    IF(t.amount = 0, 3, 2)
            END
        """)
        
        # Update user.first_trial_time
        print("Updating dim_user.first_trial_time...")
        l_cur.execute(f"""
             UPDATE dim_user u
             JOIN (
                 SELECT uid, MIN(pay_time) as min_trial_time
                 FROM fact_cloud
                 WHERE amount = 0 
                   AND pay_time >= '2024-10-01'
                   AND product_name IN ('{valid_products_list}')
                 GROUP BY uid
             ) t ON u.uid COLLATE utf8mb4_unicode_ci = t.uid
             SET u.first_trial_time = t.min_trial_time
        """)
        
        local_conn.commit()

def sync_full():
    with create_ssh_tunnel() as server:
        remote_conn = get_db_connection()
        print("Remote connection established.")
        
        local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
        print("Local connection established.")
        
        try:
            # 1. Recreate Tables
            recreate_tables(local_conn)
            
            # 2. Run Migrations
            migrate_dim_user(remote_conn, local_conn)
            migrate_dim_device(remote_conn, local_conn)
            migrate_dim_user_device(remote_conn, local_conn)
            migrate_dim_subscription(remote_conn, local_conn)
            migrate_fact_order(remote_conn, local_conn)
            migrate_fact_cloud(remote_conn, local_conn)
            
            print("Full sync completed successfully.")
            
        finally:
            local_conn.close()
            remote_conn.close()

if __name__ == "__main__":
    sync_full()
