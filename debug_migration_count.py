from config import create_ssh_tunnel, get_db_connection
import datetime

def debug_discrepancy():
    TS_CUTOFF = 1732838400
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 1. Base count
                cur.execute("SELECT COUNT(*) FROM cloud_info WHERE end_time > %s", (TS_CUTOFF,))
                base_count = cur.fetchone()[0]
                print(f"Base count (cloud_info end_time > cutoff): {base_count}")

                # 2. Ranked with both filters
                sql_bug = f"""
                    WITH RankedData AS (
                        SELECT 
                            ci.id as ci_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY ci.id 
                                ORDER BY 
                                    CASE WHEN o.id IS NULL THEN 1 ELSE 0 END,
                                    ABS(CAST(ci.start_time AS SIGNED) - CAST(o.pay_time AS SIGNED)) ASC,
                                    o.id DESC
                            ) as o_rank,
                            ROW_NUMBER() OVER (
                                PARTITION BY ci.id 
                                ORDER BY 
                                    CASE WHEN s.id IS NULL THEN 1 ELSE 0 END,
                                    s.cancel_time DESC,
                                    s.id DESC
                            ) as s_rank
                        FROM cloud_info ci
                        LEFT JOIN `order` o ON ci.order_id = o.order_id
                        LEFT JOIN subscribe s ON o.subscribe_id = s.subscribe_id
                        WHERE ci.end_time > {TS_CUTOFF}
                    )
                    SELECT COUNT(*) FROM RankedData WHERE o_rank = 1 AND s_rank = 1
                """
                cur.execute(sql_bug)
                bug_count = cur.fetchone()[0]
                print(f"Count with o_rank=1 AND s_rank=1: {bug_count}")

                # 3. Ranked with single combined rank
                sql_fixed_rank = f"""
                    WITH RankedData AS (
                        SELECT 
                            ci.id as ci_id,
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
                        WHERE ci.end_time > {TS_CUTOFF}
                    )
                    SELECT COUNT(*) FROM RankedData WHERE final_rank = 1
                """
                cur.execute(sql_fixed_rank)
                fixed_count = cur.fetchone()[0]
                print(f"Count with single final_rank=1: {fixed_count}")

        finally:
            conn.close()

if __name__ == "__main__":
    debug_discrepancy()
