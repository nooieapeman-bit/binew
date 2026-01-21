import datetime
import calendar
from config import VALID_PRODUCTS

def get_registration_device_analysis(cursor):
    print("Calculating Registration & Device Binding...")
    reg_analysis = []
    
    for month_idx in range(1, 13):
        # Time Window for Registration (UTC)
        first_day = datetime.datetime(2025, month_idx, 1)
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        last_day_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        start_ts = int(first_day.timestamp())
        end_ts = int(last_day_dt.timestamp())
        month_str = f"2025-{month_idx:02d}"
        
        # 1. Total Registered Users in Month
        sql_reg = """
            SELECT uid, register_time FROM `user` 
            WHERE register_time >= %s AND register_time <= %s
        """
        cursor.execute(sql_reg, (start_ts, end_ts))
        rows = cursor.fetchall()
        reg_uids = [r[0] for r in rows]
        user_reg_time = {r[0]: r[1] for r in rows}
        total_reg = len(reg_uids)
        
        if total_reg == 0:
            reg_analysis.append({
                'month': month_str,
                'totalReg': 0,
                'boundDevice': 0,
                'boundDevicePct': 0,
                'ownerDevice': 0,
                'ownerDevicePct': 0,
                'ownerTrials': 0,
                'ownerTrialsPct': 0,
                'ownerTrials30d': 0,
                'ownerTrials30dPct': 0
            })
            continue
            
        # 2. Check Device Binding, Trials & Paid Status
        bound_device_count = 0
        owner_device_count = 0
        owner_trial_count = 0
        owner_trial_30d_count = 0
        paid_user_count = 0
        
        chunk_size = 2000
        for i in range(0, total_reg, chunk_size):
            chunk = reg_uids[i:i+chunk_size]
            if not chunk: continue
            
            fmt = ','.join(['%s'] * len(chunk))
            
            # Count users with ANY device
            sql_any = f"SELECT COUNT(DISTINCT uid) FROM user_device WHERE uid IN ({fmt})"
            cursor.execute(sql_any, chunk)
            bound_device_count += cursor.fetchone()[0]
            
            # Fetch users with Owner device (bind_type=1)
            sql_owner_uids = f"SELECT DISTINCT uid FROM user_device WHERE bind_type=1 AND uid IN ({fmt})"
            cursor.execute(sql_owner_uids, chunk)
            owner_uids = [r[0] for r in cursor.fetchall()]
            
            chunk_owners = len(owner_uids)
            owner_device_count += chunk_owners
            
            # Count users who HAVE paid at least once
            sql_paid = f"SELECT COUNT(DISTINCT uid) FROM `order` WHERE status=1 AND amount>0 AND product_name IN %s AND uid IN ({fmt})"
            cursor.execute(sql_paid, (VALID_PRODUCTS, *chunk))
            paid_user_count += cursor.fetchone()[0]

            # Identify Trials among Owners
            if chunk_owners > 0:
                fmt_owners = ','.join(['%s'] * chunk_owners)
                # Check for trials (amount=0, status=1, valid product)
                sql_trials = f"""
                    SELECT uid, MIN(pay_time) FROM `order` 
                    WHERE amount=0 AND status=1 
                    AND product_name IN %s
                    AND uid IN ({fmt_owners})
                    GROUP BY uid
                """
                cursor.execute(sql_trials, (VALID_PRODUCTS, *owner_uids))
                trial_results = cursor.fetchall()
                
                owner_trial_count += len(trial_results)
                
                for t_uid, t_pay_time in trial_results:
                    reg_ts = user_reg_time.get(t_uid)
                    if reg_ts and (t_pay_time - reg_ts) <= (30 * 86400):
                        owner_trial_30d_count += 1

        never_paid_count = total_reg - paid_user_count

        reg_analysis.append({
            'month': month_str,
            'totalReg': total_reg,
            'neverPaid': never_paid_count,
            'neverPaidPct': round(never_paid_count / total_reg * 100, 2) if total_reg > 0 else 0,
            'boundDevice': bound_device_count,
            'boundDevicePct': round(bound_device_count / total_reg * 100, 2) if total_reg > 0 else 0,
            'ownerDevice': owner_device_count,
            'ownerDevicePct': round(owner_device_count / total_reg * 100, 2) if total_reg > 0 else 0,
            'ownerTrials': owner_trial_count,
            'ownerTrialsPct': round(owner_trial_count / owner_device_count * 100, 2) if owner_device_count > 0 else 0,
            'ownerTrials30d': owner_trial_30d_count,
            'ownerTrials30dPct': round(owner_trial_30d_count / owner_device_count * 100, 2) if owner_device_count > 0 else 0
        })
        
    return reg_analysis
