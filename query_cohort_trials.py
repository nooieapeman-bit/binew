import datetime
import calendar
from config import VALID_PRODUCTS, get_plan_type

def get_cohort_analysis(cursor):
    print("Calculating Cohort Analysis (Unaggregated)...")
    
    # Pre-fetch Pay Info for Cohort Logic
    sql_conv_base = """
        SELECT 
            subscribe_id,
            MIN(pay_time) as first_paid_time,
            product_name
        FROM `order`
        WHERE amount > 0 
          AND status = 1
          AND subscribe_id != ''
          AND product_name IN %s
        GROUP BY subscribe_id, product_name
        HAVING first_paid_time >= UNIX_TIMESTAMP('2025-01-01 00:00:00')
    """ 
    cursor.execute(sql_conv_base, (VALID_PRODUCTS,))
    paid_rows = cursor.fetchall()
    
    user_paid_info = {}
    for row in paid_rows:
        sid, pay_ts, pname = row
        if sid not in user_paid_info or pay_ts < user_paid_info[sid]['ts']:
            user_paid_info[sid] = {'ts': pay_ts, 'product': pname}
    
    # 2. Identify which Paid IDs were TRIALS
    # We query in chunks to find if these subscribe_ids have a trial order
    paid_sub_ids = list(user_paid_info.keys())
    trial_users_set = set()
    
    if paid_sub_ids:
        chunk_size = 5000
        for i in range(0, len(paid_sub_ids), chunk_size):
            chunk = paid_sub_ids[i:i+chunk_size]
            fmt = ','.join(['%s'] * len(chunk))
            sql_check = f"SELECT DISTINCT subscribe_id FROM `order` WHERE amount=0 AND status=1 AND subscribe_id IN ({fmt})"
            cursor.execute(sql_check, chunk)
            for r in cursor.fetchall():
                trial_users_set.add(r[0])
    
    # 3. Aggregate Conversions by Calendar Month of Payment
    converted_data = {}
    for sid, info in user_paid_info.items():
        if sid in trial_users_set:
            dt = datetime.datetime.fromtimestamp(info['ts'])
            month_key = dt.strftime('%Y-%m')
            ptype = get_plan_type(info['product'])
            if month_key not in converted_data: 
                converted_data[month_key] = {'total': 0, 'monthly': 0, 'yearly': 0}
            converted_data[month_key]['total'] += 1
            converted_data[month_key][ptype.lower()] += 1

    cohort_analysis = []
    
    # 4. Process Each Month Window
    for month_idx in range(1, 13):
        month_str = f"2025-{month_idx:02d}"
        conv = converted_data.get(month_str, {'total': 0, 'monthly': 0, 'yearly': 0})
        
        # Window: Month Start - 14d ~ Month End - 14d
        first_day = datetime.datetime(2025, month_idx, 1)
        window_start = first_day - datetime.timedelta(days=14)
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        last_day_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        window_end = last_day_dt - datetime.timedelta(days=14)
        start_ts = int(window_start.timestamp())
        end_ts = int(window_end.timestamp())
        
        # NOTE: Updated to count Total Orders, Not Distinct Subscribe IDs
        sql_w = """
            SELECT subscribe_id, product_name FROM `order`
            WHERE amount=0 AND status=1 AND pay_time >= %s AND pay_time <= %s
            AND product_name IN %s
        """
        cursor.execute(sql_w, (start_ts, end_ts, VALID_PRODUCTS))
        t_rows = cursor.fetchall()
        
        # No Aggregation (Count Orders)
        trials_list = [get_plan_type(r[1]) for r in t_rows]
        
        t_total = len(trials_list)
        t_monthly = sum(1 for p in trials_list if p == 'Monthly')
        t_yearly = sum(1 for p in trials_list if p == 'Yearly')
        
        # Calcs
        conv_rate = (conv['total'] / t_total * 100) if t_total > 0 else 0
        m_rate = (conv['monthly'] / t_monthly * 100) if t_monthly > 0 else 0
        y_rate = (conv['yearly'] / t_yearly * 100) if t_yearly > 0 else 0
        
        cohort_analysis.append({
            'month': month_str,
            'period': f"{window_start.strftime('%Y-%m-%d')} ~ {window_end.strftime('%Y-%m-%d')}",
            'convertedTotal': conv['total'],
            'convertedMonthly': conv['monthly'],
            'convertedMonthlyPct': round((conv['monthly']/conv['total']*100) if conv['total']>0 else 0, 2),
            'convertedYearly': conv['yearly'],
            'convertedYearlyPct': round((conv['yearly']/conv['total']*100) if conv['total']>0 else 0, 2),
            'trialTotal': t_total,
            'trialMonthly': t_monthly,
            'trialMonthlyPct': round((t_monthly/t_total*100) if t_total>0 else 0, 2),
            'trialYearly': t_yearly,
            'trialYearlyPct': round((t_yearly/t_total*100) if t_total>0 else 0, 2),
            'rate': round(conv_rate, 2),
            'monthlyRate': round(m_rate, 2),
            'yearlyRate': round(y_rate, 2)
        })
        
    return cohort_analysis
