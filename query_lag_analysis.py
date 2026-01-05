import datetime
import calendar

def get_lag_analysis(cursor):
    print("Calculating Registration Lag...")
    lag_analysis = []
    
    for month_idx in range(1, 13):
        month_str = f"2025-{month_idx:02d}"
        first_day = datetime.datetime(2025, month_idx, 1)
        window_start = first_day - datetime.timedelta(days=14)
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        last_day_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        window_end = last_day_dt - datetime.timedelta(days=14)
        start_ts = int(window_start.timestamp())
        end_ts = int(window_end.timestamp())
        
        # Remote tables: `order` and `user` (LEFT JOIN)
        sql_lag = """
            SELECT o.uid, o.pay_time, o.subscribe_id, u.register_time
            FROM `order` o
            LEFT JOIN `user` u ON o.uid = u.uid
            WHERE o.amount = 0 AND o.status = 1 AND o.pay_time >= %s AND o.pay_time <= %s
        """
        cursor.execute(sql_lag, (start_ts, end_ts))
        rows = cursor.fetchall()
        
        unique_orders = {}
        for r in rows:
            if r[2] not in unique_orders: unique_orders[r[2]] = {'uid': r[0], 'pay': r[1], 'reg': r[3]}
        
        total_t = len(unique_orders)
        bins = {
            'Same Day':0, 
            '1-3 Days':0, 
            '4-7 Days':0, 
            '8-14 Days':0, 
            '15-30 Days':0, 
            '30-60 Days':0, 
            '60-90 Days':0, 
            '> 90 Days':0, 
            'Unmatched':0
        }
        
        for o in unique_orders.values():
            if not o['reg']: 
                bins['Unmatched'] += 1
                continue
            
            try:
                reg_val = o['reg']
                if isinstance(reg_val, (int, float)): reg_date = datetime.datetime.fromtimestamp(reg_val).date()
                elif isinstance(reg_val, datetime.datetime): reg_date = reg_val.date()
                else: reg_date = datetime.datetime.fromtimestamp(int(reg_val)).date()
                
                trial_date = datetime.datetime.fromtimestamp(o['pay']).date()
                diff = (trial_date - reg_date).days
                
                if diff <= 0: bins['Same Day']+=1
                elif 1<=diff<=3: bins['1-3 Days']+=1
                elif 4<=diff<=7: bins['4-7 Days']+=1
                elif 8<=diff<=14: bins['8-14 Days']+=1
                elif 15<=diff<=30: bins['15-30 Days']+=1
                elif 31<=diff<=60: bins['30-60 Days']+=1
                elif 61<=diff<=90: bins['60-90 Days']+=1
                else: bins['> 90 Days']+=1
            except:
                bins['Unmatched'] += 1
        
        # Format
        b_fmt = {}
        for k,v in bins.items():
            b_fmt[k] = {'count': v, 'pct': round((v/total_t*100) if total_t>0 else 0, 2)}
        
        lag_analysis.append({
            'month': month_str,
            'period': f"{window_start.strftime('%Y-%m-%d')} ~ {window_end.strftime('%Y-%m-%d')}",
            'totalTrials': total_t,
            'bins': b_fmt
        })
        
    return lag_analysis
