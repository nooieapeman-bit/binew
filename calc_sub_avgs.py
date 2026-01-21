
# Data Injection
# Control Prices (Verified):
# '14-day video history event recording annually': 29.00
# '14-day history event recording monthly': 3.00
# '30-day video history event recording annually': 49.00
# '30-day video history event recording monthly': 6.00
# '7-day video history CVR recording monthly': 6.00
# ... others

# A/B Prices (Original EUR):
# Basic monthly: 2.99
# Basic yearly: 33.99
# Plus monthly: 8.99
# Plus yearly: 85.99
# Platinum monthly: 10.99
# Platinum yearly: 109.99
# Enhanced monthly: 5.99
# Enhanced yearly: 59.99

# Counts provided in previous steps
counts_a = {
    'Basic monthly': 75, 'Basic yearly': 53,
    'Plus monthly': 8, 'Plus yearly': 7,
    'Enhanced monthly': 6, 'Enhanced yearly': 2
}
counts_b = {
    'Basic monthly': 60, 'Basic yearly': 50,
    'Platinum monthly': 8, 'Platinum yearly': 5,
    'Enhanced monthly': 2, 'Enhanced yearly': 2
}
# Control Counts (Top items)
counts_c = {
    '14-day video history event recording annually': 31,
    '14-day history event recording monthly': 29,
    '30-day video history event recording annually': 22,
    '30-day video history event recording monthly': 19,
    '7-day video history CVR recording monthly': 11,
    '30-day video history event recording AI monthly': 9,
    '30-day video history event recording pro monthly': 8,
    '7-day video history CVR recording annually': 8,
    '14-day video history CVR recording monthly': 6,
    '30-day video history event recording AI annually': 6,
    '30-day video history event recording pro annually': 5,
    '14-day video history CVR recording annually': 4,
    '14-day video history CVR recording AI monthly': 3,
    '14-day video history CVR recording pro monthly': 3,
    '14-day video history CVR recording AI annually': 1
}
prices_c = {
    '14-day video history event recording annually': 29.0,
    '14-day history event recording monthly': 3.0,
    '30-day video history event recording annually': 49.0,
    '30-day video history event recording monthly': 6.0,
    '7-day video history CVR recording monthly': 6.0,
    '30-day video history event recording AI monthly': 9.0,
    '30-day video history event recording pro monthly': 8.0,
    '7-day video history CVR recording annually': 49.0,
    '14-day video history CVR recording monthly': 9.0,
    '30-day video history event recording AI annually': 89.0,
    '30-day video history event recording pro annually': 69.0,
    '14-day video history CVR recording annually': 89.0,
    '14-day video history CVR recording AI monthly': 12.0,
    '14-day video history CVR recording pro monthly': 10.0,
    '14-day video history CVR recording AI annually': 125.0
}
prices_ab = {
    'Basic monthly': 2.99, 'Basic yearly': 33.99,
    'Plus monthly': 8.99, 'Plus yearly': 85.99,
    'Platinum monthly': 10.99, 'Platinum yearly': 109.99,
    'Enhanced monthly': 5.99, 'Enhanced yearly': 59.99
}

def calc_sub_avg(desc, counts, prices_map):
    m_val, m_cnt = 0, 0
    y_val, y_cnt = 0, 0
    
    for name, cnt in counts.items():
        price = prices_map.get(name)
        if not price: continue
        
        is_yearly = 'year' in name.lower() or 'annually' in name.lower()
        if is_yearly:
            y_val += cnt * price
            y_cnt += cnt
        else:
            m_val += cnt * price
            m_cnt += cnt
            
    m_avg = m_val / m_cnt if m_cnt else 0
    y_avg = y_val / y_cnt if y_cnt else 0
    
    print(f"\n{desc}:")
    print(f"  Monthly Avg: {m_avg:.2f} EUR (N={m_cnt})")
    print(f"  Yearly Avg:  {y_avg:.2f} EUR (N={y_cnt})")

calc_sub_avg("Group A", counts_a, prices_ab)
calc_sub_avg("Group B", counts_b, prices_ab)
calc_sub_avg("Control Group", counts_c, prices_c)
