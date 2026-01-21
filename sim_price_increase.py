
# Simulation:
# Basic monthly: 2.99 -> 3.99 (+1.00)
# Basic yearly:  33.99 -> 39.99 (+6.00) in EUR

# Group A Counts (N=151):
# 'Basic monthly': 75
# 'Basic yearly': 53
# 'Plus monthly': 8, Price: 8.99
# 'Plus yearly': 7, Price: 85.99
# 'Enhanced monthly': 6, Price: 5.99
# 'Enhanced yearly': 2, Price: 59.99

# Group B Counts (N=127):
# 'Basic monthly': 60
# 'Basic yearly': 50
# 'Platinum monthly': 8, Price: 10.99
# 'Platinum yearly': 5, Price: 109.99
# 'Enhanced monthly': 2, Price: 5.99
# 'Enhanced yearly': 2, Price: 59.99

def calc_sim(group_name, counts, prices_map):
    total_val = 0
    total_cnt = 0
    for name, cnt in counts.items():
        price = prices_map.get(name)
        total_val += cnt * price
        total_cnt += cnt
    
    avg = total_val / total_cnt if total_cnt else 0
    return avg

# Adjusted Prices
prices_sim = {
    'Basic monthly': 3.99,   # NEW
    'Basic yearly': 39.99,   # NEW
    'Plus monthly': 8.99,
    'Plus yearly': 85.99,
    'Enhanced monthly': 5.99,
    'Enhanced yearly': 59.99,
    'Platinum monthly': 10.99,
    'Platinum yearly': 109.99
}

# Group A Data
counts_a = {
    'Basic monthly': 75,
    'Basic yearly': 53,
    'Plus monthly': 8,
    'Plus yearly': 7,
    'Enhanced monthly': 6,
    'Enhanced yearly': 2
}

# Group B Data
counts_b = {
    'Basic monthly': 60,
    'Basic yearly': 50,
    'Platinum monthly': 8,
    'Platinum yearly': 5,
    'Enhanced monthly': 2,
    'Enhanced yearly': 2
}

avg_a_sim = calc_sim('Group A', counts_a, prices_sim)
avg_b_sim = calc_sim('Group B', counts_b, prices_sim)

# Calculate Impact
# Old Avgs: A=18.91, B=20.86
print(f"Simulated Average Prices (EUR):")
print(f"Group A: {avg_a_sim:.2f} EUR (vs 18.91, +{(avg_a_sim-18.91):.2f})")
print(f"Group B: {avg_b_sim:.2f} EUR (vs 20.86, +{(avg_b_sim-20.86):.2f})")
