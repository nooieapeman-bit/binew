
# Data Injection for EU Region

# Group A (Basic/Plus) - N=151 Trial Subs
data_a = [
    ('Group A', 'Basic monthly', 'Monthly', 2.99, 75),
    ('Group A', 'Basic yearly', 'Yearly', 33.99, 53),
    ('Group A', 'Plus monthly', 'Monthly', 8.99, 8),
    ('Group A', 'Plus yearly', 'Yearly', 85.99, 7),
    ('Group A', 'Enhanced monthly', 'Monthly', 5.99, 6),
    ('Group A', 'Enhanced yearly', 'Yearly', 59.99, 2),
]

# Group B (Basic/Platinum) - N=127 Trial Subs
data_b = [
    ('Group B', 'Basic monthly', 'Monthly', 2.99, 60),
    ('Group B', 'Basic yearly', 'Yearly', 33.99, 50),
    ('Group B', 'Platinum monthly', 'Monthly', 10.99, 8),
    ('Group B', 'Platinum yearly', 'Yearly', 109.99, 5),
    ('Group B', 'Enhanced monthly', 'Monthly', 5.99, 2),
    ('Group B', 'Enhanced yearly', 'Yearly', 59.99, 2),
]

# Control Group - N=165 Trial Subs
data_c = [
    ('Control', '14-day video history event recording annually', 'Yearly', 29.00, 31),
    ('Control', '14-day history event recording monthly', 'Monthly', 3.00, 29),
    ('Control', '30-day video history event recording annually', 'Yearly', 49.00, 22),
    ('Control', '30-day video history event recording monthly', 'Monthly', 6.00, 19),
    ('Control', '7-day video history CVR recording monthly', 'Monthly', 6.00, 11),
    ('Control', '30-day video history event recording AI monthly', 'Monthly', 9.00, 9),
    ('Control', '30-day video history event recording pro monthly', 'Monthly', 8.00, 8),
    ('Control', '7-day video history CVR recording annually', 'Yearly', 49.00, 8),
    ('Control', '14-day video history CVR recording monthly', 'Monthly', 9.00, 6),
    ('Control', '30-day video history event recording AI annually', 'Yearly', 89.00, 6),
    ('Control', '30-day video history event recording pro annually', 'Yearly', 69.00, 5),
    ('Control', '14-day video history CVR recording annually', 'Yearly', 89.00, 4),
    ('Control', '14-day video history CVR recording AI monthly', 'Monthly', 12.00, 3),
    ('Control', '14-day video history CVR recording pro monthly', 'Monthly', 10.00, 3),
    ('Control', '14-day video history CVR recording AI annually', 'Yearly', 125.00, 1),
]

all_data = data_a + data_b + data_c

print("| 组别 | 套餐名称 | 套餐类型 | 套餐价格 (EUR) | 当前试用人数 | LTV 收入 (EUR) |")
print("| :--- | :--- | :--- | :--- | :--- | :--- |")

total_a = 0
total_b = 0
total_c = 0

for row in all_data:
    group, name, ptype, price, count = row
    
    # LTV Logic
    multiplier = 1.75 if ptype == 'Yearly' else 6.7
    revenue = price * count * multiplier
    
    if group == 'Group A': total_a += revenue
    if group == 'Group B': total_b += revenue
    if group == 'Control': total_c += revenue
    
    nice_name = name.replace('video history event recording', 'Event').replace('history event recording', 'Event')
    print(f"| {group} | {nice_name} | {ptype} | €{price:.2f} | {count} | €{revenue:.2f} |")

print(f"| | | | | |")
print(f"| **SUM Group A** | | | | **151** | **€{total_a:.2f}** |")
print(f"| **SUM Group B** | | | | **127** | **€{total_b:.2f}** |")
print(f"| **SUM Control** | | | | **165** | **€{total_c:.2f}** |")
print(f"| | | | | |")
print(f"| **ARPU Group A** | | | | | **€{total_a/151:.2f}** |")
print(f"| **ARPU Group B** | | | | | **€{total_b/127:.2f}** |")
print(f"| **ARPU Group C** | | | | | **€{total_c/165:.2f}** |")

# Also print Total Revenue per 1000 users for fair comparison (since trial rates differ)
# A: 8.81% trial rate
# B: 8.36% trial rate
# C: 6.37% trial rate

rev_per_1000_a = total_a / 151 * (8.81 * 10) # ARPU * (Trial Users per 1000 Registered) -> (Trials/Users)*1000 = 88.1
rev_per_1000_b = total_b / 127 * (8.36 * 10)
rev_per_1000_c = total_c / 165 * (6.37 * 10)

print(f"\n**综合价值 (Revenue per 1,000 Registered Users):**")
print(f"Group A: €{rev_per_1000_a:.2f}")
print(f"Group B: €{rev_per_1000_b:.2f}")
print(f"Control: €{rev_per_1000_c:.2f}")
