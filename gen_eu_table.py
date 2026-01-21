
# Data Injection for EU Region

# Group A (Basic/Plus) - N=151 Trial Subs
# Counts from Step 548
data_a = [
    ('Group A', 'Basic monthly', 'Monthly', 2.99, 75),
    ('Group A', 'Basic yearly', 'Yearly', 33.99, 53),
    ('Group A', 'Plus monthly', 'Monthly', 8.99, 8),
    ('Group A', 'Plus yearly', 'Yearly', 85.99, 7),
    ('Group A', 'Enhanced monthly', 'Monthly', 5.99, 6),
    ('Group A', 'Enhanced yearly', 'Yearly', 59.99, 2),
]

# Group B (Basic/Platinum) - N=127 Trial Subs
# Counts from Step 555
data_b = [
    ('Group B', 'Basic monthly', 'Monthly', 2.99, 60),
    ('Group B', 'Basic yearly', 'Yearly', 33.99, 50),
    ('Group B', 'Platinum monthly', 'Monthly', 10.99, 8),
    ('Group B', 'Platinum yearly', 'Yearly', 109.99, 5),
    ('Group B', 'Enhanced monthly', 'Monthly', 5.99, 2),
    ('Group B', 'Enhanced yearly', 'Yearly', 59.99, 2),
]

# Control Group - N=165 Trial Subs
# Counts from Step 713 Output & Prices from Step 726
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

print("| 组别 | 套餐名称 | 套餐类型 | 套餐价格 (EUR) | 当前试用人数 | Pot. 收入 (EUR) |")
print("| :--- | :--- | :--- | :--- | :--- | :--- |")

total_a = 0
total_b = 0
total_c = 0

for row in all_data:
    group, name, ptype, price, count = row
    revenue = price * count
    
    if group == 'Group A': total_a += revenue
    if group == 'Group B': total_b += revenue
    if group == 'Control': total_c += revenue
    
    # Shorten nice name
    nice_name = name.replace('video history event recording', 'Event').replace('history event recording', 'Event')
    print(f"| {group} | {nice_name} | {ptype} | €{price:.2f} | {count} | €{revenue:.2f} |")

print(f"| | | | | |")
print(f"| **SUM Group A** | | | | **151** | **€{total_a:.2f}** |")
print(f"| **SUM Group B** | | | | **127** | **€{total_b:.2f}** |")
print(f"| **SUM Control** | | | | **165** | **€{total_c:.2f}** |")
print(f"| | | | | |")
print(f"| **Avg Group A** | | | | | **€{total_a/151:.2f}** |")
print(f"| **Avg Group B** | | | | | **€{total_b/127:.2f}** |")
print(f"| **Avg Control** | | | | | **€{total_c/165:.2f}** |")

