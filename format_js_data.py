import json

with open('/Users/petercheng/Desktop/code/binew/all_remote_data_output_v21.txt', 'r') as f:
    content = f.read()
    json_start = content.find('{')
    json_data = json.loads(content[json_start:])

keys = [
    'revenueData', 'trialData', 'validOrdersData', 'firstPeriodData', 
    'buyerHistoryData', 'detailedActiveData', 'retentionData', 
    'renewalPeriodData', 'firstPeriodRegDist'
]

for key in keys:
    js_var = key
    if key == 'detailedActiveData': js_var = 'activeSubscriptionData'
    print(f"\n--- {js_var} ---")
    print(f"const {js_var} = {json.dumps(json_data[key], indent=4)};")
