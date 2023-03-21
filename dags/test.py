import json

tip = '././data/yelp_academic_dataset_checkin.json'
with open(tip, 'r') as f1:
    ll = [json.loads(line.strip()) for line in f1.readlines()]
    print(len(ll))