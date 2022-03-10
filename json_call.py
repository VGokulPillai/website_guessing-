import json
f = open('10000_random_records.json')
data = json.load(f)
l = len(data)
website = []
name  = []
for i in range(0,l):
    print(data[i]['website'])
    print(data[i]['name'])
    website.append(data[i]['website'])
    name.append(data[i]['name'])
for str in name:
    if(str == "Diversa Group AG"):
        print("True")
