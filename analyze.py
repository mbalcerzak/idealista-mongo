import json

with open("scraped_api.json", "r") as f:
    flats = json.load(f)[0]['elementList']

# for flat in flats:
#     # print(len(flat.keys()))
#     if len(flat.keys()) == 37:
#         print(flat.keys())

print(len(flats))