import pymongo
from pymongo import MongoClient, errors
import json
from db_mongo import get_db


def get_flat_info(propertyCode:str):
    mydb = get_db()

    collection_flats = mydb["_flats"]
    myquery = {"propertyCode": propertyCode}
    mydoc = collection_flats.find_one(myquery)

    return mydoc


def get_avg_prices_district():
    mydb = get_db()
    collection_flats = mydb["_flats"] 

    'priceByArea'
    'neighborhood'

    name_cursor = collection_flats.aggregate([
        {'$group': {'_id':'$neighborhood', 'avg_val': {'$avg': "$priceByArea"}}}
        ])
 
    prices = {n['_id']:n['avg_val'] for n in name_cursor}
    
    with open("output/avg_district_prices.json", "w") as f:
        json.dump(prices, f)


if __name__ == "__main__":        
    with open("output/most_price_changes.json", "r") as f:
        data = json.load(f)

    ids = data.keys()

    flat_data = {}

    for id in ids:
        flat_data[id] = get_flat_info(id)

    with open("output/flat_data.json", "w") as f:
        json.dump(flat_data, f)      