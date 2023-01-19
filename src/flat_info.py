import pymongo
from pymongo import MongoClient, errors
import json
from db_mongo import get_db
from utils import get_flats_multiprice_max, get_price_records_data, get_flats_id


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


def update_flat_data():
    with open("output/most_price_changes.json", "r") as f:
        data = json.load(f)

    ids = data.keys()

    flat_data = {}

    for id in ids:
        flat_data[id] = get_flat_info(id)

    with open("output/flat_data.json", "w") as f:
        json.dump(flat_data, f)  


if __name__ == "__main__":  
    max_prices_flats = get_flats_multiprice_max() 
    get_price_records_data(max_prices_flats)
    # get_price_records_data(get_flats_id(2))
    update_flat_data()
    get_avg_prices_district()
        