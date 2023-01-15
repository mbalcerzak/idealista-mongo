import json
import pymongo
from db_mongo import get_db
from bson.json_util import dumps


def strip_dict(d:dict) -> dict:
    return {k:v for k,v in d.items() if k in ["propertyCode","price","date"]}


def strip_dict_short(d:dict) -> dict:
    return {k:v for k,v in d.items() if k in ["price","date"]}


def get_flats_multiprice_max() -> list:
    """Returns data for flats with the highest number of price changes"""
    mydb = get_db()

    collection_prices = mydb["_prices"]

    issue_list = []

    name_cursor = collection_prices.aggregate([
        {'$group': {'_id':'$propertyCode', 'count': {'$sum': 1}}}, 
        {'$match': {'count': {'$gt': 1}}},
        {"$sort": {"count" : -1} }
        ])
    max_records = 0
    
    for document in name_cursor:
        if document["count"] > max_records:
            max_records = document["count"]
            max_pricesflats = []

        issue_list.append(strip_dict(document))

        if document["count"] == max_records and document["_id"] not in max_pricesflats:
            max_pricesflats.append(document["_id"])

    with open("data/flat_count.json", "w") as f:
        json.dump(issue_list, f)

    print(f"Max number of prices per flat: {max_records}")
    print(f"Flats IDs: {max_pricesflats}")

    return max_pricesflats
    

def get_price_records_data(max_pricesflats:list):
    """Takes a list of flat IDs and returns data for them"""
    mydb = get_db()
    collection_prices = mydb["_prices"]
    results = {}

    for procertyCode in max_pricesflats:
        myquery = {"propertyCode": procertyCode}

        mydoc = collection_prices.find(myquery)
        doc_data = {}
        for d in mydoc:
            doc_data[d["date"]] = d["price"]

        doc_data_sorted = dict(sorted(doc_data.items()))
        results[procertyCode] = doc_data_sorted

    with open("output/most_price_changes.json", "w") as f:
        json.dump(json.loads(dumps(results)), f)


def get_flats_id(n:int=2) -> list:
    """Returns IDs of flats that have more than N prices"""
    mydb = get_db()

    collection_prices = mydb["_prices"]

    name_cursor = collection_prices.aggregate([
        {'$group': {'_id':'$propertyCode', 'count': {'$sum': 1}}}, 
        {'$match': {'count': {'$gt': n}}},
        {"$sort": {"count" : -1} }
        ])

    return [x["_id"] for x in name_cursor]


def save_prices():
    mydb = get_db()
    collection_prices = mydb["_prices"]
    cursor = collection_prices.find({})

    with open('data/prices.json', 'w') as file:
        json.dump(json.loads(dumps(cursor)), file)


def upload_prices():
    db = get_db("admin")
    collection_prices = db["_prices"]

    with open("data/prices.json", "r") as f:
        prices = json.load(f)

    chosen_keys = ["propertyCode", "date", "price"]

    flat_prices = [{k:v for k,v in flat.items() if k in chosen_keys} for flat in prices]

    for flat_price in flat_prices:
        myquery = {
            "propertyCode": flat_price["propertyCode"], 
            "price": flat_price["price"]
            }
        mydoc = collection_prices.find(myquery)

        flat_price["price"] = int(flat_price["price"])

        if len(list(mydoc)) > 0:
            print(f"Price remains the same: {flat_price}")
        else:
            collection_prices.insert_one(flat_price)
            print(f"New price: {flat_price}")


if __name__ == "__main__":
    price_changes = get_flats_id()
    get_price_records_data(price_changes)