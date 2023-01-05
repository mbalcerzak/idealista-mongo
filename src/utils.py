import json
import pymongo
from db_mongo import get_db
from bson.json_util import dumps


def strip_dict(d:dict) -> dict:
    return {k:v for k,v in d.items() if k in ["propertyCode","price","date"]}


def get_flats_multiprice_max() -> list:
    """Returns data for flats with the highest number of price changes"""
    mydb = get_db()

    collection_prices = mydb["_prices_"]

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
            max_prices_flats = []

        issue_list.append(strip_dict(document))

        if document["count"] == max_records and document["_id"] not in max_prices_flats:
            max_prices_flats.append(document["_id"])

    with open("data/flat_count.json", "w") as f:
        json.dump(issue_list, f)

    print(f"Max number of prices per flat: {max_records}")
    print(f"Flats IDs: {max_prices_flats}")

    return max_prices_flats
    

def get_price_records_data(max_prices_flats:list):
    """Takes a list of flat IDs and returns data for them"""
    mydb = get_db()
    collection_prices = mydb["_prices_"]
    results = []

    for procertyCode in max_prices_flats:
        myquery = {"propertyCode": procertyCode}

        mydoc = collection_prices.find(myquery)
        doc_data = []
        for d in mydoc:
            doc_data.append(strip_dict(d))

        results.append(doc_data)

    with open("data/most_price_changes.json", "w") as f:
        json.dump(json.loads(dumps(results)), f)


def get_flats_id(n:int=2) -> list:
    """Returns IDs of flats that have more than N prices"""
    mydb = get_db()

    collection_prices = mydb["_prices_"]

    name_cursor = collection_prices.aggregate([
        {'$group': {'_id':'$propertyCode', 'count': {'$sum': 1}}}, 
        {'$match': {'count': {'$gt': n}}},
        {"$sort": {"count" : -1} }
        ])

    return [x["_id"] for x in name_cursor]


def save_prices():
    mydb = get_db()
    collection_prices = mydb["_prices_"]
    cursor = collection_prices.find({})

    with open('data/prices.json', 'w') as file:
        json.dump(json.loads(dumps(cursor)), file)


if __name__ == "__main__":
    # max_prices_flats = get_flats_multiprice()
    # get_price_records_data(max_prices_flats)
    price_changes = get_flats_id()
    get_price_records_data(price_changes)
