import json
import pymongo
from db_mongo import get_db
from bson.json_util import dumps


def get_flats_multiprice() -> list:
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

        issue_list.append(document)
        print(document)

        if document["count"] == max_records and document["_id"] not in max_prices_flats:
            max_prices_flats.append(document["_id"])

    with open("flat_count.json", "w") as f:
        json.dump(issue_list, f)

    print(f"Max number of prices per flat: {max_records}")
    print(f"Flats IDs: {max_prices_flats}")

    return max_prices_flats
    

def get_max_price_records_data(max_prices_flats:list):
    mydb = get_db()
    collection_prices = mydb["_prices_"]

    for procertyCode in max_prices_flats:
        myquery = {"propertyCode": procertyCode}
        print("-"*80)

        mydoc = collection_prices.find(myquery)
        for d in mydoc:
            print(d)


def save_prices():
    mydb = get_db()
    collection_prices = mydb["_prices_"]
    cursor = collection_prices.find({})

    with open('prices.json', 'w') as file:
        json.dump(json.loads(dumps(cursor)), file)


if __name__ == "__main__":
    max_prices_flats = get_flats_multiprice()
    get_max_price_records_data(max_prices_flats)
