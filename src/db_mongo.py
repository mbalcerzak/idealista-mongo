import pymongo
from pymongo import MongoClient, errors
import json

from crawler_api import get_flats


def get_db(permission:str="read"):
    with open(".db_creds/creds_mongo_mab.json", "r") as f:
        creds_mongo = json.load(f)[permission]
    username = creds_mongo["username"]
    password = creds_mongo["password"]

    cluster = pymongo.MongoClient(
        f"mongodb+srv://{username}:{password}@cluster0.io0gaio.mongodb.net/?retryWrites=true&w=majority"
        )
    mydb = cluster["flats"]
    return mydb


def main():
    flats = get_flats()

    print(f"\nScraped flats: {len(flats)}\n")

    flats_with_ids = [dict(flat, **{'_id':int(flat["propertyCode"])}) for flat in flats]

    db = get_db("admin")
    collection_flats = db["_flats"]
    collection_prices = db["_prices_"]

    new_flats, old_flats = 0,0

    for flat in flats_with_ids:    
        try:
            collection_flats.insert_one(flat)
            print(f"New flat {flat['propertyCode']}")
            new_flats += 1
        except errors.DuplicateKeyError as e:
            print(f"Flat {flat['propertyCode']} exists")
            old_flats += 1
            continue

    chosen_keys = ["propertyCode", "date", "price"]

    flat_prices = [{k:v for k,v in flat.items() if k in chosen_keys} for flat in flats]

    for price in flat_prices:
        myquery = {
            "propertyCode": price["propertyCode"], 
            "price": price["price"]
            }
        mydoc = collection_prices.find(myquery)

        if len(list(mydoc)) > 0:
            collection_prices.insert_one(price)
            print(f"New price: {price}")
        else:
            print(f"Price remains the same: {price}")

    print(f"Inserted: {new_flats}, {old_flats} found already existing")


if __name__ == "__main__":
    main()