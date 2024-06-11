import pymongo
from pymongo import MongoClient, errors
import json
import logging
from argparse import ArgumentParser

from src.crawler_api import get_flats


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


def main(args):
    print(args.__dict__)
    mab = args.mab
    house = args.house
    yolo_penthouse = args.yolo_penthouse
    n_pages_x_request = args.pages 
    rent = args.rent
    rent_penthouse = args.rent_penthouse
    flats = get_flats(
                mab=mab, 
                n_pages_x_request=n_pages_x_request, 
                house=house, 
                yolo_penthouse=yolo_penthouse, 
                rent=rent, 
                rent_penthouse=rent_penthouse
                )

    if not flats:
        print("No flats scraped today")
        return None

    print(f"\nScraped flats: {len(flats)}\n")

    flats_with_ids = [dict(flat, **{'_id':int(flat["propertyCode"])}) for flat in flats]

    db = get_db("admin")
    collection_flats = db["_flats"]

    if rent or rent_penthouse:
        print("Rented properties")
        collection_prices = db["_rent_prices"]
        collection_prices_nch = db["_rent_prices_no_change"]
    else:
        print("Properties for sale")
        collection_prices = db["_prices"]
        collection_prices_nch = db["_prices_no_change"]

    new_flats, old_flats = 0,0
    new_flats_ids = []
    new_flats_info = []

    for flat in flats_with_ids:    
        try:
            collection_flats.insert_one(flat)
            new_flats += 1
            new_flats_ids.append(flat['propertyCode'])
            new_flats_info.append(flat)
        except errors.DuplicateKeyError as e:
            old_flats += 1
            continue

    chosen_keys = ["propertyCode", "date", "price"]

    flat_prices = [{k:v for k,v in flat.items() if k in chosen_keys} for flat in flats]
    price_changes = 0

    for flat_price in flat_prices:
        flat_price["price"] = int(flat_price["price"])

        myquery = {
            "propertyCode": flat_price["propertyCode"], 
            "price": flat_price["price"]
            }
        mydoc = collection_prices.find(myquery)

        if len(list(mydoc)) > 0:
            collection_prices_nch.insert_one(flat_price)
        else:
            collection_prices.insert_one(flat_price)
            if flat_price["propertyCode"] not in new_flats_ids:
                price_changes += 1

    print(f"Inserted: {new_flats}, {old_flats} found already existing. Price changes: {price_changes}")


    if not(yolo_penthouse or mab or rent or rent_penthouse):
        with open("output/newest_flats.json", "w") as f:
            json.dump(new_flats_info, f)

    if yolo_penthouse:
        with open("output/newest_penthouses.json", "w") as f:
            json.dump(new_flats_info, f)       


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-mab', '--mab', action="store_true")
    parser.add_argument('-house', '--house', action="store_true")
    parser.add_argument('-yolo_penthouse', '--yolo_penthouse', action="store_true")
    parser.add_argument("-pages", "--pages", action="store", type=int, choices=range(2, 2001))
    parser.add_argument('-rent', '--rent', action="store_true")
    parser.add_argument('-rent_penthouse', '--rent_penthouse', action="store_true")
    args = parser.parse_args()

    main(args)