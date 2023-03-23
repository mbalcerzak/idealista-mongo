import pymongo
from pymongo import MongoClient, errors
import json
import logging
from argparse import ArgumentParser

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


def main(args, logger):
    mab = args.mab
    house = args.house
    yolo_penthouse = args.yolo_penthouse
    n_pages_x_request = args.pages 
    flats = get_flats(logger, mab=mab, n_pages_x_request=n_pages_x_request, house=house, yolo_penthouse=yolo_penthouse)

    logger.info(f"\nScraped flats: {len(flats)}\n")

    flats_with_ids = [dict(flat, **{'_id':int(flat["propertyCode"])}) for flat in flats]

    db = get_db("admin")
    collection_flats = db["_flats"]
    collection_prices = db["_prices"]
    collection_prices_nch = db["_prices_no_change"]

    new_flats, old_flats = 0,0
    new_flats_ids = []

    for flat in flats_with_ids:    
        try:
            collection_flats.insert_one(flat)
            logger.info(f"New flat {flat['propertyCode']}")
            new_flats += 1
            new_flats_ids.append(flat['propertyCode'])
        except errors.DuplicateKeyError as e:
            logger.info(f"Flat {flat['propertyCode']} exists")
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
            logger.info(f"Price remains the same: {flat_price}")
            collection_prices_nch.insert_one(flat_price)
        else:
            collection_prices.insert_one(flat_price)
            if flat_price["propertyCode"] in new_flats_ids:
                comment = "new flat"
            else:
                comment = "PRICE CHANGE"
                price_changes += 1
            logger.info(f"New price: {flat_price} --- {comment}")

    logger.info(f"Inserted: {new_flats}, {old_flats} found already existing. Price changes: {price_changes}")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-mab', '--mab', action="store_true")
    parser.add_argument('-house', '--house', action="store_true")
    parser.add_argument('-yolo_penthouse', '--yolo_penthouse', action="store_true")
    parser.add_argument("-pages", "--pages", action="store", type=int, choices=range(2, 2001))
    args = parser.parse_args()

    logger = logging.getLogger(f'flats')

    main(args, logger)