import json
import pymongo


def main():
    with open(".db_creds/creds_mongo_mab.json", "r") as f:
        creds_mongo = json.load(f)

    username = creds_mongo["username"]
    password = creds_mongo["password"]

    cluster = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@cluster0.io0gaio.mongodb.net/?retryWrites=true&w=majority")
    mydb = cluster["flats"]
    collection_flats = mydb["_flats"]
    # collection_prices = mydb["_prices"]

    myquery = {"propertyCode": "98967807"}

    mydoc = collection_flats.find(myquery)

    for x in mydoc:
        print(x)


if __name__ == "__main__":
    main()