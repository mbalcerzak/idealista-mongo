import json
import pymongo


def main():
    with open(".db_creds/creds_mongo_mab.json", "r") as f:
        creds_mongo = json.load(f)

    username = creds_mongo["username"]
    password = creds_mongo["password"]

    cluster = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@cluster0.io0gaio.mongodb.net/?retryWrites=true&w=majority")
    mydb = cluster["flats"]
    # collection_flats = mydb["_flats"]
    collection_prices = mydb["_prices"]

    # myquery = {"propertyCode": "98967807"}

    # mydoc = collection_flats.find(myquery)

    # for x in mydoc:
    #     print(x)

    issue_list = []

    name_cursor = collection_prices.aggregate(
        [
        {'$group': {'_id':'$propertyCode', 'count': {'$sum': 1}}}, 
        {'$match': {'count': {'$gt': 1}}},
        {"$sort": {"count" : -1} }
        ]
        )
    max_records = 0
    max_prices_flats = []

    for document in name_cursor:
        if document["count"] > max_records:
            max_records = document["count"]
        issue_list.append(document)
        print(document)

        if document["count"] == 7 and document["_id"] not in max_prices_flats:
            max_prices_flats.append(document["_id"])

    # print(issue_list)

    with open("flat_count.json", "w") as f:
        json.dump(issue_list, f)

    print(f"{max_records=}")
    print(f"{max_prices_flats=}")
    
 

def get_max_price_records_data():
    max_prices_flats=['98932730', '98885094', '98949988', '98928005', '87466683', '98930508', '98950861']

    with open(".db_creds/creds_mongo_mab.json", "r") as f:
        creds_mongo = json.load(f)

    username = creds_mongo["username"]
    password = creds_mongo["password"]

    cluster = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@cluster0.io0gaio.mongodb.net/?retryWrites=true&w=majority")
    mydb = cluster["flats"]
    # collection_flats = mydb["_flats"]
    collection_prices = mydb["_prices"]

    for procertyCode in max_prices_flats[:1]:
        myquery = {"propertyCode": procertyCode}
        # print("-"*40)

        mydoc = collection_prices.find(myquery)

        assert len(list(mydoc)) > 0

        # print(mydoc)


if __name__ == "__main__":
    # main()
    get_max_price_records_data()