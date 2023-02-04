import json
import pymongo
from db_mongo import get_db
from bson.json_util import dumps
import pandas as pd
from collections import defaultdict


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


def get_flat_info(propertyCode:str):
    mydb = get_db()

    collection_flats = mydb["_flats"]
    myquery = {"propertyCode": propertyCode}
    mydoc = collection_flats.find_one(myquery)

    return mydoc


def group_prices(df:pd.DataFrame, colname:str):
    grouped = df.groupby([colname, "month"]).agg({'price_m2': ['mean', 'count']})
    grouped.columns = ['price_mean', 'price_count']
    grouped = grouped.reset_index()
    grouped["area_type"] = colname
    grouped["date"] = grouped["month"].apply(lambda x: f"{x}-15")
    return grouped


def df_to_json(df, colname):
    results = defaultdict(dict)
    for _, r in df.iterrows():
        distr = r[colname]
        month = r["month"]
        price = r["price_mean"]
        count = r["price_count"]
        results[distr][month] = {"price":price, "count":count}
    return results


def get_avg_prices_district():
    mydb = get_db()
    collection_flats = mydb["_flats"] 
    collection_prices = mydb["_prices"] 

    # TODO add flats._prices_no_change to the analysis


    # ['_id', 'propertyCode', 'thumbnail', 'externalReference', 'numPhotos', 'price', 'propertyType', 
    # 'operation', 'size', 'exterior', 'rooms', 'bathrooms', 'address', 'province', 'municipality', 
    # 'country', 'latitude', 'longitude', 'showAddress', 'url', 'description', 'hasVideo', 'status', 
    # 'newDevelopment', 'priceByArea', 'detailedType', 'suggestedTexts', 'hasPlan', 'has3DTour', 
    # 'has360', 'hasStaging', 'superTopHighlight', 'topNewDevelopment', 'date', 'floor', 'hasLift',
    # 'district', 'neighborhood', 'parkingSpace', 'labels', 'newDevelopmentFinished']
    flats = collection_flats.find()
    df_flats =  pd.DataFrame(list(flats))

    flats = df_flats[['propertyCode', 'district', 'neighborhood', 'size']]

    # ['_id', 'propertyCode', 'price', 'date']
    prices = collection_prices.find()
    df_prices =  pd.DataFrame(list(prices))

    prices = df_prices[['propertyCode', 'price', 'date']]

    result = pd.merge(prices, flats, on="propertyCode")
    result["month"] = result["date"].apply(lambda x: x[:7])
    result["price_m2"] = result["price"]/result["size"]

    grouped_district = group_prices(result, "district")
    grouped_neighborhood = group_prices(result, "neighborhood")

    json_district = df_to_json(grouped_district, "district")
    json_neighborhood = df_to_json(grouped_neighborhood, "neighborhood")

    with open("output/avg_district_prices.json", "w") as f:
        json.dump(json_district, f)

    with open("output/avg_neighborhood_prices.json", "w") as f:
        json.dump(json_neighborhood, f)


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
    # price_changes = get_flats_id()
    # get_price_records_data(price_changes)

    get_avg_prices_district()