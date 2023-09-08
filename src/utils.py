import json
import pymongo
from db_mongo import get_db
from bson.json_util import dumps
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta


def strip_dict(d:dict) -> dict:
    return {k:v for k,v in d.items() if k in ["propertyCode","price","date"]}


def strip_dict_short(d:dict) -> dict:
    return {k:v for k,v in d.items() if k in ["price","date"]}


def get_flats_multiprice_max(min_count=3) -> list:
    """
    Returns data for flats with the minimum number of price changes
    """
    mydb = get_db()
    collection_prices = mydb["_prices"]
    max_pricesflats = []

    name_cursor = collection_prices.aggregate([
        {'$group': {'_id':'$propertyCode', 'count': {'$sum': 1}}}, 
        {'$match': {'count': {'$gt': 1}}},
        {"$sort": {"count" : -1} }
        ])
    
    for document in name_cursor:
        if document["count"] >= min_count and document["_id"] not in max_pricesflats:
            max_pricesflats.append(document["_id"])

    return max_pricesflats


def get_flats_multiprice_latest(weeks_ago=2, min_changes=3) -> list:
    """
    Returns flats' IDs of flats with most recent price changes
    """
    mydb = get_db()
    collection_prices = mydb["_prices"]
    max_pricesflat_ids = get_flats_multiprice_max(min_changes)

    latest_change_ids = []

    name_cursor = collection_prices.aggregate([
        {'$group': {
                '_id':'$propertyCode', 
                "latest_date": {"$last": '$date'}
                }}
        ])

    latest_price_change = collection_prices.find_one(sort=[("date", -1)])["date"]
    print(f"{latest_price_change=}")

    latest_date = datetime.strptime(latest_price_change, '%Y-%m-%d') - timedelta(weeks=weeks_ago)
    latest_date = latest_date.strftime('%Y-%m-%d')
    print(f"{latest_date=}")

    for document in name_cursor:
        if document["_id"] in max_pricesflat_ids and document["latest_date"] > latest_date:
            latest_change_ids.append(document["_id"])

    return latest_change_ids

    

def get_price_records_data(max_pricesflats:list):
    """Takes a list of flat IDs and returns data for them"""
    mydb = get_db()
    collection_prices = mydb["_prices"]
    results = []

    for propertyCode in max_pricesflats:
        myquery = {"propertyCode": propertyCode}
        info = get_flat_info(propertyCode)

        mydoc = collection_prices.find(myquery)
        dates = []
        prices = []
        for d in mydoc:
            dates.append(d["date"])
            prices.append(d["price"])

        subtext = info["suggestedTexts"]
        price_fmt = f"{round(int(prices[-1])/1000)}k EUR"

        title = f'{subtext["title"]} {subtext["subtitle"]} ({price_fmt}, {int(info["size"])} m2)'

        results.append({
                    "propertyCode": propertyCode, 
                    "prices": prices, 
                    "dates": dates,
                    "info": info,
                    "title": title
                    })
        
    return results


def get_titles(results):
    return([{"value":r["propertyCode"], "label": r["title"]} for r in results])


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

    return mydoc if mydoc else {"_id": propertyCode}


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


def get_avg_prices_district(penthouse=False):
    mydb = get_db()
    collection_flats = mydb["_flats"] 
    collection_prices = mydb["_prices"] 
    collection_prices_nchg = mydb["_prices_no_change"] 

    if penthouse:
        flats = collection_flats.find({"propertyType": "penthouse"})
    else:
        flats = collection_flats.find()
        
    df_flats =  pd.DataFrame(list(flats))

    flats = df_flats[['propertyCode', 'district', 'neighborhood', 'size']]

    # ['_id', 'propertyCode', 'price', 'date']
    prices = collection_prices.find()
    df_prices =  pd.DataFrame(list(prices))

    prices = df_prices[['propertyCode', 'price', 'date']]

    prices_nchg = collection_prices_nchg.find()
    df_prices_nchg = pd.DataFrame(list(prices_nchg))
    prices_nchg = df_prices_nchg[['propertyCode', 'price', 'date']]

    prices_all = pd.concat([prices, prices_nchg])

    result = pd.merge(prices_all, flats, on="propertyCode")
    result["month"] = result["date"].apply(lambda x: x[:7])
    result["price_m2"] = result["price"]/result["size"]

    grouped_district = group_prices(result, "district")
    grouped_neighborhood = group_prices(result, "neighborhood")

    json_district = df_to_json(grouped_district, "district")
    json_neighborhood = df_to_json(grouped_neighborhood, "neighborhood")

    return json_district, json_neighborhood


def get_full_flat_data(data):
    ids = [x["propertyCode"] for x in data]
    flat_data = []

    for id in ids:
        flat_info = get_flat_info(id)
        if flat_info["municipality"] == "València":
            flat_data.append(flat_info)

    return flat_data 


def get_highset_price_diff():
    mydb = get_db()
    collection_prices = mydb["_prices"]
    prices = collection_prices.find()

    name_cursor = collection_prices.aggregate([
        {'$group': {'_id':'$propertyCode', 'count': {'$sum': 1}}}, 
        {'$match': {'count': {'$gt': 2}}},
        {"$sort": {"count" : -1} }
        ])

    ids = [x["_id"] for x in name_cursor]

    flats_multiple_prices = defaultdict(list)

    for id in ids:
        prices_flat = collection_prices.find({"propertyCode":id},{"_id":0})
        for record in prices_flat:
            code = record["propertyCode"]
            flats_multiple_prices[code].append(record["price"])

    for code, prices in flats_multiple_prices.items():
        diff = round((min(prices) - max(prices))/max(prices)*100)
        print(f"{code}: {diff}")


def get_price_diff(ids):
    mydb = get_db()
    collection_prices = mydb["_prices"]

    flats_multiple_prices = defaultdict(list)
    results = {}

    for id in ids:
        prices_flat = collection_prices.find({"propertyCode":id},{"_id":0})
        for record in prices_flat:
            code = record["propertyCode"]
            flats_multiple_prices[code].append(record["price"])

    for code, prices in flats_multiple_prices.items():
        diff = round((min(prices) - max(prices))/max(prices)*100)
        results[code] = diff

    sorted_results = sorted(results.items(), key=lambda x:x[1])

    return sorted_results


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

    

if __name__ == "__main__":
    # price_changes = get_flats_id()
    # get_price_records_data(price_changes)
    # get_avg_prices_district()
    # get_highset_price_diff()
    pass
