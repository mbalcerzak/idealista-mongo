
import json
import pymongo
import pandas as pd


def get_db(permission:str="read"):
    with open("../.db_creds/creds_mongo_mab.json", "r") as f:
        creds_mongo = json.load(f)[permission]
    username = creds_mongo["username"]
    password = creds_mongo["password"]

    cluster = pymongo.MongoClient(
        f"mongodb+srv://{username}:{password}@cluster0.io0gaio.mongodb.net/?retryWrites=true&w=majority"
        )
    mydb = cluster["flats"]
    return mydb


def find_penthouses():
    mydb = get_db()
    collection_flats = mydb["_flats"] 
    flats = collection_flats.find({"propertyType": "penthouse", 'province': 'València'})
    result = {}
    for flat in flats:
        stats = {}
        if "district" in flat:
            stats["district"] = flat["district"]
        if "neighborhood" in flat:
            stats["neighborhood"] = flat["neighborhood"] 
        if "size" in flat:
            stats["size"] = flat["size"]   
        stats['data'] = flat
        result[flat["propertyCode"]] = stats 

    return result


def get_latest_dates(procertyCodes:list) -> dict:
    """Takes a list of flat IDs and returns the date of latest price update"""
    mydb = get_db()
    collection_prices = mydb["_prices"]

    name_cursor = collection_prices.aggregate([
        {'$group': {'_id':'$propertyCode', 'date': {'$max': "$date"}}}
        ])
    
    results = {}
    for cur in name_cursor:
        if cur["_id"] in procertyCodes:
            results[cur["_id"]] = cur["date"]

    return results


def get_latest_price(ids_dates_dict: dict) -> dict:
    mydb = get_db()
    collection_prices = mydb["_prices"]
    results = {}

    for id, date in ids_dates_dict.items():
        if date > '2023-03-01':
            flat = collection_prices.find({"propertyCode": id, "date": date})[0]
            results[flat['propertyCode']] = {'latestPrice': flat['price'], 'latestDate': flat['date']}

    return results


def get_latest_price_distr(dist):
    with open("../output/avg_district_prices_pent.json", "r") as f:
        distr_prices_pent_json = json.load(f)  
    distr_prices = distr_prices_pent_json[dist]
    latest_price = distr_prices[max(distr_prices)]["price"]
    return latest_price
    


def get_latest_price_neigh(neighborhood):
    with open("../output/avg_neighborhood_prices_pent.json", "r") as f:
        neighborhood_prices_pent_json = json.load(f) 
    neigh_prices = neighborhood_prices_pent_json[neighborhood]
    latest_price = neigh_prices[max(neigh_prices)]["price"]
    return latest_price




def get_price_data(id, flat_data, results):
    flat_size = flat_data['size']
    district_price_size = 0
    neighborhood_price_size = 0

    areaPrice = flat_data['data']['priceByArea']

    if 'district' in flat_data:
        district_price = get_latest_price_distr(flat_data['district'])
        district_price_size = flat_size * district_price
        flat_data['data']['distrPrice'] = district_price
        flat_data['data']['distrPriceDiff'] = (areaPrice - district_price)/district_price
    else:
        return

    if 'neighborhood' in flat_data:
        neighborhood_price = get_latest_price_neigh(flat_data['neighborhood'])
        neighborhood_price_size = flat_size * neighborhood_price
        flat_data['data']['neighPrice'] = neighborhood_price
        flat_data['data']['neighPriceDiff'] = (areaPrice - neighborhood_price)/neighborhood_price
    else:
        return

    if id in results:
        latest_flat_price = results[id]['latestPrice']
    else:
        return

    if district_price_size > latest_flat_price or neighborhood_price_size > latest_flat_price:
        return flat_data['data']


def main():
    penthouses_data = find_penthouses()
    penhouse_latest_dates = get_latest_dates(penthouses_data)
    results = get_latest_price(penhouse_latest_dates)

    cheap_penthouses = {}
    for penthouse_id, penthouse_data in penthouses_data.items():
        penthouse_data = get_price_data(penthouse_id, penthouse_data, results)
        if penthouse_data:
            cheap_penthouses[penthouse_id] = penthouse_data

    print(f"How many penthouses: {len(cheap_penthouses)}")

    with open("../output/cheap_penthouses.json", 'w') as f:
        json.dump(cheap_penthouses, f)


def json_into_df():
    with open("../output/cheap_penthouses.json", 'r') as f:
        data = json.load(f)

    chosen_cols = ["propertyCode","size", "price", "distrPrice", "distrPriceDiff", 
                   "district", "neighborhood", "neighPrice", "neighPriceDiff", "url"]

    df = pd.DataFrame(columns=chosen_cols)

    for _, info in data.items():
        flat_dict = {k:v for k,v in info.items() if k in chosen_cols}
        df = df.append(flat_dict, ignore_index=True)

    df.to_parquet("../output/penthouses.parquet")

    df["distrPriceDiff"] = df["distrPriceDiff"].apply(lambda x: f"{round(x*100,2)} %")
    df["neighPriceDiff"] = df["neighPriceDiff"].apply(lambda x: f"{round(x*100,2)} %")

    df2 = df[["propertyCode","size", "price", "distrPriceDiff","neighPriceDiff", "url"]]

    df2.to_parquet("../output/penthouses_prc.parquet")

if __name__ == "__main__":
    # main()
    json_into_df()





