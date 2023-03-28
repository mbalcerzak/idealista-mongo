
import json
import pymongo
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

from flat_info import save_json
from utils import get_price_records_data
from db_mongo import get_db


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

    return result, result.keys()


def get_latest_dates_prices(penthouse_ids) -> dict:
    """Takes a list of flat IDs and returns the date of latest price record"""
    mydb = get_db()
    collection_prices = mydb["_prices"]
    collection_prices_nc = mydb["_prices_no_change"]

    name_cursor = collection_prices.aggregate([
        {'$group': {
             '_id':'$propertyCode', 
             'date': {'$max': "$date"}, 
             'price': {'$first': '$price' }
             }},
        
        ])
    
    name_cursor_nc = collection_prices_nc.aggregate([
        {'$group': {
             '_id':'$propertyCode', 
             'date': {'$max': "$date"}, 
             'price': {'$first': '$price' }
             }},
        
        ])

    results = {}

    for cur in name_cursor:
        if cur["_id"] in penthouse_ids:
            results[cur["_id"]] = {'date':cur["date"], 'price':cur["price"]}

    for cur_nc in name_cursor_nc:
        if cur_nc["_id"] in results:
            if results[cur_nc["_id"]]["date"] < cur_nc["date"]:
                results[cur_nc["_id"]]["date"] = cur_nc["date"]
                results[cur_nc["_id"]]["price"] = cur_nc["price"]
        else:
            if cur_nc["_id"] in penthouse_ids:
                results[cur_nc["_id"]] = {cur_nc["date"]: cur_nc["price"]}

    return results


def get_cutoff_date(months_ago=2):
    cutoff_date = date.today() + relativedelta(months=-months_ago)
    return cutoff_date.strftime("%Y-%m-%d")


def get_latest_price_distr(dist):
    with open("output/avg_district_prices_pent.json", "r") as f:
        distr_prices_pent_json = json.load(f)  
    distr_prices = distr_prices_pent_json[dist]
    latest_price = distr_prices[max(distr_prices)]["price"]
    return latest_price
    

def get_latest_price_neigh(neighborhood):
    with open("output/avg_neighborhood_prices_pent.json", "r") as f:
        neighborhood_prices_pent_json = json.load(f) 
    neigh_prices = neighborhood_prices_pent_json[neighborhood]
    latest_price = neigh_prices[max(neigh_prices)]["price"]
    return latest_price


def get_price_data(id, flat_data, latest_date_prices, cutoff_date):
    if any([id not in latest_date_prices,'district' not in flat_data, 'neighborhood'not in flat_data]):
        return
    else:
        if latest_date_prices[id]['date'] < cutoff_date:
            return

    flat_size = flat_data['size']
    district_price_size = 0
    neighborhood_price_size = 0

    areaPrice = flat_data['data']['priceByArea']

    district_price = get_latest_price_distr(flat_data['district'])
    district_price_size = flat_size * district_price
    flat_data['data']['distrPrice'] = district_price
    flat_data['data']['distrPriceDiff'] = (areaPrice - district_price)/district_price

    neighborhood_price = get_latest_price_neigh(flat_data['neighborhood'])
    neighborhood_price_size = flat_size * neighborhood_price
    flat_data['data']['neighPrice'] = neighborhood_price
    flat_data['data']['neighPriceDiff'] = (areaPrice - neighborhood_price)/neighborhood_price

    latest_flat_price = latest_date_prices[id]['price']

    if district_price_size > latest_flat_price or neighborhood_price_size > latest_flat_price:
        return flat_data['data']


def json_into_df():
    with open("output/cheap_penthouses.json", 'r') as f:
        data = json.load(f)

    chosen_cols = ["propertyCode","size", "price", "distrPrice", "distrPriceDiff", 
                   "district", "neighborhood", "neighPrice", "neighPriceDiff", "url"]

    df = pd.DataFrame(columns=chosen_cols)

    for _, info in data.items():
        flat_dict = {k:[v] for k,v in info.items() if k in chosen_cols}
        df = pd.concat([df, pd.DataFrame.from_dict(flat_dict)], ignore_index=True)

    df.to_parquet("output/penthouses.parquet")

    df["distrPriceDiff"] = df["distrPriceDiff"].apply(lambda x: round(x*100,2))
    df["neighPriceDiff"] = df["neighPriceDiff"].apply(lambda x: round(x*100,2))

    df2 = df[["propertyCode","size", "price", "distrPriceDiff","neighPriceDiff", "url"]]

    df2.to_parquet("output/penthouses_prc.parquet")



def main():
    """
    1. Find most recent penthouses
    2. Check latest price record for each
    3. Calculate relative diff between the m2 price in the area
    4. Return a list of most extreme cases
    """
    cutoff_date = get_cutoff_date()

    penthouses_data, penthouse_ids = find_penthouses()
    latest_date_prices = get_latest_dates_prices(penthouse_ids)

    cheap_penthouses = {}
    for penthouse_id, penthouse_data in penthouses_data.items():
        penthouse_data = get_price_data(penthouse_id, penthouse_data, latest_date_prices, cutoff_date)
        if penthouse_data:
            cheap_penthouses[penthouse_id] = penthouse_data

    print(f"How many penthouses: {len(cheap_penthouses)}")

    save_json(cheap_penthouses, "cheap_penthouses")

    penthouses_price_history = get_price_records_data(cheap_penthouses.keys())
    save_json(penthouses_price_history, "penthouses_price_history")


if __name__ == "__main__":
    main()
    json_into_df()

    





