"""
Script to create JSON in a format read by mabdata.com charts

{
"dates": {
    "max_date": "",
    "min_date": ""
    },
"flats_per_location": {},
"flats_per_area_cat": {},
"scraped_per_day": {},
"scraped_per_day_m_avg": {},
"changes_per_day": {},
"changed_per_day_m_avg": {},
"scraped_per_month": {},
"posted_per_day": {},
"posted_per_day_m_avg": {},
"price_m_location": [],
"price_m_loc_area_cat": []
}
"""
import json
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from collections import defaultdict

from flat_info import save_json
from utils import get_price_records_data, get_avg_prices_district
from db_mongo import get_db


def get_dates() -> dict:
    """Get max and min dates from the database"""
    db = get_db()
    collection_flats = db["_flats"]

    max_cursor = collection_flats.find().sort('date',-1).limit(1)
    max_date = [c['date'] for c in max_cursor][0]

    min_cursos = collection_flats.find().sort('date',1).limit(1)
    min_date = [c['date'] for c in min_cursos][0]

    return {"max_date":max_date, "min_date":min_date}


def get_flats_per_location() -> dict:
    """Get flats per district"""
    db = get_db()
    collection_flats = db["_flats"]

    name_cursor = collection_flats.aggregate([
        {'$group': {
             '_id':'$neighborhood', 
             'neighborhood_count': {'$sum': 1}
             }}
        ])
    
    results = {}
    for cur in name_cursor:
        results[cur['_id']] = cur['neighborhood_count']

    return results


def get_flats_per_area_cat() -> dict:
    """Get flats per size of the flat (number of rooms)"""
    db = get_db()
    collection_flats = db["_flats"]

    name_cursor = collection_flats.aggregate([
        {'$group': {
             '_id':'$rooms', 
             'flat_count': {'$sum': 1}
             }}
        ])
    
    results = {}
    for cur in name_cursor:
        results[cur['_id']] = cur['flat_count']

    results = dict(sorted(results.items()))

    return results


def get_scraped_per_day() -> dict:
    """Get number of flats scraped per day"""
    db = get_db()
    collection_flats = db["_flats"]

    name_cursor = collection_flats.aggregate([
        {'$group': {
             '_id':'$date', 
             'scraped': {'$sum': 1}
             }}
        ])
    
    results = {}
    for cur in name_cursor:
        results[cur['_id']] = cur['scraped']

    return results


def get_scraped_per_day_m_avg() -> dict:
    """Get moving average of number of flats scraped per day"""


def get_changes_per_day() -> dict:
    """Get flats price changes per day TODO some changes are actally first prices"""
    db = get_db()
    collection_flats = db["_prices"]

    name_cursor = collection_flats.aggregate([
        {'$group': {
             '_id':'$date', 
             'changes': {'$sum': 1}
             }},
        {'$match': {'changes': {'$gt': 1}}},
        ])
    
    results = {}
    for cur in name_cursor:
        results[cur['_id']] = cur['changes']

    return results


def get_changed_per_day_m_avg() -> dict:
    """Get moving average of number of flats price changes per day"""

def get_scraped_per_month() -> dict:
    """Get number of flats scraped per day"""

def get_posted_per_day() -> dict:
    """Get number of flats posted per day"""

def get_posted_per_day_m_avg() -> dict:
    """Get moving average of number of flats posted per day"""

def get_price_m_location() -> dict:
    """Get price per location per month"""
    mydb = get_db()
    collection_flats = mydb["_flats"] 
    collection_prices = mydb["_prices"] 
    collection_prices_nchg = mydb["_prices_no_change"] 

    flats = collection_flats.find()
        
    df_flats =  pd.DataFrame(list(flats))

    flats = df_flats[['propertyCode', 'neighborhood', 'size']]

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

    grouped = result.groupby(['neighborhood', "month"]).agg({'price_m2': ['mean', 'count']})
    grouped.columns = ['price_mean', 'price_count']
    grouped = grouped.reset_index()
    grouped["area_type"] = 'neighborhood'
    grouped["date"] = grouped["month"].apply(lambda x: f"{x}-15")

    results = []
    for _, r in grouped.iterrows():
        neigh = r["neighborhood"]
        month = r["month"]
        price = r["price_mean"]
        count = r["price_count"]

        results.append({
            "location":neigh,
            "month_num":month,
            "avg_price_per_m":price, 
            "num_flats":count,
            "month":month})

    return results


def get_price_m_loc_area_cat() -> dict:
    """Get price per location per month per flat size category"""
    mydb = get_db()
    collection_flats = mydb["_flats"] 
    collection_prices = mydb["_prices"] 
    collection_prices_nchg = mydb["_prices_no_change"] 

    flats = collection_flats.find()
        
    df_flats =  pd.DataFrame(list(flats))

    flats = df_flats[['propertyCode', 'neighborhood', 'rooms', "size"]]

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

    grouped = result.groupby(['neighborhood', "month", "rooms"]).agg({'price_m2': ['mean', 'count']})
    grouped.columns = ['price_mean', 'price_count']
    grouped = grouped.reset_index()
    grouped["area_type"] = 'neighborhood'
    grouped["date"] = grouped["month"].apply(lambda x: f"{x}-15")

    results = []
    for _, r in grouped.iterrows():
        neigh = r["neighborhood"]
        month = r["month"]
        rooms = r["rooms"]
        price = r["price_mean"]
        count = r["price_count"]

        results.append({
            "location":neigh,
            "month_num":month,
            "area_category":rooms,
            "avg_price_per_m":price, 
            "num_flats":count,
            "month":month})

    return results


if __name__ == "__main__":

    combined = {}
    
    combined["dates"] = get_dates()

    combined["flats_per_location"] = get_flats_per_location()
    combined["flats_per_area_cat"] = get_flats_per_area_cat()
    combined["scraped_per_day"] = get_scraped_per_day()
    combined["changes_per_day"] = get_changes_per_day()
    combined["price_m_location"] = get_price_m_location()
    combined["posted_per_day"] = get_scraped_per_day()
    combined["price_m_loc_area_cat"] = get_price_m_loc_area_cat()

    with open("output/flats_mabdata.json", "w") as f:
        json.dump(combined, f)