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
from collections import defaultdict, Counter

from flat_info import save_json
from utils import get_price_records_data, get_avg_prices_district
from db_mongo import get_db


def get_bins_labels():
    bins=[-1, 40, 60, 80, 100, 120, 140, 160, 180, 200, 999999]
    labels=["max 40m", "40-60m", "60-80m", "80-100m", "100-120m","120-140m","140-160m", "160-180m", "180-200m", "200m+"]

    return bins, labels


def get_flat_count() -> int:
    """Returns the number of flats in the database"""
    db = get_db()
    collection_flats = db["_flats"]

    num_flats = collection_flats.estimated_document_count()
    return num_flats


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
        if cur['_id']:
            results[cur['_id']] = cur['neighborhood_count']

    results = dict(sorted(results.items()))

    return results


def get_flats_per_area_cat() -> dict:
    """Get flats per size of the flat"""
    db = get_db()
    collection_flats = db["_flats"]

    df = pd.DataFrame(list(collection_flats.find({})))

    df = df[["propertyCode", "size"]]

    bins, labels_cat = get_bins_labels()

    print(bins)
    print(labels_cat)

    df["area_cat"] = pd.cut(
                        df["size"], 
                        bins=bins,
                        labels=labels_cat)
    
    c = dict(Counter(df["area_cat"]))
    result = {}

    for lab in labels_cat:
        result[lab] = c[lab]

    return result


def get_flats_per_num_rooms() -> dict:
    """Get flats per number of rooms in the flat"""
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

    results_simple = defaultdict(int)
    for key, val in results.items():
        if key >= 7:
            results_simple["7+"] += val
        else:
            results_simple[key] = val

    return results_simple


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


def get_price_change_info() -> dict:
    db = get_db()
    collection_flats = db["_prices"]

    name_cursor = collection_flats.aggregate([
        {'$group': {
             '_id':'$propertyCode', 
             'changes': {'$sum': 1}
             }},
        {'$match': {'changes': {'$gt': 1}}},
        ])
    
    results = {}
    for cur in name_cursor:
        results[cur['_id']] = cur['changes'] - 1

    return results


def get_changes_per_flat() -> dict:
    """Get number of flats price changes per flat"""
    return get_price_change_info()


def get_changes_count() -> dict:
    """Counts flats per number of price changes"""
    price_changes = get_price_change_info()
    results = Counter(price_changes.values())

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

    flats = df_flats[['propertyCode', 'neighborhood', "size"]]

    bins, labels_cat = get_bins_labels()

    flats["area_cat"] = pd.cut(
                    flats["size"], 
                    bins=bins,
                    labels=labels_cat)

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

    grouped = result.groupby(['neighborhood', "month", "area_cat"]).agg({'price_m2': ['mean', 'count']})
    grouped.columns = ['price_mean', 'price_count']
    grouped = grouped.reset_index()
    grouped["area_type"] = 'neighborhood'
    grouped["date"] = grouped["month"].apply(lambda x: f"{x}-15")
    grouped["price_mean"] = grouped["price_mean"].fillna(0)

    results = []
    for _, r in grouped.iterrows():

        if r["price_count"] > 0:
            neigh = r["neighborhood"]
            month = r["month"]
            area_cat = r["area_cat"]
            price = round(r["price_mean"])
            count = r["price_count"]

            results.append({
                "location":neigh,
                "month_num":month,
                "area_category":area_cat,
                "avg_price_per_m":price, 
                "num_flats":count,
                "month":month})

    return results


def save_rooms_labels():
    mydb = get_db()
    collection_flats = mydb["_flats"] 
    distr = collection_flats.distinct("rooms")

    districts = [{"value": x, "label": f'{x} rooms'} for x in distr]

    with open("output/district.json", "w") as f:
        json.dump(districts, f)


def save_neighborhood_labels():
    mydb = get_db()
    collection_flats = mydb["_flats"] 
    neighborohood = collection_flats.distinct("neighborhood")

    neighborohoods = [{"value": x, "label": x} for x in neighborohood]

    with open("output/neighborhoods.json", "w") as f:
        json.dump(neighborohoods, f)


if __name__ == "__main__":
    combined = {}
    
    combined["dates"] = get_dates()
    combined["flat_count"] = get_flat_count()
    combined["flats_per_location"] = get_flats_per_location()
    combined["flats_per_area_cat"] = get_flats_per_area_cat()
    combined["flats_per_num_rooms"] = get_flats_per_num_rooms()
    combined["scraped_per_day"] = get_scraped_per_day()
    combined["changes_per_day"] = get_changes_per_day()
    combined["changes_per_flat"] = get_changes_per_flat()
    combined["changes_count"] = get_changes_count()
    combined["price_m_location"] = get_price_m_location()
    combined["posted_per_day"] = get_scraped_per_day()
    combined["price_m_loc_area_cat"] = get_price_m_loc_area_cat()

    with open("output/flats_mabdata.json", "w") as f:
        json.dump(combined, f)



    # with open("output/district.json", "w") as f:
    #     json.dump(combined, f)

    # save_rooms_labels()
    # save_neighborhood_labels()

    # print(combined)