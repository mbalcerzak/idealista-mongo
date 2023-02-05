from utils import get_flats_multiprice_max, get_price_records_data, \
                    get_full_flat_data, get_avg_prices_district, get_price_diff
import json
from bson.json_util import dumps


def save_price_records_data(results):
    with open("output/most_price_changes.json", "w") as f:
        json.dump(json.loads(dumps(results)), f)

def save_price_records_data(results):
    with open("output/most_price_changes.json", "w") as f:
        json.dump(json.loads(dumps(results)), f)

def save_json_district(json_district):
    with open("output/avg_district_prices.json", "w") as f:
        json.dump(json_district, f)

def save_json_neighborhood(json_neighborhood):
    with open("output/avg_neighborhood_prices.json", "w") as f:
        json.dump(json_neighborhood, f)

def save_full_flat_data(flat_data):
    with open("output/flat_data.json", "w") as f:
        json.dump(flat_data, f) 

def save_max_price_diff(max_price_diffs):
    with open("output/max_price_diffs.json", "w") as f:
        json.dump(max_price_diffs, f)   


if __name__ == "__main__":  
    max_prices_flats = get_flats_multiprice_max() 
    price_records_data = get_price_records_data(max_prices_flats)
    flat_data = get_full_flat_data(price_records_data)
    save_full_flat_data(flat_data)

    max_price_diffs = get_price_diff(max_prices_flats)
    save_max_price_diff(max_price_diffs) 

    json_district, json_neighborhood = get_avg_prices_district()
    save_json_district(json_district)
    save_json_neighborhood(json_neighborhood)
        