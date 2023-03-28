from utils import get_flats_multiprice_max, get_price_records_data, \
                    get_full_flat_data, get_avg_prices_district, get_price_diff
import json


def save_json(file, filename):
    with open(f"output/{filename}.json", "w") as f:
        json.dump(file, f)


if __name__ == "__main__":  
    max_prices_flats = get_flats_multiprice_max() 
    price_records_data = get_price_records_data(max_prices_flats)
    save_json(price_records_data, "most_price_changes")

    flat_data = get_full_flat_data(price_records_data)
    save_json(flat_data, "flat_data")

    max_price_diffs = get_price_diff(max_prices_flats)
    save_json(max_price_diffs, "max_price_diffs") 

    json_district, json_neighborhood = get_avg_prices_district()
    save_json(json_district, "avg_district_prices")
    save_json(json_neighborhood, "avg_neighborhood_prices")
        
    json_district, json_neighborhood = get_avg_prices_district(penthouse=True)
    save_json(json_district, "avg_district_prices_pent")
    save_json(json_neighborhood, "avg_neighborhood_prices_pent")