from utils import get_flats_multiprice_max, get_price_records_data, update_flat_data, get_avg_prices_district

if __name__ == "__main__":  
    max_prices_flats = get_flats_multiprice_max() 
    get_price_records_data(max_prices_flats)
    update_flat_data()
    get_avg_prices_district()
        