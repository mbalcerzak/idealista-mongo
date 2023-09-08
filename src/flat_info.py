from db_mongo import get_db
from utils import get_flat_info
import json


def get_price_records_for_flat(propertyCode:str):
    """Takes a list of flat IDs and returns data for them"""
    mydb = get_db()
    collection_prices = mydb["_prices"]

    myquery = {"propertyCode": propertyCode}

    if collection_prices.count_documents(myquery, limit = 1):
        mydoc = collection_prices.find(myquery)

        dates = []
        prices = []
        for d in mydoc:
            dates.append(d["date"])
            prices.append(d["price"])

            results = {"propertyCode": propertyCode, "prices": prices, "dates": dates}

        return results
    
    else:
        return("No price data available")



if __name__ == "__main__":  

    flat_id_list = ["92531310", "102228091", "102257387", "100797549"]

    results = []

    for flat_id in flat_id_list:
        print(flat_id)
        info = get_flat_info(flat_id)
        prices = get_price_records_for_flat(flat_id)

        print(info)

        results.append({"info": info, "prices": prices})

    with open("output/selected_flats.json", "w") as f:
        json.dump(results, f)