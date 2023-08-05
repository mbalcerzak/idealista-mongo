import pandas as pd
from db_mongo import get_db
from collections import Counter
from sklearn.preprocessing import OneHotEncoder


def get_flats_prq() -> dict:
    """Save flats and prices data as parquet"""
    db = get_db()
    collection_flats = db["_flats"]
    collection_prices = db["_prices"]
 
    flats = collection_flats.find()
    prices = collection_prices.find()

    cols_flats = [
        'propertyCode', 'propertyType',  'size', 'exterior', 'rooms', 'district',
        'bathrooms', 'province', 'municipality', 'description', 'status', 'newDevelopment'
        ]
    cols_price = ['propertyCode', 'price', 'date']

    df_flats = pd.DataFrame.from_dict(flats)
    df_flats = df_flats[cols_flats]

    df_prices = pd.DataFrame.from_dict(prices)
    df_prices = df_prices[cols_price]

    return df_flats, df_prices


def get_final_df():
    df_flats, df_prices = get_flats_prq()

    merged_df = pd.merge(df_prices, df_flats, on="propertyCode")

    c = Counter(merged_df["propertyCode"])
    counter = pd.DataFrame.from_dict(c, orient='index').reset_index()
    flats_price_changed = counter.rename(columns={"index":"propertyCode", 0: "priceChangeCount"})

    df = pd.merge(merged_df, flats_price_changed, on="propertyCode")
    df["priceArea"] = round(df["price"] / df["size"])

    # One Hot Encoding
    districts_org = df["district"].unique()
    districts_dict = {k:str(k).replace("- ","").replace(" ","_").replace("'","") for k in districts_org if k != None}
    districts_dict[None] = None

    df["district_renamed"] = df["district"].apply(lambda x: districts_dict[x])

    propertyType_oneHot = pd.get_dummies(df[["propertyType"]], prefix="", prefix_sep="")
    district_oneHot = pd.get_dummies(df[["district_renamed"]], prefix="", prefix_sep="")
    status = pd.get_dummies(df[["status"]], prefix="", prefix_sep="")

    df_oneHot = pd.concat([df, propertyType_oneHot, district_oneHot, status], axis = 1)

    for distr in sorted(df["district_renamed"].unique()):
        print(distr)

    return df_oneHot


def main():
    df = get_final_df()
    df.to_parquet("data/flats.parquet")


if __name__ == "__main__":
    main()